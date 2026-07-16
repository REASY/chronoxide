use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use tempfile::TempDir;

use chronoxide_core::storage::chunk::{ChunkIndexEntry, ChunkKind, write_chunk_index};
use chronoxide_core::storage::io::{PreadReader, ReadRequest};

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use chronoxide_core::storage::io::IoUringReader;

const FRAME_HEADER_LEN: u64 = 14;
const SEGMENT_DURATION_MS: u64 = 60 * 60 * 1_000;
const MIB: f64 = 1024.0 * 1024.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CandidatePattern {
    Contiguous,
    Strided,
    Random,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ReaderMode {
    Pread,
    IoUring,
    Both,
}

#[derive(Debug, Parser)]
#[command(
    name = "spec_chunk_io_bench",
    about = "Generate spec-shaped chunk files and compare pread with io_uring chunk reads"
)]
struct Cli {
    /// Directory where the benchmark dataset is generated. Defaults to a temp dir.
    #[arg(long)]
    dir: Option<PathBuf>,

    /// Keep generated files after the benchmark exits.
    #[arg(long)]
    keep_files: bool,

    /// Reuse an existing dataset in --dir instead of regenerating files.
    #[arg(long)]
    reuse_existing: bool,

    /// Number of immutable segment directories to generate.
    #[arg(long, default_value_t = 2)]
    segments: usize,

    /// Number of series per generated segment.
    #[arg(long, default_value_t = 4096)]
    total_series: usize,

    /// Number of post-selector candidate series to read from each segment.
    #[arg(long, default_value_t = 1024)]
    candidate_series: usize,

    /// Number of chunks per series per segment.
    #[arg(long, default_value_t = 1)]
    chunks_per_series: usize,

    /// Logical chunk size in KiB. This models ChunkHeader + payload bytes.
    #[arg(long, default_value_t = 64)]
    chunk_size_kb: usize,

    /// Percentage of chunks routed to ooo_chunks.bin instead of chunks.bin.
    #[arg(long, default_value_t = 0)]
    ooo_percent: u8,

    /// Candidate series locality pattern after selector evaluation.
    #[arg(long, value_enum, default_value_t = CandidatePattern::Strided)]
    pattern: CandidatePattern,

    /// Measured iterations.
    #[arg(long, default_value_t = 5)]
    iterations: usize,

    /// Warmup iterations excluded from metrics.
    #[arg(long, default_value_t = 1)]
    warmup_iters: usize,

    /// io_uring queue depths to benchmark when mode includes io_uring.
    #[arg(long, value_delimiter = ',', default_value = "8,32,128")]
    queue_depths: Vec<u32>,

    /// Reader mode to benchmark.
    #[arg(long, value_enum, default_value_t = ReaderMode::Both)]
    mode: ReaderMode,

    /// Seed for deterministic candidate and OOO planning.
    #[arg(long, default_value_t = 0x3b3f_81ad_9c7e_2f11)]
    seed: u64,

    /// Create sparse files. Useful for smoke tests; do not use for real SSD measurements.
    #[arg(long)]
    sparse: bool,
}

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    segments: usize,
    total_series: usize,
    candidate_series: usize,
    chunks_per_series: usize,
    chunk_size: usize,
    ooo_percent: u8,
    pattern: CandidatePattern,
    seed: u64,
}

#[derive(Debug, Clone)]
struct PlannedChunk {
    file_id: u8,
    min_time_ms: u64,
    max_time_ms: u64,
    offset: u64,
    length: u32,
}

#[derive(Debug, Clone)]
struct SegmentPlan {
    entries: Vec<Vec<PlannedChunk>>,
    chunk_entries: usize,
    ooo_entries: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestSummary {
    request_count: usize,
    logical_bytes: u64,
    chunk_requests: usize,
    ooo_requests: usize,
}

struct SegmentFiles {
    chunks: Arc<File>,
    ooo_chunks: Arc<File>,
    plan: SegmentPlan,
}

struct BenchDataset {
    root: PathBuf,
    _temp_dir: Option<TempDir>,
    segments: Vec<SegmentFiles>,
}

struct PlannedReads {
    requests: Vec<ReadRequest>,
    summary: RequestSummary,
}

#[derive(Debug)]
struct RunMetrics {
    label: String,
    queue_depth: Option<u32>,
    iterations: usize,
    request_count: usize,
    logical_bytes: u64,
    total: Duration,
    avg: Duration,
    min: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    throughput_mib_s: f64,
}

impl Cli {
    fn bench_config(&self) -> io::Result<BenchConfig> {
        let chunk_size = self
            .chunk_size_kb
            .checked_mul(1024)
            .ok_or_else(|| invalid_input("chunk-size-kb is too large"))?;
        let config = BenchConfig {
            segments: self.segments,
            total_series: self.total_series,
            candidate_series: self.candidate_series,
            chunks_per_series: self.chunks_per_series,
            chunk_size,
            ooo_percent: self.ooo_percent,
            pattern: self.pattern,
            seed: self.seed,
        };
        validate_config(&config)?;
        if self.iterations == 0 {
            return Err(invalid_input("iterations must be > 0"));
        }
        if matches!(self.mode, ReaderMode::IoUring | ReaderMode::Both)
            && self.queue_depths.is_empty()
        {
            return Err(invalid_input("queue-depths must not be empty"));
        }
        if self.queue_depths.contains(&0) {
            return Err(invalid_input("queue-depths must be > 0"));
        }
        if self.reuse_existing && self.dir.is_none() {
            return Err(invalid_input("reuse-existing requires --dir"));
        }
        if self.reuse_existing && self.sparse {
            return Err(invalid_input(
                "reuse-existing cannot be combined with --sparse",
            ));
        }
        Ok(config)
    }
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();
    let config = cli.bench_config()?;
    let candidates = plan_candidates(
        config.pattern,
        config.total_series,
        config.candidate_series,
        config.seed,
    )?;
    let dataset = if cli.reuse_existing {
        BenchDataset::open_existing(
            cli.dir
                .as_deref()
                .expect("validated reuse-existing requires --dir"),
            &config,
        )?
    } else {
        BenchDataset::create(cli.dir.as_deref(), cli.keep_files, cli.sparse, &config)?
    };
    let reads = dataset.plan_reads(&candidates, config.chunk_size)?;

    eprintln!("dataset={}", dataset.root.display());
    eprintln!(
        "segments={} total_series={} candidate_series={} chunks_per_series={} chunk_size_kb={} ooo_percent={} pattern={:?} sparse={} reuse_existing={}",
        config.segments,
        config.total_series,
        config.candidate_series,
        config.chunks_per_series,
        config.chunk_size / 1024,
        config.ooo_percent,
        config.pattern,
        cli.sparse,
        cli.reuse_existing
    );
    eprintln!(
        "requests={} chunks={} ooo={} logical_mib={:.2}",
        reads.summary.request_count,
        reads.summary.chunk_requests,
        reads.summary.ooo_requests,
        reads.summary.logical_bytes as f64 / MIB
    );
    let (artifact_chunk_entries, artifact_ooo_entries) = dataset.artifact_counts();
    eprintln!(
        "artifact_entries chunks={} ooo={}",
        artifact_chunk_entries, artifact_ooo_entries
    );

    println!(
        "mode,queue_depth,iterations,requests,logical_mib,total_ms,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,throughput_mib_s"
    );

    if matches!(cli.mode, ReaderMode::Pread | ReaderMode::Both) {
        let metrics = run_pread(
            &reads.requests,
            reads.summary.logical_bytes,
            cli.warmup_iters,
            cli.iterations,
        )?;
        print_metrics(&metrics);
    }

    if matches!(cli.mode, ReaderMode::IoUring | ReaderMode::Both) {
        for queue_depth in &cli.queue_depths {
            let metrics = run_io_uring(
                &reads.requests,
                reads.summary.logical_bytes,
                *queue_depth,
                cli.warmup_iters,
                cli.iterations,
            )?;
            print_metrics(&metrics);
        }
    }

    Ok(())
}

impl BenchDataset {
    fn create(
        requested_dir: Option<&Path>,
        keep_files: bool,
        sparse: bool,
        config: &BenchConfig,
    ) -> io::Result<Self> {
        let temp_dir = if requested_dir.is_none() {
            Some(
                tempfile::Builder::new()
                    .prefix("chronoxide-spec-io-")
                    .tempdir()?,
            )
        } else {
            None
        };
        let root = match requested_dir {
            Some(dir) => dir.to_path_buf(),
            None => temp_dir
                .as_ref()
                .expect("temp dir exists when requested_dir is none")
                .path()
                .to_path_buf(),
        };
        fs::create_dir_all(&root)?;

        let plans = plan_segments(config)?;
        let mut segments = Vec::with_capacity(plans.len());
        let base_buffer = seeded_chunk_buffer(config.chunk_size, config.seed);

        for (segment_idx, plan) in plans.into_iter().enumerate() {
            let dir = root.join(format!("seg-{segment_idx:06}"));
            fs::create_dir_all(&dir)?;
            let chunks_path = dir.join("chunks.bin");
            let ooo_path = dir.join("ooo_chunks.bin");
            let index_path = dir.join("chunk_index.bin");

            let mut chunks = File::create(&chunks_path)?;
            let mut ooo_chunks = File::create(&ooo_path)?;
            write_segment_data_files(
                &mut chunks,
                &mut ooo_chunks,
                &plan,
                &base_buffer,
                segment_idx,
                sparse,
            )?;
            chunks.sync_all()?;
            ooo_chunks.sync_all()?;

            let index_entries = to_chunk_index_entries(&plan);
            let mut index = File::create(index_path)?;
            write_chunk_index(&mut index, &index_entries)?;
            index.sync_all()?;

            let chunks = Arc::new(File::open(chunks_path)?);
            let ooo_chunks = Arc::new(File::open(ooo_path)?);
            segments.push(SegmentFiles {
                chunks,
                ooo_chunks,
                plan,
            });
        }

        let temp_dir = if keep_files {
            if let Some(temp_dir) = temp_dir {
                let _kept_path = temp_dir.keep();
            }
            None
        } else {
            temp_dir
        };
        Ok(Self {
            root,
            _temp_dir: temp_dir,
            segments,
        })
    }

    fn open_existing(root: &Path, config: &BenchConfig) -> io::Result<Self> {
        let plans = plan_segments(config)?;
        let mut segments = Vec::with_capacity(plans.len());

        for (segment_idx, plan) in plans.into_iter().enumerate() {
            let dir = root.join(format!("seg-{segment_idx:06}"));
            let chunks_path = dir.join("chunks.bin");
            let ooo_path = dir.join("ooo_chunks.bin");
            let index_path = dir.join("chunk_index.bin");

            validate_existing_file_len(&chunks_path, planned_file_len(&plan, 0))?;
            validate_existing_file_len(&ooo_path, planned_file_len(&plan, 1))?;
            validate_existing_file_exists(&index_path)?;

            let chunks = Arc::new(File::open(chunks_path)?);
            let ooo_chunks = Arc::new(File::open(ooo_path)?);
            segments.push(SegmentFiles {
                chunks,
                ooo_chunks,
                plan,
            });
        }

        Ok(Self {
            root: root.to_path_buf(),
            _temp_dir: None,
            segments,
        })
    }

    fn artifact_counts(&self) -> (usize, usize) {
        self.segments
            .iter()
            .fold((0usize, 0usize), |(chunks, ooo), segment| {
                (
                    chunks + segment.plan.chunk_entries,
                    ooo + segment.plan.ooo_entries,
                )
            })
    }

    fn plan_reads(&self, candidates: &[u32], chunk_size: usize) -> io::Result<PlannedReads> {
        let plans: Vec<_> = self
            .segments
            .iter()
            .map(|segment| segment.plan.clone())
            .collect();
        let summary = plan_request_summary(&plans, candidates, chunk_size)?;
        let mut requests = Vec::with_capacity(summary.request_count);

        for segment in &self.segments {
            for &series_ref in candidates {
                let entries = segment
                    .plan
                    .entries
                    .get(series_ref as usize)
                    .ok_or_else(|| invalid_input("candidate series_ref is outside segment plan"))?;
                for entry in entries {
                    let file = match entry.file_id {
                        0 => Arc::clone(&segment.chunks),
                        1 => Arc::clone(&segment.ooo_chunks),
                        _ => return Err(invalid_input("planned chunk has invalid file_id")),
                    };
                    requests.push(ReadRequest {
                        file: file.into(),
                        offset: entry.offset,
                        len: entry.length as usize,
                    });
                }
            }
        }

        Ok(PlannedReads { requests, summary })
    }
}

fn validate_config(config: &BenchConfig) -> io::Result<()> {
    if config.segments == 0 {
        return Err(invalid_input("segments must be > 0"));
    }
    if config.total_series == 0 {
        return Err(invalid_input("total-series must be > 0"));
    }
    if config.candidate_series == 0 {
        return Err(invalid_input("candidate-series must be > 0"));
    }
    if config.candidate_series > config.total_series {
        return Err(invalid_input("candidate-series must be <= total-series"));
    }
    if config.chunks_per_series == 0 {
        return Err(invalid_input("chunks-per-series must be > 0"));
    }
    if config.chunk_size == 0 {
        return Err(invalid_input("chunk-size-kb must be > 0"));
    }
    if config.chunk_size > u32::MAX as usize {
        return Err(invalid_input("chunk size must fit in u32"));
    }
    if config.ooo_percent > 100 {
        return Err(invalid_input("ooo-percent must be <= 100"));
    }
    Ok(())
}

fn plan_candidates(
    pattern: CandidatePattern,
    total_series: usize,
    candidate_series: usize,
    seed: u64,
) -> io::Result<Vec<u32>> {
    if total_series == 0 {
        return Err(invalid_input("total-series must be > 0"));
    }
    if candidate_series == 0 {
        return Err(invalid_input("candidate-series must be > 0"));
    }
    if candidate_series > total_series {
        return Err(invalid_input("candidate-series must be <= total-series"));
    }
    if total_series > u32::MAX as usize {
        return Err(invalid_input("total-series must fit in u32"));
    }

    let candidates = match pattern {
        CandidatePattern::Contiguous => (0..candidate_series as u32).collect(),
        CandidatePattern::Strided => (0..candidate_series)
            .map(|idx| ((idx * total_series) / candidate_series) as u32)
            .collect(),
        CandidatePattern::Random => {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut set = BTreeSet::new();
            while set.len() < candidate_series {
                set.insert(rng.random_range(0..total_series as u32));
            }
            set.into_iter().collect()
        }
    };

    Ok(candidates)
}

fn plan_segments(config: &BenchConfig) -> io::Result<Vec<SegmentPlan>> {
    validate_config(config)?;
    let chunk_len = config.chunk_size as u32;
    let mut segments = Vec::with_capacity(config.segments);
    let chunk_span_ms = (SEGMENT_DURATION_MS / config.chunks_per_series as u64).max(1);

    for segment_idx in 0..config.segments {
        let mut entries = vec![Vec::with_capacity(config.chunks_per_series); config.total_series];
        let mut chunk_offset = 0u64;
        let mut ooo_offset = 0u64;
        let mut chunk_entries = 0usize;
        let mut ooo_entries = 0usize;
        let segment_start_ms = segment_idx as u64 * SEGMENT_DURATION_MS;

        for (series_ref, series_entries) in entries.iter_mut().enumerate() {
            for chunk_idx in 0..config.chunks_per_series {
                let ooo = route_to_ooo(
                    config.seed,
                    segment_idx,
                    series_ref,
                    chunk_idx,
                    config.ooo_percent,
                );
                let (file_id, offset) = if ooo {
                    let offset = ooo_offset + FRAME_HEADER_LEN;
                    ooo_offset += FRAME_HEADER_LEN + config.chunk_size as u64;
                    ooo_entries += 1;
                    (1, offset)
                } else {
                    let offset = chunk_offset + FRAME_HEADER_LEN;
                    chunk_offset += FRAME_HEADER_LEN + config.chunk_size as u64;
                    chunk_entries += 1;
                    (0, offset)
                };
                let min_time_ms = segment_start_ms + chunk_idx as u64 * chunk_span_ms;
                let max_time_ms = min_time_ms.saturating_add(chunk_span_ms).saturating_sub(1);

                series_entries.push(PlannedChunk {
                    file_id,
                    min_time_ms,
                    max_time_ms,
                    offset,
                    length: chunk_len,
                });
            }
        }

        segments.push(SegmentPlan {
            entries,
            chunk_entries,
            ooo_entries,
        });
    }

    Ok(segments)
}

fn plan_request_summary(
    plans: &[SegmentPlan],
    candidates: &[u32],
    chunk_size: usize,
) -> io::Result<RequestSummary> {
    let mut request_count = 0usize;
    let mut chunk_requests = 0usize;
    let mut ooo_requests = 0usize;

    for plan in plans {
        for &series_ref in candidates {
            let entries = plan
                .entries
                .get(series_ref as usize)
                .ok_or_else(|| invalid_input("candidate series_ref is outside segment plan"))?;
            request_count += entries.len();
            for entry in entries {
                match entry.file_id {
                    0 => chunk_requests += 1,
                    1 => ooo_requests += 1,
                    _ => return Err(invalid_input("planned chunk has invalid file_id")),
                }
            }
        }
    }

    Ok(RequestSummary {
        request_count,
        logical_bytes: request_count as u64 * chunk_size as u64,
        chunk_requests,
        ooo_requests,
    })
}

fn route_to_ooo(
    seed: u64,
    segment_idx: usize,
    series_ref: usize,
    chunk_idx: usize,
    percent: u8,
) -> bool {
    match percent {
        0 => false,
        100 => true,
        percent => {
            let mixed = splitmix64(
                seed ^ ((segment_idx as u64) << 48)
                    ^ ((series_ref as u64) << 16)
                    ^ chunk_idx as u64,
            );
            mixed % 100 < percent as u64
        }
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn to_chunk_index_entries(plan: &SegmentPlan) -> Vec<Vec<ChunkIndexEntry>> {
    plan.entries
        .iter()
        .map(|series_entries| {
            series_entries
                .iter()
                .map(|entry| ChunkIndexEntry {
                    file_id: entry.file_id,
                    kind: ChunkKind::Float,
                    flags: 0,
                    min_time_ms: entry.min_time_ms,
                    max_time_ms: entry.max_time_ms,
                    offset: entry.offset,
                    length: entry.length,
                    scalar_lane_offset: 0,
                    scalar_lane_len: 0,
                })
                .collect()
        })
        .collect()
}

fn write_segment_data_files(
    chunks: &mut File,
    ooo_chunks: &mut File,
    plan: &SegmentPlan,
    base_buffer: &[u8],
    segment_idx: usize,
    sparse: bool,
) -> io::Result<()> {
    if sparse {
        chunks.set_len(planned_file_len(plan, 0))?;
        ooo_chunks.set_len(planned_file_len(plan, 1))?;
        return Ok(());
    }

    let frame_header = [0u8; FRAME_HEADER_LEN as usize];
    let mut chunk_buffer = base_buffer.to_vec();

    for (series_ref, entries) in plan.entries.iter().enumerate() {
        for (chunk_idx, entry) in entries.iter().enumerate() {
            stamp_chunk_buffer(&mut chunk_buffer, segment_idx, series_ref, chunk_idx);
            let file = match entry.file_id {
                0 => &mut *chunks,
                1 => &mut *ooo_chunks,
                _ => return Err(invalid_input("planned chunk has invalid file_id")),
            };
            file.write_all(&frame_header)?;
            file.write_all(&chunk_buffer)?;
        }
    }
    Ok(())
}

fn planned_file_len(plan: &SegmentPlan, file_id: u8) -> u64 {
    plan.entries
        .iter()
        .flat_map(|entries| entries.iter())
        .filter(|entry| entry.file_id == file_id)
        .map(|entry| entry.offset + entry.length as u64)
        .max()
        .unwrap_or(0)
}

fn seeded_chunk_buffer(len: usize, seed: u64) -> Vec<u8> {
    let mut buffer = vec![0u8; len];
    let mut rng = SmallRng::seed_from_u64(seed ^ 0x51f1_5e77_1d15_cafe);
    rng.fill_bytes(&mut buffer);
    buffer
}

fn stamp_chunk_buffer(buffer: &mut [u8], segment_idx: usize, series_ref: usize, chunk_idx: usize) {
    if buffer.len() < 24 {
        return;
    }
    buffer[0..8].copy_from_slice(&(segment_idx as u64).to_le_bytes());
    buffer[8..16].copy_from_slice(&(series_ref as u64).to_le_bytes());
    buffer[16..24].copy_from_slice(&(chunk_idx as u64).to_le_bytes());
}

fn run_pread(
    requests: &[ReadRequest],
    logical_bytes: u64,
    warmup_iters: usize,
    iterations: usize,
) -> io::Result<RunMetrics> {
    let reader = PreadReader::new();
    run_reader(
        "pread".to_string(),
        None,
        requests,
        logical_bytes,
        warmup_iters,
        iterations,
        || reader.read_many(requests),
    )
}

fn run_io_uring(
    requests: &[ReadRequest],
    logical_bytes: u64,
    queue_depth: u32,
    warmup_iters: usize,
    iterations: usize,
) -> io::Result<RunMetrics> {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        let reader = IoUringReader::new(queue_depth)?;
        run_reader(
            "io_uring".to_string(),
            Some(queue_depth),
            requests,
            logical_bytes,
            warmup_iters,
            iterations,
            || reader.read_many(requests),
        )
    }

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        let _ = requests;
        let _ = logical_bytes;
        let _ = queue_depth;
        let _ = warmup_iters;
        let _ = iterations;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring mode requires Linux and `cargo run --features io_uring`",
        ))
    }
}

fn run_reader<F>(
    label: String,
    queue_depth: Option<u32>,
    requests: &[ReadRequest],
    logical_bytes: u64,
    warmup_iters: usize,
    iterations: usize,
    mut read_many: F,
) -> io::Result<RunMetrics>
where
    F: FnMut() -> io::Result<Vec<chronoxide_core::storage::io::ReadResult>>,
{
    for _ in 0..warmup_iters {
        let results = read_many()?;
        verify_result_bytes(&results, logical_bytes)?;
        std::hint::black_box(results);
    }

    let mut durations = Vec::with_capacity(iterations);
    let total_start = Instant::now();
    for _ in 0..iterations {
        let start = Instant::now();
        let results = read_many()?;
        let elapsed = start.elapsed();
        verify_result_bytes(&results, logical_bytes)?;
        std::hint::black_box(results);
        durations.push(elapsed);
    }
    let total = total_start.elapsed();
    let mut sorted = durations.clone();
    sorted.sort_unstable();
    let avg = total / iterations as u32;
    let throughput_mib_s = (logical_bytes as f64 * iterations as f64 / MIB) / total.as_secs_f64();

    Ok(RunMetrics {
        label,
        queue_depth,
        iterations,
        request_count: requests.len(),
        logical_bytes,
        total,
        avg,
        min: *sorted.first().expect("iterations > 0"),
        p50: percentile(&sorted, 50),
        p95: percentile(&sorted, 95),
        p99: percentile(&sorted, 99),
        throughput_mib_s,
    })
}

fn verify_result_bytes(
    results: &[chronoxide_core::storage::io::ReadResult],
    expected: u64,
) -> io::Result<()> {
    let actual: u64 = results.iter().map(|result| result.bytes.len() as u64).sum();
    if actual != expected {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("read {actual} bytes, expected {expected}"),
        ));
    }
    Ok(())
}

fn percentile(sorted: &[Duration], percentile: u32) -> Duration {
    let last = sorted.len() - 1;
    let idx = ((last as u64 * percentile as u64) + 50) / 100;
    sorted[idx as usize]
}

fn print_metrics(metrics: &RunMetrics) {
    println!(
        "{},{},{},{},{:.2},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.2}",
        metrics.label,
        metrics
            .queue_depth
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        metrics.iterations,
        metrics.request_count,
        metrics.logical_bytes as f64 / MIB,
        millis(metrics.total),
        millis(metrics.avg),
        millis(metrics.min),
        millis(metrics.p50),
        millis(metrics.p95),
        millis(metrics.p99),
        metrics.throughput_mib_s
    );
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn validate_existing_file_len(path: &Path, expected_len: u64) -> io::Result<()> {
    let metadata = fs::metadata(path).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "failed to stat existing dataset file {}: {err}",
                path.display()
            ),
        )
    })?;
    let actual_len = metadata.len();
    if actual_len != expected_len {
        return Err(invalid_input(format!(
            "existing dataset file {} has length {}, expected {}; check dataset-shape arguments",
            path.display(),
            actual_len,
            expected_len
        )));
    }
    Ok(())
}

fn validate_existing_file_exists(path: &Path) -> io::Result<()> {
    fs::metadata(path).map(|_| ()).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "failed to stat existing dataset file {}: {err}",
                path.display()
            ),
        )
    })
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_candidates_are_dense_and_sorted() {
        let candidates = plan_candidates(CandidatePattern::Contiguous, 16, 5, 7).unwrap();
        assert_eq!(candidates, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn strided_candidates_are_sorted_unique_and_in_range() {
        let candidates = plan_candidates(CandidatePattern::Strided, 16, 5, 7).unwrap();
        assert_eq!(candidates.len(), 5);
        assert!(candidates.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(candidates.iter().all(|series_ref| *series_ref < 16));
    }

    #[test]
    fn random_candidates_are_seeded_and_stable() {
        let first = plan_candidates(CandidatePattern::Random, 128, 16, 42).unwrap();
        let second = plan_candidates(CandidatePattern::Random, 128, 16, 42).unwrap();
        assert_eq!(first, second);
        assert!(first.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn planned_segments_route_all_entries_to_chunks_when_ooo_is_zero() {
        let config = BenchConfig {
            segments: 1,
            total_series: 4,
            candidate_series: 2,
            chunks_per_series: 2,
            chunk_size: 4096,
            ooo_percent: 0,
            pattern: CandidatePattern::Contiguous,
            seed: 1,
        };

        let plan = plan_segments(&config).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].ooo_entries, 0);
        assert_eq!(plan[0].chunk_entries, 8);
    }

    #[test]
    fn planned_requests_count_ooo_and_logical_bytes() {
        let config = BenchConfig {
            segments: 2,
            total_series: 8,
            candidate_series: 4,
            chunks_per_series: 3,
            chunk_size: 8192,
            ooo_percent: 50,
            pattern: CandidatePattern::Strided,
            seed: 9,
        };
        let candidates = plan_candidates(
            config.pattern,
            config.total_series,
            config.candidate_series,
            config.seed,
        )
        .unwrap();
        let plan = plan_segments(&config).unwrap();
        let summary = plan_request_summary(&plan, &candidates, config.chunk_size).unwrap();

        assert_eq!(summary.request_count, 24);
        assert_eq!(summary.logical_bytes, 24 * 8192);
        assert!(summary.ooo_requests > 0);
        assert!(summary.chunk_requests > 0);
    }

    #[test]
    fn reuse_existing_requires_dir() {
        let cli = Cli {
            dir: None,
            keep_files: false,
            reuse_existing: true,
            segments: 1,
            total_series: 4,
            candidate_series: 2,
            chunks_per_series: 1,
            chunk_size_kb: 4,
            ooo_percent: 0,
            pattern: CandidatePattern::Contiguous,
            iterations: 1,
            warmup_iters: 0,
            queue_depths: vec![8],
            mode: ReaderMode::Pread,
            seed: 1,
            sparse: false,
        };

        let err = cli.bench_config().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("requires --dir"));
    }

    #[test]
    fn existing_dataset_can_be_reopened() {
        let tmp = tempfile::tempdir().unwrap();
        let config = BenchConfig {
            segments: 1,
            total_series: 4,
            candidate_series: 2,
            chunks_per_series: 2,
            chunk_size: 4096,
            ooo_percent: 0,
            pattern: CandidatePattern::Contiguous,
            seed: 1,
        };

        let generated =
            BenchDataset::create(Some(tmp.path()), true, false, &config).expect("generate dataset");
        drop(generated);

        let reopened =
            BenchDataset::open_existing(tmp.path(), &config).expect("reopen existing dataset");
        let candidates = plan_candidates(
            config.pattern,
            config.total_series,
            config.candidate_series,
            config.seed,
        )
        .unwrap();
        let reads = reopened.plan_reads(&candidates, config.chunk_size).unwrap();

        assert_eq!(reads.summary.request_count, 4);
        assert_eq!(reads.summary.logical_bytes, 4 * 4096);
    }
}
