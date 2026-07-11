use std::env;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use chronoxide_core::storage::io::IoUringReader;
use chronoxide_core::storage::io::{PreadReader, ReadRequest};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use tempfile::NamedTempFile;

const DEFAULT_FILE_MB: u64 = 256;
const DEFAULT_REQUESTS: usize = 256;
const DEFAULT_CHUNK_KB: &[usize] = &[32, 128, 256];
const DEFAULT_QUEUE_DEPTHS: &[u32] = &[8, 32, 128];
const DEFAULT_SEED: u64 = 0x3b3f_81ad_9c7e_2f11;

const ENV_FILE_MB: &str = "IO_BENCH_FILE_MB";
const ENV_REQUESTS: &str = "IO_BENCH_REQUESTS";
const ENV_CHUNK_KB: &str = "IO_BENCH_CHUNK_KB";
const ENV_QUEUE_DEPTHS: &str = "IO_BENCH_QUEUE_DEPTHS";
const ENV_DIR: &str = "IO_BENCH_DIR";
const ENV_SEED: &str = "IO_BENCH_SEED";

#[derive(Clone, Copy)]
enum Pattern {
    Sequential,
    Random,
}

impl Pattern {
    fn as_str(self) -> &'static str {
        match self {
            Pattern::Sequential => "sequential",
            Pattern::Random => "random",
        }
    }
}

struct BenchConfig {
    file_bytes: u64,
    request_count: usize,
    chunk_sizes: Vec<usize>,
    queue_depths: Vec<u32>,
    dir: Option<PathBuf>,
    seed: u64,
}

impl BenchConfig {
    fn from_env() -> Self {
        let file_mb = env_u64(ENV_FILE_MB, DEFAULT_FILE_MB);
        let request_count = env_usize(ENV_REQUESTS, DEFAULT_REQUESTS).max(1);
        let chunk_sizes = env_list_kb(ENV_CHUNK_KB, DEFAULT_CHUNK_KB);
        let queue_depths = env_list_u32(ENV_QUEUE_DEPTHS, DEFAULT_QUEUE_DEPTHS);
        let dir = env_path(ENV_DIR);
        let seed = env_u64(ENV_SEED, DEFAULT_SEED);

        Self {
            file_bytes: file_mb.saturating_mul(1024 * 1024),
            request_count,
            chunk_sizes,
            queue_depths,
            dir,
            seed,
        }
    }
}

fn io_read_benches(c: &mut Criterion) {
    let config = BenchConfig::from_env();
    let max_chunk = config
        .chunk_sizes
        .iter()
        .copied()
        .max()
        .unwrap_or(32 * 1024);
    let min_bytes = (config.request_count as u64).saturating_mul(max_chunk as u64);
    let file_bytes = config.file_bytes.max(min_bytes);
    let tmp = build_test_file(file_bytes, config.dir.as_deref()).expect("create bench file");
    let file = Arc::new(tmp.reopen().expect("reopen bench file"));

    for &chunk_size in &config.chunk_sizes {
        if chunk_size > u32::MAX as usize {
            continue;
        }
        let total_bytes = (config.request_count as u64).saturating_mul(chunk_size as u64);
        for &pattern in &[Pattern::Sequential, Pattern::Random] {
            let requests = build_requests(
                &file,
                file_bytes,
                chunk_size,
                config.request_count,
                pattern,
                config.seed,
            );

            let mut group = c.benchmark_group(format!(
                "io_read_{}_{}kb",
                pattern.as_str(),
                chunk_size / 1024
            ));
            group.throughput(Throughput::Bytes(total_bytes));

            group.bench_with_input(
                BenchmarkId::new("pread", config.request_count),
                &requests,
                |b, requests| {
                    let reader = PreadReader::new();
                    b.iter(|| {
                        let result = reader.read_many(requests).expect("pread read");
                        std::hint::black_box(result);
                    });
                },
            );

            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            for &queue_depth in &config.queue_depths {
                group.bench_with_input(
                    BenchmarkId::new(format!("io_uring_qd{queue_depth}"), config.request_count),
                    &requests,
                    |b, requests| {
                        let reader = IoUringReader::new(queue_depth).expect("io_uring init");
                        b.iter(|| {
                            let result = reader.read_many(requests).expect("io_uring read");
                            std::hint::black_box(result);
                        });
                    },
                );
            }

            group.finish();
        }
    }
}

fn build_test_file(bytes: u64, dir: Option<&Path>) -> io::Result<NamedTempFile> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("chronoxide-io-reads-");
    let mut tmp = match dir {
        Some(dir) => builder.tempfile_in(dir)?,
        None => builder.tempfile()?,
    };
    let mut remaining = bytes;
    let buf = vec![0u8; 1024 * 1024];
    while remaining > 0 {
        let write_len = remaining.min(buf.len() as u64) as usize;
        tmp.write_all(&buf[..write_len])?;
        remaining -= write_len as u64;
    }
    tmp.as_file_mut().sync_all()?;
    Ok(tmp)
}

fn build_requests(
    file: &Arc<File>,
    file_bytes: u64,
    chunk_size: usize,
    request_count: usize,
    pattern: Pattern,
    seed: u64,
) -> Vec<ReadRequest> {
    let mut requests = Vec::with_capacity(request_count);
    match pattern {
        Pattern::Sequential => {
            let mut offset = 0u64;
            for _ in 0..request_count {
                requests.push(ReadRequest {
                    file: Arc::clone(file),
                    offset,
                    len: chunk_size,
                });
                offset += chunk_size as u64;
            }
        }
        Pattern::Random => {
            let max_offset = file_bytes.saturating_sub(chunk_size as u64);
            let mut rng = SmallRng::seed_from_u64(seed ^ (chunk_size as u64) << 32);
            for _ in 0..request_count {
                let offset = rng.random_range(0..=max_offset);
                requests.push(ReadRequest {
                    file: Arc::clone(file),
                    offset,
                    len: chunk_size,
                });
            }
        }
    }
    requests
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var(name).ok().map(PathBuf::from)
}

fn env_list_kb(name: &str, default_kb: &[usize]) -> Vec<usize> {
    let parsed = env::var(name)
        .ok()
        .map(|value| parse_csv_usize(&value))
        .unwrap_or_default();
    let values = if parsed.is_empty() {
        default_kb.to_vec()
    } else {
        parsed
    };
    values
        .into_iter()
        .filter(|value| *value > 0)
        .map(|kb| kb * 1024)
        .collect()
}

fn env_list_u32(name: &str, default: &[u32]) -> Vec<u32> {
    let parsed = env::var(name)
        .ok()
        .map(|value| parse_csv_u32(&value))
        .unwrap_or_default();
    let values = if parsed.is_empty() {
        default.to_vec()
    } else {
        parsed
    };
    values.into_iter().filter(|value| *value > 0).collect()
}

fn parse_csv_usize(value: &str) -> Vec<usize> {
    value
        .split(',')
        .filter_map(|item| item.trim().parse::<usize>().ok())
        .collect()
}

fn parse_csv_u32(value: &str) -> Vec<u32> {
    value
        .split(',')
        .filter_map(|item| item.trim().parse::<u32>().ok())
        .collect()
}

criterion_group!(io_read_group, io_read_benches);
criterion_main!(io_read_group);
