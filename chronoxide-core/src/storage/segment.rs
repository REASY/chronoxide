use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;
use ulid::Ulid;

use crate::labels::SeriesRef;
use crate::promql::{METRIC_NAME_LABEL, canonicalize_labelset, series_id};
use crate::storage::chunk::{
    ChunkIndexEntry, ChunkSamples, ChunkWriter, read_chunk_index, read_chunk_record_at,
    write_chunk_index,
};
use crate::storage::index::{
    ExactPostingsIndex, read_exact_postings_index, write_exact_postings_index,
};
use crate::storage::series::{
    SERIES_KIND_FLOAT, SegmentSymbols, SeriesEntry, read_series_bin_v1, read_symbols_bin,
    write_series_bin_v1, write_symbols_bin,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentId {
    start_ms: u64,
    end_ms: u64,
    ulid: Ulid,
}

impl SegmentId {
    pub fn new(start_ms: u64, end_ms: u64) -> Result<Self, SegmentIdError> {
        Self::with_ulid(start_ms, end_ms, Ulid::new())
    }

    pub fn with_ulid(start_ms: u64, end_ms: u64, ulid: Ulid) -> Result<Self, SegmentIdError> {
        if start_ms >= end_ms {
            return Err(SegmentIdError::InvalidRange { start_ms, end_ms });
        }
        Ok(Self {
            start_ms,
            end_ms,
            ulid,
        })
    }

    pub fn start_ms(&self) -> u64 {
        self.start_ms
    }

    pub fn end_ms(&self) -> u64 {
        self.end_ms
    }

    pub fn ulid(&self) -> Ulid {
        self.ulid
    }

    pub fn dir_name(&self) -> String {
        format!("seg-{}-{}-{}", self.start_ms, self.end_ms, self.ulid)
    }

    pub fn parse_dir_name(name: &str) -> Result<Self, SegmentIdError> {
        let stripped = name
            .strip_prefix("seg-")
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;
        let mut parts = stripped.splitn(3, '-');
        let start = parts
            .next()
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;
        let end = parts
            .next()
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;
        let ulid_str = parts
            .next()
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;

        let start_ms = start
            .parse::<u64>()
            .map_err(|_| SegmentIdError::InvalidNumber(start.to_string()))?;
        let end_ms = end
            .parse::<u64>()
            .map_err(|_| SegmentIdError::InvalidNumber(end.to_string()))?;
        let ulid = ulid_str
            .parse::<Ulid>()
            .map_err(|_| SegmentIdError::InvalidUlid(ulid_str.to_string()))?;

        Self::with_ulid(start_ms, end_ms, ulid)
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.dir_name())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SegmentIdError {
    #[error("segment range invalid: start_ms={start_ms} end_ms={end_ms}")]
    InvalidRange { start_ms: u64, end_ms: u64 },
    #[error("segment dir format invalid: {0}")]
    InvalidFormat(String),
    #[error("segment dir number invalid: {0}")]
    InvalidNumber(String),
    #[error("segment ulid invalid: {0}")]
    InvalidUlid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentFile {
    MetaJson,
    Symbols,
    Series,
    Chunks,
    OooChunks,
    ChunkIndex,
    Indexes,
    Footer,
}

impl SegmentFile {
    pub fn filename(self) -> &'static str {
        match self {
            SegmentFile::MetaJson => "meta.json",
            SegmentFile::Symbols => "symbols.bin",
            SegmentFile::Series => "series.bin",
            SegmentFile::Chunks => "chunks.bin",
            SegmentFile::OooChunks => "ooo_chunks.bin",
            SegmentFile::ChunkIndex => "chunk_index.bin",
            SegmentFile::Indexes => "indexes.puffin",
            SegmentFile::Footer => "footer.bin",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentPaths {
    segments_dir: PathBuf,
    id: SegmentId,
}

impl SegmentPaths {
    pub fn new(segments_dir: impl AsRef<Path>, id: SegmentId) -> Self {
        Self {
            segments_dir: segments_dir.as_ref().to_path_buf(),
            id,
        }
    }

    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn segments_dir(&self) -> &Path {
        &self.segments_dir
    }

    pub fn dir(&self) -> PathBuf {
        self.segments_dir.join(self.id.dir_name())
    }

    pub fn temp_dir(&self) -> PathBuf {
        self.segments_dir.join(".tmp").join(self.id.dir_name())
    }

    pub fn file_path(&self, file: SegmentFile) -> PathBuf {
        self.dir().join(file.filename())
    }

    pub fn temp_file_path(&self, file: SegmentFile) -> PathBuf {
        self.temp_dir().join(file.filename())
    }

    pub fn create_temp_dir(&self) -> io::Result<SegmentTempDir> {
        let temp_dir = self.temp_dir();
        fs::create_dir_all(&temp_dir)?;
        Ok(SegmentTempDir {
            temp_dir,
            final_dir: self.dir(),
        })
    }
}

#[derive(Debug)]
pub struct SegmentTempDir {
    temp_dir: PathBuf,
    final_dir: PathBuf,
}

impl SegmentTempDir {
    pub fn path(&self) -> &Path {
        &self.temp_dir
    }

    pub fn file_path(&self, file: SegmentFile) -> PathBuf {
        self.temp_dir.join(file.filename())
    }

    pub fn publish(self) -> io::Result<PathBuf> {
        fs::rename(&self.temp_dir, &self.final_dir)?;
        Ok(self.final_dir)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
    pub segment_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub datapoints: u64,
    pub series: u64,
}

#[derive(Debug, Clone)]
pub struct SegmentWriterConfig {
    pub segments_dir: PathBuf,
    pub segment_duration: Duration,
}

impl SegmentWriterConfig {
    pub fn new(segments_dir: impl AsRef<Path>, segment_duration: Duration) -> Self {
        Self {
            segments_dir: segments_dir.as_ref().to_path_buf(),
            segment_duration,
        }
    }
}

struct ActiveSegment {
    id: SegmentId,
    start_ms: u64,
    end_ms: u64,
    datapoints: u64,
    series_map: HashMap<u32, u32>,
    source_series_refs: Vec<u32>,
    series_metadata: Vec<Option<SegmentSeriesMetadata>>,
    chunk_entries: Vec<Vec<ChunkIndexEntry>>,
    chunks: ChunkWriter,
    temp_dir: SegmentTempDir,
}

#[derive(Debug, Clone)]
struct SegmentSeriesMetadata {
    series_id: u64,
    labels: Vec<(String, String)>,
}

pub struct SegmentWriter {
    config: SegmentWriterConfig,
    active: Option<ActiveSegment>,
}

impl SegmentWriter {
    pub fn new(config: SegmentWriterConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.segments_dir)?;
        Ok(Self {
            config,
            active: None,
        })
    }

    pub fn record_sample(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: f64,
    ) -> io::Result<()> {
        self.record_samples(series, &[(timestamp_ms, value)])
    }

    pub fn record_sample_raw(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: f64,
    ) -> io::Result<()> {
        self.record_samples_raw(series, &[(timestamp_ms, value)])
    }

    pub fn record_samples(&mut self, series: SeriesRef, samples: &[(u64, f64)]) -> io::Result<()> {
        self.record_float_samples(series, None, samples, false)
    }

    pub fn record_samples_with_labels(
        &mut self,
        series: SeriesRef,
        labels: &[(String, String)],
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples(series, Some(labels), samples, false)
    }

    fn record_float_samples(
        &mut self,
        series: SeriesRef,
        labels: Option<&[(String, String)]>,
        samples: &[(u64, f64)],
        raw: bool,
    ) -> io::Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let duration_ms = self.segment_duration_ms()?;
        let mut ordered: Vec<(u64, f64)> = samples.to_vec();
        ordered.sort_by_key(|(ts, _)| *ts);

        let mut idx = 0usize;
        while idx < ordered.len() {
            let ts = ordered[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < ordered.len() {
                let next_start = segment_window(ordered[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };

            let local_ref = ensure_local_series(active, series, labels);

            let entry = if raw {
                active
                    .chunks
                    .append_float_chunk_raw(local_ref, &ordered[idx..end_idx])?
            } else {
                active
                    .chunks
                    .append_float_chunk(local_ref, &ordered[idx..end_idx])?
            };
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            idx = end_idx;
        }

        Ok(())
    }

    pub fn record_samples_raw(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples(series, None, samples, true)
    }

    pub fn record_sample_i64(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: i64,
    ) -> io::Result<()> {
        self.record_samples_i64(series, &[(timestamp_ms, value)])
    }

    pub fn record_sample_i64_raw(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: i64,
    ) -> io::Result<()> {
        self.record_samples_i64_raw(series, &[(timestamp_ms, value)])
    }

    pub fn record_samples_i64(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, i64)],
    ) -> io::Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let duration_ms = self.segment_duration_ms()?;
        let mut ordered: Vec<(u64, i64)> = samples.to_vec();
        ordered.sort_by_key(|(ts, _)| *ts);

        let mut idx = 0usize;
        while idx < ordered.len() {
            let ts = ordered[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < ordered.len() {
                let next_start = segment_window(ordered[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };

            let local_ref = ensure_local_series(active, series, None);

            let entry = active
                .chunks
                .append_int_chunk(local_ref, &ordered[idx..end_idx])?;
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            idx = end_idx;
        }

        Ok(())
    }

    pub fn record_samples_i64_raw(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, i64)],
    ) -> io::Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let duration_ms = self.segment_duration_ms()?;
        let mut ordered: Vec<(u64, i64)> = samples.to_vec();
        ordered.sort_by_key(|(ts, _)| *ts);

        let mut idx = 0usize;
        while idx < ordered.len() {
            let ts = ordered[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < ordered.len() {
                let next_start = segment_window(ordered[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };

            let local_ref = ensure_local_series(active, series, None);

            let entry = active
                .chunks
                .append_int_chunk_raw(local_ref, &ordered[idx..end_idx])?;
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            idx = end_idx;
        }

        Ok(())
    }

    // Writes metadata, chunk index, and placeholder files for non-chunk artifacts.
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };

        let start = Instant::now();
        let segment_id = active.id;
        let start_ms = active.start_ms;
        let end_ms = active.end_ms;
        let datapoints = active.datapoints;
        let series = active.series_map.len() as u64;
        let tmp = active.temp_dir;

        let meta = SegmentMeta {
            segment_id: segment_id.dir_name(),
            start_ms,
            end_ms,
            datapoints,
            series,
        };
        let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(io::Error::other)?;
        fs::write(tmp.file_path(SegmentFile::MetaJson), meta_bytes)?;

        let mut chunks = active.chunks;
        chunks.flush()?;

        let mut chunk_index = File::create(tmp.file_path(SegmentFile::ChunkIndex))?;
        write_chunk_index(&mut chunk_index, &active.chunk_entries)?;

        let (symbols, series_entries, postings) =
            build_segment_metadata(&active.source_series_refs, &active.series_metadata);

        let mut symbols_file = File::create(tmp.file_path(SegmentFile::Symbols))?;
        write_symbols_bin(&mut symbols_file, &symbols)?;

        let mut series_file = File::create(tmp.file_path(SegmentFile::Series))?;
        write_series_bin_v1(&mut series_file, &series_entries)?;

        let mut index_file = File::create(tmp.file_path(SegmentFile::Indexes))?;
        write_exact_postings_index(&mut index_file, &postings)?;
        File::create(tmp.file_path(SegmentFile::OooChunks))?;

        fs::write(tmp.file_path(SegmentFile::Footer), b"CHROSEGv1\n")?;
        let published_dir = tmp.publish()?;
        let elapsed = start.elapsed();
        let duration = Duration::from_millis(end_ms - start_ms);
        info!(
            segment_id = %segment_id,
            start_ms,
            end_ms,
            duration=?duration,
            datapoints,
            series,
            elapsed_ms = elapsed.as_millis(),
            path = %published_dir.display(),
            "Segment published"
        );
        Ok(())
    }

    fn segment_duration_ms(&self) -> io::Result<u64> {
        let ms = self.config.segment_duration.as_millis();
        if ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment_duration must be > 0",
            ));
        }
        if ms > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment_duration is too large",
            ));
        }
        Ok(ms as u64)
    }

    fn ensure_active_window(&mut self, start_ms: u64, end_ms: u64) -> io::Result<()> {
        let rotate = match &self.active {
            None => true,
            Some(active) => start_ms != active.start_ms || end_ms != active.end_ms,
        };

        if rotate {
            self.flush()?;
            let id = SegmentId::new(start_ms, end_ms)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
            let temp_dir = SegmentPaths::new(&self.config.segments_dir, id).create_temp_dir()?;
            let chunk_file = File::create(temp_dir.file_path(SegmentFile::Chunks))?;
            let chunks = ChunkWriter::new(chunk_file)?;
            self.active = Some(ActiveSegment {
                id,
                start_ms,
                end_ms,
                datapoints: 0,
                series_map: HashMap::new(),
                source_series_refs: Vec::new(),
                series_metadata: Vec::new(),
                chunk_entries: Vec::new(),
                chunks,
                temp_dir,
            });
        }

        Ok(())
    }
}

fn ensure_local_series(
    active: &mut ActiveSegment,
    series: SeriesRef,
    labels: Option<&[(String, String)]>,
) -> u32 {
    let source_ref = series.get();
    let local_ref = match active.series_map.get(&source_ref) {
        Some(&id) => id,
        None => {
            let id = active.series_map.len() as u32;
            active.series_map.insert(source_ref, id);
            active.source_series_refs.push(source_ref);
            active.series_metadata.push(None);
            active.chunk_entries.push(Vec::new());
            id
        }
    };

    if let Some(labels) = labels
        && active.series_metadata[local_ref as usize].is_none()
    {
        active.series_metadata[local_ref as usize] = Some(canonical_segment_metadata(labels));
    }

    local_ref
}

fn canonical_segment_metadata(labels: &[(String, String)]) -> SegmentSeriesMetadata {
    let metric_name = labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
        .unwrap_or("");
    let attributes: Vec<(&str, &str)> = labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL)
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let canonical = canonicalize_labelset(metric_name, &attributes);
    let id = series_id(&canonical);
    let labels = canonical
        .labels()
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();
    SegmentSeriesMetadata {
        series_id: id,
        labels,
    }
}

fn build_segment_metadata(
    source_series_refs: &[u32],
    metadata: &[Option<SegmentSeriesMetadata>],
) -> (SegmentSymbols, Vec<SeriesEntry>, ExactPostingsIndex) {
    let mut symbols = SegmentSymbols::default();
    let mut series_entries = Vec::with_capacity(source_series_refs.len());
    let mut postings = ExactPostingsIndex::default();

    for (local_ref, source_ref) in source_series_refs.iter().enumerate() {
        let (series_id_value, labels) = match metadata.get(local_ref).and_then(Option::as_ref) {
            Some(metadata) => (metadata.series_id, metadata.labels.as_slice()),
            None => (u64::from(*source_ref), &[][..]),
        };

        let mut encoded_labels = Vec::with_capacity(labels.len());
        for (key, value) in labels {
            let key_sym = symbols.intern(key);
            let value_sym = symbols.intern(value);
            postings.insert(key_sym, value_sym, local_ref as u32);
            encoded_labels.push((key_sym, value_sym));
        }

        series_entries.push(SeriesEntry {
            series_id: series_id_value,
            kind_mask: SERIES_KIND_FLOAT,
            labels: encoded_labels,
        });
    }

    (symbols, series_entries, postings)
}

pub struct SegmentReader {
    dir: PathBuf,
    meta: SegmentMeta,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentQueryResult {
    pub series_id: u64,
    pub labels: Vec<(String, String)>,
    pub samples: Vec<(u64, f64)>,
}

pub struct SegmentStoreReader {
    segments: Vec<SegmentReader>,
}

impl SegmentStoreReader {
    pub fn open(segments_dir: impl AsRef<Path>) -> io::Result<Self> {
        let mut segments = Vec::new();
        for entry in fs::read_dir(segments_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("seg-") {
                continue;
            }
            if SegmentId::parse_dir_name(&name).is_err() {
                continue;
            }
            segments.push(SegmentReader::open(entry.path())?);
        }

        segments.sort_by(|left, right| {
            left.meta
                .start_ms
                .cmp(&right.meta.start_ms)
                .then_with(|| left.meta.end_ms.cmp(&right.meta.end_ms))
                .then_with(|| left.meta.segment_id.cmp(&right.meta.segment_id))
        });

        Ok(Self { segments })
    }

    pub fn query_exact(
        &self,
        matchers: &[(&str, &str)],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let mut merged: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }

            for result in segment.query_exact(matchers, start_ms, end_ms)? {
                let entry = merged
                    .entry(result.series_id)
                    .or_insert_with(|| SegmentQueryResult {
                        series_id: result.series_id,
                        labels: result.labels.clone(),
                        samples: Vec::new(),
                    });
                entry.samples.extend(result.samples);
            }
        }

        let mut results: Vec<_> = merged.into_values().collect();
        for result in &mut results {
            result.samples.sort_by_key(|(ts, _)| *ts);
        }
        Ok(results)
    }
}

impl SegmentReader {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let meta_path = dir.join(SegmentFile::MetaJson.filename());
        let meta_bytes = fs::read(meta_path)?;
        let meta = serde_json::from_slice(&meta_bytes).map_err(io::Error::other)?;
        Ok(Self { dir, meta })
    }

    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub fn file_path(&self, file: SegmentFile) -> PathBuf {
        self.dir.join(file.filename())
    }

    pub fn open_chunks(&self) -> io::Result<File> {
        File::open(self.file_path(SegmentFile::Chunks))
    }

    pub fn read_chunk_index(&self) -> io::Result<Vec<Vec<ChunkIndexEntry>>> {
        let mut file = File::open(self.file_path(SegmentFile::ChunkIndex))?;
        read_chunk_index(&mut file)
    }

    pub fn query_exact(
        &self,
        matchers: &[(&str, &str)],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let series = read_series_bin_v1(File::open(self.file_path(SegmentFile::Series))?)?;
        let postings =
            read_exact_postings_index(File::open(self.file_path(SegmentFile::Indexes))?)?;
        let chunk_index = self.read_chunk_index()?;

        let mut candidates: Option<Vec<u32>> = None;
        for (name, value) in matchers {
            let Some(name_sym) = symbols.lookup(name) else {
                return Ok(Vec::new());
            };
            let Some(value_sym) = symbols.lookup(value) else {
                return Ok(Vec::new());
            };
            let Some(posting) = postings.get(name_sym, value_sym) else {
                return Ok(Vec::new());
            };
            candidates = Some(match candidates {
                Some(existing) => intersect_sorted(&existing, posting),
                None => posting.to_vec(),
            });
        }

        let candidate_refs =
            candidates.unwrap_or_else(|| (0..series.len()).map(|idx| idx as u32).collect());
        let mut chunk_file = self.open_chunks()?;
        let mut results = Vec::new();

        for series_ref in candidate_refs {
            let Some(entry) = series.get(series_ref as usize) else {
                continue;
            };
            let Some(entries) = chunk_index.get(series_ref as usize) else {
                continue;
            };

            let mut samples = Vec::new();
            for chunk_entry in entries {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                let record =
                    read_chunk_record_at(&mut chunk_file, chunk_entry.offset, chunk_entry.length)?;
                match record.samples {
                    ChunkSamples::Float(values) => {
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms),
                        );
                    }
                    ChunkSamples::Int64(values) => {
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms)
                                .map(|(ts, value)| (ts, value as f64)),
                        );
                    }
                }
            }

            if samples.is_empty() {
                continue;
            }
            samples.sort_by_key(|(ts, _)| *ts);

            let mut labels = Vec::with_capacity(entry.labels.len());
            for (key, value) in &entry.labels {
                let key = symbols.resolve(*key).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "series key symbol missing")
                })?;
                let value = symbols.resolve(*value).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "series value symbol missing")
                })?;
                labels.push((key.to_string(), value.to_string()));
            }

            results.push(SegmentQueryResult {
                series_id: entry.series_id,
                labels,
                samples,
            });
        }

        Ok(results)
    }
}

fn intersect_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() && ri < right.len() {
        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => li += 1,
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

fn segment_window(timestamp_ms: u64, duration_ms: u64) -> (u64, u64) {
    let start_ms = timestamp_ms.saturating_sub(timestamp_ms % duration_ms);
    (start_ms, start_ms.saturating_add(duration_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkReader, ChunkSamples};
    use std::io::{Read, Seek, SeekFrom};

    const FRAME_HEADER_LEN: u64 = 14;

    fn read_chunk_encoding(file: &mut File) -> u8 {
        file.seek(SeekFrom::Start(FRAME_HEADER_LEN + 1))
            .expect("seek to encoding");
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf).expect("read encoding");
        buf[0]
    }

    #[test]
    fn segment_id_dir_name_roundtrip() {
        let ulid = Ulid::new();
        let id = SegmentId::with_ulid(10, 20, ulid).unwrap();
        let parsed = SegmentId::parse_dir_name(&id.dir_name()).unwrap();
        assert_eq!(parsed.start_ms(), 10);
        assert_eq!(parsed.end_ms(), 20);
        assert_eq!(parsed.ulid(), ulid);
    }

    #[test]
    fn segment_id_rejects_invalid_range() {
        let err = SegmentId::with_ulid(10, 10, Ulid::new()).unwrap_err();
        assert!(matches!(
            err,
            SegmentIdError::InvalidRange {
                start_ms: 10,
                end_ms: 10
            }
        ));
    }

    #[test]
    fn segment_id_rejects_invalid_dir_name() {
        let err = SegmentId::parse_dir_name("seg-10-20").unwrap_err();
        assert!(matches!(err, SegmentIdError::InvalidFormat(_)));
    }

    #[test]
    fn segment_file_names_are_stable() {
        assert_eq!(SegmentFile::MetaJson.filename(), "meta.json");
        assert_eq!(SegmentFile::Symbols.filename(), "symbols.bin");
        assert_eq!(SegmentFile::Series.filename(), "series.bin");
        assert_eq!(SegmentFile::Chunks.filename(), "chunks.bin");
        assert_eq!(SegmentFile::OooChunks.filename(), "ooo_chunks.bin");
        assert_eq!(SegmentFile::ChunkIndex.filename(), "chunk_index.bin");
        assert_eq!(SegmentFile::Indexes.filename(), "indexes.puffin");
        assert_eq!(SegmentFile::Footer.filename(), "footer.bin");
    }

    #[test]
    fn segment_paths_are_consistent() {
        let id = SegmentId::with_ulid(1, 2, Ulid::new()).unwrap();
        let paths = SegmentPaths::new("/tmp/segments", id);
        let dir = paths.dir();
        assert!(dir.ends_with(id.dir_name()));
        let tmp = paths.temp_dir();
        assert!(tmp.ends_with(format!(".tmp/{}", id.dir_name())));
        let chunk_path = paths.file_path(SegmentFile::Chunks);
        assert!(chunk_path.ends_with("chunks.bin"));
    }

    #[test]
    fn segment_writer_creates_segment_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
        writer.flush().unwrap();

        let entries: Vec<_> = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .collect();
        assert_eq!(entries.len(), 1);

        let seg_dir = entries[0].path();
        assert!(seg_dir.join("meta.json").exists());
        assert!(seg_dir.join("chunks.bin").exists());
        assert!(seg_dir.join("series.bin").exists());
        assert!(seg_dir.join("symbols.bin").exists());
        assert!(seg_dir.join("chunk_index.bin").exists());
        assert!(seg_dir.join("indexes.puffin").exists());
        assert!(seg_dir.join("footer.bin").exists());
        let chunk_len = fs::metadata(seg_dir.join("chunks.bin")).unwrap().len();
        assert!(chunk_len > 0);
        let index_len = fs::metadata(seg_dir.join("chunk_index.bin")).unwrap().len();
        assert!(index_len > 0);
    }

    #[test]
    fn segment_writer_rotates_on_new_window() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
        writer
            .record_sample(SeriesRef::new(2), 25_000, 2.5)
            .unwrap();
        writer.flush().unwrap();

        let segments: Vec<_> = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .collect();
        assert_eq!(segments.len(), 2);
    }

    #[test]
    fn segment_writer_batches_samples_per_series() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer
            .record_samples(
                SeriesRef::new(5),
                &[(1_000, 1.0), (2_000, 2.0), (1_500, 1.5)],
            )
            .unwrap();
        writer.flush().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();

        let reader = SegmentReader::open(seg_dir).unwrap();
        assert_eq!(reader.meta().datapoints, 3);
        assert_eq!(reader.meta().series, 1);
        let entries = reader.read_chunk_index().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].len(), 1);
    }

    #[test]
    fn segment_writer_writes_int_chunks() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer
            .record_samples_i64(SeriesRef::new(11), &[(1_000, 5), (2_000, -1)])
            .unwrap();
        writer.flush().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();

        let reader = SegmentReader::open(seg_dir).unwrap();
        let chunk_file = reader.open_chunks().unwrap();
        let mut chunk_reader = ChunkReader::new(chunk_file);
        let record = chunk_reader.read_next().unwrap().unwrap();
        assert_eq!(record.kind, ChunkKind::Int64);
        assert_eq!(
            record.samples,
            ChunkSamples::Int64(vec![(1_000, 5), (2_000, -1)])
        );
    }

    #[test]
    fn segment_writer_writes_raw_float_chunks() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer
            .record_samples_raw(SeriesRef::new(12), &[(1_000, 1.0), (2_000, 2.0)])
            .unwrap();
        writer.flush().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();

        let reader = SegmentReader::open(seg_dir).unwrap();
        let mut chunk_file = reader.open_chunks().unwrap();
        let encoding = read_chunk_encoding(&mut chunk_file);
        assert_eq!(encoding, ChunkEncoding::RawF64 as u8);
        chunk_file.seek(SeekFrom::Start(0)).unwrap();

        let mut chunk_reader = ChunkReader::new(chunk_file);
        let record = chunk_reader.read_next().unwrap().unwrap();
        assert_eq!(record.kind, ChunkKind::Float);
        assert_eq!(
            record.samples,
            ChunkSamples::Float(vec![(1_000, 1.0), (2_000, 2.0)])
        );
    }

    #[test]
    fn segment_writer_writes_raw_int_chunks() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer
            .record_samples_i64_raw(SeriesRef::new(13), &[(1_000, 5), (2_000, -1)])
            .unwrap();
        writer.flush().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();

        let reader = SegmentReader::open(seg_dir).unwrap();
        let mut chunk_file = reader.open_chunks().unwrap();
        let encoding = read_chunk_encoding(&mut chunk_file);
        assert_eq!(encoding, ChunkEncoding::RawI64 as u8);
        chunk_file.seek(SeekFrom::Start(0)).unwrap();

        let mut chunk_reader = ChunkReader::new(chunk_file);
        let record = chunk_reader.read_next().unwrap().unwrap();
        assert_eq!(record.kind, ChunkKind::Int64);
        assert_eq!(
            record.samples,
            ChunkSamples::Int64(vec![(1_000, 5), (2_000, -1)])
        );
    }

    #[test]
    fn segment_reader_loads_meta() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.record_sample(SeriesRef::new(7), 5_000, 7.5).unwrap();
        writer.flush().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();

        let reader = SegmentReader::open(seg_dir).unwrap();
        assert_eq!(reader.meta().datapoints, 1);
        assert_eq!(reader.meta().series, 1);
    }
}
