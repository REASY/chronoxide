use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crc32c::{crc32c, crc32c_append};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;
use ulid::Ulid;

use crate::labels::{FlatInternedLabelSetStore, SeriesRef, SymbolId, SymbolTable};
use crate::promql::{
    METRIC_NAME_LABEL, PromqlHistogramQuantile, PromqlMatcherOp, PromqlQuery, PromqlQueryError,
    PromqlRangeFunction, PromqlRangeFunctionKind, PromqlSelector, normalize_label_name,
    normalize_metric_name, parse_query,
};
use crate::storage::chunk::{
    ChunkIndexEntry, ChunkIndexReader, ChunkKind, ChunkRecord, ChunkSamples, ChunkScalarProjection,
    ChunkScalarSample, ChunkScalarValue, ChunkWriter, read_chunk_index, read_chunk_record_at,
    read_chunk_scalar_projection_at, write_chunk_index,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramValue, HeadBuffer, HistogramValue,
    OtlpAggregationTemporality, SeriesLabelResolver, SummaryValue, TypedSampleMetadata,
    exponential_histogram_projected_bucket_count, prometheus_stale_nan,
};
use crate::storage::index::{
    ExactPostingsIndex, ExactPostingsMetadata, LabelValueFstIndex, LabelValueTimeRangeIndex,
    SegmentIndexReader, SegmentIndexes, SegmentRoutingIndex, write_segment_indexes,
};
use crate::storage::manifest::{ManifestInventory, ManifestSegment, read_manifest_inventory};
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SERIES_KIND_INT64,
    SERIES_KIND_SUMMARY, SegmentSymbols, SeriesEntry, SeriesReader, read_series_bin,
    read_symbols_bin, write_series_bin, write_symbols_bin,
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

pub trait SegmentIdProvider: fmt::Debug + Send + Sync {
    fn next_segment_id(&self, start_ms: u64, end_ms: u64) -> Result<SegmentId, SegmentIdError>;
}

#[derive(Debug, Default)]
pub struct RandomSegmentIdProvider;

impl SegmentIdProvider for RandomSegmentIdProvider {
    fn next_segment_id(&self, start_ms: u64, end_ms: u64) -> Result<SegmentId, SegmentIdError> {
        SegmentId::new(start_ms, end_ms)
    }
}

#[derive(Debug)]
pub struct DeterministicSegmentIdProvider {
    seed: u64,
    next_ordinal: AtomicU64,
}

impl DeterministicSegmentIdProvider {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            next_ordinal: AtomicU64::new(0),
        }
    }
}

impl SegmentIdProvider for DeterministicSegmentIdProvider {
    fn next_segment_id(&self, start_ms: u64, end_ms: u64) -> Result<SegmentId, SegmentIdError> {
        let ordinal = self.next_ordinal.fetch_add(1, Ordering::Relaxed);
        SegmentId::with_ulid(
            start_ms,
            end_ms,
            deterministic_segment_ulid(self.seed, start_ms, end_ms, ordinal),
        )
    }
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

const SEGMENT_FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"CSFT");
const SEGMENT_FOOTER_VERSION: u16 = 1;
const SEGMENT_SCHEMA_VERSION: u16 = 3;
const SEGMENT_FOOTER_HEADER_LEN: usize = 16;
const SEGMENT_FOOTER_TRAILER_LEN: usize = 4;
const SEGMENT_FOOTER_TRACKED_FILES: [SegmentFile; 7] = [
    SegmentFile::MetaJson,
    SegmentFile::Symbols,
    SegmentFile::Series,
    SegmentFile::Chunks,
    SegmentFile::OooChunks,
    SegmentFile::ChunkIndex,
    SegmentFile::Indexes,
];
const SEGMENT_FLUSH_SIZE_FILES: [SegmentFile; 8] = [
    SegmentFile::MetaJson,
    SegmentFile::Symbols,
    SegmentFile::Series,
    SegmentFile::Chunks,
    SegmentFile::OooChunks,
    SegmentFile::ChunkIndex,
    SegmentFile::Indexes,
    SegmentFile::Footer,
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentFooter {
    schema_version: u16,
    files: Vec<SegmentFooterFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentFooterFile {
    file: SegmentFile,
    size: u64,
    checksum_xxh64: u64,
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
    #[serde(default)]
    pub chunk_summary: Option<SegmentChunkSummary>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentChunkSummary {
    pub chunks: u64,
    pub chunk_bytes: u64,
    pub by_kind: SegmentChunkKindTotals,
}

impl SegmentChunkSummary {
    fn from_chunk_entries(entries: &[Vec<ChunkIndexEntry>]) -> Self {
        let mut summary = Self::default();
        for entry in entries.iter().flatten() {
            summary.add_chunk(entry.kind, u64::from(entry.length));
        }
        summary
    }

    fn add_chunk(&mut self, kind: ChunkKind, bytes: u64) {
        self.chunks = self.chunks.saturating_add(1);
        self.chunk_bytes = self.chunk_bytes.saturating_add(bytes);
        let stats = self.by_kind.stats_mut(kind);
        stats.chunks = stats.chunks.saturating_add(1);
        stats.chunk_bytes = stats.chunk_bytes.saturating_add(bytes);
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentChunkKindTotals {
    pub float: SegmentChunkKindStats,
    pub int64: SegmentChunkKindStats,
    pub histogram: SegmentChunkKindStats,
    pub exponential_histogram: SegmentChunkKindStats,
    pub summary: SegmentChunkKindStats,
}

impl SegmentChunkKindTotals {
    fn stats(&self, kind: ChunkKind) -> SegmentChunkKindStats {
        match kind {
            ChunkKind::Float => self.float,
            ChunkKind::Int64 => self.int64,
            ChunkKind::Histogram => self.histogram,
            ChunkKind::ExponentialHistogram => self.exponential_histogram,
            ChunkKind::Summary => self.summary,
        }
    }

    fn stats_mut(&mut self, kind: ChunkKind) -> &mut SegmentChunkKindStats {
        match kind {
            ChunkKind::Float => &mut self.float,
            ChunkKind::Int64 => &mut self.int64,
            ChunkKind::Histogram => &mut self.histogram,
            ChunkKind::ExponentialHistogram => &mut self.exponential_histogram,
            ChunkKind::Summary => &mut self.summary,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentChunkKindStats {
    pub chunks: u64,
    pub chunk_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SegmentWriterConfig {
    pub segments_dir: PathBuf,
    pub segment_duration: Duration,
    segment_id_provider: Arc<dyn SegmentIdProvider>,
}

impl SegmentWriterConfig {
    pub fn new(segments_dir: impl AsRef<Path>, segment_duration: Duration) -> Self {
        Self {
            segments_dir: segments_dir.as_ref().to_path_buf(),
            segment_duration,
            segment_id_provider: Arc::new(RandomSegmentIdProvider),
        }
    }

    pub fn with_segment_id_provider<P>(mut self, segment_id_provider: P) -> Self
    where
        P: SegmentIdProvider + 'static,
    {
        self.segment_id_provider = Arc::new(segment_id_provider);
        self
    }

    pub fn with_shared_segment_id_provider(
        mut self,
        segment_id_provider: Arc<dyn SegmentIdProvider>,
    ) -> Self {
        self.segment_id_provider = segment_id_provider;
        self
    }

    pub fn with_deterministic_segment_ids(self, seed: u64) -> Self {
        self.with_segment_id_provider(DeterministicSegmentIdProvider::new(seed))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SegmentFlushStageKind {
    MetaJson,
    ChunksFlush,
    ChunkIndex,
    SegmentMetadata,
    LabelValues,
    LabelValueTimeRanges,
    Symbols,
    Series,
    Indexes,
    RoutingIndexBuild,
    OooChunks,
    Footer,
    Publish,
}

impl SegmentFlushStageKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetaJson => "meta_json",
            Self::ChunksFlush => "chunks_flush",
            Self::ChunkIndex => "chunk_index",
            Self::SegmentMetadata => "segment_metadata",
            Self::LabelValues => "label_values",
            Self::LabelValueTimeRanges => "label_value_time_ranges",
            Self::Symbols => "symbols",
            Self::Series => "series",
            Self::Indexes => "indexes",
            Self::RoutingIndexBuild => "routing_index_build",
            Self::OooChunks => "ooo_chunks",
            Self::Footer => "footer",
            Self::Publish => "publish",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentFlushStage {
    pub kind: SegmentFlushStageKind,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentFlushFileSize {
    pub file: SegmentFile,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentRecordProfile {
    pub wall_elapsed: Duration,
    pub ensure_window: Duration,
    pub metadata: Duration,
    pub chunk_append: Duration,
    pub label_time_range: Duration,
    pub bookkeeping: Duration,
    pub chunks: u64,
    pub samples: u64,
}

impl SegmentRecordProfile {
    pub fn saturating_sub(self, baseline: Self) -> Self {
        Self {
            wall_elapsed: self.wall_elapsed.saturating_sub(baseline.wall_elapsed),
            ensure_window: self.ensure_window.saturating_sub(baseline.ensure_window),
            metadata: self.metadata.saturating_sub(baseline.metadata),
            chunk_append: self.chunk_append.saturating_sub(baseline.chunk_append),
            label_time_range: self
                .label_time_range
                .saturating_sub(baseline.label_time_range),
            bookkeeping: self.bookkeeping.saturating_sub(baseline.bookkeeping),
            chunks: self.chunks.saturating_sub(baseline.chunks),
            samples: self.samples.saturating_sub(baseline.samples),
        }
    }

    pub fn total_elapsed(self) -> Duration {
        self.ensure_window
            .saturating_add(self.metadata)
            .saturating_add(self.chunk_append)
            .saturating_add(self.label_time_range)
            .saturating_add(self.bookkeeping)
    }

    fn add_chunk(&mut self, timing: SegmentRecordChunkTiming, samples: u64) {
        self.wall_elapsed = self.wall_elapsed.saturating_add(timing.wall_elapsed);
        self.ensure_window = self.ensure_window.saturating_add(timing.ensure_window);
        self.metadata = self.metadata.saturating_add(timing.metadata);
        self.chunk_append = self.chunk_append.saturating_add(timing.chunk_append);
        self.label_time_range = self
            .label_time_range
            .saturating_add(timing.label_time_range);
        self.bookkeeping = self.bookkeeping.saturating_add(timing.bookkeeping);
        self.chunks = self.chunks.saturating_add(1);
        self.samples = self.samples.saturating_add(samples);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SegmentRecordChunkTiming {
    wall_elapsed: Duration,
    ensure_window: Duration,
    metadata: Duration,
    chunk_append: Duration,
    label_time_range: Duration,
    bookkeeping: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFlushProfile {
    pub segment_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub datapoints: u64,
    pub series: u64,
    pub total: Duration,
    stages: Vec<SegmentFlushStage>,
    stage_kinds: Vec<SegmentFlushStageKind>,
    file_sizes: Vec<SegmentFlushFileSize>,
}

impl SegmentFlushProfile {
    fn new(segment_id: String, start_ms: u64, end_ms: u64, datapoints: u64, series: u64) -> Self {
        Self {
            segment_id,
            start_ms,
            end_ms,
            datapoints,
            series,
            total: Duration::ZERO,
            stages: Vec::new(),
            stage_kinds: Vec::new(),
            file_sizes: Vec::new(),
        }
    }

    fn push_stage(&mut self, kind: SegmentFlushStageKind, elapsed: Duration) {
        self.stages.push(SegmentFlushStage { kind, elapsed });
        self.stage_kinds.push(kind);
    }

    fn set_file_sizes(&mut self, file_sizes: Vec<SegmentFlushFileSize>) {
        self.file_sizes = file_sizes;
    }

    pub fn stages(&self) -> &[SegmentFlushStage] {
        &self.stages
    }

    pub fn stage_kinds(&self) -> &[SegmentFlushStageKind] {
        &self.stage_kinds
    }

    pub fn stage_elapsed(&self, kind: SegmentFlushStageKind) -> Option<Duration> {
        self.stages
            .iter()
            .find_map(|stage| (stage.kind == kind).then_some(stage.elapsed))
    }

    pub fn file_sizes(&self) -> &[SegmentFlushFileSize] {
        &self.file_sizes
    }

    pub fn file_size_bytes(&self, file: SegmentFile) -> Option<u64> {
        self.file_sizes
            .iter()
            .find_map(|size| (size.file == file).then_some(size.bytes))
    }

    pub fn total_file_bytes(&self) -> u64 {
        self.file_sizes.iter().map(|size| size.bytes).sum()
    }

    pub fn data_file_bytes(&self) -> u64 {
        self.file_size_bytes(SegmentFile::Chunks)
            .unwrap_or_default()
            + self
                .file_size_bytes(SegmentFile::OooChunks)
                .unwrap_or_default()
    }

    pub fn metadata_file_bytes(&self) -> u64 {
        self.file_size_bytes(SegmentFile::MetaJson)
            .unwrap_or_default()
            + self
                .file_size_bytes(SegmentFile::Symbols)
                .unwrap_or_default()
            + self
                .file_size_bytes(SegmentFile::Series)
                .unwrap_or_default()
    }

    pub fn index_file_bytes(&self) -> u64 {
        self.file_size_bytes(SegmentFile::ChunkIndex)
            .unwrap_or_default()
            + self
                .file_size_bytes(SegmentFile::Indexes)
                .unwrap_or_default()
    }

    pub fn footer_file_bytes(&self) -> u64 {
        self.file_size_bytes(SegmentFile::Footer)
            .unwrap_or_default()
    }

    fn stage_elapsed_ms(&self, kind: SegmentFlushStageKind) -> u64 {
        self.stage_elapsed(kind)
            .map(duration_ms_u64)
            .unwrap_or_default()
    }
}

struct ActiveSegment {
    id: SegmentId,
    start_ms: u64,
    end_ms: u64,
    datapoints: u64,
    series_map: HashMap<u32, u32>,
    metadata_present: Vec<bool>,
    symbols: SegmentSymbols,
    series_entries: Vec<SeriesEntry>,
    postings: ExactPostingsIndex,
    normalized_names: NormalizedNameCache,
    label_value_time_ranges: LabelValueTimeRangeIndex,
    metadata_hash_scratch: Vec<u8>,
    chunk_entries: Vec<Vec<ChunkIndexEntry>>,
    chunks: ChunkWriter,
    temp_dir: SegmentTempDir,
}

#[derive(Debug, Clone)]
pub struct SegmentSeriesMetadata {
    series_id: u64,
    labels: Vec<(String, String)>,
}

impl SegmentSeriesMetadata {
    pub fn series_id(&self) -> u64 {
        self.series_id
    }

    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }
}

pub struct SegmentSeriesMetadataBuilder {
    labels: BTreeMap<String, String>,
    metric_name_seen: bool,
}

impl SegmentSeriesMetadataBuilder {
    pub fn new() -> Self {
        let mut labels = BTreeMap::new();
        labels.insert(METRIC_NAME_LABEL.to_string(), String::new());
        Self {
            labels,
            metric_name_seen: false,
        }
    }

    pub fn push_label(&mut self, name: &str, value: &str) {
        if name == METRIC_NAME_LABEL {
            if !self.metric_name_seen {
                self.labels
                    .insert(METRIC_NAME_LABEL.to_string(), normalize_metric_name(value));
                self.metric_name_seen = true;
            }
        } else {
            self.labels
                .insert(normalize_label_name(name), value.to_string());
        }
    }

    pub fn finish(self) -> SegmentSeriesMetadata {
        let labels: Vec<_> = self.labels.into_iter().collect();
        let series_id = segment_series_id(&labels);
        SegmentSeriesMetadata { series_id, labels }
    }
}

fn encode_canonical_segment_labels(
    labels: Vec<(String, String)>,
    symbols: &mut SegmentSymbols,
    postings: &mut ExactPostingsIndex,
    local_ref: u32,
) -> SeriesEntry {
    encode_borrowed_canonical_segment_labels(
        labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        symbols,
        postings,
        local_ref,
    )
}

fn encode_borrowed_canonical_segment_labels<'a>(
    labels: impl IntoIterator<Item = (&'a str, &'a str)>,
    symbols: &mut SegmentSymbols,
    postings: &mut ExactPostingsIndex,
    local_ref: u32,
) -> SeriesEntry {
    let mut bytes = Vec::new();
    let mut encoded_labels = Vec::new();
    for (key, value) in labels {
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);

        let key_sym = symbols.intern(key);
        let value_sym = symbols.intern(value);
        postings.insert_monotonic(key_sym, value_sym, local_ref);
        encoded_labels.push((key_sym, value_sym));
    }

    SeriesEntry {
        series_id: xxhash64(&bytes),
        kind_mask: SERIES_KIND_FLOAT,
        labels: encoded_labels,
    }
}

impl Default for SegmentSeriesMetadataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SegmentWriter {
    config: SegmentWriterConfig,
    active: Option<ActiveSegment>,
    last_flush_profile: Option<SegmentFlushProfile>,
    record_profile: SegmentRecordProfile,
}

impl SegmentWriter {
    pub fn new(config: SegmentWriterConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.segments_dir)?;
        Ok(Self {
            config,
            active: None,
            last_flush_profile: None,
            record_profile: SegmentRecordProfile::default(),
        })
    }

    pub fn last_flush_profile(&self) -> Option<&SegmentFlushProfile> {
        self.last_flush_profile.as_ref()
    }

    pub fn record_profile(&self) -> SegmentRecordProfile {
        self.record_profile
    }

    pub fn reserve_window_series(
        &mut self,
        start_ms: u64,
        end_ms: u64,
        series: usize,
    ) -> io::Result<()> {
        self.ensure_active_window(start_ms, end_ms)?;
        let Some(active) = &mut self.active else {
            return Ok(());
        };
        let additional = series.saturating_sub(active.series_map.len());
        active.series_map.reserve(additional);
        active.metadata_present.reserve(additional);
        active.series_entries.reserve(additional);
        active.chunk_entries.reserve(additional);
        Ok(())
    }

    pub fn reserve_series_for_timestamp(
        &mut self,
        timestamp_ms: u64,
        series: usize,
    ) -> io::Result<()> {
        let duration_ms = self.segment_duration_ms()?;
        let (start_ms, end_ms) = segment_window(timestamp_ms, duration_ms);
        self.reserve_window_series(start_ms, end_ms, series)
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
        if samples.is_empty() {
            return Ok(());
        }
        let metadata = canonical_segment_metadata(labels);
        self.record_float_samples(series, Some(&metadata), samples, false)
    }

    pub fn record_samples_with_metadata(
        &mut self,
        series: SeriesRef,
        metadata: &SegmentSeriesMetadata,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples_with_metadata_source(
            series,
            samples,
            false,
            |active, local_ref| {
                apply_segment_metadata(active, local_ref, metadata);
            },
        )
    }

    pub fn record_samples_raw_with_metadata(
        &mut self,
        series: SeriesRef,
        metadata: &SegmentSeriesMetadata,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples_with_metadata_source(
            series,
            samples,
            true,
            |active, local_ref| {
                apply_segment_metadata(active, local_ref, metadata);
            },
        )
    }

    pub fn record_samples_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_with_label_visitor(series, samples, false, visit_labels)
    }

    pub fn record_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_ordered_with_label_visitor(series, samples, false, visit_labels)
    }

    pub fn record_samples_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_float_samples_ordered_with_flat_interned_labels(
            series, samples, false, labelsets,
        )
    }

    pub fn record_samples_raw_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_with_label_visitor(series, samples, true, visit_labels)
    }

    pub fn record_samples_raw_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_ordered_with_label_visitor(series, samples, true, visit_labels)
    }

    pub fn record_samples_raw_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_float_samples_ordered_with_flat_interned_labels(
            series, samples, true, labelsets,
        )
    }

    pub fn record_histogram_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, HistogramValue)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_typed_samples_ordered_with_label_visitor(
            series,
            samples,
            SERIES_KIND_HISTOGRAM,
            ChunkWriter::append_histogram_chunk_ordered,
            visit_labels,
        )
    }

    pub fn record_histogram_samples_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, HistogramValue)],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_typed_samples_ordered_with_flat_interned_labels(
            series,
            samples,
            SERIES_KIND_HISTOGRAM,
            ChunkWriter::append_histogram_chunk_ordered,
            labelsets,
        )
    }

    pub fn record_exponential_histogram_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, ExponentialHistogramValue)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_typed_samples_ordered_with_label_visitor(
            series,
            samples,
            SERIES_KIND_EXPONENTIAL_HISTOGRAM,
            ChunkWriter::append_exponential_histogram_chunk_ordered,
            visit_labels,
        )
    }

    pub fn record_exponential_histogram_samples_ordered_with_flat_interned_labels<
        S: SymbolTable,
    >(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, ExponentialHistogramValue)],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_typed_samples_ordered_with_flat_interned_labels(
            series,
            samples,
            SERIES_KIND_EXPONENTIAL_HISTOGRAM,
            ChunkWriter::append_exponential_histogram_chunk_ordered,
            labelsets,
        )
    }

    pub fn record_summary_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, SummaryValue)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_typed_samples_ordered_with_label_visitor(
            series,
            samples,
            SERIES_KIND_SUMMARY,
            ChunkWriter::append_summary_chunk_ordered,
            visit_labels,
        )
    }

    pub fn record_summary_samples_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, SummaryValue)],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_typed_samples_ordered_with_flat_interned_labels(
            series,
            samples,
            SERIES_KIND_SUMMARY,
            ChunkWriter::append_summary_chunk_ordered,
            labelsets,
        )
    }

    fn record_float_samples_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        mut visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_with_metadata_source(series, samples, raw, |active, local_ref| {
            apply_label_visitor(active, local_ref, &mut visit_labels);
        })
    }

    fn record_float_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        mut visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_ordered_with_metadata_source(
            series,
            samples,
            raw,
            |active, local_ref| {
                apply_label_visitor(active, local_ref, &mut visit_labels);
            },
        )
    }

    fn record_float_samples_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_float_samples_ordered_with_metadata_source(
            series,
            samples,
            raw,
            |active, local_ref| {
                apply_flat_interned_label_metadata(
                    active,
                    local_ref,
                    SERIES_KIND_FLOAT,
                    series,
                    labelsets,
                );
            },
        )
    }

    fn record_typed_samples_ordered_with_label_visitor<T, F, A>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, T)],
        kind_mask: u8,
        append_chunk: A,
        mut visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
        A: Fn(&mut ChunkWriter, u32, &[(u64, T)]) -> io::Result<ChunkIndexEntry>,
    {
        self.record_typed_samples_ordered_with_metadata_source(
            series,
            samples,
            kind_mask,
            append_chunk,
            |active, local_ref| {
                apply_label_visitor_with_kind(active, local_ref, kind_mask, &mut visit_labels);
            },
        )
    }

    fn record_typed_samples_ordered_with_flat_interned_labels<T, S, A>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, T)],
        kind_mask: u8,
        append_chunk: A,
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()>
    where
        S: SymbolTable,
        A: Fn(&mut ChunkWriter, u32, &[(u64, T)]) -> io::Result<ChunkIndexEntry>,
    {
        self.record_typed_samples_ordered_with_metadata_source(
            series,
            samples,
            kind_mask,
            append_chunk,
            |active, local_ref| {
                apply_flat_interned_label_metadata(active, local_ref, kind_mask, series, labelsets);
            },
        )
    }

    fn record_float_samples(
        &mut self,
        series: SeriesRef,
        metadata: Option<&SegmentSeriesMetadata>,
        samples: &[(u64, f64)],
        raw: bool,
    ) -> io::Result<()> {
        self.record_float_samples_with_metadata_source(series, samples, raw, |active, local_ref| {
            if let Some(metadata) = metadata {
                apply_segment_metadata(active, local_ref, metadata);
            }
        })
    }

    fn record_float_samples_with_metadata_source<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        apply_metadata: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut ActiveSegment, u32),
    {
        if samples.is_empty() {
            return Ok(());
        }

        let mut ordered: Vec<(u64, f64)> = samples.to_vec();
        ordered.sort_by_key(|(ts, _)| *ts);
        self.record_float_samples_ordered_with_metadata_source(
            series,
            &ordered,
            raw,
            apply_metadata,
        )
    }

    fn record_float_samples_ordered_with_metadata_source<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        mut apply_metadata: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut ActiveSegment, u32),
    {
        if samples.is_empty() {
            return Ok(());
        }
        validate_ordered_samples(samples)?;

        let duration_ms = self.segment_duration_ms()?;
        let mut idx = 0usize;
        while idx < samples.len() {
            let ts = samples[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < samples.len() {
                let next_start = segment_window(samples[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_FLOAT);
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref);
            let metadata = metadata_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = if raw {
                active
                    .chunks
                    .append_float_chunk_raw_ordered(local_ref, &samples[idx..end_idx])?
            } else {
                active
                    .chunks
                    .append_float_chunk_ordered(local_ref, &samples[idx..end_idx])?
            };
            let chunk_append = chunk_append_start.elapsed();

            let label_time_range_start = Instant::now();
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
            let label_time_range = label_time_range_start.elapsed();

            let bookkeeping_start = Instant::now();
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata,
                    chunk_append,
                    label_time_range,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
            idx = end_idx;
        }

        Ok(())
    }

    fn record_typed_samples_ordered_with_metadata_source<T, F, A>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, T)],
        kind_mask: u8,
        append_chunk: A,
        mut apply_metadata: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut ActiveSegment, u32),
        A: Fn(&mut ChunkWriter, u32, &[(u64, T)]) -> io::Result<ChunkIndexEntry>,
    {
        if samples.is_empty() {
            return Ok(());
        }
        validate_ordered_samples(samples)?;

        let duration_ms = self.segment_duration_ms()?;
        let mut idx = 0usize;
        while idx < samples.len() {
            let ts = samples[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < samples.len() {
                let next_start = segment_window(samples[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, kind_mask);
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref);
            active.series_entries[local_ref as usize].kind_mask |= kind_mask;
            let metadata = metadata_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = append_chunk(&mut active.chunks, local_ref, &samples[idx..end_idx])?;
            let chunk_append = chunk_append_start.elapsed();

            let label_time_range_start = Instant::now();
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
            let label_time_range = label_time_range_start.elapsed();

            let bookkeeping_start = Instant::now();
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata,
                    chunk_append,
                    label_time_range,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
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

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64);
            let ensure_window = ensure_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = active
                .chunks
                .append_int_chunk_ordered(local_ref, &ordered[idx..end_idx])?;
            let chunk_append = chunk_append_start.elapsed();

            let label_time_range_start = Instant::now();
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
            let label_time_range = label_time_range_start.elapsed();

            let bookkeeping_start = Instant::now();
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata: Duration::ZERO,
                    chunk_append,
                    label_time_range,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
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

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64);
            let ensure_window = ensure_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = active
                .chunks
                .append_int_chunk_raw_ordered(local_ref, &ordered[idx..end_idx])?;
            let chunk_append = chunk_append_start.elapsed();

            let label_time_range_start = Instant::now();
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
            let label_time_range = label_time_range_start.elapsed();

            let bookkeeping_start = Instant::now();
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata: Duration::ZERO,
                    chunk_append,
                    label_time_range,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
            idx = end_idx;
        }

        Ok(())
    }

    // Writes metadata, chunk index, and placeholder files for non-chunk artifacts.
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };

        let total_start = Instant::now();
        let segment_id = active.id;
        let start_ms = active.start_ms;
        let end_ms = active.end_ms;
        let datapoints = active.datapoints;
        let series = active.series_map.len() as u64;
        let chunk_summary = SegmentChunkSummary::from_chunk_entries(&active.chunk_entries);
        let tmp = active.temp_dir;
        let mut profile =
            SegmentFlushProfile::new(segment_id.dir_name(), start_ms, end_ms, datapoints, series);

        let meta = SegmentMeta {
            segment_id: segment_id.dir_name(),
            start_ms,
            end_ms,
            datapoints,
            series,
            chunk_summary: Some(chunk_summary),
        };
        time_flush_stage(&mut profile, SegmentFlushStageKind::MetaJson, || {
            let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(io::Error::other)?;
            fs::write(tmp.file_path(SegmentFile::MetaJson), meta_bytes)
        })?;

        let mut chunks = active.chunks;
        time_flush_stage(&mut profile, SegmentFlushStageKind::ChunksFlush, || {
            chunks.flush()
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::ChunkIndex, || {
            let mut chunk_index = File::create(tmp.file_path(SegmentFile::ChunkIndex))?;
            write_chunk_index(&mut chunk_index, &active.chunk_entries)?;
            chunk_index.flush()
        })?;

        let (symbols, series_entries, postings) =
            time_flush_stage(&mut profile, SegmentFlushStageKind::SegmentMetadata, || {
                Ok((active.symbols, active.series_entries, active.postings))
            })?;
        let label_values =
            time_flush_stage(&mut profile, SegmentFlushStageKind::LabelValues, || {
                LabelValueFstIndex::from_series(&series_entries, &symbols)
            })?;
        let label_value_time_ranges = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::LabelValueTimeRanges,
            || Ok(active.label_value_time_ranges),
        )?;
        let routing_index = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::RoutingIndexBuild,
            || SegmentRoutingIndex::from_indexes(&symbols, &postings, &label_value_time_ranges),
        )?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Symbols, || {
            let mut symbols_file = File::create(tmp.file_path(SegmentFile::Symbols))?;
            write_symbols_bin(&mut symbols_file, &symbols)?;
            symbols_file.flush()
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Series, || {
            let mut series_file = File::create(tmp.file_path(SegmentFile::Series))?;
            write_series_bin(&mut series_file, &series_entries)?;
            series_file.flush()
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Indexes, || {
            let mut index_file = File::create(tmp.file_path(SegmentFile::Indexes))?;
            write_segment_indexes(
                &mut index_file,
                &SegmentIndexes {
                    exact_postings: postings,
                    label_values,
                    label_value_time_ranges,
                    routing_index: Some(routing_index),
                },
            )?;
            index_file.flush()
        })?;
        time_flush_stage(&mut profile, SegmentFlushStageKind::OooChunks, || {
            File::create(tmp.file_path(SegmentFile::OooChunks)).map(|_| ())
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Footer, || {
            write_segment_footer(tmp.path())
        })?;
        profile.set_file_sizes(collect_segment_file_sizes(tmp.path())?);
        let published_dir = time_flush_stage(&mut profile, SegmentFlushStageKind::Publish, || {
            tmp.publish()
        })?;
        profile.total = total_start.elapsed();
        let duration = Duration::from_millis(end_ms - start_ms);
        info!(
            segment_id = %segment_id,
            start_ms,
            end_ms,
            duration=?duration,
            datapoints,
            series,
            elapsed_ms = duration_ms_u64(profile.total),
            meta_json_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::MetaJson),
            chunks_flush_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::ChunksFlush),
            chunk_index_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::ChunkIndex),
            segment_metadata_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::SegmentMetadata),
            label_values_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::LabelValues),
            label_value_time_ranges_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::LabelValueTimeRanges),
            symbols_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Symbols),
            series_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Series),
            indexes_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Indexes),
            routing_index_build_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::RoutingIndexBuild),
            ooo_chunks_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::OooChunks),
            footer_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Footer),
            publish_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Publish),
            total_bytes = profile.total_file_bytes(),
            data_bytes = profile.data_file_bytes(),
            metadata_bytes = profile.metadata_file_bytes(),
            index_bytes = profile.index_file_bytes(),
            footer_bytes = profile.footer_file_bytes(),
            meta_json_bytes = profile.file_size_bytes(SegmentFile::MetaJson).unwrap_or_default(),
            symbols_bytes = profile.file_size_bytes(SegmentFile::Symbols).unwrap_or_default(),
            series_bytes = profile.file_size_bytes(SegmentFile::Series).unwrap_or_default(),
            chunks_bytes = profile.file_size_bytes(SegmentFile::Chunks).unwrap_or_default(),
            ooo_chunks_bytes = profile.file_size_bytes(SegmentFile::OooChunks).unwrap_or_default(),
            chunk_index_bytes = profile.file_size_bytes(SegmentFile::ChunkIndex).unwrap_or_default(),
            indexes_bytes = profile.file_size_bytes(SegmentFile::Indexes).unwrap_or_default(),
            footer_file_bytes = profile.file_size_bytes(SegmentFile::Footer).unwrap_or_default(),
            path = %published_dir.display(),
            "Segment published"
        );
        self.last_flush_profile = Some(profile);
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
            let id = self
                .config
                .segment_id_provider
                .next_segment_id(start_ms, end_ms)
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
                metadata_present: Vec::new(),
                symbols: SegmentSymbols::default(),
                series_entries: Vec::new(),
                postings: ExactPostingsIndex::default(),
                normalized_names: NormalizedNameCache::default(),
                label_value_time_ranges: LabelValueTimeRangeIndex::default(),
                metadata_hash_scratch: Vec::new(),
                chunk_entries: Vec::new(),
                chunks,
                temp_dir,
            });
        }

        Ok(())
    }
}

fn ensure_local_series_with_kind(
    active: &mut ActiveSegment,
    series: SeriesRef,
    kind_mask: u8,
) -> u32 {
    let source_ref = series.get();
    match active.series_map.get(&source_ref) {
        Some(&id) => {
            active.series_entries[id as usize].kind_mask |= kind_mask;
            id
        }
        None => {
            let id = active.series_map.len() as u32;
            active.series_map.insert(source_ref, id);
            active.metadata_present.push(false);
            active.series_entries.push(SeriesEntry {
                series_id: u64::from(source_ref),
                kind_mask,
                labels: Vec::new(),
            });
            active.chunk_entries.push(Vec::new());
            id
        }
    }
}

fn validate_ordered_samples<T>(samples: &[(u64, T)]) -> io::Result<()> {
    if samples.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ordered samples must be sorted by timestamp",
        ));
    }
    Ok(())
}

fn time_flush_stage<T>(
    profile: &mut SegmentFlushProfile,
    kind: SegmentFlushStageKind,
    f: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let started = Instant::now();
    let result = f();
    profile.push_stage(kind, started.elapsed());
    result
}

fn collect_segment_file_sizes(segment_dir: &Path) -> io::Result<Vec<SegmentFlushFileSize>> {
    SEGMENT_FLUSH_SIZE_FILES
        .into_iter()
        .map(|file| {
            fs::metadata(segment_dir.join(file.filename())).map(|metadata| SegmentFlushFileSize {
                file,
                bytes: metadata.len(),
            })
        })
        .collect()
}

fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn deterministic_segment_ulid(seed: u64, start_ms: u64, end_ms: u64, ordinal: u64) -> Ulid {
    let mut bytes = Vec::with_capacity(56);
    bytes.extend_from_slice(b"chronoxide-segment-id-v1");
    bytes.extend_from_slice(&seed.to_le_bytes());
    bytes.extend_from_slice(&start_ms.to_le_bytes());
    bytes.extend_from_slice(&end_ms.to_le_bytes());
    bytes.extend_from_slice(&ordinal.to_le_bytes());

    let high = xxhash64(&bytes);
    bytes.extend_from_slice(&high.to_le_bytes());
    let low = xxhash64(&bytes);
    let random = (((high as u128) & 0xffff) << 64) | low as u128;
    Ulid::from_parts(start_ms, random)
}

fn canonical_segment_metadata(labels: &[(String, String)]) -> SegmentSeriesMetadata {
    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in labels {
        builder.push_label(key, value);
    }
    builder.finish()
}

fn apply_segment_metadata(
    active: &mut ActiveSegment,
    local_ref: u32,
    metadata: &SegmentSeriesMetadata,
) {
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        return;
    }

    let mut encoded_labels = Vec::with_capacity(metadata.labels.len());
    for (key, value) in &metadata.labels {
        let key_sym = active.symbols.intern(key);
        let value_sym = active.symbols.intern(value);
        active
            .postings
            .insert_monotonic(key_sym, value_sym, local_ref);
        encoded_labels.push((key_sym, value_sym));
    }

    active.series_entries[idx] = SeriesEntry {
        series_id: metadata.series_id,
        kind_mask: SERIES_KIND_FLOAT,
        labels: encoded_labels,
    };
    active.metadata_present[idx] = true;
}

fn apply_label_visitor<F>(active: &mut ActiveSegment, local_ref: u32, visit_labels: &mut F)
where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    apply_label_visitor_with_kind(active, local_ref, SERIES_KIND_FLOAT, visit_labels);
}

fn apply_label_visitor_with_kind<F>(
    active: &mut ActiveSegment,
    local_ref: u32,
    kind_mask: u8,
    visit_labels: &mut F,
) where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        active.series_entries[idx].kind_mask |= kind_mask;
        return;
    }

    let mut entry = encode_label_visitor_metadata(
        &mut active.symbols,
        &mut active.postings,
        local_ref,
        |visit| {
            visit_labels(visit);
        },
    );
    entry.kind_mask = kind_mask;
    active.series_entries[idx] = entry;
    active.metadata_present[idx] = true;
}

fn apply_flat_interned_label_metadata<S: SymbolTable>(
    active: &mut ActiveSegment,
    local_ref: u32,
    kind_mask: u8,
    source_series: SeriesRef,
    labelsets: &FlatInternedLabelSetStore<S>,
) {
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        active.series_entries[idx].kind_mask |= kind_mask;
        return;
    }

    let mut entry = encode_flat_interned_label_metadata(
        &mut active.symbols,
        &mut active.postings,
        &mut active.normalized_names,
        &mut active.metadata_hash_scratch,
        local_ref,
        labelsets,
        source_series,
    );
    entry.kind_mask = kind_mask;
    active.series_entries[idx] = entry;
    active.metadata_present[idx] = true;
}

enum SourceLabelValue {
    Symbol(SymbolId),
    Owned(Arc<str>),
}

const MAX_NORMALIZED_NAME_CACHE_ENTRIES: usize = 262_144;

struct NormalizedNameCache {
    metric_label_name: Arc<str>,
    label_names: HashMap<SymbolId, Arc<str>>,
    metric_names: HashMap<SymbolId, Arc<str>>,
    max_entries: usize,
}

impl Default for NormalizedNameCache {
    fn default() -> Self {
        Self::with_max_entries(MAX_NORMALIZED_NAME_CACHE_ENTRIES)
    }
}

impl NormalizedNameCache {
    fn with_max_entries(max_entries: usize) -> Self {
        Self {
            metric_label_name: Arc::from(METRIC_NAME_LABEL),
            label_names: HashMap::new(),
            metric_names: HashMap::new(),
            max_entries,
        }
    }

    fn metric_label_name(&self) -> Arc<str> {
        Arc::clone(&self.metric_label_name)
    }

    fn label_name(
        &mut self,
        source_id: SymbolId,
        source_name: &str,
        normalize: impl FnOnce(&str) -> String,
    ) -> Arc<str> {
        if let Some(name) = self.label_names.get(&source_id) {
            return Arc::clone(name);
        }

        let name = Arc::from(normalize(source_name));
        if self.label_names.len() < self.max_entries {
            self.label_names.insert(source_id, Arc::clone(&name));
        }
        name
    }

    fn metric_name(
        &mut self,
        source_id: SymbolId,
        source_name: &str,
        normalize: impl FnOnce(&str) -> String,
    ) -> Arc<str> {
        if let Some(name) = self.metric_names.get(&source_id) {
            return Arc::clone(name);
        }

        let name = Arc::from(normalize(source_name));
        if self.metric_names.len() < self.max_entries {
            self.metric_names.insert(source_id, Arc::clone(&name));
        }
        name
    }
}

fn encode_flat_interned_label_metadata<S: SymbolTable>(
    symbols: &mut SegmentSymbols,
    postings: &mut ExactPostingsIndex,
    normalized_names: &mut NormalizedNameCache,
    hash_scratch: &mut Vec<u8>,
    local_ref: u32,
    labelsets: &FlatInternedLabelSetStore<S>,
    source_series: SeriesRef,
) -> SeriesEntry {
    let source_symbols = labelsets.symbols();
    let mut labels = Vec::new();
    let mut metric_name = None;
    let mut metric_name_seen = false;

    labelsets.visit_labelset_symbol_ids(source_series, |key_id, value_id| {
        let name = source_symbols.resolve(key_id);
        if name == METRIC_NAME_LABEL {
            if !metric_name_seen {
                metric_name = Some(normalized_names.metric_name(
                    value_id,
                    source_symbols.resolve(value_id),
                    normalize_metric_name,
                ));
                metric_name_seen = true;
            }
        } else {
            labels.push((
                normalized_names.label_name(key_id, name, normalize_label_name),
                SourceLabelValue::Symbol(value_id),
            ));
        }
    });

    labels.push((
        normalized_names.metric_label_name(),
        SourceLabelValue::Owned(metric_name.unwrap_or_else(|| Arc::from(""))),
    ));
    labels.sort_by(|left, right| left.0.as_ref().cmp(right.0.as_ref()));

    let mut canonical = Vec::with_capacity(labels.len());
    for (key, value) in labels {
        if let Some((last_key, last_value)) = canonical.last_mut()
            && last_key == &key
        {
            *last_value = value;
            continue;
        }
        canonical.push((key, value));
    }

    encode_flat_interned_canonical_labels(
        canonical,
        source_symbols,
        symbols,
        postings,
        hash_scratch,
        local_ref,
    )
}

fn encode_flat_interned_canonical_labels<S: SymbolTable>(
    labels: Vec<(Arc<str>, SourceLabelValue)>,
    source_symbols: &S,
    symbols: &mut SegmentSymbols,
    postings: &mut ExactPostingsIndex,
    hash_scratch: &mut Vec<u8>,
    local_ref: u32,
) -> SeriesEntry {
    hash_scratch.clear();
    let mut encoded_labels = Vec::with_capacity(labels.len());

    for (key, value) in labels {
        let value = match &value {
            SourceLabelValue::Symbol(id) => source_symbols.resolve(*id),
            SourceLabelValue::Owned(value) => value.as_ref(),
        };

        hash_scratch.extend_from_slice(key.as_ref().as_bytes());
        hash_scratch.push(0);
        hash_scratch.extend_from_slice(value.as_bytes());
        hash_scratch.push(0xff);

        let key_sym = symbols.intern(key.as_ref());
        let value_sym = symbols.intern(value);
        postings.insert_monotonic(key_sym, value_sym, local_ref);
        encoded_labels.push((key_sym, value_sym));
    }

    let series_id = xxhash64(hash_scratch);
    hash_scratch.clear();

    SeriesEntry {
        series_id,
        kind_mask: SERIES_KIND_FLOAT,
        labels: encoded_labels,
    }
}

fn encode_label_visitor_metadata<F>(
    symbols: &mut SegmentSymbols,
    postings: &mut ExactPostingsIndex,
    local_ref: u32,
    mut visit_labels: F,
) -> SeriesEntry
where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    let mut labels = Vec::new();
    let mut metric_name = String::new();
    let mut metric_name_seen = false;
    let mut push_label = |name: &str, value: &str| {
        if name == METRIC_NAME_LABEL {
            if !metric_name_seen {
                metric_name = normalize_metric_name(value);
                metric_name_seen = true;
            }
        } else {
            labels.push((normalize_label_name(name), value.to_string()));
        }
    };
    visit_labels(&mut push_label);

    labels.push((METRIC_NAME_LABEL.to_string(), metric_name));
    labels.sort_by(|left, right| left.0.cmp(&right.0));

    let mut canonical = Vec::with_capacity(labels.len());
    for (key, value) in labels {
        if let Some((last_key, last_value)) = canonical.last_mut()
            && last_key == &key
        {
            *last_value = value;
            continue;
        }
        canonical.push((key, value));
    }

    encode_canonical_segment_labels(canonical, symbols, postings, local_ref)
}

fn update_label_value_time_ranges(
    index: &mut LabelValueTimeRangeIndex,
    entry: &SeriesEntry,
    chunk: &ChunkIndexEntry,
) {
    index.insert_many(&entry.labels, chunk.min_time_ms, chunk.max_time_ms);
}

pub(crate) fn segment_series_id(labels: &[(String, String)]) -> u64 {
    let mut bytes = Vec::new();
    for (name, value) in labels {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);
    }
    xxhash64(&bytes)
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
    pub counter_reset_hints: Vec<CounterResetHint>,
}

impl SegmentQueryResult {
    pub(crate) fn new(series_id: u64, labels: Vec<(String, String)>) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
            counter_reset_hints: Vec::new(),
        }
    }

    pub(crate) fn with_samples(
        series_id: u64,
        labels: Vec<(String, String)>,
        samples: Vec<(u64, f64)>,
    ) -> Self {
        Self {
            series_id,
            labels,
            samples,
            counter_reset_hints: Vec::new(),
        }
    }

    pub(crate) fn push_sample(&mut self, timestamp_ms: u64, value: f64) {
        if self.has_counter_reset_hints() {
            self.counter_reset_hints.push(CounterResetHint::Unknown);
        } else {
            self.counter_reset_hints.clear();
        }
        self.samples.push((timestamp_ms, value));
    }

    pub(crate) fn push_sample_with_counter_reset_hint(
        &mut self,
        timestamp_ms: u64,
        value: f64,
        reset_hint: CounterResetHint,
    ) {
        self.ensure_counter_reset_hints();
        self.samples.push((timestamp_ms, value));
        self.counter_reset_hints.push(reset_hint);
    }

    pub(crate) fn extend_from(&mut self, mut other: SegmentQueryResult) {
        if other.has_counter_reset_hints() {
            self.ensure_counter_reset_hints();
            self.counter_reset_hints
                .append(&mut other.counter_reset_hints);
        } else if self.has_counter_reset_hints() {
            self.counter_reset_hints.extend(std::iter::repeat_n(
                CounterResetHint::Unknown,
                other.samples.len(),
            ));
        } else {
            self.counter_reset_hints.clear();
        }
        self.samples.append(&mut other.samples);
    }

    pub(crate) fn dedupe_samples_keep_last(&mut self) {
        let has_hints = self.has_counter_reset_hints();
        let samples = std::mem::take(&mut self.samples);
        let hints = if has_hints {
            Some(std::mem::take(&mut self.counter_reset_hints))
        } else {
            self.counter_reset_hints.clear();
            None
        };
        let mut by_timestamp = BTreeMap::<u64, (f64, Option<CounterResetHint>)>::new();
        for (idx, (timestamp_ms, value)) in samples.into_iter().enumerate() {
            let reset_hint = hints.as_ref().map(|values| values[idx]);
            by_timestamp.insert(timestamp_ms, (value, reset_hint));
        }

        let mut saw_hint = false;
        for (timestamp_ms, (value, reset_hint)) in by_timestamp {
            self.samples.push((timestamp_ms, value));
            if let Some(reset_hint) = reset_hint {
                saw_hint = true;
                self.counter_reset_hints.push(reset_hint);
            } else if saw_hint {
                self.counter_reset_hints.push(CounterResetHint::Unknown);
            }
        }
        if !saw_hint {
            self.counter_reset_hints.clear();
        }
    }

    pub(crate) fn counter_reset_hints(&self) -> Option<&[CounterResetHint]> {
        self.has_counter_reset_hints()
            .then_some(self.counter_reset_hints.as_slice())
    }

    fn ensure_counter_reset_hints(&mut self) {
        if !self.has_counter_reset_hints() {
            self.counter_reset_hints = vec![CounterResetHint::Unknown; self.samples.len()];
        }
    }

    fn has_counter_reset_hints(&self) -> bool {
        !self.counter_reset_hints.is_empty() && self.counter_reset_hints.len() == self.samples.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryExecution {
    pub results: Vec<SegmentQueryResult>,
    pub stats: QueryStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStats {
    pub segments_considered: u64,
    pub segments_skipped_by_time: u64,
    pub segments_skipped_by_missing_equality: u64,
    pub segments_skipped_by_matcher_time_range: u64,
    pub segments_queried: u64,
    pub matched_series: u64,
    pub projected_series: u64,
    pub chunk_reads: u64,
    pub bytes_read: u64,
    pub samples_decoded: u64,
    pub typed_scalar_chunks_decoded: u64,
    pub typed_full_chunks_decoded: u64,
    pub regex_values_examined: u64,
    pub index_postings_reads: u64,
    pub index_postings_bytes_read: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_matched_series: Option<u64>,
    pub max_projected_series: Option<u64>,
    pub max_chunk_reads: Option<u64>,
    pub max_bytes_read: Option<u64>,
    pub max_samples_decoded: Option<u64>,
    pub max_regex_values_examined: Option<u64>,
}

pub const PRODUCTION_QUERY_MAX_SERIES_MATCHED: u64 = 1_000_000;
pub const PRODUCTION_QUERY_MAX_PROJECTED_SERIES: u64 = 2_000_000;
pub const PRODUCTION_QUERY_MAX_CHUNKS_READ: u64 = 5_000_000;
pub const PRODUCTION_QUERY_MAX_BYTES_READ: u64 = 2 * 1024 * 1024 * 1024;
pub const PRODUCTION_QUERY_MAX_SAMPLES: u64 = 50_000_000;
pub const PRODUCTION_REGEX_MAX_EXPANDED_VALUES: u64 = 100_000;

impl QueryLimits {
    pub const fn unlimited() -> Self {
        Self {
            max_matched_series: None,
            max_projected_series: None,
            max_chunk_reads: None,
            max_bytes_read: None,
            max_samples_decoded: None,
            max_regex_values_examined: None,
        }
    }

    pub const fn production_default() -> Self {
        Self {
            max_matched_series: Some(PRODUCTION_QUERY_MAX_SERIES_MATCHED),
            max_projected_series: Some(PRODUCTION_QUERY_MAX_PROJECTED_SERIES),
            max_chunk_reads: Some(PRODUCTION_QUERY_MAX_CHUNKS_READ),
            max_bytes_read: Some(PRODUCTION_QUERY_MAX_BYTES_READ),
            max_samples_decoded: Some(PRODUCTION_QUERY_MAX_SAMPLES),
            max_regex_values_examined: Some(PRODUCTION_REGEX_MAX_EXPANDED_VALUES),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeReport {
    pub totals: SegmentStoreSmokeTotals,
    pub sample_series: Vec<SegmentStoreSmokeSeries>,
    pub queries: Vec<SegmentStoreSmokeQuery>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeTotals {
    pub segments: u64,
    pub datapoints: u64,
    pub series: u64,
    pub chunks: u64,
    pub chunk_bytes: u64,
    pub by_kind: SegmentStoreSmokeKindTotals,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeKindTotals {
    pub float: SegmentStoreSmokeKindStats,
    pub int64: SegmentStoreSmokeKindStats,
    pub histogram: SegmentStoreSmokeKindStats,
    pub exponential_histogram: SegmentStoreSmokeKindStats,
    pub summary: SegmentStoreSmokeKindStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeKindStats {
    pub chunks: u64,
    pub chunk_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStoreSmokeSeries {
    pub segment_id: String,
    pub series_ref: u32,
    pub series_id: u64,
    pub kind: ChunkKind,
    pub labels: Vec<(String, String)>,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub samples: u64,
    pub chunk_bytes: u64,
    pub bucket_le: Option<String>,
    pub quantile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStoreSmokeQuery {
    pub kind: ChunkKind,
    pub query: String,
    pub result_series: u64,
    pub result_samples: u64,
    pub matched_series: u64,
    pub projected_series: u64,
    pub chunk_reads: u64,
    pub bytes_read: u64,
    pub samples_decoded: u64,
    pub typed_scalar_chunks_decoded: u64,
    pub typed_full_chunks_decoded: u64,
}

impl SegmentStoreSmokeKindTotals {
    fn add_chunk(&mut self, kind: ChunkKind, bytes: u64) {
        let stats = self.stats_mut(kind);
        stats.chunks = stats.chunks.saturating_add(1);
        stats.chunk_bytes = stats.chunk_bytes.saturating_add(bytes);
    }

    fn add_segment_stats(&mut self, kind: ChunkKind, stats: SegmentChunkKindStats) {
        let out = self.stats_mut(kind);
        out.chunks = out.chunks.saturating_add(stats.chunks);
        out.chunk_bytes = out.chunk_bytes.saturating_add(stats.chunk_bytes);
    }

    fn stats_mut(&mut self, kind: ChunkKind) -> &mut SegmentStoreSmokeKindStats {
        match kind {
            ChunkKind::Float => &mut self.float,
            ChunkKind::Int64 => &mut self.int64,
            ChunkKind::Histogram => &mut self.histogram,
            ChunkKind::ExponentialHistogram => &mut self.exponential_histogram,
            ChunkKind::Summary => &mut self.summary,
        }
    }
}

impl SegmentStoreSmokeTotals {
    fn add_chunk_summary(&mut self, summary: &SegmentChunkSummary) {
        self.chunks = self.chunks.saturating_add(summary.chunks);
        self.chunk_bytes = self.chunk_bytes.saturating_add(summary.chunk_bytes);
        for kind in [
            ChunkKind::Float,
            ChunkKind::Int64,
            ChunkKind::Histogram,
            ChunkKind::ExponentialHistogram,
            ChunkKind::Summary,
        ] {
            self.by_kind
                .add_segment_stats(kind, summary.by_kind.stats(kind));
        }
    }
}

impl SegmentStoreSmokeReport {
    fn sample_count_for_kind(&self, kind: ChunkKind) -> usize {
        self.sample_series
            .iter()
            .filter(|sample| sample.kind == kind)
            .count()
    }

    fn sample_limits_reached_for_summary(
        &self,
        summary: &SegmentChunkSummary,
        sample_limit_per_kind: usize,
    ) -> bool {
        if sample_limit_per_kind == 0 {
            return true;
        }
        [
            ChunkKind::Float,
            ChunkKind::Int64,
            ChunkKind::Histogram,
            ChunkKind::ExponentialHistogram,
            ChunkKind::Summary,
        ]
        .into_iter()
        .all(|kind| {
            summary.by_kind.stats(kind).chunks == 0
                || self.sample_count_for_kind(kind) >= sample_limit_per_kind
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryProjectionConfig {
    exponential_histogram_bucket_boundaries: Vec<f64>,
}

impl QueryProjectionConfig {
    pub fn with_exponential_histogram_bucket_boundaries(
        mut self,
        mut boundaries: Vec<f64>,
    ) -> Self {
        assert!(
            boundaries.iter().all(|boundary| boundary.is_finite()),
            "exponential histogram projection boundaries must be finite"
        );
        boundaries.sort_by(f64::total_cmp);
        boundaries.dedup_by(|left, right| left.to_bits() == right.to_bits());
        self.exponential_histogram_bucket_boundaries = boundaries;
        self
    }

    fn exponential_histogram_bucket_boundaries(&self) -> &[f64] {
        &self.exponential_histogram_bucket_boundaries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLimit {
    MatchedSeries,
    ProjectedSeries,
    ChunkReads,
    BytesRead,
    SamplesDecoded,
    RegexValuesExamined,
}

impl QueryLimit {
    fn as_str(self) -> &'static str {
        match self {
            Self::MatchedSeries => "matched_series",
            Self::ProjectedSeries => "projected_series",
            Self::ChunkReads => "chunk_reads",
            Self::BytesRead => "bytes_read",
            Self::SamplesDecoded => "samples_decoded",
            Self::RegexValuesExamined => "regex_values_examined",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimitExceeded {
    pub limit: QueryLimit,
    pub max: u64,
}

impl fmt::Display for QueryLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "query exceeded {} limit of {}",
            self.limit.as_str(),
            self.max
        )
    }
}

impl std::error::Error for QueryLimitExceeded {}

#[derive(Debug)]
pub(crate) struct QueryBudget {
    limits: QueryLimits,
    stats: QueryStats,
    seen_series: BTreeSet<u64>,
    seen_projected_series: BTreeSet<u64>,
}

impl QueryBudget {
    pub(crate) fn new(limits: QueryLimits) -> Self {
        Self {
            limits,
            stats: QueryStats::default(),
            seen_series: BTreeSet::new(),
            seen_projected_series: BTreeSet::new(),
        }
    }

    pub(crate) fn unlimited() -> Self {
        Self::new(QueryLimits::unlimited())
    }

    pub(crate) fn stats(&self) -> QueryStats {
        self.stats
    }

    pub(crate) fn observe_matched_series(&mut self, series_id: u64) -> io::Result<()> {
        if !self.seen_series.insert(series_id) {
            return Ok(());
        }
        self.stats.matched_series = self.checked_add(
            QueryLimit::MatchedSeries,
            self.stats.matched_series,
            1,
            self.limits.max_matched_series,
        )?;
        Ok(())
    }

    pub(crate) fn observe_projected_series(&mut self, series_id: u64) -> io::Result<()> {
        if !self.seen_projected_series.insert(series_id) {
            return Ok(());
        }
        self.stats.projected_series = self.checked_add(
            QueryLimit::ProjectedSeries,
            self.stats.projected_series,
            1,
            self.limits.max_projected_series,
        )?;
        Ok(())
    }

    pub(crate) fn observe_projected_results(
        &mut self,
        results: &[SegmentQueryResult],
    ) -> io::Result<()> {
        for result in results {
            self.observe_projected_series(result.series_id)?;
        }
        Ok(())
    }

    pub(crate) fn observe_candidate_series_refs(&mut self, count: u64) -> io::Result<()> {
        if let Some(max) = self.limits.max_matched_series
            && count > max
        {
            return Err(limit_exceeded_io(QueryLimitExceeded {
                limit: QueryLimit::MatchedSeries,
                max,
            }));
        }
        Ok(())
    }

    pub(crate) fn observe_chunk_read(&mut self, bytes: u64) -> io::Result<()> {
        self.stats.chunk_reads = self.checked_add(
            QueryLimit::ChunkReads,
            self.stats.chunk_reads,
            1,
            self.limits.max_chunk_reads,
        )?;
        self.stats.bytes_read = self.checked_add(
            QueryLimit::BytesRead,
            self.stats.bytes_read,
            bytes,
            self.limits.max_bytes_read,
        )?;
        Ok(())
    }

    pub(crate) fn observe_samples_decoded(&mut self, samples: u64) -> io::Result<()> {
        self.stats.samples_decoded = self.checked_add(
            QueryLimit::SamplesDecoded,
            self.stats.samples_decoded,
            samples,
            self.limits.max_samples_decoded,
        )?;
        Ok(())
    }

    pub(crate) fn observe_typed_scalar_chunk_decoded(&mut self) {
        self.stats.typed_scalar_chunks_decoded =
            self.stats.typed_scalar_chunks_decoded.saturating_add(1);
    }

    pub(crate) fn observe_typed_full_chunk_decoded(&mut self) {
        self.stats.typed_full_chunks_decoded =
            self.stats.typed_full_chunks_decoded.saturating_add(1);
    }

    pub(crate) fn observe_regex_value(&mut self) -> io::Result<()> {
        self.stats.regex_values_examined = self.checked_add(
            QueryLimit::RegexValuesExamined,
            self.stats.regex_values_examined,
            1,
            self.limits.max_regex_values_examined,
        )?;
        Ok(())
    }

    pub(crate) fn observe_index_postings_read(&mut self, bytes: u64) {
        self.stats.index_postings_reads = self.stats.index_postings_reads.saturating_add(1);
        self.stats.index_postings_bytes_read =
            self.stats.index_postings_bytes_read.saturating_add(bytes);
    }

    pub(crate) fn observe_segment_considered(&mut self) {
        self.stats.segments_considered = self.stats.segments_considered.saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_time(&mut self) {
        self.stats.segments_skipped_by_time = self.stats.segments_skipped_by_time.saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_missing_equality(&mut self) {
        self.stats.segments_skipped_by_missing_equality = self
            .stats
            .segments_skipped_by_missing_equality
            .saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_matcher_time_range(&mut self) {
        self.stats.segments_skipped_by_matcher_time_range = self
            .stats
            .segments_skipped_by_matcher_time_range
            .saturating_add(1);
    }

    pub(crate) fn observe_segment_queried(&mut self) {
        self.stats.segments_queried = self.stats.segments_queried.saturating_add(1);
    }

    fn checked_add(
        &self,
        limit: QueryLimit,
        current: u64,
        increment: u64,
        max: Option<u64>,
    ) -> io::Result<u64> {
        let next = current.saturating_add(increment);
        if let Some(max) = max
            && next > max
        {
            return Err(limit_exceeded_io(QueryLimitExceeded { limit, max }));
        }
        Ok(next)
    }
}

fn limit_exceeded_io(exceeded: QueryLimitExceeded) -> io::Error {
    io::Error::new(io::ErrorKind::QuotaExceeded, exceeded)
}

fn query_limit_exceeded_from_io(err: &io::Error) -> Option<&QueryLimitExceeded> {
    err.get_ref()?.downcast_ref::<QueryLimitExceeded>()
}

fn promql_error_from_query_io(err: io::Error) -> PromqlQueryError {
    if err.kind() == io::ErrorKind::QuotaExceeded
        && let Some(exceeded) = query_limit_exceeded_from_io(&err)
    {
        return PromqlQueryError::LimitExceeded {
            limit: exceeded.limit.as_str().to_string(),
            max: exceeded.max,
        };
    }

    PromqlQueryError::Storage(err.to_string())
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MetadataAccumulator {
    metric_names: BTreeSet<String>,
    label_names: BTreeSet<String>,
    label_values: BTreeMap<String, BTreeSet<String>>,
}

impl MetadataAccumulator {
    pub(crate) fn add_label_name(&mut self, name: String) {
        self.label_names.insert(name);
    }

    pub(crate) fn add_label_value(&mut self, name: String, value: String) {
        self.label_names.insert(name.clone());
        self.label_values
            .entry(name.clone())
            .or_default()
            .insert(value.clone());
        if name == METRIC_NAME_LABEL {
            self.metric_names.insert(value);
        }
    }

    pub(crate) fn add_labelset(&mut self, labels: &[(String, String)]) {
        for (name, value) in labels {
            self.add_label_value(name.clone(), value.clone());
        }
    }

    pub(crate) fn metric_names(&self) -> Vec<String> {
        self.metric_names.iter().cloned().collect()
    }

    pub(crate) fn label_names(&self) -> Vec<String> {
        self.label_names.iter().cloned().collect()
    }

    pub(crate) fn label_values(&self, label_name: &str) -> Vec<String> {
        self.label_values
            .get(label_name)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: String },
    NotRegex { name: String, pattern: String },
}

impl LabelMatcher {
    pub fn eq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Eq {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn not_eq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::NotEq {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::Regex {
            name: name.into(),
            pattern: pattern.into(),
        }
    }

    pub fn not_regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::NotRegex {
            name: name.into(),
            pattern: pattern.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SegmentSelector {
    metric_name: Option<String>,
    matchers: Vec<LabelMatcher>,
    projection: SegmentProjection,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum SegmentProjection {
    #[default]
    None,
    AllPromql {
        exponential_histogram_boundaries: Vec<f64>,
    },
    Count,
    Sum,
    HistogramBucket {
        le: Option<String>,
        exponential_histogram_boundaries: Vec<f64>,
    },
    SummaryQuantile {
        quantile: Option<String>,
    },
}

impl SegmentSelector {
    pub fn new(matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: None,
            matchers,
            projection: SegmentProjection::None,
        }
    }

    pub fn metric(metric_name: impl Into<String>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers: Vec::new(),
            projection: SegmentProjection::None,
        }
    }

    pub fn with_metric(metric_name: impl Into<String>, matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers,
            projection: SegmentProjection::None,
        }
    }

    fn with_projection(mut self, projection: SegmentProjection) -> Self {
        self.projection = projection;
        self
    }

    pub(crate) fn projection(&self) -> &SegmentProjection {
        &self.projection
    }

    pub(crate) fn normalized_matchers(&self) -> Vec<NormalizedMatcher> {
        let mut normalized = Vec::with_capacity(self.matchers.len() + 1);
        if let Some(metric_name) = &self.metric_name {
            normalized.push(NormalizedMatcher::Eq {
                name: METRIC_NAME_LABEL.to_string(),
                value: normalize_metric_name(metric_name),
            });
        }

        for matcher in &self.matchers {
            match matcher {
                LabelMatcher::Eq { name, value } => {
                    let (name, value) = normalize_matcher_name_value(name, value);
                    normalized.push(NormalizedMatcher::Eq { name, value });
                }
                LabelMatcher::NotEq { name, value } => {
                    let (name, value) = normalize_matcher_name_value(name, value);
                    normalized.push(NormalizedMatcher::NotEq { name, value });
                }
                LabelMatcher::Regex { name, pattern } => {
                    let name = normalize_matcher_name(name);
                    normalized.push(NormalizedMatcher::Regex {
                        name,
                        pattern: pattern.clone(),
                    });
                }
                LabelMatcher::NotRegex { name, pattern } => {
                    let name = normalize_matcher_name(name);
                    normalized.push(NormalizedMatcher::NotRegex {
                        name,
                        pattern: pattern.clone(),
                    });
                }
            }
        }

        normalized
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NormalizedMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: String },
    NotRegex { name: String, pattern: String },
}

pub(crate) enum CompiledLabelMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: regex::Regex },
    NotRegex { name: String, pattern: regex::Regex },
}

const PROMQL_PROJECTION_SUFFIXES: [&str; 3] = ["_bucket", "_count", "_sum"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResolvedEqualityMatcher {
    name_sym: u32,
    value_sym: u32,
    postings: ExactPostingsMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentPruneReason {
    MissingEquality,
    MatcherTimeRange,
}

pub struct SegmentStoreReader {
    segments: Vec<SegmentReader>,
    query_projection_config: QueryProjectionConfig,
}

pub struct SegmentStoreQuerySession<'a> {
    query_projection_config: QueryProjectionConfig,
    segments: Vec<SegmentQuerySessionReader<'a>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreQuerySessionStats {
    pub index_routing_opens: u64,
    pub segment_context_opens: u64,
    pub symbols_bin_opens: u64,
    pub indexes_puffin_opens: u64,
    pub series_bin_opens: u64,
    pub chunk_index_bin_opens: u64,
    pub chunks_bin_opens: u64,
}

impl SegmentStoreQuerySessionStats {
    fn add(&mut self, other: Self) {
        self.index_routing_opens = self
            .index_routing_opens
            .saturating_add(other.index_routing_opens);
        self.segment_context_opens = self
            .segment_context_opens
            .saturating_add(other.segment_context_opens);
        self.symbols_bin_opens = self
            .symbols_bin_opens
            .saturating_add(other.symbols_bin_opens);
        self.indexes_puffin_opens = self
            .indexes_puffin_opens
            .saturating_add(other.indexes_puffin_opens);
        self.series_bin_opens = self.series_bin_opens.saturating_add(other.series_bin_opens);
        self.chunk_index_bin_opens = self
            .chunk_index_bin_opens
            .saturating_add(other.chunk_index_bin_opens);
        self.chunks_bin_opens = self.chunks_bin_opens.saturating_add(other.chunks_bin_opens);
    }
}

struct SegmentQuerySessionReader<'a> {
    reader: &'a SegmentReader,
    context: Option<SegmentQueryContext>,
    index_routing_reader: Option<SegmentIndexReader<File>>,
    stats: SegmentStoreQuerySessionStats,
}

struct SegmentQueryContext {
    symbols: SegmentSymbols,
    index_reader: SegmentIndexReader<File>,
    series_reader: Option<SeriesReader<File>>,
    chunk_index_reader: Option<ChunkIndexReader>,
    chunk_file: Option<File>,
    stats: SegmentStoreQuerySessionStats,
}

impl SegmentQueryContext {
    fn open(
        reader: &SegmentReader,
        index_reader: Option<SegmentIndexReader<File>>,
    ) -> io::Result<Self> {
        let (index_reader, indexes_puffin_opens) = match index_reader {
            Some(index_reader) => (index_reader, 0),
            None => (
                SegmentIndexReader::open(File::open(reader.file_path(SegmentFile::Indexes))?)?,
                1,
            ),
        };
        Ok(Self {
            symbols: read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols))?)?,
            index_reader,
            series_reader: None,
            chunk_index_reader: None,
            chunk_file: None,
            stats: SegmentStoreQuerySessionStats {
                segment_context_opens: 1,
                symbols_bin_opens: 1,
                indexes_puffin_opens,
                ..SegmentStoreQuerySessionStats::default()
            },
        })
    }

    fn series_reader(&mut self, reader: &SegmentReader) -> io::Result<&mut SeriesReader<File>> {
        if self.series_reader.is_none() {
            self.series_reader = Some(SeriesReader::open(File::open(
                reader.file_path(SegmentFile::Series),
            )?)?);
            self.stats.series_bin_opens = self.stats.series_bin_opens.saturating_add(1);
        }
        Ok(self.series_reader.as_mut().unwrap())
    }

    fn chunk_index_reader(&mut self, reader: &SegmentReader) -> io::Result<&mut ChunkIndexReader> {
        if self.chunk_index_reader.is_none() {
            self.chunk_index_reader = Some(ChunkIndexReader::open(File::open(
                reader.file_path(SegmentFile::ChunkIndex),
            )?)?);
            self.stats.chunk_index_bin_opens = self.stats.chunk_index_bin_opens.saturating_add(1);
        }
        Ok(self.chunk_index_reader.as_mut().unwrap())
    }

    fn chunk_file(&mut self, reader: &SegmentReader) -> io::Result<&mut File> {
        if self.chunk_file.is_none() {
            self.chunk_file = Some(reader.open_chunks()?);
            self.stats.chunks_bin_opens = self.stats.chunks_bin_opens.saturating_add(1);
        }
        Ok(self.chunk_file.as_mut().unwrap())
    }
}

// Metadata-only segment pruning step. Keep this independent of postings/chunk decoding so
// future scan planners, including a DataFusion TableProvider, can reuse the same decision.
fn plan_positive_equality_matchers(
    context: &SegmentQueryContext,
    matchers: &[NormalizedMatcher],
    start_ms: u64,
    end_ms: u64,
) -> Result<Vec<ResolvedEqualityMatcher>, SegmentPruneReason> {
    let mut equality_matchers = Vec::new();
    for matcher in matchers {
        let NormalizedMatcher::Eq { name, value } = matcher else {
            continue;
        };
        let Some(name_sym) = context.symbols.lookup(name) else {
            return Err(SegmentPruneReason::MissingEquality);
        };
        let Some(value_sym) = context.symbols.lookup(value) else {
            return Err(SegmentPruneReason::MissingEquality);
        };
        let Some(postings) = context
            .index_reader
            .exact_postings_metadata(name_sym, value_sym)
        else {
            return Err(SegmentPruneReason::MissingEquality);
        };
        if !postings.time_range.overlaps(start_ms, end_ms) {
            return Err(SegmentPruneReason::MatcherTimeRange);
        }
        equality_matchers.push(ResolvedEqualityMatcher {
            name_sym,
            value_sym,
            postings,
        });
    }
    equality_matchers.sort_by_key(|matcher| matcher.postings.byte_len);
    Ok(equality_matchers)
}

fn has_positive_equality_matcher(matchers: &[NormalizedMatcher]) -> bool {
    matchers
        .iter()
        .any(|matcher| matches!(matcher, NormalizedMatcher::Eq { .. }))
}

fn plan_positive_equality_matchers_from_routing_index(
    index: &SegmentRoutingIndex,
    matchers: &[NormalizedMatcher],
    start_ms: u64,
    end_ms: u64,
) -> Result<(), SegmentPruneReason> {
    for matcher in matchers {
        let NormalizedMatcher::Eq { name, value } = matcher else {
            continue;
        };
        let Some(postings) = index.exact_postings_metadata(name, value) else {
            return Err(SegmentPruneReason::MissingEquality);
        };
        if !postings.time_range.overlaps(start_ms, end_ms) {
            return Err(SegmentPruneReason::MatcherTimeRange);
        }
    }
    Ok(())
}

impl<'a> SegmentQuerySessionReader<'a> {
    fn open(reader: &'a SegmentReader) -> Self {
        Self {
            reader,
            context: None,
            index_routing_reader: None,
            stats: SegmentStoreQuerySessionStats::default(),
        }
    }

    fn context(&mut self) -> io::Result<&mut SegmentQueryContext> {
        if self.context.is_none() {
            let index_reader = self.index_routing_reader.take();
            self.context = Some(SegmentQueryContext::open(self.reader, index_reader)?);
        }
        Ok(self.context.as_mut().unwrap())
    }

    fn index_reader_for_routing(&mut self) -> io::Result<&mut SegmentIndexReader<File>> {
        if self.index_routing_reader.is_none() {
            self.index_routing_reader = Some(SegmentIndexReader::open(File::open(
                self.reader.file_path(SegmentFile::Indexes),
            )?)?);
            self.stats.index_routing_opens = self.stats.index_routing_opens.saturating_add(1);
        }
        Ok(self.index_routing_reader.as_mut().unwrap())
    }

    fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            let routing_index = self.index_reader_for_routing()?.routing_index()?;
            if let Some(index) = routing_index {
                match plan_positive_equality_matchers_from_routing_index(
                    &index, &matchers, start_ms, end_ms,
                ) {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(Vec::new());
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(Vec::new());
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.query_normalized_with_context(
            context,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
        )
    }
}

impl<'a> SegmentStoreQuerySession<'a> {
    fn open(store: &'a SegmentStoreReader) -> io::Result<Self> {
        let mut segments = Vec::with_capacity(store.segments.len());
        for segment in &store.segments {
            segments.push(SegmentQuerySessionReader::open(segment));
        }
        Ok(Self {
            query_projection_config: store.query_projection_config.clone(),
            segments,
        })
    }

    pub fn query_selector(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_limits(selector, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_selector_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let results = self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
        Ok(QueryExecution {
            results,
            stats: budget.stats(),
        })
    }

    fn query_selectors_with_limits(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        for selector in selectors {
            results.extend(self.query_selector_with_budget(
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )?);
        }
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub fn stats(&self) -> SegmentStoreQuerySessionStats {
        let mut stats = SegmentStoreQuerySessionStats::default();
        for segment in &self.segments {
            stats.add(segment.stats);
            if let Some(context) = &segment.context {
                stats.add(context.stats);
            }
        }
        stats
    }

    pub fn query_promql(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, limits)
    }

    fn execute_promql_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramQuantile(function) => {
                let mut execution =
                    self.execute_promql_query(&function.input, start_ms, end_ms, limits)?;
                execution.results =
                    evaluate_histogram_quantile(function, execution.results, end_ms);
                Ok(execution)
            }
        }
    }

    fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for segment in &mut self.segments {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            results.extend(segment.query_selector_with_budget(selector, start_ms, end_ms, budget)?);
        }

        Ok(merge_query_results(results))
    }
}

fn histogram_projected_bucket_value(
    metadata: TypedSampleMetadata,
    raw: u64,
    le: &str,
    delta_accumulators: &mut BTreeMap<String, u64>,
) -> f64 {
    if metadata.is_stale() {
        return prometheus_stale_nan();
    }
    if metadata.temporality == OtlpAggregationTemporality::Delta {
        let accumulator = delta_accumulators.entry(le.to_string()).or_insert(0);
        *accumulator = accumulator.saturating_add(raw);
        *accumulator as f64
    } else {
        raw as f64
    }
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

        sort_segment_readers(&mut segments);

        Ok(Self {
            segments,
            query_projection_config: QueryProjectionConfig::default(),
        })
    }

    pub fn open_with_query_projection_config(
        segments_dir: impl AsRef<Path>,
        query_projection_config: QueryProjectionConfig,
    ) -> io::Result<Self> {
        Ok(Self::open(segments_dir)?.with_query_projection_config(query_projection_config))
    }

    pub fn with_query_projection_config(
        mut self,
        query_projection_config: QueryProjectionConfig,
    ) -> Self {
        self.query_projection_config = query_projection_config;
        self
    }

    pub fn query_session(&self) -> io::Result<SegmentStoreQuerySession<'_>> {
        SegmentStoreQuerySession::open(self)
    }

    pub fn smoke_verify(
        &self,
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
    ) -> io::Result<SegmentStoreSmokeReport> {
        if end_ms < start_ms {
            return Ok(SegmentStoreSmokeReport::default());
        }

        let mut report = SegmentStoreSmokeReport::default();
        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }

            report.totals.segments = report.totals.segments.saturating_add(1);
            report.totals.datapoints = report
                .totals
                .datapoints
                .saturating_add(segment.meta.datapoints);
            report.totals.series = report.totals.series.saturating_add(segment.meta.series);
            let summary_covers_requested_range =
                start_ms <= segment.meta.start_ms && segment.meta.end_ms <= end_ms;
            let collect_totals = if summary_covers_requested_range {
                if let Some(summary) = &segment.meta.chunk_summary {
                    report.totals.add_chunk_summary(summary);
                    false
                } else {
                    true
                }
            } else {
                true
            };
            segment.collect_smoke_report(
                start_ms,
                end_ms,
                sample_limit_per_kind,
                collect_totals,
                &mut report,
            )?;
        }

        let queries = report
            .sample_series
            .iter()
            .flat_map(|sample| smoke_queries_for_sample(sample, start_ms, end_ms))
            .collect::<Vec<_>>();
        if queries.is_empty() {
            return Ok(report);
        }
        let mut query_session = self.query_session()?;
        for (kind, query, query_start_ms, query_end_ms) in queries {
            let execution = query_session
                .query_promql_with_limits(
                    &query,
                    query_start_ms,
                    query_end_ms,
                    smoke_query_limits(),
                )
                .map_err(|err| smoke_query_error(&query, err))?;
            let result_series = execution.results.len() as u64;
            let result_samples = execution
                .results
                .iter()
                .map(|result| result.samples.len() as u64)
                .sum::<u64>();
            if result_samples == 0 {
                return Err(io::Error::other(format!(
                    "smoke query returned no samples: {query}"
                )));
            }
            report.queries.push(SegmentStoreSmokeQuery {
                kind,
                query,
                result_series,
                result_samples,
                matched_series: execution.stats.matched_series,
                projected_series: execution.stats.projected_series,
                chunk_reads: execution.stats.chunk_reads,
                bytes_read: execution.stats.bytes_read,
                samples_decoded: execution.stats.samples_decoded,
                typed_scalar_chunks_decoded: execution.stats.typed_scalar_chunks_decoded,
                typed_full_chunks_decoded: execution.stats.typed_full_chunks_decoded,
            });
        }

        Ok(report)
    }

    pub fn open_manifest_published(
        segments_dir: impl AsRef<Path>,
        manifest_dir: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let Some(inventory) = read_manifest_inventory(manifest_dir)? else {
            return Ok(Self {
                segments: Vec::new(),
                query_projection_config: QueryProjectionConfig::default(),
            });
        };
        Self::open_manifest_inventory(segments_dir, &inventory)
    }

    pub fn open_manifest_inventory(
        segments_dir: impl AsRef<Path>,
        inventory: &ManifestInventory,
    ) -> io::Result<Self> {
        let segments_dir = segments_dir.as_ref();
        let mut segments = Vec::with_capacity(inventory.segments.len());

        for manifest_segment in &inventory.segments {
            let parsed =
                SegmentId::parse_dir_name(&manifest_segment.segment_id).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid manifest segment id: {err}"),
                    )
                })?;
            if parsed.start_ms() != manifest_segment.start_ms
                || parsed.end_ms() != manifest_segment.end_ms
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest segment id range does not match inventory range",
                ));
            }

            let reader =
                SegmentReader::open_validated(segments_dir.join(&manifest_segment.segment_id))?;
            validate_manifest_segment_meta(manifest_segment, reader.meta())?;
            segments.push(reader);
        }

        sort_segment_readers(&mut segments);
        Ok(Self {
            segments,
            query_projection_config: QueryProjectionConfig::default(),
        })
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

        let mut results = Vec::new();
        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }

            results.extend(segment.query_exact(matchers, start_ms, end_ms)?);
        }

        Ok(merge_query_results(results))
    }

    pub fn query_selector(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_limits(selector, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_selector_with_limits(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let results = self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
        Ok(QueryExecution {
            results,
            stats: budget.stats(),
        })
    }

    fn query_selectors_with_limits(
        &self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        for selector in selectors {
            results.extend(self.query_selector_with_budget(
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )?);
        }
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub fn query_selector_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        self.query_selector_with_head_with_limits(
            head,
            labels,
            selector,
            start_ms,
            end_ms,
            QueryLimits::unlimited(),
        )
        .map(|execution| execution.results)
    }

    pub fn query_selector_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::new(limits);
        let mut results =
            self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
        results.extend(head.query_selector_with_budget(
            labels,
            selector,
            start_ms,
            end_ms,
            &mut budget,
        )?);
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    fn query_selectors_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        for selector in selectors {
            results.extend(self.query_selector_with_budget(
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )?);
            results.extend(head.query_selector_with_budget(
                labels,
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )?);
        }
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub fn query_promql(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_with_limits(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, limits)
    }

    pub fn query_promql_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let query = parse_query(query)?;
        self.execute_promql_query_with_head(
            head,
            labels,
            &query,
            start_ms,
            end_ms,
            QueryLimits::unlimited(),
        )
        .map(|execution| execution.results)
    }

    pub fn query_promql_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let query = parse_query(query)?;
        self.execute_promql_query_with_head(head, labels, &query, start_ms, end_ms, limits)
    }

    fn execute_promql_query(
        &self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramQuantile(function) => {
                let mut execution =
                    self.execute_promql_query(&function.input, start_ms, end_ms, limits)?;
                execution.results =
                    evaluate_histogram_quantile(function, execution.results, end_ms);
                Ok(execution)
            }
        }
    }

    fn execute_promql_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selectors_with_head_with_limits(
                    head, labels, &selectors, start_ms, end_ms, limits,
                )
                .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramQuantile(function) => {
                let mut execution = self.execute_promql_query_with_head(
                    head,
                    labels,
                    &function.input,
                    start_ms,
                    end_ms,
                    limits,
                )?;
                execution.results =
                    evaluate_histogram_quantile(function, execution.results, end_ms);
                Ok(execution)
            }
        }
    }

    pub fn metric_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metric_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn metric_names_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metric_names(start_ms, end_ms, &mut metadata)?;
        head.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_names_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_names(start_ms, end_ms, &mut metadata)?;
        head.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_values(label_name, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    pub fn label_values_with_head<R>(
        &self,
        label_name: &str,
        head: &HeadBuffer,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_values(label_name, start_ms, end_ms, &mut metadata)?;
        head.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    fn query_selector_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for segment in &self.segments {
            budget.observe_segment_considered();
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            results.extend(segment.query_selector_with_budget(selector, start_ms, end_ms, budget)?);
        }

        Ok(merge_query_results(results))
    }

    fn collect_metric_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }
            segment.collect_metric_names(start_ms, end_ms, metadata)?;
        }

        Ok(())
    }

    fn collect_label_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }
            segment.collect_label_names(start_ms, end_ms, metadata)?;
        }

        Ok(())
    }

    fn collect_label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }
            segment.collect_label_values(label_name, start_ms, end_ms, metadata)?;
        }

        Ok(())
    }
}

fn range_function_start_ms(end_ms: u64, range_ms: u64) -> u64 {
    end_ms.saturating_sub(range_ms)
}

fn evaluate_range_function(
    function: &PromqlRangeFunction,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some(increase) = counter_increase(&result.samples, result.counter_reset_hints()) else {
            continue;
        };
        let Some((first_ts, _)) = result.samples.first().copied() else {
            continue;
        };
        let Some((last_ts, _)) = result.samples.last().copied() else {
            continue;
        };
        let value = match function.kind {
            PromqlRangeFunctionKind::Increase => increase,
            PromqlRangeFunctionKind::Rate => {
                let elapsed_ms = last_ts.saturating_sub(first_ts);
                if elapsed_ms == 0 {
                    continue;
                }
                increase / (elapsed_ms as f64 / 1_000.0)
            }
        };
        if !value.is_finite() {
            continue;
        }
        let labels = function_result_labels(&result.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    if let Some(counter_reset_hints) = counter_reset_hints {
        return counter_increase_with_reset_hints(samples, counter_reset_hints);
    }
    counter_increase_from_value_decreases(samples)
}

fn counter_increase_from_value_decreases(samples: &[(u64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples.iter();
    let (_, first) = iter.next().copied()?;
    if !first.is_finite() {
        return None;
    }
    let mut previous = first;
    let mut increase = 0.0f64;
    for (_, current) in iter.copied() {
        if !current.is_finite() {
            return None;
        }
        if current >= previous {
            increase += current - previous;
        } else {
            increase += current;
        }
        previous = current;
    }
    Some(increase)
}

fn counter_increase_with_reset_hints(
    samples: &[(u64, f64)],
    counter_reset_hints: &[CounterResetHint],
) -> Option<f64> {
    if counter_reset_hints.len() != samples.len() {
        return counter_increase_from_value_decreases(samples);
    }
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples
        .iter()
        .copied()
        .zip(counter_reset_hints.iter().copied());
    let ((_, first), _) = iter.next()?;
    if !first.is_finite() {
        return None;
    }
    let mut previous = first;
    let mut increase = 0.0f64;
    for ((_, current), reset_hint) in iter {
        if !current.is_finite() {
            return None;
        }
        match reset_hint {
            CounterResetHint::CounterReset => {
                increase += current;
            }
            CounterResetHint::NotCounterReset => {
                if current < previous {
                    return None;
                }
                increase += current - previous;
            }
            CounterResetHint::Unknown => {
                if current >= previous {
                    increase += current - previous;
                } else {
                    increase += current;
                }
            }
            CounterResetHint::GaugeType => return None,
        }
        previous = current;
    }
    Some(increase)
}

fn function_result_labels(labels: &[(String, String)]) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL)
        .cloned()
        .collect()
}

fn evaluate_histogram_quantile(
    function: &PromqlHistogramQuantile,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut groups = BTreeMap::<Vec<(String, String)>, Vec<(f64, f64)>>::new();
    for result in results {
        let Some(upper_bound) = histogram_bucket_upper_bound(&result.labels) else {
            continue;
        };
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if !value.is_finite() {
            continue;
        }
        let labels = histogram_quantile_result_labels(&result.labels);
        groups.entry(labels).or_default().push((upper_bound, value));
    }

    let mut out = Vec::new();
    for (labels, buckets) in groups {
        let Some(value) = classic_histogram_quantile(function.quantile, buckets) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn histogram_bucket_upper_bound(labels: &[(String, String)]) -> Option<f64> {
    let value = labels
        .iter()
        .find_map(|(key, value)| (key == "le").then_some(value.as_str()))?;
    if value == "+Inf" {
        return Some(f64::INFINITY);
    }
    let upper_bound = value.parse::<f64>().ok()?;
    upper_bound.is_finite().then_some(upper_bound)
}

fn histogram_quantile_result_labels(labels: &[(String, String)]) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL && key != "le")
        .cloned()
        .collect()
}

fn classic_histogram_quantile(quantile: f64, mut buckets: Vec<(f64, f64)>) -> Option<f64> {
    if quantile.is_nan() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }

    buckets.sort_by(|(left, _), (right, _)| left.total_cmp(right));
    let mut compacted = Vec::<(f64, f64)>::with_capacity(buckets.len());
    for (upper_bound, count) in buckets {
        if upper_bound.is_nan() || !count.is_finite() {
            return None;
        }
        if let Some((last_upper_bound, last_count)) = compacted.last_mut()
            && *last_upper_bound == upper_bound
        {
            *last_count = (*last_count).max(count);
            continue;
        }
        compacted.push((upper_bound, count.max(0.0)));
    }

    if compacted.len() < 2
        || !compacted
            .last()
            .is_some_and(|(bound, _)| bound.is_infinite())
    {
        return None;
    }

    let mut previous_count = 0.0;
    for (_, count) in &mut compacted {
        if *count < previous_count {
            *count = previous_count;
        } else {
            previous_count = *count;
        }
    }

    let total = compacted.last().map(|(_, count)| *count)?;
    if total <= 0.0 {
        return Some(f64::NAN);
    }

    let rank = quantile * total;
    let bucket_index = compacted
        .iter()
        .position(|(_, count)| *count >= rank)
        .unwrap_or(compacted.len() - 1);
    if bucket_index == compacted.len() - 1 {
        return compacted
            .get(bucket_index.saturating_sub(1))
            .map(|(bound, _)| *bound);
    }

    let (upper_bound, upper_count) = compacted[bucket_index];
    if bucket_index == 0 && upper_bound <= 0.0 {
        return Some(upper_bound);
    }
    let (lower_bound, lower_count) = if bucket_index == 0 {
        (0.0, 0.0)
    } else {
        compacted[bucket_index - 1]
    };
    let bucket_count = upper_count - lower_count;
    if bucket_count <= 0.0 {
        return Some(upper_bound);
    }

    Some(lower_bound + (upper_bound - lower_bound) * (rank - lower_count) / bucket_count)
}

impl SegmentReader {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let meta_path = dir.join(SegmentFile::MetaJson.filename());
        let meta_bytes = fs::read(meta_path)?;
        let meta = serde_json::from_slice(&meta_bytes).map_err(io::Error::other)?;
        Ok(Self { dir, meta })
    }

    pub fn open_validated(dir: impl AsRef<Path>) -> io::Result<Self> {
        let reader = Self::open(dir)?;
        validate_segment_footer(&reader.dir)?;
        Ok(reader)
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
        let matchers: Vec<NormalizedMatcher> = matchers
            .iter()
            .map(|(name, value)| NormalizedMatcher::Eq {
                name: (*name).to_string(),
                value: (*value).to_string(),
            })
            .collect();
        let mut budget = QueryBudget::unlimited();
        self.query_normalized(
            &matchers,
            &SegmentProjection::None,
            start_ms,
            end_ms,
            &mut budget,
        )
    }

    pub fn query_selector(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut budget = QueryBudget::unlimited();
        self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)
    }

    fn query_selector_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers = selector.normalized_matchers();
        self.query_normalized(&matchers, &selector.projection, start_ms, end_ms, budget)
    }

    pub fn metric_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metric_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_values(label_name, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    fn collect_smoke_report(
        &self,
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
        collect_totals: bool,
        report: &mut SegmentStoreSmokeReport,
    ) -> io::Result<()> {
        if !collect_totals
            && self.meta.chunk_summary.as_ref().is_some_and(|summary| {
                report.sample_limits_reached_for_summary(summary, sample_limit_per_kind)
            })
        {
            return Ok(());
        }

        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let mut series_reader =
            SeriesReader::open(File::open(self.file_path(SegmentFile::Series))?)?;
        let mut chunk_index_reader =
            ChunkIndexReader::open(File::open(self.file_path(SegmentFile::ChunkIndex))?)?;
        let mut chunk_file = self.open_chunks()?;

        if collect_totals {
            chunk_index_reader.for_each_series_entries(|series_ref, entries| {
                Self::collect_smoke_entries_for_series(
                    &self.meta.segment_id,
                    series_ref,
                    entries,
                    start_ms,
                    end_ms,
                    sample_limit_per_kind,
                    collect_totals,
                    report,
                    &symbols,
                    &mut series_reader,
                    &mut chunk_file,
                )
            })?;
        } else {
            for series_ref in 0..chunk_index_reader.len() {
                if self.meta.chunk_summary.as_ref().is_some_and(|summary| {
                    report.sample_limits_reached_for_summary(summary, sample_limit_per_kind)
                }) {
                    break;
                }
                let series_ref = u32::try_from(series_ref).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "series_ref exceeds u32")
                })?;
                let Some(entries) = chunk_index_reader.read_entries(series_ref)? else {
                    continue;
                };
                Self::collect_smoke_entries_for_series(
                    &self.meta.segment_id,
                    series_ref,
                    &entries,
                    start_ms,
                    end_ms,
                    sample_limit_per_kind,
                    collect_totals,
                    report,
                    &symbols,
                    &mut series_reader,
                    &mut chunk_file,
                )?;
            }
        }

        Ok(())
    }

    fn collect_smoke_entries_for_series(
        segment_id: &str,
        series_ref: u32,
        entries: &[ChunkIndexEntry],
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
        collect_totals: bool,
        report: &mut SegmentStoreSmokeReport,
        symbols: &SegmentSymbols,
        series_reader: &mut SeriesReader<File>,
        chunk_file: &mut File,
    ) -> io::Result<()> {
        let mut resolved_entry: Option<(SeriesEntry, Vec<(String, String)>)> = None;
        for entry in entries {
            if !chunk_overlaps_range(entry, start_ms, end_ms) {
                continue;
            }

            let chunk_bytes = u64::from(entry.length);
            if collect_totals {
                report.totals.chunks = report.totals.chunks.saturating_add(1);
                report.totals.chunk_bytes = report.totals.chunk_bytes.saturating_add(chunk_bytes);
                report.totals.by_kind.add_chunk(entry.kind, chunk_bytes);
            }

            if sample_limit_per_kind == 0
                || report.sample_count_for_kind(entry.kind) >= sample_limit_per_kind
            {
                continue;
            }

            if resolved_entry.is_none() {
                let Some(series_entry) = series_reader.read_entry(series_ref)? else {
                    continue;
                };
                let labels = Self::resolve_series_labels(symbols, &series_entry)?;
                resolved_entry = Some((series_entry, labels));
            }
            let Some((series_entry, labels)) = resolved_entry.as_ref() else {
                continue;
            };
            let record = read_chunk_record_at(chunk_file, entry.offset, entry.length)?;
            report.sample_series.push(smoke_series_sample(
                segment_id.to_string(),
                series_ref,
                series_entry.series_id,
                labels.clone(),
                &record,
                entry.length,
            ));
        }
        Ok(())
    }

    fn query_normalized(
        &self,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut context = SegmentQueryContext::open(self, None)?;
        self.query_normalized_with_context(
            &mut context,
            matchers,
            projection,
            start_ms,
            end_ms,
            budget,
        )
    }

    fn query_normalized_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => Some(compile_label_matchers(matchers)?),
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::SummaryQuantile { .. } => None,
        };

        let equality_matchers =
            match plan_positive_equality_matchers(context, matchers, start_ms, end_ms) {
                Ok(equality_matchers) => equality_matchers,
                Err(SegmentPruneReason::MissingEquality) => {
                    budget.observe_segment_skipped_by_missing_equality();
                    return Ok(Vec::new());
                }
                Err(SegmentPruneReason::MatcherTimeRange) => {
                    budget.observe_segment_skipped_by_matcher_time_range();
                    return Ok(Vec::new());
                }
            };
        budget.observe_segment_queried();

        let mut candidates: Option<Vec<u32>> = None;
        for matcher in &equality_matchers {
            let positive = if let Some(existing) = &candidates
                && should_verify_equality_candidates(existing.len(), matcher.postings.byte_len)
            {
                self.filter_candidates_by_equality_matcher(context, existing, matcher)?
            } else {
                let posting = exact_postings_with_budget(
                    &mut context.index_reader,
                    matcher.name_sym,
                    matcher.value_sym,
                    matcher.postings,
                    budget,
                )?
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing postings"))?;
                match &candidates {
                    Some(existing) => intersect_sorted(existing, &posting),
                    None => posting,
                }
            };

            if positive.is_empty() {
                return Ok(Vec::new());
            }
            candidates = Some(positive);
        }

        for matcher in matchers {
            let positive = match matcher {
                NormalizedMatcher::Eq { .. } => None,
                NormalizedMatcher::Regex { name, pattern } => Some(regex_postings(
                    name,
                    pattern,
                    &context.symbols,
                    &mut context.index_reader,
                    start_ms,
                    end_ms,
                    budget,
                    projection_matches_promql_metric_name_regex(projection)
                        && name == METRIC_NAME_LABEL,
                )?),
                NormalizedMatcher::NotEq { .. } | NormalizedMatcher::NotRegex { .. } => None,
            };

            if let Some(positive) = positive {
                if positive.is_empty() {
                    return Ok(Vec::new());
                }
                candidates = Some(match candidates {
                    Some(existing) => intersect_sorted(&existing, &positive),
                    None => positive,
                });
            }
        }

        let series_count = u32::try_from(self.meta.series).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment series count exceeds local reference range",
            )
        })?;
        let mut candidate_refs = candidates.unwrap_or_else(|| (0..series_count).collect());
        for matcher in matchers {
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let (Some(name_sym), Some(value_sym)) =
                        (context.symbols.lookup(name), context.symbols.lookup(value))
                    else {
                        continue;
                    };
                    let Some(postings) = context
                        .index_reader
                        .exact_postings_metadata(name_sym, value_sym)
                    else {
                        continue;
                    };
                    if !postings.time_range.overlaps(start_ms, end_ms) {
                        continue;
                    }
                    let Some(posting) = exact_postings_with_budget(
                        &mut context.index_reader,
                        name_sym,
                        value_sym,
                        postings,
                        budget,
                    )?
                    else {
                        continue;
                    };
                    candidate_refs = subtract_sorted(&candidate_refs, &posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = regex_postings(
                        name,
                        pattern,
                        &context.symbols,
                        &mut context.index_reader,
                        start_ms,
                        end_ms,
                        budget,
                        false,
                    )?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        budget.observe_candidate_series_refs(candidate_refs.len() as u64)?;

        let mut results = Vec::new();

        for series_ref in candidate_refs {
            let Some(entry) = context.series_reader(self)?.read_entry(series_ref)? else {
                continue;
            };
            if !series_kind_mask_matches_projection(projection, entry.kind_mask) {
                continue;
            }
            budget.observe_matched_series(entry.series_id)?;
            let Some(entries) = context.chunk_index_reader(self)?.read_entries(series_ref)? else {
                continue;
            };

            let labels = Self::resolve_series_labels(&context.symbols, &entry)?;
            let metric_name = labels
                .iter()
                .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
                .unwrap_or_default();

            let mut samples = Vec::new();
            let mut projected_results: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
            for chunk_entry in &entries {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if let Some((scalar_projection, metric_suffix)) =
                    typed_scalar_projection(projection, chunk_entry.kind)
                {
                    budget.observe_chunk_read(u64::from(chunk_entry.length))?;
                    let record = read_chunk_scalar_projection_at(
                        context.chunk_file(self)?,
                        chunk_entry.offset,
                        chunk_entry.length,
                        scalar_projection,
                    )?;
                    budget.observe_typed_scalar_chunk_decoded();
                    budget.observe_samples_decoded(record.samples.len() as u64)?;
                    Self::project_typed_scalar_samples(
                        &mut projected_results,
                        &labels,
                        metric_name,
                        metric_suffix,
                        record.samples,
                        start_ms,
                        end_ms,
                    );
                    continue;
                }
                if !chunk_kind_matches_projection(projection, chunk_entry.kind) {
                    continue;
                }
                budget.observe_chunk_read(u64::from(chunk_entry.length))?;
                let record = read_chunk_record_at(
                    context.chunk_file(self)?,
                    chunk_entry.offset,
                    chunk_entry.length,
                )?;
                if chunk_kind_is_typed(record.kind) {
                    budget.observe_typed_full_chunk_decoded();
                }
                match (projection, record.samples) {
                    (
                        SegmentProjection::None | SegmentProjection::AllPromql { .. },
                        ChunkSamples::Float(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms),
                        );
                    }
                    (
                        SegmentProjection::None | SegmentProjection::AllPromql { .. },
                        ChunkSamples::Int64(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms)
                                .map(|(ts, value)| (ts, value as f64)),
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::ExponentialHistogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_exponential_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::ExponentialHistogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_exponential_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::HistogramBucket { le, .. },
                        ChunkSamples::Histogram(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            le.as_deref(),
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::HistogramBucket {
                            le,
                            exponential_histogram_boundaries,
                        },
                        ChunkSamples::ExponentialHistogram(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_exponential_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            le.as_deref(),
                            exponential_histogram_boundaries,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::SummaryQuantile { quantile },
                        ChunkSamples::Summary(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_quantile_samples(
                            &mut projected_results,
                            &labels,
                            quantile.as_deref(),
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::AllPromql { .. }, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            None,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::AllPromql {
                            exponential_histogram_boundaries,
                        },
                        ChunkSamples::ExponentialHistogram(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_exponential_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_exponential_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_exponential_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            None,
                            exponential_histogram_boundaries,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::AllPromql { .. }, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_summary_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_summary_quantile_samples(
                            &mut projected_results,
                            &labels,
                            None,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (_, ChunkSamples::Float(_))
                    | (_, ChunkSamples::Int64(_))
                    | (_, ChunkSamples::Histogram(_))
                    | (_, ChunkSamples::ExponentialHistogram(_))
                    | (_, ChunkSamples::Summary(_)) => {}
                }
            }

            if matches!(
                projection,
                SegmentProjection::None | SegmentProjection::AllPromql { .. }
            ) {
                if !samples.is_empty()
                    && projected_label_filter
                        .as_ref()
                        .is_none_or(|filter| labels_match_compiled(&labels, filter))
                {
                    samples.sort_by_key(|(ts, _)| *ts);
                    results.push(SegmentQueryResult::with_samples(
                        entry.series_id,
                        labels.clone(),
                        samples,
                    ));
                }
                if !matches!(projection, SegmentProjection::AllPromql { .. }) {
                    continue;
                }
            }

            if let Some(filter) = &projected_label_filter {
                projected_results.retain(|_, result| labels_match_compiled(&result.labels, filter));
            }
            results.extend(projected_results.into_values());
        }

        budget.observe_projected_results(&results)?;
        Ok(results)
    }

    fn filter_candidates_by_equality_matcher(
        &self,
        context: &mut SegmentQueryContext,
        candidate_refs: &[u32],
        matcher: &ResolvedEqualityMatcher,
    ) -> io::Result<Vec<u32>> {
        let mut retained = Vec::new();
        for &series_ref in candidate_refs {
            let Some(entry) = context.series_reader(self)?.read_entry(series_ref)? else {
                continue;
            };
            if series_entry_has_label(&entry, matcher.name_sym, matcher.value_sym) {
                retained.push(series_ref);
            }
        }
        Ok(retained)
    }

    fn resolve_series_labels(
        symbols: &SegmentSymbols,
        entry: &SeriesEntry,
    ) -> io::Result<Vec<(String, String)>> {
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
        Ok(labels)
    }

    fn project_histogram_count_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, HistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_u64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_count",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        );
    }

    fn project_exponential_histogram_count_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, ExponentialHistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_u64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_count",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        );
    }

    fn project_summary_count_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, SummaryValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_u64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_count",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        );
    }

    fn project_histogram_sum_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, HistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_optional_f64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_sum",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
    }

    fn project_exponential_histogram_sum_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, ExponentialHistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_optional_f64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_sum",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
    }

    fn project_summary_sum_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, SummaryValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_optional_f64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_sum",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, Some(value.sum))),
            start_ms,
            end_ms,
        );
    }

    fn project_typed_u64_counter_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &str,
        values: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let labels = Self::projected_labels(base_labels, metric_name, metric_suffix, None);
        let mut delta_accumulator = 0u64;
        for (ts, metadata, raw) in values {
            if ts < start_ms || ts > end_ms {
                continue;
            }
            let value = if metadata.is_stale() {
                prometheus_stale_nan()
            } else if metadata.temporality == OtlpAggregationTemporality::Delta {
                delta_accumulator = delta_accumulator.saturating_add(raw);
                delta_accumulator as f64
            } else {
                raw as f64
            };
            Self::push_projected_sample_with_counter_reset_hint(
                out,
                labels.clone(),
                ts,
                value,
                metadata.reset_hint,
            );
        }
    }

    fn project_typed_optional_f64_counter_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &str,
        values: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let labels = Self::projected_labels(base_labels, metric_name, metric_suffix, None);
        let mut delta_accumulator = 0.0f64;
        for (ts, metadata, raw) in values {
            if ts < start_ms || ts > end_ms {
                continue;
            }
            let value = if metadata.is_stale() {
                prometheus_stale_nan()
            } else if let Some(raw) = raw {
                if metadata.temporality == OtlpAggregationTemporality::Delta {
                    delta_accumulator += raw;
                    delta_accumulator
                } else {
                    raw
                }
            } else {
                continue;
            };
            Self::push_projected_sample_with_counter_reset_hint(
                out,
                labels.clone(),
                ts,
                value,
                metadata.reset_hint,
            );
        }
    }

    fn project_typed_scalar_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &str,
        values: Vec<ChunkScalarSample>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let labels = Self::projected_labels(base_labels, metric_name, metric_suffix, None);
        let mut delta_count_accumulator = 0u64;
        let mut delta_sum_accumulator = 0.0f64;
        for sample in values {
            if sample.timestamp_ms < start_ms || sample.timestamp_ms > end_ms {
                continue;
            }
            let value = if sample.metadata.is_stale() {
                prometheus_stale_nan()
            } else {
                match sample.value {
                    Some(ChunkScalarValue::Count(raw)) => {
                        if sample.metadata.temporality == OtlpAggregationTemporality::Delta {
                            delta_count_accumulator = delta_count_accumulator.saturating_add(raw);
                            delta_count_accumulator as f64
                        } else {
                            raw as f64
                        }
                    }
                    Some(ChunkScalarValue::Sum(raw)) => {
                        if sample.metadata.temporality == OtlpAggregationTemporality::Delta {
                            delta_sum_accumulator += raw;
                            delta_sum_accumulator
                        } else {
                            raw
                        }
                    }
                    None => continue,
                }
            };
            Self::push_projected_sample_with_counter_reset_hint(
                out,
                labels.clone(),
                sample.timestamp_ms,
                value,
                sample.metadata.reset_hint,
            );
        }
    }

    fn project_histogram_bucket_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        le_filter: Option<&str>,
        values: Vec<(u64, HistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let mut delta_accumulators: BTreeMap<String, u64> = BTreeMap::new();
        for (ts, value) in values {
            if ts < start_ms || ts > end_ms {
                continue;
            }
            let mut cumulative = 0u64;
            for (idx, bound) in value.explicit_bounds.iter().enumerate() {
                cumulative =
                    cumulative.saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
                let le = Self::format_promql_float_label(*bound);
                if le_filter.is_none_or(|filter| filter == le) {
                    let projected = histogram_projected_bucket_value(
                        value.metadata,
                        cumulative,
                        &le,
                        &mut delta_accumulators,
                    );
                    let labels = Self::projected_labels(
                        base_labels,
                        metric_name,
                        "_bucket",
                        Some(("le", le)),
                    );
                    Self::push_projected_sample_with_counter_reset_hint(
                        out,
                        labels,
                        ts,
                        projected,
                        value.metadata.reset_hint,
                    );
                }
            }

            if le_filter.is_none_or(|filter| filter == "+Inf") {
                let projected = histogram_projected_bucket_value(
                    value.metadata,
                    value.count,
                    "+Inf",
                    &mut delta_accumulators,
                );
                let labels = Self::projected_labels(
                    base_labels,
                    metric_name,
                    "_bucket",
                    Some(("le", "+Inf".to_string())),
                );
                Self::push_projected_sample_with_counter_reset_hint(
                    out,
                    labels,
                    ts,
                    projected,
                    value.metadata.reset_hint,
                );
            }
        }
    }

    fn project_exponential_histogram_bucket_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        le_filter: Option<&str>,
        boundaries: &[f64],
        values: Vec<(u64, ExponentialHistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let mut delta_accumulators: BTreeMap<String, u64> = BTreeMap::new();
        for (ts, value) in values {
            if ts < start_ms || ts > end_ms {
                continue;
            }

            for boundary in boundaries {
                let le = Self::format_promql_float_label(*boundary);
                if le_filter.is_none_or(|filter| filter == le) {
                    let raw = exponential_histogram_projected_bucket_count(&value, *boundary);
                    let projected = histogram_projected_bucket_value(
                        value.metadata,
                        raw,
                        &le,
                        &mut delta_accumulators,
                    );
                    let labels = Self::projected_labels(
                        base_labels,
                        metric_name,
                        "_bucket",
                        Some(("le", le)),
                    );
                    Self::push_projected_sample_with_counter_reset_hint(
                        out,
                        labels,
                        ts,
                        projected,
                        value.metadata.reset_hint,
                    );
                }
            }

            if le_filter.is_none_or(|filter| filter == "+Inf") {
                let projected = histogram_projected_bucket_value(
                    value.metadata,
                    value.count,
                    "+Inf",
                    &mut delta_accumulators,
                );
                let labels = Self::projected_labels(
                    base_labels,
                    metric_name,
                    "_bucket",
                    Some(("le", "+Inf".to_string())),
                );
                Self::push_projected_sample_with_counter_reset_hint(
                    out,
                    labels,
                    ts,
                    projected,
                    value.metadata.reset_hint,
                );
            }
        }
    }

    fn project_summary_quantile_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        quantile_filter: Option<&str>,
        values: Vec<(u64, SummaryValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let metric_name = base_labels
            .iter()
            .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
            .unwrap_or_default();
        for (ts, value) in values {
            if ts < start_ms || ts > end_ms {
                continue;
            }
            for quantile in value.quantiles {
                let label = Self::format_promql_float_label(quantile.quantile);
                if quantile_filter.is_some_and(|filter| filter != label) {
                    continue;
                }
                let labels =
                    Self::projected_labels(base_labels, metric_name, "", Some(("quantile", label)));
                let projected = if value.metadata.is_stale() {
                    prometheus_stale_nan()
                } else {
                    quantile.value
                };
                Self::push_projected_sample(out, labels, ts, projected);
            }
        }
    }

    fn projected_labels(
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &str,
        extra_label: Option<(&str, String)>,
    ) -> Vec<(String, String)> {
        let mut labels = Vec::with_capacity(base_labels.len() + usize::from(extra_label.is_some()));
        let mut metric_seen = false;
        let extra_key = extra_label.as_ref().map(|(key, _)| *key);
        for (key, value) in base_labels {
            if key == METRIC_NAME_LABEL {
                labels.push((key.clone(), format!("{metric_name}{metric_suffix}")));
                metric_seen = true;
            } else if extra_key != Some(key.as_str()) {
                labels.push((key.clone(), value.clone()));
            }
        }
        if !metric_seen {
            labels.push((
                METRIC_NAME_LABEL.to_string(),
                format!("{metric_name}{metric_suffix}"),
            ));
        }
        if let Some((key, value)) = extra_label {
            labels.push((key.to_string(), value));
        }
        labels.sort_by(|left, right| left.0.cmp(&right.0));
        labels
    }

    fn push_projected_sample(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        labels: Vec<(String, String)>,
        timestamp_ms: u64,
        value: f64,
    ) {
        let series_id = segment_series_id(&labels);
        let entry = out
            .entry(series_id)
            .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
        entry.push_sample(timestamp_ms, value);
    }

    fn push_projected_sample_with_counter_reset_hint(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        labels: Vec<(String, String)>,
        timestamp_ms: u64,
        value: f64,
        reset_hint: CounterResetHint,
    ) {
        let series_id = segment_series_id(&labels);
        let entry = out
            .entry(series_id)
            .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
        entry.push_sample_with_counter_reset_hint(timestamp_ms, value, reset_hint);
    }

    fn format_promql_float_label(value: f64) -> String {
        if value.is_infinite() && value.is_sign_positive() {
            "+Inf".to_string()
        } else {
            value.to_string()
        }
    }

    fn collect_metric_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if !self.can_collect_metadata_for_range(start_ms, end_ms) {
            return Ok(());
        }

        let (symbols, mut index_reader) = self.read_symbols_and_index_reader()?;
        if !index_reader.has_label_values() {
            return self.collect_metadata_from_series_chunks(start_ms, end_ms, metadata, &symbols);
        }

        collect_metric_names_from_index(&symbols, &mut index_reader, start_ms, end_ms, metadata)
    }

    fn collect_label_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if !self.can_collect_metadata_for_range(start_ms, end_ms) {
            return Ok(());
        }

        let (symbols, mut index_reader) = self.read_symbols_and_index_reader()?;
        if !index_reader.has_label_values() {
            return self.collect_metadata_from_series_chunks(start_ms, end_ms, metadata, &symbols);
        }

        collect_label_names_from_index(&symbols, &mut index_reader, start_ms, end_ms, metadata)
    }

    fn collect_label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if !self.can_collect_metadata_for_range(start_ms, end_ms) {
            return Ok(());
        }

        let (symbols, mut index_reader) = self.read_symbols_and_index_reader()?;
        if !index_reader.has_label_values() {
            return self.collect_metadata_from_series_chunks(start_ms, end_ms, metadata, &symbols);
        }

        collect_label_values_from_index(
            &symbols,
            &mut index_reader,
            label_name,
            start_ms,
            end_ms,
            metadata,
        )
    }

    fn can_collect_metadata_for_range(&self, start_ms: u64, end_ms: u64) -> bool {
        end_ms >= start_ms && self.meta.end_ms >= start_ms && self.meta.start_ms <= end_ms
    }

    fn read_symbols_and_index_reader(
        &self,
    ) -> io::Result<(SegmentSymbols, SegmentIndexReader<File>)> {
        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let index_reader =
            SegmentIndexReader::open(File::open(self.file_path(SegmentFile::Indexes))?)?;
        Ok((symbols, index_reader))
    }

    fn collect_metadata_from_series_chunks(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
        symbols: &SegmentSymbols,
    ) -> io::Result<()> {
        let series = read_series_bin(File::open(self.file_path(SegmentFile::Series))?)?;
        let chunk_index = self.read_chunk_index()?;
        for (series_idx, entry) in series.iter().enumerate() {
            let Some(entries) = chunk_index.get(series_idx) else {
                continue;
            };
            if !entries
                .iter()
                .any(|chunk| chunk_overlaps_range(chunk, start_ms, end_ms))
            {
                continue;
            }

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
            metadata.add_labelset(&labels);
        }

        Ok(())
    }
}

fn typed_scalar_projection(
    projection: &SegmentProjection,
    kind: ChunkKind,
) -> Option<(ChunkScalarProjection, &'static str)> {
    if !chunk_kind_is_typed(kind) {
        return None;
    }
    match projection {
        SegmentProjection::Count => Some((ChunkScalarProjection::Count, "_count")),
        SegmentProjection::Sum => Some((ChunkScalarProjection::Sum, "_sum")),
        SegmentProjection::None
        | SegmentProjection::AllPromql { .. }
        | SegmentProjection::HistogramBucket { .. }
        | SegmentProjection::SummaryQuantile { .. } => None,
    }
}

pub(crate) fn projection_matches_promql_metric_name_regex(projection: &SegmentProjection) -> bool {
    matches!(
        projection,
        SegmentProjection::AllPromql { .. } | SegmentProjection::Count | SegmentProjection::Sum
    )
}

fn chunk_kind_matches_projection(projection: &SegmentProjection, kind: ChunkKind) -> bool {
    match projection {
        SegmentProjection::None => matches!(kind, ChunkKind::Float | ChunkKind::Int64),
        SegmentProjection::AllPromql { .. } => true,
        SegmentProjection::Count | SegmentProjection::Sum => chunk_kind_is_typed(kind),
        SegmentProjection::HistogramBucket { .. } => {
            matches!(kind, ChunkKind::Histogram | ChunkKind::ExponentialHistogram)
        }
        SegmentProjection::SummaryQuantile { .. } => kind == ChunkKind::Summary,
    }
}

fn chunk_kind_is_typed(kind: ChunkKind) -> bool {
    matches!(
        kind,
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
    )
}

fn series_kind_mask_matches_projection(projection: &SegmentProjection, kind_mask: u8) -> bool {
    let required = match projection {
        SegmentProjection::None => SERIES_KIND_FLOAT | SERIES_KIND_INT64,
        SegmentProjection::AllPromql { .. } => return true,
        SegmentProjection::Count | SegmentProjection::Sum => {
            SERIES_KIND_HISTOGRAM | SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY
        }
        SegmentProjection::HistogramBucket { .. } => {
            SERIES_KIND_HISTOGRAM | SERIES_KIND_EXPONENTIAL_HISTOGRAM
        }
        SegmentProjection::SummaryQuantile { .. } => SERIES_KIND_SUMMARY,
    };
    kind_mask & required != 0
}

fn collect_metric_names_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let Some(name_sym) = symbols.lookup(METRIC_NAME_LABEL) else {
        return Ok(());
    };
    collect_label_values_by_symbol_from_index(
        symbols,
        index_reader,
        name_sym,
        METRIC_NAME_LABEL,
        start_ms,
        end_ms,
        metadata,
    )
}

fn collect_label_names_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    for name_sym in index_reader.label_name_symbols() {
        if !label_name_overlaps_range(index_reader, name_sym, start_ms, end_ms) {
            continue;
        }
        let name = symbols
            .resolve(name_sym)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "label symbol missing"))?
            .to_string();
        metadata.add_label_name(name);
    }

    Ok(())
}

fn collect_label_values_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    label_name: &str,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let label_name = normalize_discovery_label_name(label_name);
    let Some(name_sym) = symbols.lookup(&label_name) else {
        return Ok(());
    };
    collect_label_values_by_symbol_from_index(
        symbols,
        index_reader,
        name_sym,
        &label_name,
        start_ms,
        end_ms,
        metadata,
    )
}

fn collect_label_values_by_symbol_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    name_sym: u32,
    label_name: &str,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let ranges = index_reader
        .label_value_time_ranges(name_sym)?
        .map(|ranges| ranges.into_iter().collect::<BTreeMap<_, _>>());

    for value in index_reader.label_values(name_sym)? {
        let overlaps = if let Some(ranges) = &ranges {
            let value_sym = symbols.lookup(&value).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
            })?;
            ranges
                .get(&value_sym)
                .is_some_and(|range| range.overlaps(start_ms, end_ms))
        } else {
            true
        };

        if overlaps {
            metadata.add_label_value(label_name.to_string(), value);
        }
    }
    Ok(())
}

fn label_name_overlaps_range(
    index_reader: &SegmentIndexReader<impl Read + Seek>,
    name_sym: u32,
    start_ms: u64,
    end_ms: u64,
) -> bool {
    match index_reader.label_time_range(name_sym) {
        Some(range) => range.overlaps(start_ms, end_ms),
        None => true,
    }
}

fn exact_postings_with_budget(
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    name_sym: u32,
    value_sym: u32,
    postings: ExactPostingsMetadata,
    budget: &mut QueryBudget,
) -> io::Result<Option<Vec<u32>>> {
    budget.observe_index_postings_read(postings.byte_len);
    index_reader.exact_postings(name_sym, value_sym)
}

fn should_verify_equality_candidates(candidate_count: usize, postings_byte_len: u64) -> bool {
    const MAX_SERIES_DRIVEN_CANDIDATES: usize = 64;
    if candidate_count == 0 || candidate_count > MAX_SERIES_DRIVEN_CANDIDATES {
        return false;
    }

    let estimated_series_verify_bytes = (candidate_count as u64).saturating_mul(32);
    estimated_series_verify_bytes < postings_byte_len
}

fn series_entry_has_label(entry: &SeriesEntry, name_sym: u32, value_sym: u32) -> bool {
    entry
        .labels
        .iter()
        .any(|(name, value)| *name == name_sym && *value == value_sym)
}

pub(crate) fn compile_label_matchers(
    matchers: &[NormalizedMatcher],
) -> io::Result<Vec<CompiledLabelMatcher>> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        compiled.push(match matcher {
            NormalizedMatcher::Eq { name, value } => CompiledLabelMatcher::Eq {
                name: name.clone(),
                value: value.clone(),
            },
            NormalizedMatcher::NotEq { name, value } => CompiledLabelMatcher::NotEq {
                name: name.clone(),
                value: value.clone(),
            },
            NormalizedMatcher::Regex { name, pattern } => CompiledLabelMatcher::Regex {
                name: name.clone(),
                pattern: compile_promql_regex(pattern)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
            },
            NormalizedMatcher::NotRegex { name, pattern } => CompiledLabelMatcher::NotRegex {
                name: name.clone(),
                pattern: compile_promql_regex(pattern)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
            },
        });
    }
    Ok(compiled)
}

pub(crate) fn compile_promql_regex(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
}

pub(crate) fn labels_match_compiled(
    labels: &[(String, String)],
    matchers: &[CompiledLabelMatcher],
) -> bool {
    matchers.iter().all(|matcher| match matcher {
        CompiledLabelMatcher::Eq { name, value } => labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && label_value == value),
        CompiledLabelMatcher::NotEq { name, value } => !labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && label_value == value),
        CompiledLabelMatcher::Regex { name, pattern } => labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && pattern.is_match(label_value)),
        CompiledLabelMatcher::NotRegex { name, pattern } => !labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && pattern.is_match(label_value)),
    })
}

pub(crate) fn promql_projection_metric_name_matches(
    metric_name: &str,
    regex: &regex::Regex,
) -> bool {
    if regex.is_match(metric_name) {
        return true;
    }

    PROMQL_PROJECTION_SUFFIXES
        .iter()
        .any(|suffix| regex.is_match(&format!("{metric_name}{suffix}")))
}

fn normalize_matcher_name_value(name: &str, value: &str) -> (String, String) {
    if name == METRIC_NAME_LABEL {
        (METRIC_NAME_LABEL.to_string(), normalize_metric_name(value))
    } else {
        (normalize_label_name(name), value.to_string())
    }
}

fn normalize_matcher_name(name: &str) -> String {
    if name == METRIC_NAME_LABEL {
        METRIC_NAME_LABEL.to_string()
    } else {
        normalize_label_name(name)
    }
}

fn normalize_discovery_label_name(name: &str) -> String {
    normalize_matcher_name(name)
}

fn chunk_overlaps_range(chunk: &ChunkIndexEntry, start_ms: u64, end_ms: u64) -> bool {
    chunk.max_time_ms >= start_ms && chunk.min_time_ms <= end_ms
}

fn smoke_series_sample(
    segment_id: String,
    series_ref: u32,
    series_id: u64,
    labels: Vec<(String, String)>,
    record: &ChunkRecord,
    chunk_bytes: u32,
) -> SegmentStoreSmokeSeries {
    let (bucket_le, quantile) = match &record.samples {
        ChunkSamples::Histogram(values) => {
            let le = values
                .first()
                .and_then(|(_, value)| value.explicit_bounds.first().copied())
                .map(SegmentReader::format_promql_float_label)
                .unwrap_or_else(|| "+Inf".to_string());
            (Some(le), None)
        }
        ChunkSamples::ExponentialHistogram(_) => (Some("+Inf".to_string()), None),
        ChunkSamples::Summary(values) => {
            let quantile = values
                .first()
                .and_then(|(_, value)| value.quantiles.first())
                .map(|value| SegmentReader::format_promql_float_label(value.quantile));
            (None, quantile)
        }
        ChunkSamples::Float(_) | ChunkSamples::Int64(_) => (None, None),
    };

    SegmentStoreSmokeSeries {
        segment_id,
        series_ref,
        series_id,
        kind: record.kind,
        labels,
        min_time_ms: record.min_time_ms,
        max_time_ms: record.max_time_ms,
        samples: chunk_record_sample_count(record) as u64,
        chunk_bytes: u64::from(chunk_bytes),
        bucket_le,
        quantile,
    }
}

fn chunk_record_sample_count(record: &ChunkRecord) -> usize {
    match &record.samples {
        ChunkSamples::Float(values) => values.len(),
        ChunkSamples::Int64(values) => values.len(),
        ChunkSamples::Histogram(values) => values.len(),
        ChunkSamples::ExponentialHistogram(values) => values.len(),
        ChunkSamples::Summary(values) => values.len(),
    }
}

fn smoke_queries_for_sample(
    sample: &SegmentStoreSmokeSeries,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(ChunkKind, String, u64, u64)> {
    let Some(metric_name) = sample_metric_name(sample) else {
        return Vec::new();
    };
    let query_start_ms = sample.min_time_ms.max(start_ms);
    let query_end_ms = sample.max_time_ms.min(end_ms);
    if query_end_ms < query_start_ms {
        return Vec::new();
    }

    match sample.kind {
        ChunkKind::Float | ChunkKind::Int64 => {
            vec![(
                sample.kind,
                promql_exact_selector(metric_name, &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )]
        }
        ChunkKind::Histogram => {
            let mut queries = vec![(
                sample.kind,
                promql_exact_selector(&format!("{metric_name}_count"), &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )];
            if let Some(le) = &sample.bucket_le {
                queries.push((
                    sample.kind,
                    promql_exact_selector(
                        &format!("{metric_name}_bucket"),
                        &sample.labels,
                        Some(("le", le.as_str())),
                    ),
                    query_start_ms,
                    query_end_ms,
                ));
            }
            queries
        }
        ChunkKind::ExponentialHistogram => {
            let mut queries = vec![(
                sample.kind,
                promql_exact_selector(&format!("{metric_name}_count"), &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )];
            if let Some(le) = &sample.bucket_le {
                queries.push((
                    sample.kind,
                    promql_exact_selector(
                        &format!("{metric_name}_bucket"),
                        &sample.labels,
                        Some(("le", le.as_str())),
                    ),
                    query_start_ms,
                    query_end_ms,
                ));
            }
            queries
        }
        ChunkKind::Summary => {
            let mut queries = vec![(
                sample.kind,
                promql_exact_selector(&format!("{metric_name}_count"), &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )];
            if let Some(quantile) = &sample.quantile {
                queries.push((
                    sample.kind,
                    promql_exact_selector(
                        metric_name,
                        &sample.labels,
                        Some(("quantile", quantile.as_str())),
                    ),
                    query_start_ms,
                    query_end_ms,
                ));
            }
            queries
        }
    }
}

fn sample_metric_name(sample: &SegmentStoreSmokeSeries) -> Option<&str> {
    sample
        .labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
}

fn promql_exact_selector(
    metric_name: &str,
    labels: &[(String, String)],
    extra_label: Option<(&str, &str)>,
) -> String {
    let mut matchers = Vec::with_capacity(labels.len() + 1 + usize::from(extra_label.is_some()));
    matchers.push(format!(
        r#"{}="{}""#,
        METRIC_NAME_LABEL,
        promql_escape_string(metric_name)
    ));
    for (key, value) in labels {
        if key == METRIC_NAME_LABEL || extra_label.is_some_and(|(extra_key, _)| extra_key == key) {
            continue;
        }
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    if let Some((key, value)) = extra_label {
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    format!("{{{}}}", matchers.join(","))
}

fn promql_escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}

fn smoke_query_limits() -> QueryLimits {
    QueryLimits {
        max_matched_series: Some(8),
        max_projected_series: Some(128),
        max_chunk_reads: Some(64),
        max_bytes_read: Some(16 * 1024 * 1024),
        max_samples_decoded: Some(4096),
        max_regex_values_examined: Some(0),
    }
}

fn smoke_query_error(query: &str, err: PromqlQueryError) -> io::Error {
    io::Error::other(format!("smoke query failed: {query}: {err}"))
}

fn encode_segment_footer(footer: &SegmentFooter) -> io::Result<Vec<u8>> {
    let file_count = u16::try_from(footer.files.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment footer file count exceeds u16",
        )
    })?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&file_count.to_le_bytes());
    payload.extend_from_slice(&0u16.to_le_bytes());

    for file in &footer.files {
        let file_id = segment_footer_file_id(file.file)?;
        payload.extend_from_slice(&file_id.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&file.size.to_le_bytes());
        payload.extend_from_slice(&file.checksum_xxh64.to_le_bytes());
    }

    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment footer payload length exceeds u64",
        )
    })?;
    let mut header = [0u8; SEGMENT_FOOTER_HEADER_LEN];
    header[0..4].copy_from_slice(&SEGMENT_FOOTER_MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&SEGMENT_FOOTER_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&footer.schema_version.to_le_bytes());
    header[8..16].copy_from_slice(&payload_len.to_le_bytes());

    let mut out =
        Vec::with_capacity(SEGMENT_FOOTER_HEADER_LEN + payload.len() + SEGMENT_FOOTER_TRAILER_LEN);
    out.extend_from_slice(&header);
    out.extend_from_slice(&payload);
    out.extend_from_slice(&segment_footer_crc(&header, &payload).to_le_bytes());
    Ok(out)
}

fn write_segment_footer(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    let segment_dir = segment_dir.as_ref();
    let footer = build_segment_footer(segment_dir)?;
    fs::write(
        segment_dir.join(SegmentFile::Footer.filename()),
        encode_segment_footer(&footer)?,
    )
}

fn read_segment_footer(segment_dir: impl AsRef<Path>) -> io::Result<SegmentFooter> {
    let bytes = fs::read(segment_dir.as_ref().join(SegmentFile::Footer.filename()))?;
    decode_segment_footer(&bytes)
}

fn validate_segment_footer(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    let segment_dir = segment_dir.as_ref();
    let footer = read_segment_footer(segment_dir)?;
    let mut seen = Vec::with_capacity(footer.files.len());

    for expected in &footer.files {
        if seen.contains(&expected.file) {
            return Err(invalid_segment_data("duplicate segment footer file entry"));
        }
        seen.push(expected.file);

        let actual = segment_footer_file(segment_dir, expected.file)?;
        if actual.size != expected.size || actual.checksum_xxh64 != expected.checksum_xxh64 {
            return Err(invalid_segment_data(
                "segment footer file size or checksum mismatch",
            ));
        }
    }

    for expected in SEGMENT_FOOTER_TRACKED_FILES {
        if !seen.contains(&expected) {
            return Err(invalid_segment_data("segment footer missing tracked file"));
        }
    }

    Ok(())
}

fn build_segment_footer(segment_dir: &Path) -> io::Result<SegmentFooter> {
    let mut files = Vec::with_capacity(SEGMENT_FOOTER_TRACKED_FILES.len());
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        files.push(segment_footer_file(segment_dir, file)?);
    }
    Ok(SegmentFooter {
        schema_version: SEGMENT_SCHEMA_VERSION,
        files,
    })
}

fn segment_footer_file(segment_dir: &Path, file: SegmentFile) -> io::Result<SegmentFooterFile> {
    let bytes = fs::read(segment_dir.join(file.filename()))?;
    let size = u64::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "segment file size exceeds u64"))?;
    Ok(SegmentFooterFile {
        file,
        size,
        checksum_xxh64: xxhash64(&bytes),
    })
}

fn decode_segment_footer(bytes: &[u8]) -> io::Result<SegmentFooter> {
    if bytes.len() < SEGMENT_FOOTER_HEADER_LEN + SEGMENT_FOOTER_TRAILER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment footer truncated",
        ));
    }
    let header: [u8; SEGMENT_FOOTER_HEADER_LEN] =
        bytes[0..SEGMENT_FOOTER_HEADER_LEN].try_into().unwrap();

    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != SEGMENT_FOOTER_MAGIC {
        return Err(invalid_segment_data("invalid segment footer magic"));
    }
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != SEGMENT_FOOTER_VERSION {
        return Err(invalid_segment_data("unsupported segment footer version"));
    }
    let schema_version = u16::from_le_bytes(header[6..8].try_into().unwrap());
    if schema_version != SEGMENT_SCHEMA_VERSION {
        return Err(invalid_segment_data(
            "unsupported segment footer schema version",
        ));
    }
    let payload_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let payload_len = usize::try_from(payload_len).map_err(|_| {
        invalid_segment_data("segment footer payload length exceeds platform usize")
    })?;
    let expected_len = SEGMENT_FOOTER_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(SEGMENT_FOOTER_TRAILER_LEN))
        .ok_or_else(|| invalid_segment_data("segment footer length overflow"))?;
    if bytes.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment footer truncated",
        ));
    }
    if bytes.len() != expected_len {
        return Err(invalid_segment_data("segment footer has trailing bytes"));
    }

    let payload = &bytes[SEGMENT_FOOTER_HEADER_LEN..SEGMENT_FOOTER_HEADER_LEN + payload_len];
    let expected_crc = u32::from_le_bytes(
        bytes[SEGMENT_FOOTER_HEADER_LEN + payload_len..][..SEGMENT_FOOTER_TRAILER_LEN]
            .try_into()
            .unwrap(),
    );
    let actual_crc = segment_footer_crc(&header, payload);
    if expected_crc != actual_crc {
        return Err(invalid_segment_data("segment footer checksum mismatch"));
    }

    let mut cursor = 0usize;
    let file_count = footer_read_u16(payload, &mut cursor)? as usize;
    let _reserved = footer_read_u16(payload, &mut cursor)?;
    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        let file_id = footer_read_u16(payload, &mut cursor)?;
        let _reserved = footer_read_u16(payload, &mut cursor)?;
        let size = footer_read_u64(payload, &mut cursor)?;
        let checksum_xxh64 = footer_read_u64(payload, &mut cursor)?;
        files.push(SegmentFooterFile {
            file: segment_file_from_footer_id(file_id)?,
            size,
            checksum_xxh64,
        });
    }
    if cursor != payload.len() {
        return Err(invalid_segment_data(
            "segment footer payload has trailing bytes",
        ));
    }

    Ok(SegmentFooter {
        schema_version,
        files,
    })
}

fn segment_footer_file_id(file: SegmentFile) -> io::Result<u16> {
    match file {
        SegmentFile::MetaJson => Ok(1),
        SegmentFile::Symbols => Ok(2),
        SegmentFile::Series => Ok(3),
        SegmentFile::Chunks => Ok(4),
        SegmentFile::OooChunks => Ok(5),
        SegmentFile::ChunkIndex => Ok(6),
        SegmentFile::Indexes => Ok(7),
        SegmentFile::Footer => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment footer cannot describe itself",
        )),
    }
}

fn segment_file_from_footer_id(file_id: u16) -> io::Result<SegmentFile> {
    match file_id {
        1 => Ok(SegmentFile::MetaJson),
        2 => Ok(SegmentFile::Symbols),
        3 => Ok(SegmentFile::Series),
        4 => Ok(SegmentFile::Chunks),
        5 => Ok(SegmentFile::OooChunks),
        6 => Ok(SegmentFile::ChunkIndex),
        7 => Ok(SegmentFile::Indexes),
        _ => Err(invalid_segment_data("unknown segment footer file id")),
    }
}

fn segment_footer_crc(header: &[u8; SEGMENT_FOOTER_HEADER_LEN], payload: &[u8]) -> u32 {
    crc32c_append(crc32c(header), payload)
}

fn invalid_segment_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn footer_read_bytes<'a>(buf: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "segment footer truncated"))?;
    if end > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment footer truncated",
        ));
    }
    let bytes = &buf[*cursor..end];
    *cursor = end;
    Ok(bytes)
}

fn footer_read_array<const N: usize>(buf: &[u8], cursor: &mut usize) -> io::Result<[u8; N]> {
    let bytes = footer_read_bytes(buf, cursor, N)?;
    Ok(bytes.try_into().unwrap())
}

fn footer_read_u16(buf: &[u8], cursor: &mut usize) -> io::Result<u16> {
    Ok(u16::from_le_bytes(footer_read_array(buf, cursor)?))
}

fn footer_read_u64(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(footer_read_array(buf, cursor)?))
}

fn xxhash64(input: &[u8]) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;
    const P3: u64 = 1_609_587_929_392_839_161;
    const P4: u64 = 9_650_029_242_287_828_579;
    const P5: u64 = 2_870_177_450_012_600_261;

    let mut cursor = 0usize;
    let mut h64;

    if input.len() >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0;
        let mut v4 = 0u64.wrapping_sub(P1);

        while cursor + 32 <= input.len() {
            v1 = xxh64_round(v1, xxh64_read_u64(input, cursor));
            cursor += 8;
            v2 = xxh64_round(v2, xxh64_read_u64(input, cursor));
            cursor += 8;
            v3 = xxh64_round(v3, xxh64_read_u64(input, cursor));
            cursor += 8;
            v4 = xxh64_round(v4, xxh64_read_u64(input, cursor));
            cursor += 8;
        }

        h64 = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h64 = xxh64_merge_round(h64, v1);
        h64 = xxh64_merge_round(h64, v2);
        h64 = xxh64_merge_round(h64, v3);
        h64 = xxh64_merge_round(h64, v4);
    } else {
        h64 = P5;
    }

    h64 = h64.wrapping_add(input.len() as u64);

    while cursor + 8 <= input.len() {
        let k1 = xxh64_round(0, xxh64_read_u64(input, cursor));
        h64 ^= k1;
        h64 = h64.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        cursor += 8;
    }

    if cursor + 4 <= input.len() {
        h64 ^= u64::from(xxh64_read_u32(input, cursor)).wrapping_mul(P1);
        h64 = h64.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        cursor += 4;
    }

    while cursor < input.len() {
        h64 ^= u64::from(input[cursor]).wrapping_mul(P5);
        h64 = h64.rotate_left(11).wrapping_mul(P1);
        cursor += 1;
    }

    h64 ^= h64 >> 33;
    h64 = h64.wrapping_mul(P2);
    h64 ^= h64 >> 29;
    h64 = h64.wrapping_mul(P3);
    h64 ^ (h64 >> 32)
}

fn xxh64_round(acc: u64, input: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;

    acc.wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

fn xxh64_merge_round(acc: u64, value: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P4: u64 = 9_650_029_242_287_828_579;

    (acc ^ xxh64_round(0, value))
        .wrapping_mul(P1)
        .wrapping_add(P4)
}

fn xxh64_read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().unwrap())
}

fn xxh64_read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().unwrap())
}

fn sort_segment_readers(segments: &mut [SegmentReader]) {
    segments.sort_by(|left, right| {
        left.meta
            .start_ms
            .cmp(&right.meta.start_ms)
            .then_with(|| left.meta.end_ms.cmp(&right.meta.end_ms))
            .then_with(|| left.meta.segment_id.cmp(&right.meta.segment_id))
    });
}

fn validate_manifest_segment_meta(
    manifest_segment: &ManifestSegment,
    meta: &SegmentMeta,
) -> io::Result<()> {
    if meta.segment_id != manifest_segment.segment_id
        || meta.start_ms != manifest_segment.start_ms
        || meta.end_ms != manifest_segment.end_ms
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest segment does not match segment meta.json",
        ));
    }
    Ok(())
}

fn storage_selectors_from_promql_with_projection_config(
    selector: PromqlSelector,
    query_projection_config: &QueryProjectionConfig,
) -> Result<Vec<SegmentSelector>, PromqlQueryError> {
    if let Some(metric_name) = selector.metric_name.as_deref() {
        if let Some(native) = metric_name.strip_suffix("_count") {
            return Ok(vec![
                storage_selector_from_promql_parts(
                    Some(metric_name.to_string()),
                    selector.matchers.clone(),
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(
                    Some(native.to_string()),
                    selector.matchers,
                    SegmentProjection::Count,
                )?,
            ]);
        }
        if let Some(native) = metric_name.strip_suffix("_sum") {
            return Ok(vec![
                storage_selector_from_promql_parts(
                    Some(metric_name.to_string()),
                    selector.matchers.clone(),
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(
                    Some(native.to_string()),
                    selector.matchers,
                    SegmentProjection::Sum,
                )?,
            ]);
        }
    }

    if selector.metric_name.is_none()
        && let Some((idx, metric_name)) = exact_metric_name_matcher(&selector.matchers)
    {
        if let Some(native) = metric_name.strip_suffix("_count") {
            let mut native_matchers = selector.matchers.clone();
            native_matchers[idx].value = native.to_string();
            return Ok(vec![
                storage_selector_from_promql_parts(
                    None,
                    selector.matchers,
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(
                    None,
                    native_matchers,
                    SegmentProjection::Count,
                )?,
            ]);
        }
        if let Some(native) = metric_name.strip_suffix("_sum") {
            let mut native_matchers = selector.matchers.clone();
            native_matchers[idx].value = native.to_string();
            return Ok(vec![
                storage_selector_from_promql_parts(
                    None,
                    selector.matchers,
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(None, native_matchers, SegmentProjection::Sum)?,
            ]);
        }
    }

    if selector.metric_name.is_none()
        && let Some(projection) = metric_name_regex_projection(&selector.matchers)
    {
        return Ok(vec![
            storage_selector_from_promql_parts(
                None,
                selector.matchers.clone(),
                SegmentProjection::None,
            )?,
            storage_selector_from_promql_parts(None, selector.matchers, projection)?,
        ]);
    }

    storage_selector_from_promql_with_projection_config(selector, query_projection_config)
        .map(|selector| vec![selector])
}

fn storage_selector_from_promql_with_projection_config(
    selector: PromqlSelector,
    query_projection_config: &QueryProjectionConfig,
) -> Result<SegmentSelector, PromqlQueryError> {
    let mut metric_name = selector.metric_name;
    let mut promql_matchers = selector.matchers;
    let mut projection = SegmentProjection::None;

    if let Some(name) = metric_name.as_deref() {
        if let Some(native) = name.strip_suffix("_bucket") {
            let le = take_virtual_eq_matcher(&mut promql_matchers, "le")?;
            metric_name = Some(native.to_string());
            projection = SegmentProjection::HistogramBucket {
                le,
                exponential_histogram_boundaries: query_projection_config
                    .exponential_histogram_bucket_boundaries()
                    .to_vec(),
            };
        } else if let Some(native) = name.strip_suffix("_count") {
            metric_name = Some(native.to_string());
            projection = SegmentProjection::Count;
        } else if let Some(native) = name.strip_suffix("_sum") {
            metric_name = Some(native.to_string());
            projection = SegmentProjection::Sum;
        }
    }

    if matches!(projection, SegmentProjection::None) {
        if let Some(quantile) = take_virtual_eq_matcher(&mut promql_matchers, "quantile")? {
            projection = SegmentProjection::SummaryQuantile {
                quantile: Some(quantile),
            };
        } else {
            projection = SegmentProjection::AllPromql {
                exponential_histogram_boundaries: query_projection_config
                    .exponential_histogram_bucket_boundaries()
                    .to_vec(),
            };
        }
    }

    storage_selector_from_promql_parts(metric_name, promql_matchers, projection)
}

fn storage_selector_from_promql_parts(
    metric_name: Option<String>,
    promql_matchers: Vec<crate::promql::PromqlMatcher>,
    projection: SegmentProjection,
) -> Result<SegmentSelector, PromqlQueryError> {
    let matchers = label_matchers_from_promql(promql_matchers)?;

    let storage_selector = match metric_name {
        Some(metric_name) => SegmentSelector::with_metric(metric_name, matchers),
        None => SegmentSelector::new(matchers),
    };
    Ok(storage_selector.with_projection(projection))
}

fn label_matchers_from_promql(
    promql_matchers: Vec<crate::promql::PromqlMatcher>,
) -> Result<Vec<LabelMatcher>, PromqlQueryError> {
    let mut matchers = Vec::with_capacity(promql_matchers.len());
    for matcher in promql_matchers {
        match matcher.op {
            PromqlMatcherOp::Eq => {
                matchers.push(LabelMatcher::eq(matcher.name, matcher.value));
            }
            PromqlMatcherOp::NotEq => {
                matchers.push(LabelMatcher::not_eq(matcher.name, matcher.value));
            }
            PromqlMatcherOp::Regex => {
                compile_promql_regex(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid regex matcher: {err}"))
                })?;
                matchers.push(LabelMatcher::regex(matcher.name, matcher.value));
            }
            PromqlMatcherOp::NotRegex => {
                compile_promql_regex(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid regex matcher: {err}"))
                })?;
                matchers.push(LabelMatcher::not_regex(matcher.name, matcher.value));
            }
        }
    }
    Ok(matchers)
}

fn exact_metric_name_matcher(matchers: &[crate::promql::PromqlMatcher]) -> Option<(usize, &str)> {
    matchers.iter().enumerate().find_map(|(idx, matcher)| {
        (matcher.name == METRIC_NAME_LABEL && matcher.op == PromqlMatcherOp::Eq)
            .then_some((idx, matcher.value.as_str()))
    })
}

fn metric_name_regex_projection(
    matchers: &[crate::promql::PromqlMatcher],
) -> Option<SegmentProjection> {
    matchers.iter().find_map(|matcher| {
        if matcher.name != METRIC_NAME_LABEL || matcher.op != PromqlMatcherOp::Regex {
            return None;
        }
        metric_name_regex_projection_suffix(&matcher.value).map(|suffix| match suffix {
            "_count" => SegmentProjection::Count,
            "_sum" => SegmentProjection::Sum,
            _ => unreachable!("unsupported projection suffix"),
        })
    })
}

fn metric_name_regex_projection_suffix(pattern: &str) -> Option<&'static str> {
    let pattern = pattern.strip_suffix('$').unwrap_or(pattern);
    if pattern.ends_with("_count") {
        Some("_count")
    } else if pattern.ends_with("_sum") {
        Some("_sum")
    } else {
        None
    }
}

fn take_virtual_eq_matcher(
    matchers: &mut Vec<crate::promql::PromqlMatcher>,
    label_name: &str,
) -> Result<Option<String>, PromqlQueryError> {
    let mut value = None;
    let mut retained = Vec::with_capacity(matchers.len());
    for matcher in matchers.drain(..) {
        if matcher.name != label_name {
            retained.push(matcher);
            continue;
        }
        if matcher.op != PromqlMatcherOp::Eq {
            return Err(PromqlQueryError::Unsupported(format!(
                "{label_name} projection matcher currently supports only equality"
            )));
        }
        if value.replace(matcher.value).is_some() {
            return Err(PromqlQueryError::Invalid(format!(
                "duplicate {label_name} projection matcher"
            )));
        }
    }
    *matchers = retained;
    Ok(value)
}

fn regex_postings(
    name: &str,
    pattern: &str,
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
    match_promql_projection_names: bool,
) -> io::Result<Vec<u32>> {
    let regex = compile_promql_regex(pattern)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let Some(name_sym) = symbols.lookup(name) else {
        return Ok(Vec::new());
    };
    if !label_name_overlaps_range(index_reader, name_sym, start_ms, end_ms) {
        return Ok(Vec::new());
    }

    let ranges = index_reader
        .label_value_time_ranges(name_sym)?
        .map(|ranges| ranges.into_iter().collect::<BTreeMap<_, _>>());

    let mut out = Vec::new();
    for value in regex_label_values(
        index_reader,
        name_sym,
        pattern,
        match_promql_projection_names,
    )? {
        budget.observe_regex_value()?;
        let matches = if match_promql_projection_names {
            promql_projection_metric_name_matches(&value, &regex)
        } else {
            regex.is_match(&value)
        };
        if !matches {
            continue;
        }
        let value_sym = symbols.lookup(&value).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
        })?;
        if let Some(ranges) = &ranges
            && !ranges
                .get(&value_sym)
                .is_some_and(|range| range.overlaps(start_ms, end_ms))
        {
            continue;
        }
        let Some(postings) = index_reader.exact_postings_metadata(name_sym, value_sym) else {
            continue;
        };
        if let Some(posting) =
            exact_postings_with_budget(index_reader, name_sym, value_sym, postings, budget)?
        {
            out = union_sorted(&out, &posting);
        }
    }

    Ok(out)
}

fn regex_label_values(
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    name_sym: u32,
    pattern: &str,
    match_promql_projection_names: bool,
) -> io::Result<Vec<String>> {
    let prefixes = regex_literal_prefixes(pattern, match_promql_projection_names);
    if prefixes.is_empty() {
        return index_reader.label_values_with_prefix(name_sym, None);
    }

    let mut values = BTreeSet::new();
    for prefix in prefixes {
        values.extend(index_reader.label_values_with_prefix(name_sym, Some(&prefix))?);
    }
    Ok(values.into_iter().collect())
}

fn regex_literal_prefixes(pattern: &str, match_promql_projection_names: bool) -> Vec<String> {
    let Some(prefix) = regex_literal_prefix(pattern) else {
        return Vec::new();
    };

    let mut prefixes = BTreeSet::from([prefix.clone()]);
    if match_promql_projection_names {
        for suffix in PROMQL_PROJECTION_SUFFIXES {
            if let Some(base_prefix) = prefix.strip_suffix(suffix)
                && !base_prefix.is_empty()
            {
                prefixes.insert(base_prefix.to_string());
            }
        }
    }
    prefixes.into_iter().collect()
}

fn regex_literal_prefix(pattern: &str) -> Option<String> {
    let mut chars = pattern.chars().peekable();
    if matches!(chars.peek(), Some('^')) {
        chars.next();
    }

    let mut prefix = String::new();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let escaped = chars.next()?;
                if is_regex_literal_escape(escaped) {
                    prefix.push(escaped);
                } else {
                    break;
                }
            }
            '.' | '*' | '+' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                break;
            }
            ch => prefix.push(ch),
        }
    }

    (!prefix.is_empty()).then_some(prefix)
}

fn is_regex_literal_escape(ch: char) -> bool {
    matches!(
        ch,
        '\\' | '.' | '*' | '+' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '-'
    )
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

fn union_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() || ri < right.len() {
        if li >= left.len() {
            out.extend_from_slice(&right[ri..]);
            break;
        }
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(right[ri]);
                ri += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

fn subtract_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() {
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

fn merge_query_results(results: Vec<SegmentQueryResult>) -> Vec<SegmentQueryResult> {
    let mut merged: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
    for result in results {
        let entry = merged
            .entry(result.series_id)
            .or_insert_with(|| SegmentQueryResult::new(result.series_id, result.labels.clone()));
        entry.extend_from(result);
    }

    let mut results: Vec<_> = merged.into_values().collect();
    for result in &mut results {
        result.dedupe_samples_keep_last();
    }
    results
}

fn segment_window(timestamp_ms: u64, duration_ms: u64) -> (u64, u64) {
    let start_ms = timestamp_ms.saturating_sub(timestamp_ms % duration_ms);
    (start_ms, start_ms.saturating_add(duration_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkReader, ChunkSamples};
    use crate::storage::head::{
        ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
        SummaryQuantileValue, SummaryValue, TypedSampleMetadata,
    };
    use crate::storage::index::LabelValueTimeRange;
    use crate::storage::series::{
        SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_HISTOGRAM, SERIES_KIND_SUMMARY,
    };
    use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};

    const FRAME_HEADER_LEN: u64 = 14;

    #[test]
    fn query_budget_counts_unique_matched_series_once() {
        let mut budget = QueryBudget::new(QueryLimits {
            max_matched_series: Some(1),
            ..QueryLimits::unlimited()
        });

        budget.observe_matched_series(10).unwrap();
        budget.observe_matched_series(10).unwrap();
        assert_eq!(budget.stats().matched_series, 1);

        let err = budget.observe_matched_series(11).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::QuotaExceeded);
        let limit = query_limit_exceeded_from_io(&err).unwrap();
        assert_eq!(limit.limit, QueryLimit::MatchedSeries);
        assert_eq!(limit.max, 1);
    }

    #[test]
    fn query_budget_counts_unique_projected_series_once() {
        let mut budget = QueryBudget::new(QueryLimits {
            max_projected_series: Some(1),
            ..QueryLimits::unlimited()
        });

        budget.observe_projected_series(10).unwrap();
        budget.observe_projected_series(10).unwrap();
        assert_eq!(budget.stats().projected_series, 1);

        let err = budget.observe_projected_series(11).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::QuotaExceeded);
        let limit = query_limit_exceeded_from_io(&err).unwrap();
        assert_eq!(limit.limit, QueryLimit::ProjectedSeries);
        assert_eq!(limit.max, 1);
    }

    #[test]
    fn query_limits_production_default_matches_storage_spec_guardrails() {
        let limits = QueryLimits::production_default();

        assert_eq!(limits.max_matched_series, Some(1_000_000));
        assert_eq!(limits.max_projected_series, Some(2_000_000));
        assert_eq!(limits.max_chunk_reads, Some(5_000_000));
        assert_eq!(limits.max_bytes_read, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(limits.max_samples_decoded, Some(50_000_000));
        assert_eq!(limits.max_regex_values_examined, Some(100_000));
    }

    #[test]
    fn query_budget_rejects_chunk_byte_sample_and_regex_limits() {
        let mut budget = QueryBudget::new(QueryLimits {
            max_chunk_reads: Some(0),
            ..QueryLimits::unlimited()
        });
        let err = budget.observe_chunk_read(1).unwrap_err();
        let limit = query_limit_exceeded_from_io(&err).unwrap();
        assert_eq!(limit.limit, QueryLimit::ChunkReads);
        assert_eq!(limit.max, 0);

        let mut budget = QueryBudget::new(QueryLimits {
            max_bytes_read: Some(4),
            ..QueryLimits::unlimited()
        });
        let err = budget.observe_chunk_read(5).unwrap_err();
        let limit = query_limit_exceeded_from_io(&err).unwrap();
        assert_eq!(limit.limit, QueryLimit::BytesRead);
        assert_eq!(limit.max, 4);

        let mut budget = QueryBudget::new(QueryLimits {
            max_samples_decoded: Some(1),
            ..QueryLimits::unlimited()
        });
        let err = budget.observe_samples_decoded(2).unwrap_err();
        let limit = query_limit_exceeded_from_io(&err).unwrap();
        assert_eq!(limit.limit, QueryLimit::SamplesDecoded);
        assert_eq!(limit.max, 1);

        let mut budget = QueryBudget::new(QueryLimits {
            max_regex_values_examined: Some(0),
            ..QueryLimits::unlimited()
        });
        let err = budget.observe_regex_value().unwrap_err();
        let limit = query_limit_exceeded_from_io(&err).unwrap();
        assert_eq!(limit.limit, QueryLimit::RegexValuesExamined);
        assert_eq!(limit.max, 0);
    }

    #[test]
    fn regex_literal_prefix_extracts_only_safe_prefixes() {
        assert_eq!(
            regex_literal_prefix("go_gc_duration_seconds.*"),
            Some("go_gc_duration_seconds".to_string())
        );
        assert_eq!(
            regex_literal_prefix("^rpc_duration.*_count"),
            Some("rpc_duration".to_string())
        );
        assert_eq!(
            regex_literal_prefix(r"http\.request\..*"),
            Some("http.request.".to_string())
        );
        assert_eq!(regex_literal_prefix(".*_count"), None);
        assert_eq!(regex_literal_prefix("[a-z].*"), None);
        assert_eq!(regex_literal_prefix(r"\d+"), None);
        assert_eq!(
            regex_literal_prefixes("rpc_duration_count", true),
            vec!["rpc_duration".to_string(), "rpc_duration_count".to_string()]
        );
        assert_eq!(
            regex_literal_prefixes("rpc_duration_count", false),
            vec!["rpc_duration_count".to_string()]
        );
    }

    #[test]
    fn metadata_accumulator_sorts_dedupes_and_tracks_metric_names() {
        let mut metadata = MetadataAccumulator::default();
        metadata.add_labelset(&[
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            ("pod_name".to_string(), "backend-2".to_string()),
        ]);
        metadata.add_labelset(&[
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            ("pod_name".to_string(), "backend-1".to_string()),
            ("namespace".to_string(), "default".to_string()),
        ]);

        assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
        assert_eq!(
            metadata.label_names(),
            vec![
                METRIC_NAME_LABEL.to_string(),
                "namespace".to_string(),
                "pod_name".to_string()
            ]
        );
        assert_eq!(
            metadata.label_values("pod_name"),
            vec!["backend-1".to_string(), "backend-2".to_string()]
        );
    }

    #[test]
    fn metric_name_index_collection_reads_only_metric_name_values() {
        let mut symbols = SegmentSymbols::default();
        let metric = symbols.intern(METRIC_NAME_LABEL);
        let cpu = symbols.intern("cpu_usage");
        let pod = symbols.intern("pod_name");
        let backend = symbols.intern("backend-1");
        let series = vec![SeriesEntry {
            series_id: 1,
            kind_mask: SERIES_KIND_FLOAT,
            labels: vec![(metric, cpu), (pod, backend)],
        }];
        let mut label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
        label_values.insert_fst(pod, b"not an fst".to_vec());
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values,
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            routing_index: None,
        };
        let mut index_reader = index_reader_for(&indexes);
        let mut metadata = MetadataAccumulator::default();

        collect_metric_names_from_index(&symbols, &mut index_reader, 0, 10_000, &mut metadata)
            .unwrap();

        assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
    }

    #[test]
    fn label_value_index_collection_reads_only_requested_label_values() {
        let mut symbols = SegmentSymbols::default();
        let metric = symbols.intern(METRIC_NAME_LABEL);
        let cpu = symbols.intern("cpu_usage");
        let pod = symbols.intern("pod_name");
        let backend = symbols.intern("backend-1");
        let series = vec![SeriesEntry {
            series_id: 1,
            kind_mask: SERIES_KIND_FLOAT,
            labels: vec![(metric, cpu), (pod, backend)],
        }];
        let mut label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
        label_values.insert_fst(metric, b"not an fst".to_vec());
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values,
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            routing_index: None,
        };
        let mut index_reader = index_reader_for(&indexes);
        let mut metadata = MetadataAccumulator::default();

        collect_label_values_from_index(
            &symbols,
            &mut index_reader,
            "pod_name",
            0,
            10_000,
            &mut metadata,
        )
        .unwrap();

        assert_eq!(
            metadata.label_values("pod_name"),
            vec!["backend-1".to_string()]
        );
    }

    fn index_reader_for(indexes: &SegmentIndexes) -> SegmentIndexReader<Cursor<Vec<u8>>> {
        let mut bytes = Vec::new();
        write_segment_indexes(&mut bytes, indexes).unwrap();
        SegmentIndexReader::open(Cursor::new(bytes)).unwrap()
    }

    fn read_chunk_encoding(file: &mut File) -> u8 {
        file.seek(SeekFrom::Start(FRAME_HEADER_LEN + 1))
            .expect("seek to encoding");
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf).expect("read encoding");
        buf[0]
    }

    fn resolved_entry_labels(
        symbols: &SegmentSymbols,
        entry: &SeriesEntry,
    ) -> Vec<(String, String)> {
        entry
            .labels
            .iter()
            .map(|(key, value)| {
                (
                    symbols.resolve(*key).unwrap().to_string(),
                    symbols.resolve(*value).unwrap().to_string(),
                )
            })
            .collect()
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
    fn segment_footer_roundtrips_file_metadata() {
        let footer = SegmentFooter {
            schema_version: SEGMENT_SCHEMA_VERSION,
            files: vec![
                SegmentFooterFile {
                    file: SegmentFile::MetaJson,
                    size: 128,
                    checksum_xxh64: 0x1122_3344_5566_7788,
                },
                SegmentFooterFile {
                    file: SegmentFile::Chunks,
                    size: 4096,
                    checksum_xxh64: 0x8877_6655_4433_2211,
                },
            ],
        };

        let bytes = encode_segment_footer(&footer).unwrap();
        let decoded = decode_segment_footer(&bytes).unwrap();

        assert_eq!(decoded, footer);
    }

    #[test]
    fn segment_footer_rejects_bad_crc32c() {
        let footer = SegmentFooter {
            schema_version: SEGMENT_SCHEMA_VERSION,
            files: vec![SegmentFooterFile {
                file: SegmentFile::MetaJson,
                size: 128,
                checksum_xxh64: 0x1122_3344_5566_7788,
            }],
        };
        let mut bytes = encode_segment_footer(&footer).unwrap();
        bytes[SEGMENT_FOOTER_HEADER_LEN] ^= 0xff;

        let err = decode_segment_footer(&bytes).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn segment_footer_validation_rejects_tracked_file_corruption() {
        let tempdir = tempfile::tempdir().unwrap();
        write_footer_test_files(tempdir.path());
        write_segment_footer(tempdir.path()).unwrap();
        validate_segment_footer(tempdir.path()).unwrap();

        let symbols_path = tempdir.path().join(SegmentFile::Symbols.filename());
        let mut symbols = fs::read(&symbols_path).unwrap();
        symbols[0] ^= 0xff;
        fs::write(symbols_path, symbols).unwrap();
        let err = validate_segment_footer(tempdir.path()).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    fn write_footer_test_files(dir: &Path) {
        for file in SEGMENT_FOOTER_TRACKED_FILES {
            fs::write(
                dir.join(file.filename()),
                format!("content:{}", file.filename()),
            )
            .unwrap();
        }
    }

    #[test]
    fn manifest_segment_meta_accepts_matching_meta() {
        let id = SegmentId::with_ulid(100, 200, Ulid::new()).unwrap();
        let manifest_segment =
            crate::storage::manifest::ManifestSegment::new(id.dir_name(), 100, 200, Some(42))
                .unwrap();
        let meta = SegmentMeta {
            segment_id: id.dir_name(),
            start_ms: 100,
            end_ms: 200,
            datapoints: 3,
            series: 1,
            chunk_summary: None,
        };

        validate_manifest_segment_meta(&manifest_segment, &meta).unwrap();
    }

    #[test]
    fn manifest_segment_meta_rejects_mismatched_meta_json() {
        let id = SegmentId::with_ulid(100, 200, Ulid::new()).unwrap();
        let manifest_segment =
            crate::storage::manifest::ManifestSegment::new(id.dir_name(), 100, 200, Some(42))
                .unwrap();
        let meta = SegmentMeta {
            segment_id: id.dir_name(),
            start_ms: 100,
            end_ms: 201,
            datapoints: 3,
            series: 1,
            chunk_summary: None,
        };

        let err = validate_manifest_segment_meta(&manifest_segment, &meta).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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
        assert!(!seg_dir.join("routing_index.bin").exists());
        assert!(seg_dir.join("footer.bin").exists());
        let chunk_len = fs::metadata(seg_dir.join("chunks.bin")).unwrap().len();
        assert!(chunk_len > 0);
        let index_len = fs::metadata(seg_dir.join("chunk_index.bin")).unwrap().len();
        assert!(index_len > 0);
    }

    #[test]
    fn segment_writer_persists_chunk_summary_in_meta() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(
                    2_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(3.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.duration");
                },
            )
            .unwrap();
        writer.flush().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        let meta: SegmentMeta =
            serde_json::from_slice(&fs::read(seg_dir.join("meta.json")).unwrap()).unwrap();
        let summary = meta.chunk_summary.expect("chunk summary");

        assert_eq!(summary.chunks, 2);
        assert_eq!(summary.by_kind.float.chunks, 1);
        assert_eq!(summary.by_kind.histogram.chunks, 1);
        assert!(summary.chunk_bytes > 0);
        assert!(summary.by_kind.float.chunk_bytes > 0);
        assert!(summary.by_kind.histogram.chunk_bytes > 0);
    }

    #[test]
    fn smoke_verify_uses_chunk_summary_for_totals_without_chunk_scan_when_not_sampling() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
        writer.flush().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        fs::remove_file(seg_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
        fs::remove_file(seg_dir.join(SegmentFile::Chunks.filename())).unwrap();

        let store = SegmentStoreReader::open(tempdir.path()).unwrap();
        let report = store.smoke_verify(0, 10_000, 0).unwrap();

        assert_eq!(report.totals.segments, 1);
        assert_eq!(report.totals.chunks, 1);
        assert_eq!(report.totals.by_kind.float.chunks, 1);
        assert!(report.sample_series.is_empty());
        assert!(report.queries.is_empty());
    }

    #[test]
    fn deterministic_segment_id_provider_replays_same_sequence() {
        let first = DeterministicSegmentIdProvider::new(7);
        let first_id = first.next_segment_id(0, 10_000).unwrap();
        let second_id = first.next_segment_id(10_000, 20_000).unwrap();
        assert_ne!(first_id, second_id);

        let replay = DeterministicSegmentIdProvider::new(7);
        assert_eq!(replay.next_segment_id(0, 10_000).unwrap(), first_id);
        assert_eq!(replay.next_segment_id(10_000, 20_000).unwrap(), second_id);
    }

    #[test]
    fn segment_writer_with_deterministic_ids_replays_same_directory_names() {
        fn write_segments(path: &Path) -> Vec<String> {
            let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
                .with_deterministic_segment_ids(42);
            let mut writer = SegmentWriter::new(config).unwrap();

            writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
            writer
                .record_sample(SeriesRef::new(1), 11_000, 2.5)
                .unwrap();
            writer.flush().unwrap();

            let mut names: Vec<_> = fs::read_dir(path)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.file_name().to_string_lossy().to_string())
                .filter(|name| name.starts_with("seg-"))
                .collect();
            names.sort();
            names
        }

        let first = tempfile::tempdir().unwrap();
        let replay = tempfile::tempdir().unwrap();

        let first_names = write_segments(first.path());
        let replay_names = write_segments(replay.path());

        assert_eq!(first_names.len(), 2);
        assert_eq!(first_names, replay_names);
    }

    #[test]
    fn segment_writer_records_flush_profile_stages() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();
        let labels = vec![
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            ("pod".to_string(), "backend-1".to_string()),
        ];

        assert!(writer.last_flush_profile().is_none());

        writer
            .record_samples_with_labels(SeriesRef::new(7), &labels, &[(1_000, 1.5), (2_000, 2.5)])
            .unwrap();
        writer.flush().unwrap();

        let profile = writer.last_flush_profile().unwrap();
        assert_eq!(profile.datapoints, 2);
        assert_eq!(profile.series, 1);
        assert_eq!(
            profile.stage_kinds(),
            &[
                SegmentFlushStageKind::MetaJson,
                SegmentFlushStageKind::ChunksFlush,
                SegmentFlushStageKind::ChunkIndex,
                SegmentFlushStageKind::SegmentMetadata,
                SegmentFlushStageKind::LabelValues,
                SegmentFlushStageKind::LabelValueTimeRanges,
                SegmentFlushStageKind::RoutingIndexBuild,
                SegmentFlushStageKind::Symbols,
                SegmentFlushStageKind::Series,
                SegmentFlushStageKind::Indexes,
                SegmentFlushStageKind::OooChunks,
                SegmentFlushStageKind::Footer,
                SegmentFlushStageKind::Publish,
            ]
        );
        assert!(
            profile
                .stage_elapsed(SegmentFlushStageKind::SegmentMetadata)
                .is_some()
        );
        assert!(
            profile.total
                >= profile
                    .stage_elapsed(SegmentFlushStageKind::Publish)
                    .unwrap()
        );
    }

    #[test]
    fn segment_writer_records_flush_profile_file_sizes() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();
        let labels = vec![
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            ("pod".to_string(), "backend-1".to_string()),
        ];

        writer
            .record_samples_with_labels(SeriesRef::new(7), &labels, &[(1_000, 1.5), (2_000, 2.5)])
            .unwrap();
        writer.flush().unwrap();

        let profile = writer.last_flush_profile().unwrap();
        for file in [
            SegmentFile::MetaJson,
            SegmentFile::Symbols,
            SegmentFile::Series,
            SegmentFile::Chunks,
            SegmentFile::OooChunks,
            SegmentFile::ChunkIndex,
            SegmentFile::Indexes,
            SegmentFile::Footer,
        ] {
            assert!(
                profile.file_size_bytes(file).is_some(),
                "missing file size for {}",
                file.filename()
            );
        }
        assert!(profile.file_size_bytes(SegmentFile::Chunks).unwrap() > 0);
        assert!(
            profile.total_file_bytes() >= profile.file_size_bytes(SegmentFile::Chunks).unwrap()
        );
        assert_eq!(
            profile.total_file_bytes(),
            profile.data_file_bytes()
                + profile.metadata_file_bytes()
                + profile.index_file_bytes()
                + profile.footer_file_bytes()
        );
    }

    #[test]
    fn segment_writer_reserves_active_window_series_structures() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.reserve_window_series(0, 10_000, 4_096).unwrap();

        let active = writer.active.as_ref().unwrap();
        assert!(active.series_map.capacity() >= 4_096);
        assert!(active.metadata_present.capacity() >= 4_096);
        assert!(active.series_entries.capacity() >= 4_096);
        assert!(active.chunk_entries.capacity() >= 4_096);
    }

    #[test]
    fn segment_writer_records_record_path_profile() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();
        let before = writer.record_profile();

        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(7),
                &[(1_000, 1.5), (2_000, 2.5)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "cpu_usage");
                    visit("pod", "backend-1");
                },
            )
            .unwrap();

        let delta = writer.record_profile().saturating_sub(before);
        assert_eq!(delta.chunks, 1);
        assert_eq!(delta.samples, 2);
        assert!(delta.total_elapsed() <= delta.wall_elapsed);
    }

    #[test]
    fn segment_series_metadata_builder_matches_raw_label_canonicalization() {
        let raw_labels = vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("namespace".to_string(), "default".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ];

        let canonical = crate::promql::canonicalize_labelset(
            "cpu.usage",
            &[("namespace", "default"), ("pod.name", "backend-1")],
        );
        let expected_series_id = crate::promql::series_id(&canonical);
        let expected_labels: Vec<_> = canonical
            .labels()
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect();

        let mut builder = SegmentSeriesMetadataBuilder::new();
        for (key, value) in &raw_labels {
            builder.push_label(key, value);
        }
        let metadata = builder.finish();

        assert_eq!(metadata.series_id, expected_series_id);
        assert_eq!(metadata.labels, expected_labels);
    }

    #[test]
    fn segment_series_metadata_builder_keeps_first_metric_name() {
        let raw_labels = vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.first".to_string()),
            (METRIC_NAME_LABEL.to_string(), "cpu.second".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ];

        let mut builder = SegmentSeriesMetadataBuilder::new();
        for (key, value) in &raw_labels {
            builder.push_label(key, value);
        }
        let metadata = builder.finish();

        assert!(metadata.labels.iter().any(|(key, value)| {
            key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.first")
        }));
        assert!(!metadata.labels.iter().any(|(key, value)| {
            key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.second")
        }));
    }

    #[test]
    fn label_visitor_encoder_matches_metadata_builder_canonicalization() {
        let raw_labels = [
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("pod.name", "backend-1"),
            ("namespace", "default"),
        ];
        let mut builder = SegmentSeriesMetadataBuilder::new();
        for (key, value) in raw_labels {
            builder.push_label(key, value);
        }
        let expected = builder.finish();

        let mut symbols = SegmentSymbols::default();
        let mut postings = ExactPostingsIndex::default();
        let entry = encode_label_visitor_metadata(&mut symbols, &mut postings, 0, |visit| {
            for (key, value) in raw_labels {
                visit(key, value);
            }
        });

        let labels = resolved_entry_labels(&symbols, &entry);
        assert_eq!(entry.series_id, expected.series_id);
        assert_eq!(labels, expected.labels);
    }

    #[test]
    fn borrowed_label_encoder_matches_owned_canonical_encoding() {
        let canonical = vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.usage"),
            ),
            (normalize_label_name("namespace"), "default".to_string()),
            (normalize_label_name("pod.name"), "backend-1".to_string()),
        ];

        let mut owned_symbols = SegmentSymbols::default();
        let mut owned_postings = ExactPostingsIndex::default();
        let owned = encode_canonical_segment_labels(
            canonical
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            &mut owned_symbols,
            &mut owned_postings,
            0,
        );

        let mut borrowed_symbols = SegmentSymbols::default();
        let mut borrowed_postings = ExactPostingsIndex::default();
        let borrowed = encode_borrowed_canonical_segment_labels(
            canonical
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
            &mut borrowed_symbols,
            &mut borrowed_postings,
            0,
        );

        assert_eq!(borrowed.series_id, owned.series_id);
        assert_eq!(
            resolved_entry_labels(&borrowed_symbols, &borrowed),
            resolved_entry_labels(&owned_symbols, &owned)
        );
    }

    #[test]
    fn flat_interned_label_encoder_matches_visitor_encoding() {
        let labels = [
            crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
            crate::labels::KeyValueRef::from(("namespace", "default")),
            crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
        ];
        let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
        let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

        let mut visitor_symbols = SegmentSymbols::default();
        let mut visitor_postings = ExactPostingsIndex::default();
        let visitor = encode_label_visitor_metadata(
            &mut visitor_symbols,
            &mut visitor_postings,
            0,
            |visit| {
                crate::labels::LabelSetStore::visit_labelset(&store, series, |key, value| {
                    visit(key, value)
                })
            },
        );

        let mut flat_symbols = SegmentSymbols::default();
        let mut flat_postings = ExactPostingsIndex::default();
        let mut normalized_names = NormalizedNameCache::default();
        let mut hash_scratch = Vec::new();
        let flat = encode_flat_interned_label_metadata(
            &mut flat_symbols,
            &mut flat_postings,
            &mut normalized_names,
            &mut hash_scratch,
            0,
            &store,
            series,
        );

        assert_eq!(flat.series_id, visitor.series_id);
        assert_eq!(
            resolved_entry_labels(&flat_symbols, &flat),
            resolved_entry_labels(&visitor_symbols, &visitor)
        );
    }

    #[test]
    fn flat_interned_label_encoder_reuses_hash_scratch_buffer() {
        let labels = [
            crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
            crate::labels::KeyValueRef::from(("namespace", "default")),
            crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
        ];
        let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
        let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

        let mut symbols = SegmentSymbols::default();
        let mut postings = ExactPostingsIndex::default();
        let mut normalized_names = NormalizedNameCache::default();
        let mut hash_scratch = Vec::with_capacity(256);
        let initial_capacity = hash_scratch.capacity();

        let first = encode_flat_interned_label_metadata(
            &mut symbols,
            &mut postings,
            &mut normalized_names,
            &mut hash_scratch,
            0,
            &store,
            series,
        );
        assert_eq!(hash_scratch.len(), 0);
        assert_eq!(hash_scratch.capacity(), initial_capacity);

        let second = encode_flat_interned_label_metadata(
            &mut symbols,
            &mut postings,
            &mut normalized_names,
            &mut hash_scratch,
            1,
            &store,
            series,
        );
        assert_eq!(hash_scratch.len(), 0);
        assert_eq!(hash_scratch.capacity(), initial_capacity);
        assert_eq!(second.series_id, first.series_id);
        assert_eq!(
            resolved_entry_labels(&symbols, &second),
            resolved_entry_labels(&symbols, &first)
        );
    }

    #[test]
    fn normalized_name_cache_reuses_label_and_metric_names_by_source_symbol_id() {
        let mut cache = NormalizedNameCache::default();
        let mut label_normalizations = 0usize;
        let mut metric_normalizations = 0usize;
        let mut source_symbols = crate::labels::DefaultSymbolTable::default();
        let label_id = source_symbols.intern("pod.name").unwrap();
        let metric_id = source_symbols.intern("cpu.usage").unwrap();

        let first_label = cache.label_name(label_id, "pod.name", |name| {
            label_normalizations += 1;
            normalize_label_name(name)
        });
        let second_label = cache.label_name(label_id, "pod.name", |name| {
            label_normalizations += 1;
            normalize_label_name(name)
        });
        let first_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
            metric_normalizations += 1;
            normalize_metric_name(name)
        });
        let second_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
            metric_normalizations += 1;
            normalize_metric_name(name)
        });

        assert_eq!(first_label.as_ref(), normalize_label_name("pod.name"));
        assert_eq!(second_label, first_label);
        assert_eq!(first_metric.as_ref(), normalize_metric_name("cpu.usage"));
        assert_eq!(second_metric, first_metric);
        assert_eq!(label_normalizations, 1);
        assert_eq!(metric_normalizations, 1);
    }

    #[test]
    fn normalized_name_cache_falls_back_to_uncached_normalization_after_cap() {
        let mut cache = NormalizedNameCache::with_max_entries(1);
        let mut source_symbols = crate::labels::DefaultSymbolTable::default();
        let first_id = source_symbols.intern("pod.name").unwrap();
        let second_id = source_symbols.intern("container.name").unwrap();
        let mut normalizations = 0usize;

        cache.label_name(first_id, "pod.name", |name| {
            normalizations += 1;
            normalize_label_name(name)
        });
        cache.label_name(first_id, "pod.name", |name| {
            normalizations += 1;
            normalize_label_name(name)
        });
        cache.label_name(second_id, "container.name", |name| {
            normalizations += 1;
            normalize_label_name(name)
        });
        cache.label_name(second_id, "container.name", |name| {
            normalizations += 1;
            normalize_label_name(name)
        });

        assert_eq!(normalizations, 3);
    }

    #[test]
    fn label_visitor_encoder_keeps_first_metric_name_and_sorts_labels() {
        let mut symbols = SegmentSymbols::default();
        let mut postings = ExactPostingsIndex::default();

        let entry = encode_label_visitor_metadata(&mut symbols, &mut postings, 7, |visit| {
            visit("z.label", "last");
            visit(METRIC_NAME_LABEL, "cpu.first");
            visit("a.label", "first");
            visit(METRIC_NAME_LABEL, "cpu.second");
        });

        assert_eq!(
            resolved_entry_labels(&symbols, &entry),
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    normalize_metric_name("cpu.first")
                ),
                (normalize_label_name("a.label"), "first".to_string()),
                (normalize_label_name("z.label"), "last".to_string()),
            ]
        );
        assert!(
            postings
                .get(
                    symbols.lookup(&normalize_label_name("a.label")).unwrap(),
                    symbols.lookup("first").unwrap()
                )
                .is_some_and(|refs| refs == [7])
        );
    }

    #[test]
    fn label_value_time_ranges_update_from_encoded_series_entry() {
        let mut index = LabelValueTimeRangeIndex::default();
        let entry = SeriesEntry {
            series_id: 7,
            kind_mask: SERIES_KIND_FLOAT,
            labels: vec![(1, 10), (2, 20)],
        };
        let first_chunk = ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 1_000,
            max_time_ms: 2_000,
            offset: 0,
            length: 1,
            reserved0: 0,
            reserved1: 0,
        };
        let second_chunk = ChunkIndexEntry {
            min_time_ms: 500,
            max_time_ms: 4_000,
            ..first_chunk.clone()
        };

        update_label_value_time_ranges(&mut index, &entry, &first_chunk);
        update_label_value_time_ranges(&mut index, &entry, &second_chunk);

        assert_eq!(
            index.get(1, 10),
            Some(LabelValueTimeRange {
                min_time_ms: 500,
                max_time_ms: 4_000,
            })
        );
        assert_eq!(
            index.get(2, 20),
            Some(LabelValueTimeRange {
                min_time_ms: 500,
                max_time_ms: 4_000,
            })
        );
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
    fn segment_writer_records_ordered_samples_with_label_visitor() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(5),
                &[(1_000, 1.0), (1_500, 1.5), (2_000, 2.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "cpu.usage");
                    visit("pod.name", "backend-1");
                },
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

        let mut chunk_reader = ChunkReader::new(reader.open_chunks().unwrap());
        let record = chunk_reader.read_next().unwrap().unwrap();
        assert_eq!(
            record.samples,
            ChunkSamples::Float(vec![(1_000, 1.0), (1_500, 1.5), (2_000, 2.0)])
        );
    }

    #[test]
    fn segment_writer_ordered_samples_reject_unsorted_input() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        let err = writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(5),
                &[(2_000, 2.0), (1_000, 1.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "cpu.usage");
                    visit("pod.name", "backend-1");
                },
            )
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
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
    fn segment_writer_writes_typed_otlp_chunks() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        let histogram = HistogramValue {
            count: 4,
            sum: Some(10.0),
            min: Some(1.0),
            max: Some(4.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0],
            bucket_counts: vec![1, 2, 1],
        };
        let exphist = ExponentialHistogramValue {
            count: 6,
            sum: Some(15.0),
            min: Some(1.0),
            max: Some(8.0),
            scale: 2,
            zero_threshold: 0.0,
            zero_count: 1,
            metadata: TypedSampleMetadata::default(),
            positive: ExponentialHistogramBuckets {
                offset: -1,
                counts: vec![2, 3],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![0],
            },
        };
        let summary = SummaryValue {
            count: 10,
            sum: 50.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![
                SummaryQuantileValue {
                    quantile: 0.5,
                    value: 4.0,
                },
                SummaryQuantileValue {
                    quantile: 0.9,
                    value: 8.0,
                },
            ],
        };

        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(21),
                &[(1_000, histogram.clone())],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.duration");
                    visit("route", "/typed");
                },
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(22),
                &[(2_000, exphist.clone())],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.size");
                    visit("route", "/typed");
                },
            )
            .unwrap();
        writer
            .record_summary_samples_ordered_with_label_visitor(
                SeriesRef::new(23),
                &[(3_000, summary.clone())],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.latency");
                    visit("route", "/typed");
                },
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
        assert_eq!(reader.meta().series, 3);

        let series = read_series_bin(
            File::open(reader.file_path(SegmentFile::Series)).expect("open series"),
        )
        .unwrap();
        assert_eq!(
            series[0].kind_mask & SERIES_KIND_HISTOGRAM,
            SERIES_KIND_HISTOGRAM
        );
        assert_eq!(
            series[1].kind_mask & SERIES_KIND_EXPONENTIAL_HISTOGRAM,
            SERIES_KIND_EXPONENTIAL_HISTOGRAM
        );
        assert_eq!(
            series[2].kind_mask & SERIES_KIND_SUMMARY,
            SERIES_KIND_SUMMARY
        );

        let chunk_entries = reader.read_chunk_index().unwrap();
        assert_eq!(chunk_entries[0][0].kind, ChunkKind::Histogram);
        assert_eq!(chunk_entries[1][0].kind, ChunkKind::ExponentialHistogram);
        assert_eq!(chunk_entries[2][0].kind, ChunkKind::Summary);

        let mut chunk_reader = ChunkReader::new(reader.open_chunks().unwrap());
        assert_eq!(
            chunk_reader.read_next().unwrap().unwrap().samples,
            ChunkSamples::Histogram(vec![(1_000, histogram)])
        );
        assert_eq!(
            chunk_reader.read_next().unwrap().unwrap().samples,
            ChunkSamples::ExponentialHistogram(vec![(2_000, exphist)])
        );
        assert_eq!(
            chunk_reader.read_next().unwrap().unwrap().samples,
            ChunkSamples::Summary(vec![(3_000, summary)])
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

    #[test]
    fn segment_store_smoke_verifier_counts_kinds_and_runs_promql_readbacks() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.0), (2_000, 2.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "cpu.usage");
                    visit("instance", "host-a");
                },
            )
            .unwrap();

        let histogram = HistogramValue {
            count: 4,
            sum: Some(10.0),
            min: Some(1.0),
            max: Some(4.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0],
            bucket_counts: vec![1, 2, 1],
        };
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(1_000, histogram)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.duration");
                    visit("route", "/typed");
                },
            )
            .unwrap();

        let exphist = ExponentialHistogramValue {
            count: 6,
            sum: Some(15.0),
            min: Some(1.0),
            max: Some(8.0),
            scale: 2,
            zero_threshold: 0.0,
            zero_count: 1,
            metadata: TypedSampleMetadata::default(),
            positive: ExponentialHistogramBuckets {
                offset: -1,
                counts: vec![2, 3],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![0],
            },
        };
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(3),
                &[(2_000, exphist)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.size");
                    visit("route", "/typed");
                },
            )
            .unwrap();

        let summary = SummaryValue {
            count: 10,
            sum: 50.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![SummaryQuantileValue {
                quantile: 0.9,
                value: 8.0,
            }],
        };
        writer
            .record_summary_samples_ordered_with_label_visitor(
                SeriesRef::new(4),
                &[(3_000, summary)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.latency");
                    visit("route", "/typed");
                },
            )
            .unwrap();
        writer.flush().unwrap();

        let store = SegmentStoreReader::open(tempdir.path()).unwrap();
        let report = store.smoke_verify(0, 10_000, 1).unwrap();

        assert_eq!(report.totals.segments, 1);
        assert_eq!(report.totals.datapoints, 5);
        assert_eq!(report.totals.by_kind.float.chunks, 1);
        assert_eq!(report.totals.by_kind.histogram.chunks, 1);
        assert_eq!(report.totals.by_kind.exponential_histogram.chunks, 1);
        assert_eq!(report.totals.by_kind.summary.chunks, 1);

        assert!(report.sample_series.iter().any(|series| {
            series.kind == ChunkKind::Float
                && series
                    .labels
                    .iter()
                    .any(|(key, value)| key == "instance" && value == "host-a")
        }));
        assert!(report.queries.iter().any(|query| {
            query.kind == ChunkKind::Float && query.result_samples > 0 && query.samples_decoded > 0
        }));
        assert!(report.queries.iter().any(|query| {
            query.kind == ChunkKind::Histogram
                && query.query.contains("_count")
                && query.result_series > 0
        }));
        assert!(report.queries.iter().any(|query| {
            query.kind == ChunkKind::Histogram
                && query.query.contains("_bucket")
                && query.query.contains(r#"le="1""#)
                && query.result_series > 0
        }));
        assert!(report.queries.iter().any(|query| {
            query.kind == ChunkKind::ExponentialHistogram
                && query.query.contains("_bucket")
                && query.query.contains(r#"le="+Inf""#)
                && query.result_series > 0
        }));
        assert!(report.queries.iter().any(|query| {
            query.kind == ChunkKind::Summary
                && query.query.contains(r#"quantile="0.9""#)
                && query.result_series > 0
        }));
    }
}
