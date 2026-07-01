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

use crate::labels::SeriesRef;
use crate::promql::{
    METRIC_NAME_LABEL, PromqlMatcherOp, PromqlQuery, PromqlQueryError, PromqlRangeFunction,
    PromqlRangeFunctionKind, PromqlSelector, normalize_label_name, normalize_metric_name,
    parse_query,
};
use crate::storage::chunk::{
    ChunkIndexEntry, ChunkIndexReader, ChunkSamples, ChunkWriter, read_chunk_index,
    read_chunk_record_at, write_chunk_index,
};
use crate::storage::head::{
    ExponentialHistogramValue, HeadBuffer, HistogramValue, OtlpAggregationTemporality,
    SeriesLabelResolver, SummaryValue, TypedSampleMetadata,
    exponential_histogram_projected_bucket_count, prometheus_stale_nan,
};
use crate::storage::index::{
    ExactPostingsIndex, LabelValueFstIndex, LabelValueTimeRangeIndex, SegmentIndexReader,
    SegmentIndexes, write_segment_indexes,
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
const SEGMENT_SCHEMA_VERSION: u16 = 1;
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
        }
    }

    fn push_stage(&mut self, kind: SegmentFlushStageKind, elapsed: Duration) {
        self.stages.push(SegmentFlushStage { kind, elapsed });
        self.stage_kinds.push(kind);
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
    label_value_time_ranges: LabelValueTimeRangeIndex,
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
    let mut bytes = Vec::new();
    let mut encoded_labels = Vec::with_capacity(labels.len());
    for (key, value) in labels {
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);

        let key_sym = symbols.intern(&key);
        let value_sym = symbols.intern(&value);
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
}

impl SegmentWriter {
    pub fn new(config: SegmentWriterConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.segments_dir)?;
        Ok(Self {
            config,
            active: None,
            last_flush_profile: None,
        })
    }

    pub fn last_flush_profile(&self) -> Option<&SegmentFlushProfile> {
        self.last_flush_profile.as_ref()
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

            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };

            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_FLOAT);
            apply_metadata(active, local_ref);

            let entry = if raw {
                active
                    .chunks
                    .append_float_chunk_raw_ordered(local_ref, &samples[idx..end_idx])?
            } else {
                active
                    .chunks
                    .append_float_chunk_ordered(local_ref, &samples[idx..end_idx])?
            };
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
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

            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };

            let local_ref = ensure_local_series_with_kind(active, series, kind_mask);
            apply_metadata(active, local_ref);
            active.series_entries[local_ref as usize].kind_mask |= kind_mask;

            let entry = append_chunk(&mut active.chunks, local_ref, &samples[idx..end_idx])?;
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
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

            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64);

            let entry = active
                .chunks
                .append_int_chunk_ordered(local_ref, &ordered[idx..end_idx])?;
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
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

            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64);

            let entry = active
                .chunks
                .append_int_chunk_raw_ordered(local_ref, &ordered[idx..end_idx])?;
            update_label_value_time_ranges(
                &mut active.label_value_time_ranges,
                &active.series_entries[local_ref as usize],
                &entry,
            );
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

        let total_start = Instant::now();
        let segment_id = active.id;
        let start_ms = active.start_ms;
        let end_ms = active.end_ms;
        let datapoints = active.datapoints;
        let series = active.series_map.len() as u64;
        let tmp = active.temp_dir;
        let mut profile =
            SegmentFlushProfile::new(segment_id.dir_name(), start_ms, end_ms, datapoints, series);

        let meta = SegmentMeta {
            segment_id: segment_id.dir_name(),
            start_ms,
            end_ms,
            datapoints,
            series,
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
            ooo_chunks_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::OooChunks),
            footer_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Footer),
            publish_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Publish),
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
                label_value_time_ranges: LabelValueTimeRangeIndex::default(),
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
    for (name, value) in &entry.labels {
        index.insert(*name, *value, chunk.min_time_ms, chunk.max_time_ms);
    }
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryExecution {
    pub results: Vec<SegmentQueryResult>,
    pub stats: QueryStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStats {
    pub matched_series: u64,
    pub chunk_reads: u64,
    pub bytes_read: u64,
    pub samples_decoded: u64,
    pub regex_values_examined: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_matched_series: Option<u64>,
    pub max_chunk_reads: Option<u64>,
    pub max_bytes_read: Option<u64>,
    pub max_samples_decoded: Option<u64>,
    pub max_regex_values_examined: Option<u64>,
}

impl QueryLimits {
    pub const fn unlimited() -> Self {
        Self {
            max_matched_series: None,
            max_chunk_reads: None,
            max_bytes_read: None,
            max_samples_decoded: None,
            max_regex_values_examined: None,
        }
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
    ChunkReads,
    BytesRead,
    SamplesDecoded,
    RegexValuesExamined,
}

impl QueryLimit {
    fn as_str(self) -> &'static str {
        match self {
            Self::MatchedSeries => "matched_series",
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
}

impl QueryBudget {
    pub(crate) fn new(limits: QueryLimits) -> Self {
        Self {
            limits,
            stats: QueryStats::default(),
            seen_series: BTreeSet::new(),
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

    pub(crate) fn observe_regex_value(&mut self) -> io::Result<()> {
        self.stats.regex_values_examined = self.checked_add(
            QueryLimit::RegexValuesExamined,
            self.stats.regex_values_examined,
            1,
            self.limits.max_regex_values_examined,
        )?;
        Ok(())
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

pub struct SegmentStoreReader {
    segments: Vec<SegmentReader>,
    query_projection_config: QueryProjectionConfig,
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
                let selector = storage_selector_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selector_with_limits(&selector, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selector = storage_selector_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selector_with_limits(&selector, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
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
                let selector = storage_selector_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selector_with_head_with_limits(
                    head, labels, &selector, start_ms, end_ms, limits,
                )
                .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selector = storage_selector_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selector_with_head_with_limits(
                        head,
                        labels,
                        &selector,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
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
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
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
        let Some(increase) = counter_increase(&result.samples) else {
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
        out.push(SegmentQueryResult {
            series_id: segment_series_id(&labels),
            labels,
            samples: vec![(eval_time_ms, value)],
        });
    }
    merge_query_results(out)
}

fn counter_increase(samples: &[(u64, f64)]) -> Option<f64> {
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

fn function_result_labels(labels: &[(String, String)]) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL)
        .cloned()
        .collect()
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

    fn query_normalized(
        &self,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let mut index_reader =
            SegmentIndexReader::open(File::open(self.file_path(SegmentFile::Indexes))?)?;

        let mut candidates: Option<Vec<u32>> = None;
        for matcher in matchers {
            let positive = match matcher {
                NormalizedMatcher::Eq { name, value } => {
                    let Some(name_sym) = symbols.lookup(name) else {
                        return Ok(Vec::new());
                    };
                    let Some(value_sym) = symbols.lookup(value) else {
                        return Ok(Vec::new());
                    };
                    let Some(posting) = index_reader.exact_postings(name_sym, value_sym)? else {
                        return Ok(Vec::new());
                    };
                    Some(posting)
                }
                NormalizedMatcher::Regex { name, pattern } => Some(regex_postings(
                    name,
                    pattern,
                    &symbols,
                    &mut index_reader,
                    budget,
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
                        (symbols.lookup(name), symbols.lookup(value))
                    else {
                        continue;
                    };
                    let Some(posting) = index_reader.exact_postings(name_sym, value_sym)? else {
                        continue;
                    };
                    candidate_refs = subtract_sorted(&candidate_refs, &posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting =
                        regex_postings(name, pattern, &symbols, &mut index_reader, budget)?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        budget.observe_candidate_series_refs(candidate_refs.len() as u64)?;

        let mut series_reader =
            SeriesReader::open(File::open(self.file_path(SegmentFile::Series))?)?;
        let mut chunk_index_reader =
            ChunkIndexReader::open(File::open(self.file_path(SegmentFile::ChunkIndex))?)?;
        let mut chunk_file = self.open_chunks()?;
        let mut results = Vec::new();

        for series_ref in candidate_refs {
            let Some(entry) = series_reader.read_entry(series_ref)? else {
                continue;
            };
            budget.observe_matched_series(entry.series_id)?;
            let Some(entries) = chunk_index_reader.read_entries(series_ref)? else {
                continue;
            };

            let labels = Self::resolve_series_labels(&symbols, &entry)?;
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
                budget.observe_chunk_read(u64::from(chunk_entry.length))?;
                let record =
                    read_chunk_record_at(&mut chunk_file, chunk_entry.offset, chunk_entry.length)?;
                match (projection, record.samples) {
                    (SegmentProjection::None, ChunkSamples::Float(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms),
                        );
                    }
                    (SegmentProjection::None, ChunkSamples::Int64(values)) => {
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
                    (_, ChunkSamples::Float(_))
                    | (_, ChunkSamples::Int64(_))
                    | (_, ChunkSamples::Histogram(_))
                    | (_, ChunkSamples::ExponentialHistogram(_))
                    | (_, ChunkSamples::Summary(_)) => {}
                }
            }

            if matches!(projection, SegmentProjection::None) {
                if samples.is_empty() {
                    continue;
                }
                samples.sort_by_key(|(ts, _)| *ts);
                results.push(SegmentQueryResult {
                    series_id: entry.series_id,
                    labels,
                    samples,
                });
            } else {
                results.extend(projected_results.into_values());
            }
        }

        Ok(results)
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
            Self::push_projected_sample(out, labels.clone(), ts, value);
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
            Self::push_projected_sample(out, labels.clone(), ts, value);
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
                    Self::push_projected_sample(out, labels, ts, projected);
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
                Self::push_projected_sample(out, labels, ts, projected);
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
                    Self::push_projected_sample(out, labels, ts, projected);
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
                Self::push_projected_sample(out, labels, ts, projected);
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
        let entry = out.entry(series_id).or_insert_with(|| SegmentQueryResult {
            series_id,
            labels,
            samples: Vec::new(),
        });
        entry.samples.push((timestamp_ms, value));
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
        }
    }

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
                regex::Regex::new(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid regex matcher: {err}"))
                })?;
                matchers.push(LabelMatcher::regex(matcher.name, matcher.value));
            }
            PromqlMatcherOp::NotRegex => {
                regex::Regex::new(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid regex matcher: {err}"))
                })?;
                matchers.push(LabelMatcher::not_regex(matcher.name, matcher.value));
            }
        }
    }

    let storage_selector = match metric_name {
        Some(metric_name) => SegmentSelector::with_metric(metric_name, matchers),
        None => SegmentSelector::new(matchers),
    };
    Ok(storage_selector.with_projection(projection))
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
    budget: &mut QueryBudget,
) -> io::Result<Vec<u32>> {
    let regex = regex::Regex::new(pattern)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let Some(name_sym) = symbols.lookup(name) else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();
    for value in index_reader.label_values(name_sym)? {
        budget.observe_regex_value()?;
        if !regex.is_match(&value) {
            continue;
        }
        let value_sym = symbols.lookup(&value).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
        })?;
        if let Some(posting) = index_reader.exact_postings(name_sym, value_sym)? {
            out = union_sorted(&out, &posting);
        }
    }

    Ok(out)
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
            .or_insert_with(|| SegmentQueryResult {
                series_id: result.series_id,
                labels: result.labels.clone(),
                samples: Vec::new(),
            });
        entry.samples.extend(result.samples);
    }

    let mut results: Vec<_> = merged.into_values().collect();
    for result in &mut results {
        dedupe_samples_keep_last(&mut result.samples);
    }
    results
}

fn dedupe_samples_keep_last(samples: &mut Vec<(u64, f64)>) {
    // Input order is source precedence; later inserts replace earlier samples
    // at the same timestamp while BTreeMap keeps the output time-sorted.
    let mut by_timestamp = BTreeMap::new();
    for (timestamp_ms, value) in samples.drain(..) {
        by_timestamp.insert(timestamp_ms, value);
    }
    samples.extend(by_timestamp);
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
            schema_version: 1,
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
            schema_version: 1,
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
        assert!(seg_dir.join("footer.bin").exists());
        let chunk_len = fs::metadata(seg_dir.join("chunks.bin")).unwrap().len();
        assert!(chunk_len > 0);
        let index_len = fs::metadata(seg_dir.join("chunk_index.bin")).unwrap().len();
        assert!(index_len > 0);
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
}
