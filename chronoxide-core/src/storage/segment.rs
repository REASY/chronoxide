use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crc32c::{crc32c, crc32c_append};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;
use ulid::Ulid;

use crate::labels::SeriesRef;
use crate::promql::{
    METRIC_NAME_LABEL, PromqlMatcherOp, PromqlQueryError, PromqlSelector, canonicalize_labelset,
    normalize_label_name, normalize_metric_name, parse_vector_selector, series_id,
};
use crate::storage::chunk::{
    ChunkIndexEntry, ChunkSamples, ChunkWriter, read_chunk_index, read_chunk_record_at,
    write_chunk_index,
};
use crate::storage::head::{HeadBuffer, SeriesLabelResolver};
use crate::storage::index::{
    ExactPostingsIndex, LabelValueFstIndex, SegmentIndexes, read_segment_indexes,
    write_segment_indexes,
};
use crate::storage::manifest::{ManifestInventory, ManifestSegment, read_manifest_inventory};
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

        {
            let mut chunk_index = File::create(tmp.file_path(SegmentFile::ChunkIndex))?;
            write_chunk_index(&mut chunk_index, &active.chunk_entries)?;
            chunk_index.flush()?;
        }

        let (symbols, series_entries, postings) =
            build_segment_metadata(&active.source_series_refs, &active.series_metadata);
        let label_values = LabelValueFstIndex::from_series(&series_entries, &symbols)?;

        {
            let mut symbols_file = File::create(tmp.file_path(SegmentFile::Symbols))?;
            write_symbols_bin(&mut symbols_file, &symbols)?;
            symbols_file.flush()?;
        }

        {
            let mut series_file = File::create(tmp.file_path(SegmentFile::Series))?;
            write_series_bin_v1(&mut series_file, &series_entries)?;
            series_file.flush()?;
        }

        {
            let mut index_file = File::create(tmp.file_path(SegmentFile::Indexes))?;
            write_segment_indexes(
                &mut index_file,
                &SegmentIndexes {
                    exact_postings: postings,
                    label_values,
                },
            )?;
            index_file.flush()?;
        }
        File::create(tmp.file_path(SegmentFile::OooChunks))?;

        write_segment_footer(tmp.path())?;
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
    pub(crate) fn add_labelset(&mut self, labels: &[(String, String)]) {
        for (name, value) in labels {
            self.label_names.insert(name.clone());
            self.label_values
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
            if name == METRIC_NAME_LABEL {
                self.metric_names.insert(value.clone());
            }
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentSelector {
    metric_name: Option<String>,
    matchers: Vec<LabelMatcher>,
}

impl SegmentSelector {
    pub fn new(matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: None,
            matchers,
        }
    }

    pub fn metric(metric_name: impl Into<String>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers: Vec::new(),
        }
    }

    pub fn with_metric(metric_name: impl Into<String>, matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers,
        }
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

        Ok(Self { segments })
    }

    pub fn open_manifest_published(
        segments_dir: impl AsRef<Path>,
        manifest_dir: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let Some(inventory) = read_manifest_inventory(manifest_dir)? else {
            return Ok(Self {
                segments: Vec::new(),
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
        let selector = storage_selector_from_promql(parse_vector_selector(query)?)?;
        Ok(self.query_selector(&selector, start_ms, end_ms)?)
    }

    pub fn query_promql_with_limits(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let selector = storage_selector_from_promql(parse_vector_selector(query)?)?;
        self.query_selector_with_limits(&selector, start_ms, end_ms, limits)
            .map_err(promql_error_from_query_io)
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
        let selector = storage_selector_from_promql(parse_vector_selector(query)?)?;
        Ok(self.query_selector_with_head(head, labels, &selector, start_ms, end_ms)?)
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
        let selector = storage_selector_from_promql(parse_vector_selector(query)?)?;
        self.query_selector_with_head_with_limits(head, labels, &selector, start_ms, end_ms, limits)
            .map_err(promql_error_from_query_io)
    }

    pub fn metric_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
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
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
        head.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
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
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
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
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
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
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
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

    fn collect_metadata(
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
            segment.collect_metadata(start_ms, end_ms, metadata)?;
        }

        Ok(())
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
        self.query_normalized(&matchers, start_ms, end_ms, &mut budget)
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
        self.query_normalized(&matchers, start_ms, end_ms, budget)
    }

    pub fn metric_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    fn query_normalized(
        &self,
        matchers: &[NormalizedMatcher],
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let series = read_series_bin_v1(File::open(self.file_path(SegmentFile::Series))?)?;
        let mut indexes = read_segment_indexes(File::open(self.file_path(SegmentFile::Indexes))?)?;
        if indexes.label_values.is_empty() {
            indexes.label_values = LabelValueFstIndex::from_series(&series, &symbols)?;
        }
        let chunk_index = self.read_chunk_index()?;

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
                    let Some(posting) = indexes.exact_postings.get(name_sym, value_sym) else {
                        return Ok(Vec::new());
                    };
                    Some(posting.to_vec())
                }
                NormalizedMatcher::Regex { name, pattern } => Some(regex_postings(
                    name,
                    pattern,
                    &symbols,
                    &indexes.label_values,
                    &indexes.exact_postings,
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

        let mut candidate_refs =
            candidates.unwrap_or_else(|| (0..series.len()).map(|idx| idx as u32).collect());
        for matcher in matchers {
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let (Some(name_sym), Some(value_sym)) =
                        (symbols.lookup(name), symbols.lookup(value))
                    else {
                        continue;
                    };
                    let Some(posting) = indexes.exact_postings.get(name_sym, value_sym) else {
                        continue;
                    };
                    candidate_refs = subtract_sorted(&candidate_refs, posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = regex_postings(
                        name,
                        pattern,
                        &symbols,
                        &indexes.label_values,
                        &indexes.exact_postings,
                        budget,
                    )?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        let mut chunk_file = self.open_chunks()?;
        let mut results = Vec::new();

        for series_ref in candidate_refs {
            let Some(entry) = series.get(series_ref as usize) else {
                continue;
            };
            budget.observe_matched_series(entry.series_id)?;
            let Some(entries) = chunk_index.get(series_ref as usize) else {
                continue;
            };

            let mut samples = Vec::new();
            for chunk_entry in entries {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                budget.observe_chunk_read(u64::from(chunk_entry.length))?;
                let record =
                    read_chunk_record_at(&mut chunk_file, chunk_entry.offset, chunk_entry.length)?;
                match record.samples {
                    ChunkSamples::Float(values) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms),
                        );
                    }
                    ChunkSamples::Int64(values) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
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

    fn collect_metadata(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let series = read_series_bin_v1(File::open(self.file_path(SegmentFile::Series))?)?;
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

fn storage_selector_from_promql(
    selector: PromqlSelector,
) -> Result<SegmentSelector, PromqlQueryError> {
    let mut matchers = Vec::with_capacity(selector.matchers.len());
    for matcher in selector.matchers {
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

    Ok(match selector.metric_name {
        Some(metric_name) => SegmentSelector::with_metric(metric_name, matchers),
        None => SegmentSelector::new(matchers),
    })
}

fn regex_postings(
    name: &str,
    pattern: &str,
    symbols: &SegmentSymbols,
    value_index: &LabelValueFstIndex,
    postings: &ExactPostingsIndex,
    budget: &mut QueryBudget,
) -> io::Result<Vec<u32>> {
    let regex = regex::Regex::new(pattern)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let Some(name_sym) = symbols.lookup(name) else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();
    for value in value_index.values(name_sym)? {
        budget.observe_regex_value()?;
        if !regex.is_match(&value) {
            continue;
        }
        let value_sym = symbols.lookup(&value).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
        })?;
        if let Some(posting) = postings.get(name_sym, value_sym) {
            out = union_sorted(&out, posting);
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
        result.samples.sort_by_key(|(ts, _)| *ts);
        dedupe_samples_keep_last(&mut result.samples);
    }
    results
}

fn dedupe_samples_keep_last(samples: &mut Vec<(u64, f64)>) {
    let mut deduped: Vec<(u64, f64)> = Vec::with_capacity(samples.len());
    for sample in samples.drain(..) {
        if let Some(last) = deduped.last_mut()
            && last.0 == sample.0
        {
            *last = sample;
            continue;
        }
        deduped.push(sample);
    }
    *samples = deduped;
}

fn segment_window(timestamp_ms: u64, duration_ms: u64) -> (u64, u64) {
    let start_ms = timestamp_ms.saturating_sub(timestamp_ms % duration_ms);
    (start_ms, start_ms.saturating_add(duration_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkReader, ChunkSamples};
    use std::io::{ErrorKind, Read, Seek, SeekFrom};

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
