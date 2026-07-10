use super::*;

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

pub(super) const SEGMENT_FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"CSFT");
pub(super) const SEGMENT_FOOTER_VERSION: u16 = 1;
pub(super) const SEGMENT_SCHEMA_VERSION: u16 = 5;
pub(super) const SEGMENT_FOOTER_HEADER_LEN: usize = 16;
pub(super) const SEGMENT_FOOTER_TRAILER_LEN: usize = 4;
pub(super) const SEGMENT_FOOTER_TRACKED_FILES: [SegmentFile; 7] = [
    SegmentFile::MetaJson,
    SegmentFile::Symbols,
    SegmentFile::Series,
    SegmentFile::Chunks,
    SegmentFile::OooChunks,
    SegmentFile::ChunkIndex,
    SegmentFile::Indexes,
];
pub(super) const SEGMENT_FLUSH_SIZE_FILES: [SegmentFile; 8] = [
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
pub(super) struct SegmentFooter {
    pub(super) schema_version: u16,
    pub(super) files: Vec<SegmentFooterFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SegmentFooterFile {
    pub(super) file: SegmentFile,
    pub(super) size: u64,
    pub(super) checksum_xxh64: u64,
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
    pub(super) fn from_chunk_entries(entries: &[Vec<ChunkIndexEntry>]) -> Self {
        let mut summary = Self::default();
        for entry in entries.iter().flatten() {
            summary.add_chunk(entry.kind, u64::from(entry.length));
        }
        summary
    }

    pub(super) fn add_chunk(&mut self, kind: ChunkKind, bytes: u64) {
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
    pub(super) fn stats(&self, kind: ChunkKind) -> SegmentChunkKindStats {
        match kind {
            ChunkKind::Float => self.float,
            ChunkKind::Int64 => self.int64,
            ChunkKind::Histogram => self.histogram,
            ChunkKind::ExponentialHistogram => self.exponential_histogram,
            ChunkKind::Summary => self.summary,
        }
    }

    pub(super) fn stats_mut(&mut self, kind: ChunkKind) -> &mut SegmentChunkKindStats {
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
    pub(super) segment_id_provider: Arc<dyn SegmentIdProvider>,
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
    MetricSeriesRanges,
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
            Self::MetricSeriesRanges => "metric_series_ranges",
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

    pub(super) fn add_chunk(&mut self, timing: SegmentRecordChunkTiming, samples: u64) {
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
pub(super) struct SegmentRecordChunkTiming {
    pub(super) wall_elapsed: Duration,
    pub(super) ensure_window: Duration,
    pub(super) metadata: Duration,
    pub(super) chunk_append: Duration,
    pub(super) label_time_range: Duration,
    pub(super) bookkeeping: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFlushProfile {
    pub segment_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub datapoints: u64,
    pub series: u64,
    pub total: Duration,
    chunk_rewrite_frames: u64,
    chunk_rewrite_payload_bytes: u64,
    stages: Vec<SegmentFlushStage>,
    stage_kinds: Vec<SegmentFlushStageKind>,
    file_sizes: Vec<SegmentFlushFileSize>,
}

impl SegmentFlushProfile {
    pub(super) fn new(
        segment_id: String,
        start_ms: u64,
        end_ms: u64,
        datapoints: u64,
        series: u64,
    ) -> Self {
        Self {
            segment_id,
            start_ms,
            end_ms,
            datapoints,
            series,
            total: Duration::ZERO,
            chunk_rewrite_frames: 0,
            chunk_rewrite_payload_bytes: 0,
            stages: Vec::new(),
            stage_kinds: Vec::new(),
            file_sizes: Vec::new(),
        }
    }

    pub(super) fn push_stage(&mut self, kind: SegmentFlushStageKind, elapsed: Duration) {
        self.stages.push(SegmentFlushStage { kind, elapsed });
        self.stage_kinds.push(kind);
    }

    pub(super) fn set_file_sizes(&mut self, file_sizes: Vec<SegmentFlushFileSize>) {
        self.file_sizes = file_sizes;
    }

    pub(super) fn add_chunk_rewrite(&mut self, frames: u64, payload_bytes: u64) {
        self.chunk_rewrite_frames = self.chunk_rewrite_frames.saturating_add(frames);
        self.chunk_rewrite_payload_bytes = self
            .chunk_rewrite_payload_bytes
            .saturating_add(payload_bytes);
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

    pub fn chunk_rewrite_frames(&self) -> u64 {
        self.chunk_rewrite_frames
    }

    pub fn chunk_rewrite_payload_bytes(&self) -> u64 {
        self.chunk_rewrite_payload_bytes
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

    pub(super) fn stage_elapsed_ms(&self, kind: SegmentFlushStageKind) -> u64 {
        self.stage_elapsed(kind)
            .map(duration_ms_u64)
            .unwrap_or_default()
    }
}
