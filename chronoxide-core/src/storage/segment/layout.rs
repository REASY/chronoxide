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
pub(super) const LEGACY_SEGMENT_SCHEMA_VERSION_FOR_LAYOUT_AB: u16 = 5;
pub(super) const SEGMENT_SCHEMA_VERSION_V6: u16 = 6;
pub(super) const SEGMENT_SCHEMA_VERSION_V7: u16 = 7;
pub(super) const SEGMENT_SCHEMA_VERSION_V8: u16 = 8;
pub(super) const SEGMENT_FOOTER_HEADER_LEN: usize = 16;
pub(super) const SEGMENT_FOOTER_TRAILER_LEN: usize = 4;
pub(super) const SEGMENT_FOOTER_FILE_COUNT_PREFIX_LEN: usize = 4;
pub(super) const SEGMENT_FOOTER_FILE_ENTRY_LEN: usize = 20;
pub(crate) const SEGMENT_FOOTER_TRACKED_FILES: [SegmentFile; 7] = [
    SegmentFile::MetaJson,
    SegmentFile::Symbols,
    SegmentFile::Series,
    SegmentFile::Chunks,
    SegmentFile::OooChunks,
    SegmentFile::ChunkIndex,
    SegmentFile::Indexes,
];
pub(super) const SEGMENT_FOOTER_ENCODED_LEN: usize = SEGMENT_FOOTER_HEADER_LEN
    + SEGMENT_FOOTER_FILE_COUNT_PREFIX_LEN
    + SEGMENT_FOOTER_TRACKED_FILES.len() * SEGMENT_FOOTER_FILE_ENTRY_LEN
    + SEGMENT_FOOTER_TRAILER_LEN;
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
        self.publish_with(&FilesystemSegmentPublishOps, false)
    }

    pub(super) fn publish_retryable(self) -> io::Result<PathBuf> {
        self.publish_with(&FilesystemSegmentPublishOps, true)
    }

    fn publish_with(
        self,
        ops: &impl SegmentPublishOps,
        allow_identical_reconciliation: bool,
    ) -> io::Result<PathBuf> {
        let temp_parent = required_parent(&self.temp_dir, "temporary segment directory")?;
        let final_parent = required_parent(&self.final_dir, "published segment directory")?;

        sync_flat_directory(&self.temp_dir, ops)?;
        match ops.rename(&self.temp_dir, &self.final_dir) {
            Ok(()) => {
                ops.sync_directory(final_parent)?;
                ops.sync_directory(temp_parent)?;
                Ok(self.final_dir)
            }
            Err(rename_error) if allow_identical_reconciliation && self.final_dir.is_dir() => {
                if !flat_directories_byte_identical(&self.temp_dir, &self.final_dir)? {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "published segment directory {} exists with different bytes after rename failed: {rename_error}",
                            self.final_dir.display()
                        ),
                    ));
                }
                sync_flat_directory(&self.final_dir, ops)?;
                ops.remove_dir_all(&self.temp_dir)?;
                ops.sync_directory(final_parent)?;
                ops.sync_directory(temp_parent)?;
                Ok(self.final_dir)
            }
            Err(rename_error) if self.final_dir.exists() => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "published segment path {} already exists after rename failed: {rename_error}",
                    self.final_dir.display()
                ),
            )),
            Err(error) => Err(error),
        }
    }
}

trait SegmentPublishOps {
    fn sync_file(&self, path: &Path) -> io::Result<()>;

    fn sync_directory(&self, path: &Path) -> io::Result<()>;

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
}

struct FilesystemSegmentPublishOps;

impl SegmentPublishOps for FilesystemSegmentPublishOps {
    fn sync_file(&self, path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        sync_directory(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn required_parent<'path>(path: &'path Path, description: &str) -> io::Result<&'path Path> {
    path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{description} {} has no parent", path.display()),
        )
    })
}

fn flat_file_entries(path: &Path) -> io::Result<BTreeMap<std::ffi::OsString, PathBuf>> {
    let mut entries = BTreeMap::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment directory contains non-file entry {}",
                    entry.path().display()
                ),
            ));
        }
        entries.insert(entry.file_name(), entry.path());
    }
    Ok(entries)
}

fn sync_flat_directory(path: &Path, ops: &impl SegmentPublishOps) -> io::Result<()> {
    for file_path in flat_file_entries(path)?.into_values() {
        ops.sync_file(&file_path)?;
    }
    ops.sync_directory(path)
}

fn flat_directories_byte_identical(left: &Path, right: &Path) -> io::Result<bool> {
    let left_entries = flat_file_entries(left)?;
    let right_entries = flat_file_entries(right)?;
    if left_entries.len() != right_entries.len() || !left_entries.keys().eq(right_entries.keys()) {
        return Ok(false);
    }

    const COMPARE_BUFFER_BYTES: usize = 1024 * 1024;
    let mut left_buffer = vec![0_u8; COMPARE_BUFFER_BYTES];
    let mut right_buffer = vec![0_u8; COMPARE_BUFFER_BYTES];
    for (name, left_path) in left_entries {
        let right_path = &right_entries[&name];
        if fs::metadata(&left_path)?.len() != fs::metadata(right_path)?.len() {
            return Ok(false);
        }
        let mut left_file = File::open(left_path)?;
        let mut right_file = File::open(right_path)?;
        loop {
            let left_read = left_file.read(&mut left_buffer)?;
            let right_read = right_file.read(&mut right_buffer)?;
            if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
                return Ok(false);
            }
            if left_read == 0 {
                break;
            }
        }
    }
    Ok(true)
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
    pub(super) fn from_chunk_entries<L>(entries: &[L]) -> Self
    where
        L: AsRef<[ChunkIndexEntry]>,
    {
        let mut summary = Self::default();
        for series_entries in entries {
            for entry in series_entries.as_ref() {
                summary.add_chunk(entry.kind, u64::from(entry.length));
            }
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
    pub(super) storage_schema: SegmentStorageSchema,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SegmentStorageSchema {
    Schema6,
    Schema7,
    #[default]
    Schema8,
}

impl SegmentStorageSchema {
    pub(super) const fn footer_version(self) -> u16 {
        match self {
            Self::Schema6 => SEGMENT_SCHEMA_VERSION_V6,
            Self::Schema7 => SEGMENT_SCHEMA_VERSION_V7,
            Self::Schema8 => SEGMENT_SCHEMA_VERSION_V8,
        }
    }
}

impl SegmentWriterConfig {
    pub fn new(segments_dir: impl AsRef<Path>, segment_duration: Duration) -> Self {
        Self {
            segments_dir: segments_dir.as_ref().to_path_buf(),
            segment_duration,
            segment_id_provider: Arc::new(RandomSegmentIdProvider),
            storage_schema: SegmentStorageSchema::Schema8,
        }
    }

    pub fn with_storage_schema(mut self, storage_schema: SegmentStorageSchema) -> Self {
        self.storage_schema = storage_schema;
        self
    }

    /// Returns the exact on-disk schema selected for newly written segments.
    ///
    /// Startup coordinators use this accessor to validate a writer before
    /// transferring its configuration and shared segment-ID provider into a
    /// specialized publication path.
    pub const fn storage_schema(&self) -> SegmentStorageSchema {
        self.storage_schema
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

    /// Allocates one stable identity for a retryable logical segment attempt.
    ///
    /// Live sealing retains the returned ID with its immutable input
    /// fragments. Rebuilding a failed writer must reuse that ID rather than
    /// advancing the provider and publishing a second logical segment.
    pub fn allocate_segment_id(&self, start_ms: u64, end_ms: u64) -> io::Result<SegmentId> {
        self.segment_id_provider
            .next_segment_id(start_ms, end_ms)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
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

    pub(super) fn add_metadata_batch(&mut self, elapsed: Duration) {
        self.wall_elapsed = self.wall_elapsed.saturating_add(elapsed);
        self.metadata = self.metadata.saturating_add(elapsed);
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

#[cfg(test)]
mod publish_tests {
    use std::cell::{Cell, RefCell};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum PublishCall {
        SyncFile(PathBuf),
        SyncDirectory(PathBuf),
        Rename { from: PathBuf, to: PathBuf },
        RemoveDirAll(PathBuf),
    }

    #[derive(Default)]
    struct RecordingPublishOps {
        calls: RefCell<Vec<PublishCall>>,
        fail_at: Cell<Option<usize>>,
    }

    impl RecordingPublishOps {
        fn failing_at(call_index: usize) -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
                fail_at: Cell::new(Some(call_index)),
            }
        }

        fn record(&self, call: PublishCall) -> io::Result<()> {
            let call_index = self.calls.borrow().len();
            self.calls.borrow_mut().push(call);
            if self.fail_at.get() == Some(call_index) {
                return Err(io::Error::other(format!(
                    "injected publish operation failure at call {call_index}"
                )));
            }
            Ok(())
        }

        fn calls(&self) -> Vec<PublishCall> {
            self.calls.borrow().clone()
        }
    }

    impl SegmentPublishOps for RecordingPublishOps {
        fn sync_file(&self, path: &Path) -> io::Result<()> {
            self.record(PublishCall::SyncFile(path.to_path_buf()))?;
            File::open(path)?.sync_all()
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            self.record(PublishCall::SyncDirectory(path.to_path_buf()))?;
            sync_directory(path)
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.record(PublishCall::Rename {
                from: from.to_path_buf(),
                to: to.to_path_buf(),
            })?;
            fs::rename(from, to)
        }

        fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
            self.record(PublishCall::RemoveDirAll(path.to_path_buf()))?;
            fs::remove_dir_all(path)
        }
    }

    fn paths(root: &Path) -> SegmentPaths {
        SegmentPaths::new(root, SegmentId::new(1_000, 2_000).unwrap())
    }

    #[test]
    fn publish_syncs_temp_files_and_directory_before_rename_then_both_parents() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("b"), b"second").unwrap();
        fs::write(pending.path().join("a"), b"first").unwrap();
        let ops = RecordingPublishOps::default();

        let published = pending.publish_with(&ops, false).unwrap();

        assert_eq!(published, paths.dir());
        assert_eq!(
            ops.calls(),
            vec![
                PublishCall::SyncFile(paths.temp_dir().join("a")),
                PublishCall::SyncFile(paths.temp_dir().join("b")),
                PublishCall::SyncDirectory(paths.temp_dir()),
                PublishCall::Rename {
                    from: paths.temp_dir(),
                    to: paths.dir(),
                },
                PublishCall::SyncDirectory(tempdir.path().to_path_buf()),
                PublishCall::SyncDirectory(tempdir.path().join(".tmp")),
            ]
        );
    }

    #[test]
    fn publish_reuses_an_existing_byte_identical_segment_directory() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("a"), b"same").unwrap();
        fs::create_dir_all(paths.dir()).unwrap();
        fs::write(paths.dir().join("a"), b"same").unwrap();

        let published = pending.publish_retryable().unwrap();

        assert_eq!(published, paths.dir());
        assert!(!paths.temp_dir().exists());
        assert_eq!(fs::read(paths.dir().join("a")).unwrap(), b"same");
    }

    #[test]
    fn ordinary_publish_rejects_an_existing_byte_identical_segment_directory() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("a"), b"same").unwrap();
        fs::create_dir_all(paths.dir()).unwrap();
        fs::write(paths.dir().join("a"), b"same").unwrap();

        let error = pending.publish().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(paths.temp_dir().exists());
        assert_eq!(fs::read(paths.dir().join("a")).unwrap(), b"same");
    }

    #[test]
    fn identical_reconciliation_syncs_final_files_before_removing_temp() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("a"), b"same").unwrap();
        fs::create_dir_all(paths.dir()).unwrap();
        fs::write(paths.dir().join("a"), b"same").unwrap();
        let ops = RecordingPublishOps::default();

        let published = pending.publish_with(&ops, true).unwrap();

        assert_eq!(published, paths.dir());
        assert_eq!(
            ops.calls(),
            vec![
                PublishCall::SyncFile(paths.temp_dir().join("a")),
                PublishCall::SyncDirectory(paths.temp_dir()),
                PublishCall::Rename {
                    from: paths.temp_dir(),
                    to: paths.dir(),
                },
                PublishCall::SyncFile(paths.dir().join("a")),
                PublishCall::SyncDirectory(paths.dir()),
                PublishCall::RemoveDirAll(paths.temp_dir()),
                PublishCall::SyncDirectory(tempdir.path().to_path_buf()),
                PublishCall::SyncDirectory(tempdir.path().join(".tmp")),
            ]
        );
        assert!(!paths.temp_dir().exists());
    }

    #[test]
    fn publish_rejects_and_retains_a_different_existing_segment_directory() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("a"), b"new").unwrap();
        fs::create_dir_all(paths.dir()).unwrap();
        fs::write(paths.dir().join("a"), b"old").unwrap();

        let error = pending.publish().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(paths.temp_dir().exists());
        assert_eq!(fs::read(paths.dir().join("a")).unwrap(), b"old");
    }

    #[test]
    fn temp_file_sync_failure_propagates_before_rename() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("a"), b"pending").unwrap();
        let ops = RecordingPublishOps::failing_at(0);

        let error = pending.publish_with(&ops, false).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(
            ops.calls(),
            vec![PublishCall::SyncFile(paths.temp_dir().join("a"))]
        );
        assert!(paths.temp_dir().exists());
        assert!(!paths.dir().exists());
    }

    #[test]
    fn parent_sync_failure_after_rename_propagates_with_final_directory_intact() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("a"), b"pending").unwrap();
        let ops = RecordingPublishOps::failing_at(3);

        let error = pending.publish_with(&ops, false).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(
            ops.calls(),
            vec![
                PublishCall::SyncFile(paths.temp_dir().join("a")),
                PublishCall::SyncDirectory(paths.temp_dir()),
                PublishCall::Rename {
                    from: paths.temp_dir(),
                    to: paths.dir(),
                },
                PublishCall::SyncDirectory(tempdir.path().to_path_buf()),
            ]
        );
        assert!(!paths.temp_dir().exists());
        assert_eq!(fs::read(paths.dir().join("a")).unwrap(), b"pending");
    }

    #[test]
    fn final_file_sync_failure_retains_both_identical_directories() {
        let tempdir = tempfile::tempdir().unwrap();
        let paths = paths(tempdir.path());
        let pending = paths.create_temp_dir().unwrap();
        fs::write(pending.path().join("a"), b"same").unwrap();
        fs::create_dir_all(paths.dir()).unwrap();
        fs::write(paths.dir().join("a"), b"same").unwrap();
        let ops = RecordingPublishOps::failing_at(3);

        let error = pending.publish_with(&ops, true).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(
            ops.calls(),
            vec![
                PublishCall::SyncFile(paths.temp_dir().join("a")),
                PublishCall::SyncDirectory(paths.temp_dir()),
                PublishCall::Rename {
                    from: paths.temp_dir(),
                    to: paths.dir(),
                },
                PublishCall::SyncFile(paths.dir().join("a")),
            ]
        );
        assert_eq!(fs::read(paths.temp_dir().join("a")).unwrap(), b"same");
        assert_eq!(fs::read(paths.dir().join("a")).unwrap(), b"same");
    }
}
