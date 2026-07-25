use super::*;

mod entries;
mod labels;
mod ordering;
mod preflight;
mod record;
mod seal;

pub(super) use entries::WriterSeriesEntryStore;
pub(crate) use labels::segment_series_id;
// Keep the original segment-internal writer facade paths after moving the
// implementations into focused child modules.
#[allow(unused_imports)]
pub(super) use labels::{
    apply_flat_interned_label_metadata, apply_flat_interned_label_metadata_counted,
    apply_label_visitor, apply_label_visitor_with_kind, apply_segment_metadata,
    canonical_segment_metadata, update_label_value_time_ranges,
};
#[cfg(test)]
pub(super) use labels::{
    encode_borrowed_canonical_segment_labels, encode_canonical_segment_labels,
    encode_flat_interned_label_metadata, encode_flat_interned_sorted_labels,
    encode_label_visitor_metadata,
};
#[allow(unused_imports)]
pub(super) use ordering::{
    finalize_segment_symbol_ids, metric_query_series_order, old_to_new_series_refs,
    remap_symbol_id, reorder_vec_by_old_indices, rewrite_chunks_in_identity_series_order,
    rewrite_chunks_in_series_major_order, synthesize_missing_metric_name,
};
use preflight::preflight_existing_store_schema;
#[allow(unused_imports)]
pub(super) use record::{ensure_local_series_with_kind, validate_ordered_samples};
#[allow(unused_imports)]
pub(super) use seal::{collect_segment_file_sizes, time_flush_stage};

pub(super) struct ActiveSegment {
    pub(super) id: SegmentId,
    /// Whether the caller retained immutable source data and selected this
    /// segment's stable identity explicitly for exact publication retry.
    pub(super) retryable_publication: bool,
    pub(super) start_ms: u64,
    pub(super) end_ms: u64,
    pub(super) datapoints: u64,
    pub(super) series_map: HashMap<u32, u32>,
    pub(super) symbols: SegmentSymbols,
    pub(super) series_entries: WriterSeriesEntryStore,
    pub(super) normalized_names: NormalizedNameCache,
    pub(super) metadata_hash_scratch: Vec<u8>,
    pub(super) metadata_label_scratch: Vec<(Arc<str>, SourceLabelValue)>,
    pub(super) chunk_entries: InlineOneChunkEntryStore,
    pub(super) chunks: ChunkWriter,
    pub(super) payload_lane: SegmentPayloadLane,
    pub(super) temp_dir: SegmentTempDir,
    pub(super) metric_query_ordered_input: bool,
    pub(super) metric_query_ordered_batch_seen: bool,
    pub(super) metric_query_ordered_series_remaining: usize,
    pub(super) deferred_flat_label_metadata: bool,
    pub(super) recording_closed: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriterSeriesEntry {
    pub(crate) series_id: u64,
    pub(crate) kind_mask: u8,
    pub(crate) labels: Vec<(u32, u32)>,
}

#[cfg(test)]
impl crate::storage::series::SeriesEntryView for WriterSeriesEntry {
    fn series_id(&self) -> u64 {
        self.series_id
    }

    fn kind_mask(&self) -> u8 {
        self.kind_mask
    }

    fn labels(&self) -> &[(u32, u32)] {
        &self.labels
    }
}

#[derive(Debug, Clone)]
pub struct SegmentSeriesMetadata {
    pub(super) series_id: u64,
    pub(super) labels: Vec<(String, String)>,
}

pub struct SegmentSeriesMetadataBuilder {
    labels: BTreeMap<String, String>,
    metric_name_seen: bool,
}

pub struct SegmentWriter {
    pub(super) config: SegmentWriterConfig,
    pub(super) active: Option<ActiveSegment>,
    pub(super) pending_manifest: Option<PendingManifestPublication>,
    pub(super) next_segment_id_override: Option<SegmentId>,
    pub(super) next_payload_lane: SegmentPayloadLane,
    pub(super) last_flush_profile: Option<SegmentFlushProfile>,
    pub(super) record_profile: SegmentRecordProfile,
    #[cfg(test)]
    pub(super) fail_next_ordinary_current_directory_sync: bool,
}

pub(super) struct PendingManifestPublication {
    pub(super) attempt: ManifestAppendAttempt,
    pub(super) meta: SegmentMeta,
    pub(super) published_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFlushOutcome {
    pub meta: SegmentMeta,
    pub published_dir: PathBuf,
    pub manifest_cut: ManifestCut,
}

/// Selects which immutable payload file receives every chunk in one segment.
///
/// Ordinary head windows use [`SegmentPayloadLane::InOrder`]. A window that
/// arrives only after its event-time range was already sealed uses
/// [`SegmentPayloadLane::OutOfOrder`] so its overlapping segment retains the
/// late-arrival precedence encoded by `chunk_index.bin`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum SegmentPayloadLane {
    #[default]
    InOrder,
    OutOfOrder,
}

impl SegmentPayloadLane {
    pub(super) const fn file(self) -> SegmentFile {
        match self {
            Self::InOrder => SegmentFile::Chunks,
            Self::OutOfOrder => SegmentFile::OooChunks,
        }
    }

    pub(super) const fn file_id(self) -> u8 {
        match self {
            Self::InOrder => 0,
            Self::OutOfOrder => 1,
        }
    }
}

/// A fresh, metric-query-ordered writer batch whose flat label metadata is
/// populated only after its sample containers have been released.
///
/// Dropping an unfinished batch aborts its temporary segment.
pub struct DeferredFlatMetadataBatch<'writer, 'labels, S: SymbolTable> {
    writer: &'writer mut SegmentWriter,
    labelsets: &'labels FlatInternedLabelSetStore<S>,
    finished: bool,
}

impl SegmentWriter {
    pub fn new(config: SegmentWriterConfig) -> io::Result<Self> {
        preflight_existing_store_schema(&config)?;
        fs::create_dir_all(&config.segments_dir)?;
        Ok(Self {
            config,
            active: None,
            pending_manifest: None,
            next_segment_id_override: None,
            next_payload_lane: SegmentPayloadLane::InOrder,
            last_flush_profile: None,
            record_profile: SegmentRecordProfile::default(),
            #[cfg(test)]
            fail_next_ordinary_current_directory_sync: false,
        })
    }

    pub fn last_flush_profile(&self) -> Option<&SegmentFlushProfile> {
        self.last_flush_profile.as_ref()
    }

    pub fn record_profile(&self) -> SegmentRecordProfile {
        self.record_profile
    }

    /// Clones the exact configuration of a writer that has not begun work.
    ///
    /// The clone preserves the same shared segment-ID provider, including its
    /// current provider state. This is intended for a startup-only ownership
    /// transfer into another writer coordinator. A writer with an active or
    /// retryable segment, a one-shot override, a non-default payload lane, or
    /// any prior record/flush work cannot be transferred.
    pub fn pristine_config_for_takeover(&self) -> io::Result<SegmentWriterConfig> {
        if self.active.is_some()
            || self.pending_manifest.is_some()
            || self.next_segment_id_override.is_some()
            || self.next_payload_lane != SegmentPayloadLane::default()
            || self.last_flush_profile.is_some()
            || self.record_profile != SegmentRecordProfile::default()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment writer configuration takeover requires a pristine writer",
            ));
        }
        Ok(self.config.clone())
    }

    /// Whether a failed flush retained an exact manifest publication attempt.
    ///
    /// A caller may retry such a writer directly. If this is false after a
    /// flush error, the active segment was consumed before a retryable
    /// manifest attempt existed and must be rebuilt from its retained source
    /// data with the same [`SegmentId`].
    pub fn has_retryable_manifest_attempt(&self) -> bool {
        self.pending_manifest.is_some()
    }

    #[cfg(test)]
    fn fail_next_ordinary_current_directory_sync(&mut self) {
        self.fail_next_ordinary_current_directory_sync = true;
    }

    /// Forces the identity of the next segment created by this otherwise
    /// empty writer.
    ///
    /// This is intentionally one-shot and exists for rebuilding a failed live
    /// seal from retained immutable fragments. The ID's time range is checked
    /// again when the first record establishes the segment window.
    pub fn set_next_segment_id_for_retry(&mut self, id: SegmentId) -> io::Result<()> {
        if self.active.is_some()
            || self.pending_manifest.is_some()
            || self.next_segment_id_override.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot select a retry segment ID while a segment is active, pending, or already overridden",
            ));
        }
        self.next_segment_id_override = Some(id);
        Ok(())
    }

    /// Routes every chunk in the next segment to `lane`.
    ///
    /// The selection is one-shot: after the next segment window is created,
    /// subsequent segments return to the in-order lane. Callers must select a
    /// lane before reserving or recording any sample for that segment.
    pub fn set_next_segment_payload_lane(&mut self, lane: SegmentPayloadLane) -> io::Result<()> {
        if self.active.is_some() || self.pending_manifest.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot select the next segment payload lane while a segment is active or awaiting manifest reconciliation",
            ));
        }
        self.next_payload_lane = lane;
        Ok(())
    }
}

pub(super) fn file_len(path: &Path) -> io::Result<u64> {
    Ok(fs::metadata(path)?.len())
}

pub(super) fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub(super) fn deterministic_segment_ulid(
    seed: u64,
    start_ms: u64,
    end_ms: u64,
    ordinal: u64,
) -> Ulid {
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

pub(super) enum SourceLabelValue {
    Symbol(SymbolId),
    Owned(Arc<str>),
}

pub(super) const MAX_NORMALIZED_NAME_CACHE_ENTRIES: usize = 262_144;

pub(super) struct NormalizedNameCache {
    metric_label_name: Arc<str>,
    label_names: HashMap<SymbolId, Arc<str>>,
    metric_names: HashMap<SymbolId, Arc<str>>,
    max_entries: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ChunkRewriteStats {
    pub(super) frames: u64,
    pub(super) payload_bytes: u64,
}

pub(super) struct FinalizedSegmentMetadata {
    symbols: SegmentSymbols,
    series_entries: WriterSeriesEntryStore,
    postings: ExactPostingsIndex,
    label_value_time_ranges: LabelValueTimeRangeIndex,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum TreeEntry {
        Directory(PathBuf),
        File(PathBuf, Vec<u8>),
    }

    fn writer_config(root: &Path, schema: SegmentStorageSchema, seed: u64) -> SegmentWriterConfig {
        SegmentWriterConfig::new(root, Duration::from_secs(10))
            .with_storage_schema(schema)
            .with_deterministic_segment_ids(seed)
    }

    fn write_one_segment(root: &Path, schema: SegmentStorageSchema, seed: u64, timestamp_ms: u64) {
        let mut writer = SegmentWriter::new(writer_config(root, schema, seed)).unwrap();
        writer
            .record_sample(
                SeriesRef::new(u32::try_from(seed).unwrap()),
                timestamp_ms,
                1.0,
            )
            .unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn pristine_takeover_clones_the_exact_config_and_shared_id_provider() {
        let tempdir = tempfile::tempdir().unwrap();
        let seed = 0x51_7a;
        let writer = SegmentWriter::new(writer_config(
            tempdir.path(),
            SegmentStorageSchema::Schema8,
            seed,
        ))
        .unwrap();

        let takeover = writer.pristine_config_for_takeover().unwrap();
        assert_eq!(takeover.segments_dir, writer.config.segments_dir);
        assert_eq!(takeover.segment_duration, writer.config.segment_duration);
        assert_eq!(takeover.storage_schema(), writer.config.storage_schema());

        let first = takeover.allocate_segment_id(0, 10_000).unwrap();
        let second = writer.config.allocate_segment_id(0, 10_000).unwrap();
        assert_eq!(
            first,
            SegmentId::with_ulid(0, 10_000, deterministic_segment_ulid(seed, 0, 10_000, 0))
                .unwrap()
        );
        assert_eq!(
            second,
            SegmentId::with_ulid(0, 10_000, deterministic_segment_ulid(seed, 0, 10_000, 1))
                .unwrap()
        );
    }

    #[test]
    fn takeover_rejects_every_nonpristine_writer_state() {
        let active_root = tempfile::tempdir().unwrap();
        let mut active = SegmentWriter::new(writer_config(
            active_root.path(),
            SegmentStorageSchema::Schema8,
            1,
        ))
        .unwrap();
        active.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();
        assert_eq!(
            active.pristine_config_for_takeover().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        let override_root = tempfile::tempdir().unwrap();
        let mut overridden = SegmentWriter::new(writer_config(
            override_root.path(),
            SegmentStorageSchema::Schema8,
            2,
        ))
        .unwrap();
        let id = overridden.config.allocate_segment_id(0, 10_000).unwrap();
        overridden.set_next_segment_id_for_retry(id).unwrap();
        assert!(overridden.pristine_config_for_takeover().is_err());

        let lane_root = tempfile::tempdir().unwrap();
        let mut lane = SegmentWriter::new(writer_config(
            lane_root.path(),
            SegmentStorageSchema::Schema8,
            3,
        ))
        .unwrap();
        lane.set_next_segment_payload_lane(SegmentPayloadLane::OutOfOrder)
            .unwrap();
        assert!(lane.pristine_config_for_takeover().is_err());

        let flushed_root = tempfile::tempdir().unwrap();
        let mut flushed = SegmentWriter::new(writer_config(
            flushed_root.path(),
            SegmentStorageSchema::Schema8,
            4,
        ))
        .unwrap();
        flushed
            .record_sample(SeriesRef::new(4), 1_000, 4.0)
            .unwrap();
        flushed.flush().unwrap();
        assert!(flushed.active.is_none());
        assert!(flushed.pristine_config_for_takeover().is_err());

        let profile_root = tempfile::tempdir().unwrap();
        let mut profiled = SegmentWriter::new(writer_config(
            profile_root.path(),
            SegmentStorageSchema::Schema8,
            5,
        ))
        .unwrap();
        profiled.record_profile.samples = 1;
        assert!(profiled.pristine_config_for_takeover().is_err());
    }

    #[test]
    fn storage_schema_accessor_reports_the_configured_schema() {
        for schema in [
            SegmentStorageSchema::Schema6,
            SegmentStorageSchema::Schema7,
            SegmentStorageSchema::Schema8,
        ] {
            let tempdir = tempfile::tempdir().unwrap();
            assert_eq!(
                writer_config(tempdir.path(), schema, 1).storage_schema(),
                schema
            );
        }
    }

    fn snapshot_tree(root: &Path) -> Vec<TreeEntry> {
        fn visit(root: &Path, dir: &Path, snapshot: &mut Vec<TreeEntry>) {
            let mut entries = fs::read_dir(dir)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            entries.sort_by_key(fs::DirEntry::file_name);
            for entry in entries {
                let path = entry.path();
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                let file_type = entry.file_type().unwrap();
                if file_type.is_dir() {
                    snapshot.push(TreeEntry::Directory(relative));
                    visit(root, &path, snapshot);
                } else if file_type.is_file() {
                    snapshot.push(TreeEntry::File(relative, fs::read(path).unwrap()));
                } else {
                    panic!("unexpected filesystem entry in segment fixture");
                }
            }
        }

        let mut snapshot = Vec::new();
        visit(root, root, &mut snapshot);
        snapshot
    }

    fn assert_schema8_upgrade_is_rejected_without_mutation(existing: SegmentStorageSchema) {
        let tempdir = tempfile::tempdir().unwrap();
        write_one_segment(tempdir.path(), existing, 1, 1_000);
        let before = snapshot_tree(tempdir.path());

        let error = SegmentWriter::new(writer_config(
            tempdir.path(),
            SegmentStorageSchema::Schema8,
            2,
        ))
        .err()
        .expect("cross-schema append must fail");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("schema preflight"));
        assert!(error.to_string().contains("configured footer schema 8"));
        assert_eq!(snapshot_tree(tempdir.path()), before);
    }

    #[test]
    fn same_schema_append_is_allowed() {
        for (schema, seed) in [
            (SegmentStorageSchema::Schema6, 10),
            (SegmentStorageSchema::Schema7, 20),
            (SegmentStorageSchema::Schema8, 30),
        ] {
            let tempdir = tempfile::tempdir().unwrap();
            write_one_segment(tempdir.path(), schema, seed, 1_000);
            write_one_segment(tempdir.path(), schema, seed + 1, 11_000);

            let inventory = read_manifest_inventory(tempdir.path().join("manifest"))
                .unwrap()
                .expect("manifest inventory");
            assert_eq!(inventory.segments.len(), 2);
            for segment in inventory.segments {
                read_segment_footer_for_exact_schema(
                    &tempdir.path().join(segment.segment_id),
                    schema.footer_version(),
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn schema6_to_schema8_is_rejected_without_mutation() {
        assert_schema8_upgrade_is_rejected_without_mutation(SegmentStorageSchema::Schema6);
    }

    #[test]
    fn schema7_to_schema8_is_rejected_without_mutation() {
        assert_schema8_upgrade_is_rejected_without_mutation(SegmentStorageSchema::Schema7);
    }

    #[test]
    fn malformed_live_footer_is_propagated_without_mutation() {
        let tempdir = tempfile::tempdir().unwrap();
        write_one_segment(tempdir.path(), SegmentStorageSchema::Schema8, 1, 1_000);
        let inventory = read_manifest_inventory(tempdir.path().join("manifest"))
            .unwrap()
            .expect("manifest inventory");
        let footer_path = tempdir
            .path()
            .join(&inventory.segments[0].segment_id)
            .join(SegmentFile::Footer.filename());
        let mut footer = fs::read(&footer_path).unwrap();
        let last = footer.len() - 1;
        footer[last] ^= 0xff;
        fs::write(&footer_path, footer).unwrap();
        let before = snapshot_tree(tempdir.path());

        let error = SegmentWriter::new(writer_config(
            tempdir.path(),
            SegmentStorageSchema::Schema8,
            2,
        ))
        .err()
        .expect("malformed live footer must fail writer preflight");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("schema preflight"));
        assert!(error.to_string().contains("footer checksum mismatch"));
        assert_eq!(snapshot_tree(tempdir.path()), before);
    }

    #[test]
    fn fresh_root_is_created_and_writable() {
        let tempdir = tempfile::tempdir().unwrap();
        let root = tempdir.path().join("fresh-segments");

        assert!(!root.exists());
        write_one_segment(&root, SegmentStorageSchema::Schema8, 1, 1_000);

        let inventory = read_manifest_inventory(root.join("manifest"))
            .unwrap()
            .expect("manifest inventory");
        assert_eq!(inventory.segments.len(), 1);
    }

    #[test]
    fn explicit_in_order_lane_preserves_default_segment_bytes() {
        for schema in [
            SegmentStorageSchema::Schema6,
            SegmentStorageSchema::Schema7,
            SegmentStorageSchema::Schema8,
        ] {
            let default_root = tempfile::tempdir().unwrap();
            write_one_segment(default_root.path(), schema, 41, 1_000);

            let explicit_root = tempfile::tempdir().unwrap();
            let mut writer =
                SegmentWriter::new(writer_config(explicit_root.path(), schema, 41)).unwrap();
            writer
                .set_next_segment_payload_lane(SegmentPayloadLane::InOrder)
                .unwrap();
            writer
                .record_sample(SeriesRef::new(41), 1_000, 1.0)
                .unwrap();
            writer.flush().unwrap();

            assert_eq!(
                snapshot_tree(default_root.path()),
                snapshot_tree(explicit_root.path())
            );
        }
    }

    #[test]
    fn retry_override_reuses_one_preallocated_segment_identity() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = writer_config(tempdir.path(), SegmentStorageSchema::Schema8, 71);
        let id = config.allocate_segment_id(0, 10_000).unwrap();
        let mut writer = SegmentWriter::new(config).unwrap();
        writer.set_next_segment_id_for_retry(id).unwrap();
        writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();

        let outcome = writer
            .flush_with_outcome()
            .unwrap()
            .expect("one segment was published");

        assert_eq!(outcome.meta.segment_id, id.dir_name());
        let inventory = read_manifest_inventory(tempdir.path().join("manifest"))
            .unwrap()
            .expect("manifest inventory");
        assert_eq!(inventory.segments.len(), 1);
        assert_eq!(inventory.segments[0].segment_id, id.dir_name());
    }

    #[test]
    fn ordinary_manifest_failure_does_not_install_live_retry_state_or_block_the_next_lane() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(writer_config(
            tempdir.path(),
            SegmentStorageSchema::Schema8,
            73,
        ))
        .unwrap();
        writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();

        writer.fail_next_ordinary_current_directory_sync();
        let error = writer.flush().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(
            !writer.has_retryable_manifest_attempt(),
            "ordinary ingestion has no retained immutable source bound to this writer"
        );
        writer
            .set_next_segment_payload_lane(SegmentPayloadLane::OutOfOrder)
            .expect("an ordinary manifest failure must not poison the next writer lane");
        writer
            .record_sample(SeriesRef::new(2), 11_000, 2.0)
            .unwrap();
        writer.flush().unwrap();
        let inventory = read_manifest_inventory(tempdir.path().join("manifest"))
            .unwrap()
            .expect("manifest inventory");
        assert_eq!(
            inventory.segments.len(),
            2,
            "the readable first CURRENT and the later window must both remain published"
        );
    }

    #[test]
    fn ordinary_deterministic_id_collision_is_strict_and_appends_no_duplicate_manifest_record() {
        let tempdir = tempfile::tempdir().unwrap();
        write_one_segment(tempdir.path(), SegmentStorageSchema::Schema8, 75, 1_000);
        let before = read_manifest_inventory(tempdir.path().join("manifest"))
            .unwrap()
            .expect("initial manifest inventory");
        assert_eq!(before.segments.len(), 1);

        let mut colliding = SegmentWriter::new(writer_config(
            tempdir.path(),
            SegmentStorageSchema::Schema8,
            75,
        ))
        .unwrap();
        colliding
            .record_sample(SeriesRef::new(75), 1_000, 1.0)
            .unwrap();
        let error = colliding.flush().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);

        let after = read_manifest_inventory(tempdir.path().join("manifest"))
            .unwrap()
            .expect("manifest inventory after rejected collision");
        assert_eq!(after, before);
    }

    #[test]
    fn explicit_retry_identity_retains_and_reconciles_the_exact_manifest_attempt() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest = ManifestCoordinator::shared(tempdir.path().join("manifest")).unwrap();
        let config = writer_config(tempdir.path(), SegmentStorageSchema::Schema8, 74);
        let id = config.allocate_segment_id(0, 10_000).unwrap();
        let mut writer = SegmentWriter::new(config).unwrap();
        writer.set_next_segment_id_for_retry(id).unwrap();
        writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();

        manifest.fail_next_completed_manifest_sync();
        let error = writer.flush_with_outcome().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(writer.has_retryable_manifest_attempt());

        let outcome = writer
            .flush_with_outcome()
            .unwrap()
            .expect("the retained exact attempt must reconcile");
        assert_eq!(outcome.meta.segment_id, id.dir_name());
        assert!(!writer.has_retryable_manifest_attempt());
        let inventory = read_manifest_inventory(tempdir.path().join("manifest"))
            .unwrap()
            .expect("manifest inventory");
        assert_eq!(inventory.segments.len(), 1);
        assert_eq!(inventory.segments[0].segment_id, id.dir_name());
    }

    #[test]
    fn retry_override_rejects_a_different_range_without_consuming_it() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = writer_config(tempdir.path(), SegmentStorageSchema::Schema8, 72);
        let id = config.allocate_segment_id(0, 10_000).unwrap();
        let mut writer = SegmentWriter::new(config).unwrap();
        writer.set_next_segment_id_for_retry(id).unwrap();

        let error = writer
            .record_sample(SeriesRef::new(1), 11_000, 1.0)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("does not match record range"));

        writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();
        let outcome = writer
            .flush_with_outcome()
            .unwrap()
            .expect("retry ID remains available");
        assert_eq!(outcome.meta.segment_id, id.dir_name());
    }

    #[test]
    fn non_segment_runtime_entries_do_not_make_a_root_manifestless() {
        let tempdir = tempfile::tempdir().unwrap();
        let root = tempdir.path().join("runtime-only-segments");
        fs::create_dir_all(root.join(".tmp")).unwrap();
        fs::write(root.join("runtime.log"), b"runtime state").unwrap();

        write_one_segment(&root, SegmentStorageSchema::Schema8, 1, 1_000);

        let inventory = read_manifest_inventory(root.join("manifest"))
            .unwrap()
            .expect("manifest inventory");
        assert_eq!(inventory.segments.len(), 1);
    }

    #[test]
    fn manifestless_segment_root_is_rejected_without_mutation() {
        let tempdir = tempfile::tempdir().unwrap();
        write_one_segment(tempdir.path(), SegmentStorageSchema::Schema8, 1, 1_000);
        fs::remove_dir_all(tempdir.path().join("manifest")).unwrap();
        let before = snapshot_tree(tempdir.path());

        let error = SegmentWriter::new(writer_config(
            tempdir.path(),
            SegmentStorageSchema::Schema8,
            2,
        ))
        .err()
        .expect("manifestless sealed segment must fail writer preflight");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("manifestless segment path"));
        assert_eq!(snapshot_tree(tempdir.path()), before);
    }

    #[test]
    fn malformed_manifestless_segment_directory_is_rejected() {
        let tempdir = tempfile::tempdir().unwrap();
        fs::create_dir(tempdir.path().join("seg-malformed")).unwrap();

        let error = SegmentWriter::new(writer_config(
            tempdir.path(),
            SegmentStorageSchema::Schema8,
            1,
        ))
        .err()
        .expect("malformed manifestless segment directory must fail writer preflight");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("manifestless segment path"));
    }
}
