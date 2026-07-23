use super::*;

mod labels;
mod ordering;
mod preflight;
mod record;
mod seal;

pub(crate) use labels::segment_series_id;
// Keep the original segment-internal writer facade paths after moving the
// implementations into focused child modules.
#[allow(unused_imports)]
pub(super) use labels::{
    apply_flat_interned_label_metadata, apply_label_visitor, apply_label_visitor_with_kind,
    apply_segment_metadata, canonical_segment_metadata, encode_borrowed_canonical_segment_labels,
    encode_canonical_segment_labels, encode_flat_interned_label_metadata,
    encode_flat_interned_sorted_labels, encode_label_visitor_metadata,
    update_label_value_time_ranges,
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
    pub(super) start_ms: u64,
    pub(super) end_ms: u64,
    pub(super) datapoints: u64,
    pub(super) series_map: HashMap<u32, u32>,
    pub(super) metadata_present: Vec<bool>,
    pub(super) symbols: SegmentSymbols,
    pub(super) series_entries: Vec<SeriesEntry>,
    pub(super) normalized_names: NormalizedNameCache,
    pub(super) metadata_hash_scratch: Vec<u8>,
    pub(super) metadata_label_scratch: Vec<(Arc<str>, SourceLabelValue)>,
    pub(super) chunk_entries: Vec<Vec<ChunkIndexEntry>>,
    pub(super) chunks: ChunkWriter,
    pub(super) temp_dir: SegmentTempDir,
    pub(super) metric_query_ordered_input: bool,
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
    pub(super) last_flush_profile: Option<SegmentFlushProfile>,
    pub(super) record_profile: SegmentRecordProfile,
}

impl SegmentWriter {
    pub fn new(config: SegmentWriterConfig) -> io::Result<Self> {
        preflight_existing_store_schema(&config)?;
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
    series_entries: Vec<SeriesEntry>,
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
