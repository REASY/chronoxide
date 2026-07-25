use std::collections::{BTreeMap, HashMap};

use super::*;
use crate::storage::manifest::{ManifestCut, ManifestSnapshot};

impl SegmentStoreReader {
    /// Opens an immutable sealed inventory from one complete, validated
    /// manifest snapshot.
    ///
    /// Unlike the legacy materialized-inventory entry points, this retains the
    /// exact manifest cut so a later generation can prove that it extends this
    /// one before sharing any readers.
    pub fn open_manifest_snapshot(
        segments_dir: impl AsRef<Path>,
        snapshot: &ManifestSnapshot,
    ) -> io::Result<Self> {
        Self::open_manifest_snapshot_with_options(
            segments_dir,
            snapshot,
            SegmentStoreOpenOptions::default(),
        )
    }

    pub fn open_manifest_snapshot_with_options(
        segments_dir: impl AsRef<Path>,
        snapshot: &ManifestSnapshot,
        options: SegmentStoreOpenOptions,
    ) -> io::Result<Self> {
        validate_live_manifest_snapshot(snapshot)?;
        let mut store =
            Self::open_manifest_inventory_with_options(segments_dir, &snapshot.inventory, options)?;
        store.manifest_snapshot = Some(Arc::new(snapshot.clone()));
        Ok(store)
    }

    /// Builds the next immutable sealed inventory while sharing every
    /// unchanged `SegmentReader` and the process-scoped metadata runtime.
    ///
    /// `snapshot` must come from `read_manifest_snapshot` or
    /// `refresh_manifest_snapshot`. A changed/truncated prefix, duplicate live
    /// seal record, or snapshot/inventory disagreement fails closed. The old
    /// store remains untouched and queryable on every error. Tombstones remove
    /// readers only from the returned inventory; physical directory deletion
    /// is deliberately left to a later lease-aware retention implementation.
    pub fn refresh_manifest_snapshot(
        &self,
        segments_dir: impl AsRef<Path>,
        snapshot: &ManifestSnapshot,
    ) -> io::Result<Self> {
        let previous = self.manifest_snapshot.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "incremental refresh requires a store opened from a manifest snapshot",
            )
        })?;
        validate_live_manifest_snapshot(snapshot)?;
        validate_manifest_successor(previous, snapshot)?;

        let segments_dir = segments_dir.as_ref();
        let previous_by_id = self
            .segments
            .iter()
            .map(|reader| (reader.meta.segment_id.as_str(), Arc::clone(reader)))
            .collect::<HashMap<_, _>>();

        // Walk the validated suffix itself, rather than only its final
        // materialized inventory. A seal followed by a tombstone in the same
        // suffix is still authenticated and opened before its candidate Arc is
        // dropped; a tombstone cannot conceal a malformed published segment.
        let mut new_manifest_segments = Vec::new();
        let mut seen_new_ids = HashSet::new();
        for record in &snapshot.records[previous.records.len()..] {
            let ManifestRecord::SegmentSealed(segment) = record else {
                continue;
            };
            if previous_by_id.contains_key(segment.segment_id.as_str())
                || !seen_new_ids.insert(segment.segment_id.as_str())
            {
                continue;
            }
            new_manifest_segments.push(segment);
        }
        let new_segment_dirs = new_manifest_segments
            .iter()
            .map(|segment| segments_dir.join(&segment.segment_id))
            .collect::<Vec<_>>();

        // Preserve the whole-store schema preflight rule for the suffix: no
        // new reader registers metadata before every newly referenced footer
        // has passed the configured schema policy.
        preflight_store_footers(&new_segment_dirs, self.open_options)?;

        let mut opened_by_id = HashMap::with_capacity(new_manifest_segments.len());
        for (manifest_segment, segment_dir) in
            new_manifest_segments.into_iter().zip(new_segment_dirs)
        {
            let reader = open_store_segment(
                segment_dir,
                self.open_options,
                self.metadata_runtime.clone(),
            )?;
            validate_manifest_segment_meta(manifest_segment, reader.meta())?;
            opened_by_id.insert(manifest_segment.segment_id.as_str(), Arc::new(reader));
        }

        let mut segments = Vec::with_capacity(snapshot.inventory.segments.len());
        for manifest_segment in &snapshot.inventory.segments {
            let reader = previous_by_id
                .get(manifest_segment.segment_id.as_str())
                .or_else(|| opened_by_id.get(manifest_segment.segment_id.as_str()))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "manifest inventory segment is missing after incremental store open",
                    )
                })?;
            validate_manifest_segment_meta(manifest_segment, reader.meta())?;
            segments.push(Arc::clone(reader));
        }

        sort_shared_segment_readers(&mut segments);
        let query_order = manifest_query_order(&segments, &snapshot.inventory)?;
        let retained_snapshot = if snapshot == previous {
            self.manifest_snapshot.clone()
        } else {
            Some(Arc::new(snapshot.clone()))
        };
        Ok(Self {
            segments,
            query_order,
            query_projection_config: self.query_projection_config.clone(),
            metadata_runtime: self.metadata_runtime.clone(),
            open_options: self.open_options,
            manifest_snapshot: retained_snapshot,
        })
    }

    /// Returns the exact validated manifest cut retained by a snapshot-backed
    /// inventory. Legacy store opens return `None`.
    pub fn validated_manifest_cut(&self) -> Option<&ManifestCut> {
        self.manifest_snapshot
            .as_deref()
            .map(|snapshot| &snapshot.cut)
    }
}

fn validate_live_manifest_snapshot(snapshot: &ManifestSnapshot) -> io::Result<()> {
    match &snapshot.cut {
        ManifestCut::Absent => {
            if !snapshot.records.is_empty() || !snapshot.inventory.segments.is_empty() {
                return Err(invalid_inventory(
                    "absent manifest cut contains records or live segments",
                ));
            }
        }
        ManifestCut::Present { .. } => {}
    }

    let materialized =
        ManifestInventory::from_records(snapshot.records.clone()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid manifest snapshot record: {error}"),
            )
        })?;
    if materialized != snapshot.inventory {
        return Err(invalid_inventory(
            "manifest snapshot inventory does not match its record stream",
        ));
    }

    // A retry may reconcile one intended record, but a completed manifest cut
    // must never contain two live seals for the same immutable directory.
    // Deletion followed by an explicit later reseal remains well-defined.
    let mut live = BTreeMap::<&str, &ManifestSegment>::new();
    for record in &snapshot.records {
        match record {
            ManifestRecord::SegmentSealed(segment) => {
                if live.insert(segment.segment_id.as_str(), segment).is_some() {
                    return Err(invalid_inventory(
                        "manifest snapshot contains duplicate live segment seals",
                    ));
                }
            }
            ManifestRecord::SegmentDeleted { segment_id } => {
                live.remove(segment_id.as_str());
            }
        }
    }

    let mut seen_inventory_ids = HashSet::with_capacity(snapshot.inventory.segments.len());
    for segment in &snapshot.inventory.segments {
        if !seen_inventory_ids.insert(segment.segment_id.as_str()) {
            return Err(invalid_inventory(
                "manifest snapshot inventory contains duplicate segment IDs",
            ));
        }
    }
    Ok(())
}

fn validate_manifest_successor(
    previous: &ManifestSnapshot,
    next: &ManifestSnapshot,
) -> io::Result<()> {
    match (&previous.cut, &next.cut) {
        (ManifestCut::Absent, _) => Ok(()),
        (ManifestCut::Present { .. }, ManifestCut::Absent) => Err(invalid_inventory(
            "manifest became absent after a published cut",
        )),
        (
            ManifestCut::Present {
                file_name: previous_name,
                validated_offset: previous_offset,
                prefix_sha256: previous_hash,
            },
            ManifestCut::Present {
                file_name: next_name,
                validated_offset: next_offset,
                prefix_sha256: next_hash,
            },
        ) if previous_name == next_name => {
            if !next.records.as_slice().starts_with(&previous.records) {
                return Err(invalid_inventory(
                    "manifest snapshot does not extend the previous record prefix",
                ));
            }
            if next_offset < previous_offset {
                return Err(invalid_inventory(
                    "manifest became shorter than its previous validated cut",
                ));
            }
            if next_offset == previous_offset
                && (next_hash != previous_hash || next.records != previous.records)
            {
                return Err(invalid_inventory(
                    "manifest bytes changed at the previous validated cut",
                ));
            }
            if next_offset > previous_offset && next.records.len() == previous.records.len() {
                return Err(invalid_inventory(
                    "manifest byte suffix contains no complete record",
                ));
            }
            Ok(())
        }
        (
            ManifestCut::Present {
                file_name: previous_name,
                ..
            },
            ManifestCut::Present {
                file_name: next_name,
                ..
            },
        ) => {
            if next_name <= previous_name {
                return Err(invalid_inventory(
                    "manifest rotation did not advance CURRENT generation",
                ));
            }
            // Version-1 manifests do not carry an explicit predecessor hash,
            // so a changed CURRENT is accepted only when the new generation
            // reproduces the complete prior logical record prefix. Merely
            // reproducing the live inventory is insufficient after
            // tombstones, and would also make the suffix offset ambiguous.
            if !next.records.as_slice().starts_with(&previous.records) {
                return Err(invalid_inventory(
                    "rotated manifest does not preserve the previous record prefix",
                ));
            }
            Ok(())
        }
    }
}

fn manifest_query_order(
    segments: &[Arc<SegmentReader>],
    inventory: &ManifestInventory,
) -> io::Result<Vec<usize>> {
    let time_order_by_id = segments
        .iter()
        .enumerate()
        .map(|(time_ordinal, segment)| (segment.meta.segment_id.as_str(), time_ordinal))
        .collect::<HashMap<_, _>>();
    inventory
        .segments
        .iter()
        .map(|segment| {
            time_order_by_id
                .get(segment.segment_id.as_str())
                .copied()
                .ok_or_else(|| {
                    invalid_inventory("manifest inventory segment is missing after store open")
                })
        })
        .collect()
}

fn sort_shared_segment_readers(segments: &mut [Arc<SegmentReader>]) {
    segments.sort_by(|left, right| {
        left.meta
            .start_ms
            .cmp(&right.meta.start_ms)
            .then_with(|| left.meta.end_ms.cmp(&right.meta.end_ms))
            .then_with(|| left.meta.segment_id.cmp(&right.meta.segment_id))
    });
}

fn invalid_inventory(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;
    use crate::storage::manifest::{
        ManifestWriter, read_manifest_snapshot as read_snapshot,
        refresh_manifest_snapshot as refresh_snapshot, write_current,
    };

    const TEST_METRIC: &str = "incremental_inventory_metric";

    fn write_one_segment(root: &Path, timestamp_ms: u64, value: f64, seed: u64) {
        let config = SegmentWriterConfig::new(root, Duration::from_secs(10))
            .with_deterministic_segment_ids(seed);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(timestamp_ms, value)],
                |visit| visit(METRIC_NAME_LABEL, TEST_METRIC),
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn values(store: &SegmentStoreReader) -> Vec<(u64, f64)> {
        store
            .query_exact(&[(METRIC_NAME_LABEL, TEST_METRIC)], 0, 30_000)
            .unwrap()
            .into_iter()
            .flat_map(|result| result.samples)
            .collect()
    }

    fn append_tombstone(root: &Path, snapshot: &ManifestSnapshot, segment_id: &str) {
        let ManifestCut::Present { file_name, .. } = &snapshot.cut else {
            panic!("fixture must have a published manifest");
        };
        let mut writer = ManifestWriter::open_append(root.join("manifest"), file_name).unwrap();
        writer
            .append(&ManifestRecord::SegmentDeleted {
                segment_id: segment_id.to_owned(),
            })
            .unwrap();
        writer.sync_all().unwrap();
    }

    fn rotate_manifest(root: &Path, segments: &[ManifestSegment]) {
        let manifest_dir = root.join("manifest");
        let mut writer = ManifestWriter::create(&manifest_dir, 2).unwrap();
        for segment in segments {
            writer
                .append(&ManifestRecord::SegmentSealed(segment.clone()))
                .unwrap();
        }
        writer.sync_all().unwrap();
        write_current(&manifest_dir, writer.file_name()).unwrap();
    }

    #[test]
    fn empty_inventory_refreshes_to_first_manifest_segment() {
        let root = tempfile::tempdir().unwrap();
        let empty_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let empty =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &empty_snapshot).unwrap();
        assert!(empty.segments.is_empty());

        write_one_segment(root.path(), 1_000, 1.0, 1);
        let first_snapshot =
            refresh_snapshot(root.path().join("manifest"), &empty_snapshot).unwrap();
        let first = empty
            .refresh_manifest_snapshot(root.path(), &first_snapshot)
            .unwrap();

        assert_eq!(first.segments.len(), 1);
        assert_eq!(values(&first), vec![(1_000, 1.0)]);
        assert!(Arc::ptr_eq(
            &empty.metadata_runtime.governor(),
            &first.metadata_runtime.governor()
        ));
    }

    #[test]
    fn suffix_refresh_opens_only_new_segment_and_shares_reader_cache() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        let first_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let first =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &first_snapshot).unwrap();
        assert_eq!(values(&first), vec![(1_000, 1.0)]);

        write_one_segment(root.path(), 11_000, 2.0, 2);
        write_one_segment(root.path(), 21_000, 3.0, 3);
        let second_snapshot =
            refresh_snapshot(root.path().join("manifest"), &first_snapshot).unwrap();
        let second = first
            .refresh_manifest_snapshot(root.path(), &second_snapshot)
            .unwrap();

        let first_id = &first.segments[0].meta.segment_id;
        let shared = second
            .segments
            .iter()
            .find(|segment| segment.meta.segment_id == *first_id)
            .unwrap();
        assert!(Arc::ptr_eq(&first.segments[0], shared));
        assert!(Arc::ptr_eq(
            &first.segments[0].query_cache,
            &shared.query_cache
        ));
        assert!(Arc::ptr_eq(
            &first.metadata_runtime.governor(),
            &second.metadata_runtime.governor()
        ));
        assert_eq!(first.segments.len(), 1);
        assert_eq!(second.segments.len(), 3);
        assert_eq!(values(&first), vec![(1_000, 1.0)]);
        assert_eq!(
            values(&second),
            vec![(1_000, 1.0), (11_000, 2.0), (21_000, 3.0)]
        );
    }

    #[test]
    fn manifest_rotation_with_complete_record_prefix_preserves_shared_readers() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        write_one_segment(root.path(), 11_000, 2.0, 2);
        let before_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let before =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &before_snapshot).unwrap();

        rotate_manifest(root.path(), &before_snapshot.inventory.segments);
        let after_snapshot =
            refresh_snapshot(root.path().join("manifest"), &before_snapshot).unwrap();
        let after = before
            .refresh_manifest_snapshot(root.path(), &after_snapshot)
            .unwrap();

        assert_eq!(values(&after), vec![(1_000, 1.0), (11_000, 2.0)]);
        assert!(
            before
                .segments
                .iter()
                .zip(&after.segments)
                .all(|(left, right)| Arc::ptr_eq(left, right))
        );
    }

    #[test]
    fn rotation_that_omits_a_previously_live_segment_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        write_one_segment(root.path(), 11_000, 2.0, 2);
        let before_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let before =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &before_snapshot).unwrap();

        rotate_manifest(root.path(), &before_snapshot.inventory.segments[1..]);
        let after_snapshot =
            refresh_snapshot(root.path().join("manifest"), &before_snapshot).unwrap();
        let error = before
            .refresh_manifest_snapshot(root.path(), &after_snapshot)
            .err()
            .expect("a rotation that loses live inventory must fail");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("previous record prefix"));
        assert_eq!(values(&before), vec![(1_000, 1.0), (11_000, 2.0)]);
    }

    #[test]
    fn compacted_rotation_after_tombstone_is_rejected_without_slicing_panic() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        write_one_segment(root.path(), 11_000, 2.0, 2);
        let initial = read_snapshot(root.path().join("manifest")).unwrap();
        let removed_id = initial.inventory.segments[0].segment_id.clone();
        append_tombstone(root.path(), &initial, &removed_id);
        let before_snapshot = refresh_snapshot(root.path().join("manifest"), &initial).unwrap();
        let before =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &before_snapshot).unwrap();
        assert_eq!(values(&before), vec![(11_000, 2.0)]);

        // A v1 compacted generation has no authenticated predecessor link.
        // Even though it reproduces the live set, it cannot prove the prior
        // seal+tombstone record history and must fail before suffix slicing.
        rotate_manifest(root.path(), &before_snapshot.inventory.segments);
        let after_snapshot =
            refresh_snapshot(root.path().join("manifest"), &before_snapshot).unwrap();
        let error = before
            .refresh_manifest_snapshot(root.path(), &after_snapshot)
            .err()
            .expect("a compacted v1 rotation must fail closed");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("previous record prefix"));
        assert_eq!(values(&before), vec![(11_000, 2.0)]);
    }

    #[test]
    fn tombstone_removes_only_new_inventory_and_old_view_stays_queryable() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        write_one_segment(root.path(), 11_000, 2.0, 2);
        let before_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let before =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &before_snapshot).unwrap();
        let removed_id = before_snapshot.inventory.segments[0].segment_id.clone();
        let removed_reader = before
            .segments
            .iter()
            .find(|segment| segment.meta.segment_id == removed_id)
            .map(Arc::clone)
            .unwrap();

        append_tombstone(root.path(), &before_snapshot, &removed_id);
        let after_snapshot =
            refresh_snapshot(root.path().join("manifest"), &before_snapshot).unwrap();
        let after = before
            .refresh_manifest_snapshot(root.path(), &after_snapshot)
            .unwrap();

        assert_eq!(after.segments.len(), 1);
        assert_eq!(values(&after), vec![(11_000, 2.0)]);
        assert_eq!(values(&before), vec![(1_000, 1.0), (11_000, 2.0)]);
        assert!(removed_reader.dir.exists());
        drop(after);
        assert_eq!(values(&before), vec![(1_000, 1.0), (11_000, 2.0)]);
    }

    #[test]
    fn malformed_suffix_propagates_without_damaging_old_inventory() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        let snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let store = SegmentStoreReader::open_manifest_snapshot(root.path(), &snapshot).unwrap();
        let ManifestCut::Present { file_name, .. } = &snapshot.cut else {
            panic!("fixture must have a published manifest");
        };
        OpenOptions::new()
            .append(true)
            .open(root.path().join("manifest").join(file_name))
            .unwrap()
            .write_all(&[0x43])
            .unwrap();

        let error = refresh_snapshot(root.path().join("manifest"), &snapshot).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(values(&store), vec![(1_000, 1.0)]);
    }

    #[test]
    fn suffix_segment_validation_failure_rolls_back_new_registrations() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        let first_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let first =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &first_snapshot).unwrap();
        let before_runtime = first.metadata_runtime_snapshot();

        write_one_segment(root.path(), 11_000, 2.0, 2);
        write_one_segment(root.path(), 21_000, 3.0, 3);
        let suffix = refresh_snapshot(root.path().join("manifest"), &first_snapshot).unwrap();
        let corrupt_id = suffix.inventory.segments.last().unwrap().segment_id.clone();
        OpenOptions::new()
            .append(true)
            .open(
                root.path()
                    .join(corrupt_id)
                    .join(SegmentFile::Chunks.filename()),
            )
            .unwrap()
            .write_all(&[0])
            .unwrap();

        let error = first
            .refresh_manifest_snapshot(root.path(), &suffix)
            .err()
            .expect("footer-tracked suffix corruption must propagate");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("length changed"));
        assert_eq!(values(&first), vec![(1_000, 1.0)]);
        assert_eq!(
            first.metadata_runtime_snapshot().cache.registered_artifacts,
            before_runtime.cache.registered_artifacts
        );
    }

    #[test]
    fn same_suffix_tombstone_cannot_hide_a_corrupt_new_segment() {
        let root = tempfile::tempdir().unwrap();
        let empty_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let empty =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &empty_snapshot).unwrap();

        write_one_segment(root.path(), 1_000, 1.0, 1);
        let sealed = read_snapshot(root.path().join("manifest")).unwrap();
        let sealed_id = sealed.inventory.segments[0].segment_id.clone();
        append_tombstone(root.path(), &sealed, &sealed_id);
        let tombstoned = refresh_snapshot(root.path().join("manifest"), &empty_snapshot).unwrap();
        assert!(tombstoned.inventory.segments.is_empty());
        OpenOptions::new()
            .append(true)
            .open(
                root.path()
                    .join(sealed_id)
                    .join(SegmentFile::Chunks.filename()),
            )
            .unwrap()
            .write_all(&[0])
            .unwrap();

        let error = empty
            .refresh_manifest_snapshot(root.path(), &tombstoned)
            .err()
            .expect("a same-suffix tombstone must not suppress segment validation");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("length changed"));
        assert!(empty.segments.is_empty());
    }

    #[test]
    fn snapshot_open_rejects_duplicate_live_seal_records() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        let mut snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        snapshot.records.push(snapshot.records[0].clone());
        snapshot.inventory = ManifestInventory::from_records(snapshot.records.clone()).unwrap();

        let error = SegmentStoreReader::open_manifest_snapshot(root.path(), &snapshot)
            .err()
            .expect("duplicate live seal must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("duplicate live"));
    }

    #[test]
    fn changed_record_prefix_is_rejected_without_reopening_segments() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        let first_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let first =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &first_snapshot).unwrap();

        write_one_segment(root.path(), 11_000, 2.0, 2);
        let mut changed = refresh_snapshot(root.path().join("manifest"), &first_snapshot).unwrap();
        changed.records.swap(0, 1);
        changed.inventory = ManifestInventory::from_records(changed.records.clone()).unwrap();

        let error = first
            .refresh_manifest_snapshot(root.path(), &changed)
            .err()
            .expect("changed record prefix must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("does not extend"));
        assert_eq!(values(&first), vec![(1_000, 1.0)]);
    }

    #[test]
    fn refresh_preserves_manifest_last_write_wins_query_order() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        let first_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let first =
            SegmentStoreReader::open_manifest_snapshot(root.path(), &first_snapshot).unwrap();

        // A newer overlapping segment for the same logical series/timestamp
        // must retain manifest append precedence even though the physical
        // inventory is independently sorted by time and segment ID.
        write_one_segment(root.path(), 1_000, 2.0, 2);
        let second_snapshot =
            refresh_snapshot(root.path().join("manifest"), &first_snapshot).unwrap();
        let second = first
            .refresh_manifest_snapshot(root.path(), &second_snapshot)
            .unwrap();

        assert_eq!(values(&first), vec![(1_000, 1.0)]);
        assert_eq!(values(&second), vec![(1_000, 2.0)]);
    }

    #[test]
    fn old_and_new_inventory_queries_are_safe_on_different_threads() {
        let root = tempfile::tempdir().unwrap();
        write_one_segment(root.path(), 1_000, 1.0, 1);
        let old_snapshot = read_snapshot(root.path().join("manifest")).unwrap();
        let old = SegmentStoreReader::open_manifest_snapshot(root.path(), &old_snapshot).unwrap();
        write_one_segment(root.path(), 11_000, 2.0, 2);
        let new_snapshot = refresh_snapshot(root.path().join("manifest"), &old_snapshot).unwrap();
        let new = old
            .refresh_manifest_snapshot(root.path(), &new_snapshot)
            .unwrap();

        let old = Arc::new(old);
        let new = Arc::new(new);
        let barrier = Arc::new(Barrier::new(3));
        let old_task = {
            let store = Arc::clone(&old);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..16 {
                    assert_eq!(values(&store), vec![(1_000, 1.0)]);
                }
            })
        };
        let new_task = {
            let store = Arc::clone(&new);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..16 {
                    assert_eq!(values(&store), vec![(1_000, 1.0), (11_000, 2.0)]);
                }
            })
        };
        barrier.wait();
        old_task.join().unwrap();
        new_task.join().unwrap();
    }
}
