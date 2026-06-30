use chronoxide_core::storage::manifest::{
    ManifestInventory, ManifestRecord, ManifestSegment, ManifestWriter, manifest_file_name,
    read_current, read_manifest_inventory, write_current,
};
use chronoxide_core::storage::segment::SegmentId;

#[test]
fn manifest_writer_appends_records_and_reader_builds_inventory() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");
    let first_id = SegmentId::new(1_000, 2_000).unwrap();
    let second_id = SegmentId::new(2_000, 3_000).unwrap();

    let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();
    writer
        .append(&ManifestRecord::SegmentSealed(
            ManifestSegment::new(first_id.dir_name(), 1_000, 2_000, Some(100)).unwrap(),
        ))
        .unwrap();
    writer
        .append(&ManifestRecord::SegmentSealed(
            ManifestSegment::new(second_id.dir_name(), 2_000, 3_000, Some(200)).unwrap(),
        ))
        .unwrap();
    writer
        .append(&ManifestRecord::SegmentDeleted {
            segment_id: first_id.dir_name(),
        })
        .unwrap();
    writer.sync_all().unwrap();
    write_current(&manifest_dir, writer.file_name()).unwrap();

    let inventory = read_manifest_inventory(&manifest_dir).unwrap().unwrap();

    assert_eq!(
        inventory,
        ManifestInventory {
            segments: vec![
                ManifestSegment::new(second_id.dir_name(), 2_000, 3_000, Some(200)).unwrap()
            ]
        }
    );
}

#[test]
fn current_is_atomically_replaced_with_latest_manifest_pointer() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");

    write_current(&manifest_dir, &manifest_file_name(1)).unwrap();
    write_current(&manifest_dir, &manifest_file_name(2)).unwrap();

    assert_eq!(
        read_current(&manifest_dir).unwrap().as_deref(),
        Some("MANIFEST-000002")
    );
    assert!(!manifest_dir.join("CURRENT.tmp").exists());
}

#[test]
fn manifest_inventory_requires_current_pointer() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");
    std::fs::create_dir_all(&manifest_dir).unwrap();

    assert!(read_manifest_inventory(&manifest_dir).unwrap().is_none());
}

#[test]
fn manifest_inventory_ignores_orphan_segment_directories() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");
    let segments_dir = tempdir.path().join("segments");
    let published_id = SegmentId::new(1_000, 2_000).unwrap();
    let orphan_id = SegmentId::new(2_000, 3_000).unwrap();
    std::fs::create_dir_all(segments_dir.join(orphan_id.dir_name())).unwrap();

    let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();
    writer
        .append(&ManifestRecord::SegmentSealed(
            ManifestSegment::new(published_id.dir_name(), 1_000, 2_000, Some(100)).unwrap(),
        ))
        .unwrap();
    writer.sync_all().unwrap();
    write_current(&manifest_dir, writer.file_name()).unwrap();

    let inventory = read_manifest_inventory(&manifest_dir).unwrap().unwrap();

    assert_eq!(inventory.segments.len(), 1);
    assert_eq!(inventory.segments[0].segment_id, published_id.dir_name());
}
