use std::fs;

use chronoxide_core::storage::manifest::{
    ManifestInventory, ManifestRecord, ManifestSegment, ManifestWriter, read_manifest_inventory,
    write_current,
};
use chronoxide_core::storage::segment::SegmentId;
use chronoxide_core::storage::wal::{truncate_wal_prefix_from_manifest, wal_file_name, wal_lsn};

fn write_wal_file(dir: &std::path::Path, sequence: u32) {
    fs::write(dir.join(wal_file_name(sequence)), format!("wal-{sequence}")).unwrap();
}

fn manifest_inventory_with_boundaries(
    manifest_dir: &std::path::Path,
    boundaries: &[Option<u64>],
) -> ManifestInventory {
    let mut writer = ManifestWriter::create(manifest_dir, 1).unwrap();
    for (idx, boundary) in boundaries.iter().copied().enumerate() {
        let start_ms = 1_000 + (idx as u64 * 1_000);
        let end_ms = start_ms + 1_000;
        let id = SegmentId::new(start_ms, end_ms).unwrap();
        writer
            .append(&ManifestRecord::SegmentSealed(
                ManifestSegment::new(id.dir_name(), start_ms, end_ms, boundary).unwrap(),
            ))
            .unwrap();
    }
    writer.sync_all().unwrap();
    write_current(manifest_dir, writer.file_name()).unwrap();
    read_manifest_inventory(manifest_dir).unwrap().unwrap()
}

#[test]
fn wal_truncation_deletes_only_closed_files_before_manifest_safe_boundary() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_dir = tempdir.path().join("wal");
    let manifest_dir = tempdir.path().join("manifest");
    fs::create_dir_all(&wal_dir).unwrap();
    for sequence in 0..=3 {
        write_wal_file(&wal_dir, sequence);
    }
    fs::write(wal_dir.join("checkpoint.meta"), b"checkpoint").unwrap();
    fs::write(wal_dir.join("notes.txt"), b"not a wal file").unwrap();
    let inventory = manifest_inventory_with_boundaries(
        &manifest_dir,
        &[Some(wal_lsn(2, 0).unwrap()), Some(wal_lsn(3, 500).unwrap())],
    );

    let report = truncate_wal_prefix_from_manifest(&wal_dir, &inventory, 3).unwrap();

    assert_eq!(
        report.deleted_files,
        vec![wal_file_name(0), wal_file_name(1)]
    );
    assert!(!wal_dir.join(wal_file_name(0)).exists());
    assert!(!wal_dir.join(wal_file_name(1)).exists());
    assert!(wal_dir.join(wal_file_name(2)).exists());
    assert!(wal_dir.join(wal_file_name(3)).exists());
    assert!(wal_dir.join("checkpoint.meta").exists());
    assert!(wal_dir.join("notes.txt").exists());
}

#[test]
fn wal_truncation_preserves_active_file_even_when_boundary_is_newer() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_dir = tempdir.path().join("wal");
    let manifest_dir = tempdir.path().join("manifest");
    fs::create_dir_all(&wal_dir).unwrap();
    for sequence in 0..=2 {
        write_wal_file(&wal_dir, sequence);
    }
    let inventory =
        manifest_inventory_with_boundaries(&manifest_dir, &[Some(wal_lsn(3, 0).unwrap())]);

    let report = truncate_wal_prefix_from_manifest(&wal_dir, &inventory, 1).unwrap();

    assert_eq!(report.deleted_files, vec![wal_file_name(0)]);
    assert!(wal_dir.join(wal_file_name(1)).exists());
    assert!(wal_dir.join(wal_file_name(2)).exists());
}

#[test]
fn wal_truncation_does_nothing_when_manifest_boundary_is_missing() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_dir = tempdir.path().join("wal");
    let manifest_dir = tempdir.path().join("manifest");
    fs::create_dir_all(&wal_dir).unwrap();
    for sequence in 0..=2 {
        write_wal_file(&wal_dir, sequence);
    }
    let inventory =
        manifest_inventory_with_boundaries(&manifest_dir, &[Some(wal_lsn(2, 0).unwrap()), None]);

    let report = truncate_wal_prefix_from_manifest(&wal_dir, &inventory, 2).unwrap();

    assert!(report.deleted_files.is_empty());
    for sequence in 0..=2 {
        assert!(wal_dir.join(wal_file_name(sequence)).exists());
    }
}
