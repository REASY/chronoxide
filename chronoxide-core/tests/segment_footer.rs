use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, write_current,
};
use chronoxide_core::storage::segment::{
    SegmentFile, SegmentReader, SegmentSelector, SegmentStoreReader, SegmentWriter,
    SegmentWriterConfig,
};

fn write_single_segment(segments_dir: &Path) -> SegmentReader {
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        segments_dir,
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), "backend-1".to_string()),
            ],
            &[(5_000, 1.0), (6_000, 1.5)],
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    SegmentReader::open(seg_dir).unwrap()
}

fn publish_manifest_segment(manifest_dir: &Path, reader: &SegmentReader) {
    let meta = reader.meta();
    let mut writer = ManifestWriter::create(manifest_dir, 1).unwrap();
    writer
        .append(&ManifestRecord::SegmentSealed(
            ManifestSegment::new(
                meta.segment_id.clone(),
                meta.start_ms,
                meta.end_ms,
                Some(100),
            )
            .unwrap(),
        ))
        .unwrap();
    writer.sync_all().unwrap();
    write_current(manifest_dir, writer.file_name()).unwrap();
}

fn open_manifest_published(
    segments_dir: &Path,
    manifest_dir: &Path,
) -> io::Result<SegmentStoreReader> {
    SegmentStoreReader::open_manifest_published(segments_dir, manifest_dir)
}

fn flip_first_byte(path: impl AsRef<Path>) {
    let path = path.as_ref();
    let mut bytes = fs::read(path).unwrap();
    bytes[0] ^= 0xff;
    fs::write(path, bytes).unwrap();
}

#[test]
fn manifest_published_segment_accepts_valid_footer() {
    let tempdir = tempfile::tempdir().unwrap();
    let segments_dir = tempdir.path().join("segments");
    let manifest_dir = tempdir.path().join("manifest");
    let reader = write_single_segment(&segments_dir);
    publish_manifest_segment(&manifest_dir, &reader);

    let store = open_manifest_published(&segments_dir, &manifest_dir).unwrap();
    let results = store
        .query_selector(&SegmentSelector::metric("cpu.usage"), 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0), (6_000, 1.5)]);
}

#[test]
fn manifest_published_segment_rejects_corrupted_tracked_file() {
    let tempdir = tempfile::tempdir().unwrap();
    let segments_dir = tempdir.path().join("segments");
    let manifest_dir = tempdir.path().join("manifest");
    let reader = write_single_segment(&segments_dir);
    publish_manifest_segment(&manifest_dir, &reader);
    flip_first_byte(reader.file_path(SegmentFile::Symbols));

    let err = match open_manifest_published(&segments_dir, &manifest_dir) {
        Ok(_) => panic!("expected corrupted segment file to fail footer validation"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn manifest_published_segment_rejects_corrupted_footer() {
    let tempdir = tempfile::tempdir().unwrap();
    let segments_dir = tempdir.path().join("segments");
    let manifest_dir = tempdir.path().join("manifest");
    let reader = write_single_segment(&segments_dir);
    publish_manifest_segment(&manifest_dir, &reader);
    flip_first_byte(reader.file_path(SegmentFile::Footer));

    let err = match open_manifest_published(&segments_dir, &manifest_dir) {
        Ok(_) => panic!("expected corrupted footer to fail footer validation"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}
