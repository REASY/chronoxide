use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, write_current,
};
use chronoxide_core::storage::segment::{
    SegmentFile, SegmentMeta, SegmentSelector, SegmentStoreOpenOptions, SegmentStoreReader,
    SegmentWriter, SegmentWriterConfig,
};

#[derive(Debug)]
struct SegmentFixture {
    dir: PathBuf,
    meta: SegmentMeta,
}

impl SegmentFixture {
    fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    fn file_path(&self, file: SegmentFile) -> PathBuf {
        self.dir.join(file.filename())
    }
}

fn write_single_segment(segments_dir: &Path) -> SegmentFixture {
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
    let meta = serde_json::from_reader(
        fs::File::open(seg_dir.join(SegmentFile::MetaJson.filename())).unwrap(),
    )
    .unwrap();
    SegmentFixture { dir: seg_dir, meta }
}

fn publish_manifest_segment(manifest_dir: &Path, fixture: &SegmentFixture) {
    let meta = fixture.meta();
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
    SegmentStoreReader::open_manifest_published_with_options(
        segments_dir,
        manifest_dir,
        SegmentStoreOpenOptions::default(),
    )
}

fn open_manifest_published_validated(
    segments_dir: &Path,
    manifest_dir: &Path,
) -> io::Result<SegmentStoreReader> {
    SegmentStoreReader::open_manifest_published_with_options(
        segments_dir,
        manifest_dir,
        SegmentStoreOpenOptions {
            validate_segment_footers: true,
            ..SegmentStoreOpenOptions::default()
        },
    )
}

fn flip_first_byte(path: impl AsRef<Path>) {
    let path = path.as_ref();
    let mut bytes = fs::read(path).unwrap();
    bytes[0] ^= 0xff;
    fs::write(path, bytes).unwrap();
}

fn rewrite_footer_schema_version(path: impl AsRef<Path>, schema_version: u16) {
    const HEADER_LEN: usize = 16;
    const TRAILER_LEN: usize = 4;

    let path = path.as_ref();
    let mut bytes = fs::read(path).unwrap();
    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let payload_end = HEADER_LEN + payload_len;
    assert_eq!(bytes.len(), payload_end + TRAILER_LEN);

    bytes[6..8].copy_from_slice(&schema_version.to_le_bytes());
    let header: [u8; HEADER_LEN] = bytes[..HEADER_LEN].try_into().unwrap();
    let checksum = crc32c::crc32c_append(crc32c::crc32c(&header), &bytes[HEADER_LEN..payload_end]);
    bytes[payload_end..].copy_from_slice(&checksum.to_le_bytes());
    fs::write(path, bytes).unwrap();
}

#[test]
fn newly_written_segment_footer_uses_schema_version_8() {
    let tempdir = tempfile::tempdir().unwrap();
    let reader = write_single_segment(&tempdir.path().join("segments"));

    let bytes = fs::read(reader.file_path(SegmentFile::Footer)).unwrap();

    assert_eq!(&bytes[..4], b"CSFT");
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 8);
}

#[test]
fn manifest_validation_rejects_legacy_segment_schema_version_5() {
    let tempdir = tempfile::tempdir().unwrap();
    let segments_dir = tempdir.path().join("segments");
    let manifest_dir = tempdir.path().join("manifest");
    let reader = write_single_segment(&segments_dir);
    publish_manifest_segment(&manifest_dir, &reader);
    rewrite_footer_schema_version(reader.file_path(SegmentFile::Footer), 5);

    let error = match open_manifest_published_validated(&segments_dir, &manifest_dir) {
        Ok(_) => panic!("legacy segment schema version 5 was unexpectedly accepted"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("unsupported segment footer schema version")
    );
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

    let err = match open_manifest_published_validated(&segments_dir, &manifest_dir) {
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

    let err = match open_manifest_published_validated(&segments_dir, &manifest_dir) {
        Ok(_) => panic!("expected corrupted footer to fail footer validation"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}
