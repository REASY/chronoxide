use std::fs;

use chronoxide_core::storage::chunk::{ChunkReader, ChunkSamples};
use chronoxide_core::storage::segment::{
    SegmentFile, SegmentId, SegmentPaths, SegmentReader, SegmentWriter, SegmentWriterConfig,
};
use std::time::Duration;
use ulid::Ulid;

#[test]
fn segment_temp_publish_moves_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let id = SegmentId::with_ulid(100, 200, Ulid::new()).unwrap();
    let paths = SegmentPaths::new(tempdir.path(), id);

    let tmp = paths.create_temp_dir().unwrap();
    let meta_path = tmp.file_path(SegmentFile::MetaJson);
    fs::write(&meta_path, b"{}").unwrap();

    let final_dir = tmp.publish().unwrap();
    assert_eq!(final_dir, paths.dir());
    assert!(paths.dir().exists());
    assert!(!paths.temp_dir().exists());
    assert!(paths.file_path(SegmentFile::MetaJson).exists());
}

#[test]
fn segment_writer_roundtrip_meta() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(15));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_sample(chronoxide_core::labels::SeriesRef::new(42), 12_000, 3.14)
        .unwrap();
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
    let chunk_len = fs::metadata(reader.file_path(SegmentFile::Chunks))
        .unwrap()
        .len();
    assert!(chunk_len > 0);
    let index_len = fs::metadata(reader.file_path(SegmentFile::ChunkIndex))
        .unwrap()
        .len();
    assert!(index_len > 0);
    let entries = reader.read_chunk_index().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 1);
    assert_eq!(entries[0][0].min_time_ms, 12_000);
    assert_eq!(entries[0][0].max_time_ms, 12_000);

    let chunk_file = reader.open_chunks().unwrap();
    let mut chunk_reader = ChunkReader::new(chunk_file);
    let record = chunk_reader.read_next().unwrap().unwrap();
    let ChunkSamples::Float(samples) = record.samples else {
        panic!("expected float samples");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0], (12_000, 3.14));
}
