use std::io::ErrorKind;

use chronoxide_core::storage::wal::{
    TransportOffset, WalCheckpoint, WalReader, WalRecordType, WalWriter, decode_checkpoint_record,
    read_checkpoint_meta, write_checkpoint_meta,
};

#[test]
fn wal_writer_appends_and_reader_replays_records_in_order() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000000.log");

    let mut writer = WalWriter::create(&wal_path).unwrap();
    assert_eq!(
        writer
            .append(WalRecordType::OtlpBatch, b"first-batch")
            .unwrap(),
        0
    );
    let second_offset = writer
        .append(WalRecordType::Checkpoint, b"checkpoint")
        .unwrap();
    assert!(second_offset > 0);
    writer.flush().unwrap();
    drop(writer);

    let mut reader = WalReader::open(&wal_path).unwrap();
    let first = reader.read_next().unwrap().unwrap();
    let second = reader.read_next().unwrap().unwrap();

    assert_eq!(first.record_type, WalRecordType::OtlpBatch);
    assert_eq!(first.payload, b"first-batch");
    assert_eq!(second.record_type, WalRecordType::Checkpoint);
    assert_eq!(second.payload, b"checkpoint");
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn wal_writer_open_append_continues_existing_file() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000001.log");

    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append(WalRecordType::OtlpBatch, b"initial-batch")
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut writer = WalWriter::open_append(&wal_path).unwrap();
    let appended_offset = writer
        .append(WalRecordType::SegmentSealed, b"segment-sealed")
        .unwrap();
    writer.flush().unwrap();
    assert!(appended_offset > 0);
    drop(writer);

    let mut reader = WalReader::open(&wal_path).unwrap();
    assert_eq!(
        reader.read_next().unwrap().unwrap().record_type,
        WalRecordType::OtlpBatch
    );
    let appended = reader.read_next().unwrap().unwrap();
    assert_eq!(appended.record_type, WalRecordType::SegmentSealed);
    assert_eq!(appended.payload, b"segment-sealed");
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn wal_writer_appends_checkpoint_record_with_record_lsn() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000002.log");

    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append(WalRecordType::OtlpBatch, b"first-batch")
        .unwrap();
    let checkpoint_lsn = writer.current_offset().unwrap();
    let checkpoint = writer
        .append_checkpoint(
            1_725_000_000_000,
            vec![TransportOffset {
                topic: "metrics".to_string(),
                partition: 0,
                next_offset: 42,
            }],
        )
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut reader = WalReader::open(&wal_path).unwrap();
    reader.read_next().unwrap().unwrap();
    let checkpoint_record = reader.read_next().unwrap().unwrap();
    let decoded = decode_checkpoint_record(&checkpoint_record).unwrap();

    assert_eq!(checkpoint.wal_lsn, decoded.wal_lsn);
    assert_eq!(checkpoint.wal_lsn, checkpoint_lsn);
    assert_eq!(decoded.wall_time_ms, 1_725_000_000_000);
    assert_eq!(decoded.offsets[0].next_offset, 42);
}

#[test]
fn checkpoint_meta_read_returns_none_when_missing() {
    let tempdir = tempfile::tempdir().unwrap();

    assert!(read_checkpoint_meta(tempdir.path()).unwrap().is_none());
}

#[test]
fn checkpoint_meta_atomic_write_and_read_latest_checkpoint() {
    let tempdir = tempfile::tempdir().unwrap();
    let first = WalCheckpoint::try_new(
        128,
        1_725_000_000_000,
        vec![TransportOffset {
            topic: "metrics".to_string(),
            partition: 0,
            next_offset: 42,
        }],
    )
    .unwrap();
    let second = WalCheckpoint::try_new(
        256,
        1_725_000_010_000,
        vec![TransportOffset {
            topic: "metrics".to_string(),
            partition: 0,
            next_offset: 84,
        }],
    )
    .unwrap();

    write_checkpoint_meta(tempdir.path(), &first).unwrap();
    write_checkpoint_meta(tempdir.path(), &second).unwrap();
    let loaded = read_checkpoint_meta(tempdir.path()).unwrap().unwrap();

    assert_eq!(loaded, second);
    assert!(!tempdir.path().join("checkpoint.meta.tmp").exists());
}

#[test]
fn checkpoint_meta_rejects_corrupt_snapshot() {
    let tempdir = tempfile::tempdir().unwrap();
    let checkpoint = WalCheckpoint::try_new(
        128,
        1_725_000_000_000,
        vec![TransportOffset {
            topic: "metrics".to_string(),
            partition: 0,
            next_offset: 42,
        }],
    )
    .unwrap();
    write_checkpoint_meta(tempdir.path(), &checkpoint).unwrap();

    let meta_path = tempdir.path().join("checkpoint.meta");
    let mut bytes = std::fs::read(&meta_path).unwrap();
    bytes[20] ^= 0xff;
    std::fs::write(&meta_path, bytes).unwrap();

    let err = read_checkpoint_meta(tempdir.path()).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidData);
}
