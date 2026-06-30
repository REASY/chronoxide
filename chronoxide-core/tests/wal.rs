use chronoxide_core::storage::wal::{WalReader, WalRecordType, WalWriter};

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
