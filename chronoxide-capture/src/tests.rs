use super::*;

fn tmp_capture_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn capture_open_error(path: &Path) -> CaptureError {
    match OtlpCaptureReader::open(path) {
        Ok(_) => panic!("capture open unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn raw_partition_prefix(compression: u8) -> Vec<u8> {
    let mut bytes = b"CHRONOXIDE_OTLP_CAPTURE_PARTITION_V2\n".to_vec();
    bytes.push(compression);
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(b"t");
    bytes.extend_from_slice(&(-2_i32).to_le_bytes());
    bytes
}

#[test]
fn uncompressed_v2_capture_bytes_are_frozen() {
    let tempdir = tmp_capture_dir();
    let path = tempdir.path();
    let mut writer = OtlpCaptureWriter::create(path, "t", CompressionMethod::Uncompressed).unwrap();
    writer.append(-2, -3, -4, -5, &[0x00, 0xff, 0x7f]).unwrap();
    writer.close().unwrap();

    let mut expected_partition = raw_partition_prefix(0);
    expected_partition.extend_from_slice(&0_u64.to_le_bytes());
    expected_partition.extend_from_slice(&(-3_i64).to_le_bytes());
    expected_partition.extend_from_slice(&(-4_i64).to_le_bytes());
    expected_partition.extend_from_slice(&(-5_i64).to_le_bytes());
    expected_partition.extend_from_slice(&3_u32.to_le_bytes());
    expected_partition.extend_from_slice(&[0x00, 0xff, 0x7f]);
    assert_eq!(
        fs::read(path.join("partition--2.capture")).unwrap(),
        expected_partition
    );

    const EXPECTED_MANIFEST: &str = concat!(
        "{\n",
        "  \"version\": 2,\n",
        "  \"topic\": \"t\",\n",
        "  \"compression\": \"uncompressed\",\n",
        "  \"partitions\": [\n",
        "    {\n",
        "      \"partition\": -2,\n",
        "      \"file_name\": \"partition--2.capture\",\n",
        "      \"message_count\": 1,\n",
        "      \"total_uncompressed_payload_bytes\": 3,\n",
        "      \"total_compressed_payload_bytes\": 3\n",
        "    }\n",
        "  ]\n",
        "}"
    );
    assert_eq!(
        fs::read(path.join("manifest.json")).unwrap(),
        EXPECTED_MANIFEST.as_bytes()
    );
}

#[test]
fn unknown_compression_remains_an_invalid_data_io_error() {
    let tempdir = tmp_capture_dir();
    let partition = tempdir.path().join("partition.capture");
    fs::write(&partition, raw_partition_prefix(2)).unwrap();

    let error = capture_open_error(&partition);
    assert!(matches!(
        error.kind(),
        CaptureErrorKind::IoError(inner)
            if inner.kind() == std::io::ErrorKind::InvalidData
    ));
    assert_eq!(error.to_string(), "IoError: unknown compression method: 2");
}

#[test]
fn malformed_manifest_remains_a_json_error() {
    let tempdir = tmp_capture_dir();
    fs::write(tempdir.path().join("manifest.json"), b"{").unwrap();

    let error = capture_open_error(tempdir.path());
    assert!(matches!(error.kind(), CaptureErrorKind::SerdeJsonError(_)));
    assert!(
        error
            .to_string()
            .starts_with("SerdeJsonError: EOF while parsing")
    );
}

#[test]
fn truncated_record_body_remains_an_unexpected_eof_io_error() {
    let tempdir = tmp_capture_dir();
    let partition = tempdir.path().join("partition.capture");
    let mut bytes = raw_partition_prefix(0);
    bytes.extend_from_slice(&0_u64.to_le_bytes());
    fs::write(&partition, bytes).unwrap();

    let mut reader = OtlpCaptureReader::open(&partition).unwrap();
    let error = reader.next().unwrap_err();
    assert!(matches!(
        error.kind(),
        CaptureErrorKind::IoError(inner)
            if inner.kind() == std::io::ErrorKind::UnexpectedEof
    ));
}

#[test]
fn partial_trailing_sequence_retains_current_clean_eof_behavior() {
    let tempdir = tmp_capture_dir();
    let partition = tempdir.path().join("partition.capture");
    let mut bytes = raw_partition_prefix(0);
    bytes.extend_from_slice(&[1, 2, 3]);
    fs::write(&partition, bytes).unwrap();

    let mut reader = OtlpCaptureReader::open(&partition).unwrap();
    assert!(reader.next().unwrap().is_none());
}

#[test]
fn capture_roundtrip_zstd_close() {
    let tempdir = tmp_capture_dir();
    let path = tempdir.path();

    let mut writer =
        OtlpCaptureWriter::create(path, "test-topic", CompressionMethod::Zstd).unwrap();
    writer
        .append(0, 1, 123, 10_000, b"hello")
        .expect("append should work");
    writer
        .append(0, 2, 124, 10_001, b"world")
        .expect("append should work");
    writer.close().expect("close should work");

    let mut reader = OtlpCaptureReader::open(path).unwrap();
    let (sequence, m1) = reader.next_with_sequence().unwrap().unwrap();
    assert_eq!(sequence, 0);
    assert_eq!(m1.topic, "test-topic");
    assert_eq!(m1.partition, 0);
    assert_eq!(m1.offset, 1);
    assert_eq!(m1.timestamp_ms, 123);
    assert_eq!(m1.captured_at_ms, 10_000);
    assert_eq!(m1.payload, b"hello");

    let (sequence, m2) = reader.next_with_sequence().unwrap().unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(m2.offset, 2);
    assert_eq!(m2.timestamp_ms, 124);
    assert_eq!(m2.captured_at_ms, 10_001);
    assert_eq!(m2.payload, b"world");

    assert!(reader.next().unwrap().is_none());
}

#[test]
fn capture_close_is_idempotent() {
    let tempdir = tmp_capture_dir();
    let path = tempdir.path();

    let mut writer =
        OtlpCaptureWriter::create(path, "test-topic", CompressionMethod::Uncompressed).unwrap();
    writer.append(0, 1, 123, 1_000, b"hello").unwrap();
    writer.close().unwrap();
    writer.close().unwrap();
}

#[test]
fn capture_manifest_tracks_partition_metadata() {
    let tempdir = tmp_capture_dir();
    let path = tempdir.path();

    let mut writer =
        OtlpCaptureWriter::create(path, "topic", CompressionMethod::Uncompressed).unwrap();
    writer.append(0, 1, 100, 1_000, b"hello").unwrap();
    writer.append(1, 2, 200, 2_000, b"world!!").unwrap();
    writer.append(0, 3, 300, 3_000, b"abc").unwrap();
    writer.close().unwrap();

    let manifest = read_manifest(path).unwrap();
    assert_eq!(manifest.topic, "topic");
    assert_eq!(manifest.compression, CompressionMethod::Uncompressed);
    assert_eq!(manifest.partitions.len(), 2);

    let p0 = manifest
        .partitions
        .iter()
        .find(|p| p.partition == 0)
        .unwrap();
    assert_eq!(p0.message_count, 2);
    assert_eq!(p0.total_uncompressed_payload_bytes, 8);
    assert_eq!(p0.total_compressed_payload_bytes, 8);

    let p1 = manifest
        .partitions
        .iter()
        .find(|p| p.partition == 1)
        .unwrap();
    assert_eq!(p1.message_count, 1);
    assert_eq!(p1.total_uncompressed_payload_bytes, 7);
    assert_eq!(p1.total_compressed_payload_bytes, 7);
}

#[test]
fn capture_open_partition_reads_single_partition() {
    let tempdir = tmp_capture_dir();
    let path = tempdir.path();

    let mut writer =
        OtlpCaptureWriter::create(path, "topic", CompressionMethod::Uncompressed).unwrap();
    writer.append(0, 1, 100, 1_000, b"p0-1").unwrap();
    writer.append(1, 2, 200, 2_000, b"p1-1").unwrap();
    writer.append(1, 3, 300, 3_000, b"p1-2").unwrap();
    writer.append(0, 4, 400, 4_000, b"p0-2").unwrap();
    writer.close().unwrap();

    let mut reader = OtlpCaptureReader::open_partition(path, 1).unwrap();
    let r1 = reader.next().unwrap().unwrap();
    assert_eq!(r1.partition, 1);
    assert_eq!(r1.payload, b"p1-1");
    let r2 = reader.next().unwrap().unwrap();
    assert_eq!(r2.partition, 1);
    assert_eq!(r2.payload, b"p1-2");
    assert!(reader.next().unwrap().is_none());
}

#[test]
fn multi_partition_reader_exposes_persisted_global_sequence() {
    let tempdir = tmp_capture_dir();
    let mut writer =
        OtlpCaptureWriter::create(tempdir.path(), "topic", CompressionMethod::Uncompressed)
            .unwrap();
    for ordinal in 0..12_u64 {
        writer
            .append(
                (ordinal % 3) as i32,
                ordinal as i64,
                ordinal as i64,
                ordinal as i64,
                &[ordinal as u8],
            )
            .unwrap();
    }
    writer.close().unwrap();

    let mut reader = OtlpCaptureReader::open(tempdir.path()).unwrap();
    for expected in 0..12_u64 {
        let (sequence, message) = reader.next_with_sequence().unwrap().unwrap();
        assert_eq!(sequence, expected);
        assert_eq!(message.payload, [expected as u8]);
    }
    assert!(reader.next_with_sequence().unwrap().is_none());
}
