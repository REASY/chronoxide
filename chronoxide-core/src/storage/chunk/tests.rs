use super::*;
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality, SummaryQuantileValue, SummaryValue,
    TypedSampleMetadata,
};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

#[derive(Default)]
struct CountingWriter {
    bytes: Vec<u8>,
    write_calls: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_calls += 1;
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_scalar_lane_test_chunk() -> (tempfile::NamedTempFile, ChunkIndexEntry) {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let entry = writer
        .append_histogram_chunk_ordered(
            4,
            &[(
                10_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0, 10.0],
                    bucket_counts: vec![1, 2, 1, 0],
                },
            )],
        )
        .unwrap();
    writer.flush().unwrap();
    (temp, entry)
}

fn read_scalar_lane_test_batch(
    temp: &tempfile::NamedTempFile,
    entry: &ChunkIndexEntry,
) -> ChunkPayloadBatch {
    let mut file = temp.reopen().unwrap();
    read_chunk_payload_batch(
        &mut file,
        &[ChunkPayloadRead {
            file_id: entry.file_id,
            offset: entry.offset,
            len: u64::from(entry.scalar_projection_read_len()),
        }],
        0,
    )
    .unwrap()
}

fn write_three_float_chunks() -> (tempfile::NamedTempFile, [ChunkIndexEntry; 3]) {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let first = writer
        .append_float_chunk_ordered(7, &[(10_000, 1.0)])
        .unwrap();
    let middle = writer
        .append_float_chunk_ordered(8, &[(10_000, 2.0)])
        .unwrap();
    let last = writer
        .append_float_chunk_ordered(9, &[(10_000, 3.0)])
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    assert!(middle.offset >= first.offset + u64::from(first.length));
    assert!(last.offset >= middle.offset + u64::from(middle.length));
    (temp, [first, middle, last])
}

fn corrupt_chunk_trailer(temp: &tempfile::NamedTempFile, entry: &ChunkIndexEntry) {
    let offset = entry
        .offset
        .checked_add(u64::from(entry.length))
        .unwrap()
        .checked_sub(1)
        .unwrap();
    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.flush().unwrap();
}

fn pread_chunk_reader(payload_coalesce_max_gap_bytes: u64) -> crate::storage::io::ChunkReader {
    crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
        mode: crate::storage::io::ChunkReadMode::Pread,
        queue_depth: 1,
        payload_coalesce_max_gap_bytes,
    })
    .unwrap()
}

#[test]
fn chunk_writer_roundtrip_single_sample() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

    let entry = writer.append_float_sample(7, 10_000, 42.5).unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.min_time_ms, 10_000);
    assert_eq!(entry.max_time_ms, 10_000);
    assert!(entry.length > 0);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 7);
    assert_eq!(record.samples, ChunkSamples::Float(vec![(10_000, 42.5)]));
}

#[test]
fn chunk_payload_batch_coalesces_reads_and_decodes_exact_records() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let first = writer
        .append_float_chunk_ordered(7, &[(10_000, 42.5), (11_000, 43.5)])
        .unwrap();
    let second = writer
        .append_float_chunk_ordered(8, &[(12_000, 44.5), (13_000, 45.5)])
        .unwrap();
    writer.flush().unwrap();

    let mut file = temp.reopen().unwrap();
    let batch = read_chunk_payload_batch(
        &mut file,
        &[
            ChunkPayloadRead {
                file_id: first.file_id,
                offset: first.offset,
                len: u64::from(first.length),
            },
            ChunkPayloadRead {
                file_id: second.file_id,
                offset: second.offset,
                len: u64::from(second.length),
            },
        ],
        4096,
    )
    .unwrap();

    assert_eq!(batch.physical_read_count(), 1);
    assert_eq!(
        batch.physical_bytes_read(),
        second.offset + u64::from(second.length) - first.offset
    );

    let first_record = batch
        .decode_chunk_record(first.offset, first.length)
        .unwrap();
    let second_record = batch
        .decode_chunk_record(second.offset, second.length)
        .unwrap();

    let positional_reader =
        crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
            mode: crate::storage::io::ChunkReadMode::Pread,
            queue_depth: 8,
            payload_coalesce_max_gap_bytes:
                crate::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        })
        .unwrap();
    let positional_batch = read_chunk_payload_batch_with_reader(
        std::sync::Arc::new(temp.reopen().unwrap()),
        &[
            ChunkPayloadRead {
                file_id: first.file_id,
                offset: first.offset,
                len: u64::from(first.length),
            },
            ChunkPayloadRead {
                file_id: second.file_id,
                offset: second.offset,
                len: u64::from(second.length),
            },
        ],
        &positional_reader,
    )
    .unwrap();
    assert_eq!(positional_batch.physical_read_count(), 1);
    assert_eq!(
        positional_batch.physical_bytes_read(),
        batch.physical_bytes_read()
    );
    assert_eq!(
        positional_batch
            .decode_chunk_record(first.offset, first.length)
            .unwrap(),
        first_record
    );
    assert_eq!(
        positional_batch
            .decode_chunk_record(second.offset, second.length)
            .unwrap(),
        second_record
    );
    assert_eq!(
        first_record.samples,
        ChunkSamples::Float(vec![(10_000, 42.5), (11_000, 43.5)])
    );
    assert_eq!(
        second_record.samples,
        ChunkSamples::Float(vec![(12_000, 44.5), (13_000, 45.5)])
    );
}

#[test]
fn chunk_payload_batch_helper_rejects_gap_above_reader_cap() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut file = temp.reopen().unwrap();
    let error = match read_chunk_payload_batch(
        &mut file,
        &[],
        crate::storage::io::MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES + 1,
    ) {
        Ok(_) => panic!("out-of-range helper gap unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "payload_coalesce_max_gap_bytes must be <= 4096"
    );
}

#[test]
fn chunk_payload_batch_planner_rejects_gap_above_reader_cap() {
    let error = match plan_chunk_payload_batch(
        &[],
        crate::storage::io::MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES + 1,
    ) {
        Ok(_) => panic!("out-of-range planner gap unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "payload_coalesce_max_gap_bytes must be <= 4096"
    );
}

#[test]
fn selected_chunk_corruption_propagates_identically_across_payload_coalesce_gaps() {
    let (temp, entries) = write_three_float_chunks();
    corrupt_chunk_trailer(&temp, &entries[2]);
    let requests = [
        ChunkPayloadRead {
            file_id: 0,
            offset: entries[0].offset,
            len: u64::from(entries[0].length),
        },
        ChunkPayloadRead {
            file_id: 0,
            offset: entries[2].offset,
            len: u64::from(entries[2].length),
        },
    ];

    let mut errors = Vec::new();
    for (gap, expected_reads) in [(0, 2), (4096, 1)] {
        let reader = pread_chunk_reader(gap);
        let batch = read_chunk_payload_batch_with_reader(
            std::sync::Arc::new(temp.reopen().unwrap()),
            &requests,
            &reader,
        )
        .unwrap();
        assert_eq!(batch.physical_read_count(), expected_reads, "gap {gap}");
        let first = batch
            .decode_chunk_record(entries[0].offset, entries[0].length)
            .unwrap();
        assert_eq!(first.series_ref, 7);
        assert_eq!(first.samples, ChunkSamples::Float(vec![(10_000, 1.0)]));
        let error = batch
            .decode_chunk_record(entries[2].offset, entries[2].length)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("crc"), "{error}");
        errors.push(error.to_string());
    }
    assert_eq!(errors[0], errors[1]);
}

#[test]
fn unselected_corruption_in_coalesced_gap_is_not_decoded() {
    let (temp, entries) = write_three_float_chunks();
    assert!(
        entries[2].offset - (entries[0].offset + u64::from(entries[0].length))
            <= crate::storage::io::MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES
    );
    corrupt_chunk_trailer(&temp, &entries[1]);

    let middle_request = [ChunkPayloadRead {
        file_id: 0,
        offset: entries[1].offset,
        len: u64::from(entries[1].length),
    }];
    let reader = pread_chunk_reader(0);
    let middle_batch = read_chunk_payload_batch_with_reader(
        std::sync::Arc::new(temp.reopen().unwrap()),
        &middle_request,
        &reader,
    )
    .unwrap();
    let middle_error = middle_batch
        .decode_chunk_record(entries[1].offset, entries[1].length)
        .unwrap_err();
    assert_eq!(middle_error.kind(), io::ErrorKind::InvalidData);
    assert!(middle_error.to_string().contains("crc"), "{middle_error}");

    let selected_requests = [
        ChunkPayloadRead {
            file_id: 0,
            offset: entries[0].offset,
            len: u64::from(entries[0].length),
        },
        ChunkPayloadRead {
            file_id: 0,
            offset: entries[2].offset,
            len: u64::from(entries[2].length),
        },
    ];
    let mut decoded = Vec::new();
    for (gap, expected_reads) in [(0, 2), (4096, 1)] {
        let reader = pread_chunk_reader(gap);
        let batch = read_chunk_payload_batch_with_reader(
            std::sync::Arc::new(temp.reopen().unwrap()),
            &selected_requests,
            &reader,
        )
        .unwrap();
        assert_eq!(batch.physical_read_count(), expected_reads, "gap {gap}");
        if gap == 4096 {
            assert_eq!(
                batch.physical_bytes_read(),
                entries[2].offset + u64::from(entries[2].length) - entries[0].offset
            );
        } else {
            assert_eq!(
                batch.physical_bytes_read(),
                u64::from(entries[0].length) + u64::from(entries[2].length)
            );
        }
        decoded.push([
            batch
                .decode_chunk_record(entries[0].offset, entries[0].length)
                .unwrap(),
            batch
                .decode_chunk_record(entries[2].offset, entries[2].length)
                .unwrap(),
        ]);
    }

    assert_eq!(decoded[0], decoded[1]);
    assert_eq!(decoded[1][0].series_ref, 7);
    assert_eq!(decoded[1][1].series_ref, 9);
    assert_eq!(
        decoded[1][0].samples,
        ChunkSamples::Float(vec![(10_000, 1.0)])
    );
    assert_eq!(
        decoded[1][1].samples,
        ChunkSamples::Float(vec![(10_000, 3.0)])
    );
}

#[test]
fn chunk_payload_batch_plan_rejects_missing_and_short_results() {
    let plan = plan_chunk_payload_batch(
        &[ChunkPayloadRead {
            file_id: 0,
            offset: 10,
            len: 4,
        }],
        0,
    )
    .unwrap();

    let missing = plan.clone().finish(Vec::new()).unwrap_err();
    assert_eq!(missing.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        missing.to_string(),
        "chunk payload result count does not match planned spans"
    );

    let short = plan
        .finish(vec![crate::storage::io::ReadResult { bytes: vec![0; 3] }])
        .unwrap_err();
    assert_eq!(short.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(short.to_string(), "failed to fill whole buffer");
}

#[test]
fn indexed_locator_authentication_restores_schema7_flags_from_the_exact_prefix() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let entry = writer
        .append_histogram_chunk_ordered(
            4,
            &[(
                10_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(9_000),
                        flags: 0,
                        temporality: OtlpAggregationTemporality::Delta,
                        reset_hint: CounterResetHint::NotCounterReset,
                    },
                    explicit_bounds: vec![1.0, 5.0, 10.0],
                    bucket_counts: vec![1, 2, 1, 0],
                },
            )],
        )
        .unwrap();
    writer.flush().unwrap();
    assert_ne!(entry.flags, 0);
    let batch = read_scalar_lane_test_batch(&temp, &entry);
    let file = std::fs::read(temp.path()).unwrap();
    let prefix_start = usize::try_from(entry.offset).unwrap();
    let prefix_end = prefix_start + CHUNK_HEADER_LEN + TYPED_SCALAR_LANE_HEADER_LEN;
    let prefix_crc = crc32c(&file[prefix_start..prefix_end]);

    let mut metadata_entry = entry.clone();
    metadata_entry.flags = 0;
    let locator =
        IndexedChunkLocator::try_schema7(4, metadata_entry.clone(), Some(prefix_crc)).unwrap();
    let authenticated = batch.authenticate_indexed_locator(&locator).unwrap();
    assert_eq!(authenticated, entry);

    let wrong_series =
        IndexedChunkLocator::try_schema7(5, metadata_entry.clone(), Some(prefix_crc)).unwrap();
    let error = batch
        .authenticate_indexed_locator(&wrong_series)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("series"));

    let wrong_crc =
        IndexedChunkLocator::try_schema7(4, metadata_entry, Some(prefix_crc ^ 1)).unwrap();
    let error = batch.authenticate_indexed_locator(&wrong_crc).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("crc"));
}

#[test]
fn schema6_indexed_locator_keeps_legacy_flags_without_prefix_io() {
    let (_temp, entry) = write_scalar_lane_test_chunk();
    let locator = IndexedChunkLocator::try_schema6_v1(4, entry.clone()).unwrap();

    assert_eq!(
        ChunkPayloadBatch::empty()
            .authenticate_indexed_locator(&locator)
            .unwrap(),
        entry
    );
}

#[test]
fn chunk_payload_batch_plan_coalesces_overlaps_and_threshold_gaps() {
    let plan = plan_chunk_payload_batch(
        &[
            ChunkPayloadRead {
                file_id: 0,
                offset: 10,
                len: 10,
            },
            ChunkPayloadRead {
                file_id: 0,
                offset: 15,
                len: 10,
            },
            ChunkPayloadRead {
                file_id: 0,
                offset: 30,
                len: 5,
            },
            ChunkPayloadRead {
                file_id: 0,
                offset: 41,
                len: 2,
            },
            ChunkPayloadRead {
                file_id: 0,
                offset: 100,
                len: 0,
            },
        ],
        5,
    )
    .unwrap();

    assert_eq!(plan.physical_read_count(), 2);
    assert_eq!(plan.physical_bytes_read(), 27);
}

#[test]
fn chunk_payload_batch_plan_applies_runtime_gap_thresholds_inclusively() {
    let requests = [
        ChunkPayloadRead {
            file_id: 0,
            offset: 0,
            len: 1,
        },
        ChunkPayloadRead {
            file_id: 0,
            offset: 257,
            len: 1,
        },
        ChunkPayloadRead {
            file_id: 0,
            offset: 1_282,
            len: 1,
        },
        ChunkPayloadRead {
            file_id: 0,
            offset: 5_379,
            len: 1,
        },
    ];

    let mut previous_physical_bytes = 0;
    for (max_gap, expected_reads, expected_bytes) in
        [(0, 4, 4), (256, 3, 260), (1024, 2, 1_284), (4096, 1, 5_380)]
    {
        let plan = plan_chunk_payload_batch(&requests, max_gap).unwrap();
        assert_eq!(plan.physical_read_count(), expected_reads, "gap {max_gap}");
        assert_eq!(plan.physical_bytes_read(), expected_bytes, "gap {max_gap}");
        assert!(
            plan.physical_bytes_read() >= previous_physical_bytes,
            "physical bytes decreased at gap {max_gap}"
        );
        previous_physical_bytes = plan.physical_bytes_read();
    }
}

#[test]
fn chunk_payload_batch_plan_preserves_zero_only_file_identity() {
    let plan = plan_chunk_payload_batch(
        &[ChunkPayloadRead {
            file_id: 1,
            offset: 100,
            len: 0,
        }],
        0,
    )
    .unwrap();
    assert_eq!(plan.file_id(), 1);
    assert_eq!(plan.physical_read_count(), 0);

    let error = plan_chunk_payload_batch(
        &[
            ChunkPayloadRead {
                file_id: 0,
                offset: 0,
                len: 0,
            },
            ChunkPayloadRead {
                file_id: 1,
                offset: 0,
                len: 0,
            },
        ],
        0,
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "chunk payload batch spans multiple files"
    );
}

#[test]
fn chunk_writer_roundtrip_multiple_samples() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

    let entry = writer
        .append_float_chunk(
            3,
            &[(12_000, 1.25), (10_000, 1.0), (10_000, 1.0), (14_000, 2.5)],
        )
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.min_time_ms, 10_000);
    assert_eq!(entry.max_time_ms, 14_000);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 3);
    assert_eq!(
        record.samples,
        ChunkSamples::Float(vec![
            (10_000, 1.0),
            (10_000, 1.0),
            (12_000, 1.25),
            (14_000, 2.5)
        ])
    );
}

#[test]
fn chunk_writer_ordered_float_samples_roundtrip_without_resorting() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

    let entry = writer
        .append_float_chunk_ordered(3, &[(10_000, 1.0), (12_000, 1.25), (14_000, 2.5)])
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.min_time_ms, 10_000);
    assert_eq!(entry.max_time_ms, 14_000);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 3);
    assert_eq!(
        record.samples,
        ChunkSamples::Float(vec![(10_000, 1.0), (12_000, 1.25), (14_000, 2.5)])
    );
}

#[test]
fn chunk_writer_ordered_float_samples_reject_unsorted_input() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

    let err = writer
        .append_float_chunk_ordered(3, &[(12_000, 1.25), (10_000, 1.0)])
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn chunk_writer_roundtrip_float_samples_raw() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

    let entry = writer
        .append_float_chunk_raw(3, &[(12_000, 1.25), (10_000, 1.0), (14_000, 2.5)])
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.min_time_ms, 10_000);
    assert_eq!(entry.max_time_ms, 14_000);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 3);
    assert_eq!(
        record.samples,
        ChunkSamples::Float(vec![(10_000, 1.0), (12_000, 1.25), (14_000, 2.5)])
    );
}

#[test]
fn chunk_writer_roundtrip_int_samples() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

    let entry = writer
        .append_int_chunk(9, &[(10_000, 5), (10_500, -2), (11_000, 10)])
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.min_time_ms, 10_000);
    assert_eq!(entry.max_time_ms, 11_000);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 9);
    assert_eq!(
        record.samples,
        ChunkSamples::Int64(vec![(10_000, 5), (10_500, -2), (11_000, 10)])
    );
}

#[test]
fn chunk_writer_roundtrip_int_samples_raw() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

    let entry = writer
        .append_int_chunk_raw(9, &[(10_000, 5), (10_500, -2), (11_000, 10)])
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.min_time_ms, 10_000);
    assert_eq!(entry.max_time_ms, 11_000);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 9);
    assert_eq!(
        record.samples,
        ChunkSamples::Int64(vec![(10_000, 5), (10_500, -2), (11_000, 10)])
    );
}

#[test]
fn chunk_writer_roundtrip_histogram_samples() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let first = HistogramValue {
        count: 4,
        sum: Some(10.0),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 5.0],
        bucket_counts: vec![1, 2, 1],
    };
    let second = HistogramValue {
        count: 7,
        sum: Some(21.0),
        min: Some(1.0),
        max: Some(6.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 5.0],
        bucket_counts: vec![2, 3, 2],
    };

    let entry = writer
        .append_histogram_chunk_ordered(4, &[(10_000, first.clone()), (12_000, second.clone())])
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.kind, ChunkKind::Histogram);
    assert_eq!(entry.min_time_ms, 10_000);
    assert_eq!(entry.max_time_ms, 12_000);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 4);
    assert_eq!(record.kind, ChunkKind::Histogram);
    assert_eq!(
        record.samples,
        ChunkSamples::Histogram(vec![(10_000, first), (12_000, second)])
    );
}

#[test]
fn chunk_reader_decodes_histogram_scalar_projections_without_full_values() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let first = HistogramValue {
        count: 4,
        sum: Some(10.0),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 5.0, 10.0],
        bucket_counts: vec![1, 2, 1, 0],
    };
    let second_metadata = TypedSampleMetadata {
        start_time_ms: Some(11_000),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    let second = HistogramValue {
        count: 7,
        sum: Some(21.0),
        min: Some(1.0),
        max: Some(6.0),
        metadata: second_metadata,
        explicit_bounds: vec![1.0, 5.0, 10.0],
        bucket_counts: vec![2, 3, 2, 0],
    };

    let entry = writer
        .append_histogram_chunk_ordered(4, &[(10_000, first.clone()), (12_000, second)])
        .unwrap();
    writer.flush().unwrap();

    let mut file = temp.reopen().unwrap();
    let count = read_chunk_scalar_projection_at(
        &mut file,
        entry.offset,
        entry.length,
        ChunkScalarProjection::Count,
    )
    .unwrap();
    assert_eq!(count.series_ref, 4);
    assert_eq!(count.kind, ChunkKind::Histogram);
    assert_eq!(
        count.samples,
        vec![
            ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata: TypedSampleMetadata::default(),
                value: Some(ChunkScalarValue::Count(4)),
            },
            ChunkScalarSample {
                timestamp_ms: 12_000,
                metadata: second_metadata,
                value: Some(ChunkScalarValue::Count(7)),
            },
        ]
    );

    let mut file = temp.reopen().unwrap();
    let sum = read_chunk_scalar_projection_at(
        &mut file,
        entry.offset,
        entry.length,
        ChunkScalarProjection::Sum,
    )
    .unwrap();
    assert_eq!(
        sum.samples,
        vec![
            ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata: TypedSampleMetadata::default(),
                value: Some(ChunkScalarValue::Sum(10.0)),
            },
            ChunkScalarSample {
                timestamp_ms: 12_000,
                metadata: second_metadata,
                value: Some(ChunkScalarValue::Sum(21.0)),
            },
        ]
    );
}

#[test]
fn typed_chunk_index_entry_records_scalar_projection_lane() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let explicit_bounds = (0..128).map(|value| value as f64).collect::<Vec<_>>();
    let bucket_counts = vec![1; explicit_bounds.len() + 1];
    let count = bucket_counts.iter().sum();
    let sample = HistogramValue {
        count,
        sum: Some(8192.0),
        min: Some(0.0),
        max: Some(128.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds,
        bucket_counts,
    };

    let entry = writer
        .append_histogram_chunk_ordered(4, &[(10_000, sample)])
        .unwrap();
    writer.flush().unwrap();

    assert!(
        entry.scalar_lane_offset > 0,
        "typed chunk index entry should store scalar lane offset"
    );
    assert!(
        entry.scalar_lane_len > 0,
        "typed chunk index entry should store scalar lane length"
    );
    assert!(
        entry
            .scalar_lane_offset
            .saturating_add(entry.scalar_lane_len)
            <= entry.length,
        "scalar lane should point inside the chunk record"
    );
    assert!(
        (CHUNK_HEADER_LEN as u32).saturating_add(entry.scalar_lane_len) < entry.length,
        "scalar projection should be readable without reading the full typed payload"
    );

    let mut file = temp.reopen().unwrap();
    let (projection, bytes_read) =
        read_chunk_indexed_scalar_projection_at(&mut file, &entry, ChunkScalarProjection::Count)
            .unwrap();
    assert_eq!(
        bytes_read,
        (CHUNK_HEADER_LEN as u32) + entry.scalar_lane_len
    );
    assert!(bytes_read < entry.length);
    assert_eq!(
        projection.samples,
        vec![ChunkScalarSample {
            timestamp_ms: 10_000,
            metadata: TypedSampleMetadata::default(),
            value: Some(ChunkScalarValue::Count(count)),
        }]
    );
}

#[test]
fn chunk_payload_batch_streams_indexed_scalar_projection_samples() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let first_metadata = TypedSampleMetadata {
        start_time_ms: Some(9_000),
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    let second_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::CounterReset,
        ..TypedSampleMetadata::default()
    };

    let entry = writer
        .append_histogram_chunk_ordered(
            4,
            &[
                (
                    10_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(10.0),
                        min: Some(1.0),
                        max: Some(4.0),
                        metadata: first_metadata,
                        explicit_bounds: vec![1.0, 5.0, 10.0],
                        bucket_counts: vec![1, 2, 1, 0],
                    },
                ),
                (
                    12_000,
                    HistogramValue {
                        count: 7,
                        sum: Some(21.0),
                        min: Some(1.0),
                        max: Some(6.0),
                        metadata: second_metadata,
                        explicit_bounds: vec![1.0, 5.0, 10.0],
                        bucket_counts: vec![2, 3, 2, 0],
                    },
                ),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let read_len = entry.scalar_projection_read_len();
    let mut file = temp.reopen().unwrap();
    let batch = read_chunk_payload_batch(
        &mut file,
        &[ChunkPayloadRead {
            file_id: entry.file_id,
            offset: entry.offset,
            len: u64::from(read_len),
        }],
        0,
    )
    .unwrap();

    let expected_header = ChunkScalarRecordHeader {
        series_ref: 4,
        kind: ChunkKind::Histogram,
        min_time_ms: 10_000,
        max_time_ms: 12_000,
        sample_count: 2,
    };
    let (planned_header, planned_read_len) =
        batch.indexed_scalar_projection_header(&entry).unwrap();
    assert_eq!(planned_header, expected_header);
    assert_eq!(planned_read_len, read_len);

    let mut streamed = Vec::new();
    let (validated_header, bytes_read) = batch
        .for_each_indexed_scalar_projection_sample_with_header(
            &entry,
            ChunkScalarProjection::Sum,
            |sample| {
                streamed.push(sample);
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(validated_header, expected_header);
    assert_eq!(bytes_read, read_len);
    assert_eq!(
        streamed,
        vec![
            ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata: first_metadata,
                value: Some(ChunkScalarValue::Sum(10.0)),
            },
            ChunkScalarSample {
                timestamp_ms: 12_000,
                metadata: second_metadata,
                value: Some(ChunkScalarValue::Sum(21.0)),
            },
        ]
    );
}

#[test]
fn chunk_payload_batch_validates_index_kind_before_scalar_lane_decode() {
    let (temp, entry) = write_scalar_lane_test_chunk();
    let batch = read_scalar_lane_test_batch(&temp, &entry);
    let mut corrupt_entry = entry;
    corrupt_entry.kind = ChunkKind::Summary;

    let err = batch
        .indexed_scalar_projection_header(&corrupt_entry)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "chunk index kind does not match chunk header"
    );

    let mut callbacks = 0usize;
    let err = batch
        .for_each_indexed_scalar_projection_sample_with_header(
            &corrupt_entry,
            ChunkScalarProjection::Count,
            |_| {
                callbacks += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(callbacks, 0);
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "chunk index kind does not match chunk header"
    );
}

#[test]
fn chunk_payload_batch_scalar_fallback_validates_index_kind_before_callbacks() {
    let (temp, entry) = write_scalar_lane_test_chunk();
    let mut file = temp.reopen().unwrap();
    let batch = read_chunk_payload_batch(
        &mut file,
        &[ChunkPayloadRead {
            file_id: entry.file_id,
            offset: entry.offset,
            len: u64::from(entry.length),
        }],
        0,
    )
    .unwrap();
    let mut corrupt_entry = entry;
    corrupt_entry.kind = ChunkKind::Summary;
    corrupt_entry.scalar_lane_offset = 0;
    corrupt_entry.scalar_lane_len = 0;

    let mut callbacks = 0usize;
    let err = batch
        .for_each_indexed_scalar_projection_sample_with_header(
            &corrupt_entry,
            ChunkScalarProjection::Count,
            |_| {
                callbacks += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(callbacks, 0);
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        err.to_string(),
        "chunk index kind does not match chunk header"
    );
}

#[test]
fn chunk_payload_batch_scalar_header_preserves_lane_range_and_batch_read_errors() {
    let (temp, entry) = write_scalar_lane_test_chunk();
    let batch = read_scalar_lane_test_batch(&temp, &entry);
    let mut incomplete_lane = entry.clone();
    incomplete_lane.scalar_lane_offset = 0;

    let err = batch
        .indexed_scalar_projection_header(&incomplete_lane)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(err.to_string(), "chunk scalar lane range is incomplete");

    let err = ChunkPayloadBatch::empty()
        .indexed_scalar_projection_header(&entry)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(err.to_string(), "chunk payload request missing from batch");
}

#[test]
fn chunk_payload_batch_header_parse_does_not_hide_scalar_lane_crc_errors() {
    let (temp, entry) = write_scalar_lane_test_chunk();
    let scalar_body_offset =
        entry.offset + u64::from(entry.scalar_lane_offset) + TYPED_SCALAR_LANE_HEADER_LEN as u64;
    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(scalar_body_offset)).unwrap();
    let mut corrupt_byte = [0u8; 1];
    file.read_exact(&mut corrupt_byte).unwrap();
    corrupt_byte[0] ^= 1;
    file.seek(SeekFrom::Start(scalar_body_offset)).unwrap();
    file.write_all(&corrupt_byte).unwrap();
    file.flush().unwrap();

    let batch = read_scalar_lane_test_batch(&temp, &entry);
    let (header, read_len) = batch.indexed_scalar_projection_header(&entry).unwrap();
    assert_eq!(
        header,
        ChunkScalarRecordHeader {
            series_ref: 4,
            kind: ChunkKind::Histogram,
            min_time_ms: 10_000,
            max_time_ms: 10_000,
            sample_count: 1,
        }
    );
    assert_eq!(read_len, entry.scalar_projection_read_len());

    let mut callbacks = 0usize;
    let err = batch
        .for_each_indexed_scalar_projection_sample_with_header(
            &entry,
            ChunkScalarProjection::Count,
            |_| {
                callbacks += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(callbacks, 0);
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(err.to_string(), "typed scalar lane crc mismatch");
}

#[test]
fn chunk_payload_batch_scalar_callback_rejects_trailing_lane_bytes() {
    let (temp, mut entry) = write_scalar_lane_test_chunk();
    let scalar_lane_offset = entry.offset + u64::from(entry.scalar_lane_offset);
    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(scalar_lane_offset + 8)).unwrap();
    let mut body_len_bytes = [0u8; 4];
    file.read_exact(&mut body_len_bytes).unwrap();
    let body_len = u32::from_le_bytes(body_len_bytes);
    let extended_body_len = body_len.checked_add(1).unwrap();
    let mut extended_body = vec![0u8; extended_body_len as usize];
    file.seek(SeekFrom::Start(
        scalar_lane_offset + TYPED_SCALAR_LANE_HEADER_LEN as u64,
    ))
    .unwrap();
    file.read_exact(&mut extended_body).unwrap();
    let extended_body_crc = crc32c(&extended_body);
    file.seek(SeekFrom::Start(scalar_lane_offset + 8)).unwrap();
    file.write_all(&extended_body_len.to_le_bytes()).unwrap();
    file.write_all(&extended_body_crc.to_le_bytes()).unwrap();
    file.flush().unwrap();
    entry.scalar_lane_len = entry.scalar_lane_len.checked_add(1).unwrap();

    let batch = read_scalar_lane_test_batch(&temp, &entry);
    let mut callbacks = 0usize;
    let err = batch
        .for_each_indexed_scalar_projection_sample_with_header(
            &entry,
            ChunkScalarProjection::Sum,
            |_| {
                callbacks += 1;
                Ok(())
            },
        )
        .unwrap_err();

    assert_eq!(callbacks, 1);
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(err.to_string(), "typed scalar lane has trailing bytes");
}

#[test]
fn chunk_payload_batch_scalar_callback_validates_header_sample_count() {
    let (temp, entry) = write_scalar_lane_test_chunk();
    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(entry.offset + 24)).unwrap();
    file.write_all(&0u32.to_le_bytes()).unwrap();
    file.flush().unwrap();

    let batch = read_scalar_lane_test_batch(&temp, &entry);
    let (header, _) = batch.indexed_scalar_projection_header(&entry).unwrap();
    assert_eq!(header.sample_count, 0);

    let mut callbacks = 0usize;
    let err = batch
        .for_each_indexed_scalar_projection_sample_with_header(
            &entry,
            ChunkScalarProjection::Count,
            |_| {
                callbacks += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(callbacks, 0);
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(err.to_string(), "typed scalar lane has trailing bytes");
}

#[test]
fn chunk_payload_batch_streaming_scalar_projection_propagates_callback_errors() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let entry = writer
        .append_histogram_chunk_ordered(
            4,
            &[(
                10_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0, 10.0],
                    bucket_counts: vec![1, 2, 1, 0],
                },
            )],
        )
        .unwrap();
    writer.flush().unwrap();

    let read_len = entry.scalar_projection_read_len();
    let mut file = temp.reopen().unwrap();
    let batch = read_chunk_payload_batch(
        &mut file,
        &[ChunkPayloadRead {
            file_id: entry.file_id,
            offset: entry.offset,
            len: u64::from(read_len),
        }],
        0,
    )
    .unwrap();

    let mut callbacks = 0usize;
    let err = batch
        .for_each_indexed_scalar_projection_sample(&entry, ChunkScalarProjection::Count, |_| {
            callbacks += 1;
            Err(io::Error::new(io::ErrorKind::Interrupted, "stop streaming"))
        })
        .unwrap_err();

    assert_eq!(callbacks, 1);
    assert_eq!(err.kind(), io::ErrorKind::Interrupted);
}

#[test]
fn chunk_payload_batch_streaming_scalar_projection_falls_back_to_full_chunk_without_scalar_lane() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    let entry = writer
        .append_histogram_chunk_ordered(
            4,
            &[
                (
                    10_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(10.0),
                        min: Some(1.0),
                        max: Some(4.0),
                        metadata,
                        explicit_bounds: vec![1.0, 5.0, 10.0],
                        bucket_counts: vec![1, 2, 1, 0],
                    },
                ),
                (
                    12_000,
                    HistogramValue {
                        count: 7,
                        sum: Some(21.0),
                        min: Some(1.0),
                        max: Some(6.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 5.0, 10.0],
                        bucket_counts: vec![2, 3, 2, 0],
                    },
                ),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let mut legacy_entry = entry.clone();
    legacy_entry.scalar_lane_offset = 0;
    legacy_entry.scalar_lane_len = 0;

    let mut file = temp.reopen().unwrap();
    let batch = read_chunk_payload_batch(
        &mut file,
        &[ChunkPayloadRead {
            file_id: entry.file_id,
            offset: entry.offset,
            len: u64::from(entry.length),
        }],
        0,
    )
    .unwrap();

    let mut streamed = Vec::new();
    let (header, bytes_read) = batch
        .for_each_indexed_scalar_projection_sample_with_header(
            &legacy_entry,
            ChunkScalarProjection::Count,
            |sample| {
                streamed.push(sample);
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(
        header,
        ChunkScalarRecordHeader {
            series_ref: 4,
            kind: ChunkKind::Histogram,
            min_time_ms: 10_000,
            max_time_ms: 12_000,
            sample_count: 2,
        }
    );
    assert_eq!(bytes_read, entry.length);
    assert!(bytes_read > entry.scalar_projection_read_len());
    assert_eq!(
        streamed,
        vec![
            ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata,
                value: Some(ChunkScalarValue::Count(4)),
            },
            ChunkScalarSample {
                timestamp_ms: 12_000,
                metadata: TypedSampleMetadata::default(),
                value: Some(ChunkScalarValue::Count(7)),
            },
        ]
    );
}

#[test]
fn chunk_writer_roundtrip_exponential_histogram_samples() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let first = ExponentialHistogramValue {
        count: 6,
        sum: Some(15.0),
        min: Some(1.0),
        max: Some(8.0),
        scale: 2,
        zero_threshold: 0.125,
        zero_count: 1,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(9_000),
            flags: OTLP_FLAG_NO_RECORDED_VALUE,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
        },
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![0],
        },
    };
    let second = ExponentialHistogramValue {
        count: 9,
        sum: Some(27.0),
        min: Some(1.0),
        max: Some(10.0),
        scale: 2,
        zero_threshold: 0.125,
        zero_count: 2,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(10_000),
            flags: 0,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::CounterReset,
        },
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![3, 4],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![0],
        },
    };

    let entry = writer
        .append_exponential_histogram_chunk_ordered(
            5,
            &[(10_000, first.clone()), (12_000, second.clone())],
        )
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.kind, ChunkKind::ExponentialHistogram);
    assert!(entry.flags & CHUNK_FLAG_HAS_START_TIME != 0);
    assert!(entry.flags & CHUNK_FLAG_HAS_PER_SAMPLE_FLAGS != 0);
    assert!(entry.flags & CHUNK_FLAG_HAS_COUNTER_RESET_HINTS != 0);
    assert!(entry.flags & CHUNK_FLAG_TEMPORALITY_DELTA != 0);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 5);
    assert_eq!(record.kind, ChunkKind::ExponentialHistogram);
    assert_eq!(
        record.samples,
        ChunkSamples::ExponentialHistogram(vec![(10_000, first), (12_000, second)])
    );
}

#[test]
fn chunk_reader_decodes_exponential_histogram_scalar_projections_without_full_values() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let metadata = TypedSampleMetadata {
        start_time_ms: Some(9_000),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::CounterReset,
    };
    let sample = ExponentialHistogramValue {
        count: 6,
        sum: Some(15.0),
        min: Some(1.0),
        max: Some(8.0),
        scale: 2,
        zero_threshold: 0.125,
        zero_count: 1,
        metadata,
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![0],
        },
    };

    let entry = writer
        .append_exponential_histogram_chunk_ordered(5, &[(10_000, sample)])
        .unwrap();
    writer.flush().unwrap();

    let mut file = temp.reopen().unwrap();
    let count = read_chunk_scalar_projection_at(
        &mut file,
        entry.offset,
        entry.length,
        ChunkScalarProjection::Count,
    )
    .unwrap();
    assert_eq!(
        count.samples,
        vec![ChunkScalarSample {
            timestamp_ms: 10_000,
            metadata,
            value: Some(ChunkScalarValue::Count(6)),
        }]
    );

    let mut file = temp.reopen().unwrap();
    let sum = read_chunk_scalar_projection_at(
        &mut file,
        entry.offset,
        entry.length,
        ChunkScalarProjection::Sum,
    )
    .unwrap();
    assert_eq!(
        sum.samples,
        vec![ChunkScalarSample {
            timestamp_ms: 10_000,
            metadata,
            value: Some(ChunkScalarValue::Sum(15.0)),
        }]
    );
}

#[test]
fn chunk_writer_roundtrip_summary_samples() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let first = SummaryValue {
        count: 10,
        sum: 50.0,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![
            SummaryQuantileValue {
                quantile: 0.5,
                value: 4.0,
            },
            SummaryQuantileValue {
                quantile: 0.9,
                value: 8.0,
            },
        ],
    };
    let second = SummaryValue {
        count: 12,
        sum: 66.0,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![
            SummaryQuantileValue {
                quantile: 0.5,
                value: 5.0,
            },
            SummaryQuantileValue {
                quantile: 0.9,
                value: 9.0,
            },
        ],
    };

    let entry = writer
        .append_summary_chunk_ordered(6, &[(10_000, first.clone()), (12_000, second.clone())])
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(entry.kind, ChunkKind::Summary);

    let mut file = temp.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut reader = ChunkReader::new(file);
    let record = reader.read_next().unwrap().unwrap();
    assert_eq!(record.series_ref, 6);
    assert_eq!(record.kind, ChunkKind::Summary);
    assert_eq!(
        record.samples,
        ChunkSamples::Summary(vec![(10_000, first), (12_000, second)])
    );
}

#[test]
fn chunk_reader_decodes_summary_scalar_projections_without_full_values() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
    let metadata = TypedSampleMetadata::default();
    let sample = SummaryValue {
        count: 10,
        sum: 50.0,
        metadata,
        quantiles: vec![
            SummaryQuantileValue {
                quantile: 0.5,
                value: 4.0,
            },
            SummaryQuantileValue {
                quantile: 0.9,
                value: 8.0,
            },
        ],
    };

    let entry = writer
        .append_summary_chunk_ordered(6, &[(10_000, sample)])
        .unwrap();
    writer.flush().unwrap();

    let mut file = temp.reopen().unwrap();
    let count = read_chunk_scalar_projection_at(
        &mut file,
        entry.offset,
        entry.length,
        ChunkScalarProjection::Count,
    )
    .unwrap();
    assert_eq!(
        count.samples,
        vec![ChunkScalarSample {
            timestamp_ms: 10_000,
            metadata,
            value: Some(ChunkScalarValue::Count(10)),
        }]
    );

    let mut file = temp.reopen().unwrap();
    let sum = read_chunk_scalar_projection_at(
        &mut file,
        entry.offset,
        entry.length,
        ChunkScalarProjection::Sum,
    )
    .unwrap();
    assert_eq!(
        sum.samples,
        vec![ChunkScalarSample {
            timestamp_ms: 10_000,
            metadata,
            value: Some(ChunkScalarValue::Sum(50.0)),
        }]
    );
}

#[test]
fn chunk_index_writer_writes_offsets() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let entries = vec![
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 1,
            max_time_ms: 2,
            offset: 10,
            length: 20,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
        Vec::new(),
    ];

    let mut file = temp.reopen().unwrap();
    write_chunk_index(&mut file, &entries).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut header = [0u8; 4];
    file.read_exact(&mut header).unwrap();
    assert_eq!(u32::from_le_bytes(header), CHUNK_INDEX_MAGIC);
}

#[test]
fn chunk_index_reader_reads_target_offsets_lazily() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let num_series = 100_000u32;
    let series_ref = 42usize;
    let offset_table_start = 4 + 2 + 2 + 4;
    let data_start = offset_table_start as u64 + (u64::from(num_series) + 1) * 8;
    let entry = ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms: 100,
        max_time_ms: 200,
        offset: 10,
        length: 20,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    };

    let mut file = temp.reopen().unwrap();
    file.write_all(&CHUNK_INDEX_MAGIC.to_le_bytes()).unwrap();
    file.write_all(&1u16.to_le_bytes()).unwrap();
    file.write_all(&0u16.to_le_bytes()).unwrap();
    file.write_all(&num_series.to_le_bytes()).unwrap();
    file.write_all(&data_start.to_le_bytes()).unwrap();
    file.seek(SeekFrom::Start(
        offset_table_start as u64 + (series_ref as u64) * 8,
    ))
    .unwrap();
    file.write_all(&data_start.to_le_bytes()).unwrap();
    file.write_all(&(data_start + chunk_entry_len() as u64).to_le_bytes())
        .unwrap();
    file.seek(SeekFrom::Start(data_start)).unwrap();
    write_chunk_entry(&mut file, &entry).unwrap();
    file.flush().unwrap();

    let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();
    let entries = reader.read_entries(series_ref as u32).unwrap().unwrap();

    assert_eq!(entries, vec![entry]);
}

#[test]
fn chunk_index_ranges_read_back_written_series_entries() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let entries = vec![
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 100,
            max_time_ms: 200,
            offset: 10,
            length: 20,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
        Vec::new(),
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Histogram,
            flags: 0,
            min_time_ms: 300,
            max_time_ms: 400,
            offset: 30,
            length: 40,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
    ];
    let ranges = chunk_index_ranges(&entries).unwrap();
    write_chunk_index(temp.reopen().unwrap(), &entries).unwrap();

    let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();

    assert_eq!(reader.read_entries_range(ranges[0]).unwrap(), entries[0]);
    assert_eq!(
        reader.read_entries_range(ranges[1]).unwrap(),
        Vec::<ChunkIndexEntry>::new()
    );
    assert_eq!(reader.read_entries_range(ranges[2]).unwrap(), entries[2]);
}

#[test]
fn chunk_index_reader_reads_multiple_ranges_in_one_call() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let entries = vec![
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 100,
            max_time_ms: 200,
            offset: 10,
            length: 20,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
        Vec::new(),
        vec![
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Histogram,
                flags: 0,
                min_time_ms: 300,
                max_time_ms: 400,
                offset: 30,
                length: 40,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Float,
                flags: 0,
                min_time_ms: 500,
                max_time_ms: 600,
                offset: 70,
                length: 80,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
        ],
    ];
    let ranges = chunk_index_ranges(&entries).unwrap();
    write_chunk_index(temp.reopen().unwrap(), &entries).unwrap();

    let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();
    let decoded = reader
        .read_entries_ranges(&[ranges[2], ranges[0], ranges[1], ranges[0]])
        .unwrap();

    assert_eq!(decoded.get(&ranges[0]).unwrap(), &entries[0]);
    assert_eq!(
        decoded.get(&ranges[1]).unwrap(),
        &Vec::<ChunkIndexEntry>::new()
    );
    assert_eq!(decoded.get(&ranges[2]).unwrap(), &entries[2]);
}

#[test]
fn chunk_index_range_rejects_offsets_before_entry_data() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let entries = vec![vec![ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms: 100,
        max_time_ms: 200,
        offset: 10,
        length: 20,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    }]];
    write_chunk_index(temp.reopen().unwrap(), &entries).unwrap();
    let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();

    let err = reader
        .read_entries_range(ChunkIndexRange {
            offset: CHUNK_INDEX_HEADER_LEN,
            len: CHUNK_ENTRY_LEN as u32,
        })
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn chunk_index_writer_buffers_underlying_writes() {
    let entries = vec![
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 100,
            max_time_ms: 200,
            offset: 10,
            length: 20,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Int64,
            flags: 0,
            min_time_ms: 300,
            max_time_ms: 400,
            offset: 30,
            length: 40,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
    ];
    let mut writer = CountingWriter::default();

    write_chunk_index(&mut writer, &entries).unwrap();

    assert!(
        writer.write_calls <= 2,
        "chunk index writer used {} underlying writes",
        writer.write_calls
    );
    let mut cursor = std::io::Cursor::new(writer.bytes);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic).unwrap();
    assert_eq!(u32::from_le_bytes(magic), CHUNK_INDEX_MAGIC);
}

#[test]
fn chunk_index_roundtrips_entries() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let entries = vec![
        vec![
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Float,
                flags: 0,
                min_time_ms: 100,
                max_time_ms: 200,
                offset: 10,
                length: 20,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Int64,
                flags: 1,
                min_time_ms: 300,
                max_time_ms: 400,
                offset: 30,
                length: 40,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
        ],
        Vec::new(),
    ];

    let mut file = temp.reopen().unwrap();
    write_chunk_index(&mut file, &entries).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let read = read_chunk_index(&mut file).unwrap();
    assert_eq!(read, entries);
}

#[test]
fn chunk_index_reader_fetches_entries_for_one_series() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let entries = vec![
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 100,
            max_time_ms: 200,
            offset: 10,
            length: 20,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
        vec![
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Int64,
                flags: 1,
                min_time_ms: 300,
                max_time_ms: 400,
                offset: 30,
                length: 40,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Float,
                flags: 0,
                min_time_ms: 500,
                max_time_ms: 600,
                offset: 70,
                length: 80,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
        ],
    ];

    let mut file = temp.reopen().unwrap();
    write_chunk_index(&mut file, &entries).unwrap();
    let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();

    assert_eq!(reader.len(), 2);
    assert_eq!(reader.read_entries(1).unwrap(), Some(entries[1].clone()));
    assert_eq!(reader.read_entries(99).unwrap(), None);
}

#[test]
fn chunk_index_reader_streams_series_entries_in_order() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let entries = vec![
        vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 100,
            max_time_ms: 200,
            offset: 10,
            length: 20,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }],
        Vec::new(),
        vec![
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Histogram,
                flags: 1,
                min_time_ms: 300,
                max_time_ms: 400,
                offset: 30,
                length: 40,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
            ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Summary,
                flags: 2,
                min_time_ms: 500,
                max_time_ms: 600,
                offset: 70,
                length: 80,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
        ],
    ];

    let mut file = temp.reopen().unwrap();
    write_chunk_index(&mut file, &entries).unwrap();
    let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();
    let mut streamed = Vec::new();

    reader
        .for_each_series_entries(|series_ref, series_entries| {
            streamed.push((series_ref, series_entries.to_vec()));
            Ok(())
        })
        .unwrap();

    assert_eq!(
        streamed,
        vec![
            (0, entries[0].clone()),
            (1, entries[1].clone()),
            (2, entries[2].clone()),
        ]
    );
}
