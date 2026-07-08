use super::*;
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality, SummaryQuantileValue, SummaryValue,
    TypedSampleMetadata,
};
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
                offset: first.offset,
                len: u64::from(first.length),
            },
            ChunkPayloadRead {
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
            offset: entry.offset,
            len: u64::from(read_len),
        }],
        0,
    )
    .unwrap();

    let mut streamed = Vec::new();
    let bytes_read = batch
        .for_each_indexed_scalar_projection_sample(&entry, ChunkScalarProjection::Sum, |sample| {
            streamed.push(sample);
            Ok(())
        })
        .unwrap();

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
            offset: entry.offset,
            len: u64::from(entry.length),
        }],
        0,
    )
    .unwrap();

    let mut streamed = Vec::new();
    let bytes_read = batch
        .for_each_indexed_scalar_projection_sample(
            &legacy_entry,
            ChunkScalarProjection::Count,
            |sample| {
                streamed.push(sample);
                Ok(())
            },
        )
        .unwrap();

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
    let mut entries = Vec::new();
    entries.push(vec![ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms: 1,
        max_time_ms: 2,
        offset: 10,
        length: 20,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    }]);
    entries.push(Vec::new());

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
