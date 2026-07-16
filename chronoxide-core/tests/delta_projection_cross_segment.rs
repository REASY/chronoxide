use std::path::Path;
use std::time::Duration;

use chronoxide_core::labels::{METRIC_NAME_LABEL, SeriesRef};
use chronoxide_core::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality, TypedSampleMetadata,
    prometheus_stale_nan,
};
use chronoxide_core::storage::segment::{
    QueryProjectionConfig, SegmentStorageSchema, SegmentStoreOpenOptions, SegmentStoreReader,
    SegmentStoreSchemaPolicy, SegmentWriter, SegmentWriterConfig,
};

const SEGMENT_DURATION: Duration = Duration::from_secs(10);

fn delta_metadata(start_time_ms: u64, reset_hint: CounterResetHint) -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint,
        ..TypedSampleMetadata::default()
    }
}

fn stale_delta_metadata() -> TypedSampleMetadata {
    TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::Unknown,
        ..TypedSampleMetadata::default()
    }
}

fn histogram(count: u64, sum: Option<f64>, metadata: TypedSampleMetadata) -> HistogramValue {
    HistogramValue {
        count,
        sum,
        min: None,
        max: None,
        metadata,
        explicit_bounds: Vec::new(),
        bucket_counts: vec![count],
    }
}

fn exponential_histogram(
    count: u64,
    sum: Option<f64>,
    metadata: TypedSampleMetadata,
) -> ExponentialHistogramValue {
    ExponentialHistogramValue {
        count,
        sum,
        min: None,
        max: None,
        metadata,
        scale: 0,
        zero_count: 0,
        zero_threshold: 0.0,
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![count],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    }
}

fn writer(path: &Path, schema: SegmentStorageSchema) -> SegmentWriter {
    SegmentWriter::new(SegmentWriterConfig::new(path, SEGMENT_DURATION).with_storage_schema(schema))
        .unwrap()
}

fn store(path: &Path, schema: SegmentStorageSchema) -> SegmentStoreReader {
    let storage_schema_policy = match schema {
        SegmentStorageSchema::Schema7 => SegmentStoreSchemaPolicy::StrictSchema7,
        SegmentStorageSchema::Schema8 => SegmentStoreSchemaPolicy::StrictSchema8,
        SegmentStorageSchema::Schema6 => unreachable!("this test covers schema 7 and schema 8"),
    };
    SegmentStoreReader::open_with_options(
        path,
        SegmentStoreOpenOptions {
            storage_schema_policy,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap()
    .with_query_projection_config(
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
}

fn query_samples(store: &SegmentStoreReader, query: &str) -> Vec<(u64, f64)> {
    query_samples_at(store, query, 40_000)
}

fn query_samples_at(store: &SegmentStoreReader, query: &str, end_ms: u64) -> Vec<(u64, f64)> {
    let results = store.query_promql(query, 0, end_ms).unwrap();
    assert_eq!(results.len(), 1, "expected one result for {query}");
    results[0].samples.clone()
}

fn assert_samples_bits(actual: &[(u64, f64)], expected: &[(u64, f64)]) {
    assert_eq!(actual.len(), expected.len());
    for ((actual_ts, actual_value), (expected_ts, expected_value)) in actual.iter().zip(expected) {
        assert_eq!(actual_ts, expected_ts);
        assert_eq!(actual_value.to_bits(), expected_value.to_bits());
    }
}

#[test]
fn delta_histogram_virtual_scalars_reset_for_same_fragment_gaps_and_overlaps() {
    for schema in [SegmentStorageSchema::Schema7, SegmentStorageSchema::Schema8] {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = writer(tempdir.path(), schema);
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(7),
                &[
                    (
                        1_000,
                        histogram(
                            2,
                            None,
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        3_000,
                        histogram(
                            3,
                            None,
                            delta_metadata(2_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        5_000,
                        histogram(
                            5,
                            None,
                            delta_metadata(2_500, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "same_fragment_hist"),
            )
            .unwrap();
        writer.flush().unwrap();

        let store = store(tempdir.path(), schema);
        let expected = [(1_000, 2.0), (3_000, 3.0), (5_000, 5.0)];
        assert_samples_bits(
            &query_samples_at(&store, "same_fragment_hist_count", 6_000),
            &expected,
        );
        assert_samples_bits(
            &query_samples_at(&store, r#"same_fragment_hist_bucket{le="+Inf"}"#, 6_000),
            &expected,
        );
        assert_samples_bits(
            &query_samples_at(&store, "resets(same_fragment_hist_count[6s])", 6_000),
            &[(6_000, 2.0)],
        );
    }
}

#[test]
fn delta_exponential_histogram_virtual_scalars_reset_for_same_fragment_gaps_and_overlaps() {
    for schema in [SegmentStorageSchema::Schema7, SegmentStorageSchema::Schema8] {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = writer(tempdir.path(), schema);
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(8),
                &[
                    (
                        1_000,
                        exponential_histogram(
                            2,
                            None,
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        3_000,
                        exponential_histogram(
                            3,
                            None,
                            delta_metadata(2_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        5_000,
                        exponential_histogram(
                            5,
                            None,
                            delta_metadata(2_500, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "same_fragment_exphist"),
            )
            .unwrap();
        writer.flush().unwrap();

        let store = store(tempdir.path(), schema);
        let expected = [(1_000, 2.0), (3_000, 3.0), (5_000, 5.0)];
        assert_samples_bits(
            &query_samples_at(&store, "same_fragment_exphist_count", 6_000),
            &expected,
        );
        assert_samples_bits(
            &query_samples_at(&store, r#"same_fragment_exphist_bucket{le="+Inf"}"#, 6_000),
            &expected,
        );
        assert_samples_bits(
            &query_samples_at(&store, "resets(same_fragment_exphist_count[6s])", 6_000),
            &[(6_000, 2.0)],
        );
    }
}

#[test]
fn delta_histogram_virtual_scalars_stitch_raw_intervals_across_segments() {
    for schema in [SegmentStorageSchema::Schema7, SegmentStorageSchema::Schema8] {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = writer(tempdir.path(), schema);
        let large_count = (1u64 << 53) + 1;
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[
                    (
                        5_000,
                        histogram(
                            large_count,
                            Some(1.0e20),
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        15_000,
                        histogram(
                            1,
                            Some(-1.0e20),
                            delta_metadata(5_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        16_000,
                        histogram(
                            1,
                            Some(1.0),
                            delta_metadata(15_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "cross_segment_hist"),
            )
            .unwrap();
        writer.flush().unwrap();

        let store = store(tempdir.path(), schema);
        let expected_count = vec![
            (5_000, large_count as f64),
            (15_000, large_count.saturating_add(1) as f64),
            (16_000, large_count.saturating_add(2) as f64),
        ];
        assert_samples_bits(
            &query_samples(&store, "cross_segment_hist_count"),
            &expected_count,
        );
        assert_samples_bits(
            &query_samples(&store, r#"cross_segment_hist_bucket{le="+Inf"}"#),
            &expected_count,
        );
        assert_samples_bits(
            &query_samples(&store, "cross_segment_hist_sum"),
            &[(5_000, 1.0e20), (15_000, 0.0), (16_000, 1.0)],
        );
    }
}

#[test]
fn delta_exponential_histogram_virtual_scalars_stitch_raw_intervals_across_segments() {
    for schema in [SegmentStorageSchema::Schema7, SegmentStorageSchema::Schema8] {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = writer(tempdir.path(), schema);
        let large_count = (1u64 << 53) + 1;
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[
                    (
                        5_000,
                        exponential_histogram(
                            large_count,
                            Some(1.0e20),
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        15_000,
                        exponential_histogram(
                            1,
                            Some(-1.0e20),
                            delta_metadata(5_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        16_000,
                        exponential_histogram(
                            1,
                            Some(1.0),
                            delta_metadata(15_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "cross_segment_exphist"),
            )
            .unwrap();
        writer.flush().unwrap();

        let store = store(tempdir.path(), schema);
        let expected_count = vec![
            (5_000, large_count as f64),
            (15_000, large_count.saturating_add(1) as f64),
            (16_000, large_count.saturating_add(2) as f64),
        ];
        assert_samples_bits(
            &query_samples(&store, "cross_segment_exphist_count"),
            &expected_count,
        );
        assert_samples_bits(
            &query_samples(&store, r#"cross_segment_exphist_bucket{le="+Inf"}"#),
            &expected_count,
        );
        assert_samples_bits(
            &query_samples(&store, "cross_segment_exphist_sum"),
            &[(5_000, 1.0e20), (15_000, 0.0), (16_000, 1.0)],
        );
    }
}

#[test]
fn delta_histogram_cross_segment_stitch_preserves_logical_boundaries_and_stale_gap() {
    for schema in [SegmentStorageSchema::Schema7, SegmentStorageSchema::Schema8] {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = writer(tempdir.path(), schema);
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(3),
                &[
                    (
                        5_000,
                        histogram(
                            2,
                            None,
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        15_000,
                        histogram(
                            3,
                            None,
                            delta_metadata(5_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        25_000,
                        histogram(
                            7,
                            None,
                            delta_metadata(20_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        35_000,
                        histogram(
                            11,
                            None,
                            delta_metadata(25_000, CounterResetHint::CounterReset),
                        ),
                    ),
                    (
                        36_000,
                        histogram(
                            13,
                            None,
                            delta_metadata(35_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "boundary_hist"),
            )
            .unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(4),
                &[
                    (
                        5_000,
                        histogram(
                            2,
                            None,
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (15_000, histogram(0, None, stale_delta_metadata())),
                    (
                        25_000,
                        histogram(
                            3,
                            None,
                            delta_metadata(15_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "stale_gap_hist"),
            )
            .unwrap();
        writer.flush().unwrap();

        let store = store(tempdir.path(), schema);
        assert_samples_bits(
            &query_samples(&store, "boundary_hist_count"),
            &[
                (5_000, 2.0),
                (15_000, 5.0),
                (25_000, 7.0),
                (35_000, 11.0),
                (36_000, 24.0),
            ],
        );
        assert_samples_bits(
            &query_samples(&store, "stale_gap_hist_count"),
            &[
                (5_000, 2.0),
                (15_000, prometheus_stale_nan()),
                (25_000, 3.0),
            ],
        );
        assert_samples_bits(
            &query_samples(&store, "increase(stale_gap_hist_count[40s])"),
            &[(40_000, 5.0)],
        );
    }
}

#[test]
fn delta_exponential_histogram_cross_segment_stitch_preserves_logical_boundaries_and_stale_gap() {
    for schema in [SegmentStorageSchema::Schema7, SegmentStorageSchema::Schema8] {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = writer(tempdir.path(), schema);
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(5),
                &[
                    (
                        5_000,
                        exponential_histogram(
                            2,
                            None,
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        15_000,
                        exponential_histogram(
                            3,
                            None,
                            delta_metadata(5_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        25_000,
                        exponential_histogram(
                            7,
                            None,
                            delta_metadata(20_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        35_000,
                        exponential_histogram(
                            11,
                            None,
                            delta_metadata(25_000, CounterResetHint::CounterReset),
                        ),
                    ),
                    (
                        36_000,
                        exponential_histogram(
                            13,
                            None,
                            delta_metadata(35_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "boundary_exphist"),
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(6),
                &[
                    (
                        5_000,
                        exponential_histogram(
                            2,
                            None,
                            delta_metadata(0, CounterResetHint::NotCounterReset),
                        ),
                    ),
                    (
                        15_000,
                        exponential_histogram(0, None, stale_delta_metadata()),
                    ),
                    (
                        25_000,
                        exponential_histogram(
                            3,
                            None,
                            delta_metadata(15_000, CounterResetHint::NotCounterReset),
                        ),
                    ),
                ],
                |visit| visit(METRIC_NAME_LABEL, "stale_gap_exphist"),
            )
            .unwrap();
        writer.flush().unwrap();

        let store = store(tempdir.path(), schema);
        assert_samples_bits(
            &query_samples(&store, "boundary_exphist_count"),
            &[
                (5_000, 2.0),
                (15_000, 5.0),
                (25_000, 7.0),
                (35_000, 11.0),
                (36_000, 24.0),
            ],
        );
        assert_samples_bits(
            &query_samples(&store, "stale_gap_exphist_count"),
            &[
                (5_000, 2.0),
                (15_000, prometheus_stale_nan()),
                (25_000, 3.0),
            ],
        );
        assert_samples_bits(
            &query_samples(&store, "increase(stale_gap_exphist_count[40s])"),
            &[(40_000, 5.0)],
        );
    }
}
