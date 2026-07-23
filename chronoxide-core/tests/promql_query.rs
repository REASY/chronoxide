use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
    SeriesRef,
};
use chronoxide_core::promql::{PromqlQueryError, normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, FloatEncoding,
    HeadBuffer, HeadConfig, HistogramValue, IntEncoding, OTLP_FLAG_NO_RECORDED_VALUE,
    OtlpAggregationTemporality, SampleValue, SummaryQuantileValue, SummaryValue,
    TypedSampleMetadata, prometheus_stale_nan,
};
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, read_current,
};
use chronoxide_core::storage::segment::{
    QueryLabelMaterializationPolicy, QueryLabelStoragePolicy, QueryLimits, QueryProjectionConfig,
    RangeExecutionFallbackReason, RangeExecutionMode, RangeExecutionTerminalReason, SegmentFile,
    SegmentId, SegmentIdError, SegmentIdProvider, SegmentQueryResult, SegmentStoreReader,
    SegmentWriter, SegmentWriterConfig,
};

#[path = "promql_query/classic_histograms.rs"]
mod classic_histograms;
#[path = "promql_query/instant_functions.rs"]
mod instant_functions;
#[path = "promql_query/instant_operators_and_aggregations.rs"]
mod instant_operators_and_aggregations;
#[path = "promql_query/native_exponential_histograms.rs"]
mod native_exponential_histograms;
#[path = "promql_query/native_histogram_operators.rs"]
mod native_histogram_operators;
#[path = "promql_query/native_histogram_runtime.rs"]
mod native_histogram_runtime;
#[path = "promql_query/range_execution.rs"]
mod range_execution;
#[path = "promql_query/scalar_range_functions.rs"]
mod scalar_range_functions;
#[path = "promql_query/selectors_sessions_and_limits.rs"]
mod selectors_sessions_and_limits;
#[path = "promql_query/typed_delta_semantics.rs"]
mod typed_delta_semantics;
#[path = "promql_query/typed_projections.rs"]
mod typed_projections;

#[derive(Debug, Clone, Copy)]
struct FixedUlidSegmentIdProvider {
    ulid: &'static str,
}

impl SegmentIdProvider for FixedUlidSegmentIdProvider {
    fn next_segment_id(&self, start_ms: u64, end_ms: u64) -> Result<SegmentId, SegmentIdError> {
        SegmentId::parse_dir_name(&format!("seg-{start_ms}-{end_ms}-{}", self.ulid))
    }
}

fn open_default_store(path: impl AsRef<Path>) -> SegmentStoreReader {
    SegmentStoreReader::open(path).unwrap()
}

fn open_default_store_with_query_projection_config(
    path: impl AsRef<Path>,
    query_projection_config: QueryProjectionConfig,
) -> std::io::Result<SegmentStoreReader> {
    SegmentStoreReader::open(path)
        .map(|store| store.with_query_projection_config(query_projection_config))
}

fn labels(
    store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    values: &[(&str, &str)],
) -> SeriesRef {
    let refs: Vec<_> = values.iter().copied().map(KeyValueRef::from).collect();
    store.intern(&refs).unwrap()
}

fn test_head() -> HeadBuffer {
    HeadBuffer::new(HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap()
}

fn write_series(
    writer: &mut SegmentWriter,
    series: SeriesRef,
    labels: Vec<(String, String)>,
    samples: &[(u64, f64)],
) {
    writer
        .record_samples_with_labels(series, &labels, samples)
        .unwrap();
}

fn assert_limit_exceeded(err: PromqlQueryError, expected_limit: &str, expected_max: u64) {
    match err {
        PromqlQueryError::LimitExceeded { limit, max } => {
            assert_eq!(limit, expected_limit);
            assert_eq!(max, expected_max);
        }
        other => panic!("expected limit exceeded error, got {other:?}"),
    }
}

fn assert_ordinary_non_finite(actual: f64, expected: f64, context: &str) {
    if expected.is_nan() {
        assert!(
            actual.is_nan(),
            "expected ordinary NaN for {context}, got {actual}"
        );
        assert_ne!(
            actual.to_bits(),
            prometheus_stale_nan().to_bits(),
            "ordinary NaN must not become the stale marker for {context}"
        );
    } else {
        assert_eq!(actual, expected, "wrong infinity for {context}");
    }
}

fn sorted_first_sample_values(results: &[SegmentQueryResult]) -> Vec<f64> {
    let mut values = results
        .iter()
        .map(|result| result.samples[0].1)
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    values
}

fn ordered_first_sample_values(results: &[SegmentQueryResult]) -> Vec<f64> {
    results.iter().map(|result| result.samples[0].1).collect()
}

fn ordered_label_values(results: &[SegmentQueryResult], label_name: &str) -> Vec<String> {
    results
        .iter()
        .map(|result| {
            result
                .labels
                .iter()
                .find_map(|(key, value)| (key == label_name).then_some(value.to_owned()))
                .unwrap_or_else(|| panic!("missing label {label_name} in {:?}", result.labels))
        })
        .collect()
}

fn samples_by_label(
    results: &[SegmentQueryResult],
    label_name: &str,
) -> BTreeMap<String, Vec<(u64, f64)>> {
    results
        .iter()
        .map(|result| {
            let label_value = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == label_name).then_some(value.to_owned()))
                .unwrap_or_else(|| panic!("missing label {label_name} in {:?}", result.labels));
            (label_value, result.samples.clone())
        })
        .collect()
}

fn samples_by_route_and_le(
    results: &[SegmentQueryResult],
) -> BTreeMap<(String, String), Vec<(u64, f64)>> {
    results
        .iter()
        .map(|result| {
            let route = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "route").then_some(value.to_owned()))
                .unwrap_or_else(|| panic!("missing route label in {:?}", result.labels));
            let le = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "le").then_some(value.to_owned()))
                .unwrap_or_else(|| panic!("missing le label in {:?}", result.labels));
            ((route, le), result.samples.clone())
        })
        .collect()
}

fn assert_approx_eq(actual: f64, expected: f64, epsilon: f64) {
    assert!(
        (actual - expected).abs() <= epsilon,
        "actual {actual} differs from expected {expected} by more than {epsilon}"
    );
}

fn segment_dir_with_start(root: &Path, start_ms: u64) -> PathBuf {
    let prefix = format!("seg-{start_ms}-");
    fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_dir())
                && entry.file_name().to_string_lossy().starts_with(&prefix)
        })
        .unwrap_or_else(|| panic!("segment starting at {start_ms} not found"))
        .path()
}

fn write_one_pass_scalar_fixture(path: &Path) -> SegmentStoreReader {
    let mut writer =
        SegmentWriter::new(SegmentWriterConfig::new(path, Duration::from_secs(600))).unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(710),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "one_pass_counter".to_string(),
            ),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[
            (0, 0.0),
            (1_000, 10.0),
            (2_000, prometheus_stale_nan()),
            (3_000, 2.0),
            (4_000, 6.0),
            (5_000, 1.0),
            (6_000, 5.0),
            (7_000, 9.0),
            (8_000, 12.0),
            (9_000, 15.0),
        ],
    );
    write_series(
        &mut writer,
        SeriesRef::new(711),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "one_pass_counter".to_string(),
            ),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "b".to_string()),
        ],
        &[
            (0, 0.0),
            (1_000, 2.0),
            (2_000, 4.0),
            (3_000, 6.0),
            (4_000, 8.0),
            (5_000, 10.0),
            (6_000, 12.0),
            (7_000, 14.0),
            (8_000, 16.0),
            (9_000, 18.0),
        ],
    );
    write_series(
        &mut writer,
        SeriesRef::new(712),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "one_pass_counter_count".to_string(),
            ),
            ("route".to_string(), "/api".to_string()),
        ],
        &[(1_000, 1.0), (9_000, 9.0)],
    );
    writer
        .record_i64_samples_ordered_with_label_visitor(
            SeriesRef::new(713),
            &[
                (0, 0),
                (1_000, 3),
                (2_000, 6),
                (3_000, 9),
                (4_000, 12),
                (5_000, 2),
                (6_000, 5),
                (7_000, 8),
                (8_000, 11),
                (9_000, 14),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "one_pass_int_counter");
                visit("route", "/int");
                visit("instance", "i64");
            },
        )
        .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(714),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "one_pass_nonfinite_counter".to_string(),
            ),
            ("route".to_string(), "/nonfinite".to_string()),
        ],
        &[
            (0, 0.0),
            (1_000, f64::NAN),
            (2_000, f64::INFINITY),
            (3_000, f64::NEG_INFINITY),
            (4_000, 4.0),
            (5_000, 5.0),
            (6_000, 6.0),
            (7_000, 7.0),
            (8_000, 8.0),
            (9_000, 9.0),
        ],
    );
    writer.flush().unwrap();
    open_default_store(path)
}

fn assert_scalar_results_bitwise_eq(
    actual: &[SegmentQueryResult],
    expected: &[SegmentQueryResult],
    context: &str,
) {
    assert_eq!(actual.len(), expected.len(), "{context}");
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.series_id, expected.series_id, "{context}");
        assert_eq!(
            actual.labels.to_vec(),
            expected.labels.to_vec(),
            "{context}"
        );
        assert_eq!(actual.samples.len(), expected.samples.len(), "{context}");
        for (actual, expected) in actual.samples.iter().zip(&expected.samples) {
            assert_eq!(actual.0, expected.0, "{context}");
            assert_eq!(actual.1.to_bits(), expected.1.to_bits(), "{context}");
        }
        assert_eq!(
            actual.counter_reset_hints, expected.counter_reset_hints,
            "{context}"
        );
    }
}

fn manifest_precedence_metadata(
    temporality: OtlpAggregationTemporality,
    start_time_ms: Option<u64>,
) -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms,
        temporality,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    }
}

fn manifest_precedence_histogram(count: u64, metadata: TypedSampleMetadata) -> HistogramValue {
    HistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata,
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    }
}

fn manifest_precedence_exponential_histogram(
    count: u64,
    metadata: TypedSampleMetadata,
) -> ExponentialHistogramValue {
    ExponentialHistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata,
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 0,
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

fn write_manifest_precedence_typed_payloads(writer: &mut SegmentWriter, later: bool) {
    let cumulative = manifest_precedence_metadata(OtlpAggregationTemporality::Cumulative, None);
    let delta = |start_time_ms| {
        manifest_precedence_metadata(OtlpAggregationTemporality::Delta, Some(start_time_ms))
    };
    let (first, second, first_metadata, second_metadata) = if later {
        (2, 4, delta(0), delta(1_000))
    } else {
        (10, 20, cumulative, cumulative)
    };

    write_series(
        writer,
        SeriesRef::new(716),
        vec![(
            METRIC_NAME_LABEL.to_string(),
            "manifest_precedence_float".to_string(),
        )],
        &[(1_000, first as f64), (5_000, second as f64)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(717),
            &[
                (1_000, manifest_precedence_histogram(first, first_metadata)),
                (
                    5_000,
                    manifest_precedence_histogram(second, second_metadata),
                ),
            ],
            |visit| visit(METRIC_NAME_LABEL, "manifest_precedence_histogram"),
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(718),
            &[
                (
                    1_000,
                    manifest_precedence_exponential_histogram(first, first_metadata),
                ),
                (
                    5_000,
                    manifest_precedence_exponential_histogram(second, second_metadata),
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "manifest_precedence_exponential_histogram",
                );
            },
        )
        .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(719),
            &[
                (
                    1_000,
                    SummaryValue {
                        count: first,
                        sum: first as f64,
                        metadata: first_metadata,
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: first as f64,
                        }],
                    },
                ),
                (
                    5_000,
                    SummaryValue {
                        count: second,
                        sum: second as f64,
                        metadata: second_metadata,
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: second as f64,
                        }],
                    },
                ),
            ],
            |visit| visit(METRIC_NAME_LABEL, "manifest_precedence_summary"),
        )
        .unwrap();
}

fn assert_manifest_precedence_samples(
    store: &SegmentStoreReader,
    query: &str,
    expected: &[(u64, f64)],
) {
    let results = store.query_promql(query, 0, 5_000).unwrap();
    assert_eq!(results.len(), 1, "{query}");
    assert_eq!(results[0].samples, expected, "{query}");
}

fn one_pass_typed_histogram(count: u64, metadata: TypedSampleMetadata) -> HistogramValue {
    HistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata,
        explicit_bounds: Vec::new(),
        bucket_counts: vec![count],
    }
}

fn one_pass_typed_exponential_histogram(
    count: u64,
    metadata: TypedSampleMetadata,
) -> ExponentialHistogramValue {
    ExponentialHistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        metadata,
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

fn write_one_pass_typed_variants(writer: &mut SegmentWriter) {
    let cumulative = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Cumulative,
        ..TypedSampleMetadata::default()
    };
    let delta = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
        temporality: OtlpAggregationTemporality::Delta,
        ..TypedSampleMetadata::default()
    };

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(721),
            &[
                (1_000, one_pass_typed_exponential_histogram(2, cumulative)),
                (5_000, one_pass_typed_exponential_histogram(4, cumulative)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "one_pass_typed_exponential");
                visit("route", "/typed-exponential");
            },
        )
        .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(722),
            &[
                (
                    1_000,
                    SummaryValue {
                        count: 2,
                        sum: 3.0,
                        metadata: TypedSampleMetadata::default(),
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: 1.5,
                        }],
                    },
                ),
                (
                    5_000,
                    SummaryValue {
                        count: 4,
                        sum: 7.0,
                        metadata: TypedSampleMetadata::default(),
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: 2.0,
                        }],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "one_pass_typed_summary");
                visit("route", "/typed-summary");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(723),
            &[
                (1_000, one_pass_typed_histogram(2, delta(0))),
                (5_000, one_pass_typed_histogram(4, delta(1_000))),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "one_pass_typed_delta");
                visit("route", "/typed-delta");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(724),
            &[
                (1_000, one_pass_typed_histogram(2, cumulative)),
                (5_000, one_pass_typed_histogram(4, delta(1_000))),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "one_pass_typed_mixed_temporality");
                visit("route", "/typed-mixed-temporality");
            },
        )
        .unwrap();
    write_series(
        writer,
        SeriesRef::new(725),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "one_pass_typed_mixed_kind".to_string(),
            ),
            ("route".to_string(), "/mixed-float".to_string()),
        ],
        &[(1_000, 1.0), (5_000, 5.0)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(726),
            &[(1_000, one_pass_typed_histogram(2, cumulative))],
            |visit| {
                visit(METRIC_NAME_LABEL, "one_pass_typed_mixed_kind");
                visit("route", "/mixed-histogram");
            },
        )
        .unwrap();
}

fn assert_delta_histogram_signed_and_non_finite_sum_path(single_interval: bool) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let (path, eval_time_ms, range_secs, expected_increase, expected_rate) = if single_interval {
        ("single", 10_000, 10, 10.0, 1.0)
    } else {
        ("multi", 20_000, 20, 10.0, 0.5)
    };
    let value = |count, sum, metadata| HistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        metadata,
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    };

    for (idx, (kind, non_finite_sum)) in [
        ("finite-negative", -10.0),
        ("nan", f64::NAN),
        ("positive-infinity", f64::INFINITY),
        ("negative-infinity", f64::NEG_INFINITY),
    ]
    .into_iter()
    .enumerate()
    {
        let metadata = |start_time_ms| TypedSampleMetadata {
            start_time_ms: Some(start_time_ms),
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
            ..TypedSampleMetadata::default()
        };
        let samples = if single_interval {
            vec![(10_000, value(10, non_finite_sum, metadata(0)))]
        } else {
            vec![
                (
                    10_000,
                    value(
                        5,
                        if kind == "finite-negative" { 0.0 } else { 5.0 },
                        metadata(0),
                    ),
                ),
                (20_000, value(5, non_finite_sum, metadata(10_000))),
            ]
        };
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(250 + idx as u32),
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, "delta.nonfinite.sum.histogram");
                    visit("kind", kind);
                    visit("path", path);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (kind, expected_sum) in [
        ("finite-negative", -10.0),
        ("nan", f64::NAN),
        ("positive-infinity", f64::INFINITY),
        ("negative-infinity", f64::NEG_INFINITY),
    ] {
        for (function, expected_count) in [("increase", expected_increase), ("rate", expected_rate)]
        {
            let expected_projected_sum = if function == "rate" {
                expected_sum / range_secs as f64
            } else {
                expected_sum
            };
            let selector = format!(r#"kind="{kind}",path="{path}""#);
            let direct_sum_query = format!(
                r#"histogram_sum({function}(delta.nonfinite.sum.histogram{{{selector}}}[{range_secs}s]))"#
            );
            let virtual_sum_query = format!(
                r#"{function}(delta.nonfinite.sum.histogram_sum{{{selector}}}[{range_secs}s])"#
            );
            let direct_count_query = format!(
                r#"histogram_count({function}(delta.nonfinite.sum.histogram{{{selector}}}[{range_secs}s]))"#
            );
            let virtual_count_query = format!(
                r#"{function}(delta.nonfinite.sum.histogram_count{{{selector}}}[{range_secs}s])"#
            );
            let virtual_bucket_query = format!(
                r#"{function}(delta.nonfinite.sum.histogram_bucket{{{selector},le="1"}}[{range_secs}s])"#
            );
            let native_bucket_query = format!(
                r#"histogram_quantile(0.5, {function}(delta.nonfinite.sum.histogram{{{selector}}}[{range_secs}s]))"#
            );
            let results = [
                store
                    .query_promql(&direct_sum_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&virtual_sum_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&direct_count_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&virtual_count_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&virtual_bucket_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&native_bucket_query, 0, eval_time_ms)
                    .unwrap(),
            ];
            assert_eq!(
                results.each_ref().map(Vec::len),
                [1; 6],
                "signed/non-finite sum invalidated a result for {kind}/{path}/{function}"
            );
            if expected_projected_sum.is_finite() {
                assert_eq!(results[0][0].samples[0].1, expected_projected_sum);
                assert_eq!(results[1][0].samples[0].1, expected_projected_sum);
            } else {
                assert_ordinary_non_finite(
                    results[0][0].samples[0].1,
                    expected_projected_sum,
                    &direct_sum_query,
                );
                assert_ordinary_non_finite(
                    results[1][0].samples[0].1,
                    expected_projected_sum,
                    &virtual_sum_query,
                );
            }
            assert_eq!(results[2][0].samples[0].1, expected_count);
            assert_eq!(results[3][0].samples[0].1, expected_count);
            assert_eq!(results[4][0].samples[0].1, expected_count);
            assert!((results[5][0].samples[0].1 - 0.5).abs() < 1e-12);
        }
    }
}

fn assert_delta_exponential_histogram_signed_and_non_finite_sum_path(single_interval: bool) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let (path, eval_time_ms, range_secs, expected_increase, expected_rate) = if single_interval {
        ("single", 10_000, 10, 10.0, 1.0)
    } else {
        ("multi", 20_000, 20, 10.0, 0.5)
    };
    let value = |count, sum, metadata| ExponentialHistogramValue {
        count,
        sum: Some(sum),
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
    };

    for (idx, (kind, non_finite_sum)) in [
        ("finite-negative", -10.0),
        ("nan", f64::NAN),
        ("positive-infinity", f64::INFINITY),
        ("negative-infinity", f64::NEG_INFINITY),
    ]
    .into_iter()
    .enumerate()
    {
        let metadata = |start_time_ms| TypedSampleMetadata {
            start_time_ms: Some(start_time_ms),
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
            ..TypedSampleMetadata::default()
        };
        let samples = if single_interval {
            vec![(10_000, value(10, non_finite_sum, metadata(0)))]
        } else {
            vec![
                (
                    10_000,
                    value(
                        5,
                        if kind == "finite-negative" { 0.0 } else { 5.0 },
                        metadata(0),
                    ),
                ),
                (20_000, value(5, non_finite_sum, metadata(10_000))),
            ]
        };
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(270 + idx as u32),
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, "delta.nonfinite.sum.exphist");
                    visit("kind", kind);
                    visit("path", path);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();
    for (kind, expected_sum) in [
        ("finite-negative", -10.0),
        ("nan", f64::NAN),
        ("positive-infinity", f64::INFINITY),
        ("negative-infinity", f64::NEG_INFINITY),
    ] {
        for (function, expected_count) in [("increase", expected_increase), ("rate", expected_rate)]
        {
            let expected_projected_sum = if function == "rate" {
                expected_sum / range_secs as f64
            } else {
                expected_sum
            };
            let selector = format!(r#"kind="{kind}",path="{path}""#);
            let direct_sum_query = format!(
                r#"histogram_sum({function}(delta.nonfinite.sum.exphist{{{selector}}}[{range_secs}s]))"#
            );
            let virtual_sum_query = format!(
                r#"{function}(delta.nonfinite.sum.exphist_sum{{{selector}}}[{range_secs}s])"#
            );
            let direct_count_query = format!(
                r#"histogram_count({function}(delta.nonfinite.sum.exphist{{{selector}}}[{range_secs}s]))"#
            );
            let virtual_count_query = format!(
                r#"{function}(delta.nonfinite.sum.exphist_count{{{selector}}}[{range_secs}s])"#
            );
            let virtual_bucket_query = format!(
                r#"{function}(delta.nonfinite.sum.exphist_bucket{{{selector},le="2"}}[{range_secs}s])"#
            );
            let native_bucket_query = format!(
                r#"histogram_quantile(0.5, {function}(delta.nonfinite.sum.exphist{{{selector}}}[{range_secs}s]))"#
            );
            let results = [
                store
                    .query_promql(&direct_sum_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&virtual_sum_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&direct_count_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&virtual_count_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&virtual_bucket_query, 0, eval_time_ms)
                    .unwrap(),
                store
                    .query_promql(&native_bucket_query, 0, eval_time_ms)
                    .unwrap(),
            ];
            assert_eq!(
                results.each_ref().map(Vec::len),
                [1; 6],
                "signed/non-finite sum invalidated a result for {kind}/{path}/{function}"
            );
            if expected_projected_sum.is_finite() {
                assert_eq!(results[0][0].samples[0].1, expected_projected_sum);
                assert_eq!(results[1][0].samples[0].1, expected_projected_sum);
            } else {
                assert_ordinary_non_finite(
                    results[0][0].samples[0].1,
                    expected_projected_sum,
                    &direct_sum_query,
                );
                assert_ordinary_non_finite(
                    results[1][0].samples[0].1,
                    expected_projected_sum,
                    &virtual_sum_query,
                );
            }
            assert_eq!(results[2][0].samples[0].1, expected_count);
            assert_eq!(results[3][0].samples[0].1, expected_count);
            assert_eq!(results[4][0].samples[0].1, expected_count);
            assert!((results[5][0].samples[0].1 - 2.0f64.sqrt()).abs() < 1e-12);
        }
    }
}
