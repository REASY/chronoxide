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
use chronoxide_core::storage::segment::{
    QueryLimits, QueryProjectionConfig, SegmentFile, SegmentQueryResult, SegmentStoreReader,
    SegmentWriter, SegmentWriterConfig,
};

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
                .find_map(|(key, value)| (key == label_name).then_some(value.clone()))
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
                .find_map(|(key, value)| (key == label_name).then_some(value.clone()))
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
                .find_map(|(key, value)| (key == "route").then_some(value.clone()))
                .unwrap_or_else(|| panic!("missing route label in {:?}", result.labels));
            let le = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "le").then_some(value.clone()))
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

#[test]
fn promql_query_merges_sealed_segments_and_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("namespace", "default"),
            ("pod.name", "backend-1"),
        ],
    );

    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(5_000, 1.0)])
        .unwrap();
    writer.flush().unwrap();

    let mut head = test_head();
    head.record_sample(series, 15_000, SampleValue::Float(2.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0), (15_000, 2.0)]);
}

#[test]
fn promql_query_reads_sealed_segments_without_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(7);
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(5_000, 1.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu.usage{pod.name="backend-1"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_sum_aggregation_over_sealed_vectors() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(5_000, 1.5)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "b".to_string()),
        ],
        &[(5_000, 2.5)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"sum by (route)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 4.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/api".to_string())]
    );
}

#[test]
fn promql_query_sum_by_metric_name_keeps_name_as_grouping_label() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, value) in [
        (SeriesRef::new(101), "cpu_by_name_usage", 1.0),
        (SeriesRef::new(102), "cpu_by_name_limit", 2.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("route".to_string(), "/by-name".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"sum by (__name__, route)({route="/by-name"})"#, 0, 10_000)
        .unwrap();

    let mut samples_by_labels = BTreeMap::new();
    for result in results {
        samples_by_labels.insert(result.labels.as_ref().to_vec(), result.samples);
    }

    assert_eq!(
        samples_by_labels,
        BTreeMap::from([
            (
                vec![
                    (
                        METRIC_NAME_LABEL.to_string(),
                        "cpu_by_name_limit".to_string()
                    ),
                    ("route".to_string(), "/by-name".to_string()),
                ],
                vec![(10_000, 2.0)]
            ),
            (
                vec![
                    (
                        METRIC_NAME_LABEL.to_string(),
                        "cpu_by_name_usage".to_string()
                    ),
                    ("route".to_string(), "/by-name".to_string()),
                ],
                vec![(10_000, 1.0)]
            ),
        ])
    );
}

#[test]
fn promql_query_count_and_avg_skip_stale_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(10),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("instance".to_string(), "finite".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(11),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("instance".to_string(), "stale".to_string()),
        ],
        &[(5_000, prometheus_stale_nan())],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql(r#"count(cpu.usage)"#, 0, 10_000)
        .unwrap();
    let avg = store.query_promql(r#"avg(cpu.usage)"#, 0, 10_000).unwrap();

    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(10_000, 1.0)]);
    assert_eq!(avg.len(), 1);
    assert_eq!(avg[0].samples, vec![(10_000, 2.0)]);
}

#[test]
fn promql_query_sum_count_and_avg_include_infinite_samples_but_skip_stale() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(101), "finite", 2.0),
        (SeriesRef::new(102), "positive-inf", f64::INFINITY),
        (SeriesRef::new(103), "stale", prometheus_stale_nan()),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/inf-agg".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql(r#"count(cpu.usage{route="/inf-agg"})"#, 0, 10_000)
        .unwrap();
    let sum = store
        .query_promql(r#"sum(cpu.usage{route="/inf-agg"})"#, 0, 10_000)
        .unwrap();
    let avg = store
        .query_promql(r#"avg(cpu.usage{route="/inf-agg"})"#, 0, 10_000)
        .unwrap();

    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(10_000, 2.0)]);
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(10_000, f64::INFINITY)]);
    assert_eq!(avg.len(), 1);
    assert_eq!(avg[0].samples, vec![(10_000, f64::INFINITY)]);
}

#[test]
fn promql_query_avg_large_finite_samples_does_not_overflow_sum() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [
        (SeriesRef::new(118), "first"),
        (SeriesRef::new(119), "second"),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/avg-large".to_string()),
            ],
            &[(5_000, f64::MAX)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let avg = store
        .query_promql(r#"avg(cpu.usage{route="/avg-large"})"#, 0, 10_000)
        .unwrap();

    assert_eq!(avg.len(), 1);
    assert_eq!(avg[0].samples, vec![(10_000, f64::MAX)]);
}

#[test]
fn promql_query_vector_scalar_binary_arithmetic_over_sealed_instant_vector() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(11),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(5_000, 0.42)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(12),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "b".to_string()),
        ],
        &[(5_000, 0.5)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu.usage{route="/api"} * 100"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(sorted_first_sample_values(&results), vec![42.0, 50.0]);
    for result in results {
        assert_eq!(result.samples[0].0, 10_000);
        assert!(
            !result
                .labels
                .iter()
                .any(|(key, _)| key == METRIC_NAME_LABEL),
            "binary arithmetic should drop metric name, got {:?}",
            result.labels
        );
        assert!(
            result
                .labels
                .iter()
                .any(|(key, value)| key == "route" && value == "/api"),
            "binary arithmetic should preserve non-metric labels, got {:?}",
            result.labels
        );
    }
}

#[test]
fn promql_query_vector_scalar_binary_arithmetic_preserves_infinite_samples_but_skips_stale() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(104), "finite", 2.0),
        (SeriesRef::new(105), "positive-inf", f64::INFINITY),
        (SeriesRef::new(106), "stale", prometheus_stale_nan()),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.inf_scalar".to_string()),
                ("route".to_string(), "/inf-binary-scalar".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu.inf_scalar{route="/inf-binary-scalar"} + 1"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(
        samples_by_label(&results, "instance"),
        BTreeMap::from([
            ("finite".to_string(), vec![(10_000, 3.0)]),
            ("positive-inf".to_string(), vec![(10_000, f64::INFINITY)])
        ])
    );
    assert!(results.iter().all(|result| {
        !result
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    }));
}

#[test]
fn promql_query_modulo_and_power_binary_arithmetic_over_sealed_instant_vector() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [(13, "a", 6.0), (14, "b", 9.0)] {
        write_series(
            &mut writer,
            SeriesRef::new(series_ref),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("route".to_string(), "/modpow".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let modulo = store
        .query_promql(r#"cpu.usage{route="/modpow"} % 4"#, 0, 10_000)
        .unwrap();
    let power = store
        .query_promql(r#"cpu.usage{route="/modpow"} ^ 2"#, 0, 10_000)
        .unwrap();
    let scalar_left_modulo = store
        .query_promql(r#"20 % cpu.usage{route="/modpow",instance="a"}"#, 0, 10_000)
        .unwrap();
    let scalar_left_power = store
        .query_promql(r#"2 ^ cpu.usage{route="/modpow",instance="a"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(modulo.len(), 2);
    assert_eq!(sorted_first_sample_values(&modulo), vec![1.0, 2.0]);
    assert_eq!(power.len(), 2);
    assert_eq!(sorted_first_sample_values(&power), vec![36.0, 81.0]);
    assert_eq!(scalar_left_modulo.len(), 1);
    assert_eq!(scalar_left_modulo[0].samples, vec![(10_000, 2.0)]);
    assert_eq!(scalar_left_power.len(), 1);
    assert_eq!(scalar_left_power[0].samples, vec![(10_000, 64.0)]);
    for result in modulo.into_iter().chain(power) {
        assert_eq!(result.samples[0].0, 10_000);
        assert!(
            !result
                .labels
                .iter()
                .any(|(key, _)| key == METRIC_NAME_LABEL),
            "binary arithmetic should drop metric name, got {:?}",
            result.labels
        );
        assert!(
            result
                .labels
                .iter()
                .any(|(key, value)| key == "route" && value == "/modpow"),
            "binary arithmetic should preserve non-metric labels, got {:?}",
            result.labels
        );
    }
}

#[test]
fn promql_query_vector_vector_binary_arithmetic_matches_labels_without_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, instance, value) in [
        (SeriesRef::new(14), "cpu.usage", "a", 10.0),
        (SeriesRef::new(15), "cpu.usage", "b", 20.0),
        (SeriesRef::new(16), "cpu.limit", "a", 4.0),
        (SeriesRef::new(17), "cpu.limit", "c", 8.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("route".to_string(), "/api".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu.usage{route="/api"} / cpu.limit{route="/api"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 2.5)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            ("instance".to_string(), "a".to_string()),
            ("route".to_string(), "/api".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_vector_binary_arithmetic_preserves_infinite_samples_but_skips_stale() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, instance, value) in [
        (SeriesRef::new(107), "cpu.inf_usage", "finite", 2.0),
        (
            SeriesRef::new(108),
            "cpu.inf_usage",
            "positive-inf",
            f64::INFINITY,
        ),
        (
            SeriesRef::new(109),
            "cpu.inf_usage",
            "stale",
            prometheus_stale_nan(),
        ),
        (SeriesRef::new(110), "cpu.inf_limit", "finite", 1.0),
        (SeriesRef::new(111), "cpu.inf_limit", "positive-inf", 1.0),
        (SeriesRef::new(112), "cpu.inf_limit", "stale", 1.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("route".to_string(), "/inf-binary-vector".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu.inf_usage{route="/inf-binary-vector"} + cpu.inf_limit{route="/inf-binary-vector"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(
        samples_by_label(&results, "instance"),
        BTreeMap::from([
            ("finite".to_string(), vec![(10_000, 3.0)]),
            ("positive-inf".to_string(), vec![(10_000, f64::INFINITY)])
        ])
    );
}

#[test]
fn promql_query_vector_vector_binary_arithmetic_matches_ignoring_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, instance, value) in [
        (SeriesRef::new(141), "cpu.usage", "left", 30.0),
        (SeriesRef::new(142), "cpu.limit", "right", 10.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("route".to_string(), "/match-ignoring".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let default = store
        .query_promql(
            r#"cpu.usage{route="/match-ignoring"} / cpu.limit{route="/match-ignoring"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert!(default.is_empty());

    let ignoring = store
        .query_promql(
            r#"cpu.usage{route="/match-ignoring"} / ignoring(instance) cpu.limit{route="/match-ignoring"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(ignoring.len(), 1);
    assert_eq!(ignoring[0].samples, vec![(10_000, 3.0)]);
    assert_eq!(
        ignoring[0].labels.as_ref(),
        &[("route".to_string(), "/match-ignoring".to_string())]
    );
}

#[test]
fn promql_query_binary_expression_uses_evaluated_scalar_function_operand() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, route, instance, samples) in [
        (
            SeriesRef::new(154),
            "/checkout",
            "a",
            vec![
                (0, 0.0),
                (10_000, 10.0),
                (20_000, 20.0),
                (30_000, 30.0),
                (40_000, 40.0),
            ],
        ),
        (
            SeriesRef::new(155),
            "/checkout",
            "b",
            vec![
                (0, 0.0),
                (10_000, 5.0),
                (20_000, 10.0),
                (30_000, 15.0),
                (40_000, 20.0),
            ],
        ),
        (
            SeriesRef::new(156),
            "/search",
            "a",
            vec![
                (0, 0.0),
                (10_000, 2.0),
                (20_000, 4.0),
                (30_000, 6.0),
                (40_000, 8.0),
            ],
        ),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "binary_scalar_requests_total".to_string(),
                ),
                ("job".to_string(), "api".to_string()),
                ("route".to_string(), route.to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &samples,
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"sum by (route)(rate(binary_scalar_requests_total{job="api"}[20s])) / scalar(count(binary_scalar_requests_total{job="api"}))"#,
            0,
            40_000,
        )
        .unwrap();

    let by_route = samples_by_label(&results, "route");
    assert_eq!(by_route.len(), 2);
    assert_approx_eq(by_route["/checkout"][0].1, 0.5, 1e-12);
    assert_approx_eq(by_route["/search"][0].1, 0.06666666666666667, 1e-12);
}

#[test]
fn promql_query_vector_vector_binary_arithmetic_matches_on_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, namespace, instance, value) in [
        (SeriesRef::new(143), "cpu.usage", "left-ns", "a", 40.0),
        (SeriesRef::new(144), "cpu.limit", "right-ns", "b", 8.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("route".to_string(), "/match-on".to_string()),
                ("namespace".to_string(), namespace.to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu.usage{route="/match-on"} / on(route) cpu.limit{route="/match-on"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 5.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/match-on".to_string())]
    );
}

#[test]
fn promql_query_vector_vector_binary_arithmetic_supports_group_left() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, method, code, value) in [
        (SeriesRef::new(145), "http.errors", "get", "500", 24.0),
        (SeriesRef::new(146), "http.errors", "get", "404", 30.0),
        (SeriesRef::new(147), "http.errors", "post", "500", 6.0),
        (SeriesRef::new(148), "http.errors", "post", "404", 21.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("method".to_string(), method.to_string()),
                ("code".to_string(), code.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    for (series_ref, method, value) in [
        (SeriesRef::new(149), "get", 600.0),
        (SeriesRef::new(150), "post", 120.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "http.requests".to_string()),
                ("method".to_string(), method.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"http.errors / ignoring(code) group_left http.requests"#,
            0,
            10_000,
        )
        .unwrap();

    let mut ratios = BTreeMap::new();
    for result in &results {
        assert!(
            !result
                .labels
                .iter()
                .any(|(key, _)| key == METRIC_NAME_LABEL)
        );
        let method = result
            .labels
            .iter()
            .find_map(|(key, value)| (key == "method").then_some(value.clone()))
            .unwrap();
        let code = result
            .labels
            .iter()
            .find_map(|(key, value)| (key == "code").then_some(value.clone()))
            .unwrap();
        ratios.insert((method, code), result.samples.clone());
    }

    assert_eq!(
        ratios,
        BTreeMap::from([
            (("get".to_string(), "404".to_string()), vec![(10_000, 0.05)]),
            (("get".to_string(), "500".to_string()), vec![(10_000, 0.04)]),
            (
                ("post".to_string(), "404".to_string()),
                vec![(10_000, 0.175)]
            ),
            (
                ("post".to_string(), "500".to_string()),
                vec![(10_000, 0.05)]
            ),
        ])
    );
}

#[test]
fn promql_query_vector_vector_binary_arithmetic_supports_group_right_include_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(151),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.limit".to_string()),
            ("route".to_string(), "/group-right".to_string()),
            ("service".to_string(), "api".to_string()),
        ],
        &[(5_000, 10.0)],
    );
    for (series_ref, instance, value) in [
        (SeriesRef::new(152), "a", 2.0),
        (SeriesRef::new(153), "b", 4.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("route".to_string(), "/group-right".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu.limit{route="/group-right"} / on(route) group_right(service) cpu.usage{route="/group-right"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 5.0)]),
            ("b".to_string(), vec![(10_000, 2.5)])
        ])
    );
    for result in results {
        assert!(
            !result
                .labels
                .iter()
                .any(|(key, _)| key == METRIC_NAME_LABEL)
        );
        assert!(
            result
                .labels
                .iter()
                .any(|(key, value)| key == "service" && value == "api"),
            "group_right should include requested one-side labels, got {:?}",
            result.labels
        );
    }
}

#[test]
fn promql_query_unary_minus_negates_sealed_instant_vector() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(13),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(5_000, 0.42)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"-cpu.usage{route="/api"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, -0.42)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL),
        "unary minus should drop metric name, got {:?}",
        results[0].labels
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == "route" && value == "/api"),
        "unary minus should preserve non-metric labels, got {:?}",
        results[0].labels
    );
}

#[test]
fn promql_query_scalar_vector_binary_arithmetic_over_active_head_instant_vector() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("instance", "a"),
            ("route", "/head-binary"),
        ],
    );
    let mut head = test_head();
    head.record_sample(series, 5_000, SampleValue::Float(0.25))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"1 - cpu.usage{route="/head-binary"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 0.75)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            ("instance".to_string(), "a".to_string()),
            ("route".to_string(), "/head-binary".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_vector_binary_arithmetic_merges_sealed_and_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(18),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/mixed-binary".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(5_000, 30.0)],
    );
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let limit_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.limit"),
            ("instance", "a"),
            ("route", "/mixed-binary"),
        ],
    );
    let mut head = test_head();
    head.record_sample(limit_series, 6_000, SampleValue::Float(10.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{route="/mixed-binary"} - cpu.limit{route="/mixed-binary"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 20.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            ("instance".to_string(), "a".to_string()),
            ("route".to_string(), "/mixed-binary".to_string())
        ]
    );
}

#[test]
fn promql_query_session_prefetches_vector_vector_binary_arithmetic_inputs() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, value) in [
        (SeriesRef::new(19), "cpu.usage", 12.0),
        (SeriesRef::new(20), "cpu.limit", 3.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("instance".to_string(), "prefetch-a".to_string()),
                ("route".to_string(), "/prefetch-binary".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let prefetch = session
        .prefetch_promql_data_with_limits(
            r#"cpu.usage{route="/prefetch-binary"} / cpu.limit{route="/prefetch-binary"}"#,
            0,
            10_000,
            QueryLimits::production_default(),
        )
        .unwrap();
    assert_eq!(prefetch.query_stats.segments_queried, 2);
    assert_eq!(prefetch.query_stats.chunk_reads, 2);

    let execution = session
        .query_promql_with_limits(
            r#"cpu.usage{route="/prefetch-binary"} / cpu.limit{route="/prefetch-binary"}"#,
            0,
            10_000,
            QueryLimits::production_default(),
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(10_000, 4.0)]);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[
            ("instance".to_string(), "prefetch-a".to_string()),
            ("route".to_string(), "/prefetch-binary".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_scalar_comparison_filters_instant_vector() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(21), "a", 0.7),
        (SeriesRef::new(22), "b", 0.4),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/compare".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu_usage{route="/compare"} > 0.5"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 0.7)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            ("instance".to_string(), "a".to_string()),
            ("route".to_string(), "/compare".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_vector_comparison_matches_labels_and_keeps_left_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, instance, value) in [
        (SeriesRef::new(23), "cpu_usage", "a", 10.0),
        (SeriesRef::new(24), "cpu_usage", "b", 20.0),
        (SeriesRef::new(25), "cpu_limit", "a", 15.0),
        (SeriesRef::new(26), "cpu_limit", "b", 10.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/compare".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu_usage{route="/compare"} > cpu_limit{route="/compare"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 20.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            ("instance".to_string(), "b".to_string()),
            ("route".to_string(), "/compare".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_vector_comparison_ignoring_keeps_left_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, instance, value) in [
        (SeriesRef::new(37), "cpu_cmp_usage", "left", 30.0),
        (SeriesRef::new(38), "cpu_cmp_limit", "right", 10.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/compare-ignoring".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu_cmp_usage{route="/compare-ignoring"} > ignoring(instance) cpu_cmp_limit{route="/compare-ignoring"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 30.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            (METRIC_NAME_LABEL.to_string(), "cpu_cmp_usage".to_string()),
            ("route".to_string(), "/compare-ignoring".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_vector_comparison_on_name_requires_matching_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, value) in [
        (SeriesRef::new(45), "cpu_on_name_usage", 30.0),
        (SeriesRef::new(46), "cpu_on_name_limit", 10.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("route".to_string(), "/compare-on-name".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu_on_name_usage > on(__name__, route) cpu_on_name_limit"#,
            0,
            10_000,
        )
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_vector_vector_comparison_on_name_drops_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, side, value) in [
        (SeriesRef::new(47), "left", 30.0),
        (SeriesRef::new(48), "right", 10.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "cpu_on_name_compare".to_string(),
                ),
                ("route".to_string(), "/compare-on-name-output".to_string()),
                ("side".to_string(), side.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu_on_name_compare{side="left"} > on(__name__, route) cpu_on_name_compare{side="right"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 30.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/compare-on-name-output".to_string())]
    );
}

#[test]
fn promql_query_vector_vector_comparison_group_left_keeps_left_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, code, value) in [
        (SeriesRef::new(39), "500", 24.0),
        (SeriesRef::new(40), "404", 6.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "http_cmp_errors".to_string()),
                ("code".to_string(), code.to_string()),
                ("method".to_string(), "get".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    write_series(
        &mut writer,
        SeriesRef::new(41),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http_cmp_requests".to_string(),
            ),
            ("method".to_string(), "get".to_string()),
        ],
        &[(5_000, 20.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"http_cmp_errors > ignoring(code) group_left http_cmp_requests"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 24.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            (METRIC_NAME_LABEL.to_string(), "http_cmp_errors".to_string()),
            ("code".to_string(), "500".to_string()),
            ("method".to_string(), "get".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_vector_comparison_group_right_keeps_right_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(42),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu_cmp_limit".to_string()),
            ("route".to_string(), "/compare-group-right".to_string()),
            ("service".to_string(), "api".to_string()),
        ],
        &[(5_000, 10.0)],
    );
    for (series_ref, instance, value) in [
        (SeriesRef::new(43), "a", 2.0),
        (SeriesRef::new(44), "b", 20.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu_cmp_usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/compare-group-right".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu_cmp_limit{route="/compare-group-right"} > on(route) group_right(service) cpu_cmp_usage{route="/compare-group-right"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 10.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            (METRIC_NAME_LABEL.to_string(), "cpu_cmp_usage".to_string()),
            ("instance".to_string(), "a".to_string()),
            ("route".to_string(), "/compare-group-right".to_string()),
            ("service".to_string(), "api".to_string())
        ]
    );
}

#[test]
fn promql_query_vector_scalar_bool_comparison_returns_zero_one_and_drops_metric_name() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(30), "a", 0.7),
        (SeriesRef::new(31), "b", 0.4),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu_bool_usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/bool".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu_bool_usage{route="/bool"} > bool 0.5"#, 0, 10_000)
        .unwrap();

    assert_eq!(
        samples_by_label(&results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 1.0)]),
            ("b".to_string(), vec![(10_000, 0.0)])
        ])
    );
    assert!(results.iter().all(|result| {
        !result
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    }));
}

#[test]
fn promql_query_vector_vector_bool_comparison_matches_and_returns_zero_one() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, instance, value) in [
        (SeriesRef::new(32), "cpu_bool_usage", "a", 10.0),
        (SeriesRef::new(33), "cpu_bool_usage", "b", 20.0),
        (SeriesRef::new(34), "cpu_bool_limit", "a", 15.0),
        (SeriesRef::new(35), "cpu_bool_limit", "b", 10.0),
        (SeriesRef::new(36), "cpu_bool_limit", "c", 1.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/bool".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu_bool_usage{route="/bool"} > bool cpu_bool_limit{route="/bool"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 0.0)]),
            ("b".to_string(), vec![(10_000, 1.0)])
        ])
    );
    assert!(results.iter().all(|result| {
        !result
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    }));
}

#[test]
fn promql_query_vector_vector_set_operators_match_non_metric_labelsets_by_default() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(27), "a", 1.0),
        (SeriesRef::new(28), "b", 2.0),
        (SeriesRef::new(29), "c", 3.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu_set_usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/set".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let and_results = store
        .query_promql(
            r#"cpu_set_usage{route="/set"} and cpu_set_usage{route="/set",instance=~"a|c"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(
        samples_by_label(&and_results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 1.0)]),
            ("c".to_string(), vec![(10_000, 3.0)])
        ])
    );
    assert!(and_results.iter().all(|result| {
        result
            .labels
            .iter()
            .any(|(key, value)| key == METRIC_NAME_LABEL && value == "cpu_set_usage")
    }));

    let or_results = store
        .query_promql(
            r#"cpu_set_usage{route="/set",instance=~"a|b"} or cpu_set_usage{route="/set",instance=~"b|c"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(
        samples_by_label(&or_results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 1.0)]),
            ("b".to_string(), vec![(10_000, 2.0)]),
            ("c".to_string(), vec![(10_000, 3.0)])
        ])
    );

    let unless_results = store
        .query_promql(
            r#"cpu_set_usage{route="/set"} unless cpu_set_usage{route="/set",instance="b"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(
        samples_by_label(&unless_results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 1.0)]),
            ("c".to_string(), vec![(10_000, 3.0)])
        ])
    );
}

#[test]
fn promql_query_vector_vector_set_operators_ignore_metric_name_by_default() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, instance, value) in [
        (SeriesRef::new(241), "cpu_set_left_default", "a", 1.0),
        (SeriesRef::new(242), "cpu_set_left_default", "b", 2.0),
        (SeriesRef::new(243), "cpu_set_right_default", "a", 10.0),
        (SeriesRef::new(244), "cpu_set_right_default", "c", 30.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/set-default".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let rows = |results: &[SegmentQueryResult]| {
        results
            .iter()
            .map(|result| {
                let metric = result
                    .labels
                    .iter()
                    .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.clone()))
                    .unwrap();
                let instance = result
                    .labels
                    .iter()
                    .find_map(|(key, value)| (key == "instance").then_some(value.clone()))
                    .unwrap();
                ((metric, instance), result.samples.clone())
            })
            .collect::<BTreeMap<_, _>>()
    };

    let and_results = store
        .query_promql(
            r#"cpu_set_left_default{route="/set-default"} and cpu_set_right_default{route="/set-default"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(
        rows(&and_results),
        BTreeMap::from([(
            ("cpu_set_left_default".to_string(), "a".to_string()),
            vec![(10_000, 1.0)]
        )])
    );

    let or_results = store
        .query_promql(
            r#"cpu_set_left_default{route="/set-default"} or cpu_set_right_default{route="/set-default"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(
        rows(&or_results),
        BTreeMap::from([
            (
                ("cpu_set_left_default".to_string(), "a".to_string()),
                vec![(10_000, 1.0)]
            ),
            (
                ("cpu_set_left_default".to_string(), "b".to_string()),
                vec![(10_000, 2.0)]
            ),
            (
                ("cpu_set_right_default".to_string(), "c".to_string()),
                vec![(10_000, 30.0)]
            )
        ])
    );

    let unless_results = store
        .query_promql(
            r#"cpu_set_left_default{route="/set-default"} unless cpu_set_right_default{route="/set-default"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(
        rows(&unless_results),
        BTreeMap::from([(
            ("cpu_set_left_default".to_string(), "b".to_string()),
            vec![(10_000, 2.0)]
        )])
    );
}

#[test]
fn promql_query_vector_vector_set_operators_preserve_infinite_samples_but_skip_stale() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(113), "positive-inf", f64::INFINITY),
        (SeriesRef::new(114), "stale", prometheus_stale_nan()),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "cpu_inf_set_usage".to_string(),
                ),
                ("route".to_string(), "/inf-set".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu_inf_set_usage{route="/inf-set"} and cpu_inf_set_usage{route="/inf-set",instance=~"positive-inf|stale"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        samples_by_label(&results, "instance"),
        BTreeMap::from([("positive-inf".to_string(), vec![(10_000, f64::INFINITY)])])
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == METRIC_NAME_LABEL && value == "cpu_inf_set_usage")
    );
}

#[test]
fn promql_query_vector_vector_set_operators_support_on_and_ignoring() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, route, instance, value) in [
        (SeriesRef::new(230), "cpu_set_left", "/set-on", "a", 1.0),
        (SeriesRef::new(231), "cpu_set_left", "/set-on", "b", 2.0),
        (
            SeriesRef::new(232),
            "cpu_set_left",
            "/set-unmatched",
            "c",
            3.0,
        ),
        (SeriesRef::new(233), "cpu_set_right", "/set-on", "x", 10.0),
        (
            SeriesRef::new(234),
            "cpu_set_right",
            "/set-right-only",
            "y",
            20.0,
        ),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), route.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let and_results = store
        .query_promql(r#"cpu_set_left and on(route) cpu_set_right"#, 0, 10_000)
        .unwrap();
    assert_eq!(
        samples_by_label(&and_results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 1.0)]),
            ("b".to_string(), vec![(10_000, 2.0)])
        ])
    );
    assert!(and_results.iter().all(|result| {
        result
            .labels
            .iter()
            .any(|(key, value)| key == METRIC_NAME_LABEL && value == "cpu_set_left")
    }));

    let or_results = store
        .query_promql(r#"cpu_set_left or on(route) cpu_set_right"#, 0, 10_000)
        .unwrap();
    assert_eq!(
        samples_by_label(&or_results, "instance"),
        BTreeMap::from([
            ("a".to_string(), vec![(10_000, 1.0)]),
            ("b".to_string(), vec![(10_000, 2.0)]),
            ("c".to_string(), vec![(10_000, 3.0)]),
            ("y".to_string(), vec![(10_000, 20.0)])
        ])
    );
    assert!(or_results.iter().any(|result| {
        result
            .labels
            .iter()
            .any(|(key, value)| key == METRIC_NAME_LABEL && value == "cpu_set_right")
            && result
                .labels
                .iter()
                .any(|(key, value)| key == "route" && value == "/set-right-only")
    }));

    let unless_results = store
        .query_promql(
            r#"cpu_set_left unless ignoring(instance) cpu_set_right"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(
        samples_by_label(&unless_results, "route"),
        BTreeMap::from([("/set-unmatched".to_string(), vec![(10_000, 3.0)])])
    );
}

#[test]
fn promql_query_min_and_max_skip_stale_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(13), "low", 1.5),
        (SeriesRef::new(14), "high", 4.0),
        (SeriesRef::new(15), "stale", prometheus_stale_nan()),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/minmax".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let min = store
        .query_promql(r#"min by (route)(cpu.usage{route="/minmax"})"#, 0, 10_000)
        .unwrap();
    let max = store
        .query_promql(
            r#"max without (instance)(cpu.usage{route="/minmax"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(min.len(), 1);
    assert_eq!(min[0].samples, vec![(10_000, 1.5)]);
    assert_eq!(
        min[0].labels.as_ref(),
        &[("route".to_string(), "/minmax".to_string())]
    );
    assert_eq!(max.len(), 1);
    assert_eq!(max[0].samples, vec![(10_000, 4.0)]);
    assert_eq!(
        max[0].labels.as_ref(),
        &[("route".to_string(), "/minmax".to_string())]
    );
}

#[test]
fn promql_query_stddev_stdvar_and_group_skip_stale_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, route, value) in [
        (SeriesRef::new(16), "a", "/stats", 2.0),
        (SeriesRef::new(17), "b", "/stats", 4.0),
        (SeriesRef::new(18), "c", "/stats", 6.0),
        (
            SeriesRef::new(19),
            "stale",
            "/stale-only",
            prometheus_stale_nan(),
        ),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), route.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let stdvar = store
        .query_promql(r#"stdvar by (route)(cpu.usage)"#, 0, 10_000)
        .unwrap();
    let stddev = store
        .query_promql(r#"stddev by (route)(cpu.usage)"#, 0, 10_000)
        .unwrap();
    let group = store
        .query_promql(r#"group without (instance)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(stdvar.len(), 1);
    assert_eq!(
        stdvar[0].labels.as_ref(),
        &[("route".to_string(), "/stats".to_string())]
    );
    assert!((stdvar[0].samples[0].1 - (8.0 / 3.0)).abs() < 1e-12);

    assert_eq!(stddev.len(), 1);
    assert_eq!(
        stddev[0].labels.as_ref(),
        &[("route".to_string(), "/stats".to_string())]
    );
    assert!((stddev[0].samples[0].1 - (8.0_f64 / 3.0).sqrt()).abs() < 1e-12);

    assert_eq!(group.len(), 1);
    assert_eq!(
        group[0].labels.as_ref(),
        &[("route".to_string(), "/stats".to_string())]
    );
    assert_eq!(group[0].samples, vec![(10_000, 1.0)]);
}

#[test]
fn promql_query_topk_and_bottomk_skip_stale_and_preserve_selected_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, route, value) in [
        (SeriesRef::new(20), "api-a", "/api", 1.0),
        (SeriesRef::new(21), "api-b", "/api", 5.0),
        (SeriesRef::new(22), "api-c", "/api", 3.0),
        (
            SeriesRef::new(23),
            "api-stale",
            "/api",
            prometheus_stale_nan(),
        ),
        (SeriesRef::new(24), "admin-a", "/admin", 4.0),
        (SeriesRef::new(25), "admin-b", "/admin", 2.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), route.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let top = store
        .query_promql(r#"topk by (route)(1 + 1, cpu.usage)"#, 0, 10_000)
        .unwrap();
    let bottom = store
        .query_promql(r#"bottomk(2, cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(
        samples_by_label(&top, "instance"),
        BTreeMap::from([
            ("admin-a".to_string(), vec![(10_000, 4.0)]),
            ("admin-b".to_string(), vec![(10_000, 2.0)]),
            ("api-b".to_string(), vec![(10_000, 5.0)]),
            ("api-c".to_string(), vec![(10_000, 3.0)]),
        ])
    );
    for result in &top {
        assert!(
            result
                .labels
                .iter()
                .any(|(key, value)| key == METRIC_NAME_LABEL
                    && value == &normalize_metric_name("cpu.usage")),
            "topk should preserve selected input labels: {:?}",
            result.labels
        );
    }

    assert_eq!(
        samples_by_label(&bottom, "instance"),
        BTreeMap::from([
            ("admin-b".to_string(), vec![(10_000, 2.0)]),
            ("api-a".to_string(), vec![(10_000, 1.0)]),
        ])
    );
}

#[test]
fn promql_query_topk_and_bottomk_rank_ieee_nan_after_finite_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(118), "finite-low", 1.0),
        (SeriesRef::new(119), "finite-high", 5.0),
        (SeriesRef::new(120), "positive-nan", f64::NAN),
        (
            SeriesRef::new(121),
            "negative-nan",
            f64::from_bits(0xfff8_0000_0000_0001),
        ),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.nan.rank".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/nan-rank".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let top = store
        .query_promql(r#"topk(2, cpu.nan.rank{route="/nan-rank"})"#, 0, 10_000)
        .unwrap();
    let bottom = store
        .query_promql(r#"bottomk(2, cpu.nan.rank{route="/nan-rank"})"#, 0, 10_000)
        .unwrap();

    assert_eq!(
        samples_by_label(&top, "instance"),
        BTreeMap::from([
            ("finite-high".to_string(), vec![(10_000, 5.0)]),
            ("finite-low".to_string(), vec![(10_000, 1.0)]),
        ])
    );
    assert_eq!(
        samples_by_label(&bottom, "instance"),
        BTreeMap::from([
            ("finite-high".to_string(), vec![(10_000, 5.0)]),
            ("finite-low".to_string(), vec![(10_000, 1.0)]),
        ])
    );
}

#[test]
fn promql_query_quantile_interpolates_grouped_finite_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, route, value) in [
        (SeriesRef::new(30), "api-a", "/api", 1.0),
        (SeriesRef::new(31), "api-b", "/api", 3.0),
        (SeriesRef::new(32), "api-c", "/api", 5.0),
        (
            SeriesRef::new(33),
            "api-stale",
            "/api",
            prometheus_stale_nan(),
        ),
        (SeriesRef::new(34), "admin-a", "/admin", 2.0),
        (SeriesRef::new(35), "admin-b", "/admin", 10.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), route.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let median_by_route = store
        .query_promql(r#"quantile by (route)(0.5, cpu.usage)"#, 0, 10_000)
        .unwrap();
    let api_quarter = store
        .query_promql(r#"quantile(1 / 4, cpu.usage{route="/api"})"#, 0, 10_000)
        .unwrap();

    assert_eq!(
        samples_by_label(&median_by_route, "route"),
        BTreeMap::from([
            ("/admin".to_string(), vec![(10_000, 6.0)]),
            ("/api".to_string(), vec![(10_000, 3.0)]),
        ])
    );
    for result in &median_by_route {
        assert!(
            result.labels.iter().all(|(key, _)| key == "route"),
            "quantile grouping should keep only grouping labels: {:?}",
            result.labels
        );
    }

    assert_eq!(api_quarter.len(), 1);
    assert_eq!(api_quarter[0].labels.as_ref(), &[]);
    assert_eq!(api_quarter[0].samples, vec![(10_000, 2.0)]);
}

#[test]
fn promql_query_quantile_orders_ieee_nan_before_finite_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(122), "finite-low", 1.0),
        (SeriesRef::new(123), "finite-high", 3.0),
        (SeriesRef::new(124), "nan", f64::NAN),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "cpu.nan.quantile".to_string(),
                ),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/nan-quantile".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let minimum = store
        .query_promql(
            r#"quantile by (route)(0, cpu.nan.quantile{route="/nan-quantile"})"#,
            0,
            10_000,
        )
        .unwrap();
    let maximum = store
        .query_promql(
            r#"quantile by (route)(1, cpu.nan.quantile{route="/nan-quantile"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(minimum.len(), 1);
    assert_eq!(
        minimum[0].labels.as_ref(),
        &[("route".to_string(), "/nan-quantile".to_string())]
    );
    assert!(minimum[0].samples[0].1.is_nan());

    assert_eq!(maximum.len(), 1);
    assert_eq!(
        maximum[0].labels.as_ref(),
        &[("route".to_string(), "/nan-quantile".to_string())]
    );
    assert_eq!(maximum[0].samples, vec![(10_000, 3.0)]);
}

#[test]
fn promql_query_count_values_counts_equal_sample_values_per_group() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, route, value) in [
        (SeriesRef::new(36), "api-a", "/api", 1.0),
        (SeriesRef::new(37), "api-b", "/api", 1.0),
        (SeriesRef::new(38), "api-c", "/api", 2.5),
        (
            SeriesRef::new(39),
            "api-stale",
            "/api",
            prometheus_stale_nan(),
        ),
        (SeriesRef::new(40), "admin-a", "/admin", 1.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), route.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"count_values by (route)("value", cpu.usage)"#, 0, 10_000)
        .unwrap();

    let mut samples_by_labels = BTreeMap::new();
    for result in results {
        assert!(
            !result
                .labels
                .iter()
                .any(|(key, _)| key == METRIC_NAME_LABEL || key == "instance"),
            "count_values should drop metric name and non-grouping labels: {:?}",
            result.labels
        );
        samples_by_labels.insert(result.labels.as_ref().to_vec(), result.samples);
    }

    assert_eq!(
        samples_by_labels,
        BTreeMap::from([
            (
                vec![
                    ("route".to_string(), "/admin".to_string()),
                    ("value".to_string(), "1".to_string()),
                ],
                vec![(10_000, 1.0)]
            ),
            (
                vec![
                    ("route".to_string(), "/api".to_string()),
                    ("value".to_string(), "1".to_string()),
                ],
                vec![(10_000, 2.0)]
            ),
            (
                vec![
                    ("route".to_string(), "/api".to_string()),
                    ("value".to_string(), "2.5".to_string()),
                ],
                vec![(10_000, 1.0)]
            ),
        ])
    );
}

#[test]
fn promql_query_count_values_normalizes_otlp_style_output_label() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(123), "api-a", 1.0),
        (SeriesRef::new(124), "api-b", 1.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/count-values-normalize".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"count_values by (route)("value.name", cpu.usage{route="/count-values-normalize"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            ("route".to_string(), "/count-values-normalize".to_string()),
            (normalize_label_name("value.name"), "1".to_string())
        ]
    );
    assert_eq!(results[0].samples, vec![(10_000, 2.0)]);
}

#[test]
fn promql_query_count_values_counts_infinite_samples_but_skips_stale() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(115), "finite", 1.0),
        (SeriesRef::new(116), "positive-inf", f64::INFINITY),
        (SeriesRef::new(117), "stale", prometheus_stale_nan()),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/count-values-inf".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"count_values by (route)("value", cpu.usage{route="/count-values-inf"})"#,
            0,
            10_000,
        )
        .unwrap();

    let mut samples_by_labels = BTreeMap::new();
    for result in results {
        assert!(
            !result
                .labels
                .iter()
                .any(|(key, _)| key == METRIC_NAME_LABEL || key == "instance"),
            "count_values should drop metric name and non-grouping labels: {:?}",
            result.labels
        );
        samples_by_labels.insert(result.labels.as_ref().to_vec(), result.samples);
    }

    assert_eq!(
        samples_by_labels,
        BTreeMap::from([
            (
                vec![
                    ("route".to_string(), "/count-values-inf".to_string()),
                    ("value".to_string(), "+Inf".to_string()),
                ],
                vec![(10_000, 1.0)]
            ),
            (
                vec![
                    ("route".to_string(), "/count-values-inf".to_string()),
                    ("value".to_string(), "1".to_string()),
                ],
                vec![(10_000, 1.0)]
            ),
        ])
    );
}

#[test]
fn promql_query_count_values_uses_promql_float_label_spelling() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, value) in [
        (SeriesRef::new(118), "large", 1_000_000.0),
        (SeriesRef::new(119), "small", 0.00001),
        (SeriesRef::new(120), "negative-zero", -0.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("instance".to_string(), instance.to_string()),
                ("route".to_string(), "/count-values-format".to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"count_values by (route)("value", cpu.usage{route="/count-values-format"})"#,
            0,
            10_000,
        )
        .unwrap();

    let mut samples_by_value = BTreeMap::new();
    for result in results {
        let value = result
            .labels
            .iter()
            .find_map(|(key, value)| (key == "value").then_some(value.clone()))
            .unwrap_or_else(|| panic!("missing value label in {:?}", result.labels));
        samples_by_value.insert(value, result.samples);
    }

    assert_eq!(
        samples_by_value,
        BTreeMap::from([
            ("-0".to_string(), vec![(10_000, 1.0)]),
            ("1e+06".to_string(), vec![(10_000, 1.0)]),
            ("1e-05".to_string(), vec![(10_000, 1.0)]),
        ])
    );
}

#[test]
fn promql_query_aggregation_treats_latest_stale_sample_as_absent() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(12),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("instance".to_string(), "stale-latest".to_string()),
            ("route".to_string(), "/stale-agg".to_string()),
        ],
        &[(4_000, 2.0), (5_000, prometheus_stale_nan())],
    );
    write_series(
        &mut writer,
        SeriesRef::new(13),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("instance".to_string(), "finite-latest".to_string()),
            ("route".to_string(), "/stale-agg".to_string()),
        ],
        &[(5_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"sum by (route)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/stale-agg".to_string())]
    );
}

#[test]
fn promql_query_absent_returns_one_with_unique_equality_labels_when_selector_is_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let results = store
        .query_promql(
            r#"absent(http.requests.total{job="api",instance=~".*",route!="admin"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("job".to_string(), "api".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
}

#[test]
fn promql_query_absent_normalizes_otlp_style_dotted_result_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let results = store
        .query_promql(
            r#"absent(cpu.usage{pod.name="backend-1",instance=~".*"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[(normalize_label_name("pod.name"), "backend-1".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
}

#[test]
fn promql_query_absent_returns_empty_when_selector_has_present_sample() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(18),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.requests.total".to_string(),
            ),
            ("job".to_string(), "api".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"absent(http.requests.total{job="api"})"#, 0, 10_000)
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_absent_treats_infinite_samples_as_present() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(121),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.requests.total".to_string(),
            ),
            ("job".to_string(), "api".to_string()),
        ],
        &[(8_000, f64::INFINITY)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let instant = store
        .query_promql(r#"absent(http.requests.total{job="api"})"#, 0, 10_000)
        .unwrap();
    let over_time = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert!(instant.is_empty());
    assert!(over_time.is_empty());
}

#[test]
fn promql_query_absent_over_non_selector_expression_uses_empty_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let results = store
        .query_promql(r#"absent(sum(http.requests.total{job="api"}))"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].labels.is_empty());
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
}

#[test]
fn promql_query_absent_over_time_returns_one_with_unique_equality_labels_when_range_is_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let results = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api",instance=~".*",route!="admin"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("job".to_string(), "api".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
}

#[test]
fn promql_query_absent_over_time_returns_empty_when_range_has_present_sample() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(19),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.requests.total".to_string(),
            ),
            ("job".to_string(), "api".to_string()),
        ],
        &[(8_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_absent_over_time_excludes_left_boundary_sample() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(122),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.requests.total".to_string(),
            ),
            ("job".to_string(), "api".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("job".to_string(), "api".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
}

#[test]
fn promql_query_absent_over_time_treats_stale_marker_only_range_as_absent() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(20),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.requests.total".to_string(),
            ),
            ("job".to_string(), "api".to_string()),
        ],
        &[(8_000, prometheus_stale_nan())],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("job".to_string(), "api".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
}

#[test]
fn promql_query_aggregation_uses_instant_lookback_for_vector_input() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(14),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("instance".to_string(), "old".to_string()),
            ("route".to_string(), "/lookback".to_string()),
        ],
        &[(50_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(15),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("instance".to_string(), "recent".to_string()),
            ("route".to_string(), "/lookback".to_string()),
        ],
        &[(250_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"sum by (route)(cpu.usage)"#, 0, 400_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(400_000, 3.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/lookback".to_string())]
    );
}

#[test]
fn promql_query_sum_without_drops_named_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (idx, instance, value) in [(1, "a", 1.0), (2, "b", 2.0)] {
        write_series(
            &mut writer,
            SeriesRef::new(idx),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("route".to_string(), "/api".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"sum without (instance)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/api".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
}

#[test]
fn promql_query_sum_aggregation_over_active_head_vectors() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let first = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("instance", "a"),
            ("route", "/head-agg"),
        ],
    );
    let second = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("instance", "b"),
            ("route", "/head-agg"),
        ],
    );
    let mut head = test_head();
    head.record_sample(first, 5_000, SampleValue::Float(1.25))
        .unwrap();
    head.record_sample(second, 5_000, SampleValue::Float(2.75))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"sum by (route)(cpu.usage)"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 4.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/head-agg".to_string())]
    );
}

#[test]
fn promql_query_sum_aggregation_merges_sealed_and_active_head_vectors() {
    let tempdir = tempfile::tempdir().unwrap();
    let sealed_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("instance".to_string(), "sealed".to_string()),
        ("route".to_string(), "/head-sealed-agg".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(21), &sealed_labels, &[(5_000, 1.25)])
        .unwrap();
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let head_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("instance", "head"),
            ("route", "/head-sealed-agg"),
        ],
    );
    let mut head = test_head();
    head.record_sample(head_series, 15_000, SampleValue::Float(2.75))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"sum by (route)(cpu.usage)"#,
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(20_000, 4.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/head-sealed-agg".to_string())]
    );
}

#[test]
fn promql_query_sort_orders_instant_vector_values() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for (series, instance, value) in [
        (SeriesRef::new(61), "a", 5.0),
        (SeriesRef::new(62), "b", 1.0),
        (SeriesRef::new(63), "c", 3.0),
    ] {
        writer
            .record_samples_with_labels(
                series,
                &[
                    (METRIC_NAME_LABEL.to_string(), "cpu.load".to_string()),
                    ("instance".to_string(), instance.to_string()),
                ],
                &[(10_000, value)],
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let ascending = store.query_promql("sort(cpu.load)", 0, 10_000).unwrap();
    let descending = store
        .query_promql("sort_desc(cpu.load)", 0, 10_000)
        .unwrap();

    assert_eq!(
        ordered_label_values(&ascending, "instance"),
        vec!["b".to_string(), "c".to_string(), "a".to_string()]
    );
    assert_eq!(
        ordered_first_sample_values(&ascending),
        vec![1.0, 3.0, 5.0],
        "sort should order by ascending sample value"
    );
    assert_eq!(
        ordered_label_values(&descending, "instance"),
        vec!["a".to_string(), "c".to_string(), "b".to_string()]
    );
    assert_eq!(
        ordered_first_sample_values(&descending),
        vec![5.0, 3.0, 1.0],
        "sort_desc should order by descending sample value"
    );
    assert!(
        ascending.iter().all(|result| {
            result.labels.iter().any(|(key, value)| {
                key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.load")
            })
        }),
        "sort should preserve metric names"
    );
}

#[test]
fn promql_query_sort_desc_orders_ieee_nan_after_finite_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for (series, instance, value) in [
        (SeriesRef::new(64), "finite", 2.0),
        (SeriesRef::new(65), "nan", f64::NAN),
        (SeriesRef::new(66), "larger", 4.0),
    ] {
        writer
            .record_samples_with_labels(
                series,
                &[
                    (METRIC_NAME_LABEL.to_string(), "cpu.nan.sort".to_string()),
                    ("instance".to_string(), instance.to_string()),
                ],
                &[(10_000, value)],
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql("sort_desc(cpu.nan.sort)", 0, 10_000)
        .unwrap();

    assert_eq!(
        ordered_label_values(&results, "instance"),
        vec![
            "larger".to_string(),
            "finite".to_string(),
            "nan".to_string()
        ]
    );
    assert!(ordered_first_sample_values(&results)[2].is_nan());
}

#[test]
fn promql_query_offset_shifts_instant_vector_lookup() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.offset".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(1_000, 1.0), (301_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let shifted = store
        .query_promql(r#"cpu.offset offset 5m"#, 0, 600_000)
        .unwrap();

    assert_eq!(shifted.len(), 1);
    assert_eq!(shifted[0].samples, vec![(600_000, 1.0)]);
    assert_eq!(
        shifted[0].labels.as_ref(),
        &[
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.offset"),
            ),
            ("instance".to_string(), "a".to_string()),
        ]
    );
}

#[test]
fn promql_query_offset_shifts_range_function_window() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.counter".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[
            (0, 0.0),
            (60_000, 60.0),
            (120_000, 120.0),
            (360_000, 1_000.0),
        ],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let shifted = store
        .query_promql(r#"increase(cpu.counter[2m] offset 5m)"#, 0, 420_000)
        .unwrap();

    assert_eq!(shifted.len(), 1);
    assert_eq!(shifted[0].samples, vec![(420_000, 120.0)]);
    assert_eq!(
        shifted[0].labels.as_ref(),
        &[("instance".to_string(), "a".to_string())]
    );
}

#[test]
fn promql_query_time_and_vector_evaluate_at_query_end() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let time = store.query_promql("time()", 0, 1_234_000).unwrap();
    let vector = store.query_promql("vector(time())", 0, 1_234_000).unwrap();

    assert_eq!(time.len(), 1);
    assert_eq!(time[0].labels.as_ref(), &[]);
    assert_eq!(time[0].samples, vec![(1_234_000, 1_234.0)]);
    assert_eq!(vector, time);
}

#[test]
fn promql_query_math_log_and_calendar_functions_over_vectors() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let cases = [
        ("abs(vector(-2.5))", 2.5),
        ("ceil(vector(2.1))", 3.0),
        ("floor(vector(2.9))", 2.0),
        ("round(vector(2.6))", 3.0),
        ("round(vector(2.75), 0.5)", 3.0),
        ("clamp(vector(7), 0, 5)", 5.0),
        ("clamp_min(vector(3), 5)", 5.0),
        ("clamp_max(vector(7), 5)", 5.0),
        ("ln(vector(1))", 0.0),
        ("log2(vector(8))", 3.0),
        ("log10(vector(100))", 2.0),
        ("minute(vector(90))", 1.0),
        ("hour(vector(7200))", 2.0),
        ("day_of_month(vector(0))", 1.0),
        ("day_of_week(vector(0))", 4.0),
        ("day_of_year(vector(0))", 1.0),
        ("days_in_month(vector(0))", 31.0),
        ("month(vector(0))", 1.0),
        ("year(vector(0))", 1970.0),
    ];

    for (query, expected) in cases {
        let results = store.query_promql(query, 0, 10_000).unwrap();
        assert_eq!(results.len(), 1, "query {query}");
        assert_eq!(results[0].labels.as_ref(), &[], "query {query}");
        assert_eq!(
            results[0].samples,
            vec![(10_000, expected)],
            "query {query}"
        );
    }
}

#[test]
fn promql_query_scalar_sgn_and_trigonometric_functions() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "scalar.source".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(10_000, 3.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "scalar.source".to_string()),
            ("instance".to_string(), "b".to_string()),
        ],
        &[(10_000, 4.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let cases = [
        ("scalar(scalar.source{instance=\"a\"})", 3.0),
        ("sgn(vector(-4))", -1.0),
        ("sgn(vector(0))", 0.0),
        ("sgn(vector(5))", 1.0),
        ("sin(vector(0))", 0.0),
        ("cos(vector(0))", 1.0),
        ("tan(vector(0))", 0.0),
        ("asin(vector(1))", std::f64::consts::FRAC_PI_2),
        ("acos(vector(1))", 0.0),
        ("atan(vector(1))", std::f64::consts::FRAC_PI_4),
        ("sinh(vector(0))", 0.0),
        ("cosh(vector(0))", 1.0),
        ("tanh(vector(0))", 0.0),
        ("asinh(vector(0))", 0.0),
        ("acosh(vector(1))", 0.0),
        ("atanh(vector(0))", 0.0),
        ("deg(vector(pi()))", 180.0),
        ("rad(vector(180))", std::f64::consts::PI),
        ("pi()", std::f64::consts::PI),
    ];

    for (query, expected) in cases {
        let results = store.query_promql(query, 0, 20_000).unwrap();
        assert_eq!(results.len(), 1, "query {query}");
        assert_eq!(results[0].labels.as_ref(), &[], "query {query}");
        assert_approx_eq(results[0].samples[0].1, expected, 1e-12);
    }

    let multi = store
        .query_promql("scalar(scalar.source)", 0, 20_000)
        .unwrap();
    assert_eq!(multi.len(), 1);
    assert!(multi[0].samples[0].1.is_nan());

    let empty = store
        .query_promql("scalar(scalar.missing)", 0, 20_000)
        .unwrap();
    assert_eq!(empty.len(), 1);
    assert!(empty[0].samples[0].1.is_nan());
}

#[test]
fn promql_query_timestamp_returns_source_sample_timestamp_seconds() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.timestamp".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(10_000, 1.0), (20_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql("timestamp(cpu.timestamp)", 0, 25_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(25_000, 20.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("instance".to_string(), "a".to_string())]
    );
}

#[test]
fn promql_query_label_replace_sets_destination_label_from_capture() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "http.requests".to_string()),
            ("job".to_string(), "api-v1".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(10_000, 7.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"label_replace(http.requests, "service", "$1", "job", "(.+)-v[0-9]+")"#,
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(20_000, 7.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("http.requests"),
            ),
            ("instance".to_string(), "a".to_string()),
            ("job".to_string(), "api-v1".to_string()),
            ("service".to_string(), "api".to_string()),
        ]
    );
}

#[test]
fn promql_query_label_join_concatenates_source_label_values() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "up".to_string()),
            ("job".to_string(), "api".to_string()),
            ("instance".to_string(), "a:9090".to_string()),
        ],
        &[(10_000, 1.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"label_join(up, "target", "/", "job", "instance", "missing")"#,
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(20_000, 1.0)]);
    assert_eq!(
        results[0].labels.as_ref(),
        &[
            (METRIC_NAME_LABEL.to_string(), "up".to_string()),
            ("instance".to_string(), "a:9090".to_string()),
            ("job".to_string(), "api".to_string()),
            ("target".to_string(), "api/a:9090/".to_string()),
        ]
    );
}

#[test]
fn promql_query_range_evaluates_expression_at_each_step() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let results = store
        .query_promql_range("time() + 1", 1_000, 5_000, 2_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].labels.as_ref(), &[]);
    assert_eq!(
        results[0].samples,
        vec![(1_000, 2.0), (3_000, 4.0), (5_000, 6.0)]
    );
}

#[test]
fn promql_query_range_covers_stored_selectors_offsets_functions_and_session() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "range.cpu".to_string()),
            ("job".to_string(), "api-v1".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let selector = store
        .query_promql_range(r#"range.cpu{instance="a"}"#, 1_000, 5_000, 2_000)
        .unwrap();
    assert_eq!(selector.len(), 1);
    assert_eq!(
        selector[0].samples,
        vec![(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)]
    );

    let offset = store
        .query_promql_range(r#"range.cpu{instance="a"} offset 2s"#, 3_000, 7_000, 2_000)
        .unwrap();
    assert_eq!(offset.len(), 1);
    assert_eq!(
        offset[0].samples,
        vec![(3_000, 1.0), (5_000, 3.0), (7_000, 5.0)]
    );

    let sum = store
        .query_promql_range(
            r#"sum_over_time(range.cpu{instance="a"}[3s])"#,
            3_000,
            5_000,
            2_000,
        )
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(3_000, 4.0), (5_000, 8.0)]);
    assert_eq!(
        sum[0].labels.as_ref(),
        &[
            ("instance".to_string(), "a".to_string()),
            ("job".to_string(), "api-v1".to_string())
        ]
    );

    let labels = store
        .query_promql_range(
            r#"label_replace(label_join(range.cpu{instance="a"}, "target", "/", "job", "instance"), "service", "$1", "job", "(.+)-v[0-9]+")"#,
            1_000,
            5_000,
            2_000,
        )
        .unwrap();
    assert_eq!(labels.len(), 1);
    assert_eq!(
        labels[0].samples,
        vec![(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)]
    );
    assert!(
        labels[0]
            .labels
            .iter()
            .any(|(key, value)| key == "service" && value == "api")
    );
    assert!(
        labels[0]
            .labels
            .iter()
            .any(|(key, value)| key == "target" && value == "api-v1/a")
    );

    let sgn = store
        .query_promql_range(r#"sgn(range.cpu{instance="a"} - 3)"#, 1_000, 5_000, 2_000)
        .unwrap();
    assert_eq!(sgn.len(), 1);
    assert_eq!(
        sgn[0].samples,
        vec![(1_000, -1.0), (3_000, 0.0), (5_000, 1.0)]
    );

    let scalar = store
        .query_promql_range(r#"scalar(range.cpu{instance="a"})"#, 1_000, 5_000, 2_000)
        .unwrap();
    assert_eq!(scalar.len(), 1);
    assert_eq!(scalar[0].labels.as_ref(), &[]);
    assert_eq!(
        scalar[0].samples,
        vec![(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)]
    );

    let mut session = store.query_session().unwrap();
    let session_sum = session
        .query_promql_range(
            r#"sum_over_time(range.cpu{instance="a"}[3s])"#,
            3_000,
            5_000,
            2_000,
        )
        .unwrap();
    assert_eq!(session_sum, sum);
}

#[test]
fn promql_query_range_with_head_covers_selectors_and_range_functions() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "range.head.cpu"),
            ("instance", "a"),
            ("job", "api"),
        ],
    );
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    write_series(
        &mut writer,
        series,
        vec![
            (METRIC_NAME_LABEL.to_string(), "range.head.cpu".to_string()),
            ("job".to_string(), "api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(1_000, 1.0)],
    );
    writer.flush().unwrap();

    let mut head = test_head();
    head.record_sample(series, 3_000, SampleValue::Float(3.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let selector = store
        .query_promql_range_with_head(
            &head,
            &label_store,
            r#"range.head.cpu{instance="a"}"#,
            1_000,
            3_000,
            2_000,
        )
        .unwrap();
    assert_eq!(selector.len(), 1);
    assert_eq!(selector[0].samples, vec![(1_000, 1.0), (3_000, 3.0)]);

    let sum = store
        .query_promql_range_with_head(
            &head,
            &label_store,
            r#"sum_over_time(range.head.cpu{instance="a"}[3s])"#,
            3_000,
            3_000,
            1_000,
        )
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(3_000, 4.0)]);
}

#[test]
fn promql_query_range_projects_histogram_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(91),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 5,
                        sum: Some(9.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![3, 2],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 7,
                        sum: Some(13.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![4, 3],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "range.request.duration");
                visit("route", "/range");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql_range(
            r#"range.request.duration_count{route="/range"}"#,
            1_000,
            3_000,
            2_000,
        )
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(1_000, 5.0), (3_000, 7.0)]);

    let bucket = store
        .query_promql_range(
            r#"range.request.duration_bucket{route="/range",le="1"}"#,
            1_000,
            3_000,
            2_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(1_000, 3.0), (3_000, 4.0)]);
    assert!(
        bucket[0]
            .labels
            .iter()
            .any(|(key, value)| key == "le" && value == "1")
    );
}

#[test]
fn promql_query_increase_evaluates_counter_range_from_sealed_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(71);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/api".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[(1_001, 0.0), (3_000, 5.0), (5_000, 2.0), (6_000, 6.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.requests.total{route="/api"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 11.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == "route" && value == "/api")
    );
}

#[test]
fn promql_query_increase_resumes_after_stale_marker() {
    let tempdir = tempfile::tempdir().unwrap();
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/stale-counter".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(75),
            &raw_labels,
            &[
                (1_000, 10.0),
                (2_000, prometheus_stale_nan()),
                (3_000, 0.0),
                (4_000, 2.0),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.requests.total{route="/stale-counter"}[4s])"#,
            0,
            4_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(4_000, 2.0)]);
}

#[test]
fn promql_query_rate_evaluates_counter_range_with_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.requests.total"),
            ("route", "/head"),
        ],
    );
    let mut head = test_head();
    for (ts, value) in [(1_000, 1.0), (3_000, 5.0), (6_000, 11.0)] {
        head.record_sample(series, ts, SampleValue::Float(value))
            .unwrap();
    }

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rate(http.requests.total{route="/head"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_rate_extrapolates_counter_to_requested_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(91);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/extrapolate".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(2_000, 2.0), (4_000, 4.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"rate(http.requests.total{route="/extrapolate"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 10_000);
    assert!((results[0].samples[0].1 - 0.5).abs() < 1e-9);
}

#[test]
fn promql_query_rate_excludes_left_boundary_sample_from_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(94);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/left-open".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(1_000, 10.0), (6_000, 15.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"rate(http.requests.total{route="/left-open"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_rate_clamps_sparse_counter_start_before_zero_point() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(93);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/sparse-zero".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(8_000, 80.0), (9_000, 180.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"rate(http.requests.total{route="/sparse-zero"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 25.0)]);
}

#[test]
fn promql_query_delta_extrapolates_gauge_to_requested_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(97);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-a".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(2_000, 20.0), (4_000, 22.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"delta(cpu.temperature.celsius{sensor="rack-a"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 5.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_irate_uses_only_last_two_counter_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(95);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/irate".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[(1_000, 0.0), (7_000, 100.0), (9_000, 106.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"irate(http.requests.total{route="/irate"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_irate_handles_reset_between_last_two_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(96);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/irate-reset".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[(1_000, 50.0), (8_000, 80.0), (9_000, 7.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"irate(http.requests.total{route="/irate-reset"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 7.0)]);
}

#[test]
fn promql_query_idelta_uses_only_last_two_gauge_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(98);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-b".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[(1_000, 20.0), (7_000, 100.0), (9_000, 80.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"idelta(cpu.temperature.celsius{sensor="rack-b"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, -20.0)]);
}

#[test]
fn promql_query_changes_counts_value_transitions_in_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(119);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-changes".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (0, 100.0),
                (1_000, 1.0),
                (2_000, 1.0),
                (3_000, prometheus_stale_nan()),
                (4_000, 2.0),
                (5_000, f64::NAN),
                (6_000, f64::NAN),
                (7_000, f64::INFINITY),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"changes(cpu.temperature.celsius{sensor="rack-changes"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == "sensor" && value == "rack-changes")
    );
}

#[test]
fn promql_query_resets_counts_counter_decreases_after_stale_boundary() {
    let tempdir = tempfile::tempdir().unwrap();
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/resets".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(120),
            &raw_labels,
            &[
                (0, 1_000.0),
                (1_000, 100.0),
                (2_000, 90.0),
                (3_000, prometheus_stale_nan()),
                (4_000, 10.0),
                (5_000, 5.0),
                (6_000, 8.0),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"resets(http.requests.total{route="/resets"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_resets_uses_histogram_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(121),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 100,
                        sum: Some(200.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![100, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 120,
                        sum: Some(240.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![120, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.reset");
                visit("route", "/resets-hint");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"resets(http.request.reset_count{route="/resets-hint"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 1.0)]);
}

#[test]
fn promql_query_last_over_time_preserves_metric_name_and_skips_stale_marker() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(99);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-c".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (1_000, 20.0),
                (7_000, 51.0),
                (9_000, prometheus_stale_nan()),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"last_over_time(cpu.temperature.celsius{sensor="rack-c"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 51.0)]);
    assert_eq!(
        results[0]
            .labels
            .iter()
            .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str())),
        Some(normalize_metric_name("cpu.temperature.celsius").as_str())
    );
}

#[test]
fn promql_query_count_over_time_counts_non_stale_range_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(100);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-d".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (0, 10.0),
                (1_000, 20.0),
                (2_000, prometheus_stale_nan()),
                (3_000, f64::INFINITY),
                (4_000, f64::NAN),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"count_over_time(cpu.temperature.celsius{sensor="rack-d"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_present_over_time_returns_one_for_any_non_stale_range_sample() {
    let tempdir = tempfile::tempdir().unwrap();
    let present_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-present".to_string()),
    ];
    let stale_only_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-stale-only".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(101),
            &present_labels,
            &[
                (0, 10.0),
                (1_000, prometheus_stale_nan()),
                (2_000, f64::NAN),
            ],
        )
        .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(102),
            &stale_only_labels,
            &[(1_000, prometheus_stale_nan())],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"present_over_time(cpu.temperature.celsius{sensor=~"rack-.*"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == "sensor" && value == "rack-present")
    );
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_sum_over_time_sums_non_stale_range_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(103);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-e".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (0, 10.0),
                (1_000, 2.0),
                (2_000, prometheus_stale_nan()),
                (3_000, 3.5),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"sum_over_time(cpu.temperature.celsius{sensor="rack-e"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 5.5)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_sum_over_time_preserves_infinite_result() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(104);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-f".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(1_000, 2.0), (2_000, f64::INFINITY)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"sum_over_time(cpu.temperature.celsius{sensor="rack-f"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    let value = results[0].samples[0].1;
    assert!(value.is_infinite());
    assert!(value.is_sign_positive());
}

#[test]
fn promql_query_avg_over_time_averages_non_stale_range_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(105);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-g".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (0, 100.0),
                (1_000, 2.0),
                (2_000, prometheus_stale_nan()),
                (3_000, 4.0),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"avg_over_time(cpu.temperature.celsius{sensor="rack-g"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_avg_over_time_large_finite_samples_do_not_overflow() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(106);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-h".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(1_000, f64::MAX), (2_000, f64::MAX)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"avg_over_time(cpu.temperature.celsius{sensor="rack-h"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, f64::MAX)]);
}

#[test]
fn promql_query_avg_over_time_preserves_infinite_result() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(107);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-i".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(1_000, 2.0), (2_000, f64::INFINITY)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"avg_over_time(cpu.temperature.celsius{sensor="rack-i"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    let value = results[0].samples[0].1;
    assert!(value.is_infinite());
    assert!(value.is_sign_positive());
}

#[test]
fn promql_query_stdvar_and_stddev_over_time_use_population_variance() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(108);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-j".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (0, 100.0),
                (1_000, 2.0),
                (2_000, prometheus_stale_nan()),
                (3_000, 4.0),
                (4_000, 4.0),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let stdvar = store
        .query_promql(
            r#"stdvar_over_time(cpu.temperature.celsius{sensor="rack-j"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();
    let stddev = store
        .query_promql(
            r#"stddev_over_time(cpu.temperature.celsius{sensor="rack-j"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(stdvar.len(), 1);
    assert_eq!(stddev.len(), 1);
    let expected_variance: f64 = 8.0 / 9.0;
    let expected_stddev = expected_variance.sqrt();
    assert!((stdvar[0].samples[0].1 - expected_variance).abs() < 1e-12);
    assert!((stddev[0].samples[0].1 - expected_stddev).abs() < 1e-12);
    assert_eq!(stdvar[0].samples[0].0, 10_000);
    assert!(
        !stdvar[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
    assert!(
        !stddev[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_stdvar_over_time_preserves_ordinary_nan_result() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(109);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-k".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(1_000, 2.0), (2_000, f64::NAN)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"stdvar_over_time(cpu.temperature.celsius{sensor="rack-k"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].samples[0].1.is_nan());
}

#[test]
fn promql_query_min_over_time_selects_non_stale_range_minimum() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(110);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-l".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (0, -100.0),
                (1_000, 7.0),
                (2_000, prometheus_stale_nan()),
                (3_000, f64::NAN),
                (4_000, f64::NEG_INFINITY),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"min_over_time(cpu.temperature.celsius{sensor="rack-l"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    let value = results[0].samples[0].1;
    assert!(value.is_infinite());
    assert!(value.is_sign_negative());
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_max_over_time_selects_non_stale_range_maximum() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(111);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "cpu.temperature.celsius".to_string(),
        ),
        ("sensor".to_string(), "rack-m".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[
                (0, 100.0),
                (1_000, 7.0),
                (2_000, prometheus_stale_nan()),
                (3_000, f64::NAN),
                (4_000, f64::INFINITY),
            ],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"max_over_time(cpu.temperature.celsius{sensor="rack-m"}[10s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    let value = results[0].samples[0].1;
    assert!(value.is_infinite());
    assert!(value.is_sign_positive());
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
}

#[test]
fn promql_query_deriv_predict_linear_and_quantile_over_time() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.temperature".to_string()),
            ("sensor".to_string(), "rack-a".to_string()),
        ],
        &[(1_000, 2.0), (11_000, 4.0), (21_000, 6.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let deriv = store
        .query_promql(r#"deriv(cpu.temperature{sensor="rack-a"}[25s])"#, 0, 21_000)
        .unwrap();
    assert_eq!(deriv.len(), 1);
    assert_eq!(
        deriv[0].labels.as_ref(),
        &[("sensor".to_string(), "rack-a".to_string())]
    );
    assert_approx_eq(deriv[0].samples[0].1, 0.2, 1e-12);

    let prediction = store
        .query_promql(
            r#"predict_linear(cpu.temperature{sensor="rack-a"}[25s], 10)"#,
            0,
            21_000,
        )
        .unwrap();
    assert_eq!(prediction.len(), 1);
    assert_eq!(
        prediction[0].labels.as_ref(),
        &[("sensor".to_string(), "rack-a".to_string())]
    );
    assert_approx_eq(prediction[0].samples[0].1, 8.0, 1e-12);

    let quantile = store
        .query_promql(
            r#"quantile_over_time(0.5, cpu.temperature{sensor="rack-a"}[25s])"#,
            0,
            21_000,
        )
        .unwrap();
    assert_eq!(quantile.len(), 1);
    assert_eq!(
        quantile[0].labels.as_ref(),
        &[("sensor".to_string(), "rack-a".to_string())]
    );
    assert_eq!(quantile[0].samples, vec![(21_000, 4.0)]);
}

#[test]
fn promql_query_double_exponential_smoothing_accepts_prometheus_aliases() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.temperature".to_string()),
            ("sensor".to_string(), "rack-a".to_string()),
        ],
        &[(1_000, 3.0), (11_000, 5.0), (21_000, 9.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    for query in [
        r#"double_exponential_smoothing(cpu.temperature{sensor="rack-a"}[25s], 0.5, 0.5)"#,
        r#"holt_winters(cpu.temperature{sensor="rack-a"}[25s], 0.5, 0.5)"#,
    ] {
        let results = store.query_promql(query, 0, 21_000).unwrap();
        assert_eq!(results.len(), 1, "query {query}");
        assert_eq!(
            results[0].labels.as_ref(),
            &[("sensor".to_string(), "rack-a".to_string())],
            "query {query}"
        );
        assert_approx_eq(results[0].samples[0].1, 8.0, 1e-12);
    }
}

#[test]
fn promql_query_sum_by_rate_uses_samples_crossing_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/cross-segment".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(92), &labels, &[(5_000, 5.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(92), &labels, &[(15_000, 20.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"sum by (route)(rate(http.requests.total{route="/cross-segment"}[15s]))"#,
            0,
            15_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 15_000);
    assert!((results[0].samples[0].1 - (20.0 / 15.0)).abs() < 1e-9);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/cross-segment".to_string())]
    );
}

#[test]
fn promql_query_histogram_quantile_evaluates_bucket_rate() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(72),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.25 + 0.25, rate(http.request.duration_bucket{route="/quantile"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL || key == "le")
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == "route" && value == "/quantile")
    );
}

#[test]
fn promql_query_histogram_quantile_returns_nan_for_malformed_classic_buckets() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, route, le, value) in [
        (SeriesRef::new(154), "/quantile-missing-inf", "1", 2.0),
        (SeriesRef::new(155), "/quantile-missing-inf", "2", 5.0),
        (SeriesRef::new(156), "/quantile-single-inf", "+Inf", 5.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "classic_duration_bucket".to_string(),
                ),
                ("route".to_string(), route.to_string()),
                ("le".to_string(), le.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, classic_duration_bucket)"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 2);
    let mut values_by_route = BTreeMap::new();
    for result in results {
        assert_eq!(result.samples.len(), 1);
        assert_eq!(result.samples[0].0, 10_000);
        assert!(
            !result
                .labels
                .iter()
                .any(|(key, _)| key == METRIC_NAME_LABEL || key == "le")
        );
        let route = result
            .labels
            .iter()
            .find_map(|(key, value)| (key == "route").then_some(value.clone()))
            .unwrap();
        values_by_route.insert(route, result.samples[0].1);
    }

    assert!(
        values_by_route["/quantile-missing-inf"].is_nan(),
        "missing +Inf bucket should return a NaN sample"
    );
    assert!(
        values_by_route["/quantile-single-inf"].is_nan(),
        "classic histogram with fewer than two buckets should return a NaN sample"
    );
}

#[test]
fn promql_query_histogram_quantile_coalesces_duplicate_bucket_bounds_by_sum() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, le, value) in [
        (SeriesRef::new(350), "classic_coalesce_a_bucket", "1", 2.0),
        (SeriesRef::new(351), "classic_coalesce_a_bucket", "2", 4.0),
        (
            SeriesRef::new(352),
            "classic_coalesce_a_bucket",
            "+Inf",
            4.0,
        ),
        (SeriesRef::new(353), "classic_coalesce_b_bucket", "1", 8.0),
        (SeriesRef::new(354), "classic_coalesce_b_bucket", "2", 8.0),
        (
            SeriesRef::new(355),
            "classic_coalesce_b_bucket",
            "+Inf",
            8.0,
        ),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (METRIC_NAME_LABEL.to_string(), metric.to_string()),
                ("route".to_string(), "/quantile-coalesce".to_string()),
                ("le".to_string(), le.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, {__name__=~"classic_coalesce_[ab]_bucket",route="/quantile-coalesce"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 10_000);
    assert!((results[0].samples[0].1 - 0.6).abs() < 1e-12);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/quantile-coalesce".to_string())]
    );
}

#[test]
fn promql_query_histogram_quantile_uses_real_classic_buckets_with_regex_le_matcher() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, le, value) in [
        (SeriesRef::new(157), "1", 2.0),
        (SeriesRef::new(158), "2", 5.0),
        (SeriesRef::new(159), "+Inf", 5.0),
        (SeriesRef::new(160), "4", 10.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "classic_regex_bucket".to_string(),
                ),
                ("route".to_string(), "/regex-le".to_string()),
                ("le".to_string(), le.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, classic_regex_bucket{route="/regex-le",le=~"1|2|[+]Inf"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 10_000);
    assert!((results[0].samples[0].1 - (7.0 / 6.0)).abs() < 1e-9);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/regex-le".to_string())]
    );
}

#[test]
fn promql_query_native_histogram_bucket_projection_filters_non_equality_le_matchers() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(161),
            &[(
                5_000,
                HistogramValue {
                    count: 10,
                    sum: Some(30.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 2.0, 4.0],
                    bucket_counts: vec![2, 3, 5, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/native-le-sealed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let head_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration"),
            ("route", "/native-le-head"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        head_series,
        6_000,
        SampleValue::Histogram(HistogramValue {
            count: 20,
            sum: Some(60.0),
            min: None,
            max: None,
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 2.0, 4.0],
            bucket_counts: vec![4, 6, 10, 0],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let regex_results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"http.request.duration_bucket{route=~"/native-le-(sealed|head)",le=~"1|4|[+]Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(regex_results.len(), 6);
    let regex_samples = samples_by_route_and_le(&regex_results);
    assert_eq!(
        regex_samples[&("/native-le-sealed".to_string(), "1".to_string())],
        vec![(5_000, 2.0)]
    );
    assert_eq!(
        regex_samples[&("/native-le-sealed".to_string(), "4".to_string())],
        vec![(5_000, 10.0)]
    );
    assert_eq!(
        regex_samples[&("/native-le-sealed".to_string(), "+Inf".to_string())],
        vec![(5_000, 10.0)]
    );
    assert_eq!(
        regex_samples[&("/native-le-head".to_string(), "1".to_string())],
        vec![(6_000, 4.0)]
    );
    assert_eq!(
        regex_samples[&("/native-le-head".to_string(), "4".to_string())],
        vec![(6_000, 20.0)]
    );
    assert_eq!(
        regex_samples[&("/native-le-head".to_string(), "+Inf".to_string())],
        vec![(6_000, 20.0)]
    );

    let not_eq_results = store
        .query_promql(
            r#"http.request.duration_bucket{route="/native-le-sealed",le!="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(not_eq_results.len(), 3);
    let not_eq_samples = samples_by_label(&not_eq_results, "le");
    assert_eq!(not_eq_samples["1"], vec![(5_000, 2.0)]);
    assert_eq!(not_eq_samples["4"], vec![(5_000, 10.0)]);
    assert_eq!(not_eq_samples["+Inf"], vec![(5_000, 10.0)]);
}

#[test]
fn promql_query_histogram_quantile_bucket_metric_name_regex_uses_classic_projection() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(205),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/quantile-regex");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate({__name__=~"http_request_duration.*_bucket",route="/quantile-regex"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(4),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(execution.stats.projected_series, 4);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_histogram_quantile_over_sum_by_bucket_rate() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(201), "a"), (SeriesRef::new(202), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata::default(),
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![2, 5, 3, 0],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(40.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata::default(),
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.duration");
                    visit("route", "/quantile-agg");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, sum by (le, route)(rate(http.request.duration_bucket{route="/quantile-agg"}[5s])))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/quantile-agg".to_string())]
    );
}

#[test]
fn promql_query_histogram_quantile_uses_instant_lookback_for_vector_input() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(203),
            &[(
                50_000,
                HistogramValue {
                    count: 10,
                    sum: Some(20.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 2.0],
                    bucket_counts: vec![2, 5, 3],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/old-quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, http.request.duration_bucket{route="/old-quantile"})"#,
            0,
            400_000,
        )
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_native_histogram_quantile_does_not_project_bucket_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(204),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.duration");
                visit("route", "/native-quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate(http.request.native.duration{route="/native-quantile"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_native_histogram_quantile_accepts_metric_name_regex_selector() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(684),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http_request_native_regex_duration");
                visit("route", "/native-regex-quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate({__name__=~"http_request_native_regex_duration",route="/native-regex-quantile"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_native_histogram_quantile_treats_le_as_regular_selector_label() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(685),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http_request_native_le_duration");
                visit("le", "literal-dimension");
                visit("route", "/native-le-quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate(http_request_native_le_duration{le="literal-dimension",route="/native-le-quantile"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[
            ("le".to_string(), "literal-dimension".to_string()),
            ("route".to_string(), "/native-le-quantile".to_string()),
        ]
    );
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_histogram_quantile_combines_classic_buckets_and_native_histograms() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, le, value) in [
        (SeriesRef::new(662), "1", 2.0),
        (SeriesRef::new(663), "2", 5.0),
        (SeriesRef::new(664), "+Inf", 5.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "http.request.mixed.quantile".to_string(),
                ),
                ("route".to_string(), "/classic-mixed-quantile".to_string()),
                ("le".to_string(), le.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(665),
            &[(
                5_000,
                HistogramValue {
                    count: 5,
                    sum: Some(8.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 2.0],
                    bucket_counts: vec![2, 3, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.mixed.quantile");
                visit("route", "/native-mixed-quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, http.request.mixed.quantile)"#,
            0,
            10_000,
        )
        .unwrap();
    let samples_by_route = samples_by_label(&results, "route");

    assert_eq!(samples_by_route.len(), 2);
    for route in ["/classic-mixed-quantile", "/native-mixed-quantile"] {
        let samples = samples_by_route
            .get(route)
            .unwrap_or_else(|| panic!("missing quantile result for {route}"));
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].0, 10_000);
        assert!((samples[0].1 - (7.0 / 6.0)).abs() < 1e-9);
    }
}

#[test]
fn promql_query_native_histogram_rate_excludes_left_boundary_sample_from_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(214),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.boundary");
                visit("route", "/native-left-open");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate(http.request.native.boundary{route="/native-left-open"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert!(execution.results.is_empty());
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_session_matches_native_histogram_quantile_store_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(205),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.session");
                visit("route", "/native-session");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let query = r#"histogram_quantile(0.5, rate(http.request.native.session{route="/native-session"}[5s]))"#;
    let limits = QueryLimits {
        max_projected_series: Some(1),
        ..QueryLimits::unlimited()
    };
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let expected = store
        .query_promql_with_limits(query, 0, 6_000, limits)
        .unwrap();
    let mut session = store.query_session().unwrap();
    let actual = session
        .query_promql_with_limits(query, 0, 6_000, limits)
        .unwrap();

    assert_eq!(actual.results, expected.results);
    assert_eq!(actual.stats.projected_series, 1);
    assert_eq!(actual.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_native_histogram_quantile_over_sum_by_rate_stays_native() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(206), "a"), (SeriesRef::new(207), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![2, 5, 3, 0],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(40.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.duration");
                    visit("route", "/native-quantile-agg");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.duration{route="/native-quantile-agg"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[("route".to_string(), "/native-quantile-agg".to_string())]
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_native_histogram_quantile_over_avg_by_rate_stays_native() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(208), "a"), (SeriesRef::new(209), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![2, 5, 3, 0],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(40.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.duration.avg");
                    visit("route", "/native-quantile-avg");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, avg by (route)(rate(http.request.native.duration.avg{route="/native-quantile-avg"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[("route".to_string(), "/native-quantile-avg".to_string())]
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_native_histogram_quantile_over_avg_without_rate_stays_native() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(218), "a"), (SeriesRef::new(219), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![2, 5, 3, 0],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(40.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(
                        METRIC_NAME_LABEL,
                        "http.request.native.duration.avg_without",
                    );
                    visit("route", "/native-quantile-avg-without");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, avg without (instance)(rate(http.request.native.duration.avg_without{route="/native-quantile-avg-without"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/native-quantile-avg-without".to_string()
        )]
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_native_histogram_scalar_functions_read_aggregated_rate_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(228), "a"), (SeriesRef::new(229), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0],
                            bucket_counts: vec![2, 5, 3],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(50.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0],
                            bucket_counts: vec![4, 10, 6],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.scalar");
                    visit("route", "/native-scalar");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let input = r#"sum by (route)(rate(http.request.native.scalar{route="/native-scalar"}[5s]))"#;
    let count = store
        .query_promql(&format!("histogram_count({input})"), 0, 6_000)
        .unwrap();
    let sum = store
        .query_promql(&format!("histogram_sum({input})"), 0, 6_000)
        .unwrap();
    let avg = store
        .query_promql(&format!("histogram_avg({input})"), 0, 6_000)
        .unwrap();
    let classic = store
        .query_promql(
            r#"histogram_count(rate(http.request.native.scalar_bucket{route="/native-scalar"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    let expected_count = 20_000.0 / 4_999.0;
    let expected_sum = 60_000.0 / 4_999.0;
    for results in [&count, &sum, &avg] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.as_ref(),
            &[("route".to_string(), "/native-scalar".to_string())]
        );
        assert_eq!(results[0].samples[0].0, 6_000);
    }
    assert!((count[0].samples[0].1 - expected_count).abs() < 1e-12);
    assert!((sum[0].samples[0].1 - expected_sum).abs() < 1e-12);
    assert!((avg[0].samples[0].1 - 3.0).abs() < 1e-12);
    assert!(classic.is_empty());
}

#[test]
fn promql_query_native_histogram_binary_scalar_arithmetic_feeds_scalar_functions() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let samples = [
        (
            0,
            HistogramValue {
                count: 5,
                sum: Some(5.0),
                min: None,
                max: None,
                metadata: TypedSampleMetadata {
                    reset_hint: CounterResetHint::NotCounterReset,
                    ..TypedSampleMetadata::default()
                },
                explicit_bounds: vec![1.0, 2.0],
                bucket_counts: vec![2, 2, 1],
            },
        ),
        (
            40_000,
            HistogramValue {
                count: 25,
                sum: Some(25.0),
                min: None,
                max: None,
                metadata: TypedSampleMetadata {
                    reset_hint: CounterResetHint::NotCounterReset,
                    ..TypedSampleMetadata::default()
                },
                explicit_bounds: vec![1.0, 2.0],
                bucket_counts: vec![10, 10, 5],
            },
        ),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(230),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.binary");
                visit("route", "/native-binary");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count_times_two = store
        .query_promql(
            r#"histogram_count(http.request.native.binary{route="/native-binary"} * 2)"#,
            0,
            40_000,
        )
        .unwrap();
    let sum_times_two = store
        .query_promql(
            r#"histogram_sum(2 * http.request.native.binary{route="/native-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let count_div_two = store
        .query_promql(
            r#"histogram_count(http.request.native.binary{route="/native-binary"} / 2)"#,
            0,
            40_000,
        )
        .unwrap();
    let scalar_div_histogram = store
        .query_promql(
            r#"histogram_count(2 / http.request.native.binary{route="/native-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let histogram_plus_scalar = store
        .query_promql(
            r#"histogram_count(http.request.native.binary{route="/native-binary"} + 2)"#,
            0,
            40_000,
        )
        .unwrap();

    for results in [&count_times_two, &sum_times_two, &count_div_two] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.as_ref(),
            &[("route".to_string(), "/native-binary".to_string())]
        );
        assert_eq!(results[0].samples[0].0, 40_000);
    }
    assert_eq!(count_times_two[0].samples[0].1, 50.0);
    assert_eq!(sum_times_two[0].samples[0].1, 50.0);
    assert_eq!(count_div_two[0].samples[0].1, 12.5);
    assert!(scalar_div_histogram.is_empty());
    assert!(histogram_plus_scalar.is_empty());
}

#[test]
fn promql_query_native_histogram_binary_vector_arithmetic_and_comparison() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, count, sum, bucket_counts) in [
        (
            SeriesRef::new(231),
            "http.request.native.binary.left",
            25,
            25.0,
            vec![10, 10, 5],
        ),
        (
            SeriesRef::new(232),
            "http.request.native.binary.right",
            7,
            7.0,
            vec![3, 2, 2],
        ),
    ] {
        let samples = [(
            40_000,
            HistogramValue {
                count,
                sum: Some(sum),
                min: None,
                max: None,
                metadata: TypedSampleMetadata {
                    reset_hint: CounterResetHint::NotCounterReset,
                    ..TypedSampleMetadata::default()
                },
                explicit_bounds: vec![1.0, 2.0],
                bucket_counts,
            },
        )];
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, metric);
                visit("route", "/native-vector-binary");
            })
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count_plus = store
        .query_promql(
            r#"histogram_count(http.request.native.binary.left{route="/native-vector-binary"} + http.request.native.binary.right{route="/native-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let sum_minus = store
        .query_promql(
            r#"histogram_sum(http.request.native.binary.left{route="/native-vector-binary"} - http.request.native.binary.right{route="/native-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let equal_left = store
        .query_promql(
            r#"histogram_count(http.request.native.binary.left{route="/native-vector-binary"} == http.request.native.binary.left{route="/native-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let not_equal = store
        .query_promql(
            r#"histogram_count(http.request.native.binary.left{route="/native-vector-binary"} != http.request.native.binary.right{route="/native-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let multiply = store
        .query_promql(
            r#"histogram_count(http.request.native.binary.left{route="/native-vector-binary"} * http.request.native.binary.right{route="/native-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let greater_than = store
        .query_promql(
            r#"histogram_count(http.request.native.binary.left{route="/native-vector-binary"} > http.request.native.binary.right{route="/native-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();

    for results in [&count_plus, &sum_minus, &equal_left, &not_equal] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.as_ref(),
            &[("route".to_string(), "/native-vector-binary".to_string())]
        );
        assert_eq!(results[0].samples[0].0, 40_000);
    }
    assert_eq!(count_plus[0].samples[0].1, 32.0);
    assert_eq!(sum_minus[0].samples[0].1, 18.0);
    assert_eq!(equal_left[0].samples[0].1, 25.0);
    assert_eq!(not_equal[0].samples[0].1, 25.0);
    assert!(multiply.is_empty());
    assert!(greater_than.is_empty());
}

#[test]
fn promql_query_native_histogram_count_aggregation_counts_histograms_not_bucket_projections() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(560), "a"), (SeriesRef::new(561), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0],
                            bucket_counts: vec![2, 5, 3],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(50.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0],
                            bucket_counts: vec![4, 10, 6],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.count.aggregate");
                    visit("route", "/native-count-aggregation");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"count by (route)(rate(http.request.native.count.aggregate{route="/native-count-aggregation"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[("route".to_string(), "/native-count-aggregation".to_string())]
    );
    assert_eq!(execution.results[0].samples, vec![(6_000, 2.0)]);
    assert_eq!(execution.stats.projected_series, 2);
}

#[test]
fn promql_query_count_aggregation_combines_scalar_and_native_histogram_elements() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(660),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.request.mixed.count".to_string(),
            ),
            (
                "route".to_string(),
                "/mixed-native-scalar-count".to_string(),
            ),
            ("source".to_string(), "scalar".to_string()),
        ],
        &[(5_000, 42.0)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(661),
            &[(
                5_000,
                HistogramValue {
                    count: 10,
                    sum: Some(20.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 2.0],
                    bucket_counts: vec![2, 5, 3],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.mixed.count");
                visit("route", "/mixed-native-scalar-count");
                visit("source", "histogram");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"count by (route)(http.request.mixed.count{route="/mixed-native-scalar-count"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/mixed-native-scalar-count".to_string()
        )]
    );
    assert_eq!(results[0].samples, vec![(10_000, 2.0)]);
}

#[test]
fn promql_query_native_histogram_changes_ignores_direct_histogram_inputs() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(562),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![2, 5, 3],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(50.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![4, 10, 6],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.changes.direct");
                visit("route", "/native-changes-direct");
                visit("instance", "a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql(
            r#"changes(http.request.native.changes.direct{route="/native-changes-direct"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert!(execution.is_empty());
}

#[test]
fn promql_query_native_histogram_count_aggregation_merges_sealed_and_head_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(563),
            &[(
                1_001,
                HistogramValue {
                    count: 10,
                    sum: Some(20.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        reset_hint: CounterResetHint::NotCounterReset,
                        ..TypedSampleMetadata::default()
                    },
                    explicit_bounds: vec![1.0, 2.0],
                    bucket_counts: vec![2, 5, 3],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.count.cross_head");
                visit("route", "/native-count-cross-head");
                visit("instance", "a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.native.count.cross_head"),
            ("instance", "a"),
            ("route", "/native-count-cross-head"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        series,
        6_000,
        SampleValue::Histogram(HistogramValue {
            count: 20,
            sum: Some(50.0),
            min: None,
            max: None,
            metadata: TypedSampleMetadata {
                reset_hint: CounterResetHint::NotCounterReset,
                ..TypedSampleMetadata::default()
            },
            explicit_bounds: vec![1.0, 2.0],
            bucket_counts: vec![4, 10, 6],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"count by (route)(rate(http.request.native.count.cross_head{route="/native-count-cross-head"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[("route".to_string(), "/native-count-cross-head".to_string())]
    );
    assert_eq!(execution.results[0].samples, vec![(6_000, 1.0)]);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.samples_decoded, 2);
}

#[test]
fn promql_query_native_histogram_sum_skips_stale_inputs() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, metadata, count, sum, bucket_counts) in [
        (
            SeriesRef::new(220),
            "valid",
            TypedSampleMetadata::default(),
            6,
            Some(12.0),
            vec![1, 4, 1],
        ),
        (
            SeriesRef::new(221),
            "stale",
            TypedSampleMetadata {
                flags: OTLP_FLAG_NO_RECORDED_VALUE,
                ..TypedSampleMetadata::default()
            },
            0,
            Some(0.0),
            vec![0, 0, 0],
        ),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[(
                    5_000,
                    HistogramValue {
                        count,
                        sum,
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0, 5.0],
                        bucket_counts,
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.stale.aggregate");
                    visit("route", "/native-stale-sum");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_count(sum by (route)(http.request.native.stale.aggregate{route="/native-stale-sum"}))"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-stale-sum".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 6.0)]);
}

#[test]
fn promql_query_native_histogram_scalar_function_accepts_metric_name_with_projection_suffix() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(224),
            &[(
                5_000,
                HistogramValue {
                    count: 6,
                    sum: Some(12.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 4, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.actual_sum");
                visit("route", "/native-suffix-name");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_count(http.request.native.actual_sum{route="/native-suffix-name"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-suffix-name".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 6.0)]);
}

#[test]
fn promql_query_native_histogram_fraction_reads_sum_without_rate_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(234), "a"), (SeriesRef::new(235), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![2, 5, 3, 0],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(40.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.fraction.without");
                    visit("route", "/native-fraction-without");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_fraction(1, 3, sum without (instance)(rate(http.request.native.fraction.without{route="/native-fraction-without"}[5s])))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-fraction-without".to_string())]
    );
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 0.65).abs() < 1e-12);
}

#[test]
fn promql_query_native_histogram_fraction_reads_rate_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(230),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.fraction");
                visit("route", "/native-fraction");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_fraction(1 / 1, 2 + 1, rate(http.request.native.fraction{route="/native-fraction"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-fraction".to_string())]
    );
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 0.65).abs() < 1e-12);
}

#[test]
fn promql_query_native_histogram_fraction_accepts_infinite_bounds() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(232),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.fraction.bounds");
                visit("route", "/native-fraction-bounds");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_fraction(-Inf, Inf, rate(http.request.native.fraction.bounds{route="/native-fraction-bounds"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-fraction-bounds".to_string())]
    );
    assert_eq!(results[0].samples, vec![(6_000, 1.0)]);
}

#[test]
fn promql_query_native_exponential_histogram_fraction_reads_rate_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(231),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![2, 3],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![4, 6],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.fraction");
                visit("route", "/native-exphist-fraction");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_fraction(1, 2, rate(http.request.native.exphist.fraction{route="/native-exphist-fraction"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-exphist-fraction".to_string())]
    );
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 0.4).abs() < 1e-12);
}

#[test]
fn promql_query_native_exponential_histogram_fraction_reads_sum_without_rate_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(236), "a"), (SeriesRef::new(237), "b")] {
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        ExponentialHistogramValue {
                            count: 5,
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![2, 3],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                    (
                        6_000,
                        ExponentialHistogramValue {
                            count: 10,
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![4, 6],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                ],
                |visit| {
                    visit(
                        METRIC_NAME_LABEL,
                        "http.request.native.exphist.fraction.without",
                    );
                    visit("route", "/native-exphist-fraction-without");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_fraction(1, 2, sum without (instance)(rate(http.request.native.exphist.fraction.without{route="/native-exphist-fraction-without"}[5s])))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/native-exphist-fraction-without".to_string()
        )]
    );
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 0.4).abs() < 1e-12);
}

#[test]
fn promql_query_native_exponential_histogram_fraction_accepts_infinite_bounds() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(233),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![2, 3],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![4, 6],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.fraction.bounds",
                );
                visit("route", "/native-exphist-fraction-bounds");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_fraction(-Inf, Inf, rate(http.request.native.exphist.fraction.bounds{route="/native-exphist-fraction-bounds"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/native-exphist-fraction-bounds".to_string()
        )]
    );
    assert_eq!(results[0].samples, vec![(6_000, 1.0)]);
}

#[test]
fn promql_query_native_histogram_rate_coarsens_custom_bucket_layout_changes() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(208),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 3.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.duration");
                visit("route", "/native-quantile-layout-change");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.duration{route="/native-quantile-layout-change"}[6s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/native-quantile-layout-change".to_string()
        )]
    );
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 2.125).abs() < 1e-9);
}

#[test]
fn promql_query_native_histogram_sum_coarsens_custom_bucket_layouts() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, bounds) in [
        (SeriesRef::new(208), "a", vec![1.0, 2.0, 4.0]),
        (SeriesRef::new(209), "b", vec![1.0, 3.0, 4.0]),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_000,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: bounds.clone(),
                            bucket_counts: vec![2, 5, 3, 0],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(40.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: bounds,
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.duration");
                    visit("route", "/native-quantile-incompatible");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.duration{route="/native-quantile-incompatible"}[6s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/native-quantile-incompatible".to_string()
        )]
    );
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 2.125).abs() < 1e-9);
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_native_histogram_quantile_reads_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.native.head"),
            ("route", "/native-head"),
        ],
    );

    let mut head = test_head();
    head.record_sample(
        series,
        1_001,
        SampleValue::Histogram(HistogramValue {
            count: 10,
            sum: Some(20.0),
            min: None,
            max: None,
            metadata: TypedSampleMetadata {
                reset_hint: CounterResetHint::NotCounterReset,
                ..TypedSampleMetadata::default()
            },
            explicit_bounds: vec![1.0, 2.0, 4.0],
            bucket_counts: vec![2, 5, 3, 0],
        }),
    )
    .unwrap();
    head.record_sample(
        series,
        6_000,
        SampleValue::Histogram(HistogramValue {
            count: 20,
            sum: Some(40.0),
            min: None,
            max: None,
            metadata: TypedSampleMetadata {
                reset_hint: CounterResetHint::NotCounterReset,
                ..TypedSampleMetadata::default()
            },
            explicit_bounds: vec![1.0, 2.0, 4.0],
            bucket_counts: vec![4, 10, 6, 0],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"histogram_quantile(0.5, rate(http.request.native.head{route="/native-head"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-12);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.samples_decoded, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_native_exponential_histogram_quantile_reads_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.native.exphist.head"),
            ("route", "/native-exphist-head"),
        ],
    );

    let mut head = test_head();
    for (timestamp_ms, count, sum, positive_counts) in
        [(1_001, 5, 12.0, vec![2, 3]), (6_000, 10, 24.0, vec![4, 6])]
    {
        head.record_sample(
            series,
            timestamp_ms,
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count,
                sum: Some(sum),
                min: None,
                max: None,
                metadata: TypedSampleMetadata {
                    reset_hint: CounterResetHint::NotCounterReset,
                    ..TypedSampleMetadata::default()
                },
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: positive_counts,
                },
                negative: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: Vec::new(),
                },
            }),
        )
        .unwrap();
    }

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"histogram_quantile(0.5, rate(http.request.native.exphist.head{route="/native-exphist-head"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.powf(1.0 / 6.0);
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - expected).abs() < 1e-12);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.samples_decoded, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_native_exponential_histogram_quantile_over_head_sum_by_rate_stays_native() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let mut head = test_head();

    for (series, first_counts, second_counts) in [
        (
            labels(
                &mut label_store,
                &[
                    (METRIC_NAME_LABEL, "http.request.native.exphist.head.agg"),
                    ("instance", "a"),
                    ("route", "/native-exphist-head-agg"),
                ],
            ),
            vec![2, 3],
            vec![4, 6],
        ),
        (
            labels(
                &mut label_store,
                &[
                    (METRIC_NAME_LABEL, "http.request.native.exphist.head.agg"),
                    ("instance", "b"),
                    ("route", "/native-exphist-head-agg"),
                ],
            ),
            vec![1, 1],
            vec![3, 5],
        ),
    ] {
        for (timestamp_ms, counts) in [(1_001, first_counts), (6_000, second_counts)] {
            head.record_sample(
                series,
                timestamp_ms,
                SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                    count: counts.iter().sum(),
                    sum: None,
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        reset_hint: CounterResetHint::NotCounterReset,
                        ..TypedSampleMetadata::default()
                    },
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets { offset: 0, counts },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                }),
            )
            .unwrap();
        }
    }

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.exphist.head.agg{route="/native-exphist-head-agg"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.powf(3.0 / 14.0);
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - expected).abs() < 1e-12);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[("route".to_string(), "/native-exphist-head-agg".to_string())]
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.samples_decoded, 4);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_native_histogram_quantile_merges_sealed_and_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.native.mixed"),
            ("route", "/native-mixed"),
        ],
    );

    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            series,
            &[(
                1_001,
                HistogramValue {
                    count: 10,
                    sum: Some(20.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        reset_hint: CounterResetHint::NotCounterReset,
                        ..TypedSampleMetadata::default()
                    },
                    explicit_bounds: vec![1.0, 2.0, 4.0],
                    bucket_counts: vec![2, 5, 3, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.mixed");
                visit("route", "/native-mixed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let mut head = test_head();
    head.record_sample(
        series,
        6_000,
        SampleValue::Histogram(HistogramValue {
            count: 20,
            sum: Some(40.0),
            min: None,
            max: None,
            metadata: TypedSampleMetadata {
                reset_hint: CounterResetHint::NotCounterReset,
                ..TypedSampleMetadata::default()
            },
            explicit_bounds: vec![1.0, 2.0, 4.0],
            bucket_counts: vec![4, 10, 6, 0],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"histogram_quantile(0.5, rate(http.request.native.mixed{route="/native-mixed"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-12);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.samples_decoded, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_native_histogram_rate_uses_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(210),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 100,
                        sum: Some(200.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![20, 50, 30, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.reset");
                visit("route", "/native-reset");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.reset{route="/native-reset"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 1.6).abs() < 1e-12);
}

#[test]
fn promql_query_native_histogram_rate_stale_sample_splits_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(211),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 0,
                        sum: Some(0.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            flags: OTLP_FLAG_NO_RECORDED_VALUE,
                            reset_hint: CounterResetHint::Unknown,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![0, 0, 0, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.stale");
                visit("route", "/native-stale");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.stale{route="/native-stale"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_native_histogram_rate_clamps_extrapolation_after_stale_marker() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(215),
            &[
                (
                    3_000,
                    HistogramValue {
                        count: 0,
                        sum: Some(0.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            flags: OTLP_FLAG_NO_RECORDED_VALUE,
                            reset_hint: CounterResetHint::Unknown,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![0, 0, 0],
                    },
                ),
                (
                    4_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![10, 0, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![20, 0, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.stale.weighted");
                visit("route", "/native-stale-weighted");
                visit("instance", "after-stale");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(216),
            &[
                (
                    4_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![0, 10, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![0, 20, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.stale.weighted");
                visit("route", "/native-stale-weighted");
                visit("instance", "no-stale");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.stale.weighted{route="/native-stale-weighted"}[5s])))"#,
            0,
            7_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 7_000);
    let value = results[0].samples[0].1;
    assert!(
        (value - 1.1).abs() < 1e-12,
        "expected quantile 1.1 after stale-clamped extrapolation, got {value}"
    );
}

#[test]
fn promql_query_native_delta_histogram_rate_uses_delta_temporality() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(219),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 100,
                        sum: None,
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![100, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![0, 10],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.delta");
                visit("route", "/native-delta");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let native = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta{route="/native-delta"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();
    let projected = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta_bucket{route="/native-delta"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].samples, vec![(6_000, 1.0)]);
    assert_eq!(native, projected);
}

#[test]
fn promql_query_native_delta_histogram_rate_uses_single_interval() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let metadata = TypedSampleMetadata {
        start_time_ms: Some(1_000),
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(221),
            &[(
                6_000,
                HistogramValue {
                    count: 10,
                    sum: None,
                    min: None,
                    max: None,
                    metadata,
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![10, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.delta.single");
                visit("route", "/native-delta-single");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let native = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta.single{route="/native-delta-single"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();
    let projected = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta.single_bucket{route="/native-delta-single"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].samples, vec![(6_000, 0.5)]);
    assert_eq!(native, projected);
}

#[test]
fn promql_query_native_exponential_histogram_quantile_uses_exponential_interpolation() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(212),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: Some(12.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![2, 3],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: Some(24.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![4, 6],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist");
                visit("route", "/native-exphist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate(http.request.native.exphist{route="/native-exphist"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.powf(1.0 / 6.0);
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - expected).abs() < 1e-12);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_native_exponential_histogram_scalar_functions_read_rate_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(230),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![2, 3],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 15,
                        sum: Some(30.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![6, 9],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.scalar");
                visit("route", "/native-exphist-scalar");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let input = r#"rate(http.request.native.exphist.scalar{route="/native-exphist-scalar"}[5s])"#;
    let count = store
        .query_promql(&format!("histogram_count({input})"), 0, 6_000)
        .unwrap();
    let sum = store
        .query_promql(&format!("histogram_sum({input})"), 0, 6_000)
        .unwrap();
    let avg = store
        .query_promql(&format!("histogram_avg({input})"), 0, 6_000)
        .unwrap();

    let expected_count = 10_000.0 / 4_999.0;
    let expected_sum = 20_000.0 / 4_999.0;
    for results in [&count, &sum, &avg] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.as_ref(),
            &[("route".to_string(), "/native-exphist-scalar".to_string())]
        );
        assert_eq!(results[0].samples[0].0, 6_000);
    }
    assert!((count[0].samples[0].1 - expected_count).abs() < 1e-12);
    assert!((sum[0].samples[0].1 - expected_sum).abs() < 1e-12);
    assert!((avg[0].samples[0].1 - 2.0).abs() < 1e-12);
}

#[test]
fn promql_query_native_exponential_histogram_binary_vector_arithmetic_and_comparison() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, count, sum, positive_counts) in [
        (
            SeriesRef::new(233),
            "http.request.native.exphist.binary.left",
            25,
            25.0,
            vec![10, 15],
        ),
        (
            SeriesRef::new(234),
            "http.request.native.exphist.binary.right",
            7,
            7.0,
            vec![3, 4],
        ),
    ] {
        let samples = [(
            40_000,
            ExponentialHistogramValue {
                count,
                sum: Some(sum),
                min: None,
                max: None,
                metadata: TypedSampleMetadata {
                    reset_hint: CounterResetHint::NotCounterReset,
                    ..TypedSampleMetadata::default()
                },
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: positive_counts,
                },
                negative: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: Vec::new(),
                },
            },
        )];
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", "/native-exphist-vector-binary");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count_plus = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.binary.left{route="/native-exphist-vector-binary"} + http.request.native.exphist.binary.right{route="/native-exphist-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let sum_minus = store
        .query_promql(
            r#"histogram_sum(http.request.native.exphist.binary.left{route="/native-exphist-vector-binary"} - http.request.native.exphist.binary.right{route="/native-exphist-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let equal_left = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.binary.left{route="/native-exphist-vector-binary"} == http.request.native.exphist.binary.left{route="/native-exphist-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let not_equal = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.binary.left{route="/native-exphist-vector-binary"} != http.request.native.exphist.binary.right{route="/native-exphist-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let multiply = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.binary.left{route="/native-exphist-vector-binary"} * http.request.native.exphist.binary.right{route="/native-exphist-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();
    let greater_than = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.binary.left{route="/native-exphist-vector-binary"} > http.request.native.exphist.binary.right{route="/native-exphist-vector-binary"})"#,
            0,
            40_000,
        )
        .unwrap();

    for results in [&count_plus, &sum_minus, &equal_left, &not_equal] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.as_ref(),
            &[(
                "route".to_string(),
                "/native-exphist-vector-binary".to_string()
            )]
        );
        assert_eq!(results[0].samples[0].0, 40_000);
    }
    assert_eq!(count_plus[0].samples[0].1, 32.0);
    assert_eq!(sum_minus[0].samples[0].1, 18.0);
    assert_eq!(equal_left[0].samples[0].1, 25.0);
    assert_eq!(not_equal[0].samples[0].1, 25.0);
    assert!(multiply.is_empty());
    assert!(greater_than.is_empty());
}

#[test]
fn promql_query_native_exponential_histogram_sum_skips_stale_inputs() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, metadata, count, sum, positive_counts) in [
        (
            SeriesRef::new(222),
            "valid",
            TypedSampleMetadata::default(),
            6,
            Some(12.0),
            vec![2, 4],
        ),
        (
            SeriesRef::new(223),
            "stale",
            TypedSampleMetadata {
                flags: OTLP_FLAG_NO_RECORDED_VALUE,
                ..TypedSampleMetadata::default()
            },
            0,
            Some(0.0),
            vec![0, 0],
        ),
    ] {
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[(
                    5_000,
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
                            counts: positive_counts,
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                )],
                |visit| {
                    visit(
                        METRIC_NAME_LABEL,
                        "http.request.native.exphist.stale.aggregate",
                    );
                    visit("route", "/native-exphist-stale-sum");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_count(sum by (route)(http.request.native.exphist.stale.aggregate{route="/native-exphist-stale-sum"}))"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-exphist-stale-sum".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 6.0)]);
}

#[test]
fn promql_query_native_exponential_histogram_scalar_function_accepts_metric_name_with_projection_suffix()
 {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(225),
            &[(
                5_000,
                ExponentialHistogramValue {
                    count: 6,
                    sum: Some(12.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![2, 4],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.actual_sum");
                visit("route", "/native-exphist-suffix-name");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.actual_sum{route="/native-exphist-suffix-name"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/native-exphist-suffix-name".to_string()
        )]
    );
    assert_eq!(results[0].samples, vec![(10_000, 6.0)]);
}

#[test]
fn promql_query_native_exponential_histogram_scalar_functions_read_avg_without_rate_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(238), "a"), (SeriesRef::new(239), "b")] {
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        ExponentialHistogramValue {
                            count: 5,
                            sum: Some(10.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![2, 3],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                    (
                        6_000,
                        ExponentialHistogramValue {
                            count: 15,
                            sum: Some(30.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![6, 9],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                ],
                |visit| {
                    visit(
                        METRIC_NAME_LABEL,
                        "http.request.native.exphist.scalar.avg_without",
                    );
                    visit("route", "/native-exphist-scalar-avg-without");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let input = r#"avg without (instance)(rate(http.request.native.exphist.scalar.avg_without{route="/native-exphist-scalar-avg-without"}[5s]))"#;
    let count = store
        .query_promql(&format!("histogram_count({input})"), 0, 6_000)
        .unwrap();
    let sum = store
        .query_promql(&format!("histogram_sum({input})"), 0, 6_000)
        .unwrap();
    let avg = store
        .query_promql(&format!("histogram_avg({input})"), 0, 6_000)
        .unwrap();

    let expected_count = 10_000.0 / 4_999.0;
    let expected_sum = 20_000.0 / 4_999.0;
    for results in [&count, &sum, &avg] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.as_ref(),
            &[(
                "route".to_string(),
                "/native-exphist-scalar-avg-without".to_string()
            )]
        );
        assert_eq!(results[0].samples[0].0, 6_000);
    }
    assert!((count[0].samples[0].1 - expected_count).abs() < 1e-12);
    assert!((sum[0].samples[0].1 - expected_sum).abs() < 1e-12);
    assert!((avg[0].samples[0].1 - 2.0).abs() < 1e-12);
}

#[test]
fn promql_query_session_matches_native_exponential_histogram_quantile_store_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(213),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: Some(12.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![2, 3],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: Some(24.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![4, 6],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.session");
                visit("route", "/native-exphist-session");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let query = r#"histogram_quantile(0.5, rate(http.request.native.exphist.session{route="/native-exphist-session"}[5s]))"#;
    let limits = QueryLimits {
        max_projected_series: Some(1),
        ..QueryLimits::unlimited()
    };
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let expected = store
        .query_promql_with_limits(query, 0, 6_000, limits)
        .unwrap();
    let mut session = store.query_session().unwrap();
    let actual = session
        .query_promql_with_limits(query, 0, 6_000, limits)
        .unwrap();

    assert_eq!(actual.results, expected.results);
    assert_eq!(actual.stats.projected_series, 1);
    assert_eq!(actual.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_native_exponential_histogram_quantile_interpolates_negative_buckets_exponentially()
{
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(223),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 1,
                            counts: vec![5],
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 1,
                            counts: vec![10],
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.negative");
                visit("route", "/native-exphist-negative");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.exphist.negative{route="/native-exphist-negative"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    let expected = -2.0 * 2.0f64.sqrt();
    assert_eq!(execution.len(), 1);
    assert_eq!(execution[0].samples.len(), 1);
    assert_eq!(execution[0].samples[0].0, 6_000);
    assert!(
        (execution[0].samples[0].1 - expected).abs() < 1e-12,
        "expected native negative exponential histogram quantile {expected}, got {}",
        execution[0].samples[0].1
    );
}

#[test]
fn promql_query_native_exponential_histogram_quantile_zero_bucket_clamps_to_observed_side() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(224),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 3,
                        zero_threshold: 0.1,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![2],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 6,
                        zero_threshold: 0.1,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![4],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.zero.positive",
                );
                visit("route", "/native-exphist-zero-positive");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(225),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 3,
                        zero_threshold: 0.1,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![2],
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 6,
                        zero_threshold: 0.1,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![4],
                        },
                    },
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.zero.negative",
                );
                visit("route", "/native-exphist-zero-negative");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let positive = store
        .query_promql(
            r#"histogram_quantile(0.1, rate(http.request.native.exphist.zero.positive{route="/native-exphist-zero-positive"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();
    let negative = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.exphist.zero.negative{route="/native-exphist-zero-negative"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(positive.len(), 1);
    assert_eq!(positive[0].samples.len(), 1);
    assert!(
        (positive[0].samples[0].1 - (0.1 / 6.0)).abs() < 1e-12,
        "expected positive-only zero bucket to start at zero, got {}",
        positive[0].samples[0].1
    );
    assert_eq!(negative.len(), 1);
    assert_eq!(negative[0].samples.len(), 1);
    assert!(
        (negative[0].samples[0].1 - (-0.1 * 5.0 / 6.0)).abs() < 1e-12,
        "expected negative-only zero bucket to end at zero, got {}",
        negative[0].samples[0].1
    );
}

#[test]
fn promql_query_native_exponential_histogram_quantile_respects_zero_threshold_bucket_edges() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(226),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 1.5,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![5],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 1.5,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![10],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.threshold.positive",
                );
                visit("route", "/native-exphist-threshold-positive");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(227),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 1.5,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![5],
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 1.5,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![10],
                        },
                    },
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.threshold.negative",
                );
                visit("route", "/native-exphist-threshold-negative");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let positive = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.exphist.threshold.positive{route="/native-exphist-threshold-positive"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();
    let negative = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.exphist.threshold.negative{route="/native-exphist-threshold-negative"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    let expected = (1.5_f64 * 2.0).sqrt();
    assert_eq!(positive.len(), 1);
    assert_eq!(positive[0].samples.len(), 1);
    assert!(
        (positive[0].samples[0].1 - expected).abs() < 1e-12,
        "expected positive bucket lower bound to honor zero_threshold, got {}",
        positive[0].samples[0].1
    );
    assert_eq!(negative.len(), 1);
    assert_eq!(negative[0].samples.len(), 1);
    assert!(
        (negative[0].samples[0].1 + expected).abs() < 1e-12,
        "expected negative bucket upper bound to honor zero_threshold, got {}",
        negative[0].samples[0].1
    );
}

#[test]
fn promql_query_native_delta_exponential_histogram_rate_uses_delta_temporality() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(220),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 100,
                        sum: None,
                        min: None,
                        max: None,
                        metadata,
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![100, 0],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: None,
                        min: None,
                        max: None,
                        metadata,
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![0, 10],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.delta.exphist");
                visit("route", "/native-delta-exphist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta.exphist{route="/native-delta-exphist"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.sqrt();
    assert_eq!(execution.len(), 1);
    assert_eq!(execution[0].samples.len(), 1);
    assert_eq!(execution[0].samples[0].0, 6_000);
    assert!(
        (execution[0].samples[0].1 - expected).abs() < 1e-12,
        "expected native delta exponential histogram quantile {expected}, got {}",
        execution[0].samples[0].1
    );
}

#[test]
fn promql_query_native_delta_exponential_histogram_rate_uses_single_interval() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let metadata = TypedSampleMetadata {
        start_time_ms: Some(1_000),
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(222),
            &[(
                6_000,
                ExponentialHistogramValue {
                    count: 10,
                    sum: None,
                    min: None,
                    max: None,
                    metadata,
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![0, 10],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.delta.exphist.single",
                );
                visit("route", "/native-delta-exphist-single");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta.exphist.single{route="/native-delta-exphist-single"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.sqrt();
    assert_eq!(execution.len(), 1);
    assert_eq!(execution[0].samples.len(), 1);
    assert_eq!(execution[0].samples[0].0, 6_000);
    assert!(
        (execution[0].samples[0].1 - expected).abs() < 1e-12,
        "expected native single-interval delta exponential histogram quantile {expected}, got {}",
        execution[0].samples[0].1
    );
}

#[test]
fn promql_query_native_exponential_histogram_quantile_over_sum_by_rate_stays_native() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, first_counts, second_counts) in [
        (SeriesRef::new(213), "a", vec![2, 3], vec![4, 6]),
        (SeriesRef::new(214), "b", vec![1, 1], vec![3, 5]),
    ] {
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        ExponentialHistogramValue {
                            count: first_counts.iter().sum(),
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: first_counts,
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                    (
                        6_000,
                        ExponentialHistogramValue {
                            count: second_counts.iter().sum(),
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: second_counts,
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.exphist.agg");
                    visit("route", "/native-exphist-agg");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.exphist.agg{route="/native-exphist-agg"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.powf(3.0 / 14.0);
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - expected).abs() < 1e-12);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[("route".to_string(), "/native-exphist-agg".to_string())]
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_native_exponential_histogram_quantile_over_avg_by_rate_stays_native() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, first_counts, second_counts) in [
        (SeriesRef::new(215), "a", vec![2, 3], vec![4, 6]),
        (SeriesRef::new(216), "b", vec![1, 1], vec![3, 5]),
    ] {
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        ExponentialHistogramValue {
                            count: first_counts.iter().sum(),
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: first_counts,
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                    (
                        6_000,
                        ExponentialHistogramValue {
                            count: second_counts.iter().sum(),
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: second_counts,
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.exphist.avg");
                    visit("route", "/native-exphist-avg");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, avg by (route)(rate(http.request.native.exphist.avg{route="/native-exphist-avg"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.powf(3.0 / 14.0);
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - expected).abs() < 1e-12);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[("route".to_string(), "/native-exphist-avg".to_string())]
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_native_exponential_histogram_quantile_over_avg_without_rate_stays_native() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, first_counts, second_counts) in [
        (SeriesRef::new(240), "a", vec![2, 3], vec![4, 6]),
        (SeriesRef::new(241), "b", vec![1, 1], vec![3, 5]),
    ] {
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_001,
                        ExponentialHistogramValue {
                            count: first_counts.iter().sum(),
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: first_counts,
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                    (
                        6_000,
                        ExponentialHistogramValue {
                            count: second_counts.iter().sum(),
                            sum: None,
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            scale: 0,
                            zero_count: 0,
                            zero_threshold: 0.0,
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: second_counts,
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: Vec::new(),
                            },
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.exphist.avg_without");
                    visit("route", "/native-exphist-avg-without");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, avg without (instance)(rate(http.request.native.exphist.avg_without{route="/native-exphist-avg-without"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.powf(3.0 / 14.0);
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - expected).abs() < 1e-12);
    assert_eq!(
        execution.results[0].labels.as_ref(),
        &[(
            "route".to_string(),
            "/native-exphist-avg-without".to_string()
        )]
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_native_exponential_histogram_quantile_empty_rate_preserves_native_stats() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(217),
            &[(
                6_000,
                ExponentialHistogramValue {
                    count: 4,
                    sum: None,
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        reset_hint: CounterResetHint::NotCounterReset,
                        ..TypedSampleMetadata::default()
                    },
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![1, 3],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.single");
                visit("route", "/native-exphist-single");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate(http.request.native.exphist.single{route="/native-exphist-single"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert!(execution.results.is_empty());
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_native_exponential_histogram_rate_clamps_extrapolation_after_stale_marker() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(217),
            &[
                (
                    3_000,
                    ExponentialHistogramValue {
                        count: 0,
                        sum: Some(0.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            flags: OTLP_FLAG_NO_RECORDED_VALUE,
                            reset_hint: CounterResetHint::Unknown,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![0, 0],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    4_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![10, 0],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 20,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![20, 0],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.stale.weighted",
                );
                visit("route", "/native-exphist-stale-weighted");
                visit("instance", "after-stale");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(218),
            &[
                (
                    4_000,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![0, 10],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![0, 20],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.stale.weighted",
                );
                visit("route", "/native-exphist-stale-weighted");
                visit("instance", "no-stale");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.exphist.stale.weighted{route="/native-exphist-stale-weighted"}[5s])))"#,
            0,
            7_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    let expected = 2.0 * 2.0f64.powf(0.1);
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 7_000);
    let value = execution.results[0].samples[0].1;
    assert!(
        (value - expected).abs() < 1e-12,
        "expected quantile {expected} after stale-clamped extrapolation, got {value}"
    );
    assert_eq!(execution.stats.projected_series, 2);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 2);
}

#[test]
fn promql_query_increase_uses_histogram_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(73),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![10, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 12,
                        sum: Some(12.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![12, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.reset");
                visit("route", "/hist-reset");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.request.reset_count{route="/hist-reset"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    let expected = 12.0 * 5_000.0 / 4_999.0;
    assert!(
        (results[0].samples[0].1 - expected).abs() < 1e-12,
        "expected reset-aware histogram count increase {expected}, got {}",
        results[0].samples[0].1
    );
}

#[test]
fn promql_query_increase_uses_histogram_reset_hints_after_stale_marker() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(76),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![10, 0],
                    },
                ),
                (
                    2_000,
                    HistogramValue {
                        count: 0,
                        sum: Some(0.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            flags: OTLP_FLAG_NO_RECORDED_VALUE,
                            reset_hint: CounterResetHint::Unknown,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![0, 0],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 0,
                        sum: Some(0.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![0, 0],
                    },
                ),
                (
                    4_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(4.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![4, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.stale_reset");
                visit("route", "/hist-stale-counter");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.request.stale_reset_count{route="/hist-stale-counter"}[4s])"#,
            0,
            4_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(4_000, 4.0)]);
}

#[test]
fn promql_query_increase_uses_histogram_bucket_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(74),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 20,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![10, 10, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![12, 8, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.bucket.reset");
                visit("route", "/hist-bucket-reset");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.request.bucket.reset_bucket{route="/hist-bucket-reset", le="1"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    let expected = 12.0 * 5_000.0 / 4_999.0;
    assert!(
        (results[0].samples[0].1 - expected).abs() < 1e-12,
        "expected reset-aware histogram bucket increase {expected}, got {}",
        results[0].samples[0].1
    );
}

#[test]
fn promql_query_rate_uses_active_head_exponential_histogram_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.reset.size"),
            ("route", "/exphist-reset"),
        ],
    );
    let mut head = test_head();

    for (ts, count, reset_hint) in [
        (1_001, 20, CounterResetHint::NotCounterReset),
        (6_000, 25, CounterResetHint::CounterReset),
    ] {
        head.record_sample(
            series,
            ts,
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count,
                sum: Some(count as f64),
                min: None,
                max: None,
                metadata: TypedSampleMetadata {
                    reset_hint,
                    ..TypedSampleMetadata::default()
                },
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
            }),
        )
        .unwrap();
    }

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rate(http.request.reset.size_count{route="/exphist-reset"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    let expected = 25_000.0 / 4_999.0;
    assert!(
        (results[0].samples[0].1 - expected).abs() < 1e-12,
        "expected reset-aware exponential histogram count rate {expected}, got {}",
        results[0].samples[0].1
    );
}

#[test]
fn promql_query_projects_classic_histogram_from_native_segment_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(31);
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            series,
            &[(
                5_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let bucket = store
        .query_promql(r#"http.request.duration_bucket{le="5"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(5_000, 3.0)]);
    assert!(
        bucket[0]
            .labels
            .iter()
            .any(|(key, value)| key == "le" && value == "5")
    );

    let inf_bucket = store
        .query_promql(r#"http.request.duration_bucket{le="+Inf"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(inf_bucket.len(), 1);
    assert_eq!(inf_bucket[0].samples, vec![(5_000, 4.0)]);

    let count = store
        .query_promql(r#"http.request.duration_count{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(5_000, 4.0)]);

    let sum = store
        .query_promql(r#"http.request.duration_sum{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(5_000, 10.0)]);
}

#[test]
fn promql_query_native_histogram_bucket_le_uses_promql_float_label_spelling() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(131);
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            series,
            &[(
                5_000,
                HistogramValue {
                    count: 6,
                    sum: Some(10.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![0.00001, 1_000_000.0],
                    bucket_counts: vec![1, 2, 3],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/bucket-format");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"http.request.duration_bucket{route="/bucket-format"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(
        samples_by_route_and_le(&results),
        BTreeMap::from([
            (
                ("/bucket-format".to_string(), "+Inf".to_string()),
                vec![(5_000, 6.0)]
            ),
            (
                ("/bucket-format".to_string(), "1e+06".to_string()),
                vec![(5_000, 3.0)]
            ),
            (
                ("/bucket-format".to_string(), "1e-05".to_string()),
                vec![(5_000, 1.0)]
            ),
        ])
    );

    let small_bucket = store
        .query_promql(
            r#"http.request.duration_bucket{route="/bucket-format",le="1e-05"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(small_bucket.len(), 1);
    assert_eq!(small_bucket[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_count_and_sum_use_typed_scalar_chunk_decode() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(131),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(10.0),
                        min: Some(1.0),
                        max: Some(4.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 5.0, 10.0],
                        bucket_counts: vec![1, 2, 1, 0],
                    },
                ),
                (
                    2_000,
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
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/scalar-decode");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql_with_limits(
            r#"http.request.duration_count{route="/scalar-decode"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(count.results.len(), 1);
    assert_eq!(count.results[0].samples, vec![(1_000, 4.0), (2_000, 7.0)]);
    assert_eq!(count.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(count.stats.typed_full_chunks_decoded, 0);

    let sum = store
        .query_promql_with_limits(
            r#"http.request.duration_sum{route="/scalar-decode"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(sum.results.len(), 1);
    assert_eq!(sum.results[0].samples, vec![(1_000, 10.0), (2_000, 21.0)]);
    assert_eq!(sum.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(sum.stats.typed_full_chunks_decoded, 0);

    let bucket = store
        .query_promql_with_limits(
            r#"http.request.duration_bucket{route="/scalar-decode",le="+Inf"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(bucket.results.len(), 1);
    assert_eq!(bucket.results[0].samples, vec![(1_000, 4.0), (2_000, 7.0)]);
    assert_eq!(bucket.stats.typed_scalar_chunks_decoded, 0);
    assert_eq!(bucket.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_count_reads_indexed_scalar_lane_instead_of_full_typed_chunk() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let explicit_bounds = (0..256).map(|value| value as f64).collect::<Vec<_>>();
    let bucket_counts = vec![1; explicit_bounds.len() + 1];
    let count = bucket_counts.iter().sum::<u64>();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(131),
            &[(
                1_000,
                HistogramValue {
                    count,
                    sum: Some(32768.0),
                    min: Some(0.0),
                    max: Some(256.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds,
                    bucket_counts,
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "wide.histogram");
                visit("route", "/indexed-scalar-lane");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count_query = store
        .query_promql_with_limits(
            r#"wide.histogram_count{route="/indexed-scalar-lane"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(count_query.results.len(), 1);
    assert_eq!(count_query.results[0].samples, vec![(1_000, count as f64)]);
    assert_eq!(count_query.stats.chunk_reads, 1);
    assert_eq!(count_query.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(count_query.stats.typed_full_chunks_decoded, 0);

    let bucket_query = store
        .query_promql_with_limits(
            r#"wide.histogram_bucket{route="/indexed-scalar-lane",le="+Inf"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(bucket_query.results.len(), 1);
    assert_eq!(bucket_query.results[0].samples, vec![(1_000, count as f64)]);
    assert_eq!(bucket_query.stats.chunk_reads, 1);
    assert_eq!(bucket_query.stats.typed_scalar_chunks_decoded, 0);
    assert_eq!(bucket_query.stats.typed_full_chunks_decoded, 1);
    assert!(
        count_query.stats.bytes_read < bucket_query.stats.bytes_read,
        "count projection should read indexed scalar lane bytes, not full chunk bytes: count={} bucket={}",
        count_query.stats.bytes_read,
        bucket_query.stats.bytes_read
    );
}

#[test]
fn promql_query_count_and_sum_metric_name_regex_use_typed_scalar_chunk_decode() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(132),
            &[
                (
                    1_000,
                    SummaryValue {
                        count: 4,
                        sum: 10.0,
                        metadata: TypedSampleMetadata::default(),
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: 2.5,
                        }],
                    },
                ),
                (
                    2_000,
                    SummaryValue {
                        count: 7,
                        sum: 21.0,
                        metadata: TypedSampleMetadata::default(),
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: 3.0,
                        }],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "rpc.duration");
                visit("route", "/regex-scalar-decode");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql_with_limits(
            r#"{__name__=~"rpc_duration.*_count",route="/regex-scalar-decode"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(count.results.len(), 1);
    assert_eq!(count.results[0].samples, vec![(1_000, 4.0), (2_000, 7.0)]);
    assert_eq!(count.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(count.stats.typed_full_chunks_decoded, 0);

    let sum = store
        .query_promql_with_limits(
            r#"{__name__=~"rpc_duration.*_sum",route="/regex-scalar-decode"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(sum.results.len(), 1);
    assert_eq!(sum.results[0].samples, vec![(1_000, 10.0), (2_000, 21.0)]);
    assert_eq!(sum.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(sum.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_bucket_metric_name_regex_keeps_full_typed_chunk_decode() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(133),
            &[(
                1_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/regex-bucket-decode");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let bucket = store
        .query_promql_with_limits(
            r#"{__name__=~"http_request_duration.*_bucket",route="/regex-bucket-decode"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(bucket.results.len(), 3);
    assert_eq!(bucket.stats.typed_scalar_chunks_decoded, 0);
    assert_eq!(bucket.stats.typed_full_chunks_decoded, 1);
}

#[test]
fn promql_query_scalar_count_decode_accumulates_delta_counts_before_f64_projection() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Delta,
        ..TypedSampleMetadata::default()
    };
    let large_count = (1u64 << 53) + 1;

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(132),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: large_count,
                        sum: None,
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: Vec::new(),
                        bucket_counts: vec![large_count],
                    },
                ),
                (
                    2_000,
                    HistogramValue {
                        count: 1,
                        sum: None,
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: Vec::new(),
                        bucket_counts: vec![1],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "large.delta.histogram");
                visit("route", "/scalar-count");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql_with_limits(
            r#"large.delta.histogram_count{route="/scalar-count"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(count.results.len(), 1);
    assert_eq!(
        count.results[0].samples,
        vec![
            (1_000, large_count as f64),
            (2_000, large_count.saturating_add(1) as f64),
        ]
    );
    assert_eq!(count.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(count.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_count_name_returns_real_scalar_and_virtual_histogram_count() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(133),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.request.duration_count".to_string(),
            ),
            ("route".to_string(), "/collision".to_string()),
            ("source".to_string(), "real".to_string()),
        ],
        &[(1_000, 42.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(139),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.request.duration".to_string(),
            ),
            ("route".to_string(), "/collision".to_string()),
            ("source".to_string(), "scalar-base".to_string()),
        ],
        &[(1_000, 99.0)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(134),
            &[(
                1_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/collision");
                visit("source", "hist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"http.request.duration_count{route="/collision"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    let by_source = samples_by_label(&execution.results, "source");
    assert_eq!(by_source["real"], vec![(1_000, 42.0)]);
    assert_eq!(by_source["hist"], vec![(1_000, 4.0)]);
    assert!(!by_source.contains_key("scalar-base"));
    assert_eq!(
        execution.stats.matched_series, 2,
        "real foo_count and native typed foo should match; scalar foo should be kind-pruned"
    );
    assert_eq!(execution.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_count_name_rejects_real_and_virtual_same_labelset_conflict() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(682),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http_request_conflict_count".to_string(),
            ),
            ("route".to_string(), "/same-labelset-count".to_string()),
        ],
        &[(1_000, 42.0)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(683),
            &[(
                1_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http_request_conflict");
                visit("route", "/same-labelset-count");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql(
            r#"http_request_conflict_count{route="/same-labelset-count"}"#,
            0,
            10_000,
        )
        .unwrap_err();

    match err {
        PromqlQueryError::Invalid(message) => {
            assert!(
                message.contains("conflicting real and virtual PromQL series"),
                "unexpected conflict message: {message}"
            );
        }
        other => panic!("expected invalid conflict error, got {other:?}"),
    }
}

#[test]
fn promql_query_sum_name_returns_real_scalar_and_virtual_histogram_sum() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(135),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.request.duration_sum".to_string(),
            ),
            ("route".to_string(), "/sum-collision".to_string()),
            ("source".to_string(), "real".to_string()),
        ],
        &[(1_000, 45.0)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(136),
            &[(
                1_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/sum-collision");
                visit("source", "hist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"http.request.duration_sum{route="/sum-collision"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&execution.results, "source")["real"],
        vec![(1_000, 45.0)]
    );
    assert_eq!(
        samples_by_label(&execution.results, "source")["hist"],
        vec![(1_000, 10.0)]
    );
    assert_eq!(execution.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_count_name_matcher_returns_real_scalar_and_virtual_histogram_count() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(135),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "http.request.duration_count".to_string(),
            ),
            ("route".to_string(), "/name-matcher-collision".to_string()),
            ("source".to_string(), "real".to_string()),
        ],
        &[(1_000, 43.0)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(136),
            &[(
                1_000,
                HistogramValue {
                    count: 5,
                    sum: Some(11.0),
                    min: Some(1.0),
                    max: Some(5.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 3, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/name-matcher-collision");
                visit("source", "hist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"{__name__="http.request.duration_count",route="/name-matcher-collision"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&execution.results, "source")["real"],
        vec![(1_000, 43.0)]
    );
    assert_eq!(
        samples_by_label(&execution.results, "source")["hist"],
        vec![(1_000, 5.0)]
    );
    assert_eq!(execution.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 0);
}

#[test]
fn promql_query_count_name_regex_returns_real_scalar_and_virtual_histogram_count() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(137),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "rpc_duration_count".to_string(),
            ),
            ("route".to_string(), "/regex-collision".to_string()),
            ("source".to_string(), "real".to_string()),
        ],
        &[(1_000, 46.0)],
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(138),
            &[(
                1_000,
                HistogramValue {
                    count: 7,
                    sum: Some(14.0),
                    min: Some(1.0),
                    max: Some(7.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 5, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "rpc_duration");
                visit("route", "/regex-collision");
                visit("source", "hist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"{__name__=~"rpc_duration_count",route="/regex-collision"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&execution.results, "source")["real"],
        vec![(1_000, 46.0)]
    );
    assert_eq!(
        samples_by_label(&execution.results, "source")["hist"],
        vec![(1_000, 7.0)]
    );
}

#[test]
fn promql_query_with_head_count_name_returns_real_scalar_and_virtual_histogram_count() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let real_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration_count"),
            ("route", "/head-collision"),
            ("source", "real"),
        ],
    );
    let histogram_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration"),
            ("route", "/head-collision"),
            ("source", "hist"),
        ],
    );
    let mut head = test_head();
    head.record_sample(real_series, 1_000, SampleValue::Float(44.0))
        .unwrap();
    head.record_sample(
        histogram_series,
        1_000,
        SampleValue::Histogram(HistogramValue {
            count: 6,
            sum: Some(12.0),
            min: Some(1.0),
            max: Some(6.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0],
            bucket_counts: vec![1, 4, 1],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"http.request.duration_count{route="/head-collision"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&execution.results, "source")["real"],
        vec![(1_000, 44.0)]
    );
    assert_eq!(
        samples_by_label(&execution.results, "source")["hist"],
        vec![(1_000, 6.0)]
    );
}

#[test]
fn promql_query_projects_stale_histogram_sample_as_stale_nan() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(33),
            &[(
                5_000,
                HistogramValue {
                    count: 0,
                    sum: Some(0.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(1_000),
                        flags: OTLP_FLAG_NO_RECORDED_VALUE,
                        temporality: OtlpAggregationTemporality::Cumulative,
                        reset_hint: CounterResetHint::Unknown,
                    },
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![0, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/stale");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql(r#"http.request.duration_count{route="/stale"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples.len(), 1);
    assert_eq!(count[0].samples[0].0, 5_000);
    assert_eq!(
        count[0].samples[0].1.to_bits(),
        prometheus_stale_nan().to_bits()
    );

    let bucket = store
        .query_promql(
            r#"http.request.duration_bucket{route="/stale", le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples.len(), 1);
    assert_eq!(bucket[0].samples[0].0, 5_000);
    assert_eq!(
        bucket[0].samples[0].1.to_bits(),
        prometheus_stale_nan().to_bits()
    );
}

#[test]
fn promql_query_projects_delta_histogram_as_cumulative_virtual_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let metadata = TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(34),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(5.0),
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                ),
                (
                    2_000,
                    HistogramValue {
                        count: 3,
                        sum: Some(7.0),
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 1],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql(r#"http.request.duration_count{route="/delta"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(1_000, 2.0), (2_000, 5.0)]);

    let sum = store
        .query_promql(r#"http.request.duration_sum{route="/delta"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(1_000, 5.0), (2_000, 12.0)]);

    let bucket = store
        .query_promql(
            r#"http.request.duration_bucket{route="/delta", le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(1_000, 2.0), (2_000, 5.0)]);
}

#[test]
fn promql_query_last_over_time_delta_histogram_count_uses_cumulative_projection_before_range_start()
{
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let metadata = TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };

    let samples = [
        (
            0,
            HistogramValue {
                count: 5,
                sum: Some(5.0),
                min: None,
                max: None,
                metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![2, 3],
            },
        ),
        (
            10_000,
            HistogramValue {
                count: 5,
                sum: Some(5.0),
                min: None,
                max: None,
                metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![2, 3],
            },
        ),
        (
            20_000,
            HistogramValue {
                count: 5,
                sum: Some(5.0),
                min: None,
                max: None,
                metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![2, 3],
            },
        ),
        (
            30_000,
            HistogramValue {
                count: 5,
                sum: Some(5.0),
                min: None,
                max: None,
                metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![2, 3],
            },
        ),
        (
            40_000,
            HistogramValue {
                count: 5,
                sum: Some(5.0),
                min: None,
                max: None,
                metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![2, 3],
            },
        ),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(35),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta-window");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql(
            r#"last_over_time(http.request.duration_count{route="/delta-window"}[30s])"#,
            0,
            40_000,
        )
        .unwrap();

    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(40_000, 25.0)]);
}

#[test]
fn promql_query_delta_histogram_count_rate_merges_sealed_and_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let metadata = TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(41),
            &[(
                1_000,
                HistogramValue {
                    count: 100,
                    sum: None,
                    min: None,
                    max: None,
                    metadata,
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![100, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta-cross-head");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration"),
            ("route", "/delta-cross-head"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        series,
        6_000,
        SampleValue::Histogram(HistogramValue {
            count: 10,
            sum: None,
            min: None,
            max: None,
            metadata,
            explicit_bounds: vec![1.0],
            bucket_counts: vec![0, 10],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rate(http.request.duration_count{route="/delta-cross-head"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_delta_histogram_bucket_rate_merges_sealed_and_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let metadata = TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(42),
            &[(
                1_000,
                HistogramValue {
                    count: 100,
                    sum: None,
                    min: None,
                    max: None,
                    metadata,
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![100, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta-bucket-cross-head");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration"),
            ("route", "/delta-bucket-cross-head"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        series,
        6_000,
        SampleValue::Histogram(HistogramValue {
            count: 10,
            sum: None,
            min: None,
            max: None,
            metadata,
            explicit_bounds: vec![1.0],
            bucket_counts: vec![0, 10],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rate(http.request.duration_bucket{route="/delta-bucket-cross-head",le="+Inf"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_delta_histogram_count_rate_uses_single_interval() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(43),
            &[(
                6_000,
                HistogramValue {
                    count: 10,
                    sum: None,
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(1_000),
                        flags: 0,
                        temporality: OtlpAggregationTemporality::Delta,
                        reset_hint: CounterResetHint::NotCounterReset,
                    },
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![2, 8],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta-single-interval");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"rate(http.request.duration_count{route="/delta-single-interval"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_delta_histogram_bucket_rate_uses_single_interval() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(44),
            &[(
                6_000,
                HistogramValue {
                    count: 10,
                    sum: None,
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(1_000),
                        flags: 0,
                        temporality: OtlpAggregationTemporality::Delta,
                        reset_hint: CounterResetHint::NotCounterReset,
                    },
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![2, 8],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta-single-bucket-interval");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"rate(http.request.duration_bucket{route="/delta-single-bucket-interval",le="+Inf"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_head_delta_histogram_count_rate_uses_single_interval() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration"),
            ("route", "/head-delta-single-interval"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        series,
        6_000,
        SampleValue::Histogram(HistogramValue {
            count: 10,
            sum: None,
            min: None,
            max: None,
            metadata: TypedSampleMetadata {
                start_time_ms: Some(1_000),
                flags: 0,
                temporality: OtlpAggregationTemporality::Delta,
                reset_hint: CounterResetHint::NotCounterReset,
            },
            explicit_bounds: vec![1.0],
            bucket_counts: vec![2, 8],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rate(http.request.duration_count{route="/head-delta-single-interval"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_delta_exponential_histogram_count_rate_uses_single_interval() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(45),
            &[(
                6_000,
                ExponentialHistogramValue {
                    count: 10,
                    sum: None,
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(1_000),
                        flags: 0,
                        temporality: OtlpAggregationTemporality::Delta,
                        reset_hint: CounterResetHint::NotCounterReset,
                    },
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![10],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.size");
                visit("route", "/delta-exphist-single-interval");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"rate(http.request.size_count{route="/delta-exphist-single-interval"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_projects_exponential_histogram_bucket_from_native_segment_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(35),
            &[(
                5_000,
                ExponentialHistogramValue {
                    count: 5,
                    sum: Some(12.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![2, 3],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.size");
                visit("route", "/exphist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let default_store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let default_bucket = default_store
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert!(default_bucket.is_empty());

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default()
            .with_exponential_histogram_bucket_boundaries(vec![2.0, 4.0]),
    )
    .unwrap();
    let bucket = store
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(5_000, 2.0)]);
    assert!(
        bucket[0]
            .labels
            .iter()
            .any(|(key, value)| key == "le" && value == "2")
    );
    let mut session = store.query_session().unwrap();
    let session_bucket = session
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(session_bucket, bucket);

    let inf_bucket = store
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(inf_bucket.len(), 1);
    assert_eq!(inf_bucket[0].samples, vec![(5_000, 5.0)]);

    let all_buckets = store
        .query_promql(r#"http.request.size_bucket{route="/exphist"}"#, 0, 10_000)
        .unwrap();
    let mut bucket_labels: Vec<_> = all_buckets
        .iter()
        .map(|result| {
            result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "le").then_some(value.as_str()))
                .unwrap()
        })
        .collect();
    bucket_labels.sort_unstable();
    assert_eq!(bucket_labels, vec!["+Inf", "2", "4"]);
}

#[test]
fn promql_query_projects_delta_exponential_histogram_bucket_from_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.size"),
            ("route", "/delta-exphist"),
        ],
    );
    let mut head = test_head();
    let metadata = TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    for (ts, counts) in [(1_000, vec![1, 1]), (2_000, vec![2, 1])] {
        head.record_sample(
            series,
            ts,
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count: counts.iter().sum(),
                sum: None,
                min: None,
                max: None,
                metadata,
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: ExponentialHistogramBuckets { offset: 0, counts },
                negative: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: Vec::new(),
                },
            }),
        )
        .unwrap();
    }

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();
    let bucket = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"http.request.size_bucket{route="/delta-exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(1_000, 1.0), (2_000, 3.0)]);
}

#[test]
fn promql_query_projects_summary_from_native_segment_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(32);
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_summary_samples_ordered_with_label_visitor(
            series,
            &[(
                5_000,
                SummaryValue {
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
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "rpc.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let quantile = store
        .query_promql(r#"rpc.duration{quantile="0.9"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(quantile.len(), 1);
    assert_eq!(quantile[0].samples, vec![(5_000, 8.0)]);
    assert!(
        quantile[0]
            .labels
            .iter()
            .any(|(key, value)| key == "quantile" && value == "0.9")
    );

    let count = store
        .query_promql(r#"rpc.duration_count{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(5_000, 10.0)]);

    let sum = store
        .query_promql(r#"rpc.duration_sum{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(5_000, 50.0)]);
}

#[test]
fn promql_query_projects_summary_series_matched_by_metric_name_regex() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(32);
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_summary_samples_ordered_with_label_visitor(
            series,
            &[(
                5_000,
                SummaryValue {
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
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "rpc.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut results = store
        .query_promql(r#"{__name__=~"rpc_duration.*",route="/typed"}"#, 0, 10_000)
        .unwrap();
    results.sort_by(|left, right| left.labels.cmp(&right.labels));

    let mut projected = results
        .into_iter()
        .map(|result| {
            let name = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.clone()))
                .unwrap();
            let quantile = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "quantile").then_some(value.clone()));
            (name, quantile, result.samples)
        })
        .collect::<Vec<_>>();
    projected.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    let metric_name = normalize_metric_name("rpc.duration");

    assert_eq!(
        projected,
        vec![
            (
                metric_name.clone(),
                Some("0.5".to_string()),
                vec![(5_000, 4.0)]
            ),
            (
                metric_name.clone(),
                Some("0.9".to_string()),
                vec![(5_000, 8.0)]
            ),
            (format!("{metric_name}_count"), None, vec![(5_000, 10.0)]),
            (format!("{metric_name}_sum"), None, vec![(5_000, 50.0)]),
        ]
    );

    let count_only = store
        .query_promql(r#"{__name__=~".*_count",route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count_only.len(), 1);
    assert_eq!(count_only[0].samples, vec![(5_000, 10.0)]);
    assert!(count_only[0].labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &format!("{metric_name}_count")
    }));
}

#[test]
fn promql_query_projected_metric_name_regex_is_fully_anchored() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (idx, (metric_name, count)) in [("rpc_duration", 10), ("rpc_duration_count_extra", 20)]
        .into_iter()
        .enumerate()
    {
        writer
            .record_summary_samples_ordered_with_label_visitor(
                SeriesRef::new(idx as u32 + 40),
                &[(
                    5_000,
                    SummaryValue {
                        count,
                        sum: count as f64,
                        metadata: TypedSampleMetadata::default(),
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.9,
                            value: count as f64,
                        }],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, metric_name);
                    visit("route", "/typed");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"{__name__=~"rpc_duration_count",route="/typed"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 10.0)]);
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| { key == METRIC_NAME_LABEL && value == "rpc_duration_count" })
    );
}

#[test]
fn promql_query_projects_typed_samples_from_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let histogram_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration"),
            ("route", "/typed"),
        ],
    );
    let summary_series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "rpc.duration"), ("route", "/typed")],
    );
    let mut head = test_head();
    head.record_sample(
        histogram_series,
        5_000,
        SampleValue::Histogram(HistogramValue {
            count: 4,
            sum: Some(10.0),
            min: Some(1.0),
            max: Some(4.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0],
            bucket_counts: vec![1, 2, 1],
        }),
    )
    .unwrap();
    head.record_sample(
        summary_series,
        5_000,
        SampleValue::Summary(SummaryValue {
            count: 10,
            sum: 50.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![SummaryQuantileValue {
                quantile: 0.9,
                value: 8.0,
            }],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let bucket = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"http.request.duration_bucket{le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(5_000, 4.0)]);

    let quantile = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rpc.duration{quantile="0.9"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(quantile.len(), 1);
    assert_eq!(quantile[0].samples, vec![(5_000, 8.0)]);

    let mut projected = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"{__name__=~"rpc_duration.*",route="/typed"}"#,
            0,
            10_000,
        )
        .unwrap()
        .into_iter()
        .map(|result| {
            let name = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.clone()))
                .unwrap();
            let quantile = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "quantile").then_some(value.clone()));
            (name, quantile, result.samples)
        })
        .collect::<Vec<_>>();
    projected.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    let summary_metric_name = normalize_metric_name("rpc.duration");
    assert_eq!(
        projected,
        vec![
            (
                summary_metric_name.clone(),
                Some("0.9".to_string()),
                vec![(5_000, 8.0)]
            ),
            (
                format!("{summary_metric_name}_count"),
                None,
                vec![(5_000, 10.0)]
            ),
            (
                format!("{summary_metric_name}_sum"),
                None,
                vec![(5_000, 50.0)]
            ),
        ]
    );

    let head_count_only = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"{__name__=~"rpc_duration.*_count",route="/typed"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(head_count_only.len(), 1);
    assert_eq!(head_count_only[0].samples, vec![(5_000, 10.0)]);
    assert!(head_count_only[0].labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &format!("{summary_metric_name}_count")
    }));
}

#[test]
fn promql_query_supports_brace_only_metric_name_and_inequality() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend_1 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let backend_2 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-2")],
    );
    let missing_pod = labels(&mut label_store, &[(METRIC_NAME_LABEL, "cpu.usage")]);

    let raw_backend_1 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let raw_backend_2 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-2".to_string()),
    ];
    let raw_missing_pod = vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())];

    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(backend_1, &raw_backend_1, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(backend_2, &raw_backend_2, &[(5_000, 2.0)])
        .unwrap();
    writer
        .record_samples_with_labels(missing_pod, &raw_missing_pod, &[(5_000, 3.0)])
        .unwrap();
    writer.flush().unwrap();

    let head = test_head();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"{__name__="cpu.usage",pod.name!="backend-1"}"#,
            0,
            10_000,
        )
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn promql_query_supports_positive_regex_matchers() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(3),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "frontend-1".to_string()),
        ],
        &[(5_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu.usage{pod.name=~"backend-[12]"}"#, 0, 10_000)
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![1.0, 2.0]);
}

#[test]
fn promql_query_supports_negative_regex_and_includes_missing_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "frontend-1".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(3),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu.usage{pod.name!~"backend-.*"}"#, 0, 10_000)
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn promql_query_combines_equality_and_regex_matchers() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("namespace".to_string(), "default".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("namespace".to_string(), "other".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu.usage{namespace="default",pod.name=~"backend-.*"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_regex_matchers_are_fully_anchored_for_sealed_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for (idx, pod) in ["aaafoobar", "foo", "foobar"].into_iter().enumerate() {
        write_series(
            &mut writer,
            SeriesRef::new(idx as u32 + 1),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), pod.to_string()),
            ],
            &[(5_000, idx as f64 + 1.0)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let exact_regex = store
        .query_promql(r#"cpu.usage{pod.name=~"foo"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sorted_first_sample_values(&exact_regex), vec![2.0]);

    let prefix_regex = store
        .query_promql(r#"cpu.usage{pod.name=~"foo.*"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sorted_first_sample_values(&prefix_regex), vec![2.0, 3.0]);

    let suffix_regex = store
        .query_promql(r#"cpu.usage{pod.name=~".*bar"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sorted_first_sample_values(&suffix_regex), vec![1.0, 3.0]);

    let err = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name=~".*bar"}"#,
            0,
            10_000,
            QueryLimits {
                max_regex_values_examined: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();
    assert_limit_exceeded(err, "regex_values_examined", 1);
}

#[test]
fn promql_query_supports_metric_name_regex_matcher() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![(METRIC_NAME_LABEL.to_string(), "memory.usage".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"{__name__=~"cpu_.*"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_returns_invalid_for_bad_regex() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let err = store
        .query_promql(r#"cpu.usage{pod.name=~"["}"#, 0, 10_000)
        .unwrap_err();

    assert!(matches!(err, PromqlQueryError::Invalid(_)));
}

#[test]
fn promql_query_supports_active_head_regex() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let frontend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "frontend-1")],
    );
    let mut head = test_head();
    head.record_sample(backend, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(frontend, 5_000, SampleValue::Float(2.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~"backend-.*"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_regex_matchers_are_fully_anchored_for_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let foo = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "foo")],
    );
    let foobar = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "foobar")],
    );
    let mut head = test_head();
    head.record_sample(foo, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(foobar, 5_000, SampleValue::Float(2.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let exact_regex = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~"foo"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(sorted_first_sample_values(&exact_regex), vec![1.0]);

    let prefix_regex = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~"foo.*"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(sorted_first_sample_values(&prefix_regex), vec![1.0, 2.0]);
}

#[test]
fn promql_query_with_limits_returns_stats_for_successful_sealed_query() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0), (6_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].samples,
        vec![(5_000, 1.0), (6_000, 2.0)]
    );
    assert_eq!(execution.stats.matched_series, 1);
    assert_eq!(execution.stats.chunk_reads, 1);
    assert!(execution.stats.bytes_read > 0);
    assert_eq!(execution.stats.samples_decoded, 2);
    assert_eq!(execution.stats.regex_values_examined, 0);
}

#[test]
fn promql_query_session_matches_store_results_and_stats_across_repeated_queries() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0), (6_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let expected_first = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    let expected_second = store
        .query_promql_with_limits("cpu.usage", 0, 10_000, QueryLimits::unlimited())
        .unwrap();

    let mut session = store.query_session().unwrap();
    let actual_first = session
        .query_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    let actual_second = session
        .query_promql_with_limits("cpu.usage", 0, 10_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(actual_first, expected_first);
    assert_eq!(actual_second, expected_second);
}

#[test]
fn promql_query_session_enforces_query_limits() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0), (6_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let err = session
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_samples_decoded: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "samples_decoded", 1);
}

#[test]
fn promql_query_session_does_not_open_non_overlapping_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(25_000, 2.0)],
    );
    writer.flush().unwrap();

    let non_overlapping = segment_dir_with_start(tempdir.path(), 20_000);
    fs::remove_file(non_overlapping.join(SegmentFile::Symbols.filename())).unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_stats_count_segment_pruning_by_segment_time() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(25_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits("cpu.usage", 0, 10_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(5_000, 1.0)]);
    assert_eq!(execution.stats.segments_considered, 2);
    assert_eq!(execution.stats.segments_skipped_by_time, 1);
    assert_eq!(execution.stats.segments_skipped_by_missing_equality, 0);
    assert_eq!(execution.stats.segments_skipped_by_matcher_time_range, 0);
    assert_eq!(execution.stats.segments_queried, 1);
}

#[test]
fn promql_query_session_does_not_open_chunk_files_when_postings_are_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let segment = segment_dir_with_start(tempdir.path(), 0);
    fs::remove_file(segment.join(SegmentFile::Chunks.filename())).unwrap();
    fs::remove_file(segment.join(SegmentFile::ChunkIndex.filename())).unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_session_stats_count_lazy_file_opens() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    assert_eq!(session.stats().segment_context_opens, 0);

    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();
    assert!(results.is_empty());

    let stats = session.stats();
    assert_eq!(stats.index_routing_opens, 1);
    assert_eq!(stats.segment_context_opens, 0);
    assert_eq!(stats.symbols_bin_opens, 0);
    assert_eq!(stats.indexes_puffin_opens, 0);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
}

#[test]
fn promql_query_session_prewarm_eliminates_first_query_file_open_deltas() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0), (6_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "mem.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(15_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let prewarm_delta = session
        .prewarm_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            20_000,
            QueryLimits::production_default(),
        )
        .unwrap();

    assert_eq!(prewarm_delta.index_routing_opens, 2);
    assert_eq!(prewarm_delta.segment_context_opens, 1);
    assert_eq!(prewarm_delta.symbols_bin_opens, 1);
    assert_eq!(prewarm_delta.indexes_puffin_opens, 0);
    assert_eq!(prewarm_delta.series_bin_opens, 1);
    assert_eq!(prewarm_delta.chunk_index_bin_opens, 1);
    assert_eq!(prewarm_delta.chunks_bin_opens, 1);

    let before_query = session.stats();
    let execution = session
        .query_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            20_000,
            QueryLimits::production_default(),
        )
        .unwrap();
    let after_query = session.stats();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].samples,
        vec![(5_000, 1.0), (6_000, 2.0)]
    );
    assert_eq!(after_query.delta_since(before_query), Default::default());
}

#[test]
fn promql_query_sessions_reuse_store_level_series_and_chunk_entry_cache() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for idx in 0..8 {
        write_series(
            &mut writer,
            SeriesRef::new(idx + 1),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), format!("backend-{idx}")),
            ],
            &[(5_000, idx as f64), (6_000, idx as f64 + 1.0)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut first_session = store.query_session().unwrap();
    let first = first_session
        .query_promql(r#"{__name__="cpu.usage"}"#, 0, 10_000)
        .unwrap();
    let first_profile = first_session.profile();
    assert_eq!(first.len(), 8);
    assert!(first_profile.series_entries_read >= 8);
    assert!(first_profile.series_entry_read > Duration::ZERO);
    assert!(first_profile.chunk_index_range_bytes > 0);
    assert!(first_profile.chunk_index_range_read > Duration::ZERO);

    let mut second_session = store.query_session().unwrap();
    let second = second_session
        .query_promql(r#"{__name__="cpu.usage"}"#, 0, 10_000)
        .unwrap();
    let second_profile = second_session.profile();

    assert_eq!(second.len(), first.len());
    assert_eq!(second_profile.series_entries_read, 0);
    assert_eq!(second_profile.series_entry_read, Duration::ZERO);
    assert_eq!(second_profile.chunk_index_range_bytes, 0);
    assert_eq!(second_profile.chunk_index_range_read, Duration::ZERO);
}

#[test]
fn promql_query_metric_name_equality_uses_metric_series_ranges_instead_of_postings() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for idx in 0..6 {
        write_series(
            &mut writer,
            SeriesRef::new(idx + 1),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), format!("backend-{idx}")),
            ],
            &[(5_000, idx as f64)],
        );
    }
    write_series(
        &mut writer,
        SeriesRef::new(100),
        vec![
            (METRIC_NAME_LABEL.to_string(), "mem.usage".to_string()),
            ("pod.name".to_string(), "backend-0".to_string()),
        ],
        &[(5_000, 100.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"{__name__="cpu.usage"}"#,
            0,
            10_000,
            QueryLimits::production_default(),
        )
        .unwrap();

    assert_eq!(execution.results.len(), 6);
    assert_eq!(
        sorted_first_sample_values(&execution.results),
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    );
    assert_eq!(execution.stats.index_postings_reads, 0);
    assert_eq!(execution.stats.index_postings_bytes_read, 0);
}

#[test]
fn promql_query_session_decodes_metric_series_ranges_once_per_segment() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for idx in 0..3 {
        write_series(
            &mut writer,
            SeriesRef::new(idx + 1),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), format!("backend-{idx}")),
            ],
            &[(5_000, idx as f64)],
        );
    }
    write_series(
        &mut writer,
        SeriesRef::new(100),
        vec![
            (METRIC_NAME_LABEL.to_string(), "mem.usage".to_string()),
            ("pod.name".to_string(), "backend-0".to_string()),
        ],
        &[(5_000, 100.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();

    let before_first = session.profile();
    let first = session
        .query_promql(r#"{__name__="cpu.usage"}"#, 0, 10_000)
        .unwrap();
    let first_delta = session.profile().delta_since(before_first);
    assert_eq!(first.len(), 3);
    assert!(first_delta.metric_series_ranges_read > Duration::ZERO);
    assert!(first_delta.metric_series_ranges_bytes > 0);

    let before_second = session.profile();
    let second = session
        .query_promql(r#"{__name__="mem.usage"}"#, 0, 10_000)
        .unwrap();
    let second_delta = session.profile().delta_since(before_second);
    assert_eq!(second.len(), 1);
    assert_eq!(second_delta.metric_series_ranges_read, Duration::ZERO);
    assert_eq!(second_delta.metric_series_ranges_bytes, 0);
}

#[test]
fn promql_query_sessions_reuse_store_level_routing_reader_cache() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut first_session = store.query_session().unwrap();
    assert!(
        first_session
            .query_promql("cpu.usage", 0, 10_000)
            .unwrap()
            .is_empty()
    );
    assert_eq!(first_session.stats().index_routing_opens, 1);
    assert!(first_session.profile().index_routing_open > Duration::ZERO);

    let mut second_session = store.query_session().unwrap();
    assert!(
        second_session
            .query_promql("cpu.usage", 0, 10_000)
            .unwrap()
            .is_empty()
    );
    assert_eq!(second_session.stats().index_routing_opens, 0);
    assert_eq!(second_session.profile().index_routing_open, Duration::ZERO);
}

#[test]
fn promql_query_session_prefetch_warms_exact_scalar_lane_ranges_before_query() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let explicit_bounds = (0..256).map(|value| value as f64).collect::<Vec<_>>();
    let bucket_counts = vec![1; explicit_bounds.len() + 1];
    let count = bucket_counts.iter().sum::<u64>();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                1_000,
                HistogramValue {
                    count,
                    sum: Some(32768.0),
                    min: Some(0.0),
                    max: Some(256.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds,
                    bucket_counts,
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "wide.histogram");
                visit("route", "/prefetch");
            },
        )
        .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "mem.usage".to_string()),
            ("host".to_string(), "host-b".to_string()),
        ],
        &[(15_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let prefetch = session
        .prefetch_promql_data_with_limits(
            r#"wide.histogram_count{route="/prefetch"}"#,
            0,
            20_000,
            QueryLimits::production_default(),
        )
        .unwrap();

    assert_eq!(prefetch.query_stats.segments_considered, 4);
    assert_eq!(prefetch.query_stats.segments_skipped_by_missing_equality, 3);
    assert_eq!(prefetch.query_stats.segments_queried, 1);
    assert_eq!(prefetch.query_stats.index_postings_reads, 1);
    assert_eq!(prefetch.series_entries_read, 1);
    assert_eq!(prefetch.chunk_index_reads, 1);
    assert!(prefetch.chunk_index_bytes_read > 0);
    assert_eq!(prefetch.query_stats.chunk_reads, 1);
    assert!(prefetch.query_stats.bytes_read > 0);

    let before_query = session.stats();
    let execution = session
        .query_promql_with_limits(
            r#"wide.histogram_count{route="/prefetch"}"#,
            0,
            20_000,
            QueryLimits::production_default(),
        )
        .unwrap();
    let after_query = session.stats();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(1_000, count as f64)]);
    assert_eq!(execution.stats.chunk_reads, 1);
    assert_eq!(execution.stats.typed_scalar_chunks_decoded, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 0);
    assert_eq!(prefetch.query_stats.bytes_read, execution.stats.bytes_read);
    assert_eq!(after_query.delta_since(before_query), Default::default());
}

#[test]
fn promql_query_session_uses_label_value_time_ranges_for_equality_pruning() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(1_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let execution = session
        .query_promql_with_limits("mem.usage", 8_000, 9_000, QueryLimits::unlimited())
        .unwrap();
    assert!(execution.results.is_empty());
    assert_eq!(execution.stats.segments_considered, 1);
    assert_eq!(execution.stats.segments_skipped_by_time, 0);
    assert_eq!(execution.stats.segments_skipped_by_missing_equality, 0);
    assert_eq!(execution.stats.segments_skipped_by_matcher_time_range, 1);
    assert_eq!(execution.stats.segments_queried, 0);

    let stats = session.stats();
    assert_eq!(stats.index_routing_opens, 1);
    assert_eq!(stats.segment_context_opens, 0);
    assert_eq!(stats.symbols_bin_opens, 0);
    assert_eq!(stats.indexes_puffin_opens, 0);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
}

#[test]
fn promql_query_stats_count_segment_pruning_from_missing_equality_metadata() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("host".to_string(), "host-a".to_string()),
        ],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "mem.usage".to_string()),
            ("host".to_string(), "host-b".to_string()),
        ],
        &[(15_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let execution = session
        .query_promql_with_limits(
            r#"cpu.usage{host="host-a"}"#,
            0,
            20_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(5_000, 1.0)]);
    assert_eq!(execution.stats.segments_considered, 2);
    assert_eq!(execution.stats.segments_skipped_by_time, 0);
    assert_eq!(execution.stats.segments_skipped_by_missing_equality, 1);
    assert_eq!(execution.stats.segments_skipped_by_matcher_time_range, 0);
    assert_eq!(execution.stats.segments_queried, 1);

    let stats = session.stats();
    assert_eq!(stats.index_routing_opens, 2);
    assert_eq!(stats.segment_context_opens, 1);
    assert_eq!(stats.chunk_index_bin_opens, 1);
    assert_eq!(stats.chunks_bin_opens, 1);
}

#[test]
fn promql_query_session_uses_label_value_time_ranges_for_regex_pruning() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(1_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let results = session
        .query_promql(r#"{__name__=~"mem\..*"}"#, 8_000, 9_000)
        .unwrap();
    assert!(results.is_empty());

    let stats = session.stats();
    assert_eq!(stats.segment_context_opens, 1);
    assert_eq!(stats.symbols_bin_opens, 1);
    assert_eq!(stats.indexes_puffin_opens, 1);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
}

#[test]
fn promql_query_uses_selective_equality_matcher_before_metric_postings() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for idx in 0..100 {
        write_series(
            &mut writer,
            SeriesRef::new(idx + 1),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("host".to_string(), format!("host-{idx:03}")),
            ],
            &[(5_000, idx as f64)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"cpu.usage{host="host-042"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(5_000, 42.0)]);
    assert_eq!(execution.stats.index_postings_reads, 1);
    assert_eq!(execution.stats.index_postings_bytes_read, 8);
}

#[test]
fn promql_query_limit_rejects_too_many_matched_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_matched_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "matched_series", 1);
}

#[test]
fn promql_query_limit_rejects_too_many_projected_histogram_bucket_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                5_000,
                HistogramValue {
                    count: 6,
                    sum: Some(2.4),
                    min: Some(0.1),
                    max: Some(1.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![0.25, 0.5],
                    bucket_counts: vec![1, 3, 2],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/projected-budget");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            r#"http.request.duration_bucket{route="/projected-budget"}"#,
            0,
            10_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "projected_series", 2);
}

#[test]
fn promql_query_limit_counts_scalar_result_series_once() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.stats.projected_series, 1);
}

#[test]
fn promql_query_limit_counts_typed_count_projection_as_one_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                5_000,
                HistogramValue {
                    count: 6,
                    sum: Some(2.4),
                    min: Some(0.1),
                    max: Some(1.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![0.25, 0.5],
                    bucket_counts: vec![1, 3, 2],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/projected-count-budget");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"http.request.duration_count{route="/projected-count-budget"}"#,
            0,
            10_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_scalar_chunks_decoded, 1);
}

#[test]
fn promql_query_with_head_limit_rejects_too_many_projected_summary_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let summary_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "rpc.duration"),
            ("route", "/projected-head-budget"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        summary_series,
        5_000,
        SampleValue::Summary(SummaryValue {
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
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"{__name__=~"rpc_duration.*",route="/projected-head-budget"}"#,
            0,
            10_000,
            QueryLimits {
                max_projected_series: Some(3),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "projected_series", 3);
}

#[test]
fn promql_query_limit_rejects_too_many_chunk_reads() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_chunk_reads: Some(0),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "chunk_reads", 0);
}

#[test]
fn promql_query_limit_rejects_too_many_bytes_read() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_bytes_read: Some(0),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "bytes_read", 0);
}

#[test]
fn promql_query_limit_rejects_too_many_samples_decoded() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0), (6_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_samples_decoded: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "samples_decoded", 1);
}

#[test]
fn promql_query_limit_rejects_too_many_regex_values_examined() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for (idx, pod) in ["backend-1", "backend-2", "frontend-1"]
        .into_iter()
        .enumerate()
    {
        write_series(
            &mut writer,
            SeriesRef::new(idx as u32 + 1),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), pod.to_string()),
            ],
            &[(5_000, idx as f64)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name=~".*"}"#,
            0,
            10_000,
            QueryLimits {
                max_regex_values_examined: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "regex_values_examined", 2);
}

#[test]
fn promql_query_metric_name_regex_uses_fst_prefix_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for (idx, metric_name) in ["alpha_metric", "beta_metric", "go_gc_duration_seconds"]
        .into_iter()
        .enumerate()
    {
        write_series(
            &mut writer,
            SeriesRef::new(idx as u32 + 1),
            vec![(METRIC_NAME_LABEL.to_string(), metric_name.to_string())],
            &[(5_000, idx as f64)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"{__name__=~"go_gc_duration_seconds.*"}"#,
            0,
            10_000,
            QueryLimits {
                max_regex_values_examined: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(5_000, 2.0)]);
    assert_eq!(execution.stats.regex_values_examined, 1);
}

#[test]
fn promql_query_with_head_limits_count_head_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let mut head = test_head();
    head.record_sample(series, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series, 6_000, SampleValue::Float(2.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits {
                max_samples_decoded: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "samples_decoded", 1);
}

#[test]
fn promql_query_with_head_limits_regex_values_examined() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend_1 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let backend_2 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-2")],
    );
    let frontend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "frontend-1")],
    );
    let mut head = test_head();
    head.record_sample(backend_1, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(backend_2, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(frontend, 5_000, SampleValue::Float(3.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~".*"}"#,
            0,
            10_000,
            QueryLimits {
                max_regex_values_examined: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "regex_values_examined", 2);
}
