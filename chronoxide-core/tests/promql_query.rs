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
use chronoxide_core::promql::{PromqlQueryError, normalize_metric_name};
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
            &[(1_000, 10.0), (3_000, 15.0), (5_000, 2.0), (6_000, 6.0)],
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
                    1_000,
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
            r#"histogram_quantile(0.5, rate(http.request.duration_bucket{route="/quantile"}[5s]))"#,
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
                    1_000,
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
                        1_000,
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
fn promql_query_native_histogram_sum_drops_incompatible_bucket_layouts() {
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
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.duration{route="/native-quantile-incompatible"}[5s])))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert!(execution.results.is_empty());
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
        1_000,
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
    assert_eq!(execution.results[0].samples, vec![(6_000, 1.6)]);
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
        [(1_000, 5, 12.0, vec![2, 3]), (6_000, 10, 24.0, vec![4, 6])]
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
        for (timestamp_ms, counts) in [(1_000, first_counts), (6_000, second_counts)] {
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
    assert_eq!(execution.results[0].samples, vec![(6_000, 1.6)]);
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
                    1_000,
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
    assert_eq!(results[0].samples, vec![(6_000, 1.6)]);
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
                    1_000,
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
                    1_000,
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
                        1_000,
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
                    1_000,
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
    assert_eq!(results[0].samples, vec![(6_000, 12.0)]);
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
                    1_000,
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
                    1_000,
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
    assert_eq!(results[0].samples, vec![(6_000, 12.0)]);
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
        (1_000, 20, CounterResetHint::NotCounterReset),
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
    assert_eq!(results[0].samples, vec![(6_000, 5.0)]);
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
