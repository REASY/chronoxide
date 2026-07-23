use super::*;

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

    let store = open_default_store(tempdir.path());
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
fn promql_query_rate_and_increase_ignore_interior_stale_marker() {
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

    let store = open_default_store(tempdir.path());
    let increase = store
        .query_promql(
            r#"increase(http.requests.total{route="/stale-counter"}[4s])"#,
            0,
            4_000,
        )
        .unwrap();
    let rate = store
        .query_promql(
            r#"rate(http.requests.total{route="/stale-counter"}[4s])"#,
            0,
            4_000,
        )
        .unwrap();

    assert_eq!(increase.len(), 1);
    assert_eq!(increase[0].samples.len(), 1);
    assert_eq!(increase[0].samples[0].0, 4_000);
    assert!((increase[0].samples[0].1 - 8.0 / 3.0).abs() < 1e-12);
    assert_eq!(rate.len(), 1);
    assert_eq!(rate[0].samples.len(), 1);
    assert_eq!(rate[0].samples[0].0, 4_000);
    assert!((rate[0].samples[0].1 - 2.0 / 3.0).abs() < 1e-12);
}
#[test]
fn promql_query_rate_and_increase_match_prometheus_float_operation_order() {
    let tempdir = tempfile::tempdir().unwrap();
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "prometheus.operation.order.total".to_string(),
        ),
        ("kind".to_string(), "scalar".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(76),
            &raw_labels,
            &[(2_000, 3.0), (7_000, 6.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let increase = store
        .query_promql(
            r#"increase(prometheus.operation.order.total{kind="scalar"}[7s])"#,
            0,
            7_000,
        )
        .unwrap();
    let rate = store
        .query_promql(
            r#"rate(prometheus.operation.order.total{kind="scalar"}[7s])"#,
            0,
            7_000,
        )
        .unwrap();

    // These exact values were verified against promtool without fuzzy
    // comparison. They distinguish factor-first arithmetic from multiplying
    // or dividing an already-rounded increase.
    assert_eq!(increase[0].samples[0].1.to_bits(), 0x4010_cccc_cccc_cccc);
    assert_eq!(rate[0].samples[0].1.to_bits(), 0x3fe3_3333_3333_3333);
}
#[test]
fn promql_query_rate_and_increase_include_epoch_zero_for_pre_epoch_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "pre.epoch.counter.total".to_string(),
        ),
        ("kind".to_string(), "scalar".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(76), &raw_labels, &[(0, 5.0), (1_000, 10.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (query, expected) in [
        (
            r#"increase(pre.epoch.counter.total{kind="scalar"}[3s])"#,
            7.5,
        ),
        (r#"rate(pre.epoch.counter.total{kind="scalar"}[3s])"#, 2.5),
    ] {
        let results = store.query_promql(query, 0, 1_000).unwrap();
        assert_eq!(results.len(), 1, "missing pre-epoch result for {query}");
        assert_eq!(results[0].samples, vec![(1_000, expected)]);
    }
}
#[test]
fn promql_query_rate_and_increase_distinguish_stale_from_ordinary_non_finite_values() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();

    for (idx, (kind, values)) in [
        ("nan-first", [f64::NAN, 1.0, 3.0, 5.0, 7.0]),
        ("nan-interior", [1.0, f64::NAN, 3.0, 5.0, 7.0]),
        ("nan-last", [1.0, 3.0, 5.0, 7.0, f64::NAN]),
        (
            "positive-infinity-interior",
            [1.0, f64::INFINITY, 3.0, 5.0, 7.0],
        ),
        (
            "negative-infinity-interior",
            [1.0, f64::NEG_INFINITY, 3.0, 5.0, 7.0],
        ),
        (
            "positive-infinity-last",
            [1.0, 3.0, 5.0, 7.0, f64::INFINITY],
        ),
        (
            "negative-infinity-last",
            [1.0, 3.0, 5.0, 7.0, f64::NEG_INFINITY],
        ),
        (
            "stale-interior",
            [1.0, prometheus_stale_nan(), 3.0, 5.0, 7.0],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let samples = [10_000, 20_000, 30_000, 40_000, 50_000]
            .into_iter()
            .zip(values)
            .collect::<Vec<_>>();
        writer
            .record_samples_with_labels(
                SeriesRef::new(80 + idx as u32),
                &[
                    (
                        METRIC_NAME_LABEL.to_string(),
                        "nonfinite.counter.total".to_string(),
                    ),
                    ("kind".to_string(), kind.to_string()),
                ],
                &samples,
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (kind, expected_increase) in [
        ("nan-first", f64::NAN),
        ("nan-interior", 7.0),
        ("nan-last", f64::NAN),
        ("positive-infinity-interior", f64::INFINITY),
        ("negative-infinity-interior", 8.0),
        ("positive-infinity-last", f64::INFINITY),
        ("negative-infinity-last", f64::NEG_INFINITY),
        ("stale-interior", 7.0),
    ] {
        for (function, expected) in [
            ("increase", expected_increase),
            ("rate", expected_increase / 50.0),
        ] {
            let query = format!(r#"{function}(nonfinite.counter.total{{kind="{kind}"}}[50s])"#);
            let results = store.query_promql(&query, 0, 50_000).unwrap();
            assert_eq!(results.len(), 1, "missing result for {query}");
            assert_eq!(
                results[0].samples.len(),
                1,
                "wrong sample count for {query}"
            );
            let actual = results[0].samples[0].1;
            if expected.is_nan() {
                assert!(actual.is_nan(), "expected NaN for {query}, got {actual}");
                assert_ne!(
                    actual.to_bits(),
                    prometheus_stale_nan().to_bits(),
                    "ordinary NaN output must not become the stale marker for {query}"
                );
            } else if expected.is_infinite() {
                assert_eq!(actual, expected, "wrong infinity for {query}");
            } else {
                assert!(
                    (actual - expected).abs() < 1e-12,
                    "expected {expected} for {query}, got {actual}"
                );
            }
        }
    }
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
            .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value)),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());

    let deriv = store
        .query_promql(r#"deriv(cpu.temperature{sensor="rack-a"}[25s])"#, 0, 21_000)
        .unwrap();
    assert_eq!(deriv.len(), 1);
    assert_eq!(
        deriv[0].labels.to_vec().as_slice(),
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
        prediction[0].labels.to_vec().as_slice(),
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
        quantile[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    for query in [
        r#"double_exponential_smoothing(cpu.temperature{sensor="rack-a"}[25s], 0.5, 0.5)"#,
        r#"holt_winters(cpu.temperature{sensor="rack-a"}[25s], 0.5, 0.5)"#,
    ] {
        let results = store.query_promql(query, 0, 21_000).unwrap();
        assert_eq!(results.len(), 1, "query {query}");
        assert_eq!(
            results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/cross-segment".to_string())]
    );

    let query = r#"sum by (route)(rate(http.requests.total{route="/cross-segment"}[15s]))"#;
    let mut repeated = store.query_session().unwrap();
    let expected = repeated
        .query_promql_range_with_limits(query, 15_000, 25_000, 5_000, QueryLimits::unlimited())
        .unwrap();
    let mut one_pass = store.query_session().unwrap();
    one_pass
        .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
        .unwrap();
    let actual = one_pass
        .query_promql_range_with_limits(query, 15_000, 25_000, 5_000, QueryLimits::unlimited())
        .unwrap();
    assert_eq!(actual.results, expected.results);
    assert_eq!(
        actual.semantic_fingerprint_sha256(),
        expected.semantic_fingerprint_sha256()
    );
    assert_eq!(
        actual.portable_semantic_fingerprint_sha256(),
        expected.portable_semantic_fingerprint_sha256()
    );
}
