use super::*;

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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
            .find_map(|(key, value)| (key == "route").then_some(value.to_owned()))
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
