use super::*;

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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
            results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
            results[0].labels.to_vec().as_slice(),
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
fn promql_query_native_exponential_histogram_binary_arithmetic_preserves_nonfinite_sum() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, sum) in [
        (
            SeriesRef::new(243),
            "http.request.native.exphist.nonfinite.left",
            f64::INFINITY,
        ),
        (
            SeriesRef::new(244),
            "http.request.native.exphist.nonfinite.right",
            f64::NEG_INFINITY,
        ),
    ] {
        let samples = [(
            40_000,
            ExponentialHistogramValue {
                count: 5,
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
                    counts: vec![2, 3],
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
                    visit("route", "/native-exphist-nonfinite");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_sum(http.request.native.exphist.nonfinite.left{route="/native-exphist-nonfinite"} + http.request.native.exphist.nonfinite.right{route="/native-exphist-nonfinite"})"#,
            0,
            40_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/native-exphist-nonfinite".to_string())]
    );
    assert_eq!(results[0].samples[0].0, 40_000);
    assert!(results[0].samples[0].1.is_nan());
}
#[test]
fn promql_query_native_exponential_histogram_sum_aggregation_preserves_nonfinite_scaled_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, count, sum, positive_counts) in [
        (SeriesRef::new(248), "a", 5, 5.0, vec![2, 3]),
        (SeriesRef::new(249), "b", 7, 7.0, vec![3, 4]),
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
                    offset: 1,
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
                    visit(
                        METRIC_NAME_LABEL,
                        "http.request.native.exphist.nonfinite.aggregate",
                    );
                    visit("route", "/native-exphist-nonfinite-aggregate");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"(histogram_count(sum by (route)(http.request.native.exphist.nonfinite.aggregate{route="/native-exphist-nonfinite-aggregate"} * (0 / 0))) != bool histogram_count(sum by (route)(http.request.native.exphist.nonfinite.aggregate{route="/native-exphist-nonfinite-aggregate"} * (0 / 0)))) + (histogram_count(sum by (route)(http.request.native.exphist.nonfinite.aggregate{route="/native-exphist-nonfinite-aggregate"} * -1)) == bool -12) + (histogram_sum(sum by (route)(http.request.native.exphist.nonfinite.aggregate{route="/native-exphist-nonfinite-aggregate"} * -1)) == bool -12)"#,
            0,
            40_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[(
            "route".to_string(),
            "/native-exphist-nonfinite-aggregate".to_string()
        )]
    );
    assert_eq!(results[0].samples, vec![(40_000, 3.0)]);
}
#[test]
fn promql_query_native_exponential_histogram_set_operators_preserve_histogram_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, route, count, sum, positive_counts) in [
        (
            SeriesRef::new(239),
            "http.request.native.exphist.set.left",
            "/native-exphist-set-match",
            25,
            25.0,
            vec![10, 15],
        ),
        (
            SeriesRef::new(240),
            "http.request.native.exphist.set.left",
            "/native-exphist-set-left-only",
            11,
            11.0,
            vec![4, 7],
        ),
        (
            SeriesRef::new(241),
            "http.request.native.exphist.set.right",
            "/native-exphist-set-match",
            7,
            7.0,
            vec![3, 4],
        ),
        (
            SeriesRef::new(242),
            "http.request.native.exphist.set.right",
            "/native-exphist-set-right-only",
            13,
            13.0,
            vec![5, 8],
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
                    visit("route", route);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let and_counts = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.set.left and http.request.native.exphist.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let unless_counts = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.set.left unless http.request.native.exphist.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let or_counts = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.set.left or http.request.native.exphist.set.right)"#,
            0,
            40_000,
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&and_counts, "route"),
        BTreeMap::from([(
            "/native-exphist-set-match".to_string(),
            vec![(40_000, 25.0)]
        )])
    );
    assert_eq!(
        samples_by_label(&unless_counts, "route"),
        BTreeMap::from([(
            "/native-exphist-set-left-only".to_string(),
            vec![(40_000, 11.0)]
        )])
    );
    assert_eq!(
        samples_by_label(&or_counts, "route"),
        BTreeMap::from([
            (
                "/native-exphist-set-left-only".to_string(),
                vec![(40_000, 11.0)]
            ),
            (
                "/native-exphist-set-match".to_string(),
                vec![(40_000, 25.0)]
            ),
            (
                "/native-exphist-set-right-only".to_string(),
                vec![(40_000, 13.0)]
            ),
        ])
    );
}
#[test]
fn promql_query_native_exponential_histogram_binary_bool_comparison_returns_scalar_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, count, sum, positive_counts) in [
        (
            SeriesRef::new(237),
            "http.request.native.exphist.binary.bool.left",
            25,
            25.0,
            vec![10, 15],
        ),
        (
            SeriesRef::new(238),
            "http.request.native.exphist.binary.bool.right",
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
                    visit("route", "/native-exphist-bool-binary");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let equal_true = store
        .query_promql(
            r#"http.request.native.exphist.binary.bool.left{route="/native-exphist-bool-binary"} == bool http.request.native.exphist.binary.bool.left{route="/native-exphist-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let equal_false = store
        .query_promql(
            r#"http.request.native.exphist.binary.bool.left{route="/native-exphist-bool-binary"} == bool http.request.native.exphist.binary.bool.right{route="/native-exphist-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let not_equal_true = store
        .query_promql(
            r#"http.request.native.exphist.binary.bool.left{route="/native-exphist-bool-binary"} != bool http.request.native.exphist.binary.bool.right{route="/native-exphist-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let not_equal_false = store
        .query_promql(
            r#"http.request.native.exphist.binary.bool.left{route="/native-exphist-bool-binary"} != bool http.request.native.exphist.binary.bool.left{route="/native-exphist-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let greater_than = store
        .query_promql(
            r#"http.request.native.exphist.binary.bool.left{route="/native-exphist-bool-binary"} > bool http.request.native.exphist.binary.bool.right{route="/native-exphist-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();

    for results in [&equal_true, &equal_false, &not_equal_true, &not_equal_false] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.to_vec().as_slice(),
            &[(
                "route".to_string(),
                "/native-exphist-bool-binary".to_string()
            )]
        );
        assert_eq!(results[0].samples[0].0, 40_000);
    }
    assert_eq!(equal_true[0].samples[0].1, 1.0);
    assert_eq!(equal_false[0].samples[0].1, 0.0);
    assert_eq!(not_equal_true[0].samples[0].1, 1.0);
    assert_eq!(not_equal_false[0].samples[0].1, 0.0);
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_count(sum by (route)(http.request.native.exphist.stale.aggregate{route="/native-exphist-stale-sum"}))"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.actual_sum{route="/native-exphist-suffix-name"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
            results[0].labels.to_vec().as_slice(),
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
fn promql_query_cross_segment_native_exponential_histogram_reads_match_default_flow() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let exponential_histogram = |count, sum, counts| ExponentialHistogramValue {
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
        positive: ExponentialHistogramBuckets { offset: 0, counts },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    };
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(213),
            &[
                (1_001, exponential_histogram(5, 12.0, vec![2, 3])),
                (6_000, exponential_histogram(10, 24.0, vec![4, 6])),
                (11_000, exponential_histogram(15, 36.0, vec![6, 9])),
                (16_000, exponential_histogram(20, 48.0, vec![8, 12])),
                (21_000, exponential_histogram(25, 60.0, vec![10, 15])),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.session");
                visit("route", "/native-exphist-session");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let query = r#"histogram_quantile(0.5, rate(http.request.native.exphist.session{route="/native-exphist-session"}[20s]))"#;
    let limits = QueryLimits::unlimited();
    let store = open_default_store(tempdir.path());
    let mut default_session = store.query_session().unwrap();
    let expected = default_session
        .query_promql_with_limits(query, 0, 21_000, limits)
        .unwrap();
    let default_profile = default_session.profile();

    let mut experimental_session = store.query_session().unwrap();
    experimental_session.set_experimental_cross_segment_chunk_reads(true);
    let actual = experimental_session
        .query_promql_with_limits(query, 0, 21_000, limits)
        .unwrap();
    let experimental_profile = experimental_session.profile();

    assert_eq!(actual.results, expected.results);
    assert_eq!(actual.stats, expected.stats);
    assert_eq!(actual.stats.typed_full_chunks_decoded, 3);
    assert_eq!(
        default_profile.chunk_payload_bytes,
        experimental_profile.chunk_payload_bytes
    );
    assert_eq!(
        default_profile.chunk_payload_physical_reads,
        experimental_profile.chunk_payload_physical_reads
    );
    assert_eq!(
        default_profile.chunk_payload_physical_bytes,
        experimental_profile.chunk_payload_physical_bytes
    );
    assert_eq!(experimental_profile.chunk_payload_physical_reads, 3);

    for limits in [
        QueryLimits {
            max_bytes_read: Some(default_profile.chunk_payload_bytes - 1),
            ..QueryLimits::unlimited()
        },
        QueryLimits {
            max_samples_decoded: Some(4),
            ..QueryLimits::unlimited()
        },
    ] {
        let mut default_session = store.query_session().unwrap();
        let expected_error = default_session
            .query_promql_with_limits(query, 0, 21_000, limits)
            .unwrap_err();
        let mut experimental_session = store.query_session().unwrap();
        experimental_session.set_experimental_cross_segment_chunk_reads(true);
        let actual_error = experimental_session
            .query_promql_with_limits(query, 0, 21_000, limits)
            .unwrap_err();
        assert_eq!(actual_error, expected_error);
    }
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
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
                        metadata: metadata(0),
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
                        metadata: metadata(1_001),
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

    let store = open_default_store(tempdir.path());
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
fn promql_query_delta_exponential_histogram_rate_and_increase_bridge_decreasing_stale_fragment() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let delta_metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    let stale_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::Unknown,
        ..TypedSampleMetadata::default()
    };
    let value = |count, sum, counts, metadata| ExponentialHistogramValue {
        count,
        sum: Some(sum),
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
    };
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(224),
            &[
                (1_000, value(20, 40.0, vec![15, 5], delta_metadata(0))),
                (10_000, value(0, 0.0, vec![0, 0], stale_metadata)),
                (20_000, value(5, 10.0, vec![1, 4], delta_metadata(10_000))),
                (30_000, value(10, 20.0, vec![2, 8], delta_metadata(20_000))),
                (40_000, value(10, 20.0, vec![2, 8], delta_metadata(30_000))),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.delta.exphist.stale.bridge");
                visit("route", "/delta-exphist-stale-bridge");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();
    let query_value = |query: &str| {
        let results = store.query_promql(query, 0, 40_000).unwrap();
        assert_eq!(results.len(), 1, "missing result for {query}");
        assert_eq!(
            results[0].samples.len(),
            1,
            "wrong sample count for {query}"
        );
        assert_eq!(results[0].samples[0].0, 40_000);
        results[0].samples[0].1
    };

    for (query, expected) in [
        (
            r#"histogram_count(rate(http.request.delta.exphist.stale.bridge{route="/delta-exphist-stale-bridge"}[40s]))"#,
            25.0 / 39.0,
        ),
        (
            r#"histogram_sum(rate(http.request.delta.exphist.stale.bridge{route="/delta-exphist-stale-bridge"}[40s]))"#,
            90.0 / 40.0,
        ),
        (
            r#"rate(http.request.delta.exphist.stale.bridge_count{route="/delta-exphist-stale-bridge"}[40s])"#,
            45.0 / 40.0,
        ),
        (
            r#"rate(http.request.delta.exphist.stale.bridge_sum{route="/delta-exphist-stale-bridge"}[40s])"#,
            90.0 / 40.0,
        ),
        (
            r#"rate(http.request.delta.exphist.stale.bridge_bucket{route="/delta-exphist-stale-bridge",le="2"}[40s])"#,
            20.0 / 40.0,
        ),
        (
            r#"histogram_count(increase(http.request.delta.exphist.stale.bridge{route="/delta-exphist-stale-bridge"}[40s]))"#,
            1_000.0 / 39.0,
        ),
        (
            r#"histogram_sum(increase(http.request.delta.exphist.stale.bridge{route="/delta-exphist-stale-bridge"}[40s]))"#,
            90.0,
        ),
        (
            r#"increase(http.request.delta.exphist.stale.bridge_count{route="/delta-exphist-stale-bridge"}[40s])"#,
            45.0,
        ),
        (
            r#"increase(http.request.delta.exphist.stale.bridge_sum{route="/delta-exphist-stale-bridge"}[40s])"#,
            90.0,
        ),
        (
            r#"increase(http.request.delta.exphist.stale.bridge_bucket{route="/delta-exphist-stale-bridge",le="2"}[40s])"#,
            20.0,
        ),
    ] {
        let actual = query_value(query);
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected} for {query}, got {actual}"
        );
    }
}
#[test]
fn promql_query_delta_exponential_histogram_equal_then_increasing_after_stale_is_not_a_reset() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let delta_metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    let stale_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::Unknown,
        ..TypedSampleMetadata::default()
    };
    let value = |count, metadata| ExponentialHistogramValue {
        count,
        sum: Some(count as f64),
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
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(227),
            &[
                (1_000, value(5, delta_metadata(0))),
                (10_000, value(0, stale_metadata)),
                (20_000, value(5, delta_metadata(10_000))),
                (30_000, value(10, delta_metadata(20_000))),
                (40_000, value(10, delta_metadata(30_000))),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.delta.exphist.stale.equal");
                visit("route", "/delta-exphist-stale-equal");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();
    for (query, expected) in [
        (
            r#"histogram_count(rate(http.request.delta.exphist.stale.equal{route="/delta-exphist-stale-equal"}[40s]))"#,
            20.0 / 39.0,
        ),
        (
            r#"rate(http.request.delta.exphist.stale.equal_count{route="/delta-exphist-stale-equal"}[40s])"#,
            30.0 / 40.0,
        ),
        (
            r#"histogram_count(increase(http.request.delta.exphist.stale.equal{route="/delta-exphist-stale-equal"}[40s]))"#,
            800.0 / 39.0,
        ),
        (
            r#"increase(http.request.delta.exphist.stale.equal_count{route="/delta-exphist-stale-equal"}[40s])"#,
            30.0,
        ),
    ] {
        let results = store.query_promql(query, 0, 40_000).unwrap();
        assert_eq!(results.len(), 1, "missing result for {query}");
        let actual = results[0].samples[0].1;
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected} for {query}, got {actual}"
        );
    }
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

    let store = open_default_store(tempdir.path());
    let execution = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta.exphist.single{route="/native-delta-exphist-single"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();
    let native_count = store
        .query_promql(
            r#"histogram_count(rate(http.request.native.delta.exphist.single{route="/native-delta-exphist-single"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();
    let projected_count = store
        .query_promql(
            r#"rate(http.request.native.delta.exphist.single_count{route="/native-delta-exphist-single"}[5s])"#,
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
    assert_eq!(native_count, projected_count);
    assert_eq!(native_count[0].samples, vec![(6_000, 2.0)]);
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
fn promql_query_native_exponential_histogram_rate_ignores_interior_stale_marker() {
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
                    0,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: Some(5.0),
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
                    10_000,
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
                    20_000,
                    ExponentialHistogramValue {
                        count: 15,
                        sum: Some(15.0),
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
                visit(METRIC_NAME_LABEL, "http.request.native.exphist.stale.rate");
                visit("route", "/native-exphist-stale-rate");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_count(rate(http.request.native.exphist.stale.rate{route="/native-exphist-stale-rate"}[40s]))"#,
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 20_000);
    assert!(
        (results[0].samples[0].1 - 0.375).abs() < 1e-12,
        "expected {}, got {}",
        0.375,
        results[0].samples[0].1
    );
}
#[test]
fn promql_query_native_exponential_histogram_rate_uses_original_range_after_stale_marker() {
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

    let store = open_default_store(tempdir.path());
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

    let expected = 2.0;
    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 7_000);
    let value = execution.results[0].samples[0].1;
    assert!(
        (value - expected).abs() < 1e-12,
        "expected quantile {expected} after stale-marker omission, got {value}"
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

    let store = open_default_store(tempdir.path());
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
                        count: 20,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![20, 0],
                    },
                ),
                (
                    4_000,
                    HistogramValue {
                        count: 24,
                        sum: Some(24.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![24, 0],
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

    let store = open_default_store(tempdir.path());
    let expected = 96_000.0 / 2_999.0;
    for query in [
        r#"increase(http.request.stale_reset_count{route="/hist-stale-counter"}[4s])"#,
        r#"histogram_count(increase(http.request.stale_reset{route="/hist-stale-counter"}[4s]))"#,
        r#"histogram_sum(increase(http.request.stale_reset{route="/hist-stale-counter"}[4s]))"#,
    ] {
        let results = store.query_promql(query, 0, 4_000).unwrap();
        assert_eq!(results.len(), 1, "missing result for {query}");
        assert_eq!(results[0].samples.len(), 1);
        assert_eq!(results[0].samples[0].0, 4_000);
        assert!(
            (results[0].samples[0].1 - expected).abs() < 1e-12,
            "expected reset-aware increase {expected} for {query}, got {}",
            results[0].samples[0].1
        );
    }
}
#[test]
fn promql_query_native_exponential_histogram_honors_reset_hint_after_stale_marker() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let value = |count, metadata| ExponentialHistogramValue {
        count,
        sum: Some(count as f64),
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
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(78),
            &[
                (
                    1_001,
                    value(
                        10,
                        TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                    ),
                ),
                (
                    2_000,
                    value(
                        0,
                        TypedSampleMetadata {
                            flags: OTLP_FLAG_NO_RECORDED_VALUE,
                            reset_hint: CounterResetHint::Unknown,
                            ..TypedSampleMetadata::default()
                        },
                    ),
                ),
                (
                    3_000,
                    value(
                        20,
                        TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                    ),
                ),
                (
                    4_000,
                    value(
                        24,
                        TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                    ),
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.exphist.stale_reset");
                visit("route", "/exphist-stale-counter");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let expected = 96_000.0 / 2_999.0;
    for query in [
        r#"increase(http.request.exphist.stale_reset_count{route="/exphist-stale-counter"}[4s])"#,
        r#"histogram_count(increase(http.request.exphist.stale_reset{route="/exphist-stale-counter"}[4s]))"#,
        r#"histogram_sum(increase(http.request.exphist.stale_reset{route="/exphist-stale-counter"}[4s]))"#,
    ] {
        let results = store.query_promql(query, 0, 4_000).unwrap();
        assert_eq!(results.len(), 1, "missing result for {query}");
        assert_eq!(results[0].samples.len(), 1);
        assert_eq!(results[0].samples[0].0, 4_000);
        assert!(
            (results[0].samples[0].1 - expected).abs() < 1e-12,
            "expected reset-aware increase {expected} for {query}, got {}",
            results[0].samples[0].1
        );
    }
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
