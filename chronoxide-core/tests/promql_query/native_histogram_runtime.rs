use super::*;

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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.duration{route="/native-quantile-layout-change"}[6s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
fn promql_query_native_histogram_rate_ignores_interior_stale_marker() {
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
                    10_000,
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
                    20_000,
                    HistogramValue {
                        count: 15,
                        sum: Some(15.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![6, 6, 3],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.stale.rate");
                visit("route", "/native-stale-rate");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_count(rate(http.request.native.stale.rate{route="/native-stale-rate"}[40s]))"#,
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
fn promql_query_native_histogram_rate_ignores_stale_marker() {
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.stale{route="/native-stale"}[5s]))"#,
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
fn promql_query_native_histogram_rate_uses_original_range_after_stale_marker() {
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

    let store = open_default_store(tempdir.path());
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
        (value - 1.0).abs() < 1e-12,
        "expected quantile 1 after stale-marker omission, got {value}"
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

    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
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
                        metadata: metadata(0),
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
                        metadata: metadata(1_001),
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

    let store = open_default_store(tempdir.path());
    let native = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.native.delta{route="/native-delta"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();
    assert_eq!(native.len(), 1);
    assert_eq!(native[0].samples, vec![(6_000, 1.0)]);
}
#[test]
fn promql_query_delta_histogram_rate_and_increase_bridge_decreasing_stale_fragment() {
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
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(223),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: delta_metadata(0),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![15, 5],
                    },
                ),
                (
                    10_000,
                    HistogramValue {
                        count: 0,
                        sum: None,
                        min: None,
                        max: None,
                        metadata: stale_metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![0, 0],
                    },
                ),
                (
                    20_000,
                    HistogramValue {
                        count: 5,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata: delta_metadata(10_000),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 4],
                    },
                ),
                (
                    30_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: delta_metadata(20_000),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 8],
                    },
                ),
                (
                    40_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: delta_metadata(30_000),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 8],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.delta.stale.bridge");
                visit("route", "/delta-stale-bridge");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
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
            r#"histogram_count(rate(http.request.delta.stale.bridge{route="/delta-stale-bridge"}[40s]))"#,
            25.0 / 39.0,
        ),
        (
            r#"histogram_sum(rate(http.request.delta.stale.bridge{route="/delta-stale-bridge"}[40s]))"#,
            90.0 / 40.0,
        ),
        (
            r#"rate(http.request.delta.stale.bridge_count{route="/delta-stale-bridge"}[40s])"#,
            45.0 / 40.0,
        ),
        (
            r#"rate(http.request.delta.stale.bridge_sum{route="/delta-stale-bridge"}[40s])"#,
            90.0 / 40.0,
        ),
        (
            r#"rate(http.request.delta.stale.bridge_bucket{route="/delta-stale-bridge",le="1"}[40s])"#,
            20.0 / 40.0,
        ),
        (
            r#"histogram_count(increase(http.request.delta.stale.bridge{route="/delta-stale-bridge"}[40s]))"#,
            1_000.0 / 39.0,
        ),
        (
            r#"histogram_sum(increase(http.request.delta.stale.bridge{route="/delta-stale-bridge"}[40s]))"#,
            90.0,
        ),
        (
            r#"increase(http.request.delta.stale.bridge_count{route="/delta-stale-bridge"}[40s])"#,
            45.0,
        ),
        (
            r#"increase(http.request.delta.stale.bridge_sum{route="/delta-stale-bridge"}[40s])"#,
            90.0,
        ),
        (
            r#"increase(http.request.delta.stale.bridge_bucket{route="/delta-stale-bridge",le="1"}[40s])"#,
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
fn promql_query_delta_histogram_equal_cross_stale_fragment_is_not_a_reset() {
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
    let value = |count, metadata| HistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata,
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(226),
            &[
                (1_000, value(5, delta_metadata(0))),
                (10_000, value(0, stale_metadata)),
                (20_000, value(5, delta_metadata(10_000))),
                (30_000, value(10, delta_metadata(20_000))),
                (40_000, value(10, delta_metadata(30_000))),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.delta.stale.equal");
                visit("route", "/delta-stale-equal");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (query, expected) in [
        (
            r#"histogram_count(rate(http.request.delta.stale.equal{route="/delta-stale-equal"}[40s]))"#,
            20.0 / 39.0,
        ),
        (
            r#"rate(http.request.delta.stale.equal_count{route="/delta-stale-equal"}[40s])"#,
            30.0 / 40.0,
        ),
        (
            r#"histogram_count(increase(http.request.delta.stale.equal{route="/delta-stale-equal"}[40s]))"#,
            800.0 / 39.0,
        ),
        (
            r#"increase(http.request.delta.stale.equal_count{route="/delta-stale-equal"}[40s])"#,
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

    let store = open_default_store(tempdir.path());
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
