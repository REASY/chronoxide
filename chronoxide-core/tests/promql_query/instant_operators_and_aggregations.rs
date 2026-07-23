use super::*;

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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"cpu.usage{pod.name="backend-1"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}
#[test]
fn promql_query_at_applies_lookback_retimestamping_and_stale_absence() {
    let tempdir = tempfile::tempdir().unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("host".to_string(), "a".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(7),
            &labels,
            &[(5_000, 1.5), (15_000, prometheus_stale_nan())],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();
    let present = session.query_promql_at("cpu_usage", 10_000).unwrap();
    assert_eq!(present.len(), 1);
    assert_eq!(present[0].samples, vec![(10_000, 1.5)]);

    let stale = session.query_promql_at("cpu_usage", 20_000).unwrap();
    assert!(stale.is_empty());
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"sum by (route)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 4.0)]);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"sum by (__name__, route)({route="/by-name"})"#, 0, 10_000)
        .unwrap();

    let mut samples_by_labels = BTreeMap::new();
    for result in results {
        samples_by_labels.insert(result.labels.to_vec(), result.samples);
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        ignoring[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
            .find_map(|(key, value)| (key == "method").then_some(value.to_owned()))
            .unwrap();
        let code = result
            .labels
            .iter()
            .find_map(|(key, value)| (key == "code").then_some(value.to_owned()))
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"cpu_usage{route="/compare"} > 0.5"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 0.7)]);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());

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

    let store = open_default_store(tempdir.path());

    let rows = |results: &[SegmentQueryResult]| {
        results
            .iter()
            .map(|result| {
                let metric = result
                    .labels
                    .iter()
                    .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.to_owned()))
                    .unwrap();
                let instance = result
                    .labels
                    .iter()
                    .find_map(|(key, value)| (key == "instance").then_some(value.to_owned()))
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());

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

    let store = open_default_store(tempdir.path());
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
        min[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/minmax".to_string())]
    );
    assert_eq!(max.len(), 1);
    assert_eq!(max[0].samples, vec![(10_000, 4.0)]);
    assert_eq!(
        max[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        stdvar[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/stats".to_string())]
    );
    assert!((stdvar[0].samples[0].1 - (8.0 / 3.0)).abs() < 1e-12);

    assert_eq!(stddev.len(), 1);
    assert_eq!(
        stddev[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/stats".to_string())]
    );
    assert!((stddev[0].samples[0].1 - (8.0_f64 / 3.0).sqrt()).abs() < 1e-12);

    assert_eq!(group.len(), 1);
    assert_eq!(
        group[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
                    && value == normalize_metric_name("cpu.usage")),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
    assert_eq!(api_quarter[0].labels.to_vec().as_slice(), &[]);
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

    let store = open_default_store(tempdir.path());
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
        minimum[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/nan-quantile".to_string())]
    );
    assert!(minimum[0].samples[0].1.is_nan());

    assert_eq!(maximum.len(), 1);
    assert_eq!(
        maximum[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        samples_by_labels.insert(result.labels.to_vec(), result.samples);
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"count_values by (route)("value.name", cpu.usage{route="/count-values-normalize"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        samples_by_labels.insert(result.labels.to_vec(), result.samples);
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

    let store = open_default_store(tempdir.path());
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
            .find_map(|(key, value)| (key == "value").then_some(value.to_owned()))
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"sum by (route)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/stale-agg".to_string())]
    );
}
#[test]
fn promql_query_absent_returns_one_with_unique_equality_labels_when_selector_is_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = open_default_store(tempdir.path());

    let results = store
        .query_promql(
            r#"absent(http.requests.total{job="api",instance=~".*",route!="admin"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[("job".to_string(), "api".to_string())]
    );
    assert_eq!(results[0].samples, vec![(10_000, 1.0)]);
}
#[test]
fn promql_query_absent_normalizes_otlp_style_dotted_result_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = open_default_store(tempdir.path());

    let results = store
        .query_promql(
            r#"absent(cpu.usage{pod.name="backend-1",instance=~".*"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
    let store = open_default_store(tempdir.path());

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
    let store = open_default_store(tempdir.path());

    let results = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api",instance=~".*",route!="admin"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"absent_over_time(http.requests.total{job="api"}[5s])"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"sum by (route)(cpu.usage)"#, 0, 400_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(400_000, 3.0)]);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"sum without (instance)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/head-sealed-agg".to_string())]
    );
}
