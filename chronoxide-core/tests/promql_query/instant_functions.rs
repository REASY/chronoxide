use super::*;

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

    let store = open_default_store(tempdir.path());
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
                key == METRIC_NAME_LABEL && value == normalize_metric_name("cpu.load")
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
    let shifted = store
        .query_promql(r#"cpu.offset offset 5m"#, 0, 600_000)
        .unwrap();

    assert_eq!(shifted.len(), 1);
    assert_eq!(shifted[0].samples, vec![(600_000, 1.0)]);
    assert_eq!(
        shifted[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let shifted = store
        .query_promql(r#"increase(cpu.counter[2m] offset 5m)"#, 0, 420_000)
        .unwrap();

    assert_eq!(shifted.len(), 1);
    assert_eq!(shifted[0].samples, vec![(420_000, 120.0)]);
    assert_eq!(
        shifted[0].labels.to_vec().as_slice(),
        &[("instance".to_string(), "a".to_string())]
    );
}
#[test]
fn promql_query_time_and_vector_evaluate_at_query_end() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = open_default_store(tempdir.path());

    let time = store.query_promql("time()", 0, 1_234_000).unwrap();
    let vector = store.query_promql("vector(time())", 0, 1_234_000).unwrap();

    assert_eq!(time.len(), 1);
    assert_eq!(time[0].labels.to_vec().as_slice(), &[]);
    assert_eq!(time[0].samples, vec![(1_234_000, 1_234.0)]);
    assert_eq!(vector, time);
}
#[test]
fn promql_query_math_log_and_calendar_functions_over_vectors() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = open_default_store(tempdir.path());

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
        assert_eq!(results[0].labels.to_vec().as_slice(), &[], "query {query}");
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

    let store = open_default_store(tempdir.path());
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
        assert_eq!(results[0].labels.to_vec().as_slice(), &[], "query {query}");
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql("timestamp(cpu.timestamp)", 0, 25_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(25_000, 20.0)]);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        results[0].labels.to_vec().as_slice(),
        &[
            (METRIC_NAME_LABEL.to_string(), "up".to_string()),
            ("instance".to_string(), "a:9090".to_string()),
            ("job".to_string(), "api".to_string()),
            ("target".to_string(), "api/a:9090/".to_string()),
        ]
    );
}
