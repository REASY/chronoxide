use super::*;

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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
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
                        metadata: metadata(0),
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
                        metadata: metadata(1_000),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
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
                        metadata: metadata(0),
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
                        metadata: metadata(1_000),
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
fn promql_query_last_over_time_delta_histogram_projection_resets_after_stale_marker() {
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
    let stale_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        ..metadata
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
                count: 0,
                sum: Some(0.0),
                min: None,
                max: None,
                metadata: stale_metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![0, 0],
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
            SeriesRef::new(36),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta-stale-window");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (query, expected) in [
        (
            r#"last_over_time(http.request.duration_count{route="/delta-stale-window"}[30s])"#,
            10.0,
        ),
        (
            r#"last_over_time(http.request.duration_sum{route="/delta-stale-window"}[30s])"#,
            10.0,
        ),
        (
            r#"last_over_time(http.request.duration_bucket{route="/delta-stale-window",le="1"}[30s])"#,
            4.0,
        ),
    ] {
        for eval_time_ms in [20_000, 40_000] {
            let results = store.query_promql(query, 0, eval_time_ms).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].samples, vec![(eval_time_ms, expected)]);
        }
    }
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let default_store = open_default_store(tempdir.path());
    let default_bucket = default_store
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert!(default_bucket.is_empty());

    let store = open_default_store_with_query_projection_config(
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
                .find_map(|(key, value)| (key == "le").then_some(value))
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
    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    for (start_time_ms, ts, counts) in [(0, 1_000, vec![1, 1]), (1_000, 2_000, vec![2, 1])] {
        head.record_sample(
            series,
            ts,
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count: counts.iter().sum(),
                sum: None,
                min: None,
                max: None,
                metadata: metadata(start_time_ms),
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

    let store = open_default_store_with_query_projection_config(
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
                .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.to_owned()))
                .unwrap();
            let quantile = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "quantile").then_some(value.to_owned()));
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
        key == METRIC_NAME_LABEL && value == format!("{metric_name}_count")
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
                .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.to_owned()))
                .unwrap();
            let quantile = result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "quantile").then_some(value.to_owned()));
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
        key == METRIC_NAME_LABEL && value == format!("{summary_metric_name}_count")
    }));
}
