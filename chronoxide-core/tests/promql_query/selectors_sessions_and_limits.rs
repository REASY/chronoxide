use super::*;

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
    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
fn promql_label_matchers_treat_absent_labels_as_empty_strings() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for (series_ref, env, shard, value) in [
        (SeriesRef::new(1), Some(""), "a", 1.0),
        (SeriesRef::new(2), None, "a", 2.0),
        (SeriesRef::new(3), Some("prod"), "a", 3.0),
        (SeriesRef::new(4), None, "b", 4.0),
    ] {
        let mut labels = vec![
            (
                METRIC_NAME_LABEL.to_string(),
                "missing_semantics".to_string(),
            ),
            ("shard".to_string(), shard.to_string()),
        ];
        if let Some(env) = env {
            labels.push(("env".to_string(), env.to_string()));
        }
        write_series(&mut writer, series_ref, labels, &[(5_000, value)]);
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (query, expected) in [
        (r#"missing_semantics{env=""}"#, vec![1.0, 2.0, 4.0]),
        (
            r#"missing_semantics{env=~"prod|"}"#,
            vec![1.0, 2.0, 3.0, 4.0],
        ),
        (r#"missing_semantics{env=~"prod"}"#, vec![3.0]),
        (r#"missing_semantics{env!="prod"}"#, vec![1.0, 2.0, 4.0]),
        (r#"missing_semantics{env!=""}"#, vec![3.0]),
        (r#"missing_semantics{env!~"prod"}"#, vec![1.0, 2.0, 4.0]),
        (r#"missing_semantics{env!~"prod|"}"#, Vec::new()),
        (r#"missing_semantics{shard="a",env=""}"#, vec![1.0, 2.0]),
        (r#"missing_semantics{unknown=""}"#, vec![1.0, 2.0, 3.0, 4.0]),
        (r#"missing_semantics{unknown!=""}"#, Vec::new()),
    ] {
        let results = store.query_promql(query, 0, 10_000).unwrap();
        assert_eq!(sorted_first_sample_values(&results), expected, "{query}");
    }
}
#[test]
fn promql_native_histogram_matchers_treat_absent_labels_as_empty_strings() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, env, count) in [
        (SeriesRef::new(11), Some(""), 6),
        (SeriesRef::new(12), None, 7),
        (SeriesRef::new(13), Some("prod"), 8),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[(
                    5_000,
                    HistogramValue {
                        count,
                        sum: Some(count as f64),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![count, 0],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "native_missing_semantics");
                    if let Some(env) = env {
                        visit("env", env);
                    }
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let empty = store
        .query_promql(
            r#"histogram_count(native_missing_semantics{env=""})"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(sorted_first_sample_values(&empty), vec![6.0, 7.0]);

    let nonempty = store
        .query_promql(
            r#"histogram_count(native_missing_semantics{env!=""})"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(sorted_first_sample_values(&nonempty), vec![8.0]);
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(r#"{__name__=~"cpu_.*"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}
#[test]
fn promql_query_returns_invalid_for_bad_regex() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = open_default_store(tempdir.path());

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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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
fn direct_sealed_queries_delegate_to_full_label_sessions() {
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
        &[(4_000, 1.0), (5_000, 2.0), (6_000, 4.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(4_000, 3.0), (5_000, 5.0), (6_000, 8.0)],
    );
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (query, start_ms, end_ms) in [
        ("cpu.usage", 0, 6_000),
        ("rate(cpu.usage[2s])", 0, 6_000),
        ("sum by (pod.name) (cpu.usage)", 0, 6_000),
        ("cpu.usage + 1", 0, 6_000),
        (
            r#"label_replace(cpu.usage, "service", "$1", "pod.name", "(.*)")"#,
            0,
            6_000,
        ),
    ] {
        let direct = store
            .query_promql_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
            .unwrap();
        let mut session = store.query_session().unwrap();
        session.set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
        let through_session = session
            .query_promql_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
            .unwrap();

        assert_eq!(direct, through_session, "query: {query}");
    }
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

    let store = open_default_store(tempdir.path());
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
fn promql_query_session_does_not_reopen_non_overlapping_segment_metadata() {
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
    let store = open_default_store(tempdir.path());
    fs::remove_file(non_overlapping.join(SegmentFile::Symbols.filename())).unwrap();

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

    let store = open_default_store(tempdir.path());
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
fn promql_query_session_does_not_reopen_chunk_files_when_postings_are_empty() {
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
    let store = open_default_store(tempdir.path());
    fs::remove_file(segment.join(SegmentFile::Chunks.filename())).unwrap();
    fs::remove_file(segment.join(SegmentFile::ChunkIndex.filename())).unwrap();

    let mut session = store.query_session().unwrap();
    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();

    assert!(results.is_empty());
}
#[test]
fn promql_query_session_uses_exact_index_without_routing() {
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

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();
    assert_eq!(session.stats().segment_context_opens, 0);

    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();
    assert!(results.is_empty());

    let stats = session.stats();
    assert_eq!(stats.index_routing_opens, 0);
    assert_eq!(stats.segment_context_opens, 1);
    assert_eq!(stats.symbols_bin_opens, 0);
    assert_eq!(stats.indexes_puffin_opens, 0);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
}
#[test]
fn promql_query_session_prewarm_keeps_only_the_payload_open_for_query_time() {
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

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();
    let prewarm_delta = session
        .prewarm_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            20_000,
            QueryLimits::production_default(),
        )
        .unwrap();

    assert_eq!(prewarm_delta.index_routing_opens, 0);
    assert_eq!(prewarm_delta.segment_context_opens, 2);
    assert_eq!(prewarm_delta.symbols_bin_opens, 0);
    assert_eq!(prewarm_delta.indexes_puffin_opens, 0);
    assert_eq!(prewarm_delta.series_bin_opens, 0);
    assert_eq!(prewarm_delta.chunk_index_bin_opens, 0);
    assert_eq!(prewarm_delta.chunks_bin_opens, 0);

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
    let query_delta = after_query.delta_since(before_query);
    assert_eq!(query_delta.index_routing_opens, 0);
    assert_eq!(query_delta.segment_context_opens, 0);
    assert_eq!(query_delta.symbols_bin_opens, 0);
    assert_eq!(query_delta.indexes_puffin_opens, 0);
    assert_eq!(query_delta.series_bin_opens, 0);
    assert_eq!(query_delta.chunk_index_bin_opens, 0);
    assert_eq!(query_delta.chunks_bin_opens, 1);
}
#[test]
fn promql_query_session_prewarm_skips_unbounded_selector_shapes() {
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
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for query in [
        r#"{__name__=~"cpu[_a-z]+"}"#,
        r#"{pod_name!="missing",__name__=~"[a-z_]+"}"#,
        r#"{pod_name="",__name__=~"[a-z_]+"}"#,
    ] {
        let mut session = store.query_session().unwrap();
        let delta = session
            .prewarm_promql_with_limits(query, 0, 10_000, QueryLimits::production_default())
            .unwrap();
        assert_eq!(
            delta.index_routing_opens, 0,
            "unexpected prewarm for {query}"
        );
        assert_eq!(
            delta.segment_context_opens, 1,
            "selector inspection must open exactly one segment context for {query}"
        );
        assert_eq!(delta.symbols_bin_opens, 0, "unexpected prewarm for {query}");
        assert_eq!(
            delta.indexes_puffin_opens, 0,
            "unexpected prewarm for {query}"
        );
        assert_eq!(delta.series_bin_opens, 0, "unexpected prewarm for {query}");
        assert_eq!(
            delta.chunk_index_bin_opens, 0,
            "unexpected prewarm for {query}"
        );
        assert_eq!(delta.chunks_bin_opens, 0, "unexpected prewarm for {query}");
        assert_eq!(session.stats(), delta);
    }
}
#[test]
fn promql_query_sessions_reuse_facade_metadata_without_legacy_read_charges() {
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

    let store = open_default_store(tempdir.path());
    let mut first_session = store.query_session().unwrap();
    let first = first_session
        .query_promql(r#"{__name__="cpu.usage"}"#, 0, 10_000)
        .unwrap();
    let first_profile = first_session.profile();
    assert_eq!(first.len(), 8);
    assert_eq!(first_profile.series_entries_read, 0);
    assert_eq!(first_profile.series_entry_read, Duration::ZERO);
    assert_eq!(first_profile.chunk_index_range_bytes, 0);
    assert_eq!(first_profile.chunk_index_range_read, Duration::ZERO);

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
fn promql_query_metric_name_equality_uses_exact_postings_without_range_authority() {
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

    let store = open_default_store(tempdir.path());
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
    assert_eq!(execution.stats.index_postings_reads, 1);
    assert!(execution.stats.index_postings_bytes_read > 0);
}
#[test]
fn promql_query_session_never_decodes_metric_series_ranges_without_authority() {
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

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();

    let before_first = session.profile();
    let first = session
        .query_promql(r#"{__name__="cpu.usage"}"#, 0, 10_000)
        .unwrap();
    let first_delta = session.profile().delta_since(before_first);
    assert_eq!(first.len(), 3);
    assert_eq!(first_delta.metric_series_ranges_read, Duration::ZERO);
    assert_eq!(first_delta.metric_series_ranges_bytes, 0);
    assert!(first_delta.exact_postings_read > Duration::ZERO);

    let before_second = session.profile();
    let second = session
        .query_promql(r#"{__name__="mem.usage"}"#, 0, 10_000)
        .unwrap();
    let second_delta = session.profile().delta_since(before_second);
    assert_eq!(second.len(), 1);
    assert_eq!(second_delta.metric_series_ranges_read, Duration::ZERO);
    assert_eq!(second_delta.metric_series_ranges_bytes, 0);
    assert!(second_delta.exact_postings_read > Duration::ZERO);
}
#[test]
fn promql_query_sessions_do_not_open_structural_routing_without_authority() {
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

    let store = open_default_store(tempdir.path());
    let mut first_session = store.query_session().unwrap();
    assert!(
        first_session
            .query_promql("cpu.usage", 0, 10_000)
            .unwrap()
            .is_empty()
    );
    assert_eq!(first_session.stats().index_routing_opens, 0);
    assert_eq!(first_session.profile().index_routing_open, Duration::ZERO);

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

    let store = open_default_store(tempdir.path());
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
    assert_eq!(prefetch.query_stats.index_postings_reads, 2);
    assert_eq!(prefetch.series_entries_read, 0);
    assert_eq!(prefetch.chunk_index_reads, 1);
    assert_eq!(prefetch.chunk_index_bytes_read, 0);
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
fn promql_query_session_facade_filters_equality_results_outside_sample_time() {
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

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();
    let execution = session
        .query_promql_with_limits("mem.usage", 8_000, 9_000, QueryLimits::unlimited())
        .unwrap();
    assert!(execution.results.is_empty());
    assert_eq!(execution.stats.segments_considered, 1);
    assert_eq!(execution.stats.segments_skipped_by_time, 0);
    assert_eq!(execution.stats.segments_skipped_by_missing_equality, 0);
    assert_eq!(execution.stats.segments_skipped_by_matcher_time_range, 0);
    assert_eq!(execution.stats.segments_queried, 1);

    let stats = session.stats();
    assert_eq!(stats.index_routing_opens, 0);
    assert_eq!(stats.segment_context_opens, 1);
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

    let store = open_default_store(tempdir.path());
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
    assert_eq!(stats.index_routing_opens, 0);
    assert_eq!(stats.segment_context_opens, 2);
    assert_eq!(stats.chunk_index_bin_opens, 0);
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

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();
    let results = session
        .query_promql(r#"{__name__=~"mem\..*"}"#, 8_000, 9_000)
        .unwrap();
    assert!(results.is_empty());

    let stats = session.stats();
    assert_eq!(stats.segment_context_opens, 1);
    assert_eq!(stats.symbols_bin_opens, 0);
    assert_eq!(stats.indexes_puffin_opens, 0);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
}
#[test]
fn promql_query_uses_selective_equality_before_metric_postings() {
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

    let store = open_default_store(tempdir.path());
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
    assert_eq!(execution.stats.index_postings_reads, 2);
    assert!(execution.stats.index_postings_bytes_read > 8);
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

    let store = open_default_store(tempdir.path());
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
fn promql_missing_label_scan_checks_candidate_limit_before_series_reads() {
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
            (METRIC_NAME_LABEL.to_string(), "limit_empty".to_string()),
            ("env".to_string(), "".to_string()),
        ],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![(METRIC_NAME_LABEL.to_string(), "limit_empty".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();
    let before = session.profile();
    let err = session
        .query_promql_with_limits(
            r#"limit_empty{env=""}"#,
            0,
            10_000,
            QueryLimits {
                max_matched_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();
    let delta = session.profile().delta_since(before);

    assert_limit_exceeded(err, "matched_series", 1);
    assert_eq!(delta.series_entries_read, 0);
    assert_eq!(delta.series_entry_read_batches, 0);
    assert_eq!(delta.series_entry_bytes, 0);
}
#[test]
fn promql_missing_label_scan_uses_facade_without_legacy_entry_cache_charges() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    const SERIES_COUNT: u32 = 257;
    for series_ref in 0..SERIES_COUNT {
        write_series(
            &mut writer,
            SeriesRef::new(series_ref + 1),
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "uncached_missing_scan".to_string(),
                ),
                ("instance".to_string(), format!("instance-{series_ref:03}")),
            ],
            &[(5_000, f64::from(series_ref))],
        );
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let mut session = store.query_session().unwrap();

    let before_scan = session.profile();
    let absent_scan = session
        .query_promql(r#"uncached_missing_scan{env!=""}"#, 0, 10_000)
        .unwrap();
    let scan_delta = session.profile().delta_since(before_scan);
    assert!(absent_scan.is_empty());
    assert_eq!(scan_delta.series_entries_read, 0);
    assert_eq!(scan_delta.series_entry_read_batches, 0);
    assert_eq!(scan_delta.series_entry_bytes, 0);

    let before_followup = session.profile();
    let followup = session
        .query_promql("uncached_missing_scan", 0, 10_000)
        .unwrap();
    let followup_delta = session.profile().delta_since(before_followup);
    assert_eq!(followup.len(), SERIES_COUNT as usize);
    assert_eq!(
        followup_delta.series_entries_read, 0,
        "facade reads must not be charged to the retired legacy entry profile"
    );
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
    let err = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name=~".+"}"#,
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
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

    let store = open_default_store(tempdir.path());
    let err = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~".+"}"#,
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
