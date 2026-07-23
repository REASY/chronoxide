use super::*;

#[test]
fn promql_query_cross_segment_native_histogram_reads_match_default_flow() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let histogram = |count, sum, bucket_counts| HistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            reset_hint: CounterResetHint::NotCounterReset,
            ..TypedSampleMetadata::default()
        },
        explicit_bounds: vec![1.0, 2.0, 4.0],
        bucket_counts,
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(205),
            &[
                (1_001, histogram(10, 20.0, vec![2, 5, 3, 0])),
                (6_000, histogram(20, 40.0, vec![4, 10, 6, 0])),
                (11_000, histogram(30, 60.0, vec![6, 15, 9, 0])),
                (16_000, histogram(40, 80.0, vec![8, 20, 12, 0])),
                (21_000, histogram(50, 100.0, vec![10, 25, 15, 0])),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.session");
                visit("route", "/native-session");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let query = r#"histogram_quantile(0.5, rate(http.request.native.session{route="/native-session"}[20s]))"#;
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
fn promql_query_cross_segment_generic_payload_kinds_match_default_flow() {
    let tempdir = tempfile::tempdir().unwrap();
    let timestamps = [1_001, 11_000, 21_000];
    for (index, timestamp_ms) in timestamps.into_iter().enumerate() {
        let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(250),
                &[(timestamp_ms, index as f64 + 1.5)],
                |visit| visit(METRIC_NAME_LABEL, "scheduler.float"),
            )
            .unwrap();
        writer
            .record_i64_samples_ordered_with_label_visitor(
                SeriesRef::new(251),
                &[(timestamp_ms, index as i64 + 1)],
                |visit| visit(METRIC_NAME_LABEL, "scheduler.int"),
            )
            .unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(252),
                &[(
                    timestamp_ms,
                    HistogramValue {
                        count: 3,
                        sum: Some(6.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![1, 1, 1],
                    },
                )],
                |visit| visit(METRIC_NAME_LABEL, "scheduler.histogram"),
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(253),
                &[(
                    timestamp_ms,
                    ExponentialHistogramValue {
                        count: 3,
                        sum: Some(6.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![1, 2],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                )],
                |visit| visit(METRIC_NAME_LABEL, "scheduler.exphist"),
            )
            .unwrap();
        writer
            .record_summary_samples_ordered_with_label_visitor(
                SeriesRef::new(254),
                &[(
                    timestamp_ms,
                    SummaryValue {
                        count: 3,
                        sum: 6.0,
                        metadata: TypedSampleMetadata::default(),
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: 2.0,
                        }],
                    },
                )],
                |visit| visit(METRIC_NAME_LABEL, "scheduler.summary"),
            )
            .unwrap();
        writer.flush().unwrap();
    }

    let store = open_default_store(tempdir.path());
    for query in [
        "scheduler.float",
        "scheduler.int",
        "scheduler.histogram_count",
        "scheduler.histogram_sum",
        r#"scheduler.histogram_bucket{le="+Inf"}"#,
        "scheduler.exphist_count",
        "scheduler.exphist_sum",
        r#"scheduler.exphist_bucket{le="+Inf"}"#,
        "scheduler.summary_count",
        "scheduler.summary_sum",
        r#"scheduler.summary{quantile="0.5"}"#,
    ] {
        let mut default_session = store.query_session().unwrap();
        let expected = default_session
            .query_promql_with_limits(query, 0, 21_000, QueryLimits::unlimited())
            .unwrap();
        let default_profile = default_session.profile();
        let mut experimental_session = store.query_session().unwrap();
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        experimental_session
            .set_chunk_read_config(chronoxide_core::storage::io::ChunkReadConfig {
                mode: chronoxide_core::storage::io::ChunkReadMode::IoUring,
                queue_depth: 8,
                payload_coalesce_max_gap_bytes:
                    chronoxide_core::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
            })
            .unwrap();
        experimental_session.set_experimental_cross_segment_chunk_reads(true);
        let actual = experimental_session
            .query_promql_with_limits(query, 0, 21_000, QueryLimits::unlimited())
            .unwrap();
        let experimental_profile = experimental_session.profile();

        assert_eq!(actual.results, expected.results, "{query}");
        assert_eq!(actual.stats, expected.stats, "{query}");
        assert_eq!(
            actual.semantic_fingerprint_sha256(),
            expected.semantic_fingerprint_sha256(),
            "{query}"
        );
        assert_eq!(
            experimental_profile.chunk_payload_bytes, default_profile.chunk_payload_bytes,
            "{query}"
        );
        assert_eq!(
            experimental_profile.chunk_payload_physical_reads,
            default_profile.chunk_payload_physical_reads,
            "{query}"
        );
        assert_eq!(
            experimental_profile.chunk_payload_physical_bytes,
            default_profile.chunk_payload_physical_bytes,
            "{query}"
        );
        assert_eq!(
            experimental_profile.chunk_read_scheduler.executions, 1,
            "{query}"
        );
    }

    let query = "scheduler.summary_count";
    for limits in [
        QueryLimits {
            max_matched_series: Some(0),
            ..QueryLimits::unlimited()
        },
        QueryLimits {
            max_projected_series: Some(0),
            ..QueryLimits::unlimited()
        },
        QueryLimits {
            max_chunk_reads: Some(2),
            ..QueryLimits::unlimited()
        },
        QueryLimits {
            max_bytes_read: Some(1),
            ..QueryLimits::unlimited()
        },
        QueryLimits {
            max_samples_decoded: Some(2),
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
        assert_eq!(actual_error, expected_error, "{limits:?}");
    }

    for cache_bytes in [0, 1024 * 1024] {
        let mut default_session = store.query_session().unwrap();
        default_session
            .set_range_scalar_cache_budget_bytes(cache_bytes)
            .unwrap();
        let expected = default_session
            .query_promql_range_with_limits(
                "scheduler.summary_count",
                1_001,
                21_001,
                10_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        let expected_cache = default_session.last_range_scalar_cache_summary().copied();

        let mut experimental_session = store.query_session().unwrap();
        experimental_session
            .set_range_scalar_cache_budget_bytes(cache_bytes)
            .unwrap();
        experimental_session.set_experimental_cross_segment_chunk_reads(true);
        let actual = experimental_session
            .query_promql_range_with_limits(
                "scheduler.summary_count",
                1_001,
                21_001,
                10_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        let actual_cache = experimental_session
            .last_range_scalar_cache_summary()
            .copied();
        assert_eq!(
            actual.results, expected.results,
            "cache bytes {cache_bytes}"
        );
        assert_eq!(actual.stats, expected.stats, "cache bytes {cache_bytes}");
        assert_eq!(actual_cache, expected_cache, "cache bytes {cache_bytes}");
    }

    let mut auto_session = store.query_session().unwrap();
    auto_session
        .set_chunk_read_config(chronoxide_core::storage::io::ChunkReadConfig {
            mode: chronoxide_core::storage::io::ChunkReadMode::Auto,
            queue_depth: 8,
            payload_coalesce_max_gap_bytes:
                chronoxide_core::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        })
        .unwrap();
    auto_session.set_experimental_cross_segment_chunk_reads(true);
    auto_session
        .query_promql_with_limits("scheduler.float", 0, 21_000, QueryLimits::unlimited())
        .unwrap();
    let auto_profile = auto_session.profile().chunk_read_scheduler;
    assert_eq!(auto_profile.executions, 3);
    assert_eq!(auto_profile.pread_decisions, 3);
    assert_eq!(auto_profile.io_uring_decisions, 0);
}
#[test]
fn promql_query_facade_uses_configured_payload_coalesce_gap_without_semantic_changes() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    for (series_ref, selected, value) in [
        (SeriesRef::new(260), "yes", 1.0),
        (SeriesRef::new(261), "no", 99.0),
        (SeriesRef::new(262), "yes", 2.0),
    ] {
        write_series(
            &mut writer,
            series_ref,
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "payload_coalesce_gap".to_string(),
                ),
                ("selected".to_string(), selected.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let run = |payload_coalesce_max_gap_bytes| {
        let mut session = store.query_session().unwrap();
        session
            .set_chunk_read_config(chronoxide_core::storage::io::ChunkReadConfig {
                mode: chronoxide_core::storage::io::ChunkReadMode::Pread,
                queue_depth: 8,
                payload_coalesce_max_gap_bytes,
            })
            .unwrap();
        let execution = session
            .query_promql_with_limits(
                r#"payload_coalesce_gap{selected="yes"}"#,
                0,
                10_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        (execution, session.profile())
    };

    let (uncoalesced, uncoalesced_profile) = run(0);
    let (coalesced, coalesced_profile) = run(4096);

    assert_eq!(coalesced.results, uncoalesced.results);
    assert_eq!(coalesced.stats, uncoalesced.stats);
    assert_eq!(
        coalesced.semantic_fingerprint_sha256(),
        uncoalesced.semantic_fingerprint_sha256()
    );
    assert_eq!(
        coalesced.portable_semantic_fingerprint_sha256(),
        uncoalesced.portable_semantic_fingerprint_sha256()
    );
    assert_eq!(
        coalesced_profile.chunk_payload_bytes,
        uncoalesced_profile.chunk_payload_bytes
    );
    assert_eq!(
        coalesced_profile.chunk_payload_locality,
        uncoalesced_profile.chunk_payload_locality
    );
    assert_eq!(uncoalesced_profile.chunk_payload_physical_reads, 2);
    assert_eq!(coalesced_profile.chunk_payload_physical_reads, 1);
    assert!(
        coalesced_profile.chunk_payload_physical_bytes
            > uncoalesced_profile.chunk_payload_physical_bytes
    );
    for (profile, expected_spans) in [(&uncoalesced_profile, 2), (&coalesced_profile, 1)] {
        let scheduler = profile.chunk_read_scheduler;
        assert_eq!(scheduler.executions, 1);
        assert_eq!(scheduler.pread_decisions, 1);
        assert_eq!(scheduler.io_uring_decisions, 0);
        assert_eq!(scheduler.logical_requests, 2);
        assert_eq!(scheduler.physical_spans, expected_spans);
        assert_eq!(scheduler.backend_submissions, expected_spans);
        assert_eq!(scheduler.submission_depth_sum, expected_spans);
        assert_eq!(scheduler.submission_depth_max, 1);
        assert_eq!(scheduler.submission_depth_1, expected_spans);
        assert_eq!(
            scheduler.total_physical_bytes_executed,
            profile.chunk_payload_physical_bytes
        );
    }
}
#[test]
fn promql_query_cross_segment_preserves_earlier_decode_error_precedence() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(255),
            &[(1_001, 1.0), (11_000, 2.0)],
            |visit| visit(METRIC_NAME_LABEL, "scheduler.corrupt"),
        )
        .unwrap();
    writer.flush().unwrap();

    let mut segment_dirs = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("seg-"))
        })
        .collect::<Vec<_>>();
    segment_dirs.sort();
    assert_eq!(segment_dirs.len(), 2);

    // Open the healthy store first, then inject query-time corruption into
    // both already-registered artifacts so this still exercises error
    // precedence.
    let store = open_default_store(tempdir.path());

    let first_chunks = segment_dirs[0].join(SegmentFile::Chunks.filename());
    let mut first_bytes = fs::read(&first_chunks).unwrap();
    let last = first_bytes.last_mut().unwrap();
    *last ^= 0xff;
    fs::write(first_chunks, first_bytes).unwrap();
    let second_index = segment_dirs[1].join(SegmentFile::ChunkIndex.filename());
    let second_len = fs::metadata(&second_index).unwrap().len();
    fs::write(
        second_index,
        vec![0u8; usize::try_from(second_len).unwrap()],
    )
    .unwrap();

    let mut default_session = store.query_session().unwrap();
    default_session
        .set_chunk_read_config(chronoxide_core::storage::io::ChunkReadConfig {
            mode: chronoxide_core::storage::io::ChunkReadMode::Pread,
            queue_depth: 8,
            payload_coalesce_max_gap_bytes: 0,
        })
        .unwrap();
    let expected = default_session
        .query_promql_with_limits("scheduler.corrupt", 0, 11_000, QueryLimits::unlimited())
        .unwrap_err();
    let mut experimental_session = store.query_session().unwrap();
    experimental_session
        .set_chunk_read_config(chronoxide_core::storage::io::ChunkReadConfig {
            mode: chronoxide_core::storage::io::ChunkReadMode::Pread,
            queue_depth: 8,
            payload_coalesce_max_gap_bytes:
                chronoxide_core::storage::io::MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        })
        .unwrap();
    experimental_session.set_experimental_cross_segment_chunk_reads(true);
    let actual = experimental_session
        .query_promql_with_limits("scheduler.corrupt", 0, 11_000, QueryLimits::unlimited())
        .unwrap_err();

    assert_eq!(actual, expected);
    assert!(
        actual.to_string().contains("crc"),
        "later planning error won over earlier decode corruption: {actual}"
    );
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
            results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
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
            results[0].labels.to_vec().as_slice(),
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
fn promql_query_native_histogram_binary_scalar_arithmetic_preserves_nonfinite_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let samples = [(
        40_000,
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
    )];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(245),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.nonfinite.scalar");
                visit("route", "/native-nonfinite-scalar");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"(histogram_count(http.request.native.nonfinite.scalar{route="/native-nonfinite-scalar"} * (0 / 0)) != bool histogram_count(http.request.native.nonfinite.scalar{route="/native-nonfinite-scalar"} * (0 / 0))) + (histogram_sum(http.request.native.nonfinite.scalar{route="/native-nonfinite-scalar"} / 0) == bool (1 / 0)) + (histogram_avg(http.request.native.nonfinite.scalar{route="/native-nonfinite-scalar"} / 0) != bool histogram_avg(http.request.native.nonfinite.scalar{route="/native-nonfinite-scalar"} / 0)) + (histogram_count(http.request.native.nonfinite.scalar{route="/native-nonfinite-scalar"} * -1) == bool -5) + (histogram_sum(http.request.native.nonfinite.scalar{route="/native-nonfinite-scalar"} * -1) == bool -5)"#,
            0,
            40_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/native-nonfinite-scalar".to_string())]
    );
    assert_eq!(results[0].samples, vec![(40_000, 5.0)]);
}
#[test]
fn promql_query_native_histogram_sum_aggregation_preserves_nonfinite_scaled_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance, count, sum, bucket_counts) in [
        (SeriesRef::new(246), "a", 5, 5.0, vec![2, 2, 1]),
        (SeriesRef::new(247), "b", 7, 7.0, vec![3, 2, 2]),
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
                visit(METRIC_NAME_LABEL, "http.request.native.nonfinite.aggregate");
                visit("route", "/native-nonfinite-aggregate");
                visit("instance", instance);
            })
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"(histogram_count(sum by (route)(http.request.native.nonfinite.aggregate{route="/native-nonfinite-aggregate"} * (0 / 0))) != bool histogram_count(sum by (route)(http.request.native.nonfinite.aggregate{route="/native-nonfinite-aggregate"} * (0 / 0)))) + (histogram_count(sum by (route)(http.request.native.nonfinite.aggregate{route="/native-nonfinite-aggregate"} * -1)) == bool -12) + (histogram_sum(sum by (route)(http.request.native.nonfinite.aggregate{route="/native-nonfinite-aggregate"} * -1)) == bool -12)"#,
            0,
            40_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[(
            "route".to_string(),
            "/native-nonfinite-aggregate".to_string()
        )]
    );
    assert_eq!(results[0].samples, vec![(40_000, 3.0)]);
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

    let store = open_default_store(tempdir.path());
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
            results[0].labels.to_vec().as_slice(),
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
fn promql_query_native_histogram_set_operators_preserve_histogram_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, route, count, sum, bucket_counts) in [
        (
            SeriesRef::new(233),
            "http.request.native.set.left",
            "/native-set-match",
            25,
            25.0,
            vec![10, 10, 5],
        ),
        (
            SeriesRef::new(234),
            "http.request.native.set.left",
            "/native-set-left-only",
            11,
            11.0,
            vec![4, 4, 3],
        ),
        (
            SeriesRef::new(235),
            "http.request.native.set.right",
            "/native-set-match",
            7,
            7.0,
            vec![3, 2, 2],
        ),
        (
            SeriesRef::new(236),
            "http.request.native.set.right",
            "/native-set-right-only",
            13,
            13.0,
            vec![5, 5, 3],
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
                visit("route", route);
            })
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let and_counts = store
        .query_promql(
            r#"histogram_count(http.request.native.set.left and http.request.native.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let unless_counts = store
        .query_promql(
            r#"histogram_count(http.request.native.set.left unless http.request.native.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let or_counts = store
        .query_promql(
            r#"histogram_count(http.request.native.set.left or http.request.native.set.right)"#,
            0,
            40_000,
        )
        .unwrap();

    assert_eq!(
        samples_by_label(&and_counts, "route"),
        BTreeMap::from([("/native-set-match".to_string(), vec![(40_000, 25.0)])])
    );
    assert_eq!(
        samples_by_label(&unless_counts, "route"),
        BTreeMap::from([("/native-set-left-only".to_string(), vec![(40_000, 11.0)])])
    );
    assert_eq!(
        samples_by_label(&or_counts, "route"),
        BTreeMap::from([
            ("/native-set-left-only".to_string(), vec![(40_000, 11.0)]),
            ("/native-set-match".to_string(), vec![(40_000, 25.0)]),
            ("/native-set-right-only".to_string(), vec![(40_000, 13.0)]),
        ])
    );
}
#[test]
fn promql_query_mixed_native_histogram_set_operators_preserve_selected_histogram_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, route, count, sum, bucket_counts) in [
        (
            SeriesRef::new(243),
            "http.request.native.mixed.set.left",
            "/native-mixed-set-match",
            25,
            25.0,
            vec![10, 10, 5],
        ),
        (
            SeriesRef::new(244),
            "http.request.native.mixed.set.left",
            "/native-mixed-set-left-only",
            11,
            11.0,
            vec![4, 4, 3],
        ),
        (
            SeriesRef::new(245),
            "http.request.native.mixed.set.right",
            "/native-mixed-set-match",
            7,
            7.0,
            vec![3, 2, 2],
        ),
        (
            SeriesRef::new(246),
            "http.request.native.mixed.set.right",
            "/native-mixed-set-right-only",
            13,
            13.0,
            vec![5, 5, 3],
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
                visit("route", route);
            })
            .unwrap();
    }

    for (series_ref, metric, route, count, sum, positive_counts) in [
        (
            SeriesRef::new(247),
            "http.request.native.exphist.mixed.set.left",
            "/native-mixed-set-match",
            25,
            25.0,
            vec![10, 15],
        ),
        (
            SeriesRef::new(248),
            "http.request.native.exphist.mixed.set.left",
            "/native-mixed-set-left-only",
            11,
            11.0,
            vec![4, 7],
        ),
        (
            SeriesRef::new(249),
            "http.request.native.exphist.mixed.set.right",
            "/native-mixed-set-match",
            7,
            7.0,
            vec![3, 4],
        ),
        (
            SeriesRef::new(250),
            "http.request.native.exphist.mixed.set.right",
            "/native-mixed-set-right-only",
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
    let custom_left_and = store
        .query_promql(
            r#"histogram_count(http.request.native.mixed.set.left and http.request.native.exphist.mixed.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let custom_left_unless = store
        .query_promql(
            r#"histogram_count(http.request.native.mixed.set.left unless http.request.native.exphist.mixed.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let custom_left_or = store
        .query_promql(
            r#"histogram_count(http.request.native.mixed.set.left or http.request.native.exphist.mixed.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let exponential_left_and = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.mixed.set.left and http.request.native.mixed.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let exponential_left_unless = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.mixed.set.left unless http.request.native.mixed.set.right)"#,
            0,
            40_000,
        )
        .unwrap();
    let exponential_left_or = store
        .query_promql(
            r#"histogram_count(http.request.native.exphist.mixed.set.left or http.request.native.mixed.set.right)"#,
            0,
            40_000,
        )
        .unwrap();

    let expected_and =
        BTreeMap::from([("/native-mixed-set-match".to_string(), vec![(40_000, 25.0)])]);
    let expected_unless = BTreeMap::from([(
        "/native-mixed-set-left-only".to_string(),
        vec![(40_000, 11.0)],
    )]);
    let expected_or = BTreeMap::from([
        (
            "/native-mixed-set-left-only".to_string(),
            vec![(40_000, 11.0)],
        ),
        ("/native-mixed-set-match".to_string(), vec![(40_000, 25.0)]),
        (
            "/native-mixed-set-right-only".to_string(),
            vec![(40_000, 13.0)],
        ),
    ]);

    assert_eq!(samples_by_label(&custom_left_and, "route"), expected_and);
    assert_eq!(
        samples_by_label(&custom_left_unless, "route"),
        expected_unless
    );
    assert_eq!(samples_by_label(&custom_left_or, "route"), expected_or);
    assert_eq!(
        samples_by_label(&exponential_left_and, "route"),
        expected_and
    );
    assert_eq!(
        samples_by_label(&exponential_left_unless, "route"),
        expected_unless
    );
    assert_eq!(samples_by_label(&exponential_left_or, "route"), expected_or);
}
#[test]
fn promql_query_mixed_native_histogram_binary_comparisons_follow_prometheus_semantics() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let custom_samples = [(
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
    )];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(251),
            &custom_samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.mixed.binary.left");
                visit("route", "/native-mixed-binary");
            },
        )
        .unwrap();

    let exponential_samples = [(
        40_000,
        ExponentialHistogramValue {
            count: 7,
            sum: Some(7.0),
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
                counts: vec![3, 4],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: Vec::new(),
            },
        },
    )];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(252),
            &exponential_samples,
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exphist.mixed.binary.right",
                );
                visit("route", "/native-mixed-binary");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let custom = r#"http.request.native.mixed.binary.left{route="/native-mixed-binary"}"#;
    let exponential =
        r#"http.request.native.exphist.mixed.binary.right{route="/native-mixed-binary"}"#;

    let arithmetic_drop = store
        .query_promql(
            &format!("histogram_count({custom} + {exponential})"),
            0,
            40_000,
        )
        .unwrap();
    let reverse_arithmetic_drop = store
        .query_promql(
            &format!("histogram_sum({exponential} - {custom})"),
            0,
            40_000,
        )
        .unwrap();
    let equal_drop = store
        .query_promql(
            &format!("histogram_count({custom} == {exponential})"),
            0,
            40_000,
        )
        .unwrap();
    let ordering_drop = store
        .query_promql(&format!("{custom} > bool {exponential}"), 0, 40_000)
        .unwrap();
    let not_equal = store
        .query_promql(
            &format!("histogram_count({custom} != {exponential})"),
            0,
            40_000,
        )
        .unwrap();
    let reverse_not_equal = store
        .query_promql(
            &format!("histogram_count({exponential} != {custom})"),
            0,
            40_000,
        )
        .unwrap();
    let equal_bool = store
        .query_promql(&format!("{custom} == bool {exponential}"), 0, 40_000)
        .unwrap();
    let not_equal_bool = store
        .query_promql(&format!("{custom} != bool {exponential}"), 0, 40_000)
        .unwrap();
    let reverse_equal_bool = store
        .query_promql(&format!("{exponential} == bool {custom}"), 0, 40_000)
        .unwrap();
    let reverse_not_equal_bool = store
        .query_promql(&format!("{exponential} != bool {custom}"), 0, 40_000)
        .unwrap();

    assert!(arithmetic_drop.is_empty());
    assert!(reverse_arithmetic_drop.is_empty());
    assert!(equal_drop.is_empty());
    assert!(ordering_drop.is_empty());
    assert_eq!(
        samples_by_label(&not_equal, "route"),
        BTreeMap::from([("/native-mixed-binary".to_string(), vec![(40_000, 25.0)])])
    );
    assert_eq!(
        samples_by_label(&reverse_not_equal, "route"),
        BTreeMap::from([("/native-mixed-binary".to_string(), vec![(40_000, 7.0)])])
    );
    for (results, expected) in [
        (&equal_bool, 0.0),
        (&not_equal_bool, 1.0),
        (&reverse_equal_bool, 0.0),
        (&reverse_not_equal_bool, 1.0),
    ] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.to_vec().as_slice(),
            &[("route".to_string(), "/native-mixed-binary".to_string())]
        );
        assert_eq!(results[0].samples, vec![(40_000, expected)]);
    }
}
#[test]
fn promql_query_native_histogram_binary_bool_comparison_returns_scalar_results() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, metric, count, sum, bucket_counts) in [
        (
            SeriesRef::new(235),
            "http.request.native.binary.bool.left",
            25,
            25.0,
            vec![10, 10, 5],
        ),
        (
            SeriesRef::new(236),
            "http.request.native.binary.bool.right",
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
                visit("route", "/native-bool-binary");
            })
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let equal_true = store
        .query_promql(
            r#"http.request.native.binary.bool.left{route="/native-bool-binary"} == bool http.request.native.binary.bool.left{route="/native-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let equal_false = store
        .query_promql(
            r#"http.request.native.binary.bool.left{route="/native-bool-binary"} == bool http.request.native.binary.bool.right{route="/native-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let not_equal_true = store
        .query_promql(
            r#"http.request.native.binary.bool.left{route="/native-bool-binary"} != bool http.request.native.binary.bool.right{route="/native-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let not_equal_false = store
        .query_promql(
            r#"http.request.native.binary.bool.left{route="/native-bool-binary"} != bool http.request.native.binary.bool.left{route="/native-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();
    let greater_than = store
        .query_promql(
            r#"http.request.native.binary.bool.left{route="/native-bool-binary"} > bool http.request.native.binary.bool.right{route="/native-bool-binary"}"#,
            0,
            40_000,
        )
        .unwrap();

    for results in [&equal_true, &equal_false, &not_equal_true, &not_equal_false] {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].labels.to_vec().as_slice(),
            &[("route".to_string(), "/native-bool-binary".to_string())]
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"count by (route)(http.request.mixed.count{route="/mixed-native-scalar-count"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[(
            "route".to_string(),
            "/mixed-native-scalar-count".to_string()
        )]
    );
    assert_eq!(results[0].samples, vec![(10_000, 2.0)]);
}
#[test]
fn promql_query_native_histogram_changes_counts_direct_histogram_changes() {
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
                        count: 12,
                        sum: Some(24.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![3, 5, 4],
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

    let store = open_default_store(tempdir.path());
    let execution = store
        .query_promql(
            r#"changes(http.request.native.changes.direct{route="/native-changes-direct"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(execution.len(), 1);
    assert_eq!(execution[0].samples, vec![(6_000, 1.0)]);
    assert!(
        !execution[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
    assert!(
        execution[0]
            .labels
            .iter()
            .any(|(key, value)| key == "route" && value == "/native-changes-direct")
    );
}
#[test]
fn promql_query_native_exponential_histogram_changes_counts_direct_histogram_changes() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(563),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        scale: 0,
                        zero_threshold: 0.0,
                        zero_count: 0,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
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
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 12,
                        sum: Some(24.0),
                        min: None,
                        max: None,
                        scale: 0,
                        zero_threshold: 0.0,
                        zero_count: 0,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![5, 7],
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
                    "http.request.native.exponential.changes.direct",
                );
                visit("route", "/native-exponential-changes-direct");
                visit("instance", "a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let execution = store
        .query_promql(
            r#"changes(http.request.native.exponential.changes.direct{route="/native-exponential-changes-direct"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(execution.len(), 1);
    assert_eq!(execution[0].samples, vec![(6_000, 1.0)]);
    assert!(
        !execution[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
    assert!(
        execution[0].labels.iter().any(|(key, value)| {
            key == "route" && value == "/native-exponential-changes-direct"
        })
    );
}
#[test]
fn promql_query_native_histogram_resets_counts_observable_component_decrease() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(564),
            &[
                (
                    1_001,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![2, 5, 3],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 5,
                        sum: Some(12.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![1, 3, 1],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.resets.direct");
                visit("route", "/native-resets-direct");
                visit("instance", "a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"resets(http.request.native.resets.direct{route="/native-resets-direct"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 1.0)]);
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
            .any(|(key, value)| key == "route" && value == "/native-resets-direct")
    );
}
#[test]
fn promql_query_native_exponential_histogram_resets_counts_observable_component_decrease() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(565),
            &[
                (
                    1_001,
                    ExponentialHistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        scale: 0,
                        zero_threshold: 0.0,
                        zero_count: 0,
                        metadata: TypedSampleMetadata::default(),
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
                (
                    6_000,
                    ExponentialHistogramValue {
                        count: 5,
                        sum: Some(12.0),
                        min: None,
                        max: None,
                        scale: 0,
                        zero_threshold: 0.0,
                        zero_count: 0,
                        metadata: TypedSampleMetadata::default(),
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
            ],
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "http.request.native.exponential.resets.direct",
                );
                visit("route", "/native-exponential-resets-direct");
                visit("instance", "a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"resets(http.request.native.exponential.resets.direct{route="/native-exponential-resets-direct"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 1.0)]);
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
            .any(|(key, value)| { key == "route" && value == "/native-exponential-resets-direct" })
    );
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

    let store = open_default_store(tempdir.path());
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
        execution.results[0].labels.to_vec().as_slice(),
        &[("route".to_string(), "/native-count-cross-head".to_string())]
    );
    assert_eq!(execution.results[0].samples, vec![(6_000, 1.0)]);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.samples_decoded, 2);
}
#[test]
fn promql_query_native_exponential_group_aggregation_merges_sealed_and_head_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(564),
            &[(
                1_001,
                ExponentialHistogramValue {
                    count: 10,
                    sum: Some(20.0),
                    min: None,
                    max: None,
                    scale: 0,
                    zero_threshold: 0.0,
                    zero_count: 0,
                    metadata: TypedSampleMetadata {
                        reset_hint: CounterResetHint::NotCounterReset,
                        ..TypedSampleMetadata::default()
                    },
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![4, 6],
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
                    "http.request.native.exp.group.cross_head",
                );
                visit("route", "/native-exp-group-cross-head");
                visit("instance", "a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (
                METRIC_NAME_LABEL,
                "http.request.native.exp.group.cross_head",
            ),
            ("instance", "a"),
            ("route", "/native-exp-group-cross-head"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        series,
        6_000,
        SampleValue::ExponentialHistogram(ExponentialHistogramValue {
            count: 20,
            sum: Some(50.0),
            min: None,
            max: None,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            metadata: TypedSampleMetadata {
                reset_hint: CounterResetHint::NotCounterReset,
                ..TypedSampleMetadata::default()
            },
            positive: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![8, 12],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: Vec::new(),
            },
        }),
    )
    .unwrap();

    let store = open_default_store(tempdir.path());
    let execution = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"group by (route)(increase(http.request.native.exp.group.cross_head{route="/native-exp-group-cross-head"}[5s]))"#,
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
        execution.results[0].labels.to_vec().as_slice(),
        &[(
            "route".to_string(),
            "/native-exp-group-cross-head".to_string(),
        )]
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_count(sum by (route)(http.request.native.stale.aggregate{route="/native-stale-sum"}))"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_count(http.request.native.actual_sum{route="/native-suffix-name"})"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_fraction(1, 3, sum without (instance)(rate(http.request.native.fraction.without{route="/native-fraction-without"}[5s])))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_fraction(1 / 1, 2 + 1, rate(http.request.native.fraction{route="/native-fraction"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_fraction(-Inf, Inf, rate(http.request.native.fraction.bounds{route="/native-fraction-bounds"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_fraction(1, 2, rate(http.request.native.exphist.fraction{route="/native-exphist-fraction"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_fraction(1, 2, sum without (instance)(rate(http.request.native.exphist.fraction.without{route="/native-exphist-fraction-without"}[5s])))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
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

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql(
            r#"histogram_fraction(-Inf, Inf, rate(http.request.native.exphist.fraction.bounds{route="/native-exphist-fraction-bounds"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].labels.to_vec().as_slice(),
        &[(
            "route".to_string(),
            "/native-exphist-fraction-bounds".to_string()
        )]
    );
    assert_eq!(results[0].samples, vec![(6_000, 1.0)]);
}
