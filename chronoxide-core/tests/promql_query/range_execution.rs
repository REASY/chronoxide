use super::*;

#[test]
fn promql_query_range_evaluates_expression_at_each_step() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = open_default_store(tempdir.path());

    let results = store
        .query_promql_range("time() + 1", 1_000, 5_000, 2_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].labels.to_vec().as_slice(), &[]);
    assert_eq!(
        results[0].samples,
        vec![(1_000, 2.0), (3_000, 4.0), (5_000, 6.0)]
    );
}
#[test]
fn promql_manifest_append_order_wins_equal_timestamps_for_repeated_and_one_pass() {
    let tempdir = tempfile::tempdir().unwrap();
    let labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "one_pass_duplicate_counter".to_string(),
        ),
        ("route".to_string(), "/duplicates".to_string()),
    ];
    let config = |ulid| {
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(600))
            .with_segment_id_provider(FixedUlidSegmentIdProvider { ulid })
    };

    let mut older = SegmentWriter::new(config("7ZZZZZZZZZZZZZZZZZZZZZZZZZ")).unwrap();
    write_series(
        &mut older,
        SeriesRef::new(715),
        labels.clone(),
        &[
            (0, 0.0),
            (1_000, 1.0),
            (2_000, 2.0),
            (3_000, 3.0),
            (4_000, 4.0),
            (5_000, 5.0),
            (6_000, 6.0),
        ],
    );
    older.flush().unwrap();
    drop(older);

    // This later manifest entry is deliberately lexically earlier, so sorting
    // segment IDs would select the wrong equal-timestamp values.
    let mut newer = SegmentWriter::new(config("00000000000000000000000001")).unwrap();
    write_series(
        &mut newer,
        SeriesRef::new(715),
        labels,
        &[(2_000, 20.0), (4_000, 40.0)],
    );
    newer.flush().unwrap();

    let store = SegmentStoreReader::open_manifest_published(
        tempdir.path(),
        tempdir.path().join("manifest"),
    )
    .unwrap();
    let merged = store
        .query_promql("one_pass_duplicate_counter", 0, 6_000)
        .unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0].samples,
        vec![
            (0, 0.0),
            (1_000, 1.0),
            (2_000, 20.0),
            (3_000, 3.0),
            (4_000, 40.0),
            (5_000, 5.0),
            (6_000, 6.0),
        ]
    );

    let query = "sum by (route)(rate(one_pass_duplicate_counter[4s]))";
    let mut repeated = store.query_session().unwrap();
    let expected = repeated
        .query_promql_range_with_limits(query, 4_000, 6_000, 1_000, QueryLimits::unlimited())
        .unwrap();
    let mut one_pass = store.query_session().unwrap();
    one_pass
        .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
        .unwrap();
    let actual = one_pass
        .query_promql_range_with_limits(query, 4_000, 6_000, 1_000, QueryLimits::unlimited())
        .unwrap();

    assert_scalar_results_bitwise_eq(&actual.results, &expected.results, query);
    assert_eq!(
        actual.semantic_fingerprint_sha256(),
        expected.semantic_fingerprint_sha256()
    );
    assert_eq!(
        actual.portable_semantic_fingerprint_sha256(),
        expected.portable_semantic_fingerprint_sha256()
    );
    let summary = one_pass.last_range_execution_summary().copied().unwrap();
    assert_eq!(
        summary.effective_mode,
        RangeExecutionMode::OnePassAssumeScalar
    );
    assert_eq!(summary.fallback_reason, None);
    assert_eq!(summary.terminal_reason, None);
    assert_eq!(summary.source_series, 1);
    assert_eq!(summary.source_samples, 7);
}
#[test]
fn promql_manifest_append_order_keeps_typed_winner_metadata_for_every_payload_kind() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = |ulid| {
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(600))
            .with_segment_id_provider(FixedUlidSegmentIdProvider { ulid })
    };

    let mut older = SegmentWriter::new(config("7ZZZZZZZZZZZZZZZZZZZZZZZZZ")).unwrap();
    write_manifest_precedence_typed_payloads(&mut older, false);
    older.flush().unwrap();
    drop(older);

    // The authoritative later manifest record is deliberately lexically
    // earlier. Every equal-timestamp winner, including its typed metadata,
    // must still come from this segment.
    let mut newer = SegmentWriter::new(config("00000000000000000000000001")).unwrap();
    write_manifest_precedence_typed_payloads(&mut newer, true);
    newer.flush().unwrap();
    drop(newer);

    let manifest_dir = tempdir.path().join("manifest");
    let store = SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir).unwrap();
    assert_manifest_precedence_samples(
        &store,
        "manifest_precedence_float",
        &[(1_000, 2.0), (5_000, 4.0)],
    );
    for query in [
        "manifest_precedence_histogram_count",
        r#"manifest_precedence_histogram_bucket{le="+Inf"}"#,
        "manifest_precedence_exponential_histogram_count",
        r#"manifest_precedence_exponential_histogram_bucket{le="+Inf"}"#,
        "manifest_precedence_summary_count",
    ] {
        assert_manifest_precedence_samples(&store, query, &[(1_000, 2.0), (5_000, 6.0)]);
    }
    for query in [
        "manifest_precedence_histogram_sum",
        "manifest_precedence_exponential_histogram_sum",
        "manifest_precedence_summary_sum",
    ] {
        assert_manifest_precedence_samples(&store, query, &[(1_000, 2.0), (5_000, 6.0)]);
    }
    assert_manifest_precedence_samples(
        &store,
        r#"manifest_precedence_summary{quantile="0.5"}"#,
        &[(1_000, 2.0), (5_000, 4.0)],
    );

    // Before the per-sample temporality sidecar, the shadowed cumulative
    // samples poisoned these retained delta winners as Mixed and rate omitted
    // the result entirely.
    for query in [
        "rate(manifest_precedence_histogram_count[5s])",
        "rate(manifest_precedence_exponential_histogram_count[5s])",
        "rate(manifest_precedence_summary_count[5s])",
        "histogram_count(rate(manifest_precedence_histogram[5s]))",
        "histogram_count(rate(manifest_precedence_exponential_histogram[5s]))",
    ] {
        let results = store.query_promql(query, 0, 5_000).unwrap();
        assert_eq!(results.len(), 1, "typed winner metadata lost for {query}");
        assert_eq!(results[0].samples.len(), 1, "{query}");
        assert_eq!(results[0].samples[0].0, 5_000, "{query}");
        assert!(results[0].samples[0].1.is_finite(), "{query}");
    }

    // The cross-segment scheduler has its own result assembly path. Stable
    // equal-timestamp order must agree with the default path for both native
    // histogram representations.
    for query in [
        "histogram_count(rate(manifest_precedence_histogram[5s]))",
        "histogram_count(rate(manifest_precedence_exponential_histogram[5s]))",
    ] {
        let mut default_session = store.query_session().unwrap();
        let expected = default_session
            .query_promql_with_limits(query, 0, 5_000, QueryLimits::unlimited())
            .unwrap();
        let mut scheduled_session = store.query_session().unwrap();
        scheduled_session.set_experimental_cross_segment_chunk_reads(true);
        let actual = scheduled_session
            .query_promql_with_limits(query, 0, 5_000, QueryLimits::unlimited())
            .unwrap();
        assert_eq!(actual.results, expected.results, "{query}");
        assert_eq!(actual.stats, expected.stats, "{query}");
    }

    // A tombstoned segment that is sealed again becomes the latest live
    // manifest entry and therefore regains equal-timestamp precedence.
    let older_id = SegmentId::parse_dir_name("seg-0-600000-7ZZZZZZZZZZZZZZZZZZZZZZZZZ").unwrap();
    let current = read_current(&manifest_dir).unwrap().unwrap();
    let mut manifest = ManifestWriter::open_append(&manifest_dir, &current).unwrap();
    manifest
        .append(&ManifestRecord::SegmentDeleted {
            segment_id: older_id.dir_name(),
        })
        .unwrap();
    manifest
        .append(&ManifestRecord::SegmentSealed(
            ManifestSegment::new(
                older_id.dir_name(),
                older_id.start_ms(),
                older_id.end_ms(),
                None,
            )
            .unwrap(),
        ))
        .unwrap();
    manifest.sync_all().unwrap();

    let resealed =
        SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir).unwrap();
    assert_manifest_precedence_samples(
        &resealed,
        "manifest_precedence_histogram_count",
        &[(1_000, 10.0), (5_000, 20.0)],
    );
    let results = resealed
        .query_promql("rate(manifest_precedence_histogram_count[5s])", 0, 5_000)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
}
#[test]
fn promql_active_head_keeps_complete_typed_metadata_when_shadowing_sealed_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let cumulative = manifest_precedence_metadata(OtlpAggregationTemporality::Cumulative, None);
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(720),
            &[
                (1_000, manifest_precedence_histogram(10, cumulative)),
                (5_000, manifest_precedence_histogram(20, cumulative)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "active_head_precedence_histogram");
                visit("route", "/head-precedence");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "active_head_precedence_histogram"),
            ("route", "/head-precedence"),
        ],
    );
    let mut head = test_head();
    head.record_sample(
        series,
        1_000,
        SampleValue::Histogram(manifest_precedence_histogram(
            2,
            manifest_precedence_metadata(OtlpAggregationTemporality::Delta, Some(0)),
        )),
    )
    .unwrap();
    head.record_sample(
        series,
        5_000,
        SampleValue::Histogram(manifest_precedence_histogram(
            4,
            manifest_precedence_metadata(OtlpAggregationTemporality::Delta, Some(1_000)),
        )),
    )
    .unwrap();

    let store = open_default_store(tempdir.path());
    let projected = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"active_head_precedence_histogram_count{route="/head-precedence"}"#,
            0,
            5_000,
        )
        .unwrap();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].samples, vec![(1_000, 2.0), (5_000, 6.0)]);

    for query in [
        r#"rate(active_head_precedence_histogram_count{route="/head-precedence"}[5s])"#,
        r#"histogram_count(rate(active_head_precedence_histogram{route="/head-precedence"}[5s]))"#,
    ] {
        let results = store
            .query_promql_with_head(&head, &label_store, query, 0, 5_000)
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "typed head winner metadata lost for {query}"
        );
        assert_eq!(results[0].samples.len(), 1, "{query}");
        assert_eq!(results[0].samples[0].0, 5_000, "{query}");
        assert!(results[0].samples[0].1.is_finite(), "{query}");
    }
}
#[test]
fn promql_one_pass_assume_scalar_matches_repeated_sum_and_count_rate_steps() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = write_one_pass_scalar_fixture(tempdir.path());

    for (query, expected_source_series, expected_source_samples) in [
        ("sum by (route)(rate(one_pass_counter[4s]))", 2, 20),
        ("count by (route)(rate(one_pass_counter[4s]))", 2, 20),
        ("sum by (route)(rate(one_pass_int_counter[4s]))", 1, 10),
        (
            "sum by (route)(rate(one_pass_nonfinite_counter[4s]))",
            1,
            10,
        ),
        ("sum by (route)(rate(one_pass_missing_counter[4s]))", 0, 0),
    ] {
        let mut repeated = store.query_session().unwrap();
        let expected = repeated
            .query_promql_range_with_limits(query, 1_000, 9_000, 2_000, QueryLimits::unlimited())
            .unwrap();

        let mut one_pass = store.query_session().unwrap();
        one_pass
            .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
            .unwrap();
        let actual = one_pass
            .query_promql_range_with_limits(query, 1_000, 9_000, 2_000, QueryLimits::unlimited())
            .unwrap();

        assert_scalar_results_bitwise_eq(&actual.results, &expected.results, query);
        assert_eq!(
            actual.semantic_fingerprint_sha256(),
            expected.semantic_fingerprint_sha256(),
            "{query}"
        );
        assert_eq!(
            actual.portable_semantic_fingerprint_sha256(),
            expected.portable_semantic_fingerprint_sha256(),
            "{query}"
        );
        assert!(
            actual.stats.chunk_reads <= expected.stats.chunk_reads,
            "{query}"
        );

        let summary = one_pass.last_range_execution_summary().copied().unwrap();
        assert_eq!(
            summary.requested_mode,
            RangeExecutionMode::OnePassAssumeScalar
        );
        assert_eq!(
            summary.effective_mode,
            RangeExecutionMode::OnePassAssumeScalar
        );
        assert_eq!(summary.fallback_reason, None);
        assert_eq!(summary.terminal_reason, None);
        assert_eq!(summary.evaluation_count, 5);
        assert_eq!(summary.union_start_ms, Some(0));
        assert_eq!(summary.union_end_ms, Some(9_000));
        assert_eq!(summary.source_series, expected_source_series);
        assert_eq!(summary.source_samples, expected_source_samples);
        if expected_source_samples == 0 {
            assert_eq!(summary.estimated_retained_bytes_peak, 0);
        } else {
            assert!(summary.estimated_retained_bytes_peak > 0);
        }
        assert_eq!(summary.retained_bytes_after_finalize, 0);
        assert!(!summary.preallocation_governed);
        assert!(summary.cache_bypassed);
        assert_eq!(
            one_pass.last_range_scalar_cache_summary().copied().unwrap(),
            chronoxide_core::storage::segment::RangeScalarCacheSummary {
                configured_budget_bytes:
                    chronoxide_core::storage::segment::DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES,
                ..chronoxide_core::storage::segment::RangeScalarCacheSummary::default()
            }
        );
    }

    let query = "sum by (route)(rate(one_pass_counter[4s]))";
    for (materialization, label_storage) in [
        (
            QueryLabelMaterializationPolicy::DemandDriven,
            QueryLabelStoragePolicy::CompactIds,
        ),
        (
            QueryLabelMaterializationPolicy::DemandDriven,
            QueryLabelStoragePolicy::OwnedStrings,
        ),
        (
            QueryLabelMaterializationPolicy::Full,
            QueryLabelStoragePolicy::CompactIds,
        ),
        (
            QueryLabelMaterializationPolicy::Full,
            QueryLabelStoragePolicy::OwnedStrings,
        ),
    ] {
        let mut repeated = store.query_session().unwrap();
        repeated.set_label_materialization_policy(materialization);
        repeated
            .set_query_label_storage_policy(label_storage)
            .unwrap();
        let expected = repeated
            .query_promql_range_with_limits(query, 1_000, 9_000, 2_000, QueryLimits::unlimited())
            .unwrap();

        let mut one_pass = store.query_session().unwrap();
        one_pass.set_label_materialization_policy(materialization);
        one_pass
            .set_query_label_storage_policy(label_storage)
            .unwrap();
        one_pass
            .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
            .unwrap();
        let actual = one_pass
            .query_promql_range_with_limits(query, 1_000, 9_000, 2_000, QueryLimits::unlimited())
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
}
#[test]
fn promql_one_pass_assume_scalar_matches_repeated_schedule_edges() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = write_one_pass_scalar_fixture(tempdir.path());
    let query = "sum by (route)(rate(one_pass_counter[4s]))";

    for (start_ms, end_ms, step_ms, expected_evaluations) in [
        (1_000, 8_500, 2_000, 4),
        (5_000, 5_000, 1_000, 1),
        (u64::MAX - 4_000, u64::MAX, 2_000, 3),
    ] {
        let mut repeated = store.query_session().unwrap();
        let expected = repeated
            .query_promql_range_with_limits(
                query,
                start_ms,
                end_ms,
                step_ms,
                QueryLimits::unlimited(),
            )
            .unwrap();

        let mut one_pass = store.query_session().unwrap();
        one_pass
            .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
            .unwrap();
        let actual = one_pass
            .query_promql_range_with_limits(
                query,
                start_ms,
                end_ms,
                step_ms,
                QueryLimits::unlimited(),
            )
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
        assert_eq!(
            one_pass
                .last_range_execution_summary()
                .unwrap()
                .evaluation_count,
            expected_evaluations
        );
    }
}
#[test]
fn promql_one_pass_assume_scalar_falls_back_before_io_for_unproved_shapes_and_limits() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = write_one_pass_scalar_fixture(tempdir.path());
    let cases = [
        (
            "avg by (route)(rate(one_pass_counter[4s]))",
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::UnsupportedAggregation,
        ),
        (
            "sum without (instance)(rate(one_pass_counter[4s]))",
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::UnsupportedGrouping,
        ),
        (
            "sum by (route)(increase(one_pass_counter[4s]))",
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::UnsupportedRangeFunction,
        ),
        (
            "sum by (route)(rate(one_pass_counter[4s] offset 1s))",
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::UnsupportedRootExpression,
        ),
        (
            "sum by (route)(rate(one_pass_counter[4s])) + 1",
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::UnsupportedRootExpression,
        ),
        (
            r#"label_replace(sum by (route)(rate(one_pass_counter[4s])), "copied_route", "$1", "route", "(.*)")"#,
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::UnsupportedRootExpression,
        ),
        (
            r#"sum by (route)(rate({route="/api"}[4s]))"#,
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::MissingDirectMetricName,
        ),
        (
            "sum by (route)(rate(one_pass_counter_count[4s]))",
            1_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::ProjectionLikeMetricName,
        ),
        (
            "sum by (route)(rate(one_pass_counter[1s]))",
            2_000,
            QueryLimits::unlimited(),
            RangeExecutionFallbackReason::StepExceedsWindow,
        ),
        (
            "sum by (route)(rate(one_pass_counter[4s]))",
            1_000,
            QueryLimits {
                max_matched_series: Some(u64::MAX),
                ..QueryLimits::unlimited()
            },
            RangeExecutionFallbackReason::FiniteLimits,
        ),
    ];

    for (query, step_ms, limits, expected_reason) in cases {
        let mut repeated = store.query_session().unwrap();
        let expected = repeated
            .query_promql_range_with_limits(query, 5_000, 9_000, step_ms, limits)
            .unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
            .unwrap();
        let actual = session
            .query_promql_range_with_limits(query, 5_000, 9_000, step_ms, limits)
            .unwrap();
        assert_eq!(actual.results, expected.results, "{query}");
        assert_eq!(actual.stats, expected.stats, "{query}");
        let summary = session.last_range_execution_summary().copied().unwrap();
        assert_eq!(
            summary.effective_mode,
            RangeExecutionMode::Repeated,
            "{query}"
        );
        assert_eq!(summary.fallback_reason, Some(expected_reason), "{query}");
        assert_eq!(summary.terminal_reason, None, "{query}");
        assert!(!summary.cache_bypassed, "{query}");
        assert_eq!(summary.union_start_ms, None, "{query}");
        assert_eq!(summary.source_series, 0, "{query}");
    }

    let mut frozen = store.query_session().unwrap();
    frozen
        .query_promql_range(
            "sum by (route)(rate(one_pass_counter[4s]))",
            5_000,
            9_000,
            1_000,
        )
        .unwrap();
    assert!(
        frozen
            .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
            .is_err()
    );
}
#[test]
fn promql_one_pass_assume_scalar_finalizes_summaries_for_parse_and_bound_errors() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = write_one_pass_scalar_fixture(tempdir.path());

    for (query, start_ms, end_ms, step_ms) in [
        ("(", 1_000, 9_000, 1_000),
        (
            "sum by (route)(rate(one_pass_counter[4s]))",
            9_000,
            1_000,
            1_000,
        ),
        (
            "sum by (route)(rate(one_pass_counter[4s]))",
            1_000,
            9_000,
            0,
        ),
    ] {
        let mut session = store.query_session().unwrap();
        session
            .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
            .unwrap();
        assert!(
            session
                .query_promql_range_with_limits(
                    query,
                    start_ms,
                    end_ms,
                    step_ms,
                    QueryLimits::unlimited(),
                )
                .is_err()
        );
        let summary = session.last_range_execution_summary().copied().unwrap();
        assert_eq!(
            summary.requested_mode,
            RangeExecutionMode::OnePassAssumeScalar
        );
        assert_eq!(summary.effective_mode, RangeExecutionMode::Repeated);
        assert_eq!(
            summary.fallback_reason,
            Some(RangeExecutionFallbackReason::InvalidQuery)
        );
        assert_eq!(summary.terminal_reason, None);
        assert_eq!(summary.evaluation_count, 0);
        assert_eq!(summary.retained_bytes_after_finalize, 0);
        assert!(session.last_range_scalar_cache_summary().is_some());
    }
}
#[test]
fn promql_one_pass_assume_scalar_errors_when_union_decode_observes_typed_source() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(720),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(3.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            temporality: OtlpAggregationTemporality::Cumulative,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                ),
                (
                    5_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(7.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            temporality: OtlpAggregationTemporality::Cumulative,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 2],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "one_pass_typed");
                visit("route", "/typed");
            },
        )
        .unwrap();
    write_one_pass_typed_variants(&mut writer);
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let projected_query = "sum by (route)(rate(one_pass_typed_count[4s]))";
    let mut projected_repeated = store.query_session().unwrap();
    projected_repeated
        .set_range_scalar_cache_budget_bytes(1024 * 1024)
        .unwrap();
    let expected = projected_repeated
        .query_promql_range(projected_query, 5_000, 9_000, 1_000)
        .unwrap();
    let expected_cache = projected_repeated
        .last_range_scalar_cache_summary()
        .copied()
        .unwrap();
    let mut projected_one_pass = store.query_session().unwrap();
    projected_one_pass
        .set_range_scalar_cache_budget_bytes(1024 * 1024)
        .unwrap();
    projected_one_pass
        .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
        .unwrap();
    let actual = projected_one_pass
        .query_promql_range(projected_query, 5_000, 9_000, 1_000)
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        projected_one_pass
            .last_range_scalar_cache_summary()
            .copied()
            .unwrap(),
        expected_cache
    );
    assert_eq!(
        projected_one_pass
            .last_range_execution_summary()
            .unwrap()
            .fallback_reason,
        Some(RangeExecutionFallbackReason::ProjectionLikeMetricName)
    );

    for (query, expected_source_series, expected_source_samples) in [
        ("sum by (route)(rate(one_pass_typed[4s]))", 0, 0),
        ("count by (route)(rate(one_pass_typed[4s]))", 0, 0),
        ("sum by (route)(rate(one_pass_typed_exponential[4s]))", 0, 0),
        ("sum by (route)(rate(one_pass_typed_summary[4s]))", 1, 2),
        ("sum by (route)(rate(one_pass_typed_delta[4s]))", 0, 0),
        (
            "sum by (route)(rate(one_pass_typed_mixed_temporality[4s]))",
            0,
            0,
        ),
        ("sum by (route)(rate(one_pass_typed_mixed_kind[4s]))", 1, 2),
    ] {
        let mut session = store.query_session().unwrap();
        session
            .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
            .unwrap();
        let error = session
            .query_promql_range(query, 5_000, 9_000, 1_000)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("one_pass_assume_scalar observed typed source chunks")
        );
        let summary = session.last_range_execution_summary().copied().unwrap();
        assert_eq!(
            summary.effective_mode,
            RangeExecutionMode::OnePassAssumeScalar
        );
        assert_eq!(summary.fallback_reason, None);
        assert_eq!(
            summary.terminal_reason,
            Some(RangeExecutionTerminalReason::TypedSourceObservedAfterDecode)
        );
        assert_eq!(summary.evaluation_count, 0);
        // `AllPromql` final-label filtering may hide typed projections. The
        // mixed-kind case deliberately retains its float source as well.
        assert_eq!(summary.source_series, expected_source_series, "{query}");
        assert_eq!(summary.source_samples, expected_source_samples, "{query}");
        assert!(summary.cache_bypassed);
        assert_eq!(summary.retained_bytes_after_finalize, 0);
    }
}
#[test]
fn promql_query_range_covers_stored_selectors_offsets_functions_and_session() {
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
            (METRIC_NAME_LABEL.to_string(), "range.cpu".to_string()),
            ("job".to_string(), "api-v1".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)],
    );
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let selector = store
        .query_promql_range(r#"range.cpu{instance="a"}"#, 1_000, 5_000, 2_000)
        .unwrap();
    assert_eq!(selector.len(), 1);
    assert_eq!(
        selector[0].samples,
        vec![(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)]
    );

    let offset = store
        .query_promql_range(r#"range.cpu{instance="a"} offset 2s"#, 3_000, 7_000, 2_000)
        .unwrap();
    assert_eq!(offset.len(), 1);
    assert_eq!(
        offset[0].samples,
        vec![(3_000, 1.0), (5_000, 3.0), (7_000, 5.0)]
    );

    let sum = store
        .query_promql_range(
            r#"sum_over_time(range.cpu{instance="a"}[3s])"#,
            3_000,
            5_000,
            2_000,
        )
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(3_000, 4.0), (5_000, 8.0)]);
    assert_eq!(
        sum[0].labels.to_vec().as_slice(),
        &[
            ("instance".to_string(), "a".to_string()),
            ("job".to_string(), "api-v1".to_string())
        ]
    );

    let labels = store
        .query_promql_range(
            r#"label_replace(label_join(range.cpu{instance="a"}, "target", "/", "job", "instance"), "service", "$1", "job", "(.+)-v[0-9]+")"#,
            1_000,
            5_000,
            2_000,
        )
        .unwrap();
    assert_eq!(labels.len(), 1);
    assert_eq!(
        labels[0].samples,
        vec![(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)]
    );
    assert!(
        labels[0]
            .labels
            .iter()
            .any(|(key, value)| key == "service" && value == "api")
    );
    assert!(
        labels[0]
            .labels
            .iter()
            .any(|(key, value)| key == "target" && value == "api-v1/a")
    );

    let sgn = store
        .query_promql_range(r#"sgn(range.cpu{instance="a"} - 3)"#, 1_000, 5_000, 2_000)
        .unwrap();
    assert_eq!(sgn.len(), 1);
    assert_eq!(
        sgn[0].samples,
        vec![(1_000, -1.0), (3_000, 0.0), (5_000, 1.0)]
    );

    let scalar = store
        .query_promql_range(r#"scalar(range.cpu{instance="a"})"#, 1_000, 5_000, 2_000)
        .unwrap();
    assert_eq!(scalar.len(), 1);
    assert_eq!(scalar[0].labels.to_vec().as_slice(), &[]);
    assert_eq!(
        scalar[0].samples,
        vec![(1_000, 1.0), (3_000, 3.0), (5_000, 5.0)]
    );

    let mut session = store.query_session().unwrap();
    let session_sum = session
        .query_promql_range(
            r#"sum_over_time(range.cpu{instance="a"}[3s])"#,
            3_000,
            5_000,
            2_000,
        )
        .unwrap();
    assert_eq!(session_sum, sum);
}
#[test]
fn promql_query_range_with_head_covers_selectors_and_range_functions() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "range.head.cpu"),
            ("instance", "a"),
            ("job", "api"),
        ],
    );
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    write_series(
        &mut writer,
        series,
        vec![
            (METRIC_NAME_LABEL.to_string(), "range.head.cpu".to_string()),
            ("job".to_string(), "api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(1_000, 1.0)],
    );
    writer.flush().unwrap();

    let mut head = test_head();
    head.record_sample(series, 3_000, SampleValue::Float(3.0))
        .unwrap();

    let store = open_default_store(tempdir.path());
    let selector = store
        .query_promql_range_with_head(
            &head,
            &label_store,
            r#"range.head.cpu{instance="a"}"#,
            1_000,
            3_000,
            2_000,
        )
        .unwrap();
    assert_eq!(selector.len(), 1);
    assert_eq!(selector[0].samples, vec![(1_000, 1.0), (3_000, 3.0)]);

    let sum = store
        .query_promql_range_with_head(
            &head,
            &label_store,
            r#"sum_over_time(range.head.cpu{instance="a"}[3s])"#,
            3_000,
            3_000,
            1_000,
        )
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(3_000, 4.0)]);
}
#[test]
fn promql_query_range_projects_histogram_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(600),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(91),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 5,
                        sum: Some(9.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![3, 2],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 7,
                        sum: Some(13.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![4, 3],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "range.request.duration");
                visit("route", "/range");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let count = store
        .query_promql_range(
            r#"range.request.duration_count{route="/range"}"#,
            1_000,
            3_000,
            2_000,
        )
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(1_000, 5.0), (3_000, 7.0)]);

    let bucket = store
        .query_promql_range(
            r#"range.request.duration_bucket{route="/range",le="1"}"#,
            1_000,
            3_000,
            2_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(1_000, 3.0), (3_000, 4.0)]);
    assert!(
        bucket[0]
            .labels
            .iter()
            .any(|(key, value)| key == "le" && value == "1")
    );
}
