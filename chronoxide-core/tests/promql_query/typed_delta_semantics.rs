use super::*;

#[test]
fn promql_query_native_histogram_rate_excludes_left_boundary_sample_from_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(214),
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
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.boundary");
                visit("route", "/native-left-open");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate(http.request.native.boundary{route="/native-left-open"}[5s]))"#,
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
fn promql_query_pre_epoch_native_histogram_rate_and_increase_match_virtual_projections() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(225),
            &[
                (
                    0,
                    HistogramValue {
                        count: 5,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![3, 2],
                    },
                ),
                (
                    1_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![6, 4],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "pre.epoch.native.histogram");
                visit("route", "/pre-epoch-histogram");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let query_value = |query: &str| {
        let results = store.query_promql(query, 0, 1_000).unwrap();
        assert_eq!(results.len(), 1, "missing pre-epoch result for {query}");
        assert_eq!(
            results[0].samples.len(),
            1,
            "wrong sample count for {query}"
        );
        assert_eq!(results[0].samples[0].0, 1_000);
        results[0].samples[0].1
    };

    for (query, expected) in [
        (
            r#"histogram_count(increase(pre.epoch.native.histogram{route="/pre-epoch-histogram"}[3s]))"#,
            7.5,
        ),
        (
            r#"increase(pre.epoch.native.histogram_count{route="/pre-epoch-histogram"}[3s])"#,
            7.5,
        ),
        (
            r#"histogram_sum(increase(pre.epoch.native.histogram{route="/pre-epoch-histogram"}[3s]))"#,
            15.0,
        ),
        (
            r#"increase(pre.epoch.native.histogram_sum{route="/pre-epoch-histogram"}[3s])"#,
            15.0,
        ),
        (
            r#"increase(pre.epoch.native.histogram_bucket{route="/pre-epoch-histogram",le="1"}[3s])"#,
            4.5,
        ),
        (
            r#"histogram_count(rate(pre.epoch.native.histogram{route="/pre-epoch-histogram"}[3s]))"#,
            2.5,
        ),
        (
            r#"rate(pre.epoch.native.histogram_count{route="/pre-epoch-histogram"}[3s])"#,
            2.5,
        ),
        (
            r#"rate(pre.epoch.native.histogram_bucket{route="/pre-epoch-histogram",le="1"}[3s])"#,
            1.5,
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
fn promql_query_native_histogram_rate_and_increase_preserve_ordinary_non_finite_sums() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };

    for (idx, (kind, sums)) in [
        (
            "nonfinite-interior",
            [1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 7.0],
        ),
        ("nan-first", [f64::NAN, 2.0, 3.0, 5.0, 7.0]),
        (
            "positive-infinity-first",
            [f64::INFINITY, 2.0, 3.0, 5.0, 7.0],
        ),
        (
            "negative-infinity-first",
            [f64::NEG_INFINITY, 2.0, 3.0, 5.0, 7.0],
        ),
        ("nan-last", [1.0, 2.0, 3.0, 5.0, f64::NAN]),
        (
            "positive-infinity-last",
            [1.0, 2.0, 3.0, 5.0, f64::INFINITY],
        ),
        (
            "negative-infinity-last",
            [1.0, 2.0, 3.0, 5.0, f64::NEG_INFINITY],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let samples = [1_u64, 2, 3, 5, 7]
            .into_iter()
            .zip(sums)
            .enumerate()
            .map(|(sample_idx, (count, sum))| {
                (
                    (sample_idx as u64 + 1) * 10_000,
                    HistogramValue {
                        count,
                        sum: Some(sum),
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![count, 0],
                    },
                )
            })
            .collect::<Vec<_>>();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(230 + idx as u32),
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, "native.nonfinite.sum.histogram");
                    visit("kind", kind);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (kind, expected_increase) in [
        ("nonfinite-interior", 7.0),
        ("nan-first", f64::NAN),
        ("positive-infinity-first", f64::NEG_INFINITY),
        ("negative-infinity-first", f64::INFINITY),
        ("nan-last", f64::NAN),
        ("positive-infinity-last", f64::INFINITY),
        ("negative-infinity-last", f64::NEG_INFINITY),
    ] {
        for (function, expected) in [
            ("increase", expected_increase),
            ("rate", expected_increase / 50.0),
        ] {
            for query in [
                format!(
                    r#"histogram_sum({function}(native.nonfinite.sum.histogram{{kind="{kind}"}}[50s]))"#
                ),
                format!(r#"{function}(native.nonfinite.sum.histogram_sum{{kind="{kind}"}}[50s])"#),
            ] {
                let results = store.query_promql(&query, 0, 50_000).unwrap();
                assert_eq!(results.len(), 1, "missing result for {query}");
                let actual = results[0].samples[0].1;
                if expected.is_nan() {
                    assert!(actual.is_nan(), "expected NaN for {query}, got {actual}");
                    assert_ne!(actual.to_bits(), prometheus_stale_nan().to_bits());
                } else if expected.is_infinite() {
                    assert_eq!(actual, expected, "wrong result for {query}");
                } else {
                    assert!(
                        (actual - expected).abs() < 1e-12,
                        "expected {expected} for {query}, got {actual}"
                    );
                }
            }

            let expected_count = if function == "rate" { 7.0 / 50.0 } else { 7.0 };
            for query in [
                format!(
                    r#"histogram_count({function}(native.nonfinite.sum.histogram{{kind="{kind}"}}[50s]))"#
                ),
                format!(
                    r#"{function}(native.nonfinite.sum.histogram_count{{kind="{kind}"}}[50s])"#
                ),
                format!(
                    r#"{function}(native.nonfinite.sum.histogram_bucket{{kind="{kind}",le="1"}}[50s])"#
                ),
            ] {
                let results = store.query_promql(&query, 0, 50_000).unwrap();
                assert_eq!(results.len(), 1, "non-finite sum removed {query}");
                assert!(
                    (results[0].samples[0].1 - expected_count).abs() < 1e-12,
                    "expected {expected_count} for {query}, got {}",
                    results[0].samples[0].1
                );
            }
        }
    }
}
#[test]
fn promql_query_single_interval_delta_histogram_preserves_signed_and_non_finite_sum() {
    assert_delta_histogram_signed_and_non_finite_sum_path(true);
}
#[test]
fn promql_query_multi_sample_delta_histogram_preserves_signed_and_non_finite_sum() {
    assert_delta_histogram_signed_and_non_finite_sum_path(false);
}
#[test]
fn promql_query_delta_histogram_sum_excludes_pre_range_projection_seed() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let value = |count, sum, start_time_ms| HistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(start_time_ms),
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
            ..TypedSampleMetadata::default()
        },
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(289),
            &[
                (10_000, value(1, -1.0, 0)),
                (20_000, value(2, -2.0, 10_000)),
                (30_000, value(3, -3.0, 20_000)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "delta.pre.range.histogram");
                visit("route", "/delta-pre-range");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (function, expected) in [("increase", -5.0), ("rate", -5.0 / 15.0)] {
        for query in [
            format!(
                r#"histogram_sum({function}(delta.pre.range.histogram{{route="/delta-pre-range"}}[15s]))"#
            ),
            format!(
                r#"{function}(delta.pre.range.histogram_sum{{route="/delta-pre-range"}}[15s])"#
            ),
        ] {
            let results = store.query_promql(&query, 0, 30_000).unwrap();
            assert_eq!(results.len(), 1, "missing result for {query}");
            assert!(
                (results[0].samples[0].1 - expected).abs() < 1e-12,
                "expected {expected} for {query}, got {}",
                results[0].samples[0].1
            );
        }
    }
}
#[test]
fn promql_query_delta_histogram_rejects_missing_or_invalid_interval_starts() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let value = |count, start_time_ms| HistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            start_time_ms,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
            ..TypedSampleMetadata::default()
        },
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    };

    let mut series_ref = 290_u32;
    for (path, invalid_timestamp_ms) in [("single", 10_000_u64), ("multi", 20_000_u64)] {
        for (kind, invalid_start_time_ms) in [
            ("missing", None),
            ("equal", Some(invalid_timestamp_ms)),
            ("future", Some(invalid_timestamp_ms + 1)),
        ] {
            let samples = if path == "single" {
                vec![(10_000, value(5, invalid_start_time_ms))]
            } else {
                vec![
                    (10_000, value(2, Some(0))),
                    (20_000, value(3, invalid_start_time_ms)),
                ]
            };
            writer
                .record_histogram_samples_ordered_with_label_visitor(
                    SeriesRef::new(series_ref),
                    &samples,
                    |visit| {
                        visit(METRIC_NAME_LABEL, "delta.invalid.start.histogram");
                        visit("kind", kind);
                        visit("path", path);
                    },
                )
                .unwrap();
            series_ref += 1;
        }
    }
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (path, eval_time_ms, range_secs) in [("single", 10_000_u64, 10_u64), ("multi", 20_000, 20)]
    {
        for kind in ["missing", "equal", "future"] {
            for function in ["increase", "rate"] {
                let selector = format!(r#"kind="{kind}",path="{path}""#);
                for query in [
                    format!(
                        r#"histogram_count({function}(delta.invalid.start.histogram{{{selector}}}[{range_secs}s]))"#
                    ),
                    format!(
                        r#"histogram_sum({function}(delta.invalid.start.histogram{{{selector}}}[{range_secs}s]))"#
                    ),
                    format!(
                        r#"histogram_quantile(0.5, {function}(delta.invalid.start.histogram{{{selector}}}[{range_secs}s]))"#
                    ),
                    format!(
                        r#"{function}(delta.invalid.start.histogram_count{{{selector}}}[{range_secs}s])"#
                    ),
                    format!(
                        r#"{function}(delta.invalid.start.histogram_sum{{{selector}}}[{range_secs}s])"#
                    ),
                    format!(
                        r#"{function}(delta.invalid.start.histogram_bucket{{{selector},le="1"}}[{range_secs}s])"#
                    ),
                ] {
                    let results = store.query_promql(&query, 0, eval_time_ms).unwrap();
                    assert!(
                        results.is_empty(),
                        "invalid {kind}/{path} delta interval unexpectedly produced {query}: {results:?}"
                    );
                }
            }
        }
    }
}
#[test]
fn promql_query_pre_epoch_native_exponential_histogram_matches_virtual_projections() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };
    let value = |count, sum, counts| ExponentialHistogramValue {
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
            SeriesRef::new(226),
            &[
                (0, value(5, 10.0, vec![3, 2])),
                (1_000, value(10, 20.0, vec![6, 4])),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "pre.epoch.native.exphist");
                visit("route", "/pre-epoch-exphist");
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
        let results = store.query_promql(query, 0, 1_000).unwrap();
        assert_eq!(results.len(), 1, "missing pre-epoch result for {query}");
        assert_eq!(
            results[0].samples.len(),
            1,
            "wrong sample count for {query}"
        );
        assert_eq!(results[0].samples[0].0, 1_000);
        results[0].samples[0].1
    };

    for (query, expected) in [
        (
            r#"histogram_count(increase(pre.epoch.native.exphist{route="/pre-epoch-exphist"}[3s]))"#,
            7.5,
        ),
        (
            r#"increase(pre.epoch.native.exphist_count{route="/pre-epoch-exphist"}[3s])"#,
            7.5,
        ),
        (
            r#"histogram_sum(increase(pre.epoch.native.exphist{route="/pre-epoch-exphist"}[3s]))"#,
            15.0,
        ),
        (
            r#"increase(pre.epoch.native.exphist_sum{route="/pre-epoch-exphist"}[3s])"#,
            15.0,
        ),
        (
            r#"increase(pre.epoch.native.exphist_bucket{route="/pre-epoch-exphist",le="2"}[3s])"#,
            4.5,
        ),
        (
            r#"histogram_count(rate(pre.epoch.native.exphist{route="/pre-epoch-exphist"}[3s]))"#,
            2.5,
        ),
        (
            r#"rate(pre.epoch.native.exphist_count{route="/pre-epoch-exphist"}[3s])"#,
            2.5,
        ),
        (
            r#"rate(pre.epoch.native.exphist_bucket{route="/pre-epoch-exphist",le="2"}[3s])"#,
            1.5,
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
fn promql_query_native_exponential_histogram_preserves_ordinary_non_finite_sums() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };

    for (idx, (kind, sums)) in [
        (
            "nonfinite-interior",
            [1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 7.0],
        ),
        ("nan-first", [f64::NAN, 2.0, 3.0, 5.0, 7.0]),
        (
            "positive-infinity-first",
            [f64::INFINITY, 2.0, 3.0, 5.0, 7.0],
        ),
        (
            "negative-infinity-first",
            [f64::NEG_INFINITY, 2.0, 3.0, 5.0, 7.0],
        ),
        ("nan-last", [1.0, 2.0, 3.0, 5.0, f64::NAN]),
        (
            "positive-infinity-last",
            [1.0, 2.0, 3.0, 5.0, f64::INFINITY],
        ),
        (
            "negative-infinity-last",
            [1.0, 2.0, 3.0, 5.0, f64::NEG_INFINITY],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let samples = [1_u64, 2, 3, 5, 7]
            .into_iter()
            .zip(sums)
            .enumerate()
            .map(|(sample_idx, (count, sum))| {
                (
                    (sample_idx as u64 + 1) * 10_000,
                    ExponentialHistogramValue {
                        count,
                        sum: Some(sum),
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
                    },
                )
            })
            .collect::<Vec<_>>();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(240 + idx as u32),
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, "native.nonfinite.sum.exphist");
                    visit("kind", kind);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = open_default_store_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();
    for (kind, expected_increase) in [
        ("nonfinite-interior", 7.0),
        ("nan-first", f64::NAN),
        ("positive-infinity-first", f64::NEG_INFINITY),
        ("negative-infinity-first", f64::INFINITY),
        ("nan-last", f64::NAN),
        ("positive-infinity-last", f64::INFINITY),
        ("negative-infinity-last", f64::NEG_INFINITY),
    ] {
        for (function, expected) in [
            ("increase", expected_increase),
            ("rate", expected_increase / 50.0),
        ] {
            for query in [
                format!(
                    r#"histogram_sum({function}(native.nonfinite.sum.exphist{{kind="{kind}"}}[50s]))"#
                ),
                format!(r#"{function}(native.nonfinite.sum.exphist_sum{{kind="{kind}"}}[50s])"#),
            ] {
                let results = store.query_promql(&query, 0, 50_000).unwrap();
                assert_eq!(results.len(), 1, "missing result for {query}");
                let actual = results[0].samples[0].1;
                if expected.is_nan() {
                    assert!(actual.is_nan(), "expected NaN for {query}, got {actual}");
                    assert_ne!(actual.to_bits(), prometheus_stale_nan().to_bits());
                } else if expected.is_infinite() {
                    assert_eq!(actual, expected, "wrong result for {query}");
                } else {
                    assert!(
                        (actual - expected).abs() < 1e-12,
                        "expected {expected} for {query}, got {actual}"
                    );
                }
            }

            let expected_count = if function == "rate" { 7.0 / 50.0 } else { 7.0 };
            for query in [
                format!(
                    r#"histogram_count({function}(native.nonfinite.sum.exphist{{kind="{kind}"}}[50s]))"#
                ),
                format!(r#"{function}(native.nonfinite.sum.exphist_count{{kind="{kind}"}}[50s])"#),
                format!(
                    r#"{function}(native.nonfinite.sum.exphist_bucket{{kind="{kind}",le="2"}}[50s])"#
                ),
            ] {
                let results = store.query_promql(&query, 0, 50_000).unwrap();
                assert_eq!(results.len(), 1, "non-finite sum removed {query}");
                assert!(
                    (results[0].samples[0].1 - expected_count).abs() < 1e-12,
                    "expected {expected_count} for {query}, got {}",
                    results[0].samples[0].1
                );
            }
        }
    }
}
#[test]
fn promql_query_single_interval_delta_exponential_histogram_preserves_signed_and_non_finite_sum() {
    assert_delta_exponential_histogram_signed_and_non_finite_sum_path(true);
}
#[test]
fn promql_query_multi_sample_delta_exponential_histogram_preserves_signed_and_non_finite_sum() {
    assert_delta_exponential_histogram_signed_and_non_finite_sum_path(false);
}
#[test]
fn promql_query_delta_exponential_histogram_sum_excludes_pre_range_projection_seed() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let value = |count, sum, start_time_ms| ExponentialHistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(start_time_ms),
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
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
    };
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(299),
            &[
                (10_000, value(1, -1.0, 0)),
                (20_000, value(2, -2.0, 10_000)),
                (30_000, value(3, -3.0, 20_000)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "delta.pre.range.exphist");
                visit("route", "/delta-pre-range");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    for (function, expected) in [("increase", -5.0), ("rate", -5.0 / 15.0)] {
        for query in [
            format!(
                r#"histogram_sum({function}(delta.pre.range.exphist{{route="/delta-pre-range"}}[15s]))"#
            ),
            format!(r#"{function}(delta.pre.range.exphist_sum{{route="/delta-pre-range"}}[15s])"#),
        ] {
            let results = store.query_promql(&query, 0, 30_000).unwrap();
            assert_eq!(results.len(), 1, "missing result for {query}");
            assert!(
                (results[0].samples[0].1 - expected).abs() < 1e-12,
                "expected {expected} for {query}, got {}",
                results[0].samples[0].1
            );
        }
    }
}
#[test]
fn promql_query_delta_exponential_histogram_rejects_missing_or_invalid_interval_starts() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    let value = |count, start_time_ms| ExponentialHistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            start_time_ms,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
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
    };

    let mut series_ref = 300_u32;
    for (path, invalid_timestamp_ms) in [("single", 10_000_u64), ("multi", 20_000_u64)] {
        for (kind, invalid_start_time_ms) in [
            ("missing", None),
            ("equal", Some(invalid_timestamp_ms)),
            ("future", Some(invalid_timestamp_ms + 1)),
        ] {
            let samples = if path == "single" {
                vec![(10_000, value(5, invalid_start_time_ms))]
            } else {
                vec![
                    (10_000, value(2, Some(0))),
                    (20_000, value(3, invalid_start_time_ms)),
                ]
            };
            writer
                .record_exponential_histogram_samples_ordered_with_label_visitor(
                    SeriesRef::new(series_ref),
                    &samples,
                    |visit| {
                        visit(METRIC_NAME_LABEL, "delta.invalid.start.exphist");
                        visit("kind", kind);
                        visit("path", path);
                    },
                )
                .unwrap();
            series_ref += 1;
        }
    }
    writer.flush().unwrap();

    let store = open_default_store_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();
    for (path, eval_time_ms, range_secs) in [("single", 10_000_u64, 10_u64), ("multi", 20_000, 20)]
    {
        for kind in ["missing", "equal", "future"] {
            for function in ["increase", "rate"] {
                let selector = format!(r#"kind="{kind}",path="{path}""#);
                for query in [
                    format!(
                        r#"histogram_count({function}(delta.invalid.start.exphist{{{selector}}}[{range_secs}s]))"#
                    ),
                    format!(
                        r#"histogram_sum({function}(delta.invalid.start.exphist{{{selector}}}[{range_secs}s]))"#
                    ),
                    format!(
                        r#"histogram_quantile(0.5, {function}(delta.invalid.start.exphist{{{selector}}}[{range_secs}s]))"#
                    ),
                    format!(
                        r#"{function}(delta.invalid.start.exphist_count{{{selector}}}[{range_secs}s])"#
                    ),
                    format!(
                        r#"{function}(delta.invalid.start.exphist_sum{{{selector}}}[{range_secs}s])"#
                    ),
                    format!(
                        r#"{function}(delta.invalid.start.exphist_bucket{{{selector},le="2"}}[{range_secs}s])"#
                    ),
                ] {
                    let results = store.query_promql(&query, 0, eval_time_ms).unwrap();
                    assert!(
                        results.is_empty(),
                        "invalid {kind}/{path} delta interval unexpectedly produced {query}: {results:?}"
                    );
                }
            }
        }
    }
}
