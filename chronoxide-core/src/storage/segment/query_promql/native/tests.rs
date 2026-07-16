use super::*;

#[test]
fn native_histogram_range_selective_labels_use_complete_dropped_identity() {
    let complete_labels = vec![
        (METRIC_NAME_LABEL.to_owned(), String::from("requests")),
        (String::from("instance"), String::from("a")),
        (String::from("service"), String::from("api")),
    ];
    let source_id = segment_series_id(&complete_labels);
    let dropped_id = segment_series_id(&complete_labels[1..]);
    let mut full =
        PromqlHistogramSeries::new(source_id, shared_query_labels(complete_labels.clone()));
    let mut selective = PromqlHistogramSeries::new(
        source_id,
        shared_query_labels(vec![
            (METRIC_NAME_LABEL.to_owned(), String::from("requests")),
            (String::from("service"), String::from("api")),
        ]),
    );
    selective.mark_labels_incomplete(Some(dropped_id));
    for timestamp_ms in [1_000, 2_000] {
        let sample = PromqlHistogramSample {
            timestamp_ms,
            start_time_ms: None,
            count: timestamp_ms as f64 / 1_000.0,
            sum: Some(timestamp_ms as f64 / 1_000.0),
            explicit_bounds: Arc::from([1.0]),
            bucket_counts: vec![timestamp_ms as f64 / 1_000.0, 0.0],
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::NotCounterReset,
            stale: false,
        };
        full.push_sample(sample.clone());
        selective.push_sample(sample);
    }
    let function = PromqlRangeFunction {
        kind: PromqlRangeFunctionKind::Rate,
        selector: PromqlSelector {
            metric_name: Some(String::from("requests")),
            matchers: Vec::new(),
        },
        range_ms: 2_000,
    };

    let full = evaluate_histogram_range_function(&function, vec![full], 2_000);
    let selective = evaluate_histogram_range_function(&function, vec![selective], 2_000);

    assert_eq!(full[0].series_id, dropped_id);
    assert_eq!(
        full[0].labels.as_ref(),
        &complete_labels[1..],
        "native rate must drop the metric name on the complete path"
    );
    assert!(full[0].labels_complete);
    assert_eq!(selective[0].series_id, dropped_id);
    assert_eq!(
        selective[0].labels.as_ref(),
        &[(String::from("service"), String::from("api"))]
    );
    assert!(!selective[0].labels_complete);
}

#[test]
fn native_exponential_terminal_count_matches_full_and_selective_range_labels() {
    let complete_labels = vec![
        (METRIC_NAME_LABEL.to_owned(), String::from("latency")),
        (String::from("instance"), String::from("a")),
        (String::from("service"), String::from("api")),
    ];
    let source_id = segment_series_id(&complete_labels);
    let dropped_id = segment_series_id(&complete_labels[1..]);
    let mut full =
        PromqlExponentialHistogramSeries::new(source_id, shared_query_labels(complete_labels));
    let mut selective = PromqlExponentialHistogramSeries::new(
        source_id,
        shared_query_labels(vec![
            (METRIC_NAME_LABEL.to_owned(), String::from("latency")),
            (String::from("service"), String::from("api")),
        ]),
    );
    selective.mark_labels_incomplete(Some(dropped_id));
    for timestamp_ms in [1_000, 2_000] {
        let count = timestamp_ms as f64 / 1_000.0;
        let sample = PromqlExponentialHistogramSample {
            timestamp_ms,
            start_time_ms: None,
            count,
            sum: Some(count),
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            positive: PromqlExponentialHistogramBuckets {
                offset: 0,
                counts: vec![count],
                sparse_counts: Vec::new(),
            },
            negative: PromqlExponentialHistogramBuckets::empty(),
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::NotCounterReset,
            stale: false,
        };
        full.push_sample(sample.clone());
        selective.push_sample(sample);
    }
    let function = PromqlRangeFunction {
        kind: PromqlRangeFunctionKind::Increase,
        selector: PromqlSelector {
            metric_name: Some(String::from("latency")),
            matchers: Vec::new(),
        },
        range_ms: 2_000,
    };
    let aggregation = PromqlAggregation {
        op: PromqlAggregationOp::Count,
        grouping: PromqlAggregationGrouping::By(vec![String::from("service")]),
        input: Box::new(PromqlQuery::Scalar(0.0)),
    };

    let full = evaluate_native_histogram_scalar_aggregation(
        &aggregation,
        Vec::new(),
        Vec::new(),
        evaluate_exponential_histogram_range_function(&function, vec![full], 2_000),
        2_000,
    );
    let selective = evaluate_native_histogram_scalar_aggregation(
        &aggregation,
        Vec::new(),
        Vec::new(),
        evaluate_exponential_histogram_range_function(&function, vec![selective], 2_000),
        2_000,
    );

    assert_eq!(full, selective);
    assert_eq!(
        selective[0].labels.as_ref(),
        &[(String::from("service"), String::from("api"))]
    );
    assert!(selective[0].labels_are_complete());
}

#[test]
fn rate_increase_scalar_samples_borrow_no_stale_input() {
    let samples = [(1_000, 1.0), (2_000, 2.0), (3_000, 3.0)];
    let hints = [
        CounterResetHint::Unknown,
        CounterResetHint::NotCounterReset,
        CounterResetHint::NotCounterReset,
    ];

    let retained = rate_increase_scalar_samples(&samples, Some(&hints), false);

    assert_eq!(retained.samples.as_ptr(), samples.as_ptr());
    assert_eq!(
        retained.counter_reset_hints.as_deref().unwrap().as_ptr(),
        hints.as_ptr()
    );
}

#[test]
fn cumulative_delta_histogram_samples_omit_stale_and_mark_next_unknown() {
    let sample = |timestamp_ms: u64, count: f64, stale: bool| PromqlHistogramSample {
        timestamp_ms,
        start_time_ms: (!stale).then_some(timestamp_ms.saturating_sub(1_000)),
        count,
        sum: Some(count),
        explicit_bounds: Arc::from([1.0]),
        bucket_counts: vec![count, 0.0],
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        stale,
    };
    let samples = [
        sample(1_000, 1.0, false),
        sample(2_000, 0.0, true),
        sample(3_000, 1.0, false),
        sample(4_000, 2.0, false),
    ];

    let cumulative = cumulative_delta_histogram_samples(&samples).unwrap();

    assert_eq!(
        cumulative
            .iter()
            .map(|sample| (sample.timestamp_ms, sample.count, sample.reset_hint))
            .collect::<Vec<_>>(),
        vec![
            (1_000, 1.0, CounterResetHint::NotCounterReset),
            (3_000, 1.0, CounterResetHint::Unknown),
            (4_000, 3.0, CounterResetHint::NotCounterReset),
        ]
    );
    assert!(cumulative.iter().all(|sample| !sample.stale));
}

#[test]
fn cumulative_delta_exponential_histogram_samples_omit_stale_and_mark_next_unknown() {
    let sample = |timestamp_ms: u64, count: f64, stale: bool| PromqlExponentialHistogramSample {
        timestamp_ms,
        start_time_ms: (!stale).then_some(timestamp_ms.saturating_sub(1_000)),
        count,
        sum: Some(count),
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 0.0,
        positive: PromqlExponentialHistogramBuckets {
            offset: 0,
            counts: vec![count],
            sparse_counts: Vec::new(),
        },
        negative: PromqlExponentialHistogramBuckets::empty(),
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
        stale,
    };
    let samples = [
        sample(1_000, 1.0, false),
        sample(2_000, 0.0, true),
        sample(3_000, 1.0, false),
        sample(4_000, 2.0, false),
    ];

    let cumulative = cumulative_delta_exponential_histogram_samples(&samples).unwrap();

    assert_eq!(
        cumulative
            .iter()
            .map(|sample| (sample.timestamp_ms, sample.count, sample.reset_hint))
            .collect::<Vec<_>>(),
        vec![
            (1_000, 1.0, CounterResetHint::NotCounterReset),
            (3_000, 1.0, CounterResetHint::Unknown),
            (4_000, 3.0, CounterResetHint::NotCounterReset),
        ]
    );
    assert!(cumulative.iter().all(|sample| !sample.stale));
}

#[test]
fn exponential_bucket_downscale_matches_independent_reference() {
    let source_scale = 4;
    let offset = -9;
    let counts = (offset..=9)
        .map(|index| f64::from(index + 10))
        .collect::<Vec<_>>();
    let dense = PromqlExponentialHistogramBuckets {
        offset,
        counts,
        sparse_counts: Vec::new(),
    };
    let mut reversed_counts = dense
        .iter_counts()
        .map(|(index, count)| (i32::try_from(index).unwrap(), count))
        .collect::<Vec<_>>();
    reversed_counts.reverse();
    let sparse = PromqlExponentialHistogramBuckets::from_sparse_counts(reversed_counts);

    for target_scale in (-2..=source_scale).rev() {
        let expected = reference_downscaled_bucket_map(&dense, source_scale, target_scale);
        let dense_direct =
            downscale_promql_exponential_buckets_to_map(&dense, source_scale, target_scale);
        let sparse_direct =
            downscale_promql_exponential_buckets_to_map(&sparse, source_scale, target_scale);
        assert_eq!(
            dense_direct.as_ref().map(bucket_map_as_btree_map),
            expected,
            "dense target scale {target_scale}"
        );
        assert_eq!(
            sparse_direct.as_ref().map(bucket_map_as_btree_map),
            expected,
            "sparse target scale {target_scale}"
        );

        let source_map =
            downscale_promql_exponential_buckets_to_map(&dense, source_scale, source_scale)
                .unwrap();
        let via_map =
            downscale_promql_exponential_bucket_map_to_map(&source_map, source_scale, target_scale);
        assert_eq!(
            via_map.as_ref().map(bucket_map_as_btree_map),
            expected,
            "map target scale {target_scale}"
        );
    }
}

#[test]
fn exponential_bucket_downscale_handles_boundaries_and_rejects_invalid_scales() {
    let positive_boundary = PromqlExponentialHistogramBuckets {
        offset: i32::MAX,
        counts: vec![1.0, 2.0],
        sparse_counts: Vec::new(),
    };
    assert!(
        downscale_promql_exponential_buckets_to_map(&positive_boundary, 0, 0).is_none(),
        "the second dense source index does not fit in i32 at the original scale"
    );
    assert_eq!(
        bucket_map_as_btree_map(
            &downscale_promql_exponential_buckets_to_map(&positive_boundary, 0, -1).unwrap()
        ),
        BTreeMap::from([(1_073_741_823, 1.0), (1_073_741_824, 2.0)])
    );

    let negative_boundary = PromqlExponentialHistogramBuckets::from_sparse_counts(vec![
        (i32::MIN, 3.0),
        (i32::MIN + 1, 4.0),
    ]);
    assert_eq!(
        bucket_map_as_btree_map(
            &downscale_promql_exponential_buckets_to_map(&negative_boundary, 0, -1).unwrap()
        ),
        BTreeMap::from([(-1_073_741_824, 7.0)])
    );

    assert!(
        downscale_promql_exponential_buckets_to_map(&negative_boundary, 0, 1).is_none(),
        "downscaling cannot increase the target scale"
    );
    assert!(
        downscale_promql_exponential_buckets_to_map(&negative_boundary, 31, -32).is_none(),
        "a scale difference of 63 cannot be represented by a positive i64 divisor"
    );
    assert!(
        downscale_promql_exponential_buckets_to_map(&negative_boundary, 31, -33).is_none(),
        "a scale difference of 64 cannot be represented by an i64 divisor"
    );
    assert!(
        downscale_promql_exponential_buckets_to_map(&negative_boundary, i32::MAX, i32::MIN,)
            .is_none(),
        "scale subtraction overflow must be rejected"
    );
}

#[test]
fn exponential_bucket_counter_delta_preserves_reset_and_missing_bucket_semantics() {
    let previous = test_bucket_map([(-3, 5.0), (-1, 2.0), (2, 7.0)]);
    let current = test_bucket_map([(-3, 8.0), (0, 4.0), (2, 3.0)]);

    assert!(
        counter_bucket_map_delta(&previous, &current, CounterResetHint::NotCounterReset,).is_none(),
        "a decrease or disappeared bucket contradicts a no-reset hint"
    );
    assert_eq!(
        bucket_map_as_btree_map(
            &counter_bucket_map_delta(&previous, &current, CounterResetHint::Unknown).unwrap()
        ),
        BTreeMap::from([(-3, 3.0), (-1, 0.0), (0, 4.0), (2, 3.0)])
    );
    assert_eq!(
        bucket_map_as_btree_map(
            &counter_bucket_map_delta(&previous, &current, CounterResetHint::CounterReset).unwrap()
        ),
        BTreeMap::from([(-3, 8.0), (-1, 0.0), (0, 4.0), (2, 3.0)])
    );
    assert!(counter_bucket_map_delta(&previous, &current, CounterResetHint::GaugeType).is_none());

    let non_finite = test_bucket_map([(0, f64::INFINITY)]);
    assert!(counter_bucket_map_delta(&non_finite, &current, CounterResetHint::Unknown).is_none());
}

#[test]
fn exponential_bucket_addition_preserves_union_and_cancellation_semantics() {
    let mut accumulated = test_bucket_map([(-2, 1.0), (0, 2.0), (5, -3.0)]);
    let input = test_bucket_map([(-3, 4.0), (0, 0.5), (5, 3.0), (8, 9.0)]);

    add_promql_exponential_bucket_maps(&mut accumulated, input);

    assert_eq!(
        bucket_map_as_btree_map(&accumulated),
        BTreeMap::from([(-3, 4.0), (-2, 1.0), (0, 2.5), (5, 0.0), (8, 9.0)])
    );
    assert_eq!(
        promql_exponential_bucket_map_to_buckets(accumulated)
            .unwrap()
            .iter_counts()
            .collect::<Vec<_>>(),
        vec![(-3, 4.0), (-2, 1.0), (0, 2.5), (8, 9.0)]
    );
}

#[test]
fn exponential_bucket_map_to_buckets_preserves_sparse_span() {
    let map = test_bucket_map([(0, 1.0), (100_000, 2.0)]);

    let buckets = promql_exponential_bucket_map_to_buckets(map).unwrap();
    let observed = buckets.iter_counts().collect::<Vec<_>>();

    assert!(
        buckets.counts.len() <= 2,
        "sparse exponential bucket maps must not expand empty spans into {} buckets",
        buckets.counts.len()
    );
    assert_eq!(observed, vec![(0, 1.0), (100_000, 2.0)]);
}

fn test_bucket_map(entries: impl IntoIterator<Item = (i32, f64)>) -> PromqlExponentialBucketMap {
    PromqlExponentialBucketMap {
        entries: BTreeMap::from_iter(entries).into_iter().collect(),
    }
}

fn bucket_map_as_btree_map(map: &PromqlExponentialBucketMap) -> BTreeMap<i32, f64> {
    map.entries.iter().copied().collect()
}

fn reference_downscaled_bucket_map(
    buckets: &PromqlExponentialHistogramBuckets,
    source_scale: i32,
    target_scale: i32,
) -> Option<BTreeMap<i32, f64>> {
    if target_scale > source_scale {
        return None;
    }
    let shift = u32::try_from(source_scale.checked_sub(target_scale)?).ok()?;
    let divisor = 1i64.checked_shl(shift).filter(|divisor| *divisor > 0)?;
    let mut expected = BTreeMap::new();
    for (source_index, count) in buckets.iter_counts() {
        let target_index = i32::try_from(source_index.div_euclid(divisor)).ok()?;
        *expected.entry(target_index).or_insert(0.0) += count;
    }
    Some(expected)
}

#[test]
fn sparse_exponential_buckets_match_dense_quantile_and_fraction() {
    let high_index = 10_000i32;
    let high_idx = usize::try_from(high_index).unwrap();
    let mut dense_positive_counts = vec![0.0; high_idx + 1];
    dense_positive_counts[0] = 2.0;
    dense_positive_counts[high_idx] = 1.0;
    let mut dense_negative_counts = vec![0.0; high_idx + 1];
    dense_negative_counts[0] = 1.0;
    dense_negative_counts[high_idx] = 2.0;

    let dense = exponential_histogram_sample_for_test(
        PromqlExponentialHistogramBuckets {
            offset: 0,
            counts: dense_positive_counts,
            sparse_counts: Vec::new(),
        },
        PromqlExponentialHistogramBuckets {
            offset: 0,
            counts: dense_negative_counts,
            sparse_counts: Vec::new(),
        },
    );
    let sparse = exponential_histogram_sample_for_test(
        PromqlExponentialHistogramBuckets::from_sparse_counts(vec![(0, 2.0), (high_index, 1.0)]),
        PromqlExponentialHistogramBuckets::from_sparse_counts(vec![(0, 1.0), (high_index, 2.0)]),
    );

    for quantile in [0.1, 0.5, 0.9] {
        assert_f64_close(
            exponential_histogram_quantile(quantile, &sparse).unwrap(),
            exponential_histogram_quantile(quantile, &dense).unwrap(),
        );
    }
    for (lower, upper) in [
        (f64::NEG_INFINITY, f64::INFINITY),
        (-10.0, 10.0),
        (-0.01, 0.01),
        (0.5, 2.0),
    ] {
        assert_f64_close(
            exponential_histogram_fraction(lower, upper, &sparse).unwrap(),
            exponential_histogram_fraction(lower, upper, &dense).unwrap(),
        );
    }
}

fn exponential_histogram_sample_for_test(
    positive: PromqlExponentialHistogramBuckets,
    negative: PromqlExponentialHistogramBuckets,
) -> PromqlExponentialHistogramSample {
    PromqlExponentialHistogramSample {
        timestamp_ms: 10_000,
        start_time_ms: None,
        count: 7.0,
        sum: None,
        scale: 8,
        zero_threshold: 0.001,
        zero_count: 1.0,
        positive,
        negative,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::Unknown,
        stale: false,
    }
}

fn assert_f64_close(actual: f64, expected: f64) {
    if actual.is_nan() && expected.is_nan() {
        return;
    }
    if actual.is_infinite() || expected.is_infinite() {
        assert_eq!(actual, expected);
        return;
    }
    let scale = actual.abs().max(expected.abs()).max(1.0);
    assert!(
        (actual - expected).abs() <= scale * 1e-12,
        "actual {actual} differs from expected {expected}"
    );
}
