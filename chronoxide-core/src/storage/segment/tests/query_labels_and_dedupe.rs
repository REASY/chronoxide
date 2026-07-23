use super::*;

#[test]
fn segment_query_result_can_share_labels_without_deep_clone() {
    let labels = shared_query_labels(vec![
        (METRIC_NAME_LABEL.to_string(), "shared_metric".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ]);

    let result = SegmentQueryResult::with_shared_labels(42, labels.clone());

    assert!(labels.ptr_eq(&result.labels));
    assert_eq!(result.labels, labels);
    assert!(result.samples.is_empty());
}

#[test]
fn last_over_time_preserves_shared_labels_through_the_public_boundary() {
    let label_values = vec![
        (METRIC_NAME_LABEL.to_string(), "shared_metric".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ];
    let series_id = segment_series_id(&label_values);
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
    let labels = interner.intern_labels(label_values);
    let before = interner.stats();
    let input = SegmentQueryResult::with_shared_samples(
        series_id,
        labels.clone(),
        vec![(1_000, 1.0), (2_000, 2.0)],
    );
    let function = PromqlRangeFunction {
        kind: PromqlRangeFunctionKind::LastOverTime,
        selector: PromqlSelector {
            metric_name: Some(String::from("shared_metric")),
            matchers: Vec::new(),
        },
        range_ms: 5_000,
    };

    let mut output = evaluate_range_function(&function, vec![input], 2_000);

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].series_id, series_id);
    assert_eq!(output[0].samples, [(2_000, 2.0)]);
    assert!(labels.ptr_eq(&output[0].labels));
    assert!(!output[0].labels.owned_compatibility_materialized());

    interner.intern_result_labels(&mut output).unwrap();

    assert_eq!(interner.stats(), before);
    assert!(labels.ptr_eq(&output[0].labels));
    assert!(!output[0].labels.owned_compatibility_materialized());
}

#[test]
fn public_query_boundary_rejects_incomplete_labels_in_release_builds() {
    let mut result =
        SegmentQueryResult::new(42, vec![(String::from("service"), String::from("api"))]);
    result.mark_labels_incomplete(None);

    let error = ensure_query_result_labels_complete(&[result])
        .expect_err("partial terminal-aggregation input must not escape");
    assert!(matches!(error, PromqlQueryError::Storage(_)));
}

#[test]
fn partial_merge_preserves_established_first_identity_on_source_id_collision() {
    let mut first = SegmentQueryResult::with_samples(
        42,
        vec![(String::from("service"), String::from("first"))],
        vec![(1, 1.0)],
    );
    first.mark_labels_incomplete(Some(11));
    let mut colliding = SegmentQueryResult::with_samples(
        42,
        vec![(String::from("service"), String::from("second"))],
        vec![(2, 2.0)],
    );
    colliding.mark_labels_incomplete(Some(22));

    first.extend_from(colliding);

    assert_eq!(first.metric_name_dropped_series_id, Some(11));
    assert_eq!(
        first.labels.to_vec().as_slice(),
        &[(String::from("service"), String::from("first"))]
    );
    assert_eq!(first.samples, [(1, 1.0), (2, 2.0)]);
}

#[test]
fn selective_rate_uses_full_path_metric_name_dropped_identity_and_sum_order() {
    #[derive(Clone)]
    struct Candidate {
        labels: Vec<(String, String)>,
        source_id: u64,
        metric_name_dropped_id: u64,
    }

    let candidates = (0..128)
        .map(|index| {
            let labels = vec![
                (METRIC_NAME_LABEL.to_string(), String::from("metric")),
                (String::from("instance"), format!("instance-{index}")),
                (String::from("service"), String::from("api")),
            ];
            let source_id = segment_series_id(&labels);
            let metric_name_dropped_id = segment_series_id(&labels[1..]);
            Candidate {
                labels,
                source_id,
                metric_name_dropped_id,
            }
        })
        .collect::<Vec<_>>();

    // Find an inversion where the large term is last in established
    // post-rate identity order, but not in source-identity order. This makes
    // the floating-point low-bit regression deterministic without coupling
    // the test to hard-coded xxHash outputs.
    let mut selected = None;
    'large: for large in 0..candidates.len() {
        let preceding = (0..candidates.len())
            .filter(|index| {
                *index != large
                    && candidates[*index].metric_name_dropped_id
                        < candidates[large].metric_name_dropped_id
            })
            .collect::<Vec<_>>();
        for &first in &preceding {
            if candidates[first].source_id <= candidates[large].source_id {
                continue;
            }
            if let Some(second) = preceding.iter().copied().find(|index| *index != first) {
                selected = Some([large, first, second]);
                break 'large;
            }
        }
    }
    let [large, first, second] = selected.expect("find a deterministic identity-order inversion");
    let selected = [
        (&candidates[large], 1.0e16),
        (&candidates[first], 1.0),
        (&candidates[second], 1.0),
    ];

    let range = PromqlRangeFunction {
        kind: PromqlRangeFunctionKind::Rate,
        selector: PromqlSelector {
            metric_name: Some(String::from("metric")),
            matchers: Vec::new(),
        },
        range_ms: 1_001,
    };
    let aggregation = PromqlAggregation {
        op: PromqlAggregationOp::Sum,
        grouping: PromqlAggregationGrouping::By(vec![String::from("service")]),
        input: Box::new(PromqlQuery::Scalar(0.0)),
    };

    let evaluate = |partial: bool, use_source_id_after_rate: bool| {
        let inputs = selected
            .iter()
            .map(|(candidate, increase)| {
                let labels = if partial {
                    vec![(String::from("service"), String::from("api"))]
                } else {
                    candidate.labels.clone()
                };
                let mut result = SegmentQueryResult::with_samples(
                    candidate.source_id,
                    labels,
                    vec![(1, 0.0), (1_001, *increase)],
                );
                if partial {
                    result.mark_labels_incomplete(Some(if use_source_id_after_rate {
                        candidate.source_id
                    } else {
                        candidate.metric_name_dropped_id
                    }));
                }
                result
            })
            .collect();
        let ranged = evaluate_range_function(&range, merge_query_results(inputs), 1_001);
        evaluate_aggregation(&aggregation, ranged, 1_001)
    };

    let full = evaluate(false, false);
    let selective = evaluate(true, false);
    let old_source_order = evaluate(true, true);
    assert_eq!(full.len(), 1);
    assert_eq!(selective.len(), 1);
    assert_eq!(full[0].labels, selective[0].labels);
    assert_eq!(
        full[0].samples[0].1.to_bits(),
        selective[0].samples[0].1.to_bits(),
        "selective rate must preserve the established exact aggregation order"
    );
    assert_ne!(
        full[0].samples[0].1.to_bits(),
        old_source_order[0].samples[0].1.to_bits(),
        "the fixture must exercise the prior source-order low-bit bug"
    );
}

#[test]
fn scalar_rate_uses_prometheus_factor_order_without_changing_delta_order() {
    let function = PromqlRangeFunction {
        kind: PromqlRangeFunctionKind::Rate,
        selector: PromqlSelector {
            metric_name: Some("operation_order".to_owned()),
            matchers: Vec::new(),
        },
        range_ms: 1_001,
    };
    let input = |temporality| {
        let mut result = SegmentQueryResult::with_samples(
            42,
            vec![(METRIC_NAME_LABEL.to_owned(), "operation_order".to_owned())],
            vec![(1, 3.0), (3, 6.0)],
        );
        result.temporality = temporality;
        result.counter_reset_hints = vec![
            CounterResetHint::NotCounterReset,
            CounterResetHint::NotCounterReset,
        ];
        if temporality == QueryResultTemporality::Delta {
            result.sample_start_times = vec![Some(0), Some(2)];
        }
        result
    };

    let cumulative = evaluate_range_function(
        &function,
        vec![input(QueryResultTemporality::Cumulative)],
        1_001,
    );
    let delta =
        evaluate_range_function(&function, vec![input(QueryResultTemporality::Delta)], 1_001);

    assert_eq!(cumulative[0].samples[0].1.to_bits(), 0x4017_f9dc_b511_2288);
    assert_eq!(delta[0].samples[0].1.to_bits(), 0x4017_f9dc_b511_2287);
}

#[test]
fn terminal_aggregation_label_demand_is_sorted_and_deduplicated() {
    let grouping = vec![
        String::from("地域"),
        String::from("service"),
        String::from("service"),
    ];
    let selector = SegmentSelector::with_metric(
        "requests_total",
        vec![
            LabelMatcher::not_eq("zone", ""),
            LabelMatcher::regex("service", ".*"),
        ],
    )
    .with_terminal_aggregation_label_demand(&grouping, false);

    assert_eq!(
        selector.label_demand().included_names().unwrap(),
        [METRIC_NAME_LABEL, "service", "zone", "地域"]
    );
    assert_eq!(
        selector.label_demand().output_names_arc().unwrap().as_ref(),
        ["service", "地域"].as_slice()
    );
    assert!(
        !selector
            .label_demand()
            .derives_metric_name_dropped_identity()
    );

    let range_selector = SegmentSelector::metric("requests_total")
        .with_terminal_aggregation_label_demand(&grouping, true);
    assert!(
        range_selector
            .label_demand()
            .derives_metric_name_dropped_identity()
    );
}

#[test]
fn terminal_aggregation_label_demand_rejects_label_exposing_shapes() {
    let aggregation = |op, grouping| PromqlAggregation {
        op,
        grouping,
        input: Box::new(PromqlQuery::Scalar(1.0)),
    };
    let by = vec![String::from("service")];

    assert_eq!(
        terminal_aggregation_grouping_names(&aggregation(
            PromqlAggregationOp::Sum,
            PromqlAggregationGrouping::By(by.clone()),
        )),
        Some(by.as_slice())
    );
    assert_eq!(
        terminal_aggregation_grouping_names(&aggregation(
            PromqlAggregationOp::Count,
            PromqlAggregationGrouping::All,
        )),
        Some(&[][..])
    );
    assert!(
        terminal_aggregation_grouping_names(&aggregation(
            PromqlAggregationOp::Sum,
            PromqlAggregationGrouping::Without(vec![String::from("instance")]),
        ))
        .is_none()
    );
    assert!(
        terminal_aggregation_grouping_names(&aggregation(
            PromqlAggregationOp::TopK(1),
            PromqlAggregationGrouping::By(by.clone()),
        ))
        .is_none()
    );
    assert!(
        terminal_aggregation_grouping_names(&aggregation(
            PromqlAggregationOp::CountValues(String::from("value")),
            PromqlAggregationGrouping::By(by),
        ))
        .is_none()
    );
}

#[test]
fn native_terminal_aggregation_label_demand_is_narrow_and_explicit() {
    let selector = || {
        PromqlQuery::Vector(PromqlSelector {
            metric_name: Some(String::from("native_histogram")),
            matchers: Vec::new(),
        })
    };
    let aggregation = |op, grouping, input| PromqlAggregation {
        op,
        grouping,
        input: Box::new(input),
    };
    let by = vec![String::from("service")];

    let direct = aggregation(
        PromqlAggregationOp::Count,
        PromqlAggregationGrouping::By(by.clone()),
        selector(),
    );
    assert_eq!(
        native_terminal_aggregation_label_demand(&direct),
        Some((by.as_slice(), false))
    );

    let range = aggregation(
        PromqlAggregationOp::Group,
        PromqlAggregationGrouping::All,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Rate,
            selector: PromqlSelector {
                metric_name: Some(String::from("native_histogram")),
                matchers: Vec::new(),
            },
            range_ms: 60_000,
        }),
    );
    assert_eq!(
        native_terminal_aggregation_label_demand(&range),
        Some((&[][..], true))
    );

    for unsupported in [
        aggregation(
            PromqlAggregationOp::Count,
            PromqlAggregationGrouping::Without(vec![String::from("instance")]),
            selector(),
        ),
        aggregation(
            PromqlAggregationOp::Sum,
            PromqlAggregationGrouping::By(by.clone()),
            selector(),
        ),
        aggregation(
            PromqlAggregationOp::Count,
            PromqlAggregationGrouping::By(by.clone()),
            PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Changes,
                selector: PromqlSelector {
                    metric_name: Some(String::from("native_histogram")),
                    matchers: Vec::new(),
                },
                range_ms: 60_000,
            }),
        ),
        aggregation(
            PromqlAggregationOp::Count,
            PromqlAggregationGrouping::By(by),
            PromqlQuery::Aggregation(PromqlAggregation {
                op: PromqlAggregationOp::Sum,
                grouping: PromqlAggregationGrouping::All,
                input: Box::new(selector()),
            }),
        ),
    ] {
        assert_eq!(native_terminal_aggregation_label_demand(&unsupported), None);
    }
}

#[test]
fn query_label_materialization_defaults_to_demand_driven() {
    assert_eq!(
        QueryLabelMaterializationPolicy::default(),
        QueryLabelMaterializationPolicy::DemandDriven
    );
}

#[test]
fn dedupe_sorted_unique_samples_keeps_existing_storage() {
    let mut result =
        SegmentQueryResult::with_samples(42, Vec::new(), vec![(10, 1.0), (20, 2.0), (30, 3.0)]);
    let samples_ptr = result.samples.as_ptr();
    let samples_capacity = result.samples.capacity();

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 1.0), (20, 2.0), (30, 3.0)]);
    assert_eq!(result.samples.as_ptr(), samples_ptr);
    assert_eq!(result.samples.capacity(), samples_capacity);
    assert!(result.counter_reset_hints.is_empty());
}

#[test]
fn dedupe_empty_samples_clears_invalid_hints() {
    let mut result = SegmentQueryResult::with_samples(42, Vec::new(), Vec::new());
    result.counter_reset_hints = vec![CounterResetHint::CounterReset];

    result.dedupe_samples_keep_last();

    assert!(result.samples.is_empty());
    assert!(result.counter_reset_hints.is_empty());
}

#[test]
fn dedupe_single_sample_preserves_valid_hint() {
    let mut result = SegmentQueryResult::with_samples(42, Vec::new(), vec![(10, 1.0)]);
    result.counter_reset_hints = vec![CounterResetHint::GaugeType];

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 1.0)]);
    assert_eq!(
        result.counter_reset_hints,
        vec![CounterResetHint::GaugeType]
    );
}

#[test]
fn dedupe_mismatched_hints_are_discarded() {
    let mut result =
        SegmentQueryResult::with_samples(42, Vec::new(), vec![(10, 1.0), (10, 2.0), (20, 3.0)]);
    result.counter_reset_hints = vec![CounterResetHint::CounterReset];

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 2.0), (20, 3.0)]);
    assert!(result.counter_reset_hints.is_empty());
}

#[test]
fn dedupe_sorted_duplicate_samples_keeps_last_with_hints() {
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![(10, 1.0), (10, 2.0), (20, 3.0), (20, 4.0), (30, 5.0)],
    );
    result.counter_reset_hints = vec![
        CounterResetHint::Unknown,
        CounterResetHint::CounterReset,
        CounterResetHint::NotCounterReset,
        CounterResetHint::GaugeType,
        CounterResetHint::Unknown,
    ];

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 2.0), (20, 4.0), (30, 5.0)]);
    assert_eq!(
        result.counter_reset_hints,
        vec![
            CounterResetHint::CounterReset,
            CounterResetHint::GaugeType,
            CounterResetHint::Unknown,
        ]
    );
}

#[test]
fn dedupe_sorted_duplicate_samples_without_hints_keeps_last_value_bits() {
    let first_bits = 0x7ff8_0000_0000_0001;
    let last_bits = 0x7ff8_0000_0000_0002;
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![
            (10, f64::from_bits(first_bits)),
            (10, f64::from_bits(last_bits)),
            (20, 3.0),
        ],
    );

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples.len(), 2);
    assert_eq!(result.samples[0].0, 10);
    assert_eq!(result.samples[0].1.to_bits(), last_bits);
    assert_eq!(result.samples[1], (20, 3.0));
    assert!(result.counter_reset_hints.is_empty());
}

#[test]
fn dedupe_unsorted_samples_sorts_and_keeps_last_with_hints() {
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![(20, 2.0), (10, 1.0), (20, 3.0), (10, 4.0)],
    );
    result.counter_reset_hints = vec![
        CounterResetHint::Unknown,
        CounterResetHint::CounterReset,
        CounterResetHint::NotCounterReset,
        CounterResetHint::GaugeType,
    ];

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 4.0), (20, 3.0)]);
    assert_eq!(
        result.counter_reset_hints,
        vec![
            CounterResetHint::GaugeType,
            CounterResetHint::NotCounterReset
        ]
    );
}

#[test]
fn dedupe_unsorted_samples_keeps_start_times_aligned() {
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![(20, 2.0), (10, 1.0), (20, 3.0), (10, 4.0)],
    );
    result.counter_reset_hints = vec![
        CounterResetHint::Unknown,
        CounterResetHint::CounterReset,
        CounterResetHint::NotCounterReset,
        CounterResetHint::GaugeType,
    ];
    result.sample_start_times = vec![Some(19), Some(9), Some(18), Some(8)];

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 4.0), (20, 3.0)]);
    assert_eq!(
        result.counter_reset_hints,
        vec![
            CounterResetHint::GaugeType,
            CounterResetHint::NotCounterReset
        ]
    );
    assert_eq!(result.sample_start_times, vec![Some(8), Some(18)]);
}

#[test]
fn dedupe_shadowed_typed_samples_keeps_complete_winner_metadata() {
    let stale = prometheus_stale_nan();
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![
            (10, 100.0),
            (20, 200.0),
            (30, 300.0),
            (10, 2.0),
            (20, 4.0),
            (30, stale),
        ],
    );
    result.counter_reset_hints = vec![
        CounterResetHint::GaugeType,
        CounterResetHint::GaugeType,
        CounterResetHint::GaugeType,
        CounterResetHint::CounterReset,
        CounterResetHint::NotCounterReset,
        CounterResetHint::Unknown,
    ];
    result.sample_start_times = vec![None, None, None, Some(0), Some(10), None];
    result.sample_temporalities = vec![
        QueryResultTemporality::Cumulative,
        QueryResultTemporality::Cumulative,
        QueryResultTemporality::Cumulative,
        QueryResultTemporality::Delta,
        QueryResultTemporality::Delta,
        QueryResultTemporality::Delta,
    ];
    result.temporality = QueryResultTemporality::Mixed;
    result.delta_projection_intervals = vec![
        None,
        None,
        None,
        Some(DeltaProjectionInterval::Count {
            raw: 2,
            reset_hint: CounterResetHint::CounterReset,
        }),
        Some(DeltaProjectionInterval::Count {
            raw: 4,
            reset_hint: CounterResetHint::NotCounterReset,
        }),
        None,
    ];

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples[..2], [(10, 2.0), (20, 6.0)]);
    assert_eq!(result.samples[2].0, 30);
    assert_eq!(result.samples[2].1.to_bits(), stale.to_bits());
    assert_eq!(
        result.counter_reset_hints,
        vec![
            CounterResetHint::CounterReset,
            CounterResetHint::NotCounterReset,
            CounterResetHint::Unknown,
        ]
    );
    assert_eq!(result.sample_start_times, vec![Some(0), Some(10), None]);
    assert_eq!(
        result.sample_temporalities,
        vec![QueryResultTemporality::Delta; 3]
    );
    assert_eq!(result.temporality, QueryResultTemporality::Delta);
    assert_eq!(
        result.delta_projection_intervals,
        vec![
            Some(DeltaProjectionInterval::Count {
                raw: 2,
                reset_hint: CounterResetHint::CounterReset,
            }),
            Some(DeltaProjectionInterval::Count {
                raw: 4,
                reset_hint: CounterResetHint::NotCounterReset,
            }),
            None,
        ]
    );
}

#[test]
fn dedupe_partially_shadowed_typed_samples_keeps_surviving_mixed_temporality() {
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![(10, 10.0), (20, 20.0), (20, 2.0), (30, 3.0)],
    );
    result.sample_temporalities = vec![
        QueryResultTemporality::Cumulative,
        QueryResultTemporality::Cumulative,
        QueryResultTemporality::Delta,
        QueryResultTemporality::Delta,
    ];
    result.temporality = QueryResultTemporality::Mixed;

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 10.0), (20, 2.0), (30, 3.0)]);
    assert_eq!(
        result.sample_temporalities,
        vec![
            QueryResultTemporality::Cumulative,
            QueryResultTemporality::Delta,
            QueryResultTemporality::Delta,
        ]
    );
    assert_eq!(result.temporality, QueryResultTemporality::Mixed);
}

#[test]
fn dedupe_partially_shadowed_typed_samples_keeps_surviving_unknown_temporality() {
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![(10, 10.0), (20, 20.0), (20, 2.0), (30, 3.0)],
    );
    result.sample_temporalities = vec![
        QueryResultTemporality::Cumulative,
        QueryResultTemporality::Cumulative,
        QueryResultTemporality::Unknown,
        QueryResultTemporality::Unknown,
    ];
    result.temporality = QueryResultTemporality::Unknown;

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 10.0), (20, 2.0), (30, 3.0)]);
    assert_eq!(
        result.sample_temporalities,
        vec![
            QueryResultTemporality::Cumulative,
            QueryResultTemporality::Unknown,
            QueryResultTemporality::Unknown,
        ]
    );
    assert_eq!(result.temporality, QueryResultTemporality::Unknown);
}

#[test]
fn dedupe_unsorted_samples_without_hints_sorts_and_keeps_last() {
    let mut result = SegmentQueryResult::with_samples(
        42,
        Vec::new(),
        vec![(20, 2.0), (10, 1.0), (20, 3.0), (10, 4.0)],
    );

    result.dedupe_samples_keep_last();

    assert_eq!(result.samples, vec![(10, 4.0), (20, 3.0)]);
    assert!(result.counter_reset_hints.is_empty());
}

#[test]
fn query_budget_counts_unique_matched_series_once() {
    let mut budget = QueryBudget::new(QueryLimits {
        max_matched_series: Some(1),
        ..QueryLimits::unlimited()
    });

    budget.observe_matched_series(10).unwrap();
    budget.observe_matched_series(10).unwrap();
    assert_eq!(budget.stats().matched_series, 1);

    let err = budget.observe_matched_series(11).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::QuotaExceeded);
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::MatchedSeries);
    assert_eq!(limit.max, 1);
}

#[test]
fn query_budget_counts_unique_projected_series_once() {
    let mut budget = QueryBudget::new(QueryLimits {
        max_projected_series: Some(1),
        ..QueryLimits::unlimited()
    });

    budget.observe_projected_series(10).unwrap();
    budget.observe_projected_series(10).unwrap();
    assert_eq!(budget.stats().projected_series, 1);

    let err = budget.observe_projected_series(11).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::QuotaExceeded);
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::ProjectedSeries);
    assert_eq!(limit.max, 1);
}

#[test]
fn query_limits_production_default_matches_storage_spec_guardrails() {
    let limits = QueryLimits::production_default();

    assert_eq!(limits.max_matched_series, Some(1_000_000));
    assert_eq!(limits.max_projected_series, Some(2_000_000));
    assert_eq!(limits.max_chunk_reads, Some(5_000_000));
    assert_eq!(limits.max_bytes_read, Some(2 * 1024 * 1024 * 1024));
    assert_eq!(limits.max_samples_decoded, Some(50_000_000));
    assert_eq!(limits.max_regex_values_examined, Some(100_000));
}

#[test]
fn query_budget_rejects_chunk_byte_sample_and_regex_limits() {
    let mut budget = QueryBudget::new(QueryLimits {
        max_chunk_reads: Some(0),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_chunk_read(1).unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::ChunkReads);
    assert_eq!(limit.max, 0);

    let mut budget = QueryBudget::new(QueryLimits {
        max_bytes_read: Some(4),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_chunk_read(5).unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::BytesRead);
    assert_eq!(limit.max, 4);

    let mut budget = QueryBudget::new(QueryLimits {
        max_samples_decoded: Some(1),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_samples_decoded(2).unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::SamplesDecoded);
    assert_eq!(limit.max, 1);

    let mut budget = QueryBudget::new(QueryLimits {
        max_regex_values_examined: Some(0),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_regex_value().unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::RegexValuesExamined);
    assert_eq!(limit.max, 0);
}
