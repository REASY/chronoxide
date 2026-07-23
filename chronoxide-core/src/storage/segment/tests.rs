use super::*;
use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkReader, ChunkSamples};
use crate::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue, SummaryQuantileValue,
    SummaryValue, TypedSampleMetadata,
};
use crate::storage::index::LabelValueTimeRange;
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_HISTOGRAM, SERIES_KIND_SUMMARY,
};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};

const FRAME_HEADER_LEN: u64 = 14;

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

#[test]
fn query_profile_reports_chunk_payload_locality() {
    let mut profile = SegmentStoreQueryProfile::default();

    profile.observe_chunk_payload_file_reads(&[(100, 10), (110, 5), (200, 10), (260, 5), (240, 5)]);

    let locality = profile.chunk_payload_locality;
    assert_eq!(locality.reads, 5);
    assert_eq!(locality.backward_jumps, 1);
    assert_eq!(locality.forward_gaps, 2);
    assert_eq!(locality.forward_gap_bytes, 135);
    assert_eq!(locality.contiguous_runs, 4);
    assert_eq!(locality.contiguous_span_bytes, 35);
    assert_eq!(locality.coalesced_4k_runs, 2);
    assert_eq!(locality.coalesced_4k_span_bytes, 170);
    assert_eq!(locality.coalesced_64k_runs, 2);
    assert_eq!(locality.coalesced_64k_span_bytes, 170);
}

#[test]
fn query_profile_reports_sorted_chunk_payload_coalescing_potential() {
    let mut profile = SegmentStoreQueryProfile::default();
    let mut ranges = [(200, 10), (100, 10), (110, 5), (260, 5), (240, 5)];

    profile.observe_sorted_chunk_payload_ranges(&mut ranges);

    let locality = profile.chunk_payload_locality;
    assert_eq!(locality.reads, 0);
    assert_eq!(locality.sorted_contiguous_runs, 4);
    assert_eq!(locality.sorted_contiguous_span_bytes, 35);
    assert_eq!(locality.sorted_coalesced_4k_runs, 1);
    assert_eq!(locality.sorted_coalesced_4k_span_bytes, 165);
    assert_eq!(locality.sorted_coalesced_64k_runs, 1);
    assert_eq!(locality.sorted_coalesced_64k_span_bytes, 165);
}

#[test]
fn metadata_runtime_reports_schema7_reads_and_reuses_governed_roots() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 42.0)], |visit| {
            visit(METRIC_NAME_LABEL, "profile.metric");
            visit("pod", "backend-1");
        })
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let query = normalize_metric_name("profile.metric");

    let before_first = store.metadata_runtime_snapshot();
    let mut first_session = store.query_session().unwrap();
    assert_eq!(
        first_session.query_promql(&query, 0, 2_000).unwrap().len(),
        1
    );
    let after_first = store.metadata_runtime_snapshot();
    let first_reads = after_first.reads.delta_since(before_first.reads);
    assert!(first_reads.issued.calls > 0);
    assert!(first_reads.issued.bytes > 0);
    assert!(after_first.cache.resident_entries > 0);
    assert!(after_first.governor.retained_bytes > 0);
    assert_eq!(after_first.governor.in_flight_bytes, 0);
    drop(first_session);

    let before_second = store.metadata_runtime_snapshot();
    let mut second_session = store.query_session().unwrap();
    assert_eq!(
        second_session.query_promql(&query, 0, 2_000).unwrap().len(),
        1
    );
    let after_second = store.metadata_runtime_snapshot();
    assert!(after_second.cache.hits > before_second.cache.hits);
    assert_eq!(after_second.governor.in_flight_bytes, 0);
    assert!(after_second.governor.retained_bytes > 0);
}

#[test]
fn regex_literal_prefix_extracts_only_safe_prefixes() {
    assert_eq!(
        regex_literal_prefix("go_gc_duration_seconds.*"),
        Some("go_gc_duration_seconds".to_string())
    );
    assert_eq!(
        regex_literal_prefix("^rpc_duration.*_count"),
        Some("rpc_duration".to_string())
    );
    assert_eq!(
        regex_literal_prefix(r"http\.request\..*"),
        Some("http.request.".to_string())
    );
    assert_eq!(regex_literal_prefix(".*_count"), None);
    assert_eq!(regex_literal_prefix("[a-z].*"), None);
    assert_eq!(regex_literal_prefix(r"\d+"), None);
    assert_eq!(regex_literal_prefix("a|c"), None);
    assert_eq!(regex_literal_prefix("foo.*|bar.*"), None);
    assert_eq!(
        regex_literal_prefixes("rpc_duration_count", true),
        vec!["rpc_duration".to_string(), "rpc_duration_count".to_string()]
    );
    assert_eq!(
        regex_literal_prefixes("rpc_duration_count", false),
        vec!["rpc_duration_count".to_string()]
    );
}

#[test]
fn regex_symbol_lookup_batches_preserve_results_with_exact_postings_fallback() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();
    let series_count = REGEX_SYMBOL_LOOKUP_BATCH_VALUES + 1;
    let long_suffix = "x".repeat(120);
    for index in 0..series_count {
        let value = format!("batch-value-{index:04}-{long_suffix}");
        let timestamp_ms = if index % 2 == 0 { 1_000 } else { 9_000 };
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(index as u32 + 1),
                &[(timestamp_ms, index as f64)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "regex_batch_metric");
                    visit("batch_value", &value);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut session = store.query_session().unwrap();
    let expected = session
        .query_promql("regex_batch_metric", 0, 2_000)
        .unwrap();
    let actual = session
        .query_promql_with_limits(
            r#"regex_batch_metric{batch_value=~"batch-value-.*"}"#,
            0,
            2_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(actual.results, expected);
    assert_eq!(actual.results.len(), series_count.div_ceil(2));
    // Schema 7 reads one integrity-checked exact/routing pair for each result.
    assert_eq!(
        actual.stats.index_postings_reads,
        u64::try_from(actual.results.len()).unwrap() * 2
    );
    assert_eq!(
        actual.stats.regex_values_examined,
        u64::try_from(series_count).unwrap()
    );
}

#[test]
fn metadata_accumulator_sorts_dedupes_and_tracks_metric_names() {
    let mut metadata = MetadataAccumulator::default();
    metadata.add_labelset(&[
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod_name".to_string(), "backend-2".to_string()),
    ]);
    metadata.add_labelset(&[
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod_name".to_string(), "backend-1".to_string()),
        ("namespace".to_string(), "default".to_string()),
    ]);

    assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
    assert_eq!(
        metadata.label_names(),
        vec![
            METRIC_NAME_LABEL.to_string(),
            "namespace".to_string(),
            "pod_name".to_string()
        ]
    );
    assert_eq!(
        metadata.label_values("pod_name"),
        vec!["backend-1".to_string(), "backend-2".to_string()]
    );
}

fn paged_symbol_reader(
    symbols: &SegmentSymbols,
) -> crate::storage::symbols::SegmentSymbolReader<Cursor<Vec<u8>>> {
    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, symbols).unwrap();
    crate::storage::symbols::SegmentSymbolReader::open(Cursor::new(bytes)).unwrap()
}

#[test]
fn batched_series_label_resolution_reads_each_required_page_once() {
    let mut symbols = SegmentSymbols::default();
    let symbol_ids = ['a', 'b', 'c', 'd', 'e', 'f']
        .into_iter()
        .map(|prefix| symbols.intern(&format!("{prefix}{}", "x".repeat(12_000))))
        .collect::<Vec<_>>();
    let entries = (0..4)
        .map(|series_id| SeriesEntry {
            series_id,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![
                (symbol_ids[0], symbol_ids[2]),
                (symbol_ids[4], symbol_ids[5]),
            ],
        })
        .collect::<Vec<_>>();

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let scalar_reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes.clone()),
        0,
    )
    .unwrap();
    for entry in &entries {
        SegmentReader::resolve_series_labels(&scalar_reader, entry).unwrap();
    }

    let batch_reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let entry_refs = entries.iter().collect::<Vec<_>>();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&batch_reader, &entry_refs, &mut label_cache)
        .unwrap();

    assert_eq!(label_cache.len(), entries.len());
    for entry in &entries {
        assert_eq!(
            label_cache.get(&entry.series_id).unwrap().to_vec(),
            resolved_entry_labels(&symbols, entry)
        );
    }
    assert_eq!(batch_reader.read_stats().page.calls, 3);
    assert_eq!(scalar_reader.read_stats().page.calls, 16);
}

#[test]
fn batched_series_label_resolution_is_bounded_across_batch_limit() {
    let mut symbols = SegmentSymbols::default();
    let symbol_ids = ['a', 'b', 'c', 'd', 'e', 'f']
        .into_iter()
        .map(|prefix| symbols.intern(&format!("{prefix}{}", "x".repeat(12_000))))
        .collect::<Vec<_>>();
    let entry_count = super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES + 1;
    let entries = (0..entry_count)
        .map(|series_id| SeriesEntry {
            series_id: u64::try_from(series_id).unwrap(),
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(symbol_ids[0], symbol_ids[4])],
        })
        .collect::<Vec<_>>();

    assert_eq!(
        entries[..super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES]
            .iter()
            .map(|entry| entry.labels.len() * 2)
            .sum::<usize>(),
        super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES * 2
    );
    const {
        assert!(
            super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES * 2
                < super::query_reader::SERIES_LABEL_BATCH_MAX_SYMBOL_REFERENCES
        );
    }

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let entry_refs = entries.iter().collect::<Vec<_>>();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&reader, &entry_refs, &mut label_cache).unwrap();

    assert_eq!(label_cache.len(), entry_count);
    for entry in &entries {
        assert_eq!(
            label_cache.get(&entry.series_id).unwrap().to_vec(),
            resolved_entry_labels(&symbols, entry)
        );
    }
    // The two referenced IDs occupy two pages. A zero-byte cache makes the
    // entry-count boundary observable independently of the reference-count
    // boundary: each bounded batch reads those pages once.
    let stats = reader.read_stats();
    assert_eq!(stats.page.calls, 4);
    assert_eq!(
        stats.logical_returned.calls,
        u64::try_from(entry_count * 2).unwrap()
    );
}

#[test]
fn batched_series_label_resolution_splits_one_oversized_series() {
    let mut symbols = SegmentSymbols::default();
    let symbol_count = super::query_reader::SERIES_LABEL_BATCH_MAX_SYMBOL_REFERENCES + 4;
    let symbol_ids = (0..symbol_count)
        .map(|index| symbols.intern(&format!("symbol-{index:05}")))
        .collect::<Vec<_>>();
    let entry = SeriesEntry {
        series_id: 7,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: symbol_ids
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect(),
    };

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let page_counter = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes.clone()),
        0,
    )
    .unwrap();
    page_counter.validate_all().unwrap();
    let page_count = page_counter.read_stats().page.calls;

    let reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&reader, &[&entry], &mut label_cache).unwrap();

    assert_eq!(
        label_cache.get(&entry.series_id).unwrap().to_vec(),
        resolved_entry_labels(&symbols, &entry)
    );
    let stats = reader.read_stats();
    assert_eq!(
        stats.logical_returned.calls,
        u64::try_from(symbol_count).unwrap()
    );
    // The second four-reference visitor batch reopens the final page with a
    // zero-byte cache, proving that one oversized series does not bypass the
    // per-visitor reference bound.
    assert_eq!(stats.page.calls, page_count + 1);
}

#[test]
fn batched_series_label_resolution_skips_later_duplicate_series() {
    let mut symbols = SegmentSymbols::default();
    let symbol_ids = ['a', 'b', 'c', 'd']
        .into_iter()
        .map(|prefix| symbols.intern(&format!("{prefix}{}", "x".repeat(12_000))))
        .collect::<Vec<_>>();
    let first = SeriesEntry {
        series_id: 42,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(symbol_ids[0], symbol_ids[1])],
    };
    let duplicate_with_missing_symbols = SeriesEntry {
        series_id: first.series_id,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(u32::MAX, u32::MAX)],
    };

    let reader = paged_symbol_reader(&symbols);
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(
        &reader,
        &[&first, &duplicate_with_missing_symbols],
        &mut label_cache,
    )
    .unwrap();

    assert_eq!(label_cache.len(), 1);
    assert_eq!(
        label_cache.get(&first.series_id).unwrap().to_vec(),
        resolved_entry_labels(&symbols, &first)
    );
    assert_eq!(reader.read_stats().logical_returned.calls, 2);
}

#[test]
fn batched_series_label_resolution_skips_duplicate_across_batch_boundary() {
    let mut symbols = SegmentSymbols::default();
    let key = symbols.intern(&format!("a{}", "x".repeat(12_000)));
    let value = symbols.intern(&format!("b{}", "x".repeat(12_000)));
    let mut entries = (0..super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES)
        .map(|series_id| SeriesEntry {
            series_id: u64::try_from(series_id).unwrap(),
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(key, value)],
        })
        .collect::<Vec<_>>();
    entries.push(SeriesEntry {
        series_id: entries[0].series_id,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(u32::MAX, u32::MAX)],
    });

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let entry_refs = entries.iter().collect::<Vec<_>>();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&reader, &entry_refs, &mut label_cache).unwrap();

    assert_eq!(
        label_cache.len(),
        super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES
    );
    assert_eq!(reader.read_stats().page.calls, 1);
    assert_eq!(
        reader.read_stats().logical_returned.calls,
        u64::try_from(super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES * 2).unwrap()
    );
}

#[test]
fn metric_name_index_collection_reads_only_metric_name_values() {
    let mut symbols = SegmentSymbols::default();
    let metric = symbols.intern(METRIC_NAME_LABEL);
    let backend = symbols.intern("backend-1");
    let cpu = symbols.intern("cpu_usage");
    let pod = symbols.intern("pod_name");
    let series = vec![SeriesEntry {
        series_id: 1,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(metric, cpu), (pod, backend)],
    }];
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values,
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };
    let mut index_reader = index_reader_with_corrupt_label_fst(&indexes, pod);
    let symbols = paged_symbol_reader(&symbols);
    let mut metadata = MetadataAccumulator::default();

    collect_metric_names_from_index(&symbols, &mut index_reader, 0, 10_000, &mut metadata).unwrap();

    assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
}

#[test]
fn label_value_index_collection_reads_only_requested_label_values() {
    let mut symbols = SegmentSymbols::default();
    let metric = symbols.intern(METRIC_NAME_LABEL);
    let backend = symbols.intern("backend-1");
    let cpu = symbols.intern("cpu_usage");
    let pod = symbols.intern("pod_name");
    let series = vec![SeriesEntry {
        series_id: 1,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(metric, cpu), (pod, backend)],
    }];
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values,
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };
    let mut index_reader = index_reader_with_corrupt_label_fst(&indexes, metric);
    let symbols = paged_symbol_reader(&symbols);
    let mut metadata = MetadataAccumulator::default();

    collect_label_values_from_index(
        &symbols,
        &mut index_reader,
        "pod_name",
        0,
        10_000,
        &mut metadata,
    )
    .unwrap();

    assert_eq!(
        metadata.label_values("pod_name"),
        vec!["backend-1".to_string()]
    );
}

fn index_reader_with_corrupt_label_fst(
    indexes: &SegmentIndexes,
    label_name_sym: u32,
) -> SegmentIndexReader<Cursor<Vec<u8>>> {
    const TRAILER_LEN: usize = 256;
    const TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET: usize = 104;
    const AUXILIARY_DIRECTORY_HEADER_LEN: usize = 64;
    const AUXILIARY_DIRECTORY_RECORD_LEN: usize = 40;
    const LABEL_VALUE_FST_KIND: u16 = 2;

    let mut bytes = Vec::new();
    write_segment_indexes_unbound_for_test(&mut bytes, indexes).unwrap();
    let trailer_start = bytes.len() - TRAILER_LEN;
    let auxiliary_directory_offset = u64::from_le_bytes(
        bytes[trailer_start + TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET
            ..trailer_start + TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    let entry_count = u64::from_le_bytes(
        bytes[auxiliary_directory_offset + 16..auxiliary_directory_offset + 24]
            .try_into()
            .unwrap(),
    ) as usize;

    let mut payload = None;
    for entry_index in 0..entry_count {
        let record_offset = auxiliary_directory_offset
            + AUXILIARY_DIRECTORY_HEADER_LEN
            + entry_index * AUXILIARY_DIRECTORY_RECORD_LEN;
        let kind = u16::from_le_bytes(bytes[record_offset..record_offset + 2].try_into().unwrap());
        let name = u32::from_le_bytes(
            bytes[record_offset + 4..record_offset + 8]
                .try_into()
                .unwrap(),
        );
        if kind == LABEL_VALUE_FST_KIND && name == label_name_sym {
            let offset = u64::from_le_bytes(
                bytes[record_offset + 8..record_offset + 16]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let len = u64::from_le_bytes(
                bytes[record_offset + 16..record_offset + 24]
                    .try_into()
                    .unwrap(),
            ) as usize;
            payload = Some(offset..offset + len);
            break;
        }
    }
    let payload = payload.expect("label FST auxiliary record");
    bytes[payload].fill(0);

    SegmentIndexReader::open(Cursor::new(bytes)).unwrap()
}

fn read_chunk_encoding(file: &mut File) -> u8 {
    file.seek(SeekFrom::Start(FRAME_HEADER_LEN + 1))
        .expect("seek to encoding");
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf).expect("read encoding");
    buf[0]
}

fn resolved_entry_labels(symbols: &SegmentSymbols, entry: &SeriesEntry) -> Vec<(String, String)> {
    entry
        .labels
        .iter()
        .map(|(key, value)| {
            (
                symbols.resolve(*key).unwrap().to_string(),
                symbols.resolve(*value).unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn segment_id_dir_name_roundtrip() {
    let ulid = Ulid::new();
    let id = SegmentId::with_ulid(10, 20, ulid).unwrap();
    let parsed = SegmentId::parse_dir_name(&id.dir_name()).unwrap();
    assert_eq!(parsed.start_ms(), 10);
    assert_eq!(parsed.end_ms(), 20);
    assert_eq!(parsed.ulid(), ulid);
}

#[test]
fn segment_id_rejects_invalid_range() {
    let err = SegmentId::with_ulid(10, 10, Ulid::new()).unwrap_err();
    assert!(matches!(
        err,
        SegmentIdError::InvalidRange {
            start_ms: 10,
            end_ms: 10
        }
    ));
}

#[test]
fn segment_id_rejects_invalid_dir_name() {
    let err = SegmentId::parse_dir_name("seg-10-20").unwrap_err();
    assert!(matches!(err, SegmentIdError::InvalidFormat(_)));
}

#[test]
fn segment_file_names_are_stable() {
    assert_eq!(SegmentFile::MetaJson.filename(), "meta.json");
    assert_eq!(SegmentFile::Symbols.filename(), "symbols.bin");
    assert_eq!(SegmentFile::Series.filename(), "series.bin");
    assert_eq!(SegmentFile::Chunks.filename(), "chunks.bin");
    assert_eq!(SegmentFile::OooChunks.filename(), "ooo_chunks.bin");
    assert_eq!(SegmentFile::ChunkIndex.filename(), "chunk_index.bin");
    assert_eq!(SegmentFile::Indexes.filename(), "indexes.puffin");
    assert_eq!(SegmentFile::Footer.filename(), "footer.bin");
}

#[test]
fn segment_paths_are_consistent() {
    let id = SegmentId::with_ulid(1, 2, Ulid::new()).unwrap();
    let paths = SegmentPaths::new("/tmp/segments", id);
    let dir = paths.dir();
    assert!(dir.ends_with(id.dir_name()));
    let tmp = paths.temp_dir();
    assert!(tmp.ends_with(format!(".tmp/{}", id.dir_name())));
    let chunk_path = paths.file_path(SegmentFile::Chunks);
    assert!(chunk_path.ends_with("chunks.bin"));
}

#[test]
fn schema8_writer_is_default_and_maps_to_footer_schema8() {
    let config = SegmentWriterConfig::new("/tmp/segments", Duration::from_secs(60));

    assert_eq!(config.storage_schema, SegmentStorageSchema::Schema8);
    assert_eq!(
        config.storage_schema.footer_version(),
        SEGMENT_SCHEMA_VERSION_V8
    );
}

#[test]
fn segment_footer_roundtrips_file_metadata() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);

    let bytes = encode_segment_footer(&footer).unwrap();
    let decoded = decode_segment_footer_for_schema6(&bytes).unwrap();

    assert_eq!(decoded, footer);
}

#[test]
fn schema8_footer_requires_explicit_schema8_decoder() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V8);
    let bytes = encode_segment_footer(&footer).unwrap();

    assert_eq!(decode_segment_footer_for_schema8(&bytes).unwrap(), footer);
    assert_eq!(
        decode_segment_footer_for_schema7(&bytes)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(
        decode_segment_footer_for_schema6(&bytes)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn schema8_footer_validation_integrity_checks_tracked_files() {
    let segment_dir = tempfile::tempdir().unwrap();
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        fs::write(
            segment_dir.path().join(file.filename()),
            file.filename().as_bytes(),
        )
        .unwrap();
    }
    write_segment_footer_for_schema(segment_dir.path(), SEGMENT_SCHEMA_VERSION_V8).unwrap();

    validate_segment_footer_for_schema8(segment_dir.path()).unwrap();
}

#[test]
fn segment_footer_rejects_bad_crc32c() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);
    let mut bytes = encode_segment_footer(&footer).unwrap();
    bytes[SEGMENT_FOOTER_HEADER_LEN] ^= 0xff;

    let err = decode_segment_footer_for_schema6(&bytes).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn layout_ab_footer_decoder_accepts_only_checksum_valid_schema5() {
    let footer = footer_test_fixture(LEGACY_SEGMENT_SCHEMA_VERSION_FOR_LAYOUT_AB);
    let bytes = encode_segment_footer(&footer).unwrap();

    assert_eq!(
        decode_segment_footer_for_schema6(&bytes)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(decode_segment_footer_for_layout_ab(&bytes).unwrap(), footer);

    let mut corrupt = bytes;
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xff;
    assert_eq!(
        decode_segment_footer_for_layout_ab(&corrupt)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn segment_footer_rejects_noncanonical_inventory() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);

    let mut missing = footer.clone();
    missing.files.pop();
    assert_eq!(
        decode_segment_footer_for_schema6(&encode_segment_footer(&missing).unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut duplicate = footer.clone();
    duplicate.files[1] = duplicate.files[0].clone();
    assert_eq!(
        decode_segment_footer_for_schema6(&encode_segment_footer(&duplicate).unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut reordered = footer;
    reordered.files.swap(0, 1);
    assert_eq!(
        decode_segment_footer_for_schema6(&encode_segment_footer(&reordered).unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn segment_footer_rejects_nonzero_reserved_fields() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);
    let encoded = encode_segment_footer(&footer).unwrap();

    let mut payload_reserved = encoded.clone();
    payload_reserved[SEGMENT_FOOTER_HEADER_LEN + 2] = 1;
    rewrite_footer_test_crc(&mut payload_reserved);
    assert_eq!(
        decode_segment_footer_for_schema6(&payload_reserved)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut entry_reserved = encoded;
    entry_reserved[SEGMENT_FOOTER_HEADER_LEN + 6] = 1;
    rewrite_footer_test_crc(&mut entry_reserved);
    assert_eq!(
        decode_segment_footer_for_schema6(&entry_reserved)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

fn footer_test_fixture(schema_version: u16) -> SegmentFooter {
    SegmentFooter {
        schema_version,
        files: SEGMENT_FOOTER_TRACKED_FILES
            .into_iter()
            .enumerate()
            .map(|(index, file)| SegmentFooterFile {
                file,
                size: 128 + index as u64 * 17,
                checksum_xxh64: 0x1122_3344_5566_7788 ^ index as u64,
            })
            .collect(),
    }
}

fn rewrite_footer_test_crc(bytes: &mut [u8]) {
    let payload_end = bytes.len() - SEGMENT_FOOTER_TRAILER_LEN;
    let header: &[u8; SEGMENT_FOOTER_HEADER_LEN] =
        bytes[..SEGMENT_FOOTER_HEADER_LEN].try_into().unwrap();
    let crc = segment_footer_crc(header, &bytes[SEGMENT_FOOTER_HEADER_LEN..payload_end]);
    bytes[payload_end..].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn segment_footer_validation_rejects_tracked_file_corruption() {
    let tempdir = tempfile::tempdir().unwrap();
    write_footer_test_files(tempdir.path());
    write_segment_footer_for_schema6(tempdir.path()).unwrap();
    validate_segment_footer_for_schema6(tempdir.path()).unwrap();

    let symbols_path = tempdir.path().join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    symbols[0] ^= 0xff;
    fs::write(symbols_path, symbols).unwrap();
    let err = validate_segment_footer_for_schema6(tempdir.path()).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn segment_footer_hashes_large_files_with_the_fixed_streaming_buffer() {
    let tempdir = tempfile::tempdir().unwrap();
    let len = SEGMENT_FOOTER_HASH_BUFFER_BYTES * 2 + 17;
    let bytes: Vec<u8> = (0..len)
        .map(|index| (index as u8).wrapping_mul(19).wrapping_add(5))
        .collect();
    fs::write(tempdir.path().join(SegmentFile::Chunks.filename()), &bytes).unwrap();

    let entry = segment_footer_file(tempdir.path(), SegmentFile::Chunks).unwrap();

    assert_eq!(SEGMENT_FOOTER_HASH_BUFFER_BYTES, 1024 * 1024);
    assert_eq!(entry.size, len as u64);
    assert_eq!(entry.checksum_xxh64, xxhash64(&bytes));
}

fn write_footer_test_files(dir: &Path) {
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        fs::write(
            dir.join(file.filename()),
            format!("content:{}", file.filename()),
        )
        .unwrap();
    }
}

#[test]
fn manifest_segment_meta_accepts_matching_meta() {
    let id = SegmentId::with_ulid(100, 200, Ulid::new()).unwrap();
    let manifest_segment =
        crate::storage::manifest::ManifestSegment::new(id.dir_name(), 100, 200, Some(42)).unwrap();
    let meta = SegmentMeta {
        segment_id: id.dir_name(),
        start_ms: 100,
        end_ms: 200,
        datapoints: 3,
        series: 1,
        chunk_summary: None,
    };

    validate_manifest_segment_meta(&manifest_segment, &meta).unwrap();
}

#[test]
fn manifest_segment_meta_rejects_mismatched_meta_json() {
    let id = SegmentId::with_ulid(100, 200, Ulid::new()).unwrap();
    let manifest_segment =
        crate::storage::manifest::ManifestSegment::new(id.dir_name(), 100, 200, Some(42)).unwrap();
    let meta = SegmentMeta {
        segment_id: id.dir_name(),
        start_ms: 100,
        end_ms: 201,
        datapoints: 3,
        series: 1,
        chunk_summary: None,
    };

    let err = validate_manifest_segment_meta(&manifest_segment, &meta).unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn segment_writer_creates_segment_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer.flush().unwrap();

    let entries: Vec<_> = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .collect();
    assert_eq!(entries.len(), 1);

    let seg_dir = entries[0].path();
    assert!(seg_dir.join("meta.json").exists());
    assert!(seg_dir.join("chunks.bin").exists());
    assert!(seg_dir.join("series.bin").exists());
    assert!(seg_dir.join("symbols.bin").exists());
    assert!(seg_dir.join("chunk_index.bin").exists());
    assert!(seg_dir.join("indexes.puffin").exists());
    assert!(!seg_dir.join("routing_index.bin").exists());
    assert!(seg_dir.join("footer.bin").exists());
    let chunk_len = fs::metadata(seg_dir.join("chunks.bin")).unwrap().len();
    assert!(chunk_len > 0);
    let index_len = fs::metadata(seg_dir.join("chunk_index.bin")).unwrap().len();
    assert!(index_len > 0);
}

#[test]
fn schema7_writer_publishes_v3_v2_v8_roots() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.5)], |visit| {
            visit(METRIC_NAME_LABEL, "schema7_metric");
            visit("service", "api");
        })
        .unwrap();
    writer.flush().unwrap();

    let stages = writer.last_flush_profile().unwrap().stage_kinds();
    let ooo_stage = stages
        .iter()
        .position(|stage| *stage == SegmentFlushStageKind::OooChunks)
        .unwrap();
    let series_stage = stages
        .iter()
        .position(|stage| *stage == SegmentFlushStageKind::Series)
        .unwrap();
    assert!(ooo_stage < series_stage);

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    validate_segment_footer_for_schema7(&segment_dir).unwrap();

    let series = fs::read(segment_dir.join(SegmentFile::Series.filename())).unwrap();
    let series_header = crate::storage::series::v3::SeriesHeaderV3::decode(
        &series[..crate::storage::series::v3::SERIES_HEADER_LEN_V3],
    )
    .unwrap();
    crate::storage::series::v3::decode_series_root_v3(
        &series[..usize::try_from(series_header.hot_pages_offset).unwrap()],
    )
    .unwrap();

    let chunk_index = fs::read(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    let overflow_root = crate::storage::chunk::decode_chunk_overflow_root_v2(
        &chunk_index[..crate::storage::chunk::CHUNK_OVERFLOW_ROOT_V2_LEN],
        chunk_index.len() as u64,
    )
    .unwrap();
    assert_eq!(overflow_root.series_count, 1);
    assert_eq!(overflow_root.blob_count, 0);

    let indexes = fs::read(segment_dir.join(SegmentFile::Indexes.filename())).unwrap();
    assert_eq!(u16::from_le_bytes(indexes[4..6].try_into().unwrap()), 8);
    assert_eq!(
        read_segment_footer_for_schema7(&segment_dir)
            .unwrap()
            .schema_version,
        SEGMENT_SCHEMA_VERSION_V7
    );
}

#[test]
fn default_schema8_writer_publishes_v3_v2_v9_roots() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.5)], |visit| {
            visit(METRIC_NAME_LABEL, "schema8_metric");
            visit("service", "api");
        })
        .unwrap();
    writer.flush().unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    validate_segment_footer_for_schema8(&segment_dir).unwrap();

    let series = fs::read(segment_dir.join(SegmentFile::Series.filename())).unwrap();
    let series_header = crate::storage::series::v3::SeriesHeaderV3::decode(
        &series[..crate::storage::series::v3::SERIES_HEADER_LEN_V3],
    )
    .unwrap();
    crate::storage::series::v3::decode_series_root_v3(
        &series[..usize::try_from(series_header.hot_pages_offset).unwrap()],
    )
    .unwrap();

    let chunk_index = fs::read(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    let overflow_root = crate::storage::chunk::decode_chunk_overflow_root_v2(
        &chunk_index[..crate::storage::chunk::CHUNK_OVERFLOW_ROOT_V2_LEN],
        chunk_index.len() as u64,
    )
    .unwrap();
    assert_eq!(overflow_root.series_count, 1);
    assert_eq!(overflow_root.blob_count, 0);

    let indexes = fs::read(segment_dir.join(SegmentFile::Indexes.filename())).unwrap();
    assert_eq!(u16::from_le_bytes(indexes[4..6].try_into().unwrap()), 9);
    assert_eq!(
        read_segment_footer_for_schema8(&segment_dir)
            .unwrap()
            .schema_version,
        SEGMENT_SCHEMA_VERSION_V8
    );
}

#[test]
fn explicit_schema6_selection_is_deterministic() {
    fn write(path: &Path) -> BTreeMap<String, Vec<u8>> {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42)
            .with_storage_schema(SegmentStorageSchema::Schema6);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.5)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "schema6_metric");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer.flush().unwrap();

        let segment = fs::read_dir(path)
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        SEGMENT_FLUSH_SIZE_FILES
            .iter()
            .map(|file| {
                (
                    file.filename().to_string(),
                    fs::read(segment.join(file.filename())).unwrap(),
                )
            })
            .collect()
    }

    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    assert_eq!(write(first.path()), write(second.path()));
}

#[test]
fn query_context_series_entry_reads_preserve_cardinality_and_reject_missing_refs() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    for (series_ref, instance, value) in [
        (SeriesRef::new(1), "first", 1.5),
        (SeriesRef::new(2), "second", 2.5),
    ] {
        writer
            .record_samples_ordered_with_label_visitor(series_ref, &[(1_000, value)], |visit| {
                visit(METRIC_NAME_LABEL, "entry_cardinality");
                visit("instance", instance);
            })
            .unwrap();
    }
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = open_schema6_segment_for_test(seg_dir).unwrap();
    let mut context = SegmentQueryContext::open(&reader).unwrap();

    let invalid_refs = [0, u32::MAX];
    let error = context
        .read_series_entries(&reader, &invalid_refs)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&u32::MAX.to_string()));
    assert!(reader.query_cache.series_entries.lock().unwrap().is_empty());

    let error = context
        .read_series_entries_uncached(&reader, &invalid_refs)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&u32::MAX.to_string()));
    assert!(reader.query_cache.series_entries.lock().unwrap().is_empty());

    let error = context
        .read_series_metadata_entries(&reader, &invalid_refs)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&u32::MAX.to_string()));
    assert!(
        reader
            .query_cache
            .series_metadata
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(
        reader
            .query_cache
            .series_locators
            .lock()
            .unwrap()
            .is_empty()
    );

    let ordered_refs = [1, 0];
    for actual in [
        context
            .read_series_entries(&reader, &ordered_refs)
            .unwrap()
            .into_iter()
            .map(|(series_ref, _)| series_ref)
            .collect::<Vec<_>>(),
        context
            .read_series_entries_uncached(&reader, &ordered_refs)
            .unwrap()
            .into_iter()
            .map(|(series_ref, _)| series_ref)
            .collect::<Vec<_>>(),
        context
            .read_series_metadata_entries(&reader, &ordered_refs)
            .unwrap()
            .into_iter()
            .map(|(series_ref, _)| series_ref)
            .collect::<Vec<_>>(),
    ] {
        assert_eq!(actual, ordered_refs);
    }

    let duplicate_refs = [0, 0];
    for error in [
        context
            .read_series_entries(&reader, &duplicate_refs)
            .unwrap_err(),
        context
            .read_series_entries_uncached(&reader, &duplicate_refs)
            .unwrap_err(),
        context
            .read_series_metadata_entries(&reader, &duplicate_refs)
            .unwrap_err(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("series ref 0"));
    }
}

#[test]
fn segment_writer_remaps_sealed_indexes_to_sorted_symbol_ids() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("z.label".to_string(), "last".to_string()),
        ("a.label".to_string(), "first".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(1_000, 1.5)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = open_schema6_segment_for_test(&seg_dir).unwrap();
    let symbols =
        read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols)).unwrap()).unwrap();
    let symbol_values: Vec<_> = (0..symbols.len())
        .map(|idx| symbols.resolve(idx as u32).unwrap().to_string())
        .collect();
    let mut sorted_symbol_values = symbol_values.clone();
    sorted_symbol_values.sort();
    assert_eq!(symbol_values, sorted_symbol_values);

    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).unwrap()).unwrap();
    assert_eq!(
        resolved_entry_labels(&symbols, &series[0]),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.usage")
            ),
            (normalize_label_name("a.label"), "first".to_string()),
            (normalize_label_name("z.label"), "last".to_string()),
        ]
    );

    let results = reader
        .query_exact(
            &[
                (
                    METRIC_NAME_LABEL,
                    normalize_metric_name("cpu.usage").as_str(),
                ),
                (normalize_label_name("a.label").as_str(), "first"),
            ],
            0,
            2_000,
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].1, 1.5);
}

#[test]
fn segment_writer_orders_sealed_series_by_metric_name_and_preserves_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();

    let z_first = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z-first".to_string()),
    ];
    let a_first = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a-first".to_string()),
    ];
    let z_second = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z-second".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(10), &z_first, &[(1_000, 10.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_first, &[(1_000, 20.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(12), &z_second, &[(1_000, 30.0)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = open_schema6_segment_for_test(&seg_dir).unwrap();
    let symbols =
        read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols)).unwrap()).unwrap();
    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).unwrap()).unwrap();

    let ordered_labels: Vec<_> = series
        .iter()
        .map(|entry| resolved_entry_labels(&symbols, entry))
        .collect();
    let label_value = |labels: &[(String, String)], name: &str| {
        labels
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
            .unwrap()
            .to_string()
    };
    assert_eq!(
        ordered_labels
            .iter()
            .map(|labels| (
                label_value(labels, METRIC_NAME_LABEL),
                label_value(labels, normalize_label_name("pod.name").as_str())
            ))
            .collect::<Vec<_>>(),
        vec![
            (normalize_metric_name("a.metric"), "a-first".to_string()),
            (normalize_metric_name("z.metric"), "z-first".to_string()),
            (normalize_metric_name("z.metric"), "z-second".to_string()),
        ]
    );

    let chunk_entries = reader.read_chunk_index().unwrap();
    assert_eq!(chunk_entries.len(), 3);
    let chunk_offsets = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            entries[0].offset
        })
        .collect::<Vec<_>>();
    assert_eq!(
        chunk_offsets,
        {
            let mut sorted = chunk_offsets.clone();
            sorted.sort_unstable();
            sorted
        },
        "chunks.bin offsets should follow final metric-query series order"
    );
    let mut chunks = reader.open_chunks().unwrap();
    let decoded: Vec<_> = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            read_chunk_record_at(&mut chunks, entries[0].offset, entries[0].length).unwrap()
        })
        .collect();
    assert_eq!(
        decoded
            .iter()
            .map(|record| record.series_ref)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        decoded
            .iter()
            .map(|record| match &record.samples {
                ChunkSamples::Float(samples) => samples[0].1,
                other => panic!("unexpected chunk samples: {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![20.0, 10.0, 30.0]
    );

    let a_results = reader
        .query_exact(
            &[(
                METRIC_NAME_LABEL,
                normalize_metric_name("a.metric").as_str(),
            )],
            0,
            2_000,
        )
        .unwrap();
    assert_eq!(a_results.len(), 1);
    assert_eq!(a_results[0].samples, vec![(1_000, 20.0)]);
}

#[test]
fn segment_writer_orders_chunk_payloads_by_series_ref_then_time() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();

    let z_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z".to_string()),
    ];
    let a_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(3_000, 30.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(10), &z_labels, &[(1_000, 10.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(1_000, 20.0)])
        .unwrap();
    writer.flush().unwrap();
    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.chunk_rewrite_frames(), 3);
    assert!(profile.chunk_rewrite_payload_bytes() > 0);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = open_schema6_segment_for_test(&seg_dir).unwrap();
    let chunk_entries = reader.read_chunk_index().unwrap();
    assert_eq!(chunk_entries.len(), 2);
    assert_eq!(
        chunk_entries[0]
            .iter()
            .map(|entry| entry.min_time_ms)
            .collect::<Vec<_>>(),
        vec![1_000, 3_000]
    );

    let chunk_offsets = chunk_entries
        .iter()
        .flat_map(|entries| entries.iter().map(|entry| entry.offset))
        .collect::<Vec<_>>();
    assert_eq!(
        chunk_offsets,
        {
            let mut sorted = chunk_offsets.clone();
            sorted.sort_unstable();
            sorted
        },
        "chunks.bin offsets should be series-major and time-ordered within each series"
    );
}

#[test]
fn segment_writer_publishes_manifest_records_for_flushed_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let manifest_dir = tempdir.path().join("manifest");
    let inventory = crate::storage::manifest::read_manifest_inventory(&manifest_dir)
        .unwrap()
        .expect("manifest inventory");
    let segment_dirs = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .collect::<Vec<_>>();

    assert_eq!(segment_dirs.len(), 1);
    assert_eq!(inventory.segments.len(), 1);
    assert_eq!(
        inventory.segments[0].segment_id,
        segment_dirs[0].file_name().to_string_lossy()
    );
}

#[test]
fn manifest_published_open_skips_footer_validation_by_default() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let manifest_dir = tempdir.path().join("manifest");
    let inventory = crate::storage::manifest::read_manifest_inventory(&manifest_dir)
        .unwrap()
        .expect("manifest inventory");
    assert_eq!(inventory.segments.len(), 1);
    let segment_dir = tempdir.path().join(&inventory.segments[0].segment_id);
    let symbols_path = segment_dir.join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    let pages_offset = u64::from_le_bytes(symbols[56..64].try_into().unwrap()) as usize;
    symbols[pages_offset] ^= 0xff;
    fs::write(symbols_path, symbols).unwrap();

    let store = SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir)
        .expect("default manifest open should skip heavy footer validation");
    assert_eq!(store.segments.len(), 1);
    let err = match SegmentStoreReader::open_manifest_published_with_options(
        tempdir.path(),
        &manifest_dir,
        SegmentStoreOpenOptions {
            validate_segment_footers: true,
            ..SegmentStoreOpenOptions::default()
        },
    ) {
        Ok(_) => panic!("validated manifest open should catch footer mismatch"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn validated_segment_open_parses_every_symbols_page_after_footer_hashing() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let symbols_path = segment_dir.join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    let pages_offset = u64::from_le_bytes(symbols[56..64].try_into().unwrap()) as usize;
    symbols[pages_offset] ^= 0xff;
    fs::write(&symbols_path, symbols).unwrap();

    // Integrity-check the deliberately malformed bytes so this test exercises
    // structural page validation rather than a footer hash mismatch.
    write_segment_footer_for_schema(&segment_dir, SEGMENT_SCHEMA_VERSION_V8).unwrap();
    SegmentReader::open(&segment_dir).unwrap();
    let error = match SegmentReader::open_validated(&segment_dir) {
        Ok(_) => panic!("validated open accepted a malformed symbols page"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("symbols page CRC mismatch"));
}

#[test]
fn ordinary_segment_open_rejects_an_old_schema_without_hashing_tracked_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    writer.record_sample(SeriesRef::new(1), 5_000, 1.0).unwrap();
    writer.flush().unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let footer_path = segment_dir.join(SegmentFile::Footer.filename());
    let mut footer = read_segment_footer_for_schema8(&segment_dir).unwrap();
    footer.schema_version = SEGMENT_SCHEMA_VERSION_V7;
    fs::write(footer_path, encode_segment_footer(&footer).unwrap()).unwrap();

    let error = match SegmentReader::open(&segment_dir) {
        Ok(_) => panic!("ordinary open accepted an old segment schema"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("schema version"));
}

fn rewrite_symbols_and_footer_as_schema5_v2_for_layout_ab(segment_dir: &Path) {
    let symbols_path = segment_dir.join(SegmentFile::Symbols.filename());
    let symbols = read_symbols_bin(File::open(&symbols_path).unwrap()).unwrap();
    let mut string_bytes = Vec::new();
    let mut offsets = Vec::with_capacity(symbols.len() + 1);
    offsets.push(0u64);
    for symbol_id in 0..symbols.len() {
        string_bytes.extend_from_slice(
            symbols
                .resolve(u32::try_from(symbol_id).unwrap())
                .unwrap()
                .as_bytes(),
        );
        offsets.push(u64::try_from(string_bytes.len()).unwrap());
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&crate::storage::symbols::SYMBOLS_V3_MAGIC.to_le_bytes());
    encoded.extend_from_slice(
        &crate::storage::symbols::SYMBOLS_V2_VERSION_FOR_LAYOUT_AB.to_le_bytes(),
    );
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&u32::try_from(symbols.len()).unwrap().to_le_bytes());
    for offset in offsets {
        encoded.extend_from_slice(&offset.to_le_bytes());
    }
    encoded.extend_from_slice(&string_bytes);
    fs::write(symbols_path, encoded).unwrap();

    let mut footer = build_segment_footer_for_schema6(segment_dir).unwrap();
    footer.schema_version = LEGACY_SEGMENT_SCHEMA_VERSION_FOR_LAYOUT_AB;
    fs::write(
        segment_dir.join(SegmentFile::Footer.filename()),
        encode_segment_footer(&footer).unwrap(),
    )
    .unwrap();
}

#[test]
fn schema6_layout_ab_rejects_schema5() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.5), (2_000, 2.5)],
            |visit| {
                visit(METRIC_NAME_LABEL, "layout.ab.metric");
                visit("service", "api");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    open_schema6_store_for_test(tempdir.path()).unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    rewrite_symbols_and_footer_as_schema5_v2_for_layout_ab(&segment_dir);
    let error = match SegmentStoreReader::open(tempdir.path()) {
        Ok(_) => panic!("production store open accepted schema 5"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);

    let error = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .err()
    .expect("schema-6 layout A/B accepted retired schema 5");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("schema version"));
}

#[test]
fn explicit_layout_ab_rejects_a_mixed_schema5_schema6_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();
    writer
        .record_sample(SeriesRef::new(1), 11_000, 2.0)
        .unwrap();
    writer.flush().unwrap();
    let mut segment_dirs = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    segment_dirs.sort();
    assert_eq!(segment_dirs.len(), 2);
    rewrite_symbols_and_footer_as_schema5_v2_for_layout_ab(&segment_dirs[0]);

    let error = match SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            ..SegmentStoreOpenOptions::default()
        },
    ) {
        Ok(_) => panic!("layout A/B open accepted a mixed-schema store"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("schema version"));
}

#[test]
fn segment_writer_persists_chunk_summary_in_meta() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(
                2_000,
                HistogramValue {
                    count: 2,
                    sum: Some(3.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let meta: SegmentMeta =
        serde_json::from_slice(&fs::read(seg_dir.join("meta.json")).unwrap()).unwrap();
    let summary = meta.chunk_summary.expect("chunk summary");

    assert_eq!(summary.chunks, 2);
    assert_eq!(summary.by_kind.float.chunks, 1);
    assert_eq!(summary.by_kind.histogram.chunks, 1);
    assert!(summary.chunk_bytes > 0);
    assert!(summary.by_kind.float.chunk_bytes > 0);
    assert!(summary.by_kind.histogram.chunk_bytes > 0);
}

#[test]
fn smoke_verify_uses_chunk_summary_for_totals_without_chunk_scan_when_not_sampling() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    fs::remove_file(seg_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    fs::remove_file(seg_dir.join(SegmentFile::Chunks.filename())).unwrap();

    let report = store.smoke_verify(0, 10_000, 0).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.chunks, 1);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert!(report.sample_series.is_empty());
    assert!(report.queries.is_empty());
}

#[test]
fn deterministic_segment_id_provider_replays_same_sequence() {
    let first = DeterministicSegmentIdProvider::new(7);
    let first_id = first.next_segment_id(0, 10_000).unwrap();
    let second_id = first.next_segment_id(10_000, 20_000).unwrap();
    assert_ne!(first_id, second_id);

    let replay = DeterministicSegmentIdProvider::new(7);
    assert_eq!(replay.next_segment_id(0, 10_000).unwrap(), first_id);
    assert_eq!(replay.next_segment_id(10_000, 20_000).unwrap(), second_id);
}

#[test]
fn segment_writer_with_deterministic_ids_replays_same_directory_names() {
    fn write_segments(path: &Path) -> Vec<String> {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42);
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
        writer
            .record_sample(SeriesRef::new(1), 11_000, 2.5)
            .unwrap();
        writer.flush().unwrap();

        let mut names: Vec<_> = fs::read_dir(path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with("seg-"))
            .collect();
        names.sort();
        names
    }

    let first = tempfile::tempdir().unwrap();
    let replay = tempfile::tempdir().unwrap();

    let first_names = write_segments(first.path());
    let replay_names = write_segments(replay.path());

    assert_eq!(first_names.len(), 2);
    assert_eq!(first_names, replay_names);
}

#[test]
fn segment_writer_records_flush_profile_stages() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ];

    assert!(writer.last_flush_profile().is_none());

    writer
        .record_samples_with_labels(SeriesRef::new(7), &labels, &[(1_000, 1.5), (2_000, 2.5)])
        .unwrap();
    writer.flush().unwrap();

    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.datapoints, 2);
    assert_eq!(profile.series, 1);
    assert_eq!(
        profile.stage_kinds(),
        &[
            SegmentFlushStageKind::MetaJson,
            SegmentFlushStageKind::ChunksFlush,
            SegmentFlushStageKind::ChunkIndex,
            SegmentFlushStageKind::SegmentMetadata,
            SegmentFlushStageKind::LabelValues,
            SegmentFlushStageKind::LabelValueTimeRanges,
            SegmentFlushStageKind::MetricSeriesRanges,
            SegmentFlushStageKind::RoutingIndexBuild,
            SegmentFlushStageKind::Symbols,
            SegmentFlushStageKind::OooChunks,
            SegmentFlushStageKind::Series,
            SegmentFlushStageKind::Indexes,
            SegmentFlushStageKind::Footer,
            SegmentFlushStageKind::Publish,
        ]
    );
    assert!(
        profile
            .stage_elapsed(SegmentFlushStageKind::SegmentMetadata)
            .is_some()
    );
    assert!(
        profile.total
            >= profile
                .stage_elapsed(SegmentFlushStageKind::Publish)
                .unwrap()
    );
}

#[test]
fn segment_writer_records_flush_profile_file_sizes() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(7), &labels, &[(1_000, 1.5), (2_000, 2.5)])
        .unwrap();
    writer.flush().unwrap();

    let profile = writer.last_flush_profile().unwrap();
    for file in [
        SegmentFile::MetaJson,
        SegmentFile::Symbols,
        SegmentFile::Series,
        SegmentFile::Chunks,
        SegmentFile::OooChunks,
        SegmentFile::ChunkIndex,
        SegmentFile::Indexes,
        SegmentFile::Footer,
    ] {
        assert!(
            profile.file_size_bytes(file).is_some(),
            "missing file size for {}",
            file.filename()
        );
    }
    assert!(profile.file_size_bytes(SegmentFile::Chunks).unwrap() > 0);
    assert!(profile.total_file_bytes() >= profile.file_size_bytes(SegmentFile::Chunks).unwrap());
    assert_eq!(
        profile.total_file_bytes(),
        profile.data_file_bytes()
            + profile.metadata_file_bytes()
            + profile.index_file_bytes()
            + profile.footer_file_bytes()
    );
}

#[test]
fn segment_writer_skips_chunk_rewrite_when_input_is_metric_query_ordered() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let a_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a".to_string()),
    ];
    let z_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(1_000, 20.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(10), &z_labels, &[(1_000, 10.0)])
        .unwrap();
    writer.flush().unwrap();

    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.chunk_rewrite_frames(), 0);
    assert_eq!(profile.chunk_rewrite_payload_bytes(), 0);
}

#[test]
fn segment_writer_reserves_active_window_series_structures() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.reserve_window_series(0, 10_000, 4_096).unwrap();

    let active = writer.active.as_ref().unwrap();
    assert!(active.series_map.capacity() >= 4_096);
    assert!(active.metadata_present.capacity() >= 4_096);
    assert!(active.series_entries.capacity() >= 4_096);
    assert!(active.chunk_entries.capacity() >= 4_096);
}

#[test]
fn segment_writer_records_record_path_profile() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let before = writer.record_profile();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(7),
            &[(1_000, 1.5), (2_000, 2.5)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu_usage");
                visit("pod", "backend-1");
            },
        )
        .unwrap();

    let delta = writer.record_profile().saturating_sub(before);
    assert_eq!(delta.chunks, 1);
    assert_eq!(delta.samples, 2);
    assert_eq!(delta.label_time_range, Duration::ZERO);
    assert!(delta.total_elapsed() <= delta.wall_elapsed);
}

#[test]
fn segment_series_metadata_builder_matches_raw_label_canonicalization() {
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];

    let canonical = crate::promql::canonicalize_labelset(
        "cpu.usage",
        &[("namespace", "default"), ("pod.name", "backend-1")],
    );
    let expected_series_id = crate::promql::series_id(&canonical);
    let expected_labels: Vec<_> = canonical
        .labels()
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();

    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in &raw_labels {
        builder.push_label(key, value);
    }
    let metadata = builder.finish();

    assert_eq!(metadata.series_id, expected_series_id);
    assert_eq!(metadata.labels, expected_labels);
}

#[test]
fn segment_series_metadata_builder_keeps_first_metric_name() {
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.first".to_string()),
        (METRIC_NAME_LABEL.to_string(), "cpu.second".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];

    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in &raw_labels {
        builder.push_label(key, value);
    }
    let metadata = builder.finish();

    assert!(metadata.labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.first")
    }));
    assert!(!metadata.labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.second")
    }));
}

#[test]
fn existing_metric_name_fast_path_leaves_metadata_unchanged() {
    let mut symbols = SegmentSymbols::default();
    let pod_key = symbols.intern("pod");
    let pod_value = symbols.intern("backend-1");
    let metric_key = symbols.intern(METRIC_NAME_LABEL);
    let metric_value = symbols.intern("cpu_usage");
    let mut entry = SeriesEntry {
        series_id: 42,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(pod_key, pod_value), (metric_key, metric_value)],
    };
    let expected_symbols = symbols.clone();
    let expected_entry = entry.clone();

    super::writer::synthesize_missing_metric_name(&mut symbols, &mut entry).unwrap();

    assert_eq!(symbols, expected_symbols);
    assert_eq!(entry, expected_entry);
}

#[test]
fn missing_metric_name_is_synthesized_and_rehashes_canonical_labels() {
    let mut symbols = SegmentSymbols::default();
    let pod_key = symbols.intern("pod");
    let pod_value = symbols.intern("backend-1");
    let namespace_key = symbols.intern("namespace");
    let namespace_value = symbols.intern("default");
    let mut entry = SeriesEntry {
        series_id: 42,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(pod_key, pod_value), (namespace_key, namespace_value)],
    };
    let expected_labels = vec![
        (METRIC_NAME_LABEL.to_string(), String::new()),
        ("namespace".to_string(), "default".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ];

    super::writer::synthesize_missing_metric_name(&mut symbols, &mut entry).unwrap();

    assert_eq!(entry.labels.len(), 3);
    assert!(entry.labels.iter().any(|(key, value)| {
        symbols.resolve(*key) == Some(METRIC_NAME_LABEL) && symbols.resolve(*value) == Some("")
    }));
    assert_eq!(entry.series_id, segment_series_id(&expected_labels));
}

#[test]
fn existing_metric_name_fast_path_still_rejects_later_missing_symbols() {
    let mut symbols = SegmentSymbols::default();
    let metric_key = symbols.intern(METRIC_NAME_LABEL);
    let metric_value = symbols.intern("cpu_usage");
    let pod_key = symbols.intern("pod");
    let pod_value = symbols.intern("backend-1");

    for (labels, expected_message) in [
        (
            vec![(metric_key, metric_value), (u32::MAX, pod_value)],
            "series references missing key symbol",
        ),
        (
            vec![(metric_key, metric_value), (pod_key, u32::MAX)],
            "series references missing value symbol",
        ),
    ] {
        let mut entry = SeriesEntry {
            series_id: 42,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels,
        };
        let expected_symbols = symbols.clone();
        let expected_entry = entry.clone();

        let error = super::writer::synthesize_missing_metric_name(&mut symbols, &mut entry)
            .expect_err("missing symbol must remain corruption");

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), expected_message);
        assert_eq!(symbols, expected_symbols);
        assert_eq!(entry, expected_entry);
    }
}

#[test]
fn label_visitor_encoder_matches_metadata_builder_canonicalization() {
    let raw_labels = [
        (METRIC_NAME_LABEL, "cpu.usage"),
        ("pod.name", "backend-1"),
        ("namespace", "default"),
    ];
    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in raw_labels {
        builder.push_label(key, value);
    }
    let expected = builder.finish();

    let mut symbols = SegmentSymbols::default();
    let entry = encode_label_visitor_metadata(&mut symbols, |visit| {
        for (key, value) in raw_labels {
            visit(key, value);
        }
    });

    let labels = resolved_entry_labels(&symbols, &entry);
    assert_eq!(entry.series_id, expected.series_id);
    assert_eq!(labels, expected.labels);
}

#[test]
fn borrowed_label_encoder_matches_owned_canonical_encoding() {
    let canonical = [
        (
            METRIC_NAME_LABEL.to_string(),
            normalize_metric_name("cpu.usage"),
        ),
        (normalize_label_name("namespace"), "default".to_string()),
        (normalize_label_name("pod.name"), "backend-1".to_string()),
    ];

    let mut owned_symbols = SegmentSymbols::default();
    let owned = encode_canonical_segment_labels(
        canonical
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        &mut owned_symbols,
    );

    let mut borrowed_symbols = SegmentSymbols::default();
    let borrowed = encode_borrowed_canonical_segment_labels(
        canonical
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        &mut borrowed_symbols,
    );

    assert_eq!(borrowed.series_id, owned.series_id);
    assert_eq!(
        resolved_entry_labels(&borrowed_symbols, &borrowed),
        resolved_entry_labels(&owned_symbols, &owned)
    );
}

#[test]
fn flat_interned_label_encoder_matches_visitor_encoding() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("namespace", "default")),
        crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut visitor_symbols = SegmentSymbols::default();
    let visitor = encode_label_visitor_metadata(&mut visitor_symbols, |visit| {
        crate::labels::LabelSetStore::visit_labelset(&store, series, |key, value| visit(key, value))
    });

    let mut flat_symbols = SegmentSymbols::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::new();
    let mut label_scratch = Vec::new();
    let flat = encode_flat_interned_label_metadata(
        &mut flat_symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );

    assert_eq!(flat.series_id, visitor.series_id);
    assert_eq!(
        resolved_entry_labels(&flat_symbols, &flat),
        resolved_entry_labels(&visitor_symbols, &visitor)
    );
}

#[test]
fn flat_interned_label_encoder_reuses_scratch_buffers() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("namespace", "default")),
        crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut symbols = SegmentSymbols::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::with_capacity(256);
    let initial_capacity = hash_scratch.capacity();
    let mut label_scratch = Vec::with_capacity(32);
    let initial_label_capacity = label_scratch.capacity();

    let first = encode_flat_interned_label_metadata(
        &mut symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );
    assert_eq!(hash_scratch.len(), 0);
    assert_eq!(hash_scratch.capacity(), initial_capacity);
    assert_eq!(label_scratch.len(), 0);
    assert_eq!(label_scratch.capacity(), initial_label_capacity);

    let second = encode_flat_interned_label_metadata(
        &mut symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );
    assert_eq!(hash_scratch.len(), 0);
    assert_eq!(hash_scratch.capacity(), initial_capacity);
    assert_eq!(label_scratch.len(), 0);
    assert_eq!(label_scratch.capacity(), initial_label_capacity);
    assert_eq!(second.series_id, first.series_id);
    assert_eq!(
        resolved_entry_labels(&symbols, &second),
        resolved_entry_labels(&symbols, &first)
    );
}

#[test]
fn flat_interned_label_encoder_matches_disambiguated_label_canonicalization() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("pod.name", "dotted")),
        crate::labels::KeyValueRef::from(("pod_name", "underscore")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut visitor_symbols = SegmentSymbols::default();
    let visitor = encode_label_visitor_metadata(&mut visitor_symbols, |visit| {
        crate::labels::LabelSetStore::visit_labelset(&store, series, |key, value| visit(key, value))
    });

    let mut flat_symbols = SegmentSymbols::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::new();
    let mut label_scratch = Vec::new();
    let flat = encode_flat_interned_label_metadata(
        &mut flat_symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );

    let flat_labels = resolved_entry_labels(&flat_symbols, &flat);
    assert_eq!(flat.series_id, visitor.series_id);
    assert_eq!(
        flat_labels,
        resolved_entry_labels(&visitor_symbols, &visitor)
    );
    assert_eq!(
        flat_labels,
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.usage")
            ),
            (normalize_label_name("pod_name"), "underscore".to_string()),
            (normalize_label_name("pod.name"), "dotted".to_string()),
        ]
    );
}

#[test]
fn flat_interned_sorted_label_encoder_keeps_last_duplicate_key() {
    let duplicate_key: Arc<str> = Arc::from("pod_name");
    let labels = vec![
        (
            Arc::from(METRIC_NAME_LABEL),
            SourceLabelValue::Owned(Arc::from("cpu_usage")),
        ),
        (
            Arc::clone(&duplicate_key),
            SourceLabelValue::Owned(Arc::from("first")),
        ),
        (duplicate_key, SourceLabelValue::Owned(Arc::from("second"))),
    ];
    let source_symbols = crate::labels::DefaultSymbolTable::default();
    let mut symbols = SegmentSymbols::default();
    let mut hash_scratch = Vec::new();

    let entry = encode_flat_interned_sorted_labels(
        &labels,
        &source_symbols,
        &mut symbols,
        &mut hash_scratch,
    );

    assert_eq!(
        resolved_entry_labels(&symbols, &entry),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            (normalize_label_name("pod_name"), "second".to_string()),
        ]
    );
}

#[test]
fn normalized_name_cache_reuses_label_and_metric_names_by_source_symbol_id() {
    let mut cache = NormalizedNameCache::default();
    let mut label_normalizations = 0usize;
    let mut metric_normalizations = 0usize;
    let mut source_symbols = crate::labels::DefaultSymbolTable::default();
    let label_id = source_symbols.intern("pod.name").unwrap();
    let metric_id = source_symbols.intern("cpu.usage").unwrap();

    let first_label = cache.label_name(label_id, "pod.name", |name| {
        label_normalizations += 1;
        normalize_label_name(name)
    });
    let second_label = cache.label_name(label_id, "pod.name", |name| {
        label_normalizations += 1;
        normalize_label_name(name)
    });
    let first_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
        metric_normalizations += 1;
        normalize_metric_name(name)
    });
    let second_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
        metric_normalizations += 1;
        normalize_metric_name(name)
    });

    assert_eq!(first_label.as_ref(), normalize_label_name("pod.name"));
    assert_eq!(second_label, first_label);
    assert_eq!(first_metric.as_ref(), normalize_metric_name("cpu.usage"));
    assert_eq!(second_metric, first_metric);
    assert_eq!(label_normalizations, 1);
    assert_eq!(metric_normalizations, 1);
}

#[test]
fn normalized_name_cache_falls_back_to_uncached_normalization_after_cap() {
    let mut cache = NormalizedNameCache::with_max_entries(1);
    let mut source_symbols = crate::labels::DefaultSymbolTable::default();
    let first_id = source_symbols.intern("pod.name").unwrap();
    let second_id = source_symbols.intern("container.name").unwrap();
    let mut normalizations = 0usize;

    cache.label_name(first_id, "pod.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(first_id, "pod.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(second_id, "container.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(second_id, "container.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });

    assert_eq!(normalizations, 3);
}

#[test]
fn label_visitor_encoder_keeps_first_metric_name_and_sorts_labels() {
    let mut symbols = SegmentSymbols::default();

    let entry = encode_label_visitor_metadata(&mut symbols, |visit| {
        visit("z.label", "last");
        visit(METRIC_NAME_LABEL, "cpu.first");
        visit("a.label", "first");
        visit(METRIC_NAME_LABEL, "cpu.second");
    });

    assert_eq!(
        resolved_entry_labels(&symbols, &entry),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.first")
            ),
            (normalize_label_name("a.label"), "first".to_string()),
            (normalize_label_name("z.label"), "last".to_string()),
        ]
    );
}

#[test]
fn label_value_time_ranges_update_from_encoded_series_entry() {
    let mut index = LabelValueTimeRangeIndex::default();
    let entry = SeriesEntry {
        series_id: 7,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(1, 10), (2, 20)],
    };
    let first_chunk = ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms: 1_000,
        max_time_ms: 2_000,
        offset: 0,
        length: 1,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    };
    let second_chunk = ChunkIndexEntry {
        min_time_ms: 500,
        max_time_ms: 4_000,
        ..first_chunk.clone()
    };

    update_label_value_time_ranges(&mut index, &entry, &first_chunk);
    update_label_value_time_ranges(&mut index, &entry, &second_chunk);

    assert_eq!(
        index.get(1, 10),
        Some(LabelValueTimeRange {
            min_time_ms: 500,
            max_time_ms: 4_000,
        })
    );
    assert_eq!(
        index.get(2, 20),
        Some(LabelValueTimeRange {
            min_time_ms: 500,
            max_time_ms: 4_000,
        })
    );
}

#[test]
fn segment_writer_rotates_on_new_window() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer
        .record_sample(SeriesRef::new(2), 25_000, 2.5)
        .unwrap();
    writer.flush().unwrap();

    let segments: Vec<_> = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .collect();
    assert_eq!(segments.len(), 2);
}

#[test]
fn segment_writer_batches_samples_per_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples(
            SeriesRef::new(5),
            &[(1_000, 1.0), (2_000, 2.0), (1_500, 1.5)],
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = open_schema6_segment_for_test(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 3);
    assert_eq!(reader.meta().series, 1);
    let entries = reader.read_chunk_index().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 1);
}

#[test]
fn segment_writer_records_ordered_samples_with_label_visitor() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(5),
            &[(1_000, 1.0), (1_500, 1.5), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu.usage");
                visit("pod.name", "backend-1");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 3);
    assert_eq!(reader.meta().series, 1);

    let mut chunk_reader = ChunkReader::new(reader.open_chunks().unwrap());
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(
        record.samples,
        ChunkSamples::Float(vec![(1_000, 1.0), (1_500, 1.5), (2_000, 2.0)])
    );
}

#[test]
fn segment_writer_ordered_samples_reject_unsorted_input() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    let err = writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(5),
            &[(2_000, 2.0), (1_000, 1.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu.usage");
                visit("pod.name", "backend-1");
            },
        )
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn segment_writer_writes_int_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_i64(SeriesRef::new(11), &[(1_000, 5), (2_000, -1)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    let chunk_file = reader.open_chunks().unwrap();
    let mut chunk_reader = ChunkReader::new(chunk_file);
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(record.kind, ChunkKind::Int64);
    assert_eq!(
        record.samples,
        ChunkSamples::Int64(vec![(1_000, 5), (2_000, -1)])
    );
}

#[test]
fn segment_writer_writes_typed_otlp_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();

    let histogram = HistogramValue {
        count: 4,
        sum: Some(10.0),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 5.0],
        bucket_counts: vec![1, 2, 1],
    };
    let exphist = ExponentialHistogramValue {
        count: 6,
        sum: Some(15.0),
        min: Some(1.0),
        max: Some(8.0),
        scale: 2,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![0],
        },
    };
    let summary = SummaryValue {
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
    };

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(21),
            &[(1_000, histogram.clone())],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(22),
            &[(2_000, exphist.clone())],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.size");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(23),
            &[(3_000, summary.clone())],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.latency");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = open_schema6_segment_for_test(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 3);
    assert_eq!(reader.meta().series, 3);

    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).expect("open series"))
            .unwrap();
    assert_eq!(
        series[0].kind_mask & SERIES_KIND_HISTOGRAM,
        SERIES_KIND_HISTOGRAM
    );
    assert_eq!(
        series[1].kind_mask & SERIES_KIND_SUMMARY,
        SERIES_KIND_SUMMARY
    );
    assert_eq!(
        series[2].kind_mask & SERIES_KIND_EXPONENTIAL_HISTOGRAM,
        SERIES_KIND_EXPONENTIAL_HISTOGRAM
    );

    let chunk_entries = reader.read_chunk_index().unwrap();
    assert_eq!(chunk_entries[0][0].kind, ChunkKind::Histogram);
    assert_eq!(chunk_entries[1][0].kind, ChunkKind::Summary);
    assert_eq!(chunk_entries[2][0].kind, ChunkKind::ExponentialHistogram);

    let mut chunks = reader.open_chunks().unwrap();
    let indexed_chunks: Vec<_> = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            read_chunk_record_at(&mut chunks, entries[0].offset, entries[0].length).unwrap()
        })
        .collect();
    assert_eq!(
        indexed_chunks[0].samples,
        ChunkSamples::Histogram(vec![(1_000, histogram)])
    );
    assert_eq!(indexed_chunks[0].series_ref, 0);
    assert_eq!(
        indexed_chunks[1].samples,
        ChunkSamples::Summary(vec![(3_000, summary)])
    );
    assert_eq!(indexed_chunks[1].series_ref, 1);
    assert_eq!(
        indexed_chunks[2].samples,
        ChunkSamples::ExponentialHistogram(vec![(2_000, exphist)])
    );
    assert_eq!(indexed_chunks[2].series_ref, 2);
}

#[test]
fn promql_count_projection_verifies_v7_candidates_before_kind_materialization() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();

    for idx in 0..10u32 {
        let series_label = format!("float-{idx}");
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(idx + 1),
                &[(1_000, f64::from(idx))],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed.metric");
                    visit("series", &series_label);
                },
            )
            .unwrap();
    }

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(100),
            &[(
                1_000,
                HistogramValue {
                    count: 2,
                    sum: Some(3.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "mixed.metric");
                visit("series", "histogram");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("mixed.metric"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(1_000, 2.0)]);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(
        profile.series_entries_read, 0,
        "schema-7 facade verification is charged to governed metadata reads"
    );
}

#[test]
fn promql_scalar_projection_reuses_projected_labels_across_queries() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                1_000,
                HistogramValue {
                    count: 2,
                    sum: Some(3.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "cache.metric");
                visit("series", "histogram");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_schema6_store_for_test(tempdir.path()).unwrap();
    let mut query_session = store.query_session().unwrap();
    let query = format!("{}_count", normalize_metric_name("cache.metric"));

    let first = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.results[0].samples, vec![(1_000, 2.0)]);
    assert_eq!(query_session.projected_label_cache.entries.len(), 1);
    assert_eq!(query_session.projected_label_cache.misses, 1);
    assert_eq!(query_session.projected_label_cache.hits, 0);

    let second = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();
    assert_eq!(second.results, first.results);
    assert_eq!(query_session.projected_label_cache.entries.len(), 1);
    assert_eq!(query_session.projected_label_cache.misses, 1);
    assert_eq!(query_session.projected_label_cache.hits, 1);
}

#[test]
fn promql_scalar_projection_verifies_v7_candidates_before_kind_materialization() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 42.0)], |visit| {
            visit(METRIC_NAME_LABEL, "mixed_scalar_count");
            visit("series", "float");
        })
        .unwrap();

    for idx in 0..10u32 {
        let series_label = format!("histogram-{idx}");
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(100 + idx),
                &[(
                    1_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(3.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_scalar_count");
                    visit("series", &series_label);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!(
        "{{{}=\"{}\"}}",
        METRIC_NAME_LABEL,
        normalize_metric_name("mixed_scalar_count")
    );
    let execution = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(1_000, 42.0)]);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(
        profile.series_entries_read, 0,
        "schema-7 facade verification is charged to governed metadata reads"
    );
}

#[test]
fn promql_projection_reuses_labels_for_same_series_across_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(7),
            &[(
                1_000,
                HistogramValue {
                    count: 2,
                    sum: Some(3.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_duration");
                visit("route", "/shared");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(7),
            &[(
                11_000,
                HistogramValue {
                    count: 3,
                    sum: Some(4.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 2],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_duration");
                visit("route", "/shared");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].samples,
        vec![(1_000, 2.0), (11_000, 3.0)]
    );
    let profile = query_session.profile().delta_since(before);
    assert_eq!(
        profile.series_entries_read, 0,
        "schema-7 facade verification is charged to governed metadata reads"
    );
}

#[test]
fn promql_projection_batches_label_materialization_for_segment_misses() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();

    for (series_ref, route, count) in [
        (SeriesRef::new(7), "/alpha", 2),
        (SeriesRef::new(8), "/beta", 3),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[(
                    1_000,
                    HistogramValue {
                        count,
                        sum: Some(count as f64),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, count - 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request_duration");
                    visit("route", route);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 2);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(profile.series_entries_read, 0);
    assert_eq!(profile.series_entry_read_batches, 0);
}

#[test]
fn promql_projection_reuses_verified_series_entries_for_label_materialization() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();

    for (series_ref, route, count) in [
        (SeriesRef::new(7), "/alpha", 2),
        (SeriesRef::new(8), "/beta", 3),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[(
                    1_000,
                    HistogramValue {
                        count,
                        sum: Some(count as f64),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, count - 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request_duration");
                    visit("route", route);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let schema7_options = SegmentStoreOpenOptions {
        storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
        ..SegmentStoreOpenOptions::default()
    };
    let metadata_runtime = open_metadata_runtime(schema7_options.metadata_governor).unwrap();
    SegmentReader::open_with_options(&seg_dir, schema7_options, metadata_runtime).unwrap();

    let store = SegmentStoreReader::open_with_options(tempdir.path(), schema7_options).unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 2);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(profile.series_entries_read, 0);
    assert_eq!(profile.series_entry_read_batches, 0);
    assert_eq!(profile.series_entry_bytes, 0);
}

#[test]
fn segment_writer_writes_raw_float_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_raw(SeriesRef::new(12), &[(1_000, 1.0), (2_000, 2.0)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    let mut chunk_file = reader.open_chunks().unwrap();
    let encoding = read_chunk_encoding(&mut chunk_file);
    assert_eq!(encoding, ChunkEncoding::RawF64 as u8);
    chunk_file.seek(SeekFrom::Start(0)).unwrap();

    let mut chunk_reader = ChunkReader::new(chunk_file);
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(record.kind, ChunkKind::Float);
    assert_eq!(
        record.samples,
        ChunkSamples::Float(vec![(1_000, 1.0), (2_000, 2.0)])
    );
}

#[test]
fn segment_writer_writes_raw_int_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_i64_raw(SeriesRef::new(13), &[(1_000, 5), (2_000, -1)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    let mut chunk_file = reader.open_chunks().unwrap();
    let encoding = read_chunk_encoding(&mut chunk_file);
    assert_eq!(encoding, ChunkEncoding::RawI64 as u8);
    chunk_file.seek(SeekFrom::Start(0)).unwrap();

    let mut chunk_reader = ChunkReader::new(chunk_file);
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(record.kind, ChunkKind::Int64);
    assert_eq!(
        record.samples,
        ChunkSamples::Int64(vec![(1_000, 5), (2_000, -1)])
    );
}

#[test]
fn segment_reader_loads_meta() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(7), 5_000, 7.5).unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 1);
    assert_eq!(reader.meta().series, 1);
}

#[test]
fn segment_store_smoke_verifier_counts_kinds_and_runs_promql_readbacks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.0), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu.usage");
                visit("instance", "host-a");
            },
        )
        .unwrap();

    let histogram = HistogramValue {
        count: 4,
        sum: Some(10.0),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 5.0],
        bucket_counts: vec![1, 2, 1],
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(1_000, histogram)],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();

    let exphist = ExponentialHistogramValue {
        count: 6,
        sum: Some(15.0),
        min: Some(1.0),
        max: Some(8.0),
        scale: 2,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![0],
        },
    };
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(3),
            &[(2_000, exphist)],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.size");
                visit("route", "/typed");
            },
        )
        .unwrap();

    let summary = SummaryValue {
        count: 10,
        sum: 50.0,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![SummaryQuantileValue {
            quantile: 0.9,
            value: 8.0,
        }],
    };
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(4),
            &[(3_000, summary)],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.latency");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let report = store.smoke_verify(0, 10_000, 1).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.datapoints, 5);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert_eq!(report.totals.by_kind.histogram.chunks, 1);
    assert_eq!(report.totals.by_kind.exponential_histogram.chunks, 1);
    assert_eq!(report.totals.by_kind.summary.chunks, 1);

    assert!(report.sample_series.iter().any(|series| {
        series.kind == ChunkKind::Float
            && series
                .labels
                .iter()
                .any(|(key, value)| key == "instance" && value == "host-a")
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Float && query.result_samples > 0 && query.samples_decoded > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Histogram
            && query.query.contains("_count")
            && query.result_series > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Histogram
            && query.query.contains("_bucket")
            && query.query.contains(r#"le="1""#)
            && query.result_series > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::ExponentialHistogram
            && query.query.contains("_bucket")
            && query.query.contains(r#"le="+Inf""#)
            && query.result_series > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Summary
            && query.query.contains(r#"quantile="0.9""#)
            && query.result_series > 0
    }));
}
