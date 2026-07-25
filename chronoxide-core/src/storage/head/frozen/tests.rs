use super::*;

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::labels::{DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore};
use crate::storage::live_coverage::{
    CoverageLedger, MessageSequence, RecordedSampleContribution, RecordedSampleOrder,
};
use crate::storage::segment::{LabelMatcher, QueryLimits};

fn test_config() -> HeadConfig {
    HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(10))
    .with_compact_numeric_series(false)
}

fn labels(
    store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    values: &[(&str, &str)],
) -> SeriesRef {
    let refs: Vec<_> = values.iter().copied().map(KeyValueRef::from).collect();
    store.intern(&refs).unwrap()
}

fn histogram(seed: u64) -> HistogramValue {
    HistogramValue {
        count: 3 + seed,
        sum: Some(6.0 + seed as f64),
        min: Some(1.0),
        max: Some(3.0 + seed as f64),
        metadata: TypedSampleMetadata {
            start_time_ms: Some(100),
            flags: 0,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::NotCounterReset,
        },
        explicit_bounds: vec![1.0],
        bucket_counts: vec![1, 2 + seed],
    }
}

fn exponential_histogram(seed: u64) -> ExponentialHistogramValue {
    ExponentialHistogramValue {
        count: 3 + seed,
        sum: Some(7.0 + seed as f64),
        min: Some(-1.0),
        max: Some(4.0),
        scale: 1,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(100),
            flags: 0,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::Unknown,
        },
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![1 + seed],
        },
        negative: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![1],
        },
    }
}

fn summary(seed: u64) -> SummaryValue {
    SummaryValue {
        count: 2 + seed,
        sum: 4.0 + seed as f64,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(100),
            flags: 0,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::GaugeType,
        },
        quantiles: vec![
            SummaryQuantileValue {
                quantile: 0.5,
                value: 2.0 + seed as f64,
            },
            SummaryQuantileValue {
                quantile: 0.9,
                value: 3.0 + seed as f64,
            },
        ],
    }
}

fn recorded_contribution(
    message_sequence: u64,
    sample_ordinal: u64,
    series: SeriesRef,
    timestamp_ms: u64,
    value: &SampleValue,
) -> RecordedSampleContribution {
    RecordedSampleContribution::for_sample(
        RecordedSampleOrder::new(MessageSequence::new(message_sequence), sample_ordinal),
        series,
        timestamp_ms,
        value,
        &mut Vec::new(),
    )
    .unwrap()
}

#[test]
fn tracked_coverage_follows_rotated_active_and_ooo_fragments_exactly() {
    let series = SeriesRef::new(4);
    let mut head = HeadBuffer::new(test_config()).unwrap();
    head.enable_live_coverage_tracking().unwrap();

    let first = SampleValue::Float(1.0);
    let first_contribution = recorded_contribution(1, 0, series, 1_000, &first);
    let first_outcome = head
        .record_sample_with_coverage(series, 1_000, first, first_contribution)
        .unwrap();
    assert!(first_outcome.recorded);
    assert!(first_outcome.completed_window.is_none());

    let next = SampleValue::Float(2.0);
    let next_contribution = recorded_contribution(1, 1, series, 11_000, &next);
    let rotated = head
        .record_sample_with_coverage(series, 11_000, next, next_contribution)
        .unwrap()
        .completed_window
        .unwrap();
    assert!(rotated.coverage_tracking_enabled());
    assert_eq!(rotated.coverage(), first_contribution.ledger());
    let rotated_range = rotated.recorded_order_range().unwrap();
    assert_eq!(rotated_range.first().sample_ordinal(), 0);
    assert_eq!(rotated_range.last().sample_ordinal(), 0);
    let rotated = rotated.try_freeze().unwrap();
    assert_eq!(rotated.coverage(), first_contribution.ledger());
    assert_eq!(rotated.recorded_orders().sample_count(), 1);
    assert_eq!(
        rotated.recorded_orders().runs()[0].first(),
        first_contribution.order()
    );

    let late = SampleValue::Float(3.0);
    let late_contribution = recorded_contribution(2, 0, series, 10_500, &late);
    let late_outcome = head
        .record_sample_with_coverage(series, 10_500, late, late_contribution)
        .unwrap();
    assert!(late_outcome.recorded);
    assert!(late_outcome.completed_window.is_none());

    let fragments = head.try_freeze_for_publication().unwrap();
    assert_eq!(fragments.len(), 2);
    let active = fragments
        .iter()
        .find(|fragment| fragment.lane() == FrozenHeadLane::InOrder)
        .unwrap();
    let ooo = fragments
        .iter()
        .find(|fragment| fragment.lane() == FrozenHeadLane::OutOfOrder)
        .unwrap();
    assert_eq!(active.coverage(), next_contribution.ledger());
    assert_eq!(ooo.coverage(), late_contribution.ledger());
    assert_eq!(active.recorded_orders().sample_count(), 1);
    assert_eq!(
        active.recorded_orders().runs()[0].first(),
        next_contribution.order()
    );
    assert_eq!(ooo.recorded_orders().sample_count(), 1);
    assert_eq!(
        ooo.recorded_orders().runs()[0].first(),
        late_contribution.order()
    );
    assert_eq!(
        active
            .coverage()
            .checked_merge(ooo.coverage())
            .unwrap()
            .checked_merge(rotated.coverage())
            .unwrap(),
        CoverageLedger::empty()
            .checked_with_contribution(first_contribution)
            .unwrap()
            .checked_with_contribution(next_contribution)
            .unwrap()
            .checked_with_contribution(late_contribution)
            .unwrap()
    );
}

#[test]
fn retained_rotation_stays_parked_until_publication() {
    let series = SeriesRef::new(4);
    let mut head = HeadBuffer::new(test_config()).unwrap();
    head.enable_live_coverage_tracking().unwrap();

    let first = SampleValue::Float(1.0);
    let first_contribution = recorded_contribution(1, 0, series, 1_000, &first);
    head.record_sample_with_coverage(series, 1_000, first, first_contribution)
        .unwrap();

    let retained_slot = head.try_reserve_retained_window_for_publication().unwrap();
    let second = SampleValue::Float(2.0);
    let second_contribution = recorded_contribution(1, 1, series, 11_000, &second);
    let rotated = head
        .record_sample_with_coverage(series, 11_000, second, second_contribution)
        .unwrap()
        .completed_window
        .unwrap();
    head.retain_completed_window_for_publication(retained_slot, rotated)
        .unwrap();

    let third = SampleValue::Float(3.0);
    let third_contribution = recorded_contribution(1, 2, series, 12_000, &third);
    let outcome = head
        .record_sample_with_coverage(series, 12_000, third, third_contribution)
        .unwrap();
    assert!(outcome.recorded);
    assert!(
        outcome.completed_window.is_none(),
        "an already-retained rotation must not be returned again"
    );

    let fragments = head.try_freeze_for_publication().unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(
        fragments
            .iter()
            .map(FrozenHeadFragment::coverage)
            .try_fold(CoverageLedger::empty(), CoverageLedger::checked_merge)
            .unwrap(),
        CoverageLedger::empty()
            .checked_with_contribution(first_contribution)
            .unwrap()
            .checked_with_contribution(second_contribution)
            .unwrap()
            .checked_with_contribution(third_contribution)
            .unwrap()
    );
}

#[test]
fn failed_retained_rotation_reservation_preserves_state_and_exact_retry() {
    let series = SeriesRef::new(4);
    let mut head = HeadBuffer::new(test_config()).unwrap();
    head.enable_live_coverage_tracking().unwrap();

    let first = SampleValue::Float(1.0);
    let first_contribution = recorded_contribution(7, 0, series, 1_000, &first);
    head.record_sample_with_coverage(series, 1_000, first, first_contribution)
        .unwrap();

    let baseline_coverage = head.window.as_ref().unwrap().coverage();
    let baseline_orders = head.window.as_ref().unwrap().recorded_orders().clone();
    let baseline_datapoints = head.window.as_ref().unwrap().datapoints;
    let baseline_last_timestamp = head.last_timestamps.get(series);
    let baseline_kind_guards = head.kind_guard_count();

    head.fail_next_retained_window_reservation();
    let error = head
        .try_reserve_retained_window_for_publication()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert!(error.to_string().contains("injected"));
    assert!(head.retained_windows.is_empty());
    assert_eq!(head.window.as_ref().unwrap().coverage(), baseline_coverage);
    assert_eq!(
        head.window.as_ref().unwrap().recorded_orders(),
        &baseline_orders
    );
    assert_eq!(
        head.window.as_ref().unwrap().datapoints,
        baseline_datapoints
    );
    assert_eq!(head.last_timestamps.get(series), baseline_last_timestamp);
    assert_eq!(head.kind_guard_count(), baseline_kind_guards);

    let retained_slot = head.try_reserve_retained_window_for_publication().unwrap();
    let second = SampleValue::Float(2.0);
    let second_contribution = recorded_contribution(7, 1, series, 11_000, &second);
    let rotated = head
        .record_sample_with_coverage(series, 11_000, second, second_contribution)
        .unwrap()
        .completed_window
        .unwrap();
    head.retain_completed_window_for_publication(retained_slot, rotated)
        .unwrap();

    let fragments = head.try_freeze_for_publication().unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(
        fragments
            .iter()
            .map(FrozenHeadFragment::coverage)
            .try_fold(CoverageLedger::empty(), CoverageLedger::checked_merge)
            .unwrap(),
        CoverageLedger::empty()
            .checked_with_contribution(first_contribution)
            .unwrap()
            .checked_with_contribution(second_contribution)
            .unwrap()
    );
    assert_eq!(
        fragments
            .iter()
            .map(FrozenHeadFragment::datapoints)
            .sum::<u64>(),
        2
    );
    let mut decoded = Vec::new();
    for fragment in &fragments {
        let SeriesSamples::Float { samples, .. } = fragment
            .series_kind_samples_in_range(series, SampleKind::Float, 0, 20_000)
            .unwrap()
            .unwrap()
        else {
            panic!("float run decoded as a different sample kind");
        };
        decoded.extend(samples);
    }
    decoded.sort_by_key(|(timestamp_ms, _value)| *timestamp_ms);
    assert_eq!(decoded, vec![(1_000, 1.0), (11_000, 2.0)]);
}

#[test]
fn tracked_kind_rejection_and_order_failure_leave_coverage_and_samples_unchanged() {
    let series = SeriesRef::new(8);
    let mut head = HeadBuffer::new(test_config()).unwrap();
    head.enable_live_coverage_tracking().unwrap();

    let first = SampleValue::Float(1.0);
    let first_contribution = recorded_contribution(4, 0, series, 1_000, &first);
    assert!(
        head.record_sample_with_coverage(series, 1_000, first, first_contribution)
            .unwrap()
            .recorded
    );
    let baseline = head.window.as_ref().unwrap().coverage();

    let mismatch = SampleValue::Int64(7);
    let mismatch_contribution = recorded_contribution(4, 1, series, 1_001, &mismatch);
    let mismatch = head
        .record_sample_with_coverage(series, 1_001, mismatch, mismatch_contribution)
        .unwrap();
    assert!(!mismatch.recorded);
    assert_eq!(head.window.as_ref().unwrap().coverage(), baseline);
    assert_eq!(head.window.as_ref().unwrap().datapoints, 1);

    let repeated_order = SampleValue::Float(2.0);
    let repeated_contribution = recorded_contribution(4, 0, series, 1_002, &repeated_order);
    assert!(
        head.record_sample_with_coverage(series, 1_002, repeated_order, repeated_contribution,)
            .is_err()
    );
    assert_eq!(head.window.as_ref().unwrap().coverage(), baseline);
    assert_eq!(head.window.as_ref().unwrap().datapoints, 1);
}

#[test]
fn tracked_and_untracked_head_modes_cannot_be_mixed() {
    let series = SeriesRef::new(1);
    let mut untracked = HeadBuffer::new(test_config()).unwrap();
    untracked
        .record_sample(series, 1_000, SampleValue::Float(1.0))
        .unwrap();
    assert!(untracked.enable_live_coverage_tracking().is_err());

    let mut tracked = HeadBuffer::new(test_config()).unwrap();
    tracked.enable_live_coverage_tracking().unwrap();
    assert!(
        tracked
            .record_sample(series, 1_000, SampleValue::Float(1.0))
            .is_err()
    );
    assert!(tracked.is_empty());
}

#[test]
fn frozen_fragment_roundtrips_all_five_kinds_in_sorted_runs() {
    let values = [
        (SeriesRef::new(10), SampleValue::Float(1.5)),
        (SeriesRef::new(2), SampleValue::Int64(-7)),
        (SeriesRef::new(7), SampleValue::Histogram(histogram(0))),
        (
            SeriesRef::new(1),
            SampleValue::ExponentialHistogram(exponential_histogram(0)),
        ),
        (SeriesRef::new(5), SampleValue::Summary(summary(0))),
    ];
    let mut head = HeadBuffer::new(test_config()).unwrap();
    for (index, (series, value)) in values.iter().enumerate() {
        let outcome = head
            .record_sample_with_outcome(*series, 1_000 + index as u64, value.clone())
            .unwrap();
        assert!(outcome.recorded);
        assert!(outcome.completed_window.is_none());
    }

    head.window.as_mut().unwrap().seal_all_series();
    let mutable_capacity = head.window.as_ref().unwrap().arena_capacity_bytes();
    assert_eq!(mutable_capacity, DEFAULT_HEAD_ARENA_PAGE_BYTES);
    let fragment = head.drain().unwrap().try_freeze().unwrap();

    let run_keys: Vec<_> = fragment.run_keys().collect();
    assert_eq!(
        run_keys
            .iter()
            .map(|(series, _, _)| *series)
            .collect::<Vec<_>>(),
        vec![
            SeriesRef::new(1),
            SeriesRef::new(2),
            SeriesRef::new(5),
            SeriesRef::new(7),
            SeriesRef::new(10),
        ]
    );
    assert_eq!(
        run_keys
            .iter()
            .map(|(_, kind, _)| *kind)
            .collect::<Vec<_>>(),
        vec![
            SampleKind::ExponentialHistogram,
            SampleKind::Int64,
            SampleKind::Summary,
            SampleKind::Histogram,
            SampleKind::Float,
        ]
    );
    assert_eq!(fragment.datapoints(), 5);
    assert_eq!(fragment.series_len(), 5);
    assert_eq!(
        fragment.arena_allocated_bytes(),
        fragment.arena_used_bytes()
    );
    assert!(fragment.arena_allocated_bytes() < mutable_capacity);

    let decoded = fragment.series_samples_in_range(0, 10_000).unwrap();
    assert_eq!(decoded.len(), 5);
    assert!(matches!(
        decoded[0],
        (series, SeriesSamples::ExponentialHistogram { .. })
            if series == SeriesRef::new(1)
    ));
    assert_eq!(
        decoded[1],
        (
            SeriesRef::new(2),
            SeriesSamples::Int64 {
                encoding: IntEncoding::DeltaZigZag,
                samples: vec![(1_001, -7)],
            },
        )
    );
    assert!(matches!(
        decoded[2],
        (series, SeriesSamples::Summary { .. }) if series == SeriesRef::new(5)
    ));
    assert!(matches!(
        decoded[3],
        (series, SeriesSamples::Histogram { .. }) if series == SeriesRef::new(7)
    ));
    assert_eq!(
        decoded[4],
        (
            SeriesRef::new(10),
            SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: vec![(1_000, 1.5)],
            },
        )
    );
}

#[test]
fn live_head_uses_small_adaptive_page_while_disabled_layout_and_decode_stay_identical() {
    let values = [
        (SeriesRef::new(1), SampleValue::Float(1.5)),
        (SeriesRef::new(2), SampleValue::Int64(-7)),
        (SeriesRef::new(3), SampleValue::Histogram(histogram(0))),
        (
            SeriesRef::new(4),
            SampleValue::ExponentialHistogram(exponential_histogram(0)),
        ),
        (SeriesRef::new(5), SampleValue::Summary(summary(0))),
    ];
    let mut disabled = HeadBuffer::new(test_config()).unwrap();
    let mut live = HeadBuffer::new(test_config()).unwrap();
    live.enable_live_coverage_tracking().unwrap();

    for (ordinal, (series, value)) in values.iter().enumerate() {
        let timestamp_ms = 1_000 + ordinal as u64;
        assert!(
            disabled
                .record_sample_with_outcome(*series, timestamp_ms, value.clone())
                .unwrap()
                .recorded
        );
        let contribution = recorded_contribution(1, ordinal as u64, *series, timestamp_ms, value);
        assert!(
            live.record_sample_with_coverage(*series, timestamp_ms, value.clone(), contribution,)
                .unwrap()
                .recorded
        );
    }

    disabled.window.as_mut().unwrap().seal_all_series();
    live.window.as_mut().unwrap().seal_all_series();
    assert_eq!(
        disabled.window.as_ref().unwrap().arena.page_capacities(),
        vec![DEFAULT_HEAD_ARENA_PAGE_BYTES]
    );
    assert_eq!(
        live.window.as_ref().unwrap().arena.page_capacities(),
        vec![LIVE_HEAD_ARENA_INITIAL_PAGE_BYTES]
    );
    assert_eq!(
        live.window.as_ref().unwrap().arena.next_page_size(),
        LIVE_HEAD_ARENA_INITIAL_PAGE_BYTES * 2
    );

    let disabled = disabled.drain().unwrap().try_freeze().unwrap();
    let live = live.drain().unwrap().try_freeze().unwrap();
    assert_eq!(
        disabled.series_samples_in_range(0, 10_000).unwrap(),
        live.series_samples_in_range(0, 10_000).unwrap()
    );
    assert_eq!(live.arena_allocated_bytes(), live.arena_used_bytes());
    assert!(live.arena_allocated_bytes() < LIVE_HEAD_ARENA_INITIAL_PAGE_BYTES);
}

#[test]
fn empty_freeze_and_failed_freeze_keep_explicit_recoverable_state() {
    let empty = HeadWindow::new(0, 10_000, true).try_freeze().unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.arena_page_count(), 0);
    assert_eq!(empty.arena_allocated_bytes(), 0);

    let mut head = HeadBuffer::new(test_config()).unwrap();
    head.record_sample(SeriesRef::new(3), 1_000, SampleValue::Float(3.0))
        .unwrap();
    let mut window = head.drain().unwrap();
    window.datapoints += 1;
    let error = window.try_freeze().unwrap_err();
    assert_eq!(error.error().kind(), io::ErrorKind::InvalidData);
    let (_source, mut recovered) = error.into_parts();
    assert_eq!(recovered.series_len(), 1);
    assert_eq!(recovered.datapoints, 2);

    recovered.datapoints = 1;
    let fragment = recovered.try_freeze().unwrap();
    assert_eq!(
        fragment.series_samples_in_range(0, 10_000).unwrap(),
        vec![(
            SeriesRef::new(3),
            SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: vec![(1_000, 3.0)],
            },
        )]
    );
}

#[test]
fn live_freeze_pair_write_failures_return_the_complete_window_and_retry_exactly() {
    for failing_write in [1, 2] {
        let series = SeriesRef::new(3);
        let value = SampleValue::Float(3.0);
        let contribution = recorded_contribution(9, 0, series, 1_000, &value);
        let mut head = HeadBuffer::new(test_config()).unwrap();
        head.enable_live_coverage_tracking().unwrap();
        assert!(
            head.record_sample_with_coverage(series, 1_000, value, contribution)
                .unwrap()
                .recorded
        );
        let mut window = head.window.take().unwrap();
        window.arena.fail_pair_write_on_call(failing_write);

        let error = window
            .try_freeze()
            .expect_err("injected live page allocation must preserve the window");
        assert_eq!(error.error().kind(), io::ErrorKind::OutOfMemory);
        let (_source, recovered) = error.into_parts();
        assert_eq!(recovered.datapoints, 1);
        assert_eq!(recovered.coverage(), contribution.ledger());
        assert_eq!(recovered.recorded_orders().sample_count(), 1);
        assert_eq!(recovered.arena.page_count(), 0);
        assert_eq!(recovered.arena.total_used_bytes(), 0);

        let fragment = recovered.try_freeze().unwrap();
        assert_eq!(fragment.coverage(), contribution.ledger());
        assert_eq!(
            fragment
                .series_kind_samples_in_range(series, SampleKind::Float, 0, 10_000)
                .unwrap(),
            Some(SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: vec![(1_000, 3.0)],
            })
        );
    }
}

#[test]
fn buffer_extraction_error_owns_completed_fragments_and_restores_failed_window() {
    let mut head = HeadBuffer::new(test_config()).unwrap();
    let series = SeriesRef::new(33);
    head.record_sample(series, 5_000, SampleValue::Float(5.0))
        .unwrap();
    head.record_sample(series, 4_000, SampleValue::Float(4.0))
        .unwrap();
    head.ooo_windows.get_mut(&(0, 10_000)).unwrap().datapoints += 1;

    let error = head.try_freeze_for_publication().unwrap_err();
    assert_eq!(error.error().kind(), io::ErrorKind::InvalidData);
    let (_source, mut completed) = error.into_parts();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].lane(), FrozenHeadLane::InOrder);
    assert_eq!(head.window.as_ref().unwrap().datapoints, 0);
    assert_eq!(head.ooo_windows[&(0, 10_000)].datapoints, 2);

    head.ooo_windows.get_mut(&(0, 10_000)).unwrap().datapoints = 1;
    let retry = head.try_freeze_for_publication().unwrap();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].lane(), FrozenHeadLane::OutOfOrder);
    completed.extend(retry);

    let view = FrozenHeadReadView::from_owned(completed);
    let mut decoded = view
        .sample_store()
        .ordered_runs(0, 9_999)
        .unwrap()
        .into_iter()
        .map(|run| {
            (
                run.key().series(),
                run.fragment()
                    .series_kind_samples_in_range(run.key().series(), run.key().kind(), 0, 10_000)
                    .unwrap()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    decoded.sort_by_key(|(_, samples)| match samples {
        SeriesSamples::Float { samples, .. } => samples[0].0,
        _ => unreachable!("test records only float samples"),
    });
    assert_eq!(
        decoded,
        vec![
            (
                series,
                SeriesSamples::Float {
                    encoding: FloatEncoding::Gorilla,
                    samples: vec![(4_000, 4.0)],
                },
            ),
            (
                series,
                SeriesSamples::Float {
                    encoding: FloatEncoding::Gorilla,
                    samples: vec![(5_000, 5.0)],
                },
            ),
        ]
    );
}

#[test]
fn failed_first_encode_creates_neither_fragment_nor_kind_guard() {
    let mut head = HeadBuffer::new(test_config()).unwrap();
    let series = SeriesRef::new(4);
    let invalid = HistogramValue {
        count: 4,
        bucket_counts: vec![1, 2],
        ..histogram(0)
    };

    assert_eq!(
        head.record_sample(series, 1_000, SampleValue::Histogram(invalid))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(head.window.is_none());
    assert!(head.try_freeze_for_publication().unwrap().is_empty());
    assert_eq!(head.kind_guard_count(), 0);

    let outcome = head
        .record_sample_with_outcome(series, 1_000, SampleValue::Float(4.0))
        .unwrap();
    assert!(outcome.recorded);
    let fragments = head.try_freeze_for_publication().unwrap();
    assert_eq!(fragments.len(), 1);
    assert_eq!(head.kind_guard_count(), 1);
}

#[test]
fn extraction_preserves_last_timestamp_and_kind_until_explicit_retirement() {
    let mut head = HeadBuffer::new(test_config()).unwrap();
    let series = SeriesRef::new(8);
    assert_eq!(head.publication_fragment_count(), 0);
    assert!(
        head.record_sample_with_outcome(series, 1_000, SampleValue::Float(1.0))
            .unwrap()
            .recorded
    );
    assert_eq!(head.publication_fragment_count(), 1);
    let fragments = head.try_freeze_for_publication().unwrap();
    assert_eq!(fragments.len(), 1);
    assert_eq!(head.publication_fragment_count(), 0);
    assert_eq!(head.last_timestamps.get(series), Some(1_000));
    assert_eq!(head.kind_guard_count(), 1);
    assert_eq!(head.window.as_ref().unwrap().datapoints, 0);
    assert!(head.window.as_ref().unwrap().series.is_empty());

    let mismatch = head
        .record_sample_with_outcome(series, 2_000, SampleValue::Int64(2))
        .unwrap();
    assert!(!mismatch.recorded);
    assert_eq!(head.last_timestamps.get(series), Some(1_000));
    assert_eq!(head.window.as_ref().unwrap().datapoints, 0);

    assert!(
        head.record_sample_with_outcome(series, 2_000, SampleValue::Float(2.0))
            .unwrap()
            .recorded
    );
    assert_eq!(
        head.validate_kind_guard_retirement(0, 10_000, FrozenHeadLane::InOrder)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(head.kind_guard_count(), 1);
    assert_eq!(
        head.retire_kind_guards(0, 10_000, FrozenHeadLane::InOrder)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let second = head.try_freeze_for_publication().unwrap();
    assert_eq!(second.len(), 1);
    head.validate_kind_guard_retirement(0, 10_000, FrozenHeadLane::InOrder)
        .unwrap();
    assert_eq!(head.kind_guard_count(), 1);
    assert_eq!(
        head.retire_kind_guards(0, 10_000, FrozenHeadLane::InOrder)
            .unwrap(),
        1
    );

    assert!(
        head.record_sample_with_outcome(series, 3_000, SampleValue::Int64(3))
            .unwrap()
            .recorded
    );
}

#[test]
fn active_and_ooo_extraction_swap_empty_tails_and_preserve_precedence() {
    let mut store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut store,
        &[(METRIC_NAME_LABEL, "temperature"), ("sensor", "a")],
    );
    let mut head = HeadBuffer::new(test_config()).unwrap();
    head.record_sample(series, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series, 6_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(series, 5_000, SampleValue::Float(3.0))
        .unwrap();
    let stats_before = head.last_timestamp_table_stats();

    let fragments = head.try_freeze_for_publication().unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(
        fragments
            .iter()
            .map(FrozenHeadFragment::lane)
            .collect::<Vec<_>>(),
        vec![FrozenHeadLane::InOrder, FrozenHeadLane::OutOfOrder]
    );
    assert_eq!(head.window.as_ref().unwrap().datapoints, 0);
    assert_eq!(head.ooo_windows[&(0, 10_000)].datapoints, 0);
    assert_eq!(head.last_timestamp_table_stats(), stats_before);
    assert_eq!(head.last_timestamps.get(series), Some(6_000));

    let view = FrozenHeadReadView::from_owned(fragments);
    let results = view
        .query_selector(&store, &SegmentSelector::metric("temperature"), 0, 9_999)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 3.0), (6_000, 2.0)]);
}

#[test]
fn frozen_multi_fragment_queries_match_unfrozen_selector_native_and_metadata_paths() {
    let mut store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let float = labels(&mut store, &[(METRIC_NAME_LABEL, "cpu"), ("host", "a")]);
    let int = labels(
        &mut store,
        &[(METRIC_NAME_LABEL, "requests"), ("host", "a")],
    );
    let hist = labels(&mut store, &[(METRIC_NAME_LABEL, "latency"), ("host", "a")]);
    let exp = labels(&mut store, &[(METRIC_NAME_LABEL, "sizes"), ("host", "b")]);
    let sum = labels(
        &mut store,
        &[(METRIC_NAME_LABEL, "quantiles"), ("host", "b")],
    );

    let mut reference = HeadBuffer::new(test_config()).unwrap();
    let mut publishing = HeadBuffer::new(test_config()).unwrap();
    let mut fragments = Vec::new();
    let batches = [
        vec![
            (float, 1_000, SampleValue::Float(1.0)),
            (int, 1_000, SampleValue::Int64(10)),
            (hist, 1_000, SampleValue::Histogram(histogram(0))),
            (
                exp,
                1_000,
                SampleValue::ExponentialHistogram(exponential_histogram(0)),
            ),
        ],
        vec![
            (float, 2_000, SampleValue::Float(2.0)),
            (int, 2_000, SampleValue::Int64(20)),
            (hist, 2_000, SampleValue::Histogram(histogram(1))),
            (
                exp,
                2_000,
                SampleValue::ExponentialHistogram(exponential_histogram(1)),
            ),
            (sum, 2_000, SampleValue::Summary(summary(0))),
        ],
        vec![
            (float, 1_500, SampleValue::Float(9.0)),
            (hist, 1_500, SampleValue::Histogram(histogram(2))),
            (sum, 3_000, SampleValue::Summary(summary(1))),
        ],
    ];
    for batch in batches {
        for (series, timestamp_ms, value) in batch {
            reference
                .record_sample(series, timestamp_ms, value.clone())
                .unwrap();
            publishing
                .record_sample(series, timestamp_ms, value)
                .unwrap();
        }
        fragments.extend(publishing.try_freeze_for_publication().unwrap());
    }
    let view = FrozenHeadReadView::from_owned(fragments);

    let selector =
        SegmentSelector::new(vec![LabelMatcher::regex(METRIC_NAME_LABEL, "cpu|requests")]);
    assert_eq!(
        view.query_selector(&store, &selector, 0, 9_999).unwrap(),
        reference
            .query_selector(&store, &selector, 0, 9_999)
            .unwrap()
    );

    let histogram_selector = SegmentSelector::metric("latency");
    let mut reference_budget = QueryBudget::new(QueryLimits::unlimited());
    let mut frozen_budget = QueryBudget::new(QueryLimits::unlimited());
    assert_eq!(
        view.query_native_histogram_with_budget(
            &store,
            &histogram_selector,
            0,
            9_999,
            &mut frozen_budget,
        )
        .unwrap(),
        reference
            .query_native_histogram_with_budget(
                &store,
                &histogram_selector,
                0,
                9_999,
                &mut reference_budget,
            )
            .unwrap()
    );

    let exponential_selector = SegmentSelector::metric("sizes");
    let mut reference_budget = QueryBudget::new(QueryLimits::unlimited());
    let mut frozen_budget = QueryBudget::new(QueryLimits::unlimited());
    assert_eq!(
        view.query_native_exponential_histogram_with_budget(
            &store,
            &exponential_selector,
            0,
            9_999,
            &mut frozen_budget,
        )
        .unwrap(),
        reference
            .query_native_exponential_histogram_with_budget(
                &store,
                &exponential_selector,
                0,
                9_999,
                &mut reference_budget,
            )
            .unwrap()
    );

    assert_eq!(
        view.metric_names(&store, 0, 9_999).unwrap(),
        reference.metric_names(&store, 0, 9_999).unwrap()
    );
    assert_eq!(
        view.label_names(&store, 0, 9_999).unwrap(),
        reference.label_names(&store, 0, 9_999).unwrap()
    );
    assert_eq!(
        view.label_values(&store, "host", 0, 9_999).unwrap(),
        reference.label_values(&store, "host", 0, 9_999).unwrap()
    );
}

#[test]
fn immutable_frozen_view_supports_deterministic_multithreaded_reads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FrozenHeadFragment>();
    assert_send_sync::<FrozenHeadReadView>();

    let mut store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut store,
        &[(METRIC_NAME_LABEL, "concurrent"), ("worker", "one")],
    );
    let mut head = HeadBuffer::new(test_config()).unwrap();
    let mut fragments = Vec::new();
    for publication in 0..4 {
        head.record_sample(
            series,
            1_000 + publication,
            SampleValue::Float(publication as f64),
        )
        .unwrap();
        fragments.extend(head.try_freeze_for_publication().unwrap());
    }

    let view = Arc::new(FrozenHeadReadView::from_owned(fragments));
    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(9));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let view = Arc::clone(&view);
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..200 {
                    let result = view
                        .query_selector(
                            store.as_ref(),
                            &SegmentSelector::metric("concurrent"),
                            0,
                            9_999,
                        )
                        .unwrap();
                    assert_eq!(result.len(), 1);
                    assert_eq!(
                        result[0].samples,
                        vec![(1_000, 0.0), (1_001, 1.0), (1_002, 2.0), (1_003, 3.0),]
                    );
                }
            })
        })
        .collect();
    barrier.wait();
    for reader in threads {
        reader.join().unwrap();
    }
}
