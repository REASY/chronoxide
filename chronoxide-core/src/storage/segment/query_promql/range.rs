use super::*;

pub(in crate::storage::segment) const DEFAULT_INSTANT_LOOKBACK_MS: u64 = 5 * 60 * 1_000;

type RangeFunctionScalarSamples<'a> = (
    &'a [(u64, f64)],
    Option<&'a [CounterResetHint]>,
    Option<&'a [Option<u64>]>,
);

type CounterSamplesAfterStale<'a> = (&'a [(u64, f64)], Option<&'a [CounterResetHint]>, u64);

pub(in crate::storage::segment) fn instant_vector_start_ms(end_ms: u64) -> u64 {
    end_ms.saturating_sub(DEFAULT_INSTANT_LOOKBACK_MS)
}

pub(in crate::storage::segment) fn range_function_start_ms(end_ms: u64, range_ms: u64) -> u64 {
    end_ms.saturating_sub(range_ms)
}

pub(super) fn range_function_start_before_epoch_ms(end_ms: u64, range_ms: u64) -> u64 {
    range_ms.saturating_sub(end_ms)
}

pub(in crate::storage::segment) fn range_selector_read_start_ms(
    selectors: &[SegmentSelector],
    range_start_ms: u64,
    end_ms: u64,
) -> u64 {
    if selectors
        .iter()
        .any(|selector| selector.projection.needs_delta_projection_seed())
    {
        instant_vector_start_ms(end_ms).min(range_start_ms)
    } else {
        range_start_ms
    }
}

pub(in crate::storage::segment) fn evaluate_range_function(
    function: &PromqlRangeFunction,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let labels_complete = result.labels_are_complete();
        let metric_name_dropped_series_id = result.metric_name_dropped_series_id;
        let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
        let range_start_before_epoch_ms =
            range_function_start_before_epoch_ms(eval_time_ms, function.range_ms);
        let include_range_start = range_start_before_epoch_ms > 0
            && matches!(
                function.kind,
                PromqlRangeFunctionKind::Rate | PromqlRangeFunctionKind::Increase
            );
        let include_delta_projection_seed = result.temporality == QueryResultTemporality::Delta
            && matches!(
                function.kind,
                PromqlRangeFunctionKind::Rate | PromqlRangeFunctionKind::Increase
            );
        let (samples, counter_reset_hints, sample_start_times) = range_function_scalar_samples(
            &result.samples,
            result.counter_reset_hints(),
            result.sample_start_times(),
            range_start_ms,
            eval_time_ms,
            include_range_start,
            include_delta_projection_seed,
        );
        let value = match function.kind {
            PromqlRangeFunctionKind::Increase => match result.temporality {
                QueryResultTemporality::Delta => extrapolated_delta_projection_increase(
                    samples,
                    counter_reset_hints,
                    sample_start_times,
                    range_start_ms,
                    include_range_start,
                    eval_time_ms,
                ),
                QueryResultTemporality::Mixed => None,
                QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                    extrapolated_counter_increase(
                        samples,
                        counter_reset_hints,
                        false,
                        range_start_ms,
                        range_start_before_epoch_ms,
                        eval_time_ms,
                    )
                }
            },
            PromqlRangeFunctionKind::Rate => {
                let increase = match result.temporality {
                    QueryResultTemporality::Delta => extrapolated_delta_projection_increase(
                        samples,
                        counter_reset_hints,
                        sample_start_times,
                        range_start_ms,
                        include_range_start,
                        eval_time_ms,
                    ),
                    QueryResultTemporality::Mixed => None,
                    QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                        extrapolated_counter_increase(
                            samples,
                            counter_reset_hints,
                            false,
                            range_start_ms,
                            range_start_before_epoch_ms,
                            eval_time_ms,
                        )
                    }
                };
                if function.range_ms == 0 {
                    None
                } else {
                    increase.map(|increase| increase / (function.range_ms as f64 / 1_000.0))
                }
            }
            PromqlRangeFunctionKind::Delta => match result.temporality {
                QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                    extrapolated_gauge_delta(samples, range_start_ms, eval_time_ms)
                }
                QueryResultTemporality::Delta | QueryResultTemporality::Mixed => None,
            },
            PromqlRangeFunctionKind::Irate => match result.temporality {
                QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                    instant_counter_rate(samples, counter_reset_hints)
                }
                QueryResultTemporality::Delta | QueryResultTemporality::Mixed => None,
            },
            PromqlRangeFunctionKind::Idelta => match result.temporality {
                QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                    instant_gauge_delta(samples)
                }
                QueryResultTemporality::Delta | QueryResultTemporality::Mixed => None,
            },
            PromqlRangeFunctionKind::Changes => changes_over_time(samples),
            PromqlRangeFunctionKind::Resets => resets_over_time(samples, counter_reset_hints),
            PromqlRangeFunctionKind::LastOverTime => {
                if result.temporality == QueryResultTemporality::Delta {
                    stitch_delta_projection_fragments_preserving_stale(
                        &result.samples,
                        result.counter_reset_hints(),
                    )
                    .and_then(|stitched| {
                        let (samples, _, _) = range_function_scalar_samples(
                            &stitched,
                            None,
                            None,
                            range_start_ms,
                            eval_time_ms,
                            false,
                            false,
                        );
                        last_over_time(samples)
                    })
                } else {
                    last_over_time(samples)
                }
            }
            PromqlRangeFunctionKind::CountOverTime => count_over_time(samples),
            PromqlRangeFunctionKind::PresentOverTime => present_over_time(samples),
            PromqlRangeFunctionKind::SumOverTime => sum_over_time(samples),
            PromqlRangeFunctionKind::AvgOverTime => avg_over_time(samples),
            PromqlRangeFunctionKind::StddevOverTime => {
                stdvar_over_time(samples).map(|value| value.sqrt())
            }
            PromqlRangeFunctionKind::StdvarOverTime => stdvar_over_time(samples),
            PromqlRangeFunctionKind::MinOverTime => min_over_time(samples),
            PromqlRangeFunctionKind::MaxOverTime => max_over_time(samples),
            PromqlRangeFunctionKind::Deriv => deriv(samples, eval_time_ms),
        };
        let Some(value) = value else {
            continue;
        };
        if !range_function_allows_non_finite_output(function.kind) && !value.is_finite() {
            continue;
        }
        let mut evaluated = if function.kind == PromqlRangeFunctionKind::LastOverTime {
            let series_id = if labels_complete {
                result.series_id
            } else {
                metric_name_dropped_series_id
                    .expect("partial range input requires a metric-name-dropped identity")
            };
            SegmentQueryResult::with_shared_labels(series_id, result.labels)
        } else {
            let labels = function_result_labels(&result.labels);
            let series_id = if labels_complete {
                segment_series_id(&labels)
            } else {
                metric_name_dropped_series_id
                    .expect("partial range input requires a metric-name-dropped identity")
            };
            SegmentQueryResult::new(series_id, labels)
        };
        let series_id = evaluated.series_id;
        if !labels_complete {
            evaluated.mark_labels_incomplete(Some(series_id));
        }
        evaluated.push_sample(eval_time_ms, value);
        out.push(evaluated);
    }
    merge_query_results(out)
}

pub(in crate::storage::segment) fn evaluate_quantile_over_time(
    function: &PromqlQuantileOverTime,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_parameterized_range_function(results, eval_time_ms, function.range_ms, |samples| {
        quantile_over_time(function.quantile, samples)
    })
}

pub(in crate::storage::segment) fn evaluate_predict_linear(
    function: &PromqlPredictLinear,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_parameterized_range_function(results, eval_time_ms, function.range_ms, |samples| {
        predict_linear(samples, eval_time_ms, function.seconds)
    })
}

pub(in crate::storage::segment) fn evaluate_double_exponential_smoothing(
    function: &PromqlDoubleExponentialSmoothing,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_parameterized_range_function(results, eval_time_ms, function.range_ms, |samples| {
        double_exponential_smoothing(samples, function.smoothing_factor, function.trend_factor)
    })
}

fn evaluate_parameterized_range_function(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    range_ms: u64,
    f: impl Fn(&[(u64, f64)]) -> Option<f64>,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let range_start_ms = range_function_start_ms(eval_time_ms, range_ms);
        let (samples, _, _) = range_function_scalar_samples(
            &result.samples,
            None,
            None,
            range_start_ms,
            eval_time_ms,
            false,
            false,
        );
        let Some(value) = f(samples) else {
            continue;
        };
        let labels = function_result_labels(&result.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn range_function_scalar_samples<'a>(
    samples: &'a [(u64, f64)],
    counter_reset_hints: Option<&'a [CounterResetHint]>,
    sample_start_times: Option<&'a [Option<u64>]>,
    range_start_ms: u64,
    range_end_ms: u64,
    include_range_start: bool,
    include_predecessor: bool,
) -> RangeFunctionScalarSamples<'a> {
    let original_len = samples.len();
    let selected_start_idx = samples.partition_point(|(timestamp_ms, _)| {
        if include_range_start {
            *timestamp_ms < range_start_ms
        } else {
            *timestamp_ms <= range_start_ms
        }
    });
    let start_idx = if include_predecessor && selected_start_idx > 0 {
        selected_start_idx - 1
    } else {
        selected_start_idx
    };
    let end_idx = start_idx
        + samples[start_idx..].partition_point(|(timestamp_ms, _)| *timestamp_ms <= range_end_ms);

    let counter_reset_hints = counter_reset_hints
        .filter(|hints| hints.len() == original_len)
        .map(|hints| &hints[start_idx..end_idx]);
    let sample_start_times = sample_start_times
        .filter(|start_times| start_times.len() == original_len)
        .map(|start_times| &start_times[start_idx..end_idx]);

    (
        &samples[start_idx..end_idx],
        counter_reset_hints,
        sample_start_times,
    )
}

pub(super) struct RateIncreaseScalarSamples<'a> {
    pub(super) samples: Cow<'a, [(u64, f64)]>,
    pub(super) counter_reset_hints: Option<Cow<'a, [CounterResetHint]>>,
}

pub(super) fn rate_increase_scalar_samples<'a>(
    samples: &'a [(u64, f64)],
    counter_reset_hints: Option<&'a [CounterResetHint]>,
    force_unknown_after_stale: bool,
) -> RateIncreaseScalarSamples<'a> {
    let counter_reset_hints = counter_reset_hints.filter(|hints| hints.len() == samples.len());
    if !samples
        .iter()
        .any(|(_, value)| is_prometheus_stale_marker(*value))
    {
        return RateIncreaseScalarSamples {
            samples: Cow::Borrowed(samples),
            counter_reset_hints: counter_reset_hints.map(Cow::Borrowed),
        };
    }

    let mut retained_samples = Vec::with_capacity(samples.len());
    let mut retained_counter_reset_hints =
        counter_reset_hints.map(|_| Vec::with_capacity(samples.len()));
    let mut detect_reset_at_fragment_start = false;

    for (idx, &(timestamp_ms, value)) in samples.iter().enumerate() {
        if is_prometheus_stale_marker(value) {
            detect_reset_at_fragment_start = true;
            continue;
        }
        retained_samples.push((timestamp_ms, value));
        if let (Some(hints), Some(retained_hints)) =
            (counter_reset_hints, retained_counter_reset_hints.as_mut())
        {
            retained_hints.push(
                if detect_reset_at_fragment_start && force_unknown_after_stale {
                    CounterResetHint::Unknown
                } else {
                    hints[idx]
                },
            );
        }
        detect_reset_at_fragment_start = false;
    }

    RateIncreaseScalarSamples {
        samples: Cow::Owned(retained_samples),
        counter_reset_hints: retained_counter_reset_hints.map(Cow::Owned),
    }
}

fn extrapolated_delta_projection_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    sample_start_times: Option<&[Option<u64>]>,
    range_start_ms: u64,
    include_range_start: bool,
    range_end_ms: u64,
) -> Option<f64> {
    if samples.is_empty() || range_end_ms <= range_start_ms {
        return None;
    }

    delta_projection_interval_increase(
        samples,
        counter_reset_hints,
        sample_start_times,
        range_start_ms,
        include_range_start,
        range_end_ms,
    )
}

fn delta_projection_interval_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    sample_start_times: Option<&[Option<u64>]>,
    range_start_ms: u64,
    include_range_start: bool,
    range_end_ms: u64,
) -> Option<f64> {
    let sample_start_times = sample_start_times?;
    if sample_start_times.len() != samples.len() {
        return None;
    }

    let mut increase = 0.0f64;
    let mut previous_raw = None::<f64>;
    let mut used_interval = false;

    for (idx, (&(timestamp_ms, raw), start_time_ms)) in
        samples.iter().zip(sample_start_times.iter()).enumerate()
    {
        if is_prometheus_stale_marker(raw) {
            previous_raw = None;
            continue;
        }
        let selected = if include_range_start {
            timestamp_ms >= range_start_ms
        } else {
            timestamp_ms > range_start_ms
        };
        if !selected {
            previous_raw = Some(raw);
            continue;
        }
        let start_time_ms = (*start_time_ms)?;
        if start_time_ms >= timestamp_ms {
            return None;
        }

        let starts_new_fragment = idx > 0
            && counter_reset_hints
                .and_then(|hints| hints.get(idx).copied())
                .is_some_and(|hint| hint == CounterResetHint::CounterReset);
        let raw_delta = if starts_new_fragment {
            raw
        } else {
            previous_raw.map_or(raw, |previous| raw - previous)
        };
        previous_raw = Some(raw);

        if delta_interval_intersects(start_time_ms, timestamp_ms, range_start_ms, range_end_ms) {
            increase += raw_delta;
            used_interval = true;
        }
    }

    used_interval.then_some(increase)
}

pub(super) fn delta_interval_intersects(
    start_time_ms: u64,
    timestamp_ms: u64,
    range_start_ms: u64,
    range_end_ms: u64,
) -> bool {
    start_time_ms < range_end_ms && timestamp_ms > range_start_ms
}

pub(super) fn validated_delta_interval_summary(
    intervals: impl IntoIterator<Item = (bool, u64, Option<u64>, Option<f64>)>,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<(usize, Option<f64>)> {
    if range_end_ms <= range_start_ms {
        return None;
    }

    let mut non_stale_count = 0usize;
    let mut sum = Some(0.0f64);
    let mut used_interval = false;
    for (stale, timestamp_ms, start_time_ms, interval_sum) in intervals {
        if stale {
            continue;
        }
        non_stale_count += 1;
        let start_time_ms = start_time_ms?;
        if start_time_ms >= timestamp_ms {
            return None;
        }
        if !delta_interval_intersects(start_time_ms, timestamp_ms, range_start_ms, range_end_ms) {
            continue;
        }
        sum = match (sum, interval_sum) {
            (Some(accumulated), Some(value)) => Some(accumulated + value),
            _ => None,
        };
        used_interval = true;
    }

    used_interval.then_some((non_stale_count, sum))
}

fn stitch_delta_projection_fragments_preserving_stale(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<Vec<(u64, f64)>> {
    let mut out = Vec::with_capacity(samples.len());
    let mut offset = 0.0f64;
    let mut previous_raw = None::<f64>;
    let mut previous_stitched = 0.0f64;

    for (idx, &(timestamp_ms, raw)) in samples.iter().enumerate() {
        if is_prometheus_stale_marker(raw) {
            out.push((timestamp_ms, raw));
            offset = 0.0;
            previous_raw = None;
            previous_stitched = 0.0;
            continue;
        }
        if !raw.is_finite() {
            return None;
        }
        let starts_new_fragment = idx > 0
            && counter_reset_hints
                .and_then(|hints| hints.get(idx).copied())
                .is_some_and(|hint| hint == CounterResetHint::CounterReset);
        if starts_new_fragment || previous_raw.is_some_and(|previous| raw < previous) {
            offset = previous_stitched;
        }
        let stitched = offset + raw;
        if !stitched.is_finite() {
            return None;
        }
        out.push((timestamp_ms, stitched));
        previous_raw = Some(raw);
        previous_stitched = stitched;
    }

    Some(out)
}

pub(in crate::storage::segment) fn counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    if let Some(counter_reset_hints) = counter_reset_hints {
        return counter_increase_with_reset_hints(samples, counter_reset_hints);
    }
    counter_increase_from_value_decreases(samples)
}

pub(in crate::storage::segment) fn extrapolated_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    force_unknown_after_stale: bool,
    range_start_ms: u64,
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    let retained =
        rate_increase_scalar_samples(samples, counter_reset_hints, force_unknown_after_stale);
    let samples = retained.samples.as_ref();
    let counter_reset_hints = retained.counter_reset_hints.as_deref();
    if samples.len() < 2 || range_end_ms <= range_start_ms {
        return None;
    }

    let raw_increase = counter_increase(samples, counter_reset_hints)?;
    let (first_ts, first_value) = samples.first().copied()?;
    let (last_ts, _) = samples.last().copied()?;
    let factor = counter_extrapolation_factor(
        samples.len(),
        first_ts,
        first_value,
        last_ts,
        raw_increase,
        range_start_ms,
        range_start_before_epoch_ms,
        range_end_ms,
    )?;

    Some(raw_increase * factor)
}

fn extrapolated_gauge_delta(
    samples: &[(u64, f64)],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if samples.len() < 2 || range_end_ms <= range_start_ms {
        return None;
    }

    let (samples, _, range_start_ms) =
        counter_samples_after_last_stale(samples, None, range_start_ms);
    if samples.len() < 2 {
        return None;
    }

    let (first_ts, first_value) = samples.first().copied()?;
    let (last_ts, last_value) = samples.last().copied()?;
    let raw_delta = last_value - first_value;
    if !raw_delta.is_finite() {
        return None;
    }
    let factor = gauge_extrapolation_factor(
        samples.len(),
        first_ts,
        last_ts,
        range_start_ms,
        range_end_ms,
    )?;

    Some(raw_delta * factor)
}

fn instant_counter_rate(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    let (samples, counter_reset_hints, _) =
        counter_samples_after_last_stale(samples, counter_reset_hints, 0);
    if samples.len() < 2 {
        return None;
    }

    let previous_idx = samples.len() - 2;
    let (previous_ts, previous_value) = samples[previous_idx];
    let (last_ts, last_value) = samples[previous_idx + 1];
    if last_ts <= previous_ts {
        return None;
    }

    let reset_hint = counter_reset_hints
        .and_then(|hints| hints.get(previous_idx + 1).copied())
        .unwrap_or(CounterResetHint::Unknown);
    let increase = counter_component_delta(previous_value, last_value, reset_hint)?;
    Some(increase / ((last_ts - previous_ts) as f64 / 1_000.0))
}

fn instant_gauge_delta(samples: &[(u64, f64)]) -> Option<f64> {
    let (samples, _, _) = counter_samples_after_last_stale(samples, None, 0);
    if samples.len() < 2 {
        return None;
    }

    let previous_idx = samples.len() - 2;
    let (previous_ts, previous_value) = samples[previous_idx];
    let (last_ts, last_value) = samples[previous_idx + 1];
    if last_ts <= previous_ts {
        return None;
    }

    let delta = last_value - previous_value;
    delta.is_finite().then_some(delta)
}

fn changes_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    let mut previous = None::<f64>;
    let mut changes = 0u64;

    for (_, value) in samples {
        if is_prometheus_stale_marker(*value) {
            continue;
        }
        if let Some(previous) = previous
            && value != &previous
            && !(value.is_nan() && previous.is_nan())
        {
            changes = changes.saturating_add(1);
        }
        previous = Some(*value);
    }

    previous.is_some().then_some(changes as f64)
}

fn resets_over_time(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    let (samples, counter_reset_hints, _) =
        counter_samples_after_last_stale(samples, counter_reset_hints, 0);
    let mut iter = samples.iter().copied();
    let (_, previous) = iter.next()?;
    if !previous.is_finite() {
        return None;
    }

    if let Some(counter_reset_hints) = counter_reset_hints {
        return resets_over_time_with_hints(samples, counter_reset_hints);
    }

    resets_over_time_from_value_decreases(previous, iter)
}

fn resets_over_time_from_value_decreases(
    mut previous: f64,
    samples: impl Iterator<Item = (u64, f64)>,
) -> Option<f64> {
    let mut resets = 0u64;
    for (_, current) in samples {
        if !current.is_finite() {
            return None;
        }
        if current < previous {
            resets = resets.saturating_add(1);
        }
        previous = current;
    }

    Some(resets as f64)
}

fn resets_over_time_with_hints(
    samples: &[(u64, f64)],
    counter_reset_hints: &[CounterResetHint],
) -> Option<f64> {
    if counter_reset_hints.len() != samples.len() {
        let mut iter = samples.iter().copied();
        let (_, previous) = iter.next()?;
        return resets_over_time_from_value_decreases(previous, iter);
    }

    let mut iter = samples
        .iter()
        .copied()
        .zip(counter_reset_hints.iter().copied());
    let ((_, mut previous), _) = iter.next()?;
    if !previous.is_finite() {
        return None;
    }

    let mut resets = 0u64;
    for ((_, current), reset_hint) in iter {
        if !current.is_finite() {
            return None;
        }
        match reset_hint {
            CounterResetHint::CounterReset => {
                resets = resets.saturating_add(1);
            }
            CounterResetHint::NotCounterReset => {
                if current < previous {
                    return None;
                }
            }
            CounterResetHint::Unknown => {
                if current < previous {
                    resets = resets.saturating_add(1);
                }
            }
            CounterResetHint::GaugeType => return None,
        }
        previous = current;
    }

    Some(resets as f64)
}

fn last_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    samples
        .iter()
        .rev()
        .find_map(|(_, value)| (!is_prometheus_stale_marker(*value)).then_some(*value))
}

fn count_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    let count = samples
        .iter()
        .filter(|(_, value)| !is_prometheus_stale_marker(*value))
        .count();
    (count > 0).then_some(count as f64)
}

fn present_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    samples
        .iter()
        .any(|(_, value)| !is_prometheus_stale_marker(*value))
        .then_some(1.0)
}

fn sum_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    let mut saw_sample = false;
    let mut sum = 0.0;
    let mut compensation = 0.0;
    for (_, value) in samples {
        if is_prometheus_stale_marker(*value) {
            continue;
        }
        saw_sample = true;
        (sum, compensation) = compensated_sum_inc(*value, sum, compensation);
    }
    if !saw_sample {
        return None;
    }
    if sum.is_infinite() {
        Some(sum)
    } else {
        Some(sum + compensation)
    }
}

fn compensated_sum_inc(value: f64, sum: f64, compensation: f64) -> (f64, f64) {
    let new_sum = sum + value;
    let new_compensation = if new_sum.is_infinite() {
        0.0
    } else if sum.abs() >= value.abs() {
        compensation + (sum - new_sum) + value
    } else {
        compensation + (value - new_sum) + sum
    };
    (new_sum, new_compensation)
}

fn avg_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    let mut saw_sample = false;
    let mut sum = 0.0;
    let mut mean = 0.0;
    let mut count = 0.0;
    let mut compensation = 0.0;
    let mut incremental_mean = false;

    for (_, value) in samples {
        if is_prometheus_stale_marker(*value) {
            continue;
        }
        saw_sample = true;
        count += 1.0;
        if !incremental_mean {
            let (new_sum, new_compensation) = compensated_sum_inc(*value, sum, compensation);
            if count == 1.0 || !new_sum.is_infinite() {
                sum = new_sum;
                compensation = new_compensation;
                continue;
            }
            incremental_mean = true;
            mean = sum / (count - 1.0);
            compensation /= count - 1.0;
        }

        if mean.is_infinite() {
            if value.is_infinite() && (mean.is_sign_positive() == value.is_sign_positive()) {
                continue;
            }
            if !value.is_infinite() && !value.is_nan() {
                continue;
            }
        }
        let corrected_mean = mean + compensation;
        (mean, compensation) = compensated_sum_inc(
            (*value / count) - (corrected_mean / count),
            mean,
            compensation,
        );
    }

    if !saw_sample {
        return None;
    }
    if incremental_mean {
        Some(mean + compensation)
    } else {
        Some((sum + compensation) / count)
    }
}

fn stdvar_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    let mut count = 0.0;
    let mut mean = 0.0;
    let mut mean_compensation = 0.0;
    let mut aux = 0.0;
    let mut aux_compensation = 0.0;

    for (_, value) in samples {
        if is_prometheus_stale_marker(*value) {
            continue;
        }
        count += 1.0;
        let corrected_mean = mean + mean_compensation;
        let delta = *value - corrected_mean;
        (mean, mean_compensation) = compensated_sum_inc(delta / count, mean, mean_compensation);
        let corrected_mean = mean + mean_compensation;
        (aux, aux_compensation) =
            compensated_sum_inc(delta * (*value - corrected_mean), aux, aux_compensation);
    }

    (count > 0.0).then_some((aux + aux_compensation) / count)
}

fn min_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    let mut min = None;
    for (_, value) in samples {
        if is_prometheus_stale_marker(*value) {
            continue;
        }
        min = Some(match min {
            Some(current) if *value < current || current.is_nan() => *value,
            Some(current) => current,
            None => *value,
        });
    }
    min
}

fn max_over_time(samples: &[(u64, f64)]) -> Option<f64> {
    let mut max = None;
    for (_, value) in samples {
        if is_prometheus_stale_marker(*value) {
            continue;
        }
        max = Some(match max {
            Some(current) if *value > current || current.is_nan() => *value,
            Some(current) => current,
            None => *value,
        });
    }
    max
}

fn quantile_over_time(quantile: f64, samples: &[(u64, f64)]) -> Option<f64> {
    let mut values = samples
        .iter()
        .filter_map(|(_, value)| (!is_prometheus_stale_marker(*value)).then_some(*value))
        .collect::<Vec<_>>();
    (!values.is_empty()).then(|| vector_quantile(quantile, &mut values))
}

fn deriv(samples: &[(u64, f64)], eval_time_ms: u64) -> Option<f64> {
    linear_regression(samples, eval_time_ms).map(|regression| regression.slope)
}

fn predict_linear(samples: &[(u64, f64)], eval_time_ms: u64, seconds: f64) -> Option<f64> {
    linear_regression(samples, eval_time_ms)
        .map(|regression| regression.intercept_at_eval + regression.slope * seconds)
}

#[derive(Debug, Clone, Copy)]
struct LinearRegression {
    slope: f64,
    intercept_at_eval: f64,
}

fn linear_regression(samples: &[(u64, f64)], eval_time_ms: u64) -> Option<LinearRegression> {
    let mut count = 0.0;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_x2 = 0.0;
    let mut sum_xy = 0.0;

    for (timestamp_ms, value) in samples {
        if is_prometheus_stale_marker(*value) {
            continue;
        }
        if !value.is_finite() {
            return Some(LinearRegression {
                slope: f64::NAN,
                intercept_at_eval: f64::NAN,
            });
        }
        let x = (*timestamp_ms as f64 - eval_time_ms as f64) / 1_000.0;
        count += 1.0;
        sum_x += x;
        sum_y += *value;
        sum_x2 += x * x;
        sum_xy += x * *value;
    }

    if count < 2.0 {
        return None;
    }

    let cov_xy = sum_xy - (sum_x * sum_y / count);
    let var_x = sum_x2 - (sum_x * sum_x / count);
    if var_x == 0.0 {
        return None;
    }
    let slope = cov_xy / var_x;
    let intercept_at_eval = (sum_y / count) - slope * (sum_x / count);
    Some(LinearRegression {
        slope,
        intercept_at_eval,
    })
}

fn double_exponential_smoothing(
    samples: &[(u64, f64)],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    let values = samples
        .iter()
        .filter_map(|(_, value)| (!is_prometheus_stale_marker(*value)).then_some(*value))
        .collect::<Vec<_>>();
    if values.len() < 2 {
        return None;
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Some(f64::NAN);
    }

    let mut smooth = values[0];
    let mut trend = values[1] - values[0];
    for value in values.into_iter().skip(1) {
        let previous_smooth = smooth;
        smooth = smoothing_factor * value + (1.0 - smoothing_factor) * (smooth + trend);
        trend = trend_factor * (smooth - previous_smooth) + (1.0 - trend_factor) * trend;
    }

    Some(smooth)
}

fn range_function_allows_non_finite_output(kind: PromqlRangeFunctionKind) -> bool {
    matches!(
        kind,
        PromqlRangeFunctionKind::Rate
            | PromqlRangeFunctionKind::Increase
            | PromqlRangeFunctionKind::LastOverTime
            | PromqlRangeFunctionKind::SumOverTime
            | PromqlRangeFunctionKind::AvgOverTime
            | PromqlRangeFunctionKind::StddevOverTime
            | PromqlRangeFunctionKind::StdvarOverTime
            | PromqlRangeFunctionKind::MinOverTime
            | PromqlRangeFunctionKind::MaxOverTime
            | PromqlRangeFunctionKind::Deriv
    )
}

fn gauge_extrapolation_factor(
    sample_count: usize,
    first_ts: u64,
    last_ts: u64,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if sample_count < 2 || last_ts <= first_ts {
        return None;
    }

    let sampled_interval = (last_ts - first_ts) as f64 / 1_000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_between_samples = sampled_interval / (sample_count - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;
    let mut duration_to_start = first_ts.saturating_sub(range_start_ms) as f64 / 1_000.0;
    let mut duration_to_end = range_end_ms.saturating_sub(last_ts) as f64 / 1_000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    Some((sampled_interval + duration_to_start + duration_to_end) / sampled_interval)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the Prometheus extrapolation formula keeps each sampled and range input explicit"
)]
pub(super) fn counter_extrapolation_factor(
    sample_count: usize,
    first_ts: u64,
    first_value: f64,
    last_ts: u64,
    raw_increase: f64,
    range_start_ms: u64,
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if sample_count < 2 || last_ts <= first_ts {
        return None;
    }

    let sampled_interval = (last_ts - first_ts) as f64 / 1_000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_between_samples = sampled_interval / (sample_count - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;
    let mut duration_to_start = first_ts
        .saturating_sub(range_start_ms)
        .saturating_add(range_start_before_epoch_ms) as f64
        / 1_000.0;
    let mut duration_to_end = range_end_ms.saturating_sub(last_ts) as f64 / 1_000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }

    if raw_increase > 0.0 && first_value >= 0.0 {
        let duration_to_zero = sampled_interval * (first_value / raw_increase);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    Some((sampled_interval + duration_to_start + duration_to_end) / sampled_interval)
}

fn counter_samples_after_last_stale<'a>(
    samples: &'a [(u64, f64)],
    counter_reset_hints: Option<&'a [CounterResetHint]>,
    range_start_ms: u64,
) -> CounterSamplesAfterStale<'a> {
    let Some((stale_idx, &(stale_ts, _))) = samples
        .iter()
        .enumerate()
        .rev()
        .find(|(_, (_, value))| !value.is_finite())
    else {
        return (samples, counter_reset_hints, range_start_ms);
    };

    let start_idx = stale_idx + 1;
    let samples = &samples[start_idx..];
    let counter_reset_hints = counter_reset_hints
        .filter(|hints| hints.len() == start_idx + samples.len())
        .map(|hints| &hints[start_idx..]);
    (samples, counter_reset_hints, range_start_ms.max(stale_ts))
}

pub(in crate::storage::segment) fn counter_increase_from_value_decreases(
    samples: &[(u64, f64)],
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples.iter();
    let (_, first) = iter.next().copied()?;
    let mut previous = first;
    let mut increase = samples.last()?.1 - first;
    for (_, current) in iter.copied() {
        if current < previous {
            increase += previous;
        }
        previous = current;
    }
    Some(increase)
}

pub(in crate::storage::segment) fn counter_increase_with_reset_hints(
    samples: &[(u64, f64)],
    counter_reset_hints: &[CounterResetHint],
) -> Option<f64> {
    if counter_reset_hints.len() != samples.len() {
        return counter_increase_from_value_decreases(samples);
    }
    if samples.len() < 2 {
        return None;
    }
    let first = samples.first()?.1;
    let last = samples.last()?.1;
    let iter = samples
        .iter()
        .copied()
        .zip(counter_reset_hints.iter().copied())
        .skip(1);
    let mut previous = first;
    let mut increase = last - first;
    for ((_, current), reset_hint) in iter {
        increase += counter_component_reset_adjustment(previous, current, reset_hint)?;
        previous = current;
    }
    Some(increase)
}

pub(in crate::storage::segment) fn function_result_labels(
    labels: &QueryLabels,
) -> Vec<(String, String)> {
    labels
        .pairs()
        .filter(|(key, _)| *key != METRIC_NAME_LABEL)
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}
