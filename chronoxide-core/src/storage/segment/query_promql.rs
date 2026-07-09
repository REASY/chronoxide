use super::*;
use crate::promql::{
    PromqlInstantFunction, PromqlInstantFunctionKind, PromqlLabelJoin, PromqlLabelReplace,
};
use chrono::{Datelike, TimeZone, Timelike, Utc};

pub(super) const DEFAULT_INSTANT_LOOKBACK_MS: u64 = 5 * 60 * 1_000;

pub(super) fn instant_vector_start_ms(end_ms: u64) -> u64 {
    end_ms.saturating_sub(DEFAULT_INSTANT_LOOKBACK_MS)
}

pub(super) fn range_function_start_ms(end_ms: u64, range_ms: u64) -> u64 {
    end_ms.saturating_sub(range_ms)
}

pub(super) fn evaluate_range_function(
    function: &PromqlRangeFunction,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
        let (samples, counter_reset_hints, sample_start_times) = range_function_scalar_samples(
            &result.samples,
            result.counter_reset_hints(),
            result.sample_start_times(),
            range_start_ms,
            eval_time_ms,
        );
        let value = match function.kind {
            PromqlRangeFunctionKind::Increase => match result.temporality {
                QueryResultTemporality::Delta => extrapolated_delta_projection_increase(
                    samples,
                    counter_reset_hints,
                    sample_start_times,
                    range_start_ms,
                    eval_time_ms,
                ),
                QueryResultTemporality::Mixed => None,
                QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                    extrapolated_counter_increase(
                        samples,
                        counter_reset_hints,
                        range_start_ms,
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
                        eval_time_ms,
                    ),
                    QueryResultTemporality::Mixed => None,
                    QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                        extrapolated_counter_increase(
                            samples,
                            counter_reset_hints,
                            range_start_ms,
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
            PromqlRangeFunctionKind::LastOverTime => last_over_time(samples),
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
        let labels = if function.kind == PromqlRangeFunctionKind::LastOverTime {
            result.labels.to_vec()
        } else {
            function_result_labels(&result.labels)
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_quantile_over_time(
    function: &PromqlQuantileOverTime,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_parameterized_range_function(results, eval_time_ms, function.range_ms, |samples| {
        quantile_over_time(function.quantile, samples)
    })
}

pub(super) fn evaluate_predict_linear(
    function: &PromqlPredictLinear,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_parameterized_range_function(results, eval_time_ms, function.range_ms, |samples| {
        predict_linear(samples, eval_time_ms, function.seconds)
    })
}

pub(super) fn evaluate_double_exponential_smoothing(
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
) -> (
    &'a [(u64, f64)],
    Option<&'a [CounterResetHint]>,
    Option<&'a [Option<u64>]>,
) {
    let original_len = samples.len();
    let start_idx = samples.partition_point(|(timestamp_ms, _)| *timestamp_ms <= range_start_ms);
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

fn extrapolated_delta_projection_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    sample_start_times: Option<&[Option<u64>]>,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if samples.is_empty() || range_end_ms <= range_start_ms {
        return None;
    }

    let (samples, counter_reset_hints, sample_start_times, range_start_ms) =
        counter_samples_after_last_stale_with_start_times(
            samples,
            counter_reset_hints,
            sample_start_times,
            range_start_ms,
        );
    if samples.is_empty() {
        return None;
    }

    if let Some(increase) = delta_projection_interval_increase(
        samples,
        counter_reset_hints,
        sample_start_times,
        range_start_ms,
        range_end_ms,
    ) {
        return Some(increase);
    }

    if samples.len() < 2 {
        return None;
    }

    let stitched = stitch_delta_projection_fragments(samples, counter_reset_hints)?;
    let counter_reset_hints = vec![CounterResetHint::NotCounterReset; stitched.len()];
    extrapolated_counter_increase(
        &stitched,
        Some(&counter_reset_hints),
        range_start_ms,
        range_end_ms,
    )
}

fn delta_projection_interval_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    sample_start_times: Option<&[Option<u64>]>,
    range_start_ms: u64,
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
        if !raw.is_finite() {
            return None;
        }
        let start_time_ms = (*start_time_ms)?;
        if start_time_ms >= timestamp_ms {
            previous_raw = Some(raw);
            continue;
        }

        let starts_new_fragment = idx > 0
            && counter_reset_hints
                .and_then(|hints| hints.get(idx).copied())
                .is_some_and(|hint| hint == CounterResetHint::CounterReset);
        let raw_delta = if starts_new_fragment || previous_raw.is_none_or(|previous| raw < previous)
        {
            raw
        } else {
            raw - previous_raw.expect("previous raw sample exists")
        };
        previous_raw = Some(raw);

        if delta_interval_intersects(start_time_ms, timestamp_ms, range_start_ms, range_end_ms) {
            if !raw_delta.is_finite() || raw_delta < 0.0 {
                return None;
            }
            increase += raw_delta;
            used_interval = true;
        }
    }

    used_interval.then_some(increase)
}

fn delta_interval_intersects(
    start_time_ms: u64,
    timestamp_ms: u64,
    range_start_ms: u64,
    range_end_ms: u64,
) -> bool {
    start_time_ms < range_end_ms && timestamp_ms > range_start_ms
}

fn stitch_delta_projection_fragments(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<Vec<(u64, f64)>> {
    let mut out = Vec::with_capacity(samples.len());
    let mut offset = 0.0f64;
    let mut previous_raw = None::<f64>;
    let mut previous_stitched = 0.0f64;

    for (idx, &(timestamp_ms, raw)) in samples.iter().enumerate() {
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

pub(super) fn counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    if let Some(counter_reset_hints) = counter_reset_hints {
        return counter_increase_with_reset_hints(samples, counter_reset_hints);
    }
    counter_increase_from_value_decreases(samples)
}

pub(super) fn extrapolated_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if samples.len() < 2 || range_end_ms <= range_start_ms {
        return None;
    }

    let (samples, counter_reset_hints, range_start_ms) =
        counter_samples_after_last_stale(samples, counter_reset_hints, range_start_ms);
    if samples.len() < 2 {
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
        if let Some(previous) = previous {
            if value != &previous && !(value.is_nan() && previous.is_nan()) {
                changes = changes.saturating_add(1);
            }
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
        PromqlRangeFunctionKind::LastOverTime
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

fn counter_extrapolation_factor(
    sample_count: usize,
    first_ts: u64,
    first_value: f64,
    last_ts: u64,
    raw_increase: f64,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if sample_count < 2 || last_ts <= first_ts || !first_value.is_finite() {
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
) -> (&'a [(u64, f64)], Option<&'a [CounterResetHint]>, u64) {
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

fn counter_samples_after_last_stale_with_start_times<'a>(
    samples: &'a [(u64, f64)],
    counter_reset_hints: Option<&'a [CounterResetHint]>,
    sample_start_times: Option<&'a [Option<u64>]>,
    range_start_ms: u64,
) -> (
    &'a [(u64, f64)],
    Option<&'a [CounterResetHint]>,
    Option<&'a [Option<u64>]>,
    u64,
) {
    let Some((stale_idx, &(stale_ts, _))) = samples
        .iter()
        .enumerate()
        .rev()
        .find(|(_, (_, value))| !value.is_finite())
    else {
        return (
            samples,
            counter_reset_hints,
            sample_start_times,
            range_start_ms,
        );
    };

    let start_idx = stale_idx + 1;
    let samples = &samples[start_idx..];
    let counter_reset_hints = counter_reset_hints
        .filter(|hints| hints.len() == start_idx + samples.len())
        .map(|hints| &hints[start_idx..]);
    let sample_start_times = sample_start_times
        .filter(|start_times| start_times.len() == start_idx + samples.len())
        .map(|start_times| &start_times[start_idx..]);
    (
        samples,
        counter_reset_hints,
        sample_start_times,
        range_start_ms.max(stale_ts),
    )
}

pub(super) fn counter_increase_from_value_decreases(samples: &[(u64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples.iter();
    let (_, first) = iter.next().copied()?;
    if !first.is_finite() {
        return None;
    }
    let mut previous = first;
    let mut increase = 0.0f64;
    for (_, current) in iter.copied() {
        if !current.is_finite() {
            return None;
        }
        if current >= previous {
            increase += current - previous;
        } else {
            increase += current;
        }
        previous = current;
    }
    Some(increase)
}

pub(super) fn counter_increase_with_reset_hints(
    samples: &[(u64, f64)],
    counter_reset_hints: &[CounterResetHint],
) -> Option<f64> {
    if counter_reset_hints.len() != samples.len() {
        return counter_increase_from_value_decreases(samples);
    }
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples
        .iter()
        .copied()
        .zip(counter_reset_hints.iter().copied());
    let ((_, first), _) = iter.next()?;
    if !first.is_finite() {
        return None;
    }
    let mut previous = first;
    let mut increase = 0.0f64;
    for ((_, current), reset_hint) in iter {
        if !current.is_finite() {
            return None;
        }
        match reset_hint {
            CounterResetHint::CounterReset => {
                increase += current;
            }
            CounterResetHint::NotCounterReset => {
                if current < previous {
                    return None;
                }
                increase += current - previous;
            }
            CounterResetHint::Unknown => {
                if current >= previous {
                    increase += current - previous;
                } else {
                    increase += current;
                }
            }
            CounterResetHint::GaugeType => return None,
        }
        previous = current;
    }
    Some(increase)
}

pub(super) fn function_result_labels(labels: &[(String, String)]) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL)
        .cloned()
        .collect()
}

pub(super) fn evaluate_aggregation(
    aggregation: &PromqlAggregation,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    if let PromqlAggregationOp::CountValues(value_label) = &aggregation.op {
        return evaluate_count_values_aggregation(
            value_label,
            &aggregation.grouping,
            results,
            eval_time_ms,
        );
    }

    if let Some((limit, largest)) = aggregation_rank_limit(&aggregation.op) {
        return evaluate_rank_aggregation(aggregation, results, eval_time_ms, limit, largest);
    }

    let collect_values = matches!(&aggregation.op, PromqlAggregationOp::Quantile(_));
    let mut groups = BTreeMap::<Vec<(String, String)>, AggregationAccumulator>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups
            .entry(labels)
            .or_default()
            .observe(value, collect_values);
    }

    let mut out = Vec::new();
    for (labels, accumulator) in groups {
        let Some(value) = accumulator.value(&aggregation.op) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_absent(
    absent: &PromqlAbsent,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    if results.iter().any(|result| {
        result
            .samples
            .last()
            .is_some_and(|(_, value)| !is_prometheus_stale_marker(*value))
    }) {
        return Vec::new();
    }

    let labels = absent.labels.clone();
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, 1.0);
    vec![result]
}

pub(super) fn evaluate_absent_over_time(
    function: &PromqlAbsentOverTime,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
    if results.iter().any(|result| {
        result.samples.iter().any(|(timestamp_ms, value)| {
            *timestamp_ms > range_start_ms
                && *timestamp_ms <= eval_time_ms
                && !is_prometheus_stale_marker(*value)
        })
    }) {
        return Vec::new();
    }

    let labels = function.labels.clone();
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, 1.0);
    vec![result]
}

pub(super) fn evaluate_instant_function(
    function: &PromqlInstantFunction,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    match function.kind {
        PromqlInstantFunctionKind::Sort => evaluate_sort(results, eval_time_ms, false),
        PromqlInstantFunctionKind::SortDesc => evaluate_sort(results, eval_time_ms, true),
        PromqlInstantFunctionKind::Abs => {
            evaluate_unary_value_function(results, eval_time_ms, f64::abs)
        }
        PromqlInstantFunctionKind::Ceil => {
            evaluate_unary_value_function(results, eval_time_ms, f64::ceil)
        }
        PromqlInstantFunctionKind::Floor => {
            evaluate_unary_value_function(results, eval_time_ms, f64::floor)
        }
        PromqlInstantFunctionKind::Round { to_nearest } => {
            evaluate_unary_value_function(results, eval_time_ms, |value| {
                round_to_nearest(value, to_nearest)
            })
        }
        PromqlInstantFunctionKind::Clamp { min, max } => {
            evaluate_clamp_function(results, eval_time_ms, min, max)
        }
        PromqlInstantFunctionKind::Ln => {
            evaluate_unary_value_function(results, eval_time_ms, f64::ln)
        }
        PromqlInstantFunctionKind::Log2 => {
            evaluate_unary_value_function(results, eval_time_ms, f64::log2)
        }
        PromqlInstantFunctionKind::Log10 => {
            evaluate_unary_value_function(results, eval_time_ms, f64::log10)
        }
        PromqlInstantFunctionKind::Sgn => evaluate_unary_value_function(results, eval_time_ms, sgn),
        PromqlInstantFunctionKind::Acos => {
            evaluate_unary_value_function(results, eval_time_ms, f64::acos)
        }
        PromqlInstantFunctionKind::Acosh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::acosh)
        }
        PromqlInstantFunctionKind::Asin => {
            evaluate_unary_value_function(results, eval_time_ms, f64::asin)
        }
        PromqlInstantFunctionKind::Asinh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::asinh)
        }
        PromqlInstantFunctionKind::Atan => {
            evaluate_unary_value_function(results, eval_time_ms, f64::atan)
        }
        PromqlInstantFunctionKind::Atanh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::atanh)
        }
        PromqlInstantFunctionKind::Cos => {
            evaluate_unary_value_function(results, eval_time_ms, f64::cos)
        }
        PromqlInstantFunctionKind::Cosh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::cosh)
        }
        PromqlInstantFunctionKind::Sin => {
            evaluate_unary_value_function(results, eval_time_ms, f64::sin)
        }
        PromqlInstantFunctionKind::Sinh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::sinh)
        }
        PromqlInstantFunctionKind::Tan => {
            evaluate_unary_value_function(results, eval_time_ms, f64::tan)
        }
        PromqlInstantFunctionKind::Tanh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::tanh)
        }
        PromqlInstantFunctionKind::Deg => {
            evaluate_unary_value_function(results, eval_time_ms, |value| {
                value * 180.0 / std::f64::consts::PI
            })
        }
        PromqlInstantFunctionKind::Rad => {
            evaluate_unary_value_function(results, eval_time_ms, |value| {
                value * std::f64::consts::PI / 180.0
            })
        }
        PromqlInstantFunctionKind::Minute
        | PromqlInstantFunctionKind::Hour
        | PromqlInstantFunctionKind::DayOfMonth
        | PromqlInstantFunctionKind::DayOfWeek
        | PromqlInstantFunctionKind::DayOfYear
        | PromqlInstantFunctionKind::DaysInMonth
        | PromqlInstantFunctionKind::Month
        | PromqlInstantFunctionKind::Year => {
            evaluate_time_extraction_function(function.kind, results, eval_time_ms)
        }
        PromqlInstantFunctionKind::Timestamp => evaluate_timestamp_function(results, eval_time_ms),
    }
}

pub(super) fn evaluate_scalar_function(
    _function: &PromqlScalarFunction,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut values = results.iter().filter_map(|result| {
        result
            .samples
            .last()
            .and_then(|(_, value)| (!is_prometheus_stale_marker(*value)).then_some(*value))
    });
    let Some(value) = values.next() else {
        return evaluate_scalar(f64::NAN, eval_time_ms);
    };
    if values.next().is_some() {
        return evaluate_scalar(f64::NAN, eval_time_ms);
    }
    evaluate_scalar(value, eval_time_ms)
}

fn sgn(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else if value.is_sign_positive() {
        1.0
    } else {
        -1.0
    }
}

fn evaluate_unary_value_function(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    f: impl Fn(f64) -> f64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels = function_result_labels(result.labels.as_ref());
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, f(value));
        out.push(result);
    }
    merge_query_results(out)
}

fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    if to_nearest == 0.0 {
        return f64::NAN;
    }
    (value / to_nearest + 0.5).floor() * to_nearest
}

fn evaluate_clamp_function(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    min: Option<f64>,
    max: Option<f64>,
) -> Vec<SegmentQueryResult> {
    if min.is_some_and(f64::is_nan)
        || max.is_some_and(f64::is_nan)
        || min.zip(max).is_some_and(|(min, max)| min > max)
    {
        return Vec::new();
    }
    evaluate_unary_value_function(results, eval_time_ms, |value| {
        let value = min.map_or(value, |min| value.max(min));
        max.map_or(value, |max| value.min(max))
    })
}

fn evaluate_time_extraction_function(
    kind: PromqlInstantFunctionKind,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_unary_value_function(results, eval_time_ms, |value| {
        extract_utc_time_component(kind, value)
    })
}

fn extract_utc_time_component(kind: PromqlInstantFunctionKind, timestamp_secs: f64) -> f64 {
    if !timestamp_secs.is_finite() {
        return f64::NAN;
    }
    let millis = timestamp_secs * 1000.0;
    if millis < i64::MIN as f64 || millis > i64::MAX as f64 {
        return f64::NAN;
    }
    let Some(datetime) = Utc.timestamp_millis_opt(millis as i64).single() else {
        return f64::NAN;
    };
    match kind {
        PromqlInstantFunctionKind::Minute => datetime.minute() as f64,
        PromqlInstantFunctionKind::Hour => datetime.hour() as f64,
        PromqlInstantFunctionKind::DayOfMonth => datetime.day() as f64,
        PromqlInstantFunctionKind::DayOfWeek => datetime.weekday().num_days_from_sunday() as f64,
        PromqlInstantFunctionKind::DayOfYear => datetime.ordinal() as f64,
        PromqlInstantFunctionKind::DaysInMonth => {
            days_in_utc_month(datetime.year(), datetime.month()) as f64
        }
        PromqlInstantFunctionKind::Month => datetime.month() as f64,
        PromqlInstantFunctionKind::Year => datetime.year() as f64,
        _ => f64::NAN,
    }
}

fn days_in_utc_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let this_month = Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap();
    let next_month = Utc
        .with_ymd_and_hms(next_year, next_month, 1, 0, 0, 0)
        .unwrap();
    (next_month - this_month).num_days() as u32
}

fn evaluate_timestamp_function(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((timestamp_ms, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels = function_result_labels(result.labels.as_ref());
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, timestamp_ms as f64 / 1000.0);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_label_replace(
    function: &PromqlLabelReplace,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let regex = regex::Regex::new(&function.regex).map_err(|err| {
        PromqlQueryError::Invalid(format!("label_replace regex is invalid: {err}"))
    })?;
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut labels = result.labels.as_ref().to_vec();
        let src_value = label_value(&labels, &function.src_label).unwrap_or_default();
        if regex.is_match(&src_value) {
            let replacement = regex
                .replace(&src_value, function.replacement.as_str())
                .into_owned();
            set_label_value(&mut labels, &function.dst_label, replacement);
        }
        labels.sort();
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

pub(super) fn evaluate_label_join(
    function: &PromqlLabelJoin,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut labels = result.labels.as_ref().to_vec();
        let joined = function
            .src_labels
            .iter()
            .map(|label| label_value(&labels, label).unwrap_or_default())
            .collect::<Vec<_>>()
            .join(&function.separator);
        set_label_value(&mut labels, &function.dst_label, joined);
        labels.sort();
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn label_value(labels: &[(String, String)], name: &str) -> Option<String> {
    labels
        .iter()
        .find(|(label_name, _)| label_name == name)
        .map(|(_, value)| value.clone())
}

fn set_label_value(labels: &mut Vec<(String, String)>, name: &str, value: String) {
    if let Some((_, existing_value)) = labels.iter_mut().find(|(label_name, _)| label_name == name)
    {
        *existing_value = value;
    } else {
        labels.push((name.to_string(), value));
    }
}

fn evaluate_sort(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    descending: bool,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut sorted_result =
            SegmentQueryResult::with_shared_labels(result.series_id, result.labels);
        sorted_result.push_sample(eval_time_ms, value);
        out.push(sorted_result);
    }

    out.sort_by(|left, right| {
        let left_value = left.samples[0].1;
        let right_value = right.samples[0].1;
        let value_order = rank_value_order(left_value, right_value, descending);
        value_order.then_with(|| left.labels.cmp(&right.labels))
    });
    out
}

fn evaluate_count_values_aggregation(
    value_label: &str,
    grouping: &PromqlAggregationGrouping,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let effective_grouping = count_values_grouping(grouping, value_label);
    let mut groups = BTreeMap::<Vec<(String, String)>, u64>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut labels = result.labels.as_ref().to_vec();
        set_count_values_label(&mut labels, value_label, count_values_label_value(value));
        let labels = aggregation_group_labels(&effective_grouping, &labels);
        let count = groups.entry(labels).or_default();
        *count = count.saturating_add(1);
    }

    let mut out = Vec::new();
    for (labels, count) in groups {
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, count as f64);
        out.push(result);
    }
    merge_query_results(out)
}

fn count_values_grouping(
    grouping: &PromqlAggregationGrouping,
    value_label: &str,
) -> PromqlAggregationGrouping {
    match grouping {
        PromqlAggregationGrouping::All => {
            PromqlAggregationGrouping::By(vec![value_label.to_string()])
        }
        PromqlAggregationGrouping::By(labels) => {
            let mut labels = labels.clone();
            if !labels.iter().any(|label| label == value_label) {
                labels.push(value_label.to_string());
            }
            PromqlAggregationGrouping::By(labels)
        }
        PromqlAggregationGrouping::Without(labels) => {
            PromqlAggregationGrouping::Without(labels.clone())
        }
    }
}

fn set_count_values_label(labels: &mut Vec<(String, String)>, value_label: &str, value: String) {
    if let Some((_, existing)) = labels.iter_mut().find(|(key, _)| key == value_label) {
        *existing = value;
    } else {
        labels.push((value_label.to_string(), value));
    }
}

fn count_values_label_value(value: f64) -> String {
    format_promql_float_label(value)
}

fn aggregation_rank_limit(op: &PromqlAggregationOp) -> Option<(usize, bool)> {
    match op {
        PromqlAggregationOp::TopK(limit) => Some((*limit, true)),
        PromqlAggregationOp::BottomK(limit) => Some((*limit, false)),
        PromqlAggregationOp::Sum
        | PromqlAggregationOp::Count
        | PromqlAggregationOp::Avg
        | PromqlAggregationOp::Min
        | PromqlAggregationOp::Max
        | PromqlAggregationOp::Stddev
        | PromqlAggregationOp::Stdvar
        | PromqlAggregationOp::Group
        | PromqlAggregationOp::Quantile(_)
        | PromqlAggregationOp::CountValues(_) => None,
    }
}

fn evaluate_rank_aggregation(
    aggregation: &PromqlAggregation,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    limit: usize,
    largest: bool,
) -> Vec<SegmentQueryResult> {
    if limit == 0 {
        return Vec::new();
    }

    let mut groups = BTreeMap::<Vec<(String, String)>, Vec<SegmentQueryResult>>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let group_labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        let ranked = SegmentQueryResult::with_shared_samples(
            result.series_id,
            result.labels,
            vec![(eval_time_ms, value)],
        );
        groups.entry(group_labels).or_default().push(ranked);
    }

    let mut out = Vec::new();
    for (_, mut group_results) in groups {
        group_results.sort_by(|left, right| {
            let left_value = left.samples[0].1;
            let right_value = right.samples[0].1;
            let value_order = rank_value_order(left_value, right_value, largest);
            value_order.then_with(|| left.labels.cmp(&right.labels))
        });
        out.extend(group_results.into_iter().take(limit));
    }
    merge_query_results(out)
}

fn rank_value_order(left: f64, right: f64, largest: bool) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => {
            if largest {
                right.total_cmp(&left)
            } else {
                left.total_cmp(&right)
            }
        }
    }
}

fn is_prometheus_stale_marker(value: f64) -> bool {
    value.to_bits() == prometheus_stale_nan().to_bits()
}

pub(super) fn evaluate_binary_vector_scalar(
    expression: &PromqlBinaryExpression,
    results: Vec<SegmentQueryResult>,
    scalar: f64,
    scalar_on_left: bool,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, vector_value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(vector_value) {
            continue;
        }
        let (value, labels) = if binary_operator_is_comparison(expression.op) {
            let matched = if scalar_on_left {
                compare_binary_operator(expression.op, scalar, vector_value)
            } else {
                compare_binary_operator(expression.op, vector_value, scalar)
            };
            if expression.return_bool {
                (
                    if matched { 1.0 } else { 0.0 },
                    function_result_labels(&result.labels),
                )
            } else if matched {
                (vector_value, result.labels.as_ref().to_vec())
            } else {
                continue;
            }
        } else {
            let value = if scalar_on_left {
                apply_binary_operator(expression.op, scalar, vector_value)
            } else {
                apply_binary_operator(expression.op, vector_value, scalar)
            };
            (value, function_result_labels(&result.labels))
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_binary_vector_vector(
    expression: &PromqlBinaryExpression,
    left_results: Vec<SegmentQueryResult>,
    right_results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;

    let left_entries = binary_vector_entries(left_results, expression.vector_matching.as_ref());
    let right_entries = binary_vector_entries(right_results, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => evaluate_binary_vector_one_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
            comparison,
            bool_comparison,
        ),
        PromqlVectorMatchingCardinality::ManyToOne => evaluate_binary_vector_many_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
            comparison,
            bool_comparison,
        ),
        PromqlVectorMatchingCardinality::OneToMany => evaluate_binary_vector_one_to_many(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
            comparison,
            bool_comparison,
        ),
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

#[derive(Debug, Clone)]
struct BinaryVectorEntry {
    labels: Vec<(String, String)>,
    key: Vec<(String, String)>,
    value: f64,
}

fn binary_vector_entries(
    results: Vec<SegmentQueryResult>,
    matching: Option<&PromqlVectorMatching>,
) -> Vec<BinaryVectorEntry> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        out.push(BinaryVectorEntry {
            key: binary_vector_match_labels(result.labels.as_ref(), matching),
            labels: result.labels.as_ref().to_vec(),
            value,
        });
    }
    out
}

fn binary_vector_matching_cardinality(
    expression: &PromqlBinaryExpression,
) -> PromqlVectorMatchingCardinality {
    expression
        .vector_matching
        .as_ref()
        .map(|matching| matching.cardinality)
        .unwrap_or(PromqlVectorMatchingCardinality::OneToOne)
}

fn evaluate_binary_vector_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryVectorEntry>,
    right_entries: Vec<BinaryVectorEntry>,
    eval_time_ms: u64,
    comparison: bool,
    bool_comparison: bool,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, (Vec<(String, String)>, f64)>::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            comparison,
            bool_comparison,
        );
        if left_by_key
            .insert(entry.key.clone(), (labels, entry.value))
            .is_some()
        {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, f64>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, entry.value).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    for (key, (labels, left)) in left_by_key {
        let Some(right) = right_by_key.get(&key) else {
            continue;
        };
        let Some(value) = evaluate_binary_vector_value(expression, comparison, left, *right) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

fn binary_vector_output_labels(
    base_labels: &[(String, String)],
    include_labels_from: &[(String, String)],
    matching: Option<&PromqlVectorMatching>,
    comparison: bool,
    bool_comparison: bool,
) -> Vec<(String, String)> {
    let mut labels = base_labels.to_vec();

    if !comparison || bool_comparison {
        labels.retain(|(key, _)| key != METRIC_NAME_LABEL);
    }

    if let Some(matching) = matching {
        if matches!(
            matching.cardinality,
            PromqlVectorMatchingCardinality::OneToOne
        ) {
            match matching.mode {
                PromqlVectorMatchingMode::On => {
                    labels.retain(|(key, _)| {
                        key != METRIC_NAME_LABEL
                            && matching
                                .labels
                                .iter()
                                .any(|matching_label| matching_label == key)
                    });
                }
                PromqlVectorMatchingMode::Ignoring => {
                    labels.retain(|(key, _)| {
                        !matching
                            .labels
                            .iter()
                            .any(|matching_label| matching_label == key)
                    });
                }
            }
        }

        for include_label in &matching.include_labels {
            match include_labels_from
                .iter()
                .find(|(key, _)| key == include_label)
            {
                Some((_, include_value)) => {
                    if let Some((_, existing_value)) =
                        labels.iter_mut().find(|(key, _)| key == include_label)
                    {
                        *existing_value = include_value.clone();
                    } else {
                        labels.push((include_label.clone(), include_value.clone()));
                    }
                }
                None => labels.retain(|(key, _)| key != include_label),
            }
        }
    }

    labels.sort();
    labels
}

fn binary_vector_group_output_labels(
    many_side_labels: &[(String, String)],
    one_side_labels: &[(String, String)],
    matching: &PromqlVectorMatching,
    comparison: bool,
    bool_comparison: bool,
) -> Vec<(String, String)> {
    binary_vector_output_labels(
        many_side_labels,
        one_side_labels,
        Some(matching),
        comparison,
        bool_comparison,
    )
}

fn evaluate_binary_vector_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryVectorEntry>,
    right_entries: Vec<BinaryVectorEntry>,
    eval_time_ms: u64,
    comparison: bool,
    bool_comparison: bool,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_left vector matching metadata".to_string())
    })?;
    let mut right_by_key = BTreeMap::<Vec<(String, String)>, BinaryVectorEntry>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for group_left binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
            continue;
        };
        let Some(value) =
            evaluate_binary_vector_value(expression, comparison, left.value, right.value)
        else {
            continue;
        };
        let labels = binary_vector_group_output_labels(
            &left.labels,
            &right.labels,
            matching,
            comparison,
            bool_comparison,
        );
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_left binary vector matching".to_string(),
            ));
        }
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

fn evaluate_binary_vector_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryVectorEntry>,
    right_entries: Vec<BinaryVectorEntry>,
    eval_time_ms: u64,
    comparison: bool,
    bool_comparison: bool,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_right vector matching metadata".to_string())
    })?;
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, BinaryVectorEntry>::new();
    for entry in left_entries {
        if left_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for group_right binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for right in right_entries {
        let Some(left) = left_by_key.get(&right.key) else {
            continue;
        };
        let Some(value) =
            evaluate_binary_vector_value(expression, comparison, left.value, right.value)
        else {
            continue;
        };
        let labels = binary_vector_group_output_labels(
            &right.labels,
            &left.labels,
            matching,
            comparison,
            bool_comparison,
        );
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_right binary vector matching".to_string(),
            ));
        }
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

fn evaluate_binary_vector_value(
    expression: &PromqlBinaryExpression,
    comparison: bool,
    left: f64,
    right: f64,
) -> Option<f64> {
    if comparison {
        let matched = compare_binary_operator(expression.op, left, right);
        if expression.return_bool {
            Some(if matched { 1.0 } else { 0.0 })
        } else if matched {
            Some(left)
        } else {
            None
        }
    } else {
        Some(apply_binary_operator(expression.op, left, right))
    }
}

pub(super) fn evaluate_binary_vector_set(
    expression: &PromqlBinaryExpression,
    left_results: Vec<SegmentQueryResult>,
    right_results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let mut left_entries = Vec::<(Vec<(String, String)>, Vec<(String, String)>, f64)>::new();
    let mut left_keys = BTreeSet::<Vec<(String, String)>>::new();
    for result in left_results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let key = binary_vector_set_match_labels(result.labels.as_ref(), expression);
        left_keys.insert(key.clone());
        left_entries.push((key, result.labels.as_ref().to_vec(), value));
    }

    let mut right_entries = Vec::<(Vec<(String, String)>, Vec<(String, String)>, f64)>::new();
    let mut right_keys = BTreeSet::<Vec<(String, String)>>::new();
    for result in right_results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let key = binary_vector_set_match_labels(result.labels.as_ref(), expression);
        right_keys.insert(key.clone());
        right_entries.push((key, result.labels.as_ref().to_vec(), value));
    }

    let mut out = Vec::new();
    match expression.op {
        PromqlBinaryOp::And => {
            for (key, labels, value) in left_entries {
                if right_keys.contains(&key) {
                    push_instant_result(&mut out, labels, value, eval_time_ms);
                }
            }
        }
        PromqlBinaryOp::Or => {
            for (_, labels, value) in left_entries {
                push_instant_result(&mut out, labels, value, eval_time_ms);
            }
            for (key, labels, value) in right_entries {
                if !left_keys.contains(&key) {
                    push_instant_result(&mut out, labels, value, eval_time_ms);
                }
            }
        }
        PromqlBinaryOp::Unless => {
            for (key, labels, value) in left_entries {
                if !right_keys.contains(&key) {
                    push_instant_result(&mut out, labels, value, eval_time_ms);
                }
            }
        }
        _ => {
            return Err(PromqlQueryError::Invalid(
                "non-set operator used for binary set evaluation".to_string(),
            ));
        }
    }
    Ok(merge_query_results(out))
}

fn binary_vector_set_match_labels(
    labels: &[(String, String)],
    expression: &PromqlBinaryExpression,
) -> Vec<(String, String)> {
    match expression.vector_matching.as_ref() {
        Some(matching) => binary_vector_match_labels(labels, Some(matching)),
        None => binary_vector_match_labels(labels, None),
    }
}

fn binary_vector_match_labels(
    labels: &[(String, String)],
    matching: Option<&PromqlVectorMatching>,
) -> Vec<(String, String)> {
    let mut labels = match matching {
        None => function_result_labels(labels),
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: matching_labels,
            ..
        }) => labels
            .iter()
            .filter(|(key, _)| matching_labels.iter().any(|label| label == key))
            .cloned()
            .collect(),
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::Ignoring,
            labels: matching_labels,
            ..
        }) => labels
            .iter()
            .filter(|(key, _)| {
                key != METRIC_NAME_LABEL && !matching_labels.iter().any(|label| label == key)
            })
            .cloned()
            .collect(),
    };
    labels.sort();
    labels
}

fn push_instant_result(
    out: &mut Vec<SegmentQueryResult>,
    labels: Vec<(String, String)>,
    value: f64,
    eval_time_ms: u64,
) {
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, value);
    out.push(result);
}

pub(super) fn evaluate_binary_scalar_scalar(
    op: PromqlBinaryOp,
    left: f64,
    right: f64,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_scalar(apply_binary_operator(op, left, right), eval_time_ms)
}

pub(super) fn evaluate_scalar(value: f64, eval_time_ms: u64) -> Vec<SegmentQueryResult> {
    let labels = Vec::new();
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, value);
    vec![result]
}

pub(super) fn scalar_expression_value(query: &PromqlQuery, eval_time_ms: u64) -> Option<f64> {
    match query {
        PromqlQuery::Scalar(value) => Some(*value),
        PromqlQuery::Time => Some(eval_time_ms as f64 / 1000.0),
        PromqlQuery::BinaryExpression(expression) => {
            if binary_operator_is_set(expression.op) {
                return None;
            }
            let left = scalar_expression_value(&expression.left, eval_time_ms)?;
            let right = scalar_expression_value(&expression.right, eval_time_ms)?;
            Some(apply_binary_operator(expression.op, left, right))
        }
        PromqlQuery::Vector(_)
        | PromqlQuery::VectorFunction(_)
        | PromqlQuery::ScalarFunction(_)
        | PromqlQuery::Offset(_)
        | PromqlQuery::LabelReplace(_)
        | PromqlQuery::LabelJoin(_)
        | PromqlQuery::RangeFunction(_)
        | PromqlQuery::QuantileOverTime(_)
        | PromqlQuery::PredictLinear(_)
        | PromqlQuery::DoubleExponentialSmoothing(_)
        | PromqlQuery::Aggregation(_)
        | PromqlQuery::Absent(_)
        | PromqlQuery::AbsentOverTime(_)
        | PromqlQuery::InstantFunction(_)
        | PromqlQuery::HistogramQuantile(_)
        | PromqlQuery::HistogramFraction(_)
        | PromqlQuery::HistogramScalarFunction(_) => None,
    }
}

pub(super) fn is_scalar_expression(query: &PromqlQuery) -> bool {
    match query {
        PromqlQuery::Scalar(_) | PromqlQuery::Time => true,
        PromqlQuery::BinaryExpression(expression)
            if !binary_operator_is_set(expression.op)
                && !expression.return_bool
                && expression.vector_matching.is_none() =>
        {
            is_scalar_expression(&expression.left) && is_scalar_expression(&expression.right)
        }
        _ => false,
    }
}

pub(super) fn binary_expression_vector_sides(
    expression: &PromqlBinaryExpression,
) -> Vec<&PromqlQuery> {
    let mut sides = Vec::with_capacity(2);
    if !is_scalar_expression(&expression.left) {
        sides.push(expression.left.as_ref());
    }
    if !is_scalar_expression(&expression.right) {
        sides.push(expression.right.as_ref());
    }
    sides
}

pub(super) fn offset_eval_time_ms(eval_time_ms: u64, offset_ms: i128) -> u64 {
    let shifted = i128::from(eval_time_ms).saturating_sub(offset_ms);
    shifted.clamp(0, i128::from(u64::MAX)) as u64
}

pub(super) fn retimestamp_instant_results(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut shifted = SegmentQueryResult::with_shared_labels(result.series_id, result.labels);
        shifted.push_sample(eval_time_ms, value);
        out.push(shifted);
    }
    merge_query_results(out)
}

pub(super) fn validate_promql_range_bounds(
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
) -> Result<(), PromqlQueryError> {
    if step_ms == 0 {
        return Err(PromqlQueryError::Invalid(
            "query_range step_ms must be greater than zero".to_string(),
        ));
    }
    if end_ms < start_ms {
        return Err(PromqlQueryError::Invalid(
            "query_range end_ms must be greater than or equal to start_ms".to_string(),
        ));
    }
    Ok(())
}

fn apply_binary_operator(op: PromqlBinaryOp, left: f64, right: f64) -> f64 {
    match op {
        PromqlBinaryOp::Add => left + right,
        PromqlBinaryOp::Sub => left - right,
        PromqlBinaryOp::Mul => left * right,
        PromqlBinaryOp::Div => left / right,
        PromqlBinaryOp::Mod => left % right,
        PromqlBinaryOp::Pow => left.powf(right),
        PromqlBinaryOp::Eq
        | PromqlBinaryOp::NotEq
        | PromqlBinaryOp::Gt
        | PromqlBinaryOp::Gte
        | PromqlBinaryOp::Lt
        | PromqlBinaryOp::Lte => {
            if compare_binary_operator(op, left, right) {
                1.0
            } else {
                0.0
            }
        }
        PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless => f64::NAN,
    }
}

pub(super) fn binary_operator_is_set(op: PromqlBinaryOp) -> bool {
    matches!(
        op,
        PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless
    )
}

fn binary_operator_is_comparison(op: PromqlBinaryOp) -> bool {
    matches!(
        op,
        PromqlBinaryOp::Eq
            | PromqlBinaryOp::NotEq
            | PromqlBinaryOp::Gt
            | PromqlBinaryOp::Gte
            | PromqlBinaryOp::Lt
            | PromqlBinaryOp::Lte
    )
}

fn compare_binary_operator(op: PromqlBinaryOp, left: f64, right: f64) -> bool {
    match op {
        PromqlBinaryOp::Eq => left == right,
        PromqlBinaryOp::NotEq => left != right,
        PromqlBinaryOp::Gt => left > right,
        PromqlBinaryOp::Gte => left >= right,
        PromqlBinaryOp::Lt => left < right,
        PromqlBinaryOp::Lte => left <= right,
        PromqlBinaryOp::Add
        | PromqlBinaryOp::Sub
        | PromqlBinaryOp::Mul
        | PromqlBinaryOp::Div
        | PromqlBinaryOp::Mod
        | PromqlBinaryOp::Pow => false,
        PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless => false,
    }
}

#[derive(Default)]
struct AggregationAccumulator {
    sum: f64,
    count: u64,
    finite_count: u64,
    nan_count: u64,
    positive_infinity_count: u64,
    negative_infinity_count: u64,
    mean: f64,
    m2: f64,
    min: Option<f64>,
    max: Option<f64>,
    values: Vec<f64>,
}

impl AggregationAccumulator {
    fn observe(&mut self, value: f64, collect_values: bool) {
        self.sum += value;
        self.count = self.count.saturating_add(1);
        if value.is_nan() {
            self.nan_count = self.nan_count.saturating_add(1);
        } else if value == f64::INFINITY {
            self.positive_infinity_count = self.positive_infinity_count.saturating_add(1);
        } else if value == f64::NEG_INFINITY {
            self.negative_infinity_count = self.negative_infinity_count.saturating_add(1);
        } else {
            self.finite_count = self.finite_count.saturating_add(1);
            let count = self.finite_count as f64;
            let delta = value - self.mean;
            self.mean += delta / count;
            let delta2 = value - self.mean;
            self.m2 += delta * delta2;
        }
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
        if collect_values {
            self.values.push(value);
        }
    }

    fn value(&self, op: &PromqlAggregationOp) -> Option<f64> {
        match op {
            PromqlAggregationOp::Sum => (self.count > 0).then_some(self.sum),
            PromqlAggregationOp::Count => (self.count > 0).then_some(self.count as f64),
            PromqlAggregationOp::Avg => self.avg_value(),
            PromqlAggregationOp::Min => self.min,
            PromqlAggregationOp::Max => self.max,
            PromqlAggregationOp::Stddev => self.stdvar_value().map(|value| value.sqrt()),
            PromqlAggregationOp::Stdvar => self.stdvar_value(),
            PromqlAggregationOp::Group => (self.count > 0).then_some(1.0),
            PromqlAggregationOp::Quantile(quantile) => {
                let mut values = self.values.clone();
                Some(vector_quantile(*quantile, &mut values))
            }
            PromqlAggregationOp::TopK(_)
            | PromqlAggregationOp::BottomK(_)
            | PromqlAggregationOp::CountValues(_) => None,
        }
    }

    fn avg_value(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        if self.nan_count > 0
            || (self.positive_infinity_count > 0 && self.negative_infinity_count > 0)
        {
            return Some(f64::NAN);
        }
        if self.positive_infinity_count > 0 {
            return Some(f64::INFINITY);
        }
        if self.negative_infinity_count > 0 {
            return Some(f64::NEG_INFINITY);
        }
        Some(self.mean)
    }

    fn stdvar_value(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        if self.finite_count != self.count {
            return Some(f64::NAN);
        }
        Some((self.m2 / self.finite_count as f64).max(0.0))
    }
}

fn vector_quantile(quantile: f64, values: &mut [f64]) -> f64 {
    if values.is_empty() || quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }

    values.sort_by(quantile_value_order);

    let n = values.len() as f64;
    let rank = quantile * (n - 1.0);
    let lower_index = rank.floor().max(0.0);
    let upper_index = (lower_index + 1.0).min(n - 1.0);
    let weight = rank - rank.floor();
    values[lower_index as usize] * (1.0 - weight) + values[upper_index as usize] * weight
}

fn quantile_value_order(left: &f64, right: &f64) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => left.total_cmp(right),
    }
}

pub(super) fn evaluate_histogram_aggregation(
    aggregation: &PromqlAggregation,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<PromqlHistogramSeries> {
    if !native_histogram_aggregation_supported(&aggregation.op) {
        return Vec::new();
    }

    let mut groups = BTreeMap::<Vec<(String, String)>, HistogramSumAccumulator>::new();
    for result in series {
        let Some(sample) = result.samples.last() else {
            continue;
        };
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups.entry(labels).or_default().observe(sample);
    }

    let mut out = Vec::new();
    for (labels, accumulator) in groups {
        let Some(sample) = accumulator.into_sample(eval_time_ms, &aggregation.op) else {
            continue;
        };
        let mut result =
            PromqlHistogramSeries::new(segment_series_id(&labels), shared_query_labels(labels));
        result.push_sample(sample);
        out.push(result);
    }
    merge_histogram_query_results(out)
}

pub(super) fn evaluate_exponential_histogram_aggregation(
    aggregation: &PromqlAggregation,
    series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<PromqlExponentialHistogramSeries> {
    if !native_histogram_aggregation_supported(&aggregation.op) {
        return Vec::new();
    }

    let mut groups = BTreeMap::<Vec<(String, String)>, ExponentialHistogramSumAccumulator>::new();
    for result in series {
        let Some(sample) = result.samples.last() else {
            continue;
        };
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups.entry(labels).or_default().observe(sample);
    }

    let mut out = Vec::new();
    for (labels, accumulator) in groups {
        let Some(sample) = accumulator.into_sample(eval_time_ms, &aggregation.op) else {
            continue;
        };
        let mut result = PromqlExponentialHistogramSeries::new(
            segment_series_id(&labels),
            shared_query_labels(labels),
        );
        result.push_sample(sample);
        out.push(result);
    }
    merge_exponential_histogram_query_results(out)
}

pub(super) fn native_histogram_aggregation_supported(op: &PromqlAggregationOp) -> bool {
    matches!(op, PromqlAggregationOp::Sum | PromqlAggregationOp::Avg)
}

pub(super) fn native_histogram_scalar_aggregation_supported(op: &PromqlAggregationOp) -> bool {
    matches!(op, PromqlAggregationOp::Count | PromqlAggregationOp::Group)
}

pub(super) fn evaluate_native_histogram_scalar_aggregation(
    aggregation: &PromqlAggregation,
    scalar_results: Vec<SegmentQueryResult>,
    histogram_series: Vec<PromqlHistogramSeries>,
    exponential_histogram_series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    if !native_histogram_scalar_aggregation_supported(&aggregation.op) {
        return Vec::new();
    }

    let mut groups = BTreeMap::<Vec<(String, String)>, NativeHistogramScalarAccumulator>::new();
    for result in scalar_results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups.entry(labels).or_default().observe();
    }
    for result in histogram_series {
        let Some(sample) = result.samples.last() else {
            continue;
        };
        if sample.stale {
            continue;
        }
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups.entry(labels).or_default().observe();
    }
    for result in exponential_histogram_series {
        let Some(sample) = result.samples.last() else {
            continue;
        };
        if sample.stale {
            continue;
        }
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups.entry(labels).or_default().observe();
    }

    let mut out = Vec::new();
    for (labels, accumulator) in groups {
        let Some(value) = accumulator.value(&aggregation.op) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

#[derive(Default)]
struct NativeHistogramScalarAccumulator {
    count: u64,
}

impl NativeHistogramScalarAccumulator {
    fn observe(&mut self) {
        self.count = self.count.saturating_add(1);
    }

    fn value(&self, op: &PromqlAggregationOp) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        match op {
            PromqlAggregationOp::Count => Some(self.count as f64),
            PromqlAggregationOp::Group => Some(1.0),
            _ => None,
        }
    }
}

#[derive(Default)]
struct HistogramSumAccumulator {
    explicit_bounds: Option<Arc<[f64]>>,
    count: f64,
    sum: Option<f64>,
    bucket_counts: Vec<f64>,
    samples: u64,
    valid: bool,
}

impl HistogramSumAccumulator {
    fn observe(&mut self, sample: &PromqlHistogramSample) {
        if sample.stale {
            return;
        }
        if self.samples == 0 {
            self.valid = true;
            self.sum = Some(0.0);
        }
        self.samples = self.samples.saturating_add(1);

        if !self.valid
            || !sample.count.is_finite()
            || sample.bucket_counts.len() != sample.explicit_bounds.len().saturating_add(1)
            || sample.bucket_counts.iter().any(|count| !count.is_finite())
            || sample.sum.is_some_and(|sum| !sum.is_finite())
        {
            self.valid = false;
            return;
        }

        match &self.explicit_bounds {
            None => {
                self.explicit_bounds = Some(sample.explicit_bounds.clone());
                self.bucket_counts = vec![0.0; sample.bucket_counts.len()];
            }
            Some(existing)
                if existing.as_ref() == sample.explicit_bounds.as_ref()
                    && self.bucket_counts.len() == sample.bucket_counts.len() => {}
            Some(_) => {
                self.valid = false;
                return;
            }
        }

        self.count += sample.count;
        for (out, value) in self
            .bucket_counts
            .iter_mut()
            .zip(sample.bucket_counts.iter())
        {
            *out += *value;
        }
        self.sum = match (self.sum, sample.sum) {
            (Some(accumulated), Some(value)) => Some(accumulated + value),
            _ => None,
        };
    }

    fn into_sample(
        self,
        timestamp_ms: u64,
        op: &PromqlAggregationOp,
    ) -> Option<PromqlHistogramSample> {
        if !self.valid || self.samples == 0 {
            return None;
        }
        let scale = native_histogram_aggregation_scale(op, self.samples)?;
        Some(PromqlHistogramSample {
            timestamp_ms,
            start_time_ms: None,
            count: self.count * scale,
            sum: self.sum.map(|sum| sum * scale),
            explicit_bounds: self.explicit_bounds?,
            bucket_counts: self
                .bucket_counts
                .into_iter()
                .map(|count| count * scale)
                .collect(),
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::GaugeType,
            stale: false,
        })
    }
}

#[derive(Default)]
struct ExponentialHistogramSumAccumulator {
    target_scale: Option<i32>,
    zero_threshold: f64,
    zero_threshold_bits: Option<u64>,
    count: f64,
    sum: Option<f64>,
    zero_count: f64,
    positive: BTreeMap<i32, f64>,
    negative: BTreeMap<i32, f64>,
    samples: u64,
    valid: bool,
}

impl ExponentialHistogramSumAccumulator {
    fn observe(&mut self, sample: &PromqlExponentialHistogramSample) {
        if sample.stale {
            return;
        }
        if self.samples == 0 {
            self.valid = true;
            self.sum = Some(0.0);
            self.zero_threshold = sample.zero_threshold;
            self.zero_threshold_bits = Some(sample.zero_threshold.to_bits());
            self.target_scale = Some(sample.scale);
        }
        self.samples = self.samples.saturating_add(1);

        if !self.valid
            || !sample.count.is_finite()
            || !sample.zero_count.is_finite()
            || sample.sum.is_some_and(|sum| !sum.is_finite())
            || self
                .zero_threshold_bits
                .is_some_and(|bits| bits != sample.zero_threshold.to_bits())
        {
            self.valid = false;
            return;
        }

        let Some(current_scale) = self.target_scale else {
            self.valid = false;
            return;
        };
        let target_scale = current_scale.min(sample.scale);
        if target_scale != current_scale {
            let Some(positive) = downscale_promql_exponential_bucket_map_to_map(
                &self.positive,
                current_scale,
                target_scale,
            ) else {
                self.valid = false;
                return;
            };
            let Some(negative) = downscale_promql_exponential_bucket_map_to_map(
                &self.negative,
                current_scale,
                target_scale,
            ) else {
                self.valid = false;
                return;
            };
            self.positive = positive;
            self.negative = negative;
            self.target_scale = Some(target_scale);
        }

        let Some(positive) = downscale_promql_exponential_buckets_to_map(
            &sample.positive,
            sample.scale,
            target_scale,
        ) else {
            self.valid = false;
            return;
        };
        let Some(negative) = downscale_promql_exponential_buckets_to_map(
            &sample.negative,
            sample.scale,
            target_scale,
        ) else {
            self.valid = false;
            return;
        };

        self.count += sample.count;
        self.zero_count += sample.zero_count;
        add_promql_exponential_bucket_maps(&mut self.positive, positive);
        add_promql_exponential_bucket_maps(&mut self.negative, negative);
        self.sum = match (self.sum, sample.sum) {
            (Some(accumulated), Some(value)) => Some(accumulated + value),
            _ => None,
        };
    }

    fn into_sample(
        self,
        timestamp_ms: u64,
        op: &PromqlAggregationOp,
    ) -> Option<PromqlExponentialHistogramSample> {
        if !self.valid || self.samples == 0 {
            return None;
        }
        let scale = native_histogram_aggregation_scale(op, self.samples)?;
        Some(PromqlExponentialHistogramSample {
            timestamp_ms,
            start_time_ms: None,
            count: self.count * scale,
            sum: self.sum.map(|sum| sum * scale),
            scale: self.target_scale?,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count * scale,
            positive: promql_exponential_bucket_map_to_buckets(
                scale_promql_exponential_bucket_map(self.positive, scale),
            )?,
            negative: promql_exponential_bucket_map_to_buckets(
                scale_promql_exponential_bucket_map(self.negative, scale),
            )?,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::GaugeType,
            stale: false,
        })
    }
}

fn native_histogram_aggregation_scale(op: &PromqlAggregationOp, samples: u64) -> Option<f64> {
    match op {
        PromqlAggregationOp::Sum => Some(1.0),
        PromqlAggregationOp::Avg => Some(1.0 / samples as f64),
        _ => None,
    }
}

fn scale_promql_exponential_bucket_map(map: BTreeMap<i32, f64>, scale: f64) -> BTreeMap<i32, f64> {
    map.into_iter()
        .map(|(index, count)| (index, count * scale))
        .collect()
}

fn aggregation_group_labels(
    grouping: &PromqlAggregationGrouping,
    labels: &[(String, String)],
) -> Vec<(String, String)> {
    let mut out = match grouping {
        PromqlAggregationGrouping::All => Vec::new(),
        PromqlAggregationGrouping::By(grouping_labels) => labels
            .iter()
            .filter(|(key, _)| grouping_labels.iter().any(|label| label == key))
            .cloned()
            .collect(),
        PromqlAggregationGrouping::Without(grouping_labels) => labels
            .iter()
            .filter(|(key, _)| {
                key != METRIC_NAME_LABEL && !grouping_labels.iter().any(|label| label == key)
            })
            .cloned()
            .collect(),
    };
    out.sort();
    out
}

pub(super) fn evaluate_histogram_quantile(
    function: &PromqlHistogramQuantile,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut groups = BTreeMap::<Vec<(String, String)>, Vec<(f64, f64)>>::new();
    for result in results {
        let Some(upper_bound) = histogram_bucket_upper_bound(&result.labels) else {
            continue;
        };
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if !value.is_finite() {
            continue;
        }
        let labels = histogram_quantile_result_labels(&result.labels);
        groups.entry(labels).or_default().push((upper_bound, value));
    }

    let mut out = Vec::new();
    for (labels, buckets) in groups {
        let Some(value) = classic_histogram_quantile(function.quantile, buckets) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_histogram_range_function(
    function: &PromqlRangeFunction,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<PromqlHistogramSeries> {
    if !matches!(
        function.kind,
        PromqlRangeFunctionKind::Rate | PromqlRangeFunctionKind::Increase
    ) {
        return Vec::new();
    }

    let mut out = Vec::new();
    let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
    for input in series {
        let samples =
            range_function_histogram_samples(&input.samples, range_start_ms, eval_time_ms);
        let (samples, effective_range_start_ms) =
            histogram_samples_after_last_stale(samples, range_start_ms);
        let Some(mut increase) =
            histogram_counter_increase(samples, effective_range_start_ms, eval_time_ms)
        else {
            continue;
        };
        if function.kind == PromqlRangeFunctionKind::Rate {
            if function.range_ms == 0 {
                continue;
            }
            let seconds = function.range_ms as f64 / 1_000.0;
            increase.count /= seconds;
            for bucket in &mut increase.bucket_counts {
                *bucket /= seconds;
            }
            if let Some(sum) = &mut increase.sum {
                *sum /= seconds;
            }
        }
        increase.timestamp_ms = eval_time_ms;
        increase.reset_hint = CounterResetHint::GaugeType;
        let mut result = PromqlHistogramSeries::new(input.series_id, input.labels.clone());
        result.push_sample(increase);
        out.push(result);
    }
    merge_histogram_query_results(out)
}

fn range_function_histogram_samples<'a>(
    samples: &'a [PromqlHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> &'a [PromqlHistogramSample] {
    let start_idx = samples.partition_point(|sample| sample.timestamp_ms <= range_start_ms);
    let end_idx = start_idx
        + samples[start_idx..].partition_point(|sample| sample.timestamp_ms <= range_end_ms);
    &samples[start_idx..end_idx]
}

fn histogram_samples_after_last_stale(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
) -> (&[PromqlHistogramSample], u64) {
    let Some(stale_idx) = samples.iter().rposition(|sample| sample.stale) else {
        return (samples, range_start_ms);
    };
    let stale_ts = samples[stale_idx].timestamp_ms;
    (
        &samples[stale_idx.saturating_add(1)..],
        range_start_ms.max(stale_ts),
    )
}

fn histogram_counter_increase(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlHistogramSample> {
    if samples
        .iter()
        .all(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        if samples.iter().all(|sample| sample.start_time_ms.is_some()) {
            return delta_histogram_interval_increase(samples, range_start_ms, range_end_ms);
        }
        let cumulative = cumulative_delta_histogram_samples(samples)?;
        return cumulative_histogram_counter_increase(&cumulative, range_start_ms, range_end_ms);
    }
    if samples
        .iter()
        .any(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        return None;
    }

    cumulative_histogram_counter_increase(samples, range_start_ms, range_end_ms)
}

fn delta_histogram_interval_increase(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlHistogramSample> {
    if samples.is_empty() || range_end_ms <= range_start_ms {
        return None;
    }

    let first = samples.first()?;
    if first.bucket_counts.len() != first.explicit_bounds.len().saturating_add(1) {
        return None;
    }
    let bounds = first.explicit_bounds.clone();
    let mut count = 0.0f64;
    let mut bucket_counts = vec![0.0f64; first.bucket_counts.len()];
    let mut sum = Some(0.0f64);
    let mut used_interval = false;

    for sample in samples {
        if sample.stale
            || sample.explicit_bounds != bounds
            || sample.bucket_counts.len() != bucket_counts.len()
            || !sample.count.is_finite()
            || sample.bucket_counts.iter().any(|count| !count.is_finite())
            || sample.sum.is_some_and(|sum| !sum.is_finite())
        {
            return None;
        }

        let start_time_ms = sample.start_time_ms?;
        if start_time_ms >= sample.timestamp_ms
            || !delta_interval_intersects(
                start_time_ms,
                sample.timestamp_ms,
                range_start_ms,
                range_end_ms,
            )
        {
            continue;
        }

        count += sample.count;
        for (out_bucket, sample_bucket) in bucket_counts
            .iter_mut()
            .zip(sample.bucket_counts.iter().copied())
        {
            *out_bucket += sample_bucket;
        }
        sum = match (sum, sample.sum) {
            (Some(accumulated), Some(value)) => Some(accumulated + value),
            _ => None,
        };
        used_interval = true;
    }

    used_interval.then_some(PromqlHistogramSample {
        timestamp_ms: range_end_ms,
        start_time_ms: None,
        count,
        sum,
        explicit_bounds: bounds,
        bucket_counts,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::GaugeType,
        stale: false,
    })
}

fn cumulative_histogram_counter_increase(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlHistogramSample> {
    let first = samples.first()?;
    let last = samples.last()?;
    if samples.len() < 2
        || samples.iter().any(|sample| sample.stale)
        || first.explicit_bounds != last.explicit_bounds
        || first.bucket_counts.len() != first.explicit_bounds.len().saturating_add(1)
    {
        return None;
    }

    let bounds = first.explicit_bounds.clone();
    let mut count = 0.0f64;
    let mut bucket_counts = vec![0.0f64; first.bucket_counts.len()];
    let mut sum = Some(0.0f64);
    let mut previous = first;

    for current in samples.iter().skip(1) {
        if current.explicit_bounds != bounds || current.bucket_counts.len() != bucket_counts.len() {
            return None;
        }
        count += counter_component_delta(previous.count, current.count, current.reset_hint)?;
        for ((out, previous_bucket), current_bucket) in bucket_counts
            .iter_mut()
            .zip(previous.bucket_counts.iter().copied())
            .zip(current.bucket_counts.iter().copied())
        {
            *out += counter_component_delta(previous_bucket, current_bucket, current.reset_hint)?;
        }
        sum = match (sum, previous.sum, current.sum) {
            (Some(accumulated), Some(previous_sum), Some(current_sum)) => Some(
                accumulated
                    + counter_component_delta(previous_sum, current_sum, current.reset_hint)?,
            ),
            _ => None,
        };
        previous = current;
    }

    let factor = counter_extrapolation_factor(
        samples.len(),
        first.timestamp_ms,
        first.count,
        last.timestamp_ms,
        count,
        range_start_ms,
        range_end_ms,
    )?;

    count *= factor;
    for bucket in &mut bucket_counts {
        *bucket *= factor;
    }
    if let Some(sum) = &mut sum {
        *sum *= factor;
    }

    Some(PromqlHistogramSample {
        timestamp_ms: range_end_ms,
        start_time_ms: None,
        count,
        sum,
        explicit_bounds: bounds,
        bucket_counts,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::GaugeType,
        stale: false,
    })
}

fn cumulative_delta_histogram_samples(
    samples: &[PromqlHistogramSample],
) -> Option<Vec<PromqlHistogramSample>> {
    let first = samples.first()?;
    if first.bucket_counts.len() != first.explicit_bounds.len().saturating_add(1) {
        return None;
    }

    let bounds = first.explicit_bounds.clone();
    let mut count = 0.0f64;
    let mut bucket_counts = vec![0.0f64; first.bucket_counts.len()];
    let mut sum = Some(0.0f64);
    let mut out = Vec::with_capacity(samples.len());

    for sample in samples {
        if sample.stale
            || sample.explicit_bounds != bounds
            || sample.bucket_counts.len() != bucket_counts.len()
            || !sample.count.is_finite()
            || sample.bucket_counts.iter().any(|count| !count.is_finite())
            || sample.sum.is_some_and(|sum| !sum.is_finite())
        {
            return None;
        }

        count += sample.count;
        for (out_bucket, sample_bucket) in bucket_counts
            .iter_mut()
            .zip(sample.bucket_counts.iter().copied())
        {
            *out_bucket += sample_bucket;
        }
        sum = match (sum, sample.sum) {
            (Some(accumulated), Some(value)) => Some(accumulated + value),
            _ => None,
        };

        out.push(PromqlHistogramSample {
            timestamp_ms: sample.timestamp_ms,
            start_time_ms: None,
            count,
            sum,
            explicit_bounds: bounds.clone(),
            bucket_counts: bucket_counts.clone(),
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::NotCounterReset,
            stale: false,
        });
    }

    Some(out)
}

fn counter_component_delta(
    previous: f64,
    current: f64,
    reset_hint: CounterResetHint,
) -> Option<f64> {
    if !previous.is_finite() || !current.is_finite() {
        return None;
    }
    match reset_hint {
        CounterResetHint::CounterReset => Some(current),
        CounterResetHint::NotCounterReset => (current >= previous).then_some(current - previous),
        CounterResetHint::Unknown => {
            if current >= previous {
                Some(current - previous)
            } else {
                Some(current)
            }
        }
        CounterResetHint::GaugeType => None,
    }
}

pub(super) fn evaluate_exponential_histogram_range_function(
    function: &PromqlRangeFunction,
    series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<PromqlExponentialHistogramSeries> {
    if !matches!(
        function.kind,
        PromqlRangeFunctionKind::Rate | PromqlRangeFunctionKind::Increase
    ) {
        return Vec::new();
    }

    let mut out = Vec::new();
    let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
    for input in series {
        let samples = range_function_exponential_histogram_samples(
            &input.samples,
            range_start_ms,
            eval_time_ms,
        );
        let (samples, effective_range_start_ms) =
            exponential_histogram_samples_after_last_stale(samples, range_start_ms);
        let Some(mut increase) =
            exponential_histogram_counter_increase(samples, effective_range_start_ms, eval_time_ms)
        else {
            continue;
        };
        if function.kind == PromqlRangeFunctionKind::Rate {
            if function.range_ms == 0 {
                continue;
            }
            let seconds = function.range_ms as f64 / 1_000.0;
            increase.count /= seconds;
            increase.zero_count /= seconds;
            let scale = 1.0 / seconds;
            increase.positive.scale_counts(scale);
            increase.negative.scale_counts(scale);
            if let Some(sum) = &mut increase.sum {
                *sum /= seconds;
            }
        }
        increase.timestamp_ms = eval_time_ms;
        increase.reset_hint = CounterResetHint::GaugeType;
        let mut result =
            PromqlExponentialHistogramSeries::new(input.series_id, input.labels.clone());
        result.push_sample(increase);
        out.push(result);
    }
    merge_exponential_histogram_query_results(out)
}

fn range_function_exponential_histogram_samples<'a>(
    samples: &'a [PromqlExponentialHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> &'a [PromqlExponentialHistogramSample] {
    let start_idx = samples.partition_point(|sample| sample.timestamp_ms <= range_start_ms);
    let end_idx = start_idx
        + samples[start_idx..].partition_point(|sample| sample.timestamp_ms <= range_end_ms);
    &samples[start_idx..end_idx]
}

fn exponential_histogram_samples_after_last_stale(
    samples: &[PromqlExponentialHistogramSample],
    range_start_ms: u64,
) -> (&[PromqlExponentialHistogramSample], u64) {
    let Some(stale_idx) = samples.iter().rposition(|sample| sample.stale) else {
        return (samples, range_start_ms);
    };
    let stale_ts = samples[stale_idx].timestamp_ms;
    (
        &samples[stale_idx.saturating_add(1)..],
        range_start_ms.max(stale_ts),
    )
}

fn exponential_histogram_counter_increase(
    samples: &[PromqlExponentialHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlExponentialHistogramSample> {
    if samples
        .iter()
        .all(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        if samples.iter().all(|sample| sample.start_time_ms.is_some()) {
            return delta_exponential_histogram_interval_increase(
                samples,
                range_start_ms,
                range_end_ms,
            );
        }
        let cumulative = cumulative_delta_exponential_histogram_samples(samples)?;
        return cumulative_exponential_histogram_counter_increase(
            &cumulative,
            range_start_ms,
            range_end_ms,
        );
    }
    if samples
        .iter()
        .any(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        return None;
    }

    cumulative_exponential_histogram_counter_increase(samples, range_start_ms, range_end_ms)
}

fn delta_exponential_histogram_interval_increase(
    samples: &[PromqlExponentialHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlExponentialHistogramSample> {
    if samples.is_empty() || range_end_ms <= range_start_ms {
        return None;
    }

    let mut target_scale = None::<i32>;
    let mut zero_threshold = 0.0f64;
    let mut zero_threshold_bits = None::<u64>;
    let mut count = 0.0f64;
    let mut zero_count = 0.0f64;
    let mut positive = BTreeMap::<i32, f64>::new();
    let mut negative = BTreeMap::<i32, f64>::new();
    let mut sum = Some(0.0f64);
    let mut used_interval = false;

    for sample in samples {
        let start_time_ms = sample.start_time_ms?;
        if start_time_ms >= sample.timestamp_ms
            || !delta_interval_intersects(
                start_time_ms,
                sample.timestamp_ms,
                range_start_ms,
                range_end_ms,
            )
        {
            continue;
        }

        if sample.stale
            || !sample.count.is_finite()
            || !sample.zero_count.is_finite()
            || sample.sum.is_some_and(|sum| !sum.is_finite())
        {
            return None;
        }

        match zero_threshold_bits {
            Some(bits) if bits != sample.zero_threshold.to_bits() => return None,
            Some(_) => {}
            None => {
                zero_threshold = sample.zero_threshold;
                zero_threshold_bits = Some(sample.zero_threshold.to_bits());
            }
        }

        match target_scale {
            Some(current_scale) => {
                let next_scale = current_scale.min(sample.scale);
                if next_scale != current_scale {
                    positive = downscale_promql_exponential_bucket_map_to_map(
                        &positive,
                        current_scale,
                        next_scale,
                    )?;
                    negative = downscale_promql_exponential_bucket_map_to_map(
                        &negative,
                        current_scale,
                        next_scale,
                    )?;
                    target_scale = Some(next_scale);
                }
            }
            None => {
                target_scale = Some(sample.scale);
            }
        }
        let target_scale = target_scale?;
        let sample_positive = downscale_promql_exponential_buckets_to_map(
            &sample.positive,
            sample.scale,
            target_scale,
        )?;
        let sample_negative = downscale_promql_exponential_buckets_to_map(
            &sample.negative,
            sample.scale,
            target_scale,
        )?;

        count += sample.count;
        zero_count += sample.zero_count;
        add_promql_exponential_bucket_maps(&mut positive, sample_positive);
        add_promql_exponential_bucket_maps(&mut negative, sample_negative);
        sum = match (sum, sample.sum) {
            (Some(accumulated), Some(value)) => Some(accumulated + value),
            _ => None,
        };
        used_interval = true;
    }

    if !used_interval {
        return None;
    }

    Some(PromqlExponentialHistogramSample {
        timestamp_ms: range_end_ms,
        start_time_ms: None,
        count,
        sum,
        scale: target_scale?,
        zero_threshold,
        zero_count,
        positive: promql_exponential_bucket_map_to_buckets(positive)?,
        negative: promql_exponential_bucket_map_to_buckets(negative)?,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::GaugeType,
        stale: false,
    })
}

fn cumulative_exponential_histogram_counter_increase(
    samples: &[PromqlExponentialHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlExponentialHistogramSample> {
    let first = samples.first()?;
    let last = samples.last()?;
    if samples.len() < 2
        || samples.iter().any(|sample| sample.stale)
        || samples
            .iter()
            .any(|sample| sample.zero_threshold.to_bits() != first.zero_threshold.to_bits())
    {
        return None;
    }

    let target_scale = samples.iter().map(|sample| sample.scale).min()?;
    let mut count = 0.0f64;
    let mut zero_count = 0.0f64;
    let mut positive = BTreeMap::<i32, f64>::new();
    let mut negative = BTreeMap::<i32, f64>::new();
    let mut sum = Some(0.0f64);
    let mut previous = first;
    let mut previous_positive = downscale_promql_exponential_buckets_to_map(
        &previous.positive,
        previous.scale,
        target_scale,
    )?;
    let mut previous_negative = downscale_promql_exponential_buckets_to_map(
        &previous.negative,
        previous.scale,
        target_scale,
    )?;

    for current in samples.iter().skip(1) {
        let current_positive = downscale_promql_exponential_buckets_to_map(
            &current.positive,
            current.scale,
            target_scale,
        )?;
        let current_negative = downscale_promql_exponential_buckets_to_map(
            &current.negative,
            current.scale,
            target_scale,
        )?;

        count += counter_component_delta(previous.count, current.count, current.reset_hint)?;
        zero_count +=
            counter_component_delta(previous.zero_count, current.zero_count, current.reset_hint)?;
        add_promql_exponential_bucket_maps(
            &mut positive,
            counter_bucket_map_delta(&previous_positive, &current_positive, current.reset_hint)?,
        );
        add_promql_exponential_bucket_maps(
            &mut negative,
            counter_bucket_map_delta(&previous_negative, &current_negative, current.reset_hint)?,
        );
        sum = match (sum, previous.sum, current.sum) {
            (Some(accumulated), Some(previous_sum), Some(current_sum)) => Some(
                accumulated
                    + counter_component_delta(previous_sum, current_sum, current.reset_hint)?,
            ),
            _ => None,
        };
        previous = current;
        previous_positive = current_positive;
        previous_negative = current_negative;
    }

    let factor = counter_extrapolation_factor(
        samples.len(),
        first.timestamp_ms,
        first.count,
        last.timestamp_ms,
        count,
        range_start_ms,
        range_end_ms,
    )?;

    count *= factor;
    zero_count *= factor;
    for bucket in positive.values_mut() {
        *bucket *= factor;
    }
    for bucket in negative.values_mut() {
        *bucket *= factor;
    }
    if let Some(sum) = &mut sum {
        *sum *= factor;
    }

    Some(PromqlExponentialHistogramSample {
        timestamp_ms: range_end_ms,
        start_time_ms: None,
        count,
        sum,
        scale: target_scale,
        zero_threshold: first.zero_threshold,
        zero_count,
        positive: promql_exponential_bucket_map_to_buckets(positive)?,
        negative: promql_exponential_bucket_map_to_buckets(negative)?,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::GaugeType,
        stale: false,
    })
}

fn cumulative_delta_exponential_histogram_samples(
    samples: &[PromqlExponentialHistogramSample],
) -> Option<Vec<PromqlExponentialHistogramSample>> {
    let first = samples.first()?;
    if samples
        .iter()
        .any(|sample| sample.zero_threshold.to_bits() != first.zero_threshold.to_bits())
    {
        return None;
    }

    let target_scale = samples.iter().map(|sample| sample.scale).min()?;
    let mut count = 0.0f64;
    let mut zero_count = 0.0f64;
    let mut positive = BTreeMap::<i32, f64>::new();
    let mut negative = BTreeMap::<i32, f64>::new();
    let mut sum = Some(0.0f64);
    let mut out = Vec::with_capacity(samples.len());

    for sample in samples {
        if sample.stale
            || !sample.count.is_finite()
            || !sample.zero_count.is_finite()
            || sample.sum.is_some_and(|sum| !sum.is_finite())
        {
            return None;
        }

        let sample_positive = downscale_promql_exponential_buckets_to_map(
            &sample.positive,
            sample.scale,
            target_scale,
        )?;
        let sample_negative = downscale_promql_exponential_buckets_to_map(
            &sample.negative,
            sample.scale,
            target_scale,
        )?;

        count += sample.count;
        zero_count += sample.zero_count;
        add_promql_exponential_bucket_maps(&mut positive, sample_positive);
        add_promql_exponential_bucket_maps(&mut negative, sample_negative);
        sum = match (sum, sample.sum) {
            (Some(accumulated), Some(value)) => Some(accumulated + value),
            _ => None,
        };

        out.push(PromqlExponentialHistogramSample {
            timestamp_ms: sample.timestamp_ms,
            start_time_ms: None,
            count,
            sum,
            scale: target_scale,
            zero_threshold: first.zero_threshold,
            zero_count,
            positive: promql_exponential_bucket_map_to_buckets(positive.clone())?,
            negative: promql_exponential_bucket_map_to_buckets(negative.clone())?,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::NotCounterReset,
            stale: false,
        });
    }

    Some(out)
}

fn downscale_promql_exponential_buckets_to_map(
    buckets: &PromqlExponentialHistogramBuckets,
    source_scale: i32,
    target_scale: i32,
) -> Option<BTreeMap<i32, f64>> {
    if target_scale > source_scale {
        return None;
    }
    let shift = source_scale.checked_sub(target_scale)?;
    let divisor = 1i64.checked_shl(u32::try_from(shift).ok()?)?;
    let mut map = BTreeMap::new();
    for (source_index, count) in buckets.iter_counts() {
        if !count.is_finite() {
            return None;
        }
        let target_index = floor_div_i64_local(source_index, divisor);
        let target_index = i32::try_from(target_index).ok()?;
        *map.entry(target_index).or_insert(0.0) += count;
    }
    Some(map)
}

fn downscale_promql_exponential_bucket_map_to_map(
    map: &BTreeMap<i32, f64>,
    source_scale: i32,
    target_scale: i32,
) -> Option<BTreeMap<i32, f64>> {
    if target_scale > source_scale {
        return None;
    }
    let shift = source_scale.checked_sub(target_scale)?;
    let divisor = 1i64.checked_shl(u32::try_from(shift).ok()?)?;
    let mut out = BTreeMap::new();
    for (&source_index, &count) in map {
        if !count.is_finite() {
            return None;
        }
        let target_index = floor_div_i64_local(i64::from(source_index), divisor);
        let target_index = i32::try_from(target_index).ok()?;
        *out.entry(target_index).or_insert(0.0) += count;
    }
    Some(out)
}

fn counter_bucket_map_delta(
    previous: &BTreeMap<i32, f64>,
    current: &BTreeMap<i32, f64>,
    reset_hint: CounterResetHint,
) -> Option<BTreeMap<i32, f64>> {
    let mut keys = BTreeSet::new();
    keys.extend(previous.keys().copied());
    keys.extend(current.keys().copied());

    let mut out = BTreeMap::new();
    for key in keys {
        let previous_value = previous.get(&key).copied().unwrap_or(0.0);
        let current_value = current.get(&key).copied().unwrap_or(0.0);
        out.insert(
            key,
            counter_component_delta(previous_value, current_value, reset_hint)?,
        );
    }
    Some(out)
}

fn add_promql_exponential_bucket_maps(out: &mut BTreeMap<i32, f64>, input: BTreeMap<i32, f64>) {
    for (index, count) in input {
        *out.entry(index).or_insert(0.0) += count;
    }
}

fn promql_exponential_bucket_map_to_buckets(
    map: BTreeMap<i32, f64>,
) -> Option<PromqlExponentialHistogramBuckets> {
    if map.is_empty() {
        return Some(PromqlExponentialHistogramBuckets::empty());
    }
    Some(PromqlExponentialHistogramBuckets::from_sparse_counts(
        map.into_iter().collect(),
    ))
}

fn floor_div_i64_local(value: i64, divisor: i64) -> i64 {
    debug_assert!(divisor > 0);
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && value < 0 {
        quotient - 1
    } else {
        quotient
    }
}

pub(super) fn evaluate_native_exponential_histogram_quantile(
    function: &PromqlHistogramQuantile,
    series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let Some(sample) = input.samples.last() else {
            continue;
        };
        let Some(value) = exponential_histogram_quantile(function.quantile, sample) else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_native_exponential_histogram_fraction(
    function: &PromqlHistogramFraction,
    series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let Some(sample) = input.samples.last() else {
            continue;
        };
        let Some(value) = exponential_histogram_fraction(function.lower, function.upper, sample)
        else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_native_exponential_histogram_scalar_function(
    function: &PromqlHistogramScalarFunction,
    series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let Some(sample) = input.samples.last() else {
            continue;
        };
        let Some(value) =
            histogram_scalar_function_value(function.kind, sample.count, sample.sum, sample.stale)
        else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn exponential_histogram_quantile(
    quantile: f64,
    sample: &PromqlExponentialHistogramSample,
) -> Option<f64> {
    if quantile.is_nan() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }
    if sample.stale || !sample.count.is_finite() || sample.count < 0.0 {
        return None;
    }
    if !sample.zero_threshold.is_finite() || sample.zero_threshold < 0.0 {
        return None;
    }
    if sample.count == 0.0 {
        return Some(f64::NAN);
    }

    let base = promql_exponential_histogram_base(sample.scale);
    let zero_threshold = sample.zero_threshold;
    let mut buckets = Vec::<ExponentialQuantileBucket>::new();
    let mut has_negative_observations = false;
    let mut has_positive_observations = false;
    for (bucket_index, count) in sample.negative.iter_counts() {
        if !count.is_finite() {
            return None;
        }
        has_negative_observations |= count > 0.0;
        if count == 0.0 {
            continue;
        }
        let bucket_index = i32::try_from(bucket_index).ok()?;
        let lower = -base.powi(bucket_index.saturating_add(1));
        let upper = (-base.powi(bucket_index)).min(-zero_threshold);
        if upper < lower {
            if count > 0.0 {
                return None;
            }
            continue;
        }
        buckets.push(ExponentialQuantileBucket {
            lower,
            upper,
            count,
            exponential: true,
        });
    }
    for (bucket_index, count) in sample.positive.iter_counts() {
        if !count.is_finite() {
            return None;
        }
        has_positive_observations |= count > 0.0;
        if count == 0.0 {
            continue;
        }
        let bucket_index = i32::try_from(bucket_index).ok()?;
        let lower = base.powi(bucket_index).max(zero_threshold);
        let upper = base.powi(bucket_index.saturating_add(1));
        if upper < lower {
            if count > 0.0 {
                return None;
            }
            continue;
        }
        buckets.push(ExponentialQuantileBucket {
            lower,
            upper,
            count,
            exponential: true,
        });
    }
    if sample.zero_count > 0.0 {
        let lower = if has_negative_observations {
            -zero_threshold
        } else {
            0.0
        };
        let upper = if has_positive_observations {
            zero_threshold
        } else {
            0.0
        };
        buckets.push(ExponentialQuantileBucket {
            lower,
            upper,
            count: sample.zero_count,
            exponential: false,
        });
    }
    buckets.sort_by(|left, right| left.upper.total_cmp(&right.upper));

    let rank = quantile * sample.count;
    let mut cumulative = 0.0f64;
    for bucket in buckets {
        let bucket_count = bucket.count.max(0.0);
        let next = cumulative + bucket_count;
        if next >= rank {
            if bucket_count <= 0.0 {
                return Some(bucket.upper);
            }
            let fraction = (rank - cumulative) / bucket_count;
            if bucket.exponential {
                if bucket.lower > 0.0 && bucket.upper > bucket.lower {
                    return Some(bucket.lower * (bucket.upper / bucket.lower).powf(fraction));
                }
                if bucket.lower < bucket.upper && bucket.upper < 0.0 {
                    let lower_abs = -bucket.lower;
                    let upper_abs = -bucket.upper;
                    if lower_abs > upper_abs && upper_abs > 0.0 {
                        return Some(-(lower_abs * (upper_abs / lower_abs).powf(fraction)));
                    }
                }
            }
            return Some(bucket.lower + (bucket.upper - bucket.lower) * fraction);
        }
        cumulative = next;
    }

    buckets_last_upper(sample, base)
}

#[derive(Debug, Clone, Copy)]
struct ExponentialQuantileBucket {
    lower: f64,
    upper: f64,
    count: f64,
    exponential: bool,
}

fn exponential_histogram_fraction(
    lower: f64,
    upper: f64,
    sample: &PromqlExponentialHistogramSample,
) -> Option<f64> {
    if lower.is_nan() || upper.is_nan() {
        return None;
    }
    if lower >= upper {
        return Some(0.0);
    }
    if sample.stale || !sample.count.is_finite() || sample.count < 0.0 {
        return None;
    }
    if !sample.zero_threshold.is_finite() || sample.zero_threshold < 0.0 {
        return None;
    }
    if sample.count == 0.0 {
        return Some(f64::NAN);
    }

    let base = promql_exponential_histogram_base(sample.scale);
    let zero_threshold = sample.zero_threshold;
    let mut buckets = Vec::<HistogramFractionBucket>::new();
    let mut has_negative_observations = false;
    let mut has_positive_observations = false;
    for (bucket_index, count) in sample.negative.iter_counts() {
        if !count.is_finite() {
            return None;
        }
        has_negative_observations |= count > 0.0;
        if count == 0.0 {
            continue;
        }
        let bucket_index = i32::try_from(bucket_index).ok()?;
        let lower_bound = -base.powi(bucket_index.saturating_add(1));
        let upper_bound = (-base.powi(bucket_index)).min(-zero_threshold);
        if upper_bound < lower_bound {
            if count > 0.0 {
                return None;
            }
            continue;
        }
        buckets.push(HistogramFractionBucket {
            lower: lower_bound,
            upper: upper_bound,
            count,
            interpolation: HistogramFractionInterpolation::Exponential,
        });
    }
    for (bucket_index, count) in sample.positive.iter_counts() {
        if !count.is_finite() {
            return None;
        }
        has_positive_observations |= count > 0.0;
        if count == 0.0 {
            continue;
        }
        let bucket_index = i32::try_from(bucket_index).ok()?;
        let lower_bound = base.powi(bucket_index).max(zero_threshold);
        let upper_bound = base.powi(bucket_index.saturating_add(1));
        if upper_bound < lower_bound {
            if count > 0.0 {
                return None;
            }
            continue;
        }
        buckets.push(HistogramFractionBucket {
            lower: lower_bound,
            upper: upper_bound,
            count,
            interpolation: HistogramFractionInterpolation::Exponential,
        });
    }
    if sample.zero_count > 0.0 {
        buckets.push(HistogramFractionBucket {
            lower: if has_negative_observations {
                -zero_threshold
            } else {
                0.0
            },
            upper: if has_positive_observations {
                zero_threshold
            } else {
                0.0
            },
            count: sample.zero_count,
            interpolation: HistogramFractionInterpolation::Linear,
        });
    }

    histogram_fraction_from_buckets(lower, upper, sample.count, buckets)
}

fn buckets_last_upper(sample: &PromqlExponentialHistogramSample, base: f64) -> Option<f64> {
    let mut positive_upper = None;
    for (bucket_index, count) in sample.positive.iter_counts() {
        if count > 0.0 {
            let bucket_index = i32::try_from(bucket_index).ok()?;
            positive_upper = Some(base.powi(bucket_index.saturating_add(1)));
        }
    }
    if positive_upper.is_some() {
        return positive_upper;
    }
    if sample.zero_count > 0.0 {
        return Some(sample.zero_threshold);
    }
    for (bucket_index, count) in sample.negative.iter_counts() {
        if count > 0.0 {
            let bucket_index = i32::try_from(bucket_index).ok()?;
            return Some(-base.powi(bucket_index));
        }
    }
    None
}

fn promql_exponential_histogram_base(scale: i32) -> f64 {
    2.0f64.powf(2.0f64.powi(-scale))
}

pub(super) fn evaluate_native_histogram_quantile(
    function: &PromqlHistogramQuantile,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let Some(sample) = input.samples.last() else {
            continue;
        };
        if sample.stale
            || !sample.count.is_finite()
            || sample.bucket_counts.len() != sample.explicit_bounds.len().saturating_add(1)
        {
            continue;
        }

        let mut cumulative = 0.0f64;
        let mut buckets = Vec::with_capacity(sample.explicit_bounds.len().saturating_add(1));
        for (upper_bound, count) in sample
            .explicit_bounds
            .iter()
            .copied()
            .zip(sample.bucket_counts.iter().copied())
        {
            cumulative += count;
            buckets.push((upper_bound, cumulative));
        }
        buckets.push((f64::INFINITY, sample.count));
        let Some(value) = classic_histogram_quantile(function.quantile, buckets) else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_native_histogram_fraction(
    function: &PromqlHistogramFraction,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let Some(sample) = input.samples.last() else {
            continue;
        };
        let Some(value) = native_histogram_fraction(function.lower, function.upper, sample) else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_native_histogram_scalar_function(
    function: &PromqlHistogramScalarFunction,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let Some(sample) = input.samples.last() else {
            continue;
        };
        let Some(value) =
            histogram_scalar_function_value(function.kind, sample.count, sample.sum, sample.stale)
        else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn native_histogram_fraction(
    lower: f64,
    upper: f64,
    sample: &PromqlHistogramSample,
) -> Option<f64> {
    if lower.is_nan() || upper.is_nan() {
        return None;
    }
    if lower >= upper {
        return Some(0.0);
    }
    if sample.stale
        || !sample.count.is_finite()
        || sample.count < 0.0
        || sample.bucket_counts.len() != sample.explicit_bounds.len().saturating_add(1)
        || sample.bucket_counts.iter().any(|count| !count.is_finite())
        || sample
            .explicit_bounds
            .iter()
            .any(|bound| !bound.is_finite())
    {
        return None;
    }
    if sample.count == 0.0 {
        return Some(f64::NAN);
    }

    let mut buckets = Vec::with_capacity(sample.bucket_counts.len());
    let mut previous_bound = if sample
        .explicit_bounds
        .first()
        .is_some_and(|bound| *bound > 0.0)
    {
        0.0
    } else {
        f64::NEG_INFINITY
    };
    for (upper_bound, count) in sample
        .explicit_bounds
        .iter()
        .copied()
        .zip(sample.bucket_counts.iter().copied())
    {
        if upper_bound <= previous_bound {
            return None;
        }
        buckets.push(HistogramFractionBucket {
            lower: previous_bound,
            upper: upper_bound,
            count,
            interpolation: HistogramFractionInterpolation::Linear,
        });
        previous_bound = upper_bound;
    }
    let infinity_bucket = sample.bucket_counts.last().copied()?;
    buckets.push(HistogramFractionBucket {
        lower: previous_bound,
        upper: f64::INFINITY,
        count: infinity_bucket,
        interpolation: HistogramFractionInterpolation::Linear,
    });

    histogram_fraction_from_buckets(lower, upper, sample.count, buckets)
}

#[derive(Debug, Clone, Copy)]
enum HistogramFractionInterpolation {
    Linear,
    Exponential,
}

#[derive(Debug, Clone, Copy)]
struct HistogramFractionBucket {
    lower: f64,
    upper: f64,
    count: f64,
    interpolation: HistogramFractionInterpolation,
}

fn histogram_fraction_from_buckets(
    lower: f64,
    upper: f64,
    total_count: f64,
    mut buckets: Vec<HistogramFractionBucket>,
) -> Option<f64> {
    if total_count == 0.0 {
        return Some(f64::NAN);
    }
    if !total_count.is_finite() || total_count < 0.0 {
        return None;
    }
    if lower >= upper {
        return Some(0.0);
    }
    buckets.sort_by(|left, right| left.upper.total_cmp(&right.upper));
    let lower_count = histogram_fraction_cumulative_count(&buckets, lower)?;
    let upper_count = histogram_fraction_cumulative_count(&buckets, upper)?;
    Some(((upper_count - lower_count) / total_count).clamp(0.0, 1.0))
}

fn histogram_fraction_cumulative_count(
    buckets: &[HistogramFractionBucket],
    boundary: f64,
) -> Option<f64> {
    if boundary.is_nan() {
        return None;
    }
    if boundary == f64::NEG_INFINITY {
        return Some(0.0);
    }

    let mut cumulative = 0.0f64;
    for bucket in buckets {
        if !bucket.count.is_finite() || bucket.count < 0.0 {
            return None;
        }
        if boundary >= bucket.upper {
            cumulative += bucket.count;
            continue;
        }
        if boundary <= bucket.lower {
            return Some(cumulative);
        }
        let fraction = histogram_fraction_bucket_portion(bucket, boundary)?;
        return Some(cumulative + bucket.count * fraction);
    }
    Some(cumulative)
}

fn histogram_fraction_bucket_portion(
    bucket: &HistogramFractionBucket,
    boundary: f64,
) -> Option<f64> {
    if !bucket.lower.is_finite() || !bucket.upper.is_finite() {
        return Some(0.0);
    }
    if bucket.upper <= bucket.lower {
        return Some(0.0);
    }
    let fraction = match bucket.interpolation {
        HistogramFractionInterpolation::Linear => {
            (boundary - bucket.lower) / (bucket.upper - bucket.lower)
        }
        HistogramFractionInterpolation::Exponential => {
            if bucket.lower > 0.0 && bucket.upper > bucket.lower && boundary > 0.0 {
                (boundary / bucket.lower).ln() / (bucket.upper / bucket.lower).ln()
            } else if bucket.lower < bucket.upper && bucket.upper < 0.0 && boundary < 0.0 {
                let lower_abs = -bucket.lower;
                let upper_abs = -bucket.upper;
                let boundary_abs = -boundary;
                (lower_abs / boundary_abs).ln() / (lower_abs / upper_abs).ln()
            } else {
                (boundary - bucket.lower) / (bucket.upper - bucket.lower)
            }
        }
    };
    Some(fraction.clamp(0.0, 1.0))
}

fn histogram_scalar_function_value(
    kind: PromqlHistogramScalarFunctionKind,
    count: f64,
    sum: Option<f64>,
    stale: bool,
) -> Option<f64> {
    if stale || !count.is_finite() || count < 0.0 {
        return None;
    }
    match kind {
        PromqlHistogramScalarFunctionKind::Count => Some(count),
        PromqlHistogramScalarFunctionKind::Sum => sum.filter(|value| value.is_finite()),
        PromqlHistogramScalarFunctionKind::Avg => {
            let sum = sum.filter(|value| value.is_finite())?;
            Some(sum / count)
        }
    }
}

pub(super) fn histogram_bucket_upper_bound(labels: &[(String, String)]) -> Option<f64> {
    let value = labels
        .iter()
        .find_map(|(key, value)| (key == "le").then_some(value.as_str()))?;
    if value == "+Inf" {
        return Some(f64::INFINITY);
    }
    let upper_bound = value.parse::<f64>().ok()?;
    upper_bound.is_finite().then_some(upper_bound)
}

pub(super) fn histogram_quantile_result_labels(
    labels: &[(String, String)],
) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL && key != "le")
        .cloned()
        .collect()
}

pub(super) fn classic_histogram_quantile(
    quantile: f64,
    mut buckets: Vec<(f64, f64)>,
) -> Option<f64> {
    if quantile.is_nan() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }

    buckets.sort_by(|(left, _), (right, _)| left.total_cmp(right));
    let mut compacted = Vec::<(f64, f64)>::with_capacity(buckets.len());
    for (upper_bound, count) in buckets {
        if upper_bound.is_nan() || !count.is_finite() {
            return None;
        }
        if let Some((last_upper_bound, last_count)) = compacted.last_mut()
            && *last_upper_bound == upper_bound
        {
            *last_count += count.max(0.0);
            continue;
        }
        compacted.push((upper_bound, count.max(0.0)));
    }

    if !compacted
        .last()
        .is_some_and(|(bound, _)| bound.is_infinite())
    {
        return Some(f64::NAN);
    }
    if compacted.len() < 2 {
        return Some(f64::NAN);
    }

    let mut previous_count = 0.0;
    for (_, count) in &mut compacted {
        if *count < previous_count {
            *count = previous_count;
        } else {
            previous_count = *count;
        }
    }

    let total = compacted.last().map(|(_, count)| *count)?;
    if total <= 0.0 {
        return Some(f64::NAN);
    }

    let rank = quantile * total;
    let bucket_index = compacted
        .iter()
        .position(|(_, count)| *count >= rank)
        .unwrap_or(compacted.len() - 1);
    if bucket_index == compacted.len() - 1 {
        return compacted
            .get(bucket_index.saturating_sub(1))
            .map(|(bound, _)| *bound);
    }

    let (upper_bound, upper_count) = compacted[bucket_index];
    if bucket_index == 0 && upper_bound <= 0.0 {
        return Some(upper_bound);
    }
    let (lower_bound, lower_count) = if bucket_index == 0 {
        (0.0, 0.0)
    } else {
        compacted[bucket_index - 1]
    };
    let bucket_count = upper_count - lower_count;
    if bucket_count <= 0.0 {
        return Some(upper_bound);
    }

    Some(lower_bound + (upper_bound - lower_bound) * (rank - lower_count) / bucket_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_bucket_map_to_buckets_preserves_sparse_span() {
        let mut map = BTreeMap::new();
        map.insert(0, 1.0);
        map.insert(100_000, 2.0);

        let buckets = promql_exponential_bucket_map_to_buckets(map).unwrap();
        let observed = buckets.iter_counts().collect::<Vec<_>>();

        assert!(
            buckets.counts.len() <= 2,
            "sparse exponential bucket maps must not expand empty spans into {} buckets",
            buckets.counts.len()
        );
        assert_eq!(observed, vec![(0, 1.0), (100_000, 2.0)]);
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
            PromqlExponentialHistogramBuckets::from_sparse_counts(vec![
                (0, 2.0),
                (high_index, 1.0),
            ]),
            PromqlExponentialHistogramBuckets::from_sparse_counts(vec![
                (0, 1.0),
                (high_index, 2.0),
            ]),
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
}
