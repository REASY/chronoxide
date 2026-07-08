use super::*;

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
        let Some(increase) = extrapolated_counter_increase(
            &result.samples,
            result.counter_reset_hints(),
            range_start_ms,
            eval_time_ms,
        ) else {
            continue;
        };
        let value = match function.kind {
            PromqlRangeFunctionKind::Increase => increase,
            PromqlRangeFunctionKind::Rate => {
                if function.range_ms == 0 {
                    continue;
                }
                increase / (function.range_ms as f64 / 1_000.0)
            }
        };
        if !value.is_finite() {
            continue;
        }
        let labels = function_result_labels(&result.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
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
    if last_ts <= first_ts || !first_value.is_finite() {
        return None;
    }

    let sampled_interval = (last_ts - first_ts) as f64 / 1_000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;
    let mut duration_to_start = first_ts.saturating_sub(range_start_ms) as f64 / 1_000.0;
    let duration_to_end = range_end_ms.saturating_sub(last_ts) as f64 / 1_000.0;

    if raw_increase > 0.0 && first_value >= 0.0 {
        let duration_to_zero = sampled_interval * (first_value / raw_increase);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let mut extrapolated_interval = sampled_interval;
    if duration_to_start >= extrapolation_threshold {
        extrapolated_interval += average_between_samples / 2.0;
    } else {
        extrapolated_interval += duration_to_start;
    }
    if duration_to_end >= extrapolation_threshold {
        extrapolated_interval += average_between_samples / 2.0;
    } else {
        extrapolated_interval += duration_to_end;
    }

    Some(raw_increase * (extrapolated_interval / sampled_interval))
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
    let mut groups = BTreeMap::<Vec<(String, String)>, AggregationAccumulator>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if !value.is_finite() {
            continue;
        }
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups.entry(labels).or_default().observe(value);
    }

    let mut out = Vec::new();
    for (labels, accumulator) in groups {
        let Some(value) = accumulator.value(aggregation.op) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

#[derive(Default)]
struct AggregationAccumulator {
    sum: f64,
    count: u64,
}

impl AggregationAccumulator {
    fn observe(&mut self, value: f64) {
        self.sum += value;
        self.count = self.count.saturating_add(1);
    }

    fn value(&self, op: PromqlAggregationOp) -> Option<f64> {
        match op {
            PromqlAggregationOp::Sum => (self.count > 0).then_some(self.sum),
            PromqlAggregationOp::Count => (self.count > 0).then_some(self.count as f64),
            PromqlAggregationOp::Avg => (self.count > 0).then_some(self.sum / self.count as f64),
        }
    }
}

fn aggregation_group_labels(
    grouping: &PromqlAggregationGrouping,
    labels: &[(String, String)],
) -> Vec<(String, String)> {
    let mut out = match grouping {
        PromqlAggregationGrouping::All => Vec::new(),
        PromqlAggregationGrouping::By(grouping_labels) => labels
            .iter()
            .filter(|(key, _)| {
                key != METRIC_NAME_LABEL && grouping_labels.iter().any(|label| label == key)
            })
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
            *last_count = (*last_count).max(count);
            continue;
        }
        compacted.push((upper_bound, count.max(0.0)));
    }

    if compacted.len() < 2
        || !compacted
            .last()
            .is_some_and(|(bound, _)| bound.is_infinite())
    {
        return None;
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
