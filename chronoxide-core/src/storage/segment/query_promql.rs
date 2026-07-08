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
        let increase = match result.temporality {
            QueryResultTemporality::Delta => extrapolated_delta_projection_increase(
                &result.samples,
                result.counter_reset_hints(),
                result.sample_start_times(),
                range_start_ms,
                eval_time_ms,
            ),
            QueryResultTemporality::Mixed => None,
            QueryResultTemporality::Unknown | QueryResultTemporality::Cumulative => {
                extrapolated_counter_increase(
                    &result.samples,
                    result.counter_reset_hints(),
                    range_start_ms,
                    eval_time_ms,
                )
            }
        };
        let Some(increase) = increase else {
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

    Some(extrapolated_interval / sampled_interval)
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
    min: Option<f64>,
    max: Option<f64>,
}

impl AggregationAccumulator {
    fn observe(&mut self, value: f64) {
        self.sum += value;
        self.count = self.count.saturating_add(1);
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
    }

    fn value(&self, op: PromqlAggregationOp) -> Option<f64> {
        match op {
            PromqlAggregationOp::Sum => (self.count > 0).then_some(self.sum),
            PromqlAggregationOp::Count => (self.count > 0).then_some(self.count as f64),
            PromqlAggregationOp::Avg => (self.count > 0).then_some(self.sum / self.count as f64),
            PromqlAggregationOp::Min => self.min,
            PromqlAggregationOp::Max => self.max,
        }
    }
}

pub(super) fn evaluate_histogram_aggregation(
    aggregation: &PromqlAggregation,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<PromqlHistogramSeries> {
    if aggregation.op != PromqlAggregationOp::Sum {
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
        let Some(sample) = accumulator.into_sample(eval_time_ms) else {
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
    if aggregation.op != PromqlAggregationOp::Sum {
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
        let Some(sample) = accumulator.into_sample(eval_time_ms) else {
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
        if self.samples == 0 {
            self.valid = true;
            self.sum = Some(0.0);
        }
        self.samples = self.samples.saturating_add(1);

        if !self.valid
            || sample.stale
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

    fn into_sample(self, timestamp_ms: u64) -> Option<PromqlHistogramSample> {
        if !self.valid || self.samples == 0 {
            return None;
        }
        Some(PromqlHistogramSample {
            timestamp_ms,
            start_time_ms: None,
            count: self.count,
            sum: self.sum,
            explicit_bounds: self.explicit_bounds?,
            bucket_counts: self.bucket_counts,
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
        if self.samples == 0 {
            self.valid = true;
            self.sum = Some(0.0);
            self.zero_threshold = sample.zero_threshold;
            self.zero_threshold_bits = Some(sample.zero_threshold.to_bits());
            self.target_scale = Some(sample.scale);
        }
        self.samples = self.samples.saturating_add(1);

        if !self.valid
            || sample.stale
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

    fn into_sample(self, timestamp_ms: u64) -> Option<PromqlExponentialHistogramSample> {
        if !self.valid || self.samples == 0 {
            return None;
        }
        Some(PromqlExponentialHistogramSample {
            timestamp_ms,
            start_time_ms: None,
            count: self.count,
            sum: self.sum,
            scale: self.target_scale?,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            positive: promql_exponential_bucket_map_to_buckets(self.positive)?,
            negative: promql_exponential_bucket_map_to_buckets(self.negative)?,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::GaugeType,
            stale: false,
        })
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

pub(super) fn evaluate_histogram_range_function(
    function: &PromqlRangeFunction,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<PromqlHistogramSeries> {
    let mut out = Vec::new();
    let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
    for mut input in series {
        let (samples, effective_range_start_ms) =
            histogram_samples_after_last_stale(&mut input.samples, range_start_ms);
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

fn histogram_samples_after_last_stale(
    samples: &mut [PromqlHistogramSample],
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
    let mut out = Vec::new();
    let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
    for mut input in series {
        let (samples, effective_range_start_ms) =
            exponential_histogram_samples_after_last_stale(&mut input.samples, range_start_ms);
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
            for bucket in &mut increase.positive.counts {
                *bucket /= seconds;
            }
            for bucket in &mut increase.negative.counts {
                *bucket /= seconds;
            }
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

fn exponential_histogram_samples_after_last_stale(
    samples: &mut [PromqlExponentialHistogramSample],
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
    for (idx, count) in buckets.counts.iter().copied().enumerate() {
        if !count.is_finite() {
            return None;
        }
        let source_index = i64::from(buckets.offset).checked_add(i64::try_from(idx).ok()?)?;
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
    let Some((&offset, _)) = map.first_key_value() else {
        return Some(PromqlExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        });
    };
    let Some((&last, _)) = map.last_key_value() else {
        unreachable!("non-empty BTreeMap has a last key");
    };
    let span = i64::from(last)
        .checked_sub(i64::from(offset))
        .and_then(|span| span.checked_add(1))?;
    let span = usize::try_from(span).ok()?;
    let mut counts = vec![0.0f64; span];
    for (index, count) in map {
        let idx = usize::try_from(i64::from(index) - i64::from(offset)).ok()?;
        counts[idx] = count;
    }
    Some(PromqlExponentialHistogramBuckets { offset, counts })
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
    for (idx, count) in sample.negative.counts.iter().copied().enumerate() {
        if !count.is_finite() {
            return None;
        }
        has_negative_observations |= count > 0.0;
        let bucket_index = sample
            .negative
            .offset
            .checked_add(i32::try_from(idx).ok()?)?;
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
    for (idx, count) in sample.positive.counts.iter().copied().enumerate() {
        if !count.is_finite() {
            return None;
        }
        has_positive_observations |= count > 0.0;
        let bucket_index = sample
            .positive
            .offset
            .checked_add(i32::try_from(idx).ok()?)?;
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

fn buckets_last_upper(sample: &PromqlExponentialHistogramSample, base: f64) -> Option<f64> {
    sample
        .positive
        .counts
        .iter()
        .enumerate()
        .rev()
        .find_map(|(idx, count)| {
            (*count > 0.0).then(|| {
                let bucket_index = sample
                    .positive
                    .offset
                    .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
                base.powi(bucket_index.saturating_add(1))
            })
        })
        .or_else(|| (sample.zero_count > 0.0).then_some(sample.zero_threshold))
        .or_else(|| {
            sample
                .negative
                .counts
                .iter()
                .enumerate()
                .find_map(|(idx, count)| {
                    (*count > 0.0).then(|| {
                        let bucket_index = sample
                            .negative
                            .offset
                            .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
                        -base.powi(bucket_index)
                    })
                })
        })
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
