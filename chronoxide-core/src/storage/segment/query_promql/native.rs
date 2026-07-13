use super::*;

pub(in crate::storage::segment) fn evaluate_histogram_aggregation(
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

pub(in crate::storage::segment) fn evaluate_exponential_histogram_aggregation(
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

pub(in crate::storage::segment) fn native_histogram_aggregation_supported(
    op: &PromqlAggregationOp,
) -> bool {
    matches!(op, PromqlAggregationOp::Sum | PromqlAggregationOp::Avg)
}

pub(in crate::storage::segment) fn native_histogram_scalar_aggregation_supported(
    op: &PromqlAggregationOp,
) -> bool {
    matches!(op, PromqlAggregationOp::Count | PromqlAggregationOp::Group)
}

pub(in crate::storage::segment) fn evaluate_native_histogram_scalar_aggregation(
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
            || sample.bucket_counts.len() != sample.explicit_bounds.len().saturating_add(1)
        {
            self.valid = false;
            return;
        }

        if !add_custom_histogram_buckets(
            &mut self.explicit_bounds,
            &mut self.bucket_counts,
            &sample.explicit_bounds,
            &sample.bucket_counts,
        ) {
            self.valid = false;
            return;
        }

        self.count += sample.count;
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

pub(super) fn add_custom_histogram_buckets(
    accumulated_bounds: &mut Option<Arc<[f64]>>,
    accumulated_counts: &mut Vec<f64>,
    sample_bounds: &Arc<[f64]>,
    sample_counts: &[f64],
) -> bool {
    if !valid_custom_histogram_layout(sample_bounds.as_ref(), sample_counts) {
        return false;
    }

    let Some(existing_bounds) = accumulated_bounds.as_ref() else {
        *accumulated_bounds = Some(sample_bounds.clone());
        *accumulated_counts = sample_counts.to_vec();
        return true;
    };

    if existing_bounds.as_ref() == sample_bounds.as_ref()
        && accumulated_counts.len() == sample_counts.len()
    {
        for (out, value) in accumulated_counts
            .iter_mut()
            .zip(sample_counts.iter().copied())
        {
            *out += value;
        }
        return true;
    }

    let common_bounds =
        common_custom_histogram_bounds(existing_bounds.as_ref(), sample_bounds.as_ref());
    let Some(mut coarsened_accumulated) = coarsen_custom_histogram_counts(
        existing_bounds.as_ref(),
        accumulated_counts,
        &common_bounds,
    ) else {
        return false;
    };
    let Some(coarsened_sample) =
        coarsen_custom_histogram_counts(sample_bounds.as_ref(), sample_counts, &common_bounds)
    else {
        return false;
    };

    for (out, value) in coarsened_accumulated
        .iter_mut()
        .zip(coarsened_sample.into_iter())
    {
        *out += value;
    }
    *accumulated_bounds = Some(Arc::from(common_bounds.into_boxed_slice()));
    *accumulated_counts = coarsened_accumulated;
    true
}

fn valid_custom_histogram_layout(bounds: &[f64], counts: &[f64]) -> bool {
    if counts.len() != bounds.len().saturating_add(1) {
        return false;
    }
    valid_custom_histogram_bounds(bounds)
}

fn valid_custom_histogram_bounds(bounds: &[f64]) -> bool {
    let mut previous = None;
    for bound in bounds {
        if !bound.is_finite() {
            return false;
        }
        if previous.is_some_and(|previous| *bound <= previous) {
            return false;
        }
        previous = Some(*bound);
    }
    true
}

fn common_custom_histogram_bounds(left: &[f64], right: &[f64]) -> Vec<f64> {
    let mut out = Vec::new();
    let mut left_idx = 0;
    let mut right_idx = 0;
    while left_idx < left.len() && right_idx < right.len() {
        if left[left_idx] == right[right_idx] {
            out.push(left[left_idx]);
            left_idx += 1;
            right_idx += 1;
        } else if left[left_idx] < right[right_idx] {
            left_idx += 1;
        } else {
            right_idx += 1;
        }
    }
    out
}

fn coarsen_custom_histogram_counts(
    source_bounds: &[f64],
    source_counts: &[f64],
    target_bounds: &[f64],
) -> Option<Vec<f64>> {
    if !valid_custom_histogram_layout(source_bounds, source_counts)
        || !valid_custom_histogram_bounds(target_bounds)
    {
        return None;
    }

    let mut out = vec![0.0f64; target_bounds.len().saturating_add(1)];
    let mut target_idx = 0;
    for (source_idx, count) in source_counts.iter().copied().enumerate() {
        if source_idx < source_bounds.len() {
            let source_upper = source_bounds[source_idx];
            if target_idx < target_bounds.len() && target_bounds[target_idx] < source_upper {
                return None;
            }
            out[target_idx] += count;
            if target_idx < target_bounds.len() && source_upper == target_bounds[target_idx] {
                target_idx += 1;
            }
        } else {
            out[target_idx] += count;
        }
    }
    (target_idx == target_bounds.len()).then_some(out)
}

#[derive(Clone, Debug, Default, PartialEq)]
struct PromqlExponentialBucketMap {
    entries: Vec<(i32, f64)>,
}

impl PromqlExponentialBucketMap {
    fn clear(&mut self) {
        self.entries.clear();
    }

    fn scale_counts(&mut self, scale: f64) {
        for (_, count) in &mut self.entries {
            *count *= scale;
        }
    }
}

#[derive(Default)]
pub(super) struct ExponentialHistogramSumAccumulator {
    target_scale: Option<i32>,
    zero_threshold: f64,
    zero_threshold_bits: Option<u64>,
    count: f64,
    sum: Option<f64>,
    zero_count: f64,
    positive: PromqlExponentialBucketMap,
    negative: PromqlExponentialBucketMap,
    samples: u64,
    valid: bool,
}

impl ExponentialHistogramSumAccumulator {
    pub(super) fn observe(&mut self, sample: &PromqlExponentialHistogramSample) {
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

    pub(super) fn into_sample(
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

fn scale_promql_exponential_bucket_map(
    mut map: PromqlExponentialBucketMap,
    scale: f64,
) -> PromqlExponentialBucketMap {
    map.scale_counts(scale);
    map
}

pub(super) fn aggregation_group_labels(
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

pub(in crate::storage::segment) fn evaluate_histogram_quantile(
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

pub(in crate::storage::segment) fn evaluate_histogram_range_function(
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
    let range_start_before_epoch_ms =
        range_function_start_before_epoch_ms(eval_time_ms, function.range_ms);
    for input in series {
        let samples = range_function_histogram_samples(
            &input.samples,
            range_start_ms,
            eval_time_ms,
            range_start_before_epoch_ms > 0,
        );
        let Some(mut increase) = histogram_counter_increase(
            samples,
            range_start_ms,
            range_start_before_epoch_ms,
            eval_time_ms,
        ) else {
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

pub(in crate::storage::segment) fn evaluate_native_histogram_scalar_range_function(
    function: &PromqlRangeFunction,
    series: Vec<PromqlHistogramSeries>,
    range_start_ms: u64,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let samples =
            range_function_histogram_samples(&input.samples, range_start_ms, eval_time_ms, false);
        let (samples, _) = histogram_samples_after_last_stale(samples, range_start_ms);
        let value = match function.kind {
            PromqlRangeFunctionKind::Changes => histogram_changes_over_time(samples),
            PromqlRangeFunctionKind::Resets => histogram_resets_over_time(samples),
            _ => None,
        };
        let Some(value) = value else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn range_function_histogram_samples<'a>(
    samples: &'a [PromqlHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
    include_range_start: bool,
) -> &'a [PromqlHistogramSample] {
    let start_idx = samples.partition_point(|sample| {
        if include_range_start {
            sample.timestamp_ms < range_start_ms
        } else {
            sample.timestamp_ms <= range_start_ms
        }
    });
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

fn histogram_changes_over_time(samples: &[PromqlHistogramSample]) -> Option<f64> {
    let mut iter = samples.iter();
    let mut previous = iter.next()?;
    if previous.stale {
        return None;
    }

    let mut changes = 0u64;
    for current in iter {
        if current.stale {
            continue;
        }
        if histogram_sample_changed(previous, current)? {
            changes = changes.saturating_add(1);
        }
        previous = current;
    }

    Some(changes as f64)
}

fn histogram_sample_changed(
    previous: &PromqlHistogramSample,
    current: &PromqlHistogramSample,
) -> Option<bool> {
    if previous.count != current.count
        || !optional_f64_equal(previous.sum, current.sum)
        || previous.explicit_bounds != current.explicit_bounds
        || previous.bucket_counts.len() != current.bucket_counts.len()
    {
        return Some(true);
    }

    Some(
        previous
            .bucket_counts
            .iter()
            .zip(&current.bucket_counts)
            .any(|(previous_count, current_count)| previous_count != current_count),
    )
}

fn optional_f64_equal(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right || (left.is_nan() && right.is_nan()),
        (None, None) => true,
        _ => false,
    }
}

fn histogram_resets_over_time(samples: &[PromqlHistogramSample]) -> Option<f64> {
    let mut iter = samples.iter();
    let mut previous = iter.next()?;
    if !valid_histogram_reset_sample(previous) {
        return None;
    }

    let mut resets = 0u64;
    for current in iter {
        if !valid_histogram_reset_sample(current) {
            return None;
        }
        if histogram_sample_has_reset(previous, current)? {
            resets = resets.saturating_add(1);
        }
        previous = current;
    }

    Some(resets as f64)
}

fn valid_histogram_reset_sample(sample: &PromqlHistogramSample) -> bool {
    !sample.stale
        && sample.count.is_finite()
        && sample.bucket_counts.iter().all(|count| count.is_finite())
}

fn histogram_sample_has_reset(
    previous: &PromqlHistogramSample,
    current: &PromqlHistogramSample,
) -> Option<bool> {
    if current.count < previous.count {
        return Some(true);
    }

    let common_bounds = common_custom_histogram_bounds(
        previous.explicit_bounds.as_ref(),
        current.explicit_bounds.as_ref(),
    );
    let previous_counts = coarsen_custom_histogram_counts(
        previous.explicit_bounds.as_ref(),
        &previous.bucket_counts,
        &common_bounds,
    )?;
    let current_counts = coarsen_custom_histogram_counts(
        current.explicit_bounds.as_ref(),
        &current.bucket_counts,
        &common_bounds,
    )?;
    Some(
        previous_counts
            .into_iter()
            .zip(current_counts)
            .any(|(previous_count, current_count)| current_count < previous_count),
    )
}

fn histogram_counter_increase(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlHistogramSample> {
    if samples
        .iter()
        .all(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        let (non_stale_count, interval_sum) = validated_delta_interval_summary(
            samples.iter().map(|sample| {
                (
                    sample.stale,
                    sample.timestamp_ms,
                    sample.start_time_ms,
                    sample.sum,
                )
            }),
            range_start_ms,
            range_end_ms,
        )?;
        if non_stale_count == 1 {
            return delta_histogram_interval_increase(samples, range_start_ms, range_end_ms);
        }
        let cumulative = cumulative_delta_histogram_samples(samples)?;
        let mut increase = cumulative_histogram_counter_increase(
            &cumulative,
            range_start_ms,
            range_start_before_epoch_ms,
            range_end_ms,
        )?;
        increase.sum = interval_sum;
        return Some(increase);
    }
    if samples
        .iter()
        .any(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        return None;
    }

    cumulative_histogram_counter_increase(
        samples,
        range_start_ms,
        range_start_before_epoch_ms,
        range_end_ms,
    )
}

fn delta_histogram_interval_increase(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlHistogramSample> {
    if samples.is_empty() || range_end_ms <= range_start_ms {
        return None;
    }

    let mut count = 0.0f64;
    let mut bounds = None;
    let mut bucket_counts = Vec::new();
    let mut sum = Some(0.0f64);
    let mut used_interval = false;

    for sample in samples {
        if sample.stale {
            continue;
        }
        if !sample.count.is_finite() {
            return None;
        }

        let start_time_ms = sample.start_time_ms?;
        if start_time_ms >= sample.timestamp_ms {
            return None;
        }
        if !delta_interval_intersects(
            start_time_ms,
            sample.timestamp_ms,
            range_start_ms,
            range_end_ms,
        ) {
            continue;
        }

        count += sample.count;
        if !add_custom_histogram_buckets(
            &mut bounds,
            &mut bucket_counts,
            &sample.explicit_bounds,
            &sample.bucket_counts,
        ) {
            return None;
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
        explicit_bounds: bounds?,
        bucket_counts,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::GaugeType,
        stale: false,
    })
}

fn cumulative_histogram_counter_increase(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlHistogramSample> {
    let sample_count = samples.iter().filter(|sample| !sample.stale).count();
    if sample_count < 2 {
        return None;
    }
    let first = samples.iter().find(|sample| !sample.stale)?;
    let last = samples.iter().rfind(|sample| !sample.stale)?;

    let mut count = 0.0f64;
    let mut bounds = None;
    let mut bucket_counts = Vec::new();
    let mut sum = match (first.sum, last.sum) {
        (Some(first), Some(last)) => Some(last - first),
        _ => None,
    };
    let mut previous = first;

    for current in samples.iter().filter(|sample| !sample.stale).skip(1) {
        count += counter_component_delta(previous.count, current.count, current.reset_hint)?;

        let interval_bounds = common_custom_histogram_bounds(
            previous.explicit_bounds.as_ref(),
            current.explicit_bounds.as_ref(),
        );
        let previous_counts = coarsen_custom_histogram_counts(
            previous.explicit_bounds.as_ref(),
            &previous.bucket_counts,
            &interval_bounds,
        )?;
        let current_counts = coarsen_custom_histogram_counts(
            current.explicit_bounds.as_ref(),
            &current.bucket_counts,
            &interval_bounds,
        )?;
        let mut interval_counts = Vec::with_capacity(interval_bounds.len().saturating_add(1));
        for (previous_bucket, current_bucket) in previous_counts.into_iter().zip(current_counts) {
            interval_counts.push(counter_component_delta(
                previous_bucket,
                current_bucket,
                current.reset_hint,
            )?);
        }
        let interval_bounds = Arc::from(interval_bounds.into_boxed_slice());
        if !add_custom_histogram_buckets(
            &mut bounds,
            &mut bucket_counts,
            &interval_bounds,
            &interval_counts,
        ) {
            return None;
        }
        sum = match (sum, previous.sum, current.sum) {
            (Some(accumulated), Some(previous_sum), Some(current_sum)) => {
                counter_component_reset_adjustment(previous_sum, current_sum, current.reset_hint)
                    .map(|adjustment| accumulated + adjustment)
            }
            _ => None,
        };
        previous = current;
    }

    let factor = counter_extrapolation_factor(
        sample_count,
        first.timestamp_ms,
        first.count,
        last.timestamp_ms,
        count,
        range_start_ms,
        range_start_before_epoch_ms,
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
        explicit_bounds: bounds?,
        bucket_counts,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::GaugeType,
        stale: false,
    })
}

fn cumulative_delta_histogram_samples(
    samples: &[PromqlHistogramSample],
) -> Option<Vec<PromqlHistogramSample>> {
    let non_stale_count = samples.iter().filter(|sample| !sample.stale).count();
    if non_stale_count == 0 {
        return None;
    }

    let mut count = 0.0f64;
    let mut bounds = None;
    let mut bucket_counts = Vec::new();
    let mut sum = Some(0.0f64);
    let mut out = Vec::with_capacity(non_stale_count);
    let mut detect_reset_at_fragment_start = false;

    for sample in samples {
        if sample.stale {
            count = 0.0;
            bounds = None;
            bucket_counts.clear();
            sum = Some(0.0);
            detect_reset_at_fragment_start = true;
            continue;
        }
        if !sample.count.is_finite() {
            return None;
        }

        count += sample.count;
        if !add_custom_histogram_buckets(
            &mut bounds,
            &mut bucket_counts,
            &sample.explicit_bounds,
            &sample.bucket_counts,
        ) {
            return None;
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
            explicit_bounds: bounds.clone()?,
            bucket_counts: bucket_counts.clone(),
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: if detect_reset_at_fragment_start {
                CounterResetHint::Unknown
            } else {
                CounterResetHint::NotCounterReset
            },
            stale: false,
        });
        detect_reset_at_fragment_start = false;
    }

    Some(out)
}

pub(super) fn counter_component_delta(
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

pub(super) fn counter_component_reset_adjustment(
    previous: f64,
    current: f64,
    reset_hint: CounterResetHint,
) -> Option<f64> {
    match reset_hint {
        CounterResetHint::CounterReset => Some(previous),
        CounterResetHint::NotCounterReset => {
            (!(previous.is_finite() && current.is_finite() && current < previous)).then_some(0.0)
        }
        CounterResetHint::Unknown => Some(if current < previous { previous } else { 0.0 }),
        CounterResetHint::GaugeType => None,
    }
}

pub(in crate::storage::segment) fn evaluate_exponential_histogram_range_function(
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
    let range_start_before_epoch_ms =
        range_function_start_before_epoch_ms(eval_time_ms, function.range_ms);
    for input in series {
        let samples = range_function_exponential_histogram_samples(
            &input.samples,
            range_start_ms,
            eval_time_ms,
            range_start_before_epoch_ms > 0,
        );
        let Some(mut increase) = exponential_histogram_counter_increase(
            samples,
            range_start_ms,
            range_start_before_epoch_ms,
            eval_time_ms,
        ) else {
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

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_scalar_range_function(
    function: &PromqlRangeFunction,
    series: Vec<PromqlExponentialHistogramSeries>,
    range_start_ms: u64,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for input in series {
        let samples = range_function_exponential_histogram_samples(
            &input.samples,
            range_start_ms,
            eval_time_ms,
            false,
        );
        let (samples, _) = exponential_histogram_samples_after_last_stale(samples, range_start_ms);
        let value = match function.kind {
            PromqlRangeFunctionKind::Changes => exponential_histogram_changes_over_time(samples),
            PromqlRangeFunctionKind::Resets => exponential_histogram_resets_over_time(samples),
            _ => None,
        };
        let Some(value) = value else {
            continue;
        };

        let labels = function_result_labels(&input.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn range_function_exponential_histogram_samples<'a>(
    samples: &'a [PromqlExponentialHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
    include_range_start: bool,
) -> &'a [PromqlExponentialHistogramSample] {
    let start_idx = samples.partition_point(|sample| {
        if include_range_start {
            sample.timestamp_ms < range_start_ms
        } else {
            sample.timestamp_ms <= range_start_ms
        }
    });
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

fn exponential_histogram_changes_over_time(
    samples: &[PromqlExponentialHistogramSample],
) -> Option<f64> {
    let mut iter = samples.iter();
    let mut previous = iter.next()?;
    if previous.stale {
        return None;
    }

    let mut changes = 0u64;
    for current in iter {
        if current.stale {
            continue;
        }
        if exponential_histogram_sample_changed(previous, current) {
            changes = changes.saturating_add(1);
        }
        previous = current;
    }

    Some(changes as f64)
}

fn exponential_histogram_sample_changed(
    previous: &PromqlExponentialHistogramSample,
    current: &PromqlExponentialHistogramSample,
) -> bool {
    previous.count != current.count
        || !optional_f64_equal(previous.sum, current.sum)
        || previous.scale != current.scale
        || previous.zero_threshold.to_bits() != current.zero_threshold.to_bits()
        || previous.zero_count != current.zero_count
        || exponential_histogram_bucket_map(&previous.positive)
            != exponential_histogram_bucket_map(&current.positive)
        || exponential_histogram_bucket_map(&previous.negative)
            != exponential_histogram_bucket_map(&current.negative)
}

fn exponential_histogram_bucket_map(
    buckets: &PromqlExponentialHistogramBuckets,
) -> BTreeMap<i64, f64> {
    buckets.iter_counts().collect()
}

fn exponential_histogram_resets_over_time(
    samples: &[PromqlExponentialHistogramSample],
) -> Option<f64> {
    let mut iter = samples.iter();
    let mut previous = iter.next()?;
    if !valid_exponential_histogram_reset_sample(previous) {
        return None;
    }

    let mut resets = 0u64;
    for current in iter {
        if !valid_exponential_histogram_reset_sample(current) {
            return None;
        }
        if exponential_histogram_sample_has_reset(previous, current)? {
            resets = resets.saturating_add(1);
        }
        previous = current;
    }

    Some(resets as f64)
}

fn valid_exponential_histogram_reset_sample(sample: &PromqlExponentialHistogramSample) -> bool {
    !sample.stale
        && sample.count.is_finite()
        && sample.zero_count.is_finite()
        && sample
            .positive
            .iter_counts()
            .all(|(_, count)| count.is_finite())
        && sample
            .negative
            .iter_counts()
            .all(|(_, count)| count.is_finite())
}

fn exponential_histogram_sample_has_reset(
    previous: &PromqlExponentialHistogramSample,
    current: &PromqlExponentialHistogramSample,
) -> Option<bool> {
    if current.count < previous.count
        || current.zero_count < previous.zero_count
        || current.scale > previous.scale
        || current.zero_threshold < previous.zero_threshold
    {
        return Some(true);
    }

    let target_scale = previous.scale.min(current.scale);
    let previous_positive = downscale_promql_exponential_buckets_to_map(
        &previous.positive,
        previous.scale,
        target_scale,
    )?;
    let current_positive = downscale_promql_exponential_buckets_to_map(
        &current.positive,
        current.scale,
        target_scale,
    )?;
    if exponential_histogram_bucket_map_decreased(&previous_positive, &current_positive) {
        return Some(true);
    }

    let previous_negative = downscale_promql_exponential_buckets_to_map(
        &previous.negative,
        previous.scale,
        target_scale,
    )?;
    let current_negative = downscale_promql_exponential_buckets_to_map(
        &current.negative,
        current.scale,
        target_scale,
    )?;
    Some(exponential_histogram_bucket_map_decreased(
        &previous_negative,
        &current_negative,
    ))
}

fn exponential_histogram_bucket_map_decreased(
    previous: &PromqlExponentialBucketMap,
    current: &PromqlExponentialBucketMap,
) -> bool {
    let mut current_idx = 0;
    previous.entries.iter().any(|&(index, previous_count)| {
        while current_idx < current.entries.len() && current.entries[current_idx].0 < index {
            current_idx += 1;
        }
        let current_count = current
            .entries
            .get(current_idx)
            .filter(|(current_index, _)| *current_index == index)
            .map(|(_, count)| *count)
            .unwrap_or(0.0);
        current_count < previous_count
    })
}

fn exponential_histogram_counter_increase(
    samples: &[PromqlExponentialHistogramSample],
    range_start_ms: u64,
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlExponentialHistogramSample> {
    if samples
        .iter()
        .all(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        let (non_stale_count, interval_sum) = validated_delta_interval_summary(
            samples.iter().map(|sample| {
                (
                    sample.stale,
                    sample.timestamp_ms,
                    sample.start_time_ms,
                    sample.sum,
                )
            }),
            range_start_ms,
            range_end_ms,
        )?;
        if non_stale_count == 1 {
            return delta_exponential_histogram_interval_increase(
                samples,
                range_start_ms,
                range_end_ms,
            );
        }
        let cumulative = cumulative_delta_exponential_histogram_samples(samples)?;
        let mut increase = cumulative_exponential_histogram_counter_increase(
            &cumulative,
            range_start_ms,
            range_start_before_epoch_ms,
            range_end_ms,
        )?;
        increase.sum = interval_sum;
        return Some(increase);
    }
    if samples
        .iter()
        .any(|sample| sample.temporality == OtlpAggregationTemporality::Delta)
    {
        return None;
    }

    cumulative_exponential_histogram_counter_increase(
        samples,
        range_start_ms,
        range_start_before_epoch_ms,
        range_end_ms,
    )
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
    let mut positive = PromqlExponentialBucketMap::default();
    let mut negative = PromqlExponentialBucketMap::default();
    let mut sum = Some(0.0f64);
    let mut used_interval = false;

    for sample in samples {
        if sample.stale {
            continue;
        }
        if !sample.count.is_finite() || !sample.zero_count.is_finite() {
            return None;
        }

        let start_time_ms = sample.start_time_ms?;
        if start_time_ms >= sample.timestamp_ms {
            return None;
        }
        if !delta_interval_intersects(
            start_time_ms,
            sample.timestamp_ms,
            range_start_ms,
            range_end_ms,
        ) {
            continue;
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
            None => target_scale = Some(sample.scale),
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
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
) -> Option<PromqlExponentialHistogramSample> {
    let sample_count = samples.iter().filter(|sample| !sample.stale).count();
    if sample_count < 2 {
        return None;
    }
    let first = samples.iter().find(|sample| !sample.stale)?;
    let last = samples.iter().rfind(|sample| !sample.stale)?;
    if samples
        .iter()
        .filter(|sample| !sample.stale)
        .any(|sample| sample.zero_threshold.to_bits() != first.zero_threshold.to_bits())
    {
        return None;
    }

    let target_scale = samples
        .iter()
        .filter(|sample| !sample.stale)
        .map(|sample| sample.scale)
        .min()?;
    let mut count = 0.0f64;
    let mut zero_count = 0.0f64;
    let mut positive = PromqlExponentialBucketMap::default();
    let mut negative = PromqlExponentialBucketMap::default();
    let mut sum = match (first.sum, last.sum) {
        (Some(first), Some(last)) => Some(last - first),
        _ => None,
    };
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

    for current in samples.iter().filter(|sample| !sample.stale).skip(1) {
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
            (Some(accumulated), Some(previous_sum), Some(current_sum)) => {
                counter_component_reset_adjustment(previous_sum, current_sum, current.reset_hint)
                    .map(|adjustment| accumulated + adjustment)
            }
            _ => None,
        };
        previous = current;
        previous_positive = current_positive;
        previous_negative = current_negative;
    }

    let factor = counter_extrapolation_factor(
        sample_count,
        first.timestamp_ms,
        first.count,
        last.timestamp_ms,
        count,
        range_start_ms,
        range_start_before_epoch_ms,
        range_end_ms,
    )?;

    count *= factor;
    zero_count *= factor;
    positive.scale_counts(factor);
    negative.scale_counts(factor);
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
    let non_stale_count = samples.iter().filter(|sample| !sample.stale).count();
    let first = samples.iter().find(|sample| !sample.stale)?;
    if samples
        .iter()
        .filter(|sample| !sample.stale)
        .any(|sample| sample.zero_threshold.to_bits() != first.zero_threshold.to_bits())
    {
        return None;
    }

    let target_scale = samples
        .iter()
        .filter(|sample| !sample.stale)
        .map(|sample| sample.scale)
        .min()?;
    let mut count = 0.0f64;
    let mut zero_count = 0.0f64;
    let mut positive = PromqlExponentialBucketMap::default();
    let mut negative = PromqlExponentialBucketMap::default();
    let mut sum = Some(0.0f64);
    let mut out = Vec::with_capacity(non_stale_count);
    let mut detect_reset_at_fragment_start = false;

    for sample in samples {
        if sample.stale {
            count = 0.0;
            zero_count = 0.0;
            positive.clear();
            negative.clear();
            sum = Some(0.0);
            detect_reset_at_fragment_start = true;
            continue;
        }
        if !sample.count.is_finite() || !sample.zero_count.is_finite() {
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
            reset_hint: if detect_reset_at_fragment_start {
                CounterResetHint::Unknown
            } else {
                CounterResetHint::NotCounterReset
            },
            stale: false,
        });
        detect_reset_at_fragment_start = false;
    }

    Some(out)
}

fn downscale_promql_exponential_buckets_to_map(
    buckets: &PromqlExponentialHistogramBuckets,
    source_scale: i32,
    target_scale: i32,
) -> Option<PromqlExponentialBucketMap> {
    downscale_promql_exponential_bucket_iter(
        buckets.iter_counts(),
        buckets.counts.len().max(buckets.sparse_counts.len()),
        source_scale,
        target_scale,
    )
}

fn downscale_promql_exponential_bucket_map_to_map(
    map: &PromqlExponentialBucketMap,
    source_scale: i32,
    target_scale: i32,
) -> Option<PromqlExponentialBucketMap> {
    downscale_promql_exponential_bucket_iter(
        map.entries
            .iter()
            .map(|&(index, count)| (i64::from(index), count)),
        map.entries.len(),
        source_scale,
        target_scale,
    )
}

fn downscale_promql_exponential_bucket_iter(
    buckets: impl Iterator<Item = (i64, f64)>,
    capacity: usize,
    source_scale: i32,
    target_scale: i32,
) -> Option<PromqlExponentialBucketMap> {
    if target_scale > source_scale {
        return None;
    }
    let shift = source_scale.checked_sub(target_scale)?;
    let divisor = 1i64
        .checked_shl(u32::try_from(shift).ok()?)
        .filter(|divisor| *divisor > 0)?;
    let mut entries = Vec::<(i32, f64)>::with_capacity(capacity);
    let mut previous_source_index = None;
    for (source_index, count) in buckets {
        if previous_source_index.is_some_and(|previous| source_index <= previous) {
            return None;
        }
        previous_source_index = Some(source_index);
        let target_index = floor_div_i64_local(source_index, divisor);
        let target_index = i32::try_from(target_index).ok()?;
        if let Some((last_index, last_count)) = entries.last_mut()
            && *last_index == target_index
        {
            *last_count += count;
        } else {
            entries.push((target_index, count));
        }
    }
    Some(PromqlExponentialBucketMap { entries })
}

fn counter_bucket_map_delta(
    previous: &PromqlExponentialBucketMap,
    current: &PromqlExponentialBucketMap,
    reset_hint: CounterResetHint,
) -> Option<PromqlExponentialBucketMap> {
    let mut entries =
        Vec::with_capacity(previous.entries.len().saturating_add(current.entries.len()));
    let mut previous_idx = 0;
    let mut current_idx = 0;
    while previous_idx < previous.entries.len() || current_idx < current.entries.len() {
        let (index, previous_value, current_value) = match (
            previous.entries.get(previous_idx),
            current.entries.get(current_idx),
        ) {
            (Some(&(previous_index, previous_value)), Some(&(current_index, current_value))) => {
                match previous_index.cmp(&current_index) {
                    std::cmp::Ordering::Less => {
                        previous_idx += 1;
                        (previous_index, previous_value, 0.0)
                    }
                    std::cmp::Ordering::Equal => {
                        previous_idx += 1;
                        current_idx += 1;
                        (previous_index, previous_value, current_value)
                    }
                    std::cmp::Ordering::Greater => {
                        current_idx += 1;
                        (current_index, 0.0, current_value)
                    }
                }
            }
            (Some(&(previous_index, previous_value)), None) => {
                previous_idx += 1;
                (previous_index, previous_value, 0.0)
            }
            (None, Some(&(current_index, current_value))) => {
                current_idx += 1;
                (current_index, 0.0, current_value)
            }
            (None, None) => break,
        };
        entries.push((
            index,
            counter_component_delta(previous_value, current_value, reset_hint)?,
        ));
    }
    Some(PromqlExponentialBucketMap { entries })
}

fn add_promql_exponential_bucket_maps(
    out: &mut PromqlExponentialBucketMap,
    input: PromqlExponentialBucketMap,
) {
    if out.entries.is_empty() {
        *out = input;
        return;
    }
    if input.entries.is_empty() {
        return;
    }

    let previous = std::mem::take(&mut out.entries);
    let mut entries = Vec::with_capacity(previous.len().saturating_add(input.entries.len()));
    let mut previous_idx = 0;
    let mut input_idx = 0;
    while previous_idx < previous.len() && input_idx < input.entries.len() {
        let previous_entry = previous[previous_idx];
        let input_entry = input.entries[input_idx];
        match previous_entry.0.cmp(&input_entry.0) {
            std::cmp::Ordering::Less => {
                entries.push(previous_entry);
                previous_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                entries.push((previous_entry.0, previous_entry.1 + input_entry.1));
                previous_idx += 1;
                input_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                entries.push(input_entry);
                input_idx += 1;
            }
        }
    }
    entries.extend_from_slice(&previous[previous_idx..]);
    entries.extend_from_slice(&input.entries[input_idx..]);
    out.entries = entries;
}

fn promql_exponential_bucket_map_to_buckets(
    mut map: PromqlExponentialBucketMap,
) -> Option<PromqlExponentialHistogramBuckets> {
    map.entries.retain(|(_, count)| *count != 0.0);
    if map.entries.is_empty() {
        return Some(PromqlExponentialHistogramBuckets::empty());
    }
    Some(PromqlExponentialHistogramBuckets {
        offset: 0,
        counts: Vec::new(),
        sparse_counts: map.entries,
    })
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

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_quantile(
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

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_fraction(
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

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_scalar_function(
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

pub(in crate::storage::segment) fn evaluate_native_histogram_quantile(
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

pub(in crate::storage::segment) fn evaluate_native_histogram_fraction(
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

pub(in crate::storage::segment) fn evaluate_native_histogram_scalar_function(
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
    if lower == f64::NEG_INFINITY && upper == f64::INFINITY {
        return Some(1.0);
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
    if stale {
        return None;
    }
    match kind {
        PromqlHistogramScalarFunctionKind::Count => Some(count),
        PromqlHistogramScalarFunctionKind::Sum => sum,
        PromqlHistogramScalarFunctionKind::Avg => {
            let sum = sum?;
            Some(sum / count)
        }
    }
}

pub(in crate::storage::segment) fn histogram_bucket_upper_bound(
    labels: &[(String, String)],
) -> Option<f64> {
    let value = labels
        .iter()
        .find_map(|(key, value)| (key == "le").then_some(value.as_str()))?;
    if value == "+Inf" {
        return Some(f64::INFINITY);
    }
    let upper_bound = value.parse::<f64>().ok()?;
    upper_bound.is_finite().then_some(upper_bound)
}

pub(in crate::storage::segment) fn histogram_quantile_result_labels(
    labels: &[(String, String)],
) -> Vec<(String, String)> {
    labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL && key != "le")
        .cloned()
        .collect()
}

pub(in crate::storage::segment) fn classic_histogram_quantile(
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
        let sample =
            |timestamp_ms: u64, count: f64, stale: bool| PromqlExponentialHistogramSample {
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
            let via_map = downscale_promql_exponential_bucket_map_to_map(
                &source_map,
                source_scale,
                target_scale,
            );
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
            counter_bucket_map_delta(&previous, &current, CounterResetHint::NotCounterReset,)
                .is_none(),
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
                &counter_bucket_map_delta(&previous, &current, CounterResetHint::CounterReset)
                    .unwrap()
            ),
            BTreeMap::from([(-3, 8.0), (-1, 0.0), (0, 4.0), (2, 3.0)])
        );
        assert!(
            counter_bucket_map_delta(&previous, &current, CounterResetHint::GaugeType).is_none()
        );

        let non_finite = test_bucket_map([(0, f64::INFINITY)]);
        assert!(
            counter_bucket_map_delta(&non_finite, &current, CounterResetHint::Unknown).is_none()
        );
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

    fn test_bucket_map(
        entries: impl IntoIterator<Item = (i32, f64)>,
    ) -> PromqlExponentialBucketMap {
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
