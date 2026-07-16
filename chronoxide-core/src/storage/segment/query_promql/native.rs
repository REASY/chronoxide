use super::*;

mod functions;
mod range_exponential;
mod range_histogram;

pub(in crate::storage::segment) use functions::{
    classic_histogram_quantile, evaluate_native_exponential_histogram_fraction,
    evaluate_native_exponential_histogram_quantile,
    evaluate_native_exponential_histogram_scalar_function, evaluate_native_histogram_fraction,
    evaluate_native_histogram_quantile, evaluate_native_histogram_scalar_function,
    histogram_bucket_upper_bound, histogram_quantile_result_labels,
};
pub(in crate::storage::segment) use range_exponential::{
    evaluate_exponential_histogram_range_function,
    evaluate_native_exponential_histogram_scalar_range_function,
};
pub(in crate::storage::segment) use range_histogram::{
    evaluate_histogram_quantile, evaluate_histogram_range_function,
    evaluate_native_histogram_scalar_range_function,
};

use range_exponential::{
    add_promql_exponential_bucket_maps, downscale_promql_exponential_bucket_map_to_map,
    downscale_promql_exponential_buckets_to_map, promql_exponential_bucket_map_to_buckets,
};
use range_histogram::optional_f64_equal;
pub(in crate::storage::segment::query_promql) use range_histogram::{
    counter_component_delta, counter_component_reset_adjustment,
};

#[cfg(test)]
use functions::{exponential_histogram_fraction, exponential_histogram_quantile};
#[cfg(test)]
use range_exponential::{counter_bucket_map_delta, cumulative_delta_exponential_histogram_samples};
#[cfg(test)]
use range_histogram::cumulative_delta_histogram_samples;

#[cfg(test)]
#[path = "native/tests.rs"]
mod tests;

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
        let labels = aggregation_group_query_labels(&aggregation.grouping, &result.labels);
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
        let labels = aggregation_group_query_labels(&aggregation.grouping, &result.labels);
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
        let labels = aggregation_group_query_labels(&aggregation.grouping, &result.labels);
        groups.entry(labels).or_default().observe();
    }
    for result in histogram_series {
        let Some(sample) = result.samples.last() else {
            continue;
        };
        if sample.stale {
            continue;
        }
        let labels = aggregation_group_query_labels(&aggregation.grouping, &result.labels);
        groups.entry(labels).or_default().observe();
    }
    for result in exponential_histogram_series {
        let Some(sample) = result.samples.last() else {
            continue;
        };
        if sample.stale {
            continue;
        }
        let labels = aggregation_group_query_labels(&aggregation.grouping, &result.labels);
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

    for (out, value) in coarsened_accumulated.iter_mut().zip(coarsened_sample) {
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
    aggregation_group_labels_from_pairs(
        grouping,
        labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    )
}

pub(super) fn aggregation_group_query_labels(
    grouping: &PromqlAggregationGrouping,
    labels: &QueryLabels,
) -> Vec<(String, String)> {
    aggregation_group_labels_from_pairs(grouping, labels.pairs())
}

fn aggregation_group_labels_from_pairs<'a>(
    grouping: &PromqlAggregationGrouping,
    labels: impl Iterator<Item = (&'a str, &'a str)>,
) -> Vec<(String, String)> {
    let mut out = match grouping {
        PromqlAggregationGrouping::All => Vec::new(),
        PromqlAggregationGrouping::By(grouping_labels) => labels
            .filter(|(key, _)| grouping_labels.iter().any(|label| label.as_str() == *key))
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
        PromqlAggregationGrouping::Without(grouping_labels) => labels
            .filter(|(key, _)| {
                *key != METRIC_NAME_LABEL
                    && !grouping_labels.iter().any(|label| label.as_str() == *key)
            })
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
    };
    out.sort();
    out
}
