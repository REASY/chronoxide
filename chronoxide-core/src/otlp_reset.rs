use std::collections::{BTreeMap, HashMap};

use crate::labels::SeriesRef;
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OtlpAggregationTemporality, downscale_exponential_histogram_buckets_to_map,
};

/// Stateful OTLP histogram reset detection shared by live ingestion and WAL replay.
///
/// State is keyed by the canonical series identity. Stale cumulative samples do
/// not replace the last observed non-stale state.
#[derive(Debug, Default)]
pub struct OtlpResetTracker {
    histogram: HashMap<SeriesRef, HistogramResetState>,
    exponential_histogram: HashMap<SeriesRef, ExponentialHistogramResetState>,
}

impl OtlpResetTracker {
    pub fn stamp_histogram(&mut self, series: SeriesRef, value: &mut HistogramValue) {
        value.metadata.reset_hint = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative => {
                if value.metadata.is_stale() {
                    CounterResetHint::Unknown
                } else {
                    let current = HistogramResetState::from_value(value);
                    let hint = self
                        .histogram
                        .get(&series)
                        .map(|previous| histogram_reset_hint(previous, &current))
                        .unwrap_or(CounterResetHint::Unknown);
                    self.histogram.insert(series, current);
                    hint
                }
            }
            OtlpAggregationTemporality::Delta => CounterResetHint::NotCounterReset,
            OtlpAggregationTemporality::Unspecified => CounterResetHint::Unknown,
        };
    }

    pub fn stamp_exponential_histogram(
        &mut self,
        series: SeriesRef,
        value: &mut ExponentialHistogramValue,
    ) {
        value.metadata.reset_hint = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative => {
                if value.metadata.is_stale() {
                    CounterResetHint::Unknown
                } else {
                    let current = ExponentialHistogramResetState::from_value(value);
                    let hint = self
                        .exponential_histogram
                        .get(&series)
                        .map(|previous| exponential_histogram_reset_hint(previous, &current))
                        .unwrap_or(CounterResetHint::Unknown);
                    self.exponential_histogram.insert(series, current);
                    hint
                }
            }
            OtlpAggregationTemporality::Delta => CounterResetHint::NotCounterReset,
            OtlpAggregationTemporality::Unspecified => CounterResetHint::Unknown,
        };
    }
}

#[derive(Debug, Clone)]
struct HistogramResetState {
    start_time_ms: Option<u64>,
    count: u64,
    sum: Option<f64>,
    explicit_bounds: Vec<f64>,
    bucket_counts: Vec<u64>,
}

impl HistogramResetState {
    fn from_value(value: &HistogramValue) -> Self {
        Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            explicit_bounds: value.explicit_bounds.clone(),
            bucket_counts: value.bucket_counts.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ExponentialHistogramResetState {
    start_time_ms: Option<u64>,
    count: u64,
    sum: Option<f64>,
    scale: i32,
    zero_threshold_bits: u64,
    zero_count: u64,
    positive: ExponentialHistogramBuckets,
    negative: ExponentialHistogramBuckets,
}

impl ExponentialHistogramResetState {
    fn from_value(value: &ExponentialHistogramValue) -> Self {
        Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            scale: value.scale,
            zero_threshold_bits: value.zero_threshold.to_bits(),
            zero_count: value.zero_count,
            positive: value.positive.clone(),
            negative: value.negative.clone(),
        }
    }
}

fn histogram_reset_hint(
    previous: &HistogramResetState,
    current: &HistogramResetState,
) -> CounterResetHint {
    if start_time_advanced(previous.start_time_ms, current.start_time_ms) {
        return CounterResetHint::CounterReset;
    }
    if previous.explicit_bounds != current.explicit_bounds {
        return CounterResetHint::Unknown;
    }
    if current.count < previous.count || optional_f64_decreased(previous.sum, current.sum) {
        return CounterResetHint::CounterReset;
    }
    if previous.bucket_counts.len() != current.bucket_counts.len() {
        return CounterResetHint::Unknown;
    }
    if previous
        .bucket_counts
        .iter()
        .zip(&current.bucket_counts)
        .any(|(previous, current)| current < previous)
    {
        return CounterResetHint::CounterReset;
    }
    CounterResetHint::NotCounterReset
}

fn exponential_histogram_reset_hint(
    previous: &ExponentialHistogramResetState,
    current: &ExponentialHistogramResetState,
) -> CounterResetHint {
    if start_time_advanced(previous.start_time_ms, current.start_time_ms) {
        return CounterResetHint::CounterReset;
    }
    if previous.zero_threshold_bits != current.zero_threshold_bits {
        return CounterResetHint::Unknown;
    }
    if current.count < previous.count
        || current.zero_count < previous.zero_count
        || optional_f64_decreased(previous.sum, current.sum)
    {
        return CounterResetHint::CounterReset;
    }

    let target_scale = previous.scale.min(current.scale);
    let Ok(previous_positive) = downscale_exponential_histogram_buckets_to_map(
        &previous.positive,
        previous.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(current_positive) = downscale_exponential_histogram_buckets_to_map(
        &current.positive,
        current.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(previous_negative) = downscale_exponential_histogram_buckets_to_map(
        &previous.negative,
        previous.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(current_negative) = downscale_exponential_histogram_buckets_to_map(
        &current.negative,
        current.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };

    if bucket_map_decreased(&previous_positive, &current_positive)
        || bucket_map_decreased(&previous_negative, &current_negative)
    {
        CounterResetHint::CounterReset
    } else {
        CounterResetHint::NotCounterReset
    }
}

fn start_time_advanced(previous: Option<u64>, current: Option<u64>) -> bool {
    matches!((previous, current), (Some(previous), Some(current)) if current > previous)
}

fn optional_f64_decreased(previous: Option<f64>, current: Option<f64>) -> bool {
    matches!((previous, current), (Some(previous), Some(current)) if current < previous)
}

fn bucket_map_decreased(previous: &BTreeMap<i32, u64>, current: &BTreeMap<i32, u64>) -> bool {
    previous
        .iter()
        .any(|(index, previous_count)| current.get(index).copied().unwrap_or(0) < *previous_count)
}
