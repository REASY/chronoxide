use super::*;

impl HistogramResetState {
    pub(super) fn from_value(value: &HistogramValue) -> Self {
        Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            explicit_bounds: value.explicit_bounds.clone(),
            bucket_counts: value.bucket_counts.clone(),
        }
    }
}

impl ExponentialHistogramResetState {
    pub(super) fn from_value(value: &ExponentialHistogramValue) -> Self {
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

pub(super) fn histogram_reset_hint(
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

pub(super) fn exponential_histogram_reset_hint(
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

pub(super) fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
