use super::*;

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

pub(super) fn exponential_histogram_quantile(
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

pub(super) fn exponential_histogram_fraction(
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
