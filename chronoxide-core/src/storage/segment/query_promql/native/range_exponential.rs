use super::*;
use crate::storage::floor_div_i64;

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
    let rate_range_seconds = if function.kind == PromqlRangeFunctionKind::Rate {
        if function.range_ms == 0 {
            return Vec::new();
        }
        Some(function.range_ms as f64 / 1_000.0)
    } else {
        None
    };
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
            rate_range_seconds,
        ) else {
            continue;
        };
        increase.timestamp_ms = eval_time_ms;
        increase.reset_hint = CounterResetHint::GaugeType;
        let (series_id, labels) = if input.labels_complete {
            let labels = function_result_labels(&input.labels);
            (segment_series_id(&labels), shared_query_labels(labels))
        } else {
            (
                input.metric_name_dropped_series_id.expect(
                    "selective native range input must carry its complete-row metric-name-dropped identity",
                ),
                input.labels,
            )
        };
        let mut result = PromqlExponentialHistogramSeries::new(series_id, labels);
        if !input.labels_complete {
            result.mark_labels_incomplete(Some(series_id));
        }
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

fn range_function_exponential_histogram_samples(
    samples: &[PromqlExponentialHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
    include_range_start: bool,
) -> &[PromqlExponentialHistogramSample] {
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
    rate_range_seconds: Option<f64>,
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
            let mut increase = delta_exponential_histogram_interval_increase(
                samples,
                range_start_ms,
                range_end_ms,
            )?;
            if let Some(range_seconds) = rate_range_seconds {
                divide_delta_exponential_histogram_rate(&mut increase, range_seconds)?;
            }
            return Some(increase);
        }
        let cumulative = cumulative_delta_exponential_histogram_samples(samples)?;
        let mut increase = cumulative_exponential_histogram_counter_increase(
            &cumulative,
            range_start_ms,
            range_start_before_epoch_ms,
            range_end_ms,
            None,
        )?;
        increase.sum = interval_sum;
        if let Some(range_seconds) = rate_range_seconds {
            divide_delta_exponential_histogram_rate(&mut increase, range_seconds)?;
        }
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
        rate_range_seconds,
    )
}

fn divide_delta_exponential_histogram_rate(
    increase: &mut PromqlExponentialHistogramSample,
    range_seconds: f64,
) -> Option<()> {
    if range_seconds <= 0.0 {
        return None;
    }
    // Delta projections have their own native/virtual contract, so preserve
    // their established divide-after-result order.
    increase.count /= range_seconds;
    increase.zero_count /= range_seconds;
    let scale = 1.0 / range_seconds;
    increase.positive.scale_counts(scale);
    increase.negative.scale_counts(scale);
    if let Some(sum) = &mut increase.sum {
        *sum /= range_seconds;
    }
    Some(())
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
    rate_range_seconds: Option<f64>,
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
        rate_range_seconds,
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

pub(super) fn cumulative_delta_exponential_histogram_samples(
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

pub(super) fn downscale_promql_exponential_buckets_to_map(
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

pub(super) fn downscale_promql_exponential_bucket_map_to_map(
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
        let target_index = floor_div_i64(source_index, divisor);
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

pub(super) fn counter_bucket_map_delta(
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

pub(super) fn add_promql_exponential_bucket_maps(
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

pub(super) fn promql_exponential_bucket_map_to_buckets(
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
