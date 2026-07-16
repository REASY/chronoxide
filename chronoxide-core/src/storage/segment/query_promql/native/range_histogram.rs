use super::*;

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
        let labels = function_result_labels(&input.labels);
        let series_id = if input.labels_complete {
            segment_series_id(&labels)
        } else {
            input.metric_name_dropped_series_id.expect(
                "selective native range input must carry its complete-row metric-name-dropped identity",
            )
        };
        let mut result = PromqlHistogramSeries::new(series_id, shared_query_labels(labels));
        if !input.labels_complete {
            result.mark_labels_incomplete(Some(series_id));
        }
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

fn range_function_histogram_samples(
    samples: &[PromqlHistogramSample],
    range_start_ms: u64,
    range_end_ms: u64,
    include_range_start: bool,
) -> &[PromqlHistogramSample] {
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

pub(super) fn optional_f64_equal(left: Option<f64>, right: Option<f64>) -> bool {
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

pub(super) fn cumulative_delta_histogram_samples(
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

pub(in crate::storage::segment::query_promql) fn counter_component_delta(
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

pub(in crate::storage::segment::query_promql) fn counter_component_reset_adjustment(
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
