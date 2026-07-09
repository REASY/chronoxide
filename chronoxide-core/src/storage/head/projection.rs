use super::*;

pub(super) fn series_samples_len(samples: &SeriesSamples) -> usize {
    match samples {
        SeriesSamples::Float { samples, .. } => samples.len(),
        SeriesSamples::Int64 { samples, .. } => samples.len(),
        SeriesSamples::Histogram { samples } => samples.len(),
        SeriesSamples::ExponentialHistogram { samples } => samples.len(),
        SeriesSamples::Summary { samples } => samples.len(),
    }
}

pub(super) fn project_head_series_samples(
    projection: &SegmentProjection,
    base_labels: &[(String, String)],
    samples: SeriesSamples,
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Vec<SegmentQueryResult>> {
    let metric_name = base_labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
        .unwrap_or_default();
    let mut projected = BTreeMap::new();

    match (projection, samples) {
        (SegmentProjection::AllPromql { .. }, SeriesSamples::Histogram { samples }) => {
            project_head_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            project_head_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            for result in project_head_series_samples(
                &SegmentProjection::HistogramBucket {
                    le: BucketLeFilter::All,
                    exponential_histogram_boundaries: Vec::new(),
                },
                base_labels,
                SeriesSamples::Histogram { samples },
                start_ms,
                end_ms,
            )? {
                projected.insert(result.series_id, result);
            }
        }
        (
            SegmentProjection::AllPromql {
                exponential_histogram_boundaries,
            },
            SeriesSamples::ExponentialHistogram { samples },
        ) => {
            project_head_exponential_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            project_head_exponential_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            for result in project_head_series_samples(
                &SegmentProjection::HistogramBucket {
                    le: BucketLeFilter::All,
                    exponential_histogram_boundaries: exponential_histogram_boundaries.clone(),
                },
                base_labels,
                SeriesSamples::ExponentialHistogram { samples },
                start_ms,
                end_ms,
            )? {
                projected.insert(result.series_id, result);
            }
        }
        (SegmentProjection::AllPromql { .. }, SeriesSamples::Summary { samples }) => {
            project_head_summary_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            project_head_summary_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            for result in project_head_series_samples(
                &SegmentProjection::SummaryQuantile { quantile: None },
                base_labels,
                SeriesSamples::Summary { samples },
                start_ms,
                end_ms,
            )? {
                projected.insert(result.series_id, result);
            }
        }
        (SegmentProjection::Count, SeriesSamples::Histogram { samples }) => {
            project_head_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Count, SeriesSamples::ExponentialHistogram { samples }) => {
            project_head_exponential_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Count, SeriesSamples::Summary { samples }) => {
            project_head_summary_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Sum, SeriesSamples::Histogram { samples }) => {
            project_head_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Sum, SeriesSamples::ExponentialHistogram { samples }) => {
            project_head_exponential_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Sum, SeriesSamples::Summary { samples }) => {
            project_head_summary_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::HistogramBucket { le, .. }, SeriesSamples::Histogram { samples }) => {
            let le_filter = compile_bucket_le_filter(le)?;
            let mut delta_accumulators = BTreeMap::new();
            let mut delta_fragments_started = BTreeSet::new();
            for (ts, value) in samples {
                if ts > end_ms {
                    continue;
                }
                let emit = ts >= start_ms;
                let mut cumulative = 0u64;
                for (idx, bound) in value.explicit_bounds.iter().enumerate() {
                    cumulative = cumulative
                        .saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
                    let le_value = format_promql_float_label(*bound);
                    if le_filter.matches(&le_value) {
                        let (projected_value, reset_hint) = project_head_histogram_bucket_value(
                            value.metadata,
                            cumulative,
                            &le_value,
                            &mut delta_accumulators,
                            &mut delta_fragments_started,
                        );
                        if !emit {
                            continue;
                        }
                        let labels = projected_head_labels(
                            base_labels,
                            metric_name,
                            "_bucket",
                            Some(("le", le_value)),
                        );
                        push_head_projected_sample_with_counter_reset_hint_and_temporality(
                            &mut projected,
                            labels,
                            ts,
                            projected_value,
                            reset_hint,
                            value.metadata.temporality,
                            value.metadata.start_time_ms,
                        );
                    }
                }
                if le_filter.matches("+Inf") {
                    let (projected_value, reset_hint) = project_head_histogram_bucket_value(
                        value.metadata,
                        value.count,
                        "+Inf",
                        &mut delta_accumulators,
                        &mut delta_fragments_started,
                    );
                    if !emit {
                        continue;
                    }
                    let labels = projected_head_labels(
                        base_labels,
                        metric_name,
                        "_bucket",
                        Some(("le", "+Inf".to_string())),
                    );
                    push_head_projected_sample_with_counter_reset_hint_and_temporality(
                        &mut projected,
                        labels,
                        ts,
                        projected_value,
                        reset_hint,
                        value.metadata.temporality,
                        value.metadata.start_time_ms,
                    );
                }
            }
        }
        (
            SegmentProjection::HistogramBucket {
                le,
                exponential_histogram_boundaries,
            },
            SeriesSamples::ExponentialHistogram { samples },
        ) => {
            let le_filter = compile_bucket_le_filter(le)?;
            project_head_exponential_histogram_bucket_samples(
                &mut projected,
                base_labels,
                metric_name,
                &le_filter,
                exponential_histogram_boundaries,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::SummaryQuantile { quantile }, SeriesSamples::Summary { samples }) => {
            for (ts, value) in samples {
                if ts < start_ms || ts > end_ms {
                    continue;
                }
                for quantile_value in value.quantiles {
                    let label = format_promql_float_label(quantile_value.quantile);
                    if quantile.as_deref().is_some_and(|filter| filter != label) {
                        continue;
                    }
                    let labels = projected_head_labels(
                        base_labels,
                        metric_name,
                        "",
                        Some(("quantile", label)),
                    );
                    let projected_value = if value.metadata.is_stale() {
                        prometheus_stale_nan()
                    } else {
                        quantile_value.value
                    };
                    push_head_projected_sample(&mut projected, labels, ts, projected_value);
                }
            }
        }
        _ => {}
    }

    Ok(projected.into_values().collect())
}

pub(super) fn project_head_histogram_count_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, HistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_u64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_count",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
}

pub(super) fn project_head_exponential_histogram_count_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, ExponentialHistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_u64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_count",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
}

pub(super) fn project_head_summary_count_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, SummaryValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_u64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_count",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
}

pub(super) fn project_head_histogram_sum_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, HistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_optional_f64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_sum",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.sum)),
        start_ms,
        end_ms,
    );
}

pub(super) fn project_head_exponential_histogram_sum_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, ExponentialHistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_optional_f64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_sum",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.sum)),
        start_ms,
        end_ms,
    );
}

pub(super) fn project_head_summary_sum_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, SummaryValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_optional_f64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_sum",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, Some(value.sum))),
        start_ms,
        end_ms,
    );
}

pub(super) fn project_head_typed_u64_counter_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    metric_suffix: &str,
    values: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) {
    let labels = projected_head_labels(base_labels, metric_name, metric_suffix, None);
    let mut delta_accumulator = 0u64;
    let mut delta_fragment_started = false;
    for (ts, metadata, raw) in values {
        if ts > end_ms {
            continue;
        }
        let emit = ts >= start_ms;
        let (value, reset_hint) = if metadata.is_stale() {
            if metadata.temporality == OtlpAggregationTemporality::Delta {
                delta_accumulator = 0;
                delta_fragment_started = false;
            }
            (prometheus_stale_nan(), metadata.reset_hint)
        } else if metadata.temporality == OtlpAggregationTemporality::Delta {
            delta_accumulator = delta_accumulator.saturating_add(raw);
            let reset_hint = delta_projection_reset_hint(&mut delta_fragment_started);
            (delta_accumulator as f64, reset_hint)
        } else {
            (raw as f64, metadata.reset_hint)
        };
        if !emit {
            continue;
        }
        push_head_projected_sample_with_counter_reset_hint_and_temporality(
            out,
            labels.clone(),
            ts,
            value,
            reset_hint,
            metadata.temporality,
            metadata.start_time_ms,
        );
    }
}

pub(super) fn project_head_typed_optional_f64_counter_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    metric_suffix: &str,
    values: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) {
    let labels = projected_head_labels(base_labels, metric_name, metric_suffix, None);
    let mut delta_accumulator = 0.0f64;
    let mut delta_fragment_started = false;
    for (ts, metadata, raw) in values {
        if ts > end_ms {
            continue;
        }
        let emit = ts >= start_ms;
        let (value, reset_hint) = if metadata.is_stale() {
            if metadata.temporality == OtlpAggregationTemporality::Delta {
                delta_accumulator = 0.0;
                delta_fragment_started = false;
            }
            (prometheus_stale_nan(), metadata.reset_hint)
        } else if let Some(raw) = raw {
            if metadata.temporality == OtlpAggregationTemporality::Delta {
                delta_accumulator += raw;
                let reset_hint = delta_projection_reset_hint(&mut delta_fragment_started);
                (delta_accumulator, reset_hint)
            } else {
                (raw, metadata.reset_hint)
            }
        } else {
            continue;
        };
        if !emit {
            continue;
        }
        push_head_projected_sample_with_counter_reset_hint_and_temporality(
            out,
            labels.clone(),
            ts,
            value,
            reset_hint,
            metadata.temporality,
            metadata.start_time_ms,
        );
    }
}

pub(super) fn project_head_histogram_bucket_value(
    metadata: TypedSampleMetadata,
    raw: u64,
    le: &str,
    delta_accumulators: &mut BTreeMap<String, u64>,
    delta_fragments_started: &mut BTreeSet<String>,
) -> (f64, CounterResetHint) {
    if metadata.is_stale() {
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            delta_accumulators.insert(le.to_string(), 0);
            delta_fragments_started.remove(le);
        }
        return (prometheus_stale_nan(), metadata.reset_hint);
    }
    if metadata.temporality == OtlpAggregationTemporality::Delta {
        let accumulator = delta_accumulators.entry(le.to_string()).or_insert(0);
        *accumulator = accumulator.saturating_add(raw);
        let reset_hint = if delta_fragments_started.insert(le.to_string()) {
            CounterResetHint::CounterReset
        } else {
            CounterResetHint::NotCounterReset
        };
        (*accumulator as f64, reset_hint)
    } else {
        (raw as f64, metadata.reset_hint)
    }
}

pub(super) fn project_head_exponential_histogram_bucket_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    le_filter: &CompiledBucketLeFilter,
    boundaries: &[f64],
    values: Vec<(u64, ExponentialHistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    let mut delta_accumulators: BTreeMap<String, u64> = BTreeMap::new();
    let mut delta_fragments_started: BTreeSet<String> = BTreeSet::new();
    for (ts, value) in values {
        if ts > end_ms {
            continue;
        }
        let emit = ts >= start_ms;

        for boundary in boundaries {
            let le = format_promql_float_label(*boundary);
            if le_filter.matches(&le) {
                let raw = exponential_histogram_projected_bucket_count(&value, *boundary);
                let (projected, reset_hint) = project_head_histogram_bucket_value(
                    value.metadata,
                    raw,
                    &le,
                    &mut delta_accumulators,
                    &mut delta_fragments_started,
                );
                if !emit {
                    continue;
                }
                let labels =
                    projected_head_labels(base_labels, metric_name, "_bucket", Some(("le", le)));
                push_head_projected_sample_with_counter_reset_hint_and_temporality(
                    out,
                    labels,
                    ts,
                    projected,
                    reset_hint,
                    value.metadata.temporality,
                    value.metadata.start_time_ms,
                );
            }
        }

        if le_filter.matches("+Inf") {
            let (projected, reset_hint) = project_head_histogram_bucket_value(
                value.metadata,
                value.count,
                "+Inf",
                &mut delta_accumulators,
                &mut delta_fragments_started,
            );
            if !emit {
                continue;
            }
            let labels = projected_head_labels(
                base_labels,
                metric_name,
                "_bucket",
                Some(("le", "+Inf".to_string())),
            );
            push_head_projected_sample_with_counter_reset_hint_and_temporality(
                out,
                labels,
                ts,
                projected,
                reset_hint,
                value.metadata.temporality,
                value.metadata.start_time_ms,
            );
        }
    }
}

pub(crate) fn exponential_histogram_projected_bucket_count(
    value: &ExponentialHistogramValue,
    le: f64,
) -> u64 {
    if le.is_infinite() && le.is_sign_positive() {
        return value.count;
    }

    let base = exponential_histogram_base(value.scale);
    let negative = exponential_histogram_negative_bucket_count_le(&value.negative, base, le);
    let zero = if le >= value.zero_threshold {
        value.zero_count
    } else {
        0
    };
    let positive = exponential_histogram_positive_bucket_count_le(&value.positive, base, le);
    negative
        .saturating_add(zero)
        .saturating_add(positive)
        .min(value.count)
}

pub fn downscale_exponential_histogram(
    value: &ExponentialHistogramValue,
    target_scale: i32,
) -> Result<ExponentialHistogramValue, ExponentialHistogramMergeError> {
    if target_scale > value.scale {
        return Err(
            ExponentialHistogramMergeError::TargetScaleHigherThanSource {
                source_scale: value.scale,
                target_scale,
            },
        );
    }

    Ok(ExponentialHistogramValue {
        scale: target_scale,
        positive: exponential_histogram_bucket_map_to_buckets(
            downscale_exponential_histogram_buckets_to_map(
                &value.positive,
                value.scale,
                target_scale,
            )?,
        )?,
        negative: exponential_histogram_bucket_map_to_buckets(
            downscale_exponential_histogram_buckets_to_map(
                &value.negative,
                value.scale,
                target_scale,
            )?,
        )?,
        ..value.clone()
    })
}

pub fn merge_exponential_histograms(
    values: &[ExponentialHistogramValue],
    scale_policy: ExponentialHistogramScalePolicy,
) -> Result<Option<ExponentialHistogramValue>, ExponentialHistogramMergeError> {
    let Some(first) = values.first() else {
        return Ok(None);
    };

    let target_scale = values
        .iter()
        .map(|value| value.scale)
        .min()
        .unwrap_or(first.scale);
    let target_scale = match scale_policy {
        ExponentialHistogramScalePolicy::Keep => target_scale,
        ExponentialHistogramScalePolicy::DownscaleToMaxScale(max_scale) => {
            target_scale.min(max_scale)
        }
    };

    let zero_threshold_bits = first.zero_threshold.to_bits();
    let mut count = 0u64;
    let mut zero_count = 0u64;
    let mut sum = 0.0f64;
    let mut all_sums_present = true;
    let mut min = None;
    let mut max = None;
    let mut positive = BTreeMap::new();
    let mut negative = BTreeMap::new();

    for value in values {
        if value.zero_threshold.to_bits() != zero_threshold_bits {
            return Err(ExponentialHistogramMergeError::ZeroThresholdMismatch);
        }

        count = count
            .checked_add(value.count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;
        zero_count = zero_count
            .checked_add(value.zero_count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;

        if let Some(value_sum) = value.sum {
            sum += value_sum;
        } else {
            all_sums_present = false;
        }

        min = merge_optional_min(min, value.min);
        max = merge_optional_max(max, value.max);

        add_exponential_histogram_bucket_maps(
            &mut positive,
            downscale_exponential_histogram_buckets_to_map(
                &value.positive,
                value.scale,
                target_scale,
            )?,
        )?;
        add_exponential_histogram_bucket_maps(
            &mut negative,
            downscale_exponential_histogram_buckets_to_map(
                &value.negative,
                value.scale,
                target_scale,
            )?,
        )?;
    }

    Ok(Some(ExponentialHistogramValue {
        count,
        sum: all_sums_present.then_some(sum),
        min,
        max,
        scale: target_scale,
        zero_threshold: first.zero_threshold,
        zero_count,
        metadata: first.metadata,
        positive: exponential_histogram_bucket_map_to_buckets(positive)?,
        negative: exponential_histogram_bucket_map_to_buckets(negative)?,
    }))
}

pub fn downscale_exponential_histogram_buckets_to_map(
    buckets: &ExponentialHistogramBuckets,
    source_scale: i32,
    target_scale: i32,
) -> Result<BTreeMap<i32, u64>, ExponentialHistogramMergeError> {
    if target_scale > source_scale {
        return Err(
            ExponentialHistogramMergeError::TargetScaleHigherThanSource {
                source_scale,
                target_scale,
            },
        );
    }
    let shift = source_scale
        .checked_sub(target_scale)
        .ok_or(ExponentialHistogramMergeError::ScaleDeltaTooLarge)?;
    let divisor = 1i64
        .checked_shl(
            u32::try_from(shift).map_err(|_| ExponentialHistogramMergeError::ScaleDeltaTooLarge)?,
        )
        .ok_or(ExponentialHistogramMergeError::ScaleDeltaTooLarge)?;

    let mut map = BTreeMap::new();
    for (idx, count) in buckets.counts.iter().copied().enumerate() {
        let source_index = i64::from(buckets.offset)
            .checked_add(
                i64::try_from(idx)
                    .map_err(|_| ExponentialHistogramMergeError::BucketIndexOverflow)?,
            )
            .ok_or(ExponentialHistogramMergeError::BucketIndexOverflow)?;
        let target_index = floor_div_i64(source_index, divisor);
        let target_index = i32::try_from(target_index)
            .map_err(|_| ExponentialHistogramMergeError::BucketIndexOverflow)?;
        let entry = map.entry(target_index).or_insert(0u64);
        *entry = entry
            .checked_add(count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;
    }
    Ok(map)
}

pub(super) fn add_exponential_histogram_bucket_maps(
    out: &mut BTreeMap<i32, u64>,
    input: BTreeMap<i32, u64>,
) -> Result<(), ExponentialHistogramMergeError> {
    for (index, count) in input {
        let entry = out.entry(index).or_insert(0);
        *entry = entry
            .checked_add(count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;
    }
    Ok(())
}

pub(super) fn exponential_histogram_bucket_map_to_buckets(
    map: BTreeMap<i32, u64>,
) -> Result<ExponentialHistogramBuckets, ExponentialHistogramMergeError> {
    let Some((&offset, _)) = map.first_key_value() else {
        return Ok(ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        });
    };
    let Some((&last, _)) = map.last_key_value() else {
        unreachable!("non-empty BTreeMap has a last key");
    };
    let span = i64::from(last)
        .checked_sub(i64::from(offset))
        .and_then(|span| span.checked_add(1))
        .ok_or(ExponentialHistogramMergeError::BucketSpanTooWide)?;
    let span =
        usize::try_from(span).map_err(|_| ExponentialHistogramMergeError::BucketSpanTooWide)?;
    let mut counts = vec![0u64; span];
    for (index, count) in map {
        let idx = usize::try_from(i64::from(index) - i64::from(offset))
            .map_err(|_| ExponentialHistogramMergeError::BucketIndexOverflow)?;
        counts[idx] = count;
    }
    Ok(ExponentialHistogramBuckets { offset, counts })
}

pub(super) fn merge_optional_min(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(super) fn merge_optional_max(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(super) fn floor_div_i64(value: i64, divisor: i64) -> i64 {
    debug_assert!(divisor > 0);
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && value < 0 {
        quotient - 1
    } else {
        quotient
    }
}

pub(super) fn exponential_histogram_base(scale: i32) -> f64 {
    2.0f64.powf(2.0f64.powi(-scale))
}

pub(super) fn exponential_histogram_positive_bucket_count_le(
    buckets: &ExponentialHistogramBuckets,
    base: f64,
    le: f64,
) -> u64 {
    buckets
        .counts
        .iter()
        .enumerate()
        .filter_map(|(idx, count)| {
            let bucket_index = buckets
                .offset
                .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
            let upper = base.powi(bucket_index.saturating_add(1));
            (upper <= le).then_some(*count)
        })
        .fold(0u64, u64::saturating_add)
}

pub(super) fn exponential_histogram_negative_bucket_count_le(
    buckets: &ExponentialHistogramBuckets,
    base: f64,
    le: f64,
) -> u64 {
    buckets
        .counts
        .iter()
        .enumerate()
        .filter_map(|(idx, count)| {
            let bucket_index = buckets
                .offset
                .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
            let upper = -base.powi(bucket_index);
            (upper <= le).then_some(*count)
        })
        .fold(0u64, u64::saturating_add)
}

pub(super) fn projected_head_labels(
    base_labels: &[(String, String)],
    metric_name: &str,
    metric_suffix: &str,
    extra_label: Option<(&str, String)>,
) -> Vec<(String, String)> {
    let mut labels = Vec::with_capacity(base_labels.len() + usize::from(extra_label.is_some()));
    let mut metric_seen = false;
    let extra_key = extra_label.as_ref().map(|(key, _)| *key);
    for (key, value) in base_labels {
        if key == METRIC_NAME_LABEL {
            labels.push((key.clone(), format!("{metric_name}{metric_suffix}")));
            metric_seen = true;
        } else if extra_key != Some(key.as_str()) {
            labels.push((key.clone(), value.clone()));
        }
    }
    if !metric_seen {
        labels.push((
            METRIC_NAME_LABEL.to_string(),
            format!("{metric_name}{metric_suffix}"),
        ));
    }
    if let Some((key, value)) = extra_label {
        labels.push((key.to_string(), value));
    }
    labels.sort_by(|left, right| left.0.cmp(&right.0));
    labels
}

pub(super) fn push_head_projected_sample(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    labels: Vec<(String, String)>,
    timestamp_ms: u64,
    value: f64,
) {
    let series_id = segment_series_id(&labels);
    let entry = out
        .entry(series_id)
        .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
    entry.push_sample(timestamp_ms, value);
}

pub(super) fn push_head_projected_sample_with_counter_reset_hint_and_temporality(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    labels: Vec<(String, String)>,
    timestamp_ms: u64,
    value: f64,
    reset_hint: CounterResetHint,
    temporality: OtlpAggregationTemporality,
    start_time_ms: Option<u64>,
) {
    let series_id = segment_series_id(&labels);
    let entry = out
        .entry(series_id)
        .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
    entry.push_sample_with_counter_reset_hint_temporality_and_start_time(
        timestamp_ms,
        value,
        reset_hint,
        temporality,
        start_time_ms,
    );
}

fn delta_projection_reset_hint(started: &mut bool) -> CounterResetHint {
    if *started {
        CounterResetHint::NotCounterReset
    } else {
        *started = true;
        CounterResetHint::CounterReset
    }
}
