use super::super::*;
use super::common::*;
use super::scalar::push_counter_range_readbacks;

pub(in super::super::super) fn exponential_histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(
        u64,
        chronoxide_core::storage::head::ExponentialHistogramValue,
    )],
    start_ms: u64,
    end_ms: u64,
    exponential_histogram_bucket_boundaries: &[f64],
) -> Vec<ExpectedReadback> {
    let (count_samples, count_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    let mut projected = vec![ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            step_ms: None,
            samples: count_samples,
            isolation_check: None,
        },
        range_hints: count_hints,
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        let (sum_samples, sum_hints) = project_optional_f64_counter_samples_with_range_hints(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
                start_ms,
                end_ms,
                step_ms: None,
                samples: sum_samples,
                isolation_check: None,
            },
            range_hints: sum_hints,
        });
    }

    for boundary in exponential_histogram_bucket_boundaries {
        let le = format_promql_float_label(*boundary);
        let (bucket_samples, bucket_hints) =
            project_exponential_histogram_bucket_samples_with_range_hints(
                samples, *boundary, start_ms, end_ms,
            );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(
                    &format!("{metric_name}_bucket"),
                    labels,
                    Some(("le", le.as_str())),
                ),
                start_ms,
                end_ms,
                step_ms: None,
                samples: bucket_samples,
                isolation_check: None,
            },
            range_hints: bucket_hints,
        });
    }

    let (inf_bucket_samples, inf_bucket_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    projected.push(ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(
                &format!("{metric_name}_bucket"),
                labels,
                Some(("le", "+Inf")),
            ),
            start_ms,
            end_ms,
            step_ms: None,
            samples: inf_bucket_samples,
            isolation_check: None,
        },
        range_hints: inf_bucket_hints,
    });

    let mut readbacks = projected
        .iter()
        .map(|projected| projected.readback.clone())
        .collect::<Vec<_>>();
    for projected in &projected {
        if let Some(hints) = &projected.range_hints {
            push_counter_range_readbacks(&mut readbacks, &projected.readback, Some(hints));
        }
    }
    readbacks
}

pub(in super::super::super) fn project_exponential_histogram_bucket_samples_with_range_hints(
    samples: &[(
        u64,
        chronoxide_core::storage::head::ExponentialHistogramValue,
    )],
    le: f64,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0u64;
    let mut previous_non_stale_delta_timestamp_ms = None;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, value) in samples {
        if *ts < start_ms || *ts > end_ms {
            continue;
        }

        let raw = exponential_histogram_projected_bucket_count(value, le);
        let projected = if value.metadata.is_stale() {
            reset_delta_projection_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                &mut accumulator,
            );
            prometheus_stale_nan()
        } else if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
            if delta_interval_starts_new_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                *ts,
                value.metadata,
            ) {
                accumulator = 0;
            }
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            previous_non_stale_delta_timestamp_ms = None;
            raw as f64
        };
        if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(value.metadata.reset_hint);
        }
        out.push((*ts, projected));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn exponential_histogram_projected_bucket_count(
    value: &chronoxide_core::storage::head::ExponentialHistogramValue,
    le: f64,
) -> u64 {
    if le.is_infinite() && le.is_sign_positive() {
        return value.count;
    }

    let base = 2.0f64.powf(2.0f64.powi(-value.scale));
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

fn exponential_histogram_positive_bucket_count_le(
    buckets: &chronoxide_core::storage::head::ExponentialHistogramBuckets,
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

fn exponential_histogram_negative_bucket_count_le(
    buckets: &chronoxide_core::storage::head::ExponentialHistogramBuckets,
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
