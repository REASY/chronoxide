use super::super::*;

#[derive(Debug, Clone)]
pub(super) struct ProjectedCounterReadback {
    pub(super) readback: ExpectedReadback,
    pub(super) range_hints: Option<Vec<CounterResetHint>>,
}

pub(in super::super::super) fn project_u64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    project_u64_counter_samples_with_range_hints(samples, start_ms, end_ms).0
}

pub(super) fn project_u64_counter_samples_with_range_hints(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0u64;
    let mut previous_non_stale_delta_timestamp_ms = None;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, metadata, raw) in samples {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            reset_delta_projection_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                &mut accumulator,
            );
            prometheus_stale_nan()
        } else if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
            if delta_interval_starts_new_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                ts,
                metadata,
            ) {
                accumulator = 0;
            }
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            previous_non_stale_delta_timestamp_ms = None;
            raw as f64
        };
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(metadata.reset_hint);
        }
        out.push((ts, value));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

pub(in super::super::super) fn project_optional_f64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    project_optional_f64_counter_samples_with_range_hints(samples, start_ms, end_ms).0
}

pub(super) fn project_optional_f64_counter_samples_with_range_hints(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0.0f64;
    let mut previous_non_stale_delta_timestamp_ms = None;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, metadata, raw) in samples {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            reset_delta_projection_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                &mut accumulator,
            );
            prometheus_stale_nan()
        } else if let Some(raw) = raw {
            if metadata.temporality == OtlpAggregationTemporality::Delta {
                range_supported = false;
                if delta_interval_starts_new_fragment(
                    &mut previous_non_stale_delta_timestamp_ms,
                    ts,
                    metadata,
                ) {
                    accumulator = 0.0;
                }
                accumulator += raw;
                accumulator
            } else {
                previous_non_stale_delta_timestamp_ms = None;
                raw
            }
        } else {
            if metadata.temporality != OtlpAggregationTemporality::Delta {
                previous_non_stale_delta_timestamp_ms = None;
            }
            continue;
        };
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(metadata.reset_hint);
        }
        out.push((ts, value));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

pub(super) fn delta_interval_starts_new_fragment(
    previous_non_stale_delta_timestamp_ms: &mut Option<u64>,
    timestamp_ms: u64,
    metadata: TypedSampleMetadata,
) -> bool {
    let discontinuous = previous_non_stale_delta_timestamp_ms
        .is_none_or(|previous_timestamp_ms| metadata.start_time_ms != Some(previous_timestamp_ms));
    *previous_non_stale_delta_timestamp_ms = Some(timestamp_ms);
    discontinuous
        || matches!(
            metadata.reset_hint,
            CounterResetHint::CounterReset | CounterResetHint::GaugeType
        )
}

pub(super) fn reset_delta_projection_fragment<T: Default>(
    previous_non_stale_delta_timestamp_ms: &mut Option<u64>,
    accumulator: &mut T,
) {
    *previous_non_stale_delta_timestamp_ms = None;
    *accumulator = T::default();
}

pub(super) fn filter_samples(
    samples: impl IntoIterator<Item = (u64, f64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    samples
        .into_iter()
        .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms)
        .collect()
}

pub(super) fn typed_f64_value(stale: bool, value: f64) -> f64 {
    if stale { prometheus_stale_nan() } else { value }
}

pub(in super::super) fn promql_sample_eq(left: (u64, f64), right: (u64, f64)) -> bool {
    left.0 == right.0 && left.1.to_bits() == right.1.to_bits()
}

pub(in super::super::super) fn promql_samples_eq(
    left: &[(u64, f64)],
    right: &[(u64, f64)],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .copied()
            .zip(right.iter().copied())
            .all(|(left, right)| promql_sample_eq(left, right))
}

pub(in super::super::super) fn promql_exact_selector(
    metric_name: &str,
    labels: &[(String, String)],
    extra_label: Option<(&str, &str)>,
) -> String {
    let mut matchers = Vec::with_capacity(labels.len() + usize::from(extra_label.is_some()));
    matchers.push(format!(
        r#"{}="{}""#,
        METRIC_NAME_LABEL,
        promql_escape_string(metric_name)
    ));
    for (key, value) in labels {
        if key == METRIC_NAME_LABEL || extra_label.is_some_and(|(extra_key, _)| extra_key == key) {
            continue;
        }
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    if let Some((key, value)) = extra_label {
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    format!("{{{}}}", matchers.join(","))
}

fn promql_escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}

pub(super) fn format_promql_float_label(value: f64) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "+Inf".to_string()
    } else {
        value.to_string()
    }
}
