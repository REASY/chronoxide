use super::super::*;

const SCALAR_RANGE_READBACK_WINDOW_MS: u64 = 15 * 60 * 1_000;
pub(in super::super::super) const SCALAR_RANGE_READBACK_STEP_MS: u64 = 5 * 60 * 1_000;
const SCALAR_RANGE_READBACK_MAX_EVALUATIONS: usize = 4;

pub(in super::super::super) fn scalar_expected_readbacks(
    base: ExpectedReadback,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![base];
    if let Some((latest_ts, latest_value)) = readbacks[0]
        .samples
        .iter()
        .rev()
        .copied()
        .find(|(_, value)| value.is_finite())
        && latest_ts == readbacks[0].end_ms
    {
        readbacks.push(ExpectedReadback {
            query: format!("({}) * 2", readbacks[0].query),
            start_ms: latest_ts,
            end_ms: latest_ts,
            step_ms: None,
            samples: vec![(latest_ts, latest_value * 2.0)],
            isolation_check: None,
        });
        readbacks.push(ExpectedReadback {
            query: format!("sum({})", readbacks[0].query),
            start_ms: latest_ts,
            end_ms: latest_ts,
            step_ms: None,
            samples: vec![(latest_ts, latest_value)],
            isolation_check: None,
        });
    }

    let base = readbacks[0].clone();
    push_counter_range_readbacks(&mut readbacks, &base, None);
    if let Some(range_readback) = bounded_scalar_counter_range_readback(&base) {
        readbacks.push(range_readback);
    }
    readbacks
}

pub(in super::super::super) fn bounded_scalar_counter_range_readback(
    base: &ExpectedReadback,
) -> Option<ExpectedReadback> {
    // Keep this expected-value path independent of the production range
    // evaluator: each endpoint is selected and extrapolated by the oracle's
    // local Prometheus-compatible counter math below.
    for evaluation_count in (2..=SCALAR_RANGE_READBACK_MAX_EVALUATIONS).rev() {
        let evaluation_span_ms =
            SCALAR_RANGE_READBACK_STEP_MS.checked_mul(u64::try_from(evaluation_count - 1).ok()?)?;
        let Some(range_start_ms) = base.end_ms.checked_sub(evaluation_span_ms) else {
            continue;
        };
        let earliest_window_start_ms =
            range_start_ms.saturating_sub(SCALAR_RANGE_READBACK_WINDOW_MS);
        let starts_before_epoch = range_start_ms < SCALAR_RANGE_READBACK_WINDOW_MS;
        if (starts_before_epoch && base.start_ms != 0)
            || (!starts_before_epoch && earliest_window_start_ms < base.start_ms)
        {
            continue;
        }

        let mut samples = Vec::with_capacity(evaluation_count);
        for evaluation_index in 0..evaluation_count {
            let endpoint_ms = range_start_ms.checked_add(
                SCALAR_RANGE_READBACK_STEP_MS.checked_mul(u64::try_from(evaluation_index).ok()?)?,
            )?;
            let Some(rate) = scalar_counter_rate_at(
                &base.samples,
                None,
                endpoint_ms,
                SCALAR_RANGE_READBACK_WINDOW_MS,
            ) else {
                break;
            };
            samples.push((endpoint_ms, rate));
        }
        if samples.len() != evaluation_count {
            continue;
        }

        return Some(ExpectedReadback {
            query: format!(
                "rate({}[{}ms])",
                base.query, SCALAR_RANGE_READBACK_WINDOW_MS
            ),
            start_ms: range_start_ms,
            end_ms: base.end_ms,
            step_ms: Some(SCALAR_RANGE_READBACK_STEP_MS),
            samples,
            isolation_check: Some(base.isolation_check_with_reason(
                "multi-step scalar counter range skipped because the exact selector did not isolate the independently decoded physical Float/Int64 series",
            )),
        });
    }
    None
}

pub(in super::super::super) fn push_counter_range_readbacks(
    readbacks: &mut Vec<ExpectedReadback>,
    base: &ExpectedReadback,
    counter_reset_hints: Option<&[CounterResetHint]>,
) {
    let Some((range_ms, increase)) = scalar_counter_range_increase(base, counter_reset_hints)
    else {
        return;
    };
    let range_seconds = range_ms as f64 / 1_000.0;
    if range_seconds <= 0.0 {
        return;
    }
    let Some(rate) =
        scalar_counter_rate_at(&base.samples, counter_reset_hints, base.end_ms, range_ms)
    else {
        return;
    };

    readbacks.push(ExpectedReadback {
        query: format!("rate({}[{}ms])", base.query, range_ms),
        start_ms: base.end_ms,
        end_ms: base.end_ms,
        step_ms: None,
        samples: vec![(base.end_ms, rate)],
        isolation_check: Some(base.isolation_check()),
    });
    readbacks.push(ExpectedReadback {
        query: format!("increase({}[{}ms])", base.query, range_ms),
        start_ms: base.end_ms,
        end_ms: base.end_ms,
        step_ms: None,
        samples: vec![(base.end_ms, increase)],
        isolation_check: Some(base.isolation_check()),
    });
}

pub(in super::super::super) fn scalar_counter_range_increase(
    readback: &ExpectedReadback,
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<(u64, f64)> {
    let latest_ts = readback.end_ms;
    let earliest_ts = readback.samples.first()?.0;
    let range_ms = latest_ts.saturating_sub(earliest_ts).saturating_add(1);
    if range_ms == 0 {
        return None;
    }
    scalar_counter_increase_at(&readback.samples, counter_reset_hints, latest_ts, range_ms)
        .map(|increase| (range_ms, increase))
}

fn scalar_counter_increase_at(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    latest_ts: u64,
    range_ms: u64,
) -> Option<f64> {
    scalar_counter_value_at(samples, counter_reset_hints, latest_ts, range_ms, None)
}

fn scalar_counter_rate_at(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    latest_ts: u64,
    range_ms: u64,
) -> Option<f64> {
    if range_ms == 0 {
        return None;
    }
    scalar_counter_value_at(
        samples,
        counter_reset_hints,
        latest_ts,
        range_ms,
        Some(range_ms as f64 / 1_000.0),
    )
}

fn scalar_counter_value_at(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    latest_ts: u64,
    range_ms: u64,
    rate_range_seconds: Option<f64>,
) -> Option<f64> {
    if range_ms == 0 {
        return None;
    }
    let range_start_ms = latest_ts.saturating_sub(range_ms);
    let range_start_before_epoch_ms = range_ms.saturating_sub(latest_ts);
    let include_range_start = range_start_before_epoch_ms > 0;
    let counter_reset_hints = counter_reset_hints.filter(|hints| hints.len() == samples.len());
    let mut selected = Vec::new();
    let mut selected_hints = counter_reset_hints.map(|_| Vec::new());
    for (idx, sample) in samples.iter().copied().enumerate() {
        let before_range = if include_range_start {
            sample.0 < range_start_ms
        } else {
            sample.0 <= range_start_ms
        };
        if before_range || sample.0 > latest_ts {
            continue;
        }
        if sample.1.to_bits() == prometheus_stale_nan().to_bits() {
            continue;
        }
        selected.push(sample);
        if let (Some(hints), Some(selected_hints)) = (counter_reset_hints, selected_hints.as_mut())
            && let Some(hint) = hints.get(idx).copied()
        {
            selected_hints.push(hint);
        }
    }
    if selected.len() < 2 {
        return None;
    }

    expected_extrapolated_counter_value(
        &selected,
        selected_hints.as_deref(),
        range_start_ms,
        range_start_before_epoch_ms,
        latest_ts,
        rate_range_seconds,
    )
}

fn expected_extrapolated_counter_value(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    range_start_ms: u64,
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
    rate_range_seconds: Option<f64>,
) -> Option<f64> {
    if samples.len() < 2 || range_end_ms <= range_start_ms {
        return None;
    }

    let (first_ts, first_value) = samples.first().copied()?;
    let (last_ts, _) = samples.last().copied()?;
    if last_ts <= first_ts {
        return None;
    }

    let raw_increase = expected_counter_increase(samples, counter_reset_hints)?;
    let sampled_interval = (last_ts - first_ts) as f64 / 1_000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;
    let mut duration_to_start = first_ts
        .saturating_sub(range_start_ms)
        .saturating_add(range_start_before_epoch_ms) as f64
        / 1_000.0;
    let mut duration_to_end = range_end_ms.saturating_sub(last_ts) as f64 / 1_000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if raw_increase > 0.0 && first_value >= 0.0 {
        let duration_to_zero = sampled_interval * (first_value / raw_increase);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    let mut factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    if let Some(range_seconds) = rate_range_seconds {
        if range_seconds <= 0.0 {
            return None;
        }
        // Keep this independent oracle faithful to Prometheus's operation
        // order: rate divides the factor before multiplying the raw increase.
        factor /= range_seconds;
    }

    Some(raw_increase * factor)
}

fn expected_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    if let Some(counter_reset_hints) = counter_reset_hints {
        return expected_counter_increase_with_reset_hints(samples, counter_reset_hints);
    }
    expected_counter_increase_from_value_decreases(samples)
}

fn expected_counter_increase_with_reset_hints(
    samples: &[(u64, f64)],
    counter_reset_hints: &[CounterResetHint],
) -> Option<f64> {
    if counter_reset_hints.len() != samples.len() {
        return expected_counter_increase_from_value_decreases(samples);
    }
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples
        .iter()
        .copied()
        .zip(counter_reset_hints.iter().copied());
    let ((_, first), _) = iter.next()?;
    let last = samples.last()?.1;

    let mut previous = first;
    let mut increase = last - first;
    for ((_, current), reset_hint) in iter {
        let adjustment = match reset_hint {
            CounterResetHint::CounterReset => previous,
            CounterResetHint::NotCounterReset => {
                if previous.is_finite() && current.is_finite() && current < previous {
                    return None;
                }
                0.0
            }
            CounterResetHint::Unknown => {
                if current < previous {
                    previous
                } else {
                    0.0
                }
            }
            CounterResetHint::GaugeType => return None,
        };
        increase += adjustment;
        previous = current;
    }
    Some(increase)
}

fn expected_counter_increase_from_value_decreases(samples: &[(u64, f64)]) -> Option<f64> {
    let (_, first) = samples.first().copied()?;
    let (_, last) = samples.last().copied()?;

    let mut previous = first;
    let mut increase = last - first;
    for (_, current) in samples.iter().skip(1).copied() {
        if current < previous {
            increase += previous;
        }
        previous = current;
    }
    Some(increase)
}
