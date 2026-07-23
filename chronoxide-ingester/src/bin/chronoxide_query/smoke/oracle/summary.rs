use super::super::*;
use super::common::*;

pub(super) fn summary_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::SummaryValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            step_ms: None,
            samples: project_u64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.count)),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        },
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            start_ms,
            end_ms,
            step_ms: None,
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, Some(value.sum))),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        },
    ];

    if let Some(quantile) = samples
        .first()
        .and_then(|(_, value)| value.quantiles.first())
        .map(|quantile| format_promql_float_label(quantile.quantile))
    {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(
                metric_name,
                labels,
                Some(("quantile", quantile.as_str())),
            ),
            start_ms,
            end_ms,
            step_ms: None,
            samples: filter_samples(
                samples.iter().map(|(ts, value)| {
                    let sample_value = value
                        .quantiles
                        .first()
                        .map(|quantile| quantile.value)
                        .unwrap_or(f64::NAN);
                    (
                        *ts,
                        typed_f64_value(value.metadata.is_stale(), sample_value),
                    )
                }),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        });
    }

    readbacks
}
