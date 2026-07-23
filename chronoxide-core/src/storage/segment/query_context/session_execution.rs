use super::*;

macro_rules! profile_promql_evaluation {
    ($session:expr, $expression:expr) => {{
        let timer = QueryStageTimer::start($session.query_instrumentation_mode);
        let value = $expression;
        $session.query_stages.promql_grouping_evaluation = $session
            .query_stages
            .promql_grouping_evaluation
            .saturating_add(timer.elapsed());
        value
    }};
}

mod binary;
mod dispatch;
mod float;
mod histogram;
mod native_exponential;
mod native_histogram;
mod selectors;

pub(in crate::storage::segment) fn histogram_projected_bucket_value(
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
