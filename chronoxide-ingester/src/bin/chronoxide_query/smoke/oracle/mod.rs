//! Intentionally independent expected-value oracle for decoded storage chunks.
//!
//! Keep this module free of production evaluator helpers: agreement must come from
//! separately implemented PromQL and typed-OTLP semantics, not shared algorithms.

use super::*;

mod common;
mod exponential_histogram;
mod histogram;
mod scalar;
mod summary;

pub(super) use common::promql_sample_eq;
#[cfg(test)]
pub(in super::super) use common::{
    project_optional_f64_counter_samples, project_u64_counter_samples,
};
pub(in super::super) use common::{promql_exact_selector, promql_samples_eq};
pub(in super::super) use exponential_histogram::exponential_histogram_expected_readbacks;
#[cfg(test)]
pub(in super::super) use exponential_histogram::project_exponential_histogram_bucket_samples_with_range_hints;
#[cfg(test)]
pub(in super::super) use histogram::project_histogram_bucket_samples_with_range_hints;
pub(in super::super) use scalar::scalar_expected_readbacks;
#[cfg(test)]
pub(in super::super) use scalar::{
    SCALAR_RANGE_READBACK_STEP_MS, bounded_scalar_counter_range_readback,
    push_counter_range_readbacks, scalar_counter_range_increase,
};

use common::filter_samples;
use histogram::histogram_expected_readbacks;
use summary::summary_expected_readbacks;

pub(super) fn expected_readbacks_for_record(
    labels: &[(String, String)],
    record: &ChunkRecord,
    start_ms: u64,
    end_ms: u64,
    exponential_histogram_bucket_boundaries: &[f64],
) -> Vec<ExpectedReadback> {
    let Some(metric_name) = labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
    else {
        return Vec::new();
    };

    match &record.samples {
        ChunkSamples::Float(samples) => scalar_expected_readbacks(ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            start_ms,
            end_ms,
            step_ms: None,
            samples: filter_samples(samples.iter().copied(), start_ms, end_ms),
            isolation_check: None,
        }),
        ChunkSamples::Int64(samples) => scalar_expected_readbacks(ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            start_ms,
            end_ms,
            step_ms: None,
            samples: filter_samples(
                samples.iter().map(|(ts, value)| (*ts, *value as f64)),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        }),
        ChunkSamples::Histogram(samples) => {
            histogram_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
        ChunkSamples::ExponentialHistogram(samples) => exponential_histogram_expected_readbacks(
            metric_name,
            labels,
            samples,
            start_ms,
            end_ms,
            exponential_histogram_bucket_boundaries,
        ),
        ChunkSamples::Summary(samples) => {
            summary_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
    }
    .into_iter()
    .filter(|readback| !readback.samples.is_empty())
    .collect()
}
