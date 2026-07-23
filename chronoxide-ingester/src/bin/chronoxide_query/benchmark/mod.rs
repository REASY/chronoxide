use super::store::{open_segment_store_for_layout_ab, query_projection_config};
use super::*;

mod metadata;
mod model;
mod output;
mod raw;
mod report;
mod runner;
mod validation;

pub(super) use metadata::*;
pub(super) use model::*;

#[cfg(test)]
pub(super) use output::{
    BenchmarkOutputKind, StagedBenchmarkOutput, publish_benchmark_outputs_with_stager,
};
#[cfg(test)]
pub(super) use raw::{
    QueryBenchmarkRawChunkReadSchedulerV2, QueryBenchmarkRawQueryLabelStorageV2,
    QueryBenchmarkRawRangeExecutionV1, QueryBenchmarkRawRangeScalarCacheV3,
    QueryBenchmarkRawSymbolReadsV5, RawQueryStatsV1,
};
pub(super) use runner::run_query_benchmark_with_all_execution_policies;
#[cfg(test)]
pub(super) use runner::{
    effective_query_end_ms, run_query_benchmark, run_query_benchmark_with_experimental_flow,
    run_query_benchmark_with_experimental_flow_and_instrumentation,
};
#[cfg(test)]
pub(super) use validation::{validate_query_label_storage_stats, validate_query_stage_accounting};

use report::{
    add_query_data_prefetch_stats, add_session_stats, query_benchmark_mode_name, raw_run_kind_name,
    render_benchmark_markdown,
};

pub(super) fn add_session_profile(
    total: &mut SegmentStoreQueryProfile,
    next: SegmentStoreQueryProfile,
) {
    report::add_session_profile(total, next);
}

pub(super) fn render_profile_table(
    markdown: &mut String,
    title: &str,
    profile: SegmentStoreQueryProfile,
) {
    report::render_profile_table(markdown, title, profile);
}

pub(super) fn scheduled_range_evaluations(start_ms: u64, end_ms: u64, step_ms: u64) -> u128 {
    report::scheduled_range_evaluations(start_ms, end_ms, step_ms)
}

#[cfg(test)]
pub(super) fn render_index_positional_read_table(
    markdown: &mut String,
    title: &str,
    stats: SegmentIndexReadStats,
) {
    report::render_index_positional_read_table(markdown, title, stats);
}

#[cfg(test)]
pub(super) fn render_query_result_index_positional_reads(
    markdown: &mut String,
    results: &[QueryBenchmarkResult],
) {
    report::render_query_result_index_positional_reads(markdown, results);
}

#[cfg(test)]
pub(super) fn render_query_label_storage(markdown: &mut String, results: &[QueryBenchmarkResult]) {
    report::render_query_label_storage(markdown, results);
}

#[cfg(test)]
pub(super) fn render_range_scalar_cache_runs(
    markdown: &mut String,
    results: &[QueryBenchmarkResult],
) {
    report::render_range_scalar_cache_runs(markdown, results);
}

#[cfg(test)]
pub(super) fn median_duration(values: Vec<Duration>) -> Option<Duration> {
    report::median_duration(values)
}

#[cfg(test)]
pub(super) fn format_payload_read_amplification(read_bytes: u64, used_bytes: u64) -> String {
    report::format_payload_read_amplification(read_bytes, used_bytes)
}
