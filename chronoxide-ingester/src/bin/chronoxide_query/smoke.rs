use super::store::{open_segment_store_for_layout_ab, query_projection_config};
use super::*;

#[path = "smoke/corpus.rs"]
mod corpus;
#[path = "smoke/model.rs"]
mod model;
#[path = "smoke/oracle/mod.rs"]
mod oracle;
#[path = "smoke/report.rs"]
mod report;
#[path = "smoke/verification.rs"]
mod verification;

#[cfg(test)]
pub(super) use corpus::{
    collect_expected_readbacks, read_chunk_record_from_payload_files, sample_limits_reached,
    segment_dirs,
};
#[cfg(test)]
pub(super) use model::ReadbackIsolationCheck;
pub(super) use model::{
    ExpectedReadback, QueryReadbackDiagnostics, QueryReadbackMismatch, QueryReadbackVerification,
    QuerySmokeDiagnostics,
};
#[cfg(test)]
pub(super) use oracle::{
    SCALAR_RANGE_READBACK_STEP_MS, bounded_scalar_counter_range_readback,
    exponential_histogram_expected_readbacks,
    project_exponential_histogram_bucket_samples_with_range_hints,
    project_histogram_bucket_samples_with_range_hints, project_optional_f64_counter_samples,
    project_u64_counter_samples, promql_exact_selector, promql_samples_eq,
    push_counter_range_readbacks, scalar_counter_range_increase, scalar_expected_readbacks,
};
pub(super) use report::render_markdown;
#[cfg(test)]
pub(super) use verification::verify_expected_readbacks;
pub(super) use verification::verify_readbacks;

#[cfg(test)]
pub(super) fn run_query_smoke(config: &QuerySmokeConfig) -> io::Result<SegmentStoreSmokeReport> {
    run_query_smoke_with_storage_layout(config, StorageLayoutArg::Schema8)
}

pub(super) fn run_query_smoke_with_storage_layout(
    config: &QuerySmokeConfig,
    storage_layout: StorageLayoutArg,
) -> io::Result<SegmentStoreSmokeReport> {
    let mut diagnostics = QuerySmokeDiagnostics::default();
    let phase_start = Instant::now();
    let store = open_segment_store_for_layout_ab(
        &config.segments_dir,
        config.validate_segment_footers,
        query_projection_config(&config.exponential_histogram_bucket_boundaries),
        storage_layout,
    )?;
    diagnostics.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let report =
        store.smoke_verify(config.start_ms, config.end_ms, config.sample_limit_per_kind)?;
    diagnostics.smoke_verify = phase_start.elapsed();

    let verification = if config.verify_readbacks {
        let (verification, readback_diagnostics) =
            verify_readbacks(config, storage_layout, &report)?;
        diagnostics.readback = Some(readback_diagnostics);
        Some(verification)
    } else {
        None
    };
    let markdown = render_markdown(
        config,
        storage_layout,
        &report,
        verification.as_ref(),
        Some(&diagnostics),
    );

    if let Some(parent) = config
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.output, markdown)?;

    if let Some(verification) = verification
        && !verification.mismatches.is_empty()
    {
        return Err(io::Error::other(format!(
            "readback verification found {} mismatches",
            verification.mismatches.len()
        )));
    }

    Ok(report)
}
