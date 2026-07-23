use super::corpus::{chunk_kind_index, collect_expected_readbacks};
use super::oracle::{promql_sample_eq, promql_samples_eq};
use super::*;

pub(in super::super) fn verify_readbacks(
    config: &QuerySmokeConfig,
    storage_layout: StorageLayoutArg,
    report: &SegmentStoreSmokeReport,
) -> io::Result<(QueryReadbackVerification, QueryReadbackDiagnostics)> {
    let mut diagnostics = QueryReadbackDiagnostics::default();
    let required_kinds = required_readback_kinds(report);

    let phase_start = Instant::now();
    let expected = collect_expected_readbacks(config, storage_layout, &required_kinds)?;
    diagnostics.collect_expected_readbacks = phase_start.elapsed();
    diagnostics.expected_queries = expected.len();
    diagnostics.multi_step_range_expected_queries = expected
        .iter()
        .filter(|readback| readback.step_ms.is_some())
        .count();

    let phase_start = Instant::now();
    let store = open_segment_store_for_layout_ab(
        &config.segments_dir,
        config.validate_segment_footers,
        query_projection_config(&config.exponential_histogram_bucket_boundaries),
        storage_layout,
    )?;
    diagnostics.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let mut query_session = store.query_session()?;
    diagnostics.query_session_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let verification = verify_expected_readbacks(&mut query_session, &expected, &mut diagnostics)?;
    diagnostics.promql_queries = phase_start.elapsed();
    diagnostics.session_stats = query_session.stats();
    diagnostics.session_profile = query_session.profile();

    Ok((verification, diagnostics))
}

pub(in super::super) fn verify_expected_readbacks(
    query_session: &mut SegmentStoreQuerySession<'_>,
    expected: &[ExpectedReadback],
    diagnostics: &mut QueryReadbackDiagnostics,
) -> io::Result<QueryReadbackVerification> {
    let mut mismatches = Vec::new();
    let mut actual_cache = ReadbackSampleCache::new();
    let mut checked_queries = 0usize;

    for expected in expected {
        if let Some(isolation_check) = &expected.isolation_check {
            let actual_samples = cached_readback_samples(
                query_session,
                &mut actual_cache,
                &isolation_check.query,
                isolation_check.start_ms,
                isolation_check.end_ms,
                None,
            )?;
            if !promql_samples_eq(&actual_samples, &isolation_check.samples) {
                diagnostics.skipped_queries = diagnostics.skipped_queries.saturating_add(1);
                diagnostics.isolation_check_skips =
                    diagnostics.isolation_check_skips.saturating_add(1);
                if expected.step_ms.is_some() {
                    diagnostics.multi_step_range_skipped_queries = diagnostics
                        .multi_step_range_skipped_queries
                        .saturating_add(1);
                }
                *diagnostics
                    .skip_reasons
                    .entry(isolation_check.failure_reason.clone())
                    .or_default() += 1;
                continue;
            }
        }

        let actual_samples = cached_readback_samples(
            query_session,
            &mut actual_cache,
            &expected.query,
            expected.start_ms,
            expected.end_ms,
            expected.step_ms,
        )?;
        diagnostics.executed_queries = diagnostics.executed_queries.saturating_add(1);
        if expected.step_ms.is_some() {
            diagnostics.multi_step_range_executed_queries = diagnostics
                .multi_step_range_executed_queries
                .saturating_add(1);
        }
        checked_queries = checked_queries.saturating_add(1);
        let missing_expected_samples = expected
            .samples
            .iter()
            .copied()
            .filter(|sample| {
                !actual_samples
                    .iter()
                    .any(|actual| promql_sample_eq(*actual, *sample))
            })
            .collect::<Vec<_>>();
        let exact_range_mismatch =
            expected.step_ms.is_some() && !promql_samples_eq(&actual_samples, &expected.samples);
        if !missing_expected_samples.is_empty() || exact_range_mismatch {
            mismatches.push(QueryReadbackMismatch {
                query: expected.query.clone(),
                missing_expected_samples,
                actual_samples,
            });
        }
    }

    Ok(QueryReadbackVerification {
        checked_queries,
        mismatches,
    })
}

type ReadbackSampleCache = BTreeMap<(String, u64, u64, Option<u64>), Vec<(u64, f64)>>;

fn cached_readback_samples(
    query_session: &mut SegmentStoreQuerySession<'_>,
    actual_cache: &mut ReadbackSampleCache,
    query: &str,
    start_ms: u64,
    end_ms: u64,
    step_ms: Option<u64>,
) -> io::Result<Vec<(u64, f64)>> {
    let key = (query.to_string(), start_ms, end_ms, step_ms);
    if let Some(samples) = actual_cache.get(&key) {
        return Ok(samples.clone());
    }

    let results = match step_ms {
        Some(step_ms) => query_session.query_promql_range(query, start_ms, end_ms, step_ms),
        None => query_session.query_promql(query, start_ms, end_ms),
    }
    .map_err(|err| io::Error::other(format!("query failed: {query}: {err}")))?;
    let samples = results
        .iter()
        .flat_map(|result| result.samples.iter().copied())
        .collect::<Vec<_>>();
    actual_cache.insert(key, samples.clone());
    Ok(samples)
}

fn required_readback_kinds(report: &SegmentStoreSmokeReport) -> [bool; 5] {
    let mut required = [false; 5];
    for sample in &report.sample_series {
        required[chunk_kind_index(sample.kind)] = true;
    }
    required
}
