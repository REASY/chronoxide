use super::output::{preflight_benchmark_outputs, publish_benchmark_outputs};
use super::raw::render_raw_benchmark_json;
use super::validation::{validate_query_label_storage_stats, validate_query_stage_accounting};
use super::*;

#[cfg(test)]
pub(in super::super) fn run_query_benchmark(
    config: &QueryBenchmarkConfig,
) -> io::Result<QueryBenchmarkReport> {
    run_query_benchmark_with_experimental_flow(
        config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::OwnedStrings,
        StorageLayoutArg::Schema8,
    )
}

#[cfg(test)]
pub(in super::super) fn run_query_benchmark_with_experimental_flow(
    config: &QueryBenchmarkConfig,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: LabelMaterializationArg,
    label_storage: LabelStorageArg,
    storage_layout: StorageLayoutArg,
) -> io::Result<QueryBenchmarkReport> {
    run_query_benchmark_with_experimental_flow_and_instrumentation(
        config,
        experimental_cross_segment_chunk_reads,
        label_materialization,
        label_storage,
        storage_layout,
        QueryInstrumentationArg::Off,
    )
}

#[cfg(test)]
pub(in super::super) fn run_query_benchmark_with_experimental_flow_and_instrumentation(
    config: &QueryBenchmarkConfig,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: LabelMaterializationArg,
    label_storage: LabelStorageArg,
    storage_layout: StorageLayoutArg,
    query_instrumentation: QueryInstrumentationArg,
) -> io::Result<QueryBenchmarkReport> {
    run_query_benchmark_with_all_execution_policies(
        config,
        experimental_cross_segment_chunk_reads,
        label_materialization,
        label_storage,
        storage_layout,
        query_instrumentation,
        RangeExecutionModeArg::Repeated,
    )
}

pub(in super::super) fn run_query_benchmark_with_all_execution_policies(
    config: &QueryBenchmarkConfig,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: LabelMaterializationArg,
    label_storage: LabelStorageArg,
    storage_layout: StorageLayoutArg,
    query_instrumentation: QueryInstrumentationArg,
    range_execution_mode: RangeExecutionModeArg,
) -> io::Result<QueryBenchmarkReport> {
    if config.queries.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "query benchmark requires at least one --query",
        ));
    }
    if config.benchmark_repeats == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "query benchmark requires --benchmark-repeats >= 1",
        ));
    }
    if range_execution_mode != RangeExecutionModeArg::Repeated {
        if !matches!(config.mode, QueryBenchmarkMode::Range { .. }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "one-pass-assume-scalar range execution requires range benchmark mode",
            ));
        }
        if config.limits != QueryLimits::unlimited() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "one-pass-assume-scalar range execution requires unlimited public query limits",
            ));
        }
    }
    if let QueryBenchmarkMode::Range { step_ms } = config.mode {
        validate_range_benchmark(
            config.start_ms,
            config.end_ms,
            step_ms,
            config.prewarm_query_contexts,
            config.prefetch_query_data,
        )?;
    }
    let range_scalar_cache_budget =
        resolve_range_scalar_cache_budget(config.range_scalar_cache_max_bytes, Some(config.mode))?;
    preflight_benchmark_outputs(&config.output, config.raw_output.as_deref())?;
    let chunk_reader = Arc::new(chronoxide_core::storage::io::ChunkReader::new(
        ChunkReadConfig {
            mode: config.chunk_read_mode.core_mode(),
            queue_depth: config.chunk_read_queue_depth,
            payload_coalesce_max_gap_bytes: config.chunk_payload_coalesce_max_gap_bytes,
        },
    )?);
    let phase_start = Instant::now();
    let store = open_segment_store_for_layout_ab(
        &config.segments_dir,
        config.validate_segment_footers,
        query_projection_config(&config.exponential_histogram_bucket_boundaries),
        storage_layout,
    )?;
    let store_open = phase_start.elapsed();
    let phase_start = Instant::now();
    let corpus_fingerprint = store.corpus_fingerprint_sha256()?;
    let corpus_fingerprint_duration = phase_start.elapsed();
    let mut report = QueryBenchmarkReport {
        store_open,
        corpus_fingerprint,
        corpus_fingerprint_duration,
        query_session_open: Duration::ZERO,
        query_context_prewarm: Duration::ZERO,
        query_context_prewarm_stats_delta: SegmentStoreQuerySessionStats::default(),
        query_context_prewarm_profile_delta: SegmentStoreQueryProfile::default(),
        query_data_prefetch: Duration::ZERO,
        query_data_prefetch_stats: QueryDataPrefetchStats::default(),
        query_data_prefetch_session_stats_delta: SegmentStoreQuerySessionStats::default(),
        query_data_prefetch_profile_delta: SegmentStoreQueryProfile::default(),
        promql_queries: Duration::ZERO,
        post_query_fingerprints: Duration::ZERO,
        session_stats: SegmentStoreQuerySessionStats::default(),
        session_profile: SegmentStoreQueryProfile::default(),
        results: Vec::new(),
        experimental_cross_segment_chunk_reads,
        label_materialization,
        label_storage,
        storage_layout,
        query_instrumentation,
        range_execution_mode,
    };
    let sample_time_range = if config.mode == QueryBenchmarkMode::Instant
        && config.end_ms == u64::MAX
        && config
            .queries
            .iter()
            .any(|query| query_needs_finite_end(query))
    {
        store.latest_window_sample_time_range()?
    } else {
        None
    };

    for query in &config.queries {
        let query_end_ms = match config.mode {
            QueryBenchmarkMode::Instant => {
                effective_query_end_ms(query, config.end_ms, sample_time_range)
            }
            QueryBenchmarkMode::Range { .. } => config.end_ms,
        };
        let (effective_start_ms, effective_end_ms, step_ms) = match config.mode {
            QueryBenchmarkMode::Instant => (config.start_ms, query_end_ms, None),
            QueryBenchmarkMode::Range { step_ms } => {
                (config.start_ms, config.end_ms, Some(step_ms))
            }
        };
        let phase_start = Instant::now();
        let mut query_session = store.query_session()?;
        query_session
            .set_query_instrumentation_mode(query_instrumentation.core_mode())
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("configure query instrumentation: {error}"),
                )
            })?;
        query_session
            .set_range_execution_mode(range_execution_mode.core_mode())
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("configure range execution mode: {error}"),
                )
            })?;
        query_session.set_chunk_reader(Arc::clone(&chunk_reader))?;
        query_session
            .set_experimental_cross_segment_chunk_reads(experimental_cross_segment_chunk_reads);
        query_session.set_label_materialization_policy(label_materialization.core_policy());
        query_session
            .set_query_label_arena_max_bytes(config.query_label_arena_max_bytes)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("configure query label arena: {error}"),
                )
            })?;
        query_session.set_query_label_storage_policy(label_storage.core_policy())?;
        let query_session_open = phase_start.elapsed();
        report.query_session_open = report.query_session_open.saturating_add(query_session_open);
        if let Some(bytes) = range_scalar_cache_budget {
            query_session
                .set_range_scalar_cache_budget_bytes(bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }

        if config.prewarm_query_contexts {
            let phase_start = Instant::now();
            let session_stats_before = query_session.stats();
            let session_profile_before = query_session.profile();
            query_session
                .prewarm_promql_with_limits(query, config.start_ms, query_end_ms, config.limits)
                .map_err(|err| io::Error::other(format!("query prewarm failed: {query}: {err}")))?;
            report.query_context_prewarm = report
                .query_context_prewarm
                .saturating_add(phase_start.elapsed());
            add_session_stats(
                &mut report.query_context_prewarm_stats_delta,
                query_session.stats().delta_since(session_stats_before),
            );
            add_session_profile(
                &mut report.query_context_prewarm_profile_delta,
                query_session.profile().delta_since(session_profile_before),
            );
        }

        if config.prefetch_query_data {
            let phase_start = Instant::now();
            let session_stats_before = query_session.stats();
            let session_profile_before = query_session.profile();
            let stats = query_session
                .prefetch_promql_data_with_limits(
                    query,
                    config.start_ms,
                    query_end_ms,
                    config.limits,
                )
                .map_err(|err| {
                    io::Error::other(format!("query data prefetch failed: {query}: {err}"))
                })?;
            report.query_data_prefetch = report
                .query_data_prefetch
                .saturating_add(phase_start.elapsed());
            add_query_data_prefetch_stats(&mut report.query_data_prefetch_stats, stats);
            add_session_stats(
                &mut report.query_data_prefetch_session_stats_delta,
                query_session.stats().delta_since(session_stats_before),
            );
            add_session_profile(
                &mut report.query_data_prefetch_profile_delta,
                query_session.profile().delta_since(session_profile_before),
            );
        }

        for run_index in 0..config.benchmark_repeats {
            let session_stats_before = query_session.stats();
            let session_profile_before = query_session.profile();
            let label_storage_before = query_session.query_label_storage_stats();
            let metadata_runtime_before = store.metadata_runtime_snapshot();
            let query_start = Instant::now();
            let execution = match step_ms {
                None => query_session.query_promql_with_limits(
                    query,
                    effective_start_ms,
                    effective_end_ms,
                    config.limits,
                ),
                Some(step_ms) => query_session.query_promql_range_with_limits(
                    query,
                    effective_start_ms,
                    effective_end_ms,
                    step_ms,
                    config.limits,
                ),
            }
            .map_err(|err| io::Error::other(format!("query failed: {query}: {err}")))?;
            let duration = query_start.elapsed();
            let metadata_runtime_after = store.metadata_runtime_snapshot();
            report.promql_queries = report.promql_queries.saturating_add(duration);
            let range_scalar_cache = match step_ms {
                Some(_) => {
                    let summary = query_session
                        .last_range_scalar_cache_summary()
                        .copied()
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "range query completed without a finalized scalar cache summary",
                            )
                        })?;
                    Some(QueryBenchmarkRangeScalarCacheReport {
                        summary,
                        process_governor: range_scalar_cache_governor_stats(),
                    })
                }
                None => None,
            };
            let range_execution = match step_ms {
                Some(_) => Some(
                    query_session
                        .last_range_execution_summary()
                        .copied()
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "range query completed without a finalized execution summary",
                            )
                        })?,
                ),
                None => None,
            };
            let fingerprint_start = Instant::now();
            let semantic_fingerprint = execution.semantic_fingerprint_sha256();
            let portable_semantic_fingerprint = execution.portable_semantic_fingerprint_sha256();
            let post_query_fingerprint = fingerprint_start.elapsed();
            report.post_query_fingerprints = report
                .post_query_fingerprints
                .saturating_add(post_query_fingerprint);
            let session_stats_after = query_session.stats();
            let session_profile_after = query_session.profile();
            let label_storage_after = query_session.query_label_storage_stats();
            let label_storage_delta = label_storage_after.delta_since(label_storage_before);
            validate_query_label_storage_stats(label_storage_delta)?;
            let result_series = execution.results.len() as u64;
            let result_samples = execution
                .results
                .iter()
                .map(|result| result.samples.len() as u64)
                .sum();
            let session_profile_delta = session_profile_after.delta_since(session_profile_before);
            validate_query_stage_accounting(
                query_instrumentation,
                query,
                duration,
                session_profile_delta.stages,
            )?;
            report.results.push(QueryBenchmarkResult {
                query: query.clone(),
                run_kind: if run_index == 0 {
                    QueryBenchmarkRunKind::Cold
                } else {
                    QueryBenchmarkRunKind::Warm
                },
                run_index,
                query_session_open: if run_index == 0 {
                    query_session_open
                } else {
                    Duration::ZERO
                },
                duration,
                post_query_fingerprint,
                effective_start_ms,
                effective_end_ms,
                step_ms,
                semantic_fingerprint,
                portable_semantic_fingerprint,
                result_series,
                result_samples,
                stats: execution.stats,
                session_stats_delta: session_stats_after.delta_since(session_stats_before),
                session_profile_delta,
                label_storage_delta,
                metadata_runtime: QueryBenchmarkMetadataRuntimeReport::between(
                    metadata_runtime_before,
                    metadata_runtime_after,
                ),
                range_scalar_cache,
                range_execution,
            });
        }

        add_session_stats(&mut report.session_stats, query_session.stats());
        add_session_profile(&mut report.session_profile, query_session.profile());
    }

    let markdown = render_benchmark_markdown(config, &report).into_bytes();
    let raw = config
        .raw_output
        .as_ref()
        .map(|_| render_raw_benchmark_json(config, &report))
        .transpose()?;
    publish_benchmark_outputs(
        &config.output,
        &markdown,
        config.raw_output.as_deref().zip(raw.as_deref()),
    )?;

    Ok(report)
}

pub(in super::super) fn effective_query_end_ms(
    query: &str,
    configured_end_ms: u64,
    segment_time_range: Option<(u64, u64)>,
) -> u64 {
    if configured_end_ms != u64::MAX {
        return configured_end_ms;
    }

    if query_needs_finite_end(query)
        && let Some((_, segment_end_ms)) = segment_time_range
    {
        return segment_end_ms;
    }

    configured_end_ms
}

fn query_needs_finite_end(query: &str) -> bool {
    parse_query(query)
        .map(|query| parsed_query_needs_finite_end(&query))
        .unwrap_or(false)
}

fn parsed_query_needs_finite_end(query: &PromqlQuery) -> bool {
    match query {
        PromqlQuery::Vector(_) | PromqlQuery::Scalar(_) | PromqlQuery::Time => false,
        PromqlQuery::VectorFunction(function) => {
            parsed_query_needs_finite_end(function.input.as_ref())
        }
        PromqlQuery::ScalarFunction(function) => {
            parsed_query_needs_finite_end(function.input.as_ref())
        }
        PromqlQuery::Offset(offset) => parsed_query_needs_finite_end(offset.input.as_ref()),
        PromqlQuery::LabelReplace(function) => {
            parsed_query_needs_finite_end(function.input.as_ref())
        }
        PromqlQuery::LabelJoin(function) => parsed_query_needs_finite_end(function.input.as_ref()),
        PromqlQuery::RangeFunction(_)
        | PromqlQuery::QuantileOverTime(_)
        | PromqlQuery::PredictLinear(_)
        | PromqlQuery::DoubleExponentialSmoothing(_)
        | PromqlQuery::Aggregation(_)
        | PromqlQuery::Absent(_)
        | PromqlQuery::AbsentOverTime(_)
        | PromqlQuery::InstantFunction(_)
        | PromqlQuery::HistogramQuantile(_)
        | PromqlQuery::HistogramFraction(_)
        | PromqlQuery::HistogramScalarFunction(_) => true,
        PromqlQuery::BinaryExpression(expression) => {
            !parsed_query_is_scalar(expression.left.as_ref())
                || !parsed_query_is_scalar(expression.right.as_ref())
        }
    }
}

fn parsed_query_is_scalar(query: &PromqlQuery) -> bool {
    match query {
        PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::ScalarFunction(_) => true,
        PromqlQuery::BinaryExpression(expression) => {
            parsed_query_is_scalar(expression.left.as_ref())
                && parsed_query_is_scalar(expression.right.as_ref())
        }
        PromqlQuery::Vector(_)
        | PromqlQuery::VectorFunction(_)
        | PromqlQuery::Offset(_)
        | PromqlQuery::LabelReplace(_)
        | PromqlQuery::LabelJoin(_)
        | PromqlQuery::RangeFunction(_)
        | PromqlQuery::QuantileOverTime(_)
        | PromqlQuery::PredictLinear(_)
        | PromqlQuery::DoubleExponentialSmoothing(_)
        | PromqlQuery::Aggregation(_)
        | PromqlQuery::Absent(_)
        | PromqlQuery::AbsentOverTime(_)
        | PromqlQuery::InstantFunction(_)
        | PromqlQuery::HistogramQuantile(_)
        | PromqlQuery::HistogramFraction(_)
        | PromqlQuery::HistogramScalarFunction(_) => false,
    }
}
