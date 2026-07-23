use super::*;

const QUERY_BENCHMARK_RAW_SCHEMA_V14: &str = "chronoxide.query-benchmark.raw/v14";

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawDocumentV14 {
    schema: &'static str,
    corpus_fingerprint_sha256: String,
    corpus_fingerprint_duration_ns: u64,
    configuration: QueryBenchmarkRawConfigurationV14,
    limits: QueryBenchmarkRawLimitsV1,
    runs: Vec<QueryBenchmarkRawRunV14>,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawConfigurationV14 {
    #[serde(flatten)]
    v12: QueryBenchmarkRawConfigurationV12,
    range_execution_mode: &'static str,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawConfigurationV12 {
    #[serde(flatten)]
    v11: QueryBenchmarkRawConfigurationV11,
    chunk_payload_coalesce_max_gap_bytes: u64,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawConfigurationV11 {
    #[serde(flatten)]
    v9: QueryBenchmarkRawConfigurationV9,
    query_instrumentation: &'static str,
    query_label_arena_max_bytes: u64,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawConfigurationV9 {
    #[serde(flatten)]
    v8: QueryBenchmarkRawConfigurationV8,
    query_label_storage: &'static str,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawConfigurationV8 {
    segments_dir: String,
    start_ms: u64,
    end_ms: u64,
    mode: &'static str,
    step_ms: Option<u64>,
    range_scalar_cache_max_bytes: Option<u64>,
    chunk_read_mode: &'static str,
    chunk_read_queue_depth: u32,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: &'static str,
    storage_layout: &'static str,
    benchmark_repeats: usize,
    queries: Vec<String>,
    prewarm_query_contexts: bool,
    prefetch_query_data: bool,
    exponential_histogram_bucket_boundaries: Vec<f64>,
    requested_segment_footer_validation: bool,
    effective_segment_footer_validation: bool,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawLimitsV1 {
    max_matched_series: Option<u64>,
    max_projected_series: Option<u64>,
    max_chunk_reads: Option<u64>,
    max_bytes_read: Option<u64>,
    max_samples_decoded: Option<u64>,
    max_regex_values_examined: Option<u64>,
}

impl From<QueryLimits> for QueryBenchmarkRawLimitsV1 {
    fn from(limits: QueryLimits) -> Self {
        Self {
            max_matched_series: limits.max_matched_series,
            max_projected_series: limits.max_projected_series,
            max_chunk_reads: limits.max_chunk_reads,
            max_bytes_read: limits.max_bytes_read,
            max_samples_decoded: limits.max_samples_decoded,
            max_regex_values_examined: limits.max_regex_values_examined,
        }
    }
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawRunV5 {
    query: String,
    run_kind: &'static str,
    run_index: usize,
    duration_ns: u64,
    effective_start_ms: u64,
    effective_end_ms: u64,
    step_ms: Option<u64>,
    semantic_fingerprint_sha256: String,
    portable_semantic_fingerprint_sha256: String,
    result_series: u64,
    result_samples: u64,
    stats: RawQueryStatsV1,
    payload_reads: QueryBenchmarkRawPayloadReadsV5,
    symbol_reads: QueryBenchmarkRawSymbolReadsV5,
    label_materialization: QueryBenchmarkRawLabelMaterializationV1,
    range_scalar_cache: Option<QueryBenchmarkRawRangeScalarCacheV3>,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawRunV9 {
    #[serde(flatten)]
    v8: QueryBenchmarkRawRunV5,
    query_label_storage: QueryBenchmarkRawQueryLabelStorageV2,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawRunV11 {
    #[serde(flatten)]
    v9: QueryBenchmarkRawRunV9,
    post_query_fingerprint_ns: u64,
    query_stages: QueryBenchmarkRawQueryStagesV1,
    metadata_runtime: QueryBenchmarkMetadataRuntimeReport,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawRunV13 {
    #[serde(flatten)]
    v11: QueryBenchmarkRawRunV11,
    chunk_read_scheduler: QueryBenchmarkRawChunkReadSchedulerV2,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawRunV14 {
    #[serde(flatten)]
    v13: QueryBenchmarkRawRunV13,
    range_execution: Option<QueryBenchmarkRawRangeExecutionV1>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(in super::super) struct QueryBenchmarkRawRangeExecutionV1 {
    requested_mode: &'static str,
    effective_mode: &'static str,
    fallback_reason: Option<&'static str>,
    terminal_reason: Option<&'static str>,
    evaluation_count: u64,
    union_start_ms: Option<u64>,
    union_end_ms: Option<u64>,
    source_series: u64,
    source_samples: u64,
    estimated_retained_bytes_peak: u64,
    retained_bytes_after_finalize: u64,
    preallocation_governed: bool,
    cache_bypassed: bool,
}

pub(super) const fn range_execution_mode_name(mode: RangeExecutionMode) -> &'static str {
    match mode {
        RangeExecutionMode::Repeated => "repeated",
        RangeExecutionMode::OnePassAssumeScalar => "one-pass-assume-scalar",
    }
}

impl From<RangeExecutionSummary> for QueryBenchmarkRawRangeExecutionV1 {
    fn from(summary: RangeExecutionSummary) -> Self {
        Self {
            requested_mode: range_execution_mode_name(summary.requested_mode),
            effective_mode: range_execution_mode_name(summary.effective_mode),
            fallback_reason: summary.fallback_reason.map(|reason| reason.as_str()),
            terminal_reason: summary.terminal_reason.map(|reason| reason.as_str()),
            evaluation_count: summary.evaluation_count,
            union_start_ms: summary.union_start_ms,
            union_end_ms: summary.union_end_ms,
            source_series: summary.source_series,
            source_samples: summary.source_samples,
            estimated_retained_bytes_peak: summary.estimated_retained_bytes_peak,
            retained_bytes_after_finalize: summary.retained_bytes_after_finalize,
            preallocation_governed: summary.preallocation_governed,
            cache_bypassed: summary.cache_bypassed,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(in super::super) struct QueryBenchmarkRawChunkReadSchedulerV2 {
    executions: u64,
    pread_decisions: u64,
    io_uring_decisions: u64,
    logical_requests: u64,
    physical_spans: u64,
    backend_submissions: u64,
    sqes_submitted: u64,
    submission_depth_sum: u64,
    /// Session high-water observed through this run. Maxima cannot be
    /// subtracted from the cumulative session profile.
    session_submission_depth_high_water: u64,
    submission_depth_1: u64,
    submission_depth_2_3: u64,
    submission_depth_4_7: u64,
    submission_depth_8_plus: u64,
    total_physical_bytes_executed: u64,
    /// Session high-water observed through this run. For the fixed-plan
    /// repeated benchmark schedule this is also the run-local peak.
    session_peak_in_flight_bytes_high_water: u64,
}

impl From<ChunkReadSchedulerProfile> for QueryBenchmarkRawChunkReadSchedulerV2 {
    fn from(profile: ChunkReadSchedulerProfile) -> Self {
        Self {
            executions: profile.executions,
            pread_decisions: profile.pread_decisions,
            io_uring_decisions: profile.io_uring_decisions,
            logical_requests: profile.logical_requests,
            physical_spans: profile.physical_spans,
            backend_submissions: profile.backend_submissions,
            sqes_submitted: profile.sqes_submitted,
            submission_depth_sum: profile.submission_depth_sum,
            session_submission_depth_high_water: profile.submission_depth_max,
            submission_depth_1: profile.submission_depth_1,
            submission_depth_2_3: profile.submission_depth_2_3,
            submission_depth_4_7: profile.submission_depth_4_7,
            submission_depth_8_plus: profile.submission_depth_8_plus,
            total_physical_bytes_executed: profile.total_physical_bytes_executed,
            session_peak_in_flight_bytes_high_water: profile.peak_in_flight_bytes,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct QueryBenchmarkRawQueryStagesV1 {
    canonical_row_decode_ns: u64,
    candidate_selection_ns: u64,
    metadata_visit_overhead_ns: u64,
    symbol_lookup_ns: u64,
    symbol_resolution_ns: u64,
    canonical_identity_ns: u64,
    matcher_evaluation_ns: u64,
    label_construction_ns: u64,
    locator_planning_ns: u64,
    payload_read_pipeline_combined_ns: u64,
    payload_decode_projection_result_processing_combined_ns: u64,
    source_merge_ns: u64,
    promql_grouping_evaluation_ns: u64,
    result_construction_ns: u64,
    exclusive_total_ns: u64,
    unclassified_ns: u64,
}

impl QueryBenchmarkRawQueryStagesV1 {
    fn from_result(result: &QueryBenchmarkResult) -> io::Result<Self> {
        let stages = result.session_profile_delta.stages;
        let exclusive_total = stages.total_exclusive();
        Ok(Self {
            canonical_row_decode_ns: duration_ns_u64(
                stages.canonical_row_decode,
                "canonical row decode stage",
            )?,
            candidate_selection_ns: duration_ns_u64(
                stages.candidate_selection,
                "candidate selection stage",
            )?,
            metadata_visit_overhead_ns: duration_ns_u64(
                stages.metadata_visit_overhead,
                "metadata visit overhead stage",
            )?,
            symbol_lookup_ns: duration_ns_u64(stages.symbol_lookup, "symbol lookup stage")?,
            symbol_resolution_ns: duration_ns_u64(
                stages.symbol_resolution,
                "symbol resolution stage",
            )?,
            canonical_identity_ns: duration_ns_u64(
                stages.canonical_identity,
                "canonical identity stage",
            )?,
            matcher_evaluation_ns: duration_ns_u64(
                stages.matcher_evaluation,
                "matcher evaluation stage",
            )?,
            label_construction_ns: duration_ns_u64(
                stages.label_construction,
                "label construction stage",
            )?,
            locator_planning_ns: duration_ns_u64(
                stages.locator_planning,
                "locator planning stage",
            )?,
            payload_read_pipeline_combined_ns: duration_ns_u64(
                stages.payload_io,
                "combined payload read-pipeline stage",
            )?,
            payload_decode_projection_result_processing_combined_ns: duration_ns_u64(
                stages.payload_decode,
                "combined payload decode, projection, and result-processing stage",
            )?,
            source_merge_ns: duration_ns_u64(stages.source_merge, "source merge stage")?,
            promql_grouping_evaluation_ns: duration_ns_u64(
                stages.promql_grouping_evaluation,
                "PromQL grouping/evaluation stage",
            )?,
            result_construction_ns: duration_ns_u64(
                stages.result_construction,
                "result construction stage",
            )?,
            exclusive_total_ns: duration_ns_u64(exclusive_total, "exclusive stage total")?,
            unclassified_ns: duration_ns_u64(
                result.duration.saturating_sub(exclusive_total),
                "unclassified query duration",
            )?,
        })
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(in super::super) struct QueryBenchmarkRawQueryLabelStorageV2 {
    label_sets: u64,
    atom_lookups: u64,
    atom_hits: u64,
    atom_misses: u64,
    unique_content_bytes: u64,
    compact_label_sets: u64,
    compact_pairs: u64,
    compact_source_symbol_translations: u64,
    compact_source_symbol_translation_hits: u64,
    compact_source_symbol_translation_misses: u64,
    compact_atom_lookups: u64,
    compact_atom_hits: u64,
    compact_atom_misses: u64,
    compact_unique_strings: u64,
    compact_unique_content_bytes: u64,
    compact_arena_budget_bytes: u64,
    compact_arena_current_bytes: u64,
    compact_arena_peak_bytes: u64,
    compact_atom_bytes: u64,
    compact_pair_bytes: u64,
    compact_hash_directory_bytes: u64,
    compact_translation_bytes: u64,
    compact_retained_bytes: u64,
    compact_arena_admission_refusals: u64,
    compact_compatibility_materializations: u64,
}

impl From<QueryLabelStorageStats> for QueryBenchmarkRawQueryLabelStorageV2 {
    fn from(stats: QueryLabelStorageStats) -> Self {
        Self {
            label_sets: stats.label_sets,
            atom_lookups: stats.atom_lookups,
            atom_hits: stats.atom_hits,
            atom_misses: stats.atom_misses,
            unique_content_bytes: stats.unique_content_bytes,
            compact_label_sets: stats.compact_label_sets,
            compact_pairs: stats.compact_pairs,
            compact_source_symbol_translations: stats.compact_source_symbol_translations,
            compact_source_symbol_translation_hits: stats.compact_source_symbol_translation_hits,
            compact_source_symbol_translation_misses: stats
                .compact_source_symbol_translation_misses,
            compact_atom_lookups: stats.compact_atom_lookups,
            compact_atom_hits: stats.compact_atom_hits,
            compact_atom_misses: stats.compact_atom_misses,
            compact_unique_strings: stats.compact_unique_strings,
            compact_unique_content_bytes: stats.compact_unique_content_bytes,
            compact_arena_budget_bytes: stats.compact_arena_budget_bytes,
            compact_arena_current_bytes: stats.compact_arena_current_bytes,
            compact_arena_peak_bytes: stats.compact_arena_peak_bytes,
            compact_atom_bytes: stats.compact_atom_bytes,
            compact_pair_bytes: stats.compact_pair_bytes,
            compact_hash_directory_bytes: stats.compact_hash_directory_bytes,
            compact_translation_bytes: stats.compact_translation_bytes,
            compact_retained_bytes: stats.compact_retained_bytes,
            compact_arena_admission_refusals: stats.compact_arena_admission_refusals,
            compact_compatibility_materializations: stats.compact_compatibility_materializations,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct QueryBenchmarkRawLabelMaterializationV1 {
    rows_integrity_checked: u64,
    pairs_integrity_checked: u64,
    rows_full_materialized: u64,
    rows_selectively_materialized: u64,
    pairs_materialized: u64,
    pairs_omitted: u64,
    content_bytes_materialized: u64,
}

impl From<SegmentStoreQueryProfile> for QueryBenchmarkRawLabelMaterializationV1 {
    fn from(profile: SegmentStoreQueryProfile) -> Self {
        Self {
            rows_integrity_checked: profile.label_rows_integrity_checked,
            pairs_integrity_checked: profile.label_pairs_integrity_checked,
            rows_full_materialized: profile.label_rows_full_materialized,
            rows_selectively_materialized: profile.label_rows_selectively_materialized,
            pairs_materialized: profile.label_pairs_materialized,
            pairs_omitted: profile.label_pairs_omitted,
            content_bytes_materialized: profile.label_content_bytes_materialized,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct QueryBenchmarkRawPayloadReadsV5 {
    logical_used_bytes: u64,
    physical_reads: u64,
    physical_bytes: u64,
}

impl From<SegmentStoreQueryProfile> for QueryBenchmarkRawPayloadReadsV5 {
    fn from(profile: SegmentStoreQueryProfile) -> Self {
        Self {
            logical_used_bytes: profile.chunk_payload_bytes,
            physical_reads: profile.chunk_payload_physical_reads,
            physical_bytes: profile.chunk_payload_physical_bytes,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct QueryBenchmarkRawReadCountV5 {
    calls: u64,
    bytes: u64,
}

impl From<SegmentSymbolReadCount> for QueryBenchmarkRawReadCountV5 {
    fn from(count: SegmentSymbolReadCount) -> Self {
        Self {
            calls: count.calls,
            bytes: count.bytes,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(in super::super) struct QueryBenchmarkRawSymbolReadsV5 {
    legacy_eager_read_delta: QueryBenchmarkRawReadCountV5,
    logical_returned_delta: QueryBenchmarkRawReadCountV5,
    root_read_delta: QueryBenchmarkRawReadCountV5,
    page_read_delta: QueryBenchmarkRawReadCountV5,
    page_validation_delta: QueryBenchmarkRawReadCountV5,
    page_validation_ns_delta: u64,
    touched_corrupt_pages_delta: u64,
    page_cache_hits_delta: u64,
    page_cache_misses_delta: u64,
    page_cache_evictions_delta: u64,
    retained_readers_after_run: u64,
    retained_open_files_after_run: u64,
    source_file_bytes_after_run: u64,
    root_encoded_bytes_after_run: u64,
    root_retained_charge_bytes_after_run: u64,
    eager_dictionary_retained_charge_bytes_after_run: u64,
    page_cache_charge_bytes_after_run: u64,
    page_cache_max_bytes_after_run: u64,
    total_retained_charge_bytes_after_run: u64,
    resource_snapshot_errors_after_run: u64,
}

impl From<SegmentStoreQueryProfile> for QueryBenchmarkRawSymbolReadsV5 {
    fn from(profile: SegmentStoreQueryProfile) -> Self {
        let stats = profile.symbol_read_stats;
        let resources = profile.symbol_resources;
        Self {
            legacy_eager_read_delta: stats.legacy_eager.into(),
            logical_returned_delta: stats.logical_returned.into(),
            root_read_delta: stats.root.into(),
            page_read_delta: stats.page.into(),
            page_validation_delta: stats.page_validation.into(),
            page_validation_ns_delta: stats.page_validation_ns,
            touched_corrupt_pages_delta: stats.touched_corrupt_pages,
            page_cache_hits_delta: stats.page_cache_hits,
            page_cache_misses_delta: stats.page_cache_misses,
            page_cache_evictions_delta: stats.page_cache_evictions,
            retained_readers_after_run: resources.retained_readers,
            retained_open_files_after_run: resources.retained_open_files,
            source_file_bytes_after_run: resources.source_file_bytes,
            root_encoded_bytes_after_run: resources.root_encoded_bytes,
            root_retained_charge_bytes_after_run: resources.root_retained_charge_bytes,
            eager_dictionary_retained_charge_bytes_after_run: resources
                .eager_dictionary_retained_charge_bytes,
            page_cache_charge_bytes_after_run: resources.page_cache_charge_bytes,
            page_cache_max_bytes_after_run: resources.page_cache_max_bytes,
            total_retained_charge_bytes_after_run: resources.total_retained_charge_bytes(),
            resource_snapshot_errors_after_run: resources.snapshot_errors,
        }
    }
}

#[derive(Debug, Serialize)]
pub(in super::super) struct QueryBenchmarkRawRangeScalarCacheV3 {
    configured_budget_bytes: u64,
    governor_lease_bytes: u64,
    governor_refused: bool,
    allocation_refused: bool,
    layout_overflow: bool,
    entry_arena_charge_bytes: u64,
    sample_arena_charge_bytes: u64,
    hits: u64,
    misses: u64,
    admitted_entries: u64,
    streaming_budget_bypasses: u64,
    unsupported_bypasses: u64,
    logical_hit_bytes: u64,
    logical_miss_or_bypass_bytes: u64,
    peak_retained_charge_bytes: u64,
    retained_charge_after_finalize: u64,
    process_governor_limit_bytes: u64,
    process_governor_current_leased_bytes: u64,
    process_governor_lifetime_peak_leased_bytes: u64,
}

impl From<QueryBenchmarkRangeScalarCacheReport> for QueryBenchmarkRawRangeScalarCacheV3 {
    fn from(report: QueryBenchmarkRangeScalarCacheReport) -> Self {
        let summary = report.summary;
        let governor = report.process_governor;
        Self {
            configured_budget_bytes: summary.configured_budget_bytes,
            governor_lease_bytes: summary.governor_lease_bytes,
            governor_refused: summary.governor_refused,
            allocation_refused: summary.allocation_refused,
            layout_overflow: summary.layout_overflow,
            entry_arena_charge_bytes: summary.entry_arena_charge_bytes,
            sample_arena_charge_bytes: summary.sample_arena_charge_bytes,
            hits: summary.hits,
            misses: summary.misses,
            admitted_entries: summary.admitted_entries,
            streaming_budget_bypasses: summary.streaming_budget_bypasses,
            unsupported_bypasses: summary.unsupported_bypasses,
            logical_hit_bytes: summary.logical_hit_bytes,
            logical_miss_or_bypass_bytes: summary.logical_miss_or_bypass_bytes,
            peak_retained_charge_bytes: summary.peak_retained_charge_bytes,
            retained_charge_after_finalize: summary.retained_charge_after_finalize,
            process_governor_limit_bytes: governor.limit_bytes,
            process_governor_current_leased_bytes: governor.current_leased_bytes,
            process_governor_lifetime_peak_leased_bytes: governor.peak_leased_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
pub(in super::super) struct RawQueryStatsV1 {
    segments_considered: u64,
    segments_skipped_by_time: u64,
    segments_skipped_by_missing_equality: u64,
    segments_skipped_by_matcher_time_range: u64,
    segments_queried: u64,
    matched_series: u64,
    projected_series: u64,
    chunk_reads: u64,
    bytes_read: u64,
    samples_decoded: u64,
    typed_scalar_chunks_decoded: u64,
    typed_full_chunks_decoded: u64,
    regex_values_examined: u64,
    index_postings_reads: u64,
    index_postings_bytes_read: u64,
}

impl From<QueryStats> for RawQueryStatsV1 {
    fn from(stats: QueryStats) -> Self {
        Self {
            segments_considered: stats.segments_considered,
            segments_skipped_by_time: stats.segments_skipped_by_time,
            segments_skipped_by_missing_equality: stats.segments_skipped_by_missing_equality,
            segments_skipped_by_matcher_time_range: stats.segments_skipped_by_matcher_time_range,
            segments_queried: stats.segments_queried,
            matched_series: stats.matched_series,
            projected_series: stats.projected_series,
            chunk_reads: stats.chunk_reads,
            bytes_read: stats.bytes_read,
            samples_decoded: stats.samples_decoded,
            typed_scalar_chunks_decoded: stats.typed_scalar_chunks_decoded,
            typed_full_chunks_decoded: stats.typed_full_chunks_decoded,
            regex_values_examined: stats.regex_values_examined,
            index_postings_reads: stats.index_postings_reads,
            index_postings_bytes_read: stats.index_postings_bytes_read,
        }
    }
}

pub(super) fn render_raw_benchmark_json(
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) -> io::Result<Vec<u8>> {
    let document = QueryBenchmarkRawDocumentV14 {
        schema: QUERY_BENCHMARK_RAW_SCHEMA_V14,
        corpus_fingerprint_sha256: report.corpus_fingerprint.to_hex(),
        corpus_fingerprint_duration_ns: duration_ns_u64(
            report.corpus_fingerprint_duration,
            "corpus fingerprint duration",
        )?,
        configuration: QueryBenchmarkRawConfigurationV14 {
            v12: QueryBenchmarkRawConfigurationV12 {
                v11: QueryBenchmarkRawConfigurationV11 {
                    v9: QueryBenchmarkRawConfigurationV9 {
                        v8: QueryBenchmarkRawConfigurationV8 {
                            segments_dir: config
                                .segments_dir
                                .to_str()
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        "segments directory is not valid UTF-8",
                                    )
                                })?
                                .to_owned(),
                            start_ms: config.start_ms,
                            end_ms: config.end_ms,
                            mode: query_benchmark_mode_name(config.mode),
                            step_ms: match config.mode {
                                QueryBenchmarkMode::Instant => None,
                                QueryBenchmarkMode::Range { step_ms } => Some(step_ms),
                            },
                            range_scalar_cache_max_bytes: resolve_range_scalar_cache_budget(
                                config.range_scalar_cache_max_bytes,
                                Some(config.mode),
                            )?,
                            chunk_read_mode: config.chunk_read_mode.name(),
                            chunk_read_queue_depth: config.chunk_read_queue_depth,
                            experimental_cross_segment_chunk_reads: report
                                .experimental_cross_segment_chunk_reads,
                            label_materialization: report.label_materialization.name(),
                            storage_layout: report.storage_layout.name(),
                            benchmark_repeats: config.benchmark_repeats,
                            queries: config.queries.clone(),
                            prewarm_query_contexts: config.prewarm_query_contexts,
                            prefetch_query_data: config.prefetch_query_data,
                            exponential_histogram_bucket_boundaries: config
                                .exponential_histogram_bucket_boundaries
                                .clone(),
                            requested_segment_footer_validation: config.validate_segment_footers,
                            effective_segment_footer_validation: config.validate_segment_footers
                                || report.storage_layout.forces_footer_validation(),
                        },
                        query_label_storage: report.label_storage.name(),
                    },
                    query_instrumentation: report.query_instrumentation.name(),
                    query_label_arena_max_bytes: config.query_label_arena_max_bytes,
                },
                chunk_payload_coalesce_max_gap_bytes: config.chunk_payload_coalesce_max_gap_bytes,
            },
            range_execution_mode: report.range_execution_mode.name(),
        },
        limits: QueryBenchmarkRawLimitsV1::from(config.limits),
        runs: report
            .results
            .iter()
            .map(|result| {
                Ok(QueryBenchmarkRawRunV14 {
                    v13: QueryBenchmarkRawRunV13 {
                        v11: QueryBenchmarkRawRunV11 {
                            v9: QueryBenchmarkRawRunV9 {
                                v8: QueryBenchmarkRawRunV5 {
                                    query: result.query.clone(),
                                    run_kind: raw_run_kind_name(result.run_kind),
                                    run_index: result.run_index,
                                    duration_ns: duration_ns_u64(
                                        result.duration,
                                        "query duration",
                                    )?,
                                    effective_start_ms: result.effective_start_ms,
                                    effective_end_ms: result.effective_end_ms,
                                    step_ms: result.step_ms,
                                    semantic_fingerprint_sha256: result
                                        .semantic_fingerprint
                                        .to_hex(),
                                    portable_semantic_fingerprint_sha256: result
                                        .portable_semantic_fingerprint
                                        .to_hex(),
                                    result_series: result.result_series,
                                    result_samples: result.result_samples,
                                    stats: RawQueryStatsV1::from(result.stats),
                                    payload_reads: QueryBenchmarkRawPayloadReadsV5::from(
                                        result.session_profile_delta,
                                    ),
                                    symbol_reads: QueryBenchmarkRawSymbolReadsV5::from(
                                        result.session_profile_delta,
                                    ),
                                    label_materialization:
                                        QueryBenchmarkRawLabelMaterializationV1::from(
                                            result.session_profile_delta,
                                        ),
                                    range_scalar_cache: result
                                        .range_scalar_cache
                                        .map(QueryBenchmarkRawRangeScalarCacheV3::from),
                                },
                                query_label_storage: QueryBenchmarkRawQueryLabelStorageV2::from(
                                    result.label_storage_delta,
                                ),
                            },
                            post_query_fingerprint_ns: duration_ns_u64(
                                result.post_query_fingerprint,
                                "post-query fingerprint duration",
                            )?,
                            query_stages: QueryBenchmarkRawQueryStagesV1::from_result(result)?,
                            metadata_runtime: result.metadata_runtime.clone(),
                        },
                        chunk_read_scheduler: QueryBenchmarkRawChunkReadSchedulerV2::from(
                            result.session_profile_delta.chunk_read_scheduler,
                        ),
                    },
                    range_execution: result
                        .range_execution
                        .map(QueryBenchmarkRawRangeExecutionV1::from),
                })
            })
            .collect::<io::Result<Vec<_>>>()?,
    };
    let mut bytes = serde_json::to_vec_pretty(&document)
        .map_err(|error| io::Error::other(format!("serialize raw query benchmark: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn duration_ns_u64(duration: Duration, field: &str) -> io::Result<u64> {
    u64::try_from(duration.as_nanos()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} does not fit in u64 nanoseconds"),
        )
    })
}
