#[derive(Debug, Clone, PartialEq)]
struct QueryBenchmarkConfig {
    segments_dir: PathBuf,
    output: PathBuf,
    raw_output: Option<PathBuf>,
    start_ms: u64,
    end_ms: u64,
    mode: QueryBenchmarkMode,
    range_scalar_cache_max_bytes: Option<u64>,
    chunk_read_mode: ChunkReadModeArg,
    chunk_read_queue_depth: u32,
    queries: Vec<String>,
    benchmark_repeats: usize,
    prewarm_query_contexts: bool,
    prefetch_query_data: bool,
    exponential_histogram_bucket_boundaries: Vec<f64>,
    limits: QueryLimits,
    validate_segment_footers: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryBenchmarkMode {
    Instant,
    Range { step_ms: u64 },
}

#[derive(Debug, Clone, PartialEq)]
struct QueryBenchmarkReport {
    store_open: Duration,
    corpus_fingerprint: SegmentCorpusFingerprint,
    corpus_fingerprint_duration: Duration,
    query_session_open: Duration,
    query_context_prewarm: Duration,
    query_context_prewarm_stats_delta: SegmentStoreQuerySessionStats,
    query_context_prewarm_profile_delta: SegmentStoreQueryProfile,
    query_data_prefetch: Duration,
    query_data_prefetch_stats: QueryDataPrefetchStats,
    query_data_prefetch_session_stats_delta: SegmentStoreQuerySessionStats,
    query_data_prefetch_profile_delta: SegmentStoreQueryProfile,
    promql_queries: Duration,
    post_query_fingerprints: Duration,
    session_stats: SegmentStoreQuerySessionStats,
    session_profile: SegmentStoreQueryProfile,
    results: Vec<QueryBenchmarkResult>,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: LabelMaterializationArg,
    label_storage: LabelStorageArg,
    storage_layout: StorageLayoutArg,
    query_instrumentation: QueryInstrumentationArg,
}

#[derive(Debug, Clone, PartialEq)]
struct QueryBenchmarkResult {
    query: String,
    run_kind: QueryBenchmarkRunKind,
    run_index: usize,
    query_session_open: Duration,
    duration: Duration,
    post_query_fingerprint: Duration,
    effective_start_ms: u64,
    effective_end_ms: u64,
    step_ms: Option<u64>,
    semantic_fingerprint: QueryExecutionFingerprint,
    portable_semantic_fingerprint: QueryExecutionFingerprint,
    result_series: u64,
    result_samples: u64,
    stats: QueryStats,
    session_stats_delta: SegmentStoreQuerySessionStats,
    session_profile_delta: SegmentStoreQueryProfile,
    label_storage_delta: QueryLabelStorageStats,
    metadata_runtime: QueryBenchmarkMetadataRuntimeReport,
    range_scalar_cache: Option<QueryBenchmarkRangeScalarCacheReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataRuntimeReport {
    counters_delta: QueryBenchmarkMetadataRuntimeCounterDeltas,
    start_gauges: QueryBenchmarkMetadataRuntimeGauges,
    end_gauges: QueryBenchmarkMetadataRuntimeGauges,
    lifetime_peaks_after_run: QueryBenchmarkMetadataRuntimeLifetimePeaks,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataRuntimeCounterDeltas {
    cache: QueryBenchmarkMetadataCacheCounterDeltas,
    governor: QueryBenchmarkMetadataGovernorCounterDeltas,
    file_manager: QueryBenchmarkMetadataFileManagerCounterDeltas,
    reads: QueryBenchmarkMetadataReadDeltas,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataCacheCounterDeltas {
    hits: u64,
    misses: u64,
    evictions: u64,
    single_flight_waits: u64,
    successful_loads: u64,
    failed_loads: u64,
    corruption_detections: u64,
    corruption_hits: u64,
    resident_admissions: u64,
    resident_admission_refusals: u64,
    resident_admission_bypasses: u64,
    class_admissions: Vec<QueryBenchmarkMetadataCacheClassAdmissionDeltas>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataCacheClassAdmissionDeltas {
    class: &'static str,
    resident_admissions: u64,
    resident_admission_refusals: u64,
    resident_admission_bypasses: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataGovernorCounterDeltas {
    retained_refusals: u64,
    in_flight_refusals: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataFileManagerCounterDeltas {
    preflight_calls: u64,
    successful_preflights: u64,
    preflight_failures: u64,
    acquire_calls: u64,
    successful_acquires: u64,
    requested_handles: u64,
    deduplicated_handles: u64,
    descriptor_opens: u64,
    descriptor_closes: u64,
    descriptor_reuses: u64,
    lease_clones: u64,
    idle_evictions: u64,
    capacity_waits: u64,
    capacity_refusals: u64,
    open_failures: u64,
    structural_replacements: u64,
    acquisition_rollbacks: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataReadDeltas {
    issued: QueryBenchmarkMetadataReadCount,
    unclassified: QueryBenchmarkMetadataReadCount,
    by_file: Vec<QueryBenchmarkMetadataFileRead>,
    by_class: Vec<QueryBenchmarkMetadataClassRead>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataReadCount {
    calls: u64,
    bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataFileRead {
    file: &'static str,
    calls: u64,
    bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataClassRead {
    class: &'static str,
    calls: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataRuntimeGauges {
    cache: QueryBenchmarkMetadataCacheEndGauges,
    governor: QueryBenchmarkMetadataGovernorEndGauges,
    file_manager: QueryBenchmarkMetadataFileManagerEndGauges,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataCacheEndGauges {
    resident_entries: u64,
    live_allocations: u64,
    active_loads: u64,
    registered_artifacts: u64,
    ledger_reserved_bytes: u64,
    ledger_in_flight_bytes: u64,
    ledger_retained_bytes: u64,
    sticky_artifacts: u64,
    sticky_charged_bytes: u64,
    class_charges: Vec<QueryBenchmarkMetadataCacheClassEndGauge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataCacheClassEndGauge {
    class: &'static str,
    in_flight_bytes: u64,
    retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataGovernorEndGauges {
    retained_max_bytes: u64,
    in_flight_max_bytes: u64,
    retained_bytes: u64,
    in_flight_bytes: u64,
    usage_charges: Vec<QueryBenchmarkMetadataUsageEndGauge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataUsageEndGauge {
    usage: &'static str,
    in_flight_bytes: u64,
    retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataFileManagerEndGauges {
    max_open_files: u32,
    max_cached_open_files: u32,
    open_files: u32,
    occupied_open_slots: u32,
    active_open_files: u32,
    cached_open_files: u32,
    opening_files: u32,
    pending_open_files: u32,
    preflighting_files: u32,
    closing_files: u32,
    active_leases: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataRuntimeLifetimePeaks {
    cache_class_charges: Vec<QueryBenchmarkMetadataCacheClassLifetimePeak>,
    governor: QueryBenchmarkMetadataGovernorLifetimePeaks,
    file_manager: QueryBenchmarkMetadataFileManagerLifetimePeaks,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataCacheClassLifetimePeak {
    class: &'static str,
    peak_in_flight_bytes: u64,
    peak_retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataGovernorLifetimePeaks {
    peak_retained_bytes: u64,
    peak_in_flight_bytes: u64,
    usage_charges: Vec<QueryBenchmarkMetadataUsageLifetimePeak>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataUsageLifetimePeak {
    usage: &'static str,
    peak_in_flight_bytes: u64,
    peak_retained_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct QueryBenchmarkMetadataFileManagerLifetimePeaks {
    peak_open_files: u32,
    peak_occupied_open_slots: u32,
    peak_active_open_files: u32,
    peak_cached_open_files: u32,
    peak_active_leases: u32,
    peak_preflighting_files: u32,
}

impl QueryBenchmarkMetadataRuntimeReport {
    fn between(before: StoreMetadataRuntimeSnapshot, after: StoreMetadataRuntimeSnapshot) -> Self {
        let reads = after.reads.delta_since(before.reads);
        Self {
            counters_delta: QueryBenchmarkMetadataRuntimeCounterDeltas {
                cache: QueryBenchmarkMetadataCacheCounterDeltas {
                    hits: after.cache.hits.saturating_sub(before.cache.hits),
                    misses: after.cache.misses.saturating_sub(before.cache.misses),
                    evictions: after.cache.evictions.saturating_sub(before.cache.evictions),
                    single_flight_waits: after
                        .cache
                        .single_flight_waits
                        .saturating_sub(before.cache.single_flight_waits),
                    successful_loads: after
                        .cache
                        .successful_loads
                        .saturating_sub(before.cache.successful_loads),
                    failed_loads: after
                        .cache
                        .failed_loads
                        .saturating_sub(before.cache.failed_loads),
                    corruption_detections: after
                        .cache
                        .corruption_detections
                        .saturating_sub(before.cache.corruption_detections),
                    corruption_hits: after
                        .cache
                        .corruption_hits
                        .saturating_sub(before.cache.corruption_hits),
                    resident_admissions: after
                        .cache
                        .resident_admissions
                        .saturating_sub(before.cache.resident_admissions),
                    resident_admission_refusals: after
                        .cache
                        .resident_admission_refusals
                        .saturating_sub(before.cache.resident_admission_refusals),
                    resident_admission_bypasses: after
                        .cache
                        .resident_admission_bypasses
                        .saturating_sub(before.cache.resident_admission_bypasses),
                    class_admissions: after
                        .cache
                        .class_admissions
                        .into_iter()
                        .enumerate()
                        .map(|(index, counters)| {
                            let before = before.cache.class_admissions[index];
                            QueryBenchmarkMetadataCacheClassAdmissionDeltas {
                                class: metadata_cache_class_name(counters.class),
                                resident_admissions: counters
                                    .resident_admissions
                                    .saturating_sub(before.resident_admissions),
                                resident_admission_refusals: counters
                                    .resident_admission_refusals
                                    .saturating_sub(before.resident_admission_refusals),
                                resident_admission_bypasses: counters
                                    .resident_admission_bypasses
                                    .saturating_sub(before.resident_admission_bypasses),
                            }
                        })
                        .collect(),
                },
                governor: QueryBenchmarkMetadataGovernorCounterDeltas {
                    retained_refusals: after
                        .governor
                        .retained_refusals
                        .saturating_sub(before.governor.retained_refusals),
                    in_flight_refusals: after
                        .governor
                        .in_flight_refusals
                        .saturating_sub(before.governor.in_flight_refusals),
                },
                file_manager: QueryBenchmarkMetadataFileManagerCounterDeltas {
                    preflight_calls: after
                        .files
                        .preflight_calls
                        .saturating_sub(before.files.preflight_calls),
                    successful_preflights: after
                        .files
                        .successful_preflights
                        .saturating_sub(before.files.successful_preflights),
                    preflight_failures: after
                        .files
                        .preflight_failures
                        .saturating_sub(before.files.preflight_failures),
                    acquire_calls: after
                        .files
                        .acquire_calls
                        .saturating_sub(before.files.acquire_calls),
                    successful_acquires: after
                        .files
                        .successful_acquires
                        .saturating_sub(before.files.successful_acquires),
                    requested_handles: after
                        .files
                        .requested_handles
                        .saturating_sub(before.files.requested_handles),
                    deduplicated_handles: after
                        .files
                        .deduplicated_handles
                        .saturating_sub(before.files.deduplicated_handles),
                    descriptor_opens: after
                        .files
                        .descriptor_opens
                        .saturating_sub(before.files.descriptor_opens),
                    descriptor_closes: after
                        .files
                        .descriptor_closes
                        .saturating_sub(before.files.descriptor_closes),
                    descriptor_reuses: after
                        .files
                        .descriptor_reuses
                        .saturating_sub(before.files.descriptor_reuses),
                    lease_clones: after
                        .files
                        .lease_clones
                        .saturating_sub(before.files.lease_clones),
                    idle_evictions: after
                        .files
                        .idle_evictions
                        .saturating_sub(before.files.idle_evictions),
                    capacity_waits: after
                        .files
                        .capacity_waits
                        .saturating_sub(before.files.capacity_waits),
                    capacity_refusals: after
                        .files
                        .capacity_refusals
                        .saturating_sub(before.files.capacity_refusals),
                    open_failures: after
                        .files
                        .open_failures
                        .saturating_sub(before.files.open_failures),
                    structural_replacements: after
                        .files
                        .structural_replacements
                        .saturating_sub(before.files.structural_replacements),
                    acquisition_rollbacks: after
                        .files
                        .acquisition_rollbacks
                        .saturating_sub(before.files.acquisition_rollbacks),
                },
                reads: QueryBenchmarkMetadataReadDeltas {
                    issued: QueryBenchmarkMetadataReadCount {
                        calls: reads.issued.calls,
                        bytes: reads.issued.bytes,
                    },
                    unclassified: QueryBenchmarkMetadataReadCount {
                        calls: reads.unclassified.calls,
                        bytes: reads.unclassified.bytes,
                    },
                    by_file: reads
                        .files
                        .into_iter()
                        .map(|entry| QueryBenchmarkMetadataFileRead {
                            file: entry.file.filename(),
                            calls: entry.issued.calls,
                            bytes: entry.issued.bytes,
                        })
                        .collect(),
                    by_class: reads
                        .classes
                        .into_iter()
                        .map(|entry| QueryBenchmarkMetadataClassRead {
                            class: metadata_cache_class_name(entry.class),
                            calls: entry.issued.calls,
                            bytes: entry.issued.bytes,
                        })
                        .collect(),
                },
            },
            start_gauges: metadata_runtime_gauges(&before),
            end_gauges: metadata_runtime_gauges(&after),
            lifetime_peaks_after_run: QueryBenchmarkMetadataRuntimeLifetimePeaks {
                cache_class_charges: after
                    .cache
                    .class_charges
                    .into_iter()
                    .map(|charge| QueryBenchmarkMetadataCacheClassLifetimePeak {
                        class: metadata_cache_class_name(charge.class),
                        peak_in_flight_bytes: charge.peak_in_flight_bytes,
                        peak_retained_bytes: charge.peak_retained_bytes,
                    })
                    .collect(),
                governor: QueryBenchmarkMetadataGovernorLifetimePeaks {
                    peak_retained_bytes: after.governor.peak_retained_bytes,
                    peak_in_flight_bytes: after.governor.peak_in_flight_bytes,
                    usage_charges: after
                        .governor
                        .usage
                        .into_iter()
                        .map(|charge| QueryBenchmarkMetadataUsageLifetimePeak {
                            usage: metadata_usage_class_name(charge.usage),
                            peak_in_flight_bytes: charge.peak_in_flight_bytes,
                            peak_retained_bytes: charge.peak_retained_bytes,
                        })
                        .collect(),
                },
                file_manager: QueryBenchmarkMetadataFileManagerLifetimePeaks {
                    peak_open_files: after.files.peak_open_files,
                    peak_occupied_open_slots: after.files.peak_occupied_open_slots,
                    peak_active_open_files: after.files.peak_active_open_files,
                    peak_cached_open_files: after.files.peak_cached_open_files,
                    peak_active_leases: after.files.peak_active_leases,
                    peak_preflighting_files: after.files.peak_preflighting_files,
                },
            },
        }
    }
}

fn metadata_runtime_gauges(
    snapshot: &StoreMetadataRuntimeSnapshot,
) -> QueryBenchmarkMetadataRuntimeGauges {
    QueryBenchmarkMetadataRuntimeGauges {
        cache: QueryBenchmarkMetadataCacheEndGauges {
            resident_entries: snapshot.cache.resident_entries,
            live_allocations: snapshot.cache.live_allocations,
            active_loads: snapshot.cache.active_loads,
            registered_artifacts: snapshot.cache.registered_artifacts,
            ledger_reserved_bytes: snapshot.cache.ledger_reserved_bytes,
            ledger_in_flight_bytes: snapshot.cache.ledger_in_flight_bytes,
            ledger_retained_bytes: snapshot.cache.ledger_retained_bytes,
            sticky_artifacts: snapshot.cache.sticky_artifacts,
            sticky_charged_bytes: snapshot.cache.sticky_charged_bytes,
            class_charges: snapshot
                .cache
                .class_charges
                .iter()
                .map(|charge| QueryBenchmarkMetadataCacheClassEndGauge {
                    class: metadata_cache_class_name(charge.class),
                    in_flight_bytes: charge.in_flight_bytes,
                    retained_bytes: charge.retained_bytes,
                })
                .collect(),
        },
        governor: QueryBenchmarkMetadataGovernorEndGauges {
            retained_max_bytes: snapshot.governor.retained_max_bytes,
            in_flight_max_bytes: snapshot.governor.in_flight_max_bytes,
            retained_bytes: snapshot.governor.retained_bytes,
            in_flight_bytes: snapshot.governor.in_flight_bytes,
            usage_charges: snapshot
                .governor
                .usage
                .iter()
                .map(|charge| QueryBenchmarkMetadataUsageEndGauge {
                    usage: metadata_usage_class_name(charge.usage),
                    in_flight_bytes: charge.in_flight_bytes,
                    retained_bytes: charge.retained_bytes,
                })
                .collect(),
        },
        file_manager: QueryBenchmarkMetadataFileManagerEndGauges {
            max_open_files: snapshot.files.max_open_files,
            max_cached_open_files: snapshot.files.max_cached_open_files,
            open_files: snapshot.files.open_files,
            occupied_open_slots: snapshot.files.occupied_open_slots,
            active_open_files: snapshot.files.active_open_files,
            cached_open_files: snapshot.files.cached_open_files,
            opening_files: snapshot.files.opening_files,
            pending_open_files: snapshot.files.pending_open_files,
            preflighting_files: snapshot.files.preflighting_files,
            closing_files: snapshot.files.closing_files,
            active_leases: snapshot.files.active_leases,
        },
    }
}

fn metadata_cache_class_name(class: MetadataCacheClass) -> &'static str {
    match class {
        MetadataCacheClass::SymbolRoot => "symbol_root",
        MetadataCacheClass::SymbolPage => "symbol_page",
        MetadataCacheClass::IndexRoot => "index_root",
        MetadataCacheClass::IndexDirectory => "index_directory",
        MetadataCacheClass::IndexPage => "index_page",
        MetadataCacheClass::MetricRange => "metric_range",
        MetadataCacheClass::SeriesRoot => "series_root",
        MetadataCacheClass::SeriesHotPage => "series_hot_page",
        MetadataCacheClass::SeriesColdPage => "series_cold_page",
        MetadataCacheClass::OverflowRoot => "overflow_root",
        MetadataCacheClass::OverflowBlob => "overflow_blob",
        MetadataCacheClass::Postings => "postings",
        MetadataCacheClass::FullValidation => "full_validation",
    }
}

fn metadata_usage_class_name(class: MetadataUsageClass) -> &'static str {
    match class {
        MetadataUsageClass::Unclassified => "unclassified",
        MetadataUsageClass::Scratch => "scratch",
        MetadataUsageClass::CorruptionLedger => "corruption_ledger",
        MetadataUsageClass::Cache(class) => metadata_cache_class_name(class),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryBenchmarkRangeScalarCacheReport {
    summary: RangeScalarCacheSummary,
    process_governor: RangeScalarCacheGovernorStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryBenchmarkRunKind {
    Cold,
    Warm,
}

const QUERY_BENCHMARK_RAW_SCHEMA_V10: &str = "chronoxide.query-benchmark.raw/v10";

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawDocumentV10 {
    schema: &'static str,
    corpus_fingerprint_sha256: String,
    corpus_fingerprint_duration_ns: u64,
    configuration: QueryBenchmarkRawConfigurationV10,
    limits: QueryBenchmarkRawLimitsV1,
    runs: Vec<QueryBenchmarkRawRunV10>,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawConfigurationV10 {
    #[serde(flatten)]
    v9: QueryBenchmarkRawConfigurationV9,
    query_instrumentation: &'static str,
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
    query_label_storage: QueryBenchmarkRawQueryLabelStorageV1,
}

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawRunV10 {
    #[serde(flatten)]
    v9: QueryBenchmarkRawRunV9,
    post_query_fingerprint_ns: u64,
    query_stages: QueryBenchmarkRawQueryStagesV1,
    metadata_runtime: QueryBenchmarkMetadataRuntimeReport,
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
struct QueryBenchmarkRawQueryLabelStorageV1 {
    label_sets: u64,
    atom_lookups: u64,
    atom_hits: u64,
    atom_misses: u64,
    unique_content_bytes: u64,
}

impl From<QueryLabelStorageStats> for QueryBenchmarkRawQueryLabelStorageV1 {
    fn from(stats: QueryLabelStorageStats) -> Self {
        Self {
            label_sets: stats.label_sets,
            atom_lookups: stats.atom_lookups,
            atom_hits: stats.atom_hits,
            atom_misses: stats.atom_misses,
            unique_content_bytes: stats.unique_content_bytes,
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
struct QueryBenchmarkRawSymbolReadsV5 {
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
struct QueryBenchmarkRawRangeScalarCacheV3 {
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
struct RawQueryStatsV1 {
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

static BENCHMARK_OUTPUT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const BENCHMARK_OUTPUT_TEMP_ATTEMPTS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedBenchmarkOutput {
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedBenchmarkOutput {
    parent: PathBuf,
    file_name: OsString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkOutputKind {
    Markdown,
    Raw,
}

#[derive(Debug)]
struct StagedBenchmarkOutput {
    destination: PreparedBenchmarkOutput,
    temp_path: PathBuf,
    published: bool,
}

impl StagedBenchmarkOutput {
    fn stage(destination: PreparedBenchmarkOutput, bytes: &[u8]) -> io::Result<Self> {
        let parent = destination.path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "benchmark output has no parent directory",
            )
        })?;
        for _ in 0..BENCHMARK_OUTPUT_TEMP_ATTEMPTS {
            let sequence = BENCHMARK_OUTPUT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let temp_path =
                parent.join(format!(".chronoxide-tmp-{}-{sequence}", std::process::id()));
            let mut file = match File::options()
                .write(true)
                .create_new(true)
                .open(&temp_path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            };
            let staged = Self {
                destination,
                temp_path,
                published: false,
            };
            let write_result = file.write_all(bytes).and_then(|_| file.sync_all());
            drop(file);
            write_result?;
            return Ok(staged);
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not reserve a unique benchmark output temporary file",
        ))
    }

    fn publish(&mut self) -> io::Result<()> {
        fs::rename(&self.temp_path, &self.destination.path)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for StagedBenchmarkOutput {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}

fn publish_benchmark_outputs(
    markdown_output: &Path,
    markdown_bytes: &[u8],
    raw: Option<(&Path, &[u8])>,
) -> io::Result<()> {
    publish_benchmark_outputs_with_stager(
        markdown_output,
        markdown_bytes,
        raw,
        |destination, bytes, _| StagedBenchmarkOutput::stage(destination.clone(), bytes),
    )
}

fn publish_benchmark_outputs_with_stager<F>(
    markdown_output: &Path,
    markdown_bytes: &[u8],
    raw: Option<(&Path, &[u8])>,
    mut stage: F,
) -> io::Result<()>
where
    F: FnMut(
        &PreparedBenchmarkOutput,
        &[u8],
        BenchmarkOutputKind,
    ) -> io::Result<StagedBenchmarkOutput>,
{
    let raw_output = raw.map(|(path, _)| path);
    let (markdown_destination, raw_destination) =
        preflight_benchmark_outputs(markdown_output, raw_output)?;
    let mut markdown_stage = stage(
        &markdown_destination,
        markdown_bytes,
        BenchmarkOutputKind::Markdown,
    )?;
    let mut raw_stage = match (raw, raw_destination.as_ref()) {
        (Some((_, bytes)), Some(destination)) => {
            Some(stage(destination, bytes, BenchmarkOutputKind::Raw)?)
        }
        (None, None) => None,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw benchmark output preflight was inconsistent",
            ));
        }
    };

    let latest_destinations = preflight_benchmark_outputs(markdown_output, raw_output)?;
    if latest_destinations != (markdown_destination, raw_destination) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark output destinations changed while staging",
        ));
    }

    if let Some(raw_stage) = &mut raw_stage {
        raw_stage.publish()?;
    }
    markdown_stage.publish()
}

fn preflight_benchmark_outputs(
    markdown_output: &Path,
    raw_output: Option<&Path>,
) -> io::Result<(PreparedBenchmarkOutput, Option<PreparedBenchmarkOutput>)> {
    let markdown = identify_benchmark_output(markdown_output)?;
    let raw = raw_output.map(identify_benchmark_output).transpose()?;

    fs::create_dir_all(&markdown.parent)?;
    if let Some(raw) = &raw {
        fs::create_dir_all(&raw.parent)?;
    }

    let markdown = validate_benchmark_output(markdown)?;
    let raw = raw.map(validate_benchmark_output).transpose()?;
    if let Some(raw) = &raw
        && (markdown.path == raw.path
            || existing_outputs_share_identity(&markdown.path, &raw.path)?)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Markdown and raw benchmark outputs resolve to the same file",
        ));
    }
    Ok((markdown, raw))
}

fn identify_benchmark_output(path: &Path) -> io::Result<UnresolvedBenchmarkOutput> {
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("benchmark output path has no filename: {}", path.display()),
        )
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(UnresolvedBenchmarkOutput {
        parent: parent.to_path_buf(),
        file_name: file_name.to_os_string(),
    })
}

fn validate_benchmark_output(
    unresolved: UnresolvedBenchmarkOutput,
) -> io::Result<PreparedBenchmarkOutput> {
    let canonical_parent = fs::canonicalize(&unresolved.parent)?;
    let normalized = canonical_parent.join(unresolved.file_name);
    match fs::symlink_metadata(&normalized) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "benchmark output destination must not be a symlink: {}",
                    normalized.display()
                ),
            ));
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "benchmark output destination must be a regular file: {}",
                    normalized.display()
                ),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(PreparedBenchmarkOutput { path: normalized })
}

#[cfg(unix)]
fn existing_outputs_share_identity(left: &Path, right: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let Some(left) = existing_output_metadata(left)? else {
        return Ok(false);
    };
    let Some(right) = existing_output_metadata(right)? else {
        return Ok(false);
    };
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn existing_outputs_share_identity(_left: &Path, _right: &Path) -> io::Result<bool> {
    Ok(false)
}

#[cfg(unix)]
fn existing_output_metadata(path: &Path) -> io::Result<Option<fs::Metadata>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
fn run_query_benchmark(config: &QueryBenchmarkConfig) -> io::Result<QueryBenchmarkReport> {
    run_query_benchmark_with_experimental_flow(
        config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::OwnedStrings,
        StorageLayoutArg::Schema8,
    )
}

#[cfg(test)]
fn run_query_benchmark_with_experimental_flow(
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

fn run_query_benchmark_with_experimental_flow_and_instrumentation(
    config: &QueryBenchmarkConfig,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: LabelMaterializationArg,
    label_storage: LabelStorageArg,
    storage_layout: StorageLayoutArg,
    query_instrumentation: QueryInstrumentationArg,
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
        query_session.set_chunk_reader(Arc::clone(&chunk_reader))?;
        query_session
            .set_experimental_cross_segment_chunk_reads(experimental_cross_segment_chunk_reads);
        query_session.set_label_materialization_policy(label_materialization.core_policy());
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
                label_storage_delta: label_storage_after.delta_since(label_storage_before),
                metadata_runtime: QueryBenchmarkMetadataRuntimeReport::between(
                    metadata_runtime_before,
                    metadata_runtime_after,
                ),
                range_scalar_cache,
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

fn validate_query_stage_accounting(
    mode: QueryInstrumentationArg,
    query: &str,
    query_duration: Duration,
    stages: QueryStageProfile,
) -> io::Result<()> {
    let exclusive_total = stages.total_exclusive();
    match mode {
        QueryInstrumentationArg::Off if exclusive_total.is_zero() => Ok(()),
        QueryInstrumentationArg::Off => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "query-stage attribution is nonzero while instrumentation is off for {query:?}: {} ns",
                exclusive_total.as_nanos(),
            ),
        )),
        QueryInstrumentationArg::Detailed if exclusive_total <= query_duration => Ok(()),
        QueryInstrumentationArg::Detailed => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "detailed query-stage attribution exceeds timed query wall for {query:?}: {} ns > {} ns",
                exclusive_total.as_nanos(),
                query_duration.as_nanos(),
            ),
        )),
    }
}

fn render_raw_benchmark_json(
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) -> io::Result<Vec<u8>> {
    let document = QueryBenchmarkRawDocumentV10 {
        schema: QUERY_BENCHMARK_RAW_SCHEMA_V10,
        corpus_fingerprint_sha256: report.corpus_fingerprint.to_hex(),
        corpus_fingerprint_duration_ns: duration_ns_u64(
            report.corpus_fingerprint_duration,
            "corpus fingerprint duration",
        )?,
        configuration: QueryBenchmarkRawConfigurationV10 {
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
        },
        limits: QueryBenchmarkRawLimitsV1::from(config.limits),
        runs: report
            .results
            .iter()
            .map(|result| {
                Ok(QueryBenchmarkRawRunV10 {
                    v9: QueryBenchmarkRawRunV9 {
                        v8: QueryBenchmarkRawRunV5 {
                            query: result.query.clone(),
                            run_kind: raw_run_kind_name(result.run_kind),
                            run_index: result.run_index,
                            duration_ns: duration_ns_u64(result.duration, "query duration")?,
                            effective_start_ms: result.effective_start_ms,
                            effective_end_ms: result.effective_end_ms,
                            step_ms: result.step_ms,
                            semantic_fingerprint_sha256: result.semantic_fingerprint.to_hex(),
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
                            label_materialization: QueryBenchmarkRawLabelMaterializationV1::from(
                                result.session_profile_delta,
                            ),
                            range_scalar_cache: result
                                .range_scalar_cache
                                .map(QueryBenchmarkRawRangeScalarCacheV3::from),
                        },
                        query_label_storage: QueryBenchmarkRawQueryLabelStorageV1::from(
                            result.label_storage_delta,
                        ),
                    },
                    post_query_fingerprint_ns: duration_ns_u64(
                        result.post_query_fingerprint,
                        "post-query fingerprint duration",
                    )?,
                    query_stages: QueryBenchmarkRawQueryStagesV1::from_result(result)?,
                    metadata_runtime: result.metadata_runtime.clone(),
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

fn effective_query_end_ms(
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
