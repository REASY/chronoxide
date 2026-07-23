use super::*;

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct QueryBenchmarkConfig {
    pub(in super::super) segments_dir: PathBuf,
    pub(in super::super) output: PathBuf,
    pub(in super::super) raw_output: Option<PathBuf>,
    pub(in super::super) start_ms: u64,
    pub(in super::super) end_ms: u64,
    pub(in super::super) mode: QueryBenchmarkMode,
    pub(in super::super) range_scalar_cache_max_bytes: Option<u64>,
    pub(in super::super) query_label_arena_max_bytes: u64,
    pub(in super::super) chunk_read_mode: ChunkReadModeArg,
    pub(in super::super) chunk_read_queue_depth: u32,
    pub(in super::super) chunk_payload_coalesce_max_gap_bytes: u64,
    pub(in super::super) queries: Vec<String>,
    pub(in super::super) benchmark_repeats: usize,
    pub(in super::super) prewarm_query_contexts: bool,
    pub(in super::super) prefetch_query_data: bool,
    pub(in super::super) exponential_histogram_bucket_boundaries: Vec<f64>,
    pub(in super::super) limits: QueryLimits,
    pub(in super::super) validate_segment_footers: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum QueryBenchmarkMode {
    Instant,
    Range { step_ms: u64 },
}

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct QueryBenchmarkReport {
    pub(in super::super) store_open: Duration,
    pub(in super::super) corpus_fingerprint: SegmentCorpusFingerprint,
    pub(in super::super) corpus_fingerprint_duration: Duration,
    pub(in super::super) query_session_open: Duration,
    pub(in super::super) query_context_prewarm: Duration,
    pub(in super::super) query_context_prewarm_stats_delta: SegmentStoreQuerySessionStats,
    pub(in super::super) query_context_prewarm_profile_delta: SegmentStoreQueryProfile,
    pub(in super::super) query_data_prefetch: Duration,
    pub(in super::super) query_data_prefetch_stats: QueryDataPrefetchStats,
    pub(in super::super) query_data_prefetch_session_stats_delta: SegmentStoreQuerySessionStats,
    pub(in super::super) query_data_prefetch_profile_delta: SegmentStoreQueryProfile,
    pub(in super::super) promql_queries: Duration,
    pub(in super::super) post_query_fingerprints: Duration,
    pub(in super::super) session_stats: SegmentStoreQuerySessionStats,
    pub(in super::super) session_profile: SegmentStoreQueryProfile,
    pub(in super::super) results: Vec<QueryBenchmarkResult>,
    pub(in super::super) experimental_cross_segment_chunk_reads: bool,
    pub(in super::super) label_materialization: LabelMaterializationArg,
    pub(in super::super) label_storage: LabelStorageArg,
    pub(in super::super) storage_layout: StorageLayoutArg,
    pub(in super::super) query_instrumentation: QueryInstrumentationArg,
    pub(in super::super) range_execution_mode: RangeExecutionModeArg,
}

impl QueryBenchmarkReport {
    pub(in super::super) fn result_count(&self) -> usize {
        self.results.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(in super::super) struct QueryBenchmarkResult {
    pub(in super::super) query: String,
    pub(in super::super) run_kind: QueryBenchmarkRunKind,
    pub(in super::super) run_index: usize,
    pub(in super::super) query_session_open: Duration,
    pub(in super::super) duration: Duration,
    pub(in super::super) post_query_fingerprint: Duration,
    pub(in super::super) effective_start_ms: u64,
    pub(in super::super) effective_end_ms: u64,
    pub(in super::super) step_ms: Option<u64>,
    pub(in super::super) semantic_fingerprint: QueryExecutionFingerprint,
    pub(in super::super) portable_semantic_fingerprint: QueryExecutionFingerprint,
    pub(in super::super) result_series: u64,
    pub(in super::super) result_samples: u64,
    pub(in super::super) stats: QueryStats,
    pub(in super::super) session_stats_delta: SegmentStoreQuerySessionStats,
    pub(in super::super) session_profile_delta: SegmentStoreQueryProfile,
    pub(in super::super) label_storage_delta: QueryLabelStorageStats,
    pub(in super::super) metadata_runtime: QueryBenchmarkMetadataRuntimeReport,
    pub(in super::super) range_scalar_cache: Option<QueryBenchmarkRangeScalarCacheReport>,
    pub(in super::super) range_execution: Option<RangeExecutionSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) struct QueryBenchmarkRangeScalarCacheReport {
    pub(in super::super) summary: RangeScalarCacheSummary,
    pub(in super::super) process_governor: RangeScalarCacheGovernorStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum QueryBenchmarkRunKind {
    Cold,
    Warm,
}
