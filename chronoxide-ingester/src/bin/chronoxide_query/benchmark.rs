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
    session_stats: SegmentStoreQuerySessionStats,
    session_profile: SegmentStoreQueryProfile,
    results: Vec<QueryBenchmarkResult>,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: LabelMaterializationArg,
    label_storage: LabelStorageArg,
    storage_layout: StorageLayoutArg,
}

#[derive(Debug, Clone, PartialEq)]
struct QueryBenchmarkResult {
    query: String,
    run_kind: QueryBenchmarkRunKind,
    run_index: usize,
    query_session_open: Duration,
    duration: Duration,
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
    range_scalar_cache: Option<QueryBenchmarkRangeScalarCacheReport>,
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

const QUERY_BENCHMARK_RAW_SCHEMA_V9: &str = "chronoxide.query-benchmark.raw/v9";

#[derive(Debug, Serialize)]
struct QueryBenchmarkRawDocumentV9 {
    schema: &'static str,
    corpus_fingerprint_sha256: String,
    corpus_fingerprint_duration_ns: u64,
    configuration: QueryBenchmarkRawConfigurationV9,
    limits: QueryBenchmarkRawLimitsV1,
    runs: Vec<QueryBenchmarkRawRunV9>,
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
    process_governor_peak_leased_bytes: u64,
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
            process_governor_peak_leased_bytes: governor.peak_leased_bytes,
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

fn run_query_benchmark_with_experimental_flow(
    config: &QueryBenchmarkConfig,
    experimental_cross_segment_chunk_reads: bool,
    label_materialization: LabelMaterializationArg,
    label_storage: LabelStorageArg,
    storage_layout: StorageLayoutArg,
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
        session_stats: SegmentStoreQuerySessionStats::default(),
        session_profile: SegmentStoreQueryProfile::default(),
        results: Vec::new(),
        experimental_cross_segment_chunk_reads,
        label_materialization,
        label_storage,
        storage_layout,
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
            let semantic_fingerprint = execution.semantic_fingerprint_sha256();
            let portable_semantic_fingerprint = execution.portable_semantic_fingerprint_sha256();
            let session_stats_after = query_session.stats();
            let session_profile_after = query_session.profile();
            let label_storage_after = query_session.query_label_storage_stats();
            let result_series = execution.results.len() as u64;
            let result_samples = execution
                .results
                .iter()
                .map(|result| result.samples.len() as u64)
                .sum();
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
                effective_start_ms,
                effective_end_ms,
                step_ms,
                semantic_fingerprint,
                portable_semantic_fingerprint,
                result_series,
                result_samples,
                stats: execution.stats,
                session_stats_delta: session_stats_after.delta_since(session_stats_before),
                session_profile_delta: session_profile_after.delta_since(session_profile_before),
                label_storage_delta: label_storage_after.delta_since(label_storage_before),
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

fn render_raw_benchmark_json(
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) -> io::Result<Vec<u8>> {
    let document = QueryBenchmarkRawDocumentV9 {
        schema: QUERY_BENCHMARK_RAW_SCHEMA_V9,
        corpus_fingerprint_sha256: report.corpus_fingerprint.to_hex(),
        corpus_fingerprint_duration_ns: duration_ns_u64(
            report.corpus_fingerprint_duration,
            "corpus fingerprint duration",
        )?,
        configuration: QueryBenchmarkRawConfigurationV9 {
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
        limits: QueryBenchmarkRawLimitsV1::from(config.limits),
        runs: report
            .results
            .iter()
            .map(|result| {
                Ok(QueryBenchmarkRawRunV9 {
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
