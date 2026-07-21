use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use chronoxide_core::promql::{METRIC_NAME_LABEL, PromqlQuery, parse_query};
use chronoxide_core::storage::chunk::{
    ChunkIndexReader, ChunkKind, ChunkRecord, ChunkSamples, read_chunk_record_at,
};
use chronoxide_core::storage::head::{
    CounterResetHint, OtlpAggregationTemporality, TypedSampleMetadata, prometheus_stale_nan,
};
use chronoxide_core::storage::index::{SegmentIndexReadCount, SegmentIndexReadStats};
use chronoxide_core::storage::io::{ChunkReadConfig, ChunkReadMode};
use chronoxide_core::storage::manifest::read_manifest_inventory;
use chronoxide_core::storage::metadata_governor::{MetadataCacheClass, MetadataUsageClass};
use chronoxide_core::storage::metadata_runtime::StoreMetadataRuntimeSnapshot;
use chronoxide_core::storage::segment::{
    DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES, DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES,
    PRODUCTION_QUERY_MAX_BYTES_READ, PRODUCTION_QUERY_MAX_CHUNKS_READ,
    PRODUCTION_QUERY_MAX_PROJECTED_SERIES, PRODUCTION_QUERY_MAX_SAMPLES,
    PRODUCTION_QUERY_MAX_SERIES_MATCHED, PRODUCTION_REGEX_MAX_EXPANDED_VALUES,
    QueryDataPrefetchStats, QueryExecutionFingerprint, QueryInstrumentationMode,
    QueryLabelMaterializationPolicy, QueryLabelStoragePolicy, QueryLabelStorageStats, QueryLimits,
    QueryProjectionConfig, QueryStageProfile, QueryStats, RangeScalarCacheGovernorStats,
    RangeScalarCacheSummary, SegmentCorpusFingerprint, SegmentFile, SegmentMeta,
    SegmentStoreOpenOptions, SegmentStoreQueryProfile, SegmentStoreQuerySession,
    SegmentStoreQuerySessionStats, SegmentStoreReader, SegmentStoreSchemaPolicy,
    SegmentStoreSmokeKindStats, SegmentStoreSmokeReport, SegmentStoreSymbolResources,
    range_scalar_cache_governor_stats, validate_range_scalar_cache_budget_bytes,
};
use chronoxide_core::storage::series::{SeriesEntry, SeriesReader};
use chronoxide_core::storage::symbols::{
    SegmentSymbolReadCount, SegmentSymbolReadStats, SegmentSymbolReader,
};
use clap::{Args as ClapArgs, Parser, ValueEnum};
use serde::Serialize;

const DEFAULT_BENCHMARK_REPEATS: usize = 3;
const MAX_BENCHMARK_RANGE_EVALUATIONS: u128 = 1_000_000;

#[derive(Debug, Parser)]
#[command(about = "Run read-path smoke queries against sealed Chronoxide segments")]
struct Args {
    #[arg(long, default_value = "data/smoke/segments-001")]
    segments_dir: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    raw_output: Option<PathBuf>,
    #[arg(long)]
    start_ms: Option<u64>,
    #[arg(long)]
    end_ms: Option<u64>,
    #[arg(long)]
    step_ms: Option<u64>,
    #[arg(long)]
    range_scalar_cache_max_bytes: Option<u64>,
    #[arg(long, value_enum, default_value_t = ChunkReadModeArg::Pread)]
    chunk_read_mode: ChunkReadModeArg,
    #[arg(long, default_value_t = 128)]
    chunk_read_queue_depth: u32,
    #[arg(long)]
    experimental_cross_segment_chunk_reads: bool,
    #[arg(
        long,
        value_enum,
        default_value_t = LabelMaterializationArg::DemandDriven,
        help = "Source-label ownership policy; full is an explicit same-binary A/B control"
    )]
    label_materialization: LabelMaterializationArg,
    #[arg(
        long,
        value_enum,
        default_value_t = LabelStorageArg::CompactIds,
        help = "Query-session label representation; compact-ids is the schema-7/8 default, while owned-strings and shared-atoms are same-binary comparators"
    )]
    query_label_storage: LabelStorageArg,
    #[arg(
        long,
        default_value_t = DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        help = "Aggregate modeled retained-allocation admission budget for the compact-ids query-label arena; allocator slack is measured with process RSS"
    )]
    query_label_arena_max_bytes: u64,
    #[arg(
        long,
        value_enum,
        default_value_t = QueryInstrumentationArg::Off,
        help = "Fine-grained query-stage timers; detailed is observer-heavy diagnostics and off is the latency-comparison default"
    )]
    query_instrumentation: QueryInstrumentationArg,
    #[arg(
        long,
        value_enum,
        default_value_t = StorageLayoutArg::Schema8,
        help = "Sealed-storage policy; schema8 is the production adaptive-postings layout, schema7 selects the prior raw-postings comparator, and schema6-ab is a read-only adapter that always validates complete segment footers"
    )]
    storage_layout: StorageLayoutArg,
    #[arg(long, default_value_t = 2)]
    sample_limit_per_kind: usize,
    #[arg(long)]
    verify_readbacks: bool,
    #[arg(long)]
    validate_segment_footers: bool,
    #[arg(long = "query")]
    queries: Vec<String>,
    #[arg(long, default_value_t = DEFAULT_BENCHMARK_REPEATS)]
    benchmark_repeats: usize,
    #[arg(long)]
    prewarm_query_contexts: bool,
    #[arg(long)]
    prefetch_query_data: bool,
    #[arg(long = "exponential-histogram-bucket-boundary", value_name = "LE")]
    exponential_histogram_bucket_boundaries: Vec<f64>,
    #[command(flatten)]
    query_limits: QueryLimitArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ChunkReadModeArg {
    Pread,
    IoUring,
    Auto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LabelMaterializationArg {
    DemandDriven,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LabelStorageArg {
    SharedAtoms,
    CompactIds,
    OwnedStrings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum QueryInstrumentationArg {
    Off,
    Detailed,
}

impl QueryInstrumentationArg {
    const fn core_mode(self) -> QueryInstrumentationMode {
        match self {
            Self::Off => QueryInstrumentationMode::Off,
            Self::Detailed => QueryInstrumentationMode::Detailed,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Detailed => "detailed",
        }
    }
}

impl LabelStorageArg {
    fn core_policy(self) -> QueryLabelStoragePolicy {
        match self {
            Self::SharedAtoms => QueryLabelStoragePolicy::SharedAtoms,
            Self::CompactIds => QueryLabelStoragePolicy::CompactIds,
            Self::OwnedStrings => QueryLabelStoragePolicy::OwnedStrings,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::SharedAtoms => "shared-atoms",
            Self::CompactIds => "compact-ids",
            Self::OwnedStrings => "owned-strings",
        }
    }
}

impl LabelMaterializationArg {
    fn core_policy(self) -> QueryLabelMaterializationPolicy {
        match self {
            Self::DemandDriven => QueryLabelMaterializationPolicy::DemandDriven,
            Self::Full => QueryLabelMaterializationPolicy::Full,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::DemandDriven => "demand-driven",
            Self::Full => "full",
        }
    }
}

impl ChunkReadModeArg {
    fn core_mode(self) -> ChunkReadMode {
        match self {
            Self::Pread => ChunkReadMode::Pread,
            Self::IoUring => ChunkReadMode::IoUring,
            Self::Auto => ChunkReadMode::Auto,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Pread => "pread",
            Self::IoUring => "io_uring",
            Self::Auto => "auto",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum StorageLayoutArg {
    Schema7,
    Schema8,
    Schema6Ab,
}

impl StorageLayoutArg {
    const fn core_policy(self) -> SegmentStoreSchemaPolicy {
        match self {
            Self::Schema7 => SegmentStoreSchemaPolicy::StrictSchema7,
            Self::Schema8 => SegmentStoreSchemaPolicy::StrictSchema8,
            Self::Schema6Ab => SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Schema7 => "schema7",
            Self::Schema8 => "schema8",
            Self::Schema6Ab => "schema6-ab",
        }
    }

    const fn forces_footer_validation(self) -> bool {
        matches!(self, Self::Schema6Ab)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ClapArgs)]
struct QueryLimitArgs {
    #[arg(long = "query-max-series-matched", default_value_t = PRODUCTION_QUERY_MAX_SERIES_MATCHED)]
    query_max_series_matched: u64,
    #[arg(long = "query-max-projected-series", default_value_t = PRODUCTION_QUERY_MAX_PROJECTED_SERIES)]
    query_max_projected_series: u64,
    #[arg(long = "query-max-chunks-read", default_value_t = PRODUCTION_QUERY_MAX_CHUNKS_READ)]
    query_max_chunks_read: u64,
    #[arg(long = "query-max-bytes-read", default_value_t = PRODUCTION_QUERY_MAX_BYTES_READ)]
    query_max_bytes_read: u64,
    #[arg(long = "query-max-samples", default_value_t = PRODUCTION_QUERY_MAX_SAMPLES)]
    query_max_samples: u64,
    #[arg(long = "regex-max-expanded-values", default_value_t = PRODUCTION_REGEX_MAX_EXPANDED_VALUES)]
    regex_max_expanded_values: u64,
}

impl QueryLimitArgs {
    fn to_query_limits(self) -> QueryLimits {
        QueryLimits {
            max_matched_series: Some(self.query_max_series_matched),
            max_projected_series: Some(self.query_max_projected_series),
            max_chunk_reads: Some(self.query_max_chunks_read),
            max_bytes_read: Some(self.query_max_bytes_read),
            max_samples_decoded: Some(self.query_max_samples),
            max_regex_values_examined: Some(self.regex_max_expanded_values),
        }
    }
}

fn main() {
    let args = Args::parse();
    let benchmark_request = if args.queries.is_empty() {
        if args.label_materialization != LabelMaterializationArg::DemandDriven {
            eprintln!(
                "query benchmark failed: --label-materialization full requires at least one --query"
            );
            std::process::exit(1);
        }
        if args.query_label_storage != LabelStorageArg::CompactIds {
            eprintln!(
                "query benchmark failed: non-default --query-label-storage requires at least one --query"
            );
            std::process::exit(1);
        }
        if args.query_instrumentation != QueryInstrumentationArg::Off {
            eprintln!(
                "query benchmark failed: --query-instrumentation detailed requires at least one --query"
            );
            std::process::exit(1);
        }
        if args.step_ms.is_some() {
            eprintln!("query benchmark failed: --step-ms requires at least one --query");
            std::process::exit(1);
        }
        if let Err(err) = range_scalar_cache_budget_from_args(&args, None) {
            eprintln!("query benchmark failed: {err}");
            std::process::exit(1);
        }
        None
    } else {
        Some(match benchmark_request_from_args(&args) {
            Ok(request) => request,
            Err(err) => {
                eprintln!("query benchmark failed: {err}");
                std::process::exit(1);
            }
        })
    };
    let range_scalar_cache_max_bytes = match benchmark_request.as_ref() {
        Some((_, _, mode)) => match range_scalar_cache_budget_from_args(&args, Some(*mode)) {
            Ok(budget) => budget,
            Err(err) => {
                eprintln!("query benchmark failed: {err}");
                std::process::exit(1);
            }
        },
        None => None,
    };
    let output = args.output.unwrap_or_else(|| {
        if args.queries.is_empty() {
            default_output_path(&args.segments_dir)
        } else {
            default_benchmark_output_path(&args.segments_dir)
        }
    });
    if !args.queries.is_empty() {
        let (start_ms, end_ms, mode) = benchmark_request.expect("query request was validated");
        let config = QueryBenchmarkConfig {
            segments_dir: args.segments_dir,
            output,
            raw_output: args.raw_output,
            start_ms,
            end_ms,
            mode,
            range_scalar_cache_max_bytes,
            query_label_arena_max_bytes: args.query_label_arena_max_bytes,
            chunk_read_mode: args.chunk_read_mode,
            chunk_read_queue_depth: args.chunk_read_queue_depth,
            queries: args.queries,
            benchmark_repeats: args.benchmark_repeats,
            prewarm_query_contexts: args.prewarm_query_contexts,
            prefetch_query_data: args.prefetch_query_data,
            exponential_histogram_bucket_boundaries: args.exponential_histogram_bucket_boundaries,
            limits: args.query_limits.to_query_limits(),
            validate_segment_footers: args.validate_segment_footers,
        };

        match run_query_benchmark_with_experimental_flow_and_instrumentation(
            &config,
            args.experimental_cross_segment_chunk_reads,
            args.label_materialization,
            args.query_label_storage,
            args.storage_layout,
            args.query_instrumentation,
        ) {
            Ok(report) => {
                println!(
                    "wrote {} with {} query runs over {} explicit queries",
                    config.output.display(),
                    report.results.len(),
                    config.queries.len()
                );
            }
            Err(err) => {
                eprintln!("query benchmark failed: {err}");
                std::process::exit(1);
            }
        }
        return;
    }

    let config = QuerySmokeConfig {
        segments_dir: args.segments_dir,
        output,
        start_ms: args.start_ms.unwrap_or(0),
        end_ms: args.end_ms.unwrap_or(u64::MAX),
        sample_limit_per_kind: args.sample_limit_per_kind,
        verify_readbacks: args.verify_readbacks,
        exponential_histogram_bucket_boundaries: args.exponential_histogram_bucket_boundaries,
        validate_segment_footers: args.validate_segment_footers,
    };

    match run_query_smoke_with_storage_layout(&config, args.storage_layout) {
        Ok(report) => {
            println!(
                "wrote {} with {} readback queries over {} segments",
                config.output.display(),
                report.queries.len(),
                report.totals.segments
            );
        }
        Err(err) => {
            eprintln!("query smoke failed: {err}");
            std::process::exit(1);
        }
    }
}

fn benchmark_request_from_args(args: &Args) -> io::Result<(u64, u64, QueryBenchmarkMode)> {
    let Some(step_ms) = args.step_ms else {
        return Ok((
            args.start_ms.unwrap_or(0),
            args.end_ms.unwrap_or(u64::MAX),
            QueryBenchmarkMode::Instant,
        ));
    };

    let start_ms = args.start_ms.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "range benchmark requires explicit --start-ms",
        )
    })?;
    let end_ms = args.end_ms.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "range benchmark requires explicit --end-ms",
        )
    })?;
    validate_range_benchmark(
        start_ms,
        end_ms,
        step_ms,
        args.prewarm_query_contexts,
        args.prefetch_query_data,
    )?;
    Ok((start_ms, end_ms, QueryBenchmarkMode::Range { step_ms }))
}

fn range_scalar_cache_budget_from_args(
    args: &Args,
    mode: Option<QueryBenchmarkMode>,
) -> io::Result<Option<u64>> {
    resolve_range_scalar_cache_budget(args.range_scalar_cache_max_bytes, mode)
}

fn resolve_range_scalar_cache_budget(
    configured_bytes: Option<u64>,
    mode: Option<QueryBenchmarkMode>,
) -> io::Result<Option<u64>> {
    match mode {
        Some(QueryBenchmarkMode::Range { .. }) => {
            let bytes = configured_bytes.unwrap_or(DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES);
            validate_range_scalar_cache_budget_bytes(bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            Ok(Some(bytes))
        }
        Some(QueryBenchmarkMode::Instant) | None => {
            if configured_bytes.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--range-scalar-cache-max-bytes requires a PromQL range workload (--query with --step-ms)",
                ));
            }
            Ok(None)
        }
    }
}

fn validate_range_benchmark(
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
    prewarm_query_contexts: bool,
    prefetch_query_data: bool,
) -> io::Result<()> {
    if step_ms == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "range benchmark requires --step-ms >= 1",
        ));
    }
    if end_ms < start_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "range benchmark requires --end-ms >= --start-ms",
        ));
    }
    let scheduled_evaluations = scheduled_range_evaluations(start_ms, end_ms, step_ms);
    if scheduled_evaluations > MAX_BENCHMARK_RANGE_EVALUATIONS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "range benchmark scheduled evaluations {scheduled_evaluations} exceed maximum {MAX_BENCHMARK_RANGE_EVALUATIONS}"
            ),
        ));
    }
    if prewarm_query_contexts {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--prewarm-query-contexts is not supported with --step-ms",
        ));
    }
    if prefetch_query_data {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--prefetch-query-data is not supported with --step-ms",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
struct QuerySmokeConfig {
    segments_dir: PathBuf,
    output: PathBuf,
    start_ms: u64,
    end_ms: u64,
    sample_limit_per_kind: usize,
    verify_readbacks: bool,
    exponential_histogram_bucket_boundaries: Vec<f64>,
    validate_segment_footers: bool,
}

fn default_output_path(segments_dir: &Path) -> PathBuf {
    let parent = segments_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = format!("query_smoke_{}.md", Utc::now().format("%Y%m%d_%H%M%S"));
    parent.join(filename)
}

fn default_benchmark_output_path(segments_dir: &Path) -> PathBuf {
    let parent = segments_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = format!("query_benchmark_{}.md", Utc::now().format("%Y%m%d_%H%M%S"));
    parent.join(filename)
}

include!("chronoxide_query/benchmark.rs");
include!("chronoxide_query/benchmark_report.rs");
#[path = "chronoxide_query/schema7_readback_oracle.rs"]
mod schema7_readback_oracle;
include!("chronoxide_query/smoke.rs");

#[cfg(test)]
#[path = "chronoxide_query/tests.rs"]
mod tests;
