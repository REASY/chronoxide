use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use chronoxide_core::promql::{METRIC_NAME_LABEL, PromqlQuery, parse_query};
use chronoxide_core::storage::chunk::{
    ChunkIndexReader, ChunkKind, ChunkRecord, ChunkSamples, read_chunk_record_at,
};
use chronoxide_core::storage::head::{
    CounterResetHint, OtlpAggregationTemporality, TypedSampleMetadata, prometheus_stale_nan,
};
use chronoxide_core::storage::manifest::read_manifest_inventory;
use chronoxide_core::storage::segment::{
    PRODUCTION_QUERY_MAX_BYTES_READ, PRODUCTION_QUERY_MAX_CHUNKS_READ,
    PRODUCTION_QUERY_MAX_PROJECTED_SERIES, PRODUCTION_QUERY_MAX_SAMPLES,
    PRODUCTION_QUERY_MAX_SERIES_MATCHED, PRODUCTION_REGEX_MAX_EXPANDED_VALUES,
    QueryDataPrefetchStats, QueryLimits, QueryStats, SegmentFile, SegmentId, SegmentReader,
    SegmentStoreOpenOptions, SegmentStoreQueryProfile, SegmentStoreQuerySession,
    SegmentStoreQuerySessionStats, SegmentStoreReader, SegmentStoreSmokeKindStats,
    SegmentStoreSmokeReport,
};
use chronoxide_core::storage::series::{
    SegmentSymbols, SeriesEntry, SeriesReader, read_symbols_bin,
};
use clap::{Args as ClapArgs, Parser};

const DEFAULT_BENCHMARK_REPEATS: usize = 3;

#[derive(Debug, Parser)]
#[command(about = "Run read-path smoke queries against sealed Chronoxide segments")]
struct Args {
    #[arg(long, default_value = "data/smoke/segments-001")]
    segments_dir: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    start_ms: u64,
    #[arg(long, default_value_t = u64::MAX)]
    end_ms: u64,
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
    #[command(flatten)]
    query_limits: QueryLimitArgs,
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
    let output = args.output.unwrap_or_else(|| {
        if args.queries.is_empty() {
            default_output_path(&args.segments_dir)
        } else {
            default_benchmark_output_path(&args.segments_dir)
        }
    });
    if !args.queries.is_empty() {
        let config = QueryBenchmarkConfig {
            segments_dir: args.segments_dir,
            output,
            start_ms: args.start_ms,
            end_ms: args.end_ms,
            queries: args.queries,
            benchmark_repeats: args.benchmark_repeats,
            prewarm_query_contexts: args.prewarm_query_contexts,
            prefetch_query_data: args.prefetch_query_data,
            limits: args.query_limits.to_query_limits(),
            validate_segment_footers: args.validate_segment_footers,
        };

        match run_query_benchmark(&config) {
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
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        sample_limit_per_kind: args.sample_limit_per_kind,
        verify_readbacks: args.verify_readbacks,
        validate_segment_footers: args.validate_segment_footers,
    };

    match run_query_smoke(&config) {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuerySmokeConfig {
    segments_dir: PathBuf,
    output: PathBuf,
    start_ms: u64,
    end_ms: u64,
    sample_limit_per_kind: usize,
    verify_readbacks: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryBenchmarkConfig {
    segments_dir: PathBuf,
    output: PathBuf,
    start_ms: u64,
    end_ms: u64,
    queries: Vec<String>,
    benchmark_repeats: usize,
    prewarm_query_contexts: bool,
    prefetch_query_data: bool,
    limits: QueryLimits,
    validate_segment_footers: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryBenchmarkReport {
    store_open: Duration,
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
}

#[derive(Debug, Clone, PartialEq)]
struct QueryBenchmarkResult {
    query: String,
    run_kind: QueryBenchmarkRunKind,
    run_index: usize,
    query_session_open: Duration,
    duration: Duration,
    result_series: u64,
    result_samples: u64,
    stats: QueryStats,
    session_stats_delta: SegmentStoreQuerySessionStats,
    session_profile_delta: SegmentStoreQueryProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryBenchmarkRunKind {
    Cold,
    Warm,
}

fn run_query_benchmark(config: &QueryBenchmarkConfig) -> io::Result<QueryBenchmarkReport> {
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

    let mut report = QueryBenchmarkReport::default();

    let phase_start = Instant::now();
    let store = open_segment_store(&config.segments_dir, config.validate_segment_footers)?;
    report.store_open = phase_start.elapsed();
    let sample_time_range = if config.end_ms == u64::MAX
        && config
            .queries
            .iter()
            .any(|query| query_needs_finite_end(query))
    {
        segment_sample_time_range(&config.segments_dir)?
    } else {
        None
    };

    for query in &config.queries {
        let query_end_ms = effective_query_end_ms(query, config.end_ms, sample_time_range);
        let phase_start = Instant::now();
        let mut query_session = store.query_session()?;
        let query_session_open = phase_start.elapsed();
        report.query_session_open = report.query_session_open.saturating_add(query_session_open);

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
            let query_start = Instant::now();
            let execution = query_session
                .query_promql_with_limits(query, config.start_ms, query_end_ms, config.limits)
                .map_err(|err| io::Error::other(format!("query failed: {query}: {err}")))?;
            let duration = query_start.elapsed();
            report.promql_queries = report.promql_queries.saturating_add(duration);
            let session_stats_after = query_session.stats();
            let session_profile_after = query_session.profile();
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
                result_series,
                result_samples,
                stats: execution.stats,
                session_stats_delta: session_stats_after.delta_since(session_stats_before),
                session_profile_delta: session_profile_after.delta_since(session_profile_before),
            });
        }

        add_session_stats(&mut report.session_stats, query_session.stats());
        add_session_profile(&mut report.session_profile, query_session.profile());
    }

    if let Some(parent) = config
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.output, render_benchmark_markdown(config, &report))?;

    Ok(report)
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
        PromqlQuery::Vector(_) | PromqlQuery::Scalar(_) => false,
        PromqlQuery::RangeFunction(_)
        | PromqlQuery::Aggregation(_)
        | PromqlQuery::Absent(_)
        | PromqlQuery::AbsentOverTime(_)
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
        PromqlQuery::Scalar(_) => true,
        PromqlQuery::BinaryExpression(expression) => {
            parsed_query_is_scalar(expression.left.as_ref())
                && parsed_query_is_scalar(expression.right.as_ref())
        }
        PromqlQuery::Vector(_)
        | PromqlQuery::RangeFunction(_)
        | PromqlQuery::Aggregation(_)
        | PromqlQuery::Absent(_)
        | PromqlQuery::AbsentOverTime(_)
        | PromqlQuery::HistogramQuantile(_)
        | PromqlQuery::HistogramFraction(_)
        | PromqlQuery::HistogramScalarFunction(_) => false,
    }
}

fn segment_sample_time_range(segments_dir: &Path) -> io::Result<Option<(u64, u64)>> {
    let mut range: Option<(u64, u64)> = None;
    let mut selected_window: Option<(u64, u64)> = None;
    let mut dirs = segment_dirs(segments_dir)?;
    dirs.reverse();

    for segment_dir in dirs {
        let Some(segment_name) = segment_dir.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Ok(segment_id) = SegmentId::parse_dir_name(segment_name) else {
            continue;
        };
        let segment_window = (segment_id.start_ms(), segment_id.end_ms());
        if selected_window.is_some_and(|window| window != segment_window) {
            break;
        }

        let mut chunk_index = ChunkIndexReader::open(File::open(
            segment_dir.join(SegmentFile::ChunkIndex.filename()),
        )?)?;
        let mut segment_range: Option<(u64, u64)> = None;
        chunk_index.for_each_series_entries(|_, entries| {
            for entry in entries {
                segment_range = Some(match segment_range {
                    Some((start_ms, end_ms)) => (
                        start_ms.min(entry.min_time_ms),
                        end_ms.max(entry.max_time_ms),
                    ),
                    None => (entry.min_time_ms, entry.max_time_ms),
                });
            }
            Ok(())
        })?;
        let Some(segment_range) = segment_range else {
            continue;
        };

        if selected_window.is_none() {
            selected_window = Some(segment_window);
        }
        range = Some(match range {
            Some((start_ms, end_ms)) => {
                (start_ms.min(segment_range.0), end_ms.max(segment_range.1))
            }
            None => segment_range,
        });
    }
    Ok(range)
}

fn render_benchmark_markdown(
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) -> String {
    let totals = benchmark_totals(report);
    let mut markdown = String::new();

    markdown.push_str("# Chronoxide Sealed Query Benchmark\n\n");
    markdown.push_str(&format!(
        "- Generated At: {}\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    markdown.push_str(&format!(
        "- Segments Directory: `{}`\n",
        config.segments_dir.display()
    ));
    markdown.push_str(&format!(
        "- Time Range: {}..{}\n\n",
        config.start_ms,
        format_end_ms(config.end_ms)
    ));
    markdown.push_str(&format!(
        "- Benchmark Repeats: {}\n\n",
        config.benchmark_repeats
    ));
    markdown.push_str(&format!(
        "- Prewarm Query Contexts: {}\n\n",
        config.prewarm_query_contexts
    ));
    markdown.push_str(&format!(
        "- Prefetch Query Data: {}\n\n",
        config.prefetch_query_data
    ));

    markdown.push_str("## Query Limits\n\n");
    markdown.push_str("| Limit | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| query_max_series_matched | {} |\n",
        format_query_limit(config.limits.max_matched_series)
    ));
    markdown.push_str(&format!(
        "| query_max_projected_series | {} |\n",
        format_query_limit(config.limits.max_projected_series)
    ));
    markdown.push_str(&format!(
        "| query_max_chunks_read | {} |\n",
        format_query_limit(config.limits.max_chunk_reads)
    ));
    markdown.push_str(&format!(
        "| query_max_bytes_read | {} |\n",
        format_query_limit(config.limits.max_bytes_read)
    ));
    markdown.push_str(&format!(
        "| query_max_samples | {} |\n",
        format_query_limit(config.limits.max_samples_decoded)
    ));
    markdown.push_str(&format!(
        "| regex_max_expanded_values | {} |\n\n",
        format_query_limit(config.limits.max_regex_values_examined)
    ));

    markdown.push_str("## Query Phases\n\n");
    markdown.push_str("| Phase | Duration |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Store Open | {} |\n",
        format_duration(report.store_open)
    ));
    markdown.push_str(&format!(
        "| Query Session Open | {} |\n",
        format_duration(report.query_session_open)
    ));
    markdown.push_str(&format!(
        "| Query Context Prewarm | {} |\n",
        format_duration(report.query_context_prewarm)
    ));
    markdown.push_str(&format!(
        "| Query Data Prefetch | {} |\n",
        format_duration(report.query_data_prefetch)
    ));
    markdown.push_str(&format!(
        "| PromQL Queries | {} |\n\n",
        format_duration(report.promql_queries)
    ));

    markdown.push_str("## Query Totals\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Queries | {} |\n", config.queries.len()));
    markdown.push_str(&format!("| Query Runs | {} |\n", report.results.len()));
    markdown.push_str(&format!(
        "| Cold Runs | {} |\n",
        report
            .results
            .iter()
            .filter(|result| result.run_kind == QueryBenchmarkRunKind::Cold)
            .count()
    ));
    markdown.push_str(&format!(
        "| Warm Runs | {} |\n",
        report
            .results
            .iter()
            .filter(|result| result.run_kind == QueryBenchmarkRunKind::Warm)
            .count()
    ));
    markdown.push_str(&format!(
        "| Segments Considered | {} |\n",
        totals.stats.segments_considered
    ));
    markdown.push_str(&format!(
        "| Segments Skipped By Time | {} |\n",
        totals.stats.segments_skipped_by_time
    ));
    markdown.push_str(&format!(
        "| Segments Skipped By Missing Equality | {} |\n",
        totals.stats.segments_skipped_by_missing_equality
    ));
    markdown.push_str(&format!(
        "| Segments Skipped By Matcher Time Range | {} |\n",
        totals.stats.segments_skipped_by_matcher_time_range
    ));
    markdown.push_str(&format!(
        "| Segments Queried | {} |\n",
        totals.stats.segments_queried
    ));
    markdown.push_str(&format!("| Result Series | {} |\n", totals.result_series));
    markdown.push_str(&format!("| Result Samples | {} |\n", totals.result_samples));
    markdown.push_str(&format!(
        "| Matched Series | {} |\n",
        totals.stats.matched_series
    ));
    markdown.push_str(&format!(
        "| Projected Series | {} |\n",
        totals.stats.projected_series
    ));
    markdown.push_str(&format!("| Chunk Reads | {} |\n", totals.stats.chunk_reads));
    markdown.push_str(&format!("| Bytes Read | {} |\n", totals.stats.bytes_read));
    markdown.push_str(&format!(
        "| Index Postings Reads | {} |\n",
        totals.stats.index_postings_reads
    ));
    markdown.push_str(&format!(
        "| Index Postings Bytes Read | {} |\n",
        totals.stats.index_postings_bytes_read
    ));
    markdown.push_str(&format!(
        "| Samples Decoded | {} |\n",
        totals.stats.samples_decoded
    ));
    markdown.push_str(&format!(
        "| Typed Scalar Chunks Decoded | {} |\n",
        totals.stats.typed_scalar_chunks_decoded
    ));
    markdown.push_str(&format!(
        "| Typed Full Chunks Decoded | {} |\n",
        totals.stats.typed_full_chunks_decoded
    ));
    markdown.push_str(&format!(
        "| Regex Values Examined | {} |\n\n",
        totals.stats.regex_values_examined
    ));

    markdown.push_str("## Session File Opens\n\n");
    markdown.push_str("| File | Opens |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Index Routing | {} |\n",
        report.session_stats.index_routing_opens
    ));
    markdown.push_str(&format!(
        "| Segment Contexts | {} |\n",
        report.session_stats.segment_context_opens
    ));
    markdown.push_str(&format!(
        "| Symbols | {} |\n",
        report.session_stats.symbols_bin_opens
    ));
    markdown.push_str(&format!(
        "| Indexes | {} |\n",
        report.session_stats.indexes_puffin_opens
    ));
    markdown.push_str(&format!(
        "| Series | {} |\n",
        report.session_stats.series_bin_opens
    ));
    markdown.push_str(&format!(
        "| Chunk Index | {} |\n",
        report.session_stats.chunk_index_bin_opens
    ));
    markdown.push_str(&format!(
        "| Chunks | {} |\n\n",
        report.session_stats.chunks_bin_opens
    ));
    render_profile_table(
        &mut markdown,
        "Session Read Profile",
        report.session_profile,
    );

    if config.prewarm_query_contexts {
        markdown.push_str("## Query Context Prewarm File Opens\n\n");
        markdown.push_str("| File | Opens |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Index Routing | {} |\n",
            report.query_context_prewarm_stats_delta.index_routing_opens
        ));
        markdown.push_str(&format!(
            "| Segment Contexts | {} |\n",
            report
                .query_context_prewarm_stats_delta
                .segment_context_opens
        ));
        markdown.push_str(&format!(
            "| Symbols | {} |\n",
            report.query_context_prewarm_stats_delta.symbols_bin_opens
        ));
        markdown.push_str(&format!(
            "| Indexes | {} |\n",
            report
                .query_context_prewarm_stats_delta
                .indexes_puffin_opens
        ));
        markdown.push_str(&format!(
            "| Series | {} |\n",
            report.query_context_prewarm_stats_delta.series_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunk Index | {} |\n",
            report
                .query_context_prewarm_stats_delta
                .chunk_index_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunks | {} |\n\n",
            report.query_context_prewarm_stats_delta.chunks_bin_opens
        ));
        render_profile_table(
            &mut markdown,
            "Query Context Prewarm Read Profile",
            report.query_context_prewarm_profile_delta,
        );
    }

    if config.prefetch_query_data {
        markdown.push_str("## Query Data Prefetch\n\n");
        markdown.push_str("| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Segments Considered | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_considered
        ));
        markdown.push_str(&format!(
            "| Segments Skipped By Time | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_skipped_by_time
        ));
        markdown.push_str(&format!(
            "| Segments Skipped By Missing Equality | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_skipped_by_missing_equality
        ));
        markdown.push_str(&format!(
            "| Segments Skipped By Matcher Time Range | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_skipped_by_matcher_time_range
        ));
        markdown.push_str(&format!(
            "| Segments Prefetched | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_queried
        ));
        markdown.push_str(&format!(
            "| Matched Series | {} |\n",
            report.query_data_prefetch_stats.query_stats.matched_series
        ));
        markdown.push_str(&format!(
            "| Series Entries Read | {} |\n",
            report.query_data_prefetch_stats.series_entries_read
        ));
        markdown.push_str(&format!(
            "| Chunk Index Reads | {} |\n",
            report.query_data_prefetch_stats.chunk_index_reads
        ));
        markdown.push_str(&format!(
            "| Chunk Index Bytes Read | {} |\n",
            report.query_data_prefetch_stats.chunk_index_bytes_read
        ));
        markdown.push_str(&format!(
            "| Chunk Prefetch Reads | {} |\n",
            report.query_data_prefetch_stats.query_stats.chunk_reads
        ));
        markdown.push_str(&format!(
            "| Chunk Prefetch Bytes | {} |\n",
            report.query_data_prefetch_stats.query_stats.bytes_read
        ));
        markdown.push_str(&format!(
            "| Index Postings Reads | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .index_postings_reads
        ));
        markdown.push_str(&format!(
            "| Index Postings Bytes Read | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .index_postings_bytes_read
        ));
        markdown.push_str(&format!(
            "| Regex Values Examined | {} |\n\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .regex_values_examined
        ));

        markdown.push_str("## Query Data Prefetch File Opens\n\n");
        markdown.push_str("| File | Opens |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Index Routing | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .index_routing_opens
        ));
        markdown.push_str(&format!(
            "| Segment Contexts | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .segment_context_opens
        ));
        markdown.push_str(&format!(
            "| Symbols | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .symbols_bin_opens
        ));
        markdown.push_str(&format!(
            "| Indexes | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .indexes_puffin_opens
        ));
        markdown.push_str(&format!(
            "| Series | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .series_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunk Index | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .chunk_index_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunks | {} |\n\n",
            report
                .query_data_prefetch_session_stats_delta
                .chunks_bin_opens
        ));
        render_profile_table(
            &mut markdown,
            "Query Data Prefetch Read Profile",
            report.query_data_prefetch_profile_delta,
        );
    }

    markdown.push_str("## Cold/Warm Query Summary\n\n");
    markdown.push_str("| Query | Cold Runs | Warm Runs | Cold Duration | Warm Mean | Warm Min | Warm Max | Result Series | Result Samples |\n");
    markdown.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for (query, summary) in benchmark_run_summaries(report) {
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&query),
            summary.cold_runs,
            summary.warm_runs,
            format_optional_duration(summary.cold_duration),
            format_optional_duration(summary.warm_mean_duration()),
            format_optional_duration(summary.warm_min_duration),
            format_optional_duration(summary.warm_max_duration),
            summary.result_series,
            summary.result_samples
        ));
    }
    markdown.push('\n');

    markdown.push_str("## Query Results\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Query Session Open | Duration | context_opens_delta | symbols_opens_delta | series_opens_delta | chunk_index_opens_delta | chunks_opens_delta | routing_opens_delta | indexes_opens_delta | segments_considered | segments_skipped_by_time | segments_skipped_by_missing_equality | segments_skipped_by_matcher_time_range | segments_queried | result_series | result_samples | matched_series | projected_series | chunk_reads | bytes_read | index_postings_reads | index_postings_bytes_read | samples_decoded | typed_scalar_chunks_decoded | typed_full_chunks_decoded | regex_values_examined |\n");
    markdown.push_str(
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for result in &report.results {
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            format_duration(result.query_session_open),
            format_duration(result.duration),
            result.session_stats_delta.segment_context_opens,
            result.session_stats_delta.symbols_bin_opens,
            result.session_stats_delta.series_bin_opens,
            result.session_stats_delta.chunk_index_bin_opens,
            result.session_stats_delta.chunks_bin_opens,
            result.session_stats_delta.index_routing_opens,
            result.session_stats_delta.indexes_puffin_opens,
            result.stats.segments_considered,
            result.stats.segments_skipped_by_time,
            result.stats.segments_skipped_by_missing_equality,
            result.stats.segments_skipped_by_matcher_time_range,
            result.stats.segments_queried,
            result.result_series,
            result.result_samples,
            result.stats.matched_series,
            result.stats.projected_series,
            result.stats.chunk_reads,
            result.stats.bytes_read,
            result.stats.index_postings_reads,
            result.stats.index_postings_bytes_read,
            result.stats.samples_decoded,
            result.stats.typed_scalar_chunks_decoded,
            result.stats.typed_full_chunks_decoded,
            result.stats.regex_values_examined
        ));
    }

    markdown.push_str("\n## Query Result Read Profiles\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | routing_open_delta | context_open_delta | indexes_open_delta | symbols_read_delta | series_open_delta | chunk_index_open_delta | chunks_open_delta | routing_read_delta | postings_read_delta | metric_series_ranges_read_delta | series_entry_read_delta | chunk_index_range_read_delta | chunk_read_delta | routing_opened_file_size_bytes_delta | indexes_opened_file_size_bytes_delta | symbols_opened_file_size_bytes_delta | series_opened_file_size_bytes_delta | chunk_index_opened_file_size_bytes_delta | chunks_opened_file_size_bytes_delta | routing_index_bytes_delta | postings_bytes_delta | metric_series_ranges_bytes_delta | series_entries_read_delta | series_entry_read_batches_delta | series_entry_bytes_delta | chunk_index_range_bytes_delta | chunk_payload_bytes_delta | chunk_payload_physical_reads_delta | chunk_payload_physical_bytes_delta |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in &report.results {
        let profile = result.session_profile_delta;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            format_duration(profile.index_routing_open),
            format_duration(profile.segment_context_open),
            format_duration(profile.indexes_open),
            format_duration(profile.symbols_read),
            format_duration(profile.series_open),
            format_duration(profile.chunk_index_open),
            format_duration(profile.chunks_open),
            format_duration(profile.routing_index_read),
            format_duration(profile.exact_postings_read),
            format_duration(profile.metric_series_ranges_read),
            format_duration(profile.series_entry_read),
            format_duration(profile.chunk_index_range_read),
            format_duration(profile.chunk_read),
            profile.index_routing_file_bytes,
            profile.indexes_file_bytes,
            profile.symbols_file_bytes,
            profile.series_file_bytes,
            profile.chunk_index_file_bytes,
            profile.chunks_file_bytes,
            profile.routing_index_bytes,
            profile.exact_postings_bytes,
            profile.metric_series_ranges_bytes,
            profile.series_entries_read,
            profile.series_entry_read_batches,
            profile.series_entry_bytes,
            profile.chunk_index_range_bytes,
            profile.chunk_payload_bytes,
            profile.chunk_payload_physical_reads,
            profile.chunk_payload_physical_bytes
        ));
    }

    markdown.push_str("\n## Query Result Chunk Payload Locality\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | payload_read_ranges | forward_gaps | forward_gap_bytes | backward_jumps | contiguous_runs | contiguous_span_bytes | coalesced_4k_runs | coalesced_4k_span_bytes | coalesced_64k_runs | coalesced_64k_span_bytes | sorted_contiguous_runs | sorted_contiguous_span_bytes | sorted_coalesced_4k_runs | sorted_coalesced_4k_span_bytes | sorted_coalesced_64k_runs | sorted_coalesced_64k_span_bytes |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in &report.results {
        let locality = result.session_profile_delta.chunk_payload_locality;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            locality.reads,
            locality.forward_gaps,
            locality.forward_gap_bytes,
            locality.backward_jumps,
            locality.contiguous_runs,
            locality.contiguous_span_bytes,
            locality.coalesced_4k_runs,
            locality.coalesced_4k_span_bytes,
            locality.coalesced_64k_runs,
            locality.coalesced_64k_span_bytes,
            locality.sorted_contiguous_runs,
            locality.sorted_contiguous_span_bytes,
            locality.sorted_coalesced_4k_runs,
            locality.sorted_coalesced_4k_span_bytes,
            locality.sorted_coalesced_64k_runs,
            locality.sorted_coalesced_64k_span_bytes
        ));
    }

    markdown
}

fn render_profile_table(markdown: &mut String, title: &str, profile: SegmentStoreQueryProfile) {
    if !markdown.ends_with("\n\n") {
        markdown.push('\n');
    }
    markdown.push_str(&format!("## {title}\n\n"));
    markdown.push_str("Opened file size bytes are summed file lengths observed when a file is opened. Logical read bytes are explicit byte ranges requested by the query path. Physical chunk payload spans are the coalesced ranges issued by the query reader before OS caching effects.\n\n");
    let split_title = title.strip_suffix(" Read Profile").unwrap_or(title);
    markdown.push_str(&format!("## {split_title} Opened File Sizes\n\n"));
    markdown.push_str("| Stage | Duration | Opened File Size Bytes |\n");
    markdown.push_str("| --- | ---: | ---: |\n");
    markdown.push_str(&format!(
        "| Index Routing Open | {} | {} |\n",
        format_duration(profile.index_routing_open),
        profile.index_routing_file_bytes
    ));
    markdown.push_str(&format!(
        "| Segment Context Open | {} | 0 |\n",
        format_duration(profile.segment_context_open)
    ));
    markdown.push_str(&format!(
        "| indexes.puffin | {} | {} |\n",
        format_duration(profile.indexes_open),
        profile.indexes_file_bytes
    ));
    markdown.push_str(&format!(
        "| symbols.bin | {} | {} |\n",
        format_duration(profile.symbols_read),
        profile.symbols_file_bytes
    ));
    markdown.push_str(&format!(
        "| series.bin | {} | {} |\n",
        format_duration(profile.series_open),
        profile.series_file_bytes
    ));
    markdown.push_str(&format!(
        "| chunk_index.bin | {} | {} |\n",
        format_duration(profile.chunk_index_open),
        profile.chunk_index_file_bytes
    ));
    markdown.push_str(&format!(
        "| chunks.bin | {} | {} |\n",
        format_duration(profile.chunks_open),
        profile.chunks_file_bytes
    ));

    markdown.push_str(&format!("\n## {split_title} Logical Read Bytes\n\n"));
    markdown.push_str("| Stage | Duration | Read Bytes | Count |\n");
    markdown.push_str("| --- | ---: | ---: | ---: |\n");
    markdown.push_str(&format!(
        "| Routing Index Blob | {} | {} | - |\n",
        format_duration(profile.routing_index_read),
        profile.routing_index_bytes
    ));
    markdown.push_str(&format!(
        "| Exact Postings | {} | {} | - |\n",
        format_duration(profile.exact_postings_read),
        profile.exact_postings_bytes
    ));
    markdown.push_str(&format!(
        "| Metric Series Ranges | {} | {} | - |\n",
        format_duration(profile.metric_series_ranges_read),
        profile.metric_series_ranges_bytes
    ));
    markdown.push_str(&format!(
        "| Series Entries | {} | {} | {} |\n",
        format_duration(profile.series_entry_read),
        profile.series_entry_bytes,
        profile.series_entries_read
    ));
    markdown.push_str(&format!(
        "| Series Entry Batches | - | - | {} |\n",
        profile.series_entry_read_batches
    ));
    markdown.push_str(&format!(
        "| Chunk Index Ranges | {} | {} | - |\n",
        format_duration(profile.chunk_index_range_read),
        profile.chunk_index_range_bytes
    ));
    markdown.push_str(&format!(
        "| Chunk Payloads | {} | {} | - |\n\n",
        format_duration(profile.chunk_read),
        profile.chunk_payload_bytes
    ));

    markdown.push_str(&format!(
        "## {split_title} Physical Chunk Payload Spans\n\n"
    ));
    markdown.push_str("| Stage | Duration | Span Bytes | Span Reads |\n");
    markdown.push_str("| --- | ---: | ---: | ---: |\n");
    markdown.push_str(&format!(
        "| Chunk Payload Spans | {} | {} | {} |\n\n",
        format_duration(profile.chunk_read),
        profile.chunk_payload_physical_bytes,
        profile.chunk_payload_physical_reads
    ));

    let locality = profile.chunk_payload_locality;
    markdown.push_str(&format!("## {split_title} Chunk Payload Locality\n\n"));
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Payload Read Ranges | {} |\n", locality.reads));
    markdown.push_str(&format!("| Forward Gaps | {} |\n", locality.forward_gaps));
    markdown.push_str(&format!(
        "| Forward Gap Bytes | {} |\n",
        locality.forward_gap_bytes
    ));
    markdown.push_str(&format!(
        "| Backward Jumps | {} |\n",
        locality.backward_jumps
    ));
    markdown.push_str(&format!(
        "| Strict Contiguous Runs | {} |\n",
        locality.contiguous_runs
    ));
    markdown.push_str(&format!(
        "| Strict Contiguous Span Bytes | {} |\n",
        locality.contiguous_span_bytes
    ));
    markdown.push_str(&format!(
        "| Coalesced 4KiB Runs | {} |\n",
        locality.coalesced_4k_runs
    ));
    markdown.push_str(&format!(
        "| Coalesced 4KiB Span Bytes | {} |\n",
        locality.coalesced_4k_span_bytes
    ));
    markdown.push_str(&format!(
        "| Coalesced 64KiB Runs | {} |\n",
        locality.coalesced_64k_runs
    ));
    markdown.push_str(&format!(
        "| Coalesced 64KiB Span Bytes | {} |\n",
        locality.coalesced_64k_span_bytes
    ));
    markdown.push_str(&format!(
        "| Sorted Strict Contiguous Runs | {} |\n",
        locality.sorted_contiguous_runs
    ));
    markdown.push_str(&format!(
        "| Sorted Strict Contiguous Span Bytes | {} |\n",
        locality.sorted_contiguous_span_bytes
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 4KiB Runs | {} |\n",
        locality.sorted_coalesced_4k_runs
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 4KiB Span Bytes | {} |\n",
        locality.sorted_coalesced_4k_span_bytes
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 64KiB Runs | {} |\n",
        locality.sorted_coalesced_64k_runs
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 64KiB Span Bytes | {} |\n\n",
        locality.sorted_coalesced_64k_span_bytes
    ));
}

fn add_query_data_prefetch_stats(total: &mut QueryDataPrefetchStats, next: QueryDataPrefetchStats) {
    add_query_stats(&mut total.query_stats, next.query_stats);
    total.series_entries_read = total
        .series_entries_read
        .saturating_add(next.series_entries_read);
    total.chunk_index_reads = total
        .chunk_index_reads
        .saturating_add(next.chunk_index_reads);
    total.chunk_index_bytes_read = total
        .chunk_index_bytes_read
        .saturating_add(next.chunk_index_bytes_read);
}

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryBenchmarkTotals {
    result_series: u64,
    result_samples: u64,
    stats: QueryStats,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QueryBenchmarkRunSummary {
    cold_runs: u64,
    warm_runs: u64,
    cold_duration: Option<Duration>,
    warm_total_duration: Duration,
    warm_min_duration: Option<Duration>,
    warm_max_duration: Option<Duration>,
    result_series: u64,
    result_samples: u64,
}

impl QueryBenchmarkRunSummary {
    fn warm_mean_duration(&self) -> Option<Duration> {
        if self.warm_runs == 0 {
            return None;
        }
        Some(duration_div(self.warm_total_duration, self.warm_runs))
    }
}

fn benchmark_totals(report: &QueryBenchmarkReport) -> QueryBenchmarkTotals {
    let mut totals = QueryBenchmarkTotals::default();
    for result in &report.results {
        totals.result_series = totals.result_series.saturating_add(result.result_series);
        totals.result_samples = totals.result_samples.saturating_add(result.result_samples);
        add_query_stats(&mut totals.stats, result.stats);
    }
    totals
}

fn benchmark_run_summaries(
    report: &QueryBenchmarkReport,
) -> BTreeMap<String, QueryBenchmarkRunSummary> {
    let mut summaries = BTreeMap::new();
    for result in &report.results {
        let summary = summaries
            .entry(result.query.clone())
            .or_insert_with(QueryBenchmarkRunSummary::default);
        match result.run_kind {
            QueryBenchmarkRunKind::Cold => {
                summary.cold_runs = summary.cold_runs.saturating_add(1);
                summary.cold_duration = Some(result.duration);
                summary.result_series = result.result_series;
                summary.result_samples = result.result_samples;
            }
            QueryBenchmarkRunKind::Warm => {
                summary.warm_runs = summary.warm_runs.saturating_add(1);
                summary.warm_total_duration =
                    summary.warm_total_duration.saturating_add(result.duration);
                summary.warm_min_duration = Some(
                    summary
                        .warm_min_duration
                        .map(|duration| duration.min(result.duration))
                        .unwrap_or(result.duration),
                );
                summary.warm_max_duration = Some(
                    summary
                        .warm_max_duration
                        .map(|duration| duration.max(result.duration))
                        .unwrap_or(result.duration),
                );
                if summary.result_series == 0 {
                    summary.result_series = result.result_series;
                }
                if summary.result_samples == 0 {
                    summary.result_samples = result.result_samples;
                }
            }
        }
    }
    summaries
}

fn run_kind_name(kind: QueryBenchmarkRunKind) -> &'static str {
    match kind {
        QueryBenchmarkRunKind::Cold => "Cold",
        QueryBenchmarkRunKind::Warm => "Warm",
    }
}

fn duration_div(duration: Duration, divisor: u64) -> Duration {
    if divisor == 0 {
        return Duration::ZERO;
    }
    let nanos = duration.as_nanos() / u128::from(divisor);
    Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

fn format_optional_duration(duration: Option<Duration>) -> String {
    duration
        .map(format_duration)
        .unwrap_or_else(|| "n/a".to_string())
}

fn add_session_stats(
    total: &mut SegmentStoreQuerySessionStats,
    next: SegmentStoreQuerySessionStats,
) {
    total.index_routing_opens = total
        .index_routing_opens
        .saturating_add(next.index_routing_opens);
    total.segment_context_opens = total
        .segment_context_opens
        .saturating_add(next.segment_context_opens);
    total.symbols_bin_opens = total
        .symbols_bin_opens
        .saturating_add(next.symbols_bin_opens);
    total.indexes_puffin_opens = total
        .indexes_puffin_opens
        .saturating_add(next.indexes_puffin_opens);
    total.series_bin_opens = total.series_bin_opens.saturating_add(next.series_bin_opens);
    total.chunk_index_bin_opens = total
        .chunk_index_bin_opens
        .saturating_add(next.chunk_index_bin_opens);
    total.chunks_bin_opens = total.chunks_bin_opens.saturating_add(next.chunks_bin_opens);
}

fn add_session_profile(total: &mut SegmentStoreQueryProfile, next: SegmentStoreQueryProfile) {
    total.index_routing_open = total
        .index_routing_open
        .saturating_add(next.index_routing_open);
    total.segment_context_open = total
        .segment_context_open
        .saturating_add(next.segment_context_open);
    total.indexes_open = total.indexes_open.saturating_add(next.indexes_open);
    total.symbols_read = total.symbols_read.saturating_add(next.symbols_read);
    total.series_open = total.series_open.saturating_add(next.series_open);
    total.chunk_index_open = total.chunk_index_open.saturating_add(next.chunk_index_open);
    total.chunks_open = total.chunks_open.saturating_add(next.chunks_open);
    total.routing_index_read = total
        .routing_index_read
        .saturating_add(next.routing_index_read);
    total.exact_postings_read = total
        .exact_postings_read
        .saturating_add(next.exact_postings_read);
    total.metric_series_ranges_read = total
        .metric_series_ranges_read
        .saturating_add(next.metric_series_ranges_read);
    total.series_entry_read = total
        .series_entry_read
        .saturating_add(next.series_entry_read);
    total.chunk_index_range_read = total
        .chunk_index_range_read
        .saturating_add(next.chunk_index_range_read);
    total.chunk_read = total.chunk_read.saturating_add(next.chunk_read);
    total.index_routing_file_bytes = total
        .index_routing_file_bytes
        .saturating_add(next.index_routing_file_bytes);
    total.indexes_file_bytes = total
        .indexes_file_bytes
        .saturating_add(next.indexes_file_bytes);
    total.symbols_file_bytes = total
        .symbols_file_bytes
        .saturating_add(next.symbols_file_bytes);
    total.series_file_bytes = total
        .series_file_bytes
        .saturating_add(next.series_file_bytes);
    total.chunk_index_file_bytes = total
        .chunk_index_file_bytes
        .saturating_add(next.chunk_index_file_bytes);
    total.chunks_file_bytes = total
        .chunks_file_bytes
        .saturating_add(next.chunks_file_bytes);
    total.routing_index_bytes = total
        .routing_index_bytes
        .saturating_add(next.routing_index_bytes);
    total.exact_postings_bytes = total
        .exact_postings_bytes
        .saturating_add(next.exact_postings_bytes);
    total.metric_series_ranges_bytes = total
        .metric_series_ranges_bytes
        .saturating_add(next.metric_series_ranges_bytes);
    total.series_entries_read = total
        .series_entries_read
        .saturating_add(next.series_entries_read);
    total.series_entry_read_batches = total
        .series_entry_read_batches
        .saturating_add(next.series_entry_read_batches);
    total.series_entry_bytes = total
        .series_entry_bytes
        .saturating_add(next.series_entry_bytes);
    total.chunk_index_range_bytes = total
        .chunk_index_range_bytes
        .saturating_add(next.chunk_index_range_bytes);
    total.chunk_payload_bytes = total
        .chunk_payload_bytes
        .saturating_add(next.chunk_payload_bytes);
    total.chunk_payload_physical_reads = total
        .chunk_payload_physical_reads
        .saturating_add(next.chunk_payload_physical_reads);
    total.chunk_payload_physical_bytes = total
        .chunk_payload_physical_bytes
        .saturating_add(next.chunk_payload_physical_bytes);
    total
        .chunk_payload_locality
        .add(next.chunk_payload_locality);
}

fn add_query_stats(total: &mut QueryStats, next: QueryStats) {
    total.segments_considered = total
        .segments_considered
        .saturating_add(next.segments_considered);
    total.segments_skipped_by_time = total
        .segments_skipped_by_time
        .saturating_add(next.segments_skipped_by_time);
    total.segments_skipped_by_missing_equality = total
        .segments_skipped_by_missing_equality
        .saturating_add(next.segments_skipped_by_missing_equality);
    total.segments_skipped_by_matcher_time_range = total
        .segments_skipped_by_matcher_time_range
        .saturating_add(next.segments_skipped_by_matcher_time_range);
    total.segments_queried = total.segments_queried.saturating_add(next.segments_queried);
    total.matched_series = total.matched_series.saturating_add(next.matched_series);
    total.projected_series = total.projected_series.saturating_add(next.projected_series);
    total.chunk_reads = total.chunk_reads.saturating_add(next.chunk_reads);
    total.bytes_read = total.bytes_read.saturating_add(next.bytes_read);
    total.index_postings_reads = total
        .index_postings_reads
        .saturating_add(next.index_postings_reads);
    total.index_postings_bytes_read = total
        .index_postings_bytes_read
        .saturating_add(next.index_postings_bytes_read);
    total.samples_decoded = total.samples_decoded.saturating_add(next.samples_decoded);
    total.typed_scalar_chunks_decoded = total
        .typed_scalar_chunks_decoded
        .saturating_add(next.typed_scalar_chunks_decoded);
    total.typed_full_chunks_decoded = total
        .typed_full_chunks_decoded
        .saturating_add(next.typed_full_chunks_decoded);
    total.regex_values_examined = total
        .regex_values_examined
        .saturating_add(next.regex_values_examined);
}

fn render_markdown(
    config: &QuerySmokeConfig,
    report: &SegmentStoreSmokeReport,
    verification: Option<&QueryReadbackVerification>,
    diagnostics: Option<&QuerySmokeDiagnostics>,
) -> String {
    let mut markdown = String::new();

    markdown.push_str("# Chronoxide Query Smoke Report\n\n");
    markdown.push_str(&format!(
        "- Generated At: {}\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    markdown.push_str(&format!(
        "- Segments Directory: `{}`\n",
        config.segments_dir.display()
    ));
    markdown.push_str(&format!(
        "- Time Range: {}..{}\n",
        config.start_ms,
        format_end_ms(config.end_ms)
    ));
    markdown.push_str(&format!(
        "- Sample Limit Per Kind: {}\n\n",
        config.sample_limit_per_kind
    ));

    markdown.push_str("## Segment Totals\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Segments | {} |\n", report.totals.segments));
    markdown.push_str(&format!(
        "| Segment Datapoints | {} |\n",
        report.totals.datapoints
    ));
    markdown.push_str(&format!("| Segment Series | {} |\n", report.totals.series));
    markdown.push_str(&format!("| Chunks | {} |\n", report.totals.chunks));
    markdown.push_str(&format!(
        "| Chunk Bytes | {} |\n\n",
        report.totals.chunk_bytes
    ));

    markdown.push_str("## Chunk Kinds\n\n");
    markdown.push_str("| Kind | Chunks | Chunk Bytes |\n");
    markdown.push_str("| --- | ---: | ---: |\n");
    for kind in [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ] {
        let stats = kind_stats(report, kind);
        markdown.push_str(&format!(
            "| {} | {} | {} |\n",
            kind_name(kind),
            stats.chunks,
            stats.chunk_bytes
        ));
    }
    markdown.push('\n');

    markdown.push_str("## Sampled Native Series\n\n");
    markdown.push_str(
        "| Kind | Metric | Segment | Series Ref | Samples | Time Range Ms | Chunk Bytes | Labels |\n",
    );
    markdown.push_str("| --- | --- | --- | ---: | ---: | --- | ---: | --- |\n");
    for sample in &report.sample_series {
        markdown.push_str(&format!(
            "| {} | `{}` | `{}` | {} | {} | {}..{} | {} | `{}` |\n",
            kind_name(sample.kind),
            markdown_escape_inline(sample_metric_name(&sample.labels)),
            markdown_escape_inline(&sample.segment_id),
            sample.series_ref,
            sample.samples,
            sample.min_time_ms,
            sample.max_time_ms,
            sample.chunk_bytes,
            markdown_escape_inline(&format_labels(&sample.labels))
        ));
    }
    markdown.push('\n');

    markdown.push_str("## PromQL Readbacks\n\n");
    markdown.push_str("| Kind | Query | result_series | result_samples | matched_series | projected_series | chunk_reads | bytes_read | samples_decoded | typed_scalar_chunks_decoded | typed_full_chunks_decoded |\n");
    markdown
        .push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for query in &report.queries {
        markdown.push_str(&format!(
            "| {} | `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            kind_name(query.kind),
            markdown_escape_inline(&query.query),
            query.result_series,
            query.result_samples,
            query.matched_series,
            query.projected_series,
            query.chunk_reads,
            query.bytes_read,
            query.samples_decoded,
            query.typed_scalar_chunks_decoded,
            query.typed_full_chunks_decoded
        ));
    }

    if let Some(verification) = verification {
        markdown.push_str("\n## Readback Verification\n\n");
        markdown.push_str("| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Checked Queries | {} |\n",
            verification.checked_queries
        ));
        markdown.push_str(&format!(
            "| Mismatches | {} |\n",
            verification.mismatches.len()
        ));

        if !verification.mismatches.is_empty() {
            markdown.push_str("\n| Query | Expected Missing Samples | Actual Samples |\n");
            markdown.push_str("| --- | --- | --- |\n");
            for mismatch in &verification.mismatches {
                markdown.push_str(&format!(
                    "| `{}` | `{}` | `{}` |\n",
                    markdown_escape_inline(&mismatch.query),
                    markdown_escape_inline(&format_samples(&mismatch.missing_expected_samples)),
                    markdown_escape_inline(&format_samples(&mismatch.actual_samples))
                ));
            }
        }
    }

    if let Some(diagnostics) = diagnostics {
        append_query_diagnostics(&mut markdown, diagnostics);
    }

    markdown
}

fn append_query_diagnostics(markdown: &mut String, diagnostics: &QuerySmokeDiagnostics) {
    markdown.push_str("\n## Query Diagnostics\n\n");
    markdown.push_str("| Phase | Duration |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Store Open | {} |\n",
        format_duration(diagnostics.store_open)
    ));
    markdown.push_str(&format!(
        "| Smoke Verify | {} |\n",
        format_duration(diagnostics.smoke_verify)
    ));

    if let Some(readback) = &diagnostics.readback {
        markdown.push_str(&format!(
            "| Collect Expected Readbacks | {} |\n",
            format_duration(readback.collect_expected_readbacks)
        ));
        markdown.push_str(&format!(
            "| Readback Store Open | {} |\n",
            format_duration(readback.store_open)
        ));
        markdown.push_str(&format!(
            "| Query Session Open | {} |\n",
            format_duration(readback.query_session_open)
        ));
        markdown.push_str(&format!(
            "| Readback PromQL Queries | {} |\n",
            format_duration(readback.promql_queries)
        ));

        markdown.push_str("\n| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Expected Readback Queries | {} |\n",
            readback.expected_queries
        ));
        markdown.push_str(&format!(
            "| Executed Readback Queries | {} |\n",
            readback.executed_queries
        ));
        markdown.push_str(&format!(
            "| Skipped Readback Queries | {} |\n",
            readback.skipped_queries
        ));
        markdown.push_str(&format!(
            "| Isolation Check Skips | {} |\n",
            readback.isolation_check_skips
        ));
        markdown.push_str(&format!(
            "| Index Routing Opens | {} |\n",
            readback.session_stats.index_routing_opens
        ));
        markdown.push_str(&format!(
            "| Segment Context Opens | {} |\n",
            readback.session_stats.segment_context_opens
        ));
        markdown.push_str(&format!(
            "| Symbols Opens | {} |\n",
            readback.session_stats.symbols_bin_opens
        ));
        markdown.push_str(&format!(
            "| Indexes Opens | {} |\n",
            readback.session_stats.indexes_puffin_opens
        ));
        markdown.push_str(&format!(
            "| Series Opens | {} |\n",
            readback.session_stats.series_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunk Index Opens | {} |\n",
            readback.session_stats.chunk_index_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunks Opens | {} |\n",
            readback.session_stats.chunks_bin_opens
        ));
        render_profile_table(
            markdown,
            "Readback Query Session Read Profile",
            readback.session_profile,
        );
    }
}

fn format_duration(duration: Duration) -> String {
    format!("{duration:?}")
}

fn run_query_smoke(config: &QuerySmokeConfig) -> io::Result<SegmentStoreSmokeReport> {
    let mut diagnostics = QuerySmokeDiagnostics::default();
    let phase_start = Instant::now();
    let store = open_segment_store(&config.segments_dir, config.validate_segment_footers)?;
    diagnostics.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let report =
        store.smoke_verify(config.start_ms, config.end_ms, config.sample_limit_per_kind)?;
    diagnostics.smoke_verify = phase_start.elapsed();

    let verification = if config.verify_readbacks {
        let (verification, readback_diagnostics) = verify_readbacks(config, &report)?;
        diagnostics.readback = Some(readback_diagnostics);
        Some(verification)
    } else {
        None
    };
    let markdown = render_markdown(config, &report, verification.as_ref(), Some(&diagnostics));

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

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryReadbackVerification {
    checked_queries: usize,
    mismatches: Vec<QueryReadbackMismatch>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QuerySmokeDiagnostics {
    store_open: Duration,
    smoke_verify: Duration,
    readback: Option<QueryReadbackDiagnostics>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QueryReadbackDiagnostics {
    collect_expected_readbacks: Duration,
    store_open: Duration,
    query_session_open: Duration,
    promql_queries: Duration,
    expected_queries: usize,
    executed_queries: usize,
    skipped_queries: usize,
    isolation_check_skips: usize,
    session_stats: SegmentStoreQuerySessionStats,
    session_profile: SegmentStoreQueryProfile,
}

#[derive(Debug, Clone, PartialEq)]
struct QueryReadbackMismatch {
    query: String,
    missing_expected_samples: Vec<(u64, f64)>,
    actual_samples: Vec<(u64, f64)>,
}

#[derive(Debug, Clone, PartialEq)]
struct ExpectedReadback {
    query: String,
    start_ms: u64,
    end_ms: u64,
    samples: Vec<(u64, f64)>,
    isolation_check: Option<ReadbackIsolationCheck>,
}

#[derive(Debug, Clone, PartialEq)]
struct ReadbackIsolationCheck {
    query: String,
    start_ms: u64,
    end_ms: u64,
    samples: Vec<(u64, f64)>,
}

impl ExpectedReadback {
    fn isolation_check(&self) -> ReadbackIsolationCheck {
        ReadbackIsolationCheck {
            query: self.query.clone(),
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            samples: self.samples.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ProjectedCounterReadback {
    readback: ExpectedReadback,
    range_hints: Option<Vec<CounterResetHint>>,
}

fn verify_readbacks(
    config: &QuerySmokeConfig,
    report: &SegmentStoreSmokeReport,
) -> io::Result<(QueryReadbackVerification, QueryReadbackDiagnostics)> {
    let mut diagnostics = QueryReadbackDiagnostics::default();
    let required_kinds = required_readback_kinds(report);

    let phase_start = Instant::now();
    let expected = collect_expected_readbacks(config, &required_kinds)?;
    diagnostics.collect_expected_readbacks = phase_start.elapsed();
    diagnostics.expected_queries = expected.len();

    let phase_start = Instant::now();
    let store = open_segment_store(&config.segments_dir, config.validate_segment_footers)?;
    diagnostics.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let mut query_session = store.query_session()?;
    diagnostics.query_session_open = phase_start.elapsed();
    let mut mismatches = Vec::new();
    let mut actual_cache = BTreeMap::<(String, u64, u64), Vec<(u64, f64)>>::new();
    let mut checked_queries = 0usize;

    let phase_start = Instant::now();
    for expected in &expected {
        if let Some(isolation_check) = &expected.isolation_check {
            let actual_samples = cached_readback_samples(
                &mut query_session,
                &mut actual_cache,
                &isolation_check.query,
                isolation_check.start_ms,
                isolation_check.end_ms,
            )?;
            if !promql_samples_eq(&actual_samples, &isolation_check.samples) {
                diagnostics.skipped_queries = diagnostics.skipped_queries.saturating_add(1);
                diagnostics.isolation_check_skips =
                    diagnostics.isolation_check_skips.saturating_add(1);
                continue;
            }
        }

        let actual_samples = cached_readback_samples(
            &mut query_session,
            &mut actual_cache,
            &expected.query,
            expected.start_ms,
            expected.end_ms,
        )?;
        diagnostics.executed_queries = diagnostics.executed_queries.saturating_add(1);
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
        if !missing_expected_samples.is_empty() {
            mismatches.push(QueryReadbackMismatch {
                query: expected.query.clone(),
                missing_expected_samples,
                actual_samples,
            });
        }
    }
    diagnostics.promql_queries = phase_start.elapsed();
    diagnostics.session_stats = query_session.stats();
    diagnostics.session_profile = query_session.profile();

    Ok((
        QueryReadbackVerification {
            checked_queries,
            mismatches,
        },
        diagnostics,
    ))
}

fn cached_readback_samples(
    query_session: &mut SegmentStoreQuerySession<'_>,
    actual_cache: &mut BTreeMap<(String, u64, u64), Vec<(u64, f64)>>,
    query: &str,
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Vec<(u64, f64)>> {
    let key = (query.to_string(), start_ms, end_ms);
    if let Some(samples) = actual_cache.get(&key) {
        return Ok(samples.clone());
    }

    let results = query_session
        .query_promql(query, start_ms, end_ms)
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

fn collect_expected_readbacks(
    config: &QuerySmokeConfig,
    required_kinds: &[bool; 5],
) -> io::Result<Vec<ExpectedReadback>> {
    let mut expected = Vec::new();
    let mut samples_by_kind = [0usize; 5];

    for segment_dir in segment_dirs(&config.segments_dir)? {
        if sample_limits_reached(
            &samples_by_kind,
            config.sample_limit_per_kind,
            required_kinds,
        ) {
            break;
        }
        let reader = SegmentReader::open(&segment_dir)?;
        if reader.meta().end_ms < config.start_ms || reader.meta().start_ms > config.end_ms {
            continue;
        }

        let symbols = read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols))?)?;
        let mut series_reader =
            SeriesReader::open(File::open(reader.file_path(SegmentFile::Series))?)?;
        let mut chunk_index_reader =
            ChunkIndexReader::open(File::open(reader.file_path(SegmentFile::ChunkIndex))?)?;
        let mut chunk_file = reader.open_chunks()?;

        for series_ref in 0..chunk_index_reader.len() {
            if sample_limits_reached(
                &samples_by_kind,
                config.sample_limit_per_kind,
                required_kinds,
            ) {
                break;
            }
            let series_ref = u32::try_from(series_ref).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "series_ref exceeds u32")
            })?;
            let Some(entries) = chunk_index_reader.read_entries(series_ref)? else {
                continue;
            };

            let mut labels = None;
            for entry in entries {
                if entry.max_time_ms < config.start_ms || entry.min_time_ms > config.end_ms {
                    continue;
                }
                let kind_index = chunk_kind_index(entry.kind);
                if !required_kinds[kind_index] {
                    continue;
                }
                if config.sample_limit_per_kind == 0
                    || samples_by_kind[kind_index] >= config.sample_limit_per_kind
                {
                    continue;
                }

                if labels.is_none() {
                    let Some(series_entry) = series_reader.read_entry(series_ref)? else {
                        continue;
                    };
                    labels = Some(resolve_series_labels(&symbols, &series_entry)?);
                }
                let Some(labels) = labels.as_ref() else {
                    continue;
                };

                let record = read_chunk_record_at(&mut chunk_file, entry.offset, entry.length)?;
                let readback_start_ms = config.start_ms.max(record.min_time_ms);
                let readback_end_ms = config.end_ms.min(record.max_time_ms);
                let mut readbacks = expected_readbacks_for_record(
                    labels,
                    &record,
                    readback_start_ms,
                    readback_end_ms,
                );
                if !readbacks.is_empty() {
                    samples_by_kind[kind_index] = samples_by_kind[kind_index].saturating_add(1);
                    expected.append(&mut readbacks);
                }
            }
        }
    }

    Ok(expected)
}

fn sample_limits_reached(
    samples_by_kind: &[usize; 5],
    sample_limit_per_kind: usize,
    required_kinds: &[bool; 5],
) -> bool {
    if sample_limit_per_kind == 0 {
        return true;
    }
    required_kinds
        .iter()
        .zip(samples_by_kind.iter())
        .all(|(required, samples)| !*required || *samples >= sample_limit_per_kind)
}

fn segment_dirs(segments_dir: &Path) -> io::Result<Vec<PathBuf>> {
    if let Some(inventory) = read_manifest_inventory(segments_dir.join("manifest"))? {
        return Ok(inventory
            .segments
            .into_iter()
            .map(|segment| segments_dir.join(segment.segment_id))
            .collect());
    }

    let mut dirs = Vec::new();
    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("seg-") {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn open_segment_store(
    segments_dir: &Path,
    validate_segment_footers: bool,
) -> io::Result<SegmentStoreReader> {
    let manifest_dir = segments_dir.join("manifest");
    if read_manifest_inventory(&manifest_dir)?.is_some() {
        SegmentStoreReader::open_manifest_published_with_options(
            segments_dir,
            &manifest_dir,
            SegmentStoreOpenOptions {
                validate_segment_footers,
            },
        )
    } else {
        SegmentStoreReader::open(segments_dir)
    }
}

fn resolve_series_labels(
    symbols: &SegmentSymbols,
    series_entry: &SeriesEntry,
) -> io::Result<Vec<(String, String)>> {
    series_entry
        .labels
        .iter()
        .map(|(key, value)| {
            let key = symbols.resolve(*key).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series label key missing")
            })?;
            let value = symbols.resolve(*value).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series label value missing")
            })?;
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

fn expected_readbacks_for_record(
    labels: &[(String, String)],
    record: &ChunkRecord,
    start_ms: u64,
    end_ms: u64,
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
            samples: filter_samples(samples.iter().copied(), start_ms, end_ms),
            isolation_check: None,
        }),
        ChunkSamples::Int64(samples) => scalar_expected_readbacks(ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            start_ms,
            end_ms,
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
        ChunkSamples::ExponentialHistogram(samples) => {
            exponential_histogram_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
        ChunkSamples::Summary(samples) => {
            summary_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
    }
    .into_iter()
    .filter(|readback| !readback.samples.is_empty())
    .collect()
}

fn scalar_expected_readbacks(base: ExpectedReadback) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![base];
    let Some((latest_ts, latest_value)) = readbacks[0]
        .samples
        .iter()
        .rev()
        .copied()
        .find(|(_, value)| value.is_finite())
    else {
        return readbacks;
    };
    if latest_ts != readbacks[0].end_ms {
        return readbacks;
    }

    readbacks.push(ExpectedReadback {
        query: format!("({}) * 2", readbacks[0].query),
        start_ms: latest_ts,
        end_ms: latest_ts,
        samples: vec![(latest_ts, latest_value * 2.0)],
        isolation_check: None,
    });
    readbacks.push(ExpectedReadback {
        query: format!("sum({})", readbacks[0].query),
        start_ms: latest_ts,
        end_ms: latest_ts,
        samples: vec![(latest_ts, latest_value)],
        isolation_check: None,
    });
    let base = readbacks[0].clone();
    push_counter_range_readbacks(&mut readbacks, &base, None);
    readbacks
}

fn push_counter_range_readbacks(
    readbacks: &mut Vec<ExpectedReadback>,
    base: &ExpectedReadback,
    counter_reset_hints: Option<&[CounterResetHint]>,
) {
    let Some((range_ms, increase)) = scalar_counter_range_increase(base, counter_reset_hints)
    else {
        return;
    };
    let range_seconds = range_ms as f64 / 1_000.0;
    if range_seconds <= 0.0 {
        return;
    }

    readbacks.push(ExpectedReadback {
        query: format!("rate({}[{}ms])", base.query, range_ms),
        start_ms: base.end_ms,
        end_ms: base.end_ms,
        samples: vec![(base.end_ms, increase / range_seconds)],
        isolation_check: Some(base.isolation_check()),
    });
    readbacks.push(ExpectedReadback {
        query: format!("increase({}[{}ms])", base.query, range_ms),
        start_ms: base.end_ms,
        end_ms: base.end_ms,
        samples: vec![(base.end_ms, increase)],
        isolation_check: Some(base.isolation_check()),
    });
}

fn scalar_counter_range_increase(
    readback: &ExpectedReadback,
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<(u64, f64)> {
    let latest_ts = readback.end_ms;
    let earliest_ts = readback.samples.first()?.0;
    let range_ms = latest_ts.saturating_sub(earliest_ts).saturating_add(1);
    if range_ms == 0 {
        return None;
    }
    let range_start_ms = latest_ts.saturating_sub(range_ms);
    let mut selected = Vec::new();
    let mut selected_hints = counter_reset_hints.map(|_| Vec::new());
    for (idx, sample) in readback.samples.iter().copied().enumerate() {
        if sample.0 <= range_start_ms || sample.0 > latest_ts {
            continue;
        }
        selected.push(sample);
        if let (Some(hints), Some(selected_hints)) = (counter_reset_hints, selected_hints.as_mut())
        {
            if let Some(hint) = hints.get(idx).copied() {
                selected_hints.push(hint);
            }
        }
    }
    if selected_hints
        .as_ref()
        .is_some_and(|hints| hints.len() != selected.len())
    {
        selected_hints = None;
    }
    let mut effective_range_start_ms = range_start_ms;
    if let Some(last_non_finite_idx) = selected.iter().rposition(|(_, value)| !value.is_finite()) {
        effective_range_start_ms = effective_range_start_ms.max(selected[last_non_finite_idx].0);
        selected.drain(..=last_non_finite_idx);
        if let Some(hints) = selected_hints.as_mut() {
            hints.drain(..=last_non_finite_idx);
        }
    }
    if selected.len() < 2 || selected.iter().any(|(_, value)| !value.is_finite()) {
        return None;
    }

    expected_extrapolated_counter_increase(
        &selected,
        selected_hints.as_deref(),
        effective_range_start_ms,
        latest_ts,
    )
    .map(|increase| (range_ms, increase))
}

fn expected_extrapolated_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if samples.len() < 2 || range_end_ms <= range_start_ms {
        return None;
    }

    let (first_ts, first_value) = samples.first().copied()?;
    let (last_ts, _) = samples.last().copied()?;
    if last_ts <= first_ts || !first_value.is_finite() {
        return None;
    }

    let raw_increase = expected_counter_increase(samples, counter_reset_hints)?;
    let sampled_interval = (last_ts - first_ts) as f64 / 1_000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;
    let mut duration_to_start = first_ts.saturating_sub(range_start_ms) as f64 / 1_000.0;
    let mut duration_to_end = range_end_ms.saturating_sub(last_ts) as f64 / 1_000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if raw_increase > 0.0 && first_value >= 0.0 {
        let duration_to_zero = sampled_interval * (first_value / raw_increase);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    Some(raw_increase * (sampled_interval + duration_to_start + duration_to_end) / sampled_interval)
}

fn expected_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    if let Some(counter_reset_hints) = counter_reset_hints {
        return expected_counter_increase_with_reset_hints(samples, counter_reset_hints);
    }
    expected_counter_increase_from_value_decreases(samples)
}

fn expected_counter_increase_with_reset_hints(
    samples: &[(u64, f64)],
    counter_reset_hints: &[CounterResetHint],
) -> Option<f64> {
    if counter_reset_hints.len() != samples.len() {
        return expected_counter_increase_from_value_decreases(samples);
    }
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples
        .iter()
        .copied()
        .zip(counter_reset_hints.iter().copied());
    let ((_, first), _) = iter.next()?;
    if !first.is_finite() {
        return None;
    }

    let mut previous = first;
    let mut increase = 0.0f64;
    for ((_, current), reset_hint) in iter {
        if !current.is_finite() {
            return None;
        }
        match reset_hint {
            CounterResetHint::CounterReset => {
                increase += current;
            }
            CounterResetHint::NotCounterReset => {
                if current < previous {
                    return None;
                }
                increase += current - previous;
            }
            CounterResetHint::Unknown => {
                if current >= previous {
                    increase += current - previous;
                } else {
                    increase += current;
                }
            }
            CounterResetHint::GaugeType => return None,
        }
        previous = current;
    }
    Some(increase)
}

fn expected_counter_increase_from_value_decreases(samples: &[(u64, f64)]) -> Option<f64> {
    let (_, first) = samples.first().copied()?;
    if !first.is_finite() {
        return None;
    }

    let mut previous = first;
    let mut increase = 0.0f64;
    for (_, current) in samples.iter().skip(1).copied() {
        if !current.is_finite() {
            return None;
        }
        if current >= previous {
            increase += current - previous;
        } else {
            increase += current;
        }
        previous = current;
    }
    Some(increase)
}

fn histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let (count_samples, count_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    let mut projected = vec![ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            samples: count_samples,
            isolation_check: None,
        },
        range_hints: count_hints,
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        let (sum_samples, sum_hints) = project_optional_f64_counter_samples_with_range_hints(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
                start_ms,
                end_ms,
                samples: sum_samples,
                isolation_check: None,
            },
            range_hints: sum_hints,
        });
    }

    if let Some(le) = samples
        .first()
        .and_then(|(_, value)| value.explicit_bounds.first().copied())
        .map(format_promql_float_label)
    {
        let (bucket_samples, bucket_hints) = project_histogram_bucket_samples_with_range_hints(
            samples,
            Some(le.as_str()),
            start_ms,
            end_ms,
        );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(
                    &format!("{metric_name}_bucket"),
                    labels,
                    Some(("le", le.as_str())),
                ),
                start_ms,
                end_ms,
                samples: bucket_samples,
                isolation_check: None,
            },
            range_hints: bucket_hints,
        });
    }

    let (inf_bucket_samples, inf_bucket_hints) =
        project_histogram_bucket_samples_with_range_hints(samples, Some("+Inf"), start_ms, end_ms);
    projected.push(ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(
                &format!("{metric_name}_bucket"),
                labels,
                Some(("le", "+Inf")),
            ),
            start_ms,
            end_ms,
            samples: inf_bucket_samples,
            isolation_check: None,
        },
        range_hints: inf_bucket_hints,
    });

    let mut readbacks = projected
        .iter()
        .map(|projected| projected.readback.clone())
        .collect::<Vec<_>>();
    for projected in &projected {
        if let Some(hints) = &projected.range_hints {
            push_counter_range_readbacks(&mut readbacks, &projected.readback, Some(hints));
        }
    }
    readbacks
}

fn exponential_histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(
        u64,
        chronoxide_core::storage::head::ExponentialHistogramValue,
    )],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let (count_samples, count_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    let mut projected = vec![ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            samples: count_samples,
            isolation_check: None,
        },
        range_hints: count_hints,
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        let (sum_samples, sum_hints) = project_optional_f64_counter_samples_with_range_hints(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
                start_ms,
                end_ms,
                samples: sum_samples,
                isolation_check: None,
            },
            range_hints: sum_hints,
        });
    }

    let (inf_bucket_samples, inf_bucket_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    projected.push(ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(
                &format!("{metric_name}_bucket"),
                labels,
                Some(("le", "+Inf")),
            ),
            start_ms,
            end_ms,
            samples: inf_bucket_samples,
            isolation_check: None,
        },
        range_hints: inf_bucket_hints,
    });

    let mut readbacks = projected
        .iter()
        .map(|projected| projected.readback.clone())
        .collect::<Vec<_>>();
    for projected in &projected {
        if let Some(hints) = &projected.range_hints {
            push_counter_range_readbacks(&mut readbacks, &projected.readback, Some(hints));
        }
    }
    readbacks
}

fn summary_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::SummaryValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            samples: project_u64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.count)),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        },
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            start_ms,
            end_ms,
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, Some(value.sum))),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        },
    ];

    if let Some(quantile) = samples
        .first()
        .and_then(|(_, value)| value.quantiles.first())
        .map(|quantile| format_promql_float_label(quantile.quantile))
    {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(
                metric_name,
                labels,
                Some(("quantile", quantile.as_str())),
            ),
            start_ms,
            end_ms,
            samples: filter_samples(
                samples.iter().map(|(ts, value)| {
                    let sample_value = value
                        .quantiles
                        .first()
                        .map(|quantile| quantile.value)
                        .unwrap_or(f64::NAN);
                    (
                        *ts,
                        typed_f64_value(value.metadata.is_stale(), sample_value),
                    )
                }),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        });
    }

    readbacks
}

fn project_u64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    project_u64_counter_samples_with_range_hints(samples, start_ms, end_ms).0
}

fn project_u64_counter_samples_with_range_hints(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0u64;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, metadata, raw) in samples {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            prometheus_stale_nan()
        } else if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            raw as f64
        };
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(metadata.reset_hint);
        }
        out.push((ts, value));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn project_optional_f64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    project_optional_f64_counter_samples_with_range_hints(samples, start_ms, end_ms).0
}

fn project_optional_f64_counter_samples_with_range_hints(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0.0f64;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, metadata, raw) in samples {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            prometheus_stale_nan()
        } else if let Some(raw) = raw {
            if metadata.temporality == OtlpAggregationTemporality::Delta {
                range_supported = false;
                accumulator += raw;
                accumulator
            } else {
                raw
            }
        } else {
            continue;
        };
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(metadata.reset_hint);
        }
        out.push((ts, value));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn project_histogram_bucket_samples_with_range_hints(
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    le_filter: Option<&str>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0u64;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, value) in samples {
        if *ts < start_ms || *ts > end_ms {
            continue;
        }

        let mut cumulative = 0u64;
        let mut raw = None;
        for (idx, bound) in value.explicit_bounds.iter().enumerate() {
            cumulative =
                cumulative.saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
            let le = format_promql_float_label(*bound);
            if le_filter.is_some_and(|filter| filter == le) {
                raw = Some(cumulative);
                break;
            }
        }
        if le_filter.is_some_and(|filter| filter == "+Inf") {
            raw = Some(value.count);
        }
        let Some(raw) = raw else {
            continue;
        };

        let projected = if value.metadata.is_stale() {
            prometheus_stale_nan()
        } else if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            raw as f64
        };
        if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(value.metadata.reset_hint);
        }
        out.push((*ts, projected));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn filter_samples(
    samples: impl IntoIterator<Item = (u64, f64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    samples
        .into_iter()
        .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms)
        .collect()
}

fn typed_f64_value(stale: bool, value: f64) -> f64 {
    if stale { prometheus_stale_nan() } else { value }
}

fn promql_sample_eq(left: (u64, f64), right: (u64, f64)) -> bool {
    left.0 == right.0 && left.1.to_bits() == right.1.to_bits()
}

fn promql_samples_eq(left: &[(u64, f64)], right: &[(u64, f64)]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .copied()
            .zip(right.iter().copied())
            .all(|(left, right)| promql_sample_eq(left, right))
}

fn chunk_kind_index(kind: ChunkKind) -> usize {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}

fn format_end_ms(end_ms: u64) -> String {
    if end_ms == u64::MAX {
        "max".to_string()
    } else {
        end_ms.to_string()
    }
}

fn format_query_limit(limit: Option<u64>) -> String {
    limit
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unlimited".to_string())
}

fn kind_stats(report: &SegmentStoreSmokeReport, kind: ChunkKind) -> SegmentStoreSmokeKindStats {
    match kind {
        ChunkKind::Float => report.totals.by_kind.float,
        ChunkKind::Int64 => report.totals.by_kind.int64,
        ChunkKind::Histogram => report.totals.by_kind.histogram,
        ChunkKind::ExponentialHistogram => report.totals.by_kind.exponential_histogram,
        ChunkKind::Summary => report.totals.by_kind.summary,
    }
}

fn kind_name(kind: ChunkKind) -> &'static str {
    match kind {
        ChunkKind::Float => "Float",
        ChunkKind::Int64 => "Int64",
        ChunkKind::Histogram => "Histogram",
        ChunkKind::ExponentialHistogram => "ExponentialHistogram",
        ChunkKind::Summary => "Summary",
    }
}

fn sample_metric_name(labels: &[(String, String)]) -> &str {
    labels
        .iter()
        .find_map(|(name, value)| (name == METRIC_NAME_LABEL).then_some(value.as_str()))
        .unwrap_or("<missing>")
}

fn format_labels(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn promql_exact_selector(
    metric_name: &str,
    labels: &[(String, String)],
    extra_label: Option<(&str, &str)>,
) -> String {
    let mut matchers = Vec::with_capacity(labels.len() + usize::from(extra_label.is_some()));
    matchers.push(format!(
        r#"{}="{}""#,
        METRIC_NAME_LABEL,
        promql_escape_string(metric_name)
    ));
    for (key, value) in labels {
        if key == METRIC_NAME_LABEL || extra_label.is_some_and(|(extra_key, _)| extra_key == key) {
            continue;
        }
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    if let Some((key, value)) = extra_label {
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    format!("{{{}}}", matchers.join(","))
}

fn promql_escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}

fn format_promql_float_label(value: f64) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "+Inf".to_string()
    } else {
        value.to_string()
    }
}

fn format_samples(samples: &[(u64, f64)]) -> String {
    samples
        .iter()
        .map(|(ts, value)| format!("({ts}, {value:?})"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn markdown_escape_inline(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace('|', "\\|")
        .replace(['\n', '\r'], " ")
}

#[cfg(test)]
#[path = "chronoxide_query/tests.rs"]
mod tests;
