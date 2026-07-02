use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::chunk::{
    ChunkIndexReader, ChunkKind, ChunkRecord, ChunkSamples, read_chunk_record_at,
};
use chronoxide_core::storage::head::{
    OtlpAggregationTemporality, TypedSampleMetadata, prometheus_stale_nan,
};
use chronoxide_core::storage::segment::{
    QueryLimits, QueryStats, SegmentFile, SegmentReader, SegmentStoreQuerySessionStats,
    SegmentStoreReader, SegmentStoreSmokeKindStats, SegmentStoreSmokeReport,
};
use chronoxide_core::storage::series::{
    SegmentSymbols, SeriesEntry, SeriesReader, read_symbols_bin,
};
use clap::Parser;

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
    #[arg(long = "query")]
    queries: Vec<String>,
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
        };

        match run_query_benchmark(&config) {
            Ok(report) => {
                println!(
                    "wrote {} with {} explicit queries",
                    config.output.display(),
                    report.results.len()
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
}

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryBenchmarkReport {
    store_open: Duration,
    query_session_open: Duration,
    promql_queries: Duration,
    session_stats: SegmentStoreQuerySessionStats,
    results: Vec<QueryBenchmarkResult>,
}

#[derive(Debug, Clone, PartialEq)]
struct QueryBenchmarkResult {
    query: String,
    duration: Duration,
    result_series: u64,
    result_samples: u64,
    stats: QueryStats,
}

fn run_query_benchmark(config: &QueryBenchmarkConfig) -> io::Result<QueryBenchmarkReport> {
    if config.queries.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "query benchmark requires at least one --query",
        ));
    }

    let mut report = QueryBenchmarkReport::default();

    let phase_start = Instant::now();
    let store = SegmentStoreReader::open(&config.segments_dir)?;
    report.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let mut query_session = store.query_session()?;
    report.query_session_open = phase_start.elapsed();

    let phase_start = Instant::now();
    for query in &config.queries {
        let query_start = Instant::now();
        let execution = query_session
            .query_promql_with_limits(
                query,
                config.start_ms,
                config.end_ms,
                QueryLimits::unlimited(),
            )
            .map_err(|err| io::Error::other(format!("query failed: {query}: {err}")))?;
        let duration = query_start.elapsed();
        let result_series = execution.results.len() as u64;
        let result_samples = execution
            .results
            .iter()
            .map(|result| result.samples.len() as u64)
            .sum();
        report.results.push(QueryBenchmarkResult {
            query: query.clone(),
            duration,
            result_series,
            result_samples,
            stats: execution.stats,
        });
    }
    report.promql_queries = phase_start.elapsed();
    report.session_stats = query_session.stats();

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
        "| PromQL Queries | {} |\n\n",
        format_duration(report.promql_queries)
    ));

    markdown.push_str("## Query Totals\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Queries | {} |\n", report.results.len()));
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

    markdown.push_str("## Query Results\n\n");
    markdown.push_str("| Query | Duration | segments_considered | segments_skipped_by_time | segments_skipped_by_missing_equality | segments_skipped_by_matcher_time_range | segments_queried | result_series | result_samples | matched_series | chunk_reads | bytes_read | index_postings_reads | index_postings_bytes_read | samples_decoded | regex_values_examined |\n");
    markdown.push_str(
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for result in &report.results {
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            format_duration(result.duration),
            result.stats.segments_considered,
            result.stats.segments_skipped_by_time,
            result.stats.segments_skipped_by_missing_equality,
            result.stats.segments_skipped_by_matcher_time_range,
            result.stats.segments_queried,
            result.result_series,
            result.result_samples,
            result.stats.matched_series,
            result.stats.chunk_reads,
            result.stats.bytes_read,
            result.stats.index_postings_reads,
            result.stats.index_postings_bytes_read,
            result.stats.samples_decoded,
            result.stats.regex_values_examined
        ));
    }

    markdown
}

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryBenchmarkTotals {
    result_series: u64,
    result_samples: u64,
    stats: QueryStats,
}

fn benchmark_totals(report: &QueryBenchmarkReport) -> QueryBenchmarkTotals {
    let mut totals = QueryBenchmarkTotals::default();
    for result in &report.results {
        totals.result_series = totals.result_series.saturating_add(result.result_series);
        totals.result_samples = totals.result_samples.saturating_add(result.result_samples);
        totals.stats.segments_considered = totals
            .stats
            .segments_considered
            .saturating_add(result.stats.segments_considered);
        totals.stats.segments_skipped_by_time = totals
            .stats
            .segments_skipped_by_time
            .saturating_add(result.stats.segments_skipped_by_time);
        totals.stats.segments_skipped_by_missing_equality = totals
            .stats
            .segments_skipped_by_missing_equality
            .saturating_add(result.stats.segments_skipped_by_missing_equality);
        totals.stats.segments_skipped_by_matcher_time_range = totals
            .stats
            .segments_skipped_by_matcher_time_range
            .saturating_add(result.stats.segments_skipped_by_matcher_time_range);
        totals.stats.segments_queried = totals
            .stats
            .segments_queried
            .saturating_add(result.stats.segments_queried);
        totals.stats.matched_series = totals
            .stats
            .matched_series
            .saturating_add(result.stats.matched_series);
        totals.stats.chunk_reads = totals
            .stats
            .chunk_reads
            .saturating_add(result.stats.chunk_reads);
        totals.stats.bytes_read = totals
            .stats
            .bytes_read
            .saturating_add(result.stats.bytes_read);
        totals.stats.index_postings_reads = totals
            .stats
            .index_postings_reads
            .saturating_add(result.stats.index_postings_reads);
        totals.stats.index_postings_bytes_read = totals
            .stats
            .index_postings_bytes_read
            .saturating_add(result.stats.index_postings_bytes_read);
        totals.stats.samples_decoded = totals
            .stats
            .samples_decoded
            .saturating_add(result.stats.samples_decoded);
        totals.stats.regex_values_examined = totals
            .stats
            .regex_values_examined
            .saturating_add(result.stats.regex_values_examined);
    }
    totals
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
    markdown.push_str("| Kind | Query | result_series | result_samples | matched_series | chunk_reads | bytes_read | samples_decoded |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for query in &report.queries {
        markdown.push_str(&format!(
            "| {} | `{}` | {} | {} | {} | {} | {} | {} |\n",
            kind_name(query.kind),
            markdown_escape_inline(&query.query),
            query.result_series,
            query.result_samples,
            query.matched_series,
            query.chunk_reads,
            query.bytes_read,
            query.samples_decoded
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
    }
}

fn format_duration(duration: Duration) -> String {
    format!("{duration:?}")
}

fn run_query_smoke(config: &QuerySmokeConfig) -> io::Result<SegmentStoreSmokeReport> {
    let mut diagnostics = QuerySmokeDiagnostics::default();
    let phase_start = Instant::now();
    let store = SegmentStoreReader::open(&config.segments_dir)?;
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
    session_stats: SegmentStoreQuerySessionStats,
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
    let store = SegmentStoreReader::open(&config.segments_dir)?;
    diagnostics.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let mut query_session = store.query_session()?;
    diagnostics.query_session_open = phase_start.elapsed();
    let mut mismatches = Vec::new();

    let phase_start = Instant::now();
    for expected in &expected {
        let results = query_session
            .query_promql(&expected.query, expected.start_ms, expected.end_ms)
            .map_err(|err| io::Error::other(format!("query failed: {}: {err}", expected.query)))?;
        diagnostics.executed_queries = diagnostics.executed_queries.saturating_add(1);
        let actual_samples = results
            .iter()
            .flat_map(|result| result.samples.iter().copied())
            .collect::<Vec<_>>();
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

    Ok((
        QueryReadbackVerification {
            checked_queries: expected.len(),
            mismatches,
        },
        diagnostics,
    ))
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
        ChunkSamples::Float(samples) => vec![ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            start_ms,
            end_ms,
            samples: filter_samples(samples.iter().copied(), start_ms, end_ms),
        }],
        ChunkSamples::Int64(samples) => vec![ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            start_ms,
            end_ms,
            samples: filter_samples(
                samples.iter().map(|(ts, value)| (*ts, *value as f64)),
                start_ms,
                end_ms,
            ),
        }],
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

fn histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![ExpectedReadback {
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
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            start_ms,
            end_ms,
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.sum)),
                start_ms,
                end_ms,
            ),
        });
    }

    if let Some(le) = samples
        .first()
        .and_then(|(_, value)| value.explicit_bounds.first().copied())
        .map(format_promql_float_label)
    {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(
                &format!("{metric_name}_bucket"),
                labels,
                Some(("le", le.as_str())),
            ),
            start_ms,
            end_ms,
            samples: project_histogram_bucket_samples(samples, Some(le.as_str()), start_ms, end_ms),
        });
    }

    readbacks.push(ExpectedReadback {
        query: promql_exact_selector(
            &format!("{metric_name}_bucket"),
            labels,
            Some(("le", "+Inf")),
        ),
        start_ms,
        end_ms,
        samples: project_histogram_bucket_samples(samples, Some("+Inf"), start_ms, end_ms),
    });
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
    let mut readbacks = vec![ExpectedReadback {
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
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            start_ms,
            end_ms,
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.sum)),
                start_ms,
                end_ms,
            ),
        });
    }

    readbacks.push(ExpectedReadback {
        query: promql_exact_selector(
            &format!("{metric_name}_bucket"),
            labels,
            Some(("le", "+Inf")),
        ),
        start_ms,
        end_ms,
        samples: project_u64_counter_samples(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        ),
    });
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
        });
    }

    readbacks
}

fn project_u64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    let mut accumulator = 0u64;
    samples
        .into_iter()
        .filter(|(ts, _, _)| *ts >= start_ms && *ts <= end_ms)
        .map(|(ts, metadata, raw)| {
            let value = if metadata.is_stale() {
                prometheus_stale_nan()
            } else if metadata.temporality == OtlpAggregationTemporality::Delta {
                accumulator = accumulator.saturating_add(raw);
                accumulator as f64
            } else {
                raw as f64
            };
            (ts, value)
        })
        .collect()
}

fn project_optional_f64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    let mut accumulator = 0.0f64;
    samples
        .into_iter()
        .filter(|(ts, _, _)| *ts >= start_ms && *ts <= end_ms)
        .filter_map(|(ts, metadata, raw)| {
            let value = if metadata.is_stale() {
                prometheus_stale_nan()
            } else if let Some(raw) = raw {
                if metadata.temporality == OtlpAggregationTemporality::Delta {
                    accumulator += raw;
                    accumulator
                } else {
                    raw
                }
            } else {
                return None;
            };
            Some((ts, value))
        })
        .collect()
}

fn project_histogram_bucket_samples(
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    le_filter: Option<&str>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    let mut accumulator = 0u64;
    let mut out = Vec::new();
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
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            raw as f64
        };
        out.push((*ts, projected));
    }
    out
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
mod tests {
    use std::fs;
    use std::time::Duration;

    use chronoxide_core::labels::SeriesRef;
    use chronoxide_core::promql::METRIC_NAME_LABEL;
    use chronoxide_core::storage::head::{
        HistogramValue, OtlpAggregationTemporality, TypedSampleMetadata,
    };
    use chronoxide_core::storage::segment::{
        SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
    };

    use super::*;

    #[test]
    fn default_output_path_is_next_to_segments_dir() {
        let output = default_output_path(Path::new("data/smoke/segments-001"));

        assert!(output.starts_with("data/smoke"));
        assert!(
            output
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("query_smoke_")
        );
    }

    #[test]
    fn render_markdown_reports_real_readback_queries() {
        let tempdir = segment_store_with_float_and_histogram();

        let store = SegmentStoreReader::open(tempdir.path()).unwrap();
        let report = store.smoke_verify(0, 10_000, 1).unwrap();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: false,
        };

        let markdown = render_markdown(&config, &report, None, None);

        assert!(markdown.contains("# Chronoxide Query Smoke Report"));
        assert!(markdown.contains("cpu_usage"));
        assert!(markdown.contains("request_duration"));
        assert!(markdown.contains("_count"));
        assert!(markdown.contains("_bucket"));
        assert!(markdown.contains("| Float | 1 |"));
        assert!(markdown.contains("| Histogram | 1 |"));
        assert!(markdown.contains("matched_series"));

        fs::write(&config.output, markdown).unwrap();
        assert!(config.output.exists());
    }

    #[test]
    fn render_markdown_reports_query_diagnostics() {
        let tempdir = segment_store_with_float_and_histogram();

        let store = SegmentStoreReader::open(tempdir.path()).unwrap();
        let report = store.smoke_verify(0, 10_000, 1).unwrap();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: false,
        };
        let diagnostics = QuerySmokeDiagnostics {
            store_open: Duration::from_millis(1),
            smoke_verify: Duration::from_millis(2),
            readback: Some(QueryReadbackDiagnostics {
                collect_expected_readbacks: Duration::from_millis(3),
                store_open: Duration::from_millis(4),
                query_session_open: Duration::from_millis(5),
                promql_queries: Duration::from_millis(6),
                expected_queries: 7,
                executed_queries: 8,
                session_stats: SegmentStoreQuerySessionStats {
                    index_routing_opens: 15,
                    segment_context_opens: 9,
                    symbols_bin_opens: 10,
                    indexes_puffin_opens: 11,
                    series_bin_opens: 12,
                    chunk_index_bin_opens: 13,
                    chunks_bin_opens: 14,
                },
            }),
        };

        let markdown = render_markdown(&config, &report, None, Some(&diagnostics));

        assert!(markdown.contains("## Query Diagnostics"));
        assert!(markdown.contains("| Store Open |"));
        assert!(markdown.contains("| Smoke Verify |"));
        assert!(markdown.contains("| Collect Expected Readbacks |"));
        assert!(markdown.contains("| Segment Context Opens | 9 |"));
        assert!(markdown.contains("| Symbols Opens | 10 |"));
        assert!(markdown.contains("| Chunks Opens | 14 |"));
    }

    #[test]
    fn run_query_smoke_writes_report_from_real_segments() {
        let tempdir = segment_store_with_float_and_histogram();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: false,
        };

        let report = run_query_smoke(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert_eq!(report.totals.segments, 1);
        assert!(markdown.contains("request_duration"));
        assert!(markdown.contains("_bucket"));
        assert!(markdown.contains("## PromQL Readbacks"));
    }

    #[test]
    fn run_query_smoke_verifies_readbacks_against_decoded_chunks() {
        let tempdir = segment_store_with_float_and_histogram();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: true,
        };

        run_query_smoke(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert!(markdown.contains("## Readback Verification"));
        assert!(markdown.contains("| Checked Queries | 5 |"));
        assert!(markdown.contains("| Mismatches | 0 |"));
    }

    #[test]
    fn run_query_smoke_verifies_delta_histogram_readbacks_after_projection() {
        let tempdir = segment_store_with_delta_histogram();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: true,
        };

        run_query_smoke(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert!(markdown.contains("| Checked Queries | 4 |"));
        assert!(markdown.contains("| Mismatches | 0 |"));
    }

    #[test]
    fn run_query_benchmark_reports_explicit_promql_without_smoke_scan_sections() {
        let tempdir = segment_store_with_float_and_histogram();
        let config = QueryBenchmarkConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_benchmark.md"),
            start_ms: 0,
            end_ms: 10_000,
            queries: vec![
                "cpu.usage".to_string(),
                r#"request.duration_count"#.to_string(),
            ],
        };

        let report = run_query_benchmark(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert_eq!(report.results.len(), 2);
        assert_eq!(report.results[0].query, "cpu.usage");
        assert_eq!(report.results[0].result_samples, 2);
        assert_eq!(report.results[1].query, "request.duration_count");
        assert_eq!(report.results[1].result_samples, 1);
        assert!(report.session_stats.segment_context_opens > 0);

        assert!(markdown.contains("# Chronoxide Sealed Query Benchmark"));
        assert!(markdown.contains("## Query Results"));
        assert!(markdown.contains("## Session File Opens"));
        assert!(markdown.contains("| Queries | 2 |"));
        assert!(markdown.contains("Segments Considered"));
        assert!(markdown.contains("segments_skipped_by_missing_equality"));
        assert!(markdown.contains("Index Postings Reads"));
        assert!(markdown.contains("index_postings_bytes_read"));
        assert!(markdown.contains("cpu.usage"));
        assert!(markdown.contains("request.duration_count"));
        assert!(!markdown.contains("## Segment Totals"));
        assert!(!markdown.contains("## Sampled Native Series"));
        assert!(!markdown.contains("| Smoke Verify |"));
    }

    #[test]
    fn collect_expected_readbacks_scopes_queries_to_sampled_chunk_range() {
        let tempdir = segment_store_with_long_float_series();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: true,
        };

        let required_kinds = [true, false, false, false, false];
        let expected = collect_expected_readbacks(&config, &required_kinds).unwrap();

        assert_eq!(expected.len(), 1);
        assert_eq!(expected[0].start_ms, 0);
        assert_eq!(expected[0].end_ms, 999);
        assert_eq!(expected[0].samples.len(), 1_000);
    }

    #[test]
    fn sample_limits_are_reached_when_only_required_kinds_are_satisfied() {
        let required_kinds = [true, false, true, false, false];

        assert!(sample_limits_reached(&[1, 0, 1, 0, 0], 1, &required_kinds));
        assert!(!sample_limits_reached(
            &[1, 10, 0, 10, 10],
            1,
            &required_kinds
        ));
        assert!(sample_limits_reached(&[0, 0, 0, 0, 0], 0, &required_kinds));
    }

    fn segment_store_with_float_and_histogram() -> tempfile::TempDir {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();

        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.0), (2_000, 2.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "cpu.usage");
                    visit("instance", "host-a");
                },
            )
            .unwrap();

        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(
                    1_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(10.0),
                        min: Some(1.0),
                        max: Some(4.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 5.0],
                        bucket_counts: vec![1, 2, 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request.duration");
                    visit("route", "/typed");
                },
            )
            .unwrap();
        writer.flush().unwrap();

        tempdir
    }

    fn segment_store_with_delta_histogram() -> tempfile::TempDir {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();
        let metadata = TypedSampleMetadata {
            temporality: OtlpAggregationTemporality::Delta,
            ..TypedSampleMetadata::default()
        };

        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[
                    (
                        1_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(2.0),
                            min: Some(2.0),
                            max: Some(2.0),
                            metadata,
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![0, 1],
                        },
                    ),
                    (
                        2_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(3.0),
                            min: Some(3.0),
                            max: Some(3.0),
                            metadata,
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![1, 0],
                        },
                    ),
                    (
                        3_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(4.0),
                            min: Some(4.0),
                            max: Some(4.0),
                            metadata,
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![0, 1],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "delta.request.duration");
                    visit("route", "/delta");
                },
            )
            .unwrap();
        writer.flush().unwrap();

        tempdir
    }

    fn segment_store_with_long_float_series() -> tempfile::TempDir {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(1));
        let mut writer = SegmentWriter::new(config).unwrap();
        let samples = (0..5_000)
            .map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64))
            .collect::<Vec<_>>();

        writer
            .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &samples, |visit| {
                visit(METRIC_NAME_LABEL, "long.range.cpu");
                visit("instance", "host-a");
            })
            .unwrap();
        writer.flush().unwrap();

        tempdir
    }
}
