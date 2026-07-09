use std::fs;
use std::io::ErrorKind;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::head::{
    HistogramValue, OtlpAggregationTemporality, TypedSampleMetadata,
};
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, write_current,
};
use chronoxide_core::storage::segment::{
    SegmentReader, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
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
        validate_segment_footers: false,
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
        validate_segment_footers: false,
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
            skipped_queries: 2,
            isolation_check_skips: 2,
            session_stats: SegmentStoreQuerySessionStats {
                index_routing_opens: 15,
                segment_context_opens: 9,
                symbols_bin_opens: 10,
                indexes_puffin_opens: 11,
                series_bin_opens: 12,
                chunk_index_bin_opens: 13,
                chunks_bin_opens: 14,
            },
            session_profile: SegmentStoreQueryProfile {
                segment_context_open: Duration::from_millis(7),
                symbols_read: Duration::from_millis(8),
                symbols_file_bytes: 17,
                ..SegmentStoreQueryProfile::default()
            },
        }),
    };

    let markdown = render_markdown(&config, &report, None, Some(&diagnostics));

    assert!(markdown.contains("## Query Diagnostics"));
    assert!(markdown.contains("| Store Open |"));
    assert!(markdown.contains("| Smoke Verify |"));
    assert!(markdown.contains("| Collect Expected Readbacks |"));
    assert!(markdown.contains("| Segment Context Opens | 9 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 2 |"));
    assert!(markdown.contains("| Isolation Check Skips | 2 |"));
    assert!(markdown.contains("| Symbols Opens | 10 |"));
    assert!(markdown.contains("| Chunks Opens | 14 |"));
    assert!(markdown.contains("## Readback Query Session Read Profile"));
    assert!(markdown.contains("## Readback Query Session Opened File Sizes"));
    assert!(markdown.contains("## Readback Query Session Logical Read Bytes"));
    assert!(markdown.contains("| Segment Context Open | 7ms | 0 |"));
    assert!(markdown.contains("| symbols.bin | 8ms | 17 |"));
    assert!(!markdown.contains("| Stage | Duration | Bytes / Count |"));
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
        validate_segment_footers: false,
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
        validate_segment_footers: false,
    };

    run_query_smoke(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert!(markdown.contains("## Readback Verification"));
    assert!(markdown.contains("| Checked Queries | 9 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
}

#[test]
fn run_query_smoke_uses_manifest_published_segments_when_present() {
    let tempdir = segment_store_with_two_windows();
    let readers = sorted_segment_readers(tempdir.path());
    assert_eq!(readers.len(), 2);
    publish_manifest_segments(tempdir.path(), &[&readers[0]]);
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_smoke.md"),
        start_ms: 0,
        end_ms: 20_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        validate_segment_footers: false,
    };

    let report = run_query_smoke(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert!(markdown.contains("| Checked Queries | 3 |"));
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
        validate_segment_footers: false,
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
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert_eq!(report.results.len(), 2);
    assert_eq!(report.results[0].query, "cpu.usage");
    assert_eq!(report.results[0].result_samples, 2);
    assert_eq!(report.results[1].query, "request.duration_count");
    assert_eq!(report.results[1].result_samples, 1);
    assert!(report.session_stats.segment_context_opens > 0);
    assert!(report.session_profile.segment_context_open > Duration::ZERO);
    assert!(report.session_profile.symbols_file_bytes > 0);
    assert!(report.session_profile.series_file_bytes > 0);
    assert!(report.session_profile.chunk_index_file_bytes > 0);
    assert!(report.results[0].session_stats_delta.segment_context_opens > 0);
    assert!(report.results[0].session_profile_delta.segment_context_open > Duration::ZERO);
    assert_eq!(
        report.results[0].session_profile_delta.exact_postings_read,
        Duration::ZERO
    );
    assert!(report.results[0].session_profile_delta.series_entry_read > Duration::ZERO);
    assert!(report.results[0].session_profile_delta.series_entry_bytes > 0);
    assert!(
        report.results[0]
            .session_profile_delta
            .chunk_index_range_read
            > Duration::ZERO
    );
    assert!(report.results[0].session_profile_delta.chunk_read > Duration::ZERO);
    assert!(report.results[1].session_stats_delta.segment_context_opens > 0);
    assert!(report.results[1].session_profile_delta.segment_context_open > Duration::ZERO);
    assert!(report.results[1].session_profile_delta.series_open > Duration::ZERO);
    assert_eq!(
        report.results[1].session_profile_delta.exact_postings_read,
        Duration::ZERO
    );
    assert!(report.results[1].session_profile_delta.chunk_read > Duration::ZERO);

    assert!(markdown.contains("# Chronoxide Sealed Query Benchmark"));
    assert!(markdown.contains("## Query Limits"));
    assert!(markdown.contains("| query_max_projected_series | 2000000 |"));
    assert!(markdown.contains("| regex_max_expanded_values | 100000 |"));
    assert!(markdown.contains("## Query Results"));
    assert!(markdown.contains("## Session File Opens"));
    assert!(markdown.contains("## Session Opened File Sizes"));
    assert!(markdown.contains("## Session Logical Read Bytes"));
    assert!(markdown.contains("## Query Result Read Profiles"));
    assert!(markdown.contains("| Segment Context Open |"));
    assert!(markdown.contains("| symbols.bin |"));
    assert!(markdown.contains("- Benchmark Repeats: 1"));
    assert!(markdown.contains("## Cold/Warm Query Summary"));
    assert!(markdown.contains("context_open_delta"));
    assert!(markdown.contains("postings_read_delta"));
    assert!(markdown.contains("metric_series_ranges_read_delta"));
    assert!(markdown.contains("chunk_read_delta"));
    assert!(markdown.contains("routing_opened_file_size_bytes_delta"));
    assert!(markdown.contains("series_opened_file_size_bytes_delta"));
    assert!(markdown.contains("metric_series_ranges_bytes_delta"));
    assert!(markdown.contains("series_entry_bytes_delta"));
    assert!(markdown.contains("| Metric Series Ranges |"));
    assert!(!markdown.contains("routing_file_bytes_delta"));
    assert!(markdown.contains("| Queries | 2 |"));
    assert!(markdown.contains("| Query Runs | 2 |"));
    assert!(markdown.contains("Segments Considered"));
    assert!(markdown.contains("context_opens_delta"));
    assert!(markdown.contains("chunks_opens_delta"));
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
fn run_query_benchmark_can_prewarm_contexts_before_measured_queries() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: 10_000,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: true,
        prefetch_query_data: false,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert_eq!(report.results.len(), 1);
    assert!(report.query_context_prewarm_stats_delta.index_routing_opens > 0);
    assert!(
        report
            .query_context_prewarm_profile_delta
            .index_routing_open
            > Duration::ZERO
    );
    assert!(
        report
            .query_context_prewarm_stats_delta
            .segment_context_opens
            > 0
    );
    assert!(
        report
            .query_context_prewarm_profile_delta
            .segment_context_open
            > Duration::ZERO
    );
    assert!(report.query_context_prewarm_stats_delta.series_bin_opens > 0);
    assert!(report.query_context_prewarm_profile_delta.series_open > Duration::ZERO);
    assert!(
        report
            .query_context_prewarm_stats_delta
            .chunk_index_bin_opens
            > 0
    );
    assert!(report.query_context_prewarm_profile_delta.chunk_index_open > Duration::ZERO);
    assert!(report.query_context_prewarm_stats_delta.chunks_bin_opens > 0);
    assert!(report.query_context_prewarm_profile_delta.chunks_open > Duration::ZERO);
    assert_eq!(
        report.results[0].session_stats_delta,
        SegmentStoreQuerySessionStats::default()
    );
    assert_eq!(
        report.results[0].session_profile_delta.segment_context_open,
        Duration::ZERO
    );
    assert_eq!(
        report.results[0].session_profile_delta.series_open,
        Duration::ZERO
    );
    assert_eq!(
        report.results[0].session_profile_delta.exact_postings_read,
        Duration::ZERO
    );
    assert!(report.results[0].session_profile_delta.chunk_read > Duration::ZERO);
    assert!(markdown.contains("- Prewarm Query Contexts: true"));
    assert!(markdown.contains("| Query Context Prewarm |"));
    assert!(markdown.contains("## Query Context Prewarm File Opens"));
    assert!(markdown.contains("## Query Context Prewarm Read Profile"));
}

#[test]
fn run_query_benchmark_can_prefetch_data_before_measured_queries() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: 10_000,
        queries: vec![
            r#"request.duration_count{route="/typed"}"#.to_string(),
            r#"request.duration_count{route="/typed"}"#.to_string(),
        ],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: true,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert_eq!(report.results.len(), 2);
    assert!(report.query_data_prefetch_stats.query_stats.chunk_reads > 0);
    assert!(report.query_data_prefetch_stats.query_stats.bytes_read > 0);
    assert!(
        report.query_data_prefetch_stats.query_stats.chunk_reads
            >= report.results[0].stats.chunk_reads
    );
    assert!(
        report.query_data_prefetch_stats.query_stats.bytes_read
            >= report.results[0].stats.bytes_read
    );
    assert!(report.query_data_prefetch_stats.series_entries_read > 0);
    assert!(report.query_data_prefetch_stats.chunk_index_reads > 0);
    assert!(
        report
            .query_data_prefetch_session_stats_delta
            .segment_context_opens
            > 0
    );
    assert_eq!(
        report.results[0].session_stats_delta,
        SegmentStoreQuerySessionStats::default()
    );
    assert_eq!(
        report.results[1].session_stats_delta,
        SegmentStoreQuerySessionStats::default()
    );
    assert!(markdown.contains("- Prefetch Query Data: true"));
    assert!(markdown.contains("| Query Data Prefetch |"));
    assert!(markdown.contains("## Query Data Prefetch"));
}

#[test]
fn run_query_benchmark_uses_manifest_published_segments_when_present() {
    let tempdir = segment_store_with_two_windows();
    let readers = sorted_segment_readers(tempdir.path());
    assert_eq!(readers.len(), 2);
    publish_manifest_segments(tempdir.path(), &[&readers[0]]);
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: 20_000,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_samples, 1);
    assert_eq!(report.results[0].result_series, 1);
}

#[test]
fn run_query_benchmark_defaults_omitted_end_for_instant_vector_expressions() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: u64::MAX,
        queries: vec!["cpu.usage * 2".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_series, 1);
    assert_eq!(report.results[0].result_samples, 1);
}

#[test]
fn run_query_benchmark_defaults_omitted_end_for_aggregations() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: u64::MAX,
        queries: vec!["sum(cpu.usage)".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_series, 1);
    assert_eq!(report.results[0].result_samples, 1);
}

#[test]
fn run_query_benchmark_uses_max_sample_time_for_omitted_instant_end() {
    let tempdir = segment_store_with_sparse_final_window();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: u64::MAX,
        queries: vec!["sparse.cpu * 2".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_series, 1);
    assert_eq!(report.results[0].result_samples, 1);
}

#[test]
fn effective_query_end_ms_only_changes_instant_vector_expressions() {
    let range = Some((1_000, 10_000));

    assert_eq!(
        effective_query_end_ms("cpu.usage", u64::MAX, range),
        u64::MAX
    );
    assert_eq!(effective_query_end_ms("1 + 2", u64::MAX, range), u64::MAX);
    assert_eq!(
        effective_query_end_ms("rate(cpu.usage[5m])", u64::MAX, range),
        10_000
    );
    assert_eq!(
        effective_query_end_ms("sum(cpu.usage)", u64::MAX, range),
        10_000
    );
    assert_eq!(
        effective_query_end_ms("sort(cpu.usage)", u64::MAX, range),
        10_000
    );
    assert_eq!(
        effective_query_end_ms("histogram_sum(cpu.usage)", u64::MAX, range),
        10_000
    );
    assert_eq!(
        effective_query_end_ms("absent(cpu.missing)", u64::MAX, range),
        10_000
    );
    assert_eq!(
        effective_query_end_ms("absent_over_time(cpu.missing[5m])", u64::MAX, range),
        10_000
    );
    assert_eq!(
        effective_query_end_ms("cpu.usage * 2", u64::MAX, range),
        10_000
    );
    assert_eq!(
        effective_query_end_ms("cpu.usage * 2", 20_000, range),
        20_000
    );
}

#[test]
fn explicit_query_args_default_to_production_query_limits_and_allow_overrides() {
    let defaults = Args::parse_from(["chronoxide-query", "--query", "cpu.usage"]);

    assert_eq!(
        defaults.query_limits.to_query_limits(),
        QueryLimits::production_default()
    );

    let overridden = Args::parse_from([
        "chronoxide-query",
        "--query",
        "cpu.usage",
        "--query-max-series-matched",
        "7",
        "--query-max-projected-series",
        "11",
        "--query-max-chunks-read",
        "13",
        "--query-max-bytes-read",
        "17",
        "--query-max-samples",
        "19",
        "--regex-max-expanded-values",
        "23",
    ]);

    assert_eq!(
        overridden.query_limits.to_query_limits(),
        QueryLimits {
            max_matched_series: Some(7),
            max_projected_series: Some(11),
            max_chunk_reads: Some(13),
            max_bytes_read: Some(17),
            max_samples_decoded: Some(19),
            max_regex_values_examined: Some(23),
        }
    );
}

#[test]
fn explicit_query_args_default_to_repeated_cold_warm_benchmark_and_allow_override() {
    let defaults = Args::parse_from(["chronoxide-query", "--query", "cpu.usage"]);
    assert_eq!(defaults.benchmark_repeats, 3);

    let overridden = Args::parse_from([
        "chronoxide-query",
        "--query",
        "cpu.usage",
        "--benchmark-repeats",
        "5",
    ]);
    assert_eq!(overridden.benchmark_repeats, 5);
}

#[test]
fn run_query_benchmark_reports_session_cold_and_warm_runs_without_smoke_scans() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: 10_000,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 3,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert_eq!(report.results.len(), 3);
    assert_eq!(report.results[0].run_kind, QueryBenchmarkRunKind::Cold);
    assert_eq!(report.results[0].run_index, 0);
    assert_eq!(report.results[1].run_kind, QueryBenchmarkRunKind::Warm);
    assert_eq!(report.results[1].run_index, 1);
    assert_eq!(report.results[2].run_kind, QueryBenchmarkRunKind::Warm);
    assert_eq!(report.results[2].run_index, 2);
    assert!(report.results[0].session_profile_delta.segment_context_open > Duration::ZERO);
    assert_eq!(
        report.results[1].session_profile_delta.segment_context_open,
        Duration::ZERO
    );
    assert_eq!(
        report.results[2].session_profile_delta.segment_context_open,
        Duration::ZERO
    );

    assert!(markdown.contains("- Benchmark Repeats: 3"));
    assert!(markdown.contains("## Cold/Warm Query Summary"));
    assert!(markdown.contains("| `cpu.usage` | 1 | 2 |"));
    assert!(markdown.contains("| `cpu.usage` | Cold | 0 |"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 1 |"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 2 |"));
    assert!(!markdown.contains("| Smoke Verify |"));
    assert!(!markdown.contains("Collect Expected Readbacks"));
    assert!(!markdown.contains("## Readback Verification"));
}

#[test]
fn segment_footer_validation_is_opt_in_for_query_open() {
    let defaults = Args::parse_from(["chronoxide-query"]);
    assert!(!defaults.validate_segment_footers);

    let validated = Args::parse_from(["chronoxide-query", "--validate-segment-footers"]);
    assert!(validated.validate_segment_footers);
}

#[test]
fn open_segment_store_validates_manifest_segment_footers_only_when_requested() {
    let tempdir = segment_store_with_two_windows();
    let readers = sorted_segment_readers(tempdir.path());
    assert_eq!(readers.len(), 2);
    publish_manifest_segments(tempdir.path(), &[&readers[0]]);

    let segment_dir = tempdir.path().join(readers[0].meta().segment_id.clone());
    let symbols_path = segment_dir.join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    symbols[0] ^= 0xff;
    fs::write(symbols_path, symbols).unwrap();

    let _store = open_segment_store(tempdir.path(), false)
        .expect("default query open should skip footer checksum validation");

    let err = match open_segment_store(tempdir.path(), true) {
        Ok(_) => panic!("validated query open should catch footer checksum mismatch"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn run_query_benchmark_enforces_configured_query_limits() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        start_ms: 0,
        end_ms: 10_000,
        queries: vec![r#"request.duration_bucket"#.to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        limits: QueryLimits {
            max_projected_series: Some(1),
            ..QueryLimits::production_default()
        },
        validate_segment_footers: false,
    };

    let err = run_query_benchmark(&config).unwrap_err();

    assert!(err.to_string().contains("projected_series"));
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
        validate_segment_footers: false,
    };

    let required_kinds = [true, false, false, false, false];
    let expected = collect_expected_readbacks(&config, &required_kinds).unwrap();

    assert_eq!(expected.len(), 5);
    assert_eq!(expected[0].start_ms, 0);
    assert_eq!(expected[0].end_ms, 999);
    assert_eq!(expected[0].samples.len(), 1_000);
    assert_eq!(expected[1].query, format!("({}) * 2", expected[0].query));
    assert_eq!(expected[1].samples, vec![(999, 1_998.0)]);
    assert_eq!(expected[2].query, format!("sum({})", expected[0].query));
    assert_eq!(expected[2].samples, vec![(999, 999.0)]);
    assert_eq!(
        expected[3].query,
        format!("rate({}[1000ms])", expected[0].query)
    );
    assert_eq!(expected[3].samples, vec![(999, 999.0)]);
    assert_eq!(
        expected[4].query,
        format!("increase({}[1000ms])", expected[0].query)
    );
    assert_eq!(expected[4].samples, vec![(999, 999.0)]);
}

#[test]
fn collect_expected_readbacks_adds_histogram_counter_range_queries() {
    let tempdir = segment_store_with_histogram_counter_series();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        validate_segment_footers: false,
    };

    let required_kinds = [false, false, true, false, false];
    let expected = collect_expected_readbacks(&config, &required_kinds).unwrap();
    let labels = [
        (
            METRIC_NAME_LABEL.to_string(),
            "request_duration_range".to_string(),
        ),
        ("route".to_string(), "/hist-range".to_string()),
    ];
    let count_selector = promql_exact_selector("request_duration_range_count", &labels, None);
    let bucket_selector = promql_exact_selector(
        "request_duration_range_bucket",
        &labels,
        Some(("le", "+Inf")),
    );
    let count_rate_query = format!("rate({count_selector}[3001ms])");
    let count_increase_query = format!("increase({count_selector}[3001ms])");
    let bucket_rate_query = format!("rate({bucket_selector}[3001ms])");

    let count_rate = expected
        .iter()
        .find(|readback| readback.query == count_rate_query)
        .expect("histogram count rate readback");
    assert_eq!(count_rate.start_ms, 4_000);
    assert_eq!(count_rate.end_ms, 4_000);
    assert_eq!(count_rate.samples.len(), 1);
    assert_eq!(count_rate.samples[0].0, 4_000);
    assert!((count_rate.samples[0].1 - 2.0).abs() < 1e-12);

    assert!(
        expected
            .iter()
            .any(|readback| readback.query == count_increase_query),
        "histogram count increase readback missing"
    );
    assert!(
        expected
            .iter()
            .any(|readback| readback.query == bucket_rate_query),
        "histogram +Inf bucket rate readback missing"
    );
}

#[test]
fn verify_readbacks_skips_histogram_range_when_exact_projection_is_not_isolated() {
    let tempdir = segment_store_with_overlapping_histogram_counter_segments();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let report = store.smoke_verify(0, 10_000, 2).unwrap();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 2,
        verify_readbacks: true,
        validate_segment_footers: false,
    };

    let (verification, diagnostics) = verify_readbacks(&config, &report).unwrap();

    assert_eq!(verification.mismatches, Vec::<QueryReadbackMismatch>::new());
    assert!(
        diagnostics.executed_queries < diagnostics.expected_queries,
        "overlapped histogram range readbacks should be skipped"
    );
    assert_eq!(diagnostics.skipped_queries, 8);
    assert_eq!(diagnostics.isolation_check_skips, 8);
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

fn segment_store_with_histogram_counter_series() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let not_reset = TypedSampleMetadata {
        reset_hint: chronoxide_core::storage::head::CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
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
                ),
                (
                    4_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(28.0),
                        min: Some(1.0),
                        max: Some(6.0),
                        metadata: not_reset,
                        explicit_bounds: vec![1.0, 5.0],
                        bucket_counts: vec![3, 4, 3],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_duration_range");
                visit("route", "/hist-range");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_overlapping_histogram_counter_segments() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let labels = |visit: &mut dyn FnMut(&str, &str)| {
        visit(METRIC_NAME_LABEL, "overlap_duration");
        visit("route", "/overlap");
    };

    let mut broad_writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(1),
    )
    .unwrap();
    broad_writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(10.0),
                        min: Some(1.0),
                        max: Some(4.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 3],
                    },
                ),
                (
                    4_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(28.0),
                        min: Some(1.0),
                        max: Some(6.0),
                        metadata: TypedSampleMetadata {
                            reset_hint:
                                chronoxide_core::storage::head::CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![3, 7],
                    },
                ),
            ],
            labels,
        )
        .unwrap();
    broad_writer.flush().unwrap();

    let mut overlapping_writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(2),
    )
    .unwrap();
    overlapping_writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    2_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(60.0),
                        min: Some(1.0),
                        max: Some(8.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 18],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 40,
                        sum: Some(120.0),
                        min: Some(1.0),
                        max: Some(9.0),
                        metadata: TypedSampleMetadata {
                            reset_hint:
                                chronoxide_core::storage::head::CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![4, 36],
                    },
                ),
            ],
            labels,
        )
        .unwrap();
    overlapping_writer.flush().unwrap();

    tempdir
}

fn segment_store_with_sparse_final_window() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(600));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.0)], |visit| {
            visit(METRIC_NAME_LABEL, "sparse.cpu");
            visit("instance", "host-a");
        })
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_two_windows() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), "published".to_string()),
            ],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(2),
            &[
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), "orphan".to_string()),
            ],
            &[(15_000, 2.0)],
        )
        .unwrap();
    writer.flush().unwrap();
    tempdir
}

fn sorted_segment_readers(segments_dir: &Path) -> Vec<SegmentReader> {
    let mut readers = fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| SegmentReader::open(entry.path()).unwrap())
        .collect::<Vec<_>>();
    readers.sort_by(|left, right| {
        left.meta()
            .start_ms
            .cmp(&right.meta().start_ms)
            .then_with(|| left.meta().end_ms.cmp(&right.meta().end_ms))
            .then_with(|| left.meta().segment_id.cmp(&right.meta().segment_id))
    });
    readers
}

fn publish_manifest_segments(segments_dir: &Path, readers: &[&SegmentReader]) {
    let manifest_dir = segments_dir.join("manifest");
    let mut writer = ManifestWriter::create(&manifest_dir, 99).unwrap();
    for reader in readers {
        let meta = reader.meta();
        writer
            .append(&ManifestRecord::SegmentSealed(
                ManifestSegment::new(meta.segment_id.clone(), meta.start_ms, meta.end_ms, None)
                    .unwrap(),
            ))
            .unwrap();
    }
    writer.sync_all().unwrap();
    write_current(&manifest_dir, writer.file_name()).unwrap();
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
