use std::fs;
use std::io::ErrorKind;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OtlpAggregationTemporality, SummaryQuantileValue, SummaryValue, TypedSampleMetadata,
};
use chronoxide_core::storage::index::{SegmentIndexReadCount, SegmentIndexReadStats};
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, write_current,
};
use chronoxide_core::storage::segment::{
    SegmentReader, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
};

use super::*;

fn sample_index_read_stats(multiplier: u64) -> SegmentIndexReadStats {
    let count = |value| SegmentIndexReadCount {
        calls: value * multiplier,
        bytes: value * multiplier * 10,
    };
    SegmentIndexReadStats {
        root: count(1),
        routing: count(2),
        exact_directory: count(3),
        exact_page: count(4),
        auxiliary_directory: count(5),
        payload: count(6),
    }
}

fn benchmark_config_for_outputs(
    segments_dir: PathBuf,
    output: PathBuf,
    raw_output: PathBuf,
) -> QueryBenchmarkConfig {
    QueryBenchmarkConfig {
        segments_dir,
        output,
        raw_output: Some(raw_output),
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    }
}

fn assert_no_benchmark_temp_files(directory: &Path) {
    if !directory.exists() {
        return;
    }
    for entry in fs::read_dir(directory).unwrap() {
        let name = entry.unwrap().file_name();
        assert!(
            !name.to_string_lossy().contains(".chronoxide-tmp-"),
            "temporary benchmark output was not cleaned up: {name:?}"
        );
    }
}

#[test]
fn render_index_positional_read_table_reports_categories_and_totals() {
    let mut markdown = String::new();

    render_index_positional_read_table(
        &mut markdown,
        "Test Index Positional Reads",
        sample_index_read_stats(1),
    );

    assert!(markdown.contains("## Test Index Positional Reads"));
    assert!(markdown.contains("successful positional-read requests"));
    assert!(markdown.contains("not physical syscalls"));
    assert!(markdown.contains("| Root | 1 | 10 |"));
    assert!(markdown.contains("| Routing | 2 | 20 |"));
    assert!(markdown.contains("| Exact Directory | 3 | 30 |"));
    assert!(markdown.contains("| Exact Page | 4 | 40 |"));
    assert!(markdown.contains("| Auxiliary Directory | 5 | 50 |"));
    assert!(markdown.contains("| Payload | 6 | 60 |"));
    assert!(markdown.contains("| Total | 21 | 210 |"));
}

#[test]
fn render_query_result_index_positional_reads_reports_each_run_by_category() {
    let tempdir = segment_store_with_float_and_histogram();
    let results = SegmentStoreReader::open(tempdir.path())
        .unwrap()
        .query_promql("cpu.usage", 0, 10_000)
        .unwrap();
    let semantic_fingerprint = chronoxide_core::storage::segment::QueryExecution {
        results,
        stats: QueryStats::default(),
    }
    .semantic_fingerprint_sha256();
    let results = vec![QueryBenchmarkResult {
        query: "cpu.usage".to_string(),
        run_kind: QueryBenchmarkRunKind::Warm,
        run_index: 2,
        query_session_open: Duration::ZERO,
        duration: Duration::ZERO,
        effective_start_ms: 0,
        effective_end_ms: 0,
        step_ms: None,
        semantic_fingerprint,
        result_series: 0,
        result_samples: 0,
        stats: QueryStats::default(),
        session_stats_delta: SegmentStoreQuerySessionStats::default(),
        session_profile_delta: SegmentStoreQueryProfile {
            index_read_stats: sample_index_read_stats(1),
            ..SegmentStoreQueryProfile::default()
        },
        range_scalar_cache: None,
    }];
    let mut markdown = String::new();

    render_query_result_index_positional_reads(&mut markdown, &results);

    assert!(markdown.contains("## Query Result Index Positional Reads"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 2 | Root | 1 | 10 |"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 2 | Exact Page | 4 | 40 |"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 2 | Total | 21 | 210 |"));
}

#[test]
fn add_session_profile_accumulates_index_read_stats() {
    let mut total = SegmentStoreQueryProfile {
        index_read_stats: sample_index_read_stats(2),
        ..SegmentStoreQueryProfile::default()
    };

    add_session_profile(
        &mut total,
        SegmentStoreQueryProfile {
            index_read_stats: sample_index_read_stats(3),
            ..SegmentStoreQueryProfile::default()
        },
    );

    assert_eq!(total.index_read_stats, sample_index_read_stats(5));
}

#[test]
fn payload_read_amplification_formats_ratio_and_empty_payload() {
    assert_eq!(format_payload_read_amplification(0, 0), "—");
    assert_eq!(format_payload_read_amplification(150, 100), "1.500x");
}

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
        exponential_histogram_bucket_boundaries: Vec::new(),
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
        exponential_histogram_bucket_boundaries: Vec::new(),
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
        exponential_histogram_bucket_boundaries: Vec::new(),
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
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    run_query_smoke(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert!(markdown.contains("## Readback Verification"));
    assert!(markdown.contains("| Checked Queries | 9 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
}

#[test]
fn run_query_smoke_verifies_int64_readbacks_against_decoded_chunks() {
    let tempdir = segment_store_with_int64();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    run_query_smoke(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert!(markdown.contains("| Int64 | 1 |"));
    assert!(markdown.contains("| Checked Queries | 5 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
}

#[test]
fn run_query_smoke_verifies_summary_readbacks_against_decoded_chunks() {
    let tempdir = segment_store_with_summary();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    run_query_smoke(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert!(markdown.contains("| Summary | 1 |"));
    assert!(markdown.contains("| Checked Queries | 3 |"));
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
        exponential_histogram_bucket_boundaries: Vec::new(),
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
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    run_query_smoke(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert!(markdown.contains("| Checked Queries | 4 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
}

#[test]
fn run_query_smoke_verifies_configured_exponential_histogram_bucket_readbacks() {
    let tempdir = segment_store_with_exponential_histogram();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: vec![2.0],
        validate_segment_footers: false,
    };

    run_query_smoke(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert!(markdown.contains("| Checked Queries | 4 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
}

#[test]
fn run_query_smoke_verifies_delta_exponential_histogram_readbacks_after_projection() {
    let tempdir = segment_store_with_delta_exponential_histogram();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: vec![2.0],
        validate_segment_footers: false,
    };
    let required_kinds = [false, false, false, true, false];
    let expected = collect_expected_readbacks(&config, &required_kinds).unwrap();
    let labels = [
        (
            METRIC_NAME_LABEL.to_string(),
            "delta_http_request_size".to_string(),
        ),
        ("route".to_string(), "/delta-exphist".to_string()),
    ];
    let bucket_selector =
        promql_exact_selector("delta_http_request_size_bucket", &labels, Some(("le", "2")));

    let finite_bucket = expected
        .iter()
        .find(|readback| readback.query == bucket_selector)
        .unwrap_or_else(|| {
            panic!(
                "finite delta exponential histogram bucket readback missing from {:?}",
                expected
                    .iter()
                    .map(|readback| readback.query.as_str())
                    .collect::<Vec<_>>()
            )
        });
    assert_eq!(finite_bucket.samples, vec![(1_000, 1.0), (2_000, 1.0)]);

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
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec![
            "cpu.usage".to_string(),
            r#"request.duration_count"#.to_string(),
        ],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
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
    let payload_used_bytes = report.results[0].session_profile_delta.chunk_payload_bytes;
    let payload_read_bytes = report.results[0]
        .session_profile_delta
        .chunk_payload_physical_bytes;
    assert!(payload_used_bytes > 0);
    assert!(payload_read_bytes >= payload_used_bytes);
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
    assert!(markdown.contains("| Payload Used Bytes |"));
    assert!(markdown.contains("| Payload Read Bytes |"));
    assert!(markdown.contains("| Payload Read / Used |"));
    assert!(markdown.contains("payload_used_bytes"));
    assert!(markdown.contains("payload_read_bytes"));
    assert!(markdown.contains("payload_read_over_used"));
    assert!(markdown.contains("do not measure storage-device traffic"));
    assert!(markdown.contains("## Session File Opens"));
    assert!(markdown.contains("## Session Opened File Sizes"));
    assert!(markdown.contains("## Session Logical Read Bytes"));
    assert!(markdown.contains("## Session Index Positional Reads"));
    assert!(markdown.contains("## Query Result Read Profiles"));
    assert!(markdown.contains("## Query Result Index Positional Reads"));
    assert!(markdown.contains("Successful Positional-Read Requests"));
    assert!(markdown.contains("Requested Bytes"));
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
fn run_query_benchmark_executes_inclusive_range_and_reports_schedule() {
    let tempdir = segment_store_with_float_and_histogram();
    let raw_output = tempdir.path().join("query_range_benchmark.json");
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_range_benchmark.md"),
        raw_output: Some(raw_output.clone()),
        start_ms: 1_000,
        end_ms: 5_000,
        mode: QueryBenchmarkMode::Range { step_ms: 2_000 },
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["time() + 1".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(&fs::read(raw_output).unwrap()).unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_series, 1);
    assert_eq!(report.results[0].result_samples, 3);
    let cache = report.results[0].range_scalar_cache.unwrap();
    assert_eq!(
        cache.summary.configured_budget_bytes,
        chronoxide_core::storage::segment::DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES
    );
    assert_eq!(cache.summary.retained_charge_after_finalize, 0);
    assert_eq!(cache.process_governor.current_leased_bytes, 0);
    assert!(markdown.contains("- Time Range: 1000..5000"));
    assert!(markdown.contains("- Evaluation Mode: query_range"));
    assert!(markdown.contains("- Chunk Read Mode: pread"));
    assert!(markdown.contains("- Chunk Read Queue Depth: 128"));
    assert!(markdown.contains("- Experimental Cross-Segment Chunk Reads: false"));
    assert!(markdown.contains("- Range Step: 2000 ms"));
    assert!(markdown.contains("- Range Scalar Cache Max Bytes: 16777216"));
    assert!(markdown.contains("- Scheduled Evaluations Per Run: 3"));
    assert!(markdown.contains("Session-local cold"));
    assert!(markdown.contains("shared store caches"));
    assert!(markdown.contains("does not flush or bypass the operating-system page cache"));
    assert!(markdown.contains("| Payload Used Bytes | 0 |"));
    assert!(markdown.contains("| Payload Read Bytes | 0 |"));
    assert!(markdown.contains("| Payload Read / Used | — |"));
    assert_eq!(
        raw["configuration"]["range_scalar_cache_max_bytes"],
        chronoxide_core::storage::segment::DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES
    );
    assert_eq!(
        raw["runs"][0]["range_scalar_cache"]["configured_budget_bytes"],
        chronoxide_core::storage::segment::DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES
    );

    let mut query_result_lines = markdown
        .lines()
        .skip_while(|line| *line != "## Query Results")
        .filter(|line| line.starts_with('|'));
    let header = query_result_lines.next().unwrap();
    let separator = query_result_lines.next().unwrap();
    let result = query_result_lines.next().unwrap();
    assert_eq!(header.matches('|').count(), separator.matches('|').count());
    assert_eq!(header.matches('|').count(), result.matches('|').count());
    assert!(result.ends_with("| 0 | 0 | — |"));
}

#[test]
fn raw_benchmark_writes_reproducible_corpus_fingerprints_and_ordered_runs() {
    let tempdir = segment_store_with_float_and_histogram();
    let raw_output = tempdir.path().join("nested/raw/query_benchmark.json");
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: Some(raw_output.clone()),
        start_ms: 1_000,
        end_ms: 5_000,
        mode: QueryBenchmarkMode::Range { step_ms: 2_000 },
        range_scalar_cache_max_bytes: Some(0),
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["time()".to_string(), "time() + 1".to_string()],
        benchmark_repeats: 2,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: vec![2.0, 4.0],
        limits: QueryLimits {
            max_matched_series: Some(11),
            max_projected_series: Some(12),
            max_chunk_reads: Some(13),
            max_bytes_read: Some(14),
            max_samples_decoded: Some(15),
            max_regex_values_examined: None,
        },
        validate_segment_footers: true,
    };

    let expected_corpus = open_segment_store(tempdir.path(), false, query_projection_config(&[]))
        .unwrap()
        .corpus_fingerprint_sha256()
        .unwrap();
    let report = run_query_benchmark(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();
    let raw_text = fs::read_to_string(&raw_output).unwrap();
    let raw: serde_json::Value = serde_json::from_str(&raw_text).unwrap();

    assert_eq!(report.corpus_fingerprint, expected_corpus);
    assert!(raw_text.ends_with('\n'));
    assert_eq!(raw["schema"], "chronoxide.query-benchmark.raw/v2");
    assert!(raw.get("generated_at").is_none());
    assert_eq!(raw["configuration"]["chunk_read_mode"], "pread");
    assert_eq!(raw["configuration"]["chunk_read_queue_depth"], 128);
    assert_eq!(
        raw["configuration"]["experimental_cross_segment_chunk_reads"],
        false
    );
    assert_eq!(
        raw["corpus_fingerprint_sha256"],
        report.corpus_fingerprint.to_hex()
    );
    assert_eq!(
        raw["corpus_fingerprint_duration_ns"].as_u64().unwrap(),
        u64::try_from(report.corpus_fingerprint_duration.as_nanos()).unwrap()
    );
    assert_eq!(
        raw["configuration"]["segments_dir"],
        config.segments_dir.to_string_lossy().as_ref()
    );
    assert_eq!(raw["configuration"]["start_ms"], 1_000);
    assert_eq!(raw["configuration"]["end_ms"], 5_000);
    assert_eq!(raw["configuration"]["mode"], "query_range");
    assert_eq!(raw["configuration"]["step_ms"], 2_000);
    assert_eq!(raw["configuration"]["range_scalar_cache_max_bytes"], 0);
    assert_eq!(raw["configuration"]["benchmark_repeats"], 2);
    assert_eq!(
        raw["configuration"]["queries"],
        serde_json::json!(["time()", "time() + 1"])
    );
    assert_eq!(raw["configuration"]["prewarm_query_contexts"], false);
    assert_eq!(raw["configuration"]["prefetch_query_data"], false);
    assert_eq!(raw["configuration"]["validate_segment_footers"], true);
    assert_eq!(
        raw["configuration"]["exponential_histogram_bucket_boundaries"],
        serde_json::json!([2.0, 4.0])
    );
    assert_eq!(raw["limits"]["max_matched_series"], 11);
    assert_eq!(raw["limits"]["max_projected_series"], 12);
    assert_eq!(raw["limits"]["max_chunk_reads"], 13);
    assert_eq!(raw["limits"]["max_bytes_read"], 14);
    assert_eq!(raw["limits"]["max_samples_decoded"], 15);
    assert!(raw["limits"]["max_regex_values_examined"].is_null());

    let runs = raw["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 4);
    assert_eq!(
        runs.iter()
            .map(|run| run["query"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["time()", "time()", "time() + 1", "time() + 1"]
    );
    assert_eq!(
        runs.iter()
            .map(|run| run["run_kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["cold", "warm", "cold", "warm"]
    );
    assert_eq!(
        runs.iter()
            .map(|run| run["run_index"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 0, 1]
    );
    for (run, result) in runs.iter().zip(&report.results) {
        assert!(run["duration_ns"].is_u64());
        assert_eq!(run["effective_start_ms"], 1_000);
        assert_eq!(run["effective_end_ms"], 5_000);
        assert_eq!(run["step_ms"], 2_000);
        assert_eq!(
            run["duration_ns"].as_u64().unwrap(),
            u64::try_from(result.duration.as_nanos()).unwrap()
        );
        assert_eq!(
            run["semantic_fingerprint_sha256"],
            result.semantic_fingerprint.to_hex()
        );
        assert_eq!(run["result_series"], result.result_series);
        assert_eq!(run["result_samples"], result.result_samples);
        let cache = result.range_scalar_cache.unwrap();
        let raw_cache = &run["range_scalar_cache"];
        assert_eq!(
            raw_cache["configured_budget_bytes"],
            cache.summary.configured_budget_bytes
        );
        assert_eq!(
            raw_cache["retained_charge_after_finalize"],
            cache.summary.retained_charge_after_finalize
        );
        assert_eq!(
            raw_cache["process_governor_current_leased_bytes"],
            cache.process_governor.current_leased_bytes
        );
        assert!(markdown.contains(&result.semantic_fingerprint.to_hex()));
    }
    assert!(markdown.contains("Segment Corpus Fingerprint SHA-256"));
    assert!(markdown.contains("Segment Corpus Fingerprint Duration"));
    assert!(markdown.contains(&report.corpus_fingerprint.to_hex()));
    assert!(markdown.contains("Warm Median"));
    assert!(markdown.contains("## Range Scalar Cache Runs"));
}

#[test]
fn raw_benchmark_stats_serialization_covers_every_query_stats_field() {
    let value = serde_json::to_value(RawQueryStatsV1::from(QueryStats {
        segments_considered: 1,
        segments_skipped_by_time: 2,
        segments_skipped_by_missing_equality: 3,
        segments_skipped_by_matcher_time_range: 4,
        segments_queried: 5,
        matched_series: 6,
        projected_series: 7,
        chunk_reads: 8,
        bytes_read: 9,
        samples_decoded: 10,
        typed_scalar_chunks_decoded: 11,
        typed_full_chunks_decoded: 12,
        regex_values_examined: 13,
        index_postings_reads: 14,
        index_postings_bytes_read: 15,
    }))
    .unwrap();
    let object = value.as_object().unwrap();

    assert_eq!(object.len(), 15);
    for (key, expected) in [
        ("segments_considered", 1),
        ("segments_skipped_by_time", 2),
        ("segments_skipped_by_missing_equality", 3),
        ("segments_skipped_by_matcher_time_range", 4),
        ("segments_queried", 5),
        ("matched_series", 6),
        ("projected_series", 7),
        ("chunk_reads", 8),
        ("bytes_read", 9),
        ("samples_decoded", 10),
        ("typed_scalar_chunks_decoded", 11),
        ("typed_full_chunks_decoded", 12),
        ("regex_values_examined", 13),
        ("index_postings_reads", 14),
        ("index_postings_bytes_read", 15),
    ] {
        assert_eq!(object[key], expected, "wrong raw value for {key}");
    }
}

#[test]
fn range_scalar_cache_raw_and_markdown_report_every_summary_and_governor_field() {
    let cache = QueryBenchmarkRangeScalarCacheReport {
        summary: chronoxide_core::storage::segment::RangeScalarCacheSummary {
            configured_budget_bytes: 1,
            governor_lease_bytes: 2,
            governor_refused: true,
            allocation_refused: false,
            layout_overflow: true,
            entry_arena_charge_bytes: 3,
            sample_arena_charge_bytes: 4,
            hits: 5,
            misses: 6,
            admitted_entries: 7,
            streaming_budget_bypasses: 8,
            unsupported_bypasses: 9,
            logical_hit_bytes: 10,
            logical_miss_or_bypass_bytes: 11,
            peak_retained_charge_bytes: 12,
            retained_charge_after_finalize: 13,
        },
        process_governor: chronoxide_core::storage::segment::RangeScalarCacheGovernorStats {
            limit_bytes: 14,
            current_leased_bytes: 15,
            peak_leased_bytes: 16,
        },
    };
    let raw = serde_json::to_value(QueryBenchmarkRawRangeScalarCacheV2::from(cache)).unwrap();
    assert_eq!(
        raw,
        serde_json::json!({
            "configured_budget_bytes": 1,
            "governor_lease_bytes": 2,
            "governor_refused": true,
            "allocation_refused": false,
            "layout_overflow": true,
            "entry_arena_charge_bytes": 3,
            "sample_arena_charge_bytes": 4,
            "hits": 5,
            "misses": 6,
            "admitted_entries": 7,
            "streaming_budget_bypasses": 8,
            "unsupported_bypasses": 9,
            "logical_hit_bytes": 10,
            "logical_miss_or_bypass_bytes": 11,
            "peak_retained_charge_bytes": 12,
            "retained_charge_after_finalize": 13,
            "process_governor_limit_bytes": 14,
            "process_governor_current_leased_bytes": 15,
            "process_governor_peak_leased_bytes": 16
        })
    );

    let semantic_fingerprint = chronoxide_core::storage::segment::QueryExecution {
        results: Vec::new(),
        stats: QueryStats::default(),
    }
    .semantic_fingerprint_sha256();
    let result = QueryBenchmarkResult {
        query: "time()".to_string(),
        run_kind: QueryBenchmarkRunKind::Warm,
        run_index: 2,
        query_session_open: Duration::ZERO,
        duration: Duration::ZERO,
        effective_start_ms: 0,
        effective_end_ms: 0,
        step_ms: Some(1),
        semantic_fingerprint,
        result_series: 0,
        result_samples: 0,
        stats: QueryStats::default(),
        session_stats_delta: SegmentStoreQuerySessionStats::default(),
        session_profile_delta: SegmentStoreQueryProfile::default(),
        range_scalar_cache: Some(cache),
    };
    let mut markdown = String::new();
    render_range_scalar_cache_runs(&mut markdown, &[result]);
    for field in [
        "configured_budget_bytes",
        "governor_lease_bytes",
        "governor_refused",
        "allocation_refused",
        "layout_overflow",
        "entry_arena_charge_bytes",
        "sample_arena_charge_bytes",
        "hits",
        "misses",
        "admitted_entries",
        "streaming_budget_bypasses",
        "unsupported_bypasses",
        "logical_hit_bytes",
        "logical_miss_or_bypass_bytes",
        "peak_retained_charge_bytes",
        "retained_charge_after_finalize",
        "process_governor_limit_bytes",
        "process_governor_current_leased_bytes",
        "process_governor_peak_leased_bytes",
    ] {
        assert!(markdown.contains(field), "missing Markdown field {field}");
    }
    assert!(markdown.contains("| `time()` | Warm | 2 | 1 | 2 | true | false | true | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 |"));
}

#[test]
fn range_scalar_cache_budget_is_propagated_to_every_query_session_and_run() {
    let tempdir = segment_store_with_float_and_histogram();
    for (index, budget) in [
        0,
        chronoxide_core::storage::segment::MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES,
    ]
    .into_iter()
    .enumerate()
    {
        let config = QueryBenchmarkConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join(format!("query_range_cache_{index}.md")),
            raw_output: None,
            start_ms: 1_000,
            end_ms: 5_000,
            mode: QueryBenchmarkMode::Range { step_ms: 2_000 },
            range_scalar_cache_max_bytes: Some(budget),
            chunk_read_mode: ChunkReadModeArg::Pread,
            chunk_read_queue_depth: 128,
            queries: vec!["time()".to_string(), "time() + 1".to_string()],
            benchmark_repeats: 2,
            prewarm_query_contexts: false,
            prefetch_query_data: false,
            exponential_histogram_bucket_boundaries: Vec::new(),
            limits: QueryLimits::production_default(),
            validate_segment_footers: false,
        };
        let report = run_query_benchmark(&config).unwrap();
        assert_eq!(report.results.len(), 4);
        assert_eq!(
            report
                .results
                .iter()
                .map(|result| result.query.as_str())
                .collect::<Vec<_>>(),
            vec!["time()", "time()", "time() + 1", "time() + 1"]
        );
        for result in &report.results {
            let cache = result.range_scalar_cache.unwrap();
            assert_eq!(cache.summary.configured_budget_bytes, budget);
            assert_eq!(cache.summary.retained_charge_after_finalize, 0);
            assert_eq!(cache.process_governor.current_leased_bytes, 0);
        }
    }

    let instant = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_instant_cache.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["time()".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };
    let report = run_query_benchmark(&instant).unwrap();
    assert_eq!(report.results[0].range_scalar_cache, None);
}

#[test]
fn range_scalar_cache_range_only_validation_happens_before_output_writes() {
    let tempdir = segment_store_with_float_and_histogram();
    let output = tempdir.path().join("must-not-exist/query_benchmark.md");
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: output.clone(),
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: Some(0),
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["time()".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let error = run_query_benchmark(&config).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("requires a PromQL range workload")
    );
    assert!(!output.exists());
    assert!(!output.parent().unwrap().exists());
}

#[test]
fn raw_benchmark_does_not_write_json_when_raw_output_is_none() {
    let tempdir = segment_store_with_float_and_histogram();
    let absent_raw_output = tempdir.path().join("query_benchmark.json");
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    run_query_benchmark(&config).unwrap();

    assert!(config.output.exists());
    assert!(!absent_raw_output.exists());
}

#[test]
fn raw_benchmark_rejects_the_markdown_output_path_as_raw_output() {
    let tempdir = segment_store_with_float_and_histogram();
    let shared_output = tempdir.path().join("query_benchmark.md");
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: shared_output.clone(),
        raw_output: Some(shared_output.clone()),
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let error = run_query_benchmark(&config).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("same file"));
    assert!(!shared_output.exists());
}

#[test]
fn raw_benchmark_stage_failure_leaves_final_outputs_and_temp_files_absent() {
    let outer = tempfile::tempdir().unwrap();
    let output_dir = outer.path().join("reports");
    let markdown_output = output_dir.join("query_benchmark.md");
    let raw_output = output_dir.join("query_benchmark.json");
    let error = publish_benchmark_outputs_with_stager(
        &markdown_output,
        b"# report\n",
        Some((&raw_output, b"{}\n")),
        |destination, bytes, kind| {
            if kind == BenchmarkOutputKind::Raw {
                return Err(io::Error::other("injected raw stage failure"));
            }
            StagedBenchmarkOutput::stage(destination.clone(), bytes)
        },
    )
    .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Other);
    assert!(error.to_string().contains("injected raw stage failure"));
    assert!(!markdown_output.exists());
    assert!(!raw_output.exists());
    assert_no_benchmark_temp_files(&output_dir);
}

#[test]
fn raw_benchmark_rejects_normalized_parent_alias_before_store_open() {
    let outer = tempfile::tempdir().unwrap();
    let output_dir = outer.path().join("reports");
    fs::create_dir_all(output_dir.join("nested")).unwrap();
    let markdown_output = output_dir.join("query_benchmark.out");
    let raw_output = output_dir
        .join("nested")
        .join("..")
        .join("query_benchmark.out");
    let config = benchmark_config_for_outputs(
        outer.path().join("missing-segments"),
        markdown_output.clone(),
        raw_output,
    );

    let error = run_query_benchmark(&config).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("same file"));
    assert!(!markdown_output.exists());
    assert_no_benchmark_temp_files(&output_dir);
}

#[test]
fn raw_benchmark_rejects_raw_parent_at_markdown_destination_before_store_open() {
    let outer = tempfile::tempdir().unwrap();
    let output_dir = outer.path().join("reports");
    let markdown_output = output_dir.join("result");
    let raw_output = markdown_output.join("raw.json");
    let config = benchmark_config_for_outputs(
        outer.path().join("missing-segments"),
        markdown_output.clone(),
        raw_output.clone(),
    );

    let error = run_query_benchmark(&config).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("regular file"));
    assert!(!markdown_output.is_file());
    assert!(!raw_output.is_file());
    assert_no_benchmark_temp_files(&output_dir);
    assert_no_benchmark_temp_files(&markdown_output);
}

#[test]
fn raw_benchmark_rejects_markdown_parent_at_raw_destination_before_store_open() {
    let outer = tempfile::tempdir().unwrap();
    let output_dir = outer.path().join("reports");
    let raw_output = output_dir.join("result");
    let markdown_output = raw_output.join("report.md");
    let config = benchmark_config_for_outputs(
        outer.path().join("missing-segments"),
        markdown_output.clone(),
        raw_output.clone(),
    );

    let error = run_query_benchmark(&config).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("regular file"));
    assert!(!markdown_output.is_file());
    assert!(!raw_output.is_file());
    assert_no_benchmark_temp_files(&output_dir);
    assert_no_benchmark_temp_files(&raw_output);
}

#[cfg(unix)]
#[test]
fn raw_benchmark_rejects_symlink_parent_alias_before_store_open() {
    use std::os::unix::fs::symlink;

    let outer = tempfile::tempdir().unwrap();
    let real_parent = outer.path().join("real");
    let alias_parent = outer.path().join("alias");
    fs::create_dir(&real_parent).unwrap();
    symlink(&real_parent, &alias_parent).unwrap();
    let markdown_output = real_parent.join("query_benchmark.out");
    let config = benchmark_config_for_outputs(
        outer.path().join("missing-segments"),
        markdown_output.clone(),
        alias_parent.join("query_benchmark.out"),
    );

    let error = run_query_benchmark(&config).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("same file"));
    assert!(!markdown_output.exists());
    assert_no_benchmark_temp_files(&real_parent);
}

#[cfg(unix)]
#[test]
fn raw_benchmark_rejects_existing_hard_link_destinations_before_store_open() {
    let outer = tempfile::tempdir().unwrap();
    let output_dir = outer.path().join("reports");
    fs::create_dir(&output_dir).unwrap();
    let markdown_output = output_dir.join("query_benchmark.md");
    let raw_output = output_dir.join("query_benchmark.json");
    fs::write(&markdown_output, b"original report").unwrap();
    fs::hard_link(&markdown_output, &raw_output).unwrap();
    let config = benchmark_config_for_outputs(
        outer.path().join("missing-segments"),
        markdown_output.clone(),
        raw_output.clone(),
    );

    let error = run_query_benchmark(&config).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("same file"));
    assert_eq!(fs::read(&markdown_output).unwrap(), b"original report");
    assert_eq!(fs::read(&raw_output).unwrap(), b"original report");
    assert_no_benchmark_temp_files(&output_dir);
}

#[test]
fn warm_median_markdown_renders_na_without_warm_runs() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    run_query_benchmark(&config).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();
    let summary_row = markdown
        .lines()
        .find(|line| line.starts_with("| `cpu.usage` | 1 | 0 |"))
        .unwrap();

    assert!(markdown.contains("| Warm Mean | Warm Median | Warm Min | Warm Max |"));
    assert_eq!(summary_row.matches("n/a").count(), 4);
}

#[test]
fn run_query_benchmark_can_prewarm_contexts_before_measured_queries() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: true,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
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
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec![
            r#"request.duration_count{route="/typed"}"#.to_string(),
            r#"request.duration_count{route="/typed"}"#.to_string(),
        ],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
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
        raw_output: None,
        start_ms: 0,
        end_ms: 20_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let expected_selected_corpus =
        open_segment_store(tempdir.path(), false, query_projection_config(&[]))
            .unwrap()
            .corpus_fingerprint_sha256()
            .unwrap();
    let full_directory_corpus = SegmentStoreReader::open(tempdir.path())
        .unwrap()
        .corpus_fingerprint_sha256()
        .unwrap();
    assert_ne!(expected_selected_corpus, full_directory_corpus);

    let report = run_query_benchmark(&config).unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_samples, 1);
    assert_eq!(report.results[0].result_series, 1);
    assert_eq!(report.corpus_fingerprint, expected_selected_corpus);
}

#[test]
fn run_query_benchmark_defaults_omitted_end_for_instant_vector_expressions() {
    let tempdir = segment_store_with_float_and_histogram();
    let raw_output = tempdir.path().join("query_benchmark.json");
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: Some(raw_output.clone()),
        start_ms: 0,
        end_ms: u64::MAX,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage * 2".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark(&config).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(&fs::read(&raw_output).unwrap()).unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_series, 1);
    assert_eq!(report.results[0].result_samples, 1);
    assert_eq!(report.results[0].range_scalar_cache, None);
    assert_eq!(raw["configuration"]["end_ms"], u64::MAX);
    assert!(raw["configuration"]["range_scalar_cache_max_bytes"].is_null());
    assert_eq!(raw["runs"][0]["effective_start_ms"], 0);
    assert_eq!(raw["runs"][0]["effective_end_ms"], 2_000);
    assert!(raw["runs"][0]["step_ms"].is_null());
    assert!(raw["runs"][0]["range_scalar_cache"].is_null());
}

#[test]
fn run_query_benchmark_defaults_omitted_end_for_aggregations() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: u64::MAX,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["sum(cpu.usage)".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
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
        raw_output: None,
        start_ms: 0,
        end_ms: u64::MAX,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["sparse.cpu * 2".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
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
    assert_eq!(defaults.chunk_read_mode, ChunkReadModeArg::Pread);
    assert_eq!(defaults.chunk_read_queue_depth, 128);
    assert!(!defaults.experimental_cross_segment_chunk_reads);

    let overridden = Args::parse_from([
        "chronoxide-query",
        "--query",
        "cpu.usage",
        "--benchmark-repeats",
        "5",
        "--chunk-read-mode",
        "io-uring",
        "--chunk-read-queue-depth",
        "8",
        "--experimental-cross-segment-chunk-reads",
    ]);
    assert_eq!(overridden.benchmark_repeats, 5);
    assert_eq!(overridden.chunk_read_mode, ChunkReadModeArg::IoUring);
    assert_eq!(overridden.chunk_read_queue_depth, 8);
    assert!(overridden.experimental_cross_segment_chunk_reads);
}

#[test]
fn raw_benchmark_cli_parses_raw_output_path() {
    let args = Args::parse_from([
        "chronoxide-query",
        "--query",
        "cpu.usage",
        "--raw-output",
        "reports/raw/query.json",
    ]);

    assert_eq!(
        args.raw_output,
        Some(PathBuf::from("reports/raw/query.json"))
    );
}

#[test]
fn range_scalar_cache_cli_defaults_accepts_boundaries_and_rejects_non_range_use() {
    let default_range = Args::try_parse_from([
        "chronoxide-query",
        "--query",
        "time()",
        "--start-ms",
        "1000",
        "--end-ms",
        "5000",
        "--step-ms",
        "2000",
    ])
    .unwrap();
    let (_, _, default_mode) = benchmark_request_from_args(&default_range).unwrap();
    assert_eq!(
        range_scalar_cache_budget_from_args(&default_range, Some(default_mode)).unwrap(),
        Some(chronoxide_core::storage::segment::DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES)
    );

    for budget in [
        0,
        chronoxide_core::storage::segment::MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES,
    ] {
        let args = Args::try_parse_from(vec![
            "chronoxide-query".to_string(),
            "--query".to_string(),
            "time()".to_string(),
            "--start-ms".to_string(),
            "1000".to_string(),
            "--end-ms".to_string(),
            "5000".to_string(),
            "--step-ms".to_string(),
            "2000".to_string(),
            "--range-scalar-cache-max-bytes".to_string(),
            budget.to_string(),
        ])
        .unwrap();
        let (_, _, mode) = benchmark_request_from_args(&args).unwrap();
        assert_eq!(
            range_scalar_cache_budget_from_args(&args, Some(mode)).unwrap(),
            Some(budget)
        );
    }

    let too_large = chronoxide_core::storage::segment::MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES + 1;
    let args = Args::try_parse_from(vec![
        "chronoxide-query".to_string(),
        "--query".to_string(),
        "time()".to_string(),
        "--start-ms".to_string(),
        "1000".to_string(),
        "--end-ms".to_string(),
        "5000".to_string(),
        "--step-ms".to_string(),
        "2000".to_string(),
        "--range-scalar-cache-max-bytes".to_string(),
        too_large.to_string(),
    ])
    .unwrap();
    let (_, _, mode) = benchmark_request_from_args(&args).unwrap();
    let expected =
        chronoxide_core::storage::segment::validate_range_scalar_cache_budget_bytes(too_large)
            .unwrap_err()
            .to_string();
    assert_eq!(
        range_scalar_cache_budget_from_args(&args, Some(mode))
            .unwrap_err()
            .to_string(),
        expected
    );

    for argv in [
        vec!["chronoxide-query", "--range-scalar-cache-max-bytes", "0"],
        vec![
            "chronoxide-query",
            "--query",
            "time()",
            "--range-scalar-cache-max-bytes",
            "0",
        ],
    ] {
        let args = Args::try_parse_from(argv).unwrap();
        let mode = (!args.queries.is_empty()).then_some(QueryBenchmarkMode::Instant);
        assert!(
            range_scalar_cache_budget_from_args(&args, mode)
                .unwrap_err()
                .to_string()
                .contains("requires a PromQL range workload")
        );
    }
}

#[test]
fn warm_median_duration_handles_empty_odd_even_and_one_sample() {
    assert_eq!(median_duration(Vec::new()), None);
    assert_eq!(
        median_duration(vec![
            Duration::from_millis(30),
            Duration::from_millis(10),
            Duration::from_millis(20),
        ]),
        Some(Duration::from_millis(20))
    );
    assert_eq!(
        median_duration(vec![
            Duration::from_millis(40),
            Duration::from_millis(10),
            Duration::from_millis(30),
            Duration::from_millis(20),
        ]),
        Some(Duration::from_millis(25))
    );
    assert_eq!(
        median_duration(vec![Duration::from_millis(17)]),
        Some(Duration::from_millis(17))
    );
    assert_eq!(
        median_duration(vec![Duration::MAX, Duration::ZERO]),
        Some(Duration::MAX / 2)
    );
}

#[test]
fn benchmark_request_defaults_to_instant_and_parses_explicit_range() {
    let instant = Args::parse_from(["chronoxide-query", "--query", "cpu.usage"]);
    assert_eq!(
        benchmark_request_from_args(&instant).unwrap(),
        (0, u64::MAX, QueryBenchmarkMode::Instant),
    );

    let range = Args::parse_from([
        "chronoxide-query",
        "--query",
        "time()",
        "--start-ms",
        "1000",
        "--end-ms",
        "5000",
        "--step-ms",
        "2000",
    ]);
    assert_eq!(
        benchmark_request_from_args(&range).unwrap(),
        (1_000, 5_000, QueryBenchmarkMode::Range { step_ms: 2_000 },),
    );
}

#[test]
fn benchmark_request_rejects_invalid_range_configuration() {
    let missing_start = Args::parse_from([
        "chronoxide-query",
        "--query",
        "time()",
        "--end-ms",
        "5000",
        "--step-ms",
        "2000",
    ]);
    assert!(
        benchmark_request_from_args(&missing_start)
            .unwrap_err()
            .to_string()
            .contains("explicit --start-ms")
    );

    let missing_end = Args::parse_from([
        "chronoxide-query",
        "--query",
        "time()",
        "--start-ms",
        "1000",
        "--step-ms",
        "2000",
    ]);
    assert!(
        benchmark_request_from_args(&missing_end)
            .unwrap_err()
            .to_string()
            .contains("explicit --end-ms")
    );

    let zero_step = Args::parse_from([
        "chronoxide-query",
        "--query",
        "time()",
        "--start-ms",
        "1000",
        "--end-ms",
        "5000",
        "--step-ms",
        "0",
    ]);
    assert!(
        benchmark_request_from_args(&zero_step)
            .unwrap_err()
            .to_string()
            .contains("--step-ms >= 1")
    );

    let reversed = Args::parse_from([
        "chronoxide-query",
        "--query",
        "time()",
        "--start-ms",
        "5000",
        "--end-ms",
        "1000",
        "--step-ms",
        "2000",
    ]);
    assert!(
        benchmark_request_from_args(&reversed)
            .unwrap_err()
            .to_string()
            .contains("--end-ms >= --start-ms")
    );

    let too_many_evaluations = Args::parse_from([
        "chronoxide-query",
        "--query",
        "time()",
        "--start-ms",
        "0",
        "--end-ms",
        "1000000",
        "--step-ms",
        "1",
    ]);
    assert!(
        benchmark_request_from_args(&too_many_evaluations)
            .unwrap_err()
            .to_string()
            .contains("scheduled evaluations")
    );

    for unsupported in ["--prewarm-query-contexts", "--prefetch-query-data"] {
        let args = Args::parse_from([
            "chronoxide-query",
            "--query",
            "time()",
            "--start-ms",
            "1000",
            "--end-ms",
            "5000",
            "--step-ms",
            "2000",
            unsupported,
        ]);
        assert!(
            benchmark_request_from_args(&args)
                .unwrap_err()
                .to_string()
                .contains(unsupported)
        );
    }
}

#[test]
fn explicit_query_args_parse_exponential_histogram_bucket_boundaries() {
    let args = Args::parse_from([
        "chronoxide-query",
        "--exponential-histogram-bucket-boundary",
        "2",
        "--exponential-histogram-bucket-boundary",
        "4",
    ]);

    assert_eq!(args.exponential_histogram_bucket_boundaries, vec![2.0, 4.0]);
}

#[test]
fn run_query_benchmark_reports_session_cold_and_warm_runs_without_smoke_scans() {
    let tempdir = segment_store_with_float_and_histogram();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 3,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
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

    let _store = open_segment_store(tempdir.path(), false, query_projection_config(&[]))
        .expect("default query open should skip footer checksum validation");

    let err = match open_segment_store(tempdir.path(), true, query_projection_config(&[])) {
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
        raw_output: None,
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec![r#"request.duration_bucket"#.to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
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
fn run_query_benchmark_rejects_range_configuration_before_store_open() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().join("missing-segments"),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 1_000,
        end_ms: 5_000,
        mode: QueryBenchmarkMode::Range { step_ms: 0 },
        range_scalar_cache_max_bytes: None,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        queries: vec!["time()".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let err = run_query_benchmark(&config).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(err.to_string().contains("--step-ms >= 1"));
    assert!(!config.output.exists());
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
        exponential_histogram_bucket_boundaries: Vec::new(),
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
fn scalar_readback_oracle_omits_exact_stale_without_rebasing_range() {
    let base = ExpectedReadback {
        query: "stale.counter".to_string(),
        start_ms: 1_000,
        end_ms: 8_000,
        samples: vec![
            (1_000, 100.0),
            (2_000, prometheus_stale_nan()),
            (7_000, 1.0),
            (8_000, 2.0),
        ],
        isolation_check: None,
    };
    let expected_increase = 2.0 * 7_001.0 / 7_000.0;

    for hints in [
        None,
        Some(
            [
                CounterResetHint::Unknown,
                CounterResetHint::NotCounterReset,
                CounterResetHint::Unknown,
                CounterResetHint::NotCounterReset,
            ]
            .as_slice(),
        ),
    ] {
        let (range_ms, increase) = scalar_counter_range_increase(&base, hints).unwrap();
        assert_eq!(range_ms, 7_001);
        assert!((increase - expected_increase).abs() < 1e-12);
    }
}

#[test]
fn scalar_readback_oracle_preserves_ordinary_non_finite_range_results() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_ne!(value.to_bits(), prometheus_stale_nan().to_bits());
        let base = ExpectedReadback {
            query: "nonfinite.counter".to_string(),
            start_ms: 1_000,
            end_ms: 2_000,
            samples: vec![(1_000, 1.0), (2_000, value)],
            isolation_check: None,
        };

        let readbacks = scalar_expected_readbacks(base.clone());
        let increase = readbacks
            .iter()
            .find(|readback| readback.query.starts_with("increase("))
            .expect("ordinary non-finite increase readback");
        let rate = readbacks
            .iter()
            .find(|readback| readback.query.starts_with("rate("))
            .expect("ordinary non-finite rate readback");
        for actual in [increase.samples[0].1, rate.samples[0].1] {
            if value.is_nan() {
                assert!(actual.is_nan());
                assert_ne!(actual.to_bits(), prometheus_stale_nan().to_bits());
            } else {
                assert_eq!(actual, value);
            }
        }

        let hinted = scalar_counter_range_increase(
            &base,
            Some(&[CounterResetHint::Unknown, CounterResetHint::NotCounterReset]),
        )
        .expect("hinted ordinary non-finite increase");
        if value.is_nan() {
            assert!(hinted.1.is_nan());
        } else {
            assert_eq!(hinted.1, value);
        }
    }
}

#[test]
fn scalar_readback_oracle_accounts_for_pre_epoch_range_duration() {
    let base = ExpectedReadback {
        query: "pre.epoch.counter".to_string(),
        start_ms: 0,
        end_ms: 1_000,
        samples: vec![(0, 5.0), (1_000, 10.0)],
        isolation_check: None,
    };

    let (range_ms, increase) = scalar_counter_range_increase(&base, None).unwrap();

    assert_eq!(range_ms, 1_001);
    assert!((increase - 5.005).abs() < 1e-12);
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
        exponential_histogram_bucket_boundaries: Vec::new(),
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
fn exponential_histogram_expected_readbacks_include_configured_finite_buckets() {
    let labels = [
        (
            METRIC_NAME_LABEL.to_string(),
            "http.request.size".to_string(),
        ),
        ("route".to_string(), "/exphist".to_string()),
    ];
    let samples = vec![(
        5_000,
        ExponentialHistogramValue {
            count: 5,
            sum: Some(12.0),
            min: None,
            max: None,
            metadata: TypedSampleMetadata::default(),
            scale: 0,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![2, 3],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: Vec::new(),
            },
        },
    )];

    let expected = exponential_histogram_expected_readbacks(
        "http.request.size",
        &labels,
        &samples,
        0,
        10_000,
        &[2.0],
    );
    let bucket_selector =
        promql_exact_selector("http.request.size_bucket", &labels, Some(("le", "2")));

    let bucket = expected
        .iter()
        .find(|readback| readback.query == bucket_selector)
        .expect("finite exponential histogram bucket readback");

    assert_eq!(bucket.samples, vec![(5_000, 2.0)]);
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
        exponential_histogram_bucket_boundaries: Vec::new(),
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
fn verify_expected_readbacks_reports_missing_expected_samples() {
    let tempdir = segment_store_with_float_and_histogram();
    let store = open_segment_store(tempdir.path(), false, query_projection_config(&[])).unwrap();
    let mut query_session = store.query_session().unwrap();
    let mut diagnostics = QueryReadbackDiagnostics::default();
    let expected = vec![ExpectedReadback {
        query: r#"{__name__="cpu.usage",instance="host-a"}"#.to_string(),
        start_ms: 1_000,
        end_ms: 1_000,
        samples: vec![(1_000, 99.0)],
        isolation_check: None,
    }];

    let verification =
        verify_expected_readbacks(&mut query_session, &expected, &mut diagnostics).unwrap();

    assert_eq!(verification.checked_queries, 1);
    assert_eq!(diagnostics.executed_queries, 1);
    assert_eq!(verification.mismatches.len(), 1);
    assert_eq!(verification.mismatches[0].query, expected[0].query);
    assert_eq!(
        verification.mismatches[0].missing_expected_samples,
        vec![(1_000, 99.0)]
    );
    assert_eq!(
        verification.mismatches[0].actual_samples,
        vec![(1_000, 1.0)]
    );
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

fn segment_store_with_int64() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_i64_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 7), (2_000, 9)],
            |visit| {
                visit(METRIC_NAME_LABEL, "queue_depth");
                visit("instance", "host-a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_summary() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                5_000,
                SummaryValue {
                    count: 10,
                    sum: 50.0,
                    metadata: TypedSampleMetadata::default(),
                    quantiles: vec![
                        SummaryQuantileValue {
                            quantile: 0.5,
                            value: 4.0,
                        },
                        SummaryQuantileValue {
                            quantile: 0.9,
                            value: 8.0,
                        },
                    ],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_latency");
                visit("route", "/summary");
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

fn segment_store_with_exponential_histogram() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                5_000,
                ExponentialHistogramValue {
                    count: 5,
                    sum: Some(12.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![2, 3],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.size");
                visit("route", "/exphist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_delta_exponential_histogram() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let metadata = TypedSampleMetadata {
        temporality: OtlpAggregationTemporality::Delta,
        ..TypedSampleMetadata::default()
    };

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    ExponentialHistogramValue {
                        count: 1,
                        sum: Some(2.0),
                        min: None,
                        max: None,
                        metadata,
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![1, 0],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    2_000,
                    ExponentialHistogramValue {
                        count: 1,
                        sum: Some(4.0),
                        min: None,
                        max: None,
                        metadata,
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![0, 1],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "delta_http_request_size");
                visit("route", "/delta-exphist");
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
