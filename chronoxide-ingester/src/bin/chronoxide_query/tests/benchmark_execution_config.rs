use super::*;

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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
    assert!(report.results[0].session_stats_delta.segment_context_opens > 0);
    assert!(report.results[0].session_profile_delta.segment_context_open > Duration::ZERO);
    assert!(report.results[0].session_profile_delta.exact_postings_read > Duration::ZERO);
    assert!(report.results[0].session_profile_delta.chunk_read > Duration::ZERO);
    let payload_used_bytes = report.results[0].session_profile_delta.chunk_payload_bytes;
    let payload_read_bytes = report.results[0]
        .session_profile_delta
        .chunk_payload_physical_bytes;
    assert!(payload_used_bytes > 0);
    assert!(payload_read_bytes >= payload_used_bytes);
    assert!(report.results[1].session_stats_delta.segment_context_opens > 0);
    assert!(report.results[1].session_profile_delta.segment_context_open > Duration::ZERO);
    assert!(report.results[1].session_profile_delta.exact_postings_read > Duration::ZERO);
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
    assert!(markdown.contains("## Session Symbol Reads And Page Cache"));
    assert!(markdown.contains("## Query Result Read Profiles"));
    assert!(markdown.contains("## Query Result Index Positional Reads"));
    assert!(markdown.contains("## Query Result Symbol Reads And Page Cache"));
    assert!(markdown.contains("Page Cache Charge Bytes After Run"));
    assert!(markdown.contains("Page Cache Max Bytes After Run"));
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
    assert!(markdown.contains("- Label Materialization: demand-driven"));
    assert!(markdown.contains("- Query Label Storage: owned-strings"));
    assert!(markdown.contains(&format!(
        "- Query Label Arena Max Bytes: {DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES}"
    )));
    assert!(markdown.contains("- Storage Layout: schema8"));
    assert!(markdown.contains("- Requested Segment Footer Validation: false"));
    assert!(markdown.contains("- Effective Segment Footer Validation: false"));
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
fn detailed_query_instrumentation_preserves_results_and_emits_bounded_stable_stages() {
    let segments = segment_store_with_float_and_histogram();
    let off_raw = segments.path().join("instrumentation-off.json");
    let mut off_config = benchmark_config_for_outputs(
        segments.path().to_path_buf(),
        segments.path().join("instrumentation-off.md"),
        off_raw,
    );
    off_config.end_ms = 2_000;
    off_config.queries = vec!["sum(cpu.usage)".to_string()];

    let detailed_raw = segments.path().join("instrumentation-detailed.json");
    let mut detailed_config = off_config.clone();
    detailed_config.output = segments.path().join("instrumentation-detailed.md");
    detailed_config.raw_output = Some(detailed_raw.clone());

    let off = run_query_benchmark_with_experimental_flow_and_instrumentation(
        &off_config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::OwnedStrings,
        StorageLayoutArg::Schema8,
        QueryInstrumentationArg::Off,
    )
    .unwrap();
    let detailed = run_query_benchmark_with_experimental_flow_and_instrumentation(
        &detailed_config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::OwnedStrings,
        StorageLayoutArg::Schema8,
        QueryInstrumentationArg::Detailed,
    )
    .unwrap();

    assert_eq!(off.query_instrumentation, QueryInstrumentationArg::Off);
    assert_eq!(
        detailed.query_instrumentation,
        QueryInstrumentationArg::Detailed
    );
    assert_eq!(off.results.len(), 1);
    assert_eq!(detailed.results.len(), 1);
    let off_result = &off.results[0];
    let detailed_result = &detailed.results[0];
    assert_eq!(
        off_result.semantic_fingerprint,
        detailed_result.semantic_fingerprint
    );
    assert_eq!(
        off_result.portable_semantic_fingerprint,
        detailed_result.portable_semantic_fingerprint
    );
    assert_eq!(off_result.stats, detailed_result.stats);
    assert_eq!(off_result.result_series, detailed_result.result_series);
    assert_eq!(off_result.result_samples, detailed_result.result_samples);
    assert_eq!(
        off_result.session_profile_delta.stages,
        QueryStageProfile::default()
    );
    let detailed_total = detailed_result
        .session_profile_delta
        .stages
        .total_exclusive();
    assert!(detailed_total > Duration::ZERO);
    assert!(detailed_total <= detailed_result.duration);

    let raw: serde_json::Value = serde_json::from_slice(&fs::read(detailed_raw).unwrap()).unwrap();
    assert_eq!(raw["configuration"]["query_instrumentation"], "detailed");
    let run = &raw["runs"][0];
    let stages = &run["query_stages"];
    let profile = detailed_result.session_profile_delta.stages;
    for (field, duration) in [
        ("canonical_row_decode_ns", profile.canonical_row_decode),
        ("candidate_selection_ns", profile.candidate_selection),
        (
            "metadata_visit_overhead_ns",
            profile.metadata_visit_overhead,
        ),
        ("symbol_lookup_ns", profile.symbol_lookup),
        ("symbol_resolution_ns", profile.symbol_resolution),
        ("canonical_identity_ns", profile.canonical_identity),
        ("matcher_evaluation_ns", profile.matcher_evaluation),
        ("label_construction_ns", profile.label_construction),
        ("locator_planning_ns", profile.locator_planning),
        ("payload_read_pipeline_combined_ns", profile.payload_io),
        (
            "payload_decode_projection_result_processing_combined_ns",
            profile.payload_decode,
        ),
        ("source_merge_ns", profile.source_merge),
        (
            "promql_grouping_evaluation_ns",
            profile.promql_grouping_evaluation,
        ),
        ("result_construction_ns", profile.result_construction),
    ] {
        assert_eq!(
            stages[field].as_u64().unwrap(),
            u64::try_from(duration.as_nanos()).unwrap(),
            "raw stage {field}"
        );
    }
    assert_eq!(
        stages["exclusive_total_ns"].as_u64().unwrap(),
        u64::try_from(detailed_total.as_nanos()).unwrap()
    );
    assert!(stages["exclusive_total_ns"].as_u64().unwrap() <= run["duration_ns"].as_u64().unwrap());
    assert_eq!(
        stages["unclassified_ns"].as_u64().unwrap(),
        run["duration_ns"].as_u64().unwrap() - stages["exclusive_total_ns"].as_u64().unwrap()
    );
    let stage_keys = stages
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        stage_keys,
        BTreeSet::from([
            "canonical_identity_ns",
            "canonical_row_decode_ns",
            "candidate_selection_ns",
            "exclusive_total_ns",
            "label_construction_ns",
            "locator_planning_ns",
            "matcher_evaluation_ns",
            "metadata_visit_overhead_ns",
            "payload_decode_projection_result_processing_combined_ns",
            "payload_read_pipeline_combined_ns",
            "promql_grouping_evaluation_ns",
            "result_construction_ns",
            "source_merge_ns",
            "symbol_lookup_ns",
            "symbol_resolution_ns",
            "unclassified_ns",
        ])
    );
    let metadata_keys = run["metadata_runtime"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        metadata_keys,
        BTreeSet::from([
            "counters_delta",
            "end_gauges",
            "lifetime_peaks_after_run",
            "start_gauges",
        ])
    );
    let markdown = fs::read_to_string(&detailed_config.output).unwrap();
    assert!(markdown.contains("- Query Instrumentation: detailed"));
    assert!(markdown.contains("every run is validated"));
    assert!(markdown.contains("observer-instrumented wall times are diagnostic"));
}

#[test]
fn benchmark_pipeline_compares_demand_driven_and_full_schema7_queries() {
    let segments = schema7_segment_store_with_inline_float();
    let full_raw = segments.path().join("full-selective-ab.json");
    let selective_raw = segments.path().join("selective-ab.json");
    let mut full_config = benchmark_config_for_outputs(
        segments.path().to_path_buf(),
        segments.path().join("full-selective-ab.md"),
        full_raw.clone(),
    );
    full_config.end_ms = 2_000;
    full_config.queries = vec![String::from("sum(rate(schema7_float[2s]))")];
    full_config.benchmark_repeats = 2;
    let mut selective_config = full_config.clone();
    selective_config.output = segments.path().join("selective-ab.md");
    selective_config.raw_output = Some(selective_raw.clone());

    let full = run_query_benchmark_with_experimental_flow(
        &full_config,
        false,
        LabelMaterializationArg::Full,
        LabelStorageArg::SharedAtoms,
        StorageLayoutArg::Schema7,
    )
    .unwrap();
    let selective = run_query_benchmark_with_experimental_flow(
        &selective_config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::SharedAtoms,
        StorageLayoutArg::Schema7,
    )
    .unwrap();

    assert_eq!(full.results.len(), selective.results.len());
    for (full, selective) in full.results.iter().zip(&selective.results) {
        assert_eq!(full.semantic_fingerprint, selective.semantic_fingerprint);
        assert_eq!(
            full.portable_semantic_fingerprint,
            selective.portable_semantic_fingerprint
        );
        assert_eq!(full.result_series, selective.result_series);
        assert_eq!(full.result_samples, selective.result_samples);
        assert_eq!(full.stats, selective.stats);
        assert_eq!(
            full.session_profile_delta
                .label_rows_selectively_materialized,
            0
        );
        assert_eq!(full.session_profile_delta.label_pairs_omitted, 0);
        assert!(
            selective
                .session_profile_delta
                .label_rows_selectively_materialized
                > 0
        );
        assert!(selective.session_profile_delta.label_pairs_omitted > 0);
        assert_eq!(
            full.session_profile_delta.label_pairs_integrity_checked,
            selective
                .session_profile_delta
                .label_pairs_integrity_checked
        );
    }

    let full_raw: serde_json::Value = serde_json::from_slice(&fs::read(full_raw).unwrap()).unwrap();
    let selective_raw: serde_json::Value =
        serde_json::from_slice(&fs::read(selective_raw).unwrap()).unwrap();
    assert_eq!(full_raw["configuration"]["label_materialization"], "full");
    assert_eq!(
        selective_raw["configuration"]["label_materialization"],
        "demand-driven"
    );
    assert!(
        selective_raw["runs"][0]["label_materialization"]["pairs_omitted"]
            .as_u64()
            .unwrap()
            > 0
    );
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
            query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
            chunk_read_mode: ChunkReadModeArg::Pread,
            chunk_read_queue_depth: 128,
            chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
    assert_eq!(
        report.query_context_prewarm_stats_delta.index_routing_opens,
        0
    );
    assert_eq!(
        report
            .query_context_prewarm_profile_delta
            .index_routing_open,
        Duration::ZERO
    );
    assert_eq!(
        report
            .query_context_prewarm_profile_delta
            .exact_postings_read,
        Duration::ZERO
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
    assert_eq!(
        report.results[0].session_stats_delta.segment_context_opens,
        0
    );
    assert_eq!(report.results[0].session_stats_delta.chunks_bin_opens, 1);
    assert_eq!(
        report.results[0].session_profile_delta.segment_context_open,
        Duration::ZERO
    );
    assert_eq!(
        report.results[0].session_profile_delta.series_open,
        Duration::ZERO
    );
    assert!(report.results[0].session_profile_delta.exact_postings_read > Duration::ZERO);
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
    let segments = sorted_segment_metadata(tempdir.path());
    assert_eq!(segments.len(), 2);
    publish_manifest_segments(tempdir.path(), &[&segments[0]]);
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: 20_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
    let full_directory_corpus = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
            ..SegmentStoreOpenOptions::default()
        },
    )
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
    assert_eq!(report.results[0].effective_end_ms, 2_000);
}

#[test]
fn run_query_benchmark_reads_schema7_max_sample_time_for_omitted_instant_end() {
    let tempdir = segment_store_with_sparse_final_window_for_schema(SegmentStorageSchema::Schema7);
    let config = QueryBenchmarkConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("query_benchmark.md"),
        raw_output: None,
        start_ms: 0,
        end_ms: u64::MAX,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        queries: vec!["sparse.cpu * 2".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    };

    let report = run_query_benchmark_with_experimental_flow(
        &config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::SharedAtoms,
        StorageLayoutArg::Schema7,
    )
    .unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].result_series, 1);
    assert_eq!(report.results[0].result_samples, 1);
    assert_eq!(report.results[0].effective_end_ms, 1_000);
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
    assert_eq!(report.results[0].effective_end_ms, 1_000);
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
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
