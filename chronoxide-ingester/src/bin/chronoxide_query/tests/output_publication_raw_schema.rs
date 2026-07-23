use super::*;

#[test]
fn raw_v14_serializes_complete_compact_query_label_accounting() {
    let stats = QueryLabelStorageStats {
        label_sets: 1,
        atom_lookups: 2,
        atom_hits: 1,
        atom_misses: 1,
        unique_content_bytes: 3,
        compact_label_sets: 4,
        compact_pairs: 5,
        compact_source_symbol_translations: 6,
        compact_source_symbol_translation_hits: 4,
        compact_source_symbol_translation_misses: 2,
        compact_atom_lookups: 7,
        compact_atom_hits: 3,
        compact_atom_misses: 4,
        compact_unique_strings: 8,
        compact_unique_content_bytes: 9,
        compact_arena_budget_bytes: 100,
        compact_arena_current_bytes: 50,
        compact_arena_peak_bytes: 60,
        compact_atom_bytes: 10,
        compact_pair_bytes: 11,
        compact_hash_directory_bytes: 12,
        compact_translation_bytes: 17,
        compact_retained_bytes: 50,
        compact_arena_admission_refusals: 13,
        compact_compatibility_materializations: 14,
    };

    validate_query_label_storage_stats(stats).unwrap();
    let raw = serde_json::to_value(QueryBenchmarkRawQueryLabelStorageV2::from(stats)).unwrap();

    assert_eq!(
        raw,
        serde_json::json!({
            "label_sets": 1,
            "atom_lookups": 2,
            "atom_hits": 1,
            "atom_misses": 1,
            "unique_content_bytes": 3,
            "compact_label_sets": 4,
            "compact_pairs": 5,
            "compact_source_symbol_translations": 6,
            "compact_source_symbol_translation_hits": 4,
            "compact_source_symbol_translation_misses": 2,
            "compact_atom_lookups": 7,
            "compact_atom_hits": 3,
            "compact_atom_misses": 4,
            "compact_unique_strings": 8,
            "compact_unique_content_bytes": 9,
            "compact_arena_budget_bytes": 100,
            "compact_arena_current_bytes": 50,
            "compact_arena_peak_bytes": 60,
            "compact_atom_bytes": 10,
            "compact_pair_bytes": 11,
            "compact_hash_directory_bytes": 12,
            "compact_translation_bytes": 17,
            "compact_retained_bytes": 50,
            "compact_arena_admission_refusals": 13,
            "compact_compatibility_materializations": 14
        })
    );
}

#[test]
fn raw_v14_serializes_complete_chunk_read_scheduler_profile() {
    let profile = ChunkReadSchedulerProfile {
        executions: 1,
        pread_decisions: 2,
        io_uring_decisions: 3,
        logical_requests: 4,
        physical_spans: 5,
        backend_submissions: 6,
        sqes_submitted: 7,
        submission_depth_sum: 8,
        submission_depth_max: 9,
        submission_depth_1: 10,
        submission_depth_2_3: 11,
        submission_depth_4_7: 12,
        submission_depth_8_plus: 13,
        total_physical_bytes_executed: 14,
        peak_in_flight_bytes: 15,
    };

    let raw = serde_json::to_value(QueryBenchmarkRawChunkReadSchedulerV2::from(profile)).unwrap();

    assert_eq!(
        raw,
        serde_json::json!({
            "executions": 1,
            "pread_decisions": 2,
            "io_uring_decisions": 3,
            "logical_requests": 4,
            "physical_spans": 5,
            "backend_submissions": 6,
            "sqes_submitted": 7,
            "submission_depth_sum": 8,
            "session_submission_depth_high_water": 9,
            "submission_depth_1": 10,
            "submission_depth_2_3": 11,
            "submission_depth_4_7": 12,
            "submission_depth_8_plus": 13,
            "total_physical_bytes_executed": 14,
            "session_peak_in_flight_bytes_high_water": 15,
        })
    );
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
        query_label_arena_max_bytes: 1_048_576,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: 1_024,
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
    assert_eq!(report.storage_layout, StorageLayoutArg::Schema8);
    assert!(raw_text.ends_with('\n'));
    assert!(markdown.contains("- Query Label Arena Max Bytes: 1048576"));
    assert!(markdown.contains("- Chunk Payload Coalesce Max Gap Bytes: 1024"));
    assert_eq!(raw["schema"], "chronoxide.query-benchmark.raw/v14");
    assert!(raw.get("generated_at").is_none());
    assert_eq!(raw["configuration"]["chunk_read_mode"], "pread");
    assert_eq!(raw["configuration"]["chunk_read_queue_depth"], 128);
    assert_eq!(
        raw["configuration"]["chunk_payload_coalesce_max_gap_bytes"],
        1_024
    );
    assert_eq!(
        raw["configuration"]["experimental_cross_segment_chunk_reads"],
        false
    );
    assert_eq!(
        raw["configuration"]["label_materialization"],
        "demand-driven"
    );
    assert_eq!(raw["configuration"]["storage_layout"], "schema8");
    assert_eq!(raw["configuration"]["query_label_storage"], "owned-strings");
    assert_eq!(
        raw["configuration"]["query_label_arena_max_bytes"],
        1_048_576
    );
    assert_eq!(raw["configuration"]["query_instrumentation"], "off");
    assert_eq!(raw["configuration"]["range_execution_mode"], "repeated");
    let configuration_keys = raw["configuration"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        configuration_keys,
        BTreeSet::from([
            "benchmark_repeats",
            "chunk_read_mode",
            "chunk_read_queue_depth",
            "chunk_payload_coalesce_max_gap_bytes",
            "effective_segment_footer_validation",
            "end_ms",
            "experimental_cross_segment_chunk_reads",
            "exponential_histogram_bucket_boundaries",
            "label_materialization",
            "mode",
            "prefetch_query_data",
            "prewarm_query_contexts",
            "queries",
            "query_instrumentation",
            "query_label_arena_max_bytes",
            "query_label_storage",
            "range_scalar_cache_max_bytes",
            "range_execution_mode",
            "requested_segment_footer_validation",
            "segments_dir",
            "start_ms",
            "step_ms",
            "storage_layout",
        ])
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
    assert_eq!(
        raw["configuration"]["requested_segment_footer_validation"],
        true
    );
    assert_eq!(
        raw["configuration"]["effective_segment_footer_validation"],
        true
    );
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
        let run_keys = run
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            run_keys,
            BTreeSet::from([
                "chunk_read_scheduler",
                "duration_ns",
                "effective_end_ms",
                "effective_start_ms",
                "label_materialization",
                "metadata_runtime",
                "payload_reads",
                "portable_semantic_fingerprint_sha256",
                "post_query_fingerprint_ns",
                "query",
                "query_label_storage",
                "query_stages",
                "range_execution",
                "range_scalar_cache",
                "result_samples",
                "result_series",
                "run_index",
                "run_kind",
                "semantic_fingerprint_sha256",
                "stats",
                "step_ms",
                "symbol_reads",
            ])
        );
        assert!(run["duration_ns"].is_u64());
        assert_eq!(run["effective_start_ms"], 1_000);
        assert_eq!(run["effective_end_ms"], 5_000);
        assert_eq!(run["step_ms"], 2_000);
        assert_eq!(
            run["range_execution"],
            serde_json::to_value(QueryBenchmarkRawRangeExecutionV1::from(
                result.range_execution.unwrap(),
            ))
            .unwrap()
        );
        assert_eq!(
            run["duration_ns"].as_u64().unwrap(),
            u64::try_from(result.duration.as_nanos()).unwrap()
        );
        assert_eq!(
            run["post_query_fingerprint_ns"].as_u64().unwrap(),
            u64::try_from(result.post_query_fingerprint.as_nanos()).unwrap()
        );
        let stages = &run["query_stages"];
        for field in [
            "canonical_row_decode_ns",
            "candidate_selection_ns",
            "metadata_visit_overhead_ns",
            "symbol_lookup_ns",
            "symbol_resolution_ns",
            "canonical_identity_ns",
            "matcher_evaluation_ns",
            "label_construction_ns",
            "locator_planning_ns",
            "payload_read_pipeline_combined_ns",
            "payload_decode_projection_result_processing_combined_ns",
            "source_merge_ns",
            "promql_grouping_evaluation_ns",
            "result_construction_ns",
            "exclusive_total_ns",
        ] {
            assert_eq!(stages[field], 0, "off-stage field {field}");
        }
        assert_eq!(stages["unclassified_ns"], run["duration_ns"]);
        assert_eq!(
            run["semantic_fingerprint_sha256"],
            result.semantic_fingerprint.to_hex()
        );
        assert_eq!(
            run["portable_semantic_fingerprint_sha256"],
            result.portable_semantic_fingerprint.to_hex()
        );
        assert_eq!(run["result_series"], result.result_series);
        assert_eq!(run["result_samples"], result.result_samples);
        assert_eq!(
            run["chunk_read_scheduler"],
            serde_json::to_value(QueryBenchmarkRawChunkReadSchedulerV2::from(
                result.session_profile_delta.chunk_read_scheduler,
            ))
            .unwrap()
        );
        assert_eq!(
            json_object_keys(&run["chunk_read_scheduler"]),
            BTreeSet::from([
                "backend_submissions",
                "executions",
                "total_physical_bytes_executed",
                "io_uring_decisions",
                "logical_requests",
                "session_peak_in_flight_bytes_high_water",
                "physical_spans",
                "pread_decisions",
                "sqes_submitted",
                "submission_depth_1",
                "submission_depth_2_3",
                "submission_depth_4_7",
                "submission_depth_8_plus",
                "session_submission_depth_high_water",
                "submission_depth_sum",
            ])
        );
        assert_eq!(
            run["query_label_storage"]["label_sets"],
            result.label_storage_delta.label_sets
        );
        assert_eq!(
            run["query_label_storage"]["atom_lookups"],
            result.label_storage_delta.atom_lookups
        );
        assert_eq!(
            run["query_label_storage"]["atom_hits"],
            result.label_storage_delta.atom_hits
        );
        assert_eq!(
            run["query_label_storage"]["atom_misses"],
            result.label_storage_delta.atom_misses
        );
        assert_eq!(
            run["query_label_storage"]["unique_content_bytes"],
            result.label_storage_delta.unique_content_bytes
        );
        let metadata = &run["metadata_runtime"];
        assert_eq!(
            json_object_keys(metadata),
            BTreeSet::from([
                "counters_delta",
                "end_gauges",
                "lifetime_peaks_after_run",
                "start_gauges",
            ])
        );
        assert_eq!(
            json_object_keys(&metadata["counters_delta"]),
            BTreeSet::from(["cache", "file_manager", "governor", "reads"])
        );
        assert_eq!(
            json_object_keys(&metadata["counters_delta"]["cache"]),
            BTreeSet::from([
                "class_admissions",
                "corruption_detections",
                "corruption_hits",
                "evictions",
                "failed_loads",
                "hits",
                "misses",
                "resident_admission_bypasses",
                "resident_admission_refusals",
                "resident_admissions",
                "single_flight_waits",
                "successful_loads",
            ])
        );
        assert_eq!(
            json_object_keys(&metadata["start_gauges"]),
            BTreeSet::from(["cache", "file_manager", "governor"])
        );
        assert_eq!(
            json_object_keys(&metadata["start_gauges"]["cache"]),
            BTreeSet::from([
                "active_loads",
                "class_charges",
                "ledger_in_flight_bytes",
                "ledger_reserved_bytes",
                "ledger_retained_bytes",
                "live_allocations",
                "registered_artifacts",
                "resident_entries",
                "sticky_artifacts",
                "sticky_charged_bytes",
            ])
        );
        assert_eq!(
            json_object_keys(&metadata["end_gauges"]),
            BTreeSet::from(["cache", "file_manager", "governor"])
        );
        assert_eq!(
            json_object_keys(&metadata["lifetime_peaks_after_run"]),
            BTreeSet::from(["cache_class_charges", "file_manager", "governor"])
        );
        assert_eq!(
            result.label_storage_delta.atom_lookups,
            result
                .label_storage_delta
                .atom_hits
                .saturating_add(result.label_storage_delta.atom_misses)
        );
        assert_eq!(
            run["payload_reads"]["logical_used_bytes"],
            result.session_profile_delta.chunk_payload_bytes
        );
        assert_eq!(
            run["payload_reads"]["physical_reads"],
            result.session_profile_delta.chunk_payload_physical_reads
        );
        assert_eq!(
            run["payload_reads"]["physical_bytes"],
            result.session_profile_delta.chunk_payload_physical_bytes
        );
        assert_eq!(
            run["label_materialization"]["rows_integrity_checked"],
            result.session_profile_delta.label_rows_integrity_checked
        );
        assert_eq!(
            run["label_materialization"]["pairs_materialized"],
            result.session_profile_delta.label_pairs_materialized
        );
        assert_eq!(
            run["label_materialization"]["pairs_omitted"],
            result.session_profile_delta.label_pairs_omitted
        );
        assert_eq!(
            run["symbol_reads"]["legacy_eager_read_delta"]["calls"],
            result
                .session_profile_delta
                .symbol_read_stats
                .legacy_eager
                .calls
        );
        assert_eq!(
            run["symbol_reads"]["logical_returned_delta"]["bytes"],
            result
                .session_profile_delta
                .symbol_read_stats
                .logical_returned
                .bytes
        );
        assert_eq!(
            run["symbol_reads"]["root_read_delta"]["calls"],
            result.session_profile_delta.symbol_read_stats.root.calls
        );
        assert_eq!(
            run["symbol_reads"]["root_read_delta"]["bytes"],
            result.session_profile_delta.symbol_read_stats.root.bytes
        );
        assert_eq!(
            run["symbol_reads"]["page_read_delta"]["calls"],
            result.session_profile_delta.symbol_read_stats.page.calls
        );
        assert_eq!(
            run["symbol_reads"]["page_read_delta"]["bytes"],
            result.session_profile_delta.symbol_read_stats.page.bytes
        );
        assert_eq!(
            run["symbol_reads"]["page_cache_charge_bytes_after_run"],
            result
                .session_profile_delta
                .symbol_resources
                .page_cache_charge_bytes
        );
        assert_eq!(
            run["symbol_reads"]["page_cache_max_bytes_after_run"],
            result
                .session_profile_delta
                .symbol_resources
                .page_cache_max_bytes
        );
        assert_eq!(
            run["symbol_reads"]["total_retained_charge_bytes_after_run"],
            result
                .session_profile_delta
                .symbol_resources
                .total_retained_charge_bytes()
        );
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
    assert!(markdown.contains("- Query Instrumentation: off"));
    assert!(markdown.contains("## Exclusive Query Stage Attribution"));
    assert!(markdown.contains("Payload Decode / Projection / Result Processing (Combined)"));
    assert!(markdown.contains("Post-Query Fingerprints"));
    assert!(
        markdown.contains("API serialization remains a separately measured API-layer boundary")
    );
    assert!(markdown.contains("the full query wall is reported as unclassified"));
    assert!(markdown.contains("## Range Scalar Cache Runs"));
    assert!(markdown.contains("## Query Result Label Materialization"));
    assert!(markdown.contains("## Query Result Metadata Runtime Counter Deltas"));
    assert!(markdown.contains("`successful_loads` means completed metadata loads"));
    assert!(markdown.contains("## Query Result Metadata Read Deltas"));
    assert!(markdown.contains("must not be added together"));
    assert!(markdown.contains("## Query Result Metadata Runtime Start Gauges"));
    assert!(markdown.contains("initial retained-cache and file-descriptor state"));
    assert!(markdown.contains("## Query Result Metadata Runtime End Gauges"));
    assert!(markdown.contains("must not be summed across runs"));
    assert!(markdown.contains("## Query Result Metadata Runtime Lifetime Peaks After Run"));
    assert!(markdown.contains("neither per-run deltas nor per-run peaks"));
}

#[test]
fn raw_v14_distinguishes_compact_shared_and_owned_query_label_storage() {
    let segments = segment_store_with_float_and_histogram();
    let shared_raw = segments.path().join("shared-labels.json");
    let mut shared_config = benchmark_config_for_outputs(
        segments.path().to_path_buf(),
        segments.path().join("shared-labels.md"),
        shared_raw.clone(),
    );
    shared_config.end_ms = 5_000;

    let owned_raw = segments.path().join("owned-labels.json");
    let mut owned_config = shared_config.clone();
    owned_config.output = segments.path().join("owned-labels.md");
    owned_config.raw_output = Some(owned_raw.clone());

    let compact_raw = segments.path().join("compact-labels.json");
    let mut compact_config = shared_config.clone();
    compact_config.output = segments.path().join("compact-labels.md");
    compact_config.raw_output = Some(compact_raw.clone());

    let shared_report = run_query_benchmark_with_experimental_flow(
        &shared_config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::SharedAtoms,
        StorageLayoutArg::Schema8,
    )
    .unwrap();
    let owned_report = run_query_benchmark_with_experimental_flow(
        &owned_config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::OwnedStrings,
        StorageLayoutArg::Schema8,
    )
    .unwrap();
    let compact_report = run_query_benchmark_with_experimental_flow(
        &compact_config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::CompactIds,
        StorageLayoutArg::Schema8,
    )
    .unwrap();

    let shared: serde_json::Value = serde_json::from_slice(&fs::read(shared_raw).unwrap()).unwrap();
    let owned: serde_json::Value = serde_json::from_slice(&fs::read(owned_raw).unwrap()).unwrap();
    let compact: serde_json::Value =
        serde_json::from_slice(&fs::read(compact_raw).unwrap()).unwrap();

    assert_eq!(shared["schema"], "chronoxide.query-benchmark.raw/v14");
    assert_eq!(owned["schema"], "chronoxide.query-benchmark.raw/v14");
    assert_eq!(compact["schema"], "chronoxide.query-benchmark.raw/v14");
    assert_eq!(
        shared["configuration"]["query_label_storage"],
        "shared-atoms"
    );
    assert_eq!(
        owned["configuration"]["query_label_storage"],
        "owned-strings"
    );
    assert_eq!(
        compact["configuration"]["query_label_storage"],
        "compact-ids"
    );

    assert_eq!(shared_report.results.len(), 1);
    assert_eq!(owned_report.results.len(), 1);
    assert_eq!(compact_report.results.len(), 1);
    let shared_result = &shared_report.results[0];
    let owned_result = &owned_report.results[0];
    let compact_result = &compact_report.results[0];
    assert_eq!(
        shared_result.semantic_fingerprint,
        owned_result.semantic_fingerprint
    );
    assert_eq!(
        shared_result.portable_semantic_fingerprint,
        owned_result.portable_semantic_fingerprint
    );
    assert_eq!(shared_result.stats, owned_result.stats);
    assert_eq!(
        compact_result.semantic_fingerprint,
        owned_result.semantic_fingerprint
    );
    assert_eq!(
        compact_result.portable_semantic_fingerprint,
        owned_result.portable_semantic_fingerprint
    );
    assert_eq!(compact_result.stats, owned_result.stats);

    let shared_labels = &shared["runs"][0]["query_label_storage"];
    let shared_lookups = shared_labels["atom_lookups"].as_u64().unwrap();
    let shared_hits = shared_labels["atom_hits"].as_u64().unwrap();
    let shared_misses = shared_labels["atom_misses"].as_u64().unwrap();
    assert!(shared_labels["label_sets"].as_u64().unwrap() > 0);
    assert!(shared_lookups > 0);
    assert_eq!(shared_lookups, shared_hits + shared_misses);
    assert!(shared_labels["unique_content_bytes"].as_u64().unwrap() > 0);

    let owned_labels = &owned["runs"][0]["query_label_storage"];
    assert!(owned_labels["label_sets"].as_u64().unwrap() > 0);
    assert_eq!(owned_labels["atom_lookups"], 0);
    assert_eq!(owned_labels["atom_hits"], 0);
    assert_eq!(owned_labels["atom_misses"], 0);
    assert_eq!(owned_labels["unique_content_bytes"], 0);

    let compact_labels = &compact["runs"][0]["query_label_storage"];
    assert!(compact_labels["compact_label_sets"].as_u64().unwrap() > 0);
    assert!(compact_labels["compact_pairs"].as_u64().unwrap() > 0);
    let translations = compact_labels["compact_source_symbol_translations"]
        .as_u64()
        .unwrap();
    let translation_hits = compact_labels["compact_source_symbol_translation_hits"]
        .as_u64()
        .unwrap();
    let translation_misses = compact_labels["compact_source_symbol_translation_misses"]
        .as_u64()
        .unwrap();
    assert!(translations > 0);
    assert_eq!(translations, translation_hits + translation_misses);
    let compact_atom_lookups = compact_labels["compact_atom_lookups"].as_u64().unwrap();
    assert!(compact_atom_lookups > 0);
    assert_eq!(
        compact_atom_lookups,
        compact_labels["compact_atom_hits"].as_u64().unwrap()
            + compact_labels["compact_atom_misses"].as_u64().unwrap()
    );
    assert!(compact_labels["compact_unique_strings"].as_u64().unwrap() > 0);
    assert_eq!(compact_labels["compact_compatibility_materializations"], 0);
    assert_eq!(
        compact_labels["compact_arena_budget_bytes"],
        DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES
    );
    assert!(
        compact_labels["compact_retained_bytes"].as_u64().unwrap()
            <= DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES
    );
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
fn raw_benchmark_symbol_reads_cover_stats_and_retained_resources() {
    let raw = serde_json::to_value(QueryBenchmarkRawSymbolReadsV5::from(
        SegmentStoreQueryProfile {
            symbol_read_stats: SegmentSymbolReadStats {
                legacy_eager: SegmentSymbolReadCount {
                    calls: 1,
                    bytes: 99,
                },
                logical_returned: SegmentSymbolReadCount {
                    calls: 8,
                    bytes: 88,
                },
                root: SegmentSymbolReadCount {
                    calls: 1,
                    bytes: 80,
                },
                page: SegmentSymbolReadCount {
                    calls: 2,
                    bytes: 65_536,
                },
                page_validation: SegmentSymbolReadCount {
                    calls: 2,
                    bytes: 65_536,
                },
                page_validation_ns: 42,
                touched_corrupt_pages: 1,
                page_cache_hits: 3,
                page_cache_misses: 4,
                page_cache_evictions: 5,
            },
            symbol_resources: chronoxide_core::storage::segment::SegmentStoreSymbolResources {
                retained_readers: 10,
                retained_open_files: 9,
                source_file_bytes: 8,
                root_encoded_bytes: 7,
                root_retained_charge_bytes: 6,
                eager_dictionary_retained_charge_bytes: 5,
                page_cache_charge_bytes: 4,
                page_cache_max_bytes: 3,
                snapshot_errors: 2,
            },
            ..SegmentStoreQueryProfile::default()
        },
    ))
    .unwrap();

    assert_eq!(
        raw,
        serde_json::json!({
            "legacy_eager_read_delta": {"calls": 1, "bytes": 99},
            "logical_returned_delta": {"calls": 8, "bytes": 88},
            "root_read_delta": {"calls": 1, "bytes": 80},
            "page_read_delta": {"calls": 2, "bytes": 65_536},
            "page_validation_delta": {"calls": 2, "bytes": 65_536},
            "page_validation_ns_delta": 42,
            "touched_corrupt_pages_delta": 1,
            "page_cache_hits_delta": 3,
            "page_cache_misses_delta": 4,
            "page_cache_evictions_delta": 5,
            "retained_readers_after_run": 10,
            "retained_open_files_after_run": 9,
            "source_file_bytes_after_run": 8,
            "root_encoded_bytes_after_run": 7,
            "root_retained_charge_bytes_after_run": 6,
            "eager_dictionary_retained_charge_bytes_after_run": 5,
            "page_cache_charge_bytes_after_run": 4,
            "page_cache_max_bytes_after_run": 3,
            "total_retained_charge_bytes_after_run": 15,
            "resource_snapshot_errors_after_run": 2
        })
    );
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
    let raw = serde_json::to_value(QueryBenchmarkRawRangeScalarCacheV3::from(cache)).unwrap();
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
            "process_governor_lifetime_peak_leased_bytes": 16
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
        post_query_fingerprint: Duration::ZERO,
        effective_start_ms: 0,
        effective_end_ms: 0,
        step_ms: Some(1),
        semantic_fingerprint,
        portable_semantic_fingerprint: semantic_fingerprint,
        result_series: 0,
        result_samples: 0,
        stats: QueryStats::default(),
        session_stats_delta: SegmentStoreQuerySessionStats::default(),
        session_profile_delta: SegmentStoreQueryProfile::default(),
        label_storage_delta: QueryLabelStorageStats::default(),
        metadata_runtime: QueryBenchmarkMetadataRuntimeReport::default(),
        range_scalar_cache: Some(cache),
        range_execution: None,
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
        "process_governor_lifetime_peak_leased_bytes",
    ] {
        assert!(markdown.contains(field), "missing Markdown field {field}");
    }
    assert!(markdown.contains("sampled after the range query finalizes"));
    assert!(markdown.contains("process-lifetime high-water mark, not a per-run peak or delta"));
    assert!(markdown.contains("| `time()` | Warm | 2 | 1 | 2 | true | false | true | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 |"));
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
