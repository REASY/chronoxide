use super::*;

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
    let results = open_segment_store_for_layout_ab(
        tempdir.path(),
        false,
        query_projection_config(&[]),
        StorageLayoutArg::Schema8,
    )
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
        post_query_fingerprint: Duration::ZERO,
        effective_start_ms: 0,
        effective_end_ms: 0,
        step_ms: None,
        semantic_fingerprint,
        portable_semantic_fingerprint: semantic_fingerprint,
        result_series: 0,
        result_samples: 0,
        stats: QueryStats::default(),
        session_stats_delta: SegmentStoreQuerySessionStats::default(),
        session_profile_delta: SegmentStoreQueryProfile {
            index_read_stats: sample_index_read_stats(1),
            ..SegmentStoreQueryProfile::default()
        },
        label_storage_delta: QueryLabelStorageStats {
            label_sets: 7,
            atom_lookups: 42,
            atom_hits: 31,
            atom_misses: 11,
            unique_content_bytes: 128,
            compact_label_sets: 5,
            compact_pairs: 12,
            compact_source_symbol_translations: 20,
            compact_source_symbol_translation_hits: 14,
            compact_source_symbol_translation_misses: 6,
            compact_atom_lookups: 18,
            compact_atom_hits: 13,
            compact_atom_misses: 5,
            compact_unique_strings: 9,
            compact_unique_content_bytes: 90,
            compact_arena_budget_bytes: 1_000,
            compact_arena_current_bytes: 500,
            compact_arena_peak_bytes: 600,
            compact_atom_bytes: 200,
            compact_pair_bytes: 100,
            compact_hash_directory_bytes: 120,
            compact_translation_bytes: 80,
            compact_retained_bytes: 500,
            compact_arena_admission_refusals: 2,
            compact_compatibility_materializations: 3,
        },
        metadata_runtime: QueryBenchmarkMetadataRuntimeReport::default(),
        range_scalar_cache: None,
        range_execution: None,
    }];
    let mut markdown = String::new();

    render_query_result_index_positional_reads(&mut markdown, &results);

    assert!(markdown.contains("## Query Result Index Positional Reads"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 2 | Root | 1 | 10 |"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 2 | Exact Page | 4 | 40 |"));
    assert!(markdown.contains("| `cpu.usage` | Warm | 2 | Total | 21 | 210 |"));

    let mut label_markdown = String::new();
    render_query_label_storage(&mut label_markdown, &results);
    assert!(label_markdown.contains("## Experimental Query Label Storage"));
    assert!(label_markdown.contains(
        "| `cpu.usage` | Warm | 2 | 7 | 42 | 31 | 11 | 128 | 5 | 12 | 20 | 14 | 6 | 18 | 13 | 5 | 9 | 90 |"
    ));
    assert!(label_markdown.contains(
        "| `cpu.usage` | Warm | 2 | 1000 | 500 | 600 | 200 | 100 | 120 | 80 | 500 | 2 | 3 |"
    ));
}

#[test]
fn compact_query_label_accounting_rejects_unreconciled_counters_and_charges() {
    let valid = QueryLabelStorageStats {
        compact_source_symbol_translations: 2,
        compact_source_symbol_translation_hits: 1,
        compact_source_symbol_translation_misses: 1,
        compact_atom_lookups: 4,
        compact_atom_hits: 3,
        compact_atom_misses: 1,
        compact_arena_budget_bytes: 100,
        compact_arena_current_bytes: 50,
        compact_arena_peak_bytes: 60,
        compact_atom_bytes: 10,
        compact_pair_bytes: 11,
        compact_hash_directory_bytes: 12,
        compact_translation_bytes: 17,
        compact_retained_bytes: 50,
        ..QueryLabelStorageStats::default()
    };
    validate_query_label_storage_stats(valid).unwrap();

    assert!(
        validate_query_label_storage_stats(QueryLabelStorageStats {
            compact_atom_lookups: 5,
            ..valid
        })
        .unwrap_err()
        .to_string()
        .contains("atom counters do not reconcile")
    );
    assert!(
        validate_query_label_storage_stats(QueryLabelStorageStats {
            compact_source_symbol_translations: 3,
            ..valid
        })
        .unwrap_err()
        .to_string()
        .contains("translation counters do not reconcile")
    );
    assert!(
        validate_query_label_storage_stats(QueryLabelStorageStats {
            compact_retained_bytes: 51,
            ..valid
        })
        .unwrap_err()
        .to_string()
        .contains("retained bytes do not reconcile")
    );
    assert!(
        validate_query_label_storage_stats(QueryLabelStorageStats {
            compact_arena_peak_bytes: 101,
            ..valid
        })
        .unwrap_err()
        .to_string()
        .contains("peak charge exceeds budget")
    );
}

#[test]
fn add_session_profile_accumulates_counters_but_keeps_latest_resource_snapshot() {
    let mut total = SegmentStoreQueryProfile {
        index_read_stats: sample_index_read_stats(2),
        symbol_read_stats: sample_symbol_read_stats(2),
        symbol_resources: SegmentStoreSymbolResources {
            retained_readers: 1,
            retained_open_files: 1,
            root_retained_charge_bytes: 100,
            page_cache_max_bytes: 200,
            ..SegmentStoreSymbolResources::default()
        },
        chunk_read_scheduler: ChunkReadSchedulerProfile {
            executions: 1,
            pread_decisions: 1,
            submission_depth_max: 1,
            peak_in_flight_bytes: 100,
            ..ChunkReadSchedulerProfile::default()
        },
        stages: QueryStageProfile {
            canonical_row_decode: Duration::from_nanos(2),
            candidate_selection: Duration::from_nanos(11),
            metadata_visit_overhead: Duration::from_nanos(13),
            payload_io: Duration::from_nanos(3),
            ..QueryStageProfile::default()
        },
        ..SegmentStoreQueryProfile::default()
    };

    add_session_profile(
        &mut total,
        SegmentStoreQueryProfile {
            index_read_stats: sample_index_read_stats(3),
            symbol_read_stats: sample_symbol_read_stats(3),
            symbol_resources: SegmentStoreSymbolResources {
                retained_readers: 2,
                retained_open_files: 2,
                root_retained_charge_bytes: 300,
                page_cache_max_bytes: 400,
                ..SegmentStoreSymbolResources::default()
            },
            chunk_read_scheduler: ChunkReadSchedulerProfile {
                executions: 2,
                io_uring_decisions: 2,
                submission_depth_max: 8,
                peak_in_flight_bytes: 800,
                ..ChunkReadSchedulerProfile::default()
            },
            stages: QueryStageProfile {
                canonical_row_decode: Duration::from_nanos(5),
                candidate_selection: Duration::from_nanos(17),
                metadata_visit_overhead: Duration::from_nanos(19),
                payload_io: Duration::from_nanos(7),
                ..QueryStageProfile::default()
            },
            ..SegmentStoreQueryProfile::default()
        },
    );

    assert_eq!(total.index_read_stats, sample_index_read_stats(5));
    assert_eq!(total.symbol_read_stats, sample_symbol_read_stats(5));
    assert_eq!(total.symbol_resources.retained_readers, 2);
    assert_eq!(total.symbol_resources.retained_open_files, 2);
    assert_eq!(total.symbol_resources.root_retained_charge_bytes, 300);
    assert_eq!(total.symbol_resources.page_cache_max_bytes, 400);
    assert_eq!(total.chunk_read_scheduler.executions, 3);
    assert_eq!(total.chunk_read_scheduler.pread_decisions, 1);
    assert_eq!(total.chunk_read_scheduler.io_uring_decisions, 2);
    assert_eq!(total.chunk_read_scheduler.submission_depth_max, 8);
    assert_eq!(total.chunk_read_scheduler.peak_in_flight_bytes, 800);
    assert_eq!(total.stages.canonical_row_decode, Duration::from_nanos(7));
    assert_eq!(total.stages.candidate_selection, Duration::from_nanos(28));
    assert_eq!(
        total.stages.metadata_visit_overhead,
        Duration::from_nanos(32)
    );
    assert_eq!(total.stages.payload_io, Duration::from_nanos(10));

    add_session_profile(&mut total, SegmentStoreQueryProfile::default());
    assert_eq!(
        total.symbol_resources,
        SegmentStoreSymbolResources::default()
    );
}

#[test]
fn query_stage_accounting_rejects_off_observation_and_detailed_over_attribution() {
    let stages = QueryStageProfile {
        candidate_selection: Duration::from_nanos(2),
        ..QueryStageProfile::default()
    };

    let off = validate_query_stage_accounting(
        QueryInstrumentationArg::Off,
        "cpu.usage",
        Duration::from_nanos(10),
        stages,
    )
    .unwrap_err();
    assert_eq!(off.kind(), ErrorKind::InvalidData);
    assert!(off.to_string().contains("instrumentation is off"));

    validate_query_stage_accounting(
        QueryInstrumentationArg::Detailed,
        "cpu.usage",
        Duration::from_nanos(2),
        stages,
    )
    .unwrap();
    let detailed = validate_query_stage_accounting(
        QueryInstrumentationArg::Detailed,
        "cpu.usage",
        Duration::from_nanos(1),
        stages,
    )
    .unwrap_err();
    assert_eq!(detailed.kind(), ErrorKind::InvalidData);
    assert!(detailed.to_string().contains("exceeds timed query wall"));
}

#[test]
fn metadata_runtime_report_separates_counter_deltas_boundary_gauges_and_lifetime_peaks() {
    let mut before = StoreMetadataRuntimeSnapshot::default();
    before.cache.hits = 10;
    before.cache.misses = 9;
    before.cache.successful_loads = 20;
    before.cache.resident_admissions = 3;
    before.cache.resident_admission_refusals = 4;
    before.cache.resident_admission_bypasses = 5;
    before.cache.class_admissions[0].resident_admissions = 2;
    before.cache.resident_entries = 13;
    before.cache.ledger_retained_bytes = 14;
    before.cache.class_charges[0].retained_bytes = 15;
    before.governor.retained_refusals = 7;
    before.governor.in_flight_refusals = 8;
    before.governor.retained_max_bytes = 900;
    before.governor.retained_bytes = 16;
    before.governor.usage[0].retained_bytes = 17;
    before.files.acquire_calls = 30;
    before.files.descriptor_opens = 40;
    before.files.max_open_files = 63;
    before.files.open_files = 4;
    before.reads.issued.calls = 11;
    before.reads.issued.bytes = 1_100;
    before.reads.unclassified.calls = 2;
    before.reads.unclassified.bytes = 200;
    before.reads.files[0].issued.calls = 3;
    before.reads.files[0].issued.bytes = 300;
    before.reads.classes[0].issued.calls = 4;
    before.reads.classes[0].issued.bytes = 400;

    let mut after = before;
    after.cache.hits = 16;
    after.cache.misses = 3;
    after.cache.successful_loads = 25;
    after.cache.resident_admissions = 7;
    after.cache.resident_admission_refusals = 6;
    after.cache.resident_admission_bypasses = 8;
    after.cache.class_admissions[0].resident_admissions = 5;
    after.cache.class_admissions[0].resident_admission_refusals = 2;
    after.cache.class_admissions[0].resident_admission_bypasses = 1;
    after.cache.resident_entries = 17;
    after.cache.ledger_retained_bytes = 18;
    after.cache.class_charges[0].in_flight_bytes = 19;
    after.cache.class_charges[0].retained_bytes = 20;
    after.cache.class_charges[0].peak_in_flight_bytes = 21;
    after.cache.class_charges[0].peak_retained_bytes = 22;
    after.governor.retained_refusals = 12;
    after.governor.in_flight_refusals = 10;
    after.governor.retained_max_bytes = 1_000;
    after.governor.retained_bytes = 23;
    after.governor.peak_retained_bytes = 24;
    after.governor.usage[0].retained_bytes = 25;
    after.governor.usage[0].peak_retained_bytes = 26;
    after.files.acquire_calls = 37;
    after.files.descriptor_opens = 44;
    after.files.max_open_files = 64;
    after.files.open_files = 5;
    after.files.peak_open_files = 6;
    after.reads.issued.calls = 16;
    after.reads.issued.bytes = 1_650;
    after.reads.unclassified.calls = 3;
    after.reads.unclassified.bytes = 250;
    after.reads.files[0].issued.calls = 7;
    after.reads.files[0].issued.bytes = 750;
    after.reads.classes[0].issued.calls = 10;
    after.reads.classes[0].issued.bytes = 1_000;

    let report = QueryBenchmarkMetadataRuntimeReport::between(before, after);

    let cache = &report.counters_delta.cache;
    assert_eq!(cache.hits, 6);
    assert_eq!(cache.misses, 0, "counter rollback must saturate");
    assert_eq!(cache.successful_loads, 5);
    assert_eq!(cache.resident_admissions, 4);
    assert_eq!(cache.resident_admission_refusals, 2);
    assert_eq!(cache.resident_admission_bypasses, 3);
    assert_eq!(cache.class_admissions[0].class, "symbol_root");
    assert_eq!(cache.class_admissions[0].resident_admissions, 3);
    assert_eq!(cache.class_admissions[0].resident_admission_refusals, 2);
    assert_eq!(cache.class_admissions[0].resident_admission_bypasses, 1);
    assert_eq!(report.counters_delta.governor.retained_refusals, 5);
    assert_eq!(report.counters_delta.governor.in_flight_refusals, 2);
    assert_eq!(report.counters_delta.file_manager.acquire_calls, 7);
    assert_eq!(report.counters_delta.file_manager.descriptor_opens, 4);
    assert_eq!(report.counters_delta.reads.issued.calls, 5);
    assert_eq!(report.counters_delta.reads.issued.bytes, 550);
    assert_eq!(report.counters_delta.reads.unclassified.calls, 1);
    assert_eq!(report.counters_delta.reads.by_file[0].file, "meta.json");
    assert_eq!(report.counters_delta.reads.by_file[0].calls, 4);
    assert_eq!(report.counters_delta.reads.by_file[0].bytes, 450);
    assert_eq!(report.counters_delta.reads.by_class[0].class, "symbol_root");
    assert_eq!(report.counters_delta.reads.by_class[0].calls, 6);
    assert_eq!(report.counters_delta.reads.by_class[0].bytes, 600);

    assert_eq!(report.start_gauges.cache.resident_entries, 13);
    assert_eq!(report.start_gauges.cache.ledger_retained_bytes, 14);
    assert_eq!(
        report.start_gauges.cache.class_charges[0].retained_bytes,
        15
    );
    assert_eq!(report.start_gauges.governor.retained_max_bytes, 900);
    assert_eq!(report.start_gauges.governor.retained_bytes, 16);
    assert_eq!(
        report.start_gauges.governor.usage_charges[0].retained_bytes,
        17
    );
    assert_eq!(report.start_gauges.file_manager.max_open_files, 63);
    assert_eq!(report.start_gauges.file_manager.open_files, 4);

    assert_eq!(report.end_gauges.cache.resident_entries, 17);
    assert_eq!(report.end_gauges.cache.ledger_retained_bytes, 18);
    assert_eq!(report.end_gauges.cache.class_charges[0].retained_bytes, 20);
    assert_eq!(report.end_gauges.governor.retained_max_bytes, 1_000);
    assert_eq!(report.end_gauges.governor.retained_bytes, 23);
    assert_eq!(
        report.end_gauges.governor.usage_charges[0].usage,
        "unclassified"
    );
    assert_eq!(
        report.end_gauges.governor.usage_charges[0].retained_bytes,
        25
    );
    assert_eq!(report.end_gauges.file_manager.max_open_files, 64);
    assert_eq!(report.end_gauges.file_manager.open_files, 5);

    assert_eq!(
        report.lifetime_peaks_after_run.cache_class_charges[0].peak_retained_bytes,
        22
    );
    assert_eq!(
        report.lifetime_peaks_after_run.governor.peak_retained_bytes,
        24
    );
    assert_eq!(
        report.lifetime_peaks_after_run.governor.usage_charges[0].peak_retained_bytes,
        26
    );
    assert_eq!(
        report.lifetime_peaks_after_run.file_manager.peak_open_files,
        6
    );

    let mut rollback_before = StoreMetadataRuntimeSnapshot::default();
    rollback_before.cache.resident_admissions = 9;
    rollback_before.cache.resident_admission_refusals = 8;
    rollback_before.cache.resident_admission_bypasses = 7;
    rollback_before.cache.class_admissions[0].resident_admissions = 6;
    rollback_before.cache.class_admissions[0].resident_admission_refusals = 5;
    rollback_before.cache.class_admissions[0].resident_admission_bypasses = 4;
    let rollback_after = StoreMetadataRuntimeSnapshot::default();

    let rollback = QueryBenchmarkMetadataRuntimeReport::between(rollback_before, rollback_after);
    assert_eq!(rollback.counters_delta.cache.resident_admissions, 0);
    assert_eq!(rollback.counters_delta.cache.resident_admission_refusals, 0);
    assert_eq!(rollback.counters_delta.cache.resident_admission_bypasses, 0);
    assert_eq!(
        rollback.counters_delta.cache.class_admissions[0].resident_admissions,
        0
    );
    assert_eq!(
        rollback.counters_delta.cache.class_admissions[0].resident_admission_refusals,
        0
    );
    assert_eq!(
        rollback.counters_delta.cache.class_admissions[0].resident_admission_bypasses,
        0
    );
}

#[test]
fn render_profile_table_reports_chunk_read_scheduler() {
    let mut markdown = String::new();
    render_profile_table(
        &mut markdown,
        "Test Read Profile",
        SegmentStoreQueryProfile {
            chunk_read_scheduler: ChunkReadSchedulerProfile {
                executions: 2,
                pread_decisions: 1,
                io_uring_decisions: 1,
                logical_requests: 20,
                physical_spans: 9,
                backend_submissions: 2,
                sqes_submitted: 9,
                submission_depth_sum: 9,
                submission_depth_max: 8,
                submission_depth_1: 1,
                submission_depth_8_plus: 1,
                total_physical_bytes_executed: 4_096,
                peak_in_flight_bytes: 3_072,
                ..ChunkReadSchedulerProfile::default()
            },
            ..SegmentStoreQueryProfile::default()
        },
    );

    assert!(markdown.contains("## Test Chunk Read Scheduler"));
    assert!(markdown.contains("| io_uring Decisions | 1 |"));
    assert!(markdown.contains("| Logical Requests | 20 |"));
    assert!(markdown.contains("| Mean Submission Depth | 4.500 |"));
    assert!(markdown.contains("| Maximum Submission Depth | 8 |"));
    assert!(markdown.contains("| Peak In-Flight Bytes | 3072 |"));
}

#[test]
fn render_profile_table_reports_symbol_reads_and_page_cache() {
    let profile = SegmentStoreQueryProfile {
        symbol_read_stats: SegmentSymbolReadStats {
            legacy_eager: SegmentSymbolReadCount::default(),
            logical_returned: SegmentSymbolReadCount {
                calls: 10,
                bytes: 1_024,
            },
            root: SegmentSymbolReadCount {
                calls: 2,
                bytes: 160,
            },
            page: SegmentSymbolReadCount {
                calls: 3,
                bytes: 98_304,
            },
            page_validation: SegmentSymbolReadCount {
                calls: 3,
                bytes: 98_304,
            },
            page_validation_ns: 42,
            touched_corrupt_pages: 1,
            page_cache_hits: 7,
            page_cache_misses: 3,
            page_cache_evictions: 1,
        },
        symbol_resources: SegmentStoreSymbolResources {
            retained_readers: 2,
            retained_open_files: 2,
            source_file_bytes: 400_000,
            root_encoded_bytes: 2_000,
            root_retained_charge_bytes: 4_000,
            eager_dictionary_retained_charge_bytes: 8_000,
            page_cache_charge_bytes: 65_536,
            page_cache_max_bytes: 262_144,
            snapshot_errors: 1,
        },
        ..SegmentStoreQueryProfile::default()
    };
    let mut markdown = String::new();

    render_profile_table(&mut markdown, "Test Read Profile", profile);

    assert!(markdown.contains("## Test Symbol Reads And Page Cache"));
    assert!(markdown.contains("| Root Read Requests | 2 |"));
    assert!(markdown.contains("| Logical Values Returned | 10 |"));
    assert!(markdown.contains("| Logical UTF-8 Bytes Returned | 1024 |"));
    assert!(markdown.contains("| Root Read Bytes | 160 |"));
    assert!(markdown.contains("| Page Read Requests | 3 |"));
    assert!(markdown.contains("| Page Read Bytes | 98304 |"));
    assert!(markdown.contains("| Page Read / Logical UTF-8 Amplification | 96.000x |"));
    assert!(markdown.contains("| Successful Page Validations | 3 |"));
    assert!(markdown.contains("| Successfully Validated Page Bytes | 98304 |"));
    assert!(markdown.contains("| Page Validation Duration | 42ns |"));
    assert!(markdown.contains("| Touched Corrupt Pages | 1 |"));
    assert!(markdown.contains("| Page Cache Hits | 7 |"));
    assert!(markdown.contains("| Page Cache Misses | 3 |"));
    assert!(markdown.contains("| Page Cache Evictions | 1 |"));
    assert!(markdown.contains("| Retained Symbol Readers | 2 |"));
    assert!(markdown.contains("| Retained Symbol Open Files | 2 |"));
    assert!(markdown.contains("| Retained Root Charge Bytes | 4000 |"));
    assert!(markdown.contains("| Retained Eager Dictionary Charge Bytes | 8000 |"));
    assert!(markdown.contains("| Page Cache Charge Bytes | 65536 |"));
    assert!(markdown.contains("| Page Cache Max Bytes | 262144 |"));
    assert!(markdown.contains("| Total Retained Symbol Charge Bytes | 77536 |"));
    assert!(markdown.contains("| Resource Snapshot Errors | 1 |"));
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

    let store = open_segment_store_for_layout_ab(
        tempdir.path(),
        false,
        query_projection_config(&[]),
        StorageLayoutArg::Schema8,
    )
    .unwrap();
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

    let markdown = render_markdown(&config, StorageLayoutArg::Schema8, &report, None, None);

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

    let store = open_segment_store_for_layout_ab(
        tempdir.path(),
        false,
        query_projection_config(&[]),
        StorageLayoutArg::Schema8,
    )
    .unwrap();
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
            multi_step_range_expected_queries: 3,
            multi_step_range_executed_queries: 2,
            multi_step_range_skipped_queries: 1,
            skip_reasons: BTreeMap::from([("fixture isolation ambiguous".to_string(), 2)]),
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

    let markdown = render_markdown(
        &config,
        StorageLayoutArg::Schema8,
        &report,
        None,
        Some(&diagnostics),
    );

    assert!(markdown.contains("## Query Diagnostics"));
    assert!(markdown.contains("| Store Open |"));
    assert!(markdown.contains("| Smoke Verify |"));
    assert!(markdown.contains("| Collect Expected Readbacks |"));
    assert!(markdown.contains("| Segment Context Opens | 9 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 2 |"));
    assert!(markdown.contains("| fixture isolation ambiguous | 2 |"));
    assert!(markdown.contains("| Isolation Check Skips | 2 |"));
    assert!(markdown.contains("| Multi-Step Range Readbacks Expected | 3 |"));
    assert!(markdown.contains("| Multi-Step Range Readbacks Executed | 2 |"));
    assert!(markdown.contains("| Multi-Step Range Readbacks Skipped | 1 |"));
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
    let markdown = fs::read_to_string(&config.output).unwrap();
    let summary_row = markdown
        .lines()
        .find(|line| line.starts_with("| `cpu.usage` | 1 | 0 |"))
        .unwrap();

    assert!(markdown.contains("| Warm Mean | Warm Median | Warm Min | Warm Max |"));
    assert_eq!(summary_row.matches("n/a").count(), 4);
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
