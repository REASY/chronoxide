use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::chunk::{ChunkIndexEntry, ChunkWriter};
use chronoxide_core::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OtlpAggregationTemporality, SummaryQuantileValue, SummaryValue, TypedSampleMetadata,
};
use chronoxide_core::storage::index::{SegmentIndexReadCount, SegmentIndexReadStats};
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, write_current,
};
use chronoxide_core::storage::segment::{
    ChunkReadSchedulerProfile, SegmentMeta, SegmentStorageSchema, SegmentWriter,
    SegmentWriterConfig,
};

use super::*;

fn json_object_keys(value: &serde_json::Value) -> BTreeSet<&str> {
    value
        .as_object()
        .expect("JSON value must be an object")
        .keys()
        .map(String::as_str)
        .collect()
}

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

fn sample_symbol_read_stats(multiplier: u64) -> SegmentSymbolReadStats {
    SegmentSymbolReadStats {
        legacy_eager: SegmentSymbolReadCount::default(),
        logical_returned: SegmentSymbolReadCount::default(),
        root: SegmentSymbolReadCount {
            calls: multiplier,
            bytes: multiplier * 10,
        },
        page: SegmentSymbolReadCount {
            calls: multiplier * 2,
            bytes: multiplier * 20,
        },
        page_validation: SegmentSymbolReadCount {
            calls: multiplier * 2,
            bytes: multiplier * 20,
        },
        page_validation_ns: multiplier * 30,
        touched_corrupt_pages: multiplier * 6,
        page_cache_hits: multiplier * 3,
        page_cache_misses: multiplier * 4,
        page_cache_evictions: multiplier * 5,
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
        },
        metadata_runtime: QueryBenchmarkMetadataRuntimeReport::default(),
        range_scalar_cache: None,
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
    assert!(label_markdown.contains("| `cpu.usage` | Warm | 2 | 7 | 42 | 31 | 11 | 128 |"));
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
                in_flight_bytes: 4_096,
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
fn schema7_independent_readback_oracle_decodes_every_inline_kind() {
    let tempdir = schema7_segment_store_with_all_inline_kinds();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: vec![2.0],
        validate_segment_footers: false,
    };

    let expected =
        collect_expected_readbacks(&config, StorageLayoutArg::Schema7, &[true; 5]).unwrap();
    let queries = expected
        .iter()
        .map(|readback| readback.query.as_str())
        .collect::<Vec<_>>();

    assert_eq!(expected.len(), 21);
    for metric in [
        "schema7_float",
        "schema7_int64",
        "schema7_histogram",
        "schema7_exponential_histogram",
        "schema7_summary",
    ] {
        assert!(
            queries.iter().any(|query| query.contains(metric)),
            "missing independent readback for {metric}: {queries:?}"
        );
    }

    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let series = fs::read(segment_dir.join(SegmentFile::Series.filename())).unwrap();
    let chunk_index = fs::read(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    assert_eq!(u16::from_le_bytes([series[4], series[5]]), 3);
    assert_eq!(u16::from_le_bytes([chunk_index[4], chunk_index[5]]), 2);
    assert_eq!(
        u32::from_le_bytes(chunk_index[24..28].try_into().unwrap()),
        0,
        "one chunk per series must remain inline"
    );
}

#[test]
fn schema7_smoke_reader_and_independent_oracle_execute_every_inline_kind() {
    let tempdir = schema7_segment_store_with_all_inline_kinds();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: vec![2.0],
        validate_segment_footers: false,
    };

    let report = run_query_smoke_with_storage_layout(&config, StorageLayoutArg::Schema7).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert_eq!(report.sample_series.len(), 5);
    for kind in [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ] {
        assert!(
            report
                .sample_series
                .iter()
                .any(|sample| sample.kind == kind)
        );
    }
    assert!(markdown.contains("| Checked Queries | 21 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
    assert!(markdown.contains("| Expected Readback Queries | 21 |"));
    assert!(markdown.contains("| Executed Readback Queries | 21 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 0 |"));
    assert!(markdown.contains("| Isolation Check Skips | 0 |"));
}

#[test]
fn schema8_smoke_reader_and_independent_oracle_execute_every_inline_kind() {
    let tempdir = schema8_segment_store_with_all_inline_kinds();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema8_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: vec![2.0],
        validate_segment_footers: true,
    };

    let report = run_query_smoke_with_storage_layout(&config, StorageLayoutArg::Schema8).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();

    assert_eq!(report.sample_series.len(), 5);
    for kind in [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ] {
        assert!(
            report
                .sample_series
                .iter()
                .any(|sample| sample.kind == kind)
        );
    }
    assert!(markdown.contains("| Checked Queries | 21 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
    assert!(markdown.contains("| Expected Readback Queries | 21 |"));
    assert!(markdown.contains("| Executed Readback Queries | 21 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 0 |"));
    assert!(markdown.contains("| Isolation Check Skips | 0 |"));
}

#[test]
fn schema7_independent_readback_oracle_decodes_multi_chunk_overflow() {
    let tempdir = schema7_segment_store_with_float_overflow();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_overflow_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 2,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    let expected = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema7,
        &[true, false, false, false, false],
    )
    .unwrap();
    assert_eq!(expected.len(), 5);
    assert!(
        expected
            .iter()
            .all(|readback| readback.query.contains("schema7_overflow"))
    );

    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let chunk_index = fs::read(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    assert_eq!(
        u32::from_le_bytes(chunk_index[24..28].try_into().unwrap()),
        1
    );
    assert_eq!(
        u32::from_le_bytes(chunk_index[80..84].try_into().unwrap()),
        2
    );
}

#[test]
fn schema8_smoke_reader_and_independent_oracle_execute_multi_chunk_overflow() {
    let tempdir = schema8_segment_store_with_float_overflow();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema8_overflow_smoke.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 2,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: true,
    };

    let store = open_segment_store_for_layout_ab(
        tempdir.path(),
        true,
        query_projection_config(&[]),
        StorageLayoutArg::Schema8,
    )
    .unwrap();
    let report = store.smoke_verify(0, 10_000, 2).unwrap();
    let (verification, diagnostics) =
        verify_readbacks(&config, StorageLayoutArg::Schema8, &report).unwrap();

    assert_eq!(report.sample_series.len(), 2);
    assert!(
        verification.mismatches.is_empty(),
        "unexpected readback mismatches: {:#?}",
        verification.mismatches
    );
    assert_eq!(diagnostics.expected_queries, 5);
    assert_eq!(diagnostics.executed_queries, 5);
    assert_eq!(diagnostics.skipped_queries, 0);
    assert_eq!(diagnostics.isolation_check_skips, 0);
}

#[test]
fn schema7_independent_readback_oracle_rejects_corrupt_indexed_prefix() {
    let tempdir = schema7_segment_store_with_all_inline_kinds();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let mut chunks = fs::read(&chunks_path).unwrap();
    for byte in &mut chunks {
        *byte ^= 1;
    }
    fs::write(chunks_path, chunks).unwrap();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_corrupt_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: vec![2.0],
        validate_segment_footers: false,
    };

    let error = collect_expected_readbacks(&config, StorageLayoutArg::Schema7, &[true; 5])
        .expect_err("corrupt authenticated prefix must fail independent readback collection");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "schema-7 oracle indexed prefix CRC mismatch"
    );
}

#[test]
fn schema7_independent_readback_oracle_rejects_authenticated_scalar_flags() {
    let tempdir = schema7_segment_store_with_inline_float();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    set_schema7_inline_chunk_flags(&segment_dir, 0, 0x0001);
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_flagged_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    let error = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema7,
        &[true, false, false, false, false],
    )
    .expect_err("authenticated reserved scalar flags must fail independent readback collection");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "schema-7 oracle scalar chunk flags must be zero"
    );
}

#[test]
fn schema7_independent_readback_oracle_routes_inline_ooo_payload() {
    let tempdir = schema7_segment_store_with_inline_float();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_float_chunk_ordered(0, &[(1_000, 99.0), (2_000, 100.0)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    let original_offset = replace_schema7_inline_locator(&segment_dir, 0, &replacement);
    assert_eq!(replacement.offset, original_offset);

    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_ooo_inline_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };
    let expected = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema7,
        &[true, false, false, false, false],
    )
    .unwrap();

    assert_eq!(expected.len(), 5);
    assert!(
        expected
            .iter()
            .all(|readback| readback.query.contains("schema7_float"))
    );
    assert!(
        expected
            .iter()
            .any(|readback| readback.samples == [(1_000, 99.0), (2_000, 100.0)])
    );

    let report = run_query_smoke_with_storage_layout(&config, StorageLayoutArg::Schema7).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();
    assert_eq!(report.sample_series.len(), 1);
    assert!(markdown.contains("| Checked Queries | 5 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 0 |"));
}

#[test]
fn schema8_independent_readback_oracle_routes_inline_ooo_payload() {
    let tempdir = schema8_segment_store_with_inline_float();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_float_chunk_ordered(0, &[(1_000, 99.0), (2_000, 100.0)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    let original_offset = replace_schema7_inline_locator(&segment_dir, 0, &replacement);
    assert_eq!(replacement.offset, original_offset);

    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema8_ooo_inline_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    let report = run_query_smoke_with_storage_layout(&config, StorageLayoutArg::Schema8).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();
    assert_eq!(report.sample_series.len(), 1);
    assert!(markdown.contains("| Checked Queries | 5 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 0 |"));
    assert!(markdown.contains("| Isolation Check Skips | 0 |"));
}

#[test]
fn schema7_independent_readback_oracle_routes_mixed_overflow_payload_files() {
    let tempdir = schema7_segment_store_with_float_overflow();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_float_chunk_ordered(0, &[(2_000, 99.0), (2_500, 100.0)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    let first_in_order_offset = replace_schema7_overflow_locator(&segment_dir, 1, &replacement);
    assert_eq!(replacement.offset, first_in_order_offset);

    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_ooo_overflow_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 2,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };
    let expected = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema7,
        &[true, false, false, false, false],
    )
    .unwrap();

    assert_eq!(expected.len(), 5);
    assert!(
        expected
            .iter()
            .all(|readback| readback.query.contains("schema7_overflow"))
    );
    assert!(expected.iter().any(|readback| {
        readback.samples
            == [
                (1_000, 1_000.0),
                (1_500, 1_500.0),
                (2_000, 99.0),
                (2_500, 100.0),
            ]
    }));
}

#[test]
fn independent_readback_decoder_routes_chunk_payload_file_ids() {
    let chunks = tempfile::NamedTempFile::new().unwrap();
    let ooo_chunks = tempfile::NamedTempFile::new().unwrap();

    let mut chunks_writer = ChunkWriter::new(chunks.reopen().unwrap()).unwrap();
    let chunks_entry = chunks_writer.append_float_sample(0, 1_000, 1.0).unwrap();
    chunks_writer.flush().unwrap();
    drop(chunks_writer);

    let mut ooo_writer = ChunkWriter::new(ooo_chunks.reopen().unwrap()).unwrap();
    let ooo_entry = ooo_writer.append_float_sample(0, 1_000, 99.0).unwrap();
    ooo_writer.flush().unwrap();
    drop(ooo_writer);

    assert_eq!(ooo_entry.offset, chunks_entry.offset);
    let mut files = [chunks.reopen().unwrap(), ooo_chunks.reopen().unwrap()];
    let record =
        read_chunk_record_from_payload_files(&mut files, 1, ooo_entry.offset, ooo_entry.length)
            .unwrap();
    let ChunkSamples::Float(samples) = record.samples else {
        panic!("expected a float payload");
    };
    assert_eq!(samples, vec![(1_000, 99.0)]);

    let error =
        read_chunk_record_from_payload_files(&mut files, 2, ooo_entry.offset, ooo_entry.length)
            .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "chunk payload file_id must be 0 or 1");
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
    let segments = sorted_segment_metadata(tempdir.path());
    assert_eq!(segments.len(), 2);
    publish_manifest_segments(tempdir.path(), &[&segments[0]]);
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
    let expected =
        collect_expected_readbacks(&config, StorageLayoutArg::Schema8, &required_kinds).unwrap();
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
    assert!(markdown.contains("- Label Materialization: demand-driven"));
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
    assert_eq!(report.storage_layout, StorageLayoutArg::Schema8);
    assert!(raw_text.ends_with('\n'));
    assert_eq!(raw["schema"], "chronoxide.query-benchmark.raw/v10");
    assert!(raw.get("generated_at").is_none());
    assert_eq!(raw["configuration"]["chunk_read_mode"], "pread");
    assert_eq!(raw["configuration"]["chunk_read_queue_depth"], 128);
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
    assert_eq!(raw["configuration"]["query_instrumentation"], "off");
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
            "query_label_storage",
            "range_scalar_cache_max_bytes",
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
fn raw_v10_distinguishes_shared_and_owned_query_label_storage() {
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

    let shared: serde_json::Value = serde_json::from_slice(&fs::read(shared_raw).unwrap()).unwrap();
    let owned: serde_json::Value = serde_json::from_slice(&fs::read(owned_raw).unwrap()).unwrap();

    assert_eq!(shared["schema"], "chronoxide.query-benchmark.raw/v10");
    assert_eq!(owned["schema"], "chronoxide.query-benchmark.raw/v10");
    assert_eq!(
        shared["configuration"]["query_label_storage"],
        "shared-atoms"
    );
    assert_eq!(
        owned["configuration"]["query_label_storage"],
        "owned-strings"
    );

    assert_eq!(shared_report.results.len(), 1);
    assert_eq!(owned_report.results.len(), 1);
    let shared_result = &shared_report.results[0];
    let owned_result = &owned_report.results[0];
    assert_eq!(
        shared_result.semantic_fingerprint,
        owned_result.semantic_fingerprint
    );
    assert_eq!(
        shared_result.portable_semantic_fingerprint,
        owned_result.portable_semantic_fingerprint
    );
    assert_eq!(shared_result.stats, owned_result.stats);

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
    assert_eq!(
        defaults.label_materialization,
        LabelMaterializationArg::DemandDriven
    );
    assert_eq!(defaults.query_label_storage, LabelStorageArg::OwnedStrings);
    assert_eq!(defaults.storage_layout, StorageLayoutArg::Schema8);
    assert_eq!(defaults.query_instrumentation, QueryInstrumentationArg::Off);

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
        "--label-materialization",
        "full",
        "--query-label-storage",
        "shared-atoms",
        "--storage-layout",
        "schema6-ab",
        "--query-instrumentation",
        "detailed",
    ]);
    assert_eq!(overridden.benchmark_repeats, 5);
    assert_eq!(overridden.chunk_read_mode, ChunkReadModeArg::IoUring);
    assert_eq!(overridden.chunk_read_queue_depth, 8);
    assert!(overridden.experimental_cross_segment_chunk_reads);
    assert_eq!(
        overridden.label_materialization,
        LabelMaterializationArg::Full
    );
    assert_eq!(overridden.query_label_storage, LabelStorageArg::SharedAtoms);
    assert_eq!(overridden.storage_layout, StorageLayoutArg::Schema6Ab);
    assert_eq!(
        overridden.query_instrumentation,
        QueryInstrumentationArg::Detailed
    );

    let auto = Args::parse_from([
        "chronoxide-query",
        "--query",
        "cpu.usage",
        "--chunk-read-mode",
        "auto",
    ]);
    assert_eq!(auto.chunk_read_mode, ChunkReadModeArg::Auto);
}

#[test]
fn storage_layout_cli_maps_schema6_schema7_and_schema8_policies() {
    let smoke = Args::parse_from(["chronoxide-query", "--storage-layout", "schema6-ab"]);
    assert_eq!(smoke.storage_layout, StorageLayoutArg::Schema6Ab);
    assert_eq!(
        smoke.storage_layout.core_policy(),
        SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb
    );
    assert!(smoke.storage_layout.forces_footer_validation());

    let benchmark = Args::parse_from([
        "chronoxide-query",
        "--storage-layout",
        "schema6-ab",
        "--query",
        "cpu.usage",
    ]);
    assert_eq!(
        benchmark.storage_layout.core_policy(),
        SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb
    );
    assert!(benchmark.storage_layout.forces_footer_validation());

    let production = Args::parse_from(["chronoxide-query", "--query", "cpu.usage"]);
    assert_eq!(
        production.storage_layout.core_policy(),
        SegmentStoreSchemaPolicy::StrictSchema8
    );
    assert!(!production.storage_layout.forces_footer_validation());

    let schema8 = Args::parse_from([
        "chronoxide-query",
        "--storage-layout",
        "schema8",
        "--query",
        "cpu.usage",
    ]);
    assert_eq!(schema8.storage_layout, StorageLayoutArg::Schema8);
    assert_eq!(
        schema8.storage_layout.core_policy(),
        SegmentStoreSchemaPolicy::StrictSchema8
    );
    assert!(!schema8.storage_layout.forces_footer_validation());
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
    let tempdir = segment_store_with_two_windows_schema7();
    let segments = sorted_segment_metadata(tempdir.path());
    assert_eq!(segments.len(), 2);
    publish_manifest_segments(tempdir.path(), &[&segments[0]]);

    let segment_dir = tempdir.path().join(segments[0].segment_id.clone());
    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let mut chunks = fs::read(&chunks_path).unwrap();
    chunks[0] ^= 0xff;
    fs::write(chunks_path, chunks).unwrap();

    let _store = open_segment_store_for_layout_ab(
        tempdir.path(),
        false,
        query_projection_config(&[]),
        StorageLayoutArg::Schema7,
    )
    .expect("default query open should skip footer checksum validation");

    let err = match open_segment_store_for_layout_ab(
        tempdir.path(),
        true,
        query_projection_config(&[]),
        StorageLayoutArg::Schema7,
    ) {
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
fn schema6_readback_oracle_scopes_queries_to_sampled_chunk_range() {
    let tempdir = segment_store_with_long_float_series(SegmentStorageSchema::Schema6);
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
    let expected =
        collect_expected_readbacks(&config, StorageLayoutArg::Schema6Ab, &required_kinds).unwrap();

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
fn schema8_readback_oracle_scopes_queries_to_selected_series_across_corpus() {
    let tempdir = segment_store_with_long_float_series(SegmentStorageSchema::Schema8);
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
    let expected =
        collect_expected_readbacks(&config, StorageLayoutArg::Schema8, &required_kinds).unwrap();

    assert_eq!(expected.len(), 5);
    assert_eq!(expected[0].start_ms, 0);
    assert_eq!(expected[0].end_ms, 4_999);
    assert_eq!(expected[0].samples.len(), 5_000);
    assert_eq!(expected[1].query, format!("({}) * 2", expected[0].query));
    assert_eq!(expected[1].samples, vec![(4_999, 9_998.0)]);
    assert_eq!(expected[2].query, format!("sum({})", expected[0].query));
    assert_eq!(expected[2].samples, vec![(4_999, 4_999.0)]);
    assert_eq!(
        expected[3].query,
        format!("rate({}[5000ms])", expected[0].query)
    );
    assert_eq!(expected[3].samples, vec![(4_999, 999.8)]);
    assert_eq!(
        expected[4].query,
        format!("increase({}[5000ms])", expected[0].query)
    );
    assert_eq!(expected[4].samples, vec![(4_999, 4_999.0)]);
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
fn readback_oracle_u64_delta_projection_restarts_discontinuous_fragments() {
    let actual = project_u64_counter_samples(delta_projection_u64_intervals(), 0, u64::MAX);

    assert_delta_projection_sequence(&actual, &delta_projection_u64_expected());
}

#[test]
fn readback_oracle_optional_sum_delta_projection_restarts_discontinuous_fragments() {
    let values = [1.5, -0.25, 4.5, -2.0, 8.0, -16.0, 64.0, 32.0];
    let actual = project_optional_f64_counter_samples(
        delta_projection_metadata()
            .into_iter()
            .zip(values)
            .map(|((timestamp_ms, metadata), value)| (timestamp_ms, metadata, Some(value))),
        0,
        u64::MAX,
    );
    let expected = [
        (1_000, 1.5),
        (2_000, 1.25),
        (3_000, 4.5),
        (4_000, -2.0),
        (5_000, 8.0),
        (6_000, -16.0),
        (7_000, prometheus_stale_nan()),
        (8_000, 32.0),
    ];

    assert_delta_projection_sequence(&actual, &expected);
}

#[test]
fn readback_oracle_histogram_bucket_delta_projection_restarts_discontinuous_fragments() {
    let samples = delta_projection_u64_intervals().map(|(timestamp_ms, metadata, raw)| {
        (
            timestamp_ms,
            HistogramValue {
                count: raw,
                sum: Some(raw as f64),
                min: None,
                max: None,
                metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![raw, 0],
            },
        )
    });
    let (actual, range_hints) =
        project_histogram_bucket_samples_with_range_hints(&samples, Some("1"), 0, u64::MAX);

    assert_delta_projection_sequence(&actual, &delta_projection_u64_expected());
    assert_eq!(range_hints, None);
}

#[test]
fn readback_oracle_exponential_histogram_bucket_delta_projection_restarts_discontinuous_fragments()
{
    let samples = delta_projection_u64_intervals().map(|(timestamp_ms, metadata, raw)| {
        (
            timestamp_ms,
            ExponentialHistogramValue {
                count: raw,
                sum: Some(raw as f64),
                min: None,
                max: None,
                metadata,
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: vec![raw],
                },
                negative: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: Vec::new(),
                },
            },
        )
    });
    let (actual, range_hints) =
        project_exponential_histogram_bucket_samples_with_range_hints(&samples, 2.0, 0, u64::MAX);

    assert_delta_projection_sequence(&actual, &delta_projection_u64_expected());
    assert_eq!(range_hints, None);
}

fn delta_projection_metadata() -> [(u64, TypedSampleMetadata); 8] {
    let metadata = |start_time_ms, reset_hint| TypedSampleMetadata {
        start_time_ms,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint,
        ..TypedSampleMetadata::default()
    };
    [
        (1_000, metadata(Some(0), CounterResetHint::Unknown)),
        (
            2_000,
            metadata(Some(1_000), CounterResetHint::NotCounterReset),
        ),
        (
            3_000,
            metadata(Some(2_500), CounterResetHint::NotCounterReset),
        ),
        (
            4_000,
            metadata(Some(2_500), CounterResetHint::NotCounterReset),
        ),
        (5_000, metadata(Some(4_000), CounterResetHint::CounterReset)),
        (6_000, metadata(Some(5_000), CounterResetHint::GaugeType)),
        (
            7_000,
            TypedSampleMetadata {
                flags: chronoxide_core::storage::head::OTLP_FLAG_NO_RECORDED_VALUE,
                temporality: OtlpAggregationTemporality::Delta,
                ..TypedSampleMetadata::default()
            },
        ),
        (
            8_000,
            metadata(Some(7_000), CounterResetHint::NotCounterReset),
        ),
    ]
}

fn delta_projection_u64_intervals() -> [(u64, TypedSampleMetadata, u64); 8] {
    let values = [1, 2, 4, 8, 16, 32, 64, 128];
    delta_projection_metadata().map(|(timestamp_ms, metadata)| {
        let value = values[usize::try_from(timestamp_ms / 1_000 - 1).unwrap()];
        (timestamp_ms, metadata, value)
    })
}

fn delta_projection_u64_expected() -> [(u64, f64); 8] {
    [
        (1_000, 1.0),
        (2_000, 3.0),
        (3_000, 4.0),
        (4_000, 8.0),
        (5_000, 16.0),
        (6_000, 32.0),
        (7_000, prometheus_stale_nan()),
        (8_000, 128.0),
    ]
}

fn assert_delta_projection_sequence(actual: &[(u64, f64)], expected: &[(u64, f64)]) {
    assert!(
        promql_samples_eq(actual, expected),
        "delta projection differs: actual={actual:?}, expected={expected:?}"
    );
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
    let expected =
        collect_expected_readbacks(&config, StorageLayoutArg::Schema8, &required_kinds).unwrap();
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
    let store = open_segment_store_for_layout_ab(
        tempdir.path(),
        false,
        query_projection_config(&[]),
        StorageLayoutArg::Schema6Ab,
    )
    .unwrap();
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

    let (verification, diagnostics) =
        verify_readbacks(&config, StorageLayoutArg::Schema6Ab, &report).unwrap();

    assert_eq!(verification.mismatches, Vec::<QueryReadbackMismatch>::new());
    assert!(
        diagnostics.executed_queries < diagnostics.expected_queries,
        "overlapped histogram range readbacks should be skipped"
    );
    assert_eq!(diagnostics.skipped_queries, 8);
    assert_eq!(diagnostics.isolation_check_skips, 8);
}

#[test]
fn schema8_corpus_oracle_executes_overlapping_histogram_range_readbacks() {
    let tempdir = schema8_segment_store_with_overlapping_histogram_counter_segments();
    let store = open_segment_store_for_layout_ab(
        tempdir.path(),
        true,
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
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: true,
    };

    let (verification, diagnostics) =
        verify_readbacks(&config, StorageLayoutArg::Schema8, &report).unwrap();
    let expected = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema8,
        &[false, false, true, false, false],
    )
    .unwrap();

    assert_eq!(verification.mismatches, Vec::<QueryReadbackMismatch>::new());
    assert_eq!(diagnostics.expected_queries, 12, "{expected:#?}");
    assert_eq!(diagnostics.executed_queries, 12, "{expected:#?}");
    assert_eq!(diagnostics.skipped_queries, 0);
    assert_eq!(diagnostics.isolation_check_skips, 0);
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

fn schema7_segment_store_with_all_inline_kinds() -> tempfile::TempDir {
    segment_store_with_all_inline_kinds_for_schema(false)
}

fn schema8_segment_store_with_all_inline_kinds() -> tempfile::TempDir {
    segment_store_with_all_inline_kinds_for_schema(true)
}

fn segment_store_with_all_inline_kinds_for_schema(schema8: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let config = if schema8 {
        config.with_storage_schema(SegmentStorageSchema::Schema8)
    } else {
        config.with_storage_schema(SegmentStorageSchema::Schema7)
    };
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.0), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_float");
                visit("kind", "float");
            },
        )
        .unwrap();
    writer
        .record_i64_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(1_000, 7), (2_000, 9)],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_int64");
                visit("kind", "int64");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(3),
            &[(
                3_000,
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
                visit(METRIC_NAME_LABEL, "schema7_histogram");
                visit("kind", "histogram");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(4),
            &[(
                4_000,
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
                visit(METRIC_NAME_LABEL, "schema7_exponential_histogram");
                visit("kind", "exponential_histogram");
            },
        )
        .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(5),
            &[(
                5_000,
                SummaryValue {
                    count: 10,
                    sum: 50.0,
                    metadata: TypedSampleMetadata::default(),
                    quantiles: vec![SummaryQuantileValue {
                        quantile: 0.5,
                        value: 4.0,
                    }],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_summary");
                visit("kind", "summary");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    tempdir
}

fn schema7_segment_store_with_inline_float() -> tempfile::TempDir {
    segment_store_with_inline_float_for_schema(false)
}

fn schema8_segment_store_with_inline_float() -> tempfile::TempDir {
    segment_store_with_inline_float_for_schema(true)
}

fn segment_store_with_inline_float_for_schema(schema8: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let config = if schema8 {
        config.with_storage_schema(SegmentStorageSchema::Schema8)
    } else {
        config.with_storage_schema(SegmentStorageSchema::Schema7)
    };
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.0), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_float");
                visit("kind", "float");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    tempdir
}

fn schema7_segment_store_with_float_overflow() -> tempfile::TempDir {
    segment_store_with_float_overflow_for_schema(false)
}

fn schema8_segment_store_with_float_overflow() -> tempfile::TempDir {
    segment_store_with_float_overflow_for_schema(true)
}

fn segment_store_with_float_overflow_for_schema(schema8: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let config = if schema8 {
        config.with_storage_schema(SegmentStorageSchema::Schema8)
    } else {
        config.with_storage_schema(SegmentStorageSchema::Schema7)
    };
    let mut writer = SegmentWriter::new(config).unwrap();
    for samples in [
        [(1_000, 1_000.0), (1_500, 1_500.0)],
        [(2_000, 2_000.0), (2_500, 2_500.0)],
    ] {
        writer
            .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &samples, |visit| {
                visit(METRIC_NAME_LABEL, "schema7_overflow");
                visit("kind", "float");
            })
            .unwrap();
    }
    writer.flush().unwrap();
    tempdir
}

fn replace_schema7_inline_locator(
    segment_dir: &Path,
    series_ref: u32,
    replacement: &ChunkIndexEntry,
) -> u64 {
    const SERIES_HEADER_LEN: usize = 176;
    const DESCRIPTOR_LEN: usize = 16;
    const HOT_PAGE_LEN: usize = 16_384;
    const HOT_PAGE_HEADER_LEN: usize = 24;
    const HOT_RECORD_LEN: usize = 40;
    const HOT_RECORDS_PER_PAGE: u32 = 409;

    assert_eq!(replacement.file_id, 1);
    let series_path = segment_dir.join(SegmentFile::Series.filename());
    let mut series = fs::read(&series_path).unwrap();
    let hot_pages_offset = usize::try_from(test_read_u64(&series, 80)).unwrap();
    let segment_start_ms = test_read_u64(&series, 144);
    let page_index = series_ref / HOT_RECORDS_PER_PAGE;
    let ordinal = usize::try_from(series_ref % HOT_RECORDS_PER_PAGE).unwrap();
    let page_offset = hot_pages_offset + usize::try_from(page_index).unwrap() * HOT_PAGE_LEN;
    let record_offset = page_offset + HOT_PAGE_HEADER_LEN + ordinal * HOT_RECORD_LEN;
    let control = test_read_u32(&series, record_offset + 16);
    assert_eq!((control >> 9) & 0b11, 1, "expected an inline hot record");
    assert_eq!((control >> 8) & 1, 0, "expected chunks.bin routing");
    assert_eq!((control >> 5) & 0b111, replacement.kind as u32);
    let original_offset = u64::from(test_read_u32(&series, record_offset + 28));

    let min_delta = u32::try_from(replacement.min_time_ms - segment_start_ms).unwrap();
    let max_delta = u32::try_from(replacement.max_time_ms - segment_start_ms).unwrap();
    let file_offset = u32::try_from(replacement.offset).unwrap();
    let prefix_crc = schema7_indexed_prefix_crc(segment_dir, replacement);
    test_put_u32(&mut series, record_offset + 16, control | (1 << 8));
    test_put_u32(&mut series, record_offset + 20, min_delta);
    test_put_u32(&mut series, record_offset + 24, max_delta);
    test_put_u32(&mut series, record_offset + 28, file_offset);
    test_put_u32(&mut series, record_offset + 32, replacement.length);
    test_put_u32(&mut series, record_offset + 36, prefix_crc);

    let page_crc = crc32c::crc32c(&series[page_offset..page_offset + HOT_PAGE_LEN]);
    let descriptor_offset =
        SERIES_HEADER_LEN + usize::try_from(page_index).unwrap() * DESCRIPTOR_LEN;
    test_put_u32(&mut series, descriptor_offset + 8, page_crc);
    series[52..56].fill(0);
    let root_crc = crc32c::crc32c(&series[..hot_pages_offset]);
    test_put_u32(&mut series, 52, root_crc);
    fs::write(series_path, series).unwrap();
    refresh_schema7_footer_file_length(segment_dir, SegmentFile::OooChunks);
    original_offset
}

fn set_schema7_inline_chunk_flags(segment_dir: &Path, series_ref: u32, flags: u16) {
    const SERIES_HEADER_LEN: usize = 176;
    const DESCRIPTOR_LEN: usize = 16;
    const HOT_PAGE_LEN: usize = 16_384;
    const HOT_PAGE_HEADER_LEN: usize = 24;
    const HOT_RECORD_LEN: usize = 40;
    const HOT_RECORDS_PER_PAGE: u32 = 409;

    let series_path = segment_dir.join(SegmentFile::Series.filename());
    let mut series = fs::read(&series_path).unwrap();
    let hot_pages_offset = usize::try_from(test_read_u64(&series, 80)).unwrap();
    let page_index = series_ref / HOT_RECORDS_PER_PAGE;
    let ordinal = usize::try_from(series_ref % HOT_RECORDS_PER_PAGE).unwrap();
    let page_offset = hot_pages_offset + usize::try_from(page_index).unwrap() * HOT_PAGE_LEN;
    let record_offset = page_offset + HOT_PAGE_HEADER_LEN + ordinal * HOT_RECORD_LEN;
    let control = test_read_u32(&series, record_offset + 16);
    assert_eq!((control >> 9) & 0b11, 1, "expected an inline hot record");
    assert_eq!((control >> 8) & 1, 0, "expected chunks.bin routing");
    let chunk_offset = usize::try_from(test_read_u32(&series, record_offset + 28)).unwrap();
    let scalar_lane_len = control >> 11;
    let prefix_len = if scalar_lane_len == 0 { 40 } else { 56 };

    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let mut chunks = fs::read(&chunks_path).unwrap();
    chunks[chunk_offset + 2..chunk_offset + 4].copy_from_slice(&flags.to_le_bytes());
    let indexed_prefix_crc = crc32c::crc32c(&chunks[chunk_offset..chunk_offset + prefix_len]);
    fs::write(chunks_path, chunks).unwrap();

    test_put_u32(&mut series, record_offset + 36, indexed_prefix_crc);
    let page_crc = crc32c::crc32c(&series[page_offset..page_offset + HOT_PAGE_LEN]);
    let descriptor_offset =
        SERIES_HEADER_LEN + usize::try_from(page_index).unwrap() * DESCRIPTOR_LEN;
    test_put_u32(&mut series, descriptor_offset + 8, page_crc);
    series[52..56].fill(0);
    let root_crc = crc32c::crc32c(&series[..hot_pages_offset]);
    test_put_u32(&mut series, 52, root_crc);
    fs::write(series_path, series).unwrap();
}

fn replace_schema7_overflow_locator(
    segment_dir: &Path,
    ordinal: u32,
    replacement: &ChunkIndexEntry,
) -> u64 {
    const CHUNK_INDEX_ROOT_LEN: usize = 64;
    const OVERFLOW_HEADER_LEN: usize = 32;
    const OVERFLOW_ENTRY_LEN: usize = 44;

    assert_eq!(replacement.file_id, 1);
    let index_path = segment_dir.join(SegmentFile::ChunkIndex.filename());
    let mut index = fs::read(&index_path).unwrap();
    assert_eq!(test_read_u32(&index, 24), 1, "expected one overflow blob");
    let chunk_count = test_read_u32(&index, CHUNK_INDEX_ROOT_LEN + 16);
    assert!(ordinal < chunk_count);
    let first_entry = CHUNK_INDEX_ROOT_LEN + OVERFLOW_HEADER_LEN;
    let first_in_order_offset = test_read_u64(&index, first_entry + 20);
    let entry_offset = first_entry + usize::try_from(ordinal).unwrap() * OVERFLOW_ENTRY_LEN;
    assert_eq!(index[entry_offset], 0, "expected chunks.bin routing");
    assert_eq!(index[entry_offset + 1], replacement.kind as u8);

    index[entry_offset] = replacement.file_id;
    index[entry_offset + 1] = replacement.kind as u8;
    index[entry_offset + 2..entry_offset + 4].fill(0);
    test_put_u64(&mut index, entry_offset + 4, replacement.min_time_ms);
    test_put_u64(&mut index, entry_offset + 12, replacement.max_time_ms);
    test_put_u64(&mut index, entry_offset + 20, replacement.offset);
    test_put_u32(&mut index, entry_offset + 28, replacement.length);
    test_put_u32(
        &mut index,
        entry_offset + 32,
        replacement.scalar_lane_offset,
    );
    test_put_u32(&mut index, entry_offset + 36, replacement.scalar_lane_len);
    test_put_u32(
        &mut index,
        entry_offset + 40,
        schema7_indexed_prefix_crc(segment_dir, replacement),
    );

    let blob_len = OVERFLOW_HEADER_LEN + usize::try_from(chunk_count).unwrap() * OVERFLOW_ENTRY_LEN;
    index[CHUNK_INDEX_ROOT_LEN + 28..CHUNK_INDEX_ROOT_LEN + 32].fill(0);
    let blob_crc = crc32c::crc32c(&index[CHUNK_INDEX_ROOT_LEN..CHUNK_INDEX_ROOT_LEN + blob_len]);
    test_put_u32(&mut index, CHUNK_INDEX_ROOT_LEN + 28, blob_crc);
    fs::write(index_path, index).unwrap();
    refresh_schema7_footer_file_length(segment_dir, SegmentFile::OooChunks);
    first_in_order_offset
}

fn refresh_schema7_footer_file_length(segment_dir: &Path, file: SegmentFile) {
    const FOOTER_HEADER_LEN: usize = 16;
    const FOOTER_ENTRY_LEN: usize = 20;

    let file_id = match file {
        SegmentFile::MetaJson => 1,
        SegmentFile::Symbols => 2,
        SegmentFile::Series => 3,
        SegmentFile::Chunks => 4,
        SegmentFile::OooChunks => 5,
        SegmentFile::ChunkIndex => 6,
        SegmentFile::Indexes => 7,
        SegmentFile::Footer => panic!("footer cannot inventory itself"),
    };
    let footer_path = segment_dir.join(SegmentFile::Footer.filename());
    let mut footer = fs::read(&footer_path).unwrap();
    let file_count = usize::from(u16::from_le_bytes(footer[16..18].try_into().unwrap()));
    let entry_start = (0..file_count)
        .map(|ordinal| FOOTER_HEADER_LEN + 4 + ordinal * FOOTER_ENTRY_LEN)
        .find(|offset| {
            u16::from_le_bytes(footer[*offset..*offset + 2].try_into().unwrap()) == file_id
        })
        .expect("footer must inventory the replacement file");
    let file_len = fs::metadata(segment_dir.join(file.filename()))
        .unwrap()
        .len();
    test_put_u64(&mut footer, entry_start + 4, file_len);
    let trailer_offset = footer.len() - 4;
    let footer_crc = crc32c::crc32c(&footer[..trailer_offset]);
    test_put_u32(&mut footer, trailer_offset, footer_crc);
    fs::write(footer_path, footer).unwrap();
}

fn schema7_indexed_prefix_crc(segment_dir: &Path, entry: &ChunkIndexEntry) -> u32 {
    let file = match entry.file_id {
        0 => SegmentFile::Chunks,
        1 => SegmentFile::OooChunks,
        other => panic!("unexpected chunk file ID {other}"),
    };
    let bytes = fs::read(segment_dir.join(file.filename())).unwrap();
    let offset = usize::try_from(entry.offset).unwrap();
    let prefix_len = if entry.scalar_lane_len == 0 { 40 } else { 56 };
    crc32c::crc32c(&bytes[offset..offset + prefix_len])
}

fn test_read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn test_read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn test_put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn test_put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
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
    segment_store_with_overlapping_histogram_counter_segments_for_schema(false)
}

fn schema8_segment_store_with_overlapping_histogram_counter_segments() -> tempfile::TempDir {
    segment_store_with_overlapping_histogram_counter_segments_for_schema(true)
}

fn segment_store_with_overlapping_histogram_counter_segments_for_schema(
    schema8: bool,
) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let labels = |visit: &mut dyn FnMut(&str, &str)| {
        visit(METRIC_NAME_LABEL, "overlap_duration");
        visit("route", "/overlap");
    };

    let broad_config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_deterministic_segment_ids(1);
    let broad_config = broad_config.with_storage_schema(if schema8 {
        SegmentStorageSchema::Schema8
    } else {
        SegmentStorageSchema::Schema6
    });
    let mut broad_writer = SegmentWriter::new(broad_config).unwrap();
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
                        count: 50,
                        sum: Some(150.0),
                        min: Some(1.0),
                        max: Some(10.0),
                        metadata: TypedSampleMetadata {
                            reset_hint:
                                chronoxide_core::storage::head::CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![5, 45],
                    },
                ),
            ],
            labels,
        )
        .unwrap();
    broad_writer.flush().unwrap();

    let overlapping_config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_deterministic_segment_ids(2);
    let overlapping_config = overlapping_config.with_storage_schema(if schema8 {
        SegmentStorageSchema::Schema8
    } else {
        SegmentStorageSchema::Schema6
    });
    let mut overlapping_writer = SegmentWriter::new(overlapping_config).unwrap();
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
    segment_store_with_sparse_final_window_for_schema(SegmentStorageSchema::Schema8)
}

fn segment_store_with_sparse_final_window_for_schema(
    storage_schema: SegmentStorageSchema,
) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(600))
        .with_storage_schema(storage_schema);
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
    segment_store_with_two_windows_for_layout(false)
}

fn segment_store_with_two_windows_schema7() -> tempfile::TempDir {
    segment_store_with_two_windows_for_layout(true)
}

fn segment_store_with_two_windows_for_layout(schema7: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(if schema7 {
            SegmentStorageSchema::Schema7
        } else {
            SegmentStorageSchema::Schema8
        });
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

fn sorted_segment_metadata(segments_dir: &Path) -> Vec<SegmentMeta> {
    let mut segments = fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| {
            serde_json::from_slice::<SegmentMeta>(
                &fs::read(entry.path().join(SegmentFile::MetaJson.filename())).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    segments.sort_by(|left, right| {
        left.start_ms
            .cmp(&right.start_ms)
            .then_with(|| left.end_ms.cmp(&right.end_ms))
            .then_with(|| left.segment_id.cmp(&right.segment_id))
    });
    segments
}

fn publish_manifest_segments(segments_dir: &Path, segments: &[&SegmentMeta]) {
    let manifest_dir = segments_dir.join("manifest");
    let mut writer = ManifestWriter::create(&manifest_dir, 99).unwrap();
    for meta in segments {
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
    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
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
                        metadata: metadata(0),
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
                        metadata: metadata(1_000),
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
                        metadata: metadata(2_000),
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
    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
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
                        metadata: metadata(0),
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
                        metadata: metadata(1_000),
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

fn segment_store_with_long_float_series(schema: SegmentStorageSchema) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(1))
        .with_storage_schema(schema);
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
