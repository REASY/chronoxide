use super::*;

#[test]
fn benchmark_rejects_an_invalid_query_label_arena_budget() {
    let segments = segment_store_with_float_and_histogram();
    let output = segments.path().join("invalid-label-arena.md");
    let raw_output = segments.path().join("invalid-label-arena.json");
    let mut config = benchmark_config_for_outputs(
        segments.path().to_path_buf(),
        output.clone(),
        raw_output.clone(),
    );
    config.query_label_arena_max_bytes =
        chronoxide_core::storage::segment::MAX_QUERY_LABEL_ARENA_BYTES + 1;

    let error = run_query_benchmark_with_experimental_flow(
        &config,
        false,
        LabelMaterializationArg::DemandDriven,
        LabelStorageArg::CompactIds,
        StorageLayoutArg::Schema8,
    )
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("configure query label arena"));
    assert!(error.to_string().contains("exceeds maximum"));
    assert!(!output.exists());
    assert!(!raw_output.exists());
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
    assert_eq!(
        defaults.chunk_payload_coalesce_max_gap_bytes,
        DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES
    );
    assert!(!defaults.experimental_cross_segment_chunk_reads);
    assert_eq!(
        defaults.label_materialization,
        LabelMaterializationArg::DemandDriven
    );
    assert_eq!(defaults.query_label_storage, LabelStorageArg::CompactIds);
    assert_eq!(
        defaults.query_label_arena_max_bytes,
        DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES
    );
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
        "--chunk-payload-coalesce-max-gap-bytes",
        "1024",
        "--experimental-cross-segment-chunk-reads",
        "--label-materialization",
        "full",
        "--query-label-storage",
        "compact-ids",
        "--query-label-arena-max-bytes",
        "1048576",
        "--storage-layout",
        "schema6-ab",
        "--query-instrumentation",
        "detailed",
    ]);
    assert_eq!(overridden.benchmark_repeats, 5);
    assert_eq!(overridden.chunk_read_mode, ChunkReadModeArg::IoUring);
    assert_eq!(overridden.chunk_read_queue_depth, 8);
    assert_eq!(overridden.chunk_payload_coalesce_max_gap_bytes, 1_024);
    assert!(overridden.experimental_cross_segment_chunk_reads);
    assert_eq!(
        overridden.label_materialization,
        LabelMaterializationArg::Full
    );
    assert_eq!(overridden.query_label_storage, LabelStorageArg::CompactIds);
    assert_eq!(overridden.query_label_arena_max_bytes, 1_048_576);
    assert_eq!(
        overridden.query_label_storage.core_policy(),
        QueryLabelStoragePolicy::CompactIds
    );
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
fn one_pass_range_cli_requires_an_explicit_unlimited_range_workload() {
    let valid = Args::try_parse_from([
        "chronoxide-query",
        "--query",
        "sum by (service)(rate(cpu_usage_total[15m]))",
        "--start-ms",
        "1000",
        "--end-ms",
        "5000",
        "--step-ms",
        "1000",
        "--range-execution-mode",
        "one-pass-assume-scalar",
        "--query-unlimited",
    ])
    .unwrap();
    assert_eq!(
        benchmark_request_from_args(&valid).unwrap(),
        (1_000, 5_000, QueryBenchmarkMode::Range { step_ms: 1_000 })
    );
    assert_eq!(
        valid.range_execution_mode,
        RangeExecutionModeArg::OnePassAssumeScalar
    );
    assert_eq!(
        valid.query_limits.to_query_limits(),
        QueryLimits::unlimited()
    );

    let finite = Args::try_parse_from([
        "chronoxide-query",
        "--query",
        "sum by (service)(rate(cpu_usage_total[15m]))",
        "--start-ms",
        "1000",
        "--end-ms",
        "5000",
        "--step-ms",
        "1000",
        "--range-execution-mode",
        "one-pass-assume-scalar",
    ])
    .unwrap();
    assert!(
        benchmark_request_from_args(&finite)
            .unwrap_err()
            .to_string()
            .contains("requires --query-unlimited")
    );

    let instant = Args::try_parse_from([
        "chronoxide-query",
        "--query",
        "cpu_usage_total",
        "--range-execution-mode",
        "one-pass-assume-scalar",
        "--query-unlimited",
    ])
    .unwrap();
    assert!(
        benchmark_request_from_args(&instant)
            .unwrap_err()
            .to_string()
            .contains("requires --step-ms")
    );

    assert!(
        Args::try_parse_from([
            "chronoxide-query",
            "--query",
            "cpu_usage_total",
            "--query-unlimited",
            "--query-max-samples",
            "10",
        ])
        .is_err()
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
