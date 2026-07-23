use super::*;

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
    assert_eq!(
        expected[3].samples,
        vec![(4_999, f64::from_bits(0x408f_3e66_6666_6667))]
    );
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
        step_ms: None,
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
            step_ms: None,
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
        step_ms: None,
        samples: vec![(0, 5.0), (1_000, 10.0)],
        isolation_check: None,
    };

    let (range_ms, increase) = scalar_counter_range_increase(&base, None).unwrap();

    assert_eq!(range_ms, 1_001);
    assert!((increase - 5.005).abs() < 1e-12);
}

#[test]
fn scalar_readback_oracle_matches_prometheus_float_operation_order_exactly() {
    let samples = vec![
        (1_782_979_454_512, 9.0),
        (1_782_979_461_753, 36.0),
        (1_782_979_461_781, 43.0),
        (1_782_979_493_066, 45.0),
        (1_782_979_505_328, 19.0),
        (1_782_979_514_618, 9.0),
        (1_782_979_521_777, 36.0),
        (1_782_979_521_784, 43.0),
        (1_782_979_553_073, 45.0),
        (1_782_979_565_331, 19.0),
    ];
    let reset_hints = [
        CounterResetHint::Unknown,
        CounterResetHint::NotCounterReset,
        CounterResetHint::CounterReset,
        CounterResetHint::NotCounterReset,
        CounterResetHint::CounterReset,
        CounterResetHint::CounterReset,
        CounterResetHint::CounterReset,
        CounterResetHint::CounterReset,
        CounterResetHint::NotCounterReset,
        CounterResetHint::CounterReset,
    ];
    let base = ExpectedReadback {
        query: "typed.histogram_sum".to_string(),
        start_ms: samples[0].0,
        end_ms: samples.last().unwrap().0,
        step_ms: None,
        samples,
        isolation_check: None,
    };
    let mut readbacks = Vec::new();

    push_counter_range_readbacks(&mut readbacks, &base, Some(&reset_hints));

    let increase = readbacks
        .iter()
        .find(|readback| readback.query.starts_with("increase("))
        .unwrap();
    let rate = readbacks
        .iter()
        .find(|readback| readback.query.starts_with("rate("))
        .unwrap();
    assert_eq!(
        increase.samples,
        vec![(1_782_979_565_331, f64::from_bits(0x4069_000e_c8d2_eb3f))]
    );
    assert_eq!(
        rate.samples,
        vec![(1_782_979_565_331, f64::from_bits(0x3ffc_e03b_f375_ff09))]
    );
}

#[test]
fn scalar_readback_oracle_builds_bounded_multi_step_rate_with_prometheus_windows() {
    let base = ExpectedReadback {
        query: "multi.step.counter".to_string(),
        start_ms: 0,
        end_ms: 1_800_000,
        step_ms: None,
        samples: vec![
            (0, 100.0),
            (300_000, 110.0),
            (600_000, prometheus_stale_nan()),
            (900_000, 5.0),
            (1_200_000, 9.0),
            (1_500_000, 2.0),
            (1_800_000, 6.0),
        ],
        isolation_check: None,
    };

    let range = bounded_scalar_counter_range_readback(&base).unwrap();

    assert_eq!(range.query, "rate(multi.step.counter[900000ms])");
    assert_eq!(range.start_ms, 900_000);
    assert_eq!(range.end_ms, 1_800_000);
    assert_eq!(range.step_ms, Some(300_000));
    let expected = [7.5 / 900.0, 6.0 / 900.0, 9.0 / 900.0, 9.0 / 900.0];
    assert_eq!(range.samples.len(), expected.len());
    for ((timestamp_ms, actual), (expected_timestamp_ms, expected)) in range.samples.iter().zip(
        [900_000, 1_200_000, 1_500_000, 1_800_000]
            .into_iter()
            .zip(expected),
    ) {
        assert_eq!(*timestamp_ms, expected_timestamp_ms);
        assert!((*actual - expected).abs() < 1e-15);
    }
    assert!(
        range
            .isolation_check
            .unwrap()
            .failure_reason
            .contains("physical Float/Int64 series")
    );
}

#[test]
fn scalar_readback_oracle_multi_step_range_includes_epoch_zero_for_pre_epoch_window() {
    let base = ExpectedReadback {
        query: "pre.epoch.multi.step.counter".to_string(),
        start_ms: 0,
        end_ms: 1_200_000,
        step_ms: None,
        samples: vec![
            (0, 5.0),
            (300_000, 10.0),
            (600_000, 15.0),
            (900_000, 20.0),
            (1_200_000, 25.0),
        ],
        isolation_check: None,
    };

    let range = bounded_scalar_counter_range_readback(&base).unwrap();

    assert_eq!(range.start_ms, 300_000);
    assert_eq!(range.step_ms, Some(300_000));
    assert_eq!(range.samples.len(), 4);
    assert_eq!(range.samples[0].0, 300_000);
    assert!((range.samples[0].1 - 7.5 / 900.0).abs() < 1e-15);
}

#[test]
fn scalar_readback_oracle_uses_the_largest_complete_bounded_endpoint_set() {
    let base = ExpectedReadback {
        query: "sparse.multi.step.counter".to_string(),
        start_ms: 0,
        end_ms: 1_800_000,
        step_ms: None,
        samples: vec![(1_200_000, 10.0), (1_500_000, 15.0), (1_800_000, 20.0)],
        isolation_check: None,
    };

    let range = bounded_scalar_counter_range_readback(&base).unwrap();

    assert_eq!(range.start_ms, 1_500_000);
    assert_eq!(range.end_ms, 1_800_000);
    assert_eq!(range.samples.len(), 2);
}

#[test]
fn schema8_readback_oracle_executes_bounded_float_and_int64_query_ranges() {
    let tempdir = segment_store_with_scalar_range_counters();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("scalar_range_readback.md"),
        start_ms: 0,
        end_ms: 1_800_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };
    let required_kinds = [true, true, false, false, false];
    let expected =
        collect_expected_readbacks(&config, StorageLayoutArg::Schema8, &required_kinds).unwrap();
    let range_readbacks = expected
        .iter()
        .filter(|readback| readback.step_ms == Some(SCALAR_RANGE_READBACK_STEP_MS))
        .collect::<Vec<_>>();

    assert_eq!(range_readbacks.len(), 2, "{expected:#?}");
    assert!(
        range_readbacks.iter().all(|readback| {
            readback.query.contains("[900000ms]") && readback.samples.len() == 4
        })
    );

    let store = open_segment_store(tempdir.path(), false, query_projection_config(&[])).unwrap();
    let report = store.smoke_verify(0, 1_800_000, 1).unwrap();
    let (verification, diagnostics) =
        verify_readbacks(&config, StorageLayoutArg::Schema8, &report).unwrap();

    assert!(verification.mismatches.is_empty(), "{verification:#?}");
    assert_eq!(diagnostics.expected_queries, expected.len());
    assert_eq!(diagnostics.executed_queries, expected.len());
    assert_eq!(diagnostics.skipped_queries, 0);
    assert_eq!(diagnostics.multi_step_range_expected_queries, 2);
    assert_eq!(diagnostics.multi_step_range_executed_queries, 2);
    assert_eq!(diagnostics.multi_step_range_skipped_queries, 0);
    assert!(diagnostics.skip_reasons.is_empty());
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
        step_ms: None,
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
fn verify_expected_readbacks_records_explicit_isolation_skip_reason() {
    let tempdir = segment_store_with_float_and_histogram();
    let store = open_segment_store(tempdir.path(), false, query_projection_config(&[])).unwrap();
    let mut query_session = store.query_session().unwrap();
    let mut diagnostics = QueryReadbackDiagnostics {
        multi_step_range_expected_queries: 1,
        ..QueryReadbackDiagnostics::default()
    };
    let reason = "fixture cannot prove exact physical scalar isolation";
    let expected = vec![ExpectedReadback {
        query: r#"rate({__name__="cpu.usage",instance="host-a"}[15m])"#.to_string(),
        start_ms: 1_000,
        end_ms: 2_000,
        step_ms: Some(1_000),
        samples: vec![(1_000, 1.0), (2_000, 1.0)],
        isolation_check: Some(ReadbackIsolationCheck {
            query: r#"{__name__="cpu.usage",instance="host-a"}"#.to_string(),
            start_ms: 1_000,
            end_ms: 2_000,
            samples: vec![(1_000, 99.0)],
            failure_reason: reason.to_string(),
        }),
    }];

    let verification =
        verify_expected_readbacks(&mut query_session, &expected, &mut diagnostics).unwrap();

    assert_eq!(verification.checked_queries, 0);
    assert_eq!(diagnostics.executed_queries, 0);
    assert_eq!(diagnostics.skipped_queries, 1);
    assert_eq!(diagnostics.isolation_check_skips, 1);
    assert_eq!(diagnostics.multi_step_range_expected_queries, 1);
    assert_eq!(diagnostics.multi_step_range_executed_queries, 0);
    assert_eq!(diagnostics.multi_step_range_skipped_queries, 1);
    assert_eq!(diagnostics.skip_reasons.get(reason), Some(&1));
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
