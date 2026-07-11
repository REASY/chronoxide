#[path = "support/promql_range_scalar_cache.rs"]
mod support;

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use chronoxide_core::labels::METRIC_NAME_LABEL;
use chronoxide_core::storage::segment::QueryStats;
use sha2::{Digest, Sha256};
use support::{
    LARGE_COUNT, ORDINARY_NAN_BITS, STALE_NAN_BITS, build_error_oracle_document,
    checkpoint_provenance_v1, exact_stats, execution_rows, ordered_labels,
    pretty_json_with_newline, sample_bits, write_duplicate_offset_fixture,
    write_large_count_fixture, write_missing_sum_nan_fixture, write_mixed_temporality_fixture,
    write_stale_reset_delta_fixture, write_start_time_reset_fixture,
};

const ERROR_ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-errors-v1.json"
);
const RESULT_ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-results-v1.json"
);
const ERROR_ORACLE_REGENERATION_ENV: &str =
    "CHRONOXIDE_REGENERATE_TRACKED_PROMQL_RANGE_ERROR_ORACLE";
const ERROR_ORACLE_REGENERATION_SENTINEL: &str = concat!(
    "I_ACKNOWLEDGE_TRACKED_PROMQL_RANGE_ERROR_ORACLE_REGENERATION_FOR_CHECKPOINT_",
    "3c09d5da18a4dbaf09cc0e623f34085bb91c933c_WITH_BENCHMARK_BINARY_",
    "37c044ca644f9496d0818f8c35ebbc5941c3b604adc166526a466a12ddfbc246"
);

const REPLAY_QUERIES: [&str; 3] = [
    "rate(go_gc_duration_seconds_count[15m])",
    "sum by (service_name_x55e50a58f9befba7)(rate(go_gc_duration_seconds_count[15m]))",
    "histogram_quantile(0.95, sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257[15m])))",
];

fn expected_checkpoint_provenance() -> serde_json::Value {
    serde_json::json!({
        "checkpoint_head": "3c09d5da18a4dbaf09cc0e623f34085bb91c933c",
        "benchmark_binary_sha256": "37c044ca644f9496d0818f8c35ebbc5941c3b604adc166526a466a12ddfbc246",
        "rustc": "rustc 1.95.0 (59807616e 2026-04-14)",
        "rustc_commit": "59807616e1fa2540724bfbac14d7976d7e4a3860",
        "host": "aarch64-apple-darwin",
        "llvm": "22.1.2",
        "cargo": "cargo 1.95.0 (f2d3ce0bd 2026-03-21)",
        "os_product": "macOS",
        "os_version": "26.5.2",
        "os_build": "25F84",
        "working_tree_dirty": true,
        "query_promql_diff_sha256": "a2b5aea77bc55f35cafdc9cd8433e6bb2b87358a596968fe67ea3fe33a0fb8cd",
    })
}

#[test]
fn prechange_stale_reset_delta_continuation_is_explicit() {
    let fixture = write_stale_reset_delta_fixture();
    assert!(fixture.path().is_dir());

    let count = fixture.run_range("cache_count", 10_000, 50_000, 10_000);
    assert_eq!(
        ordered_labels(&count),
        vec![vec![
            (METRIC_NAME_LABEL.to_string(), "cache_count".to_string()),
            ("route".to_string(), "/stale-reset".to_string()),
        ]]
    );
    assert_eq!(
        sample_bits(&count),
        vec![
            (10_000, 2.0_f64.to_bits()),
            (20_000, 5.0_f64.to_bits()),
            (40_000, 7.0_f64.to_bits()),
            (50_000, 11.0_f64.to_bits()),
        ]
    );

    let sum = fixture.run_range("cache_sum", 10_000, 50_000, 10_000);
    assert_eq!(
        ordered_labels(&sum),
        vec![vec![
            (METRIC_NAME_LABEL.to_string(), "cache_sum".to_string()),
            ("route".to_string(), "/stale-reset".to_string()),
        ]]
    );
    assert_eq!(sample_bits(&sum), sample_bits(&count));

    let raw_at_stale = fixture.run_instant("cache_count", 10_000, 30_000);
    assert_eq!(
        sample_bits(&raw_at_stale),
        vec![
            (10_000, 2.0_f64.to_bits()),
            (20_000, 5.0_f64.to_bits()),
            (30_000, STALE_NAN_BITS),
        ]
    );

    let last = fixture.run_range("last_over_time(cache_count[30s])", 30_000, 50_000, 10_000);
    assert_eq!(
        sample_bits(&last),
        vec![
            (30_000, 5.0_f64.to_bits()),
            (40_000, 7.0_f64.to_bits()),
            (50_000, 11.0_f64.to_bits()),
        ]
    );
    assert_eq!(
        fixture.run_session_range("cache_count", 10_000, 50_000, 10_000),
        count
    );

    assert_eq!(
        count.stats,
        QueryStats {
            segments_considered: 10,
            segments_skipped_by_time: 0,
            segments_skipped_by_missing_equality: 5,
            segments_skipped_by_matcher_time_range: 0,
            segments_queried: 5,
            matched_series: 5,
            projected_series: 5,
            chunk_reads: 5,
            bytes_read: 750,
            samples_decoded: 25,
            typed_scalar_chunks_decoded: 5,
            typed_full_chunks_decoded: 0,
            regex_values_examined: 0,
            index_postings_reads: 0,
            index_postings_bytes_read: 0,
        }
    );
    assert_eq!(sum.stats, count.stats);
    assert_eq!(
        last.stats,
        QueryStats {
            segments_considered: 6,
            segments_skipped_by_time: 0,
            segments_skipped_by_missing_equality: 3,
            segments_skipped_by_matcher_time_range: 0,
            segments_queried: 3,
            matched_series: 3,
            projected_series: 3,
            chunk_reads: 3,
            bytes_read: 450,
            samples_decoded: 15,
            typed_scalar_chunks_decoded: 3,
            typed_full_chunks_decoded: 0,
            regex_values_examined: 0,
            index_postings_reads: 0,
            index_postings_bytes_read: 0,
        }
    );
    assert_eq!(
        raw_at_stale.stats,
        QueryStats {
            segments_considered: 2,
            segments_skipped_by_time: 0,
            segments_skipped_by_missing_equality: 1,
            segments_skipped_by_matcher_time_range: 0,
            segments_queried: 1,
            matched_series: 1,
            projected_series: 1,
            chunk_reads: 1,
            bytes_read: 150,
            samples_decoded: 5,
            typed_scalar_chunks_decoded: 1,
            typed_full_chunks_decoded: 0,
            regex_values_examined: 0,
            index_postings_reads: 0,
            index_postings_bytes_read: 0,
        }
    );

    assert_eq!(
        count.semantic_fingerprint_sha256().to_hex(),
        "121448046eefeb19ec38a1d67ef0871bfcc10a52018e34249757f5cf167291c3"
    );
    assert_eq!(
        sum.semantic_fingerprint_sha256().to_hex(),
        "e65a0344a041df5ed2c39ea934e341572bdf8c1704e0beb5cface12fc17197c3"
    );
    assert_eq!(
        last.semantic_fingerprint_sha256().to_hex(),
        "417db4d19f2c0f35454607fdc96d16d0ee53e98e86e03c63954317635d08c493"
    );
    assert_eq!(
        raw_at_stale.semantic_fingerprint_sha256().to_hex(),
        "d83af55dfd591edb7bad65823317576454992a4bd3829fca474d3c0e64b21fb0"
    );
}

#[test]
fn prechange_error_oracle_is_exact_and_versioned() {
    let document = build_error_oracle_document();
    assert_eq!(document.rows.len(), 22);
    assert_eq!(document.checkpoint_provenance, checkpoint_provenance_v1());
    assert_eq!(
        serde_json::to_value(&document.checkpoint_provenance).unwrap(),
        expected_checkpoint_provenance()
    );
    let actual = pretty_json_with_newline(&document);
    let expected = fs::read(ERROR_ARTIFACT).unwrap();
    assert_eq!(actual, expected, "pre-change error oracle drifted");
}

#[test]
#[ignore = "explicit tracked-oracle regeneration"]
fn regenerate_prechange_error_oracle_requires_exact_checkpoint_sentinel() {
    let supplied_sentinel = std::env::var(ERROR_ORACLE_REGENERATION_ENV).unwrap_or_default();
    assert_eq!(
        supplied_sentinel, ERROR_ORACLE_REGENERATION_SENTINEL,
        "set {ERROR_ORACLE_REGENERATION_ENV} to the exact checkpoint-and-binary sentinel"
    );

    let document = build_error_oracle_document();
    assert_eq!(document.rows.len(), 22);
    assert_eq!(document.checkpoint_provenance, checkpoint_provenance_v1());
    assert_eq!(
        serde_json::to_value(&document.checkpoint_provenance).unwrap(),
        expected_checkpoint_provenance()
    );
    let actual = pretty_json_with_newline(&document);

    let artifact = Path::new(ERROR_ARTIFACT);
    let parent = artifact.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    let mut temporary = tempfile::NamedTempFile::new_in(parent).unwrap();
    temporary.write_all(&actual).unwrap();
    temporary.flush().unwrap();
    temporary.as_file().sync_all().unwrap();
    let persisted = temporary.persist(artifact).unwrap();
    persisted.sync_all().unwrap();
    File::open(parent).unwrap().sync_all().unwrap();

    panic!(
        "regenerated tracked error oracle; review the artifact diff before rerunning the normal suite"
    );
}

#[test]
fn prechange_real_replay_result_baseline_is_exact() {
    let bytes = fs::read(RESULT_ARTIFACT).unwrap();
    let digest = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        digest, "cbdab20e7c65a0675413ddc2d6cb8aa1c2f84d15742843778529202f7be30a74",
        "pre-change replay artifact bytes drifted"
    );
    let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(raw["schema"], "chronoxide.query-benchmark.raw/v1");
    assert_eq!(
        raw["checkpoint_provenance"],
        expected_checkpoint_provenance()
    );
    assert_eq!(
        raw["corpus_fingerprint_sha256"],
        "b9c1470b99726c3f6a53591bf5ec7fb8f96b0691f474e6935a27fce6de145891"
    );
    assert!(raw["corpus_fingerprint_duration_ns"].as_u64().is_some());
    assert!(raw.get("generated_at").is_none());
    let configuration = &raw["configuration"];
    assert_eq!(
        configuration["segments_dir"],
        "data/perf/segment-index-v7/segments-replay-v7-no-record-index"
    );
    assert_eq!(configuration["start_ms"], 1_782_982_800_000_u64);
    assert_eq!(configuration["end_ms"], 1_782_986_400_000_u64);
    assert_eq!(configuration["mode"], "query_range");
    assert_eq!(configuration["step_ms"], 60_000_u64);
    assert_eq!(configuration["benchmark_repeats"], 5);
    assert_eq!(configuration["queries"], serde_json::json!(REPLAY_QUERIES));
    assert_eq!(configuration["prewarm_query_contexts"], false);
    assert_eq!(configuration["prefetch_query_data"], false);
    assert_eq!(
        configuration["exponential_histogram_bucket_boundaries"],
        serde_json::json!([])
    );
    assert_eq!(configuration["validate_segment_footers"], false);
    assert_eq!(
        raw["limits"],
        serde_json::json!({
            "max_matched_series": 1_000_000,
            "max_projected_series": 2_000_000,
            "max_chunk_reads": 5_000_000,
            "max_bytes_read": 2_147_483_648_u64,
            "max_samples_decoded": 50_000_000,
            "max_regex_values_examined": 100_000,
        })
    );

    let scalar_stats = serde_json::json!({
        "segments_considered": 2196,
        "segments_skipped_by_time": 1692,
        "segments_skipped_by_missing_equality": 268,
        "segments_skipped_by_matcher_time_range": 59,
        "segments_queried": 177,
        "matched_series": 65953,
        "projected_series": 65519,
        "chunk_reads": 129249,
        "bytes_read": 63108059,
        "samples_decoded": 2259179,
        "typed_scalar_chunks_decoded": 129249,
        "typed_full_chunks_decoded": 0,
        "regex_values_examined": 0,
        "index_postings_reads": 0,
        "index_postings_bytes_read": 0,
    });
    let histogram_stats = serde_json::json!({
        "segments_considered": 3294,
        "segments_skipped_by_time": 2538,
        "segments_skipped_by_missing_equality": 93,
        "segments_skipped_by_matcher_time_range": 72,
        "segments_queried": 591,
        "matched_series": 107232,
        "projected_series": 103012,
        "chunk_reads": 195251,
        "bytes_read": 286993504,
        "samples_decoded": 2631847,
        "typed_scalar_chunks_decoded": 0,
        "typed_full_chunks_decoded": 195251,
        "regex_values_examined": 0,
        "index_postings_reads": 0,
        "index_postings_bytes_read": 0,
    });
    let expected = [
        (
            "5eb2038224f4280e3f45806f14d3585db0de494e94d59c44f3ff3168917343a2",
            1105_u64,
            65424_u64,
            &scalar_stats,
        ),
        (
            "65215f26762abdea2af50219305207bf3682732329287fafe4f1b4ba9cb08f78",
            168_u64,
            10161_u64,
            &scalar_stats,
        ),
        (
            "61362a460f33920a99b28795230354eac99500b6e38080f668bdcc169add695b",
            10_u64,
            607_u64,
            &histogram_stats,
        ),
    ];
    let runs = raw["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 15);
    for expression_index in 0..3 {
        let (hash, result_series, result_samples, stats) = expected[expression_index];
        for run_index in 0..5 {
            let run = &runs[expression_index * 5 + run_index];
            assert_eq!(run["query"], REPLAY_QUERIES[expression_index]);
            assert_eq!(
                run["run_kind"],
                if run_index == 0 { "cold" } else { "warm" }
            );
            assert_eq!(run["run_index"], run_index);
            assert!(run["duration_ns"].as_u64().is_some());
            assert_eq!(run["effective_start_ms"], 1_782_982_800_000_u64);
            assert_eq!(run["effective_end_ms"], 1_782_986_400_000_u64);
            assert_eq!(run["step_ms"], 60_000_u64);
            assert_eq!(run["semantic_fingerprint_sha256"], hash);
            assert_eq!(run["result_series"], result_series);
            assert_eq!(run["result_samples"], result_samples);
            assert_eq!(&run["stats"], stats);
        }
    }
}

#[test]
fn delta_start_time_changes_around_resets_have_exact_increase_and_rate_bits() {
    let fixture = write_start_time_reset_fixture();
    let increase = fixture.run_range(
        "increase(start_time_cases_count[20s])",
        20_000,
        40_000,
        10_000,
    );
    let rate = fixture.run_range("rate(start_time_cases_count[20s])", 20_000, 40_000, 10_000);
    // Delta intervals at 10s, 20s, 30s, and 40s carry 2, 3, 7, and 4.
    // The 20-second windows therefore select 2+3, 3+7, and 7+4; the sample
    // exactly at the later windows' left boundary is only a reconstruction
    // predecessor and must not contribute to their increase.
    assert_eq!(
        execution_rows(&increase),
        vec![(
            vec![("route".to_string(), "/start-reset".to_string())],
            vec![
                (20_000, 5.0_f64.to_bits()),
                (30_000, 10.0_f64.to_bits()),
                (40_000, 11.0_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        execution_rows(&rate),
        vec![(
            vec![("route".to_string(), "/start-reset".to_string())],
            vec![
                (20_000, 0.25_f64.to_bits()),
                (30_000, 0.5_f64.to_bits()),
                (40_000, 0.55_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        [increase.stats, rate.stats],
        [exact_stats(6, 0, 3, 3, 3, 3, 3, 414, 12, 3); 2]
    );
    assert_eq!(
        [
            increase.semantic_fingerprint_sha256().to_hex(),
            rate.semantic_fingerprint_sha256().to_hex(),
        ],
        [
            "32746872ded0897e349097b735786996f14807abe731ba2f00f4fbe31d73cb06".to_string(),
            "433e3498bb7ead4e6aa47f136d3268a5807061fccb003b540ee60db788c5b536".to_string(),
        ]
    );
}

#[test]
fn prechange_large_delta_counts_accumulate_as_u64_before_f64_projection() {
    let fixture = write_large_count_fixture();
    let count = fixture.run_range("large_count_cases_count", 1_000, 2_000, 1_000);
    assert_eq!(
        execution_rows(&count),
        vec![(
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "large_count_cases_count".to_string(),
                ),
                ("route".to_string(), "/large".to_string()),
            ],
            vec![
                (1_000, (LARGE_COUNT as f64).to_bits()),
                (2_000, ((LARGE_COUNT + 1) as f64).to_bits()),
            ],
        )]
    );
    assert_ne!(
        (LARGE_COUNT as f64 + 1.0).to_bits(),
        ((LARGE_COUNT + 1) as f64).to_bits()
    );
    assert_eq!(count.stats, exact_stats(4, 0, 2, 2, 2, 2, 2, 178, 4, 2));
    assert_eq!(
        count.semantic_fingerprint_sha256().to_hex(),
        "2cec37f6b6a12d28ab8cf6d9c3e41ff1d650c2b34d8ea5ab659efeb8853bfc51"
    );
}

#[test]
fn prechange_duplicates_and_positive_negative_nested_epoch_offsets_are_exact() {
    let fixture = write_duplicate_offset_fixture();
    let base = fixture.run_range("offset_cases_count", 0, 20_000, 10_000);
    let positive = fixture.run_range("offset_cases_count offset 5s", 5_000, 25_000, 10_000);
    let negative = fixture.run_range("offset_cases_count offset -5s", 5_000, 15_000, 10_000);
    let nested = fixture.run_range(
        "last_over_time(offset_cases_count[1s] offset 5s)",
        5_000,
        25_000,
        10_000,
    );
    let epoch = fixture.run_range("offset_cases_count offset 2s", 1_000, 1_000, 1_000);
    let labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "offset_cases_count".to_string(),
        ),
        ("route".to_string(), "/offsets".to_string()),
    ];
    assert_eq!(
        execution_rows(&base),
        vec![(
            labels.clone(),
            vec![
                (0, 9.0_f64.to_bits()),
                (10_000, 2.0_f64.to_bits()),
                (20_000, 4.0_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        execution_rows(&positive),
        vec![(
            labels.clone(),
            vec![
                (5_000, 9.0_f64.to_bits()),
                (15_000, 2.0_f64.to_bits()),
                (25_000, 4.0_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        execution_rows(&negative),
        vec![(
            labels.clone(),
            vec![(5_000, 2.0_f64.to_bits()), (15_000, 4.0_f64.to_bits()),],
        )]
    );
    assert_eq!(
        execution_rows(&nested),
        vec![(
            labels.clone(),
            vec![(15_000, 2.0_f64.to_bits()), (25_000, 4.0_f64.to_bits()),],
        )]
    );
    assert_eq!(
        execution_rows(&epoch),
        vec![(labels, vec![(1_000, 9.0_f64.to_bits())])]
    );
    assert_eq!(
        [
            base.stats,
            positive.stats,
            negative.stats,
            nested.stats,
            epoch.stats
        ],
        [
            exact_stats(6, 0, 3, 3, 3, 3, 5, 502, 11, 5),
            exact_stats(6, 0, 3, 3, 3, 3, 5, 502, 11, 5),
            exact_stats(4, 0, 2, 2, 2, 2, 4, 388, 8, 4),
            exact_stats(6, 0, 3, 3, 3, 3, 5, 502, 11, 5),
            exact_stats(2, 0, 1, 1, 1, 1, 1, 114, 3, 1),
        ]
    );
    assert_eq!(
        [
            base.semantic_fingerprint_sha256().to_hex(),
            positive.semantic_fingerprint_sha256().to_hex(),
            negative.semantic_fingerprint_sha256().to_hex(),
            nested.semantic_fingerprint_sha256().to_hex(),
            epoch.semantic_fingerprint_sha256().to_hex(),
        ],
        [
            "f7befb7b16d0d500628e3b31a38a4b7c5ae66ee9dc62b6d9cabbe9cd804bed64".to_string(),
            "fbfe486ef27d7962a4d12d9979f759afe8442055a711a3a179bf2ce862fb254b".to_string(),
            "5db1139f5580d1deefa0532e42feff27c3717bfba3f9906808db4df5e63c0a71".to_string(),
            "a435c893915087a66876c4bba7b36176853b29171aed14294cb53e463e29b584".to_string(),
            "67771bd335b13badd48eacba16ec67b53ad3e319a1a42a81f01ca5913a4dd515".to_string(),
        ]
    );
}

#[test]
fn prechange_mixed_temporality_is_explicit_within_and_across_segments() {
    let fixture = write_mixed_temporality_fixture();
    let within_count = fixture.run_range("mixed_within_count", 1_000, 3_000, 1_000);
    let within_sum = fixture.run_range("mixed_within_sum", 1_000, 3_000, 1_000);
    let within_resets = fixture.run_range("resets(mixed_within_count[5s])", 3_000, 3_000, 1_000);
    let across_count = fixture.run_range("mixed_across_count", 5_000, 25_000, 10_000);
    let across_sum = fixture.run_range("mixed_across_sum", 5_000, 25_000, 10_000);
    let across_resets = fixture.run_range("resets(mixed_across_count[30s])", 25_000, 25_000, 1_000);

    assert_eq!(
        execution_rows(&within_count),
        vec![(
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "mixed_within_count".to_string()
                ),
                ("boundary".to_string(), "within-chunk".to_string()),
            ],
            vec![
                (1_000, 10.0_f64.to_bits()),
                (2_000, 3.0_f64.to_bits()),
                (3_000, 14.0_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        execution_rows(&within_sum),
        vec![(
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "mixed_within_sum".to_string()
                ),
                ("boundary".to_string(), "within-chunk".to_string()),
            ],
            vec![
                (1_000, 100.0_f64.to_bits()),
                (2_000, 30.0_f64.to_bits()),
                (3_000, 140.0_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        execution_rows(&within_resets),
        vec![(
            vec![("boundary".to_string(), "within-chunk".to_string())],
            vec![(3_000, 1.0_f64.to_bits())],
        )]
    );
    assert_eq!(
        execution_rows(&across_count),
        vec![(
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "mixed_across_count".to_string()
                ),
                ("boundary".to_string(), "across-segments".to_string()),
            ],
            vec![
                (5_000, 5.0_f64.to_bits()),
                (15_000, 2.0_f64.to_bits()),
                (25_000, 9.0_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        execution_rows(&across_sum),
        vec![(
            vec![
                (
                    METRIC_NAME_LABEL.to_string(),
                    "mixed_across_sum".to_string()
                ),
                ("boundary".to_string(), "across-segments".to_string()),
            ],
            vec![
                (5_000, 50.0_f64.to_bits()),
                (15_000, 20.0_f64.to_bits()),
                (25_000, 90.0_f64.to_bits()),
            ],
        )]
    );
    assert_eq!(
        execution_rows(&across_resets),
        vec![(
            vec![("boundary".to_string(), "across-segments".to_string())],
            vec![(25_000, 1.0_f64.to_bits())],
        )]
    );
    assert_eq!(
        [
            within_count.stats,
            within_sum.stats,
            within_resets.stats,
            across_count.stats,
            across_sum.stats,
            across_resets.stats,
        ],
        [
            exact_stats(18, 12, 3, 3, 3, 3, 3, 342, 9, 3),
            exact_stats(18, 12, 3, 3, 3, 3, 3, 342, 9, 3),
            exact_stats(6, 4, 1, 1, 1, 1, 1, 114, 3, 1),
            exact_stats(18, 6, 6, 6, 3, 3, 6, 480, 6, 6),
            exact_stats(18, 6, 6, 6, 3, 3, 6, 480, 6, 6),
            exact_stats(6, 0, 3, 3, 1, 1, 3, 240, 3, 3),
        ]
    );
    assert_eq!(
        [
            within_count.semantic_fingerprint_sha256().to_hex(),
            within_sum.semantic_fingerprint_sha256().to_hex(),
            within_resets.semantic_fingerprint_sha256().to_hex(),
            across_count.semantic_fingerprint_sha256().to_hex(),
            across_sum.semantic_fingerprint_sha256().to_hex(),
            across_resets.semantic_fingerprint_sha256().to_hex(),
        ],
        [
            "ea9382cdea6d8b58a1923abd1a3a51841fcc645a2ac4047bdf4f907332cc9fe2".to_string(),
            "1a790f14120416a0b4a5a537d463fca870584850e0025064eaf5f90852c8884b".to_string(),
            "d34c456b9904bea04cf5e36ed39ff91e8467dc334b3f9140de0f96a129f98f35".to_string(),
            "7d11995fc36557d921165aebaf793607d93bbf6455410bcdb500375e0e8dd01c".to_string(),
            "79365063a39ab54996a993b524605d8a65275392ef2df7d368a98459ab8e3638".to_string(),
            "1b071c5536c0aee30894ecbd902e20a4ba1b68d7ef7403d26729c18ad6895051".to_string(),
        ]
    );
}

#[test]
fn prechange_histogram_and_exphist_missing_sum_nan_and_stale_are_bit_exact() {
    let fixture = write_missing_sum_nan_fixture();
    let labels = |metric: &str, case: &str, kind: &str| {
        vec![
            (METRIC_NAME_LABEL.to_string(), metric.to_string()),
            ("case".to_string(), case.to_string()),
            ("kind".to_string(), kind.to_string()),
        ]
    };
    let histogram_count = fixture.run_instant("hist_sum_cases_count", 10_000, 10_000);
    assert_eq!(
        execution_rows(&histogram_count),
        vec![
            (
                labels("hist_sum_cases_count", "stale-missing", "histogram"),
                vec![(10_000, STALE_NAN_BITS)],
            ),
            (
                labels("hist_sum_cases_count", "recorded-missing", "histogram"),
                vec![(10_000, 3.0_f64.to_bits())],
            ),
            (
                labels("hist_sum_cases_count", "recorded-nan", "histogram"),
                vec![(10_000, 4.0_f64.to_bits())],
            ),
            (
                labels("hist_sum_cases_count", "stale-nan", "histogram"),
                vec![(10_000, STALE_NAN_BITS)],
            ),
        ]
    );

    let histogram_sum = fixture.run_instant("hist_sum_cases_sum", 10_000, 10_000);
    let histogram_sum_range = fixture.run_range("hist_sum_cases_sum", 10_000, 10_000, 1_000);
    let exphist_count = fixture.run_instant("exphist_sum_cases_count", 10_000, 10_000);
    let exphist_sum = fixture.run_instant("exphist_sum_cases_sum", 10_000, 10_000);
    let exphist_sum_range = fixture.run_range("exphist_sum_cases_sum", 10_000, 10_000, 1_000);

    assert_eq!(
        execution_rows(&histogram_sum),
        vec![
            (
                labels("hist_sum_cases_sum", "stale-missing", "histogram"),
                vec![(10_000, STALE_NAN_BITS)],
            ),
            (
                labels("hist_sum_cases_sum", "recorded-nan", "histogram"),
                vec![(10_000, ORDINARY_NAN_BITS)],
            ),
            (
                labels("hist_sum_cases_sum", "stale-nan", "histogram"),
                vec![(10_000, STALE_NAN_BITS)],
            ),
        ]
    );
    assert_eq!(
        execution_rows(&histogram_sum_range),
        vec![(
            labels("hist_sum_cases_sum", "recorded-nan", "histogram"),
            vec![(10_000, ORDINARY_NAN_BITS)],
        )]
    );
    assert_eq!(
        execution_rows(&exphist_count),
        vec![
            (
                labels(
                    "exphist_sum_cases_count",
                    "stale-nan",
                    "exponential_histogram",
                ),
                vec![(10_000, STALE_NAN_BITS)],
            ),
            (
                labels(
                    "exphist_sum_cases_count",
                    "recorded-missing",
                    "exponential_histogram",
                ),
                vec![(10_000, 3.0_f64.to_bits())],
            ),
            (
                labels(
                    "exphist_sum_cases_count",
                    "stale-missing",
                    "exponential_histogram",
                ),
                vec![(10_000, STALE_NAN_BITS)],
            ),
            (
                labels(
                    "exphist_sum_cases_count",
                    "recorded-nan",
                    "exponential_histogram",
                ),
                vec![(10_000, 4.0_f64.to_bits())],
            ),
        ]
    );
    assert_eq!(
        execution_rows(&exphist_sum),
        vec![
            (
                labels(
                    "exphist_sum_cases_sum",
                    "stale-missing",
                    "exponential_histogram",
                ),
                vec![(10_000, STALE_NAN_BITS)],
            ),
            (
                labels(
                    "exphist_sum_cases_sum",
                    "recorded-nan",
                    "exponential_histogram",
                ),
                vec![(10_000, ORDINARY_NAN_BITS)],
            ),
            (
                labels(
                    "exphist_sum_cases_sum",
                    "stale-nan",
                    "exponential_histogram",
                ),
                vec![(10_000, STALE_NAN_BITS)],
            ),
        ]
    );
    assert_eq!(
        execution_rows(&exphist_sum_range),
        vec![(
            labels(
                "exphist_sum_cases_sum",
                "recorded-nan",
                "exponential_histogram",
            ),
            vec![(10_000, ORDINARY_NAN_BITS)],
        )]
    );
    assert_ne!(ORDINARY_NAN_BITS, STALE_NAN_BITS);
    let expected_stats = |projected_series| QueryStats {
        segments_considered: 2,
        segments_skipped_by_time: 0,
        segments_skipped_by_missing_equality: 1,
        segments_skipped_by_matcher_time_range: 0,
        segments_queried: 1,
        matched_series: 4,
        projected_series,
        chunk_reads: 4,
        bytes_read: 304,
        samples_decoded: 4,
        typed_scalar_chunks_decoded: 4,
        typed_full_chunks_decoded: 0,
        regex_values_examined: 0,
        index_postings_reads: 0,
        index_postings_bytes_read: 0,
    };
    assert_eq!(
        [
            histogram_count.stats,
            histogram_sum.stats,
            histogram_sum_range.stats,
            exphist_count.stats,
            exphist_sum.stats,
            exphist_sum_range.stats,
        ],
        [
            expected_stats(4),
            expected_stats(3),
            expected_stats(3),
            expected_stats(4),
            expected_stats(3),
            expected_stats(3),
        ]
    );
    assert_eq!(
        [
            histogram_count.semantic_fingerprint_sha256().to_hex(),
            histogram_sum.semantic_fingerprint_sha256().to_hex(),
            histogram_sum_range.semantic_fingerprint_sha256().to_hex(),
            exphist_count.semantic_fingerprint_sha256().to_hex(),
            exphist_sum.semantic_fingerprint_sha256().to_hex(),
            exphist_sum_range.semantic_fingerprint_sha256().to_hex(),
        ],
        [
            "8a5368d8d7c3aa53a564ee649fc81d67a468975dbf3c29f5e9663747e7d419d1".to_string(),
            "1942871ff7a81ba64f68e81d201860b2387399ff5d2ac4e74a5671048848e6eb".to_string(),
            "949cc1b5d949ba9393a2c2c19ba0c819730d7cd47106caef321aade714825840".to_string(),
            "a6c996c3c854cb32021fae99e9991ac5fd460f3ce5d229daf0ef8e2dfa08b760".to_string(),
            "da00fa6eb3750efd3020d090a9d49b41b89055b8820f080dbd40e9d98246e2fa".to_string(),
            "55997192223c95e3a4593fcd90d11ffba03a873230367698aa341608af4b9762".to_string(),
        ]
    );
}
