#[allow(dead_code)]
#[path = "support/promql_range_scalar_cache.rs"]
mod support;

use std::time::Duration;

use chronoxide_core::storage::segment::{
    QueryExecution, QueryLimits, QueryStats, RangeScalarCacheSummary, SegmentStoreQueryProfile,
    SegmentStoreQuerySessionStats,
};
use support::{
    CacheBypassKind, LARGE_COUNT, ORDINARY_NAN_BITS, STALE_NAN_BITS, TypedRangeFixture,
    exact_stats, execution_rows, write_duplicate_offset_fixture, write_large_count_fixture,
    write_missing_sum_nan_fixture, write_mixed_temporality_fixture,
    write_scalar_cache_bypass_fixture, write_stale_reset_delta_fixture,
    write_start_time_reset_fixture, write_summary_scalar_fixture,
};

const CACHE_BUDGET_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug)]
struct SessionRun {
    execution: QueryExecution,
    stats: SegmentStoreQuerySessionStats,
    profile: SegmentStoreQueryProfile,
    summary: RangeScalarCacheSummary,
}

type ExecutionRows = Vec<(Vec<(String, String)>, Vec<(u64, u64)>)>;

struct SemanticMatrixCase {
    id: &'static str,
    fixture: fn() -> TypedRangeFixture,
    query: &'static str,
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
    expected_rows: ExecutionRows,
    expected_stats: Option<QueryStats>,
    expected_fingerprint: Option<&'static str>,
    expect_cache_hits: bool,
}

fn run_session_range(
    fixture: TypedRangeFixture,
    query: &str,
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
    cache_budget_bytes: u64,
) -> SessionRun {
    let mut session = fixture.store.query_session().unwrap();
    session
        .set_range_scalar_cache_budget_bytes(cache_budget_bytes)
        .unwrap();
    let before_stats = session.stats();
    let before_profile = session.profile();
    let execution = session
        .query_promql_range_with_limits(query, start_ms, end_ms, step_ms, QueryLimits::unlimited())
        .unwrap();
    SessionRun {
        execution,
        stats: session.stats().delta_since(before_stats),
        profile: session.profile().delta_since(before_profile),
        summary: session.last_range_scalar_cache_summary().copied().unwrap(),
    }
}

fn run_matrix_case(case: &SemanticMatrixCase, cache_budget_bytes: u64) -> SessionRun {
    run_session_range(
        (case.fixture)(),
        case.query,
        case.start_ms,
        case.end_ms,
        case.step_ms,
        cache_budget_bytes,
    )
}

fn normalized_logical_profile(mut profile: SegmentStoreQueryProfile) -> SegmentStoreQueryProfile {
    profile.index_routing_open = Duration::ZERO;
    profile.segment_context_open = Duration::ZERO;
    profile.indexes_open = Duration::ZERO;
    profile.symbols_read = Duration::ZERO;
    profile.series_open = Duration::ZERO;
    profile.chunk_index_open = Duration::ZERO;
    profile.chunks_open = Duration::ZERO;
    profile.routing_index_read = Duration::ZERO;
    profile.exact_postings_read = Duration::ZERO;
    profile.metric_series_ranges_read = Duration::ZERO;
    profile.series_entry_read = Duration::ZERO;
    profile.chunk_index_range_read = Duration::ZERO;
    profile.chunk_read = Duration::ZERO;
    profile.chunk_payload_physical_reads = 0;
    profile.chunk_payload_physical_bytes = 0;
    profile
}

fn metric_labels(metric: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
    std::iter::once(("__name__".to_string(), metric.to_string()))
        .chain(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        )
        .collect()
}

fn label_only(extra: &[(&str, &str)]) -> Vec<(String, String)> {
    extra
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn assert_execution_bit_exact(id: &str, left: &QueryExecution, right: &QueryExecution) {
    assert_eq!(left.stats, right.stats, "{id} QueryStats");
    assert_eq!(left.results.len(), right.results.len(), "{id} result count");
    for (result_index, (left, right)) in left.results.iter().zip(&right.results).enumerate() {
        assert_eq!(
            left.series_id, right.series_id,
            "{id} result {result_index} series id"
        );
        assert_eq!(
            left.labels, right.labels,
            "{id} result {result_index} labels"
        );
        assert_eq!(
            left.counter_reset_hints, right.counter_reset_hints,
            "{id} result {result_index} reset hints"
        );
        assert_eq!(
            left.samples.len(),
            right.samples.len(),
            "{id} result {result_index} sample count"
        );
        for (sample_index, (left, right)) in left.samples.iter().zip(&right.samples).enumerate() {
            assert_eq!(
                (left.0, left.1.to_bits()),
                (right.0, right.1.to_bits()),
                "{id} result {result_index} sample {sample_index}"
            );
        }
    }
    // The public semantic fingerprint additionally covers the private start-
    // time and result-temporality fields, including their vector lengths.
    assert_eq!(
        left.semantic_fingerprint_sha256(),
        right.semantic_fingerprint_sha256(),
        "{id} private typed result metadata"
    );
}

fn assert_mode_equivalence(
    case: &SemanticMatrixCase,
    cache_off: &SessionRun,
    cache_on: &SessionRun,
) {
    assert_execution_bit_exact(case.id, &cache_on.execution, &cache_off.execution);
    assert_eq!(
        cache_on.execution.semantic_fingerprint_sha256(),
        cache_off.execution.semantic_fingerprint_sha256(),
        "{} changed the bit-exact semantic fingerprint",
        case.id
    );
    assert_eq!(
        cache_on.execution.stats, cache_off.execution.stats,
        "{} changed public QueryStats",
        case.id
    );
    assert_eq!(
        cache_on.stats, cache_off.stats,
        "{} changed session stats",
        case.id
    );
    assert_eq!(
        normalized_logical_profile(cache_on.profile),
        normalized_logical_profile(cache_off.profile),
        "{} changed the normalized logical profile",
        case.id
    );

    assert_eq!(
        execution_rows(&cache_off.execution),
        case.expected_rows,
        "{} drifted from the explicit pre-change values",
        case.id
    );
    if let Some(expected_stats) = case.expected_stats {
        assert_eq!(
            cache_off.execution.stats, expected_stats,
            "{} drifted from pre-change QueryStats",
            case.id
        );
    }
    if let Some(expected_fingerprint) = case.expected_fingerprint {
        assert_eq!(
            cache_off.execution.semantic_fingerprint_sha256().to_hex(),
            expected_fingerprint,
            "{} drifted from the pre-change fingerprint",
            case.id
        );
    }
}

fn assert_zero_retained_charge(id: &str, summary: RangeScalarCacheSummary) {
    assert_eq!(
        summary.retained_charge_after_finalize, 0,
        "{id} retained cache charge after finalization: {summary:?}"
    );
}

fn assert_eligible_cache_behavior(
    case: &SemanticMatrixCase,
    cache_off: &SessionRun,
    cache_on: &SessionRun,
) {
    assert_eq!(cache_off.summary.configured_budget_bytes, 0, "{}", case.id);
    assert_eq!(cache_off.summary.governor_lease_bytes, 0, "{}", case.id);
    assert_eq!(cache_off.summary.hits, 0, "{}", case.id);
    assert_eq!(cache_off.summary.admitted_entries, 0, "{}", case.id);
    assert!(
        cache_off.summary.misses > 0,
        "{}: {:?}",
        case.id,
        cache_off.summary
    );
    assert_eq!(
        cache_off.summary.streaming_budget_bypasses, cache_off.summary.misses,
        "{}: {:?}",
        case.id, cache_off.summary
    );
    assert_eq!(cache_off.summary.unsupported_bypasses, 0, "{}", case.id);
    assert_eq!(cache_off.summary.logical_hit_bytes, 0, "{}", case.id);
    assert_eq!(
        cache_off.summary.logical_miss_or_bypass_bytes, cache_off.profile.chunk_payload_bytes,
        "{} cache-off logical byte accounting",
        case.id
    );
    assert_zero_retained_charge(case.id, cache_off.summary);

    assert_eq!(
        cache_on.summary.configured_budget_bytes, CACHE_BUDGET_BYTES,
        "{}",
        case.id
    );
    assert_eq!(
        cache_on.summary.governor_lease_bytes, CACHE_BUDGET_BYTES,
        "{}: {:?}",
        case.id, cache_on.summary
    );
    assert!(!cache_on.summary.governor_refused, "{}", case.id);
    assert!(!cache_on.summary.allocation_refused, "{}", case.id);
    assert!(!cache_on.summary.layout_overflow, "{}", case.id);
    assert!(
        cache_on.summary.misses > 0,
        "{}: {:?}",
        case.id,
        cache_on.summary
    );
    assert!(
        cache_on.summary.admitted_entries > 0,
        "{}: {:?}",
        case.id,
        cache_on.summary
    );
    assert_eq!(cache_on.summary.streaming_budget_bypasses, 0, "{}", case.id);
    assert_eq!(cache_on.summary.unsupported_bypasses, 0, "{}", case.id);
    assert_eq!(
        cache_on
            .summary
            .logical_hit_bytes
            .saturating_add(cache_on.summary.logical_miss_or_bypass_bytes),
        cache_on.profile.chunk_payload_bytes,
        "{} cache-on logical byte accounting",
        case.id
    );
    assert_eq!(
        cache_on.summary.peak_retained_charge_bytes,
        cache_on
            .summary
            .entry_arena_charge_bytes
            .saturating_add(cache_on.summary.sample_arena_charge_bytes),
        "{}: {:?}",
        case.id,
        cache_on.summary
    );
    assert!(
        cache_on.summary.peak_retained_charge_bytes <= CACHE_BUDGET_BYTES,
        "{}: {:?}",
        case.id,
        cache_on.summary
    );
    assert_zero_retained_charge(case.id, cache_on.summary);

    if case.expect_cache_hits {
        assert!(
            cache_on.summary.hits > 0,
            "{}: {:?}",
            case.id,
            cache_on.summary
        );
        assert!(
            cache_on.profile.chunk_payload_physical_bytes
                < cache_off.profile.chunk_payload_physical_bytes,
            "{} did not reduce physical bytes: off={} on={} summary={:?}",
            case.id,
            cache_off.profile.chunk_payload_physical_bytes,
            cache_on.profile.chunk_payload_physical_bytes,
            cache_on.summary
        );
    } else {
        assert_eq!(
            cache_on.summary.hits, 0,
            "{}: {:?}",
            case.id, cache_on.summary
        );
        assert_eq!(
            cache_on.profile.chunk_payload_physical_bytes,
            cache_off.profile.chunk_payload_physical_bytes,
            "{} reduced physical bytes without an eligible repeated read",
            case.id
        );
    }
}

fn semantic_matrix_cases() -> Vec<SemanticMatrixCase> {
    let stale_samples = vec![
        (10_000, 2.0_f64.to_bits()),
        (20_000, 5.0_f64.to_bits()),
        (40_000, 7.0_f64.to_bits()),
        (50_000, 11.0_f64.to_bits()),
    ];
    let stale_stats = exact_stats(10, 0, 5, 5, 5, 5, 5, 750, 25, 5);
    let missing_sum_stats = exact_stats(2, 0, 1, 1, 4, 3, 4, 304, 4, 4);
    let missing_count_stats = exact_stats(2, 0, 1, 1, 4, 4, 4, 304, 4, 4);

    vec![
        SemanticMatrixCase {
            id: "histogram_count_stale_reset_delta",
            fixture: write_stale_reset_delta_fixture,
            query: "cache_count",
            start_ms: 10_000,
            end_ms: 50_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("cache_count", &[("route", "/stale-reset")]),
                stale_samples.clone(),
            )],
            expected_stats: Some(stale_stats),
            expected_fingerprint: Some(
                "121448046eefeb19ec38a1d67ef0871bfcc10a52018e34249757f5cf167291c3",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "histogram_sum_stale_reset_delta",
            fixture: write_stale_reset_delta_fixture,
            query: "cache_sum",
            start_ms: 10_000,
            end_ms: 50_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("cache_sum", &[("route", "/stale-reset")]),
                stale_samples,
            )],
            expected_stats: Some(stale_stats),
            expected_fingerprint: Some(
                "e65a0344a041df5ed2c39ea934e341572bdf8c1704e0beb5cface12fc17197c3",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "histogram_sum_absent_nan_and_stale",
            fixture: write_missing_sum_nan_fixture,
            query: "hist_sum_cases_sum",
            start_ms: 10_000,
            end_ms: 10_000,
            step_ms: 1_000,
            expected_rows: vec![(
                metric_labels(
                    "hist_sum_cases_sum",
                    &[("case", "recorded-nan"), ("kind", "histogram")],
                ),
                vec![(10_000, ORDINARY_NAN_BITS)],
            )],
            expected_stats: Some(missing_sum_stats),
            expected_fingerprint: Some(
                "949cc1b5d949ba9393a2c2c19ba0c819730d7cd47106caef321aade714825840",
            ),
            expect_cache_hits: false,
        },
        SemanticMatrixCase {
            id: "exponential_histogram_count_absent_nan_and_stale",
            fixture: write_missing_sum_nan_fixture,
            query: "exphist_sum_cases_count",
            start_ms: 10_000,
            end_ms: 10_000,
            step_ms: 1_000,
            expected_rows: vec![
                (
                    metric_labels(
                        "exphist_sum_cases_count",
                        &[
                            ("case", "recorded-missing"),
                            ("kind", "exponential_histogram"),
                        ],
                    ),
                    vec![(10_000, 3.0_f64.to_bits())],
                ),
                (
                    metric_labels(
                        "exphist_sum_cases_count",
                        &[("case", "recorded-nan"), ("kind", "exponential_histogram")],
                    ),
                    vec![(10_000, 4.0_f64.to_bits())],
                ),
            ],
            expected_stats: Some(missing_count_stats),
            expected_fingerprint: None,
            expect_cache_hits: false,
        },
        SemanticMatrixCase {
            id: "exponential_histogram_sum_absent_nan_and_stale",
            fixture: write_missing_sum_nan_fixture,
            query: "exphist_sum_cases_sum",
            start_ms: 10_000,
            end_ms: 10_000,
            step_ms: 1_000,
            expected_rows: vec![(
                metric_labels(
                    "exphist_sum_cases_sum",
                    &[("case", "recorded-nan"), ("kind", "exponential_histogram")],
                ),
                vec![(10_000, ORDINARY_NAN_BITS)],
            )],
            expected_stats: Some(missing_sum_stats),
            expected_fingerprint: Some(
                "55997192223c95e3a4593fcd90d11ffba03a873230367698aa341608af4b9762",
            ),
            expect_cache_hits: false,
        },
        SemanticMatrixCase {
            id: "summary_count_unspecified_temporality",
            fixture: write_summary_scalar_fixture,
            query: "summary_cache_count",
            start_ms: 10_000,
            end_ms: 30_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("summary_cache_count", &[("route", "/summary")]),
                vec![
                    (10_000, 2.0_f64.to_bits()),
                    (20_000, 3.0_f64.to_bits()),
                    (30_000, 5.0_f64.to_bits()),
                ],
            )],
            expected_stats: None,
            expected_fingerprint: None,
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "summary_sum_unspecified_temporality",
            fixture: write_summary_scalar_fixture,
            query: "summary_cache_sum",
            start_ms: 10_000,
            end_ms: 30_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("summary_cache_sum", &[("route", "/summary")]),
                vec![
                    (10_000, 20.0_f64.to_bits()),
                    (20_000, 30.0_f64.to_bits()),
                    (30_000, 50.0_f64.to_bits()),
                ],
            )],
            expected_stats: None,
            expected_fingerprint: None,
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "mixed_temporality_within_chunk_count",
            fixture: write_mixed_temporality_fixture,
            query: "mixed_within_count",
            start_ms: 1_000,
            end_ms: 3_000,
            step_ms: 1_000,
            expected_rows: vec![(
                metric_labels("mixed_within_count", &[("boundary", "within-chunk")]),
                vec![
                    (1_000, 10.0_f64.to_bits()),
                    (2_000, 3.0_f64.to_bits()),
                    (3_000, 14.0_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(18, 12, 3, 3, 3, 3, 3, 342, 9, 3)),
            expected_fingerprint: Some(
                "ea9382cdea6d8b58a1923abd1a3a51841fcc645a2ac4047bdf4f907332cc9fe2",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "mixed_temporality_within_chunk_sum",
            fixture: write_mixed_temporality_fixture,
            query: "mixed_within_sum",
            start_ms: 1_000,
            end_ms: 3_000,
            step_ms: 1_000,
            expected_rows: vec![(
                metric_labels("mixed_within_sum", &[("boundary", "within-chunk")]),
                vec![
                    (1_000, 100.0_f64.to_bits()),
                    (2_000, 30.0_f64.to_bits()),
                    (3_000, 140.0_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(18, 12, 3, 3, 3, 3, 3, 342, 9, 3)),
            expected_fingerprint: Some(
                "1a790f14120416a0b4a5a537d463fca870584850e0025064eaf5f90852c8884b",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "mixed_temporality_across_segments_count",
            fixture: write_mixed_temporality_fixture,
            query: "mixed_across_count",
            start_ms: 5_000,
            end_ms: 25_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("mixed_across_count", &[("boundary", "across-segments")]),
                vec![
                    (5_000, 5.0_f64.to_bits()),
                    (15_000, 2.0_f64.to_bits()),
                    (25_000, 9.0_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(18, 6, 6, 6, 3, 3, 6, 480, 6, 6)),
            expected_fingerprint: Some(
                "7d11995fc36557d921165aebaf793607d93bbf6455410bcdb500375e0e8dd01c",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "mixed_temporality_across_segments_sum",
            fixture: write_mixed_temporality_fixture,
            query: "mixed_across_sum",
            start_ms: 5_000,
            end_ms: 25_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("mixed_across_sum", &[("boundary", "across-segments")]),
                vec![
                    (5_000, 50.0_f64.to_bits()),
                    (15_000, 20.0_f64.to_bits()),
                    (25_000, 90.0_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(18, 6, 6, 6, 3, 3, 6, 480, 6, 6)),
            expected_fingerprint: Some(
                "79365063a39ab54996a993b524605d8a65275392ef2df7d368a98459ab8e3638",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "start_time_reset_increase",
            fixture: write_start_time_reset_fixture,
            query: "increase(start_time_cases_count[20s])",
            start_ms: 20_000,
            end_ms: 40_000,
            step_ms: 10_000,
            expected_rows: vec![(
                label_only(&[("route", "/start-reset")]),
                vec![
                    (20_000, 5.0_f64.to_bits()),
                    (30_000, 10.0_f64.to_bits()),
                    (40_000, 11.0_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(6, 0, 3, 3, 3, 3, 3, 414, 12, 3)),
            expected_fingerprint: Some(
                "32746872ded0897e349097b735786996f14807abe731ba2f00f4fbe31d73cb06",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "start_time_reset_rate",
            fixture: write_start_time_reset_fixture,
            query: "rate(start_time_cases_count[20s])",
            start_ms: 20_000,
            end_ms: 40_000,
            step_ms: 10_000,
            expected_rows: vec![(
                label_only(&[("route", "/start-reset")]),
                vec![
                    (20_000, 0.25_f64.to_bits()),
                    (30_000, 0.5_f64.to_bits()),
                    (40_000, 0.55_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(6, 0, 3, 3, 3, 3, 3, 414, 12, 3)),
            expected_fingerprint: Some(
                "433e3498bb7ead4e6aa47f136d3268a5807061fccb003b540ee60db788c5b536",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "large_count_u64_before_f64_projection",
            fixture: write_large_count_fixture,
            query: "large_count_cases_count",
            start_ms: 1_000,
            end_ms: 2_000,
            step_ms: 1_000,
            expected_rows: vec![(
                metric_labels("large_count_cases_count", &[("route", "/large")]),
                vec![
                    (1_000, (LARGE_COUNT as f64).to_bits()),
                    (2_000, ((LARGE_COUNT + 1) as f64).to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(4, 0, 2, 2, 2, 2, 2, 178, 4, 2)),
            expected_fingerprint: Some(
                "2cec37f6b6a12d28ab8cf6d9c3e41ff1d650c2b34d8ea5ab659efeb8853bfc51",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "duplicate_keep_last_base",
            fixture: write_duplicate_offset_fixture,
            query: "offset_cases_count",
            start_ms: 0,
            end_ms: 20_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("offset_cases_count", &[("route", "/offsets")]),
                vec![
                    (0, 9.0_f64.to_bits()),
                    (10_000, 2.0_f64.to_bits()),
                    (20_000, 4.0_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(6, 0, 3, 3, 3, 3, 5, 502, 11, 5)),
            expected_fingerprint: Some(
                "f7befb7b16d0d500628e3b31a38a4b7c5ae66ee9dc62b6d9cabbe9cd804bed64",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "positive_offset",
            fixture: write_duplicate_offset_fixture,
            query: "offset_cases_count offset 5s",
            start_ms: 5_000,
            end_ms: 25_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("offset_cases_count", &[("route", "/offsets")]),
                vec![
                    (5_000, 9.0_f64.to_bits()),
                    (15_000, 2.0_f64.to_bits()),
                    (25_000, 4.0_f64.to_bits()),
                ],
            )],
            expected_stats: Some(exact_stats(6, 0, 3, 3, 3, 3, 5, 502, 11, 5)),
            expected_fingerprint: Some(
                "fbfe486ef27d7962a4d12d9979f759afe8442055a711a3a179bf2ce862fb254b",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "negative_offset",
            fixture: write_duplicate_offset_fixture,
            query: "offset_cases_count offset -5s",
            start_ms: 5_000,
            end_ms: 15_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("offset_cases_count", &[("route", "/offsets")]),
                vec![(5_000, 2.0_f64.to_bits()), (15_000, 4.0_f64.to_bits())],
            )],
            expected_stats: Some(exact_stats(4, 0, 2, 2, 2, 2, 4, 388, 8, 4)),
            expected_fingerprint: Some(
                "5db1139f5580d1deefa0532e42feff27c3717bfba3f9906808db4df5e63c0a71",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "nested_offset",
            fixture: write_duplicate_offset_fixture,
            query: "last_over_time(offset_cases_count[1s] offset 5s)",
            start_ms: 5_000,
            end_ms: 25_000,
            step_ms: 10_000,
            expected_rows: vec![(
                metric_labels("offset_cases_count", &[("route", "/offsets")]),
                vec![(15_000, 2.0_f64.to_bits()), (25_000, 4.0_f64.to_bits())],
            )],
            expected_stats: Some(exact_stats(6, 0, 3, 3, 3, 3, 5, 502, 11, 5)),
            expected_fingerprint: Some(
                "a435c893915087a66876c4bba7b36176853b29171aed14294cb53e463e29b584",
            ),
            expect_cache_hits: true,
        },
        SemanticMatrixCase {
            id: "epoch_saturating_offset",
            fixture: write_duplicate_offset_fixture,
            query: "offset_cases_count offset 2s",
            start_ms: 1_000,
            end_ms: 1_000,
            step_ms: 1_000,
            expected_rows: vec![(
                metric_labels("offset_cases_count", &[("route", "/offsets")]),
                vec![(1_000, 9.0_f64.to_bits())],
            )],
            expected_stats: Some(exact_stats(2, 0, 1, 1, 1, 1, 1, 114, 3, 1)),
            expected_fingerprint: Some(
                "67771bd335b13badd48eacba16ec67b53ad3e319a1a42a81f01ca5913a4dd515",
            ),
            expect_cache_hits: false,
        },
    ]
}

fn assert_bypass_summary(id: &str, run: &SessionRun, configured_budget_bytes: u64) {
    let summary = run.summary;
    assert_eq!(
        summary.configured_budget_bytes, configured_budget_bytes,
        "{id}"
    );
    assert_eq!(summary.governor_lease_bytes, 0, "{id}: {summary:?}");
    assert!(!summary.governor_refused, "{id}: {summary:?}");
    assert!(!summary.allocation_refused, "{id}: {summary:?}");
    assert!(!summary.layout_overflow, "{id}: {summary:?}");
    assert_eq!(summary.entry_arena_charge_bytes, 0, "{id}: {summary:?}");
    assert_eq!(summary.sample_arena_charge_bytes, 0, "{id}: {summary:?}");
    assert_eq!(summary.hits, 0, "{id}: {summary:?}");
    assert_eq!(summary.misses, 0, "{id}: {summary:?}");
    assert_eq!(summary.admitted_entries, 0, "{id}: {summary:?}");
    assert_eq!(summary.streaming_budget_bypasses, 0, "{id}: {summary:?}");
    assert!(summary.unsupported_bypasses > 0, "{id}: {summary:?}");
    assert_eq!(summary.logical_hit_bytes, 0, "{id}: {summary:?}");
    assert_eq!(
        summary.logical_miss_or_bypass_bytes, run.profile.chunk_payload_bytes,
        "{id}: {summary:?}"
    );
    assert_eq!(summary.peak_retained_charge_bytes, 0, "{id}: {summary:?}");
    assert_zero_retained_charge(id, summary);
}

#[test]
fn range_scalar_cache_session_reuses_scalar_lane_across_overlapping_windows() {
    let cache_off = run_session_range(
        write_stale_reset_delta_fixture(),
        "rate(cache_count[30s])",
        30_000,
        50_000,
        10_000,
        0,
    );
    let cache_on = run_session_range(
        write_stale_reset_delta_fixture(),
        "rate(cache_count[30s])",
        30_000,
        50_000,
        10_000,
        CACHE_BUDGET_BYTES,
    );

    assert_eq!(cache_on.execution.results, cache_off.execution.results);
    assert_eq!(
        cache_on.execution.semantic_fingerprint_sha256(),
        cache_off.execution.semantic_fingerprint_sha256()
    );
    assert_eq!(cache_on.execution.stats, cache_off.execution.stats);
    assert_eq!(cache_on.stats, cache_off.stats);
    assert_eq!(
        normalized_logical_profile(cache_on.profile),
        normalized_logical_profile(cache_off.profile)
    );

    assert_eq!(cache_off.summary.configured_budget_bytes, 0);
    assert_eq!(cache_off.summary.retained_charge_after_finalize, 0);
    assert_eq!(cache_on.summary.configured_budget_bytes, CACHE_BUDGET_BYTES);
    assert_eq!(cache_on.summary.retained_charge_after_finalize, 0);
    assert!(
        cache_on.summary.misses > 0
            && cache_on.summary.admitted_entries > 0
            && cache_on.summary.hits > 0
            && cache_on.profile.chunk_payload_physical_bytes
                < cache_off.profile.chunk_payload_physical_bytes,
        "cache remained inactive: summary={:?} cache_off_physical_bytes={} cache_on_physical_bytes={}",
        cache_on.summary,
        cache_off.profile.chunk_payload_physical_bytes,
        cache_on.profile.chunk_payload_physical_bytes,
    );
    assert!(cache_on.summary.logical_hit_bytes > 0);
    assert!(cache_on.summary.logical_miss_or_bypass_bytes > 0);
    assert!(cache_on.summary.peak_retained_charge_bytes <= CACHE_BUDGET_BYTES);
}

#[test]
fn range_scalar_cache_semantic_matrix_matches_prechange_oracle_in_both_modes() {
    let cases = semantic_matrix_cases();
    assert_eq!(cases.len(), 19, "semantic matrix case count changed");
    assert_ne!(
        ORDINARY_NAN_BITS, STALE_NAN_BITS,
        "ordinary NaN must remain distinguishable from the Prometheus stale marker"
    );
    for case in &cases {
        let cache_off = run_matrix_case(case, 0);
        let cache_on = run_matrix_case(case, CACHE_BUDGET_BYTES);
        assert_mode_equivalence(case, &cache_off, &cache_on);
        assert_eligible_cache_behavior(case, &cache_off, &cache_on);
    }
}

#[test]
fn range_scalar_cache_no_lane_and_nonzero_file_id_are_unsupported_bypasses() {
    let cases = [
        (
            "no_scalar_lane_count",
            CacheBypassKind::NoScalarLane,
            "cache_bypass_count",
            metric_labels("cache_bypass_count", &[("layout", "no-lane")]),
            vec![
                (10_000, 2.0_f64.to_bits()),
                (20_000, 3.0_f64.to_bits()),
                (30_000, 5.0_f64.to_bits()),
            ],
        ),
        (
            "nonzero_file_id_sum",
            CacheBypassKind::NonzeroFileId,
            "cache_bypass_sum",
            metric_labels("cache_bypass_sum", &[("layout", "nonzero-file-id")]),
            vec![
                (10_000, 20.0_f64.to_bits()),
                (20_000, 30.0_f64.to_bits()),
                (30_000, 50.0_f64.to_bits()),
            ],
        ),
    ];

    for (id, kind, query, expected_labels, expected_samples) in cases {
        let cache_off = run_session_range(
            write_scalar_cache_bypass_fixture(kind),
            query,
            10_000,
            30_000,
            10_000,
            0,
        );
        let cache_on = run_session_range(
            write_scalar_cache_bypass_fixture(kind),
            query,
            10_000,
            30_000,
            10_000,
            CACHE_BUDGET_BYTES,
        );

        assert_execution_bit_exact(id, &cache_on.execution, &cache_off.execution);
        assert_eq!(
            cache_on.execution.semantic_fingerprint_sha256(),
            cache_off.execution.semantic_fingerprint_sha256(),
            "{id} fingerprint"
        );
        assert_eq!(cache_on.execution.stats, cache_off.execution.stats, "{id}");
        assert_eq!(cache_on.stats, cache_off.stats, "{id}");
        assert_eq!(
            normalized_logical_profile(cache_on.profile),
            normalized_logical_profile(cache_off.profile),
            "{id} logical profile"
        );
        assert_eq!(
            execution_rows(&cache_off.execution),
            vec![(expected_labels, expected_samples)],
            "{id} explicit values"
        );
        assert_bypass_summary(id, &cache_off, 0);
        assert_bypass_summary(id, &cache_on, CACHE_BUDGET_BYTES);
        assert_eq!(
            cache_on.profile.chunk_payload_physical_reads,
            cache_off.profile.chunk_payload_physical_reads,
            "{id} changed physical read count despite bypass"
        );
        assert_eq!(
            cache_on.profile.chunk_payload_physical_bytes,
            cache_off.profile.chunk_payload_physical_bytes,
            "{id} changed physical bytes despite bypass"
        );
    }
}
