use super::*;
use crate::storage::index::{SegmentIndexReadCount, SegmentIndexReadStats};
use crate::storage::symbols::SegmentSymbolReadCount;

fn index_stats(multiplier: u64) -> SegmentIndexReadStats {
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

fn stage_profile(multiplier: u32) -> QueryStageProfile {
    let duration = |value: u64| Duration::from_nanos(value * u64::from(multiplier));
    QueryStageProfile {
        canonical_row_decode: duration(1),
        symbol_lookup: duration(2),
        symbol_resolution: duration(3),
        candidate_selection: duration(4),
        canonical_identity: duration(5),
        metadata_visit_overhead: duration(6),
        matcher_evaluation: duration(7),
        label_construction: duration(8),
        locator_planning: duration(9),
        payload_io: duration(10),
        payload_decode: duration(11),
        source_merge: duration(12),
        promql_grouping_evaluation: duration(13),
        result_construction: duration(14),
    }
}

fn max_stage_profile() -> QueryStageProfile {
    QueryStageProfile {
        canonical_row_decode: Duration::MAX,
        symbol_lookup: Duration::MAX,
        symbol_resolution: Duration::MAX,
        candidate_selection: Duration::MAX,
        canonical_identity: Duration::MAX,
        metadata_visit_overhead: Duration::MAX,
        matcher_evaluation: Duration::MAX,
        label_construction: Duration::MAX,
        locator_planning: Duration::MAX,
        payload_io: Duration::MAX,
        payload_decode: Duration::MAX,
        source_merge: Duration::MAX,
        promql_grouping_evaluation: Duration::MAX,
        result_construction: Duration::MAX,
    }
}

#[test]
fn query_stage_profile_add_delta_and_total_are_saturating() {
    let mut total = stage_profile(2);
    total.add(stage_profile(3));
    assert_eq!(total, stage_profile(5));
    assert_eq!(total.total_exclusive(), Duration::from_nanos(525));

    let mut before = stage_profile(2);
    before.payload_decode = Duration::MAX;
    let delta = total.delta_since(before);
    let mut expected = stage_profile(3);
    expected.payload_decode = Duration::ZERO;
    assert_eq!(delta, expected);

    let mut saturated = max_stage_profile();
    saturated.add(stage_profile(1));
    assert_eq!(saturated, max_stage_profile());
    assert_eq!(saturated.total_exclusive(), Duration::MAX);
    assert_eq!(
        stage_profile(1).delta_since(max_stage_profile()),
        QueryStageProfile::default()
    );
}

#[test]
fn segment_query_profile_adds_and_deltas_query_stages() {
    let mut profile = SegmentStoreQueryProfile {
        stages: stage_profile(2),
        ..SegmentStoreQueryProfile::default()
    };
    profile.add(SegmentStoreQueryProfile {
        stages: stage_profile(3),
        ..SegmentStoreQueryProfile::default()
    });
    assert_eq!(profile.stages, stage_profile(5));

    let delta = profile.delta_since(SegmentStoreQueryProfile {
        stages: stage_profile(1),
        ..SegmentStoreQueryProfile::default()
    });
    assert_eq!(delta.stages, stage_profile(4));
}

#[test]
fn query_profile_adds_index_read_stats_by_category() {
    let mut total = SegmentStoreQueryProfile {
        index_read_stats: index_stats(2),
        ..SegmentStoreQueryProfile::default()
    };

    total.add(SegmentStoreQueryProfile {
        index_read_stats: index_stats(3),
        ..SegmentStoreQueryProfile::default()
    });

    assert_eq!(total.index_read_stats, index_stats(5));
    assert_eq!(total.index_read_stats.total_calls(), 105);
    assert_eq!(total.index_read_stats.total_bytes(), 1_050);
}

#[test]
fn query_profile_deltas_index_read_stats_by_category_with_saturation() {
    let after = SegmentStoreQueryProfile {
        index_read_stats: index_stats(5),
        ..SegmentStoreQueryProfile::default()
    };
    let mut before_stats = index_stats(2);
    before_stats.payload.calls = u64::MAX;
    before_stats.payload.bytes = u64::MAX;
    let before = SegmentStoreQueryProfile {
        index_read_stats: before_stats,
        ..SegmentStoreQueryProfile::default()
    };

    let delta = after.delta_since(before).index_read_stats;

    let mut expected = index_stats(3);
    expected.payload = SegmentIndexReadCount::default();
    assert_eq!(delta, expected);
}

#[test]
fn query_profile_deltas_symbol_counters_but_preserves_resource_gauges() {
    let before = SegmentStoreQueryProfile {
        symbol_read_stats: SegmentSymbolReadStats {
            legacy_eager: SegmentSymbolReadCount::default(),
            logical_returned: SegmentSymbolReadCount::default(),
            root: SegmentSymbolReadCount {
                calls: 2,
                bytes: 200,
            },
            page: SegmentSymbolReadCount {
                calls: 3,
                bytes: 300,
            },
            page_validation: SegmentSymbolReadCount {
                calls: 3,
                bytes: 300,
            },
            page_validation_ns: 30,
            touched_corrupt_pages: 1,
            page_cache_hits: 4,
            page_cache_misses: 5,
            page_cache_evictions: 6,
        },
        symbol_resources: SegmentStoreSymbolResources {
            retained_readers: 1,
            retained_open_files: 1,
            source_file_bytes: 100_000,
            root_encoded_bytes: 1_000,
            root_retained_charge_bytes: 2_000,
            page_cache_charge_bytes: 32_768,
            page_cache_max_bytes: 262_144,
            ..SegmentStoreSymbolResources::default()
        },
        ..SegmentStoreQueryProfile::default()
    };
    let after = SegmentStoreQueryProfile {
        symbol_read_stats: SegmentSymbolReadStats {
            legacy_eager: SegmentSymbolReadCount::default(),
            logical_returned: SegmentSymbolReadCount::default(),
            root: SegmentSymbolReadCount {
                calls: 3,
                bytes: 280,
            },
            page: SegmentSymbolReadCount {
                calls: 5,
                bytes: 500,
            },
            page_validation: SegmentSymbolReadCount {
                calls: 5,
                bytes: 500,
            },
            page_validation_ns: 80,
            touched_corrupt_pages: 3,
            page_cache_hits: 10,
            page_cache_misses: 8,
            page_cache_evictions: 7,
        },
        symbol_resources: SegmentStoreSymbolResources {
            retained_readers: 2,
            retained_open_files: 2,
            source_file_bytes: 200_000,
            root_encoded_bytes: 2_000,
            root_retained_charge_bytes: 4_000,
            page_cache_charge_bytes: 65_536,
            page_cache_max_bytes: 524_288,
            ..SegmentStoreSymbolResources::default()
        },
        ..SegmentStoreQueryProfile::default()
    };

    let delta = after.delta_since(before);

    assert_eq!(
        delta.symbol_read_stats,
        SegmentSymbolReadStats {
            legacy_eager: SegmentSymbolReadCount::default(),
            logical_returned: SegmentSymbolReadCount::default(),
            root: SegmentSymbolReadCount {
                calls: 1,
                bytes: 80,
            },
            page: SegmentSymbolReadCount {
                calls: 2,
                bytes: 200,
            },
            page_validation: SegmentSymbolReadCount {
                calls: 2,
                bytes: 200,
            },
            page_validation_ns: 50,
            touched_corrupt_pages: 2,
            page_cache_hits: 6,
            page_cache_misses: 3,
            page_cache_evictions: 1,
        }
    );
    assert_eq!(delta.symbol_resources, after.symbol_resources);
    assert_eq!(delta.symbol_resources.total_retained_charge_bytes(), 69_536);
}

#[test]
fn query_profile_adds_and_deltas_scheduler_profile() {
    let before_scheduler = ChunkReadSchedulerProfile {
        executions: 2,
        pread_decisions: 1,
        io_uring_decisions: 1,
        logical_requests: 20,
        physical_spans: 10,
        backend_submissions: 3,
        sqes_submitted: 9,
        submission_depth_sum: 10,
        submission_depth_max: 8,
        submission_depth_1: 1,
        submission_depth_2_3: 0,
        submission_depth_4_7: 0,
        submission_depth_8_plus: 1,
        total_physical_bytes_executed: 1_000,
        peak_in_flight_bytes: 800,
    };
    let next_scheduler = ChunkReadSchedulerProfile {
        executions: 1,
        io_uring_decisions: 1,
        logical_requests: 12,
        physical_spans: 9,
        backend_submissions: 2,
        sqes_submitted: 9,
        submission_depth_sum: 9,
        submission_depth_max: 8,
        submission_depth_1: 1,
        submission_depth_8_plus: 1,
        total_physical_bytes_executed: 900,
        peak_in_flight_bytes: 700,
        ..ChunkReadSchedulerProfile::default()
    };
    let before = SegmentStoreQueryProfile {
        chunk_read_scheduler: before_scheduler,
        ..SegmentStoreQueryProfile::default()
    };
    let mut after = before;
    after.add(SegmentStoreQueryProfile {
        chunk_read_scheduler: next_scheduler,
        ..SegmentStoreQueryProfile::default()
    });

    let delta = after.delta_since(before).chunk_read_scheduler;
    assert_eq!(
        delta,
        ChunkReadSchedulerProfile {
            peak_in_flight_bytes: 800,
            ..next_scheduler
        }
    );
    assert_eq!(after.chunk_read_scheduler.submission_depth_max, 8);
    assert_eq!(after.chunk_read_scheduler.peak_in_flight_bytes, 800);
}
