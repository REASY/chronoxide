use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::{io, mem};

use allocator_api2::alloc::{AllocError, Allocator, Global, Layout};

use crate::labels::{METRIC_NAME_LABEL, SeriesRef};
use crate::storage::chunk::{
    ChunkKind, ChunkScalarProjection, ChunkScalarRecordHeader, ChunkScalarSample, ChunkScalarValue,
};
use crate::storage::head::{HistogramValue, TypedSampleMetadata};

use super::range_scalar_cache::{
    DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES, DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
    ExactInitArena, MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES, MIB, RangeScalarCacheAdmission,
    RangeScalarCacheCall, RangeScalarCacheConfigError, RangeScalarCacheEntry,
    RangeScalarCacheGovernor, RangeScalarCacheInitErrorKind, RangeScalarCacheKey,
    RangeScalarCacheLayout, RangeScalarCacheLayoutError, RangeScalarCacheLookup,
    RangeScalarDecodeCache, configure_range_scalar_cache_governor_in,
    inject_range_scalar_cache_allocation_failure, validate_range_scalar_cache_budget_bytes,
};

#[test]
fn governor_handles_zero_overflow_and_monotonic_peak() {
    let zero = Arc::new(RangeScalarCacheGovernor::new(0));
    let zero_lease = zero.try_acquire(0).expect("zero-byte lease must succeed");
    assert!(zero.try_acquire(1).is_none());
    assert_eq!(zero.stats().current_leased_bytes, 0);
    drop(zero_lease);

    let maximum = Arc::new(RangeScalarCacheGovernor::new(u64::MAX));
    let maximum_lease = maximum
        .try_acquire(u64::MAX)
        .expect("maximum lease must fit exactly");
    assert!(maximum.try_acquire(1).is_none());
    assert_eq!(maximum.stats().current_leased_bytes, u64::MAX);
    drop(maximum_lease);
    assert_eq!(maximum.stats().current_leased_bytes, 0);
    assert_eq!(maximum.stats().peak_leased_bytes, u64::MAX);

    let governor = Arc::new(RangeScalarCacheGovernor::new(8));
    drop(governor.try_acquire(5).unwrap());
    drop(governor.try_acquire(3).unwrap());
    assert_eq!(governor.stats().current_leased_bytes, 0);
    assert_eq!(governor.stats().peak_leased_bytes, 5);
}

#[test]
fn governor_checked_cas_never_over_admits_concurrent_leases() {
    const THREADS: usize = 12;
    const LIMIT: usize = 4;
    let governor = Arc::new(RangeScalarCacheGovernor::new(LIMIT as u64));
    let start = Arc::new(Barrier::new(THREADS));
    let hold = Arc::new(Barrier::new(THREADS));
    let admitted = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let governor = Arc::clone(&governor);
        let start = Arc::clone(&start);
        let hold = Arc::clone(&hold);
        let admitted = Arc::clone(&admitted);
        handles.push(std::thread::spawn(move || {
            start.wait();
            let lease = governor.try_acquire(1);
            if lease.is_some() {
                admitted.fetch_add(1, Ordering::SeqCst);
            }
            hold.wait();
            drop(lease);
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(admitted.load(Ordering::SeqCst), LIMIT);
    assert_eq!(governor.stats().peak_leased_bytes, LIMIT as u64);
    assert_eq!(governor.stats().current_leased_bytes, 0);
}

#[test]
fn six_eight_mib_calls_share_sixteen_mib_governor_without_semantic_drift() {
    const CALLS: usize = 6;
    const CALL_BUDGET: u64 = 8 * MIB;
    const GOVERNOR_LIMIT: u64 = 16 * MIB;

    fn scalar_fingerprint(samples: &[ChunkScalarSample]) -> u64 {
        samples.iter().fold(0xcbf2_9ce4_8422_2325, |hash, sample| {
            let value = match sample.value {
                Some(ChunkScalarValue::Count(value)) => value,
                Some(ChunkScalarValue::Sum(value)) => value.to_bits(),
                None => u64::MAX,
            };
            hash.wrapping_mul(0x0000_0100_0000_01b3) ^ sample.timestamp_ms ^ value.rotate_left(17)
        })
    }

    let attempts = Arc::new(Barrier::new(CALLS));
    let governor = Arc::new(RangeScalarCacheGovernor::new_with_attempt_barrier(
        GOVERNOR_LIMIT,
        Arc::clone(&attempts),
    ));
    let start = Arc::new(Barrier::new(CALLS));
    let expected_samples = vec![cache_sample(101), cache_sample(202)];
    let expected_fingerprint = scalar_fingerprint(&expected_samples);
    let mut handles = Vec::with_capacity(CALLS);

    for _ in 0..CALLS {
        let governor = Arc::clone(&governor);
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            let key = cache_key();
            let mut call = RangeScalarCacheCall::new(CALL_BUDGET, governor);
            start.wait();
            assert_eq!(
                call.classify_eligible(&key, 17),
                RangeScalarCacheLookup::Miss
            );

            let admitted = call.cache_available();
            let samples = if admitted {
                assert_eq!(
                    call.admit_with(key, cache_header(key.chunk_kind, 2), 2, |emit| {
                        emit(cache_sample(101))?;
                        emit(cache_sample(202))
                    })
                    .unwrap(),
                    RangeScalarCacheAdmission::Admitted
                );
                call.lookup(&key).unwrap().1.to_vec()
            } else {
                vec![cache_sample(101), cache_sample(202)]
            };
            let fingerprint = scalar_fingerprint(&samples);
            let summary = call.finish();
            (admitted, fingerprint, summary)
        }));
    }

    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results.iter().filter(|(admitted, _, _)| *admitted).count(),
        2
    );
    assert_eq!(
        results.iter().filter(|(admitted, _, _)| !*admitted).count(),
        4
    );
    for (admitted, fingerprint, summary) in results {
        assert_eq!(fingerprint, expected_fingerprint);
        assert_eq!(summary.configured_budget_bytes, CALL_BUDGET);
        assert_eq!(summary.governor_refused, !admitted);
        assert_eq!(
            summary.governor_lease_bytes,
            if admitted { CALL_BUDGET } else { 0 }
        );
        assert_eq!(summary.admitted_entries, u64::from(admitted));
        assert_eq!(summary.streaming_budget_bypasses, u64::from(!admitted));
        assert!(summary.peak_retained_charge_bytes <= CALL_BUDGET);
        assert_eq!(summary.retained_charge_after_finalize, 0);
    }
    let stats = governor.stats();
    assert_eq!(stats.limit_bytes, GOVERNOR_LIMIT);
    assert_eq!(stats.peak_leased_bytes, GOVERNOR_LIMIT);
    assert_eq!(stats.current_leased_bytes, 0);
}

#[test]
fn isolated_governor_configuration_is_race_safe() {
    const THREADS: usize = 12;
    let cell = Arc::new(OnceLock::new());
    let start = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let cell = Arc::clone(&cell);
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();
            configure_range_scalar_cache_governor_in(&cell, 123)
        }));
    }
    for handle in handles {
        assert_eq!(handle.join().unwrap(), Ok(()));
    }
    assert_eq!(cell.get().unwrap().stats().limit_bytes, 123);

    let conflict = Arc::new(OnceLock::new());
    let barrier = Arc::new(Barrier::new(2));
    let mut conflict_handles = Vec::new();
    for limit in [11, 22] {
        let conflict = Arc::clone(&conflict);
        let barrier = Arc::clone(&barrier);
        conflict_handles.push(std::thread::spawn(move || {
            barrier.wait();
            (
                limit,
                configure_range_scalar_cache_governor_in(&conflict, limit),
            )
        }));
    }
    let results: Vec<_> = conflict_handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(
        results.iter().filter(|(_, result)| result.is_ok()).count(),
        1
    );
    let existing = conflict.get().unwrap().stats().limit_bytes;
    for (requested, result) in results {
        if requested == existing {
            assert_eq!(result, Ok(()));
        } else {
            assert_eq!(
                result,
                Err(RangeScalarCacheConfigError::GovernorAlreadyInitialized {
                    existing_bytes: existing,
                    requested_bytes: requested,
                })
            );
        }
    }
}

#[test]
fn range_cache_call_success_finishes_cache_before_releasing_lease() {
    let budget = MIB;
    let governor = Arc::new(RangeScalarCacheGovernor::new(budget));
    let allocator = FailingAllocator::fail_on_call(usize::MAX)
        .observe_deallocation_governor(Arc::clone(&governor));
    let observation = Arc::clone(&allocator.state);
    let mut call = RangeScalarCacheCall::new_in(budget, Arc::clone(&governor), allocator);
    assert_eq!(governor.stats().current_leased_bytes, 0);
    assert!(call.cache_mut().is_some());
    assert_eq!(governor.stats().current_leased_bytes, budget);
    let active = call.summary();
    assert_eq!(active.configured_budget_bytes, budget);
    assert_eq!(active.governor_lease_bytes, budget);
    assert!(!active.governor_refused);
    assert!(!active.allocation_refused);
    assert!(!active.layout_overflow);
    assert!(active.entry_arena_charge_bytes > 0);
    assert!(active.sample_arena_charge_bytes > 0);
    assert_eq!(
        active.peak_retained_charge_bytes,
        active.entry_arena_charge_bytes + active.sample_arena_charge_bytes
    );

    let finished = call.finish();
    assert_eq!(finished.retained_charge_after_finalize, 0);
    assert_eq!(
        observation
            .lease_bytes_seen_during_deallocate
            .load(Ordering::SeqCst),
        budget,
        "both arenas must deallocate while the governor lease is still held"
    );
    assert_eq!(governor.stats().current_leased_bytes, 0);
}

#[test]
fn range_cache_call_refusal_and_zero_budget_do_not_allocate_or_retry() {
    let budget = MIB;
    let governor = Arc::new(RangeScalarCacheGovernor::new(budget));
    let held = governor.try_acquire(budget).unwrap();
    let allocator = FailingAllocator::fail_on_call(usize::MAX);
    let mut refused =
        RangeScalarCacheCall::new_in(budget, Arc::clone(&governor), allocator.clone());
    assert!(refused.cache_mut().is_none());
    assert!(refused.summary().governor_refused);
    assert_eq!(refused.summary().governor_lease_bytes, 0);
    assert_eq!(refused.summary().entry_arena_charge_bytes, 0);
    assert_eq!(refused.summary().sample_arena_charge_bytes, 0);
    assert_eq!(allocator.calls(), 0);
    drop(held);
    assert!(
        refused.cache_mut().is_none(),
        "one call must never retry admission"
    );
    assert_eq!(allocator.calls(), 0);
    let summary = refused.finish();
    assert_eq!(summary.retained_charge_after_finalize, 0);

    let zero_governor = Arc::new(RangeScalarCacheGovernor::new(0));
    let zero_allocator = FailingAllocator::fail_on_call(usize::MAX);
    let mut zero =
        RangeScalarCacheCall::new_in(0, Arc::clone(&zero_governor), zero_allocator.clone());
    assert!(zero.cache_mut().is_none());
    assert_eq!(zero_allocator.calls(), 0);
    assert_eq!(zero_governor.stats().current_leased_bytes, 0);
    let summary = zero.finish();
    assert!(!summary.governor_refused);
    assert!(!summary.allocation_refused);
    assert!(!summary.layout_overflow);
}

#[test]
fn range_cache_call_classifies_allocation_and_layout_failures_and_never_retries() {
    for fail_on_call in [1, 2] {
        let budget = MIB;
        let governor = Arc::new(RangeScalarCacheGovernor::new(budget));
        let allocator = FailingAllocator::fail_on_call(fail_on_call);
        let mut call =
            RangeScalarCacheCall::new_in(budget, Arc::clone(&governor), allocator.clone());
        assert!(call.cache_mut().is_none());
        assert!(call.summary().allocation_refused);
        assert_eq!(call.summary().governor_lease_bytes, budget);
        if fail_on_call == 1 {
            assert_eq!(call.summary().peak_retained_charge_bytes, 0);
        } else {
            assert_eq!(
                call.summary().peak_retained_charge_bytes,
                call.summary().entry_arena_charge_bytes
            );
        }
        assert_eq!(governor.stats().current_leased_bytes, 0);
        assert_eq!(allocator.live_bytes(), 0);
        let calls = allocator.calls();
        assert!(call.cache_mut().is_none());
        assert_eq!(allocator.calls(), calls);
        let summary = call.finish();
        assert_eq!(summary.retained_charge_after_finalize, 0);
        assert_eq!(allocator.live_bytes(), 0);
    }

    let governor = Arc::new(RangeScalarCacheGovernor::new(u64::MAX));
    let mut overflow = RangeScalarCacheCall::new_in(u64::MAX, Arc::clone(&governor), Global);
    assert!(overflow.cache_mut().is_none());
    assert!(overflow.summary().layout_overflow);
    assert!(!overflow.summary().allocation_refused);
    assert_eq!(overflow.summary().governor_lease_bytes, u64::MAX);
    assert_eq!(governor.stats().current_leased_bytes, 0);
    let summary = overflow.finish();
    assert_eq!(summary.retained_charge_after_finalize, 0);
}

#[test]
fn emergency_range_cache_call_drop_releases_cache_and_lease() {
    let governor = Arc::new(RangeScalarCacheGovernor::new(MIB));
    {
        let mut call = RangeScalarCacheCall::new(MIB, Arc::clone(&governor));
        call.summary_mut().unsupported_bypasses = 1;
        assert_eq!(call.summary().unsupported_bypasses, 1);
        assert!(call.cache_mut().is_some());
        assert_eq!(governor.stats().current_leased_bytes, MIB);
    }
    assert_eq!(governor.stats().current_leased_bytes, 0);
}

#[test]
fn session_summary_replaces_success_on_governor_and_exact_allocation_refusals() {
    let tempdir = tempfile::tempdir().unwrap();
    let config =
        super::SegmentWriterConfig::new(tempdir.path(), std::time::Duration::from_secs(60));
    let mut writer = super::SegmentWriter::new(config).unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 1,
                        sum: Some(1.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 0],
                    },
                ),
                (
                    2_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(2.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 0],
                    },
                ),
            ],
            |visit| visit(METRIC_NAME_LABEL, "session_refusal"),
        )
        .unwrap();
    writer.flush().unwrap();
    let store = super::SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    session.set_range_scalar_cache_budget_bytes(MIB).unwrap();

    let admitted_governor = Arc::new(RangeScalarCacheGovernor::new(MIB));
    session.range_scalar_cache_governor = Arc::clone(&admitted_governor);
    let baseline = session
        .query_promql_range_with_limits(
            "session_refusal_count",
            1_000,
            2_000,
            1_000,
            super::QueryLimits::unlimited(),
        )
        .unwrap();
    let success = session.last_range_scalar_cache_summary().copied().unwrap();
    assert!(success.admitted_entries > 0, "{success:?}");
    assert_eq!(success.retained_charge_after_finalize, 0);
    assert_eq!(admitted_governor.stats().current_leased_bytes, 0);

    let refused_governor = Arc::new(RangeScalarCacheGovernor::new(0));
    session.range_scalar_cache_governor = Arc::clone(&refused_governor);
    let streamed = session
        .query_promql_range_with_limits(
            "session_refusal_count",
            1_000,
            2_000,
            1_000,
            super::QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(streamed, baseline);
    let refused = session.last_range_scalar_cache_summary().copied().unwrap();
    assert!(refused.governor_refused);
    assert!(refused.streaming_budget_bypasses > 0);
    assert_eq!(refused.retained_charge_after_finalize, 0);
    assert_ne!(refused, success);
    assert_eq!(refused_governor.stats().current_leased_bytes, 0);

    let mut previous = refused;
    for fail_on_call in [1, 2] {
        let governor = Arc::new(RangeScalarCacheGovernor::new(MIB));
        session.range_scalar_cache_governor = Arc::clone(&governor);
        let injection = inject_range_scalar_cache_allocation_failure(fail_on_call);
        let streamed = session
            .query_promql_range_with_limits(
                "session_refusal_count",
                1_000,
                2_000,
                1_000,
                super::QueryLimits::unlimited(),
            )
            .unwrap();
        drop(injection);
        assert_eq!(streamed, baseline);
        let allocation = session.last_range_scalar_cache_summary().copied().unwrap();
        assert!(allocation.allocation_refused);
        assert!(!allocation.governor_refused);
        assert_eq!(allocation.governor_lease_bytes, MIB);
        assert!(allocation.streaming_budget_bypasses > 0);
        assert_eq!(allocation.retained_charge_after_finalize, 0);
        assert_ne!(allocation, previous);
        if fail_on_call == 1 {
            assert_eq!(allocation.peak_retained_charge_bytes, 0);
        } else {
            assert_eq!(
                allocation.peak_retained_charge_bytes,
                allocation.entry_arena_charge_bytes
            );
        }
        assert_eq!(governor.stats().current_leased_bytes, 0);
        previous = allocation;
    }
}

fn assert_facade_range_scalar_cache_reuses_validated_lanes(schema8: bool) {
    const CACHE_BUDGET_BYTES: u64 = 4 * MIB;

    let tempdir = tempfile::tempdir().unwrap();
    let config =
        super::SegmentWriterConfig::new(tempdir.path(), std::time::Duration::from_secs(60));
    let config = if schema8 {
        config.with_storage_schema(super::SegmentStorageSchema::Schema8)
    } else {
        config.with_storage_schema(super::SegmentStorageSchema::Schema7)
    };
    let mut writer = super::SegmentWriter::new(config).unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 1,
                        sum: Some(1.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 0],
                    },
                ),
                (
                    2_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(3.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 3,
                        sum: Some(6.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 2],
                    },
                ),
            ],
            |visit| visit(METRIC_NAME_LABEL, "facade_cache"),
        )
        .unwrap();
    writer.flush().unwrap();

    let open_options = super::SegmentStoreOpenOptions {
        storage_schema_policy: if schema8 {
            super::SegmentStoreSchemaPolicy::StrictSchema8
        } else {
            super::SegmentStoreSchemaPolicy::StrictSchema7
        },
        ..super::SegmentStoreOpenOptions::default()
    };
    let run = |query, cache_budget_bytes| {
        let store =
            super::SegmentStoreReader::open_with_options(tempdir.path(), open_options).unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_range_scalar_cache_budget_bytes(cache_budget_bytes)
            .unwrap();
        session.range_scalar_cache_governor =
            Arc::new(RangeScalarCacheGovernor::new(CACHE_BUDGET_BYTES));
        let profile_before = session.profile();
        let execution = session
            .query_promql_range_with_limits(
                query,
                1_000,
                3_000,
                1_000,
                super::QueryLimits::unlimited(),
            )
            .unwrap();
        let profile = session.profile().delta_since(profile_before);
        let summary = session.last_range_scalar_cache_summary().copied().unwrap();
        (execution, profile, summary)
    };

    for query in ["facade_cache_count", "facade_cache_sum"] {
        let (cache_off, cache_off_profile, cache_off_summary) = run(query, 0);
        let (cache_on, cache_on_profile, cache_on_summary) = run(query, CACHE_BUDGET_BYTES);

        assert_eq!(cache_on.results, cache_off.results, "{query}");
        assert_eq!(cache_on.stats, cache_off.stats, "{query}");
        assert_eq!(
            cache_on.semantic_fingerprint_sha256(),
            cache_off.semantic_fingerprint_sha256(),
            "{query}"
        );
        assert_eq!(
            cache_on.portable_semantic_fingerprint_sha256(),
            cache_off.portable_semantic_fingerprint_sha256(),
            "{query}"
        );
        assert_eq!(
            cache_on_profile.chunk_payload_bytes, cache_off_profile.chunk_payload_bytes,
            "{query}"
        );
        assert!(
            cache_on_profile.chunk_payload_physical_reads
                < cache_off_profile.chunk_payload_physical_reads,
            "query={query} schema8={schema8} on={cache_on_profile:?} off={cache_off_profile:?}"
        );
        assert!(
            cache_on_summary.admitted_entries > 0,
            "query={query} {cache_on_summary:?}"
        );
        assert!(
            cache_on_summary.hits > 0,
            "query={query} {cache_on_summary:?}"
        );
        assert_eq!(cache_on_summary.unsupported_bypasses, 0, "{query}");
        assert_eq!(
            cache_on_summary.retained_charge_after_finalize, 0,
            "{query}"
        );
        assert_eq!(cache_off_summary.hits, 0, "{query}");
        assert_eq!(cache_off_summary.admitted_entries, 0, "{query}");
        assert!(cache_off_summary.streaming_budget_bypasses > 0, "{query}");
        assert_eq!(cache_off_summary.unsupported_bypasses, 0, "{query}");
        assert_eq!(
            cache_on_summary
                .logical_hit_bytes
                .saturating_add(cache_on_summary.logical_miss_or_bypass_bytes),
            cache_on_profile.chunk_payload_bytes,
            "{query}"
        );
        assert_eq!(
            cache_off_summary.logical_miss_or_bypass_bytes, cache_off_profile.chunk_payload_bytes,
            "{query}"
        );
    }
}

#[test]
fn schema7_facade_range_scalar_cache_reuses_validated_lanes() {
    assert_facade_range_scalar_cache_reuses_validated_lanes(false);
}

#[test]
fn schema8_facade_range_scalar_cache_reuses_validated_lanes() {
    assert_facade_range_scalar_cache_reuses_validated_lanes(true);
}

#[test]
fn schema7_facade_range_scalar_cache_never_admits_corrupt_indexed_prefix() {
    let tempdir = tempfile::tempdir().unwrap();
    let config =
        super::SegmentWriterConfig::new(tempdir.path(), std::time::Duration::from_secs(60))
            .with_storage_schema(super::SegmentStorageSchema::Schema7);
    let mut writer = super::SegmentWriter::new(config).unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                1_000,
                HistogramValue {
                    count: 1,
                    sum: Some(1.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 0],
                },
            )],
            |visit| visit(METRIC_NAME_LABEL, "facade_cache_corrupt"),
        )
        .unwrap();
    writer.flush().unwrap();

    let segment_dir = std::fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let chunks_path = segment_dir.join("chunks.bin");
    let mut chunks = std::fs::read(&chunks_path).unwrap();
    chunks[super::CHUNK_FRAME_HEADER_LEN] ^= 0xff;
    std::fs::write(&chunks_path, chunks).unwrap();

    let store = super::SegmentStoreReader::open_with_options(
        tempdir.path(),
        super::SegmentStoreOpenOptions {
            storage_schema_policy: super::SegmentStoreSchemaPolicy::StrictSchema7,
            ..super::SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut session = store.query_session().unwrap();
    session.set_range_scalar_cache_budget_bytes(MIB).unwrap();
    session.range_scalar_cache_governor = Arc::new(RangeScalarCacheGovernor::new(MIB));
    let error = session
        .query_promql_range_with_limits(
            "facade_cache_corrupt_count",
            1_000,
            2_000,
            1_000,
            super::QueryLimits::unlimited(),
        )
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("schema-7 indexed prefix crc mismatch"),
        "{error}"
    );
    let summary = session.last_range_scalar_cache_summary().copied().unwrap();
    assert_eq!(summary.hits, 0);
    assert_eq!(summary.misses, 1);
    assert_eq!(summary.admitted_entries, 0);
    assert_eq!(summary.retained_charge_after_finalize, 0);
}

#[derive(Debug, Default)]
struct FailingAllocatorState {
    calls: AtomicUsize,
    live_bytes: AtomicUsize,
    lease_bytes_seen_during_deallocate: AtomicU64,
}

#[derive(Debug, Clone)]
struct FailingAllocator {
    state: Arc<FailingAllocatorState>,
    fail_on_call: usize,
    deallocation_governor: Option<Arc<RangeScalarCacheGovernor>>,
}

impl FailingAllocator {
    fn fail_on_call(fail_on_call: usize) -> Self {
        Self {
            state: Arc::new(FailingAllocatorState::default()),
            fail_on_call,
            deallocation_governor: None,
        }
    }

    fn observe_deallocation_governor(mut self, governor: Arc<RangeScalarCacheGovernor>) -> Self {
        self.deallocation_governor = Some(governor);
        self
    }

    fn calls(&self) -> usize {
        self.state.calls.load(Ordering::SeqCst)
    }

    fn live_bytes(&self) -> usize {
        self.state.live_bytes.load(Ordering::SeqCst)
    }
}

// SAFETY: successful operations delegate to `Global` with the original
// pointer/layout contract. The injected failure occurs before allocation and
// therefore transfers no ownership.
unsafe impl Allocator for FailingAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let call = self.state.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.fail_on_call {
            return Err(AllocError);
        }
        let allocation = Global.allocate(layout)?;
        self.state
            .live_bytes
            .fetch_add(layout.size(), Ordering::SeqCst);
        Ok(allocation)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if let Some(governor) = &self.deallocation_governor {
            self.state
                .lease_bytes_seen_during_deallocate
                .store(governor.stats().current_leased_bytes, Ordering::SeqCst);
        }
        self.state
            .live_bytes
            .fetch_sub(layout.size(), Ordering::SeqCst);
        // SAFETY: upheld by this method's `Allocator::deallocate` contract and
        // the allocation was obtained from `Global` with the same layout.
        unsafe { Global.deallocate(ptr, layout) }
    }
}

#[test]
fn first_and_second_exact_allocation_failures_are_typed_and_leak_free() {
    let first = FailingAllocator::fail_on_call(1);
    let first_error = match RangeScalarDecodeCache::try_new_in(8 * MIB, first.clone()) {
        Ok(_) => panic!("first allocation must fail"),
        Err(error) => error,
    };
    assert_eq!(
        first_error.kind,
        RangeScalarCacheInitErrorKind::AllocationRefused
    );
    assert_eq!(first_error.peak_retained_charge_bytes, 0);
    assert_eq!(first.calls(), 1);
    assert_eq!(first.live_bytes(), 0);

    let second = FailingAllocator::fail_on_call(2);
    let second_error = match RangeScalarDecodeCache::try_new_in(8 * MIB, second.clone()) {
        Ok(_) => panic!("second allocation must fail"),
        Err(error) => error,
    };
    assert_eq!(
        second_error.kind,
        RangeScalarCacheInitErrorKind::AllocationRefused
    );
    assert_eq!(
        second_error.peak_retained_charge_bytes,
        second_error.entry_charge_bytes
    );
    assert!(second_error.entry_charge_bytes > 0);
    assert!(second_error.sample_charge_bytes > 0);
    assert_eq!(second.calls(), 2);
    assert_eq!(second.live_bytes(), 0);
}

#[test]
fn normal_exact_cache_drop_releases_both_allocations() {
    let allocator = FailingAllocator::fail_on_call(usize::MAX);
    let cache = RangeScalarDecodeCache::try_new_in(8 * MIB, allocator.clone())
        .expect("both allocations must succeed");
    assert_eq!(allocator.calls(), 2);
    assert_eq!(
        allocator.live_bytes(),
        (cache.entry_charge_bytes() + cache.sample_charge_bytes()) as usize
    );
    drop(cache);
    assert_eq!(allocator.live_bytes(), 0);
}

fn cache_key() -> RangeScalarCacheKey {
    RangeScalarCacheKey {
        segment_ordinal: 1,
        file_id: 0,
        chunk_offset: 10,
        chunk_len: 20,
        scalar_lane_offset: 40,
        scalar_lane_len: 50,
        projection: ChunkScalarProjection::Count,
        chunk_kind: ChunkKind::Histogram,
    }
}

fn cache_header(kind: ChunkKind, sample_count: u32) -> ChunkScalarRecordHeader {
    ChunkScalarRecordHeader {
        series_ref: 7,
        kind,
        min_time_ms: 100,
        max_time_ms: 200,
        sample_count,
    }
}

fn cache_sample(timestamp_ms: u64) -> ChunkScalarSample {
    ChunkScalarSample {
        timestamp_ms,
        metadata: TypedSampleMetadata::default(),
        value: Some(ChunkScalarValue::Count(timestamp_ms)),
    }
}

fn admit_one(
    cache: &mut RangeScalarDecodeCache,
    key: RangeScalarCacheKey,
    timestamp_ms: u64,
) -> RangeScalarCacheAdmission {
    cache
        .admit_with(key, cache_header(key.chunk_kind, 1), 1, |emit| {
            emit(cache_sample(timestamp_ms))
        })
        .expect("decode callback must succeed")
}

#[test]
fn cache_call_classifies_once_and_processing_lookup_is_non_accounting() {
    let governor = Arc::new(RangeScalarCacheGovernor::new(MIB));
    let mut call = RangeScalarCacheCall::new(MIB, governor);
    let key = cache_key();

    assert_eq!(
        call.classify_eligible(&key, 123),
        RangeScalarCacheLookup::Miss
    );
    assert!(call.cache_available());
    assert_eq!(
        call.admit_with(key, cache_header(key.chunk_kind, 1), 1, |emit| {
            emit(cache_sample(101))
        })
        .unwrap(),
        RangeScalarCacheAdmission::Admitted
    );
    let after_admission = call.summary();
    assert_eq!(after_admission.misses, 1);
    assert_eq!(after_admission.logical_miss_or_bypass_bytes, 123);
    assert_eq!(after_admission.admitted_entries, 1);
    assert_eq!(after_admission.hits, 0);

    for _ in 0..2 {
        let (header, samples) = call.lookup(&key).expect("admitted key must hit");
        assert_eq!(header, cache_header(key.chunk_kind, 1));
        assert_eq!(samples, &[cache_sample(101)]);
    }
    assert_eq!(
        call.summary(),
        after_admission,
        "processing lookups must not mutate metrics"
    );

    assert_eq!(
        call.classify_eligible(&key, 123),
        RangeScalarCacheLookup::Hit
    );
    let summary = call.summary();
    assert_eq!(summary.hits, 1);
    assert_eq!(summary.logical_hit_bytes, 123);
    assert_eq!(summary.misses, 1);
    assert_eq!(summary.logical_miss_or_bypass_bytes, 123);
    assert_eq!(summary.streaming_budget_bypasses, 0);
}

#[test]
fn cache_call_unavailable_paths_classify_once_and_stream_without_retry() {
    fn assert_unavailable<A: Allocator + Clone>(
        mut call: RangeScalarCacheCall<A>,
        expected_governor_refused: bool,
        expected_allocation_refused: bool,
    ) {
        let key = cache_key();
        assert_eq!(
            call.classify_eligible(&key, 17),
            RangeScalarCacheLookup::Miss
        );
        assert!(!call.cache_available());
        assert!(call.lookup(&key).is_none());

        let before_admission = call.summary();
        let mut callback_called = false;
        assert_eq!(
            call.admit_with(key, cache_header(key.chunk_kind, 1), 1, |_| {
                callback_called = true;
                Ok(())
            })
            .unwrap(),
            RangeScalarCacheAdmission::Unavailable
        );
        assert!(!callback_called);
        assert_eq!(call.summary(), before_admission);
        assert_eq!(before_admission.misses, 1);
        assert_eq!(before_admission.logical_miss_or_bypass_bytes, 17);
        assert_eq!(before_admission.streaming_budget_bypasses, 1);
        assert_eq!(before_admission.governor_refused, expected_governor_refused);
        assert_eq!(
            before_admission.allocation_refused,
            expected_allocation_refused
        );
    }

    assert_unavailable(
        RangeScalarCacheCall::new_in(
            0,
            Arc::new(RangeScalarCacheGovernor::new(0)),
            FailingAllocator::fail_on_call(usize::MAX),
        ),
        false,
        false,
    );

    let budget = MIB;
    let governor = Arc::new(RangeScalarCacheGovernor::new(budget));
    let held = governor.try_acquire(budget).unwrap();
    assert_unavailable(
        RangeScalarCacheCall::new_in(
            budget,
            Arc::clone(&governor),
            FailingAllocator::fail_on_call(usize::MAX),
        ),
        true,
        false,
    );
    drop(held);

    assert_unavailable(
        RangeScalarCacheCall::new_in(
            budget,
            Arc::new(RangeScalarCacheGovernor::new(budget)),
            FailingAllocator::fail_on_call(1),
        ),
        false,
        true,
    );
}

#[test]
fn cache_call_unsupported_bypass_does_not_initialize_or_count_a_miss() {
    let allocator = FailingAllocator::fail_on_call(usize::MAX);
    let mut call = RangeScalarCacheCall::new_in(
        MIB,
        Arc::new(RangeScalarCacheGovernor::new(MIB)),
        allocator.clone(),
    );

    call.classify_unsupported(u64::MAX);
    call.classify_unsupported(1);

    let summary = call.summary();
    assert_eq!(summary.unsupported_bypasses, 2);
    assert_eq!(summary.logical_miss_or_bypass_bytes, u64::MAX);
    assert_eq!(summary.misses, 0);
    assert_eq!(summary.streaming_budget_bypasses, 0);
    assert_eq!(allocator.calls(), 0);
    assert!(!call.cache_available());
}

#[test]
fn cache_call_admission_counts_only_insert_and_capacity_bypasses() {
    let budget = (4 * mem::size_of::<RangeScalarCacheEntry>()) as u64;
    let governor = Arc::new(RangeScalarCacheGovernor::new(2 * budget));
    let first_key = cache_key();
    let second_key = RangeScalarCacheKey {
        chunk_offset: 99,
        ..first_key
    };
    let mut table_full = RangeScalarCacheCall::new(budget, Arc::clone(&governor));
    assert_eq!(
        table_full.classify_eligible(&first_key, 11),
        RangeScalarCacheLookup::Miss
    );
    assert_eq!(
        table_full
            .admit_with(
                first_key,
                cache_header(first_key.chunk_kind, 1),
                1,
                |emit| emit(cache_sample(1)),
            )
            .unwrap(),
        RangeScalarCacheAdmission::Admitted
    );
    assert_eq!(
        table_full.classify_eligible(&second_key, 13),
        RangeScalarCacheLookup::Miss
    );
    let mut callback_called = false;
    assert_eq!(
        table_full
            .admit_with(
                second_key,
                cache_header(second_key.chunk_kind, 1),
                1,
                |_| {
                    callback_called = true;
                    Ok(())
                },
            )
            .unwrap(),
        RangeScalarCacheAdmission::EntryTableFull
    );
    assert!(!callback_called);
    assert_eq!(table_full.summary().admitted_entries, 1);
    assert_eq!(table_full.summary().streaming_budget_bypasses, 1);

    drop(table_full);
    let layout = RangeScalarCacheLayout::for_budget(budget).unwrap();
    let oversized_count = layout.sample_capacity + 1;
    let mut oversized = RangeScalarCacheCall::new(budget, governor);
    assert_eq!(
        oversized.classify_eligible(&first_key, 17),
        RangeScalarCacheLookup::Miss
    );
    let mut callback_called = false;
    assert_eq!(
        oversized
            .admit_with(
                first_key,
                cache_header(first_key.chunk_kind, oversized_count as u32),
                oversized_count,
                |_| {
                    callback_called = true;
                    Ok(())
                },
            )
            .unwrap(),
        RangeScalarCacheAdmission::OversizedRecord
    );
    assert!(!callback_called);
    assert_eq!(oversized.summary().admitted_entries, 0);
    assert_eq!(oversized.summary().streaming_budget_bypasses, 1);
}

#[test]
fn cache_call_already_present_reuses_admitted_entry() {
    let key = cache_key();
    let mut call = RangeScalarCacheCall::new(MIB, Arc::new(RangeScalarCacheGovernor::new(MIB)));
    assert_eq!(
        call.classify_eligible(&key, 11),
        RangeScalarCacheLookup::Miss
    );
    assert_eq!(
        call.classify_eligible(&key, 11),
        RangeScalarCacheLookup::Miss
    );
    assert_eq!(
        call.admit_with(key, cache_header(key.chunk_kind, 1), 1, |emit| {
            emit(cache_sample(7))
        })
        .unwrap(),
        RangeScalarCacheAdmission::Admitted
    );
    let mut callback_called = false;
    assert_eq!(
        call.admit_with(key, cache_header(key.chunk_kind, 1), 1, |_| {
            callback_called = true;
            Ok(())
        })
        .unwrap(),
        RangeScalarCacheAdmission::AlreadyPresent
    );
    assert!(!callback_called);
    assert_eq!(call.summary().admitted_entries, 1);
    assert_eq!(call.summary().streaming_budget_bypasses, 0);
    assert_eq!(call.lookup(&key).unwrap().1, &[cache_sample(7)]);
}

#[test]
fn cache_call_metric_counters_and_bytes_saturate() {
    let key = cache_key();
    let mut unavailable = RangeScalarCacheCall::new(0, Arc::new(RangeScalarCacheGovernor::new(0)));
    unavailable.summary_mut().misses = u64::MAX;
    unavailable.summary_mut().streaming_budget_bypasses = u64::MAX;
    unavailable.summary_mut().logical_miss_or_bypass_bytes = u64::MAX;
    unavailable.summary_mut().unsupported_bypasses = u64::MAX;
    assert_eq!(
        unavailable.classify_eligible(&key, 1),
        RangeScalarCacheLookup::Miss
    );
    unavailable.classify_unsupported(1);
    assert_eq!(unavailable.summary().misses, u64::MAX);
    assert_eq!(unavailable.summary().streaming_budget_bypasses, u64::MAX);
    assert_eq!(unavailable.summary().unsupported_bypasses, u64::MAX);
    assert_eq!(unavailable.summary().logical_miss_or_bypass_bytes, u64::MAX);

    let mut available =
        RangeScalarCacheCall::new(MIB, Arc::new(RangeScalarCacheGovernor::new(MIB)));
    assert_eq!(
        available.classify_eligible(&key, 1),
        RangeScalarCacheLookup::Miss
    );
    available.summary_mut().admitted_entries = u64::MAX;
    assert_eq!(
        available
            .admit_with(key, cache_header(key.chunk_kind, 1), 1, |emit| {
                emit(cache_sample(1))
            })
            .unwrap(),
        RangeScalarCacheAdmission::Admitted
    );
    available.summary_mut().hits = u64::MAX;
    available.summary_mut().logical_hit_bytes = u64::MAX;
    assert_eq!(
        available.classify_eligible(&key, 1),
        RangeScalarCacheLookup::Hit
    );
    assert_eq!(available.summary().admitted_entries, u64::MAX);
    assert_eq!(available.summary().hits, u64::MAX);
    assert_eq!(available.summary().logical_hit_bytes, u64::MAX);
}

#[test]
fn cache_key_covers_every_chunk_and_projection_identity_field() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<ChunkScalarSample>();

    let base = cache_key();
    let keys = [
        base,
        RangeScalarCacheKey {
            segment_ordinal: 2,
            ..base
        },
        RangeScalarCacheKey { file_id: 1, ..base },
        RangeScalarCacheKey {
            chunk_offset: 11,
            ..base
        },
        RangeScalarCacheKey {
            chunk_len: 21,
            ..base
        },
        RangeScalarCacheKey {
            scalar_lane_offset: 41,
            ..base
        },
        RangeScalarCacheKey {
            scalar_lane_len: 51,
            ..base
        },
        RangeScalarCacheKey {
            projection: ChunkScalarProjection::Sum,
            ..base
        },
        RangeScalarCacheKey {
            chunk_kind: ChunkKind::Summary,
            ..base
        },
    ];

    let mut cache =
        RangeScalarDecodeCache::try_new_in(MIB, Global).expect("cache allocation must succeed");
    for (index, key) in keys.iter().copied().rev().enumerate() {
        assert_eq!(
            admit_one(&mut cache, key, index as u64 + 1),
            RangeScalarCacheAdmission::Admitted
        );
    }
    assert_eq!(cache.entry_len(), keys.len());
    for key in keys {
        let (header, samples) = cache.lookup(&key).expect("complete key must hit");
        assert_eq!(header, cache_header(key.chunk_kind, 1));
        assert_eq!(samples.len(), 1);
    }
    assert_ne!(
        cache.lookup(&base).unwrap().1[0].timestamp_ms,
        cache
            .lookup(&RangeScalarCacheKey {
                projection: ChunkScalarProjection::Sum,
                ..base
            })
            .unwrap()
            .1[0]
            .timestamp_ms,
        "count and sum must never alias"
    );
}

#[test]
fn cache_table_full_and_oversized_records_bypass_without_partial_insert() {
    let budget = (4 * mem::size_of::<RangeScalarCacheEntry>()) as u64;
    let mut cache = RangeScalarDecodeCache::try_new_in(budget, Global)
        .expect("small exact cache allocation must succeed");
    assert_eq!(cache.entry_capacity(), 1);
    assert_eq!(
        admit_one(&mut cache, cache_key(), 1),
        RangeScalarCacheAdmission::Admitted
    );
    let samples_before = cache.sample_len();
    let mut callback_called = false;
    let second_key = RangeScalarCacheKey {
        chunk_offset: 99,
        ..cache_key()
    };
    assert_eq!(
        cache
            .admit_with(
                second_key,
                cache_header(second_key.chunk_kind, 1),
                1,
                |_| {
                    callback_called = true;
                    Ok(())
                }
            )
            .unwrap(),
        RangeScalarCacheAdmission::EntryTableFull
    );
    assert!(!callback_called);
    assert_eq!(cache.entry_len(), 1);
    assert_eq!(cache.sample_len(), samples_before);

    let mut empty = RangeScalarDecodeCache::try_new_in(budget, Global)
        .expect("small exact cache allocation must succeed");
    let oversized = empty.sample_capacity() + 1;
    let mut callback_called = false;
    assert_eq!(
        empty
            .admit_with(
                cache_key(),
                cache_header(ChunkKind::Histogram, oversized as u32),
                oversized,
                |_| {
                    callback_called = true;
                    Ok(())
                },
            )
            .unwrap(),
        RangeScalarCacheAdmission::OversizedRecord
    );
    assert!(!callback_called);
    assert_eq!(empty.entry_len(), 0);
    assert_eq!(empty.sample_len(), 0);
}

#[test]
fn cache_decode_error_rolls_back_partial_samples_and_reuses_capacity() {
    let mut cache =
        RangeScalarDecodeCache::try_new_in(MIB, Global).expect("cache allocation must succeed");
    let error = cache
        .admit_with(
            cache_key(),
            cache_header(ChunkKind::Histogram, 2),
            2,
            |emit| {
                emit(cache_sample(1))?;
                Err(io::Error::new(io::ErrorKind::InvalidData, "decode stopped"))
            },
        )
        .expect_err("decode error must propagate");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "decode stopped");
    assert_eq!(cache.entry_len(), 0);
    assert_eq!(cache.sample_len(), 0);

    assert_eq!(
        cache
            .admit_with(
                cache_key(),
                cache_header(ChunkKind::Histogram, 2),
                2,
                |emit| {
                    emit(cache_sample(1))?;
                    emit(cache_sample(2))
                }
            )
            .unwrap(),
        RangeScalarCacheAdmission::Admitted
    );
    assert_eq!(cache.sample_len(), 2);
}

#[test]
fn cache_rejects_short_or_overfull_decode_even_if_callback_ignores_emit_error() {
    let mut short =
        RangeScalarDecodeCache::try_new_in(MIB, Global).expect("cache allocation must succeed");
    let error = short
        .admit_with(
            cache_key(),
            cache_header(ChunkKind::Histogram, 2),
            2,
            |emit| emit(cache_sample(1)),
        )
        .expect_err("short decode must fail");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(short.entry_len(), 0);
    assert_eq!(short.sample_len(), 0);

    let mut overfull =
        RangeScalarDecodeCache::try_new_in(MIB, Global).expect("cache allocation must succeed");
    let error = overfull
        .admit_with(
            cache_key(),
            cache_header(ChunkKind::Histogram, 1),
            1,
            |emit| {
                emit(cache_sample(1))?;
                let _ignored = emit(cache_sample(2));
                Ok(())
            },
        )
        .expect_err("overfull decode must fail even when emit error is ignored");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(overfull.entry_len(), 0);
    assert_eq!(overfull.sample_len(), 0);
}

#[derive(Debug)]
struct DropTracker(Arc<AtomicUsize>);

impl Drop for DropTracker {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn exact_arena_has_fixed_capacity_and_safe_initialized_prefix() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut arena = ExactInitArena::try_new_in(2, Global).expect("allocation must succeed");
    assert_eq!(arena.capacity(), 2);
    assert_eq!(arena.remaining(), 2);
    assert!(arena.initialized_prefix().is_empty());

    assert!(matches!(arena.push(DropTracker(Arc::clone(&drops))), Ok(0)));
    assert_eq!(arena.initialized_len(), 1);
    assert_eq!(arena.remaining(), 1);
    assert_eq!(arena.initialized_prefix().len(), 1);

    let rejected = arena
        .push(DropTracker(Arc::clone(&drops)))
        .and_then(|_| arena.push(DropTracker(Arc::clone(&drops))))
        .expect_err("arena must never grow");
    drop(rejected);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(arena);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

#[test]
fn exact_arena_reservation_rolls_back_partial_initialization() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut arena = ExactInitArena::try_new_in(3, Global).expect("allocation must succeed");
    arena
        .push(DropTracker(Arc::clone(&drops)))
        .expect("first push must fit");

    {
        let mut reservation = arena.reserve(2).expect("reservation must fit");
        reservation
            .push(DropTracker(Arc::clone(&drops)))
            .expect("reserved push must fit");
        // A decode error drops this uncommitted reservation.
    }

    assert_eq!(arena.initialized_len(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(arena);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn exact_arena_reservation_commit_and_sorted_insert_keep_all_values() {
    let mut arena = ExactInitArena::try_new_in(4, Global).expect("allocation must succeed");
    arena.push(2).expect("push must fit");
    arena.insert(0, 1).expect("insert must fit");
    {
        let mut reservation = arena.reserve(2).expect("reservation must fit");
        reservation.push(3).expect("reserved push must fit");
        reservation.push(4).expect("reserved push must fit");
        assert_eq!(reservation.commit(), 2..4);
    }
    assert_eq!(arena.initialized_prefix(), &[1, 2, 3, 4]);
}

#[test]
fn range_scalar_cache_layout_never_charges_more_than_budget() {
    let entry_size = mem::size_of::<RangeScalarCacheEntry>();
    let sample_size = mem::size_of::<ChunkScalarSample>();
    for budget in [
        0,
        1,
        entry_size.saturating_sub(1) as u64,
        4 * MIB,
        8 * MIB,
        16 * MIB,
        32 * MIB,
    ] {
        let layout = RangeScalarCacheLayout::for_budget(budget).expect("layout must fit");
        assert!(layout.entry_charge_bytes + layout.sample_charge_bytes <= budget);
        let expected_entry_capacity = (((budget as usize) / 4) / entry_size).min(16_384);
        let expected_entry_charge = expected_entry_capacity * entry_size;
        let expected_sample_capacity = ((budget as usize) - expected_entry_charge) / sample_size;
        assert_eq!(layout.entry_capacity, expected_entry_capacity);
        assert_eq!(layout.entry_charge_bytes as usize, expected_entry_charge);
        assert_eq!(layout.sample_capacity, expected_sample_capacity);
        assert_eq!(
            layout.sample_charge_bytes as usize,
            expected_sample_capacity * sample_size
        );
    }
}

#[test]
fn range_scalar_cache_default_budget_is_sixteen_mib() {
    assert_eq!(DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES, 16 * MIB);
    let layout = RangeScalarCacheLayout::for_budget(DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES)
        .expect("default layout must fit");
    assert_eq!(
        layout.entry_capacity,
        ((DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES as usize / 4)
            / std::mem::size_of::<super::range_scalar_cache::RangeScalarCacheEntry>())
        .min(16_384)
    );
    println!(
        "range_scalar_cache_default_layout entry_size={} sample_size={} entry_capacity={} sample_capacity={} entry_charge_bytes={} sample_charge_bytes={} total_charge_bytes={}",
        mem::size_of::<RangeScalarCacheEntry>(),
        mem::size_of::<ChunkScalarSample>(),
        layout.entry_capacity,
        layout.sample_capacity,
        layout.entry_charge_bytes,
        layout.sample_charge_bytes,
        layout.entry_charge_bytes + layout.sample_charge_bytes,
    );
}

#[test]
fn range_scalar_cache_layout_overflow_is_classified() {
    assert_eq!(
        RangeScalarCacheLayout::for_budget(u64::MAX),
        Err(RangeScalarCacheLayoutError::LayoutOverflow)
    );
}

#[test]
fn public_budget_validation_rejects_only_values_above_maximum() {
    assert_eq!(MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES, 32 * MIB);
    assert_eq!(
        validate_range_scalar_cache_budget_bytes(MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES),
        Ok(())
    );
    assert_eq!(
        validate_range_scalar_cache_budget_bytes(MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES + 1),
        Err(RangeScalarCacheConfigError::BudgetTooLarge {
            requested_bytes: MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES + 1,
            maximum_bytes: MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES,
        })
    );
    assert_eq!(
        RangeScalarCacheConfigError::BudgetTooLarge {
            requested_bytes: 33,
            maximum_bytes: 32,
        }
        .to_string(),
        "range scalar cache budget exceeds maximum: requested=33 maximum=32"
    );
    assert_eq!(
        RangeScalarCacheConfigError::GovernorAlreadyInitialized {
            existing_bytes: 17,
            requested_bytes: 18,
        }
        .to_string(),
        "range scalar cache governor already initialized with a different limit: existing=17 requested=18"
    );

    let _: fn(u64) -> Result<(), RangeScalarCacheConfigError> =
        super::configure_range_scalar_cache_governor;
    let _: fn() -> super::RangeScalarCacheGovernorStats = super::range_scalar_cache_governor_stats;
    let summary = super::RangeScalarCacheSummary::default();
    assert_eq!(summary.configured_budget_bytes, 0);
    assert_eq!(summary.governor_lease_bytes, 0);
    assert!(!summary.governor_refused);
    assert!(!summary.allocation_refused);
    assert!(!summary.layout_overflow);
    assert_eq!(summary.entry_arena_charge_bytes, 0);
    assert_eq!(summary.sample_arena_charge_bytes, 0);
    assert_eq!(summary.hits, 0);
    assert_eq!(summary.misses, 0);
    assert_eq!(summary.admitted_entries, 0);
    assert_eq!(summary.streaming_budget_bypasses, 0);
    assert_eq!(summary.unsupported_bypasses, 0);
    assert_eq!(summary.logical_hit_bytes, 0);
    assert_eq!(summary.logical_miss_or_bypass_bytes, 0);
    assert_eq!(summary.peak_retained_charge_bytes, 0);
    assert_eq!(summary.retained_charge_after_finalize, 0);
}

#[test]
fn governor_refuses_over_limit_without_mutating_current_charge() {
    let governor = Arc::new(RangeScalarCacheGovernor::new(
        DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
    ));
    let lease = governor
        .try_acquire(DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES)
        .expect("complete limit must be admissible");

    assert!(governor.try_acquire(1).is_none());
    assert_eq!(
        governor.stats().current_leased_bytes,
        DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES
    );

    drop(lease);
    assert_eq!(governor.stats().current_leased_bytes, 0);
}
#[test]
fn isolated_governor_configuration_is_idempotent_and_typed_on_conflict() {
    let cell = std::sync::OnceLock::new();
    assert_eq!(configure_range_scalar_cache_governor_in(&cell, 17), Ok(()));
    assert_eq!(configure_range_scalar_cache_governor_in(&cell, 17), Ok(()));
    assert_eq!(
        configure_range_scalar_cache_governor_in(&cell, 18),
        Err(RangeScalarCacheConfigError::GovernorAlreadyInitialized {
            existing_bytes: 17,
            requested_bytes: 18,
        })
    );
}

#[test]
fn logical_chunk_observation_matches_combined_reader_profile() {
    let tempdir = tempfile::tempdir().unwrap();
    let config =
        super::SegmentWriterConfig::new(tempdir.path(), std::time::Duration::from_secs(10))
            .with_storage_schema(super::SegmentStorageSchema::Schema6);
    let mut writer = super::SegmentWriter::new(config).unwrap();
    writer
        .record_sample(super::SeriesRef::new(1), 1_000, 1.0)
        .unwrap();
    writer.flush().unwrap();

    let segment_dir = std::fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    std::fs::OpenOptions::new()
        .write(true)
        .open(segment_dir.join(super::SegmentFile::Chunks.filename()))
        .unwrap()
        .set_len(128 * 1024)
        .unwrap();
    super::write_segment_footer_for_schema6(&segment_dir).unwrap();
    let reader = super::open_schema6_segment_for_test(&segment_dir).unwrap();

    let requests = [
        super::ChunkPayloadRead {
            file_id: 0,
            offset: 70_032,
            len: 64,
        },
        super::ChunkPayloadRead {
            file_id: 0,
            offset: 150,
            len: 100,
        },
        super::ChunkPayloadRead {
            file_id: 0,
            offset: 100,
            len: 100,
        },
        super::ChunkPayloadRead {
            file_id: 0,
            offset: 4_396,
            len: 100,
        },
        super::ChunkPayloadRead {
            file_id: 0,
            offset: 250,
            len: 50,
        },
    ];

    let mut combined = super::SegmentQueryContext::open(&reader).unwrap();
    let combined_batch = combined
        .read_chunk_payload_batch(&reader, &requests)
        .unwrap();

    let mut split = super::SegmentQueryContext::open(&reader).unwrap();
    split.observe_chunk_payload_requests(&requests);
    let split_batch = split
        .read_chunk_payload_batch_physical(&reader, &requests)
        .unwrap();

    assert_eq!(combined.profile.chunk_payload_bytes, 414);
    assert_eq!(
        split.profile.chunk_payload_bytes,
        combined.profile.chunk_payload_bytes
    );
    assert_eq!(
        split.profile.chunk_payload_locality,
        combined.profile.chunk_payload_locality
    );

    assert_eq!(combined_batch.physical_read_count(), 2);
    assert_eq!(combined_batch.physical_bytes_read(), 4_460);
    assert_eq!(split_batch.physical_read_count(), 2);
    assert_eq!(split_batch.physical_bytes_read(), 4_460);
    assert_eq!(combined.profile.chunk_payload_physical_reads, 2);
    assert_eq!(combined.profile.chunk_payload_physical_bytes, 4_460);
    assert_eq!(
        split.profile.chunk_payload_physical_reads,
        combined.profile.chunk_payload_physical_reads
    );
    assert_eq!(
        split.profile.chunk_payload_physical_bytes,
        combined.profile.chunk_payload_physical_bytes
    );
}
