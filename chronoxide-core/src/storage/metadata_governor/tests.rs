use super::*;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;

fn config(retained: u64, in_flight: u64) -> MetadataGovernorConfig {
    MetadataGovernorConfig {
        retained_max_bytes: retained,
        in_flight_max_bytes: in_flight,
        max_open_files: 4,
        max_cached_open_files: 2,
    }
}

fn assert_usage_totals_reconcile(stats: MetadataGovernorStats) {
    assert_eq!(
        stats.usage.map(|entry| entry.usage),
        METADATA_USAGE_CLASS_ORDER
    );
    assert_eq!(
        stats
            .usage
            .iter()
            .map(|entry| entry.in_flight_bytes)
            .sum::<u64>(),
        stats.in_flight_bytes
    );
    assert_eq!(
        stats
            .usage
            .iter()
            .map(|entry| entry.retained_bytes)
            .sum::<u64>(),
        stats.retained_bytes
    );
    assert!(stats.peak_in_flight_bytes >= stats.in_flight_bytes);
    assert!(stats.peak_retained_bytes >= stats.retained_bytes);
    assert!(stats.usage.iter().all(|entry| {
        entry.peak_in_flight_bytes >= entry.in_flight_bytes
            && entry.peak_retained_bytes >= entry.retained_bytes
    }));
}

#[test]
fn configuration_rejects_only_invalid_hard_constraints() {
    assert_eq!(
        MetadataGovernorConfig::default(),
        MetadataGovernorConfig {
            retained_max_bytes: 64 * 1024 * 1024,
            in_flight_max_bytes: 256 * 1024 * 1024,
            max_open_files: 128,
            max_cached_open_files: 64,
        }
    );
    assert_eq!(
        config(0, 0).validate(),
        Err(MetadataGovernorConfigError::ZeroInFlightBudget)
    );
    assert_eq!(
        MetadataGovernorConfig {
            max_open_files: 0,
            ..config(0, 1)
        }
        .validate(),
        Err(MetadataGovernorConfigError::ZeroOpenFileLimit)
    );
    assert_eq!(
        MetadataGovernorConfig {
            max_cached_open_files: 5,
            ..config(0, 1)
        }
        .validate(),
        Err(
            MetadataGovernorConfigError::CachedOpenFileLimitExceedsHardLimit { cached: 5, hard: 4 }
        )
    );
    assert_eq!(config(0, 1).validate(), Ok(config(0, 1)));
    let zero_cached = MetadataGovernorConfig {
        max_cached_open_files: 0,
        ..config(0, 1)
    };
    assert_eq!(zero_cached.validate(), Ok(zero_cached));
}

#[test]
fn reservations_are_checked_and_released_by_all_error_paths() {
    let governor = MetadataGovernor::new(config(8, 10)).unwrap();
    let first = governor.reserve_in_flight(6).unwrap();
    let error = governor.reserve_in_flight(5).unwrap_err();
    assert_eq!(error.class, MetadataChargeClass::InFlight);
    assert_eq!(error.current_bytes, 6);
    assert_eq!(governor.stats().in_flight_bytes, 6);
    drop(first);
    assert_eq!(governor.stats().in_flight_bytes, 0);
    assert_eq!(governor.stats().in_flight_refusals, 1);
}

#[test]
fn reconciliation_is_atomic_on_refusal_and_shrinks_exactly() {
    let governor = MetadataGovernor::new(config(8, 10)).unwrap();
    let mut charge = governor.reserve_in_flight(6).unwrap();
    assert!(charge.reconcile(11).is_err());
    assert_eq!(charge.bytes(), 6);
    assert_eq!(governor.stats().in_flight_bytes, 6);

    charge.reconcile(3).unwrap();
    assert_eq!(charge.bytes(), 3);
    assert_eq!(governor.stats().in_flight_bytes, 3);
    drop(charge);
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn promotion_transfers_without_uncharged_or_double_charged_interval() {
    let governor = MetadataGovernor::new(config(8, 10)).unwrap();
    let mut charge = governor.reserve_in_flight(7).unwrap();
    assert!(charge.try_promote_to_retained());
    assert_eq!(charge.class(), MetadataChargeClass::Retained);
    assert_eq!(governor.stats().in_flight_bytes, 0);
    assert_eq!(governor.stats().retained_bytes, 7);
    let unclassified = governor.stats().usage(MetadataUsageClass::Unclassified);
    assert_eq!(unclassified.in_flight_bytes, 0);
    assert_eq!(unclassified.retained_bytes, 7);
    assert_eq!(unclassified.peak_in_flight_bytes, 7);
    assert_eq!(unclassified.peak_retained_bytes, 7);
    drop(charge);
    assert_eq!(governor.stats().retained_bytes, 0);
}

#[test]
fn retention_refusal_keeps_transient_charge_live() {
    let governor = MetadataGovernor::new(config(0, 10)).unwrap();
    let mut charge = governor.reserve_in_flight(7).unwrap();
    assert!(!charge.try_promote_to_retained());
    assert_eq!(charge.class(), MetadataChargeClass::InFlight);
    assert_eq!(governor.stats().in_flight_bytes, 7);
    assert_eq!(governor.stats().retained_bytes, 0);
    assert_eq!(governor.stats().retained_refusals, 1);
    drop(charge);
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn scratch_handoff_atomically_installs_retained_cache_charges() {
    let governor = MetadataGovernor::new(config(16, 16)).unwrap();
    let usage = MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage);
    let mut final_charge = governor.reserve_in_flight_for_usage(6, usage).unwrap();
    let mut scratch_charge = governor
        .reserve_in_flight_for_usage(4, MetadataUsageClass::Scratch)
        .unwrap();

    let handoff =
        admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 2, Some(3)).unwrap();

    assert_eq!(scratch_charge.bytes(), 0);
    assert_eq!(final_charge.class(), MetadataChargeClass::Retained);
    assert_eq!(handoff.live_charge.class(), MetadataChargeClass::Retained);
    let resident_charge = handoff.resident_charge.unwrap();
    assert_eq!(resident_charge.class(), MetadataChargeClass::Retained);
    let stats = governor.stats();
    assert_eq!(stats.in_flight_bytes, 0);
    assert_eq!(stats.retained_bytes, 11);
    assert_eq!(stats.usage(MetadataUsageClass::Scratch).in_flight_bytes, 0);
    assert_eq!(stats.usage(usage).in_flight_bytes, 0);
    assert_eq!(stats.usage(usage).retained_bytes, 11);
    assert_usage_totals_reconcile(stats);

    drop((
        scratch_charge,
        final_charge,
        handoff.live_charge,
        resident_charge,
    ));
    assert_eq!(governor.stats().in_flight_bytes, 0);
    assert_eq!(governor.stats().retained_bytes, 0);
}

#[test]
fn scratch_free_cache_admission_uses_the_same_atomic_transition() {
    let governor = MetadataGovernor::new(config(16, 6)).unwrap();
    let usage = MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage);
    let mut final_charge = governor.reserve_in_flight_for_usage(6, usage).unwrap();

    let handoff = admit_cache_allocation(&mut final_charge, None, 2, Some(3)).unwrap();

    assert_eq!(final_charge.class(), MetadataChargeClass::Retained);
    assert_eq!(handoff.live_charge.class(), MetadataChargeClass::Retained);
    let resident_charge = handoff.resident_charge.unwrap();
    assert_eq!(resident_charge.class(), MetadataChargeClass::Retained);
    let stats = governor.stats();
    assert_eq!(stats.in_flight_bytes, 0);
    assert_eq!(stats.retained_bytes, 11);
    assert_eq!(stats.usage(usage).retained_bytes, 11);
    assert_usage_totals_reconcile(stats);

    drop((final_charge, handoff.live_charge, resident_charge));
    assert_eq!(governor.stats().retained_bytes, 0);
}

#[test]
fn scratch_handoff_reuses_capacity_for_transient_live_charge() {
    let governor = MetadataGovernor::new(config(0, 10)).unwrap();
    let usage = MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage);
    let mut final_charge = governor.reserve_in_flight_for_usage(6, usage).unwrap();
    let mut scratch_charge = governor
        .reserve_in_flight_for_usage(4, MetadataUsageClass::Scratch)
        .unwrap();
    assert!(
        governor.reserve_in_flight_for_usage(3, usage).is_err(),
        "a separate live reservation cannot fit before scratch release"
    );

    let handoff =
        admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 3, None).unwrap();

    assert!(handoff.resident_charge.is_none());
    assert_eq!(scratch_charge.bytes(), 0);
    assert_eq!(final_charge.class(), MetadataChargeClass::InFlight);
    assert_eq!(handoff.live_charge.class(), MetadataChargeClass::InFlight);
    let stats = governor.stats();
    assert_eq!(stats.in_flight_bytes, 9);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(stats.usage(MetadataUsageClass::Scratch).in_flight_bytes, 0);
    assert_eq!(stats.usage(usage).in_flight_bytes, 9);
    assert_usage_totals_reconcile(stats);

    drop((scratch_charge, final_charge, handoff.live_charge));
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn scratch_handoff_retention_refusal_falls_back_to_transient() {
    let governor = MetadataGovernor::new(config(8, 10)).unwrap();
    let usage = MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage);
    let mut final_charge = governor.reserve_in_flight_for_usage(6, usage).unwrap();
    let mut scratch_charge = governor
        .reserve_in_flight_for_usage(4, MetadataUsageClass::Scratch)
        .unwrap();

    let handoff =
        admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 3, Some(2)).unwrap();

    assert!(handoff.resident_charge.is_none());
    assert_eq!(final_charge.class(), MetadataChargeClass::InFlight);
    let stats = governor.stats();
    assert_eq!(stats.retained_refusals, 1);
    assert_eq!(stats.in_flight_bytes, 9);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(stats.usage(MetadataUsageClass::Scratch).in_flight_bytes, 0);
    assert_eq!(stats.usage(usage).in_flight_bytes, 9);
    assert_usage_totals_reconcile(stats);

    drop((scratch_charge, final_charge, handoff.live_charge));
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn retained_overflow_falls_back_without_leaking_any_handoff_charge() {
    let governor = MetadataGovernor::new(config(u64::MAX, u64::MAX)).unwrap();
    let mut existing = governor.reserve_in_flight(u64::MAX).unwrap();
    assert!(existing.try_promote_to_retained());
    let usage = MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage);
    let mut final_charge = governor.reserve_in_flight_for_usage(6, usage).unwrap();
    let mut scratch_charge = governor
        .reserve_in_flight_for_usage(4, MetadataUsageClass::Scratch)
        .unwrap();

    let handoff =
        admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 3, Some(2)).unwrap();

    assert!(handoff.resident_charge.is_none());
    assert_eq!(governor.stats().retained_refusals, 1);
    assert_eq!(governor.stats().retained_bytes, u64::MAX);
    assert_eq!(governor.stats().in_flight_bytes, 9);
    assert_usage_totals_reconcile(governor.stats());
    drop((scratch_charge, final_charge, handoff.live_charge));
    assert_eq!(governor.stats().in_flight_bytes, 0);
    drop(existing);
    let released = governor.stats();
    assert_eq!(released.in_flight_bytes, 0);
    assert_eq!(released.retained_bytes, 0);
    assert_usage_totals_reconcile(released);
}

#[test]
fn refused_scratch_handoff_leaves_inputs_unchanged_for_cleanup() {
    let governor = MetadataGovernor::new(config(0, 8)).unwrap();
    let usage = MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage);
    let mut final_charge = governor.reserve_in_flight_for_usage(6, usage).unwrap();
    let mut scratch_charge = governor
        .reserve_in_flight_for_usage(2, MetadataUsageClass::Scratch)
        .unwrap();

    let error =
        admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 3, None).unwrap_err();
    assert_eq!(error.class, MetadataChargeClass::InFlight);
    assert_eq!(error.requested_bytes, 3);
    assert_eq!(error.current_bytes, 6);
    assert_eq!(final_charge.bytes(), 6);
    assert_eq!(scratch_charge.bytes(), 2);
    let stats = governor.stats();
    assert_eq!(stats.in_flight_bytes, 8);
    assert_eq!(stats.usage(MetadataUsageClass::Scratch).in_flight_bytes, 2);
    assert_eq!(stats.usage(usage).in_flight_bytes, 6);
    assert_usage_totals_reconcile(stats);

    drop((scratch_charge, final_charge));
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn checked_add_overflow_is_an_explicit_refusal() {
    let governor = MetadataGovernor::new(config(0, u64::MAX)).unwrap();
    let _charge = governor.reserve_in_flight(u64::MAX).unwrap();
    let error = governor.reserve_in_flight(1).unwrap_err();
    assert_eq!(error.current_bytes, u64::MAX);
    assert_eq!(error.limit_bytes, u64::MAX);
    assert_eq!(governor.stats().in_flight_refusals, 1);
}

#[test]
fn promotion_checked_add_overflow_leaves_both_charges_accounted() {
    let governor = MetadataGovernor::new(config(u64::MAX, u64::MAX)).unwrap();
    let mut retained = governor.reserve_in_flight(u64::MAX).unwrap();
    assert!(retained.try_promote_to_retained());

    let mut transient = governor.reserve_in_flight(1).unwrap();
    assert!(!transient.try_promote_to_retained());
    assert_eq!(transient.class(), MetadataChargeClass::InFlight);
    assert_eq!(governor.stats().retained_bytes, u64::MAX);
    assert_eq!(governor.stats().in_flight_bytes, 1);
    assert_eq!(governor.stats().retained_refusals, 1);

    drop(transient);
    drop(retained);
    assert_eq!(governor.stats().retained_bytes, 0);
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn concurrent_reservations_never_exceed_the_hard_budget() {
    const THREADS: usize = 16;
    const LIMIT: u64 = 8;

    let governor = MetadataGovernor::new(config(0, LIMIT)).unwrap();
    let start = Arc::new(Barrier::new(THREADS + 1));
    let observed = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    let mut workers = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let governor = Arc::clone(&governor);
        let start = Arc::clone(&start);
        let observed = Arc::clone(&observed);
        let release = Arc::clone(&release);
        workers.push(thread::spawn(move || {
            start.wait();
            let charge = governor.reserve_in_flight(1).ok();
            observed.wait();
            release.wait();
            charge.is_some()
        }));
    }

    start.wait();
    observed.wait();
    let held = governor.stats();
    assert_eq!(held.in_flight_bytes, LIMIT);
    assert_eq!(held.peak_in_flight_bytes, LIMIT);
    assert_eq!(held.in_flight_refusals, THREADS as u64 - LIMIT);
    release.wait();

    let admitted = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(admitted as u64, LIMIT);
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn concurrent_promotions_atomically_split_retained_and_transient_charges() {
    const THREADS: usize = 8;
    const RETAINED_LIMIT: u64 = 4;

    let governor = MetadataGovernor::new(config(RETAINED_LIMIT, THREADS as u64)).unwrap();
    let charges: Vec<_> = (0..THREADS)
        .map(|_| governor.reserve_in_flight(1).unwrap())
        .collect();
    let start = Arc::new(Barrier::new(THREADS + 1));
    let observed = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    let mut workers = Vec::with_capacity(THREADS);
    for mut charge in charges {
        let start = Arc::clone(&start);
        let observed = Arc::clone(&observed);
        let release = Arc::clone(&release);
        workers.push(thread::spawn(move || {
            start.wait();
            let retained = charge.try_promote_to_retained();
            observed.wait();
            release.wait();
            retained
        }));
    }

    start.wait();
    observed.wait();
    let held = governor.stats();
    assert_eq!(held.retained_bytes, RETAINED_LIMIT);
    assert_eq!(held.in_flight_bytes, THREADS as u64 - RETAINED_LIMIT);
    assert_eq!(held.retained_refusals, THREADS as u64 - RETAINED_LIMIT);
    release.wait();

    let retained = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .filter(|retained| *retained)
        .count();
    assert_eq!(retained as u64, RETAINED_LIMIT);
    let released = governor.stats();
    assert_eq!(released.retained_bytes, 0);
    assert_eq!(released.in_flight_bytes, 0);
}

#[test]
fn concurrent_scratch_handoffs_keep_usage_and_aggregate_totals_exact() {
    const THREADS: usize = 8;
    const RETAINED_ADMISSIONS: u64 = 4;
    let governor = MetadataGovernor::new(config(32, 64)).unwrap();
    let usage = MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage);
    let ready = Arc::new(Barrier::new(THREADS + 1));
    let start_admission = Arc::new(Barrier::new(THREADS + 1));
    let admitted = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    let mut workers = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let governor = Arc::clone(&governor);
        let ready = Arc::clone(&ready);
        let start_admission = Arc::clone(&start_admission);
        let admitted = Arc::clone(&admitted);
        let release = Arc::clone(&release);
        workers.push(thread::spawn(move || {
            let mut final_charge = governor.reserve_in_flight_for_usage(4, usage).unwrap();
            let mut scratch_charge = governor
                .reserve_in_flight_for_usage(4, MetadataUsageClass::Scratch)
                .unwrap();
            ready.wait();
            start_admission.wait();
            let handoff =
                admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 2, Some(2))
                    .unwrap();
            let retained = handoff.resident_charge.is_some();
            admitted.wait();
            release.wait();
            drop((handoff, scratch_charge, final_charge));
            retained
        }));
    }

    ready.wait();
    let before = governor.stats();
    assert_eq!(before.in_flight_bytes, 64);
    assert_eq!(before.retained_bytes, 0);
    assert_eq!(
        before.usage(MetadataUsageClass::Scratch).in_flight_bytes,
        32
    );
    assert_eq!(before.usage(usage).in_flight_bytes, 32);
    assert_usage_totals_reconcile(before);

    start_admission.wait();
    admitted.wait();
    let held = governor.stats();
    assert_eq!(held.retained_bytes, 32);
    assert_eq!(held.in_flight_bytes, 24);
    assert_eq!(held.retained_refusals, THREADS as u64 - RETAINED_ADMISSIONS);
    assert_eq!(held.usage(MetadataUsageClass::Scratch).in_flight_bytes, 0);
    assert_eq!(held.usage(usage).retained_bytes, 32);
    assert_eq!(held.usage(usage).in_flight_bytes, 24);
    assert_usage_totals_reconcile(held);
    release.wait();

    let retained = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .filter(|retained| *retained)
        .count();
    assert_eq!(retained as u64, RETAINED_ADMISSIONS);
    let released = governor.stats();
    assert_eq!(released.in_flight_bytes, 0);
    assert_eq!(released.retained_bytes, 0);
    assert_usage_totals_reconcile(released);
}

#[test]
fn pin_clones_share_identity_and_release_only_after_the_value_drops() {
    struct DropObserver {
        governor: Arc<MetadataGovernor>,
        charge_seen_during_drop: Arc<AtomicU64>,
    }

    impl Drop for DropObserver {
        fn drop(&mut self) {
            self.charge_seen_during_drop
                .store(self.governor.stats().in_flight_bytes, Ordering::SeqCst);
        }
    }

    let governor = MetadataGovernor::new(config(0, 10)).unwrap();
    let observed = Arc::new(AtomicU64::new(0));
    let pin = governor
        .reserve_in_flight(7)
        .unwrap()
        .into_pin(DropObserver {
            governor: Arc::clone(&governor),
            charge_seen_during_drop: Arc::clone(&observed),
        });
    let clone = pin.clone();

    assert!(MetadataPin::ptr_eq(&pin, &clone));
    assert_eq!(pin.charge_class(), MetadataChargeClass::InFlight);
    assert_eq!(pin.charged_bytes(), 7);
    assert_eq!(governor.stats().in_flight_bytes, 7);

    drop(pin);
    assert_eq!(observed.load(Ordering::SeqCst), 0);
    assert_eq!(governor.stats().in_flight_bytes, 7);

    drop(clone);
    assert_eq!(observed.load(Ordering::SeqCst), 7);
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn distinct_pins_with_equal_values_have_distinct_allocation_identity() {
    let governor = MetadataGovernor::new(config(0, 10)).unwrap();
    let first = governor.reserve_in_flight(2).unwrap().into_pin([1_u8, 2]);
    let second = governor.reserve_in_flight(2).unwrap().into_pin([1_u8, 2]);

    assert!(!MetadataPin::ptr_eq(&first, &second));
    assert_eq!(*first, *second);
    assert_eq!(governor.stats().in_flight_bytes, 4);
    drop((first, second));
    assert_eq!(governor.stats().in_flight_bytes, 0);
}

#[test]
fn retained_pin_clones_keep_one_retained_charge_until_final_drop() {
    let governor = MetadataGovernor::new(config(10, 10)).unwrap();
    let mut charge = governor.reserve_in_flight(6).unwrap();
    assert!(charge.try_promote_to_retained());
    let pin = charge.into_pin(vec![1_u8, 2, 3]);
    let clone = pin.clone();

    assert_eq!(pin.charge_class(), MetadataChargeClass::Retained);
    assert_eq!(governor.stats().retained_bytes, 6);
    assert_eq!(governor.stats().in_flight_bytes, 0);
    drop(pin);
    assert_eq!(governor.stats().retained_bytes, 6);
    drop(clone);
    assert_eq!(governor.stats().retained_bytes, 0);
}

#[test]
fn concurrent_usage_snapshots_atomically_reconcile_with_aggregate_totals() {
    const WORKERS: usize = 8;
    const ITERATIONS: usize = 2_000;

    let governor = MetadataGovernor::new(config(1_024, 1_024)).unwrap();
    let start = Arc::new(Barrier::new(WORKERS + 1));
    let active = Arc::new(AtomicUsize::new(WORKERS));
    let mut workers = Vec::with_capacity(WORKERS);
    for worker_index in 0..WORKERS {
        let governor = Arc::clone(&governor);
        let start = Arc::clone(&start);
        let active = Arc::clone(&active);
        workers.push(thread::spawn(move || {
            start.wait();
            for iteration in 0..ITERATIONS {
                let cache_class = METADATA_CACHE_CLASS_ORDER
                    [(worker_index + iteration) % METADATA_CACHE_CLASS_COUNT];
                let mut cache_charge = governor
                    .reserve_in_flight_for_usage(4, MetadataUsageClass::Cache(cache_class))
                    .unwrap();
                let mut scratch_charge = governor
                    .reserve_in_flight_for_usage(3, MetadataUsageClass::Scratch)
                    .unwrap();
                let mut ledger_charge = governor
                    .reserve_in_flight_for_usage(2, MetadataUsageClass::CorruptionLedger)
                    .unwrap();
                if iteration % 2 == 0 {
                    cache_charge.reconcile(2).unwrap();
                }
                if iteration % 3 == 0 {
                    assert!(cache_charge.try_promote_to_retained());
                    assert!(scratch_charge.try_promote_to_retained());
                    assert!(ledger_charge.try_promote_to_retained());
                }
                if iteration % 64 == 0 {
                    thread::yield_now();
                }
                drop((cache_charge, scratch_charge, ledger_charge));
            }
            active.fetch_sub(1, Ordering::Release);
        }));
    }

    start.wait();
    let mut snapshots = 0usize;
    while active.load(Ordering::Acquire) != 0 {
        assert_usage_totals_reconcile(governor.stats());
        snapshots += 1;
        thread::yield_now();
    }
    for worker in workers {
        worker.join().unwrap();
    }
    let final_stats = governor.stats();
    assert_usage_totals_reconcile(final_stats);
    assert_eq!(final_stats.in_flight_bytes, 0);
    assert_eq!(final_stats.retained_bytes, 0);
    assert!(snapshots > 0);
    assert!(
        final_stats
            .usage
            .iter()
            .all(|entry| entry.in_flight_bytes == 0 && entry.retained_bytes == 0)
    );
}
