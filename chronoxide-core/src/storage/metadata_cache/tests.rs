use super::super::metadata_governor::MetadataGovernorConfig;
use super::*;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

fn empty_cache(retained: u64, in_flight: u64) -> MetadataCache {
    let governor = MetadataGovernor::new(MetadataGovernorConfig {
        retained_max_bytes: retained,
        in_flight_max_bytes: in_flight,
        max_open_files: 4,
        max_cached_open_files: 2,
    })
    .unwrap();
    MetadataCache::new(governor)
}

fn cache(retained: u64, in_flight: u64) -> MetadataCache {
    let cache = empty_cache(retained, in_flight);
    cache
        .register_artifact("seg-stable", SegmentFile::Series)
        .unwrap();
    cache
}

fn key(offset: u64) -> MetadataCacheKey {
    key_for(
        SegmentFile::Series,
        offset,
        MetadataCacheClass::SeriesHotPage,
    )
}

fn key_for(file: SegmentFile, offset: u64, class: MetadataCacheClass) -> MetadataCacheKey {
    MetadataCacheKey::new("seg-stable", file, offset, 16, class).unwrap()
}

fn ledger_bytes() -> u64 {
    corruption_ledger_charge_bytes("seg-stable").unwrap()
}

fn class_charge(cache: &MetadataCache, class: MetadataCacheClass) -> MetadataCacheClassStats {
    cache.stats().class_charges[class.stable_index()]
}

fn assert_current_class_charges_reconcile(cache: &MetadataCache) {
    let cache_stats = cache.stats();
    let governor_stats = cache.governor_stats();
    let class_in_flight = cache_stats
        .class_charges
        .iter()
        .map(|class| class.in_flight_bytes)
        .sum::<u64>();
    let class_retained = cache_stats
        .class_charges
        .iter()
        .map(|class| class.retained_bytes)
        .sum::<u64>();
    assert_eq!(
        class_in_flight + cache_stats.ledger_in_flight_bytes,
        governor_stats.in_flight_bytes
    );
    assert_eq!(
        class_retained + cache_stats.ledger_retained_bytes,
        governor_stats.retained_bytes
    );
    assert_eq!(
        cache_stats.ledger_in_flight_bytes + cache_stats.ledger_retained_bytes,
        cache_stats.ledger_reserved_bytes
    );
}

fn wait_for_single_flight_waiters(cache: &MetadataCache, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while cache.stats().single_flight_waits < expected {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} single-flight waiters; stats={:?}",
            cache.stats()
        );
        thread::yield_now();
    }
}

struct BlockingAllocationDropProbe {
    cache: MetadataCache,
    started: Arc<Barrier>,
    release: Arc<Barrier>,
    observed_registered_while_dropping: Arc<AtomicBool>,
}

impl Drop for BlockingAllocationDropProbe {
    fn drop(&mut self) {
        self.started.wait();
        self.release.wait();
        let stats = self.cache.stats();
        self.observed_registered_while_dropping.store(
            stats.registered_artifacts == 1
                && stats.ledger_reserved_bytes == ledger_bytes()
                && stats.live_allocations == 1,
            Ordering::SeqCst,
        );
    }
}

struct ResidentAllocationDropProbe {
    cache: MetadataCache,
    observed_release_order: Arc<AtomicBool>,
}

impl Drop for ResidentAllocationDropProbe {
    fn drop(&mut self) {
        let stats = self.cache.stats();
        let class = class_charge(&self.cache, MetadataCacheClass::SeriesHotPage);
        self.observed_release_order.store(
            stats.registered_artifacts == 1
                && stats.live_allocations == 1
                && class.retained_bytes == 8 + LIVE_REGISTRY_ENTRY_BYTES,
            Ordering::SeqCst,
        );
    }
}

struct FlightResultDropProbe {
    cache: MetadataCache,
    observed_release_order: Arc<AtomicBool>,
}

impl Drop for FlightResultDropProbe {
    fn drop(&mut self) {
        let stats = self.cache.stats();
        let class = class_charge(&self.cache, MetadataCacheClass::SeriesHotPage);
        self.observed_release_order.store(
            stats.registered_artifacts == 1
                && stats.ledger_reserved_bytes == ledger_bytes()
                && stats.active_loads == 1
                && class.in_flight_bytes == SINGLE_FLIGHT_ENTRY_BYTES,
            Ordering::SeqCst,
        );
    }
}

struct BatchResidentDropProbe {
    cache: MetadataCache,
    drops: Arc<AtomicUsize>,
}

impl Drop for BatchResidentDropProbe {
    fn drop(&mut self) {
        // This would deadlock if batch retirement destroyed a resident
        // allocation while holding the cache mutex.
        let _ = self.cache.stats();
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn key_rejects_mutable_payloads_and_invalid_ranges() {
    assert_eq!(
        MetadataCacheKey::new(
            "",
            SegmentFile::Series,
            0,
            1,
            MetadataCacheClass::SeriesRoot,
        ),
        Err(MetadataCacheKeyError::EmptySegmentIdentity)
    );
    assert_eq!(
        MetadataCacheKey::new(
            "seg",
            SegmentFile::Chunks,
            0,
            1,
            MetadataCacheClass::SeriesRoot,
        ),
        Err(MetadataCacheKeyError::UnsupportedFile {
            file: SegmentFile::Chunks,
        })
    );
    assert_eq!(
        MetadataCacheKey::new(
            "seg",
            SegmentFile::Series,
            0,
            0,
            MetadataCacheClass::SeriesRoot,
        ),
        Err(MetadataCacheKeyError::EmptyRange)
    );
    assert_eq!(
        MetadataCacheKey::new(
            "seg",
            SegmentFile::Series,
            u64::MAX,
            1,
            MetadataCacheClass::SeriesRoot,
        ),
        Err(MetadataCacheKeyError::RangeOverflow {
            offset: u64::MAX,
            length: 1,
        })
    );
}

#[test]
fn stable_artifact_keys_reuse_identity_allocation_and_match_owned_keys() {
    let identity = MetadataSegmentIdentity::new(Arc::from("seg-stable"));
    let artifact = ArtifactKey::new(identity, SegmentFile::Series);
    let stable = MetadataCacheKey::with_artifact(
        artifact.clone(),
        17,
        23,
        MetadataCacheClass::SeriesColdPage,
    )
    .unwrap();
    let owned = MetadataCacheKey::new(
        "seg-stable",
        SegmentFile::Series,
        17,
        23,
        MetadataCacheClass::SeriesColdPage,
    )
    .unwrap();

    assert!(Arc::ptr_eq(
        &stable.artifact.segment_identity.value,
        &artifact.segment_identity.value,
    ));
    assert_eq!(stable, owned);
    assert_eq!(stable.prehash, owned.prehash);
}

#[test]
fn prehash_collisions_cannot_alias_artifacts_or_typed_ranges() {
    let first_artifact = ArtifactKey::new(
        MetadataSegmentIdentity::new(Arc::from("seg-first")),
        SegmentFile::Series,
    );
    let mut second_artifact = ArtifactKey::new(
        MetadataSegmentIdentity::new(Arc::from("seg-second")),
        SegmentFile::Series,
    );
    second_artifact.prehash = first_artifact.prehash;
    let mut artifacts = HashMap::new();
    artifacts.insert(first_artifact.clone(), 1_u8);
    artifacts.insert(second_artifact.clone(), 2_u8);
    assert_eq!(artifacts.len(), 2);
    assert_eq!(artifacts.get(&first_artifact), Some(&1));
    assert_eq!(artifacts.get(&second_artifact), Some(&2));

    let cache = cache(4096, 4096);
    let first = key(0);
    let mut second = key(32);
    second.prehash = first.prehash;
    assert_ne!(first, second);

    drop(
        cache
            .get_or_load(first.clone(), 8, || Ok(LoadedMetadata::new(11_u64, 8)))
            .unwrap(),
    );
    drop(
        cache
            .get_or_load(second.clone(), 8, || Ok(LoadedMetadata::new(22_u64, 8)))
            .unwrap(),
    );
    let first_hit = cache
        .get_or_load::<u64, _>(first, 8, || {
            panic!("first collided key must remain resident")
        })
        .unwrap();
    let second_hit = cache
        .get_or_load::<u64, _>(second, 8, || {
            panic!("second collided key must remain resident")
        })
        .unwrap();
    assert_eq!(*first_hit, 11_u64);
    assert_eq!(*second_hit, 22_u64);
}

#[test]
fn class_snapshot_order_is_stable_and_complete() {
    let stats = MetadataCacheStats::default();
    assert_eq!(
        stats.class_charges.map(|entry| entry.class),
        METADATA_CACHE_CLASS_ORDER
    );
    assert_eq!(
        stats.class_admissions.map(|entry| entry.class),
        METADATA_CACHE_CLASS_ORDER
    );
    assert_eq!(
        METADATA_CACHE_CLASS_ORDER,
        [
            MetadataCacheClass::SymbolRoot,
            MetadataCacheClass::SymbolPage,
            MetadataCacheClass::IndexRoot,
            MetadataCacheClass::IndexDirectory,
            MetadataCacheClass::IndexPage,
            MetadataCacheClass::MetricRange,
            MetadataCacheClass::SeriesRoot,
            MetadataCacheClass::SeriesHotPage,
            MetadataCacheClass::SeriesColdPage,
            MetadataCacheClass::OverflowRoot,
            MetadataCacheClass::OverflowBlob,
            MetadataCacheClass::Postings,
            MetadataCacheClass::FullValidation,
        ]
    );
}

#[test]
fn loads_require_precharged_inventory_registration() {
    let governor = MetadataGovernor::new(MetadataGovernorConfig {
        retained_max_bytes: 4096,
        in_flight_max_bytes: 4096,
        max_open_files: 4,
        max_cached_open_files: 2,
    })
    .unwrap();
    let cache = MetadataCache::new(governor);
    let called = AtomicUsize::new(0);
    let error = cache
        .get_or_load::<u64, _>(key(0), 8, || {
            called.fetch_add(1, Ordering::SeqCst);
            Ok(LoadedMetadata::new(1, 8))
        })
        .unwrap_err();
    assert!(matches!(
        error,
        MetadataCacheError::UnregisteredArtifact { .. }
    ));
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
}

#[test]
fn registration_is_precharged_idempotent_and_refused_atomically() {
    let cache = cache(4096, 4096);
    let before = cache.governor_stats();
    assert_eq!(cache.stats().registered_artifacts, 1);
    assert_eq!(cache.stats().ledger_reserved_bytes, ledger_bytes());
    cache
        .register_artifact("seg-stable", SegmentFile::Series)
        .unwrap();
    assert_eq!(cache.governor_stats(), before);
    assert_eq!(cache.stats().registered_artifacts, 1);

    let governor = MetadataGovernor::new(MetadataGovernorConfig {
        retained_max_bytes: 0,
        in_flight_max_bytes: corruption_ledger_charge_bytes("seg").unwrap() - 1,
        max_open_files: 4,
        max_cached_open_files: 2,
    })
    .unwrap();
    let refused = MetadataCache::new(governor);
    assert!(matches!(
        refused.register_artifact("seg", SegmentFile::Series),
        Err(MetadataArtifactRegistrationError::Budget(_))
    ));
    assert_eq!(refused.stats().registered_artifacts, 0);
    assert_eq!(refused.governor_stats().in_flight_bytes, 0);
}

#[test]
fn batch_registration_validates_every_input_before_charging() {
    let cache = empty_cache(4096, 4096);
    assert_eq!(
        cache.register_artifacts("", &[SegmentFile::Series]),
        Err(MetadataArtifactRegistrationError::EmptySegmentIdentity)
    );
    assert_eq!(
        cache.register_artifacts("seg-stable", &[]),
        Err(MetadataArtifactRegistrationError::EmptyArtifactBatch)
    );
    assert_eq!(
        cache.register_artifacts("seg-stable", &[SegmentFile::Series, SegmentFile::Footer]),
        Err(MetadataArtifactRegistrationError::UnsupportedFile {
            file: SegmentFile::Footer,
        })
    );
    assert_eq!(
        cache.register_artifacts("seg-stable", &[SegmentFile::Series, SegmentFile::Series],),
        Err(MetadataArtifactRegistrationError::DuplicateFile {
            file: SegmentFile::Series,
        })
    );
    assert_eq!(
        cache.register_artifacts("seg-stable", &[SegmentFile::Series, SegmentFile::Symbols],),
        Err(MetadataArtifactRegistrationError::NonCanonicalOrder {
            previous: SegmentFile::Series,
            file: SegmentFile::Symbols,
        })
    );
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn batch_registration_rolls_back_every_charge_on_late_budget_failure() {
    let charge = ledger_bytes();
    let cache = empty_cache(0, charge * 2 - 1);
    assert!(matches!(
        cache.register_artifacts(
            "seg-stable",
            &[SegmentFile::Series, SegmentFile::ChunkIndex],
        ),
        Err(MetadataArtifactRegistrationError::Budget(_))
    ));
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn batch_registration_is_exactly_idempotent_and_rejects_partial_inventory() {
    let files = [
        SegmentFile::Symbols,
        SegmentFile::Series,
        SegmentFile::ChunkIndex,
    ];
    let cache = empty_cache(8192, 8192);
    cache.register_artifacts("seg-stable", &files).unwrap();
    let before = cache.governor_stats();
    cache.register_artifacts("seg-stable", &files).unwrap();
    assert_eq!(cache.governor_stats(), before);
    assert_eq!(cache.stats().registered_artifacts, files.len() as u64);

    let partial = empty_cache(4096, 4096);
    partial
        .register_artifact("seg-stable", SegmentFile::Symbols)
        .unwrap();
    assert_eq!(
        partial.register_artifacts("seg-stable", &[SegmentFile::Symbols, SegmentFile::Series]),
        Err(MetadataArtifactRegistrationError::PartialInventory {
            segment_identity: Arc::from("seg-stable"),
            registered: 1,
            requested: 2,
        })
    );
    assert_eq!(partial.stats().registered_artifacts, 1);
    assert_eq!(
        partial.check_artifact("seg-stable", SegmentFile::Symbols),
        Ok(())
    );
    assert!(matches!(
        partial.check_artifact("seg-stable", SegmentFile::Series),
        Err(MetadataCacheError::UnregisteredArtifact { .. })
    ));
}

#[test]
fn batch_registration_rejects_mixed_and_all_retiring_inventory() {
    let files = [SegmentFile::Series, SegmentFile::ChunkIndex];
    let mixed = empty_cache(8192, 8192);
    mixed.register_artifacts("seg-stable", &files).unwrap();
    let series_pin = mixed
        .get_or_load(key(0), 8, || Ok(LoadedMetadata::new(1_u64, 8)))
        .unwrap();
    assert_eq!(
        mixed.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Deferred
    );
    assert_eq!(
        mixed.register_artifacts("seg-stable", &files),
        Err(MetadataArtifactRegistrationError::PartialInventory {
            segment_identity: Arc::from("seg-stable"),
            registered: 2,
            requested: 2,
        })
    );
    assert_eq!(
        mixed.check_artifact("seg-stable", SegmentFile::ChunkIndex),
        Ok(())
    );
    drop(series_pin);

    let partially_removed = empty_cache(8192, 8192);
    partially_removed
        .register_artifacts("seg-stable", &files)
        .unwrap();
    let series_pin = partially_removed
        .get_or_load(key(0), 8, || Ok(LoadedMetadata::new(1_u64, 8)))
        .unwrap();
    assert_eq!(
        partially_removed
            .retire_artifacts_after_inventory_removal("seg-stable", &files)
            .unwrap(),
        MetadataArtifactRetirement::Deferred
    );
    assert_eq!(partially_removed.stats().registered_artifacts, 1);
    assert_eq!(
        partially_removed.register_artifacts("seg-stable", &files),
        Err(MetadataArtifactRegistrationError::Retiring {
            segment_identity: Arc::from("seg-stable"),
            file: SegmentFile::Series,
        })
    );
    drop(series_pin);

    let retiring = empty_cache(8192, 8192);
    retiring.register_artifacts("seg-stable", &files).unwrap();
    let series_pin = retiring
        .get_or_load(key(0), 8, || Ok(LoadedMetadata::new(1_u64, 8)))
        .unwrap();
    let index_pin = retiring
        .get_or_load(
            key_for(SegmentFile::ChunkIndex, 0, MetadataCacheClass::IndexPage),
            8,
            || Ok(LoadedMetadata::new(2_u64, 8)),
        )
        .unwrap();
    assert_eq!(
        retiring
            .retire_artifacts_after_inventory_removal("seg-stable", &files)
            .unwrap(),
        MetadataArtifactRetirement::Deferred
    );
    assert_eq!(
        retiring.register_artifacts("seg-stable", &files),
        Err(MetadataArtifactRegistrationError::Retiring {
            segment_identity: Arc::from("seg-stable"),
            file: SegmentFile::Series,
        })
    );
    drop((series_pin, index_pin));
    assert_eq!(retiring.stats().registered_artifacts, 0);
}

#[test]
fn zero_retained_budget_never_creates_residency() {
    let cache = cache(0, 4096);
    let loads = AtomicUsize::new(0);
    let first = cache
        .get_or_load(key(0), 64, || {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(LoadedMetadata::new(vec![7_u8; 32], 32))
        })
        .unwrap();
    let stats = cache.stats();
    assert_eq!(stats.resident_entries, 0);
    assert_eq!(stats.successful_loads, 1);
    assert_eq!(stats.resident_admissions, 0);
    assert_eq!(stats.resident_admission_refusals, 0);
    assert_eq!(stats.resident_admission_bypasses, 1);
    let admissions = stats.class_admissions[MetadataCacheClass::SeriesHotPage.stable_index()];
    assert_eq!(admissions.resident_admissions, 0);
    assert_eq!(admissions.resident_admission_refusals, 0);
    assert_eq!(admissions.resident_admission_bypasses, 1);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
    assert_current_class_charges_reconcile(&cache);

    let second = cache
        .get_or_load(key(0), 64, || -> Result<LoadedMetadata<Vec<u8>>, _> {
            panic!("live allocation must be reused")
        })
        .unwrap();
    assert!(MetadataCachePin::ptr_eq(&first, &second));
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    let stats = cache.stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.resident_admission_bypasses, 1);
    drop((first, second));
    assert_eq!(cache.governor_stats().in_flight_bytes, ledger_bytes());
    assert_eq!(cache.stats().live_allocations, 0);
    let class = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(class.in_flight_bytes, 0);
    assert_eq!(class.retained_bytes, 0);
    assert_current_class_charges_reconcile(&cache);
}

#[test]
fn eviction_preserves_live_identity_and_does_not_double_charge() {
    let cache = cache(4096, 4096);
    let first = cache
        .get_or_load(key(0), 64, || Ok(LoadedMetadata::new([1_u8; 32], 32)))
        .unwrap();
    let before = cache.governor_stats().retained_bytes;
    let admitted = cache.stats();
    assert_eq!(admitted.resident_entries, 1);
    assert_eq!(admitted.resident_admissions, 1);
    assert_eq!(admitted.resident_admission_refusals, 0);
    assert_eq!(admitted.resident_admission_bypasses, 0);
    let resident = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(resident.in_flight_bytes, 0);
    assert_eq!(
        resident.retained_bytes,
        32 + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
    );
    assert_current_class_charges_reconcile(&cache);

    cache.evict_all_resident();
    let evicted = cache.stats();
    assert_eq!(evicted.resident_entries, 0);
    assert_eq!(evicted.resident_admissions, 1);
    assert_eq!(
        cache.governor_stats().retained_bytes,
        before - RESIDENT_ENTRY_BYTES
    );
    let pinned = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(pinned.in_flight_bytes, 0);
    assert_eq!(pinned.retained_bytes, 32 + LIVE_REGISTRY_ENTRY_BYTES);
    assert_current_class_charges_reconcile(&cache);
    let reused = cache
        .get_or_load(key(0), 64, || -> Result<LoadedMetadata<[u8; 32]>, _> {
            panic!("evicted but pinned allocation must not reload")
        })
        .unwrap();
    assert!(MetadataCachePin::ptr_eq(&first, &reused));
    assert_eq!(cache.stats().resident_admissions, 1);
    assert_eq!(
        cache.governor_stats().retained_bytes,
        before - RESIDENT_ENTRY_BYTES
    );
    drop((first, reused));
    assert_eq!(cache.governor_stats().retained_bytes, ledger_bytes());
    let dropped = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(dropped.in_flight_bytes, 0);
    assert_eq!(dropped.retained_bytes, 0);
    assert_current_class_charges_reconcile(&cache);
}

#[test]
fn aggregate_lru_evicts_oldest_unpinned_value_to_admit_next() {
    let cache = cache(700, 4096);
    cache
        .get_or_load(key(0), 200, || {
            Ok(LoadedMetadata::new(vec![0_u8; 200], 200))
        })
        .unwrap();
    assert_eq!(cache.stats().resident_entries, 1);
    cache
        .get_or_load(key(32), 200, || {
            Ok(LoadedMetadata::new(vec![1_u8; 200], 200))
        })
        .unwrap();
    assert_eq!(cache.stats().resident_entries, 1);
    assert_eq!(cache.stats().evictions, 1);
    assert!(cache.governor_stats().retained_bytes <= 700);
}

#[test]
fn resident_hit_promotes_entry_without_duplicate_evictions() {
    const VALUE_BYTES: u64 = 8;
    let retained_per_entry = VALUE_BYTES + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES;
    let cache = cache(retained_per_entry * 2, 4096);

    drop(
        cache
            .get_or_load(key(0), VALUE_BYTES, || {
                Ok(LoadedMetadata::new(0_u64, VALUE_BYTES))
            })
            .unwrap(),
    );
    drop(
        cache
            .get_or_load(key(32), VALUE_BYTES, || {
                Ok(LoadedMetadata::new(32_u64, VALUE_BYTES))
            })
            .unwrap(),
    );
    assert_eq!(cache.stats().resident_entries, 2);

    for _ in 0..64 {
        drop(
            cache
                .get_or_load(
                    key(0),
                    VALUE_BYTES,
                    || -> Result<LoadedMetadata<u64>, MetadataCacheError> {
                        panic!("resident entry must not reload")
                    },
                )
                .unwrap(),
        );
    }

    drop(
        cache
            .get_or_load(key(64), VALUE_BYTES, || {
                Ok(LoadedMetadata::new(64_u64, VALUE_BYTES))
            })
            .unwrap(),
    );
    assert_eq!(cache.stats().resident_entries, 2);
    assert_eq!(cache.stats().evictions, 1);

    drop(
        cache
            .get_or_load(
                key(0),
                VALUE_BYTES,
                || -> Result<LoadedMetadata<u64>, MetadataCacheError> {
                    panic!("recently used entry must remain resident")
                },
            )
            .unwrap(),
    );

    let evicted_loads = AtomicUsize::new(0);
    drop(
        cache
            .get_or_load(key(32), VALUE_BYTES, || {
                evicted_loads.fetch_add(1, Ordering::SeqCst);
                Ok(LoadedMetadata::new(32_u64, VALUE_BYTES))
            })
            .unwrap(),
    );
    assert_eq!(evicted_loads.load(Ordering::SeqCst), 1);
    assert_eq!(cache.stats().resident_entries, 2);
    assert_eq!(cache.stats().evictions, 2);
    assert_current_class_charges_reconcile(&cache);
}

#[test]
fn concurrent_identical_misses_are_single_flight() {
    const THREADS: usize = 12;
    // This admits inventory precharge at open and, after it is promoted,
    // has room for one flight plus its value but not one flight candidate
    // per waiter.
    let cache = cache(
        4096,
        SINGLE_FLIGHT_ENTRY_BYTES + 64 + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES,
    );
    let start = Arc::new(Barrier::new(THREADS));
    let release_loader = Arc::new(Barrier::new(2));
    let loader_started = Arc::new(Barrier::new(2));
    let loads = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..THREADS {
        let cache = cache.clone();
        let start = Arc::clone(&start);
        let release_loader = Arc::clone(&release_loader);
        let loader_started = Arc::clone(&loader_started);
        let loads = Arc::clone(&loads);
        workers.push(thread::spawn(move || {
            start.wait();
            cache
                .get_or_load(key(0), 64, || {
                    if loads.fetch_add(1, Ordering::SeqCst) == 0 {
                        loader_started.wait();
                        release_loader.wait();
                    }
                    Ok(LoadedMetadata::new(99_u64, 8))
                })
                .unwrap()
        }));
    }
    loader_started.wait();
    wait_for_single_flight_waiters(&cache, THREADS as u64 - 1);
    assert_eq!(
        cache.governor_stats().in_flight_bytes,
        SINGLE_FLIGHT_ENTRY_BYTES + 64
    );
    let loading = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(loading.in_flight_bytes, SINGLE_FLIGHT_ENTRY_BYTES + 64);
    assert_eq!(loading.retained_bytes, 0);
    assert_current_class_charges_reconcile(&cache);
    release_loader.wait();
    let pins: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert!(
        pins.iter()
            .all(|pin| MetadataCachePin::ptr_eq(&pins[0], pin))
    );
    let stats = cache.stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.single_flight_waits, THREADS as u64 - 1);
    assert_eq!(stats.successful_loads, 1);
    assert_eq!(stats.resident_admissions, 1);
    assert_eq!(stats.resident_admission_refusals, 0);
    assert_eq!(stats.resident_admission_bypasses, 0);
    let promoted = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(promoted.in_flight_bytes, 0);
    assert_eq!(
        promoted.retained_bytes,
        8 + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
    );
    assert_eq!(
        promoted.peak_in_flight_bytes,
        SINGLE_FLIGHT_ENTRY_BYTES + 64
    );
    assert_eq!(
        promoted.peak_retained_bytes,
        8 + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
    );
    assert_current_class_charges_reconcile(&cache);
}

#[test]
fn allocation_failure_rolls_back_and_transient_error_is_retryable() {
    let cache = cache(4096, 4096);
    let error = cache
        .get_or_load::<Vec<u8>, _>(key(0), 256, || {
            Err(MetadataCacheError::transient(
                io::ErrorKind::OutOfMemory,
                "allocation failed",
            ))
        })
        .unwrap_err();
    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::OutOfMemory,
            ..
        }
    ));
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
    let failed = cache.stats();
    assert_eq!(failed.active_loads, 0);
    assert_eq!(failed.resident_admissions, 0);
    assert_eq!(failed.resident_admission_refusals, 0);
    assert_eq!(failed.resident_admission_bypasses, 0);

    let retry = cache
        .get_or_load(key(0), 16, || Ok(LoadedMetadata::new(7_u64, 8)))
        .unwrap();
    assert_eq!(*retry, 7);
    let retried = cache.stats();
    assert_eq!(retried.misses, 2);
    assert_eq!(retried.resident_admissions, 1);
}

#[test]
fn optional_resident_admission_refusal_preserves_transient_value() {
    let cache = cache(ledger_bytes() + 63, 4096);
    let pin = cache
        .get_or_load(key(0), 64, || Ok(LoadedMetadata::new([7_u8; 64], 64)))
        .unwrap();

    let stats = cache.stats();
    assert_eq!(stats.resident_entries, 0);
    assert_eq!(stats.successful_loads, 1);
    assert_eq!(stats.resident_admissions, 0);
    assert_eq!(stats.resident_admission_refusals, 1);
    assert_eq!(stats.resident_admission_bypasses, 0);
    let admissions = stats.class_admissions[MetadataCacheClass::SeriesHotPage.stable_index()];
    assert_eq!(admissions.resident_admissions, 0);
    assert_eq!(admissions.resident_admission_refusals, 1);
    assert_eq!(admissions.resident_admission_bypasses, 0);
    assert_eq!(cache.governor_stats().retained_refusals, 1);
    assert_eq!(
        cache.governor_stats().in_flight_bytes,
        64 + LIVE_REGISTRY_ENTRY_BYTES
    );
    let transient = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(transient.in_flight_bytes, 64 + LIVE_REGISTRY_ENTRY_BYTES);
    assert_eq!(transient.retained_bytes, 0);
    assert_eq!(transient.peak_retained_bytes, 0);
    assert_current_class_charges_reconcile(&cache);
    drop(pin);
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
    assert_eq!(cache.governor_stats().retained_bytes, ledger_bytes());
    let dropped = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(dropped.in_flight_bytes, 0);
    assert_eq!(dropped.retained_bytes, 0);
    assert_current_class_charges_reconcile(&cache);
}

#[test]
fn resident_refusal_is_counted_when_transient_fallback_also_fails() {
    let cache = cache(ledger_bytes(), SINGLE_FLIGHT_ENTRY_BYTES + 64);
    let error = cache
        .get_or_load(key(0), 64, || Ok(LoadedMetadata::new([7_u8; 64], 64)))
        .unwrap_err();
    assert!(matches!(error, MetadataCacheError::Budget(_)));

    let stats = cache.stats();
    assert_eq!(stats.successful_loads, 0);
    assert_eq!(stats.failed_loads, 1);
    assert_eq!(stats.resident_admissions, 0);
    assert_eq!(stats.resident_admission_refusals, 1);
    assert_eq!(stats.resident_admission_bypasses, 0);
    let class = stats.class_admissions[MetadataCacheClass::SeriesHotPage.stable_index()];
    assert_eq!(class.resident_admissions, 0);
    assert_eq!(class.resident_admission_refusals, 1);
    assert_eq!(class.resident_admission_bypasses, 0);
    assert_eq!(cache.governor_stats().retained_refusals, 1);
    assert_eq!(cache.governor_stats().in_flight_refusals, 1);
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
}

#[test]
fn resident_admission_counters_saturate_globally_and_per_class() {
    let cache = empty_cache(4096, 4096);
    let class = MetadataCacheClass::IndexPage;
    let index = class.stable_index();
    {
        let mut state = lock(&cache.inner.state);
        state.stats.resident_admissions = u64::MAX;
        state.stats.resident_admission_refusals = u64::MAX;
        state.stats.resident_admission_bypasses = u64::MAX;
        state.stats.class_admissions[index].admissions = u64::MAX;
        state.stats.class_admissions[index].refusals = u64::MAX;
        state.stats.class_admissions[index].bypasses = u64::MAX;
        state
            .stats
            .record_resident_admission(class, ResidentAdmissionOutcome::Admitted);
        state
            .stats
            .record_resident_admission(class, ResidentAdmissionOutcome::Refused);
        state
            .stats
            .record_resident_admission(class, ResidentAdmissionOutcome::Bypassed);
    }

    let stats = cache.stats();
    assert_eq!(stats.resident_admissions, u64::MAX);
    assert_eq!(stats.resident_admission_refusals, u64::MAX);
    assert_eq!(stats.resident_admission_bypasses, u64::MAX);
    let class = stats.class_admissions[index];
    assert_eq!(class.resident_admissions, u64::MAX);
    assert_eq!(class.resident_admission_refusals, u64::MAX);
    assert_eq!(class.resident_admission_bypasses, u64::MAX);
}

#[test]
fn transient_failure_is_shared_before_a_later_retry() {
    const THREADS: usize = 6;
    let declared_error_bytes = 4096;
    let cache = cache(4096, SINGLE_FLIGHT_ENTRY_BYTES + declared_error_bytes);
    let start = Arc::new(Barrier::new(THREADS));
    let loader_started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let loads = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..THREADS {
        let cache = cache.clone();
        let start = Arc::clone(&start);
        let loader_started = Arc::clone(&loader_started);
        let release = Arc::clone(&release);
        let loads = Arc::clone(&loads);
        workers.push(thread::spawn(move || {
            start.wait();
            cache
                .get_or_load::<u64, _>(key(0), declared_error_bytes, || {
                    if loads.fetch_add(1, Ordering::SeqCst) == 0 {
                        loader_started.wait();
                        release.wait();
                    }
                    Err(MetadataCacheError::Transient {
                        kind: io::ErrorKind::Interrupted,
                        message: Arc::from("é".repeat(MAX_TRANSIENT_MESSAGE_BYTES)),
                    })
                })
                .unwrap_err()
        }));
    }
    loader_started.wait();
    wait_for_single_flight_waiters(&cache, THREADS as u64 - 1);
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(
        cache.governor_stats().in_flight_bytes,
        SINGLE_FLIGHT_ENTRY_BYTES + declared_error_bytes
    );
    let loading = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(
        loading.in_flight_bytes,
        SINGLE_FLIGHT_ENTRY_BYTES + declared_error_bytes
    );
    assert_eq!(loading.retained_bytes, 0);
    assert_current_class_charges_reconcile(&cache);
    release.wait();
    let errors: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert!(errors.iter().all(|error| error == &errors[0]));
    let MetadataCacheError::Transient { message, .. } = &errors[0] else {
        panic!("expected shared transient error")
    };
    assert_eq!(message.len(), MAX_TRANSIENT_MESSAGE_BYTES);
    assert!(message.is_char_boundary(message.len()));
    assert_eq!(cache.stats().misses, 1);
    assert_eq!(cache.stats().sticky_artifacts, 0);
    let failed = class_charge(&cache, MetadataCacheClass::SeriesHotPage);
    assert_eq!(failed.in_flight_bytes, 0);
    assert_eq!(failed.retained_bytes, 0);
    assert_eq!(
        failed.peak_in_flight_bytes,
        SINGLE_FLIGHT_ENTRY_BYTES + declared_error_bytes
    );
    assert_current_class_charges_reconcile(&cache);

    let retry = cache
        .get_or_load(key(0), 8, || {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(LoadedMetadata::new(42_u64, 8))
        })
        .unwrap();
    assert_eq!(*retry, 42);
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    assert_eq!(cache.stats().misses, 2);
}

#[test]
fn declared_bound_violation_releases_every_reservation() {
    let cache = cache(4096, 4096);
    let error = cache
        .get_or_load(key(0), 8, || Ok(LoadedMetadata::new(vec![0_u8; 9], 9)))
        .unwrap_err();
    assert_eq!(
        error,
        MetadataCacheError::DeclaredBoundExceeded {
            declared_bytes: 8,
            actual_bytes: 9,
        }
    );
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
    assert_eq!(cache.governor_stats().retained_bytes, ledger_bytes());
}

#[test]
fn sticky_corruption_survives_eviction_and_blocks_other_ranges() {
    let cache = cache(4096, 4096);
    let first = cache
        .get_or_load::<Vec<u8>, _>(key(0), 64, || {
            Err(MetadataCacheError::from_io(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad page crc",
            )))
        })
        .unwrap_err();
    cache.evict_all_resident();

    let called = AtomicUsize::new(0);
    let second = cache
        .get_or_load::<Vec<u8>, _>(key(32), 64, || {
            called.fetch_add(1, Ordering::SeqCst);
            Ok(LoadedMetadata::new(vec![1], 1))
        })
        .unwrap_err();
    assert_eq!(first, second);
    assert_eq!(called.load(Ordering::SeqCst), 0);
    let stats = cache.stats();
    assert_eq!(stats.corruption_detections, 1);
    assert_eq!(stats.corruption_hits, 1);
    assert_eq!(stats.sticky_artifacts, 1);
    assert_eq!(stats.sticky_charged_bytes, ledger_bytes());
    let charged_before_retirement = cache.governor_stats().retained_bytes;
    cache.evict_all_resident();
    assert_eq!(
        cache.governor_stats().retained_bytes,
        charged_before_retirement
    );
    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Removed
    );
    assert_eq!(cache.stats().sticky_artifacts, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn non_cacheable_chunk_replacement_is_sticky_and_first_error_wins() {
    let cache = cache(4096, 4096);
    cache
        .register_artifact("seg-stable", SegmentFile::Chunks)
        .unwrap();
    assert!(matches!(
        cache.register_artifact("seg-stable", SegmentFile::Footer),
        Err(MetadataArtifactRegistrationError::UnsupportedFile {
            file: SegmentFile::Footer,
        })
    ));

    let first = cache.record_artifact_error(
        "seg-stable",
        SegmentFile::Chunks,
        MetadataCacheError::structural(
            StructuralMetadataErrorKind::InvalidData,
            "file identity replacement",
        ),
    );
    let second = cache.record_artifact_error(
        "seg-stable",
        SegmentFile::Chunks,
        MetadataCacheError::structural(
            StructuralMetadataErrorKind::UnexpectedEof,
            "later short read",
        ),
    );
    assert_eq!(second, first);

    // The ledger is artifact-owned, so neither decoded-metadata eviction
    // nor closing/evicting an FD can touch this state.
    cache.evict_all_resident();
    assert_eq!(
        cache.check_artifact("seg-stable", SegmentFile::Chunks),
        Err(first)
    );
    assert_eq!(cache.stats().corruption_detections, 1);
    assert_eq!(cache.stats().corruption_hits, 1);
    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Chunks),
        MetadataArtifactRetirement::Removed
    );
    assert!(matches!(
        cache.check_artifact("seg-stable", SegmentFile::Chunks),
        Err(MetadataCacheError::UnregisteredArtifact { .. })
    ));
}

#[test]
fn corruption_retirement_waits_for_live_pins() {
    let cache = cache(4096, 4096);
    let pin = cache
        .get_or_load(key(0), 16, || Ok(LoadedMetadata::new(7_u64, 8)))
        .unwrap();
    cache
        .get_or_load::<u64, _>(key(32), 16, || {
            Err(MetadataCacheError::from_io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short page",
            )))
        })
        .unwrap_err();

    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Deferred
    );
    cache.evict_all_resident();
    assert_eq!(cache.stats().sticky_artifacts, 1);
    drop(pin);
    assert_eq!(cache.stats().sticky_artifacts, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn batch_retirement_marks_nothing_when_any_member_is_not_registered() {
    let cache = empty_cache(4096, 4096);
    cache
        .register_artifact("seg-stable", SegmentFile::Series)
        .unwrap();
    assert_eq!(
        cache
            .retire_artifacts_after_inventory_removal(
                "seg-stable",
                &[SegmentFile::Series, SegmentFile::ChunkIndex],
            )
            .unwrap(),
        MetadataArtifactRetirement::NotRegistered
    );
    assert_eq!(
        cache.check_artifact("seg-stable", SegmentFile::Series),
        Ok(())
    );
    assert_eq!(cache.stats().registered_artifacts, 1);
    assert_eq!(cache.stats().ledger_reserved_bytes, ledger_bytes());
}

#[test]
fn batch_retirement_detaches_every_resident_and_ledger_outside_cache_lock() {
    let files = [SegmentFile::Series, SegmentFile::ChunkIndex];
    let cache = empty_cache(8192, 8192);
    cache.register_artifacts("seg-stable", &files).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let series = cache
        .get_or_load(key(0), 8, || {
            Ok(LoadedMetadata::new(
                BatchResidentDropProbe {
                    cache: cache.clone(),
                    drops: Arc::clone(&drops),
                },
                8,
            ))
        })
        .unwrap();
    let index = cache
        .get_or_load(
            key_for(SegmentFile::ChunkIndex, 0, MetadataCacheClass::IndexPage),
            8,
            || {
                Ok(LoadedMetadata::new(
                    BatchResidentDropProbe {
                        cache: cache.clone(),
                        drops: Arc::clone(&drops),
                    },
                    8,
                ))
            },
        )
        .unwrap();
    drop((series, index));
    assert_eq!(cache.stats().resident_entries, 2);

    assert_eq!(
        cache
            .retire_artifacts_after_inventory_removal("seg-stable", &files)
            .unwrap(),
        MetadataArtifactRetirement::Removed
    );
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(cache.stats().resident_entries, 0);
    assert_eq!(cache.stats().live_allocations, 0);
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn batch_retirement_is_deferred_until_every_member_is_quiescent() {
    let files = [SegmentFile::Series, SegmentFile::ChunkIndex];
    let cache = empty_cache(8192, 8192);
    cache.register_artifacts("seg-stable", &files).unwrap();
    let pin = cache
        .get_or_load(key(0), 8, || Ok(LoadedMetadata::new(1_u64, 8)))
        .unwrap();

    assert_eq!(
        cache
            .retire_artifacts_after_inventory_removal("seg-stable", &files)
            .unwrap(),
        MetadataArtifactRetirement::Deferred
    );
    assert!(matches!(
        cache.check_artifact("seg-stable", SegmentFile::Series),
        Err(MetadataCacheError::RetiringArtifact { .. })
    ));
    assert!(matches!(
        cache.check_artifact("seg-stable", SegmentFile::ChunkIndex),
        Err(MetadataCacheError::UnregisteredArtifact { .. })
    ));
    assert_eq!(cache.stats().registered_artifacts, 1);
    assert_eq!(cache.stats().ledger_reserved_bytes, ledger_bytes());

    drop(pin);
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn retirement_waits_for_allocation_teardown_after_weak_entry_is_reaped() {
    let in_flight = ledger_bytes() + SINGLE_FLIGHT_ENTRY_BYTES + 8 + LIVE_REGISTRY_ENTRY_BYTES;
    let cache = cache(0, in_flight);
    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let observed_registered_while_dropping = Arc::new(AtomicBool::new(false));
    let pin = cache
        .get_or_load(key(0), 8, || {
            Ok(LoadedMetadata::new(
                BlockingAllocationDropProbe {
                    cache: cache.clone(),
                    started: Arc::clone(&started),
                    release: Arc::clone(&release),
                    observed_registered_while_dropping: Arc::clone(
                        &observed_registered_while_dropping,
                    ),
                },
                8,
            ))
        })
        .unwrap();

    let dropper = thread::spawn(move || drop(pin));
    started.wait();

    // The final Arc is already running its destructor, so this lookup
    // cannot upgrade the weak live-registry entry and reaps it. The exact
    // budget permits the new flight but refuses its value reservation.
    let loader_called = AtomicBool::new(false);
    let retry = cache.get_or_load::<BlockingAllocationDropProbe, _>(key(0), 8, || {
        loader_called.store(true, Ordering::SeqCst);
        Err(MetadataCacheError::transient(
            io::ErrorKind::Other,
            "loader must remain behind its reservation",
        ))
    });
    let Err(retry) = retry else {
        panic!("racing allocation unexpectedly loaded")
    };
    assert!(matches!(retry, MetadataCacheError::Budget(_)));
    assert!(!loader_called.load(Ordering::SeqCst));
    assert_eq!(cache.stats().live_allocations, 1);
    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Deferred
    );
    assert_eq!(cache.stats().registered_artifacts, 1);

    release.wait();
    dropper.join().unwrap();
    assert!(observed_registered_while_dropping.load(Ordering::SeqCst));
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.stats().live_allocations, 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
}

#[test]
fn resident_charge_drops_before_final_allocation_can_retire_ledger() {
    let cache = cache(4096, 4096);
    let observed_release_order = Arc::new(AtomicBool::new(false));
    let pin = cache
        .get_or_load(key(0), 8, || {
            Ok(LoadedMetadata::new(
                ResidentAllocationDropProbe {
                    cache: cache.clone(),
                    observed_release_order: Arc::clone(&observed_release_order),
                },
                8,
            ))
        })
        .unwrap();
    drop(pin);
    assert_eq!(cache.stats().resident_entries, 1);

    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Removed
    );
    assert!(observed_release_order.load(Ordering::SeqCst));
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn flight_result_and_charge_drop_before_flight_can_retire_ledger() {
    let cache = cache(0, ledger_bytes() + SINGLE_FLIGHT_ENTRY_BYTES);
    let artifact = key(0).artifact_key();
    let observed_release_order = Arc::new(AtomicBool::new(false));
    let result: ErasedAllocation = Arc::new(FlightResultDropProbe {
        cache: cache.clone(),
        observed_release_order: Arc::clone(&observed_release_order),
    });
    let flight_charge = cache
        .governor()
        .reserve_in_flight_for_usage(
            SINGLE_FLIGHT_ENTRY_BYTES,
            MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage),
        )
        .unwrap();
    let flight = Arc::new(Flight {
        result: Mutex::new(Some(Ok(result))),
        completed: Condvar::new(),
        bookkeeping_charge: Some(flight_charge),
        owner: Arc::downgrade(&cache.inner),
        artifact: artifact.clone(),
        inventory_tracked: AtomicBool::new(true),
    });
    lock(&cache.inner.state)
        .active_flights_by_artifact
        .insert(artifact, 1);

    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Deferred
    );
    drop(flight);

    assert!(observed_release_order.load(Ordering::SeqCst));
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.stats().active_loads, 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
    assert_eq!(cache.governor_stats().in_flight_bytes, 0);
}

#[test]
fn healthy_resident_is_detached_when_inventory_retires() {
    let cache = cache(4096, 4096);
    let pin = cache
        .get_or_load(key(0), 16, || Ok(LoadedMetadata::new(7_u64, 8)))
        .unwrap();
    drop(pin);
    assert_eq!(cache.stats().resident_entries, 1);
    assert_eq!(cache.stats().live_allocations, 1);

    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Removed
    );
    assert_eq!(cache.stats().resident_entries, 0);
    assert_eq!(cache.stats().live_allocations, 0);
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn sticky_corruption_wins_while_artifact_is_retiring() {
    let cache = cache(4096, 4096);
    let pin = cache
        .get_or_load(key(0), 16, || Ok(LoadedMetadata::new(7_u64, 8)))
        .unwrap();
    let first = cache.record_artifact_error(
        "seg-stable",
        SegmentFile::Series,
        MetadataCacheError::structural(StructuralMetadataErrorKind::InvalidData, "bad series page"),
    );

    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Deferred
    );
    assert_eq!(
        cache.check_artifact("seg-stable", SegmentFile::Series),
        Err(first)
    );
    drop(pin);
    assert_eq!(cache.stats().registered_artifacts, 0);
}

#[test]
fn inventory_retirement_waits_for_flight_completion() {
    let cache = cache(4096, 4096);
    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let worker = {
        let cache = cache.clone();
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        thread::spawn(move || {
            cache.get_or_load(key(0), 8, || {
                started.wait();
                release.wait();
                Ok(LoadedMetadata::new(7_u64, 8))
            })
        })
    };
    started.wait();
    assert_eq!(
        cache.retire_artifact_after_inventory_removal("seg-stable", SegmentFile::Series),
        MetadataArtifactRetirement::Deferred
    );
    release.wait();
    assert!(matches!(
        worker.join().unwrap(),
        Err(MetadataCacheError::RetiringArtifact { .. })
    ));
    assert_eq!(cache.stats().registered_artifacts, 0);
    assert_eq!(cache.stats().ledger_reserved_bytes, 0);
    assert_eq!(cache.governor_stats().retained_bytes, 0);
}

#[test]
fn concurrent_waiters_receive_the_same_structural_error() {
    const THREADS: usize = 8;
    let cache = cache(4096, SINGLE_FLIGHT_ENTRY_BYTES + 8);
    let start = Arc::new(Barrier::new(THREADS));
    let loader_started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let loads = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for _ in 0..THREADS {
        let cache = cache.clone();
        let start = Arc::clone(&start);
        let loader_started = Arc::clone(&loader_started);
        let release = Arc::clone(&release);
        let loads = Arc::clone(&loads);
        workers.push(thread::spawn(move || {
            start.wait();
            cache
                .get_or_load::<u64, _>(key(0), 8, || {
                    if loads.fetch_add(1, Ordering::SeqCst) == 0 {
                        loader_started.wait();
                        release.wait();
                    }
                    Err(MetadataCacheError::from_io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short metadata page",
                    )))
                })
                .unwrap_err()
        }));
    }
    loader_started.wait();
    wait_for_single_flight_waiters(&cache, THREADS as u64 - 1);
    release.wait();
    let errors: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert!(errors.iter().all(|error| error == &errors[0]));
    assert_eq!(cache.stats().single_flight_waits, THREADS as u64 - 1);
    assert_eq!(cache.stats().corruption_detections, 1);
}
