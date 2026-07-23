use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use super::*;
use crate::storage::metadata_cache::{
    LIVE_REGISTRY_ENTRY_BYTES, MetadataArtifactRegistrationError, MetadataCacheClass,
    MetadataCorruption, RESIDENT_ENTRY_BYTES,
};
use crate::storage::metadata_governor::MetadataGovernorConfig;
use crate::storage::segment::{SEGMENT_FOOTER_TRACKED_FILES, SegmentFile};
use crate::util::xxhash64;

fn config(
    retained_max_bytes: u64,
    in_flight_max_bytes: u64,
    max_open_files: u32,
    max_cached_open_files: u32,
) -> MetadataGovernorConfig {
    MetadataGovernorConfig {
        retained_max_bytes,
        in_flight_max_bytes,
        max_open_files,
        max_cached_open_files,
    }
}

fn write_inventory(
    directory: &TempDir,
    identity: &str,
    selected: Option<(SegmentFile, &[u8])>,
) -> Vec<SegmentArtifactRegistration> {
    SEGMENT_FOOTER_TRACKED_FILES
        .into_iter()
        .map(|candidate| {
            let path = directory
                .path()
                .join(format!("{identity}-{}", candidate.filename()));
            let contents = selected
                .filter(|(file, _)| *file == candidate)
                .map_or(b"fixture".as_slice(), |(_, bytes)| bytes);
            fs::write(&path, contents).expect("write canonical metadata fixture");
            SegmentArtifactRegistration::new(
                candidate,
                path,
                u64::try_from(contents.len()).expect("fixture length fits u64"),
            )
        })
        .collect()
}

fn fixture(
    directory: &TempDir,
    runtime: &StoreMetadataRuntime,
    identity: &str,
    file: SegmentFile,
    bytes: &[u8],
) -> GovernedArtifactReader {
    let inventory = write_inventory(directory, identity, Some((file, bytes)));
    let registered = runtime
        .register_segment(identity, &inventory)
        .expect("register canonical metadata fixture");
    let reader = registered.reader(file).expect("create governed reader");
    drop(registered);
    reader
}

#[test]
fn generation_provenance_rejects_same_identity_after_reregistration() {
    let directory = TempDir::new().expect("create provenance temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let inventory = write_inventory(&directory, "provenance-generation", None);
    let first = runtime
        .register_segment("provenance-generation", &inventory)
        .expect("register first generation");
    let first_guard = first.read_guard().expect("read first generation");
    let provenance = first_guard.provenance();
    assert!(provenance.matches(&first_guard));
    let first_generation = first_guard.generation();
    drop(first_guard);
    drop(first);
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);

    let second = runtime
        .register_segment("provenance-generation", &inventory)
        .expect("register second generation");
    let second_guard = second.read_guard().expect("read second generation");
    assert_ne!(second_guard.generation(), first_generation);
    assert!(!provenance.matches(&second_guard));
}

fn key(reader: &GovernedArtifactReader, offset: u64, length: u64) -> MetadataCacheKey {
    reader
        .metadata_cache_key(offset, length, MetadataCacheClass::SeriesHotPage)
        .expect("valid fixture key")
}

fn replace_same_length(reader: &GovernedArtifactReader, replacement: &[u8]) {
    assert_eq!(
        usize::try_from(reader.handle().expected_len()).expect("fixture length fits usize"),
        replacement.len()
    );
    let backup = reader.handle().path().with_extension("original");
    fs::rename(reader.handle().path(), backup).expect("retain original inode");
    fs::write(reader.handle().path(), replacement).expect("write replacement inode");
}

fn assert_no_live_io(runtime: &StoreMetadataRuntime) {
    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.files.active_leases, 0);
    assert_eq!(snapshot.files.active_open_files, 0);
    assert_eq!(snapshot.files.opening_files, 0);
    assert_eq!(snapshot.files.pending_open_files, 0);
    assert_eq!(snapshot.cache.active_loads, 0);
}

fn file_reads(stats: MetadataReadStats, file: SegmentFile) -> MetadataIssuedReadCount {
    stats
        .files
        .into_iter()
        .find(|entry| entry.file == file)
        .expect("tracked file has read counters")
        .issued
}

#[test]
fn runtime_shares_one_governor_cache_and_file_manager() {
    let runtime =
        StoreMetadataRuntime::new(config(16 * 1024, 16 * 1024, 1, 1)).expect("valid runtime");
    let clone = runtime.clone();
    assert!(Arc::ptr_eq(&runtime.governor(), &clone.governor()));
    assert!(Arc::ptr_eq(
        runtime.cache().governor(),
        clone.cache().governor()
    ));
    assert!(Arc::ptr_eq(&runtime.file_manager(), &clone.file_manager()));
    assert_eq!(runtime.snapshot(), clone.snapshot());
}

#[test]
fn canonical_inventory_is_validated_before_any_preflight_or_cache_publication() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let inventory = write_inventory(&directory, "invalid-inventory", None);

    assert!(matches!(
        runtime.register_segment("invalid-inventory", &inventory[..6]),
        Err(StoreMetadataRuntimeError::InvalidArtifactCount {
            expected: 7,
            actual: 6,
        })
    ));
    let mut reordered = inventory.clone();
    reordered.swap(0, 1);
    assert!(matches!(
        runtime.register_segment("invalid-inventory", &reordered),
        Err(StoreMetadataRuntimeError::NonCanonicalArtifact { index: 0, .. })
    ));
    assert!(matches!(
        runtime.register_segment("", &inventory),
        Err(StoreMetadataRuntimeError::EmptySegmentIdentity)
    ));

    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.files.preflight_calls, 0);
    assert_eq!(snapshot.cache.registered_artifacts, 0);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
}

#[test]
fn concurrent_same_definition_registration_preflights_once_at_fd_cap_one() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime = Arc::new(
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime"),
    );
    let inventory = Arc::new(write_inventory(&directory, "registration-join", None));
    let holders_ready = Arc::new(Barrier::new(7));
    let release_holders = Arc::new(Barrier::new(7));
    let mut workers = Vec::new();
    for _ in 0..6 {
        let runtime = Arc::clone(&runtime);
        let inventory = Arc::clone(&inventory);
        let holders_ready = Arc::clone(&holders_ready);
        let release_holders = Arc::clone(&release_holders);
        workers.push(thread::spawn(move || {
            let registered = runtime
                .register_segment("registration-join", &inventory)
                .expect("join canonical registration");
            let generation = registered.generation();
            holders_ready.wait();
            release_holders.wait();
            generation
        }));
    }
    holders_ready.wait();

    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.files.preflight_calls, 7);
    assert_eq!(snapshot.files.successful_preflights, 7);
    assert_eq!(snapshot.files.peak_occupied_open_slots, 1);
    assert_eq!(snapshot.files.open_files, 0);
    assert_eq!(snapshot.cache.registered_artifacts, 7);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));

    release_holders.wait();
    let generations = workers
        .into_iter()
        .map(|worker| worker.join().expect("registration worker joins"))
        .collect::<Vec<_>>();
    assert!(generations.iter().all(|generation| *generation == 1));
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
}

#[test]
fn waiting_registration_reserves_owner_before_publication() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime = Arc::new(
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime"),
    );
    let inventory = Arc::new(write_inventory(&directory, "reserved-join", None));
    let leader_entered = Arc::new(Barrier::new(2));
    let resume_leader = Arc::new(Barrier::new(2));
    runtime.install_registration_leader_pause_for_test(
        Arc::clone(&leader_entered),
        Arc::clone(&resume_leader),
        false,
    );

    let (leader_sender, leader_receiver) = mpsc::sync_channel(1);
    let leader_runtime = Arc::clone(&runtime);
    let leader_inventory = Arc::clone(&inventory);
    let leader = thread::spawn(move || {
        let registered = leader_runtime
            .register_segment("reserved-join", &leader_inventory)
            .expect("leader publishes registration");
        leader_sender
            .send(registered)
            .expect("send published leader owner");
    });
    leader_entered.wait();

    let joiner_entered = Arc::new(Barrier::new(2));
    let resume_joiner = Arc::new(Barrier::new(2));
    runtime.install_registration_join_wake_pause_for_test(
        Arc::clone(&joiner_entered),
        Arc::clone(&resume_joiner),
        false,
    );
    let (joiner_sender, joiner_receiver) = mpsc::sync_channel(1);
    let joiner_runtime = Arc::clone(&runtime);
    let joiner_inventory = Arc::clone(&inventory);
    let joiner = thread::spawn(move || {
        let registered = joiner_runtime
            .register_segment("reserved-join", &joiner_inventory)
            .expect("waiting caller joins published registration");
        joiner_sender.send(registered).expect("send joined owner");
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.pending_registration_for_test("reserved-join") != Some((1, 2)) {
        assert!(
            Instant::now() < deadline,
            "joining caller did not reserve an ownership slot"
        );
        thread::yield_now();
    }
    resume_leader.wait();
    let leader_owner = leader_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("leader registration returns");
    joiner_entered.wait();

    let generation = leader_owner.generation();
    drop(leader_owner);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
    assert_eq!(runtime.snapshot().files.preflight_calls, 7);

    resume_joiner.wait();
    let joiner_owner = joiner_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("reserved joiner returns");
    assert_eq!(joiner_owner.generation(), generation);
    assert_eq!(runtime.snapshot().files.preflight_calls, 7);
    leader.join().expect("leader thread joins");
    joiner.join().expect("joiner thread joins");

    drop(joiner_owner);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
}

#[test]
fn unwind_after_cache_registration_rolls_back_the_whole_transaction() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime = Arc::new(
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime"),
    );
    let inventory = Arc::new(write_inventory(&directory, "registration-unwind", None));
    let after_cache = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    runtime.install_registration_after_cache_pause_for_test(
        Arc::clone(&after_cache),
        Arc::clone(&resume),
        true,
    );

    let worker_runtime = Arc::clone(&runtime);
    let worker_inventory = Arc::clone(&inventory);
    let worker = thread::spawn(move || {
        catch_unwind(AssertUnwindSafe(|| {
            let _ = worker_runtime.register_segment("registration-unwind", &worker_inventory);
        }))
        .is_err()
    });
    after_cache.wait();
    assert_eq!(
        runtime.pending_registration_for_test("registration-unwind"),
        Some((1, 1))
    );
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
    resume.wait();
    assert!(worker.join().expect("unwind worker joins"));

    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    assert_no_live_io(&runtime);
    assert_eq!(runtime.snapshot().files.open_files, 0);

    let retry = runtime
        .register_segment("registration-unwind", &inventory)
        .expect("retry after complete unwind rollback");
    assert_eq!(retry.generation(), 2);
    assert_eq!(runtime.snapshot().files.preflight_calls, 14);
    drop(retry);
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
}

#[test]
fn active_registration_joins_exact_definition_and_rejects_conflict() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let inventory = write_inventory(&directory, "active-join", None);
    let first = runtime
        .register_segment("active-join", &inventory)
        .expect("publish first owner");
    let second = runtime
        .register_segment("active-join", &inventory)
        .expect("join active owner");
    assert_eq!(first.generation(), second.generation());
    assert_eq!(runtime.snapshot().files.preflight_calls, 7);

    let mut conflicting = inventory.clone();
    conflicting[2] = SegmentArtifactRegistration::new(
        SegmentFile::Series,
        directory.path().join("different-series.bin"),
        conflicting[2].footer_recorded_len(),
    );
    assert!(matches!(
        runtime.register_segment("active-join", &conflicting),
        Err(StoreMetadataRuntimeError::ConflictingRegistration { .. })
    ));
    assert_eq!(runtime.snapshot().files.preflight_calls, 7);

    drop(first);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));
    drop(second);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
}

#[test]
fn failed_preflight_rolls_back_and_same_identity_can_retry() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let inventory = write_inventory(&directory, "registration-retry", None);
    let mut malformed = inventory.clone();
    malformed[2] = SegmentArtifactRegistration::new(
        SegmentFile::Series,
        malformed[2].path(),
        malformed[2].footer_recorded_len() + 1,
    );
    assert!(matches!(
        runtime.register_segment("registration-retry", &malformed),
        Err(StoreMetadataRuntimeError::FileManager(
            MetadataFileManagerError::StructuralReplacement { .. }
        ))
    ));
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);

    let registered = runtime
        .register_segment("registration-retry", &inventory)
        .expect("retry corrected inventory");
    assert_eq!(registered.generation(), 2);
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
    drop(registered);
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
}

#[test]
fn final_owner_waits_for_reader_clones_before_atomic_retirement() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let inventory = write_inventory(&directory, "guarded-retirement", None);
    let registered = runtime
        .register_segment("guarded-retirement", &inventory)
        .expect("register guarded segment");
    let owner_clone = registered.clone();
    let guard = registered.read_guard().expect("create read guard");
    let reader = guard
        .reader(SegmentFile::Series)
        .expect("create guard-bound reader");

    drop(registered);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));
    drop(owner_clone);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 1, 0, 0));
    assert!(matches!(
        runtime.register_segment("guarded-retirement", &inventory),
        Err(StoreMetadataRuntimeError::SegmentRetiring { .. })
    ));

    drop(guard);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 1, 0, 0));
    drop(reader);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);

    let next = runtime
        .register_segment("guarded-retirement", &inventory)
        .expect("register next generation after complete retirement");
    assert!(next.generation() > 1);
    drop(next);
}

#[test]
fn deferred_cache_pin_blocks_same_identity_until_final_pin_drop() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let inventory = write_inventory(
        &directory,
        "deferred-pin",
        Some((SegmentFile::Series, b"value")),
    );
    let registered = runtime
        .register_segment("deferred-pin", &inventory)
        .expect("register pinned segment");
    let reader = registered
        .reader(SegmentFile::Series)
        .expect("create pinned reader");
    let pin = reader
        .get_or_load(key(&reader, 0, 5), 5, |bytes| {
            Ok(LoadedMetadata::new(bytes.to_vec(), 5))
        })
        .expect("load pinned metadata");

    drop(registered);
    drop(reader);
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 1);
    assert!(matches!(
        runtime.register_segment("deferred-pin", &inventory),
        Err(StoreMetadataRuntimeError::Cache(
            MetadataArtifactRegistrationError::Retiring { .. }
        ))
    ));
    assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));

    drop(pin);
    assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    let next = runtime
        .register_segment("deferred-pin", &inventory)
        .expect("register after final pin removes cache tombstone");
    drop(next);
}

#[test]
fn validated_cache_hit_reuses_value_without_another_fd_acquisition() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 1)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "cache-reuse",
        SegmentFile::Series,
        b"metadata",
    );
    let loads = AtomicUsize::new(0);

    let first = reader
        .get_or_load(key(&reader, 0, 8), 8, |bytes| {
            loads.fetch_add(1, Ordering::SeqCst);
            Ok(LoadedMetadata::new(bytes.to_vec(), 8))
        })
        .expect("load metadata");
    let second = reader
        .get_or_load(key(&reader, 0, 8), 8, |_| {
            panic!("cache hit must not invoke loader")
        })
        .expect("reuse metadata");
    assert!(MetadataCachePin::ptr_eq(&first, &second));
    assert_eq!(&**second, b"metadata");
    assert_eq!(loads.load(Ordering::SeqCst), 1);

    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.cache.hits, 1);
    assert_eq!(snapshot.files.acquire_calls, 1);
    assert_eq!(snapshot.files.preflight_calls, 7);
    assert_eq!(snapshot.files.successful_preflights, 7);
    assert_eq!(snapshot.files.descriptor_opens, 8);
    assert_eq!(snapshot.files.descriptor_closes, 7);
    assert_eq!(snapshot.files.cached_open_files, 1);
    assert_no_live_io(&runtime);

    drop(first);
    drop(second);
    runtime.cache().evict_all_resident();
    assert_eq!(runtime.snapshot().cache.live_allocations, 0);
}

#[test]
fn issued_read_stats_attribute_exact_spans_and_cache_hits_issue_nothing() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "issued-read-stats",
        SegmentFile::Series,
        b"abcdefgh",
    );
    let before = runtime.snapshot().reads;

    let mut unclassified = [0_u8; 1];
    reader
        .read_exact_at(0, &mut unclassified)
        .expect("read unclassified byte");
    assert_eq!(unclassified, [b'a']);
    let mut staged_root = [0_u8; 2];
    reader
        .read_exact_at_for_class(1, &mut staged_root, MetadataCacheClass::SeriesRoot)
        .expect("read staged root prefix");
    assert_eq!(staged_root, *b"bc");

    let root_key = MetadataCacheKey::new(
        reader.segment_identity(),
        SegmentFile::Series,
        1,
        5,
        MetadataCacheClass::SeriesRoot,
    )
    .expect("valid staged root key");
    let first = reader
        .get_or_load_with_prefix(root_key.clone(), 5, &staged_root, |bytes| {
            Ok(LoadedMetadata::new(bytes.to_vec(), 5))
        })
        .expect("load staged root without rereading its prefix");
    let second = reader
        .get_or_load_with_prefix(root_key, 5, &staged_root, |_| {
            panic!("warm cache hit must not issue another range")
        })
        .expect("reuse staged root");
    assert!(MetadataCachePin::ptr_eq(&first, &second));
    assert_eq!(&**first, b"bcdef");

    let delta = runtime.snapshot().reads.delta_since(before);
    assert_eq!(delta.issued, MetadataIssuedReadCount { calls: 3, bytes: 6 });
    assert_eq!(
        delta.unclassified,
        MetadataIssuedReadCount { calls: 1, bytes: 1 }
    );
    let series = delta
        .files
        .iter()
        .find(|stats| stats.file == SegmentFile::Series)
        .expect("series file stats");
    assert_eq!(
        series.issued,
        MetadataIssuedReadCount { calls: 3, bytes: 6 }
    );
    assert_eq!(
        delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
        MetadataIssuedReadCount { calls: 2, bytes: 5 }
    );
    assert_eq!(
        runtime.inner.reads.take_spans(),
        vec![
            MetadataReadSpan {
                file: SegmentFile::Series,
                class: None,
                offset: 0,
                length: 1,
            },
            MetadataReadSpan {
                file: SegmentFile::Series,
                class: Some(MetadataCacheClass::SeriesRoot),
                offset: 1,
                length: 2,
            },
            MetadataReadSpan {
                file: SegmentFile::Series,
                class: Some(MetadataCacheClass::SeriesRoot),
                offset: 3,
                length: 3,
            },
        ]
    );
}

#[test]
fn bootstrap_validation_error_is_sticky_without_an_issued_read() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "bootstrap-corruption",
        SegmentFile::Series,
        b"metadata",
    );
    let before = runtime.snapshot().reads;

    let first = reader.record_validation_error(io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid staged series header",
    ));
    assert!(matches!(first, MetadataCacheError::Structural(_)));
    let mut byte = [0_u8; 1];
    let second = reader
        .read_exact_at_for_class(0, &mut byte, MetadataCacheClass::SeriesRoot)
        .expect_err("sticky header corruption gates later reads");
    assert_eq!(first, second);
    assert_eq!(runtime.snapshot().reads.delta_since(before).issued.calls, 0);
    assert_eq!(runtime.snapshot().cache.corruption_detections, 1);
}

#[test]
fn staged_prefix_larger_than_key_is_transient_without_io_or_poisoning() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "oversized-prefix",
        SegmentFile::Series,
        b"abc",
    );
    let before = runtime.snapshot();

    let error = reader
        .get_or_load_with_prefix::<u8, _>(key(&reader, 0, 1), 1, b"ab", |_| {
            panic!("invalid prefix must not invoke validator")
        })
        .expect_err("oversized staged prefix is rejected");
    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::InvalidInput,
            ..
        }
    ));
    let after = runtime.snapshot();
    assert_eq!(after.reads.delta_since(before.reads).issued.calls, 0);
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
    assert_eq!(after.cache.sticky_artifacts, 0);
    assert_eq!(after.cache.active_loads, 0);
}

#[test]
fn overflowing_read_range_is_rejected_before_fd_acquisition_or_accounting() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "overflowing-read",
        SegmentFile::Series,
        b"abc",
    );
    let before = runtime.snapshot();
    let mut bytes = [0_u8; 2];

    let error = reader
        .read_exact_at_for_class(u64::MAX, &mut bytes, MetadataCacheClass::SeriesRoot)
        .expect_err("overflowing range is caller input, not issued I/O");
    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::InvalidInput,
            ..
        }
    ));
    let after = runtime.snapshot();
    assert_eq!(after.reads.delta_since(before.reads).issued.calls, 0);
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
    assert_eq!(after.cache.sticky_artifacts, 0);
}

#[test]
fn staged_prefix_covering_full_key_publishes_without_suffix_io() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "complete-prefix",
        SegmentFile::Series,
        b"abc",
    );
    let before = runtime.snapshot().reads;
    let mut prefix = [0_u8; 3];
    reader
        .read_exact_at_for_class(0, &mut prefix, MetadataCacheClass::SeriesRoot)
        .expect("read complete staged range");

    let root_key = MetadataCacheKey::new(
        reader.segment_identity(),
        SegmentFile::Series,
        0,
        3,
        MetadataCacheClass::SeriesRoot,
    )
    .expect("valid complete root key");
    let pin = reader
        .get_or_load_with_prefix(root_key, 3, &prefix, |bytes| {
            Ok(LoadedMetadata::new(bytes.to_vec(), 3))
        })
        .expect("publish fully seeded value");
    assert_eq!(&**pin, b"abc");

    let delta = runtime.snapshot().reads.delta_since(before);
    assert_eq!(delta.issued, MetadataIssuedReadCount { calls: 1, bytes: 3 });
    assert_eq!(
        delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
        MetadataIssuedReadCount { calls: 1, bytes: 3 }
    );
}

#[test]
fn staged_suffix_short_read_is_sticky_and_releases_all_load_resources() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "short-staged-suffix",
        SegmentFile::Series,
        b"abc",
    );
    let before = runtime.snapshot().reads;
    let mut prefix = [0_u8; 1];
    reader
        .read_exact_at_for_class(1, &mut prefix, MetadataCacheClass::SeriesRoot)
        .expect("read valid staged prefix");
    assert_eq!(prefix, [b'b']);
    let root_key = MetadataCacheKey::new(
        reader.segment_identity(),
        SegmentFile::Series,
        1,
        3,
        MetadataCacheClass::SeriesRoot,
    )
    .expect("valid key extending past EOF");

    let error = reader
        .get_or_load_with_prefix::<u8, _>(root_key, 1, &prefix, |_| {
            panic!("short suffix must not reach validation")
        })
        .expect_err("short staged suffix is corruption");
    assert!(matches!(error, MetadataCacheError::Structural(_)));
    let after = runtime.snapshot();
    let delta = after.reads.delta_since(before);
    assert_eq!(delta.issued, MetadataIssuedReadCount { calls: 2, bytes: 3 });
    assert_eq!(after.cache.successful_loads, 0);
    assert_eq!(after.cache.failed_loads, 1);
    assert_eq!(after.cache.sticky_artifacts, 1);
    assert_eq!(after.cache.active_loads, 0);
    assert_eq!(after.cache.live_allocations, 0);
    assert_eq!(
        after
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        0
    );
    assert_no_live_io(&runtime);

    let mut retry = [0_u8; 1];
    reader
        .read_exact_at_for_class(1, &mut retry, MetadataCacheClass::SeriesRoot)
        .expect_err("sticky suffix failure gates retry before I/O");
    assert_eq!(runtime.snapshot().reads, after.reads);
}

#[test]
fn validated_scratch_handoff_installs_one_retained_cache_charge_set() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "retained-handoff",
        SegmentFile::Series,
        b"x",
    );

    let pin = reader
        .get_or_load(key(&reader, 0, 1), 1, |bytes| {
            Ok(LoadedMetadata::new(bytes[0], 1))
        })
        .expect("retained metadata load");
    assert_eq!(*pin, b'x');

    let snapshot = runtime.snapshot();
    let scratch = snapshot.governor.usage(MetadataUsageClass::Scratch);
    assert_eq!(scratch.in_flight_bytes, 0);
    assert_eq!(scratch.retained_bytes, 0);
    let class = snapshot.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
    assert_eq!(class.in_flight_bytes, 0);
    assert_eq!(
        class.retained_bytes,
        1 + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
    );
    assert_eq!(snapshot.cache.resident_entries, 1);
    assert_eq!(snapshot.cache.live_allocations, 1);
    assert_no_live_io(&runtime);

    drop(pin);
    runtime.cache().evict_all_resident();
    let released = runtime.snapshot();
    let class = released.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
    assert_eq!(class.in_flight_bytes, 0);
    assert_eq!(class.retained_bytes, 0);
}

#[test]
fn validated_scratch_handoff_keeps_zero_retention_load_transient() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime = StoreMetadataRuntime::new(config(0, 64 * 1024, 1, 0)).expect("valid runtime");
    let bytes = vec![9_u8; 256];
    let reader = fixture(
        &directory,
        &runtime,
        "transient-handoff",
        SegmentFile::Series,
        &bytes,
    );

    let pin = reader
        .get_or_load(key(&reader, 0, 256), 1, |bytes| {
            Ok(LoadedMetadata::new(bytes[0], 1))
        })
        .expect("transient metadata load");
    assert_eq!(*pin, 9);

    let snapshot = runtime.snapshot();
    let scratch = snapshot.governor.usage(MetadataUsageClass::Scratch);
    assert_eq!(scratch.in_flight_bytes, 0);
    assert_eq!(scratch.retained_bytes, 0);
    let class = snapshot.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
    assert_eq!(class.in_flight_bytes, 1 + LIVE_REGISTRY_ENTRY_BYTES);
    assert_eq!(class.retained_bytes, 0);
    assert_eq!(snapshot.cache.resident_entries, 0);
    assert_eq!(snapshot.cache.live_allocations, 1);
    assert_no_live_io(&runtime);

    drop(pin);
    let released = runtime.snapshot();
    let class = released.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
    assert_eq!(class.in_flight_bytes, 0);
    assert_eq!(class.retained_bytes, 0);
}

#[test]
fn registered_hash_matches_empty_seed_zero_digest_and_still_checks_identity() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(&directory, &runtime, "hash-empty", SegmentFile::Chunks, b"");
    let before = runtime.snapshot();
    let mut scratch = [0_u8; 17];

    let actual = reader
        .hash_registered_xxh64(&mut scratch)
        .expect("hash empty registered artifact");

    assert_eq!(actual, xxhash64(b""));
    let after = runtime.snapshot();
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
    assert_eq!(after.reads.delta_since(before.reads).issued.calls, 0);
    assert_eq!(after.files.peak_occupied_open_slots, 1);
    assert_eq!(after.files.open_files, 0);
    assert_no_live_io(&runtime);
}

#[test]
fn registered_hash_streams_small_artifact_under_one_lease_and_classifies_reads() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let bytes = b"registered-artifact";
    let reader = fixture(
        &directory,
        &runtime,
        "hash-small",
        SegmentFile::Indexes,
        bytes,
    );
    let _ = runtime.inner.reads.take_spans();
    let before = runtime.snapshot();
    let mut scratch = [0_u8; 3];

    let actual = reader
        .hash_registered_xxh64(&mut scratch)
        .expect("hash small registered artifact");

    assert_eq!(actual, xxhash64(bytes));
    let after = runtime.snapshot();
    let delta = after.reads.delta_since(before.reads);
    let expected_calls =
        u64::try_from(bytes.len().div_ceil(scratch.len())).expect("fixture call count fits u64");
    let expected_bytes = u64::try_from(bytes.len()).expect("fixture length fits u64");
    let expected = MetadataIssuedReadCount {
        calls: expected_calls,
        bytes: expected_bytes,
    };
    assert_eq!(delta.issued, expected);
    assert_eq!(delta.unclassified, MetadataIssuedReadCount::default());
    assert_eq!(file_reads(delta, SegmentFile::Indexes), expected);
    assert_eq!(
        delta.classes[MetadataCacheClass::FullValidation.stable_index()].issued,
        expected
    );
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
    assert_eq!(after.files.open_files, 0);
    assert_no_live_io(&runtime);
}

#[test]
fn registered_hash_streams_more_than_one_mib_with_caller_owned_governed_scratch() {
    const HASH_BUFFER_BYTES: usize = 1024 * 1024;

    let directory = TempDir::new().expect("create temp directory");
    let hash_buffer_bytes_u64 =
        u64::try_from(HASH_BUFFER_BYTES).expect("hash buffer length fits u64");
    let runtime = StoreMetadataRuntime::new(config(64 * 1024, 2 * hash_buffer_bytes_u64, 1, 0))
        .expect("valid runtime");
    let bytes = (0..HASH_BUFFER_BYTES + 257)
        .map(|index| index.to_le_bytes()[0].wrapping_mul(37).wrapping_add(11))
        .collect::<Vec<_>>();
    let reader = fixture(
        &directory,
        &runtime,
        "hash-large",
        SegmentFile::Chunks,
        &bytes,
    );

    let governor = runtime.governor();
    let mut charge = governor
        .reserve_in_flight_for_usage(hash_buffer_bytes_u64, MetadataUsageClass::Scratch)
        .expect("reserve caller-owned hash scratch");
    let mut scratch = Vec::new();
    scratch
        .try_reserve_exact(HASH_BUFFER_BYTES)
        .expect("allocate hash scratch");
    charge
        .reconcile(u64::try_from(scratch.capacity()).expect("capacity fits u64"))
        .expect("reconcile hash scratch charge");
    scratch.resize(HASH_BUFFER_BYTES, 0);

    let _ = runtime.inner.reads.take_spans();
    let before = runtime.snapshot();
    let scratch_before = before.governor.usage(MetadataUsageClass::Scratch);
    let actual = reader
        .hash_registered_xxh64(&mut scratch)
        .expect("hash large registered artifact");

    assert_eq!(actual, xxhash64(&bytes));
    let after = runtime.snapshot();
    assert_eq!(
        after.governor.usage(MetadataUsageClass::Scratch),
        scratch_before,
        "the hash primitive must not acquire, transfer, or release the caller's charge"
    );
    let expected = MetadataIssuedReadCount {
        calls: 2,
        bytes: u64::try_from(bytes.len()).expect("fixture length fits u64"),
    };
    let delta = after.reads.delta_since(before.reads);
    assert_eq!(delta.issued, expected);
    assert_eq!(file_reads(delta, SegmentFile::Chunks), expected);
    assert_eq!(
        delta.classes[MetadataCacheClass::FullValidation.stable_index()].issued,
        expected
    );
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
    assert_eq!(after.files.max_open_files, 1);
    assert_eq!(after.files.peak_occupied_open_slots, 1);
    assert_eq!(after.files.peak_open_files, 1);
    assert_eq!(after.files.open_files, 0);
    assert_eq!(
        runtime.inner.reads.take_spans(),
        vec![
            MetadataReadSpan {
                file: SegmentFile::Chunks,
                class: Some(MetadataCacheClass::FullValidation),
                offset: 0,
                length: hash_buffer_bytes_u64,
            },
            MetadataReadSpan {
                file: SegmentFile::Chunks,
                class: Some(MetadataCacheClass::FullValidation),
                offset: hash_buffer_bytes_u64,
                length: 257,
            },
        ]
    );
    assert_no_live_io(&runtime);

    drop(scratch);
    drop(charge);
    assert_eq!(
        runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        0
    );
}

#[test]
fn registered_hash_rejects_empty_scratch_without_fd_io_or_poisoning() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "hash-no-scratch",
        SegmentFile::Series,
        b"value",
    );
    let before = runtime.snapshot();

    let error = reader
        .hash_registered_xxh64(&mut [])
        .expect_err("empty hash scratch must be rejected");

    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::InvalidInput,
            ..
        }
    ));
    let after = runtime.snapshot();
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
    assert_eq!(after.reads, before.reads);
    assert_eq!(after.cache.sticky_artifacts, 0);
    assert_no_live_io(&runtime);
}

#[test]
fn registered_hash_replacement_after_idle_eviction_is_sticky_before_reads() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 1)).expect("valid runtime");
    let inventory = write_inventory(
        &directory,
        "hash-replacement",
        Some((SegmentFile::Series, b"original")),
    );
    let registered = runtime
        .register_segment("hash-replacement", &inventory)
        .expect("register hash replacement fixture");
    let series = registered
        .reader(SegmentFile::Series)
        .expect("create series reader");
    let symbols = registered
        .reader(SegmentFile::Symbols)
        .expect("create symbols reader");
    let mut scratch = [0_u8; 3];

    series
        .hash_registered_xxh64(&mut scratch)
        .expect("hash original registered series");
    let before_eviction = runtime.snapshot().files.idle_evictions;
    symbols
        .hash_registered_xxh64(&mut scratch)
        .expect("evict original series descriptor");
    assert!(runtime.snapshot().files.idle_evictions > before_eviction);
    replace_same_length(&series, b"replaced");

    let before_failure = runtime.snapshot();
    let first = series
        .hash_registered_xxh64(&mut scratch)
        .expect_err("same-length replacement must fail identity validation");
    assert!(matches!(
        first,
        MetadataCacheError::Structural(MetadataCorruption {
            kind: StructuralMetadataErrorKind::InvalidData,
            ..
        })
    ));
    let after_failure = runtime.snapshot();
    assert_eq!(
        after_failure
            .reads
            .delta_since(before_failure.reads)
            .issued
            .calls,
        0,
        "replacement is rejected before a positional read"
    );
    assert_eq!(after_failure.files.open_files, 0);
    assert_no_live_io(&runtime);

    let acquire_calls = after_failure.files.acquire_calls;
    let second = series
        .hash_registered_xxh64(&mut scratch)
        .expect_err("sticky replacement must gate retry");
    assert_eq!(second, first);
    assert_eq!(runtime.snapshot().files.acquire_calls, acquire_calls);
    assert_eq!(runtime.snapshot().reads, after_failure.reads);
    assert_eq!(runtime.snapshot().cache.corruption_detections, 1);
}

#[test]
fn registered_hash_short_read_is_sticky_after_the_lease_is_released() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "hash-short-read",
        SegmentFile::Series,
        b"abcdefgh",
    );
    let path = reader.handle().path().to_path_buf();
    let mut scratch = [0_u8; 4];
    let mut truncated = false;
    let before = runtime.snapshot();

    let first = reader
        .hash_registered_xxh64_with_hook(&mut scratch, |offset| {
            if !truncated {
                fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .expect("open registered inode for truncation")
                    .set_len(offset)
                    .expect("truncate registered inode after first hash read");
                truncated = true;
            }
        })
        .expect_err("truncation during one lease must produce a short read");
    assert!(truncated);
    assert!(matches!(
        first,
        MetadataCacheError::Structural(MetadataCorruption {
            kind: StructuralMetadataErrorKind::UnexpectedEof,
            ..
        })
    ));
    let after = runtime.snapshot();
    let expected = MetadataIssuedReadCount { calls: 2, bytes: 8 };
    let delta = after.reads.delta_since(before.reads);
    assert_eq!(delta.issued, expected);
    assert_eq!(
        delta.classes[MetadataCacheClass::FullValidation.stable_index()].issued,
        expected
    );
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
    assert_eq!(after.files.open_files, 0);
    assert_no_live_io(&runtime);

    let acquire_calls = after.files.acquire_calls;
    let second = reader
        .hash_registered_xxh64(&mut scratch)
        .expect_err("sticky short read must gate retry before reacquisition");
    assert_eq!(second, first);
    assert_eq!(runtime.snapshot().files.acquire_calls, acquire_calls);
    assert_eq!(runtime.snapshot().reads, after.reads);
}

#[test]
fn registered_hash_rejects_an_append_after_the_last_read() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "hash-concurrent-append",
        SegmentFile::Series,
        b"abcdefgh",
    );
    let path = reader.handle().path().to_path_buf();
    let mut scratch = [0_u8; 8];
    let mut appended = false;

    let error = reader
        .hash_registered_xxh64_with_hook(&mut scratch, |offset| {
            if !appended {
                fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .expect("open registered inode for append")
                    .set_len(offset + 1)
                    .expect("append after final registered hash read");
                appended = true;
            }
        })
        .expect_err("post-hash shape check must reject an append");

    assert!(appended);
    assert!(matches!(
        error,
        MetadataCacheError::Structural(MetadataCorruption {
            kind: StructuralMetadataErrorKind::InvalidData,
            ..
        })
    ));
    assert_eq!(runtime.snapshot().reads.issued.calls, 1);
    assert_eq!(runtime.snapshot().files.open_files, 0);
    assert_no_live_io(&runtime);
}

#[test]
fn registered_hash_checks_existing_sticky_error_before_scratch_or_fd_io() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "hash-sticky-gate",
        SegmentFile::Indexes,
        b"metadata",
    );
    let recorded = reader.record_validation_error(io::Error::new(
        io::ErrorKind::InvalidData,
        "known registered-artifact corruption",
    ));
    let before = runtime.snapshot();

    let returned = reader
        .hash_registered_xxh64(&mut [])
        .expect_err("existing corruption wins over invalid scratch");

    assert_eq!(returned, recorded);
    let after = runtime.snapshot();
    assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
    assert_eq!(after.reads, before.reads);
    assert_no_live_io(&runtime);
}

#[test]
fn replacement_after_fd_eviction_becomes_sticky() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "replacement",
        SegmentFile::ChunkIndex,
        b"original",
    );
    let mut bytes = [0_u8; 8];
    reader
        .read_exact_at(0, &mut bytes)
        .expect("initial exact read");
    assert_eq!(&bytes, b"original");
    replace_same_length(&reader, b"replaced");

    let first = reader
        .read_exact_at(0, &mut bytes)
        .expect_err("replacement must fail");
    assert!(matches!(
        first,
        MetadataCacheError::Structural(MetadataCorruption {
            kind: StructuralMetadataErrorKind::InvalidData,
            ..
        })
    ));
    assert_no_live_io(&runtime);
    assert_eq!(runtime.snapshot().files.open_files, 0);
    let acquire_calls = runtime.snapshot().files.acquire_calls;

    let second = reader
        .read_exact_at(0, &mut bytes)
        .expect_err("sticky replacement must fail before acquire");
    assert_eq!(second, first);
    assert_eq!(runtime.snapshot().files.acquire_calls, acquire_calls);
    assert_eq!(runtime.snapshot().cache.corruption_detections, 1);
}

#[test]
fn transient_io_failure_is_retryable_and_not_sticky() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "transient",
        SegmentFile::Symbols,
        b"retry",
    );

    let error = reader.finish_read_failure(ArtifactReadFailure::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "injected retryable read interruption",
    )));
    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::Interrupted,
            ..
        }
    ));
    assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);

    let mut bytes = [0_u8; 5];
    reader.read_exact_at(0, &mut bytes).expect("retry succeeds");
    assert_eq!(&bytes, b"retry");
    assert_no_live_io(&runtime);
}

#[test]
fn zero_retained_and_zero_cached_budgets_leave_no_values_or_fds() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime = StoreMetadataRuntime::new(config(0, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "zero-budgets",
        SegmentFile::Series,
        b"value",
    );
    let loads = AtomicUsize::new(0);

    for _ in 0..2 {
        let pin = reader
            .get_or_load(key(&reader, 0, 5), 5, |bytes| {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(LoadedMetadata::new(bytes.to_vec(), 5))
            })
            .expect("transient metadata load");
        assert_eq!(&**pin, b"value");
        drop(pin);
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.governor.retained_bytes, 0);
        assert_eq!(snapshot.cache.resident_entries, 0);
        assert_eq!(snapshot.cache.live_allocations, 0);
        assert_eq!(snapshot.files.open_files, 0);
        assert_no_live_io(&runtime);
    }
    assert_eq!(loads.load(Ordering::SeqCst), 2);

    drop(reader);
    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.governor.retained_bytes, 0);
    assert_eq!(snapshot.governor.in_flight_bytes, 0);
}

#[test]
fn one_open_file_limit_is_respected_across_artifacts() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 1)).expect("valid runtime");
    let first = fixture(
        &directory,
        &runtime,
        "one-file-a",
        SegmentFile::Symbols,
        b"a",
    );
    let second = fixture(
        &directory,
        &runtime,
        "one-file-b",
        SegmentFile::Series,
        b"b",
    );

    let mut byte = [0_u8; 1];
    first.read_exact_at(0, &mut byte).expect("read first");
    assert_eq!(byte, *b"a");
    second.read_exact_at(0, &mut byte).expect("read second");
    assert_eq!(byte, *b"b");

    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.files.max_open_files, 1);
    assert_eq!(snapshot.files.peak_occupied_open_slots, 1);
    assert_eq!(snapshot.files.peak_open_files, 1);
    assert_eq!(snapshot.files.open_files, 1);
    assert_eq!(snapshot.files.cached_open_files, 1);
    assert_no_live_io(&runtime);
}

#[test]
fn scratch_budget_refusal_cleans_up_and_allows_a_smaller_retry() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime = StoreMetadataRuntime::new(config(0, 12 * 1024, 1, 0)).expect("valid runtime");
    let bytes = vec![7_u8; 8 * 1024];
    let reader = fixture(&directory, &runtime, "scratch", SegmentFile::Series, &bytes);
    let baseline = runtime.snapshot().governor.in_flight_bytes;
    let validates = AtomicUsize::new(0);

    let error = reader
        .get_or_load::<u8, _>(key(&reader, 0, 8 * 1024), 1, |_| {
            validates.fetch_add(1, Ordering::SeqCst);
            Ok(LoadedMetadata::new(7, 1))
        })
        .expect_err("scratch reservation must be refused");
    assert!(matches!(error, MetadataCacheError::Budget(_)));
    assert_eq!(validates.load(Ordering::SeqCst), 0);
    let refused = runtime.snapshot();
    assert_eq!(refused.governor.in_flight_bytes, baseline);
    assert_eq!(refused.cache.active_loads, 0);
    assert_eq!(refused.cache.live_allocations, 0);
    assert_eq!(refused.files.open_files, 0);
    assert_no_live_io(&runtime);

    let pin = reader
        .get_or_load(key(&reader, 0, 1), 1, |bytes| {
            validates.fetch_add(1, Ordering::SeqCst);
            Ok(LoadedMetadata::new(bytes[0], 1))
        })
        .expect("smaller retry succeeds");
    assert_eq!(*pin, 7);
    drop(pin);
    let scratch = runtime
        .snapshot()
        .governor
        .usage(MetadataUsageClass::Scratch);
    assert_eq!(scratch.in_flight_bytes, 0);
    assert_eq!(scratch.retained_bytes, 0);
    assert!(scratch.peak_in_flight_bytes >= 1);
    assert_eq!(scratch.peak_retained_bytes, 0);
    assert_no_live_io(&runtime);
}

#[test]
fn short_exact_read_records_unexpected_eof_after_lease_release() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "short-read",
        SegmentFile::Series,
        b"four",
    );
    let mut bytes = [0_u8; 2];

    let error = reader
        .read_exact_at(3, &mut bytes)
        .expect_err("range past EOF must fail");
    assert!(matches!(
        error,
        MetadataCacheError::Structural(MetadataCorruption {
            kind: StructuralMetadataErrorKind::UnexpectedEof,
            ..
        })
    ));
    assert_no_live_io(&runtime);
    assert_eq!(runtime.snapshot().files.open_files, 0);
}

#[test]
fn cache_key_mismatch_is_nonsticky_and_does_not_acquire_a_file() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "key-mismatch",
        SegmentFile::Series,
        b"value",
    );
    let wrong_key = MetadataCacheKey::new(
        "another-segment",
        SegmentFile::Series,
        0,
        5,
        MetadataCacheClass::SeriesHotPage,
    )
    .expect("valid mismatched key");

    let error = reader
        .get_or_load::<u8, _>(wrong_key, 1, |_| Ok(LoadedMetadata::new(1, 1)))
        .expect_err("mismatched key must fail");
    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::InvalidInput,
            ..
        }
    ));
    let snapshot = runtime.snapshot();
    assert_eq!(snapshot.files.acquire_calls, 0);
    assert_eq!(snapshot.cache.sticky_artifacts, 0);
}

#[test]
fn transient_file_manager_errors_are_nonsticky() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "manager-transient",
        SegmentFile::Symbols,
        b"ok",
    );
    let error = reader.finish_read_failure(ArtifactReadFailure::FileManager(
        MetadataFileManagerError::Open {
            path: PathBuf::from("injected"),
            source: io::Error::new(io::ErrorKind::PermissionDenied, "temporary denial"),
        },
    ));
    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::PermissionDenied,
            ..
        }
    ));
    assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);

    let mut bytes = [0_u8; 2];
    reader.read_exact_at(0, &mut bytes).expect("retry succeeds");
    assert_eq!(&bytes, b"ok");
}

#[test]
fn retiring_segment_file_manager_error_is_transient_would_block() {
    let directory = TempDir::new().expect("create temp directory");
    let runtime =
        StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
    let reader = fixture(
        &directory,
        &runtime,
        "manager-retiring",
        SegmentFile::Symbols,
        b"ok",
    );
    let error = reader.finish_read_failure(ArtifactReadFailure::FileManager(
        MetadataFileManagerError::SegmentRetiring {
            segment_identity: Arc::from("manager-retiring"),
        },
    ));
    assert!(matches!(
        error,
        MetadataCacheError::Transient {
            kind: io::ErrorKind::WouldBlock,
            ..
        }
    ));
    assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);
}
