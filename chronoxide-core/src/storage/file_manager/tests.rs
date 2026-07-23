use std::fs;
use std::io;
#[cfg(target_os = "linux")]
use std::process::Command;
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use super::*;

fn config(max_open_files: u32, max_cached_open_files: u32) -> MetadataGovernorConfig {
    MetadataGovernorConfig {
        max_open_files,
        max_cached_open_files,
        ..MetadataGovernorConfig::default()
    }
}

fn fixture(
    directory: &TempDir,
    segment_identity: &str,
    file: SegmentFile,
    bytes: &[u8],
) -> SegmentFileHandle {
    let path = directory
        .path()
        .join(format!("{segment_identity}-{}", file.filename()));
    fs::write(&path, bytes).expect("write governed file fixture");
    SegmentFileHandle::preflight_unmanaged_for_test(
        Arc::<str>::from(segment_identity),
        file,
        path,
        u64::try_from(bytes.len()).expect("fixture length fits u64"),
    )
    .expect("preflight governed file fixture")
}

fn replace_same_length(handle: &SegmentFileHandle, replacement: &[u8]) {
    assert_eq!(
        usize::try_from(handle.expected_len()).expect("fixture length fits usize"),
        replacement.len()
    );
    let backup = handle.path().with_extension("original");
    fs::rename(handle.path(), &backup).expect("retain original inode");
    fs::write(handle.path(), replacement).expect("write replacement inode");
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for deterministic concurrency state"
        );
        thread::yield_now();
    }
}

fn assert_failed_acquisition_is_clean(manager: &MetadataFileManager) {
    let stats = manager.stats();
    assert_eq!(stats.open_files, 0);
    assert_eq!(stats.occupied_open_slots, 0);
    assert_eq!(stats.active_open_files, 0);
    assert_eq!(stats.cached_open_files, 0);
    assert_eq!(stats.opening_files, 0);
    assert_eq!(stats.pending_open_files, 0);
    assert_eq!(stats.closing_files, 0);
    assert_eq!(stats.active_leases, 0);
}

fn assert_retiring_error(error: MetadataFileManagerError, segment_identity: &str) {
    match error {
        MetadataFileManagerError::SegmentRetiring {
            segment_identity: actual,
        } => {
            assert_eq!(actual.as_ref(), segment_identity);
        }
        other => panic!("retiring segment must return an explicit transient error: {other}"),
    }
}

fn assert_retirement_state_clean(manager: &MetadataFileManager, segment_identity: &str) {
    let state = manager.lock_state();
    assert!(!state.retirements.contains_key(segment_identity));
    assert!(
        !state
            .active_preflights_by_segment
            .contains_key(segment_identity)
    );
    assert!(
        !state
            .active_acquisitions_by_segment
            .contains_key(segment_identity)
    );
    assert!(
        !state
            .detached_closing_by_segment
            .contains_key(segment_identity)
    );
    assert!(
        !state
            .entries
            .keys()
            .any(|key| key.segment_identity.as_ref() == segment_identity)
    );
}

#[test]
fn preflight_rejects_untracked_and_changed_inventory_entries() {
    let directory = TempDir::new().expect("create temp directory");
    let path = directory.path().join("footer.bin");
    fs::write(&path, b"footer").expect("write footer fixture");
    assert!(matches!(
        SegmentFileHandle::preflight_unmanaged_for_test("segment", SegmentFile::Footer, &path, 6,),
        Err(MetadataFileManagerError::UntrackedSegmentFile {
            file: SegmentFile::Footer
        })
    ));
    assert!(matches!(
        SegmentFileHandle::preflight_unmanaged_for_test("segment", SegmentFile::Symbols, &path, 7,),
        Err(MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::Length {
                expected: 7,
                actual: 6
            },
            ..
        })
    ));
}

#[cfg(unix)]
#[test]
fn preflight_does_not_follow_final_component_symlinks() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().expect("create temp directory");
    let target = directory.path().join("target");
    let link = directory.path().join("link");
    fs::write(&target, b"metadata").expect("write symlink target");
    symlink(&target, &link).expect("create symlink");
    assert!(matches!(
        SegmentFileHandle::preflight_unmanaged_for_test("segment", SegmentFile::Symbols, link, 8,),
        Err(MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::SymbolicLink,
            ..
        })
    ));
}

#[test]
fn managed_preflight_transfers_an_idle_slot_before_opening() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 1)).expect("valid config");
    let cached = fixture(&directory, "cached", SegmentFile::Symbols, b"cached");
    drop(manager.acquire(&cached).expect("cache initial descriptor"));
    assert_eq!(manager.stats().cached_open_files, 1);

    let next_path = directory.path().join("next-symbols.bin");
    fs::write(&next_path, b"next").expect("write next artifact");
    let next = manager
        .preflight("next", SegmentFile::Symbols, &next_path, 4)
        .expect("preflight through governed slot");

    let after_preflight = manager.stats();
    assert_eq!(after_preflight.open_files, 0);
    assert_eq!(after_preflight.occupied_open_slots, 0);
    assert_eq!(after_preflight.cached_open_files, 0);
    assert_eq!(after_preflight.preflighting_files, 0);
    assert_eq!(after_preflight.peak_open_files, 1);
    assert_eq!(after_preflight.peak_occupied_open_slots, 1);
    assert_eq!(after_preflight.peak_preflighting_files, 1);
    assert_eq!(after_preflight.preflight_calls, 1);
    assert_eq!(after_preflight.successful_preflights, 1);
    assert_eq!(after_preflight.preflight_failures, 0);
    assert_eq!(after_preflight.descriptor_opens, 2);
    assert_eq!(after_preflight.descriptor_closes, 2);
    assert_eq!(after_preflight.idle_evictions, 1);

    drop(manager.acquire(&next).expect("preflight slot is reusable"));
    assert_eq!(manager.stats().peak_open_files, 1);
}

#[test]
fn failed_managed_preflight_releases_its_complete_reservation() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    let path = directory.path().join("short-symbols.bin");
    fs::write(&path, b"short").expect("write short artifact");

    assert!(matches!(
        manager.preflight("short", SegmentFile::Symbols, &path, 6),
        Err(MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::Length {
                expected: 6,
                actual: 5
            },
            ..
        })
    ));
    let failed = manager.stats();
    assert_eq!(failed.open_files, 0);
    assert_eq!(failed.occupied_open_slots, 0);
    assert_eq!(failed.preflighting_files, 0);
    assert_eq!(failed.opening_files, 0);
    assert_eq!(failed.preflight_calls, 1);
    assert_eq!(failed.successful_preflights, 0);
    assert_eq!(failed.preflight_failures, 1);
    assert_eq!(failed.open_failures, 1);
    assert_eq!(failed.structural_replacements, 1);

    let recovered = manager
        .preflight("short", SegmentFile::Symbols, &path, 5)
        .expect("failed reservation is reusable");
    drop(manager.acquire(&recovered).expect("recovered handle opens"));
}

#[test]
fn managed_preflight_waits_for_a_leased_hard_cap_slot() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 1)).expect("valid config");
    let active = fixture(&directory, "active", SegmentFile::Symbols, b"active");
    let lease = manager.acquire(&active).expect("lease only slot");
    let waiting_path = directory.path().join("waiting-symbols.bin");
    fs::write(&waiting_path, b"waiting").expect("write waiting artifact");
    let waiting_manager = Arc::clone(&manager);
    let (completed_tx, completed_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = waiting_manager.preflight("waiting", SegmentFile::Symbols, waiting_path, 7);
        completed_tx.send(result).expect("report preflight result");
    });

    wait_until(|| manager.stats().capacity_waits > 0);
    assert!(matches!(
        completed_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let waiting = manager.stats();
    assert_eq!(waiting.open_files, 1);
    assert_eq!(waiting.occupied_open_slots, 1);
    assert_eq!(waiting.active_leases, 1);
    assert_eq!(waiting.preflighting_files, 0);

    drop(lease);
    completed_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("preflight completes after lease release")
        .expect("waiting preflight succeeds");
    worker.join().expect("preflight worker joins");
    let completed = manager.stats();
    assert_eq!(completed.open_files, 0);
    assert_eq!(completed.occupied_open_slots, 0);
    assert_eq!(completed.peak_open_files, 1);
    assert_eq!(completed.peak_occupied_open_slots, 1);
}

#[test]
fn lease_arc_is_destroyed_before_zero_lease_publication() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 1)).expect("valid config");
    let active = fixture(&directory, "release-order", SegmentFile::Symbols, b"active");
    let lease = manager.acquire(&active).expect("lease only slot");
    let waiting_path = directory.path().join("release-waiting-symbols.bin");
    fs::write(&waiting_path, b"waiting").expect("write waiting artifact");

    let arc_dropped = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    *manager
        .release_lease_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(ReleaseLeaseTestHook {
        arc_dropped: Arc::clone(&arc_dropped),
        resume: Arc::clone(&resume),
    });
    let dropper = thread::spawn(move || drop(lease));
    arc_dropped.wait();
    *manager
        .release_lease_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

    {
        let state = manager.lock_state();
        match state.entries.get(active.key()) {
            Some(FileEntry::Open { file, leases, .. }) => {
                assert_eq!(*leases, 1, "lease count is not published early");
                assert_eq!(
                    Arc::strong_count(file),
                    1,
                    "only the authoritative manager Arc remains"
                );
            }
            _ => panic!("active file remains governed while release is paused"),
        }
    }

    let waiting_manager = Arc::clone(&manager);
    let (completed_tx, completed_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        completed_tx
            .send(waiting_manager.preflight(
                "release-waiting",
                SegmentFile::Symbols,
                waiting_path,
                7,
            ))
            .expect("report waiting preflight");
    });
    wait_until(|| manager.stats().capacity_waits > 0);
    assert!(matches!(
        completed_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    resume.wait();
    dropper.join().expect("lease dropper joins");
    completed_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("preflight completes after idle publication")
        .expect("waiting preflight succeeds");
    waiter.join().expect("preflight waiter joins");
    let completed = manager.stats();
    assert_eq!(completed.open_files, 0);
    assert_eq!(completed.occupied_open_slots, 0);
    assert_eq!(completed.peak_open_files, 1);
    assert_eq!(completed.peak_occupied_open_slots, 1);
}

#[test]
fn retirement_closes_idle_descriptor_and_releases_every_counter() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 1)).expect("valid config");
    let handle = fixture(&directory, "retire-idle", SegmentFile::Symbols, b"idle");
    drop(manager.acquire(&handle).expect("open idle fixture"));
    assert_eq!(manager.stats().cached_open_files, 1);

    manager
        .retire_segment("retire-idle")
        .expect("retire idle segment");

    assert_failed_acquisition_is_clean(&manager);
    assert_retirement_state_clean(&manager, "retire-idle");
    let stats = manager.stats();
    assert_eq!(stats.descriptor_opens, 1);
    assert_eq!(stats.descriptor_closes, 1);
    assert_eq!(stats.peak_open_files, 1);
    assert_eq!(stats.peak_occupied_open_slots, 1);
}

#[test]
fn concurrent_retirements_wait_for_final_lease_and_reject_new_work() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 1)).expect("valid config");
    let handle = fixture(&directory, "retire-leased", SegmentFile::Symbols, b"leased");
    let lease = manager.acquire(&handle).expect("lease retiring fixture");
    let (completed_tx, completed_rx) = mpsc::channel();
    let mut workers = Vec::new();
    for caller in 0..2 {
        let manager = Arc::clone(&manager);
        let completed_tx = completed_tx.clone();
        workers.push(thread::spawn(move || {
            let result = manager.retire_segment("retire-leased");
            completed_tx
                .send((caller, result))
                .expect("report retirement result");
        }));
    }
    drop(completed_tx);

    wait_until(|| {
        manager
            .lock_state()
            .retirements
            .get("retire-leased")
            .is_some_and(|retirement| retirement.callers == 2)
    });
    assert!(matches!(
        completed_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_retiring_error(
        manager
            .try_acquire(&handle)
            .expect_err("retirement marker rejects a new acquisition"),
        "retire-leased",
    );
    let preflight_path = directory.path().join("retire-leased-series.bin");
    fs::write(&preflight_path, b"series").expect("write rejected preflight fixture");
    assert_retiring_error(
        manager
            .preflight("retire-leased", SegmentFile::Series, preflight_path, 6)
            .expect_err("retirement marker rejects a new preflight"),
        "retire-leased",
    );

    drop(lease);
    for _ in 0..2 {
        completed_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("retirement completes after final lease release")
            .1
            .expect("joined retirement succeeds");
    }
    for worker in workers {
        worker.join().expect("retirement worker joins");
    }

    assert_failed_acquisition_is_clean(&manager);
    assert_retirement_state_clean(&manager, "retire-leased");
    let stats = manager.stats();
    assert_eq!(stats.descriptor_opens, 1);
    assert_eq!(stats.descriptor_closes, 1);
    assert_eq!(stats.preflight_failures, 1);
}

#[test]
fn retirement_waits_for_preexisting_max_one_preflight() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    let blocker = fixture(
        &directory,
        "preflight-blocker",
        SegmentFile::Symbols,
        b"blocker",
    );
    let blocker_lease = manager.acquire(&blocker).expect("lease only slot");
    let target_path = directory.path().join("retire-preflight-symbols.bin");
    fs::write(&target_path, b"target").expect("write preflight target");

    let preflight_manager = Arc::clone(&manager);
    let (preflight_tx, preflight_rx) = mpsc::channel();
    let preflight_worker = thread::spawn(move || {
        preflight_tx
            .send(preflight_manager.preflight(
                "retire-preflight",
                SegmentFile::Symbols,
                target_path,
                6,
            ))
            .expect("report preflight result");
    });
    wait_until(|| {
        let state = manager.lock_state();
        state
            .active_preflights_by_segment
            .get("retire-preflight")
            .is_some_and(|count| *count == 1)
            && state.counters.capacity_waits > 0
    });

    let retirement_manager = Arc::clone(&manager);
    let (retirement_tx, retirement_rx) = mpsc::channel();
    let retirement_worker = thread::spawn(move || {
        retirement_tx
            .send(retirement_manager.retire_segment("retire-preflight"))
            .expect("report retirement result");
    });
    wait_until(|| {
        manager
            .lock_state()
            .retirements
            .contains_key("retire-preflight")
    });
    assert!(matches!(
        retirement_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    drop(blocker_lease);
    preflight_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("pre-existing preflight completes")
        .expect("pre-existing preflight remains valid");
    preflight_worker.join().expect("preflight worker joins");
    retirement_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("retirement follows preflight completion")
        .expect("retirement succeeds");
    retirement_worker.join().expect("retirement worker joins");

    assert_failed_acquisition_is_clean(&manager);
    assert_retirement_state_clean(&manager, "retire-preflight");
    assert_eq!(manager.stats().peak_occupied_open_slots, 1);
}

#[test]
fn retirement_waits_for_opening_rollback_and_preserves_structural_error() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    let handle = fixture(
        &directory,
        "retire-opening",
        SegmentFile::Symbols,
        b"original",
    );
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    *manager
        .before_open_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(BeforeOpenTestHook {
        segment_identity: Arc::from("retire-opening"),
        entered: Arc::clone(&entered),
        resume: Arc::clone(&resume),
    });

    let acquire_manager = Arc::clone(&manager);
    let acquire_handle = handle.clone();
    let (acquire_tx, acquire_rx) = mpsc::channel();
    let acquire_worker = thread::spawn(move || {
        acquire_tx
            .send(acquire_manager.acquire(&acquire_handle))
            .expect("report acquisition result");
    });
    entered.wait();
    wait_until(|| manager.stats().opening_files == 1);

    let retirement_manager = Arc::clone(&manager);
    let (retirement_tx, retirement_rx) = mpsc::channel();
    let retirement_worker = thread::spawn(move || {
        retirement_tx
            .send(retirement_manager.retire_segment("retire-opening"))
            .expect("report retirement result");
    });
    wait_until(|| {
        manager
            .lock_state()
            .retirements
            .contains_key("retire-opening")
    });
    replace_same_length(&handle, b"replaced");
    resume.wait();
    *manager
        .before_open_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

    assert!(matches!(
        acquire_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("opening acquisition returns"),
        Err(MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::Identity { .. },
            ..
        })
    ));
    acquire_worker.join().expect("acquisition worker joins");
    retirement_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("retirement follows opening rollback")
        .expect("retirement succeeds");
    retirement_worker.join().expect("retirement worker joins");

    assert_failed_acquisition_is_clean(&manager);
    assert_retirement_state_clean(&manager, "retire-opening");
    let stats = manager.stats();
    assert_eq!(stats.structural_replacements, 1);
    assert_eq!(stats.acquisition_rollbacks, 1);
}

#[test]
fn retirement_waits_for_detached_acquisition_victim_close() {
    let directory = TempDir::new().expect("create temp directory");
    let manager = MetadataFileManager::new(config(1, 1)).expect("valid config");
    let victim = fixture(&directory, "retire-victim", SegmentFile::Symbols, b"victim");
    let replacement = fixture(
        &directory,
        "retire-replacement",
        SegmentFile::Symbols,
        b"replacement",
    );
    drop(manager.acquire(&victim).expect("cache victim descriptor"));

    let detached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    *manager
        .detached_close_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(DetachedCloseTestHook {
        segment_identity: Arc::from("retire-victim"),
        detached: Arc::clone(&detached),
        resume: Arc::clone(&resume),
    });
    let replacement_manager = Arc::clone(&manager);
    let (replacement_tx, replacement_rx) = mpsc::channel();
    let replacement_worker = thread::spawn(move || {
        let result = replacement_manager.acquire(&replacement).map(drop);
        replacement_tx
            .send(result)
            .expect("report replacement acquisition");
    });
    detached.wait();
    wait_until(|| {
        manager
            .lock_state()
            .detached_closing_by_segment
            .contains_key("retire-victim")
    });

    let retirement_manager = Arc::clone(&manager);
    let (retirement_tx, retirement_rx) = mpsc::channel();
    let retirement_worker = thread::spawn(move || {
        retirement_tx
            .send(retirement_manager.retire_segment("retire-victim"))
            .expect("report victim retirement");
    });
    wait_until(|| {
        manager
            .lock_state()
            .retirements
            .contains_key("retire-victim")
    });
    assert!(matches!(
        retirement_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    resume.wait();
    *manager
        .detached_close_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    replacement_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("replacement acquisition completes")
        .expect("replacement acquisition succeeds");
    replacement_worker.join().expect("replacement worker joins");
    retirement_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("retirement follows detached close")
        .expect("victim retirement succeeds");
    retirement_worker.join().expect("retirement worker joins");
    manager
        .retire_segment("retire-replacement")
        .expect("clean replacement descriptor");

    assert_failed_acquisition_is_clean(&manager);
    assert_retirement_state_clean(&manager, "retire-victim");
    assert_retirement_state_clean(&manager, "retire-replacement");
    assert_eq!(manager.stats().peak_occupied_open_slots, 1);
}

#[cfg(unix)]
#[test]
fn open_errno_classification_separates_structural_paths_from_transient_failures() {
    let path = Path::new("classification-does-not-touch-this-path");
    assert_eq!(
        structural_open_failure(path, &io::Error::from_raw_os_error(libc::ENOENT)),
        Some(StructuralFileChange::Missing)
    );
    assert_eq!(
        structural_open_failure(path, &io::Error::from_raw_os_error(libc::ENOTDIR)),
        Some(StructuralFileChange::PathComponentNotDirectory)
    );
    assert_eq!(
        structural_open_failure(path, &io::Error::from_raw_os_error(libc::ELOOP)),
        Some(StructuralFileChange::SymbolicLink)
    );
    for errno in [
        libc::EMFILE,
        libc::ENFILE,
        libc::EINTR,
        libc::EIO,
        libc::EACCES,
        libc::EPERM,
    ] {
        assert_eq!(
            structural_open_failure(path, &io::Error::from_raw_os_error(errno)),
            None,
            "errno {errno} must remain transient"
        );
    }
}

#[test]
fn cached_descriptor_is_reused_with_one_live_key_and_positional_reads() {
    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(&directory, "segment-a", SegmentFile::Symbols, b"0123456789");
    let manager = MetadataFileManager::new(config(2, 1)).expect("valid config");

    let first = manager.acquire(&handle).expect("first acquisition");
    let first_instance = first.open_instance_id();
    let mut middle = [0u8; 4];
    first
        .read_exact_at(3, &mut middle)
        .expect("positional read");
    assert_eq!(&middle, b"3456");
    drop(first);
    assert_eq!(
        manager.stats(),
        MetadataFileManagerStats {
            max_open_files: 2,
            max_cached_open_files: 1,
            open_files: 1,
            occupied_open_slots: 1,
            cached_open_files: 1,
            peak_open_files: 1,
            peak_occupied_open_slots: 1,
            peak_active_open_files: 1,
            peak_cached_open_files: 1,
            peak_active_leases: 1,
            acquire_calls: 1,
            successful_acquires: 1,
            requested_handles: 1,
            deduplicated_handles: 1,
            descriptor_opens: 1,
            ..MetadataFileManagerStats::default()
        }
    );

    let second = manager.acquire(&handle).expect("cached acquisition");
    assert_eq!(second.open_instance_id(), first_instance);
    let stats = manager.stats();
    assert_eq!(stats.open_files, 1);
    assert_eq!(stats.active_open_files, 1);
    assert_eq!(stats.active_leases, 1);
    assert_eq!(stats.descriptor_opens, 1);
    assert_eq!(stats.descriptor_reuses, 1);
    drop(second);
}

#[test]
fn idle_lru_evicts_only_zero_lease_descriptors_and_never_exceeds_hard_cap() {
    let directory = TempDir::new().expect("create temp directory");
    let first_handle = fixture(&directory, "segment-a", SegmentFile::Symbols, b"first");
    let second_handle = fixture(&directory, "segment-b", SegmentFile::Symbols, b"other");
    let manager = MetadataFileManager::new(config(1, 1)).expect("valid config");

    let first = manager.acquire(&first_handle).expect("open first");
    let first_instance = first.open_instance_id();
    assert!(matches!(
        manager.try_acquire(&second_handle),
        Err(MetadataFileManagerError::OpenFileCapacityUnavailable { .. })
    ));
    assert_eq!(manager.stats().active_open_files, 1);
    drop(first);

    let second = manager
        .acquire(&second_handle)
        .expect("evict and open second");
    assert_ne!(second.open_instance_id(), first_instance);
    let stats = manager.stats();
    assert_eq!(stats.open_files, 1);
    assert_eq!(stats.peak_open_files, 1);
    assert_eq!(stats.descriptor_opens, 2);
    assert_eq!(stats.descriptor_closes, 1);
    assert_eq!(stats.idle_evictions, 1);
    drop(second);
}

#[test]
fn zero_cached_file_budget_closes_after_the_last_lease() {
    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(&directory, "segment-a", SegmentFile::Series, b"series");
    let manager = MetadataFileManager::new(config(2, 0)).expect("valid config");

    let first = manager.acquire(&handle).expect("open transient file");
    let first_instance = first.open_instance_id();
    let clone = first.clone();
    drop(first);
    assert_eq!(manager.stats().active_leases, 1);
    assert_eq!(manager.stats().open_files, 1);
    drop(clone);
    let closed = manager.stats();
    assert_eq!(closed.open_files, 0);
    assert_eq!(closed.cached_open_files, 0);
    assert_eq!(closed.descriptor_closes, 1);

    let reopened = manager.acquire(&handle).expect("reopen transient file");
    assert_ne!(reopened.open_instance_id(), first_instance);
    drop(reopened);
    let stats = manager.stats();
    assert_eq!(stats.descriptor_opens, 2);
    assert_eq!(stats.descriptor_closes, 2);
    assert_eq!(stats.lease_clones, 1);
    assert_eq!(stats.peak_active_leases, 2);
}

#[test]
fn reopen_detects_same_length_platform_replacement_after_eviction() {
    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(
        &directory,
        "segment-a",
        SegmentFile::ChunkIndex,
        b"original",
    );
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    drop(manager.acquire(&handle).expect("initial open"));
    replace_same_length(&handle, b"replaced");

    let error = manager.acquire(&handle).expect_err("replacement must fail");
    assert!(error.is_structural());
    assert!(matches!(
        error,
        MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::Identity { .. },
            ..
        }
    ));
    let stats = manager.stats();
    assert_eq!(stats.open_files, 0);
    assert_eq!(stats.opening_files, 0);
    assert_eq!(stats.active_leases, 0);
    assert_eq!(stats.open_failures, 1);
    assert_eq!(stats.structural_replacements, 1);
    assert_eq!(stats.acquisition_rollbacks, 1);
}

#[test]
fn reopen_treats_a_deleted_preflighted_path_as_structural() {
    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(
        &directory,
        "segment-a",
        SegmentFile::ChunkIndex,
        b"original",
    );
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    drop(manager.acquire(&handle).expect("initial open"));
    fs::remove_file(handle.path()).expect("remove preflighted file");

    let error = manager
        .acquire(&handle)
        .expect_err("missing path must fail");
    assert!(error.is_structural());
    assert!(matches!(
        error,
        MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::Missing,
            ..
        }
    ));
    assert_failed_acquisition_is_clean(&manager);
    let stats = manager.stats();
    assert_eq!(stats.open_failures, 1);
    assert_eq!(stats.structural_replacements, 1);
    assert_eq!(stats.acquisition_rollbacks, 1);
}

#[cfg(unix)]
#[test]
fn reopen_treats_a_symlink_at_a_preflighted_path_as_structural() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(
        &directory,
        "segment-a",
        SegmentFile::ChunkIndex,
        b"original",
    );
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    drop(manager.acquire(&handle).expect("initial open"));
    let original = handle.path().with_extension("original");
    fs::rename(handle.path(), &original).expect("move preflighted file");
    symlink(&original, handle.path()).expect("substitute symlink");

    let error = manager.acquire(&handle).expect_err("symlink must fail");
    assert!(error.is_structural());
    assert!(matches!(
        error,
        MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::SymbolicLink,
            ..
        }
    ));
    assert_failed_acquisition_is_clean(&manager);
}

#[test]
fn reopen_treats_a_nonregular_preflighted_path_as_structural() {
    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(
        &directory,
        "segment-a",
        SegmentFile::ChunkIndex,
        b"original",
    );
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    drop(manager.acquire(&handle).expect("initial open"));
    let original = handle.path().with_extension("original");
    fs::rename(handle.path(), original).expect("move preflighted file");
    fs::create_dir(handle.path()).expect("substitute directory");

    let error = manager
        .acquire(&handle)
        .expect_err("nonregular path must fail");
    assert!(error.is_structural());
    assert!(matches!(
        error,
        MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::NotRegular,
            ..
        }
    ));
    assert_failed_acquisition_is_clean(&manager);
}

#[test]
fn reopen_treats_a_changed_length_as_structural() {
    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(
        &directory,
        "segment-a",
        SegmentFile::ChunkIndex,
        b"original",
    );
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    drop(manager.acquire(&handle).expect("initial open"));
    fs::write(handle.path(), b"longer-than-original").expect("change tracked length");

    let error = manager
        .acquire(&handle)
        .expect_err("changed length must fail");
    assert!(error.is_structural());
    assert!(matches!(
        error,
        MetadataFileManagerError::StructuralReplacement {
            change: StructuralFileChange::Length {
                expected: 8,
                actual: 20
            },
            ..
        }
    ));
    assert_failed_acquisition_is_clean(&manager);
}

#[test]
fn duplicate_handles_are_deduplicated_and_returned_in_stable_key_order() {
    let directory = TempDir::new().expect("create temp directory");
    let b = fixture(&directory, "b", SegmentFile::Symbols, b"b");
    let c = fixture(&directory, "c", SegmentFile::Symbols, b"c");
    let manager = MetadataFileManager::new(config(2, 0)).expect("valid config");

    let leases = manager
        .acquire_many(&[c.clone(), b.clone(), b])
        .expect("deduplicated acquisition");
    assert_eq!(leases.len(), 2);
    assert_eq!(leases[0].handle().segment_identity(), "b");
    assert_eq!(leases[1].handle().segment_identity(), "c");
    let stats = manager.stats();
    assert_eq!(stats.requested_handles, 3);
    assert_eq!(stats.deduplicated_handles, 2);
    assert_eq!(stats.active_open_files, 2);
    assert_eq!(stats.active_leases, 2);
    drop(leases);
    assert_eq!(manager.stats().open_files, 0);
}

#[test]
fn try_acquire_many_refuses_with_zero_partial_leases() {
    let directory = TempDir::new().expect("create temp directory");
    let blocker = fixture(&directory, "a", SegmentFile::Symbols, b"a");
    let b = fixture(&directory, "b", SegmentFile::Symbols, b"b");
    let c = fixture(&directory, "c", SegmentFile::Symbols, b"c");
    let manager = MetadataFileManager::new(config(2, 0)).expect("valid config");
    let held = manager.acquire(&blocker).expect("hold one slot");

    assert!(matches!(
        manager.try_acquire_many(&[b.clone(), c.clone()]),
        Err(MetadataFileManagerError::OpenFileCapacityUnavailable {
            requested_additional: 2,
            occupied: 1,
            limit: 2
        })
    ));
    let refused = manager.stats();
    assert_eq!(refused.open_files, 1);
    assert_eq!(refused.active_open_files, 1);
    assert_eq!(refused.active_leases, 1);
    assert_eq!(refused.opening_files, 0);
    assert_eq!(refused.descriptor_opens, 1);
    drop(held);

    let acquired = manager
        .acquire_many(&[b, c])
        .expect("capacity is wholly available");
    assert_eq!(acquired.len(), 2);
    assert_eq!(manager.stats().open_files, 2);
    drop(acquired);
}

#[test]
fn failed_batch_rolls_back_opened_files_and_reused_leases() {
    let directory = TempDir::new().expect("create temp directory");
    let a = fixture(&directory, "a", SegmentFile::Symbols, b"a");
    let b = fixture(&directory, "b", SegmentFile::Symbols, b"b");
    let c = fixture(&directory, "c", SegmentFile::Symbols, b"c");
    let manager = MetadataFileManager::new(config(3, 3)).expect("valid config");
    let cached_a = manager.acquire(&a).expect("open reusable file");
    let a_instance = cached_a.open_instance_id();
    drop(cached_a);
    replace_same_length(&c, b"x");

    let error = manager
        .acquire_many(&[a.clone(), b.clone(), c])
        .expect_err("last reopen must fail the whole batch");
    assert!(error.is_structural());
    let rolled_back = manager.stats();
    assert_eq!(rolled_back.open_files, 1);
    assert_eq!(rolled_back.active_open_files, 0);
    assert_eq!(rolled_back.cached_open_files, 1);
    assert_eq!(rolled_back.active_leases, 0);
    assert_eq!(rolled_back.opening_files, 0);
    assert_eq!(rolled_back.descriptor_opens, 2);
    assert_eq!(rolled_back.descriptor_closes, 1);
    assert_eq!(rolled_back.acquisition_rollbacks, 1);

    let reused_a = manager
        .acquire(&a)
        .expect("rolled-back reuse remains keyed");
    assert_eq!(reused_a.open_instance_id(), a_instance);
    drop(reused_a);
    let opened_b = manager.acquire(&b).expect("rolled-back new key can retry");
    drop(opened_b);
}

#[test]
fn concurrent_multi_file_acquisitions_never_hold_partial_sets() {
    let directory = TempDir::new().expect("create temp directory");
    let handles = [
        fixture(&directory, "a", SegmentFile::Symbols, b"a"),
        fixture(&directory, "b", SegmentFile::Symbols, b"b"),
        fixture(&directory, "c", SegmentFile::Symbols, b"c"),
        fixture(&directory, "d", SegmentFile::Symbols, b"d"),
    ];
    let manager = MetadataFileManager::new(config(2, 0)).expect("valid config");
    let start = Arc::new(Barrier::new(3));
    let release_first = Arc::new(Barrier::new(2));
    let release_second = Arc::new(Barrier::new(2));
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let spawn_worker = |worker: usize, pair: Vec<SegmentFileHandle>, release: Arc<Barrier>| {
        let manager = Arc::clone(&manager);
        let start = Arc::clone(&start);
        let acquired_tx = acquired_tx.clone();
        thread::spawn(move || {
            start.wait();
            let leases = manager.acquire_many(&pair).expect("worker acquisition");
            assert_eq!(leases.len(), 2);
            acquired_tx.send(worker).expect("announce acquisition");
            release.wait();
            drop(leases);
        })
    };
    let first_worker = spawn_worker(
        0,
        vec![handles[0].clone(), handles[1].clone()],
        Arc::clone(&release_first),
    );
    let second_worker = spawn_worker(
        1,
        vec![handles[2].clone(), handles[3].clone()],
        Arc::clone(&release_second),
    );
    drop(acquired_tx);
    start.wait();

    let first = acquired_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("one complete set acquires");
    wait_until(|| manager.stats().capacity_waits >= 1);
    assert!(matches!(
        acquired_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let held = manager.stats();
    assert_eq!(held.open_files, 2);
    assert_eq!(held.active_open_files, 2);
    assert_eq!(held.active_leases, 2);
    assert_eq!(held.peak_open_files, 2);
    if first == 0 {
        release_first.wait();
    } else {
        release_second.wait();
    }

    let second = acquired_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("waiting complete set acquires after release");
    assert_ne!(first, second);
    if second == 0 {
        release_first.wait();
    } else {
        release_second.wait();
    }
    first_worker.join().expect("first worker joins");
    second_worker.join().expect("second worker joins");
    let done = manager.stats();
    assert_eq!(done.open_files, 0);
    assert_eq!(done.peak_open_files, 2);
    assert_eq!(done.peak_active_open_files, 2);
    assert_eq!(done.peak_active_leases, 2);
}

#[test]
fn cloned_leases_support_concurrent_positional_reads_without_seek_state() {
    const THREADS: usize = 8;
    const RANGE: usize = 257;

    let directory = TempDir::new().expect("create temp directory");
    let bytes: Vec<_> = (0..THREADS * RANGE)
        .map(|index| ((index * 31 + 7) % 251) as u8)
        .collect();
    let handle = fixture(&directory, "segment-a", SegmentFile::Indexes, &bytes);
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    let lease = manager.acquire(&handle).expect("open positional file");
    let start = Arc::new(Barrier::new(THREADS + 1));
    let observed = Arc::new(Barrier::new(THREADS + 1));
    let mut workers = Vec::new();
    for thread_index in 0..THREADS {
        let lease = lease.clone();
        let start = Arc::clone(&start);
        let observed = Arc::clone(&observed);
        let expected = bytes[thread_index * RANGE..(thread_index + 1) * RANGE].to_vec();
        workers.push(thread::spawn(move || {
            start.wait();
            let mut actual = vec![0u8; RANGE];
            lease
                .read_exact_at(
                    u64::try_from(thread_index * RANGE).expect("offset fits u64"),
                    &mut actual,
                )
                .expect("concurrent positional read");
            assert_eq!(actual, expected);
            observed.wait();
        }));
    }
    start.wait();
    wait_until(|| manager.stats().active_leases == (THREADS + 1) as u32);
    observed.wait();
    for worker in workers {
        worker.join().expect("read worker joins");
    }
    let active = manager.stats();
    assert_eq!(active.active_leases, 1);
    assert_eq!(active.lease_clones, THREADS as u64);
    assert_eq!(active.peak_active_leases, (THREADS + 1) as u32);
    drop(lease);
    assert_eq!(manager.stats().open_files, 0);
}

#[cfg(target_os = "linux")]
#[test]
fn low_rlimit_proves_close_before_open_and_failed_batch_rollback() {
    const CHILD_ENV: &str = "CHRONOXIDE_FILE_MANAGER_LOW_RLIMIT_CHILD";
    const TEST_NAME: &str = concat!(
        "storage::file_manager::tests::",
        "low_rlimit_proves_close_before_open_and_failed_batch_rollback"
    );

    if std::env::var_os(CHILD_ENV).is_some() {
        run_low_rlimit_child();
        return;
    }

    let output = Command::new(std::env::current_exe().expect("locate current test binary"))
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("spawn isolated low-RLIMIT test process");
    assert!(
        output.status.success(),
        "isolated low-RLIMIT test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "linux")]
fn run_low_rlimit_child() {
    let directory = TempDir::new().expect("create temp directory");
    let a = fixture(&directory, "a", SegmentFile::Symbols, b"a");
    let b = fixture(&directory, "b", SegmentFile::Symbols, b"b");
    let c = fixture(&directory, "c", SegmentFile::Symbols, b"c");
    let d = fixture(&directory, "d", SegmentFile::Symbols, b"d");

    let cached_manager = MetadataFileManager::new(config(1, 1)).expect("valid config");
    drop(cached_manager.acquire(&a).expect("cache first descriptor"));
    assert_eq!(cached_manager.stats().cached_open_files, 1);

    let highest_fd = fs::read_dir("/proc/self/fd")
        .expect("enumerate child descriptors")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_string_lossy().parse::<u64>().ok())
        .max()
        .expect("child process has standard descriptors");
    let mut original_limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut original_limit) },
        0,
        "read RLIMIT_NOFILE"
    );
    let desired_limit = highest_fd.saturating_add(32);
    let hard_limit = original_limit.rlim_max;
    let limited_soft = if hard_limit == libc::RLIM_INFINITY {
        desired_limit as libc::rlim_t
    } else {
        (desired_limit as libc::rlim_t).min(hard_limit)
    };
    assert!(
        limited_soft > highest_fd.saturating_add(4) as libc::rlim_t,
        "hard RLIMIT_NOFILE is too low for the isolated test"
    );
    let limited = libc::rlimit {
        rlim_cur: limited_soft,
        rlim_max: hard_limit,
    };
    assert_eq!(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limited) },
        0,
        "lower child RLIMIT_NOFILE"
    );

    let mut fillers = Vec::new();
    loop {
        match OpenOptions::new().read(true).open("/dev/null") {
            Ok(file) => fillers.push(file),
            Err(error) if error.raw_os_error() == Some(libc::EMFILE) => break,
            Err(error) => panic!("fill child descriptor table: {error}"),
        }
    }

    // The descriptor table is full. With max_open_files=1, preflighting B
    // can succeed only if the idle A victim is closed before B is opened.
    let b = cached_manager
        .preflight("b", SegmentFile::Symbols, b.path(), b.expected_len())
        .expect("close cached victim before preflight open");
    let preflight_close_before_open = cached_manager.stats();
    assert_eq!(preflight_close_before_open.open_files, 0);
    assert_eq!(preflight_close_before_open.occupied_open_slots, 0);
    assert_eq!(preflight_close_before_open.peak_open_files, 1);
    assert_eq!(preflight_close_before_open.descriptor_opens, 2);
    assert_eq!(preflight_close_before_open.descriptor_closes, 2);
    assert_eq!(preflight_close_before_open.successful_preflights, 1);

    // The closed preflight descriptor leaves one kernel slot available for
    // the ordinary governed reopen.
    let b_lease = cached_manager
        .acquire(&b)
        .expect("open preflighted replacement");
    let close_before_open = cached_manager.stats();
    assert_eq!(close_before_open.open_files, 1);
    assert_eq!(close_before_open.occupied_open_slots, 1);
    assert_eq!(close_before_open.peak_open_files, 1);
    assert_eq!(close_before_open.descriptor_opens, 3);
    assert_eq!(close_before_open.descriptor_closes, 2);
    drop(b_lease);
    drop(cached_manager);

    // Dropping the cached B file leaves exactly one kernel slot free. A
    // two-file batch opens C, fails to open D with EMFILE, closes C, and
    // releases the complete all-or-none reservation before returning.
    let batch_manager = MetadataFileManager::new(config(2, 0)).expect("valid config");
    let error = batch_manager
        .acquire_many(&[c.clone(), d])
        .expect_err("second batch open must hit the child descriptor limit");
    match error {
        MetadataFileManagerError::Open { source, .. } => {
            assert_eq!(source.raw_os_error(), Some(libc::EMFILE));
        }
        other => panic!("EMFILE must remain a non-structural open error: {other}"),
    }
    assert_failed_acquisition_is_clean(&batch_manager);
    let rolled_back = batch_manager.stats();
    assert_eq!(rolled_back.descriptor_opens, 1);
    assert_eq!(rolled_back.descriptor_closes, 1);
    assert_eq!(rolled_back.open_failures, 1);
    assert_eq!(rolled_back.structural_replacements, 0);
    assert_eq!(rolled_back.acquisition_rollbacks, 1);

    drop(
        batch_manager
            .acquire(&c)
            .expect("rolled-back kernel and manager slots are reusable"),
    );
    let recovered = batch_manager.stats();
    assert_eq!(recovered.descriptor_opens, 2);
    assert_eq!(recovered.descriptor_closes, 2);
    drop(fillers);
}

#[test]
fn distinct_set_larger_than_hard_cap_fails_before_opening_any_file() {
    let directory = TempDir::new().expect("create temp directory");
    let a = fixture(&directory, "a", SegmentFile::Symbols, b"a");
    let b = fixture(&directory, "b", SegmentFile::Symbols, b"b");
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");

    assert!(matches!(
        manager.acquire_many(&[a, b]),
        Err(MetadataFileManagerError::RequestExceedsOpenFileLimit {
            requested: 2,
            limit: 1
        })
    ));
    let stats = manager.stats();
    assert_eq!(stats.open_files, 0);
    assert_eq!(stats.descriptor_opens, 0);
    assert_eq!(stats.capacity_refusals, 1);
}

#[test]
fn conflicting_stable_key_definitions_are_rejected() {
    let directory = TempDir::new().expect("create temp directory");
    let first = fixture(&directory, "same", SegmentFile::Symbols, b"first");
    let second_path = directory.path().join("second-symbols.bin");
    fs::write(&second_path, b"other").expect("write second definition");
    let second = SegmentFileHandle::preflight_unmanaged_for_test(
        "same",
        SegmentFile::Symbols,
        second_path,
        5,
    )
    .expect("preflight second definition");
    let manager = MetadataFileManager::new(config(2, 0)).expect("valid config");
    assert!(matches!(
        manager.try_acquire_many(&[first, second]),
        Err(MetadataFileManagerError::ConflictingHandle { .. })
    ));
    assert_eq!(manager.stats().open_files, 0);
}

#[test]
fn positional_read_reports_structural_short_read() {
    let directory = TempDir::new().expect("create temp directory");
    let handle = fixture(&directory, "segment-a", SegmentFile::Indexes, b"bytes");
    let manager = MetadataFileManager::new(config(1, 0)).expect("valid config");
    let lease = manager.acquire(&handle).expect("open file");
    let mut destination = [0u8; 2];
    let error = lease
        .read_exact_at(4, &mut destination)
        .expect_err("range crosses EOF");
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}
