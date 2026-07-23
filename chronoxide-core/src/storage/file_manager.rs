//! Store-wide file-descriptor governance for immutable segment metadata.
//!
//! Handles capture the regular-file identity established by footer preflight.
//! Leases expose only positional reads, so sharing a governed descriptor cannot
//! introduce a mutable seek cursor.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use thiserror::Error;

use crate::storage::index::SegmentIndexReadAt;
use crate::storage::metadata_governor::{MetadataGovernorConfig, MetadataGovernorConfigError};
use crate::storage::segment::SegmentFile;

/// The platform identity of the exact regular file opened during preflight.
///
/// On Unix this is the filesystem device/inode pair. Schema-7 production
/// support currently targets Unix; unsupported platforms fail preflight rather
/// than silently weakening replacement detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlatformFileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl fmt::Display for PlatformFileIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(unix)]
        {
            write!(formatter, "dev={} ino={}", self.device, self.inode)
        }
        #[cfg(not(unix))]
        {
            formatter.write_str("unsupported")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StructuralFileChange {
    Missing,
    PathComponentNotDirectory,
    SymbolicLink,
    NotRegular,
    Length {
        expected: u64,
        actual: u64,
    },
    Identity {
        expected: PlatformFileIdentity,
        actual: PlatformFileIdentity,
    },
}

impl fmt::Display for StructuralFileChange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => formatter.write_str("footer-tracked path is missing"),
            Self::PathComponentNotDirectory => {
                formatter.write_str("a footer-tracked path component is not a directory")
            }
            Self::SymbolicLink => formatter.write_str(
                "footer-tracked path is a symbolic link or contains a symbolic-link loop",
            ),
            Self::NotRegular => formatter.write_str("opened object is not a regular file"),
            Self::Length { expected, actual } => {
                write!(
                    formatter,
                    "length changed: expected={expected} actual={actual}"
                )
            }
            Self::Identity { expected, actual } => {
                write!(
                    formatter,
                    "identity changed: expected=({expected}) actual=({actual})"
                )
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum MetadataFileManagerError {
    #[error("stable segment identity must not be empty")]
    EmptySegmentIdentity,
    #[error("{file:?} is not tracked by the segment footer")]
    UntrackedSegmentFile { file: SegmentFile },
    #[error("failed to open governed segment file {path}")]
    Open {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("governed segment {segment_identity} is retiring")]
    SegmentRetiring { segment_identity: Arc<str> },
    #[error(
        "structural segment-file replacement for {segment_identity}/{file:?} at {path}: {change}"
    )]
    StructuralReplacement {
        segment_identity: Arc<str>,
        file: SegmentFile,
        path: PathBuf,
        change: StructuralFileChange,
    },
    #[error(
        "conflicting governed handles for stable key {segment_identity}/{file:?}: {first_path} versus {second_path}"
    )]
    ConflictingHandle {
        segment_identity: Arc<str>,
        file: SegmentFile,
        first_path: PathBuf,
        second_path: PathBuf,
    },
    #[error(
        "requested {requested} distinct governed files exceeds the hard open-file limit {limit}"
    )]
    RequestExceedsOpenFileLimit { requested: u32, limit: u32 },
    #[error(
        "governed open-file capacity is unavailable: requested_additional={requested_additional} occupied={occupied} limit={limit}"
    )]
    OpenFileCapacityUnavailable {
        requested_additional: u32,
        occupied: u32,
        limit: u32,
    },
    #[error("platform does not expose a supported stable regular-file identity")]
    UnsupportedPlatformIdentity,
}

impl MetadataFileManagerError {
    /// Structural errors are suitable for the store's sticky corruption
    /// ledger after the failed acquisition has rolled back all reservations.
    pub fn is_structural(&self) -> bool {
        matches!(self, Self::StructuralReplacement { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentFileKey {
    segment_identity: Arc<str>,
    file_rank: u8,
}

/// Immutable identity and path information captured by footer preflight.
#[derive(Clone)]
pub struct SegmentFileHandle {
    inner: Arc<SegmentFileHandleInner>,
}

#[derive(Debug)]
struct SegmentFileHandleInner {
    key: SegmentFileKey,
    file: SegmentFile,
    path: PathBuf,
    expected_len: u64,
    expected_identity: PlatformFileIdentity,
}

impl SegmentFileHandle {
    #[cfg(test)]
    fn preflight_unmanaged_for_test(
        segment_identity: impl Into<Arc<str>>,
        file: SegmentFile,
        path: impl Into<PathBuf>,
        footer_recorded_len: u64,
    ) -> Result<Self, MetadataFileManagerError> {
        let (segment_identity, path) =
            Self::validate_preflight_request(segment_identity, file, path)?;
        let opened = open_immutable(&path)
            .map_err(|source| classify_open_failure(&segment_identity, file, &path, source))?;
        Self::from_preflighted_file(segment_identity, file, path, footer_recorded_len, &opened)
    }

    fn validate_preflight_request(
        segment_identity: impl Into<Arc<str>>,
        file: SegmentFile,
        path: impl Into<PathBuf>,
    ) -> Result<(Arc<str>, PathBuf), MetadataFileManagerError> {
        let segment_identity = segment_identity.into();
        if segment_identity.is_empty() {
            return Err(MetadataFileManagerError::EmptySegmentIdentity);
        }
        if !is_footer_tracked(file) {
            return Err(MetadataFileManagerError::UntrackedSegmentFile { file });
        }
        Ok((segment_identity, path.into()))
    }

    fn from_preflighted_file(
        segment_identity: Arc<str>,
        file: SegmentFile,
        path: PathBuf,
        footer_recorded_len: u64,
        opened: &File,
    ) -> Result<Self, MetadataFileManagerError> {
        let metadata = opened
            .metadata()
            .map_err(|source| MetadataFileManagerError::Open {
                path: path.clone(),
                source,
            })?;
        if !metadata.is_file() {
            return Err(structural_replacement(
                &segment_identity,
                file,
                &path,
                StructuralFileChange::NotRegular,
            ));
        }
        if metadata.len() != footer_recorded_len {
            return Err(structural_replacement(
                &segment_identity,
                file,
                &path,
                StructuralFileChange::Length {
                    expected: footer_recorded_len,
                    actual: metadata.len(),
                },
            ));
        }
        let expected_identity = platform_file_identity(&metadata)?;

        Ok(Self {
            inner: Arc::new(SegmentFileHandleInner {
                key: SegmentFileKey {
                    segment_identity,
                    file_rank: segment_file_rank(file),
                },
                file,
                path,
                expected_len: footer_recorded_len,
                expected_identity,
            }),
        })
    }

    pub fn segment_identity(&self) -> &str {
        &self.inner.key.segment_identity
    }

    pub fn file(&self) -> SegmentFile {
        self.inner.file
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn expected_len(&self) -> u64 {
        self.inner.expected_len
    }

    pub fn expected_identity(&self) -> PlatformFileIdentity {
        self.inner.expected_identity
    }

    fn key(&self) -> &SegmentFileKey {
        &self.inner.key
    }

    fn same_definition(&self, other: &Self) -> bool {
        self.inner.file == other.inner.file
            && self.inner.path == other.inner.path
            && self.inner.expected_len == other.inner.expected_len
            && self.inner.expected_identity == other.inner.expected_identity
    }
}

impl fmt::Debug for SegmentFileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentFileHandle")
            .field("segment_identity", &self.segment_identity())
            .field("file", &self.file())
            .field("path", &self.path())
            .field("expected_len", &self.expected_len())
            .field("expected_identity", &self.expected_identity())
            .finish()
    }
}

/// A point-in-time descriptor-governance snapshot.
///
/// `open_files` is the manager-accounted count of live descriptors whose open
/// has completed, including a transient preflight descriptor still undergoing
/// validation. During a detached close it can conservatively include a
/// descriptor that the kernel has just closed; during an out-of-lock open,
/// `opening_files` can include a descriptor just before it is added to
/// `open_files`. These phase counters are not an atomic enumeration of the
/// process descriptor table.
///
/// `occupied_open_slots` additionally includes all-or-none opening
/// reservations, preflight reservations, and close-before-open transfers. It
/// is the authoritative hard cap and never falls before a detached victim has
/// closed. `descriptor_opens` and `descriptor_closes` count successfully
/// verified governed descriptors, including transient preflight descriptors;
/// a descriptor rejected during verification is reported by `open_failures`
/// but is not included in those two counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataFileManagerStats {
    pub max_open_files: u32,
    pub max_cached_open_files: u32,
    pub open_files: u32,
    pub occupied_open_slots: u32,
    pub active_open_files: u32,
    pub cached_open_files: u32,
    pub opening_files: u32,
    pub pending_open_files: u32,
    pub preflighting_files: u32,
    pub closing_files: u32,
    pub active_leases: u32,
    pub peak_open_files: u32,
    pub peak_occupied_open_slots: u32,
    pub peak_active_open_files: u32,
    pub peak_cached_open_files: u32,
    pub peak_active_leases: u32,
    pub peak_preflighting_files: u32,
    pub preflight_calls: u64,
    pub successful_preflights: u64,
    pub preflight_failures: u64,
    pub acquire_calls: u64,
    pub successful_acquires: u64,
    pub requested_handles: u64,
    pub deduplicated_handles: u64,
    pub descriptor_opens: u64,
    pub descriptor_closes: u64,
    pub descriptor_reuses: u64,
    pub lease_clones: u64,
    pub idle_evictions: u64,
    pub capacity_waits: u64,
    pub capacity_refusals: u64,
    pub open_failures: u64,
    pub structural_replacements: u64,
    pub acquisition_rollbacks: u64,
}

#[derive(Debug, Default)]
struct FileManagerCounters {
    peak_open_files: u32,
    peak_occupied_open_slots: u32,
    peak_active_open_files: u32,
    peak_cached_open_files: u32,
    peak_active_leases: u32,
    peak_preflighting_files: u32,
    preflight_calls: u64,
    successful_preflights: u64,
    preflight_failures: u64,
    acquire_calls: u64,
    successful_acquires: u64,
    requested_handles: u64,
    deduplicated_handles: u64,
    descriptor_opens: u64,
    descriptor_closes: u64,
    descriptor_reuses: u64,
    lease_clones: u64,
    idle_evictions: u64,
    capacity_waits: u64,
    capacity_refusals: u64,
    open_failures: u64,
    structural_replacements: u64,
    acquisition_rollbacks: u64,
}

#[derive(Debug, Default)]
struct FileManagerState {
    entries: BTreeMap<SegmentFileKey, FileEntry>,
    idle_lru: VecDeque<SegmentFileKey>,
    retirements: BTreeMap<Arc<str>, SegmentRetirementState>,
    active_preflights_by_segment: BTreeMap<Arc<str>, u32>,
    active_acquisitions_by_segment: BTreeMap<Arc<str>, u32>,
    detached_closing_by_segment: BTreeMap<Arc<str>, u32>,
    occupied_slots: u32,
    live_descriptors: u32,
    pending_open_descriptors: u32,
    preflight_reservations: u32,
    live_preflight_descriptors: u32,
    next_operation_id: u64,
    counters: FileManagerCounters,
}

#[derive(Debug)]
enum FileEntry {
    Opening {
        handle: SegmentFileHandle,
        operation_id: u64,
    },
    Open {
        handle: SegmentFileHandle,
        file: Arc<GovernedOpenFile>,
        leases: u32,
    },
}

#[derive(Debug)]
struct SegmentRetirementState {
    callers: u32,
    completed: bool,
}

#[derive(Debug)]
struct GovernedOpenFile {
    instance_id: u64,
    file: File,
}

/// Store-owned hard and idle descriptor governor.
#[derive(Debug)]
pub struct MetadataFileManager {
    max_open_files: u32,
    max_cached_open_files: u32,
    state: Mutex<FileManagerState>,
    capacity_changed: Condvar,
    next_file_instance_id: AtomicU64,
    #[cfg(test)]
    release_lease_test_hook: Mutex<Option<ReleaseLeaseTestHook>>,
    #[cfg(test)]
    before_open_test_hook: Mutex<Option<BeforeOpenTestHook>>,
    #[cfg(test)]
    detached_close_test_hook: Mutex<Option<DetachedCloseTestHook>>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct ReleaseLeaseTestHook {
    arc_dropped: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct BeforeOpenTestHook {
    segment_identity: Arc<str>,
    entered: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct DetachedCloseTestHook {
    segment_identity: Arc<str>,
    detached: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

impl MetadataFileManager {
    pub fn new(config: MetadataGovernorConfig) -> Result<Arc<Self>, MetadataGovernorConfigError> {
        let config = config.validate()?;
        Ok(Arc::new(Self {
            max_open_files: config.max_open_files,
            max_cached_open_files: config.max_cached_open_files,
            state: Mutex::new(FileManagerState::default()),
            capacity_changed: Condvar::new(),
            next_file_instance_id: AtomicU64::new(1),
            #[cfg(test)]
            release_lease_test_hook: Mutex::new(None),
            #[cfg(test)]
            before_open_test_hook: Mutex::new(None),
            #[cfg(test)]
            detached_close_test_hook: Mutex::new(None),
        }))
    }

    pub(crate) fn max_open_files(&self) -> u32 {
        self.max_open_files
    }

    /// Captures one footer-tracked file identity under the same hard descriptor
    /// cap used by ordinary reads.
    ///
    /// The returned handle owns no descriptor. An idle cached descriptor may
    /// be closed and its slot transferred to this preflight before the path is
    /// opened. If every slot is leased, this call waits without retaining a
    /// partial descriptor or file-manager lock.
    pub fn preflight(
        self: &Arc<Self>,
        segment_identity: impl Into<Arc<str>>,
        file: SegmentFile,
        path: impl Into<PathBuf>,
        footer_recorded_len: u64,
    ) -> Result<SegmentFileHandle, MetadataFileManagerError> {
        {
            let mut state = self.lock_state();
            state.counters.preflight_calls = state.counters.preflight_calls.saturating_add(1);
        }

        let result: Result<SegmentFileHandle, MetadataFileManagerError> = (|| {
            let (segment_identity, path) =
                SegmentFileHandle::validate_preflight_request(segment_identity, file, path)?;
            let request = self.begin_preflight(Arc::clone(&segment_identity))?;
            let permit = self.reserve_preflight_slot(request);
            let mut opened = permit.open(&segment_identity, file, &path)?;
            let handle = SegmentFileHandle::from_preflighted_file(
                segment_identity,
                file,
                path,
                footer_recorded_len,
                opened.file(),
            )?;
            opened.mark_verified();
            Ok(handle)
        })();

        let mut state = self.lock_state();
        match &result {
            Ok(_) => {
                state.counters.successful_preflights =
                    state.counters.successful_preflights.saturating_add(1);
            }
            Err(_) => {
                state.counters.preflight_failures =
                    state.counters.preflight_failures.saturating_add(1);
                if matches!(&result, Err(error) if error.is_structural()) {
                    state.counters.structural_replacements =
                        state.counters.structural_replacements.saturating_add(1);
                }
            }
        }
        result
    }

    /// Blocks until the complete deduplicated set can be reserved and opened.
    /// No partial leases are visible while waiting or when any open fails.
    ///
    /// A caller must not retain leases from a prior acquisition while making a
    /// second blocking acquisition. Operations whose distinct set exceeds the
    /// hard cap must partition it before calling this method and release each
    /// partition before acquiring the next one.
    pub fn acquire_many(
        self: &Arc<Self>,
        handles: &[SegmentFileHandle],
    ) -> Result<GovernedFileLeaseSet, MetadataFileManagerError> {
        self.acquire_many_with_mode(handles, AcquireMode::Wait)
    }

    /// Attempts one all-or-none acquisition without waiting for leased slots.
    pub fn try_acquire_many(
        self: &Arc<Self>,
        handles: &[SegmentFileHandle],
    ) -> Result<GovernedFileLeaseSet, MetadataFileManagerError> {
        self.acquire_many_with_mode(handles, AcquireMode::ReturnCapacityError)
    }

    pub fn acquire(
        self: &Arc<Self>,
        handle: &SegmentFileHandle,
    ) -> Result<GovernedFileLease, MetadataFileManagerError> {
        let mut set = self.acquire_many(std::slice::from_ref(handle))?;
        Ok(set
            .leases
            .pop()
            .expect("single-file acquisition must return one lease"))
    }

    pub fn try_acquire(
        self: &Arc<Self>,
        handle: &SegmentFileHandle,
    ) -> Result<GovernedFileLease, MetadataFileManagerError> {
        let mut set = self.try_acquire_many(std::slice::from_ref(handle))?;
        Ok(set
            .leases
            .pop()
            .expect("single-file acquisition must return one lease"))
    }

    pub fn stats(&self) -> MetadataFileManagerStats {
        let state = self.lock_state();
        snapshot(self, &state)
    }

    /// Drains every governed descriptor associated with one stable segment.
    ///
    /// The first caller installs a marker that rejects later preflights and
    /// acquisitions. Concurrent retirement callers join that same operation.
    /// Idle descriptors are detached and closed outside the manager mutex;
    /// pre-existing preflights, acquisitions, opens, and leases are allowed to
    /// finish, and their final transition wakes this waiter. The marker is
    /// cleared only after the last concurrent retirement caller observes a
    /// fully quiescent segment.
    ///
    /// This is an internal descriptor-drain primitive, not the inventory
    /// retirement boundary. Once the marker clears, a previously cloned raw
    /// handle could be acquired again. The store runtime must therefore stop
    /// issuing generation-bound read guards, wait for them to drain, call this
    /// method, and retire the matching cache inventory before it publishes the
    /// identity as vacant again.
    pub(crate) fn retire_segment(
        self: &Arc<Self>,
        segment_identity: impl Into<Arc<str>>,
    ) -> Result<(), MetadataFileManagerError> {
        let segment_identity = segment_identity.into();
        if segment_identity.is_empty() {
            return Err(MetadataFileManagerError::EmptySegmentIdentity);
        }

        let leader = {
            let mut state = self.lock_state();
            match state.retirements.entry(Arc::clone(&segment_identity)) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(SegmentRetirementState {
                        callers: 1,
                        completed: false,
                    });
                    true
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().callers = entry
                        .get()
                        .callers
                        .checked_add(1)
                        .expect("segment retirement caller count overflow");
                    false
                }
            }
        };
        // Wake capacity waiters so a pre-existing acquisition can observe the
        // marker, release its keyed request, and unblock the retirement.
        self.capacity_changed.notify_all();

        if !leader {
            self.wait_for_joined_retirement(&segment_identity);
            return Ok(());
        }

        loop {
            let mut state = self.lock_state();
            let victims = detach_idle_segment_files(&mut state, &segment_identity);
            if !victims.is_empty() {
                drop(state);
                self.finish_detached_closes(victims);
                continue;
            }

            if !segment_has_file_manager_activity(&state, &segment_identity) {
                state
                    .retirements
                    .get_mut(&segment_identity)
                    .expect("retirement leader must retain its marker")
                    .completed = true;
                release_retirement_caller(&mut state, &segment_identity);
                drop(state);
                self.capacity_changed.notify_all();
                return Ok(());
            }

            drop(self.wait_for_capacity(state));
        }
    }

    fn begin_preflight(
        self: &Arc<Self>,
        segment_identity: Arc<str>,
    ) -> Result<PreflightRequest, MetadataFileManagerError> {
        let mut state = self.lock_state();
        if state.retirements.contains_key(&segment_identity) {
            return Err(segment_retiring_error(&segment_identity));
        }
        increment_segment_activity(
            &mut state.active_preflights_by_segment,
            Arc::clone(&segment_identity),
            "preflight",
        );
        Ok(PreflightRequest {
            manager: Arc::clone(self),
            segment_identity,
            active: true,
        })
    }

    fn reserve_preflight_slot(self: &Arc<Self>, request: PreflightRequest) -> PreflightPermit {
        loop {
            let mut state = self.lock_state();
            let victim = if state.occupied_slots < self.max_open_files {
                state.occupied_slots = state
                    .occupied_slots
                    .checked_add(1)
                    .expect("preflight slot count overflow");
                None
            } else if let Some(key) = state.idle_lru.pop_front() {
                match state.entries.remove(&key) {
                    Some(FileEntry::Open {
                        file, leases: 0, ..
                    }) => {
                        state.counters.idle_evictions =
                            state.counters.idle_evictions.saturating_add(1);
                        Some(track_detached_close(
                            &mut state,
                            key,
                            file,
                            DetachedSlotDisposition::Transfer,
                        ))
                    }
                    Some(entry) => {
                        state.entries.insert(key, entry);
                        continue;
                    }
                    None => continue,
                }
            } else {
                state.counters.capacity_waits = state.counters.capacity_waits.saturating_add(1);
                drop(self.wait_for_capacity(state));
                continue;
            };

            state.preflight_reservations = state
                .preflight_reservations
                .checked_add(1)
                .expect("preflight reservation count overflow");
            observe_peaks(&mut state);
            return PreflightPermit {
                manager: Arc::clone(self),
                victim,
                request: Some(request),
                active: true,
            };
        }
    }

    fn observe_preflight_open(&self) {
        let mut state = self.lock_state();
        state.live_descriptors = state
            .live_descriptors
            .checked_add(1)
            .expect("preflight live descriptor count overflow");
        state.live_preflight_descriptors = state
            .live_preflight_descriptors
            .checked_add(1)
            .expect("live preflight descriptor count overflow");
        observe_peaks(&mut state);
    }

    fn finish_preflight_open(&self, verified: bool) {
        let mut state = self.lock_state();
        state.live_descriptors = state
            .live_descriptors
            .checked_sub(1)
            .expect("preflight close must own one live descriptor");
        state.live_preflight_descriptors = state
            .live_preflight_descriptors
            .checked_sub(1)
            .expect("preflight close must own one live preflight descriptor");
        if verified {
            state.counters.descriptor_opens = state.counters.descriptor_opens.saturating_add(1);
            state.counters.descriptor_closes = state.counters.descriptor_closes.saturating_add(1);
        } else {
            state.counters.open_failures = state.counters.open_failures.saturating_add(1);
        }
        drop(state);
        self.capacity_changed.notify_all();
    }

    fn observe_preflight_open_failure(&self) {
        let mut state = self.lock_state();
        state.counters.open_failures = state.counters.open_failures.saturating_add(1);
    }

    fn release_preflight_slot(&self) {
        let mut state = self.lock_state();
        state.preflight_reservations = state
            .preflight_reservations
            .checked_sub(1)
            .expect("preflight reservation release underflow");
        state.occupied_slots = state
            .occupied_slots
            .checked_sub(1)
            .expect("preflight slot release underflow");
        drop(state);
        self.capacity_changed.notify_all();
    }

    fn finish_preflight_request(&self, segment_identity: &Arc<str>) {
        let mut state = self.lock_state();
        decrement_segment_activity(
            &mut state.active_preflights_by_segment,
            segment_identity,
            "preflight",
        );
        drop(state);
        self.capacity_changed.notify_all();
    }

    fn finish_acquisition_request(&self, segment_identities: &[Arc<str>]) {
        let mut state = self.lock_state();
        for segment_identity in segment_identities {
            decrement_segment_activity(
                &mut state.active_acquisitions_by_segment,
                segment_identity,
                "acquisition",
            );
        }
        drop(state);
        self.capacity_changed.notify_all();
    }

    fn finish_detached_closes(&self, victims: Vec<DetachedOpenFile>) {
        if victims.is_empty() {
            return;
        }

        #[cfg(test)]
        self.pause_before_detached_close_for_test(&victims);

        let mut completions = Vec::with_capacity(victims.len());
        for victim in victims {
            let DetachedOpenFile {
                key,
                file,
                slot_disposition,
            } = victim;
            drop(file);
            completions.push((key.segment_identity, slot_disposition));
        }

        let closed = u32::try_from(completions.len()).unwrap_or(u32::MAX);
        let released_slots = u32::try_from(
            completions
                .iter()
                .filter(|(_, disposition)| *disposition == DetachedSlotDisposition::Release)
                .count(),
        )
        .unwrap_or(u32::MAX);
        let mut state = self.lock_state();
        state.live_descriptors = state
            .live_descriptors
            .checked_sub(closed)
            .expect("detached descriptors must remain live until close accounting");
        state.occupied_slots = state
            .occupied_slots
            .checked_sub(released_slots)
            .expect("released detached descriptors must own hard-cap slots");
        state.counters.descriptor_closes = state
            .counters
            .descriptor_closes
            .saturating_add(u64::from(closed));
        for (segment_identity, _) in completions {
            decrement_segment_activity(
                &mut state.detached_closing_by_segment,
                &segment_identity,
                "detached close",
            );
        }
        drop(state);
        self.capacity_changed.notify_all();
    }

    fn wait_for_joined_retirement(&self, segment_identity: &Arc<str>) {
        let mut state = self.lock_state();
        loop {
            let completed = state
                .retirements
                .get(segment_identity)
                .expect("joined retirement must retain its marker")
                .completed;
            if completed {
                release_retirement_caller(&mut state, segment_identity);
                drop(state);
                self.capacity_changed.notify_all();
                return;
            }
            state = self.wait_for_capacity(state);
        }
    }

    #[cfg(test)]
    fn pause_before_open_for_test(&self, handle: &SegmentFileHandle) {
        let hook = self
            .before_open_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook
            && hook.segment_identity.as_ref() == handle.segment_identity()
        {
            hook.entered.wait();
            hook.resume.wait();
        }
    }

    #[cfg(test)]
    fn pause_before_detached_close_for_test(&self, victims: &[DetachedOpenFile]) {
        let hook = self
            .detached_close_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook
            && victims.iter().any(|victim| {
                victim.key.segment_identity.as_ref() == hook.segment_identity.as_ref()
            })
        {
            hook.detached.wait();
            hook.resume.wait();
        }
    }

    fn acquire_many_with_mode(
        self: &Arc<Self>,
        handles: &[SegmentFileHandle],
        mode: AcquireMode,
    ) -> Result<GovernedFileLeaseSet, MetadataFileManagerError> {
        let requested_handle_count = handles.len();
        let handles = normalize_handles(handles)?;
        let requested = u32::try_from(handles.len()).unwrap_or(u32::MAX);
        let segment_identities = handles
            .iter()
            .map(|handle| Arc::clone(&handle.inner.key.segment_identity))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        {
            let mut state = self.lock_state();
            state.counters.acquire_calls = state.counters.acquire_calls.saturating_add(1);
            state.counters.requested_handles = state
                .counters
                .requested_handles
                .saturating_add(u64::try_from(requested_handle_count).unwrap_or(u64::MAX));
            state.counters.deduplicated_handles = state
                .counters
                .deduplicated_handles
                .saturating_add(u64::try_from(handles.len()).unwrap_or(u64::MAX));
            if requested > self.max_open_files {
                state.counters.capacity_refusals =
                    state.counters.capacity_refusals.saturating_add(1);
                return Err(MetadataFileManagerError::RequestExceedsOpenFileLimit {
                    requested,
                    limit: self.max_open_files,
                });
            }
            if handles.is_empty() {
                state.counters.successful_acquires =
                    state.counters.successful_acquires.saturating_add(1);
                return Ok(GovernedFileLeaseSet { leases: Vec::new() });
            }
            for segment_identity in &segment_identities {
                if state.retirements.contains_key(segment_identity) {
                    return Err(segment_retiring_error(segment_identity));
                }
            }
            for segment_identity in &segment_identities {
                increment_segment_activity(
                    &mut state.active_acquisitions_by_segment,
                    Arc::clone(segment_identity),
                    "acquisition",
                );
            }
        }
        let _request = AcquisitionRequest {
            manager: Arc::clone(self),
            segment_identities,
            active: true,
        };

        loop {
            let prepared = {
                let mut state = self.lock_state();
                match self.prepare_acquisition(&mut state, &handles)? {
                    PrepareResult::Ready(prepared) => prepared,
                    PrepareResult::Wait {
                        requested_additional,
                    } => match mode {
                        AcquireMode::ReturnCapacityError => {
                            state.counters.capacity_refusals =
                                state.counters.capacity_refusals.saturating_add(1);
                            return Err(MetadataFileManagerError::OpenFileCapacityUnavailable {
                                requested_additional,
                                occupied: state.occupied_slots,
                                limit: self.max_open_files,
                            });
                        }
                        AcquireMode::Wait => {
                            state.counters.capacity_waits =
                                state.counters.capacity_waits.saturating_add(1);
                            drop(self.wait_for_capacity(state));
                            continue;
                        }
                    },
                }
            };
            return self.complete_acquisition(handles.clone(), prepared);
        }
    }

    fn prepare_acquisition(
        &self,
        state: &mut FileManagerState,
        handles: &[SegmentFileHandle],
    ) -> Result<PrepareResult, MetadataFileManagerError> {
        let requested_keys: BTreeSet<_> =
            handles.iter().map(|handle| handle.key().clone()).collect();
        let mut existing = Vec::new();
        let mut missing = Vec::new();

        for handle in handles {
            if state
                .retirements
                .contains_key(&handle.inner.key.segment_identity)
            {
                return Err(segment_retiring_error(&handle.inner.key.segment_identity));
            }
            match state.entries.get(handle.key()) {
                Some(FileEntry::Open {
                    handle: registered, ..
                }) => {
                    ensure_same_handle(registered, handle)?;
                    existing.push(handle.key().clone());
                }
                Some(FileEntry::Opening {
                    handle: registered, ..
                }) => {
                    ensure_same_handle(registered, handle)?;
                    return Ok(PrepareResult::Wait {
                        requested_additional: u32::try_from(handles.len()).unwrap_or(u32::MAX),
                    });
                }
                None => missing.push(handle.clone()),
            }
        }

        let free_slots = self.max_open_files.saturating_sub(state.occupied_slots);
        let needed = u32::try_from(missing.len()).unwrap_or(u32::MAX);
        let victims_needed = needed.saturating_sub(free_slots);
        let victim_keys: Vec<_> = state
            .idle_lru
            .iter()
            .filter(|key| !requested_keys.contains(*key))
            .filter(|key| {
                matches!(
                    state.entries.get(*key),
                    Some(FileEntry::Open { leases: 0, .. })
                )
            })
            .take(usize::try_from(victims_needed).unwrap_or(usize::MAX))
            .cloned()
            .collect();
        if victim_keys.len() < usize::try_from(victims_needed).unwrap_or(usize::MAX) {
            return Ok(PrepareResult::Wait {
                requested_additional: needed,
            });
        }

        state.next_operation_id = state.next_operation_id.wrapping_add(1);
        let operation_id = state.next_operation_id;

        for key in &existing {
            remove_idle_key(&mut state.idle_lru, key);
            if let Some(FileEntry::Open { leases, .. }) = state.entries.get_mut(key) {
                *leases = leases
                    .checked_add(1)
                    .expect("governed file lease count overflow");
            }
        }

        let mut victims = Vec::with_capacity(victim_keys.len());
        for key in &victim_keys {
            remove_idle_key(&mut state.idle_lru, key);
            match state.entries.remove(key) {
                Some(FileEntry::Open {
                    file, leases: 0, ..
                }) => victims.push(track_detached_close(
                    state,
                    key.clone(),
                    file,
                    DetachedSlotDisposition::Transfer,
                )),
                _ => unreachable!("selected idle victim must remain idle under manager lock"),
            }
        }

        state.occupied_slots = state
            .occupied_slots
            .checked_add(needed.saturating_sub(victims_needed))
            .expect("governed open-file slot count overflow");
        for handle in &missing {
            state.entries.insert(
                handle.key().clone(),
                FileEntry::Opening {
                    handle: handle.clone(),
                    operation_id,
                },
            );
        }
        state.counters.idle_evictions = state
            .counters
            .idle_evictions
            .saturating_add(u64::try_from(victims.len()).unwrap_or(u64::MAX));
        observe_peaks(state);

        Ok(PrepareResult::Ready(PreparedAcquisition {
            operation_id,
            existing_keys: existing,
            missing_handles: missing,
            victims,
        }))
    }

    fn complete_acquisition(
        self: &Arc<Self>,
        handles: Vec<SegmentFileHandle>,
        mut prepared: PreparedAcquisition,
    ) -> Result<GovernedFileLeaseSet, MetadataFileManagerError> {
        self.finish_detached_closes(std::mem::take(&mut prepared.victims));

        let mut opened = Vec::with_capacity(prepared.missing_handles.len());
        for handle in &prepared.missing_handles {
            #[cfg(test)]
            self.pause_before_open_for_test(handle);
            match open_verified(handle) {
                Ok(file) => {
                    let instance_id = self.next_file_instance_id.fetch_add(1, Ordering::Relaxed);
                    opened.push((
                        handle.key().clone(),
                        Arc::new(GovernedOpenFile { instance_id, file }),
                    ));
                    let mut state = self.lock_state();
                    state.live_descriptors = state
                        .live_descriptors
                        .checked_add(1)
                        .expect("governed live descriptor count overflow");
                    state.pending_open_descriptors = state
                        .pending_open_descriptors
                        .checked_add(1)
                        .expect("governed pending descriptor count overflow");
                    state.counters.descriptor_opens =
                        state.counters.descriptor_opens.saturating_add(1);
                    observe_peaks(&mut state);
                }
                Err(error) => {
                    let opened_count = u64::try_from(opened.len()).unwrap_or(u64::MAX);
                    drop(opened);
                    self.rollback_acquisition(&prepared, opened_count, &error);
                    return Err(error);
                }
            }
        }

        let lease_files = {
            let mut state = self.lock_state();
            for (key, file) in &opened {
                let previous = state.entries.remove(key);
                match previous {
                    Some(FileEntry::Opening { operation_id, .. })
                        if operation_id == prepared.operation_id => {}
                    _ => unreachable!("opening reservation must belong to publishing operation"),
                }
                let handle = prepared
                    .missing_handles
                    .iter()
                    .find(|handle| handle.key() == key)
                    .expect("opened file must have a matching handle")
                    .clone();
                state.entries.insert(
                    key.clone(),
                    FileEntry::Open {
                        handle,
                        file: Arc::clone(file),
                        leases: 1,
                    },
                );
            }

            state.pending_open_descriptors = state
                .pending_open_descriptors
                .checked_sub(u32::try_from(opened.len()).unwrap_or(u32::MAX))
                .expect("published files must own pending descriptors");
            state.counters.descriptor_reuses = state
                .counters
                .descriptor_reuses
                .saturating_add(u64::try_from(prepared.existing_keys.len()).unwrap_or(u64::MAX));
            state.counters.successful_acquires =
                state.counters.successful_acquires.saturating_add(1);
            observe_peaks(&mut state);

            handles
                .iter()
                .map(|handle| match state.entries.get(handle.key()) {
                    Some(FileEntry::Open { file, .. }) => Arc::clone(file),
                    _ => unreachable!("published acquisition must contain every requested file"),
                })
                .collect::<Vec<_>>()
        };
        self.capacity_changed.notify_all();

        let leases = handles
            .into_iter()
            .zip(lease_files)
            .map(|(handle, file)| GovernedFileLease {
                manager: Arc::clone(self),
                key: handle.key().clone(),
                handle,
                file: Some(file),
            })
            .collect();
        Ok(GovernedFileLeaseSet { leases })
    }

    fn rollback_acquisition(
        &self,
        prepared: &PreparedAcquisition,
        opened_count: u64,
        error: &MetadataFileManagerError,
    ) {
        let victims = {
            let mut state = self.lock_state();
            for handle in &prepared.missing_handles {
                match state.entries.remove(handle.key()) {
                    Some(FileEntry::Opening { operation_id, .. })
                        if operation_id == prepared.operation_id =>
                    {
                        state.occupied_slots = state
                            .occupied_slots
                            .checked_sub(1)
                            .expect("opening rollback must own one hard-cap slot");
                    }
                    _ => unreachable!("rollback must own every opening reservation"),
                }
            }
            for key in &prepared.existing_keys {
                release_pending_lease(&mut state, key);
            }
            let opened_count_u32 = u32::try_from(opened_count).unwrap_or(u32::MAX);
            state.live_descriptors = state
                .live_descriptors
                .checked_sub(opened_count_u32)
                .expect("rolled-back files must own live descriptors");
            state.pending_open_descriptors = state
                .pending_open_descriptors
                .checked_sub(opened_count_u32)
                .expect("rolled-back files must own pending descriptors");
            state.counters.descriptor_closes = state
                .counters
                .descriptor_closes
                .saturating_add(opened_count);
            state.counters.open_failures = state.counters.open_failures.saturating_add(1);
            state.counters.acquisition_rollbacks =
                state.counters.acquisition_rollbacks.saturating_add(1);
            if error.is_structural() {
                state.counters.structural_replacements =
                    state.counters.structural_replacements.saturating_add(1);
            }
            let victims = trim_idle_cache(self.max_cached_open_files, &mut state);
            observe_peaks(&mut state);
            victims
        };
        self.finish_detached_closes(victims);
        self.capacity_changed.notify_all();
    }

    fn clone_lease(&self, key: &SegmentFileKey, instance_id: u64) {
        let mut state = self.lock_state();
        match state.entries.get_mut(key) {
            Some(FileEntry::Open { file, leases, .. }) if file.instance_id == instance_id => {
                *leases = leases
                    .checked_add(1)
                    .expect("governed file lease count overflow");
            }
            _ => unreachable!("a live lease must retain its keyed open-file entry"),
        }
        state.counters.lease_clones = state.counters.lease_clones.saturating_add(1);
        observe_peaks(&mut state);
    }

    fn release_lease(
        &self,
        key: &SegmentFileKey,
        instance_id: u64,
        lease_file: Arc<GovernedOpenFile>,
    ) {
        // The keyed manager entry remains authoritative while its lease count
        // is nonzero. Destroy this lease's Arc before decrementing that count,
        // so publishing the final zero-lease entry can never race an eviction
        // while an unaccounted lease-owned Arc still keeps the descriptor open.
        drop(lease_file);
        #[cfg(test)]
        let release_hook = self
            .release_lease_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        #[cfg(test)]
        if let Some(hook) = release_hook {
            hook.arc_dropped.wait();
            hook.resume.wait();
        }
        let (victims, reached_zero_leases) = {
            let mut state = self.lock_state();
            let retiring = state.retirements.contains_key(&key.segment_identity);
            let reached_zero_leases = match state.entries.get_mut(key) {
                Some(FileEntry::Open { file, leases, .. }) if file.instance_id == instance_id => {
                    *leases = leases
                        .checked_sub(1)
                        .expect("governed file lease release underflow");
                    *leases == 0
                }
                _ => unreachable!("a live lease must release its keyed open-file entry"),
            };
            let mut victims = Vec::new();
            if reached_zero_leases {
                if retiring {
                    match state.entries.remove(key) {
                        Some(FileEntry::Open {
                            file, leases: 0, ..
                        }) => victims.push(track_detached_close(
                            &mut state,
                            key.clone(),
                            file,
                            DetachedSlotDisposition::Release,
                        )),
                        _ => unreachable!("retiring final lease must own one open entry"),
                    }
                } else {
                    state.idle_lru.push_back(key.clone());
                }
            }
            victims.extend(trim_idle_cache(self.max_cached_open_files, &mut state));
            observe_peaks(&mut state);
            (victims, reached_zero_leases)
        };

        self.finish_detached_closes(victims);
        if reached_zero_leases {
            self.capacity_changed.notify_all();
        }
    }

    fn wait_for_capacity<'a>(
        &self,
        state: MutexGuard<'a, FileManagerState>,
    ) -> MutexGuard<'a, FileManagerState> {
        self.capacity_changed
            .wait(state)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_state(&self) -> MutexGuard<'_, FileManagerState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug, Clone, Copy)]
enum AcquireMode {
    Wait,
    ReturnCapacityError,
}

#[derive(Debug)]
enum PrepareResult {
    Ready(PreparedAcquisition),
    Wait { requested_additional: u32 },
}

#[derive(Debug)]
struct PreparedAcquisition {
    operation_id: u64,
    existing_keys: Vec<SegmentFileKey>,
    missing_handles: Vec<SegmentFileHandle>,
    victims: Vec<DetachedOpenFile>,
}

struct PreflightPermit {
    manager: Arc<MetadataFileManager>,
    victim: Option<DetachedOpenFile>,
    request: Option<PreflightRequest>,
    active: bool,
}

impl PreflightPermit {
    fn open(
        mut self,
        segment_identity: &Arc<str>,
        file: SegmentFile,
        path: &Path,
    ) -> Result<PreflightOpen, MetadataFileManagerError> {
        self.close_victim();
        let opened = match open_immutable(path) {
            Ok(opened) => opened,
            Err(source) => {
                self.manager.observe_preflight_open_failure();
                return Err(classify_open_failure(segment_identity, file, path, source));
            }
        };
        self.manager.observe_preflight_open();
        Ok(PreflightOpen {
            permit: Some(self),
            file: Some(opened),
            verified: false,
        })
    }

    fn close_victim(&mut self) {
        if let Some(victim) = self.victim.take() {
            self.manager.finish_detached_closes(vec![victim]);
        }
    }
}

impl Drop for PreflightPermit {
    fn drop(&mut self) {
        self.close_victim();
        if self.active {
            self.active = false;
            self.manager.release_preflight_slot();
        }
        drop(self.request.take());
    }
}

struct PreflightRequest {
    manager: Arc<MetadataFileManager>,
    segment_identity: Arc<str>,
    active: bool,
}

impl Drop for PreflightRequest {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.manager
                .finish_preflight_request(&self.segment_identity);
        }
    }
}

struct AcquisitionRequest {
    manager: Arc<MetadataFileManager>,
    segment_identities: Vec<Arc<str>>,
    active: bool,
}

impl Drop for AcquisitionRequest {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.manager
                .finish_acquisition_request(&self.segment_identities);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetachedSlotDisposition {
    Transfer,
    Release,
}

#[derive(Debug)]
struct DetachedOpenFile {
    key: SegmentFileKey,
    file: Arc<GovernedOpenFile>,
    slot_disposition: DetachedSlotDisposition,
}

struct PreflightOpen {
    // The file is explicitly destroyed before the permit releases its hard
    // slot, including on validation errors and unwinding.
    permit: Option<PreflightPermit>,
    file: Option<File>,
    verified: bool,
}

impl PreflightOpen {
    fn file(&self) -> &File {
        self.file
            .as_ref()
            .expect("preflight file remains present until Drop")
    }

    fn mark_verified(&mut self) {
        self.verified = true;
    }
}

impl Drop for PreflightOpen {
    fn drop(&mut self) {
        drop(self.file.take());
        let permit = self
            .permit
            .take()
            .expect("preflight open must retain its hard-cap permit");
        permit.manager.finish_preflight_open(self.verified);
        drop(permit);
    }
}

/// A positional-read lease on one governed descriptor.
pub struct GovernedFileLease {
    manager: Arc<MetadataFileManager>,
    key: SegmentFileKey,
    handle: SegmentFileHandle,
    file: Option<Arc<GovernedOpenFile>>,
}

impl GovernedFileLease {
    pub fn handle(&self) -> &SegmentFileHandle {
        &self.handle
    }

    pub fn len(&self) -> u64 {
        self.handle.expected_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn open_instance_id(&self) -> u64 {
        self.open_file().instance_id
    }

    pub fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        SegmentIndexReadAt::read_exact_at(&self.open_file().file, offset, destination)
    }

    pub(crate) fn verify_registered_shape(&self) -> Result<(), MetadataFileManagerError> {
        verify_opened_file(&self.handle, &self.open_file().file)
    }

    fn open_file(&self) -> &GovernedOpenFile {
        self.file
            .as_deref()
            .expect("governed lease file is present until Drop")
    }
}

impl Clone for GovernedFileLease {
    fn clone(&self) -> Self {
        let file = Arc::clone(
            self.file
                .as_ref()
                .expect("governed lease file is present until Drop"),
        );
        self.manager
            .clone_lease(&self.key, self.open_file().instance_id);
        Self {
            manager: Arc::clone(&self.manager),
            key: self.key.clone(),
            handle: self.handle.clone(),
            file: Some(file),
        }
    }
}

impl fmt::Debug for GovernedFileLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernedFileLease")
            .field("handle", &self.handle)
            .field("open_instance_id", &self.open_instance_id())
            .finish_non_exhaustive()
    }
}

impl Drop for GovernedFileLease {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let instance_id = file.instance_id;
            self.manager.release_lease(&self.key, instance_id, file);
        }
    }
}

impl SegmentIndexReadAt for GovernedFileLease {
    fn len(&self) -> io::Result<u64> {
        Ok(self.len())
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        self.read_exact_at(offset, destination)
    }
}

#[cfg(unix)]
impl std::os::fd::AsRawFd for GovernedFileLease {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.open_file().file)
    }
}

#[derive(Debug, Default)]
pub struct GovernedFileLeaseSet {
    leases: Vec<GovernedFileLease>,
}

impl GovernedFileLeaseSet {
    pub fn into_leases(self) -> Vec<GovernedFileLease> {
        self.leases
    }
}

impl Deref for GovernedFileLeaseSet {
    type Target = [GovernedFileLease];

    fn deref(&self) -> &Self::Target {
        &self.leases
    }
}

impl IntoIterator for GovernedFileLeaseSet {
    type Item = GovernedFileLease;
    type IntoIter = std::vec::IntoIter<GovernedFileLease>;

    fn into_iter(self) -> Self::IntoIter {
        self.leases.into_iter()
    }
}

fn normalize_handles(
    handles: &[SegmentFileHandle],
) -> Result<Vec<SegmentFileHandle>, MetadataFileManagerError> {
    let mut normalized = BTreeMap::<SegmentFileKey, SegmentFileHandle>::new();
    for handle in handles {
        match normalized.get(handle.key()) {
            Some(existing) => ensure_same_handle(existing, handle)?,
            None => {
                normalized.insert(handle.key().clone(), handle.clone());
            }
        }
    }
    Ok(normalized.into_values().collect())
}

fn ensure_same_handle(
    first: &SegmentFileHandle,
    second: &SegmentFileHandle,
) -> Result<(), MetadataFileManagerError> {
    if first.same_definition(second) {
        return Ok(());
    }
    Err(MetadataFileManagerError::ConflictingHandle {
        segment_identity: Arc::clone(&first.inner.key.segment_identity),
        file: first.file(),
        first_path: first.path().to_path_buf(),
        second_path: second.path().to_path_buf(),
    })
}

fn release_pending_lease(state: &mut FileManagerState, key: &SegmentFileKey) {
    let became_idle = match state.entries.get_mut(key) {
        Some(FileEntry::Open { leases, .. }) => {
            *leases = leases
                .checked_sub(1)
                .expect("pending lease rollback underflow");
            *leases == 0
        }
        _ => unreachable!("pending lease rollback requires an open entry"),
    };
    if became_idle {
        state.idle_lru.push_back(key.clone());
    }
}

fn trim_idle_cache(
    max_cached_open_files: u32,
    state: &mut FileManagerState,
) -> Vec<DetachedOpenFile> {
    let mut idle_count = state
        .entries
        .values()
        .filter(|entry| matches!(entry, FileEntry::Open { leases: 0, .. }))
        .count();
    let max_cached = usize::try_from(max_cached_open_files).unwrap_or(usize::MAX);
    let mut victims = Vec::new();
    while idle_count > max_cached {
        let Some(key) = state.idle_lru.pop_front() else {
            break;
        };
        match state.entries.remove(&key) {
            Some(FileEntry::Open {
                file, leases: 0, ..
            }) => {
                victims.push(track_detached_close(
                    state,
                    key,
                    file,
                    DetachedSlotDisposition::Release,
                ));
                idle_count -= 1;
                state.counters.idle_evictions = state.counters.idle_evictions.saturating_add(1);
            }
            Some(entry) => {
                state.entries.insert(key, entry);
            }
            None => {}
        }
    }
    victims
}

fn detach_idle_segment_files(
    state: &mut FileManagerState,
    segment_identity: &Arc<str>,
) -> Vec<DetachedOpenFile> {
    let keys = state
        .entries
        .iter()
        .filter(|(key, entry)| {
            key.segment_identity == *segment_identity
                && matches!(entry, FileEntry::Open { leases: 0, .. })
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let mut victims = Vec::with_capacity(keys.len());
    for key in keys {
        remove_idle_key(&mut state.idle_lru, &key);
        match state.entries.remove(&key) {
            Some(FileEntry::Open {
                file, leases: 0, ..
            }) => {
                state.counters.idle_evictions = state.counters.idle_evictions.saturating_add(1);
                victims.push(track_detached_close(
                    state,
                    key,
                    file,
                    DetachedSlotDisposition::Release,
                ));
            }
            _ => unreachable!("retirement-selected idle file must remain idle"),
        }
    }
    victims
}

fn track_detached_close(
    state: &mut FileManagerState,
    key: SegmentFileKey,
    file: Arc<GovernedOpenFile>,
    slot_disposition: DetachedSlotDisposition,
) -> DetachedOpenFile {
    increment_segment_activity(
        &mut state.detached_closing_by_segment,
        Arc::clone(&key.segment_identity),
        "detached close",
    );
    DetachedOpenFile {
        key,
        file,
        slot_disposition,
    }
}

fn segment_has_file_manager_activity(
    state: &FileManagerState,
    segment_identity: &Arc<str>,
) -> bool {
    state
        .active_preflights_by_segment
        .get(segment_identity)
        .is_some_and(|count| *count != 0)
        || state
            .active_acquisitions_by_segment
            .get(segment_identity)
            .is_some_and(|count| *count != 0)
        || state
            .detached_closing_by_segment
            .get(segment_identity)
            .is_some_and(|count| *count != 0)
        || state
            .entries
            .keys()
            .any(|key| key.segment_identity == *segment_identity)
}

fn increment_segment_activity(
    activity: &mut BTreeMap<Arc<str>, u32>,
    segment_identity: Arc<str>,
    description: &str,
) {
    let count = activity.entry(segment_identity).or_default();
    *count = count
        .checked_add(1)
        .unwrap_or_else(|| panic!("segment {description} count overflow"));
}

fn decrement_segment_activity(
    activity: &mut BTreeMap<Arc<str>, u32>,
    segment_identity: &Arc<str>,
    description: &str,
) {
    let remaining = {
        let count = activity
            .get_mut(segment_identity)
            .unwrap_or_else(|| panic!("missing segment {description} count"));
        *count = count
            .checked_sub(1)
            .unwrap_or_else(|| panic!("segment {description} count underflow"));
        *count
    };
    if remaining == 0 {
        activity.remove(segment_identity);
    }
}

fn release_retirement_caller(state: &mut FileManagerState, segment_identity: &Arc<str>) {
    let remove = {
        let retirement = state
            .retirements
            .get_mut(segment_identity)
            .expect("retirement caller must retain its marker");
        retirement.callers = retirement
            .callers
            .checked_sub(1)
            .expect("segment retirement caller count underflow");
        retirement.callers == 0
    };
    if remove {
        state.retirements.remove(segment_identity);
    }
}

fn segment_retiring_error(segment_identity: &Arc<str>) -> MetadataFileManagerError {
    MetadataFileManagerError::SegmentRetiring {
        segment_identity: Arc::clone(segment_identity),
    }
}

fn remove_idle_key(idle_lru: &mut VecDeque<SegmentFileKey>, key: &SegmentFileKey) {
    if let Some(position) = idle_lru.iter().position(|candidate| candidate == key) {
        idle_lru.remove(position);
    }
}

fn snapshot(manager: &MetadataFileManager, state: &FileManagerState) -> MetadataFileManagerStats {
    let shape = current_shape(state);
    MetadataFileManagerStats {
        max_open_files: manager.max_open_files,
        max_cached_open_files: manager.max_cached_open_files,
        open_files: state.live_descriptors,
        occupied_open_slots: state.occupied_slots,
        active_open_files: shape.active_open_files,
        cached_open_files: shape.cached_open_files,
        opening_files: shape.opening_files,
        pending_open_files: state.pending_open_descriptors,
        preflighting_files: state.preflight_reservations,
        closing_files: state.live_descriptors.saturating_sub(
            shape
                .open_entries
                .saturating_add(state.pending_open_descriptors)
                .saturating_add(state.live_preflight_descriptors),
        ),
        active_leases: shape.active_leases,
        peak_open_files: state.counters.peak_open_files,
        peak_occupied_open_slots: state.counters.peak_occupied_open_slots,
        peak_active_open_files: state.counters.peak_active_open_files,
        peak_cached_open_files: state.counters.peak_cached_open_files,
        peak_active_leases: state.counters.peak_active_leases,
        peak_preflighting_files: state.counters.peak_preflighting_files,
        preflight_calls: state.counters.preflight_calls,
        successful_preflights: state.counters.successful_preflights,
        preflight_failures: state.counters.preflight_failures,
        acquire_calls: state.counters.acquire_calls,
        successful_acquires: state.counters.successful_acquires,
        requested_handles: state.counters.requested_handles,
        deduplicated_handles: state.counters.deduplicated_handles,
        descriptor_opens: state.counters.descriptor_opens,
        descriptor_closes: state.counters.descriptor_closes,
        descriptor_reuses: state.counters.descriptor_reuses,
        lease_clones: state.counters.lease_clones,
        idle_evictions: state.counters.idle_evictions,
        capacity_waits: state.counters.capacity_waits,
        capacity_refusals: state.counters.capacity_refusals,
        open_failures: state.counters.open_failures,
        structural_replacements: state.counters.structural_replacements,
        acquisition_rollbacks: state.counters.acquisition_rollbacks,
    }
}

#[derive(Debug, Default)]
struct CurrentShape {
    open_entries: u32,
    active_open_files: u32,
    cached_open_files: u32,
    opening_files: u32,
    active_leases: u32,
}

fn current_shape(state: &FileManagerState) -> CurrentShape {
    let mut shape = CurrentShape {
        opening_files: state.preflight_reservations,
        ..CurrentShape::default()
    };
    for entry in state.entries.values() {
        match entry {
            FileEntry::Opening { .. } => {
                shape.opening_files = shape.opening_files.saturating_add(1);
            }
            FileEntry::Open { leases, .. } => {
                shape.open_entries = shape.open_entries.saturating_add(1);
                shape.active_leases = shape.active_leases.saturating_add(*leases);
                if *leases == 0 {
                    shape.cached_open_files = shape.cached_open_files.saturating_add(1);
                } else {
                    shape.active_open_files = shape.active_open_files.saturating_add(1);
                }
            }
        }
    }
    shape
}

fn observe_peaks(state: &mut FileManagerState) {
    let shape = current_shape(state);
    state.counters.peak_open_files = state.counters.peak_open_files.max(state.live_descriptors);
    state.counters.peak_occupied_open_slots = state
        .counters
        .peak_occupied_open_slots
        .max(state.occupied_slots);
    state.counters.peak_active_open_files = state
        .counters
        .peak_active_open_files
        .max(shape.active_open_files);
    state.counters.peak_cached_open_files = state
        .counters
        .peak_cached_open_files
        .max(shape.cached_open_files);
    state.counters.peak_active_leases = state.counters.peak_active_leases.max(shape.active_leases);
    state.counters.peak_preflighting_files = state
        .counters
        .peak_preflighting_files
        .max(state.preflight_reservations);
}

fn open_verified(handle: &SegmentFileHandle) -> Result<File, MetadataFileManagerError> {
    let file = open_immutable(handle.path()).map_err(|source| {
        classify_open_failure(
            &handle.inner.key.segment_identity,
            handle.file(),
            handle.path(),
            source,
        )
    })?;
    verify_opened_file(handle, &file)?;
    Ok(file)
}

fn verify_opened_file(
    handle: &SegmentFileHandle,
    file: &File,
) -> Result<(), MetadataFileManagerError> {
    let metadata = file
        .metadata()
        .map_err(|source| MetadataFileManagerError::Open {
            path: handle.path().to_path_buf(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(structural_replacement(
            &handle.inner.key.segment_identity,
            handle.file(),
            handle.path(),
            StructuralFileChange::NotRegular,
        ));
    }
    if metadata.len() != handle.expected_len() {
        return Err(structural_replacement(
            &handle.inner.key.segment_identity,
            handle.file(),
            handle.path(),
            StructuralFileChange::Length {
                expected: handle.expected_len(),
                actual: metadata.len(),
            },
        ));
    }
    let actual_identity = platform_file_identity(&metadata)?;
    if actual_identity != handle.expected_identity() {
        return Err(structural_replacement(
            &handle.inner.key.segment_identity,
            handle.file(),
            handle.path(),
            StructuralFileChange::Identity {
                expected: handle.expected_identity(),
                actual: actual_identity,
            },
        ));
    }
    Ok(())
}

fn structural_replacement(
    segment_identity: &Arc<str>,
    file: SegmentFile,
    path: &Path,
    change: StructuralFileChange,
) -> MetadataFileManagerError {
    MetadataFileManagerError::StructuralReplacement {
        segment_identity: Arc::clone(segment_identity),
        file,
        path: path.to_path_buf(),
        change,
    }
}

#[cfg(unix)]
pub(super) fn open_immutable(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        // O_NONBLOCK prevents a substituted FIFO/device from stalling before
        // fstat can reject it. It has no effect on regular-file reads.
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
pub(super) fn open_immutable(path: &Path) -> io::Result<File> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "governed metadata path is not a regular file",
        ));
    }
    OpenOptions::new().read(true).open(path)
}

fn classify_open_failure(
    segment_identity: &Arc<str>,
    file: SegmentFile,
    path: &Path,
    source: io::Error,
) -> MetadataFileManagerError {
    if let Some(change) = structural_open_failure(path, &source) {
        structural_replacement(segment_identity, file, path, change)
    } else {
        MetadataFileManagerError::Open {
            path: path.to_path_buf(),
            source,
        }
    }
}

fn structural_open_failure(path: &Path, source: &io::Error) -> Option<StructuralFileChange> {
    #[cfg(unix)]
    match source.raw_os_error() {
        Some(libc::ENOENT) => return Some(StructuralFileChange::Missing),
        Some(libc::ENOTDIR) => return Some(StructuralFileChange::PathComponentNotDirectory),
        Some(libc::ELOOP) => return Some(StructuralFileChange::SymbolicLink),
        // These failures do not prove that the immutable tracked object was
        // replaced. In particular, process/system descriptor exhaustion must
        // remain a retryable resource error rather than sticky corruption.
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::EINTR) | Some(libc::EIO)
        | Some(libc::EACCES) | Some(libc::EPERM) => return None,
        _ => {}
    }

    #[cfg(not(unix))]
    if source.kind() == io::ErrorKind::NotFound {
        return Some(StructuralFileChange::Missing);
    }

    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Some(StructuralFileChange::SymbolicLink)
        }
        Ok(metadata) if !metadata.file_type().is_file() => Some(StructuralFileChange::NotRegular),
        Ok(_) | Err(_) => None,
    }
}

#[cfg(unix)]
fn platform_file_identity(
    metadata: &fs::Metadata,
) -> Result<PlatformFileIdentity, MetadataFileManagerError> {
    use std::os::unix::fs::MetadataExt;

    Ok(PlatformFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn platform_file_identity(
    _metadata: &fs::Metadata,
) -> Result<PlatformFileIdentity, MetadataFileManagerError> {
    Err(MetadataFileManagerError::UnsupportedPlatformIdentity)
}

fn is_footer_tracked(file: SegmentFile) -> bool {
    !matches!(file, SegmentFile::Footer)
}

fn segment_file_rank(file: SegmentFile) -> u8 {
    match file {
        SegmentFile::MetaJson => 0,
        SegmentFile::Symbols => 1,
        SegmentFile::Series => 2,
        SegmentFile::Chunks => 3,
        SegmentFile::OooChunks => 4,
        SegmentFile::ChunkIndex => 5,
        SegmentFile::Indexes => 6,
        SegmentFile::Footer => 7,
    }
}

#[cfg(test)]
mod tests;
