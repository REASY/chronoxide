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
mod tests {
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
            SegmentFileHandle::preflight_unmanaged_for_test(
                "segment",
                SegmentFile::Footer,
                &path,
                6,
            ),
            Err(MetadataFileManagerError::UntrackedSegmentFile {
                file: SegmentFile::Footer
            })
        ));
        assert!(matches!(
            SegmentFileHandle::preflight_unmanaged_for_test(
                "segment",
                SegmentFile::Symbols,
                &path,
                7,
            ),
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
            SegmentFileHandle::preflight_unmanaged_for_test(
                "segment",
                SegmentFile::Symbols,
                link,
                8,
            ),
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
            let result =
                waiting_manager.preflight("waiting", SegmentFile::Symbols, waiting_path, 7);
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
}
