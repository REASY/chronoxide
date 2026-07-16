use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use thiserror::Error;

use super::{GovernedArtifactReader, MetadataReadCounters, MetadataReadStats};
use crate::storage::file_manager::{
    MetadataFileManager, MetadataFileManagerError, MetadataFileManagerStats, SegmentFileHandle,
};
use crate::storage::metadata_cache::{
    MetadataArtifactRegistrationError, MetadataArtifactRetirement, MetadataCache,
    MetadataCacheStats, MetadataSegmentIdentity,
};
use crate::storage::metadata_governor::{
    MetadataGovernor, MetadataGovernorConfig, MetadataGovernorConfigError, MetadataGovernorStats,
};
use crate::storage::segment::{SEGMENT_FOOTER_TRACKED_FILES, SegmentFile};

const TRACKED_FILE_COUNT: usize = SEGMENT_FOOTER_TRACKED_FILES.len();

/// One footer-authenticated path in a canonical segment inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentArtifactRegistration {
    file: SegmentFile,
    path: PathBuf,
    footer_recorded_len: u64,
}

impl SegmentArtifactRegistration {
    pub fn new(file: SegmentFile, path: impl Into<PathBuf>, footer_recorded_len: u64) -> Self {
        Self {
            file,
            path: path.into(),
            footer_recorded_len,
        }
    }

    pub fn file(&self) -> SegmentFile {
        self.file
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn footer_recorded_len(&self) -> u64 {
        self.footer_recorded_len
    }
}

/// Registration and lifecycle failures at the store-owned segment boundary.
#[derive(Debug, Error)]
pub enum StoreMetadataRuntimeError {
    #[error("stable segment identity must not be empty")]
    EmptySegmentIdentity,
    #[error(
        "canonical segment metadata inventory requires {expected} files, but received {actual}"
    )]
    InvalidArtifactCount { expected: usize, actual: usize },
    #[error(
        "noncanonical segment metadata inventory entry {index}: expected {expected:?}, found {actual:?}"
    )]
    NonCanonicalArtifact {
        index: usize,
        expected: SegmentFile,
        actual: SegmentFile,
    },
    #[error("conflicting registration for active segment {segment_identity}")]
    ConflictingRegistration { segment_identity: Arc<str> },
    #[error("segment {segment_identity} generation {generation} is not active")]
    SegmentNotActive {
        segment_identity: Arc<str>,
        generation: u64,
    },
    #[error("segment {segment_identity} is retiring")]
    SegmentRetiring { segment_identity: Arc<str> },
    #[error("segment {segment_identity} lifecycle failed: {message}")]
    LifecycleFailed {
        segment_identity: Arc<str>,
        message: Arc<str>,
    },
    #[error("segment lifecycle generation counter is exhausted")]
    GenerationExhausted,
    #[error(transparent)]
    FileManager(#[from] MetadataFileManagerError),
    #[error(transparent)]
    Cache(#[from] MetadataArtifactRegistrationError),
}

/// Store-wide metadata resources and the only segment-reader lifecycle owner.
#[derive(Clone)]
pub struct StoreMetadataRuntime {
    pub(super) inner: Arc<StoreMetadataRuntimeInner>,
}

pub(super) struct StoreMetadataRuntimeInner {
    pub(super) governor: Arc<MetadataGovernor>,
    pub(super) cache: MetadataCache,
    pub(super) files: Arc<MetadataFileManager>,
    pub(super) reads: MetadataReadCounters,
    lifecycle: Mutex<LifecycleState>,
    lifecycle_changed: Condvar,
    #[cfg(test)]
    registration_leader_test_hook: Mutex<Option<RegistrationPauseTestHook>>,
    #[cfg(test)]
    registration_join_wake_test_hook: Mutex<Option<RegistrationPauseTestHook>>,
    #[cfg(test)]
    registration_after_cache_test_hook: Mutex<Option<RegistrationPauseTestHook>>,
}

#[cfg(test)]
struct RegistrationPauseTestHook {
    entered: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
    panic_after_resume: bool,
}

#[derive(Default)]
struct LifecycleState {
    next_generation: u64,
    segments: BTreeMap<Arc<str>, SegmentLifecycle>,
}

enum SegmentLifecycle {
    Registering {
        generation: u64,
        definition: Arc<CanonicalInventory>,
        pending_owners: u32,
    },
    Active {
        generation: u64,
        definition: Arc<CanonicalInventory>,
        handles: Arc<[SegmentFileHandle; TRACKED_FILE_COUNT]>,
        owners: u32,
        readers: u32,
    },
    Retiring {
        generation: u64,
        handles: Arc<[SegmentFileHandle; TRACKED_FILE_COUNT]>,
        readers: u32,
    },
    Finalizing {
        generation: u64,
    },
    Failed {
        message: Arc<str>,
    },
}

#[derive(Debug, PartialEq, Eq)]
struct CanonicalInventory {
    artifacts: [SegmentArtifactRegistration; TRACKED_FILE_COUNT],
}

struct FinalizationTask {
    segment_identity: Arc<str>,
    generation: u64,
    handles: Arc<[SegmentFileHandle; TRACKED_FILE_COUNT]>,
}

impl StoreMetadataRuntime {
    pub fn new(config: MetadataGovernorConfig) -> Result<Self, MetadataGovernorConfigError> {
        let config = config.validate()?;
        let governor = MetadataGovernor::new(config)?;
        let cache = MetadataCache::new(Arc::clone(&governor));
        let files = MetadataFileManager::new(config)?;
        Ok(Self {
            inner: Arc::new(StoreMetadataRuntimeInner {
                governor,
                cache,
                files,
                reads: MetadataReadCounters::default(),
                lifecycle: Mutex::new(LifecycleState::default()),
                lifecycle_changed: Condvar::new(),
                #[cfg(test)]
                registration_leader_test_hook: Mutex::new(None),
                #[cfg(test)]
                registration_join_wake_test_hook: Mutex::new(None),
                #[cfg(test)]
                registration_after_cache_test_hook: Mutex::new(None),
            }),
        })
    }

    pub(super) fn from_inner(inner: Arc<StoreMetadataRuntimeInner>) -> Self {
        Self { inner }
    }

    pub fn governor(&self) -> Arc<MetadataGovernor> {
        Arc::clone(&self.inner.governor)
    }

    pub(super) fn cache(&self) -> MetadataCache {
        self.inner.cache.clone()
    }

    pub(super) fn file_manager(&self) -> Arc<MetadataFileManager> {
        Arc::clone(&self.inner.files)
    }

    pub fn evict_all_resident_metadata(&self) {
        self.inner.cache.evict_all_resident();
    }

    #[cfg(test)]
    fn pause_registration_leader_for_test(&self) {
        pause_registration_for_test(&self.inner.registration_leader_test_hook);
    }

    #[cfg(test)]
    fn pause_registration_join_wake_for_test(&self) {
        pause_registration_for_test(&self.inner.registration_join_wake_test_hook);
    }

    #[cfg(test)]
    fn pause_registration_after_cache_for_test(&self) {
        pause_registration_for_test(&self.inner.registration_after_cache_test_hook);
    }

    /// Registers one exact canonical footer inventory and returns an owner
    /// lease for its generation.
    ///
    /// Only one caller performs preflight and cache publication. Concurrent
    /// callers with the same definition wait and join the published generation;
    /// a different definition is rejected before any path is opened.
    pub fn register_segment(
        &self,
        segment_identity: impl Into<Arc<str>>,
        artifacts: &[SegmentArtifactRegistration],
    ) -> Result<RegisteredSegment, StoreMetadataRuntimeError> {
        let segment_identity = segment_identity.into();
        if segment_identity.is_empty() {
            return Err(StoreMetadataRuntimeError::EmptySegmentIdentity);
        }
        let definition = Arc::new(validate_inventory(artifacts)?);

        let mut reservation: Option<RegistrationReservation> = None;
        let generation = loop {
            let mut state = lock(&self.inner.lifecycle);
            match state.segments.get_mut(&segment_identity) {
                None => {
                    if let Some(stale) = reservation.take() {
                        stale.disarm();
                    }
                    let generation = state
                        .next_generation
                        .checked_add(1)
                        .ok_or(StoreMetadataRuntimeError::GenerationExhausted)?;
                    state.next_generation = generation;
                    state.segments.insert(
                        Arc::clone(&segment_identity),
                        SegmentLifecycle::Registering {
                            generation,
                            definition: Arc::clone(&definition),
                            pending_owners: 1,
                        },
                    );
                    reservation = Some(RegistrationReservation::new(
                        Arc::clone(&self.inner),
                        Arc::clone(&segment_identity),
                        generation,
                        true,
                    ));
                    drop(state);
                    #[cfg(test)]
                    self.pause_registration_leader_for_test();
                    break generation;
                }
                Some(SegmentLifecycle::Registering {
                    generation,
                    definition: existing,
                    pending_owners,
                }) => {
                    if **existing != *definition {
                        return Err(StoreMetadataRuntimeError::ConflictingRegistration {
                            segment_identity,
                        });
                    }
                    if reservation
                        .as_ref()
                        .map(RegistrationReservation::generation)
                        != Some(*generation)
                    {
                        if let Some(stale) = reservation.take() {
                            stale.disarm();
                        }
                        *pending_owners = pending_owners.checked_add(1).ok_or_else(|| {
                            StoreMetadataRuntimeError::LifecycleFailed {
                                segment_identity: Arc::clone(&segment_identity),
                                message: Arc::from(
                                    "pending registered-segment owner count overflow",
                                ),
                            }
                        })?;
                        reservation = Some(RegistrationReservation::new(
                            Arc::clone(&self.inner),
                            Arc::clone(&segment_identity),
                            *generation,
                            false,
                        ));
                    }
                    let state = wait(&self.inner.lifecycle_changed, state);
                    drop(state);
                    #[cfg(test)]
                    self.pause_registration_join_wake_for_test();
                }
                Some(SegmentLifecycle::Active {
                    generation,
                    definition: existing,
                    owners,
                    ..
                }) => {
                    if **existing != *definition {
                        return Err(StoreMetadataRuntimeError::ConflictingRegistration {
                            segment_identity,
                        });
                    }
                    if reservation
                        .as_ref()
                        .map(RegistrationReservation::generation)
                        == Some(*generation)
                    {
                        let registered = RegisteredSegment::new(
                            Arc::clone(&self.inner),
                            segment_identity,
                            *generation,
                        );
                        reservation
                            .take()
                            .expect("matching registration reservation exists")
                            .disarm();
                        return Ok(registered);
                    }
                    if let Some(stale) = reservation.take() {
                        stale.disarm();
                    }
                    *owners = owners.checked_add(1).ok_or_else(|| {
                        StoreMetadataRuntimeError::LifecycleFailed {
                            segment_identity: Arc::clone(&segment_identity),
                            message: Arc::from("registered-segment owner count overflow"),
                        }
                    })?;
                    return Ok(RegisteredSegment::new(
                        Arc::clone(&self.inner),
                        segment_identity,
                        *generation,
                    ));
                }
                Some(SegmentLifecycle::Retiring { .. })
                | Some(SegmentLifecycle::Finalizing { .. }) => {
                    return Err(StoreMetadataRuntimeError::SegmentRetiring { segment_identity });
                }
                Some(SegmentLifecycle::Failed { message, .. }) => {
                    return Err(StoreMetadataRuntimeError::LifecycleFailed {
                        segment_identity,
                        message: Arc::clone(message),
                    });
                }
            }
        };

        self.perform_registration(
            segment_identity,
            definition,
            generation,
            reservation.expect("registration leader retains its owner reservation"),
        )
    }

    fn perform_registration(
        &self,
        segment_identity: Arc<str>,
        definition: Arc<CanonicalInventory>,
        generation: u64,
        reservation: RegistrationReservation,
    ) -> Result<RegisteredSegment, StoreMetadataRuntimeError> {
        let mut transaction = RegistrationTransaction::new(
            Arc::clone(&self.inner),
            Arc::clone(&segment_identity),
            generation,
        );
        let mut handles = Vec::with_capacity(TRACKED_FILE_COUNT);
        for artifact in &definition.artifacts {
            let handle = self.inner.files.preflight(
                Arc::clone(&segment_identity),
                artifact.file,
                artifact.path.clone(),
                artifact.footer_recorded_len,
            )?;
            handles.push(handle);
        }

        if let Err(error) = self
            .inner
            .cache
            .register_artifacts(Arc::clone(&segment_identity), &SEGMENT_FOOTER_TRACKED_FILES)
        {
            return Err(error.into());
        }
        transaction.record_cache_registration();
        #[cfg(test)]
        self.pause_registration_after_cache_for_test();

        let handles: [SegmentFileHandle; TRACKED_FILE_COUNT] = match handles.try_into() {
            Ok(handles) => handles,
            Err(handles) => {
                drop(handles);
                return Err(StoreMetadataRuntimeError::LifecycleFailed {
                    segment_identity,
                    message: Arc::from("canonical preflight handle count changed"),
                });
            }
        };
        let handles = Arc::new(handles);
        let published = {
            let mut state = lock(&self.inner.lifecycle);
            let owners = match state.segments.get(&segment_identity) {
                Some(SegmentLifecycle::Registering {
                    generation: current,
                    definition: current_definition,
                    pending_owners,
                }) if *current == generation && **current_definition == *definition => {
                    Some(*pending_owners)
                }
                _ => None,
            };
            if let Some(owners) = owners {
                state.segments.insert(
                    Arc::clone(&segment_identity),
                    SegmentLifecycle::Active {
                        generation,
                        definition,
                        handles,
                        owners,
                        readers: 0,
                    },
                );
                transaction.commit();
                true
            } else {
                false
            }
        };
        self.inner.lifecycle_changed.notify_all();
        if !published {
            return Err(StoreMetadataRuntimeError::LifecycleFailed {
                segment_identity,
                message: Arc::from("registration marker changed before publication"),
            });
        }

        let registered =
            RegisteredSegment::new(Arc::clone(&self.inner), segment_identity, generation);
        reservation.disarm();
        Ok(registered)
    }

    /// Copies a point-in-time view of the runtime resource governors.
    pub fn snapshot(&self) -> StoreMetadataRuntimeSnapshot {
        let cache = self.inner.cache.stats();
        let files = self.inner.files.stats();
        let governor = self.inner.governor.stats();
        let reads = self.inner.reads.snapshot();
        StoreMetadataRuntimeSnapshot {
            governor,
            cache,
            files,
            reads,
        }
    }
}

/// Copied component statistics from one [`StoreMetadataRuntime`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreMetadataRuntimeSnapshot {
    pub governor: MetadataGovernorStats,
    pub cache: MetadataCacheStats,
    pub files: MetadataFileManagerStats,
    pub reads: MetadataReadStats,
}

/// Rolls back all registration side effects if publication does not commit.
struct RegistrationTransaction {
    inner: Arc<StoreMetadataRuntimeInner>,
    segment_identity: Arc<str>,
    generation: u64,
    cache_registered: bool,
    armed: bool,
}

impl RegistrationTransaction {
    fn new(
        inner: Arc<StoreMetadataRuntimeInner>,
        segment_identity: Arc<str>,
        generation: u64,
    ) -> Self {
        Self {
            inner,
            segment_identity,
            generation,
            cache_registered: false,
            armed: true,
        }
    }

    fn record_cache_registration(&mut self) {
        self.cache_registered = true;
    }

    fn commit(&mut self) {
        self.armed = false;
    }
}

impl Drop for RegistrationTransaction {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let file_error = self
            .inner
            .files
            .retire_segment(Arc::clone(&self.segment_identity))
            .err()
            .map(|error| error.to_string());
        let cache_error = if self.cache_registered {
            match self.inner.cache.retire_artifacts_after_inventory_removal(
                &self.segment_identity,
                &SEGMENT_FOOTER_TRACKED_FILES,
            ) {
                Ok(MetadataArtifactRetirement::Removed | MetadataArtifactRetirement::Deferred) => {
                    None
                }
                Ok(MetadataArtifactRetirement::NotRegistered) => Some(String::from(
                    "published cache inventory was not registered during rollback",
                )),
                Err(error) => Some(error.to_string()),
            }
        } else {
            None
        };
        let cleanup_error = match (file_error, cache_error) {
            (None, None) => None,
            (Some(file), None) => Some(Arc::from(format!(
                "registration rollback failed to retire files: {file}"
            ))),
            (None, Some(cache)) => Some(Arc::from(format!(
                "registration rollback failed to retire cache inventory: {cache}"
            ))),
            (Some(file), Some(cache)) => Some(Arc::from(format!(
                "registration rollback failed to retire files ({file}) and cache inventory ({cache})"
            ))),
        };

        let mut state = lock(&self.inner.lifecycle);
        let remove = matches!(
            state.segments.get(&self.segment_identity),
            Some(SegmentLifecycle::Registering {
                generation: current,
                ..
            }) if *current == self.generation
        );
        if remove {
            state.segments.remove(&self.segment_identity);
            if let Some(message) = cleanup_error {
                state.segments.insert(
                    Arc::clone(&self.segment_identity),
                    SegmentLifecycle::Failed { message },
                );
            }
        }
        drop(state);
        self.inner.lifecycle_changed.notify_all();
    }
}

/// One ownership slot reserved while a canonical registration is in flight.
///
/// The reservation crosses publication: before publication it is counted in
/// `Registering::pending_owners`, and afterwards the same count belongs to
/// `Active::owners`. Its destructor therefore rolls back either representation
/// if a registering caller unwinds before returning its owner token.
struct RegistrationReservation {
    inner: Arc<StoreMetadataRuntimeInner>,
    segment_identity: Arc<str>,
    generation: u64,
    leader: bool,
    armed: bool,
}

impl RegistrationReservation {
    fn new(
        inner: Arc<StoreMetadataRuntimeInner>,
        segment_identity: Arc<str>,
        generation: u64,
        leader: bool,
    ) -> Self {
        Self {
            inner,
            segment_identity,
            generation,
            leader,
            armed: true,
        }
    }

    fn generation(&self) -> u64 {
        self.generation
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for RegistrationReservation {
    fn drop(&mut self) {
        if self.armed {
            cancel_registration_reservation(
                &self.inner,
                &self.segment_identity,
                self.generation,
                self.leader,
            );
        }
    }
}

/// One owner of an active registered segment generation.
pub struct RegisteredSegment {
    inner: Arc<StoreMetadataRuntimeInner>,
    segment_identity: Arc<str>,
    generation: u64,
    active: bool,
}

impl RegisteredSegment {
    fn new(
        inner: Arc<StoreMetadataRuntimeInner>,
        segment_identity: Arc<str>,
        generation: u64,
    ) -> Self {
        Self {
            inner,
            segment_identity,
            generation,
            active: true,
        }
    }

    pub fn segment_identity(&self) -> &str {
        &self.segment_identity
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn read_guard(&self) -> Result<SegmentReadGuard, StoreMetadataRuntimeError> {
        let handles = {
            let mut state = lock(&self.inner.lifecycle);
            match state.segments.get_mut(&self.segment_identity) {
                Some(SegmentLifecycle::Active {
                    generation,
                    handles,
                    readers,
                    ..
                }) if *generation == self.generation => {
                    *readers = readers.checked_add(1).ok_or_else(|| {
                        StoreMetadataRuntimeError::LifecycleFailed {
                            segment_identity: Arc::clone(&self.segment_identity),
                            message: Arc::from("segment read-guard count overflow"),
                        }
                    })?;
                    Arc::clone(handles)
                }
                Some(SegmentLifecycle::Retiring { .. })
                | Some(SegmentLifecycle::Finalizing { .. }) => {
                    return Err(StoreMetadataRuntimeError::SegmentRetiring {
                        segment_identity: Arc::clone(&self.segment_identity),
                    });
                }
                Some(SegmentLifecycle::Failed { message, .. }) => {
                    return Err(StoreMetadataRuntimeError::LifecycleFailed {
                        segment_identity: Arc::clone(&self.segment_identity),
                        message: Arc::clone(message),
                    });
                }
                _ => {
                    return Err(StoreMetadataRuntimeError::SegmentNotActive {
                        segment_identity: Arc::clone(&self.segment_identity),
                        generation: self.generation,
                    });
                }
            }
        };

        Ok(SegmentReadGuard {
            lease: Arc::new(SegmentReadLease {
                inner: Arc::clone(&self.inner),
                segment_identity: Arc::clone(&self.segment_identity),
                cache_identity: MetadataSegmentIdentity::new(Arc::clone(&self.segment_identity)),
                generation: self.generation,
                handles: Some(handles),
            }),
        })
    }

    pub fn reader(
        &self,
        file: SegmentFile,
    ) -> Result<GovernedArtifactReader, StoreMetadataRuntimeError> {
        self.read_guard()?.reader(file)
    }
}

impl Clone for RegisteredSegment {
    fn clone(&self) -> Self {
        let mut state = lock(&self.inner.lifecycle);
        match state.segments.get_mut(&self.segment_identity) {
            Some(SegmentLifecycle::Active {
                generation, owners, ..
            }) if *generation == self.generation => {
                *owners = owners
                    .checked_add(1)
                    .expect("registered-segment owner count overflow");
            }
            _ => panic!("a registered-segment owner must retain its active generation"),
        }
        drop(state);
        Self::new(
            Arc::clone(&self.inner),
            Arc::clone(&self.segment_identity),
            self.generation,
        )
    }
}

impl Drop for RegisteredSegment {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if let Some(task) = release_owner(&self.inner, &self.segment_identity, self.generation) {
            finalize_segment(&self.inner, task);
        }
    }
}

/// A generation-bound authorization for existing segment read work.
#[derive(Clone)]
pub struct SegmentReadGuard {
    lease: Arc<SegmentReadLease>,
}

/// Allocation-free identity for one store-local segment generation.
///
/// This token keeps the store and stable identity allocation alive without
/// retaining a segment owner, read guard, file descriptor, or cache pin. It is
/// suitable for binding query-local values to the generation that produced
/// them while still allowing normal segment retirement.
#[derive(Clone)]
pub(crate) struct SegmentGenerationProvenance {
    inner: Arc<StoreMetadataRuntimeInner>,
    segment_identity: Arc<str>,
    generation: u64,
}

impl fmt::Debug for SegmentGenerationProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentGenerationProvenance")
            .field("segment_identity", &self.segment_identity)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl SegmentGenerationProvenance {
    pub(crate) fn matches(&self, guard: &SegmentReadGuard) -> bool {
        Arc::ptr_eq(&self.inner, &guard.lease.inner)
            && self.segment_identity == guard.lease.segment_identity
            && self.generation == guard.lease.generation
    }
}

impl SegmentReadGuard {
    pub fn segment_identity(&self) -> &str {
        &self.lease.segment_identity
    }

    pub(super) fn cache_identity(&self) -> MetadataSegmentIdentity {
        self.lease.cache_identity.clone()
    }

    pub fn generation(&self) -> u64 {
        self.lease.generation
    }

    pub(crate) fn provenance(&self) -> SegmentGenerationProvenance {
        SegmentGenerationProvenance {
            inner: Arc::clone(&self.lease.inner),
            segment_identity: Arc::clone(&self.lease.segment_identity),
            generation: self.lease.generation,
        }
    }

    pub fn reader(
        &self,
        file: SegmentFile,
    ) -> Result<GovernedArtifactReader, StoreMetadataRuntimeError> {
        let file_index = SEGMENT_FOOTER_TRACKED_FILES
            .iter()
            .position(|candidate| *candidate == file)
            .ok_or(MetadataFileManagerError::UntrackedSegmentFile { file })?;
        Ok(GovernedArtifactReader::from_guard(self.clone(), file_index))
    }

    pub(super) fn runtime(&self) -> StoreMetadataRuntime {
        StoreMetadataRuntime::from_inner(Arc::clone(&self.lease.inner))
    }

    pub(super) fn handle(&self, file_index: usize) -> &SegmentFileHandle {
        &self
            .lease
            .handles
            .as_ref()
            .expect("live segment read lease retains canonical handles")[file_index]
    }
}

struct SegmentReadLease {
    inner: Arc<StoreMetadataRuntimeInner>,
    segment_identity: Arc<str>,
    cache_identity: MetadataSegmentIdentity,
    generation: u64,
    handles: Option<Arc<[SegmentFileHandle; TRACKED_FILE_COUNT]>>,
}

impl Drop for SegmentReadLease {
    fn drop(&mut self) {
        drop(self.handles.take());
        if let Some(task) = release_reader(&self.inner, &self.segment_identity, self.generation) {
            finalize_segment(&self.inner, task);
        }
    }
}

fn cancel_registration_reservation(
    inner: &Arc<StoreMetadataRuntimeInner>,
    segment_identity: &Arc<str>,
    generation: u64,
    leader: bool,
) {
    let mut state = lock(&inner.lifecycle);
    let mut release_active_owner = false;
    let mut notify = false;
    match state.segments.get_mut(segment_identity) {
        Some(SegmentLifecycle::Registering {
            generation: current,
            pending_owners,
            ..
        }) if *current == generation => {
            if leader {
                state.segments.remove(segment_identity);
                notify = true;
            } else if *pending_owners > 1 {
                *pending_owners -= 1;
            } else {
                state.segments.insert(
                    Arc::clone(segment_identity),
                    SegmentLifecycle::Failed {
                        message: Arc::from(
                            "registration join reservation lost its leader ownership slot",
                        ),
                    },
                );
                notify = true;
            }
        }
        Some(SegmentLifecycle::Active {
            generation: current,
            ..
        }) if *current == generation => {
            release_active_owner = true;
        }
        _ => {}
    }
    drop(state);
    if notify {
        inner.lifecycle_changed.notify_all();
    }
    if release_active_owner && let Some(task) = release_owner(inner, segment_identity, generation) {
        finalize_segment(inner, task);
    }
}

fn release_owner(
    inner: &Arc<StoreMetadataRuntimeInner>,
    segment_identity: &Arc<str>,
    generation: u64,
) -> Option<FinalizationTask> {
    let mut state = lock(&inner.lifecycle);
    let Some(SegmentLifecycle::Active {
        generation: current,
        owners,
        ..
    }) = state.segments.get_mut(segment_identity)
    else {
        return None;
    };
    if *current != generation {
        return None;
    }
    *owners = owners
        .checked_sub(1)
        .expect("registered-segment owner count underflow");
    if *owners != 0 {
        return None;
    }

    let lifecycle = state
        .segments
        .remove(segment_identity)
        .expect("active lifecycle selected for owner release");
    let SegmentLifecycle::Active {
        handles, readers, ..
    } = lifecycle
    else {
        unreachable!("owner release must remove active lifecycle")
    };
    if readers == 0 {
        state.segments.insert(
            Arc::clone(segment_identity),
            SegmentLifecycle::Finalizing { generation },
        );
        Some(FinalizationTask {
            segment_identity: Arc::clone(segment_identity),
            generation,
            handles,
        })
    } else {
        state.segments.insert(
            Arc::clone(segment_identity),
            SegmentLifecycle::Retiring {
                generation,
                handles,
                readers,
            },
        );
        None
    }
}

fn release_reader(
    inner: &Arc<StoreMetadataRuntimeInner>,
    segment_identity: &Arc<str>,
    generation: u64,
) -> Option<FinalizationTask> {
    let mut state = lock(&inner.lifecycle);
    match state.segments.get_mut(segment_identity) {
        Some(SegmentLifecycle::Active {
            generation: current,
            readers,
            ..
        }) if *current == generation => {
            *readers = readers
                .checked_sub(1)
                .expect("active segment reader count underflow");
            None
        }
        Some(SegmentLifecycle::Retiring {
            generation: current,
            readers,
            ..
        }) if *current == generation => {
            *readers = readers
                .checked_sub(1)
                .expect("retiring segment reader count underflow");
            if *readers != 0 {
                return None;
            }
            let lifecycle = state
                .segments
                .remove(segment_identity)
                .expect("retiring lifecycle selected for final reader release");
            let SegmentLifecycle::Retiring { handles, .. } = lifecycle else {
                unreachable!("reader release must remove retiring lifecycle")
            };
            state.segments.insert(
                Arc::clone(segment_identity),
                SegmentLifecycle::Finalizing { generation },
            );
            Some(FinalizationTask {
                segment_identity: Arc::clone(segment_identity),
                generation,
                handles,
            })
        }
        _ => None,
    }
}

fn finalize_segment(inner: &Arc<StoreMetadataRuntimeInner>, task: FinalizationTask) {
    let file_result = inner
        .files
        .retire_segment(Arc::clone(&task.segment_identity));
    drop(task.handles);
    let outcome = match file_result {
        Ok(()) => match inner.cache.retire_artifacts_after_inventory_removal(
            &task.segment_identity,
            &SEGMENT_FOOTER_TRACKED_FILES,
        ) {
            Ok(MetadataArtifactRetirement::Removed | MetadataArtifactRetirement::Deferred) => {
                Ok(())
            }
            Ok(MetadataArtifactRetirement::NotRegistered) => Err(Arc::from(
                "active segment cache inventory was not registered during finalization",
            )),
            Err(error) => Err(Arc::from(error.to_string())),
        },
        Err(error) => Err(Arc::from(error.to_string())),
    };

    let mut state = lock(&inner.lifecycle);
    let current_generation = match state.segments.get(&task.segment_identity) {
        Some(SegmentLifecycle::Finalizing { generation }) => Some(*generation),
        _ => None,
    };
    if current_generation == Some(task.generation) {
        state.segments.remove(&task.segment_identity);
        if let Err(message) = outcome {
            state.segments.insert(
                Arc::clone(&task.segment_identity),
                SegmentLifecycle::Failed { message },
            );
        }
    }
    drop(state);
    inner.lifecycle_changed.notify_all();
}

fn validate_inventory(
    artifacts: &[SegmentArtifactRegistration],
) -> Result<CanonicalInventory, StoreMetadataRuntimeError> {
    if artifacts.len() != TRACKED_FILE_COUNT {
        return Err(StoreMetadataRuntimeError::InvalidArtifactCount {
            expected: TRACKED_FILE_COUNT,
            actual: artifacts.len(),
        });
    }
    for (index, (artifact, expected)) in artifacts
        .iter()
        .zip(SEGMENT_FOOTER_TRACKED_FILES)
        .enumerate()
    {
        if artifact.file != expected {
            return Err(StoreMetadataRuntimeError::NonCanonicalArtifact {
                index,
                expected,
                actual: artifact.file,
            });
        }
    }
    let artifacts = artifacts.to_vec().try_into().map_err(|_| {
        StoreMetadataRuntimeError::InvalidArtifactCount {
            expected: TRACKED_FILE_COUNT,
            actual: artifacts.len(),
        }
    })?;
    Ok(CanonicalInventory { artifacts })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
fn pause_registration_for_test(hook: &Mutex<Option<RegistrationPauseTestHook>>) {
    let hook = lock(hook).take();
    if let Some(hook) = hook {
        hook.entered.wait();
        hook.resume.wait();
        assert!(
            !hook.panic_after_resume,
            "injected registration unwind after deterministic pause"
        );
    }
}

#[cfg(test)]
impl StoreMetadataRuntime {
    pub(super) fn lifecycle_counts_for_test(&self) -> (usize, usize, usize, usize, usize) {
        let state = lock(&self.inner.lifecycle);
        let mut registering = 0;
        let mut active = 0;
        let mut retiring = 0;
        let mut finalizing = 0;
        let mut failed = 0;
        for lifecycle in state.segments.values() {
            match lifecycle {
                SegmentLifecycle::Registering { .. } => registering += 1,
                SegmentLifecycle::Active { .. } => active += 1,
                SegmentLifecycle::Retiring { .. } => retiring += 1,
                SegmentLifecycle::Finalizing { .. } => finalizing += 1,
                SegmentLifecycle::Failed { .. } => failed += 1,
            }
        }
        (registering, active, retiring, finalizing, failed)
    }

    pub(super) fn pending_registration_for_test(
        &self,
        segment_identity: &str,
    ) -> Option<(u64, u32)> {
        let state = lock(&self.inner.lifecycle);
        match state.segments.get(segment_identity) {
            Some(SegmentLifecycle::Registering {
                generation,
                pending_owners,
                ..
            }) => Some((*generation, *pending_owners)),
            _ => None,
        }
    }

    pub(super) fn install_registration_leader_pause_for_test(
        &self,
        entered: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
        panic_after_resume: bool,
    ) {
        *lock(&self.inner.registration_leader_test_hook) = Some(RegistrationPauseTestHook {
            entered,
            resume,
            panic_after_resume,
        });
    }

    pub(super) fn install_registration_join_wake_pause_for_test(
        &self,
        entered: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
        panic_after_resume: bool,
    ) {
        *lock(&self.inner.registration_join_wake_test_hook) = Some(RegistrationPauseTestHook {
            entered,
            resume,
            panic_after_resume,
        });
    }

    pub(super) fn install_registration_after_cache_pause_for_test(
        &self,
        entered: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
        panic_after_resume: bool,
    ) {
        *lock(&self.inner.registration_after_cache_test_hook) = Some(RegistrationPauseTestHook {
            entered,
            resume,
            panic_after_resume,
        });
    }
}
