use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use thiserror::Error;

use super::head::HeadReadView;
use super::live_memory::{LiveMemoryCharge, LiveMemoryClass, LiveMemoryGovernor};
use super::manifest::ManifestCut;
use super::segment::SegmentStoreReader;

/// The complete immutable storage sources bound to one live generation.
///
/// Keeping both sources in the root prevents a request from pairing a newer
/// sealed inventory with an older head (or the reverse).
pub struct LiveStorageView {
    sealed: Arc<SegmentStoreReader>,
    head: Arc<HeadReadView>,
    manifest_cut: ManifestCut,
    catalog_revision: u64,
    /// Physical live allocations shared with this generation. The values are
    /// intentionally opaque to query code; retaining their `Arc`s keeps each
    /// governor charge alive exactly as long as any view can reach the
    /// corresponding frozen payload.
    resource_leases: Box<[Arc<LiveMemoryCharge>]>,
}

impl LiveStorageView {
    pub fn new(
        sealed: Arc<SegmentStoreReader>,
        head: Arc<HeadReadView>,
    ) -> Result<Self, LiveViewError> {
        Self::with_resource_leases(sealed, head, Vec::new())
    }

    pub fn with_resource_leases(
        sealed: Arc<SegmentStoreReader>,
        head: Arc<HeadReadView>,
        resource_leases: Vec<Arc<LiveMemoryCharge>>,
    ) -> Result<Self, LiveViewError> {
        let manifest_cut = sealed
            .validated_manifest_cut()
            .cloned()
            .ok_or(LiveViewError::UnboundSealedInventory)?;
        let catalog_revision = head.catalog_revision();
        Ok(Self {
            sealed,
            head,
            manifest_cut,
            catalog_revision,
            resource_leases: resource_leases.into_boxed_slice(),
        })
    }

    pub fn sealed(&self) -> &Arc<SegmentStoreReader> {
        &self.sealed
    }

    pub fn head(&self) -> &Arc<HeadReadView> {
        &self.head
    }

    pub fn retained_resource_count(&self) -> usize {
        self.resource_leases.len()
    }

    pub fn bound_manifest_cut(&self) -> &ManifestCut {
        &self.manifest_cut
    }

    pub fn bound_catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    fn validate_binding(
        &self,
        manifest_cut: &ManifestCut,
        catalog_revision: u64,
    ) -> Result<(), LiveViewError> {
        if manifest_cut != &self.manifest_cut {
            return Err(LiveViewError::ManifestBindingMismatch);
        }
        if catalog_revision != self.catalog_revision {
            return Err(LiveViewError::CatalogBindingMismatch {
                view_revision: catalog_revision,
                head_revision: self.catalog_revision,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveReadiness {
    Uninitialized,
    Ready,
    DirtySince(Instant),
    Failed(Arc<str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStatus {
    pub readiness: LiveReadiness,
    pub status_epoch: u64,
    pub generation: Option<u64>,
}

pub struct LiveQueryView<T> {
    generation: u64,
    candidate_prepared_at: Instant,
    published_at: OnceLock<Instant>,
    manifest_cut: ManifestCut,
    visible_message_sequence: u64,
    catalog_revision: u64,
    payload: T,
}

impl<T: fmt::Debug> fmt::Debug for LiveQueryView<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveQueryView")
            .field("generation", &self.generation)
            .field("published_at", &self.published_at())
            .field("manifest_cut", &self.manifest_cut)
            .field("visible_message_sequence", &self.visible_message_sequence)
            .field("catalog_revision", &self.catalog_revision)
            .field("payload", &self.payload)
            .finish()
    }
}

impl<T> LiveQueryView<T> {
    pub fn new(
        generation: u64,
        published_at: Instant,
        manifest_cut: ManifestCut,
        visible_message_sequence: u64,
        catalog_revision: u64,
        payload: T,
    ) -> Result<Self, LiveViewError> {
        if generation == 0 {
            return Err(LiveViewError::InvalidGeneration {
                expected: 1,
                actual: 0,
            });
        }
        Ok(Self {
            generation,
            candidate_prepared_at: published_at,
            published_at: OnceLock::new(),
            manifest_cut,
            visible_message_sequence,
            catalog_revision,
            payload,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the successful root-commit anchor once this view is published.
    ///
    /// Before publication, candidate inspection retains the provisional
    /// construction timestamp supplied to [`Self::new`]. A successful commit
    /// finalizes this value exactly once before making the root reachable.
    pub fn published_at(&self) -> Instant {
        self.published_at
            .get()
            .copied()
            .unwrap_or(self.candidate_prepared_at)
    }

    pub fn manifest_cut(&self) -> &ManifestCut {
        &self.manifest_cut
    }

    pub fn visible_message_sequence(&self) -> u64 {
        self.visible_message_sequence
    }

    pub fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    fn finalize_published_at(&self, published_at: Instant) -> Result<(), LiveViewError> {
        self.published_at
            .set(published_at)
            .map_err(|_| LiveViewError::ViewAlreadyPublished)
    }
}

impl LiveQueryView<LiveStorageView> {
    /// Constructs a production storage view only when its wrapper cuts match
    /// the exact sealed inventory and head catalog carried by the payload.
    pub fn new_storage(
        generation: u64,
        published_at: Instant,
        manifest_cut: ManifestCut,
        visible_message_sequence: u64,
        catalog_revision: u64,
        payload: LiveStorageView,
    ) -> Result<Self, LiveViewError> {
        payload.validate_binding(&manifest_cut, catalog_revision)?;
        Self::new(
            generation,
            published_at,
            manifest_cut,
            visible_message_sequence,
            catalog_revision,
            payload,
        )
    }
}

struct PublishedLiveState<T> {
    current: Option<Arc<LiveQueryView<T>>>,
    readiness: LiveReadiness,
    status_epoch: u64,
}

pub struct LiveQueryHandle<T> {
    state: RwLock<PublishedLiveState<T>>,
    max_view_staleness: Duration,
    query_admission: OnceLock<LiveQueryAdmission>,
}

impl<T> fmt::Debug for LiveQueryHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveQueryHandle")
            .field("max_view_staleness", &self.max_view_staleness)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveCommitBase {
    pub status_epoch: u64,
    pub next_generation: u64,
}

/// Time spent acquiring and holding the live root state lock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LiveRootLockTiming {
    /// Time from starting the lock operation until acquiring the guard.
    pub wait: Duration,
    /// Time from acquiring the guard until explicitly releasing it.
    pub held: Duration,
}

/// Timings for one successful live root commit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LiveCommitTiming {
    /// Write-lock acquisition and critical-section timing.
    pub root_lock: LiveRootLockTiming,
    /// Time spent dropping the preceding root `Arc` after releasing the lock.
    ///
    /// This is full allocation reclamation only when that `Arc` was the final
    /// owner; a pinned older generation reduces this to its reference drop.
    pub old_root_reclaim: Duration,
}

pub struct LiveCommitCandidate<T> {
    base: LiveCommitBase,
    view: Arc<LiveQueryView<T>>,
}

#[derive(Debug)]
struct LiveQueryAdmission {
    governor: Arc<LiveMemoryGovernor>,
    retention_bytes: u64,
}

/// One admitted request's exact immutable generation.
///
/// The pinned root itself retains every shared allocation through `Arc`
/// ownership. The current publisher attaches one governor lease per frozen
/// payload; catalog and inventory accounting remains telemetry/performance
/// follow-up work. This separate query-retention token only proves that the
/// configured live-memory budget is not fully exhausted when a new request
/// starts.
pub struct LiveQueryPin<T> {
    view: Arc<LiveQueryView<T>>,
    root_lock_timing: LiveRootLockTiming,
    _retention: LiveMemoryCharge,
}

impl<T: fmt::Debug> fmt::Debug for LiveQueryPin<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveQueryPin")
            .field("view", &self.view)
            .field("root_lock_timing", &self.root_lock_timing)
            .finish_non_exhaustive()
    }
}

impl<T> LiveQueryPin<T> {
    /// Returns this exact pin's root read-lock acquisition and hold timing.
    pub fn root_lock_timing(&self) -> LiveRootLockTiming {
        self.root_lock_timing
    }
}

impl<T> Deref for LiveQueryPin<T> {
    type Target = LiveQueryView<T>;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl<T> LiveCommitCandidate<T> {
    pub fn new(base: LiveCommitBase, view: Arc<LiveQueryView<T>>) -> Self {
        Self { base, view }
    }

    pub fn base(&self) -> LiveCommitBase {
        self.base
    }

    pub fn view(&self) -> &Arc<LiveQueryView<T>> {
        &self.view
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LiveViewError {
    #[error("live view state lock is poisoned")]
    Poisoned,
    #[error("no live query view has been published")]
    Uninitialized,
    #[error("live view publication failed: {0}")]
    Failed(Arc<str>),
    #[error("live view is stale by {age_ms} ms (maximum {max_ms} ms)")]
    Stale { age_ms: u128, max_ms: u128 },
    #[error("live view generation mismatch: expected {expected}, got {actual}")]
    InvalidGeneration { expected: u64, actual: u64 },
    #[error("live view status epoch changed: expected {expected}, got {actual}")]
    StaleCandidate { expected: u64, actual: u64 },
    #[error("live view message cut regressed from {previous} to {next}")]
    MessageCutRegression { previous: u64, next: u64 },
    #[error("live view catalog revision regressed from {previous} to {next}")]
    CatalogRevisionRegression { previous: u64, next: u64 },
    #[error("live view manifest cut regressed or changed its validated prefix")]
    ManifestCutRegression,
    #[error("live storage view requires a manifest-snapshot-backed sealed inventory")]
    UnboundSealedInventory,
    #[error("live storage view manifest cut does not match its sealed inventory")]
    ManifestBindingMismatch,
    #[error(
        "live storage view catalog revision {view_revision} does not match head revision {head_revision}"
    )]
    CatalogBindingMismatch {
        view_revision: u64,
        head_revision: u64,
    },
    #[error("live query-retention admission is not configured")]
    QueryAdmissionUnconfigured,
    #[error("live query-retention admission is already configured")]
    QueryAdmissionAlreadyConfigured,
    #[error("live query-retention admission bytes must be greater than zero")]
    InvalidQueryRetentionBytes,
    #[error("live query rejected by resource-pressure admission: {0}")]
    ResourcePressure(Arc<str>),
    #[error("live view generation overflow")]
    GenerationOverflow,
    #[error("live query view was already published")]
    ViewAlreadyPublished,
    #[error("maximum live-view staleness must be greater than zero")]
    InvalidMaxStaleness,
    #[error("live view status epoch overflow")]
    StatusEpochOverflow,
}

impl<T> LiveQueryHandle<T> {
    pub fn new(max_view_staleness: Duration) -> Result<Arc<Self>, LiveViewError> {
        if max_view_staleness.is_zero() {
            return Err(LiveViewError::InvalidMaxStaleness);
        }
        Ok(Arc::new(Self {
            state: RwLock::new(PublishedLiveState {
                current: None,
                readiness: LiveReadiness::Uninitialized,
                status_epoch: 0,
            }),
            max_view_staleness,
            query_admission: OnceLock::new(),
        }))
    }

    /// Installs the process-wide admission governor before the handle is
    /// exposed to the HTTP router.
    ///
    /// `retention_bytes` is an explicit token size, not a second charge for
    /// allocations already retained by the pinned view.
    pub fn configure_query_admission(
        &self,
        governor: Arc<LiveMemoryGovernor>,
        retention_bytes: u64,
    ) -> Result<(), LiveViewError> {
        if retention_bytes == 0 {
            return Err(LiveViewError::InvalidQueryRetentionBytes);
        }
        self.query_admission
            .set(LiveQueryAdmission {
                governor,
                retention_bytes,
            })
            .map_err(|_| LiveViewError::QueryAdmissionAlreadyConfigured)
    }

    pub fn query_admission_configured(&self) -> bool {
        self.query_admission.get().is_some()
    }

    pub fn status(&self) -> Result<LiveStatus, LiveViewError> {
        let state = self.state.read().map_err(|_| LiveViewError::Poisoned)?;
        Ok(LiveStatus {
            readiness: state.readiness.clone(),
            status_epoch: state.status_epoch,
            generation: state.current.as_ref().map(|view| view.generation),
        })
    }

    pub fn begin_commit(&self) -> Result<LiveCommitBase, LiveViewError> {
        self.begin_commit_timed().map(|(base, _timing)| base)
    }

    /// Reads the next commit descriptor and reports its root write-lock time.
    pub fn begin_commit_timed(
        &self,
    ) -> Result<(LiveCommitBase, LiveRootLockTiming), LiveViewError> {
        let lock_started = Instant::now();
        let mut state = self.state.write().map_err(|_| LiveViewError::Poisoned)?;
        let lock_acquired = Instant::now();
        let next_generation = match state.current.as_ref() {
            Some(view) => match view.generation.checked_add(1) {
                Some(next) => next,
                None => {
                    fail_state_closed(&mut state, "live view generation overflow");
                    return Err(LiveViewError::GenerationOverflow);
                }
            },
            None => 1,
        };
        let base = LiveCommitBase {
            status_epoch: state.status_epoch,
            next_generation,
        };
        drop(state);
        let lock_released = Instant::now();
        Ok((
            base,
            LiveRootLockTiming {
                wait: lock_acquired.saturating_duration_since(lock_started),
                held: lock_released.saturating_duration_since(lock_acquired),
            },
        ))
    }

    pub fn pin(&self, now: Instant) -> Result<Arc<LiveQueryView<T>>, LiveViewError> {
        let state = self.state.read().map_err(|_| LiveViewError::Poisoned)?;
        self.validate_readiness(&state, now)?;
        state
            .current
            .as_ref()
            .map(Arc::clone)
            .ok_or(LiveViewError::Uninitialized)
    }

    /// Pins one exact generation and retains a live-memory admission token.
    ///
    /// Callers must acquire their independent concurrency permit before this
    /// method so queued requests do not retain obsolete generations.
    pub fn try_pin_admitted(&self, now: Instant) -> Result<LiveQueryPin<T>, LiveViewError> {
        let admission = self
            .query_admission
            .get()
            .ok_or(LiveViewError::QueryAdmissionUnconfigured)?;
        let lock_started = Instant::now();
        let state = self.state.read().map_err(|_| LiveViewError::Poisoned)?;
        let lock_acquired = Instant::now();
        self.validate_readiness(&state, now)?;
        let view = state
            .current
            .as_ref()
            .map(Arc::clone)
            .ok_or(LiveViewError::Uninitialized)?;
        // Pinning linearizes at the Arc clone. Admission accounting is
        // independent of the published-root state and must not extend the
        // root read-lock critical section.
        drop(state);
        let lock_released = Instant::now();
        let retention = admission
            .governor
            .try_charge(LiveMemoryClass::QueryRetention, admission.retention_bytes)
            .map_err(|error| LiveViewError::ResourcePressure(Arc::from(error.to_string())))?;
        Ok(LiveQueryPin {
            view,
            root_lock_timing: LiveRootLockTiming {
                wait: lock_acquired.saturating_duration_since(lock_started),
                held: lock_released.saturating_duration_since(lock_acquired),
            },
            _retention: retention,
        })
    }

    /// Performs the same readiness and resource-pressure check as a live
    /// query, then immediately releases the temporary admission token.
    pub fn can_admit_query(&self, now: Instant) -> Result<(), LiveViewError> {
        drop(self.try_pin_admitted(now)?);
        Ok(())
    }

    pub fn mark_dirty(&self, now: Instant) -> Result<(), LiveViewError> {
        let mut state = self.state.write().map_err(|_| LiveViewError::Poisoned)?;
        // A new message must not hide an unresolved publication failure.
        // Successful commit is the only transition back to Ready.
        if matches!(state.readiness, LiveReadiness::Failed(_)) {
            return Ok(());
        }
        if !matches!(state.readiness, LiveReadiness::DirtySince(_)) {
            let Some(next_status_epoch) = state.status_epoch.checked_add(1) else {
                state.readiness =
                    LiveReadiness::Failed(Arc::from("live view status epoch overflow"));
                return Err(LiveViewError::StatusEpochOverflow);
            };
            state.readiness = LiveReadiness::DirtySince(now);
            state.status_epoch = next_status_epoch;
        }
        Ok(())
    }

    pub fn mark_failed(&self, error: impl Into<Arc<str>>) -> Result<(), LiveViewError> {
        let mut state = self.state.write().map_err(|_| LiveViewError::Poisoned)?;
        let error = error.into();
        match state.status_epoch.checked_add(1) {
            Some(next_status_epoch) => {
                state.readiness = LiveReadiness::Failed(error);
                state.status_epoch = next_status_epoch;
                Ok(())
            }
            None => {
                // Epoch exhaustion is itself terminal. Preserve the maximum
                // epoch but make the indivisible state fail closed so no
                // subsequent pin can observe the preceding Ready root.
                state.readiness = LiveReadiness::Failed(error);
                Err(LiveViewError::StatusEpochOverflow)
            }
        }
    }

    pub fn commit(&self, candidate: LiveCommitCandidate<T>) -> Result<(), LiveViewError> {
        self.commit_timed(candidate).map(|_| ())
    }

    /// Commits one candidate and returns only timings from the root-swap path.
    ///
    /// Candidate construction is deliberately outside these measurements.
    /// Dropping the preceding root is timed separately after the write guard
    /// has been released.
    pub fn commit_timed(
        &self,
        candidate: LiveCommitCandidate<T>,
    ) -> Result<LiveCommitTiming, LiveViewError> {
        let lock_started = Instant::now();
        let mut state = self.state.write().map_err(|_| LiveViewError::Poisoned)?;
        let lock_acquired = Instant::now();
        if state.status_epoch != candidate.base.status_epoch {
            return Err(LiveViewError::StaleCandidate {
                expected: candidate.base.status_epoch,
                actual: state.status_epoch,
            });
        }
        let expected_generation = match state.current.as_ref() {
            Some(view) => match view.generation.checked_add(1) {
                Some(next) => next,
                None => {
                    fail_state_closed(&mut state, "live view generation overflow");
                    return Err(LiveViewError::GenerationOverflow);
                }
            },
            None => 1,
        };
        if candidate.base.next_generation != expected_generation
            || candidate.view.generation != expected_generation
        {
            return Err(LiveViewError::InvalidGeneration {
                expected: expected_generation,
                actual: candidate.view.generation,
            });
        }
        if let Some(previous) = state.current.as_ref() {
            validate_non_regressing_cut(previous, &candidate.view)?;
        }
        let Some(next_status_epoch) = state.status_epoch.checked_add(1) else {
            state.readiness = LiveReadiness::Failed(Arc::from("live view status epoch overflow"));
            return Err(LiveViewError::StatusEpochOverflow);
        };

        // Finalize the publication age only after every fallible candidate
        // validation, immediately before this root becomes current.
        candidate.view.finalize_published_at(Instant::now())?;
        let old = state.current.replace(candidate.view);
        state.readiness = LiveReadiness::Ready;
        state.status_epoch = next_status_epoch;
        drop(state);
        let lock_released = Instant::now();

        // The final old-view Arc drop may recursively reclaim large catalogs,
        // descriptor trees, arenas, and segment readers when no reader retains
        // it. It must happen after the state write lock is released.
        let old_root_reclaim_started = Instant::now();
        drop(old);
        let old_root_reclaim = old_root_reclaim_started.elapsed();
        Ok(LiveCommitTiming {
            root_lock: LiveRootLockTiming {
                wait: lock_acquired.saturating_duration_since(lock_started),
                held: lock_released.saturating_duration_since(lock_acquired),
            },
            old_root_reclaim,
        })
    }

    fn validate_readiness(
        &self,
        state: &PublishedLiveState<T>,
        now: Instant,
    ) -> Result<(), LiveViewError> {
        match &state.readiness {
            LiveReadiness::Uninitialized => Err(LiveViewError::Uninitialized),
            LiveReadiness::Failed(error) => Err(LiveViewError::Failed(Arc::clone(error))),
            LiveReadiness::DirtySince(dirty_since) => {
                let age = now.saturating_duration_since(*dirty_since);
                if age > self.max_view_staleness {
                    Err(LiveViewError::Stale {
                        age_ms: age.as_millis(),
                        max_ms: self.max_view_staleness.as_millis(),
                    })
                } else {
                    Ok(())
                }
            }
            LiveReadiness::Ready => Ok(()),
        }
    }
}

fn fail_state_closed<T>(state: &mut PublishedLiveState<T>, message: &'static str) {
    state.readiness = LiveReadiness::Failed(Arc::from(message));
    if let Some(next_status_epoch) = state.status_epoch.checked_add(1) {
        state.status_epoch = next_status_epoch;
    }
}

fn validate_non_regressing_cut<T>(
    previous: &LiveQueryView<T>,
    next: &LiveQueryView<T>,
) -> Result<(), LiveViewError> {
    if next.visible_message_sequence < previous.visible_message_sequence {
        return Err(LiveViewError::MessageCutRegression {
            previous: previous.visible_message_sequence,
            next: next.visible_message_sequence,
        });
    }
    if next.catalog_revision < previous.catalog_revision {
        return Err(LiveViewError::CatalogRevisionRegression {
            previous: previous.catalog_revision,
            next: next.catalog_revision,
        });
    }
    match (&previous.manifest_cut, &next.manifest_cut) {
        (ManifestCut::Absent, _) => Ok(()),
        (ManifestCut::Present { .. }, ManifestCut::Absent) => {
            Err(LiveViewError::ManifestCutRegression)
        }
        (
            ManifestCut::Present {
                file_name: previous_name,
                validated_offset: previous_offset,
                prefix_sha256: previous_hash,
            },
            ManifestCut::Present {
                file_name: next_name,
                validated_offset: next_offset,
                prefix_sha256: next_hash,
            },
        ) if previous_name == next_name => {
            if next_offset < previous_offset
                || (next_offset == previous_offset && next_hash != previous_hash)
            {
                Err(LiveViewError::ManifestCutRegression)
            } else {
                Ok(())
            }
        }
        // Rotation changes identity and therefore has no byte-offset ordering.
        // The incremental inventory builder must prove its logical record
        // prefix before constructing this candidate.
        (ManifestCut::Present { .. }, ManifestCut::Present { .. }) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::mpsc;
    use std::thread;

    use crate::labels::{
        KeyValueRef, LabelSetStore, METRIC_NAME_LABEL, VersionedFlatInternedLabelSetStore,
    };
    use crate::storage::head::{
        FloatEncoding, FrozenHeadReadView, HeadBuffer, HeadConfig, IntEncoding, SampleValue,
    };
    use crate::storage::manifest::ManifestSnapshot;
    use crate::storage::segment::SegmentSelector;

    use super::*;

    fn view(
        generation: u64,
        message_sequence: u64,
        catalog_revision: u64,
        payload: u64,
    ) -> Arc<LiveQueryView<u64>> {
        Arc::new(
            LiveQueryView::new(
                generation,
                Instant::now(),
                ManifestCut::Absent,
                message_sequence,
                catalog_revision,
                payload,
            )
            .unwrap(),
        )
    }

    fn publish(handle: &LiveQueryHandle<u64>, payload: u64) {
        let base = handle.begin_commit().unwrap();
        handle
            .commit(LiveCommitCandidate::new(
                base,
                view(base.next_generation, payload, payload, payload),
            ))
            .unwrap();
    }

    #[test]
    fn initial_view_is_absent_until_one_atomic_commit() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        assert_eq!(
            handle.pin(Instant::now()).unwrap_err(),
            LiveViewError::Uninitialized
        );

        publish(&handle, 7);

        let pinned = handle.pin(Instant::now()).unwrap();
        assert_eq!(pinned.generation(), 1);
        assert_eq!(*pinned.payload(), 7);
        assert_eq!(handle.status().unwrap().readiness, LiveReadiness::Ready);
    }

    #[test]
    fn pinned_reader_keeps_its_generation_while_a_new_reader_gets_the_next() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        publish(&handle, 1);
        let first = handle.pin(Instant::now()).unwrap();

        publish(&handle, 2);
        let second = handle.pin(Instant::now()).unwrap();

        assert_eq!((first.generation(), *first.payload()), (1, 1));
        assert_eq!((second.generation(), *second.payload()), (2, 2));
    }

    #[test]
    fn timed_begin_commit_reports_its_lock_and_begin_commit_remains_a_wrapper() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let started = Instant::now();
        let (base, timing) = handle.begin_commit_timed().unwrap();
        let elapsed = started.elapsed();
        assert_eq!(
            base,
            LiveCommitBase {
                status_epoch: 0,
                next_generation: 1,
            }
        );
        assert!(timing.wait <= elapsed);
        assert!(timing.held <= elapsed);

        handle
            .commit(LiveCommitCandidate::new(base, view(1, 1, 1, 1)))
            .unwrap();
        assert_eq!(handle.begin_commit().unwrap().next_generation, 2);
    }

    #[test]
    fn timed_commit_finalizes_publication_time_and_commit_remains_a_wrapper() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let provisional = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        let base = handle.begin_commit().unwrap();
        let candidate = Arc::new(
            LiveQueryView::new(
                base.next_generation,
                provisional,
                ManifestCut::Absent,
                41,
                17,
                1,
            )
            .unwrap(),
        );
        assert_eq!(candidate.published_at(), provisional);

        let commit_started = Instant::now();
        let timing = handle
            .commit_timed(LiveCommitCandidate::new(base, candidate))
            .unwrap();
        let commit_finished = Instant::now();
        let commit_elapsed = commit_finished.saturating_duration_since(commit_started);
        let pinned = handle.pin(Instant::now()).unwrap();
        assert!(pinned.published_at() >= commit_started);
        assert!(pinned.published_at() <= commit_finished);
        assert!(timing.root_lock.wait <= commit_elapsed);
        assert!(timing.root_lock.held <= commit_elapsed);
        assert!(timing.old_root_reclaim <= commit_elapsed);

        publish(&handle, 42);
        assert_eq!(handle.pin(Instant::now()).unwrap().generation(), 2);
    }

    #[test]
    fn one_view_cannot_be_published_through_two_handles() {
        let first = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let second = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let first_base = first.begin_commit().unwrap();
        let candidate = view(1, 1, 1, 1);
        first
            .commit(LiveCommitCandidate::new(first_base, Arc::clone(&candidate)))
            .unwrap();

        let second_base = second.begin_commit().unwrap();
        assert_eq!(
            second
                .commit_timed(LiveCommitCandidate::new(second_base, candidate))
                .unwrap_err(),
            LiveViewError::ViewAlreadyPublished
        );
        assert_eq!(
            second.pin(Instant::now()).unwrap_err(),
            LiveViewError::Uninitialized
        );
    }

    #[test]
    fn dirty_view_remains_queryable_until_deadline_and_failure_rejects_new_pins() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        publish(&handle, 1);
        let pinned_before_failure = handle.pin(Instant::now()).unwrap();
        let dirty_at = Instant::now();
        handle.mark_dirty(dirty_at).unwrap();

        assert!(handle.pin(dirty_at + Duration::from_secs(10)).is_ok());
        assert!(matches!(
            handle.pin(dirty_at + Duration::from_secs(11)),
            Err(LiveViewError::Stale { .. })
        ));

        handle.mark_failed("refresh failed").unwrap();
        handle.mark_dirty(Instant::now()).unwrap();
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(LiveViewError::Failed(_))
        ));
        assert_eq!(*pinned_before_failure.payload(), 1);
    }

    #[test]
    fn stale_candidate_cannot_partially_replace_a_newer_status_epoch() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        publish(&handle, 1);
        let stale = handle.begin_commit().unwrap();
        handle.mark_dirty(Instant::now()).unwrap();

        let error = handle
            .commit(LiveCommitCandidate::new(
                stale,
                view(stale.next_generation, 2, 2, 2),
            ))
            .unwrap_err();

        assert!(matches!(error, LiveViewError::StaleCandidate { .. }));
        assert_eq!(*handle.pin(Instant::now()).unwrap().payload(), 1);
    }

    #[derive(Debug)]
    struct BlockingDrop {
        value: u64,
        entered: Option<mpsc::Sender<()>>,
        release: Option<Arc<Barrier>>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.take().unwrap().wait();
            }
        }
    }

    #[test]
    fn old_root_destructor_runs_after_the_swap_lock_is_released() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new(Barrier::new(2));
        let base = handle.begin_commit().unwrap();
        handle
            .commit(LiveCommitCandidate::new(
                base,
                Arc::new(
                    LiveQueryView::new(
                        1,
                        Instant::now(),
                        ManifestCut::Absent,
                        1,
                        1,
                        BlockingDrop {
                            value: 1,
                            entered: Some(entered_tx),
                            release: Some(Arc::clone(&release)),
                        },
                    )
                    .unwrap(),
                ),
            ))
            .unwrap();

        let next_base = handle.begin_commit().unwrap();
        let commit_handle = Arc::clone(&handle);
        let commit_thread = thread::spawn(move || {
            commit_handle
                .commit(LiveCommitCandidate::new(
                    next_base,
                    Arc::new(
                        LiveQueryView::new(
                            2,
                            Instant::now(),
                            ManifestCut::Absent,
                            2,
                            2,
                            BlockingDrop {
                                value: 2,
                                entered: None,
                                release: None,
                            },
                        )
                        .unwrap(),
                    ),
                ))
                .unwrap();
        });

        entered_rx.recv().unwrap();
        let newest = handle.pin(Instant::now()).unwrap();
        assert_eq!(newest.generation(), 2);
        assert_eq!(newest.payload().value, 2);
        release.wait();
        commit_thread.join().unwrap();
    }

    #[test]
    fn actual_head_decode_does_not_hold_the_published_root_writer_lock() {
        fn head_with_sample(
            value: f64,
            decode_hook: Option<Arc<dyn Fn() + Send + Sync>>,
        ) -> Arc<HeadReadView> {
            let mut labels = VersionedFlatInternedLabelSetStore::default();
            let series = labels
                .intern(&[
                    KeyValueRef::from((METRIC_NAME_LABEL, "decode_lock")),
                    KeyValueRef::from(("host", "a")),
                ])
                .unwrap();
            let mut head = HeadBuffer::new(
                HeadConfig::with_block_size(
                    Duration::from_secs(10),
                    2,
                    FloatEncoding::Gorilla,
                    IntEncoding::DeltaZigZag,
                )
                .with_compact_numeric_series(false),
            )
            .unwrap();
            for offset in 0..3 {
                assert!(
                    head.record_sample_with_outcome(
                        series,
                        1_000 + offset,
                        SampleValue::Float(value + (offset as f64 * 0.25)),
                    )
                    .unwrap()
                    .recorded
                );
            }
            let mut frozen =
                FrozenHeadReadView::from_owned(head.try_freeze_for_publication().unwrap());
            if let Some(hook) = decode_hook {
                frozen.set_decode_hook_for_test(move || hook());
            }
            Arc::new(
                HeadReadView::new(Arc::new(frozen), Arc::new(labels.snapshot().unwrap())).unwrap(),
            )
        }

        let root = tempfile::tempdir().unwrap();
        let sealed = Arc::new(
            SegmentStoreReader::open_manifest_snapshot(root.path(), &ManifestSnapshot::absent())
                .unwrap(),
        );
        let (decode_entered_tx, decode_entered_rx) = mpsc::channel();
        let decode_release = Arc::new(Barrier::new(2));
        let release_for_decode = Arc::clone(&decode_release);
        let first_head = head_with_sample(
            1.0,
            Some(Arc::new(move || {
                decode_entered_tx.send(()).unwrap();
                release_for_decode.wait();
            })),
        );
        let first_payload = LiveStorageView::new(Arc::clone(&sealed), first_head).unwrap();
        let second_head = head_with_sample(2.0, None);
        let second_revision = second_head.catalog_revision();
        let second_payload = LiveStorageView::new(sealed, second_head).unwrap();

        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let first_base = handle.begin_commit().unwrap();
        let first_revision = first_payload.bound_catalog_revision();
        handle
            .commit(LiveCommitCandidate::new(
                first_base,
                Arc::new(
                    LiveQueryView::new_storage(
                        1,
                        Instant::now(),
                        ManifestCut::Absent,
                        1,
                        first_revision,
                        first_payload,
                    )
                    .unwrap(),
                ),
            ))
            .unwrap();

        let query_handle = Arc::clone(&handle);
        let query_thread = thread::spawn(move || {
            let pinned = query_handle.pin(Instant::now()).unwrap();
            let generation = pinned.generation();
            let mut session = pinned
                .payload()
                .sealed()
                .query_session_with_head_view(pinned.payload().head())
                .unwrap();
            let samples = session
                .query_selector(&SegmentSelector::metric("decode_lock"), 0, 2_000)
                .unwrap()
                .into_iter()
                .flat_map(|result| result.samples)
                .collect::<Vec<_>>();
            (generation, samples)
        });
        decode_entered_rx.recv().unwrap();

        let second_base = handle.begin_commit().unwrap();
        let second_view = Arc::new(
            LiveQueryView::new_storage(
                2,
                Instant::now(),
                ManifestCut::Absent,
                2,
                second_revision,
                second_payload,
            )
            .unwrap(),
        );
        let publisher_handle = Arc::clone(&handle);
        let (published_tx, published_rx) = mpsc::channel();
        let publisher_thread = thread::spawn(move || {
            let result =
                publisher_handle.commit(LiveCommitCandidate::new(second_base, second_view));
            published_tx.send(result).unwrap();
        });
        let publication = published_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("publication blocked while an old generation was paused inside head decode");
        publication.unwrap();
        assert_eq!(handle.pin(Instant::now()).unwrap().generation(), 2);

        decode_release.wait();
        assert_eq!(
            query_thread.join().unwrap(),
            (1, vec![(1_000, 1.0), (1_001, 1.25), (1_002, 1.5)])
        );
        publisher_thread.join().unwrap();

        let newest = handle.pin(Instant::now()).unwrap();
        let mut session = newest
            .payload()
            .sealed()
            .query_session_with_head_view(newest.payload().head())
            .unwrap();
        let samples = session
            .query_selector(&SegmentSelector::metric("decode_lock"), 0, 2_000)
            .unwrap()
            .into_iter()
            .flat_map(|result| result.samples)
            .collect::<Vec<_>>();
        assert_eq!(samples, vec![(1_000, 2.0), (1_001, 2.25), (1_002, 2.5)]);
    }

    #[test]
    fn simultaneous_readers_survive_repeated_publications() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        publish(&handle, 1);
        let start = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let mut readers = Vec::new();
        for _ in 0..2 {
            let handle = Arc::clone(&handle);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            readers.push(thread::spawn(move || {
                let pinned = handle.pin(Instant::now()).unwrap();
                start.wait();
                release.wait();
                (pinned.generation(), *pinned.payload())
            }));
        }
        start.wait();
        publish(&handle, 2);
        publish(&handle, 3);
        release.wait();

        for reader in readers {
            assert_eq!(reader.join().unwrap(), (1, 1));
        }
        assert_eq!(handle.pin(Instant::now()).unwrap().generation(), 3);
    }

    #[test]
    fn query_admission_requires_one_nonzero_startup_configuration() {
        let handle = LiveQueryHandle::<u64>::new(Duration::from_secs(10)).unwrap();
        assert_eq!(
            handle.try_pin_admitted(Instant::now()).unwrap_err(),
            LiveViewError::QueryAdmissionUnconfigured
        );
        let governor = LiveMemoryGovernor::new(10).unwrap();
        assert_eq!(
            handle
                .configure_query_admission(Arc::clone(&governor), 0)
                .unwrap_err(),
            LiveViewError::InvalidQueryRetentionBytes
        );
        handle
            .configure_query_admission(Arc::clone(&governor), 1)
            .unwrap();
        assert!(handle.query_admission_configured());
        assert_eq!(
            handle.configure_query_admission(governor, 1).unwrap_err(),
            LiveViewError::QueryAdmissionAlreadyConfigured
        );
    }

    #[test]
    fn admitted_pin_reports_its_exact_root_lock_timing_and_view_cut() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let governor = LiveMemoryGovernor::new(1).unwrap();
        handle
            .configure_query_admission(Arc::clone(&governor), 1)
            .unwrap();
        let base = handle.begin_commit().unwrap();
        handle
            .commit(LiveCommitCandidate::new(
                base,
                view(base.next_generation, 73, 29, 11),
            ))
            .unwrap();

        let pin_started = Instant::now();
        let pin = handle.try_pin_admitted(Instant::now()).unwrap();
        let pin_elapsed = pin_started.elapsed();
        let timing = pin.root_lock_timing();
        assert!(timing.wait <= pin_elapsed);
        assert!(timing.held <= pin_elapsed);
        assert_eq!(
            (
                pin.generation(),
                pin.visible_message_sequence(),
                pin.catalog_revision(),
                *pin.payload(),
            ),
            (1, 73, 29, 11)
        );
        drop(pin);
        assert_eq!(governor.stats().charged_bytes, 0);
    }

    #[test]
    fn concurrent_admitted_pins_saturate_and_release_deterministically() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let governor = LiveMemoryGovernor::new(2).unwrap();
        handle
            .configure_query_admission(Arc::clone(&governor), 1)
            .unwrap();
        publish(&handle, 1);
        let fixed = governor
            .try_charge(LiveMemoryClass::Other, 1)
            .expect("reserve all but one byte");

        let start = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(2));
        let (result_tx, result_rx) = mpsc::channel();
        let mut workers = Vec::new();
        for _ in 0..2 {
            let handle = Arc::clone(&handle);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let result_tx = result_tx.clone();
            workers.push(thread::spawn(move || {
                start.wait();
                match handle.try_pin_admitted(Instant::now()) {
                    Ok(pin) => {
                        result_tx.send(true).unwrap();
                        release.wait();
                        assert_eq!(*pin.payload(), 1);
                    }
                    Err(LiveViewError::ResourcePressure(_)) => {
                        result_tx.send(false).unwrap();
                    }
                    Err(error) => panic!("unexpected admission error: {error}"),
                }
            }));
        }
        drop(result_tx);
        start.wait();

        let mut admitted = [result_rx.recv().unwrap(), result_rx.recv().unwrap()];
        admitted.sort_unstable();
        assert_eq!(admitted, [false, true]);
        assert!(matches!(
            handle.can_admit_query(Instant::now()),
            Err(LiveViewError::ResourcePressure(_))
        ));

        release.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(governor.stats().charged_bytes, 1);
        assert!(handle.can_admit_query(Instant::now()).is_ok());
        drop(fixed);
        assert_eq!(governor.stats().charged_bytes, 0);
    }

    #[test]
    fn pressure_rejects_new_pins_without_invalidating_an_existing_pin() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        let governor = LiveMemoryGovernor::new(2).unwrap();
        handle
            .configure_query_admission(Arc::clone(&governor), 1)
            .unwrap();
        publish(&handle, 1);

        let first = handle.try_pin_admitted(Instant::now()).unwrap();
        let pressure = governor
            .try_charge(LiveMemoryClass::Other, 1)
            .expect("fill the remaining budget");
        assert!(matches!(
            handle.try_pin_admitted(Instant::now()),
            Err(LiveViewError::ResourcePressure(_))
        ));
        assert_eq!((first.generation(), *first.payload()), (1, 1));

        drop(pressure);
        let second = handle.try_pin_admitted(Instant::now()).unwrap();
        assert_eq!(second.generation(), 1);
        drop(first);
        assert_eq!(governor.stats().charged_bytes, 1);
        drop(second);
        assert_eq!(governor.stats().charged_bytes, 0);
    }

    #[test]
    fn generation_overflow_fails_readiness_closed() {
        let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
        {
            let mut state = handle.state.write().unwrap();
            state.current = Some(view(u64::MAX, 7, 7, 7));
            state.readiness = LiveReadiness::Ready;
            state.status_epoch = 41;
        }

        assert_eq!(
            handle.begin_commit().unwrap_err(),
            LiveViewError::GenerationOverflow
        );
        let status = handle.status().unwrap();
        assert_eq!(status.status_epoch, 42);
        assert_eq!(status.generation, Some(u64::MAX));
        assert!(matches!(status.readiness, LiveReadiness::Failed(_)));
        assert!(matches!(
            handle.pin(Instant::now()),
            Err(LiveViewError::Failed(_))
        ));
    }

    #[test]
    fn status_epoch_overflow_fails_readiness_closed() {
        for operation in ["dirty", "failed", "commit"] {
            let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
            {
                let mut state = handle.state.write().unwrap();
                state.current = Some(view(1, 1, 1, 1));
                state.readiness = LiveReadiness::Ready;
                state.status_epoch = u64::MAX;
            }

            let error = match operation {
                "dirty" => handle.mark_dirty(Instant::now()).unwrap_err(),
                "failed" => handle.mark_failed("injected failure").unwrap_err(),
                "commit" => {
                    let base = LiveCommitBase {
                        status_epoch: u64::MAX,
                        next_generation: 2,
                    };
                    handle
                        .commit(LiveCommitCandidate::new(base, view(2, 2, 2, 2)))
                        .unwrap_err()
                }
                _ => unreachable!(),
            };
            assert_eq!(error, LiveViewError::StatusEpochOverflow);
            let status = handle.status().unwrap();
            assert_eq!(status.status_epoch, u64::MAX);
            assert!(matches!(status.readiness, LiveReadiness::Failed(_)));
            assert!(matches!(
                handle.pin(Instant::now()),
                Err(LiveViewError::Failed(_))
            ));
        }
    }

    #[test]
    fn poisoned_state_lock_fails_every_readiness_path_closed() {
        let handle = LiveQueryHandle::<u64>::new(Duration::from_secs(10)).unwrap();
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let handle = Arc::clone(&handle);
            move || {
                let _guard = handle.state.write().unwrap();
                panic!("poison live state");
            }
        }));
        assert!(panic_result.is_err());

        assert_eq!(handle.status().unwrap_err(), LiveViewError::Poisoned);
        assert_eq!(handle.begin_commit().unwrap_err(), LiveViewError::Poisoned);
        assert_eq!(
            handle.pin(Instant::now()).unwrap_err(),
            LiveViewError::Poisoned
        );
        assert_eq!(
            handle.mark_dirty(Instant::now()).unwrap_err(),
            LiveViewError::Poisoned
        );
        assert_eq!(
            handle
                .mark_failed("cannot update poisoned state")
                .unwrap_err(),
            LiveViewError::Poisoned
        );
    }

    fn empty_storage_view() -> (tempfile::TempDir, LiveStorageView) {
        let root = tempfile::tempdir().unwrap();
        let sealed = Arc::new(
            SegmentStoreReader::open_manifest_snapshot(root.path(), &ManifestSnapshot::absent())
                .unwrap(),
        );
        let mut labels = VersionedFlatInternedLabelSetStore::default();
        let head = Arc::new(
            HeadReadView::new(
                Arc::new(FrozenHeadReadView::default()),
                Arc::new(labels.snapshot().unwrap()),
            )
            .unwrap(),
        );
        let payload = LiveStorageView::new(sealed, head).unwrap();
        (root, payload)
    }

    #[test]
    fn storage_view_rejects_an_inventory_without_an_exact_manifest_cut() {
        let root = tempfile::tempdir().unwrap();
        let sealed = Arc::new(SegmentStoreReader::open(root.path()).unwrap());
        let mut labels = VersionedFlatInternedLabelSetStore::default();
        let head = Arc::new(
            HeadReadView::new(
                Arc::new(FrozenHeadReadView::default()),
                Arc::new(labels.snapshot().unwrap()),
            )
            .unwrap(),
        );

        assert_eq!(
            LiveStorageView::new(sealed, head).err().unwrap(),
            LiveViewError::UnboundSealedInventory
        );
    }

    #[test]
    fn storage_query_view_requires_exact_catalog_and_manifest_bindings() {
        let (_root, payload) = empty_storage_view();
        assert!(matches!(
            LiveQueryView::new_storage(1, Instant::now(), ManifestCut::Absent, 0, 1, payload,)
                .err(),
            Some(LiveViewError::CatalogBindingMismatch {
                view_revision: 1,
                head_revision: 0,
            })
        ));

        let (_root, payload) = empty_storage_view();
        assert_eq!(
            LiveQueryView::new_storage(
                1,
                Instant::now(),
                ManifestCut::Present {
                    file_name: "MANIFEST-000001".to_string(),
                    validated_offset: 1,
                    prefix_sha256: [7; 32],
                },
                0,
                0,
                payload,
            )
            .err()
            .unwrap(),
            LiveViewError::ManifestBindingMismatch
        );

        let (_root, payload) = empty_storage_view();
        let view =
            LiveQueryView::new_storage(1, Instant::now(), ManifestCut::Absent, 0, 0, payload)
                .unwrap();
        assert_eq!(view.catalog_revision(), 0);
        assert_eq!(view.manifest_cut(), &ManifestCut::Absent);
    }
}
