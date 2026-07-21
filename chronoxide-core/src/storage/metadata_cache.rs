//! Store-wide, byte-governed cache for immutable segment metadata.
//!
//! The cache is deliberately independent of metadata readers. Callers provide
//! an immutable file/range/class key, reserve a declared allocation bound, and
//! return a fully validated value plus its measured logical byte charge.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::collections::hash_map::{Entry, RandomState};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};

use hashlink::LinkedHashMap;
use hashlink::linked_hash_map::Entry as LinkedEntry;
use thiserror::Error;

use crate::util::{XxHash64, xxhash64};

pub use super::metadata_governor::{
    METADATA_CACHE_CLASS_COUNT, METADATA_CACHE_CLASS_ORDER, MetadataCacheClass,
};
use super::metadata_governor::{
    MetadataBudgetError, MetadataCharge, MetadataGovernor, MetadataGovernorStats, MetadataPin,
    MetadataUsageClass, admit_cache_allocation,
};
use super::segment::SegmentFile;

// Logical fixed bookkeeping charges. They intentionally describe governed
// bytes, not allocator-specific usable sizes.
pub(super) const LIVE_REGISTRY_ENTRY_BYTES: u64 = 128;
pub(super) const RESIDENT_ENTRY_BYTES: u64 = 128;
const SINGLE_FLIGHT_BOOKKEEPING_BYTES: u64 = 192;
const MAX_TRANSIENT_MESSAGE_BYTES: usize = 1024;
pub(super) const SINGLE_FLIGHT_ENTRY_BYTES: u64 =
    SINGLE_FLIGHT_BOOKKEEPING_BYTES + MAX_TRANSIENT_MESSAGE_BYTES as u64;
const CORRUPTION_LEDGER_ENTRY_BYTES: u64 = 128;
const MAX_CORRUPTION_MESSAGE_BYTES: usize = 1024;
const MAX_CORRUPTION_LEDGER_CHARGE_BYTES: u64 =
    CORRUPTION_LEDGER_ENTRY_BYTES + MAX_CORRUPTION_MESSAGE_BYTES as u64;

/// Immutable identity of one typed metadata range.
///
/// The Rust value type is also part of the internal key, preventing two
/// decoders from reusing the same bytes as incompatible decoded values.
#[derive(Clone)]
pub struct MetadataCacheKey {
    artifact: ArtifactKey,
    offset: u64,
    length: u64,
    class: MetadataCacheClass,
    prehash: u64,
}

impl MetadataCacheKey {
    pub fn new(
        segment_identity: impl Into<Arc<str>>,
        file: SegmentFile,
        offset: u64,
        length: u64,
        class: MetadataCacheClass,
    ) -> Result<Self, MetadataCacheKeyError> {
        let segment_identity = MetadataSegmentIdentity::new(segment_identity.into());
        if segment_identity.as_str().is_empty() {
            return Err(MetadataCacheKeyError::EmptySegmentIdentity);
        }
        Self::with_artifact(
            ArtifactKey::new(segment_identity, file),
            offset,
            length,
            class,
        )
    }

    pub(super) fn with_artifact(
        artifact: ArtifactKey,
        offset: u64,
        length: u64,
        class: MetadataCacheClass,
    ) -> Result<Self, MetadataCacheKeyError> {
        debug_assert!(!artifact.segment_identity().is_empty());
        let file = artifact.file();
        if !is_cacheable_metadata_file(file) {
            return Err(MetadataCacheKeyError::UnsupportedFile { file });
        }
        if length == 0 {
            return Err(MetadataCacheKeyError::EmptyRange);
        }
        offset
            .checked_add(length)
            .ok_or(MetadataCacheKeyError::RangeOverflow { offset, length })?;
        let prehash = prehash(&(artifact.prehash, offset, length, class));
        Ok(Self {
            artifact,
            offset,
            length,
            class,
            prehash,
        })
    }

    pub fn segment_identity(&self) -> &str {
        self.artifact.segment_identity()
    }

    pub fn file(&self) -> SegmentFile {
        self.artifact.file()
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn class(&self) -> MetadataCacheClass {
        self.class
    }

    fn artifact_key(&self) -> ArtifactKey {
        self.artifact.clone()
    }
}

impl fmt::Debug for MetadataCacheKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCacheKey")
            .field("segment_identity", &self.artifact.segment_identity())
            .field("file", &self.artifact.file())
            .field("offset", &self.offset)
            .field("length", &self.length)
            .field("class", &self.class)
            .finish()
    }
}

impl Hash for MetadataCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.prehash);
    }
}

impl PartialEq for MetadataCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.artifact == other.artifact
            && self.offset == other.offset
            && self.length == other.length
            && self.class == other.class
    }
}

impl Eq for MetadataCacheKey {}

/// Stable segment identity retained by one governed artifact reader.
///
/// The cached hash is only a lookup accelerator. Equality still compares the
/// complete stable identity, so a hash collision cannot cross segment or
/// generation/corruption boundaries.
#[derive(Clone)]
pub(super) struct MetadataSegmentIdentity {
    value: Arc<str>,
    prehash: u64,
}

impl MetadataSegmentIdentity {
    pub(super) fn new(value: Arc<str>) -> Self {
        let prehash = xxhash64(value.as_bytes());
        Self { value, prehash }
    }

    pub(super) fn as_str(&self) -> &str {
        &self.value
    }

    fn to_arc(&self) -> Arc<str> {
        Arc::clone(&self.value)
    }

    fn prehash(&self) -> u64 {
        self.prehash
    }
}

impl PartialEq for MetadataSegmentIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for MetadataSegmentIdentity {}

impl Hash for MetadataSegmentIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.prehash);
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCacheKeyError {
    #[error("stable segment identity must not be empty")]
    EmptySegmentIdentity,
    #[error("{file:?} is not an immutable metadata file")]
    UnsupportedFile { file: SegmentFile },
    #[error("metadata cache range must not be empty")]
    EmptyRange,
    #[error("metadata cache range overflows: offset={offset} length={length}")]
    RangeOverflow { offset: u64, length: u64 },
}

/// A fully validated load result and its measured logical allocation size.
#[derive(Debug)]
pub struct LoadedMetadata<T> {
    pub value: T,
    pub charged_bytes: u64,
    scratch_charge: Option<MetadataCharge>,
}

impl<T> LoadedMetadata<T> {
    pub fn new(value: T, charged_bytes: u64) -> Self {
        Self {
            value,
            charged_bytes,
            scratch_charge: None,
        }
    }

    /// Carries validated read scratch into cache admission so the governor can
    /// exchange it for final bookkeeping without exposing released capacity.
    pub(super) fn with_scratch_charge(mut self, scratch_charge: MetadataCharge) -> Self {
        assert!(
            self.scratch_charge.is_none(),
            "loaded metadata already owns a scratch charge"
        );
        self.scratch_charge = Some(scratch_charge);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuralMetadataErrorKind {
    InvalidData,
    UnexpectedEof,
}

/// The first structural error retained for a stable segment artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCorruption {
    pub kind: StructuralMetadataErrorKind,
    pub message: Arc<str>,
}

impl fmt::Display for MetadataCorruption {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

/// Cloneable load result shared with every waiter on one single-flight miss.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MetadataCacheError {
    #[error(transparent)]
    Budget(#[from] MetadataBudgetError),
    #[error("structural metadata corruption: {0}")]
    Structural(MetadataCorruption),
    #[error("transient metadata load error ({kind:?}): {message}")]
    Transient {
        kind: io::ErrorKind,
        message: Arc<str>,
    },
    #[error(
        "metadata loader exceeded its declared allocation bound: declared={declared_bytes} actual={actual_bytes}"
    )]
    DeclaredBoundExceeded {
        declared_bytes: u64,
        actual_bytes: u64,
    },
    #[error("metadata cache internal type mismatch")]
    TypeMismatch,
    #[error("metadata artifact is not registered in active inventory: {segment_identity}/{file:?}")]
    UnregisteredArtifact {
        segment_identity: Arc<str>,
        file: SegmentFile,
    },
    #[error("metadata artifact is retiring from inventory: {segment_identity}/{file:?}")]
    RetiringArtifact {
        segment_identity: Arc<str>,
        file: SegmentFile,
    },
}

impl MetadataCacheError {
    pub fn from_io(error: io::Error) -> Self {
        let kind = error.kind();
        let message: Arc<str> = Arc::from(error.to_string());
        match kind {
            io::ErrorKind::InvalidData => Self::Structural(MetadataCorruption {
                kind: StructuralMetadataErrorKind::InvalidData,
                message,
            }),
            io::ErrorKind::UnexpectedEof => Self::Structural(MetadataCorruption {
                kind: StructuralMetadataErrorKind::UnexpectedEof,
                message,
            }),
            _ => Self::Transient {
                kind,
                message: bounded_message(message, MAX_TRANSIENT_MESSAGE_BYTES),
            },
        }
    }

    pub fn transient(kind: io::ErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self::Transient {
            kind,
            message: bounded_message(message.into(), MAX_TRANSIENT_MESSAGE_BYTES),
        }
    }

    pub fn structural(kind: StructuralMetadataErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self::Structural(bounded_corruption(MetadataCorruption {
            kind,
            message: message.into(),
        }))
    }
}

/// Cache-owned charge totals for one stable metadata class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataCacheClassStats {
    pub class: MetadataCacheClass,
    pub in_flight_bytes: u64,
    pub retained_bytes: u64,
    pub peak_in_flight_bytes: u64,
    pub peak_retained_bytes: u64,
}

/// Monotonic resident-admission counters for one stable metadata class.
///
/// These counters describe the post-validation governor decision, not load
/// completion or current residency. An admitted handoff can therefore be
/// counted even if concurrent artifact retirement prevents publication of a
/// resident entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataCacheClassAdmissionStats {
    pub class: MetadataCacheClass,
    /// Validated allocations transferred to retained accounting with resident
    /// bookkeeping.
    pub resident_admissions: u64,
    /// Enabled resident-admission attempts refused by the retained governor.
    pub resident_admission_refusals: u64,
    /// Validated allocations for which residency was disabled, so no retained
    /// admission was attempted.
    pub resident_admission_bypasses: u64,
}

/// Deterministic aggregate cache counters and current entry counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub single_flight_waits: u64,
    /// Successful load outcomes, independent of whether residency was
    /// admitted, refused, or bypassed.
    pub successful_loads: u64,
    pub failed_loads: u64,
    /// Validated allocations transferred to retained accounting with resident
    /// bookkeeping.
    pub resident_admissions: u64,
    /// Enabled resident-admission attempts refused by the retained governor.
    pub resident_admission_refusals: u64,
    /// Validated allocations for which residency was disabled, so no retained
    /// admission was attempted.
    pub resident_admission_bypasses: u64,
    pub corruption_detections: u64,
    pub corruption_hits: u64,
    pub resident_entries: u64,
    pub live_allocations: u64,
    pub active_loads: u64,
    pub registered_artifacts: u64,
    pub ledger_reserved_bytes: u64,
    pub ledger_in_flight_bytes: u64,
    pub ledger_retained_bytes: u64,
    pub sticky_artifacts: u64,
    pub sticky_charged_bytes: u64,
    pub class_charges: [MetadataCacheClassStats; METADATA_CACHE_CLASS_COUNT],
    pub class_admissions: [MetadataCacheClassAdmissionStats; METADATA_CACHE_CLASS_COUNT],
}

impl Default for MetadataCacheStats {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            single_flight_waits: 0,
            successful_loads: 0,
            failed_loads: 0,
            resident_admissions: 0,
            resident_admission_refusals: 0,
            resident_admission_bypasses: 0,
            corruption_detections: 0,
            corruption_hits: 0,
            resident_entries: 0,
            live_allocations: 0,
            active_loads: 0,
            registered_artifacts: 0,
            ledger_reserved_bytes: 0,
            ledger_in_flight_bytes: 0,
            ledger_retained_bytes: 0,
            sticky_artifacts: 0,
            sticky_charged_bytes: 0,
            class_charges: METADATA_CACHE_CLASS_ORDER.map(MetadataCacheClassStats::zero),
            class_admissions: METADATA_CACHE_CLASS_ORDER
                .map(MetadataCacheClassAdmissionStats::zero),
        }
    }
}

impl MetadataCacheClassStats {
    const fn zero(class: MetadataCacheClass) -> Self {
        Self {
            class,
            in_flight_bytes: 0,
            retained_bytes: 0,
            peak_in_flight_bytes: 0,
            peak_retained_bytes: 0,
        }
    }
}

impl MetadataCacheClassAdmissionStats {
    const fn zero(class: MetadataCacheClass) -> Self {
        Self {
            class,
            resident_admissions: 0,
            resident_admission_refusals: 0,
            resident_admission_bypasses: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataArtifactRetirement {
    NotRegistered,
    Removed,
    Deferred,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MetadataArtifactRegistrationError {
    #[error("stable segment identity must not be empty")]
    EmptySegmentIdentity,
    #[error("metadata artifact batch must not be empty")]
    EmptyArtifactBatch,
    #[error("{file:?} is not tracked by the canonical segment footer")]
    UnsupportedFile { file: SegmentFile },
    #[error("metadata artifact batch contains duplicate file {file:?}")]
    DuplicateFile { file: SegmentFile },
    #[error(
        "metadata artifact batch is not in canonical footer order: {file:?} follows {previous:?}"
    )]
    NonCanonicalOrder {
        previous: SegmentFile,
        file: SegmentFile,
    },
    #[error("stable segment identity is too large to charge")]
    SegmentIdentityTooLarge,
    #[error(transparent)]
    Budget(#[from] MetadataBudgetError),
    #[error(
        "metadata artifact batch has partial or mixed inventory for {segment_identity}: \
         {registered} of {requested} artifacts are registered"
    )]
    PartialInventory {
        segment_identity: Arc<str>,
        registered: usize,
        requested: usize,
    },
    #[error("metadata artifact is already retiring: {segment_identity}/{file:?}")]
    Retiring {
        segment_identity: Arc<str>,
        file: SegmentFile,
    },
}

/// Aggregate store-wide metadata cache.
#[derive(Clone)]
pub struct MetadataCache {
    inner: Arc<MetadataCacheInner>,
}

struct MetadataCacheInner {
    governor: Arc<MetadataGovernor>,
    state: Mutex<CacheState>,
    flight_installer: AtomicBool,
}

#[derive(Default)]
struct CacheState {
    // The linked map is both the resident-key index and the recency list.
    // Front is least recently used; back is most recently used. Keeping one
    // structure makes hit promotion, oldest eviction, and keyed retirement
    // expected O(1) without a second cloned-key queue that can diverge.
    resident: LinkedHashMap<FullKey, ResidentEntry, RandomState>,
    live: HashMap<FullKey, LiveEntry>,
    flights: HashMap<FullKey, Arc<Flight>>,
    active_allocations_by_artifact: HashMap<ArtifactKey, u64>,
    active_flights_by_artifact: HashMap<ArtifactKey, u64>,
    inventory: HashMap<ArtifactKey, InventoryEntry>,
    next_allocation_id: u64,
    stats: CacheCounters,
}

#[derive(Default)]
struct CacheCounters {
    hits: u64,
    misses: u64,
    evictions: u64,
    single_flight_waits: u64,
    successful_loads: u64,
    failed_loads: u64,
    resident_admissions: u64,
    resident_admission_refusals: u64,
    resident_admission_bypasses: u64,
    class_admissions: [ResidentAdmissionCounters; METADATA_CACHE_CLASS_COUNT],
    corruption_detections: u64,
    corruption_hits: u64,
}

#[derive(Default)]
struct ResidentAdmissionCounters {
    admissions: u64,
    refusals: u64,
    bypasses: u64,
}

#[derive(Clone, Copy)]
enum ResidentAdmissionOutcome {
    Admitted,
    Refused,
    Bypassed,
}

impl CacheCounters {
    fn record_resident_admission(
        &mut self,
        class: MetadataCacheClass,
        outcome: ResidentAdmissionOutcome,
    ) {
        let class = &mut self.class_admissions[class.stable_index()];
        match outcome {
            ResidentAdmissionOutcome::Admitted => {
                self.resident_admissions = self.resident_admissions.saturating_add(1);
                class.admissions = class.admissions.saturating_add(1);
            }
            ResidentAdmissionOutcome::Refused => {
                self.resident_admission_refusals =
                    self.resident_admission_refusals.saturating_add(1);
                class.refusals = class.refusals.saturating_add(1);
            }
            ResidentAdmissionOutcome::Bypassed => {
                self.resident_admission_bypasses =
                    self.resident_admission_bypasses.saturating_add(1);
                class.bypasses = class.bypasses.saturating_add(1);
            }
        }
    }
}

#[derive(Clone)]
struct FullKey {
    range: MetadataCacheKey,
    value_type: TypeId,
    prehash: u64,
}

impl FullKey {
    fn new(range: MetadataCacheKey, value_type: TypeId) -> Self {
        let prehash = prehash(&(range.prehash, value_type));
        Self {
            range,
            value_type,
            prehash,
        }
    }
}

impl PartialEq for FullKey {
    fn eq(&self, other: &Self) -> bool {
        self.range == other.range && self.value_type == other.value_type
    }
}

impl Eq for FullKey {}

impl Hash for FullKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.prehash);
    }
}

#[derive(Clone)]
pub(super) struct ArtifactKey {
    segment_identity: MetadataSegmentIdentity,
    file: SegmentFile,
    prehash: u64,
}

impl ArtifactKey {
    pub(super) fn new(segment_identity: MetadataSegmentIdentity, file: SegmentFile) -> Self {
        let prehash = prehash(&(segment_identity.prehash(), segment_file_rank(file)));
        Self {
            segment_identity,
            file,
            prehash,
        }
    }

    pub(super) fn segment_identity(&self) -> &str {
        self.segment_identity.as_str()
    }

    pub(super) fn file(&self) -> SegmentFile {
        self.file
    }
}

impl PartialEq for ArtifactKey {
    fn eq(&self, other: &Self) -> bool {
        self.segment_identity == other.segment_identity && self.file == other.file
    }
}

impl Eq for ArtifactKey {}

impl Hash for ArtifactKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.prehash);
    }
}

type ErasedAllocation = Arc<dyn Any + Send + Sync>;
type WeakErasedAllocation = Weak<dyn Any + Send + Sync>;

struct LiveEntry {
    allocation_id: u64,
    allocation: WeakErasedAllocation,
}

struct ResidentEntry {
    // Field order is intentional: Rust drops fields in declaration order, so
    // the resident charge is released before this cache-owned pin can destroy
    // the final allocation and allow artifact retirement to complete.
    _bookkeeping_charge: MetadataCharge,
    allocation: ErasedAllocation,
}

struct InventoryEntry {
    corruption: Option<MetadataCorruption>,
    _ledger_charge: MetadataCharge,
    retirement_requested: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactBatchInventoryState {
    Vacant,
    Active,
    Retiring { file: SegmentFile },
    PartialOrMixed { registered: usize },
}

struct Flight {
    result: Mutex<Option<Result<ErasedAllocation, MetadataCacheError>>>,
    completed: Condvar,
    bookkeeping_charge: Option<MetadataCharge>,
    owner: Weak<MetadataCacheInner>,
    artifact: ArtifactKey,
    inventory_tracked: AtomicBool,
}

struct CacheAllocation<T> {
    value: Option<MetadataPin<T>>,
    live_bookkeeping_charge: Option<MetadataCharge>,
    owner: Weak<MetadataCacheInner>,
    key: FullKey,
    allocation_id: u64,
    inventory_tracked: AtomicBool,
}

/// Typed pin to one cache allocation. Clones never add a metadata charge.
pub struct MetadataCachePin<T> {
    allocation: Arc<CacheAllocation<T>>,
}

impl<T> MetadataCachePin<T> {
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.allocation, &other.allocation)
    }

    pub fn charged_bytes(&self) -> u64 {
        self.allocation
            .value
            .as_ref()
            .expect("live cache allocation value")
            .charged_bytes()
    }
}

impl<T> Clone for MetadataCachePin<T> {
    fn clone(&self) -> Self {
        Self {
            allocation: Arc::clone(&self.allocation),
        }
    }
}

impl<T> Deref for MetadataCachePin<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.allocation
            .value
            .as_ref()
            .expect("live cache allocation value")
    }
}

impl<T: fmt::Debug> fmt::Debug for MetadataCachePin<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCachePin")
            .field("value", &**self)
            .field("charged_bytes", &self.charged_bytes())
            .finish_non_exhaustive()
    }
}

impl<T> Drop for CacheAllocation<T> {
    fn drop(&mut self) {
        // Destroy the decoded value and its final-allocation charge before
        // removing the authoritative activity count. Value destruction may
        // release governed file handles, so it must happen without cache or
        // governor mutex nesting.
        drop(self.value.take());

        let owner = self.owner.upgrade();
        if let Some(owner) = &owner {
            let mut state = lock(&owner.state);
            let remove = state
                .live
                .get(&self.key)
                .is_some_and(|entry| entry.allocation_id == self.allocation_id);
            if remove {
                state.live.remove(&self.key);
            }
        }

        // The weak live-registry entry may already have been reaped by a
        // racing lookup. Keep the authoritative allocation count until both
        // allocation-owned charges are gone.
        drop(self.live_bookkeeping_charge.take());

        if !self.inventory_tracked.swap(false, Ordering::AcqRel) {
            return;
        }
        let Some(owner) = owner else {
            return;
        };
        let retired = {
            let mut state = lock(&owner.state);
            decrement_active_artifact_count(
                &mut state.active_allocations_by_artifact,
                &self.key.range.artifact_key(),
                "cache allocation",
            );
            detach_retired_inventory_if_quiescent(&mut state, &self.key.range.artifact_key())
        };
        drop(retired);
    }
}

impl MetadataCache {
    pub fn new(governor: Arc<MetadataGovernor>) -> Self {
        Self {
            inner: Arc::new(MetadataCacheInner {
                governor,
                state: Mutex::new(CacheState::default()),
                flight_installer: AtomicBool::new(false),
            }),
        }
    }

    pub fn governor(&self) -> &Arc<MetadataGovernor> {
        &self.inner.governor
    }

    /// Pre-registers and precharges one active immutable metadata artifact.
    ///
    /// Registration is a store-open/inventory operation and must complete
    /// before any range from the artifact is loaded. Its bounded charge
    /// guarantees that the first structural error can become sticky without a
    /// fallible record-time allocation or reservation.
    pub fn register_artifact(
        &self,
        segment_identity: impl Into<Arc<str>>,
        file: SegmentFile,
    ) -> Result<(), MetadataArtifactRegistrationError> {
        self.register_artifacts(segment_identity, &[file])
    }

    /// Pre-registers one canonical, nonempty batch of immutable metadata
    /// artifacts as one inventory publication.
    ///
    /// Every input and ledger charge is validated and reserved before the
    /// cache inventory changes. An exact all-active batch is idempotent; a
    /// partial, mixed, or retiring batch is rejected without publication.
    pub fn register_artifacts(
        &self,
        segment_identity: impl Into<Arc<str>>,
        files: &[SegmentFile],
    ) -> Result<(), MetadataArtifactRegistrationError> {
        let segment_identity = MetadataSegmentIdentity::new(segment_identity.into());
        let artifacts = validate_artifact_batch(&segment_identity, files)?;
        let ledger_charge_bytes = corruption_ledger_charge_bytes(segment_identity.as_str())
            .ok_or(MetadataArtifactRegistrationError::SegmentIdentityTooLarge)?;

        loop {
            let Some(installer) = FlightInstallerGuard::try_acquire(&self.inner.flight_installer)
            else {
                std::thread::yield_now();
                continue;
            };
            let inventory_state = {
                let state = lock(&self.inner.state);
                artifact_batch_inventory_state(&state, files, &artifacts)
            };
            match inventory_state {
                ArtifactBatchInventoryState::Vacant => {}
                ArtifactBatchInventoryState::Active => return Ok(()),
                ArtifactBatchInventoryState::Retiring { file } => {
                    return Err(MetadataArtifactRegistrationError::Retiring {
                        segment_identity: segment_identity.to_arc(),
                        file,
                    });
                }
                ArtifactBatchInventoryState::PartialOrMixed { registered } => {
                    return Err(MetadataArtifactRegistrationError::PartialInventory {
                        segment_identity: segment_identity.to_arc(),
                        registered,
                        requested: artifacts.len(),
                    });
                }
            }

            let mut ledger_charges = Vec::with_capacity(artifacts.len());
            for _ in &artifacts {
                let ledger_charge = self.inner.governor.reserve_in_flight_for_usage(
                    ledger_charge_bytes,
                    MetadataUsageClass::CorruptionLedger,
                );
                let mut ledger_charge = match ledger_charge {
                    Ok(charge) => charge,
                    Err(error) => {
                        // Earlier batch reservations may already be retained.
                        // Release every one outside both cache mutexes before
                        // reporting an all-or-none registration failure.
                        drop(ledger_charges);
                        drop(installer);
                        return Err(error.into());
                    }
                };
                let _ = ledger_charge.try_promote_to_retained();
                ledger_charges.push(ledger_charge);
            }

            let mut unpublished_charges = Some(ledger_charges);
            let publication_state = {
                let mut state = lock(&self.inner.state);
                let publication_state = artifact_batch_inventory_state(&state, files, &artifacts);
                if publication_state == ArtifactBatchInventoryState::Vacant {
                    // Reserve the table before moving the first governed
                    // charge so publication itself cannot grow it halfway
                    // through the batch.
                    state.inventory.reserve(artifacts.len());
                    let charges = unpublished_charges
                        .take()
                        .expect("unpublished artifact ledger charges");
                    for (artifact, ledger_charge) in artifacts.iter().cloned().zip(charges) {
                        let Entry::Vacant(entry) = state.inventory.entry(artifact) else {
                            unreachable!("batch artifact publication invariant")
                        };
                        entry.insert(InventoryEntry {
                            corruption: None,
                            _ledger_charge: ledger_charge,
                            retirement_requested: false,
                        });
                    }
                }
                publication_state
            };

            // If inventory changed despite the installer phase, none of the
            // candidate charges were published. Destroy them without the
            // cache mutex before returning the observed state.
            drop(unpublished_charges);
            drop(installer);
            return match publication_state {
                ArtifactBatchInventoryState::Vacant | ArtifactBatchInventoryState::Active => Ok(()),
                ArtifactBatchInventoryState::Retiring { file } => {
                    Err(MetadataArtifactRegistrationError::Retiring {
                        segment_identity: segment_identity.to_arc(),
                        file,
                    })
                }
                ArtifactBatchInventoryState::PartialOrMixed { registered } => {
                    Err(MetadataArtifactRegistrationError::PartialInventory {
                        segment_identity: segment_identity.to_arc(),
                        registered,
                        requested: artifacts.len(),
                    })
                }
            };
        }
    }

    /// Checks the non-evictable structural-error state for a footer-tracked
    /// artifact before an FD acquisition, cache hit, or positional read.
    pub fn check_artifact(
        &self,
        segment_identity: impl Into<Arc<str>>,
        file: SegmentFile,
    ) -> Result<(), MetadataCacheError> {
        let segment_identity = MetadataSegmentIdentity::new(segment_identity.into());
        self.check_artifact_with_identity(&segment_identity, file)
    }

    pub(super) fn check_artifact_with_identity(
        &self,
        segment_identity: &MetadataSegmentIdentity,
        file: SegmentFile,
    ) -> Result<(), MetadataCacheError> {
        let artifact = ArtifactKey::new(segment_identity.clone(), file);
        self.check_artifact_with_key(&artifact)
    }

    pub(super) fn check_artifact_with_key(
        &self,
        artifact: &ArtifactKey,
    ) -> Result<(), MetadataCacheError> {
        let mut state = lock(&self.inner.state);
        match artifact_error_locked(&mut state, artifact) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Records the first structural error for an artifact after an external
    /// operation has released its FD leases and temporary reservations.
    ///
    /// Transient errors are returned unchanged. Structural errors require a
    /// prior `register_artifact` call, whose precharge makes this first-error-
    /// wins update non-fallible and independent of cache or FD residency.
    pub fn record_artifact_error(
        &self,
        segment_identity: impl Into<Arc<str>>,
        file: SegmentFile,
        error: MetadataCacheError,
    ) -> MetadataCacheError {
        let segment_identity = MetadataSegmentIdentity::new(segment_identity.into());
        self.record_artifact_error_with_identity(&segment_identity, file, error)
    }

    pub(super) fn record_artifact_error_with_identity(
        &self,
        segment_identity: &MetadataSegmentIdentity,
        file: SegmentFile,
        error: MetadataCacheError,
    ) -> MetadataCacheError {
        let artifact = ArtifactKey::new(segment_identity.clone(), file);
        self.record_artifact_error_with_key(&artifact, error)
    }

    pub(super) fn record_artifact_error_with_key(
        &self,
        artifact: &ArtifactKey,
        error: MetadataCacheError,
    ) -> MetadataCacheError {
        let MetadataCacheError::Structural(corruption) = error else {
            return error;
        };
        let corruption = bounded_corruption(corruption);
        let mut state = lock(&self.inner.state);
        let Some(entry) = state.inventory.get_mut(artifact) else {
            return MetadataCacheError::UnregisteredArtifact {
                segment_identity: artifact.segment_identity.to_arc(),
                file: artifact.file,
            };
        };
        if let Some(existing) = &entry.corruption {
            return MetadataCacheError::Structural(existing.clone());
        }
        entry.corruption = Some(corruption.clone());
        state.stats.corruption_detections = state.stats.corruption_detections.saturating_add(1);
        MetadataCacheError::Structural(corruption)
    }

    /// Returns an existing allocation or loads one under a declared bound.
    ///
    /// `declared_max_bytes` must cover all owned memory the loader may allocate
    /// for the final `T`. The reservation exists before `load` is called. The
    /// loader must validate the complete touched range before returning.
    pub fn get_or_load<T, F>(
        &self,
        key: MetadataCacheKey,
        declared_max_bytes: u64,
        load: F,
    ) -> Result<MetadataCachePin<T>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Result<LoadedMetadata<T>, MetadataCacheError>,
    {
        let key = FullKey::new(key, TypeId::of::<T>());

        loop {
            if let Some(result) = self.lookup::<T>(&key)? {
                return result;
            }

            let Some(installer) = FlightInstallerGuard::try_acquire(&self.inner.flight_installer)
            else {
                std::thread::yield_now();
                continue;
            };
            if let Some(role) = self.probe(&key) {
                drop(installer);
                return match role {
                    FlightRole::Immediate(result) => erased_result::<T>(result),
                    FlightRole::Wait(flight) => erased_result::<T>(flight.wait()),
                    FlightRole::Lead => unreachable!(),
                };
            }

            // The atomic installer gate serializes this reserve-and-publish
            // interval without holding the cache or governor mutex. Therefore
            // only the flight that can be installed owns this charge; waiters
            // never reserve duplicate candidate bookkeeping.
            let flight_charge = self.inner.governor.reserve_in_flight_for_usage(
                SINGLE_FLIGHT_ENTRY_BYTES,
                MetadataUsageClass::Cache(key.range.class),
            )?;
            let candidate = Arc::new(Flight {
                result: Mutex::new(None),
                completed: Condvar::new(),
                bookkeeping_charge: Some(flight_charge),
                owner: Arc::downgrade(&self.inner),
                artifact: key.range.artifact_key(),
                inventory_tracked: AtomicBool::new(false),
            });

            let role = {
                let mut state = lock(&self.inner.state);
                if let Some(error) = inventory_error_locked(&mut state, &key) {
                    FlightRole::Immediate(Err(error))
                } else if let Some(allocation) = resident_or_live_locked(&mut state, &key) {
                    FlightRole::Immediate(Ok(allocation))
                } else if let Some(existing) = state.flights.get(&key).cloned() {
                    state.stats.single_flight_waits =
                        state.stats.single_flight_waits.saturating_add(1);
                    FlightRole::Wait(existing)
                } else {
                    state.stats.misses = state.stats.misses.saturating_add(1);
                    let active = state
                        .active_flights_by_artifact
                        .entry(key.range.artifact_key())
                        .or_default();
                    *active = active.checked_add(1).expect("active flight count overflow");
                    candidate.inventory_tracked.store(true, Ordering::Release);
                    match state.flights.entry(key.clone()) {
                        Entry::Vacant(entry) => {
                            entry.insert(Arc::clone(&candidate));
                        }
                        Entry::Occupied(_) => unreachable!("single-flight insertion invariant"),
                    }
                    FlightRole::Lead
                }
            };
            drop(installer);
            match role {
                FlightRole::Immediate(result) => {
                    drop(candidate);
                    return erased_result::<T>(result);
                }
                FlightRole::Wait(existing) => {
                    drop(candidate);
                    return erased_result::<T>(existing.wait());
                }
                FlightRole::Lead => {
                    return self.load_as_leader(key, candidate, declared_max_bytes, load);
                }
            }
        }
    }

    fn probe(&self, key: &FullKey) -> Option<FlightRole> {
        let mut state = lock(&self.inner.state);
        if let Some(error) = inventory_error_locked(&mut state, key) {
            Some(FlightRole::Immediate(Err(error)))
        } else if let Some(allocation) = resident_or_live_locked(&mut state, key) {
            Some(FlightRole::Immediate(Ok(allocation)))
        } else if let Some(existing) = state.flights.get(key).cloned() {
            state.stats.single_flight_waits = state.stats.single_flight_waits.saturating_add(1);
            Some(FlightRole::Wait(existing))
        } else {
            None
        }
    }

    pub fn stats(&self) -> MetadataCacheStats {
        // Governor and cache snapshots are intentionally independent; never
        // nest their mutexes. The usage and aggregate byte fields within the
        // governor snapshot are mutually atomic.
        let governor = self.inner.governor.stats();
        let ledger = governor.usage(MetadataUsageClass::CorruptionLedger);
        let ledger_in_flight_bytes = ledger.in_flight_bytes;
        let ledger_retained_bytes = ledger.retained_bytes;
        let ledger_reserved_bytes = ledger_in_flight_bytes
            .checked_add(ledger_retained_bytes)
            .expect("metadata corruption-ledger charge overflow");
        let class_charges = METADATA_CACHE_CLASS_ORDER.map(|class| {
            let usage = governor.usage(MetadataUsageClass::Cache(class));
            MetadataCacheClassStats {
                class,
                in_flight_bytes: usage.in_flight_bytes,
                retained_bytes: usage.retained_bytes,
                peak_in_flight_bytes: usage.peak_in_flight_bytes,
                peak_retained_bytes: usage.peak_retained_bytes,
            }
        });
        let state = lock(&self.inner.state);
        let class_admissions = METADATA_CACHE_CLASS_ORDER.map(|class| {
            let counters = &state.stats.class_admissions[class.stable_index()];
            MetadataCacheClassAdmissionStats {
                class,
                resident_admissions: counters.admissions,
                resident_admission_refusals: counters.refusals,
                resident_admission_bypasses: counters.bypasses,
            }
        });
        let sticky_artifacts = state
            .inventory
            .values()
            .filter(|entry| entry.corruption.is_some())
            .count() as u64;
        let sticky_charged_bytes = state
            .inventory
            .values()
            .filter(|entry| entry.corruption.is_some())
            .map(|entry| entry._ledger_charge.bytes())
            .fold(0u64, u64::saturating_add);
        let live_allocations = checked_activity_total(
            state.active_allocations_by_artifact.values().copied(),
            "active allocation count overflow",
        );
        let active_loads = checked_activity_total(
            state.active_flights_by_artifact.values().copied(),
            "active flight count overflow",
        );
        MetadataCacheStats {
            hits: state.stats.hits,
            misses: state.stats.misses,
            evictions: state.stats.evictions,
            single_flight_waits: state.stats.single_flight_waits,
            successful_loads: state.stats.successful_loads,
            failed_loads: state.stats.failed_loads,
            resident_admissions: state.stats.resident_admissions,
            resident_admission_refusals: state.stats.resident_admission_refusals,
            resident_admission_bypasses: state.stats.resident_admission_bypasses,
            corruption_detections: state.stats.corruption_detections,
            corruption_hits: state.stats.corruption_hits,
            resident_entries: state.resident.len() as u64,
            live_allocations,
            active_loads,
            registered_artifacts: state.inventory.len() as u64,
            ledger_reserved_bytes,
            ledger_in_flight_bytes,
            ledger_retained_bytes,
            sticky_artifacts,
            sticky_charged_bytes,
            class_charges,
            class_admissions,
        }
    }

    pub fn governor_stats(&self) -> MetadataGovernorStats {
        self.inner.governor.stats()
    }

    /// Signals that an artifact has left canonical inventory and all external
    /// file handles have retired. Its sticky entry is removed only after this
    /// cache also has no live pin, resident entry, waiter, or load for it.
    pub fn retire_artifact_after_inventory_removal(
        &self,
        segment_identity: &str,
        file: SegmentFile,
    ) -> MetadataArtifactRetirement {
        self.retire_artifacts_after_inventory_removal(segment_identity, &[file])
            .unwrap_or(MetadataArtifactRetirement::NotRegistered)
    }

    /// Retires one canonical, nonempty artifact batch after it has left the
    /// authoritative segment inventory.
    ///
    /// The supplied set is marked retiring in one cache critical section. If
    /// any artifact is absent, none of the present artifacts is newly marked.
    /// Resident pins and corruption ledgers are detached afterward, with all
    /// governed destruction performed outside the cache mutex.
    pub fn retire_artifacts_after_inventory_removal(
        &self,
        segment_identity: &str,
        files: &[SegmentFile],
    ) -> Result<MetadataArtifactRetirement, MetadataArtifactRegistrationError> {
        let segment_identity = MetadataSegmentIdentity::new(Arc::from(segment_identity));
        let artifacts = validate_artifact_batch(&segment_identity, files)?;

        loop {
            let Some(installer) = FlightInstallerGuard::try_acquire(&self.inner.flight_installer)
            else {
                std::thread::yield_now();
                continue;
            };
            let all_registered = {
                let mut state = lock(&self.inner.state);
                if artifacts
                    .iter()
                    .any(|artifact| !state.inventory.contains_key(artifact))
                {
                    false
                } else {
                    for artifact in &artifacts {
                        state
                            .inventory
                            .get_mut(artifact)
                            .expect("validated batch inventory entry")
                            .retirement_requested = true;
                    }
                    true
                }
            };
            drop(installer);
            if !all_registered {
                return Ok(MetadataArtifactRetirement::NotRegistered);
            }
            break;
        }

        // Resident cache pins are not external activity and must not keep a
        // retiring batch alive indefinitely. Detach one victim per lock
        // acquisition so each allocation and charge is destroyed outside the
        // cache mutex. A concurrent publisher observes `retirement_requested`
        // for every member and cannot add a new resident entry.
        loop {
            let victim = {
                let mut state = lock(&self.inner.state);
                let victim = detach_artifacts_resident(&mut state, &artifacts);
                if victim.is_some() {
                    state.stats.evictions = state.stats.evictions.saturating_add(1);
                }
                victim
            };
            let Some(victim) = victim else {
                break;
            };
            drop(victim);
        }

        let mut retired = Vec::with_capacity(artifacts.len());
        let removed = {
            let mut state = lock(&self.inner.state);
            for artifact in &artifacts {
                if let Some(entry) = detach_retired_inventory_if_quiescent(&mut state, artifact) {
                    retired.push(entry);
                }
            }
            artifacts
                .iter()
                .all(|artifact| !state.inventory.contains_key(artifact))
        };
        drop(retired);
        if removed {
            Ok(MetadataArtifactRetirement::Removed)
        } else {
            Ok(MetadataArtifactRetirement::Deferred)
        }
    }

    /// Drops every resident LRU pin. External pins and their charges survive.
    pub fn evict_all_resident(&self) {
        loop {
            let victim = {
                let mut state = lock(&self.inner.state);
                let victim = detach_oldest_resident(&mut state);
                if victim.is_some() {
                    state.stats.evictions = state.stats.evictions.saturating_add(1);
                }
                victim
            };
            let Some(victim) = victim else {
                return;
            };
            drop(victim);
        }
    }

    fn lookup<T>(
        &self,
        key: &FullKey,
    ) -> Result<Option<Result<MetadataCachePin<T>, MetadataCacheError>>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
    {
        let role = self.probe(key);
        match role {
            Some(FlightRole::Immediate(result)) => Ok(Some(erased_result::<T>(result))),
            Some(FlightRole::Wait(flight)) => Ok(Some(erased_result::<T>(flight.wait()))),
            Some(FlightRole::Lead) => unreachable!(),
            None => Ok(None),
        }
    }

    fn load_as_leader<T, F>(
        &self,
        key: FullKey,
        flight: Arc<Flight>,
        declared_max_bytes: u64,
        load: F,
    ) -> Result<MetadataCachePin<T>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Result<LoadedMetadata<T>, MetadataCacheError>,
    {
        let result = self.perform_load(&key, declared_max_bytes, load);
        let erased = result.map(|pin| {
            let erased: ErasedAllocation = pin.allocation;
            erased
        });

        // Publish the result before removing the flight. A caller racing with
        // completion either joins this completed flight or observes the
        // cache/ledger state installed by `perform_load`; it cannot start a
        // duplicate load in between.
        flight.complete(erased.clone());
        let removed_flight = {
            let mut state = lock(&self.inner.state);
            let removed = state
                .flights
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &flight));
            let removed_flight = if removed {
                state.flights.remove(&key)
            } else {
                None
            };
            match &erased {
                Ok(_) => {
                    state.stats.successful_loads = state.stats.successful_loads.saturating_add(1);
                }
                Err(_) => {
                    state.stats.failed_loads = state.stats.failed_loads.saturating_add(1);
                }
            }
            removed_flight
        };
        drop(removed_flight);
        erased_result::<T>(erased)
    }

    fn perform_load<T, F>(
        &self,
        key: &FullKey,
        declared_max_bytes: u64,
        load: F,
    ) -> Result<MetadataCachePin<T>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Result<LoadedMetadata<T>, MetadataCacheError>,
    {
        let mut value_charge = self.inner.governor.reserve_in_flight_for_usage(
            declared_max_bytes,
            MetadataUsageClass::Cache(key.range.class),
        )?;
        let mut loaded = match load() {
            Ok(loaded) => loaded,
            Err(error) => {
                // Bound any shared error payload while the loader's declared
                // reservation still covers the original allocation.
                let error = bounded_shared_error(error);
                drop(value_charge);
                let MetadataCacheError::Structural(corruption) = error else {
                    return Err(error);
                };
                return Err(self.record_structural(&key.range, corruption));
            }
        };
        if loaded.charged_bytes > declared_max_bytes {
            drop(loaded.value);
            drop(value_charge);
            return Err(MetadataCacheError::DeclaredBoundExceeded {
                declared_bytes: declared_max_bytes,
                actual_bytes: loaded.charged_bytes,
            });
        }
        value_charge.reconcile(loaded.charged_bytes)?;

        let resident_bytes =
            (self.inner.governor.config().retained_max_bytes != 0).then_some(RESIDENT_ENTRY_BYTES);
        if resident_bytes.is_some() {
            let required = loaded
                .charged_bytes
                .checked_add(LIVE_REGISTRY_ENTRY_BYTES)
                .and_then(|bytes| bytes.checked_add(RESIDENT_ENTRY_BYTES))
                .unwrap_or(u64::MAX);
            self.evict_until_retained_space(required);
        }
        let resident_admission = admit_cache_allocation(
            &mut value_charge,
            loaded.scratch_charge.as_mut(),
            LIVE_REGISTRY_ENTRY_BYTES,
            resident_bytes,
        );
        let handoff = match resident_admission {
            Ok(handoff) => handoff,
            Err(error) => {
                // With residency enabled, the atomic handoff can fail only
                // after its retained attempt was refused and its transient
                // fallback could not be charged. Preserve that attempted
                // decision even though the overall load will fail.
                let outcome = if resident_bytes.is_some() {
                    ResidentAdmissionOutcome::Refused
                } else {
                    ResidentAdmissionOutcome::Bypassed
                };
                let mut state = lock(&self.inner.state);
                state
                    .stats
                    .record_resident_admission(key.range.class, outcome);
                return Err(error.into());
            }
        };
        let admission_outcome = match (resident_bytes, handoff.resident_charge.is_some()) {
            (Some(_), true) => ResidentAdmissionOutcome::Admitted,
            (Some(_), false) => ResidentAdmissionOutcome::Refused,
            (None, false) => ResidentAdmissionOutcome::Bypassed,
            (None, true) => unreachable!("disabled metadata residency returned a resident charge"),
        };
        // A successful transaction zeroes a present scratch handle. Destroy
        // it only after the governor mutex has been released.
        drop(loaded.scratch_charge.take());
        let live_charge = handoff.live_charge;
        let resident_charge = handoff.resident_charge;

        let value = value_charge.into_pin(loaded.value);
        let allocation_id = {
            let mut state = lock(&self.inner.state);
            state
                .stats
                .record_resident_admission(key.range.class, admission_outcome);
            state.next_allocation_id = state.next_allocation_id.wrapping_add(1);
            state.next_allocation_id
        };
        let allocation = Arc::new(CacheAllocation {
            value: Some(value),
            live_bookkeeping_charge: Some(live_charge),
            owner: Arc::downgrade(&self.inner),
            key: key.clone(),
            allocation_id,
            inventory_tracked: AtomicBool::new(false),
        });
        let erased: ErasedAllocation = allocation.clone();
        let weak = Arc::downgrade(&erased);

        let sticky = {
            let mut state = lock(&self.inner.state);
            if let Some(error) = inventory_error_locked(&mut state, key) {
                Some(error)
            } else {
                let active = state
                    .active_allocations_by_artifact
                    .entry(key.range.artifact_key())
                    .or_default();
                *active = active
                    .checked_add(1)
                    .expect("active allocation count overflow");
                allocation.inventory_tracked.store(true, Ordering::Release);
                state.live.insert(
                    key.clone(),
                    LiveEntry {
                        allocation_id,
                        allocation: weak,
                    },
                );
                if let Some(bookkeeping_charge) = resident_charge {
                    match state.resident.entry(key.clone()) {
                        LinkedEntry::Vacant(entry) => {
                            entry.insert(ResidentEntry {
                                _bookkeeping_charge: bookkeeping_charge,
                                allocation: erased,
                            });
                        }
                        LinkedEntry::Occupied(_) => {
                            unreachable!("resident insertion invariant")
                        }
                    }
                }
                None
            }
        };
        if let Some(error) = sticky {
            drop(allocation);
            return Err(error);
        }
        Ok(MetadataCachePin { allocation })
    }

    fn evict_until_retained_space(&self, required: u64) {
        loop {
            let stats = self.inner.governor.stats();
            if stats
                .retained_bytes
                .checked_add(required)
                .is_some_and(|bytes| bytes <= stats.retained_max_bytes)
            {
                return;
            }
            let victim = {
                let mut state = lock(&self.inner.state);
                let victim = detach_oldest_resident(&mut state);
                if victim.is_some() {
                    state.stats.evictions = state.stats.evictions.saturating_add(1);
                }
                victim
            };
            let Some(victim) = victim else {
                return;
            };
            drop(victim);
        }
    }

    fn record_structural(
        &self,
        key: &MetadataCacheKey,
        corruption: MetadataCorruption,
    ) -> MetadataCacheError {
        self.record_artifact_error_with_key(
            &key.artifact,
            MetadataCacheError::Structural(corruption),
        )
    }
}

enum FlightRole {
    Immediate(Result<ErasedAllocation, MetadataCacheError>),
    Wait(Arc<Flight>),
    Lead,
}

struct FlightInstallerGuard<'a> {
    gate: &'a AtomicBool,
}

impl<'a> FlightInstallerGuard<'a> {
    fn try_acquire(gate: &'a AtomicBool) -> Option<Self> {
        gate.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| Self { gate })
    }
}

impl Drop for FlightInstallerGuard<'_> {
    fn drop(&mut self) {
        self.gate.store(false, Ordering::Release);
    }
}

impl Flight {
    fn complete(&self, result: Result<ErasedAllocation, MetadataCacheError>) {
        *lock(&self.result) = Some(result);
        self.completed.notify_all();
    }

    fn wait(&self) -> Result<ErasedAllocation, MetadataCacheError> {
        let mut result = lock(&self.result);
        while result.is_none() {
            result = self
                .completed
                .wait(result)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        result
            .as_ref()
            .expect("flight completion invariant")
            .clone()
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        // A completed result can own the last allocation pin. Destroy it
        // outside both the result and cache mutexes, then release the flight
        // charge, before decrementing the count that gates ledger retirement.
        let completed_result = lock(&self.result).take();
        drop(completed_result);
        drop(self.bookkeeping_charge.take());

        if !self.inventory_tracked.swap(false, Ordering::AcqRel) {
            return;
        }
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let retired = {
            let mut state = lock(&owner.state);
            decrement_active_artifact_count(
                &mut state.active_flights_by_artifact,
                &self.artifact,
                "flight",
            );
            detach_retired_inventory_if_quiescent(&mut state, &self.artifact)
        };
        drop(retired);
    }
}

fn resident_or_live_locked(state: &mut CacheState, key: &FullKey) -> Option<ErasedAllocation> {
    if let Some(allocation) = state
        .resident
        .to_back(key)
        .map(|entry| Arc::clone(&entry.allocation))
    {
        state.stats.hits = state.stats.hits.saturating_add(1);
        return Some(allocation);
    }
    let upgraded = state
        .live
        .get(key)
        .and_then(|entry| entry.allocation.upgrade());
    if let Some(allocation) = upgraded {
        state.stats.hits = state.stats.hits.saturating_add(1);
        return Some(allocation);
    }
    state.live.remove(key);
    None
}

fn inventory_error_locked(state: &mut CacheState, key: &FullKey) -> Option<MetadataCacheError> {
    artifact_error_locked(state, &key.range.artifact)
}

fn artifact_error_locked(
    state: &mut CacheState,
    artifact: &ArtifactKey,
) -> Option<MetadataCacheError> {
    let Some(entry) = state.inventory.get(artifact) else {
        return Some(MetadataCacheError::UnregisteredArtifact {
            segment_identity: artifact.segment_identity.to_arc(),
            file: artifact.file,
        });
    };
    if let Some(corruption) = entry.corruption.clone() {
        state.stats.corruption_hits = state.stats.corruption_hits.saturating_add(1);
        return Some(MetadataCacheError::Structural(corruption));
    }
    entry
        .retirement_requested
        .then(|| MetadataCacheError::RetiringArtifact {
            segment_identity: artifact.segment_identity.to_arc(),
            file: artifact.file,
        })
}

fn bounded_corruption(corruption: MetadataCorruption) -> MetadataCorruption {
    MetadataCorruption {
        kind: corruption.kind,
        message: bounded_message(corruption.message, MAX_CORRUPTION_MESSAGE_BYTES),
    }
}

fn bounded_shared_error(error: MetadataCacheError) -> MetadataCacheError {
    match error {
        MetadataCacheError::Structural(corruption) => {
            MetadataCacheError::Structural(bounded_corruption(corruption))
        }
        MetadataCacheError::Transient { kind, message } => MetadataCacheError::Transient {
            kind,
            message: bounded_message(message, MAX_TRANSIENT_MESSAGE_BYTES),
        },
        error => error,
    }
}

fn bounded_message(message: Arc<str>, max_bytes: usize) -> Arc<str> {
    if message.len() <= max_bytes {
        return message;
    }
    let mut end = max_bytes;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    Arc::from(&message[..end])
}

fn erased_result<T>(
    result: Result<ErasedAllocation, MetadataCacheError>,
) -> Result<MetadataCachePin<T>, MetadataCacheError>
where
    T: Send + Sync + 'static,
{
    let erased = result?;
    let allocation = erased
        .downcast::<CacheAllocation<T>>()
        .map_err(|_| MetadataCacheError::TypeMismatch)?;
    Ok(MetadataCachePin { allocation })
}

fn detach_oldest_resident(state: &mut CacheState) -> Option<ResidentEntry> {
    state.resident.pop_front().map(|(_, entry)| entry)
}

fn detach_artifact_resident(
    state: &mut CacheState,
    artifact: &ArtifactKey,
) -> Option<ResidentEntry> {
    let key = state
        .resident
        .keys()
        .find(|key| key.range.artifact_key() == *artifact)
        .cloned()?;
    state.resident.remove(&key)
}

fn detach_artifacts_resident(
    state: &mut CacheState,
    artifacts: &[ArtifactKey],
) -> Option<ResidentEntry> {
    let artifact = artifacts.iter().find(|artifact| {
        state
            .resident
            .keys()
            .any(|key| key.range.artifact_key() == **artifact)
    })?;
    detach_artifact_resident(state, artifact)
}

fn detach_retired_inventory_if_quiescent(
    state: &mut CacheState,
    artifact: &ArtifactKey,
) -> Option<InventoryEntry> {
    let retirement_requested = state
        .inventory
        .get(artifact)
        .is_some_and(|entry| entry.retirement_requested);
    if !retirement_requested || artifact_has_cache_activity(state, artifact) {
        return None;
    }
    state.inventory.remove(artifact)
}

fn artifact_has_cache_activity(state: &CacheState, artifact: &ArtifactKey) -> bool {
    state
        .active_allocations_by_artifact
        .get(artifact)
        .is_some_and(|count| *count != 0)
        || state
            .active_flights_by_artifact
            .get(artifact)
            .is_some_and(|count| *count != 0)
        || state
            .live
            .keys()
            .chain(state.resident.keys())
            .chain(state.flights.keys())
            .any(|key| key.range.artifact_key() == *artifact)
}

fn decrement_active_artifact_count(
    counts: &mut HashMap<ArtifactKey, u64>,
    artifact: &ArtifactKey,
    activity: &str,
) {
    let remaining = {
        let count = counts
            .get_mut(artifact)
            .unwrap_or_else(|| panic!("missing active {activity} count"));
        *count = count
            .checked_sub(1)
            .unwrap_or_else(|| panic!("active {activity} count underflow"));
        *count
    };
    if remaining == 0 {
        counts.remove(artifact);
    }
}

fn checked_activity_total(counts: impl Iterator<Item = u64>, overflow_message: &str) -> u64 {
    counts.fold(0, |total, count| {
        total
            .checked_add(count)
            .unwrap_or_else(|| panic!("{overflow_message}"))
    })
}

fn prehash(value: &impl Hash) -> u64 {
    // Cache keys are internal, validated locators. Prehashing collapses their
    // stable identity and fields to one word before the maps apply their own
    // randomized hash. Full equality remains authoritative on collisions.
    let mut hasher = CacheKeyPrehasher(XxHash64::default());
    value.hash(&mut hasher);
    hasher.finish()
}

struct CacheKeyPrehasher(XxHash64);

impl Hasher for CacheKeyPrehasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn is_cacheable_metadata_file(file: SegmentFile) -> bool {
    matches!(
        file,
        SegmentFile::Symbols | SegmentFile::Series | SegmentFile::ChunkIndex | SegmentFile::Indexes
    )
}

fn is_footer_tracked_artifact(file: SegmentFile) -> bool {
    matches!(
        file,
        SegmentFile::MetaJson
            | SegmentFile::Symbols
            | SegmentFile::Series
            | SegmentFile::Chunks
            | SegmentFile::OooChunks
            | SegmentFile::ChunkIndex
            | SegmentFile::Indexes
    )
}

fn validate_artifact_batch(
    segment_identity: &MetadataSegmentIdentity,
    files: &[SegmentFile],
) -> Result<Vec<ArtifactKey>, MetadataArtifactRegistrationError> {
    if segment_identity.as_str().is_empty() {
        return Err(MetadataArtifactRegistrationError::EmptySegmentIdentity);
    }
    if files.is_empty() {
        return Err(MetadataArtifactRegistrationError::EmptyArtifactBatch);
    }
    for (index, &file) in files.iter().enumerate() {
        if !is_footer_tracked_artifact(file) {
            return Err(MetadataArtifactRegistrationError::UnsupportedFile { file });
        }
        if files[..index].contains(&file) {
            return Err(MetadataArtifactRegistrationError::DuplicateFile { file });
        }
    }
    for pair in files.windows(2) {
        let previous = pair[0];
        let file = pair[1];
        if segment_file_rank(previous) > segment_file_rank(file) {
            return Err(MetadataArtifactRegistrationError::NonCanonicalOrder { previous, file });
        }
    }
    Ok(files
        .iter()
        .map(|&file| ArtifactKey::new(segment_identity.clone(), file))
        .collect())
}

fn artifact_batch_inventory_state(
    state: &CacheState,
    files: &[SegmentFile],
    artifacts: &[ArtifactKey],
) -> ArtifactBatchInventoryState {
    debug_assert_eq!(files.len(), artifacts.len());
    let mut registered = 0;
    let mut retiring = 0;
    let mut first_retiring = None;
    for (&file, artifact) in files.iter().zip(artifacts) {
        let Some(entry) = state.inventory.get(artifact) else {
            continue;
        };
        registered += 1;
        if entry.retirement_requested {
            retiring += 1;
            first_retiring.get_or_insert(file);
        }
    }
    if registered == 0 {
        ArtifactBatchInventoryState::Vacant
    } else if registered != artifacts.len() && retiring == registered {
        ArtifactBatchInventoryState::Retiring {
            file: first_retiring.expect("partial retiring inventory has a retiring member"),
        }
    } else if registered != artifacts.len() || (retiring != 0 && retiring != artifacts.len()) {
        ArtifactBatchInventoryState::PartialOrMixed { registered }
    } else if let Some(file) = first_retiring {
        ArtifactBatchInventoryState::Retiring { file }
    } else {
        ArtifactBatchInventoryState::Active
    }
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

pub(super) fn corruption_ledger_charge_bytes(segment_identity: &str) -> Option<u64> {
    MAX_CORRUPTION_LEDGER_CHARGE_BYTES.checked_add(u64::try_from(segment_identity.len()).ok()?)
}

#[cfg(test)]
mod tests {
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
            MetadataCacheError::structural(
                StructuralMetadataErrorKind::InvalidData,
                "bad series page",
            ),
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
}
