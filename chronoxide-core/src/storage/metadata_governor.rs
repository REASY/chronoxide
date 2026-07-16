use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};

use thiserror::Error;

pub const DEFAULT_METADATA_RETAINED_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_METADATA_IN_FLIGHT_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_METADATA_MAX_OPEN_FILES: u32 = 128;
pub const DEFAULT_METADATA_MAX_CACHED_OPEN_FILES: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataGovernorConfig {
    pub retained_max_bytes: u64,
    pub in_flight_max_bytes: u64,
    pub max_open_files: u32,
    pub max_cached_open_files: u32,
}

impl Default for MetadataGovernorConfig {
    fn default() -> Self {
        Self {
            retained_max_bytes: DEFAULT_METADATA_RETAINED_MAX_BYTES,
            in_flight_max_bytes: DEFAULT_METADATA_IN_FLIGHT_MAX_BYTES,
            max_open_files: DEFAULT_METADATA_MAX_OPEN_FILES,
            max_cached_open_files: DEFAULT_METADATA_MAX_CACHED_OPEN_FILES,
        }
    }
}

impl MetadataGovernorConfig {
    pub fn validate(self) -> Result<Self, MetadataGovernorConfigError> {
        if self.in_flight_max_bytes == 0 {
            return Err(MetadataGovernorConfigError::ZeroInFlightBudget);
        }
        if self.max_open_files == 0 {
            return Err(MetadataGovernorConfigError::ZeroOpenFileLimit);
        }
        if self.max_cached_open_files > self.max_open_files {
            return Err(
                MetadataGovernorConfigError::CachedOpenFileLimitExceedsHardLimit {
                    cached: self.max_cached_open_files,
                    hard: self.max_open_files,
                },
            );
        }
        Ok(self)
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum MetadataGovernorConfigError {
    #[error("metadata in-flight budget must be non-zero")]
    ZeroInFlightBudget,
    #[error("metadata open-file limit must be non-zero")]
    ZeroOpenFileLimit,
    #[error("metadata cached-open-file limit exceeds hard limit: cached={cached} hard={hard}")]
    CachedOpenFileLimitExceedsHardLimit { cached: u32, hard: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataChargeClass {
    InFlight,
    Retained,
}

impl fmt::Display for MetadataChargeClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InFlight => formatter.write_str("in-flight"),
            Self::Retained => formatter.write_str("retained"),
        }
    }
}

/// Stable semantic classes for cache-owned immutable metadata ranges and
/// classified metadata reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataCacheClass {
    SymbolRoot,
    SymbolPage,
    IndexRoot,
    IndexDirectory,
    IndexPage,
    MetricRange,
    SeriesRoot,
    SeriesHotPage,
    SeriesColdPage,
    OverflowRoot,
    OverflowBlob,
    Postings,
    /// Streaming reads issued by an explicit complete-file validation pass.
    FullValidation,
}

pub const METADATA_CACHE_CLASS_COUNT: usize = 13;

/// Stable cache-class order used by snapshots and machine-readable reports.
pub const METADATA_CACHE_CLASS_ORDER: [MetadataCacheClass; METADATA_CACHE_CLASS_COUNT] = [
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
];

impl MetadataCacheClass {
    pub const fn stable_index(self) -> usize {
        match self {
            Self::SymbolRoot => 0,
            Self::SymbolPage => 1,
            Self::IndexRoot => 2,
            Self::IndexDirectory => 3,
            Self::IndexPage => 4,
            Self::MetricRange => 5,
            Self::SeriesRoot => 6,
            Self::SeriesHotPage => 7,
            Self::SeriesColdPage => 8,
            Self::OverflowRoot => 9,
            Self::OverflowBlob => 10,
            Self::Postings => 11,
            Self::FullValidation => 12,
        }
    }
}

/// Stable semantic owner of every governed metadata charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataUsageClass {
    /// Existing callers that have not selected a more precise usage.
    Unclassified,
    /// Explicit decoder/read working memory.
    Scratch,
    /// Mandatory non-evictable sticky-corruption state.
    CorruptionLedger,
    /// Cache value or bookkeeping attributable to one immutable range class.
    Cache(MetadataCacheClass),
}

pub const METADATA_USAGE_CLASS_COUNT: usize = 16;

pub const METADATA_USAGE_CLASS_ORDER: [MetadataUsageClass; METADATA_USAGE_CLASS_COUNT] = [
    MetadataUsageClass::Unclassified,
    MetadataUsageClass::Scratch,
    MetadataUsageClass::CorruptionLedger,
    MetadataUsageClass::Cache(MetadataCacheClass::SymbolRoot),
    MetadataUsageClass::Cache(MetadataCacheClass::SymbolPage),
    MetadataUsageClass::Cache(MetadataCacheClass::IndexRoot),
    MetadataUsageClass::Cache(MetadataCacheClass::IndexDirectory),
    MetadataUsageClass::Cache(MetadataCacheClass::IndexPage),
    MetadataUsageClass::Cache(MetadataCacheClass::MetricRange),
    MetadataUsageClass::Cache(MetadataCacheClass::SeriesRoot),
    MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage),
    MetadataUsageClass::Cache(MetadataCacheClass::SeriesColdPage),
    MetadataUsageClass::Cache(MetadataCacheClass::OverflowRoot),
    MetadataUsageClass::Cache(MetadataCacheClass::OverflowBlob),
    MetadataUsageClass::Cache(MetadataCacheClass::Postings),
    MetadataUsageClass::Cache(MetadataCacheClass::FullValidation),
];

impl MetadataUsageClass {
    const fn stable_index(self) -> usize {
        match self {
            Self::Unclassified => 0,
            Self::Scratch => 1,
            Self::CorruptionLedger => 2,
            Self::Cache(MetadataCacheClass::SymbolRoot) => 3,
            Self::Cache(MetadataCacheClass::SymbolPage) => 4,
            Self::Cache(MetadataCacheClass::IndexRoot) => 5,
            Self::Cache(MetadataCacheClass::IndexDirectory) => 6,
            Self::Cache(MetadataCacheClass::IndexPage) => 7,
            Self::Cache(MetadataCacheClass::MetricRange) => 8,
            Self::Cache(MetadataCacheClass::SeriesRoot) => 9,
            Self::Cache(MetadataCacheClass::SeriesHotPage) => 10,
            Self::Cache(MetadataCacheClass::SeriesColdPage) => 11,
            Self::Cache(MetadataCacheClass::OverflowRoot) => 12,
            Self::Cache(MetadataCacheClass::OverflowBlob) => 13,
            Self::Cache(MetadataCacheClass::Postings) => 14,
            Self::Cache(MetadataCacheClass::FullValidation) => 15,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataUsageStats {
    pub usage: MetadataUsageClass,
    pub in_flight_bytes: u64,
    pub retained_bytes: u64,
    pub peak_in_flight_bytes: u64,
    pub peak_retained_bytes: u64,
}

impl MetadataUsageStats {
    const fn zero(usage: MetadataUsageClass) -> Self {
        Self {
            usage,
            in_flight_bytes: 0,
            retained_bytes: 0,
            peak_in_flight_bytes: 0,
            peak_retained_bytes: 0,
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error(
    "metadata {class} budget exceeded: requested={requested_bytes} current={current_bytes} limit={limit_bytes}"
)]
pub struct MetadataBudgetError {
    pub class: MetadataChargeClass,
    pub requested_bytes: u64,
    pub current_bytes: u64,
    pub limit_bytes: u64,
}

/// A point-in-time logical-memory accounting snapshot.
///
/// Charged bytes are the caller-measured owned lengths, capacities, and fixed
/// bookkeeping covered by the governor. They intentionally exclude allocator
/// slack and are a bound on governed metadata, not process RSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataGovernorStats {
    pub retained_max_bytes: u64,
    pub in_flight_max_bytes: u64,
    pub retained_bytes: u64,
    pub in_flight_bytes: u64,
    pub peak_retained_bytes: u64,
    pub peak_in_flight_bytes: u64,
    pub retained_refusals: u64,
    pub in_flight_refusals: u64,
    pub usage: [MetadataUsageStats; METADATA_USAGE_CLASS_COUNT],
}

impl Default for MetadataGovernorStats {
    fn default() -> Self {
        Self {
            retained_max_bytes: 0,
            in_flight_max_bytes: 0,
            retained_bytes: 0,
            in_flight_bytes: 0,
            peak_retained_bytes: 0,
            peak_in_flight_bytes: 0,
            retained_refusals: 0,
            in_flight_refusals: 0,
            usage: METADATA_USAGE_CLASS_ORDER.map(MetadataUsageStats::zero),
        }
    }
}

impl MetadataGovernorStats {
    pub fn usage(&self, usage: MetadataUsageClass) -> MetadataUsageStats {
        self.usage[usage.stable_index()]
    }
}

#[derive(Debug, Default)]
struct MetadataGovernorState {
    retained_bytes: u64,
    in_flight_bytes: u64,
    peak_retained_bytes: u64,
    peak_in_flight_bytes: u64,
    retained_refusals: u64,
    in_flight_refusals: u64,
    usage: [MetadataUsageCounters; METADATA_USAGE_CLASS_COUNT],
}

#[derive(Debug, Clone, Copy, Default)]
struct MetadataUsageCounters {
    in_flight_bytes: u64,
    retained_bytes: u64,
    peak_in_flight_bytes: u64,
    peak_retained_bytes: u64,
}

#[derive(Debug)]
pub struct MetadataGovernor {
    config: MetadataGovernorConfig,
    state: Mutex<MetadataGovernorState>,
}

impl MetadataGovernor {
    pub fn new(config: MetadataGovernorConfig) -> Result<Arc<Self>, MetadataGovernorConfigError> {
        Ok(Arc::new(Self {
            config: config.validate()?,
            state: Mutex::new(MetadataGovernorState::default()),
        }))
    }

    pub fn config(&self) -> MetadataGovernorConfig {
        self.config
    }

    /// Reserves a checked logical upper bound before allocating or growing
    /// metadata working memory.
    pub fn reserve_in_flight(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<MetadataCharge, MetadataBudgetError> {
        self.reserve_in_flight_for_usage(bytes, MetadataUsageClass::Unclassified)
    }

    pub fn reserve_in_flight_for_usage(
        self: &Arc<Self>,
        bytes: u64,
        usage: MetadataUsageClass,
    ) -> Result<MetadataCharge, MetadataBudgetError> {
        self.reserve(MetadataChargeClass::InFlight, usage, bytes)
    }

    fn reserve(
        self: &Arc<Self>,
        class: MetadataChargeClass,
        usage: MetadataUsageClass,
        bytes: u64,
    ) -> Result<MetadataCharge, MetadataBudgetError> {
        let mut state = self.lock_state();
        reserve_locked(self.config, &mut state, class, usage, bytes)?;
        Ok(MetadataCharge {
            governor: Arc::clone(self),
            class,
            usage,
            bytes,
        })
    }

    pub fn stats(&self) -> MetadataGovernorStats {
        let state = self.lock_state();
        let usage = METADATA_USAGE_CLASS_ORDER.map(|usage| {
            let counters = state.usage[usage.stable_index()];
            MetadataUsageStats {
                usage,
                in_flight_bytes: counters.in_flight_bytes,
                retained_bytes: counters.retained_bytes,
                peak_in_flight_bytes: counters.peak_in_flight_bytes,
                peak_retained_bytes: counters.peak_retained_bytes,
            }
        });
        MetadataGovernorStats {
            retained_max_bytes: self.config.retained_max_bytes,
            in_flight_max_bytes: self.config.in_flight_max_bytes,
            retained_bytes: state.retained_bytes,
            in_flight_bytes: state.in_flight_bytes,
            peak_retained_bytes: state.peak_retained_bytes,
            peak_in_flight_bytes: state.peak_in_flight_bytes,
            retained_refusals: state.retained_refusals,
            in_flight_refusals: state.in_flight_refusals,
            usage,
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, MetadataGovernorState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug)]
pub struct MetadataCharge {
    governor: Arc<MetadataGovernor>,
    class: MetadataChargeClass,
    usage: MetadataUsageClass,
    bytes: u64,
}

impl MetadataCharge {
    pub fn class(&self) -> MetadataChargeClass {
        self.class
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn usage(&self) -> MetadataUsageClass {
        self.usage
    }

    /// Reconciles a declared upper-bound reservation to the measured logical
    /// allocation size. An upward refusal leaves both the charge and global
    /// accounting unchanged.
    pub fn reconcile(&mut self, actual_bytes: u64) -> Result<(), MetadataBudgetError> {
        if actual_bytes == self.bytes {
            return Ok(());
        }

        let mut state = self.governor.lock_state();
        if actual_bytes > self.bytes {
            reserve_locked(
                self.governor.config,
                &mut state,
                self.class,
                self.usage,
                actual_bytes - self.bytes,
            )?;
        } else {
            release_locked(
                &mut state,
                self.class,
                self.usage,
                self.bytes - actual_bytes,
            );
        }
        self.bytes = actual_bytes;
        Ok(())
    }

    /// Attempts to move a final validated allocation from transient to
    /// retained accounting. A refusal leaves the charge in-flight so the
    /// caller can still use the value transiently.
    pub fn try_promote_to_retained(&mut self) -> bool {
        if self.class == MetadataChargeClass::Retained {
            return true;
        }

        let mut state = self.governor.lock_state();
        let Some(next_retained) = state.retained_bytes.checked_add(self.bytes) else {
            state.retained_refusals = state.retained_refusals.saturating_add(1);
            return false;
        };
        if next_retained > self.governor.config.retained_max_bytes {
            state.retained_refusals = state.retained_refusals.saturating_add(1);
            return false;
        }

        let usage_index = self.usage.stable_index();
        let next_usage_retained = state.usage[usage_index]
            .retained_bytes
            .checked_add(self.bytes)
            .expect("metadata usage retained charge overflow");

        state.in_flight_bytes = state
            .in_flight_bytes
            .checked_sub(self.bytes)
            .expect("metadata in-flight charge invariant violated");
        state.retained_bytes = next_retained;
        state.peak_retained_bytes = state.peak_retained_bytes.max(next_retained);
        let usage = &mut state.usage[usage_index];
        usage.in_flight_bytes = usage
            .in_flight_bytes
            .checked_sub(self.bytes)
            .expect("metadata usage in-flight charge invariant violated");
        usage.retained_bytes = next_usage_retained;
        usage.peak_retained_bytes = usage.peak_retained_bytes.max(next_usage_retained);
        self.class = MetadataChargeClass::Retained;
        true
    }

    /// Attaches this charge to the allocation containing `value`.
    ///
    /// The caller must reconcile the charge to the allocation's measured
    /// logical byte size before publishing the returned pin. The governor
    /// cannot infer deep owned memory from an arbitrary `T`.
    pub fn into_pin<T>(self, value: T) -> MetadataPin<T> {
        MetadataPin {
            allocation: Arc::new(MetadataAllocation {
                // Keep the value before the charge so Rust drops the value's
                // owned memory while it is still accounted. Struct fields
                // are dropped in declaration order.
                value,
                charge: self,
            }),
        }
    }
}

/// Charges created by an atomic post-validation scratch handoff.
///
/// A resident charge is present exactly when the complete value admission was
/// transferred to retained accounting. Otherwise the value and live-registry
/// charges remain transient in-flight allocations.
#[derive(Debug)]
pub(crate) struct MetadataScratchHandoff {
    pub(crate) live_charge: MetadataCharge,
    pub(crate) resident_charge: Option<MetadataCharge>,
}

/// Atomically installs post-validation cache admission charges.
///
/// The caller already owns an in-flight final-allocation charge and may carry
/// an in-flight scratch charge from the same governor. Under one governor
/// lock, this transaction either:
///
/// - releases scratch and transfers the final allocation plus new live and
///   resident bookkeeping charges to retained accounting; or
/// - releases scratch and creates only a new transient live-registry charge,
///   leaving the final allocation in-flight.
///
/// Scratch-free generic loaders use the same transaction with no scratch
/// handle. A refusal leaves every input charge and global accounting
/// unchanged. A present scratch handle is zeroed only after its accounted
/// bytes have been consumed by a successful transaction, so dropping it later
/// cannot double-release.
pub(crate) fn admit_cache_allocation(
    final_charge: &mut MetadataCharge,
    mut scratch_charge: Option<&mut MetadataCharge>,
    live_bytes: u64,
    resident_bytes: Option<u64>,
) -> Result<MetadataScratchHandoff, MetadataBudgetError> {
    assert_eq!(
        final_charge.class,
        MetadataChargeClass::InFlight,
        "metadata cache admission requires an in-flight final allocation"
    );
    assert!(
        matches!(final_charge.usage, MetadataUsageClass::Cache(_)),
        "metadata cache admission requires a cache allocation charge"
    );
    if let Some(scratch_charge) = scratch_charge.as_ref() {
        assert!(
            Arc::ptr_eq(&final_charge.governor, &scratch_charge.governor),
            "metadata scratch handoff requires one governor"
        );
        assert_eq!(
            scratch_charge.class,
            MetadataChargeClass::InFlight,
            "metadata scratch handoff requires in-flight scratch"
        );
        assert_eq!(
            scratch_charge.usage,
            MetadataUsageClass::Scratch,
            "metadata scratch handoff requires a scratch usage charge"
        );
    }

    let governor = Arc::clone(&final_charge.governor);
    let final_usage = final_charge.usage;
    let final_bytes = final_charge.bytes;
    let scratch_bytes = scratch_charge
        .as_ref()
        .map_or(0, |scratch_charge| scratch_charge.bytes);
    let mut state = governor.lock_state();

    if let Some(resident_bytes) = resident_bytes {
        let transfer_bytes = final_bytes
            .checked_add(live_bytes)
            .and_then(|bytes| bytes.checked_add(resident_bytes));
        let next_retained =
            transfer_bytes.and_then(|bytes| state.retained_bytes.checked_add(bytes));
        let next_usage_retained = transfer_bytes.and_then(|bytes| {
            state.usage[final_usage.stable_index()]
                .retained_bytes
                .checked_add(bytes)
        });

        if let (Some(_), Some(next_retained), Some(next_usage_retained)) =
            (transfer_bytes, next_retained, next_usage_retained)
            && next_retained <= governor.config.retained_max_bytes
        {
            let released_in_flight = final_bytes
                .checked_add(scratch_bytes)
                .expect("metadata scratch handoff release overflow");
            state.in_flight_bytes = state
                .in_flight_bytes
                .checked_sub(released_in_flight)
                .expect("metadata scratch handoff in-flight invariant violated");
            state.retained_bytes = next_retained;
            state.peak_retained_bytes = state.peak_retained_bytes.max(next_retained);

            let scratch_usage = &mut state.usage[MetadataUsageClass::Scratch.stable_index()];
            scratch_usage.in_flight_bytes = scratch_usage
                .in_flight_bytes
                .checked_sub(scratch_bytes)
                .expect("metadata scratch usage invariant violated");
            let final_usage_counters = &mut state.usage[final_usage.stable_index()];
            final_usage_counters.in_flight_bytes = final_usage_counters
                .in_flight_bytes
                .checked_sub(final_bytes)
                .expect("metadata final usage invariant violated");
            final_usage_counters.retained_bytes = next_usage_retained;
            final_usage_counters.peak_retained_bytes = final_usage_counters
                .peak_retained_bytes
                .max(next_usage_retained);

            if let Some(scratch_charge) = scratch_charge.as_mut() {
                scratch_charge.bytes = 0;
            }
            final_charge.class = MetadataChargeClass::Retained;
            let live_charge = MetadataCharge {
                governor: Arc::clone(&governor),
                class: MetadataChargeClass::Retained,
                usage: final_usage,
                bytes: live_bytes,
            };
            let resident_charge = MetadataCharge {
                governor: Arc::clone(&governor),
                class: MetadataChargeClass::Retained,
                usage: final_usage,
                bytes: resident_bytes,
            };
            drop(state);
            return Ok(MetadataScratchHandoff {
                live_charge,
                resident_charge: Some(resident_charge),
            });
        }
        state.retained_refusals = state.retained_refusals.saturating_add(1);
    }

    let current_after_scratch = state
        .in_flight_bytes
        .checked_sub(scratch_bytes)
        .expect("metadata scratch handoff in-flight invariant violated");
    let Some(next_in_flight) = current_after_scratch.checked_add(live_bytes) else {
        observe_refusal(&mut state, MetadataChargeClass::InFlight);
        return Err(MetadataBudgetError {
            class: MetadataChargeClass::InFlight,
            requested_bytes: live_bytes,
            current_bytes: current_after_scratch,
            limit_bytes: governor.config.in_flight_max_bytes,
        });
    };
    if next_in_flight > governor.config.in_flight_max_bytes {
        observe_refusal(&mut state, MetadataChargeClass::InFlight);
        return Err(MetadataBudgetError {
            class: MetadataChargeClass::InFlight,
            requested_bytes: live_bytes,
            current_bytes: current_after_scratch,
            limit_bytes: governor.config.in_flight_max_bytes,
        });
    }
    let next_usage_in_flight = state.usage[final_usage.stable_index()]
        .in_flight_bytes
        .checked_add(live_bytes)
        .expect("metadata final usage charge overflow");

    state.in_flight_bytes = next_in_flight;
    state.peak_in_flight_bytes = state.peak_in_flight_bytes.max(next_in_flight);
    let scratch_usage = &mut state.usage[MetadataUsageClass::Scratch.stable_index()];
    scratch_usage.in_flight_bytes = scratch_usage
        .in_flight_bytes
        .checked_sub(scratch_bytes)
        .expect("metadata scratch usage invariant violated");
    let final_usage_counters = &mut state.usage[final_usage.stable_index()];
    final_usage_counters.in_flight_bytes = next_usage_in_flight;
    final_usage_counters.peak_in_flight_bytes = final_usage_counters
        .peak_in_flight_bytes
        .max(next_usage_in_flight);

    if let Some(scratch_charge) = scratch_charge.as_mut() {
        scratch_charge.bytes = 0;
    }
    let live_charge = MetadataCharge {
        governor: Arc::clone(&governor),
        class: MetadataChargeClass::InFlight,
        usage: final_usage,
        bytes: live_bytes,
    };
    drop(state);
    Ok(MetadataScratchHandoff {
        live_charge,
        resident_charge: None,
    })
}

impl Drop for MetadataCharge {
    fn drop(&mut self) {
        let mut state = self.governor.lock_state();
        release_locked(&mut state, self.class, self.usage, self.bytes);
    }
}

/// A typed shared handle whose final allocation owns its metadata charge.
///
/// Cloning a pin only clones the allocation identity; it does not create a
/// second charge. The charge remains live until the final pin drops, including
/// after a future cache entry releases its own resident pin.
pub struct MetadataPin<T> {
    allocation: Arc<MetadataAllocation<T>>,
}

struct MetadataAllocation<T> {
    value: T,
    charge: MetadataCharge,
}

impl<T> MetadataPin<T> {
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.allocation, &other.allocation)
    }

    pub fn charge_class(&self) -> MetadataChargeClass {
        self.allocation.charge.class()
    }

    pub fn charged_bytes(&self) -> u64 {
        self.allocation.charge.bytes()
    }
}

impl<T> Clone for MetadataPin<T> {
    fn clone(&self) -> Self {
        Self {
            allocation: Arc::clone(&self.allocation),
        }
    }
}

impl<T> Deref for MetadataPin<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.allocation.value
    }
}

impl<T: fmt::Debug> fmt::Debug for MetadataPin<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataPin")
            .field("value", &self.allocation.value)
            .field("charge_class", &self.charge_class())
            .field("charged_bytes", &self.charged_bytes())
            .finish_non_exhaustive()
    }
}

fn reserve_locked(
    config: MetadataGovernorConfig,
    state: &mut MetadataGovernorState,
    class: MetadataChargeClass,
    usage: MetadataUsageClass,
    bytes: u64,
) -> Result<(), MetadataBudgetError> {
    let (current, limit) = match class {
        MetadataChargeClass::InFlight => (state.in_flight_bytes, config.in_flight_max_bytes),
        MetadataChargeClass::Retained => (state.retained_bytes, config.retained_max_bytes),
    };
    let Some(next) = current.checked_add(bytes) else {
        observe_refusal(state, class);
        return Err(MetadataBudgetError {
            class,
            requested_bytes: bytes,
            current_bytes: current,
            limit_bytes: limit,
        });
    };
    if next > limit {
        observe_refusal(state, class);
        return Err(MetadataBudgetError {
            class,
            requested_bytes: bytes,
            current_bytes: current,
            limit_bytes: limit,
        });
    }
    let usage_counters = &state.usage[usage.stable_index()];
    let usage_next = match class {
        MetadataChargeClass::InFlight => usage_counters.in_flight_bytes.checked_add(bytes),
        MetadataChargeClass::Retained => usage_counters.retained_bytes.checked_add(bytes),
    }
    .expect("metadata usage charge overflow");

    match class {
        MetadataChargeClass::InFlight => {
            state.in_flight_bytes = next;
            state.peak_in_flight_bytes = state.peak_in_flight_bytes.max(next);
            let usage_counters = &mut state.usage[usage.stable_index()];
            usage_counters.in_flight_bytes = usage_next;
            usage_counters.peak_in_flight_bytes = usage_counters
                .peak_in_flight_bytes
                .max(usage_counters.in_flight_bytes);
        }
        MetadataChargeClass::Retained => {
            state.retained_bytes = next;
            state.peak_retained_bytes = state.peak_retained_bytes.max(next);
            let usage_counters = &mut state.usage[usage.stable_index()];
            usage_counters.retained_bytes = usage_next;
            usage_counters.peak_retained_bytes = usage_counters
                .peak_retained_bytes
                .max(usage_counters.retained_bytes);
        }
    }
    Ok(())
}

fn release_locked(
    state: &mut MetadataGovernorState,
    class: MetadataChargeClass,
    usage: MetadataUsageClass,
    bytes: u64,
) {
    let charged = match class {
        MetadataChargeClass::InFlight => &mut state.in_flight_bytes,
        MetadataChargeClass::Retained => &mut state.retained_bytes,
    };
    *charged = charged
        .checked_sub(bytes)
        .expect("metadata charge release invariant violated");
    let usage_counters = &mut state.usage[usage.stable_index()];
    let usage_charged = match class {
        MetadataChargeClass::InFlight => &mut usage_counters.in_flight_bytes,
        MetadataChargeClass::Retained => &mut usage_counters.retained_bytes,
    };
    *usage_charged = usage_charged
        .checked_sub(bytes)
        .expect("metadata usage charge release invariant violated");
}

fn observe_refusal(state: &mut MetadataGovernorState, class: MetadataChargeClass) {
    match class {
        MetadataChargeClass::InFlight => {
            state.in_flight_refusals = state.in_flight_refusals.saturating_add(1);
        }
        MetadataChargeClass::Retained => {
            state.retained_refusals = state.retained_refusals.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
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
                MetadataGovernorConfigError::CachedOpenFileLimitExceedsHardLimit {
                    cached: 5,
                    hard: 4,
                }
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
            admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 2, Some(3))
                .unwrap();

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
            admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 3, Some(2))
                .unwrap();

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
            admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 3, Some(2))
                .unwrap();

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

        let error = admit_cache_allocation(&mut final_charge, Some(&mut scratch_charge), 3, None)
            .unwrap_err();
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
                let handoff = admit_cache_allocation(
                    &mut final_charge,
                    Some(&mut scratch_charge),
                    2,
                    Some(2),
                )
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
}
