// These foundations are intentionally wired into range execution by the next
// experimental tasks; keep standalone Task 5 builds warning-free meanwhile.
#![allow(dead_code)]

use std::alloc::Layout;
#[cfg(test)]
use std::cell::Cell;
use std::io;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use allocator_api2::alloc::{AllocError, Allocator, Global};
use allocator_api2::boxed::Box as AllocBox;
use thiserror::Error;

use crate::storage::chunk::{
    ChunkKind, ChunkScalarProjection, ChunkScalarRecordHeader, ChunkScalarSample,
};

pub(super) const MIB: u64 = 1024 * 1024;
pub const DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES: u64 = 16 * MIB;
pub const MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES: u64 = 32 * MIB;
pub const DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES: u64 = 128 * MIB;

const MAX_RANGE_SCALAR_CACHE_ENTRIES: usize = 16_384;

#[cfg(test)]
thread_local! {
    static INJECTED_EXACT_ALLOCATION_FAILURE: Cell<(usize, usize)> =
        const { Cell::new((usize::MAX, 0)) };
}

#[cfg(test)]
pub(super) struct InjectedExactAllocationFailureGuard {
    previous: (usize, usize),
}

#[cfg(test)]
pub(super) fn inject_range_scalar_cache_allocation_failure(
    fail_on_call: usize,
) -> InjectedExactAllocationFailureGuard {
    assert!(fail_on_call > 0);
    let previous = INJECTED_EXACT_ALLOCATION_FAILURE.with(|state| {
        let previous = state.get();
        state.set((fail_on_call, 0));
        previous
    });
    InjectedExactAllocationFailureGuard { previous }
}

#[cfg(test)]
impl Drop for InjectedExactAllocationFailureGuard {
    fn drop(&mut self) {
        INJECTED_EXACT_ALLOCATION_FAILURE.with(|state| state.set(self.previous));
    }
}

fn maybe_refuse_exact_allocation() -> Result<(), AllocError> {
    #[cfg(test)]
    {
        INJECTED_EXACT_ALLOCATION_FAILURE.with(|state| {
            let (fail_on_call, calls) = state.get();
            let call = calls.saturating_add(1);
            state.set((fail_on_call, call));
            if call == fail_on_call {
                Err(AllocError)
            } else {
                Ok(())
            }
        })
    }
    #[cfg(not(test))]
    Ok(())
}

pub(super) struct ExactInitArena<T, A: Allocator = Global> {
    slots: AllocBox<[MaybeUninit<T>], A>,
    initialized: usize,
}

impl<T, A: Allocator> ExactInitArena<T, A> {
    pub(super) fn try_new_in(capacity: usize, allocator: A) -> Result<Self, AllocError> {
        Ok(Self {
            slots: AllocBox::<[T], A>::try_new_uninit_slice_in(capacity, allocator)?,
            initialized: 0,
        })
    }

    pub(super) fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub(super) fn initialized_len(&self) -> usize {
        self.initialized
    }

    pub(super) fn remaining(&self) -> usize {
        self.capacity() - self.initialized
    }

    pub(super) fn initialized_prefix(&self) -> &[T] {
        // SAFETY: slots `0..initialized` are initialized by `push`/`insert`; the
        // counter is advanced only after a successful write and is reduced when
        // those values are dropped.
        unsafe { slice::from_raw_parts(self.slots.as_ptr().cast::<T>(), self.initialized) }
    }

    pub(super) fn push(&mut self, value: T) -> Result<usize, T> {
        if self.initialized == self.capacity() {
            return Err(value);
        }
        let index = self.initialized;
        self.slots[index].write(value);
        self.initialized += 1;
        Ok(index)
    }

    pub(super) fn insert(&mut self, index: usize, value: T) -> Result<(), T> {
        assert!(
            index <= self.initialized,
            "arena insert index out of bounds"
        );
        if self.initialized == self.capacity() {
            return Err(value);
        }

        // SAFETY: `index <= initialized < capacity`. Moving the initialized
        // suffix one slot to the right leaves every value owned exactly once;
        // the duplicate bytes at `index` are immediately overwritten without
        // dropping because they represent the moved-from slot.
        unsafe {
            let base = self.slots.as_mut_ptr();
            std::ptr::copy(
                base.add(index),
                base.add(index + 1),
                self.initialized - index,
            );
            base.add(index).write(MaybeUninit::new(value));
        }
        self.initialized += 1;
        Ok(())
    }

    pub(super) fn reserve(&mut self, count: usize) -> Option<ExactInitReservation<'_, T, A>> {
        let end = self.initialized.checked_add(count)?;
        if end > self.capacity() {
            return None;
        }
        let start = self.initialized;
        Some(ExactInitReservation {
            arena: self,
            start,
            end,
            committed: false,
        })
    }

    fn truncate(&mut self, initialized: usize) {
        assert!(initialized <= self.initialized);
        while self.initialized > initialized {
            self.initialized -= 1;
            // SAFETY: this slot was in the initialized prefix. Decrementing the
            // counter before dropping prevents it from being dropped twice if
            // `T::drop` panics during unwinding.
            unsafe {
                self.slots[self.initialized].assume_init_drop();
            }
        }
    }
}

impl<T, A: Allocator> Drop for ExactInitArena<T, A> {
    fn drop(&mut self) {
        self.truncate(0);
    }
}

pub(super) struct ExactInitReservation<'a, T, A: Allocator> {
    arena: &'a mut ExactInitArena<T, A>,
    start: usize,
    end: usize,
    committed: bool,
}

impl<T, A: Allocator> ExactInitReservation<'_, T, A> {
    pub(super) fn push(&mut self, value: T) -> Result<(), T> {
        if self.arena.initialized_len() == self.end {
            return Err(value);
        }
        self.arena.push(value).map(|_| ())
    }

    pub(super) fn commit(mut self) -> Range<usize> {
        let range = self.start..self.arena.initialized_len();
        self.committed = true;
        range
    }
}

impl<T, A: Allocator> Drop for ExactInitReservation<'_, T, A> {
    fn drop(&mut self) {
        if !self.committed {
            self.arena.truncate(self.start);
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RangeScalarCacheConfigError {
    #[error(
        "range scalar cache budget exceeds maximum: requested={requested_bytes} maximum={maximum_bytes}"
    )]
    BudgetTooLarge {
        requested_bytes: u64,
        maximum_bytes: u64,
    },
    #[error(
        "range scalar cache governor already initialized with a different limit: existing={existing_bytes} requested={requested_bytes}"
    )]
    GovernorAlreadyInitialized {
        existing_bytes: u64,
        requested_bytes: u64,
    },
}

pub fn validate_range_scalar_cache_budget_bytes(
    requested_bytes: u64,
) -> Result<(), RangeScalarCacheConfigError> {
    if requested_bytes > MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES {
        return Err(RangeScalarCacheConfigError::BudgetTooLarge {
            requested_bytes,
            maximum_bytes: MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct RangeScalarCacheKey {
    pub segment_ordinal: usize,
    pub file_id: u8,
    pub chunk_offset: u64,
    pub chunk_len: u32,
    pub scalar_lane_offset: u32,
    pub scalar_lane_len: u32,
    pub projection: ChunkScalarProjection,
    pub chunk_kind: ChunkKind,
}

#[derive(Debug)]
pub(super) struct RangeScalarCacheEntry {
    key: RangeScalarCacheKey,
    header: ChunkScalarRecordHeader,
    samples_start: usize,
    samples_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeScalarCacheLayoutError {
    LayoutOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RangeScalarCacheLayout {
    pub entry_capacity: usize,
    pub sample_capacity: usize,
    pub entry_charge_bytes: u64,
    pub sample_charge_bytes: u64,
}

impl RangeScalarCacheLayout {
    pub(super) fn for_budget(budget_bytes: u64) -> Result<Self, RangeScalarCacheLayoutError> {
        let budget = usize::try_from(budget_bytes)
            .map_err(|_| RangeScalarCacheLayoutError::LayoutOverflow)?;
        let entry_size = std::mem::size_of::<RangeScalarCacheEntry>();
        let sample_size = std::mem::size_of::<ChunkScalarSample>();
        if entry_size == 0 || sample_size == 0 {
            return Err(RangeScalarCacheLayoutError::LayoutOverflow);
        }

        let entry_capacity = ((budget / 4) / entry_size).min(MAX_RANGE_SCALAR_CACHE_ENTRIES);
        let entry_layout = Layout::array::<RangeScalarCacheEntry>(entry_capacity)
            .map_err(|_| RangeScalarCacheLayoutError::LayoutOverflow)?;
        let remaining = budget
            .checked_sub(entry_layout.size())
            .ok_or(RangeScalarCacheLayoutError::LayoutOverflow)?;
        let sample_capacity = remaining / sample_size;
        let sample_layout = Layout::array::<ChunkScalarSample>(sample_capacity)
            .map_err(|_| RangeScalarCacheLayoutError::LayoutOverflow)?;
        let total = entry_layout
            .size()
            .checked_add(sample_layout.size())
            .ok_or(RangeScalarCacheLayoutError::LayoutOverflow)?;
        if total > budget {
            return Err(RangeScalarCacheLayoutError::LayoutOverflow);
        }

        let entry_charge_bytes = u64::try_from(entry_layout.size())
            .map_err(|_| RangeScalarCacheLayoutError::LayoutOverflow)?;
        let sample_charge_bytes = u64::try_from(sample_layout.size())
            .map_err(|_| RangeScalarCacheLayoutError::LayoutOverflow)?;
        Ok(Self {
            entry_capacity,
            sample_capacity,
            entry_charge_bytes,
            sample_charge_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeScalarCacheInitErrorKind {
    LayoutOverflow,
    AllocationRefused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RangeScalarCacheInitError {
    pub kind: RangeScalarCacheInitErrorKind,
    pub entry_charge_bytes: u64,
    pub sample_charge_bytes: u64,
    pub peak_retained_charge_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeScalarCacheAdmission {
    Admitted,
    AlreadyPresent,
    EntryTableFull,
    OversizedRecord,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RangeScalarCacheLookup {
    Hit,
    Miss,
}

pub(super) struct RangeScalarDecodeCache<A: Allocator = Global> {
    entries: ExactInitArena<RangeScalarCacheEntry, A>,
    samples: ExactInitArena<ChunkScalarSample, A>,
    layout: RangeScalarCacheLayout,
}

impl<A: Allocator + Clone> RangeScalarDecodeCache<A> {
    pub(super) fn try_new_in(
        budget_bytes: u64,
        allocator: A,
    ) -> Result<Self, RangeScalarCacheInitError> {
        let layout = RangeScalarCacheLayout::for_budget(budget_bytes).map_err(|_| {
            RangeScalarCacheInitError {
                kind: RangeScalarCacheInitErrorKind::LayoutOverflow,
                entry_charge_bytes: 0,
                sample_charge_bytes: 0,
                peak_retained_charge_bytes: 0,
            }
        })?;

        let entries = maybe_refuse_exact_allocation()
            .and_then(|()| ExactInitArena::try_new_in(layout.entry_capacity, allocator.clone()))
            .map_err(|_| RangeScalarCacheInitError {
                kind: RangeScalarCacheInitErrorKind::AllocationRefused,
                entry_charge_bytes: layout.entry_charge_bytes,
                sample_charge_bytes: layout.sample_charge_bytes,
                peak_retained_charge_bytes: 0,
            })?;
        let samples = match maybe_refuse_exact_allocation()
            .and_then(|()| ExactInitArena::try_new_in(layout.sample_capacity, allocator))
        {
            Ok(samples) => samples,
            Err(_) => {
                drop(entries);
                return Err(RangeScalarCacheInitError {
                    kind: RangeScalarCacheInitErrorKind::AllocationRefused,
                    entry_charge_bytes: layout.entry_charge_bytes,
                    sample_charge_bytes: layout.sample_charge_bytes,
                    peak_retained_charge_bytes: layout.entry_charge_bytes,
                });
            }
        };

        Ok(Self {
            entries,
            samples,
            layout,
        })
    }

    pub(super) fn entry_capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub(super) fn sample_capacity(&self) -> usize {
        self.samples.capacity()
    }

    pub(super) fn entry_len(&self) -> usize {
        self.entries.initialized_len()
    }

    pub(super) fn sample_len(&self) -> usize {
        self.samples.initialized_len()
    }

    pub(super) fn entry_charge_bytes(&self) -> u64 {
        self.layout.entry_charge_bytes
    }

    pub(super) fn sample_charge_bytes(&self) -> u64 {
        self.layout.sample_charge_bytes
    }

    pub(super) fn lookup(
        &self,
        key: &RangeScalarCacheKey,
    ) -> Option<(ChunkScalarRecordHeader, &[ChunkScalarSample])> {
        let index = self
            .entries
            .initialized_prefix()
            .binary_search_by_key(key, |entry| entry.key)
            .ok()?;
        let entry = &self.entries.initialized_prefix()[index];
        let end = entry.samples_start + entry.samples_len;
        Some((
            entry.header,
            &self.samples.initialized_prefix()[entry.samples_start..end],
        ))
    }

    pub(super) fn admit_with<F>(
        &mut self,
        key: RangeScalarCacheKey,
        header: ChunkScalarRecordHeader,
        sample_count: usize,
        decode: F,
    ) -> io::Result<RangeScalarCacheAdmission>
    where
        F: FnOnce(&mut dyn FnMut(ChunkScalarSample) -> io::Result<()>) -> io::Result<()>,
    {
        if header.kind != key.chunk_kind
            || usize::try_from(header.sample_count).ok() != Some(sample_count)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range scalar cache record header does not match key/sample count",
            ));
        }

        let insertion_index = match self
            .entries
            .initialized_prefix()
            .binary_search_by_key(&key, |entry| entry.key)
        {
            Ok(_) => return Ok(RangeScalarCacheAdmission::AlreadyPresent),
            Err(index) => index,
        };
        if self.entries.remaining() == 0 {
            return Ok(RangeScalarCacheAdmission::EntryTableFull);
        }
        if sample_count > self.samples.remaining() {
            return Ok(RangeScalarCacheAdmission::OversizedRecord);
        }

        let mut reservation = self.samples.reserve(sample_count).ok_or_else(|| {
            io::Error::other("range scalar cache sample reservation invariant violated")
        })?;
        let mut overflowed = false;
        let decode_result = {
            let mut emit = |sample| match reservation.push(sample) {
                Ok(()) => Ok(()),
                Err(_) => {
                    overflowed = true;
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "decoded scalar sample count exceeds chunk header",
                    ))
                }
            };
            decode(&mut emit)
        };
        decode_result?;
        if overflowed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded scalar sample count exceeds chunk header",
            ));
        }
        let actual_count = reservation.arena.initialized_len() - reservation.start;
        if actual_count != sample_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded scalar sample count does not match chunk header",
            ));
        }

        let samples_start = reservation.start;
        let entry = RangeScalarCacheEntry {
            key,
            header,
            samples_start,
            samples_len: sample_count,
        };
        if self.entries.insert(insertion_index, entry).is_err() {
            return Err(io::Error::other(
                "range scalar cache entry reservation invariant violated",
            ));
        }
        let committed = reservation.commit();
        debug_assert_eq!(committed, samples_start..samples_start + sample_count);
        Ok(RangeScalarCacheAdmission::Admitted)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeScalarCacheGovernorStats {
    pub limit_bytes: u64,
    pub current_leased_bytes: u64,
    pub peak_leased_bytes: u64,
}

#[derive(Debug)]
pub(super) struct RangeScalarCacheGovernor {
    limit_bytes: u64,
    current_leased_bytes: AtomicU64,
    peak_leased_bytes: AtomicU64,
    #[cfg(test)]
    attempt_barrier: Option<Arc<std::sync::Barrier>>,
}

impl RangeScalarCacheGovernor {
    pub(super) fn new(limit_bytes: u64) -> Self {
        Self {
            limit_bytes,
            current_leased_bytes: AtomicU64::new(0),
            peak_leased_bytes: AtomicU64::new(0),
            #[cfg(test)]
            attempt_barrier: None,
        }
    }

    #[cfg(test)]
    pub(super) fn new_with_attempt_barrier(
        limit_bytes: u64,
        attempt_barrier: Arc<std::sync::Barrier>,
    ) -> Self {
        Self {
            limit_bytes,
            current_leased_bytes: AtomicU64::new(0),
            peak_leased_bytes: AtomicU64::new(0),
            attempt_barrier: Some(attempt_barrier),
        }
    }

    fn finish_attempt<T>(&self, result: T) -> T {
        #[cfg(test)]
        if let Some(barrier) = &self.attempt_barrier {
            barrier.wait();
        }
        result
    }

    pub(super) fn try_acquire(self: &Arc<Self>, bytes: u64) -> Option<RangeScalarCacheLease> {
        let mut current = self.current_leased_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return self.finish_attempt(None);
            };
            if next > self.limit_bytes {
                return self.finish_attempt(None);
            }
            match self.current_leased_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak_leased_bytes.fetch_max(next, Ordering::AcqRel);
                    return self.finish_attempt(Some(RangeScalarCacheLease {
                        governor: Arc::clone(self),
                        bytes,
                    }));
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn stats(&self) -> RangeScalarCacheGovernorStats {
        RangeScalarCacheGovernorStats {
            limit_bytes: self.limit_bytes,
            current_leased_bytes: self.current_leased_bytes.load(Ordering::Acquire),
            peak_leased_bytes: self.peak_leased_bytes.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug)]
pub(super) struct RangeScalarCacheLease {
    governor: Arc<RangeScalarCacheGovernor>,
    bytes: u64,
}

impl Drop for RangeScalarCacheLease {
    fn drop(&mut self) {
        self.governor
            .current_leased_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

static RANGE_SCALAR_CACHE_GOVERNOR: OnceLock<Arc<RangeScalarCacheGovernor>> = OnceLock::new();

pub(super) fn configure_range_scalar_cache_governor_in(
    cell: &OnceLock<Arc<RangeScalarCacheGovernor>>,
    limit_bytes: u64,
) -> Result<(), RangeScalarCacheConfigError> {
    if let Some(existing) = cell.get() {
        return if existing.limit_bytes == limit_bytes {
            Ok(())
        } else {
            Err(RangeScalarCacheConfigError::GovernorAlreadyInitialized {
                existing_bytes: existing.limit_bytes,
                requested_bytes: limit_bytes,
            })
        };
    }

    let candidate = Arc::new(RangeScalarCacheGovernor::new(limit_bytes));
    match cell.set(candidate) {
        Ok(()) => Ok(()),
        Err(_) => {
            let existing = cell
                .get()
                .expect("OnceLock rejected initialization without storing a value");
            if existing.limit_bytes == limit_bytes {
                Ok(())
            } else {
                Err(RangeScalarCacheConfigError::GovernorAlreadyInitialized {
                    existing_bytes: existing.limit_bytes,
                    requested_bytes: limit_bytes,
                })
            }
        }
    }
}

pub(super) fn process_range_scalar_cache_governor() -> Arc<RangeScalarCacheGovernor> {
    Arc::clone(RANGE_SCALAR_CACHE_GOVERNOR.get_or_init(|| {
        Arc::new(RangeScalarCacheGovernor::new(
            DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
        ))
    }))
}

pub fn configure_range_scalar_cache_governor(
    limit_bytes: u64,
) -> Result<(), RangeScalarCacheConfigError> {
    configure_range_scalar_cache_governor_in(&RANGE_SCALAR_CACHE_GOVERNOR, limit_bytes)
}

pub fn range_scalar_cache_governor_stats() -> RangeScalarCacheGovernorStats {
    RANGE_SCALAR_CACHE_GOVERNOR.get().map_or(
        RangeScalarCacheGovernorStats {
            limit_bytes: DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
            current_leased_bytes: 0,
            peak_leased_bytes: 0,
        },
        |governor| governor.stats(),
    )
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeScalarCacheSummary {
    pub configured_budget_bytes: u64,
    pub governor_lease_bytes: u64,
    pub governor_refused: bool,
    pub allocation_refused: bool,
    pub layout_overflow: bool,
    pub entry_arena_charge_bytes: u64,
    pub sample_arena_charge_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub admitted_entries: u64,
    pub streaming_budget_bypasses: u64,
    pub unsupported_bypasses: u64,
    pub logical_hit_bytes: u64,
    pub logical_miss_or_bypass_bytes: u64,
    pub peak_retained_charge_bytes: u64,
    pub retained_charge_after_finalize: u64,
}

pub(super) struct RangeScalarCacheCall<A: Allocator + Clone = Global> {
    summary: RangeScalarCacheSummary,
    governor: Arc<RangeScalarCacheGovernor>,
    allocator: A,
    admission_attempted: bool,
    lease: Option<RangeScalarCacheLease>,
    cache: Option<RangeScalarDecodeCache<A>>,
}

impl RangeScalarCacheCall<Global> {
    pub(super) fn new(
        configured_budget_bytes: u64,
        governor: Arc<RangeScalarCacheGovernor>,
    ) -> Self {
        Self::new_in(configured_budget_bytes, governor, Global)
    }
}

impl<A: Allocator + Clone> RangeScalarCacheCall<A> {
    pub(super) fn new_in(
        configured_budget_bytes: u64,
        governor: Arc<RangeScalarCacheGovernor>,
        allocator: A,
    ) -> Self {
        Self {
            summary: RangeScalarCacheSummary {
                configured_budget_bytes,
                ..RangeScalarCacheSummary::default()
            },
            governor,
            allocator,
            admission_attempted: false,
            lease: None,
            cache: None,
        }
    }

    pub(super) fn summary(&self) -> RangeScalarCacheSummary {
        self.summary
    }

    pub(super) fn summary_mut(&mut self) -> &mut RangeScalarCacheSummary {
        &mut self.summary
    }

    pub(super) fn cache_mut(&mut self) -> Option<&mut RangeScalarDecodeCache<A>> {
        if !self.admission_attempted {
            self.try_initialize_cache();
        }
        self.cache.as_mut()
    }

    pub(super) fn classify_eligible(
        &mut self,
        key: &RangeScalarCacheKey,
        logical_bytes: u64,
    ) -> RangeScalarCacheLookup {
        if !self.admission_attempted {
            self.try_initialize_cache();
        }

        if self
            .cache
            .as_ref()
            .is_some_and(|cache| cache.lookup(key).is_some())
        {
            self.summary.hits = self.summary.hits.saturating_add(1);
            self.summary.logical_hit_bytes =
                self.summary.logical_hit_bytes.saturating_add(logical_bytes);
            RangeScalarCacheLookup::Hit
        } else {
            self.summary.misses = self.summary.misses.saturating_add(1);
            self.summary.logical_miss_or_bypass_bytes = self
                .summary
                .logical_miss_or_bypass_bytes
                .saturating_add(logical_bytes);
            if self.cache.is_none() {
                self.summary.streaming_budget_bypasses =
                    self.summary.streaming_budget_bypasses.saturating_add(1);
            }
            RangeScalarCacheLookup::Miss
        }
    }

    pub(super) fn classify_unsupported(&mut self, logical_bytes: u64) {
        self.summary.unsupported_bypasses = self.summary.unsupported_bypasses.saturating_add(1);
        self.summary.logical_miss_or_bypass_bytes = self
            .summary
            .logical_miss_or_bypass_bytes
            .saturating_add(logical_bytes);
    }

    pub(super) fn cache_available(&self) -> bool {
        self.cache.is_some()
    }

    pub(super) fn lookup(
        &self,
        key: &RangeScalarCacheKey,
    ) -> Option<(ChunkScalarRecordHeader, &[ChunkScalarSample])> {
        self.cache.as_ref()?.lookup(key)
    }

    pub(super) fn admit_with<F>(
        &mut self,
        key: RangeScalarCacheKey,
        header: ChunkScalarRecordHeader,
        sample_count: usize,
        decode: F,
    ) -> io::Result<RangeScalarCacheAdmission>
    where
        F: FnOnce(&mut dyn FnMut(ChunkScalarSample) -> io::Result<()>) -> io::Result<()>,
    {
        let Some(cache) = self.cache.as_mut() else {
            return Ok(RangeScalarCacheAdmission::Unavailable);
        };
        let admission = cache.admit_with(key, header, sample_count, decode)?;
        match admission {
            RangeScalarCacheAdmission::Admitted => {
                self.summary.admitted_entries = self.summary.admitted_entries.saturating_add(1);
            }
            RangeScalarCacheAdmission::EntryTableFull
            | RangeScalarCacheAdmission::OversizedRecord => {
                self.summary.streaming_budget_bypasses =
                    self.summary.streaming_budget_bypasses.saturating_add(1);
            }
            RangeScalarCacheAdmission::AlreadyPresent | RangeScalarCacheAdmission::Unavailable => {}
        }
        Ok(admission)
    }

    fn try_initialize_cache(&mut self) {
        debug_assert!(!self.admission_attempted);
        self.admission_attempted = true;
        if self.summary.configured_budget_bytes == 0 {
            return;
        }

        let Some(lease) = self
            .governor
            .try_acquire(self.summary.configured_budget_bytes)
        else {
            self.summary.governor_refused = true;
            return;
        };
        self.summary.governor_lease_bytes = self.summary.configured_budget_bytes;

        match RangeScalarDecodeCache::try_new_in(
            self.summary.configured_budget_bytes,
            self.allocator.clone(),
        ) {
            Ok(cache) => {
                self.summary.entry_arena_charge_bytes = cache.entry_charge_bytes();
                self.summary.sample_arena_charge_bytes = cache.sample_charge_bytes();
                self.summary.peak_retained_charge_bytes = self
                    .summary
                    .entry_arena_charge_bytes
                    .checked_add(self.summary.sample_arena_charge_bytes)
                    .expect("validated arena layout charge must fit u64");
                self.lease = Some(lease);
                self.cache = Some(cache);
            }
            Err(error) => {
                self.summary.entry_arena_charge_bytes = error.entry_charge_bytes;
                self.summary.sample_arena_charge_bytes = error.sample_charge_bytes;
                self.summary.peak_retained_charge_bytes = error.peak_retained_charge_bytes;
                match error.kind {
                    RangeScalarCacheInitErrorKind::LayoutOverflow => {
                        self.summary.layout_overflow = true;
                    }
                    RangeScalarCacheInitErrorKind::AllocationRefused => {
                        self.summary.allocation_refused = true;
                    }
                }
                drop(lease);
            }
        }
    }

    pub(super) fn finish(mut self) -> RangeScalarCacheSummary {
        self.cache.take();
        self.lease.take();
        self.summary.retained_charge_after_finalize = 0;
        self.summary
    }
}

impl<A: Allocator + Clone> Drop for RangeScalarCacheCall<A> {
    fn drop(&mut self) {
        self.cache.take();
        self.lease.take();
    }
}
