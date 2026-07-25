use std::collections::hash_map::Entry;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::otlp_labelset::CanonicalLabelSet;
use crate::promql::{normalize_label_name, normalize_metric_name};

use super::super::normalizer::{normalize_label_key, normalize_label_value};
use super::super::{
    KeyValueRef, SeriesRef, SymbolId, U64HashMap, estimate_hashmap_table_bytes,
    estimate_vec_buffer_bytes,
};
use super::common::{LabelSetStore, LabelSetStoreError};
use super::flat::InternedKeyValue;

const DEFAULT_SYMBOL_BYTES_PAGE_CAPACITY: usize = 64 * 1024;
const DEFAULT_SYMBOL_LOCS_PAGE_CAPACITY: usize = 8 * 1024;
const DEFAULT_SERIES_LOCS_PAGE_CAPACITY: usize = 8 * 1024;
const DEFAULT_KEY_VALUES_PAGE_CAPACITY: usize = 16 * 1024;
const MAX_ADDRESSABLE_PAGES: u64 = u32::MAX as u64 + 1;
static NEXT_STORE_LINEAGE_ID: AtomicU64 = AtomicU64::new(1);

/// A checked failure from the live-only versioned label backing.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum VersionedFlatLabelStoreError {
    #[error("{region} {field}={value} exceeds the representable or configured maximum {maximum}")]
    CapacityExceeded {
        region: &'static str,
        field: &'static str,
        value: u64,
        maximum: u64,
    },

    #[error("{region} could not reserve {requested} elements")]
    AllocationFailed {
        region: &'static str,
        requested: usize,
    },

    #[error("series ref {series_ref} is outside snapshot revision {revision}")]
    SeriesRefOutOfRange { series_ref: u32, revision: u64 },

    #[error(
        "raw live label revision {raw_revision} differs from canonical projection revision {canonical_revision}"
    )]
    InconsistentRevision {
        raw_revision: u64,
        canonical_revision: u64,
    },

    #[error("symbol id {symbol_id} is outside snapshot symbol count {symbol_count}")]
    SymbolIdOutOfRange { symbol_id: u32, symbol_count: u64 },

    #[error(
        "{region} locator page={page} offset={offset} len={len} is outside the published pages"
    )]
    InvalidLocator {
        region: &'static str,
        page: u32,
        offset: u32,
        len: u32,
    },

    #[error("published symbol {symbol_id} is not valid UTF-8")]
    InvalidUtf8 { symbol_id: u32 },
}

/// Exact page-payload counters for admission and publication telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VersionedFlatLabelStoreMemoryStats {
    /// Exact `capacity * size_of::<T>()` for element buffers that have been
    /// sealed into immutable shared pages.
    ///
    /// Arc headers, page-directory entries, hash indexes, and allocator
    /// bookkeeping are deliberately outside these payload counters.
    pub shared_allocated_bytes: usize,
    /// Exact `len * size_of::<T>()` for immutable shared page elements.
    pub shared_used_bytes: usize,
    /// Exact `capacity * size_of::<T>()` for writer-only mutable tail buffers.
    pub tail_allocated_bytes: usize,
    /// Exact `len * size_of::<T>()` for writer-only mutable tail elements.
    pub tail_used_bytes: usize,
    pub shared_pages: usize,
    pub non_empty_tails: usize,
}

impl VersionedFlatLabelStoreMemoryStats {
    fn add(self, other: Self) -> Self {
        Self {
            shared_allocated_bytes: self
                .shared_allocated_bytes
                .saturating_add(other.shared_allocated_bytes),
            shared_used_bytes: self
                .shared_used_bytes
                .saturating_add(other.shared_used_bytes),
            tail_allocated_bytes: self
                .tail_allocated_bytes
                .saturating_add(other.tail_allocated_bytes),
            tail_used_bytes: self.tail_used_bytes.saturating_add(other.tail_used_bytes),
            shared_pages: self.shared_pages.saturating_add(other.shared_pages),
            non_empty_tails: self.non_empty_tails.saturating_add(other.non_empty_tails),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageLoc {
    page: u32,
    offset: u32,
    len: u32,
}

impl PageLoc {
    const EMPTY: Self = Self {
        page: 0,
        offset: 0,
        len: 0,
    };
}

#[derive(Clone)]
struct SharedPage<T> {
    base: u64,
    values: Arc<Vec<T>>,
}

impl<T> SharedPage<T> {
    fn end(&self) -> u64 {
        self.base.saturating_add(self.values.len() as u64)
    }
}

struct PreparedAppend<T> {
    page: u32,
    offset: u32,
    replacement_tail: Option<Vec<T>>,
    seal_before: bool,
    seal_after: bool,
}

struct AppendOnlyPages<T> {
    region: &'static str,
    pages: Vec<SharedPage<T>>,
    tail: Vec<T>,
    total_len: u64,
    page_capacity: usize,
    max_pages: u64,
}

impl<T> AppendOnlyPages<T>
where
    T: Copy,
{
    fn new(region: &'static str, page_capacity: usize, max_pages: u64) -> Self {
        assert!(page_capacity > 0);
        assert!((1..=MAX_ADDRESSABLE_PAGES).contains(&max_pages));
        Self {
            region,
            pages: Vec::new(),
            tail: Vec::new(),
            total_len: 0,
            page_capacity,
            max_pages,
        }
    }

    fn len(&self) -> u64 {
        self.total_len
    }

    fn page_index_for_new_tail(
        &self,
        seal_before: bool,
    ) -> Result<u32, VersionedFlatLabelStoreError> {
        let pages_before_new_tail = (self.pages.len() as u64)
            .checked_add(u64::from(seal_before))
            .ok_or(VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "page_count",
                value: u64::MAX,
                maximum: self.max_pages,
            })?;
        if pages_before_new_tail >= self.max_pages {
            return Err(VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "page_count",
                value: pages_before_new_tail.saturating_add(1),
                maximum: self.max_pages,
            });
        }
        u32::try_from(pages_before_new_tail).map_err(|_| {
            VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "page_index",
                value: pages_before_new_tail,
                maximum: u32::MAX as u64,
            }
        })
    }

    fn prepare_append(
        &mut self,
        count: usize,
    ) -> Result<PreparedAppend<T>, VersionedFlatLabelStoreError> {
        debug_assert!(count > 0);
        let count_u64 =
            u64::try_from(count).map_err(|_| VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "append_len",
                value: u64::MAX,
                maximum: u32::MAX as u64,
            })?;
        let _new_total = self.total_len.checked_add(count_u64).ok_or(
            VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "total_len",
                value: u64::MAX,
                maximum: u64::MAX,
            },
        )?;
        let count_u32 =
            u32::try_from(count).map_err(|_| VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "append_len",
                value: count_u64,
                maximum: u32::MAX as u64,
            })?;

        let tail_room = self.page_capacity.saturating_sub(self.tail.len());
        let use_existing_tail = !self.tail.is_empty() && count <= tail_room;
        let seal_before = !self.tail.is_empty() && !use_existing_tail;
        let offset = if use_existing_tail {
            u32::try_from(self.tail.len()).map_err(|_| {
                VersionedFlatLabelStoreError::CapacityExceeded {
                    region: self.region,
                    field: "page_offset",
                    value: self.tail.len() as u64,
                    maximum: u32::MAX as u64,
                }
            })?
        } else {
            0
        };
        offset
            .checked_add(count_u32)
            .ok_or(VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "page_end",
                value: u64::from(offset).saturating_add(count_u64),
                maximum: u32::MAX as u64,
            })?;

        let target_capacity = if use_existing_tail {
            self.page_capacity
        } else {
            self.page_capacity.max(count)
        };
        let resulting_len = if use_existing_tail {
            self.tail.len().saturating_add(count)
        } else {
            count
        };
        let seal_after = resulting_len == target_capacity;
        let pages_to_seal = usize::from(seal_before) + usize::from(seal_after);
        let resulting_pages = (self.pages.len() as u64)
            .checked_add(pages_to_seal as u64)
            .ok_or(VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "page_count",
                value: u64::MAX,
                maximum: self.max_pages,
            })?;
        if resulting_pages > self.max_pages {
            return Err(VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "page_count",
                value: resulting_pages,
                maximum: self.max_pages,
            });
        }
        self.pages.try_reserve(pages_to_seal).map_err(|_| {
            VersionedFlatLabelStoreError::AllocationFailed {
                region: self.region,
                requested: pages_to_seal,
            }
        })?;

        let replacement_tail = if use_existing_tail {
            self.tail.try_reserve(count).map_err(|_| {
                VersionedFlatLabelStoreError::AllocationFailed {
                    region: self.region,
                    requested: count,
                }
            })?;
            None
        } else {
            let mut tail = Vec::new();
            // `page_capacity` is a logical rollover boundary, not an eager
            // allocation request. Publications can seal a very small tail, so
            // reserving the whole logical page here would retain large slack
            // once that tail becomes shared.
            tail.try_reserve(count).map_err(|_| {
                VersionedFlatLabelStoreError::AllocationFailed {
                    region: self.region,
                    requested: count,
                }
            })?;
            Some(tail)
        };
        let page = if use_existing_tail {
            self.page_index_for_new_tail(false)?
        } else {
            self.page_index_for_new_tail(seal_before)?
        };

        Ok(PreparedAppend {
            page,
            offset,
            replacement_tail,
            seal_before,
            seal_after,
        })
    }

    fn apply_append(&mut self, mut prepared: PreparedAppend<T>, values: &[T]) -> PageLoc {
        if prepared.seal_before {
            self.seal_tail_prepared();
        }
        if let Some(tail) = prepared.replacement_tail.take() {
            debug_assert!(self.tail.is_empty());
            self.tail = tail;
        }

        self.tail.extend_from_slice(values);
        self.total_len += values.len() as u64;
        let len =
            u32::try_from(values.len()).expect("append length was checked during preparation");
        let loc = PageLoc {
            page: prepared.page,
            offset: prepared.offset,
            len,
        };
        if prepared.seal_after {
            self.seal_tail_prepared();
        }
        loc
    }

    fn seal_tail_prepared(&mut self) {
        if self.tail.is_empty() {
            return;
        }
        debug_assert!(self.pages.len() < self.pages.capacity());
        let values = Arc::new(std::mem::take(&mut self.tail));
        let base = self.total_len - values.len() as u64;
        self.pages.push(SharedPage { base, values });
    }

    fn try_seal_tail(&mut self) -> Result<(), VersionedFlatLabelStoreError> {
        if self.tail.is_empty() {
            return Ok(());
        }
        if self.pages.len() as u64 >= self.max_pages {
            return Err(VersionedFlatLabelStoreError::CapacityExceeded {
                region: self.region,
                field: "page_count",
                value: self.pages.len() as u64 + 1,
                maximum: self.max_pages,
            });
        }
        self.pages
            .try_reserve(1)
            .map_err(|_| VersionedFlatLabelStoreError::AllocationFailed {
                region: self.region,
                requested: 1,
            })?;
        self.seal_tail_prepared();
        Ok(())
    }

    fn snapshot(&self) -> PagedSnapshot<T> {
        PagedSnapshot {
            region: self.region,
            pages: Arc::from(self.pages.clone()),
            len: self.total_len,
        }
    }

    fn get_dense(&self, index: u64) -> Option<&T> {
        if index >= self.total_len {
            return None;
        }
        if let Some(page) = find_dense_page(&self.pages, index) {
            return page.values.get((index - page.base) as usize);
        }
        let tail_base = self.total_len - self.tail.len() as u64;
        self.tail.get((index - tail_base) as usize)
    }

    fn slice(&self, loc: PageLoc) -> Option<&[T]> {
        if loc.len == 0 {
            return Some(&[]);
        }
        let page_index = loc.page as usize;
        let values = if page_index < self.pages.len() {
            self.pages[page_index].values.as_slice()
        } else if page_index == self.pages.len() {
            self.tail.as_slice()
        } else {
            return None;
        };
        checked_slice(values, loc)
    }

    fn memory_stats(&self) -> VersionedFlatLabelStoreMemoryStats {
        let element_size = std::mem::size_of::<T>();
        let shared_allocated_bytes = self
            .pages
            .iter()
            .map(|page| page.values.capacity().saturating_mul(element_size))
            .fold(0usize, usize::saturating_add);
        let shared_used_bytes = self
            .pages
            .iter()
            .map(|page| page.values.len().saturating_mul(element_size))
            .fold(0usize, usize::saturating_add);
        VersionedFlatLabelStoreMemoryStats {
            shared_allocated_bytes,
            shared_used_bytes,
            tail_allocated_bytes: self.tail.capacity().saturating_mul(element_size),
            tail_used_bytes: self.tail.len().saturating_mul(element_size),
            shared_pages: self.pages.len(),
            non_empty_tails: usize::from(!self.tail.is_empty()),
        }
    }
}

#[derive(Clone)]
struct PagedSnapshot<T> {
    region: &'static str,
    pages: Arc<[SharedPage<T>]>,
    len: u64,
}

impl<T> PagedSnapshot<T> {
    fn get_dense(&self, index: u64) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        let page = find_dense_page(self.pages.as_ref(), index)?;
        page.values.get((index - page.base) as usize)
    }

    fn slice(&self, loc: PageLoc) -> Result<&[T], VersionedFlatLabelStoreError> {
        if loc.len == 0 {
            return Ok(&[]);
        }
        let values = self
            .pages
            .get(loc.page as usize)
            .ok_or(VersionedFlatLabelStoreError::InvalidLocator {
                region: self.region,
                page: loc.page,
                offset: loc.offset,
                len: loc.len,
            })?
            .values
            .as_slice();
        checked_slice(values, loc).ok_or(VersionedFlatLabelStoreError::InvalidLocator {
            region: self.region,
            page: loc.page,
            offset: loc.offset,
            len: loc.len,
        })
    }

    fn memory_stats(&self) -> VersionedFlatLabelStoreMemoryStats {
        let element_size = std::mem::size_of::<T>();
        let shared_allocated_bytes = self
            .pages
            .iter()
            .map(|page| page.values.capacity().saturating_mul(element_size))
            .fold(0usize, usize::saturating_add);
        let shared_used_bytes = self
            .pages
            .iter()
            .map(|page| page.values.len().saturating_mul(element_size))
            .fold(0usize, usize::saturating_add);
        VersionedFlatLabelStoreMemoryStats {
            shared_allocated_bytes,
            shared_used_bytes,
            tail_allocated_bytes: 0,
            tail_used_bytes: 0,
            shared_pages: self.pages.len(),
            non_empty_tails: 0,
        }
    }
}

fn find_dense_page<T>(pages: &[SharedPage<T>], index: u64) -> Option<&SharedPage<T>> {
    let upper = pages.partition_point(|page| page.base <= index);
    let page = pages.get(upper.checked_sub(1)?)?;
    (index < page.end()).then_some(page)
}

fn checked_slice<T>(values: &[T], loc: PageLoc) -> Option<&[T]> {
    let start = loc.offset as usize;
    let end = start.checked_add(loc.len as usize)?;
    values.get(start..end)
}

#[derive(Clone, Copy)]
struct VersionedPageCapacities {
    symbol_bytes: usize,
    symbol_locs: usize,
    series_locs: usize,
    key_values: usize,
    max_symbol_byte_pages: u64,
    max_symbol_loc_pages: u64,
    max_series_loc_pages: u64,
    max_key_value_pages: u64,
    max_symbols: u64,
    max_series: u64,
}

impl Default for VersionedPageCapacities {
    fn default() -> Self {
        Self {
            symbol_bytes: DEFAULT_SYMBOL_BYTES_PAGE_CAPACITY,
            symbol_locs: DEFAULT_SYMBOL_LOCS_PAGE_CAPACITY,
            series_locs: DEFAULT_SERIES_LOCS_PAGE_CAPACITY,
            key_values: DEFAULT_KEY_VALUES_PAGE_CAPACITY,
            max_symbol_byte_pages: MAX_ADDRESSABLE_PAGES,
            max_symbol_loc_pages: MAX_ADDRESSABLE_PAGES,
            max_series_loc_pages: MAX_ADDRESSABLE_PAGES,
            max_key_value_pages: MAX_ADDRESSABLE_PAGES,
            max_symbols: MAX_ADDRESSABLE_PAGES,
            max_series: MAX_ADDRESSABLE_PAGES,
        }
    }
}

/// An append-only symbol table whose published pages can be shared by
/// immutable query views.
///
/// This is deliberately separate from `DefaultSymbolTable`: enabling live
/// publication therefore does not add a branch to the normal ingest hot path.
pub struct VersionedSymbolTable {
    hash_to_id: U64HashMap<SymbolId>,
    hash_collisions: U64HashMap<Vec<SymbolId>>,
    symbol_hash: ahash::RandomState,
    bytes: AppendOnlyPages<u8>,
    locs: AppendOnlyPages<PageLoc>,
    max_symbols: u64,
    estimated_collision_bytes: usize,
}

impl Default for VersionedSymbolTable {
    fn default() -> Self {
        Self::with_capacities(VersionedPageCapacities::default())
    }
}

impl VersionedSymbolTable {
    fn with_capacities(capacities: VersionedPageCapacities) -> Self {
        Self {
            hash_to_id: U64HashMap::default(),
            hash_collisions: U64HashMap::default(),
            symbol_hash: ahash::RandomState::new(),
            bytes: AppendOnlyPages::new(
                "live symbol bytes",
                capacities.symbol_bytes,
                capacities.max_symbol_byte_pages,
            ),
            locs: AppendOnlyPages::new(
                "live symbol locators",
                capacities.symbol_locs,
                capacities.max_symbol_loc_pages,
            ),
            max_symbols: capacities.max_symbols,
            estimated_collision_bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        usize::try_from(self.locs.len()).unwrap_or(usize::MAX)
    }

    pub fn symbol_count(&self) -> u64 {
        self.locs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.locs.len() == 0
    }

    pub fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        self.lookup_with_hash(symbol, self.symbol_hash.hash_one(symbol))
    }

    fn lookup_with_hash(&self, symbol: &str, hash: u64) -> Option<SymbolId> {
        let &first = self.hash_to_id.get(&hash)?;
        if self.try_resolve(first).ok()? == symbol {
            return Some(first);
        }
        self.hash_collisions.get(&hash).and_then(|collisions| {
            collisions
                .iter()
                .copied()
                .find(|id| self.try_resolve(*id).is_ok_and(|stored| stored == symbol))
        })
    }

    pub fn intern(&mut self, symbol: &str) -> Result<SymbolId, VersionedFlatLabelStoreError> {
        let hash = self.symbol_hash.hash_one(symbol);
        if let Some(id) = self.lookup_with_hash(symbol, hash) {
            return Ok(id);
        }

        let symbol_count = self.locs.len();
        if symbol_count >= self.max_symbols {
            return Err(VersionedFlatLabelStoreError::CapacityExceeded {
                region: "live symbols",
                field: "symbol_count",
                value: symbol_count.saturating_add(1),
                maximum: self.max_symbols,
            });
        }
        let raw_id = u32::try_from(symbol_count).map_err(|_| {
            VersionedFlatLabelStoreError::CapacityExceeded {
                region: "live symbols",
                field: "symbol_id",
                value: symbol_count,
                maximum: u32::MAX as u64,
            }
        })?;
        let bytes_append = if symbol.is_empty() {
            None
        } else {
            Some(self.bytes.prepare_append(symbol.len())?)
        };
        let loc = bytes_append
            .as_ref()
            .map_or(PageLoc::EMPTY, |append| PageLoc {
                page: append.page,
                offset: append.offset,
                len: u32::try_from(symbol.len())
                    .expect("symbol length was checked during preparation"),
            });
        let loc_append = self.locs.prepare_append(1)?;

        self.hash_to_id.try_reserve(1).map_err(|_| {
            VersionedFlatLabelStoreError::AllocationFailed {
                region: "live symbol hash index",
                requested: 1,
            }
        })?;
        let mut new_collision_list = None;
        if self.hash_to_id.contains_key(&hash) {
            self.hash_collisions.try_reserve(1).map_err(|_| {
                VersionedFlatLabelStoreError::AllocationFailed {
                    region: "live symbol collision index",
                    requested: 1,
                }
            })?;
            if let Some(collisions) = self.hash_collisions.get_mut(&hash) {
                collisions.try_reserve(1).map_err(|_| {
                    VersionedFlatLabelStoreError::AllocationFailed {
                        region: "live symbol collision list",
                        requested: 1,
                    }
                })?;
            } else {
                let mut collisions = Vec::new();
                collisions.try_reserve_exact(1).map_err(|_| {
                    VersionedFlatLabelStoreError::AllocationFailed {
                        region: "live symbol collision list",
                        requested: 1,
                    }
                })?;
                new_collision_list = Some(collisions);
            }
        }

        if let Some(bytes_append) = bytes_append {
            let appended_loc = self.bytes.apply_append(bytes_append, symbol.as_bytes());
            debug_assert_eq!(appended_loc, loc);
        }
        self.locs.apply_append(loc_append, &[loc]);
        let id = SymbolId(raw_id);
        match self.hash_to_id.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(id);
            }
            Entry::Occupied(_) => {
                let collisions = self
                    .hash_collisions
                    .entry(hash)
                    .or_insert_with(|| new_collision_list.expect("collision list was prepared"));
                let before = collisions.capacity();
                collisions.push(id);
                self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                    collisions
                        .capacity()
                        .saturating_sub(before)
                        .saturating_mul(std::mem::size_of::<SymbolId>()),
                );
            }
        }
        Ok(id)
    }

    pub fn try_resolve(&self, id: SymbolId) -> Result<&str, VersionedFlatLabelStoreError> {
        let loc = self.locs.get_dense(u64::from(id.get())).ok_or(
            VersionedFlatLabelStoreError::SymbolIdOutOfRange {
                symbol_id: id.get(),
                symbol_count: self.locs.len(),
            },
        )?;
        let bytes = self
            .bytes
            .slice(*loc)
            .ok_or(VersionedFlatLabelStoreError::InvalidLocator {
                region: "live symbol bytes",
                page: loc.page,
                offset: loc.offset,
                len: loc.len,
            })?;
        std::str::from_utf8(bytes).map_err(|_| VersionedFlatLabelStoreError::InvalidUtf8 {
            symbol_id: id.get(),
        })
    }

    pub fn resolve(&self, id: SymbolId) -> &str {
        self.try_resolve(id)
            .expect("VersionedSymbolTable symbol ID must be valid")
    }

    /// Seals only the current mutable byte/locator tails and returns an
    /// immutable view. Existing payload pages are retained by `Arc`; no symbol
    /// bytes or locators from older pages are cloned.
    pub fn snapshot(
        &mut self,
    ) -> Result<VersionedSymbolTableSnapshot, VersionedFlatLabelStoreError> {
        self.bytes.try_seal_tail()?;
        self.locs.try_seal_tail()?;
        Ok(VersionedSymbolTableSnapshot {
            bytes: self.bytes.snapshot(),
            locs: self.locs.snapshot(),
            symbol_count: self.locs.len(),
        })
    }

    pub fn memory_stats(&self) -> VersionedFlatLabelStoreMemoryStats {
        self.bytes.memory_stats().add(self.locs.memory_stats())
    }

    fn estimate_index_allocated_bytes(&self) -> usize {
        estimate_hashmap_table_bytes(&self.hash_to_id)
            .saturating_add(estimate_hashmap_table_bytes(&self.hash_collisions))
            .saturating_add(self.estimated_collision_bytes)
    }

    fn estimate_index_used_bytes(&self) -> usize {
        self.hash_to_id
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SymbolId)>())
            .saturating_add(
                self.hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SymbolId>)>()),
            )
            .saturating_add(
                self.hash_collisions
                    .values()
                    .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SymbolId>()))
                    .fold(0usize, usize::saturating_add),
            )
    }
}

/// An immutable, `Send + Sync` symbol revision pinned by a live query view.
#[derive(Clone)]
pub struct VersionedSymbolTableSnapshot {
    bytes: PagedSnapshot<u8>,
    locs: PagedSnapshot<PageLoc>,
    symbol_count: u64,
}

impl VersionedSymbolTableSnapshot {
    pub fn len(&self) -> usize {
        usize::try_from(self.symbol_count).unwrap_or(usize::MAX)
    }

    pub fn symbol_count(&self) -> u64 {
        self.symbol_count
    }

    pub fn is_empty(&self) -> bool {
        self.symbol_count == 0
    }

    pub fn try_resolve(&self, id: SymbolId) -> Result<&str, VersionedFlatLabelStoreError> {
        let loc = self.locs.get_dense(u64::from(id.get())).ok_or(
            VersionedFlatLabelStoreError::SymbolIdOutOfRange {
                symbol_id: id.get(),
                symbol_count: self.symbol_count,
            },
        )?;
        let bytes = self.bytes.slice(*loc)?;
        std::str::from_utf8(bytes).map_err(|_| VersionedFlatLabelStoreError::InvalidUtf8 {
            symbol_id: id.get(),
        })
    }

    pub fn resolve(&self, id: SymbolId) -> &str {
        self.try_resolve(id)
            .expect("published VersionedSymbolTable symbol ID must be valid")
    }

    pub fn memory_stats(&self) -> VersionedFlatLabelStoreMemoryStats {
        self.bytes.memory_stats().add(self.locs.memory_stats())
    }
}

/// A borrowed row of canonical `(key_id, value_id)` pairs.
#[derive(Clone, Copy, Debug)]
pub struct VersionedFlatInternedLabelSetRow<'a> {
    labels: &'a [InternedKeyValue],
}

impl<'a> VersionedFlatInternedLabelSetRow<'a> {
    pub fn len(self) -> usize {
        self.labels.len()
    }

    pub fn is_empty(self) -> bool {
        self.labels.is_empty()
    }

    pub fn get(self, index: usize) -> Option<(SymbolId, SymbolId)> {
        self.labels.get(index).map(|label| (label.key, label.value))
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = (SymbolId, SymbolId)> + 'a {
        self.labels.iter().map(|label| (label.key, label.value))
    }
}

/// A live-only FlatInterned label store with append-only, snapshot-shareable
/// symbol, row-locator, and key/value pages.
///
/// Its row revision is an exclusive dense count. A successful new row at
/// revision `N` always receives `SeriesRef(N)`; a failed append never consumes
/// a ref. Publishing moves only mutable tails into immutable `Arc` pages.
pub struct VersionedFlatInternedLabelSetStore {
    lineage_id: u64,
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    labelset_hash: ahash::RandomState,
    symbols: VersionedSymbolTable,
    series: AppendOnlyPages<PageLoc>,
    key_values: AppendOnlyPages<InternedKeyValue>,
    canonical_series: AppendOnlyPages<PageLoc>,
    canonical_key_values: AppendOnlyPages<InternedKeyValue>,
    encoded_scratch: Vec<InternedKeyValue>,
    canonical_scratch: Vec<InternedKeyValue>,
    max_series: u64,
    estimated_collision_bytes: usize,
}

impl Default for VersionedFlatInternedLabelSetStore {
    fn default() -> Self {
        Self::with_capacities(VersionedPageCapacities::default())
    }
}

impl VersionedFlatInternedLabelSetStore {
    fn with_capacities(capacities: VersionedPageCapacities) -> Self {
        let lineage_id = NEXT_STORE_LINEAGE_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .expect("versioned label-store lineage ID space is exhausted");
        Self {
            lineage_id,
            by_hash: U64HashMap::default(),
            by_hash_collisions: U64HashMap::default(),
            labelset_hash: ahash::RandomState::new(),
            symbols: VersionedSymbolTable::with_capacities(capacities),
            series: AppendOnlyPages::new(
                "live series locators",
                capacities.series_locs,
                capacities.max_series_loc_pages,
            ),
            key_values: AppendOnlyPages::new(
                "live label key/value pairs",
                capacities.key_values,
                capacities.max_key_value_pages,
            ),
            canonical_series: AppendOnlyPages::new(
                "live canonical series locators",
                capacities.series_locs,
                capacities.max_series_loc_pages,
            ),
            canonical_key_values: AppendOnlyPages::new(
                "live canonical label key/value pairs",
                capacities.key_values,
                capacities.max_key_value_pages,
            ),
            encoded_scratch: Vec::new(),
            canonical_scratch: Vec::new(),
            max_series: capacities.max_series,
            estimated_collision_bytes: 0,
        }
    }

    pub fn symbols(&self) -> &VersionedSymbolTable {
        &self.symbols
    }

    pub fn revision(&self) -> u64 {
        self.series.len()
    }

    pub fn len(&self) -> usize {
        usize::try_from(self.revision()).unwrap_or(usize::MAX)
    }

    pub fn is_empty(&self) -> bool {
        self.revision() == 0
    }

    pub fn intern_iter<'a>(
        &mut self,
        labels: impl ExactSizeIterator<Item = KeyValueRef<'a>>,
    ) -> Result<SeriesRef, VersionedFlatLabelStoreError> {
        self.encoded_scratch.clear();
        self.canonical_scratch.clear();
        let label_count = labels.len();
        self.encoded_scratch.try_reserve(label_count).map_err(|_| {
            VersionedFlatLabelStoreError::AllocationFailed {
                region: "live label encode scratch",
                requested: label_count,
            }
        })?;
        self.canonical_scratch
            .try_reserve(label_count)
            .map_err(|_| VersionedFlatLabelStoreError::AllocationFailed {
                region: "live canonical label encode scratch",
                requested: label_count,
            })?;
        #[cfg(debug_assertions)]
        let mut previous_key = None;

        for label in labels {
            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    previous_key.is_none_or(|key| key < label.key),
                    "LabelSet must be canonical (sorted by key, unique keys)"
                );
                previous_key = Some(label.key);
            }
            if let Err(error) = self.push_raw_and_canonical(label.key, label.value) {
                self.encoded_scratch.clear();
                self.canonical_scratch.clear();
                return Err(error);
            }
        }

        self.canonicalize_scratch();
        self.finish_encoded()
    }

    fn finish_encoded(&mut self) -> Result<SeriesRef, VersionedFlatLabelStoreError> {
        let mut hasher = self.labelset_hash.build_hasher();
        hasher.write_usize(self.encoded_scratch.len());
        for pair in &self.encoded_scratch {
            let packed = (u64::from(pair.key.get()) << 32) | u64::from(pair.value.get());
            hasher.write_u64(packed);
        }
        let hash = hasher.finish();
        if let Some(existing) = self.find_existing(hash) {
            self.encoded_scratch.clear();
            self.canonical_scratch.clear();
            return Ok(existing);
        }

        let result = self.insert_encoded(hash);
        self.encoded_scratch.clear();
        self.canonical_scratch.clear();
        result
    }

    pub fn intern_prepared_otlp(
        &mut self,
        labels: CanonicalLabelSet<'_, '_>,
    ) -> Result<SeriesRef, VersionedFlatLabelStoreError> {
        self.encoded_scratch.clear();
        self.canonical_scratch.clear();
        let label_count = labels.iter().len().max(1);
        self.encoded_scratch.try_reserve(label_count).map_err(|_| {
            VersionedFlatLabelStoreError::AllocationFailed {
                region: "live label encode scratch",
                requested: label_count,
            }
        })?;
        self.canonical_scratch
            .try_reserve(label_count)
            .map_err(|_| VersionedFlatLabelStoreError::AllocationFailed {
                region: "live canonical label encode scratch",
                requested: label_count,
            })?;
        for label in labels.iter() {
            if let Err(error) = self.push_raw_and_canonical(label.key, label.value) {
                self.encoded_scratch.clear();
                self.canonical_scratch.clear();
                return Err(error);
            }
        }

        self.canonicalize_scratch();
        self.finish_encoded()
    }

    /// Stores the exact same storage-normalized identity row as the disabled
    /// FlatInterned path while preparing a separate PromQL projection row.
    ///
    /// PromQL name normalization is intentionally not injective over raw OTLP
    /// strings, so using the projected row as the interning key would merge
    /// distinct source series before kind guards, LWW, and sealing.
    fn push_raw_and_canonical(
        &mut self,
        raw_key: &str,
        raw_value: &str,
    ) -> Result<(), VersionedFlatLabelStoreError> {
        let key = normalize_label_key(raw_key);
        let value = normalize_label_value(raw_value);
        let raw_key_id = self.symbols.intern(key.as_ref())?;
        let raw_value_id = self.symbols.intern(value.as_ref())?;
        self.encoded_scratch.push(InternedKeyValue {
            key: raw_key_id,
            value: raw_value_id,
        });

        let (canonical_key_id, canonical_value_id) =
            if key.as_ref() == crate::labels::METRIC_NAME_LABEL {
                (
                    raw_key_id,
                    self.symbols
                        .intern(&normalize_metric_name(value.as_ref()))?,
                )
            } else {
                (
                    self.symbols.intern(&normalize_label_name(key.as_ref()))?,
                    raw_value_id,
                )
            };
        self.canonical_scratch.push(InternedKeyValue {
            key: canonical_key_id,
            value: canonical_value_id,
        });
        Ok(())
    }

    fn canonicalize_scratch(&mut self) {
        let symbols = &self.symbols;
        self.canonical_scratch
            .sort_by(|left, right| symbols.resolve(left.key).cmp(symbols.resolve(right.key)));
        let mut canonical_len = 0usize;
        for read_index in 0..self.canonical_scratch.len() {
            let pair = self.canonical_scratch[read_index];
            if canonical_len > 0 && self.canonical_scratch[canonical_len - 1].key == pair.key {
                self.canonical_scratch[canonical_len - 1] = pair;
            } else {
                self.canonical_scratch[canonical_len] = pair;
                canonical_len += 1;
            }
        }
        self.canonical_scratch.truncate(canonical_len);
    }

    fn find_existing(&self, hash: u64) -> Option<SeriesRef> {
        let first = self.by_hash.get(&hash).copied()?;
        if self
            .series_slice(first)
            .is_some_and(|stored| stored == self.encoded_scratch.as_slice())
        {
            return Some(first);
        }
        self.by_hash_collisions.get(&hash).and_then(|collisions| {
            collisions.iter().copied().find(|series| {
                self.series_slice(*series)
                    .is_some_and(|stored| stored == self.encoded_scratch.as_slice())
            })
        })
    }

    fn insert_encoded(&mut self, hash: u64) -> Result<SeriesRef, VersionedFlatLabelStoreError> {
        let series_count = self.series.len();
        if series_count >= self.max_series {
            return Err(VersionedFlatLabelStoreError::CapacityExceeded {
                region: "live label rows",
                field: "series_count",
                value: series_count.saturating_add(1),
                maximum: self.max_series,
            });
        }
        let raw_series = u32::try_from(series_count).map_err(|_| {
            VersionedFlatLabelStoreError::CapacityExceeded {
                region: "live label rows",
                field: "series_ref",
                value: series_count,
                maximum: u32::MAX as u64,
            }
        })?;

        let key_value_append = if self.encoded_scratch.is_empty() {
            None
        } else {
            Some(self.key_values.prepare_append(self.encoded_scratch.len())?)
        };
        let canonical_key_value_append = if self.canonical_scratch.is_empty() {
            None
        } else {
            Some(
                self.canonical_key_values
                    .prepare_append(self.canonical_scratch.len())?,
            )
        };
        let series_append = self.series.prepare_append(1)?;
        let canonical_series_append = self.canonical_series.prepare_append(1)?;

        self.by_hash.try_reserve(1).map_err(|_| {
            VersionedFlatLabelStoreError::AllocationFailed {
                region: "live label row hash index",
                requested: 1,
            }
        })?;
        let mut new_collision_list = None;
        if self.by_hash.contains_key(&hash) {
            self.by_hash_collisions.try_reserve(1).map_err(|_| {
                VersionedFlatLabelStoreError::AllocationFailed {
                    region: "live label row collision index",
                    requested: 1,
                }
            })?;
            if let Some(collisions) = self.by_hash_collisions.get_mut(&hash) {
                collisions.try_reserve(1).map_err(|_| {
                    VersionedFlatLabelStoreError::AllocationFailed {
                        region: "live label row collision list",
                        requested: 1,
                    }
                })?;
            } else {
                let mut collisions = Vec::new();
                collisions.try_reserve_exact(1).map_err(|_| {
                    VersionedFlatLabelStoreError::AllocationFailed {
                        region: "live label row collision list",
                        requested: 1,
                    }
                })?;
                new_collision_list = Some(collisions);
            }
        }

        let loc = key_value_append.map_or(PageLoc::EMPTY, |append| {
            self.key_values
                .apply_append(append, self.encoded_scratch.as_slice())
        });
        let canonical_loc = canonical_key_value_append.map_or(PageLoc::EMPTY, |append| {
            self.canonical_key_values
                .apply_append(append, self.canonical_scratch.as_slice())
        });
        self.series.apply_append(series_append, &[loc]);
        self.canonical_series
            .apply_append(canonical_series_append, &[canonical_loc]);
        let series_ref = SeriesRef(raw_series);
        debug_assert_eq!(
            self.canonical_series_slice(series_ref),
            Some(self.canonical_scratch.as_slice())
        );
        match self.by_hash.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(series_ref);
            }
            Entry::Occupied(_) => {
                let collisions = self
                    .by_hash_collisions
                    .entry(hash)
                    .or_insert_with(|| new_collision_list.expect("collision list was prepared"));
                let before = collisions.capacity();
                collisions.push(series_ref);
                self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                    collisions
                        .capacity()
                        .saturating_sub(before)
                        .saturating_mul(std::mem::size_of::<SeriesRef>()),
                );
            }
        }
        Ok(series_ref)
    }

    fn series_slice(&self, series: SeriesRef) -> Option<&[InternedKeyValue]> {
        let loc = self.series.get_dense(u64::from(series.get()))?;
        self.key_values.slice(*loc)
    }

    fn canonical_series_slice(&self, series: SeriesRef) -> Option<&[InternedKeyValue]> {
        let loc = self.canonical_series.get_dense(u64::from(series.get()))?;
        self.canonical_key_values.slice(*loc)
    }

    /// Returns the derived PromQL/storage-normalized row without allocating.
    ///
    /// The mutable writer owns every referenced page, so the returned row is
    /// valid only for this borrow. Immutable query generations use the
    /// corresponding snapshot method.
    pub fn try_canonical_labelset_symbol_ids(
        &self,
        series: SeriesRef,
    ) -> Result<VersionedFlatInternedLabelSetRow<'_>, VersionedFlatLabelStoreError> {
        let labels = self.canonical_series_slice(series).ok_or(
            VersionedFlatLabelStoreError::SeriesRefOutOfRange {
                series_ref: series.get(),
                revision: self.revision(),
            },
        )?;
        Ok(VersionedFlatInternedLabelSetRow { labels })
    }

    pub fn try_visit_labelset(
        &self,
        series: SeriesRef,
        mut visitor: impl FnMut(&str, &str),
    ) -> Result<(), VersionedFlatLabelStoreError> {
        let labels =
            self.series_slice(series)
                .ok_or(VersionedFlatLabelStoreError::SeriesRefOutOfRange {
                    series_ref: series.get(),
                    revision: self.revision(),
                })?;
        for label in labels {
            visitor(
                self.symbols.try_resolve(label.key)?,
                self.symbols.try_resolve(label.value)?,
            );
        }
        Ok(())
    }

    /// Publishes the current exclusive row revision.
    ///
    /// The operation seals only mutable tails. It clones compact page
    /// descriptors into the returned directory, while every label/symbol
    /// payload page remains shared with the writer and older snapshots.
    pub fn snapshot(
        &mut self,
    ) -> Result<VersionedFlatInternedLabelSetSnapshot, VersionedFlatLabelStoreError> {
        let symbols = self.symbols.snapshot()?;
        self.key_values.try_seal_tail()?;
        self.series.try_seal_tail()?;
        self.canonical_key_values.try_seal_tail()?;
        self.canonical_series.try_seal_tail()?;
        if self.series.len() != self.canonical_series.len() {
            return Err(VersionedFlatLabelStoreError::InconsistentRevision {
                raw_revision: self.series.len(),
                canonical_revision: self.canonical_series.len(),
            });
        }
        Ok(VersionedFlatInternedLabelSetSnapshot {
            lineage_id: self.lineage_id,
            revision: self.series.len(),
            symbols,
            series: self.series.snapshot(),
            key_values: self.key_values.snapshot(),
            canonical_series: self.canonical_series.snapshot(),
            canonical_key_values: self.canonical_key_values.snapshot(),
        })
    }

    pub fn memory_stats(&self) -> VersionedFlatLabelStoreMemoryStats {
        self.symbols
            .memory_stats()
            .add(self.series.memory_stats())
            .add(self.key_values.memory_stats())
            .add(self.canonical_series.memory_stats())
            .add(self.canonical_key_values.memory_stats())
    }

    fn estimated_allocated_bytes(&self) -> usize {
        let memory = self.memory_stats();
        std::mem::size_of::<Self>()
            .saturating_add(memory.shared_allocated_bytes)
            .saturating_add(memory.tail_allocated_bytes)
            .saturating_add(estimate_vec_buffer_bytes(&self.encoded_scratch))
            .saturating_add(estimate_vec_buffer_bytes(&self.canonical_scratch))
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash))
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash_collisions))
            .saturating_add(self.estimated_collision_bytes)
            .saturating_add(self.symbols.estimate_index_allocated_bytes())
    }

    fn estimated_used_bytes(&self) -> usize {
        let memory = self.memory_stats();
        let row_hashes = self
            .by_hash
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
            .saturating_add(
                self.by_hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
            )
            .saturating_add(
                self.by_hash_collisions
                    .values()
                    .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
                    .fold(0usize, usize::saturating_add),
            );
        std::mem::size_of::<Self>()
            .saturating_add(memory.shared_used_bytes)
            .saturating_add(memory.tail_used_bytes)
            .saturating_add(
                self.encoded_scratch
                    .len()
                    .saturating_mul(std::mem::size_of::<InternedKeyValue>()),
            )
            .saturating_add(
                self.canonical_scratch
                    .len()
                    .saturating_mul(std::mem::size_of::<InternedKeyValue>()),
            )
            .saturating_add(row_hashes)
            .saturating_add(self.symbols.estimate_index_used_bytes())
    }
}

impl LabelSetStore for VersionedFlatInternedLabelSetStore {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        self.intern_iter(labels.iter().copied()).map_err(Into::into)
    }

    fn len(&self) -> usize {
        VersionedFlatInternedLabelSetStore::len(self)
    }

    fn visit_labelset(&self, series: SeriesRef, visitor: impl FnMut(&str, &str)) {
        self.try_visit_labelset(series, visitor)
            .expect("VersionedFlatInternedLabelSetStore series ref must be valid");
    }

    fn estimate_size_bytes(&self) -> usize {
        self.estimated_allocated_bytes()
    }

    fn estimate_used_bytes(&self) -> usize {
        self.estimated_used_bytes()
    }
}

/// An immutable label revision suitable for `SeriesLabelResolver` through the
/// blanket `LabelSetStore` implementation.
#[derive(Clone)]
pub struct VersionedFlatInternedLabelSetSnapshot {
    lineage_id: u64,
    revision: u64,
    symbols: VersionedSymbolTableSnapshot,
    series: PagedSnapshot<PageLoc>,
    key_values: PagedSnapshot<InternedKeyValue>,
    canonical_series: PagedSnapshot<PageLoc>,
    canonical_key_values: PagedSnapshot<InternedKeyValue>,
}

impl VersionedFlatInternedLabelSetSnapshot {
    /// Process-local identity of the append-only writer that minted this cut.
    ///
    /// Live catalog candidates use this to reject an unrelated snapshot
    /// without comparing every immutable row from an older revision.
    pub(crate) fn lineage_id(&self) -> u64 {
        self.lineage_id
    }

    /// Exclusive dense row count. Revision `N` contains exactly `SeriesRef`s
    /// `0..N`.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn len(&self) -> usize {
        usize::try_from(self.revision).unwrap_or(usize::MAX)
    }

    pub fn is_empty(&self) -> bool {
        self.revision == 0
    }

    pub fn symbols(&self) -> &VersionedSymbolTableSnapshot {
        &self.symbols
    }

    pub fn try_labelset_symbol_ids(
        &self,
        series: SeriesRef,
    ) -> Result<VersionedFlatInternedLabelSetRow<'_>, VersionedFlatLabelStoreError> {
        let loc = self.series.get_dense(u64::from(series.get())).ok_or(
            VersionedFlatLabelStoreError::SeriesRefOutOfRange {
                series_ref: series.get(),
                revision: self.revision,
            },
        )?;
        Ok(VersionedFlatInternedLabelSetRow {
            labels: self.key_values.slice(*loc)?,
        })
    }

    /// Returns the derived PromQL projection for one raw-identity row.
    ///
    /// This row is never used for interning or `SeriesRef` assignment. It is
    /// stored separately because PromQL name normalization may map two
    /// distinct raw OTLP names to the same projected name.
    pub(crate) fn try_canonical_labelset_symbol_ids(
        &self,
        series: SeriesRef,
    ) -> Result<VersionedFlatInternedLabelSetRow<'_>, VersionedFlatLabelStoreError> {
        let loc = self
            .canonical_series
            .get_dense(u64::from(series.get()))
            .ok_or(VersionedFlatLabelStoreError::SeriesRefOutOfRange {
                series_ref: series.get(),
                revision: self.revision,
            })?;
        Ok(VersionedFlatInternedLabelSetRow {
            labels: self.canonical_key_values.slice(*loc)?,
        })
    }

    pub fn try_visit_labelset(
        &self,
        series: SeriesRef,
        mut visitor: impl FnMut(&str, &str),
    ) -> Result<(), VersionedFlatLabelStoreError> {
        for (key, value) in self.try_labelset_symbol_ids(series)?.iter() {
            visitor(
                self.symbols.try_resolve(key)?,
                self.symbols.try_resolve(value)?,
            );
        }
        Ok(())
    }

    pub fn memory_stats(&self) -> VersionedFlatLabelStoreMemoryStats {
        self.symbols
            .memory_stats()
            .add(self.series.memory_stats())
            .add(self.key_values.memory_stats())
            .add(self.canonical_series.memory_stats())
            .add(self.canonical_key_values.memory_stats())
    }
}

impl LabelSetStore for VersionedFlatInternedLabelSetSnapshot {
    fn intern(&mut self, _labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        Err(LabelSetStoreError::SealedStore)
    }

    fn len(&self) -> usize {
        VersionedFlatInternedLabelSetSnapshot::len(self)
    }

    fn visit_labelset(&self, series: SeriesRef, visitor: impl FnMut(&str, &str)) {
        self.try_visit_labelset(series, visitor)
            .expect("VersionedFlatInternedLabelSetSnapshot series ref must be valid");
    }

    fn estimate_size_bytes(&self) -> usize {
        let memory = self.memory_stats();
        std::mem::size_of::<Self>()
            .saturating_add(memory.shared_allocated_bytes)
            .saturating_add(
                self.symbols
                    .bytes
                    .pages
                    .len()
                    .saturating_mul(std::mem::size_of::<SharedPage<u8>>()),
            )
            .saturating_add(
                self.symbols
                    .locs
                    .pages
                    .len()
                    .saturating_mul(std::mem::size_of::<SharedPage<PageLoc>>()),
            )
            .saturating_add(
                self.series
                    .pages
                    .len()
                    .saturating_mul(std::mem::size_of::<SharedPage<PageLoc>>()),
            )
            .saturating_add(
                self.key_values
                    .pages
                    .len()
                    .saturating_mul(std::mem::size_of::<SharedPage<InternedKeyValue>>()),
            )
            .saturating_add(
                self.canonical_series
                    .pages
                    .len()
                    .saturating_mul(std::mem::size_of::<SharedPage<PageLoc>>()),
            )
            .saturating_add(
                self.canonical_key_values
                    .pages
                    .len()
                    .saturating_mul(std::mem::size_of::<SharedPage<InternedKeyValue>>()),
            )
    }

    fn estimate_used_bytes(&self) -> usize {
        let memory = self.memory_stats();
        std::mem::size_of::<Self>().saturating_add(memory.shared_used_bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier, RwLock};
    use std::thread;

    use super::*;
    use crate::labels::{DefaultSymbolTable, FlatInternedLabelSetStore, SymbolTable};

    fn labels<'a>(values: &'a [(&'a str, &'a str)]) -> Vec<KeyValueRef<'a>> {
        values.iter().copied().map(KeyValueRef::from).collect()
    }

    fn decode(store: &impl LabelSetStore, series: SeriesRef) -> Vec<(String, String)> {
        let mut decoded = Vec::new();
        store.visit_labelset(series, |key, value| {
            decoded.push((key.to_string(), value.to_string()));
        });
        decoded
    }

    fn decode_canonical(
        snapshot: &VersionedFlatInternedLabelSetSnapshot,
        series: SeriesRef,
    ) -> Vec<(String, String)> {
        snapshot
            .try_canonical_labelset_symbol_ids(series)
            .unwrap()
            .iter()
            .map(|(key, value)| {
                (
                    snapshot.symbols().resolve(key).to_string(),
                    snapshot.symbols().resolve(value).to_string(),
                )
            })
            .collect()
    }

    fn tiny_capacities() -> VersionedPageCapacities {
        VersionedPageCapacities {
            symbol_bytes: 7,
            symbol_locs: 2,
            series_locs: 2,
            key_values: 3,
            ..VersionedPageCapacities::default()
        }
    }

    #[test]
    fn assigns_the_same_dense_ids_as_the_normal_flat_store() {
        let trace = [
            labels(&[]),
            labels(&[("__name__", "requests"), ("pod", "api-0")]),
            labels(&[("__name__", "requests"), ("pod", "api-1")]),
            labels(&[("__name__", "requests"), ("pod", "api-0")]),
            labels(&[("__name__", "latency"), ("namespace", "prod")]),
        ];
        let mut normal = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
        let mut versioned = VersionedFlatInternedLabelSetStore::default();

        for row in trace {
            let expected = normal.intern(&row).unwrap();
            let actual = versioned.intern(&row).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(decode(&versioned, actual), decode(&normal, expected));
        }

        let snapshot = versioned.snapshot().unwrap();
        assert_eq!(snapshot.revision(), normal.len() as u64);
        for raw in 0..normal.len() as u32 {
            let series = SeriesRef::new(raw);
            assert_eq!(decode(&snapshot, series), decode(&normal, series));
        }
    }

    #[test]
    fn promql_projection_collision_preserves_raw_series_identity() {
        let raw_name = "a.label";
        let projected_name = normalize_label_name(raw_name);
        assert_ne!(projected_name, raw_name);
        assert_eq!(normalize_label_name(&projected_name), projected_name);

        let first_pairs = [("__name__", "requests"), (raw_name, "same-value")];
        let second_pairs = [
            ("__name__", "requests"),
            (projected_name.as_str(), "same-value"),
        ];
        let first = labels(&first_pairs);
        let second = labels(&second_pairs);
        let mut normal = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
        let normal_first = normal.intern(&first).unwrap();
        let normal_second = normal.intern(&second).unwrap();
        assert_ne!(normal_first, normal_second);

        let mut versioned = VersionedFlatInternedLabelSetStore::default();
        let live_first = versioned.intern(&first).unwrap();
        let live_second = versioned.intern(&second).unwrap();
        assert_eq!((live_first, live_second), (normal_first, normal_second));
        assert_ne!(live_first, live_second);

        let snapshot = versioned.snapshot().unwrap();
        assert_eq!(decode(&snapshot, live_first), decode(&normal, normal_first));
        assert_eq!(
            decode(&snapshot, live_second),
            decode(&normal, normal_second)
        );
        assert_eq!(
            decode_canonical(&snapshot, live_first),
            decode_canonical(&snapshot, live_second),
            "the derived PromQL facade may collide without collapsing raw identity"
        );
    }

    #[test]
    fn old_snapshot_isolated_while_writer_appends_and_publishes() {
        let mut store = VersionedFlatInternedLabelSetStore::with_capacities(tiny_capacities());
        let first = store
            .intern(&labels(&[("__name__", "requests"), ("pod", "api-0")]))
            .unwrap();
        let old = store.snapshot().unwrap();
        let second = store
            .intern(&labels(&[("__name__", "requests"), ("pod", "api-1")]))
            .unwrap();
        let new = store.snapshot().unwrap();

        assert_eq!(old.revision(), 1);
        assert_eq!(new.revision(), 2);
        assert_eq!(
            decode(&old, first),
            vec![
                ("__name__".into(), "requests".into()),
                ("pod".into(), "api-0".into())
            ]
        );
        assert!(matches!(
            old.try_labelset_symbol_ids(second),
            Err(VersionedFlatLabelStoreError::SeriesRefOutOfRange {
                series_ref: 1,
                revision: 1
            })
        ));
        assert_eq!(
            decode(&new, second),
            vec![
                ("__name__".into(), "requests".into()),
                ("pod".into(), "api-1".into())
            ]
        );
    }

    #[test]
    fn tiny_pages_roll_canonically_and_publish_only_shared_payloads() {
        let mut store = VersionedFlatInternedLabelSetStore::with_capacities(tiny_capacities());
        let first = store
            .intern(&labels(&[("__name__", "m0"), ("a", "v0")]))
            .unwrap();
        let before_publish = store.memory_stats();
        assert!(before_publish.tail_used_bytes > 0);
        assert!(before_publish.non_empty_tails > 0);

        let first_snapshot = store.snapshot().unwrap();
        let after_publish = store.memory_stats();
        assert_eq!(after_publish.tail_used_bytes, 0);
        assert_eq!(after_publish.non_empty_tails, 0);
        assert!(after_publish.shared_pages >= 4);
        assert_eq!(
            first_snapshot.memory_stats().shared_used_bytes,
            after_publish.shared_used_bytes
        );

        let second = store
            .intern(&labels(&[
                ("__name__", "metric-longer-than-page"),
                ("a", "v1"),
            ]))
            .unwrap();
        let second_snapshot = store.snapshot().unwrap();
        assert!(Arc::ptr_eq(
            &first_snapshot.series.pages[0].values,
            &second_snapshot.series.pages[0].values
        ));
        assert!(Arc::ptr_eq(
            &first_snapshot.key_values.pages[0].values,
            &second_snapshot.key_values.pages[0].values
        ));
        assert_eq!(decode(&first_snapshot, first)[0].1, "m0");
        assert_eq!(
            decode(&second_snapshot, second)[0].1,
            "metric-longer-than-page"
        );
        assert!(second_snapshot.memory_stats().shared_pages > after_publish.shared_pages);
        assert!(
            second_snapshot.memory_stats().shared_allocated_bytes
                >= second_snapshot.memory_stats().shared_used_bytes
        );
    }

    #[test]
    fn memory_counters_are_exact_and_small_publications_do_not_reserve_full_pages() {
        let mut store = VersionedFlatInternedLabelSetStore::default();
        store.intern(&labels(&[("__name__", "metric")])).unwrap();

        let mutable = store.memory_stats();
        let expected_used = "__name__".len()
            + "metric".len()
            + 2 * std::mem::size_of::<PageLoc>()
            + 2 * std::mem::size_of::<PageLoc>()
            + 2 * std::mem::size_of::<InternedKeyValue>();
        assert_eq!(mutable.shared_used_bytes, 0);
        assert_eq!(mutable.tail_used_bytes, expected_used);

        let snapshot = store.snapshot().unwrap();
        let published = snapshot.memory_stats();
        assert_eq!(published.shared_used_bytes, expected_used);
        assert_eq!(published.tail_used_bytes, 0);
        assert!(published.shared_allocated_bytes >= published.shared_used_bytes);
        assert!(
            published.shared_allocated_bytes < 4 * 1024,
            "a one-row publication retained {} bytes of page capacity",
            published.shared_allocated_bytes
        );
    }

    #[test]
    fn failed_row_does_not_create_a_series_gap_and_empty_row_is_valid() {
        let mut capacities = tiny_capacities();
        capacities.max_key_value_pages = 1;
        capacities.key_values = 2;
        let mut store = VersionedFlatInternedLabelSetStore::with_capacities(capacities);

        let first = store.intern(&labels(&[("__name__", "first")])).unwrap();
        assert_eq!(first, SeriesRef::new(0));
        let failed = store
            .intern(&labels(&[
                ("__name__", "second"),
                ("a", "two"),
                ("b", "three"),
            ]))
            .unwrap_err();
        assert!(matches!(
            failed,
            LabelSetStoreError::VersionedFlat(error)
                if matches!(
                    error.as_ref(),
                    VersionedFlatLabelStoreError::CapacityExceeded {
                        region: "live label key/value pairs",
                        ..
                    }
                )
        ));
        assert_eq!(store.revision(), 1);

        let empty = store.intern(&[]).unwrap();
        assert_eq!(empty, SeriesRef::new(1));
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.revision(), 2);
        assert!(snapshot.try_labelset_symbol_ids(empty).unwrap().is_empty());
        assert!(matches!(
            snapshot.try_labelset_symbol_ids(SeriesRef::new(2)),
            Err(VersionedFlatLabelStoreError::SeriesRefOutOfRange {
                series_ref: 2,
                revision: 2
            })
        ));
    }

    #[test]
    fn configured_symbol_and_series_limits_fail_before_narrowing_casts() {
        let mut capacities = tiny_capacities();
        capacities.max_symbols = 1;
        let mut symbols = VersionedSymbolTable::with_capacities(capacities);
        assert_eq!(symbols.intern("a").unwrap(), SymbolId(0));
        assert!(matches!(
            symbols.intern("b"),
            Err(VersionedFlatLabelStoreError::CapacityExceeded {
                region: "live symbols",
                field: "symbol_count",
                value: 2,
                maximum: 1
            })
        ));

        let mut capacities = tiny_capacities();
        capacities.max_series = 1;
        let mut store = VersionedFlatInternedLabelSetStore::with_capacities(capacities);
        assert_eq!(store.intern(&[]).unwrap(), SeriesRef::new(0));
        assert!(matches!(
            store.intern(&labels(&[("__name__", "next")])),
            Err(LabelSetStoreError::VersionedFlat(error))
                if matches!(
                    error.as_ref(),
                    VersionedFlatLabelStoreError::CapacityExceeded {
                        region: "live label rows",
                        field: "series_count",
                        value: 2,
                        maximum: 1
                    }
                )
        ));
        assert_eq!(store.revision(), 1);
    }

    #[test]
    fn concurrent_readers_pin_coherent_snapshots_while_writer_advances() {
        const ROUNDS: u32 = 64;
        const READERS: usize = 4;
        let mut store = VersionedFlatInternedLabelSetStore::with_capacities(tiny_capacities());
        let initial = Arc::new(store.snapshot().unwrap());
        let current = Arc::new(RwLock::new(initial));
        let done = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(READERS + 1));

        let handles = (0..READERS)
            .map(|reader| {
                let current = Arc::clone(&current);
                let done = Arc::clone(&done);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    let mut observations = 0usize;
                    while !done.load(Ordering::Acquire) || observations < ROUNDS as usize {
                        let snapshot = Arc::clone(&current.read().unwrap());
                        let revision = snapshot.revision();
                        if revision != 0 {
                            let raw = revision as u32 - 1;
                            let decoded = decode(snapshot.as_ref(), SeriesRef::new(raw));
                            assert_eq!(
                                decoded,
                                vec![("__name__".into(), format!("metric_{raw:03}"))],
                                "reader {reader} observed a torn revision {revision}"
                            );
                            if revision <= u64::from(u32::MAX) {
                                assert!(
                                    snapshot
                                        .try_labelset_symbol_ids(SeriesRef::new(revision as u32))
                                        .is_err()
                                );
                            }
                        }
                        observations += 1;
                        thread::yield_now();
                    }
                    observations
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        let mut oldest = None;
        for raw in 0..ROUNDS {
            let metric = format!("metric_{raw:03}");
            let pairs = [("__name__", metric.as_str())];
            let row = labels(&pairs);
            assert_eq!(store.intern(&row).unwrap(), SeriesRef::new(raw));
            let snapshot = Arc::new(store.snapshot().unwrap());
            oldest.get_or_insert_with(|| Arc::clone(&snapshot));
            *current.write().unwrap() = snapshot;
            thread::yield_now();
        }
        done.store(true, Ordering::Release);

        for handle in handles {
            assert!(handle.join().unwrap() >= ROUNDS as usize);
        }
        let oldest = oldest.unwrap();
        assert_eq!(oldest.revision(), 1);
        assert_eq!(
            decode(oldest.as_ref(), SeriesRef::new(0)),
            vec![("__name__".into(), "metric_000".into())]
        );
    }

    #[test]
    fn snapshots_and_symbol_snapshots_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_series_label_resolver<T: crate::storage::head::SeriesLabelResolver>() {}
        assert_send_sync::<VersionedFlatInternedLabelSetSnapshot>();
        assert_send_sync::<VersionedSymbolTableSnapshot>();
        assert_series_label_resolver::<VersionedFlatInternedLabelSetSnapshot>();
    }

    #[test]
    fn symbol_snapshot_checks_invalid_ids() {
        let mut symbols = VersionedSymbolTable::with_capacities(tiny_capacities());
        let valid = symbols.intern("valid").unwrap();
        let empty = symbols.intern("").unwrap();
        let snapshot = symbols.snapshot().unwrap();
        assert_eq!(snapshot.try_resolve(valid).unwrap(), "valid");
        assert_eq!(snapshot.try_resolve(empty).unwrap(), "");
        assert!(matches!(
            snapshot.try_resolve(SymbolId(2)),
            Err(VersionedFlatLabelStoreError::SymbolIdOutOfRange {
                symbol_id: 2,
                symbol_count: 2
            })
        ));
    }

    #[test]
    fn snapshot_rejects_corrupt_internal_locators_instead_of_panicking() {
        let mut store = VersionedFlatInternedLabelSetStore::with_capacities(tiny_capacities());
        store.intern(&labels(&[("__name__", "metric")])).unwrap();
        let mut snapshot = store.snapshot().unwrap();

        let series_pages = Arc::make_mut(&mut snapshot.series.pages);
        let locs = Arc::make_mut(&mut series_pages[0].values);
        locs[0].page = u32::MAX;

        assert!(matches!(
            snapshot.try_labelset_symbol_ids(SeriesRef::new(0)),
            Err(VersionedFlatLabelStoreError::InvalidLocator {
                region: "live label key/value pairs",
                page: u32::MAX,
                ..
            })
        ));
    }

    #[test]
    fn normal_flat_store_remains_the_default_contiguous_disabled_path() {
        let store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
        assert_eq!(store.key_value_storage_kind(), "contiguous");
        assert_eq!(
            store
                .symbols()
                .stats()
                .to_string()
                .split_whitespace()
                .next(),
            Some("kind=arena")
        );
    }
}
