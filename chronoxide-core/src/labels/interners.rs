use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

use crate::otlp_labelset::CanonicalLabelSet;

use super::normalizer::{normalize_label_key, normalize_label_value};
use super::symbol_table::{DefaultSymbolTable, SymbolTable, SymbolTableError};
use super::{
    KeySetId, KeyValueRef, SeriesRef, SymbolId, U64HashMap, ValueCode, estimate_arc_bytes,
    estimate_hashmap_table_bytes, estimate_vec_buffer_bytes,
};

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum LabelSetStoreError {
    #[error(transparent)]
    SymbolTable(#[from] SymbolTableError),

    #[error("sealed store cannot intern new series")]
    SealedStore,

    #[error("flat interned {layout} locator {field}={value} exceeds representable maximum {max}")]
    LocatorCapacityExceeded {
        layout: &'static str,
        field: &'static str,
        value: usize,
        max: usize,
    },
}

pub trait LabelSetStore {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn visit_labelset(&self, series: SeriesRef, visitor: impl FnMut(&str, &str));

    /// Returns the exact per-key distinct value count if the store can provide it efficiently.
    ///
    /// This is primarily used by report tooling (e.g. per-key cardinality tables). Most store
    /// implementations should keep the default `None`.
    fn key_cardinality(&self, _key: &str) -> Option<usize> {
        None
    }

    /// Best-effort estimate of bytes held by this store (including heap allocations).
    ///
    /// Notes:
    /// - This is an approximation intended for comparing store layouts.
    /// - It does not account for allocator metadata, rounding, or fragmentation.
    fn estimate_size_bytes(&self) -> usize;

    /// Best-effort estimate of bytes used by live elements (more comparable to "payload size" than
    /// to reserved/allocated capacity).
    fn estimate_used_bytes(&self) -> usize;

    /// Alias for `estimate_size_bytes()`.
    fn estimate_size(&self) -> usize {
        self.estimate_size_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct InternedKeyValue {
    pub(crate) key: SymbolId,
    pub(crate) value: SymbolId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedInternedKeyValue {
    cache_id: u64,
    interned: InternedKeyValue,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum FlatInternedLabelSetHash {
    CanonicalStrings,
    InternedIdsSipHash,
    #[default]
    InternedIdsAHash,
}

impl FlatInternedLabelSetHash {
    fn kind(self) -> &'static str {
        match self {
            Self::CanonicalStrings => "canonical_strings",
            Self::InternedIdsSipHash => "interned_ids_siphash",
            Self::InternedIdsAHash => "interned_ids_ahash",
        }
    }
}

static NEXT_PREPARED_CACHE_ID: AtomicU64 = AtomicU64::new(1);

fn next_prepared_cache_id() -> u64 {
    NEXT_PREPARED_CACHE_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnedKeyValue {
    key: String,
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SeriesLoc {
    offset: u32,
    len: u32,
}

const DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY: usize = u16::MAX as usize + 1;
const MAX_INTERNED_KEY_VALUE_PAGES: usize = u16::MAX as usize + 1;

impl SeriesLoc {
    fn paged(
        page_index: usize,
        page_offset: usize,
        len: usize,
    ) -> Result<Self, LabelSetStoreError> {
        let page_index =
            u16::try_from(page_index).map_err(|_| LabelSetStoreError::LocatorCapacityExceeded {
                layout: "paged",
                field: "page_index",
                value: page_index,
                max: u16::MAX as usize,
            })?;
        let page_offset = u16::try_from(page_offset).map_err(|_| {
            LabelSetStoreError::LocatorCapacityExceeded {
                layout: "paged",
                field: "page_offset",
                value: page_offset,
                max: u16::MAX as usize,
            }
        })?;
        let len = u32::try_from(len).map_err(|_| LabelSetStoreError::LocatorCapacityExceeded {
            layout: "paged",
            field: "row_len",
            value: len,
            max: u32::MAX as usize,
        })?;

        Ok(Self {
            offset: (u32::from(page_index) << 16) | u32::from(page_offset),
            len,
        })
    }

    fn contiguous(offset: usize, len: usize) -> Result<Self, LabelSetStoreError> {
        let offset =
            u32::try_from(offset).map_err(|_| LabelSetStoreError::LocatorCapacityExceeded {
                layout: "contiguous",
                field: "offset",
                value: offset,
                max: u32::MAX as usize,
            })?;
        let len = u32::try_from(len).map_err(|_| LabelSetStoreError::LocatorCapacityExceeded {
            layout: "contiguous",
            field: "row_len",
            value: len,
            max: u32::MAX as usize,
        })?;
        Ok(Self { offset, len })
    }

    fn paged_parts(self) -> (usize, usize) {
        (
            (self.offset >> 16) as usize,
            (self.offset & u32::from(u16::MAX)) as usize,
        )
    }
}

struct PagedInternedKeyValues {
    pages: Vec<Vec<InternedKeyValue>>,
    len: usize,
    page_capacity: usize,
}

impl Default for PagedInternedKeyValues {
    fn default() -> Self {
        Self {
            pages: Vec::new(),
            len: 0,
            page_capacity: DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY,
        }
    }
}

impl PagedInternedKeyValues {
    #[cfg(test)]
    fn with_page_capacity(page_capacity: usize) -> Self {
        assert!((1..=DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY).contains(&page_capacity));
        Self {
            pages: Vec::new(),
            len: 0,
            page_capacity,
        }
    }

    fn append_row(&mut self, row: &[InternedKeyValue]) -> Result<SeriesLoc, LabelSetStoreError> {
        let row_len = row.len();
        if row_len > u32::MAX as usize {
            return Err(LabelSetStoreError::LocatorCapacityExceeded {
                layout: "paged",
                field: "row_len",
                value: row_len,
                max: u32::MAX as usize,
            });
        }
        if row_len == 0 {
            return SeriesLoc::paged(0, 0, 0);
        }

        let has_room = self.pages.last().is_some_and(|page| {
            page.len() <= self.page_capacity
                && row_len <= self.page_capacity.saturating_sub(page.len())
        });
        if !has_room {
            if self.pages.len() == MAX_INTERNED_KEY_VALUE_PAGES {
                return Err(LabelSetStoreError::LocatorCapacityExceeded {
                    layout: "paged",
                    field: "page_index",
                    value: self.pages.len(),
                    max: u16::MAX as usize,
                });
            }
            self.pages
                .push(Vec::with_capacity(self.page_capacity.max(row_len)));
        }

        let page_index = self.pages.len() - 1;
        let page = &mut self.pages[page_index];
        let offset = page.len();
        let loc = SeriesLoc::paged(page_index, offset, row_len)?;
        page.extend_from_slice(row);
        self.len = self.len.saturating_add(row_len);
        Ok(loc)
    }

    fn row(&self, loc: SeriesLoc) -> &[InternedKeyValue] {
        if loc.len == 0 {
            return &[];
        }
        let (page_index, start) = loc.paged_parts();
        let page = &self.pages[page_index];
        &page[start..start + loc.len as usize]
    }

    fn capacity(&self) -> usize {
        self.pages.iter().map(Vec::capacity).sum()
    }

    fn allocated_bytes(&self) -> usize {
        estimate_vec_buffer_bytes(&self.pages).saturating_add(
            self.pages
                .iter()
                .map(estimate_vec_buffer_bytes)
                .fold(0usize, usize::saturating_add),
        )
    }
}

enum InternedKeyValueStorage {
    Paged(PagedInternedKeyValues),
    Contiguous(Vec<InternedKeyValue>),
}

impl Default for InternedKeyValueStorage {
    fn default() -> Self {
        Self::Contiguous(Vec::new())
    }
}

impl InternedKeyValueStorage {
    fn append_row(&mut self, row: &[InternedKeyValue]) -> Result<SeriesLoc, LabelSetStoreError> {
        match self {
            Self::Paged(values) => values.append_row(row),
            Self::Contiguous(values) => {
                let offset = values.len();
                let loc = SeriesLoc::contiguous(offset, row.len())?;
                values.extend_from_slice(row);
                Ok(loc)
            }
        }
    }

    fn row(&self, loc: SeriesLoc) -> &[InternedKeyValue] {
        match self {
            Self::Paged(values) => values.row(loc),
            Self::Contiguous(values) => {
                let start = loc.offset as usize;
                &values[start..start + loc.len as usize]
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Paged(values) => values.len,
            Self::Contiguous(values) => values.len(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Paged(values) => values.capacity(),
            Self::Contiguous(values) => values.capacity(),
        }
    }

    fn page_count(&self) -> usize {
        match self {
            Self::Paged(values) => values.pages.len(),
            Self::Contiguous(values) => usize::from(values.capacity() > 0),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Paged(_) => "paged",
            Self::Contiguous(_) => "contiguous",
        }
    }

    fn allocated_bytes(&self) -> usize {
        match self {
            Self::Paged(values) => values.allocated_bytes(),
            Self::Contiguous(values) => estimate_vec_buffer_bytes(values),
        }
    }

    fn used_overhead_bytes(&self) -> usize {
        match self {
            Self::Paged(values) => values
                .pages
                .len()
                .saturating_mul(std::mem::size_of::<Vec<InternedKeyValue>>()),
            Self::Contiguous(_) => 0,
        }
    }
}

#[inline]
fn hash_interned_pair(hasher: &mut impl Hasher, interned: InternedKeyValue) {
    let pair = (u64::from(interned.key.get()) << 32) | u64::from(interned.value.get());
    hasher.write_u64(pair);
}

#[inline]
fn intern_normalized_pair<S: SymbolTable>(
    symbols: &mut S,
    label: KeyValueRef<'_>,
) -> Result<InternedKeyValue, LabelSetStoreError> {
    let key_norm = normalize_label_key(label.key);
    let value_norm = normalize_label_value(label.value);
    let key = symbols.intern(key_norm.as_ref())?;
    let value = symbols.intern(value_norm.as_ref())?;
    Ok(InternedKeyValue { key, value })
}

#[inline]
fn encode_interned_labelset_into<'a, const HASH_INTERNED_IDS: bool, S: SymbolTable, H: Hasher>(
    symbols: &mut S,
    encoded: &mut Vec<InternedKeyValue>,
    labels: impl ExactSizeIterator<Item = KeyValueRef<'a>>,
    mut hasher: H,
) -> Result<u64, LabelSetStoreError> {
    encoded.clear();
    let label_count = labels.len();
    encoded.reserve(label_count);

    if HASH_INTERNED_IDS {
        hasher.write_usize(label_count);
    }
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

        let key_norm = normalize_label_key(label.key);
        let value_norm = normalize_label_value(label.value);
        if !HASH_INTERNED_IDS {
            key_norm.as_ref().hash(&mut hasher);
            value_norm.as_ref().hash(&mut hasher);
        }

        let key = match symbols.intern(key_norm.as_ref()) {
            Ok(key) => key,
            Err(error) => {
                encoded.clear();
                return Err(error.into());
            }
        };
        let value = match symbols.intern(value_norm.as_ref()) {
            Ok(value) => value,
            Err(error) => {
                encoded.clear();
                return Err(error.into());
            }
        };
        let interned = InternedKeyValue { key, value };
        if HASH_INTERNED_IDS {
            hash_interned_pair(&mut hasher, interned);
        }
        encoded.push(interned);
    }

    Ok(hasher.finish())
}

#[inline]
fn encode_prepared_otlp_labelset_into<const HASH_INTERNED_IDS: bool, S: SymbolTable, H: Hasher>(
    symbols: &mut S,
    encoded: &mut Vec<InternedKeyValue>,
    cache_id: u64,
    labels: CanonicalLabelSet<'_, '_>,
    mut hasher: H,
) -> Result<u64, LabelSetStoreError> {
    let Some(prepared) = labels.prepared_parts() else {
        return encode_interned_labelset_into::<HASH_INTERNED_IDS, S, H>(
            symbols,
            encoded,
            labels.iter(),
            hasher,
        );
    };
    encoded.clear();
    let label_count = prepared.iter().len();
    encoded.reserve(label_count);

    if HASH_INTERNED_IDS {
        hasher.write_usize(label_count);
    }
    #[cfg(debug_assertions)]
    let mut previous_key = None;

    for (label, cached_symbols) in prepared.iter() {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                previous_key.is_none_or(|key| key < label.key),
                "LabelSet must be canonical (sorted by key, unique keys)"
            );
            previous_key = Some(label.key);
        }

        let cached = cached_symbols
            .and_then(Cell::get)
            .filter(|cached| cached.cache_id == cache_id);
        if HASH_INTERNED_IDS {
            let interned = if let Some(cached) = cached {
                cached.interned
            } else {
                let interned = match intern_normalized_pair(symbols, label) {
                    Ok(interned) => interned,
                    Err(error) => {
                        encoded.clear();
                        return Err(error);
                    }
                };
                if let Some(cached_symbols) = cached_symbols {
                    cached_symbols.set(Some(PreparedInternedKeyValue { cache_id, interned }));
                }
                interned
            };
            hash_interned_pair(&mut hasher, interned);
            encoded.push(interned);
            continue;
        }

        let key_norm = normalize_label_key(label.key);
        let value_norm = normalize_label_value(label.value);
        key_norm.as_ref().hash(&mut hasher);
        value_norm.as_ref().hash(&mut hasher);

        let interned = if let Some(cached) = cached {
            cached.interned
        } else {
            let key = match symbols.intern(key_norm.as_ref()) {
                Ok(key) => key,
                Err(error) => {
                    encoded.clear();
                    return Err(error.into());
                }
            };
            let value = match symbols.intern(value_norm.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    encoded.clear();
                    return Err(error.into());
                }
            };
            let interned = InternedKeyValue { key, value };
            if let Some(cached_symbols) = cached_symbols {
                cached_symbols.set(Some(PreparedInternedKeyValue { cache_id, interned }));
            }
            interned
        };
        encoded.push(interned);
    }

    Ok(hasher.finish())
}

/// A deliberately naive layout that stores each labelset as its own `Vec<String>`.
///
/// This is used as a baseline to illustrate why a flat/arena-like layout
/// (`FlatInternedLabelSetStore`) is preferable for high-cardinality workloads:
/// millions of small allocations amplify allocator overhead and fragmentation,
/// and each series pays an extra `Vec` header (ptr/len/cap) plus per-string
/// heap allocations.
#[derive(Default)]
pub struct NaiveLabelSetStore {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    series: Vec<Vec<OwnedKeyValue>>,
    estimated_collision_bytes: usize,
    series_vec_alloc_bytes: usize,
    series_vec_used_bytes: usize,
    series_string_alloc_bytes: usize,
    series_string_used_bytes: usize,
}

impl NaiveLabelSetStore {
    pub fn buffer_stats(&self) -> NaiveLabelSetStoreBufferStats {
        NaiveLabelSetStoreBufferStats {
            by_hash_len: self.by_hash.len(),
            by_hash_cap: self.by_hash.capacity(),
            by_hash_collisions_len: self.by_hash_collisions.len(),
            by_hash_collisions_cap: self.by_hash_collisions.capacity(),
            series_len: self.series.len(),
            series_cap: self.series.capacity(),
            series_vec_alloc_bytes: self.series_vec_alloc_bytes,
            series_vec_used_bytes: self.series_vec_used_bytes,
            series_string_alloc_bytes: self.series_string_alloc_bytes,
            series_string_used_bytes: self.series_string_used_bytes,
        }
    }

    fn series_slice(&self, series: SeriesRef) -> &[OwnedKeyValue] {
        &self.series[series.0 as usize]
    }

    fn labels_equal(stored: &[OwnedKeyValue], candidate: &[OwnedKeyValue]) -> bool {
        stored == candidate
    }

    fn encode(&self, labels: &[KeyValueRef<'_>]) -> (Vec<OwnedKeyValue>, u64) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut encoded: Vec<OwnedKeyValue> = Vec::with_capacity(labels.len());
        for label in labels {
            let key_norm = normalize_label_key(label.key);
            let value_norm = normalize_label_value(label.value);
            key_norm.as_ref().hash(&mut hasher);
            value_norm.as_ref().hash(&mut hasher);

            encoded.push(OwnedKeyValue {
                key: key_norm.into_owned(),
                value: value_norm.into_owned(),
            });
        }
        (encoded, hasher.finish())
    }
}

impl LabelSetStore for NaiveLabelSetStore {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        debug_assert!(
            labels.windows(2).all(|pair| pair[0].key < pair[1].key),
            "LabelSet must be canonical (sorted by key, unique keys)"
        );

        let (encoded, labelset_hash) = self.encode(labels);

        if let Some(&candidate_series) = self.by_hash.get(&labelset_hash) {
            if Self::labels_equal(self.series_slice(candidate_series), &encoded) {
                return Ok(candidate_series);
            }

            if let Some(collisions) = self.by_hash_collisions.get(&labelset_hash) {
                for &candidate_series in collisions {
                    if Self::labels_equal(self.series_slice(candidate_series), &encoded) {
                        return Ok(candidate_series);
                    }
                }
            }
        }

        let series_ref = SeriesRef(self.series.len() as u32);

        self.series_vec_alloc_bytes = self.series_vec_alloc_bytes.saturating_add(
            encoded
                .capacity()
                .saturating_mul(std::mem::size_of::<OwnedKeyValue>()),
        );
        self.series_vec_used_bytes = self.series_vec_used_bytes.saturating_add(
            encoded
                .len()
                .saturating_mul(std::mem::size_of::<OwnedKeyValue>()),
        );
        for label in &encoded {
            self.series_string_alloc_bytes = self
                .series_string_alloc_bytes
                .saturating_add(label.key.capacity())
                .saturating_add(label.value.capacity());
            self.series_string_used_bytes = self
                .series_string_used_bytes
                .saturating_add(label.key.len())
                .saturating_add(label.value.len());
        }

        self.series.push(encoded);

        match self.by_hash.entry(labelset_hash) {
            Entry::Vacant(entry) => {
                entry.insert(series_ref);
            }
            Entry::Occupied(_) => {
                let collisions = self.by_hash_collisions.entry(labelset_hash).or_default();
                let before = collisions.capacity();
                collisions.push(series_ref);
                let after = collisions.capacity();
                if after > before {
                    self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                        (after - before).saturating_mul(std::mem::size_of::<SeriesRef>()),
                    );
                }
            }
        }

        Ok(series_ref)
    }

    fn len(&self) -> usize {
        self.series.len()
    }

    fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        let stored = self.series_slice(series);
        for label in stored.iter() {
            visitor(label.key.as_str(), label.value.as_str());
        }
    }

    fn estimate_size_bytes(&self) -> usize {
        let by_hash_bytes = estimate_hashmap_table_bytes(&self.by_hash)
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash_collisions));
        let by_hash_collision_heap_bytes = self.estimated_collision_bytes;
        let series_bytes = estimate_vec_buffer_bytes(&self.series);
        let series_values_bytes = self
            .series_vec_alloc_bytes
            .saturating_add(self.series_string_alloc_bytes);

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(by_hash_collision_heap_bytes)
            .saturating_add(series_bytes)
            .saturating_add(series_values_bytes)
    }

    fn estimate_used_bytes(&self) -> usize {
        let by_hash_bytes = self
            .by_hash
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
            .saturating_add(
                self.by_hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
            );

        let collision_bytes = self
            .by_hash_collisions
            .values()
            .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
            .fold(0usize, usize::saturating_add);

        let series_bytes = self
            .series
            .len()
            .saturating_mul(std::mem::size_of::<Vec<OwnedKeyValue>>());
        let series_values_bytes = self
            .series_vec_used_bytes
            .saturating_add(self.series_string_used_bytes);

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(collision_bytes)
            .saturating_add(series_bytes)
            .saturating_add(series_values_bytes)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NaiveLabelSetStoreBufferStats {
    pub by_hash_len: usize,
    pub by_hash_cap: usize,
    pub by_hash_collisions_len: usize,
    pub by_hash_collisions_cap: usize,
    pub series_len: usize,
    pub series_cap: usize,
    pub series_vec_alloc_bytes: usize,
    pub series_vec_used_bytes: usize,
    pub series_string_alloc_bytes: usize,
    pub series_string_used_bytes: usize,
}

impl std::fmt::Display for NaiveLabelSetStoreBufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type={} by_hash_len={} by_hash_cap={} by_hash_collisions_len={} by_hash_collisions_cap={} series_len={} series_cap={} series_vec_alloc_bytes={} series_vec_used_bytes={} series_string_alloc_bytes={} series_string_used_bytes={}",
            std::any::type_name::<Self>(),
            self.by_hash_len,
            self.by_hash_cap,
            self.by_hash_collisions_len,
            self.by_hash_collisions_cap,
            self.series_len,
            self.series_cap,
            self.series_vec_alloc_bytes,
            self.series_vec_used_bytes,
            self.series_string_alloc_bytes,
            self.series_string_used_bytes,
        )
    }
}

pub struct FlatInternedLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    symbols: S,
    series: Vec<SeriesLoc>,
    key_values: InternedKeyValueStorage,
    labelset_hash: FlatInternedLabelSetHash,
    labelset_ahash: ahash::RandomState,
    encoded_scratch: Vec<InternedKeyValue>,
    fingerprint_calls: u64,
    fingerprint_label_pairs: u64,
    equality_checks: u64,
    equality_matches: u64,
    equality_mismatches: u64,
    collision_inserts: u64,
    estimated_collision_bytes: usize,
    prepared_cache_id: u64,
}

impl<S> Default for FlatInternedLabelSetStore<S>
where
    S: SymbolTable + Default,
{
    fn default() -> Self {
        Self {
            by_hash: U64HashMap::default(),
            by_hash_collisions: U64HashMap::default(),
            symbols: S::default(),
            series: Vec::new(),
            key_values: InternedKeyValueStorage::default(),
            labelset_hash: FlatInternedLabelSetHash::default(),
            labelset_ahash: ahash::RandomState::new(),
            encoded_scratch: Vec::new(),
            fingerprint_calls: 0,
            fingerprint_label_pairs: 0,
            equality_checks: 0,
            equality_matches: 0,
            equality_mismatches: 0,
            collision_inserts: 0,
            estimated_collision_bytes: 0,
            prepared_cache_id: next_prepared_cache_id(),
        }
    }
}

impl<S: SymbolTable> FlatInternedLabelSetStore<S> {
    /// Constructs the default contiguous key/value buffer explicitly.
    pub fn with_contiguous_key_values() -> Self
    where
        S: Default,
    {
        Self {
            key_values: InternedKeyValueStorage::Contiguous(Vec::new()),
            ..Self::default()
        }
    }

    /// Constructs bounded key/value pages for diagnostic A/B comparisons.
    ///
    /// Both layouts preserve the same series assignment and observable
    /// label-set semantics. Real-corpus evidence did not justify paging as the
    /// default because reduced reserved capacity did not reduce peak RSS.
    pub fn with_paged_key_values() -> Self
    where
        S: Default,
    {
        Self {
            key_values: InternedKeyValueStorage::Paged(PagedInternedKeyValues::default()),
            ..Self::default()
        }
    }

    /// Constructs the contiguous store with a keyed AHash fingerprint derived
    /// from already-interned symbol IDs instead of hashing canonical strings
    /// a second time.
    ///
    /// The fingerprint remains only a lookup hint. Every hit still requires
    /// full ordered `(key_id, value_id)` equality before a series is returned.
    pub fn with_interned_id_labelset_hash() -> Self
    where
        S: Default,
    {
        Self {
            key_values: InternedKeyValueStorage::Contiguous(Vec::new()),
            labelset_hash: FlatInternedLabelSetHash::InternedIdsAHash,
            ..Self::default()
        }
    }

    /// Constructs the normal contiguous, AHash-fingerprinted label-set store
    /// with an explicitly selected symbol table.
    ///
    /// This is primarily useful for controlled comparisons of symbol-table
    /// implementations without changing label-set fingerprinting or storage.
    pub fn with_interned_id_labelset_hash_and_symbols(symbols: S) -> Self
    where
        S: Default,
    {
        let mut store = Self::with_interned_id_labelset_hash();
        store.symbols = symbols;
        store
    }

    /// Constructs the contiguous store with the previous SipHash fingerprint
    /// over already-interned symbol IDs for controlled performance comparisons.
    ///
    /// The fingerprint remains only a lookup hint. Every hit still requires
    /// full ordered `(key_id, value_id)` equality before a series is returned.
    pub fn with_interned_id_siphash_labelset_hash() -> Self
    where
        S: Default,
    {
        Self {
            key_values: InternedKeyValueStorage::Contiguous(Vec::new()),
            labelset_hash: FlatInternedLabelSetHash::InternedIdsSipHash,
            ..Self::default()
        }
    }

    /// Constructs the contiguous store with the legacy canonical-string
    /// fingerprint for controlled performance comparisons.
    pub fn with_canonical_string_labelset_hash() -> Self
    where
        S: Default,
    {
        Self {
            key_values: InternedKeyValueStorage::Contiguous(Vec::new()),
            labelset_hash: FlatInternedLabelSetHash::CanonicalStrings,
            ..Self::default()
        }
    }

    #[cfg(test)]
    fn with_key_value_page_capacity(page_capacity: usize) -> Self
    where
        S: Default,
    {
        Self {
            key_values: InternedKeyValueStorage::Paged(PagedInternedKeyValues::with_page_capacity(
                page_capacity,
            )),
            ..Self::default()
        }
    }

    pub fn symbols(&self) -> &S {
        &self.symbols
    }

    pub fn key_value_storage_kind(&self) -> &'static str {
        self.key_values.kind()
    }

    pub fn labelset_hash_kind(&self) -> &'static str {
        self.labelset_hash.kind()
    }

    pub fn visit_labelset_symbol_ids(
        &self,
        series: SeriesRef,
        mut visitor: impl FnMut(SymbolId, SymbolId),
    ) {
        for label in self.series_slice(series) {
            visitor(label.key, label.value);
        }
    }

    pub fn buffer_stats(&self) -> FlatInternedLabelSetStoreBufferStats {
        FlatInternedLabelSetStoreBufferStats {
            by_hash_len: self.by_hash.len(),
            by_hash_cap: self.by_hash.capacity(),
            by_hash_collisions_len: self.by_hash_collisions.len(),
            by_hash_collisions_cap: self.by_hash_collisions.capacity(),
            series_len: self.series.len(),
            series_cap: self.series.capacity(),
            key_values_len: self.key_values.len(),
            key_values_cap: self.key_values.capacity(),
            key_values_pages: self.key_values.page_count(),
            key_values_storage: self.key_values.kind(),
            labelset_hash: self.labelset_hash.kind(),
            encoded_scratch_len: self.encoded_scratch.len(),
            encoded_scratch_cap: self.encoded_scratch.capacity(),
            fingerprint_calls: self.fingerprint_calls,
            fingerprint_label_pairs: self.fingerprint_label_pairs,
            equality_checks: self.equality_checks,
            equality_matches: self.equality_matches,
            equality_mismatches: self.equality_mismatches,
            collision_inserts: self.collision_inserts,
        }
    }

    fn series_slice(&self, series: SeriesRef) -> &[InternedKeyValue] {
        let loc = self.series[series.0 as usize];
        self.key_values.row(loc)
    }

    fn find_existing(&self, labelset_hash: u64) -> (Option<SeriesRef>, u64) {
        let Some(&candidate_series) = self.by_hash.get(&labelset_hash) else {
            return (None, 0);
        };

        let mut equality_checks = 1;
        if self.series_slice(candidate_series) == self.encoded_scratch.as_slice() {
            return (Some(candidate_series), equality_checks);
        }

        if let Some(collisions) = self.by_hash_collisions.get(&labelset_hash) {
            for &candidate_series in collisions {
                equality_checks += 1;
                if self.series_slice(candidate_series) == self.encoded_scratch.as_slice() {
                    return (Some(candidate_series), equality_checks);
                }
            }
        }

        (None, equality_checks)
    }

    fn record_successful_fingerprint(&mut self, label_pairs: u64) {
        self.fingerprint_calls += 1;
        self.fingerprint_label_pairs += label_pairs;
    }

    pub fn intern_iter<'a>(
        &mut self,
        labels: impl ExactSizeIterator<Item = KeyValueRef<'a>>,
    ) -> Result<SeriesRef, LabelSetStoreError> {
        let labelset_hash = match self.labelset_hash {
            FlatInternedLabelSetHash::CanonicalStrings => {
                encode_interned_labelset_into::<false, S, _>(
                    &mut self.symbols,
                    &mut self.encoded_scratch,
                    labels,
                    std::collections::hash_map::DefaultHasher::new(),
                )?
            }
            FlatInternedLabelSetHash::InternedIdsSipHash => {
                encode_interned_labelset_into::<true, S, _>(
                    &mut self.symbols,
                    &mut self.encoded_scratch,
                    labels,
                    std::collections::hash_map::DefaultHasher::new(),
                )?
            }
            FlatInternedLabelSetHash::InternedIdsAHash => {
                encode_interned_labelset_into::<true, S, _>(
                    &mut self.symbols,
                    &mut self.encoded_scratch,
                    labels,
                    self.labelset_ahash.build_hasher(),
                )?
            }
        };
        self.intern_encoded(labelset_hash)
    }

    pub fn intern_prepared_otlp(
        &mut self,
        labels: CanonicalLabelSet<'_, '_>,
    ) -> Result<SeriesRef, LabelSetStoreError> {
        let labelset_hash = match self.labelset_hash {
            FlatInternedLabelSetHash::CanonicalStrings => {
                encode_prepared_otlp_labelset_into::<false, S, _>(
                    &mut self.symbols,
                    &mut self.encoded_scratch,
                    self.prepared_cache_id,
                    labels,
                    std::collections::hash_map::DefaultHasher::new(),
                )?
            }
            FlatInternedLabelSetHash::InternedIdsSipHash => {
                encode_prepared_otlp_labelset_into::<true, S, _>(
                    &mut self.symbols,
                    &mut self.encoded_scratch,
                    self.prepared_cache_id,
                    labels,
                    std::collections::hash_map::DefaultHasher::new(),
                )?
            }
            FlatInternedLabelSetHash::InternedIdsAHash => {
                encode_prepared_otlp_labelset_into::<true, S, _>(
                    &mut self.symbols,
                    &mut self.encoded_scratch,
                    self.prepared_cache_id,
                    labels,
                    self.labelset_ahash.build_hasher(),
                )?
            }
        };
        self.intern_encoded(labelset_hash)
    }

    fn intern_encoded(&mut self, labelset_hash: u64) -> Result<SeriesRef, LabelSetStoreError> {
        let label_pairs = self.encoded_scratch.len() as u64;
        let (existing, equality_checks) = self.find_existing(labelset_hash);
        self.equality_checks += equality_checks;
        if let Some(series) = existing {
            self.equality_matches += 1;
            self.equality_mismatches += equality_checks - 1;
            self.record_successful_fingerprint(label_pairs);
            self.encoded_scratch.clear();
            return Ok(series);
        }
        self.equality_mismatches += equality_checks;

        let series_ref = SeriesRef(self.series.len() as u32);
        let loc = match self.key_values.append_row(&self.encoded_scratch) {
            Ok(loc) => loc,
            Err(error) => {
                self.encoded_scratch.clear();
                return Err(error);
            }
        };
        self.series.push(loc);

        match self.by_hash.entry(labelset_hash) {
            Entry::Vacant(entry) => {
                entry.insert(series_ref);
            }
            Entry::Occupied(_) => {
                self.collision_inserts += 1;
                let collisions = self.by_hash_collisions.entry(labelset_hash).or_default();
                let before = collisions.capacity();
                collisions.push(series_ref);
                let after = collisions.capacity();
                if after > before {
                    self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                        (after - before).saturating_mul(std::mem::size_of::<SeriesRef>()),
                    );
                }
            }
        }
        self.record_successful_fingerprint(label_pairs);
        self.encoded_scratch.clear();
        Ok(series_ref)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FlatInternedLabelSetStoreBufferStats {
    pub by_hash_len: usize,
    pub by_hash_cap: usize,
    pub by_hash_collisions_len: usize,
    pub by_hash_collisions_cap: usize,
    pub series_len: usize,
    pub series_cap: usize,
    pub key_values_len: usize,
    pub key_values_cap: usize,
    pub key_values_pages: usize,
    pub key_values_storage: &'static str,
    pub labelset_hash: &'static str,
    pub encoded_scratch_len: usize,
    pub encoded_scratch_cap: usize,
    pub fingerprint_calls: u64,
    pub fingerprint_label_pairs: u64,
    pub equality_checks: u64,
    pub equality_matches: u64,
    pub equality_mismatches: u64,
    pub collision_inserts: u64,
}

impl std::fmt::Display for FlatInternedLabelSetStoreBufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type={} by_hash_len={} by_hash_cap={} by_hash_collisions_len={} by_hash_collisions_cap={} series_len={} series_cap={} key_values_len={} key_values_cap={} key_values_pages={} key_values_storage={} labelset_hash={} encoded_scratch_len={} encoded_scratch_cap={} fingerprint_calls={} fingerprint_label_pairs={} equality_checks={} equality_matches={} equality_mismatches={} collision_inserts={}",
            std::any::type_name::<Self>(),
            self.by_hash_len,
            self.by_hash_cap,
            self.by_hash_collisions_len,
            self.by_hash_collisions_cap,
            self.series_len,
            self.series_cap,
            self.key_values_len,
            self.key_values_cap,
            self.key_values_pages,
            self.key_values_storage,
            self.labelset_hash,
            self.encoded_scratch_len,
            self.encoded_scratch_cap,
            self.fingerprint_calls,
            self.fingerprint_label_pairs,
            self.equality_checks,
            self.equality_matches,
            self.equality_mismatches,
            self.collision_inserts,
        )
    }
}

impl<S: SymbolTable> LabelSetStore for FlatInternedLabelSetStore<S> {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        self.intern_iter(labels.iter().copied())
    }

    fn len(&self) -> usize {
        self.series.len()
    }

    fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        let stored = self.series_slice(series);
        for label in stored.iter() {
            visitor(
                self.symbols.resolve(label.key),
                self.symbols.resolve(label.value),
            );
        }
    }

    fn estimate_size_bytes(&self) -> usize {
        let by_hash_bytes = estimate_hashmap_table_bytes(&self.by_hash)
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash_collisions));
        let by_hash_collision_heap_bytes = self.estimated_collision_bytes;
        let series_bytes = estimate_vec_buffer_bytes(&self.series);
        let key_values_bytes = self.key_values.allocated_bytes();
        let encoded_scratch_bytes = estimate_vec_buffer_bytes(&self.encoded_scratch);
        let symbols_bytes = self.symbols.estimate_allocated_bytes();

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(by_hash_collision_heap_bytes)
            .saturating_add(series_bytes)
            .saturating_add(key_values_bytes)
            .saturating_add(encoded_scratch_bytes)
            .saturating_add(symbols_bytes)
    }

    fn estimate_used_bytes(&self) -> usize {
        let by_hash_bytes = self
            .by_hash
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
            .saturating_add(
                self.by_hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
            );

        let collision_bytes = self
            .by_hash_collisions
            .values()
            .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
            .fold(0usize, usize::saturating_add);

        let series_bytes = self
            .series
            .len()
            .saturating_mul(std::mem::size_of::<SeriesLoc>());
        let key_values_bytes = self
            .key_values
            .len()
            .saturating_mul(std::mem::size_of::<InternedKeyValue>())
            .saturating_add(self.key_values.used_overhead_bytes());
        let symbols_bytes = self.symbols.estimate_used_bytes();

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(collision_bytes)
            .saturating_add(series_bytes)
            .saturating_add(key_values_bytes)
            .saturating_add(symbols_bytes)
    }
}

#[derive(Clone, Default)]
pub struct KeySetTable {
    keyset_to_id: HashMap<Arc<[SymbolId]>, KeySetId>,
    id_to_keyset: Vec<Arc<[SymbolId]>>,
    estimated_alloc_bytes: usize,
}

impl KeySetTable {
    pub fn len(&self) -> usize {
        self.id_to_keyset.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_keyset.is_empty()
    }

    pub fn intern(&mut self, keys: &[SymbolId]) -> (KeySetId, bool) {
        if let Some(id) = self.keyset_to_id.get(keys) {
            return (*id, false);
        }

        self.estimated_alloc_bytes = self
            .estimated_alloc_bytes
            .saturating_add(estimate_arc_bytes(
                keys.len().saturating_mul(std::mem::size_of::<SymbolId>()),
            ));

        let keyset: Arc<[SymbolId]> = Arc::from(keys.to_vec());
        let id = KeySetId(self.id_to_keyset.len() as u32);
        self.id_to_keyset.push(keyset.clone());
        self.keyset_to_id.insert(keyset, id);
        (id, true)
    }

    pub fn resolve(&self, id: KeySetId) -> &[SymbolId] {
        &self.id_to_keyset[id.0 as usize]
    }

    fn estimated_heap_bytes(&self) -> usize {
        estimate_hashmap_table_bytes(&self.keyset_to_id)
            .saturating_add(estimate_vec_buffer_bytes(&self.id_to_keyset))
            .saturating_add(self.estimated_alloc_bytes)
    }

    fn shrink_to_fit(&mut self) {
        self.keyset_to_id.shrink_to_fit();
        self.id_to_keyset.shrink_to_fit();
    }
}

#[derive(Clone, Default)]
pub struct ValueCodeDict {
    value_to_code: HashMap<SymbolId, ValueCode>,
    code_to_value: Vec<SymbolId>,
}

impl ValueCodeDict {
    pub fn cardinality(&self) -> usize {
        self.code_to_value.len()
    }

    pub fn intern(&mut self, value: SymbolId) -> ValueCode {
        match self.value_to_code.entry(value) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let code = ValueCode(self.code_to_value.len() as u32);
                self.code_to_value.push(value);
                entry.insert(code);
                code
            }
        }
    }

    pub fn resolve(&self, code: ValueCode) -> SymbolId {
        self.code_to_value[code.0 as usize]
    }

    fn shrink_to_fit(&mut self) {
        self.value_to_code.shrink_to_fit();
        self.code_to_value.shrink_to_fit();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SeriesEntry {
    keyset_id: KeySetId,
    row: u32,
}

#[derive(Clone, Default)]
struct KeySetRows {
    key_count: usize,
    values: Vec<ValueCode>,
}

impl KeySetRows {
    fn rows(&self) -> u32 {
        if self.key_count == 0 {
            return 0;
        }
        (self.values.len() / self.key_count) as u32
    }

    fn row_slice(&self, row: u32) -> &[ValueCode] {
        let start = row as usize * self.key_count;
        let end = start + self.key_count;
        &self.values[start..end]
    }
}

#[derive(Default)]
pub struct KeySetDictEncodedLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    symbols: S,
    keysets: KeySetTable,
    value_dicts: HashMap<SymbolId, ValueCodeDict>,
    per_keyset_rows: Vec<KeySetRows>,
    series: Vec<SeriesEntry>,
    estimated_collision_bytes: usize,
}

impl<S: SymbolTable> KeySetDictEncodedLabelSetStore<S> {
    pub fn symbols(&self) -> &S {
        &self.symbols
    }

    pub fn keysets(&self) -> &KeySetTable {
        &self.keysets
    }

    /// Debug helper that renders the internal layered layout of the store as a multi-line string.
    ///
    /// This is intended for small examples and visualization; output is truncated with defaults to
    /// avoid accidentally generating massive strings.
    pub fn dump(&self) -> String {
        const MAX_SYMBOLS: usize = 200;
        const MAX_KEYSETS: usize = 200;
        const MAX_VALUE_DICTS: usize = 200;
        const MAX_VALUES_PER_DICT: usize = 50;
        const MAX_ROWS_PER_KEYSET: u32 = 50;
        const MAX_SERIES: usize = 200;
        const MAX_HASH_ENTRIES: usize = 200;

        use std::fmt::Write as _;

        let mut out = String::new();

        let stats = self.buffer_stats();
        let _ = writeln!(&mut out, "KeySetLabelSetStore");
        let _ = writeln!(
            &mut out,
            "  series={} keysets={} value_dicts={} sum_per_key_cardinality={} symbols={}",
            stats.series_len,
            stats.keysets_len,
            stats.value_dicts_len,
            stats.sum_per_key_cardinality,
            self.symbols.len()
        );
        let _ = writeln!(
            &mut out,
            "  estimate_size_bytes={} estimate_used_bytes={}",
            self.estimate_size_bytes(),
            self.estimate_used_bytes()
        );

        let _ = writeln!(&mut out, "Symbols (first {}):", MAX_SYMBOLS);
        let symbol_len = self.symbols.len().min(MAX_SYMBOLS);
        for i in 0..symbol_len {
            let id = SymbolId(i as u32);
            let _ = writeln!(
                &mut out,
                "  SymbolId({}) {:?}",
                id.get(),
                self.symbols.resolve(id)
            );
        }
        if self.symbols.len() > MAX_SYMBOLS {
            let _ = writeln!(
                &mut out,
                "  ... ({} more)",
                self.symbols.len() - MAX_SYMBOLS
            );
        }

        let _ = writeln!(&mut out, "KeySets (first {}):", MAX_KEYSETS);
        let keyset_len = self.keysets.id_to_keyset.len().min(MAX_KEYSETS);
        for i in 0..keyset_len {
            let id = KeySetId(i as u32);
            let keys = self.keysets.resolve(id);
            let _ = write!(&mut out, "  KeySetId({}): [", id.get());
            for (idx, key) in keys.iter().enumerate() {
                if idx > 0 {
                    let _ = write!(&mut out, ", ");
                }
                let _ = write!(
                    &mut out,
                    "SymbolId({})={:?}",
                    key.get(),
                    self.symbols.resolve(*key)
                );
            }
            let _ = writeln!(&mut out, "]");
        }
        if self.keysets.id_to_keyset.len() > MAX_KEYSETS {
            let _ = writeln!(
                &mut out,
                "  ... ({} more)",
                self.keysets.id_to_keyset.len() - MAX_KEYSETS
            );
        }

        let _ = writeln!(&mut out, "Value Dictionaries (first {}):", MAX_VALUE_DICTS);
        let mut dict_keys: Vec<_> = self.value_dicts.keys().copied().collect();
        dict_keys.sort_by_key(|k| k.get());
        dict_keys.truncate(MAX_VALUE_DICTS);
        for key in dict_keys {
            let dict = self
                .value_dicts
                .get(&key)
                .expect("value dict missing for key");
            let _ = writeln!(
                &mut out,
                "  Key SymbolId({})={:?}: cardinality={}",
                key.get(),
                self.symbols.resolve(key),
                dict.cardinality()
            );

            let values_to_show = dict.cardinality().min(MAX_VALUES_PER_DICT);
            for code in 0..values_to_show {
                let value_sym = dict.resolve(ValueCode(code as u32));
                let _ = writeln!(
                    &mut out,
                    "    ValueCode({}) -> SymbolId({}) {:?}",
                    code,
                    value_sym.get(),
                    self.symbols.resolve(value_sym)
                );
            }
            if dict.cardinality() > MAX_VALUES_PER_DICT {
                let _ = writeln!(
                    &mut out,
                    "    ... ({} more values)",
                    dict.cardinality() - MAX_VALUES_PER_DICT
                );
            }
        }
        if self.value_dicts.len() > MAX_VALUE_DICTS {
            let _ = writeln!(
                &mut out,
                "  ... ({} more dicts)",
                self.value_dicts.len() - MAX_VALUE_DICTS
            );
        }

        let _ = writeln!(&mut out, "Rows per KeySet (first {}):", MAX_KEYSETS);
        let rows_len = self.per_keyset_rows.len().min(MAX_KEYSETS);
        for i in 0..rows_len {
            let keyset_id = KeySetId(i as u32);
            let rows = &self.per_keyset_rows[i];
            let _ = writeln!(
                &mut out,
                "  KeySetId({}): key_count={} rows={}",
                keyset_id.get(),
                rows.key_count,
                rows.rows()
            );
            let keys = self.keysets.resolve(keyset_id);
            let max_rows = rows.rows().min(MAX_ROWS_PER_KEYSET);
            for r in 0..max_rows {
                let codes = rows.row_slice(r);
                let _ = write!(&mut out, "    row {}: ", r);
                for (idx, (key_sym, code)) in keys.iter().zip(codes.iter()).enumerate() {
                    if idx > 0 {
                        let _ = write!(&mut out, ", ");
                    }
                    let dict = self
                        .value_dicts
                        .get(key_sym)
                        .expect("value dict missing for key");
                    let value_sym = dict.resolve(*code);
                    let _ = write!(
                        &mut out,
                        "{:?}={:?}",
                        self.symbols.resolve(*key_sym),
                        self.symbols.resolve(value_sym)
                    );
                }
                let _ = writeln!(&mut out);
            }
            if rows.rows() > MAX_ROWS_PER_KEYSET {
                let _ = writeln!(
                    &mut out,
                    "    ... ({} more rows)",
                    rows.rows() - MAX_ROWS_PER_KEYSET
                );
            }
        }
        if self.per_keyset_rows.len() > MAX_KEYSETS {
            let _ = writeln!(
                &mut out,
                "  ... ({} more keyset rows)",
                self.per_keyset_rows.len() - MAX_KEYSETS
            );
        }

        let _ = writeln!(&mut out, "Series (first {}):", MAX_SERIES);
        let series_len = self.series.len().min(MAX_SERIES);
        for i in 0..series_len {
            let entry = self.series[i];
            let _ = writeln!(
                &mut out,
                "  SeriesRef({}): KeySetId({}) row={}",
                i,
                entry.keyset_id.get(),
                entry.row
            );
        }
        if self.series.len() > MAX_SERIES {
            let _ = writeln!(&mut out, "  ... ({} more)", self.series.len() - MAX_SERIES);
        }

        let _ = writeln!(&mut out, "Hash index (first {}):", MAX_HASH_ENTRIES);
        let mut hashes: Vec<_> = self.by_hash.iter().collect();
        hashes.sort_by_key(|(h, _)| *h);
        hashes.truncate(MAX_HASH_ENTRIES);
        for (hash, series) in hashes {
            let _ = writeln!(&mut out, "  hash={} -> SeriesRef({})", hash, series.get());
        }
        if self.by_hash.len() > MAX_HASH_ENTRIES {
            let _ = writeln!(
                &mut out,
                "  ... ({} more)",
                self.by_hash.len() - MAX_HASH_ENTRIES
            );
        }

        out
    }

    pub fn buffer_stats(&self) -> KeySetLabelSetStoreBufferStats {
        let sum_per_key_cardinality = self
            .value_dicts
            .values()
            .map(|dict| dict.cardinality())
            .fold(0usize, usize::saturating_add);
        let mut global_values = HashSet::new();
        for dict in self.value_dicts.values() {
            for value in &dict.code_to_value {
                global_values.insert(*value);
            }
        }
        let global_distinct_values = global_values.len();

        KeySetLabelSetStoreBufferStats {
            by_hash_len: self.by_hash.len(),
            by_hash_cap: self.by_hash.capacity(),
            by_hash_collisions_len: self.by_hash_collisions.len(),
            by_hash_collisions_cap: self.by_hash_collisions.capacity(),
            series_len: self.series.len(),
            series_cap: self.series.capacity(),
            per_keyset_rows_len: self.per_keyset_rows.len(),
            per_keyset_rows_cap: self.per_keyset_rows.capacity(),
            per_keyset_values_len: self
                .per_keyset_rows
                .iter()
                .map(|r| r.values.len())
                .fold(0usize, usize::saturating_add),
            per_keyset_values_cap: self
                .per_keyset_rows
                .iter()
                .map(|r| r.values.capacity())
                .fold(0usize, usize::saturating_add),
            value_dicts_len: self.value_dicts.len(),
            value_dicts_cap: self.value_dicts.capacity(),
            sum_per_key_cardinality,
            global_distinct_values,
            keysets_len: self.keysets.id_to_keyset.len(),
            keysets_cap: self.keysets.id_to_keyset.capacity(),
            keyset_to_id_len: self.keysets.keyset_to_id.len(),
            keyset_to_id_cap: self.keysets.keyset_to_id.capacity(),
        }
    }

    pub fn seal_fixed_width(&self) -> FixedWidthPackedKeySetLabelSetStore<S>
    where
        S: Clone,
    {
        let mut blocks: Vec<PackedKeySetBlock> = Vec::with_capacity(self.per_keyset_rows.len());
        for rows in &self.per_keyset_rows {
            let key_count = rows.key_count;
            let mut widths: Vec<u8> = Vec::with_capacity(key_count);
            for i in 0..key_count {
                let max_code = rows
                    .values
                    .iter()
                    .skip(i)
                    .step_by(key_count)
                    .map(|v| v.0 as usize)
                    .max()
                    .unwrap_or(0);
                let width = width_for_cardinality(max_code + 1);
                widths.push(width);
            }

            let row_len = widths.iter().map(|&w| w as usize).sum();
            let mut data: Vec<u8> = Vec::with_capacity(rows.values.len() * 4);
            for (offset, code) in rows.values.iter().enumerate() {
                let width = widths[offset % key_count];
                pack_value_code(&mut data, width, *code);
            }
            data.shrink_to_fit();
            blocks.push(PackedKeySetBlock {
                widths: widths.into_boxed_slice(),
                row_len,
                data,
            });
        }
        blocks.shrink_to_fit();

        let mut packed = FixedWidthPackedKeySetLabelSetStore {
            by_hash: self.by_hash.clone(),
            by_hash_collisions: self.by_hash_collisions.clone(),
            symbols: self.symbols.clone(),
            keysets: self.keysets.clone(),
            value_dicts: self.value_dicts.clone(),
            per_keyset_blocks: blocks,
            series: self.series.clone(),
            estimated_collision_bytes: self.estimated_collision_bytes,
        };
        packed.shrink_to_fit();
        packed
    }

    pub fn seal_bit_packed(&self) -> BitPackedKeySetLabelSetStore<S>
    where
        S: Clone,
    {
        let mut blocks: Vec<BitPackedKeySetBlock> = Vec::with_capacity(self.per_keyset_rows.len());
        for rows in &self.per_keyset_rows {
            let key_count = rows.key_count;
            if key_count == 0 {
                blocks.push(BitPackedKeySetBlock {
                    widths_bits: Vec::new().into_boxed_slice(),
                    row_bits: 0,
                    data: Vec::new(),
                });
                continue;
            }
            let mut widths_bits: Vec<u8> = Vec::with_capacity(key_count);
            for i in 0..key_count {
                let max_code = rows
                    .values
                    .iter()
                    .skip(i)
                    .step_by(key_count)
                    .map(|v| v.0)
                    .max()
                    .unwrap_or(0);
                let width = bit_width_for_max_code(max_code);
                widths_bits.push(width);
            }

            let row_bits = widths_bits.iter().map(|&w| w as usize).sum::<usize>();
            let row_count = rows.values.len().saturating_div(key_count.max(1));
            let total_bits = row_bits.saturating_mul(row_count);
            let mut data = vec![0u8; total_bits.div_ceil(8)];
            let mut bit_offset = 0usize;
            for (offset, code) in rows.values.iter().enumerate() {
                let width = widths_bits[offset % key_count];
                pack_bits(&mut data, &mut bit_offset, width, code.0);
            }
            data.shrink_to_fit();
            blocks.push(BitPackedKeySetBlock {
                widths_bits: widths_bits.into_boxed_slice(),
                row_bits,
                data,
            });
        }
        blocks.shrink_to_fit();

        let mut packed = BitPackedKeySetLabelSetStore {
            by_hash: self.by_hash.clone(),
            by_hash_collisions: self.by_hash_collisions.clone(),
            symbols: self.symbols.clone(),
            keysets: self.keysets.clone(),
            value_dicts: self.value_dicts.clone(),
            per_keyset_blocks: blocks,
            series: self.series.clone(),
            estimated_collision_bytes: self.estimated_collision_bytes,
        };
        packed.shrink_to_fit();
        packed
    }

    fn encode(
        &mut self,
        labels: &[KeyValueRef<'_>],
    ) -> Result<(u64, KeySetId, Vec<ValueCode>), LabelSetStoreError> {
        debug_assert!(
            labels.windows(2).all(|pair| pair[0].key < pair[1].key),
            "LabelSet must be canonical (sorted by key, unique keys)"
        );

        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        let mut keys: Vec<SymbolId> = Vec::with_capacity(labels.len());
        let mut codes: Vec<ValueCode> = Vec::with_capacity(labels.len());

        for label in labels {
            let key_norm = normalize_label_key(label.key);
            let value_norm = normalize_label_value(label.value);
            key_norm.as_ref().hash(&mut hasher);
            value_norm.as_ref().hash(&mut hasher);

            let key = self.symbols.intern(key_norm.as_ref())?;
            let value = self.symbols.intern(value_norm.as_ref())?;

            keys.push(key);

            let dict = self.value_dicts.entry(key).or_default();
            let code = dict.intern(value);
            codes.push(code);
        }

        let labelset_hash = hasher.finish();
        let (keyset_id, is_new) = self.keysets.intern(&keys);
        if is_new {
            self.per_keyset_rows.push(KeySetRows {
                key_count: keys.len(),
                values: Vec::new(),
            });
        }

        Ok((labelset_hash, keyset_id, codes))
    }

    fn labels_equal(&self, stored: SeriesEntry, keyset_id: KeySetId, codes: &[ValueCode]) -> bool {
        if stored.keyset_id != keyset_id {
            return false;
        }
        let stored_rows = &self.per_keyset_rows[stored.keyset_id.0 as usize];
        stored_rows.row_slice(stored.row) == codes
    }
}

#[derive(Clone, Copy, Debug)]
pub struct KeySetLabelSetStoreBufferStats {
    pub by_hash_len: usize,
    pub by_hash_cap: usize,
    pub by_hash_collisions_len: usize,
    pub by_hash_collisions_cap: usize,
    pub series_len: usize,
    pub series_cap: usize,
    pub per_keyset_rows_len: usize,
    pub per_keyset_rows_cap: usize,
    pub per_keyset_values_len: usize,
    pub per_keyset_values_cap: usize,
    pub value_dicts_len: usize,
    pub value_dicts_cap: usize,
    pub sum_per_key_cardinality: usize,
    pub global_distinct_values: usize,
    pub keysets_len: usize,
    pub keysets_cap: usize,
    pub keyset_to_id_len: usize,
    pub keyset_to_id_cap: usize,
}

impl std::fmt::Display for KeySetLabelSetStoreBufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type={} by_hash_len={} by_hash_cap={} by_hash_collisions_len={} by_hash_collisions_cap={} series_len={} series_cap={} per_keyset_rows_len={} per_keyset_rows_cap={} per_keyset_values_len={} per_keyset_values_cap={} value_dicts_len={} value_dicts_cap={} sum_per_key_cardinality={} global_distinct_values={} keysets_len={} keysets_cap={} keyset_to_id_len={} keyset_to_id_cap={}",
            std::any::type_name::<Self>(),
            self.by_hash_len,
            self.by_hash_cap,
            self.by_hash_collisions_len,
            self.by_hash_collisions_cap,
            self.series_len,
            self.series_cap,
            self.per_keyset_rows_len,
            self.per_keyset_rows_cap,
            self.per_keyset_values_len,
            self.per_keyset_values_cap,
            self.value_dicts_len,
            self.value_dicts_cap,
            self.sum_per_key_cardinality,
            self.global_distinct_values,
            self.keysets_len,
            self.keysets_cap,
            self.keyset_to_id_len,
            self.keyset_to_id_cap,
        )
    }
}

impl<S: SymbolTable> LabelSetStore for KeySetDictEncodedLabelSetStore<S> {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        let (labelset_hash, keyset_id, codes) = self.encode(labels)?;

        if let Some(&candidate_series) = self.by_hash.get(&labelset_hash) {
            if self.labels_equal(self.series[candidate_series.0 as usize], keyset_id, &codes) {
                return Ok(candidate_series);
            }

            if let Some(collisions) = self.by_hash_collisions.get(&labelset_hash) {
                for &candidate_series in collisions {
                    if self.labels_equal(
                        self.series[candidate_series.0 as usize],
                        keyset_id,
                        &codes,
                    ) {
                        return Ok(candidate_series);
                    }
                }
            }
        }

        let row = self.per_keyset_rows[keyset_id.0 as usize].rows();
        self.per_keyset_rows[keyset_id.0 as usize]
            .values
            .extend_from_slice(&codes);

        let series_ref = SeriesRef(self.series.len() as u32);
        self.series.push(SeriesEntry { keyset_id, row });

        match self.by_hash.entry(labelset_hash) {
            Entry::Vacant(entry) => {
                entry.insert(series_ref);
            }
            Entry::Occupied(_) => {
                let collisions = self.by_hash_collisions.entry(labelset_hash).or_default();
                let before = collisions.capacity();
                collisions.push(series_ref);
                let after = collisions.capacity();
                if after > before {
                    self.estimated_collision_bytes = self.estimated_collision_bytes.saturating_add(
                        (after - before).saturating_mul(std::mem::size_of::<SeriesRef>()),
                    );
                }
            }
        }

        Ok(series_ref)
    }

    fn len(&self) -> usize {
        self.series.len()
    }

    fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        let entry = self.series[series.0 as usize];
        let keys = self.keysets.resolve(entry.keyset_id);
        let rows = &self.per_keyset_rows[entry.keyset_id.0 as usize];
        let values = rows.row_slice(entry.row);
        for (&key, &code) in keys.iter().zip(values.iter()) {
            let dict = self
                .value_dicts
                .get(&key)
                .expect("value dict missing for key");
            let value = dict.resolve(code);
            visitor(self.symbols.resolve(key), self.symbols.resolve(value));
        }
    }

    fn key_cardinality(&self, key: &str) -> Option<usize> {
        let key = self.symbols.lookup(key)?;
        let dict = self.value_dicts.get(&key)?;
        Some(dict.cardinality())
    }

    fn estimate_size_bytes(&self) -> usize {
        let symbols_bytes = self.symbols.estimate_allocated_bytes();

        let keysets_bytes = self.keysets.estimated_heap_bytes();

        let value_dicts_bytes = estimate_hashmap_table_bytes(&self.value_dicts);
        let value_dicts_heap_bytes = self
            .value_dicts
            .values()
            .map(|dict| {
                estimate_hashmap_table_bytes(&dict.value_to_code)
                    .saturating_add(estimate_vec_buffer_bytes(&dict.code_to_value))
            })
            .fold(0usize, usize::saturating_add);

        let per_keyset_rows_bytes = estimate_vec_buffer_bytes(&self.per_keyset_rows);
        let per_keyset_rows_heap_bytes = self
            .per_keyset_rows
            .iter()
            .map(|rows| estimate_vec_buffer_bytes(&rows.values))
            .fold(0usize, usize::saturating_add);

        let series_bytes = estimate_vec_buffer_bytes(&self.series);

        let by_hash_bytes = estimate_hashmap_table_bytes(&self.by_hash)
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash_collisions));
        let by_hash_collision_heap_bytes = self.estimated_collision_bytes;

        std::mem::size_of::<Self>()
            .saturating_add(symbols_bytes)
            .saturating_add(keysets_bytes)
            .saturating_add(value_dicts_bytes)
            .saturating_add(value_dicts_heap_bytes)
            .saturating_add(per_keyset_rows_bytes)
            .saturating_add(per_keyset_rows_heap_bytes)
            .saturating_add(series_bytes)
            .saturating_add(by_hash_bytes)
            .saturating_add(by_hash_collision_heap_bytes)
    }

    fn estimate_used_bytes(&self) -> usize {
        let symbols_bytes = self.symbols.estimate_used_bytes();

        let keysets_bytes = self
            .keysets
            .id_to_keyset
            .len()
            .saturating_mul(std::mem::size_of::<Arc<[SymbolId]>>())
            .saturating_add(
                self.keysets
                    .keyset_to_id
                    .len()
                    .saturating_mul(std::mem::size_of::<(Arc<[SymbolId]>, KeySetId)>()),
            )
            .saturating_add(self.keysets.estimated_alloc_bytes);

        let value_dicts_bytes = self
            .value_dicts
            .len()
            .saturating_mul(std::mem::size_of::<(SymbolId, ValueCodeDict)>());

        let value_dicts_used_bytes = self
            .value_dicts
            .values()
            .map(|dict| {
                dict.value_to_code
                    .len()
                    .saturating_mul(std::mem::size_of::<(SymbolId, ValueCode)>())
                    .saturating_add(
                        dict.code_to_value
                            .len()
                            .saturating_mul(std::mem::size_of::<SymbolId>()),
                    )
            })
            .fold(0usize, usize::saturating_add);

        let per_keyset_rows_bytes = self
            .per_keyset_rows
            .len()
            .saturating_mul(std::mem::size_of::<KeySetRows>());
        let per_keyset_rows_used_bytes = self
            .per_keyset_rows
            .iter()
            .map(|rows| {
                rows.values
                    .len()
                    .saturating_mul(std::mem::size_of::<ValueCode>())
            })
            .fold(0usize, usize::saturating_add);

        let series_bytes = self
            .series
            .len()
            .saturating_mul(std::mem::size_of::<SeriesEntry>());

        let by_hash_bytes = self
            .by_hash
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
            .saturating_add(
                self.by_hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
            );

        let collision_bytes = self
            .by_hash_collisions
            .values()
            .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
            .fold(0usize, usize::saturating_add);

        std::mem::size_of::<Self>()
            .saturating_add(symbols_bytes)
            .saturating_add(keysets_bytes)
            .saturating_add(value_dicts_bytes)
            .saturating_add(value_dicts_used_bytes)
            .saturating_add(per_keyset_rows_bytes)
            .saturating_add(per_keyset_rows_used_bytes)
            .saturating_add(series_bytes)
            .saturating_add(by_hash_bytes)
            .saturating_add(collision_bytes)
    }
}

#[derive(Default)]
pub struct FixedWidthPackedKeySetLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    symbols: S,
    keysets: KeySetTable,
    value_dicts: HashMap<SymbolId, ValueCodeDict>,
    per_keyset_blocks: Vec<PackedKeySetBlock>,
    series: Vec<SeriesEntry>,
    estimated_collision_bytes: usize,
}

impl<S: SymbolTable> FixedWidthPackedKeySetLabelSetStore<S> {
    pub fn symbols(&self) -> &S {
        &self.symbols
    }

    pub fn keysets(&self) -> &KeySetTable {
        &self.keysets
    }

    pub fn buffer_stats(&self) -> PackedKeySetLabelSetStoreBufferStats {
        let sum_per_key_cardinality = self
            .value_dicts
            .values()
            .map(|dict| dict.cardinality())
            .fold(0usize, usize::saturating_add);
        let mut global_values = HashSet::new();
        for dict in self.value_dicts.values() {
            for value in &dict.code_to_value {
                global_values.insert(*value);
            }
        }
        let global_distinct_values = global_values.len();

        let packed_values_len = self
            .per_keyset_blocks
            .iter()
            .map(|block| block.data.len())
            .fold(0usize, usize::saturating_add);
        let packed_values_cap = self
            .per_keyset_blocks
            .iter()
            .map(|block| block.data.capacity())
            .fold(0usize, usize::saturating_add);
        let packed_widths_len = self
            .per_keyset_blocks
            .iter()
            .map(|block| block.widths.len())
            .fold(0usize, usize::saturating_add);
        let packed_widths_cap = packed_widths_len;

        PackedKeySetLabelSetStoreBufferStats {
            by_hash_len: self.by_hash.len(),
            by_hash_cap: self.by_hash.capacity(),
            by_hash_collisions_len: self.by_hash_collisions.len(),
            by_hash_collisions_cap: self.by_hash_collisions.capacity(),
            series_len: self.series.len(),
            series_cap: self.series.capacity(),
            per_keyset_blocks_len: self.per_keyset_blocks.len(),
            per_keyset_blocks_cap: self.per_keyset_blocks.capacity(),
            packed_values_len,
            packed_values_cap,
            packed_widths_len,
            packed_widths_cap,
            value_dicts_len: self.value_dicts.len(),
            value_dicts_cap: self.value_dicts.capacity(),
            sum_per_key_cardinality,
            global_distinct_values,
            keysets_len: self.keysets.id_to_keyset.len(),
            keysets_cap: self.keysets.id_to_keyset.capacity(),
            keyset_to_id_len: self.keysets.keyset_to_id.len(),
            keyset_to_id_cap: self.keysets.keyset_to_id.capacity(),
        }
    }

    fn shrink_to_fit(&mut self) {
        self.by_hash.shrink_to_fit();
        self.by_hash_collisions.shrink_to_fit();
        for collisions in self.by_hash_collisions.values_mut() {
            collisions.shrink_to_fit();
        }
        self.keysets.shrink_to_fit();
        self.value_dicts.shrink_to_fit();
        for dict in self.value_dicts.values_mut() {
            dict.shrink_to_fit();
        }
        self.per_keyset_blocks.shrink_to_fit();
        for block in &mut self.per_keyset_blocks {
            block.data.shrink_to_fit();
        }
        self.series.shrink_to_fit();
    }

    fn resolve_row(
        &self,
        keyset_id: KeySetId,
        row: u32,
        mut visitor: impl FnMut(SymbolId, SymbolId),
    ) {
        let keys = self.keysets.resolve(keyset_id);
        let block = &self.per_keyset_blocks[keyset_id.0 as usize];
        let mut offset = row as usize * block.row_len;
        for (&key, &width) in keys.iter().zip(block.widths.iter()) {
            let code = unpack_value_code(&block.data, &mut offset, width);
            let dict = self
                .value_dicts
                .get(&key)
                .expect("value dict missing for key");
            let value = dict.resolve(code);
            visitor(key, value);
        }
    }
}

impl<S: SymbolTable> LabelSetStore for FixedWidthPackedKeySetLabelSetStore<S> {
    fn intern(&mut self, _labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        Err(LabelSetStoreError::SealedStore)
    }

    fn len(&self) -> usize {
        self.series.len()
    }

    fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        let entry = self.series[series.0 as usize];
        self.resolve_row(entry.keyset_id, entry.row, |key, value| {
            visitor(self.symbols.resolve(key), self.symbols.resolve(value));
        });
    }

    fn key_cardinality(&self, key: &str) -> Option<usize> {
        let key = self.symbols.lookup(key)?;
        let dict = self.value_dicts.get(&key)?;
        Some(dict.cardinality())
    }

    fn estimate_size_bytes(&self) -> usize {
        let symbols_bytes = self.symbols.estimate_allocated_bytes();
        let by_hash_bytes = estimate_hashmap_table_bytes(&self.by_hash)
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash_collisions));
        let by_hash_collision_heap_bytes = self.estimated_collision_bytes;

        let keysets_bytes = self.keysets.estimated_heap_bytes();

        let value_dicts_bytes = estimate_hashmap_table_bytes(&self.value_dicts);
        let value_dicts_heap_bytes = self
            .value_dicts
            .values()
            .map(|dict| {
                estimate_hashmap_table_bytes(&dict.value_to_code)
                    .saturating_add(estimate_vec_buffer_bytes(&dict.code_to_value))
            })
            .fold(0usize, usize::saturating_add);

        let per_keyset_blocks_bytes = estimate_vec_buffer_bytes(&self.per_keyset_blocks);
        let per_keyset_blocks_heap_bytes = self
            .per_keyset_blocks
            .iter()
            .map(|block| {
                estimate_vec_buffer_bytes(&block.data)
                    .saturating_add(block.widths.len().saturating_mul(std::mem::size_of::<u8>()))
            })
            .fold(0usize, usize::saturating_add);

        let series_bytes = estimate_vec_buffer_bytes(&self.series);

        std::mem::size_of::<Self>()
            .saturating_add(symbols_bytes)
            .saturating_add(by_hash_bytes)
            .saturating_add(by_hash_collision_heap_bytes)
            .saturating_add(keysets_bytes)
            .saturating_add(value_dicts_bytes)
            .saturating_add(value_dicts_heap_bytes)
            .saturating_add(per_keyset_blocks_bytes)
            .saturating_add(per_keyset_blocks_heap_bytes)
            .saturating_add(series_bytes)
    }

    fn estimate_used_bytes(&self) -> usize {
        let symbols_bytes = self.symbols.estimate_used_bytes();

        let keysets_bytes = self
            .keysets
            .id_to_keyset
            .len()
            .saturating_mul(std::mem::size_of::<Arc<[SymbolId]>>())
            .saturating_add(
                self.keysets
                    .keyset_to_id
                    .len()
                    .saturating_mul(std::mem::size_of::<(Arc<[SymbolId]>, KeySetId)>()),
            )
            .saturating_add(self.keysets.estimated_alloc_bytes);

        let value_dicts_bytes = self
            .value_dicts
            .len()
            .saturating_mul(std::mem::size_of::<(SymbolId, ValueCodeDict)>());

        let value_dicts_used_bytes = self
            .value_dicts
            .values()
            .map(|dict| {
                dict.value_to_code
                    .len()
                    .saturating_mul(std::mem::size_of::<(SymbolId, ValueCode)>())
                    .saturating_add(
                        dict.code_to_value
                            .len()
                            .saturating_mul(std::mem::size_of::<SymbolId>()),
                    )
            })
            .fold(0usize, usize::saturating_add);

        let per_keyset_blocks_bytes = self
            .per_keyset_blocks
            .len()
            .saturating_mul(std::mem::size_of::<PackedKeySetBlock>());
        let per_keyset_blocks_used_bytes = self
            .per_keyset_blocks
            .iter()
            .map(|block| {
                block
                    .widths
                    .len()
                    .saturating_mul(std::mem::size_of::<u8>())
                    .saturating_add(block.data.len())
            })
            .fold(0usize, usize::saturating_add);

        let series_bytes = self
            .series
            .len()
            .saturating_mul(std::mem::size_of::<SeriesEntry>());

        let by_hash_bytes = self
            .by_hash
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
            .saturating_add(
                self.by_hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
            );

        let collision_bytes = self
            .by_hash_collisions
            .values()
            .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
            .fold(0usize, usize::saturating_add);

        std::mem::size_of::<Self>()
            .saturating_add(symbols_bytes)
            .saturating_add(by_hash_bytes)
            .saturating_add(collision_bytes)
            .saturating_add(keysets_bytes)
            .saturating_add(value_dicts_bytes)
            .saturating_add(value_dicts_used_bytes)
            .saturating_add(per_keyset_blocks_bytes)
            .saturating_add(per_keyset_blocks_used_bytes)
            .saturating_add(series_bytes)
    }
}

#[derive(Default)]
pub struct BitPackedKeySetLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    symbols: S,
    keysets: KeySetTable,
    value_dicts: HashMap<SymbolId, ValueCodeDict>,
    per_keyset_blocks: Vec<BitPackedKeySetBlock>,
    series: Vec<SeriesEntry>,
    estimated_collision_bytes: usize,
}

impl<S: SymbolTable> BitPackedKeySetLabelSetStore<S> {
    pub fn symbols(&self) -> &S {
        &self.symbols
    }

    pub fn keysets(&self) -> &KeySetTable {
        &self.keysets
    }

    pub fn buffer_stats(&self) -> PackedKeySetLabelSetStoreBufferStats {
        let sum_per_key_cardinality = self
            .value_dicts
            .values()
            .map(|dict| dict.cardinality())
            .fold(0usize, usize::saturating_add);
        let mut global_values = HashSet::new();
        for dict in self.value_dicts.values() {
            for value in &dict.code_to_value {
                global_values.insert(*value);
            }
        }
        let global_distinct_values = global_values.len();

        let packed_values_len = self
            .per_keyset_blocks
            .iter()
            .map(|block| block.data.len())
            .fold(0usize, usize::saturating_add);
        let packed_values_cap = self
            .per_keyset_blocks
            .iter()
            .map(|block| block.data.capacity())
            .fold(0usize, usize::saturating_add);
        let packed_widths_len = self
            .per_keyset_blocks
            .iter()
            .map(|block| block.widths_bits.len())
            .fold(0usize, usize::saturating_add);
        let packed_widths_cap = packed_widths_len;

        PackedKeySetLabelSetStoreBufferStats {
            by_hash_len: self.by_hash.len(),
            by_hash_cap: self.by_hash.capacity(),
            by_hash_collisions_len: self.by_hash_collisions.len(),
            by_hash_collisions_cap: self.by_hash_collisions.capacity(),
            series_len: self.series.len(),
            series_cap: self.series.capacity(),
            per_keyset_blocks_len: self.per_keyset_blocks.len(),
            per_keyset_blocks_cap: self.per_keyset_blocks.capacity(),
            packed_values_len,
            packed_values_cap,
            packed_widths_len,
            packed_widths_cap,
            value_dicts_len: self.value_dicts.len(),
            value_dicts_cap: self.value_dicts.capacity(),
            sum_per_key_cardinality,
            global_distinct_values,
            keysets_len: self.keysets.id_to_keyset.len(),
            keysets_cap: self.keysets.id_to_keyset.capacity(),
            keyset_to_id_len: self.keysets.keyset_to_id.len(),
            keyset_to_id_cap: self.keysets.keyset_to_id.capacity(),
        }
    }

    fn shrink_to_fit(&mut self) {
        self.by_hash.shrink_to_fit();
        self.by_hash_collisions.shrink_to_fit();
        for collisions in self.by_hash_collisions.values_mut() {
            collisions.shrink_to_fit();
        }
        self.keysets.shrink_to_fit();
        self.value_dicts.shrink_to_fit();
        for dict in self.value_dicts.values_mut() {
            dict.shrink_to_fit();
        }
        self.per_keyset_blocks.shrink_to_fit();
        for block in &mut self.per_keyset_blocks {
            block.data.shrink_to_fit();
        }
        self.series.shrink_to_fit();
    }

    fn resolve_row(
        &self,
        keyset_id: KeySetId,
        row: u32,
        mut visitor: impl FnMut(SymbolId, SymbolId),
    ) {
        let keys = self.keysets.resolve(keyset_id);
        let block = &self.per_keyset_blocks[keyset_id.0 as usize];
        let mut bit_offset = row as usize * block.row_bits;
        for (&key, &width) in keys.iter().zip(block.widths_bits.iter()) {
            let code = unpack_bits(&block.data, &mut bit_offset, width);
            let dict = self
                .value_dicts
                .get(&key)
                .expect("value dict missing for key");
            let value = dict.resolve(ValueCode(code));
            visitor(key, value);
        }
    }
}

impl<S: SymbolTable> LabelSetStore for BitPackedKeySetLabelSetStore<S> {
    fn intern(&mut self, _labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        Err(LabelSetStoreError::SealedStore)
    }

    fn len(&self) -> usize {
        self.series.len()
    }

    fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        let entry = self.series[series.0 as usize];
        self.resolve_row(entry.keyset_id, entry.row, |key, value| {
            visitor(self.symbols.resolve(key), self.symbols.resolve(value));
        });
    }

    fn key_cardinality(&self, key: &str) -> Option<usize> {
        let key = self.symbols.lookup(key)?;
        let dict = self.value_dicts.get(&key)?;
        Some(dict.cardinality())
    }

    fn estimate_size_bytes(&self) -> usize {
        let symbols_bytes = self.symbols.estimate_allocated_bytes();
        let by_hash_bytes = estimate_hashmap_table_bytes(&self.by_hash)
            .saturating_add(estimate_hashmap_table_bytes(&self.by_hash_collisions));
        let by_hash_collision_heap_bytes = self.estimated_collision_bytes;

        let keysets_bytes = self.keysets.estimated_heap_bytes();

        let value_dicts_bytes = estimate_hashmap_table_bytes(&self.value_dicts);
        let value_dicts_heap_bytes = self
            .value_dicts
            .values()
            .map(|dict| {
                estimate_hashmap_table_bytes(&dict.value_to_code)
                    .saturating_add(estimate_vec_buffer_bytes(&dict.code_to_value))
            })
            .fold(0usize, usize::saturating_add);

        let per_keyset_blocks_bytes = estimate_vec_buffer_bytes(&self.per_keyset_blocks);
        let per_keyset_blocks_heap_bytes = self
            .per_keyset_blocks
            .iter()
            .map(|block| {
                estimate_vec_buffer_bytes(&block.data).saturating_add(
                    block
                        .widths_bits
                        .len()
                        .saturating_mul(std::mem::size_of::<u8>()),
                )
            })
            .fold(0usize, usize::saturating_add);

        let series_bytes = estimate_vec_buffer_bytes(&self.series);

        std::mem::size_of::<Self>()
            .saturating_add(symbols_bytes)
            .saturating_add(by_hash_bytes)
            .saturating_add(by_hash_collision_heap_bytes)
            .saturating_add(keysets_bytes)
            .saturating_add(value_dicts_bytes)
            .saturating_add(value_dicts_heap_bytes)
            .saturating_add(per_keyset_blocks_bytes)
            .saturating_add(per_keyset_blocks_heap_bytes)
            .saturating_add(series_bytes)
    }

    fn estimate_used_bytes(&self) -> usize {
        let symbols_bytes = self.symbols.estimate_used_bytes();

        let keysets_bytes = self
            .keysets
            .id_to_keyset
            .len()
            .saturating_mul(std::mem::size_of::<Arc<[SymbolId]>>())
            .saturating_add(
                self.keysets
                    .keyset_to_id
                    .len()
                    .saturating_mul(std::mem::size_of::<(Arc<[SymbolId]>, KeySetId)>()),
            )
            .saturating_add(self.keysets.estimated_alloc_bytes);

        let value_dicts_bytes = self
            .value_dicts
            .len()
            .saturating_mul(std::mem::size_of::<(SymbolId, ValueCodeDict)>());

        let value_dicts_used_bytes = self
            .value_dicts
            .values()
            .map(|dict| {
                dict.value_to_code
                    .len()
                    .saturating_mul(std::mem::size_of::<(SymbolId, ValueCode)>())
                    .saturating_add(
                        dict.code_to_value
                            .len()
                            .saturating_mul(std::mem::size_of::<SymbolId>()),
                    )
            })
            .fold(0usize, usize::saturating_add);

        let per_keyset_blocks_bytes = self
            .per_keyset_blocks
            .len()
            .saturating_mul(std::mem::size_of::<BitPackedKeySetBlock>());
        let per_keyset_blocks_used_bytes = self
            .per_keyset_blocks
            .iter()
            .map(|block| {
                block
                    .widths_bits
                    .len()
                    .saturating_mul(std::mem::size_of::<u8>())
                    .saturating_add(block.data.len())
            })
            .fold(0usize, usize::saturating_add);

        let series_bytes = self
            .series
            .len()
            .saturating_mul(std::mem::size_of::<SeriesEntry>());

        let by_hash_bytes = self
            .by_hash
            .len()
            .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
            .saturating_add(
                self.by_hash_collisions
                    .len()
                    .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
            );

        let collision_bytes = self
            .by_hash_collisions
            .values()
            .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
            .fold(0usize, usize::saturating_add);

        std::mem::size_of::<Self>()
            .saturating_add(symbols_bytes)
            .saturating_add(by_hash_bytes)
            .saturating_add(collision_bytes)
            .saturating_add(keysets_bytes)
            .saturating_add(value_dicts_bytes)
            .saturating_add(value_dicts_used_bytes)
            .saturating_add(per_keyset_blocks_bytes)
            .saturating_add(per_keyset_blocks_used_bytes)
            .saturating_add(series_bytes)
    }
}

struct PackedKeySetBlock {
    widths: Box<[u8]>,
    row_len: usize,
    data: Vec<u8>,
}

struct BitPackedKeySetBlock {
    widths_bits: Box<[u8]>,
    row_bits: usize,
    data: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
pub struct PackedKeySetLabelSetStoreBufferStats {
    pub by_hash_len: usize,
    pub by_hash_cap: usize,
    pub by_hash_collisions_len: usize,
    pub by_hash_collisions_cap: usize,
    pub series_len: usize,
    pub series_cap: usize,
    pub per_keyset_blocks_len: usize,
    pub per_keyset_blocks_cap: usize,
    pub packed_values_len: usize,
    pub packed_values_cap: usize,
    pub packed_widths_len: usize,
    pub packed_widths_cap: usize,
    pub value_dicts_len: usize,
    pub value_dicts_cap: usize,
    pub sum_per_key_cardinality: usize,
    pub global_distinct_values: usize,
    pub keysets_len: usize,
    pub keysets_cap: usize,
    pub keyset_to_id_len: usize,
    pub keyset_to_id_cap: usize,
}

impl std::fmt::Display for PackedKeySetLabelSetStoreBufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type={} by_hash_len={} by_hash_cap={} by_hash_collisions_len={} by_hash_collisions_cap={} series_len={} series_cap={} per_keyset_blocks_len={} per_keyset_blocks_cap={} packed_values_len={} packed_values_cap={} packed_widths_len={} packed_widths_cap={} value_dicts_len={} value_dicts_cap={} sum_per_key_cardinality={} global_distinct_values={} keysets_len={} keysets_cap={} keyset_to_id_len={} keyset_to_id_cap={}",
            std::any::type_name::<Self>(),
            self.by_hash_len,
            self.by_hash_cap,
            self.by_hash_collisions_len,
            self.by_hash_collisions_cap,
            self.series_len,
            self.series_cap,
            self.per_keyset_blocks_len,
            self.per_keyset_blocks_cap,
            self.packed_values_len,
            self.packed_values_cap,
            self.packed_widths_len,
            self.packed_widths_cap,
            self.value_dicts_len,
            self.value_dicts_cap,
            self.sum_per_key_cardinality,
            self.global_distinct_values,
            self.keysets_len,
            self.keysets_cap,
            self.keyset_to_id_len,
            self.keyset_to_id_cap,
        )
    }
}

fn pack_value_code(out: &mut Vec<u8>, width: u8, value: ValueCode) {
    match width {
        0 => {
            assert_eq!(value.0, 0, "0-byte width requires ValueCode(0)");
        }
        1 => out.push(value.0 as u8),
        2 => out.extend_from_slice(&(value.0 as u16).to_le_bytes()),
        4 => out.extend_from_slice(&value.0.to_le_bytes()),
        _ => panic!("invalid width {width}"),
    }
}

fn unpack_value_code(data: &[u8], offset: &mut usize, width: u8) -> ValueCode {
    let code = match width {
        0 => 0,
        1 => {
            let b0 = data[*offset];
            *offset += 1;
            b0 as u32
        }
        2 => {
            let bytes: [u8; 2] = data[*offset..*offset + 2]
                .try_into()
                .expect("invalid packed u16 slice");
            *offset += 2;
            u16::from_le_bytes(bytes) as u32
        }
        4 => {
            let bytes: [u8; 4] = data[*offset..*offset + 4]
                .try_into()
                .expect("invalid packed u32 slice");
            *offset += 4;
            u32::from_le_bytes(bytes)
        }
        _ => panic!("invalid width {width}"),
    };
    ValueCode(code)
}

fn width_for_cardinality(cardinality: usize) -> u8 {
    match cardinality {
        0 | 1 => 0,
        2..=256 => 1,
        257..=65_536 => 2,
        _ => 4,
    }
}

fn bit_width_for_max_code(max_code: u32) -> u8 {
    if max_code == 0 {
        0
    } else {
        (u32::BITS - max_code.leading_zeros()) as u8
    }
}

fn pack_bits(out: &mut [u8], bit_offset: &mut usize, width: u8, value: u32) {
    if width == 0 {
        return;
    }
    let mut remaining = width as usize;
    let mut val = value;
    while remaining > 0 {
        let byte_index = *bit_offset / 8;
        let bit_index = *bit_offset % 8;
        let bits_in_chunk = remaining.min(8 - bit_index);
        let mask = (1u32 << bits_in_chunk) - 1;
        let chunk = (val & mask) as u8;
        out[byte_index] |= chunk << bit_index;
        val >>= bits_in_chunk;
        *bit_offset += bits_in_chunk;
        remaining -= bits_in_chunk;
    }
}

fn unpack_bits(data: &[u8], bit_offset: &mut usize, width: u8) -> u32 {
    if width == 0 {
        return 0;
    }
    let mut remaining = width as usize;
    let mut shift = 0usize;
    let mut value = 0u32;
    while remaining > 0 {
        let byte_index = *bit_offset / 8;
        let bit_index = *bit_offset % 8;
        let bits_in_chunk = remaining.min(8 - bit_index);
        let mask = if bits_in_chunk == 8 {
            u8::MAX
        } else {
            (1u8 << bits_in_chunk) - 1
        };
        let chunk = (data[byte_index] >> bit_index) & mask;
        value |= (chunk as u32) << shift;
        *bit_offset += bits_in_chunk;
        remaining -= bits_in_chunk;
        shift += bits_in_chunk;
    }
    value
}

#[cfg(test)]
fn hash_labelset(labels: &[KeyValueRef<'_>]) -> u64 {
    debug_assert!(
        labels.windows(2).all(|pair| pair[0].key < pair[1].key),
        "LabelSet must be canonical (sorted by key, unique keys)"
    );
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for label in labels {
        let key_norm = normalize_label_key(label.key);
        let value_norm = normalize_label_value(label.value);
        key_norm.as_ref().hash(&mut hasher);
        value_norm.as_ref().hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::labels::{
        ArenaSymbolTableError, MAX_LABEL_NAME_BYTES, MAX_LABEL_VALUE_BYTES, SymbolTableStats,
    };
    use crate::otlp_labelset::{
        CanonicalLabelSet, OtlpLabelSetInterner, PreparedOtlpLabelSetScratch,
        PreparedOtlpResourceLabels, intern_prepared_labelset,
    };
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, KeyValue};

    fn decode(store: &impl LabelSetStore, series: SeriesRef) -> Vec<(String, String)> {
        let mut labels = Vec::new();
        store.visit_labelset(series, |key, value| {
            labels.push((key.to_string(), value.to_string()));
        });
        labels
    }

    fn owned_labels(labels: &[KeyValueRef<'_>]) -> Vec<(String, String)> {
        labels
            .iter()
            .map(|label| (label.key.to_string(), label.value.to_string()))
            .collect()
    }

    fn intern_with_hash(
        store: &mut FlatInternedLabelSetStore,
        labels: &[KeyValueRef<'_>],
        forced_hash: u64,
    ) -> SeriesRef {
        encode_interned_labelset_into::<false, _, _>(
            &mut store.symbols,
            &mut store.encoded_scratch,
            labels.iter().copied(),
            std::collections::hash_map::DefaultHasher::new(),
        )
        .unwrap();
        store.intern_encoded(forced_hash).unwrap()
    }

    #[test]
    fn interned_dedup_interns_same_series() {
        let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();
        let labels = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-123")),
        ];

        let s1 = store.intern(&labels).unwrap();
        let s2 = store.intern(&labels).unwrap();

        assert_eq!(s1, s2);
        assert_eq!(store.len(), 1);
        assert_eq!(
            decode(&store, s1),
            labels
                .iter()
                .map(|l| (l.key.to_string(), l.value.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn interned_repeated_hits_reuse_encoded_scratch_without_growing_persistent_data() {
        let keys = (0..23)
            .map(|index| format!("label_{index:02}"))
            .collect::<Vec<_>>();
        let values = (0..23)
            .map(|index| format!("value_{index:02}"))
            .collect::<Vec<_>>();
        let mut long_labels = vec![KeyValueRef::from(("__name__", "metric"))];
        long_labels.extend(
            keys.iter()
                .zip(&values)
                .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str()))),
        );
        let short_labels = [KeyValueRef::from(("__name__", "other_metric"))];
        let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();

        let long_series = store.intern(&long_labels).unwrap();
        let short_series = store.intern(&short_labels).unwrap();
        let initial = store.buffer_stats();
        let scratch_pointer = store.encoded_scratch.as_ptr();

        assert_eq!(initial.series_len, 2);
        assert_eq!(initial.key_values_len, 25);
        assert_eq!(initial.encoded_scratch_len, 0);
        assert!(initial.encoded_scratch_cap >= long_labels.len());

        for _ in 0..128 {
            assert_eq!(store.intern(&short_labels).unwrap(), short_series);
            assert_eq!(store.intern(&long_labels).unwrap(), long_series);
        }

        let after = store.buffer_stats();
        assert_eq!(after.series_len, initial.series_len);
        assert_eq!(after.series_cap, initial.series_cap);
        assert_eq!(after.key_values_len, initial.key_values_len);
        assert_eq!(after.key_values_cap, initial.key_values_cap);
        assert_eq!(after.encoded_scratch_len, 0);
        assert_eq!(after.encoded_scratch_cap, initial.encoded_scratch_cap);
        assert_eq!(store.encoded_scratch.as_ptr(), scratch_pointer);
        assert_eq!(
            decode(&store, long_series),
            long_labels
                .iter()
                .map(|label| (label.key.to_string(), label.value.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn interned_id_fingerprint_preserves_store_behavior_for_deterministic_trace() {
        let default_store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();
        assert_eq!(default_store.labelset_hash_kind(), "interned_ids_ahash");
        assert_eq!(default_store.key_value_storage_kind(), "contiguous");

        let mut trace: Vec<Vec<(String, String)>> = vec![
            Vec::new(),
            vec![("__name__".into(), "metric".into())],
            vec![("a".into(), "bc".into())],
            vec![("ab".into(), "c".into())],
            vec![("a".into(), "b".into()), ("c".into(), "d".into())],
        ];

        let base = std::iter::once(("__name__".to_string(), "wide_metric".to_string()))
            .chain((0..23).map(|index| (format!("label_{index:02}"), format!("value_{index:02}"))))
            .collect::<Vec<_>>();
        trace.push(base.clone());
        for changed_index in [0, base.len() / 2, base.len() - 1] {
            let mut changed = base.clone();
            changed[changed_index].1.push_str("_changed");
            trace.push(changed);
        }

        let raw_key = format!("{}tail", "é".repeat(MAX_LABEL_NAME_BYTES));
        let raw_value = format!("{}tail", "界".repeat(MAX_LABEL_VALUE_BYTES));
        let normalized_key = normalize_label_key(&raw_key).into_owned();
        let normalized_value = normalize_label_value(&raw_value).into_owned();
        trace.push(vec![
            ("__name__".into(), "normalized".into()),
            (raw_key, raw_value),
        ]);
        trace.push(vec![
            ("__name__".into(), "normalized".into()),
            (normalized_key, normalized_value),
        ]);

        let initial_trace = trace.clone();
        trace.extend(initial_trace.into_iter().rev());

        let mut canonical_strings: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_canonical_string_labelset_hash();
        let mut siphash_ids: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_siphash_labelset_hash();
        let mut ahash_ids_a: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        ahash_ids_a.labelset_ahash = ahash::RandomState::with_seeds(1, 2, 3, 4);
        let mut ahash_ids_b: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        ahash_ids_b.labelset_ahash = ahash::RandomState::with_seeds(5, 6, 7, 8);
        for row in &trace {
            let labels = row
                .iter()
                .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str())))
                .collect::<Vec<_>>();
            let canonical_series = canonical_strings.intern(&labels).unwrap();
            let siphash_series = siphash_ids.intern(&labels).unwrap();
            let ahash_series_a = ahash_ids_a.intern(&labels).unwrap();
            let ahash_series_b = ahash_ids_b.intern(&labels).unwrap();

            let expected = decode(&canonical_strings, canonical_series);
            for (series, store) in [
                (siphash_series, &siphash_ids),
                (ahash_series_a, &ahash_ids_a),
                (ahash_series_b, &ahash_ids_b),
            ] {
                assert_eq!(series, canonical_series);
                assert_eq!(decode(store, series), expected);
            }
        }

        for store in [&siphash_ids, &ahash_ids_a, &ahash_ids_b] {
            assert_eq!(store.len(), canonical_strings.len());
            assert_eq!(store.symbols().len(), canonical_strings.symbols().len());
        }
        assert_eq!(
            canonical_strings.buffer_stats().labelset_hash,
            "canonical_strings"
        );
        assert_eq!(
            siphash_ids.buffer_stats().labelset_hash,
            "interned_ids_siphash"
        );
        for store in [&ahash_ids_a, &ahash_ids_b] {
            assert_eq!(store.buffer_stats().labelset_hash, "interned_ids_ahash");
            assert_eq!(store.buffer_stats().key_values_storage, "contiguous");
        }
        for stats in [
            canonical_strings.buffer_stats(),
            siphash_ids.buffer_stats(),
            ahash_ids_a.buffer_stats(),
            ahash_ids_b.buffer_stats(),
        ] {
            assert_eq!(stats.fingerprint_calls, trace.len() as u64);
            assert_eq!(
                stats.fingerprint_calls,
                stats.series_len as u64 + stats.equality_matches
            );
            assert_eq!(
                stats.equality_checks,
                stats.equality_matches + stats.equality_mismatches
            );
        }
    }

    #[test]
    fn interned_id_fingerprint_matches_canonical_store_for_randomized_trace() {
        let mut state = 0x6a09_e667_f3bc_c909_u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state
        };
        let mut pool = vec![Vec::<(String, String)>::new()];
        for row_index in 0..127 {
            let label_count = 1 + (next() as usize % 12);
            let mut row = Vec::with_capacity(label_count);
            row.push(("__name__".into(), format!("metric_{:03}", next() % 29)));
            for label_index in 1..label_count {
                let value = if row_index % 19 == 0 && label_index == label_count - 1 {
                    format!("{}tail", "界".repeat(MAX_LABEL_VALUE_BYTES))
                } else {
                    format!("value_{:04}", next() % 257)
                };
                row.push((format!("label_{label_index:02}"), value));
            }
            pool.push(row);
        }

        let mut canonical_strings: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_canonical_string_labelset_hash();
        let mut siphash_ids: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_siphash_labelset_hash();
        let mut ahash_ids_a: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        ahash_ids_a.labelset_ahash = ahash::RandomState::with_seeds(11, 12, 13, 14);
        let mut ahash_ids_b: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        ahash_ids_b.labelset_ahash = ahash::RandomState::with_seeds(15, 16, 17, 18);
        for _ in 0..4_096 {
            let row = &pool[next() as usize % pool.len()];
            let labels = row
                .iter()
                .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str())))
                .collect::<Vec<_>>();
            let canonical_series = canonical_strings.intern(&labels).unwrap();
            let expected = decode(&canonical_strings, canonical_series);
            let siphash_series = siphash_ids.intern(&labels).unwrap();
            let ahash_series_a = ahash_ids_a.intern(&labels).unwrap();
            let ahash_series_b = ahash_ids_b.intern(&labels).unwrap();
            for (series, store) in [
                (siphash_series, &siphash_ids),
                (ahash_series_a, &ahash_ids_a),
                (ahash_series_b, &ahash_ids_b),
            ] {
                assert_eq!(series, canonical_series);
                assert_eq!(decode(store, series), expected);
            }
        }

        for store in [&siphash_ids, &ahash_ids_a, &ahash_ids_b] {
            assert_eq!(store.len(), canonical_strings.len());
            assert_eq!(store.symbols().len(), canonical_strings.symbols().len());
        }
    }

    #[test]
    fn paged_interned_rows_do_not_cross_page_boundaries() {
        let mut store: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_key_value_page_capacity(4);
        let first_labels = [
            KeyValueRef::from(("__name__", "first")),
            KeyValueRef::from(("a", "one")),
            KeyValueRef::from(("b", "two")),
        ];
        let second_labels = [
            KeyValueRef::from(("__name__", "second")),
            KeyValueRef::from(("a", "three")),
        ];
        let third_labels = [
            KeyValueRef::from(("__name__", "third")),
            KeyValueRef::from(("a", "four")),
        ];

        let first = store.intern(&first_labels).unwrap();
        let second = store.intern(&second_labels).unwrap();
        let third = store.intern(&third_labels).unwrap();

        assert_eq!(first, SeriesRef::new(0));
        assert_eq!(second, SeriesRef::new(1));
        assert_eq!(third, SeriesRef::new(2));
        assert_eq!(
            store.series,
            [
                SeriesLoc::paged(0, 0, 3).unwrap(),
                SeriesLoc::paged(1, 0, 2).unwrap(),
                SeriesLoc::paged(1, 2, 2).unwrap(),
            ]
        );
        let InternedKeyValueStorage::Paged(values) = &store.key_values else {
            panic!("default test layout must be paged");
        };
        assert_eq!(
            values.pages.iter().map(Vec::len).collect::<Vec<_>>(),
            [3, 4]
        );
        assert_eq!(decode(&store, first), owned_labels(&first_labels));
        assert_eq!(decode(&store, second), owned_labels(&second_labels));
        assert_eq!(decode(&store, third), owned_labels(&third_labels));

        let stats = store.buffer_stats();
        assert_eq!(stats.key_values_storage, "paged");
        assert_eq!(stats.key_values_pages, 2);
        assert_eq!(stats.key_values_len, 7);
        assert!(stats.key_values_cap >= 8);
        assert!(store.estimate_size_bytes() >= store.estimate_used_bytes());
    }

    #[test]
    fn packed_series_location_retains_the_eight_byte_layout_and_full_u16_bounds() {
        assert_eq!(std::mem::size_of::<SeriesLoc>(), 8);

        let loc = SeriesLoc::paged(u16::MAX as usize, u16::MAX as usize, u32::MAX as usize)
            .expect("maximum packed page and offset are representable");
        assert_eq!(loc.offset, u32::MAX);
        assert_eq!(loc.len, u32::MAX);
        assert_eq!(loc.paged_parts(), (u16::MAX as usize, u16::MAX as usize));
        assert_eq!(
            SeriesLoc::paged(MAX_INTERNED_KEY_VALUE_PAGES, 0, 1).unwrap_err(),
            LabelSetStoreError::LocatorCapacityExceeded {
                layout: "paged",
                field: "page_index",
                value: MAX_INTERNED_KEY_VALUE_PAGES,
                max: u16::MAX as usize,
            }
        );
    }

    #[test]
    fn paged_interned_append_uses_the_maximum_packed_offset() {
        let value = InternedKeyValue {
            key: SymbolId(0),
            value: SymbolId(1),
        };
        let mut values = PagedInternedKeyValues::default();
        values
            .pages
            .push(vec![value; DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY - 1]);
        values.len = DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY - 1;

        let loc = values.append_row(&[value]).unwrap();

        assert_eq!(loc.paged_parts(), (0, u16::MAX as usize));
        assert_eq!(values.row(loc), [value]);
        assert_eq!(
            values.pages[0].len(),
            DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY
        );
    }

    #[test]
    fn paged_interned_page_limit_is_non_mutating_and_clears_store_scratch() {
        let value = InternedKeyValue {
            key: SymbolId(0),
            value: SymbolId(1),
        };
        let mut pages = Vec::with_capacity(MAX_INTERNED_KEY_VALUE_PAGES);
        pages.resize_with(MAX_INTERNED_KEY_VALUE_PAGES, Vec::new);
        let mut values = PagedInternedKeyValues {
            pages,
            len: 0,
            page_capacity: DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY,
        };
        let max_page_loc = values.append_row(&[value]).unwrap();
        assert_eq!(max_page_loc.paged_parts(), (u16::MAX as usize, 0));
        values.pages[MAX_INTERNED_KEY_VALUE_PAGES - 1]
            .resize(DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY, value);
        values.len = DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY;
        let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore {
            key_values: InternedKeyValueStorage::Paged(values),
            labelset_hash: FlatInternedLabelSetHash::InternedIdsAHash,
            ..FlatInternedLabelSetStore::default()
        };
        let before = store.buffer_stats();
        let labels = [KeyValueRef::from(("__name__", "does_not_fit"))];

        let error = store.intern(&labels).unwrap_err();

        assert_eq!(
            error,
            LabelSetStoreError::LocatorCapacityExceeded {
                layout: "paged",
                field: "page_index",
                value: MAX_INTERNED_KEY_VALUE_PAGES,
                max: u16::MAX as usize,
            }
        );
        let after = store.buffer_stats();
        assert_eq!(after.series_len, 0);
        assert_eq!(after.key_values_len, before.key_values_len);
        assert_eq!(after.key_values_cap, before.key_values_cap);
        assert_eq!(after.key_values_pages, before.key_values_pages);
        assert_eq!(after.encoded_scratch_len, 0);
        assert!(after.encoded_scratch_cap >= labels.len());
        assert!(store.by_hash.is_empty());
        assert!(store.by_hash_collisions.is_empty());
        assert_eq!(after.fingerprint_calls, 0);
        assert_eq!(after.fingerprint_label_pairs, 0);
        assert_eq!(after.equality_checks, 0);
        assert_eq!(after.equality_matches, 0);
        assert_eq!(after.equality_mismatches, 0);
    }

    #[test]
    fn paged_interned_allocates_an_oversized_row_in_one_page() {
        let mut store: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_key_value_page_capacity(4);
        let oversized = [
            KeyValueRef::from(("__name__", "oversized")),
            KeyValueRef::from(("a", "one")),
            KeyValueRef::from(("b", "two")),
            KeyValueRef::from(("c", "three")),
            KeyValueRef::from(("d", "four")),
        ];
        let short = [KeyValueRef::from(("__name__", "short"))];

        let oversized_ref = store.intern(&oversized).unwrap();
        let short_ref = store.intern(&short).unwrap();

        assert_eq!(
            store.series,
            [
                SeriesLoc::paged(0, 0, 5).unwrap(),
                SeriesLoc::paged(1, 0, 1).unwrap(),
            ]
        );
        assert_eq!(decode(&store, oversized_ref), owned_labels(&oversized));
        assert_eq!(decode(&store, short_ref), owned_labels(&short));
        let stats = store.buffer_stats();
        assert_eq!(stats.key_values_len, 6);
        assert!(stats.key_values_cap >= 9);
        assert_eq!(stats.key_values_pages, 2);
    }

    #[test]
    fn paged_and_contiguous_interning_preserve_collision_and_assignment_semantics() {
        let mut paged: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_key_value_page_capacity(3);
        let mut contiguous = FlatInternedLabelSetStore::with_contiguous_key_values();
        let first = [
            KeyValueRef::from(("__name__", "requests")),
            KeyValueRef::from(("pod", "one")),
        ];
        let second = [
            KeyValueRef::from(("__name__", "requests")),
            KeyValueRef::from(("pod", "two")),
        ];
        let third = [
            KeyValueRef::from(("__name__", "requests")),
            KeyValueRef::from(("namespace", "prod")),
            KeyValueRef::from(("pod", "three")),
        ];
        let empty = [];
        let forced_hash = 7;

        let paged_refs = [
            &first[..],
            &second,
            &third,
            &empty,
            &first,
            &second,
            &third,
            &empty,
        ]
        .map(|labels| intern_with_hash(&mut paged, labels, forced_hash));
        let contiguous_refs = [
            &first[..],
            &second,
            &third,
            &empty,
            &first,
            &second,
            &third,
            &empty,
        ]
        .map(|labels| intern_with_hash(&mut contiguous, labels, forced_hash));

        assert_eq!(
            paged_refs,
            [
                SeriesRef::new(0),
                SeriesRef::new(1),
                SeriesRef::new(2),
                SeriesRef::new(3),
                SeriesRef::new(0),
                SeriesRef::new(1),
                SeriesRef::new(2),
                SeriesRef::new(3),
            ]
        );
        assert_eq!(contiguous_refs, paged_refs);
        assert_eq!(
            paged.by_hash_collisions[&forced_hash],
            [SeriesRef::new(1), SeriesRef::new(2), SeriesRef::new(3)]
        );
        assert_eq!(
            contiguous.by_hash_collisions[&forced_hash],
            paged.by_hash_collisions[&forced_hash]
        );
        for series in paged_refs[..4].iter().copied() {
            assert_eq!(decode(&paged, series), decode(&contiguous, series));
        }
        for stats in [paged.buffer_stats(), contiguous.buffer_stats()] {
            assert_eq!(stats.fingerprint_calls, 8);
            assert_eq!(stats.equality_checks, 16);
            assert_eq!(stats.equality_matches, 4);
            assert_eq!(stats.equality_mismatches, 12);
            assert_eq!(stats.collision_inserts, 3);
        }
        assert_eq!(paged.buffer_stats().key_values_storage, "paged");
        assert_eq!(contiguous.buffer_stats().key_values_storage, "contiguous");
        assert_eq!(
            paged.estimate_used_bytes(),
            contiguous.estimate_used_bytes()
                + paged.buffer_stats().key_values_pages
                    * std::mem::size_of::<Vec<InternedKeyValue>>()
        );
    }

    #[test]
    fn naive_dedup_interns_same_series() {
        let mut store: NaiveLabelSetStore = NaiveLabelSetStore::default();
        let labels = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-123")),
        ];

        let s1 = store.intern(&labels).unwrap();
        let s2 = store.intern(&labels).unwrap();

        assert_eq!(s1, s2);
        assert_eq!(store.len(), 1);
        assert_eq!(
            decode(&store, s1),
            labels
                .iter()
                .map(|l| (l.key.to_string(), l.value.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn flat_interned_is_more_memory_efficient_than_naive() {
        let series_count = 1000usize;

        let mut naive: NaiveLabelSetStore = NaiveLabelSetStore::default();
        let mut flat: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();

        for i in 0..series_count {
            let labels = [
                KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
                KeyValueRef::from(("cluster", "prod")),
                KeyValueRef::from(("container", if i % 2 == 0 { "web" } else { "sidecar" })),
                KeyValueRef::from(("namespace", if i % 3 == 0 { "payments" } else { "search" })),
                KeyValueRef::from(("pod", "backend")),
            ];
            naive.intern(&labels).unwrap();
            flat.intern(&labels).unwrap();
        }

        assert_eq!(naive.len(), flat.len());
        assert!(flat.estimate_used_bytes() < naive.estimate_used_bytes());
    }

    #[test]
    fn keyset_dedup_interns_same_series() {
        let mut store: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
        let labels = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-123")),
        ];

        let s1 = store.intern(&labels).unwrap();
        let s2 = store.intern(&labels).unwrap();

        assert_eq!(s1, s2);
        assert_eq!(store.len(), 1);
        assert_eq!(
            decode(&store, s1),
            labels
                .iter()
                .map(|l| (l.key.to_string(), l.value.to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn keyset_internals_are_layered_correctly() {
        let mut store: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();

        let labels = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-123")),
        ];
        let labels2 = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-1231")),
        ];

        let labels3 = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments2")),
            KeyValueRef::from(("pod", "backend-1231")),
        ];

        let labels4 = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web2")),
            KeyValueRef::from(("namespace", "payments2")),
            KeyValueRef::from(("pod", "backend-1231")),
        ];

        let len1 = labels
            .iter()
            .map(|l| l.key.len() + l.value.len())
            .sum::<usize>();
        let len2 = labels2
            .iter()
            .map(|l| l.key.len() + l.value.len())
            .sum::<usize>();
        let len = len1.saturating_add(len2);
        println!("len = {}", len);

        let s1 = store.intern(&labels).unwrap();
        let s2 = store.intern(&labels2).unwrap();
        let s3 = store.intern(&labels3).unwrap();
        let s4 = store.intern(&labels4).unwrap();

        println!("{}", store.dump());

        assert_eq!(s1, SeriesRef(0));
        assert_eq!(s2, SeriesRef(1));
        assert_eq!(s3, SeriesRef(2));
        assert_eq!(s4, SeriesRef(3));
        assert_ne!(s1, s2);
        assert_eq!(store.len(), 4);

        assert_eq!(
            decode(&store, s1),
            labels
                .iter()
                .map(|l| (l.key.to_string(), l.value.to_string()))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            decode(&store, s2),
            labels2
                .iter()
                .map(|l| (l.key.to_string(), l.value.to_string()))
                .collect::<Vec<_>>()
        );

        assert_eq!(
            decode(&store, s3),
            labels3
                .iter()
                .map(|l| (l.key.to_string(), l.value.to_string()))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            decode(&store, s4),
            labels4
                .iter()
                .map(|l| (l.key.to_string(), l.value.to_string()))
                .collect::<Vec<_>>()
        );

        assert_eq!(store.symbols.len(), 13);

        assert_eq!(store.keysets.id_to_keyset.len(), 1);
        assert_eq!(store.keysets.keyset_to_id.len(), 1);

        let key_name = store
            .symbols
            .lookup("__name__")
            .expect("missing __name__ symbol");
        let key_cluster = store
            .symbols
            .lookup("cluster")
            .expect("missing cluster symbol");
        let key_container = store
            .symbols
            .lookup("container")
            .expect("missing container symbol");
        let key_namespace = store
            .symbols
            .lookup("namespace")
            .expect("missing namespace symbol");
        let key_pod = store.symbols.lookup("pod").expect("missing pod symbol");

        let keys = store.keysets.resolve(KeySetId(0));
        assert_eq!(
            keys,
            &[key_name, key_cluster, key_container, key_namespace, key_pod]
        );

        assert_eq!(store.value_dicts.len(), 5);
        assert_eq!(
            store
                .value_dicts
                .get(&key_name)
                .expect("missing value dict for __name__")
                .cardinality(),
            1
        );
        assert_eq!(
            store
                .value_dicts
                .get(&key_cluster)
                .expect("missing value dict for cluster")
                .cardinality(),
            1
        );
        assert_eq!(
            store
                .value_dicts
                .get(&key_container)
                .expect("missing value dict for container")
                .cardinality(),
            2
        );
        assert_eq!(
            store
                .value_dicts
                .get(&key_namespace)
                .expect("missing value dict for namespace")
                .cardinality(),
            2
        );
        assert_eq!(
            store
                .value_dicts
                .get(&key_pod)
                .expect("missing value dict for pod")
                .cardinality(),
            2
        );

        let pod_dict = store.value_dicts.get(&key_pod).expect("missing pod dict");
        assert_eq!(
            store.symbols.resolve(pod_dict.resolve(ValueCode(0))),
            "backend-123"
        );
        assert_eq!(
            store.symbols.resolve(pod_dict.resolve(ValueCode(1))),
            "backend-1231"
        );

        let rows = &store.per_keyset_rows[0];
        assert_eq!(rows.key_count, 5);
        assert_eq!(rows.values.len(), 20);

        assert_eq!(
            rows.row_slice(0),
            &[
                ValueCode(0),
                ValueCode(0),
                ValueCode(0),
                ValueCode(0),
                ValueCode(0)
            ]
        );
        assert_eq!(
            rows.row_slice(1),
            &[
                ValueCode(0),
                ValueCode(0),
                ValueCode(0),
                ValueCode(0),
                ValueCode(1)
            ]
        );
        assert_eq!(
            rows.row_slice(2),
            &[
                ValueCode(0),
                ValueCode(0),
                ValueCode(0),
                ValueCode(1),
                ValueCode(1)
            ]
        );
        assert_eq!(
            rows.row_slice(3),
            &[
                ValueCode(0),
                ValueCode(0),
                ValueCode(1),
                ValueCode(1),
                ValueCode(1)
            ]
        );

        assert_eq!(store.series.len(), 4);
        assert_eq!(
            store.series[0],
            SeriesEntry {
                keyset_id: KeySetId(0),
                row: 0
            }
        );
        assert_eq!(
            store.series[1],
            SeriesEntry {
                keyset_id: KeySetId(0),
                row: 1
            }
        );
        assert_eq!(
            store.series[2],
            SeriesEntry {
                keyset_id: KeySetId(0),
                row: 2
            }
        );
        assert_eq!(
            store.series[3],
            SeriesEntry {
                keyset_id: KeySetId(0),
                row: 3
            }
        );

        assert_eq!(store.by_hash_collisions.len(), 0);
        assert_eq!(store.by_hash.len(), 4);
        let h1 = hash_labelset(&labels);
        let h2 = hash_labelset(&labels2);
        let h3 = hash_labelset(&labels3);
        let h4 = hash_labelset(&labels4);

        let mut hashes = std::collections::HashSet::new();
        assert!(hashes.insert(h1));
        assert!(hashes.insert(h2));
        assert!(hashes.insert(h3));
        assert!(hashes.insert(h4));
        assert_eq!(store.by_hash.get(&h1).copied(), Some(s1));
        assert_eq!(store.by_hash.get(&h2).copied(), Some(s2));
        assert_eq!(store.by_hash.get(&h3).copied(), Some(s3));
        assert_eq!(store.by_hash.get(&h4).copied(), Some(s4));
    }

    #[test]
    fn keyset_fixed_width_seal_roundtrips() {
        let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
        let labels_a = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-123")),
        ];
        let labels_b = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "sidecar")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-456")),
        ];

        let s1 = builder.intern(&labels_a).unwrap();
        let s2 = builder.intern(&labels_b).unwrap();
        let decoded_builder_s1 = decode(&builder, s1);
        let decoded_builder_s2 = decode(&builder, s2);

        let sealed = builder.seal_fixed_width();
        let decoded_sealed_s1 = decode(&sealed, s1);
        let decoded_sealed_s2 = decode(&sealed, s2);

        assert_eq!(decoded_builder_s1, decoded_sealed_s1);
        assert_eq!(decoded_builder_s2, decoded_sealed_s2);
    }

    #[test]
    fn keyset_bit_packed_seal_roundtrips() {
        let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
        let labels_a = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "web")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-123")),
        ];
        let labels_b = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", "sidecar")),
            KeyValueRef::from(("namespace", "payments")),
            KeyValueRef::from(("pod", "backend-456")),
        ];

        let s1 = builder.intern(&labels_a).unwrap();
        let s2 = builder.intern(&labels_b).unwrap();
        let decoded_builder_s1 = decode(&builder, s1);
        let decoded_builder_s2 = decode(&builder, s2);

        let sealed = builder.seal_bit_packed();
        let decoded_sealed_s1 = decode(&sealed, s1);
        let decoded_sealed_s2 = decode(&sealed, s2);

        assert_eq!(decoded_builder_s1, decoded_sealed_s1);
        assert_eq!(decoded_builder_s2, decoded_sealed_s2);
    }

    #[test]
    fn keyset_bit_packed_handles_large_cardinality() {
        let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
        let pods = (0..300)
            .map(|i| format!("backend-{i:03}"))
            .collect::<Vec<_>>();
        let mut series = Vec::with_capacity(pods.len());

        for pod in &pods {
            let labels = [
                KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
                KeyValueRef::from(("cluster", "prod")),
                KeyValueRef::from(("pod", pod.as_str())),
            ];
            series.push(builder.intern(&labels).unwrap());
        }

        let first = series[0];
        let last = series[series.len() - 1];
        let decoded_first = decode(&builder, first);
        let decoded_last = decode(&builder, last);

        let sealed = builder.seal_bit_packed();
        let decoded_sealed_first = decode(&sealed, first);
        let decoded_sealed_last = decode(&sealed, last);

        assert_eq!(decoded_first, decoded_sealed_first);
        assert_eq!(decoded_last, decoded_sealed_last);
    }

    #[test]
    fn store_intern_applies_normalization() {
        let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();

        let long_value = "a".repeat(crate::labels::MAX_LABEL_VALUE_BYTES + 123);
        let labels = [
            KeyValueRef::from(("__name__", "metric")),
            KeyValueRef::from(("foo", long_value.as_str())),
        ];

        let series = store.intern(&labels).unwrap();
        let decoded = decode(&store, series);
        let foo_value = decoded
            .iter()
            .find(|(k, _)| k == "foo")
            .map(|(_, v)| v.as_str())
            .expect("missing foo label");

        let expected = normalize_label_value(long_value.as_str());
        assert_eq!(foo_value, expected.as_ref());
        assert_eq!(foo_value.len(), crate::labels::MAX_LABEL_VALUE_BYTES);
        assert_eq!(store.len(), 1);
    }

    struct FailAfterSymbolTable {
        inner: DefaultSymbolTable,
        intern_calls: usize,
        fail_at_call: Option<usize>,
    }

    impl FailAfterSymbolTable {
        fn new(fail_at_call: usize) -> Self {
            Self {
                inner: DefaultSymbolTable::default(),
                intern_calls: 0,
                fail_at_call: Some(fail_at_call),
            }
        }
    }

    impl Default for FailAfterSymbolTable {
        fn default() -> Self {
            Self::new(usize::MAX)
        }
    }

    impl SymbolTable for FailAfterSymbolTable {
        fn len(&self) -> usize {
            self.inner.len()
        }

        fn lookup(&self, symbol: &str) -> Option<SymbolId> {
            self.inner.lookup(symbol)
        }

        fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError> {
            self.intern_calls += 1;
            if self.fail_at_call == Some(self.intern_calls) {
                return Err(SymbolTableError::Arena(ArenaSymbolTableError::ArenaFull {
                    offset: 0,
                    len: 1,
                    end: 1,
                    max: 0,
                }));
            }
            self.inner.intern(symbol)
        }

        fn resolve(&self, id: SymbolId) -> &str {
            self.inner.resolve(id)
        }

        fn estimate_allocated_bytes(&self) -> usize {
            self.inner.estimate_allocated_bytes()
        }

        fn estimate_used_bytes(&self) -> usize {
            self.inner.estimate_used_bytes()
        }

        fn stats(&self) -> SymbolTableStats {
            self.inner.stats()
        }
    }

    struct PreparedStoreInterner<'a> {
        store: &'a mut FlatInternedLabelSetStore<FailAfterSymbolTable>,
    }

    impl OtlpLabelSetInterner for PreparedStoreInterner<'_> {
        type Error = LabelSetStoreError;

        fn on_skipped_non_scalar(&mut self) {}

        fn on_intern_error(&mut self, error: Self::Error) {
            panic!("unexpected prepared interning error: {error}");
        }

        fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
            self.store.intern_prepared_otlp(labels)
        }
    }

    struct RecoveringPreparedStoreInterner<'a> {
        store: &'a mut FlatInternedLabelSetStore<FailAfterSymbolTable>,
        errors: &'a mut Vec<LabelSetStoreError>,
    }

    impl OtlpLabelSetInterner for RecoveringPreparedStoreInterner<'_> {
        type Error = LabelSetStoreError;

        fn on_skipped_non_scalar(&mut self) {}

        fn on_intern_error(&mut self, error: Self::Error) {
            self.errors.push(error);
        }

        fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
            self.store.intern_prepared_otlp(labels)
        }
    }

    struct DefaultPreparedStoreInterner<'a> {
        store: &'a mut FlatInternedLabelSetStore,
    }

    impl OtlpLabelSetInterner for DefaultPreparedStoreInterner<'_> {
        type Error = LabelSetStoreError;

        fn on_skipped_non_scalar(&mut self) {}

        fn on_intern_error(&mut self, error: Self::Error) {
            panic!("unexpected prepared interning error: {error}");
        }

        fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
            self.store.intern_prepared_otlp(labels)
        }
    }

    fn otlp_string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(OtlpAnyValue {
                value: Some(AnyValue::StringValue(value.to_string())),
            }),
            key_strindex: 0,
        }
    }

    #[test]
    fn prepared_otlp_prefix_reuses_interned_resource_and_metric_symbols() {
        let resource_attributes = [
            otlp_string_attribute("cluster", "prod"),
            otlp_string_attribute("service", "checkout"),
        ];
        let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
        let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
        let metric = resource.metric("request.duration");
        let mut scratch = PreparedOtlpLabelSetScratch::default();
        let symbols = FailAfterSymbolTable::default();
        let mut store = FlatInternedLabelSetStore {
            symbols,
            ..FlatInternedLabelSetStore::default()
        };

        let first = intern_prepared_labelset(
            &mut PreparedStoreInterner { store: &mut store },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        );
        assert_eq!(first, Some(SeriesRef::new(0)));
        let first_calls = store.symbols.intern_calls;
        assert_eq!(first_calls, 8);

        let second = intern_prepared_labelset(
            &mut PreparedStoreInterner { store: &mut store },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        );
        assert_eq!(second, first);
        assert_eq!(store.symbols.intern_calls - first_calls, 2);
        assert_eq!(store.len(), 1);

        let mut second_store = FlatInternedLabelSetStore {
            symbols: FailAfterSymbolTable::default(),
            ..FlatInternedLabelSetStore::default()
        };
        let cross_store = intern_prepared_labelset(
            &mut PreparedStoreInterner {
                store: &mut second_store,
            },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        );
        assert_eq!(cross_store, Some(SeriesRef::new(0)));
        assert_eq!(second_store.symbols.intern_calls, 8);
        assert_eq!(
            decode(&second_store, SeriesRef::new(0)),
            decode(&store, SeriesRef::new(0))
        );
    }

    #[test]
    fn interned_id_hash_deduplicates_legacy_and_prepared_paths_with_store_scoped_caches() {
        let resource_attributes = [
            otlp_string_attribute("cluster", "prod"),
            otlp_string_attribute("service", "checkout"),
        ];
        let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
        let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
        let metric = resource.metric("request.duration");
        let canonical = [
            KeyValueRef::from(("__name__", "request.duration")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("pod", "checkout-0")),
            KeyValueRef::from(("service", "checkout")),
        ];

        let mut legacy_first: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        let legacy_series = legacy_first.intern(&canonical).unwrap();
        let mut scratch = PreparedOtlpLabelSetScratch::default();
        let prepared_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner {
                store: &mut legacy_first,
            },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        );
        assert_eq!(prepared_series, Some(legacy_series));
        assert_eq!(legacy_first.len(), 1);

        let mut prepared_first: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        let prepared_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner {
                store: &mut prepared_first,
            },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(prepared_first.intern(&canonical).unwrap(), prepared_series);
        assert_eq!(prepared_first.len(), 1);
        assert_eq!(
            decode(&prepared_first, prepared_series),
            owned_labels(&canonical)
        );

        let mut preseeded: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        let unrelated = [
            KeyValueRef::from(("__name__", "unrelated")),
            KeyValueRef::from(("aaa", "bbb")),
        ];
        assert_eq!(preseeded.intern(&unrelated).unwrap(), SeriesRef::new(0));
        let preseeded_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner {
                store: &mut preseeded,
            },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(preseeded_series, SeriesRef::new(1));
        assert_eq!(
            decode(&preseeded, preseeded_series),
            owned_labels(&canonical)
        );
        assert_ne!(
            preseeded.series_slice(preseeded_series),
            prepared_first.series_slice(prepared_series),
            "preseeding must make the store-local SymbolIds differ"
        );
    }

    #[test]
    fn interned_id_prepared_partial_cache_recovers_after_symbol_failure() {
        let resource_attributes = [
            otlp_string_attribute("cluster", "prod"),
            otlp_string_attribute("service", "checkout"),
        ];
        let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
        let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
        let metric = resource.metric("request.duration");
        let mut scratch = PreparedOtlpLabelSetScratch::default();
        let mut store: FlatInternedLabelSetStore<FailAfterSymbolTable> =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        store.symbols = FailAfterSymbolTable::new(4);
        let mut errors = Vec::new();

        let first = intern_prepared_labelset(
            &mut RecoveringPreparedStoreInterner {
                store: &mut store,
                errors: &mut errors,
            },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        );
        assert_eq!(first, None);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            LabelSetStoreError::SymbolTable(SymbolTableError::Arena(
                ArenaSymbolTableError::ArenaFull { .. }
            ))
        ));
        assert_eq!(store.len(), 0);
        assert_eq!(store.buffer_stats().encoded_scratch_len, 0);
        assert_eq!(store.buffer_stats().fingerprint_calls, 0);
        let calls_after_failure = store.symbols.intern_calls;

        store.symbols.fail_at_call = None;
        let retry = intern_prepared_labelset(
            &mut RecoveringPreparedStoreInterner {
                store: &mut store,
                errors: &mut errors,
            },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        );
        assert_eq!(retry, Some(SeriesRef::new(0)));
        assert_eq!(errors.len(), 1);
        assert_eq!(store.symbols.intern_calls - calls_after_failure, 6);
        assert_eq!(store.buffer_stats().fingerprint_calls, 1);
        assert_eq!(
            decode(&store, SeriesRef::new(0)),
            [
                ("__name__".into(), "request.duration".into()),
                ("cluster".into(), "prod".into()),
                ("pod".into(), "checkout-0".into()),
                ("service".into(), "checkout".into()),
            ]
        );

        let repeat = intern_prepared_labelset(
            &mut RecoveringPreparedStoreInterner {
                store: &mut store,
                errors: &mut errors,
            },
            &metric,
            &datapoint_attributes,
            &mut scratch,
        );
        assert_eq!(repeat, retry);
        let stats = store.buffer_stats();
        assert_eq!(stats.fingerprint_calls, 2);
        assert_eq!(stats.equality_checks, 1);
        assert_eq!(stats.equality_matches, 1);
        assert_eq!(stats.equality_mismatches, 0);
    }

    #[test]
    fn prepared_interned_id_paths_match_across_siphash_and_ahash() {
        let resource_attributes = [otlp_string_attribute("cluster", "prod")];
        let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
        let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
        let metric = resource.metric("request.duration");
        let mut siphash_scratch = PreparedOtlpLabelSetScratch::default();
        let mut ahash_scratch = PreparedOtlpLabelSetScratch::default();
        let mut siphash: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_siphash_labelset_hash();
        let mut ahash: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();

        for expected_series in [SeriesRef::new(0), SeriesRef::new(0)] {
            let siphash_series = intern_prepared_labelset(
                &mut DefaultPreparedStoreInterner {
                    store: &mut siphash,
                },
                &metric,
                &datapoint_attributes,
                &mut siphash_scratch,
            )
            .unwrap();
            let ahash_series = intern_prepared_labelset(
                &mut DefaultPreparedStoreInterner { store: &mut ahash },
                &metric,
                &datapoint_attributes,
                &mut ahash_scratch,
            )
            .unwrap();

            assert_eq!(siphash_series, expected_series);
            assert_eq!(ahash_series, expected_series);
            assert_eq!(
                decode(&siphash, siphash_series),
                decode(&ahash, ahash_series)
            );
        }

        assert_eq!(siphash.buffer_stats().labelset_hash, "interned_ids_siphash");
        assert_eq!(ahash.buffer_stats().labelset_hash, "interned_ids_ahash");
        for stats in [siphash.buffer_stats(), ahash.buffer_stats()] {
            assert_eq!(stats.fingerprint_calls, 2);
            assert_eq!(stats.equality_checks, 1);
            assert_eq!(stats.equality_matches, 1);
            assert_eq!(stats.equality_mismatches, 0);
            assert_eq!(stats.collision_inserts, 0);
        }
    }

    #[test]
    fn interned_id_prepared_path_deduplicates_raw_and_normalized_overlength_labels() {
        let raw_key = format!("{}tail", "é".repeat(MAX_LABEL_NAME_BYTES));
        let raw_value = format!("{}tail", "界".repeat(MAX_LABEL_VALUE_BYTES));
        let normalized_key = normalize_label_key(&raw_key).into_owned();
        let normalized_value = normalize_label_value(&raw_value).into_owned();
        let raw_attributes = [otlp_string_attribute(&raw_key, &raw_value)];
        let normalized_attributes = [otlp_string_attribute(&normalized_key, &normalized_value)];
        let raw_resource = PreparedOtlpResourceLabels::new(&raw_attributes);
        let normalized_resource = PreparedOtlpResourceLabels::new(&normalized_attributes);
        let raw_metric = raw_resource.metric("overlength.metric");
        let normalized_metric = normalized_resource.metric("overlength.metric");
        let mut scratch = PreparedOtlpLabelSetScratch::default();
        let mut store: FlatInternedLabelSetStore =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();

        let raw_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner { store: &mut store },
            &raw_metric,
            &[],
            &mut scratch,
        )
        .unwrap();
        let normalized_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner { store: &mut store },
            &normalized_metric,
            &[],
            &mut scratch,
        )
        .unwrap();

        assert_eq!(normalized_series, raw_series);
        assert_eq!(store.len(), 1);
        assert_eq!(
            decode(&store, raw_series),
            [
                ("__name__".into(), "overlength.metric".into()),
                (normalized_key, normalized_value),
            ]
        );
    }

    #[test]
    fn prepared_otlp_interning_is_equivalent_across_key_value_layouts() {
        let resource_attributes = [
            otlp_string_attribute("cluster", "prod"),
            otlp_string_attribute("service", "checkout"),
        ];
        let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
        let paged_resource = PreparedOtlpResourceLabels::new(&resource_attributes);
        let contiguous_resource = PreparedOtlpResourceLabels::new(&resource_attributes);
        let paged_metric = paged_resource.metric("request.duration");
        let contiguous_metric = contiguous_resource.metric("request.duration");
        let mut paged_scratch = PreparedOtlpLabelSetScratch::default();
        let mut contiguous_scratch = PreparedOtlpLabelSetScratch::default();
        let mut paged = FlatInternedLabelSetStore::with_key_value_page_capacity(3);
        let mut contiguous = FlatInternedLabelSetStore::with_contiguous_key_values();

        for _ in 0..2 {
            let paged_series = intern_prepared_labelset(
                &mut DefaultPreparedStoreInterner { store: &mut paged },
                &paged_metric,
                &datapoint_attributes,
                &mut paged_scratch,
            );
            let contiguous_series = intern_prepared_labelset(
                &mut DefaultPreparedStoreInterner {
                    store: &mut contiguous,
                },
                &contiguous_metric,
                &datapoint_attributes,
                &mut contiguous_scratch,
            );

            assert_eq!(paged_series, Some(SeriesRef::new(0)));
            assert_eq!(contiguous_series, paged_series);
        }

        assert_eq!(
            decode(&paged, SeriesRef::new(0)),
            decode(&contiguous, SeriesRef::new(0))
        );
        assert_eq!(paged.len(), contiguous.len());
        assert_eq!(paged.symbols().len(), contiguous.symbols().len());
    }

    #[test]
    fn interned_encode_error_clears_scratch_and_allows_retry() {
        let symbols = FailAfterSymbolTable::new(4);
        let mut store: FlatInternedLabelSetStore<FailAfterSymbolTable> =
            FlatInternedLabelSetStore::with_interned_id_labelset_hash();
        store.symbols = symbols;
        let labels = [
            KeyValueRef::from(("__name__", "metric")),
            KeyValueRef::from(("foo", "bar")),
        ];

        let error = store.intern(&labels).unwrap_err();
        assert!(matches!(
            error,
            LabelSetStoreError::SymbolTable(SymbolTableError::Arena(
                ArenaSymbolTableError::ArenaFull { .. }
            ))
        ));
        assert_eq!(store.len(), 0);
        assert_eq!(store.buffer_stats().encoded_scratch_len, 0);
        assert!(store.buffer_stats().encoded_scratch_cap >= labels.len());

        store.symbols.fail_at_call = None;
        let series = store.intern(&labels).unwrap();
        assert_eq!(series, SeriesRef::new(0));
        assert_eq!(
            decode(&store, series),
            [
                ("__name__".into(), "metric".into()),
                ("foo".into(), "bar".into())
            ]
        );
        assert_eq!(store.buffer_stats().encoded_scratch_len, 0);
    }

    #[test]
    fn labelset_store_propagates_symbol_table_errors() {
        #[derive(Default)]
        struct FailingSymbolTable;

        impl SymbolTable for FailingSymbolTable {
            fn len(&self) -> usize {
                0
            }

            fn lookup(&self, _symbol: &str) -> Option<SymbolId> {
                None
            }

            fn intern(&mut self, _symbol: &str) -> Result<SymbolId, SymbolTableError> {
                Err(SymbolTableError::Arena(ArenaSymbolTableError::ArenaFull {
                    offset: 0,
                    len: 1,
                    end: 1,
                    max: 0,
                }))
            }

            fn resolve(&self, _id: SymbolId) -> &str {
                ""
            }

            fn estimate_allocated_bytes(&self) -> usize {
                0
            }

            fn estimate_used_bytes(&self) -> usize {
                0
            }

            fn stats(&self) -> SymbolTableStats {
                SymbolTableStats::Arc {
                    symbols: 0,
                    symbol_to_id_len: 0,
                    symbol_to_id_cap: 0,
                    id_to_symbol_len: 0,
                    id_to_symbol_cap: 0,
                }
            }
        }

        let mut store: FlatInternedLabelSetStore<FailingSymbolTable> =
            FlatInternedLabelSetStore::default();

        let labels = [
            KeyValueRef::from(("__name__", "metric")),
            KeyValueRef::from(("foo", "bar")),
        ];

        let err = store.intern(&labels).unwrap_err();
        assert!(matches!(
            err,
            LabelSetStoreError::SymbolTable(SymbolTableError::Arena(
                ArenaSymbolTableError::ArenaFull { .. }
            ))
        ));
    }

    #[test]
    fn keyset_bit_packed_comprehensive_widths() {
        let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();

        // Define widths to test covering various bit boundaries:
        // 0 bits (1 value)
        // 1 bit (2 values)
        // 2 bits (4 values)
        // 3 bits (8 values)
        // 4 bits (nibble)
        // 7 bits (almost byte)
        // 8 bits (byte)
        // 9 bits (byte + 1)
        // 10 bits (crossing)
        let widths = [0, 1, 2, 3, 4, 7, 8, 9, 10];

        let mut keys_info = Vec::new();
        for (i, &w) in widths.iter().enumerate() {
            // Prefix with "k_" and index to ensure uniqueness and order
            let key = format!("k_{:02}_{}", i, w);
            // Needed count to force at least one value to require `w` bits:
            // Max code must be >= 2^(w-1). So we need 2^(w-1) + 1 values (0..2^(w-1)).
            // Exception: width 0 -> 1 value.
            let needed_unique_count = if w == 0 { 1 } else { (1 << (w - 1)) + 1 };
            keys_info.push((key, needed_unique_count));
        }

        let max_count = keys_info.iter().map(|(_, c)| *c).max().unwrap();

        let mut series_refs = Vec::new();

        for j in 0..max_count {
            let mut label_pairs = Vec::new();

            // Always have a name
            label_pairs.push(("__name__".to_string(), "test".to_string()));

            for (key, count) in &keys_info {
                // Use modulo to keep cycling through values, ensuring we use high codes
                // in later series as well, mixing them up.
                // However, `intern` creates code based on insertion order.
                // Value "v_0" gets code 0, "v_1" gets code 1.
                // So as long as we eventually see "v_N", we create code N.
                let val_idx = if j < *count { j } else { j % count };
                let val = format!("v_{}", val_idx);
                label_pairs.push((key.clone(), val));
            }

            // Sort by key to satisfy labelset canonical requirement
            label_pairs.sort_by(|a, b| a.0.cmp(&b.0));

            let label_refs: Vec<KeyValueRef> = label_pairs
                .iter()
                .map(|(k, v)| KeyValueRef::from((k.as_str(), v.as_str())))
                .collect();

            let s = builder.intern(&label_refs).unwrap();
            series_refs.push(s);
        }

        let sealed = builder.seal_bit_packed();
        assert_eq!(sealed.len(), series_refs.len());

        for (i, &s) in series_refs.iter().enumerate() {
            let decoded_orig = decode(&builder, s);
            let decoded_sealed = decode(&sealed, s);
            assert_eq!(
                decoded_orig, decoded_sealed,
                "Mismatch at series index {} (series ref {:?})",
                i, s
            );
        }
    }
}
