use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::otlp_labelset::CanonicalLabelSet;

use super::super::normalizer::{normalize_label_key, normalize_label_value};
use super::super::symbol_table::{DefaultSymbolTable, SymbolTable};
use super::super::{
    KeyValueRef, SeriesRef, SymbolId, U64HashMap, estimate_hashmap_table_bytes,
    estimate_vec_buffer_bytes,
};
use super::common::{LabelSetStore, LabelSetStoreError};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct InternedKeyValue {
    pub(crate) key: SymbolId,
    pub(crate) value: SymbolId,
}

#[derive(Clone, Copy, Debug)]
pub struct FlatInternedLabelSetRow<'a> {
    labels: &'a [InternedKeyValue],
}

impl<'a> FlatInternedLabelSetRow<'a> {
    pub fn len(self) -> usize {
        self.labels.len()
    }

    pub fn is_empty(self) -> bool {
        self.labels.is_empty()
    }

    pub fn get(self, index: usize) -> Option<(SymbolId, SymbolId)> {
        self.labels.get(index).map(|label| (label.key, label.value))
    }

    /// Returns the symbol IDs at `index`.
    ///
    /// # Panics
    ///
    /// Panics when `index` is outside this row.
    pub fn symbol_ids_at(self, index: usize) -> (SymbolId, SymbolId) {
        let label = self.labels[index];
        (label.key, label.value)
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = (SymbolId, SymbolId)> + 'a {
        self.labels.iter().map(|label| (label.key, label.value))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedInternedKeyValue {
    cache_id: u64,
    interned: InternedKeyValue,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum FlatInternedLabelSetHash {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SeriesLoc {
    pub(super) offset: u32,
    pub(super) len: u32,
}

pub(super) const DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY: usize = u16::MAX as usize + 1;
pub(super) const MAX_INTERNED_KEY_VALUE_PAGES: usize = u16::MAX as usize + 1;

impl SeriesLoc {
    pub(super) fn paged(
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

    pub(super) fn paged_parts(self) -> (usize, usize) {
        (
            (self.offset >> 16) as usize,
            (self.offset & u32::from(u16::MAX)) as usize,
        )
    }
}

pub(super) struct PagedInternedKeyValues {
    pub(super) pages: Vec<Vec<InternedKeyValue>>,
    pub(super) len: usize,
    pub(super) page_capacity: usize,
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

    pub(super) fn append_row(
        &mut self,
        row: &[InternedKeyValue],
    ) -> Result<SeriesLoc, LabelSetStoreError> {
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

    pub(super) fn row(&self, loc: SeriesLoc) -> &[InternedKeyValue] {
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

pub(super) enum InternedKeyValueStorage {
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
pub(super) fn encode_interned_labelset_into<
    'a,
    const HASH_INTERNED_IDS: bool,
    S: SymbolTable,
    H: Hasher,
>(
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

pub struct FlatInternedLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    pub(super) by_hash: U64HashMap<SeriesRef>,
    pub(super) by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    pub(super) symbols: S,
    pub(super) series: Vec<SeriesLoc>,
    pub(super) key_values: InternedKeyValueStorage,
    pub(super) labelset_hash: FlatInternedLabelSetHash,
    pub(super) labelset_ahash: ahash::RandomState,
    pub(super) encoded_scratch: Vec<InternedKeyValue>,
    pub(super) fingerprint_calls: u64,
    pub(super) fingerprint_label_pairs: u64,
    pub(super) equality_checks: u64,
    pub(super) equality_matches: u64,
    pub(super) equality_mismatches: u64,
    pub(super) collision_inserts: u64,
    pub(super) estimated_collision_bytes: usize,
    pub(super) prepared_cache_id: u64,
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
    pub(super) fn with_key_value_page_capacity(page_capacity: usize) -> Self
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
        for (key, value) in self.labelset_symbol_ids(series).iter() {
            visitor(key, value);
        }
    }

    pub fn labelset_symbol_ids(&self, series: SeriesRef) -> FlatInternedLabelSetRow<'_> {
        FlatInternedLabelSetRow {
            labels: self.series_slice(series),
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

    pub(super) fn series_slice(&self, series: SeriesRef) -> &[InternedKeyValue] {
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

    pub(super) fn intern_encoded(
        &mut self,
        labelset_hash: u64,
    ) -> Result<SeriesRef, LabelSetStoreError> {
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
            super::FLAT_BUFFER_STATS_TYPE_NAME,
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
