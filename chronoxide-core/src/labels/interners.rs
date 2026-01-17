use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use thiserror::Error;

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
struct InternedKeyValue {
    key: SymbolId,
    value: SymbolId,
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

#[inline]
fn encode_interned_labelset<S: SymbolTable>(
    symbols: &mut S,
    labels: &[KeyValueRef<'_>],
) -> Result<(Vec<InternedKeyValue>, u64), LabelSetStoreError> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut encoded: Vec<InternedKeyValue> = Vec::with_capacity(labels.len());
    for label in labels {
        let key_norm = normalize_label_key(label.key);
        let value_norm = normalize_label_value(label.value);
        key_norm.as_ref().hash(&mut hasher);
        value_norm.as_ref().hash(&mut hasher);

        let key = symbols.intern(key_norm.as_ref())?;
        let value = symbols.intern(value_norm.as_ref())?;
        encoded.push(InternedKeyValue { key, value });
    }
    let labelset_hash = hasher.finish();
    Ok((encoded, labelset_hash))
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

#[derive(Default)]
pub struct FlatInternedLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    symbols: S,
    series: Vec<SeriesLoc>,
    key_values: Vec<InternedKeyValue>,
    estimated_collision_bytes: usize,
}

impl<S: SymbolTable> FlatInternedLabelSetStore<S> {
    pub fn symbols(&self) -> &S {
        &self.symbols
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
        }
    }

    fn series_slice(&self, series: SeriesRef) -> &[InternedKeyValue] {
        let loc = self.series[series.0 as usize];
        let start = loc.offset as usize;
        let end = start + loc.len as usize;
        &self.key_values[start..end]
    }

    fn labels_equal(stored: &[InternedKeyValue], candidate: &[InternedKeyValue]) -> bool {
        stored == candidate
    }

    fn encode(
        &mut self,
        labels: &[KeyValueRef<'_>],
    ) -> Result<(Vec<InternedKeyValue>, u64), LabelSetStoreError> {
        encode_interned_labelset(&mut self.symbols, labels)
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
}

impl std::fmt::Display for FlatInternedLabelSetStoreBufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type={} by_hash_len={} by_hash_cap={} by_hash_collisions_len={} by_hash_collisions_cap={} series_len={} series_cap={} key_values_len={} key_values_cap={}",
            std::any::type_name::<Self>(),
            self.by_hash_len,
            self.by_hash_cap,
            self.by_hash_collisions_len,
            self.by_hash_collisions_cap,
            self.series_len,
            self.series_cap,
            self.key_values_len,
            self.key_values_cap,
        )
    }
}

impl<S: SymbolTable> LabelSetStore for FlatInternedLabelSetStore<S> {
    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        debug_assert!(
            labels.windows(2).all(|pair| pair[0].key < pair[1].key),
            "LabelSet must be canonical (sorted by key, unique keys)"
        );

        let (encoded, labelset_hash) = self.encode(labels)?;

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
        let offset = self.key_values.len() as u32;
        let len = encoded.len() as u32;
        self.key_values.extend_from_slice(&encoded);
        self.series.push(SeriesLoc { offset, len });

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
        let key_values_bytes = estimate_vec_buffer_bytes(&self.key_values);
        let symbols_bytes = self.symbols.estimate_allocated_bytes();

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(by_hash_collision_heap_bytes)
            .saturating_add(series_bytes)
            .saturating_add(key_values_bytes)
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
            .saturating_mul(std::mem::size_of::<InternedKeyValue>());
        let symbols_bytes = self.symbols.estimate_used_bytes();

        std::mem::size_of::<Self>()
            .saturating_add(by_hash_bytes)
            .saturating_add(collision_bytes)
            .saturating_add(series_bytes)
            .saturating_add(key_values_bytes)
            .saturating_add(symbols_bytes)
    }
}

#[derive(Default)]
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
}

#[derive(Default)]
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SeriesEntry {
    keyset_id: KeySetId,
    row: u32,
}

#[derive(Default)]
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
            "  series={} keysets={} value_dicts={} total_cardinality={} symbols={}",
            stats.series_len,
            stats.keysets_len,
            stats.value_dicts_len,
            stats.total_cardinality,
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
        let total_cardinality = self
            .value_dicts
            .values()
            .map(|dict| dict.cardinality())
            .fold(0usize, usize::saturating_add);

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
            total_cardinality,
            keysets_len: self.keysets.id_to_keyset.len(),
            keysets_cap: self.keysets.id_to_keyset.capacity(),
            keyset_to_id_len: self.keysets.keyset_to_id.len(),
            keyset_to_id_cap: self.keysets.keyset_to_id.capacity(),
        }
    }

    pub fn seal_fixed_width(self) -> PackedKeySetLabelSetStore<S> {
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
            blocks.push(PackedKeySetBlock {
                widths: widths.into_boxed_slice(),
                row_len,
                data,
            });
        }

        PackedKeySetLabelSetStore {
            by_hash: self.by_hash,
            by_hash_collisions: self.by_hash_collisions,
            symbols: self.symbols,
            keysets: self.keysets,
            value_dicts: self.value_dicts,
            per_keyset_blocks: blocks,
            series: self.series,
            estimated_collision_bytes: self.estimated_collision_bytes,
        }
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
    pub total_cardinality: usize,
    pub keysets_len: usize,
    pub keysets_cap: usize,
    pub keyset_to_id_len: usize,
    pub keyset_to_id_cap: usize,
}

impl std::fmt::Display for KeySetLabelSetStoreBufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type={} by_hash_len={} by_hash_cap={} by_hash_collisions_len={} by_hash_collisions_cap={} series_len={} series_cap={} per_keyset_rows_len={} per_keyset_rows_cap={} per_keyset_values_len={} per_keyset_values_cap={} value_dicts_len={} value_dicts_cap={} total_cardinality={} keysets_len={} keysets_cap={} keyset_to_id_len={} keyset_to_id_cap={}",
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
            self.total_cardinality,
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
pub struct PackedKeySetLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    by_hash: U64HashMap<SeriesRef>,
    by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    symbols: S,
    keysets: KeySetTable,
    value_dicts: HashMap<SymbolId, ValueCodeDict>,
    per_keyset_blocks: Vec<PackedKeySetBlock>,
    series: Vec<SeriesEntry>,
    estimated_collision_bytes: usize,
}

impl<S: SymbolTable> PackedKeySetLabelSetStore<S> {
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

impl<S: SymbolTable> LabelSetStore for PackedKeySetLabelSetStore<S> {
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

struct PackedKeySetBlock {
    widths: Box<[u8]>,
    row_len: usize,
    data: Vec<u8>,
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

    use crate::labels::{ArenaSymbolTableError, SymbolTableStats};

    fn decode(store: &impl LabelSetStore, series: SeriesRef) -> Vec<(String, String)> {
        let mut labels = Vec::new();
        store.visit_labelset(series, |key, value| {
            labels.push((key.to_string(), value.to_string()));
        });
        labels
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
}
