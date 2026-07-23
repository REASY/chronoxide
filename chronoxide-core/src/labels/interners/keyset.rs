use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::super::normalizer::{normalize_label_key, normalize_label_value};
use super::super::symbol_table::{DefaultSymbolTable, SymbolTable};
use super::super::{
    KeySetId, KeyValueRef, SeriesRef, SymbolId, U64HashMap, ValueCode, estimate_arc_bytes,
    estimate_hashmap_table_bytes, estimate_vec_buffer_bytes,
};
use super::bit_packed::BitPackedKeySetLabelSetStore;
use super::common::{LabelSetStore, LabelSetStoreError};
use super::fixed_width::FixedWidthPackedKeySetLabelSetStore;
use super::packing::{
    BitPackedKeySetBlock, PackedKeySetBlock, bit_width_for_max_code, pack_bits, pack_value_code,
    width_for_cardinality,
};

#[derive(Clone, Default)]
pub struct KeySetTable {
    pub(super) keyset_to_id: HashMap<Arc<[SymbolId]>, KeySetId>,
    pub(super) id_to_keyset: Vec<Arc<[SymbolId]>>,
    pub(super) estimated_alloc_bytes: usize,
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

    pub(super) fn estimated_heap_bytes(&self) -> usize {
        estimate_hashmap_table_bytes(&self.keyset_to_id)
            .saturating_add(estimate_vec_buffer_bytes(&self.id_to_keyset))
            .saturating_add(self.estimated_alloc_bytes)
    }

    pub(super) fn shrink_to_fit(&mut self) {
        self.keyset_to_id.shrink_to_fit();
        self.id_to_keyset.shrink_to_fit();
    }
}

#[derive(Clone, Default)]
pub struct ValueCodeDict {
    pub(super) value_to_code: HashMap<SymbolId, ValueCode>,
    pub(super) code_to_value: Vec<SymbolId>,
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

    pub(super) fn shrink_to_fit(&mut self) {
        self.value_to_code.shrink_to_fit();
        self.code_to_value.shrink_to_fit();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SeriesEntry {
    pub(super) keyset_id: KeySetId,
    pub(super) row: u32,
}

#[derive(Clone, Default)]
pub(super) struct KeySetRows {
    pub(super) key_count: usize,
    pub(super) values: Vec<ValueCode>,
}

impl KeySetRows {
    fn rows(&self) -> u32 {
        if self.key_count == 0 {
            return 0;
        }
        (self.values.len() / self.key_count) as u32
    }

    pub(super) fn row_slice(&self, row: u32) -> &[ValueCode] {
        let start = row as usize * self.key_count;
        let end = start + self.key_count;
        &self.values[start..end]
    }
}

#[derive(Default)]
pub struct KeySetDictEncodedLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    pub(super) by_hash: U64HashMap<SeriesRef>,
    pub(super) by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    pub(super) symbols: S,
    pub(super) keysets: KeySetTable,
    pub(super) value_dicts: HashMap<SymbolId, ValueCodeDict>,
    pub(super) per_keyset_rows: Vec<KeySetRows>,
    pub(super) series: Vec<SeriesEntry>,
    pub(super) estimated_collision_bytes: usize,
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
            super::KEYSET_BUFFER_STATS_TYPE_NAME,
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
