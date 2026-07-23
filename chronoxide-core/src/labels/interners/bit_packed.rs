use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::super::symbol_table::{DefaultSymbolTable, SymbolTable};
use super::super::{
    KeySetId, KeyValueRef, SeriesRef, SymbolId, U64HashMap, ValueCode,
    estimate_hashmap_table_bytes, estimate_vec_buffer_bytes,
};
use super::common::{LabelSetStore, LabelSetStoreError};
use super::keyset::{KeySetTable, SeriesEntry, ValueCodeDict};
use super::packing::{BitPackedKeySetBlock, PackedKeySetLabelSetStoreBufferStats, unpack_bits};

#[derive(Default)]
pub struct BitPackedKeySetLabelSetStore<S: SymbolTable = DefaultSymbolTable> {
    pub(super) by_hash: U64HashMap<SeriesRef>,
    pub(super) by_hash_collisions: U64HashMap<Vec<SeriesRef>>,
    pub(super) symbols: S,
    pub(super) keysets: KeySetTable,
    pub(super) value_dicts: HashMap<SymbolId, ValueCodeDict>,
    pub(super) per_keyset_blocks: Vec<BitPackedKeySetBlock>,
    pub(super) series: Vec<SeriesEntry>,
    pub(super) estimated_collision_bytes: usize,
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

    pub(super) fn shrink_to_fit(&mut self) {
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
