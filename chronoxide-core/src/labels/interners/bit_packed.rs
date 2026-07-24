use std::collections::HashMap;

use super::super::symbol_table::{DefaultSymbolTable, SymbolTable};
use super::super::{KeySetId, KeyValueRef, SeriesRef, SymbolId, U64HashMap, ValueCode};
use super::common::{LabelSetStore, LabelSetStoreError};
use super::keyset::{KeySetTable, SeriesEntry, ValueCodeDict};
use super::packing::{
    BitPackedKeySetBlock, PackedKeySetLabelSetStoreBufferStats, PackedKeySetStoreAccounting,
    estimate_packed_keyset_allocated_bytes, estimate_packed_keyset_used_bytes,
    packed_keyset_buffer_stats, shrink_packed_keyset_store, unpack_bits,
};

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

    fn accounting(&self) -> PackedKeySetStoreAccounting<'_, S, BitPackedKeySetBlock> {
        PackedKeySetStoreAccounting {
            store_inline_bytes: std::mem::size_of::<Self>(),
            symbols: &self.symbols,
            by_hash: &self.by_hash,
            by_hash_collisions: &self.by_hash_collisions,
            keysets: &self.keysets,
            value_dicts: &self.value_dicts,
            per_keyset_blocks: &self.per_keyset_blocks,
            series: &self.series,
            estimated_collision_bytes: self.estimated_collision_bytes,
        }
    }

    pub fn buffer_stats(&self) -> PackedKeySetLabelSetStoreBufferStats {
        packed_keyset_buffer_stats(self.accounting())
    }

    pub(super) fn shrink_to_fit(&mut self) {
        shrink_packed_keyset_store(
            &mut self.by_hash,
            &mut self.by_hash_collisions,
            &mut self.keysets,
            &mut self.value_dicts,
            &mut self.per_keyset_blocks,
            &mut self.series,
        );
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
        estimate_packed_keyset_allocated_bytes(self.accounting())
    }

    fn estimate_used_bytes(&self) -> usize {
        estimate_packed_keyset_used_bytes(self.accounting())
    }
}
