use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::super::symbol_table::SymbolTable;
use super::super::{
    KeySetId, SeriesRef, SymbolId, U64HashMap, ValueCode, estimate_hashmap_table_bytes,
    estimate_vec_buffer_bytes,
};
use super::keyset::{KeySetTable, SeriesEntry, ValueCodeDict};

pub(super) struct PackedKeySetBlock {
    pub(super) widths: Box<[u8]>,
    pub(super) row_len: usize,
    pub(super) data: Vec<u8>,
}

pub(super) struct BitPackedKeySetBlock {
    pub(super) widths_bits: Box<[u8]>,
    pub(super) row_bits: usize,
    pub(super) data: Vec<u8>,
}

pub(super) trait PackedKeySetBlockAccounting {
    fn packed_widths_len(&self) -> usize;
    fn packed_values_len(&self) -> usize;
    fn packed_values_capacity(&self) -> usize;
    fn shrink_packed_values_to_fit(&mut self);
}

impl PackedKeySetBlockAccounting for PackedKeySetBlock {
    fn packed_widths_len(&self) -> usize {
        self.widths.len()
    }

    fn packed_values_len(&self) -> usize {
        self.data.len()
    }

    fn packed_values_capacity(&self) -> usize {
        self.data.capacity()
    }

    fn shrink_packed_values_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }
}

impl PackedKeySetBlockAccounting for BitPackedKeySetBlock {
    fn packed_widths_len(&self) -> usize {
        self.widths_bits.len()
    }

    fn packed_values_len(&self) -> usize {
        self.data.len()
    }

    fn packed_values_capacity(&self) -> usize {
        self.data.capacity()
    }

    fn shrink_packed_values_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }
}

pub(super) struct PackedKeySetStoreAccounting<'a, S, B> {
    pub(super) store_inline_bytes: usize,
    pub(super) symbols: &'a S,
    pub(super) by_hash: &'a U64HashMap<SeriesRef>,
    pub(super) by_hash_collisions: &'a U64HashMap<Vec<SeriesRef>>,
    pub(super) keysets: &'a KeySetTable,
    pub(super) value_dicts: &'a HashMap<SymbolId, ValueCodeDict>,
    pub(super) per_keyset_blocks: &'a Vec<B>,
    pub(super) series: &'a Vec<SeriesEntry>,
    pub(super) estimated_collision_bytes: usize,
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
            super::PACKED_BUFFER_STATS_TYPE_NAME,
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

pub(super) fn packed_keyset_buffer_stats<S, B>(
    store: PackedKeySetStoreAccounting<'_, S, B>,
) -> PackedKeySetLabelSetStoreBufferStats
where
    B: PackedKeySetBlockAccounting,
{
    let sum_per_key_cardinality = store
        .value_dicts
        .values()
        .map(ValueCodeDict::cardinality)
        .fold(0usize, usize::saturating_add);
    let mut global_values = HashSet::new();
    for dict in store.value_dicts.values() {
        for value in &dict.code_to_value {
            global_values.insert(*value);
        }
    }
    let global_distinct_values = global_values.len();

    let packed_values_len = store
        .per_keyset_blocks
        .iter()
        .map(PackedKeySetBlockAccounting::packed_values_len)
        .fold(0usize, usize::saturating_add);
    let packed_values_cap = store
        .per_keyset_blocks
        .iter()
        .map(PackedKeySetBlockAccounting::packed_values_capacity)
        .fold(0usize, usize::saturating_add);
    let packed_widths_len = store
        .per_keyset_blocks
        .iter()
        .map(PackedKeySetBlockAccounting::packed_widths_len)
        .fold(0usize, usize::saturating_add);

    PackedKeySetLabelSetStoreBufferStats {
        by_hash_len: store.by_hash.len(),
        by_hash_cap: store.by_hash.capacity(),
        by_hash_collisions_len: store.by_hash_collisions.len(),
        by_hash_collisions_cap: store.by_hash_collisions.capacity(),
        series_len: store.series.len(),
        series_cap: store.series.capacity(),
        per_keyset_blocks_len: store.per_keyset_blocks.len(),
        per_keyset_blocks_cap: store.per_keyset_blocks.capacity(),
        packed_values_len,
        packed_values_cap,
        packed_widths_len,
        packed_widths_cap: packed_widths_len,
        value_dicts_len: store.value_dicts.len(),
        value_dicts_cap: store.value_dicts.capacity(),
        sum_per_key_cardinality,
        global_distinct_values,
        keysets_len: store.keysets.id_to_keyset.len(),
        keysets_cap: store.keysets.id_to_keyset.capacity(),
        keyset_to_id_len: store.keysets.keyset_to_id.len(),
        keyset_to_id_cap: store.keysets.keyset_to_id.capacity(),
    }
}

pub(super) fn shrink_packed_keyset_store<B>(
    by_hash: &mut U64HashMap<SeriesRef>,
    by_hash_collisions: &mut U64HashMap<Vec<SeriesRef>>,
    keysets: &mut KeySetTable,
    value_dicts: &mut HashMap<SymbolId, ValueCodeDict>,
    per_keyset_blocks: &mut Vec<B>,
    series: &mut Vec<SeriesEntry>,
) where
    B: PackedKeySetBlockAccounting,
{
    by_hash.shrink_to_fit();
    by_hash_collisions.shrink_to_fit();
    for collisions in by_hash_collisions.values_mut() {
        collisions.shrink_to_fit();
    }
    keysets.shrink_to_fit();
    value_dicts.shrink_to_fit();
    for dict in value_dicts.values_mut() {
        dict.shrink_to_fit();
    }
    per_keyset_blocks.shrink_to_fit();
    for block in per_keyset_blocks {
        block.shrink_packed_values_to_fit();
    }
    series.shrink_to_fit();
}

pub(super) fn estimate_packed_keyset_allocated_bytes<S, B>(
    store: PackedKeySetStoreAccounting<'_, S, B>,
) -> usize
where
    S: SymbolTable,
    B: PackedKeySetBlockAccounting,
{
    let symbols_bytes = store.symbols.estimate_allocated_bytes();
    let by_hash_bytes = estimate_hashmap_table_bytes(store.by_hash)
        .saturating_add(estimate_hashmap_table_bytes(store.by_hash_collisions));
    let by_hash_collision_heap_bytes = store.estimated_collision_bytes;

    let keysets_bytes = store.keysets.estimated_heap_bytes();

    let value_dicts_bytes = estimate_hashmap_table_bytes(store.value_dicts);
    let value_dicts_heap_bytes = store
        .value_dicts
        .values()
        .map(|dict| {
            estimate_hashmap_table_bytes(&dict.value_to_code)
                .saturating_add(estimate_vec_buffer_bytes(&dict.code_to_value))
        })
        .fold(0usize, usize::saturating_add);

    let per_keyset_blocks_bytes = estimate_vec_buffer_bytes(store.per_keyset_blocks);
    let per_keyset_blocks_heap_bytes = store
        .per_keyset_blocks
        .iter()
        .map(|block| {
            block
                .packed_values_capacity()
                .saturating_mul(std::mem::size_of::<u8>())
                .saturating_add(
                    block
                        .packed_widths_len()
                        .saturating_mul(std::mem::size_of::<u8>()),
                )
        })
        .fold(0usize, usize::saturating_add);

    let series_bytes = estimate_vec_buffer_bytes(store.series);

    store
        .store_inline_bytes
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

pub(super) fn estimate_packed_keyset_used_bytes<S, B>(
    store: PackedKeySetStoreAccounting<'_, S, B>,
) -> usize
where
    S: SymbolTable,
    B: PackedKeySetBlockAccounting,
{
    let symbols_bytes = store.symbols.estimate_used_bytes();

    let keysets_bytes = store
        .keysets
        .id_to_keyset
        .len()
        .saturating_mul(std::mem::size_of::<Arc<[SymbolId]>>())
        .saturating_add(
            store
                .keysets
                .keyset_to_id
                .len()
                .saturating_mul(std::mem::size_of::<(Arc<[SymbolId]>, KeySetId)>()),
        )
        .saturating_add(store.keysets.estimated_alloc_bytes);

    let value_dicts_bytes = store
        .value_dicts
        .len()
        .saturating_mul(std::mem::size_of::<(SymbolId, ValueCodeDict)>());

    let value_dicts_used_bytes = store
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

    let per_keyset_blocks_bytes = store
        .per_keyset_blocks
        .len()
        .saturating_mul(std::mem::size_of::<B>());
    let per_keyset_blocks_used_bytes = store
        .per_keyset_blocks
        .iter()
        .map(|block| {
            block
                .packed_widths_len()
                .saturating_mul(std::mem::size_of::<u8>())
                .saturating_add(block.packed_values_len())
        })
        .fold(0usize, usize::saturating_add);

    let series_bytes = store
        .series
        .len()
        .saturating_mul(std::mem::size_of::<SeriesEntry>());

    let by_hash_bytes = store
        .by_hash
        .len()
        .saturating_mul(std::mem::size_of::<(u64, SeriesRef)>())
        .saturating_add(
            store
                .by_hash_collisions
                .len()
                .saturating_mul(std::mem::size_of::<(u64, Vec<SeriesRef>)>()),
        );

    let collision_bytes = store
        .by_hash_collisions
        .values()
        .map(|ids| ids.len().saturating_mul(std::mem::size_of::<SeriesRef>()))
        .fold(0usize, usize::saturating_add);

    store
        .store_inline_bytes
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

pub(super) fn pack_value_code(out: &mut Vec<u8>, width: u8, value: ValueCode) {
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

pub(super) fn unpack_value_code(data: &[u8], offset: &mut usize, width: u8) -> ValueCode {
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

pub(super) fn width_for_cardinality(cardinality: usize) -> u8 {
    match cardinality {
        0 | 1 => 0,
        2..=256 => 1,
        257..=65_536 => 2,
        _ => 4,
    }
}

pub(super) fn bit_width_for_max_code(max_code: u32) -> u8 {
    if max_code == 0 {
        0
    } else {
        (u32::BITS - max_code.leading_zeros()) as u8
    }
}

pub(super) fn pack_bits(out: &mut [u8], bit_offset: &mut usize, width: u8, value: u32) {
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

pub(super) fn unpack_bits(data: &[u8], bit_offset: &mut usize, width: u8) -> u32 {
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
