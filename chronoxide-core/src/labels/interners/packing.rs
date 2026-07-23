use super::super::ValueCode;

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
