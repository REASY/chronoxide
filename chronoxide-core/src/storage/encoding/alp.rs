use std::io;

use crate::storage::encoding::bitstream::{BitReader, BitWriter};
use crate::storage::encoding::{
    decode_varint, decode_zigzag_i64, encode_varint, encode_zigzag_i64,
};
use vortex_alp::{ALPFloat, Exponents, RDEncoder};
use vortex_array::ToCanonical;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_dtype::PType;

#[derive(Debug)]
pub(crate) struct AlpEncoder {
    values: Vec<f64>,
}

impl AlpEncoder {
    pub(crate) fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub(crate) fn push(&mut self, value: f64) -> io::Result<()> {
        self.values.push(value);
        Ok(())
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }

    pub(crate) fn len_bytes(&self) -> io::Result<usize> {
        encode_alp_values(&self.values).map(|buf| buf.len())
    }

    pub(crate) fn snapshot(&self) -> io::Result<Vec<u8>> {
        encode_alp_values(&self.values)
    }

    pub(crate) fn finish(self) -> io::Result<Vec<u8>> {
        encode_alp_values(&self.values)
    }
}

impl Default for AlpEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct AlpRdEncoder {
    values: Vec<f64>,
}

impl AlpRdEncoder {
    pub(crate) fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub(crate) fn push(&mut self, value: f64) -> io::Result<()> {
        self.values.push(value);
        Ok(())
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }

    pub(crate) fn len_bytes(&self) -> io::Result<usize> {
        encode_alp_rd_values(&self.values).map(|buf| buf.len())
    }

    pub(crate) fn snapshot(&self) -> io::Result<Vec<u8>> {
        encode_alp_rd_values(&self.values)
    }

    pub(crate) fn finish(self) -> io::Result<Vec<u8>> {
        encode_alp_rd_values(&self.values)
    }
}

impl Default for AlpRdEncoder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn encode_alp_values(values: &[f64]) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alp values must be non-empty",
        ));
    }

    let (exp, encoded, patch_indices, patch_values, _chunk_offsets) =
        <f64 as ALPFloat>::encode(values, None);
    let encoded = encoded.as_slice();
    let patch_indices = patch_indices.as_slice();
    let patch_values = patch_values.as_slice();

    if patch_indices.len() != patch_values.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp patch arrays mismatch",
        ));
    }

    let (min, max) = min_max_i64(encoded)?;
    let range = (max as i128 - min as i128) as u128;
    let bits = bit_width_u128(range);
    if bits > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp bit width too large",
        ));
    }

    let mut writer = BitWriter::new();
    if bits > 0 {
        for &value in encoded {
            let delta = (value as i128 - min as i128) as u128;
            writer.write_bits(delta as u64, bits);
        }
    }

    let mut out = Vec::new();
    out.push(exp.e);
    out.push(exp.f);
    out.push(bits);
    encode_varint(encode_zigzag_i64(min), &mut out);
    encode_varint(patch_indices.len() as u64, &mut out);
    for (idx, value) in patch_indices.iter().zip(patch_values.iter()) {
        encode_varint(*idx, &mut out);
        encode_f64(*value, &mut out);
    }
    out.extend_from_slice(&writer.finish());
    Ok(out)
}

pub(crate) fn decode_alp_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if buf.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "alp header too short",
        ));
    }

    let mut cursor = 0usize;
    let exp = Exponents {
        e: buf[cursor],
        f: buf[cursor + 1],
    };
    cursor += 2;
    let bits = buf[cursor];
    cursor += 1;

    let min = decode_zigzag_i64(decode_varint(buf, &mut cursor)?);
    let patch_count = decode_varint(buf, &mut cursor)? as usize;
    let mut patches = Vec::with_capacity(patch_count);
    for _ in 0..patch_count {
        let idx = decode_varint(buf, &mut cursor)? as usize;
        let value = decode_f64(buf, &mut cursor)?;
        patches.push((idx, value));
    }

    let mut encoded = Vec::with_capacity(count);
    if bits == 0 {
        encoded.resize(count, min);
    } else if bits > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp bit width invalid",
        ));
    } else {
        let mut reader = BitReader::new(&buf[cursor..]);
        for _ in 0..count {
            let delta = reader.read_bits(bits)?;
            let value = (min as i128 + delta as i128) as i64;
            encoded.push(value);
        }
    }

    let mut values = Vec::with_capacity(count);
    for encoded in &encoded {
        values.push(<f64 as ALPFloat>::decode_single(*encoded, exp));
    }
    for (idx, value) in patches {
        if idx >= values.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "alp patch index out of range",
            ));
        }
        values[idx] = value;
    }

    Ok(values)
}

pub(crate) fn encode_alp_rd_values(values: &[f64]) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alp_rd values must be non-empty",
        ));
    }

    let buffer = Buffer::copy_from(values);
    let array = PrimitiveArray::new(buffer, Validity::NonNullable);
    let encoder = RDEncoder::new(values);
    let encoded = encoder.encode(&array);

    let dict = encoded.left_parts_dictionary().as_slice();
    if dict.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd dictionary is empty",
        ));
    }

    let left_parts = encoded.left_parts().to_primitive();
    let right_parts = encoded.right_parts().to_primitive();
    let codes = read_u16_codes(&left_parts)?;
    let right = read_u64_parts(&right_parts)?;

    if codes.len() != right.len() || codes.len() != values.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd component length mismatch",
        ));
    }

    let right_bw = encoded.right_bit_width();
    if right_bw > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd right bit width invalid",
        ));
    }
    let code_bits = dict_code_bits(dict.len());

    let mut left_writer = BitWriter::new();
    if code_bits > 0 {
        for code in &codes {
            left_writer.write_bits(u64::from(*code), code_bits);
        }
    }
    let left_bytes = left_writer.finish();

    let mut right_writer = BitWriter::new();
    if right_bw > 0 {
        for value in &right {
            right_writer.write_bits(*value, right_bw);
        }
    }
    let right_bytes = right_writer.finish();

    let patches = extract_left_patches(encoded.left_parts_patches())?;

    let mut out = Vec::new();
    out.push(right_bw);
    encode_varint(dict.len() as u64, &mut out);
    for entry in dict {
        encode_varint(u64::from(*entry), &mut out);
    }
    encode_varint(patches.len() as u64, &mut out);
    for (idx, value) in patches {
        encode_varint(idx as u64, &mut out);
        encode_varint(u64::from(value), &mut out);
    }
    encode_varint(left_bytes.len() as u64, &mut out);
    out.extend_from_slice(&left_bytes);
    out.extend_from_slice(&right_bytes);
    Ok(out)
}

pub(crate) fn decode_alp_rd_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if buf.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "alp_rd header too short",
        ));
    }

    let mut cursor = 0usize;
    let right_bw = buf[cursor];
    cursor += 1;
    if right_bw > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd right bit width invalid",
        ));
    }

    let dict_len = decode_varint(buf, &mut cursor)? as usize;
    if dict_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd dictionary empty",
        ));
    }
    let mut dict = Vec::with_capacity(dict_len);
    for _ in 0..dict_len {
        let entry = decode_varint(buf, &mut cursor)? as u16;
        dict.push(entry);
    }

    let code_bits = dict_code_bits(dict_len);
    let patch_count = decode_varint(buf, &mut cursor)? as usize;
    let mut patches = Vec::with_capacity(patch_count);
    for _ in 0..patch_count {
        let idx = decode_varint(buf, &mut cursor)? as usize;
        let value = decode_varint(buf, &mut cursor)? as u16;
        patches.push((idx, value));
    }

    let left_len = decode_varint(buf, &mut cursor)? as usize;
    if cursor.saturating_add(left_len) > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "alp_rd left payload truncated",
        ));
    }
    let left_buf = &buf[cursor..cursor + left_len];
    cursor += left_len;
    let right_buf = &buf[cursor..];

    let mut left_parts = Vec::with_capacity(count);
    if code_bits == 0 {
        left_parts.resize(count, dict[0]);
    } else {
        let mut reader = BitReader::new(left_buf);
        for _ in 0..count {
            let code = reader.read_bits(code_bits)? as usize;
            if code >= dict_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "alp_rd code out of dictionary",
                ));
            }
            left_parts.push(dict[code]);
        }
    }

    for (idx, value) in patches {
        if idx >= left_parts.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "alp_rd patch index out of range",
            ));
        }
        left_parts[idx] = value;
    }

    let mut right_parts = Vec::with_capacity(count);
    if right_bw == 0 {
        right_parts.resize(count, 0u64);
    } else {
        let mut reader = BitReader::new(right_buf);
        for _ in 0..count {
            right_parts.push(reader.read_bits(right_bw)?);
        }
    }

    let mut values = Vec::with_capacity(count);
    for (left, right) in left_parts.into_iter().zip(right_parts) {
        let bits = (u64::from(left) << right_bw) | right;
        values.push(f64::from_bits(bits));
    }
    Ok(values)
}

fn encode_f64(value: f64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_f64(buf: &[u8], cursor: &mut usize) -> io::Result<f64> {
    if cursor.saturating_add(8) > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short f64"));
    }
    let value = f64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn min_max_i64(values: &[i64]) -> io::Result<(i64, i64)> {
    let mut iter = values.iter();
    let Some(first) = iter.next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alp values must be non-empty",
        ));
    };
    let mut min = *first;
    let mut max = *first;
    for value in iter {
        if *value < min {
            min = *value;
        }
        if *value > max {
            max = *value;
        }
    }
    Ok((min, max))
}

fn bit_width_u128(value: u128) -> u8 {
    if value == 0 {
        0
    } else {
        (128u32.saturating_sub(value.leading_zeros())) as u8
    }
}

fn dict_code_bits(len: usize) -> u8 {
    if len <= 1 {
        0
    } else {
        let max_code = (len - 1) as u64;
        (64u32.saturating_sub(max_code.leading_zeros())) as u8
    }
}

fn read_u16_codes(array: &PrimitiveArray) -> io::Result<Vec<u16>> {
    match array.ptype() {
        PType::U8 => Ok(array.as_slice::<u8>().iter().map(|&v| v as u16).collect()),
        PType::U16 => Ok(array.as_slice::<u16>().to_vec()),
        PType::U32 => {
            let mut out = Vec::with_capacity(array.len());
            for &value in array.as_slice::<u32>() {
                if value > u32::from(u16::MAX) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "alp_rd code overflows u16",
                    ));
                }
                out.push(value as u16);
            }
            Ok(out)
        }
        PType::U64 => {
            let mut out = Vec::with_capacity(array.len());
            for &value in array.as_slice::<u64>() {
                if value > u64::from(u16::MAX) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "alp_rd code overflows u16",
                    ));
                }
                out.push(value as u16);
            }
            Ok(out)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd codes must be unsigned ints",
        )),
    }
}

fn read_u64_parts(array: &PrimitiveArray) -> io::Result<Vec<u64>> {
    match array.ptype() {
        PType::U8 => Ok(array.as_slice::<u8>().iter().map(|&v| v as u64).collect()),
        PType::U16 => Ok(array.as_slice::<u16>().iter().map(|&v| v as u64).collect()),
        PType::U32 => Ok(array.as_slice::<u32>().iter().map(|&v| v as u64).collect()),
        PType::U64 => Ok(array.as_slice::<u64>().to_vec()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd parts must be unsigned ints",
        )),
    }
}

fn extract_left_patches(
    patches: Option<&vortex_array::patches::Patches>,
) -> io::Result<Vec<(usize, u16)>> {
    let Some(patches) = patches else {
        return Ok(Vec::new());
    };

    let indices = patches.indices().to_primitive();
    let values = patches.values().to_primitive();
    let indices = read_u64_parts(&indices)?;
    let values = read_u16_codes(&values)?;
    if indices.len() != values.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd patch arrays mismatch",
        ));
    }
    let offset = patches.offset() as u64;
    let mut out = Vec::with_capacity(indices.len());
    for (idx, value) in indices.into_iter().zip(values) {
        if idx < offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "alp_rd patch index underflow",
            ));
        }
        out.push(((idx - offset) as usize, value));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alp_roundtrip() {
        let values = vec![
            1.234,
            2_718.0 / 1_000.0,
            std::f64::consts::PI,
            4.0,
            -123.456,
        ];
        let encoded = encode_alp_values(&values).unwrap();
        let decoded = decode_alp_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }

    #[test]
    fn alp_rd_roundtrip() {
        let values = vec![1.0, 1.5, -2.25, 1000.125, 3e100];
        let encoded = encode_alp_rd_values(&values).unwrap();
        let decoded = decode_alp_rd_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }
}
