use std::io;

use alp::{ALPFloat as SpiralFloat, Exponents as SpiralExponents, RDEncoder as SpiralRdEncoder};

use crate::storage::encoding::bitstream::{BitReader, BitWriter};
use crate::storage::encoding::{
    decode_varint, decode_zigzag_i64, encode_varint, encode_zigzag_i64,
};

#[derive(Debug)]
pub(crate) struct AlpSpiralEncoder {
    values: Vec<f64>,
}

impl AlpSpiralEncoder {
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
        encode_alp_spiral_values(&self.values).map(|buf| buf.len())
    }

    pub(crate) fn snapshot(&self) -> io::Result<Vec<u8>> {
        encode_alp_spiral_values(&self.values)
    }

    pub(crate) fn finish(self) -> io::Result<Vec<u8>> {
        encode_alp_spiral_values(&self.values)
    }
}

impl Default for AlpSpiralEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct AlpRdSpiralEncoder {
    values: Vec<f64>,
}

impl AlpRdSpiralEncoder {
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
        encode_alp_rd_spiral_values(&self.values).map(|buf| buf.len())
    }

    pub(crate) fn snapshot(&self) -> io::Result<Vec<u8>> {
        encode_alp_rd_spiral_values(&self.values)
    }

    pub(crate) fn finish(self) -> io::Result<Vec<u8>> {
        encode_alp_rd_spiral_values(&self.values)
    }
}

impl Default for AlpRdSpiralEncoder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn encode_alp_spiral_values(values: &[f64]) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alp_spiral values must be non-empty",
        ));
    }

    let (exp, encoded, patch_indices, patch_values) = <f64 as SpiralFloat>::encode(values, None);

    if patch_indices.len() != patch_values.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_spiral patch arrays mismatch",
        ));
    }

    let (min, max) = min_max_i64(&encoded)?;
    let range = (max as i128 - min as i128) as u128;
    let bits = bit_width_u128(range);
    if bits > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_spiral bit width too large",
        ));
    }

    let mut writer = BitWriter::new();
    if bits > 0 {
        for &value in &encoded {
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

pub(crate) fn decode_alp_spiral_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if buf.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "alp_spiral header too short",
        ));
    }

    let mut cursor = 0usize;
    let exp = SpiralExponents {
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
            "alp_spiral bit width invalid",
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
        values.push(<f64 as SpiralFloat>::decode_single(*encoded, exp));
    }
    for (idx, value) in patches {
        if idx >= values.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "alp_spiral patch index out of range",
            ));
        }
        values[idx] = value;
    }

    Ok(values)
}

pub(crate) fn encode_alp_rd_spiral_values(values: &[f64]) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alp_rd_spiral values must be non-empty",
        ));
    }

    let encoder = SpiralRdEncoder::new(values);
    let split = encoder.split(values);
    let (left_parts, left_dict, _exceptions, right_parts, right_bw) = split.into_parts();

    if left_parts.len() != values.len() || right_parts.len() != values.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd_spiral component length mismatch",
        ));
    }
    if left_dict.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd_spiral dictionary empty",
        ));
    }

    let right_bw = right_bw;
    if right_bw > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd_spiral right bit width invalid",
        ));
    }

    let mut patches = Vec::new();
    let mut patch_values = Vec::new();
    for (idx, value) in values.iter().enumerate() {
        let left_actual = (value.to_bits() >> right_bw) as u16;
        let code = left_parts[idx] as usize;
        if code >= left_dict.len() || left_dict[code] != left_actual {
            patches.push(idx as u64);
            patch_values.push(left_actual);
        }
    }

    let code_bits = dict_code_bits(left_dict.len());
    let mut left_writer = BitWriter::new();
    if code_bits > 0 {
        for code in &left_parts {
            left_writer.write_bits(u64::from(*code), code_bits);
        }
    }
    let left_bytes = left_writer.finish();

    let mut right_writer = BitWriter::new();
    if right_bw > 0 {
        for value in &right_parts {
            right_writer.write_bits(*value, right_bw);
        }
    }
    let right_bytes = right_writer.finish();

    let mut out = Vec::new();
    out.push(right_bw);
    encode_varint(left_dict.len() as u64, &mut out);
    for entry in &left_dict {
        encode_varint(u64::from(*entry), &mut out);
    }
    encode_varint(patches.len() as u64, &mut out);
    for (idx, value) in patches.into_iter().zip(patch_values.into_iter()) {
        encode_varint(idx, &mut out);
        encode_varint(u64::from(value), &mut out);
    }
    encode_varint(left_bytes.len() as u64, &mut out);
    out.extend_from_slice(&left_bytes);
    out.extend_from_slice(&right_bytes);
    Ok(out)
}

pub(crate) fn decode_alp_rd_spiral_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if buf.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "alp_rd_spiral header too short",
        ));
    }

    let mut cursor = 0usize;
    let right_bw = buf[cursor];
    cursor += 1;
    if right_bw > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd_spiral right bit width invalid",
        ));
    }

    let dict_len = decode_varint(buf, &mut cursor)? as usize;
    if dict_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "alp_rd_spiral dictionary empty",
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
            "alp_rd_spiral left payload truncated",
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
                    "alp_rd_spiral code out of dictionary",
                ));
            }
            left_parts.push(dict[code]);
        }
    }

    for (idx, value) in patches {
        if idx >= left_parts.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "alp_rd_spiral patch index out of range",
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
    for (left, right) in left_parts.into_iter().zip(right_parts.into_iter()) {
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
            "alp_spiral values must be non-empty",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alp_spiral_roundtrip() {
        let values = vec![1.234, 2.718, std::f64::consts::PI, 4.0, -123.456];
        let encoded = encode_alp_spiral_values(&values).unwrap();
        let decoded = decode_alp_spiral_values(&encoded, values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn alp_rd_spiral_roundtrip() {
        let values = vec![1.12345, 2.34567, 3.45678, 4.0, -5.5, 12345.0];
        let encoded = encode_alp_rd_spiral_values(&values).unwrap();
        let decoded = decode_alp_rd_spiral_values(&encoded, values.len()).unwrap();
        assert_eq!(decoded, values);
    }
}
