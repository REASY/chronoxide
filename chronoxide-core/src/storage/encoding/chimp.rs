use std::io;

use crate::storage::encoding::bitstream::{BitReader, BitWriter};

const CHIMP_CENTER_BITS: u8 = 6;
const CHIMP128_RING_SIZE: usize = 128;
const CHIMP128_INDEX_BITS: u8 = 7;
const CHIMP128_HASH_BITS: usize = 14;
const CHIMP128_HASH_SIZE: usize = 1 << CHIMP128_HASH_BITS;
const CHIMP128_HASH_MASK: u64 = (1u64 << CHIMP128_HASH_BITS) - 1;
const CHIMP128_TRAIL_THRESHOLD: u32 = CHIMP128_INDEX_BITS as u32 + CHIMP_CENTER_BITS as u32;
const INVALID_INDEX: u32 = u32::MAX;
const BASELINE_LEADING_BITS: u8 = 3;
const BASELINE_TRAILING_THRESHOLD: u32 = 6;

const FLAG_VALUE_IDENTICAL: u8 = 0;
const FLAG_TRAILING_EXCEEDS: u8 = 1;
const FLAG_LEADING_EQUAL: u8 = 2;
const FLAG_LEADING_LOAD: u8 = 3;

const CHIMP_LEADING_STEPS: [u8; 8] = [0, 8, 12, 16, 18, 20, 22, 24];
const CHIMP_LEADING_ROUND: [u8; 64] = [
    0, 0, 0, 0, 0, 0, 0, 0, // 0-7
    8, 8, 8, 8, // 8-11
    12, 12, 12, 12, // 12-15
    16, 16, // 16-17
    18, 18, // 18-19
    20, 20, // 20-21
    22, 22, // 22-23
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, // 24-33
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, // 34-43
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, // 44-53
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, // 54-63
];
const CHIMP_LEADING_REPRESENTATION: [u8; 64] = [
    0, 0, 0, 0, 0, 0, 0, 0, // 0-7
    1, 1, 1, 1, // 8-11
    2, 2, 2, 2, // 12-15
    3, 3, // 16-17
    4, 4, // 18-19
    5, 5, // 20-21
    6, 6, // 22-23
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, // 24-33
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, // 34-43
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, // 44-53
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, // 54-63
];

#[derive(Debug, Default)]
struct FlagBuffer {
    bytes: Vec<u8>,
    count: u32,
}

impl FlagBuffer {
    fn push(&mut self, flag: u8) {
        let idx = self.count as usize;
        let byte_idx = idx / 4;
        let shift = 6 - ((idx % 4) * 2);
        if byte_idx == self.bytes.len() {
            self.bytes.push(0);
        }
        self.bytes[byte_idx] |= (flag & 0x3) << shift;
        self.count = self.count.saturating_add(1);
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn len_bytes(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Debug, Default)]
struct LeadingZeroBuffer {
    bytes: Vec<u8>,
    current: u32,
    count: u32,
}

impl LeadingZeroBuffer {
    fn push(&mut self, code: u8) {
        let shift = (self.count & 7) * 3;
        self.current |= u32::from(code & 0x7) << shift;
        self.count = self.count.saturating_add(1);
        if self.count & 7 == 0 {
            self.flush_current();
        }
    }

    fn into_bytes(mut self) -> Vec<u8> {
        if self.count & 7 != 0 {
            self.flush_current();
        }
        self.bytes
    }

    fn len_bytes(&self) -> usize {
        if self.count & 7 == 0 {
            self.bytes.len()
        } else {
            self.bytes.len().saturating_add(3)
        }
    }

    fn flush_current(&mut self) {
        self.bytes
            .extend_from_slice(&self.current.to_le_bytes()[..3]);
        self.current = 0;
    }
}

// Based on DucksDB version of Chimp128, https://github.com/duckdb/duckdb/blob/6bd11755cdefa8a8eb38fba1bbaa1f77eb62816d/src/include/duckdb/storage/compression/chimp/chimp.hpp
#[derive(Debug)]
pub(crate) struct Chimp128DuckDBEncoder {
    payload: BitWriter,
    flags: FlagBuffer,
    leading: LeadingZeroBuffer,
    packed: Vec<u16>,
    prev_lead_code: u8,
    prev_lead_valid: bool,
    ring: [u64; CHIMP128_RING_SIZE],
    table: [u32; CHIMP128_HASH_SIZE],
    index: u32,
    has_prev: bool,
}

impl Chimp128DuckDBEncoder {
    pub(crate) fn new() -> Self {
        Self {
            payload: BitWriter::new(),
            flags: FlagBuffer::default(),
            leading: LeadingZeroBuffer::default(),
            packed: Vec::new(),
            prev_lead_code: 0,
            prev_lead_valid: false,
            ring: [0u64; CHIMP128_RING_SIZE],
            table: [INVALID_INDEX; CHIMP128_HASH_SIZE],
            index: 0,
            has_prev: false,
        }
    }

    pub(crate) fn push(&mut self, value: f64) -> io::Result<()> {
        let bits = value.to_bits();
        if !self.has_prev {
            self.payload.write_bits(bits, 64);
            self.has_prev = true;
            self.ring[0] = bits;
            self.table[(bits & CHIMP128_HASH_MASK) as usize] = 0;
            self.index = 0;
            return Ok(());
        }

        let current_index = self.index;
        let mut reference_index = (current_index & (CHIMP128_RING_SIZE as u32 - 1)) as u8;
        let mut xor_result = self.ring[reference_index as usize] ^ bits;
        let mut trailing_exceeds = false;

        if let Some(candidate_index) = self.find_candidate(bits) {
            let candidate_slot = (candidate_index & (CHIMP128_RING_SIZE as u32 - 1)) as u8;
            let candidate_bits = self.ring[candidate_slot as usize];
            let candidate_xor = candidate_bits ^ bits;
            if candidate_xor.trailing_zeros() > CHIMP128_TRAIL_THRESHOLD {
                trailing_exceeds = true;
                reference_index = candidate_slot;
                xor_result = candidate_xor;
            }
        }

        if xor_result == 0 {
            self.flags.push(FLAG_VALUE_IDENTICAL);
            self.payload
                .write_bits(u64::from(reference_index), CHIMP128_INDEX_BITS);
            self.prev_lead_valid = false;
        } else if trailing_exceeds {
            let trailing = xor_result.trailing_zeros();
            let (lead_code, lead_rounded) = encode_leading(xor_result.leading_zeros() as u8);
            let sig_bits = 64u32
                .saturating_sub(u32::from(lead_rounded))
                .saturating_sub(trailing);
            if sig_bits == 0 || sig_bits > 63 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chimp significant bit count invalid",
                ));
            }
            self.flags.push(FLAG_TRAILING_EXCEEDS);
            self.packed
                .push(pack_meta(reference_index, lead_code, sig_bits as u8));
            self.payload
                .write_bits(xor_result >> trailing, sig_bits as u8);
            self.prev_lead_valid = false;
        } else {
            let (lead_code, lead_rounded) = encode_leading(xor_result.leading_zeros() as u8);
            let sig_bits = 64u32.saturating_sub(u32::from(lead_rounded));
            if sig_bits == 0 || sig_bits > 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chimp significant bit count invalid",
                ));
            }
            if self.prev_lead_valid && self.prev_lead_code == lead_code {
                self.flags.push(FLAG_LEADING_EQUAL);
                self.payload.write_bits(xor_result, sig_bits as u8);
            } else {
                self.flags.push(FLAG_LEADING_LOAD);
                self.leading.push(lead_code);
                self.payload.write_bits(xor_result, sig_bits as u8);
                self.prev_lead_code = lead_code;
                self.prev_lead_valid = true;
            }
        }

        let next_index = current_index.wrapping_add(1);
        let slot = (next_index & (CHIMP128_RING_SIZE as u32 - 1)) as usize;
        self.ring[slot] = bits;
        self.table[(bits & CHIMP128_HASH_MASK) as usize] = next_index;
        self.index = next_index;
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len_bytes());
        out.extend_from_slice(&self.flags.into_bytes());
        out.extend_from_slice(&self.leading.into_bytes());
        for value in self.packed {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&self.payload.finish());
        out
    }

    pub(crate) fn len_bytes(&self) -> usize {
        self.flags
            .len_bytes()
            .saturating_add(self.leading.len_bytes())
            .saturating_add(self.packed.len().saturating_mul(2))
            .saturating_add(self.payload.len_bytes())
    }

    fn find_candidate(&self, bits: u64) -> Option<u32> {
        let hash = (bits & CHIMP128_HASH_MASK) as usize;
        let cand_index = self.table[hash];
        if cand_index == INVALID_INDEX {
            return None;
        }
        if self.index.wrapping_sub(cand_index) >= CHIMP128_RING_SIZE as u32 {
            return None;
        }
        Some(cand_index)
    }
}

impl Default for Chimp128DuckDBEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct Chimp128BaselineEncoder {
    writer: BitWriter,
    prev: Option<u64>,
    prev_lead_code: u8,
    ring: [u64; CHIMP128_RING_SIZE],
    table: [u32; CHIMP128_HASH_SIZE],
    index: u32,
}

impl Chimp128BaselineEncoder {
    pub(crate) fn new() -> Self {
        Self {
            writer: BitWriter::new(),
            prev: None,
            prev_lead_code: 0,
            ring: [0u64; CHIMP128_RING_SIZE],
            table: [INVALID_INDEX; CHIMP128_HASH_SIZE],
            index: 0,
        }
    }

    pub(crate) fn push(&mut self, value: f64) -> io::Result<()> {
        let bits = value.to_bits();
        if self.prev.is_none() {
            self.writer.write_bits(bits, 64);
            self.prev = Some(bits);
            self.insert_value(bits);
            return Ok(());
        }

        let mut use_cached = false;
        let mut cached_bits = 0u64;
        let mut cached_slot = 0u8;

        if let Some((slot, candidate)) = self.find_candidate(bits) {
            let xor = candidate ^ bits;
            let trailing = xor.trailing_zeros();
            if trailing > CHIMP128_TRAIL_THRESHOLD {
                use_cached = true;
                cached_bits = candidate;
                cached_slot = slot;
            }
        }

        let prev_bits = if use_cached {
            cached_bits
        } else {
            self.prev.expect("prev must be set after first value")
        };

        self.writer.write_bit(use_cached);
        if use_cached {
            self.writer
                .write_bits(u64::from(cached_slot), CHIMP128_INDEX_BITS);
        }
        let xor = prev_bits ^ bits;
        encode_chimp_xor_baseline(&mut self.writer, xor, &mut self.prev_lead_code)?;

        self.prev = Some(bits);
        self.insert_value(bits);
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.writer.finish()
    }

    fn insert_value(&mut self, bits: u64) {
        let idx = self.index;
        let slot = (idx & (CHIMP128_RING_SIZE as u32 - 1)) as usize;
        self.ring[slot] = bits;
        let hash = (bits & CHIMP128_HASH_MASK) as usize;
        self.table[hash] = idx;
        self.index = idx.wrapping_add(1);
    }

    fn find_candidate(&self, bits: u64) -> Option<(u8, u64)> {
        let hash = (bits & CHIMP128_HASH_MASK) as usize;
        let cand_index = self.table[hash];
        if cand_index == INVALID_INDEX {
            return None;
        }
        let current = self.index;
        if current.wrapping_sub(cand_index) > CHIMP128_RING_SIZE as u32 {
            return None;
        }
        let slot = (cand_index & (CHIMP128_RING_SIZE as u32 - 1)) as u8;
        Some((slot, self.ring[slot as usize]))
    }
}

impl Default for Chimp128BaselineEncoder {
    fn default() -> Self {
        Self::new()
    }
}

fn encode_chimp_xor_baseline(
    writer: &mut BitWriter,
    xor: u64,
    prev_lead_code: &mut u8,
) -> io::Result<()> {
    let trailing = xor.trailing_zeros();
    if trailing > BASELINE_TRAILING_THRESHOLD {
        writer.write_bit(false);
        if xor == 0 {
            writer.write_bit(false);
            return Ok(());
        }

        writer.write_bit(true);
        let lead_actual = xor.leading_zeros() as u8;
        let (lead_code, lead_encoded) = encode_leading_baseline(lead_actual);
        writer.write_bits(u64::from(lead_code), BASELINE_LEADING_BITS);
        let center_bits = 64u32
            .saturating_sub(u32::from(lead_encoded))
            .saturating_sub(trailing);
        if center_bits == 0 || center_bits > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chimp center bit count invalid",
            ));
        }
        writer.write_bits(u64::from(center_bits as u8), CHIMP_CENTER_BITS);
        let payload = xor >> trailing;
        writer.write_bits(payload, center_bits as u8);
        *prev_lead_code = lead_code;
    } else {
        writer.write_bit(true);
        let lead_actual = xor.leading_zeros() as u8;
        let (lead_code, lead_encoded) = encode_leading_baseline(lead_actual);
        let sig_bits = 64u32.saturating_sub(u32::from(lead_encoded));
        if sig_bits == 0 || sig_bits > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chimp significant bit count invalid",
            ));
        }
        if *prev_lead_code == lead_code {
            writer.write_bit(false);
            writer.write_bits(xor, sig_bits as u8);
        } else {
            writer.write_bit(true);
            writer.write_bits(u64::from(lead_code), BASELINE_LEADING_BITS);
            writer.write_bits(xor, sig_bits as u8);
            *prev_lead_code = lead_code;
        }
    }

    Ok(())
}

fn decode_chimp_xor_baseline(reader: &mut BitReader, prev_lead_code: &mut u8) -> io::Result<u64> {
    let control = reader.read_bit()?;
    if control == 0 {
        let control2 = reader.read_bit()?;
        if control2 == 0 {
            return Ok(0);
        }

        let lead_code = reader.read_bits(BASELINE_LEADING_BITS)? as u8;
        let lead = decode_leading_baseline(lead_code)?;
        let center_bits = reader.read_bits(CHIMP_CENTER_BITS)? as u32;
        if center_bits == 0 || center_bits > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chimp center bit count invalid",
            ));
        }
        let trailing = 64u32
            .saturating_sub(u32::from(lead))
            .saturating_sub(center_bits);
        if trailing > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chimp trailing bit count invalid",
            ));
        }
        let payload = reader.read_bits(center_bits as u8)?;
        let xor = payload << trailing;
        *prev_lead_code = lead_code;
        Ok(xor)
    } else {
        let control2 = reader.read_bit()?;
        let lead = if control2 == 0 {
            decode_leading_baseline(*prev_lead_code)?
        } else {
            let code = reader.read_bits(BASELINE_LEADING_BITS)? as u8;
            let lead = decode_leading_baseline(code)?;
            *prev_lead_code = code;
            lead
        };
        let sig_bits = 64u32.saturating_sub(u32::from(lead));
        if sig_bits == 0 || sig_bits > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chimp significant bit count invalid",
            ));
        }
        let payload = reader.read_bits(sig_bits as u8)?;
        Ok(payload)
    }
}

fn encode_leading_baseline(lead: u8) -> (u8, u8) {
    let mut code = 0u8;
    for (idx, step) in CHIMP_LEADING_STEPS.iter().enumerate() {
        if *step <= lead {
            code = idx as u8;
        } else {
            break;
        }
    }
    (code, CHIMP_LEADING_STEPS[code as usize])
}

fn decode_leading_baseline(code: u8) -> io::Result<u8> {
    CHIMP_LEADING_STEPS
        .get(code as usize)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chimp lead code invalid"))
}

#[allow(dead_code)]
pub(crate) fn encode_chimp128_baseline_values(values: &[f64]) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "values must be non-empty",
        ));
    }

    let mut encoder = Chimp128BaselineEncoder::new();
    for value in values {
        encoder.push(*value)?;
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_chimp128_baseline_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut reader = BitReader::new(buf);
    let first = reader.read_bits(64)?;
    let mut values = Vec::with_capacity(count);
    values.push(f64::from_bits(first));

    let mut prev = first;
    let mut prev_lead_code = 0u8;
    let mut ring = [0u64; CHIMP128_RING_SIZE];
    ring[0] = first;
    let mut index: u32 = 1;

    for _ in 1..count {
        let use_cached = reader.read_bit()? == 1;
        let prev_bits = if use_cached {
            let slot = reader.read_bits(CHIMP128_INDEX_BITS)? as usize;
            if slot >= CHIMP128_RING_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chimp128 cache slot out of range",
                ));
            }
            ring[slot]
        } else {
            prev
        };

        let xor = decode_chimp_xor_baseline(&mut reader, &mut prev_lead_code)?;
        let next = prev_bits ^ xor;
        values.push(f64::from_bits(next));

        let slot = (index & (CHIMP128_RING_SIZE as u32 - 1)) as usize;
        ring[slot] = next;
        prev = next;
        index = index.wrapping_add(1);
    }

    Ok(values)
}

#[allow(dead_code)]
pub(crate) fn encode_chimp128_duckdb_values(values: &[f64]) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "values must be non-empty",
        ));
    }

    let mut encoder = Chimp128DuckDBEncoder::new();
    for value in values {
        encoder.push(*value)?;
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_chimp128_duckdb_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let flags_count = count.saturating_sub(1);
    let flags_len = flags_count.saturating_add(3) / 4;
    if buf.len() < flags_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "chimp128 flag buffer truncated",
        ));
    }
    let flags_bytes = &buf[..flags_len];
    let flags = decode_flags(flags_bytes, flags_count)?;
    let leading_count = flags
        .iter()
        .filter(|flag| **flag == FLAG_LEADING_LOAD)
        .count();
    let packed_count = flags
        .iter()
        .filter(|flag| **flag == FLAG_TRAILING_EXCEEDS)
        .count();
    let leading_len = leading_count.div_ceil(8).saturating_mul(3);
    let packed_len = packed_count.saturating_mul(2);
    let meta_len = flags_len
        .saturating_add(leading_len)
        .saturating_add(packed_len);
    if buf.len() < meta_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "chimp128 metadata buffer truncated",
        ));
    }

    let leading_bytes = &buf[flags_len..flags_len + leading_len];
    let packed_bytes = &buf[flags_len + leading_len..meta_len];
    let payload = &buf[meta_len..];
    let mut leading_reader = LeadingZeroReader::new(leading_bytes, leading_count);
    let mut packed_reader = PackedDataReader::new(packed_bytes, packed_count);
    let mut reader = BitReader::new(payload);

    let first = reader.read_bits(64)?;
    let mut values = Vec::with_capacity(count);
    values.push(f64::from_bits(first));
    let mut prev = first;
    let mut prev_lead_code = 0u8;
    let mut prev_lead_valid = false;
    let mut ring = [0u64; CHIMP128_RING_SIZE];
    ring[0] = first;
    let mut index: u32 = 0;

    for flag in flags {
        let next = match flag {
            FLAG_VALUE_IDENTICAL => {
                let slot = reader.read_bits(CHIMP128_INDEX_BITS)? as usize;
                if slot >= CHIMP128_RING_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chimp128 cache slot out of range",
                    ));
                }
                prev_lead_valid = false;
                ring[slot]
            }
            FLAG_TRAILING_EXCEEDS => {
                let packed = packed_reader.read()?;
                let (slot, lead_code, sig_bits) = unpack_meta(packed)?;
                if slot as usize >= CHIMP128_RING_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chimp128 packed slot out of range",
                    ));
                }
                let lead = decode_leading(lead_code)?;
                let sig_bits = if sig_bits == 0 { 64 } else { sig_bits };
                let trailing = 64u32
                    .saturating_sub(u32::from(lead))
                    .saturating_sub(u32::from(sig_bits));
                if trailing > 64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chimp trailing bit count invalid",
                    ));
                }
                let payload = reader.read_bits(sig_bits)?;
                prev_lead_valid = false;
                ring[slot as usize] ^ (payload << trailing)
            }
            FLAG_LEADING_EQUAL => {
                if !prev_lead_valid {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chimp leading zeros missing",
                    ));
                }
                let lead = decode_leading(prev_lead_code)?;
                let sig_bits = 64u32.saturating_sub(u32::from(lead));
                if sig_bits == 0 || sig_bits > 64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chimp significant bit count invalid",
                    ));
                }
                let payload = reader.read_bits(sig_bits as u8)?;
                prev ^ payload
            }
            FLAG_LEADING_LOAD => {
                let lead_code = leading_reader.read()?;
                let lead = decode_leading(lead_code)?;
                let sig_bits = 64u32.saturating_sub(u32::from(lead));
                if sig_bits == 0 || sig_bits > 64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chimp significant bit count invalid",
                    ));
                }
                let payload = reader.read_bits(sig_bits as u8)?;
                prev_lead_code = lead_code;
                prev_lead_valid = true;
                prev ^ payload
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chimp flag invalid",
                ));
            }
        };

        values.push(f64::from_bits(next));
        let next_index = index.wrapping_add(1);
        let slot = (next_index & (CHIMP128_RING_SIZE as u32 - 1)) as usize;
        ring[slot] = next;
        prev = next;
        index = next_index;
    }

    Ok(values)
}

fn decode_flags(buf: &[u8], count: usize) -> io::Result<Vec<u8>> {
    let expected = count.saturating_add(3) / 4;
    if buf.len() < expected {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "chimp flags buffer truncated",
        ));
    }
    let mut flags = Vec::with_capacity(count);
    for idx in 0..count {
        let byte = buf[idx / 4];
        let shift = 6 - ((idx % 4) * 2);
        let flag = (byte >> shift) & 0x3;
        flags.push(flag);
    }
    Ok(flags)
}

fn encode_leading(raw: u8) -> (u8, u8) {
    let raw = raw.min(63);
    let lead_rounded = CHIMP_LEADING_ROUND[raw as usize];
    let lead_code = CHIMP_LEADING_REPRESENTATION[lead_rounded as usize];
    (lead_code, lead_rounded)
}

fn decode_leading(code: u8) -> io::Result<u8> {
    CHIMP_LEADING_STEPS
        .get(code as usize)
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chimp lead code invalid"))
}

fn pack_meta(index: u8, lead_code: u8, sig_bits: u8) -> u16 {
    ((u16::from(index & 0x7f)) << 9)
        | ((u16::from(lead_code & 0x7)) << 6)
        | u16::from(sig_bits & 0x3f)
}

fn unpack_meta(value: u16) -> io::Result<(u8, u8, u8)> {
    let index = ((value >> 9) & 0x7f) as u8;
    let lead_code = ((value >> 6) & 0x7) as u8;
    let sig_bits = (value & 0x3f) as u8;
    if sig_bits > 63 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chimp significant bit count invalid",
        ));
    }
    Ok((index, lead_code, sig_bits))
}

struct LeadingZeroReader<'a> {
    bytes: &'a [u8],
    index: usize,
    count: usize,
    current: u32,
}

impl<'a> LeadingZeroReader<'a> {
    fn new(bytes: &'a [u8], count: usize) -> Self {
        Self {
            bytes,
            index: 0,
            count,
            current: 0,
        }
    }

    fn read(&mut self) -> io::Result<u8> {
        if self.index >= self.count {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "chimp leading buffer exhausted",
            ));
        }
        let block = self.index / 8;
        let offset = (self.index % 8) * 3;
        if offset == 0 {
            let base = block.saturating_mul(3);
            if base.saturating_add(3) > self.bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "chimp leading buffer truncated",
                ));
            }
            let mut tmp = [0u8; 4];
            tmp[..3].copy_from_slice(&self.bytes[base..base + 3]);
            self.current = u32::from_le_bytes(tmp);
        }
        let code = ((self.current >> offset) & 0x7) as u8;
        self.index = self.index.saturating_add(1);
        Ok(code)
    }
}

struct PackedDataReader<'a> {
    bytes: &'a [u8],
    index: usize,
    count: usize,
}

impl<'a> PackedDataReader<'a> {
    fn new(bytes: &'a [u8], count: usize) -> Self {
        Self {
            bytes,
            index: 0,
            count,
        }
    }

    fn read(&mut self) -> io::Result<u16> {
        if self.index >= self.count {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "chimp packed buffer exhausted",
            ));
        }
        let base = self.index.saturating_mul(2);
        if base.saturating_add(2) > self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "chimp packed buffer truncated",
            ));
        }
        let value = u16::from_le_bytes([self.bytes[base], self.bytes[base + 1]]);
        self.index = self.index.saturating_add(1);
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chimp128_duckdb_roundtrip_values() {
        let values = vec![1.0, 2.0, 1.0, 1.5, -2.25, 1000.125];
        let encoded = encode_chimp128_duckdb_values(&values).unwrap();
        let decoded = decode_chimp128_duckdb_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }

    #[test]
    fn chimp128_duckdb_trailing_small_path() {
        let base = f64::from_bits(0x3ff0_0000_0000_0000);
        let flipped = f64::from_bits(base.to_bits() ^ 1);
        let values = vec![base, flipped, base];
        let encoded = encode_chimp128_duckdb_values(&values).unwrap();
        let decoded = decode_chimp128_duckdb_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }

    #[test]
    fn chimp128_duckdb_roundtrip_large_sequence() {
        let mut values = Vec::with_capacity(8192);
        let mut state = 0x1234_5678_9abc_def0u64;
        for i in 0..8192u64 {
            if i % 5 == 0 {
                if let Some(prev) = values.last().copied() {
                    values.push(prev);
                    continue;
                }
            }
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.push(f64::from_bits(state));
        }
        let encoded = encode_chimp128_duckdb_values(&values).unwrap();
        let decoded = decode_chimp128_duckdb_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }

    #[test]
    fn chimp128_baseline_roundtrip_values() {
        let values = vec![1.0, 2.0, 1.0, 1.5, -2.25, 1000.125];
        let encoded = encode_chimp128_baseline_values(&values).unwrap();
        let decoded = decode_chimp128_baseline_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }

    #[test]
    fn chimp128_baseline_trailing_small_path() {
        let base = f64::from_bits(0x3ff0_0000_0000_0000);
        let flipped = f64::from_bits(base.to_bits() ^ 1);
        let values = vec![base, flipped, base];
        let encoded = encode_chimp128_baseline_values(&values).unwrap();
        let decoded = decode_chimp128_baseline_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }

    #[test]
    fn chimp128_baseline_roundtrip_large_sequence() {
        let mut values = Vec::with_capacity(8192);
        let mut state = 0x1234_5678_9abc_def0u64;
        for i in 0..8192u64 {
            if i % 5 == 0 {
                if let Some(prev) = values.last().copied() {
                    values.push(prev);
                    continue;
                }
            }
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.push(f64::from_bits(state));
        }
        let encoded = encode_chimp128_baseline_values(&values).unwrap();
        let decoded = decode_chimp128_baseline_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }
}
