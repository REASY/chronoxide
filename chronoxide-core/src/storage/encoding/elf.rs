use std::io;

use crate::storage::encoding::bitstream::{BitReader, BitWriter};

const LEADING_REPRESENTATION: [u8; 64] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
];
const LEADING_ROUND: [u8; 64] = [
    0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 12, 12, 12, 12, 16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
];
const LEADING_DECODE: [u8; 8] = [0, 8, 12, 16, 18, 20, 22, 24];
const ELF_CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

#[derive(Debug)]
pub(crate) struct ElfEncoder {
    xor: ElfXorCompressor,
    last_beta_star: i32,
}

impl ElfEncoder {
    pub(crate) fn new() -> Self {
        Self {
            xor: ElfXorCompressor::new(),
            last_beta_star: i32::MAX,
        }
    }

    pub(crate) fn push(&mut self, value: f64) -> io::Result<()> {
        let value_bits = value.to_bits();
        let value_prime = if value == 0.0 || value.is_infinite() {
            self.xor.write_bits(2, 2);
            value_bits
        } else if value.is_nan() {
            self.xor.write_bits(2, 2);
            ELF_CANONICAL_NAN_BITS
        } else {
            let (alpha, beta_star) =
                Elf64Utils::get_alpha_and_beta_star(value, self.last_beta_star);
            let exponent = ((value_bits >> 52) & 0x7ff) as i32;
            let g_alpha = Elf64Utils::get_f_alpha(alpha) + exponent - 1023;
            let erase_bits = 52 - g_alpha;
            if erase_bits > 4 && erase_bits < 64 {
                let mask = !0u64 << erase_bits;
                let delta = (!mask) & value_bits;
                if delta != 0 {
                    if beta_star == self.last_beta_star {
                        self.xor.write_bit(false);
                    } else {
                        let header = (beta_star as u64) | 0x30;
                        self.xor.write_bits(header, 6);
                        self.last_beta_star = beta_star;
                    }
                    value_bits & mask
                } else {
                    self.xor.write_bits(2, 2);
                    value_bits
                }
            } else {
                self.xor.write_bits(2, 2);
                value_bits
            }
        };

        self.xor.add_value(value_prime)?;
        Ok(())
    }

    pub(crate) fn len_bytes(&self) -> usize {
        self.xor.len_bytes()
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        self.xor.snapshot()
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.xor.finish()
    }
}

impl Default for ElfEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
pub(crate) fn encode_elf_values(values: &[f64]) -> io::Result<Vec<u8>> {
    if values.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "values must be non-empty",
        ));
    }
    let mut encoder = ElfEncoder::new();
    for value in values {
        encoder.push(*value)?;
    }
    Ok(encoder.finish())
}

pub(crate) fn decode_elf_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut decoder = ElfDecoder::new(buf);
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(decoder.read_value()?);
    }
    Ok(out)
}

struct ElfDecoder<'a> {
    xor: ElfXorDecompressor<'a>,
    last_beta_star: i32,
}

impl<'a> ElfDecoder<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            xor: ElfXorDecompressor::new(buf),
            last_beta_star: i32::MAX,
        }
    }

    fn read_value(&mut self) -> io::Result<f64> {
        let first = self.xor.read_bits(1)?;
        if first == 0 {
            return self.recover_value();
        }
        let second = self.xor.read_bits(1)?;
        if second == 0 {
            return self.xor.read_value();
        }
        let beta_star = self.xor.read_bits(4)? as i32;
        self.last_beta_star = beta_star;
        self.recover_value()
    }

    fn recover_value(&mut self) -> io::Result<f64> {
        let v_prime = self.xor.read_value()?;
        let sp = Elf64Utils::get_sp(v_prime.abs());
        if self.last_beta_star == 0 {
            let mut v = Elf64Utils::get_10i_n(-sp - 1);
            if v_prime.is_sign_negative() {
                v = -v;
            }
            Ok(v)
        } else {
            let alpha = self.last_beta_star - sp - 1;
            Ok(Elf64Utils::round_up(v_prime, alpha))
        }
    }
}

#[derive(Debug)]
struct ElfXorCompressor {
    writer: BitWriter,
    stored_val: u64,
    stored_leading: u8,
    stored_trailing: u8,
    first: bool,
    has_window: bool,
}

impl ElfXorCompressor {
    fn new() -> Self {
        Self {
            writer: BitWriter::new(),
            stored_val: 0,
            stored_leading: 0,
            stored_trailing: 0,
            first: true,
            has_window: false,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        self.writer.write_bit(bit);
    }

    fn write_bits(&mut self, value: u64, bits: u8) {
        self.writer.write_bits(value, bits);
    }

    fn add_value(&mut self, value: u64) -> io::Result<()> {
        if self.first {
            self.first = false;
            self.stored_val = value;
            let trailing = value.trailing_zeros() as u8;
            self.writer.write_bits(u64::from(trailing), 7);
            let payload_bits = 63u8.saturating_sub(trailing);
            if payload_bits > 0 {
                self.writer
                    .write_bits(value >> (trailing + 1), payload_bits);
            }
            return Ok(());
        }

        let xor = self.stored_val ^ value;
        if xor == 0 {
            self.writer.write_bits(1, 2);
            return Ok(());
        }

        let leading = LEADING_ROUND[xor.leading_zeros() as usize];
        let trailing = xor.trailing_zeros() as u8;
        if self.has_window && leading == self.stored_leading && trailing >= self.stored_trailing {
            let center_bits = 64u32
                .saturating_sub(u32::from(self.stored_leading))
                .saturating_sub(u32::from(self.stored_trailing));
            self.writer.write_bits(0, 2);
            self.writer
                .write_bits(xor >> self.stored_trailing, center_bits as u8);
        } else {
            self.stored_leading = leading;
            self.stored_trailing = trailing;
            self.has_window = true;
            let center_bits = 64u32
                .saturating_sub(u32::from(leading))
                .saturating_sub(u32::from(trailing));
            if center_bits <= 16 {
                let header =
                    (((0x2u16 << 3) | u16::from(LEADING_REPRESENTATION[leading as usize])) << 4)
                        | (center_bits as u16 & 0x0f);
                self.writer.write_bits(u64::from(header), 9);
                let payload_bits = center_bits.saturating_sub(1) as u8;
                if payload_bits > 0 {
                    self.writer.write_bits(xor >> (trailing + 1), payload_bits);
                }
            } else {
                let header =
                    (((0x3u16 << 3) | u16::from(LEADING_REPRESENTATION[leading as usize])) << 6)
                        | (center_bits as u16 & 0x3f);
                self.writer.write_bits(u64::from(header), 11);
                let payload_bits = center_bits.saturating_sub(1) as u8;
                if payload_bits > 0 {
                    self.writer.write_bits(xor >> (trailing + 1), payload_bits);
                }
            }
        }

        self.stored_val = value;
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.writer.finish()
    }

    fn snapshot(&self) -> Vec<u8> {
        self.writer.snapshot()
    }

    fn len_bytes(&self) -> usize {
        self.writer.len_bytes()
    }
}

struct ElfXorDecompressor<'a> {
    reader: BitReader<'a>,
    stored_val: u64,
    stored_leading: u8,
    stored_trailing: u8,
    first: bool,
}

impl<'a> ElfXorDecompressor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            reader: BitReader::new(buf),
            stored_val: 0,
            stored_leading: 0,
            stored_trailing: 0,
            first: true,
        }
    }

    fn read_bits(&mut self, bits: u8) -> io::Result<u64> {
        self.reader.read_bits(bits)
    }

    fn read_value(&mut self) -> io::Result<f64> {
        if self.first {
            self.first = false;
            let trailing = self.reader.read_bits(7)? as u8;
            if trailing < 64 {
                let payload = self.reader.read_bits(63 - trailing)?;
                self.stored_val = ((payload << 1) + 1) << trailing;
            } else {
                self.stored_val = 0;
            }
            return Ok(f64::from_bits(self.stored_val));
        }

        let flag = self.reader.read_bits(2)? as u8;
        match flag {
            3 => {
                let lead_and_center = self.reader.read_bits(9)? as u16;
                self.stored_leading = LEADING_DECODE[(lead_and_center >> 6) as usize];
                let mut center_bits = (lead_and_center & 0x3f) as u8;
                if center_bits == 0 {
                    center_bits = 64;
                }
                self.stored_trailing = 64u8.saturating_sub(self.stored_leading + center_bits);
                let payload = self.reader.read_bits(center_bits.saturating_sub(1))?;
                let value = ((payload << 1) + 1) << self.stored_trailing;
                self.stored_val ^= value;
            }
            2 => {
                let lead_and_center = self.reader.read_bits(7)? as u16;
                self.stored_leading = LEADING_DECODE[(lead_and_center >> 4) as usize];
                let mut center_bits = (lead_and_center & 0x0f) as u8;
                if center_bits == 0 {
                    center_bits = 16;
                }
                self.stored_trailing = 64u8.saturating_sub(self.stored_leading + center_bits);
                let payload = self.reader.read_bits(center_bits.saturating_sub(1))?;
                let value = ((payload << 1) + 1) << self.stored_trailing;
                self.stored_val ^= value;
            }
            1 => {}
            _ => {
                let center_bits = 64u8.saturating_sub(self.stored_leading + self.stored_trailing);
                let payload = self.reader.read_bits(center_bits)?;
                self.stored_val ^= payload << self.stored_trailing;
            }
        }

        Ok(f64::from_bits(self.stored_val))
    }
}

struct Elf64Utils;

impl Elf64Utils {
    const F_ALPHA: [i32; 21] = [
        0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57, 60, 64, 67,
    ];
    const MAP_10I_P: [f64; 21] = [
        1.0, 1.0E1, 1.0E2, 1.0E3, 1.0E4, 1.0E5, 1.0E6, 1.0E7, 1.0E8, 1.0E9, 1.0E10, 1.0E11, 1.0E12,
        1.0E13, 1.0E14, 1.0E15, 1.0E16, 1.0E17, 1.0E18, 1.0E19, 1.0E20,
    ];
    const MAP_10I_N: [f64; 21] = [
        1.0, 1.0E-1, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6, 1.0E-7, 1.0E-8, 1.0E-9, 1.0E-10,
        1.0E-11, 1.0E-12, 1.0E-13, 1.0E-14, 1.0E-15, 1.0E-16, 1.0E-17, 1.0E-18, 1.0E-19, 1.0E-20,
    ];
    const MAP_SP_GREATER1: [i64; 10] = [
        1,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
    ];
    const MAP_SP_LESS1: [f64; 11] = [
        1.0,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
        0.00000001,
        0.000000001,
        0.0000000001,
    ];
    const LOG_2_10: f64 = std::f64::consts::LN_10 / std::f64::consts::LN_2;

    fn get_f_alpha(alpha: i32) -> i32 {
        if alpha < 0 {
            panic!("alpha must be >= 0");
        }
        if alpha as usize >= Self::F_ALPHA.len() {
            (f64::from(alpha) * Self::LOG_2_10).ceil() as i32
        } else {
            Self::F_ALPHA[alpha as usize]
        }
    }

    fn get_alpha_and_beta_star(v: f64, last_beta_star: i32) -> (i32, i32) {
        let mut v = v;
        if v < 0.0 {
            v = -v;
        }
        let (sp, flag) = Self::get_sp_and_10i_n_flag(v);
        let beta = Self::get_significant_count(v, sp, last_beta_star);
        let alpha = beta - sp - 1;
        let beta_star = if flag == 1 { 0 } else { beta };
        (alpha, beta_star)
    }

    fn round_up(v: f64, alpha: i32) -> f64 {
        let scale = Self::get_10i_p(alpha);
        if v < 0.0 {
            (v * scale).floor() / scale
        } else {
            (v * scale).ceil() / scale
        }
    }

    fn get_significant_count(v: f64, sp: i32, last_beta_star: i32) -> i32 {
        let mut i = if last_beta_star != i32::MAX && last_beta_star != 0 {
            (last_beta_star - sp - 1).max(1)
        } else if last_beta_star == i32::MAX {
            (17 - sp - 1).max(0)
        } else if sp >= 0 {
            1
        } else {
            -sp
        };

        let mut temp = v * Self::get_10i_p(i);
        if !temp.is_finite() || temp.abs() > i64::MAX as f64 {
            return sp + i + 1;
        }
        let mut temp_long = temp.trunc() as i64;
        while (temp_long as f64) != temp {
            i += 1;
            temp = v * Self::get_10i_p(i);
            if !temp.is_finite() || temp.abs() > i64::MAX as f64 {
                return sp + i + 1;
            }
            temp_long = temp.trunc() as i64;
        }

        if temp / Self::get_10i_p(i) != v {
            return 17;
        }

        while i > 0 && temp_long % 10 == 0 {
            i -= 1;
            temp_long /= 10;
        }
        sp + i + 1
    }

    fn get_10i_p(i: i32) -> f64 {
        if i < 0 {
            panic!("power index must be >= 0");
        }
        let idx = i as usize;
        if idx >= Self::MAP_10I_P.len() {
            10f64.powi(i)
        } else {
            Self::MAP_10I_P[idx]
        }
    }

    fn get_10i_n(i: i32) -> f64 {
        if i < 0 {
            panic!("power index must be >= 0");
        }
        let idx = i as usize;
        if idx >= Self::MAP_10I_N.len() {
            10f64.powi(-i)
        } else {
            Self::MAP_10I_N[idx]
        }
    }

    fn get_sp(v: f64) -> i32 {
        if v == 0.0 {
            return i32::MIN;
        }
        if v >= 1.0 {
            let mut i = 0usize;
            while i < Self::MAP_SP_GREATER1.len() - 1 {
                if v < Self::MAP_SP_GREATER1[i + 1] as f64 {
                    return i as i32;
                }
                i += 1;
            }
        } else {
            let mut i = 1usize;
            while i < Self::MAP_SP_LESS1.len() {
                if v >= Self::MAP_SP_LESS1[i] {
                    return -(i as i32);
                }
                i += 1;
            }
        }
        v.log10().floor() as i32
    }

    fn get_sp_and_10i_n_flag(v: f64) -> (i32, i32) {
        if v >= 1.0 {
            let mut i = 0usize;
            while i < Self::MAP_SP_GREATER1.len() - 1 {
                if v < Self::MAP_SP_GREATER1[i + 1] as f64 {
                    return (i as i32, 0);
                }
                i += 1;
            }
        } else {
            let mut i = 1usize;
            while i < Self::MAP_SP_LESS1.len() {
                if v >= Self::MAP_SP_LESS1[i] {
                    let flag = if v == Self::MAP_SP_LESS1[i] { 1 } else { 0 };
                    return (-(i as i32), flag);
                }
                i += 1;
            }
        }
        let log10v = v.log10();
        let sp = log10v.floor() as i32;
        let flag = if log10v == log10v.trunc() { 1 } else { 0 };
        (sp, flag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elf_xor_roundtrip() {
        let values: Vec<f64> = vec![1.0, 1.0, 1.5, -2.25, 0.0, -0.0, 1000.125];
        let mut compressor = ElfXorCompressor::new();
        for value in &values {
            compressor.add_value(f64::to_bits(*value)).unwrap();
        }
        let bytes = compressor.finish();
        let mut decoder = ElfXorDecompressor::new(&bytes);
        let decoded: Vec<f64> = (0..values.len())
            .map(|_| decoder.read_value().unwrap())
            .collect();
        let in_bits: Vec<u64> = values.iter().map(|v| f64::to_bits(*v)).collect();
        let out_bits: Vec<u64> = decoded.iter().map(|v| f64::to_bits(*v)).collect();
        assert_eq!(in_bits, out_bits);
    }

    #[test]
    fn elf_roundtrip_basic() {
        let values: Vec<f64> = vec![1.0, 1.25, 10.5, -2.5, 1000.0, 0.0001, -0.0002];
        let encoded = encode_elf_values(&values).unwrap();
        let decoded = decode_elf_values(&encoded, values.len()).unwrap();
        let in_bits: Vec<u64> = values.iter().map(|v| f64::to_bits(*v)).collect();
        let out_bits: Vec<u64> = decoded.iter().map(|v| f64::to_bits(*v)).collect();
        assert_eq!(in_bits, out_bits);
    }

    #[test]
    fn elf_roundtrip_large_magnitude() {
        let values: Vec<f64> = vec![1e18, -1e18, 1e19, -1e19];
        let encoded = encode_elf_values(&values).unwrap();
        let decoded = decode_elf_values(&encoded, values.len()).unwrap();
        let in_bits: Vec<u64> = values.iter().map(|v| f64::to_bits(*v)).collect();
        let out_bits: Vec<u64> = decoded.iter().map(|v| f64::to_bits(*v)).collect();
        assert_eq!(in_bits, out_bits);
    }
}
