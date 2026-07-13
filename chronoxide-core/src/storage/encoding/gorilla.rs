use std::io;

use crate::storage::encoding::bitstream::{BitReader, BitWriter};

pub(crate) const GORILLA_LEADING_BITS: u8 = 5;
pub(crate) const GORILLA_LEADING_MAX: u8 = (1 << GORILLA_LEADING_BITS) - 1;
pub(crate) const GORILLA_SIG_BITS: u8 = 6;

#[derive(Debug)]
pub(crate) struct GorillaEncoder {
    writer: BitWriter,
    prev: Option<u64>,
    prev_leading: u8,
    prev_trailing: u8,
    has_window: bool,
}

impl GorillaEncoder {
    pub(crate) fn new() -> Self {
        Self {
            writer: BitWriter::new(),
            prev: None,
            prev_leading: 0,
            prev_trailing: 0,
            has_window: false,
        }
    }

    pub(crate) fn push(&mut self, value: f64) -> io::Result<()> {
        let bits = value.to_bits();
        let Some(prev) = self.prev else {
            self.writer.write_bits(bits, 64);
            self.prev = Some(bits);
            return Ok(());
        };

        let xor = prev ^ bits;
        if xor == 0 {
            self.writer.write_bit(false);
        } else {
            self.writer.write_bit(true);
            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            if self.has_window && leading >= self.prev_leading && trailing >= self.prev_trailing {
                self.writer.write_bit(false);
                let sig_bits = 64 - self.prev_leading as u32 - self.prev_trailing as u32;
                let payload = extract_sig_bits(xor, self.prev_trailing, sig_bits)?;
                self.writer.write_bits(payload, sig_bits as u8);
            } else {
                self.writer.write_bit(true);
                let store_leading = leading.min(GORILLA_LEADING_MAX);
                let sig_bits = 64 - store_leading as u32 - trailing as u32;
                let len_code = if sig_bits == 64 { 0 } else { sig_bits as u8 };
                self.writer
                    .write_bits(u64::from(store_leading), GORILLA_LEADING_BITS);
                self.writer
                    .write_bits(u64::from(len_code), GORILLA_SIG_BITS);
                let payload = extract_sig_bits(xor, trailing, sig_bits)?;
                self.writer.write_bits(payload, sig_bits as u8);
                self.prev_leading = store_leading;
                self.prev_trailing = trailing;
                self.has_window = true;
            }
        }

        self.prev = Some(bits);
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.writer.finish()
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        self.writer.snapshot()
    }

    pub(crate) fn len_bytes(&self) -> usize {
        self.writer.len_bytes()
    }
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn encode_gorilla_values(values: &[f64]) -> io::Result<Vec<u8>> {
    super::encode_float_values_with(
        values,
        GorillaEncoder::new,
        GorillaEncoder::push,
        GorillaEncoder::finish,
    )
}

pub(crate) fn decode_gorilla_values(buf: &[u8], count: usize) -> io::Result<Vec<f64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut reader = BitReader::new(buf);
    let first = reader.read_bits(64)?;
    let mut prev = first;
    let mut values = Vec::with_capacity(count);
    values.push(f64::from_bits(prev));

    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;
    let mut has_window = false;

    for _ in 1..count {
        let control = reader.read_bit()?;
        if control == 0 {
            values.push(f64::from_bits(prev));
            continue;
        }

        let control2 = reader.read_bit()?;
        let (leading, trailing, sig_bits) = if control2 == 0 {
            if !has_window {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "gorilla window missing",
                ));
            }
            let sig_bits = 64 - prev_leading as u32 - prev_trailing as u32;
            (prev_leading, prev_trailing, sig_bits as u8)
        } else {
            let leading = reader.read_bits(GORILLA_LEADING_BITS)? as u8;
            let len_code = reader.read_bits(GORILLA_SIG_BITS)? as u8;
            let sig_bits = if len_code == 0 { 64 } else { len_code as u32 };
            if leading as u32 + sig_bits > 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "gorilla window invalid",
                ));
            }
            let trailing = 64 - leading as u32 - sig_bits;
            let trailing_u8 = trailing as u8;
            prev_leading = leading;
            prev_trailing = trailing_u8;
            has_window = true;
            (leading, trailing_u8, sig_bits as u8)
        };

        if sig_bits == 64 && (leading != 0 || trailing != 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gorilla window length mismatch",
            ));
        }

        let xor_bits = reader.read_bits(sig_bits)?;
        let xor = xor_bits << trailing;
        let next = prev ^ xor;
        values.push(f64::from_bits(next));
        prev = next;
    }

    Ok(values)
}

fn extract_sig_bits(xor: u64, trailing: u8, sig_bits: u32) -> io::Result<u64> {
    if sig_bits == 0 || sig_bits > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid significant bit count",
        ));
    }
    if sig_bits == 64 {
        if trailing != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid trailing zeros for full-width xor",
            ));
        }
        return Ok(xor);
    }
    let mask = (1u64 << sig_bits) - 1;
    Ok((xor >> trailing) & mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gorilla_roundtrip_values() {
        let values = vec![1.0, 1.0, 1.5, 1.5, -2.25, 1000.125];
        let encoded = encode_gorilla_values(&values).unwrap();
        let decoded = decode_gorilla_values(&encoded, values.len()).unwrap();
        let bits_in: Vec<u64> = values.iter().map(|v| v.to_bits()).collect();
        let bits_out: Vec<u64> = decoded.iter().map(|v| v.to_bits()).collect();
        assert_eq!(bits_out, bits_in);
    }

    #[test]
    fn gorilla_high_leading_zeros() {
        let val1: f64 = 1.0;
        let val2 = f64::from_bits(val1.to_bits() ^ 1);

        let values = vec![val1, val2];
        let encoded = encode_gorilla_values(&values).unwrap();
        let decoded = decode_gorilla_values(&encoded, values.len()).unwrap();

        assert_eq!(values, decoded);
    }
}
