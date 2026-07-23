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
        if !buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gorilla stream has trailing bytes",
            ));
        }
        return Ok(Vec::new());
    }
    let minimum_len = minimum_gorilla_encoded_len_bytes(count)?;
    if buf.len() < minimum_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "gorilla point count is infeasible for the encoded value bytes",
        ));
    }
    let mut reader = BitReader::new(buf);
    let first = reader.read_bits(64)?;
    let mut prev = first;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
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
        let prior_leading = prev_leading;
        let prior_trailing = prev_trailing;
        let had_window = has_window;
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
        if xor == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gorilla zero xor must use the repeated-value control",
            ));
        }
        if control2 != 0 {
            let canonical_leading = (xor.leading_zeros() as u8).min(GORILLA_LEADING_MAX);
            let canonical_trailing = xor.trailing_zeros() as u8;
            if leading != canonical_leading || trailing != canonical_trailing {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "gorilla new window is noncanonical for its xor",
                ));
            }
            if had_window
                && canonical_leading >= prior_leading
                && canonical_trailing >= prior_trailing
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "gorilla new window must reuse the prior window",
                ));
            }
            prev_leading = leading;
            prev_trailing = trailing;
            has_window = true;
        }
        let next = prev ^ xor;
        values.push(f64::from_bits(next));
        prev = next;
    }

    reader.require_canonical_end()?;

    Ok(values)
}

pub(crate) fn minimum_gorilla_encoded_len_bytes(count: usize) -> io::Result<usize> {
    if count == 0 {
        return Ok(0);
    }
    let encoded_bits = count
        .checked_sub(1)
        .and_then(|remaining| remaining.checked_add(64))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "gorilla minimum encoded size overflows",
            )
        })?;
    encoded_bits
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "gorilla minimum encoded size overflows",
            )
        })
}

pub(crate) fn gorilla_encoded_len_bytes(
    values: impl IntoIterator<Item = f64>,
) -> io::Result<usize> {
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "values must be non-empty",
        ));
    };

    let mut bits = 64u64;
    let mut prev = first.to_bits();
    let mut prev_leading = 0u8;
    let mut prev_trailing = 0u8;
    let mut has_window = false;
    for value in values {
        let next = value.to_bits();
        let xor = prev ^ next;
        let encoded_bits = if xor == 0 {
            1u64
        } else {
            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;
            if has_window && leading >= prev_leading && trailing >= prev_trailing {
                2u64 + u64::from(64 - prev_leading as u32 - prev_trailing as u32)
            } else {
                let store_leading = leading.min(GORILLA_LEADING_MAX);
                let significant = 64 - store_leading as u32 - trailing as u32;
                prev_leading = store_leading;
                prev_trailing = trailing;
                has_window = true;
                2u64 + u64::from(GORILLA_LEADING_BITS)
                    + u64::from(GORILLA_SIG_BITS)
                    + u64::from(significant)
            }
        };
        bits = bits.checked_add(encoded_bits).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "gorilla encoded size overflows")
        })?;
        prev = next;
    }

    let bytes = bits
        .checked_add(7)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "gorilla size overflows"))?
        / 8;
    usize::try_from(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "gorilla size exceeds usize"))
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

    #[test]
    fn gorilla_size_estimator_matches_canonical_bytes() {
        for values in [
            vec![1.0],
            vec![1.0, 1.0],
            vec![1.0, 1.5, 1.75, 1.75, -2.25],
            vec![
                f64::from_bits(0),
                f64::from_bits(u64::MAX),
                f64::from_bits(1),
            ],
        ] {
            let encoded = encode_gorilla_values(&values).unwrap();
            assert_eq!(
                gorilla_encoded_len_bytes(values.iter().copied()).unwrap(),
                encoded.len()
            );
        }

        let mut encoder = GorillaEncoder::new();
        let mut values: Vec<f64> = Vec::new();
        let mut state = 0x243f_6a88_85a3_08d3u64;
        for index in 0..512 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let value = if index % 5 == 0 && !values.is_empty() {
                *values.last().unwrap()
            } else {
                f64::from_bits(state)
            };
            values.push(value);
            encoder.push(value).unwrap();
            assert_eq!(
                gorilla_encoded_len_bytes(values.iter().copied()).unwrap(),
                encoder.len_bytes(),
                "estimator diverged at deterministic prefix length {}",
                values.len()
            );
        }
    }

    #[test]
    fn gorilla_rejects_trailing_bytes_and_non_zero_padding() {
        let values = [1.0, 1.0];
        let encoded = encode_gorilla_values(&values).unwrap();
        assert_eq!(
            encoded,
            [0x3f, 0xf0, 0, 0, 0, 0, 0, 0, 0],
            "one repeated value has one zero control bit and seven canonical zero pad bits"
        );

        let mut trailing = encoded.clone();
        trailing.push(0);
        let error = decode_gorilla_values(&trailing, values.len()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("trailing bytes"));

        let mut non_zero_padding = encoded;
        *non_zero_padding.last_mut().unwrap() |= 1;
        let error = decode_gorilla_values(&non_zero_padding, values.len()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("padding bits"));
    }

    #[test]
    fn gorilla_rejects_noncanonical_controls_and_windows() {
        let mut zero_xor = BitWriter::new();
        zero_xor.write_bits(1.0f64.to_bits(), 64);
        zero_xor.write_bit(true);
        zero_xor.write_bit(true);
        zero_xor.write_bits(0, GORILLA_LEADING_BITS);
        zero_xor.write_bits(0, GORILLA_SIG_BITS);
        zero_xor.write_bits(0, 64);
        let error = decode_gorilla_values(&zero_xor.finish(), 2).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("zero xor"));

        let mut wider_window = BitWriter::new();
        wider_window.write_bits(0, 64);
        wider_window.write_bit(true);
        wider_window.write_bit(true);
        wider_window.write_bits(30, GORILLA_LEADING_BITS);
        wider_window.write_bits(34, GORILLA_SIG_BITS);
        wider_window.write_bits(1, 34);
        let error = decode_gorilla_values(&wider_window.finish(), 2).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("new window is noncanonical"));

        let mut unnecessary_window = BitWriter::new();
        unnecessary_window.write_bits(0, 64);
        unnecessary_window.write_bit(true);
        unnecessary_window.write_bit(true);
        unnecessary_window.write_bits(0, GORILLA_LEADING_BITS);
        unnecessary_window.write_bits(0, GORILLA_SIG_BITS);
        unnecessary_window.write_bits(u64::MAX, 64);
        unnecessary_window.write_bit(true);
        unnecessary_window.write_bit(true);
        unnecessary_window.write_bits(31, GORILLA_LEADING_BITS);
        unnecessary_window.write_bits(33, GORILLA_SIG_BITS);
        unnecessary_window.write_bits(1, 33);
        let error = decode_gorilla_values(&unnecessary_window.finish(), 3).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("must reuse"));
    }

    #[test]
    fn gorilla_rejects_infeasible_or_overflowing_counts_before_allocation() {
        let error = decode_gorilla_values(&[0; 8], u32::MAX as usize).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("infeasible"));

        let error = minimum_gorilla_encoded_len_bytes(usize::MAX).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("overflows"));
    }
}
