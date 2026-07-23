use std::io;

#[derive(Debug)]
pub(crate) struct BitWriter {
    buf: Vec<u8>,
    bit_buf: u64,
    bit_len: u8,
}

impl BitWriter {
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::new(),
            bit_buf: 0,
            bit_len: 0,
        }
    }

    pub(crate) fn write_bit(&mut self, bit: bool) {
        self.write_bits(u64::from(bit), 1);
    }

    pub(crate) fn write_bits(&mut self, value: u64, bits: u8) {
        if bits == 0 {
            return;
        }
        if bits == 64 && self.bit_len == 0 {
            self.buf.extend_from_slice(&value.to_be_bytes());
            return;
        }

        let mut value = if bits == 64 {
            value
        } else {
            value & ((1u64 << bits) - 1)
        };
        if self.bit_len == 0 && bits.is_multiple_of(8) {
            let byte_len = (bits / 8) as usize;
            let bytes = value.to_be_bytes();
            self.buf.extend_from_slice(&bytes[8 - byte_len..]);
            return;
        }

        let mut remaining = bits as u32;
        while remaining > 0 {
            let available = 64u32.saturating_sub(self.bit_len as u32);
            if remaining <= available {
                let shift = available - remaining;
                self.bit_buf |= value << shift;
                self.bit_len = self.bit_len.saturating_add(remaining as u8);
                remaining = 0;
            } else {
                let shift = remaining - available;
                let chunk = value >> shift;
                self.bit_buf |= chunk;
                self.bit_len = 64;
                if shift == 0 {
                    value = 0;
                } else {
                    value &= (1u64 << shift) - 1;
                }
                remaining = shift;
            }
            self.flush_full_bytes();
        }
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        if self.bit_len > 0 {
            self.buf.push((self.bit_buf >> 56) as u8);
        }
        self.buf
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let mut buf = self.buf.clone();
        if self.bit_len > 0 {
            buf.push((self.bit_buf >> 56) as u8);
        }
        buf
    }

    pub(crate) fn len_bytes(&self) -> usize {
        self.buf.len() + if self.bit_len > 0 { 1 } else { 0 }
    }

    fn flush_full_bytes(&mut self) {
        while self.bit_len >= 8 {
            self.buf.push((self.bit_buf >> 56) as u8);
            self.bit_buf <<= 8;
            self.bit_len = self.bit_len.saturating_sub(8);
        }
    }
}

pub(crate) struct BitReader<'a> {
    buf: &'a [u8],
    index: usize,
    bit_buf: u128,
    bit_len: u8,
}

impl<'a> BitReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            index: 0,
            bit_buf: 0,
            bit_len: 0,
        }
    }

    pub(crate) fn read_bit(&mut self) -> io::Result<u8> {
        Ok(self.read_bits(1)? as u8)
    }

    pub(crate) fn read_bits(&mut self, bits: u8) -> io::Result<u64> {
        if bits == 0 {
            return Ok(0);
        }
        if bits == 64 && self.bit_len == 0 {
            if self.index.saturating_add(8) > self.buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "bitstream exhausted",
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&self.buf[self.index..self.index + 8]);
            self.index = self.index.saturating_add(8);
            return Ok(u64::from_be_bytes(bytes));
        }

        while self.bit_len < bits {
            if self.index >= self.buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "bitstream exhausted",
                ));
            }
            let byte = self.buf[self.index];
            self.index = self.index.saturating_add(1);
            let shift = 120u32.saturating_sub(self.bit_len as u32);
            self.bit_buf |= u128::from(byte) << shift;
            self.bit_len = self.bit_len.saturating_add(8);
        }

        let shift = 128u32.saturating_sub(bits as u32);
        let mut value = (self.bit_buf >> shift) as u64;
        if bits < 64 {
            value &= (1u64 << bits) - 1;
        }
        self.bit_buf <<= bits;
        self.bit_len = self.bit_len.saturating_sub(bits);
        Ok(value)
    }

    pub(crate) fn require_canonical_end(&self) -> io::Result<()> {
        if self.index != self.buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bitstream has trailing bytes",
            ));
        }
        if self.bit_len > 0 {
            let remaining = self.bit_buf >> (128 - u32::from(self.bit_len));
            if remaining != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bitstream has non-zero padding bits",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitstream_mixed_widths_roundtrip() {
        let mut writer = BitWriter::new();
        let pattern: &[(u64, u8)] = &[
            (0b1, 1),
            (0b0, 1),
            (0b101, 3),
            (0b11_0011, 6),
            (0b1010101, 7),
            (0b10101010, 8),
            (0xdead_beef_u64, 32),
            (0x0123_4567_89ab_cdef_u64, 64),
            (0b101, 3),
            (0b1, 1),
        ];
        for (value, bits) in pattern {
            writer.write_bits(*value, *bits);
        }
        let buf = writer.finish();
        let mut reader = BitReader::new(&buf);
        for (value, bits) in pattern {
            let got = reader.read_bits(*bits).unwrap();
            let mask = if *bits == 64 {
                u64::MAX
            } else {
                (1u64 << *bits) - 1
            };
            assert_eq!(got, value & mask);
        }
    }

    #[test]
    fn bitstream_split_reads_match_written_bits() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b1_0010_0001, 9);
        let buf = writer.finish();
        let mut reader = BitReader::new(&buf);
        assert_eq!(reader.read_bits(2).unwrap(), 0b10);
        assert_eq!(reader.read_bits(7).unwrap(), 0b010_0001);
    }

    #[test]
    fn bitstream_roundtrip_unaligned_sequence() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b101_0101, 7);
        writer.write_bits(0b101_0101_0101, 11);
        writer.write_bits(0b01, 2);
        let buf = writer.finish();
        let mut reader = BitReader::new(&buf);
        assert_eq!(reader.read_bits(7).unwrap(), 0b101_0101);
        assert_eq!(reader.read_bits(11).unwrap(), 0b101_0101_0101);
        assert_eq!(reader.read_bits(2).unwrap(), 0b01);
    }

    #[test]
    fn bitstream_first_value_followed_by_flag() {
        let value = 0x3ff0_0000_0000_0000u64;
        let trailing = value.trailing_zeros() as u8;
        let payload = value >> (trailing + 1);
        let mut writer = BitWriter::new();
        writer.write_bits(u64::from(trailing), 7);
        writer.write_bits(payload, 63 - trailing);
        writer.write_bits(1, 2);
        let buf = writer.finish();
        let mut reader = BitReader::new(&buf);
        assert_eq!(reader.read_bits(7).unwrap(), u64::from(trailing));
        assert_eq!(reader.read_bits(63 - trailing).unwrap(), payload);
        assert_eq!(reader.read_bits(2).unwrap(), 1);
    }
}
