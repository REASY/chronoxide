use std::io;

pub(crate) fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn decode_varint(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        if *cursor >= buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "varint overflow",
            ));
        }
        let byte = buf[*cursor];
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint too large",
            ));
        }
    }
    Ok(value)
}

pub fn varint_len(mut value: u64) -> usize {
    let mut len = 1usize;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let values = [
            0u64,
            1,
            127,
            128,
            255,
            16_384,
            1 << 21,
            1 << 28,
            1 << 35,
            1 << 42,
            1 << 49,
            1 << 56,
            1 << 63,
        ];
        for value in values {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);
            let mut cursor = 0usize;
            let decoded = decode_varint(&buf, &mut cursor).expect("decode varint");
            assert_eq!(decoded, value);
            assert_eq!(cursor, buf.len());
            assert_eq!(varint_len(value), buf.len());
        }
    }
}
