use std::io;

pub(crate) fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn decode_varint(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let start = *cursor;
    let mut value = 0u64;
    for byte_index in 0..10u32 {
        if *cursor >= buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "varint is truncated",
            ));
        }
        let byte = buf[*cursor];
        *cursor += 1;
        if byte_index == 9 && byte & 0xfe != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint exceeds u64",
            ));
        }
        value |= u64::from(byte & 0x7f) << (byte_index * 7);
        if byte & 0x80 == 0 {
            let encoded_len = *cursor - start;
            if encoded_len != varint_len(value) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varint is not canonical",
                ));
            }
            return Ok(value);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "varint exceeds u64",
    ))
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
            u64::MAX,
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

    #[test]
    fn varint_rejects_overflow_overlong_and_truncated_encodings() {
        for (bytes, kind, message) in [
            (
                vec![0x80, 0x00],
                io::ErrorKind::InvalidData,
                "varint is not canonical",
            ),
            (
                vec![0x80; 10],
                io::ErrorKind::InvalidData,
                "varint exceeds u64",
            ),
            (
                [vec![0x80; 9], vec![0x02]].concat(),
                io::ErrorKind::InvalidData,
                "varint exceeds u64",
            ),
            (
                vec![0x80],
                io::ErrorKind::UnexpectedEof,
                "varint is truncated",
            ),
        ] {
            let mut cursor = 0;
            let error = decode_varint(&bytes, &mut cursor).unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), message);
        }
    }
}
