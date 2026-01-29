pub fn encode_zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

pub fn decode_zigzag_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_roundtrip() {
        let values = [
            0i64,
            1,
            -1,
            2,
            -2,
            127,
            -128,
            1024,
            -1024,
            i64::MAX,
            i64::MIN + 1,
        ];
        for value in values {
            let encoded = encode_zigzag_i64(value);
            let decoded = decode_zigzag_i64(encoded);
            assert_eq!(decoded, value);
        }
    }
}
