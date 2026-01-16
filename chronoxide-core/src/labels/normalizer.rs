use std::borrow::Cow;
use std::hash::Hasher;
#[allow(deprecated)]
use std::hash::SipHasher;

pub const MAX_LABEL_NAME_BYTES: usize = 1024;
pub const MAX_LABEL_VALUE_BYTES: usize = 2048;

// Truncation suffix format:
//   `~{algo_id}{hash:016x}`
// Where:
// - `algo_id` is a 1-byte ASCII marker identifying the hash algorithm & keying scheme, to allow
//   future changes without ambiguity.
// - `hash` is a 64-bit SipHash (fixed key) over the full original string bytes.
//
// Note: if you change the algorithm or keys, also change `TRUNC_HASH_ALGO_ID` so old/new truncated
// values remain distinguishable.
const TRUNC_HASH_ALGO_ID: char = 's'; // SipHash-2-4, fixed key
const TRUNC_HASH_SIPHASH_K0: u64 = 0x0706_0504_0302_0100;
const TRUNC_HASH_SIPHASH_K1: u64 = 0x0f0e_0d0c_0b0a_0908;

fn truncate_utf8_to_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    &s[..end]
}

fn siphash24_fixed_64(bytes: &[u8]) -> u64 {
    #[allow(deprecated)]
    let mut hasher = SipHasher::new_with_keys(TRUNC_HASH_SIPHASH_K0, TRUNC_HASH_SIPHASH_K1);
    hasher.write(bytes);
    hasher.finish()
}

fn truncate_with_hash(s: &str, max_bytes: usize) -> Cow<'_, str> {
    if s.len() <= max_bytes {
        return Cow::Borrowed(s);
    }

    // Keep the suffix stable across restarts by using a fixed (non-random) hash.
    // `~{algo_id}{hash:016x}` is 18 bytes total.
    const HASH_SUFFIX_BYTES: usize = 1 + 1 + 16;

    let hash = siphash24_fixed_64(s.as_bytes());
    let suffix = format!("~{TRUNC_HASH_ALGO_ID}{hash:016x}");
    debug_assert_eq!(suffix.len(), HASH_SUFFIX_BYTES);

    let prefix_max = max_bytes.saturating_sub(HASH_SUFFIX_BYTES);
    let prefix = truncate_utf8_to_boundary(s, prefix_max);

    let mut out = String::with_capacity(prefix.len() + suffix.len());
    out.push_str(prefix);
    out.push_str(&suffix);
    Cow::Owned(out)
}

pub(crate) fn normalize_label_key(key: &str) -> Cow<'_, str> {
    truncate_with_hash(key, MAX_LABEL_NAME_BYTES)
}

pub(crate) fn normalize_label_value(value: &str) -> Cow<'_, str> {
    truncate_with_hash(value, MAX_LABEL_VALUE_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn siphash24_matches_reference_vectors() {
        // Reference test vectors from the SipHash-2-4 specification for key bytes 00..0f.
        // Our fixed keys are:
        //   k0 = 0x0706050403020100
        //   k1 = 0x0f0e0d0c0b0a0908
        let vectors: &[(usize, u64)] = &[
            (0, 0x726fdb47dd0e0e31),
            (1, 0x74f839c593dc67fd),
            (2, 0x0d6c8009d9a94f5a),
            (3, 0x85676696d7fb7e2d),
            (4, 0xcf2794e0277187b7),
            (63, 0x958a324ceb064572),
        ];

        for &(len, expected) in vectors {
            let msg: Vec<u8> = (0..len).map(|i| i as u8).collect();
            assert_eq!(siphash24_fixed_64(&msg), expected, "len={len}");
        }
    }

    #[test]
    fn normalize_label_value_over_limit_is_truncated_with_hash() {
        let long_value = "a".repeat(MAX_LABEL_VALUE_BYTES + 123);
        let normalized = normalize_label_value(long_value.as_str());
        assert_eq!(normalized.len(), MAX_LABEL_VALUE_BYTES);
        assert!(normalized.as_ref().contains("~s"));
    }
}
