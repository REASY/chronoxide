use std::collections::BTreeMap;

pub use crate::labels::METRIC_NAME_LABEL;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalLabel {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalLabelSet {
    labels: Vec<CanonicalLabel>,
}

impl CanonicalLabelSet {
    pub fn labels(&self) -> &[CanonicalLabel] {
        &self.labels
    }
}

pub fn normalize_metric_name(original: &str) -> String {
    normalize_name(original, is_metric_first, is_metric_rest, false)
}

pub fn normalize_label_name(original: &str) -> String {
    normalize_name(original, is_label_first, is_label_rest, true)
}

pub fn canonicalize_labelset(metric_name: &str, labels: &[(&str, &str)]) -> CanonicalLabelSet {
    let mut canonical = BTreeMap::new();
    canonical.insert(
        METRIC_NAME_LABEL.to_string(),
        normalize_metric_name(metric_name),
    );

    for (name, value) in labels {
        canonical.insert(normalize_label_name(name), (*value).to_string());
    }

    CanonicalLabelSet {
        labels: canonical
            .into_iter()
            .map(|(name, value)| CanonicalLabel { name, value })
            .collect(),
    }
}

pub fn series_id(canonical: &CanonicalLabelSet) -> u64 {
    let mut bytes = Vec::new();
    for label in canonical.labels() {
        bytes.extend_from_slice(label.name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(label.value.as_bytes());
        bytes.push(0xff);
    }
    xxhash64(&bytes)
}

fn normalize_name(
    original: &str,
    first_ok: fn(char) -> bool,
    rest_ok: fn(char) -> bool,
    label_rules: bool,
) -> String {
    let mut out = String::with_capacity(original.len().max(1));
    for ch in original.chars() {
        if rest_ok(ch) {
            out.push(ch);
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        out.push('_');
    } else if !out.chars().next().is_some_and(first_ok) {
        out.insert(0, '_');
    }

    if label_rules && (out == METRIC_NAME_LABEL || out.starts_with("__")) {
        out.insert_str(0, "otel_");
    }

    if out != original {
        out.push_str("_x");
        out.push_str(&format!("{:016x}", xxhash64(original.as_bytes())));
    }

    out
}

fn is_metric_first(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
}

fn is_metric_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
}

fn is_label_first(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_label_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn xxhash64(input: &[u8]) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;
    const P3: u64 = 1_609_587_929_392_839_161;
    const P4: u64 = 9_650_029_242_287_828_579;
    const P5: u64 = 2_870_177_450_012_600_261;

    let mut cursor = 0usize;
    let mut h64;

    if input.len() >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0;
        let mut v4 = 0u64.wrapping_sub(P1);

        while cursor + 32 <= input.len() {
            v1 = xxh64_round(v1, read_u64(input, cursor));
            cursor += 8;
            v2 = xxh64_round(v2, read_u64(input, cursor));
            cursor += 8;
            v3 = xxh64_round(v3, read_u64(input, cursor));
            cursor += 8;
            v4 = xxh64_round(v4, read_u64(input, cursor));
            cursor += 8;
        }

        h64 = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h64 = xxh64_merge_round(h64, v1);
        h64 = xxh64_merge_round(h64, v2);
        h64 = xxh64_merge_round(h64, v3);
        h64 = xxh64_merge_round(h64, v4);
    } else {
        h64 = P5;
    }

    h64 = h64.wrapping_add(input.len() as u64);

    while cursor + 8 <= input.len() {
        let k1 = xxh64_round(0, read_u64(input, cursor));
        h64 ^= k1;
        h64 = h64.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        cursor += 8;
    }

    if cursor + 4 <= input.len() {
        h64 ^= u64::from(read_u32(input, cursor)).wrapping_mul(P1);
        h64 = h64.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        cursor += 4;
    }

    while cursor < input.len() {
        h64 ^= u64::from(input[cursor]).wrapping_mul(P5);
        h64 = h64.rotate_left(11).wrapping_mul(P1);
        cursor += 1;
    }

    h64 ^= h64 >> 33;
    h64 = h64.wrapping_mul(P2);
    h64 ^= h64 >> 29;
    h64 = h64.wrapping_mul(P3);
    h64 ^ (h64 >> 32)
}

fn xxh64_round(acc: u64, input: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;

    acc.wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

fn xxh64_merge_round(acc: u64, value: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P4: u64 = 9_650_029_242_287_828_579;

    (acc ^ xxh64_round(0, value))
        .wrapping_mul(P1)
        .wrapping_add(P4)
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().unwrap())
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().unwrap())
}
