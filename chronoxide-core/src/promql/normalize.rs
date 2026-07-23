use std::collections::BTreeMap;

use crate::hash::xxhash64;

use super::{
    METRIC_NAME_LABEL,
    ast::{CanonicalLabel, CanonicalLabelSet},
};

pub fn normalize_metric_name(original: &str) -> String {
    normalize_name(original, is_metric_first, is_metric_rest, false)
}

pub fn normalize_label_name(original: &str) -> String {
    normalize_name(original, is_label_first, is_label_rest, true)
}

pub fn format_promql_float_label(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        };
    }

    let raw = value.to_string();
    if raw.contains('e') || raw.contains('E') {
        return normalize_promql_exponent(&raw);
    }

    let abs = value.abs();
    if !(1e-4..1e6).contains(&abs) {
        return decimal_to_promql_scientific(&raw);
    }

    raw
}

fn decimal_to_promql_scientific(raw: &str) -> String {
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |stripped| (true, stripped));
    let (digits, exponent) = if let Some((integer, fraction)) = unsigned.split_once('.') {
        if integer != "0" {
            (
                format!("{integer}{fraction}"),
                integer.len().saturating_sub(1) as i32,
            )
        } else if let Some(first_non_zero) = fraction.bytes().position(|value| value != b'0') {
            (
                fraction[first_non_zero..].to_string(),
                -((first_non_zero as i32) + 1),
            )
        } else {
            ("0".to_string(), 0)
        }
    } else {
        (
            unsigned.to_string(),
            unsigned.len().saturating_sub(1) as i32,
        )
    };

    format_promql_scientific(negative, digits, exponent)
}

fn normalize_promql_exponent(raw: &str) -> String {
    let Some(exponent_separator) = raw.find(['e', 'E']) else {
        return raw.to_string();
    };
    let mantissa = &raw[..exponent_separator];
    let exponent = raw[exponent_separator + 1..].parse::<i32>().unwrap_or(0);
    format!("{mantissa}{}", format_promql_exponent(exponent))
}

fn format_promql_scientific(negative: bool, digits: String, exponent: i32) -> String {
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    let mut chars = digits.chars();
    if let Some(first) = chars.next() {
        out.push(first);
    }
    let rest = chars.as_str();
    if !rest.is_empty() {
        out.push('.');
        out.push_str(rest);
    }
    out.push_str(&format_promql_exponent(exponent));
    out
}

fn format_promql_exponent(exponent: i32) -> String {
    let sign = if exponent >= 0 { '+' } else { '-' };
    let magnitude = exponent.unsigned_abs();
    if magnitude < 10 {
        format!("e{sign}0{magnitude}")
    } else {
        format!("e{sign}{magnitude}")
    }
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

pub(super) fn is_metric_first(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
}

pub(super) fn is_metric_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
}

pub(super) fn is_label_first(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_label_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}
