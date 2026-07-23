use std::io;

use sha2::{Digest, Sha256};

use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkSamples};
use crate::storage::head::{CounterResetHint, OtlpAggregationTemporality};

use super::super::{SegmentFile, SegmentFooter, invalid_segment_data};

pub(super) fn footer_file_len(footer: &SegmentFooter, file: SegmentFile) -> io::Result<u64> {
    footer
        .files
        .iter()
        .find_map(|entry| (entry.file == file).then_some(entry.size))
        .ok_or_else(|| invalid_segment_data("segment footer omits a tracked file"))
}

pub(super) fn evenly_spaced_series_refs(series_count: u32, limit: u32) -> Vec<u32> {
    let selected = limit.min(series_count);
    match selected {
        0 => Vec::new(),
        1 => vec![0],
        selected => {
            let last = u64::from(series_count - 1);
            let denominator = u64::from(selected - 1);
            (0..selected)
                .map(|index| ((u64::from(index) * last) / denominator) as u32)
                .collect()
        }
    }
}

pub(super) fn facade_io(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub(super) fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> io::Result<()> {
    hash_u32(
        hasher,
        u32::try_from(bytes.len())
            .map_err(|_| invalid_segment_data("fingerprint byte string exceeds u32"))?,
    );
    hasher.update(bytes);
    Ok(())
}

pub(super) fn hash_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_le_bytes());
}

pub(super) fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_le_bytes());
}

pub(super) fn checked_add(target: &mut u64, value: u64, field: &'static str) -> io::Result<()> {
    *target = target
        .checked_add(value)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("{field} overflow")))?;
    Ok(())
}

pub(super) fn chunk_kind_id(kind: ChunkKind) -> u8 {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}

pub(super) fn chunk_kind_name(kind: ChunkKind) -> &'static str {
    match kind {
        ChunkKind::Float => "float",
        ChunkKind::Int64 => "int64",
        ChunkKind::Histogram => "histogram",
        ChunkKind::ExponentialHistogram => "exponential_histogram",
        ChunkKind::Summary => "summary",
    }
}

pub(super) fn chunk_kind_from_inventory_id(kind: u8) -> ChunkKind {
    match kind {
        0 => ChunkKind::Float,
        1 => ChunkKind::Int64,
        2 => ChunkKind::Histogram,
        3 => ChunkKind::ExponentialHistogram,
        4 => ChunkKind::Summary,
        _ => unreachable!("inventory keys originate from ChunkKind"),
    }
}

pub(super) fn chunk_encoding_id(encoding: ChunkEncoding) -> u8 {
    match encoding {
        ChunkEncoding::SchemaVarLen => 0,
        ChunkEncoding::RawF64 => 1,
        ChunkEncoding::RawI64 => 2,
        ChunkEncoding::Gorilla => 3,
        ChunkEncoding::IntDeltaZigZag => 4,
    }
}

pub(super) fn chunk_encoding_name(encoding: ChunkEncoding) -> &'static str {
    match encoding {
        ChunkEncoding::SchemaVarLen => "schema_varlen",
        ChunkEncoding::RawF64 => "raw_f64",
        ChunkEncoding::RawI64 => "raw_i64",
        ChunkEncoding::Gorilla => "gorilla",
        ChunkEncoding::IntDeltaZigZag => "int_delta_zigzag",
    }
}

pub(super) fn chunk_encoding_from_inventory_id(encoding: u8) -> ChunkEncoding {
    match encoding {
        0 => ChunkEncoding::SchemaVarLen,
        1 => ChunkEncoding::RawF64,
        2 => ChunkEncoding::RawI64,
        3 => ChunkEncoding::Gorilla,
        4 => ChunkEncoding::IntDeltaZigZag,
        _ => unreachable!("inventory keys originate from ChunkEncoding"),
    }
}

pub(super) fn chunk_payload_layout_name(kind: ChunkKind, encoding: ChunkEncoding) -> &'static str {
    match (kind, encoding) {
        (ChunkKind::Float, ChunkEncoding::RawF64) | (ChunkKind::Int64, ChunkEncoding::RawI64) => {
            "t0_interleaved_dt_value"
        }
        (ChunkKind::Float, ChunkEncoding::Gorilla)
        | (ChunkKind::Int64, ChunkEncoding::IntDeltaZigZag) => "t0_dt_then_values",
        (
            ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary,
            ChunkEncoding::SchemaVarLen,
        ) => "typed_scalar_lane_and_t0_dt_schema_varlen",
        _ => "invalid_kind_encoding_pair",
    }
}

pub(super) fn temporality_id(temporality: OtlpAggregationTemporality) -> u8 {
    match temporality {
        OtlpAggregationTemporality::Unspecified => 0,
        OtlpAggregationTemporality::Delta => 1,
        OtlpAggregationTemporality::Cumulative => 2,
    }
}

pub(super) fn reset_hint_id(reset_hint: CounterResetHint) -> u8 {
    match reset_hint {
        CounterResetHint::Unknown => 0,
        CounterResetHint::CounterReset => 1,
        CounterResetHint::NotCounterReset => 2,
        CounterResetHint::GaugeType => 3,
    }
}

pub(super) fn chunk_sample_count(samples: &ChunkSamples) -> u64 {
    match samples {
        ChunkSamples::Float(values) => values.len() as u64,
        ChunkSamples::Int64(values) => values.len() as u64,
        ChunkSamples::Histogram(values) => values.len() as u64,
        ChunkSamples::ExponentialHistogram(values) => values.len() as u64,
        ChunkSamples::Summary(values) => values.len() as u64,
    }
}

pub(super) fn hex_digest(digest: [u8; 32]) -> String {
    let mut value = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    value
}
