use std::io;

use crate::storage::chunk::{ChunkEncoding, DecodedChunkLayout};
use crate::storage::encoding::{
    gorilla::GORILLA_LEADING_MAX, gorilla_encoded_len_bytes, varint_len,
};
use crate::storage::head::PROMETHEUS_STALE_NAN_BITS;

use super::super::super::invalid_segment_data;
use super::super::ExperimentalFloatCodecCandidates;
use super::super::helpers::checked_add;
use super::common::PowerOfTwoHistogramAccumulator;

#[derive(Default)]
pub(in super::super) struct FloatCodecCandidatesAccumulator {
    pub(in super::super) evidence: ExperimentalFloatCodecCandidates,
    xor_significant_bits_histogram: PowerOfTwoHistogramAccumulator,
}

impl FloatCodecCandidatesAccumulator {
    pub(in super::super) fn finish(mut self) -> ExperimentalFloatCodecCandidates {
        self.evidence.tie_rule =
            "RAW_F64 wins equal payload-byte ties; then compare decode cost before activation"
                .to_owned();
        self.evidence.xor_significant_bits_histogram = self.xor_significant_bits_histogram.finish();
        self.evidence
    }
}

pub(in super::super) fn observe_float_codec_candidates(
    candidates: &mut FloatCodecCandidatesAccumulator,
    layout: &DecodedChunkLayout,
    indexed_bytes: u32,
    samples: &[(u64, f64)],
) -> io::Result<()> {
    observe_float_value_distribution(candidates, samples)?;
    let evidence = &mut candidates.evidence;
    let Some((t0_ms, _)) = samples.first() else {
        return Err(invalid_segment_data("decoded Float chunk is empty"));
    };
    let points = u64::try_from(samples.len())
        .map_err(|_| invalid_segment_data("Float point count exceeds u64"))?;
    let mut timestamp_delta_bytes = 0u64;
    for (timestamp_ms, _) in samples {
        let delta_ms = timestamp_ms
            .checked_sub(*t0_ms)
            .ok_or_else(|| invalid_segment_data("decoded Float timestamps are not ordered"))?;
        checked_add(
            &mut timestamp_delta_bytes,
            varint_len(delta_ms) as u64,
            "Float candidate timestamp bytes",
        )?;
    }
    let timestamp_bytes = 8u64
        .checked_add(timestamp_delta_bytes)
        .ok_or_else(|| invalid_segment_data("Float candidate timestamp bytes overflow"))?;
    let raw_value_bytes = points
        .checked_mul(8)
        .ok_or_else(|| invalid_segment_data("RawF64 candidate bytes overflow"))?;
    let raw_payload_bytes = timestamp_bytes
        .checked_add(raw_value_bytes)
        .ok_or_else(|| invalid_segment_data("RawF64 candidate payload bytes overflow"))?;
    let gorilla_value_bytes = u64::try_from(gorilla_encoded_len_bytes(
        samples.iter().map(|(_, value)| *value),
    )?)
    .map_err(|_| invalid_segment_data("Gorilla candidate bytes exceed u64"))?;
    let gorilla_payload_bytes = timestamp_bytes
        .checked_add(gorilla_value_bytes)
        .ok_or_else(|| invalid_segment_data("Gorilla candidate payload bytes overflow"))?;
    let raw_indexed_bytes = raw_payload_bytes
        .checked_add(40)
        .ok_or_else(|| invalid_segment_data("RawF64 candidate indexed bytes overflow"))?;
    let gorilla_indexed_bytes = gorilla_payload_bytes
        .checked_add(40)
        .ok_or_else(|| invalid_segment_data("Gorilla candidate indexed bytes overflow"))?;

    let (expected_payload_bytes, expected_indexed_bytes) = match layout.encoding {
        ChunkEncoding::RawF64 => (raw_payload_bytes, raw_indexed_bytes),
        ChunkEncoding::Gorilla => (gorilla_payload_bytes, gorilla_indexed_bytes),
        _ => {
            return Err(invalid_segment_data(
                "Float chunk uses an unsupported codec for candidate reconciliation",
            ));
        }
    };
    if expected_payload_bytes != u64::from(layout.payload_bytes)
        || expected_indexed_bytes != u64::from(indexed_bytes)
    {
        return Err(invalid_segment_data(
            "Float codec candidate disagrees with the canonical persisted layout",
        ));
    }

    checked_add(&mut evidence.chunks, 1, "Float candidate chunks")?;
    checked_add(&mut evidence.points, points, "Float candidate points")?;
    checked_add(
        &mut evidence.existing_indexed_bytes,
        u64::from(indexed_bytes),
        "Float existing indexed bytes",
    )?;
    checked_add(
        &mut evidence.existing_payload_bytes,
        u64::from(layout.payload_bytes),
        "Float existing payload bytes",
    )?;
    checked_add(
        &mut evidence.raw_f64_candidate_indexed_bytes,
        raw_indexed_bytes,
        "RawF64 candidate indexed bytes",
    )?;
    checked_add(
        &mut evidence.raw_f64_candidate_payload_bytes,
        raw_payload_bytes,
        "RawF64 candidate payload bytes",
    )?;
    checked_add(
        &mut evidence.gorilla_candidate_indexed_bytes,
        gorilla_indexed_bytes,
        "Gorilla candidate indexed bytes",
    )?;
    checked_add(
        &mut evidence.gorilla_candidate_payload_bytes,
        gorilla_payload_bytes,
        "Gorilla candidate payload bytes",
    )?;
    checked_add(
        &mut evidence.adaptive_min_indexed_bytes,
        raw_indexed_bytes.min(gorilla_indexed_bytes),
        "adaptive Float candidate indexed bytes",
    )?;
    checked_add(
        &mut evidence.adaptive_min_payload_bytes,
        raw_payload_bytes.min(gorilla_payload_bytes),
        "adaptive Float candidate payload bytes",
    )?;

    let ordering = raw_payload_bytes.cmp(&gorilla_payload_bytes);
    let winner = match ordering {
        std::cmp::Ordering::Less => &mut evidence.raw_f64_wins,
        std::cmp::Ordering::Greater => &mut evidence.gorilla_wins,
        std::cmp::Ordering::Equal => &mut evidence.ties,
    };
    checked_add(&mut winner.chunks, 1, "Float candidate winner chunks")?;
    checked_add(&mut winner.points, points, "Float candidate winner points")?;
    let selected = if ordering == std::cmp::Ordering::Greater {
        &mut evidence.adaptive_gorilla_selections
    } else {
        &mut evidence.adaptive_raw_f64_selections
    };
    checked_add(&mut selected.chunks, 1, "Float adaptive selection chunks")?;
    checked_add(
        &mut selected.points,
        points,
        "Float adaptive selection points",
    )?;
    Ok(())
}

pub(in super::super) fn observe_float_value_distribution(
    candidates: &mut FloatCodecCandidatesAccumulator,
    samples: &[(u64, f64)],
) -> io::Result<()> {
    let evidence = &mut candidates.evidence;
    let mut previous: Option<u64> = None;
    let mut previous_leading = 0u8;
    let mut previous_trailing = 0u8;
    let mut has_window = false;

    for (_, value) in samples {
        let bits = value.to_bits();
        let classification = if bits == PROMETHEUS_STALE_NAN_BITS {
            &mut evidence.stale_nan_points
        } else if value.is_nan() {
            &mut evidence.ordinary_nan_points
        } else if *value == f64::INFINITY {
            &mut evidence.positive_infinity_points
        } else if *value == f64::NEG_INFINITY {
            &mut evidence.negative_infinity_points
        } else if bits == 0 {
            &mut evidence.positive_zero_points
        } else if bits == (-0.0f64).to_bits() {
            &mut evidence.negative_zero_points
        } else {
            &mut evidence.finite_nonzero_points
        };
        checked_add(classification, 1, "Float value distribution")?;

        if let Some(previous) = previous {
            let xor = previous ^ bits;
            if xor == 0 {
                checked_add(
                    &mut evidence.repeated_xor_points,
                    1,
                    "repeated Float XOR points",
                )?;
            } else {
                let leading = xor.leading_zeros() as u8;
                let trailing = xor.trailing_zeros() as u8;
                let significant_bits = 64u64
                    .checked_sub(u64::from(leading))
                    .and_then(|bits| bits.checked_sub(u64::from(trailing)))
                    .ok_or_else(|| invalid_segment_data("Float XOR width underflows"))?;
                candidates
                    .xor_significant_bits_histogram
                    .observe(significant_bits)?;
                if has_window && leading >= previous_leading && trailing >= previous_trailing {
                    checked_add(
                        &mut evidence.reused_window_points,
                        1,
                        "reused Gorilla window points",
                    )?;
                } else {
                    checked_add(
                        &mut evidence.new_window_points,
                        1,
                        "new Gorilla window points",
                    )?;
                    previous_leading = leading.min(GORILLA_LEADING_MAX);
                    previous_trailing = trailing;
                    has_window = true;
                }
            }
        }
        previous = Some(bits);
    }
    Ok(())
}
