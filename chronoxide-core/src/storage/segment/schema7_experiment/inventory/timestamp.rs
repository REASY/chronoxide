use std::collections::BTreeMap;
use std::io;

use super::super::super::invalid_segment_data;
use super::super::helpers::{
    checked_add, chunk_encoding_from_inventory_id, chunk_encoding_name,
    chunk_kind_from_inventory_id, chunk_kind_name,
};
use super::super::{
    ExperimentalCodecWinnerTotals, ExperimentalTimestampCandidateEvidence,
    ExperimentalTimestampCodecCandidate, ExperimentalTimestampCodecCandidates,
    ExperimentalTimestampKindEncodingEvidence, ExperimentalTimestampShapeEvidence,
};

const TIMESTAMP_CODEC_SCOPE: &str =
    "native_payload_timestamp_stream_only; typed_scalar_lane_duplicate_timestamps_excluded";
pub(in super::super) const TIMESTAMP_CODEC_TIE_RULE: &str = "first minimum in stable priority order: current_offset_uleb, adjacent_delta_uleb, delta_of_delta_zigzag_uleb128, fixed_step_residual_bitpack";
const TIMESTAMP_CODEC_COUNT: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimestampBlockShape {
    SinglePoint = 0,
    ConstantZeroStep = 1,
    ConstantPositiveStep = 2,
    VariableStep = 3,
}

impl TimestampBlockShape {
    const ALL: [Self; 4] = [
        Self::SinglePoint,
        Self::ConstantZeroStep,
        Self::ConstantPositiveStep,
        Self::VariableStep,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::SinglePoint => "single_point",
            Self::ConstantZeroStep => "constant_zero_step",
            Self::ConstantPositiveStep => "constant_positive_step",
            Self::VariableStep => "variable_step",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in super::super) struct TimestampCandidateSizes {
    pub(in super::super) bytes: [u64; TIMESTAMP_CODEC_COUNT],
    shape: TimestampBlockShape,
}

#[derive(Default)]
struct TimestampCandidateEvidenceAccumulator {
    chunks: u64,
    points: u64,
    bytes: [u64; TIMESTAMP_CODEC_COUNT],
    unique_wins: [ExperimentalCodecWinnerTotals; TIMESTAMP_CODEC_COUNT],
    adaptive_selections: [ExperimentalCodecWinnerTotals; TIMESTAMP_CODEC_COUNT],
    adaptive_min_bytes: u64,
    tied_minima: ExperimentalCodecWinnerTotals,
}

impl TimestampCandidateEvidenceAccumulator {
    fn observe(&mut self, sizes: TimestampCandidateSizes, points: u64) -> io::Result<()> {
        checked_add(&mut self.chunks, 1, "timestamp candidate chunks")?;
        checked_add(&mut self.points, points, "timestamp candidate points")?;
        for (total, bytes) in self.bytes.iter_mut().zip(sizes.bytes) {
            checked_add(total, bytes, "timestamp candidate bytes")?;
        }

        let minimum = *sizes
            .bytes
            .iter()
            .min()
            .ok_or_else(|| invalid_segment_data("timestamp candidate set is empty"))?;
        checked_add(
            &mut self.adaptive_min_bytes,
            minimum,
            "adaptive timestamp bytes",
        )?;
        let mut minima = sizes
            .bytes
            .iter()
            .enumerate()
            .filter_map(|(index, bytes)| (*bytes == minimum).then_some(index));
        let selected = minima
            .next()
            .ok_or_else(|| invalid_segment_data("timestamp candidate minimum is missing"))?;
        let tied = minima.next().is_some();
        observe_winner_totals(&mut self.adaptive_selections[selected], points)?;
        if tied {
            observe_winner_totals(&mut self.tied_minima, points)?;
        } else {
            observe_winner_totals(&mut self.unique_wins[selected], points)?;
        }
        Ok(())
    }

    fn finish(self) -> ExperimentalTimestampCandidateEvidence {
        let candidate = |index| ExperimentalTimestampCodecCandidate {
            bytes: self.bytes[index],
            unique_wins: self.unique_wins[index],
            adaptive_selections: self.adaptive_selections[index],
        };
        ExperimentalTimestampCandidateEvidence {
            chunks: self.chunks,
            points: self.points,
            current_offset_uleb: candidate(0),
            adjacent_delta_uleb: candidate(1),
            delta_of_delta_zigzag_uleb128: candidate(2),
            fixed_step_residual_bitpack: candidate(3),
            adaptive_min_bytes: self.adaptive_min_bytes,
            tied_minima: self.tied_minima,
        }
    }
}

#[derive(Default)]
pub(in super::super) struct TimestampCodecCandidatesAccumulator {
    all_blocks: TimestampCandidateEvidenceAccumulator,
    by_shape: [TimestampCandidateEvidenceAccumulator; 4],
    by_kind_encoding: BTreeMap<(u8, u8), TimestampCandidateEvidenceAccumulator>,
}

impl TimestampCodecCandidatesAccumulator {
    pub(in super::super) fn observe<I>(
        &mut self,
        kind_encoding: (u8, u8),
        timestamps: I,
        expected_current_offset_uleb_bytes: u64,
    ) -> io::Result<()>
    where
        I: Clone + ExactSizeIterator<Item = u64>,
    {
        let points = u64::try_from(timestamps.len())
            .map_err(|_| invalid_segment_data("timestamp candidate point count exceeds u64"))?;
        let sizes = timestamp_candidate_sizes(timestamps)?;
        if sizes.bytes[0] != expected_current_offset_uleb_bytes {
            return Err(invalid_segment_data(
                "current offset-ULEB timestamp candidate disagrees with the native payload layout",
            ));
        }
        self.all_blocks.observe(sizes, points)?;
        self.by_shape[sizes.shape as usize].observe(sizes, points)?;
        self.by_kind_encoding
            .entry(kind_encoding)
            .or_default()
            .observe(sizes, points)
    }

    pub(in super::super) fn finish(self) -> ExperimentalTimestampCodecCandidates {
        let by_shape = TimestampBlockShape::ALL
            .into_iter()
            .zip(self.by_shape)
            .filter_map(|(shape, evidence)| {
                (evidence.chunks != 0).then(|| ExperimentalTimestampShapeEvidence {
                    shape: shape.name().to_owned(),
                    evidence: evidence.finish(),
                })
            })
            .collect();
        let by_kind_encoding = self
            .by_kind_encoding
            .into_iter()
            .map(
                |((kind, encoding), evidence)| ExperimentalTimestampKindEncodingEvidence {
                    kind: chunk_kind_name(chunk_kind_from_inventory_id(kind)).to_owned(),
                    encoding: chunk_encoding_name(chunk_encoding_from_inventory_id(encoding))
                        .to_owned(),
                    evidence: evidence.finish(),
                },
            )
            .collect();
        ExperimentalTimestampCodecCandidates {
            scope: TIMESTAMP_CODEC_SCOPE.to_owned(),
            tie_rule: TIMESTAMP_CODEC_TIE_RULE.to_owned(),
            selector_bytes_included: false,
            all_blocks: self.all_blocks.finish(),
            by_shape,
            by_kind_encoding,
        }
    }
}

fn observe_winner_totals(
    totals: &mut ExperimentalCodecWinnerTotals,
    points: u64,
) -> io::Result<()> {
    checked_add(&mut totals.chunks, 1, "codec winner chunks")?;
    checked_add(&mut totals.points, points, "codec winner points")
}

pub(in super::super) fn timestamp_candidate_sizes<I>(
    timestamps: I,
) -> io::Result<TimestampCandidateSizes>
where
    I: Clone + ExactSizeIterator<Item = u64>,
{
    let point_count = timestamps.len();
    if point_count == 0 {
        return Err(invalid_segment_data("timestamp candidate block is empty"));
    }
    let mut values = timestamps.clone();
    let first = values
        .next()
        .ok_or_else(|| invalid_segment_data("timestamp candidate block is empty"))?;
    let mut previous = first;
    let mut previous_delta = None;
    let mut first_delta = None;
    let mut constant_delta = true;
    let mut observed = 1usize;
    let mut current_offset_bytes = 8u64
        .checked_add(uleb128_u128_len(0))
        .ok_or_else(|| invalid_segment_data("current timestamp candidate overflows"))?;
    let mut adjacent_delta_bytes = 8u64;
    let mut delta_of_delta_bytes = 8u64;
    for timestamp in values {
        let offset = timestamp
            .checked_sub(first)
            .ok_or_else(|| invalid_segment_data("timestamp candidate block is not ordered"))?;
        current_offset_bytes = current_offset_bytes
            .checked_add(uleb128_u128_len(u128::from(offset)))
            .ok_or_else(|| invalid_segment_data("current timestamp candidate overflows"))?;
        let delta = timestamp
            .checked_sub(previous)
            .ok_or_else(|| invalid_segment_data("timestamp candidate block is not ordered"))?;
        adjacent_delta_bytes = adjacent_delta_bytes
            .checked_add(uleb128_u128_len(u128::from(delta)))
            .ok_or_else(|| invalid_segment_data("adjacent timestamp candidate overflows"))?;
        match previous_delta {
            None => {
                delta_of_delta_bytes = delta_of_delta_bytes
                    .checked_add(uleb128_u128_len(u128::from(delta)))
                    .ok_or_else(|| {
                        invalid_segment_data("delta-of-delta timestamp candidate overflows")
                    })?;
                first_delta = Some(delta);
            }
            Some(previous_delta) => {
                let delta_of_delta = i128::from(delta) - i128::from(previous_delta);
                delta_of_delta_bytes = delta_of_delta_bytes
                    .checked_add(uleb128_u128_len(zigzag_i128(delta_of_delta)))
                    .ok_or_else(|| {
                        invalid_segment_data("delta-of-delta timestamp candidate overflows")
                    })?;
                if first_delta != Some(delta) {
                    constant_delta = false;
                }
            }
        }
        previous_delta = Some(delta);
        previous = timestamp;
        observed = observed
            .checked_add(1)
            .ok_or_else(|| invalid_segment_data("timestamp candidate point count overflows"))?;
    }
    if observed != point_count {
        return Err(invalid_segment_data(
            "timestamp candidate iterator length is inconsistent",
        ));
    }

    let shape = if point_count == 1 {
        TimestampBlockShape::SinglePoint
    } else if constant_delta && first_delta == Some(0) {
        TimestampBlockShape::ConstantZeroStep
    } else if constant_delta {
        TimestampBlockShape::ConstantPositiveStep
    } else {
        TimestampBlockShape::VariableStep
    };
    let fixed_step_bytes = fixed_step_residual_bitpack_len(timestamps, first, previous)?;
    Ok(TimestampCandidateSizes {
        bytes: [
            current_offset_bytes,
            adjacent_delta_bytes,
            delta_of_delta_bytes,
            fixed_step_bytes,
        ],
        shape,
    })
}

fn fixed_step_residual_bitpack_len<I>(timestamps: I, first: u64, last: u64) -> io::Result<u64>
where
    I: ExactSizeIterator<Item = u64>,
{
    let point_count = timestamps.len();
    if point_count == 1 {
        return Ok(8);
    }
    let interval_count = point_count
        .checked_sub(1)
        .ok_or_else(|| invalid_segment_data("fixed-step interval count underflows"))?;
    let span = last
        .checked_sub(first)
        .ok_or_else(|| invalid_segment_data("fixed-step timestamps are not ordered"))?;
    let step = span
        / u64::try_from(interval_count)
            .map_err(|_| invalid_segment_data("fixed-step interval count exceeds u64"))?;
    let mut bit_width = 0u32;
    for (index, timestamp) in timestamps.enumerate().skip(1) {
        let baseline = u128::from(first)
            .checked_add(
                u128::try_from(index)
                    .map_err(|_| invalid_segment_data("fixed-step index exceeds u128"))?
                    .checked_mul(u128::from(step))
                    .ok_or_else(|| invalid_segment_data("fixed-step baseline overflows"))?,
            )
            .ok_or_else(|| invalid_segment_data("fixed-step baseline overflows"))?;
        let residual = i128::from(timestamp)
            - i128::try_from(baseline)
                .map_err(|_| invalid_segment_data("fixed-step baseline exceeds i128"))?;
        let encoded = zigzag_i128(residual);
        bit_width = bit_width.max(128 - encoded.leading_zeros());
    }
    let residual_bits = u64::from(bit_width)
        .checked_mul(
            u64::try_from(interval_count)
                .map_err(|_| invalid_segment_data("fixed-step interval count exceeds u64"))?,
        )
        .ok_or_else(|| invalid_segment_data("fixed-step residual bit count overflows"))?;
    let residual_bytes = residual_bits
        .checked_add(7)
        .ok_or_else(|| invalid_segment_data("fixed-step residual byte count overflows"))?
        / 8;
    8u64.checked_add(uleb128_u128_len(u128::from(step)))
        .and_then(|bytes| bytes.checked_add(1))
        .and_then(|bytes| bytes.checked_add(residual_bytes))
        .ok_or_else(|| invalid_segment_data("fixed-step timestamp candidate overflows"))
}

pub(in super::super) fn zigzag_i128(value: i128) -> u128 {
    ((value as u128) << 1) ^ ((value >> 127) as u128)
}

pub(in super::super) fn uleb128_u128_len(mut value: u128) -> u64 {
    let mut len = 1u64;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}
