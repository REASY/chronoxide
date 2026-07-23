//! Short, layout-neutral replay/readback gate for storage-schema experiments.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::time::Instant;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::storage::chunk::{
    ChunkEncoding, ChunkKind, ChunkSamples, DecodedChunkLayout, Schema7ChunkPrefixExpectation,
    decode_chunk_record_with_layout, verify_schema7_indexed_prefix,
};
use crate::storage::encoding::{
    VarLenEncoding, gorilla::GORILLA_LEADING_MAX, gorilla_encoded_len_bytes, varint_len,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OtlpAggregationTemporality, PROMETHEUS_STALE_NAN_BITS, SummaryValue, TypedSampleMetadata,
};
use crate::storage::index::SegmentIndexReadAt;

use super::full_validation::{
    RegisteredSegmentValidationPolicy, preflight_registered_segment,
    registered_validation_error_to_io,
};
use super::metadata_facade::{
    Schema7MetadataOpenContext, SegmentChunkAuthentication, SegmentMetadataLayout,
    SegmentMetadataReader, SegmentMetadataVisitControl, SegmentMetadataVisitError,
};
use super::*;

const VERIFIED_SELECTION_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-verified-storage-selection-v1\0";
const VERIFIED_EXACT_POSTINGS_FINGERPRINT_DOMAIN: &[u8] =
    b"chronoxide-verified-exact-postings-v1\0";
const VERIFIED_DECODED_SEMANTIC_FINGERPRINT_DOMAIN: &[u8] =
    b"chronoxide-verified-decoded-storage-semantics-v2\0";
const TOPOLOGY_INDEPENDENT_SEMANTIC_SERIES_DOMAIN: &[u8] =
    b"chronoxide-topology-independent-semantic-series-v1\0";
const TOPOLOGY_INDEPENDENT_SEMANTIC_RECORD_DOMAIN: &[u8] =
    b"chronoxide-topology-independent-semantic-record-v1\0";
const TOPOLOGY_INDEPENDENT_SEMANTIC_MULTISET_A_DOMAIN: &[u8] =
    b"chronoxide-topology-independent-semantic-multiset-a-v1\0";
const TOPOLOGY_INDEPENDENT_SEMANTIC_MULTISET_B_DOMAIN: &[u8] =
    b"chronoxide-topology-independent-semantic-multiset-b-v1\0";
const TOPOLOGY_INDEPENDENT_SEMANTIC_CORPUS_DOMAIN: &[u8] =
    b"chronoxide-topology-independent-semantic-corpus-v1\0";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalHistogramBucket {
    pub lower_inclusive: u64,
    pub upper_inclusive: u64,
    pub count: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ExperimentalPowerOfTwoHistogram {
    pub zero_count: u64,
    /// Non-empty `[2^n, 2^(n+1)-1]` buckets in ascending order.
    pub buckets: Vec<ExperimentalHistogramBucket>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalChunkEncodingInventory {
    pub kind: String,
    pub encoding: String,
    pub payload_layout: String,
    pub chunks: u64,
    pub points: u64,
    pub indexed_bytes: u64,
    pub common_header_bytes: u64,
    pub scalar_lane_bytes: u64,
    pub payload_bytes: u64,
    pub timestamp_base_bytes: u64,
    pub timestamp_delta_bytes: u64,
    pub value_bytes: u64,
    pub point_count_histogram: ExperimentalPowerOfTwoHistogram,
    pub cadence_ms_histogram: ExperimentalPowerOfTwoHistogram,
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct ExperimentalCodecWinnerTotals {
    pub chunks: u64,
    pub points: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ExperimentalFloatCodecCandidates {
    pub tie_rule: String,
    pub chunks: u64,
    pub points: u64,
    pub existing_indexed_bytes: u64,
    pub existing_payload_bytes: u64,
    pub raw_f64_candidate_indexed_bytes: u64,
    pub raw_f64_candidate_payload_bytes: u64,
    pub gorilla_candidate_indexed_bytes: u64,
    pub gorilla_candidate_payload_bytes: u64,
    pub adaptive_min_indexed_bytes: u64,
    pub adaptive_min_payload_bytes: u64,
    pub raw_f64_wins: ExperimentalCodecWinnerTotals,
    pub gorilla_wins: ExperimentalCodecWinnerTotals,
    pub ties: ExperimentalCodecWinnerTotals,
    pub adaptive_raw_f64_selections: ExperimentalCodecWinnerTotals,
    pub adaptive_gorilla_selections: ExperimentalCodecWinnerTotals,
    pub repeated_xor_points: u64,
    pub reused_window_points: u64,
    pub new_window_points: u64,
    pub xor_significant_bits_histogram: ExperimentalPowerOfTwoHistogram,
    pub positive_zero_points: u64,
    pub negative_zero_points: u64,
    pub finite_nonzero_points: u64,
    pub positive_infinity_points: u64,
    pub negative_infinity_points: u64,
    pub ordinary_nan_points: u64,
    pub stale_nan_points: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ExperimentalTimestampCodecCandidate {
    pub bytes: u64,
    pub unique_wins: ExperimentalCodecWinnerTotals,
    pub adaptive_selections: ExperimentalCodecWinnerTotals,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ExperimentalTimestampCandidateEvidence {
    pub chunks: u64,
    pub points: u64,
    pub current_offset_uleb: ExperimentalTimestampCodecCandidate,
    pub adjacent_delta_uleb: ExperimentalTimestampCodecCandidate,
    pub delta_of_delta_zigzag_uleb128: ExperimentalTimestampCodecCandidate,
    pub fixed_step_residual_bitpack: ExperimentalTimestampCodecCandidate,
    pub adaptive_min_bytes: u64,
    pub tied_minima: ExperimentalCodecWinnerTotals,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalTimestampShapeEvidence {
    pub shape: String,
    pub evidence: ExperimentalTimestampCandidateEvidence,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalTimestampKindEncodingEvidence {
    pub kind: String,
    pub encoding: String,
    pub evidence: ExperimentalTimestampCandidateEvidence,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalTimestampCodecCandidates {
    pub scope: String,
    pub tie_rule: String,
    pub selector_bytes_included: bool,
    pub all_blocks: ExperimentalTimestampCandidateEvidence,
    pub by_shape: Vec<ExperimentalTimestampShapeEvidence>,
    pub by_kind_encoding: Vec<ExperimentalTimestampKindEncodingEvidence>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalChunkInventory {
    pub layout: String,
    pub by_kind_encoding: Vec<ExperimentalChunkEncodingInventory>,
    pub raw_f64_vs_gorilla: ExperimentalFloatCodecCandidates,
    pub timestamp_candidates: ExperimentalTimestampCodecCandidates,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalExactPostingsVerification {
    pub logical_fingerprint: String,
    pub lists: u64,
    pub decoded_refs: u64,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalStorageVerification {
    pub schema_version: u16,
    pub footer_validation_enabled: bool,
    pub series_sample_per_segment: Option<u32>,
    pub verified_selection_fingerprint: String,
    pub decoded_semantic_fingerprint: String,
    /// Order-independent identity of every decoded `(canonical labels, kind,
    /// timestamp, logical value)` record. Present only for the explicit
    /// exhaustive topology-comparison verifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topology_independent_decoded_semantic_fingerprint: Option<String>,
    pub segments: u64,
    pub corpus_series: u64,
    pub series: u64,
    pub chunks: u64,
    pub chunks_by_kind: [u64; 5],
    pub samples: u64,
    pub logical_chunk_bytes: u64,
    pub chunk_inventory: ExperimentalChunkInventory,
    pub exact_postings: Option<ExperimentalExactPostingsVerification>,
    /// Total wall time. When footer validation is enabled this includes its
    /// registered full-file reads, also attributed to the metadata-runtime
    /// counters below.
    pub elapsed_ns: u64,
    pub metadata_read_calls: u64,
    pub metadata_read_bytes: u64,
    pub metadata_peak_retained_bytes: u64,
    pub metadata_peak_in_flight_bytes: u64,
    pub metadata_peak_open_files: u32,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
}

/// A streaming cryptographic multiset accumulator. Physical segment IDs,
/// local refs, chunk boundaries/order, offsets, and in-order/OOO lanes are not
/// inputs. Two independently domain-separated SHA-256 record digests are
/// summed modulo 2^256, retaining duplicate multiplicity without sorting the
/// corpus in memory; the final report value hashes both sums and the count.
struct TopologyIndependentDecodedSemanticAccumulator {
    sum_a: [u8; 32],
    sum_b: [u8; 32],
    samples: u64,
}

impl TopologyIndependentDecodedSemanticAccumulator {
    fn new() -> Self {
        Self {
            sum_a: [0; 32],
            sum_b: [0; 32],
            samples: 0,
        }
    }

    fn series_digest(labels: &[(String, String)]) -> io::Result<[u8; 32]> {
        let mut hasher = Sha256::new();
        hasher.update(TOPOLOGY_INDEPENDENT_SEMANTIC_SERIES_DOMAIN);
        hash_u32(
            &mut hasher,
            u32::try_from(labels.len())
                .map_err(|_| invalid_segment_data("semantic label count exceeds u32"))?,
        );
        for (name, value) in labels {
            hash_bytes(&mut hasher, name.as_bytes())?;
            hash_bytes(&mut hasher, value.as_bytes())?;
        }
        Ok(hasher.finalize().into())
    }

    fn observe_record(
        &mut self,
        series_digest: &[u8; 32],
        kind: u8,
        timestamp_ms: u64,
        value: &[u8],
    ) -> io::Result<()> {
        let mut record = Sha256::new();
        record.update(TOPOLOGY_INDEPENDENT_SEMANTIC_RECORD_DOMAIN);
        record.update(series_digest);
        record.update([kind]);
        hash_u64(&mut record, timestamp_ms);
        hash_bytes(&mut record, value)?;
        let record_digest: [u8; 32] = record.finalize().into();

        let digest_a: [u8; 32] = Sha256::new()
            .chain_update(TOPOLOGY_INDEPENDENT_SEMANTIC_MULTISET_A_DOMAIN)
            .chain_update(record_digest)
            .finalize()
            .into();
        let digest_b: [u8; 32] = Sha256::new()
            .chain_update(TOPOLOGY_INDEPENDENT_SEMANTIC_MULTISET_B_DOMAIN)
            .chain_update(record_digest)
            .finalize()
            .into();
        add_digest_mod_256(&mut self.sum_a, &digest_a);
        add_digest_mod_256(&mut self.sum_b, &digest_b);
        self.samples = self
            .samples
            .checked_add(1)
            .ok_or_else(|| invalid_segment_data("decoded semantic sample count overflows"))?;
        Ok(())
    }

    fn observe_samples(
        &mut self,
        series_digest: &[u8; 32],
        samples: &ChunkSamples,
        value_buffer: &mut Vec<u8>,
    ) -> io::Result<()> {
        match samples {
            ChunkSamples::Float(samples) => {
                for (timestamp_ms, value) in samples {
                    self.observe_record(
                        series_digest,
                        chunk_kind_id(ChunkKind::Float),
                        *timestamp_ms,
                        &value.to_bits().to_le_bytes(),
                    )?;
                }
            }
            ChunkSamples::Int64(samples) => {
                for (timestamp_ms, value) in samples {
                    self.observe_record(
                        series_digest,
                        chunk_kind_id(ChunkKind::Int64),
                        *timestamp_ms,
                        &value.to_le_bytes(),
                    )?;
                }
            }
            ChunkSamples::Histogram(samples) => {
                for (timestamp_ms, value) in samples {
                    value_buffer.clear();
                    value.encode_into(value_buffer)?;
                    self.observe_record(
                        series_digest,
                        chunk_kind_id(ChunkKind::Histogram),
                        *timestamp_ms,
                        value_buffer,
                    )?;
                }
            }
            ChunkSamples::ExponentialHistogram(samples) => {
                for (timestamp_ms, value) in samples {
                    value_buffer.clear();
                    value.encode_into(value_buffer)?;
                    self.observe_record(
                        series_digest,
                        chunk_kind_id(ChunkKind::ExponentialHistogram),
                        *timestamp_ms,
                        value_buffer,
                    )?;
                }
            }
            ChunkSamples::Summary(samples) => {
                for (timestamp_ms, value) in samples {
                    value_buffer.clear();
                    value.encode_into(value_buffer)?;
                    self.observe_record(
                        series_digest,
                        chunk_kind_id(ChunkKind::Summary),
                        *timestamp_ms,
                        value_buffer,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(TOPOLOGY_INDEPENDENT_SEMANTIC_CORPUS_DOMAIN);
        hash_u64(&mut hasher, self.samples);
        hasher.update(self.sum_a);
        hasher.update(self.sum_b);
        hex_digest(hasher.finalize().into())
    }
}

fn add_digest_mod_256(sum: &mut [u8; 32], digest: &[u8; 32]) {
    let mut carry = 0u16;
    for (target, value) in sum.iter_mut().zip(digest) {
        let next = u16::from(*target) + u16::from(*value) + carry;
        *target = next as u8;
        carry = next >> 8;
    }
}

struct ExactPostingsAccumulator {
    hasher: Sha256,
    lists: u64,
    decoded_refs: u64,
    encoded_bytes: u64,
    scratch: Vec<u8>,
}

impl ExactPostingsAccumulator {
    fn new(segment_count: u32) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(VERIFIED_EXACT_POSTINGS_FINGERPRINT_DOMAIN);
        hash_u32(&mut hasher, segment_count);
        Self {
            hasher,
            lists: 0,
            decoded_refs: 0,
            encoded_bytes: 0,
            scratch: Vec::with_capacity(64 * 1024),
        }
    }

    fn start_segment(&mut self, segment_id: &str) -> io::Result<()> {
        hash_bytes(&mut self.hasher, segment_id.as_bytes())
    }

    fn observe(
        &mut self,
        name_sym: u32,
        value_sym: u32,
        ref_count: u32,
        encoded_bytes: u64,
        refs: &[u32],
    ) -> io::Result<()> {
        if refs.len() != ref_count as usize {
            return Err(invalid_segment_data(
                "decoded exact-postings count disagrees with its protected record",
            ));
        }
        self.lists = self
            .lists
            .checked_add(1)
            .ok_or_else(|| invalid_segment_data("exact-postings list count overflows"))?;
        self.decoded_refs = self
            .decoded_refs
            .checked_add(u64::from(ref_count))
            .ok_or_else(|| invalid_segment_data("exact-postings ref count overflows"))?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| invalid_segment_data("exact-postings encoded bytes overflow"))?;

        hash_u32(&mut self.hasher, name_sym);
        hash_u32(&mut self.hasher, value_sym);
        hash_u32(&mut self.hasher, ref_count);
        for chunk in refs.chunks(16 * 1024) {
            self.scratch.clear();
            for series_ref in chunk {
                self.scratch.extend_from_slice(&series_ref.to_le_bytes());
            }
            self.hasher.update(&self.scratch);
        }
        Ok(())
    }

    fn finish(self) -> ExperimentalExactPostingsVerification {
        ExperimentalExactPostingsVerification {
            logical_fingerprint: hex_digest(self.hasher.finalize().into()),
            lists: self.lists,
            decoded_refs: self.decoded_refs,
            encoded_bytes: self.encoded_bytes,
        }
    }
}

struct PowerOfTwoHistogramAccumulator {
    zero_count: u64,
    buckets: [u64; 64],
}

impl Default for PowerOfTwoHistogramAccumulator {
    fn default() -> Self {
        Self {
            zero_count: 0,
            buckets: [0; 64],
        }
    }
}

impl PowerOfTwoHistogramAccumulator {
    fn observe(&mut self, value: u64) -> io::Result<()> {
        let slot = if value == 0 {
            &mut self.zero_count
        } else {
            let exponent = 63usize - value.leading_zeros() as usize;
            &mut self.buckets[exponent]
        };
        *slot = slot
            .checked_add(1)
            .ok_or_else(|| invalid_segment_data("histogram count overflows"))?;
        Ok(())
    }

    fn finish(self) -> ExperimentalPowerOfTwoHistogram {
        let buckets = self
            .buckets
            .into_iter()
            .enumerate()
            .filter_map(|(exponent, count)| {
                if count == 0 {
                    return None;
                }
                let lower_inclusive = 1u64 << exponent;
                let upper_inclusive = if exponent == 63 {
                    u64::MAX
                } else {
                    (1u64 << (exponent + 1)) - 1
                };
                Some(ExperimentalHistogramBucket {
                    lower_inclusive,
                    upper_inclusive,
                    count,
                })
            })
            .collect();
        ExperimentalPowerOfTwoHistogram {
            zero_count: self.zero_count,
            buckets,
        }
    }
}

#[derive(Default)]
struct ChunkEncodingInventoryAccumulator {
    chunks: u64,
    points: u64,
    indexed_bytes: u64,
    common_header_bytes: u64,
    scalar_lane_bytes: u64,
    payload_bytes: u64,
    timestamp_base_bytes: u64,
    timestamp_delta_bytes: u64,
    value_bytes: u64,
    point_count_histogram: PowerOfTwoHistogramAccumulator,
    cadence_ms_histogram: PowerOfTwoHistogramAccumulator,
}

impl ChunkEncodingInventoryAccumulator {
    fn observe(
        &mut self,
        layout: &DecodedChunkLayout,
        indexed_bytes: u32,
        expected_min_time_ms: u64,
        expected_max_time_ms: u64,
        timestamps: impl IntoIterator<Item = u64>,
    ) -> io::Result<()> {
        let indexed_components = layout
            .common_header_bytes
            .checked_add(layout.scalar_lane_bytes)
            .and_then(|bytes| bytes.checked_add(layout.payload_bytes))
            .ok_or_else(|| invalid_segment_data("indexed chunk byte components overflow"))?;
        if indexed_components != indexed_bytes {
            return Err(invalid_segment_data(
                "indexed chunk byte components do not equal its locator length",
            ));
        }
        let payload_components = layout
            .timestamp_base_bytes
            .checked_add(layout.timestamp_delta_bytes)
            .and_then(|bytes| bytes.checked_add(layout.value_bytes))
            .ok_or_else(|| invalid_segment_data("chunk payload byte components overflow"))?;
        if payload_components != layout.payload_bytes {
            return Err(invalid_segment_data(
                "chunk payload byte components do not equal its payload length",
            ));
        }

        let mut observed_points = 0u64;
        let mut first_timestamp = None;
        let mut previous_timestamp = None;
        for timestamp_ms in timestamps {
            first_timestamp.get_or_insert(timestamp_ms);
            if let Some(previous) = previous_timestamp {
                let cadence_ms = timestamp_ms.checked_sub(previous).ok_or_else(|| {
                    invalid_segment_data("decoded chunk timestamps are not ordered")
                })?;
                self.cadence_ms_histogram.observe(cadence_ms)?;
            }
            previous_timestamp = Some(timestamp_ms);
            observed_points = observed_points
                .checked_add(1)
                .ok_or_else(|| invalid_segment_data("decoded point count overflows"))?;
        }
        if observed_points != u64::from(layout.num_points) {
            return Err(invalid_segment_data(
                "decoded point count disagrees with the chunk header",
            ));
        }
        if first_timestamp != Some(expected_min_time_ms)
            || previous_timestamp != Some(expected_max_time_ms)
        {
            return Err(invalid_segment_data(
                "decoded timestamp range disagrees with the chunk header",
            ));
        }

        checked_add(&mut self.chunks, 1, "chunk inventory count")?;
        checked_add(&mut self.points, observed_points, "chunk inventory points")?;
        checked_add(
            &mut self.indexed_bytes,
            u64::from(indexed_bytes),
            "chunk inventory indexed bytes",
        )?;
        checked_add(
            &mut self.common_header_bytes,
            u64::from(layout.common_header_bytes),
            "chunk inventory common-header bytes",
        )?;
        checked_add(
            &mut self.scalar_lane_bytes,
            u64::from(layout.scalar_lane_bytes),
            "chunk inventory scalar-lane bytes",
        )?;
        checked_add(
            &mut self.payload_bytes,
            u64::from(layout.payload_bytes),
            "chunk inventory payload bytes",
        )?;
        checked_add(
            &mut self.timestamp_base_bytes,
            u64::from(layout.timestamp_base_bytes),
            "chunk inventory timestamp-base bytes",
        )?;
        checked_add(
            &mut self.timestamp_delta_bytes,
            u64::from(layout.timestamp_delta_bytes),
            "chunk inventory timestamp-delta bytes",
        )?;
        checked_add(
            &mut self.value_bytes,
            u64::from(layout.value_bytes),
            "chunk inventory value bytes",
        )?;
        self.point_count_histogram
            .observe(u64::from(layout.num_points))?;
        Ok(())
    }

    fn finish(
        self,
        kind: ChunkKind,
        encoding: ChunkEncoding,
    ) -> ExperimentalChunkEncodingInventory {
        ExperimentalChunkEncodingInventory {
            kind: chunk_kind_name(kind).to_owned(),
            encoding: chunk_encoding_name(encoding).to_owned(),
            payload_layout: chunk_payload_layout_name(kind, encoding).to_owned(),
            chunks: self.chunks,
            points: self.points,
            indexed_bytes: self.indexed_bytes,
            common_header_bytes: self.common_header_bytes,
            scalar_lane_bytes: self.scalar_lane_bytes,
            payload_bytes: self.payload_bytes,
            timestamp_base_bytes: self.timestamp_base_bytes,
            timestamp_delta_bytes: self.timestamp_delta_bytes,
            value_bytes: self.value_bytes,
            point_count_histogram: self.point_count_histogram.finish(),
            cadence_ms_histogram: self.cadence_ms_histogram.finish(),
        }
    }
}

const TIMESTAMP_CODEC_SCOPE: &str =
    "native_payload_timestamp_stream_only; typed_scalar_lane_duplicate_timestamps_excluded";
const TIMESTAMP_CODEC_TIE_RULE: &str = "first minimum in stable priority order: current_offset_uleb, adjacent_delta_uleb, delta_of_delta_zigzag_uleb128, fixed_step_residual_bitpack";
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
struct TimestampCandidateSizes {
    bytes: [u64; TIMESTAMP_CODEC_COUNT],
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
struct TimestampCodecCandidatesAccumulator {
    all_blocks: TimestampCandidateEvidenceAccumulator,
    by_shape: [TimestampCandidateEvidenceAccumulator; 4],
    by_kind_encoding: BTreeMap<(u8, u8), TimestampCandidateEvidenceAccumulator>,
}

impl TimestampCodecCandidatesAccumulator {
    fn observe<I>(
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

    fn finish(self) -> ExperimentalTimestampCodecCandidates {
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

fn timestamp_candidate_sizes<I>(timestamps: I) -> io::Result<TimestampCandidateSizes>
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

fn zigzag_i128(value: i128) -> u128 {
    ((value as u128) << 1) ^ ((value >> 127) as u128)
}

fn uleb128_u128_len(mut value: u128) -> u64 {
    let mut len = 1u64;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

#[derive(Default)]
struct FloatCodecCandidatesAccumulator {
    evidence: ExperimentalFloatCodecCandidates,
    xor_significant_bits_histogram: PowerOfTwoHistogramAccumulator,
}

impl FloatCodecCandidatesAccumulator {
    fn finish(mut self) -> ExperimentalFloatCodecCandidates {
        self.evidence.tie_rule =
            "RAW_F64 wins equal payload-byte ties; then compare decode cost before activation"
                .to_owned();
        self.evidence.xor_significant_bits_histogram = self.xor_significant_bits_histogram.finish();
        self.evidence
    }
}

#[derive(Default)]
struct ChunkInventoryAccumulator {
    by_kind_encoding: BTreeMap<(u8, u8), ChunkEncodingInventoryAccumulator>,
    float_candidates: FloatCodecCandidatesAccumulator,
    timestamp_candidates: TimestampCodecCandidatesAccumulator,
}

impl ChunkInventoryAccumulator {
    fn observe(
        &mut self,
        layout: &DecodedChunkLayout,
        indexed_bytes: u32,
        min_time_ms: u64,
        max_time_ms: u64,
        samples: &ChunkSamples,
    ) -> io::Result<()> {
        let current_offset_uleb_bytes = layout
            .timestamp_base_bytes
            .checked_add(layout.timestamp_delta_bytes)
            .map(u64::from)
            .ok_or_else(|| invalid_segment_data("native timestamp byte count overflows"))?;
        let key = (
            chunk_kind_id(layout.kind),
            chunk_encoding_id(layout.encoding),
        );
        let inventory = self.by_kind_encoding.entry(key).or_default();
        match samples {
            ChunkSamples::Float(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
                observe_float_codec_candidates(
                    &mut self.float_candidates,
                    layout,
                    indexed_bytes,
                    values,
                )?;
            }
            ChunkSamples::Int64(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
            ChunkSamples::Histogram(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
            ChunkSamples::ExponentialHistogram(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
            ChunkSamples::Summary(values) => {
                self.timestamp_candidates.observe(
                    key,
                    values.iter().map(|(ts, _)| *ts),
                    current_offset_uleb_bytes,
                )?;
                inventory.observe(
                    layout,
                    indexed_bytes,
                    min_time_ms,
                    max_time_ms,
                    values.iter().map(|(ts, _)| *ts),
                )?;
            }
        }
        Ok(())
    }

    fn finish(self) -> ExperimentalChunkInventory {
        let by_kind_encoding = self
            .by_kind_encoding
            .into_iter()
            .map(|((kind, encoding), inventory)| {
                inventory.finish(
                    chunk_kind_from_inventory_id(kind),
                    chunk_encoding_from_inventory_id(encoding),
                )
            })
            .collect();
        ExperimentalChunkInventory {
            layout: "sealed_chunk_v1".to_owned(),
            by_kind_encoding,
            raw_f64_vs_gorilla: self.float_candidates.finish(),
            timestamp_candidates: self.timestamp_candidates.finish(),
        }
    }
}

fn observe_float_codec_candidates(
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

fn observe_float_value_distribution(
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

struct DecodedSemanticAccumulator {
    hasher: Sha256,
    series_lanes: BTreeMap<(u8, u8), DecodedSemanticLaneAccumulator>,
}

impl DecodedSemanticAccumulator {
    fn new(segment_count: u32, series_sample_per_segment: Option<u32>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(VERIFIED_DECODED_SEMANTIC_FINGERPRINT_DOMAIN);
        match series_sample_per_segment {
            Some(limit) => {
                hasher.update([1]);
                hash_u32(&mut hasher, limit);
            }
            None => hasher.update([0]),
        }
        hash_u32(&mut hasher, segment_count);
        Self {
            hasher,
            series_lanes: BTreeMap::new(),
        }
    }

    fn start_segment(
        &mut self,
        segment_id: &str,
        start_ms: u64,
        end_ms: u64,
        selected_series: u32,
    ) -> io::Result<()> {
        self.hasher.update([0x01]);
        hash_bytes(&mut self.hasher, segment_id.as_bytes())?;
        hash_u64(&mut self.hasher, start_ms);
        hash_u64(&mut self.hasher, end_ms);
        hash_u32(&mut self.hasher, selected_series);
        Ok(())
    }

    fn start_series(
        &mut self,
        series_id: u64,
        kind_mask: u8,
        labels: &[(String, String)],
    ) -> io::Result<()> {
        if !self.series_lanes.is_empty() {
            return Err(invalid_segment_data(
                "semantic fingerprint started a series before finishing the prior series",
            ));
        }
        self.hasher.update([0x02]);
        hash_u64(&mut self.hasher, series_id);
        self.hasher.update([kind_mask]);
        hash_u32(
            &mut self.hasher,
            u32::try_from(labels.len())
                .map_err(|_| invalid_segment_data("semantic label count exceeds u32"))?,
        );
        for (name, value) in labels {
            hash_bytes(&mut self.hasher, name.as_bytes())?;
            hash_bytes(&mut self.hasher, value.as_bytes())?;
        }
        Ok(())
    }

    fn observe_chunk(&mut self, file_id: u8, samples: &ChunkSamples) -> io::Result<u64> {
        let kind = match samples {
            ChunkSamples::Float(_) => ChunkKind::Float,
            ChunkSamples::Int64(_) => ChunkKind::Int64,
            ChunkSamples::Histogram(_) => ChunkKind::Histogram,
            ChunkSamples::ExponentialHistogram(_) => ChunkKind::ExponentialHistogram,
            ChunkSamples::Summary(_) => ChunkKind::Summary,
        };
        self.series_lanes
            .entry((file_id, chunk_kind_id(kind)))
            .or_insert_with(|| DecodedSemanticLaneAccumulator::new(file_id, kind))
            .observe(samples)
    }

    fn finish_series(&mut self, sample_count: u64) -> io::Result<()> {
        let lanes = std::mem::take(&mut self.series_lanes);
        hash_u32(
            &mut self.hasher,
            u32::try_from(lanes.len())
                .map_err(|_| invalid_segment_data("semantic lane count exceeds u32"))?,
        );
        let mut observed_samples = 0u64;
        for ((file_id, kind), lane) in lanes {
            let (lane_samples, digest) = lane.finish();
            self.hasher.update([0x03, file_id, kind]);
            hash_u64(&mut self.hasher, lane_samples);
            self.hasher.update(digest);
            checked_add(
                &mut observed_samples,
                lane_samples,
                "semantic lane sample count",
            )?;
        }
        if observed_samples != sample_count {
            return Err(invalid_segment_data(
                "semantic lane sample total disagrees with the series sample count",
            ));
        }
        self.hasher.update([0x04]);
        hash_u64(&mut self.hasher, sample_count);
        Ok(())
    }

    fn finish(self) -> String {
        hex_digest(self.hasher.finalize().into())
    }
}

struct DecodedSemanticLaneAccumulator {
    hasher: Sha256,
    samples: u64,
}

impl DecodedSemanticLaneAccumulator {
    fn new(file_id: u8, kind: ChunkKind) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"chronoxide-decoded-semantic-lane-v1\0");
        hasher.update([file_id, chunk_kind_id(kind)]);
        Self { hasher, samples: 0 }
    }

    fn observe(&mut self, samples: &ChunkSamples) -> io::Result<u64> {
        let observed = match samples {
            ChunkSamples::Float(values) => {
                for (timestamp_ms, value) in values {
                    self.start_sample(*timestamp_ms);
                    hash_u64(&mut self.hasher, value.to_bits());
                }
                checked_sample_len(values.len())?
            }
            ChunkSamples::Int64(values) => {
                for (timestamp_ms, value) in values {
                    self.start_sample(*timestamp_ms);
                    self.hasher.update(value.to_le_bytes());
                }
                checked_sample_len(values.len())?
            }
            ChunkSamples::Histogram(values) => {
                for (timestamp_ms, value) in values {
                    self.start_sample(*timestamp_ms);
                    self.hash_histogram(value)?;
                }
                checked_sample_len(values.len())?
            }
            ChunkSamples::ExponentialHistogram(values) => {
                for (timestamp_ms, value) in values {
                    self.start_sample(*timestamp_ms);
                    self.hash_exponential_histogram(value)?;
                }
                checked_sample_len(values.len())?
            }
            ChunkSamples::Summary(values) => {
                for (timestamp_ms, value) in values {
                    self.start_sample(*timestamp_ms);
                    self.hash_summary(value)?;
                }
                checked_sample_len(values.len())?
            }
        };
        checked_add(&mut self.samples, observed, "semantic lane samples")?;
        Ok(observed)
    }

    fn finish(self) -> (u64, [u8; 32]) {
        (self.samples, self.hasher.finalize().into())
    }

    fn start_sample(&mut self, timestamp_ms: u64) {
        self.hasher.update([0x01]);
        hash_u64(&mut self.hasher, timestamp_ms);
    }

    fn hash_histogram(&mut self, value: &HistogramValue) -> io::Result<()> {
        self.hash_typed_metadata(value.metadata);
        hash_u64(&mut self.hasher, value.count);
        self.hash_optional_f64(value.sum);
        self.hash_optional_f64(value.min);
        self.hash_optional_f64(value.max);
        self.hash_f64_slice(&value.explicit_bounds)?;
        self.hash_u64_slice(&value.bucket_counts)
    }

    fn hash_exponential_histogram(&mut self, value: &ExponentialHistogramValue) -> io::Result<()> {
        self.hash_typed_metadata(value.metadata);
        hash_u64(&mut self.hasher, value.count);
        self.hash_optional_f64(value.sum);
        self.hash_optional_f64(value.min);
        self.hash_optional_f64(value.max);
        self.hasher.update(value.scale.to_le_bytes());
        hash_u64(&mut self.hasher, value.zero_threshold.to_bits());
        hash_u64(&mut self.hasher, value.zero_count);
        self.hash_exponential_histogram_buckets(&value.positive)?;
        self.hash_exponential_histogram_buckets(&value.negative)
    }

    fn hash_summary(&mut self, value: &SummaryValue) -> io::Result<()> {
        self.hash_typed_metadata(value.metadata);
        hash_u64(&mut self.hasher, value.count);
        hash_u64(&mut self.hasher, value.sum.to_bits());
        hash_u32(
            &mut self.hasher,
            u32::try_from(value.quantiles.len())
                .map_err(|_| invalid_segment_data("Summary quantile count exceeds u32"))?,
        );
        for quantile in &value.quantiles {
            hash_u64(&mut self.hasher, quantile.quantile.to_bits());
            hash_u64(&mut self.hasher, quantile.value.to_bits());
        }
        Ok(())
    }

    fn hash_typed_metadata(&mut self, metadata: TypedSampleMetadata) {
        match metadata.start_time_ms {
            Some(start_time_ms) => {
                self.hasher.update([1]);
                hash_u64(&mut self.hasher, start_time_ms);
            }
            None => self.hasher.update([0]),
        }
        hash_u32(&mut self.hasher, metadata.flags);
        self.hasher.update([
            temporality_id(metadata.temporality),
            reset_hint_id(metadata.reset_hint),
        ]);
    }

    fn hash_optional_f64(&mut self, value: Option<f64>) {
        match value {
            Some(value) => {
                self.hasher.update([1]);
                hash_u64(&mut self.hasher, value.to_bits());
            }
            None => self.hasher.update([0]),
        }
    }

    fn hash_f64_slice(&mut self, values: &[f64]) -> io::Result<()> {
        hash_u32(
            &mut self.hasher,
            u32::try_from(values.len())
                .map_err(|_| invalid_segment_data("f64 semantic value count exceeds u32"))?,
        );
        for value in values {
            hash_u64(&mut self.hasher, value.to_bits());
        }
        Ok(())
    }

    fn hash_u64_slice(&mut self, values: &[u64]) -> io::Result<()> {
        hash_u32(
            &mut self.hasher,
            u32::try_from(values.len())
                .map_err(|_| invalid_segment_data("u64 semantic value count exceeds u32"))?,
        );
        for value in values {
            hash_u64(&mut self.hasher, *value);
        }
        Ok(())
    }

    fn hash_exponential_histogram_buckets(
        &mut self,
        buckets: &ExponentialHistogramBuckets,
    ) -> io::Result<()> {
        self.hasher.update(buckets.offset.to_le_bytes());
        self.hash_u64_slice(&buckets.counts)
    }
}

fn checked_sample_len(len: usize) -> io::Result<u64> {
    u64::try_from(len).map_err(|_| invalid_segment_data("semantic sample count exceeds u64"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalChunk {
    file_id: u8,
    kind: u8,
    flags: u16,
    min_time_ms: u64,
    max_time_ms: u64,
    file_offset: u64,
    length: u32,
    scalar_lane_offset: u32,
    scalar_lane_len: u32,
    digest: [u8; 32],
}

/// Walks one homogeneous schema-6, schema-7, or schema-8 corpus and verifies each selected
/// series identity/label route and decodes each of its indexed chunks. A
/// missing sample limit selects the complete corpus; a limit selects stable,
/// evenly spaced refs in every segment for a short real-corpus A/B gate.
pub fn verify_experimental_storage_corpus(
    segments_dir: impl AsRef<Path>,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
    series_sample_per_segment: Option<u32>,
) -> io::Result<ExperimentalStorageVerification> {
    verify_experimental_storage_corpus_impl(
        segments_dir.as_ref(),
        schema,
        validate_segment_footers,
        series_sample_per_segment,
        false,
        false,
    )
}

pub fn verify_experimental_storage_corpus_with_exact_postings(
    segments_dir: impl AsRef<Path>,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
    series_sample_per_segment: Option<u32>,
) -> io::Result<ExperimentalStorageVerification> {
    verify_experimental_storage_corpus_impl(
        segments_dir.as_ref(),
        schema,
        validate_segment_footers,
        series_sample_per_segment,
        true,
        false,
    )
}

/// Performs the exhaustive exact-postings gate and additionally fingerprints
/// every decoded logical sample independently of physical topology.
pub fn verify_experimental_storage_corpus_with_decoded_semantics(
    segments_dir: impl AsRef<Path>,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
) -> io::Result<ExperimentalStorageVerification> {
    verify_experimental_storage_corpus_impl(
        segments_dir.as_ref(),
        schema,
        validate_segment_footers,
        None,
        true,
        true,
    )
}

fn verify_experimental_storage_corpus_impl(
    segments_dir: &Path,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
    series_sample_per_segment: Option<u32>,
    verify_exact_postings: bool,
    fingerprint_topology_independent_semantics: bool,
) -> io::Result<ExperimentalStorageVerification> {
    let started = Instant::now();
    let inventory = read_manifest_inventory(segments_dir.join("manifest"))?
        .ok_or_else(|| invalid_segment_data("segment manifest is missing"))?;
    let segment_count = u32::try_from(inventory.segments.len())
        .map_err(|_| invalid_segment_data("segment count exceeds u32"))?;
    let runtime = open_metadata_runtime(MetadataGovernorConfig::default())?;
    let mut hasher = Sha256::new();
    hasher.update(VERIFIED_SELECTION_FINGERPRINT_DOMAIN);
    match series_sample_per_segment {
        Some(limit) => {
            hasher.update([1]);
            hash_u32(&mut hasher, limit);
        }
        None => hasher.update([0]),
    }
    hash_u32(&mut hasher, segment_count);
    let mut decoded_semantics =
        DecodedSemanticAccumulator::new(segment_count, series_sample_per_segment);
    let mut chunk_inventory = ChunkInventoryAccumulator::default();
    let mut exact_postings = if verify_exact_postings {
        if schema == SegmentStorageSchema::Schema6 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exhaustive integrity-checked exact-postings verification requires schema 7 or 8",
            ));
        }
        Some(ExactPostingsAccumulator::new(segment_count))
    } else {
        None
    };
    let mut topology_independent_semantics = fingerprint_topology_independent_semantics
        .then(TopologyIndependentDecodedSemanticAccumulator::new);
    let mut topology_independent_value_buffer = Vec::new();

    let mut total_series = 0u64;
    let mut corpus_series = 0u64;
    let mut total_chunks = 0u64;
    let mut chunks_by_kind = [0u64; 5];
    let mut total_samples = 0u64;
    let mut total_chunk_bytes = 0u64;

    for manifest_segment in &inventory.segments {
        let segment_dir = segments_dir.join(&manifest_segment.segment_id);
        let (registered, footer, meta) = if validate_segment_footers {
            let policy = match schema {
                SegmentStorageSchema::Schema6 => {
                    RegisteredSegmentValidationPolicy::ValidatedSchema6
                }
                SegmentStorageSchema::Schema7 => RegisteredSegmentValidationPolicy::Schema7,
                SegmentStorageSchema::Schema8 => RegisteredSegmentValidationPolicy::Schema8,
            };
            let preflight = preflight_registered_segment(&runtime, &segment_dir, policy)
                .map_err(registered_validation_error_to_io)?;
            preflight
                .validate_footer_checksums()
                .map_err(registered_validation_error_to_io)?
                .into_open_parts()
        } else {
            let footer = match schema {
                SegmentStorageSchema::Schema6 => read_segment_footer_for_schema6(&segment_dir)?,
                SegmentStorageSchema::Schema7 => read_segment_footer_for_schema7(&segment_dir)?,
                SegmentStorageSchema::Schema8 => read_segment_footer_for_schema8(&segment_dir)?,
            };
            let meta: SegmentMeta = serde_json::from_slice(&fs::read(
                segment_dir.join(SegmentFile::MetaJson.filename()),
            )?)
            .map_err(io::Error::other)?;
            let registered =
                super::query_reader::register_segment_metadata(&runtime, &segment_dir, &footer)?;
            (registered, footer, meta)
        };
        validate_manifest_segment_meta(manifest_segment, &meta)?;
        let series_count = u32::try_from(meta.series)
            .map_err(|_| invalid_segment_data("segment series count exceeds u32"))?;
        corpus_series = corpus_series.saturating_add(u64::from(series_count));
        let sampled_refs = series_sample_per_segment
            .filter(|limit| *limit < series_count)
            .map(|limit| evenly_spaced_series_refs(series_count, limit));
        let selected_series = sampled_refs
            .as_ref()
            .map_or(series_count, |refs| refs.len() as u32);
        decoded_semantics.start_segment(
            &manifest_segment.segment_id,
            manifest_segment.start_ms,
            manifest_segment.end_ms,
            selected_series,
        )?;
        let layout = match schema {
            SegmentStorageSchema::Schema6 => SegmentMetadataLayout::Schema6 { series_count },
            SegmentStorageSchema::Schema7 => {
                SegmentMetadataLayout::Schema7(Schema7MetadataOpenContext {
                    series_file_len: footer_file_len(&footer, SegmentFile::Series)?,
                    chunk_index_file_len: footer_file_len(&footer, SegmentFile::ChunkIndex)?,
                    segment_start_ms: meta.start_ms,
                    segment_end_ms: meta.end_ms,
                    series_count,
                })
            }
            SegmentStorageSchema::Schema8 => {
                SegmentMetadataLayout::Schema8(Schema7MetadataOpenContext {
                    series_file_len: footer_file_len(&footer, SegmentFile::Series)?,
                    chunk_index_file_len: footer_file_len(&footer, SegmentFile::ChunkIndex)?,
                    segment_start_ms: meta.start_ms,
                    segment_end_ms: meta.end_ms,
                    series_count,
                })
            }
        };
        let metadata = SegmentMetadataReader::open(&registered, layout).map_err(facade_io)?;
        let session = metadata.query_session().map_err(facade_io)?;
        let root = session.bind_roots().map_err(facade_io)?;
        if root.series_count() != series_count {
            return Err(invalid_segment_data(
                "metadata root series count disagrees with meta.json",
            ));
        }

        if let Some(exact_postings) = exact_postings.as_mut() {
            exact_postings.start_segment(&manifest_segment.segment_id)?;
            let mut visitor_error = None;
            let exhausted = session
                .visit_authenticated_exact_postings(
                    &root,
                    |name_sym, value_sym, ref_count, encoded_bytes, refs| match exact_postings
                        .observe(name_sym, value_sym, ref_count, encoded_bytes, refs)
                    {
                        Ok(()) => true,
                        Err(error) => {
                            visitor_error = Some(error);
                            false
                        }
                    },
                )
                .map_err(facade_io)?;
            if let Some(error) = visitor_error {
                return Err(error);
            }
            if !exhausted {
                return Err(invalid_segment_data(
                    "integrity-checked exact-postings verification stopped early",
                ));
            }
        }

        hash_bytes(&mut hasher, manifest_segment.segment_id.as_bytes())?;
        hash_u64(&mut hasher, manifest_segment.start_ms);
        hash_u64(&mut hasher, manifest_segment.end_ms);
        hash_u32(&mut hasher, series_count);
        hash_u32(&mut hasher, selected_series);

        let mut chunk_files = [
            File::open(segment_dir.join(SegmentFile::Chunks.filename()))?,
            File::open(segment_dir.join(SegmentFile::OooChunks.filename()))?,
        ];
        let mut chunk_buffer = Vec::new();
        const SERIES_BATCH: u32 = 409 * 16;
        let mut selected_offset = 0u32;
        while selected_offset < selected_series {
            let batch_end = selected_offset
                .saturating_add(SERIES_BATCH)
                .min(selected_series);
            let refs = if let Some(sampled_refs) = sampled_refs.as_ref() {
                sampled_refs[selected_offset as usize..batch_end as usize].to_vec()
            } else {
                (selected_offset..batch_end).collect::<Vec<_>>()
            };
            let candidates = session.series_ref_set(&root, &refs).map_err(facade_io)?;
            let mut batch_offset = 0usize;
            let visit = session.visit_verified_series(&root, &candidates, |series| {
                if refs.get(batch_offset).copied() != Some(series.series_ref()) {
                    return Err(invalid_segment_data(
                        "verified series refs do not match the ordered selection",
                    ));
                }
                batch_offset += 1;

                hash_u32(&mut hasher, series.series_ref());
                hash_u64(&mut hasher, series.series_id());
                hasher.update([series.kind_mask()]);
                hash_u32(
                    &mut hasher,
                    u32::try_from(series.labels().len())
                        .map_err(|_| invalid_segment_data("label count exceeds u32"))?,
                );
                let mut previous_label: Option<&[u8]> = None;
                for (name, value) in series.labels() {
                    if previous_label.is_some_and(|previous| previous >= name.as_bytes()) {
                        return Err(invalid_segment_data(
                            "verified series labels are not strictly ordered",
                        ));
                    }
                    hash_bytes(&mut hasher, name.as_bytes())?;
                    hash_bytes(&mut hasher, value.as_bytes())?;
                    previous_label = Some(name.as_bytes());
                }
                decoded_semantics.start_series(
                    series.series_id(),
                    series.kind_mask(),
                    series.labels(),
                )?;
                let topology_independent_series_digest = topology_independent_semantics
                    .as_ref()
                    .map(|_| {
                        TopologyIndependentDecodedSemanticAccumulator::series_digest(
                            series.labels(),
                        )
                    })
                    .transpose()?;

                let mut canonical = Vec::new();
                canonical
                    .try_reserve_exact(series.chunks().len())
                    .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
                let mut observed_kind_mask = 0u8;
                let mut semantic_sample_count = 0u64;
                series.chunks().visit(|locator| {
                    let file = chunk_files
                        .get_mut(usize::from(locator.file_id()))
                        .ok_or_else(|| invalid_segment_data("chunk locator file id is invalid"))?;
                    let length = usize::try_from(locator.chunk_len())
                        .map_err(|_| invalid_segment_data("chunk length exceeds usize"))?;
                    chunk_buffer.clear();
                    chunk_buffer
                        .try_reserve(length)
                        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
                    chunk_buffer.resize(length, 0);
                    SegmentIndexReadAt::read_exact_at(
                        file,
                        locator.file_offset(),
                        &mut chunk_buffer,
                    )?;

                    let authenticated_flags = match locator.authentication() {
                        SegmentChunkAuthentication::Schema6Legacy => locator.flags(),
                        SegmentChunkAuthentication::Schema7IndexedPrefix { crc32c } => {
                            let prefix_len = locator.indexed_prefix_len();
                            let prefix = chunk_buffer.get(..prefix_len).ok_or_else(|| {
                                invalid_segment_data("schema-7/8 chunk prefix is short")
                            })?;
                            verify_schema7_indexed_prefix(
                                &Schema7ChunkPrefixExpectation {
                                    series_ref: locator.series_ref(),
                                    kind: locator.kind(),
                                    min_time_ms: locator.min_time_ms(),
                                    max_time_ms: locator.max_time_ms(),
                                    length: locator.chunk_len(),
                                    scalar_lane_offset: locator.scalar_lane_offset(),
                                    scalar_lane_len: locator.scalar_lane_len(),
                                    indexed_prefix_crc32c: crc32c,
                                },
                                prefix,
                            )?
                            .flags
                        }
                    };

                    let (decoded, layout) = decode_chunk_record_with_layout(&chunk_buffer)?;
                    if decoded.series_ref != locator.series_ref()
                        || decoded.kind != locator.kind()
                        || decoded.min_time_ms != locator.min_time_ms()
                        || decoded.max_time_ms != locator.max_time_ms()
                    {
                        return Err(invalid_segment_data(
                            "decoded chunk header disagrees with its metadata locator",
                        ));
                    }
                    if layout.flags != authenticated_flags
                        || layout.scalar_lane_bytes != locator.scalar_lane_len()
                    {
                        return Err(invalid_segment_data(
                            "decoded chunk layout disagrees with its authenticated locator",
                        ));
                    }
                    if locator.min_time_ms() < manifest_segment.start_ms
                        || locator.max_time_ms() >= manifest_segment.end_ms
                    {
                        return Err(invalid_segment_data(
                            "chunk time range lies outside its segment",
                        ));
                    }
                    if let (Some(accumulator), Some(series_digest)) = (
                        topology_independent_semantics.as_mut(),
                        topology_independent_series_digest.as_ref(),
                    ) {
                        accumulator.observe_samples(
                            series_digest,
                            &decoded.samples,
                            &mut topology_independent_value_buffer,
                        )?;
                    }
                    let kind = chunk_kind_id(locator.kind());
                    observed_kind_mask |= 1u8 << kind;
                    chunks_by_kind[usize::from(kind)] =
                        chunks_by_kind[usize::from(kind)].saturating_add(1);
                    total_samples =
                        total_samples.saturating_add(chunk_sample_count(&decoded.samples));
                    total_chunk_bytes =
                        total_chunk_bytes.saturating_add(u64::from(locator.chunk_len()));
                    chunk_inventory.observe(
                        &layout,
                        locator.chunk_len(),
                        decoded.min_time_ms,
                        decoded.max_time_ms,
                        &decoded.samples,
                    )?;
                    checked_add(
                        &mut semantic_sample_count,
                        decoded_semantics.observe_chunk(locator.file_id(), &decoded.samples)?,
                        "semantic series sample count",
                    )?;
                    canonical.push(CanonicalChunk {
                        file_id: locator.file_id(),
                        kind,
                        flags: authenticated_flags,
                        min_time_ms: locator.min_time_ms(),
                        max_time_ms: locator.max_time_ms(),
                        file_offset: locator.file_offset(),
                        length: locator.chunk_len(),
                        scalar_lane_offset: locator.scalar_lane_offset(),
                        scalar_lane_len: locator.scalar_lane_len(),
                        digest: Sha256::digest(&chunk_buffer).into(),
                    });
                    Ok(SegmentMetadataVisitControl::Continue)
                })?;
                decoded_semantics.finish_series(semantic_sample_count)?;
                if canonical.is_empty() {
                    return Err(invalid_segment_data("verified series has no chunks"));
                }
                if observed_kind_mask != series.kind_mask() {
                    return Err(invalid_segment_data(
                        "verified series kind mask disagrees with its chunks",
                    ));
                }
                canonical.sort_unstable_by_key(|chunk| {
                    (
                        chunk.file_id,
                        chunk.file_offset,
                        chunk.min_time_ms,
                        chunk.max_time_ms,
                        chunk.kind,
                        chunk.digest,
                    )
                });
                let chunk_count = u64::try_from(canonical.len())
                    .map_err(|_| invalid_segment_data("chunk count exceeds u64"))?;
                hash_u32(
                    &mut hasher,
                    u32::try_from(canonical.len())
                        .map_err(|_| invalid_segment_data("chunk count exceeds u32"))?,
                );
                for chunk in canonical {
                    hasher.update([chunk.file_id, chunk.kind]);
                    hasher.update(chunk.flags.to_le_bytes());
                    hash_u64(&mut hasher, chunk.min_time_ms);
                    hash_u64(&mut hasher, chunk.max_time_ms);
                    hash_u64(&mut hasher, chunk.file_offset);
                    hash_u32(&mut hasher, chunk.length);
                    hash_u32(&mut hasher, chunk.scalar_lane_offset);
                    hash_u32(&mut hasher, chunk.scalar_lane_len);
                    hasher.update(chunk.digest);
                }
                total_chunks = total_chunks.saturating_add(chunk_count);
                total_series = total_series.saturating_add(1);
                Ok(SegmentMetadataVisitControl::Continue)
            });
            match visit {
                Ok(_) => {}
                Err(SegmentMetadataVisitError::Metadata(error)) => return Err(facade_io(error)),
                Err(SegmentMetadataVisitError::Visitor(error)) => return Err(error),
            }
            if batch_offset != refs.len() {
                return Err(invalid_segment_data(
                    "verified series batch did not cover every requested ref",
                ));
            }
            selected_offset = batch_end;
        }
        if selected_offset != selected_series {
            return Err(invalid_segment_data(
                "verified series visit did not cover the complete selection",
            ));
        }
    }

    let snapshot = runtime.snapshot();
    Ok(ExperimentalStorageVerification {
        schema_version: schema.footer_version(),
        footer_validation_enabled: validate_segment_footers,
        series_sample_per_segment,
        verified_selection_fingerprint: hex_digest(hasher.finalize().into()),
        decoded_semantic_fingerprint: decoded_semantics.finish(),
        topology_independent_decoded_semantic_fingerprint: topology_independent_semantics
            .map(TopologyIndependentDecodedSemanticAccumulator::finish),
        segments: u64::from(segment_count),
        corpus_series,
        series: total_series,
        chunks: total_chunks,
        chunks_by_kind,
        samples: total_samples,
        logical_chunk_bytes: total_chunk_bytes,
        chunk_inventory: chunk_inventory.finish(),
        exact_postings: exact_postings.map(ExactPostingsAccumulator::finish),
        elapsed_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        metadata_read_calls: snapshot.reads.issued.calls,
        metadata_read_bytes: snapshot.reads.issued.bytes,
        metadata_peak_retained_bytes: snapshot.governor.peak_retained_bytes,
        metadata_peak_in_flight_bytes: snapshot.governor.peak_in_flight_bytes,
        metadata_peak_open_files: snapshot.files.peak_open_files,
        metadata_cache_hits: snapshot.cache.hits,
        metadata_cache_misses: snapshot.cache.misses,
    })
}

fn footer_file_len(footer: &SegmentFooter, file: SegmentFile) -> io::Result<u64> {
    footer
        .files
        .iter()
        .find_map(|entry| (entry.file == file).then_some(entry.size))
        .ok_or_else(|| invalid_segment_data("segment footer omits a tracked file"))
}

fn evenly_spaced_series_refs(series_count: u32, limit: u32) -> Vec<u32> {
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

fn facade_io(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> io::Result<()> {
    hash_u32(
        hasher,
        u32::try_from(bytes.len())
            .map_err(|_| invalid_segment_data("fingerprint byte string exceeds u32"))?,
    );
    hasher.update(bytes);
    Ok(())
}

fn hash_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_le_bytes());
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_le_bytes());
}

fn checked_add(target: &mut u64, value: u64, field: &'static str) -> io::Result<()> {
    *target = target
        .checked_add(value)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("{field} overflow")))?;
    Ok(())
}

fn chunk_kind_id(kind: ChunkKind) -> u8 {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}

fn chunk_kind_name(kind: ChunkKind) -> &'static str {
    match kind {
        ChunkKind::Float => "float",
        ChunkKind::Int64 => "int64",
        ChunkKind::Histogram => "histogram",
        ChunkKind::ExponentialHistogram => "exponential_histogram",
        ChunkKind::Summary => "summary",
    }
}

fn chunk_kind_from_inventory_id(kind: u8) -> ChunkKind {
    match kind {
        0 => ChunkKind::Float,
        1 => ChunkKind::Int64,
        2 => ChunkKind::Histogram,
        3 => ChunkKind::ExponentialHistogram,
        4 => ChunkKind::Summary,
        _ => unreachable!("inventory keys originate from ChunkKind"),
    }
}

fn chunk_encoding_id(encoding: ChunkEncoding) -> u8 {
    match encoding {
        ChunkEncoding::SchemaVarLen => 0,
        ChunkEncoding::RawF64 => 1,
        ChunkEncoding::RawI64 => 2,
        ChunkEncoding::Gorilla => 3,
        ChunkEncoding::IntDeltaZigZag => 4,
    }
}

fn chunk_encoding_name(encoding: ChunkEncoding) -> &'static str {
    match encoding {
        ChunkEncoding::SchemaVarLen => "schema_varlen",
        ChunkEncoding::RawF64 => "raw_f64",
        ChunkEncoding::RawI64 => "raw_i64",
        ChunkEncoding::Gorilla => "gorilla",
        ChunkEncoding::IntDeltaZigZag => "int_delta_zigzag",
    }
}

fn chunk_encoding_from_inventory_id(encoding: u8) -> ChunkEncoding {
    match encoding {
        0 => ChunkEncoding::SchemaVarLen,
        1 => ChunkEncoding::RawF64,
        2 => ChunkEncoding::RawI64,
        3 => ChunkEncoding::Gorilla,
        4 => ChunkEncoding::IntDeltaZigZag,
        _ => unreachable!("inventory keys originate from ChunkEncoding"),
    }
}

fn chunk_payload_layout_name(kind: ChunkKind, encoding: ChunkEncoding) -> &'static str {
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

fn temporality_id(temporality: OtlpAggregationTemporality) -> u8 {
    match temporality {
        OtlpAggregationTemporality::Unspecified => 0,
        OtlpAggregationTemporality::Delta => 1,
        OtlpAggregationTemporality::Cumulative => 2,
    }
}

fn reset_hint_id(reset_hint: CounterResetHint) -> u8 {
    match reset_hint {
        CounterResetHint::Unknown => 0,
        CounterResetHint::CounterReset => 1,
        CounterResetHint::NotCounterReset => 2,
        CounterResetHint::GaugeType => 3,
    }
}

fn chunk_sample_count(samples: &ChunkSamples) -> u64 {
    match samples {
        ChunkSamples::Float(values) => values.len() as u64,
        ChunkSamples::Int64(values) => values.len() as u64,
        ChunkSamples::Histogram(values) => values.len() as u64,
        ChunkSamples::ExponentialHistogram(values) => values.len() as u64,
        ChunkSamples::Summary(values) => values.len() as u64,
    }
}

fn hex_digest(digest: [u8; 32]) -> String {
    let mut value = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    value
}

#[cfg(test)]
mod tests;
