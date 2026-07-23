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
mod tests {
    use std::time::Duration;

    use crate::labels::SeriesRef;
    use crate::promql::METRIC_NAME_LABEL;
    use crate::storage::chunk::{CHUNK_FLAG_HAS_START_TIME, TYPED_SCALAR_LANE_HEADER_LEN};
    use crate::storage::head::SummaryQuantileValue;
    use crate::storage::metadata_cache::{MetadataCacheError, StructuralMetadataErrorKind};
    use crate::storage::segment::metadata_facade::SegmentMetadataFacadeError;
    use crate::storage::series::v3::Schema7MetadataReaderError;

    use super::*;

    fn reference_encode_u128_uleb(mut value: u128, out: &mut Vec<u8>) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn reference_zigzag_i128(value: i128) -> u128 {
        if value >= 0 {
            (value as u128) * 2
        } else {
            ((-value) as u128) * 2 - 1
        }
    }

    fn direct_decoded_semantic_fingerprint(chunks: &[ChunkSamples]) -> String {
        let mut accumulator = DecodedSemanticAccumulator::new(1, None);
        accumulator.start_segment("segment", 0, 10_000, 1).unwrap();
        accumulator
            .start_series(
                7,
                SERIES_KIND_HISTOGRAM | SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY,
                &[
                    (METRIC_NAME_LABEL.to_owned(), "typed".to_owned()),
                    ("service".to_owned(), "api".to_owned()),
                ],
            )
            .unwrap();
        let mut samples = 0u64;
        for chunk in chunks {
            samples += accumulator.observe_chunk(0, chunk).unwrap();
        }
        accumulator.finish_series(samples).unwrap();
        accumulator.finish()
    }

    fn assert_direct_semantic_mutation(
        baseline: &[ChunkSamples],
        field: &str,
        mutate: impl FnOnce(&mut [ChunkSamples]),
    ) {
        let baseline_fingerprint = direct_decoded_semantic_fingerprint(baseline);
        let mut mutated = baseline.to_vec();
        mutate(&mut mutated);
        assert_ne!(
            baseline_fingerprint,
            direct_decoded_semantic_fingerprint(&mutated),
            "typed field {field} was absent from the decoded semantic fingerprint"
        );
    }

    fn direct_histogram(chunks: &mut [ChunkSamples]) -> &mut HistogramValue {
        let ChunkSamples::Histogram(samples) = &mut chunks[0] else {
            panic!("direct semantic fixture lost its Histogram lane");
        };
        &mut samples[0].1
    }

    fn direct_exponential_histogram(chunks: &mut [ChunkSamples]) -> &mut ExponentialHistogramValue {
        let ChunkSamples::ExponentialHistogram(samples) = &mut chunks[1] else {
            panic!("direct semantic fixture lost its ExponentialHistogram lane");
        };
        &mut samples[0].1
    }

    fn direct_summary(chunks: &mut [ChunkSamples]) -> &mut SummaryValue {
        let ChunkSamples::Summary(samples) = &mut chunks[2] else {
            panic!("direct semantic fixture lost its Summary lane");
        };
        &mut samples[0].1
    }

    fn reference_timestamp_candidate_lengths(timestamps: &[u64]) -> [usize; 4] {
        assert!(!timestamps.is_empty());
        assert!(timestamps.windows(2).all(|pair| pair[0] <= pair[1]));
        let first = timestamps[0];

        let mut current = first.to_le_bytes().to_vec();
        for timestamp in timestamps {
            reference_encode_u128_uleb(u128::from(timestamp - first), &mut current);
        }

        let mut adjacent = first.to_le_bytes().to_vec();
        for pair in timestamps.windows(2) {
            reference_encode_u128_uleb(u128::from(pair[1] - pair[0]), &mut adjacent);
        }

        let mut delta_of_delta = first.to_le_bytes().to_vec();
        let mut deltas = timestamps.windows(2).map(|pair| pair[1] - pair[0]);
        if let Some(first_delta) = deltas.next() {
            reference_encode_u128_uleb(u128::from(first_delta), &mut delta_of_delta);
            let mut previous = first_delta;
            for delta in deltas {
                reference_encode_u128_uleb(
                    reference_zigzag_i128(i128::from(delta) - i128::from(previous)),
                    &mut delta_of_delta,
                );
                previous = delta;
            }
        }

        let mut fixed_step = first.to_le_bytes().to_vec();
        if timestamps.len() > 1 {
            let interval_count = timestamps.len() - 1;
            let step = (timestamps[timestamps.len() - 1] - first) / interval_count as u64;
            reference_encode_u128_uleb(u128::from(step), &mut fixed_step);
            let residuals = timestamps
                .iter()
                .enumerate()
                .skip(1)
                .map(|(index, timestamp)| {
                    let baseline = u128::from(first) + index as u128 * u128::from(step);
                    reference_zigzag_i128(i128::from(*timestamp) - baseline as i128)
                })
                .collect::<Vec<_>>();
            let bit_width = residuals
                .iter()
                .map(|value| 128 - value.leading_zeros())
                .max()
                .unwrap_or(0);
            fixed_step.push(bit_width as u8);
            let packed_start = fixed_step.len();
            let packed_bits = bit_width as usize * residuals.len();
            fixed_step.resize(packed_start + packed_bits.div_ceil(8), 0);
            let mut bit_offset = 0usize;
            for residual in residuals {
                for bit in 0..bit_width {
                    if (residual >> bit) & 1 != 0 {
                        fixed_step[packed_start + bit_offset / 8] |= 1 << (bit_offset % 8);
                    }
                    bit_offset += 1;
                }
            }
        }

        [
            current.len(),
            adjacent.len(),
            delta_of_delta.len(),
            fixed_step.len(),
        ]
    }

    #[test]
    fn schema6_and_schema7_verified_selection_matches_after_full_decode() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema6.path(), false);
        write_fixture(schema7.path(), true);

        let schema6_report = verify_experimental_storage_corpus(
            schema6.path(),
            SegmentStorageSchema::Schema6,
            true,
            None,
        )
        .unwrap();
        let schema7_report = verify_experimental_storage_corpus(
            schema7.path(),
            SegmentStorageSchema::Schema7,
            true,
            None,
        )
        .unwrap();

        assert_eq!(
            schema6_report.verified_selection_fingerprint,
            schema7_report.verified_selection_fingerprint
        );
        assert_eq!(
            schema6_report.decoded_semantic_fingerprint,
            schema7_report.decoded_semantic_fingerprint
        );
        assert_eq!(
            schema6_report.chunk_inventory,
            schema7_report.chunk_inventory
        );
        assert_eq!(schema6_report.segments, 1);
        assert_eq!(schema6_report.series, 2);
        assert_eq!(schema6_report.chunks, 2);
        assert_eq!(schema6_report.samples, 3);
        assert_eq!(
            schema6_report.logical_chunk_bytes,
            schema7_report.logical_chunk_bytes
        );

        let sampled = verify_experimental_storage_corpus(
            schema7.path(),
            SegmentStorageSchema::Schema7,
            false,
            Some(1),
        )
        .unwrap();
        assert_eq!(sampled.series, 1);
        assert_ne!(
            sampled.verified_selection_fingerprint, schema7_report.verified_selection_fingerprint,
            "sampled and exhaustive selections have distinct fingerprint streams"
        );
    }

    #[test]
    fn independently_written_schema7_and_schema8_corpora_match_after_full_decode() {
        let schema7 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_fixture(schema7.path(), true);
        write_schema8_fixture(schema8.path());

        let schema7_report = verify_experimental_storage_corpus_with_decoded_semantics(
            schema7.path(),
            SegmentStorageSchema::Schema7,
            true,
        )
        .unwrap();
        let schema8_report = verify_experimental_storage_corpus_with_decoded_semantics(
            schema8.path(),
            SegmentStorageSchema::Schema8,
            true,
        )
        .unwrap();

        assert_eq!(
            schema7_report.verified_selection_fingerprint,
            schema8_report.verified_selection_fingerprint
        );
        assert_eq!(
            schema7_report.decoded_semantic_fingerprint,
            schema8_report.decoded_semantic_fingerprint
        );
        assert_eq!(
            schema7_report.chunk_inventory,
            schema8_report.chunk_inventory
        );
        assert_eq!(schema7_report.segments, schema8_report.segments);
        assert_eq!(schema7_report.corpus_series, schema8_report.corpus_series);
        assert_eq!(schema7_report.series, schema8_report.series);
        assert_eq!(schema7_report.chunks, schema8_report.chunks);
        assert_eq!(schema7_report.chunks_by_kind, schema8_report.chunks_by_kind);
        assert_eq!(schema7_report.samples, schema8_report.samples);
        assert_eq!(
            schema7_report.logical_chunk_bytes,
            schema8_report.logical_chunk_bytes
        );
        assert_eq!(
            schema7_report.topology_independent_decoded_semantic_fingerprint,
            schema8_report.topology_independent_decoded_semantic_fingerprint
        );
        assert!(
            schema7_report
                .topology_independent_decoded_semantic_fingerprint
                .is_some()
        );
        let schema7_postings = schema7_report.exact_postings.unwrap();
        let schema8_postings = schema8_report.exact_postings.unwrap();
        assert_eq!(
            schema7_postings.logical_fingerprint,
            schema8_postings.logical_fingerprint
        );
        assert_eq!(schema7_postings.lists, schema8_postings.lists);
        assert_eq!(schema7_postings.decoded_refs, schema8_postings.decoded_refs);
        assert!(schema8_postings.encoded_bytes < schema7_postings.encoded_bytes);
    }

    #[test]
    fn topology_independent_fingerprint_ignores_record_order_and_detects_value_changes() {
        let labels = vec![
            (METRIC_NAME_LABEL.to_owned(), "semantic".to_owned()),
            ("instance".to_owned(), "a".to_owned()),
        ];
        let series_digest =
            TopologyIndependentDecodedSemanticAccumulator::series_digest(&labels).unwrap();

        let fingerprint = |samples: &[(u64, f64)]| {
            let mut accumulator = TopologyIndependentDecodedSemanticAccumulator::new();
            let mut scratch = Vec::new();
            accumulator
                .observe_samples(
                    &series_digest,
                    &ChunkSamples::Float(samples.to_vec()),
                    &mut scratch,
                )
                .unwrap();
            accumulator.finish()
        };

        let forward = fingerprint(&[(1_000, 1.0), (2_000, 2.0), (2_000, 3.0)]);
        let duplicate_winner_reordered = fingerprint(&[(2_000, 3.0), (1_000, 1.0), (2_000, 2.0)]);
        let changed = fingerprint(&[(1_000, 1.0), (2_000, 2.0), (2_000, 4.0)]);
        assert_eq!(
            forward, duplicate_winner_reordered,
            "the topology-independent multiset does not claim duplicate-winner order"
        );
        assert_ne!(forward, changed);
    }

    #[test]
    fn topology_independent_fingerprint_covers_typed_values_and_metadata() {
        let labels = vec![(METRIC_NAME_LABEL.to_owned(), "typed".to_owned())];
        let series_digest =
            TopologyIndependentDecodedSemanticAccumulator::series_digest(&labels).unwrap();
        let histogram = HistogramValue {
            count: 2,
            sum: Some(3.0),
            min: Some(1.0),
            max: Some(2.0),
            metadata: TypedSampleMetadata {
                flags: 7,
                ..TypedSampleMetadata::default()
            },
            explicit_bounds: vec![1.5],
            bucket_counts: vec![1, 1],
        };
        let exponential = ExponentialHistogramValue {
            count: 1,
            sum: Some(2.0),
            min: Some(2.0),
            max: Some(2.0),
            scale: 1,
            zero_threshold: 0.0,
            zero_count: 0,
            metadata: TypedSampleMetadata::default(),
            positive: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![1],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![],
            },
        };
        let summary = SummaryValue {
            count: 1,
            sum: 4.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![SummaryQuantileValue {
                quantile: 0.5,
                value: 4.0,
            }],
        };

        let fingerprint = |histogram: HistogramValue| {
            let mut accumulator = TopologyIndependentDecodedSemanticAccumulator::new();
            let mut scratch = Vec::new();
            for samples in [
                ChunkSamples::Int64(vec![(1_000, -1)]),
                ChunkSamples::Histogram(vec![(2_000, histogram)]),
                ChunkSamples::ExponentialHistogram(vec![(3_000, exponential.clone())]),
                ChunkSamples::Summary(vec![(4_000, summary.clone())]),
            ] {
                accumulator
                    .observe_samples(&series_digest, &samples, &mut scratch)
                    .unwrap();
            }
            assert_eq!(accumulator.samples, 4);
            accumulator.finish()
        };

        let original = fingerprint(histogram.clone());
        let mut changed = histogram;
        changed.metadata.flags ^= 1;
        assert_ne!(original, fingerprint(changed));
    }

    #[test]
    fn decoded_semantic_fingerprint_and_float_candidates_ignore_float_codec() {
        let gorilla = tempfile::tempdir().unwrap();
        let raw = tempfile::tempdir().unwrap();
        write_float_codec_fixture(gorilla.path(), false);
        write_float_codec_fixture(raw.path(), true);

        let gorilla_report = verify_experimental_storage_corpus(
            gorilla.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();
        let raw_report = verify_experimental_storage_corpus(
            raw.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();

        assert_ne!(
            gorilla_report.verified_selection_fingerprint,
            raw_report.verified_selection_fingerprint,
            "the existing physical identity must retain codec and exact-byte sensitivity"
        );
        assert_eq!(
            gorilla_report.decoded_semantic_fingerprint,
            raw_report.decoded_semantic_fingerprint
        );
        let candidates = &gorilla_report.chunk_inventory.raw_f64_vs_gorilla;
        let raw_candidates = &raw_report.chunk_inventory.raw_f64_vs_gorilla;
        assert_eq!(candidates.chunks, raw_candidates.chunks);
        assert_eq!(candidates.points, raw_candidates.points);
        assert_eq!(
            candidates.raw_f64_candidate_indexed_bytes,
            raw_candidates.raw_f64_candidate_indexed_bytes
        );
        assert_eq!(
            candidates.raw_f64_candidate_payload_bytes,
            raw_candidates.raw_f64_candidate_payload_bytes
        );
        assert_eq!(
            candidates.gorilla_candidate_indexed_bytes,
            raw_candidates.gorilla_candidate_indexed_bytes
        );
        assert_eq!(
            candidates.gorilla_candidate_payload_bytes,
            raw_candidates.gorilla_candidate_payload_bytes
        );
        assert_eq!(candidates.raw_f64_wins, raw_candidates.raw_f64_wins);
        assert_eq!(candidates.gorilla_wins, raw_candidates.gorilla_wins);
        assert_eq!(candidates.ties, raw_candidates.ties);
        assert_ne!(
            candidates.existing_indexed_bytes,
            raw_candidates.existing_indexed_bytes
        );
        assert_eq!(candidates.chunks, 1);
        assert_eq!(candidates.points, 4);
        assert_eq!(candidates.raw_f64_candidate_payload_bytes, 47);
        assert_eq!(candidates.raw_f64_candidate_indexed_bytes, 87);
        assert_eq!(candidates.gorilla_candidate_payload_bytes, 29);
        assert_eq!(candidates.gorilla_candidate_indexed_bytes, 69);
        assert_eq!(
            candidates.gorilla_wins,
            ExperimentalCodecWinnerTotals {
                chunks: 1,
                points: 4,
            }
        );
        assert_eq!(
            candidates.raw_f64_wins,
            ExperimentalCodecWinnerTotals::default()
        );
        assert_eq!(candidates.ties, ExperimentalCodecWinnerTotals::default());
        assert!(candidates.tie_rule.contains("RAW_F64 wins"));
        assert_eq!(
            candidates.adaptive_gorilla_selections,
            ExperimentalCodecWinnerTotals {
                chunks: 1,
                points: 4,
            }
        );
        assert_eq!(
            candidates.adaptive_raw_f64_selections,
            ExperimentalCodecWinnerTotals::default()
        );
        assert_eq!(candidates.finite_nonzero_points, 4);
        assert_eq!(candidates.repeated_xor_points, 1);
        assert_eq!(
            candidates.repeated_xor_points
                + candidates.reused_window_points
                + candidates.new_window_points,
            candidates.points - candidates.chunks
        );

        let gorilla_inventory = &gorilla_report.chunk_inventory.by_kind_encoding;
        assert_eq!(gorilla_inventory.len(), 1);
        assert_eq!(gorilla_inventory[0].kind, "float");
        assert_eq!(gorilla_inventory[0].encoding, "gorilla");
        assert_eq!(gorilla_inventory[0].chunks, 1);
        assert_eq!(gorilla_inventory[0].points, 4);
        assert_eq!(gorilla_inventory[0].indexed_bytes, 69);
        assert_eq!(gorilla_inventory[0].common_header_bytes, 40);
        assert_eq!(gorilla_inventory[0].scalar_lane_bytes, 0);
        assert_eq!(gorilla_inventory[0].payload_bytes, 29);
        assert_eq!(gorilla_inventory[0].timestamp_base_bytes, 8);
        assert_eq!(gorilla_inventory[0].timestamp_delta_bytes, 7);
        assert_eq!(gorilla_inventory[0].value_bytes, 14);
        assert_eq!(
            gorilla_inventory[0].point_count_histogram.buckets,
            vec![ExperimentalHistogramBucket {
                lower_inclusive: 4,
                upper_inclusive: 7,
                count: 1,
            }]
        );
        assert_eq!(
            gorilla_inventory[0].cadence_ms_histogram.buckets,
            vec![ExperimentalHistogramBucket {
                lower_inclusive: 512,
                upper_inclusive: 1_023,
                count: 3,
            }]
        );

        let timestamp = &gorilla_report.chunk_inventory.timestamp_candidates;
        assert_eq!(timestamp.tie_rule, TIMESTAMP_CODEC_TIE_RULE);
        assert!(!timestamp.selector_bytes_included);
        assert_eq!(timestamp.all_blocks.chunks, 1);
        assert_eq!(timestamp.all_blocks.points, 4);
        assert_eq!(timestamp.all_blocks.current_offset_uleb.bytes, 15);
        assert_eq!(timestamp.all_blocks.adjacent_delta_uleb.bytes, 14);
        assert_eq!(timestamp.all_blocks.delta_of_delta_zigzag_uleb128.bytes, 12);
        assert_eq!(timestamp.all_blocks.fixed_step_residual_bitpack.bytes, 11);
        assert_eq!(timestamp.all_blocks.adaptive_min_bytes, 11);
        assert_eq!(
            timestamp.all_blocks.fixed_step_residual_bitpack.unique_wins,
            ExperimentalCodecWinnerTotals {
                chunks: 1,
                points: 4,
            }
        );
        assert_eq!(timestamp.by_shape.len(), 1);
        assert_eq!(timestamp.by_shape[0].shape, "constant_positive_step");
        assert_eq!(timestamp.by_kind_encoding.len(), 1);
        assert_eq!(timestamp.by_kind_encoding[0].kind, "float");
        assert_eq!(timestamp.by_kind_encoding[0].encoding, "gorilla");
        assert_eq!(timestamp.by_kind_encoding[0].evidence, timestamp.all_blocks);
    }

    #[test]
    fn single_point_float_payload_tie_selects_raw_f64_deterministically() {
        let layout = DecodedChunkLayout {
            kind: ChunkKind::Float,
            encoding: ChunkEncoding::RawF64,
            flags: 0,
            num_points: 1,
            common_header_bytes: 40,
            scalar_lane_bytes: 0,
            payload_bytes: 17,
            timestamp_base_bytes: 8,
            timestamp_delta_bytes: 1,
            value_bytes: 8,
        };
        let mut accumulator = FloatCodecCandidatesAccumulator::default();
        observe_float_codec_candidates(&mut accumulator, &layout, 57, &[(1_000, -0.0)]).unwrap();
        let evidence = accumulator.finish();

        assert_eq!(evidence.raw_f64_candidate_payload_bytes, 17);
        assert_eq!(evidence.gorilla_candidate_payload_bytes, 17);
        assert_eq!(
            evidence.ties,
            ExperimentalCodecWinnerTotals {
                chunks: 1,
                points: 1,
            }
        );
        assert_eq!(
            evidence.adaptive_raw_f64_selections,
            ExperimentalCodecWinnerTotals {
                chunks: 1,
                points: 1,
            }
        );
        assert_eq!(
            evidence.adaptive_gorilla_selections,
            ExperimentalCodecWinnerTotals::default()
        );
        assert!(evidence.tie_rule.starts_with("RAW_F64 wins"));
    }

    #[test]
    fn float_value_distribution_classifies_ieee_values_and_reconciles_xor_paths() {
        let samples = [
            (0, 0.0),
            (1, -0.0),
            (2, f64::INFINITY),
            (3, f64::NEG_INFINITY),
            (4, f64::from_bits(0x7ff8_0000_0000_0042)),
            (5, f64::from_bits(PROMETHEUS_STALE_NAN_BITS)),
            (6, 1.0),
            (7, 1.0),
        ];
        let mut accumulator = FloatCodecCandidatesAccumulator::default();
        observe_float_value_distribution(&mut accumulator, &samples).unwrap();
        accumulator.evidence.chunks = 1;
        accumulator.evidence.points = samples.len() as u64;
        let evidence = accumulator.finish();

        assert_eq!(evidence.positive_zero_points, 1);
        assert_eq!(evidence.negative_zero_points, 1);
        assert_eq!(evidence.positive_infinity_points, 1);
        assert_eq!(evidence.negative_infinity_points, 1);
        assert_eq!(evidence.ordinary_nan_points, 1);
        assert_eq!(evidence.stale_nan_points, 1);
        assert_eq!(evidence.finite_nonzero_points, 2);
        assert_eq!(
            evidence.positive_zero_points
                + evidence.negative_zero_points
                + evidence.positive_infinity_points
                + evidence.negative_infinity_points
                + evidence.ordinary_nan_points
                + evidence.stale_nan_points
                + evidence.finite_nonzero_points,
            evidence.points
        );
        assert_eq!(
            evidence.repeated_xor_points
                + evidence.reused_window_points
                + evidence.new_window_points,
            evidence.points - evidence.chunks
        );
        let xor_width_observations = evidence.xor_significant_bits_histogram.zero_count
            + evidence
                .xor_significant_bits_histogram
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>();
        assert_eq!(
            xor_width_observations,
            evidence.reused_window_points + evidence.new_window_points
        );
    }

    #[test]
    fn timestamp_candidate_estimators_match_reference_encoders_and_goldens() {
        for timestamps in [
            vec![1_000],
            vec![1_000, 2_000, 3_000, 4_000],
            vec![10, 10, 10],
            vec![100, 101, 110, 111, 1_000],
            vec![0, u64::MAX, u64::MAX],
        ] {
            let estimated = timestamp_candidate_sizes(timestamps.iter().copied()).unwrap();
            let reference = reference_timestamp_candidate_lengths(&timestamps);
            assert_eq!(
                estimated.bytes,
                reference.map(|bytes| bytes as u64),
                "candidate estimator diverged for {timestamps:?}"
            );
        }

        assert_eq!(
            timestamp_candidate_sizes([1_000, 2_000, 3_000, 4_000].into_iter())
                .unwrap()
                .bytes,
            [15, 14, 12, 11]
        );
        assert_eq!(
            timestamp_candidate_sizes([10, 10, 10].into_iter())
                .unwrap()
                .bytes,
            [11, 10, 10, 10]
        );
    }

    #[test]
    fn timestamp_candidates_reject_reversed_order_and_map_signed_extremes() {
        let error = timestamp_candidate_sizes([10, 9].into_iter()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not ordered"));

        assert_eq!(zigzag_i128(i128::MIN), u128::MAX);
        assert_eq!(zigzag_i128(i128::MAX), u128::MAX - 1);
        assert_eq!(uleb128_u128_len(u128::MAX), 19);
    }

    #[test]
    fn timestamp_candidate_ties_use_the_declared_stable_priority() {
        let mut accumulator = TimestampCodecCandidatesAccumulator::default();
        accumulator.observe((0, 0), [42].into_iter(), 9).unwrap();
        accumulator
            .observe((0, 0), [10, 10, 10].into_iter(), 11)
            .unwrap();
        let report = accumulator.finish();

        assert_eq!(report.tie_rule, TIMESTAMP_CODEC_TIE_RULE);
        assert_eq!(
            report.all_blocks.tied_minima,
            ExperimentalCodecWinnerTotals {
                chunks: 2,
                points: 4,
            }
        );
        assert_eq!(
            report.all_blocks.current_offset_uleb.adaptive_selections,
            ExperimentalCodecWinnerTotals::default()
        );
        assert_eq!(
            report.all_blocks.adjacent_delta_uleb.adaptive_selections,
            ExperimentalCodecWinnerTotals {
                chunks: 2,
                points: 4,
            }
        );
    }

    #[test]
    fn decoded_semantic_fingerprint_preserves_lane_and_duplicate_order_not_chunk_boundaries() {
        fn fingerprint(chunks: &[(u8, &[(u64, f64)])]) -> String {
            let mut accumulator = DecodedSemanticAccumulator::new(1, None);
            accumulator
                .start_segment("semantic-segment", 0, 100, 1)
                .unwrap();
            accumulator
                .start_series(
                    7,
                    1u8 << chunk_kind_id(ChunkKind::Float),
                    &[("__name__".to_owned(), "semantic_metric".to_owned())],
                )
                .unwrap();
            let mut samples = 0;
            for (file_id, values) in chunks {
                samples += accumulator
                    .observe_chunk(*file_id, &ChunkSamples::Float(values.to_vec()))
                    .unwrap();
            }
            accumulator.finish_series(samples).unwrap();
            accumulator.finish()
        }

        let ordered = [(10, 1.0), (10, 2.0), (20, 3.0)];
        let first_chunk = [(10, 1.0)];
        let second_chunk = [(10, 2.0), (20, 3.0)];
        let duplicate_reordered = [(10, 2.0), (10, 1.0), (20, 3.0)];

        let one_chunk = fingerprint(&[(0, &ordered)]);
        assert_eq!(
            one_chunk,
            fingerprint(&[(0, &first_chunk), (0, &second_chunk)]),
            "physical rechunking must not change decoded semantics"
        );
        assert_ne!(
            one_chunk,
            fingerprint(&[(0, &duplicate_reordered)]),
            "same-timestamp source order is semantic evidence"
        );
        assert_ne!(
            one_chunk,
            fingerprint(&[(1, &ordered)]),
            "in-order and out-of-order lanes are distinct sources"
        );
    }

    #[test]
    fn decoded_semantic_fingerprint_is_invariant_to_mixed_kind_chunk_interleaving() {
        fn fingerprint(chunks: &[ChunkSamples]) -> String {
            let mut accumulator = DecodedSemanticAccumulator::new(1, None);
            accumulator
                .start_segment("mixed-kind-segment", 0, 100, 1)
                .unwrap();
            accumulator
                .start_series(
                    7,
                    (1u8 << chunk_kind_id(ChunkKind::Float))
                        | (1u8 << chunk_kind_id(ChunkKind::Int64)),
                    &[("__name__".to_owned(), "mixed_kind_metric".to_owned())],
                )
                .unwrap();
            let mut samples = 0u64;
            for chunk in chunks {
                samples += accumulator.observe_chunk(0, chunk).unwrap();
            }
            accumulator.finish_series(samples).unwrap();
            accumulator.finish()
        }

        let float_first = ChunkSamples::Float(vec![(10, 1.0)]);
        let float_second = ChunkSamples::Float(vec![(30, -0.0)]);
        let int = ChunkSamples::Int64(vec![(20, -7)]);
        let interleaved = fingerprint(&[float_first.clone(), int.clone(), float_second.clone()]);
        let grouped = fingerprint(&[float_first.clone(), float_second.clone(), int.clone()]);
        assert_eq!(interleaved, grouped);

        assert_ne!(
            grouped,
            fingerprint(&[
                float_first,
                float_second,
                ChunkSamples::Int64(vec![(20, -8)]),
            ])
        );
    }

    #[test]
    fn persisted_independent_rechunked_corpora_share_decoded_semantic_fingerprint() {
        let one_chunk = tempfile::tempdir().unwrap();
        let rechunked = tempfile::tempdir().unwrap();
        write_rechunked_semantic_fixture(one_chunk.path(), false);
        write_rechunked_semantic_fixture(rechunked.path(), true);

        let one_chunk = verify_experimental_storage_corpus(
            one_chunk.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();
        let rechunked = verify_experimental_storage_corpus(
            rechunked.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();

        assert_ne!(
            one_chunk.verified_selection_fingerprint,
            rechunked.verified_selection_fingerprint
        );
        assert_eq!(
            one_chunk.decoded_semantic_fingerprint,
            rechunked.decoded_semantic_fingerprint
        );
        assert_eq!(one_chunk.samples, rechunked.samples);
        assert_ne!(one_chunk.chunks, rechunked.chunks);
    }

    #[test]
    fn persisted_mixed_kind_interleaving_shares_decoded_semantic_fingerprint() {
        let interleaved = tempfile::tempdir().unwrap();
        let grouped = tempfile::tempdir().unwrap();
        write_mixed_kind_interleaving_fixture(interleaved.path(), true);
        write_mixed_kind_interleaving_fixture(grouped.path(), false);

        let interleaved = verify_experimental_storage_corpus(
            interleaved.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();
        let grouped = verify_experimental_storage_corpus(
            grouped.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();

        assert_eq!(interleaved.series, 1);
        assert_eq!(interleaved.chunks_by_kind, [2, 1, 0, 0, 0]);
        assert_eq!(interleaved.samples, grouped.samples);
        assert_eq!(
            interleaved.decoded_semantic_fingerprint,
            grouped.decoded_semantic_fingerprint
        );
    }

    #[test]
    fn persisted_representative_typed_fields_are_semantic_fingerprint_sensitive() {
        let baseline = tempfile::tempdir().unwrap();
        write_typed_sensitivity_fixture(baseline.path(), TypedSensitivityMutation::None);
        let baseline = verify_experimental_storage_corpus(
            baseline.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();

        for mutation in [
            TypedSensitivityMutation::HistogramMetadataFlags,
            TypedSensitivityMutation::HistogramSumBits,
            TypedSensitivityMutation::ExponentialHistogramScale,
            TypedSensitivityMutation::SummaryQuantileValue,
        ] {
            let corpus = tempfile::tempdir().unwrap();
            write_typed_sensitivity_fixture(corpus.path(), mutation);
            let report = verify_experimental_storage_corpus(
                corpus.path(),
                SegmentStorageSchema::Schema8,
                true,
                None,
            )
            .unwrap();
            assert_ne!(
                baseline.decoded_semantic_fingerprint, report.decoded_semantic_fingerprint,
                "mutation {mutation:?} was absent from the semantic fingerprint"
            );
        }
    }

    #[test]
    fn every_direct_typed_field_is_semantic_fingerprint_sensitive() {
        let metadata = TypedSampleMetadata {
            start_time_ms: Some(900),
            flags: 3,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::NotCounterReset,
        };
        let baseline = vec![
            ChunkSamples::Histogram(vec![(
                1_000,
                HistogramValue {
                    count: 3,
                    sum: Some(-0.0),
                    min: Some(-1.0),
                    max: Some(2.0),
                    metadata,
                    explicit_bounds: vec![-1.0, 1.0],
                    bucket_counts: vec![1, 1, 1],
                },
            )]),
            ChunkSamples::ExponentialHistogram(vec![(
                2_000,
                ExponentialHistogramValue {
                    count: 6,
                    sum: Some(3.0),
                    min: Some(-2.0),
                    max: Some(4.0),
                    scale: 1,
                    zero_threshold: -0.0,
                    zero_count: 1,
                    metadata,
                    positive: ExponentialHistogramBuckets {
                        offset: -2,
                        counts: vec![1, 2],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 4,
                        counts: vec![2],
                    },
                },
            )]),
            ChunkSamples::Summary(vec![(
                3_000,
                SummaryValue {
                    count: 2,
                    sum: 42.0,
                    metadata,
                    quantiles: vec![
                        SummaryQuantileValue {
                            quantile: 0.5,
                            value: -0.0,
                        },
                        SummaryQuantileValue {
                            quantile: 0.9,
                            value: 42.0,
                        },
                    ],
                },
            )]),
        ];

        assert_direct_semantic_mutation(&baseline, "sample timestamp", |chunks| {
            let ChunkSamples::Histogram(samples) = &mut chunks[0] else {
                unreachable!();
            };
            samples[0].0 += 1;
        });
        assert_direct_semantic_mutation(&baseline, "metadata start-time presence", |chunks| {
            direct_histogram(chunks).metadata.start_time_ms = None;
        });
        assert_direct_semantic_mutation(&baseline, "metadata start time", |chunks| {
            direct_histogram(chunks).metadata.start_time_ms = Some(899);
        });
        assert_direct_semantic_mutation(&baseline, "metadata flags", |chunks| {
            direct_histogram(chunks).metadata.flags ^= 1;
        });
        assert_direct_semantic_mutation(&baseline, "metadata temporality", |chunks| {
            direct_histogram(chunks).metadata.temporality = OtlpAggregationTemporality::Delta;
        });
        assert_direct_semantic_mutation(&baseline, "metadata reset hint", |chunks| {
            direct_histogram(chunks).metadata.reset_hint = CounterResetHint::CounterReset;
        });

        assert_direct_semantic_mutation(&baseline, "Histogram count", |chunks| {
            direct_histogram(chunks).count += 1;
        });
        assert_direct_semantic_mutation(&baseline, "Histogram sum presence", |chunks| {
            direct_histogram(chunks).sum = None;
        });
        assert_direct_semantic_mutation(&baseline, "Histogram sum bits", |chunks| {
            direct_histogram(chunks).sum = Some(0.0);
        });
        assert_direct_semantic_mutation(&baseline, "Histogram min presence", |chunks| {
            direct_histogram(chunks).min = None;
        });
        assert_direct_semantic_mutation(&baseline, "Histogram min bits", |chunks| {
            direct_histogram(chunks).min = Some(-2.0);
        });
        assert_direct_semantic_mutation(&baseline, "Histogram max presence", |chunks| {
            direct_histogram(chunks).max = None;
        });
        assert_direct_semantic_mutation(&baseline, "Histogram max bits", |chunks| {
            direct_histogram(chunks).max = Some(3.0);
        });
        assert_direct_semantic_mutation(&baseline, "Histogram bound count", |chunks| {
            direct_histogram(chunks).explicit_bounds.pop();
        });
        assert_direct_semantic_mutation(&baseline, "Histogram bound bits", |chunks| {
            direct_histogram(chunks).explicit_bounds[0] = -2.0;
        });
        assert_direct_semantic_mutation(&baseline, "Histogram bound order", |chunks| {
            direct_histogram(chunks).explicit_bounds.swap(0, 1);
        });
        assert_direct_semantic_mutation(&baseline, "Histogram bucket count", |chunks| {
            direct_histogram(chunks).bucket_counts.pop();
        });
        assert_direct_semantic_mutation(&baseline, "Histogram bucket value", |chunks| {
            direct_histogram(chunks).bucket_counts[0] += 1;
        });
        assert_direct_semantic_mutation(&baseline, "Histogram bucket order", |chunks| {
            let buckets = &mut direct_histogram(chunks).bucket_counts;
            buckets[0] = 2;
            buckets[1] = 1;
            buckets.swap(0, 1);
        });

        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram count", |chunks| {
            direct_exponential_histogram(chunks).count += 1;
        });
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram sum presence", |chunks| {
            direct_exponential_histogram(chunks).sum = None
        });
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram sum bits", |chunks| {
            direct_exponential_histogram(chunks).sum = Some(4.0);
        });
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram min presence", |chunks| {
            direct_exponential_histogram(chunks).min = None
        });
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram min bits", |chunks| {
            direct_exponential_histogram(chunks).min = Some(-3.0);
        });
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram max presence", |chunks| {
            direct_exponential_histogram(chunks).max = None
        });
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram max bits", |chunks| {
            direct_exponential_histogram(chunks).max = Some(5.0);
        });
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram scale", |chunks| {
            direct_exponential_histogram(chunks).scale += 1;
        });
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram zero-threshold bits",
            |chunks| direct_exponential_histogram(chunks).zero_threshold = 0.0,
        );
        assert_direct_semantic_mutation(&baseline, "ExponentialHistogram zero count", |chunks| {
            direct_exponential_histogram(chunks).zero_count += 1
        });
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram positive offset",
            |chunks| direct_exponential_histogram(chunks).positive.offset += 1,
        );
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram positive bucket count",
            |chunks| {
                direct_exponential_histogram(chunks).positive.counts.pop();
            },
        );
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram positive bucket value",
            |chunks| direct_exponential_histogram(chunks).positive.counts[0] += 1,
        );
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram positive bucket order",
            |chunks| {
                direct_exponential_histogram(chunks)
                    .positive
                    .counts
                    .swap(0, 1)
            },
        );
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram negative offset",
            |chunks| direct_exponential_histogram(chunks).negative.offset -= 1,
        );
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram negative bucket count",
            |chunks| direct_exponential_histogram(chunks).negative.counts.push(0),
        );
        assert_direct_semantic_mutation(
            &baseline,
            "ExponentialHistogram negative bucket value",
            |chunks| direct_exponential_histogram(chunks).negative.counts[0] += 1,
        );

        assert_direct_semantic_mutation(&baseline, "Summary count", |chunks| {
            direct_summary(chunks).count += 1;
        });
        assert_direct_semantic_mutation(&baseline, "Summary sum bits", |chunks| {
            direct_summary(chunks).sum = 43.0;
        });
        assert_direct_semantic_mutation(&baseline, "Summary quantile count", |chunks| {
            direct_summary(chunks).quantiles.pop();
        });
        assert_direct_semantic_mutation(&baseline, "Summary quantile position bits", |chunks| {
            direct_summary(chunks).quantiles[0].quantile = 0.4;
        });
        assert_direct_semantic_mutation(&baseline, "Summary quantile value bits", |chunks| {
            direct_summary(chunks).quantiles[0].value = 0.0;
        });
        assert_direct_semantic_mutation(&baseline, "Summary quantile order", |chunks| {
            direct_summary(chunks).quantiles.swap(0, 1);
        });
    }

    #[test]
    fn chunk_inventory_rejects_decoded_range_mismatch_before_codec_evidence() {
        let layout = DecodedChunkLayout {
            kind: ChunkKind::Float,
            encoding: ChunkEncoding::RawF64,
            flags: 0,
            num_points: 1,
            common_header_bytes: 40,
            scalar_lane_bytes: 0,
            payload_bytes: 17,
            timestamp_base_bytes: 8,
            timestamp_delta_bytes: 1,
            value_bytes: 8,
        };
        let mut inventory = ChunkInventoryAccumulator::default();
        let error = inventory
            .observe(&layout, 57, 9, 10, &ChunkSamples::Float(vec![(10, 1.0)]))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "decoded timestamp range disagrees with the chunk header"
        );
        assert_eq!(inventory.float_candidates.evidence.chunks, 0);
    }

    #[test]
    fn verifier_rejects_crc_valid_u32_max_chunk_count_as_byte_infeasible() {
        let corpus = tempfile::tempdir().unwrap();
        write_fixture(corpus.path(), false);
        let manifest = read_manifest_inventory(corpus.path().join("manifest"))
            .unwrap()
            .unwrap();
        let chunks_path = corpus
            .path()
            .join(&manifest.segments[0].segment_id)
            .join(SegmentFile::Chunks.filename());
        let mut chunks = fs::read(&chunks_path).unwrap();
        let chunk_start = CHUNK_FRAME_HEADER_LEN;
        chunks[chunk_start + 24..chunk_start + 28].copy_from_slice(&u32::MAX.to_le_bytes());
        let frame_len = u32::from_le_bytes(chunks[0..4].try_into().unwrap()) as usize;
        let frame_crc = crc32c::crc32c(&chunks[CHUNK_FRAME_HEADER_LEN..frame_len]);
        chunks[4..8].copy_from_slice(&frame_crc.to_le_bytes());
        fs::write(chunks_path, chunks).unwrap();

        let error = verify_experimental_storage_corpus(
            corpus.path(),
            SegmentStorageSchema::Schema6,
            false,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "chunk point count is infeasible for its encoded payload bytes"
        );
    }

    #[test]
    fn verifier_rejects_resealed_infeasible_nested_histogram_schema_count() {
        let corpus = tempfile::tempdir().unwrap();
        write_schema6_typed_lane_fixture(corpus.path());
        let chunks_path = first_segment_file(corpus.path(), SegmentFile::Chunks);
        let mut chunks = fs::read(&chunks_path).unwrap();
        let chunk_start = CHUNK_FRAME_HEADER_LEN;
        let header_len = u32::from_le_bytes(
            chunks[chunk_start + 28..chunk_start + 32]
                .try_into()
                .unwrap(),
        ) as usize;
        let payload_len = u32::from_le_bytes(
            chunks[chunk_start + 32..chunk_start + 36]
                .try_into()
                .unwrap(),
        ) as usize;
        let payload_start = chunk_start + header_len;
        let payload_end = payload_start + payload_len;
        let payload = &chunks[payload_start..payload_end];
        let point_count = u32::from_le_bytes(
            chunks[chunk_start + 24..chunk_start + 28]
                .try_into()
                .unwrap(),
        );
        assert_eq!(point_count, 1);

        let mut cursor = 8usize;
        for _ in 0..point_count {
            crate::storage::encoding::decode_varint(payload, &mut cursor).unwrap();
        }
        assert_eq!(
            crate::storage::encoding::decode_varint(payload, &mut cursor).unwrap(),
            1
        );
        let schema_len =
            crate::storage::encoding::decode_varint(payload, &mut cursor).unwrap() as usize;
        assert!(schema_len >= 5);
        let schema_start = payload_start + cursor;
        chunks[schema_start..schema_start + 5].copy_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);

        let payload_crc = crc32c::crc32c(&chunks[payload_start..payload_end]);
        chunks[chunk_start + 36..chunk_start + 40].copy_from_slice(&payload_crc.to_le_bytes());
        reseal_first_frame_crc(&mut chunks);
        fs::write(chunks_path, chunks).unwrap();

        let error = verify_experimental_storage_corpus(
            corpus.path(),
            SegmentStorageSchema::Schema6,
            false,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("histogram schema bounds count is infeasible")
        );
    }

    #[test]
    fn verifier_rejects_self_consistent_scalar_lane_native_disagreement() {
        let corpus = tempfile::tempdir().unwrap();
        write_schema6_typed_lane_fixture(corpus.path());
        let chunks_path = first_segment_file(corpus.path(), SegmentFile::Chunks);
        let mut chunks = fs::read(&chunks_path).unwrap();
        let lane_start = CHUNK_FRAME_HEADER_LEN + CHUNK_FILE_HEADER_LEN;
        let body_start = lane_start + TYPED_SCALAR_LANE_HEADER_LEN;
        let count_offset = body_start + 8 + 1 + 4;
        assert_eq!(chunks[count_offset], 4);
        chunks[count_offset] = 5;
        let body_len =
            u32::from_le_bytes(chunks[lane_start + 8..lane_start + 12].try_into().unwrap())
                as usize;
        let body_crc = crc32c::crc32c(&chunks[body_start..body_start + body_len]);
        chunks[lane_start + 12..lane_start + 16].copy_from_slice(&body_crc.to_le_bytes());
        reseal_first_frame_crc(&mut chunks);
        fs::write(chunks_path, chunks).unwrap();

        let error = verify_experimental_storage_corpus(
            corpus.path(),
            SegmentStorageSchema::Schema6,
            false,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "typed scalar lane row disagrees with the native payload"
        );
    }

    #[test]
    fn verifier_recomputes_typed_header_flags_from_native_metadata() {
        let corpus = tempfile::tempdir().unwrap();
        write_schema6_typed_lane_fixture(corpus.path());
        let chunks_path = first_segment_file(corpus.path(), SegmentFile::Chunks);
        let mut chunks = fs::read(&chunks_path).unwrap();
        let flags_offset = CHUNK_FRAME_HEADER_LEN + 2;
        chunks[flags_offset..flags_offset + 2]
            .copy_from_slice(&CHUNK_FLAG_HAS_START_TIME.to_le_bytes());
        reseal_first_frame_crc(&mut chunks);
        fs::write(chunks_path, chunks).unwrap();

        let error = verify_experimental_storage_corpus(
            corpus.path(),
            SegmentStorageSchema::Schema6,
            false,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "typed chunk header flags disagree with native metadata"
        );
    }

    #[test]
    fn decoded_semantic_fingerprint_has_a_golden_all_kind_stream() {
        let corpus = tempfile::tempdir().unwrap();
        write_all_kinds_fixture(corpus.path());
        let report = verify_experimental_storage_corpus(
            corpus.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();

        assert_eq!(report.series, 5);
        assert_eq!(report.chunks, 5);
        assert_eq!(report.chunks_by_kind, [1, 1, 1, 1, 1]);
        assert_eq!(report.samples, 6);
        assert_eq!(report.chunk_inventory.by_kind_encoding.len(), 5);
        assert_eq!(
            report.decoded_semantic_fingerprint,
            "adeb821f7586347129e55a712bf54bf43330ff8e139ed05fe27b1aa90d28aeb4"
        );
    }

    #[test]
    fn schema6_and_schema7_promql_query_facades_match() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema6.path(), false);
        write_fixture(schema7.path(), true);

        let schema6_store = SegmentStoreReader::open_with_options(
            schema6.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let query = "replay_float";
        let mut schema6_session = schema6_store.query_session().unwrap();
        let schema6_execution = schema6_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let mut schema7_session = schema7_store.query_session().unwrap();
        let schema7_execution = schema7_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();

        assert_eq!(schema6_execution.results.len(), 1);
        assert_eq!(schema6_execution.results[0].samples.len(), 2);
        assert_eq!(schema6_execution.stats, schema7_execution.stats);
        assert_eq!(
            schema6_execution.semantic_fingerprint_sha256(),
            schema7_execution.semantic_fingerprint_sha256()
        );
        assert_eq!(
            schema6_execution.portable_semantic_fingerprint_sha256(),
            schema7_execution.portable_semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn schema7_and_schema8_default_demand_driven_labels_match_forced_full_labels() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_fixture(schema6.path(), false);
        write_selective_fixture(schema7.path(), true);
        write_selective_schema8_fixture(schema8.path());

        let schema6_store = SegmentStoreReader::open_with_options(
            schema6.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema8_store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut schema6_session = schema6_store.query_session().unwrap();
        let mut schema7_session = schema7_store.query_session().unwrap();
        let mut schema7_full_session = schema7_store.query_session().unwrap();
        let mut schema8_session = schema8_store.query_session().unwrap();
        let mut schema8_full_session = schema8_store.query_session().unwrap();
        let mut schema8_owned_session = schema8_store.query_session().unwrap();
        let mut schema8_profiled_owned_session = schema8_store.query_session().unwrap();
        schema8_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();
        schema8_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap();
        schema7_full_session
            .set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
        schema8_full_session
            .set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
        schema8_owned_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::OwnedStrings)
            .unwrap();
        schema8_profiled_owned_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();

        for query in [
            "sum by (service) (replay_float)",
            "sum by (service) (rate(replay_float[3s]))",
            "sum by (service) (increase(replay_float[3s]))",
            "sum(replay_float{instance=~\"api-[12]\"})",
            "sum by (__name__) (rate(replay_float[3s]))",
            "count by (service) (replay_hist{instance=~\"api-1\"})",
            "group(replay_exp)",
            "count by (service) (rate(replay_hist[3s]))",
            "group by (service) (increase(replay_exp[3s]))",
            "count by (__name__) (replay_hist)",
            "count by (__name__) (rate(replay_hist[3s]))",
        ] {
            let schema7_profile_before = schema7_session.profile();
            let schema7_full_profile_before = schema7_full_session.profile();
            let schema8_profile_before = schema8_session.profile();
            let schema8_full_profile_before = schema8_full_session.profile();
            let schema8_owned_profile_before = schema8_owned_session.profile();
            let schema8_profiled_owned_profile_before = schema8_profiled_owned_session.profile();
            let schema6_execution = schema6_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema7_execution = schema7_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema7_full_execution = schema7_full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_started = Instant::now();
            let schema8_execution = schema8_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_elapsed = schema8_started.elapsed();
            let schema8_owned_execution = schema8_owned_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_profiled_owned_execution = schema8_profiled_owned_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_full_execution = schema8_full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema7_profile = schema7_session
                .profile()
                .delta_since(schema7_profile_before);
            let schema7_full_profile = schema7_full_session
                .profile()
                .delta_since(schema7_full_profile_before);
            let schema8_profile = schema8_session
                .profile()
                .delta_since(schema8_profile_before);
            let schema8_full_profile = schema8_full_session
                .profile()
                .delta_since(schema8_full_profile_before);
            let schema8_owned_profile = schema8_owned_session
                .profile()
                .delta_since(schema8_owned_profile_before);
            let schema8_profiled_owned_profile = schema8_profiled_owned_session
                .profile()
                .delta_since(schema8_profiled_owned_profile_before);

            assert_eq!(schema6_execution.stats, schema7_execution.stats, "{query}");
            assert_eq!(
                schema7_full_execution.stats, schema7_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema8_full_execution.stats, schema8_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema8_owned_execution.stats, schema8_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema8_profiled_owned_execution.stats, schema8_owned_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema6_execution.semantic_fingerprint_sha256(),
                schema7_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema7_full_execution.semantic_fingerprint_sha256(),
                schema7_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema6_execution.semantic_fingerprint_sha256(),
                schema8_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_full_execution.semantic_fingerprint_sha256(),
                schema8_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_owned_execution.semantic_fingerprint_sha256(),
                schema8_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_profiled_owned_execution.semantic_fingerprint_sha256(),
                schema8_owned_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema7_full_execution.portable_semantic_fingerprint_sha256(),
                schema7_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_full_execution.portable_semantic_fingerprint_sha256(),
                schema8_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_owned_execution.portable_semantic_fingerprint_sha256(),
                schema8_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_profiled_owned_execution.portable_semantic_fingerprint_sha256(),
                schema8_owned_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert!(
                schema8_execution.results.iter().all(|result| {
                    result.labels.uses_shared_atoms()
                        && !result.labels.owned_compatibility_materialized()
                }),
                "shared execution materialized an owned compatibility view for {query}"
            );
            assert!(
                schema8_owned_execution
                    .results
                    .iter()
                    .all(|result| !result.labels.uses_shared_atoms()),
                "owned comparator returned shared labels for {query}"
            );
            assert!(
                schema7_execution
                    .results
                    .iter()
                    .all(SegmentQueryResult::labels_are_complete)
            );
            assert!(
                schema8_execution
                    .results
                    .iter()
                    .all(SegmentQueryResult::labels_are_complete)
            );
            assert_eq!(
                schema7_profile.label_pairs_integrity_checked,
                schema7_full_profile.label_pairs_integrity_checked,
                "{query}"
            );
            assert!(schema7_profile.label_rows_selectively_materialized > 0);
            assert!(schema7_profile.label_pairs_omitted > 0);
            assert_eq!(schema7_full_profile.label_rows_selectively_materialized, 0);
            assert_eq!(schema7_full_profile.label_pairs_omitted, 0);
            assert!(
                schema7_profile.label_pairs_materialized
                    < schema7_full_profile.label_pairs_materialized
            );
            assert_eq!(
                schema8_profile.label_pairs_integrity_checked,
                schema8_full_profile.label_pairs_integrity_checked,
                "{query}"
            );
            assert!(schema8_profile.label_rows_selectively_materialized > 0);
            assert!(schema8_profile.label_pairs_omitted > 0);
            assert_eq!(schema8_full_profile.label_rows_selectively_materialized, 0);
            assert_eq!(schema8_full_profile.label_pairs_omitted, 0);
            assert!(
                schema8_profile.label_pairs_materialized
                    < schema8_full_profile.label_pairs_materialized
            );
            if query == "sum by (service) (replay_float)" {
                assert_eq!(schema8_owned_profile.stages, QueryStageProfile::default());
                assert!(schema8_profiled_owned_profile.stages.total_exclusive() > Duration::ZERO);
                let stages = schema8_profile.stages;
                assert!(
                    stages
                        .canonical_row_decode
                        .saturating_add(stages.symbol_resolution)
                        .saturating_add(stages.canonical_identity)
                        .saturating_add(stages.metadata_visit_overhead)
                        > Duration::ZERO
                );
                assert!(
                    stages
                        .symbol_lookup
                        .saturating_add(stages.candidate_selection)
                        .saturating_add(stages.matcher_evaluation)
                        .saturating_add(stages.locator_planning)
                        > Duration::ZERO
                );
                assert!(
                    stages
                        .payload_io
                        .saturating_add(stages.payload_decode)
                        .saturating_add(stages.source_merge)
                        > Duration::ZERO
                );
                assert!(
                    stages
                        .promql_grouping_evaluation
                        .saturating_add(stages.result_construction)
                        > Duration::ZERO
                );
                assert!(stages.total_exclusive() <= schema8_elapsed);
            }
        }

        for query in [
            "sum by (service) (last_over_time(replay_float[3s]))",
            "sum without (instance) (replay_float)",
            "topk(1, replay_float)",
            "bottomk(1, replay_float)",
            "count_values(\"sample\", replay_float)",
            "sum(sum by (service) (replay_float))",
            "label_replace(replay_float, \"copy\", \"$1\", \"instance\", \"(.*)\")",
            "replay_float + 1",
            "replay_float or replay_float",
            "sort(replay_float)",
            "absent(replay_missing)",
            "sum by (service) (replay_hist)",
            "sum by (service) (replay_exp)",
            "sum by (service) (replay_hist_count)",
            "sum by (service) (replay_exp_count)",
            "count without (instance) (replay_hist)",
            "count without (instance) (rate(replay_exp[3s]))",
            "histogram_count(replay_hist)",
            "sum(count by (service) (replay_hist))",
            "sum(group by (service) (increase(replay_exp[3s])))",
        ] {
            let profile_before = schema8_session.profile();
            let full_profile_before = schema8_full_session.profile();
            let execution = schema8_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let full_execution = schema8_full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let owned_execution = schema8_owned_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let profile = schema8_session.profile().delta_since(profile_before);
            let full_profile = schema8_full_session
                .profile()
                .delta_since(full_profile_before);

            assert_eq!(execution.stats, full_execution.stats, "{query}");
            assert_eq!(execution.stats, owned_execution.stats, "{query}");
            assert_eq!(
                execution.semantic_fingerprint_sha256(),
                full_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                execution.semantic_fingerprint_sha256(),
                owned_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert!(
                execution.results.iter().all(|result| {
                    result.labels.uses_shared_atoms()
                        && !result.labels.owned_compatibility_materialized()
                }),
                "shared execution materialized an owned compatibility view for {query}"
            );
            assert_eq!(profile.label_rows_selectively_materialized, 0, "{query}");
            assert_eq!(profile.label_pairs_omitted, 0, "{query}");
            assert_eq!(
                profile.label_pairs_materialized, full_profile.label_pairs_materialized,
                "{query}"
            );
        }

        for range_query in [
            "sum by (service) (rate(replay_float[3s]))",
            "count by (service) (rate(replay_hist[3s]))",
            "group by (service) (increase(replay_exp[3s]))",
        ] {
            let selective_profile_before = schema8_session.profile();
            let full_profile_before = schema8_full_session.profile();
            let range_execution = schema8_session
                .query_promql_range_with_limits(
                    range_query,
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            let full_range_execution = schema8_full_session
                .query_promql_range_with_limits(
                    range_query,
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            let owned_range_execution = schema8_owned_session
                .query_promql_range_with_limits(
                    range_query,
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            let selective_profile = schema8_session
                .profile()
                .delta_since(selective_profile_before);
            let full_profile = schema8_full_session
                .profile()
                .delta_since(full_profile_before);
            assert_eq!(
                range_execution.stats, full_range_execution.stats,
                "{range_query}"
            );
            assert_eq!(
                range_execution.stats, owned_range_execution.stats,
                "{range_query}"
            );
            assert_eq!(
                range_execution.semantic_fingerprint_sha256(),
                full_range_execution.semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert_eq!(
                range_execution.portable_semantic_fingerprint_sha256(),
                full_range_execution.portable_semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert_eq!(
                range_execution.semantic_fingerprint_sha256(),
                owned_range_execution.semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert_eq!(
                range_execution.portable_semantic_fingerprint_sha256(),
                owned_range_execution.portable_semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert!(
                range_execution.results.iter().all(|result| {
                    result.labels.uses_shared_atoms()
                        && !result.labels.owned_compatibility_materialized()
                }),
                "shared range execution materialized an owned compatibility view for {range_query}"
            );
            assert!(
                range_execution
                    .results
                    .iter()
                    .all(SegmentQueryResult::labels_are_complete),
                "{range_query}"
            );
            assert!(
                selective_profile.label_rows_selectively_materialized > 0,
                "{range_query}"
            );
            assert!(selective_profile.label_pairs_omitted > 0, "{range_query}");
            assert_eq!(
                full_profile.label_rows_selectively_materialized, 0,
                "{range_query}"
            );
            assert_eq!(full_profile.label_pairs_omitted, 0, "{range_query}");
        }

        let direct_name = schema8_session
            .query_promql_with_limits(
                "count by (__name__) (replay_hist)",
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        assert_eq!(
            direct_name.results[0].labels.pairs().collect::<Vec<_>>(),
            vec![(METRIC_NAME_LABEL, "replay_hist")]
        );
        let range_name = schema8_session
            .query_promql_with_limits(
                "count by (__name__) (rate(replay_hist[3s]))",
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        assert!(range_name.results[0].labels.is_empty());

        // A selective execution must not populate the session-wide full-label
        // cache with its reduced label set.
        let raw = schema7_session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        assert_eq!(raw.results.len(), 2);
        assert!(raw.results.iter().all(|result| {
            result
                .labels
                .pairs()
                .any(|(name, value)| name == "instance" && (value == "api-1" || value == "api-2"))
        }));

        let mixed_query = r#"count by (service) ({__name__=~"replay_(float|hist|exp)"})"#;
        let mixed_shared = schema8_session
            .query_promql_with_limits(mixed_query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let mixed_owned = schema8_owned_session
            .query_promql_with_limits(mixed_query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        assert_eq!(mixed_shared.stats, mixed_owned.stats);
        assert_eq!(
            mixed_shared.semantic_fingerprint_sha256(),
            mixed_owned.semantic_fingerprint_sha256()
        );
        assert_eq!(
            mixed_shared.portable_semantic_fingerprint_sha256(),
            mixed_owned.portable_semantic_fingerprint_sha256()
        );
        assert!(mixed_shared.results.iter().all(|result| {
            result.labels.uses_shared_atoms() && !result.labels.owned_compatibility_materialized()
        }));

        let detached = {
            let mut session = schema8_store.query_session().unwrap();
            session
                .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
                .unwrap();
            session
                .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
                .unwrap()
        };
        assert!(detached.results.iter().all(|result| {
            result.labels.uses_shared_atoms()
                && result
                    .labels
                    .pairs()
                    .any(|(name, _)| name == METRIC_NAME_LABEL)
        }));
        assert!(schema8_session.query_label_storage_stats().atom_hits > 0);
        assert_eq!(
            schema8_owned_session
                .query_label_storage_stats()
                .atom_lookups,
            0
        );
        let policy_change_error = schema8_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::OwnedStrings)
            .unwrap_err();
        assert_eq!(policy_change_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            schema8_session.query_label_storage_policy(),
            QueryLabelStoragePolicy::SharedAtoms
        );
    }

    #[test]
    fn schema8_demand_driven_native_mixed_kind_row_falls_back_to_full_labels() {
        let schema8 = tempfile::tempdir().unwrap();
        write_mixed_kind_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        for query in [
            "count by (service) (mixed_kind)",
            "group by (service) (mixed_kind)",
        ] {
            let mut demand_session = store.query_session().unwrap();
            let mut full_session = store.query_session().unwrap();
            demand_session
                .set_label_materialization_policy(QueryLabelMaterializationPolicy::DemandDriven);
            full_session.set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);

            let demand_before = demand_session.profile();
            let full_before = full_session.profile();
            let demand = demand_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let full = full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let demand_profile = demand_session.profile().delta_since(demand_before);
            let full_profile = full_session.profile().delta_since(full_before);

            assert_eq!(demand.stats, full.stats, "{query}");
            assert_eq!(
                demand.semantic_fingerprint_sha256(),
                full.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                demand.portable_semantic_fingerprint_sha256(),
                full.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                demand_profile.label_pairs_integrity_checked,
                full_profile.label_pairs_integrity_checked,
                "{query}"
            );
            assert_eq!(
                demand_profile.label_rows_selectively_materialized, 0,
                "mixed-kind rows must not use reduced labels for {query}"
            );
            assert_eq!(demand_profile.label_pairs_omitted, 0, "{query}");
            assert_eq!(
                demand_profile.label_pairs_materialized, full_profile.label_pairs_materialized,
                "{query}"
            );
            assert!(demand_profile.label_pairs_materialized >= 4, "{query}");
        }
    }

    #[test]
    fn schema7_and_schema8_compact_query_labels_match_owned_end_to_end() {
        let schema7 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_fixture(schema7.path(), true);
        write_selective_schema8_fixture(schema8.path());

        for (path, storage_schema_policy) in [
            (schema7.path(), SegmentStoreSchemaPolicy::StrictSchema7),
            (schema8.path(), SegmentStoreSchemaPolicy::StrictSchema8),
        ] {
            let store = SegmentStoreReader::open_with_options(
                path,
                SegmentStoreOpenOptions {
                    storage_schema_policy,
                    ..SegmentStoreOpenOptions::default()
                },
            )
            .unwrap();
            let mut owned = store.query_session().unwrap();
            let mut compact = store.query_session().unwrap();
            owned
                .set_query_label_storage_policy(QueryLabelStoragePolicy::OwnedStrings)
                .unwrap();
            compact
                .set_query_label_arena_max_bytes(16 * 1024 * 1024)
                .unwrap();
            compact
                .set_query_label_storage_policy(QueryLabelStoragePolicy::CompactIds)
                .unwrap();

            for query in [
                "replay_float",
                "sum by (service) (replay_float)",
                "sum by (service) (rate(replay_float[3s]))",
                "count by (service) (replay_hist)",
                "count by (service) (rate(replay_hist[3s]))",
                "group by (service) (increase(replay_exp[3s]))",
                "sum(replay_float{instance=~\"api-[12]\"})",
                "replay_float{service=\"does-not-exist\"}",
            ] {
                let owned_execution = owned
                    .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                    .unwrap();
                let compact_execution = compact
                    .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                    .unwrap();
                assert_eq!(owned_execution.stats, compact_execution.stats, "{query}");
                assert_eq!(
                    owned_execution.semantic_fingerprint_sha256(),
                    compact_execution.semantic_fingerprint_sha256(),
                    "{query}"
                );
                assert_eq!(
                    owned_execution.portable_semantic_fingerprint_sha256(),
                    compact_execution.portable_semantic_fingerprint_sha256(),
                    "{query}"
                );
                assert!(
                    compact_execution.results.iter().all(|result| {
                        result.labels.uses_compact_ids()
                            && !result.labels.owned_compatibility_materialized()
                            && result.labels_are_complete()
                    }),
                    "compact execution escaped or materialized compatibility labels for {query}"
                );
            }

            let owned_range = owned
                .query_promql_range_with_limits(
                    "sum by (service) (rate(replay_float[3s]))",
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            let compact_range = compact
                .query_promql_range_with_limits(
                    "sum by (service) (rate(replay_float[3s]))",
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            assert_eq!(owned_range.stats, compact_range.stats);
            assert_eq!(
                owned_range.semantic_fingerprint_sha256(),
                compact_range.semantic_fingerprint_sha256()
            );
            assert_eq!(
                owned_range.portable_semantic_fingerprint_sha256(),
                compact_range.portable_semantic_fingerprint_sha256()
            );
            assert!(compact_range.results.iter().all(|result| {
                result.labels.uses_compact_ids()
                    && !result.labels.owned_compatibility_materialized()
            }));

            let stats = compact.query_label_storage_stats();
            assert_eq!(stats.compact_arena_budget_bytes, 16 * 1024 * 1024);
            assert!(stats.compact_source_symbol_translations > 0);
            assert!(stats.compact_source_symbol_translation_misses > 0);
            assert!(stats.compact_atom_misses > 0);
            assert_eq!(stats.compact_arena_admission_refusals, 0);
            assert_eq!(stats.compact_compatibility_materializations, 0);
            assert_eq!(
                stats.compact_source_symbol_translations,
                stats
                    .compact_source_symbol_translation_hits
                    .saturating_add(stats.compact_source_symbol_translation_misses)
            );
            assert_eq!(
                stats.compact_arena_current_bytes,
                stats
                    .compact_atom_bytes
                    .saturating_add(stats.compact_pair_bytes)
                    .saturating_add(stats.compact_hash_directory_bytes)
                    .saturating_add(stats.compact_translation_bytes)
            );

            let mut compact_range_only = store.query_session().unwrap();
            compact_range_only
                .set_query_label_storage_policy(QueryLabelStoragePolicy::CompactIds)
                .unwrap();
            let carried = compact_range_only
                .query_promql_with_limits(
                    "sum by (service) (rate(replay_float[3s]))",
                    0,
                    3_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            assert!(carried.results.iter().all(|result| {
                result.labels.uses_compact_ids()
                    && !result.labels.owned_compatibility_materialized()
            }));
            let carried_stats = compact_range_only.query_label_storage_stats();
            assert_eq!(
                carried_stats.compact_atom_lookups,
                carried_stats.compact_source_symbol_translation_misses,
                "the selective range/group path must not re-intern owned label strings"
            );

            let retained_before_session_drop = compact_range.results[0]
                .labels
                .compact_charge_categories_for_test()
                .unwrap();
            assert!(retained_before_session_drop.4 > 0);
            drop(owned);
            drop(compact);
            drop(compact_range_only);
            let retained_after_session_drop = compact_range.results[0]
                .labels
                .compact_charge_categories_for_test()
                .unwrap();
            assert_eq!(retained_after_session_drop.4, 0);
            assert_eq!(
                retained_after_session_drop.0,
                retained_after_session_drop
                    .1
                    .saturating_add(retained_after_session_drop.2)
                    .saturating_add(retained_after_session_drop.3)
                    .saturating_add(retained_after_session_drop.4)
            );
            assert!(compact_range.results.iter().all(|result| {
                result
                    .labels
                    .pairs()
                    .all(|(name, value)| !name.is_empty() && !value.is_empty())
            }));
        }
    }

    #[test]
    fn compact_query_labels_reject_schema6_and_budget_refusal_never_falls_back() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_fixture(schema6.path(), false);
        write_selective_schema8_fixture(schema8.path());

        let schema6_store = SegmentStoreReader::open_with_options(
            schema6.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut schema6_session = schema6_store.query_session().unwrap();
        assert_eq!(
            schema6_session.query_label_storage_policy(),
            QueryLabelStoragePolicy::OwnedStrings
        );
        let schema6_error = schema6_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::CompactIds)
            .unwrap_err();
        assert_eq!(schema6_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            schema6_session.query_label_storage_policy(),
            QueryLabelStoragePolicy::OwnedStrings
        );

        let schema8_store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut schema8_session = schema8_store.query_session().unwrap();
        assert_eq!(
            schema8_session.query_label_storage_policy(),
            QueryLabelStoragePolicy::CompactIds
        );
        schema8_session.set_query_label_arena_max_bytes(1).unwrap();
        schema8_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::CompactIds)
            .unwrap();
        let budget_error = schema8_session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .unwrap_err();
        assert!(
            budget_error
                .to_string()
                .contains("query label arena budget")
        );
        assert_eq!(
            schema8_session.query_label_storage_policy(),
            QueryLabelStoragePolicy::CompactIds
        );
        let stats = schema8_session.query_label_storage_stats();
        assert_eq!(stats.compact_arena_admission_refusals, 1);
        assert_eq!(stats.compact_atom_lookups, 0);
        assert_eq!(stats.compact_label_sets, 0);
        assert_eq!(stats.compact_compatibility_materializations, 0);
        assert!(
            schema8_session
                .set_query_label_arena_max_bytes(16 * 1024 * 1024)
                .is_err()
        );
    }

    #[test]
    fn query_label_storage_policy_freezes_on_empty_prefetch_and_parse_error_attempts() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut empty_session = store.query_session().unwrap();
        let empty = empty_session
            .query_promql_with_limits("replay_missing", 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        assert!(empty.results.is_empty());
        let empty_error = empty_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(empty_error.kind(), io::ErrorKind::InvalidInput);

        let mut prefetch_session = store.query_session().unwrap();
        prefetch_session
            .prefetch_promql_data("replay_float", 0, 3_000)
            .unwrap();
        let prefetch_error = prefetch_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(prefetch_error.kind(), io::ErrorKind::InvalidInput);

        let mut malformed_session = store.query_session().unwrap();
        malformed_session
            .query_promql_with_limits("sum(", 0, 3_000, QueryLimits::unlimited())
            .unwrap_err();
        let malformed_error = malformed_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(malformed_error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn query_instrumentation_off_is_semantically_identical_to_detailed() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut off_session = store.query_session().unwrap();
        let mut detailed_session = store.query_session().unwrap();
        assert_eq!(
            off_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );
        detailed_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();

        let off_before = off_session.profile();
        let detailed_before = detailed_session.profile();
        let query = "sum by (service) (rate(replay_float[3s]))";
        let off = off_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let detailed = detailed_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let off_profile = off_session.profile().delta_since(off_before);
        let detailed_profile = detailed_session.profile().delta_since(detailed_before);

        assert_eq!(off.stats, detailed.stats);
        assert_eq!(
            off.semantic_fingerprint_sha256(),
            detailed.semantic_fingerprint_sha256()
        );
        assert_eq!(
            off.portable_semantic_fingerprint_sha256(),
            detailed.portable_semantic_fingerprint_sha256()
        );
        assert_eq!(off_profile.stages, QueryStageProfile::default());
        assert!(detailed_profile.stages.total_exclusive() > Duration::ZERO);
    }

    #[test]
    fn query_instrumentation_mode_freezes_on_first_query_prewarm_or_prefetch() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut query_session = store.query_session().unwrap();
        query_session
            .query_promql_with_limits("1", 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let query_error = query_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap_err();
        assert_eq!(query_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            query_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );

        let mut prewarm_session = store.query_session().unwrap();
        prewarm_session.prewarm_promql("1", 0, 3_000).unwrap();
        let prewarm_error = prewarm_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap_err();
        assert_eq!(prewarm_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            prewarm_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );

        let mut prefetch_session = store.query_session().unwrap();
        prefetch_session
            .prefetch_promql_data("1", 0, 3_000)
            .unwrap();
        let prefetch_error = prefetch_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap_err();
        assert_eq!(prefetch_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            prefetch_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );
    }

    #[test]
    fn query_instrumentation_detailed_missing_equality_records_no_payload_or_result_work() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut session = store.query_session().unwrap();
        session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();
        let before = session.profile();
        let execution = session
            .query_promql_with_limits(
                "replay_float{service=\"does-not-exist\"}",
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        let stages = session.profile().delta_since(before).stages;

        assert!(execution.results.is_empty());
        assert_eq!(stages.payload_io, Duration::ZERO);
        assert_eq!(stages.payload_decode, Duration::ZERO);
        assert_eq!(stages.source_merge, Duration::ZERO);
        assert_eq!(stages.result_construction, Duration::ZERO);
        assert!(
            stages.symbol_lookup > Duration::ZERO || stages.matcher_evaluation > Duration::ZERO
        );
    }

    #[test]
    fn query_label_storage_policy_freezes_before_touched_series_page_corruption() {
        use crate::storage::series::v3::{
            SERIES_HEADER_LEN_V3, SERIES_HOT_PAGE_HEADER_LEN_V1, SeriesHeaderV3,
        };

        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let inventory = read_manifest_inventory(schema8.path().join("manifest"))
            .unwrap()
            .unwrap();
        let series_path = schema8
            .path()
            .join(&inventory.segments[0].segment_id)
            .join(SegmentFile::Series.filename());
        let mut series = fs::read(&series_path).unwrap();
        let header = SeriesHeaderV3::decode(&series[..SERIES_HEADER_LEN_V3]).unwrap();
        let corrupt_offset =
            usize::try_from(header.hot_pages_offset).unwrap() + SERIES_HOT_PAGE_HEADER_LEN_V1;
        series[corrupt_offset] ^= 0x80;
        fs::write(&series_path, series).unwrap();

        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();
        session.set_query_label_arena_max_bytes(1).unwrap();
        session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::CompactIds)
            .unwrap();
        let profile_before = session.profile();
        let query_started = Instant::now();
        let query_error = session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .unwrap_err();
        let query_elapsed = query_started.elapsed();
        let stages = session.profile().delta_since(profile_before).stages;

        assert!(
            query_error
                .to_string()
                .contains("series v3 hot page CRC mismatch")
        );
        assert!(stages.total_exclusive() > Duration::ZERO);
        assert!(stages.total_exclusive() <= query_elapsed);
        assert!(stages.metadata_visit_overhead > Duration::ZERO);
        assert_eq!(stages.payload_io, Duration::ZERO);
        assert_eq!(stages.payload_decode, Duration::ZERO);
        assert_eq!(stages.result_construction, Duration::ZERO);
        assert_eq!(
            session.query_label_storage_stats(),
            QueryLabelStorageStats {
                compact_arena_budget_bytes: 1,
                ..QueryLabelStorageStats::default()
            },
            "the touched page must fail before label interning"
        );
        let policy_error = session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(policy_error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn query_instrumentation_detailed_records_transient_metadata_budget_refusal() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();

        let governor_before = store.metadata_runtime.snapshot().governor;
        let blocker = store
            .metadata_runtime
            .governor()
            .reserve_in_flight_for_usage(
                governor_before
                    .in_flight_max_bytes
                    .checked_sub(governor_before.in_flight_bytes)
                    .and_then(|remaining| remaining.checked_sub(1))
                    .expect("fixture leaves one reservable metadata byte"),
                crate::storage::metadata_governor::MetadataUsageClass::Scratch,
            )
            .expect("reserve all but one in-flight metadata byte");
        let runtime_before = store.metadata_runtime.snapshot();
        let profile_before = session.profile();
        let query_started = Instant::now();
        let query_error = session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .unwrap_err();
        let query_elapsed = query_started.elapsed();
        let stages = session.profile().delta_since(profile_before).stages;
        let runtime_refused = store.metadata_runtime.snapshot();

        assert!(query_error.to_string().contains("metadata"));
        assert!(stages.total_exclusive() > Duration::ZERO);
        assert!(stages.total_exclusive() <= query_elapsed);
        assert!(stages.metadata_visit_overhead > Duration::ZERO);
        assert_eq!(stages.payload_io, Duration::ZERO);
        assert_eq!(stages.payload_decode, Duration::ZERO);
        assert_eq!(stages.result_construction, Duration::ZERO);
        assert_eq!(runtime_refused.reads, runtime_before.reads);
        assert_eq!(
            runtime_refused.cache.sticky_artifacts,
            runtime_before.cache.sticky_artifacts
        );
        assert_eq!(
            runtime_refused.governor.in_flight_refusals,
            runtime_before.governor.in_flight_refusals + 1
        );

        drop(blocker);
        let retry = session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .expect("transient metadata-budget refusal must allow a clean retry");
        assert!(!retry.results.is_empty());
    }

    #[test]
    fn schema7_and_schema8_promql_query_facades_match() {
        let schema7 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_fixture(schema7.path(), true);
        write_schema8_fixture(schema8.path());

        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema8_store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let query = "replay_float";
        let mut schema7_session = schema7_store.query_session().unwrap();
        let schema7_execution = schema7_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let mut schema8_session = schema8_store.query_session().unwrap();
        let schema8_execution = schema8_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();

        assert_eq!(schema7_execution.results, schema8_execution.results);
        assert_eq!(
            schema7_execution.semantic_fingerprint_sha256(),
            schema8_execution.semantic_fingerprint_sha256()
        );
        assert_eq!(
            schema7_execution.portable_semantic_fingerprint_sha256(),
            schema8_execution.portable_semantic_fingerprint_sha256()
        );

        let mut schema8_normalized_stats = schema8_execution.stats;
        schema8_normalized_stats.index_postings_bytes_read =
            schema7_execution.stats.index_postings_bytes_read;
        assert_eq!(schema7_execution.stats, schema8_normalized_stats);
        assert!(
            schema8_execution.stats.index_postings_bytes_read
                < schema7_execution.stats.index_postings_bytes_read,
            "adaptive postings should issue fewer exact-postings payload bytes"
        );
    }

    #[test]
    fn schema8_public_store_reader_surfaces_use_the_v9_facade() {
        let schema8 = tempfile::tempdir().unwrap();
        write_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let exact = store
            .query_exact(&[(METRIC_NAME_LABEL, "replay_float")], 0, 3_000)
            .unwrap();
        let selector = store
            .query_selector(
                &SegmentSelector::with_metric(
                    "replay_float",
                    vec![LabelMatcher::eq("service", "api")],
                ),
                0,
                3_000,
            )
            .unwrap();
        let promql = store.query_promql("replay_float", 0, 3_000).unwrap();

        assert_eq!(exact, selector);
        assert_eq!(exact, promql);
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].samples, [(1_000, 1.0), (2_000, 2.0)]);
        assert_eq!(
            store.metric_names(0, 3_000).unwrap(),
            ["replay_float", "replay_int"]
        );
        assert_eq!(
            store.label_names(0, 3_000).unwrap(),
            [METRIC_NAME_LABEL, "service"]
        );
        assert_eq!(
            store.label_values("service", 0, 3_000).unwrap(),
            ["api", "worker"]
        );
    }

    #[test]
    fn schema6_and_schema7_smoke_reports_match_through_metadata_facade() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema6.path(), false);
        write_fixture(schema7.path(), true);

        let schema6_store = SegmentStoreReader::open_with_options(
            schema6.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let schema6_report = schema6_store.smoke_verify(0, 3_000, 1).unwrap();
        let schema7_report = schema7_store.smoke_verify(0, 3_000, 1).unwrap();

        assert_eq!(schema6_report, schema7_report);
        assert_eq!(schema7_report.totals.series, 2);
        assert_eq!(schema7_report.totals.chunks, 2);
        assert_eq!(schema7_report.sample_series.len(), 2);
        assert!(
            schema7_report
                .queries
                .iter()
                .all(|query| query.result_samples > 0)
        );
    }

    #[test]
    fn schema7_smoke_rejects_corrupt_indexed_chunk_prefix() {
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema7.path(), true);
        let inventory = read_manifest_inventory(schema7.path().join("manifest"))
            .unwrap()
            .unwrap();
        let chunks_path = schema7
            .path()
            .join(&inventory.segments[0].segment_id)
            .join(SegmentFile::Chunks.filename());
        let mut chunks = fs::read(&chunks_path).unwrap();
        chunks[CHUNK_FRAME_HEADER_LEN] ^= 0x80;
        fs::write(chunks_path, chunks).unwrap();

        let store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let error = store.smoke_verify(0, 3_000, 1).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("indexed prefix crc mismatch"));
    }

    #[test]
    fn schema7_smoke_resumes_a_series_across_bounded_payload_batches() {
        const CHUNKS: usize = 65;

        let schema7 = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(schema7.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(43)
            .with_storage_schema(SegmentStorageSchema::Schema7);
        let mut writer = SegmentWriter::new(config).unwrap();
        for chunk_index in 0..CHUNKS {
            let timestamp = 1_000 + chunk_index as u64;
            writer
                .record_samples_ordered_with_label_visitor(
                    SeriesRef::new(1),
                    &[(timestamp, chunk_index as f64)],
                    |visit| {
                        visit(METRIC_NAME_LABEL, "many_chunks");
                        visit("service", "api");
                    },
                )
                .unwrap();
        }
        writer.flush().unwrap();

        let store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        assert_eq!(store.segments.len(), 1);
        let mut report = SegmentStoreSmokeReport::default();
        store.segments[0]
            .collect_smoke_report(0, 3_000, CHUNKS, true, &mut report)
            .unwrap();

        assert_eq!(report.totals.chunks, CHUNKS as u64);
        assert_eq!(report.sample_series.len(), CHUNKS);
        assert_eq!(report.sample_series.first().unwrap().min_time_ms, 1_000);
        assert_eq!(report.sample_series.last().unwrap().min_time_ms, 1_064);
        assert!(
            report
                .sample_series
                .iter()
                .all(|sample| sample.samples == 1 && sample.kind == ChunkKind::Float)
        );
    }

    #[test]
    fn smoke_facade_error_mapping_preserves_transient_and_structural_kinds() {
        for (cache_error, expected_kind) in [
            (
                MetadataCacheError::transient(io::ErrorKind::TimedOut, "metadata read timed out"),
                io::ErrorKind::TimedOut,
            ),
            (
                MetadataCacheError::structural(
                    StructuralMetadataErrorKind::UnexpectedEof,
                    "metadata page is truncated",
                ),
                io::ErrorKind::UnexpectedEof,
            ),
        ] {
            let error = super::query_reader::metadata_facade_io_error(
                SegmentMetadataFacadeError::Schema7Metadata(Schema7MetadataReaderError::Cache(
                    cache_error,
                )),
            );
            assert_eq!(error.kind(), expected_kind);
        }

        let error = super::query_reader::metadata_facade_io_error(
            SegmentMetadataFacadeError::RefSetAllocation(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "series-ref allocation failed",
            )),
        );
        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    }

    fn write_fixture(path: &Path, schema7: bool) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42)
            .with_storage_schema(if schema7 {
                SegmentStorageSchema::Schema7
            } else {
                SegmentStorageSchema::Schema6
            });
        write_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn first_segment_file(path: &Path, file: SegmentFile) -> std::path::PathBuf {
        let manifest = read_manifest_inventory(path.join("manifest"))
            .unwrap()
            .unwrap();
        path.join(&manifest.segments[0].segment_id)
            .join(file.filename())
    }

    fn reseal_first_frame_crc(chunks: &mut [u8]) {
        let frame_len = u32::from_le_bytes(chunks[0..4].try_into().unwrap()) as usize;
        assert!(frame_len <= chunks.len());
        let frame_crc = crc32c::crc32c(&chunks[CHUNK_FRAME_HEADER_LEN..frame_len]);
        chunks[4..8].copy_from_slice(&frame_crc.to_le_bytes());
    }

    fn write_schema6_typed_lane_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(46)
            .with_storage_schema(SegmentStorageSchema::Schema6);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(
                    1_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(8.0),
                        min: Some(0.0),
                        max: Some(4.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 3],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "typed_lane_fixture");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_schema8_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        write_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn write_float_codec_fixture(path: &Path, raw: bool) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(45)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        let samples = [(1_000, 1.0), (2_000, 1.0), (3_000, 1.5), (4_000, -2.25)];
        if raw {
            writer
                .record_samples_raw_ordered_with_label_visitor(
                    SeriesRef::new(1),
                    &samples,
                    |visit| {
                        visit(METRIC_NAME_LABEL, "codec_float");
                        visit("service", "api");
                    },
                )
                .unwrap();
        } else {
            writer
                .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &samples, |visit| {
                    visit(METRIC_NAME_LABEL, "codec_float");
                    visit("service", "api");
                })
                .unwrap();
        }
        writer.flush().unwrap();
    }

    fn write_rechunked_semantic_fixture(path: &Path, split: bool) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(47)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        let samples = [(1_000, 1.0), (2_000, 1.5), (3_000, -0.0), (4_000, 8.0)];
        {
            let mut record = |samples: &[(u64, f64)]| {
                writer
                    .record_samples_ordered_with_label_visitor(
                        SeriesRef::new(1),
                        samples,
                        |visit| {
                            visit(METRIC_NAME_LABEL, "rechunked_semantics");
                            visit("service", "api");
                        },
                    )
                    .unwrap();
            };
            if split {
                record(&samples[..2]);
                record(&samples[2..]);
            } else {
                record(&samples);
            }
        }
        writer.flush().unwrap();
    }

    fn write_mixed_kind_interleaving_fixture(path: &Path, interleaved: bool) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(49)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        let write_float = |writer: &mut SegmentWriter, sample: &[(u64, f64)]| {
            writer
                .record_samples_ordered_with_label_visitor(SeriesRef::new(1), sample, |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_kind_rechunk");
                    visit("service", "api");
                })
                .unwrap();
        };
        let write_int = |writer: &mut SegmentWriter| {
            writer
                .record_i64_samples_ordered_with_label_visitor(
                    SeriesRef::new(1),
                    &[(2_000, -7)],
                    |visit| {
                        visit(METRIC_NAME_LABEL, "mixed_kind_rechunk");
                        visit("service", "api");
                    },
                )
                .unwrap();
        };

        write_float(&mut writer, &[(1_000, 1.0)]);
        if interleaved {
            write_int(&mut writer);
        }
        write_float(&mut writer, &[(3_000, -0.0)]);
        if !interleaved {
            write_int(&mut writer);
        }
        writer.flush().unwrap();
    }

    #[derive(Debug, Clone, Copy)]
    enum TypedSensitivityMutation {
        None,
        HistogramMetadataFlags,
        HistogramSumBits,
        ExponentialHistogramScale,
        SummaryQuantileValue,
    }

    fn write_typed_sensitivity_fixture(path: &Path, mutation: TypedSensitivityMutation) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(48)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(
                    1_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(
                            if matches!(mutation, TypedSensitivityMutation::HistogramSumBits) {
                                0.0
                            } else {
                                -0.0
                            },
                        ),
                        min: Some(0.0),
                        max: Some(2.0),
                        metadata: TypedSampleMetadata {
                            flags: u32::from(matches!(
                                mutation,
                                TypedSensitivityMutation::HistogramMetadataFlags
                            )),
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "typed_histogram");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(
                    1_100,
                    ExponentialHistogramValue {
                        count: 2,
                        sum: Some(3.0),
                        min: Some(1.0),
                        max: Some(2.0),
                        scale: if matches!(
                            mutation,
                            TypedSensitivityMutation::ExponentialHistogramScale
                        ) {
                            2
                        } else {
                            1
                        },
                        zero_threshold: 0.0,
                        zero_count: 0,
                        metadata: TypedSampleMetadata::default(),
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![1, 1],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "typed_exponential_histogram");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer
            .record_summary_samples_ordered_with_label_visitor(
                SeriesRef::new(3),
                &[(
                    1_200,
                    SummaryValue {
                        count: 1,
                        sum: 42.0,
                        metadata: TypedSampleMetadata::default(),
                        quantiles: vec![SummaryQuantileValue {
                            quantile: 0.5,
                            value: if matches!(
                                mutation,
                                TypedSensitivityMutation::SummaryQuantileValue
                            ) {
                                43.0
                            } else {
                                42.0
                            },
                        }],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "typed_summary");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_all_kinds_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(46)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[
                    (1_000, f64::from_bits(0x8000_0000_0000_0000)),
                    (2_000, f64::from_bits(0x7ff8_0000_0000_0042)),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "all_float");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer
            .record_i64_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(1_100, i64::MIN + 7)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "all_int");
                    visit("service", "worker");
                },
            )
            .unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(3),
                &[(
                    1_200,
                    HistogramValue {
                        count: 3,
                        sum: Some(f64::from_bits(0x8000_0000_0000_0000)),
                        min: None,
                        max: Some(2.0),
                        metadata: TypedSampleMetadata {
                            start_time_ms: Some(100),
                            flags: 7,
                            temporality: OtlpAggregationTemporality::Delta,
                            reset_hint: CounterResetHint::NotCounterReset,
                        },
                        explicit_bounds: vec![-1.0, 1.0],
                        bucket_counts: vec![1, 1, 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "all_hist");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(4),
                &[(
                    1_300,
                    ExponentialHistogramValue {
                        count: 4,
                        sum: Some(-3.5),
                        min: Some(-2.0),
                        max: None,
                        scale: -3,
                        zero_threshold: 0.125,
                        zero_count: 1,
                        metadata: TypedSampleMetadata {
                            start_time_ms: Some(50),
                            flags: 0,
                            temporality: OtlpAggregationTemporality::Cumulative,
                            reset_hint: CounterResetHint::CounterReset,
                        },
                        positive: ExponentialHistogramBuckets {
                            offset: -2,
                            counts: vec![1, 1],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 3,
                            counts: vec![1],
                        },
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "all_exp");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer
            .record_summary_samples_ordered_with_label_visitor(
                SeriesRef::new(5),
                &[(
                    1_400,
                    SummaryValue {
                        count: 9,
                        sum: f64::INFINITY,
                        metadata: TypedSampleMetadata {
                            start_time_ms: None,
                            flags: 1,
                            temporality: OtlpAggregationTemporality::Unspecified,
                            reset_hint: CounterResetHint::GaugeType,
                        },
                        quantiles: vec![
                            SummaryQuantileValue {
                                quantile: 0.5,
                                value: -0.0,
                            },
                            SummaryQuantileValue {
                                quantile: 0.99,
                                value: 42.0,
                            },
                        ],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "all_summary");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_selective_fixture(path: &Path, schema7: bool) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(43)
            .with_storage_schema(if schema7 {
                SegmentStorageSchema::Schema7
            } else {
                SegmentStorageSchema::Schema6
            });
        write_selective_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn write_selective_schema8_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(43)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        write_selective_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn write_mixed_kind_schema8_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(44)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_kind");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(
                    2_000,
                    HistogramValue {
                        count: 1,
                        sum: Some(0.5),
                        min: Some(0.5),
                        max: Some(0.5),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 0],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_kind");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_selective_fixture_samples(mut writer: SegmentWriter) {
        for (series_ref, instance, samples) in [
            (SeriesRef::new(1), "api-1", [(1_000, 1.0), (2_000, 2.0)]),
            (SeriesRef::new(2), "api-2", [(1_000, 2.0), (2_000, 4.0)]),
        ] {
            writer
                .record_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                    visit(METRIC_NAME_LABEL, "replay_float");
                    visit("service", "api");
                    visit("instance", instance);
                    visit("region", "sg");
                })
                .unwrap();
        }
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(3),
                &[
                    (
                        1_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(0.5),
                            min: Some(0.5),
                            max: Some(0.5),
                            metadata: TypedSampleMetadata::default(),
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![1, 0],
                        },
                    ),
                    (
                        2_000,
                        HistogramValue {
                            count: 3,
                            sum: Some(4.0),
                            min: Some(0.5),
                            max: Some(2.0),
                            metadata: TypedSampleMetadata::default(),
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![1, 2],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_hist");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(4),
                &[
                    (
                        1_000,
                        ExponentialHistogramValue {
                            count: 1,
                            sum: Some(0.5),
                            min: Some(0.5),
                            max: Some(0.5),
                            scale: 1,
                            zero_threshold: 0.0,
                            zero_count: 0,
                            metadata: TypedSampleMetadata::default(),
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![1, 0],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![0],
                            },
                        },
                    ),
                    (
                        2_000,
                        ExponentialHistogramValue {
                            count: 3,
                            sum: Some(4.0),
                            min: Some(0.5),
                            max: Some(2.0),
                            scale: 1,
                            zero_threshold: 0.0,
                            zero_count: 0,
                            metadata: TypedSampleMetadata::default(),
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![1, 2],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![0],
                            },
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_exp");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_fixture_samples(mut writer: SegmentWriter) {
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.0), (2_000, 2.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_float");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer
            .record_i64_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(1_500, 7)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_int");
                    visit("service", "worker");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }
}
