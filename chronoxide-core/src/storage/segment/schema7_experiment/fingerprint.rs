use std::collections::BTreeMap;
use std::io;

use sha2::{Digest, Sha256};

use crate::storage::chunk::{ChunkKind, ChunkSamples};
use crate::storage::encoding::VarLenEncoding;
use crate::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue, SummaryValue,
    TypedSampleMetadata,
};

use super::super::invalid_segment_data;
use super::VERIFIED_DECODED_SEMANTIC_FINGERPRINT_DOMAIN;
use super::helpers::{
    checked_add, chunk_kind_id, hash_bytes, hash_u32, hash_u64, hex_digest, reset_hint_id,
    temporality_id,
};
use super::report::ExperimentalExactPostingsVerification;

const VERIFIED_EXACT_POSTINGS_FINGERPRINT_DOMAIN: &[u8] =
    b"chronoxide-verified-exact-postings-v1\0";
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

/// A streaming cryptographic multiset accumulator. Physical segment IDs,
/// local refs, chunk boundaries/order, offsets, and in-order/OOO lanes are not
/// inputs. Two independently domain-separated SHA-256 record digests are
/// summed modulo 2^256, retaining duplicate multiplicity without sorting the
/// corpus in memory; the final report value hashes both sums and the count.
pub(super) struct TopologyIndependentDecodedSemanticAccumulator {
    sum_a: [u8; 32],
    sum_b: [u8; 32],
    pub(super) samples: u64,
}

impl TopologyIndependentDecodedSemanticAccumulator {
    pub(super) fn new() -> Self {
        Self {
            sum_a: [0; 32],
            sum_b: [0; 32],
            samples: 0,
        }
    }

    pub(super) fn series_digest(labels: &[(String, String)]) -> io::Result<[u8; 32]> {
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

    pub(super) fn observe_samples(
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

    pub(super) fn finish(self) -> String {
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

pub(super) struct ExactPostingsAccumulator {
    hasher: Sha256,
    lists: u64,
    decoded_refs: u64,
    encoded_bytes: u64,
    scratch: Vec<u8>,
}

impl ExactPostingsAccumulator {
    pub(super) fn new(segment_count: u32) -> Self {
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

    pub(super) fn start_segment(&mut self, segment_id: &str) -> io::Result<()> {
        hash_bytes(&mut self.hasher, segment_id.as_bytes())
    }

    pub(super) fn observe(
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

    pub(super) fn finish(self) -> ExperimentalExactPostingsVerification {
        ExperimentalExactPostingsVerification {
            logical_fingerprint: hex_digest(self.hasher.finalize().into()),
            lists: self.lists,
            decoded_refs: self.decoded_refs,
            encoded_bytes: self.encoded_bytes,
        }
    }
}

pub(super) struct DecodedSemanticAccumulator {
    hasher: Sha256,
    series_lanes: BTreeMap<(u8, u8), DecodedSemanticLaneAccumulator>,
}

impl DecodedSemanticAccumulator {
    pub(super) fn new(segment_count: u32, series_sample_per_segment: Option<u32>) -> Self {
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

    pub(super) fn start_segment(
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

    pub(super) fn start_series(
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

    pub(super) fn observe_chunk(&mut self, file_id: u8, samples: &ChunkSamples) -> io::Result<u64> {
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

    pub(super) fn finish_series(&mut self, sample_count: u64) -> io::Result<()> {
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

    pub(super) fn finish(self) -> String {
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
