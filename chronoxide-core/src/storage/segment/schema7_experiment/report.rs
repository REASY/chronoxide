use serde::Serialize;

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
