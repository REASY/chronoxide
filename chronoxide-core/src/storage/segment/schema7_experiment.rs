//! Short, layout-neutral replay/readback gate for storage-schema experiments.

#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::io;
#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use std::time::Instant;

#[cfg(test)]
use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkSamples, DecodedChunkLayout};
#[cfg(test)]
use crate::storage::head::PROMETHEUS_STALE_NAN_BITS;

#[cfg(test)]
use super::*;

const VERIFIED_SELECTION_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-verified-storage-selection-v1\0";
const VERIFIED_DECODED_SEMANTIC_FINGERPRINT_DOMAIN: &[u8] =
    b"chronoxide-verified-decoded-storage-semantics-v2\0";

mod fingerprint;
mod helpers;
mod inventory;
mod report;
mod verify;

pub use report::*;
pub use verify::{
    verify_experimental_storage_corpus, verify_experimental_storage_corpus_with_decoded_semantics,
    verify_experimental_storage_corpus_with_exact_postings,
};

#[cfg(test)]
use fingerprint::{DecodedSemanticAccumulator, TopologyIndependentDecodedSemanticAccumulator};
#[cfg(test)]
use helpers::chunk_kind_id;
#[cfg(test)]
use inventory::{
    ChunkInventoryAccumulator, FloatCodecCandidatesAccumulator, TIMESTAMP_CODEC_TIE_RULE,
    TimestampCodecCandidatesAccumulator, observe_float_codec_candidates,
    observe_float_value_distribution, timestamp_candidate_sizes, uleb128_u128_len, zigzag_i128,
};

#[cfg(test)]
mod tests;
