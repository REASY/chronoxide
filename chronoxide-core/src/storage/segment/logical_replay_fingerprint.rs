//! Layout-neutral logical replay identity for schema A/B validation.
//!
//! This module intentionally accepts decoded logical inputs and exact indexed
//! chunk bytes. It has no dependency on a particular series or chunk-index
//! layout, so schema-specific readers can be added later without changing the
//! canonical byte stream.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::io::{self, Write};

use sha2::{Digest, Sha256};
use thiserror::Error;

use super::ChunkKind;

pub const LOGICAL_REPLAY_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-logical-replay-v1\0";

const VALID_KIND_MASK: u8 = 0b0001_1111;
const CHUNK_FILE_IN_ORDER: u8 = 0;
const CHUNK_FILE_OUT_OF_ORDER: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalReplaySegmentOrder {
    /// The caller supplies each segment's zero-based manifest ordinal.
    Manifest,
    /// A deliberately manifestless corpus is ordered by raw segment-ID bytes.
    SegmentId,
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalReplayCorpusInput<'a> {
    pub segment_order: LogicalReplaySegmentOrder,
    pub segments: &'a [LogicalReplaySegmentInput<'a>],
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalReplaySegmentInput<'a> {
    /// Required and zero-based in manifest mode; forbidden in manifestless
    /// segment-ID mode. The ordinal validates order and is not encoded.
    pub manifest_ordinal: Option<u32>,
    pub segment_id: &'a [u8],
    pub start_ms: u64,
    pub end_ms: u64,
    pub series: &'a [LogicalReplaySeriesInput<'a>],
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalReplaySeriesInput<'a> {
    pub series_ref: u32,
    pub series_id: u64,
    pub kind_mask: u8,
    pub labels: &'a [LogicalReplayLabelInput<'a>],
    pub chunks: &'a [LogicalReplayChunkInput<'a>],
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalReplayLabelInput<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalReplayChunkInput<'a> {
    /// `0` is `chunks.bin`; `1` is `ooo_chunks.bin`.
    pub file_id: u8,
    pub kind: ChunkKind,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    /// Exact bytes addressed by the chunk locator, excluding frame bytes.
    pub exact_indexed_bytes: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogicalReplayFingerprint([u8; 32]);

impl LogicalReplayFingerprint {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }
}

impl fmt::Display for LogicalReplayFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LogicalReplayFingerprintError {
    #[error("{field} count exceeds u32: {count}")]
    CountOverflow { field: &'static str, count: usize },
    #[error("{field} byte length exceeds u32: {length}")]
    ByteLengthOverflow { field: &'static str, length: usize },
    #[error("failed to reserve canonical {field} working memory")]
    AllocationFailed { field: &'static str },
    #[error("segment {segment_index} has an empty segment ID")]
    EmptySegmentId { segment_index: usize },
    #[error(
        "segment manifest order is not canonical at index {segment_index}: expected ordinal {expected} got {actual:?}"
    )]
    ManifestOrder {
        segment_index: usize,
        expected: u32,
        actual: Option<u32>,
    },
    #[error("manifestless segment {segment_index} unexpectedly has manifest ordinal {ordinal}")]
    ManifestOrdinalInSegmentIdMode { segment_index: usize, ordinal: u32 },
    #[error("manifestless segment IDs are not strictly increasing at index {segment_index}")]
    SegmentIdOrder { segment_index: usize },
    #[error("duplicate segment ID at index {segment_index}")]
    DuplicateSegmentId { segment_index: usize },
    #[error("invalid segment bounds at index {segment_index}: start_ms={start_ms} end_ms={end_ms}")]
    InvalidSegmentBounds {
        segment_index: usize,
        start_ms: u64,
        end_ms: u64,
    },
    #[error(
        "series refs are not dense at segment {segment_index} series index {series_index}: expected {expected} got {actual}"
    )]
    NonDenseSeriesRef {
        segment_index: usize,
        series_index: usize,
        expected: u32,
        actual: u32,
    },
    #[error("invalid kind mask for series_ref {series_ref}: {kind_mask:#010b}")]
    InvalidKindMask { series_ref: u32, kind_mask: u8 },
    #[error(
        "label keys are not strictly increasing for series_ref {series_ref} at label index {label_index}"
    )]
    LabelKeyOrder { series_ref: u32, label_index: usize },
    #[error("series_ref {series_ref} has no chunks")]
    EmptyChunkList { series_ref: u32 },
    #[error("invalid chunk file_id for series_ref {series_ref} chunk {chunk_index}: {file_id}")]
    InvalidChunkFileId {
        series_ref: u32,
        chunk_index: usize,
        file_id: u8,
    },
    #[error(
        "invalid chunk time range for series_ref {series_ref} chunk {chunk_index}: min={min_time_ms} max={max_time_ms} segment=[{segment_start_ms},{segment_end_ms})"
    )]
    InvalidChunkTimeRange {
        series_ref: u32,
        chunk_index: usize,
        min_time_ms: u64,
        max_time_ms: u64,
        segment_start_ms: u64,
        segment_end_ms: u64,
    },
    #[error(
        "chunk bytes are shorter than the 40-byte indexed header for series_ref {series_ref} chunk {chunk_index}: {length}"
    )]
    ChunkTooShort {
        series_ref: u32,
        chunk_index: usize,
        length: usize,
    },
    #[error(
        "kind mask disagrees with chunk kinds for series_ref {series_ref}: stored={kind_mask:#010b} observed={observed:#010b}"
    )]
    KindMaskMismatch {
        series_ref: u32,
        kind_mask: u8,
        observed: u8,
    },
    #[error(
        "canonical chunk sort key is ambiguous for series_ref {series_ref} at canonical index {chunk_index}"
    )]
    AmbiguousChunkSortKey { series_ref: u32, chunk_index: usize },
}

#[derive(Debug, Error)]
pub enum LogicalReplayWriteError {
    #[error(transparent)]
    Fingerprint(#[from] LogicalReplayFingerprintError),
    #[error("failed to write logical replay identity")]
    Write(#[source] io::Error),
}

/// Computes the incremental SHA-256 identity without materializing the stream.
pub fn logical_replay_fingerprint_sha256(
    input: LogicalReplayCorpusInput<'_>,
) -> Result<LogicalReplayFingerprint, LogicalReplayFingerprintError> {
    let mut writer = DigestWriter(Sha256::new());
    match write_logical_replay_identity(&mut writer, input) {
        Ok(()) => Ok(LogicalReplayFingerprint(writer.0.finalize().into())),
        Err(LogicalReplayWriteError::Fingerprint(error)) => Err(error),
        Err(LogicalReplayWriteError::Write(_)) => {
            unreachable!("SHA-256 digest writer is infallible")
        }
    }
}

/// Writes the exact canonical identity stream to `writer`.
///
/// Validation is incremental, so a validation or I/O error may leave a partial
/// stream in the destination. Callers publishing bytes should use a disposable
/// buffer or temporary file. Fingerprint callers should use
/// [`logical_replay_fingerprint_sha256`] instead.
pub fn write_logical_replay_identity<W: Write>(
    writer: &mut W,
    input: LogicalReplayCorpusInput<'_>,
) -> Result<(), LogicalReplayWriteError> {
    write_all(writer, LOGICAL_REPLAY_FINGERPRINT_DOMAIN)?;
    write_u32(writer, checked_count("segment", input.segments.len())?)?;

    let mut seen_segment_ids = HashSet::new();
    seen_segment_ids
        .try_reserve(input.segments.len())
        .map_err(|_| LogicalReplayFingerprintError::AllocationFailed {
            field: "segment ID set",
        })?;
    let mut previous_segment_id: Option<&[u8]> = None;

    for (segment_index, segment) in input.segments.iter().enumerate() {
        validate_segment_order(
            input.segment_order,
            segment_index,
            segment,
            previous_segment_id,
        )?;
        if segment.segment_id.is_empty() {
            return Err(LogicalReplayFingerprintError::EmptySegmentId { segment_index }.into());
        }
        if !seen_segment_ids.insert(segment.segment_id) {
            return Err(LogicalReplayFingerprintError::DuplicateSegmentId { segment_index }.into());
        }
        if segment.start_ms >= segment.end_ms {
            return Err(LogicalReplayFingerprintError::InvalidSegmentBounds {
                segment_index,
                start_ms: segment.start_ms,
                end_ms: segment.end_ms,
            }
            .into());
        }

        write_bytes(writer, "segment ID", segment.segment_id)?;
        write_u64(writer, segment.start_ms)?;
        write_u64(writer, segment.end_ms)?;
        write_u32(writer, checked_count("series", segment.series.len())?)?;

        for (series_index, series) in segment.series.iter().enumerate() {
            let expected = u32::try_from(series_index).map_err(|_| {
                LogicalReplayFingerprintError::CountOverflow {
                    field: "series index",
                    count: series_index,
                }
            })?;
            if series.series_ref != expected {
                return Err(LogicalReplayFingerprintError::NonDenseSeriesRef {
                    segment_index,
                    series_index,
                    expected,
                    actual: series.series_ref,
                }
                .into());
            }
            validate_series(writer, segment, series)?;
        }

        previous_segment_id = Some(segment.segment_id);
    }
    Ok(())
}

fn validate_segment_order(
    order: LogicalReplaySegmentOrder,
    segment_index: usize,
    segment: &LogicalReplaySegmentInput<'_>,
    previous_segment_id: Option<&[u8]>,
) -> Result<(), LogicalReplayFingerprintError> {
    match order {
        LogicalReplaySegmentOrder::Manifest => {
            let expected = u32::try_from(segment_index).map_err(|_| {
                LogicalReplayFingerprintError::CountOverflow {
                    field: "manifest ordinal",
                    count: segment_index,
                }
            })?;
            if segment.manifest_ordinal != Some(expected) {
                return Err(LogicalReplayFingerprintError::ManifestOrder {
                    segment_index,
                    expected,
                    actual: segment.manifest_ordinal,
                });
            }
        }
        LogicalReplaySegmentOrder::SegmentId => {
            if let Some(ordinal) = segment.manifest_ordinal {
                return Err(
                    LogicalReplayFingerprintError::ManifestOrdinalInSegmentIdMode {
                        segment_index,
                        ordinal,
                    },
                );
            }
            if previous_segment_id.is_some_and(|previous| previous >= segment.segment_id) {
                return Err(LogicalReplayFingerprintError::SegmentIdOrder { segment_index });
            }
        }
    }
    Ok(())
}

fn validate_series<W: Write>(
    writer: &mut W,
    segment: &LogicalReplaySegmentInput<'_>,
    series: &LogicalReplaySeriesInput<'_>,
) -> Result<(), LogicalReplayWriteError> {
    if series.kind_mask == 0 || series.kind_mask & !VALID_KIND_MASK != 0 {
        return Err(LogicalReplayFingerprintError::InvalidKindMask {
            series_ref: series.series_ref,
            kind_mask: series.kind_mask,
        }
        .into());
    }

    write_u32(writer, series.series_ref)?;
    write_u64(writer, series.series_id)?;
    write_all(writer, &[series.kind_mask])?;
    write_u32(writer, checked_count("label", series.labels.len())?)?;

    let mut previous_key: Option<&[u8]> = None;
    for (label_index, label) in series.labels.iter().enumerate() {
        if previous_key.is_some_and(|previous| previous >= label.key) {
            return Err(LogicalReplayFingerprintError::LabelKeyOrder {
                series_ref: series.series_ref,
                label_index,
            }
            .into());
        }
        write_bytes(writer, "label key", label.key)?;
        write_bytes(writer, "label value", label.value)?;
        previous_key = Some(label.key);
    }

    if series.chunks.is_empty() {
        return Err(LogicalReplayFingerprintError::EmptyChunkList {
            series_ref: series.series_ref,
        }
        .into());
    }
    write_u32(writer, checked_count("chunk", series.chunks.len())?)?;
    let chunks = canonical_chunks(segment, series)?;
    for chunk in chunks {
        write_all(writer, &[chunk.file_id, chunk.kind])?;
        write_u64(writer, chunk.min_time_ms)?;
        write_u64(writer, chunk.max_time_ms)?;
        write_u32(writer, chunk.length)?;
        write_all(writer, &chunk.digest)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalChunk {
    file_id: u8,
    kind: u8,
    min_time_ms: u64,
    max_time_ms: u64,
    length: u32,
    digest: [u8; 32],
}

impl CanonicalChunk {
    fn sort_cmp(&self, other: &Self) -> Ordering {
        self.file_id
            .cmp(&other.file_id)
            .then_with(|| self.min_time_ms.cmp(&other.min_time_ms))
            .then_with(|| self.max_time_ms.cmp(&other.max_time_ms))
            .then_with(|| self.kind.cmp(&other.kind))
            .then_with(|| self.digest.cmp(&other.digest))
    }

    fn same_sort_key(&self, other: &Self) -> bool {
        self.file_id == other.file_id
            && self.min_time_ms == other.min_time_ms
            && self.max_time_ms == other.max_time_ms
            && self.kind == other.kind
            && self.digest == other.digest
    }
}

fn canonical_chunks(
    segment: &LogicalReplaySegmentInput<'_>,
    series: &LogicalReplaySeriesInput<'_>,
) -> Result<Vec<CanonicalChunk>, LogicalReplayFingerprintError> {
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(series.chunks.len())
        .map_err(|_| LogicalReplayFingerprintError::AllocationFailed {
            field: "chunk summaries",
        })?;
    let mut observed_kind_mask = 0u8;

    for (chunk_index, chunk) in series.chunks.iter().enumerate() {
        if !matches!(chunk.file_id, CHUNK_FILE_IN_ORDER | CHUNK_FILE_OUT_OF_ORDER) {
            return Err(LogicalReplayFingerprintError::InvalidChunkFileId {
                series_ref: series.series_ref,
                chunk_index,
                file_id: chunk.file_id,
            });
        }
        if chunk.min_time_ms < segment.start_ms
            || chunk.min_time_ms > chunk.max_time_ms
            || chunk.max_time_ms >= segment.end_ms
        {
            return Err(LogicalReplayFingerprintError::InvalidChunkTimeRange {
                series_ref: series.series_ref,
                chunk_index,
                min_time_ms: chunk.min_time_ms,
                max_time_ms: chunk.max_time_ms,
                segment_start_ms: segment.start_ms,
                segment_end_ms: segment.end_ms,
            });
        }
        if chunk.exact_indexed_bytes.len() < 40 {
            return Err(LogicalReplayFingerprintError::ChunkTooShort {
                series_ref: series.series_ref,
                chunk_index,
                length: chunk.exact_indexed_bytes.len(),
            });
        }
        let length = checked_byte_length("indexed chunk", chunk.exact_indexed_bytes.len())?;
        let kind = chunk_kind_id(chunk.kind);
        observed_kind_mask |= 1u8 << kind;
        canonical.push(CanonicalChunk {
            file_id: chunk.file_id,
            kind,
            min_time_ms: chunk.min_time_ms,
            max_time_ms: chunk.max_time_ms,
            length,
            digest: Sha256::digest(chunk.exact_indexed_bytes).into(),
        });
    }
    if observed_kind_mask != series.kind_mask {
        return Err(LogicalReplayFingerprintError::KindMaskMismatch {
            series_ref: series.series_ref,
            kind_mask: series.kind_mask,
            observed: observed_kind_mask,
        });
    }

    canonical.sort_unstable_by(CanonicalChunk::sort_cmp);
    for (index, pair) in canonical.windows(2).enumerate() {
        if pair[0].same_sort_key(&pair[1]) && pair[0].length != pair[1].length {
            return Err(LogicalReplayFingerprintError::AmbiguousChunkSortKey {
                series_ref: series.series_ref,
                chunk_index: index + 1,
            });
        }
    }
    Ok(canonical)
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

fn checked_count(field: &'static str, count: usize) -> Result<u32, LogicalReplayFingerprintError> {
    u32::try_from(count).map_err(|_| LogicalReplayFingerprintError::CountOverflow { field, count })
}

fn checked_byte_length(
    field: &'static str,
    length: usize,
) -> Result<u32, LogicalReplayFingerprintError> {
    u32::try_from(length)
        .map_err(|_| LogicalReplayFingerprintError::ByteLengthOverflow { field, length })
}

fn write_bytes<W: Write>(
    writer: &mut W,
    field: &'static str,
    bytes: &[u8],
) -> Result<(), LogicalReplayWriteError> {
    write_u32(writer, checked_byte_length(field, bytes.len())?)?;
    write_all(writer, bytes)
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), LogicalReplayWriteError> {
    write_all(writer, &value.to_le_bytes())
}

fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), LogicalReplayWriteError> {
    write_all(writer, &value.to_le_bytes())
}

fn write_all<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<(), LogicalReplayWriteError> {
    writer
        .write_all(bytes)
        .map_err(LogicalReplayWriteError::Write)
}

struct DigestWriter(Sha256);

impl Write for DigestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK_A: [u8; 40] = [0x11; 40];
    const CHUNK_B: [u8; 56] = [0x22; 56];
    const CHUNK_C: [u8; 48] = [0x33; 48];

    fn label<'a>(key: &'a [u8], value: &'a [u8]) -> LogicalReplayLabelInput<'a> {
        LogicalReplayLabelInput { key, value }
    }

    fn chunk<'a>(
        file_id: u8,
        kind: ChunkKind,
        min_time_ms: u64,
        max_time_ms: u64,
        bytes: &'a [u8],
    ) -> LogicalReplayChunkInput<'a> {
        LogicalReplayChunkInput {
            file_id,
            kind,
            min_time_ms,
            max_time_ms,
            exact_indexed_bytes: bytes,
        }
    }

    fn fingerprint_one_chunk(
        file_id: u8,
        kind: ChunkKind,
        kind_mask: u8,
        bytes: &[u8],
    ) -> Result<LogicalReplayFingerprint, LogicalReplayFingerprintError> {
        let chunks = [chunk(file_id, kind, 100, 150, bytes)];
        let labels = [label(b"__name__", b"requests")];
        let series = [LogicalReplaySeriesInput {
            series_ref: 0,
            series_id: 9,
            kind_mask,
            labels: &labels,
            chunks: &chunks,
        }];
        let segments = [LogicalReplaySegmentInput {
            manifest_ordinal: Some(0),
            segment_id: b"seg-a",
            start_ms: 100,
            end_ms: 200,
            series: &series,
        }];
        logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &segments,
        })
    }

    #[test]
    fn stable_golden_digest_and_exact_stream() {
        let chunks = [
            chunk(1, ChunkKind::Histogram, 140, 160, &CHUNK_B),
            chunk(0, ChunkKind::Float, 100, 120, &CHUNK_A),
        ];
        let labels = [label(b"__name__", b"requests"), label(b"service", b"edge")];
        let series = [LogicalReplaySeriesInput {
            series_ref: 0,
            series_id: 0x0102_0304_0506_0708,
            kind_mask: 0b0000_0101,
            labels: &labels,
            chunks: &chunks,
        }];
        let segments = [LogicalReplaySegmentInput {
            manifest_ordinal: Some(0),
            segment_id: b"seg-z",
            start_ms: 100,
            end_ms: 200,
            series: &series,
        }];
        let input = LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &segments,
        };

        let mut encoded = Vec::new();
        write_logical_replay_identity(&mut encoded, input).unwrap();

        // Assemble the specified stream independently of the production
        // encoder so the fixed digest cannot conceal a field-order bug.
        let mut expected = Vec::new();
        expected.extend_from_slice(b"chronoxide-logical-replay-v1\0");
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(&5u32.to_le_bytes());
        expected.extend_from_slice(b"seg-z");
        expected.extend_from_slice(&100u64.to_le_bytes());
        expected.extend_from_slice(&200u64.to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(&0u32.to_le_bytes());
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        expected.push(0b0000_0101);
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&8u32.to_le_bytes());
        expected.extend_from_slice(b"__name__");
        expected.extend_from_slice(&8u32.to_le_bytes());
        expected.extend_from_slice(b"requests");
        expected.extend_from_slice(&7u32.to_le_bytes());
        expected.extend_from_slice(b"service");
        expected.extend_from_slice(&4u32.to_le_bytes());
        expected.extend_from_slice(b"edge");
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&[0, 0]);
        expected.extend_from_slice(&100u64.to_le_bytes());
        expected.extend_from_slice(&120u64.to_le_bytes());
        expected.extend_from_slice(&40u32.to_le_bytes());
        expected.extend_from_slice(&<[u8; 32]>::from(Sha256::digest(CHUNK_A)));
        expected.extend_from_slice(&[1, 2]);
        expected.extend_from_slice(&140u64.to_le_bytes());
        expected.extend_from_slice(&160u64.to_le_bytes());
        expected.extend_from_slice(&56u32.to_le_bytes());
        expected.extend_from_slice(&<[u8; 32]>::from(Sha256::digest(CHUNK_B)));

        assert_eq!(encoded, expected);
        let fingerprint = logical_replay_fingerprint_sha256(input).unwrap();
        assert_eq!(
            fingerprint.as_bytes(),
            &<[u8; 32]>::from(Sha256::digest(expected))
        );
        assert_eq!(
            fingerprint.to_hex(),
            "036929bb188cfe016899963bf08c0d9cc5db04d42745cc335b475149cb438e5c"
        );
    }

    #[test]
    fn chunk_input_order_is_canonicalized_by_the_exact_contract_tuple() {
        let left_chunks = [
            chunk(1, ChunkKind::Histogram, 140, 160, &CHUNK_B),
            chunk(0, ChunkKind::Float, 100, 120, &CHUNK_A),
        ];
        let right_chunks = [left_chunks[1], left_chunks[0]];
        let labels = [label(b"__name__", b"requests")];
        let left_series = [LogicalReplaySeriesInput {
            series_ref: 0,
            series_id: 1,
            kind_mask: 0b0101,
            labels: &labels,
            chunks: &left_chunks,
        }];
        let right_series = [LogicalReplaySeriesInput {
            chunks: &right_chunks,
            ..left_series[0]
        }];
        let left_segments = [LogicalReplaySegmentInput {
            manifest_ordinal: Some(0),
            segment_id: b"seg",
            start_ms: 100,
            end_ms: 200,
            series: &left_series,
        }];
        let right_segments = [LogicalReplaySegmentInput {
            series: &right_series,
            ..left_segments[0]
        }];

        let left = logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &left_segments,
        })
        .unwrap();
        let right = logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &right_segments,
        })
        .unwrap();
        assert_eq!(left, right);
    }

    #[test]
    fn segment_series_and_label_order_are_rejected_instead_of_sorted() {
        let chunks = [chunk(0, ChunkKind::Float, 100, 120, &CHUNK_A)];
        let bad_labels = [label(b"z", b"1"), label(b"a", b"2")];
        let bad_series = [LogicalReplaySeriesInput {
            series_ref: 1,
            series_id: 1,
            kind_mask: 1,
            labels: &bad_labels,
            chunks: &chunks,
        }];
        let bad_segments = [LogicalReplaySegmentInput {
            manifest_ordinal: Some(1),
            segment_id: b"seg-b",
            start_ms: 100,
            end_ms: 200,
            series: &bad_series,
        }];
        let error = logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &bad_segments,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            LogicalReplayFingerprintError::ManifestOrder { .. }
        ));

        let dense_segment = [LogicalReplaySegmentInput {
            manifest_ordinal: Some(0),
            segment_id: b"seg",
            start_ms: 100,
            end_ms: 200,
            series: &bad_series,
        }];
        let error = logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &dense_segment,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            LogicalReplayFingerprintError::NonDenseSeriesRef { .. }
        ));

        let ordered_series = [LogicalReplaySeriesInput {
            series_ref: 0,
            ..bad_series[0]
        }];
        let label_segment = [LogicalReplaySegmentInput {
            series: &ordered_series,
            ..dense_segment[0]
        }];
        let error = logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &label_segment,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            LogicalReplayFingerprintError::LabelKeyOrder { label_index: 1, .. }
        ));
    }

    #[test]
    fn manifestless_order_is_raw_segment_id_bytes_independent_of_time_order() {
        let ascending_ids_descending_times = [
            LogicalReplaySegmentInput {
                manifest_ordinal: None,
                segment_id: b"seg-a",
                start_ms: 200,
                end_ms: 300,
                series: &[],
            },
            LogicalReplaySegmentInput {
                manifest_ordinal: None,
                segment_id: b"seg-z",
                start_ms: 100,
                end_ms: 200,
                series: &[],
            },
        ];
        logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::SegmentId,
            segments: &ascending_ids_descending_times,
        })
        .unwrap();

        let descending_ids_ascending_times = [
            LogicalReplaySegmentInput {
                manifest_ordinal: None,
                segment_id: b"seg-z",
                start_ms: 100,
                end_ms: 200,
                series: &[],
            },
            LogicalReplaySegmentInput {
                manifest_ordinal: None,
                segment_id: b"seg-a",
                start_ms: 200,
                end_ms: 300,
                series: &[],
            },
        ];
        let error = logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::SegmentId,
            segments: &descending_ids_ascending_times,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            LogicalReplayFingerprintError::SegmentIdOrder { segment_index: 1 }
        ));
    }

    #[test]
    fn routing_kind_and_exact_indexed_bytes_change_the_identity() {
        let base = fingerprint_one_chunk(0, ChunkKind::Float, 1, &CHUNK_A).unwrap();
        let routed = fingerprint_one_chunk(1, ChunkKind::Float, 1, &CHUNK_A).unwrap();
        let kind = fingerprint_one_chunk(0, ChunkKind::Int64, 2, &CHUNK_A).unwrap();
        let bytes = fingerprint_one_chunk(0, ChunkKind::Float, 1, &CHUNK_C).unwrap();
        assert_ne!(base, routed);
        assert_ne!(base, kind);
        assert_ne!(base, bytes);
    }

    #[test]
    fn invalid_routing_times_and_kind_mask_are_rejected() {
        let invalid_file = fingerprint_one_chunk(2, ChunkKind::Float, 1, &CHUNK_A).unwrap_err();
        assert!(matches!(
            invalid_file,
            LogicalReplayFingerprintError::InvalidChunkFileId { file_id: 2, .. }
        ));

        let mismatch = fingerprint_one_chunk(0, ChunkKind::Float, 2, &CHUNK_A).unwrap_err();
        assert!(matches!(
            mismatch,
            LogicalReplayFingerprintError::KindMaskMismatch { .. }
        ));

        let chunks = [chunk(0, ChunkKind::Float, 99, 120, &CHUNK_A)];
        let labels = [label(b"__name__", b"requests")];
        let series = [LogicalReplaySeriesInput {
            series_ref: 0,
            series_id: 1,
            kind_mask: 1,
            labels: &labels,
            chunks: &chunks,
        }];
        let segments = [LogicalReplaySegmentInput {
            manifest_ordinal: Some(0),
            segment_id: b"seg",
            start_ms: 100,
            end_ms: 200,
            series: &series,
        }];
        let error = logical_replay_fingerprint_sha256(LogicalReplayCorpusInput {
            segment_order: LogicalReplaySegmentOrder::Manifest,
            segments: &segments,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            LogicalReplayFingerprintError::InvalidChunkTimeRange { .. }
        ));
    }

    #[test]
    fn checked_counts_and_byte_lengths_reject_u32_overflow() {
        let overflow = usize::try_from(u64::from(u32::MAX) + 1).unwrap();
        assert_eq!(
            checked_count("series", overflow),
            Err(LogicalReplayFingerprintError::CountOverflow {
                field: "series",
                count: overflow,
            })
        );
        assert_eq!(
            checked_byte_length("label value", overflow),
            Err(LogicalReplayFingerprintError::ByteLengthOverflow {
                field: "label value",
                length: overflow,
            })
        );
    }

    #[test]
    fn short_exact_chunk_bytes_are_rejected_before_hashing() {
        let error = fingerprint_one_chunk(0, ChunkKind::Float, 1, &[0; 39]).unwrap_err();
        assert!(matches!(
            error,
            LogicalReplayFingerprintError::ChunkTooShort { length: 39, .. }
        ));
    }
}
