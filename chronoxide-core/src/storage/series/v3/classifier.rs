//! Pure schema-7 classification of final chunk locators.
//!
//! Classification happens only after chunk bytes and their external indexed-
//! prefix CRCs are final. Invalid locators are rejected before deciding
//! between the inline and overflow representations; overflow is reserved for
//! valid multi-chunk series and valid inline-width exceptions.

use std::io;

use crc32c::crc32c;

use crate::storage::chunk::{
    ChunkIndexEntry, ChunkKind, ChunkOverflowBlobLocatorV1, ChunkOverflowBlobV1,
    OverflowChunkEntryV1, Schema7ChunkPrefixExpectation, checked_chunk_overflow_blob_len,
    verify_schema7_indexed_prefix,
};

use super::{
    CHUNK_HEADER_LEN_V1, InlineChunkV3, OverflowChunksV3, SERIES_HOT_SCALAR_LANE_LEN_MAX,
    SeriesHotLocationV3, SeriesHotV3, SeriesHotV3Context,
};

const CHUNK_INDEX_ROOT_LEN_V2: u64 = 64;
const TYPED_SCALAR_LANE_HEADER_LEN_V1: u32 = 16;
const VALID_KIND_MASK: u8 = 0b1_1111;

/// One final schema-7 chunk locator and its exact raw indexed prefix.
///
/// `indexed_prefix` must be borrowed from the final chunk bytes at `entry`'s
/// file and offset, after any series-major rewrite. Classification computes
/// the external CRC and verifies the exact 40-byte chunk header plus the
/// 16-byte scalar header when present; callers cannot supply decoded header
/// facts or a CRC as proof.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FinalChunkIndexEntryV3<'a> {
    pub(crate) entry: &'a ChunkIndexEntry,
    pub(crate) indexed_prefix: &'a [u8],
}

/// All deterministic inputs needed to classify one final series.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SeriesClassifierInputV3<'a> {
    pub(crate) series_ref: u32,
    pub(crate) series_id: u64,
    pub(crate) keyset_id: u32,
    pub(crate) row: u32,
    pub(crate) kind_mask: u8,
    pub(crate) segment_start_ms: u64,
    pub(crate) segment_end_ms: u64,
    /// Exact footer-inventoried lengths for `chunks.bin` and `ooo_chunks.bin`.
    pub(crate) chunk_file_lens: [u64; 2],
    pub(crate) chunks: &'a [FinalChunkIndexEntryV3<'a>],
}

/// Canonical schema-7 result before overflow blob offsets are known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClassifiedSeriesV3 {
    Inline(SeriesHotV3),
    Overflow(PendingOverflowSeriesV3),
}

/// Identity retained while all overflow blobs are encoded in series-ref order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingOverflowSeriesV3 {
    pub(crate) series_id: u64,
    pub(crate) keyset_id: u32,
    pub(crate) row: u32,
    pub(crate) kind_mask: u8,
    pub(crate) blob: ChunkOverflowBlobV1,
}

impl PendingOverflowSeriesV3 {
    /// Binds the locator returned by the v2 overflow encoder to the hot record.
    ///
    /// This checks the encoder locator's series identity, count, exact length,
    /// and file bounds. The assembly caller must pair it with the locator for
    /// this series from the same complete v2 encoder output; canonical offset
    /// and root provenance are properties of that output, not this local bind.
    pub(crate) fn bind_blob_locator(
        &self,
        locator: ChunkOverflowBlobLocatorV1,
        chunk_index_file_len: u64,
    ) -> io::Result<SeriesHotV3> {
        let chunk_count = u32::try_from(self.blob.entries.len())
            .map_err(|_| invalid_input("schema-7 overflow chunk count exceeds u32"))?;
        let blob_len = checked_chunk_overflow_blob_len(chunk_count)
            .ok_or_else(|| invalid_input("schema-7 overflow blob length exceeds u32"))?;
        if locator.series_ref != self.blob.series_ref
            || locator.chunk_count != chunk_count
            || locator.blob_len != blob_len
        {
            return Err(invalid_input(
                "schema-7 overflow locator does not match classified series",
            ));
        }
        if locator.blob_offset < CHUNK_INDEX_ROOT_LEN_V2 {
            return Err(invalid_input(
                "schema-7 overflow locator overlaps the chunk-index root",
            ));
        }
        let blob_end = locator
            .blob_offset
            .checked_add(u64::from(locator.blob_len))
            .ok_or_else(|| invalid_input("schema-7 overflow locator range overflows"))?;
        if blob_end > chunk_index_file_len {
            return Err(invalid_input(
                "schema-7 overflow locator exceeds the chunk-index file",
            ));
        }

        Ok(SeriesHotV3 {
            series_id: self.series_id,
            keyset_id: self.keyset_id,
            row: self.row,
            kind_mask: self.kind_mask,
            location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                blob_offset: locator.blob_offset,
                blob_len: locator.blob_len,
                chunk_count: locator.chunk_count,
            }),
        })
    }
}

/// Classifies one series without performing I/O or changing input order.
pub(crate) fn classify_series_v3(
    input: SeriesClassifierInputV3<'_>,
) -> io::Result<ClassifiedSeriesV3> {
    validate_series_identity(&input)?;
    let chunk_count = u32::try_from(input.chunks.len())
        .map_err(|_| invalid_input("schema-7 overflow chunk count exceeds u32"))?;
    checked_chunk_overflow_blob_len(chunk_count)
        .ok_or_else(|| invalid_input("schema-7 overflow blob length exceeds u32"))?;

    let mut expected_kind_mask = 0u8;
    let mut previous_order_key = None;
    let single_chunk = input.chunks.len() == 1;
    let mut single_chunk_prefix_crc32c = 0;
    let mut overflow_entries = Vec::new();
    if !single_chunk {
        overflow_entries
            .try_reserve_exact(input.chunks.len())
            .map_err(|_| invalid_input("schema-7 overflow entry allocation failed"))?;
    }
    for chunk in input.chunks {
        let indexed_prefix_crc32c = validate_chunk_locator(&input, chunk)?;
        if single_chunk {
            single_chunk_prefix_crc32c = indexed_prefix_crc32c;
        } else {
            overflow_entries.push(overflow_entry(chunk, indexed_prefix_crc32c));
        }
        expected_kind_mask |= kind_bit(chunk.entry.kind);

        let order_key = entry_order_key(chunk.entry);
        if previous_order_key.is_some_and(|previous| order_key <= previous) {
            return Err(invalid_input(
                "schema-7 chunks are not strictly ordered and unique",
            ));
        }
        previous_order_key = Some(order_key);
    }
    if input.kind_mask != expected_kind_mask {
        return Err(invalid_input(
            "schema-7 series kind mask does not match its chunks",
        ));
    }

    if let [chunk] = input.chunks
        && let Some(inline) =
            inline_chunk(input.segment_start_ms, chunk, single_chunk_prefix_crc32c)
    {
        let record = SeriesHotV3 {
            series_id: input.series_id,
            keyset_id: input.keyset_id,
            row: input.row,
            kind_mask: input.kind_mask,
            location: SeriesHotLocationV3::Inline(inline),
        };
        record.validate(SeriesHotV3Context {
            segment_start_ms: input.segment_start_ms,
            segment_end_ms: input.segment_end_ms,
            chunk_file_lens: input.chunk_file_lens,
            chunk_index_file_len: CHUNK_INDEX_ROOT_LEN_V2,
        })?;
        return Ok(ClassifiedSeriesV3::Inline(record));
    }

    if single_chunk {
        overflow_entries
            .try_reserve_exact(1)
            .map_err(|_| invalid_input("schema-7 overflow entry allocation failed"))?;
        overflow_entries.push(overflow_entry(&input.chunks[0], single_chunk_prefix_crc32c));
    }
    Ok(ClassifiedSeriesV3::Overflow(PendingOverflowSeriesV3 {
        series_id: input.series_id,
        keyset_id: input.keyset_id,
        row: input.row,
        kind_mask: input.kind_mask,
        blob: ChunkOverflowBlobV1 {
            series_ref: input.series_ref,
            entries: overflow_entries,
        },
    }))
}

fn validate_series_identity(input: &SeriesClassifierInputV3<'_>) -> io::Result<()> {
    if input.segment_start_ms >= input.segment_end_ms {
        return Err(invalid_input("schema-7 segment bounds are invalid"));
    }
    if input.kind_mask == 0 || input.kind_mask & !VALID_KIND_MASK != 0 {
        return Err(invalid_input("schema-7 series kind mask is invalid"));
    }
    if input.chunks.is_empty() {
        return Err(invalid_input("schema-7 series has no chunks"));
    }
    Ok(())
}

fn validate_chunk_locator(
    input: &SeriesClassifierInputV3<'_>,
    chunk: &FinalChunkIndexEntryV3<'_>,
) -> io::Result<u32> {
    let entry = chunk.entry;
    if entry.file_id > 1 {
        return Err(invalid_input("schema-7 chunk file ID is invalid"));
    }
    if entry.min_time_ms < input.segment_start_ms
        || entry.min_time_ms > entry.max_time_ms
        || entry.max_time_ms >= input.segment_end_ms
    {
        return Err(invalid_input("schema-7 chunk time range is invalid"));
    }
    if entry.length < CHUNK_HEADER_LEN_V1 {
        return Err(invalid_input(
            "schema-7 chunk is shorter than its fixed header",
        ));
    }
    let chunk_end = entry
        .offset
        .checked_add(u64::from(entry.length))
        .ok_or_else(|| invalid_input("schema-7 chunk file range overflows"))?;
    if chunk_end > input.chunk_file_lens[usize::from(entry.file_id)] {
        return Err(invalid_input("schema-7 chunk file range is out of bounds"));
    }

    match (entry.scalar_lane_offset, entry.scalar_lane_len) {
        (0, 0) => {}
        (CHUNK_HEADER_LEN_V1, scalar_lane_len)
            if scalar_lane_len >= TYPED_SCALAR_LANE_HEADER_LEN_V1 =>
        {
            let scalar_end = entry
                .scalar_lane_offset
                .checked_add(entry.scalar_lane_len)
                .ok_or_else(|| invalid_input("schema-7 scalar lane range overflows"))?;
            if scalar_end > entry.length {
                return Err(invalid_input(
                    "schema-7 scalar lane exceeds the chunk range",
                ));
            }
        }
        _ => {
            return Err(invalid_input(
                "schema-7 scalar lane locator is not canonical",
            ));
        }
    }
    let indexed_prefix_crc32c = crc32c(chunk.indexed_prefix);
    let verified = verify_schema7_indexed_prefix(
        &Schema7ChunkPrefixExpectation {
            series_ref: input.series_ref,
            kind: entry.kind,
            min_time_ms: entry.min_time_ms,
            max_time_ms: entry.max_time_ms,
            length: entry.length,
            scalar_lane_offset: entry.scalar_lane_offset,
            scalar_lane_len: entry.scalar_lane_len,
            indexed_prefix_crc32c,
        },
        chunk.indexed_prefix,
    )?;
    if verified.flags != entry.flags {
        return Err(invalid_input(
            "schema-7 chunk-index flags do not match the authenticated header",
        ));
    }
    Ok(indexed_prefix_crc32c)
}

fn inline_chunk(
    segment_start_ms: u64,
    chunk: &FinalChunkIndexEntryV3<'_>,
    indexed_prefix_crc32c: u32,
) -> Option<InlineChunkV3> {
    let entry = chunk.entry;
    if !chunk_entry_fits_inline(segment_start_ms, entry) {
        return None;
    }
    let min_time_delta_ms = u32::try_from(entry.min_time_ms - segment_start_ms).ok()?;
    let max_time_delta_ms = u32::try_from(entry.max_time_ms - segment_start_ms).ok()?;
    let file_offset = u32::try_from(entry.offset).ok()?;
    Some(InlineChunkV3 {
        chunk_kind: entry.kind as u8,
        file_id: entry.file_id,
        scalar_lane_len: entry.scalar_lane_len,
        min_time_delta_ms,
        max_time_delta_ms,
        file_offset,
        chunk_length: entry.length,
        indexed_prefix_crc32c,
    })
}

pub(super) fn chunk_entry_fits_inline(segment_start_ms: u64, entry: &ChunkIndexEntry) -> bool {
    entry
        .min_time_ms
        .checked_sub(segment_start_ms)
        .and_then(|delta| u32::try_from(delta).ok())
        .is_some()
        && entry
            .max_time_ms
            .checked_sub(segment_start_ms)
            .and_then(|delta| u32::try_from(delta).ok())
            .is_some()
        && u32::try_from(entry.offset).is_ok()
        && entry.scalar_lane_len <= SERIES_HOT_SCALAR_LANE_LEN_MAX
}

fn overflow_entry(
    chunk: &FinalChunkIndexEntryV3<'_>,
    indexed_prefix_crc32c: u32,
) -> OverflowChunkEntryV1 {
    OverflowChunkEntryV1 {
        file_id: chunk.entry.file_id,
        kind: chunk.entry.kind,
        min_time_ms: chunk.entry.min_time_ms,
        max_time_ms: chunk.entry.max_time_ms,
        offset: chunk.entry.offset,
        length: chunk.entry.length,
        scalar_lane_offset: chunk.entry.scalar_lane_offset,
        scalar_lane_len: chunk.entry.scalar_lane_len,
        indexed_prefix_crc32c,
    }
}

fn kind_bit(kind: ChunkKind) -> u8 {
    1u8 << kind as u8
}

fn entry_order_key(entry: &ChunkIndexEntry) -> (u8, u64, u64, u64) {
    (
        entry.file_id,
        entry.min_time_ms,
        entry.max_time_ms,
        entry.offset,
    )
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests;
