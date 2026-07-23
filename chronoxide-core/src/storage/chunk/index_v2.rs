use std::io;

use crc32c::{crc32c, crc32c_append};

use super::ChunkKind;

pub(crate) const CHUNK_OVERFLOW_ROOT_V2_LEN: usize = 64;
pub(crate) const CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN: usize = 32;
pub(crate) const OVERFLOW_CHUNK_ENTRY_V1_LEN: usize = 44;

const CHUNK_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"CHIX");
const CHUNK_INDEX_VERSION_V2: u16 = 2;
const CHUNK_OVERFLOW_BLOBS_OFFSET: u64 = CHUNK_OVERFLOW_ROOT_V2_LEN as u64;
const CHUNK_OVERFLOW_BLOB_MAGIC: u32 = u32::from_le_bytes(*b"COF7");
const CHUNK_OVERFLOW_BLOB_VERSION_V1: u16 = 1;
const CHUNK_HEADER_LEN: u32 = 40;
const TYPED_SCALAR_LANE_HEADER_LEN: u32 = 16;
const MIN_CHUNK_OVERFLOW_BLOB_V1_LEN: u64 =
    (CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN + OVERFLOW_CHUNK_ENTRY_V1_LEN) as u64;

const ROOT_CRC_OFFSET: usize = 56;
const BLOB_CRC_OFFSET: usize = 28;

/// The authenticated fixed root of `chunk_index.bin` v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkOverflowRootV2 {
    pub(crate) series_count: u32,
    pub(crate) blob_count: u32,
    pub(crate) blobs_len: u64,
    pub(crate) file_len: u64,
    pub(crate) root_crc32c: u32,
}

impl ChunkOverflowRootV2 {
    /// Logical allocation bytes owned by the fixed decoded root.
    pub(crate) fn charged_bytes(self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

/// One complete schema-7 overflow chunk locator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OverflowChunkEntryV1 {
    pub(crate) file_id: u8,
    pub(crate) kind: ChunkKind,
    pub(crate) min_time_ms: u64,
    pub(crate) max_time_ms: u64,
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) scalar_lane_offset: u32,
    pub(crate) scalar_lane_len: u32,
    pub(crate) indexed_prefix_crc32c: u32,
}

/// A decoded or not-yet-encoded overflow blob for one series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChunkOverflowBlobV1 {
    pub(crate) series_ref: u32,
    pub(crate) entries: Vec<OverflowChunkEntryV1>,
}

/// The exact locator that a schema-7 overflow hot record stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkOverflowBlobLocatorV1 {
    pub(crate) series_ref: u32,
    pub(crate) blob_offset: u64,
    pub(crate) blob_len: u32,
    pub(crate) chunk_count: u32,
}

/// Identity decoded from one intrinsically authenticated physical overflow
/// blob before it is rebound to a series hot record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TouchedOverflowBlobFactsV1 {
    pub(crate) series_ref: u32,
    pub(crate) chunk_count: u32,
}

/// Deterministic complete-file encoder output, including hot-record locators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedChunkIndexV2 {
    pub(crate) bytes: Vec<u8>,
    pub(crate) root: ChunkOverflowRootV2,
    pub(crate) blob_locators: Vec<ChunkOverflowBlobLocatorV1>,
}

/// Fully decoded v2 bytes. Production readers may instead decode the root and touched blobs only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedChunkIndexV2 {
    pub(crate) root: ChunkOverflowRootV2,
    pub(crate) blobs: Vec<ChunkOverflowBlobV1>,
    pub(crate) blob_locators: Vec<ChunkOverflowBlobLocatorV1>,
}

/// Returns the exact blob length when `32 + 44 * chunk_count` fits `u32`.
pub(crate) fn checked_chunk_overflow_blob_len(chunk_count: u32) -> Option<u32> {
    chunk_count
        .checked_mul(OVERFLOW_CHUNK_ENTRY_V1_LEN as u32)
        .and_then(|body_len| body_len.checked_add(CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32))
}

/// Encodes a complete overflow-only `chunk_index.bin` v2 without reordering input.
pub(crate) fn encode_chunk_index_v2(
    series_count: u32,
    blobs: &[ChunkOverflowBlobV1],
) -> io::Result<EncodedChunkIndexV2> {
    let blob_count = u32::try_from(blobs.len())
        .map_err(|_| invalid_input("schema-7 overflow blob count exceeds u32"))?;
    if blob_count > series_count {
        return Err(invalid_input(
            "schema-7 overflow blob count exceeds series count",
        ));
    }

    validate_blob_series_order(blobs, series_count, io::ErrorKind::InvalidInput)?;

    let mut encoded_blobs = Vec::with_capacity(blobs.len());
    let mut blobs_len = 0u64;
    for blob in blobs {
        let bytes = encode_chunk_overflow_blob_v1(blob, series_count)?;
        let encoded_len = u64::try_from(bytes.len())
            .map_err(|_| invalid_input("schema-7 overflow blob length exceeds u64"))?;
        blobs_len = blobs_len
            .checked_add(encoded_len)
            .ok_or_else(|| invalid_input("schema-7 overflow blob region length overflows"))?;
        encoded_blobs.push(bytes);
    }

    let file_len = CHUNK_OVERFLOW_BLOBS_OFFSET
        .checked_add(blobs_len)
        .ok_or_else(|| invalid_input("schema-7 chunk index file length overflows"))?;
    let capacity = usize::try_from(file_len)
        .map_err(|_| invalid_input("schema-7 chunk index cannot fit in memory"))?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.resize(CHUNK_OVERFLOW_ROOT_V2_LEN, 0);

    let mut blob_locators = Vec::with_capacity(encoded_blobs.len());
    let mut blob_offset = CHUNK_OVERFLOW_BLOBS_OFFSET;
    for (blob, encoded) in blobs.iter().zip(encoded_blobs) {
        let blob_len = u32::try_from(encoded.len())
            .map_err(|_| invalid_input("schema-7 overflow blob length exceeds u32"))?;
        let chunk_count = u32::try_from(blob.entries.len())
            .map_err(|_| invalid_input("schema-7 overflow chunk count exceeds u32"))?;
        blob_locators.push(ChunkOverflowBlobLocatorV1 {
            series_ref: blob.series_ref,
            blob_offset,
            blob_len,
            chunk_count,
        });
        bytes.extend_from_slice(&encoded);
        blob_offset = blob_offset
            .checked_add(u64::from(blob_len))
            .ok_or_else(|| invalid_input("schema-7 overflow blob offset overflows"))?;
    }
    if blob_offset != file_len {
        return Err(invalid_input(
            "schema-7 overflow blob region length is inconsistent",
        ));
    }

    let root_bytes = encode_chunk_overflow_root_v2(series_count, blob_count, blobs_len)?;
    bytes[..CHUNK_OVERFLOW_ROOT_V2_LEN].copy_from_slice(&root_bytes);
    let root = decode_chunk_overflow_root_v2(&bytes[..CHUNK_OVERFLOW_ROOT_V2_LEN], file_len)?;

    Ok(EncodedChunkIndexV2 {
        bytes,
        root,
        blob_locators,
    })
}

/// Decodes and validates the complete file, including global blob coverage and ordering.
pub(crate) fn decode_chunk_index_v2(bytes: &[u8]) -> io::Result<DecodedChunkIndexV2> {
    if bytes.len() < CHUNK_OVERFLOW_ROOT_V2_LEN {
        return Err(unexpected_eof("schema-7 chunk index root is truncated"));
    }
    let actual_file_len = u64::try_from(bytes.len())
        .map_err(|_| invalid_data("schema-7 chunk index file length exceeds u64"))?;
    let root =
        decode_chunk_overflow_root_v2(&bytes[..CHUNK_OVERFLOW_ROOT_V2_LEN], actual_file_len)?;

    let mut blobs = Vec::new();
    let mut blob_locators = Vec::new();
    let mut cursor = CHUNK_OVERFLOW_ROOT_V2_LEN;
    let mut previous_series_ref = None;
    for _ in 0..root.blob_count {
        let header_end = cursor
            .checked_add(CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN)
            .ok_or_else(|| invalid_data("schema-7 overflow blob header range overflows"))?;
        if header_end > bytes.len() {
            return Err(unexpected_eof("schema-7 overflow blob header is truncated"));
        }

        let chunk_count = read_u32_at(bytes, cursor + 16);
        let declared_body_len = read_u32_at(bytes, cursor + 24);
        let expected_blob_len = checked_chunk_overflow_blob_len(chunk_count)
            .ok_or_else(|| invalid_data("schema-7 overflow blob length exceeds u32"))?;
        let expected_body_len = expected_blob_len - CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32;
        if declared_body_len != expected_body_len {
            return Err(invalid_data(
                "schema-7 overflow blob body length does not match chunk count",
            ));
        }
        let blob_len = usize::try_from(expected_blob_len)
            .map_err(|_| invalid_data("schema-7 overflow blob cannot fit in memory"))?;
        let end = cursor
            .checked_add(blob_len)
            .ok_or_else(|| invalid_data("schema-7 overflow blob range overflows"))?;
        if end > bytes.len() {
            return Err(unexpected_eof("schema-7 overflow blob body is truncated"));
        }

        let blob = decode_chunk_overflow_blob_v1(&bytes[cursor..end], root.series_count)?;
        if previous_series_ref.is_some_and(|previous| blob.series_ref <= previous) {
            return Err(invalid_data(
                "schema-7 overflow blobs are not strictly ordered by series_ref",
            ));
        }
        previous_series_ref = Some(blob.series_ref);
        blob_locators.push(ChunkOverflowBlobLocatorV1 {
            series_ref: blob.series_ref,
            blob_offset: u64::try_from(cursor)
                .map_err(|_| invalid_data("schema-7 overflow blob offset exceeds u64"))?,
            blob_len: expected_blob_len,
            chunk_count,
        });
        blobs.push(blob);
        cursor = end;
    }

    if cursor != bytes.len() {
        return Err(invalid_data(
            "schema-7 overflow blobs do not cover the declared file",
        ));
    }
    let decoded_blobs_len = u64::try_from(cursor - CHUNK_OVERFLOW_ROOT_V2_LEN)
        .map_err(|_| invalid_data("schema-7 overflow blob region exceeds u64"))?;
    if decoded_blobs_len != root.blobs_len {
        return Err(invalid_data(
            "schema-7 overflow blob region length does not match root",
        ));
    }

    Ok(DecodedChunkIndexV2 {
        root,
        blobs,
        blob_locators,
    })
}

/// Decodes the fixed root using an independently obtained file length (normally `fstat`).
pub(crate) fn decode_chunk_overflow_root_v2(
    bytes: &[u8],
    actual_file_len: u64,
) -> io::Result<ChunkOverflowRootV2> {
    require_exact_len(
        bytes,
        CHUNK_OVERFLOW_ROOT_V2_LEN,
        "schema-7 chunk index root",
    )?;

    let stored_crc = read_u32_at(bytes, ROOT_CRC_OFFSET);
    let mut crc_bytes = [0u8; CHUNK_OVERFLOW_ROOT_V2_LEN];
    crc_bytes.copy_from_slice(bytes);
    crc_bytes[ROOT_CRC_OFFSET..ROOT_CRC_OFFSET + 4].fill(0);
    if crc32c(&crc_bytes) != stored_crc {
        return Err(invalid_data("schema-7 chunk index root crc mismatch"));
    }

    if read_u32_at(bytes, 0) != CHUNK_INDEX_MAGIC {
        return Err(invalid_data("schema-7 chunk index magic mismatch"));
    }
    if read_u16_at(bytes, 4) != CHUNK_INDEX_VERSION_V2 {
        return Err(invalid_data("unsupported schema-7 chunk index version"));
    }
    if read_u16_at(bytes, 6) != 0 {
        return Err(invalid_data("schema-7 chunk index flags must be zero"));
    }
    if read_u32_at(bytes, 8) != CHUNK_OVERFLOW_ROOT_V2_LEN as u32 {
        return Err(invalid_data(
            "schema-7 chunk index root length is not canonical",
        ));
    }
    if read_u32_at(bytes, 12) != CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32 {
        return Err(invalid_data(
            "schema-7 overflow blob header length is not canonical",
        ));
    }
    if read_u32_at(bytes, 16) != OVERFLOW_CHUNK_ENTRY_V1_LEN as u32 {
        return Err(invalid_data(
            "schema-7 overflow entry length is not canonical",
        ));
    }

    let series_count = read_u32_at(bytes, 20);
    let blob_count = read_u32_at(bytes, 24);
    if blob_count > series_count {
        return Err(invalid_data(
            "schema-7 overflow blob count exceeds series count",
        ));
    }
    if read_u32_at(bytes, 28) != 0 || read_u32_at(bytes, 60) != 0 {
        return Err(invalid_data(
            "schema-7 chunk index root reserved fields must be zero",
        ));
    }
    if read_u64_at(bytes, 32) != CHUNK_OVERFLOW_BLOBS_OFFSET {
        return Err(invalid_data(
            "schema-7 overflow blob region offset is not canonical",
        ));
    }

    let blobs_len = read_u64_at(bytes, 40);
    let file_len = read_u64_at(bytes, 48);
    let expected_file_len = CHUNK_OVERFLOW_BLOBS_OFFSET
        .checked_add(blobs_len)
        .ok_or_else(|| invalid_data("schema-7 chunk index file length overflows"))?;
    if file_len != expected_file_len || file_len != actual_file_len {
        return Err(invalid_data(
            "schema-7 chunk index file length does not match root",
        ));
    }
    if (blob_count == 0) != (blobs_len == 0) {
        return Err(invalid_data(
            "schema-7 overflow blob count and region length disagree",
        ));
    }
    let minimum_blobs_len = u64::from(blob_count)
        .checked_mul(MIN_CHUNK_OVERFLOW_BLOB_V1_LEN)
        .ok_or_else(|| invalid_data("schema-7 minimum overflow blob length overflows"))?;
    if blobs_len < minimum_blobs_len {
        return Err(invalid_data(
            "schema-7 overflow blob region is too short for its blob count",
        ));
    }

    Ok(ChunkOverflowRootV2 {
        series_count,
        blob_count,
        blobs_len,
        file_len,
        root_crc32c: stored_crc,
    })
}

/// Decodes a touched blob and cross-checks the exact locator from its series hot record.
pub(crate) fn decode_touched_chunk_overflow_blob_v1(
    bytes: &[u8],
    root: &ChunkOverflowRootV2,
    locator: ChunkOverflowBlobLocatorV1,
) -> io::Result<ChunkOverflowBlobV1> {
    let validated = validate_touched_chunk_overflow_blob_bytes_v1(bytes, root, locator)?;
    let entry_count = usize::try_from(validated.chunk_count)
        .map_err(|_| invalid_data("schema-7 overflow chunk count cannot fit in memory"))?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(entry_count)
        .map_err(|_| resource_error("schema-7 overflow entry allocation failed"))?;
    validated.visit_entries(|entry| {
        entries.push(entry);
        Ok(())
    })?;
    validate_touched_blob_identity(&validated, locator)?;
    Ok(ChunkOverflowBlobV1 {
        series_ref: validated.series_ref,
        entries,
    })
}

/// Authenticates and structurally validates one touched overflow blob without
/// allocating decoded entry storage.
pub(crate) fn visit_touched_chunk_overflow_blob_v1<F>(
    bytes: &[u8],
    root: &ChunkOverflowRootV2,
    locator: ChunkOverflowBlobLocatorV1,
    visit: F,
) -> io::Result<()>
where
    F: FnMut(OverflowChunkEntryV1) -> io::Result<()>,
{
    let validated = validate_touched_chunk_overflow_blob_bytes_v1(bytes, root, locator)?;
    validated.visit_entries(visit)?;
    validate_touched_blob_identity(&validated, locator)
}

/// Authenticates one physical blob range without trusting the series identity
/// or chunk count copied into a hot-record locator. The caller must perform
/// that cross-artifact binding after cache lookup so hits and misses have the
/// same result.
pub(crate) fn visit_physical_chunk_overflow_blob_v1<F>(
    bytes: &[u8],
    root: &ChunkOverflowRootV2,
    blob_offset: u64,
    mut visit: F,
) -> io::Result<TouchedOverflowBlobFactsV1>
where
    F: FnMut(TouchedOverflowBlobFactsV1, OverflowChunkEntryV1) -> io::Result<()>,
{
    if blob_offset < CHUNK_OVERFLOW_BLOBS_OFFSET {
        return Err(invalid_data(
            "schema-7 overflow blob starts before the blob region",
        ));
    }
    let blob_len = u64::try_from(bytes.len())
        .map_err(|_| invalid_data("schema-7 overflow blob length exceeds u64"))?;
    let blob_end = blob_offset
        .checked_add(blob_len)
        .ok_or_else(|| invalid_data("schema-7 overflow blob range overflows"))?;
    if blob_end > root.file_len {
        return Err(invalid_data(
            "schema-7 overflow blob exceeds the root blob region",
        ));
    }

    let validated = validate_chunk_overflow_blob_bytes_v1(bytes, root.series_count)?;
    let facts = TouchedOverflowBlobFactsV1 {
        series_ref: validated.series_ref,
        chunk_count: validated.chunk_count,
    };
    validated.visit_entries(|entry| visit(facts, entry))?;
    Ok(facts)
}

fn validate_touched_chunk_overflow_blob_bytes_v1<'a>(
    bytes: &'a [u8],
    root: &ChunkOverflowRootV2,
    locator: ChunkOverflowBlobLocatorV1,
) -> io::Result<ValidatedChunkOverflowBlobBytes<'a>> {
    if locator.blob_offset < CHUNK_OVERFLOW_BLOBS_OFFSET {
        return Err(invalid_data(
            "schema-7 overflow blob locator starts before the blob region",
        ));
    }
    let blob_end = locator
        .blob_offset
        .checked_add(u64::from(locator.blob_len))
        .ok_or_else(|| invalid_data("schema-7 overflow blob locator range overflows"))?;
    if blob_end > root.file_len {
        return Err(invalid_data(
            "schema-7 overflow blob locator exceeds the root blob region",
        ));
    }
    let expected_blob_len = checked_chunk_overflow_blob_len(locator.chunk_count)
        .ok_or_else(|| invalid_data("schema-7 overflow blob locator length exceeds u32"))?;
    if locator.blob_len != expected_blob_len {
        return Err(invalid_data(
            "schema-7 overflow blob locator length does not match chunk count",
        ));
    }
    let locator_blob_len = usize::try_from(locator.blob_len)
        .map_err(|_| invalid_data("schema-7 overflow blob locator cannot fit in memory"))?;
    require_exact_len(bytes, locator_blob_len, "schema-7 touched overflow blob")?;

    validate_chunk_overflow_blob_bytes_v1(bytes, root.series_count)
}

fn validate_touched_blob_identity(
    validated: &ValidatedChunkOverflowBlobBytes<'_>,
    locator: ChunkOverflowBlobLocatorV1,
) -> io::Result<()> {
    if validated.series_ref != locator.series_ref {
        return Err(invalid_data(
            "schema-7 overflow blob series_ref does not match hot record",
        ));
    }
    if validated.chunk_count != locator.chunk_count {
        return Err(invalid_data(
            "schema-7 overflow blob chunk count does not match hot record",
        ));
    }
    Ok(())
}

/// Decodes one exact structural blob. Segment and chunk-file bounds are integration checks.
fn decode_chunk_overflow_blob_v1(
    bytes: &[u8],
    series_count: u32,
) -> io::Result<ChunkOverflowBlobV1> {
    let validated = validate_chunk_overflow_blob_bytes_v1(bytes, series_count)?;
    let entry_count = usize::try_from(validated.chunk_count)
        .map_err(|_| invalid_data("schema-7 overflow chunk count cannot fit in memory"))?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(entry_count)
        .map_err(|_| resource_error("schema-7 overflow entry allocation failed"))?;
    validated.visit_entries(|entry| {
        entries.push(entry);
        Ok(())
    })?;
    Ok(ChunkOverflowBlobV1 {
        series_ref: validated.series_ref,
        entries,
    })
}

struct ValidatedChunkOverflowBlobBytes<'a> {
    series_ref: u32,
    chunk_count: u32,
    encoded_entries: &'a [u8],
}

impl ValidatedChunkOverflowBlobBytes<'_> {
    fn visit_entries<F>(&self, mut visit: F) -> io::Result<()>
    where
        F: FnMut(OverflowChunkEntryV1) -> io::Result<()>,
    {
        let mut entry_count = 0u32;
        let mut previous_order_key = None;
        for entry_bytes in self
            .encoded_entries
            .chunks_exact(OVERFLOW_CHUNK_ENTRY_V1_LEN)
        {
            let entry = decode_overflow_chunk_entry_v1(entry_bytes)?;
            let order_key = overflow_entry_order_key(&entry);
            if previous_order_key.is_some_and(|previous| order_key <= previous) {
                return Err(invalid_data(
                    "schema-7 overflow entries are not strictly ordered and unique",
                ));
            }
            previous_order_key = Some(order_key);
            entry_count = entry_count
                .checked_add(1)
                .ok_or_else(|| invalid_data("schema-7 overflow entry count overflows"))?;
            visit(entry)?;
        }
        if entry_count != self.chunk_count {
            return Err(invalid_data(
                "schema-7 overflow entry count does not match blob header",
            ));
        }
        Ok(())
    }
}

fn validate_chunk_overflow_blob_bytes_v1<'a>(
    bytes: &'a [u8],
    series_count: u32,
) -> io::Result<ValidatedChunkOverflowBlobBytes<'a>> {
    if bytes.len() < CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN {
        return Err(unexpected_eof("schema-7 overflow blob header is truncated"));
    }

    let chunk_count = read_u32_at(bytes, 16);
    if chunk_count == 0 {
        return Err(invalid_data("schema-7 overflow blob has no chunks"));
    }
    let expected_blob_len_u32 = checked_chunk_overflow_blob_len(chunk_count)
        .ok_or_else(|| invalid_data("schema-7 overflow blob length exceeds u32"))?;
    let expected_body_len_u32 = expected_blob_len_u32
        .checked_sub(CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32)
        .ok_or_else(|| invalid_data("schema-7 overflow blob body length underflows"))?;
    let expected_blob_len = usize::try_from(expected_blob_len_u32)
        .map_err(|_| invalid_data("schema-7 overflow blob cannot fit in memory"))?;
    require_exact_len(bytes, expected_blob_len, "schema-7 overflow blob")?;

    if read_u32_at(bytes, 24) != expected_body_len_u32 {
        return Err(invalid_data(
            "schema-7 overflow blob body length does not match chunk count",
        ));
    }

    let stored_crc = read_u32_at(bytes, BLOB_CRC_OFFSET);
    let mut crc_header = [0u8; CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN];
    crc_header.copy_from_slice(&bytes[..CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN]);
    crc_header[BLOB_CRC_OFFSET..BLOB_CRC_OFFSET + 4].fill(0);
    let computed_crc = crc32c_append(
        crc32c(&crc_header),
        &bytes[CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN..],
    );
    if computed_crc != stored_crc {
        return Err(invalid_data("schema-7 overflow blob crc mismatch"));
    }

    if read_u32_at(bytes, 0) != CHUNK_OVERFLOW_BLOB_MAGIC {
        return Err(invalid_data("schema-7 overflow blob magic mismatch"));
    }
    if read_u16_at(bytes, 4) != CHUNK_OVERFLOW_BLOB_VERSION_V1 {
        return Err(invalid_data("unsupported schema-7 overflow blob version"));
    }
    if read_u16_at(bytes, 6) != 0 {
        return Err(invalid_data("schema-7 overflow blob flags must be zero"));
    }
    if read_u32_at(bytes, 8) != CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32 {
        return Err(invalid_data(
            "schema-7 overflow blob header length is not canonical",
        ));
    }
    let series_ref = read_u32_at(bytes, 12);
    if series_ref >= series_count {
        return Err(invalid_data(
            "schema-7 overflow blob series_ref is out of range",
        ));
    }
    if read_u32_at(bytes, 20) != 0 {
        return Err(invalid_data(
            "schema-7 overflow blob reserved field must be zero",
        ));
    }

    Ok(ValidatedChunkOverflowBlobBytes {
        series_ref,
        chunk_count,
        encoded_entries: &bytes[CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN..],
    })
}

fn encode_chunk_overflow_root_v2(
    series_count: u32,
    blob_count: u32,
    blobs_len: u64,
) -> io::Result<[u8; CHUNK_OVERFLOW_ROOT_V2_LEN]> {
    if blob_count > series_count {
        return Err(invalid_input(
            "schema-7 overflow blob count exceeds series count",
        ));
    }
    if (blob_count == 0) != (blobs_len == 0) {
        return Err(invalid_input(
            "schema-7 overflow blob count and region length disagree",
        ));
    }
    let file_len = CHUNK_OVERFLOW_BLOBS_OFFSET
        .checked_add(blobs_len)
        .ok_or_else(|| invalid_input("schema-7 chunk index file length overflows"))?;

    let mut bytes = [0u8; CHUNK_OVERFLOW_ROOT_V2_LEN];
    put_u32(&mut bytes, 0, CHUNK_INDEX_MAGIC);
    put_u16(&mut bytes, 4, CHUNK_INDEX_VERSION_V2);
    put_u32(&mut bytes, 8, CHUNK_OVERFLOW_ROOT_V2_LEN as u32);
    put_u32(&mut bytes, 12, CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32);
    put_u32(&mut bytes, 16, OVERFLOW_CHUNK_ENTRY_V1_LEN as u32);
    put_u32(&mut bytes, 20, series_count);
    put_u32(&mut bytes, 24, blob_count);
    put_u64(&mut bytes, 32, CHUNK_OVERFLOW_BLOBS_OFFSET);
    put_u64(&mut bytes, 40, blobs_len);
    put_u64(&mut bytes, 48, file_len);
    let root_crc32c = crc32c(&bytes);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc32c);
    Ok(bytes)
}

fn encode_chunk_overflow_blob_v1(
    blob: &ChunkOverflowBlobV1,
    series_count: u32,
) -> io::Result<Vec<u8>> {
    if blob.series_ref >= series_count {
        return Err(invalid_input(
            "schema-7 overflow blob series_ref is out of range",
        ));
    }
    let chunk_count = u32::try_from(blob.entries.len())
        .map_err(|_| invalid_input("schema-7 overflow chunk count exceeds u32"))?;
    if chunk_count == 0 {
        return Err(invalid_input("schema-7 overflow blob has no chunks"));
    }
    let blob_len = checked_chunk_overflow_blob_len(chunk_count)
        .ok_or_else(|| invalid_input("schema-7 overflow blob length exceeds u32"))?;
    let body_len = blob_len - CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32;
    let capacity = usize::try_from(blob_len)
        .map_err(|_| invalid_input("schema-7 overflow blob cannot fit in memory"))?;

    validate_entry_order(&blob.entries, io::ErrorKind::InvalidInput)?;

    let mut bytes = vec![0u8; CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN];
    bytes.reserve(capacity - CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN);
    put_u32(&mut bytes, 0, CHUNK_OVERFLOW_BLOB_MAGIC);
    put_u16(&mut bytes, 4, CHUNK_OVERFLOW_BLOB_VERSION_V1);
    put_u32(&mut bytes, 8, CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN as u32);
    put_u32(&mut bytes, 12, blob.series_ref);
    put_u32(&mut bytes, 16, chunk_count);
    put_u32(&mut bytes, 24, body_len);
    for entry in &blob.entries {
        encode_overflow_chunk_entry_v1(entry, &mut bytes)?;
    }
    let blob_crc32c = crc32c(&bytes);
    put_u32(&mut bytes, BLOB_CRC_OFFSET, blob_crc32c);
    Ok(bytes)
}

fn encode_overflow_chunk_entry_v1(
    entry: &OverflowChunkEntryV1,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    validate_overflow_chunk_entry_v1(entry, io::ErrorKind::InvalidInput)?;
    let start = out.len();
    let end = start
        .checked_add(OVERFLOW_CHUNK_ENTRY_V1_LEN)
        .ok_or_else(|| invalid_input("schema-7 overflow entry output length overflows"))?;
    out.resize(end, 0);
    out[start] = entry.file_id;
    out[start + 1] = entry.kind as u8;
    put_u64(out, start + 4, entry.min_time_ms);
    put_u64(out, start + 12, entry.max_time_ms);
    put_u64(out, start + 20, entry.offset);
    put_u32(out, start + 28, entry.length);
    put_u32(out, start + 32, entry.scalar_lane_offset);
    put_u32(out, start + 36, entry.scalar_lane_len);
    put_u32(out, start + 40, entry.indexed_prefix_crc32c);
    Ok(())
}

fn decode_overflow_chunk_entry_v1(bytes: &[u8]) -> io::Result<OverflowChunkEntryV1> {
    require_exact_len(
        bytes,
        OVERFLOW_CHUNK_ENTRY_V1_LEN,
        "schema-7 overflow entry",
    )?;
    if read_u16_at(bytes, 2) != 0 {
        return Err(invalid_data(
            "schema-7 overflow entry reserved field must be zero",
        ));
    }
    let kind = chunk_kind_from_u8(bytes[1])?;
    let entry = OverflowChunkEntryV1 {
        file_id: bytes[0],
        kind,
        min_time_ms: read_u64_at(bytes, 4),
        max_time_ms: read_u64_at(bytes, 12),
        offset: read_u64_at(bytes, 20),
        length: read_u32_at(bytes, 28),
        scalar_lane_offset: read_u32_at(bytes, 32),
        scalar_lane_len: read_u32_at(bytes, 36),
        indexed_prefix_crc32c: read_u32_at(bytes, 40),
    };
    validate_overflow_chunk_entry_v1(&entry, io::ErrorKind::InvalidData)?;
    Ok(entry)
}

fn validate_blob_series_order(
    blobs: &[ChunkOverflowBlobV1],
    series_count: u32,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    let mut previous = None;
    for blob in blobs {
        if blob.series_ref >= series_count {
            return Err(error(
                error_kind,
                "schema-7 overflow blob series_ref is out of range",
            ));
        }
        if previous.is_some_and(|previous| blob.series_ref <= previous) {
            return Err(error(
                error_kind,
                "schema-7 overflow blobs are not strictly ordered by series_ref",
            ));
        }
        previous = Some(blob.series_ref);
    }
    Ok(())
}

fn validate_entry_order(
    entries: &[OverflowChunkEntryV1],
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    let mut previous = None;
    for entry in entries {
        validate_overflow_chunk_entry_v1(entry, error_kind)?;
        let order_key = overflow_entry_order_key(entry);
        if previous.is_some_and(|previous| order_key <= previous) {
            return Err(error(
                error_kind,
                "schema-7 overflow entries are not strictly ordered and unique",
            ));
        }
        previous = Some(order_key);
    }
    Ok(())
}

fn validate_overflow_chunk_entry_v1(
    entry: &OverflowChunkEntryV1,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    if entry.file_id > 1 {
        return Err(error(
            error_kind,
            "schema-7 overflow entry file_id is invalid",
        ));
    }
    if entry.min_time_ms > entry.max_time_ms {
        return Err(error(
            error_kind,
            "schema-7 overflow entry time range is reversed",
        ));
    }
    if entry.length < CHUNK_HEADER_LEN {
        return Err(error(
            error_kind,
            "schema-7 overflow entry is shorter than a chunk header",
        ));
    }
    entry
        .offset
        .checked_add(u64::from(entry.length))
        .ok_or_else(|| error(error_kind, "schema-7 overflow entry file range overflows"))?;

    match (entry.scalar_lane_offset, entry.scalar_lane_len) {
        (0, 0) => {}
        (CHUNK_HEADER_LEN, scalar_lane_len) if scalar_lane_len >= TYPED_SCALAR_LANE_HEADER_LEN => {
            if !matches!(
                entry.kind,
                ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
            ) {
                return Err(error(
                    error_kind,
                    "schema-7 overflow scalar lane belongs to a non-typed chunk",
                ));
            }
            let scalar_lane_end = entry
                .scalar_lane_offset
                .checked_add(entry.scalar_lane_len)
                .ok_or_else(|| {
                    error(error_kind, "schema-7 overflow scalar lane range overflows")
                })?;
            if scalar_lane_end > entry.length {
                return Err(error(
                    error_kind,
                    "schema-7 overflow scalar lane exceeds the chunk range",
                ));
            }
        }
        _ => {
            return Err(error(
                error_kind,
                "schema-7 overflow scalar lane locator is not canonical",
            ));
        }
    }
    Ok(())
}

fn overflow_entry_order_key(entry: &OverflowChunkEntryV1) -> (u8, u64, u64, u64) {
    (
        entry.file_id,
        entry.min_time_ms,
        entry.max_time_ms,
        entry.offset,
    )
}

fn chunk_kind_from_u8(value: u8) -> io::Result<ChunkKind> {
    match value {
        value if value == ChunkKind::Float as u8 => Ok(ChunkKind::Float),
        value if value == ChunkKind::Int64 as u8 => Ok(ChunkKind::Int64),
        value if value == ChunkKind::Histogram as u8 => Ok(ChunkKind::Histogram),
        value if value == ChunkKind::ExponentialHistogram as u8 => {
            Ok(ChunkKind::ExponentialHistogram)
        }
        value if value == ChunkKind::Summary as u8 => Ok(ChunkKind::Summary),
        _ => Err(invalid_data("schema-7 overflow entry kind is invalid")),
    }
}

fn require_exact_len(bytes: &[u8], expected: usize, subject: &'static str) -> io::Result<()> {
    if bytes.len() < expected {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{subject} is truncated"),
        ));
    }
    if bytes.len() > expected {
        return Err(invalid_data_owned(format!("{subject} has trailing bytes")));
    }
    Ok(())
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn invalid_input(message: &'static str) -> io::Error {
    error(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    error(io::ErrorKind::InvalidData, message)
}

fn invalid_data_owned(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn resource_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, message)
}

fn unexpected_eof(message: &'static str) -> io::Error {
    error(io::ErrorKind::UnexpectedEof, message)
}

fn error(kind: io::ErrorKind, message: &'static str) -> io::Error {
    io::Error::new(kind, message)
}

#[cfg(test)]
mod tests;
