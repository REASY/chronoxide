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
mod tests {
    use super::*;
    use std::fmt::Write as _;

    const ROOT_SERIES_COUNT_OFFSET: usize = 20;
    const ROOT_BLOB_COUNT_OFFSET: usize = 24;
    const ROOT_RESERVED0_OFFSET: usize = 28;
    const ROOT_BLOBS_LEN_OFFSET: usize = 40;
    const ROOT_FILE_LEN_OFFSET: usize = 48;
    const BLOB_SERIES_REF_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 12;
    const BLOB_CHUNK_COUNT_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 16;
    const BLOB_RESERVED0_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 20;
    const BLOB_BODY_LEN_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 24;
    const FIRST_ENTRY_OFFSET: usize =
        CHUNK_OVERFLOW_ROOT_V2_LEN + CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN;

    fn scalar_entry(
        file_id: u8,
        kind: ChunkKind,
        min_time_ms: u64,
        max_time_ms: u64,
        offset: u64,
    ) -> OverflowChunkEntryV1 {
        OverflowChunkEntryV1 {
            file_id,
            kind,
            min_time_ms,
            max_time_ms,
            offset,
            length: 72,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
            indexed_prefix_crc32c: 0x89ab_cdef,
        }
    }

    fn typed_entry(
        file_id: u8,
        kind: ChunkKind,
        min_time_ms: u64,
        max_time_ms: u64,
        offset: u64,
    ) -> OverflowChunkEntryV1 {
        OverflowChunkEntryV1 {
            file_id,
            kind,
            min_time_ms,
            max_time_ms,
            offset,
            length: 104,
            scalar_lane_offset: 40,
            scalar_lane_len: 24,
            indexed_prefix_crc32c: 0x7654_3210,
        }
    }

    fn one_blob_file() -> EncodedChunkIndexV2 {
        encode_chunk_index_v2(
            3,
            &[ChunkOverflowBlobV1 {
                series_ref: 2,
                entries: vec![typed_entry(
                    1,
                    ChunkKind::ExponentialHistogram,
                    1_700_000_000_123,
                    1_700_000_004_567,
                    0x1020_3040,
                )],
            }],
        )
        .unwrap()
    }

    fn reseal_root(bytes: &mut [u8]) {
        put_u32(bytes, ROOT_CRC_OFFSET, 0);
        let crc = crc32c(&bytes[..CHUNK_OVERFLOW_ROOT_V2_LEN]);
        put_u32(bytes, ROOT_CRC_OFFSET, crc);
    }

    fn reseal_blob(bytes: &mut [u8], blob_offset: usize, blob_len: usize) {
        let crc_offset = blob_offset + BLOB_CRC_OFFSET;
        put_u32(bytes, crc_offset, 0);
        let crc = crc32c(&bytes[blob_offset..blob_offset + blob_len]);
        put_u32(bytes, crc_offset, crc);
    }

    fn hex(bytes: &[u8]) -> String {
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded
    }

    #[test]
    fn empty_file_has_deterministic_golden_bytes() {
        let encoded = encode_chunk_index_v2(0, &[]).unwrap();

        assert_eq!(encoded.bytes.len(), CHUNK_OVERFLOW_ROOT_V2_LEN);
        assert_eq!(encoded.root.series_count, 0);
        assert_eq!(encoded.root.blob_count, 0);
        assert_eq!(encoded.root.blobs_len, 0);
        assert_eq!(encoded.root.file_len, 64);
        assert_eq!(encoded.root.root_crc32c, 0x22c9_9139);
        assert!(encoded.blob_locators.is_empty());
        assert_eq!(
            hex(&encoded.bytes),
            concat!(
                "434849580200000040000000200000002c000000000000000000000000000000",
                "4000000000000000000000000000000040000000000000003991c92200000000",
            )
        );
        assert_eq!(
            decode_chunk_index_v2(&encoded.bytes).unwrap(),
            DecodedChunkIndexV2 {
                root: encoded.root,
                blobs: Vec::new(),
                blob_locators: Vec::new(),
            }
        );
    }

    #[test]
    fn one_blob_has_exact_layout_and_round_trips() {
        let encoded = one_blob_file();

        assert_eq!(encoded.bytes.len(), 64 + 32 + 44);
        assert_eq!(encoded.root.series_count, 3);
        assert_eq!(encoded.root.blob_count, 1);
        assert_eq!(encoded.root.blobs_len, 76);
        assert_eq!(encoded.root.file_len, 140);
        assert_eq!(encoded.root.root_crc32c, 0xc602_2e0a);
        assert_eq!(read_u32_at(&encoded.bytes, 28), 0);
        assert_eq!(read_u32_at(&encoded.bytes, 60), 0);
        assert_eq!(read_u32_at(&encoded.bytes, 64), CHUNK_OVERFLOW_BLOB_MAGIC);
        assert_eq!(read_u32_at(&encoded.bytes, 72), 32);
        assert_eq!(read_u32_at(&encoded.bytes, 76), 2);
        assert_eq!(read_u32_at(&encoded.bytes, 80), 1);
        assert_eq!(read_u32_at(&encoded.bytes, 84), 0);
        assert_eq!(read_u32_at(&encoded.bytes, 88), 44);
        assert_eq!(read_u32_at(&encoded.bytes, 92), 0x4ec4_4678);
        assert_eq!(read_u16_at(&encoded.bytes, FIRST_ENTRY_OFFSET + 2), 0);
        assert_eq!(
            hex(&encoded.bytes),
            concat!(
                "434849580200000040000000200000002c000000030000000100000000000000",
                "40000000000000004c000000000000008c000000000000000a2e02c600000000",
                "434f463701000000200000000200000001000000000000002c0000007846c44e",
                "010300007b68e5cf8b010000d779e5cf8b010000403020100000000068000000",
                "280000001800000010325476",
            )
        );
        assert_eq!(
            encoded.blob_locators,
            vec![ChunkOverflowBlobLocatorV1 {
                series_ref: 2,
                blob_offset: 64,
                blob_len: 76,
                chunk_count: 1,
            }]
        );

        let decoded = decode_chunk_index_v2(&encoded.bytes).unwrap();
        assert_eq!(decoded.root, encoded.root);
        assert_eq!(decoded.blob_locators, encoded.blob_locators);
        assert_eq!(decoded.blobs[0].series_ref, 2);
        assert_eq!(decoded.blobs[0].entries.len(), 1);
        assert_eq!(
            decoded.blobs[0].entries[0],
            typed_entry(
                1,
                ChunkKind::ExponentialHistogram,
                1_700_000_000_123,
                1_700_000_004_567,
                0x1020_3040,
            )
        );
        assert_eq!(
            decode_touched_chunk_overflow_blob_v1(
                &encoded.bytes[64..],
                &encoded.root,
                encoded.blob_locators[0],
            )
            .unwrap(),
            decoded.blobs[0]
        );
    }

    #[test]
    fn touched_blob_must_match_the_hot_record_locator() {
        let encoded = one_blob_file();
        let bytes = &encoded.bytes[64..];
        let locator = encoded.blob_locators[0];

        for mismatched in [
            ChunkOverflowBlobLocatorV1 {
                series_ref: 1,
                ..locator
            },
            ChunkOverflowBlobLocatorV1 {
                blob_len: locator.blob_len + 1,
                ..locator
            },
            ChunkOverflowBlobLocatorV1 {
                chunk_count: locator.chunk_count + 1,
                ..locator
            },
            ChunkOverflowBlobLocatorV1 {
                blob_offset: 63,
                ..locator
            },
            ChunkOverflowBlobLocatorV1 {
                blob_offset: encoded.root.file_len,
                ..locator
            },
        ] {
            assert_eq!(
                decode_touched_chunk_overflow_blob_v1(bytes, &encoded.root, mismatched)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn multiple_blobs_and_mixed_lanes_round_trip_without_reordering() {
        let blobs = vec![
            ChunkOverflowBlobV1 {
                series_ref: 0,
                entries: vec![
                    scalar_entry(0, ChunkKind::Float, 10, 20, 100),
                    typed_entry(0, ChunkKind::Histogram, 21, 30, 200),
                    typed_entry(1, ChunkKind::Summary, 10, 20, 300),
                ],
            },
            ChunkOverflowBlobV1 {
                series_ref: 4,
                entries: vec![scalar_entry(1, ChunkKind::Int64, 100, 100, 400)],
            },
        ];

        let encoded = encode_chunk_index_v2(5, &blobs).unwrap();
        let decoded = decode_chunk_index_v2(&encoded.bytes).unwrap();

        assert_eq!(decoded.blobs, blobs);
        assert_eq!(decoded.root.blob_count, 2);
        assert_eq!(decoded.blob_locators[0].blob_offset, 64);
        assert_eq!(decoded.blob_locators[0].blob_len, 164);
        assert_eq!(decoded.blob_locators[1].blob_offset, 228);
        assert_eq!(decoded.blob_locators[1].blob_len, 76);
    }

    #[test]
    fn blob_length_boundaries_follow_the_u32_contract() {
        const MAX_CHUNK_COUNT: u32 = 97_612_892;

        assert_eq!(checked_chunk_overflow_blob_len(0), Some(32));
        assert_eq!(checked_chunk_overflow_blob_len(1), Some(76));
        assert_eq!(
            checked_chunk_overflow_blob_len(MAX_CHUNK_COUNT),
            Some(4_294_967_280)
        );
        assert_eq!(checked_chunk_overflow_blob_len(MAX_CHUNK_COUNT + 1), None);
    }

    #[test]
    fn maximum_valid_entry_widths_round_trip() {
        let entry = OverflowChunkEntryV1 {
            file_id: 1,
            kind: ChunkKind::Summary,
            min_time_ms: u64::MAX,
            max_time_ms: u64::MAX,
            offset: u64::MAX - u64::from(u32::MAX),
            length: u32::MAX,
            scalar_lane_offset: 40,
            scalar_lane_len: u32::MAX - 40,
            indexed_prefix_crc32c: u32::MAX,
        };
        let blob = ChunkOverflowBlobV1 {
            series_ref: u32::MAX - 1,
            entries: vec![entry],
        };

        let encoded = encode_chunk_index_v2(u32::MAX, std::slice::from_ref(&blob)).unwrap();
        let decoded = decode_chunk_index_v2(&encoded.bytes).unwrap();

        assert_eq!(decoded.blobs, vec![blob]);
    }

    #[test]
    fn writer_rejects_noncanonical_blob_and_entry_ordering() {
        let descending_blobs = vec![
            ChunkOverflowBlobV1 {
                series_ref: 2,
                entries: vec![scalar_entry(0, ChunkKind::Float, 1, 2, 3)],
            },
            ChunkOverflowBlobV1 {
                series_ref: 1,
                entries: vec![scalar_entry(0, ChunkKind::Float, 1, 2, 3)],
            },
        ];
        assert_eq!(
            encode_chunk_index_v2(3, &descending_blobs)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let duplicate_entries = vec![
            scalar_entry(0, ChunkKind::Float, 1, 2, 3),
            scalar_entry(0, ChunkKind::Int64, 1, 2, 3),
        ];
        assert_eq!(
            encode_chunk_index_v2(
                1,
                &[ChunkOverflowBlobV1 {
                    series_ref: 0,
                    entries: duplicate_entries,
                }],
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn reader_rejects_noncanonical_entry_order_after_valid_crc() {
        let blob = ChunkOverflowBlobV1 {
            series_ref: 0,
            entries: vec![
                scalar_entry(0, ChunkKind::Float, 1, 2, 3),
                scalar_entry(0, ChunkKind::Float, 4, 5, 6),
            ],
        };
        let mut bytes = encode_chunk_index_v2(1, &[blob]).unwrap().bytes;
        let second_entry = FIRST_ENTRY_OFFSET + OVERFLOW_CHUNK_ENTRY_V1_LEN;
        bytes.copy_within(FIRST_ENTRY_OFFSET..FIRST_ENTRY_OFFSET + 44, second_entry);
        let blob_len = bytes.len() - CHUNK_OVERFLOW_ROOT_V2_LEN;
        reseal_blob(&mut bytes, CHUNK_OVERFLOW_ROOT_V2_LEN, blob_len);

        let error = decode_chunk_index_v2(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("strictly ordered"));
    }

    #[test]
    fn reader_rejects_noncanonical_blob_order_after_valid_crcs() {
        let blobs = vec![
            ChunkOverflowBlobV1 {
                series_ref: 0,
                entries: vec![scalar_entry(0, ChunkKind::Float, 1, 2, 3)],
            },
            ChunkOverflowBlobV1 {
                series_ref: 1,
                entries: vec![scalar_entry(0, ChunkKind::Float, 4, 5, 6)],
            },
        ];
        let encoded = encode_chunk_index_v2(2, &blobs).unwrap();
        let mut bytes = encoded.bytes;
        let second_blob = encoded.blob_locators[1];
        put_u32(&mut bytes, second_blob.blob_offset as usize + 12, 0);
        reseal_blob(
            &mut bytes,
            second_blob.blob_offset as usize,
            second_blob.blob_len as usize,
        );

        let error = decode_chunk_index_v2(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("strictly ordered"));
    }

    #[test]
    fn checksum_corruption_is_not_treated_as_absence() {
        let mut root_corruption = one_blob_file().bytes;
        root_corruption[8] ^= 1;
        let error = decode_chunk_index_v2(&root_corruption).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("root crc"));

        let mut blob_corruption = one_blob_file().bytes;
        blob_corruption[FIRST_ENTRY_OFFSET + 40] ^= 1;
        let error = decode_chunk_index_v2(&blob_corruption).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("blob crc"));
    }

    #[test]
    fn fixed_root_fields_and_reserved_words_are_enforced_after_valid_crc() {
        let mutations = [
            (0, 0u64, 4usize),
            (4, 3, 2),
            (6, 1u64, 2usize),
            (8, 65, 4),
            (12, 31, 4),
            (16, 40, 4),
            (ROOT_RESERVED0_OFFSET, 1, 4),
            (32, 65, 8),
            (60, 1, 4),
        ];
        for (offset, value, width) in mutations {
            let mut bytes = one_blob_file().bytes;
            match width {
                2 => put_u16(&mut bytes, offset, value as u16),
                4 => put_u32(&mut bytes, offset, value as u32),
                8 => put_u64(&mut bytes, offset, value),
                _ => unreachable!(),
            }
            reseal_root(&mut bytes);
            assert_eq!(
                decode_chunk_index_v2(&bytes).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "offset {offset}"
            );
        }
    }

    #[test]
    fn root_count_and_length_relationships_are_enforced() {
        let mut excessive_count = one_blob_file().bytes;
        put_u32(&mut excessive_count, ROOT_SERIES_COUNT_OFFSET, 0);
        reseal_root(&mut excessive_count);
        assert_eq!(
            decode_chunk_index_v2(&excessive_count).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let mut missing_blob_count = one_blob_file().bytes;
        put_u32(&mut missing_blob_count, ROOT_BLOB_COUNT_OFFSET, 0);
        reseal_root(&mut missing_blob_count);
        assert_eq!(
            decode_chunk_index_v2(&missing_blob_count)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );

        let mut wrong_file_len = one_blob_file().bytes;
        put_u64(&mut wrong_file_len, ROOT_FILE_LEN_OFFSET, 139);
        reseal_root(&mut wrong_file_len);
        assert_eq!(
            decode_chunk_index_v2(&wrong_file_len).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let mut short_region = one_blob_file().bytes;
        put_u64(&mut short_region, ROOT_BLOBS_LEN_OFFSET, 75);
        put_u64(&mut short_region, ROOT_FILE_LEN_OFFSET, 139);
        reseal_root(&mut short_region);
        assert_eq!(
            decode_chunk_overflow_root_v2(&short_region[..64], 139)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn blob_header_and_reserved_fields_are_enforced_after_valid_crc() {
        let mutations = [
            (CHUNK_OVERFLOW_ROOT_V2_LEN, 0u64, 4usize),
            (CHUNK_OVERFLOW_ROOT_V2_LEN + 4, 2, 2),
            (CHUNK_OVERFLOW_ROOT_V2_LEN + 6, 1u64, 2usize),
            (CHUNK_OVERFLOW_ROOT_V2_LEN + 8, 31, 4),
            (BLOB_RESERVED0_OFFSET, 1, 4),
        ];
        for (offset, value, width) in mutations {
            let mut bytes = one_blob_file().bytes;
            match width {
                2 => put_u16(&mut bytes, offset, value as u16),
                4 => put_u32(&mut bytes, offset, value as u32),
                _ => unreachable!(),
            }
            reseal_blob(&mut bytes, 64, 76);
            assert_eq!(
                decode_chunk_index_v2(&bytes).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "offset {offset}"
            );
        }
    }

    #[test]
    fn blob_counts_lengths_and_series_bounds_are_enforced() {
        let mut zero_chunks = one_blob_file().bytes;
        put_u32(&mut zero_chunks, BLOB_CHUNK_COUNT_OFFSET, 0);
        put_u32(&mut zero_chunks, BLOB_BODY_LEN_OFFSET, 0);
        reseal_blob(&mut zero_chunks, 64, 32);
        assert_eq!(
            decode_chunk_index_v2(&zero_chunks).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let mut wrong_body_len = one_blob_file().bytes;
        put_u32(&mut wrong_body_len, BLOB_BODY_LEN_OFFSET, 43);
        reseal_blob(&mut wrong_body_len, 64, 76);
        assert_eq!(
            decode_chunk_index_v2(&wrong_body_len).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let mut out_of_range_series = one_blob_file().bytes;
        put_u32(&mut out_of_range_series, BLOB_SERIES_REF_OFFSET, 3);
        reseal_blob(&mut out_of_range_series, 64, 76);
        assert_eq!(
            decode_chunk_index_v2(&out_of_range_series)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn entry_reserved_kind_file_time_and_range_fields_are_enforced() {
        let mutations = [
            (FIRST_ENTRY_OFFSET, 2u64, 1usize),
            (FIRST_ENTRY_OFFSET + 1, 5, 1),
            (FIRST_ENTRY_OFFSET + 2, 1, 2),
            (FIRST_ENTRY_OFFSET + 4, u64::MAX, 8),
            (FIRST_ENTRY_OFFSET + 20, u64::MAX, 8),
            (FIRST_ENTRY_OFFSET + 28, 39, 4),
        ];
        for (offset, value, width) in mutations {
            let mut bytes = one_blob_file().bytes;
            match width {
                1 => bytes[offset] = value as u8,
                2 => put_u16(&mut bytes, offset, value as u16),
                4 => put_u32(&mut bytes, offset, value as u32),
                8 => put_u64(&mut bytes, offset, value),
                _ => unreachable!(),
            }
            reseal_blob(&mut bytes, 64, 76);
            assert_eq!(
                decode_chunk_index_v2(&bytes).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "offset {offset}"
            );
        }
    }

    #[test]
    fn scalar_lane_shape_is_canonical() {
        let invalid_shapes = [
            (0, 16, ChunkKind::Histogram, 104),
            (40, 0, ChunkKind::Histogram, 104),
            (40, 15, ChunkKind::Histogram, 104),
            (40, 24, ChunkKind::Float, 104),
            (40, 65, ChunkKind::Histogram, 104),
            (u32::MAX, 16, ChunkKind::Histogram, u32::MAX),
        ];
        for (scalar_lane_offset, scalar_lane_len, kind, length) in invalid_shapes {
            let mut entry = typed_entry(0, kind, 1, 2, 3);
            entry.scalar_lane_offset = scalar_lane_offset;
            entry.scalar_lane_len = scalar_lane_len;
            entry.length = length;
            let error = encode_chunk_index_v2(
                1,
                &[ChunkOverflowBlobV1 {
                    series_ref: 0,
                    entries: vec![entry],
                }],
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn truncation_and_trailing_bytes_are_structural_errors() {
        let encoded = one_blob_file().bytes;
        for len in [0, 1, 63, 64, 95, 96, 139] {
            let error = decode_chunk_index_v2(&encoded[..len]).unwrap_err();
            assert!(matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
            ));
        }

        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            decode_chunk_index_v2(&trailing).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
