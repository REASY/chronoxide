//! Deterministic streaming assembly for schema-7 series routing metadata.
//!
//! This is intentionally isolated from `SegmentWriter`: callers supply final
//! series-major chunk locators and immutable positional sources after every
//! chunk header and enclosing frame has reached its published offset. The
//! assembly reads every exact 40/56-byte prefix once. A structural prepass
//! retains only the blobs for series that cannot use the inline layout; the
//! output pass authenticates inline candidates while retaining at most one
//! 409-record hot page. This avoids both duplicate reads and one retained hot
//! record per series.

use std::io::{self, Seek, SeekFrom, Write};

use crc32c::crc32c_append;

use crate::storage::chunk::{
    ChunkIndexEntry, ChunkOverflowBlobV1, ChunkOverflowRootV2, EncodedChunkIndexV2,
};
use crate::storage::index::SegmentIndexReadAt;

use super::super::cold_v2::{SeriesColdV2Plan, SeriesColdV2SectionOffsets};
use super::classifier::chunk_entry_fits_inline;
use super::{
    ClassifiedSeriesV3, FinalChunkIndexEntryV3, PendingOverflowSeriesV3, SERIES_COLD_PAGE_LEN_V1,
    SERIES_HOT_RECORDS_PER_PAGE_V1, SeriesClassifierInputV3, SeriesColdPageDescriptorV1,
    SeriesHeaderV3, SeriesHeaderV3Params, SeriesHotV3, classify_series_v3, decode_series_root_v3,
    encode_series_hot_page_v1, encode_series_root_v3,
};
use crate::storage::chunk::encode_chunk_index_v2;
use crate::storage::series::SeriesEntry;

const CHUNK_HEADER_LEN: usize = 40;
const INDEXED_PREFIX_WITH_SCALAR_LEN: usize = 56;
const TYPED_SCALAR_LANE_HEADER_LEN: u32 = 16;
const COLD_PAGE_BUFFER_LEN: usize = SERIES_COLD_PAGE_LEN_V1 as usize;
const ZERO_WRITE_BUFFER_LEN: usize = 16 * 1024;

/// Final immutable inputs for one schema-7 metadata assembly.
///
/// `series_entries` and `chunk_entries` use final dense series-ref order.
/// `SeriesEntry::chunk_index` is intentionally ignored because schema 7
/// replaces the schema-6 v1 span with an inline record or v2 overflow locator.
pub(crate) struct Schema7SeriesAssemblyInput<'a> {
    pub(crate) series_entries: &'a [SeriesEntry],
    pub(crate) chunk_entries: &'a [Vec<ChunkIndexEntry>],
    pub(crate) segment_start_ms: u64,
    pub(crate) segment_end_ms: u64,
    /// Exact footer-inventoried lengths for `chunks.bin` and `ooo_chunks.bin`.
    pub(crate) chunk_file_lens: [u64; 2],
    /// Immutable positional sources in the same file-ID order.
    pub(crate) chunk_sources: [&'a dyn SegmentIndexReadAt; 2],
}

/// Observable bounded-work and physical-layout facts from one assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7SeriesAssemblyStats {
    pub(crate) series_count: u32,
    pub(crate) chunk_count: u64,
    pub(crate) inline_series_count: u32,
    pub(crate) overflow_series_count: u32,
    pub(crate) first_prefix_reads: u64,
    pub(crate) first_prefix_bytes: u64,
    pub(crate) second_prefix_reads: u64,
    pub(crate) second_prefix_bytes: u64,
    pub(crate) hot_page_count: u32,
    pub(crate) cold_page_count: u32,
    pub(crate) peak_hot_records_buffered: u32,
    pub(crate) series_file_len: u64,
    pub(crate) chunk_index_file_len: u64,
}

/// Authenticated roots and stats produced by the isolated assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7SeriesAssemblyResult {
    pub(crate) series_header: SeriesHeaderV3,
    pub(crate) chunk_index_root: ChunkOverflowRootV2,
    pub(crate) stats: Schema7SeriesAssemblyStats,
}

/// Streams a complete `series.bin` v3 and `chunk_index.bin` v2 into empty,
/// seekable outputs without wiring either artifact into segment publication.
///
/// The chunk-index encoder creates every overflow locator and its bound root
/// in one deterministic result. The series root is written last, after every
/// hot-page and cross-section cold-page CRC is final.
pub(crate) fn write_schema7_series_and_chunk_index<S, C>(
    series_writer: &mut S,
    chunk_index_writer: &mut C,
    input: Schema7SeriesAssemblyInput<'_>,
) -> io::Result<Schema7SeriesAssemblyResult>
where
    S: Write + Seek,
    C: Write + Seek,
{
    require_empty_output(series_writer, "schema-7 series output")?;
    require_empty_output(chunk_index_writer, "schema-7 chunk-index output")?;
    validate_input_shape(&input)?;

    let cold = SeriesColdV2Plan::build(input.series_entries)?;
    validate_cold_row_identity(&cold, input.series_entries)?;
    let series_count = cold.num_series();
    let chunk_count =
        input
            .chunk_entries
            .iter()
            .try_fold(0u64, |total, entries| {
                total
                    .checked_add(u64::try_from(entries.len()).map_err(|_| {
                        invalid_input("schema-7 per-series chunk count exceeds u64")
                    })?)
                    .ok_or_else(|| invalid_input("schema-7 total chunk count exceeds u64"))
            })?;

    let mut first_prefix = PrefixReadStats::default();
    let mut overflow_blobs = Vec::new();
    let mut inline_series_count = 0u32;
    for series_ref in 0..series_count {
        if !series_requires_overflow_prepass(&input, series_ref)? {
            inline_series_count = inline_series_count
                .checked_add(1)
                .ok_or_else(|| invalid_input("schema-7 inline series count exceeds u32"))?;
            continue;
        }
        let ClassifiedSeriesV3::Overflow(pending) =
            classify_series_from_sources(&input, &cold, series_ref, &mut first_prefix)?
        else {
            return Err(invalid_data(
                "schema-7 structural overflow candidate classified inline",
            ));
        };
        overflow_blobs
            .try_reserve(1)
            .map_err(|_| resource_error("schema-7 overflow blob allocation failed"))?;
        overflow_blobs.push(pending.blob);
    }
    let overflow_series_count = u32::try_from(overflow_blobs.len())
        .map_err(|_| invalid_input("schema-7 overflow series count exceeds u32"))?;
    if inline_series_count
        .checked_add(overflow_series_count)
        .ok_or_else(|| invalid_input("schema-7 classified series count exceeds u32"))?
        != series_count
    {
        return Err(invalid_data(
            "schema-7 classified series count does not match input",
        ));
    }

    // The complete encoder output is the sole provenance for root and hot-record locators.
    // Keeping encoded overflow data is permitted; no complete series.bin is retained.
    let EncodedChunkIndexV2 {
        bytes: chunk_index_bytes,
        root: chunk_index_root,
        blob_locators,
    } = encode_chunk_index_v2(series_count, &overflow_blobs)?;
    if blob_locators.len() != overflow_blobs.len() {
        return Err(invalid_data(
            "schema-7 overflow encoder locator count mismatch",
        ));
    }
    chunk_index_writer.write_all(&chunk_index_bytes)?;
    require_output_len(
        chunk_index_writer,
        chunk_index_root.file_len,
        "schema-7 chunk-index output",
    )?;
    drop(chunk_index_bytes);

    let cold_lengths = cold.lengths();
    let header = SeriesHeaderV3::new(SeriesHeaderV3Params {
        num_series: series_count,
        num_keysets: cold.num_keysets(),
        num_value_dicts: cold.num_value_dicts(),
        chunk_index_root_crc32c: chunk_index_root.root_crc32c,
        keysets_len: cold_lengths.keysets,
        value_dicts_len: cold_lengths.value_dicts,
        keyset_blocks_len: cold_lengths.keyset_blocks,
        segment_start_ms: input.segment_start_ms,
        segment_end_ms: input.segment_end_ms,
        chunk_index_file_len: chunk_index_root.file_len,
    })?;
    validate_root_binding(header, chunk_index_root)?;

    // Reserve the canonical root range with real zero bytes. It is replaced only after
    // descriptors and their covered bytes are final, so stale data can never survive.
    write_zeroes(series_writer, header.hot_pages_offset)?;
    require_position(
        series_writer,
        header.hot_pages_offset,
        "schema-7 hot-page start",
    )?;

    let hot_page_capacity = usize::try_from(SERIES_HOT_RECORDS_PER_PAGE_V1)
        .map_err(|_| invalid_input("schema-7 hot-page record count exceeds usize"))?;
    let mut hot_descriptors = Vec::new();
    hot_descriptors
        .try_reserve_exact(
            usize::try_from(header.page_count)
                .map_err(|_| invalid_input("schema-7 hot-page count exceeds usize"))?,
        )
        .map_err(|_| resource_error("schema-7 hot descriptor allocation failed"))?;
    let mut second_prefix = PrefixReadStats::default();
    let mut overflow_blob_iter = overflow_blobs.into_iter();
    let mut blob_locator_iter = blob_locators.into_iter();
    let mut peak_hot_records_buffered = 0u32;
    for page_index in 0..header.page_count {
        let record_count = header.expected_hot_record_count(page_index)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(
                usize::try_from(record_count)
                    .map_err(|_| invalid_input("schema-7 hot record count exceeds usize"))?,
            )
            .map_err(|_| resource_error("schema-7 hot-page allocation failed"))?;
        let first_series_ref = page_index
            .checked_mul(SERIES_HOT_RECORDS_PER_PAGE_V1)
            .ok_or_else(|| invalid_input("schema-7 first series-ref overflows"))?;
        for page_ordinal in 0..record_count {
            let series_ref = first_series_ref
                .checked_add(page_ordinal)
                .ok_or_else(|| invalid_input("schema-7 series-ref overflows"))?;
            let record = if series_requires_overflow_prepass(&input, series_ref)? {
                let blob = overflow_blob_iter
                    .next()
                    .ok_or_else(|| invalid_data("schema-7 overflow prepass blob is missing"))?;
                let locator = blob_locator_iter
                    .next()
                    .ok_or_else(|| invalid_data("schema-7 overflow encoder locator is missing"))?;
                bind_preclassified_overflow_record(
                    &input,
                    &cold,
                    series_ref,
                    blob,
                    locator,
                    chunk_index_root.file_len,
                )?
            } else {
                let ClassifiedSeriesV3::Inline(record) =
                    classify_series_from_sources(&input, &cold, series_ref, &mut second_prefix)?
                else {
                    return Err(invalid_data(
                        "schema-7 structural inline candidate classified as overflow",
                    ));
                };
                record
            };
            records.push(record);
        }
        peak_hot_records_buffered =
            peak_hot_records_buffered
                .max(u32::try_from(records.len()).map_err(|_| {
                    invalid_input("schema-7 buffered hot record count exceeds u32")
                })?);
        if records.len() > hot_page_capacity {
            return Err(invalid_data(
                "schema-7 hot-page buffering exceeded the canonical page size",
            ));
        }
        let (descriptor, page) =
            encode_series_hot_page_v1(header, page_index, &records, input.chunk_file_lens)?;
        series_writer.write_all(&page)?;
        hot_descriptors.push(descriptor);
    }
    if overflow_blob_iter.next().is_some() || blob_locator_iter.next().is_some() {
        return Err(invalid_data(
            "schema-7 output pass did not consume every overflow locator",
        ));
    }
    require_position(
        series_writer,
        header.keysets_offset,
        "schema-7 cold-section start",
    )?;

    let cold_offsets = cold.section_offsets_at(header.keysets_offset)?;
    require_cold_offsets_match_header(header, cold_offsets)?;
    let (cold_written, cold_descriptors) = {
        let mut cold_writer = ColdPageCrcWriter::new(series_writer, header)?;
        let cold_written = cold.write_sections_at(&mut cold_writer, cold_offsets)?;
        let cold_descriptors = cold_writer.finish()?;
        (cold_written, cold_descriptors)
    };
    if cold_written != cold_lengths.total()? {
        return Err(invalid_data("schema-7 cold encoded length mismatch"));
    }
    require_position(series_writer, header.file_len, "schema-7 series EOF")?;
    require_output_len(series_writer, header.file_len, "schema-7 series output")?;

    // This is deliberately the final series write. Root bytes authenticate the completed
    // page/cold descriptors and the overflow root that supplied every stored locator.
    let (series_header, root_bytes) =
        encode_series_root_v3(header, &hot_descriptors, &cold_descriptors)?;
    let decoded_root = decode_series_root_v3(&root_bytes)?;
    if decoded_root.header != series_header {
        return Err(invalid_data(
            "schema-7 encoded root does not round-trip before publication",
        ));
    }
    validate_root_binding(series_header, chunk_index_root)?;
    series_writer.seek(SeekFrom::Start(0))?;
    series_writer.write_all(&root_bytes)?;
    require_output_len(
        series_writer,
        series_header.file_len,
        "schema-7 series output",
    )?;

    if first_prefix
        .reads
        .checked_add(second_prefix.reads)
        .ok_or_else(|| invalid_data("schema-7 prefix read count exceeds u64"))?
        != chunk_count
    {
        return Err(invalid_data(
            "schema-7 prefix read count does not match chunk count",
        ));
    }

    Ok(Schema7SeriesAssemblyResult {
        series_header,
        chunk_index_root,
        stats: Schema7SeriesAssemblyStats {
            series_count,
            chunk_count,
            inline_series_count,
            overflow_series_count,
            first_prefix_reads: first_prefix.reads,
            first_prefix_bytes: first_prefix.bytes,
            second_prefix_reads: second_prefix.reads,
            second_prefix_bytes: second_prefix.bytes,
            hot_page_count: series_header.page_count,
            cold_page_count: series_header.cold_page_count,
            peak_hot_records_buffered,
            series_file_len: series_header.file_len,
            chunk_index_file_len: chunk_index_root.file_len,
        },
    })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PrefixReadStats {
    reads: u64,
    bytes: u64,
}

fn classify_series_from_sources(
    input: &Schema7SeriesAssemblyInput<'_>,
    cold: &SeriesColdV2Plan,
    series_ref: u32,
    stats: &mut PrefixReadStats,
) -> io::Result<ClassifiedSeriesV3> {
    let series_index = usize::try_from(series_ref)
        .map_err(|_| invalid_input("schema-7 series-ref exceeds usize"))?;
    let entry = input
        .series_entries
        .get(series_index)
        .ok_or_else(|| invalid_input("schema-7 series entry is missing"))?;
    let cold_row = cold
        .series_rows()
        .get(series_index)
        .ok_or_else(|| invalid_data("schema-7 cold row is missing"))?;
    let chunks = input
        .chunk_entries
        .get(series_index)
        .ok_or_else(|| invalid_input("schema-7 chunk list is missing"))?;

    let mut prefixes = Vec::new();
    prefixes
        .try_reserve_exact(chunks.len())
        .map_err(|_| resource_error("schema-7 indexed-prefix allocation failed"))?;
    for chunk in chunks {
        let prefix_len = indexed_prefix_len_for_writer(chunk)?;
        validate_chunk_read_bounds(chunk, input.chunk_file_lens)?;
        let mut prefix = vec![0u8; prefix_len];
        input.chunk_sources[usize::from(chunk.file_id)].read_exact_at(chunk.offset, &mut prefix)?;
        stats.reads = stats
            .reads
            .checked_add(1)
            .ok_or_else(|| invalid_input("schema-7 prefix read count exceeds u64"))?;
        stats.bytes = stats
            .bytes
            .checked_add(
                u64::try_from(prefix_len)
                    .map_err(|_| invalid_input("schema-7 prefix length exceeds u64"))?,
            )
            .ok_or_else(|| invalid_input("schema-7 prefix byte count exceeds u64"))?;
        prefixes.push(prefix);
    }
    let mut finalized = Vec::new();
    finalized
        .try_reserve_exact(chunks.len())
        .map_err(|_| resource_error("schema-7 final locator allocation failed"))?;
    for (chunk, prefix) in chunks.iter().zip(&prefixes) {
        finalized.push(FinalChunkIndexEntryV3 {
            entry: chunk,
            indexed_prefix: prefix,
        });
    }

    classify_series_v3(SeriesClassifierInputV3 {
        series_ref,
        series_id: entry.series_id,
        keyset_id: cold_row.keyset_id,
        row: cold_row.row,
        kind_mask: entry.kind_mask,
        segment_start_ms: input.segment_start_ms,
        segment_end_ms: input.segment_end_ms,
        chunk_file_lens: input.chunk_file_lens,
        chunks: &finalized,
    })
}

fn series_requires_overflow_prepass(
    input: &Schema7SeriesAssemblyInput<'_>,
    series_ref: u32,
) -> io::Result<bool> {
    let series_index = usize::try_from(series_ref)
        .map_err(|_| invalid_input("schema-7 series-ref exceeds usize"))?;
    let chunks = input
        .chunk_entries
        .get(series_index)
        .ok_or_else(|| invalid_input("schema-7 chunk list is missing"))?;
    Ok(match chunks.as_slice() {
        [chunk] => !chunk_entry_fits_inline(input.segment_start_ms, chunk),
        _ => true,
    })
}

fn bind_preclassified_overflow_record(
    input: &Schema7SeriesAssemblyInput<'_>,
    cold: &SeriesColdV2Plan,
    series_ref: u32,
    blob: ChunkOverflowBlobV1,
    locator: crate::storage::chunk::ChunkOverflowBlobLocatorV1,
    chunk_index_file_len: u64,
) -> io::Result<SeriesHotV3> {
    if blob.series_ref != series_ref {
        return Err(invalid_data(
            "schema-7 overflow prepass order does not match the output series",
        ));
    }
    let series_index = usize::try_from(series_ref)
        .map_err(|_| invalid_input("schema-7 series-ref exceeds usize"))?;
    let entry = input
        .series_entries
        .get(series_index)
        .ok_or_else(|| invalid_input("schema-7 series entry is missing"))?;
    let cold_row = cold
        .series_rows()
        .get(series_index)
        .ok_or_else(|| invalid_data("schema-7 cold row is missing"))?;
    PendingOverflowSeriesV3 {
        series_id: entry.series_id,
        keyset_id: cold_row.keyset_id,
        row: cold_row.row,
        kind_mask: entry.kind_mask,
        blob,
    }
    .bind_blob_locator(locator, chunk_index_file_len)
}

fn validate_input_shape(input: &Schema7SeriesAssemblyInput<'_>) -> io::Result<()> {
    if input.series_entries.len() != input.chunk_entries.len() {
        return Err(invalid_input(
            "schema-7 series and chunk-list counts differ",
        ));
    }
    if input.segment_start_ms >= input.segment_end_ms {
        return Err(invalid_input("schema-7 segment bounds are invalid"));
    }
    for (file_id, source) in input.chunk_sources.iter().enumerate() {
        let actual_len = source.len()?;
        if actual_len != input.chunk_file_lens[file_id] {
            return Err(invalid_input(
                "schema-7 chunk source length does not match inventoried length",
            ));
        }
    }
    Ok(())
}

fn validate_cold_row_identity(cold: &SeriesColdV2Plan, entries: &[SeriesEntry]) -> io::Result<()> {
    if cold.series_rows().len() != entries.len() {
        return Err(invalid_data(
            "schema-7 cold row count does not match series count",
        ));
    }
    for (row, entry) in cold.series_rows().iter().zip(entries) {
        if row.series_id != entry.series_id || row.kind_mask != entry.kind_mask {
            return Err(invalid_data(
                "schema-7 cold row identity does not match its series",
            ));
        }
    }
    Ok(())
}

fn indexed_prefix_len_for_writer(entry: &ChunkIndexEntry) -> io::Result<usize> {
    if entry.file_id > 1 {
        return Err(invalid_input("schema-7 chunk file ID is invalid"));
    }
    if entry.length < CHUNK_HEADER_LEN as u32 {
        return Err(invalid_input(
            "schema-7 chunk is shorter than its fixed header",
        ));
    }
    match (entry.scalar_lane_offset, entry.scalar_lane_len) {
        (0, 0) => Ok(CHUNK_HEADER_LEN),
        (offset, len)
            if offset == CHUNK_HEADER_LEN as u32 && len >= TYPED_SCALAR_LANE_HEADER_LEN =>
        {
            let indexed_len = (CHUNK_HEADER_LEN as u32)
                .checked_add(len)
                .ok_or_else(|| invalid_input("schema-7 indexed chunk length overflows"))?;
            if indexed_len > entry.length {
                return Err(invalid_input(
                    "schema-7 scalar lane exceeds the chunk range",
                ));
            }
            Ok(INDEXED_PREFIX_WITH_SCALAR_LEN)
        }
        _ => Err(invalid_input(
            "schema-7 scalar lane locator is not canonical",
        )),
    }
}

fn validate_chunk_read_bounds(entry: &ChunkIndexEntry, file_lens: [u64; 2]) -> io::Result<()> {
    let file_len = file_lens
        .get(usize::from(entry.file_id))
        .copied()
        .ok_or_else(|| invalid_input("schema-7 chunk file ID is invalid"))?;
    let end = entry
        .offset
        .checked_add(u64::from(entry.length))
        .ok_or_else(|| invalid_input("schema-7 chunk range overflows"))?;
    if end > file_len {
        return Err(invalid_input(
            "schema-7 chunk range exceeds inventoried file length",
        ));
    }
    Ok(())
}

fn bind_root_cold_offsets(header: SeriesHeaderV3) -> SeriesColdV2SectionOffsets {
    SeriesColdV2SectionOffsets {
        keysets: header.keysets_offset,
        value_dicts: header.value_dicts_offset,
        keyset_blocks: header.keyset_blocks_offset,
        end: header.file_len,
    }
}

fn require_cold_offsets_match_header(
    header: SeriesHeaderV3,
    offsets: SeriesColdV2SectionOffsets,
) -> io::Result<()> {
    if offsets != bind_root_cold_offsets(header) {
        return Err(invalid_data(
            "schema-7 cold plan offsets disagree with the series root",
        ));
    }
    Ok(())
}

fn validate_root_binding(
    series: SeriesHeaderV3,
    chunk_index: ChunkOverflowRootV2,
) -> io::Result<()> {
    if series.num_series != chunk_index.series_count
        || series.chunk_index_root_crc32c != chunk_index.root_crc32c
        || series.chunk_index_file_len != chunk_index.file_len
    {
        return Err(invalid_data(
            "schema-7 series and chunk-index roots are not bound",
        ));
    }
    Ok(())
}

struct ColdPageCrcWriter<'a, W> {
    inner: &'a mut W,
    header: SeriesHeaderV3,
    descriptors: Vec<SeriesColdPageDescriptorV1>,
    page_index: u32,
    page_len: u32,
    emitted_len: u32,
    page: [u8; COLD_PAGE_BUFFER_LEN],
}

impl<'a, W: Write> ColdPageCrcWriter<'a, W> {
    fn new(inner: &'a mut W, header: SeriesHeaderV3) -> io::Result<Self> {
        let mut descriptors = Vec::new();
        descriptors
            .try_reserve_exact(
                usize::try_from(header.cold_page_count)
                    .map_err(|_| invalid_input("schema-7 cold-page count exceeds usize"))?,
            )
            .map_err(|_| resource_error("schema-7 cold descriptor allocation failed"))?;
        Ok(Self {
            inner,
            header,
            descriptors,
            page_index: 0,
            page_len: 0,
            emitted_len: 0,
            page: [0; COLD_PAGE_BUFFER_LEN],
        })
    }

    fn finish(mut self) -> io::Result<Vec<SeriesColdPageDescriptorV1>> {
        if self.page_len != 0 {
            self.emit_and_finish_page()?;
        }
        if self.page_index != self.header.cold_page_count {
            return Err(invalid_data(
                "schema-7 streamed cold-page count does not match root",
            ));
        }
        Ok(self.descriptors)
    }

    fn emit_pending(&mut self) -> io::Result<()> {
        let page_len = usize::try_from(self.page_len)
            .map_err(|_| invalid_input("schema-7 cold-page length exceeds usize"))?;
        let mut emitted_len = usize::try_from(self.emitted_len)
            .map_err(|_| invalid_input("schema-7 emitted cold-page length exceeds usize"))?;
        if emitted_len > page_len {
            return Err(invalid_data(
                "schema-7 emitted cold-page length exceeds buffered length",
            ));
        }
        while emitted_len < page_len {
            match self.inner.write(&self.page[emitted_len..page_len]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write buffered schema-7 cold page",
                    ));
                }
                Ok(written) if written <= page_len - emitted_len => {
                    emitted_len += written;
                    self.emitted_len = u32::try_from(emitted_len).map_err(|_| {
                        invalid_input("schema-7 emitted cold-page length exceeds u32")
                    })?;
                }
                Ok(_) => {
                    return Err(invalid_data(
                        "schema-7 cold-page sink reported an invalid write length",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn emit_and_finish_page(&mut self) -> io::Result<()> {
        self.emit_pending()?;
        let page_len = usize::try_from(self.page_len)
            .map_err(|_| invalid_input("schema-7 cold-page length exceeds usize"))?;
        let page_crc32c = crc32c_append(0, &self.page[..page_len]);
        let descriptor =
            SeriesColdPageDescriptorV1::new(self.header, self.page_index, page_crc32c)?;
        if descriptor.page_len != self.page_len {
            return Err(invalid_data(
                "schema-7 streamed cold-page length is noncanonical",
            ));
        }
        self.descriptors.push(descriptor);
        self.page_index = self
            .page_index
            .checked_add(1)
            .ok_or_else(|| invalid_input("schema-7 cold-page index exceeds u32"))?;
        self.page_len = 0;
        self.emitted_len = 0;
        Ok(())
    }
}

impl<W: Write> Write for ColdPageCrcWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }

        if usize::try_from(self.page_len)
            .map_err(|_| invalid_input("schema-7 cold-page length exceeds usize"))?
            == COLD_PAGE_BUFFER_LEN
        {
            self.emit_and_finish_page()?;
        }

        let page_len = usize::try_from(self.page_len)
            .map_err(|_| invalid_input("schema-7 cold-page length exceeds usize"))?;
        let remaining = COLD_PAGE_BUFFER_LEN
            .checked_sub(page_len)
            .ok_or_else(|| invalid_data("schema-7 cold-page length overflows"))?;
        let written = remaining.min(bytes.len());
        self.page[page_len..page_len + written].copy_from_slice(&bytes[..written]);
        self.page_len = self
            .page_len
            .checked_add(
                u32::try_from(written)
                    .map_err(|_| invalid_input("schema-7 cold write exceeds u32"))?,
            )
            .ok_or_else(|| invalid_input("schema-7 cold-page length exceeds u32"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit_pending()?;
        self.inner.flush()
    }
}

fn require_empty_output(writer: &mut (impl Seek + ?Sized), name: &'static str) -> io::Result<()> {
    if writer.stream_position()? != 0 || writer.seek(SeekFrom::End(0))? != 0 {
        return Err(invalid_input(match name {
            "schema-7 series output" => "schema-7 series output must be empty",
            _ => "schema-7 chunk-index output must be empty",
        }));
    }
    writer.seek(SeekFrom::Start(0))?;
    Ok(())
}

fn write_zeroes(writer: &mut impl Write, len: u64) -> io::Result<()> {
    let zeroes = [0u8; ZERO_WRITE_BUFFER_LEN];
    let mut remaining = len;
    while remaining != 0 {
        let write_len = usize::try_from(remaining.min(ZERO_WRITE_BUFFER_LEN as u64))
            .map_err(|_| invalid_input("schema-7 zero-fill length exceeds usize"))?;
        writer.write_all(&zeroes[..write_len])?;
        remaining -= write_len as u64;
    }
    Ok(())
}

fn require_position(
    writer: &mut (impl Seek + ?Sized),
    expected: u64,
    message: &'static str,
) -> io::Result<()> {
    if writer.stream_position()? != expected {
        return Err(invalid_data(message));
    }
    Ok(())
}

fn require_output_len(
    writer: &mut (impl Seek + ?Sized),
    expected: u64,
    message: &'static str,
) -> io::Result<()> {
    if writer.seek(SeekFrom::End(0))? != expected {
        return Err(invalid_data(message));
    }
    writer.seek(SeekFrom::Start(expected))?;
    Ok(())
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn resource_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use crc32c::crc32c;

    use super::super::{
        InlineChunkV3, OverflowChunksV3, SERIES_HOT_PAGE_LEN_V1, SeriesHotLocationV3,
        decode_series_hot_page_v1,
    };
    use super::*;
    use crate::storage::chunk::{ChunkEncoding, ChunkKind, decode_chunk_index_v2};
    use crate::storage::series::{
        SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM,
        SERIES_KIND_INT64, SERIES_KIND_SUMMARY,
    };

    const SEGMENT_START_MS: u64 = 1_000;
    const SEGMENT_END_MS: u64 = 1_000_000;
    const SCALAR_MAGIC: u32 = u32::from_le_bytes(*b"TSCL");

    #[derive(Debug, Clone)]
    struct Fixture {
        series: Vec<SeriesEntry>,
        chunks: Vec<Vec<ChunkIndexEntry>>,
        files: [Vec<u8>; 2],
    }

    impl Fixture {
        fn empty() -> Self {
            Self {
                series: Vec::new(),
                chunks: Vec::new(),
                files: [Vec::new(), Vec::new()],
            }
        }

        fn push_series(
            &mut self,
            series_id: u64,
            kind_mask: u8,
            labels: Vec<(u32, u32)>,
            chunks: Vec<ChunkIndexEntry>,
        ) {
            self.series.push(SeriesEntry {
                series_id,
                kind_mask,
                chunk_index: Default::default(),
                labels,
            });
            self.chunks.push(chunks);
        }

        fn append_chunk(
            &mut self,
            series_ref: u32,
            file_id: u8,
            kind: ChunkKind,
            min_time_ms: u64,
            max_time_ms: u64,
            scalar_body_len: Option<u32>,
        ) -> ChunkIndexEntry {
            let file = &mut self.files[usize::from(file_id)];
            let offset = file.len() as u64;
            let encoding = encoding_for(kind);
            let scalar_lane_len = scalar_body_len
                .map(|body_len| 16u32.checked_add(body_len).unwrap())
                .unwrap_or(0);
            let scalar_lane_offset = if scalar_lane_len == 0 { 0 } else { 40 };
            let header_len = 40u32.checked_add(scalar_lane_len).unwrap();
            let payload = [series_ref as u8 ^ 0xa5, kind as u8, 0x55, 0xaa];
            let payload_len = payload.len() as u32;
            let length = header_len.checked_add(payload_len).unwrap();

            let mut chunk = vec![0u8; length as usize];
            chunk[0] = kind as u8;
            chunk[1] = encoding as u8;
            put_u16_test(&mut chunk, 2, 0);
            put_u32_test(&mut chunk, 4, series_ref);
            put_u64_test(&mut chunk, 8, min_time_ms);
            put_u64_test(&mut chunk, 16, max_time_ms);
            put_u32_test(&mut chunk, 24, 1);
            put_u32_test(&mut chunk, 28, header_len);
            put_u32_test(&mut chunk, 32, payload_len);
            put_u32_test(&mut chunk, 36, crc32c(&payload));
            if let Some(body_len) = scalar_body_len {
                put_u32_test(&mut chunk, 40, SCALAR_MAGIC);
                put_u16_test(&mut chunk, 44, 1);
                put_u16_test(&mut chunk, 46, 0);
                put_u32_test(&mut chunk, 48, body_len);
                let body_start = 56usize;
                let body_end = body_start + body_len as usize;
                for (index, byte) in chunk[body_start..body_end].iter_mut().enumerate() {
                    *byte = index as u8 ^ 0x3c;
                }
                let body_crc32c = crc32c(&chunk[body_start..body_end]);
                put_u32_test(&mut chunk, 52, body_crc32c);
            }
            chunk[header_len as usize..].copy_from_slice(&payload);
            file.extend_from_slice(&chunk);
            ChunkIndexEntry {
                file_id,
                kind,
                flags: 0,
                min_time_ms,
                max_time_ms,
                offset,
                length,
                scalar_lane_offset,
                scalar_lane_len,
            }
        }
    }

    #[derive(Debug)]
    struct RunOutput {
        series: Vec<u8>,
        chunk_index: Vec<u8>,
        result: Schema7SeriesAssemblyResult,
    }

    fn run(fixture: &Fixture) -> io::Result<RunOutput> {
        let chunks_source = Cursor::new(fixture.files[0].clone());
        let ooo_source = Cursor::new(fixture.files[1].clone());
        let mut series = Cursor::new(Vec::new());
        let mut chunk_index = Cursor::new(Vec::new());
        let result = write_schema7_series_and_chunk_index(
            &mut series,
            &mut chunk_index,
            Schema7SeriesAssemblyInput {
                series_entries: &fixture.series,
                chunk_entries: &fixture.chunks,
                segment_start_ms: SEGMENT_START_MS,
                segment_end_ms: SEGMENT_END_MS,
                chunk_file_lens: [fixture.files[0].len() as u64, fixture.files[1].len() as u64],
                chunk_sources: [&chunks_source, &ooo_source],
            },
        )?;
        Ok(RunOutput {
            series: series.into_inner(),
            chunk_index: chunk_index.into_inner(),
            result,
        })
    }

    fn validate_streamed_output(fixture: &Fixture, output: &RunOutput) -> Vec<SeriesHotV3> {
        assert_eq!(
            output.series.len() as u64,
            output.result.stats.series_file_len
        );
        assert_eq!(
            output.chunk_index.len() as u64,
            output.result.stats.chunk_index_file_len
        );
        let root_len = output.result.series_header.hot_pages_offset as usize;
        let root = decode_series_root_v3(&output.series[..root_len]).unwrap();
        assert_eq!(root.header, output.result.series_header);

        let decoded_chunk_index = decode_chunk_index_v2(&output.chunk_index).unwrap();
        assert_eq!(decoded_chunk_index.root, output.result.chunk_index_root);
        assert_eq!(
            root.header.num_series,
            decoded_chunk_index.root.series_count
        );
        assert_eq!(
            root.header.chunk_index_root_crc32c,
            decoded_chunk_index.root.root_crc32c
        );
        assert_eq!(
            root.header.chunk_index_file_len,
            decoded_chunk_index.root.file_len
        );

        let mut records = Vec::new();
        for (page_index, descriptor) in root.hot_descriptors.iter().copied().enumerate() {
            let start = root.header.hot_pages_offset as usize + page_index * SERIES_HOT_PAGE_LEN_V1;
            let end = start + SERIES_HOT_PAGE_LEN_V1;
            let page = decode_series_hot_page_v1(
                root.header,
                page_index as u32,
                descriptor,
                &output.series[start..end],
                [fixture.files[0].len() as u64, fixture.files[1].len() as u64],
            )
            .unwrap();
            records.extend(page.records);
        }
        assert_eq!(records.len(), fixture.series.len());

        let mut overflow_ordinal = 0usize;
        for (series_ref, record) in records.iter().enumerate() {
            if let SeriesHotLocationV3::Overflow(overflow) = record.location {
                let locator = decoded_chunk_index.blob_locators[overflow_ordinal];
                assert_eq!(locator.series_ref, series_ref as u32);
                assert_eq!(locator.blob_offset, overflow.blob_offset);
                assert_eq!(locator.blob_len, overflow.blob_len);
                assert_eq!(locator.chunk_count, overflow.chunk_count);
                overflow_ordinal += 1;
            }
        }
        assert_eq!(overflow_ordinal, decoded_chunk_index.blobs.len());

        for descriptor in &root.cold_descriptors {
            let start = root.header.keysets_offset as usize
                + descriptor.page_index as usize * SERIES_COLD_PAGE_LEN_V1 as usize;
            let end = start + descriptor.page_len as usize;
            assert_eq!(crc32c(&output.series[start..end]), descriptor.page_crc32c);
        }

        let cold = SeriesColdV2Plan::build(&fixture.series).unwrap();
        let offsets = cold.section_offsets_at(root.header.keysets_offset).unwrap();
        let mut expected_cold = Vec::new();
        cold.write_sections_at(&mut expected_cold, offsets).unwrap();
        assert_eq!(
            &output.series[root.header.keysets_offset as usize..],
            expected_cold.as_slice()
        );
        records
    }

    fn assert_repeatable(fixture: &Fixture, first: &RunOutput) {
        let second = run(fixture).unwrap();
        assert_eq!(first.series, second.series);
        assert_eq!(first.chunk_index, second.chunk_index);
        assert_eq!(first.result, second.result);
    }

    fn cold_writer_header(cold_len: usize) -> SeriesHeaderV3 {
        assert!(cold_len >= 48);
        SeriesHeaderV3::new(SeriesHeaderV3Params {
            num_series: 1,
            num_keysets: 1,
            num_value_dicts: 1,
            chunk_index_root_crc32c: 0,
            keysets_len: 16,
            value_dicts_len: 16,
            keyset_blocks_len: (cold_len - 32) as u64,
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            chunk_index_file_len: 64,
        })
        .unwrap()
    }

    #[derive(Debug, Default)]
    struct RecordingWriter {
        bytes: Vec<u8>,
        write_sizes: Vec<usize>,
        flushes: usize,
    }

    impl Write for RecordingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.write_sizes.push(bytes.len());
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn cold_page_buffer_preserves_fragmented_cross_boundary_bytes_and_crcs() {
        let final_page_len = 137usize;
        let total_len = COLD_PAGE_BUFFER_LEN * 2 + final_page_len;
        let bytes: Vec<_> = (0..total_len)
            .map(|index| (index as u8).wrapping_mul(37).wrapping_add(11))
            .collect();
        let split_points = [
            0,
            1,
            3,
            COLD_PAGE_BUFFER_LEN - 3,
            COLD_PAGE_BUFFER_LEN + 5,
            COLD_PAGE_BUFFER_LEN * 2 + 1,
            total_len,
        ];
        let header = cold_writer_header(total_len);
        let mut output = RecordingWriter::default();
        let descriptors = {
            let mut writer = ColdPageCrcWriter::new(&mut output, header).unwrap();
            for bounds in split_points.windows(2) {
                writer.write_all(&bytes[bounds[0]..bounds[1]]).unwrap();
            }
            writer.finish().unwrap()
        };

        assert_eq!(output.bytes, bytes);
        assert_eq!(
            output.write_sizes,
            [COLD_PAGE_BUFFER_LEN, COLD_PAGE_BUFFER_LEN, final_page_len]
        );
        assert_eq!(descriptors.len(), 3);
        for (page_index, descriptor) in descriptors.iter().enumerate() {
            let start = page_index * COLD_PAGE_BUFFER_LEN;
            let end = (start + COLD_PAGE_BUFFER_LEN).min(bytes.len());
            assert_eq!(descriptor.page_index, page_index as u32);
            assert_eq!(descriptor.page_len, (end - start) as u32);
            assert_eq!(descriptor.page_crc32c, crc32c(&bytes[start..end]));
        }
    }

    #[test]
    fn cold_page_buffer_flushes_pending_bytes_without_splitting_the_page() {
        let bytes: Vec<_> = (0..257).map(|index| index as u8 ^ 0xa5).collect();
        let header = cold_writer_header(bytes.len());
        let mut output = RecordingWriter::default();
        let descriptors = {
            let mut writer = ColdPageCrcWriter::new(&mut output, header).unwrap();
            writer.write_all(&bytes[..17]).unwrap();
            assert!(writer.inner.bytes.is_empty());
            writer.flush().unwrap();
            assert_eq!(writer.inner.bytes, bytes[..17]);
            assert_eq!(writer.inner.flushes, 1);
            writer.write_all(&bytes[17..]).unwrap();
            writer.finish().unwrap()
        };

        assert_eq!(output.bytes, bytes);
        assert_eq!(output.write_sizes, [17, 240]);
        assert_eq!(output.flushes, 1);
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].page_len, 257);
        assert_eq!(descriptors[0].page_crc32c, crc32c(&bytes));
    }

    #[derive(Debug)]
    struct FailAfterWriter {
        bytes: Vec<u8>,
        fail_after: usize,
    }

    impl Write for FailAfterWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let remaining = self.fail_after.saturating_sub(self.bytes.len());
            if remaining == 0 {
                return Err(io::Error::other("injected cold-page write failure"));
            }
            let written = remaining.min(bytes.len());
            self.bytes.extend_from_slice(&bytes[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn cold_page_buffer_propagates_partial_sink_failure_without_a_descriptor() {
        let bytes = vec![0x5a; COLD_PAGE_BUFFER_LEN + 1];
        let header = cold_writer_header(bytes.len());
        let mut output = FailAfterWriter {
            bytes: Vec::new(),
            fail_after: 257,
        };
        let error = {
            let mut writer = ColdPageCrcWriter::new(&mut output, header).unwrap();
            let error = writer.write_all(&bytes).unwrap_err();
            assert_eq!(writer.page_len, COLD_PAGE_BUFFER_LEN as u32);
            assert_eq!(writer.emitted_len, 257);
            assert!(writer.descriptors.is_empty());
            error
        };

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(output.bytes, bytes[..257]);
    }

    #[test]
    fn empty_stream_is_canonical_repeatable_and_decoder_bound() {
        let fixture = Fixture::empty();
        let output = run(&fixture).unwrap();
        assert_repeatable(&fixture, &output);
        assert!(validate_streamed_output(&fixture, &output).is_empty());
        assert_eq!(output.series.len(), 4_120);
        assert_eq!(output.chunk_index.len(), 64);
        assert_eq!(output.result.stats.series_count, 0);
        assert_eq!(output.result.stats.hot_page_count, 0);
        assert_eq!(output.result.stats.cold_page_count, 1);
        assert_eq!(output.result.stats.peak_hot_records_buffered, 0);
        assert_eq!(output.result.stats.first_prefix_reads, 0);
        assert_eq!(output.result.stats.second_prefix_reads, 0);
        assert_eq!(crc32c(&output.series), 0x06e2_50d9);
        assert_eq!(crc32c(&output.chunk_index), 0x573e_a947);
    }

    #[test]
    fn one_record_stream_has_deterministic_golden_bytes_and_bound_decode() {
        let fixture = many_inline_fixture(1);
        let output = run(&fixture).unwrap();
        assert_repeatable(&fixture, &output);
        let records = validate_streamed_output(&fixture, &output);
        assert_eq!(records.len(), 1);
        assert!(matches!(
            records[0].location,
            SeriesHotLocationV3::Inline(InlineChunkV3 { file_id: 0, .. })
        ));
        assert_eq!(output.result.stats.inline_series_count, 1);
        assert_eq!(output.result.stats.overflow_series_count, 0);
        assert_eq!(output.result.stats.first_prefix_reads, 0);
        assert_eq!(output.result.stats.second_prefix_reads, 1);
        assert_eq!(output.series.len(), 20_569);
        assert_eq!(output.chunk_index.len(), 64);
        assert_eq!(crc32c(&output.series), 0x298e_c7b5);
        assert_eq!(crc32c(&output.chunk_index), 0x9301_79d2);
    }

    #[test]
    fn inline_and_ooo_records_are_repeatable_and_read_each_exact_prefix_once() {
        let mut fixture = Fixture::empty();
        let in_order = fixture.append_chunk(
            0,
            0,
            ChunkKind::Float,
            SEGMENT_START_MS,
            SEGMENT_START_MS + 1,
            None,
        );
        fixture.push_series(10, SERIES_KIND_FLOAT, vec![(1, 10)], vec![in_order]);
        let ooo = fixture.append_chunk(
            1,
            1,
            ChunkKind::Histogram,
            SEGMENT_START_MS + 2,
            SEGMENT_START_MS + 3,
            Some(8),
        );
        fixture.push_series(20, SERIES_KIND_HISTOGRAM, vec![(1, 20)], vec![ooo]);

        let output = run(&fixture).unwrap();
        assert_repeatable(&fixture, &output);
        let records = validate_streamed_output(&fixture, &output);
        assert_eq!(output.result.stats.inline_series_count, 2);
        assert_eq!(output.result.stats.overflow_series_count, 0);
        assert_eq!(output.result.stats.first_prefix_reads, 0);
        assert_eq!(output.result.stats.second_prefix_reads, 2);
        assert_eq!(output.result.stats.first_prefix_bytes, 0);
        assert_eq!(output.result.stats.second_prefix_bytes, 96);
        assert!(matches!(
            records[0].location,
            SeriesHotLocationV3::Inline(InlineChunkV3 { file_id: 0, .. })
        ));
        assert!(matches!(
            records[1].location,
            SeriesHotLocationV3::Inline(InlineChunkV3 { file_id: 1, .. })
        ));
        assert_eq!(output.series.len(), 20_575);
        assert_eq!(crc32c(&output.series), 0x9c01_795f);
        assert_eq!(crc32c(&output.chunk_index), 0xdaad_7e9c);
    }

    #[test]
    fn multi_chunk_series_streams_one_complete_bound_overflow_blob() {
        let mut fixture = Fixture::empty();
        let first = fixture.append_chunk(
            0,
            0,
            ChunkKind::ExponentialHistogram,
            SEGMENT_START_MS,
            SEGMENT_START_MS + 10,
            Some(4),
        );
        let second = fixture.append_chunk(
            0,
            1,
            ChunkKind::Summary,
            SEGMENT_START_MS + 20,
            SEGMENT_START_MS + 30,
            Some(12),
        );
        fixture.push_series(
            30,
            SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY,
            vec![(1, 30), (2, 31)],
            vec![first, second],
        );

        let output = run(&fixture).unwrap();
        assert_repeatable(&fixture, &output);
        let records = validate_streamed_output(&fixture, &output);
        assert_eq!(output.result.stats.inline_series_count, 0);
        assert_eq!(output.result.stats.overflow_series_count, 1);
        assert_eq!(output.result.stats.first_prefix_reads, 2);
        assert_eq!(output.result.stats.second_prefix_reads, 0);
        assert_eq!(output.result.stats.first_prefix_bytes, 112);
        assert_eq!(output.result.stats.second_prefix_bytes, 0);
        let decoded = decode_chunk_index_v2(&output.chunk_index).unwrap();
        assert_eq!(decoded.blobs.len(), 1);
        assert_eq!(decoded.blobs[0].entries.len(), 2);
        assert!(matches!(
            records[0].location,
            SeriesHotLocationV3::Overflow(OverflowChunksV3 { chunk_count: 2, .. })
        ));
        assert_eq!(output.series.len(), 20_594);
        assert_eq!(output.chunk_index.len(), 184);
        assert_eq!(crc32c(&output.series), 0x1f73_5abf);
        assert_eq!(crc32c(&output.chunk_index), 0x0ab9_a80c);
    }

    #[test]
    fn mixed_inline_and_overflow_series_read_every_prefix_exactly_once() {
        let mut fixture = Fixture::empty();
        let first_inline = fixture.append_chunk(
            0,
            0,
            ChunkKind::Float,
            SEGMENT_START_MS,
            SEGMENT_START_MS + 1,
            None,
        );
        fixture.push_series(10, SERIES_KIND_FLOAT, vec![(1, 10)], vec![first_inline]);

        let first_overflow = fixture.append_chunk(
            1,
            0,
            ChunkKind::ExponentialHistogram,
            SEGMENT_START_MS + 2,
            SEGMENT_START_MS + 3,
            Some(4),
        );
        let second_overflow = fixture.append_chunk(
            1,
            0,
            ChunkKind::Summary,
            SEGMENT_START_MS + 4,
            SEGMENT_START_MS + 5,
            Some(12),
        );
        fixture.push_series(
            20,
            SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY,
            vec![(1, 20)],
            vec![first_overflow, second_overflow],
        );

        let last_inline = fixture.append_chunk(
            2,
            1,
            ChunkKind::Int64,
            SEGMENT_START_MS + 6,
            SEGMENT_START_MS + 7,
            None,
        );
        fixture.push_series(30, SERIES_KIND_INT64, vec![(1, 30)], vec![last_inline]);

        let output = run(&fixture).unwrap();
        assert_repeatable(&fixture, &output);
        let records = validate_streamed_output(&fixture, &output);
        assert_eq!(records.len(), 3);
        assert_eq!(output.result.stats.inline_series_count, 2);
        assert_eq!(output.result.stats.overflow_series_count, 1);
        assert_eq!(output.result.stats.first_prefix_reads, 2);
        assert_eq!(output.result.stats.first_prefix_bytes, 112);
        assert_eq!(output.result.stats.second_prefix_reads, 2);
        assert_eq!(output.result.stats.second_prefix_bytes, 80);
        assert!(matches!(
            records[0].location,
            SeriesHotLocationV3::Inline(_)
        ));
        assert!(matches!(
            records[1].location,
            SeriesHotLocationV3::Overflow(_)
        ));
        assert!(matches!(
            records[2].location,
            SeriesHotLocationV3::Inline(_)
        ));
    }

    #[test]
    fn hot_page_boundary_at_409_and_410_records_is_exact_and_bounded() {
        let output_409 = run(&many_inline_fixture(409)).unwrap();
        assert_eq!(output_409.result.stats.hot_page_count, 1);
        assert_eq!(output_409.result.stats.peak_hot_records_buffered, 409);
        validate_streamed_output(&many_inline_fixture(409), &output_409);

        let fixture_410 = many_inline_fixture(410);
        let output_410 = run(&fixture_410).unwrap();
        assert_repeatable(&fixture_410, &output_410);
        let records = validate_streamed_output(&fixture_410, &output_410);
        assert_eq!(records.len(), 410);
        assert_eq!(output_410.result.stats.hot_page_count, 2);
        assert_eq!(output_410.result.stats.peak_hot_records_buffered, 409);
        let root = decode_series_root_v3(
            &output_410.series[..output_410.result.series_header.hot_pages_offset as usize],
        )
        .unwrap();
        assert_eq!(root.hot_descriptors[0].record_count, 409);
        assert_eq!(root.hot_descriptors[1].first_series_ref, 409);
        assert_eq!(root.hot_descriptors[1].record_count, 1);
        assert_eq!(output_409.series.len(), 23_019);
        assert_eq!(crc32c(&output_409.series), 0x53f7_f16c);
        assert_eq!(output_410.series.len(), 39_409);
        assert_eq!(crc32c(&output_410.series), 0x98af_0ea5);
    }

    #[test]
    fn cold_record_crossing_16k_is_authenticated_across_section_boundaries() {
        let mut fixture = Fixture::empty();
        let chunk = fixture.append_chunk(
            0,
            0,
            ChunkKind::Int64,
            SEGMENT_START_MS,
            SEGMENT_START_MS + 1,
            None,
        );
        let labels = (0..5_000u32)
            .map(|key| (key, key.wrapping_add(100_000)))
            .collect();
        fixture.push_series(40, SERIES_KIND_INT64, labels, vec![chunk]);

        let output = run(&fixture).unwrap();
        assert_repeatable(&fixture, &output);
        validate_streamed_output(&fixture, &output);
        assert!(output.result.stats.cold_page_count >= 3);
        let header = output.result.series_header;
        let keyset_entry_start = header.keysets_offset + 16;
        let keyset_entry_end = keyset_entry_start + 8 + 5_000 * 4;
        let first_cold_boundary = header.keysets_offset + SERIES_COLD_PAGE_LEN_V1;
        assert!(keyset_entry_start < first_cold_boundary);
        assert!(keyset_entry_end > first_cold_boundary);
        assert_eq!(output.series.len(), 145_544);
        assert_eq!(crc32c(&output.series), 0x7244_58d6);
        assert_eq!(output.result.stats.cold_page_count, 8);
    }

    #[test]
    fn substituted_prefix_and_exact_length_mismatches_fail_before_publication() {
        let mut fixture = many_inline_fixture(2);
        put_u32_test(&mut fixture.files[0], 4, 1);
        let error = run(&fixture).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let mut fixture = many_inline_fixture(1);
        fixture.files[0].push(0);
        fixture.chunks[0][0].length += 1;
        let error = run(&fixture).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let fixture = many_inline_fixture(1);
        let chunks_source = Cursor::new(fixture.files[0].clone());
        let ooo_source = Cursor::new(Vec::<u8>::new());
        let mut series = Cursor::new(Vec::new());
        let mut chunk_index = Cursor::new(Vec::new());
        let error = write_schema7_series_and_chunk_index(
            &mut series,
            &mut chunk_index,
            Schema7SeriesAssemblyInput {
                series_entries: &fixture.series,
                chunk_entries: &fixture.chunks,
                segment_start_ms: SEGMENT_START_MS,
                segment_end_ms: SEGMENT_END_MS,
                chunk_file_lens: [fixture.files[0].len() as u64 + 1, 0],
                chunk_sources: [&chunks_source, &ooo_source],
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(series.into_inner().is_empty());
        assert!(chunk_index.into_inner().is_empty());
    }

    #[test]
    fn one_chunk_offset_over_u32_streams_through_overflow_without_allocation() {
        let offset = u64::from(u32::MAX) + 1;
        let (entry, prefix) = standalone_chunk_prefix(
            0,
            ChunkKind::Float,
            SEGMENT_START_MS,
            SEGMENT_START_MS,
            offset,
        );
        let file_len = offset + u64::from(entry.length);
        let chunks_source = SparseSource::new(file_len, [(offset, prefix)]);
        let ooo_source = SparseSource::new(0, []);
        let series_entries = [SeriesEntry {
            series_id: 50,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(1, 1)],
        }];
        let chunk_entries = [vec![entry]];
        let mut series = Cursor::new(Vec::new());
        let mut chunk_index = Cursor::new(Vec::new());
        let result = write_schema7_series_and_chunk_index(
            &mut series,
            &mut chunk_index,
            Schema7SeriesAssemblyInput {
                series_entries: &series_entries,
                chunk_entries: &chunk_entries,
                segment_start_ms: SEGMENT_START_MS,
                segment_end_ms: SEGMENT_END_MS,
                chunk_file_lens: [file_len, 0],
                chunk_sources: [&chunks_source, &ooo_source],
            },
        )
        .unwrap();
        assert_eq!(result.stats.inline_series_count, 0);
        assert_eq!(result.stats.overflow_series_count, 1);
        assert_eq!(
            decode_chunk_index_v2(chunk_index.get_ref())
                .unwrap()
                .blobs
                .len(),
            1
        );
    }

    fn many_inline_fixture(series_count: u32) -> Fixture {
        let mut fixture = Fixture::empty();
        for series_ref in 0..series_count {
            let timestamp = SEGMENT_START_MS + u64::from(series_ref);
            let chunk =
                fixture.append_chunk(series_ref, 0, ChunkKind::Float, timestamp, timestamp, None);
            fixture.push_series(
                1_000 + u64::from(series_ref),
                SERIES_KIND_FLOAT,
                vec![(1, series_ref + 10)],
                vec![chunk],
            );
        }
        fixture
    }

    fn encoding_for(kind: ChunkKind) -> ChunkEncoding {
        match kind {
            ChunkKind::Float => ChunkEncoding::RawF64,
            ChunkKind::Int64 => ChunkEncoding::RawI64,
            ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary => {
                ChunkEncoding::SchemaVarLen
            }
        }
    }

    fn standalone_chunk_prefix(
        series_ref: u32,
        kind: ChunkKind,
        min_time_ms: u64,
        max_time_ms: u64,
        offset: u64,
    ) -> (ChunkIndexEntry, Vec<u8>) {
        let payload_len = 4u32;
        let length = 40 + payload_len;
        let mut prefix = vec![0u8; 40];
        prefix[0] = kind as u8;
        prefix[1] = encoding_for(kind) as u8;
        put_u32_test(&mut prefix, 4, series_ref);
        put_u64_test(&mut prefix, 8, min_time_ms);
        put_u64_test(&mut prefix, 16, max_time_ms);
        put_u32_test(&mut prefix, 24, 1);
        put_u32_test(&mut prefix, 28, 40);
        put_u32_test(&mut prefix, 32, payload_len);
        (
            ChunkIndexEntry {
                file_id: 0,
                kind,
                flags: 0,
                min_time_ms,
                max_time_ms,
                offset,
                length,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            },
            prefix,
        )
    }

    #[derive(Debug)]
    struct SparseSource {
        len: u64,
        ranges: BTreeMap<u64, Vec<u8>>,
    }

    impl SparseSource {
        fn new<const N: usize>(len: u64, ranges: [(u64, Vec<u8>); N]) -> Self {
            Self {
                len,
                ranges: ranges.into_iter().collect(),
            }
        }
    }

    impl SegmentIndexReadAt for SparseSource {
        fn len(&self) -> io::Result<u64> {
            Ok(self.len)
        }

        fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
            let source = self.ranges.get(&offset).ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "sparse test range is missing")
            })?;
            let source = source.get(..destination.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "sparse test range is short")
            })?;
            destination.copy_from_slice(source);
            Ok(())
        }
    }

    fn put_u16_test(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u32_test(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u64_test(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
}
