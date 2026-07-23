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
mod tests;
