//! Independent raw-byte decoder used by `chronoxide-query --verify-readbacks`.
//!
//! This deliberately does not call the production schema-7/8 metadata facade or
//! its series/chunk-index codecs. The low-level chunk payload decoder is used
//! only after the locator's independently authenticated 40/56-byte prefix has
//! been checked.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use chronoxide_core::storage::chunk::{
    ChunkIndexEntry, ChunkKind, ChunkRecord, ChunkSamples, read_chunk_record_at,
};
use chronoxide_core::storage::head::{
    CounterResetHint, OtlpAggregationTemporality, TypedSampleMetadata,
};
use chronoxide_core::storage::segment::{SegmentFile, SegmentMeta};
use crc32c::{crc32c, crc32c_append};

const SERIES_HEADER_LEN: usize = 176;
const DESCRIPTOR_LEN: usize = 16;
const HOT_PAGE_LEN: usize = 16_384;
const HOT_PAGE_HEADER_LEN: usize = 24;
const HOT_RECORD_LEN: usize = 40;
const HOT_RECORDS_PER_PAGE: u32 = 409;
const COLD_PAGE_LEN: u64 = 16_384;
const ROOT_ALIGNMENT: u64 = 4_096;
const ROOT_CRC_OFFSET: usize = 52;

const CHUNK_INDEX_ROOT_LEN: usize = 64;
const OVERFLOW_HEADER_LEN: usize = 32;
const OVERFLOW_ENTRY_LEN: usize = 44;
const MIN_OVERFLOW_BLOB_LEN: u64 = (OVERFLOW_HEADER_LEN + OVERFLOW_ENTRY_LEN) as u64;
const CHUNK_HEADER_LEN: u32 = 40;
const SCALAR_HEADER_LEN: u32 = 16;

const ENCODING_SCHEMA_VARLEN: u8 = 0;
const ENCODING_RAW_F64: u8 = 1;
const ENCODING_RAW_I64: u8 = 2;
const ENCODING_GORILLA: u8 = 3;
const ENCODING_INT_DELTA_ZIGZAG: u8 = 4;
const TYPED_CHUNK_FLAGS: u16 = 0x001e;

const SERIES_MAGIC: u32 = u32::from_le_bytes(*b"SERI");
const HOT_PAGE_MAGIC: u32 = u32::from_le_bytes(*b"SHP7");
const CHUNK_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"CHIX");
const OVERFLOW_MAGIC: u32 = u32::from_le_bytes(*b"COF7");
const SCALAR_MAGIC: u32 = u32::from_le_bytes(*b"TSCL");

#[derive(Debug, Clone)]
pub(super) struct OracleSeries {
    pub(super) series_ref: u32,
    pub(super) series_id: u64,
    keyset_id: u32,
    row: u32,
    pub(super) chunks: Vec<OracleChunk>,
}

#[derive(Debug, Clone)]
pub(super) struct OracleChunk {
    pub(super) entry: ChunkIndexEntry,
    indexed_prefix_crc32c: u32,
}

#[derive(Debug, Clone, Copy)]
struct Header {
    num_series: u32,
    page_count: u32,
    num_keysets: u32,
    num_value_dicts: u32,
    chunk_index_root_crc32c: u32,
    root_crc32c: u32,
    cold_page_count: u32,
    directory_offset: u64,
    directory_len: u64,
    hot_pages_offset: u64,
    hot_pages_len: u64,
    keysets_offset: u64,
    keysets_len: u64,
    value_dicts_offset: u64,
    value_dicts_len: u64,
    keyset_blocks_offset: u64,
    keyset_blocks_len: u64,
    segment_start_ms: u64,
    segment_end_ms: u64,
    chunk_index_file_len: u64,
    file_len: u64,
}

#[derive(Debug, Clone, Copy)]
struct HotDescriptor {
    first_series_ref: u32,
    record_count: u32,
    page_crc32c: u32,
}

#[derive(Debug, Clone, Copy)]
struct ColdDescriptor {
    page_len: u32,
    page_crc32c: u32,
}

#[derive(Debug, Clone)]
struct HotRecord {
    series_id: u64,
    keyset_id: u32,
    row: u32,
    kind_mask: u8,
    location: HotLocation,
}

#[derive(Debug, Clone)]
enum HotLocation {
    Inline(OracleChunk),
    Overflow {
        blob_offset: u64,
        blob_len: u32,
        chunk_count: u32,
    },
}

#[derive(Debug, Clone, Copy)]
struct ValueDictionaryMeta {
    key_sym: u32,
    cardinality: u32,
    values_offset: u64,
}

#[derive(Debug, Clone, Copy)]
struct VerifiedChunkPrefix {
    flags: u16,
    num_points: u32,
}

pub(super) struct Schema7OracleSegment {
    series: File,
    chunk_index: File,
    chunks: [File; 2],
    chunk_file_lens: [u64; 2],
    header: Header,
    hot_descriptors: Vec<HotDescriptor>,
    cold_descriptors: Vec<ColdDescriptor>,
    cached_hot_page: Option<(u32, Vec<HotRecord>)>,
    cached_cold_pages: BTreeMap<u32, Vec<u8>>,
    value_dictionary_meta: Option<Vec<ValueDictionaryMeta>>,
    value_dictionaries: BTreeMap<u32, Vec<u32>>,
}

impl Schema7OracleSegment {
    pub(super) fn open(segment_dir: &Path, meta: &SegmentMeta) -> io::Result<Self> {
        let mut series = File::open(segment_dir.join(SegmentFile::Series.filename()))?;
        let series_len = series.metadata()?.len();
        let mut chunk_index = File::open(segment_dir.join(SegmentFile::ChunkIndex.filename()))?;
        let chunk_index_len = chunk_index.metadata()?.len();
        let chunks = [
            File::open(segment_dir.join(SegmentFile::Chunks.filename()))?,
            File::open(segment_dir.join(SegmentFile::OooChunks.filename()))?,
        ];
        let chunk_file_lens = [chunks[0].metadata()?.len(), chunks[1].metadata()?.len()];

        let header_bytes = read_exact_at(&mut series, 0, SERIES_HEADER_LEN)?;
        let header = decode_header(&header_bytes, series_len, chunk_index_len, meta)?;
        let (hot_descriptors, cold_descriptors) = authenticate_series_root(&mut series, header)?;
        authenticate_chunk_index_root(&mut chunk_index, chunk_index_len, header)?;

        let mut oracle = Self {
            series,
            chunk_index,
            chunks,
            chunk_file_lens,
            header,
            hot_descriptors,
            cold_descriptors,
            cached_hot_page: None,
            cached_cold_pages: BTreeMap::new(),
            value_dictionary_meta: None,
            value_dictionaries: BTreeMap::new(),
        };
        oracle.validate_canonical_empty_sections()?;
        Ok(oracle)
    }

    pub(super) const fn len(&self) -> u32 {
        self.header.num_series
    }

    pub(super) fn read_series(&mut self, series_ref: u32) -> io::Result<OracleSeries> {
        if series_ref >= self.header.num_series {
            return Err(invalid_data("schema-7 oracle series_ref is out of bounds"));
        }
        let page_index = series_ref / HOT_RECORDS_PER_PAGE;
        if self
            .cached_hot_page
            .as_ref()
            .is_none_or(|(cached, _)| *cached != page_index)
        {
            let records = self.load_hot_page(page_index)?;
            self.cached_hot_page = Some((page_index, records));
        }
        let ordinal = usize::try_from(series_ref % HOT_RECORDS_PER_PAGE)
            .map_err(|_| invalid_data("schema-7 oracle hot ordinal exceeds usize"))?;
        let record = self
            .cached_hot_page
            .as_ref()
            .and_then(|(_, records)| records.get(ordinal))
            .cloned()
            .ok_or_else(|| invalid_data("schema-7 oracle hot record is missing"))?;
        let (chunks, is_overflow) = match record.location {
            HotLocation::Inline(chunk) => (vec![chunk], false),
            HotLocation::Overflow {
                blob_offset,
                blob_len,
                chunk_count,
            } => (
                self.read_overflow_blob(series_ref, blob_offset, blob_len, chunk_count)?,
                true,
            ),
        };
        let actual_kind_mask = chunks
            .iter()
            .fold(0u8, |mask, chunk| mask | (1u8 << (chunk.entry.kind as u8)));
        if actual_kind_mask != record.kind_mask {
            return Err(invalid_data(
                "schema-7 oracle chunk kinds do not match the hot kind mask",
            ));
        }
        for chunk in &chunks {
            if chunk.entry.min_time_ms < self.header.segment_start_ms
                || chunk.entry.max_time_ms >= self.header.segment_end_ms
            {
                return Err(invalid_data(
                    "schema-7 oracle chunk times exceed the segment bounds",
                ));
            }
        }
        if is_overflow
            && chunks.len() == 1
            && chunk_is_inline_representable(self.header, &chunks[0].entry)?
        {
            return Err(invalid_data(
                "schema-7 oracle one-chunk overflow should be inline",
            ));
        }
        Ok(OracleSeries {
            series_ref,
            series_id: record.series_id,
            keyset_id: record.keyset_id,
            row: record.row,
            chunks,
        })
    }

    pub(super) fn read_label_ids(&mut self, series: &OracleSeries) -> io::Result<Vec<(u32, u32)>> {
        let keys = self.read_keyset(series.keyset_id)?;
        let (rows, widths, row_bytes) = self.read_keyset_row(series.keyset_id, series.row)?;
        if series.row >= rows || widths.len() != keys.len() {
            return Err(invalid_data(
                "schema-7 oracle keyset block does not match its keyset",
            ));
        }

        let mut cursor = 0usize;
        let mut labels = Vec::with_capacity(keys.len());
        for (key_sym, width) in keys.into_iter().zip(widths) {
            let dictionary = self.value_dictionary(key_sym)?;
            let expected_width = canonical_code_width(dictionary.len())?;
            if width != expected_width {
                return Err(invalid_data(
                    "schema-7 oracle value-code width is noncanonical",
                ));
            }
            let code = read_code(&row_bytes, &mut cursor, width)?;
            let value_sym = dictionary
                .get(
                    usize::try_from(code)
                        .map_err(|_| invalid_data("schema-7 oracle value code exceeds usize"))?,
                )
                .copied()
                .ok_or_else(|| invalid_data("schema-7 oracle value code is out of bounds"))?;
            labels.push((key_sym, value_sym));
        }
        if cursor != row_bytes.len() {
            return Err(invalid_data(
                "schema-7 oracle keyset row has trailing bytes",
            ));
        }
        Ok(labels)
    }

    pub(super) fn read_verified_chunk(
        &mut self,
        series_ref: u32,
        chunk: &OracleChunk,
    ) -> io::Result<ChunkRecord> {
        let verified =
            verify_indexed_prefix(&mut self.chunks, self.chunk_file_lens, series_ref, chunk)?;
        let file = self
            .chunks
            .get_mut(usize::from(chunk.entry.file_id))
            .ok_or_else(|| invalid_data("schema-7 oracle chunk file_id is invalid"))?;
        let record = read_chunk_record_at(file, chunk.entry.offset, chunk.entry.length)?;
        validate_decoded_chunk_flags(verified, &record)?;
        Ok(record)
    }

    fn load_hot_page(&mut self, page_index: u32) -> io::Result<Vec<HotRecord>> {
        let descriptor = *self
            .hot_descriptors
            .get(
                usize::try_from(page_index)
                    .map_err(|_| invalid_data("schema-7 oracle hot page index exceeds usize"))?,
            )
            .ok_or_else(|| invalid_data("schema-7 oracle hot descriptor is missing"))?;
        let offset = self
            .header
            .hot_pages_offset
            .checked_add(
                u64::from(page_index)
                    .checked_mul(HOT_PAGE_LEN as u64)
                    .ok_or_else(|| invalid_data("schema-7 oracle hot page offset overflows"))?,
            )
            .ok_or_else(|| invalid_data("schema-7 oracle hot page offset overflows"))?;
        let bytes = read_exact_at(&mut self.series, offset, HOT_PAGE_LEN)?;
        if crc32c(&bytes) != descriptor.page_crc32c {
            return Err(invalid_data("schema-7 oracle hot page CRC mismatch"));
        }
        if read_u32(&bytes, 0)? != HOT_PAGE_MAGIC
            || read_u16(&bytes, 4)? != 1
            || read_u16(&bytes, 6)? != 0
            || read_u32(&bytes, 8)? != page_index
            || read_u32(&bytes, 12)? != descriptor.first_series_ref
            || read_u32(&bytes, 16)? != descriptor.record_count
            || read_u32(&bytes, 20)? != 0
        {
            return Err(invalid_data(
                "schema-7 oracle hot page header is noncanonical",
            ));
        }
        let record_count = usize::try_from(descriptor.record_count)
            .map_err(|_| invalid_data("schema-7 oracle hot record count exceeds usize"))?;
        let records_end = HOT_PAGE_HEADER_LEN
            .checked_add(
                record_count
                    .checked_mul(HOT_RECORD_LEN)
                    .ok_or_else(|| invalid_data("schema-7 oracle hot records length overflows"))?,
            )
            .ok_or_else(|| invalid_data("schema-7 oracle hot records end overflows"))?;
        if bytes[records_end..].iter().any(|byte| *byte != 0) {
            return Err(invalid_data("schema-7 oracle hot page padding is nonzero"));
        }
        let mut records = Vec::with_capacity(record_count);
        for ordinal in 0..record_count {
            let start = HOT_PAGE_HEADER_LEN + ordinal * HOT_RECORD_LEN;
            let series_ref = descriptor
                .first_series_ref
                .checked_add(
                    u32::try_from(ordinal)
                        .map_err(|_| invalid_data("schema-7 oracle hot ordinal exceeds u32"))?,
                )
                .ok_or_else(|| invalid_data("schema-7 oracle series_ref overflows"))?;
            records.push(decode_hot_record(
                &bytes[start..start + HOT_RECORD_LEN],
                series_ref,
                self.header,
                self.chunk_file_lens,
            )?);
        }
        Ok(records)
    }

    fn read_overflow_blob(
        &mut self,
        series_ref: u32,
        blob_offset: u64,
        blob_len: u32,
        chunk_count: u32,
    ) -> io::Result<Vec<OracleChunk>> {
        let body_len = chunk_count
            .checked_mul(OVERFLOW_ENTRY_LEN as u32)
            .ok_or_else(|| invalid_data("schema-7 oracle blob length overflows"))?;
        let expected_blob_len = (OVERFLOW_HEADER_LEN as u32)
            .checked_add(body_len)
            .ok_or_else(|| invalid_data("schema-7 oracle blob length overflows"))?;
        if chunk_count == 0 || blob_len != expected_blob_len {
            return Err(invalid_data(
                "schema-7 oracle overflow locator is noncanonical",
            ));
        }
        let end = blob_offset
            .checked_add(u64::from(blob_len))
            .ok_or_else(|| invalid_data("schema-7 oracle overflow range overflows"))?;
        if blob_offset < CHUNK_INDEX_ROOT_LEN as u64 || end > self.header.chunk_index_file_len {
            return Err(invalid_data(
                "schema-7 oracle overflow range is out of bounds",
            ));
        }
        let mut bytes = read_exact_at(
            &mut self.chunk_index,
            blob_offset,
            usize::try_from(blob_len)
                .map_err(|_| invalid_data("schema-7 oracle blob length exceeds usize"))?,
        )?;
        let stored_crc = read_u32(&bytes, 28)?;
        bytes[28..32].fill(0);
        if crc32c(&bytes) != stored_crc {
            return Err(invalid_data("schema-7 oracle overflow blob CRC mismatch"));
        }
        if read_u32(&bytes, 0)? != OVERFLOW_MAGIC
            || read_u16(&bytes, 4)? != 1
            || read_u16(&bytes, 6)? != 0
            || read_u32(&bytes, 8)? != OVERFLOW_HEADER_LEN as u32
            || read_u32(&bytes, 12)? != series_ref
            || read_u32(&bytes, 16)? != chunk_count
            || read_u32(&bytes, 20)? != 0
            || read_u32(&bytes, 24)? != body_len
        {
            return Err(invalid_data(
                "schema-7 oracle overflow blob header is noncanonical",
            ));
        }
        let mut chunks = Vec::with_capacity(
            usize::try_from(chunk_count)
                .map_err(|_| invalid_data("schema-7 oracle chunk count exceeds usize"))?,
        );
        let mut previous = None;
        for ordinal in 0..chunk_count {
            let start = usize::try_from(ordinal)
                .map_err(|_| invalid_data("schema-7 oracle chunk ordinal exceeds usize"))?
                .checked_mul(OVERFLOW_ENTRY_LEN)
                .and_then(|offset| OVERFLOW_HEADER_LEN.checked_add(offset))
                .ok_or_else(|| invalid_data("schema-7 oracle overflow entry offset overflows"))?;
            let end = start
                .checked_add(OVERFLOW_ENTRY_LEN)
                .ok_or_else(|| invalid_data("schema-7 oracle overflow entry range overflows"))?;
            let chunk = decode_overflow_entry(
                bytes.get(start..end).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "schema-7 oracle overflow entry is truncated",
                    )
                })?,
                self.chunk_file_lens,
            )?;
            let order = (
                chunk.entry.file_id,
                chunk.entry.min_time_ms,
                chunk.entry.max_time_ms,
                chunk.entry.offset,
            );
            if previous.is_some_and(|previous| previous >= order) {
                return Err(invalid_data(
                    "schema-7 oracle overflow chunks are not strictly ordered",
                ));
            }
            previous = Some(order);
            chunks.push(chunk);
        }
        Ok(chunks)
    }

    fn read_keyset(&mut self, keyset_id: u32) -> io::Result<Vec<u32>> {
        let range = self.entry_range(
            self.header.keysets_offset,
            self.header.value_dicts_offset,
            self.header.num_keysets,
            keyset_id,
        )?;
        let bytes = self.read_cold_range(range.0, range.1)?;
        if bytes.len() < 8 || read_u32(&bytes, 4)? != 0 {
            return Err(invalid_data("schema-7 oracle keyset header is invalid"));
        }
        let count = usize::try_from(read_u32(&bytes, 0)?)
            .map_err(|_| invalid_data("schema-7 oracle key count exceeds usize"))?;
        let expected_len = count
            .checked_mul(4)
            .and_then(|len| 8usize.checked_add(len))
            .ok_or_else(|| invalid_data("schema-7 oracle keyset length overflows"))?;
        if bytes.len() != expected_len {
            return Err(invalid_data("schema-7 oracle keyset length mismatch"));
        }
        let mut keys = Vec::with_capacity(count);
        for index in 0..count {
            let key = read_u32(&bytes, 8 + index * 4)?;
            if keys.last().is_some_and(|previous| *previous >= key) {
                return Err(invalid_data(
                    "schema-7 oracle keyset is not strictly ordered",
                ));
            }
            keys.push(key);
        }
        Ok(keys)
    }

    fn read_keyset_row(&mut self, keyset_id: u32, row: u32) -> io::Result<(u32, Vec<u8>, Vec<u8>)> {
        let range = self.entry_range(
            self.header.keyset_blocks_offset,
            self.header.file_len,
            self.header.num_keysets,
            keyset_id,
        )?;
        let fixed_end = range
            .0
            .checked_add(16)
            .ok_or_else(|| invalid_data("schema-7 oracle keyset block header overflows"))?;
        if fixed_end > range.1 {
            return Err(invalid_data(
                "schema-7 oracle keyset block header exceeds its entry",
            ));
        }
        let fixed = self.read_cold_range(range.0, fixed_end)?;
        let rows = read_u32(&fixed, 0)?;
        let key_count = read_u32(&fixed, 4)?;
        let row_len = read_u32(&fixed, 8)?;
        let data_len = read_u32(&fixed, 12)?;
        if rows == 0 || rows.checked_mul(row_len) != Some(data_len) || row >= rows {
            return Err(invalid_data(
                "schema-7 oracle keyset block shape is invalid",
            ));
        }
        let widths_end = range
            .0
            .checked_add(
                16u64
                    .checked_add(u64::from(key_count))
                    .ok_or_else(|| invalid_data("schema-7 oracle widths range overflows"))?,
            )
            .ok_or_else(|| invalid_data("schema-7 oracle widths range overflows"))?;
        let widths_start = range
            .0
            .checked_add(16)
            .ok_or_else(|| invalid_data("schema-7 oracle widths range overflows"))?;
        if widths_end > range.1 {
            return Err(invalid_data(
                "schema-7 oracle keyset block widths exceed its entry",
            ));
        }
        let widths = self.read_cold_range(widths_start, widths_end)?;
        let encoded_row_len = widths.iter().try_fold(0u32, |total, width| {
            total
                .checked_add(u32::from(*width))
                .ok_or_else(|| invalid_data("schema-7 oracle row width sum overflows"))
        })?;
        if widths.iter().any(|width| !matches!(width, 0 | 1 | 2 | 4))
            || encoded_row_len != row_len
            || widths_end.checked_add(u64::from(data_len)) != Some(range.1)
        {
            return Err(invalid_data(
                "schema-7 oracle keyset block encoding is invalid",
            ));
        }
        let row_start = widths_end
            .checked_add(
                u64::from(row)
                    .checked_mul(u64::from(row_len))
                    .ok_or_else(|| invalid_data("schema-7 oracle row offset overflows"))?,
            )
            .ok_or_else(|| invalid_data("schema-7 oracle row offset overflows"))?;
        let row_end = row_start
            .checked_add(u64::from(row_len))
            .ok_or_else(|| invalid_data("schema-7 oracle row range overflows"))?;
        let row_bytes = self.read_cold_range(row_start, row_end)?;
        Ok((rows, widths, row_bytes))
    }

    fn value_dictionary(&mut self, key_sym: u32) -> io::Result<&[u32]> {
        if !self.value_dictionaries.contains_key(&key_sym) {
            self.load_value_dictionary_meta()?;
            let meta = self
                .value_dictionary_meta
                .as_ref()
                .and_then(|metas| {
                    metas
                        .binary_search_by_key(&key_sym, |meta| meta.key_sym)
                        .ok()
                        .and_then(|index| metas.get(index))
                })
                .copied()
                .ok_or_else(|| invalid_data("schema-7 oracle value dictionary is missing"))?;
            let len = usize::try_from(meta.cardinality)
                .map_err(|_| invalid_data("schema-7 oracle dictionary length exceeds usize"))?;
            let values_len = u64::from(meta.cardinality)
                .checked_mul(4)
                .ok_or_else(|| invalid_data("schema-7 oracle dictionary length overflows"))?;
            let values_end = meta
                .values_offset
                .checked_add(values_len)
                .ok_or_else(|| invalid_data("schema-7 oracle dictionary range overflows"))?;
            let bytes = self.read_cold_range(meta.values_offset, values_end)?;
            let mut values = Vec::with_capacity(len);
            for index in 0..len {
                let value = read_u32(&bytes, index * 4)?;
                if values.last().is_some_and(|previous| *previous >= value) {
                    return Err(invalid_data(
                        "schema-7 oracle dictionary is not strictly ordered",
                    ));
                }
                values.push(value);
            }
            self.value_dictionaries.insert(key_sym, values);
        }
        self.value_dictionaries
            .get(&key_sym)
            .map(Vec::as_slice)
            .ok_or_else(|| invalid_data("schema-7 oracle dictionary cache is missing"))
    }

    fn load_value_dictionary_meta(&mut self) -> io::Result<()> {
        if self.value_dictionary_meta.is_some() {
            return Ok(());
        }
        let mut metas = Vec::with_capacity(
            usize::try_from(self.header.num_value_dicts)
                .map_err(|_| invalid_data("schema-7 oracle dictionary count exceeds usize"))?,
        );
        for dictionary_id in 0..self.header.num_value_dicts {
            let range = self.entry_range(
                self.header.value_dicts_offset,
                self.header.keyset_blocks_offset,
                self.header.num_value_dicts,
                dictionary_id,
            )?;
            let fixed_end = range
                .0
                .checked_add(8)
                .ok_or_else(|| invalid_data("schema-7 oracle dictionary header overflows"))?;
            if fixed_end > range.1 {
                return Err(invalid_data(
                    "schema-7 oracle dictionary header exceeds its entry",
                ));
            }
            let fixed = self.read_cold_range(range.0, fixed_end)?;
            let key_sym = read_u32(&fixed, 0)?;
            let cardinality = read_u32(&fixed, 4)?;
            let values_len = u64::from(cardinality)
                .checked_mul(4)
                .ok_or_else(|| invalid_data("schema-7 oracle dictionary length overflows"))?;
            let expected_end = range
                .0
                .checked_add(8)
                .and_then(|start| start.checked_add(values_len));
            if cardinality == 0
                || expected_end != Some(range.1)
                || metas
                    .last()
                    .is_some_and(|previous: &ValueDictionaryMeta| previous.key_sym >= key_sym)
            {
                return Err(invalid_data(
                    "schema-7 oracle value dictionary metadata is invalid",
                ));
            }
            metas.push(ValueDictionaryMeta {
                key_sym,
                cardinality,
                values_offset: fixed_end,
            });
        }
        self.value_dictionary_meta = Some(metas);
        Ok(())
    }

    fn entry_range(
        &mut self,
        section_start: u64,
        section_end: u64,
        count: u32,
        index: u32,
    ) -> io::Result<(u64, u64)> {
        if index >= count {
            return Err(invalid_data("schema-7 oracle cold entry is out of bounds"));
        }
        let table_len = u64::from(count)
            .checked_add(1)
            .and_then(|value| value.checked_mul(8))
            .ok_or_else(|| invalid_data("schema-7 oracle offset table length overflows"))?;
        let entries_start = section_start
            .checked_add(table_len)
            .ok_or_else(|| invalid_data("schema-7 oracle entries offset overflows"))?;
        if entries_start > section_end {
            return Err(invalid_data(
                "schema-7 oracle cold section is shorter than its offset table",
            ));
        }
        let pair_offset = section_start
            .checked_add(
                u64::from(index)
                    .checked_mul(8)
                    .ok_or_else(|| invalid_data("schema-7 oracle offset pair overflows"))?,
            )
            .ok_or_else(|| invalid_data("schema-7 oracle offset pair overflows"))?;
        let pair_end = pair_offset
            .checked_add(16)
            .ok_or_else(|| invalid_data("schema-7 oracle offset pair range overflows"))?;
        let pair = self.read_cold_range(pair_offset, pair_end)?;
        let start = read_u64(&pair, 0)?;
        let end = read_u64(&pair, 8)?;
        if start >= end
            || start < entries_start
            || end > section_end
            || (index == 0 && start != entries_start)
            || (index + 1 == count && end != section_end)
        {
            return Err(invalid_data(
                "schema-7 oracle cold entry offsets are invalid",
            ));
        }
        Ok((start, end))
    }

    fn read_cold_range(&mut self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if start > end || start < self.header.keysets_offset || end > self.header.file_len {
            return Err(invalid_data("schema-7 oracle cold range is out of bounds"));
        }
        if start == end {
            return Ok(Vec::new());
        }
        let first_page = (start - self.header.keysets_offset) / COLD_PAGE_LEN;
        let last_page = (end - 1 - self.header.keysets_offset) / COLD_PAGE_LEN;
        for page in first_page..=last_page {
            let page_index = u32::try_from(page)
                .map_err(|_| invalid_data("schema-7 oracle cold page index exceeds u32"))?;
            if !self.cached_cold_pages.contains_key(&page_index) {
                let descriptor = *self
                    .cold_descriptors
                    .get(usize::try_from(page_index).map_err(|_| {
                        invalid_data("schema-7 oracle cold page index exceeds usize")
                    })?)
                    .ok_or_else(|| invalid_data("schema-7 oracle cold descriptor is missing"))?;
                let page_offset = page
                    .checked_mul(COLD_PAGE_LEN)
                    .ok_or_else(|| invalid_data("schema-7 oracle cold page offset overflows"))?;
                let offset = self
                    .header
                    .keysets_offset
                    .checked_add(page_offset)
                    .ok_or_else(|| invalid_data("schema-7 oracle cold page offset overflows"))?;
                let bytes = read_exact_at(
                    &mut self.series,
                    offset,
                    usize::try_from(descriptor.page_len).map_err(|_| {
                        invalid_data("schema-7 oracle cold page length exceeds usize")
                    })?,
                )?;
                if crc32c(&bytes) != descriptor.page_crc32c {
                    return Err(invalid_data("schema-7 oracle cold page CRC mismatch"));
                }
                self.cached_cold_pages.insert(page_index, bytes);
            }
        }

        let output_len = usize::try_from(end - start)
            .map_err(|_| invalid_data("schema-7 oracle cold range exceeds usize"))?;
        let mut output = Vec::with_capacity(output_len);
        let mut cursor = start;
        while cursor < end {
            let page = (cursor - self.header.keysets_offset) / COLD_PAGE_LEN;
            let page_index = u32::try_from(page)
                .map_err(|_| invalid_data("schema-7 oracle cold page index exceeds u32"))?;
            let bytes = self
                .cached_cold_pages
                .get(&page_index)
                .ok_or_else(|| invalid_data("schema-7 oracle cold page cache is missing"))?;
            let page_start = page
                .checked_mul(COLD_PAGE_LEN)
                .and_then(|offset| self.header.keysets_offset.checked_add(offset))
                .ok_or_else(|| invalid_data("schema-7 oracle cold page offset overflows"))?;
            let within = usize::try_from(cursor - page_start)
                .map_err(|_| invalid_data("schema-7 oracle cold offset exceeds usize"))?;
            let available = bytes
                .len()
                .checked_sub(within)
                .ok_or_else(|| invalid_data("schema-7 oracle cold offset exceeds its page"))?;
            let remaining = end
                .checked_sub(cursor)
                .ok_or_else(|| invalid_data("schema-7 oracle cold copy range is reversed"))?;
            let take = usize::try_from(
                remaining.min(
                    u64::try_from(available)
                        .map_err(|_| invalid_data("schema-7 oracle cold page exceeds u64"))?,
                ),
            )
            .map_err(|_| invalid_data("schema-7 oracle cold copy length exceeds usize"))?;
            if take == 0 {
                return Err(invalid_data(
                    "schema-7 oracle cold page does not cover the requested range",
                ));
            }
            let slice_end = within
                .checked_add(take)
                .ok_or_else(|| invalid_data("schema-7 oracle cold copy range overflows"))?;
            output.extend_from_slice(&bytes[within..slice_end]);
            cursor = cursor
                .checked_add(
                    u64::try_from(take)
                        .map_err(|_| invalid_data("schema-7 oracle cold copy exceeds u64"))?,
                )
                .ok_or_else(|| invalid_data("schema-7 oracle cold cursor overflows"))?;
        }
        Ok(output)
    }

    fn validate_canonical_empty_sections(&mut self) -> io::Result<()> {
        if self.header.num_series != 0 {
            return Ok(());
        }
        for (start, end) in [
            (self.header.keysets_offset, self.header.value_dicts_offset),
            (
                self.header.value_dicts_offset,
                self.header.keyset_blocks_offset,
            ),
            (self.header.keyset_blocks_offset, self.header.file_len),
        ] {
            let bytes = self.read_cold_range(start, end)?;
            if bytes.len() != 8 || read_u64(&bytes, 0)? != end {
                return Err(invalid_data(
                    "schema-7 oracle empty cold offset table is noncanonical",
                ));
            }
        }
        Ok(())
    }
}

fn decode_header(
    bytes: &[u8],
    series_file_len: u64,
    chunk_index_file_len: u64,
    meta: &SegmentMeta,
) -> io::Result<Header> {
    if read_u32(bytes, 0)? != SERIES_MAGIC
        || read_u16(bytes, 4)? != 3
        || read_u16(bytes, 6)? != 0
        || read_u32(bytes, 8)? != SERIES_HEADER_LEN as u32
        || read_u32(bytes, 12)? != DESCRIPTOR_LEN as u32
        || read_u32(bytes, 16)? != HOT_PAGE_LEN as u32
        || read_u32(bytes, 20)? != HOT_PAGE_HEADER_LEN as u32
        || read_u32(bytes, 24)? != HOT_RECORD_LEN as u32
        || read_u32(bytes, 28)? != HOT_RECORDS_PER_PAGE
        || read_u32(bytes, 56)? != COLD_PAGE_LEN as u32
    {
        return Err(invalid_data("schema-7 oracle series header is unsupported"));
    }
    let header = Header {
        num_series: read_u32(bytes, 32)?,
        page_count: read_u32(bytes, 36)?,
        num_keysets: read_u32(bytes, 40)?,
        num_value_dicts: read_u32(bytes, 44)?,
        chunk_index_root_crc32c: read_u32(bytes, 48)?,
        root_crc32c: read_u32(bytes, 52)?,
        cold_page_count: read_u32(bytes, 60)?,
        directory_offset: read_u64(bytes, 64)?,
        directory_len: read_u64(bytes, 72)?,
        hot_pages_offset: read_u64(bytes, 80)?,
        hot_pages_len: read_u64(bytes, 88)?,
        keysets_offset: read_u64(bytes, 96)?,
        keysets_len: read_u64(bytes, 104)?,
        value_dicts_offset: read_u64(bytes, 112)?,
        value_dicts_len: read_u64(bytes, 120)?,
        keyset_blocks_offset: read_u64(bytes, 128)?,
        keyset_blocks_len: read_u64(bytes, 136)?,
        segment_start_ms: read_u64(bytes, 144)?,
        segment_end_ms: read_u64(bytes, 152)?,
        chunk_index_file_len: read_u64(bytes, 160)?,
        file_len: read_u64(bytes, 168)?,
    };
    validate_series_table_shape(header)?;
    let expected_pages = ceil_div(
        u64::from(header.num_series),
        u64::from(HOT_RECORDS_PER_PAGE),
    );
    let cold_bytes = header
        .keysets_len
        .checked_add(header.value_dicts_len)
        .and_then(|value| value.checked_add(header.keyset_blocks_len))
        .ok_or_else(|| invalid_data("schema-7 oracle cold length overflows"))?;
    let expected_cold_pages = ceil_div(cold_bytes, COLD_PAGE_LEN);
    let expected_cold_pages_u32 = u32::try_from(expected_cold_pages)
        .map_err(|_| invalid_data("schema-7 oracle cold page count exceeds u32"))?;
    let descriptor_count = expected_pages
        .checked_add(expected_cold_pages)
        .ok_or_else(|| invalid_data("schema-7 oracle descriptor count overflows"))?;
    let expected_directory_len = descriptor_count
        .checked_mul(DESCRIPTOR_LEN as u64)
        .ok_or_else(|| invalid_data("schema-7 oracle directory length overflows"))?;
    let directory_end = (SERIES_HEADER_LEN as u64)
        .checked_add(expected_directory_len)
        .ok_or_else(|| invalid_data("schema-7 oracle directory end overflows"))?;
    let expected_hot_offset = align_up(directory_end, ROOT_ALIGNMENT)?;
    let expected_hot_len = expected_pages
        .checked_mul(HOT_PAGE_LEN as u64)
        .ok_or_else(|| invalid_data("schema-7 oracle hot length overflows"))?;
    let expected_keysets = expected_hot_offset
        .checked_add(expected_hot_len)
        .ok_or_else(|| invalid_data("schema-7 oracle keysets offset overflows"))?;
    let expected_value_dicts = expected_keysets
        .checked_add(header.keysets_len)
        .ok_or_else(|| invalid_data("schema-7 oracle dictionaries offset overflows"))?;
    let expected_blocks = expected_value_dicts
        .checked_add(header.value_dicts_len)
        .ok_or_else(|| invalid_data("schema-7 oracle blocks offset overflows"))?;
    let expected_file_len = expected_blocks
        .checked_add(header.keyset_blocks_len)
        .ok_or_else(|| invalid_data("schema-7 oracle file length overflows"))?;
    if header.segment_start_ms >= header.segment_end_ms
        || header.segment_start_ms != meta.start_ms
        || header.segment_end_ms != meta.end_ms
        || u64::from(header.num_series) != meta.series
        || header.page_count as u64 != expected_pages
        || header.cold_page_count != expected_cold_pages_u32
        || header.directory_offset != SERIES_HEADER_LEN as u64
        || header.directory_len != expected_directory_len
        || header.hot_pages_offset != expected_hot_offset
        || header.hot_pages_len != expected_hot_len
        || header.keysets_offset != expected_keysets
        || header.value_dicts_offset != expected_value_dicts
        || header.keyset_blocks_offset != expected_blocks
        || header.file_len != expected_file_len
        || header.file_len != series_file_len
        || header.chunk_index_file_len != chunk_index_file_len
    {
        return Err(invalid_data(
            "schema-7 oracle series layout is noncanonical",
        ));
    }
    Ok(header)
}

fn validate_series_table_shape(header: Header) -> io::Result<()> {
    if header.chunk_index_file_len < CHUNK_INDEX_ROOT_LEN as u64 {
        return Err(invalid_data(
            "schema-7 oracle chunk index is shorter than its root",
        ));
    }
    if header.num_series != 0 && (header.num_keysets == 0 || header.num_keysets > header.num_series)
    {
        return Err(invalid_data(
            "schema-7 oracle nonempty series table has an invalid keyset count",
        ));
    }
    validate_offset_table_minimum(header.keysets_len, header.num_keysets)?;
    validate_offset_table_minimum(header.value_dicts_len, header.num_value_dicts)?;
    validate_offset_table_minimum(header.keyset_blocks_len, header.num_keysets)?;
    if header.num_series == 0
        && (header.num_keysets != 0
            || header.num_value_dicts != 0
            || header.keysets_len != 8
            || header.value_dicts_len != 8
            || header.keyset_blocks_len != 8
            || header.chunk_index_file_len != CHUNK_INDEX_ROOT_LEN as u64)
    {
        return Err(invalid_data(
            "schema-7 oracle empty series table is noncanonical",
        ));
    }
    Ok(())
}

fn validate_offset_table_minimum(section_len: u64, entry_count: u32) -> io::Result<()> {
    let minimum = u64::from(entry_count)
        .checked_add(1)
        .and_then(|count| count.checked_mul(8))
        .ok_or_else(|| invalid_data("schema-7 oracle offset table length overflows"))?;
    if section_len < minimum {
        return Err(invalid_data(
            "schema-7 oracle cold section is shorter than its offset table",
        ));
    }
    Ok(())
}

fn authenticate_series_root(
    series: &mut File,
    header: Header,
) -> io::Result<(Vec<HotDescriptor>, Vec<ColdDescriptor>)> {
    let mut first = read_exact_at(series, 0, SERIES_HEADER_LEN)?;
    first[ROOT_CRC_OFFSET..ROOT_CRC_OFFSET + 4].fill(0);
    let mut crc = crc32c(&first);
    let mut offset = SERIES_HEADER_LEN as u64;
    while offset < header.hot_pages_offset {
        let len = usize::try_from((header.hot_pages_offset - offset).min(1024 * 1024))
            .map_err(|_| invalid_data("schema-7 oracle root chunk length exceeds usize"))?;
        let bytes = read_exact_at(series, offset, len)?;
        crc = crc32c_append(crc, &bytes);
        offset = offset
            .checked_add(
                u64::try_from(len)
                    .map_err(|_| invalid_data("schema-7 oracle root chunk length exceeds u64"))?,
            )
            .ok_or_else(|| invalid_data("schema-7 oracle root cursor overflows"))?;
    }
    if crc != header.root_crc32c {
        return Err(invalid_data("schema-7 oracle series root CRC mismatch"));
    }

    let mut hot = Vec::with_capacity(
        usize::try_from(header.page_count)
            .map_err(|_| invalid_data("schema-7 oracle hot page count exceeds usize"))?,
    );
    let mut cursor = header.directory_offset;
    for page_index in 0..header.page_count {
        let bytes = read_exact_at(series, cursor, DESCRIPTOR_LEN)?;
        let descriptor = HotDescriptor {
            first_series_ref: read_u32(&bytes, 0)?,
            record_count: read_u32(&bytes, 4)?,
            page_crc32c: read_u32(&bytes, 8)?,
        };
        let expected_first = page_index
            .checked_mul(HOT_RECORDS_PER_PAGE)
            .ok_or_else(|| invalid_data("schema-7 oracle first series_ref overflows"))?;
        let remaining = header
            .num_series
            .checked_sub(expected_first)
            .ok_or_else(|| invalid_data("schema-7 oracle hot descriptor exceeds series count"))?;
        let expected_count = remaining.min(HOT_RECORDS_PER_PAGE);
        if descriptor.first_series_ref != expected_first
            || descriptor.record_count != expected_count
            || read_u32(&bytes, 12)? != 0
        {
            return Err(invalid_data(
                "schema-7 oracle hot descriptor is noncanonical",
            ));
        }
        hot.push(descriptor);
        cursor = cursor
            .checked_add(DESCRIPTOR_LEN as u64)
            .ok_or_else(|| invalid_data("schema-7 oracle descriptor cursor overflows"))?;
    }
    let mut cold = Vec::with_capacity(
        usize::try_from(header.cold_page_count)
            .map_err(|_| invalid_data("schema-7 oracle cold page count exceeds usize"))?,
    );
    let cold_bytes = header
        .file_len
        .checked_sub(header.keysets_offset)
        .ok_or_else(|| invalid_data("schema-7 oracle cold bounds are reversed"))?;
    for page_index in 0..header.cold_page_count {
        let bytes = read_exact_at(series, cursor, DESCRIPTOR_LEN)?;
        let page_start = u64::from(page_index)
            .checked_mul(COLD_PAGE_LEN)
            .ok_or_else(|| invalid_data("schema-7 oracle cold descriptor offset overflows"))?;
        let remaining = cold_bytes.checked_sub(page_start).ok_or_else(|| {
            invalid_data("schema-7 oracle cold descriptor begins past the cold region")
        })?;
        let descriptor = ColdDescriptor {
            page_len: read_u32(&bytes, 4)?,
            page_crc32c: read_u32(&bytes, 8)?,
        };
        if read_u32(&bytes, 0)? != page_index
            || u64::from(descriptor.page_len) != remaining.min(COLD_PAGE_LEN)
            || read_u32(&bytes, 12)? != 0
        {
            return Err(invalid_data(
                "schema-7 oracle cold descriptor is noncanonical",
            ));
        }
        cold.push(descriptor);
        cursor = cursor
            .checked_add(DESCRIPTOR_LEN as u64)
            .ok_or_else(|| invalid_data("schema-7 oracle descriptor cursor overflows"))?;
    }
    let directory_end = header
        .directory_offset
        .checked_add(header.directory_len)
        .ok_or_else(|| invalid_data("schema-7 oracle directory end overflows"))?;
    if cursor != directory_end {
        return Err(invalid_data(
            "schema-7 oracle descriptor directory length mismatch",
        ));
    }
    let padding = read_exact_at(
        series,
        cursor,
        usize::try_from(
            header
                .hot_pages_offset
                .checked_sub(cursor)
                .ok_or_else(|| invalid_data("schema-7 oracle root padding bounds are reversed"))?,
        )
        .map_err(|_| invalid_data("schema-7 oracle root padding exceeds usize"))?,
    )?;
    if padding.iter().any(|byte| *byte != 0) {
        return Err(invalid_data("schema-7 oracle root padding is nonzero"));
    }
    Ok((hot, cold))
}

fn authenticate_chunk_index_root(
    chunk_index: &mut File,
    file_len: u64,
    header: Header,
) -> io::Result<()> {
    if file_len < CHUNK_INDEX_ROOT_LEN as u64 {
        return Err(invalid_data(
            "schema-7 oracle chunk-index file is shorter than its root",
        ));
    }
    let mut bytes = read_exact_at(chunk_index, 0, CHUNK_INDEX_ROOT_LEN)?;
    let stored_crc = read_u32(&bytes, 56)?;
    bytes[56..60].fill(0);
    if crc32c(&bytes) != stored_crc {
        return Err(invalid_data(
            "schema-7 oracle chunk-index root CRC mismatch",
        ));
    }
    let blob_count = read_u32(&bytes, 24)?;
    let blobs_len = read_u64(&bytes, 40)?;
    let expected_file_len = (CHUNK_INDEX_ROOT_LEN as u64)
        .checked_add(blobs_len)
        .ok_or_else(|| invalid_data("schema-7 oracle chunk-index length overflows"))?;
    let minimum_blobs_len = u64::from(blob_count)
        .checked_mul(MIN_OVERFLOW_BLOB_LEN)
        .ok_or_else(|| invalid_data("schema-7 oracle minimum overflow length overflows"))?;
    if read_u32(&bytes, 0)? != CHUNK_INDEX_MAGIC
        || read_u16(&bytes, 4)? != 2
        || read_u16(&bytes, 6)? != 0
        || read_u32(&bytes, 8)? != CHUNK_INDEX_ROOT_LEN as u32
        || read_u32(&bytes, 12)? != OVERFLOW_HEADER_LEN as u32
        || read_u32(&bytes, 16)? != OVERFLOW_ENTRY_LEN as u32
        || read_u32(&bytes, 20)? != header.num_series
        || blob_count > header.num_series
        || read_u32(&bytes, 28)? != 0
        || read_u64(&bytes, 32)? != CHUNK_INDEX_ROOT_LEN as u64
        || read_u64(&bytes, 48)? != file_len
        || file_len != expected_file_len
        || stored_crc != header.chunk_index_root_crc32c
        || read_u32(&bytes, 60)? != 0
        || ((blob_count == 0) != (blobs_len == 0))
        || blobs_len < minimum_blobs_len
    {
        return Err(invalid_data(
            "schema-7 oracle chunk-index root is noncanonical",
        ));
    }
    Ok(())
}

fn decode_hot_record(
    bytes: &[u8],
    _series_ref: u32,
    header: Header,
    chunk_file_lens: [u64; 2],
) -> io::Result<HotRecord> {
    let series_id = read_u64(bytes, 0)?;
    let keyset_id = read_u32(bytes, 8)?;
    let row = read_u32(bytes, 12)?;
    let control = read_u32(bytes, 16)?;
    let kind_mask = (control & 0x1f) as u8;
    let tag = (control >> 9) & 0b11;
    if keyset_id >= header.num_keysets || kind_mask == 0 {
        return Err(invalid_data("schema-7 oracle hot record is invalid"));
    }
    let location = match tag {
        1 => {
            let kind_raw = ((control >> 5) & 0b111) as u8;
            let kind = chunk_kind(kind_raw)?;
            if kind_mask != 1u8 << kind_raw {
                return Err(invalid_data("schema-7 oracle inline kind mask is invalid"));
            }
            let file_id = ((control >> 8) & 1) as u8;
            let scalar_lane_len = control >> 11;
            validate_scalar_locator(kind, scalar_lane_len)?;
            let min_time_ms = header
                .segment_start_ms
                .checked_add(u64::from(read_u32(bytes, 20)?))
                .ok_or_else(|| invalid_data("schema-7 oracle inline minimum time overflows"))?;
            let max_time_ms = header
                .segment_start_ms
                .checked_add(u64::from(read_u32(bytes, 24)?))
                .ok_or_else(|| invalid_data("schema-7 oracle inline maximum time overflows"))?;
            let offset = u64::from(read_u32(bytes, 28)?);
            let length = read_u32(bytes, 32)?;
            validate_chunk_range(
                file_id,
                min_time_ms,
                max_time_ms,
                offset,
                length,
                scalar_lane_len,
                header,
                chunk_file_lens,
            )?;
            HotLocation::Inline(OracleChunk {
                entry: ChunkIndexEntry {
                    file_id,
                    kind,
                    flags: 0,
                    min_time_ms,
                    max_time_ms,
                    offset,
                    length,
                    scalar_lane_offset: u32::from(scalar_lane_len != 0) * CHUNK_HEADER_LEN,
                    scalar_lane_len,
                },
                indexed_prefix_crc32c: read_u32(bytes, 36)?,
            })
        }
        2 => {
            if control & !((0x1f) | (0b11 << 9)) != 0 || read_u32(bytes, 36)? != 0 {
                return Err(invalid_data("schema-7 oracle overflow control is invalid"));
            }
            HotLocation::Overflow {
                blob_offset: read_u64(bytes, 20)?,
                blob_len: read_u32(bytes, 28)?,
                chunk_count: read_u32(bytes, 32)?,
            }
        }
        _ => return Err(invalid_data("schema-7 oracle hot tag is invalid")),
    };
    Ok(HotRecord {
        series_id,
        keyset_id,
        row,
        kind_mask,
        location,
    })
}

fn decode_overflow_entry(bytes: &[u8], chunk_file_lens: [u64; 2]) -> io::Result<OracleChunk> {
    let file_id = bytes[0];
    let kind = chunk_kind(bytes[1])?;
    if read_u16(bytes, 2)? != 0 {
        return Err(invalid_data(
            "schema-7 oracle overflow entry reserved field is nonzero",
        ));
    }
    let min_time_ms = read_u64(bytes, 4)?;
    let max_time_ms = read_u64(bytes, 12)?;
    let offset = read_u64(bytes, 20)?;
    let length = read_u32(bytes, 28)?;
    let scalar_lane_offset = read_u32(bytes, 32)?;
    let scalar_lane_len = read_u32(bytes, 36)?;
    if scalar_lane_offset != u32::from(scalar_lane_len != 0) * CHUNK_HEADER_LEN {
        return Err(invalid_data(
            "schema-7 oracle overflow scalar locator is noncanonical",
        ));
    }
    validate_scalar_locator(kind, scalar_lane_len)?;
    let minimum_length = CHUNK_HEADER_LEN
        .checked_add(scalar_lane_len)
        .ok_or_else(|| invalid_data("schema-7 oracle overflow chunk length overflows"))?;
    let file_len = chunk_file_lens
        .get(usize::from(file_id))
        .copied()
        .ok_or_else(|| invalid_data("schema-7 oracle overflow file_id is invalid"))?;
    let end = offset
        .checked_add(u64::from(length))
        .ok_or_else(|| invalid_data("schema-7 oracle overflow chunk range overflows"))?;
    if file_id > 1 || min_time_ms > max_time_ms || length < minimum_length || end > file_len {
        return Err(invalid_data(
            "schema-7 oracle overflow chunk range is invalid",
        ));
    }
    Ok(OracleChunk {
        entry: ChunkIndexEntry {
            file_id,
            kind,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset,
            length,
            scalar_lane_offset,
            scalar_lane_len,
        },
        indexed_prefix_crc32c: read_u32(bytes, 40)?,
    })
}

fn verify_indexed_prefix(
    files: &mut [File; 2],
    file_lens: [u64; 2],
    series_ref: u32,
    chunk: &OracleChunk,
) -> io::Result<VerifiedChunkPrefix> {
    let entry = &chunk.entry;
    let prefix_len = if entry.scalar_lane_len == 0 { 40 } else { 56 };
    let file = files
        .get_mut(usize::from(entry.file_id))
        .ok_or_else(|| invalid_data("schema-7 oracle chunk file_id is invalid"))?;
    let file_len = file_lens
        .get(usize::from(entry.file_id))
        .copied()
        .ok_or_else(|| invalid_data("schema-7 oracle chunk file_id is invalid"))?;
    let entry_end = entry
        .offset
        .checked_add(u64::from(entry.length))
        .ok_or_else(|| invalid_data("schema-7 oracle chunk locator range overflows"))?;
    if entry_end > file_len {
        return Err(invalid_data(
            "schema-7 oracle chunk locator exceeds its file",
        ));
    }
    let prefix = read_exact_at(file, entry.offset, prefix_len)?;
    if crc32c(&prefix) != chunk.indexed_prefix_crc32c {
        return Err(invalid_data("schema-7 oracle indexed prefix CRC mismatch"));
    }
    let kind = chunk_kind(prefix[0])?;
    let encoding = prefix[1];
    validate_kind_encoding(kind, encoding)?;
    let flags = read_u16(&prefix, 2)?;
    validate_chunk_flags(kind, flags)?;
    let num_points = read_u32(&prefix, 24)?;
    let expected_header_len = CHUNK_HEADER_LEN
        .checked_add(entry.scalar_lane_len)
        .ok_or_else(|| invalid_data("schema-7 oracle chunk header length overflows"))?;
    if kind != entry.kind
        || read_u32(&prefix, 4)? != series_ref
        || read_u64(&prefix, 8)? != entry.min_time_ms
        || read_u64(&prefix, 16)? != entry.max_time_ms
        || num_points == 0
        || read_u32(&prefix, 28)? != expected_header_len
        || read_u32(&prefix, 28)?.checked_add(read_u32(&prefix, 32)?) != Some(entry.length)
    {
        return Err(invalid_data(
            "schema-7 oracle locator does not match the chunk header",
        ));
    }
    if entry.scalar_lane_len != 0 {
        if encoding != ENCODING_SCHEMA_VARLEN {
            return Err(invalid_data(
                "schema-7 oracle scalar lane requires schema-varlen encoding",
            ));
        }
        if read_u32(&prefix, 40)? != SCALAR_MAGIC
            || read_u16(&prefix, 44)? != 1
            || read_u16(&prefix, 46)? != 0
            || read_u32(&prefix, 48)?.checked_add(SCALAR_HEADER_LEN) != Some(entry.scalar_lane_len)
        {
            return Err(invalid_data(
                "schema-7 oracle scalar header is noncanonical",
            ));
        }
    }
    Ok(VerifiedChunkPrefix { flags, num_points })
}

fn validate_kind_encoding(kind: ChunkKind, encoding: u8) -> io::Result<()> {
    let valid = matches!(
        (kind, encoding),
        (ChunkKind::Float, ENCODING_RAW_F64 | ENCODING_GORILLA)
            | (
                ChunkKind::Int64,
                ENCODING_RAW_I64 | ENCODING_INT_DELTA_ZIGZAG
            )
            | (
                ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary,
                ENCODING_SCHEMA_VARLEN
            )
    );
    if !valid {
        return Err(invalid_data(
            "schema-7 oracle chunk kind/encoding pair is invalid",
        ));
    }
    Ok(())
}

fn validate_chunk_flags(kind: ChunkKind, flags: u16) -> io::Result<()> {
    match kind {
        ChunkKind::Float | ChunkKind::Int64 if flags != 0 => Err(invalid_data(
            "schema-7 oracle scalar chunk flags must be zero",
        )),
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
            if flags & !TYPED_CHUNK_FLAGS != 0 =>
        {
            Err(invalid_data(
                "schema-7 oracle typed chunk flags contain reserved bits",
            ))
        }
        _ => Ok(()),
    }
}

fn validate_decoded_chunk_flags(
    prefix: VerifiedChunkPrefix,
    record: &ChunkRecord,
) -> io::Result<()> {
    let sample_count = match &record.samples {
        ChunkSamples::Float(samples) => samples.len(),
        ChunkSamples::Int64(samples) => samples.len(),
        ChunkSamples::Histogram(samples) => {
            validate_typed_metadata_flags(
                prefix.flags,
                samples.iter().map(|(_, value)| value.metadata),
            )?;
            samples.len()
        }
        ChunkSamples::ExponentialHistogram(samples) => {
            validate_typed_metadata_flags(
                prefix.flags,
                samples.iter().map(|(_, value)| value.metadata),
            )?;
            samples.len()
        }
        ChunkSamples::Summary(samples) => {
            validate_typed_metadata_flags(
                prefix.flags,
                samples.iter().map(|(_, value)| value.metadata),
            )?;
            samples.len()
        }
    };
    if sample_count
        != usize::try_from(prefix.num_points)
            .map_err(|_| invalid_data("schema-7 oracle chunk point count exceeds usize"))?
    {
        return Err(invalid_data(
            "schema-7 oracle decoded sample count does not match its header",
        ));
    }
    Ok(())
}

fn validate_typed_metadata_flags(
    expected: u16,
    metadata: impl IntoIterator<Item = TypedSampleMetadata>,
) -> io::Result<()> {
    let mut actual = 0u16;
    let mut saw_sample = false;
    let mut all_delta = true;
    for metadata in metadata {
        saw_sample = true;
        if metadata.start_time_ms.is_some() {
            actual |= 1 << 1;
        }
        if metadata.flags != 0 {
            actual |= 1 << 2;
        }
        if metadata.reset_hint != CounterResetHint::Unknown {
            actual |= 1 << 3;
        }
        if metadata.temporality != OtlpAggregationTemporality::Delta {
            all_delta = false;
        }
    }
    if saw_sample && all_delta {
        actual |= 1 << 4;
    }
    if actual != expected {
        return Err(invalid_data(
            "schema-7 oracle chunk flags do not match decoded typed metadata",
        ));
    }
    Ok(())
}

fn validate_chunk_range(
    file_id: u8,
    min_time_ms: u64,
    max_time_ms: u64,
    offset: u64,
    length: u32,
    scalar_lane_len: u32,
    header: Header,
    chunk_file_lens: [u64; 2],
) -> io::Result<()> {
    let minimum_length = CHUNK_HEADER_LEN
        .checked_add(scalar_lane_len)
        .ok_or_else(|| invalid_data("schema-7 oracle inline chunk length overflows"))?;
    let file_len = chunk_file_lens
        .get(usize::from(file_id))
        .copied()
        .ok_or_else(|| invalid_data("schema-7 oracle inline file_id is invalid"))?;
    let end = offset
        .checked_add(u64::from(length))
        .ok_or_else(|| invalid_data("schema-7 oracle inline chunk range overflows"))?;
    if file_id > 1
        || min_time_ms > max_time_ms
        || max_time_ms >= header.segment_end_ms
        || length < minimum_length
        || end > file_len
    {
        return Err(invalid_data("schema-7 oracle inline chunk is invalid"));
    }
    Ok(())
}

fn validate_scalar_locator(kind: ChunkKind, scalar_lane_len: u32) -> io::Result<()> {
    if scalar_lane_len != 0
        && (scalar_lane_len < SCALAR_HEADER_LEN
            || !matches!(
                kind,
                ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
            ))
    {
        return Err(invalid_data("schema-7 oracle scalar locator is invalid"));
    }
    Ok(())
}

fn chunk_is_inline_representable(header: Header, entry: &ChunkIndexEntry) -> io::Result<bool> {
    let min_delta = entry
        .min_time_ms
        .checked_sub(header.segment_start_ms)
        .ok_or_else(|| invalid_data("schema-7 oracle inline minimum delta underflows"))?;
    let max_delta = entry
        .max_time_ms
        .checked_sub(header.segment_start_ms)
        .ok_or_else(|| invalid_data("schema-7 oracle inline maximum delta underflows"))?;
    Ok(min_delta <= u64::from(u32::MAX)
        && max_delta <= u64::from(u32::MAX)
        && entry.offset <= u64::from(u32::MAX)
        && entry.scalar_lane_len <= (1 << 21) - 1)
}

fn canonical_code_width(cardinality: usize) -> io::Result<u8> {
    match cardinality {
        0 => Err(invalid_data(
            "schema-7 oracle dictionary cardinality is zero",
        )),
        1 => Ok(0),
        2..=256 => Ok(1),
        257..=65_536 => Ok(2),
        _ => Ok(4),
    }
}

fn read_code(bytes: &[u8], cursor: &mut usize, width: u8) -> io::Result<u32> {
    let end = cursor
        .checked_add(usize::from(width))
        .ok_or_else(|| invalid_data("schema-7 oracle value-code range overflows"))?;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "schema-7 oracle value code is truncated",
        )
    })?;
    let value = match width {
        0 => 0,
        1 => u32::from(slice[0]),
        2 => u32::from(u16::from_le_bytes([slice[0], slice[1]])),
        4 => u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]),
        _ => return Err(invalid_data("schema-7 oracle value-code width is invalid")),
    };
    *cursor = end;
    Ok(value)
}

fn chunk_kind(value: u8) -> io::Result<ChunkKind> {
    match value {
        0 => Ok(ChunkKind::Float),
        1 => Ok(ChunkKind::Int64),
        2 => Ok(ChunkKind::Histogram),
        3 => Ok(ChunkKind::ExponentialHistogram),
        4 => Ok(ChunkKind::Summary),
        _ => Err(invalid_data("schema-7 oracle chunk kind is invalid")),
    }
}

fn read_exact_at(file: &mut File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_u16(bytes: &[u8], offset: usize) -> io::Result<u16> {
    let value = bytes.get(offset..offset + 2).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "schema-7 oracle u16 is truncated",
        )
    })?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> io::Result<u32> {
    let value = bytes.get(offset..offset + 4).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "schema-7 oracle u32 is truncated",
        )
    })?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_u64(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let value = bytes.get(offset..offset + 8).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "schema-7 oracle u64 is truncated",
        )
    })?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    if value == 0 {
        0
    } else {
        1 + (value - 1) / divisor
    }
}

fn align_up(value: u64, alignment: u64) -> io::Result<u64> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or_else(|| invalid_data("schema-7 oracle alignment overflows"))
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn empty_header() -> Header {
        Header {
            num_series: 0,
            page_count: 0,
            num_keysets: 0,
            num_value_dicts: 0,
            chunk_index_root_crc32c: 0,
            root_crc32c: 0,
            cold_page_count: 1,
            directory_offset: 176,
            directory_len: 16,
            hot_pages_offset: 4_096,
            hot_pages_len: 0,
            keysets_offset: 4_096,
            keysets_len: 8,
            value_dicts_offset: 4_104,
            value_dicts_len: 8,
            keyset_blocks_offset: 4_112,
            keyset_blocks_len: 8,
            segment_start_ms: 1,
            segment_end_ms: 2,
            chunk_index_file_len: 64,
            file_len: 4_120,
        }
    }

    fn indexed_prefix(kind: ChunkKind, encoding: u8, flags: u16, scalar_flags: u16) -> Vec<u8> {
        let scalar_len = u32::from(matches!(kind, ChunkKind::Histogram)) * SCALAR_HEADER_LEN;
        let mut prefix = vec![0; usize::try_from(CHUNK_HEADER_LEN + scalar_len).unwrap()];
        prefix[0] = kind as u8;
        prefix[1] = encoding;
        prefix[2..4].copy_from_slice(&flags.to_le_bytes());
        prefix[4..8].copy_from_slice(&7u32.to_le_bytes());
        prefix[8..16].copy_from_slice(&100u64.to_le_bytes());
        prefix[16..24].copy_from_slice(&110u64.to_le_bytes());
        prefix[24..28].copy_from_slice(&1u32.to_le_bytes());
        prefix[28..32].copy_from_slice(&(CHUNK_HEADER_LEN + scalar_len).to_le_bytes());
        if scalar_len != 0 {
            prefix[40..44].copy_from_slice(&SCALAR_MAGIC.to_le_bytes());
            prefix[44..46].copy_from_slice(&1u16.to_le_bytes());
            prefix[46..48].copy_from_slice(&scalar_flags.to_le_bytes());
        }
        prefix
    }

    fn verify_prefix_bytes(prefix: &[u8], kind: ChunkKind) -> io::Result<VerifiedChunkPrefix> {
        let mut chunk_file = tempfile::tempfile()?;
        chunk_file.write_all(prefix)?;
        let other_file = tempfile::tempfile()?;
        let scalar_lane_len = u32::from(prefix.len() == 56) * SCALAR_HEADER_LEN;
        let chunk = OracleChunk {
            entry: ChunkIndexEntry {
                file_id: 0,
                kind,
                flags: 0,
                min_time_ms: 100,
                max_time_ms: 110,
                offset: 0,
                length: u32::try_from(prefix.len()).unwrap(),
                scalar_lane_offset: u32::from(scalar_lane_len != 0) * CHUNK_HEADER_LEN,
                scalar_lane_len,
            },
            indexed_prefix_crc32c: crc32c(prefix),
        };
        verify_indexed_prefix(
            &mut [chunk_file, other_file],
            [u64::try_from(prefix.len()).unwrap(), 0],
            7,
            &chunk,
        )
    }

    #[test]
    fn fixed_root_shape_rejects_missing_tables_and_noncanonical_empty_layouts() {
        validate_series_table_shape(empty_header()).unwrap();

        let mut nonempty_without_keyset = empty_header();
        nonempty_without_keyset.num_series = 1;
        assert_eq!(
            validate_series_table_shape(nonempty_without_keyset)
                .unwrap_err()
                .to_string(),
            "schema-7 oracle nonempty series table has an invalid keyset count"
        );

        let mut short_table = empty_header();
        short_table.num_series = 1;
        short_table.num_keysets = 1;
        assert_eq!(
            validate_series_table_shape(short_table)
                .unwrap_err()
                .to_string(),
            "schema-7 oracle cold section is shorter than its offset table"
        );

        let mut noncanonical_empty = empty_header();
        noncanonical_empty.value_dicts_len = 9;
        assert_eq!(
            validate_series_table_shape(noncanonical_empty)
                .unwrap_err()
                .to_string(),
            "schema-7 oracle empty series table is noncanonical"
        );
    }

    #[test]
    fn prefix_rejects_kind_encoding_chunk_flags_and_scalar_header_flags() {
        let invalid_encoding = indexed_prefix(ChunkKind::Float, ENCODING_SCHEMA_VARLEN, 0, 0);
        assert_eq!(
            verify_prefix_bytes(&invalid_encoding, ChunkKind::Float)
                .unwrap_err()
                .to_string(),
            "schema-7 oracle chunk kind/encoding pair is invalid"
        );

        let invalid_flags = indexed_prefix(ChunkKind::Float, ENCODING_RAW_F64, 1, 0);
        assert_eq!(
            verify_prefix_bytes(&invalid_flags, ChunkKind::Float)
                .unwrap_err()
                .to_string(),
            "schema-7 oracle scalar chunk flags must be zero"
        );

        let invalid_scalar_flags =
            indexed_prefix(ChunkKind::Histogram, ENCODING_SCHEMA_VARLEN, 0, 1);
        assert_eq!(
            verify_prefix_bytes(&invalid_scalar_flags, ChunkKind::Histogram)
                .unwrap_err()
                .to_string(),
            "schema-7 oracle scalar header is noncanonical"
        );
    }

    #[test]
    fn chunk_index_root_rejects_an_impossibly_short_overflow_region() {
        let mut bytes = [0u8; CHUNK_INDEX_ROOT_LEN];
        bytes[0..4].copy_from_slice(&CHUNK_INDEX_MAGIC.to_le_bytes());
        bytes[4..6].copy_from_slice(&2u16.to_le_bytes());
        bytes[8..12].copy_from_slice(&(CHUNK_INDEX_ROOT_LEN as u32).to_le_bytes());
        bytes[12..16].copy_from_slice(&(OVERFLOW_HEADER_LEN as u32).to_le_bytes());
        bytes[16..20].copy_from_slice(&(OVERFLOW_ENTRY_LEN as u32).to_le_bytes());
        bytes[20..24].copy_from_slice(&1u32.to_le_bytes());
        bytes[24..28].copy_from_slice(&1u32.to_le_bytes());
        bytes[32..40].copy_from_slice(&(CHUNK_INDEX_ROOT_LEN as u64).to_le_bytes());
        bytes[40..48].copy_from_slice(&1u64.to_le_bytes());
        bytes[48..56].copy_from_slice(&65u64.to_le_bytes());
        let root_crc = crc32c(&bytes);
        bytes[56..60].copy_from_slice(&root_crc.to_le_bytes());

        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&bytes).unwrap();
        let mut header = empty_header();
        header.num_series = 1;
        header.chunk_index_root_crc32c = root_crc;
        header.chunk_index_file_len = 65;
        assert_eq!(
            authenticate_chunk_index_root(&mut file, 65, header)
                .unwrap_err()
                .to_string(),
            "schema-7 oracle chunk-index root is noncanonical"
        );
    }
}
