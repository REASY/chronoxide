//! Pure codecs for the fixed portions of `series.bin` version 3.
//!
//! This module deliberately does not perform I/O or decode the retained v2
//! cold-label sections. Callers authenticate a root or complete hot page here
//! before interpreting any descriptor or record returned by the decoder.

use std::io;

use crc32c::{crc32c, crc32c_append};

use super::cold_v2::reader::validate_offset_table_minimum;

mod classifier;
mod cold_reader;
mod reader;
mod root_binding;
mod runtime_reader;
mod writer;

#[allow(unused_imports)] // Used by the schema-7 segment writer integration.
pub(crate) use classifier::{
    ClassifiedSeriesV3, FinalChunkIndexEntryV3, PendingOverflowSeriesV3, SeriesClassifierInputV3,
    classify_series_v3,
};
#[allow(unused_imports)]
// Wired into governed cold-label materialization after the authenticated page boundary lands.
pub(crate) use cold_reader::ValidatedSeriesColdPage;
#[allow(unused_imports)]
// Wired into the governed schema-7 reader after the pure planner lands.
pub(crate) use reader::{
    ChunkLocatorSource, ColdLabelRowLocator, FlatChunkLocatorBatch, PlannedSeries, SeriesChunkSpan,
    ValidatedOverflowBlob, ValidatedSeriesHotPage, plan_schema7_decoded_hot_page,
    plan_schema7_decoded_overflow_blob, plan_schema7_hot_page, plan_schema7_overflow_blob,
};
#[allow(unused_imports)] // Wired into schema-7 segment open after the pure binding lands.
pub(crate) use root_binding::{
    Schema7OverflowBlobFacts, Schema7RootBinding, Schema7RootBindingContext, Schema7SeriesPageFacts,
};
#[allow(unused_imports)]
// The segment/query wiring consumes this strict governed boundary in a later change.
pub(crate) use runtime_reader::{
    BoundSchema7Roots, CanonicalLabelMaterializationProfile, GovernedChunkLocatorBatch,
    GovernedPlannedSeries, GovernedPlannedSeriesRef, GovernedVerifiedSeries, Schema7MetadataReader,
    Schema7MetadataReaderError, Schema7MetadataSession, Schema7RootPins,
};
#[allow(unused_imports)] // Wired into segment sealing after the isolated assembly is accepted.
pub(crate) use writer::{
    Schema7SeriesAssemblyInput, Schema7SeriesAssemblyResult, Schema7SeriesAssemblyStats,
    write_schema7_series_and_chunk_index,
};

pub(crate) const SERIES_HEADER_LEN_V3: usize = 176;
pub(crate) const SERIES_DESCRIPTOR_LEN_V1: usize = 16;
pub(crate) const SERIES_HOT_PAGE_LEN_V1: usize = 16_384;
pub(crate) const SERIES_HOT_PAGE_HEADER_LEN_V1: usize = 24;
pub(crate) const SERIES_HOT_RECORD_LEN_V3: usize = 40;
pub(crate) const SERIES_HOT_RECORDS_PER_PAGE_V1: u32 = 409;
pub(crate) const SERIES_COLD_PAGE_LEN_V1: u64 = 16_384;

const SERIES_MAGIC: u32 = u32::from_le_bytes(*b"SERI");
const SERIES_VERSION_V3: u16 = 3;
const SERIES_ROOT_ALIGNMENT_V3: u64 = 4_096;
const SERIES_ROOT_CRC_OFFSET_V3: usize = 52;
const SERIES_ROOT_CRC_LEN_V3: usize = 4;

const SERIES_HOT_PAGE_MAGIC_V1: u32 = u32::from_le_bytes(*b"SHP7");
const SERIES_HOT_PAGE_VERSION_V1: u16 = 1;

const SERIES_HOT_TAG_SHIFT: u32 = 9;
const SERIES_HOT_TAG_MASK: u32 = 0b11;
const SERIES_HOT_TAG_INLINE: u32 = 1;
const SERIES_HOT_TAG_OVERFLOW: u32 = 2;
const SERIES_HOT_KIND_MASK: u32 = 0b1_1111;
const SERIES_HOT_CHUNK_KIND_SHIFT: u32 = 5;
const SERIES_HOT_CHUNK_KIND_MASK: u32 = 0b111;
const SERIES_HOT_FILE_ID_SHIFT: u32 = 8;
const SERIES_HOT_SCALAR_LANE_LEN_SHIFT: u32 = 11;
const SERIES_HOT_SCALAR_LANE_LEN_MAX: u32 = (1 << 21) - 1;

const CHUNK_INDEX_ROOT_LEN_V2: u64 = 64;
const CHUNK_OVERFLOW_BLOB_HEADER_LEN_V1: u64 = 32;
const CHUNK_OVERFLOW_ENTRY_LEN_V1: u64 = 44;
const CHUNK_HEADER_LEN_V1: u32 = 40;
const TYPED_SCALAR_LANE_HEADER_LEN_V1: u32 = 16;

const CHUNK_KIND_FLOAT: u8 = 0;
const CHUNK_KIND_INT64: u8 = 1;
const CHUNK_KIND_HISTOGRAM: u8 = 2;
const CHUNK_KIND_EXPONENTIAL_HISTOGRAM: u8 = 3;
const CHUNK_KIND_SUMMARY: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHeaderV3Params {
    pub(crate) num_series: u32,
    pub(crate) num_keysets: u32,
    pub(crate) num_value_dicts: u32,
    pub(crate) chunk_index_root_crc32c: u32,
    pub(crate) keysets_len: u64,
    pub(crate) value_dicts_len: u64,
    pub(crate) keyset_blocks_len: u64,
    pub(crate) segment_start_ms: u64,
    pub(crate) segment_end_ms: u64,
    pub(crate) chunk_index_file_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHeaderV3 {
    pub(crate) num_series: u32,
    pub(crate) page_count: u32,
    pub(crate) num_keysets: u32,
    pub(crate) num_value_dicts: u32,
    pub(crate) chunk_index_root_crc32c: u32,
    pub(crate) root_crc32c: u32,
    pub(crate) cold_page_count: u32,
    pub(crate) directory_offset: u64,
    pub(crate) directory_len: u64,
    pub(crate) hot_pages_offset: u64,
    pub(crate) hot_pages_len: u64,
    pub(crate) keysets_offset: u64,
    pub(crate) keysets_len: u64,
    pub(crate) value_dicts_offset: u64,
    pub(crate) value_dicts_len: u64,
    pub(crate) keyset_blocks_offset: u64,
    pub(crate) keyset_blocks_len: u64,
    pub(crate) segment_start_ms: u64,
    pub(crate) segment_end_ms: u64,
    pub(crate) chunk_index_file_len: u64,
    pub(crate) file_len: u64,
}

impl SeriesHeaderV3 {
    pub(crate) fn new(params: SeriesHeaderV3Params) -> io::Result<Self> {
        let layout = derive_layout(
            params.num_series,
            params.keysets_len,
            params.value_dicts_len,
            params.keyset_blocks_len,
        )?;
        let header = Self {
            num_series: params.num_series,
            page_count: layout.page_count,
            num_keysets: params.num_keysets,
            num_value_dicts: params.num_value_dicts,
            chunk_index_root_crc32c: params.chunk_index_root_crc32c,
            root_crc32c: 0,
            cold_page_count: layout.cold_page_count,
            directory_offset: layout.directory_offset,
            directory_len: layout.directory_len,
            hot_pages_offset: layout.hot_pages_offset,
            hot_pages_len: layout.hot_pages_len,
            keysets_offset: layout.keysets_offset,
            keysets_len: params.keysets_len,
            value_dicts_offset: layout.value_dicts_offset,
            value_dicts_len: params.value_dicts_len,
            keyset_blocks_offset: layout.keyset_blocks_offset,
            keyset_blocks_len: params.keyset_blocks_len,
            segment_start_ms: params.segment_start_ms,
            segment_end_ms: params.segment_end_ms,
            chunk_index_file_len: params.chunk_index_file_len,
            file_len: layout.file_len,
        };
        header.validate()?;
        Ok(header)
    }

    pub(crate) fn encode(self) -> io::Result<[u8; SERIES_HEADER_LEN_V3]> {
        self.validate()?;
        let mut bytes = [0u8; SERIES_HEADER_LEN_V3];
        put_u32(&mut bytes, 0, SERIES_MAGIC);
        put_u16(&mut bytes, 4, SERIES_VERSION_V3);
        put_u16(&mut bytes, 6, 0);
        put_u32(&mut bytes, 8, SERIES_HEADER_LEN_V3 as u32);
        put_u32(&mut bytes, 12, SERIES_DESCRIPTOR_LEN_V1 as u32);
        put_u32(&mut bytes, 16, SERIES_HOT_PAGE_LEN_V1 as u32);
        put_u32(&mut bytes, 20, SERIES_HOT_PAGE_HEADER_LEN_V1 as u32);
        put_u32(&mut bytes, 24, SERIES_HOT_RECORD_LEN_V3 as u32);
        put_u32(&mut bytes, 28, SERIES_HOT_RECORDS_PER_PAGE_V1);
        put_u32(&mut bytes, 32, self.num_series);
        put_u32(&mut bytes, 36, self.page_count);
        put_u32(&mut bytes, 40, self.num_keysets);
        put_u32(&mut bytes, 44, self.num_value_dicts);
        put_u32(&mut bytes, 48, self.chunk_index_root_crc32c);
        put_u32(&mut bytes, 52, self.root_crc32c);
        put_u32(&mut bytes, 56, SERIES_COLD_PAGE_LEN_V1 as u32);
        put_u32(&mut bytes, 60, self.cold_page_count);
        put_u64(&mut bytes, 64, self.directory_offset);
        put_u64(&mut bytes, 72, self.directory_len);
        put_u64(&mut bytes, 80, self.hot_pages_offset);
        put_u64(&mut bytes, 88, self.hot_pages_len);
        put_u64(&mut bytes, 96, self.keysets_offset);
        put_u64(&mut bytes, 104, self.keysets_len);
        put_u64(&mut bytes, 112, self.value_dicts_offset);
        put_u64(&mut bytes, 120, self.value_dicts_len);
        put_u64(&mut bytes, 128, self.keyset_blocks_offset);
        put_u64(&mut bytes, 136, self.keyset_blocks_len);
        put_u64(&mut bytes, 144, self.segment_start_ms);
        put_u64(&mut bytes, 152, self.segment_end_ms);
        put_u64(&mut bytes, 160, self.chunk_index_file_len);
        put_u64(&mut bytes, 168, self.file_len);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> io::Result<Self> {
        require_len(bytes, SERIES_HEADER_LEN_V3, "series v3 header")?;
        require_eq_u32(read_u32(bytes, 0), SERIES_MAGIC, "series v3 magic")?;
        require_eq_u16(read_u16(bytes, 4), SERIES_VERSION_V3, "series v3 version")?;
        require_zero_u16(read_u16(bytes, 6), "series v3 flags")?;
        require_eq_u32(
            read_u32(bytes, 8),
            SERIES_HEADER_LEN_V3 as u32,
            "series v3 header length",
        )?;
        require_eq_u32(
            read_u32(bytes, 12),
            SERIES_DESCRIPTOR_LEN_V1 as u32,
            "series v3 descriptor length",
        )?;
        require_eq_u32(
            read_u32(bytes, 16),
            SERIES_HOT_PAGE_LEN_V1 as u32,
            "series v3 hot page length",
        )?;
        require_eq_u32(
            read_u32(bytes, 20),
            SERIES_HOT_PAGE_HEADER_LEN_V1 as u32,
            "series v3 hot page header length",
        )?;
        require_eq_u32(
            read_u32(bytes, 24),
            SERIES_HOT_RECORD_LEN_V3 as u32,
            "series v3 hot record length",
        )?;
        require_eq_u32(
            read_u32(bytes, 28),
            SERIES_HOT_RECORDS_PER_PAGE_V1,
            "series v3 records per page",
        )?;
        require_eq_u32(
            read_u32(bytes, 56),
            SERIES_COLD_PAGE_LEN_V1 as u32,
            "series v3 cold page length",
        )?;

        let header = Self {
            num_series: read_u32(bytes, 32),
            page_count: read_u32(bytes, 36),
            num_keysets: read_u32(bytes, 40),
            num_value_dicts: read_u32(bytes, 44),
            chunk_index_root_crc32c: read_u32(bytes, 48),
            root_crc32c: read_u32(bytes, 52),
            cold_page_count: read_u32(bytes, 60),
            directory_offset: read_u64(bytes, 64),
            directory_len: read_u64(bytes, 72),
            hot_pages_offset: read_u64(bytes, 80),
            hot_pages_len: read_u64(bytes, 88),
            keysets_offset: read_u64(bytes, 96),
            keysets_len: read_u64(bytes, 104),
            value_dicts_offset: read_u64(bytes, 112),
            value_dicts_len: read_u64(bytes, 120),
            keyset_blocks_offset: read_u64(bytes, 128),
            keyset_blocks_len: read_u64(bytes, 136),
            segment_start_ms: read_u64(bytes, 144),
            segment_end_ms: read_u64(bytes, 152),
            chunk_index_file_len: read_u64(bytes, 160),
            file_len: read_u64(bytes, 168),
        };
        header.validate()?;
        Ok(header)
    }

    pub(crate) fn validate(self) -> io::Result<()> {
        if self.segment_start_ms >= self.segment_end_ms {
            return Err(invalid_data("series v3 segment bounds are invalid"));
        }
        if self.chunk_index_file_len < CHUNK_INDEX_ROOT_LEN_V2 {
            return Err(invalid_data(
                "series v3 chunk index is shorter than its root",
            ));
        }
        if self.num_series != 0 && (self.num_keysets == 0 || self.num_keysets > self.num_series) {
            return Err(invalid_data(
                "series v3 nonempty table has an invalid keyset count",
            ));
        }
        validate_offset_table_minimum(self.keysets_len, self.num_keysets, "keysets")?;
        validate_offset_table_minimum(
            self.value_dicts_len,
            self.num_value_dicts,
            "value dictionaries",
        )?;
        validate_offset_table_minimum(self.keyset_blocks_len, self.num_keysets, "keyset blocks")?;

        let expected = derive_layout(
            self.num_series,
            self.keysets_len,
            self.value_dicts_len,
            self.keyset_blocks_len,
        )?;
        if self.page_count != expected.page_count
            || self.cold_page_count != expected.cold_page_count
            || self.directory_offset != expected.directory_offset
            || self.directory_len != expected.directory_len
            || self.hot_pages_offset != expected.hot_pages_offset
            || self.hot_pages_len != expected.hot_pages_len
            || self.keysets_offset != expected.keysets_offset
            || self.value_dicts_offset != expected.value_dicts_offset
            || self.keyset_blocks_offset != expected.keyset_blocks_offset
            || self.file_len != expected.file_len
        {
            return Err(invalid_data("series v3 section layout is noncanonical"));
        }

        if self.num_series == 0
            && (self.num_keysets != 0
                || self.num_value_dicts != 0
                || self.keysets_len != 8
                || self.value_dicts_len != 8
                || self.keyset_blocks_len != 8
                || self.chunk_index_file_len != CHUNK_INDEX_ROOT_LEN_V2)
        {
            return Err(invalid_data("series v3 empty table is noncanonical"));
        }
        Ok(())
    }

    pub(crate) fn cold_bytes_len(self) -> io::Result<u64> {
        checked_sum3(
            self.keysets_len,
            self.value_dicts_len,
            self.keyset_blocks_len,
            "series v3 cold bytes overflow",
        )
    }

    fn expected_hot_record_count(self, page_index: u32) -> io::Result<u32> {
        if page_index >= self.page_count {
            return Err(invalid_data("series v3 hot page index is out of range"));
        }
        let first = page_index
            .checked_mul(SERIES_HOT_RECORDS_PER_PAGE_V1)
            .ok_or_else(|| invalid_data("series v3 first series ref overflow"))?;
        Ok((self.num_series - first).min(SERIES_HOT_RECORDS_PER_PAGE_V1))
    }

    fn expected_cold_page_len(self, page_index: u32) -> io::Result<u32> {
        if page_index >= self.cold_page_count {
            return Err(invalid_data("series v3 cold page index is out of range"));
        }
        let start = u64::from(page_index)
            .checked_mul(SERIES_COLD_PAGE_LEN_V1)
            .ok_or_else(|| invalid_data("series v3 cold page offset overflow"))?;
        let remaining = self
            .cold_bytes_len()?
            .checked_sub(start)
            .ok_or_else(|| invalid_data("series v3 cold page starts past EOF"))?;
        u32::try_from(remaining.min(SERIES_COLD_PAGE_LEN_V1))
            .map_err(|_| invalid_data("series v3 cold page length exceeds u32"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DerivedSeriesLayoutV3 {
    page_count: u32,
    cold_page_count: u32,
    directory_offset: u64,
    directory_len: u64,
    hot_pages_offset: u64,
    hot_pages_len: u64,
    keysets_offset: u64,
    value_dicts_offset: u64,
    keyset_blocks_offset: u64,
    file_len: u64,
}

fn derive_layout(
    num_series: u32,
    keysets_len: u64,
    value_dicts_len: u64,
    keyset_blocks_len: u64,
) -> io::Result<DerivedSeriesLayoutV3> {
    let page_count = ceil_div_u64(
        u64::from(num_series),
        u64::from(SERIES_HOT_RECORDS_PER_PAGE_V1),
    );
    let page_count = u32::try_from(page_count)
        .map_err(|_| invalid_data("series v3 hot page count exceeds u32"))?;
    let cold_bytes_len = checked_sum3(
        keysets_len,
        value_dicts_len,
        keyset_blocks_len,
        "series v3 cold bytes overflow",
    )?;
    let cold_page_count = ceil_div_u64(cold_bytes_len, SERIES_COLD_PAGE_LEN_V1);
    let cold_page_count = u32::try_from(cold_page_count)
        .map_err(|_| invalid_data("series v3 cold page count exceeds u32"))?;

    let directory_offset = SERIES_HEADER_LEN_V3 as u64;
    let descriptor_count = u64::from(page_count)
        .checked_add(u64::from(cold_page_count))
        .ok_or_else(|| invalid_data("series v3 descriptor count overflow"))?;
    let directory_len = descriptor_count
        .checked_mul(SERIES_DESCRIPTOR_LEN_V1 as u64)
        .ok_or_else(|| invalid_data("series v3 directory length overflow"))?;
    let directory_end = directory_offset
        .checked_add(directory_len)
        .ok_or_else(|| invalid_data("series v3 directory end overflow"))?;
    let hot_pages_offset = checked_align_up(directory_end, SERIES_ROOT_ALIGNMENT_V3)?;
    let hot_pages_len = u64::from(page_count)
        .checked_mul(SERIES_HOT_PAGE_LEN_V1 as u64)
        .ok_or_else(|| invalid_data("series v3 hot pages length overflow"))?;
    let keysets_offset = hot_pages_offset
        .checked_add(hot_pages_len)
        .ok_or_else(|| invalid_data("series v3 keysets offset overflow"))?;
    let value_dicts_offset = keysets_offset
        .checked_add(keysets_len)
        .ok_or_else(|| invalid_data("series v3 value dictionaries offset overflow"))?;
    let keyset_blocks_offset = value_dicts_offset
        .checked_add(value_dicts_len)
        .ok_or_else(|| invalid_data("series v3 keyset blocks offset overflow"))?;
    let file_len = keyset_blocks_offset
        .checked_add(keyset_blocks_len)
        .ok_or_else(|| invalid_data("series v3 file length overflow"))?;

    Ok(DerivedSeriesLayoutV3 {
        page_count,
        cold_page_count,
        directory_offset,
        directory_len,
        hot_pages_offset,
        hot_pages_len,
        keysets_offset,
        value_dicts_offset,
        keyset_blocks_offset,
        file_len,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHotPageDescriptorV1 {
    pub(crate) first_series_ref: u32,
    pub(crate) record_count: u32,
    pub(crate) page_crc32c: u32,
}

impl SeriesHotPageDescriptorV1 {
    fn encode(self) -> [u8; SERIES_DESCRIPTOR_LEN_V1] {
        let mut bytes = [0u8; SERIES_DESCRIPTOR_LEN_V1];
        put_u32(&mut bytes, 0, self.first_series_ref);
        put_u32(&mut bytes, 4, self.record_count);
        put_u32(&mut bytes, 8, self.page_crc32c);
        put_u32(&mut bytes, 12, 0);
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        require_len(bytes, SERIES_DESCRIPTOR_LEN_V1, "series v3 hot descriptor")?;
        require_zero_u32(read_u32(bytes, 12), "series v3 hot descriptor reserved0")?;
        Ok(Self {
            first_series_ref: read_u32(bytes, 0),
            record_count: read_u32(bytes, 4),
            page_crc32c: read_u32(bytes, 8),
        })
    }

    fn validate(self, header: SeriesHeaderV3, page_index: u32) -> io::Result<()> {
        let expected_first = page_index
            .checked_mul(SERIES_HOT_RECORDS_PER_PAGE_V1)
            .ok_or_else(|| invalid_data("series v3 first series ref overflow"))?;
        if self.first_series_ref != expected_first
            || self.record_count != header.expected_hot_record_count(page_index)?
        {
            return Err(invalid_data("series v3 hot descriptor is noncanonical"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesColdPageDescriptorV1 {
    pub(crate) page_index: u32,
    pub(crate) page_len: u32,
    pub(crate) page_crc32c: u32,
}

impl SeriesColdPageDescriptorV1 {
    pub(crate) fn new(
        header: SeriesHeaderV3,
        page_index: u32,
        page_crc32c: u32,
    ) -> io::Result<Self> {
        let descriptor = Self {
            page_index,
            page_len: header.expected_cold_page_len(page_index)?,
            page_crc32c,
        };
        descriptor.validate(header, page_index)?;
        Ok(descriptor)
    }

    fn encode(self) -> [u8; SERIES_DESCRIPTOR_LEN_V1] {
        let mut bytes = [0u8; SERIES_DESCRIPTOR_LEN_V1];
        put_u32(&mut bytes, 0, self.page_index);
        put_u32(&mut bytes, 4, self.page_len);
        put_u32(&mut bytes, 8, self.page_crc32c);
        put_u32(&mut bytes, 12, 0);
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        require_len(bytes, SERIES_DESCRIPTOR_LEN_V1, "series v3 cold descriptor")?;
        require_zero_u32(read_u32(bytes, 12), "series v3 cold descriptor reserved0")?;
        Ok(Self {
            page_index: read_u32(bytes, 0),
            page_len: read_u32(bytes, 4),
            page_crc32c: read_u32(bytes, 8),
        })
    }

    fn validate(self, header: SeriesHeaderV3, page_index: u32) -> io::Result<()> {
        if self.page_index != page_index
            || self.page_len != header.expected_cold_page_len(page_index)?
        {
            return Err(invalid_data("series v3 cold descriptor is noncanonical"));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SeriesRootV3 {
    header: SeriesHeaderV3,
    hot_descriptors: Vec<SeriesHotPageDescriptorV1>,
    cold_descriptors: Vec<SeriesColdPageDescriptorV1>,
}

impl SeriesRootV3 {
    /// Conservative final-allocation bound for a root discovered from its
    /// fixed header. The encoded root length covers every decoded descriptor,
    /// while the fixed value charge covers both `Vec` headers.
    pub(crate) fn declared_max_bytes(root_len: u64) -> io::Result<u64> {
        let fixed = u64::try_from(std::mem::size_of::<Self>())
            .map_err(|_| resource_error("series v3 root charge exceeds u64"))?;
        fixed
            .checked_add(root_len)
            .ok_or_else(|| resource_error("series v3 root declared charge overflows"))
    }

    /// Returns the measured logical bytes owned by this decoded root.
    pub(crate) fn charged_bytes(&self) -> io::Result<u64> {
        let hot_bytes = self
            .hot_descriptors
            .capacity()
            .checked_mul(std::mem::size_of::<SeriesHotPageDescriptorV1>())
            .ok_or_else(|| resource_error("series v3 root charge overflows"))?;
        let cold_bytes = self
            .cold_descriptors
            .capacity()
            .checked_mul(std::mem::size_of::<SeriesColdPageDescriptorV1>())
            .ok_or_else(|| resource_error("series v3 root charge overflows"))?;
        let charged = std::mem::size_of::<Self>()
            .checked_add(hot_bytes)
            .and_then(|bytes| bytes.checked_add(cold_bytes))
            .ok_or_else(|| resource_error("series v3 root charge overflows"))?;
        u64::try_from(charged).map_err(|_| resource_error("series v3 root charge exceeds u64"))
    }
}

pub(crate) fn encode_series_root_v3(
    mut header: SeriesHeaderV3,
    hot_descriptors: &[SeriesHotPageDescriptorV1],
    cold_descriptors: &[SeriesColdPageDescriptorV1],
) -> io::Result<(SeriesHeaderV3, Vec<u8>)> {
    header.validate()?;
    require_count(
        hot_descriptors.len(),
        header.page_count,
        "series v3 hot descriptor count",
    )?;
    require_count(
        cold_descriptors.len(),
        header.cold_page_count,
        "series v3 cold descriptor count",
    )?;
    for (page_index, descriptor) in hot_descriptors.iter().copied().enumerate() {
        descriptor.validate(header, checked_u32(page_index, "hot page index")?)?;
    }
    for (page_index, descriptor) in cold_descriptors.iter().copied().enumerate() {
        descriptor.validate(header, checked_u32(page_index, "cold page index")?)?;
    }

    header.root_crc32c = 0;
    let root_len = checked_usize(header.hot_pages_offset, "series v3 root length")?;
    let mut bytes = zeroed_vec(root_len, "series v3 root")?;
    bytes[..SERIES_HEADER_LEN_V3].copy_from_slice(&header.encode()?);
    let mut cursor = SERIES_HEADER_LEN_V3;
    for descriptor in hot_descriptors {
        let end = cursor + SERIES_DESCRIPTOR_LEN_V1;
        bytes[cursor..end].copy_from_slice(&descriptor.encode());
        cursor = end;
    }
    for descriptor in cold_descriptors {
        let end = cursor + SERIES_DESCRIPTOR_LEN_V1;
        bytes[cursor..end].copy_from_slice(&descriptor.encode());
        cursor = end;
    }
    debug_assert!(bytes[cursor..].iter().all(|byte| *byte == 0));

    header.root_crc32c = compute_series_root_crc32c(&bytes)?;
    put_u32(&mut bytes, SERIES_ROOT_CRC_OFFSET_V3, header.root_crc32c);
    Ok((header, bytes))
}

pub(crate) fn decode_series_root_v3(bytes: &[u8]) -> io::Result<SeriesRootV3> {
    if bytes.len() < SERIES_HEADER_LEN_V3 {
        return Err(invalid_data("series v3 root is shorter than its header"));
    }
    let header = SeriesHeaderV3::decode(&bytes[..SERIES_HEADER_LEN_V3])?;
    if bytes.len() != checked_usize(header.hot_pages_offset, "series v3 root length")? {
        return Err(invalid_data("series v3 root length"));
    }
    if compute_series_root_crc32c(bytes)? != header.root_crc32c {
        return Err(invalid_data("series v3 root CRC mismatch"));
    }

    let hot_count = usize::try_from(header.page_count)
        .map_err(|_| invalid_data("series v3 hot descriptor count exceeds usize"))?;
    let cold_count = usize::try_from(header.cold_page_count)
        .map_err(|_| invalid_data("series v3 cold descriptor count exceeds usize"))?;
    let descriptor_count = hot_count
        .checked_add(cold_count)
        .ok_or_else(|| invalid_data("series v3 descriptor count exceeds usize"))?;
    let directory_len = descriptor_count
        .checked_mul(SERIES_DESCRIPTOR_LEN_V1)
        .ok_or_else(|| invalid_data("series v3 directory length exceeds usize"))?;
    let directory_end = SERIES_HEADER_LEN_V3
        .checked_add(directory_len)
        .ok_or_else(|| invalid_data("series v3 directory end exceeds usize"))?;
    if bytes[directory_end..].iter().any(|byte| *byte != 0) {
        return Err(invalid_data("series v3 root padding is nonzero"));
    }

    let mut hot_descriptors = Vec::new();
    hot_descriptors
        .try_reserve_exact(hot_count)
        .map_err(|_| resource_error("series v3 hot descriptor allocation failed"))?;
    let mut cursor = SERIES_HEADER_LEN_V3;
    for page_index in 0..header.page_count {
        let end = cursor + SERIES_DESCRIPTOR_LEN_V1;
        let descriptor = SeriesHotPageDescriptorV1::decode(&bytes[cursor..end])?;
        descriptor.validate(header, page_index)?;
        hot_descriptors.push(descriptor);
        cursor = end;
    }

    let mut cold_descriptors = Vec::new();
    cold_descriptors
        .try_reserve_exact(cold_count)
        .map_err(|_| resource_error("series v3 cold descriptor allocation failed"))?;
    for page_index in 0..header.cold_page_count {
        let end = cursor + SERIES_DESCRIPTOR_LEN_V1;
        let descriptor = SeriesColdPageDescriptorV1::decode(&bytes[cursor..end])?;
        descriptor.validate(header, page_index)?;
        cold_descriptors.push(descriptor);
        cursor = end;
    }
    debug_assert_eq!(cursor, directory_end);

    Ok(SeriesRootV3 {
        header,
        hot_descriptors,
        cold_descriptors,
    })
}

fn compute_series_root_crc32c(bytes: &[u8]) -> io::Result<u32> {
    if bytes.len() < SERIES_ROOT_CRC_OFFSET_V3 + SERIES_ROOT_CRC_LEN_V3 {
        return Err(invalid_data("series v3 root is too short for its CRC"));
    }
    let crc = crc32c(&bytes[..SERIES_ROOT_CRC_OFFSET_V3]);
    let crc = crc32c_append(crc, &[0; SERIES_ROOT_CRC_LEN_V3]);
    Ok(crc32c_append(
        crc,
        &bytes[SERIES_ROOT_CRC_OFFSET_V3 + SERIES_ROOT_CRC_LEN_V3..],
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHotV3Context {
    segment_start_ms: u64,
    segment_end_ms: u64,
    chunk_file_lens: [u64; 2],
    chunk_index_file_len: u64,
}

impl SeriesHotV3Context {
    pub(crate) fn from_header(
        header: SeriesHeaderV3,
        chunk_file_lens: [u64; 2],
    ) -> io::Result<Self> {
        header.validate()?;
        Ok(Self {
            segment_start_ms: header.segment_start_ms,
            segment_end_ms: header.segment_end_ms,
            chunk_file_lens,
            chunk_index_file_len: header.chunk_index_file_len,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InlineChunkV3 {
    pub(crate) chunk_kind: u8,
    pub(crate) file_id: u8,
    pub(crate) scalar_lane_len: u32,
    pub(crate) min_time_delta_ms: u32,
    pub(crate) max_time_delta_ms: u32,
    pub(crate) file_offset: u32,
    pub(crate) chunk_length: u32,
    pub(crate) indexed_prefix_crc32c: u32,
}

impl InlineChunkV3 {
    pub(crate) fn scalar_lane_offset(self) -> u32 {
        if self.scalar_lane_len == 0 {
            0
        } else {
            CHUNK_HEADER_LEN_V1
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OverflowChunksV3 {
    pub(crate) blob_offset: u64,
    pub(crate) blob_len: u32,
    pub(crate) chunk_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeriesHotLocationV3 {
    Inline(InlineChunkV3),
    Overflow(OverflowChunksV3),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHotV3 {
    pub(crate) series_id: u64,
    pub(crate) keyset_id: u32,
    pub(crate) row: u32,
    pub(crate) kind_mask: u8,
    pub(crate) location: SeriesHotLocationV3,
}

impl SeriesHotV3 {
    pub(crate) fn encode(
        self,
        context: SeriesHotV3Context,
    ) -> io::Result<[u8; SERIES_HOT_RECORD_LEN_V3]> {
        self.validate(context)?;
        let mut bytes = [0u8; SERIES_HOT_RECORD_LEN_V3];
        put_u64(&mut bytes, 0, self.series_id);
        put_u32(&mut bytes, 8, self.keyset_id);
        put_u32(&mut bytes, 12, self.row);

        match self.location {
            SeriesHotLocationV3::Inline(inline) => {
                let control = u32::from(self.kind_mask)
                    | (u32::from(inline.chunk_kind) << SERIES_HOT_CHUNK_KIND_SHIFT)
                    | (u32::from(inline.file_id) << SERIES_HOT_FILE_ID_SHIFT)
                    | (SERIES_HOT_TAG_INLINE << SERIES_HOT_TAG_SHIFT)
                    | (inline.scalar_lane_len << SERIES_HOT_SCALAR_LANE_LEN_SHIFT);
                put_u32(&mut bytes, 16, control);
                put_u32(&mut bytes, 20, inline.min_time_delta_ms);
                put_u32(&mut bytes, 24, inline.max_time_delta_ms);
                put_u32(&mut bytes, 28, inline.file_offset);
                put_u32(&mut bytes, 32, inline.chunk_length);
                put_u32(&mut bytes, 36, inline.indexed_prefix_crc32c);
            }
            SeriesHotLocationV3::Overflow(overflow) => {
                let control =
                    u32::from(self.kind_mask) | (SERIES_HOT_TAG_OVERFLOW << SERIES_HOT_TAG_SHIFT);
                put_u32(&mut bytes, 16, control);
                put_u64(&mut bytes, 20, overflow.blob_offset);
                put_u32(&mut bytes, 28, overflow.blob_len);
                put_u32(&mut bytes, 32, overflow.chunk_count);
                put_u32(&mut bytes, 36, 0);
            }
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], context: SeriesHotV3Context) -> io::Result<Self> {
        require_len(bytes, SERIES_HOT_RECORD_LEN_V3, "series v3 hot record")?;
        let control = read_u32(bytes, 16);
        let kind_mask = (control & SERIES_HOT_KIND_MASK) as u8;
        let tag = (control >> SERIES_HOT_TAG_SHIFT) & SERIES_HOT_TAG_MASK;
        let location = match tag {
            SERIES_HOT_TAG_INLINE => SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind: ((control >> SERIES_HOT_CHUNK_KIND_SHIFT) & SERIES_HOT_CHUNK_KIND_MASK)
                    as u8,
                file_id: ((control >> SERIES_HOT_FILE_ID_SHIFT) & 1) as u8,
                scalar_lane_len: control >> SERIES_HOT_SCALAR_LANE_LEN_SHIFT,
                min_time_delta_ms: read_u32(bytes, 20),
                max_time_delta_ms: read_u32(bytes, 24),
                file_offset: read_u32(bytes, 28),
                chunk_length: read_u32(bytes, 32),
                indexed_prefix_crc32c: read_u32(bytes, 36),
            }),
            SERIES_HOT_TAG_OVERFLOW => {
                let non_kind_control = control
                    & !((SERIES_HOT_KIND_MASK) | (SERIES_HOT_TAG_MASK << SERIES_HOT_TAG_SHIFT));
                if non_kind_control != 0 {
                    return Err(invalid_data(
                        "series v3 overflow record has nonzero inline control fields",
                    ));
                }
                require_zero_u32(read_u32(bytes, 36), "series v3 overflow reserved0")?;
                SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                    blob_offset: read_u64(bytes, 20),
                    blob_len: read_u32(bytes, 28),
                    chunk_count: read_u32(bytes, 32),
                })
            }
            _ => return Err(invalid_data("series v3 hot record tag is invalid")),
        };
        let record = Self {
            series_id: read_u64(bytes, 0),
            keyset_id: read_u32(bytes, 8),
            row: read_u32(bytes, 12),
            kind_mask,
            location,
        };
        record.validate(context)?;
        Ok(record)
    }

    pub(crate) fn validate(self, context: SeriesHotV3Context) -> io::Result<()> {
        if self.kind_mask == 0 || u32::from(self.kind_mask) & !SERIES_HOT_KIND_MASK != 0 {
            return Err(invalid_data("series v3 kind mask is invalid"));
        }
        if context.segment_start_ms >= context.segment_end_ms {
            return Err(invalid_data("series v3 record segment bounds are invalid"));
        }
        if context.chunk_index_file_len < CHUNK_INDEX_ROOT_LEN_V2 {
            return Err(invalid_data("series v3 record chunk index is too short"));
        }

        match self.location {
            SeriesHotLocationV3::Inline(inline) => validate_inline(self.kind_mask, inline, context),
            SeriesHotLocationV3::Overflow(overflow) => validate_overflow(overflow, context),
        }
    }
}

fn validate_inline(
    kind_mask: u8,
    inline: InlineChunkV3,
    context: SeriesHotV3Context,
) -> io::Result<()> {
    if inline.chunk_kind > CHUNK_KIND_SUMMARY {
        return Err(invalid_data("series v3 inline chunk kind is invalid"));
    }
    let expected_kind_mask = 1u8
        .checked_shl(u32::from(inline.chunk_kind))
        .ok_or_else(|| invalid_data("series v3 inline chunk kind shift overflow"))?;
    if kind_mask != expected_kind_mask {
        return Err(invalid_data(
            "series v3 inline kind mask does not match chunk kind",
        ));
    }
    if inline.file_id > 1 {
        return Err(invalid_data("series v3 inline file ID is invalid"));
    }
    if inline.scalar_lane_len > SERIES_HOT_SCALAR_LANE_LEN_MAX {
        return Err(invalid_data(
            "series v3 inline scalar lane exceeds its field width",
        ));
    }
    if inline.scalar_lane_len != 0 && inline.scalar_lane_len < TYPED_SCALAR_LANE_HEADER_LEN_V1 {
        return Err(invalid_data(
            "series v3 inline scalar lane is shorter than its header",
        ));
    }
    if inline.scalar_lane_len != 0
        && !matches!(
            inline.chunk_kind,
            CHUNK_KIND_HISTOGRAM | CHUNK_KIND_EXPONENTIAL_HISTOGRAM | CHUNK_KIND_SUMMARY
        )
    {
        return Err(invalid_data(
            "series v3 inline scalar lane is invalid for its chunk kind",
        ));
    }
    let minimum_chunk_len = CHUNK_HEADER_LEN_V1
        .checked_add(inline.scalar_lane_len)
        .ok_or_else(|| invalid_data("series v3 inline chunk length overflow"))?;
    if inline.chunk_length < minimum_chunk_len {
        return Err(invalid_data(
            "series v3 inline chunk does not cover its indexed prefix",
        ));
    }

    let min_time_ms = context
        .segment_start_ms
        .checked_add(u64::from(inline.min_time_delta_ms))
        .ok_or_else(|| invalid_data("series v3 inline minimum time overflow"))?;
    let max_time_ms = context
        .segment_start_ms
        .checked_add(u64::from(inline.max_time_delta_ms))
        .ok_or_else(|| invalid_data("series v3 inline maximum time overflow"))?;
    if min_time_ms > max_time_ms || max_time_ms >= context.segment_end_ms {
        return Err(invalid_data("series v3 inline time range is invalid"));
    }

    let chunk_end = u64::from(inline.file_offset)
        .checked_add(u64::from(inline.chunk_length))
        .ok_or_else(|| invalid_data("series v3 inline chunk range overflow"))?;
    if chunk_end > context.chunk_file_lens[usize::from(inline.file_id)] {
        return Err(invalid_data(
            "series v3 inline chunk range is out of bounds",
        ));
    }
    Ok(())
}

fn validate_overflow(overflow: OverflowChunksV3, context: SeriesHotV3Context) -> io::Result<()> {
    if overflow.chunk_count == 0 {
        return Err(invalid_data("series v3 overflow chunk count is zero"));
    }
    let body_len = u64::from(overflow.chunk_count)
        .checked_mul(CHUNK_OVERFLOW_ENTRY_LEN_V1)
        .ok_or_else(|| invalid_data("series v3 overflow body length overflow"))?;
    if body_len > u64::from(u32::MAX) - CHUNK_OVERFLOW_BLOB_HEADER_LEN_V1 {
        return Err(invalid_data("series v3 overflow blob exceeds u32"));
    }
    let expected_blob_len = CHUNK_OVERFLOW_BLOB_HEADER_LEN_V1
        .checked_add(body_len)
        .ok_or_else(|| invalid_data("series v3 overflow blob length overflow"))?;
    if u64::from(overflow.blob_len) != expected_blob_len {
        return Err(invalid_data(
            "series v3 overflow blob length is noncanonical",
        ));
    }
    if overflow.blob_offset < CHUNK_INDEX_ROOT_LEN_V2 {
        return Err(invalid_data("series v3 overflow blob overlaps its root"));
    }
    let blob_end = overflow
        .blob_offset
        .checked_add(u64::from(overflow.blob_len))
        .ok_or_else(|| invalid_data("series v3 overflow blob range overflow"))?;
    if blob_end > context.chunk_index_file_len {
        return Err(invalid_data(
            "series v3 overflow blob range is out of bounds",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesHotPageHeaderV1 {
    pub(crate) page_index: u32,
    pub(crate) first_series_ref: u32,
    pub(crate) record_count: u32,
}

impl SeriesHotPageHeaderV1 {
    fn encode(self) -> [u8; SERIES_HOT_PAGE_HEADER_LEN_V1] {
        let mut bytes = [0u8; SERIES_HOT_PAGE_HEADER_LEN_V1];
        put_u32(&mut bytes, 0, SERIES_HOT_PAGE_MAGIC_V1);
        put_u16(&mut bytes, 4, SERIES_HOT_PAGE_VERSION_V1);
        put_u16(&mut bytes, 6, 0);
        put_u32(&mut bytes, 8, self.page_index);
        put_u32(&mut bytes, 12, self.first_series_ref);
        put_u32(&mut bytes, 16, self.record_count);
        put_u32(&mut bytes, 20, 0);
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        require_len(
            bytes,
            SERIES_HOT_PAGE_HEADER_LEN_V1,
            "series v3 hot page header",
        )?;
        require_eq_u32(
            read_u32(bytes, 0),
            SERIES_HOT_PAGE_MAGIC_V1,
            "series v3 hot page magic",
        )?;
        require_eq_u16(
            read_u16(bytes, 4),
            SERIES_HOT_PAGE_VERSION_V1,
            "series v3 hot page version",
        )?;
        require_zero_u16(read_u16(bytes, 6), "series v3 hot page flags")?;
        require_zero_u32(read_u32(bytes, 20), "series v3 hot page reserved0")?;
        Ok(Self {
            page_index: read_u32(bytes, 8),
            first_series_ref: read_u32(bytes, 12),
            record_count: read_u32(bytes, 16),
        })
    }

    fn validate(
        self,
        header: SeriesHeaderV3,
        descriptor: SeriesHotPageDescriptorV1,
        page_index: u32,
    ) -> io::Result<()> {
        descriptor.validate(header, page_index)?;
        if self.page_index != page_index
            || self.first_series_ref != descriptor.first_series_ref
            || self.record_count != descriptor.record_count
        {
            return Err(invalid_data("series v3 hot page header is noncanonical"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeriesHotPageV1 {
    pub(crate) header: SeriesHotPageHeaderV1,
    pub(crate) records: Vec<SeriesHotV3>,
}

struct ValidatedSeriesHotPageBytes<'a> {
    page_header: SeriesHotPageHeaderV1,
    encoded_records: &'a [u8],
    context: SeriesHotV3Context,
}

impl ValidatedSeriesHotPageBytes<'_> {
    fn visit_records<F>(&self, mut visit: F) -> io::Result<()>
    where
        F: FnMut(u32, SeriesHotV3) -> io::Result<()>,
    {
        for (record_index, encoded) in self
            .encoded_records
            .chunks_exact(SERIES_HOT_RECORD_LEN_V3)
            .enumerate()
        {
            let record_index = checked_u32(record_index, "series v3 hot record index exceeds u32")?;
            let series_ref = self
                .page_header
                .first_series_ref
                .checked_add(record_index)
                .ok_or_else(|| invalid_data("series v3 hot series ref overflows"))?;
            visit(series_ref, SeriesHotV3::decode(encoded, self.context)?)?;
        }
        Ok(())
    }
}

pub(crate) fn encode_series_hot_page_v1(
    header: SeriesHeaderV3,
    page_index: u32,
    records: &[SeriesHotV3],
    chunk_file_lens: [u64; 2],
) -> io::Result<(SeriesHotPageDescriptorV1, Vec<u8>)> {
    header.validate()?;
    require_count(
        records.len(),
        header.expected_hot_record_count(page_index)?,
        "series v3 hot page record count",
    )?;
    let first_series_ref = page_index
        .checked_mul(SERIES_HOT_RECORDS_PER_PAGE_V1)
        .ok_or_else(|| invalid_data("series v3 first series ref overflow"))?;
    let page_header = SeriesHotPageHeaderV1 {
        page_index,
        first_series_ref,
        record_count: checked_u32(records.len(), "hot page record count")?,
    };
    let context = SeriesHotV3Context::from_header(header, chunk_file_lens)?;
    let mut bytes = zeroed_vec(SERIES_HOT_PAGE_LEN_V1, "series v3 hot page")?;
    bytes[..SERIES_HOT_PAGE_HEADER_LEN_V1].copy_from_slice(&page_header.encode());
    let mut cursor = SERIES_HOT_PAGE_HEADER_LEN_V1;
    for record in records {
        let end = cursor + SERIES_HOT_RECORD_LEN_V3;
        bytes[cursor..end].copy_from_slice(&record.encode(context)?);
        cursor = end;
    }
    debug_assert!(bytes[cursor..].iter().all(|byte| *byte == 0));
    let descriptor = SeriesHotPageDescriptorV1 {
        first_series_ref,
        record_count: page_header.record_count,
        page_crc32c: crc32c(&bytes),
    };
    descriptor.validate(header, page_index)?;
    Ok((descriptor, bytes))
}

pub(crate) fn decode_series_hot_page_v1(
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesHotPageDescriptorV1,
    bytes: &[u8],
    chunk_file_lens: [u64; 2],
) -> io::Result<SeriesHotPageV1> {
    let validated =
        validate_series_hot_page_bytes_v1(header, page_index, descriptor, bytes, chunk_file_lens)?;
    let record_count = usize::try_from(validated.page_header.record_count)
        .map_err(|_| invalid_data("series v3 hot page record count exceeds usize"))?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(record_count)
        .map_err(|_| resource_error("series v3 hot record allocation failed"))?;
    validated.visit_records(|_, record| {
        records.push(record);
        Ok(())
    })?;
    Ok(SeriesHotPageV1 {
        header: validated.page_header,
        records,
    })
}

/// Authenticates and structurally validates a complete hot page without
/// allocating decoded record storage.
pub(crate) fn visit_series_hot_page_v1<F>(
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesHotPageDescriptorV1,
    bytes: &[u8],
    chunk_file_lens: [u64; 2],
    visit: F,
) -> io::Result<()>
where
    F: FnMut(u32, SeriesHotV3) -> io::Result<()>,
{
    validate_series_hot_page_bytes_v1(header, page_index, descriptor, bytes, chunk_file_lens)?
        .visit_records(visit)
}

fn validate_series_hot_page_bytes_v1<'a>(
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesHotPageDescriptorV1,
    bytes: &'a [u8],
    chunk_file_lens: [u64; 2],
) -> io::Result<ValidatedSeriesHotPageBytes<'a>> {
    header.validate()?;
    require_len(bytes, SERIES_HOT_PAGE_LEN_V1, "series v3 hot page")?;
    descriptor.validate(header, page_index)?;
    if crc32c(bytes) != descriptor.page_crc32c {
        return Err(invalid_data("series v3 hot page CRC mismatch"));
    }

    let page_header = SeriesHotPageHeaderV1::decode(&bytes[..SERIES_HOT_PAGE_HEADER_LEN_V1])?;
    page_header.validate(header, descriptor, page_index)?;
    let record_count = usize::try_from(page_header.record_count)
        .map_err(|_| invalid_data("series v3 hot page record count exceeds usize"))?;
    let records_len = record_count
        .checked_mul(SERIES_HOT_RECORD_LEN_V3)
        .ok_or_else(|| invalid_data("series v3 hot page records length overflow"))?;
    let records_end = SERIES_HOT_PAGE_HEADER_LEN_V1
        .checked_add(records_len)
        .ok_or_else(|| invalid_data("series v3 hot page records end overflow"))?;
    if bytes[records_end..].iter().any(|byte| *byte != 0) {
        return Err(invalid_data("series v3 hot page padding is nonzero"));
    }

    Ok(ValidatedSeriesHotPageBytes {
        page_header,
        encoded_records: &bytes[SERIES_HOT_PAGE_HEADER_LEN_V1..records_end],
        context: SeriesHotV3Context::from_header(header, chunk_file_lens)?,
    })
}

fn checked_sum3(a: u64, b: u64, c: u64, message: &'static str) -> io::Result<u64> {
    a.checked_add(b)
        .and_then(|sum| sum.checked_add(c))
        .ok_or_else(|| invalid_data(message))
}

fn checked_align_up(value: u64, alignment: u64) -> io::Result<u64> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or_else(|| invalid_data("series v3 alignment overflow"))
    }
}

fn ceil_div_u64(value: u64, divisor: u64) -> u64 {
    if value == 0 {
        0
    } else {
        1 + (value - 1) / divisor
    }
}

fn checked_usize(value: u64, name: &'static str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_data(name))
}

fn checked_u32(value: usize, name: &'static str) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| invalid_data(name))
}

fn zeroed_vec(len: usize, name: &'static str) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| resource_error(name))?;
    bytes.resize(len, 0);
    Ok(bytes)
}

fn require_len(bytes: &[u8], expected: usize, name: &'static str) -> io::Result<()> {
    if bytes.len() != expected {
        return Err(invalid_data(name));
    }
    Ok(())
}

fn require_count(actual: usize, expected: u32, name: &'static str) -> io::Result<()> {
    let expected = usize::try_from(expected).map_err(|_| invalid_data(name))?;
    if actual != expected {
        return Err(invalid_data(name));
    }
    Ok(())
}

fn require_eq_u16(actual: u16, expected: u16, name: &'static str) -> io::Result<()> {
    if actual != expected {
        return Err(invalid_data(name));
    }
    Ok(())
}

fn require_eq_u32(actual: u32, expected: u32, name: &'static str) -> io::Result<()> {
    if actual != expected {
        return Err(invalid_data(name));
    }
    Ok(())
}

fn require_zero_u16(value: u16, name: &'static str) -> io::Result<()> {
    require_eq_u16(value, 0, name)
}

fn require_zero_u32(value: u32, name: &'static str) -> io::Result<()> {
    require_eq_u32(value, 0, name)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn resource_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, message)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed-width slice"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed-width slice"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed-width slice"),
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK_FILE_LENS: [u64; 2] = [u32::MAX as u64 + 4_096; 2];

    fn header_params(num_series: u32) -> SeriesHeaderV3Params {
        SeriesHeaderV3Params {
            num_series,
            num_keysets: if num_series == 0 { 0 } else { 1 },
            num_value_dicts: if num_series == 0 { 0 } else { 1 },
            chunk_index_root_crc32c: 0x1020_3040,
            keysets_len: if num_series == 0 { 8 } else { 16 },
            value_dicts_len: if num_series == 0 { 8 } else { 16 },
            keyset_blocks_len: if num_series == 0 { 8 } else { 16 },
            segment_start_ms: 1_000,
            segment_end_ms: u64::from(u32::MAX) + 1_001,
            chunk_index_file_len: if num_series == 0 { 64 } else { 1 << 20 },
        }
    }

    fn header(num_series: u32) -> SeriesHeaderV3 {
        SeriesHeaderV3::new(header_params(num_series)).unwrap()
    }

    fn inline_record(index: u32) -> SeriesHotV3 {
        SeriesHotV3 {
            series_id: u64::from(index) + 100,
            keyset_id: 0,
            row: index,
            kind_mask: 1 << CHUNK_KIND_FLOAT,
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind: CHUNK_KIND_FLOAT,
                file_id: 0,
                scalar_lane_len: 0,
                min_time_delta_ms: index,
                max_time_delta_ms: index,
                file_offset: index,
                chunk_length: CHUNK_HEADER_LEN_V1,
                indexed_prefix_crc32c: 0xa0b0_c000 | index,
            }),
        }
    }

    fn update_root_crc(bytes: &mut [u8]) {
        put_u32(bytes, SERIES_ROOT_CRC_OFFSET_V3, 0);
        let crc = compute_series_root_crc32c(bytes).unwrap();
        put_u32(bytes, SERIES_ROOT_CRC_OFFSET_V3, crc);
    }

    #[test]
    fn canonical_empty_header_has_fixed_golden_layout() {
        let header = header(0);
        assert_eq!(header.page_count, 0);
        assert_eq!(header.cold_page_count, 1);
        assert_eq!(header.directory_offset, 176);
        assert_eq!(header.directory_len, 16);
        assert_eq!(header.hot_pages_offset, 4_096);
        assert_eq!(header.hot_pages_len, 0);
        assert_eq!(header.keysets_offset, 4_096);
        assert_eq!(header.value_dicts_offset, 4_104);
        assert_eq!(header.keyset_blocks_offset, 4_112);
        assert_eq!(header.file_len, 4_120);

        let bytes = header.encode().unwrap();
        assert_eq!(
            &bytes[0..32],
            &[
                b'S', b'E', b'R', b'I', 3, 0, 0, 0, 176, 0, 0, 0, 16, 0, 0, 0, 0, 64, 0, 0, 24, 0,
                0, 0, 40, 0, 0, 0, 153, 1, 0, 0,
            ]
        );
        assert_eq!(read_u32(&bytes, 32), 0);
        assert_eq!(read_u32(&bytes, 36), 0);
        assert_eq!(read_u32(&bytes, 60), 1);
        assert_eq!(read_u64(&bytes, 64), 176);
        assert_eq!(read_u64(&bytes, 80), 4_096);
        assert_eq!(read_u64(&bytes, 96), 4_096);
        assert_eq!(read_u64(&bytes, 112), 4_104);
        assert_eq!(read_u64(&bytes, 128), 4_112);
        assert_eq!(read_u64(&bytes, 168), 4_120);
        assert_eq!(SeriesHeaderV3::decode(&bytes).unwrap(), header);

        let descriptor = SeriesColdPageDescriptorV1::new(header, 0, 0x5566_7788).unwrap();
        let (encoded_header, root) = encode_series_root_v3(header, &[], &[descriptor]).unwrap();
        assert_ne!(encoded_header.root_crc32c, 0);
        assert_eq!(root.len(), 4_096);
        assert_eq!(decode_series_root_v3(&root).unwrap().header, encoded_header);
    }

    #[test]
    fn empty_header_rejects_each_noncanonical_empty_field() {
        let mut params = header_params(0);
        params.num_keysets = 1;
        assert!(SeriesHeaderV3::new(params).is_err());

        let mut params = header_params(0);
        params.num_value_dicts = 1;
        assert!(SeriesHeaderV3::new(params).is_err());

        let mut params = header_params(0);
        params.keysets_len = 9;
        assert!(SeriesHeaderV3::new(params).is_err());

        let mut params = header_params(0);
        params.value_dicts_len = 9;
        assert!(SeriesHeaderV3::new(params).is_err());

        let mut params = header_params(0);
        params.keyset_blocks_len = 9;
        assert!(SeriesHeaderV3::new(params).is_err());

        let mut params = header_params(0);
        params.chunk_index_file_len = CHUNK_INDEX_ROOT_LEN_V2 + 1;
        assert!(SeriesHeaderV3::new(params).is_err());
    }

    #[test]
    fn nonempty_header_enforces_keyset_counts_and_cold_offset_table_minima() {
        let boundary = header_params(1);
        SeriesHeaderV3::new(boundary).unwrap();

        let mut params = boundary;
        params.num_keysets = 0;
        let error = SeriesHeaderV3::new(params).unwrap_err();
        assert!(error.to_string().contains("invalid keyset count"));

        let mut params = boundary;
        params.num_keysets = 2;
        params.keysets_len = 24;
        params.keyset_blocks_len = 24;
        let error = SeriesHeaderV3::new(params).unwrap_err();
        assert!(error.to_string().contains("invalid keyset count"));

        for (params, expected) in [
            (
                {
                    let mut params = boundary;
                    params.keysets_len = 15;
                    params
                },
                "keysets section",
            ),
            (
                {
                    let mut params = boundary;
                    params.value_dicts_len = 15;
                    params
                },
                "value dictionaries section",
            ),
            (
                {
                    let mut params = boundary;
                    params.keyset_blocks_len = 15;
                    params
                },
                "keyset blocks section",
            ),
        ] {
            let error = SeriesHeaderV3::new(params).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }

        let encoded = header(1).encode().unwrap();
        let mut corrupted = encoded;
        put_u32(&mut corrupted, 40, 0);
        let error = SeriesHeaderV3::decode(&corrupted).unwrap_err();
        assert!(error.to_string().contains("invalid keyset count"));

        for (offset, expected) in [
            (104, "keysets section"),
            (120, "value dictionaries section"),
            (136, "keyset blocks section"),
        ] {
            let mut corrupted = encoded;
            put_u64(&mut corrupted, offset, 15);
            let error = SeriesHeaderV3::decode(&corrupted).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn header_page_arithmetic_is_exact_at_409_and_410_records() {
        let one_page = header(409);
        assert_eq!(one_page.page_count, 1);
        assert_eq!(one_page.hot_pages_len, 16_384);
        assert_eq!(one_page.expected_hot_record_count(0).unwrap(), 409);

        let two_pages = header(410);
        assert_eq!(two_pages.page_count, 2);
        assert_eq!(two_pages.hot_pages_len, 32_768);
        assert_eq!(two_pages.expected_hot_record_count(0).unwrap(), 409);
        assert_eq!(two_pages.expected_hot_record_count(1).unwrap(), 1);

        let records = (0..410).map(inline_record).collect::<Vec<_>>();
        let (first_descriptor, first_page) =
            encode_series_hot_page_v1(two_pages, 0, &records[..409], CHUNK_FILE_LENS).unwrap();
        let (second_descriptor, second_page) =
            encode_series_hot_page_v1(two_pages, 1, &records[409..], CHUNK_FILE_LENS).unwrap();
        assert_eq!(first_descriptor.record_count, 409);
        assert_eq!(second_descriptor.first_series_ref, 409);
        assert_eq!(second_descriptor.record_count, 1);
        assert_eq!(
            decode_series_hot_page_v1(
                two_pages,
                0,
                first_descriptor,
                &first_page,
                CHUNK_FILE_LENS,
            )
            .unwrap()
            .records,
            records[..409],
        );
        assert_eq!(
            decode_series_hot_page_v1(
                two_pages,
                1,
                second_descriptor,
                &second_page,
                CHUNK_FILE_LENS,
            )
            .unwrap()
            .records,
            records[409..],
        );
    }

    #[test]
    fn inline_record_has_exact_golden_bytes() {
        let header = header(1);
        let context = SeriesHotV3Context::from_header(header, CHUNK_FILE_LENS).unwrap();
        let record = SeriesHotV3 {
            series_id: 0x0807_0605_0403_0201,
            keyset_id: 0x0c0b_0a09,
            row: 0x100f_0e0d,
            kind_mask: 1 << CHUNK_KIND_HISTOGRAM,
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind: CHUNK_KIND_HISTOGRAM,
                file_id: 1,
                scalar_lane_len: 16,
                min_time_delta_ms: 0x1413_1211,
                max_time_delta_ms: 0x1817_1615,
                file_offset: 0x1c1b_1a19,
                chunk_length: 0x201f_1e1d,
                indexed_prefix_crc32c: 0x2423_2221,
            }),
        };
        let bytes = record.encode(context).unwrap();
        assert_eq!(
            bytes,
            [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 68, 131, 0, 0, 17, 18, 19,
                20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
            ]
        );
        assert_eq!(SeriesHotV3::decode(&bytes, context).unwrap(), record);
        let SeriesHotLocationV3::Inline(inline) = record.location else {
            panic!("expected inline record");
        };
        assert_eq!(inline.scalar_lane_offset(), 40);
    }

    #[test]
    fn overflow_record_has_exact_golden_bytes() {
        let header = header(1);
        let context = SeriesHotV3Context::from_header(header, CHUNK_FILE_LENS).unwrap();
        let record = SeriesHotV3 {
            series_id: 0x0807_0605_0403_0201,
            keyset_id: 0x0c0b_0a09,
            row: 0x100f_0e0d,
            kind_mask: 0b1_0101,
            location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                blob_offset: 64,
                blob_len: 32 + 44,
                chunk_count: 1,
            }),
        };
        let bytes = record.encode(context).unwrap();
        assert_eq!(
            bytes,
            [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0x15, 0x04, 0, 0, 64, 0, 0,
                0, 0, 0, 0, 0, 76, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
        assert_eq!(SeriesHotV3::decode(&bytes, context).unwrap(), record);
    }

    #[test]
    fn inline_records_accept_all_five_chunk_kinds() {
        let context = SeriesHotV3Context::from_header(header(5), CHUNK_FILE_LENS).unwrap();
        for chunk_kind in [
            CHUNK_KIND_FLOAT,
            CHUNK_KIND_INT64,
            CHUNK_KIND_HISTOGRAM,
            CHUNK_KIND_EXPONENTIAL_HISTOGRAM,
            CHUNK_KIND_SUMMARY,
        ] {
            let record = SeriesHotV3 {
                series_id: u64::from(chunk_kind),
                keyset_id: 0,
                row: u32::from(chunk_kind),
                kind_mask: 1 << chunk_kind,
                location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                    chunk_kind,
                    file_id: 0,
                    scalar_lane_len: 0,
                    min_time_delta_ms: 0,
                    max_time_delta_ms: 0,
                    file_offset: 0,
                    chunk_length: CHUNK_HEADER_LEN_V1,
                    indexed_prefix_crc32c: 0,
                }),
            };
            let bytes = record.encode(context).unwrap();
            assert_eq!(SeriesHotV3::decode(&bytes, context).unwrap(), record);
        }
    }

    #[test]
    fn hot_records_reject_invalid_tags_and_reserved_overflow_fields() {
        let context = SeriesHotV3Context::from_header(header(1), CHUNK_FILE_LENS).unwrap();
        let mut bytes = inline_record(0).encode(context).unwrap();
        let control = read_u32(&bytes, 16);
        put_u32(
            &mut bytes,
            16,
            control & !(SERIES_HOT_TAG_MASK << SERIES_HOT_TAG_SHIFT),
        );
        assert!(SeriesHotV3::decode(&bytes, context).is_err());
        put_u32(
            &mut bytes,
            16,
            (control & !(SERIES_HOT_TAG_MASK << SERIES_HOT_TAG_SHIFT))
                | (3 << SERIES_HOT_TAG_SHIFT),
        );
        assert!(SeriesHotV3::decode(&bytes, context).is_err());

        let overflow = SeriesHotV3 {
            series_id: 7,
            keyset_id: 0,
            row: 0,
            kind_mask: 1,
            location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                blob_offset: 64,
                blob_len: 32 + 44,
                chunk_count: 1,
            }),
        };
        let mut bytes = overflow.encode(context).unwrap();
        let control = read_u32(&bytes, 16);
        put_u32(&mut bytes, 16, control | (1 << SERIES_HOT_FILE_ID_SHIFT));
        assert!(SeriesHotV3::decode(&bytes, context).is_err());
        put_u32(&mut bytes, 16, control);
        put_u32(&mut bytes, 36, 1);
        assert!(SeriesHotV3::decode(&bytes, context).is_err());
    }

    #[test]
    fn inline_scalar_width_and_shape_are_canonical() {
        let context = SeriesHotV3Context::from_header(header(1), CHUNK_FILE_LENS).unwrap();
        let base = SeriesHotV3 {
            series_id: 1,
            keyset_id: 0,
            row: 0,
            kind_mask: 1 << CHUNK_KIND_HISTOGRAM,
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind: CHUNK_KIND_HISTOGRAM,
                file_id: 0,
                scalar_lane_len: 16,
                min_time_delta_ms: 0,
                max_time_delta_ms: 1,
                file_offset: 0,
                chunk_length: 56,
                indexed_prefix_crc32c: 9,
            }),
        };
        assert!(base.encode(context).is_ok());

        let with_scalar_len = |scalar_lane_len: u32, chunk_length: u32| SeriesHotV3 {
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                scalar_lane_len,
                chunk_length,
                ..match base.location {
                    SeriesHotLocationV3::Inline(inline) => inline,
                    SeriesHotLocationV3::Overflow(_) => unreachable!(),
                }
            }),
            ..base
        };
        assert!(with_scalar_len(15, 55).encode(context).is_err());
        assert!(
            with_scalar_len(
                SERIES_HOT_SCALAR_LANE_LEN_MAX,
                CHUNK_HEADER_LEN_V1 + SERIES_HOT_SCALAR_LANE_LEN_MAX,
            )
            .encode(context)
            .is_ok()
        );
        assert!(
            with_scalar_len(SERIES_HOT_SCALAR_LANE_LEN_MAX + 1, u32::MAX)
                .encode(context)
                .is_err()
        );
        assert!(with_scalar_len(16, 55).encode(context).is_err());

        let scalar_float = SeriesHotV3 {
            kind_mask: 1 << CHUNK_KIND_FLOAT,
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind: CHUNK_KIND_FLOAT,
                ..match base.location {
                    SeriesHotLocationV3::Inline(inline) => inline,
                    SeriesHotLocationV3::Overflow(_) => unreachable!(),
                }
            }),
            ..base
        };
        assert!(scalar_float.encode(context).is_err());
    }

    #[test]
    fn root_and_hot_page_reject_authenticated_nonzero_padding_and_reserved_bytes() {
        let header = header(1);
        let records = [inline_record(0)];
        let (hot_descriptor, mut page) =
            encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
        let cold_descriptor = SeriesColdPageDescriptorV1::new(header, 0, 0).unwrap();
        let (_, mut root) =
            encode_series_root_v3(header, &[hot_descriptor], &[cold_descriptor]).unwrap();

        let mut bad_root_crc = root.clone();
        bad_root_crc[SERIES_ROOT_CRC_OFFSET_V3] ^= 1;
        assert!(decode_series_root_v3(&bad_root_crc).is_err());

        let mut bad_page_crc = page.clone();
        bad_page_crc[SERIES_HOT_PAGE_HEADER_LEN_V1] ^= 1;
        assert!(
            decode_series_hot_page_v1(header, 0, hot_descriptor, &bad_page_crc, CHUNK_FILE_LENS,)
                .is_err()
        );

        let directory_end = SERIES_HEADER_LEN_V3 + 2 * SERIES_DESCRIPTOR_LEN_V1;
        root[directory_end] = 1;
        update_root_crc(&mut root);
        assert!(decode_series_root_v3(&root).is_err());

        let (_, mut root) =
            encode_series_root_v3(header, &[hot_descriptor], &[cold_descriptor]).unwrap();
        put_u32(&mut root, SERIES_HEADER_LEN_V3 + 12, 1);
        update_root_crc(&mut root);
        assert!(decode_series_root_v3(&root).is_err());

        let (_, mut root) =
            encode_series_root_v3(header, &[hot_descriptor], &[cold_descriptor]).unwrap();
        put_u32(
            &mut root,
            SERIES_HEADER_LEN_V3 + SERIES_DESCRIPTOR_LEN_V1 + 12,
            1,
        );
        update_root_crc(&mut root);
        assert!(decode_series_root_v3(&root).is_err());

        let padding_start = SERIES_HOT_PAGE_HEADER_LEN_V1 + SERIES_HOT_RECORD_LEN_V3;
        page[padding_start] = 1;
        let descriptor = SeriesHotPageDescriptorV1 {
            page_crc32c: crc32c(&page),
            ..hot_descriptor
        };
        assert!(decode_series_hot_page_v1(header, 0, descriptor, &page, CHUNK_FILE_LENS).is_err());

        page[padding_start] = 0;
        put_u32(&mut page, 20, 1);
        let descriptor = SeriesHotPageDescriptorV1 {
            page_crc32c: crc32c(&page),
            ..hot_descriptor
        };
        assert!(decode_series_hot_page_v1(header, 0, descriptor, &page, CHUNK_FILE_LENS).is_err());
    }

    #[test]
    fn checked_header_and_record_arithmetic_rejects_overflow_and_bounds_errors() {
        let mut params = header_params(1);
        params.keysets_len = u64::MAX;
        assert!(SeriesHeaderV3::new(params).is_err());

        let context = SeriesHotV3Context::from_header(header(1), CHUNK_FILE_LENS).unwrap();
        let mut record = inline_record(0);
        let SeriesHotLocationV3::Inline(ref mut inline) = record.location else {
            unreachable!();
        };
        inline.file_id = 1;
        inline.file_offset = (1 << 20) - 39;
        let narrow_file_context = SeriesHotV3Context {
            chunk_file_lens: [CHUNK_FILE_LENS[0], 1 << 20],
            ..context
        };
        assert!(record.encode(narrow_file_context).is_err());

        let overflowing_time_context = SeriesHotV3Context {
            segment_start_ms: u64::MAX - 5,
            segment_end_ms: u64::MAX,
            chunk_file_lens: CHUNK_FILE_LENS,
            chunk_index_file_len: 1 << 20,
        };
        let mut record = inline_record(0);
        let SeriesHotLocationV3::Inline(ref mut inline) = record.location else {
            unreachable!();
        };
        inline.min_time_delta_ms = 6;
        inline.max_time_delta_ms = 6;
        assert!(record.encode(overflowing_time_context).is_err());

        let exact_u32_boundary = SeriesHotV3 {
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                min_time_delta_ms: u32::MAX,
                max_time_delta_ms: u32::MAX,
                file_offset: u32::MAX,
                chunk_length: CHUNK_HEADER_LEN_V1,
                ..match inline_record(0).location {
                    SeriesHotLocationV3::Inline(inline) => inline,
                    SeriesHotLocationV3::Overflow(_) => unreachable!(),
                }
            }),
            ..inline_record(0)
        };
        assert!(exact_u32_boundary.encode(context).is_ok());

        let too_many_chunks = u32::MAX / CHUNK_OVERFLOW_ENTRY_LEN_V1 as u32;
        let overflow = SeriesHotV3 {
            series_id: 1,
            keyset_id: 0,
            row: 0,
            kind_mask: 1,
            location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                blob_offset: 64,
                blob_len: u32::MAX,
                chunk_count: too_many_chunks,
            }),
        };
        assert!(overflow.encode(context).is_err());
    }
}
