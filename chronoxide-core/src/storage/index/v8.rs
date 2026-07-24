//! Private container-v8 encoder and byte-layout primitives.
//!
//! Its only parent-visible writer entry point first binds the finalized series
//! inventory and symbol dictionary from the same seal. The raw numeric-count
//! encoder remains private so caller-provided counts cannot become production
//! authority.

use std::io::{self, BufWriter, Seek, SeekFrom, Write};

use crc32c::{crc32c, crc32c_append};
use fst::{Set, Streamer};

use super::{
    LabelValueTimeRange, MetricSeriesRangeBlobBounds, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, SEGMENT_INDEX_TRAILER_MAGIC, SEGMENT_INDEXES_MAGIC,
    SegmentIndexes, validate_metric_series_range_sequence, walk_metric_series_ranges_blob,
    write_metric_series_ranges_blob,
};

mod codec;
#[allow(dead_code)] // Wired through the schema-neutral facade after this governed checkpoint.
pub(super) mod runtime;
mod seal;

const VERSION_V8: u16 = 8;
const VERSION_V9: u16 = 9;
const HEADER_LEN: usize = 16;
const TRAILER_LEN: usize = 256;
const TERMINAL_MAGIC_V8: u32 = u32::from_le_bytes(*b"S8ND");
const TERMINAL_MAGIC_V9: u32 = u32::from_le_bytes(*b"S9ND");

const EXACT_DIRECTORY_MAGIC_V8: u32 = u32::from_le_bytes(*b"EXD8");
const EXACT_DIRECTORY_MAGIC_V9: u32 = u32::from_le_bytes(*b"EXD9");
const EXACT_DIRECTORY_VERSION_V8: u16 = 2;
const EXACT_DIRECTORY_VERSION_V9: u16 = 3;
const EXACT_DIRECTORY_HEADER_LEN: usize = 64;
const EXACT_PAGE_DESCRIPTOR_LEN: usize = 32;
const EXACT_PAGE_MAGIC_V8: u32 = u32::from_le_bytes(*b"XPG8");
const EXACT_PAGE_MAGIC_V9: u32 = u32::from_le_bytes(*b"XPG9");
const EXACT_PAGE_VERSION_V8: u16 = 2;
const EXACT_PAGE_VERSION_V9: u16 = 3;
const EXACT_PAGE_LEN: usize = 16_384;
const EXACT_PAGE_HEADER_LEN: usize = 16;
const EXACT_RECORD_LEN: usize = 48;
const EXACT_RECORDS_PER_PAGE: usize = 341;

const AUXILIARY_DIRECTORY_MAGIC: u32 = u32::from_le_bytes(*b"AUX8");
const AUXILIARY_DIRECTORY_VERSION: u16 = 2;
const AUXILIARY_DIRECTORY_HEADER_LEN: usize = 64;
const AUXILIARY_RECORD_LEN: usize = 48;

const TRAILER_FILE_LEN_OFFSET: usize = 16;
const TRAILER_ROUTING_LOCATOR_OFFSET: usize = 24;
const TRAILER_METRIC_LOCATOR_OFFSET: usize = 40;
const TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET: usize = 56;
const TRAILER_EXACT_PAGES_LOCATOR_OFFSET: usize = 72;
const TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET: usize = 88;
const TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET: usize = 104;
const TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET: usize = 120;
const TRAILER_EXACT_ENTRY_COUNT_OFFSET: usize = 136;
const TRAILER_EXACT_PAGE_COUNT_OFFSET: usize = 144;
const TRAILER_EXACT_RECORD_LEN_OFFSET: usize = 148;
const TRAILER_EXACT_PAGE_LEN_OFFSET: usize = 152;
const TRAILER_AUX_ENTRY_COUNT_OFFSET: usize = 156;
const TRAILER_CRC_OFFSET: usize = 160;
const TRAILER_SERIES_COUNT_OFFSET: usize = 164;
const TRAILER_SYMBOL_COUNT_OFFSET: usize = 168;
const TRAILER_EXACT_DIRECTORY_CRC_OFFSET: usize = 172;
const TRAILER_AUX_DIRECTORY_CRC_OFFSET: usize = 176;
const TRAILER_RESERVED_OFFSET: usize = 180;
const TRAILER_TERMINAL_MAGIC_OFFSET: usize = 252;

const OUTPUT_BUFFER_LEN: usize = 64 * 1024;
const POSTINGS_SCRATCH_LEN: usize = 64 * 1024;
const EXACT_POSTINGS_CODEC_RAW32: u8 = 0;
const EXACT_POSTINGS_CODEC_DELTA_ULEB128: u8 = 1;
const EXACT_POSTINGS_V9_HEADER_LEN: u64 = 4;
const UNCONSTRAINED_TIME_RANGE: LabelValueTimeRange = LabelValueTimeRange {
    min_time_ms: 0,
    max_time_ms: u64::MAX,
};

pub(super) fn write_segment_indexes_v8_for_roots<
    S: crate::storage::series::SeriesEntryStore + ?Sized,
>(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &crate::storage::series::SegmentSymbols,
    series: &S,
) -> io::Result<()> {
    seal::write_segment_indexes_v8_for_roots(writer, indexes, num_series, symbols, series)
}

pub(super) fn write_segment_indexes_v9_for_roots<
    S: crate::storage::series::SeriesEntryStore + ?Sized,
>(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &crate::storage::series::SegmentSymbols,
    series: &S,
) -> io::Result<()> {
    seal::write_segment_indexes_v9_for_roots(writer, indexes, num_series, symbols, series)
}

#[cfg(test)]
pub(super) fn write_segment_indexes_v8_unbound_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    series_count: u32,
    symbol_count: u32,
) -> io::Result<()> {
    encode_segment_indexes_v8(
        writer,
        indexes,
        RootCounts {
            series: series_count,
            symbols: symbol_count,
        },
    )
}

#[cfg(test)]
pub(super) fn write_segment_indexes_v9_unbound_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    series_count: u32,
    symbol_count: u32,
) -> io::Result<()> {
    encode_segment_indexes_v9(
        writer,
        indexes,
        RootCounts {
            series: series_count,
            symbols: symbol_count,
        },
    )
}

#[cfg(test)]
pub(super) fn corrupt_exact_postings_payload_for_test(
    bytes: &mut [u8],
    key: (u32, u32),
) -> io::Result<()> {
    let trailer_start = bytes
        .len()
        .checked_sub(TRAILER_LEN)
        .ok_or_else(|| invalid_data("v8 test index has no trailer"))?;
    let trailer = bytes
        .get(trailer_start..)
        .ok_or_else(|| invalid_data("v8 test index trailer is truncated"))?;
    let exact_pages = BlobLocator {
        offset: read_u64(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET),
        len: read_u64(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET + 8),
    };
    let page_count = read_u32(trailer, TRAILER_EXACT_PAGE_COUNT_OFFSET);
    for page_index in 0..page_count {
        let page_offset = exact_pages
            .offset
            .checked_add(u64::from(page_index) * EXACT_PAGE_LEN as u64)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or_else(|| invalid_data("v8 test exact-page offset overflows"))?;
        let page = bytes
            .get(page_offset..page_offset + EXACT_PAGE_LEN)
            .ok_or_else(|| invalid_data("v8 test exact page is truncated"))?;
        let record_count = usize::try_from(read_u32(page, 12))
            .map_err(|_| invalid_data("v8 test exact record count exceeds usize"))?;
        for record_index in 0..record_count {
            let record_offset = EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN;
            let record = page
                .get(record_offset..record_offset + EXACT_RECORD_LEN)
                .ok_or_else(|| invalid_data("v8 test exact record is truncated"))?;
            if (read_u32(record, 0), read_u32(record, 4)) != key {
                continue;
            }
            let payload_offset = usize::try_from(read_u64(record, 8))
                .map_err(|_| invalid_data("v8 test postings offset exceeds usize"))?;
            let payload_len = usize::try_from(read_u64(record, 16))
                .map_err(|_| invalid_data("v8 test postings length exceeds usize"))?;
            let payload = bytes
                .get_mut(payload_offset..payload_offset + payload_len)
                .ok_or_else(|| invalid_data("v8 test postings payload is truncated"))?;
            let byte = payload
                .last_mut()
                .ok_or_else(|| invalid_data("v8 test postings payload is empty"))?;
            *byte ^= 1;
            return Ok(());
        }
    }
    Err(invalid_data("v8 test exact key is absent"))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BlobLocator {
    offset: u64,
    len: u64,
}

impl BlobLocator {
    fn end(self, description: &'static str) -> io::Result<u64> {
        self.offset
            .checked_add(self.len)
            .ok_or_else(|| invalid_data(description))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RootCounts {
    series: u32,
    symbols: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AuthenticatedIndexFormat {
    V8Raw,
    V9Adaptive,
}

impl AuthenticatedIndexFormat {
    const fn version(self) -> u16 {
        match self {
            Self::V8Raw => VERSION_V8,
            Self::V9Adaptive => VERSION_V9,
        }
    }

    const fn terminal_magic(self) -> u32 {
        match self {
            Self::V8Raw => TERMINAL_MAGIC_V8,
            Self::V9Adaptive => TERMINAL_MAGIC_V9,
        }
    }

    const fn exact_directory_magic(self) -> u32 {
        match self {
            Self::V8Raw => EXACT_DIRECTORY_MAGIC_V8,
            Self::V9Adaptive => EXACT_DIRECTORY_MAGIC_V9,
        }
    }

    const fn exact_directory_version(self) -> u16 {
        match self {
            Self::V8Raw => EXACT_DIRECTORY_VERSION_V8,
            Self::V9Adaptive => EXACT_DIRECTORY_VERSION_V9,
        }
    }

    const fn exact_page_magic(self) -> u32 {
        match self {
            Self::V8Raw => EXACT_PAGE_MAGIC_V8,
            Self::V9Adaptive => EXACT_PAGE_MAGIC_V9,
        }
    }

    const fn exact_page_version(self) -> u16 {
        match self {
            Self::V8Raw => EXACT_PAGE_VERSION_V8,
            Self::V9Adaptive => EXACT_PAGE_VERSION_V9,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentIndexV8Layout {
    format: AuthenticatedIndexFormat,
    routing: BlobLocator,
    metric: BlobLocator,
    exact_directory: BlobLocator,
    exact_pages: BlobLocator,
    exact_postings: BlobLocator,
    auxiliary_directory: BlobLocator,
    auxiliary_payloads: BlobLocator,
    exact_entry_count: u64,
    exact_page_count: u32,
    auxiliary_entry_count: u32,
    exact_directory_crc32c: u32,
    auxiliary_directory_crc32c: u32,
    counts: RootCounts,
    file_len: u64,
}

#[derive(Debug, Clone, Copy)]
struct PayloadLengths {
    routing: Option<u64>,
    metric: u64,
    exact_postings: u64,
    auxiliary: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExactPageDescriptor {
    first_key: (u32, u32),
    last_key: (u32, u32),
    record_count: u32,
    page_crc32c: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExactRecord {
    key: (u32, u32),
    postings: BlobLocator,
    time_range: LabelValueTimeRange,
    ref_count: u32,
    payload_crc32c: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExactDirectory {
    descriptors: Vec<ExactPageDescriptor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AuxiliaryRecord {
    kind: u16,
    label_name_sym: u32,
    payload: BlobLocator,
    time_range: LabelValueTimeRange,
    item_count: u32,
    payload_crc32c: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuxiliaryDirectory {
    records: Vec<AuxiliaryRecord>,
}

impl AuxiliaryDirectory {
    fn record(&self, kind: u16, label_name_sym: u32) -> Option<AuxiliaryRecord> {
        self.records
            .binary_search_by_key(&(kind, label_name_sym), |record| {
                (record.kind, record.label_name_sym)
            })
            .ok()
            .map(|index| self.records[index])
    }
}

#[derive(Debug, Clone)]
struct TimeRangeGroup {
    label_name_sym: u32,
    entries: Vec<(u32, LabelValueTimeRange)>,
    time_range: LabelValueTimeRange,
    payload_len: u64,
    payload_crc32c: u32,
}

#[derive(Debug)]
struct AuxiliaryPlan {
    time_range_groups: Vec<TimeRangeGroup>,
    payload_len: u64,
    entry_count: u32,
}

fn encode_segment_indexes_v8(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    counts: RootCounts,
) -> io::Result<()> {
    validate_indexes_for_private_write(indexes, counts)?;
    encode_validated_segment_indexes(writer, indexes, counts, AuthenticatedIndexFormat::V8Raw)
}

fn encode_segment_indexes_v9(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    counts: RootCounts,
) -> io::Result<()> {
    validate_indexes_for_private_write(indexes, counts)?;
    encode_validated_segment_indexes(
        writer,
        indexes,
        counts,
        AuthenticatedIndexFormat::V9Adaptive,
    )
}

/// Encodes indexes already proven by either the private structural validator
/// or the stronger same-seal validator. Keeping this entry private to the
/// authenticated-index module makes the proof boundary explicit and prevents
/// production callers from substituting root counts for finalized artifacts.
fn encode_validated_segment_indexes(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    counts: RootCounts,
    format: AuthenticatedIndexFormat,
) -> io::Result<()> {
    let routing_payload = indexes
        .routing_index
        .as_ref()
        .map(super::SegmentRoutingIndex::encode)
        .transpose()?;
    let metric_payload = write_metric_series_ranges_blob(&indexes.metric_series_ranges)?;
    walk_metric_series_ranges_blob(
        &metric_payload,
        Some(MetricSeriesRangeBlobBounds {
            num_series: counts.series,
            symbol_count: counts.symbols,
        }),
        |_| Ok(()),
    )?;
    let auxiliary_plan = plan_auxiliary(indexes)?;
    let exact_postings_len =
        indexes
            .exact_postings
            .entries()
            .try_fold(0u64, |total, (_name, _value, refs)| {
                total
                    .checked_add(exact_payload_len(format, refs)?)
                    .ok_or_else(|| invalid_input("exact-postings region length overflows"))
            })?;
    let exact_entry_count = u64::try_from(indexes.exact_postings.len())
        .map_err(|_| invalid_input("exact entry count exceeds u64"))?;
    let mut layout = plan_layout(
        PayloadLengths {
            routing: routing_payload
                .as_ref()
                .map(|bytes| usize_to_u64(bytes.len(), "routing payload length"))
                .transpose()?,
            metric: usize_to_u64(metric_payload.len(), "metric payload length")?,
            exact_postings: exact_postings_len,
            auxiliary: auxiliary_plan.payload_len,
        },
        exact_entry_count,
        u64::from(auxiliary_plan.entry_count),
        counts,
        format,
    )?;

    let auxiliary_directory = encode_auxiliary_directory(indexes, &auxiliary_plan, layout)?;
    layout.auxiliary_directory_crc32c = read_u32(&auxiliary_directory, 40);
    let header = encode_header(format);

    let mut writer = BufWriter::with_capacity(OUTPUT_BUFFER_LEN, writer);
    let mut written = 0u64;
    write_bytes(&mut writer, &mut written, &header)?;
    if let Some(payload) = routing_payload.as_deref() {
        write_bytes(&mut writer, &mut written, payload)?;
    }
    write_bytes(&mut writer, &mut written, &metric_payload)?;
    write_exact_payloads(
        &mut writer,
        &mut written,
        indexes,
        layout.exact_postings,
        format,
    )?;
    write_auxiliary_payloads(
        &mut writer,
        &mut written,
        indexes,
        &auxiliary_plan,
        layout.auxiliary_payloads,
    )?;
    if written != layout.exact_directory.offset {
        return Err(invalid_data(
            "exact directory begins at the wrong planned offset",
        ));
    }

    // The authenticated directory physically precedes the pages whose CRCs it
    // carries. Skip that known range, build and write every page exactly once,
    // then backpatch the complete directory after the descriptors are known.
    // Segment files are private temporary artifacts until the seal publishes
    // them, so the short-lived hole is never query-visible.
    let exact_pages_start = layout
        .exact_directory
        .end("exact directory end overflows")?;
    seek_exact(&mut writer, exact_pages_start, "exact-pages start")?;
    written = exact_pages_start;
    let exact_descriptors = write_exact_pages(
        &mut writer,
        &mut written,
        indexes,
        layout.exact_postings,
        layout.exact_pages,
        format,
    )?;
    let exact_directory = encode_exact_directory(&exact_descriptors, layout)?;
    layout.exact_directory_crc32c = read_u32(&exact_directory, 56);
    write_bytes(&mut writer, &mut written, &auxiliary_directory)?;
    let trailer = encode_trailer(layout);
    write_bytes(&mut writer, &mut written, &trailer)?;
    if written != layout.file_len {
        return Err(invalid_data("v8 writer emitted a noncanonical file length"));
    }

    seek_exact(
        &mut writer,
        layout.exact_directory.offset,
        "exact-directory backpatch",
    )?;
    let mut directory_written = layout.exact_directory.offset;
    write_bytes(&mut writer, &mut directory_written, &exact_directory)?;
    if directory_written != exact_pages_start {
        return Err(invalid_data(
            "exact directory backpatch disagrees with its planned range",
        ));
    }
    seek_exact(&mut writer, layout.file_len, "authenticated index file end")?;
    writer.flush()
}

/// Root-unbound fixture entry. Production code can reach the v8 encoder only
/// through [`write_segment_indexes_v8_for_roots`].
#[cfg(test)]
fn write_segment_indexes_v8(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    counts: RootCounts,
) -> io::Result<()> {
    encode_segment_indexes_v8(writer, indexes, counts)
}

fn validate_indexes_for_private_write(
    indexes: &SegmentIndexes,
    counts: RootCounts,
) -> io::Result<()> {
    indexes
        .metric_series_ranges
        .validate_complete_partition(counts.series, counts.symbols)?;
    for (_metric_sym, ranges) in indexes.metric_series_ranges.entries() {
        validate_metric_series_range_sequence(ranges, io::ErrorKind::InvalidInput)?;
    }
    for (name, value, refs) in indexes.exact_postings.entries() {
        if name >= counts.symbols || value >= counts.symbols {
            return Err(invalid_input(
                "exact-postings symbol exceeds the root symbol count",
            ));
        }
        if refs.is_empty() {
            return Err(invalid_input("exact-postings entry has no refs"));
        }
        if refs.iter().any(|series_ref| *series_ref >= counts.series) {
            return Err(invalid_input(
                "exact-postings ref exceeds the root series count",
            ));
        }
        if !refs.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(invalid_input(
                "exact-postings refs are not strictly ordered and unique",
            ));
        }
        let range = indexes
            .label_value_time_ranges
            .get(name, value)
            .ok_or_else(|| {
                invalid_input("exact-postings entry has no matching label-value time range")
            })?;
        if range.min_time_ms > range.max_time_ms {
            return Err(invalid_input("exact-postings time range is reversed"));
        }
    }
    for (label_name_sym, fst_bytes) in &indexes.label_values.fsts {
        if *label_name_sym >= counts.symbols {
            return Err(invalid_input(
                "FST label symbol exceeds the root symbol count",
            ));
        }
        validate_fst_bytes(fst_bytes, None)?;
    }
    for ((name, value), range) in &indexes.label_value_time_ranges.ranges {
        if *name >= counts.symbols || *value >= counts.symbols {
            return Err(invalid_input(
                "label-value range symbol exceeds the root symbol count",
            ));
        }
        if range.min_time_ms > range.max_time_ms {
            return Err(invalid_input("label-value time range is reversed"));
        }
    }
    Ok(())
}

fn plan_auxiliary(indexes: &SegmentIndexes) -> io::Result<AuxiliaryPlan> {
    let mut grouped = std::collections::BTreeMap::<u32, Vec<(u32, LabelValueTimeRange)>>::new();
    for ((name, value), range) in &indexes.label_value_time_ranges.ranges {
        grouped.entry(*name).or_default().push((*value, *range));
    }
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(grouped.len())
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    let mut payload_len = 0u64;
    for (label_name_sym, mut entries) in grouped {
        entries.sort_unstable_by_key(|(value, _range)| *value);
        if !entries.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            return Err(invalid_input(
                "label-value range symbols are not strictly ordered and unique",
            ));
        }
        let mut time_range = LabelValueTimeRange {
            min_time_ms: u64::MAX,
            max_time_ms: 0,
        };
        for (_value, range) in &entries {
            time_range.min_time_ms = time_range.min_time_ms.min(range.min_time_ms);
            time_range.max_time_ms = time_range.max_time_ms.max(range.max_time_ms);
        }
        let bytes = encode_time_range_payload(&entries)?;
        let group = TimeRangeGroup {
            label_name_sym,
            payload_len: usize_to_u64(bytes.len(), "time-range payload length")?,
            payload_crc32c: crc32c(&bytes),
            entries,
            time_range,
        };
        payload_len = payload_len
            .checked_add(group.payload_len)
            .ok_or_else(|| invalid_input("auxiliary payload length overflows"))?;
        groups.push(group);
    }
    for (label_name_sym, fst_bytes) in &indexes.label_values.fsts {
        let set = Set::new(fst_bytes.as_slice())
            .map_err(|error| invalid_input_owned(format!("invalid label-value FST: {error}")))?;
        let item_count =
            u32::try_from(set.len()).map_err(|_| invalid_input("FST item count exceeds u32"))?;
        if let Ok(group_index) =
            groups.binary_search_by_key(label_name_sym, |group| group.label_name_sym)
            && usize::try_from(item_count).ok() != Some(groups[group_index].entries.len())
        {
            return Err(invalid_input(
                "paired FST and time-range item counts disagree",
            ));
        }
        payload_len = payload_len
            .checked_add(usize_to_u64(fst_bytes.len(), "FST payload length")?)
            .ok_or_else(|| invalid_input("auxiliary payload length overflows"))?;
    }
    let entry_count = indexes
        .label_values
        .fsts
        .len()
        .checked_add(groups.len())
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| invalid_input("auxiliary entry count exceeds u32"))?;
    Ok(AuxiliaryPlan {
        time_range_groups: groups,
        payload_len,
        entry_count,
    })
}

fn plan_layout(
    lengths: PayloadLengths,
    exact_entry_count: u64,
    auxiliary_entry_count: u64,
    counts: RootCounts,
    format: AuthenticatedIndexFormat,
) -> io::Result<SegmentIndexV8Layout> {
    if (exact_entry_count == 0) != (lengths.exact_postings == 0) {
        return Err(invalid_input(
            "exact entries and exact payload presence disagree",
        ));
    }
    if (auxiliary_entry_count == 0) != (lengths.auxiliary == 0) {
        return Err(invalid_input(
            "auxiliary entries and payload presence disagree",
        ));
    }
    let exact_page_count = page_count(exact_entry_count)?;
    let auxiliary_entry_count = u32::try_from(auxiliary_entry_count)
        .map_err(|_| invalid_input("auxiliary entry count exceeds u32"))?;
    let exact_directory_len = (EXACT_DIRECTORY_HEADER_LEN as u64)
        .checked_add(
            u64::from(exact_page_count)
                .checked_mul(EXACT_PAGE_DESCRIPTOR_LEN as u64)
                .ok_or_else(|| invalid_input("exact directory length overflows"))?,
        )
        .ok_or_else(|| invalid_input("exact directory length overflows"))?;
    let exact_pages_len = u64::from(exact_page_count)
        .checked_mul(EXACT_PAGE_LEN as u64)
        .ok_or_else(|| invalid_input("exact-pages region length overflows"))?;
    let auxiliary_directory_len = (AUXILIARY_DIRECTORY_HEADER_LEN as u64)
        .checked_add(
            u64::from(auxiliary_entry_count)
                .checked_mul(AUXILIARY_RECORD_LEN as u64)
                .ok_or_else(|| invalid_input("auxiliary directory length overflows"))?,
        )
        .ok_or_else(|| invalid_input("auxiliary directory length overflows"))?;

    let mut cursor = HEADER_LEN as u64;
    let routing = optional_region(&mut cursor, lengths.routing, "routing payload")?;
    let metric = required_region(&mut cursor, lengths.metric, "metric payload")?;
    let exact_postings = optional_region(
        &mut cursor,
        (exact_entry_count != 0).then_some(lengths.exact_postings),
        "exact-postings payloads",
    )?;
    let auxiliary_payloads = optional_region(
        &mut cursor,
        (auxiliary_entry_count != 0).then_some(lengths.auxiliary),
        "auxiliary payloads",
    )?;
    let exact_directory = required_region(&mut cursor, exact_directory_len, "exact directory")?;
    let exact_pages = optional_region(
        &mut cursor,
        (exact_page_count != 0).then_some(exact_pages_len),
        "exact pages",
    )?;
    let auxiliary_directory =
        required_region(&mut cursor, auxiliary_directory_len, "auxiliary directory")?;
    let file_len = cursor
        .checked_add(TRAILER_LEN as u64)
        .ok_or_else(|| invalid_input("v8 file length overflows"))?;
    Ok(SegmentIndexV8Layout {
        format,
        routing,
        metric,
        exact_directory,
        exact_pages,
        exact_postings,
        auxiliary_directory,
        auxiliary_payloads,
        exact_entry_count,
        exact_page_count,
        auxiliary_entry_count,
        exact_directory_crc32c: 0,
        auxiliary_directory_crc32c: 0,
        counts,
        file_len,
    })
}

fn required_region(
    cursor: &mut u64,
    len: u64,
    description: &'static str,
) -> io::Result<BlobLocator> {
    if len == 0 {
        return Err(invalid_input_owned(format!(
            "zero-length {description} is not canonical"
        )));
    }
    let offset = *cursor;
    *cursor = cursor
        .checked_add(len)
        .ok_or_else(|| invalid_input_owned(format!("{description} end overflows")))?;
    Ok(BlobLocator { offset, len })
}

fn optional_region(
    cursor: &mut u64,
    len: Option<u64>,
    description: &'static str,
) -> io::Result<BlobLocator> {
    len.map_or(Ok(BlobLocator::default()), |len| {
        required_region(cursor, len, description)
    })
}

fn encode_header(format: AuthenticatedIndexFormat) -> [u8; HEADER_LEN] {
    let mut bytes = [0u8; HEADER_LEN];
    put_u32(&mut bytes, 0, SEGMENT_INDEXES_MAGIC);
    put_u16(&mut bytes, 4, format.version());
    put_u32(&mut bytes, 8, HEADER_LEN as u32);
    bytes
}

fn encode_trailer(layout: SegmentIndexV8Layout) -> [u8; TRAILER_LEN] {
    let mut bytes = [0u8; TRAILER_LEN];
    put_u32(&mut bytes, 0, SEGMENT_INDEX_TRAILER_MAGIC);
    put_u16(&mut bytes, 4, layout.format.version());
    put_u32(&mut bytes, 8, TRAILER_LEN as u32);
    put_u64(&mut bytes, TRAILER_FILE_LEN_OFFSET, layout.file_len);
    put_locator(&mut bytes, TRAILER_ROUTING_LOCATOR_OFFSET, layout.routing);
    put_locator(&mut bytes, TRAILER_METRIC_LOCATOR_OFFSET, layout.metric);
    put_locator(
        &mut bytes,
        TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET,
        layout.exact_directory,
    );
    put_locator(
        &mut bytes,
        TRAILER_EXACT_PAGES_LOCATOR_OFFSET,
        layout.exact_pages,
    );
    put_locator(
        &mut bytes,
        TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET,
        layout.exact_postings,
    );
    put_locator(
        &mut bytes,
        TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        layout.auxiliary_directory,
    );
    put_locator(
        &mut bytes,
        TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET,
        layout.auxiliary_payloads,
    );
    put_u64(
        &mut bytes,
        TRAILER_EXACT_ENTRY_COUNT_OFFSET,
        layout.exact_entry_count,
    );
    put_u32(
        &mut bytes,
        TRAILER_EXACT_PAGE_COUNT_OFFSET,
        layout.exact_page_count,
    );
    put_u32(
        &mut bytes,
        TRAILER_EXACT_RECORD_LEN_OFFSET,
        EXACT_RECORD_LEN as u32,
    );
    put_u32(
        &mut bytes,
        TRAILER_EXACT_PAGE_LEN_OFFSET,
        EXACT_PAGE_LEN as u32,
    );
    put_u32(
        &mut bytes,
        TRAILER_AUX_ENTRY_COUNT_OFFSET,
        layout.auxiliary_entry_count,
    );
    put_u32(
        &mut bytes,
        TRAILER_SERIES_COUNT_OFFSET,
        layout.counts.series,
    );
    put_u32(
        &mut bytes,
        TRAILER_SYMBOL_COUNT_OFFSET,
        layout.counts.symbols,
    );
    put_u32(
        &mut bytes,
        TRAILER_EXACT_DIRECTORY_CRC_OFFSET,
        layout.exact_directory_crc32c,
    );
    put_u32(
        &mut bytes,
        TRAILER_AUX_DIRECTORY_CRC_OFFSET,
        layout.auxiliary_directory_crc32c,
    );
    put_u32(
        &mut bytes,
        TRAILER_TERMINAL_MAGIC_OFFSET,
        layout.format.terminal_magic(),
    );
    let crc = crc_with_zeroed_field(&bytes, TRAILER_CRC_OFFSET);
    put_u32(&mut bytes, TRAILER_CRC_OFFSET, crc);
    bytes
}

fn encode_exact_directory(
    descriptors: &[ExactPageDescriptor],
    layout: SegmentIndexV8Layout,
) -> io::Result<Vec<u8>> {
    let capacity = u64_to_usize(layout.exact_directory.len, "exact directory length")?;
    let mut bytes = vec![0u8; EXACT_DIRECTORY_HEADER_LEN];
    bytes
        .try_reserve_exact(capacity.saturating_sub(EXACT_DIRECTORY_HEADER_LEN))
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    put_u32(&mut bytes, 0, layout.format.exact_directory_magic());
    put_u16(&mut bytes, 4, layout.format.exact_directory_version());
    put_u32(&mut bytes, 8, EXACT_DIRECTORY_HEADER_LEN as u32);
    put_u32(&mut bytes, 12, EXACT_PAGE_DESCRIPTOR_LEN as u32);
    put_u32(&mut bytes, 16, EXACT_PAGE_LEN as u32);
    put_u32(&mut bytes, 20, EXACT_RECORD_LEN as u32);
    put_u64(&mut bytes, 24, layout.exact_entry_count);
    put_u32(&mut bytes, 32, layout.exact_page_count);
    put_u32(&mut bytes, 36, EXACT_RECORDS_PER_PAGE as u32);
    put_u64(&mut bytes, 40, EXACT_DIRECTORY_HEADER_LEN as u64);
    put_u64(
        &mut bytes,
        48,
        u64::from(layout.exact_page_count) * EXACT_PAGE_DESCRIPTOR_LEN as u64,
    );
    if descriptors.len()
        != usize::try_from(layout.exact_page_count)
            .map_err(|_| invalid_data("exact page count exceeds usize"))?
    {
        return Err(invalid_data(
            "exact descriptor count disagrees with its plan",
        ));
    }
    for &descriptor in descriptors {
        append_exact_descriptor(&mut bytes, descriptor);
    }
    if bytes.len() != capacity {
        return Err(invalid_data(
            "exact directory length disagrees with its plan",
        ));
    }
    let crc = crc_with_zeroed_field(&bytes, 56);
    put_u32(&mut bytes, 56, crc);
    Ok(bytes)
}

fn append_exact_descriptor(bytes: &mut Vec<u8>, descriptor: ExactPageDescriptor) {
    let mut encoded = [0u8; EXACT_PAGE_DESCRIPTOR_LEN];
    put_u32(&mut encoded, 0, descriptor.first_key.0);
    put_u32(&mut encoded, 4, descriptor.first_key.1);
    put_u32(&mut encoded, 8, descriptor.last_key.0);
    put_u32(&mut encoded, 12, descriptor.last_key.1);
    put_u32(&mut encoded, 16, descriptor.record_count);
    put_u32(&mut encoded, 24, descriptor.page_crc32c);
    bytes.extend_from_slice(&encoded);
}

fn write_exact_pages(
    writer: &mut impl Write,
    written: &mut u64,
    indexes: &SegmentIndexes,
    exact_postings: BlobLocator,
    exact_pages: BlobLocator,
    format: AuthenticatedIndexFormat,
) -> io::Result<Vec<ExactPageDescriptor>> {
    if indexes.exact_postings.is_empty() {
        if exact_postings != BlobLocator::default() || exact_pages != BlobLocator::default() {
            return Err(invalid_data(
                "empty exact inventory has non-empty payload or page regions",
            ));
        }
        return Ok(Vec::new());
    }
    if exact_postings == BlobLocator::default() || exact_pages == BlobLocator::default() {
        return Err(invalid_data(
            "non-empty exact inventory has no payload or page region",
        ));
    }
    if *written != exact_pages.offset {
        return Err(invalid_data("exact pages begin at the wrong offset"));
    }

    let mut entries = indexes.exact_postings.entries().peekable();
    let mut page_index = 0u32;
    let mut postings_offset = exact_postings.offset;
    let mut page = vec![0u8; EXACT_PAGE_LEN];
    let mut scratch = vec![0u8; POSTINGS_SCRATCH_LEN];
    let expected_page_count = page_count(
        u64::try_from(indexes.exact_postings.len())
            .map_err(|_| invalid_input("exact entry count exceeds u64"))?,
    )?;
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(
            usize::try_from(expected_page_count)
                .map_err(|_| invalid_input("exact page count exceeds usize"))?,
        )
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    while entries.peek().is_some() {
        page.fill(0);
        put_u32(&mut page, 0, format.exact_page_magic());
        put_u16(&mut page, 4, format.exact_page_version());
        put_u32(&mut page, 8, page_index);
        let mut first_key = None;
        let mut last_key = None;
        let mut record_count = 0u32;
        for record_index in 0..EXACT_RECORDS_PER_PAGE {
            let Some((name, value, refs)) = entries.next() else {
                break;
            };
            let ref_count = u32::try_from(refs.len())
                .map_err(|_| invalid_input("exact ref count exceeds u32"))?;
            let postings_len = exact_payload_len(format, refs)?;
            let payload_crc32c = exact_payload_crc(format, refs, &mut scratch)?;
            let time_range = indexes
                .label_value_time_ranges
                .get(name, value)
                .ok_or_else(|| {
                    invalid_input("exact-postings entry has no matching label-value time range")
                })?;
            let offset = EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN;
            put_u32(&mut page, offset, name);
            put_u32(&mut page, offset + 4, value);
            put_u64(&mut page, offset + 8, postings_offset);
            put_u64(&mut page, offset + 16, postings_len);
            put_u64(&mut page, offset + 24, time_range.min_time_ms);
            put_u64(&mut page, offset + 32, time_range.max_time_ms);
            put_u32(&mut page, offset + 40, ref_count);
            put_u32(&mut page, offset + 44, payload_crc32c);
            postings_offset = postings_offset
                .checked_add(postings_len)
                .ok_or_else(|| invalid_input("exact-postings offset overflows"))?;
            first_key.get_or_insert((name, value));
            last_key = Some((name, value));
            record_count += 1;
        }
        put_u32(&mut page, 12, record_count);
        let descriptor = ExactPageDescriptor {
            first_key: first_key.ok_or_else(|| invalid_data("exact page is empty"))?,
            last_key: last_key.ok_or_else(|| invalid_data("exact page is empty"))?,
            record_count,
            page_crc32c: crc32c(&page),
        };
        write_bytes(writer, written, &page)?;
        descriptors.push(descriptor);
        page_index = page_index
            .checked_add(1)
            .ok_or_else(|| invalid_input("exact page count exceeds u32"))?;
    }
    if page_index != expected_page_count {
        return Err(invalid_data("exact page count disagrees with its plan"));
    }
    if postings_offset != exact_postings.end("exact-postings end overflows")? {
        return Err(invalid_data(
            "exact page locators disagree with the postings region",
        ));
    }
    if *written != exact_pages.end("exact-pages end overflows")? {
        return Err(invalid_data("exact page bytes disagree with the plan"));
    }
    Ok(descriptors)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExactPostingsCodec {
    Raw32,
    DeltaUleb128,
}

impl ExactPostingsCodec {
    const fn id(self) -> u8 {
        match self {
            Self::Raw32 => EXACT_POSTINGS_CODEC_RAW32,
            Self::DeltaUleb128 => EXACT_POSTINGS_CODEC_DELTA_ULEB128,
        }
    }
}

fn exact_raw_payload_len(ref_count: usize) -> io::Result<u64> {
    let count = u32::try_from(ref_count)
        .map_err(|_| invalid_input("exact-postings ref count exceeds u32"))?;
    Ok(4 + u64::from(count) * 4)
}

fn exact_payload_len(format: AuthenticatedIndexFormat, refs: &[u32]) -> io::Result<u64> {
    let raw_len = exact_raw_payload_len(refs.len())?;
    match format {
        AuthenticatedIndexFormat::V8Raw => Ok(raw_len),
        AuthenticatedIndexFormat::V9Adaptive => {
            let (codec, delta_len) = select_v9_codec(refs, raw_len)?;
            Ok(match codec {
                ExactPostingsCodec::Raw32 => raw_len,
                ExactPostingsCodec::DeltaUleb128 => delta_len,
            })
        }
    }
}

fn select_v9_codec(refs: &[u32], raw_len: u64) -> io::Result<(ExactPostingsCodec, u64)> {
    let first = refs
        .first()
        .copied()
        .ok_or_else(|| invalid_input("exact-postings entry has no refs"))?;
    let mut delta_len = EXACT_POSTINGS_V9_HEADER_LEN
        .checked_add(uleb128_u32_len(first) as u64)
        .ok_or_else(|| invalid_input("delta exact-postings length overflows"))?;
    let mut previous = first;
    for &series_ref in &refs[1..] {
        let gap = series_ref
            .checked_sub(previous)
            .filter(|gap| *gap != 0)
            .ok_or_else(|| {
                invalid_input("exact-postings refs are not strictly ordered and unique")
            })?;
        delta_len = delta_len
            .checked_add(uleb128_u32_len(gap) as u64)
            .ok_or_else(|| invalid_input("delta exact-postings length overflows"))?;
        previous = series_ref;
    }
    let codec = if delta_len < raw_len {
        ExactPostingsCodec::DeltaUleb128
    } else {
        ExactPostingsCodec::Raw32
    };
    Ok((codec, delta_len))
}

fn exact_payload_crc(
    format: AuthenticatedIndexFormat,
    refs: &[u32],
    scratch: &mut [u8],
) -> io::Result<u32> {
    let minimum_scratch_len = match format {
        AuthenticatedIndexFormat::V8Raw => 4,
        AuthenticatedIndexFormat::V9Adaptive => 5,
    };
    if scratch.len() < minimum_scratch_len {
        return Err(invalid_data("exact-postings scratch buffer is too small"));
    }
    let raw_len = exact_raw_payload_len(refs.len())?;
    let (header, codec) = match format {
        AuthenticatedIndexFormat::V8Raw => {
            let count = u32::try_from(refs.len())
                .map_err(|_| invalid_input("exact-postings ref count exceeds u32"))?;
            (count.to_le_bytes(), ExactPostingsCodec::Raw32)
        }
        AuthenticatedIndexFormat::V9Adaptive => {
            let (codec, _delta_len) = select_v9_codec(refs, raw_len)?;
            ([codec.id(), 0, 0, 0], codec)
        }
    };
    let mut crc = crc32c(&header);
    if codec == ExactPostingsCodec::DeltaUleb128 {
        return append_delta_crc(crc, refs, scratch);
    }
    let refs_per_chunk = scratch.len() / 4;
    for chunk in refs.chunks(refs_per_chunk) {
        let byte_len = chunk.len() * 4;
        for (slot, series_ref) in scratch[..byte_len].chunks_exact_mut(4).zip(chunk) {
            slot.copy_from_slice(&series_ref.to_le_bytes());
        }
        crc = crc32c_append(crc, &scratch[..byte_len]);
    }
    Ok(crc)
}

fn append_delta_crc(mut crc: u32, refs: &[u32], scratch: &mut [u8]) -> io::Result<u32> {
    let first = refs
        .first()
        .copied()
        .ok_or_else(|| invalid_input("exact-postings entry has no refs"))?;
    let mut used = 0usize;
    let mut previous = None;
    for series_ref in std::iter::once(first).chain(refs[1..].iter().copied()) {
        let value = match previous {
            None => series_ref,
            Some(previous) => series_ref
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .ok_or_else(|| {
                    invalid_input("exact-postings refs are not strictly ordered and unique")
                })?,
        };
        let mut encoded = [0u8; 5];
        let len = encode_uleb128_u32(value, &mut encoded);
        if scratch.len() - used < len {
            crc = crc32c_append(crc, &scratch[..used]);
            used = 0;
        }
        scratch[used..used + len].copy_from_slice(&encoded[..len]);
        used += len;
        previous = Some(series_ref);
    }
    if used != 0 {
        crc = crc32c_append(crc, &scratch[..used]);
    }
    Ok(crc)
}

fn write_exact_payloads(
    writer: &mut impl Write,
    written: &mut u64,
    indexes: &SegmentIndexes,
    locator: BlobLocator,
    format: AuthenticatedIndexFormat,
) -> io::Result<()> {
    if locator == BlobLocator::default() {
        return (!indexes.exact_postings.is_empty())
            .then(|| invalid_data("exact entries have no payload region"))
            .map_or(Ok(()), Err);
    }
    if *written != locator.offset {
        return Err(invalid_data(
            "exact payload region begins at the wrong offset",
        ));
    }
    let mut scratch = vec![0u8; POSTINGS_SCRATCH_LEN];
    for (_name, _value, refs) in indexes.exact_postings.entries() {
        let raw_len = exact_raw_payload_len(refs.len())?;
        let codec = match format {
            AuthenticatedIndexFormat::V8Raw => {
                let count = u32::try_from(refs.len())
                    .map_err(|_| invalid_input("exact-postings ref count exceeds u32"))?;
                write_bytes(writer, written, &count.to_le_bytes())?;
                ExactPostingsCodec::Raw32
            }
            AuthenticatedIndexFormat::V9Adaptive => {
                let (codec, _delta_len) = select_v9_codec(refs, raw_len)?;
                write_bytes(writer, written, &[codec.id(), 0, 0, 0])?;
                codec
            }
        };
        if codec == ExactPostingsCodec::DeltaUleb128 {
            write_delta_refs(writer, written, refs, &mut scratch)?;
            continue;
        }
        let refs_per_chunk = scratch.len() / 4;
        for chunk in refs.chunks(refs_per_chunk) {
            let byte_len = chunk.len() * 4;
            for (slot, series_ref) in scratch[..byte_len].chunks_exact_mut(4).zip(chunk) {
                slot.copy_from_slice(&series_ref.to_le_bytes());
            }
            write_bytes(writer, written, &scratch[..byte_len])?;
        }
    }
    if *written != locator.end("exact payload region end overflows")? {
        return Err(invalid_data("exact payload bytes disagree with the plan"));
    }
    Ok(())
}

fn write_delta_refs(
    writer: &mut impl Write,
    written: &mut u64,
    refs: &[u32],
    scratch: &mut [u8],
) -> io::Result<()> {
    let first = refs
        .first()
        .copied()
        .ok_or_else(|| invalid_input("exact-postings entry has no refs"))?;
    let mut used = 0usize;
    let mut previous = None;
    for series_ref in std::iter::once(first).chain(refs[1..].iter().copied()) {
        let value = match previous {
            None => series_ref,
            Some(previous) => series_ref
                .checked_sub(previous)
                .filter(|gap| *gap != 0)
                .ok_or_else(|| {
                    invalid_input("exact-postings refs are not strictly ordered and unique")
                })?,
        };
        let mut encoded = [0u8; 5];
        let len = encode_uleb128_u32(value, &mut encoded);
        if scratch.len() - used < len {
            write_bytes(writer, written, &scratch[..used])?;
            used = 0;
        }
        scratch[used..used + len].copy_from_slice(&encoded[..len]);
        used += len;
        previous = Some(series_ref);
    }
    if used != 0 {
        write_bytes(writer, written, &scratch[..used])?;
    }
    Ok(())
}

const fn uleb128_u32_len(mut value: u32) -> usize {
    let mut len = 1usize;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn encode_uleb128_u32(mut value: u32, output: &mut [u8; 5]) -> usize {
    let mut len = 0usize;
    while value >= 0x80 {
        output[len] = (value as u8) | 0x80;
        value >>= 7;
        len += 1;
    }
    output[len] = value as u8;
    len + 1
}

fn encode_auxiliary_directory(
    indexes: &SegmentIndexes,
    plan: &AuxiliaryPlan,
    layout: SegmentIndexV8Layout,
) -> io::Result<Vec<u8>> {
    let capacity = u64_to_usize(layout.auxiliary_directory.len, "auxiliary directory length")?;
    let mut bytes = vec![0u8; AUXILIARY_DIRECTORY_HEADER_LEN];
    bytes
        .try_reserve_exact(capacity.saturating_sub(AUXILIARY_DIRECTORY_HEADER_LEN))
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    put_u32(&mut bytes, 0, AUXILIARY_DIRECTORY_MAGIC);
    put_u16(&mut bytes, 4, AUXILIARY_DIRECTORY_VERSION);
    put_u32(&mut bytes, 8, AUXILIARY_DIRECTORY_HEADER_LEN as u32);
    put_u32(&mut bytes, 12, AUXILIARY_RECORD_LEN as u32);
    put_u64(&mut bytes, 16, u64::from(plan.entry_count));
    put_u64(&mut bytes, 24, AUXILIARY_DIRECTORY_HEADER_LEN as u64);
    put_u64(
        &mut bytes,
        32,
        u64::from(plan.entry_count) * AUXILIARY_RECORD_LEN as u64,
    );
    let mut payload_offset = layout.auxiliary_payloads.offset;
    let mut previous_key = None;
    for (label_name_sym, fst_bytes) in &indexes.label_values.fsts {
        let set = Set::new(fst_bytes.as_slice())
            .map_err(|error| invalid_input_owned(format!("invalid label-value FST: {error}")))?;
        let item_count =
            u32::try_from(set.len()).map_err(|_| invalid_input("FST item count exceeds u32"))?;
        let time_range = plan
            .time_range_groups
            .binary_search_by_key(label_name_sym, |group| group.label_name_sym)
            .ok()
            .map(|index| plan.time_range_groups[index].time_range)
            .unwrap_or(UNCONSTRAINED_TIME_RANGE);
        let payload_len = usize_to_u64(fst_bytes.len(), "FST payload length")?;
        append_auxiliary_record(
            &mut bytes,
            &mut previous_key,
            AuxiliaryRecord {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
                label_name_sym: *label_name_sym,
                payload: BlobLocator {
                    offset: payload_offset,
                    len: payload_len,
                },
                time_range,
                item_count,
                payload_crc32c: crc32c(fst_bytes),
            },
        )?;
        payload_offset = payload_offset
            .checked_add(payload_len)
            .ok_or_else(|| invalid_input("auxiliary payload offset overflows"))?;
    }
    for group in &plan.time_range_groups {
        let item_count = u32::try_from(group.entries.len())
            .map_err(|_| invalid_input("time-range item count exceeds u32"))?;
        append_auxiliary_record(
            &mut bytes,
            &mut previous_key,
            AuxiliaryRecord {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                label_name_sym: group.label_name_sym,
                payload: BlobLocator {
                    offset: payload_offset,
                    len: group.payload_len,
                },
                time_range: group.time_range,
                item_count,
                payload_crc32c: group.payload_crc32c,
            },
        )?;
        payload_offset = payload_offset
            .checked_add(group.payload_len)
            .ok_or_else(|| invalid_input("auxiliary payload offset overflows"))?;
    }
    if payload_offset
        != layout
            .auxiliary_payloads
            .end("auxiliary payload end overflows")?
    {
        return Err(invalid_data(
            "auxiliary payload bytes disagree with the plan",
        ));
    }
    if bytes.len() != capacity {
        return Err(invalid_data(
            "auxiliary directory length disagrees with its plan",
        ));
    }
    let crc = crc_with_zeroed_field(&bytes, 40);
    put_u32(&mut bytes, 40, crc);
    Ok(bytes)
}

fn append_auxiliary_record(
    bytes: &mut Vec<u8>,
    previous_key: &mut Option<(u16, u32)>,
    record: AuxiliaryRecord,
) -> io::Result<()> {
    let key = (record.kind, record.label_name_sym);
    if previous_key.is_some_and(|previous| previous >= key) {
        return Err(invalid_input(
            "auxiliary records are not strictly ordered and unique",
        ));
    }
    let mut encoded = [0u8; AUXILIARY_RECORD_LEN];
    put_u16(&mut encoded, 0, record.kind);
    put_u32(&mut encoded, 4, record.label_name_sym);
    put_u64(&mut encoded, 8, record.payload.offset);
    put_u64(&mut encoded, 16, record.payload.len);
    put_u64(&mut encoded, 24, record.time_range.min_time_ms);
    put_u64(&mut encoded, 32, record.time_range.max_time_ms);
    put_u32(&mut encoded, 40, record.item_count);
    put_u32(&mut encoded, 44, record.payload_crc32c);
    bytes.extend_from_slice(&encoded);
    *previous_key = Some(key);
    Ok(())
}

fn write_auxiliary_payloads(
    writer: &mut impl Write,
    written: &mut u64,
    indexes: &SegmentIndexes,
    plan: &AuxiliaryPlan,
    locator: BlobLocator,
) -> io::Result<()> {
    if locator == BlobLocator::default() {
        return (plan.entry_count != 0)
            .then(|| invalid_data("auxiliary entries have no payload region"))
            .map_or(Ok(()), Err);
    }
    if *written != locator.offset {
        return Err(invalid_data(
            "auxiliary payload region begins at the wrong offset",
        ));
    }
    for fst_bytes in indexes.label_values.fsts.values() {
        write_bytes(writer, written, fst_bytes)?;
    }
    for group in &plan.time_range_groups {
        let bytes = encode_time_range_payload(&group.entries)?;
        write_bytes(writer, written, &bytes)?;
    }
    if *written != locator.end("auxiliary payload end overflows")? {
        return Err(invalid_data(
            "auxiliary payload bytes disagree with the plan",
        ));
    }
    Ok(())
}

fn encode_time_range_payload(entries: &[(u32, LabelValueTimeRange)]) -> io::Result<Vec<u8>> {
    let count = u32::try_from(entries.len())
        .map_err(|_| invalid_input("time-range item count exceeds u32"))?;
    if count == 0 {
        return Err(invalid_input("time-range payload has no items"));
    }
    let capacity = 4usize
        .checked_add(
            entries
                .len()
                .checked_mul(20)
                .ok_or_else(|| invalid_input("time-range payload length overflows"))?,
        )
        .ok_or_else(|| invalid_input("time-range payload length overflows"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    bytes.extend_from_slice(&count.to_le_bytes());
    for (value, range) in entries {
        bytes.extend_from_slice(&value.to_le_bytes());
        bytes.extend_from_slice(&range.min_time_ms.to_le_bytes());
        bytes.extend_from_slice(&range.max_time_ms.to_le_bytes());
    }
    Ok(bytes)
}

fn page_count(entry_count: u64) -> io::Result<u32> {
    let count = entry_count
        .checked_add(EXACT_RECORDS_PER_PAGE as u64 - 1)
        .ok_or_else(|| invalid_data("exact page count overflows"))?
        / EXACT_RECORDS_PER_PAGE as u64;
    u32::try_from(count).map_err(|_| invalid_data("exact page count exceeds u32"))
}

fn validate_fst_bytes(bytes: &[u8], expected_item_count: Option<u32>) -> io::Result<()> {
    let set = Set::new(bytes)
        .map_err(|error| invalid_input_owned(format!("invalid label-value FST: {error}")))?;
    if set.is_empty() {
        return Err(invalid_input("label-value FST has no values"));
    }
    if expected_item_count.is_some_and(|expected| expected as usize != set.len()) {
        return Err(invalid_data("FST item count disagrees with its record"));
    }
    let mut stream = set.stream();
    while let Some(value) = stream.next() {
        std::str::from_utf8(value)
            .map_err(|error| invalid_input_owned(format!("invalid UTF-8 in FST: {error}")))?;
    }
    Ok(())
}

fn crc_with_zeroed_field(bytes: &[u8], field_offset: usize) -> u32 {
    let crc = crc32c(&bytes[..field_offset]);
    let crc = crc32c_append(crc, &[0; 4]);
    crc32c_append(crc, &bytes[field_offset + 4..])
}

fn write_bytes(writer: &mut impl Write, written: &mut u64, bytes: &[u8]) -> io::Result<()> {
    for chunk in bytes.chunks(OUTPUT_BUFFER_LEN) {
        writer.write_all(chunk)?;
    }
    *written = written
        .checked_add(usize_to_u64(bytes.len(), "written byte count")?)
        .ok_or_else(|| invalid_input("written byte count overflows"))?;
    Ok(())
}

fn seek_exact(writer: &mut impl Seek, offset: u64, description: &'static str) -> io::Result<()> {
    let actual = writer.seek(SeekFrom::Start(offset))?;
    if actual != offset {
        return Err(invalid_data_owned(format!(
            "{description} seek returned the wrong offset"
        )));
    }
    Ok(())
}

fn put_locator(bytes: &mut [u8], offset: usize, locator: BlobLocator) {
    put_u64(bytes, offset, locator.offset);
    put_u64(bytes, offset + 8, locator.len);
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

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("validated slice"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("validated slice"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated slice"),
    )
}

fn usize_to_u64(value: usize, description: &'static str) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| invalid_input(description))
}

fn u64_to_usize(value: u64, description: &'static str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_data(description))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_input_owned(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_data_owned(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
