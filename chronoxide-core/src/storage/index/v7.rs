use std::io::{self, BufWriter, Write};

use crc32c::crc32c;

use super::*;

mod codec;
mod reader;
pub(super) use reader::SegmentIndexV7Reader;
#[allow(dead_code)] // Wired into the schema-neutral metadata backend after the governed adapter.
pub(super) mod runtime;

const SEGMENT_INDEX_V7_VERSION: u16 = 7;
const SEGMENT_INDEX_V7_HEADER_LEN: usize = 16;
const SEGMENT_INDEX_V7_TRAILER_LEN: usize = 256;
const SEGMENT_INDEX_V7_LOCATOR_LEN: usize = 16;
const SEGMENT_INDEX_V7_TERMINAL_MAGIC: u32 = u32::from_le_bytes(*b"S7ND");

const EXACT_DIRECTORY_MAGIC: u32 = u32::from_le_bytes(*b"EXD7");
const EXACT_DIRECTORY_VERSION: u16 = 1;
const EXACT_DIRECTORY_HEADER_LEN: usize = 64;
const EXACT_PAGE_DESCRIPTOR_LEN: usize = 32;
const EXACT_PAGE_MAGIC: u32 = u32::from_le_bytes(*b"XPG7");
const EXACT_PAGE_VERSION: u16 = 1;
const EXACT_PAGE_LEN: usize = 16_384;
const EXACT_PAGE_HEADER_LEN: usize = 16;
const EXACT_RECORD_LEN: usize = 40;
const EXACT_RECORDS_PER_PAGE: usize = 409;

const AUXILIARY_DIRECTORY_MAGIC: u32 = u32::from_le_bytes(*b"AUX7");
const AUXILIARY_DIRECTORY_VERSION: u16 = 1;
const AUXILIARY_DIRECTORY_HEADER_LEN: usize = 64;
const AUXILIARY_DIRECTORY_RECORD_LEN: usize = 40;
const V7_OUTPUT_BUFFER_LEN: usize = 64 * 1024;
const EXACT_POSTINGS_SCRATCH_LEN: usize = 64 * 1024;
const EXACT_POSTINGS_REFS_PER_SCRATCH: usize = EXACT_POSTINGS_SCRATCH_LEN / 4;

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
const TRAILER_TERMINAL_MAGIC_OFFSET: usize = 252;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BlobLocator {
    offset: u64,
    len: u64,
}

#[derive(Debug, Clone, Copy)]
struct SegmentIndexV7PayloadLengths {
    routing: Option<u64>,
    metric: u64,
    exact_postings: u64,
    auxiliary: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentIndexV7Layout {
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
    file_len: u64,
}

#[derive(Debug, Clone, Copy)]
struct AuxiliaryTimeRangeGroup {
    label_name_sym: u32,
    key_start: usize,
    key_end: usize,
    payload_len: u64,
    time_range: LabelValueTimeRange,
}

#[derive(Debug)]
struct AuxiliaryPlan {
    sorted_time_range_keys: Vec<(u32, u32)>,
    time_range_groups: Vec<AuxiliaryTimeRangeGroup>,
    payload_len: u64,
    entry_count: u64,
}

#[derive(Debug, Clone, Copy)]
struct ExactPageDescriptor {
    first_label_name_sym: u32,
    first_label_value_sym: u32,
    last_label_name_sym: u32,
    last_label_value_sym: u32,
    record_count: u32,
    page_crc32c: u32,
}

pub(super) fn write_segment_indexes_v7(
    writer: impl Write,
    indexes: &SegmentIndexes,
) -> io::Result<()> {
    validate_segment_indexes_v7_for_write(indexes)?;
    let routing_payload = indexes
        .routing_index
        .as_ref()
        .map(SegmentRoutingIndex::encode)
        .transpose()?;
    let metric_payload = write_metric_series_ranges_blob(&indexes.metric_series_ranges)?;
    let auxiliary_plan = plan_auxiliary_payloads(indexes)?;

    let exact_postings_len =
        indexes
            .exact_postings
            .entries()
            .try_fold(0u64, |total, (_name, _value, refs)| {
                checked_add(
                    total,
                    exact_postings_blob_len_v7(refs)?,
                    "exact postings payload region",
                )
            })?;
    let exact_entry_count = usize_to_u64(indexes.exact_postings.len(), "exact entry count")?;
    let layout = plan_segment_indexes_v7_layout(
        SegmentIndexV7PayloadLengths {
            routing: routing_payload
                .as_ref()
                .map(|payload| usize_to_u64(payload.len(), "routing payload length"))
                .transpose()?,
            metric: usize_to_u64(metric_payload.len(), "metric payload length")?,
            exact_postings: exact_postings_len,
            auxiliary: auxiliary_plan.payload_len,
        },
        exact_entry_count,
        auxiliary_plan.entry_count,
    )?;

    let exact_directory = encode_exact_directory(indexes, layout)?;
    let auxiliary_directory = encode_auxiliary_directory(indexes, &auxiliary_plan, layout)?;
    let trailer = encode_segment_indexes_v7_trailer(layout);
    let header = encode_segment_indexes_v7_header();

    let mut writer = BufWriter::with_capacity(V7_OUTPUT_BUFFER_LEN, writer);
    let mut written = 0u64;
    write_v7_bytes(&mut writer, &mut written, &header)?;
    if let Some(payload) = routing_payload.as_deref() {
        write_v7_bytes(&mut writer, &mut written, payload)?;
    }
    write_v7_bytes(&mut writer, &mut written, &metric_payload)?;
    write_exact_postings_payloads(&mut writer, &mut written, indexes, layout.exact_postings)?;
    write_auxiliary_payloads(
        &mut writer,
        &mut written,
        indexes,
        &auxiliary_plan,
        layout.auxiliary_payloads,
    )?;
    write_v7_bytes(&mut writer, &mut written, &exact_directory)?;
    visit_exact_pages(
        indexes,
        layout.exact_postings.offset,
        |_descriptor, page| write_v7_bytes(&mut writer, &mut written, page),
    )?;
    write_v7_bytes(&mut writer, &mut written, &auxiliary_directory)?;
    write_v7_bytes(&mut writer, &mut written, &trailer)?;

    if written != layout.file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment index v7 writer emitted {written} bytes, planned {}",
                layout.file_len
            ),
        ));
    }
    writer.flush()?;
    Ok(())
}

fn plan_auxiliary_payloads(indexes: &SegmentIndexes) -> io::Result<AuxiliaryPlan> {
    let mut sorted_time_range_keys = Vec::new();
    try_reserve_exact_vec(
        &mut sorted_time_range_keys,
        indexes.label_value_time_ranges.ranges.len(),
        "auxiliary time-range key plan",
    )?;
    sorted_time_range_keys.extend(indexes.label_value_time_ranges.ranges.keys().copied());
    sorted_time_range_keys.sort_unstable();
    if !sorted_time_range_keys
        .windows(2)
        .all(|pair| pair[0] < pair[1])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "auxiliary time-range keys are not strictly ordered and unique",
        ));
    }

    let group_count = sorted_time_range_keys
        .first()
        .map(|_| {
            sorted_time_range_keys
                .windows(2)
                .filter(|pair| pair[0].0 != pair[1].0)
                .count()
                .checked_add(1)
                .ok_or_else(|| layout_too_large("auxiliary time-range group count"))
        })
        .transpose()?
        .unwrap_or(0);
    let mut time_range_groups = Vec::new();
    try_reserve_exact_vec(
        &mut time_range_groups,
        group_count,
        "auxiliary time-range group plan",
    )?;

    let mut payload_len = 0u64;
    for fst_bytes in indexes.label_values.fsts.values() {
        if fst_bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment index v7 cannot encode a zero-length auxiliary payload",
            ));
        }
        let fst = Set::new(fst_bytes.as_slice()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("segment index v7 label-value FST is invalid: {error}"),
            )
        })?;
        if fst.len() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment index v7 cannot encode a label-value FST with no values",
            ));
        }
        payload_len = checked_add(
            payload_len,
            usize_to_u64(fst_bytes.len(), "label FST payload length")?,
            "auxiliary payload region",
        )?;
    }

    let mut key_start = 0usize;
    while key_start < sorted_time_range_keys.len() {
        let label_name_sym = sorted_time_range_keys[key_start].0;
        let mut key_end = key_start + 1;
        while key_end < sorted_time_range_keys.len()
            && sorted_time_range_keys[key_end].0 == label_name_sym
        {
            key_end += 1;
        }
        let payload_group_len = label_value_time_range_payload_len(key_end - key_start)?;
        let mut time_range: Option<LabelValueTimeRange> = None;
        for key in &sorted_time_range_keys[key_start..key_end] {
            let range = indexes
                .label_value_time_ranges
                .ranges
                .get(key)
                .copied()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "auxiliary time-range plan references a missing range",
                    )
                })?;
            time_range = Some(match time_range {
                Some(existing) => LabelValueTimeRange {
                    min_time_ms: existing.min_time_ms.min(range.min_time_ms),
                    max_time_ms: existing.max_time_ms.max(range.max_time_ms),
                },
                None => range,
            });
        }
        let time_range = time_range.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "auxiliary time-range group cannot be empty",
            )
        })?;
        time_range_groups.push(AuxiliaryTimeRangeGroup {
            label_name_sym,
            key_start,
            key_end,
            payload_len: payload_group_len,
            time_range,
        });
        payload_len = checked_add(payload_len, payload_group_len, "auxiliary payload region")?;
        key_start = key_end;
    }

    let entry_count = usize_to_u64(indexes.label_values.fsts.len(), "label FST entry count")?
        .checked_add(usize_to_u64(
            time_range_groups.len(),
            "label time-range entry count",
        )?)
        .ok_or_else(|| layout_too_large("auxiliary entry count"))?;
    Ok(AuxiliaryPlan {
        sorted_time_range_keys,
        time_range_groups,
        payload_len,
        entry_count,
    })
}

fn write_exact_postings_payloads(
    writer: &mut impl Write,
    written: &mut u64,
    indexes: &SegmentIndexes,
    locator: BlobLocator,
) -> io::Result<()> {
    if locator.len == 0 {
        if !indexes.exact_postings.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "exact postings entries have no planned payload region",
            ));
        }
        return Ok(());
    }
    if *written != locator.offset {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "exact postings payload offset does not match planned layout",
        ));
    }

    let mut scratch = Vec::new();
    for (_name, _value, refs) in indexes.exact_postings.entries() {
        let entry_start = *written;
        let ref_count = u32::try_from(refs.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "exact postings reference count exceeds u32",
            )
        })?;
        write_v7_bytes(writer, written, &ref_count.to_le_bytes())?;
        for refs_chunk in refs.chunks(EXACT_POSTINGS_REFS_PER_SCRATCH) {
            scratch.clear();
            let chunk_len = refs_chunk
                .len()
                .checked_mul(4)
                .ok_or_else(|| layout_too_large("exact postings scratch buffer"))?;
            try_reserve_exact_vec(&mut scratch, chunk_len, "exact postings scratch buffer")?;
            for series_ref in refs_chunk {
                scratch.extend_from_slice(&series_ref.to_le_bytes());
            }
            write_v7_bytes(writer, written, &scratch)?;
        }
        let emitted = written.checked_sub(entry_start).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "exact postings emitted byte count moved backwards",
            )
        })?;
        if emitted != exact_postings_blob_len_v7(refs)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "exact postings payload length does not match planned length",
            ));
        }
    }
    let expected_end = checked_add(locator.offset, locator.len, "exact postings payload region")?;
    if *written != expected_end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "exact postings payload region does not match planned length",
        ));
    }
    Ok(())
}

fn write_auxiliary_payloads(
    writer: &mut impl Write,
    written: &mut u64,
    indexes: &SegmentIndexes,
    plan: &AuxiliaryPlan,
    locator: BlobLocator,
) -> io::Result<()> {
    if locator.len == 0 {
        if plan.entry_count != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "auxiliary entries have no planned payload region",
            ));
        }
        return Ok(());
    }
    if *written != locator.offset {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "auxiliary payload offset does not match planned layout",
        ));
    }

    for fst_bytes in indexes.label_values.fsts.values() {
        write_v7_bytes(writer, written, fst_bytes)?;
    }
    let mut scratch = Vec::new();
    for group in &plan.time_range_groups {
        encode_label_value_time_range_payload(indexes, plan, *group, &mut scratch)?;
        write_v7_bytes(writer, written, &scratch)?;
    }
    let expected_end = checked_add(locator.offset, locator.len, "auxiliary payload region")?;
    if *written != expected_end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "auxiliary payload region does not match planned length",
        ));
    }
    Ok(())
}

fn encode_label_value_time_range_payload(
    indexes: &SegmentIndexes,
    plan: &AuxiliaryPlan,
    group: AuxiliaryTimeRangeGroup,
    scratch: &mut Vec<u8>,
) -> io::Result<()> {
    let keys = &plan.sorted_time_range_keys[group.key_start..group.key_end];
    let key_count = u32::try_from(keys.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "label value time range count exceeds u32",
        )
    })?;
    let expected_len = usize::try_from(group.payload_len)
        .map_err(|_| layout_too_large("label value time-range payload allocation"))?;
    scratch.clear();
    try_reserve_exact_vec(
        scratch,
        expected_len,
        "label value time-range payload allocation",
    )?;
    scratch.extend_from_slice(&key_count.to_le_bytes());
    for key in keys {
        let range = indexes
            .label_value_time_ranges
            .ranges
            .get(key)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "auxiliary time-range plan references a missing range",
                )
            })?;
        scratch.extend_from_slice(&key.1.to_le_bytes());
        scratch.extend_from_slice(&range.min_time_ms.to_le_bytes());
        scratch.extend_from_slice(&range.max_time_ms.to_le_bytes());
    }
    if scratch.len() != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time-range payload length does not match plan",
        ));
    }
    Ok(())
}

fn label_value_time_range_payload_len(entry_count: usize) -> io::Result<u64> {
    let entry_count = usize_to_u64(entry_count, "label value time range count")?;
    u32::try_from(entry_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "label value time range count exceeds u32",
        )
    })?;
    checked_add(
        4,
        checked_mul(entry_count, 20, "label value time-range payload")?,
        "label value time-range payload",
    )
}

fn validate_segment_indexes_v7_for_write(indexes: &SegmentIndexes) -> io::Result<()> {
    for ((label_name_sym, label_value_sym), time_range) in &indexes.label_value_time_ranges.ranges {
        validate_time_range(*time_range, || {
            format!("label-value time range ({label_name_sym}, {label_value_sym})")
        })?;
    }
    for (_metric_sym, ranges) in indexes.metric_series_ranges.entries() {
        validate_metric_series_range_sequence(ranges, io::ErrorKind::InvalidInput)?;
    }
    Ok(())
}

fn validate_time_range(
    time_range: LabelValueTimeRange,
    description: impl FnOnce() -> String,
) -> io::Result<()> {
    if time_range.min_time_ms > time_range.max_time_ms {
        let description = description();
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "invalid {description}: min_time_ms {} exceeds max_time_ms {}",
                time_range.min_time_ms, time_range.max_time_ms
            ),
        ));
    }
    Ok(())
}

fn plan_segment_indexes_v7_layout(
    lengths: SegmentIndexV7PayloadLengths,
    exact_entry_count: u64,
    auxiliary_entry_count: u64,
) -> io::Result<SegmentIndexV7Layout> {
    if exact_entry_count == 0 && lengths.exact_postings != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "exact postings payload exists without exact entries",
        ));
    }
    if exact_entry_count != 0 && lengths.exact_postings == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "exact entries require an exact postings payload",
        ));
    }
    if auxiliary_entry_count == 0 && lengths.auxiliary != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "auxiliary payload exists without auxiliary entries",
        ));
    }
    if auxiliary_entry_count != 0 && lengths.auxiliary == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "auxiliary entries require an auxiliary payload",
        ));
    }

    let exact_page_count_u64 = exact_entry_count
        .checked_div(EXACT_RECORDS_PER_PAGE as u64)
        .and_then(|pages| {
            pages.checked_add(u64::from(
                exact_entry_count % EXACT_RECORDS_PER_PAGE as u64 != 0,
            ))
        })
        .ok_or_else(|| layout_too_large("exact page count"))?;
    let exact_page_count =
        u32::try_from(exact_page_count_u64).map_err(|_| layout_too_large("exact page count"))?;
    let auxiliary_entry_count = u32::try_from(auxiliary_entry_count)
        .map_err(|_| layout_too_large("auxiliary entry count"))?;

    let exact_directory_len = checked_add(
        EXACT_DIRECTORY_HEADER_LEN as u64,
        checked_mul(
            exact_page_count_u64,
            EXACT_PAGE_DESCRIPTOR_LEN as u64,
            "exact directory",
        )?,
        "exact directory",
    )?;
    let exact_pages_len = checked_mul(exact_page_count_u64, EXACT_PAGE_LEN as u64, "exact pages")?;
    let auxiliary_directory_len = checked_add(
        AUXILIARY_DIRECTORY_HEADER_LEN as u64,
        checked_mul(
            u64::from(auxiliary_entry_count),
            AUXILIARY_DIRECTORY_RECORD_LEN as u64,
            "auxiliary directory",
        )?,
        "auxiliary directory",
    )?;

    let mut cursor = SEGMENT_INDEX_V7_HEADER_LEN as u64;
    let routing = match lengths.routing {
        Some(len) => required_region(&mut cursor, len, "routing payload")?,
        None => BlobLocator::default(),
    };
    let metric = required_region(&mut cursor, lengths.metric, "metric payload")?;
    let exact_postings = optional_region(
        &mut cursor,
        lengths.exact_postings,
        exact_entry_count != 0,
        "exact postings payload",
    )?;
    let auxiliary_payloads = optional_region(
        &mut cursor,
        lengths.auxiliary,
        auxiliary_entry_count != 0,
        "auxiliary payload",
    )?;
    let exact_directory = required_region(&mut cursor, exact_directory_len, "exact directory")?;
    let exact_pages = optional_region(
        &mut cursor,
        exact_pages_len,
        exact_page_count != 0,
        "exact pages",
    )?;
    let auxiliary_directory =
        required_region(&mut cursor, auxiliary_directory_len, "auxiliary directory")?;
    let file_len = checked_add(
        cursor,
        SEGMENT_INDEX_V7_TRAILER_LEN as u64,
        "segment index v7",
    )?;

    Ok(SegmentIndexV7Layout {
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
        file_len,
    })
}

fn encode_segment_indexes_v7_header() -> [u8; SEGMENT_INDEX_V7_HEADER_LEN] {
    let mut header = [0u8; SEGMENT_INDEX_V7_HEADER_LEN];
    set_u32(&mut header, 0, SEGMENT_INDEXES_MAGIC);
    set_u16(&mut header, 4, SEGMENT_INDEX_V7_VERSION);
    set_u16(&mut header, 6, 0);
    set_u32(&mut header, 8, SEGMENT_INDEX_V7_HEADER_LEN as u32);
    set_u32(&mut header, 12, 0);
    header
}

fn validate_segment_indexes_v7_header(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index v7 header truncated",
        ));
    }
    if read_u32_at(bytes, 0) != SEGMENT_INDEXES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index v7 magic mismatch",
        ));
    }
    let version = read_u16_at(bytes, 4);
    if version != SEGMENT_INDEX_V7_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected segment index version 7, found version {version}"),
        ));
    }
    if bytes.len() < SEGMENT_INDEX_V7_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index v7 header truncated",
        ));
    }
    if read_u16_at(bytes, 6) != 0
        || read_u32_at(bytes, 8) != SEGMENT_INDEX_V7_HEADER_LEN as u32
        || read_u32_at(bytes, 12) != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid segment index v7 header fields",
        ));
    }
    Ok(())
}

fn decode_segment_indexes_v7_root(
    actual_file_len: u64,
    header: &[u8; SEGMENT_INDEX_V7_HEADER_LEN],
    trailer: &[u8; SEGMENT_INDEX_V7_TRAILER_LEN],
) -> io::Result<SegmentIndexV7Layout> {
    validate_segment_indexes_v7_header(header)?;
    if actual_file_len < (SEGMENT_INDEX_V7_HEADER_LEN + SEGMENT_INDEX_V7_TRAILER_LEN) as u64 {
        return Err(invalid_v7_root("segment index v7 file is too short"));
    }
    if read_u32_at(trailer, 0) != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(invalid_v7_root("segment index v7 trailer magic mismatch"));
    }
    if read_u16_at(trailer, 4) != SEGMENT_INDEX_V7_VERSION {
        return Err(invalid_v7_root("segment index v7 trailer version mismatch"));
    }
    if read_u16_at(trailer, 6) != 0 {
        return Err(invalid_v7_root(
            "segment index v7 trailer flags are non-zero",
        ));
    }
    if read_u32_at(trailer, 8) != SEGMENT_INDEX_V7_TRAILER_LEN as u32 {
        return Err(invalid_v7_root(
            "segment index v7 trailer length is invalid",
        ));
    }
    if read_u32_at(trailer, 12) != 0 {
        return Err(invalid_v7_root(
            "segment index v7 trailer reserved0 is non-zero",
        ));
    }
    if trailer[164..TRAILER_TERMINAL_MAGIC_OFFSET]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(invalid_v7_root(
            "segment index v7 trailer reserved1 is non-zero",
        ));
    }
    if read_u32_at(trailer, TRAILER_TERMINAL_MAGIC_OFFSET) != SEGMENT_INDEX_V7_TERMINAL_MAGIC {
        return Err(invalid_v7_root(
            "segment index v7 trailer terminal magic mismatch",
        ));
    }

    let stored_crc = read_u32_at(trailer, TRAILER_CRC_OFFSET);
    let mut crc_bytes = *trailer;
    set_u32(&mut crc_bytes, TRAILER_CRC_OFFSET, 0);
    if crc32c(&crc_bytes) != stored_crc {
        return Err(invalid_v7_root("segment index v7 trailer CRC mismatch"));
    }

    let recorded_file_len = read_u64_at(trailer, TRAILER_FILE_LEN_OFFSET);
    if recorded_file_len != actual_file_len {
        return Err(invalid_v7_root(
            "segment index v7 recorded file length does not match the actual file length",
        ));
    }
    if read_u32_at(trailer, TRAILER_EXACT_RECORD_LEN_OFFSET) != EXACT_RECORD_LEN as u32 {
        return Err(invalid_v7_root(
            "segment index v7 exact record length is invalid",
        ));
    }
    if read_u32_at(trailer, TRAILER_EXACT_PAGE_LEN_OFFSET) != EXACT_PAGE_LEN as u32 {
        return Err(invalid_v7_root(
            "segment index v7 exact page length is invalid",
        ));
    }

    let routing = decode_v7_root_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET);
    let metric = decode_v7_root_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET);
    let exact_directory = decode_v7_root_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
    let exact_pages = decode_v7_root_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
    let exact_postings = decode_v7_root_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET);
    let auxiliary_directory = decode_v7_root_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    let auxiliary_payloads = decode_v7_root_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET);
    validate_v7_optional_locator(routing, "routing")?;
    validate_v7_required_locator(metric, "metric ranges")?;
    validate_v7_required_locator(exact_directory, "exact directory")?;
    validate_v7_optional_locator(exact_pages, "exact pages")?;
    validate_v7_optional_locator(exact_postings, "exact postings")?;
    validate_v7_required_locator(auxiliary_directory, "auxiliary directory")?;
    validate_v7_optional_locator(auxiliary_payloads, "auxiliary payloads")?;

    let exact_entry_count = read_u64_at(trailer, TRAILER_EXACT_ENTRY_COUNT_OFFSET);
    let exact_page_count = read_u32_at(trailer, TRAILER_EXACT_PAGE_COUNT_OFFSET);
    let auxiliary_entry_count = read_u32_at(trailer, TRAILER_AUX_ENTRY_COUNT_OFFSET);
    let expected_exact_page_count = v7_exact_page_count(exact_entry_count)?;
    if exact_page_count != expected_exact_page_count {
        return Err(invalid_v7_root(
            "segment index v7 exact page count does not match the entry count",
        ));
    }

    if exact_entry_count == 0 {
        require_v7_absent(exact_pages, "exact pages for an empty exact index")?;
        require_v7_absent(exact_postings, "exact postings for an empty exact index")?;
    } else {
        require_v7_present(exact_pages, "exact pages for a non-empty exact index")?;
        require_v7_present(exact_postings, "exact postings for a non-empty exact index")?;
    }
    if auxiliary_entry_count == 0 {
        require_v7_absent(
            auxiliary_payloads,
            "auxiliary payloads for an empty auxiliary index",
        )?;
    } else {
        require_v7_present(
            auxiliary_payloads,
            "auxiliary payloads for a non-empty auxiliary index",
        )?;
    }

    let expected_exact_directory_len = root_checked_add(
        EXACT_DIRECTORY_HEADER_LEN as u64,
        root_checked_mul(
            u64::from(exact_page_count),
            EXACT_PAGE_DESCRIPTOR_LEN as u64,
            "exact directory length",
        )?,
        "exact directory length",
    )?;
    if exact_directory.len != expected_exact_directory_len {
        return Err(invalid_v7_root(
            "segment index v7 exact directory length is inconsistent",
        ));
    }
    let expected_exact_pages_len = root_checked_mul(
        u64::from(exact_page_count),
        EXACT_PAGE_LEN as u64,
        "exact pages length",
    )?;
    if exact_pages.len != expected_exact_pages_len {
        return Err(invalid_v7_root(
            "segment index v7 exact pages length is inconsistent",
        ));
    }
    let expected_auxiliary_directory_len = root_checked_add(
        AUXILIARY_DIRECTORY_HEADER_LEN as u64,
        root_checked_mul(
            u64::from(auxiliary_entry_count),
            AUXILIARY_DIRECTORY_RECORD_LEN as u64,
            "auxiliary directory length",
        )?,
        "auxiliary directory length",
    )?;
    if auxiliary_directory.len != expected_auxiliary_directory_len {
        return Err(invalid_v7_root(
            "segment index v7 auxiliary directory length is inconsistent",
        ));
    }

    let trailer_offset = actual_file_len - SEGMENT_INDEX_V7_TRAILER_LEN as u64;
    let ordered_regions = [
        ("routing", routing),
        ("metric ranges", metric),
        ("exact postings", exact_postings),
        ("auxiliary payloads", auxiliary_payloads),
        ("exact directory", exact_directory),
        ("exact pages", exact_pages),
        ("auxiliary directory", auxiliary_directory),
    ];
    let mut previous_end = SEGMENT_INDEX_V7_HEADER_LEN as u64;
    for (name, locator) in ordered_regions {
        if locator == BlobLocator::default() {
            continue;
        }
        let end = validate_v7_root_region(locator, trailer_offset, name)?;
        if locator.offset < previous_end {
            return Err(invalid_v7_root(
                "segment index v7 top-level regions overlap or are out of physical order",
            ));
        }
        previous_end = end;
    }

    Ok(SegmentIndexV7Layout {
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
        file_len: actual_file_len,
    })
}

fn decode_v7_root_locator(
    trailer: &[u8; SEGMENT_INDEX_V7_TRAILER_LEN],
    offset: usize,
) -> BlobLocator {
    BlobLocator {
        offset: read_u64_at(trailer, offset),
        len: read_u64_at(trailer, offset + 8),
    }
}

fn validate_v7_optional_locator(locator: BlobLocator, name: &'static str) -> io::Result<()> {
    if (locator.offset == 0) != (locator.len == 0) {
        return Err(invalid_v7_root(match name {
            "routing" => "segment index v7 routing locator is half-empty",
            "exact pages" => "segment index v7 exact pages locator is half-empty",
            "exact postings" => "segment index v7 exact postings locator is half-empty",
            "auxiliary payloads" => "segment index v7 auxiliary payloads locator is half-empty",
            _ => "segment index v7 optional locator is half-empty",
        }));
    }
    Ok(())
}

fn validate_v7_required_locator(locator: BlobLocator, name: &'static str) -> io::Result<()> {
    if locator.offset == 0 || locator.len == 0 {
        return Err(invalid_v7_root(match name {
            "metric ranges" => "segment index v7 metric ranges locator is missing or half-empty",
            "exact directory" => {
                "segment index v7 exact directory locator is missing or half-empty"
            }
            "auxiliary directory" => {
                "segment index v7 auxiliary directory locator is missing or half-empty"
            }
            _ => "segment index v7 required locator is missing or half-empty",
        }));
    }
    Ok(())
}

fn require_v7_absent(locator: BlobLocator, description: &'static str) -> io::Result<()> {
    if locator != BlobLocator::default() {
        return Err(invalid_v7_root(description));
    }
    Ok(())
}

fn require_v7_present(locator: BlobLocator, description: &'static str) -> io::Result<()> {
    if locator == BlobLocator::default() {
        return Err(invalid_v7_root(description));
    }
    Ok(())
}

fn v7_exact_page_count(exact_entry_count: u64) -> io::Result<u32> {
    let full_pages = exact_entry_count / EXACT_RECORDS_PER_PAGE as u64;
    let page_count = full_pages
        .checked_add(u64::from(
            exact_entry_count % EXACT_RECORDS_PER_PAGE as u64 != 0,
        ))
        .ok_or_else(|| invalid_v7_root("segment index v7 exact page count overflows"))?;
    u32::try_from(page_count)
        .map_err(|_| invalid_v7_root("segment index v7 exact page count exceeds u32"))
}

fn validate_v7_root_region(
    locator: BlobLocator,
    trailer_offset: u64,
    _name: &'static str,
) -> io::Result<u64> {
    if locator.offset < SEGMENT_INDEX_V7_HEADER_LEN as u64 {
        return Err(invalid_v7_root(
            "segment index v7 locator starts inside the fixed header",
        ));
    }
    let end = root_checked_add(locator.offset, locator.len, "top-level locator end")?;
    if end > trailer_offset {
        return Err(invalid_v7_root(
            "segment index v7 locator extends into or beyond the fixed trailer",
        ));
    }
    Ok(end)
}

fn root_checked_add(left: u64, right: u64, description: &'static str) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid_v7_root(description))
}

fn root_checked_mul(left: u64, right: u64, description: &'static str) -> io::Result<u64> {
    left.checked_mul(right)
        .ok_or_else(|| invalid_v7_root(description))
}

fn invalid_v7_root(description: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, description)
}

fn encode_exact_directory(
    indexes: &SegmentIndexes,
    layout: SegmentIndexV7Layout,
) -> io::Result<Vec<u8>> {
    let capacity = usize::try_from(layout.exact_directory.len)
        .map_err(|_| layout_too_large("exact directory allocation"))?;
    let mut directory = vec![0u8; EXACT_DIRECTORY_HEADER_LEN];
    try_reserve_exact_vec(
        &mut directory,
        capacity.saturating_sub(EXACT_DIRECTORY_HEADER_LEN),
        "exact directory allocation",
    )?;
    set_u32(&mut directory, 0, EXACT_DIRECTORY_MAGIC);
    set_u16(&mut directory, 4, EXACT_DIRECTORY_VERSION);
    set_u16(&mut directory, 6, 0);
    set_u32(&mut directory, 8, EXACT_DIRECTORY_HEADER_LEN as u32);
    set_u32(&mut directory, 12, EXACT_PAGE_DESCRIPTOR_LEN as u32);
    set_u32(&mut directory, 16, EXACT_PAGE_LEN as u32);
    set_u32(&mut directory, 20, EXACT_RECORD_LEN as u32);
    set_u64(&mut directory, 24, layout.exact_entry_count);
    set_u32(&mut directory, 32, layout.exact_page_count);
    set_u32(&mut directory, 36, EXACT_RECORDS_PER_PAGE as u32);
    set_u64(&mut directory, 40, EXACT_DIRECTORY_HEADER_LEN as u64);
    set_u64(
        &mut directory,
        48,
        checked_mul(
            u64::from(layout.exact_page_count),
            EXACT_PAGE_DESCRIPTOR_LEN as u64,
            "exact directory descriptors",
        )?,
    );
    visit_exact_pages(
        indexes,
        layout.exact_postings.offset,
        |descriptor, _page| {
            directory.extend_from_slice(&encode_exact_page_descriptor(descriptor));
            Ok(())
        },
    )?;
    if directory.len() != capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "exact directory length does not match planned layout",
        ));
    }
    let crc = crc32c(&directory);
    set_u32(&mut directory, 56, crc);
    Ok(directory)
}

fn visit_exact_pages(
    indexes: &SegmentIndexes,
    exact_postings_offset: u64,
    mut visit: impl FnMut(ExactPageDescriptor, &[u8]) -> io::Result<()>,
) -> io::Result<()> {
    let mut entries = indexes.exact_postings.entries().peekable();
    let mut page_index = 0u32;
    let mut postings_offset = exact_postings_offset;
    let mut page = vec![0u8; EXACT_PAGE_LEN];
    while entries.peek().is_some() {
        page.fill(0);
        set_u32(&mut page, 0, EXACT_PAGE_MAGIC);
        set_u16(&mut page, 4, EXACT_PAGE_VERSION);
        set_u16(&mut page, 6, 0);
        set_u32(&mut page, 8, page_index);

        let mut first_key = None;
        let mut last_key = None;
        let mut record_count = 0u32;
        for record_index in 0..EXACT_RECORDS_PER_PAGE {
            let Some((label_name_sym, label_value_sym, refs)) = entries.next() else {
                break;
            };
            let postings_len = exact_postings_blob_len_v7(refs)?;
            let time_range = indexes
                .label_value_time_ranges
                .get(label_name_sym, label_value_sym)
                .unwrap_or(default_time_range());
            let record_offset = EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN;
            set_u32(&mut page, record_offset, label_name_sym);
            set_u32(&mut page, record_offset + 4, label_value_sym);
            set_u64(&mut page, record_offset + 8, postings_offset);
            set_u64(&mut page, record_offset + 16, postings_len);
            set_u64(&mut page, record_offset + 24, time_range.min_time_ms);
            set_u64(&mut page, record_offset + 32, time_range.max_time_ms);
            postings_offset = checked_add(
                postings_offset,
                postings_len,
                "exact postings payload offset",
            )?;
            first_key.get_or_insert((label_name_sym, label_value_sym));
            last_key = Some((label_name_sym, label_value_sym));
            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| layout_too_large("exact page record count"))?;
        }
        set_u32(&mut page, 12, record_count);
        let (first_label_name_sym, first_label_value_sym) = first_key.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "exact page cannot be empty")
        })?;
        let (last_label_name_sym, last_label_value_sym) = last_key.unwrap();
        let descriptor = ExactPageDescriptor {
            first_label_name_sym,
            first_label_value_sym,
            last_label_name_sym,
            last_label_value_sym,
            record_count,
            page_crc32c: crc32c(&page),
        };
        visit(descriptor, &page)?;
        page_index = page_index
            .checked_add(1)
            .ok_or_else(|| layout_too_large("exact page count"))?;
    }
    Ok(())
}

fn encode_exact_page_descriptor(
    descriptor: ExactPageDescriptor,
) -> [u8; EXACT_PAGE_DESCRIPTOR_LEN] {
    let mut bytes = [0u8; EXACT_PAGE_DESCRIPTOR_LEN];
    set_u32(&mut bytes, 0, descriptor.first_label_name_sym);
    set_u32(&mut bytes, 4, descriptor.first_label_value_sym);
    set_u32(&mut bytes, 8, descriptor.last_label_name_sym);
    set_u32(&mut bytes, 12, descriptor.last_label_value_sym);
    set_u32(&mut bytes, 16, descriptor.record_count);
    set_u32(&mut bytes, 20, 0);
    set_u32(&mut bytes, 24, descriptor.page_crc32c);
    set_u32(&mut bytes, 28, 0);
    bytes
}

fn encode_auxiliary_directory(
    indexes: &SegmentIndexes,
    plan: &AuxiliaryPlan,
    layout: SegmentIndexV7Layout,
) -> io::Result<Vec<u8>> {
    let capacity = usize::try_from(layout.auxiliary_directory.len)
        .map_err(|_| layout_too_large("auxiliary directory allocation"))?;
    let mut directory = vec![0u8; AUXILIARY_DIRECTORY_HEADER_LEN];
    try_reserve_exact_vec(
        &mut directory,
        capacity.saturating_sub(AUXILIARY_DIRECTORY_HEADER_LEN),
        "auxiliary directory allocation",
    )?;
    set_u32(&mut directory, 0, AUXILIARY_DIRECTORY_MAGIC);
    set_u16(&mut directory, 4, AUXILIARY_DIRECTORY_VERSION);
    set_u16(&mut directory, 6, 0);
    set_u32(&mut directory, 8, AUXILIARY_DIRECTORY_HEADER_LEN as u32);
    set_u32(&mut directory, 12, AUXILIARY_DIRECTORY_RECORD_LEN as u32);
    set_u64(&mut directory, 16, u64::from(layout.auxiliary_entry_count));
    set_u64(&mut directory, 24, AUXILIARY_DIRECTORY_HEADER_LEN as u64);
    set_u64(
        &mut directory,
        32,
        checked_mul(
            u64::from(layout.auxiliary_entry_count),
            AUXILIARY_DIRECTORY_RECORD_LEN as u64,
            "auxiliary directory records",
        )?,
    );

    let mut payload_offset = layout.auxiliary_payloads.offset;
    let mut previous_key = None;
    for (label_name_sym, fst_bytes) in &indexes.label_values.fsts {
        let payload_len = usize_to_u64(fst_bytes.len(), "label FST payload length")?;
        let time_range = auxiliary_group_for_label(plan, *label_name_sym)
            .map(|group| group.time_range)
            .unwrap_or(default_time_range());
        append_auxiliary_record(
            &mut directory,
            &mut previous_key,
            SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            *label_name_sym,
            payload_offset,
            payload_len,
            time_range,
        )?;
        payload_offset = checked_add(payload_offset, payload_len, "auxiliary payload offset")?;
    }
    for group in &plan.time_range_groups {
        append_auxiliary_record(
            &mut directory,
            &mut previous_key,
            SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
            group.label_name_sym,
            payload_offset,
            group.payload_len,
            group.time_range,
        )?;
        payload_offset = checked_add(
            payload_offset,
            group.payload_len,
            "auxiliary payload offset",
        )?;
    }
    let expected_payload_end = checked_add(
        layout.auxiliary_payloads.offset,
        layout.auxiliary_payloads.len,
        "auxiliary payload region",
    )?;
    if payload_offset != expected_payload_end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "auxiliary directory payload ranges do not match planned region",
        ));
    }
    if directory.len() != capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "auxiliary directory length does not match planned layout",
        ));
    }
    let crc = crc32c(&directory);
    set_u32(&mut directory, 40, crc);
    Ok(directory)
}

#[allow(clippy::too_many_arguments)]
fn append_auxiliary_record(
    directory: &mut Vec<u8>,
    previous_key: &mut Option<(u16, u32)>,
    kind: u16,
    label_name_sym: u32,
    payload_offset: u64,
    payload_len: u64,
    time_range: LabelValueTimeRange,
) -> io::Result<()> {
    let key = (kind, label_name_sym);
    if previous_key.is_some_and(|previous| previous >= key) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "auxiliary directory records are not strictly ordered and unique",
        ));
    }
    if payload_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment index v7 cannot encode a zero-length auxiliary payload",
        ));
    }
    let mut record = [0u8; AUXILIARY_DIRECTORY_RECORD_LEN];
    set_u16(&mut record, 0, kind);
    set_u16(&mut record, 2, 0);
    set_u32(&mut record, 4, label_name_sym);
    set_u64(&mut record, 8, payload_offset);
    set_u64(&mut record, 16, payload_len);
    set_u64(&mut record, 24, time_range.min_time_ms);
    set_u64(&mut record, 32, time_range.max_time_ms);
    directory.extend_from_slice(&record);
    *previous_key = Some(key);
    Ok(())
}

fn auxiliary_group_for_label(
    plan: &AuxiliaryPlan,
    label_name_sym: u32,
) -> Option<&AuxiliaryTimeRangeGroup> {
    plan.time_range_groups
        .binary_search_by_key(&label_name_sym, |group| group.label_name_sym)
        .ok()
        .map(|index| &plan.time_range_groups[index])
}

fn encode_segment_indexes_v7_trailer(
    layout: SegmentIndexV7Layout,
) -> [u8; SEGMENT_INDEX_V7_TRAILER_LEN] {
    let mut trailer = [0u8; SEGMENT_INDEX_V7_TRAILER_LEN];
    set_u32(&mut trailer, 0, SEGMENT_INDEX_TRAILER_MAGIC);
    set_u16(&mut trailer, 4, SEGMENT_INDEX_V7_VERSION);
    set_u16(&mut trailer, 6, 0);
    set_u32(&mut trailer, 8, SEGMENT_INDEX_V7_TRAILER_LEN as u32);
    set_u32(&mut trailer, 12, 0);
    set_u64(&mut trailer, TRAILER_FILE_LEN_OFFSET, layout.file_len);
    set_locator(&mut trailer, TRAILER_ROUTING_LOCATOR_OFFSET, layout.routing);
    set_locator(&mut trailer, TRAILER_METRIC_LOCATOR_OFFSET, layout.metric);
    set_locator(
        &mut trailer,
        TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET,
        layout.exact_directory,
    );
    set_locator(
        &mut trailer,
        TRAILER_EXACT_PAGES_LOCATOR_OFFSET,
        layout.exact_pages,
    );
    set_locator(
        &mut trailer,
        TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET,
        layout.exact_postings,
    );
    set_locator(
        &mut trailer,
        TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        layout.auxiliary_directory,
    );
    set_locator(
        &mut trailer,
        TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET,
        layout.auxiliary_payloads,
    );
    set_u64(
        &mut trailer,
        TRAILER_EXACT_ENTRY_COUNT_OFFSET,
        layout.exact_entry_count,
    );
    set_u32(
        &mut trailer,
        TRAILER_EXACT_PAGE_COUNT_OFFSET,
        layout.exact_page_count,
    );
    set_u32(
        &mut trailer,
        TRAILER_EXACT_RECORD_LEN_OFFSET,
        EXACT_RECORD_LEN as u32,
    );
    set_u32(
        &mut trailer,
        TRAILER_EXACT_PAGE_LEN_OFFSET,
        EXACT_PAGE_LEN as u32,
    );
    set_u32(
        &mut trailer,
        TRAILER_AUX_ENTRY_COUNT_OFFSET,
        layout.auxiliary_entry_count,
    );
    set_u32(
        &mut trailer,
        TRAILER_TERMINAL_MAGIC_OFFSET,
        SEGMENT_INDEX_V7_TERMINAL_MAGIC,
    );
    let crc = crc32c(&trailer);
    set_u32(&mut trailer, TRAILER_CRC_OFFSET, crc);
    trailer
}

#[cfg(test)]
fn plan_segment_indexes_v7_layout_for_test(routing_len: u64) -> io::Result<()> {
    plan_segment_indexes_v7_layout(
        SegmentIndexV7PayloadLengths {
            routing: Some(routing_len),
            metric: 0,
            exact_postings: 0,
            auxiliary: 0,
        },
        0,
        0,
    )
    .map(|_| ())
}

fn exact_postings_blob_len_from_count_v7(ref_count: u64) -> io::Result<u64> {
    u32::try_from(ref_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "exact postings reference count exceeds u32",
        )
    })?;
    checked_add(
        checked_mul(ref_count, 4, "exact postings reference count")?,
        4,
        "exact postings payload",
    )
}

fn exact_postings_blob_len_v7(refs: &[u32]) -> io::Result<u64> {
    exact_postings_blob_len_from_count_v7(usize_to_u64(
        refs.len(),
        "exact postings reference count",
    )?)
}

fn required_region(
    cursor: &mut u64,
    len: u64,
    description: &'static str,
) -> io::Result<BlobLocator> {
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("zero-length {description} is not allowed"),
        ));
    }
    let offset = *cursor;
    *cursor = checked_add(*cursor, len, description)?;
    Ok(BlobLocator { offset, len })
}

fn optional_region(
    cursor: &mut u64,
    len: u64,
    present: bool,
    description: &'static str,
) -> io::Result<BlobLocator> {
    if present {
        required_region(cursor, len, description)
    } else if len == 0 {
        Ok(BlobLocator::default())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("absent {description} has non-zero length"),
        ))
    }
}

fn checked_add(left: u64, right: u64, description: &'static str) -> io::Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| layout_too_large(description))
}

fn checked_mul(left: u64, right: u64, description: &'static str) -> io::Result<u64> {
    left.checked_mul(right)
        .ok_or_else(|| layout_too_large(description))
}

fn usize_to_u64(value: usize, description: &'static str) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| layout_too_large(description))
}

fn try_reserve_exact_vec<T>(
    values: &mut Vec<T>,
    additional: usize,
    description: &'static str,
) -> io::Result<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| layout_too_large(description))
}

fn layout_too_large(description: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{description} is too large"),
    )
}

fn default_time_range() -> LabelValueTimeRange {
    LabelValueTimeRange {
        min_time_ms: 0,
        max_time_ms: u64::MAX,
    }
}

fn write_v7_bytes(writer: &mut impl Write, written: &mut u64, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)?;
    *written = checked_add(
        *written,
        usize_to_u64(bytes.len(), "written byte count")?,
        "written byte count",
    )?;
    Ok(())
}

fn set_locator(bytes: &mut [u8], offset: usize, locator: BlobLocator) {
    debug_assert_eq!(SEGMENT_INDEX_V7_LOCATOR_LEN, 16);
    set_u64(bytes, offset, locator.offset);
    set_u64(bytes, offset + 8, locator.len);
}

fn set_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn set_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn set_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

#[cfg(test)]
#[path = "v7/tests/mod.rs"]
mod tests;
