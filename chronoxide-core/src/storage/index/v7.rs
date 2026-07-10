use std::io::{self, BufWriter, Write};

use crc32c::crc32c;

use super::*;

mod reader;
#[allow(unused_imports)]
pub(super) use reader::SegmentIndexV7Reader;

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

#[derive(Debug, Clone, Copy)]
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
mod tests {
    use crc32c::crc32c;

    use super::*;
    use crate::storage::series::SERIES_KIND_FLOAT;

    const TRAILER_FILE_LEN_OFFSET: usize = 16;
    const TRAILER_ROUTING_LOCATOR_OFFSET: usize = 24;
    const TRAILER_METRIC_LOCATOR_OFFSET: usize = 40;
    const TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET: usize = 56;
    const TRAILER_EXACT_PAGES_LOCATOR_OFFSET: usize = 72;
    const TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET: usize = 88;
    const TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET: usize = 104;
    const TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET: usize = 120;
    const TRAILER_CRC_OFFSET: usize = 160;
    const TRAILER_RESERVED_OFFSET: usize = 164;
    const TRAILER_TERMINAL_MAGIC_OFFSET: usize = 252;

    #[derive(Default)]
    struct CountingSink {
        bytes: Vec<u8>,
        write_calls: usize,
        flush_calls: usize,
    }

    impl Write for CountingSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.write_calls += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flush_calls += 1;
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RootFixture {
        actual_file_len: u64,
        header: [u8; SEGMENT_INDEX_V7_HEADER_LEN],
        trailer: [u8; SEGMENT_INDEX_V7_TRAILER_LEN],
    }

    fn root_fixture(indexes: &SegmentIndexes) -> RootFixture {
        let bytes = encode_v7(indexes);
        let mut header = [0u8; SEGMENT_INDEX_V7_HEADER_LEN];
        header.copy_from_slice(&bytes[..SEGMENT_INDEX_V7_HEADER_LEN]);
        let mut trailer = [0u8; SEGMENT_INDEX_V7_TRAILER_LEN];
        trailer.copy_from_slice(&bytes[bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN..]);
        RootFixture {
            actual_file_len: bytes.len() as u64,
            header,
            trailer,
        }
    }

    fn recompute_root_trailer_crc(trailer: &mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]) {
        put_u32(trailer, TRAILER_CRC_OFFSET, 0);
        let crc = crc32c(trailer);
        put_u32(trailer, TRAILER_CRC_OFFSET, crc);
    }

    fn assert_invalid_root(fixture: &RootFixture, case: &str) {
        let error = decode_segment_indexes_v7_root(
            fixture.actual_file_len,
            &fixture.header,
            &fixture.trailer,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
    }

    fn mutate_root_trailer(
        fixture: &RootFixture,
        mutate: impl FnOnce(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]),
    ) -> RootFixture {
        let mut fixture = fixture.clone();
        mutate(&mut fixture.trailer);
        recompute_root_trailer_crc(&mut fixture.trailer);
        fixture
    }

    #[derive(Default)]
    struct WriteFailSink;

    impl Write for WriteFailSink {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("injected index write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct FlushFailSink {
        bytes: Vec<u8>,
    }

    impl Write for FlushFailSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("injected index flush failure"))
        }
    }

    fn encode_v7(indexes: &SegmentIndexes) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_segment_indexes_v7(&mut bytes, indexes).unwrap();
        bytes
    }

    fn minimal_indexes() -> SegmentIndexes {
        let mut exact_postings = ExactPostingsIndex::default();
        exact_postings.insert(1, 2, 7);

        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(1, 2, 1_000, 2_000);

        SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        }
    }

    fn deterministic_indexes(reverse: bool) -> SegmentIndexes {
        let entries = if reverse {
            [(3, 30, 300), (1, 10, 100), (2, 20, 200)]
        } else {
            [(2, 20, 200), (1, 10, 100), (3, 30, 300)]
        };
        let mut exact_postings = ExactPostingsIndex::default();
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        for (name, value, series_ref) in entries {
            exact_postings.insert(name, value, series_ref);
            label_value_time_ranges.insert(
                name,
                value,
                u64::from(series_ref),
                u64::from(series_ref) + 10,
            );
        }
        SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        }
    }

    fn routing_indexes() -> SegmentIndexes {
        let mut symbols = SegmentSymbols::default();
        let name = symbols.intern(METRIC_NAME_LABEL);
        let value = symbols.intern("request_duration_seconds");
        let mut exact_postings = ExactPostingsIndex::default();
        exact_postings.insert(name, value, 0);
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(name, value, 1_000, 2_000);
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            value,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            },
        );
        let routing_index =
            SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &label_value_time_ranges)
                .unwrap();
        SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges,
            metric_series_ranges,
            routing_index: Some(routing_index),
        }
    }

    fn exact_boundary_indexes(entry_count: u32, reverse: bool) -> SegmentIndexes {
        let mut exact_postings = ExactPostingsIndex::default();
        if reverse {
            for value_sym in (0..entry_count).rev() {
                exact_postings.insert(7, value_sym, value_sym + 10_000);
            }
        } else {
            for value_sym in 0..entry_count {
                exact_postings.insert(7, value_sym, value_sym + 10_000);
            }
        }
        SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        }
    }

    fn expected_zero_entry_v7_bytes() -> Vec<u8> {
        let metric_payload = b"MSRG\x01\x00\x00\x00\x00\x00\x00\x00";
        let metric_offset = 16u64;
        let exact_directory_offset = metric_offset + metric_payload.len() as u64;
        let auxiliary_directory_offset = exact_directory_offset + 64;
        let trailer_offset = auxiliary_directory_offset + 64;
        let file_len = trailer_offset + 256;

        let mut exact_directory = vec![0u8; 64];
        put_u32(&mut exact_directory, 0, u32::from_le_bytes(*b"EXD7"));
        put_u16(&mut exact_directory, 4, 1);
        put_u32(&mut exact_directory, 8, 64);
        put_u32(&mut exact_directory, 12, 32);
        put_u32(&mut exact_directory, 16, 16_384);
        put_u32(&mut exact_directory, 20, 40);
        put_u32(&mut exact_directory, 36, 409);
        put_u64(&mut exact_directory, 40, 64);
        let exact_crc = crc32c(&exact_directory);
        put_u32(&mut exact_directory, 56, exact_crc);

        let mut auxiliary_directory = vec![0u8; 64];
        put_u32(&mut auxiliary_directory, 0, u32::from_le_bytes(*b"AUX7"));
        put_u16(&mut auxiliary_directory, 4, 1);
        put_u32(&mut auxiliary_directory, 8, 64);
        put_u32(&mut auxiliary_directory, 12, 40);
        put_u64(&mut auxiliary_directory, 24, 64);
        let auxiliary_crc = crc32c(&auxiliary_directory);
        put_u32(&mut auxiliary_directory, 40, auxiliary_crc);

        let mut trailer = vec![0u8; 256];
        put_u32(&mut trailer, 0, u32::from_le_bytes(*b"SIDT"));
        put_u16(&mut trailer, 4, 7);
        put_u32(&mut trailer, 8, 256);
        put_u64(&mut trailer, 16, file_len);
        put_locator(&mut trailer, 24, 0, 0);
        put_locator(&mut trailer, 40, metric_offset, metric_payload.len() as u64);
        put_locator(&mut trailer, 56, exact_directory_offset, 64);
        put_locator(&mut trailer, 72, 0, 0);
        put_locator(&mut trailer, 88, 0, 0);
        put_locator(&mut trailer, 104, auxiliary_directory_offset, 64);
        put_locator(&mut trailer, 120, 0, 0);
        put_u32(&mut trailer, 148, 40);
        put_u32(&mut trailer, 152, 16_384);
        put_u32(&mut trailer, 252, u32::from_le_bytes(*b"S7ND"));
        let trailer_crc = crc32c(&trailer);
        put_u32(&mut trailer, 160, trailer_crc);

        let mut expected = Vec::new();
        expected.extend_from_slice(b"SIDX\x07\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00");
        expected.extend_from_slice(metric_payload);
        expected.extend_from_slice(&exact_directory);
        expected.extend_from_slice(&auxiliary_directory);
        expected.extend_from_slice(&trailer);
        assert_eq!(expected.len(), 412);
        expected
    }

    fn expected_minimal_v7_bytes() -> Vec<u8> {
        let metric_offset = SEGMENT_INDEX_V7_HEADER_LEN as u64;
        let metric_payload = [
            METRIC_SERIES_RANGES_MAGIC.to_le_bytes().as_slice(),
            METRIC_SERIES_RANGES_VERSION.to_le_bytes().as_slice(),
            0u16.to_le_bytes().as_slice(),
            0u32.to_le_bytes().as_slice(),
        ]
        .concat();
        let exact_postings_offset = metric_offset + metric_payload.len() as u64;
        let exact_postings = [1u32.to_le_bytes(), 7u32.to_le_bytes()].concat();
        let auxiliary_payloads_offset = exact_postings_offset + exact_postings.len() as u64;
        let auxiliary_payload = [
            1u32.to_le_bytes().as_slice(),
            2u32.to_le_bytes().as_slice(),
            1_000u64.to_le_bytes().as_slice(),
            2_000u64.to_le_bytes().as_slice(),
        ]
        .concat();
        let exact_directory_offset = auxiliary_payloads_offset + auxiliary_payload.len() as u64;
        let exact_directory_len = (EXACT_DIRECTORY_HEADER_LEN + EXACT_PAGE_DESCRIPTOR_LEN) as u64;
        let exact_pages_offset = exact_directory_offset + exact_directory_len;
        let auxiliary_directory_offset = exact_pages_offset + EXACT_PAGE_LEN as u64;
        let auxiliary_directory_len =
            (AUXILIARY_DIRECTORY_HEADER_LEN + AUXILIARY_DIRECTORY_RECORD_LEN) as u64;
        let trailer_offset = auxiliary_directory_offset + auxiliary_directory_len;
        let file_len = trailer_offset + SEGMENT_INDEX_V7_TRAILER_LEN as u64;

        let mut exact_page = vec![0u8; EXACT_PAGE_LEN];
        put_u32(&mut exact_page, 0, EXACT_PAGE_MAGIC);
        put_u16(&mut exact_page, 4, EXACT_PAGE_VERSION);
        put_u16(&mut exact_page, 6, 0);
        put_u32(&mut exact_page, 8, 0);
        put_u32(&mut exact_page, 12, 1);
        put_u32(&mut exact_page, EXACT_PAGE_HEADER_LEN, 1);
        put_u32(&mut exact_page, EXACT_PAGE_HEADER_LEN + 4, 2);
        put_u64(
            &mut exact_page,
            EXACT_PAGE_HEADER_LEN + 8,
            exact_postings_offset,
        );
        put_u64(
            &mut exact_page,
            EXACT_PAGE_HEADER_LEN + 16,
            exact_postings.len() as u64,
        );
        put_u64(&mut exact_page, EXACT_PAGE_HEADER_LEN + 24, 1_000);
        put_u64(&mut exact_page, EXACT_PAGE_HEADER_LEN + 32, 2_000);
        let exact_page_crc = crc32c(&exact_page);

        let mut descriptor = vec![0u8; EXACT_PAGE_DESCRIPTOR_LEN];
        put_u32(&mut descriptor, 0, 1);
        put_u32(&mut descriptor, 4, 2);
        put_u32(&mut descriptor, 8, 1);
        put_u32(&mut descriptor, 12, 2);
        put_u32(&mut descriptor, 16, 1);
        put_u32(&mut descriptor, 24, exact_page_crc);

        let mut exact_directory = vec![0u8; EXACT_DIRECTORY_HEADER_LEN];
        put_u32(&mut exact_directory, 0, EXACT_DIRECTORY_MAGIC);
        put_u16(&mut exact_directory, 4, EXACT_DIRECTORY_VERSION);
        put_u16(&mut exact_directory, 6, 0);
        put_u32(&mut exact_directory, 8, EXACT_DIRECTORY_HEADER_LEN as u32);
        put_u32(&mut exact_directory, 12, EXACT_PAGE_DESCRIPTOR_LEN as u32);
        put_u32(&mut exact_directory, 16, EXACT_PAGE_LEN as u32);
        put_u32(&mut exact_directory, 20, EXACT_RECORD_LEN as u32);
        put_u64(&mut exact_directory, 24, 1);
        put_u32(&mut exact_directory, 32, 1);
        put_u32(&mut exact_directory, 36, EXACT_RECORDS_PER_PAGE as u32);
        put_u64(&mut exact_directory, 40, EXACT_DIRECTORY_HEADER_LEN as u64);
        put_u64(&mut exact_directory, 48, EXACT_PAGE_DESCRIPTOR_LEN as u64);
        exact_directory.extend_from_slice(&descriptor);
        let exact_directory_crc = crc32c(&exact_directory);
        put_u32(&mut exact_directory, 56, exact_directory_crc);

        let mut auxiliary_record = vec![0u8; AUXILIARY_DIRECTORY_RECORD_LEN];
        put_u16(
            &mut auxiliary_record,
            0,
            SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
        );
        put_u16(&mut auxiliary_record, 2, 0);
        put_u32(&mut auxiliary_record, 4, 1);
        put_u64(&mut auxiliary_record, 8, auxiliary_payloads_offset);
        put_u64(&mut auxiliary_record, 16, auxiliary_payload.len() as u64);
        put_u64(&mut auxiliary_record, 24, 1_000);
        put_u64(&mut auxiliary_record, 32, 2_000);

        let mut auxiliary_directory = vec![0u8; AUXILIARY_DIRECTORY_HEADER_LEN];
        put_u32(&mut auxiliary_directory, 0, AUXILIARY_DIRECTORY_MAGIC);
        put_u16(&mut auxiliary_directory, 4, AUXILIARY_DIRECTORY_VERSION);
        put_u16(&mut auxiliary_directory, 6, 0);
        put_u32(
            &mut auxiliary_directory,
            8,
            AUXILIARY_DIRECTORY_HEADER_LEN as u32,
        );
        put_u32(
            &mut auxiliary_directory,
            12,
            AUXILIARY_DIRECTORY_RECORD_LEN as u32,
        );
        put_u64(&mut auxiliary_directory, 16, 1);
        put_u64(
            &mut auxiliary_directory,
            24,
            AUXILIARY_DIRECTORY_HEADER_LEN as u64,
        );
        put_u64(
            &mut auxiliary_directory,
            32,
            AUXILIARY_DIRECTORY_RECORD_LEN as u64,
        );
        auxiliary_directory.extend_from_slice(&auxiliary_record);
        let auxiliary_directory_crc = crc32c(&auxiliary_directory);
        put_u32(&mut auxiliary_directory, 40, auxiliary_directory_crc);

        let mut trailer = vec![0u8; SEGMENT_INDEX_V7_TRAILER_LEN];
        put_u32(&mut trailer, 0, SEGMENT_INDEX_TRAILER_MAGIC);
        put_u16(&mut trailer, 4, SEGMENT_INDEX_V7_VERSION);
        put_u16(&mut trailer, 6, 0);
        put_u32(&mut trailer, 8, SEGMENT_INDEX_V7_TRAILER_LEN as u32);
        put_u32(&mut trailer, 12, 0);
        put_u64(&mut trailer, TRAILER_FILE_LEN_OFFSET, file_len);
        put_locator(&mut trailer, TRAILER_ROUTING_LOCATOR_OFFSET, 0, 0);
        put_locator(
            &mut trailer,
            TRAILER_METRIC_LOCATOR_OFFSET,
            metric_offset,
            metric_payload.len() as u64,
        );
        put_locator(
            &mut trailer,
            TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET,
            exact_directory_offset,
            exact_directory_len,
        );
        put_locator(
            &mut trailer,
            TRAILER_EXACT_PAGES_LOCATOR_OFFSET,
            exact_pages_offset,
            EXACT_PAGE_LEN as u64,
        );
        put_locator(
            &mut trailer,
            TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET,
            exact_postings_offset,
            exact_postings.len() as u64,
        );
        put_locator(
            &mut trailer,
            TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
            auxiliary_directory_offset,
            auxiliary_directory_len,
        );
        put_locator(
            &mut trailer,
            TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET,
            auxiliary_payloads_offset,
            auxiliary_payload.len() as u64,
        );
        put_u64(&mut trailer, 136, 1);
        put_u32(&mut trailer, 144, 1);
        put_u32(&mut trailer, 148, EXACT_RECORD_LEN as u32);
        put_u32(&mut trailer, 152, EXACT_PAGE_LEN as u32);
        put_u32(&mut trailer, 156, 1);
        put_u32(
            &mut trailer,
            TRAILER_TERMINAL_MAGIC_OFFSET,
            SEGMENT_INDEX_V7_TERMINAL_MAGIC,
        );
        let trailer_crc = crc32c(&trailer);
        put_u32(&mut trailer, TRAILER_CRC_OFFSET, trailer_crc);

        let mut expected = Vec::with_capacity(file_len as usize);
        expected.extend_from_slice(&SEGMENT_INDEXES_MAGIC.to_le_bytes());
        expected.extend_from_slice(&SEGMENT_INDEX_V7_VERSION.to_le_bytes());
        expected.extend_from_slice(&0u16.to_le_bytes());
        expected.extend_from_slice(&(SEGMENT_INDEX_V7_HEADER_LEN as u32).to_le_bytes());
        expected.extend_from_slice(&0u32.to_le_bytes());
        expected.extend_from_slice(&metric_payload);
        expected.extend_from_slice(&exact_postings);
        expected.extend_from_slice(&auxiliary_payload);
        expected.extend_from_slice(&exact_directory);
        expected.extend_from_slice(&exact_page);
        expected.extend_from_slice(&auxiliary_directory);
        expected.extend_from_slice(&trailer);
        assert_eq!(expected.len() as u64, file_len);
        expected
    }

    fn assert_exact_page_boundary(entry_count: u32, reverse: bool) {
        let bytes = encode_v7(&exact_boundary_indexes(entry_count, reverse));
        let trailer = &bytes[bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN..];
        let expected_page_count = if entry_count == 409 { 1 } else { 2 };
        let (directory_offset, directory_len) =
            read_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
        let (pages_offset, pages_len) = read_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
        let (postings_offset, postings_len) =
            read_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET);

        assert_eq!(read_u64_at(trailer, 136), u64::from(entry_count));
        assert_eq!(read_u32_at(trailer, 144), expected_page_count);
        assert_eq!(directory_len, 64 + u64::from(expected_page_count) * 32);
        assert_eq!(pages_len, u64::from(expected_page_count) * 16_384);
        assert_eq!(postings_len, u64::from(entry_count) * 8);

        let directory_start = directory_offset as usize;
        let directory_end = directory_start + directory_len as usize;
        let directory = &bytes[directory_start..directory_end];
        let expected_directory_crc = read_u32_at(directory, 56);
        let mut crc_bytes = directory.to_vec();
        put_u32(&mut crc_bytes, 56, 0);
        assert_eq!(crc32c(&crc_bytes), expected_directory_crc);

        let first_descriptor = &directory[64..96];
        assert_eq!(read_u32_at(first_descriptor, 0), 7);
        assert_eq!(read_u32_at(first_descriptor, 4), 0);
        assert_eq!(read_u32_at(first_descriptor, 8), 7);
        assert_eq!(read_u32_at(first_descriptor, 12), 408);
        assert_eq!(read_u32_at(first_descriptor, 16), 409);

        let first_page_start = pages_offset as usize;
        let first_page = &bytes[first_page_start..first_page_start + EXACT_PAGE_LEN];
        assert_eq!(read_u32_at(first_page, 8), 0);
        assert_eq!(read_u32_at(first_page, 12), 409);
        assert_eq!(crc32c(first_page), read_u32_at(first_descriptor, 24));
        assert_eq!(&first_page[16_376..], &[0u8; 8]);
        let last_first_page_record = EXACT_PAGE_HEADER_LEN + 408 * EXACT_RECORD_LEN;
        assert_eq!(
            read_u64_at(first_page, last_first_page_record + 8),
            postings_offset + 408 * 8
        );

        if entry_count == 410 {
            let second_descriptor = &directory[96..128];
            assert_eq!(read_u32_at(second_descriptor, 0), 7);
            assert_eq!(read_u32_at(second_descriptor, 4), 409);
            assert_eq!(read_u32_at(second_descriptor, 8), 7);
            assert_eq!(read_u32_at(second_descriptor, 12), 409);
            assert_eq!(read_u32_at(second_descriptor, 16), 1);

            let second_page_start = first_page_start + EXACT_PAGE_LEN;
            let second_page = &bytes[second_page_start..second_page_start + EXACT_PAGE_LEN];
            assert_eq!(read_u32_at(second_page, 8), 1);
            assert_eq!(read_u32_at(second_page, 12), 1);
            assert_eq!(read_u32_at(second_page, 16), 7);
            assert_eq!(read_u32_at(second_page, 20), 409);
            assert_eq!(crc32c(second_page), read_u32_at(second_descriptor, 24));
            assert_eq!(&second_page[56..], vec![0u8; EXACT_PAGE_LEN - 56]);
            assert_eq!(read_u64_at(second_page, 24), postings_offset + 409 * 8);
            assert_eq!(
                read_u64_at(first_page, last_first_page_record + 8)
                    + read_u64_at(first_page, last_first_page_record + 16),
                read_u64_at(second_page, 24)
            );
        }
    }

    #[test]
    fn segment_index_v7_minimal_golden_bytes() {
        let actual = encode_v7(&minimal_indexes());
        let expected = expected_minimal_v7_bytes();

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 16_900);
        assert_eq!(
            &actual[..SEGMENT_INDEX_V7_HEADER_LEN],
            b"SIDX\x07\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00"
        );
        let trailer_start = actual.len() - SEGMENT_INDEX_V7_TRAILER_LEN;
        let trailer = &actual[trailer_start..];
        assert_eq!(
            &trailer[..16],
            b"SIDT\x07\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(
            read_u64_at(trailer, TRAILER_FILE_LEN_OFFSET),
            actual.len() as u64
        );
        assert_eq!(read_u32_at(trailer, 148), 40);
        assert_eq!(read_u32_at(trailer, 152), 16_384);
        assert_eq!(
            &trailer[TRAILER_RESERVED_OFFSET..TRAILER_TERMINAL_MAGIC_OFFSET],
            &[0u8; 88]
        );
        assert_eq!(&trailer[252..], b"S7ND");
        let expected_crc = read_u32_at(trailer, TRAILER_CRC_OFFSET);
        let mut crc_bytes = trailer.to_vec();
        put_u32(&mut crc_bytes, TRAILER_CRC_OFFSET, 0);
        assert_eq!(crc32c(&crc_bytes), expected_crc);
    }

    #[test]
    fn segment_index_v7_zero_entry_golden_bytes() {
        let actual = encode_v7(&SegmentIndexes::default());
        let expected = expected_zero_entry_v7_bytes();

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 412);
        let trailer = &actual[actual.len() - SEGMENT_INDEX_V7_TRAILER_LEN..];
        assert_eq!(
            read_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET),
            (0, 0)
        );
        assert_eq!(
            read_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET),
            (0, 0)
        );
        assert_eq!(
            read_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET),
            (0, 0)
        );
        assert_eq!(
            read_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET),
            (0, 0)
        );
        assert_eq!(
            read_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET),
            (28, 64)
        );
        assert_eq!(
            read_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET),
            (92, 64)
        );
        assert_eq!(read_u64_at(trailer, 136), 0);
        assert_eq!(read_u32_at(trailer, 144), 0);
        assert_eq!(read_u32_at(trailer, 156), 0);
    }

    #[test]
    fn segment_index_v7_exact_page_boundary_409() {
        assert_exact_page_boundary(409, false);
    }

    #[test]
    fn segment_index_v7_exact_page_boundary_410_is_canonical() {
        assert_exact_page_boundary(410, true);
    }

    #[test]
    fn segment_index_v7_auxiliary_directory_orders_fsts_before_time_ranges() {
        let mut fst_2_builder = fst::SetBuilder::memory();
        fst_2_builder.insert("alpha").unwrap();
        let fst_2 = fst_2_builder.into_inner().unwrap();
        let mut fst_9_builder = fst::SetBuilder::memory();
        fst_9_builder.insert("beta").unwrap();
        fst_9_builder.insert("gamma").unwrap();
        let fst_9 = fst_9_builder.into_inner().unwrap();
        let mut label_values = LabelValueFstIndex::default();
        label_values.insert_fst(9, fst_9.clone());
        label_values.insert_fst(2, fst_2.clone());
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(1, 30, 300, 399);
        label_value_time_ranges.insert(1, 10, 100, 199);
        label_value_time_ranges.insert(1, 20, 200, 299);
        label_value_time_ranges.insert(3, 5, 500, 599);
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values,
            label_value_time_ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };

        let bytes = encode_v7(&indexes);
        let trailer = &bytes[bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN..];
        let (directory_offset, directory_len) =
            read_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
        let (payloads_offset, payloads_len) =
            read_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET);
        assert_eq!(directory_len, 64 + 4 * 40);
        assert_eq!(payloads_len, (fst_2.len() + fst_9.len() + 64 + 24) as u64);
        let directory = &bytes[directory_offset as usize..][..directory_len as usize];
        let records = &directory[64..];
        let expected = [(2, 2), (2, 9), (3, 1), (3, 3)];
        for (record_index, (expected_kind, expected_name)) in expected.into_iter().enumerate() {
            let record = &records[record_index * 40..][..40];
            assert_eq!(read_u16_at(record, 0), expected_kind);
            assert_eq!(read_u32_at(record, 4), expected_name);
        }
        let first_time_record = &records[2 * 40..][..40];
        let second_time_record = &records[3 * 40..][..40];
        let fst_payloads_len = (fst_2.len() + fst_9.len()) as u64;
        assert_eq!(
            read_u64_at(first_time_record, 8),
            payloads_offset + fst_payloads_len
        );
        assert_eq!(read_u64_at(first_time_record, 16), 64);
        assert_eq!(
            read_u64_at(second_time_record, 8),
            payloads_offset + fst_payloads_len + 64
        );
        assert_eq!(read_u64_at(second_time_record, 16), 24);
        assert_eq!(
            read_u32_at(&bytes, read_u64_at(first_time_record, 8) as usize),
            3
        );
        assert_eq!(
            read_u32_at(&bytes, read_u64_at(second_time_record, 8) as usize),
            1
        );
        let fst_payload_start = payloads_offset as usize;
        assert_eq!(
            &bytes[fst_payload_start..fst_payload_start + fst_2.len()],
            fst_2.as_slice()
        );
        assert_eq!(
            &bytes[fst_payload_start + fst_2.len()..fst_payload_start + fst_2.len() + fst_9.len()],
            fst_9.as_slice()
        );
        let first_time_payload = &bytes[read_u64_at(first_time_record, 8) as usize..][..64];
        assert_eq!(read_u32_at(first_time_payload, 0), 3);
        for (record_index, (value_sym, min_time_ms, max_time_ms)) in
            [(10, 100, 199), (20, 200, 299), (30, 300, 399)]
                .into_iter()
                .enumerate()
        {
            let record_offset = 4 + record_index * 20;
            assert_eq!(read_u32_at(first_time_payload, record_offset), value_sym);
            assert_eq!(
                read_u64_at(first_time_payload, record_offset + 4),
                min_time_ms
            );
            assert_eq!(
                read_u64_at(first_time_payload, record_offset + 12),
                max_time_ms
            );
        }
        let second_time_payload = &bytes[read_u64_at(second_time_record, 8) as usize..][..24];
        assert_eq!(read_u32_at(second_time_payload, 0), 1);
        assert_eq!(read_u32_at(second_time_payload, 4), 5);
        assert_eq!(read_u64_at(second_time_payload, 8), 500);
        assert_eq!(read_u64_at(second_time_payload, 16), 599);
    }

    #[test]
    fn segment_index_v7_buffers_underlying_writes_below_exact_entry_count() {
        let indexes = exact_boundary_indexes(2_000, true);
        let expected = encode_v7(&indexes);
        let mut sink = CountingSink::default();

        write_segment_indexes_v7(&mut sink, &indexes).unwrap();

        assert_eq!(sink.bytes, expected);
        assert!(
            sink.write_calls <= 64,
            "expected buffered output, observed {} writes for {} exact entries",
            sink.write_calls,
            indexes.exact_postings.len()
        );
        assert_eq!(sink.flush_calls, 1);
    }

    #[test]
    fn segment_index_v7_propagates_buffered_write_and_flush_errors() {
        let indexes = minimal_indexes();

        let write_error = write_segment_indexes_v7(WriteFailSink, &indexes).unwrap_err();
        assert_eq!(write_error.kind(), io::ErrorKind::Other);
        assert!(
            write_error
                .to_string()
                .contains("injected index write failure")
        );

        let mut flush_sink = FlushFailSink::default();
        let flush_error = write_segment_indexes_v7(&mut flush_sink, &indexes).unwrap_err();
        assert_eq!(flush_error.kind(), io::ErrorKind::Other);
        assert!(
            flush_error
                .to_string()
                .contains("injected index flush failure")
        );
        assert!(!flush_sink.bytes.is_empty());
    }

    #[test]
    fn segment_index_v7_preserves_multichunk_exact_postings_payload_bytes() {
        let mut exact_postings = ExactPostingsIndex::default();
        for series_ref in 0..=EXACT_POSTINGS_REFS_PER_SCRATCH as u32 {
            exact_postings.insert_monotonic(2, 20, series_ref);
        }
        let indexes = SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };

        let bytes = encode_v7(&indexes);
        let trailer = &bytes[bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN..];
        let (postings_offset, postings_len) =
            read_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET);
        let refs = indexes.exact_postings.get(2, 20).unwrap();
        let expected = super::super::write_exact_postings_blob(refs).unwrap();

        assert!(expected.len() > EXACT_POSTINGS_SCRATCH_LEN);
        assert_eq!(postings_len, expected.len() as u64);
        assert_eq!(
            &bytes[postings_offset as usize..][..postings_len as usize],
            expected
        );
    }

    #[test]
    fn segment_index_v7_preserves_v6_exact_postings_payload_bytes() {
        let mut exact_postings = ExactPostingsIndex::default();
        for series_ref in [9, 1, 5] {
            exact_postings.insert(2, 20, series_ref);
        }
        exact_postings.insert(3, 30, 42);
        let indexes = SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };

        let bytes = encode_v7(&indexes);
        let trailer = &bytes[bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN..];
        let (postings_offset, postings_len) =
            read_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET);
        let mut expected = Vec::new();
        for (_name, _value, refs) in indexes.exact_postings.entries() {
            expected.extend_from_slice(&super::super::write_exact_postings_blob(refs).unwrap());
        }

        assert_eq!(
            &bytes[postings_offset as usize..][..postings_len as usize],
            expected
        );
    }

    #[test]
    fn segment_index_v7_root_decodes_valid_zero_minimal_and_410_layouts() {
        let cases = [
            ("zero", root_fixture(&SegmentIndexes::default()), 0, 0, 0),
            ("minimal", root_fixture(&minimal_indexes()), 1, 1, 1),
            (
                "410",
                root_fixture(&exact_boundary_indexes(410, true)),
                410,
                2,
                0,
            ),
        ];

        for (case, fixture, exact_entries, exact_pages, auxiliary_entries) in cases {
            let layout = decode_segment_indexes_v7_root(
                fixture.actual_file_len,
                &fixture.header,
                &fixture.trailer,
            )
            .unwrap_or_else(|error| panic!("{case}: {error}"));

            assert_eq!(layout.file_len, fixture.actual_file_len, "{case}");
            assert_eq!(layout.exact_entry_count, exact_entries, "{case}");
            assert_eq!(layout.exact_page_count, exact_pages, "{case}");
            assert_eq!(layout.auxiliary_entry_count, auxiliary_entries, "{case}");
            assert_eq!(
                layout.routing,
                locator_at(&fixture.trailer, TRAILER_ROUTING_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.metric,
                locator_at(&fixture.trailer, TRAILER_METRIC_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.exact_directory,
                locator_at(&fixture.trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.exact_pages,
                locator_at(&fixture.trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.exact_postings,
                locator_at(&fixture.trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.auxiliary_directory,
                locator_at(&fixture.trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.auxiliary_payloads,
                locator_at(&fixture.trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET),
                "{case}"
            );
        }
    }

    #[test]
    fn segment_index_v7_root_allows_gaps_between_ordered_regions() {
        let mut fixture = root_fixture(&SegmentIndexes::default());
        fixture.actual_file_len = 1_024;
        put_u64(&mut fixture.trailer, TRAILER_FILE_LEN_OFFSET, 1_024);
        put_locator(&mut fixture.trailer, TRAILER_METRIC_LOCATOR_OFFSET, 16, 12);
        put_locator(
            &mut fixture.trailer,
            TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET,
            128,
            64,
        );
        put_locator(
            &mut fixture.trailer,
            TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
            512,
            64,
        );
        recompute_root_trailer_crc(&mut fixture.trailer);

        let layout = decode_segment_indexes_v7_root(
            fixture.actual_file_len,
            &fixture.header,
            &fixture.trailer,
        )
        .unwrap();

        assert_eq!(
            layout.metric,
            BlobLocator {
                offset: 16,
                len: 12
            }
        );
        assert_eq!(layout.exact_directory.offset, 128);
        assert_eq!(layout.auxiliary_directory.offset, 512);
    }

    #[test]
    fn segment_index_v7_root_rejects_header_field_mutations() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_HEADER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("magic", |header| {
                put_u32(header, 0, u32::from_le_bytes(*b"BAD!"))
            }),
            ("v6", |header| put_u16(header, 4, 6)),
            ("flags", |header| put_u16(header, 6, 1)),
            ("header length", |header| put_u32(header, 8, 15)),
            ("reserved", |header| put_u32(header, 12, 1)),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let mut fixture = valid.clone();
            mutate(&mut fixture.header);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_trailer_identity_and_reserved_mutations() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("magic", |trailer| {
                put_u32(trailer, 0, u32::from_le_bytes(*b"BAD!"))
            }),
            ("version", |trailer| put_u16(trailer, 4, 6)),
            ("flags", |trailer| put_u16(trailer, 6, 1)),
            ("trailer length", |trailer| put_u32(trailer, 8, 255)),
            ("reserved0", |trailer| put_u32(trailer, 12, 1)),
            ("reserved1", |trailer| trailer[164] = 1),
            ("terminal magic", |trailer| put_u32(trailer, 252, 0)),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let fixture = mutate_root_trailer(&valid, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_crc_and_file_length_mismatches() {
        let valid = root_fixture(&SegmentIndexes::default());

        let mut bad_crc = valid.clone();
        bad_crc.trailer[TRAILER_CRC_OFFSET] ^= 0x80;
        assert_invalid_root(&bad_crc, "trailer CRC");

        let mut wrong_actual_length = valid.clone();
        wrong_actual_length.actual_file_len += 1;
        assert_invalid_root(&wrong_actual_length, "actual file length");

        let wrong_recorded_length = mutate_root_trailer(&valid, |trailer| {
            put_u64(trailer, TRAILER_FILE_LEN_OFFSET, valid.actual_file_len + 1)
        });
        assert_invalid_root(&wrong_recorded_length, "recorded file length");

        let mut too_short = valid.clone();
        too_short.actual_file_len = 200;
        put_u64(&mut too_short.trailer, TRAILER_FILE_LEN_OFFSET, 200);
        recompute_root_trailer_crc(&mut too_short.trailer);
        assert_invalid_root(&too_short, "file shorter than fixed roots");
    }

    #[test]
    fn segment_index_v7_root_rejects_noncanonical_and_out_of_bounds_locators() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("routing offset only", |trailer| {
                put_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET, 16, 0)
            }),
            ("routing length only", |trailer| {
                put_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET, 0, 4)
            }),
            ("exact pages offset only", |trailer| {
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 28, 0)
            }),
            ("exact postings length only", |trailer| {
                put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 0, 8)
            }),
            ("auxiliary payload offset only", |trailer| {
                put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 28, 0)
            }),
            ("missing metric", |trailer| {
                put_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET, 0, 0)
            }),
            ("half metric", |trailer| {
                put_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET, 16, 0)
            }),
            ("missing exact directory", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 0, 0)
            }),
            ("half exact directory", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 0, 64)
            }),
            ("missing auxiliary directory", |trailer| {
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 0, 0)
            }),
            ("half auxiliary directory", |trailer| {
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 92, 0)
            }),
            ("before header", |trailer| {
                put_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET, 8, 12)
            }),
            ("past trailer", |trailer| {
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 150, 64)
            }),
            ("offset overflow", |trailer| {
                put_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET, u64::MAX - 3, 8)
            }),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let fixture = mutate_root_trailer(&valid, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_overlapping_and_out_of_order_regions() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("overlap", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 20, 64)
            }),
            ("directory order", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 92, 64);
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 28, 64);
            }),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let fixture = mutate_root_trailer(&valid, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_count_locator_mismatches() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let zero_cases: &[(&str, Mutation)] = &[
            ("zero exact count with pages", |trailer| {
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 28, 16_384)
            }),
            ("zero exact count with postings", |trailer| {
                put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 28, 8)
            }),
            ("zero auxiliary count with payload", |trailer| {
                put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 28, 4)
            }),
        ];
        let zero = root_fixture(&SegmentIndexes::default());
        for (case, mutate) in zero_cases {
            let fixture = mutate_root_trailer(&zero, mutate);
            assert_invalid_root(&fixture, case);
        }

        let minimal_cases: &[(&str, Mutation)] = &[
            ("nonzero exact count without pages", |trailer| {
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 0, 0)
            }),
            ("nonzero exact count without postings", |trailer| {
                put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 0, 0)
            }),
            ("nonzero auxiliary count without payload", |trailer| {
                put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 0, 0)
            }),
        ];
        let minimal = root_fixture(&minimal_indexes());
        for (case, mutate) in minimal_cases {
            let fixture = mutate_root_trailer(&minimal, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_size_and_count_formula_mismatches() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let minimal_cases: &[(&str, Mutation)] = &[
            ("record length", |trailer| put_u32(trailer, 148, 39)),
            ("page length", |trailer| put_u32(trailer, 152, 16_383)),
            ("page count formula", |trailer| put_u32(trailer, 144, 2)),
            ("exact directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, offset, 95)
            }),
            ("exact pages length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, offset, 16_383)
            }),
            ("auxiliary directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, offset, 103)
            }),
        ];
        let minimal = root_fixture(&minimal_indexes());
        for (case, mutate) in minimal_cases {
            let fixture = mutate_root_trailer(&minimal, mutate);
            assert_invalid_root(&fixture, case);
        }

        let zero_cases: &[(&str, Mutation)] = &[
            ("empty exact directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, offset, 63)
            }),
            ("empty auxiliary directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, offset, 63)
            }),
        ];
        let zero = root_fixture(&SegmentIndexes::default());
        for (case, mutate) in zero_cases {
            let fixture = mutate_root_trailer(&zero, mutate);
            assert_invalid_root(&fixture, case);
        }

        let fixture = mutate_root_trailer(
            &root_fixture(&exact_boundary_indexes(410, false)),
            |trailer| put_u32(trailer, 144, 1),
        );
        assert_invalid_root(&fixture, "410 page count formula");
    }

    #[test]
    fn segment_index_v7_root_rejects_impossible_counts_without_allocation() {
        let zero = root_fixture(&SegmentIndexes::default());
        let impossible_exact = mutate_root_trailer(&zero, |trailer| {
            put_u64(trailer, 136, u64::MAX);
            put_u32(trailer, 144, u32::MAX);
            put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 28, 8);
            put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 36, 16_384);
        });
        assert_invalid_root(&impossible_exact, "exact count exceeds page-count domain");

        let impossible_auxiliary = mutate_root_trailer(&zero, |trailer| {
            put_u32(trailer, 156, u32::MAX);
            put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 28, 4);
        });
        assert_invalid_root(
            &impossible_auxiliary,
            "auxiliary count requires impossible directory length",
        );
    }

    #[test]
    fn segment_index_v7_is_deterministic_across_insertion_order() {
        assert_eq!(
            encode_v7(&deterministic_indexes(false)),
            encode_v7(&deterministic_indexes(true))
        );
    }

    #[test]
    fn segment_index_v7_writes_routing_first() {
        let bytes = encode_v7(&routing_indexes());
        assert!(bytes.len() >= SEGMENT_INDEX_V7_TRAILER_LEN);
        let trailer = &bytes[bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN..];
        assert_eq!(read_u32_at(trailer, 0), SEGMENT_INDEX_TRAILER_MAGIC);
        let (routing_offset, routing_len) = read_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET);
        let (metric_offset, _metric_len) = read_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET);
        assert_eq!(routing_offset, SEGMENT_INDEX_V7_HEADER_LEN as u64);
        assert!(routing_len > 0);
        assert_eq!(metric_offset, routing_offset + routing_len);
        assert_eq!(
            read_u32_at(&bytes, routing_offset as usize),
            ROUTING_INDEX_MAGIC
        );
    }

    #[test]
    fn segment_index_v7_codec_rejects_v6_header() {
        let v6 = b"SIDX\x06\x00\x00\x00";

        let error = validate_segment_indexes_v7_header(v6).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("version 7"));
    }

    #[test]
    fn segment_index_v7_layout_rejects_u64_overflow() {
        let error = plan_segment_indexes_v7_layout_for_test(u64::MAX).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("too large"));
    }

    #[test]
    fn segment_index_v7_rejects_empty_auxiliary_payload() {
        let mut indexes = minimal_indexes();
        indexes.label_values.insert_fst(1, Vec::new());
        let mut bytes = Vec::new();

        let error = write_segment_indexes_v7(&mut bytes, &indexes).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("zero-length auxiliary payload"));
        assert!(bytes.is_empty());
    }

    #[test]
    fn segment_index_v7_rejects_label_value_fst_without_values() {
        let mut indexes = minimal_indexes();
        let empty_fst = fst::SetBuilder::memory().into_inner().unwrap();
        indexes.label_values.insert_fst(1, empty_fst);
        let mut bytes = Vec::new();

        let error = write_segment_indexes_v7(&mut bytes, &indexes).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("no values"));
        assert!(bytes.is_empty());
    }

    #[test]
    fn segment_index_v7_rejects_invalid_exact_time_range() {
        let mut exact_postings = ExactPostingsIndex::default();
        exact_postings.insert(1, 2, 7);
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(1, 2, 2_000, 1_000);
        let indexes = SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };
        let mut bytes = Vec::new();

        let error = write_segment_indexes_v7(&mut bytes, &indexes).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("time range"));
        assert!(bytes.is_empty());
    }

    #[test]
    fn segment_index_v7_rejects_invalid_auxiliary_time_range() {
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(1, 2, 2_000, 1_000);
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };
        let mut bytes = Vec::new();

        let error = write_segment_indexes_v7(&mut bytes, &indexes).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("time range"));
        assert!(bytes.is_empty());
    }

    #[test]
    fn segment_index_v7_rejects_invalid_metric_time_range() {
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            1,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 2_000,
                max_time_ms: 1_000,
            },
        );
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges,
            routing_index: None,
        };
        let mut bytes = Vec::new();

        let error = write_segment_indexes_v7(&mut bytes, &indexes).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("time range"));
        assert!(bytes.is_empty());
    }

    #[test]
    fn segment_index_v7_rejects_noncanonical_metric_ranges_before_writing() {
        let cases = [
            "zero series count",
            "series end",
            "reversed time",
            "overlap",
        ];
        for case in cases {
            let mut metric_series_ranges = MetricSeriesRangeIndex::default();
            match case {
                "zero series count" => metric_series_ranges.insert_range(
                    1,
                    MetricSeriesRange {
                        start_series_ref: 0,
                        series_count: 0,
                        kind_mask: u16::from(SERIES_KIND_FLOAT),
                        min_time_ms: 100,
                        max_time_ms: 200,
                    },
                ),
                "series end" => metric_series_ranges.insert_range(
                    1,
                    MetricSeriesRange {
                        start_series_ref: u32::MAX,
                        series_count: 2,
                        kind_mask: u16::from(SERIES_KIND_FLOAT),
                        min_time_ms: 100,
                        max_time_ms: 200,
                    },
                ),
                "reversed time" => metric_series_ranges.insert_range(
                    1,
                    MetricSeriesRange {
                        start_series_ref: 0,
                        series_count: 1,
                        kind_mask: u16::from(SERIES_KIND_FLOAT),
                        min_time_ms: 200,
                        max_time_ms: 100,
                    },
                ),
                "overlap" => {
                    metric_series_ranges.insert_range(
                        1,
                        MetricSeriesRange {
                            start_series_ref: 0,
                            series_count: 2,
                            kind_mask: u16::from(SERIES_KIND_FLOAT),
                            min_time_ms: 100,
                            max_time_ms: 200,
                        },
                    );
                    metric_series_ranges.insert_range(
                        1,
                        MetricSeriesRange {
                            start_series_ref: 1,
                            series_count: 1,
                            kind_mask: u16::from(SERIES_KIND_FLOAT),
                            min_time_ms: 201,
                            max_time_ms: 300,
                        },
                    );
                }
                _ => unreachable!(),
            }
            let indexes = SegmentIndexes {
                exact_postings: ExactPostingsIndex::default(),
                label_values: LabelValueFstIndex::default(),
                label_value_time_ranges: LabelValueTimeRangeIndex::default(),
                metric_series_ranges,
                routing_index: None,
            };
            let mut bytes = Vec::new();

            let error = write_segment_indexes_v7(&mut bytes, &indexes).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{case}");
            assert!(bytes.is_empty(), "{case}");
        }
    }

    #[test]
    fn segment_index_v7_layout_rejects_zero_length_required_region() {
        let error = plan_segment_indexes_v7_layout(
            SegmentIndexV7PayloadLengths {
                routing: None,
                metric: 0,
                exact_postings: 0,
                auxiliary: 0,
            },
            0,
            0,
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("zero-length metric payload"));
    }

    #[test]
    fn segment_index_v7_rejects_postings_count_above_u32_before_writing() {
        let error = exact_postings_blob_len_from_count_v7(u64::from(u32::MAX) + 1).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("u32"));
    }

    fn put_locator(bytes: &mut [u8], offset: usize, blob_offset: u64, blob_len: u64) {
        assert_eq!(SEGMENT_INDEX_V7_LOCATOR_LEN, 16);
        put_u64(bytes, offset, blob_offset);
        put_u64(bytes, offset + 8, blob_len);
    }

    fn read_locator(bytes: &[u8], offset: usize) -> (u64, u64) {
        (read_u64_at(bytes, offset), read_u64_at(bytes, offset + 8))
    }

    fn locator_at(bytes: &[u8], offset: usize) -> BlobLocator {
        let (offset, len) = read_locator(bytes, offset);
        BlobLocator { offset, len }
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

    fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
    }

    fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    }
}
