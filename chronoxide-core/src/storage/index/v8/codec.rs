//! Pure root validation for the schema-7 index-container v8 reader.

use std::io;

use crc32c::crc32c;
use fst::{Set, Streamer};

use super::*;

/// Validates and binds the fixed v8 root before any non-root locator is used.
///
/// `expected_counts` must come from the same registered generation's already
/// validated series and symbol roots. This decoder intentionally has no
/// unbound variant: a trailer cannot authorize its own cardinalities.
pub(super) fn decode_root(
    actual_file_len: u64,
    header: &[u8],
    trailer: &[u8],
    expected_counts: RootCounts,
    format: AuthenticatedIndexFormat,
) -> io::Result<SegmentIndexV8Layout> {
    validate_exact_slice_len(header, HEADER_LEN, "v8 header")?;
    validate_exact_slice_len(trailer, TRAILER_LEN, "v8 trailer")?;
    if actual_file_len < (HEADER_LEN + TRAILER_LEN) as u64 {
        return Err(invalid_data("v8 file is shorter than its fixed root"));
    }
    validate_header(header, format)?;
    validate_trailer_fixed_fields(trailer, format)?;

    let stored_trailer_crc = read_u32(trailer, TRAILER_CRC_OFFSET);
    if crc_with_zeroed_field(trailer, TRAILER_CRC_OFFSET) != stored_trailer_crc {
        return Err(invalid_data("v8 trailer CRC mismatch"));
    }
    if read_u64(trailer, TRAILER_FILE_LEN_OFFSET) != actual_file_len {
        return Err(invalid_data(
            "v8 recorded file length does not match the actual file length",
        ));
    }

    let counts = RootCounts {
        series: read_u32(trailer, TRAILER_SERIES_COUNT_OFFSET),
        symbols: read_u32(trailer, TRAILER_SYMBOL_COUNT_OFFSET),
    };
    if counts != expected_counts {
        return Err(invalid_data(
            "v8 trailer counts do not match the bound series/symbol roots",
        ));
    }

    let exact_entry_count = read_u64(trailer, TRAILER_EXACT_ENTRY_COUNT_OFFSET);
    let exact_page_count = read_u32(trailer, TRAILER_EXACT_PAGE_COUNT_OFFSET);
    if exact_page_count != page_count(exact_entry_count)? {
        return Err(invalid_data(
            "v8 exact page count does not match the exact entry count",
        ));
    }
    if read_u32(trailer, TRAILER_EXACT_RECORD_LEN_OFFSET) != EXACT_RECORD_LEN as u32 {
        return Err(invalid_data("v8 exact record length is invalid"));
    }
    if read_u32(trailer, TRAILER_EXACT_PAGE_LEN_OFFSET) != EXACT_PAGE_LEN as u32 {
        return Err(invalid_data("v8 exact page length is invalid"));
    }
    let auxiliary_entry_count = read_u32(trailer, TRAILER_AUX_ENTRY_COUNT_OFFSET);

    let routing = decode_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET);
    let metric = decode_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET);
    let exact_directory = decode_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
    let exact_pages = decode_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
    let exact_postings = decode_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET);
    let auxiliary_directory = decode_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    let auxiliary_payloads = decode_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET);

    validate_optional_locator(routing, "routing")?;
    validate_required_locator(metric, "metric ranges")?;
    validate_required_locator(exact_directory, "exact directory")?;
    validate_optional_locator(exact_pages, "exact pages")?;
    validate_optional_locator(exact_postings, "exact postings")?;
    validate_required_locator(auxiliary_directory, "auxiliary directory")?;
    validate_optional_locator(auxiliary_payloads, "auxiliary payloads")?;

    validate_presence(
        exact_postings,
        exact_entry_count != 0,
        "v8 exact-postings locator presence disagrees with the entry count",
    )?;
    validate_presence(
        exact_pages,
        exact_page_count != 0,
        "v8 exact-pages locator presence disagrees with the page count",
    )?;
    validate_presence(
        auxiliary_payloads,
        auxiliary_entry_count != 0,
        "v8 auxiliary-payload locator presence disagrees with the entry count",
    )?;

    let expected_exact_directory_len = (EXACT_DIRECTORY_HEADER_LEN as u64)
        .checked_add(
            u64::from(exact_page_count)
                .checked_mul(EXACT_PAGE_DESCRIPTOR_LEN as u64)
                .ok_or_else(|| invalid_data("v8 exact directory length overflows"))?,
        )
        .ok_or_else(|| invalid_data("v8 exact directory length overflows"))?;
    if exact_directory.len != expected_exact_directory_len {
        return Err(invalid_data("v8 exact directory length is inconsistent"));
    }
    let expected_exact_pages_len = u64::from(exact_page_count)
        .checked_mul(EXACT_PAGE_LEN as u64)
        .ok_or_else(|| invalid_data("v8 exact-pages length overflows"))?;
    if exact_pages.len != expected_exact_pages_len {
        return Err(invalid_data("v8 exact-pages length is inconsistent"));
    }
    let expected_auxiliary_directory_len = (AUXILIARY_DIRECTORY_HEADER_LEN as u64)
        .checked_add(
            u64::from(auxiliary_entry_count)
                .checked_mul(AUXILIARY_RECORD_LEN as u64)
                .ok_or_else(|| invalid_data("v8 auxiliary directory length overflows"))?,
        )
        .ok_or_else(|| invalid_data("v8 auxiliary directory length overflows"))?;
    if auxiliary_directory.len != expected_auxiliary_directory_len {
        return Err(invalid_data(
            "v8 auxiliary directory length is inconsistent",
        ));
    }

    let trailer_offset = actual_file_len - TRAILER_LEN as u64;
    let ordered_regions = [
        ("routing", routing),
        ("metric ranges", metric),
        ("exact postings", exact_postings),
        ("auxiliary payloads", auxiliary_payloads),
        ("exact directory", exact_directory),
        ("exact pages", exact_pages),
        ("auxiliary directory", auxiliary_directory),
    ];
    let mut expected_offset = HEADER_LEN as u64;
    for (name, locator) in ordered_regions {
        if locator == BlobLocator::default() {
            continue;
        }
        if locator.offset != expected_offset {
            return Err(invalid_data_owned(format!(
                "v8 {name} region is not canonically adjacent"
            )));
        }
        let end = locator
            .offset
            .checked_add(locator.len)
            .ok_or_else(|| invalid_data_owned(format!("v8 {name} region end overflows")))?;
        if end > trailer_offset {
            return Err(invalid_data_owned(format!(
                "v8 {name} region extends into the fixed trailer"
            )));
        }
        expected_offset = end;
    }
    if expected_offset != trailer_offset {
        return Err(invalid_data(
            "v8 final directory is not adjacent to the fixed trailer",
        ));
    }

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
        exact_directory_crc32c: read_u32(trailer, TRAILER_EXACT_DIRECTORY_CRC_OFFSET),
        auxiliary_directory_crc32c: read_u32(trailer, TRAILER_AUX_DIRECTORY_CRC_OFFSET),
        counts,
        file_len: actual_file_len,
    })
}

/// Decodes the CRC-bound exact-directory header and page descriptors.
pub(super) fn decode_exact_directory(
    bytes: &[u8],
    root: SegmentIndexV8Layout,
) -> io::Result<ExactDirectory> {
    let expected_len = locator_len(root.exact_directory, "v8 exact directory")?;
    validate_exact_slice_len(bytes, expected_len, "v8 exact directory")?;
    if expected_len < EXACT_DIRECTORY_HEADER_LEN {
        return Err(invalid_data(
            "v8 exact directory is shorter than its header",
        ));
    }

    let encoded_crc = read_u32(bytes, 56);
    if encoded_crc != root.exact_directory_crc32c {
        return Err(invalid_data(
            "v8 exact directory CRC disagrees with the root",
        ));
    }
    if crc_with_zeroed_field(bytes, 56) != encoded_crc {
        return Err(invalid_data("v8 exact directory CRC mismatch"));
    }

    if read_u32(bytes, 0) != root.format.exact_directory_magic() {
        return Err(invalid_data("v8 exact directory magic mismatch"));
    }
    if read_u16(bytes, 4) != root.format.exact_directory_version() {
        return Err(invalid_data("v8 exact directory version mismatch"));
    }
    if read_u16(bytes, 6) != 0 {
        return Err(invalid_data("v8 exact directory flags are non-zero"));
    }
    if read_u32(bytes, 8) != EXACT_DIRECTORY_HEADER_LEN as u32 {
        return Err(invalid_data("v8 exact directory header length is invalid"));
    }
    if read_u32(bytes, 12) != EXACT_PAGE_DESCRIPTOR_LEN as u32 {
        return Err(invalid_data(
            "v8 exact directory descriptor length is invalid",
        ));
    }
    if read_u32(bytes, 16) != EXACT_PAGE_LEN as u32 {
        return Err(invalid_data("v8 exact directory page length is invalid"));
    }
    if read_u32(bytes, 20) != EXACT_RECORD_LEN as u32 {
        return Err(invalid_data("v8 exact directory record length is invalid"));
    }
    if read_u64(bytes, 24) != root.exact_entry_count {
        return Err(invalid_data(
            "v8 exact directory entry count disagrees with the root",
        ));
    }
    if read_u32(bytes, 32) != root.exact_page_count {
        return Err(invalid_data(
            "v8 exact directory page count disagrees with the root",
        ));
    }
    if read_u32(bytes, 36) != EXACT_RECORDS_PER_PAGE as u32 {
        return Err(invalid_data(
            "v8 exact directory records-per-page value is invalid",
        ));
    }
    if read_u64(bytes, 40) != EXACT_DIRECTORY_HEADER_LEN as u64 {
        return Err(invalid_data(
            "v8 exact directory descriptors offset is invalid",
        ));
    }
    let descriptors_len = u64::from(root.exact_page_count)
        .checked_mul(EXACT_PAGE_DESCRIPTOR_LEN as u64)
        .ok_or_else(|| invalid_data("v8 exact descriptor length overflows"))?;
    if read_u64(bytes, 48) != descriptors_len {
        return Err(invalid_data(
            "v8 exact directory descriptors length is invalid",
        ));
    }
    if read_u32(bytes, 60) != 0 {
        return Err(invalid_data(
            "v8 exact directory reserved field is non-zero",
        ));
    }

    let page_count = usize::try_from(root.exact_page_count)
        .map_err(|_| invalid_data("v8 exact page count exceeds platform usize"))?;
    let mut descriptors = Vec::new();
    descriptors.try_reserve_exact(page_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("v8 exact descriptor allocation failed: {error}"),
        )
    })?;
    let mut previous_last_key = None;
    let mut decoded_entry_count = 0u64;
    for page_index in 0..page_count {
        let offset = page_index
            .checked_mul(EXACT_PAGE_DESCRIPTOR_LEN)
            .and_then(|offset| offset.checked_add(EXACT_DIRECTORY_HEADER_LEN))
            .ok_or_else(|| invalid_data("v8 exact descriptor offset overflows"))?;
        let descriptor = bytes
            .get(offset..offset + EXACT_PAGE_DESCRIPTOR_LEN)
            .ok_or_else(|| unexpected_eof("v8 exact page descriptor is truncated"))?;
        let first_key = (read_u32(descriptor, 0), read_u32(descriptor, 4));
        let last_key = (read_u32(descriptor, 8), read_u32(descriptor, 12));
        if first_key.0 >= root.counts.symbols
            || first_key.1 >= root.counts.symbols
            || last_key.0 >= root.counts.symbols
            || last_key.1 >= root.counts.symbols
        {
            return Err(invalid_data(
                "v8 exact descriptor symbol exceeds the bound symbol count",
            ));
        }
        if first_key > last_key {
            return Err(invalid_data("v8 exact descriptor key range is reversed"));
        }
        if previous_last_key.is_some_and(|previous| previous >= first_key) {
            return Err(invalid_data(
                "v8 exact descriptors are unordered or overlapping",
            ));
        }
        if read_u32(descriptor, 20) != 0 || read_u32(descriptor, 28) != 0 {
            return Err(invalid_data(
                "v8 exact descriptor reserved field is non-zero",
            ));
        }
        let record_count = read_u32(descriptor, 16);
        let remaining = root
            .exact_entry_count
            .checked_sub(decoded_entry_count)
            .ok_or_else(|| invalid_data("v8 exact descriptor count underflows"))?;
        let expected_record_count = remaining.min(EXACT_RECORDS_PER_PAGE as u64);
        if record_count == 0 || u64::from(record_count) != expected_record_count {
            return Err(invalid_data(
                "v8 exact descriptor record count is noncanonical",
            ));
        }
        decoded_entry_count = decoded_entry_count
            .checked_add(u64::from(record_count))
            .ok_or_else(|| invalid_data("v8 exact descriptor count overflows"))?;
        let page_end = u64::try_from(page_index)
            .ok()
            .and_then(|page_index| page_index.checked_add(1))
            .and_then(|page_index| page_index.checked_mul(EXACT_PAGE_LEN as u64))
            .ok_or_else(|| invalid_data("v8 exact page range overflows"))?;
        if page_end > root.exact_pages.len {
            return Err(invalid_data(
                "v8 exact descriptor lies outside the exact-pages region",
            ));
        }
        descriptors.push(ExactPageDescriptor {
            first_key,
            last_key,
            record_count,
            page_crc32c: read_u32(descriptor, 24),
        });
        previous_last_key = Some(last_key);
    }
    if decoded_entry_count != root.exact_entry_count {
        return Err(invalid_data(
            "v8 exact descriptor counts disagree with the root",
        ));
    }
    Ok(ExactDirectory { descriptors })
}

/// Validates and decodes one complete exact-directory page.
pub(super) fn decode_exact_page(
    page: &[u8],
    page_index: usize,
    descriptor: ExactPageDescriptor,
    root: SegmentIndexV8Layout,
) -> io::Result<Vec<ExactRecord>> {
    validate_exact_slice_len(page, EXACT_PAGE_LEN, "v8 exact page")?;
    if crc32c(page) != descriptor.page_crc32c {
        return Err(invalid_data("v8 exact page CRC mismatch"));
    }

    let expected_page_index =
        u32::try_from(page_index).map_err(|_| invalid_data("v8 exact page index exceeds u32"))?;
    if expected_page_index >= root.exact_page_count {
        return Err(invalid_data("v8 exact page index is outside the root"));
    }
    let preceding = u64::from(expected_page_index)
        .checked_mul(EXACT_RECORDS_PER_PAGE as u64)
        .ok_or_else(|| invalid_data("v8 exact page entry offset overflows"))?;
    let remaining = root
        .exact_entry_count
        .checked_sub(preceding)
        .ok_or_else(|| invalid_data("v8 exact page entry count underflows"))?;
    let expected_record_count = remaining.min(EXACT_RECORDS_PER_PAGE as u64);
    if descriptor.record_count == 0 || u64::from(descriptor.record_count) != expected_record_count {
        return Err(invalid_data(
            "v8 exact page descriptor count is noncanonical",
        ));
    }
    if descriptor.first_key.0 >= root.counts.symbols
        || descriptor.first_key.1 >= root.counts.symbols
        || descriptor.last_key.0 >= root.counts.symbols
        || descriptor.last_key.1 >= root.counts.symbols
        || descriptor.first_key > descriptor.last_key
    {
        return Err(invalid_data(
            "v8 exact page descriptor key range is invalid",
        ));
    }

    if read_u32(page, 0) != root.format.exact_page_magic() {
        return Err(invalid_data("v8 exact page magic mismatch"));
    }
    if read_u16(page, 4) != root.format.exact_page_version() {
        return Err(invalid_data("v8 exact page version mismatch"));
    }
    if read_u16(page, 6) != 0 {
        return Err(invalid_data("v8 exact page flags are non-zero"));
    }
    if read_u32(page, 8) != expected_page_index {
        return Err(invalid_data("v8 exact page ordinal is invalid"));
    }
    if read_u32(page, 12) != descriptor.record_count {
        return Err(invalid_data(
            "v8 exact page count disagrees with its descriptor",
        ));
    }
    let record_count = usize::try_from(descriptor.record_count)
        .map_err(|_| invalid_data("v8 exact page count exceeds platform usize"))?;
    let records_end = record_count
        .checked_mul(EXACT_RECORD_LEN)
        .and_then(|len| len.checked_add(EXACT_PAGE_HEADER_LEN))
        .ok_or_else(|| invalid_data("v8 exact page records length overflows"))?;
    if records_end > page.len() {
        return Err(unexpected_eof("v8 exact page records are truncated"));
    }
    if page[records_end..].iter().any(|byte| *byte != 0) {
        return Err(invalid_data("v8 exact page padding is non-zero"));
    }

    let mut records = Vec::new();
    records.try_reserve_exact(record_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("v8 exact record allocation failed: {error}"),
        )
    })?;
    let mut previous_key = None;
    for record_index in 0..record_count {
        let offset = EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN;
        let bytes = page
            .get(offset..offset + EXACT_RECORD_LEN)
            .ok_or_else(|| unexpected_eof("v8 exact page record is truncated"))?;
        let key = (read_u32(bytes, 0), read_u32(bytes, 4));
        if key.0 >= root.counts.symbols || key.1 >= root.counts.symbols {
            return Err(invalid_data(
                "v8 exact record symbol exceeds the bound symbol count",
            ));
        }
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid_data(
                "v8 exact records are not strictly ordered and unique",
            ));
        }
        let record = ExactRecord {
            key,
            postings: BlobLocator {
                offset: read_u64(bytes, 8),
                len: read_u64(bytes, 16),
            },
            time_range: LabelValueTimeRange {
                min_time_ms: read_u64(bytes, 24),
                max_time_ms: read_u64(bytes, 32),
            },
            ref_count: read_u32(bytes, 40),
            payload_crc32c: read_u32(bytes, 44),
        };
        validate_exact_record(record, root)?;
        records.push(record);
        previous_key = Some(key);
    }
    if records.first().map(|record| record.key) != Some(descriptor.first_key)
        || records.last().map(|record| record.key) != Some(descriptor.last_key)
    {
        return Err(invalid_data(
            "v8 exact page fences disagree with its descriptor",
        ));
    }
    Ok(records)
}

/// Decodes one exact-postings payload after verifying its protected checksum.
pub(super) fn decode_exact_postings(
    bytes: &[u8],
    record: ExactRecord,
    root: SegmentIndexV8Layout,
) -> io::Result<Vec<u32>> {
    validate_exact_record(record, root)?;
    let expected_len = locator_len(record.postings, "v8 exact postings payload")?;
    validate_exact_slice_len(bytes, expected_len, "v8 exact postings payload")?;
    if crc32c(bytes) != record.payload_crc32c {
        return Err(invalid_data("v8 exact postings payload CRC mismatch"));
    }
    let count = usize::try_from(record.ref_count)
        .map_err(|_| invalid_data("v8 exact postings count exceeds platform usize"))?;
    if root.format == AuthenticatedIndexFormat::V8Raw && read_u32(bytes, 0) != record.ref_count {
        return Err(invalid_data(
            "v8 exact postings body count disagrees with its directory record",
        ));
    }
    if root.format == AuthenticatedIndexFormat::V9Adaptive
        && (bytes[1] != 0 || read_u16(bytes, 2) != 0)
    {
        return Err(invalid_data(
            "v9 exact postings header flags or reserved bytes are non-zero",
        ));
    }
    let mut refs = Vec::new();
    refs.try_reserve_exact(count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("v8 exact postings allocation failed: {error}"),
        )
    })?;
    match root.format {
        AuthenticatedIndexFormat::V8Raw => {
            decode_raw_refs(bytes, 4, count, root.counts.series, &mut refs)?;
        }
        AuthenticatedIndexFormat::V9Adaptive => match bytes[0] {
            EXACT_POSTINGS_CODEC_RAW32 => {
                decode_raw_refs(bytes, 4, count, root.counts.series, &mut refs)?;
                let raw_len = exact_raw_payload_len(count)?;
                let (canonical, _delta_len) = select_v9_codec(&refs, raw_len)?;
                if canonical != ExactPostingsCodec::Raw32 {
                    return Err(invalid_data(
                        "v9 exact postings RAW32 codec choice is noncanonical",
                    ));
                }
            }
            EXACT_POSTINGS_CODEC_DELTA_ULEB128 => {
                let mut cursor = EXACT_POSTINGS_V9_HEADER_LEN as usize;
                let mut previous = None;
                for index in 0..count {
                    let value = decode_canonical_uleb128_u32(bytes, &mut cursor)?;
                    let series_ref = if index == 0 {
                        value
                    } else {
                        if value == 0 {
                            return Err(invalid_data("v9 exact postings delta gap is zero"));
                        }
                        previous
                            .and_then(|previous: u32| previous.checked_add(value))
                            .ok_or_else(|| {
                                invalid_data("v9 exact postings delta addition overflows")
                            })?
                    };
                    validate_decoded_ref(series_ref, previous, root.counts.series)?;
                    refs.push(series_ref);
                    previous = Some(series_ref);
                }
                if cursor != bytes.len() {
                    return Err(invalid_data(
                        "v9 exact postings delta body has trailing bytes",
                    ));
                }
                let raw_len = exact_raw_payload_len(count)?;
                let (canonical, delta_len) = select_v9_codec(&refs, raw_len)?;
                if canonical != ExactPostingsCodec::DeltaUleb128 || delta_len != record.postings.len
                {
                    return Err(invalid_data(
                        "v9 exact postings delta codec choice is noncanonical",
                    ));
                }
            }
            _ => return Err(invalid_data("v9 exact postings codec is unknown")),
        },
    }
    Ok(refs)
}

fn decode_raw_refs(
    bytes: &[u8],
    body_offset: usize,
    count: usize,
    series_count: u32,
    refs: &mut Vec<u32>,
) -> io::Result<()> {
    let expected_len = body_offset
        .checked_add(
            count
                .checked_mul(4)
                .ok_or_else(|| invalid_data("exact postings raw length overflows"))?,
        )
        .ok_or_else(|| invalid_data("exact postings raw length overflows"))?;
    validate_exact_slice_len(bytes, expected_len, "exact postings raw payload")?;
    let mut previous = None;
    for index in 0..count {
        let offset = body_offset + index * 4;
        let series_ref = read_u32(bytes, offset);
        validate_decoded_ref(series_ref, previous, series_count)?;
        refs.push(series_ref);
        previous = Some(series_ref);
    }
    Ok(())
}

fn validate_decoded_ref(
    series_ref: u32,
    previous: Option<u32>,
    series_count: u32,
) -> io::Result<()> {
    if series_ref >= series_count {
        return Err(invalid_data(
            "exact postings ref exceeds the bound series count",
        ));
    }
    if previous.is_some_and(|previous| previous >= series_ref) {
        return Err(invalid_data(
            "exact postings refs are not strictly ordered and unique",
        ));
    }
    Ok(())
}

fn decode_canonical_uleb128_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    let start = *cursor;
    let mut value = 0u32;
    for index in 0..5usize {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "v9 exact postings varint is truncated",
            )
        })?;
        *cursor += 1;
        if index == 4 && (byte & 0xf0) != 0 {
            return Err(invalid_data("v9 exact postings varint exceeds u32"));
        }
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if *cursor - start != uleb128_u32_len(value) {
                return Err(invalid_data(
                    "v9 exact postings varint is not canonically encoded",
                ));
            }
            return Ok(value);
        }
    }
    Err(invalid_data("v9 exact postings varint exceeds u32"))
}

/// Decodes the CRC-bound auxiliary directory and validates paired summaries.
pub(super) fn decode_auxiliary_directory(
    bytes: &[u8],
    root: SegmentIndexV8Layout,
) -> io::Result<AuxiliaryDirectory> {
    let expected_len = locator_len(root.auxiliary_directory, "v8 auxiliary directory")?;
    validate_exact_slice_len(bytes, expected_len, "v8 auxiliary directory")?;
    if expected_len < AUXILIARY_DIRECTORY_HEADER_LEN {
        return Err(invalid_data(
            "v8 auxiliary directory is shorter than its header",
        ));
    }

    let encoded_crc = read_u32(bytes, 40);
    if encoded_crc != root.auxiliary_directory_crc32c {
        return Err(invalid_data(
            "v8 auxiliary directory CRC disagrees with the root",
        ));
    }
    if crc_with_zeroed_field(bytes, 40) != encoded_crc {
        return Err(invalid_data("v8 auxiliary directory CRC mismatch"));
    }

    if read_u32(bytes, 0) != AUXILIARY_DIRECTORY_MAGIC {
        return Err(invalid_data("v8 auxiliary directory magic mismatch"));
    }
    if read_u16(bytes, 4) != AUXILIARY_DIRECTORY_VERSION {
        return Err(invalid_data("v8 auxiliary directory version mismatch"));
    }
    if read_u16(bytes, 6) != 0 {
        return Err(invalid_data("v8 auxiliary directory flags are non-zero"));
    }
    if read_u32(bytes, 8) != AUXILIARY_DIRECTORY_HEADER_LEN as u32 {
        return Err(invalid_data(
            "v8 auxiliary directory header length is invalid",
        ));
    }
    if read_u32(bytes, 12) != AUXILIARY_RECORD_LEN as u32 {
        return Err(invalid_data(
            "v8 auxiliary directory record length is invalid",
        ));
    }
    if read_u64(bytes, 16) != u64::from(root.auxiliary_entry_count) {
        return Err(invalid_data(
            "v8 auxiliary directory count disagrees with the root",
        ));
    }
    if read_u64(bytes, 24) != AUXILIARY_DIRECTORY_HEADER_LEN as u64 {
        return Err(invalid_data(
            "v8 auxiliary directory records offset is invalid",
        ));
    }
    let records_len = u64::from(root.auxiliary_entry_count)
        .checked_mul(AUXILIARY_RECORD_LEN as u64)
        .ok_or_else(|| invalid_data("v8 auxiliary records length overflows"))?;
    if read_u64(bytes, 32) != records_len {
        return Err(invalid_data(
            "v8 auxiliary directory records length is invalid",
        ));
    }
    if bytes[44..AUXILIARY_DIRECTORY_HEADER_LEN]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(invalid_data(
            "v8 auxiliary directory reserved bytes are non-zero",
        ));
    }

    let entry_count = usize::try_from(root.auxiliary_entry_count)
        .map_err(|_| invalid_data("v8 auxiliary count exceeds platform usize"))?;
    let mut records = Vec::new();
    records.try_reserve_exact(entry_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("v8 auxiliary record allocation failed: {error}"),
        )
    })?;
    let mut previous_key = None;
    for record_index in 0..entry_count {
        let offset = record_index
            .checked_mul(AUXILIARY_RECORD_LEN)
            .and_then(|offset| offset.checked_add(AUXILIARY_DIRECTORY_HEADER_LEN))
            .ok_or_else(|| invalid_data("v8 auxiliary record offset overflows"))?;
        let bytes = bytes
            .get(offset..offset + AUXILIARY_RECORD_LEN)
            .ok_or_else(|| unexpected_eof("v8 auxiliary record is truncated"))?;
        let record = AuxiliaryRecord {
            kind: read_u16(bytes, 0),
            label_name_sym: read_u32(bytes, 4),
            payload: BlobLocator {
                offset: read_u64(bytes, 8),
                len: read_u64(bytes, 16),
            },
            time_range: LabelValueTimeRange {
                min_time_ms: read_u64(bytes, 24),
                max_time_ms: read_u64(bytes, 32),
            },
            item_count: read_u32(bytes, 40),
            payload_crc32c: read_u32(bytes, 44),
        };
        if read_u16(bytes, 2) != 0 {
            return Err(invalid_data("v8 auxiliary record flags are non-zero"));
        }
        validate_auxiliary_record(record, root)?;
        let key = (record.kind, record.label_name_sym);
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid_data(
                "v8 auxiliary records are not strictly ordered and unique",
            ));
        }
        records.push(record);
        previous_key = Some(key);
    }
    let directory = AuxiliaryDirectory { records };
    for fst_record in directory
        .records
        .iter()
        .filter(|record| record.kind == SEGMENT_INDEX_BLOB_LABEL_VALUE_FST)
    {
        match directory.record(
            SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
            fst_record.label_name_sym,
        ) {
            Some(range_record)
                if range_record.item_count != fst_record.item_count
                    || range_record.time_range != fst_record.time_range =>
            {
                return Err(invalid_data(
                    "v8 paired FST and time-range summaries disagree",
                ));
            }
            None if fst_record.time_range != UNCONSTRAINED_TIME_RANGE => {
                return Err(invalid_data("v8 unpaired FST summary is not unconstrained"));
            }
            Some(_) | None => {}
        }
    }
    Ok(directory)
}

/// Validates one protected raw FST payload and returns its borrowed set.
pub(super) fn decode_auxiliary_fst(
    bytes: &[u8],
    record: AuxiliaryRecord,
    root: SegmentIndexV8Layout,
) -> io::Result<Set<&[u8]>> {
    validate_auxiliary_record_kind(record, root, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST)?;
    let expected_len = locator_len(record.payload, "v8 auxiliary FST payload")?;
    validate_exact_slice_len(bytes, expected_len, "v8 auxiliary FST payload")?;
    if crc32c(bytes) != record.payload_crc32c {
        return Err(invalid_data("v8 auxiliary FST payload CRC mismatch"));
    }
    let set = Set::new(bytes).map_err(|error| {
        invalid_data_owned(format!("v8 auxiliary FST payload is invalid: {error}"))
    })?;
    if set.is_empty() {
        return Err(invalid_data("v8 auxiliary FST payload has no values"));
    }
    let expected_item_count = usize::try_from(record.item_count)
        .map_err(|_| invalid_data("v8 auxiliary FST count exceeds platform usize"))?;
    if set.len() != expected_item_count {
        return Err(invalid_data(
            "v8 auxiliary FST item count disagrees with its directory record",
        ));
    }
    let mut stream = set.stream();
    while let Some(value) = stream.next() {
        std::str::from_utf8(value).map_err(|error| {
            invalid_data_owned(format!("v8 auxiliary FST value is not UTF-8: {error}"))
        })?;
    }
    Ok(set)
}

/// Decodes one protected label-value time-range payload.
pub(super) fn decode_auxiliary_time_ranges(
    bytes: &[u8],
    record: AuxiliaryRecord,
    root: SegmentIndexV8Layout,
) -> io::Result<Vec<(u32, LabelValueTimeRange)>> {
    validate_auxiliary_record_kind(record, root, SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES)?;
    let expected_len = locator_len(record.payload, "v8 auxiliary time-range payload")?;
    validate_exact_slice_len(bytes, expected_len, "v8 auxiliary time-range payload")?;
    if crc32c(bytes) != record.payload_crc32c {
        return Err(invalid_data("v8 auxiliary time-range payload CRC mismatch"));
    }
    if read_u32(bytes, 0) != record.item_count {
        return Err(invalid_data(
            "v8 auxiliary time-range body count disagrees with its directory record",
        ));
    }

    let count = usize::try_from(record.item_count)
        .map_err(|_| invalid_data("v8 auxiliary item count exceeds platform usize"))?;
    let mut ranges = Vec::new();
    ranges.try_reserve_exact(count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("v8 auxiliary time-range allocation failed: {error}"),
        )
    })?;
    let mut previous_value = None;
    let mut aggregate = LabelValueTimeRange {
        min_time_ms: u64::MAX,
        max_time_ms: 0,
    };
    for index in 0..count {
        let offset = 4 + index * 20;
        let value_sym = read_u32(bytes, offset);
        if value_sym >= root.counts.symbols {
            return Err(invalid_data(
                "v8 auxiliary value symbol exceeds the bound symbol count",
            ));
        }
        if previous_value.is_some_and(|previous| previous >= value_sym) {
            return Err(invalid_data(
                "v8 auxiliary value symbols are not strictly ordered and unique",
            ));
        }
        let range = LabelValueTimeRange {
            min_time_ms: read_u64(bytes, offset + 4),
            max_time_ms: read_u64(bytes, offset + 12),
        };
        if range.min_time_ms > range.max_time_ms {
            return Err(invalid_data("v8 auxiliary time range is reversed"));
        }
        aggregate.min_time_ms = aggregate.min_time_ms.min(range.min_time_ms);
        aggregate.max_time_ms = aggregate.max_time_ms.max(range.max_time_ms);
        ranges.push((value_sym, range));
        previous_value = Some(value_sym);
    }
    if aggregate != record.time_range {
        return Err(invalid_data(
            "v8 auxiliary time-range aggregate disagrees with its directory record",
        ));
    }
    Ok(ranges)
}

fn validate_exact_record(record: ExactRecord, root: SegmentIndexV8Layout) -> io::Result<()> {
    if record.key.0 >= root.counts.symbols || record.key.1 >= root.counts.symbols {
        return Err(invalid_data(
            "v8 exact record symbol exceeds the bound symbol count",
        ));
    }
    if record.ref_count == 0 {
        return Err(invalid_data("v8 exact record has no refs"));
    }
    if record.ref_count > root.counts.series {
        return Err(invalid_data(
            "exact postings ref count exceeds the bound series count",
        ));
    }
    let raw_len = 4u64
        .checked_add(
            u64::from(record.ref_count)
                .checked_mul(4)
                .ok_or_else(|| invalid_data("exact postings count-derived length overflows"))?,
        )
        .ok_or_else(|| invalid_data("exact postings count-derived length overflows"))?;
    match root.format {
        AuthenticatedIndexFormat::V8Raw if record.postings.len != raw_len => {
            return Err(invalid_data(
                "v8 exact postings locator length disagrees with its ref count",
            ));
        }
        AuthenticatedIndexFormat::V9Adaptive => {
            let minimum_len = EXACT_POSTINGS_V9_HEADER_LEN
                .checked_add(u64::from(record.ref_count))
                .ok_or_else(|| invalid_data("v9 exact postings minimum length overflows"))?;
            if record.postings.len < minimum_len || record.postings.len > raw_len {
                return Err(invalid_data(
                    "v9 exact postings locator length is outside its count-derived bounds",
                ));
            }
        }
        _ => {}
    }
    validate_child_locator(
        record.postings,
        root.exact_postings,
        "v8 exact postings payload",
    )?;
    if record.time_range.min_time_ms > record.time_range.max_time_ms {
        return Err(invalid_data("v8 exact record time range is reversed"));
    }
    Ok(())
}

fn validate_auxiliary_record(
    record: AuxiliaryRecord,
    root: SegmentIndexV8Layout,
) -> io::Result<()> {
    if !matches!(
        record.kind,
        SEGMENT_INDEX_BLOB_LABEL_VALUE_FST | SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES
    ) {
        return Err(invalid_data("v8 auxiliary record kind is unsupported"));
    }
    if record.label_name_sym >= root.counts.symbols {
        return Err(invalid_data(
            "v8 auxiliary label-name symbol exceeds the bound symbol count",
        ));
    }
    if record.item_count == 0 {
        return Err(invalid_data("v8 auxiliary record has no items"));
    }
    validate_child_locator(
        record.payload,
        root.auxiliary_payloads,
        "v8 auxiliary payload",
    )?;
    if record.time_range.min_time_ms > record.time_range.max_time_ms {
        return Err(invalid_data("v8 auxiliary record time range is reversed"));
    }
    if record.kind == SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES {
        let expected_len = 4u64
            .checked_add(
                u64::from(record.item_count)
                    .checked_mul(20)
                    .ok_or_else(|| invalid_data("v8 auxiliary count-derived length overflows"))?,
            )
            .ok_or_else(|| invalid_data("v8 auxiliary count-derived length overflows"))?;
        if record.payload.len != expected_len {
            return Err(invalid_data(
                "v8 auxiliary time-range length disagrees with its item count",
            ));
        }
    }
    Ok(())
}

fn validate_auxiliary_record_kind(
    record: AuxiliaryRecord,
    root: SegmentIndexV8Layout,
    expected_kind: u16,
) -> io::Result<()> {
    validate_auxiliary_record(record, root)?;
    if record.kind != expected_kind {
        return Err(invalid_data(
            "v8 auxiliary payload decoder does not match the record kind",
        ));
    }
    Ok(())
}

fn validate_child_locator(
    child: BlobLocator,
    parent: BlobLocator,
    description: &'static str,
) -> io::Result<()> {
    if child.offset == 0 || child.len == 0 {
        return Err(invalid_data_owned(format!(
            "{description} locator is empty or half-empty"
        )));
    }
    let child_end = child
        .offset
        .checked_add(child.len)
        .ok_or_else(|| invalid_data_owned(format!("{description} locator overflows")))?;
    let parent_end = parent
        .offset
        .checked_add(parent.len)
        .ok_or_else(|| invalid_data_owned(format!("{description} parent range overflows")))?;
    if parent == BlobLocator::default() || child.offset < parent.offset || child_end > parent_end {
        return Err(invalid_data_owned(format!(
            "{description} lies outside its root region"
        )));
    }
    Ok(())
}

fn locator_len(locator: BlobLocator, description: &'static str) -> io::Result<usize> {
    usize::try_from(locator.len)
        .map_err(|_| invalid_data_owned(format!("{description} length exceeds platform usize")))
}

fn validate_header(header: &[u8], format: AuthenticatedIndexFormat) -> io::Result<()> {
    if read_u32(header, 0) != SEGMENT_INDEXES_MAGIC {
        return Err(invalid_data("v8 header magic mismatch"));
    }
    if read_u16(header, 4) != format.version() {
        return Err(invalid_data("v8 header version mismatch"));
    }
    if read_u16(header, 6) != 0 {
        return Err(invalid_data("v8 header flags are non-zero"));
    }
    if read_u32(header, 8) != HEADER_LEN as u32 {
        return Err(invalid_data("v8 header length is invalid"));
    }
    if read_u32(header, 12) != 0 {
        return Err(invalid_data("v8 header reserved field is non-zero"));
    }
    Ok(())
}

fn validate_trailer_fixed_fields(
    trailer: &[u8],
    format: AuthenticatedIndexFormat,
) -> io::Result<()> {
    if read_u32(trailer, 0) != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(invalid_data("v8 trailer magic mismatch"));
    }
    if read_u16(trailer, 4) != format.version() {
        return Err(invalid_data("v8 trailer version mismatch"));
    }
    if read_u16(trailer, 6) != 0 {
        return Err(invalid_data("v8 trailer flags are non-zero"));
    }
    if read_u32(trailer, 8) != TRAILER_LEN as u32 {
        return Err(invalid_data("v8 trailer length is invalid"));
    }
    if read_u32(trailer, 12) != 0 {
        return Err(invalid_data("v8 trailer reserved0 is non-zero"));
    }
    if trailer[TRAILER_RESERVED_OFFSET..TRAILER_TERMINAL_MAGIC_OFFSET]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(invalid_data("v8 trailer reserved bytes are non-zero"));
    }
    if read_u32(trailer, TRAILER_TERMINAL_MAGIC_OFFSET) != format.terminal_magic() {
        return Err(invalid_data("v8 trailer terminal magic mismatch"));
    }
    Ok(())
}

fn validate_exact_slice_len(bytes: &[u8], expected: usize, name: &'static str) -> io::Result<()> {
    if bytes.len() < expected {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{name} is truncated"),
        ));
    }
    if bytes.len() != expected {
        return Err(invalid_data_owned(format!("{name} has trailing bytes")));
    }
    Ok(())
}

fn decode_locator(trailer: &[u8], offset: usize) -> BlobLocator {
    BlobLocator {
        offset: read_u64(trailer, offset),
        len: read_u64(trailer, offset + 8),
    }
}

fn validate_optional_locator(locator: BlobLocator, name: &'static str) -> io::Result<()> {
    if (locator.offset == 0) != (locator.len == 0) {
        return Err(invalid_data_owned(format!(
            "v8 {name} locator is half-empty"
        )));
    }
    Ok(())
}

fn validate_required_locator(locator: BlobLocator, name: &'static str) -> io::Result<()> {
    if locator.offset == 0 || locator.len == 0 {
        return Err(invalid_data_owned(format!(
            "v8 required {name} locator is missing or half-empty"
        )));
    }
    Ok(())
}

fn validate_presence(
    locator: BlobLocator,
    expected_present: bool,
    message: &'static str,
) -> io::Result<()> {
    if (locator != BlobLocator::default()) != expected_present {
        return Err(invalid_data(message));
    }
    Ok(())
}

fn invalid_data_owned(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn unexpected_eof(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, message)
}

#[cfg(test)]
pub(super) fn rewrite_trailer_crc(trailer: &mut [u8]) {
    put_u32(trailer, TRAILER_CRC_OFFSET, 0);
    let crc = crc32c(trailer);
    put_u32(trailer, TRAILER_CRC_OFFSET, crc);
}
