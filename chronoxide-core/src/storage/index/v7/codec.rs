//! Pure validators shared by legacy and governed v7 index readers.

use std::io;

use crc32c::{crc32c, crc32c_append};
use fst::{IntoStreamer, Set, Streamer};

use super::super::{
    ExactPostingsMetadata, ExactPostingsSelection, LabelValueTimeRange, MetricSeriesRange,
    MetricSeriesRangeBlobBounds, MetricSeriesRangeBlobEvent, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, read_label_value_time_ranges_blob,
    walk_metric_series_ranges_blob,
};
use super::{
    AUXILIARY_DIRECTORY_HEADER_LEN, AUXILIARY_DIRECTORY_MAGIC, AUXILIARY_DIRECTORY_RECORD_LEN,
    AUXILIARY_DIRECTORY_VERSION, BlobLocator, EXACT_DIRECTORY_HEADER_LEN, EXACT_DIRECTORY_MAGIC,
    EXACT_DIRECTORY_VERSION, EXACT_PAGE_DESCRIPTOR_LEN, EXACT_PAGE_HEADER_LEN, EXACT_PAGE_LEN,
    EXACT_PAGE_MAGIC, EXACT_PAGE_VERSION, EXACT_RECORD_LEN, EXACT_RECORDS_PER_PAGE,
    SegmentIndexV7Layout, read_u16_at, read_u32_at, read_u64_at,
};

#[derive(Debug)]
pub(super) struct ExactDirectory {
    pub(super) descriptors: Vec<ExactPageDescriptor>,
}

#[derive(Debug)]
pub(super) struct AuxiliaryDirectory {
    pub(super) records: Box<[AuxiliaryRecord]>,
    pub(super) fst_count: usize,
}

impl AuxiliaryDirectory {
    pub(super) fn record(&self, kind: u16, label_name_sym: u32) -> Option<AuxiliaryRecord> {
        self.records
            .binary_search_by_key(&(kind, label_name_sym), |record| {
                (record.kind, record.label_name_sym)
            })
            .ok()
            .map(|index| self.records[index])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AuxiliaryRecord {
    pub(super) kind: u16,
    pub(super) label_name_sym: u32,
    pub(super) payload: BlobLocator,
    pub(super) time_range: LabelValueTimeRange,
}

#[derive(Debug)]
pub(super) struct MetricSeriesRangeDirectory {
    pub(super) groups: Vec<MetricSeriesRangeGroupDescriptor>,
}

impl MetricSeriesRangeDirectory {
    fn group(&self, metric_sym: u32) -> Option<MetricSeriesRangeGroupDescriptor> {
        self.groups
            .binary_search_by_key(&metric_sym, |group| group.metric_sym)
            .ok()
            .map(|index| self.groups[index])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MetricSeriesRangeGroupDescriptor {
    metric_sym: u32,
    ranges_offset: usize,
    range_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExactPageDescriptor {
    pub(super) first_key: (u32, u32),
    pub(super) last_key: (u32, u32),
    pub(super) record_count: u32,
    pub(super) page_crc32c: u32,
}

pub(super) struct ValidatedExactPage<'a> {
    bytes: &'a [u8],
    record_count: usize,
}

impl ValidatedExactPage<'_> {
    pub(super) fn record(&self, record_index: usize) -> ((u32, u32), ExactPostingsSelection) {
        let offset = EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN;
        let record = &self.bytes[offset..offset + EXACT_RECORD_LEN];
        let postings = BlobLocator {
            offset: read_u64_at(record, 8),
            len: read_u64_at(record, 16),
        };
        (
            (read_u32_at(record, 0), read_u32_at(record, 4)),
            ExactPostingsSelection::new(
                ExactPostingsMetadata {
                    byte_len: postings.len,
                    time_range: LabelValueTimeRange {
                        min_time_ms: read_u64_at(record, 24),
                        max_time_ms: read_u64_at(record, 32),
                    },
                },
                postings.offset,
                postings.len,
            ),
        )
    }

    pub(super) fn selection(&self, key: (u32, u32)) -> Option<ExactPostingsSelection> {
        let mut left = 0usize;
        let mut right = self.record_count;
        while left < right {
            let middle = left + (right - left) / 2;
            let (middle_key, selection) = self.record(middle);
            match middle_key.cmp(&key) {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Some(selection),
            }
        }
        None
    }

    pub(super) fn records(
        &self,
    ) -> impl Iterator<Item = ((u32, u32), ExactPostingsSelection)> + '_ {
        (0..self.record_count).map(|record_index| self.record(record_index))
    }
}

pub(super) fn decode_exact_directory(
    bytes: &[u8],
    root: SegmentIndexV7Layout,
    symbol_count: Option<u32>,
) -> io::Result<ExactDirectory> {
    if bytes.len() < EXACT_DIRECTORY_HEADER_LEN {
        return Err(invalid_exact_data(
            "exact directory is shorter than its fixed header",
        ));
    }
    if read_u32_at(bytes, 0) != EXACT_DIRECTORY_MAGIC {
        return Err(invalid_exact_data("exact directory magic mismatch"));
    }
    if read_u16_at(bytes, 4) != EXACT_DIRECTORY_VERSION {
        return Err(invalid_exact_data("exact directory version mismatch"));
    }
    if read_u16_at(bytes, 6) != 0 {
        return Err(invalid_exact_data("exact directory flags are non-zero"));
    }
    if read_u32_at(bytes, 8) != EXACT_DIRECTORY_HEADER_LEN as u32 {
        return Err(invalid_exact_data(
            "exact directory header length is invalid",
        ));
    }
    if read_u32_at(bytes, 12) != EXACT_PAGE_DESCRIPTOR_LEN as u32 {
        return Err(invalid_exact_data(
            "exact directory descriptor length is invalid",
        ));
    }
    if read_u32_at(bytes, 16) != EXACT_PAGE_LEN as u32 {
        return Err(invalid_exact_data("exact directory page length is invalid"));
    }
    if read_u32_at(bytes, 20) != EXACT_RECORD_LEN as u32 {
        return Err(invalid_exact_data(
            "exact directory record length is invalid",
        ));
    }
    if read_u64_at(bytes, 24) != root.exact_entry_count {
        return Err(invalid_exact_data(
            "exact directory entry count does not match the root",
        ));
    }
    if read_u32_at(bytes, 32) != root.exact_page_count {
        return Err(invalid_exact_data(
            "exact directory page count does not match the root",
        ));
    }
    if read_u32_at(bytes, 36) != EXACT_RECORDS_PER_PAGE as u32 {
        return Err(invalid_exact_data(
            "exact directory records-per-page value is invalid",
        ));
    }
    if read_u64_at(bytes, 40) != EXACT_DIRECTORY_HEADER_LEN as u64 {
        return Err(invalid_exact_data(
            "exact directory descriptors offset is invalid",
        ));
    }
    let expected_descriptors_len = u64::from(root.exact_page_count)
        .checked_mul(EXACT_PAGE_DESCRIPTOR_LEN as u64)
        .ok_or_else(|| invalid_exact_data("exact directory descriptor length overflows"))?;
    if read_u64_at(bytes, 48) != expected_descriptors_len {
        return Err(invalid_exact_data(
            "exact directory descriptors length is invalid",
        ));
    }
    if read_u32_at(bytes, 60) != 0 {
        return Err(invalid_exact_data(
            "exact directory reserved field is non-zero",
        ));
    }
    let expected_directory_len = (EXACT_DIRECTORY_HEADER_LEN as u64)
        .checked_add(expected_descriptors_len)
        .ok_or_else(|| invalid_exact_data("exact directory length overflows"))?;
    if expected_directory_len != root.exact_directory.len
        || usize::try_from(expected_directory_len).ok() != Some(bytes.len())
    {
        return Err(invalid_exact_data("exact directory length is inconsistent"));
    }
    let stored_crc = read_u32_at(bytes, 56);
    let crc = crc32c_append(
        crc32c_append(crc32c_append(0, &bytes[..56]), &[0; 4]),
        &bytes[60..],
    );
    if crc != stored_crc {
        return Err(invalid_exact_data("exact directory CRC mismatch"));
    }
    let page_count = usize::try_from(root.exact_page_count)
        .map_err(|_| invalid_exact_data("exact directory page count exceeds platform usize"))?;
    let mut descriptors = Vec::new();
    descriptors.try_reserve_exact(page_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("exact directory descriptor allocation failed: {error}"),
        )
    })?;
    let mut previous_last_key = None;
    let mut decoded_entry_count = 0u64;
    for page_index in 0..page_count {
        let offset = EXACT_DIRECTORY_HEADER_LEN + page_index * EXACT_PAGE_DESCRIPTOR_LEN;
        let descriptor = bytes
            .get(offset..offset + EXACT_PAGE_DESCRIPTOR_LEN)
            .ok_or_else(|| invalid_exact_data("exact page descriptor truncated"))?;
        let first_key = (read_u32_at(descriptor, 0), read_u32_at(descriptor, 4));
        let last_key = (read_u32_at(descriptor, 8), read_u32_at(descriptor, 12));
        if symbol_count.is_some_and(|symbol_count| {
            first_key.0 >= symbol_count
                || first_key.1 >= symbol_count
                || last_key.0 >= symbol_count
                || last_key.1 >= symbol_count
        }) {
            return Err(invalid_exact_data(
                "exact page descriptor symbol exceeds the authoritative symbol count",
            ));
        }
        let record_count = read_u32_at(descriptor, 16);
        if read_u32_at(descriptor, 20) != 0 || read_u32_at(descriptor, 28) != 0 {
            return Err(invalid_exact_data(
                "exact page descriptor reserved field is non-zero",
            ));
        }
        if first_key > last_key {
            return Err(invalid_exact_data(
                "exact page descriptor key range is reversed",
            ));
        }
        if previous_last_key.is_some_and(|previous| previous >= first_key) {
            return Err(invalid_exact_data(
                "exact page descriptors are unordered or overlapping",
            ));
        }
        let remaining_entries = root
            .exact_entry_count
            .checked_sub(decoded_entry_count)
            .ok_or_else(|| invalid_exact_data("exact descriptor entry count underflows"))?;
        let expected_record_count = remaining_entries.min(EXACT_RECORDS_PER_PAGE as u64);
        if u64::from(record_count) != expected_record_count || record_count == 0 {
            return Err(invalid_exact_data(
                "exact page descriptor record count is invalid",
            ));
        }
        decoded_entry_count = decoded_entry_count
            .checked_add(u64::from(record_count))
            .ok_or_else(|| invalid_exact_data("exact descriptor entry count overflows"))?;
        let relative_page_offset = u64::try_from(page_index)
            .ok()
            .and_then(|index| index.checked_mul(EXACT_PAGE_LEN as u64))
            .ok_or_else(|| invalid_exact_data("exact page offset overflows"))?;
        let relative_page_end = relative_page_offset
            .checked_add(EXACT_PAGE_LEN as u64)
            .ok_or_else(|| invalid_exact_data("exact page end overflows"))?;
        if relative_page_end > root.exact_pages.len {
            return Err(invalid_exact_data(
                "exact page descriptor lies outside the exact-pages region",
            ));
        }
        descriptors.push(ExactPageDescriptor {
            first_key,
            last_key,
            record_count,
            page_crc32c: read_u32_at(descriptor, 24),
        });
        previous_last_key = Some(last_key);
    }
    if decoded_entry_count != root.exact_entry_count {
        return Err(invalid_exact_data(
            "exact descriptor counts do not match the root entry count",
        ));
    }
    Ok(ExactDirectory { descriptors })
}

pub(super) fn validate_exact_page<'a>(
    page: &'a [u8],
    page_index: usize,
    descriptor: ExactPageDescriptor,
    root: SegmentIndexV7Layout,
    symbol_count: Option<u32>,
) -> io::Result<ValidatedExactPage<'a>> {
    if page.len() != EXACT_PAGE_LEN {
        return Err(invalid_exact_data(
            "exact page buffer has the wrong exact length",
        ));
    }
    if crc32c(page) != descriptor.page_crc32c {
        return Err(invalid_exact_data("exact page CRC mismatch"));
    }
    if read_u32_at(page, 0) != EXACT_PAGE_MAGIC {
        return Err(invalid_exact_data("exact page magic mismatch"));
    }
    if read_u16_at(page, 4) != EXACT_PAGE_VERSION {
        return Err(invalid_exact_data("exact page version mismatch"));
    }
    if read_u16_at(page, 6) != 0 {
        return Err(invalid_exact_data("exact page flags are non-zero"));
    }
    let expected_page_index = u32::try_from(page_index)
        .map_err(|_| invalid_exact_data("exact page index exceeds u32"))?;
    if read_u32_at(page, 8) != expected_page_index {
        return Err(invalid_exact_data("exact page index is invalid"));
    }
    if read_u32_at(page, 12) != descriptor.record_count {
        return Err(invalid_exact_data(
            "exact page record count does not match its descriptor",
        ));
    }
    let record_count = usize::try_from(descriptor.record_count)
        .map_err(|_| invalid_exact_data("exact page record count exceeds platform usize"))?;
    let records_end = record_count
        .checked_mul(EXACT_RECORD_LEN)
        .and_then(|len| len.checked_add(EXACT_PAGE_HEADER_LEN))
        .ok_or_else(|| invalid_exact_data("exact page records length overflows"))?;
    if records_end > page.len() {
        return Err(invalid_exact_data("exact page records exceed the page"));
    }
    if page[records_end..].iter().any(|byte| *byte != 0) {
        return Err(invalid_exact_data("exact page padding is non-zero"));
    }
    let exact_postings_end = root
        .exact_postings
        .offset
        .checked_add(root.exact_postings.len)
        .ok_or_else(|| invalid_exact_data("exact postings root range overflows"))?;
    let mut previous_key = None;
    let mut first_key = None;
    let mut last_key = None;
    for record_index in 0..record_count {
        let offset = EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN;
        let record = page
            .get(offset..offset + EXACT_RECORD_LEN)
            .ok_or_else(|| invalid_exact_data("exact page record truncated"))?;
        let record_key = (read_u32_at(record, 0), read_u32_at(record, 4));
        if symbol_count.is_some_and(|symbol_count| {
            record_key.0 >= symbol_count || record_key.1 >= symbol_count
        }) {
            return Err(invalid_exact_data(
                "exact page record symbol exceeds the authoritative symbol count",
            ));
        }
        if previous_key.is_some_and(|previous| previous >= record_key) {
            return Err(invalid_exact_data(
                "exact page records are not strictly ordered and unique",
            ));
        }
        first_key.get_or_insert(record_key);
        last_key = Some(record_key);
        previous_key = Some(record_key);
        let postings = BlobLocator {
            offset: read_u64_at(record, 8),
            len: read_u64_at(record, 16),
        };
        if postings.len < 4 || !(postings.len - 4).is_multiple_of(4) {
            return Err(invalid_exact_data(
                "exact postings locator length is not a canonical payload length",
            ));
        }
        let postings_end = postings
            .offset
            .checked_add(postings.len)
            .ok_or_else(|| invalid_exact_data("exact postings locator overflows"))?;
        if postings.offset < root.exact_postings.offset || postings_end > exact_postings_end {
            return Err(invalid_exact_data(
                "exact postings locator lies outside the postings region",
            ));
        }
        let min_time_ms = read_u64_at(record, 24);
        let max_time_ms = read_u64_at(record, 32);
        if min_time_ms > max_time_ms {
            return Err(invalid_exact_data("exact page time range is reversed"));
        }
    }
    if first_key != Some(descriptor.first_key) || last_key != Some(descriptor.last_key) {
        return Err(invalid_exact_data(
            "exact page key bounds do not match its descriptor",
        ));
    }
    Ok(ValidatedExactPage {
        bytes: page,
        record_count,
    })
}

/// Reconstructs a borrowed view only after a governed cached value has matched
/// the complete root, page ordinal, and descriptor context used at validation.
pub(super) fn trusted_exact_page_selection(
    page: &[u8],
    descriptor: ExactPageDescriptor,
    key: (u32, u32),
) -> Option<ExactPostingsSelection> {
    ValidatedExactPage {
        bytes: page,
        record_count: descriptor.record_count as usize,
    }
    .selection(key)
}

pub(super) fn decode_exact_postings(bytes: &[u8]) -> io::Result<Vec<u32>> {
    if bytes.len() < 4 {
        return Err(invalid_exact_data(
            "exact postings payload is shorter than its count",
        ));
    }
    let count = read_u32_at(bytes, 0) as usize;
    if count == 0 {
        return Err(invalid_exact_data("exact postings payload has no refs"));
    }
    let expected_len = count
        .checked_mul(4)
        .and_then(|len| len.checked_add(4))
        .ok_or_else(|| invalid_exact_data("exact postings count overflows"))?;
    if expected_len != bytes.len() {
        return Err(invalid_exact_data(
            "exact postings count does not match payload length",
        ));
    }
    let mut previous_ref = None;
    for offset in (4..bytes.len()).step_by(4) {
        let series_ref = read_u32_at(bytes, offset);
        if previous_ref.is_some_and(|previous| previous >= series_ref) {
            return Err(invalid_exact_data(
                "exact postings refs are not strictly ordered and unique",
            ));
        }
        previous_ref = Some(series_ref);
    }
    let mut refs = Vec::new();
    refs.try_reserve_exact(count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("exact postings allocation failed: {error}"),
        )
    })?;
    refs.extend(
        (4..bytes.len())
            .step_by(4)
            .map(|offset| read_u32_at(bytes, offset)),
    );
    Ok(refs)
}

pub(super) fn decode_auxiliary_directory(
    bytes: &[u8],
    root: SegmentIndexV7Layout,
    symbol_count: Option<u32>,
) -> io::Result<AuxiliaryDirectory> {
    if bytes.len() < AUXILIARY_DIRECTORY_HEADER_LEN {
        return Err(invalid_auxiliary_data(
            "auxiliary directory is shorter than its fixed header",
        ));
    }
    if read_u32_at(bytes, 0) != AUXILIARY_DIRECTORY_MAGIC {
        return Err(invalid_auxiliary_data("auxiliary directory magic mismatch"));
    }
    if read_u16_at(bytes, 4) != AUXILIARY_DIRECTORY_VERSION {
        return Err(invalid_auxiliary_data(
            "auxiliary directory version mismatch",
        ));
    }
    if read_u16_at(bytes, 6) != 0 {
        return Err(invalid_auxiliary_data(
            "auxiliary directory flags are non-zero",
        ));
    }
    if read_u32_at(bytes, 8) != AUXILIARY_DIRECTORY_HEADER_LEN as u32 {
        return Err(invalid_auxiliary_data(
            "auxiliary directory header length is invalid",
        ));
    }
    if read_u32_at(bytes, 12) != AUXILIARY_DIRECTORY_RECORD_LEN as u32 {
        return Err(invalid_auxiliary_data(
            "auxiliary directory record length is invalid",
        ));
    }
    let entry_count = read_u64_at(bytes, 16);
    if entry_count != u64::from(root.auxiliary_entry_count) {
        return Err(invalid_auxiliary_data(
            "auxiliary directory entry count does not match the root",
        ));
    }
    if read_u64_at(bytes, 24) != AUXILIARY_DIRECTORY_HEADER_LEN as u64 {
        return Err(invalid_auxiliary_data(
            "auxiliary directory records offset is invalid",
        ));
    }
    let expected_records_len = entry_count
        .checked_mul(AUXILIARY_DIRECTORY_RECORD_LEN as u64)
        .ok_or_else(|| invalid_auxiliary_data("auxiliary directory record length overflows"))?;
    if read_u64_at(bytes, 32) != expected_records_len {
        return Err(invalid_auxiliary_data(
            "auxiliary directory records length is invalid",
        ));
    }
    if bytes[44..AUXILIARY_DIRECTORY_HEADER_LEN]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(invalid_auxiliary_data(
            "auxiliary directory reserved bytes are non-zero",
        ));
    }
    let expected_directory_len = (AUXILIARY_DIRECTORY_HEADER_LEN as u64)
        .checked_add(expected_records_len)
        .ok_or_else(|| invalid_auxiliary_data("auxiliary directory length overflows"))?;
    if expected_directory_len != root.auxiliary_directory.len
        || usize::try_from(expected_directory_len).ok() != Some(bytes.len())
    {
        return Err(invalid_auxiliary_data(
            "auxiliary directory length is inconsistent",
        ));
    }
    let stored_crc = read_u32_at(bytes, 40);
    let crc = crc32c_append(
        crc32c_append(crc32c_append(0, &bytes[..40]), &[0; 4]),
        &bytes[44..],
    );
    if crc != stored_crc {
        return Err(invalid_auxiliary_data("auxiliary directory CRC mismatch"));
    }

    let entry_count = usize::try_from(entry_count).map_err(|_| {
        invalid_auxiliary_data("auxiliary directory entry count exceeds platform usize")
    })?;
    let mut records = Vec::new();
    records.try_reserve_exact(entry_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("auxiliary directory record allocation failed: {error}"),
        )
    })?;
    let auxiliary_payloads_end = root
        .auxiliary_payloads
        .offset
        .checked_add(root.auxiliary_payloads.len)
        .ok_or_else(|| invalid_auxiliary_data("auxiliary payload root range overflows"))?;
    let mut previous_key = None;
    let mut fst_count = 0usize;
    for record_index in 0..entry_count {
        let offset = record_index
            .checked_mul(AUXILIARY_DIRECTORY_RECORD_LEN)
            .and_then(|offset| offset.checked_add(AUXILIARY_DIRECTORY_HEADER_LEN))
            .ok_or_else(|| invalid_auxiliary_data("auxiliary record offset overflows"))?;
        let record = bytes
            .get(offset..offset + AUXILIARY_DIRECTORY_RECORD_LEN)
            .ok_or_else(|| invalid_auxiliary_data("auxiliary directory record truncated"))?;
        let kind = read_u16_at(record, 0);
        if !matches!(
            kind,
            SEGMENT_INDEX_BLOB_LABEL_VALUE_FST | SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES
        ) {
            return Err(invalid_auxiliary_data(
                "auxiliary directory record kind is unsupported",
            ));
        }
        if read_u16_at(record, 2) != 0 {
            return Err(invalid_auxiliary_data(
                "auxiliary directory record flags are non-zero",
            ));
        }
        let label_name_sym = read_u32_at(record, 4);
        if symbol_count.is_some_and(|symbol_count| label_name_sym >= symbol_count) {
            return Err(invalid_auxiliary_data(
                "auxiliary label-name symbol exceeds the authoritative symbol count",
            ));
        }
        let key = (kind, label_name_sym);
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid_auxiliary_data(
                "auxiliary directory records are not strictly ordered and unique",
            ));
        }
        let payload = BlobLocator {
            offset: read_u64_at(record, 8),
            len: read_u64_at(record, 16),
        };
        if payload.len == 0 {
            return Err(invalid_auxiliary_data(
                "auxiliary directory record has a zero-length payload",
            ));
        }
        let payload_end = payload
            .offset
            .checked_add(payload.len)
            .ok_or_else(|| invalid_auxiliary_data("auxiliary payload range overflows"))?;
        if payload.offset < root.auxiliary_payloads.offset || payload_end > auxiliary_payloads_end {
            return Err(invalid_auxiliary_data(
                "auxiliary payload lies outside the auxiliary-payload region",
            ));
        }
        let time_range = LabelValueTimeRange {
            min_time_ms: read_u64_at(record, 24),
            max_time_ms: read_u64_at(record, 32),
        };
        if time_range.min_time_ms > time_range.max_time_ms {
            return Err(invalid_auxiliary_data(
                "auxiliary directory time range is reversed",
            ));
        }
        if kind == SEGMENT_INDEX_BLOB_LABEL_VALUE_FST {
            fst_count = fst_count
                .checked_add(1)
                .ok_or_else(|| invalid_auxiliary_data("auxiliary FST count overflows"))?;
        }
        records.push(AuxiliaryRecord {
            kind,
            label_name_sym,
            payload,
            time_range,
        });
        previous_key = Some(key);
    }
    let directory = AuxiliaryDirectory {
        records: records.into_boxed_slice(),
        fst_count,
    };
    for fst_record in &directory.records[..directory.fst_count] {
        match directory.record(
            SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
            fst_record.label_name_sym,
        ) {
            Some(time_record) if time_record.time_range != fst_record.time_range => {
                return Err(invalid_auxiliary_data(
                    "auxiliary FST and time-range summaries do not match",
                ));
            }
            None if fst_record.time_range
                != (LabelValueTimeRange {
                    min_time_ms: 0,
                    max_time_ms: u64::MAX,
                }) =>
            {
                return Err(invalid_auxiliary_data(
                    "auxiliary FST without time ranges has a noncanonical summary",
                ));
            }
            Some(_) | None => {}
        }
    }
    Ok(directory)
}

pub(super) fn validate_label_value_fst(bytes: &[u8]) -> io::Result<()> {
    let set = Set::new(bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid label value FST: {error}"),
        )
    })?;
    if set.is_empty() {
        return Err(invalid_auxiliary_data("label value FST contains no values"));
    }
    let mut stream = set.stream();
    while let Some(value) = stream.next() {
        std::str::from_utf8(value).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid utf8 FST value: {error}"),
            )
        })?;
    }
    Ok(())
}

/// Visits validated values without materializing an unbounded `Vec<String>`.
/// Returns `false` when the visitor stops the stream early.
pub(super) fn visit_label_value_fst(
    bytes: &[u8],
    prefix: Option<&str>,
    mut visitor: impl FnMut(&str) -> bool,
) -> io::Result<bool> {
    let set = Set::new(bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid label value FST: {error}"),
        )
    })?;
    if set.is_empty() {
        return Err(invalid_auxiliary_data("label value FST contains no values"));
    }
    let prefix = prefix.filter(|prefix| !prefix.is_empty());
    let mut stream = match prefix {
        Some(prefix) => set.range().ge(prefix).into_stream(),
        None => set.stream(),
    };
    while let Some(value) = stream.next() {
        if prefix.is_some_and(|prefix| !value.starts_with(prefix.as_bytes())) {
            break;
        }
        let value = std::str::from_utf8(value).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid utf8 FST value: {error}"),
            )
        })?;
        if !visitor(value) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn decode_label_value_time_ranges(
    bytes: &[u8],
    expected_summary: LabelValueTimeRange,
    symbol_count: Option<u32>,
) -> io::Result<Vec<(u32, LabelValueTimeRange)>> {
    let ranges = read_label_value_time_ranges_blob(bytes)?;
    if symbol_count.is_some_and(|symbol_count| {
        ranges
            .iter()
            .any(|(label_value_sym, _)| *label_value_sym >= symbol_count)
    }) {
        return Err(invalid_auxiliary_data(
            "label-value time-range symbol exceeds the authoritative symbol count",
        ));
    }
    let aggregate = ranges.iter().fold(
        LabelValueTimeRange {
            min_time_ms: u64::MAX,
            max_time_ms: 0,
        },
        |aggregate, (_, range)| LabelValueTimeRange {
            min_time_ms: aggregate.min_time_ms.min(range.min_time_ms),
            max_time_ms: aggregate.max_time_ms.max(range.max_time_ms),
        },
    );
    if aggregate != expected_summary {
        return Err(invalid_auxiliary_data(
            "label value time range payload does not match its directory summary",
        ));
    }
    Ok(ranges)
}

pub(super) fn decode_metric_series_range_directory(
    bytes: &[u8],
    num_series: u32,
    symbol_count: u32,
) -> io::Result<MetricSeriesRangeDirectory> {
    let mut groups = Vec::new();
    walk_metric_series_ranges_blob(
        bytes,
        Some(MetricSeriesRangeBlobBounds {
            num_series,
            symbol_count,
        }),
        |event| {
            match event {
                MetricSeriesRangeBlobEvent::Header { metric_count } => {
                    groups.try_reserve_exact(metric_count).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            format!("metric series range directory allocation failed: {error}"),
                        )
                    })?;
                }
                MetricSeriesRangeBlobEvent::Group {
                    metric_sym,
                    range_count,
                    ranges_offset,
                } => groups.push(MetricSeriesRangeGroupDescriptor {
                    metric_sym,
                    ranges_offset,
                    range_count,
                }),
                MetricSeriesRangeBlobEvent::Range { .. } => {}
            }
            Ok(())
        },
    )?;
    Ok(MetricSeriesRangeDirectory { groups })
}

pub(super) fn visit_metric_series_ranges(
    bytes: &[u8],
    directory: &MetricSeriesRangeDirectory,
    metric_sym: u32,
    mut visitor: impl FnMut(MetricSeriesRange) -> bool,
) -> io::Result<bool> {
    let Some(group) = directory.group(metric_sym) else {
        return Ok(true);
    };
    for range_index in 0..group.range_count {
        let offset = range_index
            .checked_mul(super::super::METRIC_SERIES_RANGE_RECORD_LEN)
            .and_then(|offset| offset.checked_add(group.ranges_offset))
            .ok_or_else(|| invalid_metric_range_data("metric range offset overflows"))?;
        let end = offset
            .checked_add(super::super::METRIC_SERIES_RANGE_RECORD_LEN)
            .ok_or_else(|| invalid_metric_range_data("metric range end overflows"))?;
        let record = bytes
            .get(offset..end)
            .ok_or_else(|| invalid_metric_range_data("validated metric range is truncated"))?;
        let range = MetricSeriesRange {
            start_series_ref: read_u32_at(record, 0),
            series_count: read_u32_at(record, 4),
            kind_mask: read_u16_at(record, 8),
            min_time_ms: read_u64_at(record, 12),
            max_time_ms: read_u64_at(record, 20),
        };
        if !visitor(range) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn invalid_exact_data(description: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, description.into())
}

fn invalid_auxiliary_data(description: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, description.into())
}

fn invalid_metric_range_data(description: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, description.into())
}
