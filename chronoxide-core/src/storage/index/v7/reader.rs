use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crc32c::{crc32c, crc32c_append};

use super::super::{
    ExactPostingsMetadata, LabelValueTimeRange, MetricSeriesRange, MetricSeriesRangeIndex,
    ROUTING_INDEX_BUCKET_LEN, ROUTING_INDEX_HEADER_LEN, RoutingBucketRecord, RoutingIndexHeader,
    RoutingLookupResult, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, SegmentIndexReadAt, SegmentRoutingIndex,
    read_fst_values_with_prefix, read_label_value_time_ranges_blob, read_metric_series_ranges_blob,
    routing_key_bytes, routing_key_hash, validate_routing_bucket_key,
};
use super::{
    AUXILIARY_DIRECTORY_HEADER_LEN, AUXILIARY_DIRECTORY_MAGIC, AUXILIARY_DIRECTORY_RECORD_LEN,
    AUXILIARY_DIRECTORY_VERSION, BlobLocator, EXACT_DIRECTORY_HEADER_LEN, EXACT_DIRECTORY_MAGIC,
    EXACT_DIRECTORY_VERSION, EXACT_PAGE_DESCRIPTOR_LEN, EXACT_PAGE_HEADER_LEN, EXACT_PAGE_LEN,
    EXACT_PAGE_MAGIC, EXACT_PAGE_VERSION, EXACT_RECORD_LEN, EXACT_RECORDS_PER_PAGE,
    SEGMENT_INDEX_V7_HEADER_LEN, SEGMENT_INDEX_V7_TRAILER_LEN, SegmentIndexV7Layout,
    decode_segment_indexes_v7_root, read_u16_at, read_u32_at, read_u64_at,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct SegmentIndexV7ReadCount {
    pub(super) calls: u64,
    pub(super) bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct SegmentIndexV7ReadStats {
    pub(super) root: SegmentIndexV7ReadCount,
    pub(super) routing: SegmentIndexV7ReadCount,
    pub(super) exact_directory: SegmentIndexV7ReadCount,
    pub(super) exact_page: SegmentIndexV7ReadCount,
    pub(super) auxiliary_directory: SegmentIndexV7ReadCount,
    pub(super) payload: SegmentIndexV7ReadCount,
}

impl SegmentIndexV7ReadStats {
    pub(super) fn total_calls(self) -> u64 {
        self.root
            .calls
            .saturating_add(self.routing.calls)
            .saturating_add(self.exact_directory.calls)
            .saturating_add(self.exact_page.calls)
            .saturating_add(self.auxiliary_directory.calls)
            .saturating_add(self.payload.calls)
    }

    pub(super) fn total_bytes(self) -> u64 {
        self.root
            .bytes
            .saturating_add(self.routing.bytes)
            .saturating_add(self.exact_directory.bytes)
            .saturating_add(self.exact_page.bytes)
            .saturating_add(self.auxiliary_directory.bytes)
            .saturating_add(self.payload.bytes)
    }
}

#[derive(Debug, Default)]
struct AtomicReadCount {
    calls: AtomicU64,
    bytes: AtomicU64,
}

impl AtomicReadCount {
    fn record(&self, bytes: u64) {
        let _ = self
            .calls
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |calls| {
                Some(calls.saturating_add(1))
            });
        let _ = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(bytes))
            });
    }

    fn snapshot(&self) -> SegmentIndexV7ReadCount {
        SegmentIndexV7ReadCount {
            calls: self.calls.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum SegmentIndexV7ReadCategory {
    Root,
    Routing,
    ExactDirectory,
    ExactPage,
    AuxiliaryDirectory,
    Payload,
}

#[derive(Debug, Default)]
struct SegmentIndexV7ReadCounters {
    root: AtomicReadCount,
    routing: AtomicReadCount,
    exact_directory: AtomicReadCount,
    exact_page: AtomicReadCount,
    auxiliary_directory: AtomicReadCount,
    payload: AtomicReadCount,
}

impl SegmentIndexV7ReadCounters {
    fn record(&self, category: SegmentIndexV7ReadCategory, bytes: u64) {
        let counter = match category {
            SegmentIndexV7ReadCategory::Root => &self.root,
            SegmentIndexV7ReadCategory::Routing => &self.routing,
            SegmentIndexV7ReadCategory::ExactDirectory => &self.exact_directory,
            SegmentIndexV7ReadCategory::ExactPage => &self.exact_page,
            SegmentIndexV7ReadCategory::AuxiliaryDirectory => &self.auxiliary_directory,
            SegmentIndexV7ReadCategory::Payload => &self.payload,
        };
        counter.record(bytes);
    }

    fn snapshot(&self) -> SegmentIndexV7ReadStats {
        SegmentIndexV7ReadStats {
            root: self.root.snapshot(),
            routing: self.routing.snapshot(),
            exact_directory: self.exact_directory.snapshot(),
            exact_page: self.exact_page.snapshot(),
            auxiliary_directory: self.auxiliary_directory.snapshot(),
            payload: self.payload.snapshot(),
        }
    }
}

struct SegmentIndexV7ReaderState {
    root: SegmentIndexV7Layout,
    exact_directory: OnceLock<Result<ExactDirectory, CachedIoError>>,
    auxiliary_directory: OnceLock<Result<AuxiliaryDirectory, CachedIoError>>,
}

#[derive(Debug, Clone)]
struct CachedIoError {
    kind: io::ErrorKind,
    message: String,
}

impl CachedIoError {
    fn from_error(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn to_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

#[derive(Debug)]
struct ExactDirectory {
    descriptors: Vec<ExactPageDescriptor>,
}

#[derive(Debug, Clone, Copy)]
struct ExactPageDescriptor {
    first_key: (u32, u32),
    last_key: (u32, u32),
    record_count: u32,
    page_crc32c: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExactPostingsSelection {
    metadata: ExactPostingsMetadata,
    postings: BlobLocator,
}

#[derive(Debug)]
struct AuxiliaryDirectory {
    records: Box<[AuxiliaryRecord]>,
    fst_count: usize,
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

#[derive(Debug, Clone, Copy)]
struct AuxiliaryRecord {
    kind: u16,
    label_name_sym: u32,
    payload: BlobLocator,
    time_range: LabelValueTimeRange,
}

pub(super) struct SegmentIndexV7Reader<R>
where
    R: SegmentIndexReadAt,
{
    source: Arc<R>,
    state: Arc<SegmentIndexV7ReaderState>,
    counters: SegmentIndexV7ReadCounters,
}

impl<R> SegmentIndexV7Reader<R>
where
    R: SegmentIndexReadAt,
{
    pub(super) fn open(source: R) -> io::Result<Self> {
        let file_len = source.len()?;
        if file_len < (SEGMENT_INDEX_V7_HEADER_LEN + SEGMENT_INDEX_V7_TRAILER_LEN) as u64 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment index v7 source is shorter than its fixed roots",
            ));
        }

        let source = Arc::new(source);
        let counters = SegmentIndexV7ReadCounters::default();
        let mut header = [0u8; SEGMENT_INDEX_V7_HEADER_LEN];
        read_exact_at_counted(
            source.as_ref(),
            &counters,
            SegmentIndexV7ReadCategory::Root,
            0,
            &mut header,
        )?;
        let trailer_offset = file_len - SEGMENT_INDEX_V7_TRAILER_LEN as u64;
        let mut trailer = [0u8; SEGMENT_INDEX_V7_TRAILER_LEN];
        read_exact_at_counted(
            source.as_ref(),
            &counters,
            SegmentIndexV7ReadCategory::Root,
            trailer_offset,
            &mut trailer,
        )?;
        let root = decode_segment_indexes_v7_root(file_len, &header, &trailer)?;

        Ok(Self {
            source,
            state: Arc::new(SegmentIndexV7ReaderState {
                root,
                exact_directory: OnceLock::new(),
                auxiliary_directory: OnceLock::new(),
            }),
            counters,
        })
    }

    pub(super) fn try_clone_reader(&self) -> io::Result<Self> {
        Ok(Self {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            counters: SegmentIndexV7ReadCounters::default(),
        })
    }

    pub(super) fn stats(&self) -> SegmentIndexV7ReadStats {
        self.counters.snapshot()
    }

    pub(super) fn routing_exact_postings_metadata(
        &self,
        label_name: &str,
        label_value: &str,
    ) -> io::Result<RoutingLookupResult> {
        let locator = self.state.root.routing;
        if locator == BlobLocator::default() {
            return Ok(RoutingLookupResult {
                index_present: false,
                metadata: None,
                bytes_read: 0,
            });
        }

        let mut bytes_read = 0u64;
        let header_bytes = self.read_blob_range(
            locator,
            0,
            ROUTING_INDEX_HEADER_LEN as u64,
            SegmentIndexV7ReadCategory::Routing,
        )?;
        bytes_read = bytes_read.saturating_add(header_bytes.len() as u64);
        let header = RoutingIndexHeader::decode(&header_bytes, locator.len)?;
        let key = routing_key_bytes(label_name, label_value)?;
        let key_hash = routing_key_hash(&key);
        let mut bucket_index = (key_hash as u32) & (header.bucket_count - 1);

        for _ in 0..header.bucket_count {
            let bucket_offset = header.bucket_offset(bucket_index)?;
            let bucket_bytes = self.read_blob_range(
                locator,
                bucket_offset,
                ROUTING_INDEX_BUCKET_LEN as u64,
                SegmentIndexV7ReadCategory::Routing,
            )?;
            bytes_read = bytes_read.saturating_add(bucket_bytes.len() as u64);
            let bucket = RoutingBucketRecord::decode(&bucket_bytes)?;
            let Some(key_range) = bucket.validate_touched(header)? else {
                return Ok(RoutingLookupResult {
                    index_present: true,
                    metadata: None,
                    bytes_read,
                });
            };
            let stored_key = self.read_blob_range(
                locator,
                key_range.offset,
                key_range.len as u64,
                SegmentIndexV7ReadCategory::Routing,
            )?;
            bytes_read = bytes_read.saturating_add(stored_key.len() as u64);
            validate_routing_bucket_key(bucket, &stored_key)?;
            if bucket.hash == key_hash && stored_key == key {
                return Ok(RoutingLookupResult {
                    index_present: true,
                    metadata: Some(bucket.metadata),
                    bytes_read,
                });
            }
            bucket_index = (bucket_index + 1) & (header.bucket_count - 1);
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "routing index probe exhausted without empty bucket",
        ))
    }

    pub(super) fn routing_index(&self) -> io::Result<Option<SegmentRoutingIndex>> {
        let locator = self.state.root.routing;
        if locator == BlobLocator::default() {
            return Ok(None);
        }
        let bytes = self.read_blob(locator, SegmentIndexV7ReadCategory::Routing)?;
        Ok(Some(SegmentRoutingIndex::decode(&bytes)?))
    }

    pub(super) fn metric_series_ranges(
        &self,
        metric_sym: u32,
    ) -> io::Result<Vec<MetricSeriesRange>> {
        let index = self.metric_series_range_index()?;
        Ok(index.ranges(metric_sym).to_vec())
    }

    pub(super) fn metric_series_range_index(&self) -> io::Result<MetricSeriesRangeIndex> {
        let bytes = self.read_blob(self.state.root.metric, SegmentIndexV7ReadCategory::Payload)?;
        read_metric_series_ranges_blob(&bytes)
    }

    pub(super) fn exact_postings_metadata(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<ExactPostingsMetadata>> {
        Ok(self
            .exact_postings_selection(label_name_sym, label_value_sym)?
            .map(|selection| selection.metadata))
    }

    pub(super) fn exact_postings(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<Vec<u32>>> {
        let Some(selection) = self.exact_postings_selection(label_name_sym, label_value_sym)?
        else {
            return Ok(None);
        };
        Ok(Some(self.read_exact_postings_selection(selection)?))
    }

    fn exact_postings_selection(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<ExactPostingsSelection>> {
        let directory = self.exact_directory()?;
        let key = (label_name_sym, label_value_sym);
        let descriptor_index = directory
            .descriptors
            .partition_point(|descriptor| descriptor.last_key < key);
        let Some(descriptor) = directory.descriptors.get(descriptor_index).copied() else {
            return Ok(None);
        };
        if key < descriptor.first_key {
            return Ok(None);
        }
        self.read_exact_page_selection(descriptor_index, descriptor, key)
    }

    fn exact_directory(&self) -> io::Result<&ExactDirectory> {
        match self.state.exact_directory.get_or_init(|| {
            self.load_exact_directory()
                .map_err(CachedIoError::from_error)
        }) {
            Ok(directory) => Ok(directory),
            Err(error) => Err(error.to_error()),
        }
    }

    fn load_exact_directory(&self) -> io::Result<ExactDirectory> {
        let bytes = self.read_blob(
            self.state.root.exact_directory,
            SegmentIndexV7ReadCategory::ExactDirectory,
        )?;
        if bytes.len() < EXACT_DIRECTORY_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "exact directory is shorter than its fixed header",
            ));
        }
        if read_u32_at(&bytes, 0) != EXACT_DIRECTORY_MAGIC {
            return Err(invalid_exact_data("exact directory magic mismatch"));
        }
        if read_u16_at(&bytes, 4) != EXACT_DIRECTORY_VERSION {
            return Err(invalid_exact_data("exact directory version mismatch"));
        }
        if read_u16_at(&bytes, 6) != 0 {
            return Err(invalid_exact_data("exact directory flags are non-zero"));
        }
        if read_u32_at(&bytes, 8) != EXACT_DIRECTORY_HEADER_LEN as u32 {
            return Err(invalid_exact_data(
                "exact directory header length is invalid",
            ));
        }
        if read_u32_at(&bytes, 12) != EXACT_PAGE_DESCRIPTOR_LEN as u32 {
            return Err(invalid_exact_data(
                "exact directory descriptor length is invalid",
            ));
        }
        if read_u32_at(&bytes, 16) != EXACT_PAGE_LEN as u32 {
            return Err(invalid_exact_data("exact directory page length is invalid"));
        }
        if read_u32_at(&bytes, 20) != EXACT_RECORD_LEN as u32 {
            return Err(invalid_exact_data(
                "exact directory record length is invalid",
            ));
        }
        if read_u64_at(&bytes, 24) != self.state.root.exact_entry_count {
            return Err(invalid_exact_data(
                "exact directory entry count does not match the root",
            ));
        }
        if read_u32_at(&bytes, 32) != self.state.root.exact_page_count {
            return Err(invalid_exact_data(
                "exact directory page count does not match the root",
            ));
        }
        if read_u32_at(&bytes, 36) != EXACT_RECORDS_PER_PAGE as u32 {
            return Err(invalid_exact_data(
                "exact directory records-per-page value is invalid",
            ));
        }
        if read_u64_at(&bytes, 40) != EXACT_DIRECTORY_HEADER_LEN as u64 {
            return Err(invalid_exact_data(
                "exact directory descriptors offset is invalid",
            ));
        }
        let expected_descriptors_len = u64::from(self.state.root.exact_page_count)
            .checked_mul(EXACT_PAGE_DESCRIPTOR_LEN as u64)
            .ok_or_else(|| invalid_exact_data("exact directory descriptor length overflows"))?;
        if read_u64_at(&bytes, 48) != expected_descriptors_len {
            return Err(invalid_exact_data(
                "exact directory descriptors length is invalid",
            ));
        }
        if read_u32_at(&bytes, 60) != 0 {
            return Err(invalid_exact_data(
                "exact directory reserved field is non-zero",
            ));
        }
        let expected_directory_len = (EXACT_DIRECTORY_HEADER_LEN as u64)
            .checked_add(expected_descriptors_len)
            .ok_or_else(|| invalid_exact_data("exact directory length overflows"))?;
        if expected_directory_len != self.state.root.exact_directory.len
            || usize::try_from(expected_directory_len).ok() != Some(bytes.len())
        {
            return Err(invalid_exact_data("exact directory length is inconsistent"));
        }
        let stored_crc = read_u32_at(&bytes, 56);
        let crc = crc32c_append(
            crc32c_append(crc32c_append(0, &bytes[..56]), &[0; 4]),
            &bytes[60..],
        );
        if crc != stored_crc {
            return Err(invalid_exact_data("exact directory CRC mismatch"));
        }
        let page_count = usize::try_from(self.state.root.exact_page_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "exact directory page count exceeds platform usize",
            )
        })?;
        let mut descriptors = Vec::new();
        descriptors
            .try_reserve_exact(page_count)
            .map_err(|_| io::Error::other("exact directory descriptor allocation failed"))?;
        let mut previous_last_key = None;
        let mut decoded_entry_count = 0u64;
        for page_index in 0..page_count {
            let offset = EXACT_DIRECTORY_HEADER_LEN + page_index * EXACT_PAGE_DESCRIPTOR_LEN;
            let descriptor = bytes
                .get(offset..offset + EXACT_PAGE_DESCRIPTOR_LEN)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "exact page descriptor truncated",
                    )
                })?;
            let first_key = (read_u32_at(descriptor, 0), read_u32_at(descriptor, 4));
            let last_key = (read_u32_at(descriptor, 8), read_u32_at(descriptor, 12));
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
            let remaining_entries = self
                .state
                .root
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
            if relative_page_end > self.state.root.exact_pages.len {
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
        if decoded_entry_count != self.state.root.exact_entry_count {
            return Err(invalid_exact_data(
                "exact descriptor counts do not match the root entry count",
            ));
        }
        Ok(ExactDirectory { descriptors })
    }

    fn read_exact_page_selection(
        &self,
        page_index: usize,
        descriptor: ExactPageDescriptor,
        key: (u32, u32),
    ) -> io::Result<Option<ExactPostingsSelection>> {
        let page_offset = u64::try_from(page_index)
            .ok()
            .and_then(|index| index.checked_mul(EXACT_PAGE_LEN as u64))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "exact page offset overflows")
            })?;
        let mut page = [0u8; EXACT_PAGE_LEN];
        self.read_blob_range_into(
            self.state.root.exact_pages,
            page_offset,
            &mut page,
            SegmentIndexV7ReadCategory::ExactPage,
        )?;
        if crc32c(&page) != descriptor.page_crc32c {
            return Err(invalid_exact_data("exact page CRC mismatch"));
        }
        if read_u32_at(&page, 0) != EXACT_PAGE_MAGIC {
            return Err(invalid_exact_data("exact page magic mismatch"));
        }
        if read_u16_at(&page, 4) != EXACT_PAGE_VERSION {
            return Err(invalid_exact_data("exact page version mismatch"));
        }
        if read_u16_at(&page, 6) != 0 {
            return Err(invalid_exact_data("exact page flags are non-zero"));
        }
        let expected_page_index = u32::try_from(page_index)
            .map_err(|_| invalid_exact_data("exact page index exceeds u32"))?;
        if read_u32_at(&page, 8) != expected_page_index {
            return Err(invalid_exact_data("exact page index is invalid"));
        }
        if read_u32_at(&page, 12) != descriptor.record_count {
            return Err(invalid_exact_data(
                "exact page record count does not match its descriptor",
            ));
        }
        let record_count = usize::try_from(descriptor.record_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "exact page record count exceeds platform usize",
            )
        })?;
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
        let exact_postings_end = self
            .state
            .root
            .exact_postings
            .offset
            .checked_add(self.state.root.exact_postings.len)
            .ok_or_else(|| invalid_exact_data("exact postings root range overflows"))?;
        let mut previous_key = None;
        let mut first_key = None;
        let mut last_key = None;
        let mut selected = None;
        for record_index in 0..record_count {
            let offset = EXACT_PAGE_HEADER_LEN + record_index * EXACT_RECORD_LEN;
            let record = page.get(offset..offset + EXACT_RECORD_LEN).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "exact page record truncated")
            })?;
            let record_key = (read_u32_at(record, 0), read_u32_at(record, 4));
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
            if postings.len < 4 || (postings.len - 4) % 4 != 0 {
                return Err(invalid_exact_data(
                    "exact postings locator length is not a canonical payload length",
                ));
            }
            let postings_end = postings
                .offset
                .checked_add(postings.len)
                .ok_or_else(|| invalid_exact_data("exact postings locator overflows"))?;
            if postings.offset < self.state.root.exact_postings.offset
                || postings_end > exact_postings_end
            {
                return Err(invalid_exact_data(
                    "exact postings locator lies outside the postings region",
                ));
            }
            let min_time_ms = read_u64_at(record, 24);
            let max_time_ms = read_u64_at(record, 32);
            if min_time_ms > max_time_ms {
                return Err(invalid_exact_data("exact page time range is reversed"));
            }
            if record_key == key {
                selected = Some(ExactPostingsSelection {
                    metadata: ExactPostingsMetadata {
                        byte_len: postings.len,
                        time_range: LabelValueTimeRange {
                            min_time_ms,
                            max_time_ms,
                        },
                    },
                    postings,
                });
            }
        }
        if first_key != Some(descriptor.first_key) || last_key != Some(descriptor.last_key) {
            return Err(invalid_exact_data(
                "exact page key bounds do not match its descriptor",
            ));
        }
        Ok(selected)
    }

    fn read_exact_postings_selection(
        &self,
        selection: ExactPostingsSelection,
    ) -> io::Result<Vec<u32>> {
        let bytes = self.read_blob(selection.postings, SegmentIndexV7ReadCategory::Payload)?;
        if bytes.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "exact postings payload is shorter than its count",
            ));
        }
        let count = read_u32_at(&bytes, 0) as usize;
        let expected_len = count
            .checked_mul(4)
            .and_then(|len| len.checked_add(4))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "exact postings count overflows")
            })?;
        if expected_len != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "exact postings count does not match payload length",
            ));
        }
        let mut refs = Vec::new();
        refs.try_reserve_exact(count)
            .map_err(|_| io::Error::other("exact postings allocation failed"))?;
        let mut previous_ref = None;
        for offset in (4..bytes.len()).step_by(4) {
            let series_ref = read_u32_at(&bytes, offset);
            if previous_ref.is_some_and(|previous| previous >= series_ref) {
                return Err(invalid_exact_data(
                    "exact postings refs are not strictly ordered and unique",
                ));
            }
            refs.push(series_ref);
            previous_ref = Some(series_ref);
        }
        Ok(refs)
    }

    pub(super) fn label_name_symbols(&self) -> io::Result<Vec<u32>> {
        let directory = self.auxiliary_directory()?;
        let mut symbols = Vec::new();
        symbols
            .try_reserve_exact(directory.fst_count)
            .map_err(|_| io::Error::other("label name symbol allocation failed"))?;
        symbols.extend(
            directory.records[..directory.fst_count]
                .iter()
                .map(|record| record.label_name_sym),
        );
        Ok(symbols)
    }

    pub(super) fn has_label_values(&self) -> io::Result<bool> {
        Ok(self.auxiliary_directory()?.fst_count != 0)
    }

    pub(super) fn label_time_range(
        &self,
        label_name_sym: u32,
    ) -> io::Result<Option<LabelValueTimeRange>> {
        Ok(self
            .auxiliary_directory()?
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, label_name_sym)
            .map(|record| record.time_range))
    }

    pub(super) fn label_values(&self, label_name_sym: u32) -> io::Result<Vec<String>> {
        self.label_values_with_prefix(label_name_sym, None)
    }

    pub(super) fn label_values_with_prefix(
        &self,
        label_name_sym: u32,
        prefix: Option<&str>,
    ) -> io::Result<Vec<String>> {
        let Some(record) = self
            .auxiliary_directory()?
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, label_name_sym)
        else {
            return Ok(Vec::new());
        };
        let bytes = self.read_blob(record.payload, SegmentIndexV7ReadCategory::Payload)?;
        read_fst_values_with_prefix(&bytes, prefix)
    }

    pub(super) fn label_value_time_range(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<LabelValueTimeRange>> {
        let Some(ranges) = self.label_value_time_ranges(label_name_sym)? else {
            return Ok(None);
        };
        Ok(ranges
            .binary_search_by_key(&label_value_sym, |(value_sym, _)| *value_sym)
            .ok()
            .map(|index| ranges[index].1))
    }

    pub(super) fn label_value_time_ranges(
        &self,
        label_name_sym: u32,
    ) -> io::Result<Option<Vec<(u32, LabelValueTimeRange)>>> {
        let Some(record) = self
            .auxiliary_directory()?
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, label_name_sym)
        else {
            return Ok(None);
        };
        let bytes = self.read_blob(record.payload, SegmentIndexV7ReadCategory::Payload)?;
        let ranges = read_label_value_time_ranges_blob(&bytes)?;
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
        if aggregate != record.time_range {
            return Err(invalid_auxiliary_data(
                "label value time range payload does not match its directory summary",
            ));
        }
        Ok(Some(ranges))
    }

    fn auxiliary_directory(&self) -> io::Result<&AuxiliaryDirectory> {
        match self.state.auxiliary_directory.get_or_init(|| {
            self.load_auxiliary_directory()
                .map_err(CachedIoError::from_error)
        }) {
            Ok(directory) => Ok(directory),
            Err(error) => Err(error.to_error()),
        }
    }

    fn load_auxiliary_directory(&self) -> io::Result<AuxiliaryDirectory> {
        let bytes = self.read_blob(
            self.state.root.auxiliary_directory,
            SegmentIndexV7ReadCategory::AuxiliaryDirectory,
        )?;
        if bytes.len() < AUXILIARY_DIRECTORY_HEADER_LEN {
            return Err(invalid_auxiliary_data(
                "auxiliary directory is shorter than its fixed header",
            ));
        }
        if read_u32_at(&bytes, 0) != AUXILIARY_DIRECTORY_MAGIC {
            return Err(invalid_auxiliary_data("auxiliary directory magic mismatch"));
        }
        if read_u16_at(&bytes, 4) != AUXILIARY_DIRECTORY_VERSION {
            return Err(invalid_auxiliary_data(
                "auxiliary directory version mismatch",
            ));
        }
        if read_u16_at(&bytes, 6) != 0 {
            return Err(invalid_auxiliary_data(
                "auxiliary directory flags are non-zero",
            ));
        }
        if read_u32_at(&bytes, 8) != AUXILIARY_DIRECTORY_HEADER_LEN as u32 {
            return Err(invalid_auxiliary_data(
                "auxiliary directory header length is invalid",
            ));
        }
        if read_u32_at(&bytes, 12) != AUXILIARY_DIRECTORY_RECORD_LEN as u32 {
            return Err(invalid_auxiliary_data(
                "auxiliary directory record length is invalid",
            ));
        }
        let entry_count = read_u64_at(&bytes, 16);
        if entry_count != u64::from(self.state.root.auxiliary_entry_count) {
            return Err(invalid_auxiliary_data(
                "auxiliary directory entry count does not match the root",
            ));
        }
        if read_u64_at(&bytes, 24) != AUXILIARY_DIRECTORY_HEADER_LEN as u64 {
            return Err(invalid_auxiliary_data(
                "auxiliary directory records offset is invalid",
            ));
        }
        let expected_records_len = entry_count
            .checked_mul(AUXILIARY_DIRECTORY_RECORD_LEN as u64)
            .ok_or_else(|| invalid_auxiliary_data("auxiliary directory record length overflows"))?;
        if read_u64_at(&bytes, 32) != expected_records_len {
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
        if expected_directory_len != self.state.root.auxiliary_directory.len
            || usize::try_from(expected_directory_len).ok() != Some(bytes.len())
        {
            return Err(invalid_auxiliary_data(
                "auxiliary directory length is inconsistent",
            ));
        }
        let stored_crc = read_u32_at(&bytes, 40);
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
        records
            .try_reserve_exact(entry_count)
            .map_err(|_| io::Error::other("auxiliary directory record allocation failed"))?;
        let auxiliary_payloads_end = self
            .state
            .root
            .auxiliary_payloads
            .offset
            .checked_add(self.state.root.auxiliary_payloads.len)
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
            if payload.offset < self.state.root.auxiliary_payloads.offset
                || payload_end > auxiliary_payloads_end
            {
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

    fn read_blob(
        &self,
        locator: BlobLocator,
        category: SegmentIndexV7ReadCategory,
    ) -> io::Result<Vec<u8>> {
        self.read_blob_range(locator, 0, locator.len, category)
    }

    fn read_blob_range(
        &self,
        locator: BlobLocator,
        relative_offset: u64,
        len: u64,
        category: SegmentIndexV7ReadCategory,
    ) -> io::Result<Vec<u8>> {
        let relative_end = relative_offset.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "segment index range overflow")
        })?;
        if relative_end > locator.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment index range exceeds its root locator",
            ));
        }
        let len = usize::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment index read length exceeds platform usize",
            )
        })?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "segment index read allocation is too large",
            )
        })?;
        bytes.resize(len, 0);
        self.read_blob_range_into(locator, relative_offset, &mut bytes, category)?;
        Ok(bytes)
    }

    fn read_blob_range_into(
        &self,
        locator: BlobLocator,
        relative_offset: u64,
        bytes: &mut [u8],
        category: SegmentIndexV7ReadCategory,
    ) -> io::Result<()> {
        let len = u64::try_from(bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment index read length exceeds u64",
            )
        })?;
        let relative_end = relative_offset.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "segment index range overflow")
        })?;
        if relative_end > locator.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment index range exceeds its root locator",
            ));
        }
        let file_offset = locator.offset.checked_add(relative_offset).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "segment index offset overflow")
        })?;
        read_exact_at_counted(
            self.source.as_ref(),
            &self.counters,
            category,
            file_offset,
            bytes,
        )
    }
}

fn invalid_exact_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_auxiliary_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_exact_at_counted(
    source: &impl SegmentIndexReadAt,
    counters: &SegmentIndexV7ReadCounters,
    category: SegmentIndexV7ReadCategory,
    offset: u64,
    destination: &mut [u8],
) -> io::Result<()> {
    source.read_exact_at(offset, destination)?;
    let bytes = u64::try_from(destination.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index positional read length exceeds u64",
        )
    })?;
    counters.record(category, bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{self, Write};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    use crc32c::crc32c;

    use super::*;
    use crate::labels::METRIC_NAME_LABEL;
    use crate::storage::index::{
        ExactPostingsIndex, ExactPostingsMetadata, LabelValueFstIndex, LabelValueTimeRangeIndex,
        MetricSeriesRange, MetricSeriesRangeIndex, SegmentIndexes, SegmentRoutingIndex,
    };
    use crate::storage::series::{SERIES_KIND_FLOAT, SegmentSymbols, SeriesEntry};

    const LABEL_NAME: &str = METRIC_NAME_LABEL;
    const LABEL_VALUE: &str = "request_duration_seconds";
    const THREAD_COUNT: usize = 16;
    const THREAD_ITERATIONS: usize = 40;

    struct CountingSourceState {
        bytes: Vec<u8>,
        len_calls: AtomicU64,
        reads: Mutex<Vec<(u64, usize)>>,
        fail_offset: Option<u64>,
    }

    #[derive(Clone)]
    struct CountingSource {
        state: Arc<CountingSourceState>,
    }

    impl CountingSource {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                state: Arc::new(CountingSourceState {
                    bytes,
                    len_calls: AtomicU64::new(0),
                    reads: Mutex::new(Vec::new()),
                    fail_offset: None,
                }),
            }
        }

        fn failing_at(bytes: Vec<u8>, fail_offset: u64) -> Self {
            Self {
                state: Arc::new(CountingSourceState {
                    bytes,
                    len_calls: AtomicU64::new(0),
                    reads: Mutex::new(Vec::new()),
                    fail_offset: Some(fail_offset),
                }),
            }
        }

        fn len_calls(&self) -> u64 {
            self.state.len_calls.load(Ordering::Relaxed)
        }

        fn reads(&self) -> Vec<(u64, usize)> {
            self.state.reads.lock().unwrap().clone()
        }
    }

    impl SegmentIndexReadAt for CountingSource {
        fn len(&self) -> io::Result<u64> {
            self.state.len_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.state.bytes.len() as u64)
        }

        fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
            self.state
                .reads
                .lock()
                .unwrap()
                .push((offset, destination.len()));
            if self.state.fail_offset == Some(offset) {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "injected short positional read",
                ));
            }
            let start = usize::try_from(offset).map_err(|_| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "offset exceeds usize")
            })?;
            let end = start
                .checked_add(destination.len())
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "read end overflow"))?;
            let source = self.state.bytes.get(start..end).ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "short positional read")
            })?;
            destination.copy_from_slice(source);
            Ok(())
        }
    }

    struct ReaderFixture {
        bytes: Vec<u8>,
        indexes: SegmentIndexes,
        metric_sym: u32,
        expected_metadata: ExactPostingsMetadata,
    }

    fn reader_fixture(include_routing: bool) -> ReaderFixture {
        let mut symbols = SegmentSymbols::default();
        let label_name_sym = symbols.intern(LABEL_NAME);
        let metric_sym = symbols.intern(LABEL_VALUE);
        let mut exact_postings = ExactPostingsIndex::default();
        exact_postings.insert(label_name_sym, metric_sym, 7);
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(label_name_sym, metric_sym, 1_000, 2_000);
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            metric_sym,
            MetricSeriesRange {
                start_series_ref: 7,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            },
        );
        let routing_index = include_routing
            .then(|| {
                SegmentRoutingIndex::from_indexes(
                    &symbols,
                    &exact_postings,
                    &label_value_time_ranges,
                )
            })
            .transpose()
            .unwrap();
        let expected_metadata = ExactPostingsMetadata {
            byte_len: 8,
            time_range: super::super::super::LabelValueTimeRange {
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            },
        };
        let indexes = SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges,
            metric_series_ranges,
            routing_index,
        };
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        ReaderFixture {
            bytes,
            indexes,
            metric_sym,
            expected_metadata,
        }
    }

    fn collision_reader_fixture() -> (ReaderFixture, String, ExactPostingsMetadata, usize) {
        let candidates = (0u32..256)
            .map(|index| format!("collision-{index:04}"))
            .collect::<Vec<_>>();
        let mut pair = None;
        'outer: for (left_index, left) in candidates.iter().enumerate() {
            let left_key = super::super::super::routing_key_bytes(LABEL_NAME, left).unwrap();
            for right in &candidates[left_index + 1..] {
                let right_key = super::super::super::routing_key_bytes(LABEL_NAME, right).unwrap();
                if super::super::super::routing_key_hash(&left_key) & 7
                    == super::super::super::routing_key_hash(&right_key) & 7
                {
                    pair = Some((left.clone(), right.clone()));
                    break 'outer;
                }
            }
        }
        let (first_value, target_value) = pair.unwrap();
        let mut symbols = SegmentSymbols::default();
        let label_name_sym = symbols.intern(LABEL_NAME);
        let first_sym = symbols.intern(&first_value);
        let target_sym = symbols.intern(&target_value);
        let mut exact_postings = ExactPostingsIndex::default();
        exact_postings.insert(label_name_sym, first_sym, 1);
        exact_postings.insert(label_name_sym, target_sym, 2);
        let mut ranges = LabelValueTimeRangeIndex::default();
        ranges.insert(label_name_sym, first_sym, 100, 199);
        ranges.insert(label_name_sym, target_sym, 200, 299);
        let routing_index =
            SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &ranges).unwrap();
        let expected_metadata = routing_index
            .exact_postings_metadata(LABEL_NAME, &target_value)
            .unwrap();
        let indexes = SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: Some(routing_index),
        };
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        let expected_bytes = 40
            + 2 * 40
            + (4 + LABEL_NAME.len() + first_value.len())
            + (4 + LABEL_NAME.len() + target_value.len());
        (
            ReaderFixture {
                bytes,
                indexes,
                metric_sym: target_sym,
                expected_metadata,
            },
            target_value,
            expected_metadata,
            expected_bytes,
        )
    }

    fn metric_validation_fixture() -> ReaderFixture {
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            10,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: 2,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 100,
                max_time_ms: 199,
            },
        );
        metric_series_ranges.insert_range(
            10,
            MetricSeriesRange {
                start_series_ref: 2,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 200,
                max_time_ms: 299,
            },
        );
        metric_series_ranges.insert_range(
            20,
            MetricSeriesRange {
                start_series_ref: 10,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 300,
                max_time_ms: 399,
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
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        ReaderFixture {
            bytes,
            indexes,
            metric_sym: 10,
            expected_metadata: ExactPostingsMetadata {
                byte_len: 0,
                time_range: super::super::super::LabelValueTimeRange {
                    min_time_ms: 0,
                    max_time_ms: 0,
                },
            },
        }
    }

    fn exact_reader_bytes(entries: &[(u32, Vec<u32>)]) -> Vec<u8> {
        let mut exact_postings = ExactPostingsIndex::default();
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        for (label_value_sym, refs) in entries {
            for series_ref in refs {
                exact_postings.insert(7, *label_value_sym, *series_ref);
            }
            label_value_time_ranges.insert(
                7,
                *label_value_sym,
                1_000 + u64::from(*label_value_sym),
                2_000 + u64::from(*label_value_sym),
            );
        }
        let indexes = SegmentIndexes {
            exact_postings,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        bytes
    }

    fn exact_boundary_entries(entry_count: u32) -> Vec<(u32, Vec<u32>)> {
        (0..entry_count)
            .map(|label_value_sym| (label_value_sym, vec![10_000 + label_value_sym]))
            .collect()
    }

    struct AuxiliaryReaderFixture {
        bytes: Vec<u8>,
        service_name_sym: u32,
        zone_name_sym: u32,
        api_value_sym: u32,
        worker_value_sym: u32,
        time_only_name_sym: u32,
        time_only_value_sym: u32,
    }

    fn auxiliary_reader_fixture() -> AuxiliaryReaderFixture {
        let mut symbols = SegmentSymbols::default();
        let service_name_sym = symbols.intern("service");
        let api_value_sym = symbols.intern("api");
        let worker_value_sym = symbols.intern("worker");
        let zone_name_sym = symbols.intern("zone");
        let east_value_sym = symbols.intern("east");
        let series = vec![
            SeriesEntry {
                series_id: 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![
                    (service_name_sym, api_value_sym),
                    (zone_name_sym, east_value_sym),
                ],
            },
            SeriesEntry {
                series_id: 2,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(service_name_sym, worker_value_sym)],
            },
        ];
        let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(service_name_sym, api_value_sym, 100, 199);
        label_value_time_ranges.insert(service_name_sym, worker_value_sym, 300, 399);
        label_value_time_ranges.insert(zone_name_sym, east_value_sym, 500, 599);
        let time_only_name_sym = 90;
        let time_only_value_sym = 91;
        label_value_time_ranges.insert(time_only_name_sym, time_only_value_sym, 700, 799);
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values,
            label_value_time_ranges,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        AuxiliaryReaderFixture {
            bytes,
            service_name_sym,
            zone_name_sym,
            api_value_sym,
            worker_value_sym,
            time_only_name_sym,
            time_only_value_sym,
        }
    }

    fn auxiliary_record_offset(bytes: &[u8], kind: u16, label_name_sym: u32) -> usize {
        let directory = locator(bytes, super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
        let start = directory.offset as usize;
        let entry_count = read_u64_at(bytes, start + 16) as usize;
        (0..entry_count)
            .map(|index| start + 64 + index * 40)
            .find(|offset| {
                u16::from_le_bytes(bytes[*offset..*offset + 2].try_into().unwrap()) == kind
                    && read_u32_at(bytes, *offset + 4) == label_name_sym
            })
            .unwrap()
    }

    fn auxiliary_payload_locator(bytes: &[u8], kind: u16, label_name_sym: u32) -> BlobLocator {
        let record = auxiliary_record_offset(bytes, kind, label_name_sym);
        BlobLocator {
            offset: read_u64_at(bytes, record + 8),
            len: read_u64_at(bytes, record + 16),
        }
    }

    fn refresh_auxiliary_directory_crc(bytes: &mut [u8]) {
        let directory = locator(bytes, super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
        let start = directory.offset as usize;
        let end = start + directory.len as usize;
        put_u32_at(bytes, start + 40, 0);
        let crc = crc32c(&bytes[start..end]);
        put_u32_at(bytes, start + 40, crc);
    }

    fn refresh_exact_directory_crc(bytes: &mut [u8]) {
        let directory = locator(bytes, super::super::TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
        let start = directory.offset as usize;
        let end = start + directory.len as usize;
        put_u32_at(bytes, start + 56, 0);
        let crc = crc32c(&bytes[start..end]);
        put_u32_at(bytes, start + 56, crc);
    }

    fn refresh_exact_page_crc(bytes: &mut [u8], page_index: usize) {
        let pages = locator(bytes, super::super::TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
        let page_start = pages.offset as usize + page_index * EXACT_PAGE_LEN;
        let page_end = page_start + EXACT_PAGE_LEN;
        let page_crc = crc32c(&bytes[page_start..page_end]);
        let directory = locator(bytes, super::super::TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
        let descriptor = directory.offset as usize
            + EXACT_DIRECTORY_HEADER_LEN
            + page_index * EXACT_PAGE_DESCRIPTOR_LEN;
        put_u32_at(bytes, descriptor + 24, page_crc);
        refresh_exact_directory_crc(bytes);
    }

    fn routing_occupied_bucket_offsets(bytes: &[u8]) -> Vec<usize> {
        let routing = locator(bytes, super::super::TRAILER_ROUTING_LOCATOR_OFFSET);
        let start = routing.offset as usize;
        let bucket_count = read_u32_at(bytes, start + 12);
        let buckets_offset = read_u64_at(bytes, start + 16) as usize;
        (0..bucket_count)
            .map(|bucket| start + buckets_offset + bucket as usize * 40)
            .filter(|bucket_offset| read_u32_at(bytes, bucket_offset + 12) != 0)
            .collect()
    }

    fn put_u16_at(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u32_at(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u64_at(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
    }

    fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    }

    fn trailer(bytes: &[u8]) -> &[u8] {
        &bytes[bytes.len() - super::super::SEGMENT_INDEX_V7_TRAILER_LEN..]
    }

    fn locator(bytes: &[u8], trailer_offset: usize) -> BlobLocator {
        let trailer = trailer(bytes);
        BlobLocator {
            offset: u64::from_le_bytes(
                trailer[trailer_offset..trailer_offset + 8]
                    .try_into()
                    .unwrap(),
            ),
            len: u64::from_le_bytes(
                trailer[trailer_offset + 8..trailer_offset + 16]
                    .try_into()
                    .unwrap(),
            ),
        }
    }

    fn assert_category_sums(stats: SegmentIndexV7ReadStats) {
        let expected_calls = stats.root.calls
            + stats.routing.calls
            + stats.exact_directory.calls
            + stats.exact_page.calls
            + stats.auxiliary_directory.calls
            + stats.payload.calls;
        let expected_bytes = stats.root.bytes
            + stats.routing.bytes
            + stats.exact_directory.bytes
            + stats.exact_page.bytes
            + stats.auxiliary_directory.bytes
            + stats.payload.bytes;
        assert_eq!(stats.total_calls(), expected_calls);
        assert_eq!(stats.total_bytes(), expected_bytes);
    }

    fn file_with_bytes(bytes: &[u8]) -> File {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn segment_index_v7_reader_open_reads_only_fixed_roots() {
        let fixture = reader_fixture(true);
        let source = CountingSource::new(fixture.bytes.clone());
        let probe = source.clone();

        let reader = SegmentIndexV7Reader::open(source).unwrap();

        assert_eq!(probe.len_calls(), 1);
        assert_eq!(
            probe.reads(),
            vec![(0, 16), (fixture.bytes.len() as u64 - 256, 256)]
        );
        let stats = reader.stats();
        assert_eq!(
            stats.root,
            SegmentIndexV7ReadCount {
                calls: 2,
                bytes: 272
            }
        );
        assert_eq!(stats.routing, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_directory, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_eq!(
            stats.auxiliary_directory,
            SegmentIndexV7ReadCount::default()
        );
        assert_eq!(stats.payload, SegmentIndexV7ReadCount::default());
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_open_rejects_short_lengths_and_reads() {
        let short = CountingSource::new(vec![0u8; 271]);
        let short_probe = short.clone();
        let error = SegmentIndexV7Reader::open(short).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(short_probe.len_calls(), 1);
        assert!(short_probe.reads().is_empty());

        let fixture = reader_fixture(true);
        for fail_offset in [0, fixture.bytes.len() as u64 - 256] {
            let source = CountingSource::failing_at(fixture.bytes.clone(), fail_offset);
            let error = SegmentIndexV7Reader::open(source).err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    #[test]
    fn segment_index_v7_reader_open_rejects_root_corruption() {
        let mut fixture = reader_fixture(true);
        fixture.bytes[0] ^= 0xff;
        let source = CountingSource::new(fixture.bytes);
        let probe = source.clone();

        let error = SegmentIndexV7Reader::open(source).err().unwrap();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(probe.reads().len(), 2);
    }

    #[test]
    fn segment_index_v7_reader_absent_routing_performs_no_additional_reads() {
        let fixture = reader_fixture(false);
        let source = CountingSource::new(fixture.bytes);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let reads_after_open = probe.reads();

        let lookup = reader
            .routing_exact_postings_metadata(LABEL_NAME, LABEL_VALUE)
            .unwrap();
        let full = reader.routing_index().unwrap();

        assert!(!lookup.index_present);
        assert_eq!(lookup.metadata, None);
        assert_eq!(lookup.bytes_read, 0);
        assert_eq!(full, None);
        assert_eq!(probe.reads(), reads_after_open);
        assert_eq!(reader.stats().routing, SegmentIndexV7ReadCount::default());
    }

    #[test]
    fn segment_index_v7_reader_routing_hit_and_miss_read_only_routing_bytes() {
        let fixture = reader_fixture(true);
        let key_len = 4 + LABEL_NAME.len() + LABEL_VALUE.len();
        let source = CountingSource::new(fixture.bytes.clone());
        let reader = SegmentIndexV7Reader::open(source).unwrap();

        let hit = reader
            .routing_exact_postings_metadata(LABEL_NAME, LABEL_VALUE)
            .unwrap();

        assert!(hit.index_present);
        assert_eq!(hit.metadata, Some(fixture.expected_metadata));
        assert_eq!(hit.bytes_read, (40 + 40 + key_len) as u64);
        let hit_stats = reader.stats();
        assert_eq!(
            hit_stats.routing,
            SegmentIndexV7ReadCount {
                calls: 3,
                bytes: hit.bytes_read,
            }
        );
        assert_eq!(hit_stats.payload, SegmentIndexV7ReadCount::default());
        assert_eq!(
            hit_stats.exact_directory,
            SegmentIndexV7ReadCount::default()
        );
        assert_eq!(hit_stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_eq!(
            hit_stats.auxiliary_directory,
            SegmentIndexV7ReadCount::default()
        );

        let hit_bucket = (super::super::super::routing_key_hash(
            &super::super::super::routing_key_bytes(LABEL_NAME, LABEL_VALUE).unwrap(),
        ) as u32)
            & 3;
        let missing = (0u32..)
            .map(|suffix| format!("missing-{suffix}"))
            .find(|candidate| {
                let key = super::super::super::routing_key_bytes(LABEL_NAME, candidate).unwrap();
                (super::super::super::routing_key_hash(&key) as u32 & 3) != hit_bucket
            })
            .unwrap();
        let miss_source = CountingSource::new(fixture.bytes);
        let miss_reader = SegmentIndexV7Reader::open(miss_source).unwrap();

        let miss = miss_reader
            .routing_exact_postings_metadata(LABEL_NAME, &missing)
            .unwrap();

        assert!(miss.index_present);
        assert_eq!(miss.metadata, None);
        assert_eq!(miss.bytes_read, 80);
        assert_eq!(
            miss_reader.stats().routing,
            SegmentIndexV7ReadCount {
                calls: 2,
                bytes: 80
            }
        );
        assert_category_sums(miss_reader.stats());
    }

    #[test]
    fn segment_index_v7_reader_routing_corruption_is_routing_only() {
        let mut fixture = reader_fixture(true);
        let routing = locator(&fixture.bytes, super::super::TRAILER_ROUTING_LOCATOR_OFFSET);
        fixture.bytes[routing.offset as usize] ^= 0xff;
        let source = CountingSource::new(fixture.bytes);
        let reader = SegmentIndexV7Reader::open(source).unwrap();

        let error = reader
            .routing_exact_postings_metadata(LABEL_NAME, LABEL_VALUE)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let stats = reader.stats();
        assert_eq!(
            stats.routing,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: 40
            }
        );
        assert_eq!(stats.payload, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_directory, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_eq!(
            stats.auxiliary_directory,
            SegmentIndexV7ReadCount::default()
        );
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_rejects_noncanonical_touched_routing_buckets() {
        #[derive(Clone, Copy)]
        enum Corruption {
            Flags,
            NoncanonicalEmpty,
            KeyRange,
            Hash,
            ReversedTime,
            ZeroByteLen,
        }
        let cases = [
            ("flags", Corruption::Flags, 1, 40),
            ("noncanonical empty", Corruption::NoncanonicalEmpty, 2, 80),
            ("key range", Corruption::KeyRange, 2, 80),
            (
                "hash mismatch",
                Corruption::Hash,
                3,
                80 + 4 + LABEL_NAME.len() + LABEL_VALUE.len(),
            ),
            ("reversed time", Corruption::ReversedTime, 2, 80),
            ("zero byte length", Corruption::ZeroByteLen, 2, 80),
        ];

        for (case, corruption, point_calls, point_bytes) in cases {
            let mut fixture = reader_fixture(true);
            let routing = locator(&fixture.bytes, super::super::TRAILER_ROUTING_LOCATOR_OFFSET);
            let routing_start = routing.offset as usize;
            let bucket = routing_occupied_bucket_offsets(&fixture.bytes)[0];
            match corruption {
                Corruption::Flags => put_u16_at(&mut fixture.bytes, routing_start + 6, 1),
                Corruption::NoncanonicalEmpty => put_u32_at(&mut fixture.bytes, bucket + 12, 0),
                Corruption::KeyRange => {
                    let declared_len = read_u64_at(&fixture.bytes, routing_start + 32);
                    put_u64_at(&mut fixture.bytes, routing_start + 32, declared_len - 1);
                }
                Corruption::Hash => {
                    let hash = read_u64_at(&fixture.bytes, bucket);
                    put_u64_at(&mut fixture.bytes, bucket, hash ^ 0x80);
                }
                Corruption::ReversedTime => {
                    put_u64_at(&mut fixture.bytes, bucket + 16, 2_000);
                    put_u64_at(&mut fixture.bytes, bucket + 24, 1_000);
                }
                Corruption::ZeroByteLen => put_u64_at(&mut fixture.bytes, bucket + 32, 0),
            }

            let point_reader =
                SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes.clone())).unwrap();
            let point_error = point_reader
                .routing_exact_postings_metadata(LABEL_NAME, LABEL_VALUE)
                .unwrap_err();
            assert_eq!(point_error.kind(), io::ErrorKind::InvalidData, "{case}");
            assert_eq!(
                point_reader.stats().routing,
                SegmentIndexV7ReadCount {
                    calls: point_calls,
                    bytes: point_bytes as u64,
                },
                "{case}"
            );

            let full_reader =
                SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();
            let full_error = full_reader.routing_index().unwrap_err();
            assert_eq!(full_error.kind(), io::ErrorKind::InvalidData, "{case}");
            assert_eq!(
                full_reader.stats().routing,
                SegmentIndexV7ReadCount {
                    calls: 1,
                    bytes: routing.len,
                },
                "{case}"
            );
        }
    }

    #[test]
    fn segment_index_v7_reader_full_routing_decode_rejects_duplicate_logical_keys() {
        let (mut fixture, _target, _metadata, _expected_bytes) = collision_reader_fixture();
        let buckets = routing_occupied_bucket_offsets(&fixture.bytes);
        assert_eq!(buckets.len(), 2);
        let first_hash = read_u64_at(&fixture.bytes, buckets[0]);
        let first_key_offset = read_u32_at(&fixture.bytes, buckets[0] + 8);
        let first_key_len = read_u32_at(&fixture.bytes, buckets[0] + 12);
        put_u64_at(&mut fixture.bytes, buckets[1], first_hash);
        put_u32_at(&mut fixture.bytes, buckets[1] + 8, first_key_offset);
        put_u32_at(&mut fixture.bytes, buckets[1] + 12, first_key_len);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        let error = reader.routing_index().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn segment_index_v7_reader_valid_collision_chain_validates_each_touched_key() {
        let (fixture, target, expected_metadata, expected_bytes) = collision_reader_fixture();
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        let lookup = reader
            .routing_exact_postings_metadata(LABEL_NAME, &target)
            .unwrap();

        assert_eq!(lookup.metadata, Some(expected_metadata));
        assert_eq!(lookup.bytes_read, expected_bytes as u64);
        assert_eq!(
            reader.stats().routing,
            SegmentIndexV7ReadCount {
                calls: 5,
                bytes: expected_bytes as u64,
            }
        );
    }

    #[test]
    fn segment_index_v7_reader_full_routing_decode_reads_one_routing_blob() {
        let fixture = reader_fixture(true);
        let expected = fixture.indexes.routing_index.clone();
        let routing = locator(&fixture.bytes, super::super::TRAILER_ROUTING_LOCATOR_OFFSET);
        let source = CountingSource::new(fixture.bytes);
        let reader = SegmentIndexV7Reader::open(source).unwrap();

        let actual = reader.routing_index().unwrap();

        assert_eq!(actual, expected);
        assert_eq!(
            reader.stats().routing,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: routing.len,
            }
        );
    }

    #[test]
    fn segment_index_v7_reader_metric_lookup_reads_one_payload_blob() {
        let fixture = reader_fixture(true);
        let expected = fixture
            .indexes
            .metric_series_ranges
            .ranges(fixture.metric_sym)
            .to_vec();
        let metric = locator(&fixture.bytes, super::super::TRAILER_METRIC_LOCATOR_OFFSET);
        let source = CountingSource::new(fixture.bytes);
        let reader = SegmentIndexV7Reader::open(source).unwrap();

        let actual = reader.metric_series_ranges(fixture.metric_sym).unwrap();

        assert_eq!(actual, expected);
        let stats = reader.stats();
        assert_eq!(
            stats.payload,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: metric.len,
            }
        );
        assert_eq!(stats.routing, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_directory, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_eq!(
            stats.auxiliary_directory,
            SegmentIndexV7ReadCount::default()
        );
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_empty_initializes_required_directory_once() {
        let bytes = exact_reader_bytes(&[]);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        assert!(!reader.has_label_values().unwrap());
        assert!(reader.label_name_symbols().unwrap().is_empty());
        assert_eq!(reader.label_time_range(7).unwrap(), None);
        assert!(reader.label_values(7).unwrap().is_empty());
        assert!(
            reader
                .label_values_with_prefix(7, Some("missing"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(reader.label_value_time_range(7, 9).unwrap(), None);
        assert_eq!(reader.label_value_time_ranges(7).unwrap(), None);

        let stats = reader.stats();
        assert_eq!(
            stats.auxiliary_directory,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: 64,
            }
        );
        assert_eq!(stats.exact_directory, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.payload, SegmentIndexV7ReadCount::default());
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_fst_only_summary_is_canonical() {
        let mut symbols = SegmentSymbols::default();
        let name_sym = symbols.intern("service");
        let value_sym = symbols.intern("api");
        let series = vec![SeriesEntry {
            series_id: 1,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(name_sym, value_sym)],
        }];
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::from_series(&series, &symbols).unwrap(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes.clone())).unwrap();
        assert_eq!(
            reader.label_time_range(name_sym).unwrap(),
            Some(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            })
        );

        let fst_record = auxiliary_record_offset(
            &bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            name_sym,
        );
        put_u64_at(&mut bytes, fst_record + 24, 1);
        refresh_auxiliary_directory_crc(&mut bytes);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        let error = reader.label_time_range(name_sym).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(reader.stats().payload.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_round_trips_metadata_fsts_and_time_ranges() {
        let fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        let service_fst = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        let service_ranges = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
            fixture.service_name_sym,
        );
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        assert!(reader.has_label_values().unwrap());
        assert_eq!(
            reader.label_name_symbols().unwrap(),
            vec![fixture.service_name_sym, fixture.zone_name_sym]
        );
        assert_eq!(
            reader.label_time_range(fixture.service_name_sym).unwrap(),
            Some(LabelValueTimeRange {
                min_time_ms: 100,
                max_time_ms: 399,
            })
        );
        assert_eq!(
            reader.label_time_range(fixture.time_only_name_sym).unwrap(),
            None
        );
        assert_eq!(
            reader.stats().auxiliary_directory,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: directory.len,
            }
        );
        assert_eq!(reader.stats().payload, SegmentIndexV7ReadCount::default());

        assert_eq!(
            reader.label_values(fixture.service_name_sym).unwrap(),
            vec!["api".to_string(), "worker".to_string()]
        );
        assert_eq!(
            reader
                .label_values_with_prefix(fixture.service_name_sym, Some("ap"))
                .unwrap(),
            vec!["api".to_string()]
        );
        assert!(reader.label_values(u32::MAX).unwrap().is_empty());
        assert_eq!(
            reader.stats().payload,
            SegmentIndexV7ReadCount {
                calls: 2,
                bytes: service_fst.len * 2,
            }
        );

        assert_eq!(
            reader
                .label_value_time_range(fixture.service_name_sym, fixture.api_value_sym)
                .unwrap(),
            Some(LabelValueTimeRange {
                min_time_ms: 100,
                max_time_ms: 199,
            })
        );
        assert_eq!(
            reader
                .label_value_time_ranges(fixture.service_name_sym)
                .unwrap(),
            Some(vec![
                (
                    fixture.api_value_sym,
                    LabelValueTimeRange {
                        min_time_ms: 100,
                        max_time_ms: 199,
                    },
                ),
                (
                    fixture.worker_value_sym,
                    LabelValueTimeRange {
                        min_time_ms: 300,
                        max_time_ms: 399,
                    },
                ),
            ])
        );
        assert_eq!(
            reader
                .label_value_time_ranges(fixture.time_only_name_sym)
                .unwrap(),
            Some(vec![(
                fixture.time_only_value_sym,
                LabelValueTimeRange {
                    min_time_ms: 700,
                    max_time_ms: 799,
                },
            )])
        );
        let stats = reader.stats();
        assert_eq!(
            stats.payload,
            SegmentIndexV7ReadCount {
                calls: 5,
                bytes: service_fst.len * 2 + service_ranges.len * 2 + 24,
            }
        );
        assert_eq!(stats.exact_directory, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_cache_is_shared_and_race_safe() {
        let fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        let source = CountingSource::new(fixture.bytes);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));
        let handles = (0..THREAD_COUNT)
            .map(|_| {
                let cloned = reader.try_clone_reader().unwrap();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    assert!(cloned.has_label_values().unwrap());
                    cloned.stats()
                })
            })
            .collect::<Vec<_>>();

        let stats = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            probe
                .reads()
                .into_iter()
                .filter(|read| *read == (directory.offset, directory.len as usize))
                .count(),
            1
        );
        assert_eq!(
            stats
                .iter()
                .map(|stats| stats.auxiliary_directory.calls)
                .sum::<u64>(),
            1
        );
        assert_eq!(
            stats
                .iter()
                .map(|stats| stats.auxiliary_directory.bytes)
                .sum::<u64>(),
            directory.len
        );
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_errors_are_cached_across_clones() {
        let mut fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        put_u32_at(&mut fixture.bytes, directory.offset as usize, 0);
        refresh_auxiliary_directory_crc(&mut fixture.bytes);
        let source = CountingSource::new(fixture.bytes);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let cloned = reader.try_clone_reader().unwrap();

        let first = reader.has_label_values().unwrap_err();
        let second = cloned.label_name_symbols().unwrap_err();

        assert_eq!(first.kind(), io::ErrorKind::InvalidData);
        assert_eq!(first.kind(), second.kind());
        assert_eq!(first.to_string(), second.to_string());
        assert_eq!(
            probe
                .reads()
                .into_iter()
                .filter(|read| *read == (directory.offset, directory.len as usize))
                .count(),
            1
        );
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(cloned.stats().auxiliary_directory.calls, 0);

        let fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        let source = CountingSource::failing_at(fixture.bytes, directory.offset);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let cloned = reader.try_clone_reader().unwrap();
        let first = reader.has_label_values().unwrap_err();
        let second = cloned.has_label_values().unwrap_err();
        assert_eq!(first.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(first.kind(), second.kind());
        assert_eq!(first.to_string(), second.to_string());
        assert_eq!(
            probe
                .reads()
                .into_iter()
                .filter(|read| *read == (directory.offset, directory.len as usize))
                .count(),
            1
        );
        assert_eq!(reader.stats().auxiliary_directory.calls, 0);
        assert_eq!(cloned.stats().auxiliary_directory.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_rejects_header_corruption() {
        enum Corruption {
            Magic,
            Version,
            Flags,
            HeaderLen,
            RecordLen,
            EntryCount,
            RecordsOffset,
            RecordsLen,
            Crc,
            Reserved,
        }
        let cases = [
            ("magic", Corruption::Magic),
            ("version", Corruption::Version),
            ("flags", Corruption::Flags),
            ("header len", Corruption::HeaderLen),
            ("record len", Corruption::RecordLen),
            ("entry count", Corruption::EntryCount),
            ("records offset", Corruption::RecordsOffset),
            ("records len", Corruption::RecordsLen),
            ("crc", Corruption::Crc),
            ("reserved", Corruption::Reserved),
        ];

        for (case, corruption) in cases {
            let mut fixture = auxiliary_reader_fixture();
            let directory = locator(
                &fixture.bytes,
                super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
            );
            let start = directory.offset as usize;
            let refresh_crc = !matches!(corruption, Corruption::Crc);
            match corruption {
                Corruption::Magic => put_u32_at(&mut fixture.bytes, start, 0),
                Corruption::Version => put_u16_at(&mut fixture.bytes, start + 4, 2),
                Corruption::Flags => put_u16_at(&mut fixture.bytes, start + 6, 1),
                Corruption::HeaderLen => put_u32_at(&mut fixture.bytes, start + 8, 63),
                Corruption::RecordLen => put_u32_at(&mut fixture.bytes, start + 12, 39),
                Corruption::EntryCount => put_u64_at(&mut fixture.bytes, start + 16, 6),
                Corruption::RecordsOffset => put_u64_at(&mut fixture.bytes, start + 24, 63),
                Corruption::RecordsLen => put_u64_at(&mut fixture.bytes, start + 32, 199),
                Corruption::Crc => {
                    let crc = read_u32_at(&fixture.bytes, start + 40);
                    put_u32_at(&mut fixture.bytes, start + 40, crc ^ 1);
                }
                Corruption::Reserved => fixture.bytes[start + 44] = 1,
            }
            if refresh_crc {
                refresh_auxiliary_directory_crc(&mut fixture.bytes);
            }
            let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

            let error = reader.has_label_values().unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(
                reader.stats().auxiliary_directory,
                SegmentIndexV7ReadCount {
                    calls: 1,
                    bytes: directory.len,
                },
                "{case}"
            );
            assert_eq!(reader.stats().payload.calls, 0, "{case}");
        }
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_rejects_record_corruption() {
        enum Corruption {
            Kind,
            Flags,
            Duplicate,
            ZeroPayload,
            BeforePayloads,
            AfterPayloads,
            Overflow,
            ReversedTime,
            SummaryMismatch,
        }
        let cases = [
            ("kind", Corruption::Kind),
            ("flags", Corruption::Flags),
            ("duplicate", Corruption::Duplicate),
            ("zero payload", Corruption::ZeroPayload),
            ("before payloads", Corruption::BeforePayloads),
            ("after payloads", Corruption::AfterPayloads),
            ("overflow", Corruption::Overflow),
            ("reversed time", Corruption::ReversedTime),
            ("summary mismatch", Corruption::SummaryMismatch),
        ];

        for (case, corruption) in cases {
            let mut fixture = auxiliary_reader_fixture();
            let directory = locator(
                &fixture.bytes,
                super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
            );
            let payloads = locator(
                &fixture.bytes,
                super::super::TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET,
            );
            let first_record = directory.offset as usize + 64;
            let second_record = first_record + 40;
            match corruption {
                Corruption::Kind => put_u16_at(&mut fixture.bytes, first_record, 4),
                Corruption::Flags => put_u16_at(&mut fixture.bytes, first_record + 2, 1),
                Corruption::Duplicate => {
                    let first_name = read_u32_at(&fixture.bytes, first_record + 4);
                    put_u32_at(&mut fixture.bytes, second_record + 4, first_name);
                }
                Corruption::ZeroPayload => put_u64_at(&mut fixture.bytes, first_record + 16, 0),
                Corruption::BeforePayloads => {
                    put_u64_at(&mut fixture.bytes, first_record + 8, payloads.offset - 1)
                }
                Corruption::AfterPayloads => put_u64_at(
                    &mut fixture.bytes,
                    first_record + 8,
                    payloads.offset + payloads.len,
                ),
                Corruption::Overflow => {
                    put_u64_at(&mut fixture.bytes, first_record + 8, u64::MAX - 3);
                    put_u64_at(&mut fixture.bytes, first_record + 16, 8);
                }
                Corruption::ReversedTime => {
                    put_u64_at(&mut fixture.bytes, first_record + 24, 2_000);
                    put_u64_at(&mut fixture.bytes, first_record + 32, 1_000);
                }
                Corruption::SummaryMismatch => {
                    let service_fst = auxiliary_record_offset(
                        &fixture.bytes,
                        super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
                        fixture.service_name_sym,
                    );
                    put_u64_at(&mut fixture.bytes, service_fst + 24, 101);
                }
            }
            refresh_auxiliary_directory_crc(&mut fixture.bytes);
            let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

            let error = reader.label_name_symbols().unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().auxiliary_directory.calls, 1, "{case}");
            assert_eq!(reader.stats().payload.calls, 0, "{case}");
        }
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_rejects_invalid_fst_after_one_payload_read() {
        let mut fixture = auxiliary_reader_fixture();
        let fst = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        fixture.bytes[fst.offset as usize..(fst.offset + fst.len) as usize].fill(0xff);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        let error = reader.label_values(fixture.service_name_sym).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(
            reader.stats().payload,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: fst.len,
            }
        );
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_rejects_fst_without_values() {
        let mut fixture = auxiliary_reader_fixture();
        let record = auxiliary_record_offset(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        let fst = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        let empty_fst = fst::SetBuilder::memory().into_inner().unwrap();
        assert!(empty_fst.len() <= fst.len as usize);
        let start = fst.offset as usize;
        fixture.bytes[start..start + empty_fst.len()].copy_from_slice(&empty_fst);
        put_u64_at(&mut fixture.bytes, record + 16, empty_fst.len() as u64);
        refresh_auxiliary_directory_crc(&mut fixture.bytes);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        let error = reader.label_values(fixture.service_name_sym).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(reader.stats().payload.calls, 1);
        assert_eq!(reader.stats().payload.bytes, empty_fst.len() as u64);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_rejects_time_range_payload_corruption() {
        enum Corruption {
            HugeCount,
            Truncated,
            Duplicate,
            Descending,
            ReversedAfterTarget,
            Empty,
            SummaryMismatch,
        }
        let cases = [
            ("huge count", Corruption::HugeCount),
            ("truncated", Corruption::Truncated),
            ("duplicate", Corruption::Duplicate),
            ("descending", Corruption::Descending),
            ("reversed after target", Corruption::ReversedAfterTarget),
            ("empty", Corruption::Empty),
            ("summary mismatch", Corruption::SummaryMismatch),
        ];

        for (case, corruption) in cases {
            let mut fixture = auxiliary_reader_fixture();
            let record = auxiliary_record_offset(
                &fixture.bytes,
                super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                fixture.service_name_sym,
            );
            let payload = auxiliary_payload_locator(
                &fixture.bytes,
                super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                fixture.service_name_sym,
            );
            let first_value = read_u32_at(&fixture.bytes, payload.offset as usize + 4);
            let second_record = payload.offset as usize + 24;
            let expected_read_len = match corruption {
                Corruption::HugeCount => {
                    put_u32_at(&mut fixture.bytes, payload.offset as usize, u32::MAX);
                    payload.len
                }
                Corruption::Truncated => {
                    put_u64_at(&mut fixture.bytes, record + 16, payload.len - 1);
                    refresh_auxiliary_directory_crc(&mut fixture.bytes);
                    payload.len - 1
                }
                Corruption::Duplicate => {
                    put_u32_at(&mut fixture.bytes, second_record, first_value);
                    payload.len
                }
                Corruption::Descending => {
                    put_u32_at(
                        &mut fixture.bytes,
                        second_record,
                        first_value.saturating_sub(1),
                    );
                    payload.len
                }
                Corruption::ReversedAfterTarget => {
                    put_u64_at(&mut fixture.bytes, second_record + 4, 900);
                    put_u64_at(&mut fixture.bytes, second_record + 12, 800);
                    payload.len
                }
                Corruption::Empty => {
                    put_u32_at(&mut fixture.bytes, payload.offset as usize, 0);
                    put_u64_at(&mut fixture.bytes, record + 16, 4);
                    refresh_auxiliary_directory_crc(&mut fixture.bytes);
                    4
                }
                Corruption::SummaryMismatch => {
                    put_u64_at(&mut fixture.bytes, payload.offset as usize + 8, 101);
                    payload.len
                }
            };
            let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

            let error = reader
                .label_value_time_range(fixture.service_name_sym, fixture.api_value_sym)
                .unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().auxiliary_directory.calls, 1, "{case}");
            assert_eq!(
                reader.stats().payload,
                SegmentIndexV7ReadCount {
                    calls: 1,
                    bytes: expected_read_len,
                },
                "{case}"
            );
        }
    }

    #[test]
    fn segment_index_v7_reader_exact_empty_initializes_required_directory_once() {
        let bytes = exact_reader_bytes(&[]);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        assert_eq!(reader.exact_postings_metadata(7, 1).unwrap(), None);
        assert_eq!(reader.exact_postings(7, 1).unwrap(), None);

        let stats = reader.stats();
        assert_eq!(
            stats.exact_directory,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: 64,
            }
        );
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.payload, SegmentIndexV7ReadCount::default());
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_exact_selection_reads_payload_without_second_page() {
        let bytes = exact_reader_bytes(&[(12, vec![3, 7, 11])]);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        let selection = reader.exact_postings_selection(7, 12).unwrap().unwrap();

        assert_eq!(
            selection.metadata,
            ExactPostingsMetadata {
                byte_len: 16,
                time_range: super::super::super::LabelValueTimeRange {
                    min_time_ms: 1_012,
                    max_time_ms: 2_012,
                },
            }
        );
        assert_eq!(
            reader.stats().exact_directory,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: 96,
            }
        );
        assert_eq!(
            reader.stats().exact_page,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: 16_384,
            }
        );

        let refs = reader.read_exact_postings_selection(selection).unwrap();

        assert_eq!(refs, vec![3, 7, 11]);
        assert_eq!(reader.stats().exact_page.calls, 1);
        assert_eq!(
            reader.stats().payload,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: 16,
            }
        );
        assert_category_sums(reader.stats());
    }

    #[test]
    fn segment_index_v7_reader_exact_selects_409_and_410_page_boundaries() {
        let entries_409 = exact_boundary_entries(409);
        let bytes_409 = exact_reader_bytes(&entries_409);
        let reader_409 = SegmentIndexV7Reader::open(CountingSource::new(bytes_409)).unwrap();

        assert!(
            reader_409
                .exact_postings_metadata(7, 408)
                .unwrap()
                .is_some()
        );
        assert_eq!(reader_409.exact_postings_metadata(7, 409).unwrap(), None);
        assert_eq!(reader_409.stats().exact_directory.calls, 1);
        assert_eq!(reader_409.stats().exact_page.calls, 1);

        let entries_410 = exact_boundary_entries(410);
        let bytes_410 = exact_reader_bytes(&entries_410);
        let pages = locator(&bytes_410, super::super::TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
        let source = CountingSource::new(bytes_410);
        let probe = source.clone();
        let reader_410 = SegmentIndexV7Reader::open(source).unwrap();

        assert!(
            reader_410
                .exact_postings_metadata(7, 409)
                .unwrap()
                .is_some()
        );

        assert_eq!(reader_410.stats().exact_directory.calls, 1);
        assert_eq!(reader_410.stats().exact_page.calls, 1);
        assert!(probe.reads().contains(&(pages.offset + 16_384, 16_384)));
    }

    #[test]
    fn segment_index_v7_reader_exact_descriptor_gap_avoids_page_read() {
        let mut entries = exact_boundary_entries(409);
        entries.push((1_000, vec![20_000]));
        let bytes = exact_reader_bytes(&entries);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        assert_eq!(reader.exact_postings_metadata(7, 500).unwrap(), None);

        assert_eq!(reader.stats().exact_directory.calls, 1);
        assert_eq!(
            reader.stats().exact_page,
            SegmentIndexV7ReadCount::default()
        );
    }

    #[test]
    fn segment_index_v7_reader_exact_missing_key_inside_page_range_reads_one_page() {
        let bytes = exact_reader_bytes(&[(12, vec![3]), (14, vec![7])]);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        assert_eq!(reader.exact_postings_metadata(7, 13).unwrap(), None);

        assert_eq!(reader.stats().exact_directory.calls, 1);
        assert_eq!(reader.stats().exact_page.calls, 1);
        assert_eq!(reader.stats().payload.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_exact_directory_cache_is_shared_across_clones() {
        let bytes = exact_reader_bytes(&[(12, vec![3])]);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();
        let cloned = reader.try_clone_reader().unwrap();

        assert_eq!(reader.exact_postings_metadata(7, 99).unwrap(), None);
        assert_eq!(cloned.exact_postings_metadata(7, 99).unwrap(), None);

        assert_eq!(reader.stats().exact_directory.calls, 1);
        assert_eq!(cloned.stats().exact_directory.calls, 0);
        assert_eq!(reader.stats().exact_page.calls, 0);
        assert_eq!(cloned.stats().exact_page.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_exact_directory_concurrent_first_init_reads_once() {
        let bytes = exact_reader_bytes(&[]);
        let source = CountingSource::new(bytes.clone());
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let directory = locator(&bytes, super::super::TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));
        let handles = (0..THREAD_COUNT)
            .map(|_| {
                let cloned = reader.try_clone_reader().unwrap();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    assert_eq!(cloned.exact_postings_metadata(7, 1).unwrap(), None);
                    cloned.stats()
                })
            })
            .collect::<Vec<_>>();

        let stats = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            probe
                .reads()
                .into_iter()
                .filter(|read| *read == (directory.offset, 64))
                .count(),
            1
        );
        assert_eq!(
            stats
                .iter()
                .map(|stats| stats.exact_directory.calls)
                .sum::<u64>(),
            1
        );
    }

    #[test]
    fn segment_index_v7_reader_exact_directory_rejects_structural_corruption() {
        enum Corruption {
            Magic,
            Version,
            Flags,
            HeaderLen,
            DescriptorLen,
            PageLen,
            RecordLen,
            EntryCount,
            PageCount,
            RecordsPerPage,
            DescriptorsOffset,
            DescriptorsLen,
            Crc,
            Reserved,
            DescriptorCount,
            DescriptorReserved0,
            DescriptorReserved1,
            DescriptorRange,
        }
        let cases = [
            ("magic", Corruption::Magic),
            ("version", Corruption::Version),
            ("flags", Corruption::Flags),
            ("header len", Corruption::HeaderLen),
            ("descriptor len", Corruption::DescriptorLen),
            ("page len", Corruption::PageLen),
            ("record len", Corruption::RecordLen),
            ("entry count", Corruption::EntryCount),
            ("page count", Corruption::PageCount),
            ("records per page", Corruption::RecordsPerPage),
            ("descriptors offset", Corruption::DescriptorsOffset),
            ("descriptors len", Corruption::DescriptorsLen),
            ("crc", Corruption::Crc),
            ("reserved", Corruption::Reserved),
            ("descriptor count", Corruption::DescriptorCount),
            ("descriptor reserved0", Corruption::DescriptorReserved0),
            ("descriptor reserved1", Corruption::DescriptorReserved1),
            ("descriptor range", Corruption::DescriptorRange),
        ];

        for (case, corruption) in cases {
            let mut bytes = exact_reader_bytes(&[(12, vec![3])]);
            let directory = locator(&bytes, super::super::TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
            let start = directory.offset as usize;
            let descriptor = start + 64;
            let refresh_crc = !matches!(corruption, Corruption::Crc);
            match corruption {
                Corruption::Magic => put_u32_at(&mut bytes, start, 0),
                Corruption::Version => put_u16_at(&mut bytes, start + 4, 2),
                Corruption::Flags => put_u16_at(&mut bytes, start + 6, 1),
                Corruption::HeaderLen => put_u32_at(&mut bytes, start + 8, 63),
                Corruption::DescriptorLen => put_u32_at(&mut bytes, start + 12, 31),
                Corruption::PageLen => put_u32_at(&mut bytes, start + 16, 16_383),
                Corruption::RecordLen => put_u32_at(&mut bytes, start + 20, 39),
                Corruption::EntryCount => put_u64_at(&mut bytes, start + 24, 2),
                Corruption::PageCount => put_u32_at(&mut bytes, start + 32, 2),
                Corruption::RecordsPerPage => put_u32_at(&mut bytes, start + 36, 408),
                Corruption::DescriptorsOffset => put_u64_at(&mut bytes, start + 40, 63),
                Corruption::DescriptorsLen => put_u64_at(&mut bytes, start + 48, 31),
                Corruption::Crc => {
                    let crc = read_u32_at(&bytes, start + 56);
                    put_u32_at(&mut bytes, start + 56, crc ^ 1);
                }
                Corruption::Reserved => put_u32_at(&mut bytes, start + 60, 1),
                Corruption::DescriptorCount => put_u32_at(&mut bytes, descriptor + 16, 2),
                Corruption::DescriptorReserved0 => put_u32_at(&mut bytes, descriptor + 20, 1),
                Corruption::DescriptorReserved1 => put_u32_at(&mut bytes, descriptor + 28, 1),
                Corruption::DescriptorRange => put_u32_at(&mut bytes, descriptor + 12, 11),
            }
            if refresh_crc {
                refresh_exact_directory_crc(&mut bytes);
            }
            let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

            let error = reader.exact_postings_metadata(7, 12).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().exact_directory.calls, 1, "{case}");
            assert_eq!(reader.stats().exact_page.calls, 0, "{case}");
        }
    }

    #[test]
    fn segment_index_v7_reader_exact_directory_failures_are_cached_across_clones() {
        let mut bytes = exact_reader_bytes(&[(12, vec![3])]);
        let directory = locator(&bytes, super::super::TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
        put_u32_at(&mut bytes, directory.offset as usize, 0);
        refresh_exact_directory_crc(&mut bytes);
        let source = CountingSource::new(bytes);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let cloned = reader.try_clone_reader().unwrap();

        let first = reader.exact_postings_metadata(7, 12).unwrap_err();
        let second = cloned.exact_postings_metadata(7, 12).unwrap_err();

        assert_eq!(first.kind(), io::ErrorKind::InvalidData);
        assert_eq!(first.kind(), second.kind());
        assert_eq!(first.to_string(), second.to_string());
        assert_eq!(
            probe
                .reads()
                .into_iter()
                .filter(|read| *read == (directory.offset, directory.len as usize))
                .count(),
            1
        );
        assert_eq!(reader.stats().exact_directory.calls, 1);
        assert_eq!(cloned.stats().exact_directory.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_exact_directory_io_failure_is_cached_without_counting_success() {
        let bytes = exact_reader_bytes(&[(12, vec![3])]);
        let directory = locator(&bytes, super::super::TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
        let source = CountingSource::failing_at(bytes, directory.offset);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let cloned = reader.try_clone_reader().unwrap();

        let first = reader.exact_postings_metadata(7, 12).unwrap_err();
        let second = cloned.exact_postings_metadata(7, 12).unwrap_err();

        assert_eq!(first.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(first.kind(), second.kind());
        assert_eq!(first.to_string(), second.to_string());
        assert_eq!(
            probe
                .reads()
                .into_iter()
                .filter(|read| *read == (directory.offset, directory.len as usize))
                .count(),
            1
        );
        assert_eq!(reader.stats().exact_directory.calls, 0);
        assert_eq!(cloned.stats().exact_directory.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_exact_page_rejects_structural_corruption() {
        enum Corruption {
            Crc,
            Magic,
            Version,
            Flags,
            PageIndex,
            RecordCount,
            DescriptorFirst,
            DescriptorLast,
            RecordOrder,
            PostingsBeforeRegion,
            PostingsAfterRegion,
            PostingsOverflow,
            ReversedTime,
            Padding,
        }
        let cases = [
            ("crc", Corruption::Crc),
            ("magic", Corruption::Magic),
            ("version", Corruption::Version),
            ("flags", Corruption::Flags),
            ("page index", Corruption::PageIndex),
            ("record count", Corruption::RecordCount),
            ("descriptor first", Corruption::DescriptorFirst),
            ("descriptor last", Corruption::DescriptorLast),
            ("record order", Corruption::RecordOrder),
            ("postings before", Corruption::PostingsBeforeRegion),
            ("postings after", Corruption::PostingsAfterRegion),
            ("postings overflow", Corruption::PostingsOverflow),
            ("reversed time", Corruption::ReversedTime),
            ("padding", Corruption::Padding),
        ];

        for (case, corruption) in cases {
            let mut bytes = exact_reader_bytes(&[(12, vec![3]), (13, vec![7])]);
            let directory = locator(&bytes, super::super::TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
            let pages = locator(&bytes, super::super::TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
            let postings = locator(&bytes, super::super::TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET);
            let descriptor = directory.offset as usize + EXACT_DIRECTORY_HEADER_LEN;
            let page = pages.offset as usize;
            let first_record = page + 16;
            let second_record = first_record + EXACT_RECORD_LEN;
            let mut refresh_page = true;
            let mut refresh_directory_only = false;
            match corruption {
                Corruption::Crc => {
                    bytes[page + EXACT_PAGE_LEN - 1] ^= 1;
                    refresh_page = false;
                }
                Corruption::Magic => put_u32_at(&mut bytes, page, 0),
                Corruption::Version => put_u16_at(&mut bytes, page + 4, 2),
                Corruption::Flags => put_u16_at(&mut bytes, page + 6, 1),
                Corruption::PageIndex => put_u32_at(&mut bytes, page + 8, 1),
                Corruption::RecordCount => put_u32_at(&mut bytes, page + 12, 1),
                Corruption::DescriptorFirst => {
                    put_u32_at(&mut bytes, descriptor + 4, 11);
                    refresh_page = false;
                    refresh_directory_only = true;
                }
                Corruption::DescriptorLast => {
                    put_u32_at(&mut bytes, descriptor + 12, 14);
                    refresh_page = false;
                    refresh_directory_only = true;
                }
                Corruption::RecordOrder => put_u32_at(&mut bytes, second_record + 4, 12),
                Corruption::PostingsBeforeRegion => {
                    put_u64_at(&mut bytes, first_record + 8, postings.offset - 1)
                }
                Corruption::PostingsAfterRegion => {
                    put_u64_at(&mut bytes, first_record + 8, postings.offset + postings.len)
                }
                Corruption::PostingsOverflow => {
                    put_u64_at(&mut bytes, first_record + 8, u64::MAX - 3)
                }
                Corruption::ReversedTime => {
                    put_u64_at(&mut bytes, first_record + 24, 3_000);
                    put_u64_at(&mut bytes, first_record + 32, 1_000);
                }
                Corruption::Padding => bytes[page + EXACT_PAGE_LEN - 1] = 1,
            }
            if refresh_page {
                refresh_exact_page_crc(&mut bytes, 0);
            } else if refresh_directory_only {
                refresh_exact_directory_crc(&mut bytes);
            }
            let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

            let error = reader.exact_postings_metadata(7, 12).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().exact_directory.calls, 1, "{case}");
            assert_eq!(reader.stats().exact_page.calls, 1, "{case}");
            assert_eq!(reader.stats().payload.calls, 0, "{case}");
        }
    }

    #[test]
    fn segment_index_v7_reader_exact_postings_rejects_forged_count_and_unsorted_refs() {
        enum Corruption {
            Count,
            Order,
        }
        for (case, corruption) in [("count", Corruption::Count), ("order", Corruption::Order)] {
            let mut bytes = exact_reader_bytes(&[(12, vec![3, 7])]);
            let pages = locator(&bytes, super::super::TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
            let record = pages.offset as usize + 16;
            let payload = read_u64_at(&bytes, record + 8) as usize;
            match corruption {
                Corruption::Count => put_u32_at(&mut bytes, payload, u32::MAX),
                Corruption::Order => put_u32_at(&mut bytes, payload + 8, 3),
            }
            let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

            let error = reader.exact_postings(7, 12).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().exact_directory.calls, 1, "{case}");
            assert_eq!(reader.stats().exact_page.calls, 1, "{case}");
            assert_eq!(reader.stats().payload.calls, 1, "{case}");
        }
    }

    #[test]
    fn segment_index_v7_reader_rejects_malformed_metric_payloads_after_one_read() {
        enum Corruption {
            Flags,
            MetricCount,
            RangeCount,
            DuplicateMetric,
            Reserved,
            ZeroSeriesCount,
            SeriesEnd,
            ReversedTime,
            Overlap,
        }
        let cases = [
            ("flags", Corruption::Flags),
            ("metric count", Corruption::MetricCount),
            ("range count", Corruption::RangeCount),
            ("duplicate metric", Corruption::DuplicateMetric),
            ("reserved", Corruption::Reserved),
            ("zero series count", Corruption::ZeroSeriesCount),
            ("series end", Corruption::SeriesEnd),
            ("reversed time", Corruption::ReversedTime),
            ("overlap", Corruption::Overlap),
        ];

        for (case, corruption) in cases {
            let mut fixture = metric_validation_fixture();
            let metric = locator(&fixture.bytes, super::super::TRAILER_METRIC_LOCATOR_OFFSET);
            let start = metric.offset as usize;
            let first_record = start + 20;
            let second_record = first_record + 28;
            let second_metric = start + 12 + 8 + 2 * 28;
            match corruption {
                Corruption::Flags => put_u16_at(&mut fixture.bytes, start + 6, 1),
                Corruption::MetricCount => put_u32_at(&mut fixture.bytes, start + 8, u32::MAX),
                Corruption::RangeCount => put_u32_at(&mut fixture.bytes, start + 16, 1_000),
                Corruption::DuplicateMetric => put_u32_at(&mut fixture.bytes, second_metric, 10),
                Corruption::Reserved => put_u16_at(&mut fixture.bytes, first_record + 10, 1),
                Corruption::ZeroSeriesCount => put_u32_at(&mut fixture.bytes, first_record + 4, 0),
                Corruption::SeriesEnd => {
                    put_u32_at(&mut fixture.bytes, first_record, u32::MAX);
                    put_u32_at(&mut fixture.bytes, first_record + 4, 2);
                }
                Corruption::ReversedTime => {
                    put_u64_at(&mut fixture.bytes, first_record + 12, 200);
                    put_u64_at(&mut fixture.bytes, first_record + 20, 100);
                }
                Corruption::Overlap => put_u32_at(&mut fixture.bytes, second_record, 1),
            }
            let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

            let error = reader.metric_series_range_index().unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(
                reader.stats().payload,
                SegmentIndexV7ReadCount {
                    calls: 1,
                    bytes: metric.len,
                },
                "{case}"
            );
            assert_category_sums(reader.stats());
        }
    }

    #[test]
    fn segment_index_v7_reader_allocation_failures_are_other_errors() {
        let fixture = reader_fixture(false);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        let error = reader
            .read_blob(
                BlobLocator {
                    offset: 16,
                    len: u64::MAX,
                },
                SegmentIndexV7ReadCategory::Payload,
            )
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(reader.stats().payload, SegmentIndexV7ReadCount::default());
    }

    #[test]
    fn segment_index_v7_reader_clone_shares_arcs_without_reads_and_resets_stats() {
        let fixture = reader_fixture(true);
        let source = CountingSource::new(fixture.bytes);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let reads_after_open = probe.reads();

        let cloned = reader.try_clone_reader().unwrap();

        assert!(Arc::ptr_eq(&reader.source, &cloned.source));
        assert!(Arc::ptr_eq(&reader.state, &cloned.state));
        assert_eq!(probe.reads(), reads_after_open);
        assert_eq!(cloned.stats(), SegmentIndexV7ReadStats::default());
        assert_eq!(reader.stats().root.calls, 2);

        cloned
            .routing_exact_postings_metadata(LABEL_NAME, LABEL_VALUE)
            .unwrap();
        assert!(cloned.stats().routing.calls > 0);
        assert_eq!(reader.stats().routing.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_file_clones_concurrently_mix_routing_and_metric_reads() {
        let fixture = reader_fixture(true);
        let expected_ranges = fixture
            .indexes
            .metric_series_ranges
            .ranges(fixture.metric_sym)
            .to_vec();
        let file = file_with_bytes(&fixture.bytes);
        let reader = SegmentIndexV7Reader::open(file).unwrap();
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));
        let handles = (0..THREAD_COUNT)
            .map(|thread_index| {
                let cloned = reader.try_clone_reader().unwrap();
                let barrier = Arc::clone(&barrier);
                let expected_ranges = expected_ranges.clone();
                let expected_metadata = fixture.expected_metadata;
                let metric_sym = fixture.metric_sym;
                thread::spawn(move || {
                    barrier.wait();
                    for iteration in 0..THREAD_ITERATIONS {
                        if (thread_index + iteration) % 2 == 0 {
                            let lookup = cloned
                                .routing_exact_postings_metadata(LABEL_NAME, LABEL_VALUE)
                                .unwrap();
                            assert_eq!(lookup.metadata, Some(expected_metadata));
                        } else {
                            assert_eq!(
                                cloned.metric_series_ranges(metric_sym).unwrap(),
                                expected_ranges
                            );
                        }
                    }
                    cloned.stats()
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let stats = handle.join().unwrap();
            assert!(stats.routing.calls > 0);
            assert!(stats.payload.calls > 0);
            assert_eq!(stats.root, SegmentIndexV7ReadCount::default());
            assert_category_sums(stats);
        }
    }
}
