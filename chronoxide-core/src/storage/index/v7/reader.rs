use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crc32c::{crc32c, crc32c_append};
use fst::{Set, Streamer};

use super::super::{
    ExactPostingsMetadata, ExactPostingsSelection, LabelValueTimeRange, MetricSeriesRange,
    MetricSeriesRangeIndex, ROUTING_INDEX_BUCKET_LEN, ROUTING_INDEX_HEADER_LEN,
    RoutingBucketRecord, RoutingIndexHeader, RoutingLookupResult,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
    SegmentIndexReadAt, SegmentIndexReadCount as SegmentIndexV7ReadCount,
    SegmentIndexReadStats as SegmentIndexV7ReadStats, SegmentIndexes, SegmentRoutingIndex,
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

struct ValidatedExactPage<'a> {
    bytes: &'a [u8],
    record_count: usize,
}

impl ValidatedExactPage<'_> {
    fn record(&self, record_index: usize) -> ((u32, u32), ExactPostingsSelection) {
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

    fn selection(&self, key: (u32, u32)) -> Option<ExactPostingsSelection> {
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

    fn records(&self) -> impl Iterator<Item = ((u32, u32), ExactPostingsSelection)> + '_ {
        (0..self.record_count).map(|record_index| self.record(record_index))
    }
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

pub(in crate::storage::index) struct SegmentIndexV7Reader<R>
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
    pub(in crate::storage::index) fn open(source: R) -> io::Result<Self> {
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

    pub(in crate::storage::index) fn try_clone_reader(&self) -> io::Result<Self> {
        Ok(Self {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            counters: SegmentIndexV7ReadCounters::default(),
        })
    }

    pub(in crate::storage::index) fn stats(&self) -> SegmentIndexV7ReadStats {
        self.counters.snapshot()
    }

    pub(in crate::storage::index) fn routing_exact_postings_metadata(
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

    pub(in crate::storage::index) fn routing_index(
        &self,
    ) -> io::Result<Option<SegmentRoutingIndex>> {
        let locator = self.state.root.routing;
        if locator == BlobLocator::default() {
            return Ok(None);
        }
        let bytes = self.read_blob(locator, SegmentIndexV7ReadCategory::Routing)?;
        Ok(Some(SegmentRoutingIndex::decode(&bytes)?))
    }

    pub(in crate::storage::index) fn routing_index_byte_len(&self) -> Option<u64> {
        (self.state.root.routing != BlobLocator::default()).then_some(self.state.root.routing.len)
    }

    pub(in crate::storage::index) fn metric_series_ranges(
        &self,
        metric_sym: u32,
    ) -> io::Result<Vec<MetricSeriesRange>> {
        let index = self.metric_series_range_index()?;
        Ok(index.ranges(metric_sym).to_vec())
    }

    pub(in crate::storage::index) fn metric_series_range_index(
        &self,
    ) -> io::Result<MetricSeriesRangeIndex> {
        let bytes = self.read_blob(self.state.root.metric, SegmentIndexV7ReadCategory::Payload)?;
        read_metric_series_ranges_blob(&bytes)
    }

    pub(in crate::storage::index) fn metric_series_ranges_byte_len(&self) -> u64 {
        self.state.root.metric.len
    }

    pub(in crate::storage::index) fn exact_postings_metadata(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<ExactPostingsMetadata>> {
        Ok(self
            .exact_postings_selection(label_name_sym, label_value_sym)?
            .map(ExactPostingsSelection::metadata))
    }

    pub(in crate::storage::index) fn exact_postings(
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

    pub(in crate::storage::index) fn exact_postings_selection(
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
        let mut page = [0u8; EXACT_PAGE_LEN];
        let validated = self.read_validated_exact_page(page_index, descriptor, &mut page)?;
        Ok(validated.selection(key))
    }

    fn read_validated_exact_page<'a>(
        &self,
        page_index: usize,
        descriptor: ExactPageDescriptor,
        page: &'a mut [u8],
    ) -> io::Result<ValidatedExactPage<'a>> {
        if page.len() != EXACT_PAGE_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exact page scratch buffer has the wrong length",
            ));
        }
        let page_offset = u64::try_from(page_index)
            .ok()
            .and_then(|index| index.checked_mul(EXACT_PAGE_LEN as u64))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "exact page offset overflows")
            })?;
        self.read_blob_range_into(
            self.state.root.exact_pages,
            page_offset,
            page,
            SegmentIndexV7ReadCategory::ExactPage,
        )?;
        if crc32c(page) != descriptor.page_crc32c {
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

    pub(in crate::storage::index) fn read_exact_postings_selection(
        &self,
        selection: ExactPostingsSelection,
    ) -> io::Result<Vec<u32>> {
        let (postings_offset, postings_len) = selection.postings();
        let bytes = self.read_blob(
            BlobLocator {
                offset: postings_offset,
                len: postings_len,
            },
            SegmentIndexV7ReadCategory::Payload,
        )?;
        if bytes.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "exact postings payload is shorter than its count",
            ));
        }
        let count = read_u32_at(&bytes, 0) as usize;
        if count == 0 {
            return Err(invalid_exact_data("exact postings payload has no refs"));
        }
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
        let mut previous_ref = None;
        for offset in (4..bytes.len()).step_by(4) {
            let series_ref = read_u32_at(&bytes, offset);
            if previous_ref.is_some_and(|previous| previous >= series_ref) {
                return Err(invalid_exact_data(
                    "exact postings refs are not strictly ordered and unique",
                ));
            }
            previous_ref = Some(series_ref);
        }
        let mut refs = Vec::new();
        refs.try_reserve_exact(count)
            .map_err(|_| io::Error::other("exact postings allocation failed"))?;
        refs.extend(
            (4..bytes.len())
                .step_by(4)
                .map(|offset| read_u32_at(&bytes, offset)),
        );
        Ok(refs)
    }

    pub(in crate::storage::index) fn label_name_symbols(&self) -> io::Result<Vec<u32>> {
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

    pub(in crate::storage::index) fn has_label_values(&self) -> io::Result<bool> {
        Ok(self.auxiliary_directory()?.fst_count != 0)
    }

    pub(in crate::storage::index) fn label_time_range(
        &self,
        label_name_sym: u32,
    ) -> io::Result<Option<LabelValueTimeRange>> {
        Ok(self
            .auxiliary_directory()?
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, label_name_sym)
            .map(|record| record.time_range))
    }

    pub(in crate::storage::index) fn label_values(
        &self,
        label_name_sym: u32,
    ) -> io::Result<Vec<String>> {
        self.label_values_with_prefix(label_name_sym, None)
    }

    pub(in crate::storage::index) fn label_values_with_prefix(
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

    pub(in crate::storage::index) fn label_value_time_range(
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

    pub(in crate::storage::index) fn label_value_time_ranges(
        &self,
        label_name_sym: u32,
    ) -> io::Result<Option<Vec<(u32, LabelValueTimeRange)>>> {
        let Some(record) = self
            .auxiliary_directory()?
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, label_name_sym)
        else {
            return Ok(None);
        };
        Ok(Some(self.read_label_value_time_range_record(record)?))
    }

    fn read_label_value_time_range_record(
        &self,
        record: AuxiliaryRecord,
    ) -> io::Result<Vec<(u32, LabelValueTimeRange)>> {
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
        Ok(ranges)
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

    pub(in crate::storage::index) fn materialize(&self) -> io::Result<SegmentIndexes> {
        let routing_index = self.routing_index()?;
        let metric_series_ranges = self.metric_series_range_index()?;

        let mut exact_postings = super::super::ExactPostingsIndex::default();
        let exact_directory = self.exact_directory()?;
        let mut page_scratch = Vec::new();
        if !exact_directory.descriptors.is_empty() {
            page_scratch
                .try_reserve_exact(EXACT_PAGE_LEN)
                .map_err(|_| io::Error::other("exact page scratch allocation failed"))?;
            page_scratch.resize(EXACT_PAGE_LEN, 0);
        }
        for (page_index, descriptor) in exact_directory.descriptors.iter().copied().enumerate() {
            let page = self.read_validated_exact_page(
                page_index,
                descriptor,
                page_scratch.as_mut_slice(),
            )?;
            for ((label_name_sym, label_value_sym), selection) in page.records() {
                let refs = self.read_exact_postings_selection(selection)?;
                for series_ref in refs {
                    exact_postings.insert_monotonic(label_name_sym, label_value_sym, series_ref);
                }
            }
        }

        let mut label_values = super::super::LabelValueFstIndex::default();
        let mut label_value_time_ranges = super::super::LabelValueTimeRangeIndex::default();
        for record in self.auxiliary_directory()?.records.iter().copied() {
            match record.kind {
                SEGMENT_INDEX_BLOB_LABEL_VALUE_FST => {
                    let bytes =
                        self.read_blob(record.payload, SegmentIndexV7ReadCategory::Payload)?;
                    validate_materialized_fst(&bytes)?;
                    label_values.insert_fst(record.label_name_sym, bytes);
                }
                SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES => {
                    for (label_value_sym, range) in
                        self.read_label_value_time_range_record(record)?
                    {
                        label_value_time_ranges.insert(
                            record.label_name_sym,
                            label_value_sym,
                            range.min_time_ms,
                            range.max_time_ms,
                        );
                    }
                }
                _ => {
                    return Err(invalid_auxiliary_data(
                        "validated auxiliary directory contains an unsupported kind",
                    ));
                }
            }
        }

        Ok(SegmentIndexes {
            exact_postings,
            label_values,
            label_value_time_ranges,
            metric_series_ranges,
            routing_index,
        })
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

fn validate_materialized_fst(bytes: &[u8]) -> io::Result<()> {
    let set = Set::new(bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid label value FST: {error}"),
        )
    })?;
    if set.len() == 0 {
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
#[path = "reader/tests/mod.rs"]
mod tests;
