use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use super::super::{
    ExactPostingsMetadata, ExactPostingsSelection, LabelValueTimeRange, MetricSeriesRange,
    MetricSeriesRangeIndex, ROUTING_INDEX_BUCKET_LEN, ROUTING_INDEX_HEADER_LEN,
    RoutingBucketRecord, RoutingIndexHeader, RoutingLookupResult,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
    SegmentIndexReadAt, SegmentIndexReadCount as SegmentIndexV7ReadCount,
    SegmentIndexReadStats as SegmentIndexV7ReadStats, SegmentIndexes, SegmentRoutingIndex,
    read_fst_values_with_prefix, read_metric_series_ranges_blob, routing_key_bytes,
    routing_key_hash, validate_routing_bucket_key,
};
use super::codec::{
    AuxiliaryDirectory, AuxiliaryRecord, ExactDirectory, ExactPageDescriptor, ValidatedExactPage,
    decode_auxiliary_directory, decode_exact_directory, decode_exact_postings,
    decode_label_value_time_ranges, validate_exact_page, validate_label_value_fst,
};
use super::{
    BlobLocator, EXACT_PAGE_LEN, SEGMENT_INDEX_V7_HEADER_LEN, SEGMENT_INDEX_V7_TRAILER_LEN,
    SegmentIndexV7Layout, decode_segment_indexes_v7_root,
};
#[cfg(test)]
use super::{EXACT_DIRECTORY_HEADER_LEN, EXACT_PAGE_DESCRIPTOR_LEN, EXACT_RECORD_LEN};

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
        decode_exact_directory(&bytes, self.state.root, None)
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
        validate_exact_page(page, page_index, descriptor, self.state.root, None)
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
        decode_exact_postings(&bytes)
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
        decode_label_value_time_ranges(&bytes, record.time_range, None)
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
        decode_auxiliary_directory(&bytes, self.state.root, None)
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
                    validate_label_value_fst(&bytes)?;
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
#[path = "reader/tests/mod.rs"]
mod tests;
