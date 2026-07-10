use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::super::{
    MetricSeriesRange, MetricSeriesRangeIndex, ROUTING_INDEX_BUCKET_LEN, ROUTING_INDEX_HEADER_LEN,
    RoutingBucketRecord, RoutingIndexHeader, RoutingLookupResult, SegmentIndexReadAt,
    SegmentRoutingIndex, read_metric_series_ranges_blob, routing_key_bytes, routing_key_hash,
    validate_routing_bucket_key,
};
use super::{
    BlobLocator, SEGMENT_INDEX_V7_HEADER_LEN, SEGMENT_INDEX_V7_TRAILER_LEN, SegmentIndexV7Layout,
    decode_segment_indexes_v7_root,
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
            state: Arc::new(SegmentIndexV7ReaderState { root }),
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
        let file_offset = locator.offset.checked_add(relative_offset).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "segment index offset overflow")
        })?;
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
        read_exact_at_counted(
            self.source.as_ref(),
            &self.counters,
            category,
            file_offset,
            &mut bytes,
        )?;
        Ok(bytes)
    }
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

    use super::*;
    use crate::labels::METRIC_NAME_LABEL;
    use crate::storage::index::{
        ExactPostingsIndex, ExactPostingsMetadata, LabelValueFstIndex, LabelValueTimeRangeIndex,
        MetricSeriesRange, MetricSeriesRangeIndex, SegmentIndexes, SegmentRoutingIndex,
    };
    use crate::storage::series::{SERIES_KIND_FLOAT, SegmentSymbols};

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
