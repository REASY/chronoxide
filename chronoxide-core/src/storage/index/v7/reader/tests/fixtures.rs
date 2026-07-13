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

    fn materialize_fixture(exact_value_count: u32) -> (Vec<u8>, SegmentIndexes) {
        let mut symbols = SegmentSymbols::default();
        let label_name_sym = symbols.intern("label");
        let mut exact_postings = ExactPostingsIndex::default();
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        let mut series = Vec::new();
        for index in 0..exact_value_count {
            let value_sym = symbols.intern(&format!("value-{index:04}"));
            exact_postings.insert_monotonic(label_name_sym, value_sym, index);
            label_value_time_ranges.insert(
                label_name_sym,
                value_sym,
                1_000 + u64::from(index),
                2_000 + u64::from(index),
            );
            series.push(SeriesEntry {
                series_id: u64::from(index) + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(label_name_sym, value_sym)],
            });
        }
        let zone_name_sym = symbols.intern("zone");
        let zone_value_sym = symbols.intern("east");
        exact_postings.insert_monotonic(zone_name_sym, zone_value_sym, exact_value_count);
        label_value_time_ranges.insert(zone_name_sym, zone_value_sym, 3_000, 4_000);
        series.push(SeriesEntry {
            series_id: u64::from(exact_value_count) + 1,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(zone_name_sym, zone_value_sym)],
        });
        let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            zone_value_sym,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: exact_value_count + 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 1_000,
                max_time_ms: 4_000,
            },
        );
        let routing_index = Some(
            SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &label_value_time_ranges)
                .unwrap(),
        );
        let indexes = SegmentIndexes {
            exact_postings,
            label_values,
            label_value_time_ranges,
            metric_series_ranges,
            routing_index,
        };
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        (bytes, indexes)
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
