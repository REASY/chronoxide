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
