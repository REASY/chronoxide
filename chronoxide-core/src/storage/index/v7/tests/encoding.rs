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
