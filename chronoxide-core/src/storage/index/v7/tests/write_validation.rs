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
