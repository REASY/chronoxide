    #[test]
    fn segment_index_v7_reader_materializes_empty_indexes() {
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &SegmentIndexes::default()).unwrap();
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        let actual = reader.materialize().unwrap();

        assert_eq!(actual, SegmentIndexes::default());
        let stats = reader.stats();
        assert_eq!(stats.root.calls, 2);
        assert_eq!(stats.root.bytes, 272);
        assert_eq!(stats.exact_directory.calls, 1);
        assert_eq!(stats.exact_directory.bytes, 64);
        assert_eq!(stats.exact_page.calls, 0);
        assert_eq!(stats.auxiliary_directory.calls, 1);
        assert_eq!(stats.auxiliary_directory.bytes, 64);
        assert_eq!(stats.routing.calls, 0);
        assert_eq!(stats.payload.calls, 1);
    }

    #[test]
    fn segment_index_v7_reader_materializes_mixed_indexes_with_exact_read_counts() {
        let (bytes, expected) = materialize_fixture(410);
        let source = CountingSource::new(bytes);
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let root = reader.state.root;

        let actual = reader.materialize().unwrap();

        assert_eq!(actual, expected);
        assert_eq!(actual.label_values.fsts, expected.label_values.fsts);
        let stats = reader.stats();
        assert_eq!(stats.root.calls, 2);
        assert_eq!(stats.root.bytes, 272);
        assert_eq!(stats.routing.calls, 1);
        assert_eq!(stats.routing.bytes, root.routing.len);
        assert_eq!(stats.exact_directory.calls, 1);
        assert_eq!(stats.exact_directory.bytes, root.exact_directory.len);
        assert_eq!(stats.exact_page.calls, u64::from(root.exact_page_count));
        assert_eq!(stats.exact_page.bytes, root.exact_pages.len);
        assert_eq!(stats.auxiliary_directory.calls, 1);
        assert_eq!(
            stats.auxiliary_directory.bytes,
            root.auxiliary_directory.len
        );
        assert_eq!(
            stats.payload.calls,
            1 + root.exact_entry_count + u64::from(root.auxiliary_entry_count)
        );
        assert_eq!(
            stats.payload.bytes,
            root.metric.len + root.exact_postings.len + root.auxiliary_payloads.len
        );
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_materialization_reaches_later_page_corruption() {
        let entries = exact_boundary_entries(410);
        let mut bytes = exact_reader_bytes(&entries);
        let pages = locator(&bytes, super::super::TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
        bytes[pages.offset as usize + EXACT_PAGE_LEN] ^= 1;
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        assert!(reader.exact_postings_metadata(7, 0).unwrap().is_some());
        let error = reader.materialize().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.stats().exact_directory.calls, 1);
        assert_eq!(reader.stats().exact_page.calls, 3);
    }
