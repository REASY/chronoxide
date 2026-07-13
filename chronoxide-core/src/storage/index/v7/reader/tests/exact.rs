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
            selection.metadata(),
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
            Empty,
        }
        for (case, corruption) in [
            ("count", Corruption::Count),
            ("order", Corruption::Order),
            ("empty", Corruption::Empty),
        ] {
            let mut bytes = exact_reader_bytes(&[(12, vec![3, 7])]);
            let pages = locator(&bytes, super::super::TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
            let record = pages.offset as usize + 16;
            let payload = read_u64_at(&bytes, record + 8) as usize;
            match corruption {
                Corruption::Count => put_u32_at(&mut bytes, payload, u32::MAX),
                Corruption::Order => put_u32_at(&mut bytes, payload + 8, 3),
                Corruption::Empty => {
                    put_u32_at(&mut bytes, payload, 0);
                    put_u64_at(&mut bytes, record + 16, 4);
                    refresh_exact_page_crc(&mut bytes, 0);
                }
            }
            let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

            let error = reader.exact_postings(7, 12).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().exact_directory.calls, 1, "{case}");
            assert_eq!(reader.stats().exact_page.calls, 1, "{case}");
            assert_eq!(reader.stats().payload.calls, 1, "{case}");
        }
    }
