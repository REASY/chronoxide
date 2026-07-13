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
