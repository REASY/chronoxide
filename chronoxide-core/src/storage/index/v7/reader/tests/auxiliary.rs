    #[test]
    fn segment_index_v7_reader_auxiliary_empty_initializes_required_directory_once() {
        let bytes = exact_reader_bytes(&[]);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        assert!(!reader.has_label_values().unwrap());
        assert!(reader.label_name_symbols().unwrap().is_empty());
        assert_eq!(reader.label_time_range(7).unwrap(), None);
        assert!(reader.label_values(7).unwrap().is_empty());
        assert!(
            reader
                .label_values_with_prefix(7, Some("missing"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(reader.label_value_time_range(7, 9).unwrap(), None);
        assert_eq!(reader.label_value_time_ranges(7).unwrap(), None);

        let stats = reader.stats();
        assert_eq!(
            stats.auxiliary_directory,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: 64,
            }
        );
        assert_eq!(stats.exact_directory, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.payload, SegmentIndexV7ReadCount::default());
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_fst_only_summary_is_canonical() {
        let mut symbols = SegmentSymbols::default();
        let name_sym = symbols.intern("service");
        let value_sym = symbols.intern("api");
        let series = vec![SeriesEntry {
            series_id: 1,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(name_sym, value_sym)],
        }];
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::from_series(&series, &symbols).unwrap(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };
        let mut bytes = Vec::new();
        super::super::write_segment_indexes_v7(&mut bytes, &indexes).unwrap();
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes.clone())).unwrap();
        assert_eq!(
            reader.label_time_range(name_sym).unwrap(),
            Some(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            })
        );

        let fst_record = auxiliary_record_offset(
            &bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            name_sym,
        );
        put_u64_at(&mut bytes, fst_record + 24, 1);
        refresh_auxiliary_directory_crc(&mut bytes);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(bytes)).unwrap();

        let error = reader.label_time_range(name_sym).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(reader.stats().payload.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_round_trips_metadata_fsts_and_time_ranges() {
        let fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        let service_fst = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        let service_ranges = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
            fixture.service_name_sym,
        );
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        assert!(reader.has_label_values().unwrap());
        assert_eq!(
            reader.label_name_symbols().unwrap(),
            vec![fixture.service_name_sym, fixture.zone_name_sym]
        );
        assert_eq!(
            reader.label_time_range(fixture.service_name_sym).unwrap(),
            Some(LabelValueTimeRange {
                min_time_ms: 100,
                max_time_ms: 399,
            })
        );
        assert_eq!(
            reader.label_time_range(fixture.time_only_name_sym).unwrap(),
            None
        );
        assert_eq!(
            reader.stats().auxiliary_directory,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: directory.len,
            }
        );
        assert_eq!(reader.stats().payload, SegmentIndexV7ReadCount::default());

        assert_eq!(
            reader.label_values(fixture.service_name_sym).unwrap(),
            vec!["api".to_string(), "worker".to_string()]
        );
        assert_eq!(
            reader
                .label_values_with_prefix(fixture.service_name_sym, Some("ap"))
                .unwrap(),
            vec!["api".to_string()]
        );
        assert!(reader.label_values(u32::MAX).unwrap().is_empty());
        assert_eq!(
            reader.stats().payload,
            SegmentIndexV7ReadCount {
                calls: 2,
                bytes: service_fst.len * 2,
            }
        );

        assert_eq!(
            reader
                .label_value_time_range(fixture.service_name_sym, fixture.api_value_sym)
                .unwrap(),
            Some(LabelValueTimeRange {
                min_time_ms: 100,
                max_time_ms: 199,
            })
        );
        assert_eq!(
            reader
                .label_value_time_ranges(fixture.service_name_sym)
                .unwrap(),
            Some(vec![
                (
                    fixture.api_value_sym,
                    LabelValueTimeRange {
                        min_time_ms: 100,
                        max_time_ms: 199,
                    },
                ),
                (
                    fixture.worker_value_sym,
                    LabelValueTimeRange {
                        min_time_ms: 300,
                        max_time_ms: 399,
                    },
                ),
            ])
        );
        assert_eq!(
            reader
                .label_value_time_ranges(fixture.time_only_name_sym)
                .unwrap(),
            Some(vec![(
                fixture.time_only_value_sym,
                LabelValueTimeRange {
                    min_time_ms: 700,
                    max_time_ms: 799,
                },
            )])
        );
        let stats = reader.stats();
        assert_eq!(
            stats.payload,
            SegmentIndexV7ReadCount {
                calls: 5,
                bytes: service_fst.len * 2 + service_ranges.len * 2 + 24,
            }
        );
        assert_eq!(stats.exact_directory, SegmentIndexV7ReadCount::default());
        assert_eq!(stats.exact_page, SegmentIndexV7ReadCount::default());
        assert_category_sums(stats);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_cache_is_shared_and_race_safe() {
        let fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        let source = CountingSource::new(fixture.bytes);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));
        let handles = (0..THREAD_COUNT)
            .map(|_| {
                let cloned = reader.try_clone_reader().unwrap();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    assert!(cloned.has_label_values().unwrap());
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
                .filter(|read| *read == (directory.offset, directory.len as usize))
                .count(),
            1
        );
        assert_eq!(
            stats
                .iter()
                .map(|stats| stats.auxiliary_directory.calls)
                .sum::<u64>(),
            1
        );
        assert_eq!(
            stats
                .iter()
                .map(|stats| stats.auxiliary_directory.bytes)
                .sum::<u64>(),
            directory.len
        );
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_errors_are_cached_across_clones() {
        let mut fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        put_u32_at(&mut fixture.bytes, directory.offset as usize, 0);
        refresh_auxiliary_directory_crc(&mut fixture.bytes);
        let source = CountingSource::new(fixture.bytes);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let cloned = reader.try_clone_reader().unwrap();

        let first = reader.has_label_values().unwrap_err();
        let second = cloned.label_name_symbols().unwrap_err();

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
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(cloned.stats().auxiliary_directory.calls, 0);

        let fixture = auxiliary_reader_fixture();
        let directory = locator(
            &fixture.bytes,
            super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
        );
        let source = CountingSource::failing_at(fixture.bytes, directory.offset);
        let probe = source.clone();
        let reader = SegmentIndexV7Reader::open(source).unwrap();
        let cloned = reader.try_clone_reader().unwrap();
        let first = reader.has_label_values().unwrap_err();
        let second = cloned.has_label_values().unwrap_err();
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
        assert_eq!(reader.stats().auxiliary_directory.calls, 0);
        assert_eq!(cloned.stats().auxiliary_directory.calls, 0);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_rejects_header_corruption() {
        enum Corruption {
            Magic,
            Version,
            Flags,
            HeaderLen,
            RecordLen,
            EntryCount,
            RecordsOffset,
            RecordsLen,
            Crc,
            Reserved,
        }
        let cases = [
            ("magic", Corruption::Magic),
            ("version", Corruption::Version),
            ("flags", Corruption::Flags),
            ("header len", Corruption::HeaderLen),
            ("record len", Corruption::RecordLen),
            ("entry count", Corruption::EntryCount),
            ("records offset", Corruption::RecordsOffset),
            ("records len", Corruption::RecordsLen),
            ("crc", Corruption::Crc),
            ("reserved", Corruption::Reserved),
        ];

        for (case, corruption) in cases {
            let mut fixture = auxiliary_reader_fixture();
            let directory = locator(
                &fixture.bytes,
                super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
            );
            let start = directory.offset as usize;
            let refresh_crc = !matches!(corruption, Corruption::Crc);
            match corruption {
                Corruption::Magic => put_u32_at(&mut fixture.bytes, start, 0),
                Corruption::Version => put_u16_at(&mut fixture.bytes, start + 4, 2),
                Corruption::Flags => put_u16_at(&mut fixture.bytes, start + 6, 1),
                Corruption::HeaderLen => put_u32_at(&mut fixture.bytes, start + 8, 63),
                Corruption::RecordLen => put_u32_at(&mut fixture.bytes, start + 12, 39),
                Corruption::EntryCount => put_u64_at(&mut fixture.bytes, start + 16, 6),
                Corruption::RecordsOffset => put_u64_at(&mut fixture.bytes, start + 24, 63),
                Corruption::RecordsLen => put_u64_at(&mut fixture.bytes, start + 32, 199),
                Corruption::Crc => {
                    let crc = read_u32_at(&fixture.bytes, start + 40);
                    put_u32_at(&mut fixture.bytes, start + 40, crc ^ 1);
                }
                Corruption::Reserved => fixture.bytes[start + 44] = 1,
            }
            if refresh_crc {
                refresh_auxiliary_directory_crc(&mut fixture.bytes);
            }
            let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

            let error = reader.has_label_values().unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(
                reader.stats().auxiliary_directory,
                SegmentIndexV7ReadCount {
                    calls: 1,
                    bytes: directory.len,
                },
                "{case}"
            );
            assert_eq!(reader.stats().payload.calls, 0, "{case}");
        }
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_directory_rejects_record_corruption() {
        enum Corruption {
            Kind,
            Flags,
            Duplicate,
            ZeroPayload,
            BeforePayloads,
            AfterPayloads,
            Overflow,
            ReversedTime,
            SummaryMismatch,
        }
        let cases = [
            ("kind", Corruption::Kind),
            ("flags", Corruption::Flags),
            ("duplicate", Corruption::Duplicate),
            ("zero payload", Corruption::ZeroPayload),
            ("before payloads", Corruption::BeforePayloads),
            ("after payloads", Corruption::AfterPayloads),
            ("overflow", Corruption::Overflow),
            ("reversed time", Corruption::ReversedTime),
            ("summary mismatch", Corruption::SummaryMismatch),
        ];

        for (case, corruption) in cases {
            let mut fixture = auxiliary_reader_fixture();
            let directory = locator(
                &fixture.bytes,
                super::super::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
            );
            let payloads = locator(
                &fixture.bytes,
                super::super::TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET,
            );
            let first_record = directory.offset as usize + 64;
            let second_record = first_record + 40;
            match corruption {
                Corruption::Kind => put_u16_at(&mut fixture.bytes, first_record, 4),
                Corruption::Flags => put_u16_at(&mut fixture.bytes, first_record + 2, 1),
                Corruption::Duplicate => {
                    let first_name = read_u32_at(&fixture.bytes, first_record + 4);
                    put_u32_at(&mut fixture.bytes, second_record + 4, first_name);
                }
                Corruption::ZeroPayload => put_u64_at(&mut fixture.bytes, first_record + 16, 0),
                Corruption::BeforePayloads => {
                    put_u64_at(&mut fixture.bytes, first_record + 8, payloads.offset - 1)
                }
                Corruption::AfterPayloads => put_u64_at(
                    &mut fixture.bytes,
                    first_record + 8,
                    payloads.offset + payloads.len,
                ),
                Corruption::Overflow => {
                    put_u64_at(&mut fixture.bytes, first_record + 8, u64::MAX - 3);
                    put_u64_at(&mut fixture.bytes, first_record + 16, 8);
                }
                Corruption::ReversedTime => {
                    put_u64_at(&mut fixture.bytes, first_record + 24, 2_000);
                    put_u64_at(&mut fixture.bytes, first_record + 32, 1_000);
                }
                Corruption::SummaryMismatch => {
                    let service_fst = auxiliary_record_offset(
                        &fixture.bytes,
                        super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
                        fixture.service_name_sym,
                    );
                    put_u64_at(&mut fixture.bytes, service_fst + 24, 101);
                }
            }
            refresh_auxiliary_directory_crc(&mut fixture.bytes);
            let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

            let error = reader.label_name_symbols().unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().auxiliary_directory.calls, 1, "{case}");
            assert_eq!(reader.stats().payload.calls, 0, "{case}");
        }
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_rejects_invalid_fst_after_one_payload_read() {
        let mut fixture = auxiliary_reader_fixture();
        let fst = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        fixture.bytes[fst.offset as usize..(fst.offset + fst.len) as usize].fill(0xff);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        let error = reader.label_values(fixture.service_name_sym).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(
            reader.stats().payload,
            SegmentIndexV7ReadCount {
                calls: 1,
                bytes: fst.len,
            }
        );
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_rejects_fst_without_values() {
        let mut fixture = auxiliary_reader_fixture();
        let record = auxiliary_record_offset(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        let fst = auxiliary_payload_locator(
            &fixture.bytes,
            super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            fixture.service_name_sym,
        );
        let empty_fst = fst::SetBuilder::memory().into_inner().unwrap();
        assert!(empty_fst.len() <= fst.len as usize);
        let start = fst.offset as usize;
        fixture.bytes[start..start + empty_fst.len()].copy_from_slice(&empty_fst);
        put_u64_at(&mut fixture.bytes, record + 16, empty_fst.len() as u64);
        refresh_auxiliary_directory_crc(&mut fixture.bytes);
        let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

        let error = reader.label_values(fixture.service_name_sym).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reader.stats().auxiliary_directory.calls, 1);
        assert_eq!(reader.stats().payload.calls, 1);
        assert_eq!(reader.stats().payload.bytes, empty_fst.len() as u64);
    }

    #[test]
    fn segment_index_v7_reader_auxiliary_rejects_time_range_payload_corruption() {
        enum Corruption {
            HugeCount,
            Truncated,
            Duplicate,
            Descending,
            ReversedAfterTarget,
            Empty,
            SummaryMismatch,
        }
        let cases = [
            ("huge count", Corruption::HugeCount),
            ("truncated", Corruption::Truncated),
            ("duplicate", Corruption::Duplicate),
            ("descending", Corruption::Descending),
            ("reversed after target", Corruption::ReversedAfterTarget),
            ("empty", Corruption::Empty),
            ("summary mismatch", Corruption::SummaryMismatch),
        ];

        for (case, corruption) in cases {
            let mut fixture = auxiliary_reader_fixture();
            let record = auxiliary_record_offset(
                &fixture.bytes,
                super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                fixture.service_name_sym,
            );
            let payload = auxiliary_payload_locator(
                &fixture.bytes,
                super::super::super::SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                fixture.service_name_sym,
            );
            let first_value = read_u32_at(&fixture.bytes, payload.offset as usize + 4);
            let second_record = payload.offset as usize + 24;
            let expected_read_len = match corruption {
                Corruption::HugeCount => {
                    put_u32_at(&mut fixture.bytes, payload.offset as usize, u32::MAX);
                    payload.len
                }
                Corruption::Truncated => {
                    put_u64_at(&mut fixture.bytes, record + 16, payload.len - 1);
                    refresh_auxiliary_directory_crc(&mut fixture.bytes);
                    payload.len - 1
                }
                Corruption::Duplicate => {
                    put_u32_at(&mut fixture.bytes, second_record, first_value);
                    payload.len
                }
                Corruption::Descending => {
                    put_u32_at(
                        &mut fixture.bytes,
                        second_record,
                        first_value.saturating_sub(1),
                    );
                    payload.len
                }
                Corruption::ReversedAfterTarget => {
                    put_u64_at(&mut fixture.bytes, second_record + 4, 900);
                    put_u64_at(&mut fixture.bytes, second_record + 12, 800);
                    payload.len
                }
                Corruption::Empty => {
                    put_u32_at(&mut fixture.bytes, payload.offset as usize, 0);
                    put_u64_at(&mut fixture.bytes, record + 16, 4);
                    refresh_auxiliary_directory_crc(&mut fixture.bytes);
                    4
                }
                Corruption::SummaryMismatch => {
                    put_u64_at(&mut fixture.bytes, payload.offset as usize + 8, 101);
                    payload.len
                }
            };
            let reader = SegmentIndexV7Reader::open(CountingSource::new(fixture.bytes)).unwrap();

            let error = reader
                .label_value_time_range(fixture.service_name_sym, fixture.api_value_sym)
                .unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}: {error}");
            assert_eq!(reader.stats().auxiliary_directory.calls, 1, "{case}");
            assert_eq!(
                reader.stats().payload,
                SegmentIndexV7ReadCount {
                    calls: 1,
                    bytes: expected_read_len,
                },
                "{case}"
            );
        }
    }
