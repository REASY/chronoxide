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
        #[derive(Clone, Copy)]
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
