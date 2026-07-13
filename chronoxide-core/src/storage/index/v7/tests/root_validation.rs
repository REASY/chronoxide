    #[test]
    fn segment_index_v7_root_decodes_valid_zero_minimal_and_410_layouts() {
        let cases = [
            ("zero", root_fixture(&SegmentIndexes::default()), 0, 0, 0),
            ("minimal", root_fixture(&minimal_indexes()), 1, 1, 1),
            (
                "410",
                root_fixture(&exact_boundary_indexes(410, true)),
                410,
                2,
                0,
            ),
        ];

        for (case, fixture, exact_entries, exact_pages, auxiliary_entries) in cases {
            let layout = decode_segment_indexes_v7_root(
                fixture.actual_file_len,
                &fixture.header,
                &fixture.trailer,
            )
            .unwrap_or_else(|error| panic!("{case}: {error}"));

            assert_eq!(layout.file_len, fixture.actual_file_len, "{case}");
            assert_eq!(layout.exact_entry_count, exact_entries, "{case}");
            assert_eq!(layout.exact_page_count, exact_pages, "{case}");
            assert_eq!(layout.auxiliary_entry_count, auxiliary_entries, "{case}");
            assert_eq!(
                layout.routing,
                locator_at(&fixture.trailer, TRAILER_ROUTING_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.metric,
                locator_at(&fixture.trailer, TRAILER_METRIC_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.exact_directory,
                locator_at(&fixture.trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.exact_pages,
                locator_at(&fixture.trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.exact_postings,
                locator_at(&fixture.trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.auxiliary_directory,
                locator_at(&fixture.trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET),
                "{case}"
            );
            assert_eq!(
                layout.auxiliary_payloads,
                locator_at(&fixture.trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET),
                "{case}"
            );
        }
    }

    #[test]
    fn segment_index_v7_root_allows_gaps_between_ordered_regions() {
        let mut fixture = root_fixture(&SegmentIndexes::default());
        fixture.actual_file_len = 1_024;
        put_u64(&mut fixture.trailer, TRAILER_FILE_LEN_OFFSET, 1_024);
        put_locator(&mut fixture.trailer, TRAILER_METRIC_LOCATOR_OFFSET, 16, 12);
        put_locator(
            &mut fixture.trailer,
            TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET,
            128,
            64,
        );
        put_locator(
            &mut fixture.trailer,
            TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
            512,
            64,
        );
        recompute_root_trailer_crc(&mut fixture.trailer);

        let layout = decode_segment_indexes_v7_root(
            fixture.actual_file_len,
            &fixture.header,
            &fixture.trailer,
        )
        .unwrap();

        assert_eq!(
            layout.metric,
            BlobLocator {
                offset: 16,
                len: 12
            }
        );
        assert_eq!(layout.exact_directory.offset, 128);
        assert_eq!(layout.auxiliary_directory.offset, 512);
    }

    #[test]
    fn segment_index_v7_root_rejects_header_field_mutations() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_HEADER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("magic", |header| {
                put_u32(header, 0, u32::from_le_bytes(*b"BAD!"))
            }),
            ("v6", |header| put_u16(header, 4, 6)),
            ("flags", |header| put_u16(header, 6, 1)),
            ("header length", |header| put_u32(header, 8, 15)),
            ("reserved", |header| put_u32(header, 12, 1)),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let mut fixture = valid.clone();
            mutate(&mut fixture.header);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_trailer_identity_and_reserved_mutations() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("magic", |trailer| {
                put_u32(trailer, 0, u32::from_le_bytes(*b"BAD!"))
            }),
            ("version", |trailer| put_u16(trailer, 4, 6)),
            ("flags", |trailer| put_u16(trailer, 6, 1)),
            ("trailer length", |trailer| put_u32(trailer, 8, 255)),
            ("reserved0", |trailer| put_u32(trailer, 12, 1)),
            ("reserved1", |trailer| trailer[164] = 1),
            ("terminal magic", |trailer| put_u32(trailer, 252, 0)),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let fixture = mutate_root_trailer(&valid, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_crc_and_file_length_mismatches() {
        let valid = root_fixture(&SegmentIndexes::default());

        let mut bad_crc = valid.clone();
        bad_crc.trailer[TRAILER_CRC_OFFSET] ^= 0x80;
        assert_invalid_root(&bad_crc, "trailer CRC");

        let mut wrong_actual_length = valid.clone();
        wrong_actual_length.actual_file_len += 1;
        assert_invalid_root(&wrong_actual_length, "actual file length");

        let wrong_recorded_length = mutate_root_trailer(&valid, |trailer| {
            put_u64(trailer, TRAILER_FILE_LEN_OFFSET, valid.actual_file_len + 1)
        });
        assert_invalid_root(&wrong_recorded_length, "recorded file length");

        let mut too_short = valid.clone();
        too_short.actual_file_len = 200;
        put_u64(&mut too_short.trailer, TRAILER_FILE_LEN_OFFSET, 200);
        recompute_root_trailer_crc(&mut too_short.trailer);
        assert_invalid_root(&too_short, "file shorter than fixed roots");
    }

    #[test]
    fn segment_index_v7_root_rejects_noncanonical_and_out_of_bounds_locators() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("routing offset only", |trailer| {
                put_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET, 16, 0)
            }),
            ("routing length only", |trailer| {
                put_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET, 0, 4)
            }),
            ("exact pages offset only", |trailer| {
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 28, 0)
            }),
            ("exact postings length only", |trailer| {
                put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 0, 8)
            }),
            ("auxiliary payload offset only", |trailer| {
                put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 28, 0)
            }),
            ("missing metric", |trailer| {
                put_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET, 0, 0)
            }),
            ("half metric", |trailer| {
                put_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET, 16, 0)
            }),
            ("missing exact directory", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 0, 0)
            }),
            ("half exact directory", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 0, 64)
            }),
            ("missing auxiliary directory", |trailer| {
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 0, 0)
            }),
            ("half auxiliary directory", |trailer| {
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 92, 0)
            }),
            ("before header", |trailer| {
                put_locator(trailer, TRAILER_METRIC_LOCATOR_OFFSET, 8, 12)
            }),
            ("past trailer", |trailer| {
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 150, 64)
            }),
            ("offset overflow", |trailer| {
                put_locator(trailer, TRAILER_ROUTING_LOCATOR_OFFSET, u64::MAX - 3, 8)
            }),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let fixture = mutate_root_trailer(&valid, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_overlapping_and_out_of_order_regions() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let cases: &[(&str, Mutation)] = &[
            ("overlap", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 20, 64)
            }),
            ("directory order", |trailer| {
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, 92, 64);
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, 28, 64);
            }),
        ];
        let valid = root_fixture(&SegmentIndexes::default());

        for (case, mutate) in cases {
            let fixture = mutate_root_trailer(&valid, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_count_locator_mismatches() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let zero_cases: &[(&str, Mutation)] = &[
            ("zero exact count with pages", |trailer| {
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 28, 16_384)
            }),
            ("zero exact count with postings", |trailer| {
                put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 28, 8)
            }),
            ("zero auxiliary count with payload", |trailer| {
                put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 28, 4)
            }),
        ];
        let zero = root_fixture(&SegmentIndexes::default());
        for (case, mutate) in zero_cases {
            let fixture = mutate_root_trailer(&zero, mutate);
            assert_invalid_root(&fixture, case);
        }

        let minimal_cases: &[(&str, Mutation)] = &[
            ("nonzero exact count without pages", |trailer| {
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 0, 0)
            }),
            ("nonzero exact count without postings", |trailer| {
                put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 0, 0)
            }),
            ("nonzero auxiliary count without payload", |trailer| {
                put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 0, 0)
            }),
        ];
        let minimal = root_fixture(&minimal_indexes());
        for (case, mutate) in minimal_cases {
            let fixture = mutate_root_trailer(&minimal, mutate);
            assert_invalid_root(&fixture, case);
        }
    }

    #[test]
    fn segment_index_v7_root_rejects_size_and_count_formula_mismatches() {
        type Mutation = fn(&mut [u8; SEGMENT_INDEX_V7_TRAILER_LEN]);
        let minimal_cases: &[(&str, Mutation)] = &[
            ("record length", |trailer| put_u32(trailer, 148, 39)),
            ("page length", |trailer| put_u32(trailer, 152, 16_383)),
            ("page count formula", |trailer| put_u32(trailer, 144, 2)),
            ("exact directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, offset, 95)
            }),
            ("exact pages length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, offset, 16_383)
            }),
            ("auxiliary directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, offset, 103)
            }),
        ];
        let minimal = root_fixture(&minimal_indexes());
        for (case, mutate) in minimal_cases {
            let fixture = mutate_root_trailer(&minimal, mutate);
            assert_invalid_root(&fixture, case);
        }

        let zero_cases: &[(&str, Mutation)] = &[
            ("empty exact directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET, offset, 63)
            }),
            ("empty auxiliary directory length", |trailer| {
                let (offset, _) = read_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
                put_locator(trailer, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET, offset, 63)
            }),
        ];
        let zero = root_fixture(&SegmentIndexes::default());
        for (case, mutate) in zero_cases {
            let fixture = mutate_root_trailer(&zero, mutate);
            assert_invalid_root(&fixture, case);
        }

        let fixture = mutate_root_trailer(
            &root_fixture(&exact_boundary_indexes(410, false)),
            |trailer| put_u32(trailer, 144, 1),
        );
        assert_invalid_root(&fixture, "410 page count formula");
    }

    #[test]
    fn segment_index_v7_root_rejects_impossible_counts_without_allocation() {
        let zero = root_fixture(&SegmentIndexes::default());
        let impossible_exact = mutate_root_trailer(&zero, |trailer| {
            put_u64(trailer, 136, u64::MAX);
            put_u32(trailer, 144, u32::MAX);
            put_locator(trailer, TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET, 28, 8);
            put_locator(trailer, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, 36, 16_384);
        });
        assert_invalid_root(&impossible_exact, "exact count exceeds page-count domain");

        let impossible_auxiliary = mutate_root_trailer(&zero, |trailer| {
            put_u32(trailer, 156, u32::MAX);
            put_locator(trailer, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET, 28, 4);
        });
        assert_invalid_root(
            &impossible_auxiliary,
            "auxiliary count requires impossible directory length",
        );
    }
