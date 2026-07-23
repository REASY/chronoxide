use super::*;

const CHUNK_FILE_LENS: [u64; 2] = [u32::MAX as u64 + 4_096; 2];

fn header_params(num_series: u32) -> SeriesHeaderV3Params {
    SeriesHeaderV3Params {
        num_series,
        num_keysets: if num_series == 0 { 0 } else { 1 },
        num_value_dicts: if num_series == 0 { 0 } else { 1 },
        chunk_index_root_crc32c: 0x1020_3040,
        keysets_len: if num_series == 0 { 8 } else { 16 },
        value_dicts_len: if num_series == 0 { 8 } else { 16 },
        keyset_blocks_len: if num_series == 0 { 8 } else { 16 },
        segment_start_ms: 1_000,
        segment_end_ms: u64::from(u32::MAX) + 1_001,
        chunk_index_file_len: if num_series == 0 { 64 } else { 1 << 20 },
    }
}

fn header(num_series: u32) -> SeriesHeaderV3 {
    SeriesHeaderV3::new(header_params(num_series)).unwrap()
}

fn inline_record(index: u32) -> SeriesHotV3 {
    SeriesHotV3 {
        series_id: u64::from(index) + 100,
        keyset_id: 0,
        row: index,
        kind_mask: 1 << CHUNK_KIND_FLOAT,
        location: SeriesHotLocationV3::Inline(InlineChunkV3 {
            chunk_kind: CHUNK_KIND_FLOAT,
            file_id: 0,
            scalar_lane_len: 0,
            min_time_delta_ms: index,
            max_time_delta_ms: index,
            file_offset: index,
            chunk_length: CHUNK_HEADER_LEN_V1,
            indexed_prefix_crc32c: 0xa0b0_c000 | index,
        }),
    }
}

fn update_root_crc(bytes: &mut [u8]) {
    put_u32(bytes, SERIES_ROOT_CRC_OFFSET_V3, 0);
    let crc = compute_series_root_crc32c(bytes).unwrap();
    put_u32(bytes, SERIES_ROOT_CRC_OFFSET_V3, crc);
}

#[test]
fn canonical_empty_header_has_fixed_golden_layout() {
    let header = header(0);
    assert_eq!(header.page_count, 0);
    assert_eq!(header.cold_page_count, 1);
    assert_eq!(header.directory_offset, 176);
    assert_eq!(header.directory_len, 16);
    assert_eq!(header.hot_pages_offset, 4_096);
    assert_eq!(header.hot_pages_len, 0);
    assert_eq!(header.keysets_offset, 4_096);
    assert_eq!(header.value_dicts_offset, 4_104);
    assert_eq!(header.keyset_blocks_offset, 4_112);
    assert_eq!(header.file_len, 4_120);

    let bytes = header.encode().unwrap();
    assert_eq!(
        &bytes[0..32],
        &[
            b'S', b'E', b'R', b'I', 3, 0, 0, 0, 176, 0, 0, 0, 16, 0, 0, 0, 0, 64, 0, 0, 24, 0, 0,
            0, 40, 0, 0, 0, 153, 1, 0, 0,
        ]
    );
    assert_eq!(read_u32(&bytes, 32), 0);
    assert_eq!(read_u32(&bytes, 36), 0);
    assert_eq!(read_u32(&bytes, 60), 1);
    assert_eq!(read_u64(&bytes, 64), 176);
    assert_eq!(read_u64(&bytes, 80), 4_096);
    assert_eq!(read_u64(&bytes, 96), 4_096);
    assert_eq!(read_u64(&bytes, 112), 4_104);
    assert_eq!(read_u64(&bytes, 128), 4_112);
    assert_eq!(read_u64(&bytes, 168), 4_120);
    assert_eq!(SeriesHeaderV3::decode(&bytes).unwrap(), header);

    let descriptor = SeriesColdPageDescriptorV1::new(header, 0, 0x5566_7788).unwrap();
    let (encoded_header, root) = encode_series_root_v3(header, &[], &[descriptor]).unwrap();
    assert_ne!(encoded_header.root_crc32c, 0);
    assert_eq!(root.len(), 4_096);
    assert_eq!(decode_series_root_v3(&root).unwrap().header, encoded_header);
}

#[test]
fn empty_header_rejects_each_noncanonical_empty_field() {
    let mut params = header_params(0);
    params.num_keysets = 1;
    assert!(SeriesHeaderV3::new(params).is_err());

    let mut params = header_params(0);
    params.num_value_dicts = 1;
    assert!(SeriesHeaderV3::new(params).is_err());

    let mut params = header_params(0);
    params.keysets_len = 9;
    assert!(SeriesHeaderV3::new(params).is_err());

    let mut params = header_params(0);
    params.value_dicts_len = 9;
    assert!(SeriesHeaderV3::new(params).is_err());

    let mut params = header_params(0);
    params.keyset_blocks_len = 9;
    assert!(SeriesHeaderV3::new(params).is_err());

    let mut params = header_params(0);
    params.chunk_index_file_len = CHUNK_INDEX_ROOT_LEN_V2 + 1;
    assert!(SeriesHeaderV3::new(params).is_err());
}

#[test]
fn nonempty_header_enforces_keyset_counts_and_cold_offset_table_minima() {
    let boundary = header_params(1);
    SeriesHeaderV3::new(boundary).unwrap();

    let mut params = boundary;
    params.num_keysets = 0;
    let error = SeriesHeaderV3::new(params).unwrap_err();
    assert!(error.to_string().contains("invalid keyset count"));

    let mut params = boundary;
    params.num_keysets = 2;
    params.keysets_len = 24;
    params.keyset_blocks_len = 24;
    let error = SeriesHeaderV3::new(params).unwrap_err();
    assert!(error.to_string().contains("invalid keyset count"));

    for (params, expected) in [
        (
            {
                let mut params = boundary;
                params.keysets_len = 15;
                params
            },
            "keysets section",
        ),
        (
            {
                let mut params = boundary;
                params.value_dicts_len = 15;
                params
            },
            "value dictionaries section",
        ),
        (
            {
                let mut params = boundary;
                params.keyset_blocks_len = 15;
                params
            },
            "keyset blocks section",
        ),
    ] {
        let error = SeriesHeaderV3::new(params).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    let encoded = header(1).encode().unwrap();
    let mut corrupted = encoded;
    put_u32(&mut corrupted, 40, 0);
    let error = SeriesHeaderV3::decode(&corrupted).unwrap_err();
    assert!(error.to_string().contains("invalid keyset count"));

    for (offset, expected) in [
        (104, "keysets section"),
        (120, "value dictionaries section"),
        (136, "keyset blocks section"),
    ] {
        let mut corrupted = encoded;
        put_u64(&mut corrupted, offset, 15);
        let error = SeriesHeaderV3::decode(&corrupted).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn header_page_arithmetic_is_exact_at_409_and_410_records() {
    let one_page = header(409);
    assert_eq!(one_page.page_count, 1);
    assert_eq!(one_page.hot_pages_len, 16_384);
    assert_eq!(one_page.expected_hot_record_count(0).unwrap(), 409);

    let two_pages = header(410);
    assert_eq!(two_pages.page_count, 2);
    assert_eq!(two_pages.hot_pages_len, 32_768);
    assert_eq!(two_pages.expected_hot_record_count(0).unwrap(), 409);
    assert_eq!(two_pages.expected_hot_record_count(1).unwrap(), 1);

    let records = (0..410).map(inline_record).collect::<Vec<_>>();
    let (first_descriptor, first_page) =
        encode_series_hot_page_v1(two_pages, 0, &records[..409], CHUNK_FILE_LENS).unwrap();
    let (second_descriptor, second_page) =
        encode_series_hot_page_v1(two_pages, 1, &records[409..], CHUNK_FILE_LENS).unwrap();
    assert_eq!(first_descriptor.record_count, 409);
    assert_eq!(second_descriptor.first_series_ref, 409);
    assert_eq!(second_descriptor.record_count, 1);
    assert_eq!(
        decode_series_hot_page_v1(two_pages, 0, first_descriptor, &first_page, CHUNK_FILE_LENS,)
            .unwrap()
            .records,
        records[..409],
    );
    assert_eq!(
        decode_series_hot_page_v1(
            two_pages,
            1,
            second_descriptor,
            &second_page,
            CHUNK_FILE_LENS,
        )
        .unwrap()
        .records,
        records[409..],
    );
}

#[test]
fn inline_record_has_exact_golden_bytes() {
    let header = header(1);
    let context = SeriesHotV3Context::from_header(header, CHUNK_FILE_LENS).unwrap();
    let record = SeriesHotV3 {
        series_id: 0x0807_0605_0403_0201,
        keyset_id: 0x0c0b_0a09,
        row: 0x100f_0e0d,
        kind_mask: 1 << CHUNK_KIND_HISTOGRAM,
        location: SeriesHotLocationV3::Inline(InlineChunkV3 {
            chunk_kind: CHUNK_KIND_HISTOGRAM,
            file_id: 1,
            scalar_lane_len: 16,
            min_time_delta_ms: 0x1413_1211,
            max_time_delta_ms: 0x1817_1615,
            file_offset: 0x1c1b_1a19,
            chunk_length: 0x201f_1e1d,
            indexed_prefix_crc32c: 0x2423_2221,
        }),
    };
    let bytes = record.encode(context).unwrap();
    assert_eq!(
        bytes,
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 68, 131, 0, 0, 17, 18, 19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
        ]
    );
    assert_eq!(SeriesHotV3::decode(&bytes, context).unwrap(), record);
    let SeriesHotLocationV3::Inline(inline) = record.location else {
        panic!("expected inline record");
    };
    assert_eq!(inline.scalar_lane_offset(), 40);
}

#[test]
fn overflow_record_has_exact_golden_bytes() {
    let header = header(1);
    let context = SeriesHotV3Context::from_header(header, CHUNK_FILE_LENS).unwrap();
    let record = SeriesHotV3 {
        series_id: 0x0807_0605_0403_0201,
        keyset_id: 0x0c0b_0a09,
        row: 0x100f_0e0d,
        kind_mask: 0b1_0101,
        location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
            blob_offset: 64,
            blob_len: 32 + 44,
            chunk_count: 1,
        }),
    };
    let bytes = record.encode(context).unwrap();
    assert_eq!(
        bytes,
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0x15, 0x04, 0, 0, 64, 0, 0, 0,
            0, 0, 0, 0, 76, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
    assert_eq!(SeriesHotV3::decode(&bytes, context).unwrap(), record);
}

#[test]
fn inline_records_accept_all_five_chunk_kinds() {
    let context = SeriesHotV3Context::from_header(header(5), CHUNK_FILE_LENS).unwrap();
    for chunk_kind in [
        CHUNK_KIND_FLOAT,
        CHUNK_KIND_INT64,
        CHUNK_KIND_HISTOGRAM,
        CHUNK_KIND_EXPONENTIAL_HISTOGRAM,
        CHUNK_KIND_SUMMARY,
    ] {
        let record = SeriesHotV3 {
            series_id: u64::from(chunk_kind),
            keyset_id: 0,
            row: u32::from(chunk_kind),
            kind_mask: 1 << chunk_kind,
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind,
                file_id: 0,
                scalar_lane_len: 0,
                min_time_delta_ms: 0,
                max_time_delta_ms: 0,
                file_offset: 0,
                chunk_length: CHUNK_HEADER_LEN_V1,
                indexed_prefix_crc32c: 0,
            }),
        };
        let bytes = record.encode(context).unwrap();
        assert_eq!(SeriesHotV3::decode(&bytes, context).unwrap(), record);
    }
}

#[test]
fn hot_records_reject_invalid_tags_and_reserved_overflow_fields() {
    let context = SeriesHotV3Context::from_header(header(1), CHUNK_FILE_LENS).unwrap();
    let mut bytes = inline_record(0).encode(context).unwrap();
    let control = read_u32(&bytes, 16);
    put_u32(
        &mut bytes,
        16,
        control & !(SERIES_HOT_TAG_MASK << SERIES_HOT_TAG_SHIFT),
    );
    assert!(SeriesHotV3::decode(&bytes, context).is_err());
    put_u32(
        &mut bytes,
        16,
        (control & !(SERIES_HOT_TAG_MASK << SERIES_HOT_TAG_SHIFT)) | (3 << SERIES_HOT_TAG_SHIFT),
    );
    assert!(SeriesHotV3::decode(&bytes, context).is_err());

    let overflow = SeriesHotV3 {
        series_id: 7,
        keyset_id: 0,
        row: 0,
        kind_mask: 1,
        location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
            blob_offset: 64,
            blob_len: 32 + 44,
            chunk_count: 1,
        }),
    };
    let mut bytes = overflow.encode(context).unwrap();
    let control = read_u32(&bytes, 16);
    put_u32(&mut bytes, 16, control | (1 << SERIES_HOT_FILE_ID_SHIFT));
    assert!(SeriesHotV3::decode(&bytes, context).is_err());
    put_u32(&mut bytes, 16, control);
    put_u32(&mut bytes, 36, 1);
    assert!(SeriesHotV3::decode(&bytes, context).is_err());
}

#[test]
fn inline_scalar_width_and_shape_are_canonical() {
    let context = SeriesHotV3Context::from_header(header(1), CHUNK_FILE_LENS).unwrap();
    let base = SeriesHotV3 {
        series_id: 1,
        keyset_id: 0,
        row: 0,
        kind_mask: 1 << CHUNK_KIND_HISTOGRAM,
        location: SeriesHotLocationV3::Inline(InlineChunkV3 {
            chunk_kind: CHUNK_KIND_HISTOGRAM,
            file_id: 0,
            scalar_lane_len: 16,
            min_time_delta_ms: 0,
            max_time_delta_ms: 1,
            file_offset: 0,
            chunk_length: 56,
            indexed_prefix_crc32c: 9,
        }),
    };
    assert!(base.encode(context).is_ok());

    let with_scalar_len = |scalar_lane_len: u32, chunk_length: u32| SeriesHotV3 {
        location: SeriesHotLocationV3::Inline(InlineChunkV3 {
            scalar_lane_len,
            chunk_length,
            ..match base.location {
                SeriesHotLocationV3::Inline(inline) => inline,
                SeriesHotLocationV3::Overflow(_) => unreachable!(),
            }
        }),
        ..base
    };
    assert!(with_scalar_len(15, 55).encode(context).is_err());
    assert!(
        with_scalar_len(
            SERIES_HOT_SCALAR_LANE_LEN_MAX,
            CHUNK_HEADER_LEN_V1 + SERIES_HOT_SCALAR_LANE_LEN_MAX,
        )
        .encode(context)
        .is_ok()
    );
    assert!(
        with_scalar_len(SERIES_HOT_SCALAR_LANE_LEN_MAX + 1, u32::MAX)
            .encode(context)
            .is_err()
    );
    assert!(with_scalar_len(16, 55).encode(context).is_err());

    let scalar_float = SeriesHotV3 {
        kind_mask: 1 << CHUNK_KIND_FLOAT,
        location: SeriesHotLocationV3::Inline(InlineChunkV3 {
            chunk_kind: CHUNK_KIND_FLOAT,
            ..match base.location {
                SeriesHotLocationV3::Inline(inline) => inline,
                SeriesHotLocationV3::Overflow(_) => unreachable!(),
            }
        }),
        ..base
    };
    assert!(scalar_float.encode(context).is_err());
}

#[test]
fn root_and_hot_page_reject_authenticated_nonzero_padding_and_reserved_bytes() {
    let header = header(1);
    let records = [inline_record(0)];
    let (hot_descriptor, mut page) =
        encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
    let cold_descriptor = SeriesColdPageDescriptorV1::new(header, 0, 0).unwrap();
    let (_, mut root) =
        encode_series_root_v3(header, &[hot_descriptor], &[cold_descriptor]).unwrap();

    let mut bad_root_crc = root.clone();
    bad_root_crc[SERIES_ROOT_CRC_OFFSET_V3] ^= 1;
    assert!(decode_series_root_v3(&bad_root_crc).is_err());

    let mut bad_page_crc = page.clone();
    bad_page_crc[SERIES_HOT_PAGE_HEADER_LEN_V1] ^= 1;
    assert!(
        decode_series_hot_page_v1(header, 0, hot_descriptor, &bad_page_crc, CHUNK_FILE_LENS,)
            .is_err()
    );

    let directory_end = SERIES_HEADER_LEN_V3 + 2 * SERIES_DESCRIPTOR_LEN_V1;
    root[directory_end] = 1;
    update_root_crc(&mut root);
    assert!(decode_series_root_v3(&root).is_err());

    let (_, mut root) =
        encode_series_root_v3(header, &[hot_descriptor], &[cold_descriptor]).unwrap();
    put_u32(&mut root, SERIES_HEADER_LEN_V3 + 12, 1);
    update_root_crc(&mut root);
    assert!(decode_series_root_v3(&root).is_err());

    let (_, mut root) =
        encode_series_root_v3(header, &[hot_descriptor], &[cold_descriptor]).unwrap();
    put_u32(
        &mut root,
        SERIES_HEADER_LEN_V3 + SERIES_DESCRIPTOR_LEN_V1 + 12,
        1,
    );
    update_root_crc(&mut root);
    assert!(decode_series_root_v3(&root).is_err());

    let padding_start = SERIES_HOT_PAGE_HEADER_LEN_V1 + SERIES_HOT_RECORD_LEN_V3;
    page[padding_start] = 1;
    let descriptor = SeriesHotPageDescriptorV1 {
        page_crc32c: crc32c(&page),
        ..hot_descriptor
    };
    assert!(decode_series_hot_page_v1(header, 0, descriptor, &page, CHUNK_FILE_LENS).is_err());

    page[padding_start] = 0;
    put_u32(&mut page, 20, 1);
    let descriptor = SeriesHotPageDescriptorV1 {
        page_crc32c: crc32c(&page),
        ..hot_descriptor
    };
    assert!(decode_series_hot_page_v1(header, 0, descriptor, &page, CHUNK_FILE_LENS).is_err());
}

#[test]
fn checked_header_and_record_arithmetic_rejects_overflow_and_bounds_errors() {
    let mut params = header_params(1);
    params.keysets_len = u64::MAX;
    assert!(SeriesHeaderV3::new(params).is_err());

    let context = SeriesHotV3Context::from_header(header(1), CHUNK_FILE_LENS).unwrap();
    let mut record = inline_record(0);
    let SeriesHotLocationV3::Inline(ref mut inline) = record.location else {
        unreachable!();
    };
    inline.file_id = 1;
    inline.file_offset = (1 << 20) - 39;
    let narrow_file_context = SeriesHotV3Context {
        chunk_file_lens: [CHUNK_FILE_LENS[0], 1 << 20],
        ..context
    };
    assert!(record.encode(narrow_file_context).is_err());

    let overflowing_time_context = SeriesHotV3Context {
        segment_start_ms: u64::MAX - 5,
        segment_end_ms: u64::MAX,
        chunk_file_lens: CHUNK_FILE_LENS,
        chunk_index_file_len: 1 << 20,
    };
    let mut record = inline_record(0);
    let SeriesHotLocationV3::Inline(ref mut inline) = record.location else {
        unreachable!();
    };
    inline.min_time_delta_ms = 6;
    inline.max_time_delta_ms = 6;
    assert!(record.encode(overflowing_time_context).is_err());

    let exact_u32_boundary = SeriesHotV3 {
        location: SeriesHotLocationV3::Inline(InlineChunkV3 {
            min_time_delta_ms: u32::MAX,
            max_time_delta_ms: u32::MAX,
            file_offset: u32::MAX,
            chunk_length: CHUNK_HEADER_LEN_V1,
            ..match inline_record(0).location {
                SeriesHotLocationV3::Inline(inline) => inline,
                SeriesHotLocationV3::Overflow(_) => unreachable!(),
            }
        }),
        ..inline_record(0)
    };
    assert!(exact_u32_boundary.encode(context).is_ok());

    let too_many_chunks = u32::MAX / CHUNK_OVERFLOW_ENTRY_LEN_V1 as u32;
    let overflow = SeriesHotV3 {
        series_id: 1,
        keyset_id: 0,
        row: 0,
        kind_mask: 1,
        location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
            blob_offset: 64,
            blob_len: u32::MAX,
            chunk_count: too_many_chunks,
        }),
    };
    assert!(overflow.encode(context).is_err());
}
