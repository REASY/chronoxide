use super::*;
use std::fmt::Write as _;

const ROOT_SERIES_COUNT_OFFSET: usize = 20;
const ROOT_BLOB_COUNT_OFFSET: usize = 24;
const ROOT_RESERVED0_OFFSET: usize = 28;
const ROOT_BLOBS_LEN_OFFSET: usize = 40;
const ROOT_FILE_LEN_OFFSET: usize = 48;
const BLOB_SERIES_REF_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 12;
const BLOB_CHUNK_COUNT_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 16;
const BLOB_RESERVED0_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 20;
const BLOB_BODY_LEN_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + 24;
const FIRST_ENTRY_OFFSET: usize = CHUNK_OVERFLOW_ROOT_V2_LEN + CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN;

fn scalar_entry(
    file_id: u8,
    kind: ChunkKind,
    min_time_ms: u64,
    max_time_ms: u64,
    offset: u64,
) -> OverflowChunkEntryV1 {
    OverflowChunkEntryV1 {
        file_id,
        kind,
        min_time_ms,
        max_time_ms,
        offset,
        length: 72,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
        indexed_prefix_crc32c: 0x89ab_cdef,
    }
}

fn typed_entry(
    file_id: u8,
    kind: ChunkKind,
    min_time_ms: u64,
    max_time_ms: u64,
    offset: u64,
) -> OverflowChunkEntryV1 {
    OverflowChunkEntryV1 {
        file_id,
        kind,
        min_time_ms,
        max_time_ms,
        offset,
        length: 104,
        scalar_lane_offset: 40,
        scalar_lane_len: 24,
        indexed_prefix_crc32c: 0x7654_3210,
    }
}

fn one_blob_file() -> EncodedChunkIndexV2 {
    encode_chunk_index_v2(
        3,
        &[ChunkOverflowBlobV1 {
            series_ref: 2,
            entries: vec![typed_entry(
                1,
                ChunkKind::ExponentialHistogram,
                1_700_000_000_123,
                1_700_000_004_567,
                0x1020_3040,
            )],
        }],
    )
    .unwrap()
}

fn reseal_root(bytes: &mut [u8]) {
    put_u32(bytes, ROOT_CRC_OFFSET, 0);
    let crc = crc32c(&bytes[..CHUNK_OVERFLOW_ROOT_V2_LEN]);
    put_u32(bytes, ROOT_CRC_OFFSET, crc);
}

fn reseal_blob(bytes: &mut [u8], blob_offset: usize, blob_len: usize) {
    let crc_offset = blob_offset + BLOB_CRC_OFFSET;
    put_u32(bytes, crc_offset, 0);
    let crc = crc32c(&bytes[blob_offset..blob_offset + blob_len]);
    put_u32(bytes, crc_offset, crc);
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded
}

#[test]
fn empty_file_has_deterministic_golden_bytes() {
    let encoded = encode_chunk_index_v2(0, &[]).unwrap();

    assert_eq!(encoded.bytes.len(), CHUNK_OVERFLOW_ROOT_V2_LEN);
    assert_eq!(encoded.root.series_count, 0);
    assert_eq!(encoded.root.blob_count, 0);
    assert_eq!(encoded.root.blobs_len, 0);
    assert_eq!(encoded.root.file_len, 64);
    assert_eq!(encoded.root.root_crc32c, 0x22c9_9139);
    assert!(encoded.blob_locators.is_empty());
    assert_eq!(
        hex(&encoded.bytes),
        concat!(
            "434849580200000040000000200000002c000000000000000000000000000000",
            "4000000000000000000000000000000040000000000000003991c92200000000",
        )
    );
    assert_eq!(
        decode_chunk_index_v2(&encoded.bytes).unwrap(),
        DecodedChunkIndexV2 {
            root: encoded.root,
            blobs: Vec::new(),
            blob_locators: Vec::new(),
        }
    );
}

#[test]
fn one_blob_has_exact_layout_and_round_trips() {
    let encoded = one_blob_file();

    assert_eq!(encoded.bytes.len(), 64 + 32 + 44);
    assert_eq!(encoded.root.series_count, 3);
    assert_eq!(encoded.root.blob_count, 1);
    assert_eq!(encoded.root.blobs_len, 76);
    assert_eq!(encoded.root.file_len, 140);
    assert_eq!(encoded.root.root_crc32c, 0xc602_2e0a);
    assert_eq!(read_u32_at(&encoded.bytes, 28), 0);
    assert_eq!(read_u32_at(&encoded.bytes, 60), 0);
    assert_eq!(read_u32_at(&encoded.bytes, 64), CHUNK_OVERFLOW_BLOB_MAGIC);
    assert_eq!(read_u32_at(&encoded.bytes, 72), 32);
    assert_eq!(read_u32_at(&encoded.bytes, 76), 2);
    assert_eq!(read_u32_at(&encoded.bytes, 80), 1);
    assert_eq!(read_u32_at(&encoded.bytes, 84), 0);
    assert_eq!(read_u32_at(&encoded.bytes, 88), 44);
    assert_eq!(read_u32_at(&encoded.bytes, 92), 0x4ec4_4678);
    assert_eq!(read_u16_at(&encoded.bytes, FIRST_ENTRY_OFFSET + 2), 0);
    assert_eq!(
        hex(&encoded.bytes),
        concat!(
            "434849580200000040000000200000002c000000030000000100000000000000",
            "40000000000000004c000000000000008c000000000000000a2e02c600000000",
            "434f463701000000200000000200000001000000000000002c0000007846c44e",
            "010300007b68e5cf8b010000d779e5cf8b010000403020100000000068000000",
            "280000001800000010325476",
        )
    );
    assert_eq!(
        encoded.blob_locators,
        vec![ChunkOverflowBlobLocatorV1 {
            series_ref: 2,
            blob_offset: 64,
            blob_len: 76,
            chunk_count: 1,
        }]
    );

    let decoded = decode_chunk_index_v2(&encoded.bytes).unwrap();
    assert_eq!(decoded.root, encoded.root);
    assert_eq!(decoded.blob_locators, encoded.blob_locators);
    assert_eq!(decoded.blobs[0].series_ref, 2);
    assert_eq!(decoded.blobs[0].entries.len(), 1);
    assert_eq!(
        decoded.blobs[0].entries[0],
        typed_entry(
            1,
            ChunkKind::ExponentialHistogram,
            1_700_000_000_123,
            1_700_000_004_567,
            0x1020_3040,
        )
    );
    assert_eq!(
        decode_touched_chunk_overflow_blob_v1(
            &encoded.bytes[64..],
            &encoded.root,
            encoded.blob_locators[0],
        )
        .unwrap(),
        decoded.blobs[0]
    );
}

#[test]
fn touched_blob_must_match_the_hot_record_locator() {
    let encoded = one_blob_file();
    let bytes = &encoded.bytes[64..];
    let locator = encoded.blob_locators[0];

    for mismatched in [
        ChunkOverflowBlobLocatorV1 {
            series_ref: 1,
            ..locator
        },
        ChunkOverflowBlobLocatorV1 {
            blob_len: locator.blob_len + 1,
            ..locator
        },
        ChunkOverflowBlobLocatorV1 {
            chunk_count: locator.chunk_count + 1,
            ..locator
        },
        ChunkOverflowBlobLocatorV1 {
            blob_offset: 63,
            ..locator
        },
        ChunkOverflowBlobLocatorV1 {
            blob_offset: encoded.root.file_len,
            ..locator
        },
    ] {
        assert_eq!(
            decode_touched_chunk_overflow_blob_v1(bytes, &encoded.root, mismatched)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}

#[test]
fn multiple_blobs_and_mixed_lanes_round_trip_without_reordering() {
    let blobs = vec![
        ChunkOverflowBlobV1 {
            series_ref: 0,
            entries: vec![
                scalar_entry(0, ChunkKind::Float, 10, 20, 100),
                typed_entry(0, ChunkKind::Histogram, 21, 30, 200),
                typed_entry(1, ChunkKind::Summary, 10, 20, 300),
            ],
        },
        ChunkOverflowBlobV1 {
            series_ref: 4,
            entries: vec![scalar_entry(1, ChunkKind::Int64, 100, 100, 400)],
        },
    ];

    let encoded = encode_chunk_index_v2(5, &blobs).unwrap();
    let decoded = decode_chunk_index_v2(&encoded.bytes).unwrap();

    assert_eq!(decoded.blobs, blobs);
    assert_eq!(decoded.root.blob_count, 2);
    assert_eq!(decoded.blob_locators[0].blob_offset, 64);
    assert_eq!(decoded.blob_locators[0].blob_len, 164);
    assert_eq!(decoded.blob_locators[1].blob_offset, 228);
    assert_eq!(decoded.blob_locators[1].blob_len, 76);
}

#[test]
fn blob_length_boundaries_follow_the_u32_contract() {
    const MAX_CHUNK_COUNT: u32 = 97_612_892;

    assert_eq!(checked_chunk_overflow_blob_len(0), Some(32));
    assert_eq!(checked_chunk_overflow_blob_len(1), Some(76));
    assert_eq!(
        checked_chunk_overflow_blob_len(MAX_CHUNK_COUNT),
        Some(4_294_967_280)
    );
    assert_eq!(checked_chunk_overflow_blob_len(MAX_CHUNK_COUNT + 1), None);
}

#[test]
fn maximum_valid_entry_widths_round_trip() {
    let entry = OverflowChunkEntryV1 {
        file_id: 1,
        kind: ChunkKind::Summary,
        min_time_ms: u64::MAX,
        max_time_ms: u64::MAX,
        offset: u64::MAX - u64::from(u32::MAX),
        length: u32::MAX,
        scalar_lane_offset: 40,
        scalar_lane_len: u32::MAX - 40,
        indexed_prefix_crc32c: u32::MAX,
    };
    let blob = ChunkOverflowBlobV1 {
        series_ref: u32::MAX - 1,
        entries: vec![entry],
    };

    let encoded = encode_chunk_index_v2(u32::MAX, std::slice::from_ref(&blob)).unwrap();
    let decoded = decode_chunk_index_v2(&encoded.bytes).unwrap();

    assert_eq!(decoded.blobs, vec![blob]);
}

#[test]
fn writer_rejects_noncanonical_blob_and_entry_ordering() {
    let descending_blobs = vec![
        ChunkOverflowBlobV1 {
            series_ref: 2,
            entries: vec![scalar_entry(0, ChunkKind::Float, 1, 2, 3)],
        },
        ChunkOverflowBlobV1 {
            series_ref: 1,
            entries: vec![scalar_entry(0, ChunkKind::Float, 1, 2, 3)],
        },
    ];
    assert_eq!(
        encode_chunk_index_v2(3, &descending_blobs)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );

    let duplicate_entries = vec![
        scalar_entry(0, ChunkKind::Float, 1, 2, 3),
        scalar_entry(0, ChunkKind::Int64, 1, 2, 3),
    ];
    assert_eq!(
        encode_chunk_index_v2(
            1,
            &[ChunkOverflowBlobV1 {
                series_ref: 0,
                entries: duplicate_entries,
            }],
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn reader_rejects_noncanonical_entry_order_after_valid_crc() {
    let blob = ChunkOverflowBlobV1 {
        series_ref: 0,
        entries: vec![
            scalar_entry(0, ChunkKind::Float, 1, 2, 3),
            scalar_entry(0, ChunkKind::Float, 4, 5, 6),
        ],
    };
    let mut bytes = encode_chunk_index_v2(1, &[blob]).unwrap().bytes;
    let second_entry = FIRST_ENTRY_OFFSET + OVERFLOW_CHUNK_ENTRY_V1_LEN;
    bytes.copy_within(FIRST_ENTRY_OFFSET..FIRST_ENTRY_OFFSET + 44, second_entry);
    let blob_len = bytes.len() - CHUNK_OVERFLOW_ROOT_V2_LEN;
    reseal_blob(&mut bytes, CHUNK_OVERFLOW_ROOT_V2_LEN, blob_len);

    let error = decode_chunk_index_v2(&bytes).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("strictly ordered"));
}

#[test]
fn reader_rejects_noncanonical_blob_order_after_valid_crcs() {
    let blobs = vec![
        ChunkOverflowBlobV1 {
            series_ref: 0,
            entries: vec![scalar_entry(0, ChunkKind::Float, 1, 2, 3)],
        },
        ChunkOverflowBlobV1 {
            series_ref: 1,
            entries: vec![scalar_entry(0, ChunkKind::Float, 4, 5, 6)],
        },
    ];
    let encoded = encode_chunk_index_v2(2, &blobs).unwrap();
    let mut bytes = encoded.bytes;
    let second_blob = encoded.blob_locators[1];
    put_u32(&mut bytes, second_blob.blob_offset as usize + 12, 0);
    reseal_blob(
        &mut bytes,
        second_blob.blob_offset as usize,
        second_blob.blob_len as usize,
    );

    let error = decode_chunk_index_v2(&bytes).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("strictly ordered"));
}

#[test]
fn checksum_corruption_is_not_treated_as_absence() {
    let mut root_corruption = one_blob_file().bytes;
    root_corruption[8] ^= 1;
    let error = decode_chunk_index_v2(&root_corruption).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("root crc"));

    let mut blob_corruption = one_blob_file().bytes;
    blob_corruption[FIRST_ENTRY_OFFSET + 40] ^= 1;
    let error = decode_chunk_index_v2(&blob_corruption).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("blob crc"));
}

#[test]
fn fixed_root_fields_and_reserved_words_are_enforced_after_valid_crc() {
    let mutations = [
        (0, 0u64, 4usize),
        (4, 3, 2),
        (6, 1u64, 2usize),
        (8, 65, 4),
        (12, 31, 4),
        (16, 40, 4),
        (ROOT_RESERVED0_OFFSET, 1, 4),
        (32, 65, 8),
        (60, 1, 4),
    ];
    for (offset, value, width) in mutations {
        let mut bytes = one_blob_file().bytes;
        match width {
            2 => put_u16(&mut bytes, offset, value as u16),
            4 => put_u32(&mut bytes, offset, value as u32),
            8 => put_u64(&mut bytes, offset, value),
            _ => unreachable!(),
        }
        reseal_root(&mut bytes);
        assert_eq!(
            decode_chunk_index_v2(&bytes).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "offset {offset}"
        );
    }
}

#[test]
fn root_count_and_length_relationships_are_enforced() {
    let mut excessive_count = one_blob_file().bytes;
    put_u32(&mut excessive_count, ROOT_SERIES_COUNT_OFFSET, 0);
    reseal_root(&mut excessive_count);
    assert_eq!(
        decode_chunk_index_v2(&excessive_count).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    let mut missing_blob_count = one_blob_file().bytes;
    put_u32(&mut missing_blob_count, ROOT_BLOB_COUNT_OFFSET, 0);
    reseal_root(&mut missing_blob_count);
    assert_eq!(
        decode_chunk_index_v2(&missing_blob_count)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );

    let mut wrong_file_len = one_blob_file().bytes;
    put_u64(&mut wrong_file_len, ROOT_FILE_LEN_OFFSET, 139);
    reseal_root(&mut wrong_file_len);
    assert_eq!(
        decode_chunk_index_v2(&wrong_file_len).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    let mut short_region = one_blob_file().bytes;
    put_u64(&mut short_region, ROOT_BLOBS_LEN_OFFSET, 75);
    put_u64(&mut short_region, ROOT_FILE_LEN_OFFSET, 139);
    reseal_root(&mut short_region);
    assert_eq!(
        decode_chunk_overflow_root_v2(&short_region[..64], 139)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn blob_header_and_reserved_fields_are_enforced_after_valid_crc() {
    let mutations = [
        (CHUNK_OVERFLOW_ROOT_V2_LEN, 0u64, 4usize),
        (CHUNK_OVERFLOW_ROOT_V2_LEN + 4, 2, 2),
        (CHUNK_OVERFLOW_ROOT_V2_LEN + 6, 1u64, 2usize),
        (CHUNK_OVERFLOW_ROOT_V2_LEN + 8, 31, 4),
        (BLOB_RESERVED0_OFFSET, 1, 4),
    ];
    for (offset, value, width) in mutations {
        let mut bytes = one_blob_file().bytes;
        match width {
            2 => put_u16(&mut bytes, offset, value as u16),
            4 => put_u32(&mut bytes, offset, value as u32),
            _ => unreachable!(),
        }
        reseal_blob(&mut bytes, 64, 76);
        assert_eq!(
            decode_chunk_index_v2(&bytes).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "offset {offset}"
        );
    }
}

#[test]
fn blob_counts_lengths_and_series_bounds_are_enforced() {
    let mut zero_chunks = one_blob_file().bytes;
    put_u32(&mut zero_chunks, BLOB_CHUNK_COUNT_OFFSET, 0);
    put_u32(&mut zero_chunks, BLOB_BODY_LEN_OFFSET, 0);
    reseal_blob(&mut zero_chunks, 64, 32);
    assert_eq!(
        decode_chunk_index_v2(&zero_chunks).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    let mut wrong_body_len = one_blob_file().bytes;
    put_u32(&mut wrong_body_len, BLOB_BODY_LEN_OFFSET, 43);
    reseal_blob(&mut wrong_body_len, 64, 76);
    assert_eq!(
        decode_chunk_index_v2(&wrong_body_len).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    let mut out_of_range_series = one_blob_file().bytes;
    put_u32(&mut out_of_range_series, BLOB_SERIES_REF_OFFSET, 3);
    reseal_blob(&mut out_of_range_series, 64, 76);
    assert_eq!(
        decode_chunk_index_v2(&out_of_range_series)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn entry_reserved_kind_file_time_and_range_fields_are_enforced() {
    let mutations = [
        (FIRST_ENTRY_OFFSET, 2u64, 1usize),
        (FIRST_ENTRY_OFFSET + 1, 5, 1),
        (FIRST_ENTRY_OFFSET + 2, 1, 2),
        (FIRST_ENTRY_OFFSET + 4, u64::MAX, 8),
        (FIRST_ENTRY_OFFSET + 20, u64::MAX, 8),
        (FIRST_ENTRY_OFFSET + 28, 39, 4),
    ];
    for (offset, value, width) in mutations {
        let mut bytes = one_blob_file().bytes;
        match width {
            1 => bytes[offset] = value as u8,
            2 => put_u16(&mut bytes, offset, value as u16),
            4 => put_u32(&mut bytes, offset, value as u32),
            8 => put_u64(&mut bytes, offset, value),
            _ => unreachable!(),
        }
        reseal_blob(&mut bytes, 64, 76);
        assert_eq!(
            decode_chunk_index_v2(&bytes).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "offset {offset}"
        );
    }
}

#[test]
fn scalar_lane_shape_is_canonical() {
    let invalid_shapes = [
        (0, 16, ChunkKind::Histogram, 104),
        (40, 0, ChunkKind::Histogram, 104),
        (40, 15, ChunkKind::Histogram, 104),
        (40, 24, ChunkKind::Float, 104),
        (40, 65, ChunkKind::Histogram, 104),
        (u32::MAX, 16, ChunkKind::Histogram, u32::MAX),
    ];
    for (scalar_lane_offset, scalar_lane_len, kind, length) in invalid_shapes {
        let mut entry = typed_entry(0, kind, 1, 2, 3);
        entry.scalar_lane_offset = scalar_lane_offset;
        entry.scalar_lane_len = scalar_lane_len;
        entry.length = length;
        let error = encode_chunk_index_v2(
            1,
            &[ChunkOverflowBlobV1 {
                series_ref: 0,
                entries: vec![entry],
            }],
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}

#[test]
fn truncation_and_trailing_bytes_are_structural_errors() {
    let encoded = one_blob_file().bytes;
    for len in [0, 1, 63, 64, 95, 96, 139] {
        let error = decode_chunk_index_v2(&encoded[..len]).unwrap_err();
        assert!(matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
        ));
    }

    let mut trailing = encoded;
    trailing.push(0);
    assert_eq!(
        decode_chunk_index_v2(&trailing).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}
