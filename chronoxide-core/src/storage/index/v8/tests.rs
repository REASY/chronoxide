use std::io::{self, Cursor};

use super::codec::{
    decode_auxiliary_directory, decode_auxiliary_fst, decode_auxiliary_time_ranges,
    decode_exact_directory, decode_exact_page, decode_exact_postings, decode_root,
    rewrite_trailer_crc,
};
use super::*;
use crate::storage::index::MetricSeriesRange;
use crate::storage::series::SERIES_KIND_FLOAT;

mod goldens;
use goldens::*;

type MalformedPostingsCase<'a> = (&'a str, &'a [u8], u32, u32, io::ErrorKind, &'a str);

fn encode(indexes: &SegmentIndexes, counts: RootCounts) -> io::Result<Vec<u8>> {
    let mut bytes = Cursor::new(Vec::new());
    write_segment_indexes_v8(&mut bytes, indexes, counts)?;
    Ok(bytes.into_inner())
}

fn encode_v9(indexes: &SegmentIndexes, counts: RootCounts) -> io::Result<Vec<u8>> {
    let mut bytes = Cursor::new(Vec::new());
    crate::storage::index::write_segment_indexes_v9_unbound_for_test(
        &mut bytes,
        indexes,
        counts.series,
        counts.symbols,
    )?;
    Ok(bytes.into_inner())
}

fn decode(bytes: &[u8], counts: RootCounts) -> io::Result<SegmentIndexV8Layout> {
    decode_with_format(bytes, counts, AuthenticatedIndexFormat::V8Raw)
}

fn decode_v9(bytes: &[u8], counts: RootCounts) -> io::Result<SegmentIndexV8Layout> {
    decode_with_format(bytes, counts, AuthenticatedIndexFormat::V9Adaptive)
}

fn decode_with_format(
    bytes: &[u8],
    counts: RootCounts,
    format: AuthenticatedIndexFormat,
) -> io::Result<SegmentIndexV8Layout> {
    let trailer_offset = bytes
        .len()
        .checked_sub(TRAILER_LEN)
        .expect("fixture has a trailer");
    decode_root(
        bytes.len() as u64,
        &bytes[..HEADER_LEN],
        &bytes[trailer_offset..],
        counts,
        format,
    )
}

fn trailer_mut(bytes: &mut [u8]) -> &mut [u8] {
    let offset = bytes.len() - TRAILER_LEN;
    &mut bytes[offset..]
}

fn one_series_indexes() -> SegmentIndexes {
    let mut indexes = SegmentIndexes::default();
    indexes.metric_series_ranges.insert_range(
        0,
        MetricSeriesRange {
            start_series_ref: 0,
            series_count: 1,
            kind_mask: u16::from(SERIES_KIND_FLOAT),
            min_time_ms: 10,
            max_time_ms: 20,
        },
    );
    indexes
}

fn region(bytes: &[u8], locator: BlobLocator) -> &[u8] {
    let start = usize::try_from(locator.offset).unwrap();
    let len = usize::try_from(locator.len).unwrap();
    &bytes[start..start + len]
}

fn exact_indexes(entry_count: u32, series_count: u32) -> SegmentIndexes {
    let mut indexes = SegmentIndexes::default();
    if series_count != 0 {
        indexes.metric_series_ranges.insert_range(
            0,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 10,
                max_time_ms: 20,
            },
        );
    }
    for value_sym in 1..=entry_count {
        indexes.exact_postings.insert(0, value_sym, 0);
        indexes.label_value_time_ranges.insert(
            0,
            value_sym,
            u64::from(value_sym),
            u64::from(value_sym) + 10,
        );
    }
    indexes
}

fn exact_postings_indexes(refs: &[u32], series_count: u32) -> SegmentIndexes {
    assert!(!refs.is_empty());
    let mut indexes = SegmentIndexes::default();
    indexes.metric_series_ranges.insert_range(
        0,
        MetricSeriesRange {
            start_series_ref: 0,
            series_count,
            kind_mask: u16::from(SERIES_KIND_FLOAT),
            min_time_ms: 10,
            max_time_ms: 20,
        },
    );
    for &series_ref in refs {
        indexes.exact_postings.insert(0, 1, series_ref);
    }
    indexes.label_value_time_ranges.insert(0, 1, 10, 20);
    indexes
}

fn one_exact_record(bytes: &[u8], root: SegmentIndexV8Layout) -> io::Result<ExactRecord> {
    let directory = decode_exact_directory(region(bytes, root.exact_directory), root)?;
    if directory.descriptors.len() != 1 {
        return Err(io::Error::other("fixture does not have one exact page"));
    }
    let records = decode_exact_page(
        region(bytes, root.exact_pages),
        0,
        directory.descriptors[0],
        root,
    )?;
    if records.len() != 1 {
        return Err(io::Error::other("fixture does not have one exact record"));
    }
    Ok(records[0])
}

fn v9_fixture(
    refs: &[u32],
    series_count: u32,
) -> io::Result<(Vec<u8>, SegmentIndexV8Layout, ExactRecord)> {
    let counts = RootCounts {
        series: series_count,
        symbols: 2,
    };
    let bytes = encode_v9(&exact_postings_indexes(refs, series_count), counts)?;
    let root = decode_v9(&bytes, counts)?;
    let record = one_exact_record(&bytes, root)?;
    Ok((bytes, root, record))
}

fn decode_constructed_v9_payload(
    payload: &[u8],
    ref_count: u32,
    series_count: u32,
) -> io::Result<Vec<u32>> {
    let (_bytes, mut root, mut record) = v9_fixture(&[0], series_count)?;
    record.ref_count = ref_count;
    record.postings.len = payload.len() as u64;
    record.payload_crc32c = crc32c(payload);
    root.exact_postings = record.postings;
    decode_exact_postings(payload, record, root)
}

fn build_fst(values: &[&[u8]]) -> Vec<u8> {
    let mut builder = fst::SetBuilder::memory();
    for value in values {
        builder.insert(value).unwrap();
    }
    builder.into_inner().unwrap()
}

fn assert_structural_error(error: &io::Error) {
    assert!(matches!(
        error.kind(),
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
    ));
    assert_ne!(error.kind(), io::ErrorKind::InvalidInput);
}

fn literal_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("two-byte golden field"))
}

fn literal_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("four-byte golden field"))
}

fn literal_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("eight-byte golden field"))
}

#[test]
fn independent_empty_v8_container_matches_fixed_golden() {
    let counts = RootCounts {
        series: 0,
        symbols: 0,
    };
    let encoded = encode(&SegmentIndexes::default(), counts).unwrap();

    assert_eq!(encoded.len(), 412);
    assert_sha256(&encoded, EMPTY_SHA256);
    assert_hex(&encoded[..28], EMPTY_PREAMBLE);
    assert_hex(&encoded[28..92], EMPTY_EXACT_DIRECTORY);
    assert_hex(&encoded[92..156], EMPTY_AUXILIARY_DIRECTORY);

    // Read the root fields directly at their normative offsets. This expected
    // side intentionally does not use decode_root or any production accessor.
    let trailer = &encoded[156..];
    assert_eq!(&trailer[..4], b"SIDT");
    assert_eq!(literal_u16(&trailer[4..6]), 8);
    assert_eq!(literal_u32(&trailer[8..12]), 256);
    assert_eq!(literal_u64(&trailer[16..24]), 412);
    assert_eq!(literal_u64(&trailer[40..48]), 16);
    assert_eq!(literal_u64(&trailer[48..56]), 12);
    assert_eq!(literal_u64(&trailer[56..64]), 28);
    assert_eq!(literal_u64(&trailer[64..72]), 64);
    assert_eq!(literal_u64(&trailer[104..112]), 92);
    assert_eq!(literal_u64(&trailer[112..120]), 64);
    assert_eq!(literal_u32(&trailer[148..152]), 48);
    assert_eq!(literal_u32(&trailer[152..156]), 16_384);
    assert_eq!(literal_u32(&trailer[160..164]), 0x36ee_b500);
    assert_eq!(literal_u32(&trailer[172..176]), 0x43db_52ba);
    assert_eq!(literal_u32(&trailer[176..180]), 0x8033_216a);
    assert!(trailer[180..252].iter().all(|byte| *byte == 0));
    assert_eq!(&trailer[252..], b"S8ND");
}

#[test]
fn independent_one_exact_entry_matches_fixed_page_directory_and_trailer_golden() {
    let counts = RootCounts {
        series: 1,
        symbols: 2,
    };
    let encoded = encode(&exact_indexes(1, 1), counts).unwrap();

    assert_eq!(encoded.len(), 16_944);
    assert_sha256(&encoded, EXACT_ONE_SHA256);
    assert_hex(&encoded[64..72], "0100000000000000");
    assert_hex(
        &encoded[72..96],
        "010000000100000001000000000000000b00000000000000",
    );
    assert_hex(&encoded[96..192], EXACT_ONE_DIRECTORY);

    let page = &encoded[192..16_576];
    assert_hex(&page[..64], EXACT_ONE_PAGE_PREFIX);
    assert!(page[64..].iter().all(|byte| *byte == 0));
    assert_hex(&encoded[16_576..16_688], EXACT_ONE_AUXILIARY_DIRECTORY);

    let trailer = &encoded[16_688..];
    assert_eq!(literal_u64(&trailer[16..24]), 16_944);
    assert_eq!(literal_u64(&trailer[56..64]), 96);
    assert_eq!(literal_u64(&trailer[64..72]), 96);
    assert_eq!(literal_u64(&trailer[72..80]), 192);
    assert_eq!(literal_u64(&trailer[80..88]), 16_384);
    assert_eq!(literal_u64(&trailer[88..96]), 64);
    assert_eq!(literal_u64(&trailer[96..104]), 8);
    assert_eq!(literal_u64(&trailer[104..112]), 16_576);
    assert_eq!(literal_u64(&trailer[112..120]), 112);
    assert_eq!(literal_u64(&trailer[120..128]), 72);
    assert_eq!(literal_u64(&trailer[128..136]), 24);
    assert_eq!(literal_u64(&trailer[136..144]), 1);
    assert_eq!(literal_u32(&trailer[144..148]), 1);
    assert_eq!(literal_u32(&trailer[156..160]), 1);
    assert_eq!(literal_u32(&trailer[160..164]), 0x836b_b9b3);
    assert_eq!(literal_u32(&trailer[164..168]), 1);
    assert_eq!(literal_u32(&trailer[168..172]), 2);
    assert_eq!(literal_u32(&trailer[172..176]), 0x19af_b3cf);
    assert_eq!(literal_u32(&trailer[176..180]), 0xa51b_bc35);
}

#[test]
fn independent_exact_341_and_342_page_boundary_goldens() {
    let exact_341 = encode(
        &exact_indexes(341, 1),
        RootCounts {
            series: 1,
            symbols: 342,
        },
    )
    .unwrap();
    assert_eq!(exact_341.len(), 26_464);
    assert_sha256(&exact_341, EXACT_341_SHA256);
    assert_hex(&exact_341[9_616..9_712], EXACT_341_DIRECTORY);
    let full_page = &exact_341[9_712..26_096];
    assert_hex(&full_page[..64], EXACT_341_PAGE_PREFIX);
    // 341 records exactly fill 16 + 341 * 48 bytes: this is a record,
    // not padding, at the final byte of the page.
    assert_hex(&full_page[EXACT_PAGE_LEN - 48..], EXACT_341_LAST_RECORD);

    let exact_342 = encode(
        &exact_indexes(342, 1),
        RootCounts {
            series: 1,
            symbols: 343,
        },
    )
    .unwrap();
    assert_eq!(exact_342.len(), 42_908);
    assert_sha256(&exact_342, EXACT_342_SHA256);
    assert_hex(&exact_342[9_644..9_772], EXACT_342_DIRECTORY);
    let second_page = &exact_342[26_156..42_540];
    assert_hex(&second_page[..64], EXACT_342_SECOND_PAGE_PREFIX);
    // Page two has one record, so every byte after its 16 + 48 bytes is the
    // canonical authenticated zero padding.
    assert!(second_page[64..].iter().all(|byte| *byte == 0));
}

#[test]
fn independent_auxiliary_modes_match_fixed_payload_directory_and_crc_goldens() {
    let counts = RootCounts {
        series: 0,
        symbols: 4,
    };
    let fst_payload = goldens::bytes(FST_PAYLOAD);
    let range_payload = goldens::bytes(TIME_RANGE_PAYLOAD);

    let mut fst_only = SegmentIndexes::default();
    fst_only.label_values.insert_fst(1, fst_payload.clone());
    let fst_encoded = encode(&fst_only, counts).unwrap();
    assert_eq!(fst_encoded.len(), 512);
    assert_sha256(&fst_encoded, FST_ONLY_SHA256);
    assert_eq!(&fst_encoded[28..80], fst_payload);
    assert_hex(&fst_encoded[144..256], FST_ONLY_DIRECTORY);

    let mut range_only = SegmentIndexes::default();
    range_only.label_value_time_ranges.insert(1, 2, 100, 199);
    range_only.label_value_time_ranges.insert(1, 3, 300, 399);
    let range_encoded = encode(&range_only, counts).unwrap();
    assert_eq!(range_encoded.len(), 504);
    assert_sha256(&range_encoded, RANGE_ONLY_SHA256);
    assert_eq!(&range_encoded[28..72], range_payload);
    assert_hex(&range_encoded[136..248], RANGE_ONLY_DIRECTORY);

    let mut paired = range_only;
    paired.label_values.insert_fst(1, fst_payload.clone());
    let paired_encoded = encode(&paired, counts).unwrap();
    assert_eq!(paired_encoded.len(), 604);
    assert_sha256(&paired_encoded, PAIRED_AUXILIARY_SHA256);
    assert_eq!(&paired_encoded[28..80], fst_payload);
    assert_eq!(&paired_encoded[80..124], range_payload);
    assert_hex(&paired_encoded[188..348], PAIRED_AUXILIARY_DIRECTORY);
}

#[test]
fn valid_crc_reserved_order_and_auxiliary_truncation_are_corruption() {
    let exact_counts = RootCounts {
        series: 1,
        symbols: 343,
    };
    let exact = encode(&exact_indexes(342, 1), exact_counts).unwrap();
    let exact_root = decode(&exact, exact_counts).unwrap();
    let exact_directory = region(&exact, exact_root.exact_directory);

    let mut reserved_descriptor = exact_directory.to_vec();
    put_u32(&mut reserved_descriptor, EXACT_DIRECTORY_HEADER_LEN + 20, 1);
    let reserved_crc = crc_with_zeroed_field(&reserved_descriptor, 56);
    put_u32(&mut reserved_descriptor, 56, reserved_crc);
    let mut reserved_root = exact_root;
    reserved_root.exact_directory_crc32c = reserved_crc;
    let error = decode_exact_directory(&reserved_descriptor, reserved_root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("reserved"));

    let mut overlapping_descriptors = exact_directory.to_vec();
    // The second page's first value fence becomes 341, equal to the first
    // page's last fence. Recomputing both CRC authorities must not turn that
    // structurally ordered corruption into a valid directory.
    put_u32(
        &mut overlapping_descriptors,
        EXACT_DIRECTORY_HEADER_LEN + EXACT_PAGE_DESCRIPTOR_LEN + 4,
        341,
    );
    let overlapping_crc = crc_with_zeroed_field(&overlapping_descriptors, 56);
    put_u32(&mut overlapping_descriptors, 56, overlapping_crc);
    let mut overlapping_root = exact_root;
    overlapping_root.exact_directory_crc32c = overlapping_crc;
    let error = decode_exact_directory(&overlapping_descriptors, overlapping_root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("unordered or overlapping"));

    let mut indexes = SegmentIndexes::default();
    indexes
        .label_values
        .insert_fst(1, goldens::bytes(FST_PAYLOAD));
    indexes.label_value_time_ranges.insert(1, 2, 100, 199);
    indexes.label_value_time_ranges.insert(1, 3, 300, 399);
    let auxiliary_counts = RootCounts {
        series: 0,
        symbols: 4,
    };
    let auxiliary = encode(&indexes, auxiliary_counts).unwrap();
    let auxiliary_root = decode(&auxiliary, auxiliary_counts).unwrap();
    let auxiliary_directory = region(&auxiliary, auxiliary_root.auxiliary_directory);

    let mut record_flags = auxiliary_directory.to_vec();
    put_u16(&mut record_flags, AUXILIARY_DIRECTORY_HEADER_LEN + 2, 1);
    let flags_crc = crc_with_zeroed_field(&record_flags, 40);
    put_u32(&mut record_flags, 40, flags_crc);
    let mut flags_root = auxiliary_root;
    flags_root.auxiliary_directory_crc32c = flags_crc;
    let error = decode_auxiliary_directory(&record_flags, flags_root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("flags"));

    let mut reversed_records = auxiliary_directory.to_vec();
    let first = reversed_records
        [AUXILIARY_DIRECTORY_HEADER_LEN..AUXILIARY_DIRECTORY_HEADER_LEN + AUXILIARY_RECORD_LEN]
        .to_vec();
    let second = reversed_records[AUXILIARY_DIRECTORY_HEADER_LEN + AUXILIARY_RECORD_LEN
        ..AUXILIARY_DIRECTORY_HEADER_LEN + 2 * AUXILIARY_RECORD_LEN]
        .to_vec();
    reversed_records
        [AUXILIARY_DIRECTORY_HEADER_LEN..AUXILIARY_DIRECTORY_HEADER_LEN + AUXILIARY_RECORD_LEN]
        .copy_from_slice(&second);
    reversed_records[AUXILIARY_DIRECTORY_HEADER_LEN + AUXILIARY_RECORD_LEN
        ..AUXILIARY_DIRECTORY_HEADER_LEN + 2 * AUXILIARY_RECORD_LEN]
        .copy_from_slice(&first);
    let reversed_crc = crc_with_zeroed_field(&reversed_records, 40);
    put_u32(&mut reversed_records, 40, reversed_crc);
    let mut reversed_root = auxiliary_root;
    reversed_root.auxiliary_directory_crc32c = reversed_crc;
    let error = decode_auxiliary_directory(&reversed_records, reversed_root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("strictly ordered"));

    let decoded = decode_auxiliary_directory(auxiliary_directory, auxiliary_root).unwrap();
    let fst_record = decoded
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, 1)
        .unwrap();
    let fst = region(&auxiliary, fst_record.payload);
    let error =
        decode_auxiliary_fst(&fst[..fst.len() - 1], fst_record, auxiliary_root).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);

    let range_record = decoded
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, 1)
        .unwrap();
    let ranges = region(&auxiliary, range_record.payload);
    let error =
        decode_auxiliary_time_ranges(&ranges[..ranges.len() - 1], range_record, auxiliary_root)
            .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn empty_and_minimal_roots_round_trip_deterministically() {
    let empty_counts = RootCounts {
        series: 0,
        symbols: 0,
    };
    let empty = encode(&SegmentIndexes::default(), empty_counts).unwrap();
    assert_eq!(
        empty,
        encode(&SegmentIndexes::default(), empty_counts).unwrap()
    );
    let empty_root = decode(&empty, empty_counts).unwrap();
    assert_eq!(empty_root.counts, empty_counts);
    assert_eq!(empty_root.exact_entry_count, 0);
    assert_eq!(empty_root.exact_page_count, 0);
    assert_eq!(empty_root.auxiliary_entry_count, 0);
    assert_eq!(empty_root.routing, BlobLocator::default());
    assert_eq!(empty_root.exact_postings, BlobLocator::default());
    assert_eq!(empty_root.exact_pages, BlobLocator::default());
    assert_eq!(empty_root.auxiliary_payloads, BlobLocator::default());
    assert_eq!(empty_root.metric.offset, HEADER_LEN as u64);
    assert_eq!(
        empty_root.exact_directory.len,
        EXACT_DIRECTORY_HEADER_LEN as u64
    );
    assert_eq!(
        empty_root.auxiliary_directory.len,
        AUXILIARY_DIRECTORY_HEADER_LEN as u64
    );
    assert_eq!(empty_root.file_len as usize, empty.len());

    let minimal_counts = RootCounts {
        series: 1,
        symbols: 1,
    };
    let minimal_indexes = one_series_indexes();
    let minimal = encode(&minimal_indexes, minimal_counts).unwrap();
    assert_eq!(minimal, encode(&minimal_indexes, minimal_counts).unwrap());
    let minimal_root = decode(&minimal, minimal_counts).unwrap();
    assert_eq!(minimal_root.counts, minimal_counts);
    assert_eq!(minimal_root.metric.offset, HEADER_LEN as u64);
    assert_eq!(minimal_root.file_len as usize, minimal.len());
}

#[test]
fn root_rejects_mutation_truncation_and_trailing_root_bytes() {
    let counts = RootCounts {
        series: 0,
        symbols: 0,
    };
    let bytes = encode(&SegmentIndexes::default(), counts).unwrap();
    let trailer_offset = bytes.len() - TRAILER_LEN;

    for len in [0, 3, 8, HEADER_LEN - 1] {
        let error = decode_root(
            bytes.len() as u64,
            &bytes[..len],
            &bytes[trailer_offset..],
            counts,
            AuthenticatedIndexFormat::V8Raw,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }
    for len in [0, 3, 160, TRAILER_LEN - 1] {
        let error = decode_root(
            bytes.len() as u64,
            &bytes[..HEADER_LEN],
            &bytes[trailer_offset..trailer_offset + len],
            counts,
            AuthenticatedIndexFormat::V8Raw,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    let mut header = bytes[..HEADER_LEN].to_vec();
    header[0] ^= 1;
    assert_eq!(
        decode_root(
            bytes.len() as u64,
            &header,
            &bytes[trailer_offset..],
            counts,
            AuthenticatedIndexFormat::V8Raw,
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidData
    );

    let mut bad_crc = bytes.clone();
    trailer_mut(&mut bad_crc)[TRAILER_EXACT_DIRECTORY_CRC_OFFSET] ^= 1;
    assert!(decode(&bad_crc, counts).is_err());

    let mut reserved = bytes.clone();
    let trailer = trailer_mut(&mut reserved);
    trailer[TRAILER_RESERVED_OFFSET] = 1;
    rewrite_trailer_crc(trailer);
    assert!(decode(&reserved, counts).is_err());

    assert!(
        decode_root(
            bytes.len() as u64 + 1,
            &bytes[..HEADER_LEN],
            &bytes[trailer_offset..],
            counts,
            AuthenticatedIndexFormat::V8Raw,
        )
        .is_err()
    );
    let mut header_with_trailing_byte = bytes[..HEADER_LEN].to_vec();
    header_with_trailing_byte.push(0);
    assert!(
        decode_root(
            bytes.len() as u64,
            &header_with_trailing_byte,
            &bytes[trailer_offset..],
            counts,
            AuthenticatedIndexFormat::V8Raw,
        )
        .is_err()
    );
    let mut trailer_with_trailing_byte = bytes[trailer_offset..].to_vec();
    trailer_with_trailing_byte.push(0);
    assert!(
        decode_root(
            bytes.len() as u64,
            &bytes[..HEADER_LEN],
            &trailer_with_trailing_byte,
            counts,
            AuthenticatedIndexFormat::V8Raw,
        )
        .is_err()
    );
}

#[test]
fn root_requires_same_generation_series_and_symbol_counts() {
    let counts = RootCounts {
        series: 1,
        symbols: 1,
    };
    let bytes = encode(&one_series_indexes(), counts).unwrap();
    for wrong in [
        RootCounts {
            series: 0,
            symbols: 1,
        },
        RootCounts {
            series: 1,
            symbols: 0,
        },
    ] {
        let error = decode(&bytes, wrong).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("bound series/symbol roots"));
    }
}

#[test]
fn root_enforces_required_and_count_dependent_optional_locators() {
    let counts = RootCounts {
        series: 0,
        symbols: 0,
    };
    let bytes = encode(&SegmentIndexes::default(), counts).unwrap();

    let mut missing_metric = bytes.clone();
    let trailer = trailer_mut(&mut missing_metric);
    put_u64(trailer, TRAILER_METRIC_LOCATOR_OFFSET, 0);
    put_u64(trailer, TRAILER_METRIC_LOCATOR_OFFSET + 8, 0);
    rewrite_trailer_crc(trailer);
    assert!(decode(&missing_metric, counts).is_err());

    let mut half_routing = bytes.clone();
    let trailer = trailer_mut(&mut half_routing);
    put_u64(trailer, TRAILER_ROUTING_LOCATOR_OFFSET, HEADER_LEN as u64);
    rewrite_trailer_crc(trailer);
    assert!(decode(&half_routing, counts).is_err());

    for locator_offset in [
        TRAILER_EXACT_POSTINGS_LOCATOR_OFFSET,
        TRAILER_EXACT_PAGES_LOCATOR_OFFSET,
        TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET,
    ] {
        let mut unexpected = bytes.clone();
        let trailer = trailer_mut(&mut unexpected);
        put_u64(trailer, locator_offset, HEADER_LEN as u64);
        put_u64(trailer, locator_offset + 8, 1);
        rewrite_trailer_crc(trailer);
        assert!(decode(&unexpected, counts).is_err());
    }

    let mut missing_exact_regions = bytes.clone();
    let trailer = trailer_mut(&mut missing_exact_regions);
    put_u64(trailer, TRAILER_EXACT_ENTRY_COUNT_OFFSET, 1);
    put_u32(trailer, TRAILER_EXACT_PAGE_COUNT_OFFSET, 1);
    rewrite_trailer_crc(trailer);
    assert!(decode(&missing_exact_regions, counts).is_err());

    let mut missing_auxiliary_payload = bytes.clone();
    let trailer = trailer_mut(&mut missing_auxiliary_payload);
    put_u32(trailer, TRAILER_AUX_ENTRY_COUNT_OFFSET, 1);
    rewrite_trailer_crc(trailer);
    assert!(decode(&missing_auxiliary_payload, counts).is_err());
}

#[test]
fn root_rejects_a_physically_inserted_unaccounted_gap() {
    let counts = RootCounts {
        series: 0,
        symbols: 0,
    };
    let mut bytes = encode(&SegmentIndexes::default(), counts).unwrap();
    bytes.insert(HEADER_LEN, 0);
    let new_file_len = bytes.len() as u64;
    let trailer = trailer_mut(&mut bytes);
    put_u64(trailer, TRAILER_FILE_LEN_OFFSET, new_file_len);
    for locator_offset in [
        TRAILER_METRIC_LOCATOR_OFFSET,
        TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET,
        TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET,
    ] {
        let shifted = read_u64(trailer, locator_offset) + 1;
        put_u64(trailer, locator_offset, shifted);
    }
    rewrite_trailer_crc(trailer);

    let error = decode(&bytes, counts).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("canonically adjacent"));
}

#[test]
fn private_writer_rejects_exact_postings_without_a_time_range() {
    let mut indexes = one_series_indexes();
    indexes.exact_postings.insert(0, 1, 0);
    let error = encode(
        &indexes,
        RootCounts {
            series: 1,
            symbols: 2,
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(
        error
            .to_string()
            .contains("matching label-value time range")
    );
}

#[test]
fn exact_directories_decode_at_v8_page_boundaries() {
    for entry_count in [1u32, 341, 342] {
        let indexes = exact_indexes(entry_count, 1);
        let counts = RootCounts {
            series: 1,
            symbols: entry_count + 1,
        };
        let bytes = encode(&indexes, counts).unwrap();
        assert_eq!(bytes, encode(&indexes, counts).unwrap());
        let root = decode(&bytes, counts).unwrap();
        let expected_pages = if entry_count <= 341 { 1 } else { 2 };
        assert_eq!(root.exact_entry_count, u64::from(entry_count));
        assert_eq!(root.exact_page_count, expected_pages);
        assert_eq!(
            root.exact_directory.len,
            (EXACT_DIRECTORY_HEADER_LEN
                + usize::try_from(expected_pages).unwrap() * EXACT_PAGE_DESCRIPTOR_LEN)
                as u64
        );
        assert_eq!(
            root.exact_pages.len,
            u64::from(expected_pages) * EXACT_PAGE_LEN as u64
        );

        let directory = decode_exact_directory(region(&bytes, root.exact_directory), root).unwrap();
        assert_eq!(directory.descriptors.len(), expected_pages as usize);
        assert_eq!(directory.descriptors[0].record_count, entry_count.min(341));
        if entry_count == 342 {
            assert_eq!(directory.descriptors[1].record_count, 1);
        }

        let mut decoded_count = 0usize;
        for (page_index, descriptor) in directory.descriptors.iter().copied().enumerate() {
            let page_offset =
                usize::try_from(root.exact_pages.offset).unwrap() + page_index * EXACT_PAGE_LEN;
            let records = decode_exact_page(
                &bytes[page_offset..page_offset + EXACT_PAGE_LEN],
                page_index,
                descriptor,
                root,
            )
            .unwrap();
            for record in records {
                let refs =
                    decode_exact_postings(region(&bytes, record.postings), record, root).unwrap();
                assert_eq!(refs, [0]);
                decoded_count += 1;
            }
        }
        assert_eq!(decoded_count, entry_count as usize);
    }
}

#[test]
fn v9_adaptive_postings_round_trip_delta_raw_and_raw_tie_deterministically() {
    let cases: &[(&[u32], u32, &[u8])] = &[
        (&[0, 1, 2, 3], 4, &[1, 0, 0, 0, 0, 1, 1, 1]),
        (&[1 << 21], (1 << 21) + 1, &[0, 0, 0, 0, 0, 0, 32, 0]),
        (&[u32::MAX - 1], u32::MAX, &[0, 0, 0, 0, 254, 255, 255, 255]),
    ];

    for &(refs, series_count, expected_payload) in cases {
        let indexes = exact_postings_indexes(refs, series_count);
        let counts = RootCounts {
            series: series_count,
            symbols: 2,
        };
        let bytes = encode_v9(&indexes, counts).unwrap();
        assert_eq!(bytes, encode_v9(&indexes, counts).unwrap());
        assert_eq!(&bytes[..16], b"SIDX\x09\0\0\0\x10\0\0\0\0\0\0\0");
        assert_eq!(&bytes[bytes.len() - 4..], b"S9ND");

        let root = decode_v9(&bytes, counts).unwrap();
        assert_eq!(root.format, AuthenticatedIndexFormat::V9Adaptive);
        assert_eq!(&region(&bytes, root.exact_directory)[..4], b"EXD9");
        assert_eq!(&region(&bytes, root.exact_pages)[..4], b"XPG9");
        let record = one_exact_record(&bytes, root).unwrap();
        let payload = region(&bytes, record.postings);
        assert_eq!(payload, expected_payload);
        assert_eq!(decode_exact_postings(payload, record, root).unwrap(), refs);
    }
}

#[test]
fn v8_and_v9_containers_reject_cross_version_decoding() {
    let counts = RootCounts {
        series: 1,
        symbols: 1,
    };
    let indexes = one_series_indexes();
    let v8 = encode(&indexes, counts).unwrap();
    let v9 = encode_v9(&indexes, counts).unwrap();

    for (name, result) in [
        ("v8 as v9", decode_v9(&v8, counts)),
        ("v9 as v8", decode(&v9, counts)),
    ] {
        let error = result.expect_err(name);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}: {error}");
        assert!(
            error.to_string().contains("header version mismatch"),
            "{name}: {error}"
        );
    }
}

#[test]
fn v9_delta_uleb_round_trips_first_ref_and_gap_boundaries() {
    let first_ref_boundaries = [
        0,
        0x7f,
        0x80,
        0x3fff,
        0x4000,
        0x1f_ffff,
        0x20_0000,
        0x0fff_ffff,
        0x1000_0000,
        u32::MAX - 4,
    ];
    for first in first_ref_boundaries {
        let refs = [first, first + 1, first + 2, first + 3];
        let series_count = refs[3]
            .checked_add(1)
            .expect("boundary fixture remains below u32::MAX");
        let (bytes, root, record) = v9_fixture(&refs, series_count).unwrap();
        let payload = region(&bytes, record.postings);
        assert_eq!(payload[0], EXACT_POSTINGS_CODEC_DELTA_ULEB128);
        assert_eq!(
            payload.len(),
            EXACT_POSTINGS_V9_HEADER_LEN as usize + uleb128_u32_len(first) + 3
        );
        assert_eq!(decode_exact_postings(payload, record, root).unwrap(), refs);
    }

    let gap_boundaries = [
        1,
        0x7f,
        0x80,
        0x3fff,
        0x4000,
        0x1f_ffff,
        0x20_0000,
        0x0fff_ffff,
        0x1000_0000,
        u32::MAX - 1,
    ];
    for gap in gap_boundaries {
        let refs = [0, gap];
        let series_count = gap
            .checked_add(1)
            .expect("maximum boundary fixture still has a valid series bound");
        let (bytes, root, record) = v9_fixture(&refs, series_count).unwrap();
        let payload = region(&bytes, record.postings);
        assert_eq!(payload[0], EXACT_POSTINGS_CODEC_DELTA_ULEB128);
        assert_eq!(
            payload.len(),
            EXACT_POSTINGS_V9_HEADER_LEN as usize + 1 + uleb128_u32_len(gap)
        );
        assert_eq!(decode_exact_postings(payload, record, root).unwrap(), refs);
    }
}

#[test]
fn v9_adaptive_postings_reject_malformed_headers_and_varints() {
    let cases: &[MalformedPostingsCase<'_>] = &[
        (
            "unknown codec",
            &[2, 0, 0, 0, 0],
            1,
            1,
            io::ErrorKind::InvalidData,
            "codec is unknown",
        ),
        (
            "flags",
            &[1, 1, 0, 0, 0],
            1,
            1,
            io::ErrorKind::InvalidData,
            "flags or reserved",
        ),
        (
            "reserved",
            &[1, 0, 1, 0, 0],
            1,
            1,
            io::ErrorKind::InvalidData,
            "flags or reserved",
        ),
        (
            "noncanonical varint",
            &[1, 0, 0, 0, 0x80, 0],
            1,
            1,
            io::ErrorKind::InvalidData,
            "not canonically encoded",
        ),
        (
            "truncated varint",
            &[1, 0, 0, 0, 0x80],
            1,
            1,
            io::ErrorKind::UnexpectedEof,
            "varint is truncated",
        ),
        (
            "overflowing varint",
            &[1, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x10, 1],
            2,
            u32::MAX,
            io::ErrorKind::InvalidData,
            "varint exceeds u32",
        ),
    ];

    for &(name, payload, ref_count, series_count, expected_kind, expected_message) in cases {
        let error =
            decode_constructed_v9_payload(payload, ref_count, series_count).expect_err(name);
        assert_eq!(error.kind(), expected_kind, "{name}: {error}");
        assert!(
            error.to_string().contains(expected_message),
            "{name}: {error}"
        );
    }
}

#[test]
fn v9_adaptive_postings_accepts_canonical_u32_max_varint_before_ref_bounds() {
    // No valid posting can contain u32::MAX because a series ref must be less
    // than the u32 series-count authority. Reaching the semantic bound error
    // proves that the exact five-byte u32::MAX encoding itself was accepted.
    let payload = [1, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f, 1];
    let error = decode_constructed_v9_payload(&payload, 2, u32::MAX).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("bound series count"), "{error}");
}

#[test]
fn v9_adaptive_postings_reject_locator_lengths_outside_count_bounds() {
    let ref_count = 2;
    let minimum_len = EXACT_POSTINGS_V9_HEADER_LEN as usize + ref_count as usize;
    let maximum_len = EXACT_POSTINGS_V9_HEADER_LEN as usize + 4 * ref_count as usize;

    for (name, payload) in [
        ("below minimum", vec![0; minimum_len - 1]),
        ("above maximum", vec![0; maximum_len + 1]),
    ] {
        let error = decode_constructed_v9_payload(&payload, ref_count, ref_count).expect_err(name);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}: {error}");
        assert!(
            error.to_string().contains("count-derived bounds"),
            "{name}: {error}"
        );
    }
}

#[test]
fn v9_adaptive_postings_reject_payload_crc_mismatch() {
    let (bytes, root, record) = v9_fixture(&[0, 1, 2, 3], 4).unwrap();
    let mut payload = region(&bytes, record.postings).to_vec();
    *payload.last_mut().unwrap() ^= 1;

    let error = decode_exact_postings(&payload, record, root).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains("payload CRC mismatch"),
        "{error}"
    );
}

#[test]
fn v9_adaptive_raw_postings_reject_duplicate_and_decreasing_refs() {
    let large_ref = 1 << 21;
    for (name, refs) in [
        ("duplicate", [large_ref, large_ref]),
        ("decreasing", [large_ref, 0]),
    ] {
        let mut payload = vec![EXACT_POSTINGS_CODEC_RAW32, 0, 0, 0];
        payload.extend(refs.into_iter().flat_map(u32::to_le_bytes));
        let error = decode_constructed_v9_payload(&payload, 2, large_ref + 1).expect_err(name);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}: {error}");
        assert!(
            error.to_string().contains("strictly ordered and unique"),
            "{name}: {error}"
        );
    }
}

#[test]
fn v9_adaptive_postings_reject_invalid_delta_semantics() {
    let cases: &[(&str, &[u8], u32, u32, &str)] = &[
        ("zero gap", &[1, 0, 0, 0, 0, 0], 2, 2, "delta gap is zero"),
        (
            "addition overflow",
            &[1, 0, 0, 0, 0xfe, 0xff, 0xff, 0xff, 0x0f, 2],
            2,
            u32::MAX,
            "delta addition overflows",
        ),
        (
            "trailing bytes",
            &[1, 0, 0, 0, 0, 0],
            1,
            1,
            "trailing bytes",
        ),
        ("out of range", &[1, 0, 0, 0, 1], 1, 1, "bound series count"),
    ];

    for &(name, payload, ref_count, series_count, expected_message) in cases {
        let error =
            decode_constructed_v9_payload(payload, ref_count, series_count).expect_err(name);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}: {error}");
        assert!(
            error.to_string().contains(expected_message),
            "{name}: {error}"
        );
    }
}

#[test]
fn v9_adaptive_postings_reject_noncanonical_codec_choices() {
    let noncanonical_raw = [0, 0, 0, 0, 0, 0, 0, 0];
    let error = decode_constructed_v9_payload(&noncanonical_raw, 1, 1).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("RAW32 codec choice"));

    // A four-byte first ref makes the delta payload exactly as long as RAW32.
    // RAW32 wins ties, so encoding the same ref as DELTA_ULEB128 is rejected.
    let noncanonical_delta_tie = [1, 0, 0, 0, 0x80, 0x80, 0x80, 1];
    let error =
        decode_constructed_v9_payload(&noncanonical_delta_tie, 1, (1 << 21) + 1).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("delta codec choice"));
}

#[test]
fn empty_exact_and_auxiliary_directories_are_authenticated() {
    let counts = RootCounts {
        series: 0,
        symbols: 0,
    };
    let bytes = encode(&SegmentIndexes::default(), counts).unwrap();
    let root = decode(&bytes, counts).unwrap();
    assert!(
        decode_exact_directory(region(&bytes, root.exact_directory), root)
            .unwrap()
            .descriptors
            .is_empty()
    );
    assert!(
        decode_auxiliary_directory(region(&bytes, root.auxiliary_directory), root)
            .unwrap()
            .records
            .is_empty()
    );

    let mut wrong_exact_root = root;
    wrong_exact_root.exact_directory_crc32c ^= 1;
    let error =
        decode_exact_directory(region(&bytes, root.exact_directory), wrong_exact_root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("root"));

    let mut wrong_auxiliary_root = root;
    wrong_auxiliary_root.auxiliary_directory_crc32c ^= 1;
    let error = decode_auxiliary_directory(
        region(&bytes, root.auxiliary_directory),
        wrong_auxiliary_root,
    )
    .unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("root"));
}

#[test]
fn exact_reader_rejects_directory_page_and_payload_corruption() {
    let indexes = exact_indexes(2, 1);
    let counts = RootCounts {
        series: 1,
        symbols: 3,
    };
    let bytes = encode(&indexes, counts).unwrap();
    let root = decode(&bytes, counts).unwrap();
    let directory_bytes = region(&bytes, root.exact_directory);
    let directory = decode_exact_directory(directory_bytes, root).unwrap();

    let mut corrupted_directory = directory_bytes.to_vec();
    corrupted_directory[EXACT_DIRECTORY_HEADER_LEN] ^= 1;
    let error = decode_exact_directory(&corrupted_directory, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("CRC"));

    let descriptor = directory.descriptors[0];
    let page = region(&bytes, root.exact_pages);
    let mut corrupted_page = page.to_vec();
    corrupted_page[EXACT_PAGE_HEADER_LEN] ^= 1;
    let error = decode_exact_page(&corrupted_page, 0, descriptor, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("CRC"));

    let mut nonzero_padding = page.to_vec();
    *nonzero_padding.last_mut().unwrap() = 1;
    let mut padding_descriptor = descriptor;
    padding_descriptor.page_crc32c = crc32c(&nonzero_padding);
    let error = decode_exact_page(&nonzero_padding, 0, padding_descriptor, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("padding"));

    let mut duplicate_key = page.to_vec();
    let first_key = read_u64(&duplicate_key, EXACT_PAGE_HEADER_LEN);
    put_u64(
        &mut duplicate_key,
        EXACT_PAGE_HEADER_LEN + EXACT_RECORD_LEN,
        first_key,
    );
    let mut duplicate_descriptor = descriptor;
    duplicate_descriptor.page_crc32c = crc32c(&duplicate_key);
    let error = decode_exact_page(&duplicate_key, 0, duplicate_descriptor, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("strictly ordered"));

    let records = decode_exact_page(page, 0, descriptor, root).unwrap();
    let record = records[0];
    let payload = region(&bytes, record.postings);
    let mut corrupted_payload = payload.to_vec();
    corrupted_payload[4] ^= 1;
    let error = decode_exact_postings(&corrupted_payload, record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("CRC"));

    for truncated in [
        &directory_bytes[..directory_bytes.len() - 1],
        &page[..page.len() - 1],
        &payload[..payload.len() - 1],
    ] {
        let error = if truncated.len() == directory_bytes.len() - 1 {
            decode_exact_directory(truncated, root).unwrap_err()
        } else if truncated.len() == page.len() - 1 {
            decode_exact_page(truncated, 0, descriptor, root).unwrap_err()
        } else {
            decode_exact_postings(truncated, record, root).unwrap_err()
        };
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    let mut trailing_payload = payload.to_vec();
    trailing_payload.push(0);
    let error = decode_exact_postings(&trailing_payload, record, root).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn exact_reader_rejects_valid_crc_count_order_and_bound_violations() {
    let mut indexes = exact_indexes(1, 3);
    indexes.exact_postings.insert(0, 1, 2);
    let counts = RootCounts {
        series: 3,
        symbols: 2,
    };
    let bytes = encode(&indexes, counts).unwrap();
    let root = decode(&bytes, counts).unwrap();
    let directory = decode_exact_directory(region(&bytes, root.exact_directory), root).unwrap();
    let records = decode_exact_page(
        region(&bytes, root.exact_pages),
        0,
        directory.descriptors[0],
        root,
    )
    .unwrap();
    let record = records[0];
    let payload = region(&bytes, record.postings);

    let mut count_mismatch = payload.to_vec();
    put_u32(&mut count_mismatch, 0, record.ref_count - 1);
    let mut count_record = record;
    count_record.payload_crc32c = crc32c(&count_mismatch);
    let error = decode_exact_postings(&count_mismatch, count_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("body count"));

    let mut duplicate = payload.to_vec();
    put_u32(&mut duplicate, 8, 0);
    let mut duplicate_record = record;
    duplicate_record.payload_crc32c = crc32c(&duplicate);
    let error = decode_exact_postings(&duplicate, duplicate_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("strictly ordered"));

    let mut out_of_range = payload.to_vec();
    put_u32(&mut out_of_range, 8, counts.series);
    let mut bound_record = record;
    bound_record.payload_crc32c = crc32c(&out_of_range);
    let error = decode_exact_postings(&out_of_range, bound_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("bound series count"));
}

#[test]
fn exact_equal_length_payload_swap_fails_protected_checksums() {
    let mut indexes = exact_indexes(2, 3);
    indexes.exact_postings.insert(0, 1, 2);
    indexes.exact_postings.insert(0, 2, 1);
    let counts = RootCounts {
        series: 3,
        symbols: 3,
    };
    let bytes = encode(&indexes, counts).unwrap();
    let root = decode(&bytes, counts).unwrap();
    let directory = decode_exact_directory(region(&bytes, root.exact_directory), root).unwrap();
    let records = decode_exact_page(
        region(&bytes, root.exact_pages),
        0,
        directory.descriptors[0],
        root,
    )
    .unwrap();
    assert_eq!(records[0].postings.len, records[1].postings.len);
    let swapped = region(&bytes, records[1].postings);
    let error = decode_exact_postings(swapped, records[0], root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("CRC"));
}

#[test]
fn auxiliary_fst_only_range_only_and_paired_payloads_decode() {
    let mut fst_only = SegmentIndexes::default();
    fst_only
        .label_values
        .insert_fst(1, build_fst(&[b"alpha", b"beta"]));
    let fst_counts = RootCounts {
        series: 0,
        symbols: 2,
    };
    let fst_bytes = encode(&fst_only, fst_counts).unwrap();
    let fst_root = decode(&fst_bytes, fst_counts).unwrap();
    let fst_directory =
        decode_auxiliary_directory(region(&fst_bytes, fst_root.auxiliary_directory), fst_root)
            .unwrap();
    let fst_record = fst_directory
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, 1)
        .unwrap();
    assert_eq!(fst_record.time_range, UNCONSTRAINED_TIME_RANGE);
    let fst =
        decode_auxiliary_fst(region(&fst_bytes, fst_record.payload), fst_record, fst_root).unwrap();
    assert!(fst.contains("alpha"));
    assert!(fst.contains("beta"));

    let mut range_only = SegmentIndexes::default();
    range_only.label_value_time_ranges.insert(1, 2, 100, 199);
    range_only.label_value_time_ranges.insert(1, 3, 300, 399);
    let range_counts = RootCounts {
        series: 0,
        symbols: 4,
    };
    let range_bytes = encode(&range_only, range_counts).unwrap();
    let range_root = decode(&range_bytes, range_counts).unwrap();
    let range_directory = decode_auxiliary_directory(
        region(&range_bytes, range_root.auxiliary_directory),
        range_root,
    )
    .unwrap();
    let range_record = range_directory
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, 1)
        .unwrap();
    assert_eq!(
        decode_auxiliary_time_ranges(
            region(&range_bytes, range_record.payload),
            range_record,
            range_root,
        )
        .unwrap(),
        [
            (
                2,
                LabelValueTimeRange {
                    min_time_ms: 100,
                    max_time_ms: 199,
                },
            ),
            (
                3,
                LabelValueTimeRange {
                    min_time_ms: 300,
                    max_time_ms: 399,
                },
            ),
        ]
    );

    let mut paired = range_only;
    paired
        .label_values
        .insert_fst(1, build_fst(&[b"alpha", b"beta"]));
    let paired_bytes = encode(&paired, range_counts).unwrap();
    let paired_root = decode(&paired_bytes, range_counts).unwrap();
    let paired_directory = decode_auxiliary_directory(
        region(&paired_bytes, paired_root.auxiliary_directory),
        paired_root,
    )
    .unwrap();
    let paired_fst = paired_directory
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, 1)
        .unwrap();
    let paired_ranges = paired_directory
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, 1)
        .unwrap();
    assert_eq!(paired_fst.item_count, paired_ranges.item_count);
    assert_eq!(paired_fst.time_range, paired_ranges.time_range);
    decode_auxiliary_fst(
        region(&paired_bytes, paired_fst.payload),
        paired_fst,
        paired_root,
    )
    .unwrap();
    decode_auxiliary_time_ranges(
        region(&paired_bytes, paired_ranges.payload),
        paired_ranges,
        paired_root,
    )
    .unwrap();
}

#[test]
fn auxiliary_reader_rejects_crc_count_symbol_utf8_and_pair_corruption() {
    let mut indexes = SegmentIndexes::default();
    indexes
        .label_values
        .insert_fst(1, build_fst(&[b"alpha", b"beta"]));
    indexes.label_value_time_ranges.insert(1, 2, 100, 199);
    indexes.label_value_time_ranges.insert(1, 3, 300, 399);
    let counts = RootCounts {
        series: 0,
        symbols: 4,
    };
    let bytes = encode(&indexes, counts).unwrap();
    let root = decode(&bytes, counts).unwrap();
    let directory_bytes = region(&bytes, root.auxiliary_directory);
    let directory = decode_auxiliary_directory(directory_bytes, root).unwrap();
    let fst_record = directory
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, 1)
        .unwrap();
    let range_record = directory
        .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, 1)
        .unwrap();

    let mut corrupted_fst = region(&bytes, fst_record.payload).to_vec();
    corrupted_fst[0] ^= 1;
    let error = decode_auxiliary_fst(&corrupted_fst, fst_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("CRC"));

    let replacement_fst = build_fst(&[b"gamma", b"zeta"]);
    assert_eq!(replacement_fst.len(), fst_record.payload.len as usize);
    let error = decode_auxiliary_fst(&replacement_fst, fst_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("CRC"));

    let mut wrong_fst_count = fst_record;
    wrong_fst_count.item_count += 1;
    let error = decode_auxiliary_fst(region(&bytes, fst_record.payload), wrong_fst_count, root)
        .unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("item count"));

    let mut corrupted_ranges = region(&bytes, range_record.payload).to_vec();
    put_u64(&mut corrupted_ranges, 16, 198);
    let error = decode_auxiliary_time_ranges(&corrupted_ranges, range_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("CRC"));

    let mut count_mismatch = region(&bytes, range_record.payload).to_vec();
    put_u32(&mut count_mismatch, 0, range_record.item_count - 1);
    let mut count_record = range_record;
    count_record.payload_crc32c = crc32c(&count_mismatch);
    let error = decode_auxiliary_time_ranges(&count_mismatch, count_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("body count"));

    let mut foreign_symbol = region(&bytes, range_record.payload).to_vec();
    put_u32(&mut foreign_symbol, 24, counts.symbols);
    let mut foreign_record = range_record;
    foreign_record.payload_crc32c = crc32c(&foreign_symbol);
    let error = decode_auxiliary_time_ranges(&foreign_symbol, foreign_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("bound symbol count"));

    let mut duplicate_symbol = region(&bytes, range_record.payload).to_vec();
    put_u32(&mut duplicate_symbol, 24, 2);
    let mut duplicate_record = range_record;
    duplicate_record.payload_crc32c = crc32c(&duplicate_symbol);
    let error =
        decode_auxiliary_time_ranges(&duplicate_symbol, duplicate_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("strictly ordered"));

    let mut aggregate_mismatch = region(&bytes, range_record.payload).to_vec();
    put_u64(&mut aggregate_mismatch, 8, 101);
    let mut aggregate_record = range_record;
    aggregate_record.payload_crc32c = crc32c(&aggregate_mismatch);
    let error =
        decode_auxiliary_time_ranges(&aggregate_mismatch, aggregate_record, root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("aggregate"));

    let valid_single = build_fst(&[b"x"]);
    let invalid_utf8 = build_fst(&[&[0xff]]);
    assert!(!valid_single.is_empty());
    let mut utf8_root = root;
    let mut utf8_record = fst_record;
    utf8_record.payload.len = invalid_utf8.len() as u64;
    utf8_record.payload_crc32c = crc32c(&invalid_utf8);
    utf8_root.auxiliary_payloads.len = utf8_root
        .auxiliary_payloads
        .len
        .max(utf8_record.payload.len);
    utf8_record.item_count = 1;
    let error = decode_auxiliary_fst(&invalid_utf8, utf8_record, utf8_root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("UTF-8"));

    let mut pair_mismatch = directory_bytes.to_vec();
    put_u32(
        &mut pair_mismatch,
        AUXILIARY_DIRECTORY_HEADER_LEN + 40,
        fst_record.item_count - 1,
    );
    let new_crc = crc_with_zeroed_field(&pair_mismatch, 40);
    put_u32(&mut pair_mismatch, 40, new_crc);
    let mut pair_root = root;
    pair_root.auxiliary_directory_crc32c = new_crc;
    let error = decode_auxiliary_directory(&pair_mismatch, pair_root).unwrap_err();
    assert_structural_error(&error);
    assert!(error.to_string().contains("paired FST"));

    let mut truncated = directory_bytes.to_vec();
    truncated.pop();
    let error = decode_auxiliary_directory(&truncated, root).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}
