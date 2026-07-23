use crc32c::crc32c;

use crate::storage::chunk::{
    CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN, CHUNK_OVERFLOW_ROOT_V2_LEN, ChunkOverflowBlobV1,
    EncodedChunkIndexV2, IndexedChunkAuthentication, encode_chunk_index_v2,
};

use super::super::{
    InlineChunkV3, SERIES_HOT_SCALAR_LANE_LEN_MAX, SeriesHeaderV3Params, SeriesHotV3,
    encode_series_hot_page_v1,
};
use super::*;

const SEGMENT_START_MS: u64 = 1_000;
const SEGMENT_END_MS: u64 = SEGMENT_START_MS + u32::MAX as u64 + 100;
const CHUNK_FILE_LENS: [u64; 2] = [u32::MAX as u64 + 10_000_000, u32::MAX as u64 + 10_000_000];

fn kind_mask(kinds: impl IntoIterator<Item = ChunkKind>) -> u8 {
    kinds
        .into_iter()
        .fold(0, |mask, kind| mask | kind_bit(kind))
}

fn header_for_index(series_count: u32, index: &EncodedChunkIndexV2) -> SeriesHeaderV3 {
    header_for_index_with_bounds(series_count, index, SEGMENT_START_MS, SEGMENT_END_MS)
}

fn header_for_index_with_bounds(
    series_count: u32,
    index: &EncodedChunkIndexV2,
    segment_start_ms: u64,
    segment_end_ms: u64,
) -> SeriesHeaderV3 {
    SeriesHeaderV3::new(SeriesHeaderV3Params {
        num_series: series_count,
        num_keysets: series_count.min(3),
        num_value_dicts: u32::from(series_count != 0),
        chunk_index_root_crc32c: index.root.root_crc32c,
        keysets_len: if series_count == 0 { 8 } else { 32 },
        value_dicts_len: if series_count == 0 { 8 } else { 16 },
        keyset_blocks_len: if series_count == 0 { 8 } else { 32 },
        segment_start_ms,
        segment_end_ms,
        chunk_index_file_len: index.root.file_len,
    })
    .unwrap()
}

fn series_page_facts(header: SeriesHeaderV3) -> Schema7SeriesPageFacts {
    Schema7SeriesPageFacts {
        root_len: header.hot_pages_offset,
        hot_page_count: header.page_count,
        hot_pages_offset: header.hot_pages_offset,
        hot_pages_len: header.hot_pages_len,
        cold_page_count: header.cold_page_count,
        cold_pages_offset: header.keysets_offset,
        cold_pages_len: header.cold_bytes_len().unwrap(),
        file_len: header.file_len,
    }
}

fn overflow_blob_facts(root: ChunkOverflowRootV2) -> Schema7OverflowBlobFacts {
    Schema7OverflowBlobFacts {
        root_len: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
        blob_count: root.blob_count,
        blobs_offset: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
        blobs_len: root.blobs_len,
        file_len: root.file_len,
    }
}

fn inline_record(
    series_ref: u32,
    kind: ChunkKind,
    file_id: u8,
    scalar_lane_len: u32,
    indexed_prefix_crc32c: u32,
) -> SeriesHotV3 {
    let delta = series_ref;
    SeriesHotV3 {
        series_id: 10_000 + u64::from(series_ref),
        keyset_id: series_ref % 3,
        row: 20_000 + series_ref,
        kind_mask: kind_bit(kind),
        location: SeriesHotLocationV3::Inline(InlineChunkV3 {
            chunk_kind: kind as u8,
            file_id,
            scalar_lane_len,
            min_time_delta_ms: delta,
            max_time_delta_ms: delta + 1,
            file_offset: series_ref * 64,
            chunk_length: 48 + scalar_lane_len,
            indexed_prefix_crc32c,
        }),
    }
}

fn page_records(first_series_ref: u32, count: u32) -> Vec<SeriesHotV3> {
    (first_series_ref..first_series_ref + count)
        .map(|series_ref| {
            inline_record(series_ref, ChunkKind::Float, 0, 0, 0x8000_0000 | series_ref)
        })
        .collect()
}

fn assert_invalid(error: io::Error, expected: &str) {
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error.to_string().contains(expected),
        "unexpected error: {error}"
    );
}

#[test]
fn plans_boundary_refs_and_preserves_exact_inline_locator_facts() {
    let index = encode_chunk_index_v2(411, &[]).unwrap();
    let header = header_for_index(411, &index);
    let facts = series_page_facts(header);

    let mut page0_records = page_records(0, 409);
    page0_records[0] = inline_record(0, ChunkKind::Float, 0, 0, 0);
    page0_records[408] = inline_record(408, ChunkKind::Histogram, 1, 16, 0xaabb_ccdd);
    let (page0_descriptor, page0_bytes) =
        encode_series_hot_page_v1(header, 0, &page0_records, CHUNK_FILE_LENS).unwrap();
    let page0 = plan_schema7_hot_page(
        header,
        facts,
        0,
        page0_descriptor,
        &page0_bytes,
        CHUNK_FILE_LENS,
        &[0, 408],
    )
    .unwrap();
    assert_eq!(page0.len(), 2);
    assert_eq!(page0[0].series_ref, 0);
    assert_eq!(page0[0].cold_labels.keyset_id, 0);
    assert_eq!(page0[0].cold_labels.row, 20_000);
    let ChunkLocatorSource::Inline(first) = &page0[0].chunks else {
        panic!("expected inline locator");
    };
    assert_eq!(first.series_ref(), 0);
    assert_eq!(first.entry().file_id, 0);
    assert_eq!(first.entry().kind, ChunkKind::Float);
    assert_eq!(first.entry().min_time_ms, SEGMENT_START_MS);
    assert_eq!(first.entry().max_time_ms, SEGMENT_START_MS + 1);
    assert_eq!(first.entry().offset, 0);
    assert_eq!(first.entry().length, 48);
    assert_eq!(first.entry().scalar_lane_offset, 0);
    assert_eq!(first.entry().scalar_lane_len, 0);
    assert_eq!(
        first.authentication(),
        IndexedChunkAuthentication::Schema7 {
            indexed_prefix_crc32c: 0
        }
    );

    let ChunkLocatorSource::Inline(last) = &page0[1].chunks else {
        panic!("expected inline locator");
    };
    assert_eq!(last.series_ref(), 408);
    assert_eq!(last.entry().file_id, 1);
    assert_eq!(last.entry().kind, ChunkKind::Histogram);
    assert_eq!(last.entry().min_time_ms, SEGMENT_START_MS + 408);
    assert_eq!(last.entry().max_time_ms, SEGMENT_START_MS + 409);
    assert_eq!(last.entry().offset, 408 * 64);
    assert_eq!(last.entry().length, 64);
    assert_eq!(last.entry().scalar_lane_offset, 40);
    assert_eq!(last.entry().scalar_lane_len, 16);
    assert_eq!(
        last.authentication(),
        IndexedChunkAuthentication::Schema7 {
            indexed_prefix_crc32c: 0xaabb_ccdd
        }
    );

    let page1_records = vec![
        inline_record(409, ChunkKind::Int64, 0, 0, 9),
        inline_record(410, ChunkKind::ExponentialHistogram, 1, 24, 10),
    ];
    let (page1_descriptor, page1_bytes) =
        encode_series_hot_page_v1(header, 1, &page1_records, CHUNK_FILE_LENS).unwrap();
    let decoded_page1 =
        ValidatedSeriesHotPage::decode(header, 1, page1_descriptor, &page1_bytes, CHUNK_FILE_LENS)
            .unwrap();
    let page1 = plan_schema7_decoded_hot_page(
        header,
        facts,
        1,
        page1_descriptor,
        &decoded_page1,
        CHUNK_FILE_LENS,
        &[409, 410],
    )
    .unwrap();
    assert_eq!(
        page1
            .iter()
            .map(|series| series.series_ref)
            .collect::<Vec<_>>(),
        [409, 410]
    );
    let ChunkLocatorSource::Inline(last) = &page1[1].chunks else {
        panic!("expected inline locator");
    };
    assert_eq!(last.entry().file_id, 1);
    assert_eq!(last.entry().kind, ChunkKind::ExponentialHistogram);
    assert_eq!(last.entry().scalar_lane_offset, 40);
    assert_eq!(last.entry().scalar_lane_len, 24);
}

#[test]
fn hot_page_selection_requires_sorted_unique_refs_from_exact_page() {
    let index = encode_chunk_index_v2(411, &[]).unwrap();
    let header = header_for_index(411, &index);
    let facts = series_page_facts(header);
    let records = page_records(0, 409);
    let (descriptor, bytes) =
        encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
    let plan = |refs: &[u32]| {
        plan_schema7_hot_page(header, facts, 0, descriptor, &bytes, CHUNK_FILE_LENS, refs)
    };

    assert!(plan(&[]).unwrap().is_empty());
    assert_invalid(plan(&[408, 0]).unwrap_err(), "not sorted and unique");
    assert_invalid(plan(&[0, 0]).unwrap_err(), "not sorted and unique");
    assert_invalid(plan(&[409]).unwrap_err(), "does not belong");
}

#[test]
fn hot_page_authentication_and_bound_facts_are_mandatory() {
    let index = encode_chunk_index_v2(1, &[]).unwrap();
    let header = header_for_index(1, &index);
    let facts = series_page_facts(header);
    let records = page_records(0, 1);
    let (descriptor, bytes) =
        encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();

    let mut corrupt = bytes.clone();
    corrupt[100] ^= 1;
    assert_invalid(
        plan_schema7_hot_page(
            header,
            facts,
            0,
            descriptor,
            &corrupt,
            CHUNK_FILE_LENS,
            &[0],
        )
        .unwrap_err(),
        "CRC mismatch",
    );

    let mut substituted_facts = facts;
    substituted_facts.hot_pages_offset += 1;
    assert_invalid(
        plan_schema7_hot_page(
            header,
            substituted_facts,
            0,
            descriptor,
            &bytes,
            CHUNK_FILE_LENS,
            &[0],
        )
        .unwrap_err(),
        "page facts do not match",
    );

    let cached_page =
        ValidatedSeriesHotPage::decode(header, 0, descriptor, &bytes, CHUNK_FILE_LENS).unwrap();
    let substituted_descriptor = SeriesHotPageDescriptorV1 {
        page_crc32c: descriptor.page_crc32c ^ 1,
        ..descriptor
    };
    assert_invalid(
        plan_schema7_decoded_hot_page(
            header,
            facts,
            0,
            substituted_descriptor,
            &cached_page,
            CHUNK_FILE_LENS,
            &[0],
        )
        .unwrap_err(),
        "decode context does not match",
    );
}

#[test]
fn hot_page_cache_admission_rejects_impossible_keysets_and_measures_owned_bytes() {
    let index = encode_chunk_index_v2(1, &[]).unwrap();
    let header = header_for_index(1, &index);
    let records = page_records(0, 1);
    let (descriptor, bytes) =
        encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
    let page =
        ValidatedSeriesHotPage::decode(header, 0, descriptor, &bytes, CHUNK_FILE_LENS).unwrap();
    let expected_charge = std::mem::size_of::<ValidatedSeriesHotPage>() + SERIES_HOT_PAGE_LEN_V1;
    assert_eq!(page.charged_bytes().unwrap(), expected_charge as u64);
    assert_eq!(
        page.charged_bytes().unwrap(),
        ValidatedSeriesHotPage::declared_max_bytes(descriptor).unwrap()
    );
    let owned_page =
        ValidatedSeriesHotPage::decode_owned(header, 0, descriptor, bytes.clone(), CHUNK_FILE_LENS)
            .unwrap();
    assert_eq!(owned_page, page);

    let mut bad_records = records;
    bad_records[0].keyset_id = header.num_keysets;
    let (bad_descriptor, bad_bytes) =
        encode_series_hot_page_v1(header, 0, &bad_records, CHUNK_FILE_LENS).unwrap();
    assert_invalid(
        ValidatedSeriesHotPage::decode(header, 0, bad_descriptor, &bad_bytes, CHUNK_FILE_LENS)
            .unwrap_err(),
        "keyset ID is out of range",
    );
}

#[test]
fn hot_page_cache_hit_rejects_valid_header_and_file_length_substitution() {
    let index = encode_chunk_index_v2(1, &[]).unwrap();
    let header = header_for_index(1, &index);
    let facts = series_page_facts(header);
    let records = page_records(0, 1);
    let (descriptor, bytes) =
        encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
    let cached_page =
        ValidatedSeriesHotPage::decode(header, 0, descriptor, &bytes, CHUNK_FILE_LENS).unwrap();

    let substituted_header =
        header_for_index_with_bounds(1, &index, SEGMENT_START_MS + 10, SEGMENT_END_MS + 10);
    let substituted_facts = series_page_facts(substituted_header);
    assert!(
        ValidatedSeriesHotPage::decode(substituted_header, 0, descriptor, &bytes, CHUNK_FILE_LENS,)
            .is_ok(),
        "the same descriptor/page bytes are independently valid under the substituted header"
    );
    assert_invalid(
        plan_schema7_decoded_hot_page(
            substituted_header,
            substituted_facts,
            0,
            descriptor,
            &cached_page,
            CHUNK_FILE_LENS,
            &[0],
        )
        .unwrap_err(),
        "decode context does not match",
    );

    let substituted_chunk_file_lens = [CHUNK_FILE_LENS[0] - 1, CHUNK_FILE_LENS[1]];
    assert!(
        ValidatedSeriesHotPage::decode(header, 0, descriptor, &bytes, substituted_chunk_file_lens,)
            .is_ok(),
        "the same descriptor/page bytes are independently valid under both file inventories"
    );
    assert_invalid(
        plan_schema7_decoded_hot_page(
            header,
            facts,
            0,
            descriptor,
            &cached_page,
            substituted_chunk_file_lens,
            &[0],
        )
        .unwrap_err(),
        "decode context does not match",
    );
}

fn overflow_entry(
    file_id: u8,
    kind: ChunkKind,
    min_time_ms: u64,
    offset: u64,
    scalar_lane_len: u32,
    indexed_prefix_crc32c: u32,
) -> OverflowChunkEntryV1 {
    OverflowChunkEntryV1 {
        file_id,
        kind,
        min_time_ms,
        max_time_ms: min_time_ms + 1,
        offset,
        length: 48 + scalar_lane_len,
        scalar_lane_offset: u32::from(scalar_lane_len != 0) * 40,
        scalar_lane_len,
        indexed_prefix_crc32c,
    }
}

struct OverflowFixture {
    header: SeriesHeaderV3,
    root: ChunkOverflowRootV2,
    facts: Schema7OverflowBlobFacts,
    planned: PlannedSeries,
    blob_bytes: Vec<u8>,
}

fn overflow_fixture(
    series_count: u32,
    series_ref: u32,
    entries: Vec<OverflowChunkEntryV1>,
) -> OverflowFixture {
    let mask = kind_mask(entries.iter().map(|entry| entry.kind));
    let encoded = encode_chunk_index_v2(
        series_count,
        &[ChunkOverflowBlobV1 {
            series_ref,
            entries,
        }],
    )
    .unwrap();
    let locator = encoded.blob_locators[0];
    let blob_start = usize::try_from(locator.blob_offset).unwrap();
    let blob_end = blob_start + usize::try_from(locator.blob_len).unwrap();
    let root = encoded.root;
    OverflowFixture {
        header: header_for_index(series_count, &encoded),
        root,
        facts: overflow_blob_facts(root),
        planned: PlannedSeries {
            series_ref,
            kind_mask: mask,
            cold_labels: ColdLabelRowLocator {
                keyset_id: 3,
                row: 5,
            },
            chunks: ChunkLocatorSource::Overflow {
                locator,
                expected_kind_mask: mask,
            },
            expected_label_identity: 55,
        },
        blob_bytes: encoded.bytes[blob_start..blob_end].to_vec(),
    }
}

#[test]
fn overflow_mapping_preserves_stored_order_files_scalars_kinds_and_authentication() {
    let entries = vec![
        overflow_entry(0, ChunkKind::Float, 1_010, 64, 0, 0),
        overflow_entry(0, ChunkKind::Histogram, 1_020, 128, 16, 0x1111_2222),
        overflow_entry(
            1,
            ChunkKind::ExponentialHistogram,
            1_030,
            256,
            24,
            0x3333_4444,
        ),
    ];
    let fixture = overflow_fixture(16, 7, entries);
    let decoded_blob = ValidatedOverflowBlob::decode_bound(
        &fixture.blob_bytes,
        fixture.header,
        &fixture.root,
        fixture.facts,
        &fixture.planned,
        CHUNK_FILE_LENS,
    )
    .unwrap();
    let expected_charge = std::mem::size_of::<ValidatedOverflowBlob>() + fixture.blob_bytes.len();
    assert_eq!(
        decoded_blob.charged_bytes().unwrap(),
        expected_charge as u64
    );
    let ChunkLocatorSource::Overflow { locator, .. } = &fixture.planned.chunks else {
        unreachable!()
    };
    assert_eq!(
        decoded_blob.charged_bytes().unwrap(),
        ValidatedOverflowBlob::declared_max_bytes(*locator).unwrap()
    );
    let owned_blob = ValidatedOverflowBlob::decode_bound_owned(
        fixture.blob_bytes.clone(),
        fixture.header,
        &fixture.root,
        fixture.facts,
        &fixture.planned,
        CHUNK_FILE_LENS,
    )
    .unwrap();
    assert_eq!(owned_blob, decoded_blob);
    let batch = plan_schema7_decoded_overflow_blob(
        fixture.header,
        &fixture.root,
        fixture.facts,
        &fixture.planned,
        &decoded_blob,
        CHUNK_FILE_LENS,
    )
    .unwrap();

    assert_eq!(
        batch.series_spans,
        [SeriesChunkSpan {
            series_ref: 7,
            start: 0,
            len: 3,
        }]
    );
    assert_eq!(batch.locators.len(), 3);
    assert_eq!(batch.locators[0].entry().file_id, 0);
    assert_eq!(batch.locators[0].entry().kind, ChunkKind::Float);
    assert_eq!(batch.locators[0].entry().min_time_ms, 1_010);
    assert_eq!(batch.locators[0].entry().offset, 64);
    assert_eq!(batch.locators[0].entry().scalar_lane_offset, 0);
    assert_eq!(batch.locators[0].entry().scalar_lane_len, 0);
    assert_eq!(
        batch.locators[0].authentication(),
        IndexedChunkAuthentication::Schema7 {
            indexed_prefix_crc32c: 0,
        }
    );
    assert_eq!(batch.locators[1].entry().kind, ChunkKind::Histogram);
    assert_eq!(batch.locators[1].entry().scalar_lane_offset, 40);
    assert_eq!(batch.locators[1].entry().scalar_lane_len, 16);
    assert_eq!(batch.locators[2].entry().file_id, 1);
    assert_eq!(
        batch.locators[2].entry().kind,
        ChunkKind::ExponentialHistogram
    );
    assert_eq!(batch.locators[2].entry().scalar_lane_len, 24);
}

#[test]
fn overflow_rejects_segment_file_and_kind_mask_mismatches() {
    let before_segment = overflow_fixture(
        2,
        0,
        vec![
            overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS - 1, 0, 0, 1),
            overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS + 1, 64, 0, 2),
        ],
    );
    assert_invalid(
        ValidatedOverflowBlob::decode_bound(
            &before_segment.blob_bytes,
            before_segment.header,
            &before_segment.root,
            before_segment.facts,
            &before_segment.planned,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "outside its segment",
    );

    let file_bounds = overflow_fixture(
        2,
        0,
        vec![
            overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS, 0, 0, 1),
            overflow_entry(1, ChunkKind::Float, SEGMENT_START_MS + 1, 64, 0, 2),
        ],
    );
    assert_invalid(
        plan_schema7_overflow_blob(
            file_bounds.header,
            &file_bounds.root,
            file_bounds.facts,
            &file_bounds.planned,
            &file_bounds.blob_bytes,
            [1_000, 100],
        )
        .unwrap_err(),
        "file range is out of bounds",
    );

    let mut wrong_mask = overflow_fixture(
        2,
        0,
        vec![
            overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS, 0, 0, 1),
            overflow_entry(0, ChunkKind::Int64, SEGMENT_START_MS + 1, 64, 0, 2),
        ],
    );
    wrong_mask.planned.kind_mask = kind_bit(ChunkKind::Float);
    let ChunkLocatorSource::Overflow {
        expected_kind_mask, ..
    } = &mut wrong_mask.planned.chunks
    else {
        unreachable!()
    };
    *expected_kind_mask = wrong_mask.planned.kind_mask;
    assert_invalid(
        ValidatedOverflowBlob::decode_bound(
            &wrong_mask.blob_bytes,
            wrong_mask.header,
            &wrong_mask.root,
            wrong_mask.facts,
            &wrong_mask.planned,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "kind mask does not match",
    );
}

#[test]
fn overflow_rejects_crc_order_identity_and_bound_fact_substitution() {
    let entries = vec![
        overflow_entry(0, ChunkKind::Float, 1_010, 64, 0, 1),
        overflow_entry(0, ChunkKind::Float, 1_020, 128, 0, 2),
    ];
    let fixture = overflow_fixture(4, 0, entries.clone());

    let mut corrupt = fixture.blob_bytes.clone();
    corrupt[CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN + 10] ^= 1;
    assert_invalid(
        plan_schema7_overflow_blob(
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            &corrupt,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "crc mismatch",
    );

    let mut reversed = fixture.blob_bytes.clone();
    let first = CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN;
    let second = first + 44;
    for index in 0..44 {
        reversed.swap(first + index, second + index);
    }
    refresh_blob_crc(&mut reversed);
    assert_invalid(
        plan_schema7_overflow_blob(
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            &reversed,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "not strictly ordered",
    );

    let substituted = overflow_fixture(4, 1, entries);
    assert_eq!(fixture.blob_bytes.len(), substituted.blob_bytes.len());
    assert_invalid(
        plan_schema7_overflow_blob(
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            &substituted.blob_bytes,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "series_ref does not match",
    );

    let mut wrong_facts = fixture.facts;
    wrong_facts.blobs_len -= 1;
    assert_invalid(
        plan_schema7_overflow_blob(
            fixture.header,
            &fixture.root,
            wrong_facts,
            &fixture.planned,
            &fixture.blob_bytes,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "blob facts do not match",
    );
}

#[test]
fn one_entry_overflow_requires_an_actual_inline_width_exception() {
    let canonical = overflow_fixture(
        2,
        0,
        vec![overflow_entry(
            1,
            ChunkKind::Summary,
            SEGMENT_START_MS,
            u64::from(u32::MAX),
            SERIES_HOT_SCALAR_LANE_LEN_MAX,
            0,
        )],
    );
    assert_invalid(
        plan_schema7_overflow_blob(
            canonical.header,
            &canonical.root,
            canonical.facts,
            &canonical.planned,
            &canonical.blob_bytes,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "one-chunk overflow blob is noncanonical",
    );

    let width_exceptions = [
        overflow_fixture(
            2,
            0,
            vec![overflow_entry(
                0,
                ChunkKind::Float,
                SEGMENT_START_MS,
                u64::from(u32::MAX) + 1,
                0,
                0,
            )],
        ),
        overflow_fixture(
            2,
            0,
            vec![overflow_entry(
                0,
                ChunkKind::Histogram,
                SEGMENT_START_MS,
                0,
                SERIES_HOT_SCALAR_LANE_LEN_MAX + 1,
                0,
            )],
        ),
        overflow_fixture(
            2,
            0,
            vec![overflow_entry(
                0,
                ChunkKind::Float,
                SEGMENT_START_MS + u64::from(u32::MAX) + 1,
                0,
                0,
                0,
            )],
        ),
    ];
    for fixture in width_exceptions {
        let batch = plan_schema7_overflow_blob(
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            &fixture.blob_bytes,
            CHUNK_FILE_LENS,
        )
        .unwrap();
        assert_eq!(batch.locators.len(), 1);
        assert_eq!(batch.series_spans[0].len, 1);
    }
}

#[test]
fn overflow_source_identity_is_rechecked_before_blob_decode() {
    let mut fixture = overflow_fixture(
        2,
        0,
        vec![
            overflow_entry(0, ChunkKind::Float, 1_010, 0, 0, 1),
            overflow_entry(0, ChunkKind::Float, 1_020, 64, 0, 2),
        ],
    );
    let ChunkLocatorSource::Overflow { locator, .. } = &mut fixture.planned.chunks else {
        unreachable!()
    };
    locator.series_ref = 1;
    assert_invalid(
        plan_schema7_overflow_blob(
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            &fixture.blob_bytes,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "source does not match",
    );
}

#[test]
fn overflow_cache_hit_rejects_a_different_valid_root_for_the_same_blob() {
    let entries = vec![
        overflow_entry(0, ChunkKind::Float, 1_010, 0, 0, 1),
        overflow_entry(0, ChunkKind::Float, 1_020, 64, 0, 2),
    ];
    let fixture = overflow_fixture(4, 0, entries.clone());
    let ChunkLocatorSource::Overflow { locator, .. } = &fixture.planned.chunks else {
        unreachable!()
    };
    let locator = *locator;
    let cached_blob = ValidatedOverflowBlob::decode_bound(
        &fixture.blob_bytes,
        fixture.header,
        &fixture.root,
        fixture.facts,
        &fixture.planned,
        CHUNK_FILE_LENS,
    )
    .unwrap();

    let substituted_index = encode_chunk_index_v2(
        4,
        &[
            ChunkOverflowBlobV1 {
                series_ref: 0,
                entries,
            },
            ChunkOverflowBlobV1 {
                series_ref: 1,
                entries: vec![overflow_entry(1, ChunkKind::Summary, 1_030, 128, 16, 3)],
            },
        ],
    )
    .unwrap();
    let substituted_root = substituted_index.root;
    let substituted_locator = substituted_index.blob_locators[0];
    assert_eq!(substituted_locator, locator);
    assert_ne!(substituted_root, fixture.root);
    let blob_start = usize::try_from(substituted_locator.blob_offset).unwrap();
    let blob_end = blob_start + usize::try_from(substituted_locator.blob_len).unwrap();
    assert_eq!(
        &substituted_index.bytes[blob_start..blob_end],
        fixture.blob_bytes
    );

    assert_invalid(
        plan_schema7_decoded_overflow_blob(
            header_for_index(4, &substituted_index),
            &substituted_root,
            overflow_blob_facts(substituted_root),
            &fixture.planned,
            &cached_blob,
            CHUNK_FILE_LENS,
        )
        .unwrap_err(),
        "decode context does not match",
    );

    assert_invalid(
        plan_schema7_decoded_overflow_blob(
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            &cached_blob,
            [CHUNK_FILE_LENS[0] - 1, CHUNK_FILE_LENS[1]],
        )
        .unwrap_err(),
        "decode context does not match",
    );
}

fn refresh_blob_crc(bytes: &mut [u8]) {
    bytes[28..32].fill(0);
    let checksum = crc32c(bytes);
    bytes[28..32].copy_from_slice(&checksum.to_le_bytes());
}
