use super::*;
use crate::storage::chunk::{CHUNK_FLAG_HAS_START_TIME, ChunkEncoding};

const SEGMENT_START_MS: u64 = 10;
const SEGMENT_END_MS: u64 = SEGMENT_START_MS + u32::MAX as u64 + 2;
const SERIES_REF: u32 = 7;

#[derive(Debug, Clone)]
struct TestChunk {
    entry: ChunkIndexEntry,
    encoding: ChunkEncoding,
    indexed_prefix: Vec<u8>,
}

impl TestChunk {
    fn new(kind: ChunkKind, file_id: u8, time: u64, offset: u64) -> Self {
        let entry = ChunkIndexEntry {
            file_id,
            kind,
            flags: 0,
            min_time_ms: time,
            max_time_ms: time,
            offset,
            length: 64,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        };
        let encoding = encoding_for(kind);
        let indexed_prefix = build_indexed_prefix(&entry, encoding, SERIES_REF, 1);
        Self {
            entry,
            encoding,
            indexed_prefix,
        }
    }

    fn refresh_prefix(&mut self) {
        self.indexed_prefix = build_indexed_prefix(&self.entry, self.encoding, SERIES_REF, 1);
    }

    fn set_scalar_lane(&mut self, scalar_lane_len: u32) {
        self.entry.scalar_lane_offset = CHUNK_HEADER_LEN_V1;
        self.entry.scalar_lane_len = scalar_lane_len;
        self.entry.length = CHUNK_HEADER_LEN_V1
            .checked_add(scalar_lane_len)
            .and_then(|length| length.checked_add(1))
            .unwrap();
        self.refresh_prefix();
    }
}

fn encoding_for(kind: ChunkKind) -> ChunkEncoding {
    match kind {
        ChunkKind::Float => ChunkEncoding::Gorilla,
        ChunkKind::Int64 => ChunkEncoding::IntDeltaZigZag,
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary => {
            ChunkEncoding::SchemaVarLen
        }
    }
}

fn build_indexed_prefix(
    entry: &ChunkIndexEntry,
    encoding: ChunkEncoding,
    series_ref: u32,
    num_points: u32,
) -> Vec<u8> {
    let has_scalar_lane = entry.scalar_lane_len != 0;
    let prefix_len = if has_scalar_lane { 56 } else { 40 };
    let mut prefix = vec![0u8; prefix_len];
    prefix[0] = entry.kind as u8;
    prefix[1] = encoding as u8;
    put_test_u16(&mut prefix, 2, entry.flags);
    put_test_u32(&mut prefix, 4, series_ref);
    put_test_u64(&mut prefix, 8, entry.min_time_ms);
    put_test_u64(&mut prefix, 16, entry.max_time_ms);
    put_test_u32(&mut prefix, 24, num_points);
    let header_len = CHUNK_HEADER_LEN_V1
        .checked_add(entry.scalar_lane_len)
        .unwrap();
    put_test_u32(&mut prefix, 28, header_len);
    put_test_u32(
        &mut prefix,
        32,
        entry
            .length
            .checked_sub(header_len)
            .expect("test chunk must cover its header"),
    );
    put_test_u32(&mut prefix, 36, 0x89ab_cdef);

    if has_scalar_lane {
        assert!(entry.scalar_lane_len >= TYPED_SCALAR_LANE_HEADER_LEN_V1);
        put_test_u32(&mut prefix, 40, u32::from_le_bytes(*b"TSCL"));
        put_test_u16(&mut prefix, 44, 1);
        put_test_u16(&mut prefix, 46, 0);
        put_test_u32(
            &mut prefix,
            48,
            entry.scalar_lane_len - TYPED_SCALAR_LANE_HEADER_LEN_V1,
        );
        put_test_u32(&mut prefix, 52, 0x7654_3210);
    }
    prefix
}

fn final_entries(chunks: &[TestChunk]) -> Vec<FinalChunkIndexEntryV3<'_>> {
    chunks
        .iter()
        .map(|chunk| FinalChunkIndexEntryV3 {
            entry: &chunk.entry,
            indexed_prefix: &chunk.indexed_prefix,
        })
        .collect()
}

fn input<'a>(
    kind_mask: u8,
    chunks: &'a [FinalChunkIndexEntryV3<'a>],
) -> SeriesClassifierInputV3<'a> {
    SeriesClassifierInputV3 {
        series_ref: SERIES_REF,
        series_id: 11,
        keyset_id: 13,
        row: 17,
        kind_mask,
        segment_start_ms: SEGMENT_START_MS,
        segment_end_ms: SEGMENT_END_MS,
        chunk_file_lens: [u64::from(u32::MAX) + 128, u64::from(u32::MAX) + 128],
        chunks,
    }
}

fn classify(kind_mask: u8, chunks: &[TestChunk]) -> io::Result<ClassifiedSeriesV3> {
    classify_with_file_lens(
        kind_mask,
        chunks,
        [u64::from(u32::MAX) + 128, u64::from(u32::MAX) + 128],
    )
}

fn classify_with_file_lens(
    kind_mask: u8,
    chunks: &[TestChunk],
    chunk_file_lens: [u64; 2],
) -> io::Result<ClassifiedSeriesV3> {
    let final_entries = final_entries(chunks);
    let mut classifier_input = input(kind_mask, &final_entries);
    classifier_input.chunk_file_lens = chunk_file_lens;
    classify_series_v3(classifier_input)
}

fn expect_inline(result: ClassifiedSeriesV3) -> InlineChunkV3 {
    let ClassifiedSeriesV3::Inline(record) = result else {
        panic!("expected inline classification");
    };
    let SeriesHotLocationV3::Inline(inline) = record.location else {
        panic!("expected inline hot record");
    };
    inline
}

fn expect_overflow(result: ClassifiedSeriesV3) -> PendingOverflowSeriesV3 {
    let ClassifiedSeriesV3::Overflow(overflow) = result else {
        panic!("expected overflow classification");
    };
    overflow
}

#[test]
fn valid_40_byte_prefix_is_inline_and_deterministic() {
    let chunks = [TestChunk::new(ChunkKind::Float, 0, 20, 30)];
    let expected_crc = crc32c(&chunks[0].indexed_prefix);
    let first = classify(kind_bit(ChunkKind::Float), &chunks).unwrap();
    let second = classify(kind_bit(ChunkKind::Float), &chunks).unwrap();
    assert_eq!(first, second);
    let inline = expect_inline(first);
    assert_eq!(inline.chunk_kind, ChunkKind::Float as u8);
    assert_eq!(inline.file_id, 0);
    assert_eq!(inline.min_time_delta_ms, 10);
    assert_eq!(inline.file_offset, 30);
    assert_eq!(inline.indexed_prefix_crc32c, expected_crc);
}

#[test]
fn valid_56_byte_prefix_is_inline_and_uses_verified_flags() {
    let mut chunk = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    chunk.entry.flags = CHUNK_FLAG_HAS_START_TIME;
    chunk.set_scalar_lane(17);
    let expected_crc = crc32c(&chunk.indexed_prefix);
    let inline = expect_inline(classify(kind_bit(ChunkKind::Histogram), &[chunk]).unwrap());
    assert_eq!(inline.scalar_lane_len, 17);
    assert_eq!(inline.indexed_prefix_crc32c, expected_crc);
}

#[test]
fn exact_u32_time_and_offset_boundaries_are_inline() {
    let chunks = [TestChunk::new(
        ChunkKind::Int64,
        0,
        SEGMENT_START_MS + u32::MAX as u64,
        u64::from(u32::MAX),
    )];
    assert!(chunk_entry_fits_inline(SEGMENT_START_MS, &chunks[0].entry));
    let inline = expect_inline(classify(kind_bit(ChunkKind::Int64), &chunks).unwrap());
    assert_eq!(inline.min_time_delta_ms, u32::MAX);
    assert_eq!(inline.max_time_delta_ms, u32::MAX);
    assert_eq!(inline.file_offset, u32::MAX);
}

#[test]
fn one_over_u32_time_or_offset_uses_overflow() {
    let time_chunks = [TestChunk::new(
        ChunkKind::Float,
        0,
        SEGMENT_START_MS + u32::MAX as u64 + 1,
        10,
    )];
    assert!(!chunk_entry_fits_inline(
        SEGMENT_START_MS,
        &time_chunks[0].entry
    ));
    assert!(matches!(
        classify(kind_bit(ChunkKind::Float), &time_chunks).unwrap(),
        ClassifiedSeriesV3::Overflow(_)
    ));

    let offset_chunks = [TestChunk::new(
        ChunkKind::Float,
        0,
        20,
        u64::from(u32::MAX) + 1,
    )];
    assert!(!chunk_entry_fits_inline(
        SEGMENT_START_MS,
        &offset_chunks[0].entry
    ));
    assert!(matches!(
        classify(kind_bit(ChunkKind::Float), &offset_chunks).unwrap(),
        ClassifiedSeriesV3::Overflow(_)
    ));
}

#[test]
fn scalar_lane_21_bit_boundary_selects_inline_then_overflow() {
    let mut at_max = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    at_max.set_scalar_lane(SERIES_HOT_SCALAR_LANE_LEN_MAX);
    assert!(chunk_entry_fits_inline(SEGMENT_START_MS, &at_max.entry));
    let inline =
        expect_inline(classify(kind_bit(ChunkKind::Histogram), &[at_max.clone()]).unwrap());
    assert_eq!(inline.scalar_lane_len, SERIES_HOT_SCALAR_LANE_LEN_MAX);

    let mut one_over = at_max;
    one_over.set_scalar_lane(SERIES_HOT_SCALAR_LANE_LEN_MAX + 1);
    assert!(!chunk_entry_fits_inline(SEGMENT_START_MS, &one_over.entry));
    assert!(matches!(
        classify(kind_bit(ChunkKind::Histogram), &[one_over]).unwrap(),
        ClassifiedSeriesV3::Overflow(_)
    ));
}

#[test]
fn multiple_chunks_emit_a_complete_ordered_overflow_blob_and_bind_identity() {
    let chunks = [
        TestChunk::new(ChunkKind::Float, 0, 20, 30),
        TestChunk::new(ChunkKind::Float, 0, 21, 94),
    ];
    let expected_crcs = [
        crc32c(&chunks[0].indexed_prefix),
        crc32c(&chunks[1].indexed_prefix),
    ];
    let overflow = expect_overflow(classify(kind_bit(ChunkKind::Float), &chunks).unwrap());
    assert_eq!(overflow.blob.series_ref, SERIES_REF);
    assert_eq!(overflow.blob.entries.len(), 2);
    assert_eq!(overflow.blob.entries[0].offset, 30);
    assert_eq!(overflow.blob.entries[1].offset, 94);
    assert_eq!(
        overflow
            .blob
            .entries
            .iter()
            .map(|entry| entry.indexed_prefix_crc32c)
            .collect::<Vec<_>>(),
        expected_crcs
    );

    let blob_len = checked_chunk_overflow_blob_len(2).unwrap();
    let locator = ChunkOverflowBlobLocatorV1 {
        series_ref: SERIES_REF,
        blob_offset: CHUNK_INDEX_ROOT_LEN_V2,
        blob_len,
        chunk_count: 2,
    };
    let record = overflow
        .bind_blob_locator(locator, CHUNK_INDEX_ROOT_LEN_V2 + u64::from(blob_len))
        .unwrap();
    assert_eq!(record.series_id, 11);
    assert_eq!(record.keyset_id, 13);
    assert_eq!(record.row, 17);
    assert_eq!(record.kind_mask, kind_bit(ChunkKind::Float));
}

#[test]
fn ooo_only_chunk_can_inline_and_mixed_kind_lane_series_overflows() {
    let ooo = [TestChunk::new(ChunkKind::Summary, 1, 20, 30)];
    let inline = expect_inline(classify(kind_bit(ChunkKind::Summary), &ooo).unwrap());
    assert_eq!(inline.file_id, 1);

    let mut scalar = TestChunk::new(ChunkKind::Summary, 1, 22, 30);
    scalar.set_scalar_lane(TYPED_SCALAR_LANE_HEADER_LEN_V1);
    let mixed = [TestChunk::new(ChunkKind::Histogram, 0, 20, 30), scalar];
    let expected_mask = kind_bit(ChunkKind::Histogram) | kind_bit(ChunkKind::Summary);
    let overflow = expect_overflow(classify(expected_mask, &mixed).unwrap());
    assert_eq!(overflow.blob.entries[0].file_id, 0);
    assert_eq!(overflow.blob.entries[1].file_id, 1);
    assert_eq!(overflow.blob.entries[1].scalar_lane_len, 16);
}

#[test]
fn zero_chunks_and_kind_mask_mismatches_are_rejected() {
    assert!(classify(kind_bit(ChunkKind::Float), &[]).is_err());

    let chunks = [TestChunk::new(ChunkKind::Float, 0, 20, 30)];
    assert!(classify(kind_bit(ChunkKind::Int64), &chunks).is_err());
    assert!(classify(0, &chunks).is_err());
    assert!(classify(VALID_KIND_MASK | 0x80, &chunks).is_err());
}

#[test]
fn malformed_locator_shapes_are_rejected_not_overflowed() {
    let mut reversed = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    reversed.entry.max_time_ms = 19;
    assert!(classify(kind_bit(ChunkKind::Float), &[reversed]).is_err());

    let outside = [TestChunk::new(ChunkKind::Float, 0, SEGMENT_END_MS, 30)];
    assert!(classify(kind_bit(ChunkKind::Float), &outside).is_err());

    let bad_file = [TestChunk::new(ChunkKind::Float, 2, 20, 30)];
    assert!(classify(kind_bit(ChunkKind::Float), &bad_file).is_err());

    let out_of_bounds = [TestChunk::new(ChunkKind::Float, 0, 20, 200)];
    assert!(classify_with_file_lens(kind_bit(ChunkKind::Float), &out_of_bounds, [250, 0]).is_err());

    let mut bad_scalar = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    bad_scalar.entry.scalar_lane_offset = CHUNK_HEADER_LEN_V1;
    bad_scalar.entry.scalar_lane_len = 15;
    assert!(classify(kind_bit(ChunkKind::Histogram), &[bad_scalar]).is_err());

    let mut partial_scalar = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    partial_scalar.entry.scalar_lane_len = 16;
    assert!(classify(kind_bit(ChunkKind::Histogram), &[partial_scalar]).is_err());

    let mut scalar_past_chunk = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    scalar_past_chunk.entry.scalar_lane_offset = CHUNK_HEADER_LEN_V1;
    scalar_past_chunk.entry.scalar_lane_len = 25;
    assert!(classify(kind_bit(ChunkKind::Histogram), &[scalar_past_chunk]).is_err());

    let mut scalar_float = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    scalar_float.set_scalar_lane(16);
    assert!(classify(kind_bit(ChunkKind::Float), &[scalar_float]).is_err());
}

#[test]
fn malformed_authenticated_chunk_header_is_rejected_before_classification() {
    let mut zero_points = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    put_test_u32(&mut zero_points.indexed_prefix, 24, 0);
    assert!(classify(kind_bit(ChunkKind::Float), &[zero_points]).is_err());

    let mut wrong_series = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    put_test_u32(&mut wrong_series.indexed_prefix, 4, SERIES_REF + 1);
    assert!(classify(kind_bit(ChunkKind::Float), &[wrong_series]).is_err());

    let mut wrong_kind = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    wrong_kind.indexed_prefix[0] = ChunkKind::Int64 as u8;
    wrong_kind.indexed_prefix[1] = ChunkEncoding::IntDeltaZigZag as u8;
    assert!(classify(kind_bit(ChunkKind::Float), &[wrong_kind]).is_err());

    let mut wrong_time = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    put_test_u64(&mut wrong_time.indexed_prefix, 8, 21);
    assert!(classify(kind_bit(ChunkKind::Float), &[wrong_time]).is_err());

    let mut wrong_encoding = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    wrong_encoding.indexed_prefix[1] = ChunkEncoding::Gorilla as u8;
    assert!(classify(kind_bit(ChunkKind::Histogram), &[wrong_encoding]).is_err());

    let mut locator_flags_mismatch = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    locator_flags_mismatch.entry.flags = CHUNK_FLAG_HAS_START_TIME;
    assert!(classify(kind_bit(ChunkKind::Histogram), &[locator_flags_mismatch],).is_err());

    let mut invalid_header_flags = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    invalid_header_flags.entry.flags = 1;
    invalid_header_flags.refresh_prefix();
    assert!(classify(kind_bit(ChunkKind::Float), &[invalid_header_flags]).is_err());
}

#[test]
fn malformed_exact_lengths_and_prefix_spans_are_rejected() {
    let mut wrong_header_len = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    put_test_u32(&mut wrong_header_len.indexed_prefix, 28, 41);
    assert!(classify(kind_bit(ChunkKind::Float), &[wrong_header_len]).is_err());

    let mut wrong_payload_len = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    put_test_u32(&mut wrong_payload_len.indexed_prefix, 32, 25);
    assert!(classify(kind_bit(ChunkKind::Float), &[wrong_payload_len]).is_err());

    let mut truncated = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    truncated.indexed_prefix.pop();
    assert!(classify(kind_bit(ChunkKind::Float), &[truncated]).is_err());

    let mut trailing = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    trailing.indexed_prefix.push(0);
    assert!(classify(kind_bit(ChunkKind::Float), &[trailing]).is_err());
}

#[test]
fn malformed_scalar_header_is_rejected_before_width_selection() {
    let mut bad_body_len = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    bad_body_len.set_scalar_lane(17);
    put_test_u32(&mut bad_body_len.indexed_prefix, 48, 2);
    assert!(classify(kind_bit(ChunkKind::Histogram), &[bad_body_len]).is_err());

    let mut bad_scalar_flags = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    bad_scalar_flags.set_scalar_lane(17);
    put_test_u16(&mut bad_scalar_flags.indexed_prefix, 46, 1);
    assert!(classify(kind_bit(ChunkKind::Histogram), &[bad_scalar_flags]).is_err());

    let mut truncated = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    truncated.set_scalar_lane(17);
    truncated.indexed_prefix.pop();
    assert!(classify(kind_bit(ChunkKind::Histogram), &[truncated]).is_err());

    let mut trailing = TestChunk::new(ChunkKind::Histogram, 0, 20, 30);
    trailing.set_scalar_lane(17);
    trailing.indexed_prefix.push(0);
    assert!(classify(kind_bit(ChunkKind::Histogram), &[trailing]).is_err());
}

#[test]
fn duplicate_and_unordered_entries_are_rejected() {
    let duplicate = TestChunk::new(ChunkKind::Float, 0, 20, 30);
    assert!(classify(kind_bit(ChunkKind::Float), &[duplicate.clone(), duplicate],).is_err());

    let unordered = [
        TestChunk::new(ChunkKind::Float, 1, 20, 30),
        TestChunk::new(ChunkKind::Float, 0, 21, 94),
    ];
    assert!(classify(kind_bit(ChunkKind::Float), &unordered).is_err());
}

#[test]
fn mismatched_encoder_locator_is_rejected() {
    let chunks = [
        TestChunk::new(ChunkKind::Float, 0, 20, 30),
        TestChunk::new(ChunkKind::Float, 0, 21, 94),
    ];
    let overflow = expect_overflow(classify(kind_bit(ChunkKind::Float), &chunks).unwrap());
    let blob_len = checked_chunk_overflow_blob_len(2).unwrap();
    let wrong_series = ChunkOverflowBlobLocatorV1 {
        series_ref: SERIES_REF + 1,
        blob_offset: 64,
        blob_len,
        chunk_count: 2,
    };
    assert!(
        overflow
            .bind_blob_locator(wrong_series, 64 + u64::from(blob_len))
            .is_err()
    );

    let past_eof = ChunkOverflowBlobLocatorV1 {
        series_ref: SERIES_REF,
        blob_offset: 65,
        blob_len,
        chunk_count: 2,
    };
    assert!(
        overflow
            .bind_blob_locator(past_eof, 64 + u64::from(blob_len))
            .is_err()
    );
}

fn put_test_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_test_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_test_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
