use std::collections::BTreeMap;
use std::io::Cursor;

use crc32c::crc32c;

use super::super::{
    InlineChunkV3, OverflowChunksV3, SERIES_HOT_PAGE_LEN_V1, SeriesHotLocationV3,
    decode_series_hot_page_v1,
};
use super::*;
use crate::storage::chunk::{ChunkEncoding, ChunkKind, decode_chunk_index_v2};
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SERIES_KIND_INT64,
    SERIES_KIND_SUMMARY,
};

const SEGMENT_START_MS: u64 = 1_000;
const SEGMENT_END_MS: u64 = 1_000_000;
const SCALAR_MAGIC: u32 = u32::from_le_bytes(*b"TSCL");

#[derive(Debug, Clone)]
struct Fixture {
    series: Vec<SeriesEntry>,
    chunks: Vec<Vec<ChunkIndexEntry>>,
    files: [Vec<u8>; 2],
}

impl Fixture {
    fn empty() -> Self {
        Self {
            series: Vec::new(),
            chunks: Vec::new(),
            files: [Vec::new(), Vec::new()],
        }
    }

    fn push_series(
        &mut self,
        series_id: u64,
        kind_mask: u8,
        labels: Vec<(u32, u32)>,
        chunks: Vec<ChunkIndexEntry>,
    ) {
        self.series.push(SeriesEntry {
            series_id,
            kind_mask,
            chunk_index: Default::default(),
            labels,
        });
        self.chunks.push(chunks);
    }

    fn append_chunk(
        &mut self,
        series_ref: u32,
        file_id: u8,
        kind: ChunkKind,
        min_time_ms: u64,
        max_time_ms: u64,
        scalar_body_len: Option<u32>,
    ) -> ChunkIndexEntry {
        let file = &mut self.files[usize::from(file_id)];
        let offset = file.len() as u64;
        let encoding = encoding_for(kind);
        let scalar_lane_len = scalar_body_len
            .map(|body_len| 16u32.checked_add(body_len).unwrap())
            .unwrap_or(0);
        let scalar_lane_offset = if scalar_lane_len == 0 { 0 } else { 40 };
        let header_len = 40u32.checked_add(scalar_lane_len).unwrap();
        let payload = [series_ref as u8 ^ 0xa5, kind as u8, 0x55, 0xaa];
        let payload_len = payload.len() as u32;
        let length = header_len.checked_add(payload_len).unwrap();

        let mut chunk = vec![0u8; length as usize];
        chunk[0] = kind as u8;
        chunk[1] = encoding as u8;
        put_u16_test(&mut chunk, 2, 0);
        put_u32_test(&mut chunk, 4, series_ref);
        put_u64_test(&mut chunk, 8, min_time_ms);
        put_u64_test(&mut chunk, 16, max_time_ms);
        put_u32_test(&mut chunk, 24, 1);
        put_u32_test(&mut chunk, 28, header_len);
        put_u32_test(&mut chunk, 32, payload_len);
        put_u32_test(&mut chunk, 36, crc32c(&payload));
        if let Some(body_len) = scalar_body_len {
            put_u32_test(&mut chunk, 40, SCALAR_MAGIC);
            put_u16_test(&mut chunk, 44, 1);
            put_u16_test(&mut chunk, 46, 0);
            put_u32_test(&mut chunk, 48, body_len);
            let body_start = 56usize;
            let body_end = body_start + body_len as usize;
            for (index, byte) in chunk[body_start..body_end].iter_mut().enumerate() {
                *byte = index as u8 ^ 0x3c;
            }
            let body_crc32c = crc32c(&chunk[body_start..body_end]);
            put_u32_test(&mut chunk, 52, body_crc32c);
        }
        chunk[header_len as usize..].copy_from_slice(&payload);
        file.extend_from_slice(&chunk);
        ChunkIndexEntry {
            file_id,
            kind,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset,
            length,
            scalar_lane_offset,
            scalar_lane_len,
        }
    }
}

#[derive(Debug)]
struct RunOutput {
    series: Vec<u8>,
    chunk_index: Vec<u8>,
    result: Schema7SeriesAssemblyResult,
}

fn run(fixture: &Fixture) -> io::Result<RunOutput> {
    let chunks_source = Cursor::new(fixture.files[0].clone());
    let ooo_source = Cursor::new(fixture.files[1].clone());
    let mut series = Cursor::new(Vec::new());
    let mut chunk_index = Cursor::new(Vec::new());
    let result = write_schema7_series_and_chunk_index(
        &mut series,
        &mut chunk_index,
        Schema7SeriesAssemblyInput {
            series_entries: &fixture.series,
            chunk_entries: &fixture.chunks,
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            chunk_file_lens: [fixture.files[0].len() as u64, fixture.files[1].len() as u64],
            chunk_sources: [&chunks_source, &ooo_source],
        },
    )?;
    Ok(RunOutput {
        series: series.into_inner(),
        chunk_index: chunk_index.into_inner(),
        result,
    })
}

fn validate_streamed_output(fixture: &Fixture, output: &RunOutput) -> Vec<SeriesHotV3> {
    assert_eq!(
        output.series.len() as u64,
        output.result.stats.series_file_len
    );
    assert_eq!(
        output.chunk_index.len() as u64,
        output.result.stats.chunk_index_file_len
    );
    let root_len = output.result.series_header.hot_pages_offset as usize;
    let root = decode_series_root_v3(&output.series[..root_len]).unwrap();
    assert_eq!(root.header, output.result.series_header);

    let decoded_chunk_index = decode_chunk_index_v2(&output.chunk_index).unwrap();
    assert_eq!(decoded_chunk_index.root, output.result.chunk_index_root);
    assert_eq!(
        root.header.num_series,
        decoded_chunk_index.root.series_count
    );
    assert_eq!(
        root.header.chunk_index_root_crc32c,
        decoded_chunk_index.root.root_crc32c
    );
    assert_eq!(
        root.header.chunk_index_file_len,
        decoded_chunk_index.root.file_len
    );

    let mut records = Vec::new();
    for (page_index, descriptor) in root.hot_descriptors.iter().copied().enumerate() {
        let start = root.header.hot_pages_offset as usize + page_index * SERIES_HOT_PAGE_LEN_V1;
        let end = start + SERIES_HOT_PAGE_LEN_V1;
        let page = decode_series_hot_page_v1(
            root.header,
            page_index as u32,
            descriptor,
            &output.series[start..end],
            [fixture.files[0].len() as u64, fixture.files[1].len() as u64],
        )
        .unwrap();
        records.extend(page.records);
    }
    assert_eq!(records.len(), fixture.series.len());

    let mut overflow_ordinal = 0usize;
    for (series_ref, record) in records.iter().enumerate() {
        if let SeriesHotLocationV3::Overflow(overflow) = record.location {
            let locator = decoded_chunk_index.blob_locators[overflow_ordinal];
            assert_eq!(locator.series_ref, series_ref as u32);
            assert_eq!(locator.blob_offset, overflow.blob_offset);
            assert_eq!(locator.blob_len, overflow.blob_len);
            assert_eq!(locator.chunk_count, overflow.chunk_count);
            overflow_ordinal += 1;
        }
    }
    assert_eq!(overflow_ordinal, decoded_chunk_index.blobs.len());

    for descriptor in &root.cold_descriptors {
        let start = root.header.keysets_offset as usize
            + descriptor.page_index as usize * SERIES_COLD_PAGE_LEN_V1 as usize;
        let end = start + descriptor.page_len as usize;
        assert_eq!(crc32c(&output.series[start..end]), descriptor.page_crc32c);
    }

    let cold = SeriesColdV2Plan::build(&fixture.series).unwrap();
    let offsets = cold.section_offsets_at(root.header.keysets_offset).unwrap();
    let mut expected_cold = Vec::new();
    cold.write_sections_at(&mut expected_cold, offsets).unwrap();
    assert_eq!(
        &output.series[root.header.keysets_offset as usize..],
        expected_cold.as_slice()
    );
    records
}

fn assert_repeatable(fixture: &Fixture, first: &RunOutput) {
    let second = run(fixture).unwrap();
    assert_eq!(first.series, second.series);
    assert_eq!(first.chunk_index, second.chunk_index);
    assert_eq!(first.result, second.result);
}

fn cold_writer_header(cold_len: usize) -> SeriesHeaderV3 {
    assert!(cold_len >= 48);
    SeriesHeaderV3::new(SeriesHeaderV3Params {
        num_series: 1,
        num_keysets: 1,
        num_value_dicts: 1,
        chunk_index_root_crc32c: 0,
        keysets_len: 16,
        value_dicts_len: 16,
        keyset_blocks_len: (cold_len - 32) as u64,
        segment_start_ms: SEGMENT_START_MS,
        segment_end_ms: SEGMENT_END_MS,
        chunk_index_file_len: 64,
    })
    .unwrap()
}

#[derive(Debug, Default)]
struct RecordingWriter {
    bytes: Vec<u8>,
    write_sizes: Vec<usize>,
    flushes: usize,
}

impl Write for RecordingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_sizes.push(bytes.len());
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        Ok(())
    }
}

#[test]
fn cold_page_buffer_preserves_fragmented_cross_boundary_bytes_and_crcs() {
    let final_page_len = 137usize;
    let total_len = COLD_PAGE_BUFFER_LEN * 2 + final_page_len;
    let bytes: Vec<_> = (0..total_len)
        .map(|index| (index as u8).wrapping_mul(37).wrapping_add(11))
        .collect();
    let split_points = [
        0,
        1,
        3,
        COLD_PAGE_BUFFER_LEN - 3,
        COLD_PAGE_BUFFER_LEN + 5,
        COLD_PAGE_BUFFER_LEN * 2 + 1,
        total_len,
    ];
    let header = cold_writer_header(total_len);
    let mut output = RecordingWriter::default();
    let descriptors = {
        let mut writer = ColdPageCrcWriter::new(&mut output, header).unwrap();
        for bounds in split_points.windows(2) {
            writer.write_all(&bytes[bounds[0]..bounds[1]]).unwrap();
        }
        writer.finish().unwrap()
    };

    assert_eq!(output.bytes, bytes);
    assert_eq!(
        output.write_sizes,
        [COLD_PAGE_BUFFER_LEN, COLD_PAGE_BUFFER_LEN, final_page_len]
    );
    assert_eq!(descriptors.len(), 3);
    for (page_index, descriptor) in descriptors.iter().enumerate() {
        let start = page_index * COLD_PAGE_BUFFER_LEN;
        let end = (start + COLD_PAGE_BUFFER_LEN).min(bytes.len());
        assert_eq!(descriptor.page_index, page_index as u32);
        assert_eq!(descriptor.page_len, (end - start) as u32);
        assert_eq!(descriptor.page_crc32c, crc32c(&bytes[start..end]));
    }
}

#[test]
fn cold_page_buffer_flushes_pending_bytes_without_splitting_the_page() {
    let bytes: Vec<_> = (0..257).map(|index| index as u8 ^ 0xa5).collect();
    let header = cold_writer_header(bytes.len());
    let mut output = RecordingWriter::default();
    let descriptors = {
        let mut writer = ColdPageCrcWriter::new(&mut output, header).unwrap();
        writer.write_all(&bytes[..17]).unwrap();
        assert!(writer.inner.bytes.is_empty());
        writer.flush().unwrap();
        assert_eq!(writer.inner.bytes, bytes[..17]);
        assert_eq!(writer.inner.flushes, 1);
        writer.write_all(&bytes[17..]).unwrap();
        writer.finish().unwrap()
    };

    assert_eq!(output.bytes, bytes);
    assert_eq!(output.write_sizes, [17, 240]);
    assert_eq!(output.flushes, 1);
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].page_len, 257);
    assert_eq!(descriptors[0].page_crc32c, crc32c(&bytes));
}

#[derive(Debug)]
struct FailAfterWriter {
    bytes: Vec<u8>,
    fail_after: usize,
}

impl Write for FailAfterWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = self.fail_after.saturating_sub(self.bytes.len());
        if remaining == 0 {
            return Err(io::Error::other("injected cold-page write failure"));
        }
        let written = remaining.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn cold_page_buffer_propagates_partial_sink_failure_without_a_descriptor() {
    let bytes = vec![0x5a; COLD_PAGE_BUFFER_LEN + 1];
    let header = cold_writer_header(bytes.len());
    let mut output = FailAfterWriter {
        bytes: Vec::new(),
        fail_after: 257,
    };
    let error = {
        let mut writer = ColdPageCrcWriter::new(&mut output, header).unwrap();
        let error = writer.write_all(&bytes).unwrap_err();
        assert_eq!(writer.page_len, COLD_PAGE_BUFFER_LEN as u32);
        assert_eq!(writer.emitted_len, 257);
        assert!(writer.descriptors.is_empty());
        error
    };

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(output.bytes, bytes[..257]);
}

#[test]
fn empty_stream_is_canonical_repeatable_and_decoder_bound() {
    let fixture = Fixture::empty();
    let output = run(&fixture).unwrap();
    assert_repeatable(&fixture, &output);
    assert!(validate_streamed_output(&fixture, &output).is_empty());
    assert_eq!(output.series.len(), 4_120);
    assert_eq!(output.chunk_index.len(), 64);
    assert_eq!(output.result.stats.series_count, 0);
    assert_eq!(output.result.stats.hot_page_count, 0);
    assert_eq!(output.result.stats.cold_page_count, 1);
    assert_eq!(output.result.stats.peak_hot_records_buffered, 0);
    assert_eq!(output.result.stats.first_prefix_reads, 0);
    assert_eq!(output.result.stats.second_prefix_reads, 0);
    assert_eq!(crc32c(&output.series), 0x06e2_50d9);
    assert_eq!(crc32c(&output.chunk_index), 0x573e_a947);
}

#[test]
fn one_record_stream_has_deterministic_golden_bytes_and_bound_decode() {
    let fixture = many_inline_fixture(1);
    let output = run(&fixture).unwrap();
    assert_repeatable(&fixture, &output);
    let records = validate_streamed_output(&fixture, &output);
    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].location,
        SeriesHotLocationV3::Inline(InlineChunkV3 { file_id: 0, .. })
    ));
    assert_eq!(output.result.stats.inline_series_count, 1);
    assert_eq!(output.result.stats.overflow_series_count, 0);
    assert_eq!(output.result.stats.first_prefix_reads, 0);
    assert_eq!(output.result.stats.second_prefix_reads, 1);
    assert_eq!(output.series.len(), 20_569);
    assert_eq!(output.chunk_index.len(), 64);
    assert_eq!(crc32c(&output.series), 0x298e_c7b5);
    assert_eq!(crc32c(&output.chunk_index), 0x9301_79d2);
}

#[test]
fn inline_and_ooo_records_are_repeatable_and_read_each_exact_prefix_once() {
    let mut fixture = Fixture::empty();
    let in_order = fixture.append_chunk(
        0,
        0,
        ChunkKind::Float,
        SEGMENT_START_MS,
        SEGMENT_START_MS + 1,
        None,
    );
    fixture.push_series(10, SERIES_KIND_FLOAT, vec![(1, 10)], vec![in_order]);
    let ooo = fixture.append_chunk(
        1,
        1,
        ChunkKind::Histogram,
        SEGMENT_START_MS + 2,
        SEGMENT_START_MS + 3,
        Some(8),
    );
    fixture.push_series(20, SERIES_KIND_HISTOGRAM, vec![(1, 20)], vec![ooo]);

    let output = run(&fixture).unwrap();
    assert_repeatable(&fixture, &output);
    let records = validate_streamed_output(&fixture, &output);
    assert_eq!(output.result.stats.inline_series_count, 2);
    assert_eq!(output.result.stats.overflow_series_count, 0);
    assert_eq!(output.result.stats.first_prefix_reads, 0);
    assert_eq!(output.result.stats.second_prefix_reads, 2);
    assert_eq!(output.result.stats.first_prefix_bytes, 0);
    assert_eq!(output.result.stats.second_prefix_bytes, 96);
    assert!(matches!(
        records[0].location,
        SeriesHotLocationV3::Inline(InlineChunkV3 { file_id: 0, .. })
    ));
    assert!(matches!(
        records[1].location,
        SeriesHotLocationV3::Inline(InlineChunkV3 { file_id: 1, .. })
    ));
    assert_eq!(output.series.len(), 20_575);
    assert_eq!(crc32c(&output.series), 0x9c01_795f);
    assert_eq!(crc32c(&output.chunk_index), 0xdaad_7e9c);
}

#[test]
fn multi_chunk_series_streams_one_complete_bound_overflow_blob() {
    let mut fixture = Fixture::empty();
    let first = fixture.append_chunk(
        0,
        0,
        ChunkKind::ExponentialHistogram,
        SEGMENT_START_MS,
        SEGMENT_START_MS + 10,
        Some(4),
    );
    let second = fixture.append_chunk(
        0,
        1,
        ChunkKind::Summary,
        SEGMENT_START_MS + 20,
        SEGMENT_START_MS + 30,
        Some(12),
    );
    fixture.push_series(
        30,
        SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY,
        vec![(1, 30), (2, 31)],
        vec![first, second],
    );

    let output = run(&fixture).unwrap();
    assert_repeatable(&fixture, &output);
    let records = validate_streamed_output(&fixture, &output);
    assert_eq!(output.result.stats.inline_series_count, 0);
    assert_eq!(output.result.stats.overflow_series_count, 1);
    assert_eq!(output.result.stats.first_prefix_reads, 2);
    assert_eq!(output.result.stats.second_prefix_reads, 0);
    assert_eq!(output.result.stats.first_prefix_bytes, 112);
    assert_eq!(output.result.stats.second_prefix_bytes, 0);
    let decoded = decode_chunk_index_v2(&output.chunk_index).unwrap();
    assert_eq!(decoded.blobs.len(), 1);
    assert_eq!(decoded.blobs[0].entries.len(), 2);
    assert!(matches!(
        records[0].location,
        SeriesHotLocationV3::Overflow(OverflowChunksV3 { chunk_count: 2, .. })
    ));
    assert_eq!(output.series.len(), 20_594);
    assert_eq!(output.chunk_index.len(), 184);
    assert_eq!(crc32c(&output.series), 0x1f73_5abf);
    assert_eq!(crc32c(&output.chunk_index), 0x0ab9_a80c);
}

#[test]
fn mixed_inline_and_overflow_series_read_every_prefix_exactly_once() {
    let mut fixture = Fixture::empty();
    let first_inline = fixture.append_chunk(
        0,
        0,
        ChunkKind::Float,
        SEGMENT_START_MS,
        SEGMENT_START_MS + 1,
        None,
    );
    fixture.push_series(10, SERIES_KIND_FLOAT, vec![(1, 10)], vec![first_inline]);

    let first_overflow = fixture.append_chunk(
        1,
        0,
        ChunkKind::ExponentialHistogram,
        SEGMENT_START_MS + 2,
        SEGMENT_START_MS + 3,
        Some(4),
    );
    let second_overflow = fixture.append_chunk(
        1,
        0,
        ChunkKind::Summary,
        SEGMENT_START_MS + 4,
        SEGMENT_START_MS + 5,
        Some(12),
    );
    fixture.push_series(
        20,
        SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY,
        vec![(1, 20)],
        vec![first_overflow, second_overflow],
    );

    let last_inline = fixture.append_chunk(
        2,
        1,
        ChunkKind::Int64,
        SEGMENT_START_MS + 6,
        SEGMENT_START_MS + 7,
        None,
    );
    fixture.push_series(30, SERIES_KIND_INT64, vec![(1, 30)], vec![last_inline]);

    let output = run(&fixture).unwrap();
    assert_repeatable(&fixture, &output);
    let records = validate_streamed_output(&fixture, &output);
    assert_eq!(records.len(), 3);
    assert_eq!(output.result.stats.inline_series_count, 2);
    assert_eq!(output.result.stats.overflow_series_count, 1);
    assert_eq!(output.result.stats.first_prefix_reads, 2);
    assert_eq!(output.result.stats.first_prefix_bytes, 112);
    assert_eq!(output.result.stats.second_prefix_reads, 2);
    assert_eq!(output.result.stats.second_prefix_bytes, 80);
    assert!(matches!(
        records[0].location,
        SeriesHotLocationV3::Inline(_)
    ));
    assert!(matches!(
        records[1].location,
        SeriesHotLocationV3::Overflow(_)
    ));
    assert!(matches!(
        records[2].location,
        SeriesHotLocationV3::Inline(_)
    ));
}

#[test]
fn hot_page_boundary_at_409_and_410_records_is_exact_and_bounded() {
    let output_409 = run(&many_inline_fixture(409)).unwrap();
    assert_eq!(output_409.result.stats.hot_page_count, 1);
    assert_eq!(output_409.result.stats.peak_hot_records_buffered, 409);
    validate_streamed_output(&many_inline_fixture(409), &output_409);

    let fixture_410 = many_inline_fixture(410);
    let output_410 = run(&fixture_410).unwrap();
    assert_repeatable(&fixture_410, &output_410);
    let records = validate_streamed_output(&fixture_410, &output_410);
    assert_eq!(records.len(), 410);
    assert_eq!(output_410.result.stats.hot_page_count, 2);
    assert_eq!(output_410.result.stats.peak_hot_records_buffered, 409);
    let root = decode_series_root_v3(
        &output_410.series[..output_410.result.series_header.hot_pages_offset as usize],
    )
    .unwrap();
    assert_eq!(root.hot_descriptors[0].record_count, 409);
    assert_eq!(root.hot_descriptors[1].first_series_ref, 409);
    assert_eq!(root.hot_descriptors[1].record_count, 1);
    assert_eq!(output_409.series.len(), 23_019);
    assert_eq!(crc32c(&output_409.series), 0x53f7_f16c);
    assert_eq!(output_410.series.len(), 39_409);
    assert_eq!(crc32c(&output_410.series), 0x98af_0ea5);
}

#[test]
fn cold_record_crossing_16k_is_authenticated_across_section_boundaries() {
    let mut fixture = Fixture::empty();
    let chunk = fixture.append_chunk(
        0,
        0,
        ChunkKind::Int64,
        SEGMENT_START_MS,
        SEGMENT_START_MS + 1,
        None,
    );
    let labels = (0..5_000u32)
        .map(|key| (key, key.wrapping_add(100_000)))
        .collect();
    fixture.push_series(40, SERIES_KIND_INT64, labels, vec![chunk]);

    let output = run(&fixture).unwrap();
    assert_repeatable(&fixture, &output);
    validate_streamed_output(&fixture, &output);
    assert!(output.result.stats.cold_page_count >= 3);
    let header = output.result.series_header;
    let keyset_entry_start = header.keysets_offset + 16;
    let keyset_entry_end = keyset_entry_start + 8 + 5_000 * 4;
    let first_cold_boundary = header.keysets_offset + SERIES_COLD_PAGE_LEN_V1;
    assert!(keyset_entry_start < first_cold_boundary);
    assert!(keyset_entry_end > first_cold_boundary);
    assert_eq!(output.series.len(), 145_544);
    assert_eq!(crc32c(&output.series), 0x7244_58d6);
    assert_eq!(output.result.stats.cold_page_count, 8);
}

#[test]
fn substituted_prefix_and_exact_length_mismatches_fail_before_publication() {
    let mut fixture = many_inline_fixture(2);
    put_u32_test(&mut fixture.files[0], 4, 1);
    let error = run(&fixture).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let mut fixture = many_inline_fixture(1);
    fixture.files[0].push(0);
    fixture.chunks[0][0].length += 1;
    let error = run(&fixture).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let fixture = many_inline_fixture(1);
    let chunks_source = Cursor::new(fixture.files[0].clone());
    let ooo_source = Cursor::new(Vec::<u8>::new());
    let mut series = Cursor::new(Vec::new());
    let mut chunk_index = Cursor::new(Vec::new());
    let error = write_schema7_series_and_chunk_index(
        &mut series,
        &mut chunk_index,
        Schema7SeriesAssemblyInput {
            series_entries: &fixture.series,
            chunk_entries: &fixture.chunks,
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            chunk_file_lens: [fixture.files[0].len() as u64 + 1, 0],
            chunk_sources: [&chunks_source, &ooo_source],
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(series.into_inner().is_empty());
    assert!(chunk_index.into_inner().is_empty());
}

#[test]
fn one_chunk_offset_over_u32_streams_through_overflow_without_allocation() {
    let offset = u64::from(u32::MAX) + 1;
    let (entry, prefix) = standalone_chunk_prefix(
        0,
        ChunkKind::Float,
        SEGMENT_START_MS,
        SEGMENT_START_MS,
        offset,
    );
    let file_len = offset + u64::from(entry.length);
    let chunks_source = SparseSource::new(file_len, [(offset, prefix)]);
    let ooo_source = SparseSource::new(0, []);
    let series_entries = [SeriesEntry {
        series_id: 50,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(1, 1)],
    }];
    let chunk_entries = [vec![entry]];
    let mut series = Cursor::new(Vec::new());
    let mut chunk_index = Cursor::new(Vec::new());
    let result = write_schema7_series_and_chunk_index(
        &mut series,
        &mut chunk_index,
        Schema7SeriesAssemblyInput {
            series_entries: &series_entries,
            chunk_entries: &chunk_entries,
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            chunk_file_lens: [file_len, 0],
            chunk_sources: [&chunks_source, &ooo_source],
        },
    )
    .unwrap();
    assert_eq!(result.stats.inline_series_count, 0);
    assert_eq!(result.stats.overflow_series_count, 1);
    assert_eq!(
        decode_chunk_index_v2(chunk_index.get_ref())
            .unwrap()
            .blobs
            .len(),
        1
    );
}

fn many_inline_fixture(series_count: u32) -> Fixture {
    let mut fixture = Fixture::empty();
    for series_ref in 0..series_count {
        let timestamp = SEGMENT_START_MS + u64::from(series_ref);
        let chunk =
            fixture.append_chunk(series_ref, 0, ChunkKind::Float, timestamp, timestamp, None);
        fixture.push_series(
            1_000 + u64::from(series_ref),
            SERIES_KIND_FLOAT,
            vec![(1, series_ref + 10)],
            vec![chunk],
        );
    }
    fixture
}

fn encoding_for(kind: ChunkKind) -> ChunkEncoding {
    match kind {
        ChunkKind::Float => ChunkEncoding::RawF64,
        ChunkKind::Int64 => ChunkEncoding::RawI64,
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary => {
            ChunkEncoding::SchemaVarLen
        }
    }
}

fn standalone_chunk_prefix(
    series_ref: u32,
    kind: ChunkKind,
    min_time_ms: u64,
    max_time_ms: u64,
    offset: u64,
) -> (ChunkIndexEntry, Vec<u8>) {
    let payload_len = 4u32;
    let length = 40 + payload_len;
    let mut prefix = vec![0u8; 40];
    prefix[0] = kind as u8;
    prefix[1] = encoding_for(kind) as u8;
    put_u32_test(&mut prefix, 4, series_ref);
    put_u64_test(&mut prefix, 8, min_time_ms);
    put_u64_test(&mut prefix, 16, max_time_ms);
    put_u32_test(&mut prefix, 24, 1);
    put_u32_test(&mut prefix, 28, 40);
    put_u32_test(&mut prefix, 32, payload_len);
    (
        ChunkIndexEntry {
            file_id: 0,
            kind,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset,
            length,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        },
        prefix,
    )
}

#[derive(Debug)]
struct SparseSource {
    len: u64,
    ranges: BTreeMap<u64, Vec<u8>>,
}

impl SparseSource {
    fn new<const N: usize>(len: u64, ranges: [(u64, Vec<u8>); N]) -> Self {
        Self {
            len,
            ranges: ranges.into_iter().collect(),
        }
    }
}

impl SegmentIndexReadAt for SparseSource {
    fn len(&self) -> io::Result<u64> {
        Ok(self.len)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        let source = self.ranges.get(&offset).ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "sparse test range is missing")
        })?;
        let source = source.get(..destination.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "sparse test range is short")
        })?;
        destination.copy_from_slice(source);
        Ok(())
    }
}

fn put_u16_test(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32_test(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64_test(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
