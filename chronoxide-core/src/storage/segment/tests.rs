use super::*;
use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkReader, ChunkSamples};
use crate::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue, SummaryQuantileValue,
    SummaryValue, TypedSampleMetadata,
};
use crate::storage::index::LabelValueTimeRange;
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_HISTOGRAM, SERIES_KIND_SUMMARY,
};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};

const FRAME_HEADER_LEN: u64 = 14;

#[path = "tests/footer_layout_and_corruption.rs"]
mod footer_layout_and_corruption;
#[path = "tests/metadata_resolution_and_cache.rs"]
mod metadata_resolution_and_cache;
#[path = "tests/projections_and_smoke.rs"]
mod projections_and_smoke;
#[path = "tests/query_labels_and_dedupe.rs"]
mod query_labels_and_dedupe;
#[path = "tests/writer_and_flush.rs"]
mod writer_and_flush;
#[path = "tests/writer_label_encoding.rs"]
mod writer_label_encoding;

fn paged_symbol_reader(
    symbols: &SegmentSymbols,
) -> crate::storage::symbols::SegmentSymbolReader<Cursor<Vec<u8>>> {
    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, symbols).unwrap();
    crate::storage::symbols::SegmentSymbolReader::open(Cursor::new(bytes)).unwrap()
}

fn index_reader_with_corrupt_label_fst(
    indexes: &SegmentIndexes,
    label_name_sym: u32,
) -> SegmentIndexReader<Cursor<Vec<u8>>> {
    const TRAILER_LEN: usize = 256;
    const TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET: usize = 104;
    const AUXILIARY_DIRECTORY_HEADER_LEN: usize = 64;
    const AUXILIARY_DIRECTORY_RECORD_LEN: usize = 40;
    const LABEL_VALUE_FST_KIND: u16 = 2;

    let mut bytes = Vec::new();
    write_segment_indexes_unbound_for_test(&mut bytes, indexes).unwrap();
    let trailer_start = bytes.len() - TRAILER_LEN;
    let auxiliary_directory_offset = u64::from_le_bytes(
        bytes[trailer_start + TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET
            ..trailer_start + TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    let entry_count = u64::from_le_bytes(
        bytes[auxiliary_directory_offset + 16..auxiliary_directory_offset + 24]
            .try_into()
            .unwrap(),
    ) as usize;

    let mut payload = None;
    for entry_index in 0..entry_count {
        let record_offset = auxiliary_directory_offset
            + AUXILIARY_DIRECTORY_HEADER_LEN
            + entry_index * AUXILIARY_DIRECTORY_RECORD_LEN;
        let kind = u16::from_le_bytes(bytes[record_offset..record_offset + 2].try_into().unwrap());
        let name = u32::from_le_bytes(
            bytes[record_offset + 4..record_offset + 8]
                .try_into()
                .unwrap(),
        );
        if kind == LABEL_VALUE_FST_KIND && name == label_name_sym {
            let offset = u64::from_le_bytes(
                bytes[record_offset + 8..record_offset + 16]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let len = u64::from_le_bytes(
                bytes[record_offset + 16..record_offset + 24]
                    .try_into()
                    .unwrap(),
            ) as usize;
            payload = Some(offset..offset + len);
            break;
        }
    }
    let payload = payload.expect("label FST auxiliary record");
    bytes[payload].fill(0);

    SegmentIndexReader::open(Cursor::new(bytes)).unwrap()
}

fn read_chunk_encoding(file: &mut File) -> u8 {
    file.seek(SeekFrom::Start(FRAME_HEADER_LEN + 1))
        .expect("seek to encoding");
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf).expect("read encoding");
    buf[0]
}

fn resolved_entry_labels(
    symbols: &SegmentSymbols,
    entry: &impl SeriesEntryView,
) -> Vec<(String, String)> {
    entry
        .labels()
        .iter()
        .map(|(key, value)| {
            (
                symbols.resolve(*key).unwrap().to_string(),
                symbols.resolve(*value).unwrap().to_string(),
            )
        })
        .collect()
}

fn footer_test_fixture(schema_version: u16) -> SegmentFooter {
    SegmentFooter {
        schema_version,
        files: SEGMENT_FOOTER_TRACKED_FILES
            .into_iter()
            .enumerate()
            .map(|(index, file)| SegmentFooterFile {
                file,
                size: 128 + index as u64 * 17,
                checksum_xxh64: 0x1122_3344_5566_7788 ^ index as u64,
            })
            .collect(),
    }
}

fn rewrite_footer_test_crc(bytes: &mut [u8]) {
    let payload_end = bytes.len() - SEGMENT_FOOTER_TRAILER_LEN;
    let header: &[u8; SEGMENT_FOOTER_HEADER_LEN] =
        bytes[..SEGMENT_FOOTER_HEADER_LEN].try_into().unwrap();
    let crc = segment_footer_crc(header, &bytes[SEGMENT_FOOTER_HEADER_LEN..payload_end]);
    bytes[payload_end..].copy_from_slice(&crc.to_le_bytes());
}

fn write_footer_test_files(dir: &Path) {
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        fs::write(
            dir.join(file.filename()),
            format!("content:{}", file.filename()),
        )
        .unwrap();
    }
}

fn rewrite_symbols_and_footer_as_schema5_v2_for_layout_ab(segment_dir: &Path) {
    let symbols_path = segment_dir.join(SegmentFile::Symbols.filename());
    let symbols = read_symbols_bin(File::open(&symbols_path).unwrap()).unwrap();
    let mut string_bytes = Vec::new();
    let mut offsets = Vec::with_capacity(symbols.len() + 1);
    offsets.push(0u64);
    for symbol_id in 0..symbols.len() {
        string_bytes.extend_from_slice(
            symbols
                .resolve(u32::try_from(symbol_id).unwrap())
                .unwrap()
                .as_bytes(),
        );
        offsets.push(u64::try_from(string_bytes.len()).unwrap());
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&crate::storage::symbols::SYMBOLS_V3_MAGIC.to_le_bytes());
    encoded.extend_from_slice(
        &crate::storage::symbols::SYMBOLS_V2_VERSION_FOR_LAYOUT_AB.to_le_bytes(),
    );
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&u32::try_from(symbols.len()).unwrap().to_le_bytes());
    for offset in offsets {
        encoded.extend_from_slice(&offset.to_le_bytes());
    }
    encoded.extend_from_slice(&string_bytes);
    fs::write(symbols_path, encoded).unwrap();

    let mut footer = build_segment_footer_for_schema6(segment_dir).unwrap();
    footer.schema_version = LEGACY_SEGMENT_SCHEMA_VERSION_FOR_LAYOUT_AB;
    fs::write(
        segment_dir.join(SegmentFile::Footer.filename()),
        encode_segment_footer(&footer).unwrap(),
    )
    .unwrap();
}
