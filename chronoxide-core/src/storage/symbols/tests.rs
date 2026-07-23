use std::io::{self, Cursor, ErrorKind};

use crc32c::crc32c;

use super::format::{
    ROOT_CRC_OFFSET, SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB, SymbolPageDescriptor, SymbolRoot,
    put_u16, put_u32, put_u64, read_u16_at, read_u32_at, read_u64_at, symbols_root_crc,
};
use super::reader::symbols_short_read;
use super::writer::{SymbolWriterOperationalLimits, write_symbols_bin_v3_with_operational_limits};
use super::*;

struct HeaderOnlySource {
    header: [u8; SYMBOLS_V3_HEADER_LEN],
    file_len: u64,
}

impl SegmentSymbolReadAt for HeaderOnlySource {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file_len)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        if offset == 0 && destination.len() == self.header.len() {
            destination.copy_from_slice(&self.header);
            return Ok(());
        }
        Err(symbols_short_read())
    }
}

struct SparsePrefixSource {
    prefix: Vec<u8>,
    file_len: u64,
}

impl SegmentSymbolReadAt for SparsePrefixSource {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file_len)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        let start = usize::try_from(offset).map_err(|_| symbols_short_read())?;
        let end = start
            .checked_add(destination.len())
            .ok_or_else(symbols_short_read)?;
        let source = self.prefix.get(start..end).ok_or_else(symbols_short_read)?;
        destination.copy_from_slice(source);
        Ok(())
    }
}

fn encoded(values: &[String]) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_symbols_bin_v3(&mut bytes, values).unwrap();
    bytes
}

fn encoded_v2_for_layout_ab(values: &[String]) -> Vec<u8> {
    let mut strings = Vec::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0u64);
    for value in values {
        strings.extend_from_slice(value.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SYMBOLS_V3_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&SYMBOLS_V2_VERSION_FOR_LAYOUT_AB.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for offset in offsets {
        bytes.extend_from_slice(&offset.to_le_bytes());
    }
    bytes.extend_from_slice(&strings);
    bytes
}

fn page_count(bytes: &[u8]) -> u32 {
    read_u32_at(bytes, 20)
}

fn descriptor_offset(page_index: usize) -> usize {
    SYMBOLS_V3_HEADER_LEN + page_index * SYMBOLS_V3_PAGE_DESCRIPTOR_LEN
}

fn page_offset(bytes: &[u8], page_index: usize) -> usize {
    read_u64_at(bytes, descriptor_offset(page_index) + 8) as usize
}

#[derive(Clone, Copy)]
enum TestFieldValue {
    U16(u16),
    U32(u32),
    U64(u64),
}

impl TestFieldValue {
    fn write(self, bytes: &mut [u8], offset: usize) {
        match self {
            Self::U16(value) => put_u16(bytes, offset, value),
            Self::U32(value) => put_u32(bytes, offset, value),
            Self::U64(value) => put_u64(bytes, offset, value),
        }
    }
}

fn repair_root_crc_with_len(bytes: &mut [u8], root_len: usize) {
    put_u32(bytes, ROOT_CRC_OFFSET, 0);
    let root_crc = symbols_root_crc(&bytes[..root_len]);
    put_u32(bytes, ROOT_CRC_OFFSET, root_crc);
}

fn repair_root_crc(bytes: &mut [u8]) {
    let root_len = read_u64_at(bytes, 56) as usize;
    repair_root_crc_with_len(bytes, root_len);
}

fn repair_page_and_root_crcs(bytes: &mut [u8], page_index: usize) {
    let descriptor = descriptor_offset(page_index);
    let page_offset = read_u64_at(bytes, descriptor + 8) as usize;
    let page_len = read_u32_at(bytes, descriptor + 16) as usize;
    let page_crc = crc32c(&bytes[page_offset..page_offset + page_len]);
    put_u32(bytes, descriptor + 20, page_crc);
    repair_root_crc(bytes);
}

fn multi_page_values() -> Vec<String> {
    (0..5_000)
        .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
        .collect()
}

#[test]
fn v3_roundtrips_sorted_values_and_lazy_lookups() {
    let values = vec![
        "".to_string(),
        "__name__".to_string(),
        "alpha".to_string(),
        "omega".to_string(),
    ];
    let bytes = encoded(&values);
    assert_eq!(read_u16_at(&bytes, 4), SYMBOLS_V3_VERSION);
    assert_eq!(
        read_symbols_bin_v3(Cursor::new(bytes.clone())).unwrap(),
        values
    );

    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    assert_eq!(reader.len(), 4);
    assert_eq!(reader.lookup("alpha").unwrap(), Some(2));
    assert_eq!(reader.lookup("missing").unwrap(), None);
    assert_eq!(reader.resolve(3).unwrap().unwrap().as_str(), "omega");
    assert!(reader.resolve(4).unwrap().is_none());
    assert_eq!(
        reader.lookup_many(&["", "omega", "zeta"]).unwrap(),
        vec![Some(0), Some(3), None]
    );
}

#[test]
fn lookup_many_groups_cross_page_duplicates_and_preserves_misses() {
    let values = multi_page_values();
    let bytes = encoded(&values);
    let reader = SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
    assert!(reader.state.root.descriptors.len() > 1);
    let second_page_id = reader.state.root.descriptors[1].first_symbol_id as usize;
    let in_page_miss = format!("{}!", values[0]);
    let queries = vec![
        values[second_page_id].clone(),
        values[0].clone(),
        values[second_page_id].clone(),
        in_page_miss,
        "!before-first".to_string(),
        "zzzz-after-last".to_string(),
        values[0].clone(),
    ];

    assert_eq!(
        reader.lookup_many(&queries).unwrap(),
        vec![
            Some(second_page_id as u32),
            Some(0),
            Some(second_page_id as u32),
            None,
            None,
            None,
            Some(0),
        ]
    );
    let stats = reader.read_stats();
    assert_eq!(stats.page.calls, 2);
    assert_eq!(stats.page_cache_misses, 2);
    assert_eq!(stats.page_cache_hits, 0);
    assert_eq!(stats.logical_returned.calls, 4);
    assert_eq!(
        stats.logical_returned.bytes,
        (2 * values[0].len() + 2 * values[second_page_id].len()) as u64
    );
}

#[test]
fn resolve_many_groups_cross_page_duplicates_and_preserves_out_of_range_ids() {
    let values = multi_page_values();
    let bytes = encoded(&values);
    let reader = SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
    assert!(reader.state.root.descriptors.len() > 1);
    let second_page_id = reader.state.root.descriptors[1].first_symbol_id;
    let ids = [
        second_page_id,
        0,
        second_page_id,
        u32::MAX,
        0,
        reader.state.root.symbol_count,
    ];

    let resolved = reader.resolve_many(&ids).unwrap();
    assert_eq!(resolved.len(), ids.len());
    assert_eq!(
        resolved
            .iter()
            .map(|value| value.as_ref().map(|value| value.as_str()))
            .collect::<Vec<_>>(),
        vec![
            Some(values[second_page_id as usize].as_str()),
            Some(values[0].as_str()),
            Some(values[second_page_id as usize].as_str()),
            None,
            Some(values[0].as_str()),
            None,
        ]
    );
    assert_eq!(resolved[0].as_ref().unwrap().symbol_id(), second_page_id);
    assert_eq!(resolved[1].as_ref().unwrap().symbol_id(), 0);
    assert_eq!(resolved[2].as_ref().unwrap().symbol_id(), second_page_id);
    assert_eq!(resolved[4].as_ref().unwrap().symbol_id(), 0);
    let stats = reader.read_stats();
    assert_eq!(stats.page.calls, 2);
    assert_eq!(stats.page_cache_misses, 2);
    assert_eq!(stats.page_cache_hits, 0);
    assert_eq!(stats.logical_returned.calls, 4);
    assert_eq!(
        stats.logical_returned.bytes,
        (2 * values[0].len() + 2 * values[second_page_id as usize].len()) as u64
    );
}

#[test]
fn batched_page_load_order_and_sticky_corruption_match_scalar_requests() {
    let values = multi_page_values();
    let mut bytes = encoded(&values);
    assert!(page_count(&bytes) > 1);
    let second_page_id = read_u32_at(&bytes, descriptor_offset(1));
    let first_page_offset = page_offset(&bytes, 0);
    put_u32(&mut bytes, first_page_offset, 0);
    repair_page_and_root_crcs(&mut bytes, 0);
    let second_page_offset = page_offset(&bytes, 1);
    put_u32(&mut bytes, second_page_offset + 28, 1);
    repair_page_and_root_crcs(&mut bytes, 1);

    let reader = SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
    let error = reader
        .lookup_many(&[values[second_page_id as usize].as_str(), values[0].as_str()])
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols page reserved field is non-zero");
    assert_eq!(reader.read_stats().page.calls, 1);
    assert_eq!(reader.read_stats().page_cache_misses, 1);

    let sticky = reader.resolve_many(&[u32::MAX]).unwrap_err();
    assert_eq!(sticky.to_string(), error.to_string());
    assert!(reader.lookup_many::<&str>(&[]).unwrap().is_empty());
    assert!(reader.resolve_many(&[]).unwrap().is_empty());
}

#[test]
fn visitor_propagates_touched_corruption_before_a_later_missing_id() {
    let values = multi_page_values();
    let mut bytes = encoded(&values);
    let first_page_offset = page_offset(&bytes, 0);
    put_u32(&mut bytes, first_page_offset, 0);
    repair_page_and_root_crcs(&mut bytes, 0);

    let reader =
        SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes.clone()), 0).unwrap();
    let error = reader
        .visit_resolved_many(&[0, u32::MAX], |_, _| Ok(()))
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols page magic mismatch");
    assert_eq!(reader.read_stats().page.calls, 1);
    let sticky = reader.resolve(u32::MAX).unwrap_err();
    assert_eq!(sticky.to_string(), error.to_string());

    let missing_first =
        SegmentSymbolReader::open_with_cache_max_bytes(Cursor::new(bytes), 0).unwrap();
    let mut visits = 0;
    let all_resolved = missing_first
        .visit_resolved_many(&[u32::MAX, 0], |_, _| {
            visits += 1;
            Ok(())
        })
        .unwrap();
    assert!(!all_resolved);
    assert_eq!(visits, 0);
    assert_eq!(missing_first.read_stats().page.calls, 0);
}

#[test]
fn empty_v3_root_is_deterministic_and_decodable() {
    let first = encoded(&[]);
    let second = encoded(&[]);
    assert_eq!(first, second);
    assert_eq!(first.len(), SYMBOLS_V3_HEADER_LEN);
    assert_eq!(read_u32_at(&first, 16), 0);
    assert_eq!(read_u32_at(&first, 20), 0);
    assert_eq!(read_u64_at(&first, 56), SYMBOLS_V3_HEADER_LEN as u64);
    assert_eq!(read_u64_at(&first, 64), SYMBOLS_V3_HEADER_LEN as u64);
    assert!(read_symbols_bin_v3(Cursor::new(first)).unwrap().is_empty());
}

#[test]
fn singleton_v3_layout_and_checksums_match_the_golden_encoding() {
    let values = vec!["a".to_string()];
    let first = encoded(&values);
    let second = encoded(&values);
    assert_eq!(first, second);
    assert_eq!(first.len(), 171);

    assert_eq!(&first[0..4], b"SYMB");
    assert_eq!(read_u16_at(&first, 4), 3);
    assert_eq!(read_u16_at(&first, 6), 0);
    assert_eq!(read_u32_at(&first, 8), 80);
    assert_eq!(read_u32_at(&first, 12), 48);
    assert_eq!(read_u32_at(&first, 16), 1);
    assert_eq!(read_u32_at(&first, 20), 1);
    assert_eq!(read_u64_at(&first, 24), 80);
    assert_eq!(read_u64_at(&first, 32), 48);
    assert_eq!(read_u64_at(&first, 40), 128);
    assert_eq!(read_u64_at(&first, 48), 2);
    assert_eq!(read_u64_at(&first, 56), 130);
    assert_eq!(read_u64_at(&first, 64), 171);
    assert_eq!(read_u32_at(&first, 72), 0x04ca_4a2c);
    assert_eq!(read_u32_at(&first, 76), 0);

    let descriptor = descriptor_offset(0);
    assert_eq!(read_u32_at(&first, descriptor), 0);
    assert_eq!(read_u32_at(&first, descriptor + 4), 1);
    assert_eq!(read_u64_at(&first, descriptor + 8), 130);
    assert_eq!(read_u32_at(&first, descriptor + 16), 41);
    assert_eq!(read_u32_at(&first, descriptor + 20), 0xd58e_45db);
    assert_eq!(read_u32_at(&first, descriptor + 24), 0);
    assert_eq!(read_u32_at(&first, descriptor + 28), 1);
    assert_eq!(read_u32_at(&first, descriptor + 32), 1);
    assert_eq!(read_u32_at(&first, descriptor + 36), 1);
    assert_eq!(read_u32_at(&first, descriptor + 40), 1);
    assert_eq!(read_u32_at(&first, descriptor + 44), 0);
    assert_eq!(&first[128..130], b"aa");

    let page = 130;
    assert_eq!(&first[page..page + 4], b"SYPG");
    assert_eq!(read_u16_at(&first, page + 4), 1);
    assert_eq!(read_u16_at(&first, page + 6), 0);
    assert_eq!(read_u32_at(&first, page + 8), 0);
    assert_eq!(read_u32_at(&first, page + 12), 0);
    assert_eq!(read_u32_at(&first, page + 16), 1);
    assert_eq!(read_u32_at(&first, page + 20), 8);
    assert_eq!(read_u32_at(&first, page + 24), 1);
    assert_eq!(read_u32_at(&first, page + 28), 0);
    assert_eq!(read_u32_at(&first, page + 32), 0);
    assert_eq!(read_u32_at(&first, page + 36), 1);
    assert_eq!(&first[page + 40..], b"a");
    assert_eq!(read_symbols_bin_v3(Cursor::new(first)).unwrap(), values);
}

#[test]
fn root_rejects_impossible_page_count_before_root_allocation() {
    let mut header = [0u8; SYMBOLS_V3_HEADER_LEN];
    put_u32(&mut header, 0, SYMBOLS_V3_MAGIC);
    put_u16(&mut header, 4, SYMBOLS_V3_VERSION);
    put_u32(&mut header, 8, SYMBOLS_V3_HEADER_LEN as u32);
    put_u32(&mut header, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
    put_u32(&mut header, 16, 1);
    put_u32(&mut header, 20, 2);

    let error = SegmentSymbolReader::open(HeaderOnlySource {
        header,
        file_len: SYMBOLS_V3_HEADER_LEN as u64,
    })
    .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols page count exceeds symbol count");
}

#[test]
fn root_rejects_operational_size_limit_before_reading_the_root() {
    let root_len = SYMBOLS_V3_MAX_ROOT_BYTES as u64 + 1;
    let mut header = [0u8; SYMBOLS_V3_HEADER_LEN];
    put_u32(&mut header, 0, SYMBOLS_V3_MAGIC);
    put_u16(&mut header, 4, SYMBOLS_V3_VERSION);
    put_u32(&mut header, 8, SYMBOLS_V3_HEADER_LEN as u32);
    put_u32(&mut header, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
    put_u32(&mut header, 16, 1);
    put_u32(&mut header, 20, 1);
    put_u64(&mut header, 24, SYMBOLS_V3_HEADER_LEN as u64);
    put_u64(&mut header, 32, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u64);
    let fence_offset = (SYMBOLS_V3_HEADER_LEN + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN) as u64;
    put_u64(&mut header, 40, fence_offset);
    put_u64(&mut header, 48, root_len - fence_offset);
    put_u64(&mut header, 56, root_len);
    put_u64(&mut header, 64, root_len);

    let error = SegmentSymbolReader::open(HeaderOnlySource {
        header,
        file_len: root_len,
    })
    .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "symbols root exceeds the operational size limit"
    );
}

#[test]
fn root_rejects_page_size_limit_before_allocating_the_page() {
    let page_len = u32::try_from(SYMBOLS_V3_MAX_PAGE_BYTES + 1).unwrap();
    let pages_offset = (SYMBOLS_V3_HEADER_LEN + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN) as u64;
    let file_len = pages_offset + u64::from(page_len);
    let mut root = vec![0u8; pages_offset as usize];
    put_u32(&mut root, 0, SYMBOLS_V3_MAGIC);
    put_u16(&mut root, 4, SYMBOLS_V3_VERSION);
    put_u32(&mut root, 8, SYMBOLS_V3_HEADER_LEN as u32);
    put_u32(&mut root, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
    put_u32(&mut root, 16, 1);
    put_u32(&mut root, 20, 1);
    put_u64(&mut root, 24, SYMBOLS_V3_HEADER_LEN as u64);
    put_u64(&mut root, 32, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u64);
    put_u64(&mut root, 40, pages_offset);
    put_u64(&mut root, 48, 0);
    put_u64(&mut root, 56, pages_offset);
    put_u64(&mut root, 64, file_len);
    let descriptor = descriptor_offset(0);
    put_u32(&mut root, descriptor, 0);
    put_u32(&mut root, descriptor + 4, 1);
    put_u64(&mut root, descriptor + 8, pages_offset);
    put_u32(&mut root, descriptor + 16, page_len);
    put_u32(&mut root, descriptor + 20, 0);
    put_u32(&mut root, descriptor + 24, 0);
    put_u32(&mut root, descriptor + 28, 0);
    put_u32(&mut root, descriptor + 32, 0);
    put_u32(&mut root, descriptor + 36, 0);
    put_u32(&mut root, descriptor + 40, 0);
    put_u32(&mut root, descriptor + 44, 0);
    let root_crc = symbols_root_crc(&root);
    put_u32(&mut root, ROOT_CRC_OFFSET, root_crc);

    let error = SegmentSymbolReader::open(SparsePrefixSource {
        prefix: root,
        file_len,
    })
    .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "symbols page exceeds the operational size limit"
    );
}

#[test]
fn touched_short_page_read_is_sticky_corruption_and_counted() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let bytes = encoded(&values);
    let pages_offset = read_u64_at(&bytes, 56) as usize;
    let reader = SegmentSymbolReader::open(SparsePrefixSource {
        prefix: bytes[..pages_offset].to_vec(),
        file_len: bytes.len() as u64,
    })
    .unwrap();

    let error = reader.resolve(0).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols positional read reached EOF");
    assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
    let sticky = reader.resolve(1).unwrap_err();
    assert_eq!(sticky.to_string(), error.to_string());
    assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
}

#[test]
fn valid_crc_root_field_corruptions_are_rejected_by_field_validation() {
    struct Case {
        name: &'static str,
        offset: usize,
        value: TestFieldValue,
        expected: &'static str,
    }

    let pristine = encoded(&["a".to_string()]);
    let root_len = read_u64_at(&pristine, 56) as usize;
    let descriptor = descriptor_offset(0);
    let cases = [
        Case {
            name: "magic",
            offset: 0,
            value: TestFieldValue::U32(0),
            expected: "symbols magic mismatch",
        },
        Case {
            name: "flags",
            offset: 6,
            value: TestFieldValue::U16(1),
            expected: "symbols flags are non-zero",
        },
        Case {
            name: "header length",
            offset: 8,
            value: TestFieldValue::U32(79),
            expected: "symbols header length is invalid",
        },
        Case {
            name: "descriptor length",
            offset: 12,
            value: TestFieldValue::U32(47),
            expected: "symbols page descriptor length is invalid",
        },
        Case {
            name: "header symbol count",
            offset: 16,
            value: TestFieldValue::U32(2),
            expected: "symbols descriptor counts do not match the header",
        },
        Case {
            name: "directory offset",
            offset: 24,
            value: TestFieldValue::U64(81),
            expected: "symbols directory offset is invalid",
        },
        Case {
            name: "directory length",
            offset: 32,
            value: TestFieldValue::U64(49),
            expected: "symbols directory length is invalid",
        },
        Case {
            name: "fence offset",
            offset: 40,
            value: TestFieldValue::U64(129),
            expected: "symbols fence offset is invalid",
        },
        Case {
            name: "fence length",
            offset: 48,
            value: TestFieldValue::U64(3),
            expected: "symbols pages offset is invalid",
        },
        Case {
            name: "pages offset",
            offset: 56,
            value: TestFieldValue::U64(131),
            expected: "symbols pages offset is invalid",
        },
        Case {
            name: "file length",
            offset: 64,
            value: TestFieldValue::U64(170),
            expected: "symbols file length is invalid",
        },
        Case {
            name: "root reserved",
            offset: 76,
            value: TestFieldValue::U32(1),
            expected: "symbols reserved field is non-zero",
        },
        Case {
            name: "descriptor first id",
            offset: descriptor,
            value: TestFieldValue::U32(1),
            expected: "symbols page symbol ids are not contiguous",
        },
        Case {
            name: "descriptor count",
            offset: descriptor + 4,
            value: TestFieldValue::U32(0),
            expected: "symbols page descriptor has no symbols",
        },
        Case {
            name: "descriptor page offset",
            offset: descriptor + 8,
            value: TestFieldValue::U64(131),
            expected: "symbols page byte ranges are not contiguous",
        },
        Case {
            name: "descriptor page length",
            offset: descriptor + 16,
            value: TestFieldValue::U32(42),
            expected: "symbols page length is inconsistent",
        },
        Case {
            name: "descriptor reserved",
            offset: descriptor + 44,
            value: TestFieldValue::U32(1),
            expected: "symbols page descriptor reserved field is non-zero",
        },
    ];

    for case in cases {
        let mut bytes = pristine.clone();
        case.value.write(&mut bytes, case.offset);
        repair_root_crc_with_len(&mut bytes, root_len);
        assert_eq!(
            symbols_root_crc(&bytes[..root_len]),
            read_u32_at(&bytes, ROOT_CRC_OFFSET),
            "{} mutation did not retain a valid root CRC",
            case.name
        );

        let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData, "{}", case.name);
        assert_eq!(error.to_string(), case.expected, "{}", case.name);
    }
}

#[test]
fn writer_rejects_unsorted_or_duplicate_values() {
    let mut bytes = Vec::new();
    let error = write_symbols_bin_v3(&mut bytes, ["zeta", "alpha"]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let error = write_symbols_bin_v3(&mut bytes, ["alpha", "alpha"]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[test]
fn writer_rejects_page_and_root_operational_size_limits() {
    let mut page_output = Vec::new();
    let page_error = write_symbols_bin_v3_with_operational_limits(
        &mut page_output,
        ["a"],
        SymbolWriterOperationalLimits {
            max_page_bytes: SYMBOLS_V3_PAGE_HEADER_LEN + 2 * std::mem::size_of::<u32>(),
            max_root_bytes: SYMBOLS_V3_MAX_ROOT_BYTES,
        },
    )
    .unwrap_err();
    assert_eq!(page_error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        page_error.to_string(),
        "symbols page exceeds the operational size limit"
    );
    assert!(page_output.is_empty());

    let mut root_output = Vec::new();
    let root_error = write_symbols_bin_v3_with_operational_limits(
        &mut root_output,
        ["a"],
        SymbolWriterOperationalLimits {
            max_page_bytes: SYMBOLS_V3_MAX_PAGE_BYTES,
            max_root_bytes: SYMBOLS_V3_HEADER_LEN + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN + 1,
        },
    )
    .unwrap_err();
    assert_eq!(root_error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        root_error.to_string(),
        "symbols root exceeds the operational size limit"
    );
    assert!(root_output.is_empty());
}

#[test]
fn greedy_pages_are_deterministic_and_bounded() {
    let values = (0..5_000)
        .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
        .collect::<Vec<_>>();
    let bytes = encoded(&values);
    assert!(page_count(&bytes) > 1);
    for page_index in 0..page_count(&bytes) as usize {
        let descriptor = descriptor_offset(page_index);
        let count = read_u32_at(&bytes, descriptor + 4);
        let length = read_u32_at(&bytes, descriptor + 16) as usize;
        assert!(count == 1 || length <= SYMBOLS_V3_PAGE_TARGET_BYTES);
    }
    assert_eq!(read_symbols_bin_v3(Cursor::new(bytes)).unwrap(), values);
}

#[test]
fn greedy_page_split_is_exact_at_the_target_and_one_byte_over() {
    let second_len_at_boundary = SYMBOLS_V3_PAGE_TARGET_BYTES
        - SYMBOLS_V3_PAGE_HEADER_LEN
        - 3 * std::mem::size_of::<u32>()
        - 1;
    let exact_values = vec!["a".to_string(), "b".repeat(second_len_at_boundary)];
    let exact_first = encoded(&exact_values);
    let exact_second = encoded(&exact_values);
    assert_eq!(exact_first, exact_second);
    assert_eq!(page_count(&exact_first), 1);
    assert_eq!(read_u32_at(&exact_first, descriptor_offset(0) + 4), 2);
    assert_eq!(
        read_u32_at(&exact_first, descriptor_offset(0) + 16) as usize,
        SYMBOLS_V3_PAGE_TARGET_BYTES
    );
    assert_eq!(
        read_symbols_bin_v3(Cursor::new(exact_first)).unwrap(),
        exact_values
    );

    let over_values = vec!["a".to_string(), "b".repeat(second_len_at_boundary + 1)];
    let over_first = encoded(&over_values);
    let over_second = encoded(&over_values);
    assert_eq!(over_first, over_second);
    assert_eq!(page_count(&over_first), 2);
    assert_eq!(read_u32_at(&over_first, descriptor_offset(0) + 4), 1);
    assert_eq!(read_u32_at(&over_first, descriptor_offset(1) + 4), 1);
    assert_eq!(read_u32_at(&over_first, descriptor_offset(0) + 16), 41);
    assert_eq!(
        read_u32_at(&over_first, descriptor_offset(1) + 16) as usize,
        SYMBOLS_V3_PAGE_TARGET_BYTES - 4
    );
    assert_eq!(
        read_symbols_bin_v3(Cursor::new(over_first)).unwrap(),
        over_values
    );
}

#[test]
fn root_rejects_a_nonmaximal_page_with_a_valid_crc() {
    let values = multi_page_values();
    let mut bytes = encoded(&values);
    let page_count = page_count(&bytes) as usize;
    assert!(page_count > 1);
    let current = descriptor_offset(page_count - 2);
    let next = descriptor_offset(page_count - 1);
    let shifted_bytes = 1_024u32;
    let current_len = read_u32_at(&bytes, current + 16);
    let current_strings_len = read_u32_at(&bytes, current + 40);
    let next_offset = read_u64_at(&bytes, next + 8);
    let next_len = read_u32_at(&bytes, next + 16);
    let next_strings_len = read_u32_at(&bytes, next + 40);
    assert!(current_len > shifted_bytes);
    assert!(current_strings_len > shifted_bytes);
    assert!(next_len.saturating_add(shifted_bytes) <= SYMBOLS_V3_PAGE_TARGET_BYTES as u32);

    put_u32(&mut bytes, current + 16, current_len - shifted_bytes);
    put_u32(
        &mut bytes,
        current + 40,
        current_strings_len - shifted_bytes,
    );
    put_u64(&mut bytes, next + 8, next_offset - u64::from(shifted_bytes));
    put_u32(&mut bytes, next + 16, next_len + shifted_bytes);
    put_u32(&mut bytes, next + 40, next_strings_len + shifted_bytes);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
    let pages_offset = read_u64_at(&bytes, 56) as usize;
    let root_crc = symbols_root_crc(&bytes[..pages_offset]);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

    let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols page is not greedily maximal");
}

#[test]
fn oversized_symbol_uses_a_singleton_page() {
    let values = vec!["x".repeat(SYMBOLS_V3_PAGE_TARGET_BYTES + 100)];
    let bytes = encoded(&values);
    assert_eq!(page_count(&bytes), 1);
    let descriptor = descriptor_offset(0);
    assert_eq!(read_u32_at(&bytes, descriptor + 4), 1);
    assert!(read_u32_at(&bytes, descriptor + 16) as usize > SYMBOLS_V3_PAGE_TARGET_BYTES);
    assert_eq!(read_symbols_bin_v3(Cursor::new(bytes)).unwrap(), values);
}

#[test]
fn v2_is_rejected_at_the_version_boundary() {
    let mut bytes = vec![0u8; SYMBOLS_V3_HEADER_LEN];
    put_u32(&mut bytes, 0, SYMBOLS_V3_MAGIC);
    put_u16(&mut bytes, 4, 2);
    let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "unsupported symbols version");
}

#[test]
fn explicit_layout_ab_v2_reader_is_eager_fallible_and_api_equivalent() {
    let values = vec![
        "".to_string(),
        "alpha".to_string(),
        "lambda".to_string(),
        "omega".to_string(),
        "ω".to_string(),
    ];
    let bytes = encoded_v2_for_layout_ab(&values);
    let encoded_len = bytes.len() as u64;
    let reader = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(bytes)).unwrap();

    assert_eq!(reader.len(), values.len());
    assert_eq!(reader.lookup("lambda").unwrap(), Some(2));
    assert_eq!(reader.lookup("missing").unwrap(), None);
    assert_eq!(
        reader
            .lookup_many(&["omega", "", "omega", "missing"])
            .unwrap(),
        vec![Some(3), Some(0), Some(3), None]
    );
    assert_eq!(reader.resolve(4).unwrap().unwrap().as_str(), "ω");
    assert!(reader.resolve(5).unwrap().is_none());
    let resolved = reader.resolve_many(&[3, 0, 3, 9]).unwrap();
    assert_eq!(
        resolved
            .iter()
            .map(|value| value.as_ref().map(|value| value.as_str()))
            .collect::<Vec<_>>(),
        vec![Some("omega"), Some(""), Some("omega"), None]
    );

    let stats = reader.read_stats();
    assert_eq!(
        stats.legacy_eager,
        SegmentSymbolReadCount {
            calls: 1,
            bytes: encoded_len,
        }
    );
    assert_eq!(stats.root, SegmentSymbolReadCount::default());
    assert_eq!(stats.page, SegmentSymbolReadCount::default());
    let resources = reader.resource_snapshot().unwrap();
    assert_eq!(resources.source_file_bytes, encoded_len);
    assert_eq!(resources.retained_open_files, 0);
    assert_eq!(resources.root_encoded_bytes, 0);
    assert!(resources.eager_dictionary_retained_charge_bytes > 0);
    assert_eq!(resources.page_cache_charge_bytes, 0);
    assert_eq!(resources.page_cache_max_bytes, 0);

    let clone = reader.try_clone_reader().unwrap();
    assert_eq!(clone.read_stats(), SegmentSymbolReadStats::default());
    assert_eq!(clone.lookup("alpha").unwrap(), Some(1));
    assert_eq!(
        clone.read_stats().logical_returned,
        SegmentSymbolReadCount { calls: 1, bytes: 5 }
    );
}

#[test]
fn visitor_reports_equal_repeated_logical_work_for_v2_and_v3() {
    let values = vec![
        "".to_string(),
        "alpha".to_string(),
        "lambda".to_string(),
        "omega".to_string(),
    ];
    let ids = [3, 0, 3, 1, 0];

    let v3 = SegmentSymbolReader::open(Cursor::new(encoded(&values))).unwrap();
    let mut v3_values = vec![None; ids.len()];
    assert!(
        v3.visit_resolved_many(&ids, |slot, value| {
            v3_values[slot] = Some(value.to_string());
            Ok(())
        })
        .unwrap()
    );

    let v2 = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(
        encoded_v2_for_layout_ab(&values),
    ))
    .unwrap();
    let mut v2_values = vec![None; ids.len()];
    assert!(
        v2.visit_resolved_many(&ids, |slot, value| {
            v2_values[slot] = Some(value.to_string());
            Ok(())
        })
        .unwrap()
    );

    assert_eq!(v3_values, v2_values);
    assert_eq!(
        v3.read_stats().logical_returned,
        v2.read_stats().logical_returned
    );
    assert_eq!(
        v3.read_stats().logical_returned,
        SegmentSymbolReadCount {
            calls: 5,
            bytes: 15,
        }
    );
}

#[test]
fn explicit_layout_ab_v2_reader_propagates_whole_dictionary_corruption() {
    let values = vec!["alpha".to_string(), "omega".to_string()];

    let mut nonzero_flags = encoded_v2_for_layout_ab(&values);
    put_u16(&mut nonzero_flags, 6, 1);
    let error =
        SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(nonzero_flags)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);

    let mut bad_final_offset = encoded_v2_for_layout_ab(&values);
    put_u64(
        &mut bad_final_offset,
        SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB + 16,
        1,
    );
    let error = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(bad_final_offset))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);

    let duplicate_values = vec!["same".to_string(), "same".to_string()];
    let error = SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(
        encoded_v2_for_layout_ab(&duplicate_values),
    ))
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);

    let mut invalid_utf8 = encoded_v2_for_layout_ab(&values);
    *invalid_utf8.last_mut().unwrap() = 0xff;
    let error =
        SegmentSymbolReader::open_legacy_v2_for_layout_ab(Cursor::new(invalid_utf8)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn root_crc_covers_descriptors_and_fences() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    bytes[descriptor_offset(0)] ^= 1;
    let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
    assert_eq!(error.to_string(), "symbols root CRC mismatch");
}

#[test]
fn root_rejects_invalid_utf8_fence_with_a_repaired_crc() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    let descriptor = descriptor_offset(0);
    let fences_offset = read_u64_at(&bytes, 40) as usize;
    let first_fence_offset = read_u32_at(&bytes, descriptor + 24) as usize;
    bytes[fences_offset + first_fence_offset] = 0xff;
    repair_root_crc(&mut bytes);

    let root_len = read_u64_at(&bytes, 56) as usize;
    assert_eq!(
        symbols_root_crc(&bytes[..root_len]),
        read_u32_at(&bytes, ROOT_CRC_OFFSET)
    );
    let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols fence is not valid UTF-8");
}

#[test]
fn root_rejects_noncanonical_fence_aliasing_with_a_valid_crc() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    let descriptor = descriptor_offset(0);
    put_u32(&mut bytes, descriptor + 32, 0);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
    let pages_offset = read_u64_at(&bytes, 56) as usize;
    let root_crc = symbols_root_crc(&bytes[..pages_offset]);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

    let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "symbols last fence is not canonically positioned"
    );
}

#[test]
fn root_rejects_equal_fences_for_a_multi_symbol_page_with_a_valid_crc() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    let descriptor = descriptor_offset(0);
    assert_eq!(read_u32_at(&bytes, descriptor + 4), 2);
    let fence_offset = read_u64_at(&bytes, 40) as usize;
    let first_offset = read_u32_at(&bytes, descriptor + 24) as usize;
    let first_len = read_u32_at(&bytes, descriptor + 28) as usize;
    let last_offset = read_u32_at(&bytes, descriptor + 32) as usize;
    let last_len = read_u32_at(&bytes, descriptor + 36) as usize;
    assert_eq!(first_len, last_len);
    let first =
        bytes[fence_offset + first_offset..fence_offset + first_offset + first_len].to_vec();
    bytes[fence_offset + last_offset..fence_offset + last_offset + last_len]
        .copy_from_slice(&first);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
    let pages_offset = read_u64_at(&bytes, 56) as usize;
    let root_crc = symbols_root_crc(&bytes[..pages_offset]);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

    let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "multi-symbol page fences are not strictly ordered"
    );
}

#[test]
fn root_rejects_two_symbol_length_not_proven_by_fences() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    let descriptor = descriptor_offset(0);
    assert_eq!(read_u32_at(&bytes, descriptor + 4), 2);
    let page_len = read_u32_at(&bytes, descriptor + 16);
    let string_bytes_len = read_u32_at(&bytes, descriptor + 40);
    let file_len = read_u64_at(&bytes, 64);
    put_u32(&mut bytes, descriptor + 16, page_len + 1);
    put_u32(&mut bytes, descriptor + 40, string_bytes_len + 1);
    put_u64(&mut bytes, 64, file_len + 1);
    bytes.push(0);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, 0);
    let pages_offset = read_u64_at(&bytes, 56) as usize;
    let root_crc = symbols_root_crc(&bytes[..pages_offset]);
    put_u32(&mut bytes, ROOT_CRC_OFFSET, root_crc);

    let error = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "two-symbol page string length disagrees with its fences"
    );
}

#[test]
fn valid_crc_page_field_corruptions_are_rejected_when_touched() {
    struct Case {
        name: &'static str,
        relative_offset: usize,
        value: TestFieldValue,
        expected: &'static str,
    }

    let pristine = encoded(&["a".to_string(), "bb".to_string(), "ccc".to_string()]);
    let page = page_offset(&pristine, 0);
    let cases = [
        Case {
            name: "page magic",
            relative_offset: 0,
            value: TestFieldValue::U32(0),
            expected: "symbols page magic mismatch",
        },
        Case {
            name: "page version",
            relative_offset: 4,
            value: TestFieldValue::U16(2),
            expected: "symbols page version mismatch",
        },
        Case {
            name: "page flags",
            relative_offset: 6,
            value: TestFieldValue::U16(1),
            expected: "symbols page flags are non-zero",
        },
        Case {
            name: "page first id",
            relative_offset: 12,
            value: TestFieldValue::U32(1),
            expected: "symbols page first id mismatch",
        },
        Case {
            name: "page count",
            relative_offset: 16,
            value: TestFieldValue::U32(2),
            expected: "symbols page count mismatch",
        },
        Case {
            name: "page offsets length",
            relative_offset: 20,
            value: TestFieldValue::U32(12),
            expected: "symbols page offsets length mismatch",
        },
        Case {
            name: "page strings length",
            relative_offset: 24,
            value: TestFieldValue::U32(5),
            expected: "symbols page strings length mismatch",
        },
        Case {
            name: "first local offset",
            relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN,
            value: TestFieldValue::U32(1),
            expected: "symbols page first offset must be zero",
        },
        Case {
            name: "final local offset",
            relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN + 3 * 4,
            value: TestFieldValue::U32(5),
            expected: "symbols page final offset does not match strings",
        },
        Case {
            name: "out-of-order local offset",
            relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN + 4,
            value: TestFieldValue::U32(4),
            expected: "symbols page offsets are out of order",
        },
        Case {
            name: "out-of-bounds local offset",
            relative_offset: SYMBOLS_V3_PAGE_HEADER_LEN + 4,
            value: TestFieldValue::U32(7),
            expected: "symbols page offset is out of bounds",
        },
    ];

    for case in cases {
        let mut bytes = pristine.clone();
        case.value.write(&mut bytes, page + case.relative_offset);
        repair_page_and_root_crcs(&mut bytes, 0);
        let descriptor = descriptor_offset(0);
        let page_len = read_u32_at(&bytes, descriptor + 16) as usize;
        assert_eq!(
            crc32c(&bytes[page..page + page_len]),
            read_u32_at(&bytes, descriptor + 20),
            "{} mutation did not retain a valid page CRC",
            case.name
        );
        let root_len = read_u64_at(&bytes, 56) as usize;
        assert_eq!(
            symbols_root_crc(&bytes[..root_len]),
            read_u32_at(&bytes, ROOT_CRC_OFFSET),
            "{} mutation did not retain a valid root CRC",
            case.name
        );

        let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
        let error = reader.resolve(0).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData, "{}", case.name);
        assert_eq!(error.to_string(), case.expected, "{}", case.name);
    }
}

#[test]
fn page_crc_is_checked_only_when_the_page_is_touched_and_is_sticky() {
    let values = (0..5_000)
        .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
        .collect::<Vec<_>>();
    let mut bytes = encoded(&values);
    assert!(page_count(&bytes) > 1);
    let corrupt_page = 1usize;
    let corrupt_offset = page_offset(&bytes, corrupt_page) + SYMBOLS_V3_PAGE_HEADER_LEN;
    bytes[corrupt_offset] ^= 1;

    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    assert_eq!(reader.resolve(0).unwrap().unwrap().as_str(), values[0]);
    let corrupt_id = reader.state.root.descriptors[corrupt_page].first_symbol_id;
    let error = reader.resolve(corrupt_id).unwrap_err();
    assert_eq!(error.to_string(), "symbols page CRC mismatch");
    assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
    let sticky = reader.resolve(0).unwrap_err();
    assert_eq!(sticky.to_string(), "symbols page CRC mismatch");
    assert_eq!(reader.read_stats().touched_corrupt_pages, 1);
}

#[test]
fn touched_page_rejects_semantic_corruption_even_with_repaired_crcs() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    let page = page_offset(&bytes, 0);
    let strings_len = read_u32_at(&bytes, descriptor_offset(0) + 40);
    put_u32(
        &mut bytes,
        page + SYMBOLS_V3_PAGE_HEADER_LEN + 4,
        strings_len,
    );
    repair_page_and_root_crcs(&mut bytes, 0);

    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    let error = reader.resolve(0).unwrap_err();
    assert_eq!(
        error.to_string(),
        "symbols page values are not strictly sorted and unique"
    );
}

#[test]
fn touched_page_rejects_reserved_bytes_even_with_repaired_crcs() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    let page = page_offset(&bytes, 0);
    put_u32(&mut bytes, page + 28, 1);
    repair_page_and_root_crcs(&mut bytes, 0);

    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    let error = reader.resolve(0).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols page reserved field is non-zero");
}

#[test]
fn touched_page_rejects_invalid_utf8_with_repaired_crcs() {
    let values = vec!["alpha".to_string(), "omega".to_string()];
    let mut bytes = encoded(&values);
    let descriptor = descriptor_offset(0);
    let page = page_offset(&bytes, 0);
    let offsets_len = read_u32_at(&bytes, page + 20) as usize;
    bytes[page + SYMBOLS_V3_PAGE_HEADER_LEN + offsets_len] = 0xff;
    repair_page_and_root_crcs(&mut bytes, 0);

    let page_len = read_u32_at(&bytes, descriptor + 16) as usize;
    assert_eq!(
        crc32c(&bytes[page..page + page_len]),
        read_u32_at(&bytes, descriptor + 20)
    );
    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    let error = reader.resolve(0).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "symbols page value is not valid UTF-8");
}

#[test]
fn page_identity_rejects_literal_swapped_pages_with_repaired_crcs() {
    let values = multi_page_values();
    let mut bytes = encoded(&values);
    assert!(page_count(&bytes) > 1);
    let first_offset = page_offset(&bytes, 0);
    let second_offset = page_offset(&bytes, 1);
    let first_len = read_u32_at(&bytes, descriptor_offset(0) + 16) as usize;
    let second_len = read_u32_at(&bytes, descriptor_offset(1) + 16) as usize;
    assert_eq!(first_len, second_len);
    let first_page = bytes[first_offset..first_offset + first_len].to_vec();
    let second_page = bytes[second_offset..second_offset + second_len].to_vec();
    bytes[first_offset..first_offset + first_len].copy_from_slice(&second_page);
    bytes[second_offset..second_offset + second_len].copy_from_slice(&first_page);
    repair_page_and_root_crcs(&mut bytes, 0);
    repair_page_and_root_crcs(&mut bytes, 1);

    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    let error = reader.resolve(0).unwrap_err();
    assert_eq!(error.to_string(), "symbols page index mismatch");
}

#[test]
fn cache_is_bounded_shared_by_clones_and_stats_are_per_reader() {
    let values = (0..5_000)
        .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
        .collect::<Vec<_>>();
    let bytes = encoded(&values);
    let reader = SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        SYMBOLS_V3_PAGE_TARGET_BYTES * 2,
    )
    .unwrap();
    let clone = reader.try_clone_reader().unwrap();
    let root_stats = reader.read_stats();
    assert_eq!(root_stats.root.calls, 2);
    assert_eq!(clone.read_stats(), SegmentSymbolReadStats::default());

    assert_eq!(reader.resolve(0).unwrap().unwrap().as_str(), values[0]);
    assert!(reader.cache_charge_bytes().unwrap() <= reader.cache_max_bytes());
    assert_eq!(reader.read_stats().page_validation.calls, 1);
    assert_eq!(
        reader.read_stats().page_validation.bytes,
        reader.read_stats().page.bytes
    );
    assert_eq!(clone.resolve(0).unwrap().unwrap().as_str(), values[0]);
    assert_eq!(reader.read_stats().page_cache_misses, 1);
    assert_eq!(clone.read_stats().page_cache_hits, 1);
    assert_eq!(reader.read_stats().logical_returned.calls, 1);
    assert_eq!(
        reader.read_stats().logical_returned.bytes,
        values[0].len() as u64
    );
    assert_eq!(clone.read_stats().logical_returned.calls, 1);
    assert_eq!(
        clone.read_stats().logical_returned.bytes,
        values[0].len() as u64
    );
}

#[test]
fn resource_snapshot_charges_the_root_once_per_shared_state() {
    let bytes = encoded(&multi_page_values());
    let expected_file_bytes = bytes.len() as u64;
    let expected_root_bytes = read_u64_at(&bytes, 56);
    let expected_root_charge = std::mem::size_of::<SymbolRoot>()
        + page_count(&bytes) as usize * std::mem::size_of::<SymbolPageDescriptor>()
        + read_u64_at(&bytes, 48) as usize;
    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    let clone = reader.try_clone_reader().unwrap();

    assert_eq!(reader.state_identity(), clone.state_identity());
    let before = reader.resource_snapshot().unwrap();
    assert_eq!(before.retained_open_files, 0);
    assert_eq!(before.source_file_bytes, expected_file_bytes);
    assert_eq!(before.root_encoded_bytes, expected_root_bytes);
    assert_eq!(
        before.root_retained_charge_bytes,
        expected_root_charge as u64
    );
    assert_eq!(before.eager_dictionary_retained_charge_bytes, 0);
    assert_eq!(before.page_cache_charge_bytes, 0);
    assert_eq!(before.page_cache_max_bytes, 256 * 1024);
    assert_eq!(
        before.total_retained_charge_bytes(),
        before.root_retained_charge_bytes
    );

    clone.resolve(0).unwrap().unwrap();
    let after = reader.resource_snapshot().unwrap();
    assert!(after.page_cache_charge_bytes > 0);
    assert_eq!(after, clone.resource_snapshot().unwrap());
    assert_eq!(
        after.total_retained_charge_bytes(),
        after
            .root_retained_charge_bytes
            .saturating_add(after.page_cache_charge_bytes)
    );
}

#[test]
fn validate_all_detects_an_otherwise_untouched_bad_page() {
    let values = (0..5_000)
        .map(|index| format!("symbol-{index:08}-{}", "x".repeat(24)))
        .collect::<Vec<_>>();
    let mut bytes = encoded(&values);
    let last_page = page_count(&bytes) as usize - 1;
    let corrupt_offset = page_offset(&bytes, last_page) + SYMBOLS_V3_PAGE_HEADER_LEN;
    bytes[corrupt_offset] ^= 1;
    let reader = SegmentSymbolReader::open(Cursor::new(bytes)).unwrap();
    let error = reader.validate_all().unwrap_err();
    assert_eq!(error.to_string(), "symbols page CRC mismatch");
}
