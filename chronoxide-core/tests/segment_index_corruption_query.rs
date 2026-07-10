use std::fs::{self, File};
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::{METRIC_NAME_LABEL, PromqlQueryError, normalize_label_name};
use chronoxide_core::storage::segment::{
    LabelMatcher, SegmentFile, SegmentSelector, SegmentStoreReader, SegmentWriter,
    SegmentWriterConfig,
};
use chronoxide_core::storage::series::read_symbols_bin;

const V7_TRAILER_LEN: usize = 256;
const TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET: usize = 56;
const TRAILER_EXACT_PAGES_LOCATOR_OFFSET: usize = 72;
const TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET: usize = 104;
const TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET: usize = 120;
const EXACT_DIRECTORY_HEADER_LEN: usize = 64;
const EXACT_PAGE_DESCRIPTOR_LEN: usize = 32;
const AUXILIARY_DIRECTORY_HEADER_LEN: usize = 64;
const AUXILIARY_DIRECTORY_RECORD_LEN: usize = 40;
const AUXILIARY_DIRECTORY_CRC_OFFSET: usize = 40;
const LABEL_VALUE_FST_KIND: u16 = 2;

#[derive(Debug, Clone, Copy)]
struct Locator {
    offset: usize,
    len: usize,
}

struct SegmentFixture {
    _tempdir: tempfile::TempDir,
    segments_dir: PathBuf,
    segment_dir: PathBuf,
}

impl SegmentFixture {
    fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();

        writer
            .record_samples_with_labels(
                SeriesRef::new(1),
                &[
                    (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                    ("pod.name".to_string(), "backend-1".to_string()),
                    ("zone".to_string(), "east".to_string()),
                ],
                &[(5_000, 1.0)],
            )
            .unwrap();
        writer
            .record_samples_with_labels(
                SeriesRef::new(2),
                &[
                    (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                    ("pod.name".to_string(), "backend-2".to_string()),
                    ("zone".to_string(), "west".to_string()),
                ],
                &[(6_000, 2.0)],
            )
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let segments_dir = tempdir.path().to_path_buf();
        let segment_dir = fs::read_dir(&segments_dir)
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        Self {
            _tempdir: tempdir,
            segments_dir,
            segment_dir,
        }
    }

    fn store(&self) -> SegmentStoreReader {
        // Default open intentionally skips footer validation so corruption is
        // discovered only when the corresponding lazy V7 region is touched.
        SegmentStoreReader::open(&self.segments_dir).unwrap()
    }

    fn indexes_path(&self) -> PathBuf {
        self.segment_dir.join(SegmentFile::Indexes.filename())
    }

    fn symbol(&self, value: &str) -> u32 {
        let symbols = read_symbols_bin(
            File::open(self.segment_dir.join(SegmentFile::Symbols.filename())).unwrap(),
        )
        .unwrap();
        symbols.lookup(value).unwrap()
    }

    fn mutate_indexes(&self, mutate: impl FnOnce(&mut [u8])) {
        let path = self.indexes_path();
        let mut bytes = fs::read(&path).unwrap();
        mutate(&mut bytes);
        fs::write(path, bytes).unwrap();
    }
}

#[test]
fn exact_page_corruption_propagates_through_queries_prewarm_and_prefetch() {
    let fixture = SegmentFixture::new();
    let name_sym = fixture.symbol(&normalize_label_name("pod.name"));
    let value_sym = fixture.symbol("backend-1");
    fixture.mutate_indexes(|bytes| corrupt_exact_page_for_key(bytes, (name_sym, value_sym)));

    let store = fixture.store();
    assert!(!store.label_names(0, 10_000).unwrap().is_empty());

    let positive = SegmentSelector::new(vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let error = store.query_selector(&positive, 0, 10_000).unwrap_err();
    assert_invalid_data(error, "exact page CRC mismatch");

    let negative = SegmentSelector::new(vec![LabelMatcher::not_eq("pod.name", "backend-1")]);
    let error = store.query_selector(&negative, 0, 10_000).unwrap_err();
    assert_invalid_data(error, "exact page CRC mismatch");

    let mut session = store.query_session().unwrap();
    let error = session
        .prewarm_promql(r#"cpu.usage{pod.name="backend-1"}"#, 0, 10_000)
        .unwrap_err();
    assert_promql_storage_error(error, "exact page CRC mismatch");

    let mut session = store.query_session().unwrap();
    let error = session
        .prefetch_promql_data(r#"cpu.usage{pod.name="backend-1"}"#, 0, 10_000)
        .unwrap_err();
    assert_promql_storage_error(error, "exact page CRC mismatch");
}

#[test]
fn auxiliary_directory_crc_corruption_is_not_treated_as_missing_metadata_or_regex_values() {
    let fixture = SegmentFixture::new();
    fixture.mutate_indexes(corrupt_auxiliary_directory_crc);

    let store = fixture.store();
    let exact = SegmentSelector::new(vec![LabelMatcher::eq("pod.name", "backend-1")]);
    assert_eq!(store.query_selector(&exact, 0, 10_000).unwrap().len(), 1);

    let error = store.label_names(0, 10_000).unwrap_err();
    assert_invalid_data(error, "auxiliary directory CRC mismatch");

    let regex = SegmentSelector::new(vec![LabelMatcher::regex("pod.name", "backend-.*")]);
    let error = store.query_selector(&regex, 0, 10_000).unwrap_err();
    assert_invalid_data(error, "auxiliary directory CRC mismatch");
}

#[test]
fn touched_label_value_fst_corruption_propagates_from_discovery_and_regex_query() {
    let fixture = SegmentFixture::new();
    let name_sym = fixture.symbol(&normalize_label_name("pod.name"));
    fixture.mutate_indexes(|bytes| corrupt_label_value_fst(bytes, name_sym));

    let store = fixture.store();
    assert!(
        store
            .label_names(0, 10_000)
            .unwrap()
            .contains(&normalize_label_name("pod.name"))
    );

    let error = store.label_values("pod.name", 0, 10_000).unwrap_err();
    assert_invalid_data(error, "FST");

    let regex = SegmentSelector::new(vec![LabelMatcher::regex("pod.name", "backend-.*")]);
    let error = store.query_selector(&regex, 0, 10_000).unwrap_err();
    assert_invalid_data(error, "FST");
}

fn corrupt_exact_page_for_key(bytes: &mut [u8], key: (u32, u32)) {
    let directory = locator(bytes, TRAILER_EXACT_DIRECTORY_LOCATOR_OFFSET);
    assert_eq!(
        read_u32_at(bytes, directory.offset),
        u32::from_le_bytes(*b"EXD7")
    );
    let page_count = read_u32_at(bytes, directory.offset + 32) as usize;
    let descriptors_offset = read_u64_at(bytes, directory.offset + 40) as usize;
    assert_eq!(descriptors_offset, EXACT_DIRECTORY_HEADER_LEN);

    let page_index = (0..page_count)
        .find(|page_index| {
            let descriptor =
                directory.offset + descriptors_offset + page_index * EXACT_PAGE_DESCRIPTOR_LEN;
            let first = (
                read_u32_at(bytes, descriptor),
                read_u32_at(bytes, descriptor + 4),
            );
            let last = (
                read_u32_at(bytes, descriptor + 8),
                read_u32_at(bytes, descriptor + 12),
            );
            first <= key && key <= last
        })
        .expect("target exact key must have a page descriptor");

    let pages = locator(bytes, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
    let page_len = pages.len / page_count;
    assert_eq!(page_len, 16_384);
    let page_start = pages.offset + page_index * page_len;
    bytes[page_start] ^= 0x80;
}

fn corrupt_auxiliary_directory_crc(bytes: &mut [u8]) {
    let directory = locator(bytes, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    assert!(directory.len >= AUXILIARY_DIRECTORY_HEADER_LEN);
    bytes[directory.offset + AUXILIARY_DIRECTORY_CRC_OFFSET] ^= 0x80;
}

fn corrupt_label_value_fst(bytes: &mut [u8], label_name_sym: u32) {
    let directory = locator(bytes, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    assert_eq!(
        read_u32_at(bytes, directory.offset),
        u32::from_le_bytes(*b"AUX7")
    );
    let entry_count = read_u64_at(bytes, directory.offset + 16) as usize;
    let records_offset = read_u64_at(bytes, directory.offset + 24) as usize;
    assert_eq!(records_offset, AUXILIARY_DIRECTORY_HEADER_LEN);

    let record = (0..entry_count)
        .map(|index| directory.offset + records_offset + index * AUXILIARY_DIRECTORY_RECORD_LEN)
        .find(|record| {
            read_u16_at(bytes, *record) == LABEL_VALUE_FST_KIND
                && read_u32_at(bytes, *record + 4) == label_name_sym
        })
        .expect("target label must have a label-value FST record");
    let payload = Locator {
        offset: read_u64_at(bytes, record + 8) as usize,
        len: read_u64_at(bytes, record + 16) as usize,
    };
    let payload_region = locator(bytes, TRAILER_AUX_PAYLOADS_LOCATOR_OFFSET);
    assert!(payload.len > 0);
    assert!(payload.offset >= payload_region.offset);
    assert!(payload.offset + payload.len <= payload_region.offset + payload_region.len);
    bytes[payload.offset] ^= 0x80;
}

fn locator(bytes: &[u8], trailer_relative_offset: usize) -> Locator {
    assert!(bytes.len() >= V7_TRAILER_LEN);
    let trailer = bytes.len() - V7_TRAILER_LEN;
    let offset = read_u64_at(bytes, trailer + trailer_relative_offset) as usize;
    let len = read_u64_at(bytes, trailer + trailer_relative_offset + 8) as usize;
    assert!(
        offset
            .checked_add(len)
            .is_some_and(|end| end <= bytes.len())
    );
    Locator { offset, len }
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn assert_invalid_data(error: io::Error, expected_message: &str) {
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert!(
        error.to_string().contains(expected_message),
        "expected {expected_message:?} in {error:?}"
    );
}

fn assert_promql_storage_error(error: PromqlQueryError, expected_message: &str) {
    let PromqlQueryError::Storage(message) = error else {
        panic!("expected storage error, got {error:?}");
    };
    assert!(
        message.contains(expected_message),
        "expected {expected_message:?} in {message:?}"
    );
}
