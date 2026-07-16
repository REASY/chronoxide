use std::io::Cursor;

use chronoxide_core::storage::index::{
    ExactPostingsIndex, LabelValueFstIndex, LabelValueIndex, LabelValueTimeRange,
    LabelValueTimeRangeIndex, SegmentIndexReader, read_exact_postings_index, read_segment_indexes,
    write_exact_postings_index,
};
use chronoxide_core::storage::series::{SegmentSymbols, SeriesEntry};

fn legacy_v6_segment_indexes_fixture() -> Vec<u8> {
    const HEADER_LEN: u64 = 8;
    const RANGE_COUNT: u32 = 7;
    const METRIC_PAYLOAD_LEN: u64 = 12 + 8 + RANGE_COUNT as u64 * 28;
    const METRIC_BLOB_KIND: u16 = 5;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"SIDX");
    bytes.extend_from_slice(&6u16.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());

    bytes.extend_from_slice(b"MSRG");
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&7u32.to_le_bytes());
    bytes.extend_from_slice(&RANGE_COUNT.to_le_bytes());
    for range_index in 0..RANGE_COUNT {
        bytes.extend_from_slice(&(range_index * 10).to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&(u64::from(range_index) * 100).to_le_bytes());
        bytes.extend_from_slice(&(u64::from(range_index) * 100 + 99).to_le_bytes());
    }
    assert_eq!(bytes.len() as u64, HEADER_LEN + METRIC_PAYLOAD_LEN);

    let mut footer = Vec::new();
    footer.extend_from_slice(b"SIDF");
    footer.extend_from_slice(&6u16.to_le_bytes());
    footer.extend_from_slice(&0u16.to_le_bytes());
    footer.extend_from_slice(&1u32.to_le_bytes());
    footer.extend_from_slice(&0u32.to_le_bytes());
    footer.extend_from_slice(&METRIC_BLOB_KIND.to_le_bytes());
    footer.extend_from_slice(&0u16.to_le_bytes());
    footer.extend_from_slice(&u32::MAX.to_le_bytes());
    footer.extend_from_slice(&u32::MAX.to_le_bytes());
    footer.extend_from_slice(&HEADER_LEN.to_le_bytes());
    footer.extend_from_slice(&METRIC_PAYLOAD_LEN.to_le_bytes());
    footer.extend_from_slice(&0u64.to_le_bytes());
    footer.extend_from_slice(&u64::MAX.to_le_bytes());

    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&(footer.len() as u64).to_le_bytes());
    bytes.extend_from_slice(b"SIDT");
    bytes
}

#[test]
fn public_segment_index_readers_reject_legacy_v6_fixture() {
    let bytes = legacy_v6_segment_indexes_fixture();

    let lazy_error = match SegmentIndexReader::open(Cursor::new(bytes.clone())) {
        Ok(_) => panic!("SegmentIndexReader unexpectedly accepted a V6 index"),
        Err(error) => error,
    };
    let eager_error = read_segment_indexes(Cursor::new(bytes)).unwrap_err();

    assert_eq!(lazy_error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(eager_error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn exact_postings_index_roundtrips_sorted_deduped_postings() {
    let mut index = ExactPostingsIndex::default();
    index.insert(1, 10, 2);
    index.insert(1, 10, 1);
    index.insert(1, 10, 2);
    index.insert(1, 11, 3);
    index.insert(2, 20, 1);

    assert_eq!(index.get(1, 10), Some(&[1, 2][..]));
    assert_eq!(index.get(1, 11), Some(&[3][..]));
    assert_eq!(index.get(9, 99), None);

    let mut bytes = Vec::new();
    write_exact_postings_index(&mut bytes, &index).unwrap();

    let restored = read_exact_postings_index(&mut Cursor::new(bytes)).unwrap();
    assert_eq!(restored.get(1, 10), Some(&[1, 2][..]));
    assert_eq!(restored.get(1, 11), Some(&[3][..]));
    assert_eq!(restored.get(2, 20), Some(&[1][..]));
    assert_eq!(restored.get(2, 99), None);
}

#[test]
fn label_value_index_tracks_sorted_deduped_values_by_label_name() {
    let mut index = LabelValueIndex::default();
    index.insert(1, 12);
    index.insert(1, 10);
    index.insert(1, 12);
    index.insert(2, 20);

    assert_eq!(index.values(1), &[10, 12]);
    assert_eq!(index.values(2), &[20]);
    assert!(index.values(9).is_empty());
}

#[test]
fn exact_postings_index_monotonic_insert_keeps_sorted_deduped_postings() {
    let mut index = ExactPostingsIndex::default();

    index.insert_monotonic(1, 2, 2);
    index.insert_monotonic(1, 2, 4);
    index.insert_monotonic(1, 2, 1);
    index.insert_monotonic(1, 2, 4);

    assert_eq!(index.get(1, 2), Some(&[1, 2, 4][..]));
}

#[test]
fn label_value_index_builds_from_series_entries() {
    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(1, 10), (2, 20)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(1, 11), (2, 20)],
        },
    ];

    let index = LabelValueIndex::from_series(&series);

    assert_eq!(index.values(1), &[10, 11]);
    assert_eq!(index.values(2), &[20]);
}

#[test]
fn label_value_fst_index_builds_from_series_entries() {
    let mut symbols = SegmentSymbols::default();
    let pod = symbols.intern("pod_name");
    let backend_2 = symbols.intern("backend-2");
    let backend_1 = symbols.intern("backend-1");
    let namespace = symbols.intern("namespace");
    let default = symbols.intern("default");

    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(pod, backend_2), (namespace, default)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(pod, backend_1), (namespace, default)],
        },
    ];

    let index = LabelValueFstIndex::from_series(&series, &symbols).unwrap();

    assert_eq!(
        index.values(pod).unwrap(),
        vec!["backend-1".to_string(), "backend-2".to_string()]
    );
    assert_eq!(
        index.values(namespace).unwrap(),
        vec!["default".to_string()]
    );
    assert_eq!(index.label_name_symbols(), vec![pod, namespace]);
    assert!(index.values(99).unwrap().is_empty());
}

#[test]
fn label_value_time_range_index_expands_ranges_by_label_value() {
    let mut index = LabelValueTimeRangeIndex::default();
    index.insert(1, 10, 5_000, 6_000);
    index.insert(1, 10, 1_000, 2_000);
    index.insert(1, 11, 8_000, 9_000);

    assert_eq!(
        index.get(1, 10),
        Some(LabelValueTimeRange {
            min_time_ms: 1_000,
            max_time_ms: 6_000,
        })
    );
    assert_eq!(
        index.get(1, 11),
        Some(LabelValueTimeRange {
            min_time_ms: 8_000,
            max_time_ms: 9_000,
        })
    );
    assert!(index.get(9, 99).is_none());
    assert!(index.get(1, 10).unwrap().overlaps(2_000, 3_000));
    assert!(!index.get(1, 10).unwrap().overlaps(6_001, 7_000));
}
