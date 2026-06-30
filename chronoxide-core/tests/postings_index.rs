use std::io::Cursor;

use chronoxide_core::storage::index::{
    ExactPostingsIndex, LabelValueFstIndex, LabelValueIndex, SegmentIndexes,
    read_exact_postings_index, read_segment_indexes, write_exact_postings_index,
    write_segment_indexes,
};
use chronoxide_core::storage::series::{SegmentSymbols, SeriesEntry};

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
fn label_value_index_builds_from_series_entries() {
    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: 1,
            labels: vec![(1, 10), (2, 20)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
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
            labels: vec![(pod, backend_2), (namespace, default)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
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
    assert!(index.values(99).unwrap().is_empty());
}

#[test]
fn segment_indexes_roundtrip_exact_postings_and_value_fsts() {
    let mut symbols = SegmentSymbols::default();
    let pod = symbols.intern("pod_name");
    let backend_1 = symbols.intern("backend-1");
    let backend_2 = symbols.intern("backend-2");
    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: 1,
            labels: vec![(pod, backend_1)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
            labels: vec![(pod, backend_2)],
        },
    ];

    let mut postings = ExactPostingsIndex::default();
    postings.insert(pod, backend_1, 0);
    postings.insert(pod, backend_2, 1);
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let indexes = SegmentIndexes {
        exact_postings: postings,
        label_values,
    };

    let mut bytes = Vec::new();
    write_segment_indexes(&mut bytes, &indexes).unwrap();
    let restored = read_segment_indexes(&mut Cursor::new(bytes)).unwrap();

    assert_eq!(restored.exact_postings.get(pod, backend_1), Some(&[0][..]));
    assert_eq!(restored.exact_postings.get(pod, backend_2), Some(&[1][..]));
    assert_eq!(
        restored.label_values.values(pod).unwrap(),
        vec!["backend-1".to_string(), "backend-2".to_string()]
    );
}
