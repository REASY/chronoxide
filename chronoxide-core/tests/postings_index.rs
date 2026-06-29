use std::io::Cursor;

use chronoxide_core::storage::index::{
    ExactPostingsIndex, LabelValueIndex, read_exact_postings_index, write_exact_postings_index,
};
use chronoxide_core::storage::series::SeriesEntry;

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
