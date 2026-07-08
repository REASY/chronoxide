use std::io::Cursor;

use chronoxide_core::storage::index::{
    ExactPostingsIndex, LabelValueFstIndex, LabelValueIndex, LabelValueTimeRange,
    LabelValueTimeRangeIndex, MetricSeriesRangeIndex, SegmentIndexReader, SegmentIndexes,
    SegmentRoutingIndex, read_exact_postings_index, read_segment_indexes,
    write_exact_postings_index, write_segment_indexes,
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
            chunk_index: Default::default(),
            labels: vec![(pod, backend_1)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(pod, backend_2)],
        },
    ];

    let mut postings = ExactPostingsIndex::default();
    postings.insert(pod, backend_1, 0);
    postings.insert(pod, backend_2, 1);
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    label_value_time_ranges.insert(pod, backend_1, 1_000, 2_000);
    label_value_time_ranges.insert(pod, backend_2, 11_000, 12_000);
    let indexes = SegmentIndexes {
        exact_postings: postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
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
    assert_eq!(
        restored.label_value_time_ranges.get(pod, backend_1),
        Some(LabelValueTimeRange {
            min_time_ms: 1_000,
            max_time_ms: 2_000,
        })
    );
    assert_eq!(
        restored.label_value_time_ranges.get(pod, backend_2),
        Some(LabelValueTimeRange {
            min_time_ms: 11_000,
            max_time_ms: 12_000,
        })
    );
}

#[test]
fn segment_index_reader_fetches_directory_addressed_blobs() {
    let mut symbols = SegmentSymbols::default();
    let pod = symbols.intern("pod_name");
    let namespace = symbols.intern("namespace");
    let backend_1 = symbols.intern("backend-1");
    let backend_2 = symbols.intern("backend-2");
    let default = symbols.intern("default");
    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(pod, backend_1), (namespace, default)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(pod, backend_2), (namespace, default)],
        },
    ];

    let mut postings = ExactPostingsIndex::default();
    postings.insert(pod, backend_1, 0);
    postings.insert(pod, backend_2, 1);
    postings.insert(namespace, default, 0);
    postings.insert(namespace, default, 1);
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    label_value_time_ranges.insert(pod, backend_1, 1_000, 2_000);
    label_value_time_ranges.insert(pod, backend_2, 11_000, 12_000);
    label_value_time_ranges.insert(namespace, default, 1_000, 12_000);
    let indexes = SegmentIndexes {
        exact_postings: postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };

    let mut bytes = Vec::new();
    write_segment_indexes(&mut bytes, &indexes).unwrap();
    let mut reader = SegmentIndexReader::open(Cursor::new(bytes)).unwrap();

    assert_eq!(reader.label_name_symbols(), vec![pod, namespace]);
    assert_eq!(
        reader.label_values(pod).unwrap(),
        vec!["backend-1".to_string(), "backend-2".to_string()]
    );
    assert_eq!(
        reader.exact_postings(pod, backend_1).unwrap(),
        Some(vec![0])
    );
    assert_eq!(
        reader.exact_postings(namespace, default).unwrap(),
        Some(vec![0, 1])
    );
    assert_eq!(
        reader.label_value_time_range(pod, backend_2).unwrap(),
        Some(LabelValueTimeRange {
            min_time_ms: 11_000,
            max_time_ms: 12_000,
        })
    );
    assert_eq!(
        reader.label_time_range(pod),
        Some(LabelValueTimeRange {
            min_time_ms: 1_000,
            max_time_ms: 12_000,
        })
    );
}

#[test]
fn segment_index_reader_fetches_embedded_routing_index() {
    let mut symbols = SegmentSymbols::default();
    let pod = symbols.intern("pod_name");
    let backend_1 = symbols.intern("backend-1");
    let backend_2 = symbols.intern("backend-2");
    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(pod, backend_1)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(pod, backend_2)],
        },
    ];

    let mut postings = ExactPostingsIndex::default();
    postings.insert(pod, backend_1, 0);
    postings.insert(pod, backend_2, 1);
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    label_value_time_ranges.insert(pod, backend_1, 1_000, 2_000);
    label_value_time_ranges.insert(pod, backend_2, 11_000, 12_000);
    let routing_index =
        SegmentRoutingIndex::from_indexes(&symbols, &postings, &label_value_time_ranges).unwrap();
    let indexes = SegmentIndexes {
        exact_postings: postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: Some(routing_index),
    };

    let mut bytes = Vec::new();
    write_segment_indexes(&mut bytes, &indexes).unwrap();
    let mut reader = SegmentIndexReader::open(Cursor::new(bytes)).unwrap();
    let routing_index = reader.routing_index().unwrap().unwrap();

    let metadata = routing_index
        .exact_postings_metadata("pod_name", "backend-2")
        .unwrap();
    assert_eq!(metadata.byte_len, 8);
    assert_eq!(
        metadata.time_range,
        LabelValueTimeRange {
            min_time_ms: 11_000,
            max_time_ms: 12_000,
        }
    );
    assert!(
        routing_index
            .exact_postings_metadata("pod_name", "missing")
            .is_none()
    );
}

#[test]
fn segment_index_reader_uses_lazy_routing_point_lookup() {
    let mut symbols = SegmentSymbols::default();
    let name = symbols.intern("__name__");
    let mut series = Vec::new();
    let mut postings = ExactPostingsIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();

    for idx in 0..1_000u32 {
        let value = symbols.intern(&format!("metric_{idx:04}"));
        series.push(SeriesEntry {
            series_id: u64::from(idx),
            kind_mask: 1,
            chunk_index: Default::default(),
            labels: vec![(name, value)],
        });
        postings.insert(name, value, idx);
        label_value_time_ranges.insert(name, value, 1_000 + u64::from(idx), 2_000 + u64::from(idx));
    }

    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let routing_index =
        SegmentRoutingIndex::from_indexes(&symbols, &postings, &label_value_time_ranges).unwrap();
    let indexes = SegmentIndexes {
        exact_postings: postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: Some(routing_index),
    };

    let mut bytes = Vec::new();
    write_segment_indexes(&mut bytes, &indexes).unwrap();
    let mut reader = SegmentIndexReader::open(Cursor::new(bytes)).unwrap();
    let routing_blob_len = reader.routing_index_byte_len().unwrap();

    let lookup = reader
        .routing_exact_postings_metadata("__name__", "metric_0999")
        .unwrap();
    let metadata = lookup.metadata.unwrap();

    assert_eq!(metadata.byte_len, 8);
    assert_eq!(
        metadata.time_range,
        LabelValueTimeRange {
            min_time_ms: 1_999,
            max_time_ms: 2_999,
        }
    );
    assert!(
        lookup.bytes_read < routing_blob_len / 10,
        "point lookup read {} bytes from {} byte routing blob",
        lookup.bytes_read,
        routing_blob_len
    );

    let missing = reader
        .routing_exact_postings_metadata("__name__", "missing")
        .unwrap();
    assert!(missing.metadata.is_none());
    assert!(missing.bytes_read < routing_blob_len / 10);
}
