use super::*;
use crate::storage::series::{SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM};
use std::io::Cursor;

#[test]
fn label_value_time_range_index_bulk_insert_merges_ranges() {
    let mut index = LabelValueTimeRangeIndex::default();

    index.insert_many(&[(2, 20), (1, 10)], 1_000, 2_000);
    index.insert_many(&[(1, 10), (3, 30)], 500, 4_000);

    assert_eq!(index.len(), 3);
    assert_eq!(
        index.get(1, 10),
        Some(LabelValueTimeRange {
            min_time_ms: 500,
            max_time_ms: 4_000,
        })
    );
    assert_eq!(
        index.get(2, 20),
        Some(LabelValueTimeRange {
            min_time_ms: 1_000,
            max_time_ms: 2_000,
        })
    );
    assert_eq!(
        index.get(3, 30),
        Some(LabelValueTimeRange {
            min_time_ms: 500,
            max_time_ms: 4_000,
        })
    );
}

#[test]
fn segment_index_serializes_label_value_time_ranges_deterministically() {
    let mut forward = LabelValueTimeRangeIndex::default();
    forward.insert_many(&[(1, 10), (1, 20), (2, 30)], 1_000, 2_000);

    let mut reverse = LabelValueTimeRangeIndex::default();
    reverse.insert_many(&[(2, 30), (1, 20), (1, 10)], 1_000, 2_000);

    let mut forward_bytes = Vec::new();
    write_segment_indexes_unbound_for_test(
        &mut forward_bytes,
        &SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: forward,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        },
    )
    .unwrap();

    let mut reverse_bytes = Vec::new();
    write_segment_indexes_unbound_for_test(
        &mut reverse_bytes,
        &SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: reverse,
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        },
    )
    .unwrap();

    assert_eq!(forward_bytes, reverse_bytes);
}

#[test]
fn routing_index_builder_rejects_missing_time_range() {
    let mut symbols = SegmentSymbols::default();
    let name = symbols.intern("route");
    let value = symbols.intern("/api");
    let mut postings = ExactPostingsIndex::default();
    postings.insert(name, value, 0);

    let error = SegmentRoutingIndex::from_indexes(
        &symbols,
        &postings,
        &LabelValueTimeRangeIndex::default(),
    )
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("has no label-value time range"));
}

#[test]
fn routing_index_builder_rejects_unresolved_symbols() {
    let mut symbols = SegmentSymbols::default();
    let present = symbols.intern("present");
    for (name_sym, value_sym, expected) in [(2, present, "label-name"), (present, 2, "label-value")]
    {
        let mut postings = ExactPostingsIndex::default();
        postings.insert(name_sym, value_sym, 0);
        let mut ranges = LabelValueTimeRangeIndex::default();
        ranges.insert(name_sym, value_sym, 100, 200);

        let error = SegmentRoutingIndex::from_indexes(&symbols, &postings, &ranges).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains(expected));
        assert!(error.to_string().contains("cannot be resolved"));
    }
}

#[test]
fn routing_index_builder_is_complete_and_deterministic() {
    let mut symbols = SegmentSymbols::default();
    let metric_name = symbols.intern(METRIC_NAME_LABEL);
    let metric = symbols.intern("request_duration_seconds");
    let route = symbols.intern("route");
    let api = symbols.intern("/api");

    let mut forward_postings = ExactPostingsIndex::default();
    forward_postings.insert(route, api, 2);
    forward_postings.insert(metric_name, metric, 1);
    forward_postings.insert(route, api, 0);
    forward_postings.insert(metric_name, metric, 0);
    let mut reverse_postings = ExactPostingsIndex::default();
    reverse_postings.insert(metric_name, metric, 0);
    reverse_postings.insert(route, api, 0);
    reverse_postings.insert(metric_name, metric, 1);
    reverse_postings.insert(route, api, 2);

    let mut forward_ranges = LabelValueTimeRangeIndex::default();
    forward_ranges.insert(route, api, 300, 350);
    forward_ranges.insert(metric_name, metric, 100, 200);
    forward_ranges.insert(route, api, 250, 400);
    let mut reverse_ranges = LabelValueTimeRangeIndex::default();
    reverse_ranges.insert(metric_name, metric, 100, 200);
    reverse_ranges.insert(route, api, 250, 400);

    let forward =
        SegmentRoutingIndex::from_indexes(&symbols, &forward_postings, &forward_ranges).unwrap();
    let reverse =
        SegmentRoutingIndex::from_indexes(&symbols, &reverse_postings, &reverse_ranges).unwrap();

    assert_eq!(forward.len(), forward_postings.len());
    assert_eq!(forward, reverse);
    assert_eq!(forward.encode().unwrap(), reverse.encode().unwrap());
    assert_eq!(
        forward.exact_postings_metadata("route", "/api"),
        Some(ExactPostingsMetadata {
            byte_len: 12,
            time_range: LabelValueTimeRange {
                min_time_ms: 250,
                max_time_ms: 400,
            },
        })
    );
}

#[test]
fn routing_direct_encoder_matches_staged_reference() {
    let mut cases = vec![Vec::new(), vec![("one".to_owned(), "entry".to_owned())]];
    for entry_count in [2usize, 3, 4, 7, 8, 15, 16] {
        cases.push(
            (0..entry_count)
                .map(|entry| (format!("name-{}", entry % 3), format!("value-{entry:03}")))
                .collect(),
        );
    }
    cases.push(vec![
        ("z".to_owned(), String::new()),
        ("aa".to_owned(), "embedded\0nul".to_owned()),
        ("标签".to_owned(), "значение".to_owned()),
        ("long".to_owned(), "λ".repeat(300)),
    ]);

    for (case_index, keys) in cases.iter().enumerate() {
        let mut symbols = SegmentSymbols::default();
        let mut postings = ExactPostingsIndex::default();
        let mut ranges = LabelValueTimeRangeIndex::default();
        for (entry_index, (name, value)) in keys.iter().enumerate() {
            let name_sym = symbols.intern(name);
            let value_sym = symbols.intern(value);
            let series_ref = u32::try_from(entry_index).unwrap();
            let timestamp = 1_000 + u64::try_from(entry_index).unwrap();
            postings.insert(name_sym, value_sym, series_ref);
            ranges.insert(name_sym, value_sym, timestamp, timestamp + 100);
        }
        let routing = SegmentRoutingIndex::from_indexes(&symbols, &postings, &ranges).unwrap();

        let direct = routing.encode().unwrap();
        let staged = routing.encode_staged_reference_for_test().unwrap();

        assert_eq!(direct, staged, "case {case_index}");
        assert_eq!(
            SegmentRoutingIndex::decode(&direct).unwrap(),
            routing,
            "case {case_index}"
        );
    }
}

#[test]
fn adaptive_routing_records_the_canonical_encoded_postings_length() {
    let mut symbols = SegmentSymbols::default();
    let name = symbols.intern("route");
    let dense_value = symbols.intern("dense");
    let tie_value = symbols.intern("raw-tie");
    let mut postings = ExactPostingsIndex::default();
    for series_ref in 0..4 {
        postings.insert(name, dense_value, series_ref);
    }
    postings.insert(name, tie_value, 1 << 21);
    let mut ranges = LabelValueTimeRangeIndex::default();
    ranges.insert(name, dense_value, 100, 200);
    ranges.insert(name, tie_value, 100, 200);

    let raw = SegmentRoutingIndex::from_indexes(&symbols, &postings, &ranges).unwrap();
    let adaptive =
        SegmentRoutingIndex::from_indexes_adaptive(&symbols, &postings, &ranges).unwrap();

    assert_eq!(
        raw.exact_postings_metadata("route", "dense")
            .unwrap()
            .byte_len,
        20
    );
    assert_eq!(
        adaptive
            .exact_postings_metadata("route", "dense")
            .unwrap()
            .byte_len,
        8,
        "four dense refs use the four-byte header plus four one-byte deltas"
    );
    assert_eq!(
        adaptive
            .exact_postings_metadata("route", "raw-tie")
            .unwrap()
            .byte_len,
        8,
        "a four-byte singleton varint ties RAW32, which must win"
    );
}

#[test]
fn metric_series_ranges_group_metric_major_series() {
    let mut symbols = SegmentSymbols::default();
    let metric = symbols.intern(METRIC_NAME_LABEL);
    let cpu = symbols.intern("cpu_usage");
    let memory = symbols.intern("memory_usage");
    let pod = symbols.intern("pod");
    let pod_a = symbols.intern("a");
    let pod_b = symbols.intern("b");
    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(metric, cpu), (pod, pod_a)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: SERIES_KIND_HISTOGRAM,
            chunk_index: Default::default(),
            labels: vec![(metric, cpu), (pod, pod_b)],
        },
        SeriesEntry {
            series_id: 3,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(metric, memory), (pod, pod_a)],
        },
    ];
    let mut time_ranges = LabelValueTimeRangeIndex::default();
    time_ranges.insert(metric, cpu, 1_000, 2_000);
    time_ranges.insert(metric, memory, 3_000, 4_000);

    let ranges = MetricSeriesRangeIndex::from_series(&series, &symbols, &time_ranges).unwrap();

    assert_eq!(
        ranges.ranges(cpu),
        &[MetricSeriesRange {
            start_series_ref: 0,
            series_count: 2,
            kind_mask: u16::from(SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM),
            min_time_ms: 1_000,
            max_time_ms: 2_000,
        }]
    );
    assert_eq!(
        ranges.ranges(memory),
        &[MetricSeriesRange {
            start_series_ref: 2,
            series_count: 1,
            kind_mask: u16::from(SERIES_KIND_FLOAT),
            min_time_ms: 3_000,
            max_time_ms: 4_000,
        }]
    );
    ranges
        .validate_complete_partition(3, u32::try_from(symbols.len()).unwrap())
        .unwrap();

    let indexes = SegmentIndexes {
        metric_series_ranges: ranges.clone(),
        ..SegmentIndexes::default()
    };
    let mut encoded = Vec::new();
    write_segment_indexes_for_roots(&mut encoded, &indexes, 3, &symbols).unwrap();
    assert!(!encoded.is_empty());

    let trailing_gap =
        write_segment_indexes_for_roots(Vec::new(), &indexes, 4, &symbols).unwrap_err();
    assert_eq!(trailing_gap.kind(), io::ErrorKind::InvalidData);

    let mut short_symbols = SegmentSymbols::default();
    short_symbols.intern("first");
    short_symbols.intern("second");
    let foreign_symbol =
        write_segment_indexes_for_roots(Vec::new(), &indexes, 3, &short_symbols).unwrap_err();
    assert_eq!(foreign_symbol.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn production_index_writer_rejects_foreign_root_references() {
    let mut symbols = SegmentSymbols::default();
    symbols.intern("zero");
    symbols.intern("one");
    symbols.intern("two");
    let mut metric_series_ranges = MetricSeriesRangeIndex::default();
    metric_series_ranges.insert_range(
        1,
        MetricSeriesRange {
            start_series_ref: 0,
            series_count: 1,
            kind_mask: u16::from(SERIES_KIND_FLOAT),
            min_time_ms: 100,
            max_time_ms: 200,
        },
    );
    let valid = SegmentIndexes {
        metric_series_ranges,
        ..SegmentIndexes::default()
    };

    let mut foreign_exact_symbol = valid.clone();
    foreign_exact_symbol.exact_postings.insert(3, 1, 0);
    let error = write_segment_indexes_for_roots(Vec::new(), &foreign_exact_symbol, 1, &symbols)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let mut foreign_series_ref = valid.clone();
    foreign_series_ref.exact_postings.insert(0, 1, 1);
    let error =
        write_segment_indexes_for_roots(Vec::new(), &foreign_series_ref, 1, &symbols).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let mut foreign_fst_symbol = valid.clone();
    foreign_fst_symbol.label_values.insert_fst(3, vec![0]);
    let error =
        write_segment_indexes_for_roots(Vec::new(), &foreign_fst_symbol, 1, &symbols).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let mut foreign_time_range_symbol = valid;
    foreign_time_range_symbol
        .label_value_time_ranges
        .insert(0, 3, 100, 200);
    let error =
        write_segment_indexes_for_roots(Vec::new(), &foreign_time_range_symbol, 1, &symbols)
            .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn production_index_writer_rejects_stale_routing_metadata() {
    let mut symbols = SegmentSymbols::default();
    let metric_name = symbols.intern(METRIC_NAME_LABEL);
    let metric = symbols.intern("request_duration_seconds");
    let mut exact_postings = ExactPostingsIndex::default();
    exact_postings.insert(metric_name, metric, 0);
    let mut time_ranges = LabelValueTimeRangeIndex::default();
    time_ranges.insert(metric_name, metric, 100, 200);
    let routing_index =
        SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &time_ranges).unwrap();
    time_ranges.insert(metric_name, metric, 50, 250);
    let mut metric_series_ranges = MetricSeriesRangeIndex::default();
    metric_series_ranges.insert_range(
        metric,
        MetricSeriesRange {
            start_series_ref: 0,
            series_count: 1,
            kind_mask: u16::from(SERIES_KIND_FLOAT),
            min_time_ms: 50,
            max_time_ms: 250,
        },
    );
    let indexes = SegmentIndexes {
        exact_postings,
        label_value_time_ranges: time_ranges,
        metric_series_ranges,
        routing_index: Some(routing_index),
        ..SegmentIndexes::default()
    };

    let error = write_segment_indexes_for_roots(Vec::new(), &indexes, 1, &symbols).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("routing index metadata does not match")
    );
}

#[test]
fn metric_series_ranges_decoder_rejects_zero_range_count() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&7u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());

    let error = read_metric_series_ranges_blob(&bytes).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn metric_series_ranges_reject_impossible_group_count_before_visiting() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&2u32.to_le_bytes());
    // One canonical minimum-size group (8-byte group header plus one
    // 28-byte range) cannot encode the declared two groups.
    bytes.resize(12 + 8 + METRIC_SERIES_RANGE_RECORD_LEN, 0);
    let mut visited = false;

    let error = walk_metric_series_ranges_blob(&bytes, None, |_| {
        visited = true;
        Ok(())
    })
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        !visited,
        "invalid group count reached an allocating visitor"
    );
}

#[test]
fn segment_index_roundtrips_required_metric_series_ranges() {
    let mut metric_series_ranges = MetricSeriesRangeIndex::default();
    metric_series_ranges.insert_range(
        10,
        MetricSeriesRange {
            start_series_ref: 4,
            series_count: 3,
            kind_mask: u16::from(SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM),
            min_time_ms: 1_000,
            max_time_ms: 2_000,
        },
    );
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values: LabelValueFstIndex::default(),
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges,
        routing_index: None,
    };

    let mut bytes = Vec::new();
    write_segment_indexes_unbound_for_test(&mut bytes, &indexes).unwrap();
    let reader = SegmentIndexReader::open(Cursor::new(bytes)).unwrap();

    assert_eq!(
        reader.metric_series_ranges(10).unwrap(),
        vec![MetricSeriesRange {
            start_series_ref: 4,
            series_count: 3,
            kind_mask: u16::from(SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM),
            min_time_ms: 1_000,
            max_time_ms: 2_000,
        }]
    );
    assert!(reader.metric_series_ranges(11).unwrap().is_empty());
}

#[test]
fn segment_index_reader_rejects_v6_container() {
    let mut bytes = vec![0u8; 272];
    bytes[0..4].copy_from_slice(&SEGMENT_INDEXES_MAGIC.to_le_bytes());
    bytes[4..6].copy_from_slice(&SEGMENT_INDEX_VERSION.to_le_bytes());

    let err = match SegmentIndexReader::open(Cursor::new(bytes.clone())) {
        Ok(_) => panic!("expected v6 rejection"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("expected segment index version 7"));

    let err = read_segment_indexes(Cursor::new(bytes)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("expected segment index version 7"));
}

#[test]
fn segment_index_reader_streams_label_values_by_prefix() {
    let mut symbols = SegmentSymbols::default();
    let name = symbols.intern(METRIC_NAME_LABEL);
    let values = [
        "alpha_metric",
        "beta_metric",
        "go_gc_duration_seconds",
        "go_gc_duration_seconds_count",
        "http_requests_total",
    ];
    let series: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(idx, value)| SeriesEntry {
            series_id: idx as u64 + 1,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(name, symbols.intern(value))],
        })
        .collect();
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values: LabelValueFstIndex::from_series(&series, &symbols).unwrap(),
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };

    let mut bytes = Vec::new();
    write_segment_indexes_unbound_for_test(&mut bytes, &indexes).unwrap();
    let reader = SegmentIndexReader::open(Cursor::new(bytes)).unwrap();

    assert_eq!(
        reader
            .label_values_with_prefix(name, Some("go_gc_duration_seconds"))
            .unwrap(),
        vec![
            "go_gc_duration_seconds".to_string(),
            "go_gc_duration_seconds_count".to_string()
        ]
    );
    assert!(
        reader
            .label_values_with_prefix(name, Some("missing"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader.label_values_with_prefix(name, None).unwrap().len(),
        5
    );
}

#[test]
fn label_value_fsts_from_exact_postings_match_series_scan() {
    let mut symbols = SegmentSymbols::default();
    let pod = symbols.intern("pod");
    let value_z = symbols.intern("z-value");
    let namespace = symbols.intern("namespace");
    let value_default = symbols.intern("default");
    let value_a = symbols.intern("a-value");
    let series = vec![
        SeriesEntry {
            series_id: 1,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(pod, value_z), (namespace, value_default), (pod, value_z)],
        },
        SeriesEntry {
            series_id: 2,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(pod, value_a), (namespace, value_default)],
        },
    ];
    let mut postings = ExactPostingsIndex::default();
    for (series_ref, entry) in series.iter().enumerate() {
        let series_ref = u32::try_from(series_ref).unwrap();
        for &(name, value) in &entry.labels {
            postings.insert(name, value, series_ref);
        }
    }

    let scanned = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let from_postings = LabelValueFstIndex::from_exact_postings(&postings, &symbols).unwrap();

    assert_eq!(from_postings, scanned);
    assert_eq!(
        from_postings.values(pod).unwrap(),
        vec!["a-value".to_string(), "z-value".to_string()]
    );
    assert_eq!(
        from_postings.values(namespace).unwrap(),
        vec!["default".to_string()]
    );
}

#[test]
fn label_value_fsts_from_exact_postings_match_empty_and_missing_symbol_behavior() {
    let symbols = SegmentSymbols::default();
    assert_eq!(
        LabelValueFstIndex::from_exact_postings(&ExactPostingsIndex::default(), &symbols).unwrap(),
        LabelValueFstIndex::from_series(&[], &symbols).unwrap()
    );

    let mut symbols = SegmentSymbols::default();
    let name = symbols.intern("pod");
    let missing_value = u32::MAX;
    let series = vec![SeriesEntry {
        series_id: 1,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(name, missing_value)],
    }];
    let mut postings = ExactPostingsIndex::default();
    postings.insert(name, missing_value, 0);

    let scanned_error = LabelValueFstIndex::from_series(&series, &symbols).unwrap_err();
    let postings_error = LabelValueFstIndex::from_exact_postings(&postings, &symbols).unwrap_err();
    assert_eq!(postings_error.kind(), scanned_error.kind());
    assert_eq!(postings_error.to_string(), scanned_error.to_string());
}

#[test]
fn segment_index_reader_clones_share_immutable_directory() {
    let mut symbols = SegmentSymbols::default();
    let metric_name = symbols.intern(METRIC_NAME_LABEL);
    let metric = symbols.intern("request_duration_seconds");
    let series = vec![SeriesEntry {
        series_id: 7,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(metric_name, metric)],
    }];

    let mut exact_postings = ExactPostingsIndex::default();
    exact_postings.insert(metric_name, metric, 0);
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    label_value_time_ranges.insert(metric_name, metric, 1_000, 2_000);
    let mut metric_series_ranges = MetricSeriesRangeIndex::default();
    metric_series_ranges.insert_range(
        metric,
        MetricSeriesRange {
            start_series_ref: 0,
            series_count: 1,
            kind_mask: u16::from(SERIES_KIND_FLOAT),
            min_time_ms: 1_000,
            max_time_ms: 2_000,
        },
    );
    let routing_index =
        SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &label_value_time_ranges)
            .unwrap();
    let indexes = SegmentIndexes {
        exact_postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges,
        routing_index: Some(routing_index),
    };

    let mut file = tempfile::tempfile().unwrap();
    write_segment_indexes_unbound_for_test(&mut file, &indexes).unwrap();
    let reader = SegmentIndexReader::open(file).unwrap();
    let cloned = reader.try_clone_reader().unwrap();

    assert!(reader.read_stats().root.calls > 0);
    assert_eq!(cloned.read_stats(), SegmentIndexReadStats::default());
    assert_eq!(
        reader.exact_postings(metric_name, metric).unwrap(),
        cloned.exact_postings(metric_name, metric).unwrap()
    );
    assert_eq!(
        reader.label_values(metric_name).unwrap(),
        cloned.label_values(metric_name).unwrap()
    );
    assert_eq!(
        reader.label_value_time_range(metric_name, metric).unwrap(),
        cloned.label_value_time_range(metric_name, metric).unwrap()
    );
    assert_eq!(
        reader.metric_series_ranges(metric).unwrap(),
        cloned.metric_series_ranges(metric).unwrap()
    );
    assert_eq!(
        reader
            .routing_exact_postings_metadata(METRIC_NAME_LABEL, "request_duration_seconds")
            .unwrap(),
        cloned
            .routing_exact_postings_metadata(METRIC_NAME_LABEL, "request_duration_seconds")
            .unwrap()
    );
}
