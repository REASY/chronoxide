use super::*;

#[test]
fn segment_series_metadata_builder_matches_raw_label_canonicalization() {
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];

    let canonical = crate::promql::canonicalize_labelset(
        "cpu.usage",
        &[("namespace", "default"), ("pod.name", "backend-1")],
    );
    let expected_series_id = crate::promql::series_id(&canonical);
    let expected_labels: Vec<_> = canonical
        .labels()
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();

    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in &raw_labels {
        builder.push_label(key, value);
    }
    let metadata = builder.finish();

    assert_eq!(metadata.series_id, expected_series_id);
    assert_eq!(metadata.labels, expected_labels);
}

#[test]
fn segment_series_metadata_builder_keeps_first_metric_name() {
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.first".to_string()),
        (METRIC_NAME_LABEL.to_string(), "cpu.second".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];

    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in &raw_labels {
        builder.push_label(key, value);
    }
    let metadata = builder.finish();

    assert!(metadata.labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.first")
    }));
    assert!(!metadata.labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.second")
    }));
}

#[test]
fn existing_metric_name_fast_path_leaves_metadata_unchanged() {
    let mut symbols = SegmentSymbols::default();
    let pod_key = symbols.intern("pod");
    let pod_value = symbols.intern("backend-1");
    let metric_key = symbols.intern(METRIC_NAME_LABEL);
    let metric_value = symbols.intern("cpu_usage");
    let entry = super::writer::WriterSeriesEntry {
        series_id: 42,
        kind_mask: SERIES_KIND_FLOAT,
        labels: vec![(pod_key, pod_value), (metric_key, metric_value)],
    };
    let expected_symbols = symbols.clone();
    let mut entries = super::writer::WriterSeriesEntryStore::from_owned(vec![entry]).unwrap();
    let expected_entries = entries.clone();

    super::writer::synthesize_missing_metric_name(&mut symbols, &mut entries, 0).unwrap();

    assert_eq!(symbols, expected_symbols);
    assert_eq!(entries, expected_entries);
}

#[test]
fn missing_metric_name_is_synthesized_and_rehashes_canonical_labels() {
    let mut symbols = SegmentSymbols::default();
    let pod_key = symbols.intern("pod");
    let pod_value = symbols.intern("backend-1");
    let namespace_key = symbols.intern("namespace");
    let namespace_value = symbols.intern("default");
    let entry = super::writer::WriterSeriesEntry {
        series_id: 42,
        kind_mask: SERIES_KIND_FLOAT,
        labels: vec![(pod_key, pod_value), (namespace_key, namespace_value)],
    };
    let mut entries = super::writer::WriterSeriesEntryStore::from_owned(vec![entry]).unwrap();
    let expected_labels = vec![
        (METRIC_NAME_LABEL.to_string(), String::new()),
        ("namespace".to_string(), "default".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ];

    super::writer::synthesize_missing_metric_name(&mut symbols, &mut entries, 0).unwrap();

    let entry = entries.get_entry(0).unwrap();
    assert_eq!(entry.labels().len(), 3);
    assert!(entry.labels().iter().any(|(key, value)| {
        symbols.resolve(*key) == Some(METRIC_NAME_LABEL) && symbols.resolve(*value) == Some("")
    }));
    assert_eq!(entry.series_id(), segment_series_id(&expected_labels));
}

#[test]
fn existing_metric_name_fast_path_still_rejects_later_missing_symbols() {
    let mut symbols = SegmentSymbols::default();
    let metric_key = symbols.intern(METRIC_NAME_LABEL);
    let metric_value = symbols.intern("cpu_usage");
    let pod_key = symbols.intern("pod");
    let pod_value = symbols.intern("backend-1");

    for (labels, expected_message) in [
        (
            vec![(metric_key, metric_value), (u32::MAX, pod_value)],
            "series references missing key symbol",
        ),
        (
            vec![(metric_key, metric_value), (pod_key, u32::MAX)],
            "series references missing value symbol",
        ),
    ] {
        let entry = super::writer::WriterSeriesEntry {
            series_id: 42,
            kind_mask: SERIES_KIND_FLOAT,
            labels,
        };
        let expected_symbols = symbols.clone();
        let mut entries = super::writer::WriterSeriesEntryStore::from_owned(vec![entry]).unwrap();
        let expected_entries = entries.clone();

        let error = super::writer::synthesize_missing_metric_name(&mut symbols, &mut entries, 0)
            .expect_err("missing symbol must remain corruption");

        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(error.to_string(), expected_message);
        assert_eq!(symbols, expected_symbols);
        assert_eq!(entries, expected_entries);
    }
}

#[test]
fn label_visitor_encoder_matches_metadata_builder_canonicalization() {
    let raw_labels = [
        (METRIC_NAME_LABEL, "cpu.usage"),
        ("pod.name", "backend-1"),
        ("namespace", "default"),
    ];
    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in raw_labels {
        builder.push_label(key, value);
    }
    let expected = builder.finish();

    let mut symbols = SegmentSymbols::default();
    let entry = encode_label_visitor_metadata(&mut symbols, |visit| {
        for (key, value) in raw_labels {
            visit(key, value);
        }
    });

    let labels = resolved_entry_labels(&symbols, &entry);
    assert_eq!(entry.series_id, expected.series_id);
    assert_eq!(labels, expected.labels);
}

#[test]
fn borrowed_label_encoder_matches_owned_canonical_encoding() {
    let canonical = [
        (
            METRIC_NAME_LABEL.to_string(),
            normalize_metric_name("cpu.usage"),
        ),
        (normalize_label_name("namespace"), "default".to_string()),
        (normalize_label_name("pod.name"), "backend-1".to_string()),
    ];

    let mut owned_symbols = SegmentSymbols::default();
    let owned = encode_canonical_segment_labels(
        canonical
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        &mut owned_symbols,
    );

    let mut borrowed_symbols = SegmentSymbols::default();
    let borrowed = encode_borrowed_canonical_segment_labels(
        canonical
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        &mut borrowed_symbols,
    );

    assert_eq!(borrowed.series_id, owned.series_id);
    assert_eq!(
        resolved_entry_labels(&borrowed_symbols, &borrowed),
        resolved_entry_labels(&owned_symbols, &owned)
    );
}

#[test]
fn flat_interned_label_encoder_matches_visitor_encoding() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("namespace", "default")),
        crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut visitor_symbols = SegmentSymbols::default();
    let visitor = encode_label_visitor_metadata(&mut visitor_symbols, |visit| {
        crate::labels::LabelSetStore::visit_labelset(&store, series, |key, value| visit(key, value))
    });

    let mut flat_symbols = SegmentSymbols::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::new();
    let mut label_scratch = Vec::new();
    let flat = encode_flat_interned_label_metadata(
        &mut flat_symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );

    assert_eq!(flat.series_id, visitor.series_id);
    assert_eq!(
        resolved_entry_labels(&flat_symbols, &flat),
        resolved_entry_labels(&visitor_symbols, &visitor)
    );
}

#[test]
fn flat_interned_label_encoder_reuses_scratch_buffers() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("namespace", "default")),
        crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut symbols = SegmentSymbols::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::with_capacity(256);
    let initial_capacity = hash_scratch.capacity();
    let mut label_scratch = Vec::with_capacity(32);
    let initial_label_capacity = label_scratch.capacity();

    let first = encode_flat_interned_label_metadata(
        &mut symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );
    assert_eq!(hash_scratch.len(), 0);
    assert_eq!(hash_scratch.capacity(), initial_capacity);
    assert_eq!(label_scratch.len(), 0);
    assert_eq!(label_scratch.capacity(), initial_label_capacity);

    let second = encode_flat_interned_label_metadata(
        &mut symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );
    assert_eq!(hash_scratch.len(), 0);
    assert_eq!(hash_scratch.capacity(), initial_capacity);
    assert_eq!(label_scratch.len(), 0);
    assert_eq!(label_scratch.capacity(), initial_label_capacity);
    assert_eq!(second.series_id, first.series_id);
    assert_eq!(
        resolved_entry_labels(&symbols, &second),
        resolved_entry_labels(&symbols, &first)
    );
}

#[test]
fn flat_interned_label_encoder_matches_disambiguated_label_canonicalization() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("pod.name", "dotted")),
        crate::labels::KeyValueRef::from(("pod_name", "underscore")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut visitor_symbols = SegmentSymbols::default();
    let visitor = encode_label_visitor_metadata(&mut visitor_symbols, |visit| {
        crate::labels::LabelSetStore::visit_labelset(&store, series, |key, value| visit(key, value))
    });

    let mut flat_symbols = SegmentSymbols::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::new();
    let mut label_scratch = Vec::new();
    let flat = encode_flat_interned_label_metadata(
        &mut flat_symbols,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        &store,
        series,
    );

    let flat_labels = resolved_entry_labels(&flat_symbols, &flat);
    assert_eq!(flat.series_id, visitor.series_id);
    assert_eq!(
        flat_labels,
        resolved_entry_labels(&visitor_symbols, &visitor)
    );
    assert_eq!(
        flat_labels,
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.usage")
            ),
            (normalize_label_name("pod_name"), "underscore".to_string()),
            (normalize_label_name("pod.name"), "dotted".to_string()),
        ]
    );
}

#[test]
fn flat_interned_sorted_label_encoder_keeps_last_duplicate_key() {
    let duplicate_key: Arc<str> = Arc::from("pod_name");
    let labels = vec![
        (
            Arc::from(METRIC_NAME_LABEL),
            SourceLabelValue::Owned(Arc::from("cpu_usage")),
        ),
        (
            Arc::clone(&duplicate_key),
            SourceLabelValue::Owned(Arc::from("first")),
        ),
        (duplicate_key, SourceLabelValue::Owned(Arc::from("second"))),
    ];
    let source_symbols = crate::labels::DefaultSymbolTable::default();
    let mut symbols = SegmentSymbols::default();
    let mut hash_scratch = Vec::new();

    let entry = encode_flat_interned_sorted_labels(
        &labels,
        &source_symbols,
        &mut symbols,
        &mut hash_scratch,
    );

    assert_eq!(
        resolved_entry_labels(&symbols, &entry),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            (normalize_label_name("pod_name"), "second".to_string()),
        ]
    );
}

#[test]
fn normalized_name_cache_reuses_label_and_metric_names_by_source_symbol_id() {
    let mut cache = NormalizedNameCache::default();
    let mut label_normalizations = 0usize;
    let mut metric_normalizations = 0usize;
    let mut source_symbols = crate::labels::DefaultSymbolTable::default();
    let label_id = source_symbols.intern("pod.name").unwrap();
    let metric_id = source_symbols.intern("cpu.usage").unwrap();

    let first_label = cache.label_name(label_id, "pod.name", |name| {
        label_normalizations += 1;
        normalize_label_name(name)
    });
    let second_label = cache.label_name(label_id, "pod.name", |name| {
        label_normalizations += 1;
        normalize_label_name(name)
    });
    let first_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
        metric_normalizations += 1;
        normalize_metric_name(name)
    });
    let second_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
        metric_normalizations += 1;
        normalize_metric_name(name)
    });

    assert_eq!(first_label.as_ref(), normalize_label_name("pod.name"));
    assert_eq!(second_label, first_label);
    assert_eq!(first_metric.as_ref(), normalize_metric_name("cpu.usage"));
    assert_eq!(second_metric, first_metric);
    assert_eq!(label_normalizations, 1);
    assert_eq!(metric_normalizations, 1);
}

#[test]
fn normalized_name_cache_falls_back_to_uncached_normalization_after_cap() {
    let mut cache = NormalizedNameCache::with_max_entries(1);
    let mut source_symbols = crate::labels::DefaultSymbolTable::default();
    let first_id = source_symbols.intern("pod.name").unwrap();
    let second_id = source_symbols.intern("container.name").unwrap();
    let mut normalizations = 0usize;

    cache.label_name(first_id, "pod.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(first_id, "pod.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(second_id, "container.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(second_id, "container.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });

    assert_eq!(normalizations, 3);
}

#[test]
fn label_visitor_encoder_keeps_first_metric_name_and_sorts_labels() {
    let mut symbols = SegmentSymbols::default();

    let entry = encode_label_visitor_metadata(&mut symbols, |visit| {
        visit("z.label", "last");
        visit(METRIC_NAME_LABEL, "cpu.first");
        visit("a.label", "first");
        visit(METRIC_NAME_LABEL, "cpu.second");
    });

    assert_eq!(
        resolved_entry_labels(&symbols, &entry),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.first")
            ),
            (normalize_label_name("a.label"), "first".to_string()),
            (normalize_label_name("z.label"), "last".to_string()),
        ]
    );
}

#[test]
fn label_value_time_ranges_update_from_encoded_series_entry() {
    let mut index = LabelValueTimeRangeIndex::default();
    let entry = SeriesEntry {
        series_id: 7,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(1, 10), (2, 20)],
    };
    let first_chunk = ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms: 1_000,
        max_time_ms: 2_000,
        offset: 0,
        length: 1,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    };
    let second_chunk = ChunkIndexEntry {
        min_time_ms: 500,
        max_time_ms: 4_000,
        ..first_chunk.clone()
    };

    update_label_value_time_ranges(&mut index, &entry, &first_chunk);
    update_label_value_time_ranges(&mut index, &entry, &second_chunk);

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
            min_time_ms: 500,
            max_time_ms: 4_000,
        })
    );
}
