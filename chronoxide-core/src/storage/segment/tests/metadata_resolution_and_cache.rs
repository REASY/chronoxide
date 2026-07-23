use super::*;

#[test]
fn query_profile_reports_chunk_payload_locality() {
    let mut profile = SegmentStoreQueryProfile::default();

    profile.observe_chunk_payload_file_reads(&[(100, 10), (110, 5), (200, 10), (260, 5), (240, 5)]);

    let locality = profile.chunk_payload_locality;
    assert_eq!(locality.reads, 5);
    assert_eq!(locality.backward_jumps, 1);
    assert_eq!(locality.forward_gaps, 2);
    assert_eq!(locality.forward_gap_bytes, 135);
    assert_eq!(locality.contiguous_runs, 4);
    assert_eq!(locality.contiguous_span_bytes, 35);
    assert_eq!(locality.coalesced_4k_runs, 2);
    assert_eq!(locality.coalesced_4k_span_bytes, 170);
    assert_eq!(locality.coalesced_64k_runs, 2);
    assert_eq!(locality.coalesced_64k_span_bytes, 170);
}

#[test]
fn query_profile_reports_sorted_chunk_payload_coalescing_potential() {
    let mut profile = SegmentStoreQueryProfile::default();
    let mut ranges = [(200, 10), (100, 10), (110, 5), (260, 5), (240, 5)];

    profile.observe_sorted_chunk_payload_ranges(&mut ranges);

    let locality = profile.chunk_payload_locality;
    assert_eq!(locality.reads, 0);
    assert_eq!(locality.sorted_contiguous_runs, 4);
    assert_eq!(locality.sorted_contiguous_span_bytes, 35);
    assert_eq!(locality.sorted_coalesced_4k_runs, 1);
    assert_eq!(locality.sorted_coalesced_4k_span_bytes, 165);
    assert_eq!(locality.sorted_coalesced_64k_runs, 1);
    assert_eq!(locality.sorted_coalesced_64k_span_bytes, 165);
}

#[test]
fn metadata_runtime_reports_schema7_reads_and_reuses_governed_roots() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 42.0)], |visit| {
            visit(METRIC_NAME_LABEL, "profile.metric");
            visit("pod", "backend-1");
        })
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let query = normalize_metric_name("profile.metric");

    let before_first = store.metadata_runtime_snapshot();
    let mut first_session = store.query_session().unwrap();
    assert_eq!(
        first_session.query_promql(&query, 0, 2_000).unwrap().len(),
        1
    );
    let after_first = store.metadata_runtime_snapshot();
    let first_reads = after_first.reads.delta_since(before_first.reads);
    assert!(first_reads.issued.calls > 0);
    assert!(first_reads.issued.bytes > 0);
    assert!(after_first.cache.resident_entries > 0);
    assert!(after_first.governor.retained_bytes > 0);
    assert_eq!(after_first.governor.in_flight_bytes, 0);
    drop(first_session);

    let before_second = store.metadata_runtime_snapshot();
    let mut second_session = store.query_session().unwrap();
    assert_eq!(
        second_session.query_promql(&query, 0, 2_000).unwrap().len(),
        1
    );
    let after_second = store.metadata_runtime_snapshot();
    assert!(after_second.cache.hits > before_second.cache.hits);
    assert_eq!(after_second.governor.in_flight_bytes, 0);
    assert!(after_second.governor.retained_bytes > 0);
}

#[test]
fn regex_literal_prefix_extracts_only_safe_prefixes() {
    assert_eq!(
        regex_literal_prefix("go_gc_duration_seconds.*"),
        Some("go_gc_duration_seconds".to_string())
    );
    assert_eq!(
        regex_literal_prefix("^rpc_duration.*_count"),
        Some("rpc_duration".to_string())
    );
    assert_eq!(
        regex_literal_prefix(r"http\.request\..*"),
        Some("http.request.".to_string())
    );
    assert_eq!(regex_literal_prefix(".*_count"), None);
    assert_eq!(regex_literal_prefix("[a-z].*"), None);
    assert_eq!(regex_literal_prefix(r"\d+"), None);
    assert_eq!(regex_literal_prefix("a|c"), None);
    assert_eq!(regex_literal_prefix("foo.*|bar.*"), None);
    assert_eq!(
        regex_literal_prefixes("rpc_duration_count", true),
        vec!["rpc_duration".to_string(), "rpc_duration_count".to_string()]
    );
    assert_eq!(
        regex_literal_prefixes("rpc_duration_count", false),
        vec!["rpc_duration_count".to_string()]
    );
}

#[test]
fn regex_symbol_lookup_batches_preserve_results_with_exact_postings_fallback() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();
    let series_count = REGEX_SYMBOL_LOOKUP_BATCH_VALUES + 1;
    let long_suffix = "x".repeat(120);
    for index in 0..series_count {
        let value = format!("batch-value-{index:04}-{long_suffix}");
        let timestamp_ms = if index % 2 == 0 { 1_000 } else { 9_000 };
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(index as u32 + 1),
                &[(timestamp_ms, index as f64)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "regex_batch_metric");
                    visit("batch_value", &value);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut session = store.query_session().unwrap();
    let expected = session
        .query_promql("regex_batch_metric", 0, 2_000)
        .unwrap();
    let actual = session
        .query_promql_with_limits(
            r#"regex_batch_metric{batch_value=~"batch-value-.*"}"#,
            0,
            2_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(actual.results, expected);
    assert_eq!(actual.results.len(), series_count.div_ceil(2));
    // Schema 7 reads one integrity-checked exact/routing pair for each result.
    assert_eq!(
        actual.stats.index_postings_reads,
        u64::try_from(actual.results.len()).unwrap() * 2
    );
    assert_eq!(
        actual.stats.regex_values_examined,
        u64::try_from(series_count).unwrap()
    );
}

#[test]
fn metadata_accumulator_sorts_dedupes_and_tracks_metric_names() {
    let mut metadata = MetadataAccumulator::default();
    metadata.add_labelset(&[
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod_name".to_string(), "backend-2".to_string()),
    ]);
    metadata.add_labelset(&[
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod_name".to_string(), "backend-1".to_string()),
        ("namespace".to_string(), "default".to_string()),
    ]);

    assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
    assert_eq!(
        metadata.label_names(),
        vec![
            METRIC_NAME_LABEL.to_string(),
            "namespace".to_string(),
            "pod_name".to_string()
        ]
    );
    assert_eq!(
        metadata.label_values("pod_name"),
        vec!["backend-1".to_string(), "backend-2".to_string()]
    );
}

#[test]
fn batched_series_label_resolution_reads_each_required_page_once() {
    let mut symbols = SegmentSymbols::default();
    let symbol_ids = ['a', 'b', 'c', 'd', 'e', 'f']
        .into_iter()
        .map(|prefix| symbols.intern(&format!("{prefix}{}", "x".repeat(12_000))))
        .collect::<Vec<_>>();
    let entries = (0..4)
        .map(|series_id| SeriesEntry {
            series_id,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![
                (symbol_ids[0], symbol_ids[2]),
                (symbol_ids[4], symbol_ids[5]),
            ],
        })
        .collect::<Vec<_>>();

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let scalar_reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes.clone()),
        0,
    )
    .unwrap();
    for entry in &entries {
        SegmentReader::resolve_series_labels(&scalar_reader, entry).unwrap();
    }

    let batch_reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let entry_refs = entries.iter().collect::<Vec<_>>();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&batch_reader, &entry_refs, &mut label_cache)
        .unwrap();

    assert_eq!(label_cache.len(), entries.len());
    for entry in &entries {
        assert_eq!(
            label_cache.get(&entry.series_id).unwrap().to_vec(),
            resolved_entry_labels(&symbols, entry)
        );
    }
    assert_eq!(batch_reader.read_stats().page.calls, 3);
    assert_eq!(scalar_reader.read_stats().page.calls, 16);
}

#[test]
fn batched_series_label_resolution_is_bounded_across_batch_limit() {
    let mut symbols = SegmentSymbols::default();
    let symbol_ids = ['a', 'b', 'c', 'd', 'e', 'f']
        .into_iter()
        .map(|prefix| symbols.intern(&format!("{prefix}{}", "x".repeat(12_000))))
        .collect::<Vec<_>>();
    let entry_count = super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES + 1;
    let entries = (0..entry_count)
        .map(|series_id| SeriesEntry {
            series_id: u64::try_from(series_id).unwrap(),
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(symbol_ids[0], symbol_ids[4])],
        })
        .collect::<Vec<_>>();

    assert_eq!(
        entries[..super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES]
            .iter()
            .map(|entry| entry.labels.len() * 2)
            .sum::<usize>(),
        super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES * 2
    );
    const {
        assert!(
            super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES * 2
                < super::query_reader::SERIES_LABEL_BATCH_MAX_SYMBOL_REFERENCES
        );
    }

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let entry_refs = entries.iter().collect::<Vec<_>>();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&reader, &entry_refs, &mut label_cache).unwrap();

    assert_eq!(label_cache.len(), entry_count);
    for entry in &entries {
        assert_eq!(
            label_cache.get(&entry.series_id).unwrap().to_vec(),
            resolved_entry_labels(&symbols, entry)
        );
    }
    // The two referenced IDs occupy two pages. A zero-byte cache makes the
    // entry-count boundary observable independently of the reference-count
    // boundary: each bounded batch reads those pages once.
    let stats = reader.read_stats();
    assert_eq!(stats.page.calls, 4);
    assert_eq!(
        stats.logical_returned.calls,
        u64::try_from(entry_count * 2).unwrap()
    );
}

#[test]
fn batched_series_label_resolution_splits_one_oversized_series() {
    let mut symbols = SegmentSymbols::default();
    let symbol_count = super::query_reader::SERIES_LABEL_BATCH_MAX_SYMBOL_REFERENCES + 4;
    let symbol_ids = (0..symbol_count)
        .map(|index| symbols.intern(&format!("symbol-{index:05}")))
        .collect::<Vec<_>>();
    let entry = SeriesEntry {
        series_id: 7,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: symbol_ids
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect(),
    };

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let page_counter = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes.clone()),
        0,
    )
    .unwrap();
    page_counter.validate_all().unwrap();
    let page_count = page_counter.read_stats().page.calls;

    let reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&reader, &[&entry], &mut label_cache).unwrap();

    assert_eq!(
        label_cache.get(&entry.series_id).unwrap().to_vec(),
        resolved_entry_labels(&symbols, &entry)
    );
    let stats = reader.read_stats();
    assert_eq!(
        stats.logical_returned.calls,
        u64::try_from(symbol_count).unwrap()
    );
    // The second four-reference visitor batch reopens the final page with a
    // zero-byte cache, proving that one oversized series does not bypass the
    // per-visitor reference bound.
    assert_eq!(stats.page.calls, page_count + 1);
}

#[test]
fn batched_series_label_resolution_skips_later_duplicate_series() {
    let mut symbols = SegmentSymbols::default();
    let symbol_ids = ['a', 'b', 'c', 'd']
        .into_iter()
        .map(|prefix| symbols.intern(&format!("{prefix}{}", "x".repeat(12_000))))
        .collect::<Vec<_>>();
    let first = SeriesEntry {
        series_id: 42,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(symbol_ids[0], symbol_ids[1])],
    };
    let duplicate_with_missing_symbols = SeriesEntry {
        series_id: first.series_id,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(u32::MAX, u32::MAX)],
    };

    let reader = paged_symbol_reader(&symbols);
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(
        &reader,
        &[&first, &duplicate_with_missing_symbols],
        &mut label_cache,
    )
    .unwrap();

    assert_eq!(label_cache.len(), 1);
    assert_eq!(
        label_cache.get(&first.series_id).unwrap().to_vec(),
        resolved_entry_labels(&symbols, &first)
    );
    assert_eq!(reader.read_stats().logical_returned.calls, 2);
}

#[test]
fn batched_series_label_resolution_skips_duplicate_across_batch_boundary() {
    let mut symbols = SegmentSymbols::default();
    let key = symbols.intern(&format!("a{}", "x".repeat(12_000)));
    let value = symbols.intern(&format!("b{}", "x".repeat(12_000)));
    let mut entries = (0..super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES)
        .map(|series_id| SeriesEntry {
            series_id: u64::try_from(series_id).unwrap(),
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(key, value)],
        })
        .collect::<Vec<_>>();
    entries.push(SeriesEntry {
        series_id: entries[0].series_id,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(u32::MAX, u32::MAX)],
    });

    let mut bytes = Vec::new();
    write_symbols_bin(&mut bytes, &symbols).unwrap();
    let reader = crate::storage::symbols::SegmentSymbolReader::open_with_cache_max_bytes(
        Cursor::new(bytes),
        0,
    )
    .unwrap();
    let entry_refs = entries.iter().collect::<Vec<_>>();
    let mut label_cache = SeriesLabelCache::default();
    SegmentReader::populate_series_label_cache(&reader, &entry_refs, &mut label_cache).unwrap();

    assert_eq!(
        label_cache.len(),
        super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES
    );
    assert_eq!(reader.read_stats().page.calls, 1);
    assert_eq!(
        reader.read_stats().logical_returned.calls,
        u64::try_from(super::query_reader::SERIES_LABEL_BATCH_MAX_ENTRIES * 2).unwrap()
    );
}

#[test]
fn metric_name_index_collection_reads_only_metric_name_values() {
    let mut symbols = SegmentSymbols::default();
    let metric = symbols.intern(METRIC_NAME_LABEL);
    let backend = symbols.intern("backend-1");
    let cpu = symbols.intern("cpu_usage");
    let pod = symbols.intern("pod_name");
    let series = vec![SeriesEntry {
        series_id: 1,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(metric, cpu), (pod, backend)],
    }];
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values,
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };
    let mut index_reader = index_reader_with_corrupt_label_fst(&indexes, pod);
    let symbols = paged_symbol_reader(&symbols);
    let mut metadata = MetadataAccumulator::default();

    collect_metric_names_from_index(&symbols, &mut index_reader, 0, 10_000, &mut metadata).unwrap();

    assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
}

#[test]
fn label_value_index_collection_reads_only_requested_label_values() {
    let mut symbols = SegmentSymbols::default();
    let metric = symbols.intern(METRIC_NAME_LABEL);
    let backend = symbols.intern("backend-1");
    let cpu = symbols.intern("cpu_usage");
    let pod = symbols.intern("pod_name");
    let series = vec![SeriesEntry {
        series_id: 1,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(metric, cpu), (pod, backend)],
    }];
    let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values,
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };
    let mut index_reader = index_reader_with_corrupt_label_fst(&indexes, metric);
    let symbols = paged_symbol_reader(&symbols);
    let mut metadata = MetadataAccumulator::default();

    collect_label_values_from_index(
        &symbols,
        &mut index_reader,
        "pod_name",
        0,
        10_000,
        &mut metadata,
    )
    .unwrap();

    assert_eq!(
        metadata.label_values("pod_name"),
        vec!["backend-1".to_string()]
    );
}

#[test]
fn query_context_series_entry_reads_preserve_cardinality_and_reject_missing_refs() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    for (series_ref, instance, value) in [
        (SeriesRef::new(1), "first", 1.5),
        (SeriesRef::new(2), "second", 2.5),
    ] {
        writer
            .record_samples_ordered_with_label_visitor(series_ref, &[(1_000, value)], |visit| {
                visit(METRIC_NAME_LABEL, "entry_cardinality");
                visit("instance", instance);
            })
            .unwrap();
    }
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = open_schema6_segment_for_test(seg_dir).unwrap();
    let mut context = SegmentQueryContext::open(&reader).unwrap();

    let invalid_refs = [0, u32::MAX];
    let error = context
        .read_series_entries(&reader, &invalid_refs)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&u32::MAX.to_string()));
    assert!(reader.query_cache.series_entries.lock().unwrap().is_empty());

    let error = context
        .read_series_entries_uncached(&reader, &invalid_refs)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&u32::MAX.to_string()));
    assert!(reader.query_cache.series_entries.lock().unwrap().is_empty());

    let error = context
        .read_series_metadata_entries(&reader, &invalid_refs)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&u32::MAX.to_string()));
    assert!(
        reader
            .query_cache
            .series_metadata
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(
        reader
            .query_cache
            .series_locators
            .lock()
            .unwrap()
            .is_empty()
    );

    let ordered_refs = [1, 0];
    for actual in [
        context
            .read_series_entries(&reader, &ordered_refs)
            .unwrap()
            .into_iter()
            .map(|(series_ref, _)| series_ref)
            .collect::<Vec<_>>(),
        context
            .read_series_entries_uncached(&reader, &ordered_refs)
            .unwrap()
            .into_iter()
            .map(|(series_ref, _)| series_ref)
            .collect::<Vec<_>>(),
        context
            .read_series_metadata_entries(&reader, &ordered_refs)
            .unwrap()
            .into_iter()
            .map(|(series_ref, _)| series_ref)
            .collect::<Vec<_>>(),
    ] {
        assert_eq!(actual, ordered_refs);
    }

    let duplicate_refs = [0, 0];
    for error in [
        context
            .read_series_entries(&reader, &duplicate_refs)
            .unwrap_err(),
        context
            .read_series_entries_uncached(&reader, &duplicate_refs)
            .unwrap_err(),
        context
            .read_series_metadata_entries(&reader, &duplicate_refs)
            .unwrap_err(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("series ref 0"));
    }
}
