use std::time::Duration;

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
    SeriesRef,
};
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{
    FloatEncoding, HeadBuffer, HeadConfig, HistogramValue, IntEncoding, SampleValue,
    TypedSampleMetadata,
};
use chronoxide_core::storage::segment::{
    LabelMatcher, SegmentSelector, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
};

fn open_default_store(path: impl AsRef<std::path::Path>) -> SegmentStoreReader {
    SegmentStoreReader::open(path).unwrap()
}

fn labels(
    store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    values: &[(&str, &str)],
) -> SeriesRef {
    let refs: Vec<_> = values.iter().copied().map(KeyValueRef::from).collect();
    store.intern(&refs).unwrap()
}

fn test_head() -> HeadBuffer {
    HeadBuffer::new(HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap()
}

#[test]
fn head_query_normalizes_metric_shorthand_and_label_matchers() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series_1 = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("namespace", "default"),
            ("pod.name", "backend-1"),
        ],
    );
    let series_2 = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("namespace", "default"),
            ("pod.name", "backend-2"),
        ],
    );

    let mut head = test_head();
    head.record_sample(series_1, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series_2, 6_000, SampleValue::Float(2.0))
        .unwrap();

    let metric_results = head
        .query_selector(
            &label_store,
            &SegmentSelector::metric("cpu.usage"),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(metric_results.len(), 2);

    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![
            LabelMatcher::eq("namespace", "default"),
            LabelMatcher::eq("pod.name", "backend-1"),
        ],
    );
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
    assert!(results[0].labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == normalize_metric_name("cpu.usage")
    }));
    assert!(
        results[0].labels.iter().any(|(key, value)| {
            key == normalize_label_name("pod.name") && value == "backend-1"
        })
    );
}

#[test]
fn head_query_not_equal_excludes_matching_value_and_includes_missing_labels() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend_1 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let backend_2 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-2")],
    );
    let missing_pod = labels(&mut label_store, &[(METRIC_NAME_LABEL, "cpu.usage")]);

    let mut head = test_head();
    head.record_sample(backend_1, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(backend_2, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(missing_pod, 5_000, SampleValue::Float(3.0))
        .unwrap();

    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![LabelMatcher::not_eq("pod.name", "backend-1")],
    );
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn head_query_converts_integer_number_samples_to_promql_f64() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "request.count"), ("route", "/api")],
    );

    let mut head = test_head();
    head.record_sample(series, 5_000, SampleValue::Int64(42))
        .unwrap();

    let selector =
        SegmentSelector::with_metric("request.count", vec![LabelMatcher::eq("route", "/api")]);
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 42.0)]);
}

#[test]
fn head_query_supports_regex_matchers() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let frontend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "frontend-1")],
    );

    let mut head = test_head();
    head.record_sample(backend, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(frontend, 5_000, SampleValue::Float(2.0))
        .unwrap();

    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![LabelMatcher::regex("pod.name", "backend-.*")],
    );
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn head_query_supports_negative_regex_and_includes_missing_labels() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let frontend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "frontend-1")],
    );
    let missing_pod = labels(&mut label_store, &[(METRIC_NAME_LABEL, "cpu.usage")]);

    let mut head = test_head();
    head.record_sample(backend, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(frontend, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(missing_pod, 5_000, SampleValue::Float(3.0))
        .unwrap();

    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![LabelMatcher::not_regex("pod.name", "backend-.*")],
    );
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn head_query_matchers_treat_absent_labels_as_empty_strings() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let explicit_empty = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("env", ""),
            ("shard", "a"),
        ],
    );
    let missing = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("shard", "a")],
    );
    let nonempty = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("env", "prod"),
            ("shard", "b"),
        ],
    );

    let mut head = test_head();
    head.record_sample(explicit_empty, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(missing, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(nonempty, 5_000, SampleValue::Float(3.0))
        .unwrap();

    for (matchers, expected) in [
        (vec![LabelMatcher::eq("env", "")], vec![1.0, 2.0]),
        (vec![LabelMatcher::not_eq("env", "")], vec![3.0]),
        (
            vec![LabelMatcher::regex("env", "prod|")],
            vec![1.0, 2.0, 3.0],
        ),
        (vec![LabelMatcher::not_regex("env", "prod|")], Vec::new()),
        (
            vec![LabelMatcher::eq("shard", "a"), LabelMatcher::eq("env", "")],
            vec![1.0, 2.0],
        ),
    ] {
        let selector = SegmentSelector::with_metric("cpu.usage", matchers);
        let results = head
            .query_selector(&label_store, &selector, 0, 10_000)
            .unwrap();
        let mut values = results
            .iter()
            .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
            .collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        assert_eq!(values, expected);
    }
}

#[test]
fn head_promql_native_histogram_matcher_treats_absent_label_as_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let explicit_empty = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "head_native_missing"), ("env", "")],
    );
    let missing = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "head_native_missing")],
    );
    let nonempty = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "head_native_missing"), ("env", "prod")],
    );

    let histogram = |count| {
        SampleValue::Histogram(HistogramValue {
            count,
            sum: Some(count as f64),
            min: None,
            max: None,
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0],
            bucket_counts: vec![count, 0],
        })
    };
    let mut head = test_head();
    head.record_sample(explicit_empty, 5_000, histogram(6))
        .unwrap();
    head.record_sample(missing, 5_000, histogram(7)).unwrap();
    head.record_sample(nonempty, 5_000, histogram(8)).unwrap();

    let store = open_default_store(tempdir.path());
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"histogram_count(head_native_missing{env=""})"#,
            0,
            10_000,
        )
        .unwrap();
    let mut values = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![6.0, 7.0]);
}

#[test]
fn store_query_selector_with_head_merges_sealed_and_active_head_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "cpu.usage"),
            ("namespace", "default"),
            ("pod.name", "backend-1"),
        ],
    );

    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(5_000, 1.0)])
        .unwrap();
    writer.flush().unwrap();

    let mut head = test_head();
    head.record_sample(series, 15_000, SampleValue::Float(2.0))
        .unwrap();

    let store = open_default_store(tempdir.path());
    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = store
        .query_selector_with_head(&head, &label_store, &selector, 0, 20_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0), (15_000, 2.0)]);
}

#[test]
fn store_query_selector_with_head_merges_ooo_head_samples_before_flush() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );

    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(5_000, 1.0)])
        .unwrap();
    writer.flush().unwrap();

    let mut head = HeadBuffer::new(
        HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_out_of_order_time_window(Duration::from_secs(6)),
    )
    .unwrap();
    head.record_sample(series, 15_000, SampleValue::Float(3.0))
        .unwrap();
    head.record_sample(series, 9_500, SampleValue::Float(2.0))
        .unwrap();

    let store = open_default_store(tempdir.path());
    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = store
        .query_selector_with_head(&head, &label_store, &selector, 0, 20_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].samples,
        vec![(5_000, 1.0), (9_500, 2.0), (15_000, 3.0)]
    );
}

#[test]
fn store_query_selector_with_head_prefers_head_value_for_duplicate_timestamp() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );

    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(15_000, 1.0)])
        .unwrap();
    writer.flush().unwrap();

    let mut head = test_head();
    head.record_sample(series, 15_000, SampleValue::Float(9.0))
        .unwrap();

    let store = open_default_store(tempdir.path());
    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = store
        .query_selector_with_head(&head, &label_store, &selector, 10_000, 20_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(15_000, 9.0)]);
}

#[test]
fn store_query_selector_with_head_prefers_late_ooo_head_duplicate() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );

    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(4_000, 1.0)])
        .unwrap();
    writer.flush().unwrap();

    let mut head = HeadBuffer::new(
        HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_out_of_order_time_window(Duration::from_secs(2)),
    )
    .unwrap();
    head.record_sample(series, 4_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(series, 5_000, SampleValue::Float(3.0))
        .unwrap();
    head.record_sample(series, 4_000, SampleValue::Float(4.0))
        .unwrap();

    let store = open_default_store(tempdir.path());
    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = store
        .query_selector_with_head(&head, &label_store, &selector, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(4_000, 4.0), (5_000, 3.0)]);
}
