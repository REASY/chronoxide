use std::time::Duration;

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
    SeriesRef,
};
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{
    FloatEncoding, HeadBuffer, HeadConfig, IntEncoding, SampleValue,
};
use chronoxide_core::storage::segment::{
    LabelMatcher, SegmentSelector, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
};

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
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.usage")
    }));
    assert!(
        results[0].labels.iter().any(|(key, value)| {
            key == &normalize_label_name("pod.name") && value == "backend-1"
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

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
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

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
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

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
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

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = store
        .query_selector_with_head(&head, &label_store, &selector, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(4_000, 4.0), (5_000, 3.0)]);
}
