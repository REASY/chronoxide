use std::fs;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::{METRIC_NAME_LABEL, normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::segment::{
    LabelMatcher, SegmentReader, SegmentSelector, SegmentStoreReader, SegmentWriter,
    SegmentWriterConfig,
};

#[test]
fn segment_reader_queries_exact_matchers_and_returns_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let labels_1 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let labels_2 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-2".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels_1, &[(5_000, 1.0), (6_000, 1.5)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(2), &labels_2, &[(5_000, 2.0)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(seg_dir).unwrap();

    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = reader
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "backend-1"),
            ],
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0), (6_000, 1.5)]);
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| { key == METRIC_NAME_LABEL && value == metric.as_str() })
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| { key == pod_label.as_str() && value == "backend-1" })
    );
}

#[test]
fn segment_store_reader_queries_and_merges_multiple_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let other_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-2".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(15_000, 1.5)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(2), &other_labels, &[(15_000, 2.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = store
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "backend-1"),
            ],
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0), (15_000, 1.5)]);
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| { key == pod_label.as_str() && value == "backend-1" })
    );
}

#[test]
fn selector_query_normalizes_metric_shorthand_and_label_matchers() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let labels_1 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let labels_2 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-2".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels_1, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(2), &labels_2, &[(5_000, 2.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let metric_results = store
        .query_selector(&SegmentSelector::metric("cpu.usage"), 0, 10_000)
        .unwrap();
    assert_eq!(metric_results.len(), 2);

    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![
            LabelMatcher::eq("namespace", "default"),
            LabelMatcher::eq("pod.name", "backend-1"),
        ],
    );
    let results = store.query_selector(&selector, 0, 10_000).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn selector_not_equal_excludes_matching_value_and_includes_missing_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let backend_1 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let backend_2 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-2".to_string()),
    ];
    let missing_pod = vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())];

    writer
        .record_samples_with_labels(SeriesRef::new(1), &backend_1, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(2), &backend_2, &[(5_000, 2.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(3), &missing_pod, &[(5_000, 3.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![LabelMatcher::not_eq("pod.name", "backend-1")],
    );
    let results = store.query_selector(&selector, 0, 10_000).unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn selector_query_merges_matches_across_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(15_000, 1.5)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = store.query_selector(&selector, 0, 20_000).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0), (15_000, 1.5)]);
}
