use std::fs;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::{METRIC_NAME_LABEL, normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::segment::{SegmentReader, SegmentWriter, SegmentWriterConfig};

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
