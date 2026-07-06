use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::{METRIC_NAME_LABEL, normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, append_retention_tombstones,
    read_manifest_inventory, write_current,
};
use chronoxide_core::storage::segment::{
    LabelMatcher, SegmentFile, SegmentId, SegmentReader, SegmentSelector,
    SegmentSeriesMetadataBuilder, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
};

fn segment_readers(segments_dir: &Path) -> Vec<SegmentReader> {
    let mut readers: Vec<_> = fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| SegmentReader::open(entry.path()).unwrap())
        .collect();
    readers.sort_by(|left, right| {
        left.meta()
            .start_ms
            .cmp(&right.meta().start_ms)
            .then_with(|| left.meta().end_ms.cmp(&right.meta().end_ms))
            .then_with(|| left.meta().segment_id.cmp(&right.meta().segment_id))
    });
    readers
}

fn publish_manifest_segments(manifest_dir: &Path, readers: &[&SegmentReader]) {
    let mut writer = ManifestWriter::create(manifest_dir, 1).unwrap();
    for (idx, reader) in readers.iter().enumerate() {
        let meta = reader.meta();
        writer
            .append(&ManifestRecord::SegmentSealed(
                ManifestSegment::new(
                    meta.segment_id.clone(),
                    meta.start_ms,
                    meta.end_ms,
                    Some(100 + idx as u64),
                )
                .unwrap(),
            ))
            .unwrap();
    }
    writer.sync_all().unwrap();
    write_current(manifest_dir, writer.file_name()).unwrap();
}

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
fn segment_reader_queries_samples_recorded_with_prebuilt_metadata() {
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
    let mut metadata = SegmentSeriesMetadataBuilder::new();
    for (key, value) in &labels {
        metadata.push_label(key, value);
    }
    let metadata = metadata.finish();

    writer
        .record_samples_with_metadata(SeriesRef::new(1), &metadata, &[(5_000, 1.0)])
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
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn segment_reader_queries_samples_recorded_with_direct_label_visitor() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let labels = [
        (METRIC_NAME_LABEL, "cpu.usage"),
        ("namespace", "default"),
        ("pod.name", "backend-1"),
    ];

    writer
        .record_samples_with_label_visitor(SeriesRef::new(1), &[(5_000, 1.0)], |visit| {
            for (key, value) in labels {
                visit(key, value);
            }
        })
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
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn segment_reader_queries_series_when_labels_arrive_after_unlabeled_sample() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer.record_sample(SeriesRef::new(1), 4_000, 0.5).unwrap();
    writer
        .record_samples_with_label_visitor(SeriesRef::new(1), &[(5_000, 1.0)], |visit| {
            visit(METRIC_NAME_LABEL, "cpu.usage");
            visit("pod.name", "backend-1");
        })
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
    assert_eq!(results[0].samples, vec![(4_000, 0.5), (5_000, 1.0)]);
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
fn manifest_published_segment_store_ignores_orphan_segment_directories() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let published_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "published".to_string()),
    ];
    let orphan_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "orphan".to_string()),
    ];
    writer
        .record_samples_with_labels(SeriesRef::new(1), &published_labels, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(2), &orphan_labels, &[(15_000, 2.0)])
        .unwrap();
    writer.flush().unwrap();

    let readers = segment_readers(tempdir.path());
    assert_eq!(readers.len(), 2);
    publish_manifest_segments(&manifest_dir, &[&readers[0]]);

    let store = SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir).unwrap();
    let results = store
        .query_selector(&SegmentSelector::metric("cpu.usage"), 0, 20_000)
        .unwrap();
    let values: Vec<_> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();

    assert_eq!(values, vec![1.0]);
}

#[test]
fn manifest_retention_tombstones_hide_expired_segments_without_deleting_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");
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

    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(15_000, 2.0)])
        .unwrap();
    writer.flush().unwrap();
    let readers = segment_readers(tempdir.path());
    assert_eq!(readers.len(), 2);
    publish_manifest_segments(&manifest_dir, &[&readers[0], &readers[1]]);

    let inventory = read_manifest_inventory(&manifest_dir)
        .unwrap()
        .expect("inventory");
    let mut manifest_writer = ManifestWriter::open_append(&manifest_dir, "MANIFEST-000001")
        .expect("open manifest append");
    let report = append_retention_tombstones(&mut manifest_writer, &inventory, 10_000).unwrap();
    manifest_writer.sync_all().unwrap();

    assert_eq!(
        report.tombstoned_segments,
        vec![readers[0].meta().segment_id.clone()]
    );
    assert!(readers[0].file_path(SegmentFile::MetaJson).exists());
    assert!(readers[1].file_path(SegmentFile::MetaJson).exists());

    let store = SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir).unwrap();
    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = store.query_selector(&selector, 0, 20_000).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(15_000, 2.0)]);
}

#[test]
fn manifest_published_segment_store_returns_empty_without_current() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer.flush().unwrap();
    fs::remove_dir_all(&manifest_dir).unwrap();
    fs::create_dir_all(&manifest_dir).unwrap();

    let store = SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir).unwrap();
    let results = store
        .query_selector(&SegmentSelector::metric("cpu.usage"), 0, 10_000)
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn manifest_published_segment_store_errors_when_published_segment_is_missing() {
    let tempdir = tempfile::tempdir().unwrap();
    let manifest_dir = tempdir.path().join("manifest");
    let missing_id = SegmentId::new(0, 10_000).unwrap();
    let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();
    writer
        .append(&ManifestRecord::SegmentSealed(
            ManifestSegment::new(missing_id.dir_name(), 0, 10_000, Some(100)).unwrap(),
        ))
        .unwrap();
    writer.sync_all().unwrap();
    write_current(&manifest_dir, writer.file_name()).unwrap();

    let err = match SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir) {
        Ok(_) => panic!("expected missing published segment to fail"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), io::ErrorKind::NotFound);
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
