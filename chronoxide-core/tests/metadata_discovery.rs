use std::time::Duration;

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
    SeriesRef,
};
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{
    FloatEncoding, HeadBuffer, HeadConfig, IntEncoding, SampleValue,
};
use chronoxide_core::storage::segment::{SegmentStoreReader, SegmentWriter, SegmentWriterConfig};

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

fn write_series(
    writer: &mut SegmentWriter,
    series: SeriesRef,
    labels: Vec<(String, String)>,
    samples: &[(u64, f64)],
) {
    writer
        .record_samples_with_labels(series, &labels, samples)
        .unwrap();
}

#[test]
fn segment_store_discovers_metric_names_label_names_and_label_values_by_time_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("namespace".to_string(), "default".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("namespace".to_string(), "default".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(15_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(3),
        vec![
            (METRIC_NAME_LABEL.to_string(), "memory.usage".to_string()),
            ("namespace".to_string(), "infra".to_string()),
            ("pod.name".to_string(), "frontend-1".to_string()),
        ],
        &[(15_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    assert_eq!(
        store.metric_names(0, 20_000).unwrap(),
        vec![
            normalize_metric_name("cpu.usage"),
            normalize_metric_name("memory.usage")
        ]
    );
    assert_eq!(
        store.metric_names(0, 10_000).unwrap(),
        vec![normalize_metric_name("cpu.usage")]
    );

    assert_eq!(
        store.label_names(0, 20_000).unwrap(),
        vec![
            METRIC_NAME_LABEL.to_string(),
            normalize_label_name("namespace"),
            normalize_label_name("pod.name"),
        ]
    );
    assert_eq!(
        store.label_values("pod.name", 0, 20_000).unwrap(),
        vec![
            "backend-1".to_string(),
            "backend-2".to_string(),
            "frontend-1".to_string()
        ]
    );
    assert_eq!(
        store.label_values("pod.name", 0, 10_000).unwrap(),
        vec!["backend-1".to_string()]
    );
    assert_eq!(
        store.label_values(METRIC_NAME_LABEL, 0, 20_000).unwrap(),
        vec![
            normalize_metric_name("cpu.usage"),
            normalize_metric_name("memory.usage")
        ]
    );
}

#[test]
fn segment_store_discovers_metadata_with_active_head_overlay() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-1".to_string()),
        ],
        &[(5_000, 1.0)],
    );
    writer.flush().unwrap();

    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let head_cpu = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-2")],
    );
    let head_memory = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "memory.usage"), ("namespace", "infra")],
    );
    let mut head = test_head();
    head.record_sample(head_cpu, 15_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(head_memory, 15_000, SampleValue::Float(3.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    assert_eq!(
        store
            .metric_names_with_head(&head, &label_store, 0, 20_000)
            .unwrap(),
        vec![
            normalize_metric_name("cpu.usage"),
            normalize_metric_name("memory.usage")
        ]
    );
    assert_eq!(
        store
            .label_names_with_head(&head, &label_store, 0, 20_000)
            .unwrap(),
        vec![
            METRIC_NAME_LABEL.to_string(),
            normalize_label_name("namespace"),
            normalize_label_name("pod.name"),
        ]
    );
    assert_eq!(
        store
            .label_values_with_head("pod.name", &head, &label_store, 0, 20_000)
            .unwrap(),
        vec!["backend-1".to_string(), "backend-2".to_string()]
    );
}
