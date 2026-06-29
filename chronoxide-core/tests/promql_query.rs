use std::time::Duration;

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
    SeriesRef,
};
use chronoxide_core::promql::PromqlQueryError;
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

#[test]
fn promql_query_merges_sealed_segments_and_active_head() {
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
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0), (15_000, 2.0)]);
}

#[test]
fn promql_query_reads_sealed_segments_without_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(7);
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

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu.usage{pod.name="backend-1"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_supports_brace_only_metric_name_and_inequality() {
    let tempdir = tempfile::tempdir().unwrap();
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

    let raw_backend_1 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];
    let raw_backend_2 = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("pod.name".to_string(), "backend-2".to_string()),
    ];
    let raw_missing_pod = vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())];

    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(backend_1, &raw_backend_1, &[(5_000, 1.0)])
        .unwrap();
    writer
        .record_samples_with_labels(backend_2, &raw_backend_2, &[(5_000, 2.0)])
        .unwrap();
    writer
        .record_samples_with_labels(missing_pod, &raw_missing_pod, &[(5_000, 3.0)])
        .unwrap();
    writer.flush().unwrap();

    let head = test_head();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"{__name__="cpu.usage",pod.name!="backend-1"}"#,
            0,
            10_000,
        )
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn promql_query_returns_unsupported_for_regex_without_scanning() {
    let tempdir = tempfile::tempdir().unwrap();
    let head = test_head();
    let label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let err = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~"backend-.*"}"#,
            0,
            10_000,
        )
        .unwrap_err();

    assert_eq!(
        err,
        PromqlQueryError::Unsupported("regex matchers are not implemented".to_string())
    );
}
