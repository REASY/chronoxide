use std::time::Duration;

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
    SeriesRef,
};
use chronoxide_core::promql::PromqlQueryError;
use chronoxide_core::storage::head::{
    FloatEncoding, HeadBuffer, HeadConfig, IntEncoding, SampleValue,
};
use chronoxide_core::storage::segment::{
    QueryLimits, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
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

fn assert_limit_exceeded(err: PromqlQueryError, expected_limit: &str, expected_max: u64) {
    match err {
        PromqlQueryError::LimitExceeded { limit, max } => {
            assert_eq!(limit, expected_limit);
            assert_eq!(max, expected_max);
        }
        other => panic!("expected limit exceeded error, got {other:?}"),
    }
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
fn promql_query_supports_positive_regex_matchers() {
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
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(3),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "frontend-1".to_string()),
        ],
        &[(5_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu.usage{pod.name=~"backend-[12]"}"#, 0, 10_000)
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![1.0, 2.0]);
}

#[test]
fn promql_query_supports_negative_regex_and_includes_missing_labels() {
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
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "frontend-1".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(3),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"cpu.usage{pod.name!~"backend-.*"}"#, 0, 10_000)
        .unwrap();
    let mut values: Vec<f64> = results
        .iter()
        .flat_map(|result| result.samples.iter().map(|(_, value)| *value))
        .collect();
    values.sort_by(f64::total_cmp);

    assert_eq!(values, vec![2.0, 3.0]);
}

#[test]
fn promql_query_combines_equality_and_regex_matchers() {
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
            ("namespace".to_string(), "other".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"cpu.usage{namespace="default",pod.name=~"backend-.*"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_supports_metric_name_regex_matcher() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![(METRIC_NAME_LABEL.to_string(), "memory.usage".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"{__name__=~"cpu_.*"}"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_returns_invalid_for_bad_regex() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let err = store
        .query_promql(r#"cpu.usage{pod.name=~"["}"#, 0, 10_000)
        .unwrap_err();

    assert!(matches!(err, PromqlQueryError::Invalid(_)));
}

#[test]
fn promql_query_supports_active_head_regex() {
    let tempdir = tempfile::tempdir().unwrap();
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

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~"backend-.*"}"#,
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_with_limits_returns_stats_for_successful_sealed_query() {
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
        &[(5_000, 1.0), (6_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].samples,
        vec![(5_000, 1.0), (6_000, 2.0)]
    );
    assert_eq!(execution.stats.matched_series, 1);
    assert_eq!(execution.stats.chunk_reads, 1);
    assert!(execution.stats.bytes_read > 0);
    assert_eq!(execution.stats.samples_decoded, 2);
    assert_eq!(execution.stats.regex_values_examined, 0);
}

#[test]
fn promql_query_limit_rejects_too_many_matched_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_matched_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "matched_series", 1);
}

#[test]
fn promql_query_limit_rejects_too_many_chunk_reads() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_chunk_reads: Some(0),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "chunk_reads", 0);
}

#[test]
fn promql_query_limit_rejects_too_many_bytes_read() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_bytes_read: Some(0),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "bytes_read", 0);
}

#[test]
fn promql_query_limit_rejects_too_many_samples_decoded() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 1.0), (6_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            "cpu.usage",
            0,
            10_000,
            QueryLimits {
                max_samples_decoded: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "samples_decoded", 1);
}

#[test]
fn promql_query_limit_rejects_too_many_regex_values_examined() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for (idx, pod) in ["backend-1", "backend-2", "frontend-1"]
        .into_iter()
        .enumerate()
    {
        write_series(
            &mut writer,
            SeriesRef::new(idx as u32 + 1),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), pod.to_string()),
            ],
            &[(5_000, idx as f64)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name=~".*"}"#,
            0,
            10_000,
            QueryLimits {
                max_regex_values_examined: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "regex_values_examined", 2);
}

#[test]
fn promql_query_with_head_limits_count_head_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let mut head = test_head();
    head.record_sample(series, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series, 6_000, SampleValue::Float(2.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits {
                max_samples_decoded: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "samples_decoded", 1);
}

#[test]
fn promql_query_with_head_limits_regex_values_examined() {
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
    let frontend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "frontend-1")],
    );
    let mut head = test_head();
    head.record_sample(backend_1, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(backend_2, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(frontend, 5_000, SampleValue::Float(3.0))
        .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let err = store
        .query_promql_with_head_with_limits(
            &head,
            &label_store,
            r#"cpu.usage{pod.name=~".*"}"#,
            0,
            10_000,
            QueryLimits {
                max_regex_values_examined: Some(2),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();

    assert_limit_exceeded(err, "regex_values_examined", 2);
}
