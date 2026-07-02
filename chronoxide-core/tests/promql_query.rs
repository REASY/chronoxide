use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
    SeriesRef,
};
use chronoxide_core::promql::PromqlQueryError;
use chronoxide_core::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, FloatEncoding,
    HeadBuffer, HeadConfig, HistogramValue, IntEncoding, OTLP_FLAG_NO_RECORDED_VALUE,
    OtlpAggregationTemporality, SampleValue, SummaryQuantileValue, SummaryValue,
    TypedSampleMetadata, prometheus_stale_nan,
};
use chronoxide_core::storage::segment::{
    QueryLimits, QueryProjectionConfig, SegmentFile, SegmentStoreReader, SegmentWriter,
    SegmentWriterConfig,
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

fn segment_dir_with_start(root: &Path, start_ms: u64) -> PathBuf {
    let prefix = format!("seg-{start_ms}-");
    fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_dir())
                && entry.file_name().to_string_lossy().starts_with(&prefix)
        })
        .unwrap_or_else(|| panic!("segment starting at {start_ms} not found"))
        .path()
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
fn promql_query_increase_evaluates_counter_range_from_sealed_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(71);
    let raw_labels = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            "http.requests.total".to_string(),
        ),
        ("route".to_string(), "/api".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            series,
            &raw_labels,
            &[(1_000, 10.0), (3_000, 15.0), (5_000, 2.0), (6_000, 6.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.requests.total{route="/api"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 11.0)]);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL)
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == "route" && value == "/api")
    );
}

#[test]
fn promql_query_rate_evaluates_counter_range_with_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.requests.total"),
            ("route", "/head"),
        ],
    );
    let mut head = test_head();
    for (ts, value) in [(1_000, 1.0), (3_000, 5.0), (6_000, 11.0)] {
        head.record_sample(series, ts, SampleValue::Float(value))
            .unwrap();
    }

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rate(http.requests.total{route="/head"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
}

#[test]
fn promql_query_histogram_quantile_evaluates_bucket_rate() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(72),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, rate(http.request.duration_bucket{route="/quantile"}[5s]))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert!(
        !results[0]
            .labels
            .iter()
            .any(|(key, _)| key == METRIC_NAME_LABEL || key == "le")
    );
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == "route" && value == "/quantile")
    );
}

#[test]
fn promql_query_increase_uses_histogram_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(73),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(10.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![10, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 12,
                        sum: Some(12.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![12, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.reset");
                visit("route", "/hist-reset");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.request.reset_count{route="/hist-reset"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 12.0)]);
}

#[test]
fn promql_query_increase_uses_histogram_bucket_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(74),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![10, 10, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::CounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0],
                        bucket_counts: vec![12, 8, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.bucket.reset");
                visit("route", "/hist-bucket-reset");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"increase(http.request.bucket.reset_bucket{route="/hist-bucket-reset", le="1"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 12.0)]);
}

#[test]
fn promql_query_rate_uses_active_head_exponential_histogram_counter_reset_hint() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.reset.size"),
            ("route", "/exphist-reset"),
        ],
    );
    let mut head = test_head();

    for (ts, count, reset_hint) in [
        (1_000, 20, CounterResetHint::NotCounterReset),
        (6_000, 25, CounterResetHint::CounterReset),
    ] {
        head.record_sample(
            series,
            ts,
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count,
                sum: Some(count as f64),
                min: None,
                max: None,
                metadata: TypedSampleMetadata {
                    reset_hint,
                    ..TypedSampleMetadata::default()
                },
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: vec![count],
                },
                negative: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: Vec::new(),
                },
            }),
        )
        .unwrap();
    }

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rate(http.request.reset.size_count{route="/exphist-reset"}[5s])"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 5.0)]);
}

#[test]
fn promql_query_projects_classic_histogram_from_native_segment_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(31);
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            series,
            &[(
                5_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let bucket = store
        .query_promql(r#"http.request.duration_bucket{le="5"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(5_000, 3.0)]);
    assert!(
        bucket[0]
            .labels
            .iter()
            .any(|(key, value)| key == "le" && value == "5")
    );

    let inf_bucket = store
        .query_promql(r#"http.request.duration_bucket{le="+Inf"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(inf_bucket.len(), 1);
    assert_eq!(inf_bucket[0].samples, vec![(5_000, 4.0)]);

    let count = store
        .query_promql(r#"http.request.duration_count{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(5_000, 4.0)]);

    let sum = store
        .query_promql(r#"http.request.duration_sum{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(5_000, 10.0)]);
}

#[test]
fn promql_query_projects_stale_histogram_sample_as_stale_nan() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(33),
            &[(
                5_000,
                HistogramValue {
                    count: 0,
                    sum: Some(0.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(1_000),
                        flags: OTLP_FLAG_NO_RECORDED_VALUE,
                        temporality: OtlpAggregationTemporality::Cumulative,
                        reset_hint: CounterResetHint::Unknown,
                    },
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![0, 0],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/stale");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql(r#"http.request.duration_count{route="/stale"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples.len(), 1);
    assert_eq!(count[0].samples[0].0, 5_000);
    assert_eq!(
        count[0].samples[0].1.to_bits(),
        prometheus_stale_nan().to_bits()
    );

    let bucket = store
        .query_promql(
            r#"http.request.duration_bucket{route="/stale", le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples.len(), 1);
    assert_eq!(bucket[0].samples[0].0, 5_000);
    assert_eq!(
        bucket[0].samples[0].1.to_bits(),
        prometheus_stale_nan().to_bits()
    );
}

#[test]
fn promql_query_projects_delta_histogram_as_cumulative_virtual_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let metadata = TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(34),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(5.0),
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                ),
                (
                    2_000,
                    HistogramValue {
                        count: 3,
                        sum: Some(7.0),
                        min: None,
                        max: None,
                        metadata,
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 1],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.duration");
                visit("route", "/delta");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store
        .query_promql(r#"http.request.duration_count{route="/delta"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(1_000, 2.0), (2_000, 5.0)]);

    let sum = store
        .query_promql(r#"http.request.duration_sum{route="/delta"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(1_000, 5.0), (2_000, 12.0)]);

    let bucket = store
        .query_promql(
            r#"http.request.duration_bucket{route="/delta", le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(1_000, 2.0), (2_000, 5.0)]);
}

#[test]
fn promql_query_projects_exponential_histogram_bucket_from_native_segment_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(35),
            &[(
                5_000,
                ExponentialHistogramValue {
                    count: 5,
                    sum: Some(12.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![2, 3],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.size");
                visit("route", "/exphist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let default_store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let default_bucket = default_store
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert!(default_bucket.is_empty());

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default()
            .with_exponential_histogram_bucket_boundaries(vec![2.0, 4.0]),
    )
    .unwrap();
    let bucket = store
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(5_000, 2.0)]);
    assert!(
        bucket[0]
            .labels
            .iter()
            .any(|(key, value)| key == "le" && value == "2")
    );
    let mut session = store.query_session().unwrap();
    let session_bucket = session
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(session_bucket, bucket);

    let inf_bucket = store
        .query_promql(
            r#"http.request.size_bucket{route="/exphist", le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(inf_bucket.len(), 1);
    assert_eq!(inf_bucket[0].samples, vec![(5_000, 5.0)]);

    let all_buckets = store
        .query_promql(r#"http.request.size_bucket{route="/exphist"}"#, 0, 10_000)
        .unwrap();
    let mut bucket_labels: Vec<_> = all_buckets
        .iter()
        .map(|result| {
            result
                .labels
                .iter()
                .find_map(|(key, value)| (key == "le").then_some(value.as_str()))
                .unwrap()
        })
        .collect();
    bucket_labels.sort_unstable();
    assert_eq!(bucket_labels, vec!["+Inf", "2", "4"]);
}

#[test]
fn promql_query_projects_delta_exponential_histogram_bucket_from_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.size"),
            ("route", "/delta-exphist"),
        ],
    );
    let mut head = test_head();
    let metadata = TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    for (ts, counts) in [(1_000, vec![1, 1]), (2_000, vec![2, 1])] {
        head.record_sample(
            series,
            ts,
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count: counts.iter().sum(),
                sum: None,
                min: None,
                max: None,
                metadata,
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: ExponentialHistogramBuckets { offset: 0, counts },
                negative: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: Vec::new(),
                },
            }),
        )
        .unwrap();
    }

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();
    let bucket = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"http.request.size_bucket{route="/delta-exphist", le="2"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(1_000, 1.0), (2_000, 3.0)]);
}

#[test]
fn promql_query_projects_summary_from_native_segment_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(32);
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_summary_samples_ordered_with_label_visitor(
            series,
            &[(
                5_000,
                SummaryValue {
                    count: 10,
                    sum: 50.0,
                    metadata: TypedSampleMetadata::default(),
                    quantiles: vec![
                        SummaryQuantileValue {
                            quantile: 0.5,
                            value: 4.0,
                        },
                        SummaryQuantileValue {
                            quantile: 0.9,
                            value: 8.0,
                        },
                    ],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "rpc.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let quantile = store
        .query_promql(r#"rpc.duration{quantile="0.9"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(quantile.len(), 1);
    assert_eq!(quantile[0].samples, vec![(5_000, 8.0)]);
    assert!(
        quantile[0]
            .labels
            .iter()
            .any(|(key, value)| key == "quantile" && value == "0.9")
    );

    let count = store
        .query_promql(r#"rpc.duration_count{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(5_000, 10.0)]);

    let sum = store
        .query_promql(r#"rpc.duration_sum{route="/typed"}"#, 0, 10_000)
        .unwrap();
    assert_eq!(sum.len(), 1);
    assert_eq!(sum[0].samples, vec![(5_000, 50.0)]);
}

#[test]
fn promql_query_projects_typed_samples_from_active_head() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let histogram_series = labels(
        &mut label_store,
        &[
            (METRIC_NAME_LABEL, "http.request.duration"),
            ("route", "/typed"),
        ],
    );
    let summary_series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "rpc.duration"), ("route", "/typed")],
    );
    let mut head = test_head();
    head.record_sample(
        histogram_series,
        5_000,
        SampleValue::Histogram(HistogramValue {
            count: 4,
            sum: Some(10.0),
            min: Some(1.0),
            max: Some(4.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0],
            bucket_counts: vec![1, 2, 1],
        }),
    )
    .unwrap();
    head.record_sample(
        summary_series,
        5_000,
        SampleValue::Summary(SummaryValue {
            count: 10,
            sum: 50.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![SummaryQuantileValue {
                quantile: 0.9,
                value: 8.0,
            }],
        }),
    )
    .unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let bucket = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"http.request.duration_bucket{le="+Inf"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].samples, vec![(5_000, 4.0)]);

    let quantile = store
        .query_promql_with_head(
            &head,
            &label_store,
            r#"rpc.duration{quantile="0.9"}"#,
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(quantile.len(), 1);
    assert_eq!(quantile[0].samples, vec![(5_000, 8.0)]);
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
fn promql_query_session_matches_store_results_and_stats_across_repeated_queries() {
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
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("pod.name".to_string(), "backend-2".to_string()),
        ],
        &[(5_000, 3.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let expected_first = store
        .query_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    let expected_second = store
        .query_promql_with_limits("cpu.usage", 0, 10_000, QueryLimits::unlimited())
        .unwrap();

    let mut session = store.query_session().unwrap();
    let actual_first = session
        .query_promql_with_limits(
            r#"cpu.usage{pod.name="backend-1"}"#,
            0,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    let actual_second = session
        .query_promql_with_limits("cpu.usage", 0, 10_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(actual_first, expected_first);
    assert_eq!(actual_second, expected_second);
}

#[test]
fn promql_query_session_enforces_query_limits() {
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
    let mut session = store.query_session().unwrap();
    let err = session
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
fn promql_query_session_does_not_open_non_overlapping_segments() {
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
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(25_000, 2.0)],
    );
    writer.flush().unwrap();

    let non_overlapping = segment_dir_with_start(tempdir.path(), 20_000);
    fs::remove_file(non_overlapping.join(SegmentFile::Symbols.filename())).unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.0)]);
}

#[test]
fn promql_query_session_does_not_open_chunk_files_when_postings_are_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let segment = segment_dir_with_start(tempdir.path(), 0);
    fs::remove_file(segment.join(SegmentFile::Chunks.filename())).unwrap();
    fs::remove_file(segment.join(SegmentFile::ChunkIndex.filename())).unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();

    assert!(results.is_empty());
}

#[test]
fn promql_query_session_stats_count_lazy_file_opens() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(5_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    assert_eq!(session.stats().segment_context_opens, 0);

    let results = session.query_promql("cpu.usage", 0, 10_000).unwrap();
    assert!(results.is_empty());

    let stats = session.stats();
    assert_eq!(stats.segment_context_opens, 1);
    assert_eq!(stats.symbols_bin_opens, 1);
    assert_eq!(stats.indexes_puffin_opens, 1);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
}

#[test]
fn promql_query_session_uses_label_value_time_ranges_for_equality_pruning() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(1_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let results = session.query_promql("mem.usage", 8_000, 9_000).unwrap();
    assert!(results.is_empty());

    let stats = session.stats();
    assert_eq!(stats.segment_context_opens, 1);
    assert_eq!(stats.symbols_bin_opens, 1);
    assert_eq!(stats.indexes_puffin_opens, 1);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
}

#[test]
fn promql_query_session_uses_label_value_time_ranges_for_regex_pruning() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![(METRIC_NAME_LABEL.to_string(), "mem.usage".to_string())],
        &[(1_000, 2.0)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut session = store.query_session().unwrap();
    let results = session
        .query_promql(r#"{__name__=~"mem\..*"}"#, 8_000, 9_000)
        .unwrap();
    assert!(results.is_empty());

    let stats = session.stats();
    assert_eq!(stats.segment_context_opens, 1);
    assert_eq!(stats.symbols_bin_opens, 1);
    assert_eq!(stats.indexes_puffin_opens, 1);
    assert_eq!(stats.series_bin_opens, 0);
    assert_eq!(stats.chunk_index_bin_opens, 0);
    assert_eq!(stats.chunks_bin_opens, 0);
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
