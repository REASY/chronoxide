use std::collections::VecDeque;
use std::path::Path;
use std::time::Duration;

use chrono::TimeDelta;
use chronoxide_core::error::ChronoxideError;
use chronoxide_core::otlp_capture::{CompressionMethod, OtlpCaptureWriter};
use chronoxide_core::storage::head::{FloatEncoding, HeadConfig, IntEncoding};
use chronoxide_core::storage::segment::{
    QueryProjectionConfig, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
};
use chronoxide_ingester::app_config::LabelSetStoreKind;
use chronoxide_ingester::ingester::{Ingester, IngestionConfig};
use chronoxide_ingester::processor::{EventTimePolicy, OtlpLabelSetProcessor};
use chronoxide_ingester::source::{FileSource, MessageSource, SourceMessage};
use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, exponential_histogram_data_point::Buckets,
    summary_data_point::ValueAtQuantile,
};
use prost::Message;
use tokio_util::sync::CancellationToken;

struct VecSource {
    messages: VecDeque<SourceMessage>,
}

impl VecSource {
    fn new(messages: Vec<SourceMessage>) -> Self {
        Self {
            messages: messages.into(),
        }
    }

    fn one(message: SourceMessage) -> Self {
        Self::new(vec![message])
    }
}

impl MessageSource for VecSource {
    fn next_message(&mut self) -> Result<Option<SourceMessage>, ChronoxideError> {
        Ok(self.messages.pop_front())
    }
}

#[test]
fn source_level_e2e_decodes_ingests_seals_and_reads_controlled_otlp_metrics() {
    let segments_dir = tempfile::tempdir().unwrap();
    let payload = encode_request(controlled_request());
    let source = VecSource::one(source_message(7, 5_000, 5_000, payload));

    run_ingester(source, segments_dir.path(), None).unwrap();

    let store = query_store(segments_dir.path());

    assert_controlled_readbacks(&store);
}

#[test]
fn source_level_e2e_handles_malformed_payload_without_writing_segments() {
    let segments_dir = tempfile::tempdir().unwrap();
    let source = VecSource::one(source_message(1, 5_000, 5_000, vec![0xff, 0xff, 0xff]));

    run_ingester(source, segments_dir.path(), None).unwrap();

    assert!(segment_dir_names(segments_dir.path()).is_empty());
}

#[test]
fn source_level_e2e_rejects_missing_timestamp_without_using_source_timestamp() {
    let segments_dir = tempfile::tempdir().unwrap();
    let mut missing = number_dp(vec![kv_str("test.case", "missing-timestamp")]);
    missing.time_unix_nano = 0;
    missing.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let request = request(
        vec![],
        vec![metric_gauge("source.missing.timestamp", vec![missing])],
    );
    let source = VecSource::one(source_message(1, 95_000, 100_000, encode_request(request)));

    run_ingester(source, segments_dir.path(), None).unwrap();

    assert!(segment_dir_names(segments_dir.path()).is_empty());
}

#[test]
fn source_level_e2e_applies_event_time_policy_from_captured_at_ms() {
    let segments_dir = tempfile::tempdir().unwrap();
    let mut accepted = number_dp(vec![kv_str("test.case", "accepted")]);
    accepted.time_unix_nano = 95_000_000_000;
    accepted.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut old = number_dp(vec![kv_str("test.case", "old")]);
    old.time_unix_nano = 89_999_000_000;
    old.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    let mut future = number_dp(vec![kv_str("test.case", "future")]);
    future.time_unix_nano = 100_001_000_000;
    future.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(3.0));
    let request = request(
        vec![],
        vec![metric_gauge("source.policy", vec![accepted, old, future])],
    );
    let source = VecSource::one(source_message(1, 1_000, 100_000, encode_request(request)));

    run_ingester(source, segments_dir.path(), None).unwrap();

    let store = query_store(segments_dir.path());
    assert_promql_samples(
        &store,
        r#"source.policy{test.case="accepted"}"#,
        vec![(95_000, 1.0)],
    );
    assert_promql_empty(&store, r#"source.policy{test.case="old"}"#);
    assert_promql_empty(&store, r#"source.policy{test.case="future"}"#);
}

#[test]
fn source_level_e2e_does_not_store_missing_number_values() {
    let segments_dir = tempfile::tempdir().unwrap();
    let missing = number_dp(vec![kv_str("test.case", "missing-number")]);
    let request = request(
        vec![],
        vec![metric_gauge("source.missing.number", vec![missing])],
    );
    let source = VecSource::one(source_message(1, 5_000, 5_000, encode_request(request)));

    run_ingester(source, segments_dir.path(), None).unwrap();

    assert!(segment_dir_names(segments_dir.path()).is_empty());
}

#[test]
fn capture_replay_matches_direct_ingest_segment_names_and_promql_results() {
    let payload = encode_request(controlled_request());
    let direct_segments = tempfile::tempdir().unwrap();
    let replay_segments = tempfile::tempdir().unwrap();
    let capture_dir = tempfile::tempdir().unwrap();

    run_ingester(
        VecSource::one(source_message(7, 5_000, 5_000, payload.clone())),
        direct_segments.path(),
        Some(42),
    )
    .unwrap();

    let mut capture = OtlpCaptureWriter::create(
        capture_dir.path(),
        "controlled",
        CompressionMethod::Uncompressed,
    )
    .unwrap();
    capture.append(0, 7, 5_000, 5_000, &payload).unwrap();
    capture.close().unwrap();
    let replay_source = FileSource::new(capture_dir.path().to_path_buf()).unwrap();
    run_ingester(replay_source, replay_segments.path(), Some(42)).unwrap();

    assert_eq!(
        segment_dir_names(direct_segments.path()),
        segment_dir_names(replay_segments.path())
    );
    assert_controlled_readbacks(&query_store(direct_segments.path()));
    assert_controlled_readbacks(&query_store(replay_segments.path()));
}

#[test]
fn capture_replay_preserves_ordered_records_captured_at_anchor_and_segment_ids() {
    let first_payload = encode_request(ordered_gauge_request(5_000, 1.0));
    let second_payload = encode_request(ordered_gauge_request(6_000, 2.0));
    let direct_segments = tempfile::tempdir().unwrap();
    let replay_segments = tempfile::tempdir().unwrap();
    let second_replay_segments = tempfile::tempdir().unwrap();
    let capture_dir = tempfile::tempdir().unwrap();

    run_ingester(
        VecSource::new(vec![
            source_message(7, 100_000, 5_000, first_payload.clone()),
            source_message(8, 100_000, 6_000, second_payload.clone()),
        ]),
        direct_segments.path(),
        Some(42),
    )
    .unwrap();

    let mut capture = OtlpCaptureWriter::create(
        capture_dir.path(),
        "ordered",
        CompressionMethod::Uncompressed,
    )
    .unwrap();
    capture
        .append(0, 7, 100_000, 5_000, &first_payload)
        .unwrap();
    capture
        .append(1, 8, 100_000, 6_000, &second_payload)
        .unwrap();
    capture.close().unwrap();
    let replay_source = FileSource::new(capture_dir.path().to_path_buf()).unwrap();
    run_ingester(replay_source, replay_segments.path(), Some(42)).unwrap();
    let second_replay_source = FileSource::new(capture_dir.path().to_path_buf()).unwrap();
    run_ingester(
        second_replay_source,
        second_replay_segments.path(),
        Some(42),
    )
    .unwrap();

    assert_eq!(
        segment_dir_names(replay_segments.path()),
        segment_dir_names(second_replay_segments.path())
    );
    assert_ordered_replay_readbacks(&query_store(direct_segments.path()));
    assert_ordered_replay_readbacks(&query_store(replay_segments.path()));
    assert_ordered_replay_readbacks(&query_store(second_replay_segments.path()));
}

fn source_message(
    offset: i64,
    timestamp_ms: i64,
    captured_at_ms: i64,
    payload: Vec<u8>,
) -> SourceMessage {
    SourceMessage {
        topic: "controlled".to_string(),
        partition: 0,
        offset,
        timestamp_ms,
        captured_at_ms,
        payload,
    }
}

fn run_ingester<S: MessageSource>(
    source: S,
    segments_dir: &Path,
    deterministic_id_seed: Option<u64>,
) -> Result<(), ChronoxideError> {
    let mut writer_config = SegmentWriterConfig::new(segments_dir, Duration::from_secs(10));
    if let Some(seed) = deterministic_id_seed {
        writer_config = writer_config.with_deterministic_segment_ids(seed);
    }
    let segment_writer = SegmentWriter::new(writer_config).unwrap();
    let head_config = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head_config,
        Some(segment_writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        TimeDelta::seconds(10),
        TimeDelta::seconds(0),
        true,
    ))
    .with_shutdown_report(false);
    let config = IngestionConfig {
        max_event_age: TimeDelta::seconds(10),
        max_event_lead: TimeDelta::seconds(0),
        drop_outdated: true,
        labelset_store: LabelSetStoreKind::FlatInterned,
        labelset_report_interval: Duration::from_secs(3600),
        stop_after_messages: None,
        replay_from: None,
        capture_to: None,
        capture_only: false,
        segment_writer: None,
    };

    let meter = opentelemetry::global::meter("source-level-e2e");
    let mut ingester =
        Ingester::new(source, config, processor, meter, CancellationToken::new()).unwrap();
    ingester.start()
}

fn query_store(segments_dir: &Path) -> SegmentStoreReader {
    SegmentStoreReader::open_with_query_projection_config(
        segments_dir,
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap()
}

fn assert_controlled_readbacks(store: &SegmentStoreReader) {
    assert_promql_samples(
        store,
        r#"source.gauge{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 1.25)],
    );
    assert_promql_samples(
        store,
        r#"source.sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 42.0)],
    );
    assert_promql_samples(
        store,
        r#"source.histogram_count{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        store,
        r#"source.histogram_sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        store,
        r#"source.histogram_bucket{test.case="source-e2e",service.name="source-level-suite",le="5"}"#,
        vec![(5_000, 3.0)],
    );
    assert_promql_samples(
        store,
        r#"source.histogram_bucket{test.case="source-e2e",service.name="source-level-suite",le="+Inf"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        store,
        r#"source.exphist_count{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        store,
        r#"source.exphist_sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 12.0)],
    );
    assert_promql_samples(
        store,
        r#"source.exphist_bucket{test.case="source-e2e",service.name="source-level-suite",le="2"}"#,
        vec![(5_000, 2.0)],
    );
    assert_promql_samples(
        store,
        r#"source.exphist_bucket{test.case="source-e2e",service.name="source-level-suite",le="+Inf"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        store,
        r#"source.summary_count{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        store,
        r#"source.summary_sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 50.0)],
    );
    assert_promql_samples(
        store,
        r#"source.summary{test.case="source-e2e",service.name="source-level-suite",quantile="0.9"}"#,
        vec![(5_000, 8.0)],
    );
}

fn assert_ordered_replay_readbacks(store: &SegmentStoreReader) {
    assert_promql_samples(
        store,
        r#"source.ordered{test.case="ordered-replay",service.name="source-level-suite"}"#,
        vec![(5_000, 1.0), (6_000, 2.0)],
    );
}

fn assert_promql_samples(store: &SegmentStoreReader, query: &str, expected: Vec<(u64, f64)>) {
    let results = store.query_promql(query, 0, 200_000).unwrap();
    assert_eq!(results.len(), 1, "query {query}");
    assert_eq!(results[0].samples, expected, "query {query}");
}

fn assert_promql_empty(store: &SegmentStoreReader, query: &str) {
    let results = store.query_promql(query, 0, 200_000).unwrap();
    assert!(results.is_empty(), "query {query}: {results:?}");
}

fn segment_dir_names(segments_dir: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_dir()))
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with("seg-"))
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn encode_request(req: ExportMetricsServiceRequest) -> Vec<u8> {
    let mut payload = Vec::new();
    req.encode(&mut payload).unwrap();
    payload
}

fn controlled_request() -> ExportMetricsServiceRequest {
    let mut gauge = number_dp(vec![kv_str("test.case", "source-e2e")]);
    gauge.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.25));

    let mut sum = number_dp(vec![kv_str("test.case", "source-e2e")]);
    sum.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));

    let mut hist = histogram_dp(vec![kv_str("test.case", "source-e2e")]);
    hist.count = 4;
    hist.sum = Some(10.0);
    hist.min = Some(1.0);
    hist.max = Some(4.0);
    hist.explicit_bounds = vec![1.0, 5.0];
    hist.bucket_counts = vec![1, 2, 1];

    let mut exphist = exp_histogram_dp(vec![kv_str("test.case", "source-e2e")]);
    exphist.count = 5;
    exphist.sum = Some(12.0);
    exphist.min = Some(1.0);
    exphist.max = Some(3.0);
    exphist.scale = 0;
    exphist.zero_count = 0;
    exphist.positive = Some(Buckets {
        offset: 0,
        bucket_counts: vec![2, 3],
    });
    exphist.negative = Some(Buckets {
        offset: 0,
        bucket_counts: vec![],
    });

    let mut summary = summary_dp(vec![kv_str("test.case", "source-e2e")]);
    summary.count = 10;
    summary.sum = 50.0;
    summary.quantile_values = vec![
        ValueAtQuantile {
            quantile: 0.5,
            value: 4.0,
        },
        ValueAtQuantile {
            quantile: 0.9,
            value: 8.0,
        },
    ];

    request(
        vec![kv_str("service.name", "source-level-suite")],
        vec![
            metric_gauge("source.gauge", vec![gauge]),
            metric_sum("source.sum", vec![sum]),
            metric_histogram("source.histogram", vec![hist]),
            metric_exp_histogram("source.exphist", vec![exphist]),
            metric_summary("source.summary", vec![summary]),
        ],
    )
}

fn ordered_gauge_request(timestamp_ms: u64, value: f64) -> ExportMetricsServiceRequest {
    let mut gauge = number_dp(vec![kv_str("test.case", "ordered-replay")]);
    gauge.time_unix_nano = timestamp_ms.saturating_mul(1_000_000);
    gauge.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
        value,
    ));

    request(
        vec![kv_str("service.name", "source-level-suite")],
        vec![metric_gauge("source.ordered", vec![gauge])],
    )
}

fn request(
    resource_attrs: Vec<tonic::common::v1::KeyValue>,
    metrics: Vec<tonic::metrics::v1::Metric>,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![tonic::metrics::v1::ResourceMetrics {
            resource: Some(tonic::resource::v1::Resource {
                attributes: resource_attrs,
                ..Default::default()
            }),
            scope_metrics: vec![tonic::metrics::v1::ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn kv_str(key: &str, value: &str) -> tonic::common::v1::KeyValue {
    tonic::common::v1::KeyValue {
        key: key.to_string(),
        value: Some(tonic::common::v1::AnyValue {
            value: Some(tonic::common::v1::any_value::Value::StringValue(
                value.to_string(),
            )),
        }),
    }
}

fn number_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::NumberDataPoint {
    tonic::metrics::v1::NumberDataPoint {
        attributes: attrs,
        time_unix_nano: 5_000_000_000,
        ..Default::default()
    }
}

fn histogram_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::HistogramDataPoint {
    tonic::metrics::v1::HistogramDataPoint {
        attributes: attrs,
        time_unix_nano: 5_000_000_000,
        ..Default::default()
    }
}

fn exp_histogram_dp(
    attrs: Vec<tonic::common::v1::KeyValue>,
) -> tonic::metrics::v1::ExponentialHistogramDataPoint {
    tonic::metrics::v1::ExponentialHistogramDataPoint {
        attributes: attrs,
        time_unix_nano: 5_000_000_000,
        ..Default::default()
    }
}

fn summary_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::SummaryDataPoint {
    tonic::metrics::v1::SummaryDataPoint {
        attributes: attrs,
        time_unix_nano: 5_000_000_000,
        ..Default::default()
    }
}

fn metric_gauge(
    name: &str,
    dps: Vec<tonic::metrics::v1::NumberDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Gauge(
            tonic::metrics::v1::Gauge {
                data_points: dps,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn metric_sum(
    name: &str,
    dps: Vec<tonic::metrics::v1::NumberDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Sum(
            tonic::metrics::v1::Sum {
                data_points: dps,
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            },
        )),
        ..Default::default()
    }
}

fn metric_histogram(
    name: &str,
    dps: Vec<tonic::metrics::v1::HistogramDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Histogram(
            tonic::metrics::v1::Histogram {
                data_points: dps,
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            },
        )),
        ..Default::default()
    }
}

fn metric_exp_histogram(
    name: &str,
    dps: Vec<tonic::metrics::v1::ExponentialHistogramDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(
            tonic::metrics::v1::ExponentialHistogram {
                data_points: dps,
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            },
        )),
        ..Default::default()
    }
}

fn metric_summary(
    name: &str,
    dps: Vec<tonic::metrics::v1::SummaryDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Summary(
            tonic::metrics::v1::Summary { data_points: dps },
        )),
        ..Default::default()
    }
}
