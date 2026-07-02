use std::collections::VecDeque;
use std::time::Duration;

use chrono::TimeDelta;
use chronoxide_core::error::ChronoxideError;
use chronoxide_core::storage::head::{FloatEncoding, HeadConfig, IntEncoding};
use chronoxide_core::storage::segment::{
    QueryProjectionConfig, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
};
use chronoxide_ingester::app_config::LabelSetStoreKind;
use chronoxide_ingester::ingester::{Ingester, IngestionConfig};
use chronoxide_ingester::processor::{EventTimePolicy, OtlpLabelSetProcessor};
use chronoxide_ingester::source::{MessageSource, SourceMessage};
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
    fn one(message: SourceMessage) -> Self {
        Self {
            messages: VecDeque::from([message]),
        }
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
    let source = VecSource::one(SourceMessage {
        topic: "controlled".to_string(),
        partition: 0,
        offset: 7,
        timestamp_ms: 5_000,
        captured_at_ms: 5_000,
        payload,
    });

    let segment_writer = SegmentWriter::new(SegmentWriterConfig::new(
        segments_dir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
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
    ingester.start().unwrap();

    let store = SegmentStoreReader::open_with_query_projection_config(
        segments_dir.path(),
        QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0]),
    )
    .unwrap();

    assert_promql_samples(
        &store,
        r#"source.gauge{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 1.25)],
    );
    assert_promql_samples(
        &store,
        r#"source.sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 42.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.histogram_count{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.histogram_sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.histogram_bucket{test.case="source-e2e",service.name="source-level-suite",le="5"}"#,
        vec![(5_000, 3.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.histogram_bucket{test.case="source-e2e",service.name="source-level-suite",le="+Inf"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.exphist_count{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.exphist_sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 12.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.exphist_bucket{test.case="source-e2e",service.name="source-level-suite",le="2"}"#,
        vec![(5_000, 2.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.exphist_bucket{test.case="source-e2e",service.name="source-level-suite",le="+Inf"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.summary_count{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.summary_sum{test.case="source-e2e",service.name="source-level-suite"}"#,
        vec![(5_000, 50.0)],
    );
    assert_promql_samples(
        &store,
        r#"source.summary{test.case="source-e2e",service.name="source-level-suite",quantile="0.9"}"#,
        vec![(5_000, 8.0)],
    );
}

fn assert_promql_samples(store: &SegmentStoreReader, query: &str, expected: Vec<(u64, f64)>) {
    let results = store.query_promql(query, 0, 10_000).unwrap();
    assert_eq!(results.len(), 1, "query {query}");
    assert_eq!(results[0].samples, expected, "query {query}");
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

    ExportMetricsServiceRequest {
        resource_metrics: vec![tonic::metrics::v1::ResourceMetrics {
            resource: Some(tonic::resource::v1::Resource {
                attributes: vec![kv_str("service.name", "source-level-suite")],
                ..Default::default()
            }),
            scope_metrics: vec![tonic::metrics::v1::ScopeMetrics {
                metrics: vec![
                    metric_gauge("source.gauge", vec![gauge]),
                    metric_sum("source.sum", vec![sum]),
                    metric_histogram("source.histogram", vec![hist]),
                    metric_exp_histogram("source.exphist", vec![exphist]),
                    metric_summary("source.summary", vec![summary]),
                ],
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
