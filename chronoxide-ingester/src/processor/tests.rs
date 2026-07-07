use super::*;
use crate::app_config::LabelSetStoreKind;
use crate::source::SourceMessageMetadata;
use chronoxide_core::labels::METRIC_NAME_LABEL;
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::chunk::{ChunkKind, ChunkReader, ChunkSamples};
use chronoxide_core::storage::head::{
    CounterResetHint, HeadConfig, IntEncoding, OtlpAggregationTemporality,
};
use chronoxide_core::storage::index::read_segment_indexes;
use chronoxide_core::storage::segment::{
    QueryProjectionConfig, SegmentFile, SegmentReader, SegmentStoreReader, SegmentWriterConfig,
};
use chronoxide_core::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_HISTOGRAM, SERIES_KIND_SUMMARY, read_series_bin,
    read_symbols_bin,
};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, exponential_histogram_data_point::Buckets,
    summary_data_point::ValueAtQuantile,
};
use std::fs::{self, File};

fn kv_any(key: &str, value: tonic::common::v1::any_value::Value) -> tonic::common::v1::KeyValue {
    tonic::common::v1::KeyValue {
        key: key.to_string(),
        value: Some(tonic::common::v1::AnyValue { value: Some(value) }),
    }
}

fn kv_str(key: &str, value: &str) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::StringValue(value.to_string()),
    )
}

fn kv_bool(key: &str, value: bool) -> tonic::common::v1::KeyValue {
    kv_any(key, tonic::common::v1::any_value::Value::BoolValue(value))
}

fn kv_int(key: &str, value: i64) -> tonic::common::v1::KeyValue {
    kv_any(key, tonic::common::v1::any_value::Value::IntValue(value))
}

fn kv_double(key: &str, value: f64) -> tonic::common::v1::KeyValue {
    kv_any(key, tonic::common::v1::any_value::Value::DoubleValue(value))
}

fn kv_bytes(key: &str, value: &[u8]) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::BytesValue(value.to_vec()),
    )
}

fn kv_array(key: &str) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::ArrayValue(tonic::common::v1::ArrayValue {
            values: vec![],
        }),
    )
}

fn kv_kvlist(key: &str) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::KvlistValue(tonic::common::v1::KeyValueList {
            values: vec![],
        }),
    )
}

fn number_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::NumberDataPoint {
    tonic::metrics::v1::NumberDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
        ..Default::default()
    }
}

fn histogram_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::HistogramDataPoint {
    tonic::metrics::v1::HistogramDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
        ..Default::default()
    }
}

fn exp_histogram_dp(
    attrs: Vec<tonic::common::v1::KeyValue>,
) -> tonic::metrics::v1::ExponentialHistogramDataPoint {
    tonic::metrics::v1::ExponentialHistogramDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
        ..Default::default()
    }
}

fn summary_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::SummaryDataPoint {
    tonic::metrics::v1::SummaryDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
            tonic::metrics::v1::Summary {
                data_points: dps,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
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
        ..Default::default()
    }
}

fn segment_dir_count(segments_dir: &std::path::Path) -> usize {
    fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .count()
}

fn collect_labelset(processor: &OtlpLabelSetProcessor, series: SeriesRef) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    match &processor.labelsets {
        LabelSetInterner::Naive(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
        LabelSetInterner::FlatInterned(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
        LabelSetInterner::KeySetDictEncoded(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
    }
    out.sort();
    out
}

#[test]
fn labelset_interner_builds_segment_metadata_for_all_store_kinds() {
    let labels = [
        KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        KeyValueRef::from(("namespace", "default")),
        KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut expected = SegmentSeriesMetadataBuilder::new();
    for label in &labels {
        expected.push_label(label.key, label.value);
    }
    let expected = expected.finish();

    for store in [
        LabelSetStoreKind::Naive,
        LabelSetStoreKind::FlatInterned,
        LabelSetStoreKind::KeySetDictEncoded,
    ] {
        let mut stats = OtlpMetricsIngestionStats::new();
        let mut interner = LabelSetInterner::new(store);
        let series = interner.intern(&labels, &mut stats).unwrap();

        let metadata = interner.segment_metadata(series);

        assert_eq!(metadata.series_id(), expected.series_id());
        assert_eq!(metadata.labels(), expected.labels());
    }
}

#[test]
fn format_window_ms_formats_positive_and_negative() {
    assert_eq!(format_window_ms(0), "00:00:00.000");
    assert_eq!(format_window_ms(3_661_001), "01:01:01.001");
    assert_eq!(format_window_ms(-1), "-00:00:00.001");
}

#[test]
fn processor_drops_old_and_future_datapoints_using_captured_at_ms() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ));

    let mut accepted = number_dp(vec![kv_str("pod.name", "accepted")]);
    accepted.time_unix_nano = 95_000_000_000;
    accepted.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut too_old = number_dp(vec![kv_str("pod.name", "old")]);
    too_old.time_unix_nano = 89_999_000_000;
    too_old.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    let mut too_future = number_dp(vec![kv_str("pod.name", "future")]);
    too_future.time_unix_nano = 105_001_000_000;
    too_future.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(3.0));

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 100_000,
            },
            request(
                vec![],
                vec![metric_gauge(
                    "cpu.usage",
                    vec![accepted, too_old, too_future],
                )],
            ),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.observed_datapoints, 3);
    assert_eq!(snap.totals.datapoints, 1);
    assert_eq!(snap.totals.observed_datapoint_types.gauge, 3);
    assert_eq!(snap.totals.datapoint_types.gauge, 1);
    assert_eq!(snap.totals.datapoint_policy.accepted, 1);
    assert_eq!(snap.totals.datapoint_policy.dropped_too_old, 1);
    assert_eq!(snap.totals.datapoint_policy.dropped_too_future, 1);
    assert_eq!(snap.totals.datapoint_policy.missing_timestamp, 0);
    let skew = snap.totals.event_time_skew;
    let all_skew = skew.all.unwrap();
    assert_eq!(all_skew.count, 3);
    assert_eq!(all_skew.min, -10_001);
    assert_eq!(all_skew.max, 5_001);
    assert_eq!(skew.accepted.unwrap().min, -5_000);
    assert_eq!(skew.accepted.unwrap().max, -5_000);
    assert_eq!(skew.dropped_too_old.unwrap().min, -10_001);
    assert_eq!(skew.dropped_too_future.unwrap().max, 5_001);
    assert_eq!(processor.labelsets.stats().series, 1);

    processor.flush_head().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = store
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "accepted"),
            ],
            0,
            200_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(95_000, 1.0)]);
    assert_eq!(segment_dir_count(tempdir.path()), 1);
}

#[test]
#[should_panic(expected = "max_event_lead must be non-negative")]
fn event_time_policy_rejects_negative_event_lead() {
    EventTimePolicy::new(
        chrono::TimeDelta::seconds(60),
        chrono::TimeDelta::seconds(-1),
        true,
    );
}

#[test]
fn processor_rejects_missing_otlp_timestamp_instead_of_using_kafka_timestamp() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ));

    let mut missing = number_dp(vec![kv_str("pod.name", "missing")]);
    missing.time_unix_nano = 0;
    missing.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 95_000,
                captured_at_ms: 100_000,
            },
            request(vec![], vec![metric_gauge("cpu.usage", vec![missing])]),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::DroppedOutdated);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.observed_datapoints, 1);
    assert_eq!(snap.totals.datapoints, 0);
    assert_eq!(snap.totals.observed_datapoint_types.gauge, 1);
    assert_eq!(snap.totals.datapoint_types.gauge, 0);
    assert_eq!(snap.totals.datapoint_policy.accepted, 0);
    assert_eq!(snap.totals.datapoint_policy.missing_timestamp, 1);
    assert_eq!(processor.labelsets.stats().series, 0);

    processor.flush_head().unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 0);
}

#[test]
fn processor_counts_missing_number_values_separately_from_time_policy_acceptance() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ));

    let attrs = vec![kv_str("pod.name", "same")];
    let mut valid = number_dp(attrs.clone());
    valid.time_unix_nano = 95_000_000_000;
    valid.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut missing_value = number_dp(attrs);
    missing_value.time_unix_nano = 95_001_000_000;
    missing_value.value = None;

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 100_000,
            },
            request(
                vec![],
                vec![metric_gauge("cpu.usage", vec![valid, missing_value])],
            ),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.datapoints, 2);
    assert_eq!(snap.totals.datapoint_policy.accepted, 2);
    assert_eq!(snap.totals.datapoint_policy.rejected(), 0);
    assert_eq!(snap.totals.datapoint_storage.recorded_samples, 1);
    assert_eq!(snap.totals.datapoint_storage.missing_number_values, 1);
    assert_eq!(snap.window.datapoint_storage, snap.totals.datapoint_storage);

    processor.flush_head().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = store
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "same"),
            ],
            0,
            200_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(95_000, 1.0)]);
}

#[test]
fn processor_canonicalizes_labels_and_skips_non_scalar_values() {
    for store in [
        LabelSetStoreKind::FlatInterned,
        LabelSetStoreKind::KeySetDictEncoded,
    ] {
        let mut processor =
            OtlpLabelSetProcessor::new(store, Duration::from_secs(3600), None, None);

        let resource_attrs = vec![
            kv_str("cluster", "prod"),
            kv_str("resource_only", "r1"),
            kv_int("int_value", 42),
        ];
        let dp_attrs = vec![
            kv_str("cluster", "staging"), // overrides resource
            kv_str("pod", "backend-123"),
            kv_str(chronoxide_core::labels::METRIC_NAME_LABEL, "ignored"),
            kv_bool("bool_value", true),
            kv_double("double_value", 3.14),
            kv_bytes("bytes_value", b"abc"),
            kv_array("array_value"),
            kv_kvlist("kvlist_value"),
            kv_str("", "ignored_empty_key"),
        ];

        let req = request(
            resource_attrs,
            vec![metric_gauge("cpu_usage", vec![number_dp(dp_attrs)])],
        );

        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 1_000,
                    captured_at_ms: 10_000,
                },
                req,
            )
            .unwrap();

        let store_stats = processor.labelsets.stats();
        assert_eq!(store_stats.series, 1);

        let labels = collect_labelset(&processor, SeriesRef::new(0));
        let mut expected = vec![
            ("__name__".to_string(), "cpu_usage".to_string()),
            ("bool_value".to_string(), "true".to_string()),
            ("cluster".to_string(), "staging".to_string()),
            ("double_value".to_string(), "3.14".to_string()),
            ("int_value".to_string(), "42".to_string()),
            ("pod".to_string(), "backend-123".to_string()),
            ("resource_only".to_string(), "r1".to_string()),
        ];
        expected.sort();
        assert_eq!(labels, expected);

        let snap = processor.labelset_stats.snapshot();
        assert_eq!(snap.totals.skipped_non_scalar_values, 3);
    }
}

#[test]
fn processor_counts_metric_and_datapoint_types_and_dedups_series() {
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        None,
        None,
    );

    let same_attrs = vec![kv_str("pod", "same")];
    let req = request(
        vec![],
        vec![
            metric_gauge(
                "m_gauge",
                vec![number_dp(same_attrs.clone()), number_dp(same_attrs)],
            ),
            metric_sum("m_sum", vec![number_dp(vec![kv_str("pod", "sum")])]),
            metric_histogram("m_hist", vec![histogram_dp(vec![kv_str("pod", "hist")])]),
            metric_exp_histogram(
                "m_exphist",
                vec![exp_histogram_dp(vec![kv_str("pod", "exphist")])],
            ),
            metric_summary(
                "m_summary",
                vec![summary_dp(vec![kv_str("pod", "summary")])],
            ),
        ],
    );

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 1,
                offset: 123,
                timestamp_ms: 2_000,
                captured_at_ms: 10_001,
            },
            req,
        )
        .unwrap();

    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.messages, 1);
    assert_eq!(snap.totals.metrics, 5);
    assert_eq!(snap.totals.unique_metrics, 5);
    assert_eq!(snap.totals.datapoints, 6);

    assert_eq!(snap.totals.metric_types.gauge, 1);
    assert_eq!(snap.totals.metric_types.sum, 1);
    assert_eq!(snap.totals.metric_types.histogram, 1);
    assert_eq!(snap.totals.metric_types.exponential_histogram, 1);
    assert_eq!(snap.totals.metric_types.summary, 1);

    assert_eq!(snap.totals.datapoint_types.gauge, 2);
    assert_eq!(snap.totals.datapoint_types.sum, 1);
    assert_eq!(snap.totals.datapoint_types.histogram, 1);
    assert_eq!(snap.totals.datapoint_types.exponential_histogram, 1);
    assert_eq!(snap.totals.datapoint_types.summary, 1);

    let store_stats = processor.labelsets.stats();
    assert_eq!(store_stats.series, 5); // gauge datapoints dedup to 1 series

    assert_eq!(snap.partition_watermarks.len(), 1);
    let ((topic, partition), wm) = &snap.partition_watermarks[0];
    assert_eq!(topic, "t");
    assert_eq!(*partition, 1);
    assert_eq!(wm.messages, 1);
    assert_eq!(wm.datapoints, 6);

    processor.maybe_report_labelset_stats(true);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.window.messages, 0);
    assert_eq!(snap.window.metrics, 0);
    assert_eq!(snap.window.datapoints, 0);
    assert_eq!(snap.window.unique_metrics, 0);
}

#[test]
fn data_type_counts_markdown_reports_metric_records_and_datapoints() {
    let mut metric_types = OtlpDataTypeCounts::default();
    metric_types.gauge = 1;
    metric_types.sum = 2;
    metric_types.histogram = 3;
    metric_types.exponential_histogram = 4;
    metric_types.summary = 5;
    let mut observed_datapoint_types = OtlpDataTypeCounts::default();
    observed_datapoint_types.gauge = 10;
    observed_datapoint_types.sum = 20;
    observed_datapoint_types.histogram = 30;
    observed_datapoint_types.exponential_histogram = 40;
    observed_datapoint_types.summary = 50;
    let mut accepted_datapoint_types = OtlpDataTypeCounts::default();
    accepted_datapoint_types.gauge = 8;
    accepted_datapoint_types.sum = 18;
    accepted_datapoint_types.histogram = 28;
    accepted_datapoint_types.exponential_histogram = 38;
    accepted_datapoint_types.summary = 48;

    let markdown = data_type_counts_markdown(
        &metric_types,
        &observed_datapoint_types,
        &accepted_datapoint_types,
    );

    assert!(markdown.contains("## OTLP Data Type Counts"));
    assert!(
        markdown.contains("| Type | Metric Records | Observed Datapoints | Accepted Datapoints |")
    );
    assert!(markdown.contains("| Gauge | 1 | 10 | 8 |"));
    assert!(markdown.contains("| Sum | 2 | 20 | 18 |"));
    assert!(markdown.contains("| Histogram | 3 | 30 | 28 |"));
    assert!(markdown.contains("| Exponential Histogram | 4 | 40 | 38 |"));
    assert!(markdown.contains("| Summary | 5 | 50 | 48 |"));
}

#[test]
fn datapoint_policy_counts_markdown_reports_drop_reasons() {
    let totals = DatapointPolicyCounts {
        accepted: 10,
        dropped_too_old: 2,
        dropped_too_future: 3,
        missing_timestamp: 4,
    };
    let window = DatapointPolicyCounts {
        accepted: 1,
        dropped_too_old: 0,
        dropped_too_future: 1,
        missing_timestamp: 0,
    };

    let markdown = datapoint_policy_counts_markdown(&totals, &window);

    assert!(markdown.contains("## Datapoint Policy Counts"));
    assert!(markdown.contains("| Observed | 19 | 2 |"));
    assert!(markdown.contains("| Time-Policy Accepted | 10 | 1 |"));
    assert!(markdown.contains("| Dropped Too Old | 2 | 0 |"));
    assert!(markdown.contains("| Dropped Too Future | 3 | 1 |"));
    assert!(markdown.contains("| Missing Timestamp | 4 | 0 |"));
    assert!(markdown.contains("| Rejected Total | 9 | 1 |"));
}

#[test]
fn datapoint_storage_counts_markdown_reports_recorded_and_missing_number_values() {
    let totals = DatapointStorageCounts {
        recorded_samples: 7,
        missing_number_values: 2,
    };
    let window = DatapointStorageCounts {
        recorded_samples: 3,
        missing_number_values: 1,
    };
    let policy_totals = DatapointPolicyCounts {
        accepted: 10,
        ..Default::default()
    };
    let policy_window = DatapointPolicyCounts {
        accepted: 4,
        ..Default::default()
    };

    let markdown =
        datapoint_storage_counts_markdown(&totals, &window, &policy_totals, &policy_window);

    assert!(markdown.contains("## Datapoint Storage Counts"));
    assert!(markdown.contains("| Time-Policy Accepted | 10 | 4 |"));
    assert!(markdown.contains("| Recorded Samples | 7 | 3 |"));
    assert!(markdown.contains("| Missing Number Value | 2 | 1 |"));
    assert!(markdown.contains("| Accepted Not Recorded | 3 | 1 |"));
}

#[test]
fn event_time_skew_markdown_reports_signed_distributions() {
    let mut stats = OtlpMetricsIngestionStats::new();
    stats.record_event_time_skew(metrics_ingestion_stats::EventTimeSkewOutcome::Accepted, -5);
    stats.record_event_time_skew(
        metrics_ingestion_stats::EventTimeSkewOutcome::DroppedTooOld,
        -10,
    );
    stats.record_event_time_skew(
        metrics_ingestion_stats::EventTimeSkewOutcome::DroppedTooFuture,
        3,
    );
    let snapshot = stats.snapshot();

    let markdown = event_time_skew_markdown(&snapshot.totals.event_time_skew);

    assert!(markdown.contains("## Event Time Skew"));
    assert!(markdown.contains("event_ms - captured_at_ms"));
    assert!(markdown.contains("| All Timestamped | 3 |"));
    assert!(markdown.contains("| Accepted | 1 |"));
    assert!(markdown.contains("| Dropped Too Old | 1 |"));
    assert!(markdown.contains("| Dropped Too Future | 1 |"));
}

#[test]
fn number_value_handles_int_and_double() {
    let mut dp = number_dp(vec![]);
    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(5));
    assert_eq!(number_value(&dp), Some(SampleValue::Int64(5)));

    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.5));
    assert_eq!(number_value(&dp), Some(SampleValue::Float(2.5)));

    dp.value = None;
    assert_eq!(number_value(&dp), None);
}

#[test]
fn processor_writes_segment_meta() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut dp = number_dp(vec![kv_str("pod", "backend-1")]);
    dp.time_unix_nano = 5_000_000_000;
    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(3.14));
    let req = request(vec![], vec![metric_gauge("cpu_usage", vec![dp])]);

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_002,
            },
            req,
        )
        .unwrap();
    processor.flush_head().unwrap();
    let profile = processor.last_head_window_write_profile().unwrap();
    assert_eq!(profile.series, 1);
    assert_eq!(profile.datapoints, 1);
    assert_eq!(profile.record_subphases.chunks, 1);
    assert_eq!(profile.record_subphases.samples, 1);
    assert!(profile.series_reserve <= profile.total);
    assert!(profile.total >= profile.writer_flush);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 1);
    assert_eq!(reader.meta().series, 1);
    let chunk_len = fs::metadata(reader.file_path(SegmentFile::Chunks))
        .unwrap()
        .len();
    assert!(chunk_len > 0);
}

#[test]
fn processor_writes_segment_series_metadata_and_exact_postings() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut dp1 = number_dp(vec![
        kv_str("namespace", "default"),
        kv_str("pod.name", "backend-1"),
    ]);
    dp1.time_unix_nano = 5_000_000_000;
    dp1.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));

    let mut dp2 = number_dp(vec![
        kv_str("namespace", "default"),
        kv_str("pod.name", "backend-2"),
    ]);
    dp2.time_unix_nano = 6_000_000_000;
    dp2.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));

    let req = request(vec![], vec![metric_gauge("cpu.usage", vec![dp1, dp2])]);

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_003,
            },
            req,
        )
        .unwrap();
    processor.flush_head().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(seg_dir).unwrap();

    let symbols =
        read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols)).expect("open symbols"))
            .unwrap();
    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).expect("open series"))
            .unwrap();
    let indexes = read_segment_indexes(
        File::open(reader.file_path(SegmentFile::Indexes)).expect("open indexes"),
    )
    .unwrap();
    let postings = indexes.exact_postings;

    assert_eq!(series.len(), 2);
    let metric_sym = symbols.lookup(METRIC_NAME_LABEL).unwrap();
    let metric_value = series[0]
        .labels
        .iter()
        .find_map(|(key, value)| (*key == metric_sym).then_some(*value))
        .and_then(|sym| symbols.resolve(sym))
        .unwrap();
    assert!(metric_value.starts_with("cpu_usage_x"));

    let namespace_sym = symbols.lookup("namespace").unwrap();
    let default_sym = symbols.lookup("default").unwrap();
    assert_eq!(postings.get(namespace_sym, default_sym), Some(&[0, 1][..]));

    let labels: Vec<_> = series
        .iter()
        .flat_map(|entry| {
            entry.labels.iter().map(|(key, value)| {
                (
                    symbols.resolve(*key).unwrap().to_string(),
                    symbols.resolve(*value).unwrap().to_string(),
                )
            })
        })
        .collect();
    assert!(
        labels
            .iter()
            .any(|(key, value)| { key.starts_with("pod_name_x") && value == "backend-1" })
    );
    assert!(
        labels
            .iter()
            .any(|(key, value)| { key.starts_with("pod_name_x") && value == "backend-2" })
    );
}

#[test]
fn processor_writes_integer_number_datapoints_as_promql_float_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut dp = number_dp(vec![kv_str("pod.name", "backend-1")]);
    dp.time_unix_nano = 5_000_000_000;
    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));
    let req = request(vec![], vec![metric_sum("requests.total", vec![dp])]);

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_004,
            },
            req,
        )
        .unwrap();
    processor.flush_head().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(seg_dir).unwrap();

    let metric = normalize_metric_name("requests.total");
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
    assert_eq!(results[0].samples, vec![(5_000, 42.0)]);
}

#[test]
fn processor_writes_typed_otlp_datapoints_to_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut hist = histogram_dp(vec![kv_str("pod.name", "hist")]);
    hist.time_unix_nano = 5_000_000_000;
    hist.count = 4;
    hist.sum = Some(10.0);
    hist.min = Some(1.0);
    hist.max = Some(4.0);
    hist.explicit_bounds = vec![1.0, 5.0];
    hist.bucket_counts = vec![1, 2, 1];

    let mut exphist = exp_histogram_dp(vec![kv_str("pod.name", "exphist")]);
    exphist.time_unix_nano = 6_000_000_000;
    exphist.count = 6;
    exphist.sum = Some(15.0);
    exphist.min = Some(1.0);
    exphist.max = Some(8.0);
    exphist.scale = 2;
    exphist.zero_count = 1;
    exphist.positive = Some(Buckets {
        offset: -1,
        bucket_counts: vec![2, 3],
    });
    exphist.negative = Some(Buckets {
        offset: 0,
        bucket_counts: vec![0],
    });

    let mut summary = summary_dp(vec![kv_str("pod.name", "summary")]);
    summary.time_unix_nano = 7_000_000_000;
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

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_005,
            },
            request(
                vec![],
                vec![
                    metric_histogram("request.duration", vec![hist]),
                    metric_exp_histogram("request.size", vec![exphist]),
                    metric_summary("request.latency", vec![summary]),
                ],
            ),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let profile = processor.last_head_window_write_profile().unwrap();
    assert_eq!(profile.datapoints, 3);
    assert_eq!(profile.series, 3);
    assert_eq!(profile.record_subphases.chunks, 3);
    assert_eq!(profile.record_subphases.samples, 3);
    assert!(profile.series_reserve <= profile.total);
    assert_eq!(profile.dropped_histogram_series, 0);
    assert_eq!(profile.dropped_exponential_histogram_series, 0);
    assert_eq!(profile.dropped_summary_series, 0);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 3);
    assert_eq!(reader.meta().series, 3);

    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).expect("open series"))
            .unwrap();
    let kind_masks: Vec<u8> = series.iter().map(|entry| entry.kind_mask).collect();
    assert!(
        kind_masks
            .iter()
            .any(|mask| mask & SERIES_KIND_HISTOGRAM == SERIES_KIND_HISTOGRAM)
    );
    assert!(kind_masks.iter().any(|mask| {
        mask & SERIES_KIND_EXPONENTIAL_HISTOGRAM == SERIES_KIND_EXPONENTIAL_HISTOGRAM
    }));
    assert!(
        kind_masks
            .iter()
            .any(|mask| mask & SERIES_KIND_SUMMARY == SERIES_KIND_SUMMARY)
    );

    let mut chunk_reader = ChunkReader::new(reader.open_chunks().unwrap());
    let mut chunk_kinds = Vec::new();
    while let Some(record) = chunk_reader.read_next().unwrap() {
        chunk_kinds.push(record.kind);
        match record.samples {
            ChunkSamples::Histogram(samples) => assert_eq!(samples.len(), 1),
            ChunkSamples::ExponentialHistogram(samples) => assert_eq!(samples.len(), 1),
            ChunkSamples::Summary(samples) => assert_eq!(samples.len(), 1),
            ChunkSamples::Float(_) | ChunkSamples::Int64(_) => {
                panic!("unexpected scalar chunk in typed ingest test")
            }
        }
    }
    assert!(chunk_kinds.contains(&ChunkKind::Histogram));
    assert!(chunk_kinds.contains(&ChunkKind::ExponentialHistogram));
    assert!(chunk_kinds.contains(&ChunkKind::Summary));
}

#[test]
fn e2e_roundtrips_controlled_otlp_metrics_through_segments_and_promql() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(0),
        true,
    ));

    let mut gauge = number_dp(vec![kv_str("test.case", "roundtrip")]);
    gauge.time_unix_nano = 5_000_000_000;
    gauge.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.25));

    let mut sum = number_dp(vec![kv_str("test.case", "roundtrip")]);
    sum.time_unix_nano = 5_000_000_000;
    sum.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));

    let mut hist = histogram_dp(vec![kv_str("test.case", "roundtrip")]);
    hist.time_unix_nano = 5_000_000_000;
    hist.count = 4;
    hist.sum = Some(10.0);
    hist.min = Some(1.0);
    hist.max = Some(4.0);
    hist.explicit_bounds = vec![1.0, 5.0];
    hist.bucket_counts = vec![1, 2, 1];

    let mut exphist = exp_histogram_dp(vec![kv_str("test.case", "roundtrip")]);
    exphist.time_unix_nano = 5_000_000_000;
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

    let mut summary = summary_dp(vec![kv_str("test.case", "roundtrip")]);
    summary.time_unix_nano = 5_000_000_000;
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

    processor
        .process(
            SourceMessageMetadata {
                topic: "controlled".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 5_000,
                captured_at_ms: 5_000,
            },
            request(
                vec![kv_str("service.name", "roundtrip-suite")],
                vec![
                    metric_gauge("controlled.gauge", vec![gauge]),
                    metric_sum("controlled.sum", vec![sum]),
                    metric_histogram("controlled.histogram", vec![hist]),
                    metric_exp_histogram("controlled.exphist", vec![exphist]),
                    metric_summary("controlled.summary", vec![summary]),
                ],
            ),
        )
        .unwrap();
    processor.flush_head().unwrap();

    assert_eq!(segment_dir_count(tempdir.path()), 1);
    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        QueryProjectionConfig::default()
            .with_exponential_histogram_bucket_boundaries(vec![2.0, 4.0]),
    )
    .unwrap();

    assert_promql_samples(
        &store,
        r#"controlled.gauge{test.case="roundtrip",service.name="roundtrip-suite"}"#,
        vec![(5_000, 1.25)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.sum{test.case="roundtrip",service.name="roundtrip-suite"}"#,
        vec![(5_000, 42.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_count{test.case="roundtrip"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_sum{test.case="roundtrip"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_bucket{test.case="roundtrip",le="1"}"#,
        vec![(5_000, 1.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_bucket{test.case="roundtrip",le="5"}"#,
        vec![(5_000, 3.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_bucket{test.case="roundtrip",le="+Inf"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_count{test.case="roundtrip"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_sum{test.case="roundtrip"}"#,
        vec![(5_000, 12.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_bucket{test.case="roundtrip",le="2"}"#,
        vec![(5_000, 2.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_bucket{test.case="roundtrip",le="+Inf"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.summary_count{test.case="roundtrip"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.summary_sum{test.case="roundtrip"}"#,
        vec![(5_000, 50.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.summary{test.case="roundtrip",quantile="0.9"}"#,
        vec![(5_000, 8.0)],
    );
}

fn assert_promql_samples(store: &SegmentStoreReader, query: &str, expected: Vec<(u64, f64)>) {
    let results = store.query_promql(query, 0, 10_000).unwrap();
    assert_eq!(results.len(), 1, "query {query}");
    assert_eq!(results[0].samples, expected, "query {query}");
}

#[test]
fn processor_stamps_cumulative_histogram_reset_hints() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut first = histogram_dp(vec![kv_str("pod.name", "hist")]);
    first.start_time_unix_nano = 1_000_000_000;
    first.time_unix_nano = 5_000_000_000;
    first.count = 10;
    first.sum = Some(20.0);
    first.explicit_bounds = vec![1.0];
    first.bucket_counts = vec![4, 6];

    let mut reset = histogram_dp(vec![kv_str("pod.name", "hist")]);
    reset.start_time_unix_nano = 4_000_000_000;
    reset.time_unix_nano = 6_000_000_000;
    reset.count = 3;
    reset.sum = Some(7.0);
    reset.explicit_bounds = vec![1.0];
    reset.bucket_counts = vec![1, 2];

    let mut metric = metric_histogram("request.duration", vec![first, reset]);
    if let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) = &mut metric.data {
        histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;
    }

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_005,
            },
            request(vec![], vec![metric]),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(seg_dir).unwrap();
    let mut chunk_reader = ChunkReader::new(reader.open_chunks().unwrap());
    let record = chunk_reader.read_next().unwrap().unwrap();
    let ChunkSamples::Histogram(samples) = record.samples else {
        panic!("expected histogram chunk");
    };

    assert_eq!(samples.len(), 2);
    assert_eq!(
        samples[0].1.metadata.temporality,
        OtlpAggregationTemporality::Cumulative
    );
    assert_eq!(samples[0].1.metadata.reset_hint, CounterResetHint::Unknown);
    assert_eq!(
        samples[1].1.metadata.reset_hint,
        CounterResetHint::CounterReset
    );
}

#[test]
fn processor_flushes_bounded_late_sample_as_overlapping_segment() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(
        HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_out_of_order_time_window(Duration::from_secs(6)),
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut first = number_dp(vec![kv_str("pod.name", "backend-1")]);
    first.time_unix_nano = 15_000_000_000;
    first.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_005,
            },
            request(vec![], vec![metric_gauge("cpu.usage", vec![first])]),
        )
        .unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 0);

    let mut late = number_dp(vec![kv_str("pod.name", "backend-1")]);
    late.time_unix_nano = 9_500_000_000;
    late.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 1,
                timestamp_ms: 2_000,
                captured_at_ms: 10_006,
            },
            request(vec![], vec![metric_gauge("cpu.usage", vec![late])]),
        )
        .unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 0);

    processor.flush_head().unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 2);

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
    assert_eq!(results[0].samples, vec![(9_500, 2.0), (15_000, 1.0)]);
}
