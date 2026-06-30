use std::io::Write;
use std::time::Duration;

use chronoxide_core::labels::{DefaultSymbolTable, FlatInternedLabelSetStore, METRIC_NAME_LABEL};
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{FloatEncoding, HeadBuffer, HeadConfig, IntEncoding};
use chronoxide_core::storage::segment::{LabelMatcher, SegmentSelector};
use chronoxide_core::storage::wal::{
    OtlpWalBatch, TransportOffset, WalWriter, write_checkpoint_meta,
};
use chronoxide_core::storage::wal_replay::{
    WalReplayStopReason, replay_wal_file_into_head, replay_wal_file_into_head_from_checkpoint,
};
use opentelemetry_proto::tonic;

#[test]
fn wal_replay_rebuilds_queryable_head_from_otlp_batches() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000000.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            request: request(
                vec![kv("service.name", "checkout")],
                vec![
                    number_metric("cpu.usage", "backend-1", 5_000, NumberValue::Float(1.5)),
                    number_metric("request.count", "backend-1", 6_000, NumberValue::Int(42)),
                ],
            ),
            fallback_ts_ms: None,
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let report = replay_wal_file_into_head(&wal_path, &mut head, &mut labels).unwrap();

    assert_eq!(report.batches_replayed, 1);
    assert_eq!(report.datapoints_replayed, 2);
    assert_eq!(report.stop_reason, None);

    let cpu = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric(
                "cpu.usage",
                vec![LabelMatcher::eq("pod.name", "backend-1")],
            ),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(cpu.len(), 1);
    assert_eq!(cpu[0].samples, vec![(5_000, 1.5)]);
    assert!(cpu[0].labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.usage")
    }));

    let count = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("request.count", vec![]),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(6_000, 42.0)]);
}

#[test]
fn wal_replay_uses_checkpoint_meta_to_start_after_checkpoint_record() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000001.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            request: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "before",
                    5_000,
                    NumberValue::Float(1.0),
                )],
            ),
            fallback_ts_ms: None,
        })
        .unwrap();
    let checkpoint = writer
        .append_checkpoint(
            1_725_000_000_000,
            vec![TransportOffset {
                topic: "metrics".to_string(),
                partition: 0,
                next_offset: 10,
            }],
        )
        .unwrap();
    write_checkpoint_meta(tempdir.path(), &checkpoint).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            request: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "after",
                    6_000,
                    NumberValue::Float(2.0),
                )],
            ),
            fallback_ts_ms: None,
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let report = replay_wal_file_into_head_from_checkpoint(
        &wal_path,
        tempdir.path(),
        &mut head,
        &mut labels,
    )
    .unwrap();

    assert_eq!(report.checkpoint_lsn, Some(checkpoint.wal_lsn));
    assert_eq!(report.batches_replayed, 1);

    let results = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("cpu.usage", vec![]),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(6_000, 2.0)]);
    assert!(
        results[0]
            .labels
            .iter()
            .any(|(key, value)| key == &normalize_label_name("pod.name") && value == "after")
    );
}

#[test]
fn wal_replay_stops_at_first_invalid_record_and_keeps_prior_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-000002.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            request: request(
                vec![],
                vec![number_metric(
                    "cpu.usage",
                    "backend-1",
                    5_000,
                    NumberValue::Float(1.5),
                )],
            ),
            fallback_ts_ms: None,
        })
        .unwrap();
    let invalid_lsn = writer.current_offset().unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    file.write_all(b"torn").unwrap();
    drop(file);

    let mut head = test_head();
    let mut labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let report = replay_wal_file_into_head(&wal_path, &mut head, &mut labels).unwrap();

    assert_eq!(report.batches_replayed, 1);
    assert_eq!(report.stopped_at_lsn, Some(invalid_lsn));
    assert_eq!(report.stop_reason, Some(WalReplayStopReason::UnexpectedEof));

    let results = head
        .query_selector(
            &labels,
            &SegmentSelector::with_metric("cpu.usage", vec![]),
            0,
            10_000,
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 1.5)]);
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

#[derive(Clone, Copy)]
enum NumberValue {
    Float(f64),
    Int(i64),
}

fn number_metric(
    metric_name: &str,
    pod_name: &str,
    timestamp_ms: u64,
    value: NumberValue,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: metric_name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Gauge(
            tonic::metrics::v1::Gauge {
                data_points: vec![tonic::metrics::v1::NumberDataPoint {
                    attributes: vec![kv("pod.name", pod_name)],
                    time_unix_nano: timestamp_ms * 1_000_000,
                    value: Some(match value {
                        NumberValue::Float(value) => {
                            tonic::metrics::v1::number_data_point::Value::AsDouble(value)
                        }
                        NumberValue::Int(value) => {
                            tonic::metrics::v1::number_data_point::Value::AsInt(value)
                        }
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn request(
    resource_attrs: Vec<tonic::common::v1::KeyValue>,
    metrics: Vec<tonic::metrics::v1::Metric>,
) -> tonic::collector::metrics::v1::ExportMetricsServiceRequest {
    tonic::collector::metrics::v1::ExportMetricsServiceRequest {
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

fn kv(key: &str, value: &str) -> tonic::common::v1::KeyValue {
    tonic::common::v1::KeyValue {
        key: key.to_string(),
        value: Some(tonic::common::v1::AnyValue {
            value: Some(tonic::common::v1::any_value::Value::StringValue(
                value.to_string(),
            )),
        }),
    }
}
