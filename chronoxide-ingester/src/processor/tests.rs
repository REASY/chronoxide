use super::*;
use crate::app_config::LabelSetStoreKind;
use crate::source::SourceMessageMetadata;
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, LabelSetStore, METRIC_NAME_LABEL,
};
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::chunk::{ChunkKind, ChunkReader, ChunkSamples, read_chunk_index};
use chronoxide_core::storage::head::{
    CounterResetHint, HeadConfig, IntEncoding, OtlpAggregationTemporality, SeriesSamples,
    prometheus_stale_nan,
};
use chronoxide_core::storage::index::read_segment_indexes;
use chronoxide_core::storage::live_coverage::{
    CoverageLedger, MessageSequence, RecordedSampleOrder,
};
use chronoxide_core::storage::live_view::{
    LiveQueryHandle, LiveQueryPin, LiveReadiness, LiveStorageView,
};
use chronoxide_core::storage::segment::{
    QueryProjectionConfig, SegmentFile, SegmentMeta, SegmentSelector, SegmentStorageSchema,
    SegmentStoreReader, SegmentWriterConfig,
};
use chronoxide_core::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_HISTOGRAM, SERIES_KIND_SUMMARY, read_series_bin,
    read_symbols_bin,
};
use chronoxide_core::storage::wal::{OtlpWalBatch, WalWriter};
use chronoxide_core::storage::wal_replay::replay_wal_file_into_head;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, exponential_histogram_data_point::Buckets,
    summary_data_point::ValueAtQuantile,
};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;

fn kv_any(key: &str, value: tonic::common::v1::any_value::Value) -> tonic::common::v1::KeyValue {
    tonic::common::v1::KeyValue {
        key: key.to_string(),
        value: Some(tonic::common::v1::AnyValue { value: Some(value) }),
        key_strindex: 0,
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
            tonic::metrics::v1::Gauge { data_points: dps },
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
            tonic::metrics::v1::Summary { data_points: dps },
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
    }
}

fn tracked_metadata(offset: i64, captured_at_ms: i64) -> SourceMessageMetadata {
    source_metadata("tracked", 3, offset, captured_at_ms)
}

fn source_metadata(
    topic: &str,
    partition: i32,
    offset: i64,
    captured_at_ms: i64,
) -> SourceMessageMetadata {
    SourceMessageMetadata {
        topic: topic.to_string(),
        partition,
        offset,
        timestamp_ms: captured_at_ms,
        captured_at_ms,
    }
}

fn live_test_processor(
    segments_dir: &Path,
    out_of_order_window: Duration,
    publish_interval: Duration,
) -> (OtlpLabelSetProcessor, Arc<LiveQueryHandle<LiveStorageView>>) {
    let window_duration = Duration::from_secs(10);
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(segments_dir, window_duration)
            .with_storage_schema(SegmentStorageSchema::Schema8)
            .with_deterministic_segment_ids(0x1_1e),
    )
    .unwrap();
    let head = HeadConfig::new(
        window_duration,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(out_of_order_window);
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);
    let handle = processor
        .enable_live_publication(LivePublisherConfig {
            publish_interval,
            max_view_staleness: Duration::from_secs(120),
            memory_admission_bytes: 64 * 1024 * 1024,
        })
        .unwrap();
    (processor, handle)
}

fn merge_frozen_partition_coverage(
    processor: &mut OtlpLabelSetProcessor,
) -> (
    CoverageLedger,
    Option<(RecordedSampleOrder, RecordedSampleOrder)>,
) {
    let partition = PartitionKey::new("tracked", 3);
    let fragments = processor
        .partition_heads
        .get_mut(&partition)
        .unwrap()
        .head
        .try_freeze_for_publication()
        .unwrap();
    let coverage = fragments
        .iter()
        .try_fold(CoverageLedger::empty(), |coverage, fragment| {
            coverage.checked_merge(fragment.coverage())
        })
        .unwrap();
    let bounds = fragments
        .iter()
        .filter_map(|fragment| fragment.recorded_order_range())
        .fold(
            None::<(RecordedSampleOrder, RecordedSampleOrder)>,
            |bounds, range| {
                Some(match bounds {
                    None => (range.first(), range.last()),
                    Some((first, last)) => (first.min(range.first()), last.max(range.last())),
                })
            },
        );
    (coverage, bounds)
}

#[test]
fn live_coverage_counts_only_successful_records_and_zero_messages_advance_the_cut() {
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(10));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        None,
    )
    .with_shutdown_report(false);
    processor.enable_live_coverage_tracking().unwrap();

    let mut valid_float = number_dp(vec![kv_str("sample", "first")]);
    valid_float.time_unix_nano = 2_000_000_000;
    valid_float.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
        f64::from_bits(0x7ff8_0000_0000_0042),
    ));
    let mut missing_value = number_dp(vec![kv_str("sample", "missing-value")]);
    missing_value.time_unix_nano = 2_100_000_000;
    missing_value.value = None;
    let mut missing_timestamp = number_dp(vec![kv_str("sample", "missing-time")]);
    missing_timestamp.time_unix_nano = 0;
    missing_timestamp.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(9));
    let mut valid_int = number_dp(vec![kv_str("sample", "last")]);
    valid_int.time_unix_nano = 2_200_000_000;
    valid_int.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(
        i64::MIN,
    ));

    Processor::begin_acquired_message(&mut processor, MessageSequence::new(10)).unwrap();
    Processor::process(
        &mut processor,
        tracked_metadata(1, 2_200),
        request(
            vec![],
            vec![
                metric_gauge(
                    "tracked.gauge",
                    vec![valid_float, missing_value, missing_timestamp],
                ),
                metric_gauge("tracked.int", vec![valid_int]),
            ],
        ),
    )
    .unwrap();
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(10)).unwrap();

    Processor::begin_acquired_message(&mut processor, MessageSequence::new(11)).unwrap();
    Processor::process(
        &mut processor,
        tracked_metadata(2, 2_201),
        ExportMetricsServiceRequest::default(),
    )
    .unwrap();
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(11)).unwrap();

    let first = processor.pop_completed_message_coverage().unwrap();
    let empty = processor.pop_completed_message_coverage().unwrap();
    assert_eq!(first.message_sequence.get(), 10);
    assert_eq!(first.coverage.sample_count(), 2);
    assert_eq!(first.successful_orders.sample_count(), 2);
    assert_eq!(first.successful_orders.run_count(), 2);
    assert_eq!(
        first.successful_orders.runs()[0].first().sample_ordinal(),
        0
    );
    assert_eq!(
        first.successful_orders.runs()[1].first().sample_ordinal(),
        3
    );
    assert_eq!(empty.message_sequence.get(), 11);
    assert_eq!(empty.coverage, CoverageLedger::empty());
    assert!(empty.successful_orders.is_empty());
    assert_eq!(empty.completed_prefix, first.completed_prefix);
    assert!(processor.pop_completed_message_coverage().is_none());

    let (head_coverage, recorded_bounds) = merge_frozen_partition_coverage(&mut processor);
    let (first_recorded, last_recorded) = recorded_bounds.unwrap();
    assert_eq!(first_recorded.sample_ordinal(), 0);
    assert_eq!(last_recorded.sample_ordinal(), 3);
    assert_eq!(head_coverage, first.completed_prefix);
}

#[test]
fn completed_coverage_capacity_is_reserved_before_a_message_can_mutate_state() {
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        None,
    )
    .with_shutdown_report(false);
    processor.enable_live_coverage_tracking().unwrap();
    processor
        .live_coverage
        .as_mut()
        .unwrap()
        .fail_next_completed_reserve = true;

    let error =
        Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap_err();
    let crate::error::ErrorKind::IoError(error) = error.kind() else {
        panic!("expected an injected I/O allocation failure");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory);
    let tracking = processor.live_coverage.as_ref().unwrap();
    assert!(tracking.active.is_none());
    assert!(tracking.completed.is_empty());
    assert!(tracking.last_completed.is_none());
    assert!(processor.partition_heads.is_empty());

    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    Processor::process(
        &mut processor,
        tracked_metadata(1, 1_000),
        ExportMetricsServiceRequest::default(),
    )
    .unwrap();
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    let completed = processor.pop_completed_message_coverage().unwrap();
    assert_eq!(completed.message_sequence, MessageSequence::new(1));
    assert_eq!(completed.coverage, CoverageLedger::empty());
}

#[test]
fn pristine_coverage_only_mode_can_upgrade_to_live_publication() {
    let root = tempfile::tempdir().unwrap();
    let writer_config = SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema8)
        .with_deterministic_segment_ids(0xc0_0e);
    let writer = SegmentWriter::new(writer_config.clone()).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    processor.enable_live_coverage_tracking().unwrap();
    let handle = processor
        .enable_live_publication(LivePublisherConfig {
            publish_interval: Duration::from_secs(60),
            max_view_staleness: Duration::from_secs(120),
            memory_admission_bytes: 64 * 1024 * 1024,
        })
        .unwrap();
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::VersionedFlatInterned(_)
    ));

    let mut sample = number_dp(vec![kv_str("pod", "upgrade")]);
    sample.time_unix_nano = 1_000_000_000;
    sample.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(7.0));
    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    Processor::process(
        &mut processor,
        tracked_metadata(1, 1_000),
        request(vec![], vec![metric_gauge("coverage_upgrade", vec![sample])]),
    )
    .unwrap();
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    let pin = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(
        query_live_gauge(&pin, "coverage_upgrade", 0, 10_000),
        vec![(1_000, 7.0)]
    );
}

#[test]
fn observed_coverage_only_mode_rejects_late_live_publication_atomically() {
    let root = tempfile::tempdir().unwrap();
    let writer_config = SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema8);
    let writer = SegmentWriter::new(writer_config.clone()).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    processor.enable_live_coverage_tracking().unwrap();
    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    Processor::process(
        &mut processor,
        tracked_metadata(1, 1_000),
        ExportMetricsServiceRequest::default(),
    )
    .unwrap();
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();

    let error = match processor.enable_live_publication(LivePublisherConfig {
        publish_interval: Duration::from_secs(60),
        max_view_staleness: Duration::from_secs(120),
        memory_admission_bytes: 64 * 1024 * 1024,
    }) {
        Ok(_) => panic!("late live publication unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("must be enabled before processing messages")
    );
    assert!(processor.segment_writer.is_some());
    assert!(processor.live_publisher.is_none());
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::FlatInterned(_)
    ));
}

#[test]
fn observed_versioned_coverage_mode_rejects_late_live_publication_atomically() {
    let root = tempfile::tempdir().unwrap();
    let writer_config = SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema8);
    let writer = SegmentWriter::new(writer_config.clone()).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    processor.enable_live_query_mode().unwrap();
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::VersionedFlatInterned(_)
    ));
    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    Processor::process(
        &mut processor,
        tracked_metadata(1, 1_000),
        ExportMetricsServiceRequest::default(),
    )
    .unwrap();
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();

    let error = match processor.enable_live_publication(LivePublisherConfig {
        publish_interval: Duration::from_secs(60),
        max_view_staleness: Duration::from_secs(120),
        memory_admission_bytes: 64 * 1024 * 1024,
    }) {
        Ok(_) => panic!("late Versioned live publication unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("must be enabled before processing messages")
    );
    assert!(processor.segment_writer.is_some());
    assert!(processor.live_publisher.is_none());
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::VersionedFlatInterned(_)
    ));
    assert_eq!(processor.live_coverage.as_ref().unwrap().completed.len(), 1);
}

#[test]
fn observed_versioned_mode_rejects_reenable_without_erasing_coverage() {
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        None,
    )
    .with_shutdown_report(false);

    processor.enable_live_query_mode().unwrap();
    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    Processor::process(
        &mut processor,
        tracked_metadata(1, 1_000),
        ExportMetricsServiceRequest::default(),
    )
    .unwrap();
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();

    let error = processor.enable_live_query_mode().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("must be enabled before processing messages")
    );
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::VersionedFlatInterned(_)
    ));
    let tracking = processor.live_coverage.as_ref().unwrap();
    assert_eq!(tracking.completed.len(), 1);
    assert_eq!(tracking.last_completed, Some(MessageSequence::new(1)));
}

#[test]
fn live_publication_rejects_non_schema8_writer_without_mutating_processor() {
    let root = tempfile::tempdir().unwrap();
    let writer_config = SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let writer = SegmentWriter::new(writer_config).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    let error = match processor.enable_live_publication(LivePublisherConfig {
        publish_interval: Duration::from_secs(1),
        max_view_staleness: Duration::from_secs(10),
        memory_admission_bytes: 64 * 1024 * 1024,
    }) {
        Ok(_) => panic!("non-Schema-8 live publication unexpectedly succeeded"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("Schema 8"));
    assert!(processor.live_publisher.is_none());
    assert!(processor.live_coverage.is_none());
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::FlatInterned(_)
    ));
    assert_eq!(
        processor
            .segment_writer
            .as_ref()
            .unwrap()
            .pristine_config_for_takeover()
            .unwrap()
            .storage_schema(),
        SegmentStorageSchema::Schema7
    );
}

#[test]
fn live_publication_rejects_head_writer_duration_mismatch_atomically() {
    let root = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema8),
    )
    .unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(11),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    let error = match processor.enable_live_publication(LivePublisherConfig {
        publish_interval: Duration::from_secs(1),
        max_view_staleness: Duration::from_secs(10),
        memory_admission_bytes: 64 * 1024 * 1024,
    }) {
        Ok(_) => panic!("duration-mismatched live publication unexpectedly succeeded"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("must equal segment writer duration")
    );
    assert!(processor.live_publisher.is_none());
    assert!(processor.live_coverage.is_none());
    assert!(processor.segment_writer.is_some());
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::FlatInterned(_)
    ));
}

#[test]
fn live_publication_rejects_non_pristine_writer_without_taking_ownership() {
    let root = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema8),
    )
    .unwrap();
    writer.record_sample(SeriesRef::new(7), 1_000, 1.0).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    let error = match processor.enable_live_publication(LivePublisherConfig {
        publish_interval: Duration::from_secs(1),
        max_view_staleness: Duration::from_secs(10),
        memory_admission_bytes: 64 * 1024 * 1024,
    }) {
        Ok(_) => panic!("non-pristine writer takeover unexpectedly succeeded"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("requires a pristine writer"));
    assert!(processor.live_publisher.is_none());
    assert!(processor.live_coverage.is_none());
    assert!(
        processor
            .segment_writer
            .as_ref()
            .unwrap()
            .pristine_config_for_takeover()
            .is_err()
    );
    assert!(matches!(
        &processor.labelsets,
        LabelSetInterner::FlatInterned(_)
    ));
}

#[test]
fn accepted_prefix_error_reinserts_head_and_finalizes_its_exact_coverage() {
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_millis(500));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        None,
    )
    .with_shutdown_report(false);
    processor.enable_live_coverage_tracking().unwrap();

    let mut accepted = number_dp(vec![kv_str("sample", "same-series")]);
    accepted.time_unix_nano = 5_000_000_000;
    accepted.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut failing_suffix = number_dp(vec![kv_str("sample", "same-series")]);
    failing_suffix.time_unix_nano = 1_000_000_000;
    failing_suffix.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));

    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    let processing_error = Processor::process(
        &mut processor,
        tracked_metadata(1, 5_000),
        request(
            vec![],
            vec![metric_gauge(
                "tracked.prefix",
                vec![accepted, failing_suffix],
            )],
        ),
    )
    .unwrap_err();
    assert!(
        processing_error
            .to_string()
            .contains("out_of_order_time_window")
    );
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();

    let completed = processor.pop_completed_message_coverage().unwrap();
    assert_eq!(completed.coverage.sample_count(), 1);
    assert_eq!(
        merge_frozen_partition_coverage(&mut processor).0,
        completed.coverage
    );
}

#[test]
fn accepted_prefix_error_is_published_and_queryable_at_the_completed_boundary() {
    let root = tempfile::tempdir().unwrap();
    let (mut processor, handle) = live_test_processor(
        root.path(),
        Duration::from_millis(500),
        Duration::from_secs(60),
    );

    let mut accepted = number_dp(vec![kv_str("sample", "same-series")]);
    accepted.time_unix_nano = 5_000_000_000;
    accepted.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut failing_suffix = number_dp(vec![kv_str("sample", "same-series")]);
    failing_suffix.time_unix_nano = 1_000_000_000;
    failing_suffix.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));

    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    let processing_error = Processor::process(
        &mut processor,
        tracked_metadata(1, 5_000),
        request(
            vec![],
            vec![metric_gauge(
                "tracked.prefix.query",
                vec![accepted, failing_suffix],
            )],
        ),
    )
    .unwrap_err();
    assert!(
        processing_error
            .to_string()
            .contains("out_of_order_time_window"),
        "the caller must still receive the suffix error: {processing_error}"
    );
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();

    let pin = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(pin.generation(), 1);
    assert_eq!(pin.visible_message_sequence(), 1);
    assert_eq!(
        query_live_gauge(&pin, "tracked.prefix.query", 0, 10_000),
        vec![(5_000, 1.0)],
        "the successfully recorded prefix must publish without inventing the failing suffix"
    );
}

#[test]
fn every_zero_record_message_has_empty_coverage_and_advances_its_completed_cut() {
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        None,
    )
    .with_shutdown_report(false);
    processor.enable_live_coverage_tracking().unwrap();

    let mut rejected = number_dp(vec![kv_str("case", "rejected")]);
    rejected.time_unix_nano = 0;
    rejected.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(7));

    let mut missing_number = number_dp(vec![kv_str("case", "missing-number")]);
    missing_number.time_unix_nano = 2_000_000_000;
    missing_number.value = None;

    let mut invalid_typed = histogram_dp(vec![kv_str("case", "typed-invalid")]);
    invalid_typed.time_unix_nano = 3_000_000_000;
    invalid_typed.count = u64::MAX;
    invalid_typed.explicit_bounds = vec![1.0];
    invalid_typed.bucket_counts = vec![u64::MAX, 1];

    for (sequence, expected_result, message) in [
        (1, ProcessResult::Ok, ExportMetricsServiceRequest::default()),
        (
            2,
            ProcessResult::DroppedOutdated,
            request(
                vec![],
                vec![metric_gauge("zero.record.rejected", vec![rejected])],
            ),
        ),
        (
            3,
            ProcessResult::Ok,
            request(
                vec![],
                vec![metric_gauge("zero.record.missing", vec![missing_number])],
            ),
        ),
        (
            4,
            ProcessResult::Ok,
            request(
                vec![],
                vec![metric_histogram("zero.record.invalid", vec![invalid_typed])],
            ),
        ),
    ] {
        let sequence = MessageSequence::new(sequence);
        Processor::begin_acquired_message(&mut processor, sequence).unwrap();
        assert_eq!(
            Processor::process(
                &mut processor,
                tracked_metadata(i64::try_from(sequence.get()).unwrap(), 4_000),
                message,
            )
            .unwrap(),
            expected_result
        );
        Processor::complete_acquired_message(&mut processor, sequence).unwrap();
        let completed = processor.pop_completed_message_coverage().unwrap();
        assert_eq!(completed.message_sequence, sequence);
        assert_eq!(completed.coverage, CoverageLedger::empty());
        assert!(completed.successful_orders.is_empty());
        assert_eq!(completed.completed_prefix, CoverageLedger::empty());
    }

    assert!(processor.partition_heads.values_mut().all(|partition| {
        partition
            .head
            .try_freeze_for_publication()
            .unwrap()
            .is_empty()
    }));
    assert!(processor.pop_completed_message_coverage().is_none());
}

#[derive(Clone, Copy, Debug)]
enum ResetTransactionKind {
    Histogram,
    ExponentialHistogram,
}

fn cumulative_reset_transaction_metric(
    kind: ResetTransactionKind,
    points: &[(u64, u64)],
) -> tonic::metrics::v1::Metric {
    match kind {
        ResetTransactionKind::Histogram => {
            let data_points = points
                .iter()
                .map(|&(timestamp_ms, count)| {
                    let mut point = histogram_dp(vec![kv_str("sample", "same-series")]);
                    point.start_time_unix_nano = 500_000_000;
                    point.time_unix_nano = timestamp_ms * 1_000_000;
                    point.count = count;
                    point.sum = Some(count as f64);
                    point.explicit_bounds = vec![1.0];
                    point.bucket_counts = vec![count, 0];
                    point
                })
                .collect();
            let mut metric = metric_histogram("transactional.reset", data_points);
            let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) = metric.data.as_mut()
            else {
                unreachable!("histogram helper returned another metric kind");
            };
            histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;
            metric
        }
        ResetTransactionKind::ExponentialHistogram => {
            let data_points = points
                .iter()
                .map(|&(timestamp_ms, count)| {
                    let mut point = exp_histogram_dp(vec![kv_str("sample", "same-series")]);
                    point.start_time_unix_nano = 500_000_000;
                    point.time_unix_nano = timestamp_ms * 1_000_000;
                    point.count = count;
                    point.sum = Some(count as f64);
                    point.positive = Some(Buckets {
                        offset: 0,
                        bucket_counts: vec![count],
                    });
                    point
                })
                .collect();
            let mut metric = metric_exp_histogram("transactional.reset", data_points);
            let Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) =
                metric.data.as_mut()
            else {
                unreachable!("exponential-histogram helper returned another metric kind");
            };
            histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;
            metric
        }
    }
}

fn run_accepted_prefix_reset_transaction(
    kind: ResetTransactionKind,
    live_coverage: bool,
) -> Vec<(u64, CounterResetHint)> {
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_millis(500));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        None,
    )
    .with_shutdown_report(false);
    if live_coverage {
        processor.enable_live_coverage_tracking().unwrap();
        Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    }

    let processing_error = Processor::process(
        &mut processor,
        tracked_metadata(1, 5_000),
        request(
            vec![],
            vec![cumulative_reset_transaction_metric(
                kind,
                &[(5_000, 100), (1_000, 200)],
            )],
        ),
    )
    .unwrap_err();
    assert!(
        processing_error
            .to_string()
            .contains("out_of_order_time_window")
    );
    if live_coverage {
        Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
        let completed = processor.pop_completed_message_coverage().unwrap();
        assert_eq!(completed.coverage.sample_count(), 1);
        assert_eq!(completed.successful_orders.sample_count(), 1);
    }

    if live_coverage {
        Processor::begin_acquired_message(&mut processor, MessageSequence::new(2)).unwrap();
    }
    assert_eq!(
        Processor::process(
            &mut processor,
            tracked_metadata(2, 6_000),
            request(
                vec![],
                vec![cumulative_reset_transaction_metric(kind, &[(6_000, 150)])],
            ),
        )
        .unwrap(),
        ProcessResult::Ok
    );
    if live_coverage {
        Processor::complete_acquired_message(&mut processor, MessageSequence::new(2)).unwrap();
        let completed = processor.pop_completed_message_coverage().unwrap();
        assert_eq!(completed.coverage.sample_count(), 1);
        assert_eq!(completed.completed_prefix.sample_count(), 2);
    }

    let window = processor
        .partition_heads
        .get_mut(&PartitionKey::new("tracked", 3))
        .unwrap()
        .head
        .drain()
        .expect("the two stored samples remain in the active window");
    let mut series_samples = window.into_series_samples().unwrap();
    assert_eq!(series_samples.len(), 1);
    match (kind, series_samples.pop().unwrap().1) {
        (ResetTransactionKind::Histogram, SeriesSamples::Histogram { samples }) => samples
            .into_iter()
            .map(|(timestamp_ms, value)| (timestamp_ms, value.metadata.reset_hint))
            .collect(),
        (
            ResetTransactionKind::ExponentialHistogram,
            SeriesSamples::ExponentialHistogram { samples },
        ) => samples
            .into_iter()
            .map(|(timestamp_ms, value)| (timestamp_ms, value.metadata.reset_hint))
            .collect(),
        (_, samples) => panic!("unexpected samples for {kind:?}: {samples:?}"),
    }
}

#[test]
fn accepted_prefix_histogram_failure_does_not_advance_reset_history() {
    let expected = vec![
        (5_000, CounterResetHint::Unknown),
        (6_000, CounterResetHint::NotCounterReset),
    ];
    assert_eq!(
        run_accepted_prefix_reset_transaction(ResetTransactionKind::Histogram, false),
        expected
    );
    assert_eq!(
        run_accepted_prefix_reset_transaction(ResetTransactionKind::Histogram, true),
        expected,
        "live exact-coverage tracking must not alter stored reset semantics"
    );
}

#[test]
fn accepted_prefix_exponential_histogram_failure_does_not_advance_reset_history() {
    let expected = vec![
        (5_000, CounterResetHint::Unknown),
        (6_000, CounterResetHint::NotCounterReset),
    ];
    assert_eq!(
        run_accepted_prefix_reset_transaction(ResetTransactionKind::ExponentialHistogram, false),
        expected
    );
    assert_eq!(
        run_accepted_prefix_reset_transaction(ResetTransactionKind::ExponentialHistogram, true),
        expected,
        "live exact-coverage tracking must not alter stored reset semantics"
    );
}

#[test]
fn rejected_sample_kind_does_not_advance_native_reset_history() {
    for rejected_kind in [
        ResetTransactionKind::Histogram,
        ResetTransactionKind::ExponentialHistogram,
    ] {
        let head = HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        );
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            Some(head),
            None,
        )
        .with_shutdown_report(false);
        let resident_kind = match rejected_kind {
            ResetTransactionKind::Histogram => ResetTransactionKind::ExponentialHistogram,
            ResetTransactionKind::ExponentialHistogram => ResetTransactionKind::Histogram,
        };

        for (offset, kind, timestamp_ms, count) in [
            (0, resident_kind, 5_000, 100),
            (1, rejected_kind, 6_000, 200),
        ] {
            assert_eq!(
                Processor::process(
                    &mut processor,
                    tracked_metadata(offset, timestamp_ms as i64),
                    request(
                        vec![],
                        vec![cumulative_reset_transaction_metric(
                            kind,
                            &[(timestamp_ms, count)],
                        )],
                    ),
                )
                .unwrap(),
                ProcessResult::Ok
            );
        }

        let resident = processor
            .partition_heads
            .get_mut(&PartitionKey::new("tracked", 3))
            .unwrap()
            .head
            .drain()
            .expect("the resident sample creates a window")
            .into_series_samples()
            .unwrap();
        assert_eq!(resident.len(), 1, "the mismatched kind was not recorded");

        Processor::process(
            &mut processor,
            tracked_metadata(2, 7_000),
            request(
                vec![],
                vec![cumulative_reset_transaction_metric(
                    rejected_kind,
                    &[(7_000, 150)],
                )],
            ),
        )
        .unwrap();
        let mut samples = processor
            .partition_heads
            .get_mut(&PartitionKey::new("tracked", 3))
            .unwrap()
            .head
            .drain()
            .expect("the later sample is accepted after draining the conflicting window")
            .into_series_samples()
            .unwrap();
        assert_eq!(samples.len(), 1);
        let hint = match (rejected_kind, samples.pop().unwrap().1) {
            (ResetTransactionKind::Histogram, SeriesSamples::Histogram { samples }) => {
                assert_eq!(samples.len(), 1);
                samples[0].1.metadata.reset_hint
            }
            (
                ResetTransactionKind::ExponentialHistogram,
                SeriesSamples::ExponentialHistogram { samples },
            ) => {
                assert_eq!(samples.len(), 1);
                samples[0].1.metadata.reset_hint
            }
            (_, samples) => panic!("unexpected samples for {rejected_kind:?}: {samples:?}"),
        };
        assert_eq!(
            hint,
            CounterResetHint::Unknown,
            "a rejected {rejected_kind:?} sample must not become reset history"
        );
    }
}

fn query_live_gauge(
    pin: &LiveQueryPin<LiveStorageView>,
    metric: &str,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    let mut session = pin
        .payload()
        .sealed()
        .query_session_with_head_view(pin.payload().head())
        .unwrap();
    let mut results = session
        .query_selector(&SegmentSelector::metric(metric), start_ms, end_ms)
        .unwrap();
    assert_eq!(results.len(), 1, "expected exactly one live gauge series");
    results.pop().unwrap().samples
}

fn process_completed_live_message(
    processor: &mut OtlpLabelSetProcessor,
    sequence: u64,
    metadata: SourceMessageMetadata,
    message: ExportMetricsServiceRequest,
) -> ProcessResult {
    let sequence = MessageSequence::new(sequence);
    Processor::begin_acquired_message(processor, sequence).unwrap();
    let result = Processor::process(processor, metadata, message).unwrap();
    Processor::complete_acquired_message(processor, sequence).unwrap();
    result
}

#[test]
fn zero_record_messages_between_samples_advance_the_live_cut_without_inventing_data() {
    let root = tempfile::tempdir().unwrap();
    let (mut processor, handle) =
        live_test_processor(root.path(), Duration::ZERO, Duration::from_secs(60));

    let mut first = number_dp(vec![kv_str("series", "one")]);
    first.time_unix_nano = 1_000_000_000;
    first.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    assert_eq!(
        process_completed_live_message(
            &mut processor,
            1,
            tracked_metadata(1, 1_000),
            request(vec![], vec![metric_gauge("zero.record.live", vec![first])],),
        ),
        ProcessResult::Ok
    );
    let first_pin = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(first_pin.visible_message_sequence(), 1);
    assert_eq!(
        query_live_gauge(&first_pin, "zero.record.live", 0, 20_000),
        vec![(1_000, 1.0)]
    );
    drop(first_pin);

    let mut rejected = number_dp(vec![kv_str("case", "rejected")]);
    rejected.time_unix_nano = 0;
    rejected.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(7));
    let mut missing_number = number_dp(vec![kv_str("case", "missing-number")]);
    missing_number.time_unix_nano = 2_000_000_000;
    missing_number.value = None;
    let mut invalid_typed = histogram_dp(vec![kv_str("case", "typed-invalid")]);
    invalid_typed.time_unix_nano = 3_000_000_000;
    invalid_typed.count = u64::MAX;
    invalid_typed.explicit_bounds = vec![1.0];
    invalid_typed.bucket_counts = vec![u64::MAX, 1];

    for (sequence, expected_result, message) in [
        (2, ProcessResult::Ok, ExportMetricsServiceRequest::default()),
        (
            3,
            ProcessResult::DroppedOutdated,
            request(
                vec![],
                vec![metric_gauge("zero.record.rejected", vec![rejected])],
            ),
        ),
        (
            4,
            ProcessResult::Ok,
            request(
                vec![],
                vec![metric_gauge("zero.record.missing", vec![missing_number])],
            ),
        ),
        (
            5,
            ProcessResult::Ok,
            request(
                vec![],
                vec![metric_histogram("zero.record.invalid", vec![invalid_typed])],
            ),
        ),
    ] {
        assert_eq!(
            process_completed_live_message(
                &mut processor,
                sequence,
                tracked_metadata(i64::try_from(sequence).unwrap(), 5_000),
                message,
            ),
            expected_result
        );
        let status = handle.status().unwrap();
        assert_eq!(
            status.generation,
            Some(1),
            "the 60-second policy should coalesce each zero-record boundary"
        );
        assert!(matches!(status.readiness, LiveReadiness::DirtySince(_)));
    }

    let mut last = number_dp(vec![kv_str("series", "one")]);
    last.time_unix_nano = 11_000_000_000;
    last.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    assert_eq!(
        process_completed_live_message(
            &mut processor,
            6,
            tracked_metadata(6, 11_000),
            request(vec![], vec![metric_gauge("zero.record.live", vec![last])],),
        ),
        ProcessResult::Ok
    );

    let final_pin = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(final_pin.generation(), 2);
    assert_eq!(
        final_pin.visible_message_sequence(),
        6,
        "all zero-record message boundaries must advance the published cut"
    );
    assert_eq!(
        query_live_gauge(&final_pin, "zero.record.live", 0, 20_000),
        vec![(1_000, 1.0), (11_000, 2.0)]
    );
    let session = final_pin
        .payload()
        .sealed()
        .query_session_with_head_view(final_pin.payload().head())
        .unwrap();
    assert_eq!(
        session.metric_names(0, 20_000).unwrap(),
        vec![normalize_metric_name("zero.record.live")],
        "rejected, missing-value, and typed-invalid work must create no visible series"
    );
}

#[test]
fn disjoint_series_from_multiple_partitions_share_one_live_query_view() {
    let root = tempfile::tempdir().unwrap();
    let (mut processor, handle) =
        live_test_processor(root.path(), Duration::ZERO, Duration::from_nanos(1));

    for (sequence, topic, partition, metric, value) in [
        (1, "topic-a", 7, "partition.alpha", 1.0),
        (2, "topic-b", 7, "partition.beta", 2.0),
        (3, "topic-c", 9, "partition.gamma", 3.0),
    ] {
        let mut sample = number_dp(vec![kv_str("source", topic)]);
        sample.time_unix_nano = sequence * 1_000_000_000;
        sample.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
            value,
        ));
        assert_eq!(
            process_completed_live_message(
                &mut processor,
                sequence,
                source_metadata(topic, partition, i64::try_from(sequence).unwrap(), 5_000),
                request(vec![], vec![metric_gauge(metric, vec![sample])]),
            ),
            ProcessResult::Ok
        );
    }

    let pin = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(pin.visible_message_sequence(), 3);
    assert_eq!(
        query_live_gauge(&pin, "partition.alpha", 0, 10_000),
        vec![(1_000, 1.0)]
    );
    assert_eq!(
        query_live_gauge(&pin, "partition.beta", 0, 10_000),
        vec![(2_000, 2.0)]
    );
    assert_eq!(
        query_live_gauge(&pin, "partition.gamma", 0, 10_000),
        vec![(3_000, 3.0)]
    );
    assert_eq!(
        pin.payload()
            .head()
            .samples()
            .metric_names(pin.payload().head().labels().as_ref(), 0, 10_000)
            .unwrap(),
        vec![
            normalize_metric_name("partition.alpha"),
            normalize_metric_name("partition.beta"),
            normalize_metric_name("partition.gamma"),
        ]
    );
}

#[test]
fn equal_numeric_partitions_in_different_topics_remain_distinct_live_owners() {
    let root = tempfile::tempdir().unwrap();
    let (mut processor, handle) =
        live_test_processor(root.path(), Duration::ZERO, Duration::from_nanos(1));

    for (sequence, topic, value) in [(1, "topic-a", 1.0), (2, "topic-b", 2.0)] {
        let mut sample = number_dp(vec![kv_str("series", "same")]);
        sample.time_unix_nano = sequence * 1_000_000_000;
        sample.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
            value,
        ));
        assert_eq!(
            process_completed_live_message(
                &mut processor,
                sequence,
                source_metadata(topic, 7, i64::try_from(sequence).unwrap(), 5_000),
                request(
                    vec![],
                    vec![metric_gauge("partition.same-owner", vec![sample])],
                ),
            ),
            ProcessResult::Ok
        );
    }

    let status = handle.status().unwrap();
    assert_eq!(status.generation, Some(1));
    let LiveReadiness::Failed(error) = status.readiness else {
        panic!(
            "the same canonical series in topic-a:7 and topic-b:7 must be recognized as two active owners"
        );
    };
    assert!(
        error.to_string().contains("simultaneously owned"),
        "unexpected owner-conflict error: {error}"
    );
    assert!(
        handle.try_pin_admitted(Instant::now()).is_err(),
        "new readers must fail closed after the distinct-owner conflict"
    );
}

#[test]
fn ownership_transfers_after_handoff_while_an_old_generation_is_pinned() {
    let root = tempfile::tempdir().unwrap();
    let (mut processor, handle) =
        live_test_processor(root.path(), Duration::ZERO, Duration::from_nanos(1));

    let mut owned = number_dp(vec![kv_str("series", "transfer")]);
    owned.time_unix_nano = 1_000_000_000;
    owned.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    assert_eq!(
        process_completed_live_message(
            &mut processor,
            1,
            source_metadata("topic-a", 7, 1, 1_000),
            request(
                vec![],
                vec![metric_gauge("partition.transfer", vec![owned])],
            ),
        ),
        ProcessResult::Ok
    );
    let old_pin = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(old_pin.generation(), 1);

    let reader_ready = Arc::new(Barrier::new(2));
    let reader_release = Arc::new(Barrier::new(2));
    let ready = Arc::clone(&reader_ready);
    let release_for_reader = Arc::clone(&reader_release);
    let old_reader = thread::spawn(move || {
        ready.wait();
        release_for_reader.wait();
        (
            old_pin.generation(),
            query_live_gauge(&old_pin, "partition.transfer", 0, 20_000),
        )
    });
    reader_ready.wait();

    let mut rotation_trigger = number_dp(vec![kv_str("series", "trigger")]);
    rotation_trigger.time_unix_nano = 11_000_000_000;
    rotation_trigger.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(9.0));
    assert_eq!(
        process_completed_live_message(
            &mut processor,
            2,
            source_metadata("topic-a", 7, 2, 11_000),
            request(
                vec![],
                vec![metric_gauge(
                    "partition.rotation-trigger",
                    vec![rotation_trigger],
                )],
            ),
        ),
        ProcessResult::Ok
    );
    let handed = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(handed.generation(), 2);
    assert_eq!(
        query_live_gauge(&handed, "partition.transfer", 0, 20_000),
        vec![(1_000, 1.0)]
    );
    assert!(
        handed
            .payload()
            .head()
            .samples()
            .metric_names(handed.payload().head().labels().as_ref(), 0, 10_000)
            .unwrap()
            .is_empty(),
        "the previous owner's range must have logically handed off"
    );
    drop(handed);

    let mut transferred = number_dp(vec![kv_str("series", "transfer")]);
    transferred.time_unix_nano = 12_000_000_000;
    transferred.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    assert_eq!(
        process_completed_live_message(
            &mut processor,
            3,
            source_metadata("topic-b", 7, 3, 12_000),
            request(
                vec![],
                vec![metric_gauge("partition.transfer", vec![transferred])],
            ),
        ),
        ProcessResult::Ok
    );

    let current = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(current.generation(), 3);
    assert_eq!(
        query_live_gauge(&current, "partition.transfer", 0, 20_000),
        vec![(1_000, 1.0), (12_000, 2.0)]
    );
    reader_release.wait();
    let (old_generation, old_samples) = old_reader.join().unwrap();
    assert_eq!(old_generation, 1);
    assert_eq!(old_samples, vec![(1_000, 1.0)]);
}

#[test]
fn processor_live_publication_pins_old_generation_while_sealing_the_next() {
    let root = tempfile::tempdir().unwrap();
    let writer_config = SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema8)
        .with_deterministic_segment_ids(0x11_1e);
    let writer = SegmentWriter::new(writer_config.clone()).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(10));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);
    let handle = processor
        .enable_live_publication(LivePublisherConfig {
            publish_interval: Duration::from_nanos(1),
            max_view_staleness: Duration::from_secs(60),
            memory_admission_bytes: 64 * 1024 * 1024,
        })
        .unwrap();
    assert!(
        handle.query_admission_configured(),
        "the processor's live publisher must configure query admission"
    );

    let mut first = number_dp(vec![kv_str("pod", "backend-1")]);
    first.time_unix_nano = 1_000_000_000;
    first.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    Processor::begin_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();
    assert_eq!(
        Processor::process(
            &mut processor,
            tracked_metadata(1, 1_000),
            request(
                vec![],
                vec![metric_gauge("live_processor_gauge", vec![first])],
            ),
        )
        .unwrap(),
        ProcessResult::Ok
    );
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(1)).unwrap();

    let generation_one = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(generation_one.generation(), 1);
    assert_eq!(generation_one.visible_message_sequence(), 1);
    assert_eq!(
        generation_one
            .payload()
            .head()
            .samples()
            .metric_names(generation_one.payload().head().labels().as_ref(), 0, 20_000,)
            .unwrap(),
        vec!["live_processor_gauge"]
    );
    assert_eq!(
        query_live_gauge(&generation_one, "live_processor_gauge", 0, 20_000),
        vec![(1_000, 1.0)]
    );

    let pinned = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let reader_pinned = Arc::clone(&pinned);
    let reader_release = Arc::clone(&release);
    let old_reader = thread::spawn(move || {
        reader_pinned.wait();
        reader_release.wait();
        (
            generation_one.generation(),
            generation_one.visible_message_sequence(),
            query_live_gauge(&generation_one, "live_processor_gauge", 0, 20_000),
        )
    });
    pinned.wait();

    let mut second = number_dp(vec![kv_str("pod", "backend-1")]);
    second.time_unix_nano = 11_000_000_000;
    second.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    Processor::begin_acquired_message(&mut processor, MessageSequence::new(2)).unwrap();
    assert_eq!(
        Processor::process(
            &mut processor,
            tracked_metadata(2, 11_000),
            request(
                vec![],
                vec![metric_gauge("live_processor_gauge", vec![second])],
            ),
        )
        .unwrap(),
        ProcessResult::Ok
    );
    let dirty_since = match handle.status().unwrap().readiness {
        chronoxide_core::storage::live_view::LiveReadiness::DirtySince(dirty_since) => dirty_since,
        readiness => panic!("in-flight head mutation left readiness at {readiness:?}"),
    };
    assert_eq!(handle.status().unwrap().generation, Some(1));
    assert!(
        handle
            .try_pin_admitted(dirty_since + Duration::from_secs(60))
            .is_ok()
    );
    assert!(matches!(
        handle.try_pin_admitted(dirty_since + Duration::from_secs(61)),
        Err(chronoxide_core::storage::live_view::LiveViewError::Stale { .. })
    ));
    Processor::complete_acquired_message(&mut processor, MessageSequence::new(2)).unwrap();

    let generation_two = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(generation_two.generation(), 2);
    assert_eq!(generation_two.visible_message_sequence(), 2);
    assert_eq!(
        query_live_gauge(&generation_two, "live_processor_gauge", 0, 20_000),
        vec![(1_000, 1.0), (11_000, 2.0)]
    );
    assert!(
        !generation_two.payload().head().is_empty(),
        "the second sample must remain in the mutable head"
    );
    let mut sealed_session = generation_two.payload().sealed().query_session().unwrap();
    let mut sealed_results = sealed_session
        .query_selector(&SegmentSelector::metric("live_processor_gauge"), 0, 20_000)
        .unwrap();
    assert_eq!(sealed_results.len(), 1);
    assert_eq!(
        sealed_results.pop().unwrap().samples,
        vec![(1_000, 1.0)],
        "rotation must expose the first window from the sealed inventory"
    );

    release.wait();
    let (old_generation, old_message_cut, old_samples) = old_reader.join().unwrap();
    assert_eq!(old_generation, 1);
    assert_eq!(old_message_cut, 1);
    assert_eq!(old_samples, vec![(1_000, 1.0)]);
}

#[test]
fn real_ingestion_publishes_while_an_old_generation_is_paused_inside_head_decode() {
    let root = tempfile::tempdir().unwrap();
    let window_duration = Duration::from_secs(10);
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(root.path(), window_duration)
            .with_storage_schema(SegmentStorageSchema::Schema8)
            .with_deterministic_segment_ids(0x00de_c0de),
    )
    .unwrap();
    // Two samples per block guarantees that the real query reaches an
    // encoded-arena read instead of being satisfied entirely by an encoder's
    // unflushed tail.
    let head = HeadConfig::with_block_size(
        window_duration,
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(window_duration)
    .with_compact_numeric_series(false);
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);
    let handle = processor
        .enable_live_publication(LivePublisherConfig {
            publish_interval: Duration::from_nanos(1),
            max_view_staleness: Duration::from_secs(120),
            memory_admission_bytes: 64 * 1024 * 1024,
        })
        .unwrap();

    let (decode_entered_tx, decode_entered_rx) = std::sync::mpsc::channel();
    let decode_release = Arc::new(Barrier::new(2));
    let release_from_hook = Arc::clone(&decode_release);
    processor.set_next_live_head_decode_hook(move || {
        decode_entered_tx.send(()).unwrap();
        release_from_hook.wait();
    });

    let first = [(1_000_u64, 1.0), (1_001, 1.25), (1_002, 1.5)]
        .into_iter()
        .map(|(timestamp_ms, value)| {
            let mut datapoint = number_dp(vec![kv_str("pod", "backend-1")]);
            datapoint.time_unix_nano = timestamp_ms * 1_000_000;
            datapoint.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
                value,
            ));
            datapoint
        })
        .collect();
    assert_eq!(
        process_completed_live_message(
            &mut processor,
            1,
            tracked_metadata(1, 1_000),
            request(vec![], vec![metric_gauge("decode_publish_race", first)],),
        ),
        ProcessResult::Ok
    );
    let generation_one_probe = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(generation_one_probe.generation(), 1);
    assert!(
        !generation_one_probe.payload().head().is_empty()
            && generation_one_probe
                .payload()
                .head()
                .samples()
                .fragment_count()
                > 0,
        "generation 1 must retain an encoded head fragment for the decode race"
    );
    assert!(
        generation_one_probe
            .payload()
            .head()
            .samples()
            .has_decode_hook_for_test(),
        "the deterministic decode hook must be attached to generation 1"
    );
    drop(generation_one_probe);

    let query_a_handle = Arc::clone(&handle);
    let query_a = thread::spawn(move || {
        let generation_one = query_a_handle.try_pin_admitted(Instant::now()).unwrap();
        let generation = generation_one.generation();
        let message_cut = generation_one.visible_message_sequence();
        let samples = query_live_gauge(&generation_one, "decode_publish_race", 0, 20_000);
        (generation, message_cut, samples)
    });
    if let Err(error) = decode_entered_rx.recv_timeout(Duration::from_secs(10)) {
        if query_a.is_finished() {
            panic!(
                "query A terminated before reaching the first encoded head-arena read: {:?}",
                query_a.join()
            );
        }
        panic!("query A did not reach the first encoded head-arena read: {error}");
    }

    let mut second = number_dp(vec![kv_str("pod", "backend-1")]);
    second.time_unix_nano = 11_000_000_000;
    second.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    let (ingestion_done_tx, ingestion_done_rx) = std::sync::mpsc::channel();
    let ingestion = thread::spawn(move || {
        let result = process_completed_live_message(
            &mut processor,
            2,
            tracked_metadata(2, 11_000),
            request(
                vec![],
                vec![metric_gauge("decode_publish_race", vec![second])],
            ),
        );
        ingestion_done_tx.send(()).unwrap();
        (processor, result)
    });

    if let Err(error) = ingestion_done_rx.recv_timeout(Duration::from_secs(10)) {
        // Release the reader before failing so a genuine lock regression
        // produces a bounded, diagnosable test rather than orphaned threads.
        decode_release.wait();
        let _ = query_a.join();
        let _ = ingestion.join();
        panic!("ingestion/publication blocked on query A's head decode: {error}");
    }
    let (mut processor, ingestion_result) = ingestion.join().unwrap();
    assert_eq!(ingestion_result, ProcessResult::Ok);

    // Query B pins after publication while query A is still stopped inside
    // generation 1's decode. It must observe exactly the complete new cut.
    let generation_two = handle.try_pin_admitted(Instant::now()).unwrap();
    assert_eq!(generation_two.generation(), 2);
    assert_eq!(generation_two.visible_message_sequence(), 2);
    assert_eq!(
        query_live_gauge(&generation_two, "decode_publish_race", 0, 20_000),
        vec![(1_000, 1.0), (1_001, 1.25), (1_002, 1.5), (11_000, 2.0),]
    );
    let charged_with_both_generations = processor.live_memory_stats().unwrap().charged_bytes;

    decode_release.wait();
    assert_eq!(
        query_a.join().unwrap(),
        (1, 1, vec![(1_000, 1.0), (1_001, 1.25), (1_002, 1.5)]),
        "query A must finish against only its pinned generation"
    );
    assert!(
        processor.live_memory_stats().unwrap().charged_bytes < charged_with_both_generations,
        "dropping query A's obsolete pin must safely release its exclusive retention"
    );

    drop(generation_two);
    Processor::shutdown(&mut processor).unwrap();
    let after_pin_drop = handle.try_pin_admitted(Instant::now()).unwrap();
    assert!(after_pin_drop.generation() >= 2);
    assert_eq!(
        query_live_gauge(&after_pin_drop, "decode_publish_race", 0, 20_000),
        vec![(1_000, 1.0), (1_001, 1.25), (1_002, 1.5), (11_000, 2.0),],
        "dropping both earlier pins must not corrupt a later publication"
    );
}

fn segment_dir_count(segments_dir: &std::path::Path) -> usize {
    fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .count()
}

fn snapshot_tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn visit(root: &Path, dir: &Path, files: &mut Vec<(String, Vec<u8>)>) {
        let mut entries = fs::read_dir(dir)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                files.push((relative, fs::read(path).unwrap()));
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    files
}

#[derive(Clone, Copy)]
enum LivePublicationSchedule {
    Disabled,
    Coalesced,
    EveryMessage,
}

fn write_promql_label_collision_fixture(
    root: &Path,
    publication_schedule: LivePublicationSchedule,
) -> Vec<(String, Vec<u8>)> {
    fs::create_dir_all(root).unwrap();
    let writer_config = SegmentWriterConfig::new(root, Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema8)
        .with_deterministic_segment_ids(0x1abe_1c01);
    let writer = SegmentWriter::new(writer_config.clone()).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);
    if !matches!(publication_schedule, LivePublicationSchedule::Disabled) {
        let publish_interval = match publication_schedule {
            LivePublicationSchedule::Disabled => unreachable!(),
            LivePublicationSchedule::Coalesced => Duration::from_secs(60),
            LivePublicationSchedule::EveryMessage => Duration::from_nanos(1),
        };
        processor
            .enable_live_publication(LivePublisherConfig {
                publish_interval,
                max_view_staleness: Duration::from_secs(120),
                memory_admission_bytes: 64 * 1024 * 1024,
            })
            .unwrap();
    }

    let raw_name = "a.label";
    let projected_name = normalize_label_name(raw_name);
    assert_ne!(raw_name, projected_name);
    let mut raw = number_dp(vec![kv_str(raw_name, "same-value")]);
    raw.time_unix_nano = 5_000_000_000;
    raw.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut already_projected = number_dp(vec![kv_str(&projected_name, "same-value")]);
    already_projected.time_unix_nano = 6_000_000_000;
    already_projected.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    let raw_metric_name = "metric.with.dot";
    let projected_metric_name = normalize_metric_name(raw_metric_name);
    assert_ne!(raw_metric_name, projected_metric_name);
    let mut raw_metric = number_dp(vec![kv_str("case", "metric-name")]);
    raw_metric.time_unix_nano = 15_000_000_000;
    raw_metric.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(3.0));
    let mut projected_metric = number_dp(vec![kv_str("case", "metric-name")]);
    projected_metric.time_unix_nano = 16_000_000_000;
    projected_metric.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(4.0));
    let messages = [
        (5_000, metric_gauge("promql_collision_metric", vec![raw])),
        (
            6_000,
            metric_gauge("promql_collision_metric", vec![already_projected]),
        ),
        (15_000, metric_gauge(raw_metric_name, vec![raw_metric])),
        (
            16_000,
            metric_gauge(&projected_metric_name, vec![projected_metric]),
        ),
    ];
    for (index, (captured_at_ms, metric)) in messages.into_iter().enumerate() {
        let sequence = MessageSequence::new(u64::try_from(index + 1).unwrap());
        if !matches!(publication_schedule, LivePublicationSchedule::Disabled) {
            Processor::begin_acquired_message(&mut processor, sequence).unwrap();
        }
        assert_eq!(
            Processor::process(
                &mut processor,
                tracked_metadata(i64::try_from(index + 1).unwrap(), captured_at_ms),
                request(vec![], vec![metric]),
            )
            .unwrap(),
            ProcessResult::Ok
        );
        if !matches!(publication_schedule, LivePublicationSchedule::Disabled) {
            Processor::complete_acquired_message(&mut processor, sequence).unwrap();
        }
    }
    assert_eq!(
        processor.labelsets.stats().series,
        4,
        "raw FlatInterned identity must remain distinct before projection"
    );

    Processor::shutdown(&mut processor).unwrap();
    drop(processor);
    snapshot_tree(root)
}

#[test]
fn live_and_disabled_modes_emit_identical_bytes_for_promql_label_collision() {
    let tempdir = tempfile::tempdir().unwrap();
    let disabled_root = tempdir.path().join("disabled");
    let coalesced_root = tempdir.path().join("live-coalesced");
    let every_message_root = tempdir.path().join("live-every-message");

    let disabled =
        write_promql_label_collision_fixture(&disabled_root, LivePublicationSchedule::Disabled);
    assert_eq!(
        write_promql_label_collision_fixture(&coalesced_root, LivePublicationSchedule::Coalesced,),
        disabled,
        "coalesced live publication changed the complete deterministic storage tree"
    );
    assert_eq!(
        write_promql_label_collision_fixture(
            &every_message_root,
            LivePublicationSchedule::EveryMessage,
        ),
        disabled,
        "per-message live publication changed the complete deterministic storage tree"
    );
}

fn metric_with_temporality(
    mut metric: tonic::metrics::v1::Metric,
    temporality: AggregationTemporality,
) -> tonic::metrics::v1::Metric {
    match metric.data.as_mut() {
        Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) => {
            histogram.aggregation_temporality = temporality as i32;
        }
        Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) => {
            histogram.aggregation_temporality = temporality as i32;
        }
        other => panic!("temporality fixture requires a histogram metric, got {other:?}"),
    }
    metric
}

fn all_kind_parity_number(
    timestamp_ms: u64,
    value: tonic::metrics::v1::number_data_point::Value,
) -> tonic::metrics::v1::NumberDataPoint {
    let mut point = number_dp(vec![kv_str("case", "all-kind-parity")]);
    point.time_unix_nano = timestamp_ms * 1_000_000;
    point.value = Some(value);
    point
}

fn all_kind_parity_histogram(
    timestamp_ms: u64,
    start_ms: u64,
    flags: u32,
    bucket_counts: [u64; 3],
) -> tonic::metrics::v1::HistogramDataPoint {
    let mut point = histogram_dp(vec![kv_str("case", "all-kind-parity")]);
    point.time_unix_nano = timestamp_ms * 1_000_000;
    point.start_time_unix_nano = start_ms * 1_000_000;
    point.flags = flags;
    point.count = bucket_counts.iter().sum();
    point.sum = Some(point.count as f64 + 0.25);
    point.min = Some(0.25);
    point.max = Some(8.0);
    point.explicit_bounds = vec![1.0, 5.0];
    point.bucket_counts = bucket_counts.into();
    point
}

fn all_kind_parity_exponential_histogram(
    timestamp_ms: u64,
    start_ms: u64,
    flags: u32,
    positive_counts: [u64; 2],
    negative_count: u64,
) -> tonic::metrics::v1::ExponentialHistogramDataPoint {
    let mut point = exp_histogram_dp(vec![kv_str("case", "all-kind-parity")]);
    point.time_unix_nano = timestamp_ms * 1_000_000;
    point.start_time_unix_nano = start_ms * 1_000_000;
    point.flags = flags;
    point.zero_count = 1;
    point.count = point.zero_count + positive_counts.iter().sum::<u64>() + negative_count;
    point.sum = Some(point.count as f64 + 0.5);
    point.min = Some(-2.0);
    point.max = Some(8.0);
    point.scale = 1;
    point.zero_threshold = 0.001;
    point.positive = Some(Buckets {
        offset: -1,
        bucket_counts: positive_counts.into(),
    });
    point.negative = Some(Buckets {
        offset: 0,
        bucket_counts: vec![negative_count],
    });
    point
}

fn all_kind_parity_summary(
    timestamp_ms: u64,
    start_ms: u64,
    flags: u32,
    count: u64,
    median: f64,
) -> tonic::metrics::v1::SummaryDataPoint {
    let mut point = summary_dp(vec![kv_str("case", "all-kind-parity")]);
    point.time_unix_nano = timestamp_ms * 1_000_000;
    point.start_time_unix_nano = start_ms * 1_000_000;
    point.flags = flags;
    point.count = count;
    point.sum = median * count as f64;
    point.quantile_values = vec![
        ValueAtQuantile {
            quantile: 0.5,
            value: median,
        },
        ValueAtQuantile {
            quantile: 0.99,
            value: median * 2.0,
        },
    ];
    point
}

fn all_kind_parity_messages() -> Vec<(i64, Vec<tonic::metrics::v1::Metric>)> {
    use tonic::metrics::v1::number_data_point::Value::{AsDouble, AsInt};

    let scalar = metric_gauge(
        "parity.scalar",
        vec![
            all_kind_parity_number(1_000, AsDouble(f64::from_bits(0x7ff8_0000_0000_0042))),
            all_kind_parity_number(2_000, AsDouble(f64::INFINITY)),
            all_kind_parity_number(3_000, AsDouble(f64::NEG_INFINITY)),
            all_kind_parity_number(4_000, AsDouble(prometheus_stale_nan())),
            all_kind_parity_number(6_000, AsDouble(6.0)),
            all_kind_parity_number(7_000, AsDouble(7.0)),
        ],
    );
    let integer = metric_sum(
        "parity.integer",
        vec![
            all_kind_parity_number(5_000, AsInt(-9)),
            all_kind_parity_number(5_500, AsInt(-9_007_199_254_740)),
        ],
    );
    let cumulative_histogram = metric_with_temporality(
        metric_histogram(
            "parity.cumulative_histogram",
            vec![
                all_kind_parity_histogram(2_000, 1_000, 0x10, [1, 1, 0]),
                all_kind_parity_histogram(4_000, 1_000, 0x20, [2, 2, 0]),
                all_kind_parity_histogram(8_000, 1_000, 0x41, [2, 2, 0]),
            ],
        ),
        AggregationTemporality::Cumulative,
    );
    let cumulative_exponential_histogram = metric_with_temporality(
        metric_exp_histogram(
            "parity.cumulative_exponential_histogram",
            vec![
                all_kind_parity_exponential_histogram(2_500, 1_000, 0x100, [1, 1], 1),
                all_kind_parity_exponential_histogram(4_500, 1_000, 0x200, [2, 1], 1),
                all_kind_parity_exponential_histogram(8_200, 1_000, 0x401, [2, 1], 1),
            ],
        ),
        AggregationTemporality::Cumulative,
    );
    let delta_histogram = metric_with_temporality(
        metric_histogram(
            "parity.delta_histogram",
            vec![all_kind_parity_histogram(3_500, 3_000, 0x800, [1, 0, 1])],
        ),
        AggregationTemporality::Delta,
    );
    let delta_exponential_histogram = metric_with_temporality(
        metric_exp_histogram(
            "parity.delta_exponential_histogram",
            vec![all_kind_parity_exponential_histogram(
                3_600,
                3_000,
                0x1_000,
                [1, 0],
                1,
            )],
        ),
        AggregationTemporality::Delta,
    );
    let summary = metric_summary(
        "parity.summary",
        vec![
            all_kind_parity_summary(3_800, 1_000, 0x2_000, 10, 4.0),
            all_kind_parity_summary(8_400, 1_000, 0x4_001, 10, 4.0),
        ],
    );

    // Every point in this message is older than a prior point for its series.
    // It therefore exercises the pre-seal OOO lane, including equal-timestamp
    // last-write-wins after Float/Int64 scalar conversion.
    let preseal_ooo = vec![
        metric_gauge(
            "parity.scalar",
            vec![all_kind_parity_number(6_000, AsDouble(66.0))],
        ),
        metric_sum(
            "parity.integer",
            vec![all_kind_parity_number(5_000, AsInt(-10))],
        ),
        metric_with_temporality(
            metric_histogram(
                "parity.cumulative_histogram",
                vec![
                    all_kind_parity_histogram(5_000, 1_000, 0x40, [3, 2, 0]),
                    all_kind_parity_histogram(7_000, 6_500, 0x80, [1, 0, 0]),
                ],
            ),
            AggregationTemporality::Cumulative,
        ),
        metric_with_temporality(
            metric_exp_histogram(
                "parity.cumulative_exponential_histogram",
                vec![
                    all_kind_parity_exponential_histogram(5_500, 1_000, 0x800, [2, 2], 1),
                    all_kind_parity_exponential_histogram(7_200, 6_500, 0x1_000, [1, 0], 0),
                ],
            ),
            AggregationTemporality::Cumulative,
        ),
        metric_with_temporality(
            metric_histogram(
                "parity.delta_histogram",
                vec![all_kind_parity_histogram(3_500, 3_000, 0x2_000, [1, 1, 1])],
            ),
            AggregationTemporality::Delta,
        ),
        metric_with_temporality(
            metric_exp_histogram(
                "parity.delta_exponential_histogram",
                vec![all_kind_parity_exponential_histogram(
                    3_600,
                    3_000,
                    0x4_000,
                    [1, 1],
                    1,
                )],
            ),
            AggregationTemporality::Delta,
        ),
        metric_summary(
            "parity.summary",
            vec![all_kind_parity_summary(3_800, 1_000, 0x8_000, 11, 5.0)],
        ),
    ];

    let rotation = vec![metric_gauge(
        "parity.rotation",
        vec![all_kind_parity_number(12_000, AsDouble(12.0))],
    )];

    // The preceding rotation is a mandatory publication boundary, independent
    // of the configured live publication interval. These samples therefore
    // belong to a newer overlapping OOO-only segment in every mode.
    let postseal_ooo = vec![
        metric_gauge(
            "parity.scalar",
            vec![all_kind_parity_number(6_000, AsDouble(600.0))],
        ),
        metric_sum(
            "parity.integer",
            vec![all_kind_parity_number(5_000, AsInt(-11))],
        ),
        metric_with_temporality(
            metric_histogram(
                "parity.cumulative_histogram",
                vec![all_kind_parity_histogram(7_000, 6_500, 0x10_000, [1, 1, 0])],
            ),
            AggregationTemporality::Cumulative,
        ),
        metric_with_temporality(
            metric_exp_histogram(
                "parity.cumulative_exponential_histogram",
                vec![all_kind_parity_exponential_histogram(
                    7_200,
                    6_500,
                    0x20_000,
                    [1, 1],
                    0,
                )],
            ),
            AggregationTemporality::Cumulative,
        ),
        metric_with_temporality(
            metric_histogram(
                "parity.delta_histogram",
                vec![all_kind_parity_histogram(3_500, 3_000, 0x40_000, [2, 1, 1])],
            ),
            AggregationTemporality::Delta,
        ),
        metric_with_temporality(
            metric_exp_histogram(
                "parity.delta_exponential_histogram",
                vec![all_kind_parity_exponential_histogram(
                    3_600,
                    3_000,
                    0x80_000,
                    [2, 1],
                    1,
                )],
            ),
            AggregationTemporality::Delta,
        ),
        metric_summary(
            "parity.summary",
            vec![all_kind_parity_summary(3_800, 1_000, 0x10_0000, 12, 6.0)],
        ),
    ];

    vec![
        (
            9_000,
            vec![
                scalar,
                integer,
                cumulative_histogram,
                cumulative_exponential_histogram,
                delta_histogram,
                delta_exponential_histogram,
                summary,
            ],
        ),
        (9_000, preseal_ooo),
        (12_000, rotation),
        (12_000, postseal_ooo),
    ]
}

fn verify_all_kind_parity_fixture(root: &Path) {
    let mut segment_dirs = fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    segment_dirs.sort();
    assert_eq!(
        segment_dirs.len(),
        3,
        "fixture must produce base, overlapping OOO, and active-tail segments"
    );

    let mut kinds = BTreeSet::new();
    let mut ooo_kinds = BTreeSet::new();
    let mut float_bits = Vec::new();
    let mut has_start_time = false;
    let mut has_custom_flags = false;
    let mut has_stale_typed_value = false;
    let mut has_unspecified_temporality = false;
    let mut has_delta_temporality = false;
    let mut has_cumulative_temporality = false;
    let mut has_unknown_reset = false;
    let mut has_counter_reset = false;
    let mut has_not_counter_reset = false;

    for segment_dir in segment_dirs {
        for (file, out_of_order) in [(SegmentFile::Chunks, false), (SegmentFile::OooChunks, true)] {
            let mut reader =
                ChunkReader::new(File::open(segment_dir.join(file.filename())).unwrap());
            while let Some(record) = reader.read_next().unwrap() {
                kinds.insert(record.kind);
                if out_of_order {
                    ooo_kinds.insert(record.kind);
                }
                match record.samples {
                    ChunkSamples::Float(samples) => {
                        float_bits.extend(samples.into_iter().map(|(_, value)| value.to_bits()));
                    }
                    ChunkSamples::Int64(_) => {
                        panic!("the ingester must persist OTLP integers in its Float lane")
                    }
                    ChunkSamples::Histogram(samples) => {
                        for (_, value) in samples {
                            let metadata = value.metadata;
                            has_start_time |= metadata.start_time_ms.is_some();
                            has_custom_flags |= metadata.flags & !1 != 0;
                            has_stale_typed_value |= metadata.is_stale();
                            has_unspecified_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Unspecified;
                            has_delta_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Delta;
                            has_cumulative_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Cumulative;
                            has_unknown_reset |= metadata.reset_hint == CounterResetHint::Unknown;
                            has_counter_reset |=
                                metadata.reset_hint == CounterResetHint::CounterReset;
                            has_not_counter_reset |=
                                metadata.reset_hint == CounterResetHint::NotCounterReset;
                        }
                    }
                    ChunkSamples::ExponentialHistogram(samples) => {
                        for (_, value) in samples {
                            let metadata = value.metadata;
                            has_start_time |= metadata.start_time_ms.is_some();
                            has_custom_flags |= metadata.flags & !1 != 0;
                            has_stale_typed_value |= metadata.is_stale();
                            has_unspecified_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Unspecified;
                            has_delta_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Delta;
                            has_cumulative_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Cumulative;
                            has_unknown_reset |= metadata.reset_hint == CounterResetHint::Unknown;
                            has_counter_reset |=
                                metadata.reset_hint == CounterResetHint::CounterReset;
                            has_not_counter_reset |=
                                metadata.reset_hint == CounterResetHint::NotCounterReset;
                        }
                    }
                    ChunkSamples::Summary(samples) => {
                        for (_, value) in samples {
                            let metadata = value.metadata;
                            has_start_time |= metadata.start_time_ms.is_some();
                            has_custom_flags |= metadata.flags & !1 != 0;
                            has_stale_typed_value |= metadata.is_stale();
                            has_unspecified_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Unspecified;
                            has_delta_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Delta;
                            has_cumulative_temporality |=
                                metadata.temporality == OtlpAggregationTemporality::Cumulative;
                            has_unknown_reset |= metadata.reset_hint == CounterResetHint::Unknown;
                            has_counter_reset |=
                                metadata.reset_hint == CounterResetHint::CounterReset;
                            has_not_counter_reset |=
                                metadata.reset_hint == CounterResetHint::NotCounterReset;
                        }
                    }
                }
            }
        }
    }

    // ChunkKind::Int64 is a writer/reader format capability, but the OTLP
    // processor deliberately converts every AsInt datapoint into the PromQL
    // Float lane at seal time. The fixture supplies AsInt values in all three
    // ordering phases and asserts that no physical Int64 chunk escapes.
    let persisted_kinds = BTreeSet::from([
        ChunkKind::Float,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ]);
    assert_eq!(
        kinds, persisted_kinds,
        "fixture must exercise every kind persisted by OTLP ingestion"
    );
    assert_eq!(
        ooo_kinds, persisted_kinds,
        "post-seal OOO payload must exercise every persisted kind"
    );
    for expected in [
        0x7ff8_0000_0000_0042,
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        prometheus_stale_nan().to_bits(),
        66.0f64.to_bits(),
        600.0f64.to_bits(),
        (-10.0f64).to_bits(),
        (-11.0f64).to_bits(),
    ] {
        assert!(
            float_bits.contains(&expected),
            "fixture lost scalar IEEE/stale value 0x{expected:016x}"
        );
    }
    assert!(has_start_time);
    assert!(has_custom_flags);
    assert!(has_stale_typed_value);
    assert!(has_unspecified_temporality);
    assert!(has_delta_temporality);
    assert!(has_cumulative_temporality);
    assert!(has_unknown_reset);
    assert!(has_counter_reset);
    assert!(has_not_counter_reset);

    let store = SegmentStoreReader::open_manifest_published(root, root.join("manifest")).unwrap();
    for (query, timestamp_ms, expected_value) in [
        (r#"parity.scalar{case="all-kind-parity"}"#, 6_000, 600.0_f64),
        (
            r#"parity.integer{case="all-kind-parity"}"#,
            5_000,
            -11.0_f64,
        ),
    ] {
        let results = store.query_promql(query, 0, 20_000).unwrap();
        assert_eq!(results.len(), 1, "query {query}");
        let winners = results[0]
            .samples
            .iter()
            .filter(|(timestamp, _)| *timestamp == timestamp_ms)
            .collect::<Vec<_>>();
        assert_eq!(
            winners.len(),
            1,
            "query {query} must return one last-write-wins sample"
        );
        assert_eq!(
            winners[0].1.to_bits(),
            expected_value.to_bits(),
            "post-seal OOO must win query {query}"
        );
    }
}

fn write_all_kind_tree_parity_fixture(
    root: &Path,
    publication_schedule: LivePublicationSchedule,
) -> Vec<(String, Vec<u8>)> {
    fs::create_dir_all(root).unwrap();
    let writer_config = SegmentWriterConfig::new(root, Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema8)
        .with_deterministic_segment_ids(0x0a11_c1d5);
    let writer = SegmentWriter::new(writer_config).unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(10));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);
    if !matches!(publication_schedule, LivePublicationSchedule::Disabled) {
        let publish_interval = match publication_schedule {
            LivePublicationSchedule::Disabled => unreachable!(),
            LivePublicationSchedule::Coalesced => Duration::from_secs(60),
            LivePublicationSchedule::EveryMessage => Duration::from_nanos(1),
        };
        processor
            .enable_live_publication(LivePublisherConfig {
                publish_interval,
                max_view_staleness: Duration::from_secs(120),
                memory_admission_bytes: 64 * 1024 * 1024,
            })
            .unwrap();
    }

    for (index, (captured_at_ms, metrics)) in all_kind_parity_messages().into_iter().enumerate() {
        let message_sequence = MessageSequence::new(u64::try_from(index + 1).unwrap());
        if !matches!(publication_schedule, LivePublicationSchedule::Disabled) {
            Processor::begin_acquired_message(&mut processor, message_sequence).unwrap();
        }
        assert_eq!(
            Processor::process(
                &mut processor,
                source_metadata(
                    "all-kind-parity",
                    7,
                    i64::try_from(index + 1).unwrap(),
                    captured_at_ms,
                ),
                request(vec![kv_str("service.name", "all-kind-parity")], metrics,),
            )
            .unwrap(),
            ProcessResult::Ok
        );
        if !matches!(publication_schedule, LivePublicationSchedule::Disabled) {
            Processor::complete_acquired_message(&mut processor, message_sequence).unwrap();
        }
    }

    Processor::shutdown(&mut processor).unwrap();
    drop(processor);
    verify_all_kind_parity_fixture(root);
    snapshot_tree(root)
}

#[test]
fn live_publication_schedules_preserve_complete_all_kind_output_tree() {
    let tempdir = tempfile::tempdir().unwrap();
    let disabled = write_all_kind_tree_parity_fixture(
        &tempdir.path().join("disabled"),
        LivePublicationSchedule::Disabled,
    );
    assert_eq!(
        write_all_kind_tree_parity_fixture(
            &tempdir.path().join("live-coalesced"),
            LivePublicationSchedule::Coalesced,
        ),
        disabled,
        "coalesced publication changed the complete all-kind storage tree"
    );
    assert_eq!(
        write_all_kind_tree_parity_fixture(
            &tempdir.path().join("live-every-message"),
            LivePublicationSchedule::EveryMessage,
        ),
        disabled,
        "per-message publication changed the complete all-kind storage tree"
    );
}

fn write_partition_drain_fixture(segments_dir: &Path, reverse: bool) {
    fs::create_dir_all(segments_dir).unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(segments_dir, Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6)
            .with_deterministic_segment_ids(0x5eed),
    )
    .unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    let partitions: Vec<i32> = if reverse {
        (0..16).rev().collect()
    } else {
        (0..16).collect()
    };
    for partition in partitions {
        // Every partition drains the same [10s, 20s) range with co-resident
        // active and OOO lanes. This makes byte determinism depend on grouping
        // the lanes and applying the complete (range, partition) order rather
        // than accidentally sorting by distinct time ranges first. Partition
        // zero also exercises the multi-kind fallback in each fresh process.
        for (ordinal, timestamp_ms) in [15_000_u64, 12_000].into_iter().enumerate() {
            let metric = if partition == 0 && ordinal == 1 {
                let mut point = histogram_dp(vec![kv_str("host", "shared")]);
                point.start_time_unix_nano = 10_000_000_000;
                point.time_unix_nano = timestamp_ms * 1_000_000;
                point.count = 2;
                point.sum = Some(3.0);
                point.explicit_bounds = vec![1.0];
                point.bucket_counts = vec![1, 1];
                metric_histogram("drain.order", vec![point])
            } else {
                let mut point = number_dp(vec![kv_str("host", "shared")]);
                point.time_unix_nano = timestamp_ms * 1_000_000;
                point.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(
                    i64::from(partition) * 2 + i64::try_from(ordinal).unwrap(),
                ));
                metric_gauge("drain.order", vec![point])
            };
            processor
                .process(
                    SourceMessageMetadata {
                        topic: "metrics".to_owned(),
                        partition,
                        offset: i64::from(partition) * 2 + i64::try_from(ordinal).unwrap(),
                        timestamp_ms: timestamp_ms as i64,
                        captured_at_ms: 15_000,
                    },
                    request(vec![], vec![metric]),
                )
                .unwrap();
        }
    }
    processor.flush_head().unwrap();
}

#[test]
fn processor_partition_drain_is_byte_deterministic_across_fresh_processes() {
    const CHILD_DIR_ENV: &str = "CHRONOXIDE_PARTITION_DRAIN_CHILD_DIR";
    const CHILD_REVERSE_ENV: &str = "CHRONOXIDE_PARTITION_DRAIN_CHILD_REVERSE";
    if let Some(segments_dir) = std::env::var_os(CHILD_DIR_ENV) {
        write_partition_drain_fixture(
            Path::new(&segments_dir),
            std::env::var_os(CHILD_REVERSE_ENV).is_some(),
        );
        return;
    }

    let tempdir = tempfile::tempdir().unwrap();
    let mut snapshots = Vec::new();
    for ordinal in 0..4 {
        let segments_dir = tempdir.path().join(format!("run-{ordinal}"));
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("processor_partition_drain_is_byte_deterministic_across_fresh_processes")
            .arg("--nocapture")
            .env(CHILD_DIR_ENV, &segments_dir);
        if ordinal % 2 == 1 {
            command.env(CHILD_REVERSE_ENV, "1");
        }
        let status = command.status().unwrap();
        assert!(status.success(), "fresh-process fixture {ordinal} failed");
        snapshots.push(snapshot_tree(&segments_dir));
    }

    for snapshot in &snapshots[1..] {
        assert_eq!(snapshot, &snapshots[0]);
    }
}

fn open_default_store(segments_dir: &std::path::Path) -> SegmentStoreReader {
    SegmentStoreReader::open(segments_dir).unwrap()
}

fn read_segment_meta(segment_dir: &std::path::Path) -> SegmentMeta {
    serde_json::from_slice(&fs::read(segment_dir.join(SegmentFile::MetaJson.filename())).unwrap())
        .unwrap()
}

fn segment_dirs_for_range(
    segments_dir: &std::path::Path,
    start_ms: u64,
    end_ms: u64,
) -> Vec<std::path::PathBuf> {
    let mut segments = fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| entry.path())
        .filter(|path| {
            let meta = read_segment_meta(path);
            meta.start_ms == start_ms && meta.end_ms == end_ms
        })
        .collect::<Vec<_>>();
    segments.sort();
    segments
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
        LabelSetInterner::VersionedFlatInterned(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
        LabelSetInterner::KeySetDictEncoded(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
    }
    out.sort();
    out
}

fn reset_hints_for_metric(
    samples_by_labels: &BTreeMap<Vec<(String, String)>, SeriesSamples>,
    metric_name: &str,
) -> Vec<CounterResetHint> {
    let samples = samples_by_labels
        .iter()
        .find_map(|(labels, samples)| {
            labels
                .iter()
                .any(|(key, value)| key == METRIC_NAME_LABEL && value == metric_name)
                .then_some(samples)
        })
        .unwrap_or_else(|| {
            let available = samples_by_labels
                .keys()
                .flat_map(|labels| labels.iter())
                .filter_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value))
                .collect::<Vec<_>>();
            panic!("missing samples for metric {metric_name}; available={available:?}")
        });
    match samples {
        SeriesSamples::Histogram { samples } => samples
            .iter()
            .map(|(_, value)| value.metadata.reset_hint)
            .collect(),
        SeriesSamples::ExponentialHistogram { samples } => samples
            .iter()
            .map(|(_, value)| value.metadata.reset_hint)
            .collect(),
        other => panic!("expected typed histogram samples, got {other:?}"),
    }
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
        LabelSetStoreKind::ExperimentalFlatInternedPaged,
        LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash,
        LabelSetStoreKind::ExperimentalFlatInternedSipHash,
        LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols,
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
fn flat_interned_config_selects_default_and_experimental_comparators() {
    let contiguous = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let paged = LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedPaged);
    let canonical_string_hash =
        LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash);
    let siphash = LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedSipHash);
    let siphash_symbols =
        LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols);

    assert_eq!(
        contiguous
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .key_values_storage,
        "contiguous"
    );
    assert_eq!(
        contiguous
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_ahash"
    );
    assert_eq!(contiguous.kind(), "FlatInterned");
    assert_eq!(
        paged
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .key_values_storage,
        "paged"
    );
    assert_eq!(paged.kind(), "ExperimentalFlatInternedPaged");
    assert_eq!(
        paged
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_ahash"
    );
    assert_eq!(
        canonical_string_hash
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .key_values_storage,
        "contiguous"
    );
    assert_eq!(
        canonical_string_hash
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "canonical_strings"
    );
    assert_eq!(
        canonical_string_hash.kind(),
        "ExperimentalFlatInternedCanonicalStringHash"
    );
    assert_eq!(
        siphash
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_siphash"
    );
    assert_eq!(siphash.kind(), "ExperimentalFlatInternedSipHash");
    assert_eq!(
        siphash_symbols
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_ahash"
    );
    assert_eq!(
        siphash_symbols
            .as_flat_interned()
            .expect("flat interner")
            .symbols()
            .symbol_hash_kind(),
        "siphash"
    );
    assert_eq!(
        siphash_symbols.kind(),
        "ExperimentalFlatInternedSipHashSymbols"
    );
}

fn assert_flat_metric_order_matches_owned_reference(
    interner: &LabelSetInterner,
    source: Vec<(SeriesRef, SeriesSamples)>,
) -> Vec<(SeriesRef, SeriesSamples)> {
    let mut indirect = source.clone();
    let mut reference = source;
    let canonical_label_counts =
        order_series_samples_for_metric_query(&mut indirect, interner).unwrap();
    order_flat_interned_series_samples_for_metric_query_owned_reference(
        &mut reference,
        interner.as_flat_interned().unwrap(),
    )
    .unwrap();
    assert_eq!(indirect, reference);
    assert_eq!(
        canonical_label_counts,
        indirect
            .iter()
            .map(|(series, _)| {
                checked_canonical_label_count(interner.segment_metadata(*series).labels().len())
                    .unwrap()
            })
            .collect::<Vec<_>>()
    );
    indirect
}

fn metric_order_test_samples(kind: usize, marker: u64) -> SeriesSamples {
    match kind % 5 {
        0 => SeriesSamples::Float {
            encoding: FloatEncoding::Gorilla,
            samples: vec![(marker, marker as f64)],
        },
        1 => SeriesSamples::Int64 {
            encoding: IntEncoding::DeltaZigZag,
            samples: vec![(marker, marker as i64)],
        },
        2 => SeriesSamples::Histogram {
            samples: Vec::new(),
        },
        3 => SeriesSamples::ExponentialHistogram {
            samples: Vec::new(),
        },
        _ => SeriesSamples::Summary {
            samples: Vec::new(),
        },
    }
}

#[test]
fn metric_query_order_returns_empty_and_singleton_label_counts() {
    for store_kind in [LabelSetStoreKind::FlatInterned, LabelSetStoreKind::Naive] {
        let mut stats = OtlpMetricsIngestionStats::new();
        let mut interner = LabelSetInterner::new(store_kind);
        let mut empty = Vec::new();

        assert_eq!(
            order_series_samples_for_metric_query(&mut empty, &interner).unwrap(),
            Vec::<u32>::new()
        );

        let normalized_name = normalize_label_name("pod.name");
        let series = interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "singleton.metric")),
                    KeyValueRef::from(("pod.name", "first")),
                    KeyValueRef::from((normalized_name.as_str(), "last")),
                ],
                &mut stats,
            )
            .unwrap();
        let sample = metric_order_test_samples(0, 1);
        let mut singleton = vec![(series, sample.clone())];
        let expected_count =
            u32::try_from(interner.segment_metadata(series).labels().len()).unwrap();

        assert_eq!(
            order_series_samples_for_metric_query(&mut singleton, &interner).unwrap(),
            vec![expected_count]
        );
        assert_eq!(singleton, vec![(series, sample)]);
    }
}

#[test]
fn fallback_metric_query_order_returns_label_counts_in_reordered_series_order() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::Naive);
    let a_series = interner
        .intern(
            &[KeyValueRef::from((METRIC_NAME_LABEL, "a.metric"))],
            &mut stats,
        )
        .unwrap();
    let normalized_name = normalize_label_name("pod.name");
    let middle_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "middle.metric")),
                KeyValueRef::from(("pod.name", "first")),
                KeyValueRef::from((normalized_name.as_str(), "last")),
            ],
            &mut stats,
        )
        .unwrap();
    let z_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "z.metric")),
                KeyValueRef::from(("namespace", "default")),
                KeyValueRef::from(("pod", "backend")),
            ],
            &mut stats,
        )
        .unwrap();
    let sample = metric_order_test_samples(0, 1);
    let mut source = vec![
        (z_series, sample.clone()),
        (middle_series, sample.clone()),
        (a_series, sample),
    ];

    let canonical_label_counts =
        order_series_samples_for_metric_query(&mut source, &interner).unwrap();

    assert_eq!(
        source.iter().map(|(series, _)| *series).collect::<Vec<_>>(),
        vec![a_series, middle_series, z_series]
    );
    assert_eq!(canonical_label_counts, vec![1, 2, 3]);
    assert_eq!(
        canonical_label_counts,
        source
            .iter()
            .map(|(series, _)| {
                u32::try_from(interner.segment_metadata(*series).labels().len()).unwrap()
            })
            .collect::<Vec<_>>()
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
fn canonical_metric_order_label_count_rejects_u32_overflow() {
    let error = checked_canonical_label_count(u32::MAX as usize + 1).unwrap_err();

    let crate::error::ErrorKind::IoError(error) = error.kind() else {
        panic!("expected an I/O error");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "canonical metric-order label count exceeds u32"
    );
}

#[test]
fn flat_metric_query_order_matches_metadata_order_for_normalized_labels() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let z_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "z.metric")),
                KeyValueRef::from(("pod.name", "z")),
            ],
            &mut stats,
        )
        .unwrap();
    let normalized_collision_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                KeyValueRef::from(("a.label", "dropped")),
                KeyValueRef::from(("a_label", "kept")),
            ],
            &mut stats,
        )
        .unwrap();
    let a_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "a.metric")),
                KeyValueRef::from(("pod.name", "a")),
            ],
            &mut stats,
        )
        .unwrap();

    let samples = SeriesSamples::Float {
        encoding: FloatEncoding::Gorilla,
        samples: vec![(1_000, 1.0)],
    };
    let source = vec![
        (z_series, samples.clone()),
        (normalized_collision_series, samples.clone()),
        (a_series, samples),
    ];
    let fast = assert_flat_metric_order_matches_owned_reference(&interner, source.clone());
    let mut fallback = source;

    let fallback_label_counts =
        order_series_samples_for_metric_query_with_metadata(&mut fallback, &interner).unwrap();

    let fast_refs: Vec<_> = fast.iter().map(|(series, _)| *series).collect();
    let fallback_refs: Vec<_> = fallback.iter().map(|(series, _)| *series).collect();
    assert_eq!(fast_refs, fallback_refs);
    assert_eq!(fallback_label_counts, vec![2, 3, 2]);
    assert_eq!(
        fast_refs,
        vec![a_series, normalized_collision_series, z_series]
    );
}

#[test]
fn metric_query_order_uses_source_series_ref_after_normalized_label_collision() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let normalized_name = normalize_label_name("a.label");
    let first = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                KeyValueRef::from(("a.label", "same")),
            ],
            &mut stats,
        )
        .unwrap();
    let second = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                KeyValueRef::from((normalized_name.as_str(), "same")),
            ],
            &mut stats,
        )
        .unwrap();
    assert_ne!(first, second);
    let first_metadata = interner.segment_metadata(first);
    let second_metadata = interner.segment_metadata(second);
    assert_eq!(
        (first_metadata.series_id(), first_metadata.labels()),
        (second_metadata.series_id(), second_metadata.labels()),
        "test setup must reach the exact canonical ordering tie"
    );

    let samples = SeriesSamples::Float {
        encoding: FloatEncoding::Gorilla,
        samples: vec![(1_000, 1.0)],
    };
    let expected = vec![first.min(second), first.max(second)];

    for input in [vec![first, second], vec![second, first]] {
        let source = input
            .into_iter()
            .map(|series| (series, samples.clone()))
            .collect::<Vec<_>>();
        let fast = assert_flat_metric_order_matches_owned_reference(&interner, source.clone());
        let mut fallback = source;

        order_series_samples_for_metric_query_with_metadata(&mut fallback, &interner).unwrap();

        assert_eq!(
            fast.iter().map(|(series, _)| *series).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(fast, fallback);
    }
}

#[test]
fn flat_metric_query_order_handles_metric_name_edges() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let raw_metric = "raw.metric";
    let normalized_metric = normalize_metric_name(raw_metric);
    let cases = [
        (Vec::new(), String::new()),
        (vec![KeyValueRef::from(("tag", "same"))], String::new()),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, "")),
                KeyValueRef::from(("tag", "same")),
            ],
            normalize_metric_name(""),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, "9invalid.metric")),
                KeyValueRef::from(("tag", "same")),
            ],
            normalize_metric_name("9invalid.metric"),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, raw_metric)),
                KeyValueRef::from(("tag", "same")),
            ],
            normalized_metric.clone(),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, normalized_metric.as_str())),
                KeyValueRef::from(("tag", "same")),
            ],
            normalized_metric.clone(),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, "z.first")),
                KeyValueRef::from(("tag", "same")),
            ],
            normalize_metric_name("z.first"),
        ),
    ];

    let mut expected = Vec::new();
    for (labels, projected_metric) in cases {
        let series = interner.intern(&labels, &mut stats).unwrap();
        expected.push((projected_metric, series));
    }
    expected.sort();

    let source = expected
        .iter()
        .rev()
        .enumerate()
        .map(|(index, (_, series))| (*series, metric_order_test_samples(0, index as u64 + 1)))
        .collect::<Vec<_>>();
    let ordered = assert_flat_metric_order_matches_owned_reference(&interner, source);

    assert_eq!(
        ordered
            .iter()
            .map(|(series, _)| *series)
            .collect::<Vec<_>>(),
        expected
            .into_iter()
            .map(|(_, series)| series)
            .collect::<Vec<_>>()
    );
}

#[test]
fn flat_metric_query_order_keeps_last_normalized_label_collision() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let raw_name = "a.label";
    let normalized_name = normalize_label_name(raw_name);
    let collision = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same")),
                KeyValueRef::from((raw_name, "z-first")),
                KeyValueRef::from((normalized_name.as_str(), "a-last")),
            ],
            &mut stats,
        )
        .unwrap();
    let middle = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same")),
                KeyValueRef::from((normalized_name.as_str(), "m-middle")),
            ],
            &mut stats,
        )
        .unwrap();
    let samples = metric_order_test_samples(0, 1);

    for refs in [[collision, middle], [middle, collision]] {
        let ordered = assert_flat_metric_order_matches_owned_reference(
            &interner,
            refs.into_iter()
                .map(|series| (series, samples.clone()))
                .collect(),
        );
        assert_eq!(
            ordered
                .iter()
                .map(|(series, _)| *series)
                .collect::<Vec<_>>(),
            [collision, middle]
        );
    }
}

#[test]
fn flat_metric_query_order_matches_reference_for_all_sample_kinds() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let series = interner
        .intern(
            &[KeyValueRef::from((METRIC_NAME_LABEL, "same"))],
            &mut stats,
        )
        .unwrap();
    let source = vec![
        (series, metric_order_test_samples(4, 10)),
        (series, metric_order_test_samples(1, 20)),
        (series, metric_order_test_samples(3, 30)),
        (series, metric_order_test_samples(0, 40)),
        (series, metric_order_test_samples(2, 50)),
    ];
    let ordered = assert_flat_metric_order_matches_owned_reference(&interner, source);

    assert!(matches!(ordered[0].1, SeriesSamples::Int64 { .. }));
    assert!(matches!(ordered[1].1, SeriesSamples::Float { .. }));
    assert!(matches!(ordered[2].1, SeriesSamples::Histogram { .. }));
    assert!(matches!(
        ordered[3].1,
        SeriesSamples::ExponentialHistogram { .. }
    ));
    assert!(matches!(ordered[4].1, SeriesSamples::Summary { .. }));
}

#[test]
fn flat_metric_query_order_uses_original_index_as_the_final_tie_break() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let series = interner
        .intern(
            &[KeyValueRef::from((METRIC_NAME_LABEL, "same"))],
            &mut stats,
        )
        .unwrap();
    let source = vec![
        (series, metric_order_test_samples(0, 30)),
        (series, metric_order_test_samples(0, 10)),
        (series, metric_order_test_samples(0, 20)),
    ];
    let ordered = assert_flat_metric_order_matches_owned_reference(&interner, source.clone());

    assert_eq!(ordered, source);
}

#[test]
fn flat_metric_query_order_matches_owned_reference_for_generated_inputs() {
    for store_kind in [
        LabelSetStoreKind::FlatInterned,
        LabelSetStoreKind::ExperimentalFlatInternedPaged,
    ] {
        let mut stats = OtlpMetricsIngestionStats::new();
        let mut interner = LabelSetInterner::new(store_kind);
        let raw_label = "generated.label";
        let normalized_label = normalize_label_name(raw_label);
        let raw_metric = "generated.metric";
        let normalized_metric = normalize_metric_name(raw_metric);
        let mut source = Vec::new();
        let mut state = 0x9e37_79b9_u32;

        for index in 0..512_usize {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let metric_case = (state as usize + index) % 6;
            let mut owned_labels = Vec::<(String, String)>::new();
            match metric_case {
                0 => {}
                1 => owned_labels.push((METRIC_NAME_LABEL.to_string(), String::new())),
                2 => owned_labels.push((METRIC_NAME_LABEL.to_string(), raw_metric.to_string())),
                3 => {
                    owned_labels.push((METRIC_NAME_LABEL.to_string(), normalized_metric.clone()));
                }
                4 => owned_labels.push((
                    METRIC_NAME_LABEL.to_string(),
                    format!("9invalid.metric.{}", index % 17),
                )),
                _ => owned_labels.push((
                    METRIC_NAME_LABEL.to_string(),
                    format!("first.metric.{}", index % 13),
                )),
            }
            owned_labels.push((
                format!("zone.{}", index % 7),
                format!("value-{}", state % 23),
            ));
            if index % 3 == 0 {
                owned_labels.push((raw_label.to_string(), format!("first-{}", index % 5)));
                owned_labels.push((
                    normalized_label.clone(),
                    format!("last-{}", (index + 1) % 5),
                ));
            }
            if index % 5 == 0 {
                owned_labels.push(("__reserved".to_string(), format!("r-{}", index % 19)));
            }
            owned_labels.sort_by(|left, right| left.0.cmp(&right.0));

            let labels = owned_labels
                .iter()
                .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str())))
                .collect::<Vec<_>>();
            let series = interner.intern(&labels, &mut stats).unwrap();
            source.push((
                series,
                metric_order_test_samples((state as usize) % 5, index as u64 + 1),
            ));
        }

        for index in 0..source.len() {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let swap_with = state as usize % source.len();
            source.swap(index, swap_with);
        }

        assert_flat_metric_order_matches_owned_reference(&interner, source);
    }
}

#[test]
fn indirect_metric_order_is_byte_identical_to_owned_reference_order() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let normalized_label = normalize_label_name("a.label");
    let series = [
        interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "z.metric")),
                    KeyValueRef::from(("pod", "z")),
                ],
                &mut stats,
            )
            .unwrap(),
        interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                    KeyValueRef::from(("a.label", "z-first")),
                    KeyValueRef::from((normalized_label.as_str(), "a-last")),
                ],
                &mut stats,
            )
            .unwrap(),
        interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "a.metric")),
                    KeyValueRef::from(("pod", "a")),
                ],
                &mut stats,
            )
            .unwrap(),
    ];
    let source = series
        .into_iter()
        .enumerate()
        .map(|(index, series)| (series, metric_order_test_samples(0, index as u64 + 1)))
        .collect::<Vec<_>>();

    let write = |root: &Path, indirect: bool| {
        let mut ordered = source.clone();
        if indirect {
            order_series_samples_for_metric_query(&mut ordered, &interner).unwrap();
        } else {
            order_flat_interned_series_samples_for_metric_query_owned_reference(
                &mut ordered,
                interner.as_flat_interned().unwrap(),
            )
            .unwrap();
        }
        assert_ne!(ordered, source);

        let mut writer = SegmentWriter::new(
            SegmentWriterConfig::new(root, Duration::from_secs(10))
                .with_deterministic_segment_ids(0x001d_1ec7)
                .with_storage_schema(SegmentStorageSchema::Schema8),
        )
        .unwrap();
        writer
            .reserve_metric_query_ordered_window_series(0, 10_000, ordered.len())
            .unwrap();
        for (series, samples) in ordered {
            let SeriesSamples::Float { samples, .. } = samples else {
                panic!("fixture uses float samples");
            };
            record_segment_float_samples(&interner, &mut writer, series, &samples, false).unwrap();
        }
        writer.flush().unwrap();
        assert_eq!(
            writer.last_flush_profile().unwrap().chunk_rewrite_frames(),
            0
        );
    };

    let indirect_root = tempdir.path().join("indirect");
    let reference_root = tempdir.path().join("reference");
    fs::create_dir_all(&indirect_root).unwrap();
    fs::create_dir_all(&reference_root).unwrap();
    write(&indirect_root, true);
    write(&reference_root, false);

    assert_eq!(
        snapshot_tree(&indirect_root),
        snapshot_tree(&reference_root)
    );
}

#[test]
fn processor_non_flat_metric_order_fallback_matches_flat_segment_bytes_and_readback() {
    fn write_fixture(root: &Path, store_kind: LabelSetStoreKind) -> Vec<(String, Vec<u8>)> {
        fs::create_dir_all(root).unwrap();
        let writer = SegmentWriter::new(
            SegmentWriterConfig::new(root, Duration::from_secs(10))
                .with_deterministic_segment_ids(0x00fa_11ba)
                .with_storage_schema(SegmentStorageSchema::Schema8),
        )
        .unwrap();
        let head = Some(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ));
        let mut processor =
            OtlpLabelSetProcessor::new(store_kind, Duration::from_secs(3600), head, Some(writer))
                .with_shutdown_report(false);

        // Intern the lexically later metric first so the head drains in the
        // opposite order from the metric-query layout. The two series also
        // have unequal canonical label counts (three versus two).
        let mut z = number_dp(vec![kv_str("namespace", "default"), kv_str("pod", "z")]);
        z.time_unix_nano = 5_000_000_000;
        z.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(30.0));
        let mut a = number_dp(vec![kv_str("pod", "a")]);
        a.time_unix_nano = 5_000_000_000;
        a.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(10.0));

        assert_eq!(
            processor
                .process(
                    SourceMessageMetadata {
                        topic: "metrics".to_owned(),
                        partition: 0,
                        offset: 0,
                        timestamp_ms: 5_000,
                        captured_at_ms: 10_000,
                    },
                    request(
                        vec![],
                        vec![
                            metric_gauge("z_metric", vec![z]),
                            metric_gauge("a_metric", vec![a]),
                        ],
                    ),
                )
                .unwrap(),
            ProcessResult::Ok
        );
        processor.flush_head().unwrap();
        let profile = processor.last_head_window_write_profile().unwrap();
        assert_eq!(profile.series, 2);
        assert_eq!(profile.datapoints, 2);
        drop(processor);

        snapshot_tree(root)
    }

    let tempdir = tempfile::tempdir().unwrap();
    let flat_root = tempdir.path().join("flat");
    let naive_root = tempdir.path().join("naive");
    let keyset_root = tempdir.path().join("keyset");
    let flat = write_fixture(&flat_root, LabelSetStoreKind::FlatInterned);
    let naive = write_fixture(&naive_root, LabelSetStoreKind::Naive);
    let keyset = write_fixture(&keyset_root, LabelSetStoreKind::KeySetDictEncoded);

    assert_eq!(
        naive, flat,
        "Naive fallback changed segment or manifest bytes"
    );
    assert_eq!(
        keyset, flat,
        "KeySetDictEncoded fallback changed segment or manifest bytes"
    );

    assert_promql_samples(
        &open_default_store(&keyset_root),
        r#"a_metric{pod="a"}"#,
        vec![(5_000, 10.0)],
    );
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
    ))
    .with_shutdown_report(false);

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
    let store = open_default_store(tempdir.path());
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
            request(
                vec![
                    kv_str("rejected.resource", "must-not-intern"),
                    kv_bytes("rejected.non-scalar", b"must-not-count"),
                ],
                vec![metric_gauge("cpu.usage", vec![missing])],
            ),
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
    let store_stats = processor.labelsets.stats();
    assert_eq!(store_stats.series, 0);
    assert_eq!(store_stats.symbols, Some(0));
    assert_eq!(snap.totals.skipped_non_scalar_values, 0);

    processor.flush_head().unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 0);
}

#[test]
fn processor_applies_the_same_event_time_policy_to_every_otlp_metric_kind() {
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        None,
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ))
    .with_shutdown_report(false);

    let timestamps = [
        ("accepted", 95_000),
        ("old", 89_999),
        ("future", 105_001),
        ("missing", 0),
    ];
    let number_points = || {
        timestamps
            .into_iter()
            .map(|(case, timestamp_ms)| {
                let mut point = number_dp(vec![kv_str("case", case)]);
                point.time_unix_nano = timestamp_ms * 1_000_000;
                point.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
                point
            })
            .collect::<Vec<_>>()
    };
    let histograms = timestamps
        .into_iter()
        .map(|(case, timestamp_ms)| {
            let mut point = histogram_dp(vec![kv_str("case", case)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 1;
            point.bucket_counts = vec![1];
            point
        })
        .collect();
    let exponential_histograms = timestamps
        .into_iter()
        .map(|(case, timestamp_ms)| {
            let mut point = exp_histogram_dp(vec![kv_str("case", case)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 1;
            point.zero_count = 1;
            point
        })
        .collect();
    let summaries = timestamps
        .into_iter()
        .map(|(case, timestamp_ms)| {
            let mut point = summary_dp(vec![kv_str("case", case)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 1;
            point.sum = 1.0;
            point
        })
        .collect();

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "metrics".to_string(),
                partition: 0,
                offset: 1,
                // This deliberately contradictory source timestamp is diagnostic only.
                timestamp_ms: 9_999_999,
                captured_at_ms: 100_000,
            },
            request(
                vec![],
                vec![
                    metric_gauge("policy.gauge", number_points()),
                    metric_sum("policy.sum", number_points()),
                    metric_histogram("policy.histogram", histograms),
                    metric_exp_histogram("policy.exponential_histogram", exponential_histograms),
                    metric_summary("policy.summary", summaries),
                ],
            ),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snapshot = processor.labelset_stats.snapshot();
    assert_eq!(snapshot.totals.observed_datapoints, 20);
    assert_eq!(snapshot.totals.datapoints, 5);
    assert_eq!(snapshot.totals.datapoint_policy.accepted, 5);
    assert_eq!(snapshot.totals.datapoint_policy.dropped_too_old, 5);
    assert_eq!(snapshot.totals.datapoint_policy.dropped_too_future, 5);
    assert_eq!(snapshot.totals.datapoint_policy.missing_timestamp, 5);
    assert_eq!(snapshot.totals.datapoint_types.gauge, 1);
    assert_eq!(snapshot.totals.datapoint_types.sum, 1);
    assert_eq!(snapshot.totals.datapoint_types.histogram, 1);
    assert_eq!(snapshot.totals.datapoint_types.exponential_histogram, 1);
    assert_eq!(snapshot.totals.datapoint_types.summary, 1);
    assert_eq!(processor.labelsets.stats().series, 5);
    let symbols = processor
        .labelsets
        .as_flat_interned()
        .expect("test uses the flat interned label store")
        .symbols();
    for rejected_value in ["old", "future", "missing"] {
        assert_eq!(
            symbols.lookup(rejected_value),
            None,
            "rejected datapoints must not intern symbol {rejected_value}"
        );
    }

    let head = &mut processor
        .partition_heads
        .values_mut()
        .next()
        .expect("partition head exists")
        .head;
    let samples = head
        .drain()
        .expect("accepted datapoints create a head window")
        .into_series_samples()
        .unwrap();
    assert_eq!(samples.len(), 5);
    for (_, samples) in samples {
        let timestamps = match samples {
            SeriesSamples::Float { samples, .. } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::Int64 { samples, .. } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::Histogram { samples } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::ExponentialHistogram { samples } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::Summary { samples } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
        };
        assert_eq!(timestamps, vec![95_000]);
    }
}

#[test]
fn ingest_moves_only_accepted_histogram_bucket_storage() {
    let head_config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head_config.clone()),
        None,
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ))
    .with_shutdown_report(false);
    let mut head_state = PartitionHead {
        head: HeadBuffer::new(head_config).unwrap(),
        stats: HeadBufferStats::new(),
        seal_ready_ranges: BTreeSet::new(),
    };

    let timestamps = [95_000, 89_999, 105_001, 0];
    let histograms = timestamps
        .into_iter()
        .enumerate()
        .map(|(index, timestamp_ms)| {
            let mut point = histogram_dp(vec![kv_int("case", index as i64)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 3;
            point.explicit_bounds = vec![index as f64 + 0.5];
            point.bucket_counts = vec![index as u64, 3 - index as u64];
            point
        })
        .collect();
    let exponential_histograms = timestamps
        .into_iter()
        .enumerate()
        .map(|(index, timestamp_ms)| {
            let mut point = exp_histogram_dp(vec![kv_int("case", index as i64)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 3;
            point.positive = Some(Buckets {
                offset: index as i32,
                bucket_counts: vec![index as u64],
            });
            point.negative = Some(Buckets {
                offset: -(index as i32),
                bucket_counts: vec![3 - index as u64],
            });
            point
        })
        .collect();
    let mut req = request(
        vec![],
        vec![
            metric_histogram("move.histogram", histograms),
            metric_exp_histogram("move.exponential_histogram", exponential_histograms),
        ],
    );

    let result = processor
        .ingest_otlp_metrics(&mut req, 100_000, Some(&mut head_state), true)
        .unwrap();
    assert_eq!(
        result,
        DatapointIngestResult {
            accepted: 2,
            dropped_too_old: 2,
            dropped_too_future: 2,
            missing_timestamp: 2,
            invalid_typed: 0,
        }
    );

    let metrics = &req.resource_metrics[0].scope_metrics[0].metrics;
    let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) = &metrics[0].data else {
        panic!("expected histogram metric");
    };
    assert!(histogram.data_points[0].explicit_bounds.is_empty());
    assert!(histogram.data_points[0].bucket_counts.is_empty());
    for (index, point) in histogram.data_points.iter().enumerate().skip(1) {
        assert_eq!(point.explicit_bounds, vec![index as f64 + 0.5]);
        assert_eq!(point.bucket_counts, vec![index as u64, 3 - index as u64]);
    }

    let Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) = &metrics[1].data
    else {
        panic!("expected exponential histogram metric");
    };
    assert!(histogram.data_points[0].positive.is_none());
    assert!(histogram.data_points[0].negative.is_none());
    for (index, point) in histogram.data_points.iter().enumerate().skip(1) {
        assert_eq!(
            point.positive.as_ref().unwrap().bucket_counts,
            vec![index as u64]
        );
        assert_eq!(
            point.negative.as_ref().unwrap().bucket_counts,
            vec![3 - index as u64]
        );
    }

    let samples = head_state
        .head
        .drain()
        .expect("accepted datapoints create a head window")
        .into_series_samples()
        .unwrap();
    assert_eq!(samples.len(), 2);
}

#[test]
fn live_and_wal_replay_preserve_label_allocation_and_typed_head_semantics() {
    let mut missing_value = number_dp(vec![kv_str("case", "missing-value")]);
    missing_value.time_unix_nano = 90_000_000_000;
    missing_value.value = None;
    let mut gauge = number_dp(vec![kv_str("case", "equivalence")]);
    gauge.time_unix_nano = 95_000_000_000;
    gauge.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.25));
    let mut sum = number_dp(vec![kv_str("case", "equivalence")]);
    sum.time_unix_nano = 95_000_000_000;
    sum.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));

    let histogram_point =
        |case: &str, start_ms: u64, timestamp_ms: u64, bucket_counts: Vec<u64>| {
            let mut point = histogram_dp(vec![kv_str("case", case)]);
            point.start_time_unix_nano = start_ms * 1_000_000;
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = bucket_counts.iter().sum();
            point.sum = Some(point.count as f64);
            point.explicit_bounds = vec![1.0];
            point.bucket_counts = bucket_counts;
            point
        };
    let mut cumulative_histogram = metric_histogram(
        "equivalence.cumulative_histogram",
        vec![
            histogram_point("cumulative", 80_000, 91_000, vec![4, 6]),
            histogram_point("cumulative", 80_000, 92_000, vec![5, 7]),
            histogram_point("cumulative", 92_000, 93_000, vec![1, 2]),
        ],
    );
    let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) =
        cumulative_histogram.data.as_mut()
    else {
        unreachable!("histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;

    let exponential_histogram_point =
        |case: &str, start_ms: u64, timestamp_ms: u64, bucket_counts: Vec<u64>| {
            let mut point = exp_histogram_dp(vec![kv_str("case", case)]);
            point.start_time_unix_nano = start_ms * 1_000_000;
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = bucket_counts.iter().sum();
            point.sum = Some(point.count as f64);
            point.positive = Some(Buckets {
                offset: 0,
                bucket_counts,
            });
            point
        };
    let mut cumulative_exponential_histogram = metric_exp_histogram(
        "equivalence.cumulative_exponential_histogram",
        vec![
            exponential_histogram_point("cumulative", 80_000, 94_000, vec![4, 6]),
            exponential_histogram_point("cumulative", 80_000, 95_000, vec![5, 7]),
            exponential_histogram_point("cumulative", 95_000, 96_000, vec![1, 2]),
        ],
    );
    let Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) =
        cumulative_exponential_histogram.data.as_mut()
    else {
        unreachable!("exponential histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;

    let mut delta_histogram = metric_histogram(
        "equivalence.delta_histogram",
        vec![histogram_point("delta", 96_000, 97_000, vec![1, 2])],
    );
    let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) =
        delta_histogram.data.as_mut()
    else {
        unreachable!("histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Delta as i32;

    let mut delta_exponential_histogram = metric_exp_histogram(
        "equivalence.delta_exponential_histogram",
        vec![exponential_histogram_point(
            "delta",
            97_000,
            98_000,
            vec![1, 2],
        )],
    );
    let Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) =
        delta_exponential_histogram.data.as_mut()
    else {
        unreachable!("exponential histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Delta as i32;

    let mut summary = summary_dp(vec![kv_str("case", "equivalence")]);
    summary.start_time_unix_nano = 90_000_000_000;
    summary.time_unix_nano = 95_000_000_000;
    summary.flags = 1;
    summary.count = 10;
    summary.sum = 50.0;
    summary.quantile_values = vec![ValueAtQuantile {
        quantile: 0.5,
        value: 4.0,
    }];

    let request = request(
        vec![
            kv_str("service.name", "checkout-first"),
            kv_str("service.name", "checkout"),
            kv_bytes("resource.unsupported", b"count-per-accepted-datapoint"),
        ],
        vec![
            metric_gauge("equivalence.gauge", vec![missing_value, gauge]),
            metric_sum("equivalence.sum", vec![sum]),
            cumulative_histogram,
            cumulative_exponential_histogram,
            delta_histogram,
            delta_exponential_histogram,
            metric_summary("equivalence.summary", vec![summary]),
        ],
    );
    let policy = EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    );

    let head_config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut live = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head_config.clone()),
        None,
    )
    .with_event_time_policy(policy)
    .with_shutdown_report(false);
    live.process(
        SourceMessageMetadata {
            topic: "metrics".to_string(),
            partition: 3,
            offset: 44,
            timestamp_ms: 9_999_999,
            captured_at_ms: 100_000,
        },
        request.clone(),
    )
    .unwrap();
    let live_snapshot = live.labelset_stats.snapshot();
    let live_series_count = live.labelsets.stats().series;
    let live_labelsets = (0..live_series_count)
        .map(|series| {
            collect_labelset(
                &live,
                SeriesRef::new(u32::try_from(series).expect("test series count fits u32")),
            )
        })
        .collect::<Vec<_>>();
    let live_samples = live
        .partition_heads
        .values_mut()
        .next()
        .expect("live partition head exists")
        .head
        .drain()
        .expect("live request creates a window")
        .into_series_samples()
        .unwrap();
    let mut live_by_labels = BTreeMap::new();
    for (series, samples) in live_samples {
        let mut labels = Vec::new();
        live.labelsets.visit_labelset(series, |key, value| {
            labels.push((key.to_string(), value.to_string()))
        });
        labels.sort();
        live_by_labels.insert(labels, samples);
    }

    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-live-equivalence.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            topic: "metrics".to_string(),
            partition: 3,
            offset: 44,
            source_timestamp_ms: 9_999_999,
            captured_at_ms: 100_000,
            payload: request.encode_to_vec(),
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut replay_head = HeadBuffer::new(head_config).unwrap();
    let mut replay_labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let replay_outcome =
        replay_wal_file_into_head(&wal_path, policy, &mut replay_head, &mut replay_labels).unwrap();
    assert!(replay_outcome.completed_windows.is_empty());
    let replay_partition = replay_outcome.partition.as_ref().unwrap();
    assert_eq!(replay_partition.topic, "metrics");
    assert_eq!(replay_partition.partition, 3);
    let replay_report = replay_outcome.report;
    let replay_labelsets = (0..replay_labels.len())
        .map(|series| {
            let mut labels = Vec::new();
            replay_labels.visit_labelset(
                SeriesRef::new(u32::try_from(series).expect("test series count fits u32")),
                |key, value| labels.push((key.to_string(), value.to_string())),
            );
            labels.sort();
            labels
        })
        .collect::<Vec<_>>();
    let replay_samples = replay_head
        .drain()
        .expect("WAL replay creates a window")
        .into_series_samples()
        .unwrap();
    let mut replay_by_labels = BTreeMap::new();
    for (series, samples) in replay_samples {
        let mut labels = Vec::new();
        replay_labels.visit_labelset(series, |key, value| {
            labels.push((key.to_string(), value.to_string()))
        });
        labels.sort();
        replay_by_labels.insert(labels, samples);
    }

    assert_eq!(live_snapshot.totals.datapoint_policy.accepted, 12);
    assert_eq!(live_snapshot.totals.datapoint_policy.rejected(), 0);
    assert_eq!(live_snapshot.totals.datapoint_storage.recorded_samples, 11);
    assert_eq!(
        live_snapshot.totals.datapoint_storage.missing_number_values,
        1
    );
    assert_eq!(live_series_count, 8);
    assert_eq!(live_snapshot.totals.skipped_non_scalar_values, 12);
    assert_eq!(replay_report.policy_accepted_datapoints, 12);
    assert_eq!(replay_report.dropped_too_old_datapoints, 0);
    assert_eq!(replay_report.dropped_too_future_datapoints, 0);
    assert_eq!(replay_report.missing_timestamp_datapoints, 0);
    assert_eq!(replay_report.datapoints_replayed, 11);
    assert_eq!(replay_report.skipped_non_scalar_labels, 12);
    assert_eq!(replay_labelsets, live_labelsets);
    assert_eq!(replay_by_labels, live_by_labels);
    assert_eq!(
        reset_hints_for_metric(&replay_by_labels, "equivalence.cumulative_histogram"),
        vec![
            CounterResetHint::Unknown,
            CounterResetHint::NotCounterReset,
            CounterResetHint::CounterReset,
        ]
    );
    assert_eq!(
        reset_hints_for_metric(
            &replay_by_labels,
            "equivalence.cumulative_exponential_histogram"
        ),
        vec![
            CounterResetHint::Unknown,
            CounterResetHint::NotCounterReset,
            CounterResetHint::CounterReset,
        ]
    );
    assert_eq!(
        reset_hints_for_metric(&replay_by_labels, "equivalence.delta_histogram"),
        vec![CounterResetHint::NotCounterReset]
    );
    assert_eq!(
        reset_hints_for_metric(&replay_by_labels, "equivalence.delta_exponential_histogram"),
        vec![CounterResetHint::NotCounterReset]
    );
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
    let store = open_default_store(tempdir.path());
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
fn processor_rejects_invalid_histogram_before_reset_and_head_mutation() {
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        None,
    )
    .with_shutdown_report(false);

    let make_point = |timestamp_ms: u64, pod: &str, count: u64, bucket_counts: Vec<u64>| {
        let mut point = histogram_dp(vec![kv_str("pod.name", pod)]);
        point.time_unix_nano = timestamp_ms * 1_000_000;
        point.start_time_unix_nano = 500_000_000;
        point.count = count;
        point.explicit_bounds = vec![1.0];
        point.bucket_counts = bucket_counts;
        point
    };
    let metric = tonic::metrics::v1::Metric {
        name: "request.duration".to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Histogram(
            tonic::metrics::v1::Histogram {
                aggregation_temporality: tonic::metrics::v1::AggregationTemporality::Cumulative
                    as i32,
                data_points: vec![
                    make_point(1_000, "same", 10, vec![4, 6]),
                    make_point(2_000, "same", u64::MAX, vec![u64::MAX, 1]),
                    make_point(2_500, "invalid-only", u64::MAX, vec![u64::MAX, 1]),
                    make_point(3_000, "same", 12, vec![5, 7]),
                ],
            },
        )),
        ..Default::default()
    };

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "metrics".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 3_000,
                captured_at_ms: 3_000,
            },
            request(vec![], vec![metric]),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snapshot = processor.labelset_stats.snapshot();
    assert_eq!(snapshot.totals.messages, 1);
    assert_eq!(snapshot.totals.datapoint_policy.accepted, 4);
    assert_eq!(snapshot.totals.datapoint_storage.recorded_samples, 2);
    assert_eq!(snapshot.totals.datapoint_storage.invalid_typed_values, 2);
    assert_eq!(processor.labelsets.stats().series, 1);

    let window = processor
        .partition_heads
        .values_mut()
        .next()
        .unwrap()
        .head
        .drain()
        .unwrap();
    let mut samples = window.into_series_samples().unwrap();
    assert_eq!(samples.len(), 1);
    let SeriesSamples::Histogram { samples } = samples.pop().unwrap().1 else {
        panic!("expected histogram samples");
    };
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].0, 1_000);
    assert_eq!(samples[0].1.metadata.reset_hint, CounterResetHint::Unknown);
    assert_eq!(samples[1].0, 3_000);
    assert_eq!(
        samples[1].1.metadata.reset_hint,
        CounterResetHint::NotCounterReset
    );
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
            kv_double("double_value", 314.0 / 100.0),
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
    let metric_types = OtlpDataTypeCounts {
        gauge: 1,
        sum: 2,
        histogram: 3,
        exponential_histogram: 4,
        summary: 5,
    };
    let observed_datapoint_types = OtlpDataTypeCounts {
        gauge: 10,
        sum: 20,
        histogram: 30,
        exponential_histogram: 40,
        summary: 50,
    };
    let accepted_datapoint_types = OtlpDataTypeCounts {
        gauge: 8,
        sum: 18,
        histogram: 28,
        exponential_histogram: 38,
        summary: 48,
    };

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
        invalid_typed_values: 1,
    };
    let window = DatapointStorageCounts {
        recorded_samples: 3,
        missing_number_values: 1,
        invalid_typed_values: 0,
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
    assert!(markdown.contains("| Invalid Typed Value | 1 | 0 |"));
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
    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
        314.0 / 100.0,
    ));
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

    let meta = read_segment_meta(&seg_dir);
    assert_eq!(meta.datapoints, 1);
    assert_eq!(meta.series, 1);
    let chunk_len = fs::metadata(seg_dir.join(SegmentFile::Chunks.filename()))
        .unwrap()
        .len();
    assert!(chunk_len > 0);
}

#[test]
fn processor_writes_segment_series_metadata_and_exact_postings() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
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
    let symbols = read_symbols_bin(
        File::open(seg_dir.join(SegmentFile::Symbols.filename())).expect("open symbols"),
    )
    .unwrap();
    let series = read_series_bin(
        File::open(seg_dir.join(SegmentFile::Series.filename())).expect("open series"),
    )
    .unwrap();
    let indexes = read_segment_indexes(
        File::open(seg_dir.join(SegmentFile::Indexes.filename())).expect("open indexes"),
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
fn processor_writes_head_window_chunks_in_metric_query_order_without_rewrite() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
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

    let mut z_dp = number_dp(vec![kv_str("pod.name", "z")]);
    z_dp.time_unix_nano = 5_000_000_000;
    z_dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(10.0));
    let mut a_dp = number_dp(vec![kv_str("pod.name", "a")]);
    a_dp.time_unix_nano = 5_000_000_000;
    a_dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(20.0));

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_003,
            },
            request(
                vec![],
                vec![
                    metric_gauge("z.metric", vec![z_dp]),
                    metric_gauge("a.metric", vec![a_dp]),
                ],
            ),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let writer = processor.segment_writer.as_ref().unwrap();
    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.chunk_rewrite_frames(), 0);
    assert_eq!(profile.chunk_rewrite_payload_bytes(), 0);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let symbols = read_symbols_bin(
        File::open(seg_dir.join(SegmentFile::Symbols.filename())).expect("open symbols"),
    )
    .unwrap();
    let series = read_series_bin(
        File::open(seg_dir.join(SegmentFile::Series.filename())).expect("open series"),
    )
    .unwrap();
    let mut chunk_index = File::open(seg_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    let chunk_entries = read_chunk_index(&mut chunk_index).unwrap();
    assert_eq!(series.len(), 2);
    assert_eq!(chunk_entries.len(), 2);

    let metric_sym = symbols.lookup(METRIC_NAME_LABEL).unwrap();
    let metric_names = series
        .iter()
        .map(|entry| {
            entry
                .labels
                .iter()
                .find_map(|(key, value)| (*key == metric_sym).then_some(*value))
                .and_then(|sym| symbols.resolve(sym))
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        metric_names,
        vec![
            normalize_metric_name("a.metric"),
            normalize_metric_name("z.metric")
        ]
    );

    let chunk_offsets = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            entries[0].offset
        })
        .collect::<Vec<_>>();
    assert_eq!(chunk_offsets, {
        let mut sorted = chunk_offsets.clone();
        sorted.sort_unstable();
        sorted
    });
}

#[test]
fn processor_writes_raw_head_float_samples_through_deferred_metadata() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );
    let mut datapoint = number_dp(vec![kv_str("pod.name", "raw")]);
    datapoint.time_unix_nano = 5_000_000_000;
    datapoint.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(-0.0));

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_004,
            },
            request(vec![], vec![metric_gauge("raw.metric", vec![datapoint])]),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let segment = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let mut chunks = ChunkReader::new(
        File::open(segment.join(SegmentFile::Chunks.filename())).expect("open chunks"),
    );
    let record = chunks.read_next().unwrap().unwrap();
    let ChunkSamples::Float(samples) = record.samples else {
        panic!("expected raw float chunk");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].0, 5_000);
    assert_eq!(samples[0].1.to_bits(), (-0.0_f64).to_bits());
    assert!(chunks.read_next().unwrap().is_none());
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

    let metric = normalize_metric_name("requests.total");
    let pod_label = normalize_label_name("pod.name");
    let results = open_default_store(tempdir.path())
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
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
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
    let meta = read_segment_meta(&seg_dir);
    assert_eq!(meta.datapoints, 3);
    assert_eq!(meta.series, 3);

    let series = read_series_bin(
        File::open(seg_dir.join(SegmentFile::Series.filename())).expect("open series"),
    )
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

    let mut chunk_reader = ChunkReader::new(
        File::open(seg_dir.join(SegmentFile::Chunks.filename())).expect("open chunks"),
    );
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
    let store = open_default_store(tempdir.path()).with_query_projection_config(
        QueryProjectionConfig::default()
            .with_exponential_histogram_bucket_boundaries(vec![2.0, 4.0]),
    );

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

fn process_gauge_sample(
    processor: &mut OtlpLabelSetProcessor,
    timestamp_ms: u64,
    value: f64,
    offset: i64,
) {
    let mut point = number_dp(vec![kv_str("pod.name", "backend-1")]);
    point.time_unix_nano = timestamp_ms * 1_000_000;
    point.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
        value,
    ));
    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset,
                timestamp_ms: i64::try_from(timestamp_ms).unwrap(),
                captured_at_ms: 20_000,
            },
            request(vec![], vec![metric_gauge("cpu_usage", vec![point])]),
        )
        .unwrap();
}

fn ooo_test_processor(segments_dir: &Path) -> OtlpLabelSetProcessor {
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        segments_dir,
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
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
    let mut chunk_reader = ChunkReader::new(
        File::open(seg_dir.join(SegmentFile::Chunks.filename())).expect("open chunks"),
    );
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
fn processor_final_flush_merges_preseal_ooo_into_one_in_order_segment() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut processor = ooo_test_processor(tempdir.path());

    process_gauge_sample(&mut processor, 4_000, 1.0, 0);
    process_gauge_sample(&mut processor, 5_000, 2.0, 1);
    let mut late_int = number_dp(vec![kv_str("pod.name", "backend-1")]);
    late_int.time_unix_nano = 4_000_000_000;
    late_int.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(3));
    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 2,
                timestamp_ms: 4_000,
                captured_at_ms: 20_000,
            },
            request(vec![], vec![metric_gauge("cpu_usage", vec![late_int])]),
        )
        .unwrap();

    processor.flush_head().unwrap();

    let segments = segment_dirs_for_range(tempdir.path(), 0, 10_000);
    assert_eq!(
        segments.len(),
        1,
        "co-resident active and OOO lanes must seal once"
    );
    assert!(
        fs::metadata(segments[0].join(SegmentFile::Chunks.filename()))
            .unwrap()
            .len()
            > 0
    );
    assert_eq!(
        fs::metadata(segments[0].join(SegmentFile::OooChunks.filename()))
            .unwrap()
            .len(),
        0,
        "pre-seal OOO belongs in the canonical in-order payload"
    );
    assert_eq!(
        read_segment_meta(&segments[0]).datapoints,
        2,
        "segment metadata must count the deduplicated physical rows"
    );

    let store = SegmentStoreReader::open_manifest_published(
        tempdir.path(),
        tempdir.path().join("manifest"),
    )
    .unwrap();
    assert_promql_samples(&store, "cpu_usage", vec![(4_000, 3.0), (5_000, 2.0)]);
}

#[test]
fn processor_rotation_merges_preseal_ooo_into_the_base_segment() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut processor = ooo_test_processor(tempdir.path());

    process_gauge_sample(&mut processor, 4_000, 1.0, 0);
    process_gauge_sample(&mut processor, 5_000, 2.0, 1);
    process_gauge_sample(&mut processor, 4_500, 2.5, 2);
    process_gauge_sample(&mut processor, 4_000, 3.0, 3);
    process_gauge_sample(&mut processor, 10_000, 4.0, 4);

    let segments = segment_dirs_for_range(tempdir.path(), 0, 10_000);
    assert_eq!(
        segments.len(),
        1,
        "rotation must merge the matching OOO buffer before publishing"
    );
    assert!(
        fs::metadata(segments[0].join(SegmentFile::Chunks.filename()))
            .unwrap()
            .len()
            > 0
    );
    assert_eq!(
        fs::metadata(segments[0].join(SegmentFile::OooChunks.filename()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        read_segment_meta(&segments[0]).datapoints,
        3,
        "segment metadata must count the sorted, deduplicated physical rows"
    );

    let store = SegmentStoreReader::open_manifest_published(
        tempdir.path(),
        tempdir.path().join("manifest"),
    )
    .unwrap();
    assert_promql_samples(
        &store,
        "cpu_usage",
        vec![(4_000, 3.0), (4_500, 2.5), (5_000, 2.0)],
    );
}

#[test]
fn processor_preseal_merge_preserves_multiple_kinds_for_one_flat_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut processor = ooo_test_processor(tempdir.path());

    process_gauge_sample(&mut processor, 4_000, 1.0, 0);
    process_gauge_sample(&mut processor, 5_000, 2.0, 1);

    let mut histogram = histogram_dp(vec![kv_str("pod.name", "backend-1")]);
    histogram.start_time_unix_nano = 1_000_000_000;
    histogram.time_unix_nano = 4_000_000_000;
    histogram.count = 4;
    histogram.sum = Some(10.0);
    histogram.min = Some(1.0);
    histogram.max = Some(4.0);
    histogram.explicit_bounds = vec![1.0, 5.0];
    histogram.bucket_counts = vec![1, 2, 1];
    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 2,
                timestamp_ms: 4_000,
                captured_at_ms: 20_000,
            },
            request(vec![], vec![metric_histogram("cpu_usage", vec![histogram])]),
        )
        .unwrap();
    process_gauge_sample(&mut processor, 10_000, 3.0, 3);

    let segments = segment_dirs_for_range(tempdir.path(), 0, 10_000);
    assert_eq!(segments.len(), 1);
    assert!(
        fs::metadata(segments[0].join(SegmentFile::Chunks.filename()))
            .unwrap()
            .len()
            > 0
    );
    assert_eq!(
        fs::metadata(segments[0].join(SegmentFile::OooChunks.filename()))
            .unwrap()
            .len(),
        0
    );

    let meta = read_segment_meta(&segments[0]);
    assert_eq!(meta.series, 1);
    assert_eq!(meta.datapoints, 3);

    let mut chunks = ChunkReader::new(
        File::open(segments[0].join(SegmentFile::Chunks.filename())).expect("open chunks"),
    );
    let mut float_samples = None;
    let mut histogram_samples = None;
    while let Some(chunk) = chunks.read_next().unwrap() {
        match chunk.samples {
            ChunkSamples::Float(samples) => float_samples = Some(samples),
            ChunkSamples::Histogram(samples) => histogram_samples = Some(samples),
            other => panic!("unexpected co-sealed chunk kind: {other:?}"),
        }
    }
    assert_eq!(float_samples, Some(vec![(4_000, 1.0), (5_000, 2.0)]));
    let histogram_samples = histogram_samples.expect("histogram stream must survive co-seal");
    assert_eq!(histogram_samples.len(), 1);
    assert_eq!(histogram_samples[0].0, 4_000);
    assert_eq!(histogram_samples[0].1.count, 4);
    assert_eq!(histogram_samples[0].1.sum, Some(10.0));

    let store = SegmentStoreReader::open_manifest_published(
        tempdir.path(),
        tempdir.path().join("manifest"),
    )
    .unwrap();
    let smoke = store.smoke_verify(0, 10_000, 1).unwrap();
    assert!(
        smoke
            .sample_series
            .iter()
            .any(|sample| sample.kind == ChunkKind::Float)
    );
    assert!(
        smoke
            .sample_series
            .iter()
            .any(|sample| sample.kind == ChunkKind::Histogram)
    );
}

#[test]
fn processor_query_merges_active_preseal_ooo_and_postseal_ooo() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut processor = ooo_test_processor(tempdir.path());

    process_gauge_sample(&mut processor, 4_000, 1.0, 0);
    process_gauge_sample(&mut processor, 5_000, 2.0, 1);
    process_gauge_sample(&mut processor, 4_500, 2.5, 2);
    process_gauge_sample(&mut processor, 4_000, 3.0, 3);
    process_gauge_sample(&mut processor, 10_000, 10.0, 4);

    let base_segments = segment_dirs_for_range(tempdir.path(), 0, 10_000);
    assert_eq!(
        base_segments.len(),
        1,
        "rotation must first publish one pre-seal-coalesced base segment"
    );
    assert!(
        fs::metadata(base_segments[0].join(SegmentFile::Chunks.filename()))
            .unwrap()
            .len()
            > 0
    );
    assert_eq!(
        fs::metadata(base_segments[0].join(SegmentFile::OooChunks.filename()))
            .unwrap()
            .len(),
        0
    );
    let store = SegmentStoreReader::open_manifest_published(
        tempdir.path(),
        tempdir.path().join("manifest"),
    )
    .unwrap();
    assert_promql_samples(
        &store,
        "cpu_usage",
        vec![(4_000, 3.0), (4_500, 2.5), (5_000, 2.0)],
    );
    drop(store);

    process_gauge_sample(&mut processor, 4_750, 3.5, 5);
    process_gauge_sample(&mut processor, 4_000, 4.0, 6);
    processor.flush_head().unwrap();

    let overlapping_segments = segment_dirs_for_range(tempdir.path(), 0, 10_000);
    assert_eq!(
        overlapping_segments.len(),
        2,
        "post-seal OOO must add one newer overlapping segment"
    );
    let payload_sizes = overlapping_segments
        .iter()
        .map(|segment| {
            (
                fs::metadata(segment.join(SegmentFile::Chunks.filename()))
                    .unwrap()
                    .len(),
                fs::metadata(segment.join(SegmentFile::OooChunks.filename()))
                    .unwrap()
                    .len(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        payload_sizes
            .iter()
            .filter(|(chunks, ooo)| *chunks > 0 && *ooo == 0)
            .count(),
        1,
        "exactly one base segment must use chunks.bin: {payload_sizes:?}"
    );
    assert_eq!(
        payload_sizes
            .iter()
            .filter(|(chunks, ooo)| *chunks == 0 && *ooo > 0)
            .count(),
        1,
        "exactly one late segment must use ooo_chunks.bin: {payload_sizes:?}"
    );

    let store = SegmentStoreReader::open_manifest_published(
        tempdir.path(),
        tempdir.path().join("manifest"),
    )
    .unwrap();
    assert_promql_samples(
        &store,
        "cpu_usage",
        vec![
            (4_000, 4.0),
            (4_500, 2.5),
            (4_750, 3.5),
            (5_000, 2.0),
            (10_000, 10.0),
        ],
    );
}

#[test]
fn processor_routes_postseal_ooo_to_ooo_chunks_with_late_duplicate_precedence() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut processor = ooo_test_processor(tempdir.path());

    process_gauge_sample(&mut processor, 4_000, 1.0, 0);
    process_gauge_sample(&mut processor, 10_000, 2.0, 1);
    assert_eq!(
        segment_dirs_for_range(tempdir.path(), 0, 10_000).len(),
        1,
        "advancing to the next window must publish the base segment"
    );

    process_gauge_sample(&mut processor, 4_500, 2.5, 2);
    process_gauge_sample(&mut processor, 4_000, 3.0, 3);
    processor.flush_head().unwrap();

    let segments = segment_dirs_for_range(tempdir.path(), 0, 10_000);
    assert_eq!(
        segments.len(),
        2,
        "late data must use an overlapping segment"
    );
    let payload_sizes = segments
        .iter()
        .map(|segment| {
            (
                fs::metadata(segment.join(SegmentFile::Chunks.filename()))
                    .unwrap()
                    .len(),
                fs::metadata(segment.join(SegmentFile::OooChunks.filename()))
                    .unwrap()
                    .len(),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        payload_sizes
            .iter()
            .any(|(chunks, ooo)| *chunks > 0 && *ooo == 0),
        "the base segment must remain in chunks.bin: {payload_sizes:?}"
    );
    assert!(
        payload_sizes
            .iter()
            .any(|(chunks, ooo)| *chunks == 0 && *ooo > 0),
        "the post-seal segment must route all payload into ooo_chunks.bin: {payload_sizes:?}"
    );

    let store = SegmentStoreReader::open_manifest_published(
        tempdir.path(),
        tempdir.path().join("manifest"),
    )
    .unwrap();
    assert_promql_samples(
        &store,
        "cpu_usage",
        vec![(4_000, 3.0), (4_500, 2.5), (10_000, 2.0)],
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

    let store = open_default_store(tempdir.path());
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
