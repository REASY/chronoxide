use super::*;
use crate::processor::{EventTimePolicy, OtlpLabelSetProcessor};
use crate::source::FileSource;
use crate::source::SourceMessage;
use chronoxide_capture::{CompressionMethod, OtlpCaptureWriter};
use chronoxide_core::labels::METRIC_NAME_LABEL;
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{FloatEncoding, HeadConfig, IntEncoding};
use chronoxide_core::storage::live_coverage::MessageSequence;
use chronoxide_core::storage::segment::{SegmentStoreReader, SegmentWriter, SegmentWriterConfig};
use opentelemetry_proto::tonic;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

struct MockSource {
    messages: VecDeque<SourceMessage>,
    flush_count: Arc<AtomicUsize>,
}

impl MockSource {
    fn new(messages: Vec<SourceMessage>, flush_count: Arc<AtomicUsize>) -> Self {
        Self {
            messages: messages.into(),
            flush_count,
        }
    }
}

impl MessageSource for MockSource {
    fn next_message(&mut self) -> Result<Option<SourceMessage>, ChronoxideError> {
        Ok(self.messages.pop_front())
    }

    fn flush(&mut self) -> Result<(), ChronoxideError> {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct MockProcessor {
    processed_count: usize,
    shutdown_count: Arc<AtomicUsize>,
}

impl MockProcessor {
    fn new(shutdown_count: Arc<AtomicUsize>) -> Self {
        Self {
            processed_count: 0,
            shutdown_count,
        }
    }
}

impl Processor for MockProcessor {
    fn process(
        &mut self,
        _metadata: SourceMessageMetadata,
        _decoded: ExportMetricsServiceRequest,
    ) -> Result<ProcessResult, ChronoxideError> {
        self.processed_count += 1;
        Ok(ProcessResult::Ok)
    }

    fn force_report(&mut self) {}

    fn shutdown(&mut self) -> Result<(), ChronoxideError> {
        self.shutdown_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// Helper to create a dummy valid OTLP message payload
fn create_dummy_payload() -> Vec<u8> {
    let req = ExportMetricsServiceRequest::default();
    let mut buf = Vec::new();
    req.encode(&mut buf).unwrap();
    buf
}

fn create_metric_payload(timestamp_ms: u64, value: f64) -> Vec<u8> {
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![tonic::metrics::v1::ResourceMetrics {
            resource: None,
            scope_metrics: vec![tonic::metrics::v1::ScopeMetrics {
                metrics: vec![tonic::metrics::v1::Metric {
                    name: "cpu.usage".to_string(),
                    data: Some(tonic::metrics::v1::metric::Data::Gauge(
                        tonic::metrics::v1::Gauge {
                            data_points: vec![tonic::metrics::v1::NumberDataPoint {
                                attributes: vec![tonic::common::v1::KeyValue {
                                    key: "pod.name".to_string(),
                                    value: Some(tonic::common::v1::AnyValue {
                                        value: Some(
                                            tonic::common::v1::any_value::Value::StringValue(
                                                "backend-1".to_string(),
                                            ),
                                        ),
                                    }),
                                    key_strindex: 0,
                                }],
                                time_unix_nano: timestamp_ms * 1_000_000,
                                value: Some(
                                    tonic::metrics::v1::number_data_point::Value::AsDouble(value),
                                ),
                                ..Default::default()
                            }],
                        },
                    )),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut buf = Vec::new();
    req.encode(&mut buf).unwrap();
    buf
}

#[test]
fn test_ingester_flow() {
    let shutdown_count = Arc::new(AtomicUsize::new(0));
    let flush_count = Arc::new(AtomicUsize::new(0));

    let messages = vec![
        SourceMessage {
            topic: "test".to_string(),
            partition: 0,
            offset: 1,
            timestamp_ms: 1000,
            captured_at_ms: 10_000,
            payload: create_dummy_payload(),
        },
        SourceMessage {
            topic: "test".to_string(),
            partition: 0,
            offset: 2,
            timestamp_ms: 2000,
            captured_at_ms: 10_001,
            payload: create_dummy_payload(),
        },
    ];

    let source = MockSource::new(messages, flush_count.clone());
    let processor = MockProcessor::new(shutdown_count.clone());
    let meter = opentelemetry::global::meter("test");
    let ct = CancellationToken::new();

    let config = IngestionConfig {
        max_event_age: TimeDelta::seconds(3600),
        max_event_lead: TimeDelta::seconds(3600),
        drop_outdated: false,
        labelset_store: LabelSetStoreKind::FlatInterned,
        labelset_report_interval: Duration::from_secs(60),
        stop_after_messages: None,
        replay_from: None,
        capture_to: None,
        capture_only: false,
        segment_writer: None,
    };

    let mut ingester = Ingester::new(source, config, processor, meter, ct).unwrap();
    ingester.start().unwrap();

    assert_eq!(ingester.processor.processed_count, 2);
    assert_eq!(shutdown_count.load(Ordering::Relaxed), 1);
    assert_eq!(flush_count.load(Ordering::Relaxed), 1);
}

#[test]
fn file_replay_uses_captured_at_for_event_time_policy() {
    let capture_dir = tempfile::tempdir().unwrap();
    let mut capture =
        OtlpCaptureWriter::create(capture_dir.path(), "topic", CompressionMethod::Uncompressed)
            .unwrap();
    let payload = create_metric_payload(95_000, 1.5);
    capture
        .append(1, 42, 9_999_999_999_999, 100_000, payload.as_slice())
        .unwrap();
    capture.close().unwrap();

    let segments_dir = tempfile::tempdir().unwrap();
    let source = FileSource::new(capture_dir.path().to_path_buf()).unwrap();
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
    ));
    let meter = opentelemetry::global::meter("test");
    let ct = CancellationToken::new();
    let config = IngestionConfig {
        max_event_age: TimeDelta::seconds(10),
        max_event_lead: TimeDelta::seconds(0),
        drop_outdated: true,
        labelset_store: LabelSetStoreKind::FlatInterned,
        labelset_report_interval: Duration::from_secs(3600),
        stop_after_messages: None,
        replay_from: Some(capture_dir.path().to_path_buf()),
        capture_to: None,
        capture_only: false,
        segment_writer: None,
    };

    let mut ingester = Ingester::new(source, config, processor, meter, ct).unwrap();
    ingester.start().unwrap();

    let store = SegmentStoreReader::open(segments_dir.path()).unwrap();
    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = store
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "backend-1"),
            ],
            0,
            200_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(95_000, 1.5)]);
}

#[test]
fn test_ingester_shutdown_on_immediate_cancel() {
    let shutdown_count = Arc::new(AtomicUsize::new(0));
    let flush_count = Arc::new(AtomicUsize::new(0));

    let source = MockSource::new(Vec::new(), flush_count.clone());
    let processor = MockProcessor::new(shutdown_count.clone());
    let meter = opentelemetry::global::meter("test");
    let ct = CancellationToken::new();
    ct.cancel();

    let config = IngestionConfig {
        max_event_age: TimeDelta::seconds(3600),
        max_event_lead: TimeDelta::seconds(3600),
        drop_outdated: false,
        labelset_store: LabelSetStoreKind::FlatInterned,
        labelset_report_interval: Duration::from_secs(60),
        stop_after_messages: None,
        replay_from: None,
        capture_to: None,
        capture_only: false,
        segment_writer: None,
    };

    let mut ingester = Ingester::new(source, config, processor, meter, ct).unwrap();
    ingester.start().unwrap();

    assert_eq!(shutdown_count.load(Ordering::Relaxed), 1);
    assert_eq!(flush_count.load(Ordering::Relaxed), 1);
}

struct ShutdownErrorProcessor;

impl Processor for ShutdownErrorProcessor {
    fn process(
        &mut self,
        _metadata: SourceMessageMetadata,
        _decoded: ExportMetricsServiceRequest,
    ) -> Result<ProcessResult, ChronoxideError> {
        Ok(ProcessResult::Ok)
    }

    fn force_report(&mut self) {}

    fn shutdown(&mut self) -> Result<(), ChronoxideError> {
        Err(std::io::Error::other("head flush failed").into())
    }
}

#[test]
fn processor_shutdown_failure_makes_ingester_fail() {
    let flush_count = Arc::new(AtomicUsize::new(0));
    let source = MockSource::new(Vec::new(), flush_count.clone());
    let meter = opentelemetry::global::meter("test");
    let ct = CancellationToken::new();
    let config = IngestionConfig {
        max_event_age: TimeDelta::seconds(3600),
        max_event_lead: TimeDelta::seconds(3600),
        drop_outdated: false,
        labelset_store: LabelSetStoreKind::FlatInterned,
        labelset_report_interval: Duration::from_secs(60),
        stop_after_messages: None,
        replay_from: None,
        capture_to: None,
        capture_only: false,
        segment_writer: None,
    };

    let mut ingester = Ingester::new(source, config, ShutdownErrorProcessor, meter, ct).unwrap();
    let error = ingester.start().unwrap_err();

    assert!(matches!(error.kind(), ErrorKind::IoError(_)));
    assert_eq!(flush_count.load(Ordering::Relaxed), 1);
}

struct SinkClosedProcessor {
    shutdown_count: Arc<AtomicUsize>,
}

impl SinkClosedProcessor {
    fn new(shutdown_count: Arc<AtomicUsize>) -> Self {
        Self { shutdown_count }
    }
}

impl Processor for SinkClosedProcessor {
    fn process(
        &mut self,
        _metadata: SourceMessageMetadata,
        _decoded: ExportMetricsServiceRequest,
    ) -> Result<ProcessResult, ChronoxideError> {
        Ok(ProcessResult::SinkChannelClosed("test-sink".to_string()))
    }

    fn force_report(&mut self) {}

    fn shutdown(&mut self) -> Result<(), ChronoxideError> {
        self.shutdown_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn test_ingester_flushes_on_sink_channel_closed() {
    let shutdown_count = Arc::new(AtomicUsize::new(0));
    let flush_count = Arc::new(AtomicUsize::new(0));

    let messages = vec![SourceMessage {
        topic: "test".to_string(),
        partition: 0,
        offset: 1,
        timestamp_ms: 1000,
        captured_at_ms: 10_000,
        payload: create_dummy_payload(),
    }];

    let source = MockSource::new(messages, flush_count.clone());
    let processor = SinkClosedProcessor::new(shutdown_count.clone());
    let meter = opentelemetry::global::meter("test");
    let ct = CancellationToken::new();

    let config = IngestionConfig {
        max_event_age: TimeDelta::seconds(3600),
        max_event_lead: TimeDelta::seconds(3600),
        drop_outdated: false,
        labelset_store: LabelSetStoreKind::FlatInterned,
        labelset_report_interval: Duration::from_secs(60),
        stop_after_messages: None,
        replay_from: None,
        capture_to: None,
        capture_only: false,
        segment_writer: None,
    };

    let mut ingester = Ingester::new(source, config, processor, meter, ct).unwrap();
    let err = ingester.start().unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::ChannelError(_)));

    assert_eq!(shutdown_count.load(Ordering::Relaxed), 1);
    assert_eq!(flush_count.load(Ordering::Relaxed), 1);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundaryEvent {
    Begin(u64),
    Complete(u64),
}

struct TrackingProcessor {
    events: Arc<Mutex<Vec<BoundaryEvent>>>,
    process_calls: usize,
    fail_process_call: Option<usize>,
    shutdown_count: Arc<AtomicUsize>,
}

impl TrackingProcessor {
    fn new(
        events: Arc<Mutex<Vec<BoundaryEvent>>>,
        shutdown_count: Arc<AtomicUsize>,
        fail_process_call: Option<usize>,
    ) -> Self {
        Self {
            events,
            process_calls: 0,
            fail_process_call,
            shutdown_count,
        }
    }
}

impl Processor for TrackingProcessor {
    fn process(
        &mut self,
        _metadata: SourceMessageMetadata,
        _decoded: ExportMetricsServiceRequest,
    ) -> Result<ProcessResult, ChronoxideError> {
        self.process_calls += 1;
        if self.fail_process_call == Some(self.process_calls) {
            return Err(std::io::Error::other("injected processing failure").into());
        }
        Ok(ProcessResult::Ok)
    }

    fn force_report(&mut self) {}

    fn live_message_tracking_enabled(&self) -> bool {
        true
    }

    fn begin_acquired_message(&mut self, sequence: MessageSequence) -> Result<(), ChronoxideError> {
        self.events
            .lock()
            .unwrap()
            .push(BoundaryEvent::Begin(sequence.get()));
        Ok(())
    }

    fn complete_acquired_message(
        &mut self,
        sequence: MessageSequence,
    ) -> Result<(), ChronoxideError> {
        self.events
            .lock()
            .unwrap()
            .push(BoundaryEvent::Complete(sequence.get()));
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), ChronoxideError> {
        self.shutdown_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn test_ingestion_config() -> IngestionConfig {
    IngestionConfig {
        max_event_age: TimeDelta::seconds(3600),
        max_event_lead: TimeDelta::seconds(3600),
        drop_outdated: false,
        labelset_store: LabelSetStoreKind::FlatInterned,
        labelset_report_interval: Duration::from_secs(60),
        stop_after_messages: None,
        replay_from: None,
        capture_to: None,
        capture_only: false,
        segment_writer: None,
    }
}

fn source_message(offset: i64, payload: Vec<u8>) -> SourceMessage {
    SourceMessage {
        topic: "tracked".to_string(),
        partition: 2,
        offset,
        timestamp_ms: 1_000 + offset,
        captured_at_ms: 10_000 + offset,
        payload,
    }
}

#[test]
fn acquired_message_boundaries_cover_decode_and_processing_errors() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let shutdown_count = Arc::new(AtomicUsize::new(0));
    let flush_count = Arc::new(AtomicUsize::new(0));
    let source = MockSource::new(
        vec![
            source_message(1, create_dummy_payload()),
            source_message(2, vec![0xff, 0xff]),
            source_message(3, create_dummy_payload()),
            source_message(4, create_dummy_payload()),
        ],
        Arc::clone(&flush_count),
    );
    let processor =
        TrackingProcessor::new(Arc::clone(&events), Arc::clone(&shutdown_count), Some(2));
    let mut ingester = Ingester::new(
        source,
        test_ingestion_config(),
        processor,
        opentelemetry::global::meter("tracked-boundaries"),
        CancellationToken::new(),
    )
    .unwrap();

    ingester.start().unwrap();

    assert_eq!(ingester.processor.process_calls, 3);
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            BoundaryEvent::Begin(1),
            BoundaryEvent::Complete(1),
            BoundaryEvent::Begin(2),
            BoundaryEvent::Complete(2),
            BoundaryEvent::Begin(3),
            BoundaryEvent::Complete(3),
            BoundaryEvent::Begin(4),
            BoundaryEvent::Complete(4),
        ]
    );
    assert_eq!(shutdown_count.load(Ordering::Relaxed), 1);
    assert_eq!(flush_count.load(Ordering::Relaxed), 1);
}

#[test]
fn acquired_message_sequence_reaches_max_once_then_stops_before_processing_next() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let shutdown_count = Arc::new(AtomicUsize::new(0));
    let flush_count = Arc::new(AtomicUsize::new(0));
    let source = MockSource::new(
        vec![
            source_message(1, create_dummy_payload()),
            source_message(2, create_dummy_payload()),
        ],
        Arc::clone(&flush_count),
    );
    let processor = TrackingProcessor::new(Arc::clone(&events), Arc::clone(&shutdown_count), None);
    let mut ingester = Ingester::new(
        source,
        test_ingestion_config(),
        processor,
        opentelemetry::global::meter("tracked-overflow"),
        CancellationToken::new(),
    )
    .unwrap()
    .with_initial_message_sequence(MessageSequence::new(u64::MAX))
    .unwrap();

    let error = ingester.start().unwrap_err();

    assert!(matches!(error.kind(), ErrorKind::IoError(_)));
    assert!(error.to_string().contains("sequence exhausted"));
    assert_eq!(ingester.processor.process_calls, 1);
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            BoundaryEvent::Begin(u64::MAX),
            BoundaryEvent::Complete(u64::MAX),
        ]
    );
    assert_eq!(shutdown_count.load(Ordering::Relaxed), 1);
    assert_eq!(flush_count.load(Ordering::Relaxed), 1);
}

#[test]
fn real_otlp_tracker_finalizes_malformed_protobuf_as_a_zero_record_message() {
    let flush_count = Arc::new(AtomicUsize::new(0));
    let source = MockSource::new(
        vec![
            SourceMessage {
                topic: "tracked".to_string(),
                partition: 0,
                offset: 1,
                timestamp_ms: 10_000,
                captured_at_ms: 10_000,
                payload: vec![0xff, 0xff],
            },
            SourceMessage {
                topic: "tracked".to_string(),
                partition: 0,
                offset: 2,
                timestamp_ms: 10_001,
                captured_at_ms: 10_000,
                payload: create_metric_payload(9_500, 1.5),
            },
        ],
        Arc::clone(&flush_count),
    );
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
    let mut ingester = Ingester::new(
        source,
        test_ingestion_config(),
        processor,
        opentelemetry::global::meter("real-tracked-decode-error"),
        CancellationToken::new(),
    )
    .unwrap();

    ingester.start().unwrap();

    let malformed = ingester.processor.pop_completed_message_coverage().unwrap();
    let valid = ingester.processor.pop_completed_message_coverage().unwrap();
    assert_eq!(malformed.message_sequence.get(), 1);
    assert_eq!(malformed.coverage.sample_count(), 0);
    assert_eq!(valid.message_sequence.get(), 2);
    assert_eq!(valid.coverage.sample_count(), 1);
    assert_eq!(valid.completed_prefix.sample_count(), 1);
    assert_eq!(flush_count.load(Ordering::Relaxed), 1);
}
