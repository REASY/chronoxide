use crate::app_config::LabelSetStoreKind;
use crate::processor::{ProcessResult, Processor};
use crate::source::{MessageSource, SourceMessageMetadata};
use chrono::TimeDelta;
use chronoxide_core::error::{ChronoxideError, ErrorKind, should_log};
use chronoxide_core::storage::segment::SegmentWriterConfig as CoreSegmentWriterConfig;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{Level, error, info, warn};

#[derive(Clone, Debug)]
pub struct KafkaConsumerConfig {
    pub brokers: String,
    pub group_id: String,
    pub topic: String,
    pub client_id: String,
    pub assigned_partitions: Option<Vec<i32>>,
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub session_timeout_ms: i32,
    pub enable_auto_commit: bool,
    pub auto_offset_reset: String,
    pub max_inflight: i32,
    pub fetch_min_bytes: i32,
    pub fetch_wait_max_ms: i32,
}

#[derive(Debug, Clone)]
pub struct IngestionConfig {
    #[allow(dead_code)]
    pub max_event_age: TimeDelta,

    #[allow(dead_code)]
    pub max_event_lead: TimeDelta,

    #[allow(dead_code)]
    pub drop_outdated: bool,

    pub labelset_store: LabelSetStoreKind,
    pub labelset_report_interval: Duration,
    pub stop_after_messages: Option<u64>,

    #[allow(dead_code)]
    pub replay_from: Option<PathBuf>,

    #[allow(dead_code)]
    pub capture_to: Option<PathBuf>,

    pub capture_only: bool,

    #[allow(dead_code)]
    pub segment_writer: Option<CoreSegmentWriterConfig>,
}

pub struct Ingester<S, P> {
    source: S,
    ingestion_config: IngestionConfig,
    meter: Meter,
    ct: CancellationToken,
    processor: P,
}

impl<S: MessageSource, P: Processor> Ingester<S, P> {
    pub fn new(
        source: S,
        ingestion_config: IngestionConfig,
        processor: P,
        meter: Meter,
        ct: CancellationToken,
    ) -> Result<Self, ChronoxideError> {
        if ingestion_config.capture_only {
            info!("capture_only=true (skipping decode/processing)");
        }
        Ok(Self {
            source,
            ingestion_config,
            meter,
            ct,
            processor,
        })
    }

    pub fn start(&mut self) -> Result<(), ChronoxideError> {
        let counter = self
            .meter
            .u64_counter("chronoxide.kafka-consumer.processed")
            .build();

        let dropped_counter: Counter<u64> = self
            .meter
            .u64_counter("chronoxide.kafka-consumer.dropped")
            .build();

        let mut messages_read = 0u64;
        let mut stop_after_reached = false;
        let mut processor_shutdown = false;
        let mut exit_error: Option<ChronoxideError> = None;

        loop {
            if self.ct.is_cancelled() {
                info!("Cancelled, exiting...");
                break;
            }

            if stop_after_reached {
                std::thread::sleep(Duration::from_millis(1000));
                break;
            }

            let source_msg = match self.source.next_message() {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    info!("Source exhausted (EOF)");
                    break;
                }
                Err(err) => {
                    if self.ct.is_cancelled() {
                        info!("Cancelled, exiting...");
                        break;
                    }
                    if should_log(Level::WARN, err.kind().as_ref(), Instant::now()) {
                        warn!("Error reading message: {}", err);
                    }
                    continue;
                }
            };

            messages_read += 1;

            let metadata = SourceMessageMetadata {
                topic: source_msg.topic.clone(),
                partition: source_msg.partition,
                offset: source_msg.offset,
                timestamp_ms: source_msg.timestamp_ms,
            };

            let process_result = if self.ingestion_config.capture_only {
                Ok(ProcessResult::CapturedOnly)
            } else {
                match ExportMetricsServiceRequest::decode(source_msg.payload.as_slice()) {
                    Ok(decoded) => self.processor.process(metadata, decoded),
                    Err(err) => Err(ChronoxideError::new(ErrorKind::ProtobufDecodeError(err))),
                }
            };

            match process_result {
                Ok(process_result) => {
                    if let ProcessResult::SinkChannelClosed(sink_name) = &process_result {
                        error!(
                            "Sink {} channel closed; cancelling ingestion loop",
                            sink_name
                        );
                        self.ct.cancel();
                        exit_error = Some(ChronoxideError::new(ErrorKind::ChannelError(format!(
                            "sink {} channel closed",
                            sink_name
                        ))));
                        break;
                    }

                    if matches!(process_result, ProcessResult::DroppedOutdated) {
                        dropped_counter.add(
                            1,
                            &[
                                KeyValue::new("topic", source_msg.topic.clone()),
                                KeyValue::new("partition", format!("{}", source_msg.partition)),
                            ],
                        );
                    }

                    counter.add(
                        1,
                        &[
                            KeyValue::new(
                                "is_success",
                                matches!(process_result, ProcessResult::Ok)
                                    || matches!(process_result, ProcessResult::CapturedOnly),
                            ),
                            KeyValue::new("process_result", process_result.to_string()),
                            KeyValue::new("topic", source_msg.topic),
                            KeyValue::new("partition", format!("{}", source_msg.partition)),
                        ],
                    )
                }
                Err(err) => {
                    if should_log(Level::WARN, err.kind().as_ref(), Instant::now()) {
                        warn!("Error processing message: {}", err);
                    }
                    counter.add(
                        1,
                        &[
                            KeyValue::new("is_success", "false"),
                            KeyValue::new("topic", source_msg.topic),
                            KeyValue::new("partition", format!("{}", source_msg.partition)),
                        ],
                    )
                }
            }

            if let Some(stop_after_messages) = self.ingestion_config.stop_after_messages
                && messages_read >= stop_after_messages
            {
                info!(
                    "Reached stop_after_messages={}, pausing ingestion (Ctrl+C to exit)",
                    stop_after_messages
                );
                self.processor.shutdown();
                processor_shutdown = true;
                self.source.flush()?;

                self.source.pause()?;
                stop_after_reached = true;
            }
        }

        if !processor_shutdown {
            self.processor.shutdown();
        }

        if let Err(err) = self.source.flush() {
            if exit_error.is_none() {
                exit_error = Some(err);
            } else if should_log(Level::WARN, err.kind().as_ref(), Instant::now()) {
                warn!("Error flushing source during shutdown: {}", err);
            }
        }

        match exit_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SourceMessage;
    use std::collections::VecDeque;
    use std::sync::Arc;
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

        fn shutdown(&mut self) {
            self.shutdown_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Helper to create a dummy valid OTLP message payload
    fn create_dummy_payload() -> Vec<u8> {
        let req = ExportMetricsServiceRequest::default();
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
                payload: create_dummy_payload(),
            },
            SourceMessage {
                topic: "test".to_string(),
                partition: 0,
                offset: 2,
                timestamp_ms: 2000,
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

        fn shutdown(&mut self) {
            self.shutdown_count.fetch_add(1, Ordering::Relaxed);
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
}
