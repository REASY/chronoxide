use crate::app_config::LabelSetStoreKind;
use crate::error::{ChronoxideError, ErrorKind, should_log};
use crate::processor::{ProcessResult, Processor};
use crate::source::{MessageSource, SourceMessageMetadata};
use chrono::TimeDelta;
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
                captured_at_ms: source_msg.captured_at_ms,
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
                info!("Reached stop_after_messages={}", stop_after_messages);
                if let Err(err) = self.processor.shutdown() {
                    if exit_error.is_none() {
                        exit_error = Some(err);
                    } else if should_log(Level::WARN, err.kind().as_ref(), Instant::now()) {
                        warn!("Error shutting down processor: {}", err);
                    }
                }
                processor_shutdown = true;
                self.source.flush()?;

                self.source.pause()?;
                stop_after_reached = true;
            }
        }

        if !processor_shutdown && let Err(err) = self.processor.shutdown() {
            if exit_error.is_none() {
                exit_error = Some(err);
            } else if should_log(Level::WARN, err.kind().as_ref(), Instant::now()) {
                warn!("Error shutting down processor: {}", err);
            }
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
mod tests;
