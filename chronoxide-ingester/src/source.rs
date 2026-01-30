use crate::ingester::KafkaConsumerConfig;
use chronoxide_core::error::{ChronoxideError, ErrorKind, should_log};
use chronoxide_core::otlp_capture::OtlpCaptureWriter;
pub use chronoxide_core::source::{
    FileSource, MessageSource, SourceMessage, SourceMessageMetadata,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::Metadata;
use rdkafka::{ClientConfig, Message, Timestamp, TopicPartitionList};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{Level, info, warn};

fn build_consumer(cfg: &KafkaConsumerConfig) -> Result<BaseConsumer, ChronoxideError> {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("client.id", &cfg.client_id)
        .set("session.timeout.ms", cfg.session_timeout_ms.to_string())
        .set(
            "enable.auto.commit",
            if cfg.enable_auto_commit {
                "true"
            } else {
                "false"
            },
        )
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", &cfg.auto_offset_reset)
        .set("fetch.min.bytes", cfg.fetch_min_bytes.to_string())
        .set("fetch.wait.max.ms", cfg.fetch_wait_max_ms.to_string())
        .set(
            "max.in.flight.requests.per.connection",
            cfg.max_inflight.to_string(),
        );

    if let Some(proto) = &cfg.security_protocol {
        client.set("security.protocol", proto);
    }
    if let Some(mech) = &cfg.sasl_mechanism {
        client.set("sasl.mechanism", mech);
    }
    if let Some(user) = &cfg.sasl_username {
        client.set("sasl.username", user);
    }
    if let Some(pass) = &cfg.sasl_password {
        client.set("sasl.password", pass);
    }
    if let Some(ca) = &cfg.ssl_ca_location {
        client.set("ssl.ca.location", ca);
    }

    let instance = client.create()?;
    Ok(instance)
}

pub struct KafkaSource {
    consumer: BaseConsumer,
    ct: CancellationToken,
}

impl KafkaSource {
    pub fn new(cfg: KafkaConsumerConfig, ct: CancellationToken) -> Result<Self, ChronoxideError> {
        let consumer = build_consumer(&cfg)?;

        info!("Starting Kafka consumer...");
        let md: Metadata =
            consumer.fetch_metadata(Some(cfg.topic.as_str()), Duration::from_secs(10))?;
        let topic_md = md.topics().first().ok_or_else(|| {
            ChronoxideError::new(ErrorKind::ChannelError(format!(
                "topic metadata not found for '{}'",
                cfg.topic
            )))
        })?;

        let mut partition_ids = topic_md
            .partitions()
            .iter()
            .map(|x| x.id())
            .collect::<Vec<_>>();
        partition_ids.sort();
        info!(
            "Topic {} has {} partitions: {:?}",
            cfg.topic,
            partition_ids.len(),
            partition_ids
        );

        let assigned_partitions = cfg.assigned_partitions.as_deref().unwrap_or(&[]);
        if !assigned_partitions.is_empty() {
            let mut topic_partition = TopicPartitionList::new();
            for partition_id in assigned_partitions {
                topic_partition.add_partition(cfg.topic.as_str(), *partition_id);
            }
            consumer.assign(&topic_partition)?;
            info!("Assigned to topic {:?}", topic_partition);
        } else {
            consumer.subscribe(&[cfg.topic.as_str()])?;
            info!("Subscribed to topic {}", cfg.topic);
        }

        Ok(Self { consumer, ct })
    }
}

impl MessageSource for KafkaSource {
    fn next_message(&mut self) -> Result<Option<SourceMessage>, ChronoxideError> {
        loop {
            if self.ct.is_cancelled() {
                return Err(ChronoxideError::new(ErrorKind::ChannelError(
                    "cancelled".to_string(),
                )));
            }

            match self.consumer.poll(Duration::from_millis(5)) {
                Some(maybe_msg) => match maybe_msg {
                    Ok(msg) => {
                        let topic = msg.topic().to_string();
                        let partition = msg.partition();
                        let offset = msg.offset();
                        let timestamp_ms = match msg.timestamp() {
                            Timestamp::NotAvailable => -1,
                            Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) => ms,
                        };

                        let Some(payload) = msg.payload() else {
                            if should_log(Level::WARN, "No payload, ignoring...", Instant::now()) {
                                warn!("No payload, ignoring...");
                            }
                            continue;
                        };

                        return Ok(Some(SourceMessage {
                            topic,
                            partition,
                            offset,
                            timestamp_ms,
                            payload: payload.to_vec(),
                        }));
                    }
                    Err(err) => {
                        let msg = format!("KafkaError: {}", err);
                        if should_log(Level::WARN, &msg, Instant::now()) {
                            warn!("Error processing message: {}", err);
                        }
                        // Continue loop on error
                    }
                },
                None => {
                    // Timeout, keep polling
                    continue;
                }
            }
        }
    }

    fn pause(&mut self) -> Result<(), ChronoxideError> {
        if let Ok(assignment) = self.consumer.assignment()
            && let Err(err) = self.consumer.pause(&assignment)
        {
            warn!("Failed to pause Kafka consumer: {}", err);
        }
        Ok(())
    }
}

pub struct CapturingSource<S> {
    inner: S,
    writer: OtlpCaptureWriter,
}

impl<S: MessageSource> CapturingSource<S> {
    pub fn new(inner: S, writer: OtlpCaptureWriter) -> Self {
        let resolved_path = writer
            .path()
            .canonicalize()
            .or_else(|_| std::env::current_dir().map(|cwd| cwd.join(writer.path())))
            .unwrap_or_else(|_| writer.path().to_path_buf());
        info!(
            "Capturing OTLP ExportMetricsServiceRequest messages to {}",
            resolved_path.display()
        );
        Self { inner, writer }
    }
}

impl<S: MessageSource> MessageSource for CapturingSource<S> {
    fn next_message(&mut self) -> Result<Option<SourceMessage>, ChronoxideError> {
        let msg = self.inner.next_message()?;
        if let Some(msg) = &msg {
            self.writer
                .append(msg.partition, msg.offset, msg.timestamp_ms, &msg.payload)?;
        }
        Ok(msg)
    }

    fn pause(&mut self) -> Result<(), ChronoxideError> {
        self.writer.flush()?;
        self.inner.pause()
    }

    fn flush(&mut self) -> Result<(), ChronoxideError> {
        self.writer.close()?;
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronoxide_core::otlp_capture::{CompressionMethod, OtlpCaptureReader};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn temp_path(stem: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!(
            "chronoxide_test_{}_{}_{}",
            stem,
            std::process::id(),
            nanos
        ));
        path
    }

    struct VecSource {
        messages: Vec<SourceMessage>,
        pause_calls: AtomicUsize,
        flush_calls: AtomicUsize,
    }

    impl VecSource {
        fn new(messages: Vec<SourceMessage>) -> Self {
            Self {
                messages,
                pause_calls: AtomicUsize::new(0),
                flush_calls: AtomicUsize::new(0),
            }
        }
    }

    impl MessageSource for VecSource {
        fn next_message(&mut self) -> Result<Option<SourceMessage>, ChronoxideError> {
            if self.messages.is_empty() {
                return Ok(None);
            }
            Ok(Some(self.messages.remove(0)))
        }

        fn pause(&mut self) -> Result<(), ChronoxideError> {
            self.pause_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn flush(&mut self) -> Result<(), ChronoxideError> {
            self.flush_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn capturing_source_writes_messages_and_propagates_pause_and_flush() {
        let path = temp_path("capturing_source");
        let inner = VecSource::new(vec![
            SourceMessage {
                topic: "t".to_string(),
                partition: 1,
                offset: 1,
                timestamp_ms: 1_000,
                payload: vec![9],
            },
            SourceMessage {
                topic: "t".to_string(),
                partition: 2,
                offset: 2,
                timestamp_ms: 2_000,
                payload: vec![8, 7],
            },
        ]);

        let writer =
            OtlpCaptureWriter::create(&path, "t", CompressionMethod::Uncompressed).unwrap();
        let mut source = CapturingSource::new(inner, writer);

        let m1 = source.next_message().unwrap().unwrap();
        assert_eq!(m1.partition, 1);
        let m2 = source.next_message().unwrap().unwrap();
        assert_eq!(m2.partition, 2);
        assert!(source.next_message().unwrap().is_none());

        source.pause().unwrap();
        source.flush().unwrap();

        let mut reader = OtlpCaptureReader::open(&path).unwrap();
        let r1 = reader.next().unwrap().unwrap();
        assert_eq!(r1.partition, 1);
        assert_eq!(r1.payload, vec![9]);
        let r2 = reader.next().unwrap().unwrap();
        assert_eq!(r2.partition, 2);
        assert_eq!(r2.payload, vec![8, 7]);
        assert!(reader.next().unwrap().is_none());

        // Ensure pause/flush were forwarded to the inner source.
        let inner = source.inner;
        assert_eq!(inner.pause_calls.load(Ordering::Relaxed), 1);
        assert_eq!(inner.flush_calls.load(Ordering::Relaxed), 1);

        let _ = std::fs::remove_dir_all(path);
    }
}
