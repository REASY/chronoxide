use crate::error::ChronoxideError;
use chronoxide_capture::OtlpCaptureReader;
use std::path::PathBuf;
use tracing::info;

pub struct SourceMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Kafka/source timestamp metadata. This is not a trusted replay clock.
    pub timestamp_ms: i64,
    /// Local wall-clock timestamp recorded by this process when the message was captured/accepted.
    pub captured_at_ms: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SourceMessageMetadata {
    pub topic: String,
    pub partition: i32,
    #[allow(dead_code)]
    pub offset: i64,
    /// Kafka/source timestamp metadata. This is not a trusted replay clock.
    pub timestamp_ms: i64,
    /// Trusted local wall-clock timestamp for the source record.
    #[allow(dead_code)]
    pub captured_at_ms: i64,
}

/// Abstraction for a source of OTLP messages.
pub trait MessageSource: Send {
    /// Returns the next message from the source.
    /// Returns Ok(None) when the source is exhausted (e.g. end of file).
    fn next_message(&mut self) -> Result<Option<SourceMessage>, ChronoxideError>;

    /// Called when ingestion is paused (e.g. stop_after_messages reached).
    fn pause(&mut self) -> Result<(), ChronoxideError> {
        Ok(())
    }

    /// Called when ingestion is finished to flush any buffers.
    fn flush(&mut self) -> Result<(), ChronoxideError> {
        Ok(())
    }
}

pub struct FileSource {
    reader: OtlpCaptureReader,
}

impl FileSource {
    pub fn new(path: PathBuf) -> Result<Self, ChronoxideError> {
        info!(
            target: "chronoxide_core::source",
            "Replaying OTLP ExportMetricsServiceRequest messages from {}",
            path.display()
        );
        Ok(Self {
            reader: OtlpCaptureReader::open(path)?,
        })
    }
}

impl MessageSource for FileSource {
    fn next_message(&mut self) -> Result<Option<SourceMessage>, ChronoxideError> {
        match self.reader.next()? {
            Some(msg) => Ok(Some(SourceMessage {
                topic: msg.topic,
                partition: msg.partition,
                offset: msg.offset,
                timestamp_ms: msg.timestamp_ms,
                captured_at_ms: msg.captured_at_ms,
                payload: msg.payload,
            })),
            None => Ok(None),
        }
    }

    fn pause(&mut self) -> Result<(), ChronoxideError> {
        // File source doesn't need explicit pausing as we just stop calling next(),
        // but we can sleep to simulate pause if needed.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronoxide_capture::{CompressionMethod, OtlpCaptureWriter};

    #[test]
    fn file_source_replays_capture_messages() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path();
        let mut writer =
            OtlpCaptureWriter::create(path, "topic", CompressionMethod::Uncompressed).unwrap();
        writer.append(0, 10, 1_000, 9_000, &[1, 2, 3, 4]).unwrap();
        writer.close().unwrap();

        let mut source = FileSource::new(path.to_path_buf()).unwrap();
        let msg = source.next_message().unwrap().unwrap();
        assert_eq!(msg.topic, "topic");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.offset, 10);
        assert_eq!(msg.timestamp_ms, 1_000);
        assert_eq!(msg.captured_at_ms, 9_000);
        assert_eq!(msg.payload, vec![1, 2, 3, 4]);
        assert!(source.next_message().unwrap().is_none());
    }
}
