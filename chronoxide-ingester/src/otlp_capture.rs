use chrono::DateTime;
use chronoxide_core::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tracing::info;

const MAGIC: &[u8] = b"CHRONOXIDE_OTLP_CAPTURE_V2\n";

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMethod {
    Uncompressed = 0,
    Zstd = 1,
}

impl TryFrom<u8> for CompressionMethod {
    type Error = std::io::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Uncompressed),
            1 => Ok(Self::Zstd),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown compression method: {}", other),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecordedOtlpMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
    pub payload: Vec<u8>,
}

pub struct OtlpCaptureWriter {
    path: PathBuf,
    writer: Option<CaptureWriter>,
    messages_written: u64,
}

enum CaptureWriter {
    Uncompressed(BufWriter<File>),
    Zstd(zstd::stream::write::Encoder<'static, BufWriter<File>>),
}

impl OtlpCaptureWriter {
    pub fn create(
        path: impl AsRef<Path>,
        topic: impl Into<String>,
        compression_method: CompressionMethod,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let topic = topic.into();
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        // Write header
        writer.write_all(MAGIC)?;

        writer.write_all(&[compression_method as u8])?;

        write_u32(&mut writer, topic.len() as u32)?;
        writer.write_all(topic.as_bytes())?;

        let writer = match compression_method {
            CompressionMethod::Zstd => {
                // Level 0 selects default (usually 3)
                let mut encoder = zstd::stream::write::Encoder::new(writer, 0)?;
                encoder.include_checksum(true)?;
                CaptureWriter::Zstd(encoder)
            }
            CompressionMethod::Uncompressed => CaptureWriter::Uncompressed(writer),
        };

        Ok(Self {
            path,
            writer: Some(writer),
            messages_written: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(dead_code)]
    pub fn messages_written(&self) -> u64 {
        self.messages_written
    }

    pub fn append(
        &mut self,
        partition: i32,
        offset: i64,
        timestamp_ms: i64,
        payload: &[u8],
    ) -> Result<()> {
        let Some(writer) = self.writer.as_mut() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "capture writer is closed",
            )
            .into());
        };

        match writer {
            CaptureWriter::Uncompressed(writer) => {
                write_i32(writer, partition)?;
                write_i64(writer, offset)?;
                write_i64(writer, timestamp_ms)?;
                write_u32(writer, payload.len().min(u32::MAX as usize) as u32)?;
                writer.write_all(payload)?;
            }
            CaptureWriter::Zstd(writer) => {
                write_i32(writer, partition)?;
                write_i64(writer, offset)?;
                write_i64(writer, timestamp_ms)?;
                write_u32(writer, payload.len().min(u32::MAX as usize) as u32)?;
                writer.write_all(payload)?;
            }
        }

        self.messages_written += 1;
        if self.messages_written.is_multiple_of(10000) {
            let dt = DateTime::from_timestamp_millis(timestamp_ms)
                .expect("Failed to convert timestamp to DateTime");
            info!(
                "{} messages written to {:?}. Last partition: {}, offset: {}, timestamp: {} [{}]",
                self.messages_written, self.path, partition, offset, dt, timestamp_ms
            );
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let Some(writer) = self.writer.as_mut() else {
            return Ok(());
        };

        match writer {
            CaptureWriter::Uncompressed(writer) => writer.flush()?,
            CaptureWriter::Zstd(writer) => writer.flush()?,
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };

        match writer {
            CaptureWriter::Uncompressed(mut writer) => {
                writer.flush()?;
            }
            CaptureWriter::Zstd(writer) => {
                let mut writer = writer.finish()?;
                writer.flush()?;
            }
        }
        Ok(())
    }
}

impl Drop for OtlpCaptureWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub struct OtlpCaptureReader {
    topic: String,
    reader: Box<dyn Read + Send>,
    messages_read: u64,
}

impl OtlpCaptureReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        let mut magic = vec![0u8; MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if magic.as_slice() != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "invalid capture file magic: expected {:?}, got {:?}",
                    MAGIC, magic
                ),
            )
            .into());
        }

        let mut flag = [0u8; 1];
        reader.read_exact(&mut flag)?;
        let method = CompressionMethod::try_from(flag[0])?;

        let topic_len = read_u32(&mut reader)? as usize;
        let mut topic_bytes = vec![0u8; topic_len];
        reader.read_exact(&mut topic_bytes)?;
        let topic = String::from_utf8(topic_bytes)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;

        let reader: Box<dyn Read + Send> = match method {
            CompressionMethod::Zstd => Box::new(zstd::stream::read::Decoder::new(reader)?),
            CompressionMethod::Uncompressed => Box::new(reader),
        };

        Ok(Self {
            topic,
            reader,
            messages_read: 0,
        })
    }

    #[allow(dead_code)]
    pub fn messages_read(&self) -> u64 {
        self.messages_read
    }

    pub fn next(&mut self) -> Result<Option<RecordedOtlpMessage>> {
        let Some(partition) = read_i32_or_eof(&mut self.reader)? else {
            return Ok(None);
        };

        let offset = read_i64(&mut self.reader)?;
        let timestamp_ms = read_i64(&mut self.reader)?;
        let payload_len = read_u32(&mut self.reader)? as usize;
        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload)?;

        self.messages_read += 1;
        Ok(Some(RecordedOtlpMessage {
            topic: self.topic.clone(),
            partition,
            offset,
            timestamp_ms,
            payload,
        }))
    }
}

fn read_i32_or_eof(reader: &mut impl Read) -> Result<Option<i32>> {
    let mut buf = [0u8; 4];
    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(i32::from_le_bytes(buf))),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn read_i64(reader: &mut impl Read) -> Result<i64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn write_i32(writer: &mut impl Write, value: i32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_i64(writer: &mut impl Write, value: i64) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u32(writer: &mut impl Write, value: u32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_capture_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("chronoxide_capture_test_{}.bin", nanos))
    }

    #[test]
    fn capture_roundtrip_zstd_close() {
        let path = tmp_capture_path();

        let mut writer =
            OtlpCaptureWriter::create(&path, "test-topic", CompressionMethod::Zstd).unwrap();
        writer
            .append(0, 1, 123, b"hello")
            .expect("append should work");
        writer
            .append(0, 2, 124, b"world")
            .expect("append should work");
        writer.close().expect("close should work");

        let mut reader = OtlpCaptureReader::open(&path).unwrap();
        let m1 = reader.next().unwrap().unwrap();
        assert_eq!(m1.topic, "test-topic");
        assert_eq!(m1.partition, 0);
        assert_eq!(m1.offset, 1);
        assert_eq!(m1.timestamp_ms, 123);
        assert_eq!(m1.payload, b"hello");

        let m2 = reader.next().unwrap().unwrap();
        assert_eq!(m2.offset, 2);
        assert_eq!(m2.timestamp_ms, 124);
        assert_eq!(m2.payload, b"world");

        assert!(reader.next().unwrap().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn capture_close_is_idempotent() {
        let path = tmp_capture_path();

        let mut writer =
            OtlpCaptureWriter::create(&path, "test-topic", CompressionMethod::Uncompressed)
                .unwrap();
        writer.append(0, 1, 123, b"hello").unwrap();
        writer.close().unwrap();
        writer.close().unwrap();

        let _ = std::fs::remove_file(&path);
    }
}
