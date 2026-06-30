use crate::prelude::*;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

const PARTITION_MAGIC: &[u8] = b"CHRONOXIDE_OTLP_CAPTURE_PARTITION_V2\n";
const MANIFEST_FILE_NAME: &str = "manifest.json";
const MANIFEST_VERSION: u32 = 2;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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
    /// Kafka/source timestamp metadata. This is not a trusted replay clock.
    pub timestamp_ms: i64,
    /// Local wall-clock timestamp recorded by this process when the message was captured.
    pub captured_at_ms: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureManifest {
    pub version: u32,
    pub topic: String,
    pub compression: CompressionMethod,
    pub partitions: Vec<CapturePartitionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturePartitionMetadata {
    pub partition: i32,
    pub file_name: String,
    pub message_count: u64,
    pub total_uncompressed_payload_bytes: u64,
    pub total_compressed_payload_bytes: u64,
}

pub struct OtlpCaptureWriter {
    path: PathBuf,
    topic: String,
    compression_method: CompressionMethod,
    writers: Option<BTreeMap<i32, PartitionWriter>>,
    messages_written: u64,
}

struct PartitionWriter {
    partition: i32,
    file_name: String,
    compression_method: CompressionMethod,
    writer: BufWriter<File>,
    message_count: u64,
    total_uncompressed_payload_bytes: u64,
    total_compressed_payload_bytes: u64,
}

impl OtlpCaptureWriter {
    pub fn create(
        path: impl AsRef<Path>,
        topic: impl Into<String>,
        compression_method: CompressionMethod,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let topic = topic.into();

        ensure_capture_dir(&path)?;

        let writer = Self {
            path,
            topic,
            compression_method,
            writers: Some(BTreeMap::new()),
            messages_written: 0,
        };
        Ok(writer)
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
        captured_at_ms: i64,
        payload: &[u8],
    ) -> Result<()> {
        let needs_create = {
            let writers = self.writers.as_ref().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "capture writer is closed")
            })?;
            !writers.contains_key(&partition)
        };

        if needs_create {
            let new_writer = PartitionWriter::create(
                &self.path,
                &self.topic,
                self.compression_method,
                partition,
            )?;
            let writers = self.writers.as_mut().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "capture writer is closed")
            })?;
            writers.insert(partition, new_writer);
        }

        let writers = self.writers.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "capture writer is closed")
        })?;
        let writer = writers.get_mut(&partition).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "partition writer missing")
        })?;

        let sequence = self.messages_written;
        writer.append(sequence, offset, timestamp_ms, captured_at_ms, payload)?;

        self.messages_written += 1;
        if self.messages_written.is_multiple_of(10000) {
            let dt = format_timestamp_ms(timestamp_ms);
            let captured_at = format_timestamp_ms(captured_at_ms);
            info!(
                "{} messages written to {:?}. Last partition: {}, offset: {}, timestamp: {} [{}], captured_at: {} [{}]",
                self.messages_written,
                self.path,
                partition,
                offset,
                dt,
                timestamp_ms,
                captured_at,
                captured_at_ms
            );
        }
        Ok(())
    }

    pub fn append_captured_now(
        &mut self,
        partition: i32,
        offset: i64,
        timestamp_ms: i64,
        payload: &[u8],
    ) -> Result<()> {
        self.append(
            partition,
            offset,
            timestamp_ms,
            current_unix_time_ms(),
            payload,
        )
    }

    pub fn flush(&mut self) -> Result<()> {
        let Some(writers) = self.writers.as_mut() else {
            return Ok(());
        };

        for writer in writers.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        let Some(writers) = self.writers.take() else {
            return Ok(());
        };

        let mut partitions = Vec::with_capacity(writers.len());
        for (_, writer) in writers {
            partitions.push(writer.finish()?);
        }

        self.write_manifest(partitions)?;
        Ok(())
    }

    fn write_manifest(&self, partitions: Vec<CapturePartitionMetadata>) -> Result<()> {
        let manifest = CaptureManifest {
            version: MANIFEST_VERSION,
            topic: self.topic.clone(),
            compression: self.compression_method,
            partitions,
        };

        let manifest_path = self.path.join(MANIFEST_FILE_NAME);
        let file = File::create(&manifest_path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &manifest)?;
        Ok(())
    }
}

impl Drop for OtlpCaptureWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub struct OtlpCaptureReader {
    reader: ReaderKind,
    messages_read: u64,
    manifest: Option<CaptureManifest>,
}

enum ReaderKind {
    Single(PartitionReader),
    Multi(MultiPartitionReader),
}

impl OtlpCaptureReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.is_dir() {
            let manifest = read_manifest(path)?;
            if manifest.version != MANIFEST_VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported capture manifest version: {}", manifest.version),
                )
                .into());
            }
            let mut readers = Vec::with_capacity(manifest.partitions.len());
            for partition in &manifest.partitions {
                let partition_path = path.join(&partition.file_name);
                let reader = PartitionReader::open(&partition_path)?;
                if reader.partition != partition.partition {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "partition id mismatch for {}: manifest={}, file={}",
                            partition.file_name, partition.partition, reader.partition
                        ),
                    )
                    .into());
                }
                if reader.topic != manifest.topic {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "topic mismatch for {}: manifest={}, file={}",
                            partition.file_name, manifest.topic, reader.topic
                        ),
                    )
                    .into());
                }
                if reader.compression_method != manifest.compression {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "compression mismatch for {}: manifest={:?}, file={:?}",
                            partition.file_name, manifest.compression, reader.compression_method
                        ),
                    )
                    .into());
                }
                readers.push(reader);
            }

            let reader = MultiPartitionReader::new(readers)?;
            Ok(Self {
                reader: ReaderKind::Multi(reader),
                messages_read: 0,
                manifest: Some(manifest),
            })
        } else {
            let reader = PartitionReader::open(path)?;
            Ok(Self {
                reader: ReaderKind::Single(reader),
                messages_read: 0,
                manifest: None,
            })
        }
    }

    pub fn open_partition(path: impl AsRef<Path>, partition: i32) -> Result<Self> {
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("capture path {} is not a directory", path.display()),
            )
            .into());
        }

        let manifest = read_manifest(path)?;
        if manifest.version != MANIFEST_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported capture manifest version: {}", manifest.version),
            )
            .into());
        }

        let metadata = manifest
            .partitions
            .iter()
            .find(|entry| entry.partition == partition)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("partition {} not found in manifest", partition),
                )
            })?;

        let partition_path = path.join(&metadata.file_name);
        let reader = PartitionReader::open(&partition_path)?;
        if reader.partition != metadata.partition {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "partition id mismatch for {}: manifest={}, file={}",
                    metadata.file_name, metadata.partition, reader.partition
                ),
            )
            .into());
        }
        if reader.topic != manifest.topic {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "topic mismatch for {}: manifest={}, file={}",
                    metadata.file_name, manifest.topic, reader.topic
                ),
            )
            .into());
        }
        if reader.compression_method != manifest.compression {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "compression mismatch for {}: manifest={:?}, file={:?}",
                    metadata.file_name, manifest.compression, reader.compression_method
                ),
            )
            .into());
        }

        Ok(Self {
            reader: ReaderKind::Single(reader),
            messages_read: 0,
            manifest: Some(manifest),
        })
    }

    #[allow(dead_code)]
    pub fn messages_read(&self) -> u64 {
        self.messages_read
    }

    pub fn manifest(&self) -> Option<&CaptureManifest> {
        self.manifest.as_ref()
    }

    pub fn next(&mut self) -> Result<Option<RecordedOtlpMessage>> {
        let result = match &mut self.reader {
            ReaderKind::Single(reader) => reader.next()?,
            ReaderKind::Multi(reader) => reader.next()?,
        };

        if result.is_some() {
            self.messages_read += 1;
        }
        Ok(result)
    }
}

struct PartitionReader {
    topic: String,
    partition: i32,
    compression_method: CompressionMethod,
    reader: BufReader<File>,
}

impl PartitionReader {
    fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = vec![0u8; PARTITION_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if magic.as_slice() != PARTITION_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "invalid partition magic for {}: expected {:?}, got {:?}",
                    path.display(),
                    PARTITION_MAGIC,
                    magic
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

        let partition = read_i32(&mut reader)?;

        Ok(Self {
            topic,
            partition,
            compression_method: method,
            reader,
        })
    }

    fn next(&mut self) -> Result<Option<RecordedOtlpMessage>> {
        match self.next_with_sequence()? {
            Some((_, msg)) => Ok(Some(msg)),
            None => Ok(None),
        }
    }

    fn next_with_sequence(&mut self) -> Result<Option<(u64, RecordedOtlpMessage)>> {
        let Some(sequence) = read_u64_or_eof(&mut self.reader)? else {
            return Ok(None);
        };

        let offset = read_i64(&mut self.reader)?;
        let timestamp_ms = read_i64(&mut self.reader)?;
        let captured_at_ms = read_i64(&mut self.reader)?;

        let payload = match self.compression_method {
            CompressionMethod::Uncompressed => {
                let payload_len = read_u32(&mut self.reader)? as usize;
                let mut payload = vec![0u8; payload_len];
                self.reader.read_exact(&mut payload)?;
                payload
            }
            CompressionMethod::Zstd => {
                let uncompressed_len = read_u32(&mut self.reader)? as usize;
                let compressed_len = read_u32(&mut self.reader)? as usize;
                let mut compressed = vec![0u8; compressed_len];
                self.reader.read_exact(&mut compressed)?;
                zstd::bulk::decompress(&compressed, uncompressed_len)?
            }
        };

        Ok(Some((
            sequence,
            RecordedOtlpMessage {
                topic: self.topic.clone(),
                partition: self.partition,
                offset,
                timestamp_ms,
                captured_at_ms,
                payload,
            },
        )))
    }
}

struct MultiPartitionReader {
    readers: Vec<PartitionReader>,
    heap: BinaryHeap<Reverse<HeapItem>>,
}

impl MultiPartitionReader {
    fn new(mut readers: Vec<PartitionReader>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some((sequence, msg)) = reader.next_with_sequence()? {
                heap.push(Reverse(HeapItem {
                    sequence,
                    reader_index: index,
                    msg,
                }));
            }
        }

        Ok(Self { readers, heap })
    }

    fn next(&mut self) -> Result<Option<RecordedOtlpMessage>> {
        let Some(Reverse(item)) = self.heap.pop() else {
            return Ok(None);
        };

        let msg = item.msg;
        let reader_index = item.reader_index;

        if let Some((sequence, next_msg)) = self.readers[reader_index].next_with_sequence()? {
            self.heap.push(Reverse(HeapItem {
                sequence,
                reader_index,
                msg: next_msg,
            }));
        }

        Ok(Some(msg))
    }
}

#[derive(Debug)]
struct HeapItem {
    sequence: u64,
    reader_index: usize,
    msg: RecordedOtlpMessage,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence && self.reader_index == other.reader_index
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sequence
            .cmp(&other.sequence)
            .then_with(|| self.reader_index.cmp(&other.reader_index))
    }
}

impl PartitionWriter {
    fn create(
        base_dir: &Path,
        topic: &str,
        compression_method: CompressionMethod,
        partition: i32,
    ) -> Result<Self> {
        let file_name = partition_file_name(partition);
        let path = base_dir.join(&file_name);
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        writer.write_all(PARTITION_MAGIC)?;
        writer.write_all(&[compression_method as u8])?;

        let topic_len = checked_u32_len(topic.len(), "topic length")?;
        write_u32(&mut writer, topic_len)?;
        writer.write_all(topic.as_bytes())?;

        write_i32(&mut writer, partition)?;

        Ok(Self {
            partition,
            file_name,
            compression_method,
            writer,
            message_count: 0,
            total_uncompressed_payload_bytes: 0,
            total_compressed_payload_bytes: 0,
        })
    }

    fn append(
        &mut self,
        sequence: u64,
        offset: i64,
        timestamp_ms: i64,
        captured_at_ms: i64,
        payload: &[u8],
    ) -> Result<()> {
        write_u64(&mut self.writer, sequence)?;
        write_i64(&mut self.writer, offset)?;
        write_i64(&mut self.writer, timestamp_ms)?;
        write_i64(&mut self.writer, captured_at_ms)?;

        let uncompressed_len = checked_u32_len(payload.len(), "payload length")?;
        match self.compression_method {
            CompressionMethod::Uncompressed => {
                write_u32(&mut self.writer, uncompressed_len)?;
                self.writer.write_all(payload)?;
                self.total_compressed_payload_bytes += uncompressed_len as u64;
            }
            CompressionMethod::Zstd => {
                let compressed = zstd::bulk::compress(payload, 0)?;
                let compressed_len =
                    checked_u32_len(compressed.len(), "compressed payload length")?;
                write_u32(&mut self.writer, uncompressed_len)?;
                write_u32(&mut self.writer, compressed_len)?;
                self.writer.write_all(&compressed)?;
                self.total_compressed_payload_bytes += compressed_len as u64;
            }
        }

        self.message_count += 1;
        self.total_uncompressed_payload_bytes += uncompressed_len as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn finish(mut self) -> Result<CapturePartitionMetadata> {
        self.writer.flush()?;
        Ok(CapturePartitionMetadata {
            partition: self.partition,
            file_name: self.file_name,
            message_count: self.message_count,
            total_uncompressed_payload_bytes: self.total_uncompressed_payload_bytes,
            total_compressed_payload_bytes: self.total_compressed_payload_bytes,
        })
    }
}

pub fn read_manifest(path: impl AsRef<Path>) -> Result<CaptureManifest> {
    let path = path.as_ref().join(MANIFEST_FILE_NAME);
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    Ok(serde_json::from_reader(reader)?)
}

fn ensure_capture_dir(path: &Path) -> Result<()> {
    if path.exists() {
        if path.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("capture path {} is a file", path.display()),
            )
            .into());
        }
        let mut entries = fs::read_dir(path)?;
        if entries.next().is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("capture directory {} is not empty", path.display()),
            )
            .into());
        }
    } else {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

fn partition_file_name(partition: i32) -> String {
    format!("partition-{}.capture", partition)
}

fn checked_u32_len(len: usize, label: &str) -> Result<u32> {
    if len > u32::MAX as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{label} exceeds u32::MAX: {len}"),
        )
        .into());
    }
    Ok(len as u32)
}

fn current_unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn format_timestamp_ms(timestamp_ms: i64) -> String {
    DateTime::from_timestamp_millis(timestamp_ms)
        .map(|dt| dt.to_string())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn read_u64_or_eof(reader: &mut impl Read) -> Result<Option<u64>> {
    let mut buf = [0u8; 8];
    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(u64::from_le_bytes(buf))),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn read_i32(reader: &mut impl Read) -> Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(i32::from_le_bytes(buf))
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

fn write_u64(writer: &mut impl Write, value: u64) -> Result<()> {
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
        std::env::temp_dir().join(format!("chronoxide_capture_test_{}", nanos))
    }

    #[test]
    fn capture_roundtrip_zstd_close() {
        let path = tmp_capture_path();

        let mut writer =
            OtlpCaptureWriter::create(&path, "test-topic", CompressionMethod::Zstd).unwrap();
        writer
            .append(0, 1, 123, 10_000, b"hello")
            .expect("append should work");
        writer
            .append(0, 2, 124, 10_001, b"world")
            .expect("append should work");
        writer.close().expect("close should work");

        let mut reader = OtlpCaptureReader::open(&path).unwrap();
        let m1 = reader.next().unwrap().unwrap();
        assert_eq!(m1.topic, "test-topic");
        assert_eq!(m1.partition, 0);
        assert_eq!(m1.offset, 1);
        assert_eq!(m1.timestamp_ms, 123);
        assert_eq!(m1.captured_at_ms, 10_000);
        assert_eq!(m1.payload, b"hello");

        let m2 = reader.next().unwrap().unwrap();
        assert_eq!(m2.offset, 2);
        assert_eq!(m2.timestamp_ms, 124);
        assert_eq!(m2.captured_at_ms, 10_001);
        assert_eq!(m2.payload, b"world");

        assert!(reader.next().unwrap().is_none());

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn capture_close_is_idempotent() {
        let path = tmp_capture_path();

        let mut writer =
            OtlpCaptureWriter::create(&path, "test-topic", CompressionMethod::Uncompressed)
                .unwrap();
        writer.append(0, 1, 123, 1_000, b"hello").unwrap();
        writer.close().unwrap();
        writer.close().unwrap();

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn capture_manifest_tracks_partition_metadata() {
        let path = tmp_capture_path();

        let mut writer =
            OtlpCaptureWriter::create(&path, "topic", CompressionMethod::Uncompressed).unwrap();
        writer.append(0, 1, 100, 1_000, b"hello").unwrap();
        writer.append(1, 2, 200, 2_000, b"world!!").unwrap();
        writer.append(0, 3, 300, 3_000, b"abc").unwrap();
        writer.close().unwrap();

        let manifest = read_manifest(&path).unwrap();
        assert_eq!(manifest.topic, "topic");
        assert_eq!(manifest.compression, CompressionMethod::Uncompressed);
        assert_eq!(manifest.partitions.len(), 2);

        let p0 = manifest
            .partitions
            .iter()
            .find(|p| p.partition == 0)
            .unwrap();
        assert_eq!(p0.message_count, 2);
        assert_eq!(p0.total_uncompressed_payload_bytes, 8);
        assert_eq!(p0.total_compressed_payload_bytes, 8);

        let p1 = manifest
            .partitions
            .iter()
            .find(|p| p.partition == 1)
            .unwrap();
        assert_eq!(p1.message_count, 1);
        assert_eq!(p1.total_uncompressed_payload_bytes, 7);
        assert_eq!(p1.total_compressed_payload_bytes, 7);

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn capture_open_partition_reads_single_partition() {
        let path = tmp_capture_path();

        let mut writer =
            OtlpCaptureWriter::create(&path, "topic", CompressionMethod::Uncompressed).unwrap();
        writer.append(0, 1, 100, 1_000, b"p0-1").unwrap();
        writer.append(1, 2, 200, 2_000, b"p1-1").unwrap();
        writer.append(1, 3, 300, 3_000, b"p1-2").unwrap();
        writer.append(0, 4, 400, 4_000, b"p0-2").unwrap();
        writer.close().unwrap();

        let mut reader = OtlpCaptureReader::open_partition(&path, 1).unwrap();
        let r1 = reader.next().unwrap().unwrap();
        assert_eq!(r1.partition, 1);
        assert_eq!(r1.payload, b"p1-1");
        let r2 = reader.next().unwrap().unwrap();
        assert_eq!(r2.partition, 1);
        assert_eq!(r2.payload, b"p1-2");
        assert!(reader.next().unwrap().is_none());

        let _ = std::fs::remove_dir_all(&path);
    }
}
