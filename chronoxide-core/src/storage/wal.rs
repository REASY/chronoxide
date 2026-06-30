use std::fs::{self, File, OpenOptions};
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32c::{crc32c, crc32c_append};

pub const WAL_RECORD_MAGIC: u32 = u32::from_le_bytes(*b"CWAL");
pub const WAL_RECORD_VERSION: u16 = 1;
pub const WAL_RECORD_HEADER_LEN: usize = 16;
pub const WAL_RECORD_TRAILER_LEN: usize = 4;
pub const CHECKPOINT_META_FILE_NAME: &str = "checkpoint.meta";

const CHECKPOINT_PAYLOAD_MAGIC: u32 = u32::from_le_bytes(*b"WCHK");
const CHECKPOINT_PAYLOAD_VERSION: u16 = 1;
const CHECKPOINT_META_MAGIC: u32 = u32::from_le_bytes(*b"CMET");
const CHECKPOINT_META_VERSION: u16 = 1;
const CHECKPOINT_META_TEMP_FILE_NAME: &str = "checkpoint.meta.tmp";
const CHECKPOINT_META_HEADER_LEN: usize = 16;
const CHECKPOINT_META_TRAILER_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WalRecordType {
    OtlpBatch = 1,
    Checkpoint = 2,
    SegmentSealed = 3,
}

impl WalRecordType {
    fn from_u16(value: u16) -> io::Result<Self> {
        match value {
            1 => Ok(Self::OtlpBatch),
            2 => Ok(Self::Checkpoint),
            3 => Ok(Self::SegmentSealed),
            _ => Err(invalid_data("unknown WAL record type")),
        }
    }

    fn as_u16(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub record_type: WalRecordType,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportOffset {
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCheckpoint {
    pub wal_lsn: u64,
    pub wall_time_ms: i64,
    pub offsets: Vec<TransportOffset>,
}

impl WalCheckpoint {
    pub fn try_new(
        wal_lsn: u64,
        wall_time_ms: i64,
        offsets: Vec<TransportOffset>,
    ) -> io::Result<Self> {
        Ok(Self {
            wal_lsn,
            wall_time_ms,
            offsets: canonicalize_offsets(offsets, ErrorKind::InvalidInput)?,
        })
    }
}

pub fn encode_checkpoint_payload(checkpoint: &WalCheckpoint) -> io::Result<Vec<u8>> {
    let offsets = canonicalize_offsets(checkpoint.offsets.clone(), ErrorKind::InvalidInput)?;
    let offset_count = u32::try_from(offsets.len()).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "checkpoint offset count exceeds u32",
        )
    })?;

    let mut out = Vec::new();
    out.extend_from_slice(&CHECKPOINT_PAYLOAD_MAGIC.to_le_bytes());
    out.extend_from_slice(&CHECKPOINT_PAYLOAD_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&checkpoint.wal_lsn.to_le_bytes());
    out.extend_from_slice(&checkpoint.wall_time_ms.to_le_bytes());
    out.extend_from_slice(&offset_count.to_le_bytes());

    for offset in offsets {
        let topic = offset.topic.as_bytes();
        let topic_len = u16::try_from(topic.len())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "checkpoint topic too long"))?;
        out.extend_from_slice(&topic_len.to_le_bytes());
        out.extend_from_slice(topic);
        out.extend_from_slice(&offset.partition.to_le_bytes());
        out.extend_from_slice(&offset.next_offset.to_le_bytes());
    }

    Ok(out)
}

pub fn decode_checkpoint_payload(payload: &[u8]) -> io::Result<WalCheckpoint> {
    let mut cursor = 0usize;
    let magic = read_u32(payload, &mut cursor)?;
    if magic != CHECKPOINT_PAYLOAD_MAGIC {
        return Err(invalid_data("invalid checkpoint payload magic"));
    }

    let version = read_u16(payload, &mut cursor)?;
    if version != CHECKPOINT_PAYLOAD_VERSION {
        return Err(invalid_data("unsupported checkpoint payload version"));
    }
    let _reserved = read_u16(payload, &mut cursor)?;

    let wal_lsn = read_u64(payload, &mut cursor)?;
    let wall_time_ms = read_i64(payload, &mut cursor)?;
    let offset_count = read_u32(payload, &mut cursor)? as usize;
    let mut offsets = Vec::with_capacity(offset_count);

    for _ in 0..offset_count {
        let topic_len = read_u16(payload, &mut cursor)? as usize;
        let topic_bytes = read_bytes(payload, &mut cursor, topic_len)?;
        let topic = std::str::from_utf8(topic_bytes)
            .map_err(|_| invalid_data("checkpoint topic is not valid UTF-8"))?
            .to_string();
        let partition = read_i32(payload, &mut cursor)?;
        let next_offset = read_i64(payload, &mut cursor)?;
        offsets.push(TransportOffset {
            topic,
            partition,
            next_offset,
        });
    }

    if cursor != payload.len() {
        return Err(invalid_data("checkpoint payload has trailing bytes"));
    }

    Ok(WalCheckpoint {
        wal_lsn,
        wall_time_ms,
        offsets: canonicalize_offsets(offsets, ErrorKind::InvalidData)?,
    })
}

pub fn decode_checkpoint_record(record: &WalRecord) -> io::Result<WalCheckpoint> {
    if record.record_type != WalRecordType::Checkpoint {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "WAL record is not a checkpoint",
        ));
    }
    decode_checkpoint_payload(&record.payload)
}

pub fn write_wal_record<W: Write>(
    writer: &mut W,
    record_type: WalRecordType,
    payload: &[u8],
) -> io::Result<()> {
    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "WAL record payload length exceeds u64",
        )
    })?;
    let mut header = [0u8; WAL_RECORD_HEADER_LEN];
    header[0..4].copy_from_slice(&WAL_RECORD_MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&WAL_RECORD_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&record_type.as_u16().to_le_bytes());
    header[8..16].copy_from_slice(&payload_len.to_le_bytes());

    writer.write_all(&header)?;
    writer.write_all(payload)?;

    let crc = record_crc_parts(&header, payload);
    writer.write_all(&crc.to_le_bytes())
}

pub fn read_wal_record<R: Read>(reader: &mut R) -> io::Result<Option<WalRecord>> {
    let mut header = [0u8; WAL_RECORD_HEADER_LEN];
    match reader.read(&mut header[..1])? {
        0 => return Ok(None),
        1 => {}
        _ => unreachable!("single-byte read returned more than one byte"),
    }
    reader.read_exact(&mut header[1..])?;

    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != WAL_RECORD_MAGIC {
        return Err(invalid_data("invalid WAL record magic"));
    }

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != WAL_RECORD_VERSION {
        return Err(invalid_data("unsupported WAL record version"));
    }

    let raw_type = u16::from_le_bytes(header[6..8].try_into().unwrap());
    let record_type = WalRecordType::from_u16(raw_type)?;

    let payload_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| invalid_data("WAL record payload length exceeds usize"))?;

    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;

    let mut crc_buf = [0u8; WAL_RECORD_TRAILER_LEN];
    reader.read_exact(&mut crc_buf)?;
    let expected_crc = u32::from_le_bytes(crc_buf);
    let actual_crc = record_crc_parts(&header, &payload);
    if actual_crc != expected_crc {
        return Err(invalid_data("WAL record checksum mismatch"));
    }

    Ok(Some(WalRecord {
        record_type,
        payload,
    }))
}

pub struct WalWriter {
    file: File,
}

impl WalWriter {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn open_append(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file })
    }

    pub fn append(&mut self, record_type: WalRecordType, payload: &[u8]) -> io::Result<u64> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        write_wal_record(&mut self.file, record_type, payload)?;
        Ok(offset)
    }

    pub fn current_offset(&mut self) -> io::Result<u64> {
        self.file.seek(SeekFrom::End(0))
    }

    pub fn append_checkpoint(
        &mut self,
        wall_time_ms: i64,
        offsets: Vec<TransportOffset>,
    ) -> io::Result<WalCheckpoint> {
        let wal_lsn = self.current_offset()?;
        let checkpoint = WalCheckpoint::try_new(wal_lsn, wall_time_ms, offsets)?;
        let payload = encode_checkpoint_payload(&checkpoint)?;
        let appended_lsn = self.append(WalRecordType::Checkpoint, &payload)?;
        debug_assert_eq!(wal_lsn, appended_lsn);
        Ok(checkpoint)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

pub struct WalReader {
    file: File,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
        })
    }

    pub fn read_next(&mut self) -> io::Result<Option<WalRecord>> {
        read_wal_record(&mut self.file)
    }
}

pub fn checkpoint_meta_path(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join(CHECKPOINT_META_FILE_NAME)
}

pub fn write_checkpoint_meta(dir: impl AsRef<Path>, checkpoint: &WalCheckpoint) -> io::Result<()> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir)?;
    let payload = encode_checkpoint_payload(checkpoint)?;
    let header = checkpoint_meta_header(payload.len())?;
    let crc = checkpoint_meta_crc(&header, &payload);

    let temp_path = dir.join(CHECKPOINT_META_TEMP_FILE_NAME);
    let final_path = checkpoint_meta_path(dir);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(&header)?;
        file.write_all(&payload)?;
        file.write_all(&crc.to_le_bytes())?;
        file.sync_all()?;
    }

    fs::rename(&temp_path, &final_path)?;
    sync_directory(dir)
}

pub fn read_checkpoint_meta(dir: impl AsRef<Path>) -> io::Result<Option<WalCheckpoint>> {
    let path = checkpoint_meta_path(dir);
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };

    let mut header = [0u8; CHECKPOINT_META_HEADER_LEN];
    file.read_exact(&mut header)?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != CHECKPOINT_META_MAGIC {
        return Err(invalid_data("invalid checkpoint.meta magic"));
    }

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != CHECKPOINT_META_VERSION {
        return Err(invalid_data("unsupported checkpoint.meta version"));
    }

    let payload_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| invalid_data("checkpoint.meta payload length exceeds usize"))?;
    let mut payload = vec![0u8; payload_len];
    file.read_exact(&mut payload)?;

    let mut crc_buf = [0u8; CHECKPOINT_META_TRAILER_LEN];
    file.read_exact(&mut crc_buf)?;
    let expected_crc = u32::from_le_bytes(crc_buf);
    let actual_crc = checkpoint_meta_crc(&header, &payload);
    if expected_crc != actual_crc {
        return Err(invalid_data("checkpoint.meta checksum mismatch"));
    }

    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(invalid_data("checkpoint.meta has trailing bytes"));
    }

    decode_checkpoint_payload(&payload).map(Some)
}

fn invalid_data(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn record_crc_parts(header: &[u8; WAL_RECORD_HEADER_LEN], payload: &[u8]) -> u32 {
    crc32c_append(crc32c(header), payload)
}

fn checkpoint_meta_header(payload_len: usize) -> io::Result<[u8; CHECKPOINT_META_HEADER_LEN]> {
    let payload_len = u64::try_from(payload_len).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "checkpoint.meta payload length exceeds u64",
        )
    })?;
    let mut header = [0u8; CHECKPOINT_META_HEADER_LEN];
    header[0..4].copy_from_slice(&CHECKPOINT_META_MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&CHECKPOINT_META_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&0u16.to_le_bytes());
    header[8..16].copy_from_slice(&payload_len.to_le_bytes());
    Ok(header)
}

fn checkpoint_meta_crc(header: &[u8; CHECKPOINT_META_HEADER_LEN], payload: &[u8]) -> u32 {
    crc32c_append(crc32c(header), payload)
}

fn canonicalize_offsets(
    mut offsets: Vec<TransportOffset>,
    duplicate_error_kind: ErrorKind,
) -> io::Result<Vec<TransportOffset>> {
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then_with(|| left.partition.cmp(&right.partition))
    });

    for pair in offsets.windows(2) {
        if pair[0].topic == pair[1].topic && pair[0].partition == pair[1].partition {
            return Err(Error::new(
                duplicate_error_kind,
                "duplicate checkpoint offset for topic partition",
            ));
        }
    }

    Ok(offsets)
}

fn read_bytes<'a>(buf: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "checkpoint payload truncated"))?;
    if end > buf.len() {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "checkpoint payload truncated",
        ));
    }
    let bytes = &buf[*cursor..end];
    *cursor = end;
    Ok(bytes)
}

fn read_array<const N: usize>(buf: &[u8], cursor: &mut usize) -> io::Result<[u8; N]> {
    let bytes = read_bytes(buf, cursor, N)?;
    Ok(bytes.try_into().unwrap())
}

fn read_u16(buf: &[u8], cursor: &mut usize) -> io::Result<u16> {
    Ok(u16::from_le_bytes(read_array(buf, cursor)?))
}

fn read_u32(buf: &[u8], cursor: &mut usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(read_array(buf, cursor)?))
}

fn read_i32(buf: &[u8], cursor: &mut usize) -> io::Result<i32> {
    Ok(i32::from_le_bytes(read_array(buf, cursor)?))
}

fn read_u64(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(read_array(buf, cursor)?))
}

fn read_i64(buf: &[u8], cursor: &mut usize) -> io::Result<i64> {
    Ok(i64::from_le_bytes(read_array(buf, cursor)?))
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
fn record_crc(record_without_crc: &[u8]) -> u32 {
    crc32c(record_without_crc)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, ErrorKind};

    use super::*;

    #[test]
    fn wal_record_roundtrips_payload() {
        let mut bytes = Vec::new();
        write_wal_record(&mut bytes, WalRecordType::OtlpBatch, b"batch-1").unwrap();

        let mut cursor = Cursor::new(bytes);
        let record = read_wal_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record.record_type, WalRecordType::OtlpBatch);
        assert_eq!(record.payload, b"batch-1");
        assert!(read_wal_record(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn wal_record_reader_returns_none_on_clean_eof() {
        let mut cursor = Cursor::new(Vec::new());

        assert!(read_wal_record(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn wal_record_rejects_bad_crc32c() {
        let mut bytes = Vec::new();
        write_wal_record(&mut bytes, WalRecordType::Checkpoint, b"checkpoint").unwrap();
        bytes[WAL_RECORD_HEADER_LEN] ^= 0xff;

        let err = read_wal_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn wal_record_rejects_invalid_magic() {
        let mut bytes = Vec::new();
        write_wal_record(&mut bytes, WalRecordType::OtlpBatch, b"batch-1").unwrap();
        bytes[0] ^= 0xff;

        let err = read_wal_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn wal_record_rejects_unsupported_version() {
        let payload = b"payload";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&WAL_RECORD_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&99u16.to_le_bytes());
        bytes.extend_from_slice(&(WalRecordType::OtlpBatch as u16).to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&record_crc(&bytes).to_le_bytes());

        let err = read_wal_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn wal_record_rejects_truncated_payload() {
        let mut bytes = Vec::new();
        write_wal_record(&mut bytes, WalRecordType::OtlpBatch, b"batch-1").unwrap();
        bytes.truncate(bytes.len() - WAL_RECORD_TRAILER_LEN - 2);

        let err = read_wal_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn wal_record_rejects_truncated_crc32c_trailer() {
        let mut bytes = Vec::new();
        write_wal_record(&mut bytes, WalRecordType::OtlpBatch, b"batch-1").unwrap();
        bytes.truncate(bytes.len() - 2);

        let err = read_wal_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn wal_record_rejects_unknown_type() {
        let payload = b"payload";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&WAL_RECORD_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&WAL_RECORD_VERSION.to_le_bytes());
        bytes.extend_from_slice(&99u16.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&record_crc(&bytes).to_le_bytes());

        let err = read_wal_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn checkpoint_payload_roundtrips_sorted_transport_offsets() {
        let checkpoint = WalCheckpoint::try_new(
            128,
            1_725_000_000_000,
            vec![
                TransportOffset {
                    topic: "metrics-b".to_string(),
                    partition: 1,
                    next_offset: 30,
                },
                TransportOffset {
                    topic: "metrics-a".to_string(),
                    partition: 2,
                    next_offset: 20,
                },
                TransportOffset {
                    topic: "metrics-a".to_string(),
                    partition: 0,
                    next_offset: 10,
                },
            ],
        )
        .unwrap();

        let payload = encode_checkpoint_payload(&checkpoint).unwrap();
        let decoded = decode_checkpoint_payload(&payload).unwrap();

        assert_eq!(decoded.wal_lsn, 128);
        assert_eq!(decoded.wall_time_ms, 1_725_000_000_000);
        assert_eq!(
            decoded.offsets,
            vec![
                TransportOffset {
                    topic: "metrics-a".to_string(),
                    partition: 0,
                    next_offset: 10,
                },
                TransportOffset {
                    topic: "metrics-a".to_string(),
                    partition: 2,
                    next_offset: 20,
                },
                TransportOffset {
                    topic: "metrics-b".to_string(),
                    partition: 1,
                    next_offset: 30,
                },
            ]
        );
    }

    #[test]
    fn checkpoint_payload_rejects_duplicate_partition_offsets() {
        let err = WalCheckpoint::try_new(
            128,
            1_725_000_000_000,
            vec![
                TransportOffset {
                    topic: "metrics".to_string(),
                    partition: 0,
                    next_offset: 10,
                },
                TransportOffset {
                    topic: "metrics".to_string(),
                    partition: 0,
                    next_offset: 11,
                },
            ],
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn checkpoint_payload_rejects_truncated_offset_entry() {
        let checkpoint = WalCheckpoint::try_new(
            128,
            1_725_000_000_000,
            vec![TransportOffset {
                topic: "metrics".to_string(),
                partition: 0,
                next_offset: 10,
            }],
        )
        .unwrap();
        let mut payload = encode_checkpoint_payload(&checkpoint).unwrap();
        payload.truncate(payload.len() - 3);

        let err = decode_checkpoint_payload(&payload).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }
}
