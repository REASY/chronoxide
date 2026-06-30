use std::fs::{File, OpenOptions};
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crc32c::{crc32c, crc32c_append};

pub const WAL_RECORD_MAGIC: u32 = u32::from_le_bytes(*b"CWAL");
pub const WAL_RECORD_VERSION: u16 = 1;
pub const WAL_RECORD_HEADER_LEN: usize = 16;
pub const WAL_RECORD_TRAILER_LEN: usize = 4;

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

fn invalid_data(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn record_crc_parts(header: &[u8; WAL_RECORD_HEADER_LEN], payload: &[u8]) -> u32 {
    crc32c_append(crc32c(header), payload)
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
}
