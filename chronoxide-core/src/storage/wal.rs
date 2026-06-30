use std::fs::{self, File, OpenOptions};
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32c::{crc32c, crc32c_append};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;

use crate::storage::manifest::ManifestInventory;

pub const WAL_RECORD_MAGIC: u32 = u32::from_le_bytes(*b"CWAL");
pub const WAL_RECORD_VERSION: u16 = 1;
pub const WAL_RECORD_HEADER_LEN: usize = 16;
pub const WAL_RECORD_TRAILER_LEN: usize = 4;
pub const CHECKPOINT_META_FILE_NAME: &str = "checkpoint.meta";
pub const WAL_LSN_SEQUENCE_SHIFT: u32 = 40;
pub const WAL_LSN_OFFSET_MASK: u64 = (1u64 << WAL_LSN_SEQUENCE_SHIFT) - 1;
pub const WAL_LSN_MAX_SEQUENCE: u32 = (u64::MAX >> WAL_LSN_SEQUENCE_SHIFT) as u32;

const CHECKPOINT_PAYLOAD_MAGIC: u32 = u32::from_le_bytes(*b"WCHK");
const CHECKPOINT_PAYLOAD_VERSION: u16 = 1;
const CHECKPOINT_META_MAGIC: u32 = u32::from_le_bytes(*b"CMET");
const CHECKPOINT_META_VERSION: u16 = 1;
const CHECKPOINT_META_TEMP_FILE_NAME: &str = "checkpoint.meta.tmp";
const CHECKPOINT_META_HEADER_LEN: usize = 16;
const CHECKPOINT_META_TRAILER_LEN: usize = 4;
const OTLP_BATCH_PAYLOAD_MAGIC: u32 = u32::from_le_bytes(*b"OBAT");
const OTLP_BATCH_PAYLOAD_VERSION: u16 = 1;
const OTLP_BATCH_FLAG_FALLBACK_TS: u16 = 1;
const OTLP_BATCH_HEADER_LEN: usize = 24;

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

#[derive(Debug, Clone, PartialEq)]
pub struct OtlpWalBatch {
    pub request: ExportMetricsServiceRequest,
    pub fallback_ts_ms: Option<i64>,
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

pub fn encode_otlp_batch_payload(batch: &OtlpWalBatch) -> io::Result<Vec<u8>> {
    let mut proto = Vec::with_capacity(batch.request.encoded_len());
    batch
        .request
        .encode(&mut proto)
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;
    let proto_len = u64::try_from(proto.len()).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "OTLP WAL batch protobuf length exceeds u64",
        )
    })?;

    let mut flags = 0u16;
    let fallback_ts_ms = match batch.fallback_ts_ms {
        Some(value) => {
            flags |= OTLP_BATCH_FLAG_FALLBACK_TS;
            value
        }
        None => 0,
    };

    let mut out = Vec::with_capacity(OTLP_BATCH_HEADER_LEN + proto.len());
    out.extend_from_slice(&OTLP_BATCH_PAYLOAD_MAGIC.to_le_bytes());
    out.extend_from_slice(&OTLP_BATCH_PAYLOAD_VERSION.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&fallback_ts_ms.to_le_bytes());
    out.extend_from_slice(&proto_len.to_le_bytes());
    out.extend_from_slice(&proto);
    Ok(out)
}

pub fn decode_otlp_batch_payload(payload: &[u8]) -> io::Result<OtlpWalBatch> {
    let mut cursor = 0usize;
    let magic = read_u32(payload, &mut cursor)?;
    if magic != OTLP_BATCH_PAYLOAD_MAGIC {
        return Err(invalid_data("invalid OTLP WAL batch payload magic"));
    }

    let version = read_u16(payload, &mut cursor)?;
    if version != OTLP_BATCH_PAYLOAD_VERSION {
        return Err(invalid_data("unsupported OTLP WAL batch payload version"));
    }

    let flags = read_u16(payload, &mut cursor)?;
    if flags & !OTLP_BATCH_FLAG_FALLBACK_TS != 0 {
        return Err(invalid_data("unsupported OTLP WAL batch payload flags"));
    }

    let fallback_ts_ms = read_i64(payload, &mut cursor)?;
    let proto_len = read_u64(payload, &mut cursor)?;
    let proto_len = usize::try_from(proto_len)
        .map_err(|_| invalid_data("OTLP WAL batch protobuf length exceeds usize"))?;
    let proto = read_bytes(payload, &mut cursor, proto_len)?;
    if cursor != payload.len() {
        return Err(invalid_data("OTLP WAL batch payload has trailing bytes"));
    }

    let request = ExportMetricsServiceRequest::decode(proto)
        .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
    Ok(OtlpWalBatch {
        request,
        fallback_ts_ms: if flags & OTLP_BATCH_FLAG_FALLBACK_TS != 0 {
            Some(fallback_ts_ms)
        } else {
            None
        },
    })
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

    pub fn append_otlp_batch(&mut self, batch: &OtlpWalBatch) -> io::Result<u64> {
        let payload = encode_otlp_batch_payload(batch)?;
        self.append(WalRecordType::OtlpBatch, &payload)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalTruncationReport {
    pub safe_lsn: Option<u64>,
    pub active_sequence: u32,
    pub deleted_files: Vec<String>,
    pub kept_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalFileEntry {
    sequence: u32,
    file_name: String,
    path: PathBuf,
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

pub fn wal_file_name(sequence: u32) -> String {
    format!("wal-{sequence:06}.log")
}

pub fn parse_wal_file_name(file_name: &str) -> io::Result<u32> {
    let Some(raw_sequence) = file_name
        .strip_prefix("wal-")
        .and_then(|value| value.strip_suffix(".log"))
    else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "WAL file name must be wal-000000.log style",
        ));
    };
    if raw_sequence.len() != 6 || !raw_sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "WAL file sequence must be six decimal digits",
        ));
    }
    raw_sequence
        .parse::<u32>()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))
}

pub fn wal_lsn(sequence: u32, offset: u64) -> io::Result<u64> {
    if sequence > WAL_LSN_MAX_SEQUENCE {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "WAL file sequence exceeds encodable LSN range",
        ));
    }
    if offset > WAL_LSN_OFFSET_MASK {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "WAL file offset exceeds encodable LSN range",
        ));
    }
    Ok(((sequence as u64) << WAL_LSN_SEQUENCE_SHIFT) | offset)
}

pub fn wal_lsn_sequence(lsn: u64) -> u32 {
    (lsn >> WAL_LSN_SEQUENCE_SHIFT) as u32
}

pub fn wal_lsn_offset(lsn: u64) -> u64 {
    lsn & WAL_LSN_OFFSET_MASK
}

pub fn safe_wal_truncation_lsn(inventory: &ManifestInventory) -> Option<u64> {
    let mut safe_lsn: Option<u64> = None;
    for segment in &inventory.segments {
        let boundary = segment.wal_lsn_boundary?;
        safe_lsn = Some(match safe_lsn {
            Some(existing) => existing.min(boundary),
            None => boundary,
        });
    }
    safe_lsn
}

pub fn truncate_wal_prefix_from_manifest(
    wal_dir: impl AsRef<Path>,
    inventory: &ManifestInventory,
    active_sequence: u32,
) -> io::Result<WalTruncationReport> {
    let safe_lsn = safe_wal_truncation_lsn(inventory);
    truncate_wal_prefix(wal_dir, safe_lsn, active_sequence)
}

pub fn truncate_wal_prefix(
    wal_dir: impl AsRef<Path>,
    safe_lsn: Option<u64>,
    active_sequence: u32,
) -> io::Result<WalTruncationReport> {
    let wal_dir = wal_dir.as_ref();
    let files = discover_wal_files(wal_dir)?;
    let mut report = WalTruncationReport {
        safe_lsn,
        active_sequence,
        deleted_files: Vec::new(),
        kept_files: Vec::new(),
    };

    let Some(safe_lsn) = safe_lsn else {
        report
            .kept_files
            .extend(files.into_iter().map(|file| file.file_name));
        return Ok(report);
    };
    let safe_sequence = wal_lsn_sequence(safe_lsn);

    for file in files {
        if file.sequence < safe_sequence && file.sequence < active_sequence {
            fs::remove_file(&file.path)?;
            report.deleted_files.push(file.file_name);
        } else {
            report.kept_files.push(file.file_name);
        }
    }

    if !report.deleted_files.is_empty() {
        sync_directory(wal_dir)?;
    }
    Ok(report)
}

fn discover_wal_files(wal_dir: &Path) -> io::Result<Vec<WalFileEntry>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        let Ok(sequence) = parse_wal_file_name(&file_name) else {
            continue;
        };
        files.push(WalFileEntry {
            sequence,
            file_name,
            path: entry.path(),
        });
    }
    files.sort_by(|left, right| {
        left.sequence
            .cmp(&right.sequence)
            .then_with(|| left.file_name.cmp(&right.file_name))
    });
    Ok(files)
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
    use crate::storage::manifest::{ManifestInventory, ManifestRecord, ManifestSegment};
    use crate::storage::segment::SegmentId;

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
    fn wal_truncation_file_names_roundtrip_strict_sequence_names() {
        assert_eq!(wal_file_name(7), "wal-000007.log");
        assert_eq!(parse_wal_file_name("wal-000007.log").unwrap(), 7);
        assert!(parse_wal_file_name("wal-7.log").is_err());
        assert!(parse_wal_file_name("../wal-000007.log").is_err());
    }

    #[test]
    fn wal_truncation_lsn_encodes_file_sequence_and_offset() {
        let lsn = wal_lsn(3, 12_345).unwrap();

        assert_eq!(wal_lsn_sequence(lsn), 3);
        assert_eq!(wal_lsn_offset(lsn), 12_345);
        assert!(wal_lsn(1, WAL_LSN_OFFSET_MASK + 1).is_err());
    }

    #[test]
    fn wal_truncation_safe_lsn_uses_min_manifest_boundary() {
        let first = SegmentId::new(1_000, 2_000).unwrap();
        let second = SegmentId::new(2_000, 3_000).unwrap();
        let inventory = ManifestInventory::from_records(vec![
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(
                    first.dir_name(),
                    1_000,
                    2_000,
                    Some(wal_lsn(2, 400).unwrap()),
                )
                .unwrap(),
            ),
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(
                    second.dir_name(),
                    2_000,
                    3_000,
                    Some(wal_lsn(1, 900).unwrap()),
                )
                .unwrap(),
            ),
        ])
        .unwrap();

        assert_eq!(
            safe_wal_truncation_lsn(&inventory),
            Some(wal_lsn(1, 900).unwrap())
        );
    }

    #[test]
    fn wal_truncation_safe_lsn_requires_every_live_segment_boundary() {
        let first = SegmentId::new(1_000, 2_000).unwrap();
        let second = SegmentId::new(2_000, 3_000).unwrap();
        let inventory = ManifestInventory::from_records(vec![
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(
                    first.dir_name(),
                    1_000,
                    2_000,
                    Some(wal_lsn(2, 400).unwrap()),
                )
                .unwrap(),
            ),
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(second.dir_name(), 2_000, 3_000, None).unwrap(),
            ),
        ])
        .unwrap();

        assert_eq!(safe_wal_truncation_lsn(&inventory), None);
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

    #[test]
    fn otlp_batch_payload_roundtrips_request_and_fallback_timestamp() {
        let batch = OtlpWalBatch {
            request: test_request("cpu.usage", 5_000, 1.5),
            fallback_ts_ms: Some(1_725_000_000_000),
        };

        let payload = encode_otlp_batch_payload(&batch).unwrap();
        let decoded = decode_otlp_batch_payload(&payload).unwrap();

        assert_eq!(decoded.fallback_ts_ms, Some(1_725_000_000_000));
        assert_eq!(decoded.request, batch.request);
    }

    #[test]
    fn otlp_batch_payload_rejects_truncated_protobuf() {
        let batch = OtlpWalBatch {
            request: test_request("cpu.usage", 5_000, 1.5),
            fallback_ts_ms: None,
        };
        let mut payload = encode_otlp_batch_payload(&batch).unwrap();
        payload.truncate(payload.len() - 2);

        let err = decode_otlp_batch_payload(&payload).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    fn test_request(
        metric_name: &str,
        timestamp_ms: u64,
        value: f64,
    ) -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
        use opentelemetry_proto::tonic;

        tonic::collector::metrics::v1::ExportMetricsServiceRequest {
            resource_metrics: vec![tonic::metrics::v1::ResourceMetrics {
                scope_metrics: vec![tonic::metrics::v1::ScopeMetrics {
                    metrics: vec![tonic::metrics::v1::Metric {
                        name: metric_name.to_string(),
                        data: Some(tonic::metrics::v1::metric::Data::Gauge(
                            tonic::metrics::v1::Gauge {
                                data_points: vec![tonic::metrics::v1::NumberDataPoint {
                                    time_unix_nano: timestamp_ms * 1_000_000,
                                    value: Some(
                                        tonic::metrics::v1::number_data_point::Value::AsDouble(
                                            value,
                                        ),
                                    ),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}
