use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32c::{crc32c, crc32c_append};

use crate::storage::segment::SegmentId;

pub const CURRENT_FILE_NAME: &str = "CURRENT";
pub const CURRENT_TEMP_FILE_NAME: &str = "CURRENT.tmp";
pub const MANIFEST_RECORD_HEADER_LEN: usize = 16;
pub const MANIFEST_RECORD_TRAILER_LEN: usize = 4;

const MANIFEST_RECORD_MAGIC: u32 = u32::from_le_bytes(*b"CMNF");
const MANIFEST_RECORD_VERSION: u16 = 1;
const MANIFEST_RECORD_TYPE_SEGMENT_SEALED: u16 = 1;
const MANIFEST_RECORD_TYPE_SEGMENT_DELETED: u16 = 2;
const MANIFEST_SEGMENT_FLAG_WAL_LSN_BOUNDARY: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSegment {
    pub segment_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub wal_lsn_boundary: Option<u64>,
}

impl ManifestSegment {
    pub fn new(
        segment_id: String,
        start_ms: u64,
        end_ms: u64,
        wal_lsn_boundary: Option<u64>,
    ) -> io::Result<Self> {
        validate_segment_id(&segment_id, start_ms, end_ms)?;
        Ok(Self {
            segment_id,
            start_ms,
            end_ms,
            wal_lsn_boundary,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRecord {
    SegmentSealed(ManifestSegment),
    SegmentDeleted { segment_id: String },
}

impl ManifestRecord {
    pub fn segment(&self) -> Option<&ManifestSegment> {
        match self {
            Self::SegmentSealed(segment) => Some(segment),
            Self::SegmentDeleted { .. } => None,
        }
    }

    fn record_type(&self) -> u16 {
        match self {
            Self::SegmentSealed(_) => MANIFEST_RECORD_TYPE_SEGMENT_SEALED,
            Self::SegmentDeleted { .. } => MANIFEST_RECORD_TYPE_SEGMENT_DELETED,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestInventory {
    /// Live segments in their latest seal-record append order. Query merge
    /// precedence relies on later entries appearing later in this vector.
    pub segments: Vec<ManifestSegment>,
}

impl ManifestInventory {
    pub fn from_records(records: Vec<ManifestRecord>) -> io::Result<Self> {
        let mut live = BTreeMap::<String, (usize, ManifestSegment)>::new();
        for (record_ordinal, record) in records.into_iter().enumerate() {
            match record {
                ManifestRecord::SegmentSealed(segment) => {
                    validate_segment_id(&segment.segment_id, segment.start_ms, segment.end_ms)?;
                    live.insert(segment.segment_id.clone(), (record_ordinal, segment));
                }
                ManifestRecord::SegmentDeleted { segment_id } => {
                    validate_manifest_segment_name(&segment_id)?;
                    live.remove(&segment_id);
                }
            }
        }

        let mut live: Vec<_> = live.into_values().collect();
        live.sort_by_key(|(record_ordinal, _)| *record_ordinal);
        let segments = live.into_iter().map(|(_, segment)| segment).collect();
        Ok(Self { segments })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionTombstoneReport {
    pub tombstoned_segments: Vec<String>,
}

pub fn append_retention_tombstones(
    writer: &mut ManifestWriter,
    inventory: &ManifestInventory,
    retain_after_ms: u64,
) -> io::Result<RetentionTombstoneReport> {
    let mut tombstoned_segments = Vec::new();
    for segment in &inventory.segments {
        if segment.end_ms > retain_after_ms {
            continue;
        }
        writer.append(&ManifestRecord::SegmentDeleted {
            segment_id: segment.segment_id.clone(),
        })?;
        tombstoned_segments.push(segment.segment_id.clone());
    }
    if !tombstoned_segments.is_empty() {
        writer.sync_all()?;
    }
    Ok(RetentionTombstoneReport {
        tombstoned_segments,
    })
}

pub fn manifest_file_name(sequence: u64) -> String {
    format!("MANIFEST-{sequence:06}")
}

pub fn write_manifest_record<W: Write>(writer: &mut W, record: &ManifestRecord) -> io::Result<()> {
    let payload = encode_manifest_payload(record)?;
    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "manifest record payload length exceeds u64",
        )
    })?;
    let mut header = [0u8; MANIFEST_RECORD_HEADER_LEN];
    header[0..4].copy_from_slice(&MANIFEST_RECORD_MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&MANIFEST_RECORD_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&record.record_type().to_le_bytes());
    header[8..16].copy_from_slice(&payload_len.to_le_bytes());

    writer.write_all(&header)?;
    writer.write_all(&payload)?;
    writer.write_all(&manifest_record_crc(&header, &payload).to_le_bytes())
}

pub fn read_manifest_record<R: Read>(reader: &mut R) -> io::Result<Option<ManifestRecord>> {
    let mut header = [0u8; MANIFEST_RECORD_HEADER_LEN];
    match reader.read(&mut header[..1])? {
        0 => return Ok(None),
        1 => {}
        _ => unreachable!("single-byte read returned more than one byte"),
    }
    reader.read_exact(&mut header[1..])?;

    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != MANIFEST_RECORD_MAGIC {
        return Err(invalid_data("invalid manifest record magic"));
    }

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != MANIFEST_RECORD_VERSION {
        return Err(invalid_data("unsupported manifest record version"));
    }

    let record_type = u16::from_le_bytes(header[6..8].try_into().unwrap());
    let payload_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| invalid_data("manifest record payload length exceeds usize"))?;
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;

    let mut crc_buf = [0u8; MANIFEST_RECORD_TRAILER_LEN];
    reader.read_exact(&mut crc_buf)?;
    let expected_crc = u32::from_le_bytes(crc_buf);
    let actual_crc = manifest_record_crc(&header, &payload);
    if expected_crc != actual_crc {
        return Err(invalid_data("manifest record checksum mismatch"));
    }

    decode_manifest_payload(record_type, &payload).map(Some)
}

pub struct ManifestWriter {
    file: File,
    path: PathBuf,
    file_name: String,
}

impl ManifestWriter {
    pub fn create(manifest_dir: impl AsRef<Path>, sequence: u64) -> io::Result<Self> {
        let file_name = manifest_file_name(sequence);
        let manifest_dir = manifest_dir.as_ref();
        fs::create_dir_all(manifest_dir)?;
        let path = manifest_dir.join(&file_name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            file,
            path,
            file_name,
        })
    }

    pub fn open_append(manifest_dir: impl AsRef<Path>, file_name: &str) -> io::Result<Self> {
        validate_manifest_file_name(file_name)?;
        let manifest_dir = manifest_dir.as_ref();
        let path = manifest_dir.join(file_name);
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(&path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            path,
            file_name: file_name.to_string(),
        })
    }

    pub fn append(&mut self, record: &ManifestRecord) -> io::Result<u64> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        write_manifest_record(&mut self.file, record)?;
        Ok(offset)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }
}

pub fn write_current(manifest_dir: impl AsRef<Path>, manifest_file_name: &str) -> io::Result<()> {
    validate_manifest_file_name(manifest_file_name)?;
    let manifest_dir = manifest_dir.as_ref();
    fs::create_dir_all(manifest_dir)?;
    let temp_path = manifest_dir.join(CURRENT_TEMP_FILE_NAME);
    let final_path = manifest_dir.join(CURRENT_FILE_NAME);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(manifest_file_name.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    fs::rename(&temp_path, &final_path)?;
    sync_directory(manifest_dir)
}

pub fn read_current(manifest_dir: impl AsRef<Path>) -> io::Result<Option<String>> {
    let path = manifest_dir.as_ref().join(CURRENT_FILE_NAME);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let raw = std::str::from_utf8(&bytes)
        .map_err(|_| invalid_data("manifest CURRENT is not valid UTF-8"))?
        .trim();
    validate_manifest_file_name(raw)?;
    Ok(Some(raw.to_string()))
}

pub fn read_manifest_inventory(
    manifest_dir: impl AsRef<Path>,
) -> io::Result<Option<ManifestInventory>> {
    let manifest_dir = manifest_dir.as_ref();
    let Some(file_name) = read_current(manifest_dir)? else {
        return Ok(None);
    };
    let records = read_manifest_records(manifest_dir.join(file_name))?;
    ManifestInventory::from_records(records).map(Some)
}

pub fn read_manifest_records(path: impl AsRef<Path>) -> io::Result<Vec<ManifestRecord>> {
    let mut file = File::open(path)?;
    let mut records = Vec::new();
    while let Some(record) = read_manifest_record(&mut file)? {
        records.push(record);
    }
    Ok(records)
}

fn encode_manifest_payload(record: &ManifestRecord) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    match record {
        ManifestRecord::SegmentSealed(segment) => {
            validate_segment_id(&segment.segment_id, segment.start_ms, segment.end_ms)?;
            let mut flags = 0u16;
            if segment.wal_lsn_boundary.is_some() {
                flags |= MANIFEST_SEGMENT_FLAG_WAL_LSN_BOUNDARY;
            }
            out.extend_from_slice(&flags.to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(&segment.start_ms.to_le_bytes());
            out.extend_from_slice(&segment.end_ms.to_le_bytes());
            write_string(&mut out, &segment.segment_id)?;
            if let Some(wal_lsn_boundary) = segment.wal_lsn_boundary {
                out.extend_from_slice(&wal_lsn_boundary.to_le_bytes());
            }
        }
        ManifestRecord::SegmentDeleted { segment_id } => {
            validate_manifest_segment_name(segment_id)?;
            write_string(&mut out, segment_id)?;
        }
    }
    Ok(out)
}

fn decode_manifest_payload(record_type: u16, payload: &[u8]) -> io::Result<ManifestRecord> {
    let mut cursor = 0usize;
    let record = match record_type {
        MANIFEST_RECORD_TYPE_SEGMENT_SEALED => {
            let flags = read_u16(payload, &mut cursor)?;
            if flags & !MANIFEST_SEGMENT_FLAG_WAL_LSN_BOUNDARY != 0 {
                return Err(invalid_data("unsupported manifest segment flags"));
            }
            let _reserved = read_u16(payload, &mut cursor)?;
            let start_ms = read_u64(payload, &mut cursor)?;
            let end_ms = read_u64(payload, &mut cursor)?;
            let segment_id = read_string(payload, &mut cursor)?;
            let wal_lsn_boundary = if flags & MANIFEST_SEGMENT_FLAG_WAL_LSN_BOUNDARY != 0 {
                Some(read_u64(payload, &mut cursor)?)
            } else {
                None
            };
            ManifestRecord::SegmentSealed(ManifestSegment::new(
                segment_id,
                start_ms,
                end_ms,
                wal_lsn_boundary,
            )?)
        }
        MANIFEST_RECORD_TYPE_SEGMENT_DELETED => {
            let segment_id = read_string(payload, &mut cursor)?;
            validate_manifest_segment_name(&segment_id)?;
            ManifestRecord::SegmentDeleted { segment_id }
        }
        _ => return Err(invalid_data("unknown manifest record type")),
    };
    if cursor != payload.len() {
        return Err(invalid_data("manifest record payload has trailing bytes"));
    }
    Ok(record)
}

fn write_string(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let len = u16::try_from(value.len())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "manifest string too long"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(buf: &[u8], cursor: &mut usize) -> io::Result<String> {
    let len = read_u16(buf, cursor)? as usize;
    let bytes = read_bytes(buf, cursor, len)?;
    std::str::from_utf8(bytes)
        .map(|value| value.to_string())
        .map_err(|_| invalid_data("manifest string is not valid UTF-8"))
}

fn validate_segment_id(segment_id: &str, start_ms: u64, end_ms: u64) -> io::Result<()> {
    let parsed = SegmentId::parse_dir_name(segment_id).map_err(|err| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("invalid manifest segment id: {err}"),
        )
    })?;
    if parsed.start_ms() != start_ms || parsed.end_ms() != end_ms {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "manifest segment id range does not match record range",
        ));
    }
    Ok(())
}

fn validate_manifest_segment_name(segment_id: &str) -> io::Result<()> {
    SegmentId::parse_dir_name(segment_id)
        .map(|_| ())
        .map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid segment id: {err}"),
            )
        })
}

fn validate_manifest_file_name(file_name: &str) -> io::Result<()> {
    let Some(number) = file_name.strip_prefix("MANIFEST-") else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "manifest file name must start with MANIFEST-",
        ));
    };
    if number.len() != 6 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "manifest file name must be MANIFEST-000000 style",
        ));
    }
    Ok(())
}

fn manifest_record_crc(header: &[u8; MANIFEST_RECORD_HEADER_LEN], payload: &[u8]) -> u32 {
    crc32c_append(crc32c(header), payload)
}

fn invalid_data(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn read_bytes<'a>(buf: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "manifest payload truncated"))?;
    if end > buf.len() {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "manifest payload truncated",
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

fn read_u64(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(read_array(buf, cursor)?))
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
mod tests {
    use std::io::{Cursor, ErrorKind};

    use super::*;
    use crate::storage::segment::SegmentId;

    #[test]
    fn manifest_record_roundtrips_segment_sealed() {
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let segment =
            ManifestSegment::new(segment_id.dir_name(), 1_000, 2_000, Some(9_999)).unwrap();
        let record = ManifestRecord::SegmentSealed(segment.clone());
        let mut bytes = Vec::new();

        write_manifest_record(&mut bytes, &record).unwrap();
        let decoded = read_manifest_record(&mut Cursor::new(bytes))
            .unwrap()
            .unwrap();

        assert_eq!(decoded, record);
        assert_eq!(decoded.segment().unwrap(), &segment);
    }

    #[test]
    fn manifest_record_roundtrips_segment_deleted() {
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let mut bytes = Vec::new();

        write_manifest_record(&mut bytes, &record).unwrap();
        let decoded = read_manifest_record(&mut Cursor::new(bytes))
            .unwrap()
            .unwrap();

        assert_eq!(decoded, record);
    }

    #[test]
    fn manifest_record_rejects_bad_crc32c() {
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let mut bytes = Vec::new();
        write_manifest_record(&mut bytes, &record).unwrap();
        bytes[MANIFEST_RECORD_HEADER_LEN] ^= 0xff;

        let err = read_manifest_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn manifest_record_rejects_truncated_payload() {
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let mut bytes = Vec::new();
        write_manifest_record(&mut bytes, &record).unwrap();
        bytes.truncate(bytes.len() - MANIFEST_RECORD_TRAILER_LEN - 1);

        let err = read_manifest_record(&mut Cursor::new(bytes)).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn manifest_inventory_applies_sealed_and_deleted_records() {
        let first_id = SegmentId::new(1_000, 2_000).unwrap();
        let second_id = SegmentId::new(2_000, 3_000).unwrap();
        let records = vec![
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(first_id.dir_name(), 1_000, 2_000, Some(100)).unwrap(),
            ),
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(second_id.dir_name(), 2_000, 3_000, Some(200)).unwrap(),
            ),
            ManifestRecord::SegmentDeleted {
                segment_id: first_id.dir_name(),
            },
        ];

        let inventory = ManifestInventory::from_records(records).unwrap();

        assert_eq!(inventory.segments.len(), 1);
        assert_eq!(inventory.segments[0].segment_id, second_id.dir_name());
    }

    #[test]
    fn manifest_inventory_preserves_live_seal_append_order() {
        let lexically_later =
            SegmentId::parse_dir_name("seg-1000-2000-7ZZZZZZZZZZZZZZZZZZZZZZZZZ").unwrap();
        let lexically_earlier =
            SegmentId::parse_dir_name("seg-1000-2000-00000000000000000000000001").unwrap();
        let sealed = |id: SegmentId| {
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(id.dir_name(), 1_000, 2_000, None).unwrap(),
            )
        };

        let inventory = ManifestInventory::from_records(vec![
            sealed(lexically_later),
            sealed(lexically_earlier),
        ])
        .unwrap();

        assert_eq!(
            inventory
                .segments
                .iter()
                .map(|segment| segment.segment_id.clone())
                .collect::<Vec<_>>(),
            vec![lexically_later.dir_name(), lexically_earlier.dir_name()]
        );

        let resealed = ManifestInventory::from_records(vec![
            sealed(lexically_later),
            sealed(lexically_earlier),
            ManifestRecord::SegmentDeleted {
                segment_id: lexically_later.dir_name(),
            },
            sealed(lexically_later),
        ])
        .unwrap();
        assert_eq!(
            resealed
                .segments
                .iter()
                .map(|segment| segment.segment_id.clone())
                .collect::<Vec<_>>(),
            vec![lexically_earlier.dir_name(), lexically_later.dir_name()]
        );
    }

    #[test]
    fn manifest_retention_tombstones_segments_at_or_before_cutoff() {
        let first_id = SegmentId::new(1_000, 2_000).unwrap();
        let second_id = SegmentId::new(2_000, 3_000).unwrap();
        let third_id = SegmentId::new(3_000, 4_000).unwrap();
        let records = vec![
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(first_id.dir_name(), 1_000, 2_000, Some(100)).unwrap(),
            ),
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(second_id.dir_name(), 2_000, 3_000, Some(200)).unwrap(),
            ),
            ManifestRecord::SegmentSealed(
                ManifestSegment::new(third_id.dir_name(), 3_000, 4_000, Some(300)).unwrap(),
            ),
        ];
        let inventory = ManifestInventory::from_records(records.clone()).unwrap();
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();

        let report = append_retention_tombstones(&mut writer, &inventory, 3_000).unwrap();
        writer.sync_all().unwrap();

        assert_eq!(
            report.tombstoned_segments,
            vec![first_id.dir_name(), second_id.dir_name()]
        );
        let mut replay = records;
        replay.extend(read_manifest_records(writer.path()).unwrap());
        let retained = ManifestInventory::from_records(replay).unwrap();
        assert_eq!(retained.segments.len(), 1);
        assert_eq!(retained.segments[0].segment_id, third_id.dir_name());
    }

    #[test]
    fn current_rejects_unsafe_manifest_file_names() {
        assert!(validate_manifest_file_name("../MANIFEST-000001").is_err());
        assert!(validate_manifest_file_name("MANIFEST-current").is_err());
        assert!(validate_manifest_file_name("MANIFEST-000001").is_ok());
    }
}
