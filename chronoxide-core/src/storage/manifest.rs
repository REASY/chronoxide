use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crc32c::{crc32c, crc32c_append};
use sha2::{Digest, Sha256};

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

/// One integrity-validated manifest prefix used by an immutable query view.
///
/// `Absent` is a proven initial state, not an error fallback: it is valid only
/// before the first manifest publication leaves any `CURRENT`, `CURRENT.tmp`,
/// or `MANIFEST-*` evidence and permits a head-only live view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestCut {
    Absent,
    Present {
        file_name: String,
        validated_offset: u64,
        prefix_sha256: [u8; 32],
    },
}

impl ManifestCut {
    pub fn validated_offset(&self) -> u64 {
        match self {
            Self::Absent => 0,
            Self::Present {
                validated_offset, ..
            } => *validated_offset,
        }
    }
}

/// A complete, CRC-validated manifest state and the exact byte prefix that
/// produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSnapshot {
    pub cut: ManifestCut,
    pub records: Vec<ManifestRecord>,
    pub inventory: ManifestInventory,
}

impl ManifestSnapshot {
    pub fn absent() -> Self {
        Self {
            cut: ManifestCut::Absent,
            records: Vec::new(),
            inventory: ManifestInventory {
                segments: Vec::new(),
            },
        }
    }
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
    writer.write_all(&encode_manifest_record(record)?)
}

/// Encodes exactly the bytes appended for one version-1 manifest record.
///
/// Retryable publication retains this value so an ambiguous short append can
/// be reconciled against the intended bytes without inventing a new record
/// format or re-encoding mutable input.
pub fn encode_manifest_record(record: &ManifestRecord) -> io::Result<Vec<u8>> {
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

    let capacity = MANIFEST_RECORD_HEADER_LEN
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(MANIFEST_RECORD_TRAILER_LEN))
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "manifest record length overflow"))?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(capacity)
        .map_err(|error| Error::new(ErrorKind::OutOfMemory, error))?;
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(&payload);
    encoded.extend_from_slice(&manifest_record_crc(&header, &payload).to_le_bytes());
    Ok(encoded)
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

/// Process-local exclusive coordinator for manifest mutation and ambiguous
/// append reconciliation.
///
/// All callers for the same canonical manifest directory receive the same
/// coordinator. The byte format remains manifest version 1.
#[derive(Debug)]
pub struct ManifestCoordinator {
    manifest_dir: PathBuf,
    mutation: Mutex<ManifestCoordinatorState>,
    #[cfg(test)]
    completed_manifest_syncs: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fail_next_completed_manifest_sync: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_next_current_directory_sync: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Default)]
struct ManifestCoordinatorState {
    active_token: Option<u64>,
    next_token: u64,
}

fn manifest_coordinator_registry() -> &'static Mutex<HashMap<PathBuf, Weak<ManifestCoordinator>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<ManifestCoordinator>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

impl ManifestCoordinator {
    pub fn shared(manifest_dir: impl AsRef<Path>) -> io::Result<Arc<Self>> {
        let manifest_dir = manifest_dir.as_ref();
        fs::create_dir_all(manifest_dir)?;
        let canonical = fs::canonicalize(manifest_dir)?;
        let mut registry = manifest_coordinator_registry()
            .lock()
            .map_err(|_| Error::other("manifest coordinator registry lock poisoned"))?;
        if let Some(existing) = registry.get(&canonical).and_then(Weak::upgrade) {
            return Ok(existing);
        }
        registry.retain(|_, coordinator| coordinator.strong_count() > 0);
        let coordinator = Arc::new(Self {
            manifest_dir: canonical.clone(),
            mutation: Mutex::new(ManifestCoordinatorState::default()),
            #[cfg(test)]
            completed_manifest_syncs: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            fail_next_completed_manifest_sync: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_next_current_directory_sync: std::sync::atomic::AtomicBool::new(false),
        });
        registry.insert(canonical, Arc::downgrade(&coordinator));
        Ok(coordinator)
    }

    /// Captures the exact intended bytes and known pre-append prefix.
    ///
    /// The returned attempt is reusable. Calling `commit` again after an
    /// ambiguous I/O error either proves the record already committed or
    /// repairs only its exact incomplete prefix.
    pub fn prepare_append(
        self: &Arc<Self>,
        record: ManifestRecord,
    ) -> io::Result<ManifestAppendAttempt> {
        let mut state = self
            .mutation
            .lock()
            .map_err(|_| Error::other("manifest coordinator lock poisoned"))?;
        if state.active_token.is_some() {
            return Err(Error::new(
                ErrorKind::WouldBlock,
                "another manifest mutation attempt is still active",
            ));
        }
        let snapshot = read_manifest_snapshot(&self.manifest_dir)?;
        let (file_name, pre_append_offset, pre_append_prefix_sha256) = match &snapshot.cut {
            ManifestCut::Absent => (manifest_file_name(1), 0, sha256(&[])),
            ManifestCut::Present {
                file_name,
                validated_offset,
                prefix_sha256,
            } => (file_name.clone(), *validated_offset, *prefix_sha256),
        };
        let encoded_record = encode_manifest_record(&record)?;
        let token = state
            .next_token
            .checked_add(1)
            .ok_or_else(|| Error::other("manifest coordinator token overflow"))?;
        state.next_token = token;
        state.active_token = Some(token);
        Ok(ManifestAppendAttempt {
            coordinator: Arc::clone(self),
            token,
            record,
            encoded_record,
            file_name,
            pre_append_offset,
            pre_append_prefix_sha256,
        })
    }

    fn commit_attempt(&self, attempt: &ManifestAppendAttempt) -> io::Result<ManifestSnapshot> {
        let mut state = self
            .mutation
            .lock()
            .map_err(|_| Error::other("manifest coordinator lock poisoned"))?;
        if state.active_token != Some(attempt.token) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "manifest append attempt is no longer active",
            ));
        }
        validate_manifest_file_name(&attempt.file_name)?;
        let path = self.manifest_dir.join(&attempt.file_name);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let bytes = {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            bytes
        };
        let offset = usize::try_from(attempt.pre_append_offset)
            .map_err(|_| invalid_data("manifest append offset exceeds address space"))?;
        let prefix = bytes
            .get(..offset)
            .ok_or_else(|| invalid_data("manifest became shorter than append prefix"))?;
        if sha256(prefix) != attempt.pre_append_prefix_sha256 {
            return Err(invalid_data(
                "manifest append prefix changed before reconciliation",
            ));
        }
        let suffix = &bytes[offset..];
        if suffix == attempt.encoded_record {
            // The append reached stable bytes but CURRENT or its directory
            // sync may have failed. Re-establish a successful manifest
            // durability barrier before finishing publication below.
        } else if suffix.is_empty()
            || (suffix.len() < attempt.encoded_record.len()
                && attempt.encoded_record.starts_with(suffix))
        {
            if !suffix.is_empty() {
                file.set_len(attempt.pre_append_offset)?;
                file.sync_all()?;
            }
            file.seek(SeekFrom::Start(attempt.pre_append_offset))?;
            file.write_all(&attempt.encoded_record)?;
        } else {
            return Err(invalid_data(
                "manifest tail is neither empty, intended record, nor its exact prefix",
            ));
        }

        // This is required even when the exact complete record was already
        // present: the preceding attempt may have returned a manifest fsync
        // error, so byte equality alone is not a durability proof.
        self.sync_completed_manifest(&file)?;
        #[cfg(test)]
        let fail_current_directory_sync = self
            .fail_next_current_directory_sync
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(test))]
        let fail_current_directory_sync = false;
        write_current_with(
            &self.manifest_dir,
            &attempt.file_name,
            fail_current_directory_sync,
        )?;
        let snapshot = read_manifest_snapshot(&self.manifest_dir)?;
        let expected_offset = attempt
            .pre_append_offset
            .checked_add(u64::try_from(attempt.encoded_record.len()).map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "encoded manifest record length exceeds u64",
                )
            })?)
            .ok_or_else(|| invalid_data("manifest append end offset overflow"))?;
        if snapshot.cut.validated_offset() != expected_offset
            || snapshot.records.last() != Some(&attempt.record)
        {
            return Err(invalid_data(
                "manifest append reconciliation produced an unexpected cut",
            ));
        }
        state.active_token = None;
        Ok(snapshot)
    }

    fn sync_completed_manifest(&self, file: &File) -> io::Result<()> {
        #[cfg(test)]
        {
            self.completed_manifest_syncs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self
                .fail_next_completed_manifest_sync
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                return Err(Error::other("injected completed-manifest sync failure"));
            }
        }
        file.sync_all()
    }

    #[cfg(test)]
    fn completed_manifest_sync_count(&self) -> u64 {
        self.completed_manifest_syncs
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_completed_manifest_sync(&self) {
        self.fail_next_completed_manifest_sync
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_current_directory_sync(&self) {
        self.fail_next_current_directory_sync
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct ManifestAppendAttempt {
    coordinator: Arc<ManifestCoordinator>,
    token: u64,
    record: ManifestRecord,
    encoded_record: Vec<u8>,
    file_name: String,
    pre_append_offset: u64,
    pre_append_prefix_sha256: [u8; 32],
}

impl ManifestAppendAttempt {
    pub fn record(&self) -> &ManifestRecord {
        &self.record
    }

    pub fn encoded_record(&self) -> &[u8] {
        &self.encoded_record
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn pre_append_offset(&self) -> u64 {
        self.pre_append_offset
    }

    pub fn commit(&self) -> io::Result<ManifestSnapshot> {
        self.coordinator.commit_attempt(self)
    }
}

impl Drop for ManifestAppendAttempt {
    fn drop(&mut self) {
        if let Ok(mut state) = self.coordinator.mutation.lock()
            && state.active_token == Some(self.token)
        {
            state.active_token = None;
        }
    }
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
    write_current_with(manifest_dir.as_ref(), manifest_file_name, false)
}

pub(crate) fn write_current_with(
    manifest_dir: &Path,
    manifest_file_name: &str,
    fail_directory_sync: bool,
) -> io::Result<()> {
    validate_manifest_file_name(manifest_file_name)?;
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
    #[cfg(test)]
    if fail_directory_sync {
        return Err(Error::other("injected CURRENT directory sync failure"));
    }
    #[cfg(not(test))]
    let _ = fail_directory_sync;
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

/// Reads a complete current manifest and binds the decoded records to the
/// exact validated bytes. A partial record is an error; it is never treated as
/// a shorter authoritative prefix by this general reader.
pub fn read_manifest_snapshot(manifest_dir: impl AsRef<Path>) -> io::Result<ManifestSnapshot> {
    let manifest_dir = manifest_dir.as_ref();
    let Some(file_name) = read_current(manifest_dir)? else {
        validate_proven_absent_manifest(manifest_dir)?;
        return Ok(ManifestSnapshot::absent());
    };
    let bytes = fs::read(manifest_dir.join(&file_name))?;
    manifest_snapshot_from_bytes(file_name, &bytes)
}

fn validate_proven_absent_manifest(manifest_dir: &Path) -> io::Result<()> {
    let entries = match fs::read_dir(manifest_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == CURRENT_FILE_NAME
            || name == CURRENT_TEMP_FILE_NAME
            || name.starts_with("MANIFEST-")
        {
            return Err(invalid_data(
                "manifest CURRENT is absent despite existing publication evidence",
            ));
        }
    }
    Ok(())
}

/// Refreshes a manifest snapshot while proving that an unchanged manifest
/// identity still has the exact previously validated prefix.
///
/// Manifest rotation is parsed as a complete new identity. The caller that
/// coordinates rotation is responsible for validating its logical
/// predecessor relationship before publishing the returned inventory.
pub fn refresh_manifest_snapshot(
    manifest_dir: impl AsRef<Path>,
    previous: &ManifestSnapshot,
) -> io::Result<ManifestSnapshot> {
    let manifest_dir = manifest_dir.as_ref();
    let current = read_current(manifest_dir)?;
    match (&previous.cut, current) {
        (ManifestCut::Absent, None) => {
            validate_proven_absent_manifest(manifest_dir)?;
            Ok(previous.clone())
        }
        (ManifestCut::Present { .. }, None) => Err(invalid_data(
            "manifest CURRENT disappeared after a published manifest cut",
        )),
        (_, Some(file_name)) => {
            let bytes = fs::read(manifest_dir.join(&file_name))?;
            if let ManifestCut::Present {
                file_name: previous_file_name,
                validated_offset,
                prefix_sha256,
            } = &previous.cut
                && previous_file_name == &file_name
            {
                let offset = usize::try_from(*validated_offset).map_err(|_| {
                    invalid_data("previous manifest cut offset exceeds address space")
                })?;
                let prefix = bytes.get(..offset).ok_or_else(|| {
                    invalid_data("manifest became shorter than its validated prefix")
                })?;
                if sha256(prefix) != *prefix_sha256 {
                    return Err(invalid_data("manifest validated prefix changed"));
                }
            }
            manifest_snapshot_from_bytes(file_name, &bytes)
        }
    }
}

pub fn read_manifest_records(path: impl AsRef<Path>) -> io::Result<Vec<ManifestRecord>> {
    let mut file = File::open(path)?;
    let mut records = Vec::new();
    while let Some(record) = read_manifest_record(&mut file)? {
        records.push(record);
    }
    Ok(records)
}

fn manifest_snapshot_from_bytes(file_name: String, bytes: &[u8]) -> io::Result<ManifestSnapshot> {
    validate_manifest_file_name(&file_name)?;
    let mut reader = Cursor::new(bytes);
    let mut records = Vec::new();
    while let Some(record) = read_manifest_record(&mut reader)? {
        records.push(record);
    }
    let validated_offset = reader.position();
    if validated_offset != bytes.len() as u64 {
        return Err(invalid_data(
            "manifest parser stopped before the validated byte boundary",
        ));
    }
    let inventory = ManifestInventory::from_records(records.clone())?;
    Ok(ManifestSnapshot {
        cut: ManifestCut::Present {
            file_name,
            validated_offset,
            prefix_sha256: sha256(bytes),
        },
        records,
        inventory,
    })
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
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
    fn encoded_manifest_record_is_exactly_the_writer_output() {
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let encoded = encode_manifest_record(&record).unwrap();
        let mut written = Vec::new();

        write_manifest_record(&mut written, &record).unwrap();

        assert_eq!(encoded, written);
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

    #[test]
    fn snapshot_distinguishes_absent_manifest_from_empty_published_manifest() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");

        let absent = read_manifest_snapshot(&manifest_dir).unwrap();
        assert_eq!(absent, ManifestSnapshot::absent());

        let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();
        writer.sync_all().unwrap();
        write_current(&manifest_dir, writer.file_name()).unwrap();

        let published = read_manifest_snapshot(&manifest_dir).unwrap();
        assert!(matches!(
            published.cut,
            ManifestCut::Present {
                validated_offset: 0,
                ..
            }
        ));
        assert!(published.records.is_empty());
        assert!(published.inventory.segments.is_empty());
    }

    #[test]
    fn snapshot_and_absent_refresh_reject_publication_evidence_without_current() {
        for evidence_name in [manifest_file_name(1), CURRENT_TEMP_FILE_NAME.to_string()] {
            let tempdir = tempfile::tempdir().unwrap();
            let manifest_dir = tempdir.path().join("manifest");
            fs::create_dir_all(&manifest_dir).unwrap();
            fs::write(manifest_dir.join(evidence_name), b"orphaned publication").unwrap();

            let read_error = read_manifest_snapshot(&manifest_dir).unwrap_err();
            assert_eq!(read_error.kind(), ErrorKind::InvalidData);
            assert!(
                read_error.to_string().contains("publication evidence"),
                "{read_error}"
            );

            let refresh_error =
                refresh_manifest_snapshot(&manifest_dir, &ManifestSnapshot::absent()).unwrap_err();
            assert_eq!(refresh_error.kind(), ErrorKind::InvalidData);
            assert!(
                refresh_error.to_string().contains("publication evidence"),
                "{refresh_error}"
            );
        }
    }

    #[test]
    fn incremental_snapshot_verifies_prefix_and_applies_complete_suffix() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let first_id = SegmentId::new(1_000, 2_000).unwrap();
        let second_id = SegmentId::new(2_000, 3_000).unwrap();
        let first = ManifestRecord::SegmentSealed(
            ManifestSegment::new(first_id.dir_name(), 1_000, 2_000, None).unwrap(),
        );
        let second = ManifestRecord::SegmentSealed(
            ManifestSegment::new(second_id.dir_name(), 2_000, 3_000, None).unwrap(),
        );
        let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();
        writer.append(&first).unwrap();
        writer.sync_all().unwrap();
        write_current(&manifest_dir, writer.file_name()).unwrap();
        let initial = read_manifest_snapshot(&manifest_dir).unwrap();

        writer.append(&second).unwrap();
        writer.sync_all().unwrap();
        let refreshed = refresh_manifest_snapshot(&manifest_dir, &initial).unwrap();

        assert_eq!(refreshed.records, vec![first, second]);
        assert_eq!(refreshed.inventory.segments.len(), 2);
        assert!(refreshed.cut.validated_offset() > initial.cut.validated_offset());
    }

    #[test]
    fn incremental_snapshot_rejects_changed_or_shortened_validated_prefix() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();
        writer.append(&record).unwrap();
        writer.sync_all().unwrap();
        write_current(&manifest_dir, writer.file_name()).unwrap();
        let initial = read_manifest_snapshot(&manifest_dir).unwrap();
        let manifest_path = writer.path().to_path_buf();

        let mut changed = fs::read(&manifest_path).unwrap();
        changed[MANIFEST_RECORD_HEADER_LEN] ^= 1;
        fs::write(&manifest_path, changed).unwrap();
        let changed_error = refresh_manifest_snapshot(&manifest_dir, &initial).unwrap_err();
        assert_eq!(changed_error.kind(), ErrorKind::InvalidData);
        assert!(changed_error.to_string().contains("prefix changed"));

        fs::write(
            &manifest_path,
            &encode_manifest_record(&record).unwrap()[..MANIFEST_RECORD_HEADER_LEN],
        )
        .unwrap();
        let short_error = refresh_manifest_snapshot(&manifest_dir, &initial).unwrap_err();
        assert_eq!(short_error.kind(), ErrorKind::InvalidData);
        assert!(short_error.to_string().contains("became shorter"));
    }

    #[test]
    fn snapshot_rejects_partial_tail_instead_of_silently_truncating_cut() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let mut writer = ManifestWriter::create(&manifest_dir, 1).unwrap();
        writer.append(&record).unwrap();
        writer.sync_all().unwrap();
        write_current(&manifest_dir, writer.file_name()).unwrap();
        let initial = read_manifest_snapshot(&manifest_dir).unwrap();

        let encoded = encode_manifest_record(&record).unwrap();
        let mut append = OpenOptions::new().append(true).open(writer.path()).unwrap();
        append.write_all(&encoded[..encoded.len() - 1]).unwrap();
        append.sync_all().unwrap();

        let error = refresh_manifest_snapshot(&manifest_dir, &initial).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn coordinator_is_shared_per_canonical_manifest_directory() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");

        let first = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let second = ManifestCoordinator::shared(&manifest_dir).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn coordinator_commits_one_exact_version_one_record() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let attempt = coordinator.prepare_append(record.clone()).unwrap();

        let snapshot = attempt.commit().unwrap();

        assert_eq!(snapshot.records, vec![record]);
        assert_eq!(
            fs::read(manifest_dir.join(manifest_file_name(1))).unwrap(),
            attempt.encoded_record()
        );
    }

    #[test]
    fn coordinator_repairs_only_an_exact_incomplete_intended_tail() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let first_id = SegmentId::new(1_000, 2_000).unwrap();
        let second_id = SegmentId::new(2_000, 3_000).unwrap();
        let first = ManifestRecord::SegmentDeleted {
            segment_id: first_id.dir_name(),
        };
        let second = ManifestRecord::SegmentDeleted {
            segment_id: second_id.dir_name(),
        };
        coordinator
            .prepare_append(first.clone())
            .unwrap()
            .commit()
            .unwrap();
        let attempt = coordinator.prepare_append(second.clone()).unwrap();
        let path = manifest_dir.join(attempt.file_name());
        let prefix_len = attempt.encoded_record().len() / 2;
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&attempt.encoded_record()[..prefix_len])
            .unwrap();
        file.sync_all().unwrap();

        let snapshot = attempt.commit().unwrap();

        assert_eq!(snapshot.records, vec![first, second]);
        assert_eq!(
            snapshot.cut.validated_offset(),
            attempt
                .pre_append_offset()
                .checked_add(attempt.encoded_record().len() as u64)
                .unwrap()
        );
    }

    #[test]
    fn coordinator_syncs_an_already_complete_exact_tail_before_current() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let attempt = coordinator.prepare_append(record.clone()).unwrap();
        let path = manifest_dir.join(attempt.file_name());
        fs::write(&path, attempt.encoded_record()).unwrap();
        let syncs_before = coordinator.completed_manifest_sync_count();

        let snapshot = attempt.commit().unwrap();

        assert_eq!(
            coordinator.completed_manifest_sync_count(),
            syncs_before + 1,
            "an exact tail may follow a failed fsync and needs a fresh durability barrier"
        );
        assert_eq!(snapshot.records, vec![record]);
        assert_eq!(
            fs::read(&path).unwrap(),
            attempt.encoded_record(),
            "reconciliation must not append the retained record twice"
        );
    }

    #[test]
    fn coordinator_keeps_exact_tail_retryable_after_manifest_sync_failure() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let attempt = coordinator.prepare_append(record.clone()).unwrap();
        let path = manifest_dir.join(attempt.file_name());
        fs::write(&path, attempt.encoded_record()).unwrap();
        coordinator.fail_next_completed_manifest_sync();

        let error = attempt.commit().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected completed-manifest sync failure"),
            "{error}"
        );
        assert_eq!(read_current(&manifest_dir).unwrap(), None);
        assert_eq!(
            fs::read(&path).unwrap(),
            attempt.encoded_record(),
            "a failed sync must leave the exact retained bytes retryable"
        );

        let snapshot = attempt.commit().unwrap();
        assert_eq!(snapshot.records, vec![record]);
        assert_eq!(
            coordinator.completed_manifest_sync_count(),
            2,
            "the failed barrier and successful retry must both be attempted"
        );
    }

    #[test]
    fn coordinator_retries_exact_tail_after_current_rewrite_failure() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let attempt = coordinator.prepare_append(record.clone()).unwrap();
        let path = manifest_dir.join(attempt.file_name());
        let current_path = manifest_dir.join(CURRENT_FILE_NAME);
        fs::create_dir(&current_path).unwrap();

        attempt
            .commit()
            .expect_err("a directory must obstruct the atomic CURRENT rename");
        assert_eq!(fs::read(&path).unwrap(), attempt.encoded_record());
        assert!(manifest_dir.join(CURRENT_TEMP_FILE_NAME).is_file());

        fs::remove_dir(&current_path).unwrap();
        let syncs_before_retry = coordinator.completed_manifest_sync_count();
        let snapshot = attempt.commit().unwrap();

        assert_eq!(
            coordinator.completed_manifest_sync_count(),
            syncs_before_retry + 1,
            "retry must re-sync the exact manifest tail before publishing CURRENT"
        );
        assert_eq!(snapshot.records, vec![record]);
        assert_eq!(
            read_current(&manifest_dir).unwrap().as_deref(),
            Some(attempt.file_name())
        );
        assert!(!manifest_dir.join(CURRENT_TEMP_FILE_NAME).exists());
        assert_eq!(
            fs::read(&path).unwrap(),
            attempt.encoded_record(),
            "CURRENT retry must leave exactly one manifest record"
        );
    }

    #[test]
    fn coordinator_retries_exact_tail_after_current_directory_sync_failure() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let attempt = coordinator.prepare_append(record.clone()).unwrap();
        coordinator.fail_next_current_directory_sync();

        let error = attempt
            .commit()
            .expect_err("the injected post-rename directory sync must be ambiguous");
        assert!(error.to_string().contains("directory sync failure"));
        assert_eq!(
            read_current(&manifest_dir).unwrap().as_deref(),
            Some(attempt.file_name()),
            "CURRENT may already name the intended manifest after an ambiguous sync failure"
        );
        assert_eq!(
            fs::read(manifest_dir.join(attempt.file_name())).unwrap(),
            attempt.encoded_record()
        );

        let snapshot = attempt.commit().unwrap();
        assert_eq!(snapshot.records, vec![record]);
        assert_eq!(
            fs::read(manifest_dir.join(attempt.file_name())).unwrap(),
            attempt.encoded_record(),
            "retry must authenticate and retain exactly one intended record"
        );
        assert!(!manifest_dir.join(CURRENT_TEMP_FILE_NAME).exists());
    }

    #[test]
    fn coordinator_authenticates_the_entire_preappend_prefix_on_every_retry() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let first_id = SegmentId::new(1_000, 2_000).unwrap();
        let second_id = SegmentId::new(2_000, 3_000).unwrap();
        let first = ManifestRecord::SegmentDeleted {
            segment_id: first_id.dir_name(),
        };
        let second = ManifestRecord::SegmentDeleted {
            segment_id: second_id.dir_name(),
        };
        coordinator
            .prepare_append(first.clone())
            .unwrap()
            .commit()
            .unwrap();
        let attempt = coordinator.prepare_append(second.clone()).unwrap();
        let path = manifest_dir.join(attempt.file_name());
        let original_prefix = fs::read(&path).unwrap();
        assert_eq!(attempt.pre_append_offset(), original_prefix.len() as u64);

        let mut changed = original_prefix.clone();
        changed[0] ^= 0xff;
        changed.extend_from_slice(attempt.encoded_record());
        fs::write(&path, changed).unwrap();
        let error = attempt.commit().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("prefix changed"), "{error}");

        let mut repaired = original_prefix;
        repaired.extend_from_slice(attempt.encoded_record());
        fs::write(&path, repaired).unwrap();
        let snapshot = attempt.commit().unwrap();

        assert_eq!(snapshot.records, vec![first, second]);
        assert_eq!(
            snapshot.cut.validated_offset(),
            attempt.pre_append_offset() + attempt.encoded_record().len() as u64
        );
    }

    #[test]
    fn coordinator_rejects_foreign_tail_and_allows_retry_after_repair() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let segment_id = SegmentId::new(1_000, 2_000).unwrap();
        let record = ManifestRecord::SegmentDeleted {
            segment_id: segment_id.dir_name(),
        };
        let attempt = coordinator.prepare_append(record.clone()).unwrap();
        let path = manifest_dir.join(attempt.file_name());
        fs::write(&path, b"foreign").unwrap();

        let error = attempt.commit().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("neither empty"));

        fs::write(&path, []).unwrap();
        let snapshot = attempt.commit().unwrap();
        assert_eq!(snapshot.records, vec![record]);
    }

    #[test]
    fn coordinator_keeps_one_attempt_active_until_commit_or_drop() {
        let tempdir = tempfile::tempdir().unwrap();
        let manifest_dir = tempdir.path().join("manifest");
        let coordinator = ManifestCoordinator::shared(&manifest_dir).unwrap();
        let first_id = SegmentId::new(1_000, 2_000).unwrap();
        let second_id = SegmentId::new(2_000, 3_000).unwrap();
        let attempt = coordinator
            .prepare_append(ManifestRecord::SegmentDeleted {
                segment_id: first_id.dir_name(),
            })
            .unwrap();

        let blocked = coordinator
            .prepare_append(ManifestRecord::SegmentDeleted {
                segment_id: second_id.dir_name(),
            })
            .unwrap_err();
        assert_eq!(blocked.kind(), ErrorKind::WouldBlock);

        drop(attempt);
        coordinator
            .prepare_append(ManifestRecord::SegmentDeleted {
                segment_id: second_id.dir_name(),
            })
            .unwrap()
            .commit()
            .unwrap();
    }
}
