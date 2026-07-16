//! Registered, generation-bound complete-file checksum preflight.
//!
//! This is deliberately an intermediate validation layer. Production query
//! opens use it for registered footer checksums, but it does not mint schema-7/8
//! routing or metric-range authority. Strict full metadata walkers consume
//! this proof in a later change.

use std::io::{self, Read};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::storage::metadata_cache::{MetadataCacheError, StructuralMetadataErrorKind};
use crate::storage::metadata_governor::{
    MetadataBudgetError, MetadataCacheClass, MetadataUsageClass,
};
use crate::storage::metadata_runtime::{
    RegisteredSegment, SegmentArtifactRegistration, SegmentGenerationProvenance,
    StoreMetadataRuntime, StoreMetadataRuntimeError,
};

use super::{
    SEGMENT_FOOTER_HASH_BUFFER_BYTES, SEGMENT_FOOTER_TRACKED_FILES, SEGMENT_SCHEMA_VERSION_V6,
    SEGMENT_SCHEMA_VERSION_V7, SEGMENT_SCHEMA_VERSION_V8, SegmentFile, SegmentFooter, SegmentId,
    SegmentMeta, invalid_segment_data, read_segment_footer_for_exact_schema,
};

const FULL_VALIDATION_HASH_BUFFER_BYTES: usize = SEGMENT_FOOTER_HASH_BUFFER_BYTES;
pub(super) const SEGMENT_META_MAX_BYTES: u64 = 64 * 1024;

/// Exact schema policy for one registered validation attempt.
///
/// There is intentionally no "allow either" variant. Store-level callers
/// select one homogeneous contract before opening any segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegisteredSegmentValidationPolicy {
    Schema7,
    Schema8,
    ValidatedSchema6,
}

impl RegisteredSegmentValidationPolicy {
    const fn expected_schema_version(self) -> u16 {
        match self {
            Self::Schema7 => SEGMENT_SCHEMA_VERSION_V7,
            Self::Schema8 => SEGMENT_SCHEMA_VERSION_V8,
            Self::ValidatedSchema6 => SEGMENT_SCHEMA_VERSION_V6,
        }
    }
}

#[derive(Debug, Error)]
pub(super) enum RegisteredSegmentValidationError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Cache(#[from] MetadataCacheError),
    #[error(transparent)]
    Budget(#[from] MetadataBudgetError),
}

/// Canonical footer inventory after all seven paths have been registered.
///
/// No tracked artifact bytes have been parsed at this point. The registered
/// owner captures the exact regular-file identities and footer lengths which
/// every later read must continue to use.
pub(super) struct RegisteredSegmentPreflight {
    dir: PathBuf,
    segment_id: SegmentId,
    footer: SegmentFooter,
    policy: RegisteredSegmentValidationPolicy,
    registered: RegisteredSegment,
}

/// Complete-file checksum proof for one still-owned registered generation.
///
/// This intermediate remains private and intentionally carries no routing,
/// metric-range, or production-open authority. It owns no read guard,
/// descriptor lease, metadata pin, or scratch charge.
pub(super) struct FooterChecksummedSegment {
    registered: RegisteredSegment,
    provenance: SegmentGenerationProvenance,
    segment_id: SegmentId,
    footer: SegmentFooter,
    meta: SegmentMeta,
    policy: RegisteredSegmentValidationPolicy,
}

impl RegisteredSegmentPreflight {
    /// Reads and validates `meta.json` from the registered immutable
    /// generation without hashing the other tracked artifacts. This is the
    /// lightweight schema-7/8 open path; malformed metadata becomes sticky while
    /// transient read and resource failures remain retryable.
    pub(super) fn read_registered_meta(
        self,
    ) -> Result<(RegisteredSegment, SegmentFooter, SegmentMeta), RegisteredSegmentValidationError>
    {
        let guard = self.registered.read_guard()?;
        let meta_reader = guard.reader(SegmentFile::MetaJson)?;
        meta_reader.check_recorded_error()?;

        let scratch_len = usize::try_from(meta_reader.len().max(1)).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "registered meta.json length exceeds platform usize",
            )
        })?;
        let declared_scratch = u64::try_from(scratch_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "registered meta.json scratch length exceeds u64",
            )
        })?;
        let governor = meta_reader.runtime().governor();
        let mut scratch_charge =
            governor.reserve_in_flight_for_usage(declared_scratch, MetadataUsageClass::Scratch)?;
        let mut scratch = Vec::new();
        scratch.try_reserve_exact(scratch_len).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to allocate registered meta.json scratch: {error}"),
            )
        })?;
        scratch_charge.reconcile(u64::try_from(scratch.capacity()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "registered meta.json scratch capacity exceeds u64",
            )
        })?)?;
        scratch.resize(scratch_len, 0);

        let meta_result =
            parse_registered_meta(&meta_reader, &mut scratch, None).and_then(|meta| {
                validate_meta_identity(&self.dir, self.segment_id, &meta)?;
                Ok(meta)
            });
        let meta = match meta_result {
            Ok(meta) => meta,
            Err(error) => {
                drop(scratch);
                drop(scratch_charge);
                return Err(meta_reader.record_validation_error(error).into());
            }
        };

        drop(scratch);
        drop(scratch_charge);
        drop(guard);
        Ok((self.registered, self.footer, meta))
    }

    /// Hashes every registered artifact in canonical footer order using one
    /// reusable, aggregate-governed 1 MiB buffer. The same buffer then backs a
    /// streaming registered read of `meta.json`; no whole-file metadata copy is
    /// allocated.
    pub(super) fn validate_footer_checksums(
        self,
    ) -> Result<FooterChecksummedSegment, RegisteredSegmentValidationError> {
        let guard = self.registered.read_guard()?;

        // A known structural error wins before scratch reservation, descriptor
        // acquisition, or content I/O on a retry.
        for file in SEGMENT_FOOTER_TRACKED_FILES {
            guard.reader(file)?.check_recorded_error()?;
        }

        let governor = guard.reader(SegmentFile::MetaJson)?.runtime().governor();
        let declared_scratch = u64::try_from(FULL_VALIDATION_HASH_BUFFER_BYTES).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "full-validation hash buffer length exceeds u64",
            )
        })?;
        let mut scratch_charge =
            governor.reserve_in_flight_for_usage(declared_scratch, MetadataUsageClass::Scratch)?;
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(FULL_VALIDATION_HASH_BUFFER_BYTES)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("failed to allocate full-validation hash buffer: {error}"),
                )
            })?;
        scratch_charge.reconcile(u64::try_from(scratch.capacity()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "full-validation hash buffer capacity exceeds u64",
            )
        })?)?;
        scratch.resize(FULL_VALIDATION_HASH_BUFFER_BYTES, 0);

        for expected in &self.footer.files {
            let reader = guard.reader(expected.file)?;
            let actual = reader.hash_registered_xxh64(&mut scratch)?;
            if actual != expected.checksum_xxh64 {
                // Structural recording may take the cache ledger lock. Destroy
                // all allocation accounting first; the hash method has already
                // released its one descriptor lease.
                drop(scratch);
                drop(scratch_charge);
                return Err(reader
                    .record_validation_error(invalid_segment_data(
                        "segment footer file checksum mismatch",
                    ))
                    .into());
            }
        }

        let meta_reader = guard.reader(SegmentFile::MetaJson)?;
        let meta_result = parse_registered_meta(
            &meta_reader,
            &mut scratch,
            Some(MetadataCacheClass::FullValidation),
        )
        .and_then(|meta| {
            validate_meta_identity(&self.dir, self.segment_id, &meta)?;
            Ok(meta)
        });
        let meta = match meta_result {
            Ok(meta) => meta,
            Err(error) => {
                drop(scratch);
                drop(scratch_charge);
                return Err(meta_reader.record_validation_error(error).into());
            }
        };

        drop(scratch);
        drop(scratch_charge);
        let provenance = guard.provenance();
        drop(guard);

        Ok(FooterChecksummedSegment {
            registered: self.registered,
            provenance,
            segment_id: self.segment_id,
            footer: self.footer,
            meta,
            policy: self.policy,
        })
    }

    #[cfg(test)]
    fn registered(&self) -> &RegisteredSegment {
        &self.registered
    }
}

impl FooterChecksummedSegment {
    #[cfg(test)]
    fn matches_registered_generation(&self) -> bool {
        self.registered
            .read_guard()
            .is_ok_and(|guard| self.provenance.matches(&guard))
    }

    pub(super) fn into_open_parts(self) -> (RegisteredSegment, SegmentFooter, SegmentMeta) {
        debug_assert_eq!(
            self.footer.schema_version,
            self.policy.expected_schema_version(),
            "footer checksum proof must retain its exact schema policy"
        );
        debug_assert_eq!(self.meta.start_ms, self.segment_id.start_ms());
        debug_assert_eq!(self.meta.end_ms, self.segment_id.end_ms());
        debug_assert!(
            self.registered
                .read_guard()
                .is_ok_and(|guard| self.provenance.matches(&guard)),
            "footer checksum proof must retain its registered generation"
        );
        (self.registered, self.footer, self.meta)
    }
}

pub(super) fn registered_validation_error_to_io(
    error: RegisteredSegmentValidationError,
) -> io::Error {
    match error {
        RegisteredSegmentValidationError::Io(error) => error,
        RegisteredSegmentValidationError::Runtime(error) => {
            let kind = match &error {
                StoreMetadataRuntimeError::FileManager(error) if error.is_structural() => {
                    io::ErrorKind::InvalidData
                }
                _ => io::ErrorKind::Other,
            };
            io::Error::new(kind, error)
        }
        RegisteredSegmentValidationError::Cache(error) => metadata_cache_error_to_io(error),
        RegisteredSegmentValidationError::Budget(error) => {
            io::Error::new(io::ErrorKind::OutOfMemory, error)
        }
    }
}

/// Reads the bounded canonical footer, validates the exact selected schema and
/// segment-directory identity, then registers every tracked path before any
/// tracked content is parsed.
pub(super) fn preflight_registered_segment(
    runtime: &StoreMetadataRuntime,
    segment_dir: impl AsRef<Path>,
    policy: RegisteredSegmentValidationPolicy,
) -> Result<RegisteredSegmentPreflight, RegisteredSegmentValidationError> {
    let dir = segment_dir.as_ref().to_path_buf();
    let footer = read_segment_footer_for_exact_schema(&dir, policy.expected_schema_version())?;
    let meta_len = footer
        .files
        .iter()
        .find_map(|entry| (entry.file == SegmentFile::MetaJson).then_some(entry.size))
        .ok_or_else(|| invalid_segment_data("segment footer omits meta.json"))?;
    if meta_len > SEGMENT_META_MAX_BYTES {
        return Err(invalid_segment_data("segment meta.json exceeds the operational limit").into());
    }
    let (segment_identity, segment_id) = parse_canonical_segment_identity(&dir)?;
    let artifacts = footer
        .files
        .iter()
        .map(|entry| {
            SegmentArtifactRegistration::new(
                entry.file,
                dir.join(entry.file.filename()),
                entry.size,
            )
        })
        .collect::<Vec<_>>();
    let registered = runtime.register_segment(segment_identity, &artifacts)?;

    Ok(RegisteredSegmentPreflight {
        dir,
        segment_id,
        footer,
        policy,
        registered,
    })
}

fn parse_canonical_segment_identity(segment_dir: &Path) -> io::Result<(String, SegmentId)> {
    let identity = segment_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_segment_data("segment directory name is not valid UTF-8"))?;
    let parsed = SegmentId::parse_dir_name(identity).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid segment directory identity: {error}"),
        )
    })?;
    if parsed.dir_name() != identity {
        return Err(invalid_segment_data(
            "segment directory identity is not canonical",
        ));
    }
    Ok((identity.to_owned(), parsed))
}

fn parse_registered_meta(
    reader: &crate::storage::metadata_runtime::GovernedArtifactReader,
    scratch: &mut [u8],
    class: Option<MetadataCacheClass>,
) -> io::Result<SegmentMeta> {
    let mut buffered = GovernedBufferedArtifact::new(reader, scratch, class)?;
    serde_json::from_reader(&mut buffered).map_err(registered_meta_decode_error)
}

fn registered_meta_decode_error(error: serde_json::Error) -> io::Error {
    let kind = error.io_error_kind().unwrap_or(io::ErrorKind::InvalidData);
    io::Error::new(
        kind,
        format!("invalid registered segment meta.json: {error}"),
    )
}

fn validate_meta_identity(
    segment_dir: &Path,
    segment_id: SegmentId,
    meta: &SegmentMeta,
) -> io::Result<()> {
    let expected_identity = segment_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_segment_data("segment directory name is not valid UTF-8"))?;
    if meta.segment_id != expected_identity
        || meta.start_ms != segment_id.start_ms()
        || meta.end_ms != segment_id.end_ms()
    {
        return Err(invalid_segment_data(
            "registered segment meta.json does not match its directory identity",
        ));
    }
    Ok(())
}

/// Streaming `Read` adapter backed by the already charged hash buffer.
///
/// Refills use positional reads against the registered identity and retain no
/// descriptor between calls. The source length is captured by registration,
/// so the adapter never accepts EOF as an implicit truncation.
struct GovernedBufferedArtifact<'a> {
    reader: &'a crate::storage::metadata_runtime::GovernedArtifactReader,
    buffer: &'a mut [u8],
    class: Option<MetadataCacheClass>,
    buffered_start: usize,
    buffered_end: usize,
    next_offset: u64,
}

impl<'a> GovernedBufferedArtifact<'a> {
    fn new(
        reader: &'a crate::storage::metadata_runtime::GovernedArtifactReader,
        buffer: &'a mut [u8],
        class: Option<MetadataCacheClass>,
    ) -> io::Result<Self> {
        if buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered artifact stream buffer must not be empty",
            ));
        }
        Ok(Self {
            reader,
            buffer,
            class,
            buffered_start: 0,
            buffered_end: 0,
            next_offset: 0,
        })
    }

    fn refill(&mut self) -> io::Result<bool> {
        if self.next_offset == self.reader.len() {
            return Ok(false);
        }
        let remaining = self
            .reader
            .len()
            .checked_sub(self.next_offset)
            .ok_or_else(|| {
                invalid_segment_data("registered artifact stream offset exceeds file length")
            })?;
        let read_len_u64 = remaining.min(u64::try_from(self.buffer.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered artifact stream buffer length exceeds u64",
            )
        })?);
        let read_len = usize::try_from(read_len_u64).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered artifact stream read length exceeds usize",
            )
        })?;
        match self.class {
            Some(class) => self.reader.read_exact_at_for_class(
                self.next_offset,
                &mut self.buffer[..read_len],
                class,
            ),
            None => self
                .reader
                .read_exact_at(self.next_offset, &mut self.buffer[..read_len]),
        }
        .map_err(metadata_cache_error_to_io)?;
        self.next_offset = self
            .next_offset
            .checked_add(read_len_u64)
            .ok_or_else(|| invalid_segment_data("registered artifact stream offset overflows"))?;
        self.buffered_start = 0;
        self.buffered_end = read_len;
        Ok(true)
    }
}

impl Read for GovernedBufferedArtifact<'_> {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        if destination.is_empty() {
            return Ok(0);
        }
        if self.buffered_start == self.buffered_end && !self.refill()? {
            return Ok(0);
        }
        let available = &self.buffer[self.buffered_start..self.buffered_end];
        let copied = available.len().min(destination.len());
        destination[..copied].copy_from_slice(&available[..copied]);
        self.buffered_start += copied;
        Ok(copied)
    }
}

fn metadata_cache_error_to_io(error: MetadataCacheError) -> io::Error {
    let kind = match &error {
        MetadataCacheError::Structural(corruption) => match corruption.kind {
            StructuralMetadataErrorKind::InvalidData => io::ErrorKind::InvalidData,
            StructuralMetadataErrorKind::UnexpectedEof => io::ErrorKind::UnexpectedEof,
        },
        MetadataCacheError::Transient { kind, .. } => *kind,
        MetadataCacheError::Budget(_) => io::ErrorKind::OutOfMemory,
        MetadataCacheError::DeclaredBoundExceeded { .. }
        | MetadataCacheError::TypeMismatch
        | MetadataCacheError::UnregisteredArtifact { .. }
        | MetadataCacheError::RetiringArtifact { .. } => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

#[cfg(test)]
mod tests;
