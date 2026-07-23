//! Shared runtime for byte, cache, and descriptor-governed metadata reads.

use std::io;
#[cfg(test)]
use std::sync::Arc;
use std::sync::Mutex;

use super::file_manager::{GovernedFileLease, MetadataFileManagerError};
use super::metadata_cache::{
    ArtifactKey, LoadedMetadata, MetadataCacheError, MetadataCacheKey, MetadataCacheKeyError,
    MetadataCachePin, StructuralMetadataErrorKind,
};
use super::metadata_governor::{
    METADATA_CACHE_CLASS_COUNT, METADATA_CACHE_CLASS_ORDER, MetadataCacheClass, MetadataUsageClass,
};
use super::segment::{SEGMENT_FOOTER_TRACKED_FILES, SegmentFile};
use crate::util::XxHash64;

mod lifecycle;

pub(crate) use lifecycle::SegmentGenerationProvenance;
pub use lifecycle::{
    RegisteredSegment, SegmentArtifactRegistration, SegmentReadGuard, StoreMetadataRuntime,
    StoreMetadataRuntimeError, StoreMetadataRuntimeSnapshot,
};

pub const METADATA_READ_FILE_COUNT: usize = SEGMENT_FOOTER_TRACKED_FILES.len();
pub const METADATA_READ_FILE_ORDER: [SegmentFile; METADATA_READ_FILE_COUNT] =
    SEGMENT_FOOTER_TRACKED_FILES;

/// Exact process-issued metadata read totals for one stable dimension value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataIssuedReadCount {
    pub calls: u64,
    pub bytes: u64,
}

/// Exact process-issued metadata reads attributed to one tracked segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataFileReadStats {
    pub file: SegmentFile,
    pub issued: MetadataIssuedReadCount,
}

/// Exact process-issued metadata reads attributed to one cache semantic class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataClassReadStats {
    pub class: MetadataCacheClass,
    pub issued: MetadataIssuedReadCount,
}

/// Store-wide exact metadata read counters.
///
/// A call is recorded only after a descriptor lease has been acquired and
/// immediately before one positional range request is issued. Cache hits add
/// no issued bytes. `files` and `classes` are independent projections of the
/// same issued requests; callers must not add them together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataReadStats {
    pub issued: MetadataIssuedReadCount,
    pub unclassified: MetadataIssuedReadCount,
    pub files: [MetadataFileReadStats; METADATA_READ_FILE_COUNT],
    pub classes: [MetadataClassReadStats; METADATA_CACHE_CLASS_COUNT],
}

impl Default for MetadataReadStats {
    fn default() -> Self {
        Self {
            issued: MetadataIssuedReadCount::default(),
            unclassified: MetadataIssuedReadCount::default(),
            files: METADATA_READ_FILE_ORDER.map(|file| MetadataFileReadStats {
                file,
                issued: MetadataIssuedReadCount::default(),
            }),
            classes: METADATA_CACHE_CLASS_ORDER.map(|class| MetadataClassReadStats {
                class,
                issued: MetadataIssuedReadCount::default(),
            }),
        }
    }
}

impl MetadataReadStats {
    /// Returns saturating counter deltas for one measured operation.
    pub fn delta_since(self, before: Self) -> Self {
        Self {
            issued: self.issued.delta_since(before.issued),
            unclassified: self.unclassified.delta_since(before.unclassified),
            files: std::array::from_fn(|index| MetadataFileReadStats {
                file: self.files[index].file,
                issued: self.files[index]
                    .issued
                    .delta_since(before.files[index].issued),
            }),
            classes: std::array::from_fn(|index| MetadataClassReadStats {
                class: self.classes[index].class,
                issued: self.classes[index]
                    .issued
                    .delta_since(before.classes[index].issued),
            }),
        }
    }
}

impl MetadataIssuedReadCount {
    fn record(&mut self, bytes: u64) {
        self.calls = self.calls.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn delta_since(self, before: Self) -> Self {
        Self {
            calls: self.calls.saturating_sub(before.calls),
            bytes: self.bytes.saturating_sub(before.bytes),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct MetadataReadCounters {
    state: Mutex<MetadataReadCounterState>,
}

#[derive(Debug, Default)]
struct MetadataReadCounterState {
    issued: MetadataIssuedReadCount,
    unclassified: MetadataIssuedReadCount,
    files: [MetadataIssuedReadCount; METADATA_READ_FILE_COUNT],
    classes: [MetadataIssuedReadCount; METADATA_CACHE_CLASS_COUNT],
    #[cfg(test)]
    spans: Vec<MetadataReadSpan>,
}

impl MetadataReadCounters {
    fn record(
        &self,
        file: SegmentFile,
        class: Option<MetadataCacheClass>,
        offset: u64,
        length: u64,
    ) {
        let file_index = METADATA_READ_FILE_ORDER
            .iter()
            .position(|candidate| *candidate == file)
            .expect("governed artifact reader uses a tracked segment file");
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.issued.record(length);
        state.files[file_index].record(length);
        if let Some(class) = class {
            state.classes[class.stable_index()].record(length);
        } else {
            state.unclassified.record(length);
        }
        #[cfg(test)]
        state.spans.push(MetadataReadSpan {
            file,
            class,
            offset,
            length,
        });
        #[cfg(not(test))]
        let _ = offset;
    }

    pub(super) fn snapshot(&self) -> MetadataReadStats {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        MetadataReadStats {
            issued: state.issued,
            unclassified: state.unclassified,
            files: std::array::from_fn(|index| MetadataFileReadStats {
                file: METADATA_READ_FILE_ORDER[index],
                issued: state.files[index],
            }),
            classes: std::array::from_fn(|index| MetadataClassReadStats {
                class: METADATA_CACHE_CLASS_ORDER[index],
                issued: state.classes[index],
            }),
        }
    }

    #[cfg(test)]
    fn take_spans(&self) -> Vec<MetadataReadSpan> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut state.spans)
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataReadSpan {
    file: SegmentFile,
    class: Option<MetadataCacheClass>,
    offset: u64,
    length: u64,
}

/// Stateless positional reader for one preflighted immutable artifact.
///
/// The reader owns no descriptor, decoded cache pin, or scratch allocation.
#[derive(Clone)]
pub struct GovernedArtifactReader {
    guard: SegmentReadGuard,
    file_index: usize,
    cache_artifact: ArtifactKey,
}

impl GovernedArtifactReader {
    fn from_guard(guard: SegmentReadGuard, file_index: usize) -> Self {
        let cache_artifact =
            ArtifactKey::new(guard.cache_identity(), guard.handle(file_index).file());
        Self {
            guard,
            file_index,
            cache_artifact,
        }
    }

    pub(crate) fn runtime(&self) -> StoreMetadataRuntime {
        self.guard.runtime()
    }

    fn handle(&self) -> &super::file_manager::SegmentFileHandle {
        self.guard.handle(self.file_index)
    }

    pub fn segment_identity(&self) -> &str {
        self.cache_artifact.segment_identity()
    }

    pub fn file(&self) -> crate::storage::segment::SegmentFile {
        self.handle().file()
    }

    pub fn len(&self) -> u64 {
        self.handle().expected_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Constructs a range key without allocating or rehashing the stable
    /// segment identity retained by this reader.
    pub(super) fn metadata_cache_key(
        &self,
        offset: u64,
        length: u64,
        class: MetadataCacheClass,
    ) -> Result<MetadataCacheKey, MetadataCacheKeyError> {
        MetadataCacheKey::with_artifact(self.cache_artifact.clone(), offset, length, class)
    }

    /// Acquires one all-or-none descriptor set for a scheduler submission.
    ///
    /// The returned leases follow `readers` order even though the file manager
    /// normalizes acquisition order internally. Callers must retain them until
    /// every asynchronous completion that references their descriptors has
    /// been consumed.
    pub(crate) fn acquire_file_leases(
        readers: &[Self],
    ) -> Result<Vec<GovernedFileLease>, MetadataCacheError> {
        let Some(first) = readers.first() else {
            return Ok(Vec::new());
        };
        let runtime = first.runtime();
        for reader in readers {
            if !std::sync::Arc::ptr_eq(&runtime.inner, &reader.runtime().inner) {
                return Err(MetadataCacheError::transient(
                    io::ErrorKind::InvalidInput,
                    "cannot acquire one descriptor set across metadata runtimes",
                ));
            }
            runtime
                .cache()
                .check_artifact_with_key(&reader.cache_artifact)?;
        }

        let handles = readers
            .iter()
            .map(|reader| reader.handle().clone())
            .collect::<Vec<_>>();
        let acquired = runtime
            .file_manager()
            .acquire_many(&handles)
            .map_err(|error| {
                let failure_reader = match &error {
                    MetadataFileManagerError::StructuralReplacement {
                        segment_identity,
                        file,
                        ..
                    } => readers
                        .iter()
                        .find(|reader| {
                            reader.segment_identity() == segment_identity.as_ref()
                                && reader.file() == *file
                        })
                        .unwrap_or(first),
                    _ => first,
                };
                failure_reader.finish_read_failure(ArtifactReadFailure::FileManager(error))
            })?
            .into_leases();

        readers
            .iter()
            .map(|reader| {
                acquired
                    .iter()
                    .find(|lease| {
                        lease.handle().segment_identity() == reader.segment_identity()
                            && lease.handle().file() == reader.file()
                    })
                    .cloned()
                    .ok_or_else(|| {
                        MetadataCacheError::transient(
                            io::ErrorKind::Other,
                            "governed descriptor acquisition omitted a requested artifact",
                        )
                    })
            })
            .collect()
    }

    pub(crate) fn file_lease_limit(&self) -> u32 {
        self.runtime().file_manager().max_open_files()
    }

    /// Records an externally scheduled positional-read failure after every
    /// request and descriptor lease for that submission has been released.
    pub(crate) fn record_scheduled_read_error(&self, error: io::Error) -> MetadataCacheError {
        self.finish_read_failure(ArtifactReadFailure::Io(error))
    }

    /// Reads one exact range after checking the artifact corruption ledger.
    ///
    /// Any descriptor lease is released before an I/O failure is converted or
    /// structural corruption is recorded.
    pub fn read_exact_at(
        &self,
        offset: u64,
        destination: &mut [u8],
    ) -> Result<(), MetadataCacheError> {
        self.read_exact_at_classified(offset, destination, None)
    }

    /// Streams this registered artifact through a seed-zero XXH64 hash.
    ///
    /// The caller owns and governs the non-empty scratch buffer. This method
    /// acquires exactly one descriptor lease for the captured registered file
    /// identity and retains it until every positional read is complete. It
    /// never reopens or accepts a path.
    pub(crate) fn hash_registered_xxh64(
        &self,
        scratch: &mut [u8],
    ) -> Result<u64, MetadataCacheError> {
        self.hash_registered_xxh64_impl(scratch, |_| {})
    }

    #[cfg(test)]
    fn hash_registered_xxh64_with_hook(
        &self,
        scratch: &mut [u8],
        after_read: impl FnMut(u64),
    ) -> Result<u64, MetadataCacheError> {
        self.hash_registered_xxh64_impl(scratch, after_read)
    }

    fn hash_registered_xxh64_impl(
        &self,
        scratch: &mut [u8],
        mut after_read: impl FnMut(u64),
    ) -> Result<u64, MetadataCacheError> {
        self.check_recorded_error()?;
        let scratch_len = u64::try_from(scratch.len()).map_err(|_| {
            MetadataCacheError::transient(
                io::ErrorKind::InvalidInput,
                "registered-artifact hash scratch length exceeds u64",
            )
        })?;
        if scratch_len == 0 {
            return Err(MetadataCacheError::transient(
                io::ErrorKind::InvalidInput,
                "registered-artifact hash scratch buffer must not be empty",
            ));
        }

        let runtime = self.runtime();
        let lease = match runtime.file_manager().acquire(self.handle()) {
            Ok(lease) => lease,
            Err(error) => {
                return Err(self.finish_read_failure(ArtifactReadFailure::FileManager(error)));
            }
        };
        let mut hash = XxHash64::default();
        let mut offset = 0_u64;
        let mut read_error = None;
        while offset < self.len() {
            let length_u64 = (self.len() - offset).min(scratch_len);
            let length = usize::try_from(length_u64).map_err(|_| {
                MetadataCacheError::transient(
                    io::ErrorKind::InvalidInput,
                    "registered-artifact hash read length exceeds usize",
                )
            })?;
            runtime.inner.reads.record(
                self.handle().file(),
                Some(MetadataCacheClass::FullValidation),
                offset,
                length_u64,
            );
            if let Err(error) = lease.read_exact_at(offset, &mut scratch[..length]) {
                read_error = Some(error);
                break;
            }
            hash.update(&scratch[..length]);
            offset += length_u64;
            after_read(offset);
        }
        let failure = read_error.map(ArtifactReadFailure::Io).or_else(|| {
            lease
                .verify_registered_shape()
                .err()
                .map(ArtifactReadFailure::FileManager)
        });
        drop(lease);

        match failure {
            Some(failure) => Err(self.finish_read_failure(failure)),
            None => Ok(hash.finish()),
        }
    }

    /// Reads one exact range while attributing the issued request to a stable
    /// metadata class. This is used for staged roots whose cache key cannot be
    /// constructed until a fixed prefix has been decoded.
    pub(super) fn read_exact_at_for_class(
        &self,
        offset: u64,
        destination: &mut [u8],
        class: MetadataCacheClass,
    ) -> Result<(), MetadataCacheError> {
        self.read_exact_at_classified(offset, destination, Some(class))
    }

    fn read_exact_at_classified(
        &self,
        offset: u64,
        destination: &mut [u8],
        class: Option<MetadataCacheClass>,
    ) -> Result<(), MetadataCacheError> {
        match self.read_exact_at_inner(offset, destination, class) {
            Ok(()) => Ok(()),
            Err(error) => Err(self.finish_read_failure(error)),
        }
    }

    /// Returns an existing validated value or reads, validates, and publishes
    /// one immutable metadata range.
    ///
    /// The final value reservation is owned by the aggregate metadata cache.
    /// The raw range uses a separate in-flight scratch reservation made before the
    /// allocation. The descriptor lease and scratch buffer are released before
    /// publication, while the reconciled scratch charge is carried into cache
    /// admission for one atomic governor-accounting handoff.
    ///
    /// The validated value must contain decoded immutable data only. It must not
    /// retain a [`GovernedArtifactReader`], [`SegmentReadGuard`], or
    /// [`RegisteredSegment`]: lifecycle ownership inside a resident value would
    /// create a cycle that prevents the segment and its cache inventory from
    /// retiring.
    pub(super) fn get_or_load<T, F>(
        &self,
        key: MetadataCacheKey,
        declared_max_bytes: u64,
        validate: F,
    ) -> Result<MetadataCachePin<T>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&[u8]) -> Result<LoadedMetadata<T>, MetadataCacheError>,
    {
        self.get_or_load_seeded_owned(key, declared_max_bytes, &[], move |scratch| {
            validate(&scratch)
        })
    }

    /// Returns a validated value which may take ownership of the governed raw
    /// range allocation. This lets page/blob cache values retain authenticated
    /// bytes without an unaccounted copy.
    pub(super) fn get_or_load_owned<T, F>(
        &self,
        key: MetadataCacheKey,
        declared_max_bytes: u64,
        validate: F,
    ) -> Result<MetadataCachePin<T>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
        F: FnOnce(Vec<u8>) -> Result<LoadedMetadata<T>, MetadataCacheError>,
    {
        self.get_or_load_seeded_owned(key, declared_max_bytes, &[], validate)
    }

    /// Loads a variable-size range after the caller has already read and
    /// validated its fixed prefix to discover the complete cache key.
    ///
    /// On a miss, the prefix is copied into the governed full-range scratch
    /// allocation and only the remaining suffix is issued to the file. On a
    /// hit, the loader is not invoked. The caller is responsible for recording
    /// a structural prefix-decode failure through [`Self::record_validation_error`].
    pub(super) fn get_or_load_with_prefix<T, F>(
        &self,
        key: MetadataCacheKey,
        declared_max_bytes: u64,
        prefix: &[u8],
        validate: F,
    ) -> Result<MetadataCachePin<T>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&[u8]) -> Result<LoadedMetadata<T>, MetadataCacheError>,
    {
        if prefix.is_empty() {
            return Err(MetadataCacheError::transient(
                io::ErrorKind::InvalidInput,
                "staged metadata prefix must not be empty",
            ));
        }
        self.get_or_load_seeded_owned(key, declared_max_bytes, prefix, move |scratch| {
            validate(&scratch)
        })
    }

    fn get_or_load_seeded_owned<T, F>(
        &self,
        key: MetadataCacheKey,
        declared_max_bytes: u64,
        prefix: &[u8],
        validate: F,
    ) -> Result<MetadataCachePin<T>, MetadataCacheError>
    where
        T: Send + Sync + 'static,
        F: FnOnce(Vec<u8>) -> Result<LoadedMetadata<T>, MetadataCacheError>,
    {
        if key.segment_identity() != self.handle().segment_identity()
            || key.file() != self.handle().file()
        {
            return Err(MetadataCacheError::transient(
                io::ErrorKind::InvalidInput,
                format!(
                    "metadata cache key does not match governed artifact: key={}/{:?} handle={}/{:?}",
                    key.segment_identity(),
                    key.file(),
                    self.handle().segment_identity(),
                    self.handle().file()
                ),
            ));
        }

        let scratch_len = usize::try_from(key.length()).map_err(|_| {
            MetadataCacheError::transient(
                io::ErrorKind::OutOfMemory,
                format!(
                    "metadata scratch length does not fit this platform: {}",
                    key.length()
                ),
            )
        })?;
        if prefix.len() > scratch_len {
            return Err(MetadataCacheError::transient(
                io::ErrorKind::InvalidInput,
                format!(
                    "staged metadata prefix exceeds complete range: prefix={} range={scratch_len}",
                    prefix.len()
                ),
            ));
        }
        let prefix_len = prefix.len();
        let offset = key.offset();
        let scratch_bytes = u64::try_from(scratch_len).map_err(|_| {
            MetadataCacheError::transient(
                io::ErrorKind::OutOfMemory,
                "metadata scratch length does not fit the governor counter",
            )
        })?;
        let class = key.class();
        let runtime = self.runtime();
        let reader = self.clone();
        let cache = runtime.cache();
        cache.get_or_load(key, declared_max_bytes, move || {
            let mut scratch_charge = runtime
                .inner
                .governor
                .reserve_in_flight_for_usage(scratch_bytes, MetadataUsageClass::Scratch)?;
            let mut scratch = Vec::new();
            scratch.try_reserve_exact(scratch_len).map_err(|error| {
                MetadataCacheError::transient(
                    io::ErrorKind::OutOfMemory,
                    format!("failed to allocate metadata scratch buffer: {error}"),
                )
            })?;
            let scratch_capacity = u64::try_from(scratch.capacity()).map_err(|_| {
                MetadataCacheError::transient(
                    io::ErrorKind::OutOfMemory,
                    "metadata scratch capacity does not fit the governor counter",
                )
            })?;
            scratch_charge.reconcile(scratch_capacity)?;
            scratch.resize(scratch_len, 0);
            scratch[..prefix_len].copy_from_slice(prefix);

            let suffix_offset = offset.checked_add(u64::try_from(prefix_len).map_err(|_| {
                MetadataCacheError::transient(
                    io::ErrorKind::InvalidInput,
                    "staged metadata prefix length exceeds u64",
                )
            })?);
            let suffix_offset = suffix_offset.ok_or_else(|| {
                MetadataCacheError::transient(
                    io::ErrorKind::InvalidInput,
                    "staged metadata suffix offset overflows",
                )
            })?;
            let read_result =
                reader.read_exact_at_inner(suffix_offset, &mut scratch[prefix_len..], Some(class));
            if let Err(error) = read_result {
                drop(scratch);
                drop(scratch_charge);
                return Err(reader.finish_read_failure(error));
            }

            let loaded = validate(scratch);
            match loaded {
                Ok(loaded) => Ok(loaded.with_scratch_charge(scratch_charge)),
                Err(error) => {
                    // Validation failed after the scratch allocation was
                    // destroyed. Release its charge before the cache records
                    // structural corruption or completes the single-flight.
                    drop(scratch_charge);
                    Err(error)
                }
            }
        })
    }

    fn read_exact_at_inner(
        &self,
        offset: u64,
        destination: &mut [u8],
        class: Option<MetadataCacheClass>,
    ) -> Result<(), ArtifactReadFailure> {
        let runtime = self.runtime();
        runtime
            .cache()
            .check_artifact_with_key(&self.cache_artifact)
            .map_err(ArtifactReadFailure::Cache)?;

        if destination.is_empty() {
            return Ok(());
        }

        let length = u64::try_from(destination.len()).map_err(|_| {
            ArtifactReadFailure::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata read length does not fit the issued-byte counter",
            ))
        })?;
        offset.checked_add(length).ok_or_else(|| {
            ArtifactReadFailure::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata read range overflows u64",
            ))
        })?;
        let lease = runtime
            .file_manager()
            .acquire(self.handle())
            .map_err(ArtifactReadFailure::FileManager)?;
        runtime
            .inner
            .reads
            .record(self.handle().file(), class, offset, length);
        let result = lease.read_exact_at(offset, destination);
        drop(lease);
        result.map_err(ArtifactReadFailure::Io)
    }

    /// Checks whether immutable-artifact corruption was already recorded.
    pub(super) fn check_recorded_error(&self) -> Result<(), MetadataCacheError> {
        self.check_artifact()
    }

    /// Gates reuse of query-local decoded state on the sticky artifact ledger.
    pub(super) fn check_artifact(&self) -> Result<(), MetadataCacheError> {
        self.runtime()
            .cache()
            .check_artifact_with_key(&self.cache_artifact)
    }

    /// Records a structural error discovered outside a cache loader, after
    /// the touched bytes and every descriptor/scratch lease have been released.
    /// Resource and caller-input errors remain transient and never poison the
    /// immutable artifact.
    pub(super) fn record_validation_error(&self, error: io::Error) -> MetadataCacheError {
        self.record_validation_failure(MetadataCacheError::from_io(error))
    }

    pub(super) fn record_validation_failure(
        &self,
        error: MetadataCacheError,
    ) -> MetadataCacheError {
        self.runtime()
            .cache()
            .record_artifact_error_with_key(&self.cache_artifact, error)
    }

    fn finish_read_failure(&self, failure: ArtifactReadFailure) -> MetadataCacheError {
        let error = match failure {
            ArtifactReadFailure::Cache(error) => return error,
            ArtifactReadFailure::FileManager(error) if error.is_structural() => {
                MetadataCacheError::structural(
                    StructuralMetadataErrorKind::InvalidData,
                    error.to_string(),
                )
            }
            ArtifactReadFailure::FileManager(error) => MetadataCacheError::transient(
                transient_file_manager_kind(&error),
                error.to_string(),
            ),
            ArtifactReadFailure::Io(error) => MetadataCacheError::from_io(error),
        };

        self.runtime()
            .cache()
            .record_artifact_error_with_key(&self.cache_artifact, error)
    }
}

enum ArtifactReadFailure {
    Cache(MetadataCacheError),
    FileManager(MetadataFileManagerError),
    Io(io::Error),
}

fn transient_file_manager_kind(error: &MetadataFileManagerError) -> io::ErrorKind {
    match error {
        MetadataFileManagerError::Open { source, .. } => source.kind(),
        MetadataFileManagerError::SegmentRetiring { .. }
        | MetadataFileManagerError::OpenFileCapacityUnavailable { .. } => io::ErrorKind::WouldBlock,
        MetadataFileManagerError::UnsupportedPlatformIdentity => io::ErrorKind::Unsupported,
        MetadataFileManagerError::EmptySegmentIdentity
        | MetadataFileManagerError::UntrackedSegmentFile { .. }
        | MetadataFileManagerError::ConflictingHandle { .. }
        | MetadataFileManagerError::RequestExceedsOpenFileLimit { .. }
        | MetadataFileManagerError::StructuralReplacement { .. } => io::ErrorKind::InvalidInput,
    }
}

#[cfg(test)]
mod tests;
