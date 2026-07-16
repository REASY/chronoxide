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
mod tests {
    use std::fs;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use super::*;
    use crate::storage::metadata_cache::{
        LIVE_REGISTRY_ENTRY_BYTES, MetadataArtifactRegistrationError, MetadataCacheClass,
        MetadataCorruption, RESIDENT_ENTRY_BYTES,
    };
    use crate::storage::metadata_governor::MetadataGovernorConfig;
    use crate::storage::segment::{SEGMENT_FOOTER_TRACKED_FILES, SegmentFile};
    use crate::util::xxhash64;

    fn config(
        retained_max_bytes: u64,
        in_flight_max_bytes: u64,
        max_open_files: u32,
        max_cached_open_files: u32,
    ) -> MetadataGovernorConfig {
        MetadataGovernorConfig {
            retained_max_bytes,
            in_flight_max_bytes,
            max_open_files,
            max_cached_open_files,
        }
    }

    fn write_inventory(
        directory: &TempDir,
        identity: &str,
        selected: Option<(SegmentFile, &[u8])>,
    ) -> Vec<SegmentArtifactRegistration> {
        SEGMENT_FOOTER_TRACKED_FILES
            .into_iter()
            .map(|candidate| {
                let path = directory
                    .path()
                    .join(format!("{identity}-{}", candidate.filename()));
                let contents = selected
                    .filter(|(file, _)| *file == candidate)
                    .map_or(b"fixture".as_slice(), |(_, bytes)| bytes);
                fs::write(&path, contents).expect("write canonical metadata fixture");
                SegmentArtifactRegistration::new(
                    candidate,
                    path,
                    u64::try_from(contents.len()).expect("fixture length fits u64"),
                )
            })
            .collect()
    }

    fn fixture(
        directory: &TempDir,
        runtime: &StoreMetadataRuntime,
        identity: &str,
        file: SegmentFile,
        bytes: &[u8],
    ) -> GovernedArtifactReader {
        let inventory = write_inventory(directory, identity, Some((file, bytes)));
        let registered = runtime
            .register_segment(identity, &inventory)
            .expect("register canonical metadata fixture");
        let reader = registered.reader(file).expect("create governed reader");
        drop(registered);
        reader
    }

    #[test]
    fn generation_provenance_rejects_same_identity_after_reregistration() {
        let directory = TempDir::new().expect("create provenance temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let inventory = write_inventory(&directory, "provenance-generation", None);
        let first = runtime
            .register_segment("provenance-generation", &inventory)
            .expect("register first generation");
        let first_guard = first.read_guard().expect("read first generation");
        let provenance = first_guard.provenance();
        assert!(provenance.matches(&first_guard));
        let first_generation = first_guard.generation();
        drop(first_guard);
        drop(first);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);

        let second = runtime
            .register_segment("provenance-generation", &inventory)
            .expect("register second generation");
        let second_guard = second.read_guard().expect("read second generation");
        assert_ne!(second_guard.generation(), first_generation);
        assert!(!provenance.matches(&second_guard));
    }

    fn key(reader: &GovernedArtifactReader, offset: u64, length: u64) -> MetadataCacheKey {
        reader
            .metadata_cache_key(offset, length, MetadataCacheClass::SeriesHotPage)
            .expect("valid fixture key")
    }

    fn replace_same_length(reader: &GovernedArtifactReader, replacement: &[u8]) {
        assert_eq!(
            usize::try_from(reader.handle().expected_len()).expect("fixture length fits usize"),
            replacement.len()
        );
        let backup = reader.handle().path().with_extension("original");
        fs::rename(reader.handle().path(), backup).expect("retain original inode");
        fs::write(reader.handle().path(), replacement).expect("write replacement inode");
    }

    fn assert_no_live_io(runtime: &StoreMetadataRuntime) {
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.files.active_leases, 0);
        assert_eq!(snapshot.files.active_open_files, 0);
        assert_eq!(snapshot.files.opening_files, 0);
        assert_eq!(snapshot.files.pending_open_files, 0);
        assert_eq!(snapshot.cache.active_loads, 0);
    }

    fn file_reads(stats: MetadataReadStats, file: SegmentFile) -> MetadataIssuedReadCount {
        stats
            .files
            .into_iter()
            .find(|entry| entry.file == file)
            .expect("tracked file has read counters")
            .issued
    }

    #[test]
    fn runtime_shares_one_governor_cache_and_file_manager() {
        let runtime =
            StoreMetadataRuntime::new(config(16 * 1024, 16 * 1024, 1, 1)).expect("valid runtime");
        let clone = runtime.clone();
        assert!(Arc::ptr_eq(&runtime.governor(), &clone.governor()));
        assert!(Arc::ptr_eq(
            runtime.cache().governor(),
            clone.cache().governor()
        ));
        assert!(Arc::ptr_eq(&runtime.file_manager(), &clone.file_manager()));
        assert_eq!(runtime.snapshot(), clone.snapshot());
    }

    #[test]
    fn canonical_inventory_is_validated_before_any_preflight_or_cache_publication() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let inventory = write_inventory(&directory, "invalid-inventory", None);

        assert!(matches!(
            runtime.register_segment("invalid-inventory", &inventory[..6]),
            Err(StoreMetadataRuntimeError::InvalidArtifactCount {
                expected: 7,
                actual: 6,
            })
        ));
        let mut reordered = inventory.clone();
        reordered.swap(0, 1);
        assert!(matches!(
            runtime.register_segment("invalid-inventory", &reordered),
            Err(StoreMetadataRuntimeError::NonCanonicalArtifact { index: 0, .. })
        ));
        assert!(matches!(
            runtime.register_segment("", &inventory),
            Err(StoreMetadataRuntimeError::EmptySegmentIdentity)
        ));

        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.files.preflight_calls, 0);
        assert_eq!(snapshot.cache.registered_artifacts, 0);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    }

    #[test]
    fn concurrent_same_definition_registration_preflights_once_at_fd_cap_one() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime = Arc::new(
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime"),
        );
        let inventory = Arc::new(write_inventory(&directory, "registration-join", None));
        let holders_ready = Arc::new(Barrier::new(7));
        let release_holders = Arc::new(Barrier::new(7));
        let mut workers = Vec::new();
        for _ in 0..6 {
            let runtime = Arc::clone(&runtime);
            let inventory = Arc::clone(&inventory);
            let holders_ready = Arc::clone(&holders_ready);
            let release_holders = Arc::clone(&release_holders);
            workers.push(thread::spawn(move || {
                let registered = runtime
                    .register_segment("registration-join", &inventory)
                    .expect("join canonical registration");
                let generation = registered.generation();
                holders_ready.wait();
                release_holders.wait();
                generation
            }));
        }
        holders_ready.wait();

        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.files.preflight_calls, 7);
        assert_eq!(snapshot.files.successful_preflights, 7);
        assert_eq!(snapshot.files.peak_occupied_open_slots, 1);
        assert_eq!(snapshot.files.open_files, 0);
        assert_eq!(snapshot.cache.registered_artifacts, 7);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));

        release_holders.wait();
        let generations = workers
            .into_iter()
            .map(|worker| worker.join().expect("registration worker joins"))
            .collect::<Vec<_>>();
        assert!(generations.iter().all(|generation| *generation == 1));
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    }

    #[test]
    fn waiting_registration_reserves_owner_before_publication() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime = Arc::new(
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime"),
        );
        let inventory = Arc::new(write_inventory(&directory, "reserved-join", None));
        let leader_entered = Arc::new(Barrier::new(2));
        let resume_leader = Arc::new(Barrier::new(2));
        runtime.install_registration_leader_pause_for_test(
            Arc::clone(&leader_entered),
            Arc::clone(&resume_leader),
            false,
        );

        let (leader_sender, leader_receiver) = mpsc::sync_channel(1);
        let leader_runtime = Arc::clone(&runtime);
        let leader_inventory = Arc::clone(&inventory);
        let leader = thread::spawn(move || {
            let registered = leader_runtime
                .register_segment("reserved-join", &leader_inventory)
                .expect("leader publishes registration");
            leader_sender
                .send(registered)
                .expect("send published leader owner");
        });
        leader_entered.wait();

        let joiner_entered = Arc::new(Barrier::new(2));
        let resume_joiner = Arc::new(Barrier::new(2));
        runtime.install_registration_join_wake_pause_for_test(
            Arc::clone(&joiner_entered),
            Arc::clone(&resume_joiner),
            false,
        );
        let (joiner_sender, joiner_receiver) = mpsc::sync_channel(1);
        let joiner_runtime = Arc::clone(&runtime);
        let joiner_inventory = Arc::clone(&inventory);
        let joiner = thread::spawn(move || {
            let registered = joiner_runtime
                .register_segment("reserved-join", &joiner_inventory)
                .expect("waiting caller joins published registration");
            joiner_sender.send(registered).expect("send joined owner");
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while runtime.pending_registration_for_test("reserved-join") != Some((1, 2)) {
            assert!(
                Instant::now() < deadline,
                "joining caller did not reserve an ownership slot"
            );
            thread::yield_now();
        }
        resume_leader.wait();
        let leader_owner = leader_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("leader registration returns");
        joiner_entered.wait();

        let generation = leader_owner.generation();
        drop(leader_owner);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
        assert_eq!(runtime.snapshot().files.preflight_calls, 7);

        resume_joiner.wait();
        let joiner_owner = joiner_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("reserved joiner returns");
        assert_eq!(joiner_owner.generation(), generation);
        assert_eq!(runtime.snapshot().files.preflight_calls, 7);
        leader.join().expect("leader thread joins");
        joiner.join().expect("joiner thread joins");

        drop(joiner_owner);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    }

    #[test]
    fn unwind_after_cache_registration_rolls_back_the_whole_transaction() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime = Arc::new(
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime"),
        );
        let inventory = Arc::new(write_inventory(&directory, "registration-unwind", None));
        let after_cache = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        runtime.install_registration_after_cache_pause_for_test(
            Arc::clone(&after_cache),
            Arc::clone(&resume),
            true,
        );

        let worker_runtime = Arc::clone(&runtime);
        let worker_inventory = Arc::clone(&inventory);
        let worker = thread::spawn(move || {
            catch_unwind(AssertUnwindSafe(|| {
                let _ = worker_runtime.register_segment("registration-unwind", &worker_inventory);
            }))
            .is_err()
        });
        after_cache.wait();
        assert_eq!(
            runtime.pending_registration_for_test("registration-unwind"),
            Some((1, 1))
        );
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
        resume.wait();
        assert!(worker.join().expect("unwind worker joins"));

        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
        assert_no_live_io(&runtime);
        assert_eq!(runtime.snapshot().files.open_files, 0);

        let retry = runtime
            .register_segment("registration-unwind", &inventory)
            .expect("retry after complete unwind rollback");
        assert_eq!(retry.generation(), 2);
        assert_eq!(runtime.snapshot().files.preflight_calls, 14);
        drop(retry);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    }

    #[test]
    fn active_registration_joins_exact_definition_and_rejects_conflict() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let inventory = write_inventory(&directory, "active-join", None);
        let first = runtime
            .register_segment("active-join", &inventory)
            .expect("publish first owner");
        let second = runtime
            .register_segment("active-join", &inventory)
            .expect("join active owner");
        assert_eq!(first.generation(), second.generation());
        assert_eq!(runtime.snapshot().files.preflight_calls, 7);

        let mut conflicting = inventory.clone();
        conflicting[2] = SegmentArtifactRegistration::new(
            SegmentFile::Series,
            directory.path().join("different-series.bin"),
            conflicting[2].footer_recorded_len(),
        );
        assert!(matches!(
            runtime.register_segment("active-join", &conflicting),
            Err(StoreMetadataRuntimeError::ConflictingRegistration { .. })
        ));
        assert_eq!(runtime.snapshot().files.preflight_calls, 7);

        drop(first);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));
        drop(second);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
    }

    #[test]
    fn failed_preflight_rolls_back_and_same_identity_can_retry() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let inventory = write_inventory(&directory, "registration-retry", None);
        let mut malformed = inventory.clone();
        malformed[2] = SegmentArtifactRegistration::new(
            SegmentFile::Series,
            malformed[2].path(),
            malformed[2].footer_recorded_len() + 1,
        );
        assert!(matches!(
            runtime.register_segment("registration-retry", &malformed),
            Err(StoreMetadataRuntimeError::FileManager(
                MetadataFileManagerError::StructuralReplacement { .. }
            ))
        ));
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);

        let registered = runtime
            .register_segment("registration-retry", &inventory)
            .expect("retry corrected inventory");
        assert_eq!(registered.generation(), 2);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 7);
        drop(registered);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    }

    #[test]
    fn final_owner_waits_for_reader_clones_before_atomic_retirement() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let inventory = write_inventory(&directory, "guarded-retirement", None);
        let registered = runtime
            .register_segment("guarded-retirement", &inventory)
            .expect("register guarded segment");
        let owner_clone = registered.clone();
        let guard = registered.read_guard().expect("create read guard");
        let reader = guard
            .reader(SegmentFile::Series)
            .expect("create guard-bound reader");

        drop(registered);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 1, 0, 0, 0));
        drop(owner_clone);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 1, 0, 0));
        assert!(matches!(
            runtime.register_segment("guarded-retirement", &inventory),
            Err(StoreMetadataRuntimeError::SegmentRetiring { .. })
        ));

        drop(guard);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 1, 0, 0));
        drop(reader);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);

        let next = runtime
            .register_segment("guarded-retirement", &inventory)
            .expect("register next generation after complete retirement");
        assert!(next.generation() > 1);
        drop(next);
    }

    #[test]
    fn deferred_cache_pin_blocks_same_identity_until_final_pin_drop() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let inventory = write_inventory(
            &directory,
            "deferred-pin",
            Some((SegmentFile::Series, b"value")),
        );
        let registered = runtime
            .register_segment("deferred-pin", &inventory)
            .expect("register pinned segment");
        let reader = registered
            .reader(SegmentFile::Series)
            .expect("create pinned reader");
        let pin = reader
            .get_or_load(key(&reader, 0, 5), 5, |bytes| {
                Ok(LoadedMetadata::new(bytes.to_vec(), 5))
            })
            .expect("load pinned metadata");

        drop(registered);
        drop(reader);
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 1);
        assert!(matches!(
            runtime.register_segment("deferred-pin", &inventory),
            Err(StoreMetadataRuntimeError::Cache(
                MetadataArtifactRegistrationError::Retiring { .. }
            ))
        ));
        assert_eq!(runtime.lifecycle_counts_for_test(), (0, 0, 0, 0, 0));

        drop(pin);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
        let next = runtime
            .register_segment("deferred-pin", &inventory)
            .expect("register after final pin removes cache tombstone");
        drop(next);
    }

    #[test]
    fn validated_cache_hit_reuses_value_without_another_fd_acquisition() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 1)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "cache-reuse",
            SegmentFile::Series,
            b"metadata",
        );
        let loads = AtomicUsize::new(0);

        let first = reader
            .get_or_load(key(&reader, 0, 8), 8, |bytes| {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(LoadedMetadata::new(bytes.to_vec(), 8))
            })
            .expect("load metadata");
        let second = reader
            .get_or_load(key(&reader, 0, 8), 8, |_| {
                panic!("cache hit must not invoke loader")
            })
            .expect("reuse metadata");
        assert!(MetadataCachePin::ptr_eq(&first, &second));
        assert_eq!(&**second, b"metadata");
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.cache.hits, 1);
        assert_eq!(snapshot.files.acquire_calls, 1);
        assert_eq!(snapshot.files.preflight_calls, 7);
        assert_eq!(snapshot.files.successful_preflights, 7);
        assert_eq!(snapshot.files.descriptor_opens, 8);
        assert_eq!(snapshot.files.descriptor_closes, 7);
        assert_eq!(snapshot.files.cached_open_files, 1);
        assert_no_live_io(&runtime);

        drop(first);
        drop(second);
        runtime.cache().evict_all_resident();
        assert_eq!(runtime.snapshot().cache.live_allocations, 0);
    }

    #[test]
    fn issued_read_stats_attribute_exact_spans_and_cache_hits_issue_nothing() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "issued-read-stats",
            SegmentFile::Series,
            b"abcdefgh",
        );
        let before = runtime.snapshot().reads;

        let mut unclassified = [0_u8; 1];
        reader
            .read_exact_at(0, &mut unclassified)
            .expect("read unclassified byte");
        assert_eq!(unclassified, [b'a']);
        let mut staged_root = [0_u8; 2];
        reader
            .read_exact_at_for_class(1, &mut staged_root, MetadataCacheClass::SeriesRoot)
            .expect("read staged root prefix");
        assert_eq!(staged_root, *b"bc");

        let root_key = MetadataCacheKey::new(
            reader.segment_identity(),
            SegmentFile::Series,
            1,
            5,
            MetadataCacheClass::SeriesRoot,
        )
        .expect("valid staged root key");
        let first = reader
            .get_or_load_with_prefix(root_key.clone(), 5, &staged_root, |bytes| {
                Ok(LoadedMetadata::new(bytes.to_vec(), 5))
            })
            .expect("load staged root without rereading its prefix");
        let second = reader
            .get_or_load_with_prefix(root_key, 5, &staged_root, |_| {
                panic!("warm cache hit must not issue another range")
            })
            .expect("reuse staged root");
        assert!(MetadataCachePin::ptr_eq(&first, &second));
        assert_eq!(&**first, b"bcdef");

        let delta = runtime.snapshot().reads.delta_since(before);
        assert_eq!(delta.issued, MetadataIssuedReadCount { calls: 3, bytes: 6 });
        assert_eq!(
            delta.unclassified,
            MetadataIssuedReadCount { calls: 1, bytes: 1 }
        );
        let series = delta
            .files
            .iter()
            .find(|stats| stats.file == SegmentFile::Series)
            .expect("series file stats");
        assert_eq!(
            series.issued,
            MetadataIssuedReadCount { calls: 3, bytes: 6 }
        );
        assert_eq!(
            delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
            MetadataIssuedReadCount { calls: 2, bytes: 5 }
        );
        assert_eq!(
            runtime.inner.reads.take_spans(),
            vec![
                MetadataReadSpan {
                    file: SegmentFile::Series,
                    class: None,
                    offset: 0,
                    length: 1,
                },
                MetadataReadSpan {
                    file: SegmentFile::Series,
                    class: Some(MetadataCacheClass::SeriesRoot),
                    offset: 1,
                    length: 2,
                },
                MetadataReadSpan {
                    file: SegmentFile::Series,
                    class: Some(MetadataCacheClass::SeriesRoot),
                    offset: 3,
                    length: 3,
                },
            ]
        );
    }

    #[test]
    fn bootstrap_validation_error_is_sticky_without_an_issued_read() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "bootstrap-corruption",
            SegmentFile::Series,
            b"metadata",
        );
        let before = runtime.snapshot().reads;

        let first = reader.record_validation_error(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid staged series header",
        ));
        assert!(matches!(first, MetadataCacheError::Structural(_)));
        let mut byte = [0_u8; 1];
        let second = reader
            .read_exact_at_for_class(0, &mut byte, MetadataCacheClass::SeriesRoot)
            .expect_err("sticky header corruption gates later reads");
        assert_eq!(first, second);
        assert_eq!(runtime.snapshot().reads.delta_since(before).issued.calls, 0);
        assert_eq!(runtime.snapshot().cache.corruption_detections, 1);
    }

    #[test]
    fn staged_prefix_larger_than_key_is_transient_without_io_or_poisoning() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "oversized-prefix",
            SegmentFile::Series,
            b"abc",
        );
        let before = runtime.snapshot();

        let error = reader
            .get_or_load_with_prefix::<u8, _>(key(&reader, 0, 1), 1, b"ab", |_| {
                panic!("invalid prefix must not invoke validator")
            })
            .expect_err("oversized staged prefix is rejected");
        assert!(matches!(
            error,
            MetadataCacheError::Transient {
                kind: io::ErrorKind::InvalidInput,
                ..
            }
        ));
        let after = runtime.snapshot();
        assert_eq!(after.reads.delta_since(before.reads).issued.calls, 0);
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
        assert_eq!(after.cache.sticky_artifacts, 0);
        assert_eq!(after.cache.active_loads, 0);
    }

    #[test]
    fn overflowing_read_range_is_rejected_before_fd_acquisition_or_accounting() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "overflowing-read",
            SegmentFile::Series,
            b"abc",
        );
        let before = runtime.snapshot();
        let mut bytes = [0_u8; 2];

        let error = reader
            .read_exact_at_for_class(u64::MAX, &mut bytes, MetadataCacheClass::SeriesRoot)
            .expect_err("overflowing range is caller input, not issued I/O");
        assert!(matches!(
            error,
            MetadataCacheError::Transient {
                kind: io::ErrorKind::InvalidInput,
                ..
            }
        ));
        let after = runtime.snapshot();
        assert_eq!(after.reads.delta_since(before.reads).issued.calls, 0);
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
        assert_eq!(after.cache.sticky_artifacts, 0);
    }

    #[test]
    fn staged_prefix_covering_full_key_publishes_without_suffix_io() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "complete-prefix",
            SegmentFile::Series,
            b"abc",
        );
        let before = runtime.snapshot().reads;
        let mut prefix = [0_u8; 3];
        reader
            .read_exact_at_for_class(0, &mut prefix, MetadataCacheClass::SeriesRoot)
            .expect("read complete staged range");

        let root_key = MetadataCacheKey::new(
            reader.segment_identity(),
            SegmentFile::Series,
            0,
            3,
            MetadataCacheClass::SeriesRoot,
        )
        .expect("valid complete root key");
        let pin = reader
            .get_or_load_with_prefix(root_key, 3, &prefix, |bytes| {
                Ok(LoadedMetadata::new(bytes.to_vec(), 3))
            })
            .expect("publish fully seeded value");
        assert_eq!(&**pin, b"abc");

        let delta = runtime.snapshot().reads.delta_since(before);
        assert_eq!(delta.issued, MetadataIssuedReadCount { calls: 1, bytes: 3 });
        assert_eq!(
            delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
            MetadataIssuedReadCount { calls: 1, bytes: 3 }
        );
    }

    #[test]
    fn staged_suffix_short_read_is_sticky_and_releases_all_load_resources() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "short-staged-suffix",
            SegmentFile::Series,
            b"abc",
        );
        let before = runtime.snapshot().reads;
        let mut prefix = [0_u8; 1];
        reader
            .read_exact_at_for_class(1, &mut prefix, MetadataCacheClass::SeriesRoot)
            .expect("read valid staged prefix");
        assert_eq!(prefix, [b'b']);
        let root_key = MetadataCacheKey::new(
            reader.segment_identity(),
            SegmentFile::Series,
            1,
            3,
            MetadataCacheClass::SeriesRoot,
        )
        .expect("valid key extending past EOF");

        let error = reader
            .get_or_load_with_prefix::<u8, _>(root_key, 1, &prefix, |_| {
                panic!("short suffix must not reach validation")
            })
            .expect_err("short staged suffix is corruption");
        assert!(matches!(error, MetadataCacheError::Structural(_)));
        let after = runtime.snapshot();
        let delta = after.reads.delta_since(before);
        assert_eq!(delta.issued, MetadataIssuedReadCount { calls: 2, bytes: 3 });
        assert_eq!(after.cache.successful_loads, 0);
        assert_eq!(after.cache.failed_loads, 1);
        assert_eq!(after.cache.sticky_artifacts, 1);
        assert_eq!(after.cache.active_loads, 0);
        assert_eq!(after.cache.live_allocations, 0);
        assert_eq!(
            after
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            0
        );
        assert_no_live_io(&runtime);

        let mut retry = [0_u8; 1];
        reader
            .read_exact_at_for_class(1, &mut retry, MetadataCacheClass::SeriesRoot)
            .expect_err("sticky suffix failure gates retry before I/O");
        assert_eq!(runtime.snapshot().reads, after.reads);
    }

    #[test]
    fn validated_scratch_handoff_installs_one_retained_cache_charge_set() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "retained-handoff",
            SegmentFile::Series,
            b"x",
        );

        let pin = reader
            .get_or_load(key(&reader, 0, 1), 1, |bytes| {
                Ok(LoadedMetadata::new(bytes[0], 1))
            })
            .expect("retained metadata load");
        assert_eq!(*pin, b'x');

        let snapshot = runtime.snapshot();
        let scratch = snapshot.governor.usage(MetadataUsageClass::Scratch);
        assert_eq!(scratch.in_flight_bytes, 0);
        assert_eq!(scratch.retained_bytes, 0);
        let class = snapshot.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
        assert_eq!(class.in_flight_bytes, 0);
        assert_eq!(
            class.retained_bytes,
            1 + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
        );
        assert_eq!(snapshot.cache.resident_entries, 1);
        assert_eq!(snapshot.cache.live_allocations, 1);
        assert_no_live_io(&runtime);

        drop(pin);
        runtime.cache().evict_all_resident();
        let released = runtime.snapshot();
        let class = released.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
        assert_eq!(class.in_flight_bytes, 0);
        assert_eq!(class.retained_bytes, 0);
    }

    #[test]
    fn validated_scratch_handoff_keeps_zero_retention_load_transient() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime = StoreMetadataRuntime::new(config(0, 64 * 1024, 1, 0)).expect("valid runtime");
        let bytes = vec![9_u8; 256];
        let reader = fixture(
            &directory,
            &runtime,
            "transient-handoff",
            SegmentFile::Series,
            &bytes,
        );

        let pin = reader
            .get_or_load(key(&reader, 0, 256), 1, |bytes| {
                Ok(LoadedMetadata::new(bytes[0], 1))
            })
            .expect("transient metadata load");
        assert_eq!(*pin, 9);

        let snapshot = runtime.snapshot();
        let scratch = snapshot.governor.usage(MetadataUsageClass::Scratch);
        assert_eq!(scratch.in_flight_bytes, 0);
        assert_eq!(scratch.retained_bytes, 0);
        let class = snapshot.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
        assert_eq!(class.in_flight_bytes, 1 + LIVE_REGISTRY_ENTRY_BYTES);
        assert_eq!(class.retained_bytes, 0);
        assert_eq!(snapshot.cache.resident_entries, 0);
        assert_eq!(snapshot.cache.live_allocations, 1);
        assert_no_live_io(&runtime);

        drop(pin);
        let released = runtime.snapshot();
        let class = released.cache.class_charges[MetadataCacheClass::SeriesHotPage.stable_index()];
        assert_eq!(class.in_flight_bytes, 0);
        assert_eq!(class.retained_bytes, 0);
    }

    #[test]
    fn registered_hash_matches_empty_seed_zero_digest_and_still_checks_identity() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(&directory, &runtime, "hash-empty", SegmentFile::Chunks, b"");
        let before = runtime.snapshot();
        let mut scratch = [0_u8; 17];

        let actual = reader
            .hash_registered_xxh64(&mut scratch)
            .expect("hash empty registered artifact");

        assert_eq!(actual, xxhash64(b""));
        let after = runtime.snapshot();
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
        assert_eq!(after.reads.delta_since(before.reads).issued.calls, 0);
        assert_eq!(after.files.peak_occupied_open_slots, 1);
        assert_eq!(after.files.open_files, 0);
        assert_no_live_io(&runtime);
    }

    #[test]
    fn registered_hash_streams_small_artifact_under_one_lease_and_classifies_reads() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let bytes = b"registered-artifact";
        let reader = fixture(
            &directory,
            &runtime,
            "hash-small",
            SegmentFile::Indexes,
            bytes,
        );
        let _ = runtime.inner.reads.take_spans();
        let before = runtime.snapshot();
        let mut scratch = [0_u8; 3];

        let actual = reader
            .hash_registered_xxh64(&mut scratch)
            .expect("hash small registered artifact");

        assert_eq!(actual, xxhash64(bytes));
        let after = runtime.snapshot();
        let delta = after.reads.delta_since(before.reads);
        let expected_calls = u64::try_from(bytes.len().div_ceil(scratch.len()))
            .expect("fixture call count fits u64");
        let expected_bytes = u64::try_from(bytes.len()).expect("fixture length fits u64");
        let expected = MetadataIssuedReadCount {
            calls: expected_calls,
            bytes: expected_bytes,
        };
        assert_eq!(delta.issued, expected);
        assert_eq!(delta.unclassified, MetadataIssuedReadCount::default());
        assert_eq!(file_reads(delta, SegmentFile::Indexes), expected);
        assert_eq!(
            delta.classes[MetadataCacheClass::FullValidation.stable_index()].issued,
            expected
        );
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
        assert_eq!(after.files.open_files, 0);
        assert_no_live_io(&runtime);
    }

    #[test]
    fn registered_hash_streams_more_than_one_mib_with_caller_owned_governed_scratch() {
        const HASH_BUFFER_BYTES: usize = 1024 * 1024;

        let directory = TempDir::new().expect("create temp directory");
        let hash_buffer_bytes_u64 =
            u64::try_from(HASH_BUFFER_BYTES).expect("hash buffer length fits u64");
        let runtime = StoreMetadataRuntime::new(config(64 * 1024, 2 * hash_buffer_bytes_u64, 1, 0))
            .expect("valid runtime");
        let bytes = (0..HASH_BUFFER_BYTES + 257)
            .map(|index| index.to_le_bytes()[0].wrapping_mul(37).wrapping_add(11))
            .collect::<Vec<_>>();
        let reader = fixture(
            &directory,
            &runtime,
            "hash-large",
            SegmentFile::Chunks,
            &bytes,
        );

        let governor = runtime.governor();
        let mut charge = governor
            .reserve_in_flight_for_usage(hash_buffer_bytes_u64, MetadataUsageClass::Scratch)
            .expect("reserve caller-owned hash scratch");
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(HASH_BUFFER_BYTES)
            .expect("allocate hash scratch");
        charge
            .reconcile(u64::try_from(scratch.capacity()).expect("capacity fits u64"))
            .expect("reconcile hash scratch charge");
        scratch.resize(HASH_BUFFER_BYTES, 0);

        let _ = runtime.inner.reads.take_spans();
        let before = runtime.snapshot();
        let scratch_before = before.governor.usage(MetadataUsageClass::Scratch);
        let actual = reader
            .hash_registered_xxh64(&mut scratch)
            .expect("hash large registered artifact");

        assert_eq!(actual, xxhash64(&bytes));
        let after = runtime.snapshot();
        assert_eq!(
            after.governor.usage(MetadataUsageClass::Scratch),
            scratch_before,
            "the hash primitive must not acquire, transfer, or release the caller's charge"
        );
        let expected = MetadataIssuedReadCount {
            calls: 2,
            bytes: u64::try_from(bytes.len()).expect("fixture length fits u64"),
        };
        let delta = after.reads.delta_since(before.reads);
        assert_eq!(delta.issued, expected);
        assert_eq!(file_reads(delta, SegmentFile::Chunks), expected);
        assert_eq!(
            delta.classes[MetadataCacheClass::FullValidation.stable_index()].issued,
            expected
        );
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
        assert_eq!(after.files.max_open_files, 1);
        assert_eq!(after.files.peak_occupied_open_slots, 1);
        assert_eq!(after.files.peak_open_files, 1);
        assert_eq!(after.files.open_files, 0);
        assert_eq!(
            runtime.inner.reads.take_spans(),
            vec![
                MetadataReadSpan {
                    file: SegmentFile::Chunks,
                    class: Some(MetadataCacheClass::FullValidation),
                    offset: 0,
                    length: hash_buffer_bytes_u64,
                },
                MetadataReadSpan {
                    file: SegmentFile::Chunks,
                    class: Some(MetadataCacheClass::FullValidation),
                    offset: hash_buffer_bytes_u64,
                    length: 257,
                },
            ]
        );
        assert_no_live_io(&runtime);

        drop(scratch);
        drop(charge);
        assert_eq!(
            runtime
                .snapshot()
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            0
        );
    }

    #[test]
    fn registered_hash_rejects_empty_scratch_without_fd_io_or_poisoning() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "hash-no-scratch",
            SegmentFile::Series,
            b"value",
        );
        let before = runtime.snapshot();

        let error = reader
            .hash_registered_xxh64(&mut [])
            .expect_err("empty hash scratch must be rejected");

        assert!(matches!(
            error,
            MetadataCacheError::Transient {
                kind: io::ErrorKind::InvalidInput,
                ..
            }
        ));
        let after = runtime.snapshot();
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
        assert_eq!(after.reads, before.reads);
        assert_eq!(after.cache.sticky_artifacts, 0);
        assert_no_live_io(&runtime);
    }

    #[test]
    fn registered_hash_replacement_after_idle_eviction_is_sticky_before_reads() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 1)).expect("valid runtime");
        let inventory = write_inventory(
            &directory,
            "hash-replacement",
            Some((SegmentFile::Series, b"original")),
        );
        let registered = runtime
            .register_segment("hash-replacement", &inventory)
            .expect("register hash replacement fixture");
        let series = registered
            .reader(SegmentFile::Series)
            .expect("create series reader");
        let symbols = registered
            .reader(SegmentFile::Symbols)
            .expect("create symbols reader");
        let mut scratch = [0_u8; 3];

        series
            .hash_registered_xxh64(&mut scratch)
            .expect("hash original registered series");
        let before_eviction = runtime.snapshot().files.idle_evictions;
        symbols
            .hash_registered_xxh64(&mut scratch)
            .expect("evict original series descriptor");
        assert!(runtime.snapshot().files.idle_evictions > before_eviction);
        replace_same_length(&series, b"replaced");

        let before_failure = runtime.snapshot();
        let first = series
            .hash_registered_xxh64(&mut scratch)
            .expect_err("same-length replacement must fail identity validation");
        assert!(matches!(
            first,
            MetadataCacheError::Structural(MetadataCorruption {
                kind: StructuralMetadataErrorKind::InvalidData,
                ..
            })
        ));
        let after_failure = runtime.snapshot();
        assert_eq!(
            after_failure
                .reads
                .delta_since(before_failure.reads)
                .issued
                .calls,
            0,
            "replacement is rejected before a positional read"
        );
        assert_eq!(after_failure.files.open_files, 0);
        assert_no_live_io(&runtime);

        let acquire_calls = after_failure.files.acquire_calls;
        let second = series
            .hash_registered_xxh64(&mut scratch)
            .expect_err("sticky replacement must gate retry");
        assert_eq!(second, first);
        assert_eq!(runtime.snapshot().files.acquire_calls, acquire_calls);
        assert_eq!(runtime.snapshot().reads, after_failure.reads);
        assert_eq!(runtime.snapshot().cache.corruption_detections, 1);
    }

    #[test]
    fn registered_hash_short_read_is_sticky_after_the_lease_is_released() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "hash-short-read",
            SegmentFile::Series,
            b"abcdefgh",
        );
        let path = reader.handle().path().to_path_buf();
        let mut scratch = [0_u8; 4];
        let mut truncated = false;
        let before = runtime.snapshot();

        let first = reader
            .hash_registered_xxh64_with_hook(&mut scratch, |offset| {
                if !truncated {
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .expect("open registered inode for truncation")
                        .set_len(offset)
                        .expect("truncate registered inode after first hash read");
                    truncated = true;
                }
            })
            .expect_err("truncation during one lease must produce a short read");
        assert!(truncated);
        assert!(matches!(
            first,
            MetadataCacheError::Structural(MetadataCorruption {
                kind: StructuralMetadataErrorKind::UnexpectedEof,
                ..
            })
        ));
        let after = runtime.snapshot();
        let expected = MetadataIssuedReadCount { calls: 2, bytes: 8 };
        let delta = after.reads.delta_since(before.reads);
        assert_eq!(delta.issued, expected);
        assert_eq!(
            delta.classes[MetadataCacheClass::FullValidation.stable_index()].issued,
            expected
        );
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls + 1);
        assert_eq!(after.files.open_files, 0);
        assert_no_live_io(&runtime);

        let acquire_calls = after.files.acquire_calls;
        let second = reader
            .hash_registered_xxh64(&mut scratch)
            .expect_err("sticky short read must gate retry before reacquisition");
        assert_eq!(second, first);
        assert_eq!(runtime.snapshot().files.acquire_calls, acquire_calls);
        assert_eq!(runtime.snapshot().reads, after.reads);
    }

    #[test]
    fn registered_hash_rejects_an_append_after_the_last_read() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "hash-concurrent-append",
            SegmentFile::Series,
            b"abcdefgh",
        );
        let path = reader.handle().path().to_path_buf();
        let mut scratch = [0_u8; 8];
        let mut appended = false;

        let error = reader
            .hash_registered_xxh64_with_hook(&mut scratch, |offset| {
                if !appended {
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .expect("open registered inode for append")
                        .set_len(offset + 1)
                        .expect("append after final registered hash read");
                    appended = true;
                }
            })
            .expect_err("post-hash shape check must reject an append");

        assert!(appended);
        assert!(matches!(
            error,
            MetadataCacheError::Structural(MetadataCorruption {
                kind: StructuralMetadataErrorKind::InvalidData,
                ..
            })
        ));
        assert_eq!(runtime.snapshot().reads.issued.calls, 1);
        assert_eq!(runtime.snapshot().files.open_files, 0);
        assert_no_live_io(&runtime);
    }

    #[test]
    fn registered_hash_checks_existing_sticky_error_before_scratch_or_fd_io() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "hash-sticky-gate",
            SegmentFile::Indexes,
            b"metadata",
        );
        let recorded = reader.record_validation_error(io::Error::new(
            io::ErrorKind::InvalidData,
            "known registered-artifact corruption",
        ));
        let before = runtime.snapshot();

        let returned = reader
            .hash_registered_xxh64(&mut [])
            .expect_err("existing corruption wins over invalid scratch");

        assert_eq!(returned, recorded);
        let after = runtime.snapshot();
        assert_eq!(after.files.acquire_calls, before.files.acquire_calls);
        assert_eq!(after.reads, before.reads);
        assert_no_live_io(&runtime);
    }

    #[test]
    fn replacement_after_fd_eviction_becomes_sticky() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "replacement",
            SegmentFile::ChunkIndex,
            b"original",
        );
        let mut bytes = [0_u8; 8];
        reader
            .read_exact_at(0, &mut bytes)
            .expect("initial exact read");
        assert_eq!(&bytes, b"original");
        replace_same_length(&reader, b"replaced");

        let first = reader
            .read_exact_at(0, &mut bytes)
            .expect_err("replacement must fail");
        assert!(matches!(
            first,
            MetadataCacheError::Structural(MetadataCorruption {
                kind: StructuralMetadataErrorKind::InvalidData,
                ..
            })
        ));
        assert_no_live_io(&runtime);
        assert_eq!(runtime.snapshot().files.open_files, 0);
        let acquire_calls = runtime.snapshot().files.acquire_calls;

        let second = reader
            .read_exact_at(0, &mut bytes)
            .expect_err("sticky replacement must fail before acquire");
        assert_eq!(second, first);
        assert_eq!(runtime.snapshot().files.acquire_calls, acquire_calls);
        assert_eq!(runtime.snapshot().cache.corruption_detections, 1);
    }

    #[test]
    fn transient_io_failure_is_retryable_and_not_sticky() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "transient",
            SegmentFile::Symbols,
            b"retry",
        );

        let error = reader.finish_read_failure(ArtifactReadFailure::Io(io::Error::new(
            io::ErrorKind::Interrupted,
            "injected retryable read interruption",
        )));
        assert!(matches!(
            error,
            MetadataCacheError::Transient {
                kind: io::ErrorKind::Interrupted,
                ..
            }
        ));
        assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);

        let mut bytes = [0_u8; 5];
        reader.read_exact_at(0, &mut bytes).expect("retry succeeds");
        assert_eq!(&bytes, b"retry");
        assert_no_live_io(&runtime);
    }

    #[test]
    fn zero_retained_and_zero_cached_budgets_leave_no_values_or_fds() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime = StoreMetadataRuntime::new(config(0, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "zero-budgets",
            SegmentFile::Series,
            b"value",
        );
        let loads = AtomicUsize::new(0);

        for _ in 0..2 {
            let pin = reader
                .get_or_load(key(&reader, 0, 5), 5, |bytes| {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(LoadedMetadata::new(bytes.to_vec(), 5))
                })
                .expect("transient metadata load");
            assert_eq!(&**pin, b"value");
            drop(pin);
            let snapshot = runtime.snapshot();
            assert_eq!(snapshot.governor.retained_bytes, 0);
            assert_eq!(snapshot.cache.resident_entries, 0);
            assert_eq!(snapshot.cache.live_allocations, 0);
            assert_eq!(snapshot.files.open_files, 0);
            assert_no_live_io(&runtime);
        }
        assert_eq!(loads.load(Ordering::SeqCst), 2);

        drop(reader);
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.governor.retained_bytes, 0);
        assert_eq!(snapshot.governor.in_flight_bytes, 0);
    }

    #[test]
    fn one_open_file_limit_is_respected_across_artifacts() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 1)).expect("valid runtime");
        let first = fixture(
            &directory,
            &runtime,
            "one-file-a",
            SegmentFile::Symbols,
            b"a",
        );
        let second = fixture(
            &directory,
            &runtime,
            "one-file-b",
            SegmentFile::Series,
            b"b",
        );

        let mut byte = [0_u8; 1];
        first.read_exact_at(0, &mut byte).expect("read first");
        assert_eq!(byte, *b"a");
        second.read_exact_at(0, &mut byte).expect("read second");
        assert_eq!(byte, *b"b");

        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.files.max_open_files, 1);
        assert_eq!(snapshot.files.peak_occupied_open_slots, 1);
        assert_eq!(snapshot.files.peak_open_files, 1);
        assert_eq!(snapshot.files.open_files, 1);
        assert_eq!(snapshot.files.cached_open_files, 1);
        assert_no_live_io(&runtime);
    }

    #[test]
    fn scratch_budget_refusal_cleans_up_and_allows_a_smaller_retry() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime = StoreMetadataRuntime::new(config(0, 12 * 1024, 1, 0)).expect("valid runtime");
        let bytes = vec![7_u8; 8 * 1024];
        let reader = fixture(&directory, &runtime, "scratch", SegmentFile::Series, &bytes);
        let baseline = runtime.snapshot().governor.in_flight_bytes;
        let validates = AtomicUsize::new(0);

        let error = reader
            .get_or_load::<u8, _>(key(&reader, 0, 8 * 1024), 1, |_| {
                validates.fetch_add(1, Ordering::SeqCst);
                Ok(LoadedMetadata::new(7, 1))
            })
            .expect_err("scratch reservation must be refused");
        assert!(matches!(error, MetadataCacheError::Budget(_)));
        assert_eq!(validates.load(Ordering::SeqCst), 0);
        let refused = runtime.snapshot();
        assert_eq!(refused.governor.in_flight_bytes, baseline);
        assert_eq!(refused.cache.active_loads, 0);
        assert_eq!(refused.cache.live_allocations, 0);
        assert_eq!(refused.files.open_files, 0);
        assert_no_live_io(&runtime);

        let pin = reader
            .get_or_load(key(&reader, 0, 1), 1, |bytes| {
                validates.fetch_add(1, Ordering::SeqCst);
                Ok(LoadedMetadata::new(bytes[0], 1))
            })
            .expect("smaller retry succeeds");
        assert_eq!(*pin, 7);
        drop(pin);
        let scratch = runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Scratch);
        assert_eq!(scratch.in_flight_bytes, 0);
        assert_eq!(scratch.retained_bytes, 0);
        assert!(scratch.peak_in_flight_bytes >= 1);
        assert_eq!(scratch.peak_retained_bytes, 0);
        assert_no_live_io(&runtime);
    }

    #[test]
    fn short_exact_read_records_unexpected_eof_after_lease_release() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "short-read",
            SegmentFile::Series,
            b"four",
        );
        let mut bytes = [0_u8; 2];

        let error = reader
            .read_exact_at(3, &mut bytes)
            .expect_err("range past EOF must fail");
        assert!(matches!(
            error,
            MetadataCacheError::Structural(MetadataCorruption {
                kind: StructuralMetadataErrorKind::UnexpectedEof,
                ..
            })
        ));
        assert_no_live_io(&runtime);
        assert_eq!(runtime.snapshot().files.open_files, 0);
    }

    #[test]
    fn cache_key_mismatch_is_nonsticky_and_does_not_acquire_a_file() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "key-mismatch",
            SegmentFile::Series,
            b"value",
        );
        let wrong_key = MetadataCacheKey::new(
            "another-segment",
            SegmentFile::Series,
            0,
            5,
            MetadataCacheClass::SeriesHotPage,
        )
        .expect("valid mismatched key");

        let error = reader
            .get_or_load::<u8, _>(wrong_key, 1, |_| Ok(LoadedMetadata::new(1, 1)))
            .expect_err("mismatched key must fail");
        assert!(matches!(
            error,
            MetadataCacheError::Transient {
                kind: io::ErrorKind::InvalidInput,
                ..
            }
        ));
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.files.acquire_calls, 0);
        assert_eq!(snapshot.cache.sticky_artifacts, 0);
    }

    #[test]
    fn transient_file_manager_errors_are_nonsticky() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "manager-transient",
            SegmentFile::Symbols,
            b"ok",
        );
        let error = reader.finish_read_failure(ArtifactReadFailure::FileManager(
            MetadataFileManagerError::Open {
                path: PathBuf::from("injected"),
                source: io::Error::new(io::ErrorKind::PermissionDenied, "temporary denial"),
            },
        ));
        assert!(matches!(
            error,
            MetadataCacheError::Transient {
                kind: io::ErrorKind::PermissionDenied,
                ..
            }
        ));
        assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);

        let mut bytes = [0_u8; 2];
        reader.read_exact_at(0, &mut bytes).expect("retry succeeds");
        assert_eq!(&bytes, b"ok");
    }

    #[test]
    fn retiring_segment_file_manager_error_is_transient_would_block() {
        let directory = TempDir::new().expect("create temp directory");
        let runtime =
            StoreMetadataRuntime::new(config(64 * 1024, 64 * 1024, 1, 0)).expect("valid runtime");
        let reader = fixture(
            &directory,
            &runtime,
            "manager-retiring",
            SegmentFile::Symbols,
            b"ok",
        );
        let error = reader.finish_read_failure(ArtifactReadFailure::FileManager(
            MetadataFileManagerError::SegmentRetiring {
                segment_identity: Arc::from("manager-retiring"),
            },
        ));
        assert!(matches!(
            error,
            MetadataCacheError::Transient {
                kind: io::ErrorKind::WouldBlock,
                ..
            }
        ));
        assert_eq!(runtime.snapshot().cache.sticky_artifacts, 0);
    }
}
