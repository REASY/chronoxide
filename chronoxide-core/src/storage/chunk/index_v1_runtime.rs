//! Governed read-only runtime for the schema-6 chunk index.
//!
//! Schema 6 keeps a self-describing global offsets directory while
//! `SeriesEntryV2` duplicates each selected series span. This adapter binds
//! those two independently decoded facts before reading a span: every lookup
//! first loads the exact authoritative 16-byte offsets pair through the
//! aggregate governor and requires byte-range equality with the series entry.

use std::io;
use std::ops::Deref;

use thiserror::Error;

use crate::storage::metadata_cache::{
    LoadedMetadata, MetadataCacheError, MetadataCacheKey, MetadataCacheKeyError, MetadataCachePin,
};
use crate::storage::metadata_governor::{MetadataCacheClass, MetadataCharge, MetadataUsageClass};
use crate::storage::metadata_runtime::{
    GovernedArtifactReader, RegisteredSegment, SegmentGenerationProvenance, SegmentReadGuard,
    StoreMetadataRuntimeError,
};
use crate::storage::segment::SegmentFile;

use super::{
    CHUNK_ENTRY_LEN, CHUNK_INDEX_HEADER_LEN, CHUNK_INDEX_MAGIC, ChunkIndexEntry, ChunkIndexRange,
    IndexedChunkLocator,
};

const CHUNK_INDEX_ROOT_V1_LEN: usize = 20;
const CHUNK_INDEX_VERSION_V1: u16 = 1;
const CHUNK_INDEX_DIRECTORY_PAIR_LEN: u64 = 16;

/// Strict facts decoded from the exact 20-byte schema-6 chunk-index root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema6ChunkIndexRootV1 {
    pub(crate) num_series: u32,
    pub(crate) data_start: u64,
    pub(crate) file_len: u64,
}

impl Schema6ChunkIndexRootV1 {
    fn charged_bytes(self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

/// Governed schema-6 chunk-index failures.
#[derive(Debug, Error)]
pub(crate) enum Schema6ChunkIndexReaderError {
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Cache(#[from] MetadataCacheError),
    #[error(transparent)]
    CacheKey(#[from] MetadataCacheKeyError),
    #[error("schema-6 chunk-index value belongs to another segment generation")]
    ForeignSegmentGeneration,
}

/// Long-lived schema-6 reader. It owns one segment-generation lease and
/// independently authenticated context, but no decoded chunk-index root, read
/// guard, file descriptor, or cache pin.
pub(crate) struct GovernedSchema6ChunkIndexReader {
    registered: RegisteredSegment,
    expected_num_series: u32,
    chunk_file_lens: [u64; 2],
}

/// Query-scoped schema-6 chunk-index authorization.
pub(crate) struct GovernedSchema6ChunkIndexSession {
    guard: SegmentReadGuard,
    expected_num_series: u32,
    chunk_file_lens: [u64; 2],
}

/// Query-local pin for the independently cached fixed root.
#[derive(Debug)]
pub(crate) struct GovernedSchema6ChunkIndexRoot {
    provenance: SegmentGenerationProvenance,
    value: MetadataCachePin<Schema6ChunkIndexRootV1>,
}

impl Deref for GovernedSchema6ChunkIndexRoot {
    type Target = Schema6ChunkIndexRootV1;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// One fully checked physical span. The cached value contains no lifecycle
/// owner, read guard, descriptor, or cache pin.
#[derive(Debug)]
struct ValidatedSchema6ChunkIndexSpan {
    entries: Vec<ChunkIndexEntry>,
}

impl ValidatedSchema6ChunkIndexSpan {
    fn charged_bytes(&self) -> io::Result<u64> {
        checked_cached_span_charge(self.entries.capacity())
    }
}

/// One exact authoritative offsets-directory pair. The cached value is pure
/// decoded metadata and contains no lifecycle or descriptor ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ValidatedSchema6ChunkIndexDirectoryPair {
    series_ref: u32,
    range: ChunkIndexRange,
    entry_count: usize,
}

impl ValidatedSchema6ChunkIndexDirectoryPair {
    fn charged_bytes(self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

/// Query-local pin for one selected schema-6 series span.
#[derive(Debug)]
pub(crate) struct GovernedSchema6ChunkLocators {
    provenance: SegmentGenerationProvenance,
    series_ref: u32,
    locators: Vec<IndexedChunkLocator>,
    _source: Option<MetadataCachePin<ValidatedSchema6ChunkIndexSpan>>,
    _charge: Option<MetadataCharge>,
}

impl GovernedSchema6ChunkLocators {
    pub(crate) fn series_ref(&self) -> u32 {
        self.series_ref
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.locators.is_empty()
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.as_ref().map_or(0, |charge| charge.bytes())
    }
}

impl GovernedSchema6ChunkIndexReader {
    /// Opens the fixed root through the shared governor and verifies its
    /// series count against the independently authenticated schema-6 series
    /// root. The returned long-lived reader retains neither root pin nor read
    /// guard.
    pub(crate) fn open(
        registered: &RegisteredSegment,
        expected_num_series: u32,
    ) -> Result<Self, Schema6ChunkIndexReaderError> {
        let guard = registered.read_guard()?;
        let chunk_file_lens = [
            guard.reader(SegmentFile::Chunks)?.len(),
            guard.reader(SegmentFile::OooChunks)?.len(),
        ];
        let session = GovernedSchema6ChunkIndexSession {
            guard,
            expected_num_series,
            chunk_file_lens,
        };
        let root = session.load_root()?;
        drop(root);
        drop(session);

        Ok(Self {
            registered: registered.clone(),
            expected_num_series,
            chunk_file_lens,
        })
    }

    pub(crate) fn query_session(
        &self,
    ) -> Result<GovernedSchema6ChunkIndexSession, Schema6ChunkIndexReaderError> {
        Ok(GovernedSchema6ChunkIndexSession {
            guard: self.registered.read_guard()?,
            expected_num_series: self.expected_num_series,
            chunk_file_lens: self.chunk_file_lens,
        })
    }

    pub(crate) fn segment_identity(&self) -> &str {
        self.registered.segment_identity()
    }
}

impl GovernedSchema6ChunkIndexSession {
    pub(crate) fn ensure_same_generation(
        &self,
        guard: &SegmentReadGuard,
    ) -> Result<(), Schema6ChunkIndexReaderError> {
        if self.guard.provenance().matches(guard) {
            Ok(())
        } else {
            Err(Schema6ChunkIndexReaderError::ForeignSegmentGeneration)
        }
    }

    /// Verifies that a separately supplied root belongs to this exact query
    /// session. Callers use this even for an empty series batch so provenance
    /// cannot be skipped merely because no per-series range is touched.
    pub(crate) fn ensure_root(
        &self,
        root: &GovernedSchema6ChunkIndexRoot,
    ) -> Result<(), Schema6ChunkIndexReaderError> {
        self.ensure_provenance(&root.provenance)
    }

    /// Binds the independently decoded schema-6 series and chunk-index roots.
    /// The two readers may have been constructed with different caller-supplied
    /// expectations, so generation provenance alone is not sufficient.
    pub(crate) fn bind_series_count(
        &self,
        root: &GovernedSchema6ChunkIndexRoot,
        series_count: u32,
    ) -> Result<(), Schema6ChunkIndexReaderError> {
        self.ensure_root(root)?;
        if root.num_series == series_count {
            return Ok(());
        }
        let actual = root.num_series;
        let reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        Err(reader
            .record_validation_error(invalid_data(format!(
                "schema-6 series and chunk-index root counts disagree: series={series_count} chunk_index={actual}"
            )))
            .into())
    }

    /// Loads the exact fixed 20-byte v1 root. Selected offsets-directory pairs
    /// are loaded independently and lazily when their ranges are validated.
    pub(crate) fn load_root(
        &self,
    ) -> Result<GovernedSchema6ChunkIndexRoot, Schema6ChunkIndexReaderError> {
        let reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        let key = metadata_key(
            &reader,
            0,
            CHUNK_INDEX_ROOT_V1_LEN as u64,
            MetadataCacheClass::IndexRoot,
        )?;
        let file_len = reader.len();
        let value = reader.get_or_load(
            key,
            std::mem::size_of::<Schema6ChunkIndexRootV1>() as u64,
            move |bytes| {
                let root = decode_schema6_chunk_index_root_v1(bytes, file_len)
                    .map_err(MetadataCacheError::from_io)?;
                Ok(LoadedMetadata::new(root, root.charged_bytes()))
            },
        )?;

        if value.num_series != self.expected_num_series {
            let actual = value.num_series;
            drop(value);
            return Err(reader
                .record_validation_error(invalid_data(format!(
                    "schema-6 chunk-index series count mismatch: expected={} actual={actual}",
                    self.expected_num_series
                )))
                .into());
        }

        Ok(GovernedSchema6ChunkIndexRoot {
            provenance: self.guard.provenance(),
            value,
        })
    }

    /// Binds the per-series span carried by `SeriesEntryV2` to the exact
    /// authoritative v1 offsets pair, then reads that span.
    pub(crate) fn read_series_entries(
        &self,
        root: &GovernedSchema6ChunkIndexRoot,
        series_ref: u32,
        range: ChunkIndexRange,
    ) -> Result<GovernedSchema6ChunkLocators, Schema6ChunkIndexReaderError> {
        self.validate_series_range(root, series_ref, range)?;
        let reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        let entry_count = match validate_schema6_series_range(&root.value, series_ref, range) {
            Ok(count) => count,
            Err(error) => return Err(reader.record_validation_error(error).into()),
        };

        if entry_count == 0 {
            return Ok(GovernedSchema6ChunkLocators {
                provenance: self.guard.provenance(),
                series_ref,
                locators: Vec::new(),
                _source: None,
                _charge: None,
            });
        }

        // Reserve and allocate the query-local schema-neutral locators before
        // issuing the span read. A resource refusal is therefore transient and
        // cannot arrive after avoidable metadata I/O.
        let locator_declared =
            checked_locator_bytes(entry_count).map_err(MetadataCacheError::from_io)?;
        let mut locator_charge = reader
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(locator_declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let mut locators = Vec::new();
        locators.try_reserve_exact(entry_count).map_err(|error| {
            MetadataCacheError::transient(
                io::ErrorKind::OutOfMemory,
                format!("failed to allocate schema-6 chunk locators: {error}"),
            )
        })?;
        locator_charge
            .reconcile(
                checked_locator_bytes(locators.capacity()).map_err(MetadataCacheError::from_io)?,
            )
            .map_err(MetadataCacheError::from)?;

        let key = metadata_key(
            &reader,
            range.offset,
            u64::from(range.len),
            MetadataCacheClass::IndexPage,
        )?;
        let declared =
            checked_cached_span_charge(entry_count).map_err(MetadataCacheError::from_io)?;
        let chunk_file_lens = self.chunk_file_lens;
        let value = reader.get_or_load_owned(key, declared, move |bytes| {
            let value = decode_schema6_chunk_index_span(bytes, entry_count, chunk_file_lens)
                .map_err(MetadataCacheError::from_io)?;
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;

        for entry in &value.entries {
            // Cache identity is the physical span, independent of the series
            // that referenced it. Bind the already validated entries to the
            // authenticated series_ref in query-local scratch.
            locators.push(
                IndexedChunkLocator::try_schema6_v1(series_ref, entry.clone()).map_err(
                    |error| {
                        reader.record_validation_error(invalid_data(format!(
                            "invalid cached schema-6 chunk locator: {error}"
                        )))
                    },
                )?,
            );
        }

        Ok(GovernedSchema6ChunkLocators {
            provenance: self.guard.provenance(),
            series_ref,
            locators,
            _source: Some(value),
            _charge: Some(locator_charge),
        })
    }

    /// Verifies that a series-metadata span is exactly the authoritative span
    /// encoded by the schema-6 chunk-index offsets directory. This does not
    /// read or decode the chunk-entry body.
    pub(crate) fn validate_series_range(
        &self,
        root: &GovernedSchema6ChunkIndexRoot,
        series_ref: u32,
        range: ChunkIndexRange,
    ) -> Result<(), Schema6ChunkIndexReaderError> {
        self.ensure_root(root)?;
        let reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        let directory = self.load_directory_pair(&reader, root, series_ref)?;
        if directory.series_ref != series_ref || directory.range != range {
            let authoritative = directory.range;
            drop(directory);
            return Err(reader
                .record_validation_error(invalid_data(format!(
                    "schema-6 series chunk-index span disagrees with its authoritative directory pair: series_ref={series_ref} series_range={range:?} directory_range={authoritative:?}"
                )))
                .into());
        }
        Ok(())
    }

    /// Returns raw locators only while the matching generation session is
    /// borrowed. Callers cannot consume a detached batch without rechecking
    /// its provenance against an active read guard.
    pub(crate) fn locators<'a>(
        &'a self,
        locators: &'a GovernedSchema6ChunkLocators,
    ) -> Result<&'a [IndexedChunkLocator], Schema6ChunkIndexReaderError> {
        self.ensure_provenance(&locators.provenance)?;
        Ok(&locators.locators)
    }

    fn load_directory_pair(
        &self,
        reader: &GovernedArtifactReader,
        root: &GovernedSchema6ChunkIndexRoot,
        series_ref: u32,
    ) -> Result<
        MetadataCachePin<ValidatedSchema6ChunkIndexDirectoryPair>,
        Schema6ChunkIndexReaderError,
    > {
        let pair_offset = match schema6_directory_pair_offset(&root.value, series_ref) {
            Ok(offset) => offset,
            Err(error) => return Err(reader.record_validation_error(error).into()),
        };
        let key = metadata_key(
            reader,
            pair_offset,
            CHUNK_INDEX_DIRECTORY_PAIR_LEN,
            MetadataCacheClass::IndexDirectory,
        )?;
        let root = *root.value;
        let value = reader.get_or_load(
            key,
            std::mem::size_of::<ValidatedSchema6ChunkIndexDirectoryPair>() as u64,
            move |bytes| {
                let pair = decode_schema6_chunk_index_directory_pair(bytes, root, series_ref)
                    .map_err(MetadataCacheError::from_io)?;
                Ok(LoadedMetadata::new(pair, pair.charged_bytes()))
            },
        )?;
        if value.series_ref != series_ref {
            drop(value);
            return Err(reader
                .record_validation_error(invalid_data(
                    "schema-6 cached chunk-index directory pair belongs to another series",
                ))
                .into());
        }
        Ok(value)
    }

    fn ensure_provenance(
        &self,
        provenance: &SegmentGenerationProvenance,
    ) -> Result<(), Schema6ChunkIndexReaderError> {
        if provenance.matches(&self.guard) {
            Ok(())
        } else {
            Err(Schema6ChunkIndexReaderError::ForeignSegmentGeneration)
        }
    }
}

fn decode_schema6_chunk_index_root_v1(
    bytes: &[u8],
    actual_file_len: u64,
) -> io::Result<Schema6ChunkIndexRootV1> {
    if bytes.len() != CHUNK_INDEX_ROOT_V1_LEN {
        return Err(if bytes.len() < CHUNK_INDEX_ROOT_V1_LEN {
            unexpected_eof("schema-6 chunk-index root is truncated")
        } else {
            invalid_data("schema-6 chunk-index root has trailing bytes")
        });
    }
    if read_u32(bytes, 0) != CHUNK_INDEX_MAGIC {
        return Err(invalid_data("schema-6 chunk-index magic mismatch"));
    }
    if read_u16(bytes, 4) != CHUNK_INDEX_VERSION_V1 {
        return Err(invalid_data("unsupported schema-6 chunk-index version"));
    }
    if read_u16(bytes, 6) != 0 {
        return Err(invalid_data("schema-6 chunk-index flags must be zero"));
    }

    let num_series = read_u32(bytes, 8);
    let offset_count = u64::from(num_series)
        .checked_add(1)
        .ok_or_else(|| invalid_data("schema-6 chunk-index offset count overflows"))?;
    let directory_len = offset_count
        .checked_mul(8)
        .ok_or_else(|| invalid_data("schema-6 chunk-index directory length overflows"))?;
    let data_start = CHUNK_INDEX_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| invalid_data("schema-6 chunk-index data offset overflows"))?;
    if read_u64(bytes, 12) != data_start {
        return Err(invalid_data(
            "schema-6 chunk-index first offset is not canonical",
        ));
    }
    if actual_file_len < data_start {
        return Err(unexpected_eof(
            "schema-6 chunk-index file is shorter than its offsets directory",
        ));
    }
    if num_series == 0 && actual_file_len != data_start {
        return Err(invalid_data(
            "empty schema-6 chunk index must end at its terminal first offset",
        ));
    }
    if !(actual_file_len - data_start).is_multiple_of(CHUNK_ENTRY_LEN as u64) {
        return Err(invalid_data(
            "schema-6 chunk-index file length is not entry aligned",
        ));
    }

    Ok(Schema6ChunkIndexRootV1 {
        num_series,
        data_start,
        file_len: actual_file_len,
    })
}

fn schema6_directory_pair_offset(
    root: &Schema6ChunkIndexRootV1,
    series_ref: u32,
) -> io::Result<u64> {
    if series_ref >= root.num_series {
        return Err(invalid_data(format!(
            "schema-6 chunk-index series_ref is out of range: series_ref={series_ref} num_series={}",
            root.num_series
        )));
    }
    let offset = u64::from(series_ref)
        .checked_mul(8)
        .and_then(|relative| CHUNK_INDEX_HEADER_LEN.checked_add(relative))
        .ok_or_else(|| invalid_data("schema-6 chunk-index directory pair offset overflows"))?;
    let end = offset
        .checked_add(CHUNK_INDEX_DIRECTORY_PAIR_LEN)
        .ok_or_else(|| invalid_data("schema-6 chunk-index directory pair end overflows"))?;
    if end > root.data_start {
        return Err(invalid_data(
            "schema-6 chunk-index directory pair exceeds the offsets directory",
        ));
    }
    Ok(offset)
}

fn decode_schema6_chunk_index_directory_pair(
    bytes: &[u8],
    root: Schema6ChunkIndexRootV1,
    series_ref: u32,
) -> io::Result<ValidatedSchema6ChunkIndexDirectoryPair> {
    schema6_directory_pair_offset(&root, series_ref)?;
    let expected_len = usize::try_from(CHUNK_INDEX_DIRECTORY_PAIR_LEN)
        .map_err(|_| invalid_data("schema-6 chunk-index directory pair length exceeds usize"))?;
    if bytes.len() != expected_len {
        return Err(if bytes.len() < expected_len {
            unexpected_eof("schema-6 chunk-index directory pair is truncated")
        } else {
            invalid_data("schema-6 chunk-index directory pair has trailing bytes")
        });
    }

    let start = read_u64(bytes, 0);
    let end = read_u64(bytes, 8);
    if end < start {
        return Err(invalid_data(
            "schema-6 chunk-index directory offsets are out of order",
        ));
    }
    if start < root.data_start {
        return Err(invalid_data(
            "schema-6 chunk-index directory span starts inside the offsets directory",
        ));
    }
    if end > root.file_len {
        return Err(unexpected_eof(
            "schema-6 chunk-index directory span exceeds the registered file",
        ));
    }
    if series_ref == 0 && start != root.data_start {
        return Err(invalid_data(
            "schema-6 chunk-index first directory offset is not canonical",
        ));
    }
    if series_ref
        .checked_add(1)
        .is_some_and(|next| next == root.num_series)
        && end != root.file_len
    {
        return Err(invalid_data(
            "schema-6 chunk-index terminal directory offset does not match the file length",
        ));
    }
    if !(start - root.data_start).is_multiple_of(CHUNK_ENTRY_LEN as u64)
        || !(end - root.data_start).is_multiple_of(CHUNK_ENTRY_LEN as u64)
    {
        return Err(invalid_data(
            "schema-6 chunk-index directory span is not entry aligned",
        ));
    }

    let len = u32::try_from(end - start)
        .map_err(|_| invalid_data("schema-6 chunk-index directory span exceeds u32"))?;
    let range = ChunkIndexRange { offset: start, len };
    let entry_count = validate_schema6_series_range(&root, series_ref, range)?;
    Ok(ValidatedSchema6ChunkIndexDirectoryPair {
        series_ref,
        range,
        entry_count,
    })
}

fn validate_schema6_series_range(
    root: &Schema6ChunkIndexRootV1,
    series_ref: u32,
    range: ChunkIndexRange,
) -> io::Result<usize> {
    if series_ref >= root.num_series {
        return Err(invalid_data(format!(
            "schema-6 chunk-index series_ref is out of range: series_ref={series_ref} num_series={}",
            root.num_series
        )));
    }
    if range.offset < root.data_start {
        return Err(invalid_data(
            "schema-6 series chunk-index span starts inside the offsets directory",
        ));
    }
    let relative_offset = range.offset - root.data_start;
    if !relative_offset.is_multiple_of(CHUNK_ENTRY_LEN as u64) {
        return Err(invalid_data(
            "schema-6 series chunk-index span offset is not entry aligned",
        ));
    }
    if !usize::try_from(range.len)
        .map_err(|_| invalid_data("schema-6 series chunk-index span exceeds usize"))?
        .is_multiple_of(CHUNK_ENTRY_LEN)
    {
        return Err(invalid_data(
            "schema-6 series chunk-index span length is not entry aligned",
        ));
    }
    let end = range
        .offset
        .checked_add(u64::from(range.len))
        .ok_or_else(|| invalid_data("schema-6 series chunk-index span overflows"))?;
    if end > root.file_len {
        return Err(unexpected_eof(
            "schema-6 series chunk-index span exceeds the registered file",
        ));
    }

    usize::try_from(range.len / CHUNK_ENTRY_LEN as u32)
        .map_err(|_| invalid_data("schema-6 series chunk count exceeds usize"))
}

fn decode_schema6_chunk_index_span(
    bytes: Vec<u8>,
    expected_entry_count: usize,
    chunk_file_lens: [u64; 2],
) -> io::Result<ValidatedSchema6ChunkIndexSpan> {
    let expected_len = expected_entry_count
        .checked_mul(CHUNK_ENTRY_LEN)
        .ok_or_else(|| invalid_data("schema-6 chunk-index span length overflows"))?;
    if bytes.len() != expected_len {
        return Err(if bytes.len() < expected_len {
            unexpected_eof("schema-6 chunk-index span is truncated")
        } else {
            invalid_data("schema-6 chunk-index span has trailing bytes")
        });
    }

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(expected_entry_count)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("schema-6 chunk-index entry allocation failed: {error}"),
            )
        })?;
    let mut previous_by_file: [Option<(u64, u64, u64)>; 2] = [None, None];
    for entry_bytes in bytes.chunks_exact(CHUNK_ENTRY_LEN) {
        let entry = decode_schema6_chunk_entry_v1(entry_bytes)?;
        let locator = IndexedChunkLocator::try_schema6_v1(0, entry.clone())
            .map_err(|error| invalid_data(format!("invalid schema-6 chunk locator: {error}")))?;
        let file_index = usize::from(locator.entry().file_id);
        let payload_end = locator
            .entry()
            .offset
            .checked_add(u64::from(locator.entry().length))
            .ok_or_else(|| invalid_data("schema-6 chunk locator file range overflows"))?;
        if payload_end > chunk_file_lens[file_index] {
            return Err(unexpected_eof(format!(
                "schema-6 chunk locator exceeds {:?}: end={payload_end} file_len={}",
                if file_index == 0 {
                    SegmentFile::Chunks
                } else {
                    SegmentFile::OooChunks
                },
                chunk_file_lens[file_index]
            )));
        }

        let order_key = (
            locator.entry().min_time_ms,
            locator.entry().max_time_ms,
            locator.entry().offset,
        );
        if previous_by_file[file_index].is_some_and(|previous| order_key < previous) {
            return Err(invalid_data(
                "schema-6 chunk-index entries are out of order within a file lane",
            ));
        }
        previous_by_file[file_index] = Some(order_key);
        entries.push(entry);
    }
    if entries.len() != expected_entry_count {
        return Err(invalid_data(
            "schema-6 decoded chunk count does not match the series span",
        ));
    }

    Ok(ValidatedSchema6ChunkIndexSpan { entries })
}

fn decode_schema6_chunk_entry_v1(bytes: &[u8]) -> io::Result<ChunkIndexEntry> {
    if bytes.len() != CHUNK_ENTRY_LEN {
        return Err(unexpected_eof("schema-6 chunk-index entry is truncated"));
    }
    Ok(ChunkIndexEntry {
        file_id: bytes[0],
        kind: super::codec::chunk_kind_from_u8(bytes[1])?,
        flags: read_u16(bytes, 2),
        min_time_ms: read_u64(bytes, 4),
        max_time_ms: read_u64(bytes, 12),
        offset: read_u64(bytes, 20),
        length: read_u32(bytes, 28),
        scalar_lane_offset: read_u32(bytes, 32),
        scalar_lane_len: read_u32(bytes, 36),
    })
}

fn checked_cached_span_charge(entry_count: usize) -> io::Result<u64> {
    let vector_bytes = entry_count
        .checked_mul(std::mem::size_of::<ChunkIndexEntry>())
        .ok_or_else(|| invalid_data("schema-6 chunk-locator allocation charge overflows"))?;
    let total = std::mem::size_of::<ValidatedSchema6ChunkIndexSpan>()
        .checked_add(vector_bytes)
        .ok_or_else(|| invalid_data("schema-6 chunk-locator allocation charge overflows"))?;
    u64::try_from(total)
        .map_err(|_| invalid_data("schema-6 chunk-locator allocation charge exceeds u64"))
}

fn checked_locator_bytes(entry_count: usize) -> io::Result<u64> {
    let bytes = entry_count
        .checked_mul(std::mem::size_of::<IndexedChunkLocator>())
        .ok_or_else(|| invalid_data("schema-6 query-locator allocation charge overflows"))?;
    u64::try_from(bytes)
        .map_err(|_| invalid_data("schema-6 query-locator allocation charge exceeds u64"))
}

fn metadata_key(
    reader: &GovernedArtifactReader,
    offset: u64,
    length: u64,
    class: MetadataCacheClass,
) -> Result<MetadataCacheKey, MetadataCacheKeyError> {
    reader.metadata_cache_key(offset, length, class)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn unexpected_eof(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, message.into())
}

#[cfg(test)]
mod tests;
