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
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use crate::storage::metadata_governor::{MetadataGovernorConfig, MetadataUsageClass};
    use crate::storage::metadata_runtime::{
        MetadataIssuedReadCount, SegmentArtifactRegistration, StoreMetadataRuntime,
    };
    use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;

    use super::*;
    use crate::storage::chunk::{
        CHUNK_HEADER_LEN, ChunkKind, chunk_index_ranges, write_chunk_index,
    };

    const CHUNKS_LEN: usize = 4096;
    const OOO_CHUNKS_LEN: usize = 2048;

    struct Fixture {
        _directory: TempDir,
        runtime: StoreMetadataRuntime,
        registered: Option<RegisteredSegment>,
        ranges: Vec<ChunkIndexRange>,
        chunk_index_path: std::path::PathBuf,
    }

    fn runtime(retained_max_bytes: u64, in_flight_max_bytes: u64) -> StoreMetadataRuntime {
        StoreMetadataRuntime::new(MetadataGovernorConfig {
            retained_max_bytes,
            in_flight_max_bytes,
            max_open_files: 1,
            max_cached_open_files: 0,
        })
        .expect("valid schema-6 chunk-index test runtime")
    }

    fn entry(file_id: u8, time_ms: u64, offset: u64) -> ChunkIndexEntry {
        ChunkIndexEntry {
            file_id,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: time_ms,
            max_time_ms: time_ms + 1,
            offset,
            length: CHUNK_HEADER_LEN as u32,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }
    }

    fn default_entries() -> Vec<Vec<ChunkIndexEntry>> {
        let mut scalar = entry(0, 100, 64);
        scalar.flags = 0xa5a5;
        let mut typed = entry(0, 200, 128);
        typed.kind = ChunkKind::Histogram;
        typed.flags = 0x5a5a;
        vec![vec![scalar, typed], vec![entry(1, 300, 256)]]
    }

    fn encoded_index(entries: &[Vec<ChunkIndexEntry>]) -> (Vec<u8>, Vec<ChunkIndexRange>) {
        let ranges = chunk_index_ranges(entries).expect("compute schema-6 chunk-index ranges");
        let mut bytes = Vec::new();
        write_chunk_index(&mut bytes, entries).expect("encode schema-6 chunk index");
        (bytes, ranges)
    }

    fn fixture(
        identity: &str,
        runtime: StoreMetadataRuntime,
        chunk_index: Vec<u8>,
        ranges: Vec<ChunkIndexRange>,
    ) -> Fixture {
        let directory = TempDir::new().expect("create schema-6 chunk-index fixture directory");
        let mut chunk_index_path = None;
        let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
            let path = directory.path().join(file.filename());
            match file {
                SegmentFile::MetaJson => fs::write(&path, b"{}").expect("write meta fixture"),
                SegmentFile::Symbols => {
                    fs::write(&path, b"symbols").expect("write symbols fixture")
                }
                SegmentFile::Series => fs::write(&path, b"series").expect("write series fixture"),
                SegmentFile::Chunks => {
                    fs::write(&path, vec![0; CHUNKS_LEN]).expect("write chunks fixture")
                }
                SegmentFile::OooChunks => {
                    fs::write(&path, vec![0; OOO_CHUNKS_LEN]).expect("write OOO fixture")
                }
                SegmentFile::ChunkIndex => {
                    fs::write(&path, &chunk_index).expect("write chunk-index fixture");
                    chunk_index_path = Some(path.clone());
                }
                SegmentFile::Indexes => {
                    fs::write(&path, b"indexes").expect("write indexes fixture")
                }
                SegmentFile::Footer => unreachable!("footer is not runtime-inventoried"),
            }
            let len = fs::metadata(&path).expect("stat fixture artifact").len();
            SegmentArtifactRegistration::new(file, path, len)
        });
        let registered = runtime
            .register_segment(identity, &artifacts)
            .expect("register schema-6 chunk-index fixture");
        Fixture {
            _directory: directory,
            runtime,
            registered: Some(registered),
            ranges,
            chunk_index_path: chunk_index_path.expect("chunk-index path captured"),
        }
    }

    fn standard_fixture(
        identity: &str,
        retained_max_bytes: u64,
        in_flight_max_bytes: u64,
    ) -> Fixture {
        let entries = default_entries();
        let (bytes, ranges) = encoded_index(&entries);
        fixture(
            identity,
            runtime(retained_max_bytes, in_flight_max_bytes),
            bytes,
            ranges,
        )
    }

    fn open_reader(fixture: &Fixture) -> GovernedSchema6ChunkIndexReader {
        GovernedSchema6ChunkIndexReader::open(
            fixture.registered.as_ref().expect("fixture owner exists"),
            fixture.ranges.len() as u32,
        )
        .expect("open governed schema-6 chunk-index reader")
    }

    fn class_reads(
        runtime: &StoreMetadataRuntime,
        class: MetadataCacheClass,
    ) -> MetadataIssuedReadCount {
        runtime.snapshot().reads.classes[class.stable_index()].issued
    }

    fn delta(
        after: MetadataIssuedReadCount,
        before: MetadataIssuedReadCount,
    ) -> MetadataIssuedReadCount {
        MetadataIssuedReadCount {
            calls: after.calls - before.calls,
            bytes: after.bytes - before.bytes,
        }
    }

    #[test]
    fn root_decoder_is_exact_and_strict() {
        let entries = default_entries();
        let (bytes, ranges) = encoded_index(&entries);
        let root_bytes = &bytes[..CHUNK_INDEX_ROOT_V1_LEN];
        let root = decode_schema6_chunk_index_root_v1(root_bytes, bytes.len() as u64)
            .expect("decode valid schema-6 root");
        assert_eq!(root.num_series, 2);
        assert_eq!(root.data_start, 36);
        assert_eq!(root.file_len, bytes.len() as u64);

        for (offset, replacement) in [
            (0, 0_u32.to_le_bytes().to_vec()),
            (4, 2_u16.to_le_bytes().to_vec()),
            (6, 1_u16.to_le_bytes().to_vec()),
            (12, 35_u64.to_le_bytes().to_vec()),
        ] {
            let mut malformed = root_bytes.to_vec();
            malformed[offset..offset + replacement.len()].copy_from_slice(&replacement);
            assert!(
                decode_schema6_chunk_index_root_v1(&malformed, bytes.len() as u64).is_err(),
                "root mutation at byte {offset} must fail"
            );
        }
        assert!(decode_schema6_chunk_index_root_v1(&root_bytes[..19], bytes.len() as u64).is_err());
        assert!(decode_schema6_chunk_index_root_v1(root_bytes, bytes.len() as u64 - 1).is_err());
        assert!(decode_schema6_chunk_index_root_v1(root_bytes, 19).is_err());

        let (empty, empty_ranges) = encoded_index(&[]);
        assert!(empty_ranges.is_empty());
        assert_eq!(empty.len(), CHUNK_INDEX_ROOT_V1_LEN);
        let empty_root = decode_schema6_chunk_index_root_v1(&empty, empty.len() as u64)
            .expect("decode canonical empty schema-6 chunk index");
        assert_eq!(empty_root.num_series, 0);
        assert_eq!(empty_root.data_start, CHUNK_INDEX_ROOT_V1_LEN as u64);
        let mut empty_with_body = empty.clone();
        empty_with_body.extend_from_slice(&[0; CHUNK_ENTRY_LEN]);
        assert!(
            decode_schema6_chunk_index_root_v1(
                &empty_with_body[..CHUNK_INDEX_ROOT_V1_LEN],
                empty_with_body.len() as u64,
            )
            .is_err()
        );

        let pair_offset = usize::try_from(
            schema6_directory_pair_offset(&root, 1).expect("compute second directory pair"),
        )
        .expect("directory pair offset fits usize");
        let pair_bytes = &bytes[pair_offset..pair_offset + 16];
        let pair = decode_schema6_chunk_index_directory_pair(pair_bytes, root, 1)
            .expect("decode exact second directory pair");
        assert_eq!(pair.series_ref, 1);
        assert_eq!(pair.range, ranges[1]);
        assert_eq!(pair.entry_count, 1);

        let truncated = decode_schema6_chunk_index_directory_pair(&pair_bytes[..15], root, 1)
            .expect_err("truncated directory pair must fail");
        assert_eq!(truncated.kind(), io::ErrorKind::UnexpectedEof);
        let mut trailing = pair_bytes.to_vec();
        trailing.push(0);
        let trailing = decode_schema6_chunk_index_directory_pair(&trailing, root, 1)
            .expect_err("directory pair with trailing bytes must fail");
        assert_eq!(trailing.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn exact_root_and_series_spans_are_cached_and_reused() {
        let fixture = standard_fixture("schema6-cached-spans", 1024 * 1024, 1024 * 1024);
        let before_open = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
        let reader = open_reader(&fixture);
        assert_eq!(reader.segment_identity(), "schema6-cached-spans");
        assert_eq!(
            delta(
                class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
                before_open
            ),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_INDEX_ROOT_V1_LEN as u64,
            }
        );

        let session = reader.query_session().expect("open schema-6 query session");
        let root = session.load_root().expect("reuse governed root");
        assert_eq!(root.data_start, 36);
        let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        session
            .validate_series_range(&root, 0, fixture.ranges[0])
            .expect("validate authoritative range without reading its body");
        let after_validation_directory =
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        assert_eq!(
            delta(after_validation_directory, before_directory),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
            }
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            before_span
        );

        let first = session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .expect("read exact first-series span");
        assert_eq!(first.series_ref(), 0);
        let first_locators = session
            .locators(&first)
            .expect("consume first locators through their owning session");
        assert_eq!(first_locators.len(), 2);
        assert_eq!(first_locators[0].payload_identity(), (0, 64, 40));
        assert_eq!(first_locators[1].payload_identity(), (0, 128, 40));
        assert_eq!(first_locators[0].entry().kind, ChunkKind::Float);
        assert_eq!(first_locators[0].entry().flags, 0xa5a5);
        assert_eq!(first_locators[1].entry().kind, ChunkKind::Histogram);
        assert_eq!(first_locators[1].entry().flags, 0x5a5a);
        assert!(first.charged_bytes() > 0);
        let after_first_directory =
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        assert_eq!(after_first_directory, after_validation_directory);
        let after_first = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        assert_eq!(
            delta(after_first, before_span),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: u64::from(fixture.ranges[0].len),
            }
        );

        let second = session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .expect("reuse exact first-series span");
        assert_eq!(
            session
                .locators(&second)
                .expect("consume reused locators through their owning session"),
            first_locators
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
            after_first_directory
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            after_first
        );
    }

    #[test]
    fn zero_retention_releases_directory_and_span_pins_and_reissues_exact_reads() {
        let fixture = standard_fixture("schema6-zero-retention", 0, 1024 * 1024);
        let reader = open_reader(&fixture);
        let session = reader.query_session().expect("open schema-6 query session");
        let root = session.load_root().expect("load query-local root");
        let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        {
            let first = session
                .read_series_entries(&root, 1, fixture.ranges[1])
                .expect("read transient span");
            assert_eq!(
                session
                    .locators(&first)
                    .expect("consume transient locators through their owning session")[0]
                    .payload_identity(),
                (1, 256, 40)
            );
        }
        let middle_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        assert_eq!(
            delta(middle_directory, before_directory),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
            }
        );
        let middle_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        assert_eq!(
            delta(middle_span, before_span),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: u64::from(fixture.ranges[1].len),
            }
        );
        let second = session
            .read_series_entries(&root, 1, fixture.ranges[1])
            .expect("reload released transient span");
        assert_eq!(
            session
                .locators(&second)
                .expect("consume reloaded locators through their owning session")
                .len(),
            1
        );
        assert_eq!(
            delta(
                class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
                middle_directory,
            ),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
            }
        );
        assert_eq!(
            delta(
                class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
                middle_span
            ),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: u64::from(fixture.ranges[1].len),
            }
        );
    }

    #[test]
    fn tiny_budget_refusals_before_directory_and_body_io_are_retryable() {
        let fixture = standard_fixture("schema6-budget", 1024 * 1024, 4096);
        let reader = open_reader(&fixture);
        let session = reader.query_session().expect("open schema-6 query session");
        let root = session.load_root().expect("load cached root");
        let blocker = fixture
            .runtime
            .governor()
            .reserve_in_flight_for_usage(4096, MetadataUsageClass::Scratch)
            .expect("reserve competing in-flight bytes");
        let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        let error = session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .expect_err("tiny budget must refuse directory pair before I/O");
        assert!(matches!(
            error,
            Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Budget(_))
        ));
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
            before_directory
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            before_span
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

        drop(blocker);
        session
            .validate_series_range(&root, 0, fixture.ranges[0])
            .expect("load authoritative directory pair after budget is released");
        let cached_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        assert_eq!(delta(cached_directory, before_directory).calls, 1);

        let blocker = fixture
            .runtime
            .governor()
            .reserve_in_flight_for_usage(3000, MetadataUsageClass::Scratch)
            .expect("reserve competing in-flight bytes after caching the directory pair");
        let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        let error = session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .expect_err("tiny budget must refuse locator/body work before body I/O");
        assert!(matches!(
            error,
            Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Budget(_))
        ));
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
            cached_directory
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            before_span
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

        drop(blocker);
        let retried = session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .expect("budget refusal must be retryable");
        assert_eq!(
            session
                .locators(&retried)
                .expect("consume retried locators through their owning session")
                .len(),
            2
        );
    }

    #[test]
    fn authoritative_directory_rejects_aligned_swapped_and_shifted_ranges_before_body_io() {
        {
            let fixture = standard_fixture("schema6-swapped-range", 1024 * 1024, 1024 * 1024);
            let reader = open_reader(&fixture);
            let session = reader.query_session().expect("open swapped-range session");
            let root = session.load_root().expect("load swapped-range root");
            let swapped = fixture.ranges[1];
            assert!(validate_schema6_series_range(&root, 0, swapped).is_ok());
            let before_directory =
                class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
            let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

            let error = session
                .read_series_entries(&root, 0, swapped)
                .expect_err("locally valid swapped range must disagree with the directory");
            assert!(matches!(
                error,
                Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
            ));
            let after_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
            assert_eq!(
                delta(after_directory, before_directory),
                MetadataIssuedReadCount {
                    calls: 1,
                    bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
                }
            );
            assert_eq!(
                class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
                before_span
            );
            assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

            fixture.runtime.evict_all_resident_metadata();
            assert!(session.read_series_entries(&root, 0, swapped).is_err());
            assert_eq!(
                class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
                after_directory
            );
        }

        {
            let fixture = standard_fixture("schema6-shifted-range", 1024 * 1024, 1024 * 1024);
            let reader = open_reader(&fixture);
            let session = reader.query_session().expect("open shifted-range session");
            let root = session.load_root().expect("load shifted-range root");
            let shifted = ChunkIndexRange {
                offset: fixture.ranges[0].offset + CHUNK_ENTRY_LEN as u64,
                len: CHUNK_ENTRY_LEN as u32,
            };
            assert!(validate_schema6_series_range(&root, 0, shifted).is_ok());
            let before_directory =
                class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
            let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

            let error = session
                .read_series_entries(&root, 0, shifted)
                .expect_err("locally valid shifted range must disagree with the directory");
            assert!(matches!(
                error,
                Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
            ));
            assert_eq!(
                delta(
                    class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
                    before_directory,
                ),
                MetadataIssuedReadCount {
                    calls: 1,
                    bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
                }
            );
            assert_eq!(
                class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
                before_span
            );
            assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
        }
    }

    #[test]
    fn malformed_directory_ordering_is_sticky_and_never_reads_the_body() {
        let entries = default_entries();
        let (mut bytes, ranges) = encoded_index(&entries);
        bytes[20..28].copy_from_slice(&(ranges[0].offset - 1).to_le_bytes());
        let fixture = fixture(
            "schema6-sticky-directory-ordering",
            runtime(0, 1024 * 1024),
            bytes,
            ranges,
        );
        let reader = open_reader(&fixture);
        let session = reader
            .query_session()
            .expect("open malformed-directory session");
        let root = session.load_root().expect("load malformed-directory root");
        let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

        let error = session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .expect_err("out-of-order directory pair must fail");
        assert!(matches!(
            error,
            Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        assert_eq!(
            delta(after_directory, before_directory),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
            }
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            before_span
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

        fixture.runtime.evict_all_resident_metadata();
        assert!(
            session
                .read_series_entries(&root, 0, fixture.ranges[0])
                .is_err()
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
            after_directory
        );
    }

    #[test]
    fn root_count_range_and_entry_validation_are_strict() {
        let fixture = standard_fixture("schema6-strict", 1024 * 1024, 1024 * 1024);
        let count_error = GovernedSchema6ChunkIndexReader::open(
            fixture.registered.as_ref().expect("fixture owner exists"),
            3,
        )
        .err()
        .expect("cross-root series count mismatch must fail");
        assert!(matches!(
            count_error,
            Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
        ));

        let entries = default_entries();
        let (bytes, ranges) = encoded_index(&entries);
        let root = decode_schema6_chunk_index_root_v1(
            &bytes[..CHUNK_INDEX_ROOT_V1_LEN],
            bytes.len() as u64,
        )
        .expect("decode valid root");
        assert!(
            validate_schema6_series_range(
                &root,
                0,
                ChunkIndexRange {
                    offset: ranges[0].offset + 1,
                    len: ranges[0].len,
                },
            )
            .is_err()
        );
        assert!(
            validate_schema6_series_range(
                &root,
                0,
                ChunkIndexRange {
                    offset: ranges[0].offset,
                    len: ranges[0].len - 1,
                },
            )
            .is_err()
        );

        let mut invalid_file = bytes
            [ranges[0].offset as usize..(ranges[0].offset + u64::from(ranges[0].len)) as usize]
            .to_vec();
        assert!(
            decode_schema6_chunk_index_span(
                invalid_file.clone(),
                1,
                [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],
            )
            .is_err()
        );
        invalid_file[0] = 2;
        assert!(
            decode_schema6_chunk_index_span(
                invalid_file,
                2,
                [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],
            )
            .is_err()
        );

        let mut out_of_bounds = bytes
            [ranges[0].offset as usize..(ranges[0].offset + u64::from(ranges[0].len)) as usize]
            .to_vec();
        out_of_bounds[20..28].copy_from_slice(&(CHUNKS_LEN as u64).to_le_bytes());
        assert!(
            decode_schema6_chunk_index_span(
                out_of_bounds,
                2,
                [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],
            )
            .is_err()
        );

        let mut reversed = bytes
            [ranges[0].offset as usize..(ranges[0].offset + u64::from(ranges[0].len)) as usize]
            .to_vec();
        let (first, second) = reversed.split_at_mut(CHUNK_ENTRY_LEN);
        first.swap_with_slice(second);
        assert!(
            decode_schema6_chunk_index_span(
                reversed,
                2,
                [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],
            )
            .is_err()
        );
    }

    #[test]
    fn touched_corruption_and_truncation_are_sticky() {
        let entries = default_entries();
        let (mut bytes, ranges) = encoded_index(&entries);
        bytes[ranges[0].offset as usize] = 2;
        let fixture = fixture(
            "schema6-sticky-corruption",
            runtime(0, 1024 * 1024),
            bytes,
            ranges,
        );
        let reader = open_reader(&fixture);
        let session = reader.query_session().expect("open query session");
        let root = session.load_root().expect("load root");
        let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        assert!(
            session
                .read_series_entries(&root, 0, fixture.ranges[0])
                .is_err()
        );
        let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
        assert_eq!(delta(after, before).calls, 1);
        fixture.runtime.evict_all_resident_metadata();
        assert!(
            session
                .read_series_entries(&root, 0, fixture.ranges[0])
                .is_err()
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            after
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

        let truncation = standard_fixture("schema6-sticky-truncation", 0, 1024 * 1024);
        let truncation_reader = open_reader(&truncation);
        let truncation_session = truncation_reader
            .query_session()
            .expect("open truncation session");
        let truncation_root = truncation_session
            .load_root()
            .expect("load truncation root");
        let file_len = fs::metadata(&truncation.chunk_index_path)
            .expect("stat chunk index")
            .len();
        fs::OpenOptions::new()
            .write(true)
            .open(&truncation.chunk_index_path)
            .expect("open chunk index for truncation")
            .set_len(file_len - 1)
            .expect("truncate chunk-index fixture");
        assert!(
            truncation_session
                .read_series_entries(&truncation_root, 0, truncation.ranges[0])
                .is_err()
        );
        assert_eq!(truncation.runtime.snapshot().cache.sticky_artifacts, 1);
    }

    #[test]
    fn reader_owner_and_query_guard_have_explicit_lifetimes() {
        let mut fixture = standard_fixture("schema6-owner", 0, 1024 * 1024);
        let reader = open_reader(&fixture);
        drop(fixture.registered.take());
        assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 7);

        let session = reader.query_session().expect("open guarded session");
        let root = session.load_root().expect("load guarded root");
        let locators = session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .expect("load guarded locators");
        assert_eq!(
            session
                .locators(&locators)
                .expect("locators retain matching provenance")
                .len(),
            2
        );
        drop(reader);
        assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 7);

        drop(locators);
        drop(root);
        drop(session);
        assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 0);
        assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    }

    #[test]
    fn chunk_index_session_rejects_a_foreign_generation_without_io_or_poisoning() {
        let shared_runtime = runtime(0, 1024 * 1024);
        let entries = default_entries();
        let (first_bytes, first_ranges) = encoded_index(&entries);
        let first = fixture(
            "schema6-session-generation-first",
            shared_runtime.clone(),
            first_bytes,
            first_ranges,
        );
        let (second_bytes, second_ranges) = encoded_index(&entries);
        let second = fixture(
            "schema6-session-generation-second",
            shared_runtime,
            second_bytes,
            second_ranges,
        );
        let reader = open_reader(&first);
        let session = reader.query_session().expect("open first query session");
        let own_guard = first
            .registered
            .as_ref()
            .expect("first fixture owner exists")
            .read_guard()
            .expect("open own read guard");
        let foreign_guard = second
            .registered
            .as_ref()
            .expect("second fixture owner exists")
            .read_guard()
            .expect("open foreign read guard");
        let before_root = class_reads(&first.runtime, MetadataCacheClass::IndexRoot);
        let before_directory = class_reads(&first.runtime, MetadataCacheClass::IndexDirectory);
        let before_page = class_reads(&first.runtime, MetadataCacheClass::IndexPage);

        session
            .ensure_same_generation(&own_guard)
            .expect("own generation must match");
        assert!(matches!(
            session.ensure_same_generation(&foreign_guard),
            Err(Schema6ChunkIndexReaderError::ForeignSegmentGeneration)
        ));
        assert_eq!(
            class_reads(&first.runtime, MetadataCacheClass::IndexRoot),
            before_root
        );
        assert_eq!(
            class_reads(&first.runtime, MetadataCacheClass::IndexDirectory),
            before_directory
        );
        assert_eq!(
            class_reads(&first.runtime, MetadataCacheClass::IndexPage),
            before_page
        );
        assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
    }

    #[test]
    fn locator_provenance_rejects_another_segment_generation() {
        let shared_runtime = runtime(0, 1024 * 1024);
        let entries = default_entries();
        let (first_bytes, first_ranges) = encoded_index(&entries);
        let first = fixture(
            "schema6-provenance-first",
            shared_runtime.clone(),
            first_bytes,
            first_ranges,
        );
        let (second_bytes, second_ranges) = encoded_index(&entries);
        let second = fixture(
            "schema6-provenance-second",
            shared_runtime,
            second_bytes,
            second_ranges,
        );

        let first_reader = open_reader(&first);
        let first_session = first_reader.query_session().expect("open first session");
        let first_root = first_session.load_root().expect("load first root");
        let locators = first_session
            .read_series_entries(&first_root, 0, first.ranges[0])
            .expect("read first locators");

        let second_reader = open_reader(&second);
        let second_session = second_reader.query_session().expect("open second session");
        assert!(matches!(
            second_session.locators(&locators),
            Err(Schema6ChunkIndexReaderError::ForeignSegmentGeneration)
        ));
    }
}
