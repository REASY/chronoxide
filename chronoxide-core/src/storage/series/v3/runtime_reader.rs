//! Strict governed runtime boundary for schema-7 series metadata.
//!
//! The long-lived reader retains only the registered segment and immutable
//! facts discovered during open. A query session owns the lifecycle guard;
//! decoded roots, pages, and overflow blobs remain independent cache values
//! whose pins are held only for the operation that needs them.

mod cold;
mod labels;
mod materialize;
mod overflow;
mod roots;

use std::io;
use std::ops::{Deref, Range};
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::hash::XxHash64;
use crate::promql::METRIC_NAME_LABEL;
use crate::storage::chunk::{
    CHUNK_OVERFLOW_ROOT_V2_LEN, ChunkOverflowRootV2, decode_chunk_overflow_root_v2,
};
use crate::storage::metadata_cache::{
    LoadedMetadata, MetadataCacheError, MetadataCacheKey, MetadataCacheKeyError, MetadataCachePin,
};
use crate::storage::metadata_governor::{MetadataCacheClass, MetadataCharge, MetadataUsageClass};
use crate::storage::metadata_runtime::{
    GovernedArtifactReader, RegisteredSegment, SegmentGenerationProvenance, SegmentReadGuard,
    StoreMetadataRuntimeError,
};
use crate::storage::segment::SegmentFile;
use crate::storage::series::GovernedSeriesCountBinding;
use crate::storage::symbols::{GovernedSymbolReaderError, GovernedSymbolSession};

use super::super::cold_v2::reader as cold_v2_reader;
use super::{
    ChunkLocatorSource, FlatChunkLocatorBatch, PlannedSeries, SERIES_COLD_PAGE_LEN_V1,
    SERIES_HEADER_LEN_V3, SERIES_HOT_PAGE_LEN_V1, Schema7OverflowBlobFacts, Schema7RootBinding,
    Schema7RootBindingContext, Schema7SeriesPageFacts, SeriesHeaderV3, SeriesRootV3,
    ValidatedOverflowBlob, ValidatedSeriesColdPage, ValidatedSeriesHotPage, decode_series_root_v3,
    plan_schema7_decoded_hot_page, plan_schema7_decoded_overflow_blob,
};

/// Errors at the governed schema-7 metadata boundary.
#[derive(Debug, Error)]
pub(crate) enum Schema7MetadataReaderError {
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Cache(#[from] MetadataCacheError),
    #[error(transparent)]
    CacheKey(#[from] MetadataCacheKeyError),
    #[error("schema-7 metadata planning failed: {0}")]
    Planning(#[source] io::Error),
    #[error("schema-7 metadata value belongs to another segment generation")]
    ForeignSegmentGeneration,
    #[error(transparent)]
    Symbols(#[from] GovernedSymbolReaderError),
}

/// Long-lived, generation-owning schema-7 reader with no descriptor or cache
/// pins. Query work must first acquire a [`Schema7MetadataSession`].
pub(crate) struct Schema7MetadataReader {
    registered: RegisteredSegment,
    root_len: u64,
    context: Schema7RootBindingContext,
    chunk_file_lens: [u64; 2],
}

/// Query-scoped authorization and resource boundary for one segment.
pub(crate) struct Schema7MetadataSession {
    guard: SegmentReadGuard,
    root_len: u64,
    context: Schema7RootBindingContext,
    chunk_file_lens: [u64; 2],
}

/// Separately cached fixed roots. This type deliberately contains no guard or
/// registered-segment owner.
#[derive(Debug)]
pub(crate) struct Schema7RootPins {
    provenance: SegmentGenerationProvenance,
    series: MetadataCachePin<SeriesRootV3>,
    overflow: MetadataCachePin<ChunkOverflowRootV2>,
}

/// Ephemeral cross-root binding which keeps both independent cache values
/// pinned for descriptor lookup.
#[derive(Debug)]
pub(crate) struct BoundSchema7Roots {
    roots: Schema7RootPins,
    series_pages: Schema7SeriesPageFacts,
    overflow_blobs: Schema7OverflowBlobFacts,
}

/// Query-local planned series whose heap allocation remains charged as scratch
/// until the caller drops the batch.
#[derive(Debug)]
pub(crate) struct GovernedPlannedSeries {
    provenance: SegmentGenerationProvenance,
    values: Vec<PlannedSeries>,
    _charge: MetadataCharge,
}

impl GovernedPlannedSeries {
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn get(&self, index: usize) -> Option<GovernedPlannedSeriesRef<'_>> {
        self.values
            .get(index)
            .map(|value| GovernedPlannedSeriesRef {
                provenance: &self.provenance,
                value,
            })
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

/// One planned series which cannot be detached from the generation that
/// integrity-checked its hot record.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GovernedPlannedSeriesRef<'a> {
    provenance: &'a SegmentGenerationProvenance,
    value: &'a PlannedSeries,
}

impl Deref for GovernedPlannedSeriesRef<'_> {
    type Target = PlannedSeries;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

/// Canonical labels and stable identity returned only after the complete cold
/// row and every required symbol pass their integrity checks and reproduce the
/// stored fingerprint.
#[derive(Debug)]
pub(crate) struct GovernedVerifiedSeries {
    series_ref: u32,
    series_id: u64,
    metric_name_dropped_series_id: Option<u64>,
    kind_mask: u8,
    labels_complete: bool,
    integrity_checked_label_count: usize,
    labels: Vec<(String, String)>,
    _charge: MetadataCharge,
}

pub(crate) type ProfiledGovernedVerifiedSeries =
    (GovernedVerifiedSeries, CanonicalLabelMaterializationProfile);

/// Fully verified canonical labels in their source-symbol representation.
///
/// This value becomes observable only after every referenced symbol has been
/// resolved and the complete canonical row has reproduced the authenticated
/// series identity. The source IDs remain generation-bound at the metadata
/// facade; query code may not interpret or cache them without that capability.
#[derive(Debug)]
pub(crate) struct GovernedVerifiedEncodedSeries {
    series_ref: u32,
    series_id: u64,
    metric_name_dropped_series_id: Option<u64>,
    kind_mask: u8,
    labels_complete: bool,
    integrity_checked_label_count: usize,
    labels: Vec<(u32, u32)>,
    _charge: MetadataCharge,
}

pub(crate) type ProfiledGovernedVerifiedEncodedSeries = (
    GovernedVerifiedEncodedSeries,
    CanonicalLabelMaterializationProfile,
);

type MaterializedCanonicalLabels = (Vec<(String, String)>, MetadataCharge, Option<u64>);

/// Exclusive production-query attribution for schema-7/8 canonical rows.
///
/// The complete materialization elapsed time is partitioned across these
/// fields. Metadata-page reads and validation needed to reconstruct the
/// encoded row are charged to `canonical_row_decode`; symbol page work is
/// charged to `symbol_resolution`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CanonicalLabelMaterializationProfile {
    pub(crate) canonical_row_decode: Duration,
    pub(crate) symbol_resolution: Duration,
    pub(crate) canonical_identity: Duration,
    pub(crate) label_construction: Duration,
}

impl CanonicalLabelMaterializationProfile {
    fn attributed(self) -> Duration {
        Duration::ZERO
            .saturating_add(self.canonical_row_decode)
            .saturating_add(self.symbol_resolution)
            .saturating_add(self.canonical_identity)
            .saturating_add(self.label_construction)
    }

    fn finish_row(&mut self, elapsed: Duration) {
        self.canonical_row_decode = self
            .canonical_row_decode
            .saturating_add(elapsed.saturating_sub(self.attributed()));
    }
}

#[inline(always)]
fn detailed_stage_started<const DETAILED: bool>() -> Option<Instant> {
    if DETAILED { Some(Instant::now()) } else { None }
}

#[inline(always)]
fn finish_materialization_profile<const DETAILED: bool>(
    profile: &mut CanonicalLabelMaterializationProfile,
    started: Option<Instant>,
) {
    if DETAILED {
        profile.finish_row(started.expect("detailed stage timer exists").elapsed());
    } else {
        debug_assert!(started.is_none());
    }
}

#[derive(Clone, Copy)]
enum CanonicalLabelSelection<'a> {
    All,
    Requested {
        names: &'a [String],
        derive_metric_name_dropped_identity: bool,
    },
}

impl CanonicalLabelSelection<'_> {
    fn includes(self, label_name: &str) -> bool {
        match self {
            Self::All => true,
            Self::Requested { names, .. } => names.iter().any(|name| name == label_name),
        }
    }

    fn output_capacity(self, label_count: usize) -> usize {
        match self {
            Self::All => label_count,
            Self::Requested { names, .. } => names.len().min(label_count),
        }
    }

    fn labels_complete(self) -> bool {
        matches!(self, Self::All)
    }

    fn derives_metric_name_dropped_identity(self) -> bool {
        matches!(
            self,
            Self::Requested {
                derive_metric_name_dropped_identity: true,
                ..
            }
        )
    }
}

impl GovernedVerifiedSeries {
    pub(crate) fn series_ref(&self) -> u32 {
        self.series_ref
    }

    pub(crate) fn series_id(&self) -> u64 {
        self.series_id
    }

    pub(crate) fn metric_name_dropped_series_id(&self) -> Option<u64> {
        self.metric_name_dropped_series_id
    }

    pub(crate) fn kind_mask(&self) -> u8 {
        self.kind_mask
    }

    pub(crate) fn labels(&self) -> &[(String, String)] {
        &self.labels
    }

    pub(crate) fn labels_complete(&self) -> bool {
        self.labels_complete
    }

    pub(crate) fn integrity_checked_label_count(&self) -> usize {
        self.integrity_checked_label_count
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

impl GovernedVerifiedEncodedSeries {
    pub(crate) fn series_ref(&self) -> u32 {
        self.series_ref
    }

    pub(crate) fn series_id(&self) -> u64 {
        self.series_id
    }

    pub(crate) fn metric_name_dropped_series_id(&self) -> Option<u64> {
        self.metric_name_dropped_series_id
    }

    pub(crate) fn kind_mask(&self) -> u8 {
        self.kind_mask
    }

    pub(crate) fn labels(&self) -> &[(u32, u32)] {
        &self.labels
    }

    pub(crate) fn labels_complete(&self) -> bool {
        self.labels_complete
    }

    pub(crate) fn integrity_checked_label_count(&self) -> usize {
        self.integrity_checked_label_count
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

#[derive(Clone, Copy)]
struct ColdSection {
    offset: u64,
    end: u64,
    count: u32,
}

enum GovernedColdRangeBytes {
    Empty,
    Borrowed {
        page: MetadataCachePin<ValidatedSeriesColdPage>,
        header: SeriesHeaderV3,
        page_index: u32,
        descriptor: super::SeriesColdPageDescriptorV1,
        range: Range<usize>,
    },
    Owned(Vec<u8>),
}

struct GovernedColdRange {
    bytes: GovernedColdRangeBytes,
    _charge: MetadataCharge,
}

impl GovernedColdRange {
    fn as_slice(&self) -> io::Result<&[u8]> {
        match &self.bytes {
            GovernedColdRangeBytes::Empty => Ok(&[]),
            GovernedColdRangeBytes::Borrowed {
                page,
                header,
                page_index,
                descriptor,
                range,
            } => Ok(&page.bytes_for(*header, *page_index, *descriptor)?[range.clone()]),
            GovernedColdRangeBytes::Owned(bytes) => Ok(bytes),
        }
    }
}

struct GovernedDecodedVec<T> {
    values: Vec<T>,
    _charge: MetadataCharge,
}

struct GovernedBlockMeta {
    value: cold_v2_reader::KeySetBlockMeta,
    _charge: MetadataCharge,
}

struct DecodedKeysetPlan {
    keyset: GovernedDecodedVec<u32>,
    block: GovernedBlockMeta,
}

/// Lazy hot-page materialization state. Decoded cold metadata is reused after
/// the first visited row, while later rows and symbols remain untouched until
/// the caller actually advances to them.
pub(crate) struct Schema7MaterializationContext {
    provenance: SegmentGenerationProvenance,
    cache: Option<Schema7MaterializationCache>,
}

struct Schema7MaterializationCache {
    plans: Vec<(u32, DecodedKeysetPlan)>,
    dictionaries: Vec<(u32, GovernedDecodedVec<u32>)>,
    _charge: MetadataCharge,
}

#[derive(Clone, Copy)]
struct ValueDictEntryMeta {
    value: cold_v2_reader::ValueDictMeta,
    entry_end: u64,
}

/// Query-local overflow locators whose heap allocations remain charged as
/// scratch until the caller drops the batch.
#[derive(Debug)]
pub(crate) struct GovernedChunkLocatorBatch {
    value: FlatChunkLocatorBatch,
    _charge: MetadataCharge,
}

impl GovernedChunkLocatorBatch {
    pub(crate) fn locators(&self) -> &[crate::storage::chunk::IndexedChunkLocator] {
        self.value.locators()
    }

    pub(crate) fn series_spans(&self) -> &[super::SeriesChunkSpan] {
        self.value.series_spans()
    }

    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

impl Schema7MetadataReader {
    /// Opens and strictly validates both fixed roots without retaining a guard,
    /// descriptor, or cache pin.
    ///
    /// The fixed 176-byte header is issued once to discover the root range. On
    /// a root-cache miss, that prefix seeds the governed scratch allocation and
    /// only `[176, root_len)` is issued. Thus open never rereads the prefix.
    pub(crate) fn open(
        registered: &RegisteredSegment,
        context: Schema7RootBindingContext,
    ) -> Result<Self, Schema7MetadataReaderError> {
        let guard = registered.read_guard()?;
        let series_reader = guard.reader(SegmentFile::Series)?;
        let chunk_index_reader = guard.reader(SegmentFile::ChunkIndex)?;

        validate_inventory_len(
            &series_reader,
            context.series_file_len,
            "schema-7 series footer length does not match registered artifact",
        )?;
        validate_inventory_len(
            &chunk_index_reader,
            context.chunk_index_file_len,
            "schema-7 chunk-index footer length does not match registered artifact",
        )?;

        let chunk_file_lens = [
            guard.reader(SegmentFile::Chunks)?.len(),
            guard.reader(SegmentFile::OooChunks)?.len(),
        ];
        let mut prefix = [0u8; SERIES_HEADER_LEN_V3];
        series_reader.read_exact_at_for_class(0, &mut prefix, MetadataCacheClass::SeriesRoot)?;
        let header = SeriesHeaderV3::decode(&prefix)
            .map_err(|error| series_reader.record_validation_error(error))?;
        if header.file_len != series_reader.len() {
            return Err(series_reader
                .record_validation_error(invalid_data(
                    "schema-7 series header length does not match registered artifact",
                ))
                .into());
        }

        let root_len = header.hot_pages_offset;
        let session = Schema7MetadataSession {
            guard,
            root_len,
            context,
            chunk_file_lens,
        };
        let roots = session.load_roots_with_prefix(Some(&prefix))?;
        let bound = session.bind(roots)?;
        drop(bound);
        drop(session);

        Ok(Self {
            registered: registered.clone(),
            root_len,
            context,
            chunk_file_lens,
        })
    }

    pub(crate) fn query_session(
        &self,
    ) -> Result<Schema7MetadataSession, Schema7MetadataReaderError> {
        Ok(Schema7MetadataSession {
            guard: self.registered.read_guard()?,
            root_len: self.root_len,
            context: self.context,
            chunk_file_lens: self.chunk_file_lens,
        })
    }

    pub(crate) fn segment_identity(&self) -> &str {
        self.registered.segment_identity()
    }

    pub(crate) fn root_len(&self) -> u64 {
        self.root_len
    }
}

impl Schema7MetadataSession {
    fn reserve_series_scratch(
        &self,
        bytes: u64,
    ) -> Result<MetadataCharge, Schema7MetadataReaderError> {
        self.guard
            .reader(SegmentFile::Series)?
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(bytes, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)
            .map_err(Schema7MetadataReaderError::from)
    }

    fn ensure_bound_roots(
        &self,
        roots: &BoundSchema7Roots,
    ) -> Result<(), Schema7MetadataReaderError> {
        self.ensure_provenance(&roots.roots.provenance)
    }

    fn ensure_provenance(
        &self,
        provenance: &SegmentGenerationProvenance,
    ) -> Result<(), Schema7MetadataReaderError> {
        if provenance.matches(&self.guard) {
            Ok(())
        } else {
            Err(Schema7MetadataReaderError::ForeignSegmentGeneration)
        }
    }

    fn record_cross_artifact_error(&self, error: io::Error) -> Schema7MetadataReaderError {
        let kind = error.kind();
        let message = error.to_string();
        let series = match self.guard.reader(SegmentFile::Series) {
            Ok(reader) => reader,
            Err(error) => return error.into(),
        };
        let overflow = match self.guard.reader(SegmentFile::ChunkIndex) {
            Ok(reader) => reader,
            Err(error) => return error.into(),
        };
        let recorded = series.record_validation_error(io::Error::new(kind, message.clone()));
        let _ = overflow.record_validation_error(io::Error::new(kind, message));
        recorded.into()
    }

    fn record_series_result<T>(
        &self,
        result: io::Result<T>,
    ) -> Result<T, Schema7MetadataReaderError> {
        result.map_err(|error| self.record_series_error(error))
    }

    fn record_series_error(&self, error: io::Error) -> Schema7MetadataReaderError {
        match self.guard.reader(SegmentFile::Series) {
            Ok(reader) => Schema7MetadataReaderError::Cache(reader.record_validation_error(error)),
            Err(error) => Schema7MetadataReaderError::Runtime(error),
        }
    }
}

impl BoundSchema7Roots {
    fn series_root(&self) -> &SeriesRootV3 {
        &self.roots.series
    }

    fn overflow_root(&self) -> &ChunkOverflowRootV2 {
        &self.roots.overflow
    }

    fn series_pages(&self) -> Schema7SeriesPageFacts {
        self.series_pages
    }

    fn overflow_blobs(&self) -> Schema7OverflowBlobFacts {
        self.overflow_blobs
    }

    fn hot_descriptor(
        &self,
        page_index: u32,
    ) -> Result<super::SeriesHotPageDescriptorV1, Schema7MetadataReaderError> {
        self.roots
            .series
            .hot_descriptors
            .get(
                usize::try_from(page_index)
                    .map_err(|_| planning_error("schema-7 hot page index exceeds usize"))?,
            )
            .copied()
            .ok_or_else(|| planning_error("schema-7 hot page index is out of range"))
    }

    fn cold_descriptor(
        &self,
        page_index: u32,
    ) -> Result<super::SeriesColdPageDescriptorV1, Schema7MetadataReaderError> {
        self.roots
            .series
            .cold_descriptors
            .get(
                usize::try_from(page_index)
                    .map_err(|_| planning_error("schema-7 cold page index exceeds usize"))?,
            )
            .copied()
            .ok_or_else(|| planning_error("schema-7 cold page index is out of range"))
    }
}

fn cache_key(
    reader: &GovernedArtifactReader,
    offset: u64,
    length: u64,
    class: MetadataCacheClass,
) -> Result<MetadataCacheKey, MetadataCacheKeyError> {
    reader.metadata_cache_key(offset, length, class)
}

fn validate_inventory_len(
    reader: &GovernedArtifactReader,
    expected: u64,
    message: &'static str,
) -> Result<(), Schema7MetadataReaderError> {
    if reader.len() == expected {
        Ok(())
    } else {
        Err(reader.record_validation_error(invalid_data(message)).into())
    }
}

fn checked_vec_bytes<T>(
    len: usize,
    message: &'static str,
) -> Result<u64, Schema7MetadataReaderError> {
    let bytes = len
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| planning_error(message))?;
    u64::try_from(bytes).map_err(|_| Schema7MetadataReaderError::Planning(planning_io(message)))
}

fn checked_add_bytes(
    left: u64,
    right: u64,
    message: &'static str,
) -> Result<u64, Schema7MetadataReaderError> {
    left.checked_add(right)
        .ok_or_else(|| planning_error(message))
}

fn is_budget_error(error: &Schema7MetadataReaderError) -> bool {
    matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Budget(_))
            | Schema7MetadataReaderError::Symbols(GovernedSymbolReaderError::Cache(
                MetadataCacheError::Budget(_)
            ))
    )
}

fn is_optional_materialization_cache_error(error: &Schema7MetadataReaderError) -> bool {
    is_budget_error(error)
        || matches!(
            error,
            Schema7MetadataReaderError::Planning(error)
                if error.kind() == io::ErrorKind::OutOfMemory
        )
}

fn checked_batch_bytes(locators: usize, spans: usize) -> Result<u64, Schema7MetadataReaderError> {
    let locators = checked_vec_bytes::<crate::storage::chunk::IndexedChunkLocator>(
        locators,
        "schema-7 locator allocation charge overflows",
    )?;
    let spans = checked_vec_bytes::<super::SeriesChunkSpan>(
        spans,
        "schema-7 locator-span allocation charge overflows",
    )?;
    locators
        .checked_add(spans)
        .ok_or_else(|| planning_error("schema-7 locator batch charge overflows"))
}

fn try_vec_with_capacity<T>(
    capacity: usize,
    message: &'static str,
) -> Result<Vec<T>, Schema7MetadataReaderError> {
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).map_err(|_| {
        Schema7MetadataReaderError::Planning(io::Error::new(io::ErrorKind::OutOfMemory, message))
    })?;
    Ok(values)
}

fn planning_error(message: &'static str) -> Schema7MetadataReaderError {
    Schema7MetadataReaderError::Planning(planning_io(message))
}

fn planning_io(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
