//! Strict governed runtime boundary for schema-7 series metadata.
//!
//! The long-lived reader retains only the registered segment and immutable
//! facts discovered during open. A query session owns the lifecycle guard;
//! decoded roots, pages, and overflow blobs remain independent cache values
//! whose pins are held only for the operation that needs them.

use std::io;
use std::ops::{Deref, Range};
use std::time::{Duration, Instant};

use thiserror::Error;

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
use crate::util::XxHash64;

use super::super::cold_v2::reader as cold_v2_reader;
use super::{
    ChunkLocatorSource, FlatChunkLocatorBatch, PlannedSeries, SERIES_HEADER_LEN_V3,
    SERIES_HOT_PAGE_LEN_V1, Schema7OverflowBlobFacts, Schema7RootBinding,
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
    /// Loads the two roots as separate pure cache values.
    pub(crate) fn load_roots(&self) -> Result<Schema7RootPins, Schema7MetadataReaderError> {
        self.load_roots_with_prefix(None)
    }

    fn load_roots_with_prefix(
        &self,
        series_prefix: Option<&[u8]>,
    ) -> Result<Schema7RootPins, Schema7MetadataReaderError> {
        let series_reader = self.guard.reader(SegmentFile::Series)?;
        let series_key = cache_key(
            &series_reader,
            0,
            self.root_len,
            MetadataCacheClass::SeriesRoot,
        )?;
        let series_declared =
            SeriesRootV3::declared_max_bytes(self.root_len).map_err(MetadataCacheError::from_io)?;
        let load_series = |bytes: &[u8]| {
            let root = decode_series_root_v3(bytes).map_err(MetadataCacheError::from_io)?;
            let charged = root.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(root, charged))
        };
        let series = match series_prefix {
            Some(prefix) => series_reader.get_or_load_with_prefix(
                series_key,
                series_declared,
                prefix,
                load_series,
            )?,
            None => series_reader.get_or_load(series_key, series_declared, load_series)?,
        };

        let overflow_reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        let overflow_key = cache_key(
            &overflow_reader,
            0,
            CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
            MetadataCacheClass::OverflowRoot,
        )?;
        let expected_file_len = self.context.chunk_index_file_len;
        let overflow = overflow_reader.get_or_load(
            overflow_key,
            std::mem::size_of::<ChunkOverflowRootV2>() as u64,
            move |bytes| {
                let root = decode_chunk_overflow_root_v2(bytes, expected_file_len)
                    .map_err(MetadataCacheError::from_io)?;
                let charged = root.charged_bytes();
                Ok(LoadedMetadata::new(root, charged))
            },
        )?;

        Ok(Schema7RootPins {
            provenance: self.guard.provenance(),
            series,
            overflow,
        })
    }

    /// Cross-validates separately cached roots while retaining their pins only
    /// in the returned query-local binding.
    pub(crate) fn bind(
        &self,
        roots: Schema7RootPins,
    ) -> Result<BoundSchema7Roots, Schema7MetadataReaderError> {
        self.ensure_provenance(&roots.provenance)?;
        match Schema7RootBinding::bind_decoded(&roots.series, &roots.overflow, self.context) {
            Ok((series_pages, overflow_blobs)) => Ok(BoundSchema7Roots {
                roots,
                series_pages,
                overflow_blobs,
            }),
            Err(error) => Err(self.record_cross_artifact_error(error)),
        }
    }

    /// Exposes the schema-neutral series-count capability only after both
    /// schema-7 roots have been cross-validated for this generation.
    pub(crate) fn series_count_binding(
        &self,
        roots: &BoundSchema7Roots,
    ) -> Result<GovernedSeriesCountBinding, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        Ok(GovernedSeriesCountBinding::new(
            self.guard.provenance(),
            roots.series_root().header.num_series,
        ))
    }

    /// Loads and authenticates one exact fixed-size hot page.
    fn load_hot_page(
        &self,
        roots: &BoundSchema7Roots,
        page_index: u32,
    ) -> Result<MetadataCachePin<ValidatedSeriesHotPage>, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let descriptor = roots.hot_descriptor(page_index)?;
        let page_offset = roots
            .series_pages
            .hot_pages_offset
            .checked_add(
                u64::from(page_index)
                    .checked_mul(SERIES_HOT_PAGE_LEN_V1 as u64)
                    .ok_or_else(|| planning_error("schema-7 hot page offset overflows"))?,
            )
            .ok_or_else(|| planning_error("schema-7 hot page offset overflows"))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = cache_key(
            &reader,
            page_offset,
            SERIES_HOT_PAGE_LEN_V1 as u64,
            MetadataCacheClass::SeriesHotPage,
        )?;
        let declared = ValidatedSeriesHotPage::declared_max_bytes(descriptor)
            .map_err(MetadataCacheError::from_io)?;
        let header = roots.series_root().header;
        let chunk_file_lens = self.chunk_file_lens;
        Ok(reader.get_or_load_owned(key, declared, move |bytes| {
            let page = ValidatedSeriesHotPage::decode_owned(
                header,
                page_index,
                descriptor,
                bytes,
                chunk_file_lens,
            )
            .map_err(MetadataCacheError::from_io)?;
            let charged = page.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(page, charged))
        })?)
    }

    /// Plans selected series from one authenticated page and retains a scratch
    /// charge for the resulting query-local vector.
    pub(crate) fn plan_hot_page(
        &self,
        roots: &BoundSchema7Roots,
        page_index: u32,
        selected_series_refs: &[u32],
    ) -> Result<GovernedPlannedSeries, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let declared = checked_vec_bytes::<PlannedSeries>(
            selected_series_refs.len(),
            "schema-7 planned-series allocation charge overflows",
        )?;
        let mut charge = self
            .guard
            .reader(SegmentFile::Series)?
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let descriptor = roots.hot_descriptor(page_index)?;
        let page = self.load_hot_page(roots, page_index)?;
        let values = plan_schema7_decoded_hot_page(
            roots.series_root().header,
            roots.series_pages,
            page_index,
            descriptor,
            &page,
            self.chunk_file_lens,
            selected_series_refs,
        )
        .map_err(Schema7MetadataReaderError::Planning)?;
        charge
            .reconcile(checked_vec_bytes::<PlannedSeries>(
                values.capacity(),
                "schema-7 planned-series allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedPlannedSeries {
            provenance: self.guard.provenance(),
            values,
            _charge: charge,
        })
    }

    /// Loads and authenticates one exact cold-label page. The returned pin
    /// keeps the page bytes governed until the caller finishes materializing
    /// labels from them.
    fn load_cold_page(
        &self,
        roots: &BoundSchema7Roots,
        page_index: u32,
    ) -> Result<MetadataCachePin<ValidatedSeriesColdPage>, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let descriptor = roots.cold_descriptor(page_index)?;
        let page_offset = roots
            .series_pages
            .cold_pages_offset
            .checked_add(
                u64::from(page_index)
                    .checked_mul(super::SERIES_COLD_PAGE_LEN_V1)
                    .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?,
            )
            .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = cache_key(
            &reader,
            page_offset,
            u64::from(descriptor.page_len),
            MetadataCacheClass::SeriesColdPage,
        )?;
        let declared = ValidatedSeriesColdPage::declared_max_bytes(descriptor)
            .map_err(MetadataCacheError::from_io)?;
        let header = roots.series_root().header;
        Ok(reader.get_or_load_owned(key, declared, move |bytes| {
            let page = ValidatedSeriesColdPage::decode_owned(header, page_index, descriptor, bytes)
                .map_err(MetadataCacheError::from_io)?;
            let charged = page.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(page, charged))
        })?)
    }

    /// Materializes one complete v2 cold-label row and exposes its stable
    /// identity only after the same-generation symbol bytes reproduce the
    /// fingerprint stored in the authenticated hot record.
    pub(crate) fn materialize_verified(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_with_selection::<false>(
            roots,
            symbols,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )
    }

    /// Integrity-checks the complete canonical label row and stable identity,
    /// but owns only labels whose names were requested by the caller.
    pub(crate) fn materialize_verified_selected(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_with_selection::<false>(
            roots,
            symbols,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )
    }

    fn materialize_verified_with_selection<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;

        let keyset = self.load_keyset(roots, planned.cold_labels.keyset_id)?;
        self.validate_key_symbols(symbols, &keyset.values)?;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            keyset.values.len(),
            "schema-7 materialized-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            keyset.values.len(),
            "schema-7 materialized-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 materialized-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;

        let block = self.load_keyset_block(roots, planned.cold_labels.keyset_id)?;
        self.record_series_result(cold_v2_reader::validate_keyset_block_key_count(
            &block.value,
            keyset.values.len(),
        ))?;
        let row = if block.value.row_len_bytes == 0 {
            if planned.cold_labels.row >= block.value.rows {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 series cold row is out of bounds",
                )));
            }
            None
        } else {
            let range = self.record_series_result(cold_v2_reader::keyset_block_row_range(
                &block.value,
                planned.cold_labels.row,
            ))?;
            Some(self.read_authenticated_cold_range_owned(roots, range)?)
        };
        let row_bytes = match row.as_ref() {
            Some(row) => self.record_series_result(row.as_slice())?,
            None => &[],
        };
        let mut cursor = 0usize;
        for (index, key_sym) in keyset.values.iter().copied().enumerate() {
            let dictionary = self.find_value_dictionary(roots, symbols, key_sym)?;
            let width = *block.value.widths.get(index).ok_or_else(|| {
                self.record_series_error(invalid_data("schema-7 keyset block width is missing"))
            })?;
            self.record_series_result(cold_v2_reader::validate_value_code_width(
                width,
                dictionary.values.len() as u32,
            ))?;
            let code = self.record_series_result(cold_v2_reader::read_value_code(
                row_bytes,
                &mut cursor,
                width,
            ))?;
            let value_sym = dictionary
                .values
                .get(usize::try_from(code).map_err(|_| {
                    self.record_series_error(invalid_data("schema-7 value code exceeds usize"))
                })?)
                .copied()
                .ok_or_else(|| {
                    self.record_series_error(invalid_data("schema-7 value code is out of bounds"))
                })?;
            encoded_labels.push((key_sym, value_sym));
        }
        if cursor != row_bytes.len() {
            return Err(self
                .record_series_error(invalid_data("schema-7 series cold row has trailing bytes")));
        }
        let (labels, output_charge, metric_name_dropped_series_id) = self
            .materialize_and_verify_canonical_labels::<DETAILED>(
                symbols,
                planned.expected_label_identity,
                &encoded_labels,
                selection,
                materialization_profile,
            )?;
        let integrity_checked_label_count = encoded_labels.len();
        drop(encoded_labels);
        drop(encoded_charge);
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );

        Ok(GovernedVerifiedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels,
            _charge: output_charge,
        })
    }

    fn materialize_verified_encoded_with_selection<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;

        let keyset = self.load_keyset(roots, planned.cold_labels.keyset_id)?;
        self.validate_key_symbols(symbols, &keyset.values)?;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            keyset.values.len(),
            "schema-7 encoded-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            keyset.values.len(),
            "schema-7 encoded-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 encoded-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;

        let block = self.load_keyset_block(roots, planned.cold_labels.keyset_id)?;
        self.record_series_result(cold_v2_reader::validate_keyset_block_key_count(
            &block.value,
            keyset.values.len(),
        ))?;
        let row = if block.value.row_len_bytes == 0 {
            if planned.cold_labels.row >= block.value.rows {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 series cold row is out of bounds",
                )));
            }
            None
        } else {
            let range = self.record_series_result(cold_v2_reader::keyset_block_row_range(
                &block.value,
                planned.cold_labels.row,
            ))?;
            Some(self.read_authenticated_cold_range_owned(roots, range)?)
        };
        let row_bytes = match row.as_ref() {
            Some(row) => self.record_series_result(row.as_slice())?,
            None => &[],
        };
        let mut cursor = 0usize;
        for (index, key_sym) in keyset.values.iter().copied().enumerate() {
            let dictionary = self.find_value_dictionary(roots, symbols, key_sym)?;
            let width = *block.value.widths.get(index).ok_or_else(|| {
                self.record_series_error(invalid_data("schema-7 keyset block width is missing"))
            })?;
            self.record_series_result(cold_v2_reader::validate_value_code_width(
                width,
                u32::try_from(dictionary.values.len()).map_err(|_| {
                    self.record_series_error(invalid_data(
                        "schema-7 value dictionary length exceeds u32",
                    ))
                })?,
            ))?;
            let code = self.record_series_result(cold_v2_reader::read_value_code(
                row_bytes,
                &mut cursor,
                width,
            ))?;
            let value_sym = dictionary
                .values
                .get(usize::try_from(code).map_err(|_| {
                    self.record_series_error(invalid_data("schema-7 value code exceeds usize"))
                })?)
                .copied()
                .ok_or_else(|| {
                    self.record_series_error(invalid_data("schema-7 value code is out of bounds"))
                })?;
            encoded_labels.push((key_sym, value_sym));
        }
        if cursor != row_bytes.len() {
            return Err(self
                .record_series_error(invalid_data("schema-7 series cold row has trailing bytes")));
        }
        let integrity_checked_label_count = encoded_labels.len();
        let metric_name_dropped_series_id = self.verify_and_select_encoded_labels::<DETAILED>(
            symbols,
            planned.expected_label_identity,
            &mut encoded_labels,
            selection,
            materialization_profile,
        )?;
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );

        Ok(GovernedVerifiedEncodedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels: encoded_labels,
            _charge: encoded_charge,
        })
    }

    /// Creates best-effort lazy reuse state for one planned hot-page batch.
    /// If its fixed bookkeeping reservation cannot fit, materialization remains
    /// correct and falls back to the scalar path instead of failing the query.
    pub(crate) fn materialization_context(
        &self,
        roots: &BoundSchema7Roots,
        planned_capacity: usize,
    ) -> Result<Schema7MaterializationContext, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let provenance = self.guard.provenance();
        let dictionary_capacity = usize::try_from(roots.series_root().header.num_value_dicts)
            .map_err(|_| planning_error("schema-7 value dictionary count exceeds usize"))?;
        let cache = match self.try_materialization_cache(planned_capacity, dictionary_capacity) {
            Ok(cache) => Some(cache),
            Err(error) if is_optional_materialization_cache_error(&error) => None,
            Err(error) => return Err(error),
        };
        Ok(Schema7MaterializationContext { provenance, cache })
    }

    fn try_materialization_cache(
        &self,
        planned_capacity: usize,
        dictionary_capacity: usize,
    ) -> Result<Schema7MaterializationCache, Schema7MetadataReaderError> {
        let declared = checked_add_bytes(
            checked_vec_bytes::<(u32, DecodedKeysetPlan)>(
                planned_capacity,
                "schema-7 decoded keyset-plan allocation charge overflows",
            )?,
            checked_vec_bytes::<(u32, GovernedDecodedVec<u32>)>(
                dictionary_capacity,
                "schema-7 decoded dictionary-cache allocation charge overflows",
            )?,
            "schema-7 materialization-cache allocation charge overflows",
        )?;
        let mut charge = self.reserve_series_scratch(declared)?;
        let plans = try_vec_with_capacity(
            planned_capacity,
            "schema-7 decoded keyset-plan allocation failed",
        )?;
        let dictionaries = try_vec_with_capacity(
            dictionary_capacity,
            "schema-7 decoded dictionary-cache allocation failed",
        )?;
        charge
            .reconcile(checked_add_bytes(
                checked_vec_bytes::<(u32, DecodedKeysetPlan)>(
                    plans.capacity(),
                    "schema-7 decoded keyset-plan allocation charge overflows",
                )?,
                checked_vec_bytes::<(u32, GovernedDecodedVec<u32>)>(
                    dictionaries.capacity(),
                    "schema-7 decoded dictionary-cache allocation charge overflows",
                )?,
                "schema-7 materialization-cache allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(Schema7MaterializationCache {
            plans,
            dictionaries,
            _charge: charge,
        })
    }

    /// Materializes only the current series while retaining already decoded
    /// shared cold metadata for later series. A visitor that stops after this
    /// value therefore cannot observe corruption or I/O belonging exclusively
    /// to an unvisited later row.
    pub(crate) fn materialize_verified_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )
    }

    /// Cached counterpart to [`Self::materialize_verified_selected`]. Shared
    /// cold metadata remains reusable while omitted labels are still decoded,
    /// integrity-checked, and included in stable-identity verification.
    pub(crate) fn materialize_verified_selected_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )
    }

    pub(crate) fn materialize_verified_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<ProfiledGovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    pub(crate) fn materialize_verified_selected_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<ProfiledGovernedVerifiedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    /// Compact-label counterpart to [`Self::materialize_verified_cached`].
    /// The complete row and all symbol bytes are authenticated before source
    /// symbol IDs are exposed to the generation-bound facade.
    pub(crate) fn materialize_verified_encoded_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_encoded_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )
    }

    pub(crate) fn materialize_verified_encoded_selected_cached(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        self.materialize_verified_encoded_selected_cached_impl::<false>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )
    }

    pub(crate) fn materialize_verified_encoded_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<ProfiledGovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_encoded_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    pub(crate) fn materialize_verified_encoded_selected_cached_profiled(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        requested_label_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Result<ProfiledGovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let mut profile = CanonicalLabelMaterializationProfile::default();
        let verified = self.materialize_verified_encoded_selected_cached_impl::<true>(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
            &mut profile,
        )?;
        Ok((verified, profile))
    }

    fn materialize_verified_selected_cached_impl<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(&context.provenance)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;
        self.guard.reader(SegmentFile::Series)?.check_artifact()?;
        let Some(cache) = context.cache.as_ref() else {
            let verified = self.materialize_verified_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        };
        let keyset_id = planned.cold_labels.keyset_id;
        if cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
            .is_err()
            && cache.plans.len() == cache.plans.capacity()
        {
            context.cache = None;
            let verified = self.materialize_verified_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }

        let result = match context.cache.as_mut() {
            Some(cache) => self.materialize_verified_with_cache::<DETAILED>(
                roots, symbols, cache, planned, selection, profile,
            ),
            None => {
                return self.materialize_verified_with_selection::<DETAILED>(
                    roots, symbols, planned, selection, profile,
                );
            }
        };
        if result.as_ref().is_err_and(is_budget_error) {
            // Cached decoded values are an optimization, not semantic state.
            // Release them before retrying the established scalar path so a
            // tight in-flight budget does not become a new query failure.
            context.cache = None;
            let verified = self.materialize_verified_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }
        result.inspect(|_| {
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
        })
    }

    fn materialize_verified_encoded_selected_cached_impl<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(&context.provenance)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;
        self.guard.reader(SegmentFile::Series)?.check_artifact()?;
        let Some(cache) = context.cache.as_ref() else {
            let verified = self.materialize_verified_encoded_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        };
        let keyset_id = planned.cold_labels.keyset_id;
        if cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
            .is_err()
            && cache.plans.len() == cache.plans.capacity()
        {
            context.cache = None;
            let verified = self.materialize_verified_encoded_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }

        let result = match context.cache.as_mut() {
            Some(cache) => self.materialize_verified_encoded_with_cache::<DETAILED>(
                roots, symbols, cache, planned, selection, profile,
            ),
            None => {
                return self.materialize_verified_encoded_with_selection::<DETAILED>(
                    roots, symbols, planned, selection, profile,
                );
            }
        };
        if result.as_ref().is_err_and(is_budget_error) {
            context.cache = None;
            let verified = self.materialize_verified_encoded_with_selection::<DETAILED>(
                roots, symbols, planned, selection, profile,
            )?;
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
            return Ok(verified);
        }
        result.inspect(|_| {
            finish_materialization_profile::<DETAILED>(profile, materialization_started);
        })
    }

    fn materialize_verified_with_cache<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        cache: &mut Schema7MaterializationCache,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        let keyset_id = planned.cold_labels.keyset_id;
        let plan_index = match cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
        {
            Ok(index) => index,
            Err(index) => {
                let plan = self.load_decoded_keyset_plan(roots, symbols, keyset_id)?;
                cache.plans.insert(index, (keyset_id, plan));
                index
            }
        };
        let plan = &cache.plans[plan_index].1;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            plan.keyset.values.len(),
            "schema-7 materialized-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            plan.keyset.values.len(),
            "schema-7 materialized-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 materialized-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        self.decode_encoded_labels(
            roots,
            symbols,
            plan,
            &mut cache.dictionaries,
            planned.cold_labels.row,
            &mut encoded_labels,
        )?;
        let (labels, output_charge, metric_name_dropped_series_id) = self
            .materialize_and_verify_canonical_labels::<DETAILED>(
                symbols,
                planned.expected_label_identity,
                &encoded_labels,
                selection,
                materialization_profile,
            )?;
        let integrity_checked_label_count = encoded_labels.len();
        drop(encoded_labels);
        drop(encoded_charge);
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );
        Ok(GovernedVerifiedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels,
            _charge: output_charge,
        })
    }

    fn materialize_verified_encoded_with_cache<const DETAILED: bool>(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        cache: &mut Schema7MaterializationCache,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<GovernedVerifiedEncodedSeries, Schema7MetadataReaderError> {
        let materialization_started = detailed_stage_started::<DETAILED>();
        let keyset_id = planned.cold_labels.keyset_id;
        let plan_index = match cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
        {
            Ok(index) => index,
            Err(index) => {
                let plan = self.load_decoded_keyset_plan(roots, symbols, keyset_id)?;
                cache.plans.insert(index, (keyset_id, plan));
                index
            }
        };
        let plan = &cache.plans[plan_index].1;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            plan.keyset.values.len(),
            "schema-7 encoded-label allocation charge overflows",
        )?;
        let mut encoded_charge = self.reserve_series_scratch(declared_labels)?;
        let mut encoded_labels = try_vec_with_capacity(
            plan.keyset.values.len(),
            "schema-7 encoded-label allocation failed",
        )?;
        encoded_charge
            .reconcile(checked_vec_bytes::<(u32, u32)>(
                encoded_labels.capacity(),
                "schema-7 encoded-label allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        self.decode_encoded_labels(
            roots,
            symbols,
            plan,
            &mut cache.dictionaries,
            planned.cold_labels.row,
            &mut encoded_labels,
        )?;
        let integrity_checked_label_count = encoded_labels.len();
        let metric_name_dropped_series_id = self.verify_and_select_encoded_labels::<DETAILED>(
            symbols,
            planned.expected_label_identity,
            &mut encoded_labels,
            selection,
            materialization_profile,
        )?;
        finish_materialization_profile::<DETAILED>(
            materialization_profile,
            materialization_started,
        );
        Ok(GovernedVerifiedEncodedSeries {
            series_ref: planned.series_ref,
            series_id: planned.expected_label_identity,
            metric_name_dropped_series_id,
            kind_mask: planned.kind_mask,
            labels_complete: selection.labels_complete(),
            integrity_checked_label_count,
            labels: encoded_labels,
            _charge: encoded_charge,
        })
    }

    fn load_decoded_keyset_plan(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        keyset_id: u32,
    ) -> Result<DecodedKeysetPlan, Schema7MetadataReaderError> {
        let keyset = self.load_keyset(roots, keyset_id)?;
        self.validate_key_symbols(symbols, &keyset.values)?;
        let block = self.load_keyset_block(roots, keyset_id)?;
        self.record_series_result(cold_v2_reader::validate_keyset_block_key_count(
            &block.value,
            keyset.values.len(),
        ))?;
        Ok(DecodedKeysetPlan { keyset, block })
    }

    fn decode_encoded_labels(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        plan: &DecodedKeysetPlan,
        dictionaries: &mut Vec<(u32, GovernedDecodedVec<u32>)>,
        row_index: u32,
        encoded_labels: &mut Vec<(u32, u32)>,
    ) -> Result<(), Schema7MetadataReaderError> {
        let row = if plan.block.value.row_len_bytes == 0 {
            if row_index >= plan.block.value.rows {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 series cold row is out of bounds",
                )));
            }
            None
        } else {
            let range = self.record_series_result(cold_v2_reader::keyset_block_row_range(
                &plan.block.value,
                row_index,
            ))?;
            Some(self.read_authenticated_cold_range_owned(roots, range)?)
        };
        let row_bytes = match row.as_ref() {
            Some(row) => self.record_series_result(row.as_slice())?,
            None => &[],
        };
        let mut cursor = 0usize;
        for (index, key_sym) in plan.keyset.values.iter().copied().enumerate() {
            let dictionary_index =
                match dictionaries.binary_search_by_key(&key_sym, |(key_sym, _)| *key_sym) {
                    Ok(index) => index,
                    Err(insert_at) => {
                        let dictionary = self.find_value_dictionary(roots, symbols, key_sym)?;
                        dictionaries.insert(insert_at, (key_sym, dictionary));
                        insert_at
                    }
                };
            let dictionary = &dictionaries[dictionary_index].1;
            let width = *plan.block.value.widths.get(index).ok_or_else(|| {
                self.record_series_error(invalid_data("schema-7 keyset block width is missing"))
            })?;
            self.record_series_result(cold_v2_reader::validate_value_code_width(
                width,
                u32::try_from(dictionary.values.len()).map_err(|_| {
                    self.record_series_error(invalid_data(
                        "schema-7 value dictionary length exceeds u32",
                    ))
                })?,
            ))?;
            let code = self.record_series_result(cold_v2_reader::read_value_code(
                row_bytes,
                &mut cursor,
                width,
            ))?;
            let value_sym = dictionary
                .values
                .get(usize::try_from(code).map_err(|_| {
                    self.record_series_error(invalid_data("schema-7 value code exceeds usize"))
                })?)
                .copied()
                .ok_or_else(|| {
                    self.record_series_error(invalid_data("schema-7 value code is out of bounds"))
                })?;
            encoded_labels.push((key_sym, value_sym));
        }
        if cursor != row_bytes.len() {
            return Err(self
                .record_series_error(invalid_data("schema-7 series cold row has trailing bytes")));
        }
        Ok(())
    }

    /// Returns one logical cold range only after every intersecting physical
    /// page has been loaded, CRC-authenticated, and rebound to this root. A
    /// single-page range borrows its pinned page; only cross-page ranges need
    /// an owned assembly buffer.
    fn read_authenticated_cold_range(
        &self,
        roots: &BoundSchema7Roots,
        range: Range<u64>,
    ) -> Result<GovernedColdRange, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let header = roots.series_root().header;
        if range.start > range.end
            || range.start < header.keysets_offset
            || range.end > header.file_len
        {
            return Err(self.record_series_error(invalid_data(
                "schema-7 cold logical range is outside the cold stream",
            )));
        }
        let byte_len_u64 = range.end - range.start;
        let byte_len = usize::try_from(byte_len_u64)
            .map_err(|_| planning_error("schema-7 cold logical range length exceeds usize"))?;
        if range.is_empty() {
            return Ok(GovernedColdRange {
                bytes: GovernedColdRangeBytes::Empty,
                _charge: self.reserve_series_scratch(0)?,
            });
        }

        let first_page = (range.start - header.keysets_offset) / super::SERIES_COLD_PAGE_LEN_V1;
        let final_page = (range.end - 1 - header.keysets_offset) / super::SERIES_COLD_PAGE_LEN_V1;
        let page_count_u64 = final_page
            .checked_sub(first_page)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| planning_error("schema-7 cold page span overflows"))?;
        let page_count = usize::try_from(page_count_u64)
            .map_err(|_| planning_error("schema-7 cold page span exceeds usize"))?;
        if page_count == 1 {
            let page_index = u32::try_from(first_page)
                .map_err(|_| planning_error("schema-7 cold page index exceeds u32"))?;
            let descriptor = roots.cold_descriptor(page_index)?;
            let page = self.load_cold_page(roots, page_index)?;
            let page_bytes =
                self.record_series_result(page.bytes_for(header, page_index, descriptor))?;
            let page_start = header
                .keysets_offset
                .checked_add(
                    first_page
                        .checked_mul(super::SERIES_COLD_PAGE_LEN_V1)
                        .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?;
            let local_start = usize::try_from(range.start - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice start exceeds usize"))?;
            let local_end = usize::try_from(range.end - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice end exceeds usize"))?;
            if local_start > local_end
                || local_end > page_bytes.len()
                || local_end - local_start != byte_len
            {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 authenticated cold logical range is incomplete",
                )));
            }
            return Ok(GovernedColdRange {
                bytes: GovernedColdRangeBytes::Borrowed {
                    page,
                    header,
                    page_index,
                    descriptor,
                    range: local_start..local_end,
                },
                _charge: self.reserve_series_scratch(0)?,
            });
        }
        let declared = checked_vec_bytes::<MetadataCachePin<ValidatedSeriesColdPage>>(
            page_count,
            "schema-7 cold-page pin allocation charge overflows",
        )?
        .checked_add(byte_len_u64)
        .ok_or_else(|| planning_error("schema-7 cold-range allocation charge overflows"))?;
        let mut charge = self.reserve_series_scratch(declared)?;
        let mut pages =
            try_vec_with_capacity(page_count, "schema-7 cold-page pin allocation failed")?;
        let mut bytes = try_vec_with_capacity(byte_len, "schema-7 cold-range allocation failed")?;
        charge
            .reconcile(
                checked_vec_bytes::<MetadataCachePin<ValidatedSeriesColdPage>>(
                    pages.capacity(),
                    "schema-7 cold-page pin allocation charge overflows",
                )?
                .checked_add(checked_vec_bytes::<u8>(
                    bytes.capacity(),
                    "schema-7 cold-range allocation charge overflows",
                )?)
                .ok_or_else(|| planning_error("schema-7 cold-range allocation charge overflows"))?,
            )
            .map_err(MetadataCacheError::from)?;

        for page_index in first_page..=final_page {
            let page_index = u32::try_from(page_index)
                .map_err(|_| planning_error("schema-7 cold page index exceeds u32"))?;
            pages.push(self.load_cold_page(roots, page_index)?);
        }

        // Rebind every pin before returning any byte from the logical range.
        // Only after all pages succeed do we copy their intersecting slices.
        for (ordinal, page) in pages.iter().enumerate() {
            let page_index_u64 = first_page
                .checked_add(
                    u64::try_from(ordinal)
                        .map_err(|_| planning_error("schema-7 cold page ordinal exceeds u64"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page index overflows"))?;
            let page_index = u32::try_from(page_index_u64)
                .map_err(|_| planning_error("schema-7 cold page index exceeds u32"))?;
            let descriptor = roots.cold_descriptor(page_index)?;
            let page_bytes =
                self.record_series_result(page.bytes_for(header, page_index, descriptor))?;
            let page_start = header
                .keysets_offset
                .checked_add(
                    page_index_u64
                        .checked_mul(super::SERIES_COLD_PAGE_LEN_V1)
                        .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?;
            let page_end = page_start
                .checked_add(
                    u64::try_from(page_bytes.len())
                        .map_err(|_| planning_error("schema-7 cold page length exceeds u64"))?,
                )
                .ok_or_else(|| planning_error("schema-7 cold page end overflows"))?;
            let copy_start = range.start.max(page_start);
            let copy_end = range.end.min(page_end);
            if copy_start >= copy_end {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 authenticated cold page does not intersect its logical range",
                )));
            }
            let local_start = usize::try_from(copy_start - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice start exceeds usize"))?;
            let local_end = usize::try_from(copy_end - page_start)
                .map_err(|_| planning_error("schema-7 cold page slice end exceeds usize"))?;
            bytes.extend_from_slice(&page_bytes[local_start..local_end]);
        }
        if bytes.len() != byte_len {
            return Err(self.record_series_error(invalid_data(
                "schema-7 authenticated cold logical range is incomplete",
            )));
        }
        drop(pages);
        charge
            .reconcile(checked_vec_bytes::<u8>(
                bytes.capacity(),
                "schema-7 cold-range allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedColdRange {
            bytes: GovernedColdRangeBytes::Owned(bytes),
            _charge: charge,
        })
    }

    /// Returns owned bytes before the caller starts nested metadata reads or
    /// reserves decoded-output scratch. Retaining a transient 16-KiB page pin
    /// across that work would raise the minimum viable in-flight budget versus
    /// the original copy-and-release path. Leaf parsers may borrow directly.
    fn read_authenticated_cold_range_owned(
        &self,
        roots: &BoundSchema7Roots,
        range: Range<u64>,
    ) -> Result<GovernedColdRange, Schema7MetadataReaderError> {
        let governed = self.read_authenticated_cold_range(roots, range)?;
        let GovernedColdRange {
            bytes,
            _charge: existing_charge,
        } = governed;
        let GovernedColdRangeBytes::Borrowed {
            page,
            header,
            page_index,
            descriptor,
            range,
        } = bytes
        else {
            return Ok(GovernedColdRange {
                bytes,
                _charge: existing_charge,
            });
        };

        let byte_len = range.len();
        let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u8>(
            byte_len,
            "schema-7 owned cold-range allocation charge overflows",
        )?)?;
        let mut owned =
            try_vec_with_capacity(byte_len, "schema-7 owned cold-range allocation failed")?;
        charge
            .reconcile(checked_vec_bytes::<u8>(
                owned.capacity(),
                "schema-7 owned cold-range allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        let page_bytes =
            self.record_series_result(page.bytes_for(header, page_index, descriptor))?;
        let source = page_bytes.get(range).ok_or_else(|| {
            self.record_series_error(invalid_data(
                "schema-7 authenticated cold logical range is incomplete",
            ))
        })?;
        owned.extend_from_slice(source);
        drop(existing_charge);
        drop(page);
        Ok(GovernedColdRange {
            bytes: GovernedColdRangeBytes::Owned(owned),
            _charge: charge,
        })
    }

    fn load_keyset(
        &self,
        roots: &BoundSchema7Roots,
        keyset_id: u32,
    ) -> Result<GovernedDecodedVec<u32>, Schema7MetadataReaderError> {
        let header = roots.series_root().header;
        let range = self.load_cold_entry_range(
            roots,
            ColdSection {
                offset: header.keysets_offset,
                end: header.value_dicts_offset,
                count: header.num_keysets,
            },
            keyset_id,
        )?;
        let bytes = self.read_authenticated_cold_range_owned(roots, range.clone())?;
        let bytes_slice = self.record_series_result(bytes.as_slice())?;
        let declared_count = bytes_slice.len() / std::mem::size_of::<u32>();
        let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u32>(
            declared_count,
            "schema-7 decoded keyset allocation charge overflows",
        )?)?;
        let values = self.record_series_result(cold_v2_reader::decode_keyset_entry(
            bytes_slice,
            range.start,
            range.end,
        ))?;
        charge
            .reconcile(checked_vec_bytes::<u32>(
                values.capacity(),
                "schema-7 decoded keyset allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedDecodedVec {
            values,
            _charge: charge,
        })
    }

    fn load_keyset_block(
        &self,
        roots: &BoundSchema7Roots,
        keyset_id: u32,
    ) -> Result<GovernedBlockMeta, Schema7MetadataReaderError> {
        let header = roots.series_root().header;
        let range = self.load_cold_entry_range(
            roots,
            ColdSection {
                offset: header.keyset_blocks_offset,
                end: header.file_len,
                count: header.num_keysets,
            },
            keyset_id,
        )?;
        let fixed_range = self.record_series_result(cold_v2_reader::keyset_block_header_range(
            range.start,
            range.end,
        ))?;
        let fixed = self.read_authenticated_cold_range_owned(roots, fixed_range)?;
        let fixed_slice = self.record_series_result(fixed.as_slice())?;
        let widths_range = self.record_series_result(cold_v2_reader::keyset_block_widths_range(
            fixed_slice,
            range.start,
            range.end,
        ))?;
        let widths = self.read_authenticated_cold_range_owned(roots, widths_range)?;
        let widths_slice = self.record_series_result(widths.as_slice())?;
        let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u8>(
            widths_slice.len(),
            "schema-7 decoded width allocation charge overflows",
        )?)?;
        let value = self.record_series_result(cold_v2_reader::decode_keyset_block_meta(
            fixed_slice,
            widths_slice,
            range.start,
            range.end,
        ))?;
        charge
            .reconcile(checked_vec_bytes::<u8>(
                value.widths.capacity(),
                "schema-7 decoded width allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedBlockMeta {
            value,
            _charge: charge,
        })
    }

    fn find_value_dictionary(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        key_sym: u32,
    ) -> Result<GovernedDecodedVec<u32>, Schema7MetadataReaderError> {
        let header = roots.series_root().header;
        let section = ColdSection {
            offset: header.value_dicts_offset,
            end: header.keyset_blocks_offset,
            count: header.num_value_dicts,
        };
        let mut low = 0u32;
        let mut high = section.count;
        while low < high {
            let mid = low + (high - low) / 2;
            let meta = self.load_value_dictionary_meta(roots, section, mid)?;
            match meta.value.key_sym.cmp(&key_sym) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => {
                    let values_range = meta.value.values_offset..meta.entry_end;
                    let bytes = self.read_authenticated_cold_range_owned(roots, values_range)?;
                    let declared_count = usize::try_from(meta.value.cardinality).map_err(|_| {
                        planning_error("schema-7 value dictionary count exceeds usize")
                    })?;
                    let mut charge = self.reserve_series_scratch(checked_vec_bytes::<u32>(
                        declared_count,
                        "schema-7 value dictionary allocation charge overflows",
                    )?)?;
                    let bytes_slice = self.record_series_result(bytes.as_slice())?;
                    let values = self.record_series_result(
                        cold_v2_reader::decode_value_dict_values(bytes_slice, meta.value),
                    )?;
                    self.validate_value_dictionary(symbols, &values)?;
                    charge
                        .reconcile(checked_vec_bytes::<u32>(
                            values.capacity(),
                            "schema-7 value dictionary allocation charge overflows",
                        )?)
                        .map_err(MetadataCacheError::from)?;
                    return Ok(GovernedDecodedVec {
                        values,
                        _charge: charge,
                    });
                }
            }
        }
        Err(self.record_series_error(invalid_data("schema-7 value dictionary is missing")))
    }

    fn load_value_dictionary_meta(
        &self,
        roots: &BoundSchema7Roots,
        section: ColdSection,
        dict_id: u32,
    ) -> Result<ValueDictEntryMeta, Schema7MetadataReaderError> {
        let range = self.load_cold_entry_range(roots, section, dict_id)?;
        let header_range = self.record_series_result(cold_v2_reader::value_dict_header_range(
            range.start,
            range.end,
        ))?;
        let bytes = self.read_authenticated_cold_range(roots, header_range)?;
        let bytes_slice = self.record_series_result(bytes.as_slice())?;
        let value = self.record_series_result(cold_v2_reader::decode_value_dict_meta(
            bytes_slice,
            range.start,
            range.end,
        ))?;
        Ok(ValueDictEntryMeta {
            value,
            entry_end: range.end,
        })
    }

    fn load_cold_entry_range(
        &self,
        roots: &BoundSchema7Roots,
        section: ColdSection,
        entry_index: u32,
    ) -> Result<Range<u64>, Schema7MetadataReaderError> {
        let pair_range = self.record_series_result(cold_v2_reader::offset_pair_range(
            section.offset,
            section.end,
            section.count,
            entry_index,
        ))?;
        let bytes = self.read_authenticated_cold_range(roots, pair_range)?;
        let bytes_slice = self.record_series_result(bytes.as_slice())?;
        self.record_series_result(cold_v2_reader::decode_entry_range(
            bytes_slice,
            section.offset,
            section.end,
            section.count,
            entry_index,
        ))
    }

    fn validate_value_dictionary(
        &self,
        symbols: &GovernedSymbolSession,
        values: &[u32],
    ) -> Result<(), Schema7MetadataReaderError> {
        let mut previous = None;
        for &value in values {
            if previous.is_some_and(|previous| previous >= value) {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 value dictionary symbols are not strictly increasing",
                )));
            }
            if usize::try_from(value).map_or(true, |value| value >= symbols.len()) {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 value dictionary symbol exceeds the bound symbol count",
                )));
            }
            previous = Some(value);
        }
        Ok(())
    }

    fn validate_key_symbols(
        &self,
        symbols: &GovernedSymbolSession,
        key_symbols: &[u32],
    ) -> Result<(), Schema7MetadataReaderError> {
        for &key_sym in key_symbols {
            if usize::try_from(key_sym).map_or(true, |key_sym| key_sym >= symbols.len()) {
                return Err(self.record_series_error(invalid_data(
                    "schema-7 key symbol exceeds the bound symbol count",
                )));
            }
        }
        Ok(())
    }

    fn materialize_and_verify_canonical_labels<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        expected_series_id: u64,
        encoded_labels: &[(u32, u32)],
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<MaterializedCanonicalLabels, Schema7MetadataReaderError> {
        let label_construction_started = detailed_stage_started::<DETAILED>();
        let output_capacity = selection.output_capacity(encoded_labels.len());
        let declared = checked_vec_bytes::<(String, String)>(
            output_capacity,
            "schema-7 canonical-label vector charge overflows",
        )?;
        let mut charge = self.reserve_series_scratch(declared)?;
        let mut labels = try_vec_with_capacity(
            output_capacity,
            "schema-7 canonical-label vector allocation failed",
        )?;
        let mut charged_bytes = checked_vec_bytes::<(String, String)>(
            labels.capacity(),
            "schema-7 canonical-label vector charge overflows",
        )?;
        charge
            .reconcile(charged_bytes)
            .map_err(MetadataCacheError::from)?;
        if DETAILED {
            materialization_profile.label_construction =
                materialization_profile.label_construction.saturating_add(
                    label_construction_started
                        .expect("detailed label-construction timer exists")
                        .elapsed(),
                );
        }
        let mut hash = XxHash64::default();
        let mut metric_name_dropped_hash = selection
            .derives_metric_name_dropped_identity()
            .then(XxHash64::default);
        for &(key_sym, value_sym) in encoded_labels {
            let mut include_in_metric_name_dropped_identity = true;
            let key = self.resolve_canonical_component::<DETAILED>(
                symbols,
                key_sym,
                0,
                &mut hash,
                &mut charge,
                &mut charged_bytes,
                materialization_profile,
                "schema-7 canonical label-name allocation failed",
                |resolved| {
                    include_in_metric_name_dropped_identity = resolved != METRIC_NAME_LABEL;
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0]);
                    }
                    selection.includes(resolved)
                },
            )?;
            let selected = key.is_some();
            let value = self.resolve_canonical_component::<DETAILED>(
                symbols,
                value_sym,
                0xff,
                &mut hash,
                &mut charge,
                &mut charged_bytes,
                materialization_profile,
                "schema-7 canonical label-value allocation failed",
                |resolved| {
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0xff]);
                    }
                    selected
                },
            )?;
            let construction_started = detailed_stage_started::<DETAILED>();
            match (key, value) {
                (Some(key), Some(value)) => labels.push((key, value)),
                (None, None) => {}
                _ => unreachable!("label name and value ownership selection must stay aligned"),
            }
            if DETAILED {
                materialization_profile.label_construction =
                    materialization_profile.label_construction.saturating_add(
                        construction_started
                            .expect("detailed label-push timer exists")
                            .elapsed(),
                    );
            }
        }
        let identity_started = detailed_stage_started::<DETAILED>();
        let actual_series_id = hash.finish();
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    identity_started
                        .expect("detailed identity timer exists")
                        .elapsed(),
                );
        }
        if actual_series_id != expected_series_id {
            return Err(self.record_series_error(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "schema-7 series identity mismatch: expected={expected_series_id} actual={actual_series_id}"
                ),
            )));
        }
        debug_assert_eq!(charge.bytes(), charged_bytes);
        let secondary_identity_started = detailed_stage_started::<DETAILED>();
        let metric_name_dropped_series_id = metric_name_dropped_hash.map(|hash| hash.finish());
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    secondary_identity_started
                        .expect("detailed secondary-identity timer exists")
                        .elapsed(),
                );
        }
        Ok((labels, charge, metric_name_dropped_series_id))
    }

    /// Verifies the complete canonical row without allocating per-component
    /// strings, then compacts the source-ID vector to the requested labels.
    /// Compaction may overwrite already-verified entries, but the shortened
    /// row is not exposed unless all later symbols and the final identity also
    /// succeed.
    fn verify_and_select_encoded_labels<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        expected_series_id: u64,
        encoded_labels: &mut Vec<(u32, u32)>,
        selection: CanonicalLabelSelection<'_>,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
    ) -> Result<Option<u64>, Schema7MetadataReaderError> {
        let mut hash = XxHash64::default();
        let mut metric_name_dropped_hash = selection
            .derives_metric_name_dropped_identity()
            .then(XxHash64::default);
        let mut output_len = 0usize;

        for read_index in 0..encoded_labels.len() {
            let (key_sym, value_sym) = encoded_labels[read_index];
            let mut selected = false;
            let mut include_in_metric_name_dropped_identity = true;
            self.visit_encoded_canonical_component::<DETAILED>(
                symbols,
                key_sym,
                0,
                &mut hash,
                materialization_profile,
                |resolved| {
                    include_in_metric_name_dropped_identity = resolved != METRIC_NAME_LABEL;
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0]);
                    }
                    selected = selection.includes(resolved);
                },
            )?;
            self.visit_encoded_canonical_component::<DETAILED>(
                symbols,
                value_sym,
                0xff,
                &mut hash,
                materialization_profile,
                |resolved| {
                    if include_in_metric_name_dropped_identity
                        && let Some(hash) = metric_name_dropped_hash.as_mut()
                    {
                        hash.update(resolved.as_bytes());
                        hash.update(&[0xff]);
                    }
                },
            )?;
            if selected {
                let construction_started = detailed_stage_started::<DETAILED>();
                encoded_labels[output_len] = (key_sym, value_sym);
                output_len += 1;
                if DETAILED {
                    materialization_profile.label_construction =
                        materialization_profile.label_construction.saturating_add(
                            construction_started
                                .expect("detailed encoded-label timer exists")
                                .elapsed(),
                        );
                }
            }
        }

        let identity_started = detailed_stage_started::<DETAILED>();
        let actual_series_id = hash.finish();
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    identity_started
                        .expect("detailed encoded-identity timer exists")
                        .elapsed(),
                );
        }
        if actual_series_id != expected_series_id {
            return Err(self.record_series_error(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "schema-7 series identity mismatch: expected={expected_series_id} actual={actual_series_id}"
                ),
            )));
        }

        let secondary_identity_started = detailed_stage_started::<DETAILED>();
        let metric_name_dropped_series_id = metric_name_dropped_hash.map(|hash| hash.finish());
        if DETAILED {
            materialization_profile.canonical_identity =
                materialization_profile.canonical_identity.saturating_add(
                    secondary_identity_started
                        .expect("detailed encoded secondary-identity timer exists")
                        .elapsed(),
                );
        }
        encoded_labels.truncate(output_len);
        Ok(metric_name_dropped_series_id)
    }

    fn visit_encoded_canonical_component<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        symbol_id: u32,
        delimiter: u8,
        hash: &mut XxHash64,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
        visit_resolved: impl FnOnce(&str),
    ) -> Result<(), Schema7MetadataReaderError> {
        let resolution_started = detailed_stage_started::<DETAILED>();
        let identity_before = materialization_profile.canonical_identity;
        let visit = symbols.visit_required_resolved(symbol_id, |resolved| {
            let identity_started = detailed_stage_started::<DETAILED>();
            hash.update(resolved.as_bytes());
            hash.update(&[delimiter]);
            visit_resolved(resolved);
            if DETAILED {
                materialization_profile.canonical_identity =
                    materialization_profile.canonical_identity.saturating_add(
                        identity_started
                            .expect("detailed encoded component timer exists")
                            .elapsed(),
                    );
            }
            Ok(())
        });
        if DETAILED {
            let identity_elapsed = materialization_profile
                .canonical_identity
                .saturating_sub(identity_before);
            materialization_profile.symbol_resolution =
                materialization_profile.symbol_resolution.saturating_add(
                    resolution_started
                        .expect("detailed encoded resolution timer exists")
                        .elapsed()
                        .saturating_sub(identity_elapsed),
                );
        }
        visit.map_err(Schema7MetadataReaderError::from)
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_canonical_component<const DETAILED: bool>(
        &self,
        symbols: &GovernedSymbolSession,
        symbol_id: u32,
        delimiter: u8,
        hash: &mut XxHash64,
        charge: &mut MetadataCharge,
        charged_bytes: &mut u64,
        materialization_profile: &mut CanonicalLabelMaterializationProfile,
        allocation_message: &'static str,
        should_own: impl FnOnce(&str) -> bool,
    ) -> Result<Option<String>, Schema7MetadataReaderError> {
        let resolution_started = detailed_stage_started::<DETAILED>();
        let identity_before = materialization_profile.canonical_identity;
        let construction_before = materialization_profile.label_construction;
        let mut owned = None;
        let mut deferred_error = None;
        let visit = symbols.visit_required_resolved(symbol_id, |resolved| {
            let identity_started = detailed_stage_started::<DETAILED>();
            hash.update(resolved.as_bytes());
            hash.update(&[delimiter]);
            let should_own = should_own(resolved);
            if DETAILED {
                materialization_profile.canonical_identity =
                    materialization_profile.canonical_identity.saturating_add(
                        identity_started
                            .expect("detailed identity timer exists")
                            .elapsed(),
                    );
            }
            let construction_started = detailed_stage_started::<DETAILED>();
            if !should_own {
                if DETAILED {
                    materialization_profile.label_construction =
                        materialization_profile.label_construction.saturating_add(
                            construction_started
                                .expect("detailed label-construction timer exists")
                                .elapsed(),
                        );
                }
                return Ok(());
            }
            let requested_bytes = u64::try_from(resolved.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical label length exceeds u64",
                )
            })?;
            let requested_total =
                (*charged_bytes)
                    .checked_add(requested_bytes)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            "schema-7 canonical-label charge overflows",
                        )
                    })?;
            if let Err(error) = charge.reconcile(requested_total) {
                deferred_error = Some(Schema7MetadataReaderError::Cache(MetadataCacheError::from(
                    error,
                )));
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical-label reservation was refused",
                ));
            }
            let mut value = String::new();
            value
                .try_reserve_exact(resolved.len())
                .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, allocation_message))?;
            let actual_bytes = u64::try_from(value.capacity()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical label capacity exceeds u64",
                )
            })?;
            let actual_total = (*charged_bytes).checked_add(actual_bytes).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical-label charge overflows",
                )
            })?;
            if let Err(error) = charge.reconcile(actual_total) {
                deferred_error = Some(Schema7MetadataReaderError::Cache(MetadataCacheError::from(
                    error,
                )));
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "schema-7 canonical-label capacity reconciliation was refused",
                ));
            }
            *charged_bytes = actual_total;
            value.push_str(resolved);
            owned = Some(value);
            if DETAILED {
                materialization_profile.label_construction =
                    materialization_profile.label_construction.saturating_add(
                        construction_started
                            .expect("detailed label-construction timer exists")
                            .elapsed(),
                    );
            }
            Ok(())
        });
        if DETAILED {
            let attributed_in_callback = materialization_profile
                .canonical_identity
                .saturating_sub(identity_before)
                .saturating_add(
                    materialization_profile
                        .label_construction
                        .saturating_sub(construction_before),
                );
            materialization_profile.symbol_resolution =
                materialization_profile.symbol_resolution.saturating_add(
                    resolution_started
                        .expect("detailed symbol-resolution timer exists")
                        .elapsed()
                        .saturating_sub(attributed_in_callback),
                );
        }
        if let Some(error) = deferred_error {
            return Err(error);
        }
        visit?;
        Ok(owned)
    }

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

    /// Loads and fully integration-validates one exact overflow blob.
    fn load_overflow_blob(
        &self,
        roots: &BoundSchema7Roots,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<MetadataCachePin<ValidatedOverflowBlob>, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        let ChunkLocatorSource::Overflow { locator, .. } = planned.chunks else {
            return Err(planning_error(
                "schema-7 inline series has no overflow blob",
            ));
        };
        let reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        let key = cache_key(
            &reader,
            locator.blob_offset,
            u64::from(locator.blob_len),
            MetadataCacheClass::OverflowBlob,
        )?;
        let declared = ValidatedOverflowBlob::declared_max_bytes(locator)
            .map_err(MetadataCacheError::from_io)?;
        let header = roots.series_root().header;
        let overflow_root = *roots.overflow_root();
        let overflow_blobs = roots.overflow_blobs;
        let chunk_file_lens = self.chunk_file_lens;
        let blob = reader.get_or_load_owned(key, declared, move |bytes| {
            let blob = ValidatedOverflowBlob::decode_physical_owned(
                bytes,
                header,
                &overflow_root,
                overflow_blobs,
                locator.blob_offset,
                chunk_file_lens,
            )
            .map_err(MetadataCacheError::from_io)?;
            let charged = blob.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(blob, charged))
        });
        let blob = blob?;
        if let Err(error) = blob.validate_bound_context(
            header,
            &overflow_root,
            overflow_blobs,
            planned.value,
            chunk_file_lens,
        ) {
            return Err(self.record_cross_artifact_error(error));
        }
        Ok(blob)
    }

    /// Resolves an overflow-backed series and retains scratch accounting for
    /// both flat vectors until the caller drops the result.
    pub(crate) fn plan_overflow_blob(
        &self,
        roots: &BoundSchema7Roots,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedChunkLocatorBatch, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        let ChunkLocatorSource::Overflow { locator, .. } = planned.chunks else {
            return Err(planning_error(
                "schema-7 inline series has no overflow blob",
            ));
        };
        let declared = checked_batch_bytes(
            usize::try_from(locator.chunk_count)
                .map_err(|_| planning_error("schema-7 overflow chunk count exceeds usize"))?,
            1,
        )?;
        let mut charge = self
            .guard
            .reader(SegmentFile::ChunkIndex)?
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let blob = self.load_overflow_blob(roots, planned)?;
        let value = plan_schema7_decoded_overflow_blob(
            roots.series_root().header,
            roots.overflow_root(),
            roots.overflow_blobs,
            planned.value,
            &blob,
            self.chunk_file_lens,
        )
        .map_err(Schema7MetadataReaderError::Planning)?;
        let (locator_capacity, span_capacity) = value.capacities();
        charge
            .reconcile(checked_batch_bytes(locator_capacity, span_capacity)?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedChunkLocatorBatch {
            value,
            _charge: charge,
        })
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
