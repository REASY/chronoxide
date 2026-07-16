//! Strict governed runtime boundary for schema-7 series metadata.
//!
//! The long-lived reader retains only the registered segment and immutable
//! facts discovered during open. A query session owns the lifecycle guard;
//! decoded roots, pages, and overflow blobs remain independent cache values
//! whose pins are held only for the operation that needs them.

use std::io;
use std::ops::{Deref, Range};

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

type MaterializedCanonicalLabels = (Vec<(String, String)>, MetadataCharge, Option<u64>);

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
        self.materialize_verified_with_selection(
            roots,
            symbols,
            planned,
            CanonicalLabelSelection::All,
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
        self.materialize_verified_with_selection(
            roots,
            symbols,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
        )
    }

    fn materialize_verified_with_selection(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
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
            .materialize_and_verify_canonical_labels(
                symbols,
                planned.expected_label_identity,
                &encoded_labels,
                selection,
            )?;
        let integrity_checked_label_count = encoded_labels.len();
        drop(encoded_labels);
        drop(encoded_charge);

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
        self.materialize_verified_selected_cached_impl(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::All,
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
        self.materialize_verified_selected_cached_impl(
            roots,
            symbols,
            context,
            planned,
            CanonicalLabelSelection::Requested {
                names: requested_label_names,
                derive_metric_name_dropped_identity,
            },
        )
    }

    fn materialize_verified_selected_cached_impl(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        context: &mut Schema7MaterializationContext,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(&context.provenance)?;
        self.ensure_provenance(planned.provenance)?;
        symbols.ensure_same_generation(&self.guard)?;
        self.guard.reader(SegmentFile::Series)?.check_artifact()?;
        let Some(cache) = context.cache.as_ref() else {
            return self.materialize_verified_with_selection(roots, symbols, planned, selection);
        };
        let keyset_id = planned.cold_labels.keyset_id;
        if cache
            .plans
            .binary_search_by_key(&keyset_id, |(keyset_id, _)| *keyset_id)
            .is_err()
            && cache.plans.len() == cache.plans.capacity()
        {
            context.cache = None;
            return self.materialize_verified_with_selection(roots, symbols, planned, selection);
        }

        let result = match context.cache.as_mut() {
            Some(cache) => {
                self.materialize_verified_with_cache(roots, symbols, cache, planned, selection)
            }
            None => {
                return self
                    .materialize_verified_with_selection(roots, symbols, planned, selection);
            }
        };
        if result.as_ref().is_err_and(is_budget_error) {
            // Cached decoded values are an optimization, not semantic state.
            // Release them before retrying the established scalar path so a
            // tight in-flight budget does not become a new query failure.
            context.cache = None;
            return self.materialize_verified_with_selection(roots, symbols, planned, selection);
        }
        result
    }

    fn materialize_verified_with_cache(
        &self,
        roots: &BoundSchema7Roots,
        symbols: &GovernedSymbolSession,
        cache: &mut Schema7MaterializationCache,
        planned: GovernedPlannedSeriesRef<'_>,
        selection: CanonicalLabelSelection<'_>,
    ) -> Result<GovernedVerifiedSeries, Schema7MetadataReaderError> {
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
            .materialize_and_verify_canonical_labels(
                symbols,
                planned.expected_label_identity,
                &encoded_labels,
                selection,
            )?;
        let integrity_checked_label_count = encoded_labels.len();
        drop(encoded_labels);
        drop(encoded_charge);
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

    fn materialize_and_verify_canonical_labels(
        &self,
        symbols: &GovernedSymbolSession,
        expected_series_id: u64,
        encoded_labels: &[(u32, u32)],
        selection: CanonicalLabelSelection<'_>,
    ) -> Result<MaterializedCanonicalLabels, Schema7MetadataReaderError> {
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
        let mut hash = XxHash64::default();
        let mut metric_name_dropped_hash = selection
            .derives_metric_name_dropped_identity()
            .then(XxHash64::default);
        for &(key_sym, value_sym) in encoded_labels {
            let mut include_in_metric_name_dropped_identity = true;
            let key = self.resolve_canonical_component(
                symbols,
                key_sym,
                0,
                &mut hash,
                &mut charge,
                &mut charged_bytes,
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
            let value = self.resolve_canonical_component(
                symbols,
                value_sym,
                0xff,
                &mut hash,
                &mut charge,
                &mut charged_bytes,
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
            match (key, value) {
                (Some(key), Some(value)) => labels.push((key, value)),
                (None, None) => {}
                _ => unreachable!("label name and value ownership selection must stay aligned"),
            }
        }
        let actual_series_id = hash.finish();
        if actual_series_id != expected_series_id {
            return Err(self.record_series_error(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "schema-7 series identity mismatch: expected={expected_series_id} actual={actual_series_id}"
                ),
            )));
        }
        debug_assert_eq!(charge.bytes(), charged_bytes);
        Ok((
            labels,
            charge,
            metric_name_dropped_hash.map(|hash| hash.finish()),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_canonical_component(
        &self,
        symbols: &GovernedSymbolSession,
        symbol_id: u32,
        delimiter: u8,
        hash: &mut XxHash64,
        charge: &mut MetadataCharge,
        charged_bytes: &mut u64,
        allocation_message: &'static str,
        should_own: impl FnOnce(&str) -> bool,
    ) -> Result<Option<String>, Schema7MetadataReaderError> {
        let mut owned = None;
        let mut deferred_error = None;
        let visit = symbols.visit_required_resolved(symbol_id, |resolved| {
            hash.update(resolved.as_bytes());
            hash.update(&[delimiter]);
            if !should_own(resolved) {
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
            Ok(())
        });
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
mod tests {
    use std::fs;

    use crc32c::crc32c;
    use tempfile::TempDir;

    use crate::storage::chunk::{
        ChunkIndexRange, ChunkKind, ChunkOverflowBlobV1, OverflowChunkEntryV1,
        encode_chunk_index_v2,
    };
    use crate::storage::metadata_cache::{LIVE_REGISTRY_ENTRY_BYTES, RESIDENT_ENTRY_BYTES};
    use crate::storage::metadata_governor::MetadataGovernorConfig;
    use crate::storage::metadata_runtime::{SegmentArtifactRegistration, StoreMetadataRuntime};
    use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;
    use crate::storage::series::SeriesEntry;
    use crate::storage::series::cold_v2::SeriesColdV2Plan;
    use crate::storage::symbols::{GovernedSymbolReader, write_symbols_bin_v3};

    use super::super::{
        InlineChunkV3, OverflowChunksV3, SeriesColdPageDescriptorV1, SeriesHeaderV3Params,
        SeriesHotLocationV3, SeriesHotV3, encode_series_hot_page_v1, encode_series_root_v3,
    };
    use super::*;

    const SEGMENT_START_MS: u64 = 1_000;
    const SEGMENT_END_MS: u64 = 2_000;
    const CHUNK_FILE_LENS: [u64; 2] = [256, 128];

    struct Fixture {
        _directory: TempDir,
        runtime: StoreMetadataRuntime,
        registered: Option<RegisteredSegment>,
        context: Schema7RootBindingContext,
        root_len: u64,
        cold_bytes: Vec<u8>,
        entries: Vec<SeriesEntry>,
    }

    #[derive(Clone, Copy, Default)]
    struct FixtureOptions {
        corrupt_header: bool,
        corrupt_series_root_suffix: bool,
        corrupt_hot: bool,
        corrupt_cold: bool,
        corrupt_overflow_root: bool,
        corrupt_blob: bool,
        duplicate_blob_locator: bool,
        identity_mismatch: bool,
        substitute_row: bool,
        multi_label: bool,
        cross_page_keyset: bool,
        corrupt_second_cold_page: bool,
        corrupt_last_symbol_page: bool,
        symbol_count_limit: Option<usize>,
    }

    fn runtime() -> StoreMetadataRuntime {
        runtime_with_budgets(1024 * 1024, 1024 * 1024)
    }

    fn runtime_with_budgets(
        retained_max_bytes: u64,
        in_flight_max_bytes: u64,
    ) -> StoreMetadataRuntime {
        StoreMetadataRuntime::new(MetadataGovernorConfig {
            retained_max_bytes,
            in_flight_max_bytes,
            max_open_files: 1,
            max_cached_open_files: 0,
        })
        .expect("valid one-descriptor runtime")
    }

    fn fixture(
        identity: &str,
        corrupt_header: bool,
        corrupt_hot: bool,
        corrupt_blob: bool,
    ) -> Fixture {
        fixture_with_runtime(
            identity,
            FixtureOptions {
                corrupt_header,
                corrupt_hot,
                corrupt_blob,
                ..FixtureOptions::default()
            },
            runtime(),
        )
    }

    fn fixture_with_runtime(
        identity: &str,
        options: FixtureOptions,
        runtime: StoreMetadataRuntime,
    ) -> Fixture {
        let directory = TempDir::new().expect("create schema-7 runtime fixture directory");

        let overflow_entries = vec![
            overflow_entry(64, SEGMENT_START_MS + 2),
            overflow_entry(104, SEGMENT_START_MS + 3),
        ];
        let mut chunk_index = encode_chunk_index_v2(
            2,
            &[ChunkOverflowBlobV1 {
                series_ref: 1,
                entries: overflow_entries,
            }],
        )
        .expect("encode schema-7 overflow index");
        let locator = chunk_index.blob_locators[0];

        let (symbols, entries) = if options.cross_page_keyset {
            cross_page_fixture_data()
        } else {
            let symbols = fixture_symbols();
            let entries = if options.multi_label {
                multi_label_fixture_entries(&symbols)
            } else {
                fixture_entries(&symbols)
            };
            (symbols, entries)
        };
        let cold = SeriesColdV2Plan::build(&entries).expect("build schema-7 cold fixture");
        let cold_lengths = cold.lengths();

        let header = SeriesHeaderV3::new(SeriesHeaderV3Params {
            num_series: 2,
            num_keysets: cold.num_keysets(),
            num_value_dicts: cold.num_value_dicts(),
            chunk_index_root_crc32c: chunk_index.root.root_crc32c,
            keysets_len: cold_lengths.keysets,
            value_dicts_len: cold_lengths.value_dicts,
            keyset_blocks_len: cold_lengths.keyset_blocks,
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            chunk_index_file_len: chunk_index.root.file_len,
        })
        .expect("construct schema-7 series header");
        let cold_rows = cold.series_rows();
        let mut records = vec![
            SeriesHotV3 {
                series_id: entries[0].series_id,
                keyset_id: cold_rows[0].keyset_id,
                row: cold_rows[0].row,
                kind_mask: 1,
                location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                    chunk_kind: ChunkKind::Float as u8,
                    file_id: 0,
                    scalar_lane_len: 0,
                    min_time_delta_ms: 0,
                    max_time_delta_ms: 1,
                    file_offset: 0,
                    chunk_length: 40,
                    indexed_prefix_crc32c: 0x1111_1111,
                }),
            },
            SeriesHotV3 {
                series_id: entries[1].series_id,
                keyset_id: cold_rows[1].keyset_id,
                row: cold_rows[1].row,
                kind_mask: 1,
                location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                    blob_offset: locator.blob_offset,
                    blob_len: locator.blob_len,
                    chunk_count: locator.chunk_count,
                }),
            },
        ];
        if options.identity_mismatch {
            records[0].series_id ^= 1;
        }
        if options.substitute_row {
            assert_eq!(records[0].keyset_id, records[1].keyset_id);
            records[0].row = records[1].row;
        }
        if options.duplicate_blob_locator {
            records[0].location = SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                blob_offset: locator.blob_offset,
                blob_len: locator.blob_len,
                chunk_count: locator.chunk_count,
            });
        }
        let (hot_descriptor, mut hot_page) =
            encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS)
                .expect("encode schema-7 hot page");
        let cold_offsets = cold
            .section_offsets_at(header.keysets_offset)
            .expect("derive schema-7 cold offsets");
        let mut cold_bytes = Vec::new();
        cold.write_sections_at(&mut cold_bytes, cold_offsets)
            .expect("encode schema-7 cold bytes");
        let cold_descriptors = cold_bytes
            .chunks(super::super::SERIES_COLD_PAGE_LEN_V1 as usize)
            .enumerate()
            .map(|(page_index, bytes)| {
                SeriesColdPageDescriptorV1::new(
                    header,
                    u32::try_from(page_index).expect("cold page index fits u32"),
                    crc32c(bytes),
                )
                .expect("construct schema-7 cold descriptor")
            })
            .collect::<Vec<_>>();
        let (header, mut root) =
            encode_series_root_v3(header, &[hot_descriptor], &cold_descriptors)
                .expect("encode schema-7 series root");

        if options.corrupt_header {
            root[0] ^= 1;
        }
        if options.corrupt_series_root_suffix {
            root[SERIES_HEADER_LEN_V3] ^= 1;
        }
        if options.corrupt_hot {
            hot_page[8_192] ^= 1;
        }
        let mut cold_bytes = cold_bytes;
        if options.corrupt_cold {
            cold_bytes[0] ^= 1;
        }
        if options.corrupt_second_cold_page {
            cold_bytes[super::super::SERIES_COLD_PAGE_LEN_V1 as usize] ^= 1;
        }
        if options.corrupt_overflow_root {
            chunk_index.bytes[0] ^= 1;
        }
        if options.corrupt_blob {
            let last = chunk_index.bytes.len() - 1;
            chunk_index.bytes[last] ^= 1;
        }

        let mut symbol_bytes = Vec::new();
        let encoded_symbol_count = options.symbol_count_limit.unwrap_or(symbols.len());
        write_symbols_bin_v3(
            &mut symbol_bytes,
            symbols
                .get(..encoded_symbol_count)
                .expect("fixture symbol limit is in range")
                .iter(),
        )
        .expect("encode schema-7 symbols fixture");
        if options.corrupt_last_symbol_page {
            *symbol_bytes
                .last_mut()
                .expect("fixture symbols must contain a physical page") ^= 1;
        }

        let mut series = root;
        series.extend_from_slice(&hot_page);
        series.extend_from_slice(&cold_bytes);
        assert_eq!(series.len() as u64, header.file_len);

        let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
            let bytes: &[u8] = match file {
                SegmentFile::MetaJson => b"{}",
                SegmentFile::Symbols => &symbol_bytes,
                SegmentFile::Series => &series,
                SegmentFile::Chunks => &[0; CHUNK_FILE_LENS[0] as usize],
                SegmentFile::OooChunks => &[0; CHUNK_FILE_LENS[1] as usize],
                SegmentFile::ChunkIndex => &chunk_index.bytes,
                SegmentFile::Indexes => b"indexes",
                SegmentFile::Footer => unreachable!("footer is not runtime-inventoried"),
            };
            let path = directory.path().join(file.filename());
            fs::write(&path, bytes).expect("write schema-7 runtime artifact");
            SegmentArtifactRegistration::new(file, path, bytes.len() as u64)
        });
        let registered = runtime
            .register_segment(identity, &artifacts)
            .expect("register schema-7 runtime fixture");

        Fixture {
            _directory: directory,
            runtime,
            registered: Some(registered),
            context: Schema7RootBindingContext {
                series_file_len: header.file_len,
                chunk_index_file_len: chunk_index.root.file_len,
                segment_start_ms: SEGMENT_START_MS,
                segment_end_ms: SEGMENT_END_MS,
                series_count: 2,
            },
            root_len: header.hot_pages_offset,
            cold_bytes,
            entries,
        }
    }

    fn fixture_symbols() -> Vec<String> {
        (0..=5)
            .map(|symbol_id| format!("s{symbol_id:02}"))
            .collect()
    }

    fn fixture_entries(symbols: &[String]) -> Vec<SeriesEntry> {
        let mut entries = vec![
            SeriesEntry {
                series_id: 0,
                kind_mask: 1,
                chunk_index: ChunkIndexRange::default(),
                labels: vec![(1, 3)],
            },
            SeriesEntry {
                series_id: 0,
                kind_mask: 1,
                chunk_index: ChunkIndexRange::default(),
                labels: vec![(1, 4)],
            },
        ];
        for entry in &mut entries {
            let mut hash = XxHash64::default();
            for &(key_sym, value_sym) in &entry.labels {
                hash.update(symbols[key_sym as usize].as_bytes());
                hash.update(&[0]);
                hash.update(symbols[value_sym as usize].as_bytes());
                hash.update(&[0xff]);
            }
            entry.series_id = hash.finish();
        }
        entries
    }

    fn multi_label_fixture_entries(symbols: &[String]) -> Vec<SeriesEntry> {
        let mut entries = fixture_entries(symbols);
        for entry in &mut entries {
            entry.labels.push((2, 5));
            let mut hash = XxHash64::default();
            for &(key_sym, value_sym) in &entry.labels {
                hash.update(symbols[key_sym as usize].as_bytes());
                hash.update(&[0]);
                hash.update(symbols[value_sym as usize].as_bytes());
                hash.update(&[0xff]);
            }
            entry.series_id = hash.finish();
        }
        entries
    }

    fn cross_page_fixture_data() -> (Vec<String>, Vec<SeriesEntry>) {
        const LARGE_KEY_COUNT: u32 = 4_087;
        let value_sym = LARGE_KEY_COUNT + 1;
        let symbols = (0..=value_sym)
            .map(|symbol_id| format!("s{symbol_id:04}"))
            .collect::<Vec<_>>();
        let mut entries = vec![
            SeriesEntry {
                series_id: 0,
                kind_mask: 1,
                chunk_index: ChunkIndexRange::default(),
                labels: (0..LARGE_KEY_COUNT)
                    .map(|key_sym| (key_sym, value_sym))
                    .collect(),
            },
            SeriesEntry {
                series_id: 0,
                kind_mask: 1,
                chunk_index: ChunkIndexRange::default(),
                labels: vec![(LARGE_KEY_COUNT, value_sym)],
            },
        ];
        for entry in &mut entries {
            let mut hash = XxHash64::default();
            for &(key_sym, value_sym) in &entry.labels {
                hash.update(symbols[key_sym as usize].as_bytes());
                hash.update(&[0]);
                hash.update(symbols[value_sym as usize].as_bytes());
                hash.update(&[0xff]);
            }
            entry.series_id = hash.finish();
        }
        (symbols, entries)
    }

    fn open_symbol_session(fixture: &Fixture) -> GovernedSymbolSession {
        let reader = GovernedSymbolReader::open(
            fixture
                .registered
                .as_ref()
                .expect("fixture owner available"),
        )
        .expect("open governed symbol reader");
        reader
            .query_session()
            .expect("open governed symbol session")
    }

    fn overflow_entry(offset: u64, timestamp_ms: u64) -> OverflowChunkEntryV1 {
        OverflowChunkEntryV1 {
            file_id: 0,
            kind: ChunkKind::Float,
            min_time_ms: timestamp_ms,
            max_time_ms: timestamp_ms,
            offset,
            length: 40,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
            indexed_prefix_crc32c: offset as u32,
        }
    }

    fn open_fixture(fixture: &mut Fixture) -> Schema7MetadataReader {
        Schema7MetadataReader::open(
            fixture
                .registered
                .as_ref()
                .expect("fixture owner available"),
            fixture.context,
        )
        .expect("open strict schema-7 metadata reader")
    }

    fn class_reads(
        runtime: &StoreMetadataRuntime,
        class: MetadataCacheClass,
    ) -> crate::storage::metadata_runtime::MetadataIssuedReadCount {
        runtime.snapshot().reads.classes[class.stable_index()].issued
    }

    fn read_delta(
        after: crate::storage::metadata_runtime::MetadataIssuedReadCount,
        before: crate::storage::metadata_runtime::MetadataIssuedReadCount,
    ) -> crate::storage::metadata_runtime::MetadataIssuedReadCount {
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: after.calls - before.calls,
            bytes: after.bytes - before.bytes,
        }
    }

    #[test]
    fn open_stages_exact_root_ranges_and_warm_roots_issue_no_io() {
        let mut fixture = fixture("schema7-roots", false, false, false);
        let before = fixture.runtime.snapshot();
        let reader = open_fixture(&mut fixture);
        let open_delta = fixture.runtime.snapshot().reads.delta_since(before.reads);

        assert_eq!(reader.segment_identity(), "schema7-roots");
        assert_eq!(reader.root_len(), fixture.root_len);
        assert_eq!(open_delta.issued.calls, 3);
        assert_eq!(open_delta.issued.bytes, fixture.root_len + 64);
        assert_eq!(
            open_delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 2,
                bytes: fixture.root_len,
            }
        );
        assert_eq!(
            open_delta.classes[MetadataCacheClass::OverflowRoot.stable_index()].issued,
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 1,
                bytes: 64,
            }
        );
        assert_eq!(fixture.runtime.snapshot().files.peak_open_files, 1);

        let session = reader.query_session().expect("open query session");
        let before_warm = fixture.runtime.snapshot();
        let roots = session.load_roots().expect("load warm roots");
        let series_root_charge = roots.series.charged_bytes();
        let overflow_root_charge = roots.overflow.charged_bytes();
        let root_usage = fixture.runtime.snapshot().governor;
        assert_eq!(
            root_usage
                .usage(MetadataUsageClass::Cache(MetadataCacheClass::SeriesRoot))
                .retained_bytes,
            series_root_charge + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
        );
        assert_eq!(
            root_usage
                .usage(MetadataUsageClass::Cache(MetadataCacheClass::OverflowRoot,))
                .retained_bytes,
            overflow_root_charge + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
        );
        let bound = session.bind(roots).expect("bind warm roots");
        let warm_delta = fixture.runtime.snapshot();
        assert_eq!(
            warm_delta.reads.delta_since(before_warm.reads).issued.calls,
            0
        );
        assert_eq!(warm_delta.cache.hits - before_warm.cache.hits, 2);
        assert_eq!(bound.series_pages().root_len, fixture.root_len);
        assert_eq!(bound.overflow_blobs().root_len, 64);
    }

    #[test]
    fn zero_retention_reads_transiently_without_resident_cache_entries() {
        let runtime = runtime_with_budgets(0, 1024 * 1024);
        let mut fixture = fixture_with_runtime(
            "schema7-zero-retention",
            FixtureOptions::default(),
            runtime.clone(),
        );

        let reader = open_fixture(&mut fixture);
        let after_open = runtime.snapshot();
        assert_eq!(after_open.cache.resident_entries, 0);
        assert_eq!(after_open.governor.retained_bytes, 0);
        assert_eq!(after_open.cache.active_loads, 0);
        assert_eq!(after_open.files.open_files, 0);

        let session = reader
            .query_session()
            .expect("open transient query session");
        let roots = session.load_roots().expect("load transient roots");
        let bound = session.bind(roots).expect("bind transient roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0, 1])
            .expect("plan transient hot page");
        assert_eq!(planned.len(), 2);

        let while_pinned = runtime.snapshot();
        assert_eq!(while_pinned.cache.resident_entries, 0);
        assert_eq!(while_pinned.governor.retained_bytes, 0);
        assert_eq!(while_pinned.cache.active_loads, 0);
        assert_eq!(while_pinned.files.open_files, 0);
        assert_eq!(
            while_pinned
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            planned.charged_bytes()
        );

        drop(planned);
        drop(bound);
        drop(session);
        drop(reader);
        let after_drop = runtime.snapshot();
        assert_eq!(after_drop.cache.resident_entries, 0);
        assert_eq!(after_drop.cache.live_allocations, 0);
        assert_eq!(after_drop.cache.active_loads, 0);
        assert_eq!(after_drop.governor.retained_bytes, 0);
        assert_eq!(
            after_drop
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            0
        );
        assert_eq!(after_drop.files.open_files, 0);
    }

    #[test]
    fn tiny_in_flight_budget_refuses_hot_page_before_io_without_poisoning() {
        let runtime = runtime_with_budgets(1024 * 1024, 16 * 1024);
        let mut fixture = fixture_with_runtime(
            "schema7-tiny-in-flight",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let reader = open_fixture(&mut fixture);
        let session = reader
            .query_session()
            .expect("open constrained query session");
        let roots = session.load_roots().expect("load constrained roots");
        let bound = session.bind(roots).expect("bind constrained roots");
        let before = runtime.snapshot();

        let error = session
            .plan_hot_page(&bound, 0, &[0, 1])
            .expect_err("hot-page scratch reservation must exceed the budget");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Budget(_))
        ));

        let after = runtime.snapshot();
        assert_eq!(after.reads, before.reads);
        assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
        assert_eq!(after.cache.active_loads, 0);
        assert_eq!(after.cache.live_allocations, before.cache.live_allocations);
        assert_eq!(
            after
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            before
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes
        );
        assert_eq!(
            after.governor.in_flight_bytes,
            before.governor.in_flight_bytes
        );
        assert_eq!(after.files.open_files, 0);
    }

    #[test]
    fn foreign_roots_bounds_and_plans_are_rejected_before_io_or_poisoning() {
        let runtime = runtime();
        let mut first = fixture_with_runtime(
            "schema7-provenance-a",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let mut second = fixture_with_runtime(
            "schema7-provenance-b",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let first_reader = open_fixture(&mut first);
        let second_reader = open_fixture(&mut second);
        let first_session = first_reader.query_session().expect("first query session");
        let second_session = second_reader.query_session().expect("second query session");

        let foreign_roots = first_session.load_roots().expect("first roots");
        let before_foreign_roots = runtime.snapshot();
        let error = second_session
            .bind(foreign_roots)
            .expect_err("foreign root pins must not bind");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::ForeignSegmentGeneration
        ));
        let after_foreign_roots = runtime.snapshot();
        assert_eq!(after_foreign_roots.reads, before_foreign_roots.reads);
        assert_eq!(
            after_foreign_roots.cache.sticky_artifacts,
            before_foreign_roots.cache.sticky_artifacts
        );

        let first_roots = first_session.load_roots().expect("reload first roots");
        let first_bound = first_session.bind(first_roots).expect("bind first roots");
        let second_roots = second_session.load_roots().expect("load second roots");
        let second_bound = second_session
            .bind(second_roots)
            .expect("bind second roots");
        let series_count = first_session
            .series_count_binding(&first_bound)
            .expect("mint first schema-7 series-count capability");
        assert_eq!(series_count.num_series(), 2);
        let before_foreign_count = runtime.snapshot();
        let error = second_session
            .series_count_binding(&first_bound)
            .expect_err("foreign bound roots must not mint a series-count capability");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::ForeignSegmentGeneration
        ));
        assert_eq!(runtime.snapshot().reads, before_foreign_count.reads);
        assert_eq!(
            runtime.snapshot().cache.sticky_artifacts,
            before_foreign_count.cache.sticky_artifacts
        );
        let first_planned = first_session
            .plan_hot_page(&first_bound, 0, &[1])
            .expect("plan first overflow series");

        let before_foreign_values = runtime.snapshot();
        let error = second_session
            .load_hot_page(&first_bound, 0)
            .expect_err("foreign bound roots must not load a page");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::ForeignSegmentGeneration
        ));
        let error = second_session
            .plan_overflow_blob(
                &second_bound,
                first_planned.get(0).expect("first planned overflow series"),
            )
            .expect_err("foreign planned series must not resolve a blob");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::ForeignSegmentGeneration
        ));
        let after_foreign_values = runtime.snapshot();
        assert_eq!(after_foreign_values.reads, before_foreign_values.reads);
        assert_eq!(
            after_foreign_values.cache.sticky_artifacts,
            before_foreign_values.cache.sticky_artifacts
        );
        assert_eq!(
            after_foreign_values
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            before_foreign_values
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes
        );
    }

    #[test]
    fn exact_hot_cold_and_blob_ranges_are_cached_and_planner_output_is_governed() {
        let mut fixture = fixture("schema7-touched", false, false, false);
        let reader = open_fixture(&mut fixture);
        let session = reader.query_session().expect("open query session");
        let roots = session.load_roots().expect("load roots");
        let bound = session.bind(roots).expect("bind roots");

        let before_hot = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        let planned = session
            .plan_hot_page(&bound, 0, &[0, 1])
            .expect("plan selected schema-7 series");
        let after_hot = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        assert_eq!(
            read_delta(after_hot, before_hot),
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 1,
                bytes: SERIES_HOT_PAGE_LEN_V1 as u64,
            }
        );
        assert_eq!(planned.len(), 2);
        assert!(!planned.is_empty());
        assert_eq!(planned.get(0).expect("first planned series").series_ref, 0);
        assert_eq!(planned.get(1).expect("second planned series").series_ref, 1);
        let after_hot_snapshot = fixture.runtime.snapshot();
        assert_eq!(
            after_hot_snapshot
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            planned.charged_bytes()
        );
        assert_eq!(
            after_hot_snapshot
                .governor
                .usage(MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage,))
                .retained_bytes,
            std::mem::size_of::<ValidatedSeriesHotPage>() as u64
                + SERIES_HOT_PAGE_LEN_V1 as u64
                + LIVE_REGISTRY_ENTRY_BYTES
                + RESIDENT_ENTRY_BYTES
        );

        let before_hot_hit = fixture.runtime.snapshot();
        let second_page = session
            .load_hot_page(&bound, 0)
            .expect("reuse authenticated hot page");
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .reads
                .delta_since(before_hot_hit.reads)
                .issued
                .calls,
            0
        );

        let before_cold = class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage);
        let cold = session
            .load_cold_page(&bound, 0)
            .expect("load authenticated cold page");
        assert_eq!(
            read_delta(
                class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage),
                before_cold,
            ),
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 1,
                bytes: fixture.cold_bytes.len() as u64,
            }
        );
        assert_eq!(
            cold.bytes_for(
                bound.series_root().header,
                0,
                bound.cold_descriptor(0).expect("cold descriptor"),
            )
            .expect("bind cold cache hit"),
            fixture.cold_bytes
        );
        let before_cold_hit = fixture.runtime.snapshot();
        let _second_cold = session
            .load_cold_page(&bound, 0)
            .expect("reuse authenticated cold page");
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .reads
                .delta_since(before_cold_hit.reads)
                .issued
                .calls,
            0
        );
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .governor
                .usage(MetadataUsageClass::Cache(
                    MetadataCacheClass::SeriesColdPage,
                ))
                .retained_bytes,
            std::mem::size_of::<ValidatedSeriesColdPage>() as u64
                + fixture.cold_bytes.len() as u64
                + LIVE_REGISTRY_ENTRY_BYTES
                + RESIDENT_ENTRY_BYTES
        );

        let scratch_with_planned = fixture
            .runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes;
        assert_eq!(scratch_with_planned, planned.charged_bytes());
        let before_blob = class_reads(&fixture.runtime, MetadataCacheClass::OverflowBlob);
        let overflow = planned.get(1).expect("overflow planned series");
        let batch = session
            .plan_overflow_blob(&bound, overflow)
            .expect("plan authenticated overflow locators");
        assert_eq!(batch.locators().len(), 2);
        assert_eq!(batch.series_spans().len(), 1);
        assert_eq!(
            read_delta(
                class_reads(&fixture.runtime, MetadataCacheClass::OverflowBlob),
                before_blob,
            ),
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 1,
                bytes: 32 + 2 * 44,
            }
        );
        let after_blob_snapshot = fixture.runtime.snapshot();
        assert_eq!(
            after_blob_snapshot
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            scratch_with_planned + batch.charged_bytes()
        );
        assert_eq!(
            after_blob_snapshot
                .governor
                .usage(MetadataUsageClass::Cache(MetadataCacheClass::OverflowBlob))
                .retained_bytes,
            std::mem::size_of::<ValidatedOverflowBlob>() as u64
                + (32 + 2 * 44) as u64
                + LIVE_REGISTRY_ENTRY_BYTES
                + RESIDENT_ENTRY_BYTES
        );
        let before_blob_hit = fixture.runtime.snapshot();
        let _second_blob = session
            .load_overflow_blob(&bound, overflow)
            .expect("reuse authenticated overflow blob");
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .reads
                .delta_since(before_blob_hit.reads)
                .issued
                .calls,
            0
        );

        drop(batch);
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            scratch_with_planned
        );
        drop(planned);
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            0
        );
        drop(second_page);
    }

    #[test]
    fn materialize_verified_returns_owned_canonical_labels_and_stable_identity() {
        let mut fixture = fixture("schema7-materialize", false, false, false);
        let expected = fixture_entries(&fixture_symbols());
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open materialization session");
        let roots = session.load_roots().expect("load materialization roots");
        let bound = session.bind(roots).expect("bind materialization roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan materialized series");
        let routed = planned.get(0).expect("planned materialized series");

        let before = fixture.runtime.snapshot();
        let before_symbols = symbols.logical_stats();
        let verified = session
            .materialize_verified(&bound, &symbols, routed)
            .expect("materialize and verify canonical labels");
        assert_eq!(verified.series_ref(), 0);
        assert_eq!(verified.series_id(), expected[0].series_id);
        assert_eq!(verified.kind_mask(), expected[0].kind_mask);
        assert_eq!(
            verified.labels(),
            &[(String::from("s01"), String::from("s03"))]
        );
        let expected_charge = (verified.labels.capacity() * std::mem::size_of::<(String, String)>())
            as u64
            + verified
                .labels
                .iter()
                .map(|(key, value)| (key.capacity() + value.capacity()) as u64)
                .sum::<u64>();
        assert_eq!(verified.charged_bytes(), expected_charge);
        let after = fixture.runtime.snapshot();
        assert!(
            after.reads.delta_since(before.reads).classes
                [MetadataCacheClass::SeriesColdPage.stable_index()]
            .issued
            .calls
                >= 1
        );
        assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
        let after_symbols = symbols.logical_stats();
        assert_eq!(
            after_symbols.returned_values - before_symbols.returned_values,
            2,
            "each canonical name/value must be resolved exactly once"
        );
        assert_eq!(
            after_symbols.returned_utf8_bytes - before_symbols.returned_utf8_bytes,
            6
        );

        let before_warm = fixture.runtime.snapshot();
        let before_warm_symbols = symbols.logical_stats();
        let second = session
            .materialize_verified(&bound, &symbols, routed)
            .expect("repeat materialization from authenticated cache values");
        assert_eq!(second.series_id(), verified.series_id());
        assert_eq!(second.labels(), verified.labels());
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .reads
                .delta_since(before_warm.reads)
                .issued
                .calls,
            0
        );
        let after_warm_symbols = symbols.logical_stats();
        assert_eq!(
            after_warm_symbols.returned_values - before_warm_symbols.returned_values,
            2
        );
        assert_eq!(
            after_warm_symbols.returned_utf8_bytes - before_warm_symbols.returned_utf8_bytes,
            6
        );
    }

    #[test]
    fn selective_materialization_owns_only_requested_labels_but_hashes_every_pair() {
        let mut fixture = fixture_with_runtime(
            "schema7-selective-materialization",
            FixtureOptions {
                multi_label: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let expected = multi_label_fixture_entries(&fixture_symbols());
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open selective materialization session");
        let roots = session
            .load_roots()
            .expect("load selective materialization roots");
        let bound = session
            .bind(roots)
            .expect("bind selective materialization roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan selectively materialized series");
        let routed = planned
            .get(0)
            .expect("planned selectively materialized series");
        let mut context = session
            .materialization_context(&bound, planned.len())
            .expect("create selective materialization context");
        let requested = vec![String::from("s02")];

        let before_symbols = symbols.logical_stats();
        let selected = session
            .materialize_verified_selected_cached(
                &bound,
                &symbols,
                &mut context,
                routed,
                &requested,
                true,
            )
            .expect("selectively materialize and verify canonical labels");
        assert_eq!(selected.series_id(), expected[0].series_id);
        assert_eq!(
            selected.metric_name_dropped_series_id(),
            Some(expected[0].series_id),
            "this fixture has no __name__ pair, so the derived identity is unchanged"
        );
        assert!(!selected.labels_complete());
        assert_eq!(
            selected.labels(),
            &[(String::from("s02"), String::from("s05"))]
        );
        let after_symbols = symbols.logical_stats();
        assert_eq!(
            after_symbols.returned_values - before_symbols.returned_values,
            4,
            "both components of both canonical pairs must still resolve"
        );
        assert_eq!(
            after_symbols.returned_utf8_bytes - before_symbols.returned_utf8_bytes,
            12
        );
        let expected_charge = (selected.labels.capacity() * std::mem::size_of::<(String, String)>())
            as u64
            + selected
                .labels
                .iter()
                .map(|(key, value)| (key.capacity() + value.capacity()) as u64)
                .sum::<u64>();
        assert_eq!(selected.charged_bytes(), expected_charge);

        let selected_without_derived_identity = session
            .materialize_verified_selected_cached(
                &bound,
                &symbols,
                &mut context,
                routed,
                &requested,
                false,
            )
            .expect("selectively materialize without an unused range identity");
        assert!(!selected_without_derived_identity.labels_complete());
        assert_eq!(
            selected_without_derived_identity.metric_name_dropped_series_id(),
            None
        );
        assert_eq!(
            selected_without_derived_identity.labels(),
            selected.labels()
        );

        let full = session
            .materialize_verified(&bound, &symbols, routed)
            .expect("full wrapper must retain established behavior");
        assert!(full.labels_complete());
        assert_eq!(full.metric_name_dropped_series_id(), None);
        assert_eq!(full.labels().len(), 2);
        assert!(selected.charged_bytes() < full.charged_bytes());
    }

    #[test]
    fn selective_materialization_cannot_hide_omitted_identity_mismatch() {
        let mut fixture = fixture_with_runtime(
            "schema7-selective-identity-mismatch",
            FixtureOptions {
                identity_mismatch: true,
                multi_label: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open selective identity session");
        let roots = session.load_roots().expect("load selective identity roots");
        let bound = session.bind(roots).expect("bind selective identity roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan selectively omitted labels");
        let routed = planned.get(0).expect("planned mismatched series");

        let before_symbols = symbols.logical_stats();
        let error = session
            .materialize_verified_selected(&bound, &symbols, routed, &[], false)
            .expect_err("omitting every owned label must not bypass identity verification");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after_symbols = symbols.logical_stats();
        assert_eq!(
            after_symbols.returned_values - before_symbols.returned_values,
            4,
            "omitted labels must still resolve before the mismatch is reported"
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

        session
            .materialize_verified_selected(&bound, &symbols, routed, &[], false)
            .expect_err("sticky omitted-label identity corruption must gate retry");
    }

    #[test]
    fn selective_materialization_cannot_hide_an_omitted_symbol_bounds_error() {
        let mut fixture = fixture_with_runtime(
            "schema7-selective-omitted-symbol-bounds",
            FixtureOptions {
                multi_label: true,
                symbol_count_limit: Some(5),
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open omitted-symbol-bounds session");
        let roots = session
            .load_roots()
            .expect("load omitted-symbol-bounds roots");
        let bound = session
            .bind(roots)
            .expect("bind omitted-symbol-bounds roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan row with omitted out-of-bounds value symbol");
        let requested = vec![String::from("s01")];

        let error = session
            .materialize_verified_selected(
                &bound,
                &symbols,
                planned.get(0).expect("planned malformed row"),
                &requested,
                false,
            )
            .expect_err("an invalid omitted label symbol must fail complete row validation");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    }

    #[test]
    fn lazy_materialization_reuses_decoded_cold_metadata_between_visited_rows() {
        let runtime = runtime_with_budgets(0, 1024 * 1024);
        let mut fixture = fixture_with_runtime(
            "schema7-lazy-materialization-reuse",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open lazy materialization session");
        let roots = session
            .load_roots()
            .expect("load lazy materialization roots");
        let bound = session
            .bind(roots)
            .expect("bind lazy materialization roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0, 1])
            .expect("plan shared-keyset rows");
        let mut context = session
            .materialization_context(&bound, planned.len())
            .expect("create lazy materialization context");

        let first = session
            .materialize_verified_cached(
                &bound,
                &symbols,
                &mut context,
                planned.get(0).expect("first shared-keyset row"),
            )
            .expect("materialize first shared-keyset row");
        assert_eq!(first.series_id(), fixture.entries[0].series_id);
        let before_second = class_reads(&runtime, MetadataCacheClass::SeriesColdPage);
        let second = session
            .materialize_verified_cached(
                &bound,
                &symbols,
                &mut context,
                planned.get(1).expect("second shared-keyset row"),
            )
            .expect("materialize second shared-keyset row");
        assert_eq!(second.series_id(), fixture.entries[1].series_id);
        assert_eq!(
            read_delta(
                class_reads(&runtime, MetadataCacheClass::SeriesColdPage),
                before_second,
            )
            .calls,
            1,
            "only the newly visited row should reload a cold page with zero retention"
        );
    }

    #[test]
    fn materialization_context_budget_refusal_falls_back_to_scalar_decode() {
        let runtime = runtime_with_budgets(1024 * 1024, 128 * 1024);
        let mut fixture = fixture_with_runtime(
            "schema7-lazy-materialization-budget-fallback",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open budget fallback session");
        let roots = session.load_roots().expect("load budget fallback roots");
        let bound = session.bind(roots).expect("bind budget fallback roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan budget fallback row");
        let usage = runtime.snapshot().governor;
        let competing_bytes = usage
            .in_flight_max_bytes
            .checked_sub(usage.in_flight_bytes)
            .and_then(|bytes| bytes.checked_sub(1))
            .expect("leave one in-flight byte for context reservation");
        let blocker = runtime
            .governor()
            .reserve_in_flight_for_usage(competing_bytes, MetadataUsageClass::Scratch)
            .expect("reserve competing context scratch");
        let mut context = session
            .materialization_context(&bound, planned.len())
            .expect("context reservation refusal is an optimization fallback");
        assert!(context.cache.is_none());
        drop(blocker);

        let verified = session
            .materialize_verified_cached(
                &bound,
                &symbols,
                &mut context,
                planned.get(0).expect("budget fallback row"),
            )
            .expect("scalar fallback must materialize the row");
        assert_eq!(verified.series_id(), fixture.entries[0].series_id);
    }

    #[test]
    fn materialization_context_propagates_planning_overflow() {
        let runtime = runtime();
        let mut fixture = fixture_with_runtime(
            "schema7-materialization-context-overflow",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let reader = open_fixture(&mut fixture);
        let session = reader
            .query_session()
            .expect("open planning-overflow session");
        let roots = session.load_roots().expect("load planning-overflow roots");
        let bound = session.bind(roots).expect("bind planning-overflow roots");
        let before = runtime.snapshot();

        let error = session
            .materialization_context(&bound, usize::MAX)
            .err()
            .expect("planning overflow must not be swallowed as an optional cache miss");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Planning(ref error)
                if error.kind() == io::ErrorKind::InvalidInput
        ));
        let after = runtime.snapshot();
        assert_eq!(after.reads, before.reads);
        assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
        assert_eq!(
            after.governor.in_flight_bytes,
            before.governor.in_flight_bytes
        );
    }

    #[test]
    fn materialize_verified_rejects_identity_mismatch_and_makes_it_sticky() {
        let mut fixture = fixture_with_runtime(
            "schema7-identity-mismatch",
            FixtureOptions {
                identity_mismatch: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader.query_session().expect("open identity session");
        let roots = session.load_roots().expect("load identity roots");
        let bound = session.bind(roots).expect("bind identity roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan mismatched identity series");
        let routed = planned.get(0).expect("planned mismatched series");

        let error = session
            .materialize_verified(&bound, &symbols, routed)
            .expect_err("mismatched stored identity must fail");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = fixture.runtime.snapshot();
        assert_eq!(after.cache.sticky_artifacts, 1);

        session
            .materialize_verified(&bound, &symbols, routed)
            .expect_err("sticky identity corruption must gate retry");
        assert_eq!(fixture.runtime.snapshot().reads, after.reads);
    }

    #[test]
    fn materialize_verified_rejects_warm_row_substitution_without_more_io() {
        let mut fixture = fixture_with_runtime(
            "schema7-row-substitution",
            FixtureOptions {
                substitute_row: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader.query_session().expect("open substitution session");
        let roots = session.load_roots().expect("load substitution roots");
        let bound = session.bind(roots).expect("bind substitution roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0, 1])
            .expect("plan substituted and canonical rows");
        let substituted = planned.get(0).expect("substituted row plan");
        let canonical = planned.get(1).expect("canonical row plan");

        let warmed = session
            .materialize_verified(&bound, &symbols, canonical)
            .expect("warm the shared row, dictionary, and symbol pages");
        assert_eq!(warmed.series_id(), fixture.entries[1].series_id);
        let before = fixture.runtime.snapshot();
        let error = session
            .materialize_verified(&bound, &symbols, substituted)
            .expect_err("valid row substitution must fail identity verification");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = fixture.runtime.snapshot();
        assert_eq!(after.reads, before.reads);
        assert_eq!(
            after.cache.sticky_artifacts,
            before.cache.sticky_artifacts + 1
        );

        session
            .materialize_verified(&bound, &symbols, substituted)
            .expect_err("sticky substitution must gate retry");
        assert_eq!(fixture.runtime.snapshot().reads, after.reads);
    }

    #[test]
    fn cross_page_keyset_materializes_only_after_both_pages_authenticate() {
        let mut fixture = fixture_with_runtime(
            "schema7-cross-page-keyset",
            FixtureOptions {
                cross_page_keyset: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        assert!(
            fixture.cold_bytes.len() > super::super::SERIES_COLD_PAGE_LEN_V1 as usize,
            "fixture must span at least two physical cold pages"
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader.query_session().expect("open cross-page session");
        let roots = session.load_roots().expect("load cross-page roots");
        let bound = session.bind(roots).expect("bind cross-page roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[1])
            .expect("plan cross-page keyset series");
        let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage);
        let verified = session
            .materialize_verified(
                &bound,
                &symbols,
                planned.get(0).expect("cross-page series plan"),
            )
            .expect("materialize cross-page keyset");
        assert_eq!(verified.series_id(), fixture.entries[1].series_id);
        assert_eq!(
            verified.labels(),
            &[(String::from("s4087"), String::from("s4088"))]
        );
        let delta = read_delta(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage),
            before,
        );
        assert!(delta.calls >= 2, "both intersected pages must be issued");
    }

    #[test]
    fn cross_page_keyset_corruption_is_sticky_before_any_row_is_returned() {
        let mut fixture = fixture_with_runtime(
            "schema7-cross-page-corruption",
            FixtureOptions {
                cross_page_keyset: true,
                corrupt_second_cold_page: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open corrupt cross-page session");
        let roots = session.load_roots().expect("load corrupt cross-page roots");
        let bound = session.bind(roots).expect("bind corrupt cross-page roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[1])
            .expect("plan corrupt cross-page series");
        let routed = planned.get(0).expect("corrupt cross-page series plan");

        let error = session
            .materialize_verified(&bound, &symbols, routed)
            .expect_err("second intersected page CRC must reject the complete keyset");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = fixture.runtime.snapshot();
        assert_eq!(after.cache.sticky_artifacts, 1);
        session
            .materialize_verified(&bound, &symbols, routed)
            .expect_err("sticky cross-page corruption must gate retry");
        assert_eq!(fixture.runtime.snapshot().reads, after.reads);
    }

    #[test]
    fn selective_materialization_integrity_checks_an_omitted_symbol_page_crc() {
        let mut fixture = fixture_with_runtime(
            "schema7-selective-omitted-symbol-page-corruption",
            FixtureOptions {
                cross_page_keyset: true,
                corrupt_last_symbol_page: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open omitted symbol-page corruption session");
        let roots = session
            .load_roots()
            .expect("load omitted symbol-page corruption roots");
        let bound = session
            .bind(roots)
            .expect("bind omitted symbol-page corruption roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[1])
            .expect("plan row whose omitted labels use the corrupt symbol page");
        let routed = planned.get(0).expect("corrupt symbol-page series plan");

        let error = session
            .materialize_verified_selected(&bound, &symbols, routed, &[], false)
            .expect_err("an omitted label's symbol-page CRC must remain authoritative");
        assert!(
            matches!(
                error,
                Schema7MetadataReaderError::Symbols(GovernedSymbolReaderError::Cache(
                    MetadataCacheError::Structural(_)
                ))
            ),
            "unexpected omitted symbol-page corruption error: {error:?}"
        );
        let after = fixture.runtime.snapshot();
        assert_eq!(after.cache.sticky_artifacts, 1);
        session
            .materialize_verified_selected(&bound, &symbols, routed, &[], false)
            .expect_err("sticky omitted symbol-page corruption must gate retry");
        assert_eq!(fixture.runtime.snapshot().reads, after.reads);
    }

    #[test]
    fn materialization_budget_refusal_precedes_cold_io_and_is_retryable() {
        let runtime = runtime_with_budgets(1024 * 1024, 64 * 1024);
        let mut fixture = fixture_with_runtime(
            "schema7-materialize-budget",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader.query_session().expect("open budget session");
        let roots = session.load_roots().expect("load budget roots");
        let bound = session.bind(roots).expect("bind budget roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan budget series");
        let routed = planned.get(0).expect("budget series plan");
        let usage = runtime.snapshot().governor;
        let competing_bytes = usage
            .in_flight_max_bytes
            .checked_sub(usage.in_flight_bytes)
            .and_then(|bytes| bytes.checked_sub(1))
            .expect("leave one in-flight byte available");
        let blocker = runtime
            .governor()
            .reserve_in_flight_for_usage(competing_bytes, MetadataUsageClass::Scratch)
            .expect("reserve competing materialization scratch");
        let before = runtime.snapshot();
        let error = session
            .materialize_verified(&bound, &symbols, routed)
            .expect_err("cold-range scratch must be refused before I/O");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Budget(_))
        ));
        let after = runtime.snapshot();
        assert_eq!(after.reads, before.reads);
        assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
        drop(blocker);

        let verified = session
            .materialize_verified(&bound, &symbols, routed)
            .expect("budget refusal must be retryable");
        assert_eq!(verified.series_id(), fixture.entries[0].series_id);
    }

    #[test]
    fn foreign_symbol_generation_is_rejected_before_cold_io_or_poisoning() {
        let runtime = runtime();
        let mut first = fixture_with_runtime(
            "schema7-materialize-generation-a",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let second = fixture_with_runtime(
            "schema7-materialize-generation-b",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let reader = open_fixture(&mut first);
        let foreign_symbols = open_symbol_session(&second);
        let session = reader.query_session().expect("open generation session");
        let roots = session.load_roots().expect("load generation roots");
        let bound = session.bind(roots).expect("bind generation roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan generation series");
        let before = runtime.snapshot();

        let error = session
            .materialize_verified(
                &bound,
                &foreign_symbols,
                planned.get(0).expect("generation series plan"),
            )
            .expect_err("foreign symbol session must not materialize labels");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Symbols(
                GovernedSymbolReaderError::ForeignSegmentGeneration
            )
        ));
        let after = runtime.snapshot();
        assert_eq!(after.reads, before.reads);
        assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
    }

    #[test]
    fn out_of_range_key_symbol_is_sticky_series_corruption_not_symbol_corruption() {
        let mut fixture = fixture_with_runtime(
            "schema7-key-symbol-bound",
            FixtureOptions {
                symbol_count_limit: Some(1),
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader.query_session().expect("open key-bound session");
        let roots = session.load_roots().expect("load key-bound roots");
        let bound = session.bind(roots).expect("bind key-bound roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan key-bound series");
        let routed = planned.get(0).expect("key-bound series plan");

        let error = session
            .materialize_verified(&bound, &symbols, routed)
            .expect_err("out-of-range key symbol must fail as series corruption");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = fixture.runtime.snapshot();
        assert_eq!(after.cache.sticky_artifacts, 1);

        let owner = fixture
            .registered
            .as_ref()
            .expect("fixture owner available");
        let mut byte = [0u8; 1];
        owner
            .reader(SegmentFile::Series)
            .expect("series attribution reader")
            .read_exact_at_for_class(0, &mut byte, MetadataCacheClass::SeriesRoot)
            .expect_err("series artifact must carry the sticky key-symbol error");
        symbols
            .visit_required_resolved(0, |_| Ok(()))
            .expect("symbols artifact must remain healthy");
        let before_retry = fixture.runtime.snapshot();
        session
            .materialize_verified(&bound, &symbols, routed)
            .expect_err("sticky series corruption must gate retry");
        assert_eq!(fixture.runtime.snapshot().reads, before_retry.reads);
    }

    #[test]
    fn zero_retention_materialization_resolves_each_symbol_with_one_page_read() {
        let runtime = runtime_with_budgets(0, 1024 * 1024);
        let mut fixture = fixture_with_runtime(
            "schema7-zero-retention-materialize",
            FixtureOptions::default(),
            runtime.clone(),
        );
        let reader = open_fixture(&mut fixture);
        let symbols = open_symbol_session(&fixture);
        let session = reader
            .query_session()
            .expect("open zero-retention materialization session");
        let roots = session
            .load_roots()
            .expect("load zero-retention materialization roots");
        let bound = session
            .bind(roots)
            .expect("bind zero-retention materialization roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0])
            .expect("plan zero-retention materialization series");
        let routed = planned
            .get(0)
            .expect("zero-retention materialization series plan");
        let before_reads = class_reads(&runtime, MetadataCacheClass::SymbolPage);
        let before_symbols = symbols.logical_stats();

        let verified = session
            .materialize_verified(&bound, &symbols, routed)
            .expect("materialize with zero retention");
        assert_eq!(verified.series_id(), fixture.entries[0].series_id);
        let symbol_reads = read_delta(
            class_reads(&runtime, MetadataCacheClass::SymbolPage),
            before_reads,
        );
        assert_eq!(symbol_reads.calls, 2);
        assert!(symbol_reads.bytes > 0);
        let after_symbols = symbols.logical_stats();
        assert_eq!(
            after_symbols.returned_values - before_symbols.returned_values,
            2
        );
        assert_eq!(
            after_symbols.returned_utf8_bytes - before_symbols.returned_utf8_bytes,
            6
        );
    }

    #[test]
    fn duplicate_hot_blob_range_is_sticky_on_a_cache_hit_without_more_io() {
        let mut fixture = fixture_with_runtime(
            "schema7-duplicate-blob-range",
            FixtureOptions {
                duplicate_blob_locator: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let session = reader
            .query_session()
            .expect("open duplicate-range session");
        let roots = session.load_roots().expect("load duplicate-range roots");
        let bound = session.bind(roots).expect("bind duplicate-range roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[0, 1])
            .expect("plan both aliased hot records");
        let matching = planned.get(1).expect("blob identity matching series");
        session
            .load_overflow_blob(&bound, matching)
            .expect("admit intrinsically valid physical blob");

        let before_alias = fixture.runtime.snapshot();
        let alias = planned.get(0).expect("aliased hot record");
        let error = session
            .load_overflow_blob(&bound, alias)
            .expect_err("incompatible hot-record alias must fail on cache hit");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after_alias = fixture.runtime.snapshot();
        assert_eq!(after_alias.reads, before_alias.reads);
        assert_eq!(after_alias.cache.hits - before_alias.cache.hits, 1);
        assert_eq!(after_alias.cache.sticky_artifacts, 2);

        let before_retry = fixture.runtime.snapshot();
        session
            .load_overflow_blob(&bound, alias)
            .expect_err("cross-artifact corruption must gate retry");
        let after_retry = fixture.runtime.snapshot();
        assert_eq!(after_retry.reads, before_retry.reads);
        assert_eq!(after_retry.cache.sticky_artifacts, 2);
    }

    #[test]
    fn cold_page_corruption_is_sticky_without_admission_or_resource_leaks() {
        let mut fixture = fixture_with_runtime(
            "schema7-bad-cold-page",
            FixtureOptions {
                corrupt_cold: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let reader = open_fixture(&mut fixture);
        let session = reader.query_session().expect("open cold-page session");
        let roots = session.load_roots().expect("load cold-page roots");
        let bound = session.bind(roots).expect("bind cold-page roots");
        let before = fixture.runtime.snapshot();

        let error = session
            .load_cold_page(&bound, 0)
            .expect_err("cold-page CRC corruption must fail");
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = fixture.runtime.snapshot();
        assert_eq!(
            after.reads.delta_since(before.reads).classes
                [MetadataCacheClass::SeriesColdPage.stable_index()]
            .issued,
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 1,
                bytes: fixture.cold_bytes.len() as u64,
            }
        );
        assert_eq!(after.cache.successful_loads, before.cache.successful_loads);
        assert_eq!(after.cache.active_loads, 0);
        assert_eq!(after.cache.sticky_artifacts, 1);
        assert_eq!(
            after
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            before
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes
        );
        assert_eq!(after.files.open_files, 0);

        let before_retry = fixture.runtime.snapshot();
        session
            .load_cold_page(&bound, 0)
            .expect_err("sticky cold-page corruption gates retry");
        assert_eq!(fixture.runtime.snapshot().reads, before_retry.reads);
    }

    #[test]
    fn bootstrap_decode_corruption_is_sticky_before_root_admission() {
        let fixture = fixture("schema7-bad-header", true, false, false);
        let owner = fixture.registered.as_ref().expect("fixture owner");
        let error = match Schema7MetadataReader::open(owner, fixture.context) {
            Ok(_) => panic!("corrupt fixed header must fail open"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = fixture.runtime.snapshot();
        assert_eq!(after.reads.issued.calls, 1);
        assert_eq!(after.reads.issued.bytes, SERIES_HEADER_LEN_V3 as u64);
        assert_eq!(after.cache.successful_loads, 0);
        assert_eq!(after.cache.resident_entries, 0);
        assert_eq!(after.cache.sticky_artifacts, 1);

        let series = owner.reader(SegmentFile::Series).expect("series reader");
        let before_retry = fixture.runtime.snapshot();
        let mut byte = [0u8; 1];
        series
            .read_exact_at_for_class(0, &mut byte, MetadataCacheClass::SeriesRoot)
            .expect_err("sticky header corruption gates retry");
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .reads
                .delta_since(before_retry.reads)
                .issued
                .calls,
            0
        );
    }

    #[test]
    fn root_suffix_and_overflow_root_corruption_are_sticky_with_exact_reads() {
        let series_fixture = fixture_with_runtime(
            "schema7-bad-series-root-suffix",
            FixtureOptions {
                corrupt_series_root_suffix: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let series_owner = series_fixture
            .registered
            .as_ref()
            .expect("series-root fixture owner");
        let before_series = series_fixture.runtime.snapshot();
        let error = match Schema7MetadataReader::open(series_owner, series_fixture.context) {
            Ok(_) => panic!("corrupt series-root suffix must fail open"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after_series = series_fixture.runtime.snapshot();
        let series_delta = after_series.reads.delta_since(before_series.reads);
        assert_eq!(
            series_delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 2,
                bytes: series_fixture.root_len,
            }
        );
        assert_eq!(after_series.cache.successful_loads, 0);
        assert_eq!(after_series.cache.active_loads, 0);
        assert_eq!(after_series.cache.sticky_artifacts, 1);
        assert_eq!(after_series.files.open_files, 0);
        assert_eq!(
            after_series
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            0
        );
        let mut retry = [0u8; 1];
        series_owner
            .reader(SegmentFile::Series)
            .expect("series-root retry reader")
            .read_exact_at_for_class(0, &mut retry, MetadataCacheClass::SeriesRoot)
            .expect_err("sticky series-root corruption gates retry");
        assert_eq!(series_fixture.runtime.snapshot().reads, after_series.reads);

        let overflow_fixture = fixture_with_runtime(
            "schema7-bad-overflow-root",
            FixtureOptions {
                corrupt_overflow_root: true,
                ..FixtureOptions::default()
            },
            runtime(),
        );
        let overflow_owner = overflow_fixture
            .registered
            .as_ref()
            .expect("overflow-root fixture owner");
        let before_overflow = overflow_fixture.runtime.snapshot();
        let error = match Schema7MetadataReader::open(overflow_owner, overflow_fixture.context) {
            Ok(_) => panic!("corrupt overflow root must fail open"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after_overflow = overflow_fixture.runtime.snapshot();
        let overflow_delta = after_overflow.reads.delta_since(before_overflow.reads);
        assert_eq!(
            overflow_delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 2,
                bytes: overflow_fixture.root_len,
            }
        );
        assert_eq!(
            overflow_delta.classes[MetadataCacheClass::OverflowRoot.stable_index()].issued,
            crate::storage::metadata_runtime::MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
            }
        );
        assert_eq!(after_overflow.cache.successful_loads, 1);
        assert_eq!(after_overflow.cache.active_loads, 0);
        assert_eq!(after_overflow.cache.sticky_artifacts, 1);
        assert_eq!(after_overflow.files.open_files, 0);
        assert_eq!(
            after_overflow
                .governor
                .usage(MetadataUsageClass::Scratch)
                .in_flight_bytes,
            0
        );
        overflow_owner
            .reader(SegmentFile::ChunkIndex)
            .expect("overflow-root retry reader")
            .read_exact_at_for_class(0, &mut retry, MetadataCacheClass::OverflowRoot)
            .expect_err("sticky overflow-root corruption gates retry");
        assert_eq!(
            overflow_fixture.runtime.snapshot().reads,
            after_overflow.reads
        );
    }

    #[test]
    fn touched_page_and_blob_corruption_are_sticky_and_never_admitted() {
        let mut bad_page = fixture("schema7-bad-page", false, true, false);
        let page_reader = open_fixture(&mut bad_page);
        let session = page_reader.query_session().expect("page query session");
        let roots = session.load_roots().expect("load page roots");
        let bound = session.bind(roots).expect("bind page roots");
        let before = bad_page.runtime.snapshot();
        let first = session
            .load_hot_page(&bound, 0)
            .expect_err("hot-page CRC corruption must fail");
        assert!(matches!(
            first,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = bad_page.runtime.snapshot();
        assert_eq!(after.cache.resident_entries, before.cache.resident_entries);
        assert_eq!(after.cache.successful_loads, before.cache.successful_loads);
        assert_eq!(after.cache.sticky_artifacts, 1);
        let before_retry = bad_page.runtime.snapshot();
        session
            .load_hot_page(&bound, 0)
            .expect_err("sticky hot-page corruption gates retry");
        assert_eq!(
            bad_page
                .runtime
                .snapshot()
                .reads
                .delta_since(before_retry.reads)
                .issued
                .calls,
            0
        );

        let mut bad_blob = fixture("schema7-bad-blob", false, false, true);
        let blob_reader = open_fixture(&mut bad_blob);
        let session = blob_reader.query_session().expect("blob query session");
        let roots = session.load_roots().expect("load blob roots");
        let bound = session.bind(roots).expect("bind blob roots");
        let planned = session
            .plan_hot_page(&bound, 0, &[1])
            .expect("plan overflow series");
        let overflow = planned.get(0).expect("overflow planned series");
        let before = bad_blob.runtime.snapshot();
        let first = session
            .load_overflow_blob(&bound, overflow)
            .expect_err("overflow-blob CRC corruption must fail");
        assert!(matches!(
            first,
            Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after = bad_blob.runtime.snapshot();
        assert_eq!(after.cache.resident_entries, before.cache.resident_entries);
        assert_eq!(after.cache.successful_loads, before.cache.successful_loads);
        assert_eq!(after.cache.sticky_artifacts, 1);
        let before_retry = bad_blob.runtime.snapshot();
        session
            .load_overflow_blob(&bound, overflow)
            .expect_err("sticky overflow corruption gates retry");
        assert_eq!(
            bad_blob
                .runtime
                .snapshot()
                .reads
                .delta_since(before_retry.reads)
                .issued
                .calls,
            0
        );
    }

    #[test]
    fn session_and_external_pin_retire_without_an_ownership_cycle() {
        let mut fixture = fixture("schema7-lifecycle", false, false, false);
        let owner = fixture.registered.take().expect("fixture owner available");
        let reader = Schema7MetadataReader::open(&owner, fixture.context)
            .expect("open lifecycle schema-7 reader");
        let session = reader.query_session().expect("open lifecycle session");
        let roots = session.load_roots().expect("load lifecycle roots");
        let bound = session.bind(roots).expect("bind lifecycle roots");
        let page = session
            .load_hot_page(&bound, 0)
            .expect("pin lifecycle hot page");

        drop(bound);
        drop(reader);
        drop(owner);
        assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 7);
        drop(session);
        assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 1);
        assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
        drop(page);
        let final_snapshot = fixture.runtime.snapshot();
        assert_eq!(final_snapshot.cache.registered_artifacts, 0);
        assert_eq!(final_snapshot.cache.resident_entries, 0);
        assert_eq!(final_snapshot.cache.live_allocations, 0);
        assert_eq!(final_snapshot.files.open_files, 0);
        assert_eq!(final_snapshot.governor.in_flight_bytes, 0);
        assert_eq!(final_snapshot.governor.retained_bytes, 0);
    }
}
