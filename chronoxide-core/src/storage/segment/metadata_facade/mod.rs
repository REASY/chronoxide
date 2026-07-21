//! Dormant schema-neutral, generation-bound metadata facade.
//!
//! The facade authenticates selector metadata, verified series identities,
//! canonical labels, and exact chunk locators without exposing raw v7/v8
//! directories or layout-specific routing offsets. Footer selection and the
//! production query path remain deliberately outside this isolated boundary.

use std::io;

use thiserror::Error;

use crate::storage::chunk::{
    GovernedSchema6ChunkIndexReader, GovernedSchema6ChunkIndexRoot,
    GovernedSchema6ChunkIndexSession, Schema6ChunkIndexReaderError,
};
use crate::storage::index::{
    ExactPostingsMetadata, GovernedSchema6BoundIndexRoot, GovernedSchema6ExactPostings,
    GovernedSchema6ExactPostingsSelection, GovernedSchema6IndexReader, GovernedSchema6IndexSession,
    GovernedSchema7BoundIndexRoot, GovernedSchema7ExactPostings,
    GovernedSchema7ExactPostingsSelection, GovernedSchema7IndexReader, GovernedSchema7IndexSession,
    Schema6IndexReaderError, Schema7IndexReaderError,
};
use crate::storage::metadata_governor::MetadataBudgetError;
use crate::storage::metadata_runtime::{
    RegisteredSegment, SegmentGenerationProvenance, SegmentReadGuard, StoreMetadataRuntimeError,
};
use crate::storage::series::v2_runtime::{
    GovernedSchema6SeriesReader, GovernedSchema6SeriesRoot, GovernedSchema6SeriesSession,
    Schema6SeriesReaderError,
};
use crate::storage::series::v3::{
    BoundSchema7Roots, CanonicalLabelMaterializationProfile, Schema7MetadataReader,
    Schema7MetadataReaderError, Schema7MetadataSession, Schema7RootBindingContext,
};
use crate::storage::symbols::{
    GovernedSymbolReader, GovernedSymbolReaderError, GovernedSymbolSession,
};

mod ref_set;
mod routing;
mod selector;
#[allow(unused_imports)] // Public within the crate once the dormant facade is integrated.
pub(crate) use ref_set::GovernedSeriesRefSet;
#[allow(unused_imports)] // Public within the crate once the dormant facade is integrated.
pub(crate) use routing::{
    SegmentChunkAuthentication, SegmentChunkLocator, SegmentChunkLocatorBatch,
    SegmentMetadataVisitControl, SegmentMetadataVisitError, SegmentMetadataVisitOutcome,
    SegmentVerifiedSeries,
};

/// Footer-independent construction facts for the dormant facade.
///
/// The final footer integration owns creation of this value. Bare counts and
/// lengths authorize only opening the corresponding governed roots; all
/// cross-file query authority still comes from capabilities minted by those
/// roots in [`SegmentMetadataSession::bind_roots`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentMetadataLayout {
    Schema6 { series_count: u32 },
    Schema7(Schema7MetadataOpenContext),
    Schema8(Schema7MetadataOpenContext),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7MetadataOpenContext {
    pub(crate) series_file_len: u64,
    pub(crate) chunk_index_file_len: u64,
    pub(crate) segment_start_ms: u64,
    pub(crate) segment_end_ms: u64,
    pub(crate) series_count: u32,
}

impl From<Schema7MetadataOpenContext> for Schema7RootBindingContext {
    fn from(value: Schema7MetadataOpenContext) -> Self {
        Self {
            series_file_len: value.series_file_len,
            chunk_index_file_len: value.chunk_index_file_len,
            segment_start_ms: value.segment_start_ms,
            segment_end_ms: value.segment_end_ms,
            series_count: value.series_count,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum SegmentMetadataFacadeError {
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Symbols(#[from] GovernedSymbolReaderError),
    #[error(transparent)]
    Schema6Series(#[from] Schema6SeriesReaderError),
    #[error(transparent)]
    Schema6ChunkIndex(#[from] Schema6ChunkIndexReaderError),
    #[error(transparent)]
    Schema6Index(#[from] Schema6IndexReaderError),
    #[error(transparent)]
    Schema7Metadata(#[from] Schema7MetadataReaderError),
    #[error(transparent)]
    Schema7Index(#[from] Schema7IndexReaderError),
    #[error(transparent)]
    Budget(#[from] MetadataBudgetError),
    #[error("segment metadata handle belongs to another segment generation")]
    ForeignSegmentGeneration,
    #[error("segment metadata handle belongs to another storage layout backend")]
    ForeignLayoutBackend,
    #[error("series ref {series_ref} exceeds the bound series count {series_count}")]
    InvalidSeriesRef { series_ref: u32, series_count: u32 },
    #[error("query time range is reversed: start={start_ms} end={end_ms}")]
    ReversedTimeRange { start_ms: u64, end_ms: u64 },
    #[error("governed series-ref set allocation failed: {0}")]
    RefSetAllocation(#[source] io::Error),
    #[error("governed series-ref set size overflows")]
    RefSetSizeOverflow,
}

/// Long-lived generation owner. Every backend reader retains no root pin,
/// descriptor, or decoded per-series map.
pub(crate) struct SegmentMetadataReader {
    registered: RegisteredSegment,
    symbols: GovernedSymbolReader,
    backend: SegmentMetadataReaderBackend,
}

enum SegmentMetadataReaderBackend {
    Schema6 {
        series: GovernedSchema6SeriesReader,
        chunk_index: GovernedSchema6ChunkIndexReader,
        index: GovernedSchema6IndexReader,
    },
    Schema7 {
        series: Schema7MetadataReader,
        index: GovernedSchema7IndexReader,
    },
}

/// Query-local generation and resource boundary.
pub(crate) struct SegmentMetadataSession {
    guard: SegmentReadGuard,
    symbols: GovernedSymbolSession,
    backend: SegmentMetadataSessionBackend,
}

enum SegmentMetadataSessionBackend {
    Schema6 {
        series: GovernedSchema6SeriesSession,
        chunk_index: GovernedSchema6ChunkIndexSession,
        index: GovernedSchema6IndexSession,
    },
    Schema7 {
        series: Schema7MetadataSession,
        index: GovernedSchema7IndexSession,
    },
}

/// Opaque, query-local binding of every fixed root required by one layout.
pub(crate) struct SegmentMetadataRoot {
    provenance: SegmentGenerationProvenance,
    series_count: u32,
    backend: SegmentMetadataRootBackend,
}

enum SegmentMetadataRootBackend {
    Schema6 {
        series: GovernedSchema6SeriesRoot,
        chunk_index: GovernedSchema6ChunkIndexRoot,
        index: GovernedSchema6BoundIndexRoot,
    },
    Schema7 {
        series: BoundSchema7Roots,
        index: GovernedSchema7BoundIndexRoot,
    },
}

/// Opaque exact-postings selection. No physical locator crosses the facade.
pub(crate) struct SegmentExactPostingsSelection {
    provenance: SegmentGenerationProvenance,
    backend: SegmentExactPostingsSelectionBackend,
}

enum SegmentExactPostingsSelectionBackend {
    Schema6(GovernedSchema6ExactPostingsSelection),
    Schema7(GovernedSchema7ExactPostingsSelection),
}

/// Opaque pinned exact-postings payload. Refs are available only to bounded
/// visitors or a newly charged facade set.
pub(crate) struct SegmentExactPostings {
    provenance: SegmentGenerationProvenance,
    backend: SegmentExactPostingsBackend,
}

enum SegmentExactPostingsBackend {
    Schema6(GovernedSchema6ExactPostings),
    Schema7(GovernedSchema7ExactPostings),
}

impl SegmentMetadataReader {
    pub(crate) fn open(
        registered: &RegisteredSegment,
        layout: SegmentMetadataLayout,
    ) -> Result<Self, SegmentMetadataFacadeError> {
        let symbols = GovernedSymbolReader::open(registered)?;
        let backend = match layout {
            SegmentMetadataLayout::Schema6 { series_count } => {
                SegmentMetadataReaderBackend::Schema6 {
                    series: GovernedSchema6SeriesReader::open(registered, series_count)?,
                    chunk_index: GovernedSchema6ChunkIndexReader::open(registered, series_count)?,
                    index: GovernedSchema6IndexReader::open(registered)?,
                }
            }
            SegmentMetadataLayout::Schema7(context) => SegmentMetadataReaderBackend::Schema7 {
                series: Schema7MetadataReader::open(registered, context.into())?,
                index: GovernedSchema7IndexReader::open(registered)?,
            },
            SegmentMetadataLayout::Schema8(context) => SegmentMetadataReaderBackend::Schema7 {
                series: Schema7MetadataReader::open(registered, context.into())?,
                index: GovernedSchema7IndexReader::open_v9(registered)?,
            },
        };
        Ok(Self {
            registered: registered.clone(),
            symbols,
            backend,
        })
    }

    pub(crate) fn segment_identity(&self) -> &str {
        self.registered.segment_identity()
    }

    pub(crate) fn query_session(
        &self,
    ) -> Result<SegmentMetadataSession, SegmentMetadataFacadeError> {
        let guard = self.registered.read_guard()?;
        let symbols = self.symbols.query_session()?;
        let backend = match &self.backend {
            SegmentMetadataReaderBackend::Schema6 {
                series,
                chunk_index,
                index,
            } => SegmentMetadataSessionBackend::Schema6 {
                series: series.query_session()?,
                chunk_index: chunk_index.query_session()?,
                index: index.query_session()?,
            },
            SegmentMetadataReaderBackend::Schema7 { series, index } => {
                SegmentMetadataSessionBackend::Schema7 {
                    series: series.query_session()?,
                    index: index.query_session()?,
                }
            }
        };
        Ok(SegmentMetadataSession {
            guard,
            symbols,
            backend,
        })
    }

    pub(crate) fn validate_all_symbols(&self) -> Result<(), SegmentMetadataFacadeError> {
        self.symbols.query_session()?.validate_all_pages()?;
        Ok(())
    }
}

impl SegmentMetadataSession {
    /// Loads and binds every fixed root required by the selected backend.
    /// Bare construction counts are consumed only by the underlying readers;
    /// the index receives unforgeable same-generation capabilities.
    pub(crate) fn bind_roots(&self) -> Result<SegmentMetadataRoot, SegmentMetadataFacadeError> {
        let (series_count, backend) = match &self.backend {
            SegmentMetadataSessionBackend::Schema6 {
                series,
                chunk_index,
                index,
            } => {
                let series_root = series.load_root()?;
                let chunk_index_root = chunk_index.load_root()?;
                chunk_index.bind_series_count(&chunk_index_root, series_root.num_series())?;
                let series_count = series_root.num_series();
                let series_binding = series.series_count_binding(&series_root)?;
                let symbol_binding = self.symbols.symbol_count_binding();
                let index_root = index.load_root()?;
                let index = index.bind_segment_roots(index_root, series_binding, symbol_binding)?;
                (
                    series_count,
                    SegmentMetadataRootBackend::Schema6 {
                        series: series_root,
                        chunk_index: chunk_index_root,
                        index,
                    },
                )
            }
            SegmentMetadataSessionBackend::Schema7 { series, index } => {
                let roots = series.load_roots()?;
                let roots = series.bind(roots)?;
                let series_binding = series.series_count_binding(&roots)?;
                let series_count = series_binding.num_series();
                let symbol_binding = self.symbols.symbol_count_binding();
                let index = index.bind_segment_roots(series_binding, symbol_binding)?;
                (
                    series_count,
                    SegmentMetadataRootBackend::Schema7 {
                        series: roots,
                        index,
                    },
                )
            }
        };
        Ok(SegmentMetadataRoot {
            provenance: self.guard.provenance(),
            series_count,
            backend,
        })
    }

    pub(crate) fn lookup_symbol(
        &self,
        root: &SegmentMetadataRoot,
        value: &str,
    ) -> Result<Option<u32>, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        Ok(self.symbols.lookup(value)?)
    }

    /// Visits one resolved symbol while its governed page remains pinned.
    /// `Ok(false)` means the caller-provided ID is outside the dictionary.
    pub(crate) fn visit_resolved_symbol(
        &self,
        root: &SegmentMetadataRoot,
        symbol_id: u32,
        mut visitor: impl FnMut(u32, &str),
    ) -> Result<bool, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        Ok(self.symbols.visit_resolved_many(&[symbol_id], |_, value| {
            visitor(symbol_id, value);
            Ok(())
        })?)
    }

    pub(crate) fn select_exact_postings(
        &self,
        root: &SegmentMetadataRoot,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> Result<Option<SegmentExactPostingsSelection>, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        let backend = match (&self.backend, &root.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { index, .. },
                SegmentMetadataRootBackend::Schema6 { index: root, .. },
            ) => index
                .select_exact_postings(root, label_name_sym, label_value_sym)?
                .map(SegmentExactPostingsSelectionBackend::Schema6),
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
            ) => index
                .select_exact_postings(root, label_name_sym, label_value_sym)?
                .map(SegmentExactPostingsSelectionBackend::Schema7),
            _ => return Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        };
        Ok(backend.map(|backend| SegmentExactPostingsSelection {
            provenance: self.guard.provenance(),
            backend,
        }))
    }

    pub(crate) fn exact_postings_encoded_len(
        &self,
        root: &SegmentMetadataRoot,
        selection: &SegmentExactPostingsSelection,
    ) -> Result<u64, SegmentMetadataFacadeError> {
        Ok(self.selection_metadata(root, selection)?.byte_len)
    }

    /// Returns a format-neutral key proportional to the decoded postings
    /// cardinality. Schema 6 stores raw postings, so its authenticated encoded
    /// length is already `4 + 4 * ref_count`. Schema 7/8 use the protected
    /// exact-page count and map it onto that same scale. Query planning must
    /// not use schema-8's compressed length as a selectivity estimate.
    pub(crate) fn exact_postings_cardinality_key(
        &self,
        root: &SegmentMetadataRoot,
        selection: &SegmentExactPostingsSelection,
    ) -> Result<u64, SegmentMetadataFacadeError> {
        self.ensure_selection(root, selection)?;
        match (&self.backend, &root.backend, &selection.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { index, .. },
                SegmentMetadataRootBackend::Schema6 { index: root, .. },
                SegmentExactPostingsSelectionBackend::Schema6(selection),
            ) => Ok(index.selection_metadata(root, selection)?.byte_len),
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
                SegmentExactPostingsSelectionBackend::Schema7(selection),
            ) => Ok(4 + u64::from(index.selection_ref_count(root, selection)?) * 4),
            _ => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        }
    }

    /// Schema-6 v7 range summaries are advisory and therefore always retain
    /// the candidate. Schema 7/8 may prune only from the CRC-authenticated v8/v9
    /// exact-page record carried by the opaque selection.
    pub(crate) fn exact_postings_overlaps(
        &self,
        root: &SegmentMetadataRoot,
        selection: &SegmentExactPostingsSelection,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<bool, SegmentMetadataFacadeError> {
        validate_time_range(start_ms, end_ms)?;
        self.ensure_selection(root, selection)?;
        match (&self.backend, &root.backend, &selection.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { .. },
                SegmentMetadataRootBackend::Schema6 { .. },
                SegmentExactPostingsSelectionBackend::Schema6(_),
            ) => Ok(true),
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
                SegmentExactPostingsSelectionBackend::Schema7(selection),
            ) => Ok(index
                .selection_metadata(root, selection)?
                .time_range
                .overlaps(start_ms, end_ms)),
            _ => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        }
    }

    pub(crate) fn read_exact_postings(
        &self,
        root: &SegmentMetadataRoot,
        selection: &SegmentExactPostingsSelection,
    ) -> Result<SegmentExactPostings, SegmentMetadataFacadeError> {
        self.ensure_selection(root, selection)?;
        let backend = match (&self.backend, &root.backend, &selection.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { index, .. },
                SegmentMetadataRootBackend::Schema6 { index: root, .. },
                SegmentExactPostingsSelectionBackend::Schema6(selection),
            ) => SegmentExactPostingsBackend::Schema6(index.read_exact_postings(root, selection)?),
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
                SegmentExactPostingsSelectionBackend::Schema7(selection),
            ) => SegmentExactPostingsBackend::Schema7(index.read_exact_postings(root, selection)?),
            _ => return Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        };
        Ok(SegmentExactPostings {
            provenance: self.guard.provenance(),
            backend,
        })
    }

    pub(crate) fn visit_exact_postings_refs(
        &self,
        root: &SegmentMetadataRoot,
        postings: &SegmentExactPostings,
        mut visitor: impl FnMut(u32) -> bool,
    ) -> Result<bool, SegmentMetadataFacadeError> {
        let refs = self.exact_postings_refs(root, postings)?;
        for series_ref in refs.iter().copied() {
            if !visitor(series_ref) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Exhaustively visits the authenticated exact-postings inventory in
    /// canonical key order. This is an offline verification surface for the
    /// schema-7/8 authenticated index; ordinary queries continue to select
    /// only touched postings.
    pub(crate) fn visit_authenticated_exact_postings(
        &self,
        root: &SegmentMetadataRoot,
        mut visitor: impl FnMut(u32, u32, u32, u64, &[u32]) -> bool,
    ) -> Result<bool, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        match (&self.backend, &root.backend) {
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
            ) => Ok(index.visit_exact_postings_selections(
                root,
                |name_sym, value_sym, selection| {
                    let encoded_len = index.selection_metadata(root, selection)?.byte_len;
                    let ref_count = index.selection_ref_count(root, selection)?;
                    let postings = index.read_exact_postings(root, selection)?;
                    let refs = index.postings(root, &postings)?;
                    Ok(visitor(name_sym, value_sym, ref_count, encoded_len, refs))
                },
            )?),
            (
                SegmentMetadataSessionBackend::Schema6 { .. },
                SegmentMetadataRootBackend::Schema6 { .. },
            ) => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
            _ => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        }
    }

    pub(crate) fn visit_label_names(
        &self,
        root: &SegmentMetadataRoot,
        mut visitor: impl FnMut(u32, &str) -> bool,
    ) -> Result<bool, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        match (&self.backend, &root.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { index, .. },
                SegmentMetadataRootBackend::Schema6 { index: root, .. },
            ) => {
                let directory = index.load_auxiliary_directory(root)?;
                for symbol_id in index.label_name_symbols(&directory)? {
                    let mut keep_going = true;
                    self.symbols.visit_required_resolved(symbol_id, |value| {
                        keep_going = visitor(symbol_id, value);
                        Ok(())
                    })?;
                    if !keep_going {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
            ) => {
                let directory = index.load_auxiliary_directory(root)?;
                for symbol_id in index.label_name_symbols(root, &directory)? {
                    let mut keep_going = true;
                    self.symbols.visit_required_resolved(symbol_id, |value| {
                        keep_going = visitor(symbol_id, value);
                        Ok(())
                    })?;
                    if !keep_going {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        }
    }

    /// Visits FST values without materializing a value vector. When a time
    /// range is supplied, schema 6 deliberately ignores its advisory v7
    /// summaries. Schema 7/8 prunes only from paired CRC-authenticated v8/v9
    /// ranges. An authenticated FST with no range record is canonically unconstrained and
    /// is emitted conservatively; a missing emitted value inside an existing
    /// paired inventory is sticky index corruption.
    pub(crate) fn visit_label_values(
        &self,
        root: &SegmentMetadataRoot,
        label_name_sym: u32,
        prefix: Option<&str>,
        time_range: Option<(u64, u64)>,
        mut visitor: impl FnMut(u32, &str) -> bool,
    ) -> Result<bool, SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        if let Some((start_ms, end_ms)) = time_range {
            validate_time_range(start_ms, end_ms)?;
        }
        match (&self.backend, &root.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { index, .. },
                SegmentMetadataRootBackend::Schema6 { index: root, .. },
            ) => {
                let directory = index.load_auxiliary_directory(root)?;
                let Some(values) = index.load_label_value_fst(&directory, label_name_sym)? else {
                    return Ok(true);
                };
                Ok(
                    index.visit_label_values_with_prefix(
                        &values,
                        &self.symbols,
                        prefix,
                        visitor,
                    )?,
                )
            }
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
            ) => {
                let directory = index.load_auxiliary_directory(root)?;
                let Some(values) = index.load_label_value_fst(root, &directory, label_name_sym)?
                else {
                    return Ok(true);
                };
                let ranges = if time_range.is_some() {
                    index.load_label_value_time_ranges(root, &directory, label_name_sym)?
                } else {
                    None
                };
                let mut range_error = None;
                let exhausted = index.visit_label_values_with_prefix(
                    root,
                    &values,
                    &self.symbols,
                    prefix,
                    |symbol_id, value| {
                        if let (Some((start_ms, end_ms)), Some(ranges)) =
                            (time_range, ranges.as_ref())
                        {
                            match index.required_label_value_time_range(root, ranges, symbol_id) {
                                Ok(range) if !range.overlaps(start_ms, end_ms) => return true,
                                Ok(_) => {}
                                Err(error) => {
                                    range_error = Some(error);
                                    return false;
                                }
                            }
                        }
                        visitor(symbol_id, value)
                    },
                );
                if let Some(error) = range_error {
                    return Err(error.into());
                }
                Ok(exhausted?)
            }
            _ => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        }
    }

    fn selection_metadata(
        &self,
        root: &SegmentMetadataRoot,
        selection: &SegmentExactPostingsSelection,
    ) -> Result<ExactPostingsMetadata, SegmentMetadataFacadeError> {
        self.ensure_selection(root, selection)?;
        match (&self.backend, &root.backend, &selection.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { index, .. },
                SegmentMetadataRootBackend::Schema6 { index: root, .. },
                SegmentExactPostingsSelectionBackend::Schema6(selection),
            ) => Ok(index.selection_metadata(root, selection)?),
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
                SegmentExactPostingsSelectionBackend::Schema7(selection),
            ) => Ok(index.selection_metadata(root, selection)?),
            _ => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        }
    }

    fn exact_postings_refs<'a>(
        &'a self,
        root: &SegmentMetadataRoot,
        postings: &'a SegmentExactPostings,
    ) -> Result<&'a [u32], SegmentMetadataFacadeError> {
        self.ensure_postings(root, postings)?;
        match (&self.backend, &root.backend, &postings.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 { index, .. },
                SegmentMetadataRootBackend::Schema6 { .. },
                SegmentExactPostingsBackend::Schema6(postings),
            ) => Ok(index.postings(postings)?),
            (
                SegmentMetadataSessionBackend::Schema7 { index, .. },
                SegmentMetadataRootBackend::Schema7 { index: root, .. },
                SegmentExactPostingsBackend::Schema7(postings),
            ) => Ok(index.postings(root, postings)?),
            _ => Err(SegmentMetadataFacadeError::ForeignLayoutBackend),
        }
    }

    fn ensure_root(&self, root: &SegmentMetadataRoot) -> Result<(), SegmentMetadataFacadeError> {
        if root.provenance.matches(&self.guard) {
            Ok(())
        } else {
            Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
        }
    }

    fn ensure_selection(
        &self,
        root: &SegmentMetadataRoot,
        selection: &SegmentExactPostingsSelection,
    ) -> Result<(), SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        if selection.provenance.matches(&self.guard) {
            Ok(())
        } else {
            Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
        }
    }

    fn ensure_postings(
        &self,
        root: &SegmentMetadataRoot,
        postings: &SegmentExactPostings,
    ) -> Result<(), SegmentMetadataFacadeError> {
        self.ensure_root(root)?;
        if postings.provenance.matches(&self.guard) {
            Ok(())
        } else {
            Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
        }
    }
}

impl SegmentMetadataRoot {
    pub(crate) fn series_count(&self) -> u32 {
        self.series_count
    }
}

fn validate_time_range(start_ms: u64, end_ms: u64) -> Result<(), SegmentMetadataFacadeError> {
    if start_ms <= end_ms {
        Ok(())
    } else {
        Err(SegmentMetadataFacadeError::ReversedTimeRange { start_ms, end_ms })
    }
}

#[cfg(test)]
mod tests;
