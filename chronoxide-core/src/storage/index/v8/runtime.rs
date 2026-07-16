//! Aggregate-governed positional runtime for schema-7 `indexes.puffin` v8.
//!
//! The long-lived owner retains only the registered segment generation. Open
//! stages and validates the two fixed root ranges; all directories, pages and
//! payloads remain lazy. A query session may decode the complete v8 root only
//! after consuming same-generation series- and symbol-count capabilities.

use std::io;
use std::ops::Deref;

use thiserror::Error;

use crate::storage::index::ExactPostingsMetadata;
use crate::storage::metadata_cache::{
    LoadedMetadata, MetadataCacheError, MetadataCacheKey, MetadataCacheKeyError, MetadataCachePin,
};
use crate::storage::metadata_governor::MetadataCacheClass;
use crate::storage::metadata_runtime::{
    GovernedArtifactReader, RegisteredSegment, SegmentGenerationProvenance, SegmentReadGuard,
    StoreMetadataRuntimeError,
};
use crate::storage::segment::SegmentFile;
use crate::storage::series::GovernedSeriesCountBinding;
use crate::storage::symbols::{GovernedSymbolCountBinding, GovernedSymbolReaderError};

use super::codec::{decode_exact_directory, decode_exact_page, decode_exact_postings, decode_root};
use super::{
    AuthenticatedIndexFormat, EXACT_PAGE_LEN, ExactDirectory, ExactPageDescriptor, ExactRecord,
    HEADER_LEN, RootCounts, SegmentIndexV8Layout, TRAILER_CRC_OFFSET,
    TRAILER_EXACT_PAGE_COUNT_OFFSET, TRAILER_EXACT_PAGE_LEN_OFFSET,
    TRAILER_EXACT_RECORD_LEN_OFFSET, TRAILER_FILE_LEN_OFFSET, TRAILER_LEN, TRAILER_RESERVED_OFFSET,
    TRAILER_TERMINAL_MAGIC_OFFSET,
};

mod auxiliary;

/// Failures at the governed schema-7 index-container boundary.
#[derive(Debug, Error)]
pub(crate) enum Schema7IndexReaderError {
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Cache(#[from] MetadataCacheError),
    #[error(transparent)]
    CacheKey(#[from] MetadataCacheKeyError),
    #[error(transparent)]
    Symbols(#[from] GovernedSymbolReaderError),
    #[error("schema-7 index value belongs to another segment generation")]
    ForeignSegmentGeneration,
    #[error("schema-7 index value belongs to another validated root or protected record")]
    ForeignRootContext,
}

#[derive(Debug)]
struct ValidatedIndexHeaderV8 {
    bytes: [u8; HEADER_LEN],
    format: AuthenticatedIndexFormat,
}

#[derive(Debug)]
struct ValidatedIndexTrailerV8 {
    bytes: [u8; TRAILER_LEN],
    format: AuthenticatedIndexFormat,
}

/// Immutable facts decoded from one count-bound v8 root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7IndexRootV8 {
    layout: SegmentIndexV8Layout,
}

impl Schema7IndexRootV8 {
    pub(crate) fn file_len(&self) -> u64 {
        self.layout.file_len
    }

    pub(crate) fn exact_entry_count(&self) -> u64 {
        self.layout.exact_entry_count
    }

    pub(crate) fn exact_page_count(&self) -> u32 {
        self.layout.exact_page_count
    }

    pub(crate) fn auxiliary_entry_count(&self) -> u32 {
        self.layout.auxiliary_entry_count
    }

    pub(crate) fn series_count(&self) -> u32 {
        self.layout.counts.series
    }

    pub(crate) fn symbol_count(&self) -> u32 {
        self.layout.counts.symbols
    }
}

/// Long-lived generation owner with no read guard, file lease, or cache pin.
pub(crate) struct GovernedSchema7IndexReader {
    registered: RegisteredSegment,
    format: AuthenticatedIndexFormat,
}

/// Query-scoped authorization for schema-7 index metadata.
pub(crate) struct GovernedSchema7IndexSession {
    guard: SegmentReadGuard,
    format: AuthenticatedIndexFormat,
}

/// Query-local root, bound only from same-generation root capabilities.
pub(crate) struct GovernedSchema7BoundIndexRoot {
    provenance: SegmentGenerationProvenance,
    _header: MetadataCachePin<ValidatedIndexHeaderV8>,
    _trailer: MetadataCachePin<ValidatedIndexTrailerV8>,
    value: Schema7IndexRootV8,
}

#[derive(Debug)]
struct ValidatedExactDirectory {
    root: Schema7IndexRootV8,
    value: ExactDirectory,
}

impl ValidatedExactDirectory {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<ExactPageDescriptor>(
                self.value.descriptors.capacity(),
                "schema-7 exact-directory charge overflows",
            )?)
            .ok_or_else(|| invalid_data("schema-7 exact-directory charge overflows"))
    }
}

#[derive(Debug)]
struct ValidatedExactPage {
    root: Schema7IndexRootV8,
    page_index: u32,
    descriptor: ExactPageDescriptor,
    records: Vec<ExactRecord>,
}

impl ValidatedExactPage {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<ExactRecord>(
                self.records.capacity(),
                "schema-7 exact-page charge overflows",
            )?)
            .ok_or_else(|| invalid_data("schema-7 exact-page charge overflows"))
    }
}

#[derive(Debug)]
struct ValidatedExactPostings {
    root: Schema7IndexRootV8,
    record: ExactRecord,
    refs: Vec<u32>,
}

impl ValidatedExactPostings {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<u32>(
                self.refs.capacity(),
                "schema-7 exact-postings charge overflows",
            )?)
            .ok_or_else(|| invalid_data("schema-7 exact-postings charge overflows"))
    }
}

/// Protected locator which cannot be detached from its root and generation.
#[derive(Debug)]
pub(crate) struct GovernedSchema7ExactPostingsSelection {
    provenance: SegmentGenerationProvenance,
    page: MetadataCachePin<ValidatedExactPage>,
    record_index: usize,
    #[cfg(test)]
    substitution: Option<ExactSelectionSubstitution>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
enum ExactSelectionSubstitution {
    Key,
    Locator,
    RefCount,
    PayloadCrc,
    PageIndex,
    Descriptor,
}

/// Query-local decoded postings pin.
#[derive(Debug)]
pub(crate) struct GovernedSchema7ExactPostings {
    provenance: SegmentGenerationProvenance,
    root: Schema7IndexRootV8,
    record: ExactRecord,
    value: MetadataCachePin<ValidatedExactPostings>,
}

impl GovernedSchema7ExactPostings {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    fn substitute_record_for_test(&mut self) {
        self.record.key.1 ^= 1;
    }
}

impl Deref for GovernedSchema7BoundIndexRoot {
    type Target = Schema7IndexRootV8;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl GovernedSchema7IndexReader {
    /// Stages and validates only the fixed 16-byte header and 256-byte trailer.
    /// Count- and locator-bound root validation is deferred to a query session.
    pub(crate) fn open(registered: &RegisteredSegment) -> Result<Self, Schema7IndexReaderError> {
        Self::open_with_format(registered, AuthenticatedIndexFormat::V8Raw)
    }

    pub(crate) fn open_v9(registered: &RegisteredSegment) -> Result<Self, Schema7IndexReaderError> {
        Self::open_with_format(registered, AuthenticatedIndexFormat::V9Adaptive)
    }

    fn open_with_format(
        registered: &RegisteredSegment,
        format: AuthenticatedIndexFormat,
    ) -> Result<Self, Schema7IndexReaderError> {
        let guard = registered.read_guard()?;
        let (header, trailer) = load_fixed_root(&guard, format)?;
        drop(trailer);
        drop(header);
        drop(guard);
        Ok(Self {
            registered: registered.clone(),
            format,
        })
    }

    pub(crate) fn query_session(
        &self,
    ) -> Result<GovernedSchema7IndexSession, Schema7IndexReaderError> {
        Ok(GovernedSchema7IndexSession {
            guard: self.registered.read_guard()?,
            format: self.format,
        })
    }

    pub(crate) fn segment_identity(&self) -> &str {
        self.registered.segment_identity()
    }
}

impl GovernedSchema7IndexSession {
    /// Consumes only unforgeable same-generation series/symbol count bindings.
    /// The trailer's own counts never authorize a root or payload read.
    pub(crate) fn bind_segment_roots(
        &self,
        series: GovernedSeriesCountBinding,
        symbols: GovernedSymbolCountBinding,
    ) -> Result<GovernedSchema7BoundIndexRoot, Schema7IndexReaderError> {
        if !series.matches(&self.guard) || !symbols.matches(&self.guard) {
            return Err(Schema7IndexReaderError::ForeignSegmentGeneration);
        }
        let (header, trailer) = load_fixed_root(&self.guard, self.format)?;
        let expected_counts = RootCounts {
            series: series.num_series(),
            symbols: symbols.symbol_count(),
        };
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let layout = match decode_root(
            reader.len(),
            &header.bytes,
            &trailer.bytes,
            expected_counts,
            self.format,
        ) {
            Ok(layout) => layout,
            Err(error) => {
                drop(trailer);
                drop(header);
                return Err(reader.record_validation_error(error).into());
            }
        };
        Ok(GovernedSchema7BoundIndexRoot {
            provenance: self.guard.provenance(),
            _header: header,
            _trailer: trailer,
            value: Schema7IndexRootV8 { layout },
        })
    }

    pub(crate) fn root<'a>(
        &'a self,
        root: &'a GovernedSchema7BoundIndexRoot,
    ) -> Result<&'a Schema7IndexRootV8, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        Ok(root)
    }

    pub(crate) fn exact_postings_metadata(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> Result<Option<ExactPostingsMetadata>, Schema7IndexReaderError> {
        let Some(selection) = self.select_exact_postings(root, label_name_sym, label_value_sym)?
        else {
            return Ok(None);
        };
        Ok(Some(self.selection_metadata(root, &selection)?))
    }

    pub(crate) fn select_exact_postings(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> Result<Option<GovernedSchema7ExactPostingsSelection>, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        if label_name_sym >= root.value.layout.counts.symbols
            || label_value_sym >= root.value.layout.counts.symbols
        {
            return Ok(None);
        }
        let directory = self.load_exact_directory(root)?;
        let key = (label_name_sym, label_value_sym);
        let page_index = directory
            .value
            .descriptors
            .partition_point(|descriptor| descriptor.last_key < key);
        let Some(descriptor) = directory.value.descriptors.get(page_index).copied() else {
            return Ok(None);
        };
        if key < descriptor.first_key {
            return Ok(None);
        }
        let page = self.load_exact_page(root, page_index, descriptor)?;
        let Some(record_index) = page
            .records
            .binary_search_by_key(&key, |record| record.key)
            .ok()
        else {
            return Ok(None);
        };
        Ok(Some(GovernedSchema7ExactPostingsSelection {
            provenance: self.guard.provenance(),
            page,
            record_index,
            #[cfg(test)]
            substitution: None,
        }))
    }

    pub(crate) fn visit_exact_postings_selections(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        mut visitor: impl FnMut(
            u32,
            u32,
            &GovernedSchema7ExactPostingsSelection,
        ) -> Result<bool, Schema7IndexReaderError>,
    ) -> Result<bool, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        let directory = self.load_exact_directory(root)?;
        let mut visited = 0u64;
        for (page_index, descriptor) in directory.value.descriptors.iter().copied().enumerate() {
            let page = self.load_exact_page(root, page_index, descriptor)?;
            for (record_index, record) in page.records.iter().enumerate() {
                let selection = GovernedSchema7ExactPostingsSelection {
                    provenance: self.guard.provenance(),
                    page: page.clone(),
                    record_index,
                    #[cfg(test)]
                    substitution: None,
                };
                if !visitor(record.key.0, record.key.1, &selection)? {
                    return Ok(false);
                }
                visited = visited.checked_add(1).ok_or_else(|| {
                    Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                        "authenticated exact-postings visit count overflows",
                    )))
                })?;
            }
        }
        if visited != root.value.layout.exact_entry_count {
            return Err(Schema7IndexReaderError::Cache(self.record_index_error(
                invalid_data("authenticated exact-postings visit count disagrees with the root"),
            )));
        }
        Ok(true)
    }

    pub(crate) fn selection_metadata(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        selection: &GovernedSchema7ExactPostingsSelection,
    ) -> Result<ExactPostingsMetadata, Schema7IndexReaderError> {
        let record = self.selection_record(root, selection)?;
        Ok(exact_postings_metadata(record))
    }

    pub(crate) fn selection_ref_count(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        selection: &GovernedSchema7ExactPostingsSelection,
    ) -> Result<u32, Schema7IndexReaderError> {
        Ok(self.selection_record(root, selection)?.ref_count)
    }

    pub(crate) fn read_exact_postings(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        selection: &GovernedSchema7ExactPostingsSelection,
    ) -> Result<GovernedSchema7ExactPostings, Schema7IndexReaderError> {
        let record = self.selection_record(root, selection)?;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            record.postings.offset,
            record.postings.len,
            MetadataCacheClass::Postings,
        )?;
        let declared = (std::mem::size_of::<ValidatedExactPostings>() as u64)
            .checked_add(u64::from(record.ref_count).checked_mul(4).ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 exact-postings declared charge overflows",
                )))
            })?)
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 exact-postings declared charge overflows",
                )))
            })?;
        let root_context = root.value;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let refs = decode_exact_postings(bytes, record, root_context.layout)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedExactPostings {
                root: root_context,
                record,
                refs,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context || value.record != record {
            return Err(self.record_cached_context_error(
                "cached schema-7 exact postings have a foreign root or protected record",
            ));
        }
        Ok(GovernedSchema7ExactPostings {
            provenance: self.guard.provenance(),
            root: root_context,
            record,
            value,
        })
    }

    pub(crate) fn postings<'a>(
        &'a self,
        root: &GovernedSchema7BoundIndexRoot,
        postings: &'a GovernedSchema7ExactPostings,
    ) -> Result<&'a [u32], Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        self.ensure_provenance(&postings.provenance)?;
        if postings.root != root.value || postings.value.root != postings.root {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        if postings.value.record != postings.record {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        Ok(&postings.value.refs)
    }

    fn load_exact_directory(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
    ) -> Result<MetadataCachePin<ValidatedExactDirectory>, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        let root_context = root.value;
        let locator = root_context.layout.exact_directory;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            locator.offset,
            locator.len,
            MetadataCacheClass::IndexDirectory,
        )?;
        let declared = (std::mem::size_of::<ValidatedExactDirectory>() as u64)
            .checked_add(
                u64::from(root_context.layout.exact_page_count)
                    .checked_mul(std::mem::size_of::<ExactPageDescriptor>() as u64)
                    .ok_or_else(|| {
                        Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                            "schema-7 exact-directory declared charge overflows",
                        )))
                    })?,
            )
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 exact-directory declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let directory = decode_exact_directory(bytes, root_context.layout)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedExactDirectory {
                root: root_context,
                value: directory,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context {
            return Err(self.record_cached_context_error(
                "cached schema-7 exact directory has a foreign root",
            ));
        }
        Ok(value)
    }

    fn load_exact_page(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        page_index: usize,
        descriptor: ExactPageDescriptor,
    ) -> Result<MetadataCachePin<ValidatedExactPage>, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        let root_context = root.value;
        let relative_offset = u64::try_from(page_index)
            .ok()
            .and_then(|page_index| page_index.checked_mul(EXACT_PAGE_LEN as u64))
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(
                    self.record_index_error(invalid_data("schema-7 exact-page offset overflows")),
                )
            })?;
        let offset = root_context
            .layout
            .exact_pages
            .offset
            .checked_add(relative_offset)
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(
                    self.record_index_error(invalid_data("schema-7 exact-page offset overflows")),
                )
            })?;
        let page_index_u32 = u32::try_from(page_index).map_err(|_| {
            Schema7IndexReaderError::Cache(
                self.record_index_error(invalid_data("schema-7 exact-page index exceeds u32")),
            )
        })?;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            offset,
            EXACT_PAGE_LEN as u64,
            MetadataCacheClass::IndexPage,
        )?;
        let declared = (std::mem::size_of::<ValidatedExactPage>() as u64)
            .checked_add(
                u64::from(descriptor.record_count)
                    .checked_mul(std::mem::size_of::<ExactRecord>() as u64)
                    .ok_or_else(|| {
                        Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                            "schema-7 exact-page declared charge overflows",
                        )))
                    })?,
            )
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 exact-page declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let records = decode_exact_page(bytes, page_index, descriptor, root_context.layout)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedExactPage {
                root: root_context,
                page_index: page_index_u32,
                descriptor,
                records,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context
            || value.page_index != page_index_u32
            || value.descriptor != descriptor
        {
            return Err(self.record_cached_context_error(
                "cached schema-7 exact page has foreign root, ordinal, or descriptor context",
            ));
        }
        Ok(value)
    }

    fn selection_record(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        selection: &GovernedSchema7ExactPostingsSelection,
    ) -> Result<ExactRecord, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        self.ensure_provenance(&selection.provenance)?;
        let page = &selection.page;
        if page.root != root.value {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        let page_index = page.page_index;
        let descriptor = page.descriptor;
        let Some(record) = page.records.get(selection.record_index).copied() else {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        };
        #[cfg(test)]
        let (page_index, descriptor, record) =
            selection.substituted_context(page_index, descriptor, record);
        if page_index != page.page_index
            || descriptor != page.descriptor
            || record != page.records[selection.record_index]
        {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        if page_index >= root.value.layout.exact_page_count
            || usize::try_from(descriptor.record_count).ok() != Some(page.records.len())
            || page.records.first().map(|record| record.key) != Some(descriptor.first_key)
            || page.records.last().map(|record| record.key) != Some(descriptor.last_key)
            || descriptor.first_key > record.key
            || descriptor.last_key < record.key
        {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        let preceding = u64::from(page_index)
            .checked_mul(super::EXACT_RECORDS_PER_PAGE as u64)
            .ok_or(Schema7IndexReaderError::ForeignRootContext)?;
        let remaining = root
            .value
            .layout
            .exact_entry_count
            .checked_sub(preceding)
            .ok_or(Schema7IndexReaderError::ForeignRootContext)?;
        if u64::from(descriptor.record_count) != remaining.min(super::EXACT_RECORDS_PER_PAGE as u64)
        {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        Ok(record)
    }

    fn ensure_bound_root(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
    ) -> Result<(), Schema7IndexReaderError> {
        self.ensure_provenance(&root.provenance)?;

        // `value` is a compact copy used by every lazy locator path, while the
        // two cache pins are the immutable root authority. Re-derive the value
        // from those pinned bytes before trusting the copy. This is pure root
        // decoding: it performs no file I/O and a substituted wrapper is a
        // caller-context error, not artifact corruption.
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let layout = decode_root(
            reader.len(),
            &root._header.bytes,
            &root._trailer.bytes,
            root.value.layout.counts,
            self.format,
        )
        .map_err(|_| Schema7IndexReaderError::ForeignRootContext)?;
        if layout != root.value.layout {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        Ok(())
    }

    fn ensure_provenance(
        &self,
        provenance: &SegmentGenerationProvenance,
    ) -> Result<(), Schema7IndexReaderError> {
        if !provenance.matches(&self.guard) {
            return Err(Schema7IndexReaderError::ForeignSegmentGeneration);
        }
        self.guard
            .reader(SegmentFile::Indexes)?
            .check_recorded_error()?;
        Ok(())
    }

    fn record_index_error(&self, error: io::Error) -> MetadataCacheError {
        match self.guard.reader(SegmentFile::Indexes) {
            Ok(reader) => reader.record_validation_error(error),
            Err(error) => MetadataCacheError::from_io(io::Error::other(error.to_string())),
        }
    }

    fn record_cached_context_error(&self, message: &'static str) -> Schema7IndexReaderError {
        Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(message)))
    }

    #[cfg(test)]
    fn inject_exact_postings_cache_context_collision_for_test(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        selection: &GovernedSchema7ExactPostingsSelection,
    ) -> Result<(), Schema7IndexReaderError> {
        let record = self.selection_record(root, selection)?;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            record.postings.offset,
            record.postings.len,
            MetadataCacheClass::Postings,
        )?;
        let declared = (std::mem::size_of::<ValidatedExactPostings>() as u64)
            .checked_add(u64::from(record.ref_count).checked_mul(4).ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 exact-postings test charge overflows",
                )))
            })?)
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 exact-postings test charge overflows",
                )))
            })?;
        let root_context = root.value;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let refs = decode_exact_postings(bytes, record, root_context.layout)
                .map_err(MetadataCacheError::from_io)?;
            let mut foreign_record = record;
            foreign_record.payload_crc32c ^= 1;
            let value = ValidatedExactPostings {
                root: root_context,
                record: foreign_record,
                refs,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        drop(value);
        Ok(())
    }
}

impl GovernedSchema7ExactPostingsSelection {
    #[cfg(test)]
    fn substituted_context(
        &self,
        mut page_index: u32,
        mut descriptor: ExactPageDescriptor,
        mut record: ExactRecord,
    ) -> (u32, ExactPageDescriptor, ExactRecord) {
        match self.substitution {
            Some(ExactSelectionSubstitution::Key) => record.key.1 ^= 1,
            Some(ExactSelectionSubstitution::Locator) => record.postings.offset ^= 1,
            Some(ExactSelectionSubstitution::RefCount) => record.ref_count ^= 1,
            Some(ExactSelectionSubstitution::PayloadCrc) => record.payload_crc32c ^= 1,
            Some(ExactSelectionSubstitution::PageIndex) => page_index ^= 1,
            Some(ExactSelectionSubstitution::Descriptor) => descriptor.first_key.1 ^= 1,
            None => {}
        }
        (page_index, descriptor, record)
    }

    #[cfg(test)]
    fn substitute_record_for_test(&mut self) {
        self.substitution = Some(ExactSelectionSubstitution::Key);
    }

    #[cfg(test)]
    fn substitute_locator_for_test(&mut self) {
        self.substitution = Some(ExactSelectionSubstitution::Locator);
    }

    #[cfg(test)]
    fn substitute_ref_count_for_test(&mut self) {
        self.substitution = Some(ExactSelectionSubstitution::RefCount);
    }

    #[cfg(test)]
    fn substitute_payload_crc_for_test(&mut self) {
        self.substitution = Some(ExactSelectionSubstitution::PayloadCrc);
    }

    #[cfg(test)]
    fn substitute_page_index_for_test(&mut self) {
        self.substitution = Some(ExactSelectionSubstitution::PageIndex);
    }

    #[cfg(test)]
    fn substitute_descriptor_for_test(&mut self) {
        self.substitution = Some(ExactSelectionSubstitution::Descriptor);
    }
}

fn exact_postings_metadata(record: ExactRecord) -> ExactPostingsMetadata {
    ExactPostingsMetadata {
        byte_len: record.postings.len,
        time_range: record.time_range,
    }
}

fn load_fixed_root(
    guard: &SegmentReadGuard,
    format: AuthenticatedIndexFormat,
) -> Result<
    (
        MetadataCachePin<ValidatedIndexHeaderV8>,
        MetadataCachePin<ValidatedIndexTrailerV8>,
    ),
    Schema7IndexReaderError,
> {
    let reader = guard.reader(SegmentFile::Indexes)?;
    let minimum_len = (HEADER_LEN + TRAILER_LEN) as u64;
    if reader.len() < minimum_len {
        return Err(reader
            .record_validation_error(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "schema-7 index source is shorter than its fixed root",
            ))
            .into());
    }
    let header_key = metadata_key(&reader, 0, HEADER_LEN as u64, MetadataCacheClass::IndexRoot)?;
    let header = reader.get_or_load_owned(
        header_key,
        std::mem::size_of::<ValidatedIndexHeaderV8>() as u64,
        |bytes| {
            let bytes: [u8; HEADER_LEN] = bytes.try_into().map_err(|_| {
                MetadataCacheError::from_io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "schema-7 index header has the wrong exact length",
                ))
            })?;
            validate_fixed_header(&bytes, format).map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(
                ValidatedIndexHeaderV8 { bytes, format },
                std::mem::size_of::<ValidatedIndexHeaderV8>() as u64,
            ))
        },
    )?;
    if header.format != format {
        return Err(reader
            .record_validation_error(invalid_data(
                "cached authenticated index header has a foreign format",
            ))
            .into());
    }

    let trailer_offset = reader.len() - TRAILER_LEN as u64;
    let trailer_key = metadata_key(
        &reader,
        trailer_offset,
        TRAILER_LEN as u64,
        MetadataCacheClass::IndexRoot,
    )?;
    let file_len = reader.len();
    let trailer = reader.get_or_load_owned(
        trailer_key,
        std::mem::size_of::<ValidatedIndexTrailerV8>() as u64,
        move |bytes| {
            let bytes: [u8; TRAILER_LEN] = bytes.try_into().map_err(|_| {
                MetadataCacheError::from_io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "schema-7 index trailer has the wrong exact length",
                ))
            })?;
            validate_fixed_trailer(&bytes, file_len, format)
                .map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(
                ValidatedIndexTrailerV8 { bytes, format },
                std::mem::size_of::<ValidatedIndexTrailerV8>() as u64,
            ))
        },
    )?;
    if trailer.format != format {
        return Err(reader
            .record_validation_error(invalid_data(
                "cached authenticated index trailer has a foreign format",
            ))
            .into());
    }
    Ok((header, trailer))
}

fn validate_fixed_header(
    header: &[u8; HEADER_LEN],
    format: AuthenticatedIndexFormat,
) -> io::Result<()> {
    if super::read_u32(header, 0) != super::SEGMENT_INDEXES_MAGIC {
        return Err(invalid_data("schema-7 index header magic mismatch"));
    }
    if super::read_u16(header, 4) != format.version() {
        return Err(invalid_data("schema-7 index header version mismatch"));
    }
    if super::read_u16(header, 6) != 0
        || super::read_u32(header, 8) != HEADER_LEN as u32
        || super::read_u32(header, 12) != 0
    {
        return Err(invalid_data(
            "schema-7 index header fields are noncanonical",
        ));
    }
    Ok(())
}

fn validate_fixed_trailer(
    trailer: &[u8; TRAILER_LEN],
    file_len: u64,
    format: AuthenticatedIndexFormat,
) -> io::Result<()> {
    if super::read_u32(trailer, 0) != super::SEGMENT_INDEX_TRAILER_MAGIC
        || super::read_u16(trailer, 4) != format.version()
        || super::read_u16(trailer, 6) != 0
        || super::read_u32(trailer, 8) != TRAILER_LEN as u32
        || super::read_u32(trailer, 12) != 0
    {
        return Err(invalid_data(
            "schema-7 index trailer fields are noncanonical",
        ));
    }
    if super::read_u64(trailer, TRAILER_FILE_LEN_OFFSET) != file_len {
        return Err(invalid_data("schema-7 index trailer file length mismatch"));
    }
    if super::read_u32(trailer, TRAILER_EXACT_RECORD_LEN_OFFSET) != super::EXACT_RECORD_LEN as u32
        || super::read_u32(trailer, TRAILER_EXACT_PAGE_LEN_OFFSET) != EXACT_PAGE_LEN as u32
    {
        return Err(invalid_data(
            "schema-7 index trailer exact layout is invalid",
        ));
    }
    let exact_entries = super::read_u64(trailer, super::TRAILER_EXACT_ENTRY_COUNT_OFFSET);
    let expected_pages = exact_entries
        .checked_add(super::EXACT_RECORDS_PER_PAGE as u64 - 1)
        .and_then(|value| value.checked_div(super::EXACT_RECORDS_PER_PAGE as u64))
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| invalid_data("schema-7 index exact page count overflows"))?;
    if super::read_u32(trailer, TRAILER_EXACT_PAGE_COUNT_OFFSET) != expected_pages {
        return Err(invalid_data("schema-7 index exact page count is invalid"));
    }
    if trailer[TRAILER_RESERVED_OFFSET..TRAILER_TERMINAL_MAGIC_OFFSET]
        .iter()
        .any(|byte| *byte != 0)
        || super::read_u32(trailer, TRAILER_TERMINAL_MAGIC_OFFSET) != format.terminal_magic()
    {
        return Err(invalid_data(
            "schema-7 index trailer reserved bytes are non-zero",
        ));
    }
    let stored_crc = super::read_u32(trailer, TRAILER_CRC_OFFSET);
    if super::crc_with_zeroed_field(trailer, TRAILER_CRC_OFFSET) != stored_crc {
        return Err(invalid_data("schema-7 index trailer CRC mismatch"));
    }
    Ok(())
}

fn metadata_key(
    reader: &GovernedArtifactReader,
    offset: u64,
    length: u64,
    class: MetadataCacheClass,
) -> Result<MetadataCacheKey, MetadataCacheKeyError> {
    reader.metadata_cache_key(offset, length, class)
}

fn checked_vec_capacity_bytes<T>(capacity: usize, message: &'static str) -> io::Result<u64> {
    u64::try_from(capacity)
        .ok()
        .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<T>() as u64))
        .ok_or_else(|| invalid_data(message))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests;
