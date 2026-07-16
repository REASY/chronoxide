//! Governed read-only runtime for schema-6 `indexes.puffin` v7.
//!
//! Schema 7 retains the index-container byte layout. The schema-6/schema-7
//! A/B path therefore shares this positional, aggregate-governed adapter
//! instead of retaining the legacy reader's per-instance roots or descriptor.

use std::io;
use std::ops::Deref;

use thiserror::Error;

use crate::storage::index::{ExactPostingsMetadata, ExactPostingsSelection};
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

use super::codec::{
    ExactDirectory, ExactPageDescriptor, decode_exact_directory, decode_exact_postings,
    trusted_exact_page_selection, validate_exact_page,
};
use super::{
    EXACT_PAGE_LEN, SEGMENT_INDEX_V7_HEADER_LEN, SEGMENT_INDEX_V7_TRAILER_LEN,
    SegmentIndexV7Layout, decode_segment_indexes_v7_root, validate_segment_indexes_v7_header,
};

pub(crate) mod auxiliary;
pub(crate) mod metric;
pub(crate) mod routing;

/// Failures at the governed schema-6 index-container boundary.
#[derive(Debug, Error)]
pub(crate) enum Schema6IndexReaderError {
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Cache(#[from] MetadataCacheError),
    #[error(transparent)]
    CacheKey(#[from] MetadataCacheKeyError),
    #[error(transparent)]
    Symbols(#[from] GovernedSymbolReaderError),
    #[error("schema-6 index value belongs to another segment generation")]
    ForeignSegmentGeneration,
    #[error("schema-6 index value belongs to another validated root")]
    ForeignRootContext,
    #[error(
        "schema-6 postings series-count binding changed: cached={cached_num_series} requested={requested_num_series}"
    )]
    ForeignSeriesCountBinding {
        cached_num_series: u32,
        requested_num_series: u32,
    },
    #[error(
        "schema-6 index symbol-count binding changed: cached={cached_symbol_count} requested={requested_symbol_count}"
    )]
    ForeignSymbolCountBinding {
        cached_symbol_count: u32,
        requested_symbol_count: u32,
    },
}

#[derive(Debug)]
struct ValidatedIndexHeaderV7 {
    bytes: [u8; SEGMENT_INDEX_V7_HEADER_LEN],
}

impl ValidatedIndexHeaderV7 {
    fn charged_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

/// Immutable facts decoded from the exact schema-6 index header and trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema6IndexRootV7 {
    layout: SegmentIndexV7Layout,
}

impl Schema6IndexRootV7 {
    fn charged_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

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
}

/// Long-lived generation owner with no guard, descriptor, or root pin.
pub(crate) struct GovernedSchema6IndexReader {
    registered: RegisteredSegment,
}

/// Query-scoped authorization for schema-6 index metadata.
pub(crate) struct GovernedSchema6IndexSession {
    guard: SegmentReadGuard,
}

/// Query-local pins for both non-contiguous fixed root ranges.
#[derive(Debug)]
pub(crate) struct GovernedSchema6IndexRoot {
    provenance: SegmentGenerationProvenance,
    _header: MetadataCachePin<ValidatedIndexHeaderV7>,
    value: MetadataCachePin<Schema6IndexRootV7>,
}

/// Query-local binding between an index root and the authoritative series
/// count supplied by the schema-neutral segment facade.
pub(crate) struct GovernedSchema6BoundIndexRoot {
    provenance: SegmentGenerationProvenance,
    root: GovernedSchema6IndexRoot,
    num_series: u32,
    symbol_count: u32,
}

#[derive(Debug)]
struct ValidatedExactDirectory {
    root: Schema6IndexRootV7,
    symbol_count: u32,
    value: ExactDirectory,
}

impl ValidatedExactDirectory {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<ExactPageDescriptor>(
                self.value.descriptors.capacity(),
                "governed exact-directory charge overflows",
            )?)
            .ok_or_else(|| invalid_data("governed exact-directory charge overflows"))
    }
}

#[derive(Debug)]
struct ValidatedExactPageOwned {
    root: Schema6IndexRootV7,
    symbol_count: u32,
    page_index: u32,
    descriptor: ExactPageDescriptor,
    bytes: Vec<u8>,
}

impl ValidatedExactPageOwned {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<u8>(
                self.bytes.capacity(),
                "governed exact-page charge overflows",
            )?)
            .ok_or_else(|| invalid_data("governed exact-page charge overflows"))
    }

    fn selection(
        &self,
        root: Schema6IndexRootV7,
        symbol_count: u32,
        page_index: usize,
        descriptor: ExactPageDescriptor,
        key: (u32, u32),
    ) -> Result<Option<ExactPostingsSelection>, Schema6IndexReaderError> {
        if self.root != root
            || self.symbol_count != symbol_count
            || usize::try_from(self.page_index).ok() != Some(page_index)
            || self.descriptor != descriptor
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(trusted_exact_page_selection(&self.bytes, descriptor, key))
    }
}

#[derive(Debug)]
struct ValidatedExactPostings {
    root: Schema6IndexRootV7,
    offset: u64,
    length: u64,
    num_series: u32,
    refs: Vec<u32>,
}

impl ValidatedExactPostings {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<u32>(
                self.refs.capacity(),
                "governed exact-postings charge overflows",
            )?)
            .ok_or_else(|| invalid_data("governed exact-postings charge overflows"))
    }
}

/// Authenticated exact-postings locator bound to one segment generation and
/// one validated index root. It is intentionally neither `Copy` nor `Clone`.
#[derive(Debug)]
pub(crate) struct GovernedSchema6ExactPostingsSelection {
    provenance: SegmentGenerationProvenance,
    root: Schema6IndexRootV7,
    symbol_count: u32,
    value: ExactPostingsSelection,
}

/// Query-local exact postings pin. The decoded refs cannot be detached from
/// the session which proved their segment generation.
#[derive(Debug)]
pub(crate) struct GovernedSchema6ExactPostings {
    provenance: SegmentGenerationProvenance,
    root: Schema6IndexRootV7,
    offset: u64,
    length: u64,
    num_series: u32,
    value: MetadataCachePin<ValidatedExactPostings>,
}

impl GovernedSchema6ExactPostings {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }
}

impl Deref for GovernedSchema6IndexRoot {
    type Target = Schema6IndexRootV7;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl GovernedSchema6IndexReader {
    /// Validates both fixed roots and releases all query-scoped state before
    /// returning the long-lived owner.
    pub(crate) fn open(registered: &RegisteredSegment) -> Result<Self, Schema6IndexReaderError> {
        let session = GovernedSchema6IndexSession {
            guard: registered.read_guard()?,
        };
        let root = session.load_root()?;
        drop(root);
        drop(session);
        Ok(Self {
            registered: registered.clone(),
        })
    }

    pub(crate) fn query_session(
        &self,
    ) -> Result<GovernedSchema6IndexSession, Schema6IndexReaderError> {
        Ok(GovernedSchema6IndexSession {
            guard: self.registered.read_guard()?,
        })
    }

    pub(crate) fn segment_identity(&self) -> &str {
        self.registered.segment_identity()
    }
}

impl GovernedSchema6IndexSession {
    pub(crate) fn load_root(&self) -> Result<GovernedSchema6IndexRoot, Schema6IndexReaderError> {
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let file_len = reader.len();
        let minimum_len = (SEGMENT_INDEX_V7_HEADER_LEN + SEGMENT_INDEX_V7_TRAILER_LEN) as u64;
        if file_len < minimum_len {
            return Err(reader
                .record_validation_error(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "segment index v7 source is shorter than its fixed roots",
                ))
                .into());
        }

        let header_key = metadata_key(
            &reader,
            0,
            SEGMENT_INDEX_V7_HEADER_LEN as u64,
            MetadataCacheClass::IndexRoot,
        )?;
        let header = reader.get_or_load_owned(
            header_key,
            std::mem::size_of::<ValidatedIndexHeaderV7>() as u64,
            |bytes| {
                let bytes: [u8; SEGMENT_INDEX_V7_HEADER_LEN] = bytes.try_into().map_err(|_| {
                    MetadataCacheError::from_io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "segment index v7 header has the wrong exact length",
                    ))
                })?;
                validate_segment_indexes_v7_header(&bytes).map_err(MetadataCacheError::from_io)?;
                let value = ValidatedIndexHeaderV7 { bytes };
                let charged = value.charged_bytes();
                Ok(LoadedMetadata::new(value, charged))
            },
        )?;

        let trailer_offset = file_len - SEGMENT_INDEX_V7_TRAILER_LEN as u64;
        let trailer_key = metadata_key(
            &reader,
            trailer_offset,
            SEGMENT_INDEX_V7_TRAILER_LEN as u64,
            MetadataCacheClass::IndexRoot,
        )?;
        let header_bytes = header.bytes;
        let value = reader.get_or_load_owned(
            trailer_key,
            std::mem::size_of::<Schema6IndexRootV7>() as u64,
            move |bytes| {
                let trailer: [u8; SEGMENT_INDEX_V7_TRAILER_LEN] =
                    bytes.try_into().map_err(|_| {
                        MetadataCacheError::from_io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "segment index v7 trailer has the wrong exact length",
                        ))
                    })?;
                let layout = decode_segment_indexes_v7_root(file_len, &header_bytes, &trailer)
                    .map_err(MetadataCacheError::from_io)?;
                let value = Schema6IndexRootV7 { layout };
                let charged = value.charged_bytes();
                Ok(LoadedMetadata::new(value, charged))
            },
        )?;
        if value.layout.file_len != file_len {
            drop(value);
            drop(header);
            return Err(reader
                .record_validation_error(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cached segment index v7 root has a foreign file length",
                ))
                .into());
        }

        Ok(GovernedSchema6IndexRoot {
            provenance: self.guard.provenance(),
            _header: header,
            value,
        })
    }

    pub(crate) fn root<'a>(
        &'a self,
        root: &'a GovernedSchema6IndexRoot,
    ) -> Result<&'a Schema6IndexRootV7, Schema6IndexReaderError> {
        self.ensure_provenance(&root.provenance)?;
        Ok(root)
    }

    pub(crate) fn exact_postings_metadata(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> Result<Option<ExactPostingsMetadata>, Schema6IndexReaderError> {
        Ok(self
            .select_exact_postings(bound, label_name_sym, label_value_sym)?
            .map(|selection| selection.value.metadata()))
    }

    pub(crate) fn bind_segment_roots(
        &self,
        root: GovernedSchema6IndexRoot,
        series: GovernedSeriesCountBinding,
        symbols: GovernedSymbolCountBinding,
    ) -> Result<GovernedSchema6BoundIndexRoot, Schema6IndexReaderError> {
        self.ensure_provenance(&root.provenance)?;
        if !series.matches(&self.guard) || !symbols.matches(&self.guard) {
            return Err(Schema6IndexReaderError::ForeignSegmentGeneration);
        }
        Ok(GovernedSchema6BoundIndexRoot {
            provenance: self.guard.provenance(),
            root,
            num_series: series.num_series(),
            symbol_count: symbols.symbol_count(),
        })
    }

    pub(crate) fn select_exact_postings(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> Result<Option<GovernedSchema6ExactPostingsSelection>, Schema6IndexReaderError> {
        self.ensure_bound_root(bound)?;
        let directory = self.load_exact_directory(bound)?;
        if label_name_sym >= bound.symbol_count || label_value_sym >= bound.symbol_count {
            return Ok(None);
        }
        let root = &bound.root;
        let key = (label_name_sym, label_value_sym);
        let descriptor_index = directory
            .value
            .descriptors
            .partition_point(|descriptor| descriptor.last_key < key);
        let Some(descriptor) = directory.value.descriptors.get(descriptor_index).copied() else {
            return Ok(None);
        };
        if key < descriptor.first_key {
            return Ok(None);
        }
        let page = self.load_exact_page(bound, descriptor_index, descriptor)?;
        let Some(value) = page.selection(
            *root.value,
            bound.symbol_count,
            descriptor_index,
            descriptor,
            key,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(GovernedSchema6ExactPostingsSelection {
            provenance: self.guard.provenance(),
            root: *root.value,
            symbol_count: bound.symbol_count,
            value,
        }))
    }

    pub(crate) fn selection_metadata(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
        selection: &GovernedSchema6ExactPostingsSelection,
    ) -> Result<ExactPostingsMetadata, Schema6IndexReaderError> {
        self.ensure_bound_root(bound)?;
        self.ensure_provenance(&selection.provenance)?;
        if selection.root.layout != bound.root.value.layout
            || selection.symbol_count != bound.symbol_count
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(selection.value.metadata())
    }

    pub(crate) fn read_exact_postings(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
        selection: &GovernedSchema6ExactPostingsSelection,
    ) -> Result<GovernedSchema6ExactPostings, Schema6IndexReaderError> {
        self.ensure_provenance(&bound.provenance)?;
        let root = &bound.root;
        let num_series = bound.num_series;
        self.ensure_provenance(&root.provenance)?;
        self.ensure_provenance(&selection.provenance)?;
        let root_context = root.value.layout;
        if selection.root.layout != root_context || selection.symbol_count != bound.symbol_count {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        let (offset, length) = selection.value.postings();
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(&reader, offset, length, MetadataCacheClass::Postings)?;
        let max_count = length
            .checked_sub(4)
            .and_then(|bytes| bytes.checked_div(4))
            .ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed exact-postings length is not canonical",
                )))
            })?;
        let declared = (std::mem::size_of::<ValidatedExactPostings>() as u64)
            .checked_add(max_count.checked_mul(4).ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed exact-postings declared charge overflows",
                )))
            })?)
            .ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed exact-postings declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let refs = decode_exact_postings(bytes).map_err(MetadataCacheError::from_io)?;
            if refs.iter().any(|series_ref| *series_ref >= num_series) {
                return Err(MetadataCacheError::from_io(invalid_data(
                    "exact postings reference exceeds the bound series count",
                )));
            }
            let value = ValidatedExactPostings {
                root: Schema6IndexRootV7 {
                    layout: root_context,
                },
                offset,
                length,
                num_series,
                refs,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root.layout != root_context || value.offset != offset || value.length != length {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        if value.num_series != num_series {
            return Err(Schema6IndexReaderError::ForeignSeriesCountBinding {
                cached_num_series: value.num_series,
                requested_num_series: num_series,
            });
        }
        Ok(GovernedSchema6ExactPostings {
            provenance: self.guard.provenance(),
            root: Schema6IndexRootV7 {
                layout: root_context,
            },
            offset,
            length,
            num_series,
            value,
        })
    }

    pub(crate) fn postings<'a>(
        &'a self,
        values: &'a GovernedSchema6ExactPostings,
    ) -> Result<&'a [u32], Schema6IndexReaderError> {
        self.ensure_provenance(&values.provenance)?;
        if values.value.root != values.root
            || values.value.offset != values.offset
            || values.value.length != values.length
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        if values.value.num_series != values.num_series {
            return Err(Schema6IndexReaderError::ForeignSeriesCountBinding {
                cached_num_series: values.value.num_series,
                requested_num_series: values.num_series,
            });
        }
        Ok(&values.value.refs)
    }

    fn load_exact_directory(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
    ) -> Result<MetadataCachePin<ValidatedExactDirectory>, Schema6IndexReaderError> {
        self.ensure_bound_root(bound)?;
        let root = &bound.root;
        let root_context = root.value.layout;
        let symbol_count = bound.symbol_count;
        let locator = root_context.exact_directory;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            locator.offset,
            locator.len,
            MetadataCacheClass::IndexDirectory,
        )?;
        let declared = (std::mem::size_of::<ValidatedExactDirectory>() as u64)
            .checked_add(
                u64::from(root_context.exact_page_count)
                    .checked_mul(std::mem::size_of::<ExactPageDescriptor>() as u64)
                    .ok_or_else(|| {
                        Schema6IndexReaderError::Cache(reader.record_validation_error(
                            invalid_data("governed exact-directory declared charge overflows"),
                        ))
                    })?,
            )
            .ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed exact-directory declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let directory = decode_exact_directory(bytes, root_context, Some(symbol_count))
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedExactDirectory {
                root: Schema6IndexRootV7 {
                    layout: root_context,
                },
                symbol_count,
                value: directory,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root.layout != root_context || value.symbol_count != symbol_count {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(value)
    }

    fn load_exact_page(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
        page_index: usize,
        descriptor: ExactPageDescriptor,
    ) -> Result<MetadataCachePin<ValidatedExactPageOwned>, Schema6IndexReaderError> {
        self.ensure_bound_root(bound)?;
        let root = &bound.root;
        let root_context = root.value.layout;
        let symbol_count = bound.symbol_count;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let relative_offset =
            u64::try_from(page_index)
                .ok()
                .and_then(|index| index.checked_mul(EXACT_PAGE_LEN as u64))
                .ok_or_else(|| {
                    Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                        "governed exact-page offset overflows",
                    )))
                })?;
        let offset =
            root_context
                .exact_pages
                .offset
                .checked_add(relative_offset)
                .ok_or_else(|| {
                    Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                        "governed exact-page offset overflows",
                    )))
                })?;
        let key = metadata_key(
            &reader,
            offset,
            EXACT_PAGE_LEN as u64,
            MetadataCacheClass::IndexPage,
        )?;
        let declared = (std::mem::size_of::<ValidatedExactPageOwned>() as u64)
            .checked_add(EXACT_PAGE_LEN as u64)
            .ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed exact-page declared charge overflows",
                )))
            })?;
        let page_index_u32 = u32::try_from(page_index).map_err(|_| {
            Schema6IndexReaderError::Cache(
                reader
                    .record_validation_error(invalid_data("governed exact-page index exceeds u32")),
            )
        })?;
        let value = reader.get_or_load_owned(key, declared, move |bytes| {
            validate_exact_page(
                &bytes,
                page_index,
                descriptor,
                root_context,
                Some(symbol_count),
            )
            .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedExactPageOwned {
                root: Schema6IndexRootV7 {
                    layout: root_context,
                },
                symbol_count,
                page_index: page_index_u32,
                descriptor,
                bytes,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root.layout != root_context
            || value.symbol_count != symbol_count
            || value.page_index != page_index_u32
            || value.descriptor != descriptor
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(value)
    }

    fn ensure_bound_root(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
    ) -> Result<(), Schema6IndexReaderError> {
        self.ensure_provenance(&bound.provenance)?;
        self.ensure_provenance(&bound.root.provenance)
    }

    fn ensure_provenance(
        &self,
        provenance: &SegmentGenerationProvenance,
    ) -> Result<(), Schema6IndexReaderError> {
        if !provenance.matches(&self.guard) {
            return Err(Schema6IndexReaderError::ForeignSegmentGeneration);
        }
        self.guard
            .reader(SegmentFile::Indexes)?
            .check_recorded_error()?;
        Ok(())
    }
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
