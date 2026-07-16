use std::io;

use fst::{IntoStreamer, Set, Streamer};

use crate::storage::index::LabelValueTimeRange;
use crate::storage::metadata_cache::{LoadedMetadata, MetadataCacheError, MetadataCachePin};
use crate::storage::metadata_governor::MetadataCacheClass;
use crate::storage::segment::SegmentFile;
use crate::storage::symbols::{GovernedSymbolReaderError, GovernedSymbolSession};

use super::super::codec::{
    decode_auxiliary_directory, decode_auxiliary_fst, decode_auxiliary_time_ranges,
};
use super::super::{
    AuxiliaryDirectory, AuxiliaryRecord, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
};
use super::*;

#[derive(Debug)]
struct ValidatedAuxiliaryDirectory {
    root: Schema7IndexRootV8,
    value: AuxiliaryDirectory,
}

impl ValidatedAuxiliaryDirectory {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<AuxiliaryRecord>(
                self.value.records.capacity(),
                "schema-7 auxiliary-directory charge overflows",
            )?)
            .ok_or_else(|| invalid_data("schema-7 auxiliary-directory charge overflows"))
    }
}

#[derive(Debug)]
struct ValidatedLabelValueFst {
    root: Schema7IndexRootV8,
    record: AuxiliaryRecord,
    bytes: Vec<u8>,
}

impl ValidatedLabelValueFst {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<u8>(
                self.bytes.capacity(),
                "schema-7 label-value FST charge overflows",
            )?)
            .ok_or_else(|| invalid_data("schema-7 label-value FST charge overflows"))
    }
}

#[derive(Debug)]
struct ValidatedLabelValueTimeRanges {
    root: Schema7IndexRootV8,
    record: AuxiliaryRecord,
    ranges: Vec<(u32, LabelValueTimeRange)>,
}

impl ValidatedLabelValueTimeRanges {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<(u32, LabelValueTimeRange)>(
                self.ranges.capacity(),
                "schema-7 label-value time-range charge overflows",
            )?)
            .ok_or_else(|| invalid_data("schema-7 label-value time-range charge overflows"))
    }
}

/// Query-local pin for the complete root-authenticated auxiliary directory.
#[derive(Debug)]
pub(crate) struct GovernedSchema7AuxiliaryDirectory {
    provenance: SegmentGenerationProvenance,
    root: Schema7IndexRootV8,
    value: MetadataCachePin<ValidatedAuxiliaryDirectory>,
}

/// Opaque protected FST payload. The record and root cannot be detached.
#[derive(Debug)]
pub(crate) struct GovernedSchema7LabelValueFst {
    provenance: SegmentGenerationProvenance,
    root: Schema7IndexRootV8,
    record: AuxiliaryRecord,
    value: MetadataCachePin<ValidatedLabelValueFst>,
}

impl GovernedSchema7LabelValueFst {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    pub(super) fn substitute_record_for_test(&mut self) {
        self.record.label_name_sym ^= 1;
    }
}

/// Opaque protected range payload. The returned slice remains pinned.
#[derive(Debug)]
pub(crate) struct GovernedSchema7LabelValueTimeRanges {
    provenance: SegmentGenerationProvenance,
    root: Schema7IndexRootV8,
    record: AuxiliaryRecord,
    value: MetadataCachePin<ValidatedLabelValueTimeRanges>,
}

impl GovernedSchema7LabelValueTimeRanges {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    pub(super) fn substitute_record_for_test(&mut self) {
        self.record.label_name_sym ^= 1;
    }
}

impl GovernedSchema7AuxiliaryDirectory {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    pub(super) fn substitute_root_for_test(&mut self) {
        self.root.layout.auxiliary_entry_count ^= 1;
    }
}

impl GovernedSchema7IndexSession {
    pub(crate) fn load_auxiliary_directory(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
    ) -> Result<GovernedSchema7AuxiliaryDirectory, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        let root_context = root.value;
        let locator = root_context.layout.auxiliary_directory;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            locator.offset,
            locator.len,
            MetadataCacheClass::IndexDirectory,
        )?;
        let declared = (std::mem::size_of::<ValidatedAuxiliaryDirectory>() as u64)
            .checked_add(
                u64::from(root_context.layout.auxiliary_entry_count)
                    .checked_mul(std::mem::size_of::<AuxiliaryRecord>() as u64)
                    .ok_or_else(|| {
                        Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                            "schema-7 auxiliary-directory declared charge overflows",
                        )))
                    })?,
            )
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 auxiliary-directory declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let directory = decode_auxiliary_directory(bytes, root_context.layout)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedAuxiliaryDirectory {
                root: root_context,
                value: directory,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context {
            return Err(self.record_cached_context_error(
                "cached schema-7 auxiliary directory has a foreign root",
            ));
        }
        Ok(GovernedSchema7AuxiliaryDirectory {
            provenance: self.guard.provenance(),
            root: root_context,
            value,
        })
    }

    pub(crate) fn has_label_values(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        directory: &GovernedSchema7AuxiliaryDirectory,
    ) -> Result<bool, Schema7IndexReaderError> {
        let directory = self.auxiliary_directory_value(root, directory)?;
        Ok(directory
            .value
            .records
            .iter()
            .any(|record| record.kind == SEGMENT_INDEX_BLOB_LABEL_VALUE_FST))
    }

    pub(crate) fn label_name_symbols<'a>(
        &'a self,
        root: &GovernedSchema7BoundIndexRoot,
        directory: &'a GovernedSchema7AuxiliaryDirectory,
    ) -> Result<impl ExactSizeIterator<Item = u32> + 'a, Schema7IndexReaderError> {
        let directory = self.auxiliary_directory_value(root, directory)?;
        let fst_count = directory
            .value
            .records
            .partition_point(|record| record.kind == SEGMENT_INDEX_BLOB_LABEL_VALUE_FST);
        Ok(directory.value.records[..fst_count]
            .iter()
            .map(|record| record.label_name_sym))
    }

    pub(crate) fn label_time_range(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        directory: &GovernedSchema7AuxiliaryDirectory,
        label_name_sym: u32,
    ) -> Result<Option<LabelValueTimeRange>, Schema7IndexReaderError> {
        let directory = self.auxiliary_directory_value(root, directory)?;
        Ok(directory
            .value
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, label_name_sym)
            .map(|record| record.time_range))
    }

    pub(crate) fn load_label_value_fst(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        directory: &GovernedSchema7AuxiliaryDirectory,
        label_name_sym: u32,
    ) -> Result<Option<GovernedSchema7LabelValueFst>, Schema7IndexReaderError> {
        let directory_value = self.auxiliary_directory_value(root, directory)?;
        if label_name_sym >= root.value.layout.counts.symbols {
            return Ok(None);
        }
        let Some(record) = directory_value
            .value
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, label_name_sym)
        else {
            return Ok(None);
        };
        let root_context = root.value;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            record.payload.offset,
            record.payload.len,
            MetadataCacheClass::IndexPage,
        )?;
        let declared = (std::mem::size_of::<ValidatedLabelValueFst>() as u64)
            .checked_add(record.payload.len)
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 label-value FST declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load_owned(key, declared, move |bytes| {
            decode_auxiliary_fst(&bytes, record, root_context.layout)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedLabelValueFst {
                root: root_context,
                record,
                bytes,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context || value.record != record {
            return Err(self.record_cached_context_error(
                "cached schema-7 label-value FST has foreign root or protected record context",
            ));
        }
        Ok(Some(GovernedSchema7LabelValueFst {
            provenance: self.guard.provenance(),
            root: root_context,
            record,
            value,
        }))
    }

    /// Visits FST values without an unbounded intermediate allocation.
    /// Every emitted string must first resolve through the bound symbol session.
    pub(crate) fn visit_label_values_with_prefix(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        values: &GovernedSchema7LabelValueFst,
        symbols: &GovernedSymbolSession,
        prefix: Option<&str>,
        mut visitor: impl FnMut(u32, &str) -> bool,
    ) -> Result<bool, Schema7IndexReaderError> {
        self.ensure_fst(root, values)?;
        symbols
            .ensure_same_generation(&self.guard)
            .map_err(map_symbol_generation_error)?;
        let set = Set::new(values.value.bytes.as_slice()).map_err(|error| {
            self.record_cached_context_error_owned(format!(
                "cached schema-7 label-value FST became invalid: {error}"
            ))
        })?;
        let prefix = prefix.filter(|prefix| !prefix.is_empty());
        let mut stream = match prefix {
            Some(prefix) => set.range().ge(prefix).into_stream(),
            None => set.stream(),
        };
        while let Some(value) = stream.next() {
            if prefix.is_some_and(|prefix| !value.starts_with(prefix.as_bytes())) {
                break;
            }
            let value = std::str::from_utf8(value).map_err(|error| {
                self.record_cached_context_error_owned(format!(
                    "cached schema-7 FST emitted invalid UTF-8: {error}"
                ))
            })?;
            let symbol_id = match symbols.lookup(value) {
                Ok(Some(symbol_id)) => symbol_id,
                Ok(None) => {
                    return Err(self.record_cached_context_error_owned(format!(
                        "schema-7 FST value {value:?} does not resolve through the bound symbol root"
                    )));
                }
                Err(error) => return Err(Schema7IndexReaderError::Symbols(error)),
            };
            if !visitor(symbol_id, value) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn load_label_value_time_ranges(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        directory: &GovernedSchema7AuxiliaryDirectory,
        label_name_sym: u32,
    ) -> Result<Option<GovernedSchema7LabelValueTimeRanges>, Schema7IndexReaderError> {
        let directory_value = self.auxiliary_directory_value(root, directory)?;
        if label_name_sym >= root.value.layout.counts.symbols {
            return Ok(None);
        }
        let Some(record) = directory_value
            .value
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, label_name_sym)
        else {
            return Ok(None);
        };
        let root_context = root.value;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            record.payload.offset,
            record.payload.len,
            MetadataCacheClass::IndexPage,
        )?;
        let declared = (std::mem::size_of::<ValidatedLabelValueTimeRanges>() as u64)
            .checked_add(
                u64::from(record.item_count)
                    .checked_mul(std::mem::size_of::<(u32, LabelValueTimeRange)>() as u64)
                    .ok_or_else(|| {
                        Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                            "schema-7 label-value time-range declared charge overflows",
                        )))
                    })?,
            )
            .ok_or_else(|| {
                Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(
                    "schema-7 label-value time-range declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let ranges = decode_auxiliary_time_ranges(bytes, record, root_context.layout)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedLabelValueTimeRanges {
                root: root_context,
                record,
                ranges,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context || value.record != record {
            return Err(self.record_cached_context_error(
                "cached schema-7 label-value ranges have foreign root or protected record context",
            ));
        }
        Ok(Some(GovernedSchema7LabelValueTimeRanges {
            provenance: self.guard.provenance(),
            root: root_context,
            record,
            value,
        }))
    }

    pub(crate) fn label_value_time_ranges<'a>(
        &'a self,
        root: &GovernedSchema7BoundIndexRoot,
        ranges: &'a GovernedSchema7LabelValueTimeRanges,
    ) -> Result<&'a [(u32, LabelValueTimeRange)], Schema7IndexReaderError> {
        self.ensure_ranges(root, ranges)?;
        Ok(&ranges.value.ranges)
    }

    pub(crate) fn label_value_time_range(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        ranges: &GovernedSchema7LabelValueTimeRanges,
        label_value_sym: u32,
    ) -> Result<Option<LabelValueTimeRange>, Schema7IndexReaderError> {
        let ranges = self.label_value_time_ranges(root, ranges)?;
        Ok(ranges
            .binary_search_by_key(&label_value_sym, |(value_sym, _)| *value_sym)
            .ok()
            .map(|index| ranges[index].1))
    }

    /// Resolves a value emitted by an existing paired authenticated FST/range
    /// inventory. Equal directory counts are not sufficient authority for
    /// pruning: a missing emitted value is sticky index corruption. A wholly
    /// absent kind-3 record is different: v8 defines that FST as canonically
    /// unconstrained and callers must conservatively emit it.
    pub(crate) fn required_label_value_time_range(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        ranges: &GovernedSchema7LabelValueTimeRanges,
        label_value_sym: u32,
    ) -> Result<LabelValueTimeRange, Schema7IndexReaderError> {
        self.label_value_time_range(root, ranges, label_value_sym)?
            .ok_or_else(|| {
                self.record_cached_context_error_owned(format!(
                    "schema-7 FST value symbol {label_value_sym} has no paired authenticated time range"
                ))
            })
    }

    fn auxiliary_directory_value<'a>(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        directory: &'a GovernedSchema7AuxiliaryDirectory,
    ) -> Result<&'a ValidatedAuxiliaryDirectory, Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        self.ensure_provenance(&directory.provenance)?;
        if directory.root != root.value {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        if directory.value.root != directory.root {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        Ok(&directory.value)
    }

    fn ensure_fst(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        values: &GovernedSchema7LabelValueFst,
    ) -> Result<(), Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        self.ensure_provenance(&values.provenance)?;
        if values.root != root.value {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        if values.value.root != values.root || values.value.record != values.record {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        Ok(())
    }

    fn ensure_ranges(
        &self,
        root: &GovernedSchema7BoundIndexRoot,
        ranges: &GovernedSchema7LabelValueTimeRanges,
    ) -> Result<(), Schema7IndexReaderError> {
        self.ensure_bound_root(root)?;
        self.ensure_provenance(&ranges.provenance)?;
        if ranges.root != root.value {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        if ranges.value.root != ranges.root || ranges.value.record != ranges.record {
            return Err(Schema7IndexReaderError::ForeignRootContext);
        }
        Ok(())
    }

    fn record_cached_context_error_owned(&self, message: String) -> Schema7IndexReaderError {
        Schema7IndexReaderError::Cache(self.record_index_error(invalid_data(message)))
    }
}

fn map_symbol_generation_error(error: GovernedSymbolReaderError) -> Schema7IndexReaderError {
    match error {
        GovernedSymbolReaderError::ForeignSegmentGeneration => {
            Schema7IndexReaderError::ForeignSegmentGeneration
        }
        error => Schema7IndexReaderError::Symbols(error),
    }
}
