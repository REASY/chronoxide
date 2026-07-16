use crate::storage::index::{
    LabelValueTimeRange, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
};
use crate::storage::symbols::{GovernedSymbolReaderError, GovernedSymbolSession};

use super::super::codec::{
    AuxiliaryDirectory, AuxiliaryRecord, decode_auxiliary_directory,
    decode_label_value_time_ranges, validate_label_value_fst, visit_label_value_fst,
};
use super::*;

#[derive(Debug)]
struct ValidatedAuxiliaryDirectory {
    root: Schema6IndexRootV7,
    symbol_count: u32,
    value: AuxiliaryDirectory,
}

impl ValidatedAuxiliaryDirectory {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(
                u64::try_from(self.value.records.len())
                    .ok()
                    .and_then(|count| {
                        count.checked_mul(std::mem::size_of::<AuxiliaryRecord>() as u64)
                    })
                    .ok_or_else(|| invalid_data("governed auxiliary-directory charge overflows"))?,
            )
            .ok_or_else(|| invalid_data("governed auxiliary-directory charge overflows"))
    }
}

#[derive(Debug)]
struct ValidatedLabelValueFst {
    root: Schema6IndexRootV7,
    symbol_count: u32,
    record: AuxiliaryRecord,
    bytes: Vec<u8>,
}

impl ValidatedLabelValueFst {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<u8>(
                self.bytes.capacity(),
                "governed label-value FST charge overflows",
            )?)
            .ok_or_else(|| invalid_data("governed label-value FST charge overflows"))
    }
}

#[derive(Debug)]
struct ValidatedLabelValueTimeRanges {
    root: Schema6IndexRootV7,
    symbol_count: u32,
    record: AuxiliaryRecord,
    ranges: Vec<(u32, LabelValueTimeRange)>,
}

impl ValidatedLabelValueTimeRanges {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<(u32, LabelValueTimeRange)>(
                self.ranges.capacity(),
                "governed label-value time-range charge overflows",
            )?)
            .ok_or_else(|| invalid_data("governed label-value time-range charge overflows"))
    }
}

/// Query-local pin for the complete validated auxiliary directory. Payload
/// locators remain private and cannot be detached from this generation/root
/// context.
#[derive(Debug)]
pub(crate) struct GovernedSchema6AuxiliaryDirectory {
    provenance: SegmentGenerationProvenance,
    root: Schema6IndexRootV7,
    symbol_count: u32,
    value: MetadataCachePin<ValidatedAuxiliaryDirectory>,
}

/// Query-local validated FST bytes. Values are exposed only through the
/// stop-capable visitor API and the payload locator remains private.
#[derive(Debug)]
pub(crate) struct GovernedSchema6LabelValueFst {
    provenance: SegmentGenerationProvenance,
    root: Schema6IndexRootV7,
    symbol_count: u32,
    record: AuxiliaryRecord,
    value: MetadataCachePin<ValidatedLabelValueFst>,
}

impl GovernedSchema6LabelValueFst {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    pub(super) fn substitute_record_for_test(&mut self) {
        self.record.label_name_sym ^= 1;
    }
}

/// Query-local decoded label-value time ranges. The borrowed slice cannot
/// outlive its generation-bound cache pin.
#[derive(Debug)]
pub(crate) struct GovernedSchema6LabelValueTimeRanges {
    provenance: SegmentGenerationProvenance,
    root: Schema6IndexRootV7,
    symbol_count: u32,
    record: AuxiliaryRecord,
    value: MetadataCachePin<ValidatedLabelValueTimeRanges>,
}

impl GovernedSchema6LabelValueTimeRanges {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    pub(super) fn substitute_record_for_test(&mut self) {
        self.record.label_name_sym ^= 1;
    }
}

impl GovernedSchema6AuxiliaryDirectory {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    pub(super) fn substitute_root_for_test(&mut self) {
        self.root.layout.auxiliary_entry_count ^= 1;
        self.symbol_count ^= 1;
    }
}

impl GovernedSchema6IndexSession {
    pub(crate) fn load_auxiliary_directory(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
    ) -> Result<GovernedSchema6AuxiliaryDirectory, Schema6IndexReaderError> {
        self.ensure_bound_root(bound)?;
        let root = &bound.root;
        let root_context = *root.value;
        let symbol_count = bound.symbol_count;
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
                        Schema6IndexReaderError::Cache(reader.record_validation_error(
                            invalid_data("governed auxiliary-directory declared charge overflows"),
                        ))
                    })?,
            )
            .ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed auxiliary-directory declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let directory =
                decode_auxiliary_directory(bytes, root_context.layout, Some(symbol_count))
                    .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedAuxiliaryDirectory {
                root: root_context,
                symbol_count,
                value: directory,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context || value.symbol_count != symbol_count {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(GovernedSchema6AuxiliaryDirectory {
            provenance: self.guard.provenance(),
            root: root_context,
            symbol_count,
            value,
        })
    }

    pub(crate) fn has_label_values(
        &self,
        directory: &GovernedSchema6AuxiliaryDirectory,
    ) -> Result<bool, Schema6IndexReaderError> {
        let value = self.auxiliary_directory_value(directory)?;
        Ok(value.value.fst_count != 0)
    }

    pub(crate) fn label_name_symbols<'a>(
        &'a self,
        directory: &'a GovernedSchema6AuxiliaryDirectory,
    ) -> Result<impl ExactSizeIterator<Item = u32> + 'a, Schema6IndexReaderError> {
        let value = self.auxiliary_directory_value(directory)?;
        Ok(value.value.records[..value.value.fst_count]
            .iter()
            .map(|record| record.label_name_sym))
    }

    /// Returns only an advisory v7 summary. It is not authoritative for
    /// candidate removal or time pruning.
    pub(crate) fn label_time_range(
        &self,
        directory: &GovernedSchema6AuxiliaryDirectory,
        label_name_sym: u32,
    ) -> Result<Option<LabelValueTimeRange>, Schema6IndexReaderError> {
        let value = self.auxiliary_directory_value(directory)?;
        Ok(value
            .value
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, label_name_sym)
            .map(|record| record.time_range))
    }

    pub(crate) fn load_label_value_fst(
        &self,
        directory: &GovernedSchema6AuxiliaryDirectory,
        label_name_sym: u32,
    ) -> Result<Option<GovernedSchema6LabelValueFst>, Schema6IndexReaderError> {
        let directory_value = self.auxiliary_directory_value(directory)?;
        let Some(record) = directory_value
            .value
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, label_name_sym)
        else {
            return Ok(None);
        };
        let root_context = directory.root;
        let symbol_count = directory.symbol_count;
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
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed label-value FST declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load_owned(key, declared, move |bytes| {
            validate_label_value_fst(&bytes).map_err(MetadataCacheError::from_io)?;
            let value = ValidatedLabelValueFst {
                root: root_context,
                symbol_count,
                record,
                bytes,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context
            || value.symbol_count != symbol_count
            || value.record != record
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(Some(GovernedSchema6LabelValueFst {
            provenance: self.guard.provenance(),
            root: root_context,
            symbol_count,
            record,
            value,
        }))
    }

    /// Visits values without creating an unbounded intermediate vector.
    /// Every emitted FST value is resolved through the same-generation symbol
    /// session before it is exposed. Returning `false` from the visitor stops
    /// enumeration; this method then returns `Ok(false)`. `Ok(true)` means the
    /// selected prefix was exhausted.
    pub(crate) fn visit_label_values_with_prefix(
        &self,
        values: &GovernedSchema6LabelValueFst,
        symbols: &GovernedSymbolSession,
        prefix: Option<&str>,
        mut visitor: impl FnMut(u32, &str) -> bool,
    ) -> Result<bool, Schema6IndexReaderError> {
        symbols
            .ensure_same_generation(&self.guard)
            .map_err(map_symbol_generation_error)?;
        self.ensure_provenance(&values.provenance)?;
        if values.value.root != values.root
            || values.value.symbol_count != values.symbol_count
            || values.value.record != values.record
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }

        enum ResolutionFailure {
            Unresolved,
            Symbols(GovernedSymbolReaderError),
        }

        let mut resolution_failure = None;
        let result = visit_label_value_fst(&values.value.bytes, prefix, |value| {
            let symbol_id = match symbols.lookup(value) {
                Ok(Some(symbol_id)) => symbol_id,
                Ok(None) => {
                    resolution_failure = Some(ResolutionFailure::Unresolved);
                    return false;
                }
                Err(error) => {
                    resolution_failure = Some(ResolutionFailure::Symbols(error));
                    return false;
                }
            };
            visitor(symbol_id, value)
        });
        if let Some(failure) = resolution_failure {
            return match failure {
                ResolutionFailure::Unresolved => {
                    let reader = self.guard.reader(SegmentFile::Indexes)?;
                    Err(Schema6IndexReaderError::Cache(
                        reader.record_validation_error(invalid_data(
                            "schema-6 FST value does not resolve through the bound symbol root",
                        )),
                    ))
                }
                ResolutionFailure::Symbols(error) => Err(Schema6IndexReaderError::Symbols(error)),
            };
        }
        match result {
            Ok(exhausted) => Ok(exhausted),
            Err(error) => {
                let reader = self.guard.reader(SegmentFile::Indexes)?;
                Err(Schema6IndexReaderError::Cache(
                    reader.record_validation_error(error),
                ))
            }
        }
    }

    pub(crate) fn load_label_value_time_ranges(
        &self,
        directory: &GovernedSchema6AuxiliaryDirectory,
        label_name_sym: u32,
    ) -> Result<Option<GovernedSchema6LabelValueTimeRanges>, Schema6IndexReaderError> {
        let directory_value = self.auxiliary_directory_value(directory)?;
        let Some(record) = directory_value
            .value
            .record(SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES, label_name_sym)
        else {
            return Ok(None);
        };
        let root_context = directory.root;
        let symbol_count = directory.symbol_count;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            record.payload.offset,
            record.payload.len,
            MetadataCacheClass::IndexPage,
        )?;
        let max_count = record.payload.len.saturating_sub(4) / 20;
        let declared = (std::mem::size_of::<ValidatedLabelValueTimeRanges>() as u64)
            .checked_add(
                max_count
                    .checked_mul(std::mem::size_of::<(u32, LabelValueTimeRange)>() as u64)
                    .ok_or_else(|| {
                        Schema6IndexReaderError::Cache(reader.record_validation_error(
                            invalid_data(
                                "governed label-value time-range declared charge overflows",
                            ),
                        ))
                    })?,
            )
            .ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed label-value time-range declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load(key, declared, move |bytes| {
            let ranges =
                decode_label_value_time_ranges(bytes, record.time_range, Some(symbol_count))
                    .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedLabelValueTimeRanges {
                root: root_context,
                symbol_count,
                record,
                ranges,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context
            || value.symbol_count != symbol_count
            || value.record != record
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(Some(GovernedSchema6LabelValueTimeRanges {
            provenance: self.guard.provenance(),
            root: root_context,
            symbol_count,
            record,
            value,
        }))
    }

    /// Returns only advisory v7 summaries. Callers must retain complete final
    /// label-predicate verification and may not prune candidates from them.
    pub(crate) fn label_value_time_ranges<'a>(
        &'a self,
        ranges: &'a GovernedSchema6LabelValueTimeRanges,
    ) -> Result<&'a [(u32, LabelValueTimeRange)], Schema6IndexReaderError> {
        self.ensure_provenance(&ranges.provenance)?;
        if ranges.value.root != ranges.root
            || ranges.value.symbol_count != ranges.symbol_count
            || ranges.value.record != ranges.record
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(&ranges.value.ranges)
    }

    pub(crate) fn label_value_time_range(
        &self,
        ranges: &GovernedSchema6LabelValueTimeRanges,
        label_value_sym: u32,
    ) -> Result<Option<LabelValueTimeRange>, Schema6IndexReaderError> {
        let ranges = self.label_value_time_ranges(ranges)?;
        Ok(ranges
            .binary_search_by_key(&label_value_sym, |(value_sym, _)| *value_sym)
            .ok()
            .map(|index| ranges[index].1))
    }

    fn auxiliary_directory_value<'a>(
        &self,
        directory: &'a GovernedSchema6AuxiliaryDirectory,
    ) -> Result<&'a ValidatedAuxiliaryDirectory, Schema6IndexReaderError> {
        self.ensure_provenance(&directory.provenance)?;
        if directory.value.root != directory.root
            || directory.value.symbol_count != directory.symbol_count
        {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        Ok(&directory.value)
    }
}

fn map_symbol_generation_error(error: GovernedSymbolReaderError) -> Schema6IndexReaderError {
    match error {
        GovernedSymbolReaderError::ForeignSegmentGeneration => {
            Schema6IndexReaderError::ForeignSegmentGeneration
        }
        error => Schema6IndexReaderError::Symbols(error),
    }
}
