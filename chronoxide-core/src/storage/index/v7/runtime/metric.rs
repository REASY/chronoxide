use crate::storage::index::MetricSeriesRange;

use super::super::codec::{
    MetricSeriesRangeDirectory, MetricSeriesRangeGroupDescriptor,
    decode_metric_series_range_directory, visit_metric_series_ranges,
};
use super::*;

#[derive(Debug)]
struct StructurallyValidatedMetricSeriesRanges {
    root: Schema6IndexRootV7,
    num_series: u32,
    symbol_count: u32,
    bytes: Vec<u8>,
    directory: MetricSeriesRangeDirectory,
}

impl StructurallyValidatedMetricSeriesRanges {
    fn charged_bytes(&self) -> io::Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(checked_vec_capacity_bytes::<u8>(
                self.bytes.capacity(),
                "governed metric-series range byte charge overflows",
            )?)
            .and_then(|charged| {
                checked_vec_capacity_bytes::<MetricSeriesRangeGroupDescriptor>(
                    self.directory.groups.capacity(),
                    "governed metric-series range directory charge overflows",
                )
                .ok()
                .and_then(|directory| charged.checked_add(directory))
            })
            .ok_or_else(|| invalid_data("governed metric-series range charge overflows"))
    }
}

/// Query-local structurally validated metric-series range blob. The raw bytes
/// and compact group directory remain pinned to one segment generation, index
/// root, and authoritative count bindings.
///
/// This pin does not authenticate metric ownership or time/kind summaries and
/// therefore is not, by itself, authority for candidate exclusion.
#[derive(Debug)]
pub(crate) struct GovernedSchema6MetricSeriesRanges {
    provenance: SegmentGenerationProvenance,
    root: Schema6IndexRootV7,
    num_series: u32,
    symbol_count: u32,
    value: MetadataCachePin<StructurallyValidatedMetricSeriesRanges>,
}

impl GovernedSchema6MetricSeriesRanges {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.value.charged_bytes()
    }

    #[cfg(test)]
    pub(super) fn substitute_context_for_test(&mut self) {
        self.root.layout.metric.len ^= 1;
        self.num_series ^= 1;
        self.symbol_count ^= 1;
    }
}

impl GovernedSchema6IndexSession {
    pub(crate) fn load_metric_series_ranges(
        &self,
        bound: &GovernedSchema6BoundIndexRoot,
    ) -> Result<GovernedSchema6MetricSeriesRanges, Schema6IndexReaderError> {
        self.ensure_provenance(&bound.provenance)?;
        let root = &bound.root;
        self.ensure_provenance(&root.provenance)?;
        let root_context = *root.value;
        let num_series = bound.num_series;
        let symbol_count = bound.symbol_count;
        let locator = root_context.layout.metric;
        let reader = self.guard.reader(SegmentFile::Indexes)?;
        let key = metadata_key(
            &reader,
            locator.offset,
            locator.len,
            MetadataCacheClass::MetricRange,
        )?;
        let max_group_count = locator.len.saturating_sub(12) / 36;
        let declared = (std::mem::size_of::<StructurallyValidatedMetricSeriesRanges>() as u64)
            .checked_add(locator.len)
            .and_then(|charged| {
                max_group_count
                    .checked_mul(std::mem::size_of::<MetricSeriesRangeGroupDescriptor>() as u64)
                    .and_then(|directory| charged.checked_add(directory))
            })
            .ok_or_else(|| {
                Schema6IndexReaderError::Cache(reader.record_validation_error(invalid_data(
                    "governed metric-series range declared charge overflows",
                )))
            })?;
        let value = reader.get_or_load_owned(key, declared, move |bytes| {
            let directory = decode_metric_series_range_directory(&bytes, num_series, symbol_count)
                .map_err(MetadataCacheError::from_io)?;
            let value = StructurallyValidatedMetricSeriesRanges {
                root: root_context,
                num_series,
                symbol_count,
                bytes,
                directory,
            };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        if value.root != root_context {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        if value.num_series != num_series {
            return Err(Schema6IndexReaderError::ForeignSeriesCountBinding {
                cached_num_series: value.num_series,
                requested_num_series: num_series,
            });
        }
        if value.symbol_count != symbol_count {
            return Err(Schema6IndexReaderError::ForeignSymbolCountBinding {
                cached_symbol_count: value.symbol_count,
                requested_symbol_count: symbol_count,
            });
        }
        Ok(GovernedSchema6MetricSeriesRanges {
            provenance: self.guard.provenance(),
            root: root_context,
            num_series,
            symbol_count,
            value,
        })
    }

    /// Visits only the requested metric's ranges without materializing a
    /// metric-specific vector. `Ok(false)` means the visitor stopped early.
    pub(crate) fn visit_metric_series_ranges(
        &self,
        ranges: &GovernedSchema6MetricSeriesRanges,
        metric_sym: u32,
        visitor: impl FnMut(MetricSeriesRange) -> bool,
    ) -> Result<bool, Schema6IndexReaderError> {
        self.ensure_provenance(&ranges.provenance)?;
        if ranges.value.root != ranges.root {
            return Err(Schema6IndexReaderError::ForeignRootContext);
        }
        if ranges.value.num_series != ranges.num_series {
            return Err(Schema6IndexReaderError::ForeignSeriesCountBinding {
                cached_num_series: ranges.value.num_series,
                requested_num_series: ranges.num_series,
            });
        }
        if ranges.value.symbol_count != ranges.symbol_count {
            return Err(Schema6IndexReaderError::ForeignSymbolCountBinding {
                cached_symbol_count: ranges.value.symbol_count,
                requested_symbol_count: ranges.symbol_count,
            });
        }
        match visit_metric_series_ranges(
            &ranges.value.bytes,
            &ranges.value.directory,
            metric_sym,
            visitor,
        ) {
            Ok(exhausted) => Ok(exhausted),
            Err(error) => {
                let reader = self.guard.reader(SegmentFile::Indexes)?;
                Err(Schema6IndexReaderError::Cache(
                    reader.record_validation_error(error),
                ))
            }
        }
    }
}
