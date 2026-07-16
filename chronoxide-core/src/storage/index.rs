use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::io::SeekFrom;
use std::io::{self, Read, Seek, Write};

use fst::{IntoStreamer, Set, SetBuilder, Streamer};

use crate::labels::METRIC_NAME_LABEL;
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SERIES_KIND_INT64,
    SERIES_KIND_SUMMARY, SegmentSymbols, SeriesEntry,
};

mod read_at;
#[doc(hidden)]
pub use read_at::SegmentIndexReadAt;

mod v7;
pub(super) use v7::runtime::{
    GovernedSchema6BoundIndexRoot, GovernedSchema6ExactPostings,
    GovernedSchema6ExactPostingsSelection, GovernedSchema6IndexReader, GovernedSchema6IndexSession,
    Schema6IndexReaderError,
};
#[allow(dead_code)] // Made reachable only through the schema-7 same-seal validator.
mod v8;
pub(super) use v8::runtime::{
    GovernedSchema7BoundIndexRoot, GovernedSchema7ExactPostings,
    GovernedSchema7ExactPostingsSelection, GovernedSchema7IndexReader, GovernedSchema7IndexSession,
    Schema7IndexReaderError,
};

const EXACT_POSTINGS_MAGIC: u32 = u32::from_le_bytes(*b"PIDX");
const LABEL_VALUE_FST_MAGIC: u32 = u32::from_le_bytes(*b"LVIX");
const LABEL_VALUE_TIME_RANGE_MAGIC: u32 = u32::from_le_bytes(*b"LVTR");
const METRIC_SERIES_RANGES_MAGIC: u32 = u32::from_le_bytes(*b"MSRG");
const METRIC_SERIES_RANGES_VERSION: u16 = 1;
const METRIC_SERIES_RANGE_RECORD_LEN: usize = 28;
const VALID_METRIC_SERIES_KIND_MASK: u16 = (SERIES_KIND_FLOAT
    | SERIES_KIND_INT64
    | SERIES_KIND_HISTOGRAM
    | SERIES_KIND_EXPONENTIAL_HISTOGRAM
    | SERIES_KIND_SUMMARY) as u16;
const SEGMENT_INDEXES_MAGIC: u32 = u32::from_le_bytes(*b"SIDX");
#[cfg(test)]
const SEGMENT_INDEX_FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"SIDF");
const SEGMENT_INDEX_TRAILER_MAGIC: u32 = u32::from_le_bytes(*b"SIDT");
#[cfg(test)]
const SEGMENT_INDEX_VERSION: u16 = 6;
#[cfg(test)]
const SEGMENT_INDEX_HEADER_LEN: u64 = 8;
#[cfg(test)]
const SEGMENT_INDEX_TRAILER_LEN: u64 = 12;
const ROUTING_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"RIDX");
const ROUTING_INDEX_VERSION: u16 = 2;
const ROUTING_INDEX_HEADER_LEN: usize = 40;
const ROUTING_INDEX_BUCKET_LEN: usize = 40;
#[cfg(test)]
const SEGMENT_INDEX_BLOB_EXACT_POSTINGS: u16 = 1;
const SEGMENT_INDEX_BLOB_LABEL_VALUE_FST: u16 = 2;
const SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES: u16 = 3;
#[cfg(test)]
const SEGMENT_INDEX_BLOB_ROUTING: u16 = 4;
#[cfg(test)]
const SEGMENT_INDEX_BLOB_METRIC_SERIES_RANGES: u16 = 5;
#[cfg(test)]
const NO_LABEL_VALUE_SYM: u32 = u32::MAX;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExactPostingsIndex {
    postings: BTreeMap<(u32, u32), Vec<u32>>,
}

impl ExactPostingsIndex {
    pub fn insert(&mut self, label_name_sym: u32, label_value_sym: u32, series_ref: u32) {
        let refs = self
            .postings
            .entry((label_name_sym, label_value_sym))
            .or_default();
        match refs.binary_search(&series_ref) {
            Ok(_) => {}
            Err(idx) => refs.insert(idx, series_ref),
        }
    }

    pub fn insert_monotonic(&mut self, label_name_sym: u32, label_value_sym: u32, series_ref: u32) {
        let refs = self
            .postings
            .entry((label_name_sym, label_value_sym))
            .or_default();
        match refs.last().copied() {
            Some(last) if last == series_ref => {}
            Some(last) if last < series_ref => refs.push(series_ref),
            _ => match refs.binary_search(&series_ref) {
                Ok(_) => {}
                Err(idx) => refs.insert(idx, series_ref),
            },
        }
    }

    pub fn get(&self, label_name_sym: u32, label_value_sym: u32) -> Option<&[u32]> {
        self.postings
            .get(&(label_name_sym, label_value_sym))
            .map(Vec::as_slice)
    }

    pub fn len(&self) -> usize {
        self.postings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = (u32, u32, &[u32])> {
        self.postings
            .iter()
            .map(|((name, value), refs)| (*name, *value, refs.as_slice()))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LabelValueIndex {
    values: BTreeMap<u32, Vec<u32>>,
}

impl LabelValueIndex {
    pub fn insert(&mut self, label_name_sym: u32, label_value_sym: u32) {
        let values = self.values.entry(label_name_sym).or_default();
        match values.binary_search(&label_value_sym) {
            Ok(_) => {}
            Err(idx) => values.insert(idx, label_value_sym),
        }
    }

    pub fn values(&self, label_name_sym: u32) -> &[u32] {
        self.values
            .get(&label_name_sym)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn from_series(series: &[SeriesEntry]) -> Self {
        let mut index = Self::default();
        for entry in series {
            for (name, value) in &entry.labels {
                index.insert(*name, *value);
            }
        }
        index
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LabelValueFstIndex {
    fsts: BTreeMap<u32, Vec<u8>>,
}

impl LabelValueFstIndex {
    pub fn from_series(series: &[SeriesEntry], symbols: &SegmentSymbols) -> io::Result<Self> {
        let mut values: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for entry in series {
            for (name, value) in &entry.labels {
                values.entry(*name).or_default().push(*value);
            }
        }

        let mut fsts = BTreeMap::new();
        for (name, mut values) in values {
            values.sort_unstable();
            values.dedup();
            values.sort_by(|left, right| {
                let left = symbols.resolve(*left).unwrap_or("");
                let right = symbols.resolve(*right).unwrap_or("");
                left.cmp(right)
            });

            let mut builder = SetBuilder::memory();
            for value in values {
                let value = symbols.resolve(value).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "value symbol missing")
                })?;
                builder.insert(value).map_err(fst_io_error)?;
            }
            fsts.insert(name, builder.into_inner().map_err(fst_io_error)?);
        }

        Ok(Self { fsts })
    }

    pub fn insert_fst(&mut self, label_name_sym: u32, fst_bytes: Vec<u8>) {
        self.fsts.insert(label_name_sym, fst_bytes);
    }

    pub fn values(&self, label_name_sym: u32) -> io::Result<Vec<String>> {
        let Some(bytes) = self.fsts.get(&label_name_sym) else {
            return Ok(Vec::new());
        };
        read_fst_values(bytes)
    }

    pub fn label_name_symbols(&self) -> Vec<u32> {
        self.fsts.keys().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.fsts.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LabelValueTimeRange {
    pub min_time_ms: u64,
    pub max_time_ms: u64,
}

impl LabelValueTimeRange {
    pub fn overlaps(self, start_ms: u64, end_ms: u64) -> bool {
        self.max_time_ms >= start_ms && self.min_time_ms <= end_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactPostingsMetadata {
    pub byte_len: u64,
    pub time_range: LabelValueTimeRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage) struct ExactPostingsSelection {
    metadata: ExactPostingsMetadata,
    postings_offset: u64,
    postings_len: u64,
}

impl ExactPostingsSelection {
    fn new(metadata: ExactPostingsMetadata, postings_offset: u64, postings_len: u64) -> Self {
        Self {
            metadata,
            postings_offset,
            postings_len,
        }
    }

    pub(in crate::storage) fn metadata(self) -> ExactPostingsMetadata {
        self.metadata
    }

    fn postings(self) -> (u64, u64) {
        (self.postings_offset, self.postings_len)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentIndexReadCount {
    pub calls: u64,
    pub bytes: u64,
}

impl SegmentIndexReadCount {
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            calls: self.calls.saturating_add(other.calls),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            calls: self.calls.saturating_sub(other.calls),
            bytes: self.bytes.saturating_sub(other.bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentIndexReadStats {
    pub root: SegmentIndexReadCount,
    pub routing: SegmentIndexReadCount,
    pub exact_directory: SegmentIndexReadCount,
    pub exact_page: SegmentIndexReadCount,
    pub auxiliary_directory: SegmentIndexReadCount,
    pub payload: SegmentIndexReadCount,
}

impl SegmentIndexReadStats {
    pub fn saturating_add(self, other: Self) -> Self {
        self.zip(other, SegmentIndexReadCount::saturating_add)
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        self.zip(other, SegmentIndexReadCount::saturating_sub)
    }

    fn zip(
        self,
        other: Self,
        combine: impl Fn(SegmentIndexReadCount, SegmentIndexReadCount) -> SegmentIndexReadCount,
    ) -> Self {
        Self {
            root: combine(self.root, other.root),
            routing: combine(self.routing, other.routing),
            exact_directory: combine(self.exact_directory, other.exact_directory),
            exact_page: combine(self.exact_page, other.exact_page),
            auxiliary_directory: combine(self.auxiliary_directory, other.auxiliary_directory),
            payload: combine(self.payload, other.payload),
        }
    }

    pub fn total_calls(self) -> u64 {
        self.root
            .calls
            .saturating_add(self.routing.calls)
            .saturating_add(self.exact_directory.calls)
            .saturating_add(self.exact_page.calls)
            .saturating_add(self.auxiliary_directory.calls)
            .saturating_add(self.payload.calls)
    }

    pub fn total_bytes(self) -> u64 {
        self.root
            .bytes
            .saturating_add(self.routing.bytes)
            .saturating_add(self.exact_directory.bytes)
            .saturating_add(self.exact_page.bytes)
            .saturating_add(self.auxiliary_directory.bytes)
            .saturating_add(self.payload.bytes)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoutingLookupResult {
    pub index_present: bool,
    pub metadata: Option<ExactPostingsMetadata>,
    pub bytes_read: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LabelValueTimeRangeIndex {
    ranges: HashMap<(u32, u32), LabelValueTimeRange>,
}

impl LabelValueTimeRangeIndex {
    pub fn insert(
        &mut self,
        label_name_sym: u32,
        label_value_sym: u32,
        min_time_ms: u64,
        max_time_ms: u64,
    ) {
        let range = LabelValueTimeRange {
            min_time_ms,
            max_time_ms,
        };
        self.ranges
            .entry((label_name_sym, label_value_sym))
            .and_modify(|existing| {
                existing.min_time_ms = existing.min_time_ms.min(range.min_time_ms);
                existing.max_time_ms = existing.max_time_ms.max(range.max_time_ms);
            })
            .or_insert(range);
    }

    pub fn insert_many(&mut self, labels: &[(u32, u32)], min_time_ms: u64, max_time_ms: u64) {
        self.ranges.reserve(labels.len());
        for (name, value) in labels {
            self.insert(*name, *value, min_time_ms, max_time_ms);
        }
    }

    pub fn get(&self, label_name_sym: u32, label_value_sym: u32) -> Option<LabelValueTimeRange> {
        self.ranges.get(&(label_name_sym, label_value_sym)).copied()
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[cfg(test)]
    fn label_time_ranges(&self) -> BTreeMap<u32, LabelValueTimeRange> {
        let mut out = BTreeMap::new();
        for ((name, _value), range) in &self.ranges {
            out.entry(*name)
                .and_modify(|existing: &mut LabelValueTimeRange| {
                    existing.min_time_ms = existing.min_time_ms.min(range.min_time_ms);
                    existing.max_time_ms = existing.max_time_ms.max(range.max_time_ms);
                })
                .or_insert(*range);
        }
        out
    }

    #[cfg(test)]
    fn ranges_by_label(&self) -> BTreeMap<u32, Vec<(u32, LabelValueTimeRange)>> {
        let mut out: BTreeMap<u32, Vec<(u32, LabelValueTimeRange)>> = BTreeMap::new();
        for ((name, value), range) in &self.ranges {
            out.entry(*name).or_default().push((*value, *range));
        }
        for ranges in out.values_mut() {
            ranges.sort_unstable_by_key(|(value, _range)| *value);
        }
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSeriesRange {
    pub start_series_ref: u32,
    pub series_count: u32,
    pub kind_mask: u16,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
}

impl MetricSeriesRange {
    pub fn overlaps(self, start_ms: u64, end_ms: u64) -> bool {
        self.max_time_ms >= start_ms && self.min_time_ms <= end_ms
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MetricSeriesRangeIndex {
    ranges: BTreeMap<u32, Vec<MetricSeriesRange>>,
}

impl MetricSeriesRangeIndex {
    pub fn from_series(
        series: &[SeriesEntry],
        symbols: &SegmentSymbols,
        time_ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<Self> {
        let Some(metric_name_sym) = symbols.lookup(METRIC_NAME_LABEL) else {
            if series.is_empty() {
                return Ok(Self::default());
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric name symbol missing",
            ));
        };

        let mut index = Self::default();
        let mut current: Option<(u32, MetricSeriesRange)> = None;
        for (series_ref, entry) in series.iter().enumerate() {
            let series_ref = u32::try_from(series_ref).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "series_ref exceeds u32")
            })?;
            let Some(metric_sym) = entry
                .labels
                .iter()
                .find_map(|(name, value)| (*name == metric_name_sym).then_some(*value))
            else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series metric name label missing",
                ));
            };
            let time_range =
                time_ranges
                    .get(metric_name_sym, metric_sym)
                    .unwrap_or(LabelValueTimeRange {
                        min_time_ms: 0,
                        max_time_ms: u64::MAX,
                    });

            match current.as_mut() {
                Some((current_metric, range)) if *current_metric == metric_sym => {
                    range.series_count = range.series_count.checked_add(1).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "metric series range too large")
                    })?;
                    range.kind_mask |= u16::from(entry.kind_mask);
                    range.min_time_ms = range.min_time_ms.min(time_range.min_time_ms);
                    range.max_time_ms = range.max_time_ms.max(time_range.max_time_ms);
                }
                Some((current_metric, range)) => {
                    index.insert_range(*current_metric, *range);
                    current = Some((
                        metric_sym,
                        MetricSeriesRange {
                            start_series_ref: series_ref,
                            series_count: 1,
                            kind_mask: u16::from(entry.kind_mask),
                            min_time_ms: time_range.min_time_ms,
                            max_time_ms: time_range.max_time_ms,
                        },
                    ));
                }
                None => {
                    current = Some((
                        metric_sym,
                        MetricSeriesRange {
                            start_series_ref: series_ref,
                            series_count: 1,
                            kind_mask: u16::from(entry.kind_mask),
                            min_time_ms: time_range.min_time_ms,
                            max_time_ms: time_range.max_time_ms,
                        },
                    ));
                }
            }
        }

        if let Some((metric_sym, range)) = current {
            index.insert_range(metric_sym, range);
        }

        Ok(index)
    }

    pub fn insert_range(&mut self, metric_sym: u32, range: MetricSeriesRange) {
        self.ranges.entry(metric_sym).or_default().push(range);
    }

    pub fn ranges(&self, metric_sym: u32) -> &[MetricSeriesRange] {
        self.ranges
            .get(&metric_sym)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Cross-validates this required routing index against the exact series
    /// and symbol counts which will be published in the same segment.
    pub(crate) fn validate_complete_partition(
        &self,
        num_series: u32,
        symbol_count: u32,
    ) -> io::Result<()> {
        let mut next_series_ref = 0u64;
        for (metric_sym, ranges) in self.entries() {
            if metric_sym >= symbol_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range symbol exceeds the authoritative symbol count",
                ));
            }
            validate_metric_series_range_sequence(ranges, io::ErrorKind::InvalidData)?;
            for range in ranges {
                if u64::from(range.start_series_ref) != next_series_ref {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "metric series ranges do not form a canonical complete partition",
                    ));
                }
                next_series_ref = u64::from(range.start_series_ref)
                    .checked_add(u64::from(range.series_count))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "metric series range series end overflows",
                        )
                    })?;
            }
        }
        if next_series_ref != u64::from(num_series) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series ranges do not cover the authoritative series count",
            ));
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    fn entries(&self) -> impl Iterator<Item = (u32, &[MetricSeriesRange])> {
        self.ranges
            .iter()
            .map(|(metric_sym, ranges)| (*metric_sym, ranges.as_slice()))
    }
}

fn validate_metric_series_range_sequence(
    ranges: &[MetricSeriesRange],
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    if ranges.is_empty() {
        return Err(io::Error::new(
            error_kind,
            "metric series range metric has no ranges",
        ));
    }
    let mut previous_end = None;
    for range in ranges {
        if range.series_count == 0 {
            return Err(io::Error::new(
                error_kind,
                "metric series range series count is zero",
            ));
        }
        let series_end = u64::from(range.start_series_ref)
            .checked_add(u64::from(range.series_count))
            .ok_or_else(|| {
                io::Error::new(error_kind, "metric series range series end overflows")
            })?;
        if series_end > u64::from(u32::MAX) + 1 {
            return Err(io::Error::new(
                error_kind,
                "metric series range series end exceeds the u32 domain",
            ));
        }
        if previous_end.is_some_and(|previous| u64::from(range.start_series_ref) < previous) {
            return Err(io::Error::new(
                error_kind,
                "metric series ranges are unordered or overlapping",
            ));
        }
        previous_end = Some(series_end);
        if range.min_time_ms > range.max_time_ms {
            return Err(io::Error::new(
                error_kind,
                "metric series time range is reversed",
            ));
        }
        if range.kind_mask == 0 || range.kind_mask & !VALID_METRIC_SERIES_KIND_MASK != 0 {
            return Err(io::Error::new(
                error_kind,
                "metric series range kind mask is zero or contains unknown bits",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::series::{SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM};
    use std::io::Cursor;

    #[test]
    fn label_value_time_range_index_bulk_insert_merges_ranges() {
        let mut index = LabelValueTimeRangeIndex::default();

        index.insert_many(&[(2, 20), (1, 10)], 1_000, 2_000);
        index.insert_many(&[(1, 10), (3, 30)], 500, 4_000);

        assert_eq!(index.len(), 3);
        assert_eq!(
            index.get(1, 10),
            Some(LabelValueTimeRange {
                min_time_ms: 500,
                max_time_ms: 4_000,
            })
        );
        assert_eq!(
            index.get(2, 20),
            Some(LabelValueTimeRange {
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            })
        );
        assert_eq!(
            index.get(3, 30),
            Some(LabelValueTimeRange {
                min_time_ms: 500,
                max_time_ms: 4_000,
            })
        );
    }

    #[test]
    fn segment_index_serializes_label_value_time_ranges_deterministically() {
        let mut forward = LabelValueTimeRangeIndex::default();
        forward.insert_many(&[(1, 10), (1, 20), (2, 30)], 1_000, 2_000);

        let mut reverse = LabelValueTimeRangeIndex::default();
        reverse.insert_many(&[(2, 30), (1, 20), (1, 10)], 1_000, 2_000);

        let mut forward_bytes = Vec::new();
        write_segment_indexes_unbound_for_test(
            &mut forward_bytes,
            &SegmentIndexes {
                exact_postings: ExactPostingsIndex::default(),
                label_values: LabelValueFstIndex::default(),
                label_value_time_ranges: forward,
                metric_series_ranges: MetricSeriesRangeIndex::default(),
                routing_index: None,
            },
        )
        .unwrap();

        let mut reverse_bytes = Vec::new();
        write_segment_indexes_unbound_for_test(
            &mut reverse_bytes,
            &SegmentIndexes {
                exact_postings: ExactPostingsIndex::default(),
                label_values: LabelValueFstIndex::default(),
                label_value_time_ranges: reverse,
                metric_series_ranges: MetricSeriesRangeIndex::default(),
                routing_index: None,
            },
        )
        .unwrap();

        assert_eq!(forward_bytes, reverse_bytes);
    }

    #[test]
    fn routing_index_builder_rejects_missing_time_range() {
        let mut symbols = SegmentSymbols::default();
        let name = symbols.intern("route");
        let value = symbols.intern("/api");
        let mut postings = ExactPostingsIndex::default();
        postings.insert(name, value, 0);

        let error = SegmentRoutingIndex::from_indexes(
            &symbols,
            &postings,
            &LabelValueTimeRangeIndex::default(),
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("has no label-value time range"));
    }

    #[test]
    fn routing_index_builder_rejects_unresolved_symbols() {
        let mut symbols = SegmentSymbols::default();
        let present = symbols.intern("present");
        for (name_sym, value_sym, expected) in
            [(2, present, "label-name"), (present, 2, "label-value")]
        {
            let mut postings = ExactPostingsIndex::default();
            postings.insert(name_sym, value_sym, 0);
            let mut ranges = LabelValueTimeRangeIndex::default();
            ranges.insert(name_sym, value_sym, 100, 200);

            let error =
                SegmentRoutingIndex::from_indexes(&symbols, &postings, &ranges).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(error.to_string().contains(expected));
            assert!(error.to_string().contains("cannot be resolved"));
        }
    }

    #[test]
    fn routing_index_builder_is_complete_and_deterministic() {
        let mut symbols = SegmentSymbols::default();
        let metric_name = symbols.intern(METRIC_NAME_LABEL);
        let metric = symbols.intern("request_duration_seconds");
        let route = symbols.intern("route");
        let api = symbols.intern("/api");

        let mut forward_postings = ExactPostingsIndex::default();
        forward_postings.insert(route, api, 2);
        forward_postings.insert(metric_name, metric, 1);
        forward_postings.insert(route, api, 0);
        forward_postings.insert(metric_name, metric, 0);
        let mut reverse_postings = ExactPostingsIndex::default();
        reverse_postings.insert(metric_name, metric, 0);
        reverse_postings.insert(route, api, 0);
        reverse_postings.insert(metric_name, metric, 1);
        reverse_postings.insert(route, api, 2);

        let mut forward_ranges = LabelValueTimeRangeIndex::default();
        forward_ranges.insert(route, api, 300, 350);
        forward_ranges.insert(metric_name, metric, 100, 200);
        forward_ranges.insert(route, api, 250, 400);
        let mut reverse_ranges = LabelValueTimeRangeIndex::default();
        reverse_ranges.insert(metric_name, metric, 100, 200);
        reverse_ranges.insert(route, api, 250, 400);

        let forward =
            SegmentRoutingIndex::from_indexes(&symbols, &forward_postings, &forward_ranges)
                .unwrap();
        let reverse =
            SegmentRoutingIndex::from_indexes(&symbols, &reverse_postings, &reverse_ranges)
                .unwrap();

        assert_eq!(forward.len(), forward_postings.len());
        assert_eq!(forward, reverse);
        assert_eq!(forward.encode().unwrap(), reverse.encode().unwrap());
        assert_eq!(
            forward.exact_postings_metadata("route", "/api"),
            Some(ExactPostingsMetadata {
                byte_len: 12,
                time_range: LabelValueTimeRange {
                    min_time_ms: 250,
                    max_time_ms: 400,
                },
            })
        );
    }

    #[test]
    fn adaptive_routing_records_the_canonical_encoded_postings_length() {
        let mut symbols = SegmentSymbols::default();
        let name = symbols.intern("route");
        let dense_value = symbols.intern("dense");
        let tie_value = symbols.intern("raw-tie");
        let mut postings = ExactPostingsIndex::default();
        for series_ref in 0..4 {
            postings.insert(name, dense_value, series_ref);
        }
        postings.insert(name, tie_value, 1 << 21);
        let mut ranges = LabelValueTimeRangeIndex::default();
        ranges.insert(name, dense_value, 100, 200);
        ranges.insert(name, tie_value, 100, 200);

        let raw = SegmentRoutingIndex::from_indexes(&symbols, &postings, &ranges).unwrap();
        let adaptive =
            SegmentRoutingIndex::from_indexes_adaptive(&symbols, &postings, &ranges).unwrap();

        assert_eq!(
            raw.exact_postings_metadata("route", "dense")
                .unwrap()
                .byte_len,
            20
        );
        assert_eq!(
            adaptive
                .exact_postings_metadata("route", "dense")
                .unwrap()
                .byte_len,
            8,
            "four dense refs use the four-byte header plus four one-byte deltas"
        );
        assert_eq!(
            adaptive
                .exact_postings_metadata("route", "raw-tie")
                .unwrap()
                .byte_len,
            8,
            "a four-byte singleton varint ties RAW32, which must win"
        );
    }

    #[test]
    fn metric_series_ranges_group_metric_major_series() {
        let mut symbols = SegmentSymbols::default();
        let metric = symbols.intern(METRIC_NAME_LABEL);
        let cpu = symbols.intern("cpu_usage");
        let memory = symbols.intern("memory_usage");
        let pod = symbols.intern("pod");
        let pod_a = symbols.intern("a");
        let pod_b = symbols.intern("b");
        let series = vec![
            SeriesEntry {
                series_id: 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(metric, cpu), (pod, pod_a)],
            },
            SeriesEntry {
                series_id: 2,
                kind_mask: SERIES_KIND_HISTOGRAM,
                chunk_index: Default::default(),
                labels: vec![(metric, cpu), (pod, pod_b)],
            },
            SeriesEntry {
                series_id: 3,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(metric, memory), (pod, pod_a)],
            },
        ];
        let mut time_ranges = LabelValueTimeRangeIndex::default();
        time_ranges.insert(metric, cpu, 1_000, 2_000);
        time_ranges.insert(metric, memory, 3_000, 4_000);

        let ranges = MetricSeriesRangeIndex::from_series(&series, &symbols, &time_ranges).unwrap();

        assert_eq!(
            ranges.ranges(cpu),
            &[MetricSeriesRange {
                start_series_ref: 0,
                series_count: 2,
                kind_mask: u16::from(SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM),
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            }]
        );
        assert_eq!(
            ranges.ranges(memory),
            &[MetricSeriesRange {
                start_series_ref: 2,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 3_000,
                max_time_ms: 4_000,
            }]
        );
        ranges
            .validate_complete_partition(3, u32::try_from(symbols.len()).unwrap())
            .unwrap();

        let indexes = SegmentIndexes {
            metric_series_ranges: ranges.clone(),
            ..SegmentIndexes::default()
        };
        let mut encoded = Vec::new();
        write_segment_indexes_for_roots(&mut encoded, &indexes, 3, &symbols).unwrap();
        assert!(!encoded.is_empty());

        let trailing_gap =
            write_segment_indexes_for_roots(Vec::new(), &indexes, 4, &symbols).unwrap_err();
        assert_eq!(trailing_gap.kind(), io::ErrorKind::InvalidData);

        let mut short_symbols = SegmentSymbols::default();
        short_symbols.intern("first");
        short_symbols.intern("second");
        let foreign_symbol =
            write_segment_indexes_for_roots(Vec::new(), &indexes, 3, &short_symbols).unwrap_err();
        assert_eq!(foreign_symbol.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn production_index_writer_rejects_foreign_root_references() {
        let mut symbols = SegmentSymbols::default();
        symbols.intern("zero");
        symbols.intern("one");
        symbols.intern("two");
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            1,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 100,
                max_time_ms: 200,
            },
        );
        let valid = SegmentIndexes {
            metric_series_ranges,
            ..SegmentIndexes::default()
        };

        let mut foreign_exact_symbol = valid.clone();
        foreign_exact_symbol.exact_postings.insert(3, 1, 0);
        let error = write_segment_indexes_for_roots(Vec::new(), &foreign_exact_symbol, 1, &symbols)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let mut foreign_series_ref = valid.clone();
        foreign_series_ref.exact_postings.insert(0, 1, 1);
        let error = write_segment_indexes_for_roots(Vec::new(), &foreign_series_ref, 1, &symbols)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let mut foreign_fst_symbol = valid.clone();
        foreign_fst_symbol.label_values.insert_fst(3, vec![0]);
        let error = write_segment_indexes_for_roots(Vec::new(), &foreign_fst_symbol, 1, &symbols)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let mut foreign_time_range_symbol = valid;
        foreign_time_range_symbol
            .label_value_time_ranges
            .insert(0, 3, 100, 200);
        let error =
            write_segment_indexes_for_roots(Vec::new(), &foreign_time_range_symbol, 1, &symbols)
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn production_index_writer_rejects_stale_routing_metadata() {
        let mut symbols = SegmentSymbols::default();
        let metric_name = symbols.intern(METRIC_NAME_LABEL);
        let metric = symbols.intern("request_duration_seconds");
        let mut exact_postings = ExactPostingsIndex::default();
        exact_postings.insert(metric_name, metric, 0);
        let mut time_ranges = LabelValueTimeRangeIndex::default();
        time_ranges.insert(metric_name, metric, 100, 200);
        let routing_index =
            SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &time_ranges).unwrap();
        time_ranges.insert(metric_name, metric, 50, 250);
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            metric,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 50,
                max_time_ms: 250,
            },
        );
        let indexes = SegmentIndexes {
            exact_postings,
            label_value_time_ranges: time_ranges,
            metric_series_ranges,
            routing_index: Some(routing_index),
            ..SegmentIndexes::default()
        };

        let error = write_segment_indexes_for_roots(Vec::new(), &indexes, 1, &symbols).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("routing index metadata does not match")
        );
    }

    #[test]
    fn metric_series_ranges_decoder_rejects_zero_range_count() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&METRIC_SERIES_RANGES_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&METRIC_SERIES_RANGES_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&7u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());

        let error = read_metric_series_ranges_blob(&bytes).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn metric_series_ranges_reject_impossible_group_count_before_visiting() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&METRIC_SERIES_RANGES_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&METRIC_SERIES_RANGES_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        // One canonical minimum-size group (8-byte group header plus one
        // 28-byte range) cannot encode the declared two groups.
        bytes.resize(12 + 8 + METRIC_SERIES_RANGE_RECORD_LEN, 0);
        let mut visited = false;

        let error = walk_metric_series_ranges_blob(&bytes, None, |_| {
            visited = true;
            Ok(())
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            !visited,
            "invalid group count reached an allocating visitor"
        );
    }

    #[test]
    fn segment_index_roundtrips_required_metric_series_ranges() {
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            10,
            MetricSeriesRange {
                start_series_ref: 4,
                series_count: 3,
                kind_mask: u16::from(SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM),
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            },
        );
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges,
            routing_index: None,
        };

        let mut bytes = Vec::new();
        write_segment_indexes_unbound_for_test(&mut bytes, &indexes).unwrap();
        let reader = SegmentIndexReader::open(Cursor::new(bytes)).unwrap();

        assert_eq!(
            reader.metric_series_ranges(10).unwrap(),
            vec![MetricSeriesRange {
                start_series_ref: 4,
                series_count: 3,
                kind_mask: u16::from(SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM),
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            }]
        );
        assert!(reader.metric_series_ranges(11).unwrap().is_empty());
    }

    #[test]
    fn segment_index_reader_rejects_v6_container() {
        let mut bytes = vec![0u8; 272];
        bytes[0..4].copy_from_slice(&SEGMENT_INDEXES_MAGIC.to_le_bytes());
        bytes[4..6].copy_from_slice(&SEGMENT_INDEX_VERSION.to_le_bytes());

        let err = match SegmentIndexReader::open(Cursor::new(bytes.clone())) {
            Ok(_) => panic!("expected v6 rejection"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("expected segment index version 7"));

        let err = read_segment_indexes(Cursor::new(bytes)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("expected segment index version 7"));
    }

    #[test]
    fn segment_index_reader_streams_label_values_by_prefix() {
        let mut symbols = SegmentSymbols::default();
        let name = symbols.intern(METRIC_NAME_LABEL);
        let values = [
            "alpha_metric",
            "beta_metric",
            "go_gc_duration_seconds",
            "go_gc_duration_seconds_count",
            "http_requests_total",
        ];
        let series: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(idx, value)| SeriesEntry {
                series_id: idx as u64 + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(name, symbols.intern(value))],
            })
            .collect();
        let indexes = SegmentIndexes {
            exact_postings: ExactPostingsIndex::default(),
            label_values: LabelValueFstIndex::from_series(&series, &symbols).unwrap(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
            metric_series_ranges: MetricSeriesRangeIndex::default(),
            routing_index: None,
        };

        let mut bytes = Vec::new();
        write_segment_indexes_unbound_for_test(&mut bytes, &indexes).unwrap();
        let reader = SegmentIndexReader::open(Cursor::new(bytes)).unwrap();

        assert_eq!(
            reader
                .label_values_with_prefix(name, Some("go_gc_duration_seconds"))
                .unwrap(),
            vec![
                "go_gc_duration_seconds".to_string(),
                "go_gc_duration_seconds_count".to_string()
            ]
        );
        assert!(
            reader
                .label_values_with_prefix(name, Some("missing"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            reader.label_values_with_prefix(name, None).unwrap().len(),
            5
        );
    }

    #[test]
    fn segment_index_reader_clones_share_immutable_directory() {
        let mut symbols = SegmentSymbols::default();
        let metric_name = symbols.intern(METRIC_NAME_LABEL);
        let metric = symbols.intern("request_duration_seconds");
        let series = vec![SeriesEntry {
            series_id: 7,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: Default::default(),
            labels: vec![(metric_name, metric)],
        }];

        let mut exact_postings = ExactPostingsIndex::default();
        exact_postings.insert(metric_name, metric, 0);
        let label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
        let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
        label_value_time_ranges.insert(metric_name, metric, 1_000, 2_000);
        let mut metric_series_ranges = MetricSeriesRangeIndex::default();
        metric_series_ranges.insert_range(
            metric,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: 1,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            },
        );
        let routing_index =
            SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &label_value_time_ranges)
                .unwrap();
        let indexes = SegmentIndexes {
            exact_postings,
            label_values,
            label_value_time_ranges,
            metric_series_ranges,
            routing_index: Some(routing_index),
        };

        let mut file = tempfile::tempfile().unwrap();
        write_segment_indexes_unbound_for_test(&mut file, &indexes).unwrap();
        let reader = SegmentIndexReader::open(file).unwrap();
        let cloned = reader.try_clone_reader().unwrap();

        assert!(reader.read_stats().root.calls > 0);
        assert_eq!(cloned.read_stats(), SegmentIndexReadStats::default());
        assert_eq!(
            reader.exact_postings(metric_name, metric).unwrap(),
            cloned.exact_postings(metric_name, metric).unwrap()
        );
        assert_eq!(
            reader.label_values(metric_name).unwrap(),
            cloned.label_values(metric_name).unwrap()
        );
        assert_eq!(
            reader.label_value_time_range(metric_name, metric).unwrap(),
            cloned.label_value_time_range(metric_name, metric).unwrap()
        );
        assert_eq!(
            reader.metric_series_ranges(metric).unwrap(),
            cloned.metric_series_ranges(metric).unwrap()
        );
        assert_eq!(
            reader
                .routing_exact_postings_metadata(METRIC_NAME_LABEL, "request_duration_seconds")
                .unwrap(),
            cloned
                .routing_exact_postings_metadata(METRIC_NAME_LABEL, "request_duration_seconds")
                .unwrap()
        );
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SegmentIndexes {
    pub exact_postings: ExactPostingsIndex,
    pub label_values: LabelValueFstIndex,
    pub label_value_time_ranges: LabelValueTimeRangeIndex,
    pub metric_series_ranges: MetricSeriesRangeIndex,
    pub routing_index: Option<SegmentRoutingIndex>,
}

impl SegmentIndexes {
    fn validate_root_bounds(&self, num_series: u32, symbols: &SegmentSymbols) -> io::Result<()> {
        let symbol_count = u32::try_from(symbols.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "authoritative symbol count exceeds u32",
            )
        })?;
        self.metric_series_ranges
            .validate_complete_partition(num_series, symbol_count)?;
        for (label_name_sym, label_value_sym, refs) in self.exact_postings.entries() {
            if label_name_sym >= symbol_count || label_value_sym >= symbol_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "exact postings symbol exceeds the authoritative symbol count",
                ));
            }
            if refs.iter().any(|series_ref| *series_ref >= num_series) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "exact postings reference exceeds the authoritative series count",
                ));
            }
        }
        if self
            .label_values
            .fsts
            .keys()
            .any(|label_name_sym| *label_name_sym >= symbol_count)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label-value FST symbol exceeds the authoritative symbol count",
            ));
        }
        if self
            .label_value_time_ranges
            .ranges
            .keys()
            .any(|(label_name_sym, label_value_sym)| {
                *label_name_sym >= symbol_count || *label_value_sym >= symbol_count
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label-value time-range symbol exceeds the authoritative symbol count",
            ));
        }
        if let Some(routing_index) = &self.routing_index {
            routing_index.validate_against_indexes(
                symbols,
                &self.exact_postings,
                &self.label_value_time_ranges,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentRoutingIndex {
    labels: BTreeMap<String, BTreeMap<String, ExactPostingsMetadata>>,
}

impl SegmentRoutingIndex {
    /// Builds one routing entry for every exact-postings key.
    ///
    /// Missing source metadata is an inconsistent index build, never an
    /// instruction to omit the key: omission could make early pruning return
    /// a false negative.
    pub fn from_indexes(
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<Self> {
        Self::from_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V8Raw,
        )
    }

    pub fn from_indexes_adaptive(
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<Self> {
        Self::from_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V9Adaptive,
        )
    }

    fn from_indexes_with_length_format(
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
        format: ExactPostingsLengthFormat,
    ) -> io::Result<Self> {
        let mut index = Self::default();
        for (name_sym, value_sym, refs) in postings.entries() {
            let (name, value, metadata) =
                routing_entry_from_indexes(symbols, ranges, name_sym, value_sym, refs, format)?;
            let previous = index
                .labels
                .entry(name.to_string())
                .or_default()
                .insert(value.to_string(), metadata);
            if previous.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing source indexes contain a duplicate logical label key",
                ));
            }
        }
        Ok(index)
    }

    fn validate_against_indexes(
        &self,
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<()> {
        self.validate_against_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V8Raw,
        )
    }

    fn validate_against_indexes_adaptive(
        &self,
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
    ) -> io::Result<()> {
        self.validate_against_indexes_with_length_format(
            symbols,
            postings,
            ranges,
            ExactPostingsLengthFormat::V9Adaptive,
        )
    }

    fn validate_against_indexes_with_length_format(
        &self,
        symbols: &SegmentSymbols,
        postings: &ExactPostingsIndex,
        ranges: &LabelValueTimeRangeIndex,
        format: ExactPostingsLengthFormat,
    ) -> io::Result<()> {
        if self.len() != postings.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index entry count does not match exact postings",
            ));
        }
        for (name_sym, value_sym, refs) in postings.entries() {
            let (name, value, expected) =
                routing_entry_from_indexes(symbols, ranges, name_sym, value_sym, refs, format)?;
            if self.exact_postings_metadata(name, value) != Some(expected) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing index metadata does not match exact postings and time ranges",
                ));
            }
        }
        Ok(())
    }

    pub fn exact_postings_metadata(
        &self,
        name: &str,
        value: &str,
    ) -> Option<ExactPostingsMetadata> {
        self.labels
            .get(name)
            .and_then(|values| values.get(value))
            .copied()
    }

    fn encode(&self) -> io::Result<Vec<u8>> {
        let mut entries = Vec::new();
        for (name, values) in &self.labels {
            for (value, metadata) in values {
                entries.push((routing_key_bytes(name, value)?, *metadata));
            }
        }
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        let bucket_count = routing_bucket_count(entries.len())?;
        let buckets_offset = ROUTING_INDEX_HEADER_LEN as u64;
        let key_bytes_offset = buckets_offset
            .checked_add(
                u64::try_from(bucket_count)
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "routing bucket count exceeds u64",
                        )
                    })?
                    .checked_mul(ROUTING_INDEX_BUCKET_LEN as u64)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "routing index too large")
                    })?,
            )
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "routing index too large")
            })?;

        let mut buckets = vec![RoutingBucketRecord::default(); bucket_count];
        let mut key_bytes = Vec::new();
        for (key, metadata) in entries {
            let hash = routing_key_hash(&key);
            let mut bucket = (hash as usize) & (bucket_count - 1);
            loop {
                if buckets[bucket].is_empty() {
                    let key_offset = u32_len(key_bytes.len(), "routing key bytes offset")?;
                    let key_len = u32_len(key.len(), "routing key length")?;
                    key_bytes.extend_from_slice(&key);
                    buckets[bucket] = RoutingBucketRecord {
                        hash,
                        key_offset,
                        key_len,
                        metadata,
                    };
                    break;
                }
                bucket = (bucket + 1) & (bucket_count - 1);
            }
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ROUTING_INDEX_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&ROUTING_INDEX_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&u32_len(self.len(), "routing entry count")?.to_le_bytes());
        bytes.extend_from_slice(&u32_len(bucket_count, "routing bucket count")?.to_le_bytes());
        bytes.extend_from_slice(&buckets_offset.to_le_bytes());
        bytes.extend_from_slice(&key_bytes_offset.to_le_bytes());
        bytes.extend_from_slice(
            &u64::try_from(key_bytes.len())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "routing key bytes length exceeds u64",
                    )
                })?
                .to_le_bytes(),
        );
        for bucket in buckets {
            bucket.encode(&mut bytes);
        }
        bytes.extend_from_slice(&key_bytes);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let header = RoutingIndexHeader::decode(bytes, bytes.len() as u64)?;
        let mut labels = BTreeMap::new();
        let mut decoded_entries = 0u32;
        for bucket_index in 0..header.bucket_count {
            let offset = header.bucket_offset(bucket_index)?;
            let bucket = RoutingBucketRecord::decode(read_bytes_at(
                bytes,
                offset,
                ROUTING_INDEX_BUCKET_LEN,
            )?)?;
            let Some(key_range) = bucket.validate_touched(header)? else {
                continue;
            };
            let key = read_bytes_at(bytes, key_range.offset, key_range.len)?;
            let (name, value) = validate_routing_bucket_key(bucket, key)?;
            if labels
                .get(name)
                .is_some_and(|values: &BTreeMap<String, ExactPostingsMetadata>| {
                    values.contains_key(value)
                })
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing index contains a duplicate logical key",
                ));
            }
            labels
                .entry(name.to_owned())
                .or_insert_with(BTreeMap::new)
                .insert(value.to_owned(), bucket.metadata);
            decoded_entries = decoded_entries.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing entry count overflow")
            })?;
        }
        if decoded_entries != header.entry_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index entry count mismatch",
            ));
        }
        Ok(Self { labels })
    }

    fn len(&self) -> usize {
        self.labels.values().map(BTreeMap::len).sum()
    }
}

fn routing_entry_from_indexes<'a>(
    symbols: &'a SegmentSymbols,
    ranges: &LabelValueTimeRangeIndex,
    name_sym: u32,
    value_sym: u32,
    refs: &[u32],
    format: ExactPostingsLengthFormat,
) -> io::Result<(&'a str, &'a str, ExactPostingsMetadata)> {
    let name = symbols.resolve(name_sym).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing label-name symbol {name_sym} cannot be resolved"),
        )
    })?;
    let value = symbols.resolve(value_sym).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing label-value symbol {value_sym} cannot be resolved"),
        )
    })?;
    let range = ranges.get(name_sym, value_sym).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing source ({name_sym}, {value_sym}) has no label-value time range"),
        )
    })?;
    if range.min_time_ms > range.max_time_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("routing source ({name_sym}, {value_sym}) has a reversed time range"),
        ));
    }
    Ok((
        name,
        value,
        ExactPostingsMetadata {
            byte_len: match format {
                ExactPostingsLengthFormat::V8Raw => exact_postings_blob_len(refs)?,
                ExactPostingsLengthFormat::V9Adaptive => adaptive_exact_postings_blob_len(refs)?,
            },
            time_range: range,
        },
    ))
}

#[derive(Debug, Clone, Copy)]
enum ExactPostingsLengthFormat {
    V8Raw,
    V9Adaptive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoutingIndexHeader {
    entry_count: u32,
    bucket_count: u32,
    buckets_offset: u64,
    key_bytes_offset: u64,
    key_bytes_len: u64,
}

impl RoutingIndexHeader {
    fn decode(bytes: &[u8], blob_len: u64) -> io::Result<Self> {
        if bytes.len() < ROUTING_INDEX_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing index header truncated",
            ));
        }
        let mut cursor = 0usize;
        let magic = read_u32(bytes, &mut cursor)?;
        if magic != ROUTING_INDEX_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index magic mismatch",
            ));
        }
        let version = read_u16(bytes, &mut cursor)?;
        if version != ROUTING_INDEX_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported routing index version",
            ));
        }
        let flags = read_u16(bytes, &mut cursor)?;
        if flags != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index flags are non-zero",
            ));
        }
        let entry_count = read_u32(bytes, &mut cursor)?;
        let bucket_count = read_u32(bytes, &mut cursor)?;
        let buckets_offset = read_u64(bytes, &mut cursor)?;
        let key_bytes_offset = read_u64(bytes, &mut cursor)?;
        let key_bytes_len = read_u64(bytes, &mut cursor)?;
        let header = Self {
            entry_count,
            bucket_count,
            buckets_offset,
            key_bytes_offset,
            key_bytes_len,
        };
        header.validate(blob_len)?;
        Ok(header)
    }

    fn validate(self, blob_len: u64) -> io::Result<()> {
        if self.bucket_count == 0 || !self.bucket_count.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index bucket count must be a non-zero power of two",
            ));
        }
        if self.buckets_offset < ROUTING_INDEX_HEADER_LEN as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index bucket offset overlaps header",
            ));
        }
        let bucket_bytes = u64::from(self.bucket_count)
            .checked_mul(ROUTING_INDEX_BUCKET_LEN as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing bucket table too large")
            })?;
        let buckets_end = self
            .buckets_offset
            .checked_add(bucket_bytes)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing bucket table too large")
            })?;
        if buckets_end > blob_len || buckets_end > self.key_bytes_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket table is out of bounds",
            ));
        }
        let key_bytes_end = self
            .key_bytes_offset
            .checked_add(self.key_bytes_len)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing key bytes too large")
            })?;
        if key_bytes_end > blob_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing key bytes are out of bounds",
            ));
        }
        if u64::from(self.entry_count) >= u64::from(self.bucket_count) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing index load factor is invalid",
            ));
        }
        Ok(())
    }

    fn bucket_offset(self, bucket_index: u32) -> io::Result<u64> {
        if bucket_index >= self.bucket_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "routing bucket index out of bounds",
            ));
        }
        self.buckets_offset
            .checked_add(u64::from(bucket_index) * ROUTING_INDEX_BUCKET_LEN as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "routing bucket offset overflow")
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoutingBucketRecord {
    hash: u64,
    key_offset: u32,
    key_len: u32,
    metadata: ExactPostingsMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoutingBucketKeyRange {
    offset: u64,
    len: usize,
}

impl Default for RoutingBucketRecord {
    fn default() -> Self {
        Self {
            hash: 0,
            key_offset: 0,
            key_len: 0,
            metadata: ExactPostingsMetadata {
                byte_len: 0,
                time_range: LabelValueTimeRange {
                    min_time_ms: 0,
                    max_time_ms: 0,
                },
            },
        }
    }
}

impl RoutingBucketRecord {
    fn is_empty(self) -> bool {
        self.key_len == 0
    }

    fn encode(self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.hash.to_le_bytes());
        bytes.extend_from_slice(&self.key_offset.to_le_bytes());
        bytes.extend_from_slice(&self.key_len.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.time_range.min_time_ms.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.time_range.max_time_ms.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.byte_len.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != ROUTING_INDEX_BUCKET_LEN {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing bucket record truncated",
            ));
        }
        let mut cursor = 0usize;
        let hash = read_u64(bytes, &mut cursor)?;
        let key_offset = read_u32(bytes, &mut cursor)?;
        let key_len = read_u32(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        let byte_len = read_u64(bytes, &mut cursor)?;
        Ok(Self {
            hash,
            key_offset,
            key_len,
            metadata: ExactPostingsMetadata {
                byte_len,
                time_range: LabelValueTimeRange {
                    min_time_ms,
                    max_time_ms,
                },
            },
        })
    }

    fn validate_touched(
        self,
        header: RoutingIndexHeader,
    ) -> io::Result<Option<RoutingBucketKeyRange>> {
        if self.key_len == 0 {
            if self.hash != 0
                || self.key_offset != 0
                || self.metadata.byte_len != 0
                || self.metadata.time_range.min_time_ms != 0
                || self.metadata.time_range.max_time_ms != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing empty bucket is not canonical",
                ));
            }
            return Ok(None);
        }
        if self.metadata.byte_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket postings byte length is zero",
            ));
        }
        if self.metadata.time_range.min_time_ms > self.metadata.time_range.max_time_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket time range is reversed",
            ));
        }
        let relative_offset = u64::from(self.key_offset);
        let relative_end = relative_offset
            .checked_add(u64::from(self.key_len))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing bucket key range overflow",
                )
            })?;
        if relative_end > header.key_bytes_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket key range exceeds declared key bytes",
            ));
        }
        let offset = header
            .key_bytes_offset
            .checked_add(relative_offset)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "routing bucket key offset overflow",
                )
            })?;
        let len = usize::try_from(self.key_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "routing bucket key length exceeds platform usize",
            )
        })?;
        Ok(Some(RoutingBucketKeyRange { offset, len }))
    }
}

fn validate_routing_bucket_key(
    bucket: RoutingBucketRecord,
    key: &[u8],
) -> io::Result<(&str, &str)> {
    let parts = routing_key_parts(key)?;
    if routing_key_hash(key) != bucket.hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "routing bucket hash does not match its stored key",
        ));
    }
    Ok(parts)
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentIndexDirectoryEntry {
    kind: u16,
    label_name_sym: u32,
    label_value_sym: u32,
    offset: u64,
    len: u64,
    min_time_ms: u64,
    max_time_ms: u64,
}

pub struct SegmentIndexReader<R>
where
    R: SegmentIndexReadAt,
{
    inner: v7::SegmentIndexV7Reader<R>,
}

impl<R> SegmentIndexReader<R>
where
    R: SegmentIndexReadAt,
{
    pub fn open(source: R) -> io::Result<Self> {
        Ok(Self {
            inner: v7::SegmentIndexV7Reader::open(source)?,
        })
    }

    pub fn try_clone_reader(&self) -> io::Result<Self> {
        Ok(Self {
            inner: self.inner.try_clone_reader()?,
        })
    }

    pub fn read_stats(&self) -> SegmentIndexReadStats {
        self.inner.stats()
    }

    pub fn label_name_symbols(&self) -> io::Result<Vec<u32>> {
        self.inner.label_name_symbols()
    }

    pub fn has_label_values(&self) -> io::Result<bool> {
        self.inner.has_label_values()
    }

    pub fn label_time_range(&self, label_name_sym: u32) -> io::Result<Option<LabelValueTimeRange>> {
        self.inner.label_time_range(label_name_sym)
    }

    pub fn label_values(&self, label_name_sym: u32) -> io::Result<Vec<String>> {
        self.inner.label_values(label_name_sym)
    }

    pub fn label_values_with_prefix(
        &self,
        label_name_sym: u32,
        prefix: Option<&str>,
    ) -> io::Result<Vec<String>> {
        self.inner.label_values_with_prefix(label_name_sym, prefix)
    }

    pub fn exact_postings(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<Vec<u32>>> {
        self.inner.exact_postings(label_name_sym, label_value_sym)
    }

    pub fn exact_postings_metadata(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<ExactPostingsMetadata>> {
        self.inner
            .exact_postings_metadata(label_name_sym, label_value_sym)
    }

    pub(in crate::storage) fn select_exact_postings(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<ExactPostingsSelection>> {
        self.inner
            .exact_postings_selection(label_name_sym, label_value_sym)
    }

    pub(in crate::storage) fn read_exact_postings(
        &self,
        selection: ExactPostingsSelection,
    ) -> io::Result<Vec<u32>> {
        self.inner.read_exact_postings_selection(selection)
    }

    pub fn metric_series_ranges(&self, metric_sym: u32) -> io::Result<Vec<MetricSeriesRange>> {
        self.inner.metric_series_ranges(metric_sym)
    }

    pub fn metric_series_range_index(&self) -> io::Result<MetricSeriesRangeIndex> {
        self.inner.metric_series_range_index()
    }

    pub fn metric_series_ranges_byte_len(&self) -> u64 {
        self.inner.metric_series_ranges_byte_len()
    }

    pub fn routing_exact_postings_metadata(
        &self,
        label_name: &str,
        label_value: &str,
    ) -> io::Result<RoutingLookupResult> {
        self.inner
            .routing_exact_postings_metadata(label_name, label_value)
    }

    pub fn routing_index(&self) -> io::Result<Option<SegmentRoutingIndex>> {
        self.inner.routing_index()
    }

    pub fn routing_index_byte_len(&self) -> Option<u64> {
        self.inner.routing_index_byte_len()
    }

    pub fn label_value_time_range(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<LabelValueTimeRange>> {
        self.inner
            .label_value_time_range(label_name_sym, label_value_sym)
    }

    pub fn label_value_time_ranges(
        &self,
        label_name_sym: u32,
    ) -> io::Result<Option<Vec<(u32, LabelValueTimeRange)>>> {
        self.inner.label_value_time_ranges(label_name_sym)
    }

    fn materialize(&self) -> io::Result<SegmentIndexes> {
        self.inner.materialize()
    }
}

pub fn write_exact_postings_index(
    mut writer: impl Write,
    index: &ExactPostingsIndex,
) -> io::Result<()> {
    writer.write_all(&EXACT_POSTINGS_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.postings.len() as u32).to_le_bytes())?;

    for ((name, value), refs) in &index.postings {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
        writer.write_all(&(refs.len() as u32).to_le_bytes())?;
        for series_ref in refs {
            writer.write_all(&series_ref.to_le_bytes())?;
        }
    }

    Ok(())
}

pub fn read_exact_postings_index(mut reader: impl Read) -> io::Result<ExactPostingsIndex> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let mut cursor = 0usize;

    let magic = read_u32(&bytes, &mut cursor)?;
    if magic != EXACT_POSTINGS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "postings magic mismatch",
        ));
    }
    let version = read_u16(&bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported postings version",
        ));
    }
    let _flags = read_u16(&bytes, &mut cursor)?;
    let term_count = read_u32(&bytes, &mut cursor)? as usize;

    let mut index = ExactPostingsIndex::default();
    for _ in 0..term_count {
        let name = read_u32(&bytes, &mut cursor)?;
        let value = read_u32(&bytes, &mut cursor)?;
        let count = read_u32(&bytes, &mut cursor)? as usize;
        for _ in 0..count {
            let series_ref = read_u32(&bytes, &mut cursor)?;
            index.insert(name, value, series_ref);
        }
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "postings index has trailing bytes",
        ));
    }

    Ok(index)
}

pub fn write_label_value_fst_index(
    mut writer: impl Write,
    index: &LabelValueFstIndex,
) -> io::Result<()> {
    writer.write_all(&LABEL_VALUE_FST_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.fsts.len() as u32).to_le_bytes())?;

    for (name, bytes) in &index.fsts {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        writer.write_all(bytes)?;
    }

    Ok(())
}

pub fn read_label_value_fst_index(mut reader: impl Read) -> io::Result<LabelValueFstIndex> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    read_label_value_fst_index_bytes(&bytes)
}

pub fn write_label_value_time_range_index(
    mut writer: impl Write,
    index: &LabelValueTimeRangeIndex,
) -> io::Result<()> {
    writer.write_all(&LABEL_VALUE_TIME_RANGE_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.ranges.len() as u32).to_le_bytes())?;

    let mut ranges: Vec<_> = index.ranges.iter().collect();
    ranges.sort_unstable_by_key(|((name, value), _range)| (*name, *value));
    for ((name, value), range) in ranges {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
        writer.write_all(&range.min_time_ms.to_le_bytes())?;
        writer.write_all(&range.max_time_ms.to_le_bytes())?;
    }

    Ok(())
}

/// Test-fixture codec which deliberately omits cross-root validation.
///
/// Production segment writers must use [`write_segment_indexes_for_roots`].
#[cfg(test)]
pub(crate) fn write_segment_indexes_unbound_for_test(
    writer: impl Write,
    indexes: &SegmentIndexes,
) -> io::Result<()> {
    v7::write_segment_indexes_v7(writer, indexes)
}

/// Production writer entry point which proves every root-bound reference and
/// the derived routing map against the series and symbols emitted by the same
/// seal operation.
pub(crate) fn write_segment_indexes_for_roots(
    writer: impl Write,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
) -> io::Result<()> {
    indexes.validate_root_bounds(num_series, symbols)?;
    v7::write_segment_indexes_v7(writer, indexes)
}

#[cfg(test)]
pub(crate) fn write_segment_indexes_v8_for_roots_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    v8::write_segment_indexes_v8_for_roots(writer, indexes, num_series, symbols, series)
}

pub(crate) fn write_segment_indexes_v8_for_roots(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    v8::write_segment_indexes_v8_for_roots(writer, indexes, num_series, symbols, series)
}

pub(crate) fn write_segment_indexes_v9_for_roots(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    v8::write_segment_indexes_v9_for_roots(writer, indexes, num_series, symbols, series)
}

#[cfg(test)]
pub(crate) fn write_segment_indexes_v8_unbound_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    series_count: u32,
    symbol_count: u32,
) -> io::Result<()> {
    v8::write_segment_indexes_v8_unbound_for_test(writer, indexes, series_count, symbol_count)
}

#[cfg(test)]
pub(crate) fn write_segment_indexes_v9_unbound_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    series_count: u32,
    symbol_count: u32,
) -> io::Result<()> {
    v8::write_segment_indexes_v9_unbound_for_test(writer, indexes, series_count, symbol_count)
}

#[cfg(test)]
pub(crate) fn corrupt_v8_exact_postings_payload_for_test(
    bytes: &mut [u8],
    label_name_sym: u32,
    label_value_sym: u32,
) -> io::Result<()> {
    v8::corrupt_exact_postings_payload_for_test(bytes, (label_name_sym, label_value_sym))
}

#[cfg(test)]
#[allow(dead_code)]
fn write_segment_indexes_v6(mut writer: impl Write, indexes: &SegmentIndexes) -> io::Result<()> {
    writer.write_all(&SEGMENT_INDEXES_MAGIC.to_le_bytes())?;
    writer.write_all(&SEGMENT_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;

    let mut entries = Vec::new();
    let mut offset = SEGMENT_INDEX_HEADER_LEN;
    let label_time_ranges = indexes.label_value_time_ranges.label_time_ranges();

    if let Some(routing_index) = &indexes.routing_index {
        let payload = routing_index.encode()?;
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_ROUTING,
                label_name_sym: NO_LABEL_VALUE_SYM,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            },
            &payload,
        )?;
    }

    let payload = write_metric_series_ranges_blob(&indexes.metric_series_ranges)?;
    write_segment_index_blob(
        &mut writer,
        &mut entries,
        &mut offset,
        SegmentIndexDirectoryEntry {
            kind: SEGMENT_INDEX_BLOB_METRIC_SERIES_RANGES,
            label_name_sym: NO_LABEL_VALUE_SYM,
            label_value_sym: NO_LABEL_VALUE_SYM,
            offset: 0,
            len: 0,
            min_time_ms: 0,
            max_time_ms: u64::MAX,
        },
        &payload,
    )?;

    for ((name, value), refs) in &indexes.exact_postings.postings {
        let payload = write_exact_postings_blob(refs)?;
        let range = indexes
            .label_value_time_ranges
            .get(*name, *value)
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_EXACT_POSTINGS,
                label_name_sym: *name,
                label_value_sym: *value,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            &payload,
        )?;
    }

    for (name, fst_bytes) in &indexes.label_values.fsts {
        let range = label_time_ranges
            .get(name)
            .copied()
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
                label_name_sym: *name,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            fst_bytes,
        )?;
    }

    for (name, ranges) in indexes.label_value_time_ranges.ranges_by_label() {
        let payload = write_label_value_time_ranges_blob(&ranges)?;
        let range = label_time_ranges
            .get(&name)
            .copied()
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                label_name_sym: name,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            &payload,
        )?;
    }

    let footer = encode_segment_index_footer(&entries)?;
    writer.write_all(&footer)?;
    writer.write_all(&(footer.len() as u64).to_le_bytes())?;
    writer.write_all(&SEGMENT_INDEX_TRAILER_MAGIC.to_le_bytes())?;

    Ok(())
}

pub fn read_segment_indexes(mut reader: impl Read) -> io::Result<SegmentIndexes> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    SegmentIndexReader::open(std::io::Cursor::new(bytes))?.materialize()
}

#[cfg(test)]
fn write_segment_index_blob(
    writer: &mut impl Write,
    entries: &mut Vec<SegmentIndexDirectoryEntry>,
    offset: &mut u64,
    mut entry: SegmentIndexDirectoryEntry,
    payload: &[u8],
) -> io::Result<()> {
    entry.offset = *offset;
    entry.len = u64::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment index blob length exceeds u64",
        )
    })?;
    writer.write_all(payload)?;
    *offset = offset
        .checked_add(entry.len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "segment index too large"))?;
    entries.push(entry);
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
fn read_segment_indexes_v6_bytes(bytes: &[u8]) -> io::Result<SegmentIndexes> {
    let entries = parse_segment_index_directory(bytes)?;
    let mut exact_postings = ExactPostingsIndex::default();
    let mut label_values = LabelValueFstIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    let mut metric_series_ranges = None;
    let mut routing_index = None;

    for entry in entries {
        let payload = segment_index_blob_bytes(bytes, entry)?;
        match entry.kind {
            SEGMENT_INDEX_BLOB_ROUTING => {
                routing_index = Some(SegmentRoutingIndex::decode(payload)?);
            }
            SEGMENT_INDEX_BLOB_EXACT_POSTINGS => {
                for series_ref in read_exact_postings_blob(payload)? {
                    exact_postings.insert(entry.label_name_sym, entry.label_value_sym, series_ref);
                }
            }
            SEGMENT_INDEX_BLOB_LABEL_VALUE_FST => {
                label_values.insert_fst(entry.label_name_sym, payload.to_vec());
            }
            SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES => {
                for (value_sym, range) in read_label_value_time_ranges_blob(payload)? {
                    label_value_time_ranges.insert(
                        entry.label_name_sym,
                        value_sym,
                        range.min_time_ms,
                        range.max_time_ms,
                    );
                }
            }
            SEGMENT_INDEX_BLOB_METRIC_SERIES_RANGES => {
                metric_series_ranges = Some(read_metric_series_ranges_blob(payload)?);
            }
            _ => {}
        }
    }
    let metric_series_ranges = metric_series_ranges.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "required metric series ranges index blob is missing",
        )
    })?;

    Ok(SegmentIndexes {
        exact_postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges,
        routing_index,
    })
}

#[cfg(test)]
#[allow(dead_code)]
fn read_segment_index_directory(
    reader: &mut (impl Read + Seek),
) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    let len = reader.seek(SeekFrom::End(0))?;
    if len < SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index truncated",
        ));
    }

    reader.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; SEGMENT_INDEX_HEADER_LEN as usize];
    reader.read_exact(&mut header)?;
    validate_segment_index_header(&header)?;

    reader.seek(SeekFrom::End(-(SEGMENT_INDEX_TRAILER_LEN as i64)))?;
    let mut trailer = [0u8; SEGMENT_INDEX_TRAILER_LEN as usize];
    reader.read_exact(&mut trailer)?;
    let footer_len = u64::from_le_bytes(trailer[0..8].try_into().unwrap());
    let trailer_magic = u32::from_le_bytes(trailer[8..12].try_into().unwrap());
    if trailer_magic != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index trailer magic mismatch",
        ));
    }
    if footer_len > len.saturating_sub(SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length invalid",
        ));
    }

    let footer_start = len - SEGMENT_INDEX_TRAILER_LEN - footer_len;
    reader.seek(SeekFrom::Start(footer_start))?;
    let footer_len = usize::try_from(footer_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length exceeds platform usize",
        )
    })?;
    let mut footer = vec![0u8; footer_len];
    reader.read_exact(&mut footer)?;
    decode_segment_index_footer(&footer)
}

#[cfg(test)]
fn parse_segment_index_directory(bytes: &[u8]) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    if bytes.len() < (SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN) as usize {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index truncated",
        ));
    }
    validate_segment_index_header(&bytes[..SEGMENT_INDEX_HEADER_LEN as usize])?;

    let trailer_start = bytes.len() - SEGMENT_INDEX_TRAILER_LEN as usize;
    let footer_len =
        u64::from_le_bytes(bytes[trailer_start..trailer_start + 8].try_into().unwrap());
    let trailer_magic = u32::from_le_bytes(
        bytes[trailer_start + 8..trailer_start + 12]
            .try_into()
            .unwrap(),
    );
    if trailer_magic != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index trailer magic mismatch",
        ));
    }
    let footer_len = usize::try_from(footer_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length exceeds platform usize",
        )
    })?;
    if footer_len > trailer_start.saturating_sub(SEGMENT_INDEX_HEADER_LEN as usize) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length invalid",
        ));
    }
    let footer_start = trailer_start - footer_len;
    decode_segment_index_footer(&bytes[footer_start..trailer_start])
}

#[cfg(test)]
fn validate_segment_index_header(header: &[u8]) -> io::Result<()> {
    let mut cursor = 0usize;
    let magic = read_u32(header, &mut cursor)?;
    if magic != SEGMENT_INDEXES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment indexes magic mismatch",
        ));
    }
    let version = read_u16(header, &mut cursor)?;
    if version != SEGMENT_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported segment indexes version",
        ));
    }
    let _flags = read_u16(header, &mut cursor)?;
    Ok(())
}

#[cfg(test)]
fn encode_segment_index_footer(entries: &[SegmentIndexDirectoryEntry]) -> io::Result<Vec<u8>> {
    let mut footer = Vec::new();
    footer.extend_from_slice(&SEGMENT_INDEX_FOOTER_MAGIC.to_le_bytes());
    footer.extend_from_slice(&SEGMENT_INDEX_VERSION.to_le_bytes());
    footer.extend_from_slice(&0u16.to_le_bytes());
    footer.extend_from_slice(
        &(u32::try_from(entries.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment index directory entry count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    footer.extend_from_slice(&0u32.to_le_bytes());

    for entry in entries {
        footer.extend_from_slice(&entry.kind.to_le_bytes());
        footer.extend_from_slice(&0u16.to_le_bytes());
        footer.extend_from_slice(&entry.label_name_sym.to_le_bytes());
        footer.extend_from_slice(&entry.label_value_sym.to_le_bytes());
        footer.extend_from_slice(&entry.offset.to_le_bytes());
        footer.extend_from_slice(&entry.len.to_le_bytes());
        footer.extend_from_slice(&entry.min_time_ms.to_le_bytes());
        footer.extend_from_slice(&entry.max_time_ms.to_le_bytes());
    }

    Ok(footer)
}

#[cfg(test)]
fn decode_segment_index_footer(bytes: &[u8]) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    let mut cursor = 0usize;
    let magic = read_u32(bytes, &mut cursor)?;
    if magic != SEGMENT_INDEX_FOOTER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != SEGMENT_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported segment index footer version",
        ));
    }
    let _flags = read_u16(bytes, &mut cursor)?;
    let entry_count = read_u32(bytes, &mut cursor)? as usize;
    let _reserved = read_u32(bytes, &mut cursor)?;

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let kind = read_u16(bytes, &mut cursor)?;
        let _flags = read_u16(bytes, &mut cursor)?;
        let label_name_sym = read_u32(bytes, &mut cursor)?;
        let label_value_sym = read_u32(bytes, &mut cursor)?;
        let offset = read_u64(bytes, &mut cursor)?;
        let len = read_u64(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        entries.push(SegmentIndexDirectoryEntry {
            kind,
            label_name_sym,
            label_value_sym,
            offset,
            len,
            min_time_ms,
            max_time_ms,
        });
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer has trailing bytes",
        ));
    }
    Ok(entries)
}

#[cfg(test)]
fn segment_index_blob_bytes(bytes: &[u8], entry: SegmentIndexDirectoryEntry) -> io::Result<&[u8]> {
    let mut cursor = usize::try_from(entry.offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index blob offset exceeds platform usize",
        )
    })?;
    let len = usize::try_from(entry.len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index blob length exceeds platform usize",
        )
    })?;
    read_bytes(bytes, &mut cursor, len)
}

#[cfg(test)]
fn write_exact_postings_blob(refs: &[u32]) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &(u32::try_from(refs.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "postings list length exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for series_ref in refs {
        bytes.extend_from_slice(&series_ref.to_le_bytes());
    }
    Ok(bytes)
}

fn exact_postings_blob_len(refs: &[u32]) -> io::Result<u64> {
    let refs_len = u64::try_from(refs.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "postings list length exceeds u64",
        )
    })?;
    refs_len
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(4))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "postings blob too large"))
}

fn adaptive_exact_postings_blob_len(refs: &[u32]) -> io::Result<u64> {
    let raw_len = exact_postings_blob_len(refs)?;
    let first = refs.first().copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "adaptive postings list is empty",
        )
    })?;
    let mut delta_len = 4u64
        .checked_add(uleb128_u32_len(first) as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "postings blob too large"))?;
    let mut previous = first;
    for &series_ref in &refs[1..] {
        let gap = series_ref
            .checked_sub(previous)
            .filter(|gap| *gap != 0)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "adaptive postings refs are not strictly ordered and unique",
                )
            })?;
        delta_len = delta_len
            .checked_add(uleb128_u32_len(gap) as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "postings blob too large")
            })?;
        previous = series_ref;
    }
    Ok(raw_len.min(delta_len))
}

const fn uleb128_u32_len(mut value: u32) -> usize {
    let mut len = 1usize;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn u32_len(len: usize, description: &'static str) -> io::Result<u32> {
    u32::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{description} exceeds u32"),
        )
    })
}

fn routing_bucket_count(entry_count: usize) -> io::Result<usize> {
    let min_buckets = entry_count
        .checked_mul(2)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "routing index too large"))?
        .max(2);
    let bucket_count = min_buckets.checked_next_power_of_two().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "routing bucket count too large",
        )
    })?;
    u32_len(bucket_count, "routing bucket count")?;
    Ok(bucket_count)
}

fn routing_key_bytes(label_name: &str, label_value: &str) -> io::Result<Vec<u8>> {
    let capacity = 4usize
        .checked_add(label_name.len())
        .and_then(|len| len.checked_add(label_value.len()))
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "routing key length overflows")
        })?;
    u32_len(capacity, "routing key length")?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(capacity).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("routing key allocation failed: {error}"),
        )
    })?;
    bytes.extend_from_slice(&u32_len(label_name.len(), "routing label name length")?.to_le_bytes());
    bytes.extend_from_slice(label_name.as_bytes());
    bytes.extend_from_slice(label_value.as_bytes());
    Ok(bytes)
}

fn routing_key_parts(bytes: &[u8]) -> io::Result<(&str, &str)> {
    let mut cursor = 0usize;
    let name_len = read_u32(bytes, &mut cursor)? as usize;
    let name = read_bytes(bytes, &mut cursor, name_len)?;
    let value_len = bytes.len().saturating_sub(cursor);
    let value = read_bytes(bytes, &mut cursor, value_len)?;
    let name = std::str::from_utf8(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "routing label name is not valid utf-8",
        )
    })?;
    let value = std::str::from_utf8(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "routing label value is not valid utf-8",
        )
    })?;
    Ok((name, value))
}

fn routing_key_hash(bytes: &[u8]) -> u64 {
    routing_hash_parts(std::iter::once(bytes))
}

fn routing_key_hash_parts(label_name: &str, label_value: &str) -> io::Result<u64> {
    let name_len = u32_len(label_name.len(), "routing label name length")?;
    let key_len = 4usize
        .checked_add(label_name.len())
        .and_then(|len| len.checked_add(label_value.len()))
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "routing key length overflows")
        })?;
    u32_len(key_len, "routing key length")?;
    let encoded_name_len = name_len.to_le_bytes();
    Ok(routing_hash_parts([
        encoded_name_len.as_slice(),
        label_name.as_bytes(),
        label_value.as_bytes(),
    ]))
}

fn routing_hash_parts<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;

    let mut hash = FNV_OFFSET_BASIS;
    for part in parts {
        for byte in part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

#[cfg(test)]
fn read_exact_postings_blob(bytes: &[u8]) -> io::Result<Vec<u32>> {
    let mut cursor = 0usize;
    let count = read_u32(bytes, &mut cursor)? as usize;
    let mut refs = Vec::with_capacity(count);
    for _ in 0..count {
        refs.push(read_u32(bytes, &mut cursor)?);
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "exact postings blob has trailing bytes",
        ));
    }
    Ok(refs)
}

#[cfg(test)]
fn write_label_value_time_ranges_blob(
    ranges: &[(u32, LabelValueTimeRange)],
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &(u32::try_from(ranges.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "label value time range count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for (value_sym, range) in ranges {
        bytes.extend_from_slice(&value_sym.to_le_bytes());
        bytes.extend_from_slice(&range.min_time_ms.to_le_bytes());
        bytes.extend_from_slice(&range.max_time_ms.to_le_bytes());
    }
    Ok(bytes)
}

fn read_label_value_time_ranges_blob(bytes: &[u8]) -> io::Result<Vec<(u32, LabelValueTimeRange)>> {
    if bytes.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time ranges payload is shorter than its count",
        ));
    }
    let mut cursor = 0usize;
    let count = usize::try_from(read_u32(bytes, &mut cursor)?).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range count exceeds platform usize",
        )
    })?;
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range payload has no records",
        ));
    }
    let expected_len = count
        .checked_mul(20)
        .and_then(|len| len.checked_add(4))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "label value time range count overflows its payload length",
            )
        })?;
    if expected_len != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range count does not match payload length",
        ));
    }
    let mut ranges = Vec::new();
    ranges
        .try_reserve_exact(count)
        .map_err(|_| io::Error::other("label value time range allocation failed"))?;
    let mut previous_value_sym = None;
    for _ in 0..count {
        let value_sym = read_u32(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        if previous_value_sym.is_some_and(|previous| previous >= value_sym) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label value time ranges are not strictly ordered and unique",
            ));
        }
        if min_time_ms > max_time_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label value time range is reversed",
            ));
        }
        ranges.push((
            value_sym,
            LabelValueTimeRange {
                min_time_ms,
                max_time_ms,
            },
        ));
        previous_value_sym = Some(value_sym);
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time ranges blob has trailing bytes",
        ));
    }
    Ok(ranges)
}

fn write_metric_series_ranges_blob(index: &MetricSeriesRangeIndex) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(
        &(u32::try_from(index.ranges.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "metric range count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for (metric_sym, ranges) in index.entries() {
        bytes.extend_from_slice(&metric_sym.to_le_bytes());
        // We keep range_count because it costs little and keeps the format robust if a
        // future writer splits the same metric by kind or lane.
        bytes.extend_from_slice(
            &(u32::try_from(ranges.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "metric series range count exceeds u32",
                )
            })?)
            .to_le_bytes(),
        );
        for range in ranges {
            bytes.extend_from_slice(&range.start_series_ref.to_le_bytes());
            bytes.extend_from_slice(&range.series_count.to_le_bytes());
            bytes.extend_from_slice(&range.kind_mask.to_le_bytes());
            bytes.extend_from_slice(&0u16.to_le_bytes());
            bytes.extend_from_slice(&range.min_time_ms.to_le_bytes());
            bytes.extend_from_slice(&range.max_time_ms.to_le_bytes());
        }
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy)]
pub(in crate::storage::index) enum MetricSeriesRangeBlobEvent {
    Header {
        metric_count: usize,
    },
    Group {
        metric_sym: u32,
        range_count: usize,
        ranges_offset: usize,
    },
    Range {
        metric_sym: u32,
        range: MetricSeriesRange,
    },
}

#[derive(Debug, Clone, Copy)]
pub(in crate::storage::index) struct MetricSeriesRangeBlobBounds {
    pub(in crate::storage::index) num_series: u32,
    pub(in crate::storage::index) symbol_count: u32,
}

pub(in crate::storage::index) fn walk_metric_series_ranges_blob(
    bytes: &[u8],
    bounds: Option<MetricSeriesRangeBlobBounds>,
    mut visitor: impl FnMut(MetricSeriesRangeBlobEvent) -> io::Result<()>,
) -> io::Result<()> {
    let mut cursor = 0usize;
    let magic = read_u32(bytes, &mut cursor)?;
    if magic != METRIC_SERIES_RANGES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != METRIC_SERIES_RANGES_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported metric series ranges version",
        ));
    }
    let flags = read_u16(bytes, &mut cursor)?;
    if flags != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges flags are non-zero",
        ));
    }
    let metric_count = read_u32(bytes, &mut cursor)? as usize;
    if metric_count > bytes.len().saturating_sub(cursor) / (8 + METRIC_SERIES_RANGE_RECORD_LEN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series range metric count exceeds the minimum remaining group bytes",
        ));
    }
    visitor(MetricSeriesRangeBlobEvent::Header { metric_count })?;
    let mut previous_metric_sym = None;
    let mut next_series_ref = 0u64;
    for _ in 0..metric_count {
        let metric_sym = read_u32(bytes, &mut cursor)?;
        if previous_metric_sym.is_some_and(|previous| metric_sym <= previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range metric symbols are not strictly increasing",
            ));
        }
        if bounds.is_some_and(|bounds| metric_sym >= bounds.symbol_count) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range symbol exceeds the authoritative symbol count",
            ));
        }
        previous_metric_sym = Some(metric_sym);
        let range_count = read_u32(bytes, &mut cursor)? as usize;
        if range_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range metric has no ranges",
            ));
        }
        let range_bytes = range_count
            .checked_mul(METRIC_SERIES_RANGE_RECORD_LEN)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range count overflows",
                )
            })?;
        if range_bytes > bytes.len().saturating_sub(cursor) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range count exceeds remaining bytes",
            ));
        }
        visitor(MetricSeriesRangeBlobEvent::Group {
            metric_sym,
            range_count,
            ranges_offset: cursor,
        })?;
        let mut previous_series_end = None;
        for _ in 0..range_count {
            let start_series_ref = read_u32(bytes, &mut cursor)?;
            let series_count = read_u32(bytes, &mut cursor)?;
            let kind_mask = read_u16(bytes, &mut cursor)?;
            let reserved = read_u16(bytes, &mut cursor)?;
            let min_time_ms = read_u64(bytes, &mut cursor)?;
            let max_time_ms = read_u64(bytes, &mut cursor)?;
            if reserved != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range reserved field is non-zero",
                ));
            }
            if series_count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range series count is zero",
                ));
            }
            let series_end = u64::from(start_series_ref)
                .checked_add(u64::from(series_count))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "metric series range series end overflows",
                    )
                })?;
            if series_end > u64::from(u32::MAX) + 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range series end exceeds the u32 domain",
                ));
            }
            if bounds.is_some_and(|bounds| series_end > u64::from(bounds.num_series)) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range exceeds the bound series count",
                ));
            }
            if bounds.is_some() && u64::from(start_series_ref) != next_series_ref {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series ranges do not form a canonical complete partition",
                ));
            }
            if previous_series_end.is_some_and(|previous| u64::from(start_series_ref) < previous) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series ranges are unordered or overlapping",
                ));
            }
            previous_series_end = Some(series_end);
            if min_time_ms > max_time_ms {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range time bounds are reversed",
                ));
            }
            if kind_mask == 0 || kind_mask & !VALID_METRIC_SERIES_KIND_MASK != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range kind mask is zero or contains unknown bits",
                ));
            }
            next_series_ref = series_end;
            visitor(MetricSeriesRangeBlobEvent::Range {
                metric_sym,
                range: MetricSeriesRange {
                    start_series_ref,
                    series_count,
                    kind_mask,
                    min_time_ms,
                    max_time_ms,
                },
            })?;
        }
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges blob has trailing bytes",
        ));
    }
    if bounds.is_some_and(|bounds| next_series_ref != u64::from(bounds.num_series)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges do not cover the authoritative series count",
        ));
    }
    Ok(())
}

fn read_metric_series_ranges_blob(bytes: &[u8]) -> io::Result<MetricSeriesRangeIndex> {
    let mut index = MetricSeriesRangeIndex::default();
    walk_metric_series_ranges_blob(bytes, None, |event| {
        match event {
            MetricSeriesRangeBlobEvent::Header { .. } => {}
            MetricSeriesRangeBlobEvent::Group {
                metric_sym,
                range_count,
                ..
            } => {
                let mut ranges = Vec::new();
                ranges
                    .try_reserve_exact(range_count)
                    .map_err(|_| io::Error::other("metric series range allocation failed"))?;
                index.ranges.insert(metric_sym, ranges);
            }
            MetricSeriesRangeBlobEvent::Range { metric_sym, range } => {
                index
                    .ranges
                    .get_mut(&metric_sym)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "metric series range group state is missing",
                        )
                    })?
                    .push(range);
            }
        }
        Ok(())
    })?;
    Ok(index)
}

fn read_fst_values(bytes: &[u8]) -> io::Result<Vec<String>> {
    read_fst_values_with_prefix(bytes, None)
}

fn read_fst_values_with_prefix(bytes: &[u8], prefix: Option<&str>) -> io::Result<Vec<String>> {
    let set = Set::new(bytes).map_err(fst_io_error)?;
    if set.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value FST contains no values",
        ));
    }
    let mut stream = match prefix {
        Some(prefix) if !prefix.is_empty() => {
            let mut builder = set.range().ge(prefix);
            if let Some(upper) = prefix_upper_bound(prefix.as_bytes()) {
                builder = builder.lt(upper);
            }
            builder.into_stream()
        }
        Some(_) | None => set.stream(),
    };
    let mut values = Vec::new();
    while let Some(value) = stream.next() {
        let value = std::str::from_utf8(value).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid utf8 fst value: {err}"),
            )
        })?;
        values.push(value.to_string());
    }
    Ok(values)
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    for index in (0..bound.len()).rev() {
        if bound[index] == u8::MAX {
            continue;
        }
        bound[index] = bound[index].saturating_add(1);
        bound.truncate(index + 1);
        return Some(bound);
    }
    None
}

fn read_label_value_fst_index_bytes(bytes: &[u8]) -> io::Result<LabelValueFstIndex> {
    let mut cursor = 0usize;

    let magic = read_u32(bytes, &mut cursor)?;
    if magic != LABEL_VALUE_FST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value index magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported label value index version",
        ));
    }
    let _flags = read_u16(bytes, &mut cursor)?;
    let label_count = read_u32(bytes, &mut cursor)? as usize;

    let mut index = LabelValueFstIndex::default();
    for _ in 0..label_count {
        let name = read_u32(bytes, &mut cursor)?;
        let fst_len = read_u32(bytes, &mut cursor)? as usize;
        index.insert_fst(name, read_bytes(bytes, &mut cursor, fst_len)?.to_vec());
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value index has trailing bytes",
        ));
    }

    Ok(index)
}

fn fst_io_error(err: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> io::Result<u16> {
    if cursor.saturating_add(2) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u16::from_le_bytes(bytes[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    if cursor.saturating_add(4) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    if cursor.saturating_add(8) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    if cursor.saturating_add(len) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let out = &bytes[*cursor..*cursor + len];
    *cursor += len;
    Ok(out)
}

fn read_bytes_at(bytes: &[u8], offset: u64, len: usize) -> io::Result<&[u8]> {
    let offset = usize::try_from(offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "byte slice offset exceeds platform usize",
        )
    })?;
    if offset.saturating_add(len) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    Ok(&bytes[offset..offset + len])
}
