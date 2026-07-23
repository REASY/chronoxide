use super::*;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExactPostingsIndex {
    pub(in crate::storage::index) postings: BTreeMap<(u32, u32), Vec<u32>>,
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
    pub(in crate::storage::index) fsts: BTreeMap<u32, Vec<u8>>,
}

impl LabelValueFstIndex {
    pub fn from_series(series: &[SeriesEntry], symbols: &SegmentSymbols) -> io::Result<Self> {
        let mut values: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for entry in series {
            for (name, value) in &entry.labels {
                values.entry(*name).or_default().push(*value);
            }
        }

        Self::from_symbol_values(values, symbols)
    }

    pub(in crate::storage) fn from_exact_postings(
        postings: &ExactPostingsIndex,
        symbols: &SegmentSymbols,
    ) -> io::Result<Self> {
        let mut values: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for (name, value, _) in postings.entries() {
            values.entry(name).or_default().push(value);
        }

        Self::from_symbol_values(values, symbols)
    }

    fn from_symbol_values(
        values: BTreeMap<u32, Vec<u32>>,
        symbols: &SegmentSymbols,
    ) -> io::Result<Self> {
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
    pub(in crate::storage::index) fn new(
        metadata: ExactPostingsMetadata,
        postings_offset: u64,
        postings_len: u64,
    ) -> Self {
        Self {
            metadata,
            postings_offset,
            postings_len,
        }
    }

    pub(in crate::storage) fn metadata(self) -> ExactPostingsMetadata {
        self.metadata
    }

    pub(in crate::storage::index) fn postings(self) -> (u64, u64) {
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
    pub(in crate::storage::index) ranges: HashMap<(u32, u32), LabelValueTimeRange>,
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
    pub(in crate::storage::index) fn label_time_ranges(
        &self,
    ) -> BTreeMap<u32, LabelValueTimeRange> {
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
    pub(in crate::storage::index) fn ranges_by_label(
        &self,
    ) -> BTreeMap<u32, Vec<(u32, LabelValueTimeRange)>> {
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
    pub(in crate::storage::index) ranges: BTreeMap<u32, Vec<MetricSeriesRange>>,
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

    pub(in crate::storage::index) fn entries(
        &self,
    ) -> impl Iterator<Item = (u32, &[MetricSeriesRange])> {
        self.ranges
            .iter()
            .map(|(metric_sym, ranges)| (*metric_sym, ranges.as_slice()))
    }
}

pub(in crate::storage::index) fn validate_metric_series_range_sequence(
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SegmentIndexes {
    pub exact_postings: ExactPostingsIndex,
    pub label_values: LabelValueFstIndex,
    pub label_value_time_ranges: LabelValueTimeRangeIndex,
    pub metric_series_ranges: MetricSeriesRangeIndex,
    pub routing_index: Option<SegmentRoutingIndex>,
}

impl SegmentIndexes {
    pub(in crate::storage::index) fn validate_root_bounds(
        &self,
        num_series: u32,
        symbols: &SegmentSymbols,
    ) -> io::Result<()> {
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
