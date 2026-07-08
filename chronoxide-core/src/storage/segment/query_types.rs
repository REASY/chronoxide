use super::*;

pub struct SegmentReader {
    pub(super) dir: PathBuf,
    pub(super) meta: SegmentMeta,
    pub(super) query_cache: Arc<SegmentReaderQueryCache>,
}

#[derive(Default)]
pub(super) struct SegmentReaderQueryCache {
    pub(super) index_reader: Mutex<Option<SegmentIndexReader<File>>>,
    pub(super) symbols: Mutex<Option<Arc<SegmentSymbols>>>,
    pub(super) metric_series_ranges: Mutex<Option<Arc<MetricSeriesRangeIndex>>>,
    pub(super) series_locators: Mutex<HashMap<u32, Arc<SeriesEntryLocator>>>,
    pub(super) series_metadata: Mutex<HashMap<u32, Arc<SeriesEntryMetadata>>>,
    pub(super) series_entries: Mutex<HashMap<u32, Arc<SeriesEntry>>>,
    pub(super) chunk_entries: Mutex<HashMap<ChunkIndexRange, Arc<Vec<ChunkIndexEntry>>>>,
}

pub(super) struct CachedIndexReader {
    pub(super) reader: SegmentIndexReader<File>,
    pub(super) cache_hit: bool,
    pub(super) file_bytes: u64,
    pub(super) open_elapsed: Duration,
}

pub(super) struct CachedSymbols {
    pub(super) symbols: Arc<SegmentSymbols>,
    pub(super) cache_hit: bool,
    pub(super) file_bytes: u64,
    pub(super) open_elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentQueryResult {
    pub series_id: u64,
    pub labels: QueryLabels,
    pub samples: Vec<(u64, f64)>,
    pub counter_reset_hints: Vec<CounterResetHint>,
    pub(crate) temporality: QueryResultTemporality,
}

impl SegmentQueryResult {
    pub(crate) fn new(series_id: u64, labels: Vec<(String, String)>) -> Self {
        Self {
            series_id,
            labels: shared_query_labels(labels),
            samples: Vec::new(),
            counter_reset_hints: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
        }
    }

    pub(crate) fn with_shared_labels(series_id: u64, labels: QueryLabels) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
            counter_reset_hints: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
        }
    }

    pub(crate) fn with_samples(
        series_id: u64,
        labels: Vec<(String, String)>,
        samples: Vec<(u64, f64)>,
    ) -> Self {
        Self::with_shared_samples(series_id, shared_query_labels(labels), samples)
    }

    pub(crate) fn with_shared_samples(
        series_id: u64,
        labels: QueryLabels,
        samples: Vec<(u64, f64)>,
    ) -> Self {
        Self {
            series_id,
            labels,
            samples,
            counter_reset_hints: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
        }
    }

    pub(crate) fn push_sample(&mut self, timestamp_ms: u64, value: f64) {
        if self.has_counter_reset_hints() {
            self.counter_reset_hints.push(CounterResetHint::Unknown);
        } else {
            self.counter_reset_hints.clear();
        }
        self.samples.push((timestamp_ms, value));
    }

    pub(crate) fn push_sample_with_counter_reset_hint(
        &mut self,
        timestamp_ms: u64,
        value: f64,
        reset_hint: CounterResetHint,
    ) {
        self.ensure_counter_reset_hints();
        self.samples.push((timestamp_ms, value));
        self.counter_reset_hints.push(reset_hint);
    }

    pub(crate) fn push_sample_with_counter_reset_hint_and_temporality(
        &mut self,
        timestamp_ms: u64,
        value: f64,
        reset_hint: CounterResetHint,
        temporality: OtlpAggregationTemporality,
    ) {
        self.push_sample_with_counter_reset_hint(timestamp_ms, value, reset_hint);
        self.observe_temporality(QueryResultTemporality::from(temporality));
    }

    pub(crate) fn extend_from(&mut self, mut other: SegmentQueryResult) {
        let self_samples = self.samples.len();
        let other_samples = other.samples.len();
        if other.has_counter_reset_hints() {
            self.ensure_counter_reset_hints();
            self.counter_reset_hints
                .append(&mut other.counter_reset_hints);
        } else if self.has_counter_reset_hints() {
            self.counter_reset_hints.extend(std::iter::repeat_n(
                CounterResetHint::Unknown,
                other.samples.len(),
            ));
        } else {
            self.counter_reset_hints.clear();
        }
        self.samples.append(&mut other.samples);
        self.temporality = merge_result_temporality(
            self.temporality,
            self_samples,
            other.temporality,
            other_samples,
        );
    }

    pub(crate) fn dedupe_samples_keep_last(&mut self) {
        let has_hints = self.has_counter_reset_hints();
        if !has_hints {
            self.counter_reset_hints.clear();
        }

        if self.samples.len() < 2 {
            return;
        }

        match sample_timestamp_order(&self.samples) {
            SampleTimestampOrder::StrictlyIncreasing => return,
            SampleTimestampOrder::SortedWithDuplicates => {
                self.compact_sorted_samples_keep_last(has_hints);
                return;
            }
            SampleTimestampOrder::Unsorted => {}
        }

        self.sort_and_dedupe_samples_keep_last(has_hints);
    }

    fn compact_sorted_samples_keep_last(&mut self, has_hints: bool) {
        let mut write_idx = 0;
        let mut read_idx = 0;
        while read_idx < self.samples.len() {
            let timestamp_ms = self.samples[read_idx].0;
            let mut last_idx = read_idx;
            read_idx += 1;
            while read_idx < self.samples.len() && self.samples[read_idx].0 == timestamp_ms {
                last_idx = read_idx;
                read_idx += 1;
            }

            if write_idx != last_idx {
                self.samples[write_idx] = self.samples[last_idx];
                if has_hints {
                    self.counter_reset_hints[write_idx] = self.counter_reset_hints[last_idx];
                }
            }
            write_idx += 1;
        }

        self.samples.truncate(write_idx);
        if has_hints {
            self.counter_reset_hints.truncate(write_idx);
        } else {
            self.counter_reset_hints.clear();
        }
    }

    fn sort_and_dedupe_samples_keep_last(&mut self, has_hints: bool) {
        if has_hints {
            let mut rows: Vec<_> = self
                .samples
                .drain(..)
                .zip(self.counter_reset_hints.drain(..))
                .collect();
            rows.sort_by_key(|((timestamp_ms, _), _)| *timestamp_ms);

            for (sample, reset_hint) in rows {
                if self
                    .samples
                    .last()
                    .is_some_and(|(timestamp_ms, _)| *timestamp_ms == sample.0)
                {
                    *self.samples.last_mut().expect("last sample exists") = sample;
                    *self
                        .counter_reset_hints
                        .last_mut()
                        .expect("last reset hint exists") = reset_hint;
                } else {
                    self.samples.push(sample);
                    self.counter_reset_hints.push(reset_hint);
                }
            }
        } else {
            self.samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
            self.compact_sorted_samples_keep_last(false);
        }
    }

    pub(crate) fn counter_reset_hints(&self) -> Option<&[CounterResetHint]> {
        self.has_counter_reset_hints()
            .then_some(self.counter_reset_hints.as_slice())
    }

    fn ensure_counter_reset_hints(&mut self) {
        if !self.has_counter_reset_hints() {
            self.counter_reset_hints = vec![CounterResetHint::Unknown; self.samples.len()];
        }
    }

    fn has_counter_reset_hints(&self) -> bool {
        !self.counter_reset_hints.is_empty() && self.counter_reset_hints.len() == self.samples.len()
    }

    fn observe_temporality(&mut self, temporality: QueryResultTemporality) {
        self.temporality = merge_result_temporality(
            self.temporality,
            self.samples.len().saturating_sub(1),
            temporality,
            1,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryResultTemporality {
    Unknown,
    Cumulative,
    Delta,
    Mixed,
}

impl From<OtlpAggregationTemporality> for QueryResultTemporality {
    fn from(value: OtlpAggregationTemporality) -> Self {
        match value {
            OtlpAggregationTemporality::Delta => Self::Delta,
            OtlpAggregationTemporality::Cumulative => Self::Cumulative,
            OtlpAggregationTemporality::Unspecified => Self::Unknown,
        }
    }
}

fn merge_result_temporality(
    left: QueryResultTemporality,
    left_samples: usize,
    right: QueryResultTemporality,
    right_samples: usize,
) -> QueryResultTemporality {
    if left_samples == 0 {
        return right;
    }
    if right_samples == 0 || left == right {
        return left;
    }
    if left == QueryResultTemporality::Unknown || right == QueryResultTemporality::Unknown {
        return QueryResultTemporality::Unknown;
    }
    QueryResultTemporality::Mixed
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromqlHistogramSeries {
    pub(crate) series_id: u64,
    pub(crate) labels: QueryLabels,
    pub(crate) samples: Vec<PromqlHistogramSample>,
}

impl PromqlHistogramSeries {
    pub(crate) fn new(series_id: u64, labels: QueryLabels) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
        }
    }

    pub(crate) fn push_sample(&mut self, sample: PromqlHistogramSample) {
        self.samples.push(sample);
    }

    pub(crate) fn extend_from(&mut self, mut other: PromqlHistogramSeries) {
        self.samples.append(&mut other.samples);
    }

    pub(crate) fn dedupe_samples_keep_last(&mut self) {
        if self.samples.len() < 2 {
            return;
        }
        self.samples.sort_by_key(|sample| sample.timestamp_ms);

        let mut write_idx = 0;
        let mut read_idx = 0;
        while read_idx < self.samples.len() {
            let timestamp_ms = self.samples[read_idx].timestamp_ms;
            let mut last_idx = read_idx;
            read_idx += 1;
            while read_idx < self.samples.len()
                && self.samples[read_idx].timestamp_ms == timestamp_ms
            {
                last_idx = read_idx;
                read_idx += 1;
            }

            if write_idx != last_idx {
                self.samples[write_idx] = self.samples[last_idx].clone();
            }
            write_idx += 1;
        }
        self.samples.truncate(write_idx);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromqlHistogramSample {
    pub(crate) timestamp_ms: u64,
    pub(crate) count: f64,
    pub(crate) sum: Option<f64>,
    pub(crate) explicit_bounds: Arc<[f64]>,
    pub(crate) bucket_counts: Vec<f64>,
    pub(crate) temporality: OtlpAggregationTemporality,
    pub(crate) reset_hint: CounterResetHint,
    pub(crate) stale: bool,
}

impl PromqlHistogramSample {
    pub(crate) fn from_histogram_value(timestamp_ms: u64, value: HistogramValue) -> Self {
        let stale = value.metadata.is_stale();
        Self {
            timestamp_ms,
            count: value.count as f64,
            sum: value.sum,
            explicit_bounds: Arc::from(value.explicit_bounds.into_boxed_slice()),
            bucket_counts: value
                .bucket_counts
                .into_iter()
                .map(|count| count as f64)
                .collect(),
            temporality: value.metadata.temporality,
            reset_hint: value.metadata.reset_hint,
            stale,
        }
    }
}

pub(crate) fn merge_histogram_query_results(
    mut results: Vec<PromqlHistogramSeries>,
) -> Vec<PromqlHistogramSeries> {
    if results.len() < 2 {
        for result in &mut results {
            result.dedupe_samples_keep_last();
        }
        return results;
    }

    results.sort_by_key(|result| result.series_id);
    let mut merged = Vec::<PromqlHistogramSeries>::with_capacity(results.len());
    for result in results {
        if let Some(last) = merged.last_mut()
            && last.series_id == result.series_id
        {
            last.extend_from(result);
            continue;
        }
        merged.push(result);
    }
    for result in &mut merged {
        result.dedupe_samples_keep_last();
    }
    merged
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromqlExponentialHistogramSeries {
    pub(crate) series_id: u64,
    pub(crate) labels: QueryLabels,
    pub(crate) samples: Vec<PromqlExponentialHistogramSample>,
}

impl PromqlExponentialHistogramSeries {
    pub(crate) fn new(series_id: u64, labels: QueryLabels) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
        }
    }

    pub(crate) fn push_sample(&mut self, sample: PromqlExponentialHistogramSample) {
        self.samples.push(sample);
    }

    pub(crate) fn extend_from(&mut self, mut other: PromqlExponentialHistogramSeries) {
        self.samples.append(&mut other.samples);
    }

    pub(crate) fn dedupe_samples_keep_last(&mut self) {
        if self.samples.len() < 2 {
            return;
        }
        self.samples.sort_by_key(|sample| sample.timestamp_ms);

        let mut write_idx = 0;
        let mut read_idx = 0;
        while read_idx < self.samples.len() {
            let timestamp_ms = self.samples[read_idx].timestamp_ms;
            let mut last_idx = read_idx;
            read_idx += 1;
            while read_idx < self.samples.len()
                && self.samples[read_idx].timestamp_ms == timestamp_ms
            {
                last_idx = read_idx;
                read_idx += 1;
            }

            if write_idx != last_idx {
                self.samples[write_idx] = self.samples[last_idx].clone();
            }
            write_idx += 1;
        }
        self.samples.truncate(write_idx);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromqlExponentialHistogramSample {
    pub(crate) timestamp_ms: u64,
    pub(crate) count: f64,
    pub(crate) sum: Option<f64>,
    pub(crate) scale: i32,
    pub(crate) zero_threshold: f64,
    pub(crate) zero_count: f64,
    pub(crate) positive: PromqlExponentialHistogramBuckets,
    pub(crate) negative: PromqlExponentialHistogramBuckets,
    pub(crate) temporality: OtlpAggregationTemporality,
    pub(crate) reset_hint: CounterResetHint,
    pub(crate) stale: bool,
}

impl PromqlExponentialHistogramSample {
    pub(crate) fn from_exponential_histogram_value(
        timestamp_ms: u64,
        value: ExponentialHistogramValue,
    ) -> Self {
        let stale = value.metadata.is_stale();
        Self {
            timestamp_ms,
            count: value.count as f64,
            sum: value.sum,
            scale: value.scale,
            zero_threshold: value.zero_threshold,
            zero_count: value.zero_count as f64,
            positive: PromqlExponentialHistogramBuckets::from(value.positive),
            negative: PromqlExponentialHistogramBuckets::from(value.negative),
            temporality: value.metadata.temporality,
            reset_hint: value.metadata.reset_hint,
            stale,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromqlExponentialHistogramBuckets {
    pub(crate) offset: i32,
    pub(crate) counts: Vec<f64>,
}

impl From<ExponentialHistogramBuckets> for PromqlExponentialHistogramBuckets {
    fn from(value: ExponentialHistogramBuckets) -> Self {
        Self {
            offset: value.offset,
            counts: value.counts.into_iter().map(|count| count as f64).collect(),
        }
    }
}

pub(crate) fn merge_exponential_histogram_query_results(
    mut results: Vec<PromqlExponentialHistogramSeries>,
) -> Vec<PromqlExponentialHistogramSeries> {
    if results.len() < 2 {
        for result in &mut results {
            result.dedupe_samples_keep_last();
        }
        return results;
    }

    results.sort_by_key(|result| result.series_id);
    let mut merged = Vec::<PromqlExponentialHistogramSeries>::with_capacity(results.len());
    for result in results {
        if let Some(last) = merged.last_mut()
            && last.series_id == result.series_id
        {
            last.extend_from(result);
            continue;
        }
        merged.push(result);
    }
    for result in &mut merged {
        result.dedupe_samples_keep_last();
    }
    merged
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleTimestampOrder {
    StrictlyIncreasing,
    SortedWithDuplicates,
    Unsorted,
}

fn sample_timestamp_order(samples: &[(u64, f64)]) -> SampleTimestampOrder {
    match samples.windows(2).try_fold(false, |has_duplicate, window| {
        let previous = window[0].0;
        let current = window[1].0;
        if previous > current {
            Err(())
        } else {
            Ok(has_duplicate || previous == current)
        }
    }) {
        Ok(false) => SampleTimestampOrder::StrictlyIncreasing,
        Ok(true) => SampleTimestampOrder::SortedWithDuplicates,
        Err(()) => SampleTimestampOrder::Unsorted,
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct QueryLabels(Arc<[(String, String)]>);

impl QueryLabels {
    pub(crate) fn from_vec(labels: Vec<(String, String)>) -> Self {
        Self(Arc::from(labels.into_boxed_slice()))
    }

    pub fn as_slice(&self) -> &[(String, String)] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl AsRef<[(String, String)]> for QueryLabels {
    fn as_ref(&self) -> &[(String, String)] {
        self.as_slice()
    }
}

impl std::ops::Deref for QueryLabels {
    type Target = [(String, String)];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl PartialEq<Vec<(String, String)>> for QueryLabels {
    fn eq(&self, other: &Vec<(String, String)>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<QueryLabels> for Vec<(String, String)> {
    fn eq(&self, other: &QueryLabels) -> bool {
        self.as_slice() == other.as_slice()
    }
}

pub(crate) fn shared_query_labels(labels: Vec<(String, String)>) -> QueryLabels {
    QueryLabels::from_vec(labels)
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryExecution {
    pub results: Vec<SegmentQueryResult>,
    pub stats: QueryStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryDataPrefetchStats {
    pub query_stats: QueryStats,
    pub series_entries_read: u64,
    pub chunk_index_reads: u64,
    pub chunk_index_bytes_read: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStats {
    pub segments_considered: u64,
    pub segments_skipped_by_time: u64,
    pub segments_skipped_by_missing_equality: u64,
    pub segments_skipped_by_matcher_time_range: u64,
    pub segments_queried: u64,
    pub matched_series: u64,
    pub projected_series: u64,
    pub chunk_reads: u64,
    pub bytes_read: u64,
    pub samples_decoded: u64,
    pub typed_scalar_chunks_decoded: u64,
    pub typed_full_chunks_decoded: u64,
    pub regex_values_examined: u64,
    pub index_postings_reads: u64,
    pub index_postings_bytes_read: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_matched_series: Option<u64>,
    pub max_projected_series: Option<u64>,
    pub max_chunk_reads: Option<u64>,
    pub max_bytes_read: Option<u64>,
    pub max_samples_decoded: Option<u64>,
    pub max_regex_values_examined: Option<u64>,
}

pub const PRODUCTION_QUERY_MAX_SERIES_MATCHED: u64 = 1_000_000;
pub const PRODUCTION_QUERY_MAX_PROJECTED_SERIES: u64 = 2_000_000;
pub const PRODUCTION_QUERY_MAX_CHUNKS_READ: u64 = 5_000_000;
pub const PRODUCTION_QUERY_MAX_BYTES_READ: u64 = 2 * 1024 * 1024 * 1024;
pub const PRODUCTION_QUERY_MAX_SAMPLES: u64 = 50_000_000;
pub const PRODUCTION_REGEX_MAX_EXPANDED_VALUES: u64 = 100_000;

impl QueryLimits {
    pub const fn unlimited() -> Self {
        Self {
            max_matched_series: None,
            max_projected_series: None,
            max_chunk_reads: None,
            max_bytes_read: None,
            max_samples_decoded: None,
            max_regex_values_examined: None,
        }
    }

    pub const fn production_default() -> Self {
        Self {
            max_matched_series: Some(PRODUCTION_QUERY_MAX_SERIES_MATCHED),
            max_projected_series: Some(PRODUCTION_QUERY_MAX_PROJECTED_SERIES),
            max_chunk_reads: Some(PRODUCTION_QUERY_MAX_CHUNKS_READ),
            max_bytes_read: Some(PRODUCTION_QUERY_MAX_BYTES_READ),
            max_samples_decoded: Some(PRODUCTION_QUERY_MAX_SAMPLES),
            max_regex_values_examined: Some(PRODUCTION_REGEX_MAX_EXPANDED_VALUES),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeReport {
    pub totals: SegmentStoreSmokeTotals,
    pub sample_series: Vec<SegmentStoreSmokeSeries>,
    pub queries: Vec<SegmentStoreSmokeQuery>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeTotals {
    pub segments: u64,
    pub datapoints: u64,
    pub series: u64,
    pub chunks: u64,
    pub chunk_bytes: u64,
    pub by_kind: SegmentStoreSmokeKindTotals,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeKindTotals {
    pub float: SegmentStoreSmokeKindStats,
    pub int64: SegmentStoreSmokeKindStats,
    pub histogram: SegmentStoreSmokeKindStats,
    pub exponential_histogram: SegmentStoreSmokeKindStats,
    pub summary: SegmentStoreSmokeKindStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeKindStats {
    pub chunks: u64,
    pub chunk_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStoreSmokeSeries {
    pub segment_id: String,
    pub series_ref: u32,
    pub series_id: u64,
    pub kind: ChunkKind,
    pub labels: Vec<(String, String)>,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub samples: u64,
    pub chunk_bytes: u64,
    pub bucket_le: Option<String>,
    pub quantile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStoreSmokeQuery {
    pub kind: ChunkKind,
    pub query: String,
    pub result_series: u64,
    pub result_samples: u64,
    pub matched_series: u64,
    pub projected_series: u64,
    pub chunk_reads: u64,
    pub bytes_read: u64,
    pub samples_decoded: u64,
    pub typed_scalar_chunks_decoded: u64,
    pub typed_full_chunks_decoded: u64,
}

impl SegmentStoreSmokeKindTotals {
    pub(super) fn add_chunk(&mut self, kind: ChunkKind, bytes: u64) {
        let stats = self.stats_mut(kind);
        stats.chunks = stats.chunks.saturating_add(1);
        stats.chunk_bytes = stats.chunk_bytes.saturating_add(bytes);
    }

    pub(super) fn add_segment_stats(&mut self, kind: ChunkKind, stats: SegmentChunkKindStats) {
        let out = self.stats_mut(kind);
        out.chunks = out.chunks.saturating_add(stats.chunks);
        out.chunk_bytes = out.chunk_bytes.saturating_add(stats.chunk_bytes);
    }

    fn stats_mut(&mut self, kind: ChunkKind) -> &mut SegmentStoreSmokeKindStats {
        match kind {
            ChunkKind::Float => &mut self.float,
            ChunkKind::Int64 => &mut self.int64,
            ChunkKind::Histogram => &mut self.histogram,
            ChunkKind::ExponentialHistogram => &mut self.exponential_histogram,
            ChunkKind::Summary => &mut self.summary,
        }
    }
}

impl SegmentStoreSmokeTotals {
    pub(super) fn add_chunk_summary(&mut self, summary: &SegmentChunkSummary) {
        self.chunks = self.chunks.saturating_add(summary.chunks);
        self.chunk_bytes = self.chunk_bytes.saturating_add(summary.chunk_bytes);
        for kind in [
            ChunkKind::Float,
            ChunkKind::Int64,
            ChunkKind::Histogram,
            ChunkKind::ExponentialHistogram,
            ChunkKind::Summary,
        ] {
            self.by_kind
                .add_segment_stats(kind, summary.by_kind.stats(kind));
        }
    }
}

impl SegmentStoreSmokeReport {
    pub(super) fn sample_count_for_kind(&self, kind: ChunkKind) -> usize {
        self.sample_series
            .iter()
            .filter(|sample| sample.kind == kind)
            .count()
    }

    pub(super) fn sample_limits_reached_for_summary(
        &self,
        summary: &SegmentChunkSummary,
        sample_limit_per_kind: usize,
    ) -> bool {
        if sample_limit_per_kind == 0 {
            return true;
        }
        [
            ChunkKind::Float,
            ChunkKind::Int64,
            ChunkKind::Histogram,
            ChunkKind::ExponentialHistogram,
            ChunkKind::Summary,
        ]
        .into_iter()
        .all(|kind| {
            summary.by_kind.stats(kind).chunks == 0
                || self.sample_count_for_kind(kind) >= sample_limit_per_kind
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryProjectionConfig {
    exponential_histogram_bucket_boundaries: Vec<f64>,
}

impl QueryProjectionConfig {
    pub fn with_exponential_histogram_bucket_boundaries(
        mut self,
        mut boundaries: Vec<f64>,
    ) -> Self {
        assert!(
            boundaries.iter().all(|boundary| boundary.is_finite()),
            "exponential histogram projection boundaries must be finite"
        );
        boundaries.sort_by(f64::total_cmp);
        boundaries.dedup_by(|left, right| left.to_bits() == right.to_bits());
        self.exponential_histogram_bucket_boundaries = boundaries;
        self
    }

    pub(super) fn exponential_histogram_bucket_boundaries(&self) -> &[f64] {
        &self.exponential_histogram_bucket_boundaries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLimit {
    MatchedSeries,
    ProjectedSeries,
    ChunkReads,
    BytesRead,
    SamplesDecoded,
    RegexValuesExamined,
}

impl QueryLimit {
    fn as_str(self) -> &'static str {
        match self {
            Self::MatchedSeries => "matched_series",
            Self::ProjectedSeries => "projected_series",
            Self::ChunkReads => "chunk_reads",
            Self::BytesRead => "bytes_read",
            Self::SamplesDecoded => "samples_decoded",
            Self::RegexValuesExamined => "regex_values_examined",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimitExceeded {
    pub limit: QueryLimit,
    pub max: u64,
}

impl fmt::Display for QueryLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "query exceeded {} limit of {}",
            self.limit.as_str(),
            self.max
        )
    }
}

impl std::error::Error for QueryLimitExceeded {}

#[derive(Debug)]
pub(crate) struct QueryBudget {
    limits: QueryLimits,
    stats: QueryStats,
    seen_series: BTreeSet<u64>,
    seen_projected_series: BTreeSet<u64>,
}

impl QueryBudget {
    pub(crate) fn new(limits: QueryLimits) -> Self {
        Self {
            limits,
            stats: QueryStats::default(),
            seen_series: BTreeSet::new(),
            seen_projected_series: BTreeSet::new(),
        }
    }

    pub(crate) fn unlimited() -> Self {
        Self::new(QueryLimits::unlimited())
    }

    pub(crate) fn stats(&self) -> QueryStats {
        self.stats
    }

    pub(crate) fn observe_matched_series(&mut self, series_id: u64) -> io::Result<()> {
        if !self.seen_series.insert(series_id) {
            return Ok(());
        }
        self.stats.matched_series = self.checked_add(
            QueryLimit::MatchedSeries,
            self.stats.matched_series,
            1,
            self.limits.max_matched_series,
        )?;
        Ok(())
    }

    pub(crate) fn observe_projected_series(&mut self, series_id: u64) -> io::Result<()> {
        if !self.seen_projected_series.insert(series_id) {
            return Ok(());
        }
        self.stats.projected_series = self.checked_add(
            QueryLimit::ProjectedSeries,
            self.stats.projected_series,
            1,
            self.limits.max_projected_series,
        )?;
        Ok(())
    }

    pub(crate) fn observe_projected_results(
        &mut self,
        results: &[SegmentQueryResult],
    ) -> io::Result<()> {
        for result in results {
            self.observe_projected_series(result.series_id)?;
        }
        Ok(())
    }

    pub(crate) fn observe_candidate_series_refs(&mut self, count: u64) -> io::Result<()> {
        if let Some(max) = self.limits.max_matched_series
            && count > max
        {
            return Err(limit_exceeded_io(QueryLimitExceeded {
                limit: QueryLimit::MatchedSeries,
                max,
            }));
        }
        Ok(())
    }

    pub(crate) fn observe_chunk_read(&mut self, bytes: u64) -> io::Result<()> {
        self.stats.chunk_reads = self.checked_add(
            QueryLimit::ChunkReads,
            self.stats.chunk_reads,
            1,
            self.limits.max_chunk_reads,
        )?;
        self.stats.bytes_read = self.checked_add(
            QueryLimit::BytesRead,
            self.stats.bytes_read,
            bytes,
            self.limits.max_bytes_read,
        )?;
        Ok(())
    }

    pub(crate) fn observe_samples_decoded(&mut self, samples: u64) -> io::Result<()> {
        self.stats.samples_decoded = self.checked_add(
            QueryLimit::SamplesDecoded,
            self.stats.samples_decoded,
            samples,
            self.limits.max_samples_decoded,
        )?;
        Ok(())
    }

    pub(crate) fn observe_typed_scalar_chunk_decoded(&mut self) {
        self.stats.typed_scalar_chunks_decoded =
            self.stats.typed_scalar_chunks_decoded.saturating_add(1);
    }

    pub(crate) fn observe_typed_full_chunk_decoded(&mut self) {
        self.stats.typed_full_chunks_decoded =
            self.stats.typed_full_chunks_decoded.saturating_add(1);
    }

    pub(crate) fn observe_regex_value(&mut self) -> io::Result<()> {
        self.stats.regex_values_examined = self.checked_add(
            QueryLimit::RegexValuesExamined,
            self.stats.regex_values_examined,
            1,
            self.limits.max_regex_values_examined,
        )?;
        Ok(())
    }

    pub(crate) fn observe_index_postings_read(&mut self, bytes: u64) {
        self.stats.index_postings_reads = self.stats.index_postings_reads.saturating_add(1);
        self.stats.index_postings_bytes_read =
            self.stats.index_postings_bytes_read.saturating_add(bytes);
    }

    pub(crate) fn observe_segment_considered(&mut self) {
        self.stats.segments_considered = self.stats.segments_considered.saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_time(&mut self) {
        self.stats.segments_skipped_by_time = self.stats.segments_skipped_by_time.saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_missing_equality(&mut self) {
        self.stats.segments_skipped_by_missing_equality = self
            .stats
            .segments_skipped_by_missing_equality
            .saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_matcher_time_range(&mut self) {
        self.stats.segments_skipped_by_matcher_time_range = self
            .stats
            .segments_skipped_by_matcher_time_range
            .saturating_add(1);
    }

    pub(crate) fn observe_segment_queried(&mut self) {
        self.stats.segments_queried = self.stats.segments_queried.saturating_add(1);
    }

    fn checked_add(
        &self,
        limit: QueryLimit,
        current: u64,
        increment: u64,
        max: Option<u64>,
    ) -> io::Result<u64> {
        let next = current.saturating_add(increment);
        if let Some(max) = max
            && next > max
        {
            return Err(limit_exceeded_io(QueryLimitExceeded { limit, max }));
        }
        Ok(next)
    }
}

pub(super) fn limit_exceeded_io(exceeded: QueryLimitExceeded) -> io::Error {
    io::Error::new(io::ErrorKind::QuotaExceeded, exceeded)
}

pub(super) fn query_limit_exceeded_from_io(err: &io::Error) -> Option<&QueryLimitExceeded> {
    err.get_ref()?.downcast_ref::<QueryLimitExceeded>()
}

pub(super) fn promql_error_from_query_io(err: io::Error) -> PromqlQueryError {
    if err.kind() == io::ErrorKind::QuotaExceeded
        && let Some(exceeded) = query_limit_exceeded_from_io(&err)
    {
        return PromqlQueryError::LimitExceeded {
            limit: exceeded.limit.as_str().to_string(),
            max: exceeded.max,
        };
    }

    PromqlQueryError::Storage(err.to_string())
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MetadataAccumulator {
    metric_names: BTreeSet<String>,
    label_names: BTreeSet<String>,
    label_values: BTreeMap<String, BTreeSet<String>>,
}

impl MetadataAccumulator {
    pub(crate) fn add_label_name(&mut self, name: String) {
        self.label_names.insert(name);
    }

    pub(crate) fn add_label_value(&mut self, name: String, value: String) {
        self.label_names.insert(name.clone());
        self.label_values
            .entry(name.clone())
            .or_default()
            .insert(value.clone());
        if name == METRIC_NAME_LABEL {
            self.metric_names.insert(value);
        }
    }

    pub(crate) fn add_labelset(&mut self, labels: &[(String, String)]) {
        for (name, value) in labels {
            self.add_label_value(name.clone(), value.clone());
        }
    }

    pub(crate) fn metric_names(&self) -> Vec<String> {
        self.metric_names.iter().cloned().collect()
    }

    pub(crate) fn label_names(&self) -> Vec<String> {
        self.label_names.iter().cloned().collect()
    }

    pub(crate) fn label_values(&self, label_name: &str) -> Vec<String> {
        self.label_values
            .get(label_name)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: String },
    NotRegex { name: String, pattern: String },
}

impl LabelMatcher {
    pub fn eq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Eq {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn not_eq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::NotEq {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::Regex {
            name: name.into(),
            pattern: pattern.into(),
        }
    }

    pub fn not_regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::NotRegex {
            name: name.into(),
            pattern: pattern.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SegmentSelector {
    pub(super) metric_name: Option<String>,
    pub(super) matchers: Vec<LabelMatcher>,
    pub(super) projection: SegmentProjection,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum SegmentProjection {
    #[default]
    None,
    AllPromql {
        exponential_histogram_boundaries: Vec<f64>,
    },
    Count,
    Sum,
    HistogramBucket {
        le: Option<String>,
        exponential_histogram_boundaries: Vec<f64>,
    },
    NativeHistogram,
    NativeExponentialHistogram,
    SummaryQuantile {
        quantile: Option<String>,
    },
}

impl SegmentSelector {
    pub fn new(matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: None,
            matchers,
            projection: SegmentProjection::None,
        }
    }

    pub fn metric(metric_name: impl Into<String>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers: Vec::new(),
            projection: SegmentProjection::None,
        }
    }

    pub fn with_metric(metric_name: impl Into<String>, matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers,
            projection: SegmentProjection::None,
        }
    }

    pub(super) fn with_projection(mut self, projection: SegmentProjection) -> Self {
        self.projection = projection;
        self
    }

    pub(crate) fn projection(&self) -> &SegmentProjection {
        &self.projection
    }

    pub(crate) fn normalized_matchers(&self) -> Vec<NormalizedMatcher> {
        let mut normalized = Vec::with_capacity(self.matchers.len() + 1);
        if let Some(metric_name) = &self.metric_name {
            normalized.push(NormalizedMatcher::Eq {
                name: METRIC_NAME_LABEL.to_string(),
                value: normalize_metric_name(metric_name),
            });
        }

        for matcher in &self.matchers {
            match matcher {
                LabelMatcher::Eq { name, value } => {
                    let (name, value) = normalize_matcher_name_value(name, value);
                    normalized.push(NormalizedMatcher::Eq { name, value });
                }
                LabelMatcher::NotEq { name, value } => {
                    let (name, value) = normalize_matcher_name_value(name, value);
                    normalized.push(NormalizedMatcher::NotEq { name, value });
                }
                LabelMatcher::Regex { name, pattern } => {
                    let name = normalize_matcher_name(name);
                    normalized.push(NormalizedMatcher::Regex {
                        name,
                        pattern: pattern.clone(),
                    });
                }
                LabelMatcher::NotRegex { name, pattern } => {
                    let name = normalize_matcher_name(name);
                    normalized.push(NormalizedMatcher::NotRegex {
                        name,
                        pattern: pattern.clone(),
                    });
                }
            }
        }

        normalized
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NormalizedMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: String },
    NotRegex { name: String, pattern: String },
}

pub(crate) enum CompiledLabelMatcher {
    Eq { name: String, value: String },
    NotEq { name: String, value: String },
    Regex { name: String, pattern: regex::Regex },
    NotRegex { name: String, pattern: regex::Regex },
}

pub(super) const PROMQL_PROJECTION_SUFFIXES: [&str; 3] = ["_bucket", "_count", "_sum"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResolvedEqualityMatcher {
    pub(super) name_sym: u32,
    pub(super) value_sym: u32,
    pub(super) postings: ExactPostingsMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentPruneReason {
    MissingEquality,
    MatcherTimeRange,
}

pub struct SegmentStoreReader {
    pub(super) segments: Vec<SegmentReader>,
    pub(super) query_projection_config: QueryProjectionConfig,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreOpenOptions {
    pub validate_segment_footers: bool,
}

pub struct SegmentStoreQuerySession<'a> {
    pub(super) query_projection_config: QueryProjectionConfig,
    pub(super) segments: Vec<SegmentQuerySessionReader<'a>>,
    pub(super) label_cache: SeriesLabelCache,
    pub(super) projected_label_cache: ProjectedLabelCache,
}

pub(super) type SeriesLabelCache = HashMap<u64, QueryLabels>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ProjectedLabelCacheKey {
    pub(super) source_series_id: u64,
    pub(super) metric_suffix: &'static str,
}

#[derive(Debug)]
pub(super) struct ProjectedSeriesLabels {
    pub(super) series_id: u64,
    pub(super) labels: QueryLabels,
}

#[derive(Debug, Default)]
pub(super) struct ProjectedLabelCache {
    pub(super) entries: HashMap<ProjectedLabelCacheKey, Arc<ProjectedSeriesLabels>>,
    pub(super) hits: u64,
    pub(super) misses: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreQuerySessionStats {
    pub index_routing_opens: u64,
    pub segment_context_opens: u64,
    pub symbols_bin_opens: u64,
    pub indexes_puffin_opens: u64,
    pub series_bin_opens: u64,
    pub chunk_index_bin_opens: u64,
    pub chunks_bin_opens: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkPayloadLocalityProfile {
    pub reads: u64,
    pub forward_gaps: u64,
    pub forward_gap_bytes: u64,
    pub backward_jumps: u64,
    pub contiguous_runs: u64,
    pub contiguous_span_bytes: u64,
    pub coalesced_4k_runs: u64,
    pub coalesced_4k_span_bytes: u64,
    pub coalesced_64k_runs: u64,
    pub coalesced_64k_span_bytes: u64,
    pub sorted_contiguous_runs: u64,
    pub sorted_contiguous_span_bytes: u64,
    pub sorted_coalesced_4k_runs: u64,
    pub sorted_coalesced_4k_span_bytes: u64,
    pub sorted_coalesced_64k_runs: u64,
    pub sorted_coalesced_64k_span_bytes: u64,
    initialized: bool,
    last_offset: u64,
    last_end: u64,
    contiguous_end: u64,
    coalesced_4k_end: u64,
    coalesced_64k_end: u64,
}

impl ChunkPayloadLocalityProfile {
    const GAP_4K: u64 = 4 * 1024;
    const GAP_64K: u64 = 64 * 1024;

    fn observe(&mut self, offset: u64, len: u64) {
        let end = offset.saturating_add(len);
        let backward_jump = self.initialized && offset < self.last_offset;

        self.reads = self.reads.saturating_add(1);
        if self.initialized {
            if backward_jump {
                self.backward_jumps = self.backward_jumps.saturating_add(1);
            } else if offset > self.last_end {
                let gap = offset - self.last_end;
                self.forward_gaps = self.forward_gaps.saturating_add(1);
                self.forward_gap_bytes = self.forward_gap_bytes.saturating_add(gap);
            }
        }

        observe_coalesced_range(
            offset,
            end,
            0,
            backward_jump,
            &mut self.contiguous_runs,
            &mut self.contiguous_span_bytes,
            &mut self.contiguous_end,
        );
        observe_coalesced_range(
            offset,
            end,
            Self::GAP_4K,
            backward_jump,
            &mut self.coalesced_4k_runs,
            &mut self.coalesced_4k_span_bytes,
            &mut self.coalesced_4k_end,
        );
        observe_coalesced_range(
            offset,
            end,
            Self::GAP_64K,
            backward_jump,
            &mut self.coalesced_64k_runs,
            &mut self.coalesced_64k_span_bytes,
            &mut self.coalesced_64k_end,
        );

        self.initialized = true;
        self.last_offset = offset;
        self.last_end = end;
    }

    fn observe_sorted(&mut self, ranges: &mut [(u64, u64)]) {
        if ranges.is_empty() {
            return;
        }

        ranges.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });

        let (runs, span_bytes) = coalesced_summary(ranges, 0);
        self.sorted_contiguous_runs = self.sorted_contiguous_runs.saturating_add(runs);
        self.sorted_contiguous_span_bytes =
            self.sorted_contiguous_span_bytes.saturating_add(span_bytes);

        let (runs, span_bytes) = coalesced_summary(ranges, Self::GAP_4K);
        self.sorted_coalesced_4k_runs = self.sorted_coalesced_4k_runs.saturating_add(runs);
        self.sorted_coalesced_4k_span_bytes = self
            .sorted_coalesced_4k_span_bytes
            .saturating_add(span_bytes);

        let (runs, span_bytes) = coalesced_summary(ranges, Self::GAP_64K);
        self.sorted_coalesced_64k_runs = self.sorted_coalesced_64k_runs.saturating_add(runs);
        self.sorted_coalesced_64k_span_bytes = self
            .sorted_coalesced_64k_span_bytes
            .saturating_add(span_bytes);
    }

    pub fn add(&mut self, other: Self) {
        self.reads = self.reads.saturating_add(other.reads);
        self.forward_gaps = self.forward_gaps.saturating_add(other.forward_gaps);
        self.forward_gap_bytes = self
            .forward_gap_bytes
            .saturating_add(other.forward_gap_bytes);
        self.backward_jumps = self.backward_jumps.saturating_add(other.backward_jumps);
        self.contiguous_runs = self.contiguous_runs.saturating_add(other.contiguous_runs);
        self.contiguous_span_bytes = self
            .contiguous_span_bytes
            .saturating_add(other.contiguous_span_bytes);
        self.coalesced_4k_runs = self
            .coalesced_4k_runs
            .saturating_add(other.coalesced_4k_runs);
        self.coalesced_4k_span_bytes = self
            .coalesced_4k_span_bytes
            .saturating_add(other.coalesced_4k_span_bytes);
        self.coalesced_64k_runs = self
            .coalesced_64k_runs
            .saturating_add(other.coalesced_64k_runs);
        self.coalesced_64k_span_bytes = self
            .coalesced_64k_span_bytes
            .saturating_add(other.coalesced_64k_span_bytes);
        self.sorted_contiguous_runs = self
            .sorted_contiguous_runs
            .saturating_add(other.sorted_contiguous_runs);
        self.sorted_contiguous_span_bytes = self
            .sorted_contiguous_span_bytes
            .saturating_add(other.sorted_contiguous_span_bytes);
        self.sorted_coalesced_4k_runs = self
            .sorted_coalesced_4k_runs
            .saturating_add(other.sorted_coalesced_4k_runs);
        self.sorted_coalesced_4k_span_bytes = self
            .sorted_coalesced_4k_span_bytes
            .saturating_add(other.sorted_coalesced_4k_span_bytes);
        self.sorted_coalesced_64k_runs = self
            .sorted_coalesced_64k_runs
            .saturating_add(other.sorted_coalesced_64k_runs);
        self.sorted_coalesced_64k_span_bytes = self
            .sorted_coalesced_64k_span_bytes
            .saturating_add(other.sorted_coalesced_64k_span_bytes);
    }

    fn delta_since(self, before: Self) -> Self {
        Self {
            reads: self.reads.saturating_sub(before.reads),
            forward_gaps: self.forward_gaps.saturating_sub(before.forward_gaps),
            forward_gap_bytes: self
                .forward_gap_bytes
                .saturating_sub(before.forward_gap_bytes),
            backward_jumps: self.backward_jumps.saturating_sub(before.backward_jumps),
            contiguous_runs: self.contiguous_runs.saturating_sub(before.contiguous_runs),
            contiguous_span_bytes: self
                .contiguous_span_bytes
                .saturating_sub(before.contiguous_span_bytes),
            coalesced_4k_runs: self
                .coalesced_4k_runs
                .saturating_sub(before.coalesced_4k_runs),
            coalesced_4k_span_bytes: self
                .coalesced_4k_span_bytes
                .saturating_sub(before.coalesced_4k_span_bytes),
            coalesced_64k_runs: self
                .coalesced_64k_runs
                .saturating_sub(before.coalesced_64k_runs),
            coalesced_64k_span_bytes: self
                .coalesced_64k_span_bytes
                .saturating_sub(before.coalesced_64k_span_bytes),
            sorted_contiguous_runs: self
                .sorted_contiguous_runs
                .saturating_sub(before.sorted_contiguous_runs),
            sorted_contiguous_span_bytes: self
                .sorted_contiguous_span_bytes
                .saturating_sub(before.sorted_contiguous_span_bytes),
            sorted_coalesced_4k_runs: self
                .sorted_coalesced_4k_runs
                .saturating_sub(before.sorted_coalesced_4k_runs),
            sorted_coalesced_4k_span_bytes: self
                .sorted_coalesced_4k_span_bytes
                .saturating_sub(before.sorted_coalesced_4k_span_bytes),
            sorted_coalesced_64k_runs: self
                .sorted_coalesced_64k_runs
                .saturating_sub(before.sorted_coalesced_64k_runs),
            sorted_coalesced_64k_span_bytes: self
                .sorted_coalesced_64k_span_bytes
                .saturating_sub(before.sorted_coalesced_64k_span_bytes),
            ..Self::default()
        }
    }
}

fn coalesced_summary(ranges: &[(u64, u64)], max_gap: u64) -> (u64, u64) {
    let mut runs = 0;
    let mut span_bytes = 0;
    let mut run_end = 0;
    for &(offset, len) in ranges {
        let end = offset.saturating_add(len);
        observe_coalesced_range(
            offset,
            end,
            max_gap,
            false,
            &mut runs,
            &mut span_bytes,
            &mut run_end,
        );
    }
    (runs, span_bytes)
}

fn observe_coalesced_range(
    offset: u64,
    end: u64,
    max_gap: u64,
    force_new_run: bool,
    runs: &mut u64,
    span_bytes: &mut u64,
    run_end: &mut u64,
) {
    let starts_new_run = *runs == 0 || force_new_run || offset > run_end.saturating_add(max_gap);
    if starts_new_run {
        *runs = (*runs).saturating_add(1);
        *span_bytes = (*span_bytes).saturating_add(end.saturating_sub(offset));
        *run_end = end;
    } else if end > *run_end {
        *span_bytes = (*span_bytes).saturating_add(end - *run_end);
        *run_end = end;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreQueryProfile {
    pub index_routing_open: Duration,
    pub segment_context_open: Duration,
    pub indexes_open: Duration,
    pub symbols_read: Duration,
    pub series_open: Duration,
    pub chunk_index_open: Duration,
    pub chunks_open: Duration,
    pub routing_index_read: Duration,
    pub exact_postings_read: Duration,
    pub metric_series_ranges_read: Duration,
    pub series_entry_read: Duration,
    pub chunk_index_range_read: Duration,
    pub chunk_read: Duration,
    pub index_routing_file_bytes: u64,
    pub indexes_file_bytes: u64,
    pub symbols_file_bytes: u64,
    pub series_file_bytes: u64,
    pub chunk_index_file_bytes: u64,
    pub chunks_file_bytes: u64,
    pub routing_index_bytes: u64,
    pub exact_postings_bytes: u64,
    pub metric_series_ranges_bytes: u64,
    pub series_entries_read: u64,
    pub series_entry_read_batches: u64,
    pub series_entry_bytes: u64,
    pub chunk_index_range_bytes: u64,
    pub chunk_payload_bytes: u64,
    pub chunk_payload_physical_reads: u64,
    pub chunk_payload_physical_bytes: u64,
    pub chunk_payload_locality: ChunkPayloadLocalityProfile,
}

impl SegmentStoreQueryProfile {
    pub(super) fn observe_chunk_payload_read(&mut self, offset: u64, len: u64) {
        self.chunk_payload_bytes = self.chunk_payload_bytes.saturating_add(len);
        self.chunk_payload_locality.observe(offset, len);
    }

    pub(super) fn observe_chunk_payload_physical_reads(&mut self, reads: u64, bytes: u64) {
        self.chunk_payload_physical_reads = self.chunk_payload_physical_reads.saturating_add(reads);
        self.chunk_payload_physical_bytes = self.chunk_payload_physical_bytes.saturating_add(bytes);
    }

    pub(super) fn observe_sorted_chunk_payload_ranges(&mut self, ranges: &mut [(u64, u64)]) {
        self.chunk_payload_locality.observe_sorted(ranges);
    }

    pub(super) fn add(&mut self, other: Self) {
        self.index_routing_open = self
            .index_routing_open
            .saturating_add(other.index_routing_open);
        self.segment_context_open = self
            .segment_context_open
            .saturating_add(other.segment_context_open);
        self.indexes_open = self.indexes_open.saturating_add(other.indexes_open);
        self.symbols_read = self.symbols_read.saturating_add(other.symbols_read);
        self.series_open = self.series_open.saturating_add(other.series_open);
        self.chunk_index_open = self.chunk_index_open.saturating_add(other.chunk_index_open);
        self.chunks_open = self.chunks_open.saturating_add(other.chunks_open);
        self.routing_index_read = self
            .routing_index_read
            .saturating_add(other.routing_index_read);
        self.exact_postings_read = self
            .exact_postings_read
            .saturating_add(other.exact_postings_read);
        self.metric_series_ranges_read = self
            .metric_series_ranges_read
            .saturating_add(other.metric_series_ranges_read);
        self.series_entry_read = self
            .series_entry_read
            .saturating_add(other.series_entry_read);
        self.chunk_index_range_read = self
            .chunk_index_range_read
            .saturating_add(other.chunk_index_range_read);
        self.chunk_read = self.chunk_read.saturating_add(other.chunk_read);
        self.index_routing_file_bytes = self
            .index_routing_file_bytes
            .saturating_add(other.index_routing_file_bytes);
        self.indexes_file_bytes = self
            .indexes_file_bytes
            .saturating_add(other.indexes_file_bytes);
        self.symbols_file_bytes = self
            .symbols_file_bytes
            .saturating_add(other.symbols_file_bytes);
        self.series_file_bytes = self
            .series_file_bytes
            .saturating_add(other.series_file_bytes);
        self.chunk_index_file_bytes = self
            .chunk_index_file_bytes
            .saturating_add(other.chunk_index_file_bytes);
        self.chunks_file_bytes = self
            .chunks_file_bytes
            .saturating_add(other.chunks_file_bytes);
        self.routing_index_bytes = self
            .routing_index_bytes
            .saturating_add(other.routing_index_bytes);
        self.exact_postings_bytes = self
            .exact_postings_bytes
            .saturating_add(other.exact_postings_bytes);
        self.metric_series_ranges_bytes = self
            .metric_series_ranges_bytes
            .saturating_add(other.metric_series_ranges_bytes);
        self.series_entries_read = self
            .series_entries_read
            .saturating_add(other.series_entries_read);
        self.series_entry_read_batches = self
            .series_entry_read_batches
            .saturating_add(other.series_entry_read_batches);
        self.series_entry_bytes = self
            .series_entry_bytes
            .saturating_add(other.series_entry_bytes);
        self.chunk_index_range_bytes = self
            .chunk_index_range_bytes
            .saturating_add(other.chunk_index_range_bytes);
        self.chunk_payload_bytes = self
            .chunk_payload_bytes
            .saturating_add(other.chunk_payload_bytes);
        self.chunk_payload_physical_reads = self
            .chunk_payload_physical_reads
            .saturating_add(other.chunk_payload_physical_reads);
        self.chunk_payload_physical_bytes = self
            .chunk_payload_physical_bytes
            .saturating_add(other.chunk_payload_physical_bytes);
        self.chunk_payload_locality
            .add(other.chunk_payload_locality);
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            index_routing_open: self
                .index_routing_open
                .saturating_sub(before.index_routing_open),
            segment_context_open: self
                .segment_context_open
                .saturating_sub(before.segment_context_open),
            indexes_open: self.indexes_open.saturating_sub(before.indexes_open),
            symbols_read: self.symbols_read.saturating_sub(before.symbols_read),
            series_open: self.series_open.saturating_sub(before.series_open),
            chunk_index_open: self
                .chunk_index_open
                .saturating_sub(before.chunk_index_open),
            chunks_open: self.chunks_open.saturating_sub(before.chunks_open),
            routing_index_read: self
                .routing_index_read
                .saturating_sub(before.routing_index_read),
            exact_postings_read: self
                .exact_postings_read
                .saturating_sub(before.exact_postings_read),
            metric_series_ranges_read: self
                .metric_series_ranges_read
                .saturating_sub(before.metric_series_ranges_read),
            series_entry_read: self
                .series_entry_read
                .saturating_sub(before.series_entry_read),
            chunk_index_range_read: self
                .chunk_index_range_read
                .saturating_sub(before.chunk_index_range_read),
            chunk_read: self.chunk_read.saturating_sub(before.chunk_read),
            index_routing_file_bytes: self
                .index_routing_file_bytes
                .saturating_sub(before.index_routing_file_bytes),
            indexes_file_bytes: self
                .indexes_file_bytes
                .saturating_sub(before.indexes_file_bytes),
            symbols_file_bytes: self
                .symbols_file_bytes
                .saturating_sub(before.symbols_file_bytes),
            series_file_bytes: self
                .series_file_bytes
                .saturating_sub(before.series_file_bytes),
            chunk_index_file_bytes: self
                .chunk_index_file_bytes
                .saturating_sub(before.chunk_index_file_bytes),
            chunks_file_bytes: self
                .chunks_file_bytes
                .saturating_sub(before.chunks_file_bytes),
            routing_index_bytes: self
                .routing_index_bytes
                .saturating_sub(before.routing_index_bytes),
            exact_postings_bytes: self
                .exact_postings_bytes
                .saturating_sub(before.exact_postings_bytes),
            metric_series_ranges_bytes: self
                .metric_series_ranges_bytes
                .saturating_sub(before.metric_series_ranges_bytes),
            series_entries_read: self
                .series_entries_read
                .saturating_sub(before.series_entries_read),
            series_entry_read_batches: self
                .series_entry_read_batches
                .saturating_sub(before.series_entry_read_batches),
            series_entry_bytes: self
                .series_entry_bytes
                .saturating_sub(before.series_entry_bytes),
            chunk_index_range_bytes: self
                .chunk_index_range_bytes
                .saturating_sub(before.chunk_index_range_bytes),
            chunk_payload_bytes: self
                .chunk_payload_bytes
                .saturating_sub(before.chunk_payload_bytes),
            chunk_payload_physical_reads: self
                .chunk_payload_physical_reads
                .saturating_sub(before.chunk_payload_physical_reads),
            chunk_payload_physical_bytes: self
                .chunk_payload_physical_bytes
                .saturating_sub(before.chunk_payload_physical_bytes),
            chunk_payload_locality: self
                .chunk_payload_locality
                .delta_since(before.chunk_payload_locality),
        }
    }
}

impl SegmentStoreQuerySessionStats {
    pub(super) fn add(&mut self, other: Self) {
        self.index_routing_opens = self
            .index_routing_opens
            .saturating_add(other.index_routing_opens);
        self.segment_context_opens = self
            .segment_context_opens
            .saturating_add(other.segment_context_opens);
        self.symbols_bin_opens = self
            .symbols_bin_opens
            .saturating_add(other.symbols_bin_opens);
        self.indexes_puffin_opens = self
            .indexes_puffin_opens
            .saturating_add(other.indexes_puffin_opens);
        self.series_bin_opens = self.series_bin_opens.saturating_add(other.series_bin_opens);
        self.chunk_index_bin_opens = self
            .chunk_index_bin_opens
            .saturating_add(other.chunk_index_bin_opens);
        self.chunks_bin_opens = self.chunks_bin_opens.saturating_add(other.chunks_bin_opens);
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            index_routing_opens: self
                .index_routing_opens
                .saturating_sub(before.index_routing_opens),
            segment_context_opens: self
                .segment_context_opens
                .saturating_sub(before.segment_context_opens),
            symbols_bin_opens: self
                .symbols_bin_opens
                .saturating_sub(before.symbols_bin_opens),
            indexes_puffin_opens: self
                .indexes_puffin_opens
                .saturating_sub(before.indexes_puffin_opens),
            series_bin_opens: self
                .series_bin_opens
                .saturating_sub(before.series_bin_opens),
            chunk_index_bin_opens: self
                .chunk_index_bin_opens
                .saturating_sub(before.chunk_index_bin_opens),
            chunks_bin_opens: self
                .chunks_bin_opens
                .saturating_sub(before.chunks_bin_opens),
        }
    }
}
