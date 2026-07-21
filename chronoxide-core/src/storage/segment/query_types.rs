use super::metadata_facade::SegmentEncodedLabels;
use super::*;
use crate::storage::index::SegmentIndexReadStats;
use crate::storage::metadata_runtime::SegmentGenerationProvenance;
use crate::storage::symbols::{
    SegmentSymbolReadStats, SegmentSymbolReader, SegmentSymbolResourceSnapshot,
};
use smallvec::SmallVec;

pub struct SegmentReader {
    pub(super) dir: PathBuf,
    pub(super) meta: SegmentMeta,
    pub(super) storage_schema_policy: SegmentStoreSchemaPolicy,
    pub(super) metadata_reader: super::metadata_facade::SegmentMetadataReader,
    pub(super) symbol_format: SegmentSymbolFormat,
    pub(super) query_cache: Arc<SegmentReaderQueryCache>,
    pub(super) registered_metadata: RegisteredSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SegmentSymbolFormat {
    PagedV3,
    #[allow(dead_code)] // Removed with the remaining schema-5 fingerprint adapter.
    LegacyV2ForLayoutAb,
}

#[derive(Default)]
pub(super) struct SegmentReaderQueryCache {
    pub(super) index_reader: Mutex<Option<SegmentIndexReader<File>>>,
    pub(super) symbols: Mutex<Option<SegmentSymbolReader<File>>>,
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
    pub(super) open_read_stats: SegmentIndexReadStats,
}

pub(super) struct CachedSymbols {
    pub(super) symbols: Arc<SegmentSymbolReader<File>>,
    pub(super) cache_hit: bool,
    pub(super) file_bytes: u64,
    pub(super) open_elapsed: Duration,
    pub(super) open_read_stats: SegmentSymbolReadStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentQueryResult {
    pub series_id: u64,
    pub labels: QueryLabels,
    pub samples: Vec<(u64, f64)>,
    pub counter_reset_hints: Vec<CounterResetHint>,
    pub(crate) sample_start_times: Vec<Option<u64>>,
    pub(crate) temporality: QueryResultTemporality,
    /// Whether `labels` is the complete externally observable label set.
    ///
    /// False is allowed only for an internal terminal-aggregation input.
    /// Before range evaluation `series_id` is the integrity-checked full source
    /// identity. After `rate`/`increase`, it is the canonical identity of the
    /// same fully integrity-checked label set with `__name__` removed, matching
    /// the established full-label path.
    pub(crate) labels_complete: bool,
    /// Established result identity after removing `__name__`, computed during
    /// the fully integrity-checked borrowed-row pass. It is present on incomplete
    /// inputs only when the whitelisted child range function needs it; direct
    /// terminal aggregations avoid this otherwise-unused second hash.
    pub(crate) metric_name_dropped_series_id: Option<u64>,
    /// Raw delta intervals behind cumulative-shaped virtual scalar projections.
    ///
    /// This sidecar stays aligned with `samples` across chunk, segment, and
    /// head merges. Replaying the raw intervals after merge prevents physical
    /// storage boundaries from becoming observable counter resets while
    /// preserving saturating `u64` count arithmetic and the exact IEEE order
    /// of optional-sum addition.
    pub(crate) delta_projection_intervals: Vec<Option<DeltaProjectionInterval>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum DeltaProjectionInterval {
    Count {
        raw: u64,
        reset_hint: CounterResetHint,
    },
    Sum {
        raw: f64,
        reset_hint: CounterResetHint,
    },
}

impl SegmentQueryResult {
    pub(crate) fn new(series_id: u64, labels: Vec<(String, String)>) -> Self {
        Self {
            series_id,
            labels: shared_query_labels(labels),
            samples: Vec::new(),
            counter_reset_hints: Vec::new(),
            sample_start_times: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
            labels_complete: true,
            metric_name_dropped_series_id: None,
            delta_projection_intervals: Vec::new(),
        }
    }

    pub(crate) fn with_shared_labels(series_id: u64, labels: QueryLabels) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
            counter_reset_hints: Vec::new(),
            sample_start_times: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
            labels_complete: true,
            metric_name_dropped_series_id: None,
            delta_projection_intervals: Vec::new(),
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
            sample_start_times: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
            labels_complete: true,
            metric_name_dropped_series_id: None,
            delta_projection_intervals: Vec::new(),
        }
    }

    pub(crate) fn mark_labels_incomplete(&mut self, metric_name_dropped_series_id: Option<u64>) {
        self.labels_complete = false;
        self.metric_name_dropped_series_id = metric_name_dropped_series_id;
    }

    pub(crate) fn labels_are_complete(&self) -> bool {
        self.labels_complete
    }

    pub(crate) fn push_sample(&mut self, timestamp_ms: u64, value: f64) {
        if self.has_counter_reset_hints() {
            self.counter_reset_hints.push(CounterResetHint::Unknown);
        } else {
            self.counter_reset_hints.clear();
        }
        if self.has_sample_start_times() {
            self.sample_start_times.push(None);
        } else {
            self.sample_start_times.clear();
        }
        if self.has_delta_projection_intervals() {
            self.delta_projection_intervals.push(None);
        } else {
            self.delta_projection_intervals.clear();
        }
        self.samples.push((timestamp_ms, value));
    }

    pub(crate) fn push_sample_with_counter_reset_hint_temporality_and_start_time(
        &mut self,
        timestamp_ms: u64,
        value: f64,
        reset_hint: CounterResetHint,
        temporality: OtlpAggregationTemporality,
        start_time_ms: Option<u64>,
    ) {
        let start_time_ms = (temporality == OtlpAggregationTemporality::Delta)
            .then_some(start_time_ms)
            .flatten();
        self.ensure_counter_reset_hints();
        if start_time_ms.is_some() {
            self.ensure_sample_start_times();
        }
        if start_time_ms.is_some() || self.has_sample_start_times() {
            self.sample_start_times.push(start_time_ms);
        } else {
            self.sample_start_times.clear();
        }
        if self.has_delta_projection_intervals() {
            self.delta_projection_intervals.push(None);
        } else {
            self.delta_projection_intervals.clear();
        }
        self.samples.push((timestamp_ms, value));
        self.counter_reset_hints.push(reset_hint);
        self.observe_temporality(QueryResultTemporality::from(temporality));
    }

    pub(crate) fn mark_last_delta_projection_interval(
        &mut self,
        interval: DeltaProjectionInterval,
    ) {
        self.ensure_delta_projection_intervals();
        let last = self
            .delta_projection_intervals
            .last_mut()
            .expect("a projected interval must have an aligned sample");
        *last = Some(interval);
    }

    pub(crate) fn extend_from(&mut self, mut other: SegmentQueryResult) {
        if let Some(other_identity) = other.metric_name_dropped_series_id
            && self.metric_name_dropped_series_id.is_none()
        {
            // Keep the first merged identity just as the established
            // source-ID merge keeps the first label set. A 64-bit source
            // identity collision must not introduce a selective-only
            // panic or claim a stronger collision contract.
            self.metric_name_dropped_series_id = Some(other_identity);
        }
        self.labels_complete &= other.labels_complete;
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
        if other.has_sample_start_times() {
            self.ensure_sample_start_times();
            self.sample_start_times
                .append(&mut other.sample_start_times);
        } else if self.has_sample_start_times() {
            self.sample_start_times
                .extend(std::iter::repeat_n(None, other.samples.len()));
        } else {
            self.sample_start_times.clear();
        }
        if other.has_delta_projection_intervals() {
            self.ensure_delta_projection_intervals();
            self.delta_projection_intervals
                .append(&mut other.delta_projection_intervals);
        } else if self.has_delta_projection_intervals() {
            self.delta_projection_intervals
                .extend(std::iter::repeat_n(None, other.samples.len()));
        } else {
            self.delta_projection_intervals.clear();
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
        let has_start_times = self.has_sample_start_times();
        let has_delta_intervals = self.has_delta_projection_intervals();
        if !has_hints {
            self.counter_reset_hints.clear();
        }
        if !has_start_times {
            self.sample_start_times.clear();
        }
        if !has_delta_intervals {
            self.delta_projection_intervals.clear();
        }

        if self.samples.len() >= 2 {
            match sample_timestamp_order(&self.samples) {
                SampleTimestampOrder::StrictlyIncreasing => {}
                SampleTimestampOrder::SortedWithDuplicates => {
                    self.compact_sorted_samples_keep_last(
                        has_hints,
                        has_start_times,
                        has_delta_intervals,
                    );
                }
                SampleTimestampOrder::Unsorted => {
                    self.sort_and_dedupe_samples_keep_last(
                        has_hints,
                        has_start_times,
                        has_delta_intervals,
                    );
                }
            }
        }

        self.materialize_delta_projection_intervals();
    }

    fn compact_sorted_samples_keep_last(
        &mut self,
        has_hints: bool,
        has_start_times: bool,
        has_delta_intervals: bool,
    ) {
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
                if has_start_times {
                    self.sample_start_times[write_idx] = self.sample_start_times[last_idx];
                }
                if has_delta_intervals {
                    self.delta_projection_intervals[write_idx] =
                        self.delta_projection_intervals[last_idx];
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
        if has_start_times {
            self.sample_start_times.truncate(write_idx);
        } else {
            self.sample_start_times.clear();
        }
        if has_delta_intervals {
            self.delta_projection_intervals.truncate(write_idx);
        } else {
            self.delta_projection_intervals.clear();
        }
    }

    fn sort_and_dedupe_samples_keep_last(
        &mut self,
        has_hints: bool,
        has_start_times: bool,
        has_delta_intervals: bool,
    ) {
        if !has_hints && !has_start_times && !has_delta_intervals {
            self.samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
            self.compact_sorted_samples_keep_last(false, false, false);
            return;
        }

        let hints = if has_hints {
            Some(std::mem::take(&mut self.counter_reset_hints))
        } else {
            None
        };
        let start_times = if has_start_times {
            Some(std::mem::take(&mut self.sample_start_times))
        } else {
            None
        };
        let delta_intervals = if has_delta_intervals {
            Some(std::mem::take(&mut self.delta_projection_intervals))
        } else {
            None
        };
        let mut rows: Vec<_> = std::mem::take(&mut self.samples)
            .into_iter()
            .enumerate()
            .map(|(idx, sample)| {
                (
                    sample,
                    hints.as_ref().map(|values| values[idx]),
                    start_times.as_ref().map(|values| values[idx]),
                    delta_intervals.as_ref().map(|values| values[idx]),
                )
            })
            .collect();
        rows.sort_by_key(|(sample, _, _, _)| sample.0);

        for (sample, reset_hint, start_time, delta_interval) in rows {
            if self
                .samples
                .last()
                .is_some_and(|(timestamp_ms, _)| *timestamp_ms == sample.0)
            {
                *self.samples.last_mut().expect("last sample exists") = sample;
                if has_hints {
                    *self
                        .counter_reset_hints
                        .last_mut()
                        .expect("last reset hint exists") = reset_hint.expect("reset hint exists");
                }
                if has_start_times {
                    *self
                        .sample_start_times
                        .last_mut()
                        .expect("last sample start time exists") =
                        start_time.expect("sample start time exists");
                }
                if has_delta_intervals {
                    *self
                        .delta_projection_intervals
                        .last_mut()
                        .expect("last delta interval exists") =
                        delta_interval.expect("delta interval exists");
                }
            } else {
                self.samples.push(sample);
                if has_hints {
                    self.counter_reset_hints
                        .push(reset_hint.expect("reset hint exists"));
                }
                if has_start_times {
                    self.sample_start_times
                        .push(start_time.expect("sample start time exists"));
                }
                if has_delta_intervals {
                    self.delta_projection_intervals
                        .push(delta_interval.expect("delta interval exists"));
                }
            }
        }
    }

    fn materialize_delta_projection_intervals(&mut self) {
        if !self.has_delta_projection_intervals() {
            return;
        }

        self.ensure_counter_reset_hints();
        let mut count_accumulator = 0u64;
        let mut sum_accumulator = 0.0f64;
        let mut active_kind = None;
        let mut fragment_started = false;
        let mut previous_delta_timestamp_ms = None;

        for idx in 0..self.samples.len() {
            if self.samples[idx].1.to_bits() == prometheus_stale_nan().to_bits() {
                // Keep the established direct-projection shape: a stale gap
                // closes the cumulative fragment. Range evaluation later
                // omits the marker and normalizes this synthetic restart to
                // unknown-reset detection rather than inventing a reset.
                count_accumulator = 0;
                sum_accumulator = 0.0;
                active_kind = None;
                fragment_started = false;
                previous_delta_timestamp_ms = None;
                continue;
            }

            let Some(interval) = self.delta_projection_intervals[idx] else {
                count_accumulator = 0;
                sum_accumulator = 0.0;
                active_kind = None;
                fragment_started = false;
                previous_delta_timestamp_ms = None;
                continue;
            };

            let interval_kind = match interval {
                DeltaProjectionInterval::Count { .. } => 0u8,
                DeltaProjectionInterval::Sum { .. } => 1u8,
            };
            let stored_reset_hint = match interval {
                DeltaProjectionInterval::Count { reset_hint, .. }
                | DeltaProjectionInterval::Sum { reset_hint, .. } => reset_hint,
            };
            let stored_reset = matches!(
                stored_reset_hint,
                CounterResetHint::CounterReset | CounterResetHint::GaugeType
            );
            let start_time_continues = previous_delta_timestamp_ms.is_some()
                && self.sample_start_times.get(idx).copied().flatten()
                    == previous_delta_timestamp_ms;
            // Every gap or overlap between adjacent OTLP delta intervals is a
            // logical boundary, regardless of whether it also crosses a
            // chunk, segment, or head projection boundary. A stored explicit
            // reset remains authoritative either way.
            if active_kind != Some(interval_kind)
                || stored_reset
                || (fragment_started && !start_time_continues)
            {
                count_accumulator = 0;
                sum_accumulator = 0.0;
                fragment_started = false;
            }
            active_kind = Some(interval_kind);

            self.samples[idx].1 = match interval {
                DeltaProjectionInterval::Count { raw, .. } => {
                    count_accumulator = count_accumulator.saturating_add(raw);
                    count_accumulator as f64
                }
                DeltaProjectionInterval::Sum { raw, .. } => {
                    sum_accumulator += raw;
                    sum_accumulator
                }
            };
            self.counter_reset_hints[idx] = if fragment_started {
                CounterResetHint::NotCounterReset
            } else {
                fragment_started = true;
                CounterResetHint::CounterReset
            };
            previous_delta_timestamp_ms = Some(self.samples[idx].0);
        }
    }

    pub(crate) fn counter_reset_hints(&self) -> Option<&[CounterResetHint]> {
        self.has_counter_reset_hints()
            .then_some(self.counter_reset_hints.as_slice())
    }

    pub(crate) fn sample_start_times(&self) -> Option<&[Option<u64>]> {
        self.has_sample_start_times()
            .then_some(self.sample_start_times.as_slice())
    }

    fn ensure_counter_reset_hints(&mut self) {
        if !self.has_counter_reset_hints() {
            self.counter_reset_hints = vec![CounterResetHint::Unknown; self.samples.len()];
        }
    }

    fn ensure_sample_start_times(&mut self) {
        if !self.has_sample_start_times() {
            self.sample_start_times = vec![None; self.samples.len()];
        }
    }

    fn ensure_delta_projection_intervals(&mut self) {
        if !self.has_delta_projection_intervals() {
            self.delta_projection_intervals = vec![None; self.samples.len()];
        }
    }

    fn has_counter_reset_hints(&self) -> bool {
        !self.counter_reset_hints.is_empty() && self.counter_reset_hints.len() == self.samples.len()
    }

    fn has_sample_start_times(&self) -> bool {
        !self.sample_start_times.is_empty() && self.sample_start_times.len() == self.samples.len()
    }

    fn has_delta_projection_intervals(&self) -> bool {
        !self.delta_projection_intervals.is_empty()
            && self.delta_projection_intervals.len() == self.samples.len()
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
    /// Whether `labels` contains the complete source label set. Incomplete
    /// native series are scoped to a proven root `count`/`group` aggregation.
    pub(crate) labels_complete: bool,
    /// Complete-row identity after removing `__name__`, used by selective
    /// native `rate`/`increase` before the terminal aggregation consumes it.
    pub(crate) metric_name_dropped_series_id: Option<u64>,
}

impl PromqlHistogramSeries {
    pub(crate) fn new(series_id: u64, labels: QueryLabels) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
            labels_complete: true,
            metric_name_dropped_series_id: None,
        }
    }

    pub(crate) fn mark_labels_incomplete(&mut self, metric_name_dropped_series_id: Option<u64>) {
        self.labels_complete = false;
        self.metric_name_dropped_series_id = metric_name_dropped_series_id;
    }

    pub(crate) fn push_sample(&mut self, sample: PromqlHistogramSample) {
        self.samples.push(sample);
    }

    pub(crate) fn extend_from(&mut self, mut other: PromqlHistogramSeries) {
        if self.metric_name_dropped_series_id.is_none() {
            self.metric_name_dropped_series_id = other.metric_name_dropped_series_id;
        }
        self.labels_complete &= other.labels_complete;
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
    pub(crate) start_time_ms: Option<u64>,
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
            start_time_ms: (value.metadata.temporality == OtlpAggregationTemporality::Delta)
                .then_some(value.metadata.start_time_ms)
                .flatten(),
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
    /// Whether `labels` contains the complete source label set. Incomplete
    /// native series are scoped to a proven root `count`/`group` aggregation.
    pub(crate) labels_complete: bool,
    /// Complete-row identity after removing `__name__`, used by selective
    /// native `rate`/`increase` before the terminal aggregation consumes it.
    pub(crate) metric_name_dropped_series_id: Option<u64>,
}

impl PromqlExponentialHistogramSeries {
    pub(crate) fn new(series_id: u64, labels: QueryLabels) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
            labels_complete: true,
            metric_name_dropped_series_id: None,
        }
    }

    pub(crate) fn mark_labels_incomplete(&mut self, metric_name_dropped_series_id: Option<u64>) {
        self.labels_complete = false;
        self.metric_name_dropped_series_id = metric_name_dropped_series_id;
    }

    pub(crate) fn push_sample(&mut self, sample: PromqlExponentialHistogramSample) {
        self.samples.push(sample);
    }

    pub(crate) fn extend_from(&mut self, mut other: PromqlExponentialHistogramSeries) {
        if self.metric_name_dropped_series_id.is_none() {
            self.metric_name_dropped_series_id = other.metric_name_dropped_series_id;
        }
        self.labels_complete &= other.labels_complete;
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
    pub(crate) start_time_ms: Option<u64>,
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
            start_time_ms: (value.metadata.temporality == OtlpAggregationTemporality::Delta)
                .then_some(value.metadata.start_time_ms)
                .flatten(),
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
    pub(crate) sparse_counts: Vec<(i32, f64)>,
}

impl From<ExponentialHistogramBuckets> for PromqlExponentialHistogramBuckets {
    fn from(value: ExponentialHistogramBuckets) -> Self {
        Self {
            offset: value.offset,
            counts: value.counts.into_iter().map(|count| count as f64).collect(),
            sparse_counts: Vec::new(),
        }
    }
}

impl PromqlExponentialHistogramBuckets {
    pub(crate) fn empty() -> Self {
        Self {
            offset: 0,
            counts: Vec::new(),
            sparse_counts: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_sparse_counts(mut counts: Vec<(i32, f64)>) -> Self {
        counts.retain(|(_, count)| *count != 0.0);
        counts.sort_by_key(|(index, _)| *index);

        let mut sparse_counts = Vec::<(i32, f64)>::with_capacity(counts.len());
        for (index, count) in counts {
            if let Some((last_index, last_count)) = sparse_counts.last_mut()
                && *last_index == index
            {
                *last_count += count;
                continue;
            }
            sparse_counts.push((index, count));
        }
        sparse_counts.retain(|(_, count)| *count != 0.0);

        Self {
            offset: 0,
            counts: Vec::new(),
            sparse_counts,
        }
    }

    pub(crate) fn iter_counts(&self) -> PromqlExponentialHistogramBucketIter<'_> {
        if self.sparse_counts.is_empty() {
            PromqlExponentialHistogramBucketIter::Dense {
                offset: i64::from(self.offset),
                idx: 0,
                counts: self.counts.iter(),
            }
        } else {
            PromqlExponentialHistogramBucketIter::Sparse(self.sparse_counts.iter())
        }
    }

    pub(crate) fn scale_counts(&mut self, scale: f64) {
        if self.sparse_counts.is_empty() {
            for count in &mut self.counts {
                *count *= scale;
            }
        } else {
            for (_, count) in &mut self.sparse_counts {
                *count *= scale;
            }
        }
    }
}

pub(crate) enum PromqlExponentialHistogramBucketIter<'a> {
    Dense {
        offset: i64,
        idx: usize,
        counts: std::slice::Iter<'a, f64>,
    },
    Sparse(std::slice::Iter<'a, (i32, f64)>),
}

impl Iterator for PromqlExponentialHistogramBucketIter<'_> {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PromqlExponentialHistogramBucketIter::Dense {
                offset,
                idx,
                counts,
            } => {
                let count = counts.next()?;
                let index = offset.checked_add(i64::try_from(*idx).ok()?)?;
                *idx = (*idx).saturating_add(1);
                Some((index, *count))
            }
            PromqlExponentialHistogramBucketIter::Sparse(counts) => counts
                .next()
                .map(|(index, count)| (i64::from(*index), *count)),
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

/// Query-result labels with the established owned-string layout, the prior
/// shared-string comparator, or query-local compact string IDs.
///
/// Iterate with [`Self::pairs`] or [`Self::visit_pairs`]. Callers that
/// explicitly need owned strings may use [`Self::to_vec`]; the returned copy
/// is caller-owned and is never retained inside the governed query result.
#[derive(Debug, Clone)]
pub struct QueryLabels(QueryLabelStorage);

#[derive(Debug, Clone)]
enum QueryLabelStorage {
    Owned(Arc<[(String, String)]>),
    Shared(Arc<SharedQueryLabels>),
    Compact(Arc<CompactQueryLabels>),
}

#[derive(Debug)]
struct SharedQueryLabels {
    pairs: Arc<[(Arc<str>, Arc<str>)]>,
}

const COMPACT_QUERY_LABEL_PAIR_BYTES: u64 = std::mem::size_of::<CompactQueryLabelPair>() as u64;
// `std::collections::HashMap` does not expose its raw table layout. Charge a
// deliberately conservative per-admission envelope (two full key/value
// entries plus control/allocation slack), and also retain a fixed first-table
// reserve below. This is a portable admission model, not allocator usable_size
// or a claim about implementation-specific HashMap capacity growth.
const COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES: u64 =
    (2 * std::mem::size_of::<(u64, SmallVec<[u32; 1]>)>() + 32) as u64;
const COMPACT_QUERY_LABEL_DERIVED_ENTRY_BYTES: u64 =
    (2 * std::mem::size_of::<((u32, &'static str), u32)>() + 32) as u64;
const COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES: u64 = 512;
const COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN: usize = 4096;
const COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN: usize = 4096;
const COMPACT_QUERY_LABEL_UNTRANSLATED: u32 = u32::MAX;

pub const DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_QUERY_LABEL_ARENA_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactQueryLabelPair {
    name_id: u32,
    value_id: u32,
}

#[derive(Debug, Clone, Copy)]
enum CompactQueryLabelChargeCategory {
    Atoms,
    Pairs,
    HashDirectory,
    Translations,
}

/// Releases an admitted modeled charge only after payload fields declared
/// before this guard have been dropped.
#[derive(Debug)]
struct CompactQueryLabelChargeGuard {
    arena: Arc<CompactQueryLabelArena>,
    category: CompactQueryLabelChargeCategory,
    bytes: AtomicU64,
}

impl CompactQueryLabelChargeGuard {
    fn from_reserved(
        arena: Arc<CompactQueryLabelArena>,
        category: CompactQueryLabelChargeCategory,
        bytes: u64,
    ) -> Self {
        Self {
            arena,
            category,
            bytes: AtomicU64::new(bytes),
        }
    }

    fn add_reserved(&self, bytes: u64) {
        self.bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(bytes)
            })
            .expect("compact query-label charge guard overflow");
    }
}

impl Drop for CompactQueryLabelChargeGuard {
    fn drop(&mut self) {
        self.arena
            .release_category(self.category, self.bytes.load(Ordering::Relaxed));
    }
}

#[derive(Debug)]
struct CompactQueryLabels {
    pairs: Box<[CompactQueryLabelPair]>,
    arena: Arc<CompactQueryLabelArena>,
    // Keep this last: fields are dropped in declaration order, so the pair
    // allocation is gone before its modeled charge becomes reusable.
    _charge_guard: CompactQueryLabelChargeGuard,
}

const fn align_up_saturating(value: usize, alignment: usize) -> usize {
    value.saturating_add(alignment.saturating_sub(1)) / alignment * alignment
}

const fn modeled_arc_allocation_bytes<T>() -> u64 {
    let header_bytes = 2usize.saturating_mul(std::mem::size_of::<usize>());
    let value_alignment = std::mem::align_of::<T>();
    let allocation_alignment = if value_alignment > std::mem::align_of::<usize>() {
        value_alignment
    } else {
        std::mem::align_of::<usize>()
    };
    let value_offset = align_up_saturating(header_bytes, value_alignment);
    align_up_saturating(
        value_offset.saturating_add(std::mem::size_of::<T>()),
        allocation_alignment,
    ) as u64
}

fn modeled_arc_str_allocation_bytes(content_bytes: u64) -> io::Result<u64> {
    let header_bytes =
        u64::try_from(2usize.saturating_mul(std::mem::size_of::<usize>())).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label atom header charge exceeds u64",
            )
        })?;
    let alignment = u64::try_from(std::mem::align_of::<usize>()).expect("usize alignment fits u64");
    let unaligned = header_bytes.checked_add(content_bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "compact query label atom charge overflows",
        )
    })?;
    unaligned
        .checked_add(alignment - 1)
        .map(|bytes| bytes / alignment * alignment)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label aligned atom charge overflows",
            )
        })
}

const COMPACT_QUERY_LABEL_OBJECT_BYTES: u64 = modeled_arc_allocation_bytes::<CompactQueryLabels>();

fn compact_query_label_block_bytes(pair_count: u64) -> io::Result<u64> {
    pair_count
        .checked_mul(COMPACT_QUERY_LABEL_PAIR_BYTES)
        .and_then(|bytes| bytes.checked_add(COMPACT_QUERY_LABEL_OBJECT_BYTES))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label-block charge overflows",
            )
        })
}

type CompactQueryLabelAtomChunk = Box<[OnceLock<Arc<str>>]>;
type CompactQueryLabelAtomDirectory = Box<[OnceLock<CompactQueryLabelAtomChunk>]>;

#[derive(Debug)]
struct CompactQueryLabelArena {
    max_bytes: u64,
    current_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    atom_bytes: AtomicU64,
    pair_bytes: AtomicU64,
    hash_directory_bytes: AtomicU64,
    translation_bytes: AtomicU64,
    admission_refusals: AtomicU64,
    compatibility_materializations: AtomicU64,
    atom_chunks: CompactQueryLabelAtomDirectory,
    hash_builder: ahash::RandomState,
    state: Mutex<CompactQueryLabelArenaState>,
}

#[derive(Debug, Default)]
struct CompactQueryLabelArenaState {
    hash_buckets: HashMap<u64, SmallVec<[u32; 1]>>,
    derived_metric_atoms: HashMap<(u32, &'static str), u32>,
    next_atom_id: u32,
    lookups: u64,
    hits: u64,
    misses: u64,
    unique_content_bytes: u64,
    label_sets: u64,
    label_pairs: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct CompactQueryLabelArenaSnapshot {
    lookups: u64,
    hits: u64,
    misses: u64,
    unique_content_bytes: u64,
    label_sets: u64,
    label_pairs: u64,
    current_bytes: u64,
    peak_bytes: u64,
    admission_refusals: u64,
    compatibility_materializations: u64,
    unique_strings: u64,
    budget_bytes: u64,
    atom_bytes: u64,
    pair_bytes: u64,
    hash_directory_bytes: u64,
    translation_bytes: u64,
}

impl CompactQueryLabelArena {
    fn new(max_bytes: u64) -> io::Result<Self> {
        if max_bytes > MAX_QUERY_LABEL_ARENA_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "query label arena budget {max_bytes} exceeds maximum {MAX_QUERY_LABEL_ARENA_BYTES}"
                ),
            ));
        }
        let minimum_atom_bytes = COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES.max(1);
        let max_atoms_from_budget = max_bytes
            .checked_div(minimum_atom_bytes)
            .unwrap_or(0)
            .saturating_add(1)
            .min(u64::from(u32::MAX));
        let max_atoms = usize::try_from(max_atoms_from_budget).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "query label arena atom limit exceeds usize",
            )
        })?;
        let chunk_count = max_atoms.saturating_add(COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN - 1)
            / COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let directory_bytes = u64::try_from(
            chunk_count.saturating_mul(std::mem::size_of::<OnceLock<CompactQueryLabelAtomChunk>>()),
        )
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "query label arena chunk-directory charge exceeds u64",
            )
        })?;
        let arena_base_bytes = modeled_arc_allocation_bytes::<CompactQueryLabelArena>()
            .checked_add(directory_bytes)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "query label arena base charge overflows",
                )
            })?;
        let initial_charge = arena_base_bytes
            .checked_add(COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "query label arena initial charge overflows",
                )
            })?;
        if initial_charge > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "query label arena budget of {max_bytes} bytes cannot admit its {initial_charge}-byte base allocation"
                ),
            ));
        }
        let mut chunks = Vec::new();
        chunks.try_reserve_exact(chunk_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "query label arena chunk-directory allocation failed",
            )
        })?;
        chunks.resize_with(chunk_count, OnceLock::new);
        Ok(Self {
            max_bytes,
            current_bytes: AtomicU64::new(initial_charge),
            peak_bytes: AtomicU64::new(initial_charge),
            // The atom-storage category includes the arena Arc/root and its
            // fixed atom-directory allocation in addition to admitted atom
            // chunks and Arc<str> payloads. Charges model requested live
            // allocation bytes; allocator metadata and size-class slack are
            // intentionally measured separately through process RSS.
            atom_bytes: AtomicU64::new(arena_base_bytes),
            pair_bytes: AtomicU64::new(0),
            hash_directory_bytes: AtomicU64::new(
                COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES,
            ),
            translation_bytes: AtomicU64::new(0),
            admission_refusals: AtomicU64::new(0),
            compatibility_materializations: AtomicU64::new(0),
            atom_chunks: chunks.into_boxed_slice(),
            hash_builder: ahash::RandomState::new(),
            state: Mutex::new(CompactQueryLabelArenaState::default()),
        })
    }

    fn reserve(&self, bytes: u64) -> io::Result<()> {
        let reserved =
            self.current_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|next| *next <= self.max_bytes)
                });
        let previous = match reserved {
            Ok(previous) => previous,
            Err(_) => {
                self.admission_refusals.fetch_add(1, Ordering::Relaxed);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!(
                        "query label arena budget of {} bytes refused {bytes} bytes",
                        self.max_bytes
                    ),
                ));
            }
        };
        self.peak_bytes
            .fetch_max(previous.saturating_add(bytes), Ordering::Relaxed);
        Ok(())
    }

    fn release(&self, bytes: u64) {
        self.current_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(bytes)
            })
            .expect("query label arena charge underflow");
    }

    fn category_counter(&self, category: CompactQueryLabelChargeCategory) -> &AtomicU64 {
        match category {
            CompactQueryLabelChargeCategory::Atoms => &self.atom_bytes,
            CompactQueryLabelChargeCategory::Pairs => &self.pair_bytes,
            CompactQueryLabelChargeCategory::HashDirectory => &self.hash_directory_bytes,
            CompactQueryLabelChargeCategory::Translations => &self.translation_bytes,
        }
    }

    fn reserve_category(
        &self,
        category: CompactQueryLabelChargeCategory,
        bytes: u64,
    ) -> io::Result<()> {
        self.reserve(bytes)?;
        self.category_counter(category)
            .fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    fn release_category(&self, category: CompactQueryLabelChargeCategory, bytes: u64) {
        self.category_counter(category)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(bytes)
            })
            .expect("query label category charge underflow");
        self.release(bytes);
    }

    fn intern_pairs(self: &Arc<Self>, labels: Vec<(String, String)>) -> io::Result<QueryLabels> {
        let pair_count = u64::try_from(labels.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label-pair count exceeds u64",
            )
        })?;
        let charged_label_block_bytes = compact_query_label_block_bytes(pair_count)?;
        // The pairs category covers both the Box<[CompactQueryLabelPair]>
        // payload and the one Arc<CompactQueryLabels> allocation that owns it.
        // QueryLabels clones share that block and add no arena charge.
        self.reserve_category(
            CompactQueryLabelChargeCategory::Pairs,
            charged_label_block_bytes,
        )?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut pairs = Vec::new();
        if pairs.try_reserve_exact(labels.len()).is_err() {
            drop(state);
            self.release_category(
                CompactQueryLabelChargeCategory::Pairs,
                charged_label_block_bytes,
            );
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label-pair allocation failed",
            ));
        }
        for (name, value) in labels {
            let name_id = match self.intern_locked(&mut state, &name) {
                Ok(id) => id,
                Err(error) => {
                    drop(state);
                    self.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        charged_label_block_bytes,
                    );
                    return Err(error);
                }
            };
            let value_id = match self.intern_locked(&mut state, &value) {
                Ok(id) => id,
                Err(error) => {
                    drop(state);
                    self.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        charged_label_block_bytes,
                    );
                    return Err(error);
                }
            };
            pairs.push(CompactQueryLabelPair { name_id, value_id });
        }
        state.label_sets = state.label_sets.saturating_add(1);
        state.label_pairs = state.label_pairs.saturating_add(pair_count);
        drop(state);
        Ok(self.labels_from_pairs_reserved(pairs, charged_label_block_bytes))
    }

    fn intern_borrowed(&self, value: &str) -> io::Result<u32> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.intern_locked(&mut state, value)
    }

    fn projected_metric_atom(&self, metric_name_id: u32, suffix: &'static str) -> io::Result<u32> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(&projected_id) = state.derived_metric_atoms.get(&(metric_name_id, suffix)) {
            return Ok(projected_id);
        }

        let metric_name = self.resolve(metric_name_id);
        let mut projected = String::new();
        projected
            .try_reserve_exact(metric_name.len().saturating_add(suffix.len()))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "projected compact metric-name allocation failed",
                )
            })?;
        projected.push_str(metric_name);
        projected.push_str(suffix);
        let projected_id = self.intern_locked(&mut state, &projected)?;

        self.reserve_category(
            CompactQueryLabelChargeCategory::HashDirectory,
            COMPACT_QUERY_LABEL_DERIVED_ENTRY_BYTES,
        )?;
        if state.derived_metric_atoms.try_reserve(1).is_err() {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_DERIVED_ENTRY_BYTES,
            );
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "projected compact metric-name cache allocation failed",
            ));
        }
        state
            .derived_metric_atoms
            .insert((metric_name_id, suffix), projected_id);
        Ok(projected_id)
    }

    fn project_metric_suffix(
        self: &Arc<Self>,
        labels: &CompactQueryLabels,
        suffix: &'static str,
    ) -> io::Result<QueryLabels> {
        if !Arc::ptr_eq(self, &labels.arena) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compact projected labels belong to a different query arena",
            ));
        }
        let metric_pair_index = labels
            .pairs
            .iter()
            .position(|pair| self.resolve(pair.name_id) == METRIC_NAME_LABEL)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "verified compact labels are missing __name__ during typed projection",
                )
            })?;
        let metric_name_id = labels.pairs[metric_pair_index].value_id;
        let projected_metric_id = self.projected_metric_atom(metric_name_id, suffix)?;
        let pair_count = u64::try_from(labels.pairs.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "projected compact label-pair count exceeds u64",
            )
        })?;
        let label_block_bytes = compact_query_label_block_bytes(pair_count)?;
        self.reserve_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes)?;
        let mut pairs = Vec::new();
        if pairs.try_reserve_exact(labels.pairs.len()).is_err() {
            self.release_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "projected compact label-pair allocation failed",
            ));
        }
        pairs.extend_from_slice(&labels.pairs);
        pairs[metric_pair_index].value_id = projected_metric_id;
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.label_sets = state.label_sets.saturating_add(1);
            state.label_pairs = state.label_pairs.saturating_add(pair_count);
        }
        Ok(self.labels_from_pairs_reserved(pairs, label_block_bytes))
    }

    fn intern_locked(
        &self,
        state: &mut CompactQueryLabelArenaState,
        value: &str,
    ) -> io::Result<u32> {
        let content_hash = self.hash_builder.hash_one(value.as_bytes());
        self.intern_locked_with_hash(state, value, content_hash)
    }

    fn intern_locked_with_hash(
        &self,
        state: &mut CompactQueryLabelArenaState,
        value: &str,
        content_hash: u64,
    ) -> io::Result<u32> {
        state.lookups = state.lookups.saturating_add(1);
        if let Some(ids) = state.hash_buckets.get(&content_hash) {
            for &id in ids {
                if self.resolve(id) == value {
                    state.hits = state.hits.saturating_add(1);
                    return Ok(id);
                }
            }
        }

        let id = state.next_atom_id;
        let next_atom_id = id
            .checked_add(1)
            .filter(|next| *next != u32::MAX)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label arena exceeds usable u32 atom IDs",
                )
            })?;
        let atom_index = usize::try_from(id).expect("u32 query label ID fits usize");
        let chunk_index = atom_index / COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let local_index = atom_index % COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let chunk_slot = self.atom_chunks.get(chunk_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label arena exceeds its configured atom directory",
            )
        })?;
        let new_chunk_bytes = if chunk_slot.get().is_none() {
            u64::try_from(
                COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN
                    .saturating_mul(std::mem::size_of::<OnceLock<Arc<str>>>()),
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label atom-chunk charge exceeds u64",
                )
            })?
        } else {
            0
        };
        let content_bytes = u64::try_from(value.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label length exceeds u64",
            )
        })?;
        let arc_allocation_bytes = modeled_arc_str_allocation_bytes(content_bytes)?;
        let atom_charge = new_chunk_bytes
            .checked_add(arc_allocation_bytes)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label atom charge overflows",
                )
            })?;
        self.reserve_category(CompactQueryLabelChargeCategory::Atoms, atom_charge)?;
        if let Err(error) = self.reserve_category(
            CompactQueryLabelChargeCategory::HashDirectory,
            COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
        ) {
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(error);
        }

        let mut new_chunk = None;
        if new_chunk_bytes != 0 {
            let mut slots = Vec::new();
            if slots
                .try_reserve_exact(COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN)
                .is_err()
            {
                self.release_category(
                    CompactQueryLabelChargeCategory::HashDirectory,
                    COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
                );
                self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compact query label atom-chunk allocation failed",
                ));
            }
            slots.resize_with(COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN, OnceLock::new);
            new_chunk = Some(slots.into_boxed_slice());
        }
        let hash_capacity_failed = if let Some(ids) = state.hash_buckets.get_mut(&content_hash) {
            ids.try_reserve(1).is_err()
        } else {
            state.hash_buckets.try_reserve(1).is_err()
        };
        if hash_capacity_failed {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
            );
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact query label hash-directory allocation failed",
            ));
        }
        let atom: Arc<str> = Arc::from(value);
        if let Some(new_chunk) = new_chunk
            && chunk_slot.set(new_chunk).is_err()
        {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
            );
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(io::Error::other(
                "compact query label atom chunk was initialized concurrently",
            ));
        }
        let slot = &chunk_slot
            .get()
            .expect("compact query label chunk was initialized")[local_index];
        if slot.set(atom).is_err() {
            self.release_category(
                CompactQueryLabelChargeCategory::HashDirectory,
                COMPACT_QUERY_LABEL_HASH_ENTRY_BYTES,
            );
            self.release_category(CompactQueryLabelChargeCategory::Atoms, atom_charge);
            return Err(io::Error::other(
                "compact query label atom slot was initialized concurrently",
            ));
        }
        state.hash_buckets.entry(content_hash).or_default().push(id);
        state.next_atom_id = next_atom_id;
        state.misses = state.misses.saturating_add(1);
        state.unique_content_bytes = state.unique_content_bytes.saturating_add(content_bytes);
        Ok(id)
    }

    fn resolve(&self, id: u32) -> &str {
        let atom_index = usize::try_from(id).expect("u32 query label ID fits usize");
        let chunk_index = atom_index / COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        let local_index = atom_index % COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN;
        self.atom_chunks[chunk_index]
            .get()
            .expect("published compact query label chunk exists")[local_index]
            .get()
            .expect("published compact query label atom exists")
            .as_ref()
    }

    fn labels_from_pairs_reserved(
        self: &Arc<Self>,
        pairs: Vec<CompactQueryLabelPair>,
        charged_label_block_bytes: u64,
    ) -> QueryLabels {
        QueryLabels(QueryLabelStorage::Compact(Arc::new(CompactQueryLabels {
            pairs: pairs.into_boxed_slice(),
            arena: Arc::clone(self),
            _charge_guard: CompactQueryLabelChargeGuard::from_reserved(
                Arc::clone(self),
                CompactQueryLabelChargeCategory::Pairs,
                charged_label_block_bytes,
            ),
        })))
    }

    fn snapshot(&self) -> CompactQueryLabelArenaSnapshot {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        CompactQueryLabelArenaSnapshot {
            lookups: state.lookups,
            hits: state.hits,
            misses: state.misses,
            unique_content_bytes: state.unique_content_bytes,
            label_sets: state.label_sets,
            label_pairs: state.label_pairs,
            current_bytes: self.current_bytes.load(Ordering::Relaxed),
            peak_bytes: self.peak_bytes.load(Ordering::Relaxed),
            admission_refusals: self.admission_refusals.load(Ordering::Relaxed),
            compatibility_materializations: self
                .compatibility_materializations
                .load(Ordering::Relaxed),
            unique_strings: state.misses,
            budget_bytes: self.max_bytes,
            atom_bytes: self.atom_bytes.load(Ordering::Relaxed),
            pair_bytes: self.pair_bytes.load(Ordering::Relaxed),
            hash_directory_bytes: self.hash_directory_bytes.load(Ordering::Relaxed),
            translation_bytes: self.translation_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct SegmentAtomTranslations {
    provenance: SegmentGenerationProvenance,
    symbol_count: u32,
    pages: Box<[Option<Box<[u32]>>]>,
    // Keep this last so the page directory and admitted pages are dropped
    // before their modeled charge is released.
    charge_guard: CompactQueryLabelChargeGuard,
}

// Vec growth is implementation-specific. Four element widths per logical
// admission is the deliberately conservative portable model used here; RSS
// remains the authority for allocator capacity/slack.
const COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES: u64 =
    (4 * std::mem::size_of::<SegmentAtomTranslations>()) as u64;

impl SegmentAtomTranslations {
    fn new(
        provenance: SegmentGenerationProvenance,
        symbol_count: u32,
        arena: Arc<CompactQueryLabelArena>,
    ) -> io::Result<Self> {
        let symbol_count = usize::try_from(symbol_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "segment symbol count exceeds usize",
            )
        })?;
        let page_count = symbol_count
            .checked_add(COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN - 1)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "segment translation page count overflows",
                )
            })?
            / COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        let directory_payload_bytes =
            u64::try_from(page_count.saturating_mul(std::mem::size_of::<Option<Box<[u32]>>>()))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "segment translation directory charge exceeds u64",
                    )
                })?;
        let directory_bytes = directory_payload_bytes;
        arena.reserve_category(
            CompactQueryLabelChargeCategory::Translations,
            directory_bytes,
        )?;
        let mut pages = Vec::new();
        if pages.try_reserve_exact(page_count).is_err() {
            arena.release_category(
                CompactQueryLabelChargeCategory::Translations,
                directory_bytes,
            );
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "segment translation directory allocation failed",
            ));
        }
        pages.resize_with(page_count, || None);
        Ok(Self {
            provenance,
            symbol_count: u32::try_from(symbol_count).expect("source symbol count came from u32"),
            pages: pages.into_boxed_slice(),
            charge_guard: CompactQueryLabelChargeGuard::from_reserved(
                arena,
                CompactQueryLabelChargeCategory::Translations,
                directory_bytes,
            ),
        })
    }

    fn lookup(&self, source_id: u32) -> io::Result<Option<u32>> {
        if source_id >= self.symbol_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source symbol ID {source_id} exceeds segment symbol count {}",
                    self.symbol_count
                ),
            ));
        }
        let source_index = usize::try_from(source_id).expect("u32 source symbol ID fits usize");
        let page_index = source_index / COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        let local_index = source_index % COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        Ok(self.pages[page_index].as_ref().and_then(|page| {
            let value = page[local_index];
            (value != COMPACT_QUERY_LABEL_UNTRANSLATED).then_some(value)
        }))
    }

    fn publish(&mut self, source_id: u32, query_id: u32) -> io::Result<()> {
        if query_id == COMPACT_QUERY_LABEL_UNTRANSLATED {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "query label atom ID collides with translation sentinel",
            ));
        }
        if source_id >= self.symbol_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source symbol ID {source_id} exceeds segment symbol count {}",
                    self.symbol_count
                ),
            ));
        }
        let source_index = usize::try_from(source_id).expect("u32 source symbol ID fits usize");
        let page_index = source_index / COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        let local_index = source_index % COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN;
        if self.pages[page_index].is_none() {
            let page_bytes = u64::try_from(
                COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN.saturating_mul(std::mem::size_of::<u32>()),
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "segment translation page charge exceeds u64",
                )
            })?;
            self.charge_guard
                .arena
                .reserve_category(CompactQueryLabelChargeCategory::Translations, page_bytes)?;
            let mut page = Vec::new();
            if page
                .try_reserve_exact(COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN)
                .is_err()
            {
                self.charge_guard
                    .arena
                    .release_category(CompactQueryLabelChargeCategory::Translations, page_bytes);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "segment translation page allocation failed",
                ));
            }
            page.resize(
                COMPACT_QUERY_LABEL_TRANSLATION_PAGE_LEN,
                COMPACT_QUERY_LABEL_UNTRANSLATED,
            );
            self.pages[page_index] = Some(page.into_boxed_slice());
            self.charge_guard.add_reserved(page_bytes);
        }
        let slot = &mut self.pages[page_index]
            .as_mut()
            .expect("translation page was initialized")[local_index];
        if *slot != COMPACT_QUERY_LABEL_UNTRANSLATED && *slot != query_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source symbol translation changed within one segment generation",
            ));
        }
        *slot = query_id;
        Ok(())
    }
}

pub struct QueryLabelPairs<'a> {
    inner: QueryLabelPairsInner<'a>,
}

enum QueryLabelPairsInner<'a> {
    Owned(std::slice::Iter<'a, (String, String)>),
    Shared(std::slice::Iter<'a, (Arc<str>, Arc<str>)>),
    Compact {
        pairs: std::slice::Iter<'a, CompactQueryLabelPair>,
        arena: &'a CompactQueryLabelArena,
    },
}

impl<'a> Iterator for QueryLabelPairs<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            QueryLabelPairsInner::Owned(pairs) => pairs
                .next()
                .map(|(name, value)| (name.as_str(), value.as_str())),
            QueryLabelPairsInner::Shared(pairs) => pairs
                .next()
                .map(|(name, value)| (name.as_ref(), value.as_ref())),
            QueryLabelPairsInner::Compact { pairs, arena } => pairs
                .next()
                .map(|pair| (arena.resolve(pair.name_id), arena.resolve(pair.value_id))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            QueryLabelPairsInner::Owned(pairs) => pairs.size_hint(),
            QueryLabelPairsInner::Shared(pairs) => pairs.size_hint(),
            QueryLabelPairsInner::Compact { pairs, .. } => pairs.size_hint(),
        }
    }
}

impl ExactSizeIterator for QueryLabelPairs<'_> {}

impl std::iter::FusedIterator for QueryLabelPairs<'_> {}

impl QueryLabels {
    pub(crate) fn from_vec(labels: Vec<(String, String)>) -> Self {
        Self(QueryLabelStorage::Owned(Arc::from(
            labels.into_boxed_slice(),
        )))
    }

    fn from_shared(pairs: Vec<(Arc<str>, Arc<str>)>) -> Self {
        Self(QueryLabelStorage::Shared(Arc::new(SharedQueryLabels {
            pairs: Arc::from(pairs.into_boxed_slice()),
        })))
    }

    pub fn pairs(&self) -> QueryLabelPairs<'_> {
        let inner = match &self.0 {
            QueryLabelStorage::Owned(labels) => QueryLabelPairsInner::Owned(labels.iter()),
            QueryLabelStorage::Shared(labels) => QueryLabelPairsInner::Shared(labels.pairs.iter()),
            QueryLabelStorage::Compact(labels) => QueryLabelPairsInner::Compact {
                pairs: labels.pairs.iter(),
                arena: &labels.arena,
            },
        };
        QueryLabelPairs { inner }
    }

    /// Iterates borrowed label strings without materializing an owned slice.
    pub fn iter(&self) -> QueryLabelPairs<'_> {
        self.pairs()
    }

    /// Visits labels without forcing the owned-string compatibility view.
    pub fn visit_pairs(&self, mut visit: impl FnMut(&str, &str)) {
        for (name, value) in self.pairs() {
            visit(name, value);
        }
    }

    pub fn len(&self) -> usize {
        match &self.0 {
            QueryLabelStorage::Owned(labels) => labels.len(),
            QueryLabelStorage::Shared(labels) => labels.pairs.len(),
            QueryLabelStorage::Compact(labels) => labels.pairs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn uses_shared_atoms(&self) -> bool {
        matches!(&self.0, QueryLabelStorage::Shared(_))
    }

    pub(crate) fn uses_compact_ids(&self) -> bool {
        matches!(&self.0, QueryLabelStorage::Compact(_))
    }

    pub fn to_vec(&self) -> Vec<(String, String)> {
        match &self.0 {
            QueryLabelStorage::Owned(labels) => labels.to_vec(),
            QueryLabelStorage::Shared(labels) => labels
                .pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            QueryLabelStorage::Compact(_) => self
                .pairs()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
        }
    }

    /// Narrows an already matcher-verified selective label set to the names
    /// that the terminal aggregation may expose. Compact labels retain their
    /// query-global IDs and allocate only a new governed pair vector.
    pub(crate) fn try_retain_names(self, names: &[String]) -> io::Result<Self> {
        let selected = |name: &str| {
            names
                .binary_search_by(|candidate| candidate.as_str().cmp(name))
                .is_ok()
        };
        if self.pairs().all(|(name, _)| selected(name)) {
            return Ok(self);
        }

        match self.0 {
            QueryLabelStorage::Owned(labels) => {
                let selected_count = labels.iter().filter(|(name, _)| selected(name)).count();
                let mut output = Vec::new();
                output.try_reserve_exact(selected_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected owned query-label allocation failed",
                    )
                })?;
                output.extend(labels.iter().filter(|(name, _)| selected(name)).cloned());
                Ok(Self::from_vec(output))
            }
            QueryLabelStorage::Shared(labels) => {
                let selected_count = labels
                    .pairs
                    .iter()
                    .filter(|(name, _)| selected(name))
                    .count();
                let mut output = Vec::new();
                output.try_reserve_exact(selected_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected shared query-label allocation failed",
                    )
                })?;
                output.extend(
                    labels
                        .pairs
                        .iter()
                        .filter(|(name, _)| selected(name))
                        .cloned(),
                );
                Ok(Self::from_shared(output))
            }
            QueryLabelStorage::Compact(labels) => {
                let selected_count = labels
                    .pairs
                    .iter()
                    .filter(|pair| selected(labels.arena.resolve(pair.name_id)))
                    .count();
                let pair_count = u64::try_from(selected_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected compact query-label pair count exceeds u64",
                    )
                })?;
                let label_block_bytes = compact_query_label_block_bytes(pair_count)?;
                labels
                    .arena
                    .reserve_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes)?;
                let mut output = Vec::new();
                if output.try_reserve_exact(selected_count).is_err() {
                    labels.arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "projected compact query-label allocation failed",
                    ));
                }
                output.extend(
                    labels
                        .pairs
                        .iter()
                        .filter(|pair| selected(labels.arena.resolve(pair.name_id)))
                        .copied(),
                );
                {
                    let mut state = labels
                        .arena
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.label_sets = state.label_sets.saturating_add(1);
                    state.label_pairs = state.label_pairs.saturating_add(pair_count);
                }
                Ok(labels
                    .arena
                    .labels_from_pairs_reserved(output, label_block_bytes))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (QueryLabelStorage::Owned(left), QueryLabelStorage::Owned(right)) => {
                Arc::ptr_eq(left, right)
            }
            (QueryLabelStorage::Shared(left), QueryLabelStorage::Shared(right)) => {
                Arc::ptr_eq(left, right)
            }
            (QueryLabelStorage::Compact(left), QueryLabelStorage::Compact(right)) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn shared_atom_ptrs(&self) -> Option<Vec<(*const str, *const str)>> {
        match &self.0 {
            QueryLabelStorage::Owned(_) => None,
            QueryLabelStorage::Shared(labels) => Some(
                labels
                    .pairs
                    .iter()
                    .map(|(name, value)| (Arc::as_ptr(name), Arc::as_ptr(value)))
                    .collect(),
            ),
            QueryLabelStorage::Compact(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn owned_compatibility_materialized(&self) -> bool {
        false
    }

    #[cfg(test)]
    pub(crate) fn compact_charge_categories_for_test(&self) -> Option<(u64, u64, u64, u64, u64)> {
        let QueryLabelStorage::Compact(labels) = &self.0 else {
            return None;
        };
        let snapshot = labels.arena.snapshot();
        Some((
            snapshot.current_bytes,
            snapshot.atom_bytes,
            snapshot.pair_bytes,
            snapshot.hash_directory_bytes,
            snapshot.translation_bytes,
        ))
    }

    /// Test diagnostic for consumers that must not force the owned-string
    /// compatibility view while handling shared query-label atoms.
    #[doc(hidden)]
    pub fn shared_atoms_compatibility_view_materialized_for_test(&self) -> Option<bool> {
        match &self.0 {
            QueryLabelStorage::Owned(_) => None,
            QueryLabelStorage::Shared(_) => Some(false),
            QueryLabelStorage::Compact(_) => None,
        }
    }

    #[doc(hidden)]
    pub fn compact_ids_compatibility_view_materialized_for_test(&self) -> Option<bool> {
        match &self.0 {
            QueryLabelStorage::Compact(_) => Some(false),
            QueryLabelStorage::Owned(_) | QueryLabelStorage::Shared(_) => None,
        }
    }
}

impl PartialEq for QueryLabels {
    fn eq(&self, other: &Self) -> bool {
        if let (QueryLabelStorage::Compact(left), QueryLabelStorage::Compact(right)) =
            (&self.0, &other.0)
            && Arc::ptr_eq(&left.arena, &right.arena)
        {
            return left.pairs == right.pairs;
        }
        self.pairs().eq(other.pairs())
    }
}

impl Eq for QueryLabels {}

impl PartialOrd for QueryLabels {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueryLabels {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pairs().cmp(other.pairs())
    }
}

impl PartialEq<Vec<(String, String)>> for QueryLabels {
    fn eq(&self, other: &Vec<(String, String)>) -> bool {
        self.pairs().eq(other
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())))
    }
}

impl PartialEq<QueryLabels> for Vec<(String, String)> {
    fn eq(&self, other: &QueryLabels) -> bool {
        other == self
    }
}

pub(crate) fn shared_query_labels(labels: Vec<(String, String)>) -> QueryLabels {
    QueryLabels::from_vec(labels)
}

pub(crate) fn query_labels_series_id(labels: &QueryLabels) -> u64 {
    let mut hash = XxHash64::default();
    for (name, value) in labels.pairs() {
        hash.update(name.as_bytes());
        hash.update(&[0]);
        hash.update(value.as_bytes());
        hash.update(&[0xff]);
    }
    hash.finish()
}

/// Runtime-selectable source-label representation for one query session.
/// `OwnedStrings` is the exact established ownership comparator; it is never
/// selected as corruption recovery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryLabelStoragePolicy {
    SharedAtoms,
    CompactIds,
    #[default]
    OwnedStrings,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLabelStorageStats {
    pub label_sets: u64,
    pub atom_lookups: u64,
    pub atom_hits: u64,
    pub atom_misses: u64,
    pub unique_content_bytes: u64,
    pub compact_label_sets: u64,
    pub compact_pairs: u64,
    pub compact_atom_lookups: u64,
    pub compact_atom_hits: u64,
    pub compact_atom_misses: u64,
    pub compact_source_symbol_translations: u64,
    pub compact_source_symbol_translation_hits: u64,
    pub compact_source_symbol_translation_misses: u64,
    pub compact_unique_strings: u64,
    pub compact_unique_content_bytes: u64,
    pub compact_arena_budget_bytes: u64,
    pub compact_arena_current_bytes: u64,
    pub compact_arena_peak_bytes: u64,
    pub compact_atom_bytes: u64,
    pub compact_pair_bytes: u64,
    pub compact_hash_directory_bytes: u64,
    pub compact_translation_bytes: u64,
    pub compact_retained_bytes: u64,
    pub compact_arena_admission_refusals: u64,
    pub compact_compatibility_materializations: u64,
}

impl QueryLabelStorageStats {
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            label_sets: self.label_sets.saturating_sub(earlier.label_sets),
            atom_lookups: self.atom_lookups.saturating_sub(earlier.atom_lookups),
            atom_hits: self.atom_hits.saturating_sub(earlier.atom_hits),
            atom_misses: self.atom_misses.saturating_sub(earlier.atom_misses),
            unique_content_bytes: self
                .unique_content_bytes
                .saturating_sub(earlier.unique_content_bytes),
            compact_label_sets: self
                .compact_label_sets
                .saturating_sub(earlier.compact_label_sets),
            compact_pairs: self.compact_pairs.saturating_sub(earlier.compact_pairs),
            compact_atom_lookups: self
                .compact_atom_lookups
                .saturating_sub(earlier.compact_atom_lookups),
            compact_atom_hits: self
                .compact_atom_hits
                .saturating_sub(earlier.compact_atom_hits),
            compact_atom_misses: self
                .compact_atom_misses
                .saturating_sub(earlier.compact_atom_misses),
            compact_source_symbol_translations: self
                .compact_source_symbol_translations
                .saturating_sub(earlier.compact_source_symbol_translations),
            compact_source_symbol_translation_hits: self
                .compact_source_symbol_translation_hits
                .saturating_sub(earlier.compact_source_symbol_translation_hits),
            compact_source_symbol_translation_misses: self
                .compact_source_symbol_translation_misses
                .saturating_sub(earlier.compact_source_symbol_translation_misses),
            compact_unique_strings: self
                .compact_unique_strings
                .saturating_sub(earlier.compact_unique_strings),
            compact_unique_content_bytes: self
                .compact_unique_content_bytes
                .saturating_sub(earlier.compact_unique_content_bytes),
            compact_arena_budget_bytes: self.compact_arena_budget_bytes,
            compact_arena_current_bytes: self.compact_arena_current_bytes,
            compact_arena_peak_bytes: self.compact_arena_peak_bytes,
            compact_atom_bytes: self.compact_atom_bytes,
            compact_pair_bytes: self.compact_pair_bytes,
            compact_hash_directory_bytes: self.compact_hash_directory_bytes,
            compact_translation_bytes: self.compact_translation_bytes,
            compact_retained_bytes: self.compact_retained_bytes,
            compact_arena_admission_refusals: self
                .compact_arena_admission_refusals
                .saturating_sub(earlier.compact_arena_admission_refusals),
            compact_compatibility_materializations: self
                .compact_compatibility_materializations
                .saturating_sub(earlier.compact_compatibility_materializations),
        }
    }
}

#[derive(Debug)]
pub(super) struct QueryLabelInterner {
    policy: QueryLabelStoragePolicy,
    atoms: HashSet<Arc<str>>,
    stats: QueryLabelStorageStats,
    compact_arena_max_bytes: u64,
    compact_arena: Option<Arc<CompactQueryLabelArena>>,
    compact_translations: Vec<SegmentAtomTranslations>,
    // Keep this after the Vec: its modeled capacity charge is released only
    // after the translation elements and outer Vec allocation are dropped.
    compact_translation_list_charge: Option<CompactQueryLabelChargeGuard>,
}

impl Default for QueryLabelInterner {
    fn default() -> Self {
        Self {
            policy: QueryLabelStoragePolicy::default(),
            atoms: HashSet::default(),
            stats: QueryLabelStorageStats::default(),
            compact_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
            compact_arena: None,
            compact_translations: Vec::new(),
            compact_translation_list_charge: None,
        }
    }
}

impl QueryLabelInterner {
    pub(super) fn set_policy(&mut self, policy: QueryLabelStoragePolicy) {
        self.policy = policy;
    }

    pub(super) fn set_compact_arena_max_bytes(&mut self, max_bytes: u64) -> io::Result<()> {
        if max_bytes > MAX_QUERY_LABEL_ARENA_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "query label arena budget {max_bytes} exceeds maximum {MAX_QUERY_LABEL_ARENA_BYTES}"
                ),
            ));
        }
        if self.compact_arena.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query label arena budget must be set before compact labels are created",
            ));
        }
        self.compact_arena_max_bytes = max_bytes;
        Ok(())
    }

    pub(super) fn policy(&self) -> QueryLabelStoragePolicy {
        self.policy
    }

    pub(super) fn stats(&self) -> QueryLabelStorageStats {
        let compact = self
            .compact_arena
            .as_deref()
            .map(CompactQueryLabelArena::snapshot)
            .unwrap_or_else(|| CompactQueryLabelArenaSnapshot {
                budget_bytes: if self.policy == QueryLabelStoragePolicy::CompactIds {
                    self.compact_arena_max_bytes
                } else {
                    0
                },
                admission_refusals: self.stats.compact_arena_admission_refusals,
                ..CompactQueryLabelArenaSnapshot::default()
            });
        QueryLabelStorageStats {
            compact_label_sets: compact.label_sets,
            compact_pairs: compact.label_pairs,
            compact_atom_lookups: compact.lookups,
            compact_atom_hits: compact.hits,
            compact_atom_misses: compact.misses,
            compact_unique_strings: compact.unique_strings,
            compact_unique_content_bytes: compact.unique_content_bytes,
            compact_arena_budget_bytes: compact.budget_bytes,
            compact_arena_current_bytes: compact.current_bytes,
            compact_arena_peak_bytes: compact.peak_bytes,
            compact_atom_bytes: compact.atom_bytes,
            compact_pair_bytes: compact.pair_bytes,
            compact_hash_directory_bytes: compact.hash_directory_bytes,
            compact_translation_bytes: compact.translation_bytes,
            compact_retained_bytes: compact.current_bytes,
            compact_arena_admission_refusals: compact.admission_refusals,
            compact_compatibility_materializations: compact.compatibility_materializations,
            ..self.stats
        }
    }

    pub(super) fn try_intern_labels(
        &mut self,
        labels: Vec<(String, String)>,
    ) -> io::Result<QueryLabels> {
        self.stats.label_sets = self.stats.label_sets.saturating_add(1);
        if self.policy == QueryLabelStoragePolicy::OwnedStrings {
            return Ok(QueryLabels::from_vec(labels));
        }
        if self.policy == QueryLabelStoragePolicy::CompactIds {
            let arena = self.compact_arena()?;
            return arena.intern_pairs(labels);
        }

        let pairs = labels
            .into_iter()
            .map(|(name, value)| (self.intern(name), self.intern(value)))
            .collect();
        Ok(QueryLabels::from_shared(pairs))
    }

    /// Rewrites the canonical metric name for a typed PromQL scalar
    /// projection. Compact inputs retain every existing ID and intern the
    /// derived metric atom once per `(metric_name_id, suffix)` pair.
    pub(super) fn try_project_metric_suffix_labels(
        &mut self,
        labels: &QueryLabels,
        suffix: &'static str,
    ) -> io::Result<QueryLabels> {
        if self.policy == QueryLabelStoragePolicy::CompactIds
            && let QueryLabelStorage::Compact(compact) = &labels.0
        {
            self.stats.label_sets = self.stats.label_sets.saturating_add(1);
            let arena = self.compact_arena()?;
            return arena.project_metric_suffix(compact, suffix);
        }

        let mut projected = labels.to_vec();
        let mut metric_seen = false;
        for (name, value) in &mut projected {
            if name == METRIC_NAME_LABEL {
                value.push_str(suffix);
                metric_seen = true;
                break;
            }
        }
        if !metric_seen {
            projected.push((METRIC_NAME_LABEL.to_string(), suffix.to_string()));
            projected.sort_by(|left, right| left.0.cmp(&right.0));
        }
        self.try_intern_labels(projected)
    }

    /// Translates one fully verified schema-7/8 source row into compact
    /// query-global IDs. A generation-local source symbol is hashed and
    /// compared only on its first translation miss; all later occurrences are
    /// direct paged-table lookups.
    pub(super) fn try_intern_encoded_labels(
        &mut self,
        labels: SegmentEncodedLabels<'_>,
    ) -> io::Result<QueryLabels> {
        if self.policy != QueryLabelStoragePolicy::CompactIds {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "encoded source labels require the compact-ids query label policy",
            ));
        }
        self.stats.label_sets = self.stats.label_sets.saturating_add(1);
        let arena = self.compact_arena()?;
        let pair_count = u64::try_from(labels.pairs().len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact source-label pair count exceeds u64",
            )
        })?;
        let label_block_bytes = compact_query_label_block_bytes(pair_count)?;
        arena.reserve_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes)?;
        let mut pairs = Vec::new();
        if pairs.try_reserve_exact(labels.pairs().len()).is_err() {
            arena.release_category(CompactQueryLabelChargeCategory::Pairs, label_block_bytes);
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "compact source-label pair allocation failed",
            ));
        }

        let translation_index = match self
            .compact_translations
            .iter()
            .position(|translation| translation.provenance.same_generation(labels.provenance()))
        {
            Some(index) => {
                if self.compact_translations[index].symbol_count != labels.symbol_count() {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "same-generation source symbol count changed during query",
                    ));
                }
                index
            }
            None => {
                let translation = match SegmentAtomTranslations::new(
                    labels.provenance().clone(),
                    labels.symbol_count(),
                    Arc::clone(&arena),
                ) {
                    Ok(translation) => translation,
                    Err(error) => {
                        arena.release_category(
                            CompactQueryLabelChargeCategory::Pairs,
                            label_block_bytes,
                        );
                        return Err(error);
                    }
                };
                if let Err(error) = arena.reserve_category(
                    CompactQueryLabelChargeCategory::Translations,
                    COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES,
                ) {
                    drop(translation);
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(error);
                }
                if self.compact_translations.try_reserve(1).is_err() {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Translations,
                        COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES,
                    );
                    drop(translation);
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "segment translation-table list allocation failed",
                    ));
                }
                self.compact_translations.push(translation);
                if let Some(charge) = &self.compact_translation_list_charge {
                    charge.add_reserved(COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES);
                } else {
                    self.compact_translation_list_charge =
                        Some(CompactQueryLabelChargeGuard::from_reserved(
                            Arc::clone(&arena),
                            CompactQueryLabelChargeCategory::Translations,
                            COMPACT_QUERY_LABEL_TRANSLATION_LIST_ENTRY_BYTES,
                        ));
                }
                self.compact_translations.len() - 1
            }
        };

        for &(source_name_id, source_value_id) in labels.pairs() {
            let name_id = match self.translate_source_symbol(
                translation_index,
                source_name_id,
                labels,
                &arena,
            ) {
                Ok(id) => id,
                Err(error) => {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(error);
                }
            };
            let value_id = match self.translate_source_symbol(
                translation_index,
                source_value_id,
                labels,
                &arena,
            ) {
                Ok(id) => id,
                Err(error) => {
                    arena.release_category(
                        CompactQueryLabelChargeCategory::Pairs,
                        label_block_bytes,
                    );
                    return Err(error);
                }
            };
            pairs.push(CompactQueryLabelPair { name_id, value_id });
        }
        {
            let mut state = arena
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.label_sets = state.label_sets.saturating_add(1);
            state.label_pairs = state.label_pairs.saturating_add(pair_count);
        }
        Ok(arena.labels_from_pairs_reserved(pairs, label_block_bytes))
    }

    fn translate_source_symbol(
        &mut self,
        translation_index: usize,
        source_id: u32,
        labels: SegmentEncodedLabels<'_>,
        arena: &Arc<CompactQueryLabelArena>,
    ) -> io::Result<u32> {
        self.stats.compact_source_symbol_translations = self
            .stats
            .compact_source_symbol_translations
            .saturating_add(1);
        if let Some(query_id) = self.compact_translations[translation_index].lookup(source_id)? {
            self.stats.compact_source_symbol_translation_hits = self
                .stats
                .compact_source_symbol_translation_hits
                .saturating_add(1);
            return Ok(query_id);
        }
        self.stats.compact_source_symbol_translation_misses = self
            .stats
            .compact_source_symbol_translation_misses
            .saturating_add(1);
        let mut query_id = None;
        labels.visit_required_symbol(source_id, |resolved| {
            query_id = Some(arena.intern_borrowed(resolved)?);
            Ok(())
        })?;
        let query_id = query_id.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "required source symbol resolver returned no value",
            )
        })?;
        self.compact_translations[translation_index].publish(source_id, query_id)?;
        Ok(query_id)
    }

    #[cfg(test)]
    pub(super) fn intern_labels(&mut self, labels: Vec<(String, String)>) -> QueryLabels {
        self.try_intern_labels(labels)
            .expect("legacy query label policies are infallible")
    }

    pub(super) fn intern_result_labels(
        &mut self,
        results: &mut [SegmentQueryResult],
    ) -> io::Result<()> {
        if self.policy == QueryLabelStoragePolicy::OwnedStrings {
            return Ok(());
        }
        for result in results {
            if result.labels.uses_shared_atoms() || result.labels.uses_compact_ids() {
                continue;
            }
            result.labels = self.try_intern_labels(result.labels.to_vec())?;
        }
        Ok(())
    }

    fn intern(&mut self, value: String) -> Arc<str> {
        self.stats.atom_lookups = self.stats.atom_lookups.saturating_add(1);
        let content_bytes = u64::try_from(value.len()).unwrap_or(u64::MAX);
        let (atom, inserted) = intern_query_label_atom(&mut self.atoms, value);
        if !inserted {
            self.stats.atom_hits = self.stats.atom_hits.saturating_add(1);
            return atom;
        }

        self.stats.atom_misses = self.stats.atom_misses.saturating_add(1);
        self.stats.unique_content_bytes = self
            .stats
            .unique_content_bytes
            .saturating_add(content_bytes);
        atom
    }

    fn compact_arena(&mut self) -> io::Result<Arc<CompactQueryLabelArena>> {
        if let Some(arena) = &self.compact_arena {
            return Ok(Arc::clone(arena));
        }
        let arena = match CompactQueryLabelArena::new(self.compact_arena_max_bytes) {
            Ok(arena) => Arc::new(arena),
            Err(error) => {
                self.stats.compact_arena_admission_refusals = self
                    .stats
                    .compact_arena_admission_refusals
                    .saturating_add(1);
                return Err(error);
            }
        };
        self.compact_arena = Some(Arc::clone(&arena));
        Ok(arena)
    }
}

fn intern_query_label_atom<S>(atoms: &mut HashSet<Arc<str>, S>, value: String) -> (Arc<str>, bool)
where
    S: std::hash::BuildHasher,
{
    if let Some(existing) = atoms.get(value.as_str()) {
        return (Arc::clone(existing), false);
    }
    let atom: Arc<str> = Arc::from(value.into_boxed_str());
    atoms.insert(Arc::clone(&atom));
    (atom, true)
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryExecution {
    pub results: Vec<SegmentQueryResult>,
    pub stats: QueryStats,
}

pub(super) fn ensure_query_result_labels_complete(
    results: &[SegmentQueryResult],
) -> Result<(), PromqlQueryError> {
    if results.iter().all(SegmentQueryResult::labels_are_complete) {
        Ok(())
    } else {
        Err(PromqlQueryError::Storage(
            "internal query invariant violated: incomplete labels escaped their terminal aggregation"
                .to_string(),
        ))
    }
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

impl QueryDataPrefetchStats {
    pub(crate) fn merge_from(&mut self, other: Self) {
        self.query_stats.merge_from(other.query_stats);
        self.series_entries_read = self
            .series_entries_read
            .saturating_add(other.series_entries_read);
        self.chunk_index_reads = self
            .chunk_index_reads
            .saturating_add(other.chunk_index_reads);
        self.chunk_index_bytes_read = self
            .chunk_index_bytes_read
            .saturating_add(other.chunk_index_bytes_read);
    }
}

impl QueryStats {
    pub(crate) fn merge_from(&mut self, other: Self) {
        self.segments_considered = self
            .segments_considered
            .saturating_add(other.segments_considered);
        self.segments_skipped_by_time = self
            .segments_skipped_by_time
            .saturating_add(other.segments_skipped_by_time);
        self.segments_skipped_by_missing_equality = self
            .segments_skipped_by_missing_equality
            .saturating_add(other.segments_skipped_by_missing_equality);
        self.segments_skipped_by_matcher_time_range = self
            .segments_skipped_by_matcher_time_range
            .saturating_add(other.segments_skipped_by_matcher_time_range);
        self.segments_queried = self.segments_queried.saturating_add(other.segments_queried);
        self.matched_series = self.matched_series.saturating_add(other.matched_series);
        self.projected_series = self.projected_series.saturating_add(other.projected_series);
        self.chunk_reads = self.chunk_reads.saturating_add(other.chunk_reads);
        self.bytes_read = self.bytes_read.saturating_add(other.bytes_read);
        self.samples_decoded = self.samples_decoded.saturating_add(other.samples_decoded);
        self.typed_scalar_chunks_decoded = self
            .typed_scalar_chunks_decoded
            .saturating_add(other.typed_scalar_chunks_decoded);
        self.typed_full_chunks_decoded = self
            .typed_full_chunks_decoded
            .saturating_add(other.typed_full_chunks_decoded);
        self.regex_values_examined = self
            .regex_values_examined
            .saturating_add(other.regex_values_examined);
        self.index_postings_reads = self
            .index_postings_reads
            .saturating_add(other.index_postings_reads);
        self.index_postings_bytes_read = self
            .index_postings_bytes_read
            .saturating_add(other.index_postings_bytes_read);
    }

    pub(crate) fn check_limits(self, limits: QueryLimits) -> Result<(), PromqlQueryError> {
        check_query_stat_limit(
            QueryLimit::MatchedSeries,
            self.matched_series,
            limits.max_matched_series,
        )?;
        check_query_stat_limit(
            QueryLimit::ProjectedSeries,
            self.projected_series,
            limits.max_projected_series,
        )?;
        check_query_stat_limit(
            QueryLimit::ChunkReads,
            self.chunk_reads,
            limits.max_chunk_reads,
        )?;
        check_query_stat_limit(
            QueryLimit::BytesRead,
            self.bytes_read,
            limits.max_bytes_read,
        )?;
        check_query_stat_limit(
            QueryLimit::SamplesDecoded,
            self.samples_decoded,
            limits.max_samples_decoded,
        )?;
        check_query_stat_limit(
            QueryLimit::RegexValuesExamined,
            self.regex_values_examined,
            limits.max_regex_values_examined,
        )
    }
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

fn check_query_stat_limit(
    limit: QueryLimit,
    value: u64,
    max: Option<u64>,
) -> Result<(), PromqlQueryError> {
    if let Some(max) = max
        && value > max
    {
        return Err(PromqlQueryError::LimitExceeded {
            limit: limit.as_str().to_string(),
            max,
        });
    }
    Ok(())
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
        if let Some(count) = observe_unique_series(
            &mut self.seen_series,
            series_id,
            self.stats.matched_series,
            QueryLimit::MatchedSeries,
            self.limits.max_matched_series,
        )? {
            self.stats.matched_series = count;
        }
        Ok(())
    }

    pub(crate) fn observe_projected_series(&mut self, series_id: u64) -> io::Result<()> {
        if let Some(count) = observe_unique_series(
            &mut self.seen_projected_series,
            series_id,
            self.stats.projected_series,
            QueryLimit::ProjectedSeries,
            self.limits.max_projected_series,
        )? {
            self.stats.projected_series = count;
        }
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

fn observe_unique_series(
    seen: &mut BTreeSet<u64>,
    series_id: u64,
    current: u64,
    limit: QueryLimit,
    max: Option<u64>,
) -> io::Result<Option<u64>> {
    if !seen.insert(series_id) {
        return Ok(None);
    }
    let next = current.saturating_add(1);
    if let Some(max) = max
        && next > max
    {
        return Err(limit_exceeded_io(QueryLimitExceeded { limit, max }));
    }
    Ok(Some(next))
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

    if err.kind() == io::ErrorKind::InvalidData {
        let message = err.to_string();
        if message.contains("conflicting real and virtual PromQL series") {
            return PromqlQueryError::Invalid(message);
        }
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
    pub(super) label_demand: QueryLabelDemand,
}

/// Internal ownership demand for labels consumed by a terminal aggregation.
/// Raw selector APIs always use `Full`; an `Include` value must not escape the
/// aggregation execution that created it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) enum QueryLabelDemand {
    #[default]
    Full,
    Include {
        /// Superset required while verifying matchers and projection kind.
        names: Arc<[String]>,
        /// Exact names observable from the terminal aggregation input.
        output_names: Arc<[String]>,
        derive_metric_name_dropped_identity: bool,
    },
}

/// Controls whether query planning may reduce owned source labels to the set
/// proven observable by the expression. `Full` is retained for one-binary
/// semantic and performance comparisons; it is not an error-recovery path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryLabelMaterializationPolicy {
    #[default]
    DemandDriven,
    Full,
}

/// Controls observer-heavy, fine-grained query stage timing for one session.
///
/// Production sessions default to `Off`. `Detailed` is intended for isolated
/// diagnostic runs because it reads the monotonic clock inside hot row and
/// symbol-processing paths and can materially perturb broad scans.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryInstrumentationMode {
    #[default]
    Off,
    Detailed,
}

impl QueryLabelDemand {
    pub(super) fn included_names(&self) -> Option<&[String]> {
        match self {
            Self::Full => None,
            Self::Include { names, .. } => Some(names),
        }
    }

    pub(super) fn derives_metric_name_dropped_identity(&self) -> bool {
        matches!(
            self,
            Self::Include {
                derive_metric_name_dropped_identity: true,
                ..
            }
        )
    }

    pub(super) fn output_names_arc(&self) -> Option<Arc<[String]>> {
        match self {
            Self::Full => None,
            Self::Include { output_names, .. } => Some(Arc::clone(output_names)),
        }
    }
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
        le: BucketLeFilter,
        exponential_histogram_boundaries: Vec<f64>,
    },
    NativeHistogram,
    NativeExponentialHistogram,
    SummaryQuantile {
        quantile: Option<String>,
    },
}

impl SegmentProjection {
    pub(crate) fn needs_delta_projection_seed(&self) -> bool {
        matches!(
            self,
            SegmentProjection::Count
                | SegmentProjection::Sum
                | SegmentProjection::HistogramBucket { .. }
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum BucketLeFilter {
    #[default]
    All,
    Exact(String),
    Matchers(Vec<BucketLeMatcher>),
}

impl BucketLeFilter {
    pub(crate) fn from_matchers(matchers: Vec<BucketLeMatcher>) -> Self {
        match matchers.as_slice() {
            [] => Self::All,
            [BucketLeMatcher::Eq(value)] => Self::Exact(value.clone()),
            _ => Self::Matchers(matchers),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BucketLeMatcher {
    Eq(String),
    NotEq(String),
    Regex(String),
    NotRegex(String),
}

impl SegmentSelector {
    pub fn new(matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: None,
            matchers,
            projection: SegmentProjection::None,
            label_demand: QueryLabelDemand::Full,
        }
    }

    pub fn metric(metric_name: impl Into<String>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers: Vec::new(),
            projection: SegmentProjection::None,
            label_demand: QueryLabelDemand::Full,
        }
    }

    pub fn with_metric(metric_name: impl Into<String>, matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers,
            projection: SegmentProjection::None,
            label_demand: QueryLabelDemand::Full,
        }
    }

    pub(super) fn with_projection(mut self, projection: SegmentProjection) -> Self {
        self.projection = projection;
        self
    }

    pub(super) fn with_terminal_aggregation_label_demand(
        mut self,
        grouping_names: &[String],
        derive_metric_name_dropped_identity: bool,
    ) -> Self {
        let mut output_names = grouping_names.to_vec();
        if derive_metric_name_dropped_identity {
            output_names.retain(|name| name != METRIC_NAME_LABEL);
        }
        output_names.sort_unstable();
        output_names.dedup();
        let normalized_matchers = self.normalized_matchers();
        let mut names = Vec::with_capacity(
            grouping_names
                .len()
                .saturating_add(normalized_matchers.len())
                .saturating_add(1),
        );
        names.extend(grouping_names.iter().cloned());
        names.extend(
            normalized_matchers
                .into_iter()
                .map(|matcher| match matcher {
                    NormalizedMatcher::Eq { name, .. }
                    | NormalizedMatcher::NotEq { name, .. }
                    | NormalizedMatcher::Regex { name, .. }
                    | NormalizedMatcher::NotRegex { name, .. } => name,
                }),
        );
        // Matchers and typed/scalar branch selection inspect the physical
        // metric name before a range function is allowed to remove it.
        names.push(METRIC_NAME_LABEL.to_string());
        names.sort_unstable();
        names.dedup();
        self.label_demand = QueryLabelDemand::Include {
            names: Arc::from(names.into_boxed_slice()),
            output_names: Arc::from(output_names.into_boxed_slice()),
            derive_metric_name_dropped_identity,
        };
        self
    }

    pub(super) fn label_demand(&self) -> &QueryLabelDemand {
        &self.label_demand
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
    pub(super) postings: ExactPostingsMetadata,
    pub(super) selection: crate::storage::index::ExactPostingsSelection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentPruneReason {
    MissingEquality,
    MatcherTimeRange,
}

pub struct SegmentStoreReader {
    pub(super) segments: Vec<SegmentReader>,
    pub(super) query_projection_config: QueryProjectionConfig,
    pub(super) metadata_runtime: StoreMetadataRuntime,
}

/// Selects one exact sealed-segment schema for the complete store open.
///
/// This is an explicit whole-store policy. It never probes individual
/// segments to choose a reader, and a corpus containing any other schema is
/// rejected during footer preflight.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SegmentStoreSchemaPolicy {
    /// Prior-format schema-7 reader. Every segment must use footer schema 7.
    StrictSchema7,
    /// Production schema-8 reader using integrity-checked adaptive postings.
    #[default]
    StrictSchema8,
    /// Read-only schema-6 benchmark adapter with mandatory footer validation.
    ValidatedSchema6LayoutAb,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreOpenOptions {
    pub validate_segment_footers: bool,
    /// Exact schema required for every segment in this store.
    pub storage_schema_policy: SegmentStoreSchemaPolicy,
    /// Aggregate metadata and file-descriptor limits, fixed before any segment opens.
    pub metadata_governor: MetadataGovernorConfig,
}

impl SegmentStoreOpenOptions {
    pub(super) fn requires_complete_footer_validation(
        self,
        policy: SegmentStoreSchemaPolicy,
    ) -> bool {
        self.validate_segment_footers
            || policy == SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb
    }
}

pub struct SegmentStoreQuerySession<'a> {
    pub(super) query_projection_config: QueryProjectionConfig,
    pub(super) segments: Vec<SegmentQuerySessionReader<'a>>,
    pub(super) label_cache: SeriesLabelCache,
    pub(super) projected_label_cache: ProjectedLabelCache,
    pub(super) range_scalar_cache_budget_bytes: u64,
    pub(super) range_scalar_cache_governor:
        Arc<super::range_scalar_cache::RangeScalarCacheGovernor>,
    pub(super) last_range_scalar_cache_summary: Option<RangeScalarCacheSummary>,
    pub(super) experimental_cross_segment_chunk_reads: bool,
    pub(super) label_materialization_policy: QueryLabelMaterializationPolicy,
    pub(super) query_label_storage_policy_frozen: bool,
    pub(super) query_instrumentation_mode: QueryInstrumentationMode,
    pub(super) query_instrumentation_mode_frozen: bool,
    pub(super) label_interner: QueryLabelInterner,
    pub(super) query_stages: QueryStageProfile,
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
pub struct ChunkReadSchedulerProfile {
    pub executions: u64,
    pub pread_decisions: u64,
    pub io_uring_decisions: u64,
    pub logical_requests: u64,
    pub physical_spans: u64,
    pub backend_submissions: u64,
    pub sqes_submitted: u64,
    pub submission_depth_sum: u64,
    pub submission_depth_max: u64,
    pub submission_depth_1: u64,
    pub submission_depth_2_3: u64,
    pub submission_depth_4_7: u64,
    pub submission_depth_8_plus: u64,
    /// Total physical bytes executed by the scheduler. Results may remain
    /// retained until their bounded scheduler group is decoded.
    pub total_physical_bytes_executed: u64,
    /// Session high-water mark for bytes concurrently submitted to a backend:
    /// one span for pread, or up to the configured queue depth for io_uring. A
    /// delta containing new executions retains the session maximum because
    /// maxima cannot be subtracted exactly.
    pub peak_in_flight_bytes: u64,
}

impl ChunkReadSchedulerProfile {
    pub fn add(&mut self, other: Self) {
        self.executions = self.executions.saturating_add(other.executions);
        self.pread_decisions = self.pread_decisions.saturating_add(other.pread_decisions);
        self.io_uring_decisions = self
            .io_uring_decisions
            .saturating_add(other.io_uring_decisions);
        self.logical_requests = self.logical_requests.saturating_add(other.logical_requests);
        self.physical_spans = self.physical_spans.saturating_add(other.physical_spans);
        self.backend_submissions = self
            .backend_submissions
            .saturating_add(other.backend_submissions);
        self.sqes_submitted = self.sqes_submitted.saturating_add(other.sqes_submitted);
        self.submission_depth_sum = self
            .submission_depth_sum
            .saturating_add(other.submission_depth_sum);
        self.submission_depth_max = self.submission_depth_max.max(other.submission_depth_max);
        self.submission_depth_1 = self
            .submission_depth_1
            .saturating_add(other.submission_depth_1);
        self.submission_depth_2_3 = self
            .submission_depth_2_3
            .saturating_add(other.submission_depth_2_3);
        self.submission_depth_4_7 = self
            .submission_depth_4_7
            .saturating_add(other.submission_depth_4_7);
        self.submission_depth_8_plus = self
            .submission_depth_8_plus
            .saturating_add(other.submission_depth_8_plus);
        self.total_physical_bytes_executed = self
            .total_physical_bytes_executed
            .saturating_add(other.total_physical_bytes_executed);
        self.peak_in_flight_bytes = self.peak_in_flight_bytes.max(other.peak_in_flight_bytes);
    }

    fn delta_since(self, before: Self) -> Self {
        let has_new_executions = self.executions > before.executions;
        Self {
            executions: self.executions.saturating_sub(before.executions),
            pread_decisions: self.pread_decisions.saturating_sub(before.pread_decisions),
            io_uring_decisions: self
                .io_uring_decisions
                .saturating_sub(before.io_uring_decisions),
            logical_requests: self
                .logical_requests
                .saturating_sub(before.logical_requests),
            physical_spans: self.physical_spans.saturating_sub(before.physical_spans),
            backend_submissions: self
                .backend_submissions
                .saturating_sub(before.backend_submissions),
            sqes_submitted: self.sqes_submitted.saturating_sub(before.sqes_submitted),
            submission_depth_sum: self
                .submission_depth_sum
                .saturating_sub(before.submission_depth_sum),
            submission_depth_max: if has_new_executions {
                self.submission_depth_max
            } else {
                0
            },
            submission_depth_1: self
                .submission_depth_1
                .saturating_sub(before.submission_depth_1),
            submission_depth_2_3: self
                .submission_depth_2_3
                .saturating_sub(before.submission_depth_2_3),
            submission_depth_4_7: self
                .submission_depth_4_7
                .saturating_sub(before.submission_depth_4_7),
            submission_depth_8_plus: self
                .submission_depth_8_plus
                .saturating_sub(before.submission_depth_8_plus),
            total_physical_bytes_executed: self
                .total_physical_bytes_executed
                .saturating_sub(before.total_physical_bytes_executed),
            peak_in_flight_bytes: if has_new_executions {
                self.peak_in_flight_bytes
            } else {
                0
            },
        }
    }
}

/// Store-wide snapshot of currently retained symbol-reader resources.
///
/// One shared reader state may be cloned into multiple query sessions. The
/// collector deduplicates those states before filling this snapshot, so every
/// field is a current-value gauge rather than a per-session counter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreSymbolResources {
    pub retained_readers: u64,
    pub retained_open_files: u64,
    pub source_file_bytes: u64,
    pub root_encoded_bytes: u64,
    pub root_retained_charge_bytes: u64,
    pub eager_dictionary_retained_charge_bytes: u64,
    pub page_cache_charge_bytes: u64,
    pub page_cache_max_bytes: u64,
    pub snapshot_errors: u64,
}

impl SegmentStoreSymbolResources {
    fn observe(&mut self, resources: SegmentSymbolResourceSnapshot) {
        self.retained_readers = self.retained_readers.saturating_add(1);
        self.retained_open_files = self
            .retained_open_files
            .saturating_add(resources.retained_open_files);
        self.source_file_bytes = self
            .source_file_bytes
            .saturating_add(resources.source_file_bytes);
        self.root_encoded_bytes = self
            .root_encoded_bytes
            .saturating_add(resources.root_encoded_bytes);
        self.root_retained_charge_bytes = self
            .root_retained_charge_bytes
            .saturating_add(resources.root_retained_charge_bytes);
        self.eager_dictionary_retained_charge_bytes = self
            .eager_dictionary_retained_charge_bytes
            .saturating_add(resources.eager_dictionary_retained_charge_bytes);
        self.page_cache_charge_bytes = self
            .page_cache_charge_bytes
            .saturating_add(resources.page_cache_charge_bytes);
        self.page_cache_max_bytes = self
            .page_cache_max_bytes
            .saturating_add(resources.page_cache_max_bytes);
    }

    pub fn total_retained_charge_bytes(self) -> u64 {
        self.root_retained_charge_bytes
            .saturating_add(self.eager_dictionary_retained_charge_bytes)
            .saturating_add(self.page_cache_charge_bytes)
    }

    pub(super) fn snapshot_segment_readers<'a>(
        readers: impl IntoIterator<Item = &'a SegmentReader>,
    ) -> Self {
        let mut snapshot = Self::default();
        let mut seen_states = BTreeSet::new();
        for reader in readers {
            let cached = match reader.query_cache.symbols.lock() {
                Ok(cached) => cached,
                Err(_) => {
                    snapshot.snapshot_errors = snapshot.snapshot_errors.saturating_add(1);
                    continue;
                }
            };
            let Some(symbols) = cached.as_ref() else {
                continue;
            };
            if !seen_states.insert(symbols.state_identity()) {
                continue;
            }
            match symbols.resource_snapshot() {
                Ok(resources) => snapshot.observe(resources),
                Err(_) => {
                    snapshot.snapshot_errors = snapshot.snapshot_errors.saturating_add(1);
                }
            }
        }
        snapshot
    }
}

/// Mutually exclusive elapsed-time attribution for query execution stages.
///
/// These fields are leaf-stage diagnostics. They may be summed with
/// [`Self::total_exclusive`]. The older open/read durations on
/// [`SegmentStoreQueryProfile`] are inclusive I/O diagnostics and must not be
/// added to this profile.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStageProfile {
    pub canonical_row_decode: Duration,
    pub symbol_lookup: Duration,
    pub symbol_resolution: Duration,
    /// Index/postings/FST traversal and set work used to produce candidate
    /// series references. This is separate from authoritative row matching.
    pub candidate_selection: Duration,
    pub canonical_identity: Duration,
    /// Schema-neutral visit, cache/governor, and callback-dispatch overhead
    /// left after the explicitly attributed canonical-row leaf work.
    pub metadata_visit_overhead: Duration,
    pub matcher_evaluation: Duration,
    pub label_construction: Duration,
    pub locator_planning: Duration,
    /// Combined payload read pipeline. The ordinary per-segment path includes
    /// coalescing-plan construction and governed file acquisition; the cross-
    /// segment path starts at scheduler execution after planning.
    pub payload_io: Duration,
    /// Combined payload decode, projection, and result-processing work.
    pub payload_decode: Duration,
    pub source_merge: Duration,
    pub promql_grouping_evaluation: Duration,
    pub result_construction: Duration,
}

impl QueryStageProfile {
    pub fn total_exclusive(self) -> Duration {
        Duration::ZERO
            .saturating_add(self.canonical_row_decode)
            .saturating_add(self.symbol_lookup)
            .saturating_add(self.symbol_resolution)
            .saturating_add(self.candidate_selection)
            .saturating_add(self.canonical_identity)
            .saturating_add(self.metadata_visit_overhead)
            .saturating_add(self.matcher_evaluation)
            .saturating_add(self.label_construction)
            .saturating_add(self.locator_planning)
            .saturating_add(self.payload_io)
            .saturating_add(self.payload_decode)
            .saturating_add(self.source_merge)
            .saturating_add(self.promql_grouping_evaluation)
            .saturating_add(self.result_construction)
    }

    pub(super) fn add(&mut self, other: Self) {
        self.canonical_row_decode = self
            .canonical_row_decode
            .saturating_add(other.canonical_row_decode);
        self.symbol_lookup = self.symbol_lookup.saturating_add(other.symbol_lookup);
        self.symbol_resolution = self
            .symbol_resolution
            .saturating_add(other.symbol_resolution);
        self.candidate_selection = self
            .candidate_selection
            .saturating_add(other.candidate_selection);
        self.canonical_identity = self
            .canonical_identity
            .saturating_add(other.canonical_identity);
        self.metadata_visit_overhead = self
            .metadata_visit_overhead
            .saturating_add(other.metadata_visit_overhead);
        self.matcher_evaluation = self
            .matcher_evaluation
            .saturating_add(other.matcher_evaluation);
        self.label_construction = self
            .label_construction
            .saturating_add(other.label_construction);
        self.locator_planning = self.locator_planning.saturating_add(other.locator_planning);
        self.payload_io = self.payload_io.saturating_add(other.payload_io);
        self.payload_decode = self.payload_decode.saturating_add(other.payload_decode);
        self.source_merge = self.source_merge.saturating_add(other.source_merge);
        self.promql_grouping_evaluation = self
            .promql_grouping_evaluation
            .saturating_add(other.promql_grouping_evaluation);
        self.result_construction = self
            .result_construction
            .saturating_add(other.result_construction);
    }

    pub fn delta_since(self, before: Self) -> Self {
        Self {
            canonical_row_decode: self
                .canonical_row_decode
                .saturating_sub(before.canonical_row_decode),
            symbol_lookup: self.symbol_lookup.saturating_sub(before.symbol_lookup),
            symbol_resolution: self
                .symbol_resolution
                .saturating_sub(before.symbol_resolution),
            candidate_selection: self
                .candidate_selection
                .saturating_sub(before.candidate_selection),
            canonical_identity: self
                .canonical_identity
                .saturating_sub(before.canonical_identity),
            metadata_visit_overhead: self
                .metadata_visit_overhead
                .saturating_sub(before.metadata_visit_overhead),
            matcher_evaluation: self
                .matcher_evaluation
                .saturating_sub(before.matcher_evaluation),
            label_construction: self
                .label_construction
                .saturating_sub(before.label_construction),
            locator_planning: self
                .locator_planning
                .saturating_sub(before.locator_planning),
            payload_io: self.payload_io.saturating_sub(before.payload_io),
            payload_decode: self.payload_decode.saturating_sub(before.payload_decode),
            source_merge: self.source_merge.saturating_sub(before.source_merge),
            promql_grouping_evaluation: self
                .promql_grouping_evaluation
                .saturating_sub(before.promql_grouping_evaluation),
            result_construction: self
                .result_construction
                .saturating_sub(before.result_construction),
        }
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
    pub label_rows_integrity_checked: u64,
    pub label_pairs_integrity_checked: u64,
    pub label_rows_full_materialized: u64,
    pub label_rows_selectively_materialized: u64,
    pub label_pairs_materialized: u64,
    pub label_pairs_omitted: u64,
    pub label_content_bytes_materialized: u64,
    pub chunk_index_range_bytes: u64,
    pub chunk_payload_bytes: u64,
    pub chunk_payload_physical_reads: u64,
    pub chunk_payload_physical_bytes: u64,
    pub index_read_stats: SegmentIndexReadStats,
    pub symbol_read_stats: SegmentSymbolReadStats,
    pub symbol_resources: SegmentStoreSymbolResources,
    pub chunk_payload_locality: ChunkPayloadLocalityProfile,
    pub chunk_read_scheduler: ChunkReadSchedulerProfile,
    pub stages: QueryStageProfile,
}

impl SegmentStoreQueryProfile {
    pub(super) fn observe_label_materialization(
        &mut self,
        integrity_checked_label_count: usize,
        labels_complete: bool,
        labels: &[(String, String)],
    ) {
        let integrity_checked = u64::try_from(integrity_checked_label_count).unwrap_or(u64::MAX);
        let materialized = u64::try_from(labels.len()).unwrap_or(u64::MAX);
        self.label_rows_integrity_checked = self.label_rows_integrity_checked.saturating_add(1);
        self.label_pairs_integrity_checked = self
            .label_pairs_integrity_checked
            .saturating_add(integrity_checked);
        if labels_complete {
            self.label_rows_full_materialized = self.label_rows_full_materialized.saturating_add(1);
        } else {
            self.label_rows_selectively_materialized =
                self.label_rows_selectively_materialized.saturating_add(1);
        }
        self.label_pairs_materialized = self.label_pairs_materialized.saturating_add(materialized);
        self.label_pairs_omitted = self
            .label_pairs_omitted
            .saturating_add(integrity_checked.saturating_sub(materialized));
        let content_bytes = labels.iter().fold(0u64, |total, (name, value)| {
            total
                .saturating_add(u64::try_from(name.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
        });
        self.label_content_bytes_materialized = self
            .label_content_bytes_materialized
            .saturating_add(content_bytes);
    }

    pub(super) fn observe_query_label_materialization(
        &mut self,
        integrity_checked_label_count: usize,
        labels_complete: bool,
        labels: &QueryLabels,
    ) {
        let integrity_checked = u64::try_from(integrity_checked_label_count).unwrap_or(u64::MAX);
        let materialized = u64::try_from(labels.len()).unwrap_or(u64::MAX);
        self.label_rows_integrity_checked = self.label_rows_integrity_checked.saturating_add(1);
        self.label_pairs_integrity_checked = self
            .label_pairs_integrity_checked
            .saturating_add(integrity_checked);
        if labels_complete {
            self.label_rows_full_materialized = self.label_rows_full_materialized.saturating_add(1);
        } else {
            self.label_rows_selectively_materialized =
                self.label_rows_selectively_materialized.saturating_add(1);
        }
        self.label_pairs_materialized = self.label_pairs_materialized.saturating_add(materialized);
        self.label_pairs_omitted = self
            .label_pairs_omitted
            .saturating_add(integrity_checked.saturating_sub(materialized));
        let content_bytes = labels.pairs().fold(0u64, |total, (name, value)| {
            total
                .saturating_add(u64::try_from(name.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX))
        });
        self.label_content_bytes_materialized = self
            .label_content_bytes_materialized
            .saturating_add(content_bytes);
    }

    /// Observes one payload file as its own address space.
    ///
    /// Offsets from `chunks.bin` and `ooo_chunks.bin` are not comparable. A
    /// temporary per-file stream prevents equal or decreasing offsets in two
    /// files from being reported as one contiguous run or a backward jump.
    pub(super) fn observe_chunk_payload_file_reads(&mut self, ranges: &[(u64, u64)]) {
        let mut locality = ChunkPayloadLocalityProfile::default();
        for &(offset, len) in ranges {
            self.chunk_payload_bytes = self.chunk_payload_bytes.saturating_add(len);
            locality.observe(offset, len);
        }
        self.chunk_payload_locality.add(locality);
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
        self.label_rows_integrity_checked = self
            .label_rows_integrity_checked
            .saturating_add(other.label_rows_integrity_checked);
        self.label_pairs_integrity_checked = self
            .label_pairs_integrity_checked
            .saturating_add(other.label_pairs_integrity_checked);
        self.label_rows_full_materialized = self
            .label_rows_full_materialized
            .saturating_add(other.label_rows_full_materialized);
        self.label_rows_selectively_materialized = self
            .label_rows_selectively_materialized
            .saturating_add(other.label_rows_selectively_materialized);
        self.label_pairs_materialized = self
            .label_pairs_materialized
            .saturating_add(other.label_pairs_materialized);
        self.label_pairs_omitted = self
            .label_pairs_omitted
            .saturating_add(other.label_pairs_omitted);
        self.label_content_bytes_materialized = self
            .label_content_bytes_materialized
            .saturating_add(other.label_content_bytes_materialized);
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
        self.index_read_stats = self.index_read_stats.saturating_add(other.index_read_stats);
        self.symbol_read_stats = self
            .symbol_read_stats
            .saturating_add(other.symbol_read_stats);
        self.symbol_resources = other.symbol_resources;
        self.chunk_payload_locality
            .add(other.chunk_payload_locality);
        self.chunk_read_scheduler.add(other.chunk_read_scheduler);
        self.stages.add(other.stages);
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
            label_rows_integrity_checked: self
                .label_rows_integrity_checked
                .saturating_sub(before.label_rows_integrity_checked),
            label_pairs_integrity_checked: self
                .label_pairs_integrity_checked
                .saturating_sub(before.label_pairs_integrity_checked),
            label_rows_full_materialized: self
                .label_rows_full_materialized
                .saturating_sub(before.label_rows_full_materialized),
            label_rows_selectively_materialized: self
                .label_rows_selectively_materialized
                .saturating_sub(before.label_rows_selectively_materialized),
            label_pairs_materialized: self
                .label_pairs_materialized
                .saturating_sub(before.label_pairs_materialized),
            label_pairs_omitted: self
                .label_pairs_omitted
                .saturating_sub(before.label_pairs_omitted),
            label_content_bytes_materialized: self
                .label_content_bytes_materialized
                .saturating_sub(before.label_content_bytes_materialized),
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
            index_read_stats: self
                .index_read_stats
                .saturating_sub(before.index_read_stats),
            symbol_read_stats: self.symbol_read_stats.delta_since(before.symbol_read_stats),
            // These are store-wide current-value gauges, not monotonic
            // counters. Preserve the after snapshot so warm-run deltas still
            // report all resources retained by the shared store.
            symbol_resources: self.symbol_resources,
            chunk_payload_locality: self
                .chunk_payload_locality
                .delta_since(before.chunk_payload_locality),
            chunk_read_scheduler: self
                .chunk_read_scheduler
                .delta_since(before.chunk_read_scheduler),
            stages: self.stages.delta_since(before.stages),
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

#[cfg(test)]
mod query_label_storage_tests {
    use super::*;
    use std::hash::{BuildHasherDefault, Hasher};

    #[derive(Default)]
    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    fn labels(metric: &str, service: &str) -> Vec<(String, String)> {
        vec![
            (METRIC_NAME_LABEL.to_owned(), metric.to_owned()),
            ("service_name".to_owned(), service.to_owned()),
            ("synthetic".to_owned(), "+Inf".to_owned()),
        ]
    }

    #[test]
    fn shared_query_labels_reuse_atoms_without_touching_owned_compatibility() {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
        let first = interner.intern_labels(labels("requests_total", "api"));
        let second = interner.intern_labels(labels("errors_total", "api"));

        let first_ptrs = first.shared_atom_ptrs().expect("shared labels");
        let second_ptrs = second.shared_atom_ptrs().expect("shared labels");
        assert!(std::ptr::eq(first_ptrs[0].0, second_ptrs[0].0));
        assert!(std::ptr::eq(first_ptrs[1].0, second_ptrs[1].0));
        assert!(std::ptr::eq(first_ptrs[1].1, second_ptrs[1].1));
        assert!(std::ptr::eq(first_ptrs[2].0, second_ptrs[2].0));
        assert!(std::ptr::eq(first_ptrs[2].1, second_ptrs[2].1));

        assert_eq!(first.pairs().count(), 3);
        assert_eq!(first.to_vec(), labels("requests_total", "api"));
        assert!(!first.owned_compatibility_materialized());
        assert!(!second.owned_compatibility_materialized());
        assert_eq!(
            interner.stats(),
            QueryLabelStorageStats {
                label_sets: 2,
                atom_lookups: 12,
                atom_hits: 5,
                atom_misses: 7,
                unique_content_bytes: 62,
                ..QueryLabelStorageStats::default()
            }
        );

        assert_eq!(first.to_vec(), labels("requests_total", "api"));
        assert!(!first.owned_compatibility_materialized());
        assert!(!second.owned_compatibility_materialized());
    }

    #[test]
    fn owned_query_labels_are_the_default_and_keep_the_legacy_representation() {
        let mut interner = QueryLabelInterner::default();
        assert_eq!(interner.policy(), QueryLabelStoragePolicy::OwnedStrings);
        let owned = interner.intern_labels(labels("requests_total", "api"));

        assert!(owned.shared_atom_ptrs().is_none());
        assert_eq!(owned.to_vec(), labels("requests_total", "api"));
        assert_eq!(
            interner.stats(),
            QueryLabelStorageStats {
                label_sets: 1,
                ..QueryLabelStorageStats::default()
            }
        );
    }

    #[test]
    fn shared_labels_remain_valid_after_the_session_interner_is_dropped() {
        let labels = {
            let mut interner = QueryLabelInterner::default();
            interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
            interner.intern_labels(labels("requests_total", "api"))
        };

        assert_eq!(
            labels.pairs().collect::<Vec<_>>(),
            vec![
                (METRIC_NAME_LABEL, "requests_total"),
                ("service_name", "api"),
                ("synthetic", "+Inf"),
            ]
        );
        assert!(!labels.owned_compatibility_materialized());
    }

    #[test]
    fn atom_interning_resolves_content_under_forced_hash_collisions() {
        let mut atoms = HashSet::<Arc<str>, BuildHasherDefault<ConstantHasher>>::default();
        let (alpha, alpha_inserted) = intern_query_label_atom(&mut atoms, "alpha".to_owned());
        let (beta, beta_inserted) = intern_query_label_atom(&mut atoms, "beta".to_owned());
        let (alpha_again, alpha_again_inserted) =
            intern_query_label_atom(&mut atoms, "alpha".to_owned());

        assert!(alpha_inserted);
        assert!(beta_inserted);
        assert!(!alpha_again_inserted);
        assert_eq!(atoms.len(), 2);
        assert_ne!(alpha.as_ref(), beta.as_ref());
        assert!(Arc::ptr_eq(&alpha, &alpha_again));
    }

    #[test]
    fn owned_and_shared_labels_have_identical_order_and_content_semantics() {
        let expected = labels("requests_total", "api");
        let owned = QueryLabels::from_vec(expected.clone());
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
        let shared = interner.intern_labels(expected.clone());
        let different = interner.intern_labels(labels("requests_total", "worker"));

        assert_eq!(owned, shared);
        assert_eq!(owned.cmp(&shared), std::cmp::Ordering::Equal);
        assert_eq!(shared, expected);
        assert_ne!(shared, different);
        assert!(!shared.owned_compatibility_materialized());
    }

    #[test]
    fn compact_label_pairs_are_exactly_eight_bytes() {
        assert_eq!(std::mem::size_of::<CompactQueryLabelPair>(), 8);
        assert_eq!(std::mem::align_of::<CompactQueryLabelPair>(), 4);
    }

    #[test]
    fn compact_arena_base_and_label_blocks_charge_modeled_live_allocations() {
        let arena =
            Arc::new(CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap());
        let base = arena.snapshot();
        let expected_base = modeled_arc_allocation_bytes::<CompactQueryLabelArena>()
            + u64::try_from(
                arena
                    .atom_chunks
                    .len()
                    .saturating_mul(std::mem::size_of::<OnceLock<CompactQueryLabelAtomChunk>>()),
            )
            .unwrap();
        assert_eq!(base.atom_bytes, expected_base);
        assert_eq!(
            base.hash_directory_bytes,
            COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES
        );
        assert_eq!(
            base.current_bytes,
            expected_base + COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES
        );
        assert_eq!(base.peak_bytes, base.current_bytes);

        let labels = arena
            .intern_pairs(vec![
                (METRIC_NAME_LABEL.to_owned(), "requests_total".to_owned()),
                ("service".to_owned(), "api".to_owned()),
            ])
            .unwrap();
        let after_intern = arena.snapshot();
        let expected_label_block = COMPACT_QUERY_LABEL_OBJECT_BYTES
            + 2 * std::mem::size_of::<CompactQueryLabelPair>() as u64;
        assert_eq!(after_intern.pair_bytes, expected_label_block);

        let clone = labels.clone();
        drop(labels);
        assert_eq!(arena.snapshot().pair_bytes, expected_label_block);
        drop(clone);
        assert_eq!(arena.snapshot().pair_bytes, 0);
        assert_eq!(
            arena.snapshot().current_bytes,
            after_intern.current_bytes - expected_label_block
        );
    }

    #[test]
    fn compact_atom_charge_includes_arc_str_tail_alignment() {
        let arena =
            Arc::new(CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap());
        let before = arena.snapshot();
        let labels = arena
            .intern_pairs(vec![("a".to_owned(), "bc".to_owned())])
            .unwrap();
        let after = arena.snapshot();
        let atom_chunk_bytes =
            (COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN * std::mem::size_of::<OnceLock<Arc<str>>>()) as u64;
        let expected_atoms = atom_chunk_bytes
            + modeled_arc_str_allocation_bytes(1).unwrap()
            + modeled_arc_str_allocation_bytes(2).unwrap();
        assert_eq!(after.atom_bytes - before.atom_bytes, expected_atoms);
        for content_bytes in 0..=(2 * std::mem::align_of::<usize>() as u64) {
            let allocation = modeled_arc_str_allocation_bytes(content_bytes).unwrap();
            assert_eq!(allocation % std::mem::align_of::<usize>() as u64, 0);
            assert!(allocation >= 2 * std::mem::size_of::<usize>() as u64 + content_bytes);
        }
        drop(labels);
    }

    #[test]
    fn compact_empty_label_set_still_charges_its_shared_object_once() {
        let arena =
            Arc::new(CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap());
        let labels = arena.intern_pairs(Vec::new()).unwrap();
        assert_eq!(
            arena.snapshot().pair_bytes,
            COMPACT_QUERY_LABEL_OBJECT_BYTES
        );
        drop(labels);
        assert_eq!(arena.snapshot().pair_bytes, 0);
    }

    #[test]
    fn compact_labels_reuse_ids_without_retaining_owned_compatibility() {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        let first = interner
            .try_intern_labels(labels("requests_total", "api"))
            .unwrap();
        let second = interner
            .try_intern_labels(labels("errors_total", "api"))
            .unwrap();

        assert!(first.uses_compact_ids());
        assert!(second.uses_compact_ids());
        assert_eq!(first.pairs().count(), 3);
        assert_eq!(first.to_vec(), labels("requests_total", "api"));
        assert!(!first.owned_compatibility_materialized());
        assert!(!second.owned_compatibility_materialized());
        let stats = interner.stats();
        assert_eq!(stats.compact_label_sets, 2);
        assert_eq!(stats.compact_pairs, 6);
        assert_eq!(stats.compact_atom_lookups, 12);
        assert_eq!(stats.compact_atom_hits + stats.compact_atom_misses, 12);
        assert_eq!(stats.compact_unique_strings, stats.compact_atom_misses);
        assert_eq!(
            stats.compact_arena_current_bytes,
            stats
                .compact_atom_bytes
                .saturating_add(stats.compact_pair_bytes)
                .saturating_add(stats.compact_hash_directory_bytes)
                .saturating_add(stats.compact_translation_bytes)
        );

        assert_eq!(first.to_vec(), labels("requests_total", "api"));
        assert!(!first.owned_compatibility_materialized());
        assert_eq!(interner.stats().compact_compatibility_materializations, 0);
        assert!(!second.owned_compatibility_materialized());
    }

    #[test]
    fn compact_labels_keep_the_arena_alive_after_the_interner_is_dropped() {
        let labels = {
            let mut interner = QueryLabelInterner::default();
            interner.set_policy(QueryLabelStoragePolicy::CompactIds);
            interner
                .try_intern_labels(labels("requests_total", "api"))
                .unwrap()
        };

        assert_eq!(
            labels.pairs().collect::<Vec<_>>(),
            vec![
                (METRIC_NAME_LABEL, "requests_total"),
                ("service_name", "api"),
                ("synthetic", "+Inf"),
            ]
        );
        assert!(!labels.owned_compatibility_materialized());
    }

    #[test]
    fn compact_labels_compare_by_content_across_independent_arenas() {
        let expected = labels("requests_total", "api");
        let mut left_interner = QueryLabelInterner::default();
        left_interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        let left = left_interner.try_intern_labels(expected.clone()).unwrap();
        let mut right_interner = QueryLabelInterner::default();
        right_interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        let right = right_interner.try_intern_labels(expected.clone()).unwrap();
        let different = right_interner
            .try_intern_labels(labels("requests_total", "worker"))
            .unwrap();

        assert_eq!(left, right);
        assert_eq!(left.cmp(&right), std::cmp::Ordering::Equal);
        assert_eq!(left, expected);
        assert_ne!(left, different);
        assert_eq!(query_labels_series_id(&left), segment_series_id(&expected));
        assert!(!left.owned_compatibility_materialized());
        assert!(!right.owned_compatibility_materialized());
    }

    #[test]
    fn compact_labels_distinguish_missing_from_explicit_empty_values() {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        let missing = interner.try_intern_labels(Vec::new()).unwrap();
        let explicit_empty = interner
            .try_intern_labels(vec![("zone".to_owned(), String::new())])
            .unwrap();

        assert!(missing.is_empty());
        assert_eq!(
            explicit_empty.pairs().collect::<Vec<_>>(),
            vec![("zone", "")]
        );
        assert_ne!(missing, explicit_empty);
    }

    #[test]
    fn compact_labels_project_terminal_names_without_materializing_strings() {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        let labels = interner
            .try_intern_labels(vec![
                (METRIC_NAME_LABEL.to_owned(), "requests_total".to_owned()),
                ("instance".to_owned(), "api-1".to_owned()),
                ("service".to_owned(), "api".to_owned()),
            ])
            .unwrap();
        let projected = labels.try_retain_names(&["service".to_owned()]).unwrap();

        assert_eq!(projected.pairs().collect::<Vec<_>>(), [("service", "api")]);
        assert!(!projected.owned_compatibility_materialized());
        let stats = interner.stats();
        assert_eq!(stats.compact_label_sets, 2);
        assert_eq!(stats.compact_pairs, 4);
        assert_eq!(stats.compact_compatibility_materializations, 0);
        assert_eq!(
            stats.compact_arena_current_bytes,
            stats
                .compact_atom_bytes
                .saturating_add(stats.compact_pair_bytes)
                .saturating_add(stats.compact_hash_directory_bytes)
                .saturating_add(stats.compact_translation_bytes)
        );
    }

    #[test]
    fn compact_typed_scalar_projection_reuses_ids_and_derived_metric_atom() {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        let source = interner
            .try_intern_labels(labels("requests", "api"))
            .unwrap();

        let first = interner
            .try_project_metric_suffix_labels(&source, "_count")
            .unwrap();
        let after_first = interner.stats();
        let second = interner
            .try_project_metric_suffix_labels(&source, "_count")
            .unwrap();
        let after_second = interner.stats();

        let expected = labels("requests_count", "api");
        assert_eq!(first, expected);
        assert_eq!(second, expected);
        assert_eq!(query_labels_series_id(&first), segment_series_id(&expected));
        assert!(source.uses_compact_ids());
        assert!(first.uses_compact_ids());
        assert!(second.uses_compact_ids());
        assert!(!source.owned_compatibility_materialized());
        assert!(!first.owned_compatibility_materialized());
        assert!(!second.owned_compatibility_materialized());
        assert_eq!(
            after_second.compact_atom_lookups, after_first.compact_atom_lookups,
            "the derived metric atom must be reused without another UTF-8 lookup",
        );
        assert_eq!(after_second.compact_label_sets, 3);
        assert_eq!(after_second.compact_pairs, 9);
        assert_eq!(after_second.compact_compatibility_materializations, 0);
        assert_eq!(
            after_second.compact_arena_current_bytes,
            after_second
                .compact_atom_bytes
                .saturating_add(after_second.compact_pair_bytes)
                .saturating_add(after_second.compact_hash_directory_bytes)
                .saturating_add(after_second.compact_translation_bytes)
        );
    }

    #[test]
    fn compact_budget_refusal_occurs_before_atoms_and_never_falls_back() {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        interner.set_compact_arena_max_bytes(1).unwrap();
        assert_eq!(interner.stats().compact_arena_budget_bytes, 1);

        let error = interner
            .try_intern_labels(labels("requests_total", "api"))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(interner.policy(), QueryLabelStoragePolicy::CompactIds);
        let stats = interner.stats();
        assert_eq!(stats.compact_atom_lookups, 0);
        assert_eq!(stats.compact_atom_misses, 0);
        assert_eq!(stats.compact_label_sets, 0);
        assert_eq!(stats.compact_arena_current_bytes, 0);
        assert_eq!(stats.compact_arena_admission_refusals, 1);
    }

    #[test]
    fn compact_hash_buckets_verify_full_content_under_collisions() {
        let arena = CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap();
        let mut state = arena.state.lock().unwrap();
        let alpha = arena
            .intern_locked_with_hash(&mut state, "alpha", 7)
            .unwrap();
        let beta = arena
            .intern_locked_with_hash(&mut state, "beta", 7)
            .unwrap();
        let alpha_again = arena
            .intern_locked_with_hash(&mut state, "alpha", 7)
            .unwrap();

        assert_ne!(alpha, beta);
        assert_eq!(alpha, alpha_again);
        assert_eq!(arena.resolve(alpha), "alpha");
        assert_eq!(arena.resolve(beta), "beta");
        assert_eq!(state.lookups, 3);
        assert_eq!(state.hits, 1);
        assert_eq!(state.misses, 2);
    }
}

#[cfg(test)]
mod index_read_profile_tests {
    use super::*;
    use crate::storage::index::{SegmentIndexReadCount, SegmentIndexReadStats};
    use crate::storage::symbols::SegmentSymbolReadCount;

    fn index_stats(multiplier: u64) -> SegmentIndexReadStats {
        let count = |value| SegmentIndexReadCount {
            calls: value * multiplier,
            bytes: value * multiplier * 10,
        };
        SegmentIndexReadStats {
            root: count(1),
            routing: count(2),
            exact_directory: count(3),
            exact_page: count(4),
            auxiliary_directory: count(5),
            payload: count(6),
        }
    }

    fn stage_profile(multiplier: u32) -> QueryStageProfile {
        let duration = |value: u64| Duration::from_nanos(value * u64::from(multiplier));
        QueryStageProfile {
            canonical_row_decode: duration(1),
            symbol_lookup: duration(2),
            symbol_resolution: duration(3),
            candidate_selection: duration(4),
            canonical_identity: duration(5),
            metadata_visit_overhead: duration(6),
            matcher_evaluation: duration(7),
            label_construction: duration(8),
            locator_planning: duration(9),
            payload_io: duration(10),
            payload_decode: duration(11),
            source_merge: duration(12),
            promql_grouping_evaluation: duration(13),
            result_construction: duration(14),
        }
    }

    fn max_stage_profile() -> QueryStageProfile {
        QueryStageProfile {
            canonical_row_decode: Duration::MAX,
            symbol_lookup: Duration::MAX,
            symbol_resolution: Duration::MAX,
            candidate_selection: Duration::MAX,
            canonical_identity: Duration::MAX,
            metadata_visit_overhead: Duration::MAX,
            matcher_evaluation: Duration::MAX,
            label_construction: Duration::MAX,
            locator_planning: Duration::MAX,
            payload_io: Duration::MAX,
            payload_decode: Duration::MAX,
            source_merge: Duration::MAX,
            promql_grouping_evaluation: Duration::MAX,
            result_construction: Duration::MAX,
        }
    }

    #[test]
    fn query_stage_profile_add_delta_and_total_are_saturating() {
        let mut total = stage_profile(2);
        total.add(stage_profile(3));
        assert_eq!(total, stage_profile(5));
        assert_eq!(total.total_exclusive(), Duration::from_nanos(525));

        let mut before = stage_profile(2);
        before.payload_decode = Duration::MAX;
        let delta = total.delta_since(before);
        let mut expected = stage_profile(3);
        expected.payload_decode = Duration::ZERO;
        assert_eq!(delta, expected);

        let mut saturated = max_stage_profile();
        saturated.add(stage_profile(1));
        assert_eq!(saturated, max_stage_profile());
        assert_eq!(saturated.total_exclusive(), Duration::MAX);
        assert_eq!(
            stage_profile(1).delta_since(max_stage_profile()),
            QueryStageProfile::default()
        );
    }

    #[test]
    fn segment_query_profile_adds_and_deltas_query_stages() {
        let mut profile = SegmentStoreQueryProfile {
            stages: stage_profile(2),
            ..SegmentStoreQueryProfile::default()
        };
        profile.add(SegmentStoreQueryProfile {
            stages: stage_profile(3),
            ..SegmentStoreQueryProfile::default()
        });
        assert_eq!(profile.stages, stage_profile(5));

        let delta = profile.delta_since(SegmentStoreQueryProfile {
            stages: stage_profile(1),
            ..SegmentStoreQueryProfile::default()
        });
        assert_eq!(delta.stages, stage_profile(4));
    }

    #[test]
    fn query_profile_adds_index_read_stats_by_category() {
        let mut total = SegmentStoreQueryProfile {
            index_read_stats: index_stats(2),
            ..SegmentStoreQueryProfile::default()
        };

        total.add(SegmentStoreQueryProfile {
            index_read_stats: index_stats(3),
            ..SegmentStoreQueryProfile::default()
        });

        assert_eq!(total.index_read_stats, index_stats(5));
        assert_eq!(total.index_read_stats.total_calls(), 105);
        assert_eq!(total.index_read_stats.total_bytes(), 1_050);
    }

    #[test]
    fn query_profile_deltas_index_read_stats_by_category_with_saturation() {
        let after = SegmentStoreQueryProfile {
            index_read_stats: index_stats(5),
            ..SegmentStoreQueryProfile::default()
        };
        let mut before_stats = index_stats(2);
        before_stats.payload.calls = u64::MAX;
        before_stats.payload.bytes = u64::MAX;
        let before = SegmentStoreQueryProfile {
            index_read_stats: before_stats,
            ..SegmentStoreQueryProfile::default()
        };

        let delta = after.delta_since(before).index_read_stats;

        let mut expected = index_stats(3);
        expected.payload = SegmentIndexReadCount::default();
        assert_eq!(delta, expected);
    }

    #[test]
    fn query_profile_deltas_symbol_counters_but_preserves_resource_gauges() {
        let before = SegmentStoreQueryProfile {
            symbol_read_stats: SegmentSymbolReadStats {
                legacy_eager: SegmentSymbolReadCount::default(),
                logical_returned: SegmentSymbolReadCount::default(),
                root: SegmentSymbolReadCount {
                    calls: 2,
                    bytes: 200,
                },
                page: SegmentSymbolReadCount {
                    calls: 3,
                    bytes: 300,
                },
                page_validation: SegmentSymbolReadCount {
                    calls: 3,
                    bytes: 300,
                },
                page_validation_ns: 30,
                touched_corrupt_pages: 1,
                page_cache_hits: 4,
                page_cache_misses: 5,
                page_cache_evictions: 6,
            },
            symbol_resources: SegmentStoreSymbolResources {
                retained_readers: 1,
                retained_open_files: 1,
                source_file_bytes: 100_000,
                root_encoded_bytes: 1_000,
                root_retained_charge_bytes: 2_000,
                page_cache_charge_bytes: 32_768,
                page_cache_max_bytes: 262_144,
                ..SegmentStoreSymbolResources::default()
            },
            ..SegmentStoreQueryProfile::default()
        };
        let after = SegmentStoreQueryProfile {
            symbol_read_stats: SegmentSymbolReadStats {
                legacy_eager: SegmentSymbolReadCount::default(),
                logical_returned: SegmentSymbolReadCount::default(),
                root: SegmentSymbolReadCount {
                    calls: 3,
                    bytes: 280,
                },
                page: SegmentSymbolReadCount {
                    calls: 5,
                    bytes: 500,
                },
                page_validation: SegmentSymbolReadCount {
                    calls: 5,
                    bytes: 500,
                },
                page_validation_ns: 80,
                touched_corrupt_pages: 3,
                page_cache_hits: 10,
                page_cache_misses: 8,
                page_cache_evictions: 7,
            },
            symbol_resources: SegmentStoreSymbolResources {
                retained_readers: 2,
                retained_open_files: 2,
                source_file_bytes: 200_000,
                root_encoded_bytes: 2_000,
                root_retained_charge_bytes: 4_000,
                page_cache_charge_bytes: 65_536,
                page_cache_max_bytes: 524_288,
                ..SegmentStoreSymbolResources::default()
            },
            ..SegmentStoreQueryProfile::default()
        };

        let delta = after.delta_since(before);

        assert_eq!(
            delta.symbol_read_stats,
            SegmentSymbolReadStats {
                legacy_eager: SegmentSymbolReadCount::default(),
                logical_returned: SegmentSymbolReadCount::default(),
                root: SegmentSymbolReadCount {
                    calls: 1,
                    bytes: 80,
                },
                page: SegmentSymbolReadCount {
                    calls: 2,
                    bytes: 200,
                },
                page_validation: SegmentSymbolReadCount {
                    calls: 2,
                    bytes: 200,
                },
                page_validation_ns: 50,
                touched_corrupt_pages: 2,
                page_cache_hits: 6,
                page_cache_misses: 3,
                page_cache_evictions: 1,
            }
        );
        assert_eq!(delta.symbol_resources, after.symbol_resources);
        assert_eq!(delta.symbol_resources.total_retained_charge_bytes(), 69_536);
    }

    #[test]
    fn query_profile_adds_and_deltas_scheduler_profile() {
        let before_scheduler = ChunkReadSchedulerProfile {
            executions: 2,
            pread_decisions: 1,
            io_uring_decisions: 1,
            logical_requests: 20,
            physical_spans: 10,
            backend_submissions: 3,
            sqes_submitted: 9,
            submission_depth_sum: 10,
            submission_depth_max: 8,
            submission_depth_1: 1,
            submission_depth_2_3: 0,
            submission_depth_4_7: 0,
            submission_depth_8_plus: 1,
            total_physical_bytes_executed: 1_000,
            peak_in_flight_bytes: 800,
        };
        let next_scheduler = ChunkReadSchedulerProfile {
            executions: 1,
            io_uring_decisions: 1,
            logical_requests: 12,
            physical_spans: 9,
            backend_submissions: 2,
            sqes_submitted: 9,
            submission_depth_sum: 9,
            submission_depth_max: 8,
            submission_depth_1: 1,
            submission_depth_8_plus: 1,
            total_physical_bytes_executed: 900,
            peak_in_flight_bytes: 700,
            ..ChunkReadSchedulerProfile::default()
        };
        let before = SegmentStoreQueryProfile {
            chunk_read_scheduler: before_scheduler,
            ..SegmentStoreQueryProfile::default()
        };
        let mut after = before;
        after.add(SegmentStoreQueryProfile {
            chunk_read_scheduler: next_scheduler,
            ..SegmentStoreQueryProfile::default()
        });

        let delta = after.delta_since(before).chunk_read_scheduler;
        assert_eq!(
            delta,
            ChunkReadSchedulerProfile {
                peak_in_flight_bytes: 800,
                ..next_scheduler
            }
        );
        assert_eq!(after.chunk_read_scheduler.submission_depth_max, 8);
        assert_eq!(after.chunk_read_scheduler.peak_in_flight_bytes, 800);
    }
}
