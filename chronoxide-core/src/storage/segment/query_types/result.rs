use super::super::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OtlpAggregationTemporality, prometheus_stale_nan,
};
use super::labels::{QueryLabels, shared_query_labels};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentQueryResult {
    pub series_id: u64,
    pub labels: QueryLabels,
    pub samples: Vec<(u64, f64)>,
    pub counter_reset_hints: Vec<CounterResetHint>,
    pub(crate) sample_start_times: Vec<Option<u64>>,
    /// Per-sample temporality retained until equal-timestamp precedence has
    /// selected the winning sample. Empty means the complete series has no
    /// typed temporality metadata; otherwise it is aligned with `samples`.
    pub(crate) sample_temporalities: Vec<QueryResultTemporality>,
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
            sample_temporalities: Vec::new(),
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
            sample_temporalities: Vec::new(),
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
            sample_temporalities: Vec::new(),
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
        let has_sample_temporalities = self.has_sample_temporalities();
        if has_sample_temporalities {
            self.sample_temporalities
                .push(QueryResultTemporality::Unknown);
        } else {
            self.sample_temporalities.clear();
        }
        if self.has_delta_projection_intervals() {
            self.delta_projection_intervals.push(None);
        } else {
            self.delta_projection_intervals.clear();
        }
        self.samples.push((timestamp_ms, value));
        if has_sample_temporalities {
            self.observe_temporality(QueryResultTemporality::Unknown);
        }
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
        self.ensure_sample_temporalities();
        if self.has_delta_projection_intervals() {
            self.delta_projection_intervals.push(None);
        } else {
            self.delta_projection_intervals.clear();
        }
        self.samples.push((timestamp_ms, value));
        self.counter_reset_hints.push(reset_hint);
        let temporality = QueryResultTemporality::from(temporality);
        self.sample_temporalities.push(temporality);
        self.observe_temporality(temporality);
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
        if other.has_sample_temporalities() {
            self.ensure_sample_temporalities();
            self.sample_temporalities
                .append(&mut other.sample_temporalities);
        } else if self.has_sample_temporalities() {
            self.sample_temporalities
                .extend(std::iter::repeat_n(other.temporality, other.samples.len()));
        } else {
            self.sample_temporalities.clear();
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
        let has_temporalities = self.has_sample_temporalities();
        let has_delta_intervals = self.has_delta_projection_intervals();
        if !has_hints {
            self.counter_reset_hints.clear();
        }
        if !has_start_times {
            self.sample_start_times.clear();
        }
        if !has_temporalities {
            self.sample_temporalities.clear();
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
                        has_temporalities,
                        has_delta_intervals,
                    );
                }
                SampleTimestampOrder::Unsorted => {
                    self.sort_and_dedupe_samples_keep_last(
                        has_hints,
                        has_start_times,
                        has_temporalities,
                        has_delta_intervals,
                    );
                }
            }
        }

        if has_temporalities {
            self.recompute_temporality_from_samples();
        }
        self.materialize_delta_projection_intervals();
    }

    fn compact_sorted_samples_keep_last(
        &mut self,
        has_hints: bool,
        has_start_times: bool,
        has_temporalities: bool,
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
                if has_temporalities {
                    self.sample_temporalities[write_idx] = self.sample_temporalities[last_idx];
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
        if has_temporalities {
            self.sample_temporalities.truncate(write_idx);
        } else {
            self.sample_temporalities.clear();
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
        has_temporalities: bool,
        has_delta_intervals: bool,
    ) {
        if !has_hints && !has_start_times && !has_temporalities && !has_delta_intervals {
            self.samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
            self.compact_sorted_samples_keep_last(false, false, false, false);
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
        let temporalities = if has_temporalities {
            Some(std::mem::take(&mut self.sample_temporalities))
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
                    temporalities.as_ref().map(|values| values[idx]),
                    delta_intervals.as_ref().map(|values| values[idx]),
                )
            })
            .collect();
        rows.sort_by_key(|(sample, _, _, _, _)| sample.0);

        for (sample, reset_hint, start_time, temporality, delta_interval) in rows {
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
                if has_temporalities {
                    *self
                        .sample_temporalities
                        .last_mut()
                        .expect("last sample temporality exists") =
                        temporality.expect("sample temporality exists");
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
                if has_temporalities {
                    self.sample_temporalities
                        .push(temporality.expect("sample temporality exists"));
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

    fn ensure_sample_temporalities(&mut self) {
        if !self.has_sample_temporalities() {
            self.sample_temporalities = vec![self.temporality; self.samples.len()];
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

    fn has_sample_temporalities(&self) -> bool {
        !self.sample_temporalities.is_empty()
            && self.sample_temporalities.len() == self.samples.len()
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

    pub(in crate::storage::segment) fn recompute_temporality_from_samples(&mut self) {
        let mut temporality = QueryResultTemporality::Unknown;
        let mut sample_count = 0usize;
        for sample_temporality in self.sample_temporalities.iter().copied() {
            temporality =
                merge_result_temporality(temporality, sample_count, sample_temporality, 1);
            sample_count = sample_count.saturating_add(1);
        }
        self.temporality = temporality;
    }
}

#[repr(u8)]
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
