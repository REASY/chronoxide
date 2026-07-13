use crate::app_config::LabelSetStoreKind;
use crate::statistics::LatencySamples;
use chrono::{DateTime, TimeZone, Utc};
use chronoxide_core::labels::U64IdentityHasher;
use chronoxide_core::statistics::{
    DEFAULT_TDIGEST_BUFFER_CAPACITY, DEFAULT_TDIGEST_MAX_CENTROIDS, DistI64, Stats,
};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::time::{Duration, Instant};

type U64BuildHasher = BuildHasherDefault<U64IdentityHasher>;
type U64HashSet = HashSet<u64, U64BuildHasher>;

fn hash_u64(bytes: &[u8]) -> u64 {
    let mut hasher = U64IdentityHasher::default();
    hasher.write(bytes);
    hasher.finish()
}

fn saturating_add_both(total: &mut u64, window: &mut u64, value: u64) {
    *total = total.saturating_add(value);
    *window = window.saturating_add(value);
}

#[derive(Clone, Debug)]
pub struct PartitionWatermark {
    pub min_ts: DateTime<Utc>,
    pub max_ts: DateTime<Utc>,
    pub messages: u64,
    pub datapoints: u64,
}

impl PartitionWatermark {
    fn new(ts: DateTime<Utc>, datapoints: u64) -> Self {
        Self {
            min_ts: ts,
            max_ts: ts,
            messages: 1,
            datapoints,
        }
    }

    fn update(&mut self, ts: DateTime<Utc>, datapoints: u64) {
        if ts < self.min_ts {
            self.min_ts = ts;
        }
        if ts > self.max_ts {
            self.max_ts = ts;
        }
        self.messages = self.messages.saturating_add(1);
        self.datapoints = self.datapoints.saturating_add(datapoints);
    }

    pub fn window_ms(&self) -> i64 {
        (self.max_ts - self.min_ts).num_milliseconds()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtlpDataTypeCounts {
    pub gauge: u64,
    pub sum: u64,
    pub histogram: u64,
    pub exponential_histogram: u64,
    pub summary: u64,
}

impl OtlpDataTypeCounts {
    fn incr(&mut self, kind: MetricDataType, value: u64) {
        match kind {
            MetricDataType::Gauge => self.gauge = self.gauge.saturating_add(value),
            MetricDataType::Sum => self.sum = self.sum.saturating_add(value),
            MetricDataType::Histogram => self.histogram = self.histogram.saturating_add(value),
            MetricDataType::ExponentialHistogram => {
                self.exponential_histogram = self.exponential_histogram.saturating_add(value);
            }
            MetricDataType::Summary => self.summary = self.summary.saturating_add(value),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DatapointPolicyCounts {
    pub accepted: u64,
    pub dropped_too_old: u64,
    pub dropped_too_future: u64,
    pub missing_timestamp: u64,
}

impl DatapointPolicyCounts {
    pub fn rejected(&self) -> u64 {
        self.dropped_too_old
            .saturating_add(self.dropped_too_future)
            .saturating_add(self.missing_timestamp)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DatapointStorageCounts {
    pub recorded_samples: u64,
    pub missing_number_values: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventTimeSkewOutcome {
    Accepted,
    DroppedTooOld,
    DroppedTooFuture,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EventTimeSkewSnapshot {
    pub all: Option<DistI64>,
    pub accepted: Option<DistI64>,
    pub dropped_too_old: Option<DistI64>,
    pub dropped_too_future: Option<DistI64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricDataType {
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

#[derive(Clone, Copy, Debug)]
pub struct MessageScope {
    intern_time_at_start: Duration,
}

#[derive(Clone, Debug)]
pub struct TotalsSnapshot {
    pub messages: u64,
    pub metrics: u64,
    pub observed_datapoints: u64,
    pub datapoints: u64,
    pub unique_metrics: usize,
    pub metric_types: OtlpDataTypeCounts,
    pub observed_datapoint_types: OtlpDataTypeCounts,
    pub datapoint_types: OtlpDataTypeCounts,
    pub datapoint_policy: DatapointPolicyCounts,
    pub datapoint_storage: DatapointStorageCounts,
    pub event_time_skew: EventTimeSkewSnapshot,
    pub processing_time: Duration,
    pub intern_time: Duration,
    pub skipped_non_scalar_values: u64,
    pub skipped_labelset_errors: u64,
}

#[derive(Clone, Debug)]
pub struct WindowSnapshot {
    pub elapsed: Duration,
    pub messages: u64,
    pub metrics: u64,
    pub observed_datapoints: u64,
    pub datapoints: u64,
    pub datapoint_policy: DatapointPolicyCounts,
    pub datapoint_storage: DatapointStorageCounts,
    pub event_time_skew: EventTimeSkewSnapshot,
    pub unique_metrics: u64,
    pub processing_time: Duration,
    pub intern_time: Duration,
    #[allow(dead_code)]
    pub intern_time_interned: Duration,
    #[allow(dead_code)]
    pub intern_time_keyset: Duration,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub totals: TotalsSnapshot,
    pub window: WindowSnapshot,
    pub partition_watermarks: Vec<((String, i32), PartitionWatermark)>,
}

#[derive(Default)]
struct OtlpTotals {
    messages: u64,
    metrics: u64,
    observed_datapoints: u64,
    datapoints: u64,

    metric_types: OtlpDataTypeCounts,
    observed_datapoint_types: OtlpDataTypeCounts,
    datapoint_types: OtlpDataTypeCounts,
    datapoint_policy: DatapointPolicyCounts,
    datapoint_storage: DatapointStorageCounts,
    event_time_skew: EventTimeSkewStats,

    processing_time: Duration,
    intern_time: Duration,

    unique_metric_names: U64HashSet,

    skipped_non_scalar_values: u64,
    skipped_labelset_errors: u64,
}

struct OtlpReportWindow {
    messages: u64,
    metrics: u64,
    observed_datapoints: u64,
    datapoints: u64,
    datapoint_policy: DatapointPolicyCounts,
    datapoint_storage: DatapointStorageCounts,
    event_time_skew: EventTimeSkewStats,

    processing_time: Duration,
    intern_time: Duration,
    intern_time_interned: Duration,
    intern_time_keyset: Duration,

    unique_metrics: u64,

    started_at: Instant,
}

impl OtlpReportWindow {
    fn new() -> Self {
        Self {
            messages: 0,
            metrics: 0,
            observed_datapoints: 0,
            datapoints: 0,
            datapoint_policy: DatapointPolicyCounts::default(),
            datapoint_storage: DatapointStorageCounts::default(),
            event_time_skew: EventTimeSkewStats::new(),
            processing_time: Duration::from_secs(0),
            intern_time: Duration::from_secs(0),
            intern_time_interned: Duration::from_secs(0),
            intern_time_keyset: Duration::from_secs(0),
            unique_metrics: 0,
            started_at: Instant::now(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

struct EventTimeSkewStats {
    all: Stats<i64>,
    accepted: Stats<i64>,
    dropped_too_old: Stats<i64>,
    dropped_too_future: Stats<i64>,
}

impl EventTimeSkewStats {
    fn new() -> Self {
        Self {
            all: new_skew_stats(),
            accepted: new_skew_stats(),
            dropped_too_old: new_skew_stats(),
            dropped_too_future: new_skew_stats(),
        }
    }

    fn record(&mut self, outcome: EventTimeSkewOutcome, skew_ms: i64) {
        self.all.insert(skew_ms);
        match outcome {
            EventTimeSkewOutcome::Accepted => self.accepted.insert(skew_ms),
            EventTimeSkewOutcome::DroppedTooOld => self.dropped_too_old.insert(skew_ms),
            EventTimeSkewOutcome::DroppedTooFuture => self.dropped_too_future.insert(skew_ms),
        }
    }

    fn snapshot(&self) -> EventTimeSkewSnapshot {
        EventTimeSkewSnapshot {
            all: self.all.summarize(),
            accepted: self.accepted.summarize(),
            dropped_too_old: self.dropped_too_old.summarize(),
            dropped_too_future: self.dropped_too_future.summarize(),
        }
    }
}

impl Default for EventTimeSkewStats {
    fn default() -> Self {
        Self::new()
    }
}

fn new_skew_stats() -> Stats<i64> {
    Stats::new_tdigest(
        DEFAULT_TDIGEST_MAX_CENTROIDS,
        DEFAULT_TDIGEST_BUFFER_CAPACITY,
    )
}

pub struct OtlpMetricsIngestionStats {
    totals: OtlpTotals,
    window: OtlpReportWindow,
    latency_samples: LatencySamples,
    partition_watermarks: HashMap<(String, i32), PartitionWatermark>,
}

impl OtlpMetricsIngestionStats {
    pub fn new() -> Self {
        Self {
            totals: OtlpTotals::default(),
            window: OtlpReportWindow::new(),
            latency_samples: LatencySamples::new(),
            partition_watermarks: HashMap::new(),
        }
    }

    pub fn begin_message(&self) -> MessageScope {
        MessageScope {
            intern_time_at_start: self.window.intern_time,
        }
    }

    pub fn finish_message(
        &mut self,
        scope: MessageScope,
        total: Duration,
        accepted_datapoints: u64,
        observed_datapoints: u64,
    ) {
        self.totals.messages = self.totals.messages.saturating_add(1);
        self.window.messages = self.window.messages.saturating_add(1);
        self.totals.observed_datapoints = self
            .totals
            .observed_datapoints
            .saturating_add(observed_datapoints);
        self.window.observed_datapoints = self
            .window
            .observed_datapoints
            .saturating_add(observed_datapoints);
        self.totals.datapoints = self.totals.datapoints.saturating_add(accepted_datapoints);
        self.window.datapoints = self.window.datapoints.saturating_add(accepted_datapoints);
        self.totals.datapoint_policy.accepted = self
            .totals
            .datapoint_policy
            .accepted
            .saturating_add(accepted_datapoints);
        self.window.datapoint_policy.accepted = self
            .window
            .datapoint_policy
            .accepted
            .saturating_add(accepted_datapoints);

        self.totals.processing_time += total;
        self.window.processing_time += total;

        let intern_elapsed = self
            .window
            .intern_time
            .saturating_sub(scope.intern_time_at_start);
        let build_elapsed = total.saturating_sub(intern_elapsed);
        self.latency_samples
            .record(total, intern_elapsed, build_elapsed, observed_datapoints);
    }

    pub fn record_intern(&mut self, kind: LabelSetStoreKind, elapsed: Duration) {
        self.totals.intern_time += elapsed;
        self.window.intern_time += elapsed;

        match kind {
            LabelSetStoreKind::Naive => self.window.intern_time_interned += elapsed,
            LabelSetStoreKind::FlatInterned => self.window.intern_time_interned += elapsed,
            LabelSetStoreKind::KeySetDictEncoded => self.window.intern_time_keyset += elapsed,
        }
    }

    pub fn record_metric_record(
        &mut self,
        metric_name: &str,
        metric_type: MetricDataType,
        observed_datapoints: u64,
        accepted_datapoints: u64,
    ) {
        self.totals.metrics = self.totals.metrics.saturating_add(1);
        self.window.metrics = self.window.metrics.saturating_add(1);

        self.totals.metric_types.incr(metric_type, 1);
        self.totals
            .observed_datapoint_types
            .incr(metric_type, observed_datapoints);
        self.totals
            .datapoint_types
            .incr(metric_type, accepted_datapoints);

        if !metric_name.is_empty() {
            let hash = hash_u64(metric_name.as_bytes());
            if self.totals.unique_metric_names.insert(hash) {
                self.window.unique_metrics = self.window.unique_metrics.saturating_add(1);
            }
        }
    }

    pub fn record_partition_watermark(
        &mut self,
        topic: String,
        partition: i32,
        timestamp_ms: i64,
        datapoints: u64,
    ) {
        if timestamp_ms < 0 {
            return;
        }

        let chrono::LocalResult::Single(ts) = Utc.timestamp_millis_opt(timestamp_ms) else {
            return;
        };

        self.partition_watermarks
            .entry((topic, partition))
            .and_modify(|wm| wm.update(ts, datapoints))
            .or_insert_with(|| PartitionWatermark::new(ts, datapoints));
    }

    pub fn record_dropped_too_old_datapoints(&mut self, count: u64) {
        saturating_add_both(
            &mut self.totals.datapoint_policy.dropped_too_old,
            &mut self.window.datapoint_policy.dropped_too_old,
            count,
        );
    }

    pub fn record_dropped_too_future_datapoints(&mut self, count: u64) {
        saturating_add_both(
            &mut self.totals.datapoint_policy.dropped_too_future,
            &mut self.window.datapoint_policy.dropped_too_future,
            count,
        );
    }

    pub fn record_missing_timestamp_datapoints(&mut self, count: u64) {
        saturating_add_both(
            &mut self.totals.datapoint_policy.missing_timestamp,
            &mut self.window.datapoint_policy.missing_timestamp,
            count,
        );
    }

    pub fn record_recorded_samples(&mut self, count: u64) {
        saturating_add_both(
            &mut self.totals.datapoint_storage.recorded_samples,
            &mut self.window.datapoint_storage.recorded_samples,
            count,
        );
    }

    pub fn record_missing_number_values(&mut self, count: u64) {
        saturating_add_both(
            &mut self.totals.datapoint_storage.missing_number_values,
            &mut self.window.datapoint_storage.missing_number_values,
            count,
        );
    }

    pub fn record_event_time_skew(&mut self, outcome: EventTimeSkewOutcome, skew_ms: i64) {
        self.totals.event_time_skew.record(outcome, skew_ms);
        self.window.event_time_skew.record(outcome, skew_ms);
    }

    pub fn record_skipped_non_scalar_value(&mut self) {
        self.totals.skipped_non_scalar_values =
            self.totals.skipped_non_scalar_values.saturating_add(1);
    }

    pub fn record_labelset_error(&mut self) {
        self.totals.skipped_labelset_errors = self.totals.skipped_labelset_errors.saturating_add(1);
    }

    pub fn latency_samples(&self) -> &LatencySamples {
        &self.latency_samples
    }

    pub fn window_elapsed(&self) -> Duration {
        self.window.elapsed()
    }

    pub fn reset_window(&mut self) {
        self.window.reset();
    }

    pub fn snapshot(&self) -> Snapshot {
        let partition_watermarks = self
            .partition_watermarks
            .iter()
            .map(|((topic, part), wm)| ((topic.clone(), *part), wm.clone()))
            .collect();

        Snapshot {
            totals: TotalsSnapshot {
                messages: self.totals.messages,
                metrics: self.totals.metrics,
                observed_datapoints: self.totals.observed_datapoints,
                datapoints: self.totals.datapoints,
                unique_metrics: self.totals.unique_metric_names.len(),
                metric_types: self.totals.metric_types,
                observed_datapoint_types: self.totals.observed_datapoint_types,
                datapoint_types: self.totals.datapoint_types,
                datapoint_policy: self.totals.datapoint_policy,
                datapoint_storage: self.totals.datapoint_storage,
                event_time_skew: self.totals.event_time_skew.snapshot(),
                processing_time: self.totals.processing_time,
                intern_time: self.totals.intern_time,
                skipped_non_scalar_values: self.totals.skipped_non_scalar_values,
                skipped_labelset_errors: self.totals.skipped_labelset_errors,
            },
            window: WindowSnapshot {
                elapsed: self.window.elapsed(),
                messages: self.window.messages,
                metrics: self.window.metrics,
                observed_datapoints: self.window.observed_datapoints,
                datapoints: self.window.datapoints,
                datapoint_policy: self.window.datapoint_policy,
                datapoint_storage: self.window.datapoint_storage,
                event_time_skew: self.window.event_time_skew.snapshot(),
                unique_metrics: self.window.unique_metrics,
                processing_time: self.window.processing_time,
                intern_time: self.window.intern_time,
                intern_time_interned: self.window.intern_time_interned,
                intern_time_keyset: self.window.intern_time_keyset,
            },
            partition_watermarks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_metrics_counts_only_on_first_insert() {
        let mut stats = OtlpMetricsIngestionStats::new();
        stats.record_metric_record("m1", MetricDataType::Gauge, 1, 1);
        stats.record_metric_record("m1", MetricDataType::Gauge, 2, 1);

        let snap = stats.snapshot();
        assert_eq!(snap.totals.unique_metrics, 1);
        assert_eq!(snap.window.unique_metrics, 1);
        assert_eq!(snap.totals.metrics, 2);
        assert_eq!(snap.window.metrics, 2);
        assert_eq!(snap.totals.observed_datapoint_types.gauge, 3);
        assert_eq!(snap.totals.datapoint_types.gauge, 2);

        stats.reset_window();
        stats.record_metric_record("m1", MetricDataType::Gauge, 1, 1);
        let snap = stats.snapshot();
        assert_eq!(snap.totals.unique_metrics, 1);
        assert_eq!(snap.window.unique_metrics, 0);
    }

    #[test]
    fn finish_message_updates_totals_window_and_latency_samples() {
        let mut stats = OtlpMetricsIngestionStats::new();

        let scope = stats.begin_message();
        stats.record_intern(LabelSetStoreKind::FlatInterned, Duration::from_millis(2));
        stats.finish_message(scope, Duration::from_millis(10), 5, 7);

        let snap = stats.snapshot();
        assert_eq!(snap.totals.messages, 1);
        assert_eq!(snap.window.messages, 1);
        assert_eq!(snap.totals.datapoints, 5);
        assert_eq!(snap.window.datapoints, 5);
        assert_eq!(snap.totals.observed_datapoints, 7);
        assert_eq!(snap.window.observed_datapoints, 7);
        assert_eq!(snap.totals.processing_time, Duration::from_millis(10));
        assert_eq!(snap.window.processing_time, Duration::from_millis(10));
        assert_eq!(snap.totals.intern_time, Duration::from_millis(2));
        assert_eq!(snap.window.intern_time, Duration::from_millis(2));
        assert_eq!(snap.window.intern_time_interned, Duration::from_millis(2));

        let samples = stats.latency_samples();
        assert_eq!(samples.msg_seen, 1);
        assert_eq!(samples.dp_seen, 7);
        assert_eq!(samples.msg_sample_count(), 1);
        assert_eq!(samples.dp_sample_count(), 1);
    }

    #[test]
    fn datapoint_policy_counts_track_accepted_and_rejected_points() {
        let mut stats = OtlpMetricsIngestionStats::new();

        let scope = stats.begin_message();
        stats.record_dropped_too_old_datapoints(2);
        stats.record_dropped_too_future_datapoints(3);
        stats.record_missing_timestamp_datapoints(4);
        stats.finish_message(scope, Duration::from_millis(1), 5, 14);

        let snap = stats.snapshot();
        assert_eq!(snap.totals.observed_datapoints, 14);
        assert_eq!(snap.window.observed_datapoints, 14);
        assert_eq!(snap.totals.datapoints, 5);
        assert_eq!(snap.window.datapoints, 5);
        assert_eq!(snap.totals.datapoint_policy.accepted, 5);
        assert_eq!(snap.totals.datapoint_policy.dropped_too_old, 2);
        assert_eq!(snap.totals.datapoint_policy.dropped_too_future, 3);
        assert_eq!(snap.totals.datapoint_policy.missing_timestamp, 4);
        assert_eq!(snap.totals.datapoint_policy.rejected(), 9);
        assert_eq!(snap.window.datapoint_policy, snap.totals.datapoint_policy);

        stats.reset_window();
        let snap = stats.snapshot();
        assert_eq!(snap.totals.datapoint_policy.accepted, 5);
        assert_eq!(
            snap.window.datapoint_policy,
            DatapointPolicyCounts::default()
        );
    }

    #[test]
    fn datapoint_storage_counts_track_recorded_and_missing_number_values() {
        let mut stats = OtlpMetricsIngestionStats::new();

        stats.record_recorded_samples(3);
        stats.record_missing_number_values(2);

        let snap = stats.snapshot();
        assert_eq!(snap.totals.datapoint_storage.recorded_samples, 3);
        assert_eq!(snap.totals.datapoint_storage.missing_number_values, 2);
        assert_eq!(snap.window.datapoint_storage, snap.totals.datapoint_storage);

        stats.reset_window();
        let snap = stats.snapshot();
        assert_eq!(snap.totals.datapoint_storage.recorded_samples, 3);
        assert_eq!(snap.totals.datapoint_storage.missing_number_values, 2);
        assert_eq!(
            snap.window.datapoint_storage,
            DatapointStorageCounts::default()
        );
    }

    #[test]
    fn event_time_skew_stats_track_outcomes_and_reset_window() {
        let mut stats = OtlpMetricsIngestionStats::new();

        stats.record_event_time_skew(EventTimeSkewOutcome::Accepted, -5_000);
        stats.record_event_time_skew(EventTimeSkewOutcome::DroppedTooOld, -10_001);
        stats.record_event_time_skew(EventTimeSkewOutcome::DroppedTooFuture, 5_001);

        let snap = stats.snapshot();
        let totals = snap.totals.event_time_skew;
        let all = totals.all.unwrap();
        assert_eq!(all.count, 3);
        assert_eq!(all.min, -10_001);
        assert_eq!(all.max, 5_001);
        assert_eq!(totals.accepted.unwrap().min, -5_000);
        assert_eq!(totals.accepted.unwrap().max, -5_000);
        assert_eq!(totals.dropped_too_old.unwrap().min, -10_001);
        assert_eq!(totals.dropped_too_future.unwrap().max, 5_001);

        let window = snap.window.event_time_skew;
        assert_eq!(window.all.unwrap().count, 3);
        assert_eq!(window.accepted.unwrap().min, -5_000);

        stats.reset_window();
        let snap = stats.snapshot();
        assert_eq!(snap.totals.event_time_skew.all.unwrap().count, 3);
        assert!(snap.window.event_time_skew.all.is_none());
    }

    #[test]
    fn partition_watermark_tracks_min_max_and_counts() {
        let mut stats = OtlpMetricsIngestionStats::new();

        stats.record_partition_watermark("t".to_string(), 0, 2_000, 10);
        stats.record_partition_watermark("t".to_string(), 0, 1_000, 1);
        stats.record_partition_watermark("t".to_string(), 0, 3_000, 2);
        stats.record_partition_watermark("t".to_string(), 1, -1, 999);

        let snap = stats.snapshot();
        assert_eq!(snap.partition_watermarks.len(), 1);
        let ((topic, partition), wm) = &snap.partition_watermarks[0];
        assert_eq!(topic, "t");
        assert_eq!(*partition, 0);
        assert_eq!(wm.messages, 3);
        assert_eq!(wm.datapoints, 13);
        assert_eq!(wm.min_ts, Utc.timestamp_millis_opt(1_000).single().unwrap());
        assert_eq!(wm.max_ts, Utc.timestamp_millis_opt(3_000).single().unwrap());
    }

    #[test]
    fn reset_window_clears_window_counters() {
        let mut stats = OtlpMetricsIngestionStats::new();
        stats.record_metric_record("m1", MetricDataType::Gauge, 3, 2);
        stats.record_intern(
            LabelSetStoreKind::KeySetDictEncoded,
            Duration::from_millis(1),
        );
        stats.reset_window();

        let snap = stats.snapshot();
        assert_eq!(snap.window.messages, 0);
        assert_eq!(snap.window.metrics, 0);
        assert_eq!(snap.window.observed_datapoints, 0);
        assert_eq!(snap.window.datapoints, 0);
        assert_eq!(snap.window.unique_metrics, 0);
        assert_eq!(snap.window.intern_time, Duration::from_secs(0));
        assert_eq!(snap.window.intern_time_interned, Duration::from_secs(0));
        assert_eq!(snap.window.intern_time_keyset, Duration::from_secs(0));
    }
}
