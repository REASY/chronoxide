use crate::app_config::LabelSetStoreKind;
use crate::source::SourceMessageMetadata;
use crate::statistics::{label_tag_stats_from_store, per_key_value_stats_markdown_from_store};
use chrono::{DateTime, Local, TimeDelta, Utc};
use chronoxide_core::error::should_log;
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, LabelSetStoreError, NaiveLabelSetStore, SeriesRef, SymbolTable as _, TmpLabel,
};
use chronoxide_core::otlp::{
    exponential_histogram_value, histogram_value, number_value, summary_value,
};
use chronoxide_core::otlp_labelset::{
    OtlpLabelSetInterner, intern_labelset as intern_otlp_labelset,
};
use chronoxide_core::prelude::*;
use chronoxide_core::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, FloatEncoding,
    HeadBuffer, HeadConfig, HeadWindow, HistogramValue, OtlpAggregationTemporality, SampleValue,
    SeriesSamples, SummaryValue, downscale_exponential_histogram_buckets_to_map,
};
use chronoxide_core::storage::segment::{SegmentRecordProfile, SegmentWriter};
#[cfg(test)]
use chronoxide_core::storage::segment::{SegmentSeriesMetadata, SegmentSeriesMetadataBuilder};
use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{Level, error, info};

mod head_stats;
mod metrics_ingestion_stats;

use self::head_stats::HeadBufferStats;
use self::metrics_ingestion_stats::{
    DatapointPolicyCounts, DatapointStorageCounts, EventTimeSkewOutcome, EventTimeSkewSnapshot,
    MetricDataType, OtlpDataTypeCounts, OtlpMetricsIngestionStats,
};

type InternedStore = FlatInternedLabelSetStore<DefaultSymbolTable>;
type KeysetStore = KeySetDictEncodedLabelSetStore<DefaultSymbolTable>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic: String,
    partition: i32,
}

impl PartitionKey {
    fn new(topic: &str, partition: i32) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
        }
    }
}

impl std::cmp::Ord for PartitionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.topic
            .cmp(&other.topic)
            .then_with(|| self.partition.cmp(&other.partition))
    }
}

impl std::cmp::PartialOrd for PartitionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.topic, self.partition)
    }
}

struct PartitionHead {
    head: HeadBuffer,
    stats: HeadBufferStats,
}

fn format_window_ms(mut ms: i64) -> String {
    let sign = if ms < 0 {
        ms = -ms;
        "-"
    } else {
        ""
    };

    let hours = ms / 3_600_000;
    let minutes = (ms % 3_600_000) / 60_000;
    let seconds = (ms % 60_000) / 1_000;
    let millis = ms % 1_000;

    format!(
        "{}{:02}:{:02}:{:02}.{:03}",
        sign, hours, minutes, seconds, millis
    )
}

#[derive(Debug, Eq, PartialEq, strum_macros::Display)]
pub enum ProcessResult {
    #[allow(dead_code)]
    EmptyPayload,
    #[allow(dead_code)]
    DroppedOutdated,
    #[allow(dead_code)]
    SinkChannelClosed(String),
    CapturedOnly,
    Ok,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct EventTimePolicy {
    // Maximum accepted backfill age relative to trusted capture time:
    // event_ms must be >= captured_at_ms - max_event_age_ms.
    max_event_age_ms: i64,
    // Maximum accepted future skew relative to trusted capture time:
    // event_ms must be <= captured_at_ms + max_event_lead_ms.
    max_event_lead_ms: i64,
    drop_outdated: bool,
}

impl EventTimePolicy {
    pub fn new(max_event_age: TimeDelta, max_event_lead: TimeDelta, drop_outdated: bool) -> Self {
        assert!(
            max_event_age >= TimeDelta::zero(),
            "max_event_age must be non-negative"
        );
        assert!(
            max_event_lead >= TimeDelta::zero(),
            "max_event_lead must be non-negative"
        );
        Self {
            max_event_age_ms: max_event_age.num_milliseconds(),
            max_event_lead_ms: max_event_lead.num_milliseconds(),
            drop_outdated,
        }
    }

    fn evaluate(&self, time_unix_nano: u64, captured_at_ms: i64) -> DatapointTimeEvaluation {
        if time_unix_nano == 0 {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::MissingTimestamp,
                skew_ms: None,
            };
        }

        let event_ms = time_unix_nano / 1_000_000;
        let event_ms_i128 = i128::from(event_ms);
        let captured_at_ms_i128 = i128::from(captured_at_ms);
        let skew_ms = Some(saturating_i128_to_i64(event_ms_i128 - captured_at_ms_i128));

        if !self.drop_outdated {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::Accepted(event_ms),
                skew_ms,
            };
        }

        let min_event_ms = captured_at_ms_i128 - i128::from(self.max_event_age_ms);
        if event_ms_i128 < min_event_ms {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::DroppedTooOld,
                skew_ms,
            };
        }

        let max_event_ms = captured_at_ms_i128 + i128::from(self.max_event_lead_ms);
        if event_ms_i128 > max_event_ms {
            return DatapointTimeEvaluation {
                decision: DatapointTimeDecision::DroppedTooFuture,
                skew_ms,
            };
        }

        DatapointTimeEvaluation {
            decision: DatapointTimeDecision::Accepted(event_ms),
            skew_ms,
        }
    }
}

impl Default for EventTimePolicy {
    fn default() -> Self {
        Self {
            max_event_age_ms: 0,
            max_event_lead_ms: 0,
            drop_outdated: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DatapointTimeDecision {
    Accepted(u64),
    DroppedTooOld,
    DroppedTooFuture,
    MissingTimestamp,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct DatapointTimeEvaluation {
    decision: DatapointTimeDecision,
    skew_ms: Option<i64>,
}

fn saturating_i128_to_i64(value: i128) -> i64 {
    if value > i128::from(i64::MAX) {
        i64::MAX
    } else if value < i128::from(i64::MIN) {
        i64::MIN
    } else {
        value as i64
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct DatapointIngestResult {
    accepted: u64,
    dropped_too_old: u64,
    dropped_too_future: u64,
    missing_timestamp: u64,
}

impl DatapointIngestResult {
    fn record(&mut self, decision: DatapointTimeDecision) -> Option<u64> {
        match decision {
            DatapointTimeDecision::Accepted(ts_ms) => {
                self.accepted = self.accepted.saturating_add(1);
                Some(ts_ms)
            }
            DatapointTimeDecision::DroppedTooOld => {
                self.dropped_too_old = self.dropped_too_old.saturating_add(1);
                None
            }
            DatapointTimeDecision::DroppedTooFuture => {
                self.dropped_too_future = self.dropped_too_future.saturating_add(1);
                None
            }
            DatapointTimeDecision::MissingTimestamp => {
                self.missing_timestamp = self.missing_timestamp.saturating_add(1);
                None
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.accepted = self.accepted.saturating_add(other.accepted);
        self.dropped_too_old = self.dropped_too_old.saturating_add(other.dropped_too_old);
        self.dropped_too_future = self
            .dropped_too_future
            .saturating_add(other.dropped_too_future);
        self.missing_timestamp = self
            .missing_timestamp
            .saturating_add(other.missing_timestamp);
    }

    fn rejected(&self) -> u64 {
        self.dropped_too_old
            .saturating_add(self.dropped_too_future)
            .saturating_add(self.missing_timestamp)
    }

    fn observed(&self) -> u64 {
        self.accepted.saturating_add(self.rejected())
    }
}

pub trait Processor {
    fn process(
        &mut self,
        metadata: SourceMessageMetadata,
        decoded: ExportMetricsServiceRequest,
    ) -> Result<ProcessResult>;

    fn force_report(&mut self);

    fn shutdown(&mut self) {}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeadWindowWriteProfile {
    pub start_ms: u64,
    pub end_ms: u64,
    pub datapoints: u64,
    pub series: u64,
    pub total: Duration,
    pub seal_decode: Duration,
    pub series_reserve: Duration,
    pub label_clone: Duration,
    pub int_conversion: Duration,
    pub record_samples: Duration,
    pub record_subphases: SegmentRecordProfile,
    pub writer_flush: Duration,
    pub dropped_histogram_series: u64,
    pub dropped_exponential_histogram_series: u64,
    pub dropped_summary_series: u64,
}

pub struct OtlpLabelSetProcessor {
    report_interval: Duration,
    labelsets: LabelSetInterner,
    labelset_stats: OtlpMetricsIngestionStats,
    event_time_policy: EventTimePolicy,
    head_config: Option<HeadConfig>,
    partition_heads: HashMap<PartitionKey, PartitionHead>,
    histogram_reset_state: HashMap<SeriesRef, HistogramResetState>,
    exponential_histogram_reset_state: HashMap<SeriesRef, ExponentialHistogramResetState>,
    segment_writer: Option<SegmentWriter>,
    last_head_window_write_profile: Option<HeadWindowWriteProfile>,
    shutdown_report: bool,
}

#[derive(Debug, Clone)]
struct HistogramResetState {
    start_time_ms: Option<u64>,
    count: u64,
    sum: Option<f64>,
    explicit_bounds: Vec<f64>,
    bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone)]
struct ExponentialHistogramResetState {
    start_time_ms: Option<u64>,
    count: u64,
    sum: Option<f64>,
    scale: i32,
    zero_threshold_bits: u64,
    zero_count: u64,
    positive: ExponentialHistogramBuckets,
    negative: ExponentialHistogramBuckets,
}

impl OtlpLabelSetProcessor {
    pub fn new(
        store: LabelSetStoreKind,
        report_interval: Duration,
        head_config: Option<HeadConfig>,
        segment_writer: Option<SegmentWriter>,
    ) -> Self {
        Self {
            report_interval,
            labelsets: LabelSetInterner::new(store),
            labelset_stats: OtlpMetricsIngestionStats::new(),
            event_time_policy: EventTimePolicy::default(),
            head_config,
            partition_heads: HashMap::new(),
            histogram_reset_state: HashMap::new(),
            exponential_histogram_reset_state: HashMap::new(),
            segment_writer,
            last_head_window_write_profile: None,
            shutdown_report: true,
        }
    }

    pub fn with_event_time_policy(mut self, policy: EventTimePolicy) -> Self {
        self.event_time_policy = policy;
        self
    }

    pub fn last_head_window_write_profile(&self) -> Option<&HeadWindowWriteProfile> {
        self.last_head_window_write_profile.as_ref()
    }

    pub fn with_shutdown_report(mut self, enabled: bool) -> Self {
        self.shutdown_report = enabled;
        self
    }

    fn stamp_histogram_reset_hint(&mut self, series: SeriesRef, value: &mut HistogramValue) {
        value.metadata.reset_hint = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative => {
                if value.metadata.is_stale() {
                    CounterResetHint::Unknown
                } else {
                    let current = HistogramResetState::from_value(value);
                    let hint = self
                        .histogram_reset_state
                        .get(&series)
                        .map(|previous| histogram_reset_hint(previous, &current))
                        .unwrap_or(CounterResetHint::Unknown);
                    self.histogram_reset_state.insert(series, current);
                    hint
                }
            }
            OtlpAggregationTemporality::Delta => CounterResetHint::NotCounterReset,
            OtlpAggregationTemporality::Unspecified => CounterResetHint::Unknown,
        };
    }

    fn stamp_exponential_histogram_reset_hint(
        &mut self,
        series: SeriesRef,
        value: &mut ExponentialHistogramValue,
    ) {
        value.metadata.reset_hint = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative => {
                if value.metadata.is_stale() {
                    CounterResetHint::Unknown
                } else {
                    let current = ExponentialHistogramResetState::from_value(value);
                    let hint = self
                        .exponential_histogram_reset_state
                        .get(&series)
                        .map(|previous| exponential_histogram_reset_hint(previous, &current))
                        .unwrap_or(CounterResetHint::Unknown);
                    self.exponential_histogram_reset_state
                        .insert(series, current);
                    hint
                }
            }
            OtlpAggregationTemporality::Delta => CounterResetHint::NotCounterReset,
            OtlpAggregationTemporality::Unspecified => CounterResetHint::Unknown,
        };
    }

    fn write_markdown_report(&mut self) {
        let report_start = Instant::now();
        let ingestion = self.labelset_stats.snapshot();
        let store_stats_start = Instant::now();
        let store_stats = self.labelsets.stats();
        let store_stats_time = store_stats_start.elapsed();
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let filename = format!("ingestion_stats_{}.md", timestamp);
        let path = PathBuf::from(filename);
        let mut md = String::new();
        md.push_str("# Ingestion Statistics\n\n");

        let general_stats_start = Instant::now();
        md.push_str("## General Stats\n\n");
        md.push_str("| Metric | Value |\n|---|---|\n");
        md.push_str(&format!(
            "| Total Messages | {} |\n",
            ingestion.totals.messages
        ));
        md.push_str(&format!(
            "| Total OTLP Metric Records | {} |\n",
            ingestion.totals.metrics
        ));
        md.push_str(&format!(
            "| Total Unique Metrics (`__name__`) | {} |\n",
            ingestion.totals.unique_metrics
        ));
        md.push_str(&format!(
            "| Total Series (unique label sets) | {} |\n",
            store_stats.series
        ));
        md.push_str(&format!(
            "| Observed OTLP Datapoints | {} |\n",
            ingestion.totals.observed_datapoints
        ));
        md.push_str(&format!(
            "| Accepted Datapoints | {} |\n",
            ingestion.totals.datapoints
        ));
        md.push_str(&format!(
            "| Total Processing Time | {:?} |\n",
            ingestion.totals.processing_time
        ));
        md.push_str(&format!(
            "| Total Intern Time | {:?} |\n",
            ingestion.totals.intern_time
        ));
        md.push_str(&format!(
            "| Skipped Non-Scalar | {} |\n",
            ingestion.totals.skipped_non_scalar_values
        ));
        md.push_str(&format!(
            "| Recorded Samples | {} |\n",
            ingestion.totals.datapoint_storage.recorded_samples
        ));
        md.push_str(&format!(
            "| Missing Number Value | {} |\n",
            ingestion.totals.datapoint_storage.missing_number_values
        ));
        md.push('\n');

        md.push_str(&datapoint_policy_counts_markdown(
            &ingestion.totals.datapoint_policy,
            &ingestion.window.datapoint_policy,
        ));
        md.push_str(&datapoint_storage_counts_markdown(
            &ingestion.totals.datapoint_storage,
            &ingestion.window.datapoint_storage,
            &ingestion.totals.datapoint_policy,
            &ingestion.window.datapoint_policy,
        ));
        md.push_str(&event_time_skew_markdown(&ingestion.totals.event_time_skew));
        let general_stats_time = general_stats_start.elapsed();

        let data_type_counts_start = Instant::now();
        md.push_str(&data_type_counts_markdown(
            &ingestion.totals.metric_types,
            &ingestion.totals.observed_datapoint_types,
            &ingestion.totals.datapoint_types,
        ));
        let data_type_counts_time = data_type_counts_start.elapsed();

        let partition_watermarks_start = Instant::now();
        if !ingestion.partition_watermarks.is_empty() {
            let mut rows = ingestion.partition_watermarks.clone();
            rows.sort_by(|((topic_a, part_a), _), ((topic_b, part_b), _)| {
                topic_a.cmp(topic_b).then_with(|| part_a.cmp(part_b))
            });

            let mut overall_min: Option<DateTime<Utc>> = None;
            let mut overall_max: Option<DateTime<Utc>> = None;
            let mut tracked_messages: u64 = 0;
            let mut tracked_datapoints: u64 = 0;

            for (_, wm) in &rows {
                overall_min = Some(overall_min.map_or(wm.min_ts, |cur| cur.min(wm.min_ts)));
                overall_max = Some(overall_max.map_or(wm.max_ts, |cur| cur.max(wm.max_ts)));
                tracked_messages = tracked_messages.saturating_add(wm.messages);
                tracked_datapoints = tracked_datapoints.saturating_add(wm.datapoints);
            }

            md.push_str("## Partition Watermarks\n\n");
            md.push_str(
                "Based on Kafka record timestamps (`timestamp_ms`) seen per `(topic, partition)`.\n\n",
            );
            md.push_str("| Metric | Value |\n|---|---|\n");
            md.push_str(&format!("| Tracked Messages | {} |\n", tracked_messages));
            md.push_str(&format!(
                "| Tracked Datapoints | {} |\n",
                tracked_datapoints
            ));
            md.push_str(&format!(
                "| Missing Timestamp Messages | {} |\n",
                ingestion.totals.messages.saturating_sub(tracked_messages)
            ));
            md.push_str(&format!(
                "| Missing Timestamp Datapoints | {} |\n",
                ingestion
                    .totals
                    .datapoints
                    .saturating_sub(tracked_datapoints)
            ));

            if let (Some(min_ts), Some(max_ts)) = (overall_min, overall_max) {
                let window_ms = (max_ts - min_ts).num_milliseconds();
                md.push_str(&format!(
                    "| Overall Min TS | {} |\n",
                    min_ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                ));
                md.push_str(&format!(
                    "| Overall Max TS | {} |\n",
                    max_ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                ));
                md.push_str(&format!(
                    "| Overall Window | {} ({}ms) |\n",
                    format_window_ms(window_ms),
                    window_ms
                ));
                if window_ms > 0 {
                    let window_s = window_ms as f64 / 1000.0;
                    md.push_str(&format!(
                        "| Tracked Msg/s (event time) | {:.2} |\n",
                        tracked_messages as f64 / window_s
                    ));
                    md.push_str(&format!(
                        "| Tracked DP/s (event time) | {:.2} |\n",
                        tracked_datapoints as f64 / window_s
                    ));
                }
            }
            md.push('\n');

            md.push_str("| Topic | Partition | Messages | Datapoints | Min TS | Max TS | Window | Msg/s | DP/s |\n");
            md.push_str("|---|---:|---:|---:|---|---|---|---:|---:|\n");
            for ((topic, partition), wm) in rows {
                let window_ms = wm.window_ms();
                let (msg_s, dp_s) = if window_ms > 0 {
                    let window_s = window_ms as f64 / 1000.0;
                    (
                        format!("{:.2}", wm.messages as f64 / window_s),
                        format!("{:.2}", wm.datapoints as f64 / window_s),
                    )
                } else {
                    ("n/a".to_string(), "n/a".to_string())
                };

                md.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                    topic,
                    partition,
                    wm.messages,
                    wm.datapoints,
                    wm.min_ts
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    wm.max_ts
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    format_window_ms(window_ms),
                    msg_s,
                    dp_s
                ));
            }
            md.push('\n');
        }
        let partition_watermarks_time = partition_watermarks_start.elapsed();

        let latency_stats_start = Instant::now();
        let latency_md_build_start = Instant::now();
        let latency_md = self.labelset_stats.latency_samples().to_markdown();
        let latency_md_build_time = latency_md_build_start.elapsed();
        let latency_md_append_start = Instant::now();
        md.push_str(&latency_md);
        let latency_md_append_time = latency_md_append_start.elapsed();
        let latency_stats_time = latency_stats_start.elapsed();

        let head_stats_start = Instant::now();
        if !self.partition_heads.is_empty() {
            let mut partitions: Vec<_> = self.partition_heads.iter().collect();
            partitions.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut wrote_section = false;

            for (partition, state) in partitions {
                let dists = state.stats.distributions();
                let mut dist_rows = Vec::new();
                if let Some(dist) = dists.call_latency {
                    dist_rows.push(dist.to_markdown_row("head_call_latency"));
                }
                if let Some(dist) = dists.batch_sizes {
                    dist_rows.push(dist.to_markdown_row("batch_sizes"));
                }
                if let Some(dist) = dists.series_sample_counts {
                    dist_rows.push(dist.to_markdown_row("series_sample_counts"));
                }
                if let Some(dist) = dists.blocks_per_series {
                    dist_rows.push(dist.to_markdown_row("blocks_per_series"));
                }
                if let Some(dist) = dists.samples_per_block {
                    dist_rows.push(dist.to_markdown_row("samples_per_block"));
                }

                let density = state.stats.series_density();
                if dist_rows.is_empty() && density.is_none() {
                    continue;
                }

                if !wrote_section {
                    md.push_str("## Head Buffer Stats (by partition)\n\n");
                    wrote_section = true;
                }

                md.push_str(&format!("### Partition {}\n\n", partition));

                if !dist_rows.is_empty() {
                    md.push_str("#### Distributions\n\n");
                    md.push_str(
                        "| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |\n",
                    );
                    md.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
                    for row in dist_rows {
                        md.push_str(&row);
                    }
                    md.push('\n');
                }

                if let Some(density) = density {
                    md.push_str("#### Series Density\n\n");
                    md.push_str("| Metric | Value |\n|---|---|\n");
                    md.push_str(&format!(
                        "| series_single_sample_count | {} |\n",
                        density.series_single_sample_count
                    ));
                    md.push_str(&format!(
                        "| series_single_sample_ratio | {:.3} |\n",
                        density.series_single_sample_ratio
                    ));
                    md.push_str(&format!(
                        "| series_multi_sample_count | {} |\n",
                        density.series_multi_sample_count
                    ));
                    md.push('\n');
                }
            }
        }
        let head_stats_time = head_stats_start.elapsed();

        let label_tag_stats_compute_start = Instant::now();
        let label_tag_stats = match &self.labelsets {
            LabelSetInterner::Naive(store) => label_tag_stats_from_store(store, None),
            LabelSetInterner::FlatInterned(store) => label_tag_stats_from_store(store, None),
            LabelSetInterner::KeySetDictEncoded(store) => label_tag_stats_from_store(store, None),
        };
        let label_tag_stats_compute_time = label_tag_stats_compute_start.elapsed();
        let label_tag_stats_markdown_start = Instant::now();
        let label_tag_stats_md = label_tag_stats.to_markdown();
        let label_tag_stats_markdown_time = label_tag_stats_markdown_start.elapsed();
        let label_tag_stats_append_start = Instant::now();
        md.push_str(&label_tag_stats_md);
        let label_tag_stats_append_time = label_tag_stats_append_start.elapsed();

        let store_section_start = Instant::now();
        md.push_str("## Store Statistics\n\n");
        md.push_str("| Metric | Value |\n|---|---|\n");
        md.push_str(&format!("| Store Kind | {} |\n", self.labelsets.kind()));
        let symbol_table_name = if store_stats.symbols.is_some() {
            std::any::type_name::<DefaultSymbolTable>()
                .split("::")
                .last()
                .unwrap_or("Unknown")
        } else {
            "None"
        };
        md.push_str(&format!("| Symbol Table | {} |\n", symbol_table_name));
        md.push_str(&format!("| Series Count | {} |\n", store_stats.series));
        md.push_str(&format!(
            "| Allocated Bytes | {} |\n",
            store_stats.alloc_bytes
        ));
        md.push_str(&format!("| Used Bytes | {} |\n", store_stats.used_bytes));

        let series = store_stats.series.max(1);
        let alloc_bits_per_series = store_stats.alloc_bytes as f64 / series as f64;
        let used_bits_per_series = store_stats.used_bytes as f64 / series as f64;
        md.push_str(&format!(
            "| Allocated Bytes/Series | {:.2} |\n",
            alloc_bits_per_series
        ));
        md.push_str(&format!(
            "| Used Bytes/Series | {:.2} |\n",
            used_bits_per_series
        ));

        if let Some(s) = store_stats.symbols {
            md.push_str(&format!("| Symbols | {} |\n", s));
        }
        md.push('\n');
        let store_section_time = store_section_start.elapsed();

        let buffer_stats_start = Instant::now();
        if let Some(bs) = &store_stats.buffer_stats {
            md.push_str("## Buffer Statistics\n\n");
            md.push_str("| Metric | Value |\n|---|---|\n");
            for part in bs.split_whitespace() {
                if let Some((k, v)) = part.split_once('=') {
                    md.push_str(&format!("| {} | {} |\n", k, v));
                }
            }
            md.push('\n');
        }
        let buffer_stats_time = buffer_stats_start.elapsed();

        let symbol_table_stats_start = Instant::now();
        if let Some(sts) = &store_stats.symbol_table_stats {
            md.push_str("## Symbol Table Statistics\n\n");
            md.push_str("| Metric | Value |\n|---|---|\n");
            for part in sts.split_whitespace() {
                if let Some((k, v)) = part.split_once('=') {
                    md.push_str(&format!("| {} | {} |\n", k, v));
                }
            }
            md.push('\n');
        }
        let symbol_table_stats_time = symbol_table_stats_start.elapsed();

        let per_key_stats_build_start = Instant::now();
        let per_key_stats_md = match &self.labelsets {
            LabelSetInterner::Naive(store) => per_key_value_stats_markdown_from_store(store, None),
            LabelSetInterner::FlatInterned(store) => {
                per_key_value_stats_markdown_from_store(store, None)
            }
            LabelSetInterner::KeySetDictEncoded(store) => {
                per_key_value_stats_markdown_from_store(store, None)
            }
        };

        let per_key_stats_build_time = per_key_stats_build_start.elapsed();

        let packed_stats_start = Instant::now();
        let mut bit_packed_stats_time = Duration::ZERO;
        let mut packed_stats_time = Duration::ZERO;
        let mut packed_stats_md = String::new();
        if let LabelSetInterner::KeySetDictEncoded(store) = &mut self.labelsets {
            packed_stats_md = Self::get_packed_stats(store);
            packed_stats_time = packed_stats_start.elapsed();

            let bit_packed_start = Instant::now();
            packed_stats_md += &Self::get_bit_packed_stats(store);
            bit_packed_stats_time = bit_packed_start.elapsed();
        }
        if !packed_stats_md.is_empty() {
            md.push_str(&packed_stats_md);
        }

        let label_tag_stats_total_time = label_tag_stats_compute_time
            .saturating_add(label_tag_stats_markdown_time)
            .saturating_add(label_tag_stats_append_time);
        // Use just build time, append time is super small
        let per_key_stats_total_time = per_key_stats_build_time;

        let report_build_time = report_start.elapsed();

        let accounted_time = store_stats_time
            .saturating_add(general_stats_time)
            .saturating_add(data_type_counts_time)
            .saturating_add(partition_watermarks_time)
            .saturating_add(latency_stats_time)
            .saturating_add(head_stats_time)
            .saturating_add(label_tag_stats_total_time)
            .saturating_add(per_key_stats_total_time)
            .saturating_add(store_section_time)
            .saturating_add(buffer_stats_time)
            .saturating_add(symbol_table_stats_time)
            .saturating_add(bit_packed_stats_time);
        let unaccounted_time = report_build_time.saturating_sub(accounted_time);

        md.push_str("## Report Generation Timing\n\n");
        md.push_str("| Metric | Value |\n|---|---|\n");
        md.push_str(&format!(
            "| Report Build Time (no file I/O) | {:?} |\n",
            report_build_time
        ));
        md.push_str(&format!("| Accounted Time | {:?} |\n", accounted_time));
        md.push_str(&format!("| Unaccounted Time | {:?} |\n", unaccounted_time));
        md.push_str(&format!(
            "| Store Stats Snapshot Time | {:?} |\n",
            store_stats_time
        ));
        md.push_str(&format!(
            "| General Stats Build Time | {:?} |\n",
            general_stats_time
        ));
        md.push_str(&format!(
            "| Data Type Counts Build Time | {:?} |\n",
            data_type_counts_time
        ));
        md.push_str(&format!(
            "| Partition Watermarks Build Time | {:?} |\n",
            partition_watermarks_time
        ));
        md.push_str(&format!(
            "| Latency Stats Total Time | {:?} |\n",
            latency_stats_time
        ));
        md.push_str(&format!(
            "| Head Buffer Stats Build Time | {:?} |\n",
            head_stats_time
        ));
        md.push_str(&format!(
            "| Latency Stats Markdown Build Time | {:?} |\n",
            latency_md_build_time
        ));
        md.push_str(&format!(
            "| Latency Stats Markdown Append Time | {:?} |\n",
            latency_md_append_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Total Time | {:?} |\n",
            label_tag_stats_total_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Compute Time | {:?} |\n",
            label_tag_stats_compute_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Markdown Build Time | {:?} |\n",
            label_tag_stats_markdown_time
        ));
        md.push_str(&format!(
            "| Label Tag Stats Markdown Append Time | {:?} |\n",
            label_tag_stats_append_time
        ));
        md.push_str(&format!(
            "| Per-Key Stats Total Time | {:?} |\n",
            per_key_stats_total_time
        ));
        md.push_str(&format!(
            "| Per-Key Stats Build Time | {:?} |\n",
            per_key_stats_build_time
        ));
        md.push_str(&format!(
            "| Store Stats Section Build Time | {:?} |\n",
            store_section_time
        ));
        md.push_str(&format!(
            "| Buffer Stats Section Build Time | {:?} |\n",
            buffer_stats_time
        ));
        md.push_str(&format!(
            "| Symbol Table Stats Section Build Time | {:?} |\n",
            symbol_table_stats_time
        ));
        md.push_str(&format!(
            "| Packed KeySet Stats Build Time | {:?} |\n",
            packed_stats_time
        ));
        md.push_str(&format!(
            "| Bit-Packed KeySet Stats Build Time | {:?} |\n",
            bit_packed_stats_time
        ));
        md.push('\n');

        md.push_str(&per_key_stats_md);
        md.push('\n');

        if let Ok(mut file) = File::create(&path) {
            if let Err(e) = file.write_all(md.as_bytes()) {
                let report_total_time = report_start.elapsed();
                error!(
                    "Failed to write markdown report to {:?}: {} (time_total={:?}, time_build={:?}, time_per_key_stats={:?})",
                    path, e, report_total_time, report_build_time, per_key_stats_build_time
                );
            } else {
                let report_total_time = report_start.elapsed();
                info!(
                    "Markdown report written to {:?} (time_total={:?}, time_build={:?}, time_per_key_stats={:?})",
                    path, report_total_time, report_build_time, per_key_stats_build_time
                );
            }
        } else {
            let report_total_time = report_start.elapsed();
            error!(
                "Failed to create markdown report file at {:?} (time_total={:?}, time_build={:?}, time_per_key_stats={:?})",
                path, report_total_time, report_build_time, per_key_stats_build_time
            );
        }
    }

    fn get_bit_packed_stats(store: &KeySetDictEncodedLabelSetStore) -> String {
        let mut packed_stats_md: String = String::new();

        let bit_packed = store.seal_bit_packed();

        packed_stats_md.push_str("## Bit-Packed KeySet Store Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        packed_stats_md.push_str("| Store Kind | BitPackedKeySetDictEncoded |\n");
        packed_stats_md.push_str(&format!("| Series Count | {} |\n", bit_packed.len()));
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes | {} |\n",
            bit_packed.estimate_size_bytes()
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes | {} |\n",
            bit_packed.estimate_used_bytes()
        ));
        let series = bit_packed.len().max(1) as f64;
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes/Series | {:.2} |\n",
            bit_packed.estimate_size_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes/Series | {:.2} |\n",
            bit_packed.estimate_used_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!("| Symbols | {} |\n", bit_packed.symbols().len()));
        packed_stats_md.push_str(&format!("| KeySets | {} |\n", bit_packed.keysets().len()));
        packed_stats_md.push('\n');

        let bit_packed_buffer_stats = bit_packed.buffer_stats();
        packed_stats_md.push_str("### Bit-Packed Buffer Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        for part in bit_packed_buffer_stats.to_string().split_whitespace() {
            if let Some((k, v)) = part.split_once('=') {
                packed_stats_md.push_str(&format!("| {} | {} |\n", k, v));
            }
        }
        packed_stats_md.push('\n');
        packed_stats_md
    }

    fn get_packed_stats(store: &KeySetDictEncodedLabelSetStore) -> String {
        let mut packed_stats_md: String = String::new();

        let packed = store.seal_fixed_width();

        packed_stats_md.push_str("## Packed KeySet Store Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        packed_stats_md.push_str("| Store Kind | PackedKeySetDictEncoded |\n");
        packed_stats_md.push_str(&format!("| Series Count | {} |\n", packed.len()));
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes | {} |\n",
            packed.estimate_size_bytes()
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes | {} |\n",
            packed.estimate_used_bytes()
        ));
        let series = packed.len().max(1) as f64;
        packed_stats_md.push_str(&format!(
            "| Allocated Bytes/Series | {:.2} |\n",
            packed.estimate_size_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!(
            "| Used Bytes/Series | {:.2} |\n",
            packed.estimate_used_bytes() as f64 / series
        ));
        packed_stats_md.push_str(&format!("| Symbols | {} |\n", packed.symbols().len()));
        packed_stats_md.push_str(&format!("| KeySets | {} |\n", packed.keysets().len()));
        packed_stats_md.push('\n');

        let packed_buffer_stats = packed.buffer_stats();
        packed_stats_md.push_str("### Packed Buffer Statistics\n\n");
        packed_stats_md.push_str("| Metric | Value |\n|---|---|\n");
        for part in packed_buffer_stats.to_string().split_whitespace() {
            if let Some((k, v)) = part.split_once('=') {
                packed_stats_md.push_str(&format!("| {} | {} |\n", k, v));
            }
        }
        packed_stats_md.push('\n');
        packed_stats_md
    }
}

impl Processor for OtlpLabelSetProcessor {
    fn process(
        &mut self,
        metadata: SourceMessageMetadata,
        decoded: ExportMetricsServiceRequest,
    ) -> Result<ProcessResult> {
        self.process_message(metadata, decoded)
    }

    fn force_report(&mut self) {
        self.maybe_report_labelset_stats(true);
    }

    fn shutdown(&mut self) {
        self.force_report();
        if let Err(err) = self.flush_head()
            && should_log(Level::ERROR, "HeadFlushError", Instant::now())
        {
            error!("Head flush failed: {}", err);
        }
        if self.shutdown_report {
            self.write_markdown_report();
        }
    }
}

impl OtlpLabelSetProcessor {
    fn process_message(
        &mut self,
        metadata: SourceMessageMetadata,
        decoded: ExportMetricsServiceRequest,
    ) -> Result<ProcessResult> {
        let scope = self.labelset_stats.begin_message();
        let start = Instant::now();
        let partition = PartitionKey::new(&metadata.topic, metadata.partition);
        // Temporarily move the partition head out so we can mutably borrow other fields
        // during ingestion without repeated lookups.
        self.ensure_partition_head(&partition)?;
        let mut head_state = if self.head_config.is_some() {
            Some(
                self.partition_heads
                    .remove(&partition)
                    .expect("partition head exists"),
            )
        } else {
            None
        };
        let record_non_number_samples = head_state.is_some();
        let result = self.ingest_otlp_metrics(
            &decoded,
            metadata.captured_at_ms,
            head_state.as_mut(),
            record_non_number_samples,
        );
        if let Some(head_state) = head_state {
            self.partition_heads.insert(partition.clone(), head_state);
        }
        let datapoints = result?;
        self.record_datapoint_policy_drops(datapoints);
        let elapsed = start.elapsed();
        self.labelset_stats.finish_message(
            scope,
            elapsed,
            datapoints.accepted,
            datapoints.observed(),
        );
        self.labelset_stats.record_partition_watermark(
            metadata.topic,
            metadata.partition,
            metadata.timestamp_ms,
            datapoints.observed(),
        );

        self.maybe_report_labelset_stats(false);

        if datapoints.accepted == 0 && datapoints.rejected() > 0 {
            Ok(ProcessResult::DroppedOutdated)
        } else {
            Ok(ProcessResult::Ok)
        }
    }

    fn record_datapoint_policy_drops(&mut self, result: DatapointIngestResult) {
        self.labelset_stats
            .record_dropped_too_old_datapoints(result.dropped_too_old);
        self.labelset_stats
            .record_dropped_too_future_datapoints(result.dropped_too_future);
        self.labelset_stats
            .record_missing_timestamp_datapoints(result.missing_timestamp);
    }

    fn evaluate_datapoint_time(
        &mut self,
        time_unix_nano: u64,
        captured_at_ms: i64,
    ) -> DatapointTimeDecision {
        let evaluation = self
            .event_time_policy
            .evaluate(time_unix_nano, captured_at_ms);
        if let Some(skew_ms) = evaluation.skew_ms {
            let outcome = match evaluation.decision {
                DatapointTimeDecision::Accepted(_) => EventTimeSkewOutcome::Accepted,
                DatapointTimeDecision::DroppedTooOld => EventTimeSkewOutcome::DroppedTooOld,
                DatapointTimeDecision::DroppedTooFuture => EventTimeSkewOutcome::DroppedTooFuture,
                DatapointTimeDecision::MissingTimestamp => return evaluation.decision,
            };
            self.labelset_stats.record_event_time_skew(outcome, skew_ms);
        }
        evaluation.decision
    }

    fn maybe_report_labelset_stats(&mut self, force: bool) {
        let report_elapsed = self.labelset_stats.window_elapsed();
        if !force && report_elapsed < self.report_interval {
            return;
        }

        let store_stats = self.labelsets.stats();
        let ingestion = self.labelset_stats.snapshot();
        self.log_labelset_window(&ingestion, &store_stats, report_elapsed);
        self.log_store_stats(&store_stats);
        self.log_metric_types(&ingestion, report_elapsed);
        self.log_unique_metrics(&ingestion, report_elapsed);
        self.log_datapoint_types(&ingestion, report_elapsed);
        self.log_event_time_skew(&ingestion);
        self.log_partition_watermarks(&ingestion);
        self.report_latency_window();
        self.report_head_stats_window();

        self.labelset_stats.reset_window();
    }

    fn ensure_partition_head(&mut self, partition: &PartitionKey) -> Result<()> {
        let Some(head_config) = self.head_config.as_ref() else {
            return Ok(());
        };
        if self.partition_heads.contains_key(partition) {
            return Ok(());
        }
        let head = HeadBuffer::new(head_config.clone())?;
        let stats = HeadBufferStats::new();
        self.partition_heads
            .insert(partition.clone(), PartitionHead { head, stats });
        Ok(())
    }

    fn record_head_sample(
        &mut self,
        head_state: &mut PartitionHead,
        series: SeriesRef,
        ts_ms: u64,
        value: SampleValue,
    ) -> Result<()> {
        let call_start = Instant::now();
        let window = head_state.head.record_sample(series, ts_ms, value)?;
        let elapsed = call_start.elapsed();
        head_state
            .stats
            .record_call(elapsed, 1, usize::from(window.is_some()));

        let window = if let Some(window) = window {
            head_state.stats.record_window(&window);
            Some(window)
        } else {
            None
        };

        if let Some(window) = window {
            self.write_head_window_samples(window)?;
        }
        self.labelset_stats.record_recorded_samples(1);
        Ok(())
    }

    fn flush_head(&mut self) -> Result<()> {
        if self.partition_heads.is_empty() {
            return Ok(());
        }
        let mut drained: Vec<HeadWindow> = Vec::new();
        for (_partition, state) in &mut self.partition_heads {
            for window in state.head.drain_windows() {
                state.stats.record_window(&window);
                drained.push(window);
            }
        }
        for window in drained {
            self.write_head_window_samples(window)?;
        }
        if let Some(writer) = &mut self.segment_writer {
            writer.flush()?;
        }
        Ok(())
    }

    fn write_head_window_samples(&mut self, window: HeadWindow) -> Result<()> {
        if self.segment_writer.is_none() {
            return Ok(());
        }
        let profile_start = Instant::now();
        let start_ms = window.start_ms;
        let end_ms = window.end_ms;
        let datapoints = window.datapoints;
        let series_count = window.series_len() as u64;
        let mut profile = HeadWindowWriteProfile {
            start_ms,
            end_ms,
            datapoints,
            series: series_count,
            ..HeadWindowWriteProfile::default()
        };

        let seal_decode_start = Instant::now();
        let series_samples = window.into_series_samples()?;
        profile.seal_decode = seal_decode_start.elapsed();

        if let Some(first_timestamp_ms) = first_series_samples_timestamp(&series_samples) {
            if let Some(writer) = &mut self.segment_writer {
                let reserve_start = Instant::now();
                writer.reserve_series_for_timestamp(first_timestamp_ms, series_samples.len())?;
                profile.series_reserve = reserve_start.elapsed();
            }
        }

        let record_profile_before = self
            .segment_writer
            .as_ref()
            .map(SegmentWriter::record_profile);

        for (series, samples) in series_samples {
            match samples {
                SeriesSamples::Float { encoding, samples } => match encoding {
                    FloatEncoding::Gorilla
                    | FloatEncoding::Elf
                    | FloatEncoding::Alp
                    | FloatEncoding::AlpRd
                    | FloatEncoding::AlpSpiral
                    | FloatEncoding::AlpRdSpiral
                    | FloatEncoding::Chimp128DuckDB
                    | FloatEncoding::Chimp128Baseline => {
                        let labelsets = &self.labelsets;
                        let Some(writer) = &mut self.segment_writer else {
                            return Ok(());
                        };
                        let record_start = Instant::now();
                        record_segment_float_samples(labelsets, writer, series, &samples, false)?;
                        profile.record_samples += record_start.elapsed();
                    }
                    FloatEncoding::Raw => {
                        let labelsets = &self.labelsets;
                        let Some(writer) = &mut self.segment_writer else {
                            return Ok(());
                        };
                        let record_start = Instant::now();
                        record_segment_float_samples(labelsets, writer, series, &samples, true)?;
                        profile.record_samples += record_start.elapsed();
                    }
                },
                SeriesSamples::Int64 { samples, .. } => {
                    let conversion_start = Instant::now();
                    let float_samples: Vec<(u64, f64)> = samples
                        .into_iter()
                        .map(|(ts, value)| (ts, value as f64))
                        .collect();
                    profile.int_conversion += conversion_start.elapsed();

                    let labelsets = &self.labelsets;
                    let Some(writer) = &mut self.segment_writer else {
                        return Ok(());
                    };
                    let record_start = Instant::now();
                    record_segment_float_samples(labelsets, writer, series, &float_samples, false)?;
                    profile.record_samples += record_start.elapsed();
                }
                SeriesSamples::Histogram { samples } => {
                    let labelsets = &self.labelsets;
                    let Some(writer) = &mut self.segment_writer else {
                        return Ok(());
                    };
                    let record_start = Instant::now();
                    record_segment_histogram_samples(labelsets, writer, series, &samples)?;
                    profile.record_samples += record_start.elapsed();
                }
                SeriesSamples::ExponentialHistogram { samples } => {
                    let labelsets = &self.labelsets;
                    let Some(writer) = &mut self.segment_writer else {
                        return Ok(());
                    };
                    let record_start = Instant::now();
                    record_segment_exponential_histogram_samples(
                        labelsets, writer, series, &samples,
                    )?;
                    profile.record_samples += record_start.elapsed();
                }
                SeriesSamples::Summary { samples } => {
                    let labelsets = &self.labelsets;
                    let Some(writer) = &mut self.segment_writer else {
                        return Ok(());
                    };
                    let record_start = Instant::now();
                    record_segment_summary_samples(labelsets, writer, series, &samples)?;
                    profile.record_samples += record_start.elapsed();
                }
            }
        }
        if let (Some(before), Some(writer)) = (record_profile_before, self.segment_writer.as_ref())
        {
            profile.record_subphases = writer.record_profile().saturating_sub(before);
        }
        if let Some(writer) = &mut self.segment_writer {
            let writer_flush_start = Instant::now();
            writer.flush()?;
            profile.writer_flush = writer_flush_start.elapsed();
        }
        profile.total = profile_start.elapsed();
        info!(
            start_ms,
            end_ms,
            datapoints,
            series = series_count,
            elapsed_ms = duration_ms_u64(profile.total),
            seal_decode_ms = duration_ms_u64(profile.seal_decode),
            series_reserve_ms = duration_ms_u64(profile.series_reserve),
            label_clone_ms = duration_ms_u64(profile.label_clone),
            int_conversion_ms = duration_ms_u64(profile.int_conversion),
            record_samples_ms = duration_ms_u64(profile.record_samples),
            record_wall_ms = duration_ms_u64(profile.record_subphases.wall_elapsed),
            record_accounted_ms = duration_ms_u64(profile.record_subphases.total_elapsed()),
            record_ensure_window_ms = duration_ms_u64(profile.record_subphases.ensure_window),
            record_metadata_ms = duration_ms_u64(profile.record_subphases.metadata),
            record_chunk_append_ms = duration_ms_u64(profile.record_subphases.chunk_append),
            record_label_time_range_ms = duration_ms_u64(profile.record_subphases.label_time_range),
            record_bookkeeping_ms = duration_ms_u64(profile.record_subphases.bookkeeping),
            record_chunks = profile.record_subphases.chunks,
            record_profile_samples = profile.record_subphases.samples,
            writer_flush_ms = duration_ms_u64(profile.writer_flush),
            dropped_histogram_series = profile.dropped_histogram_series,
            dropped_exponential_histogram_series = profile.dropped_exponential_histogram_series,
            dropped_summary_series = profile.dropped_summary_series,
            "Head window written"
        );
        self.last_head_window_write_profile = Some(profile);
        Ok(())
    }

    fn log_labelset_window(
        &self,
        ingestion: &metrics_ingestion_stats::Snapshot,
        store_stats: &LabelSetStoreStats,
        report_elapsed: Duration,
    ) {
        let seconds = report_elapsed.as_secs_f64();
        let intern_time = ingestion.window.intern_time;
        let build_time = ingestion.window.processing_time.saturating_sub(intern_time);

        let msg_rate = if seconds > 0.0 {
            ingestion.window.messages as f64 / seconds
        } else {
            0.0
        };
        let observed_dp_rate = if seconds > 0.0 {
            ingestion.window.observed_datapoints as f64 / seconds
        } else {
            0.0
        };
        let accepted_dp_rate = if seconds > 0.0 {
            ingestion.window.datapoints as f64 / seconds
        } else {
            0.0
        };

        let avg_msg_time = if ingestion.window.messages == 0 {
            Duration::from_secs(0)
        } else {
            let denom = ingestion.window.messages.min(u64::from(u32::MAX)) as u32;
            ingestion.window.processing_time / denom
        };
        let avg_dp_time = if ingestion.window.observed_datapoints == 0 {
            Duration::from_secs(0)
        } else {
            let denom = ingestion
                .window
                .observed_datapoints
                .min(u64::from(u32::MAX)) as u32;
            ingestion.window.processing_time / denom
        };

        info!(
            "LabelSets store={} messages={} (+{}, {:.2} msg/s in {:?}) observed_datapoints={} (+{}, {:.2} dp/s) accepted_datapoints={} (+{}, {:.2} dp/s) recorded_samples={} missing_number_values={} dropped_too_old={} dropped_too_future={} missing_timestamp={} series={} symbols={} keysets={} skipped_non_scalar_values={} skipped_labelset_errors={} processing_time={:?} intern_time={:?} build_time={:?} avg_msg_time={:?} avg_observed_dp_time={:?}",
            self.labelsets.kind(),
            ingestion.totals.messages,
            ingestion.window.messages,
            msg_rate,
            ingestion.window.elapsed,
            ingestion.totals.observed_datapoints,
            ingestion.window.observed_datapoints,
            observed_dp_rate,
            ingestion.totals.datapoints,
            ingestion.window.datapoints,
            accepted_dp_rate,
            ingestion.totals.datapoint_storage.recorded_samples,
            ingestion.totals.datapoint_storage.missing_number_values,
            ingestion.totals.datapoint_policy.dropped_too_old,
            ingestion.totals.datapoint_policy.dropped_too_future,
            ingestion.totals.datapoint_policy.missing_timestamp,
            store_stats.series,
            store_stats.symbols.unwrap_or(0),
            store_stats.keysets.unwrap_or(0),
            ingestion.totals.skipped_non_scalar_values,
            ingestion.totals.skipped_labelset_errors,
            ingestion.window.processing_time,
            intern_time,
            build_time,
            avg_msg_time,
            avg_dp_time,
        );
    }

    fn log_store_stats(&self, store_stats: &LabelSetStoreStats) {
        let series = store_stats.series.max(1);
        let alloc_bps = store_stats.alloc_bytes.checked_div(series).unwrap_or(0);
        let used_bps = store_stats.used_bytes.checked_div(series).unwrap_or(0);

        info!(
            "LabelSetStoreSize store={} series={} alloc_bytes={} alloc_bps={} used_bytes={} used_bps={} symbols_alloc_bytes={} symbols_used_bytes={}",
            self.labelsets.kind(),
            store_stats.series,
            store_stats.alloc_bytes,
            alloc_bps,
            store_stats.used_bytes,
            used_bps,
            store_stats.symbols_alloc_bytes,
            store_stats.symbols_used_bytes,
        );

        if let Some(buffer_stats) = &store_stats.buffer_stats {
            info!(
                "LabelSetStoreBuffers store={} {}",
                self.labelsets.kind(),
                buffer_stats
            );
        }

        if let Some(symbol_table_stats) = &store_stats.symbol_table_stats {
            info!(
                "SymbolTableStats store={} {}",
                self.labelsets.kind(),
                symbol_table_stats
            );
        }
    }

    fn log_metric_types(
        &self,
        ingestion: &metrics_ingestion_stats::Snapshot,
        report_elapsed: Duration,
    ) {
        let seconds = report_elapsed.as_secs_f64();
        let metrics_rate = if seconds > 0.0 {
            ingestion.window.metrics as f64 / seconds
        } else {
            0.0
        };

        info!(
            "OtlpMetricTypes store={} metrics={} (+{}, {:.2} metrics/s) gauge={} sum={} histogram={} exponential_histogram={} summary={}",
            self.labelsets.kind(),
            ingestion.totals.metrics,
            ingestion.window.metrics,
            metrics_rate,
            ingestion.totals.metric_types.gauge,
            ingestion.totals.metric_types.sum,
            ingestion.totals.metric_types.histogram,
            ingestion.totals.metric_types.exponential_histogram,
            ingestion.totals.metric_types.summary,
        );
    }

    fn log_unique_metrics(
        &self,
        ingestion: &metrics_ingestion_stats::Snapshot,
        report_elapsed: Duration,
    ) {
        let seconds = report_elapsed.as_secs_f64();
        let unique_rate = if seconds > 0.0 {
            ingestion.window.unique_metrics as f64 / seconds
        } else {
            0.0
        };

        info!(
            "OtlpUniqueMetrics store={} unique_metrics={} (+{}, {:.2} unique/s)",
            self.labelsets.kind(),
            ingestion.totals.unique_metrics,
            ingestion.window.unique_metrics,
            unique_rate,
        );
    }

    fn log_datapoint_types(
        &self,
        ingestion: &metrics_ingestion_stats::Snapshot,
        report_elapsed: Duration,
    ) {
        let seconds = report_elapsed.as_secs_f64();
        let observed_dp_rate = if seconds > 0.0 {
            ingestion.window.observed_datapoints as f64 / seconds
        } else {
            0.0
        };
        let accepted_dp_rate = if seconds > 0.0 {
            ingestion.window.datapoints as f64 / seconds
        } else {
            0.0
        };

        info!(
            "OtlpDatapointTypes store={} observed_datapoints={} (+{}, {:.2} dp/s) accepted_datapoints={} (+{}, {:.2} dp/s) observed_gauge={} observed_sum={} observed_histogram={} observed_exponential_histogram={} observed_summary={} accepted_gauge={} accepted_sum={} accepted_histogram={} accepted_exponential_histogram={} accepted_summary={}",
            self.labelsets.kind(),
            ingestion.totals.observed_datapoints,
            ingestion.window.observed_datapoints,
            observed_dp_rate,
            ingestion.totals.datapoints,
            ingestion.window.datapoints,
            accepted_dp_rate,
            ingestion.totals.observed_datapoint_types.gauge,
            ingestion.totals.observed_datapoint_types.sum,
            ingestion.totals.observed_datapoint_types.histogram,
            ingestion
                .totals
                .observed_datapoint_types
                .exponential_histogram,
            ingestion.totals.observed_datapoint_types.summary,
            ingestion.totals.datapoint_types.gauge,
            ingestion.totals.datapoint_types.sum,
            ingestion.totals.datapoint_types.histogram,
            ingestion.totals.datapoint_types.exponential_histogram,
            ingestion.totals.datapoint_types.summary,
        );
    }

    fn log_event_time_skew(&self, ingestion: &metrics_ingestion_stats::Snapshot) {
        let skew = &ingestion.window.event_time_skew;
        if skew.all.is_none() {
            return;
        }

        let fmt = |dist: Option<chronoxide_core::statistics::DistI64>| {
            dist.map(|d| d.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        };

        info!(
            "OtlpEventTimeSkew store={} basis=\"event_ms - captured_at_ms\" all=\"{}\" accepted=\"{}\" dropped_too_old=\"{}\" dropped_too_future=\"{}\"",
            self.labelsets.kind(),
            fmt(skew.all),
            fmt(skew.accepted),
            fmt(skew.dropped_too_old),
            fmt(skew.dropped_too_future),
        );
    }

    fn log_partition_watermarks(&self, ingestion: &metrics_ingestion_stats::Snapshot) {
        for ((topic, partition), wm) in &ingestion.partition_watermarks {
            info!(
                "PartitionWatermark {} messages with datapoints={} for topic={} partition={} min_ts={} max_ts={}, duration={}",
                wm.messages,
                wm.datapoints,
                topic,
                partition,
                wm.min_ts,
                wm.max_ts,
                wm.max_ts - wm.min_ts
            );
        }
    }

    fn report_latency_window(&self) {
        let samples = self.labelset_stats.latency_samples();
        if samples.is_empty() {
            return;
        }

        let msg_total = samples.msg_total_ns.summarize();
        let dp_total = samples.dp_total_ns.summarize();
        let dp_intern = samples.dp_intern_ns.summarize();
        let dp_build = samples.dp_build_ns.summarize();
        let dp_per_msg = samples.datapoints_per_msg.summarize();

        info!(
            "LabelSetLatency store={} msg_samples={} msg_seen={} dp_samples={} dp_seen={} msg_total={} dp_total={} dp_intern={} dp_build={} dp_per_msg={}",
            self.labelsets.kind(),
            samples.msg_sample_count(),
            samples.msg_seen,
            samples.dp_sample_count(),
            samples.dp_seen,
            msg_total
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "n/a".to_string()),
            dp_total
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "n/a".to_string()),
            dp_intern
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "n/a".to_string()),
            dp_build
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "n/a".to_string()),
            dp_per_msg
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "n/a".to_string()),
        );
    }

    fn report_head_stats_window(&self) {
        if self.partition_heads.is_empty() {
            return;
        }

        let mut partitions: Vec<_> = self.partition_heads.iter().collect();
        partitions.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (partition, state) in partitions {
            let dists = state.stats.distributions();
            if let Some(dist) = dists.call_latency {
                info!("head_call_latency partition={} {}", partition, dist);
            }
            if let Some(dist) = dists.batch_sizes {
                info!("batch_sizes partition={} {}", partition, dist);
            }
            if let Some(dist) = dists.series_sample_counts {
                info!("series_sample_counts partition={} {}", partition, dist);
            }
            if let Some(dist) = dists.blocks_per_series {
                info!("blocks_per_series partition={} {}", partition, dist);
            }
            if let Some(dist) = dists.samples_per_block {
                info!("samples_per_block partition={} {}", partition, dist);
            }

            if let Some(density) = state.stats.series_density() {
                info!(
                    "series_single_sample_count partition={} count={} ratio={:.3} series_multi_sample_count={}",
                    partition,
                    density.series_single_sample_count,
                    density.series_single_sample_ratio,
                    density.series_multi_sample_count
                );
            }
        }
    }

    fn ingest_otlp_metrics(
        &mut self,
        req: &ExportMetricsServiceRequest,
        captured_at_ms: i64,
        mut head_state: Option<&mut PartitionHead>,
        record_non_number_samples: bool,
    ) -> Result<DatapointIngestResult> {
        let mut result = DatapointIngestResult::default();

        let mut scratch_values: Vec<Box<str>> = Vec::new();
        let mut tmp_labels: Vec<TmpLabel<'_>> = Vec::new();

        for resource_metrics in &req.resource_metrics {
            let resource_attrs = resource_metrics
                .resource
                .as_ref()
                .map(|res| res.attributes.as_slice())
                .unwrap_or(&[]);

            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    let metric_name = metric.name.as_str();
                    let Some(metric_data) = metric.data.as_ref() else {
                        continue;
                    };

                    match metric_data {
                        tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            let count = ingest_number_datapoints(
                                self,
                                head_state.as_deref_mut(),
                                resource_attrs,
                                metric_name,
                                &gauge.data_points,
                                &mut scratch_values,
                                &mut tmp_labels,
                                captured_at_ms,
                            )?;
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Gauge,
                                gauge.data_points.len() as u64,
                                count.accepted,
                            );
                            result.merge(count);
                        }
                        tonic::metrics::v1::metric::Data::Sum(sum) => {
                            let count = ingest_number_datapoints(
                                self,
                                head_state.as_deref_mut(),
                                resource_attrs,
                                metric_name,
                                &sum.data_points,
                                &mut scratch_values,
                                &mut tmp_labels,
                                captured_at_ms,
                            )?;
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Sum,
                                sum.data_points.len() as u64,
                                count.accepted,
                            );
                            result.merge(count);
                        }
                        tonic::metrics::v1::metric::Data::Histogram(hist) => {
                            let mut count = DatapointIngestResult::default();
                            for dp in &hist.data_points {
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let series = intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                )?;
                                if record_non_number_samples
                                    && let Some(series) = series
                                    && let Some(head_state) = head_state.as_deref_mut()
                                {
                                    let mut value =
                                        histogram_value(dp, hist.aggregation_temporality);
                                    if let SampleValue::Histogram(histogram) = &mut value {
                                        self.stamp_histogram_reset_hint(series, histogram);
                                    }
                                    self.record_head_sample(head_state, series, ts_ms, value)?;
                                }
                            }
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Histogram,
                                hist.data_points.len() as u64,
                                count.accepted,
                            );
                            result.merge(count);
                        }
                        tonic::metrics::v1::metric::Data::ExponentialHistogram(hist) => {
                            let mut count = DatapointIngestResult::default();
                            for dp in &hist.data_points {
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let series = intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                )?;
                                if record_non_number_samples
                                    && let Some(series) = series
                                    && let Some(head_state) = head_state.as_deref_mut()
                                {
                                    let mut value = exponential_histogram_value(
                                        dp,
                                        hist.aggregation_temporality,
                                    );
                                    if let SampleValue::ExponentialHistogram(histogram) = &mut value
                                    {
                                        self.stamp_exponential_histogram_reset_hint(
                                            series, histogram,
                                        );
                                    }
                                    self.record_head_sample(head_state, series, ts_ms, value)?;
                                }
                            }
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::ExponentialHistogram,
                                hist.data_points.len() as u64,
                                count.accepted,
                            );
                            result.merge(count);
                        }
                        tonic::metrics::v1::metric::Data::Summary(summary) => {
                            let mut count = DatapointIngestResult::default();
                            for dp in &summary.data_points {
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let series = intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                )?;
                                if record_non_number_samples
                                    && let Some(series) = series
                                    && let Some(head_state) = head_state.as_deref_mut()
                                {
                                    let value = summary_value(dp);
                                    self.record_head_sample(head_state, series, ts_ms, value)?;
                                }
                            }
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Summary,
                                summary.data_points.len() as u64,
                                count.accepted,
                            );
                            result.merge(count);
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}

impl HistogramResetState {
    fn from_value(value: &HistogramValue) -> Self {
        Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            explicit_bounds: value.explicit_bounds.clone(),
            bucket_counts: value.bucket_counts.clone(),
        }
    }
}

impl ExponentialHistogramResetState {
    fn from_value(value: &ExponentialHistogramValue) -> Self {
        Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            scale: value.scale,
            zero_threshold_bits: value.zero_threshold.to_bits(),
            zero_count: value.zero_count,
            positive: value.positive.clone(),
            negative: value.negative.clone(),
        }
    }
}

fn histogram_reset_hint(
    previous: &HistogramResetState,
    current: &HistogramResetState,
) -> CounterResetHint {
    if start_time_advanced(previous.start_time_ms, current.start_time_ms) {
        return CounterResetHint::CounterReset;
    }
    if previous.explicit_bounds != current.explicit_bounds {
        return CounterResetHint::Unknown;
    }
    if current.count < previous.count || optional_f64_decreased(previous.sum, current.sum) {
        return CounterResetHint::CounterReset;
    }
    if previous.bucket_counts.len() != current.bucket_counts.len() {
        return CounterResetHint::Unknown;
    }
    if previous
        .bucket_counts
        .iter()
        .zip(&current.bucket_counts)
        .any(|(previous, current)| current < previous)
    {
        return CounterResetHint::CounterReset;
    }
    CounterResetHint::NotCounterReset
}

fn exponential_histogram_reset_hint(
    previous: &ExponentialHistogramResetState,
    current: &ExponentialHistogramResetState,
) -> CounterResetHint {
    if start_time_advanced(previous.start_time_ms, current.start_time_ms) {
        return CounterResetHint::CounterReset;
    }
    if previous.zero_threshold_bits != current.zero_threshold_bits {
        return CounterResetHint::Unknown;
    }
    if current.count < previous.count
        || current.zero_count < previous.zero_count
        || optional_f64_decreased(previous.sum, current.sum)
    {
        return CounterResetHint::CounterReset;
    }

    let target_scale = previous.scale.min(current.scale);
    let Ok(previous_positive) = downscale_exponential_histogram_buckets_to_map(
        &previous.positive,
        previous.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(current_positive) = downscale_exponential_histogram_buckets_to_map(
        &current.positive,
        current.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(previous_negative) = downscale_exponential_histogram_buckets_to_map(
        &previous.negative,
        previous.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(current_negative) = downscale_exponential_histogram_buckets_to_map(
        &current.negative,
        current.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };

    if bucket_map_decreased(&previous_positive, &current_positive)
        || bucket_map_decreased(&previous_negative, &current_negative)
    {
        CounterResetHint::CounterReset
    } else {
        CounterResetHint::NotCounterReset
    }
}

fn start_time_advanced(previous: Option<u64>, current: Option<u64>) -> bool {
    matches!((previous, current), (Some(previous), Some(current)) if current > previous)
}

fn optional_f64_decreased(previous: Option<f64>, current: Option<f64>) -> bool {
    matches!((previous, current), (Some(previous), Some(current)) if current < previous)
}

fn bucket_map_decreased(previous: &BTreeMap<i32, u64>, current: &BTreeMap<i32, u64>) -> bool {
    previous
        .iter()
        .any(|(index, previous_count)| current.get(index).copied().unwrap_or(0) < *previous_count)
}

fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn first_series_samples_timestamp(series_samples: &[(SeriesRef, SeriesSamples)]) -> Option<u64> {
    series_samples
        .iter()
        .find_map(|(_, samples)| series_samples_first_timestamp(samples))
}

fn series_samples_first_timestamp(samples: &SeriesSamples) -> Option<u64> {
    match samples {
        SeriesSamples::Float { samples, .. } => samples.first().map(|(ts, _)| *ts),
        SeriesSamples::Int64 { samples, .. } => samples.first().map(|(ts, _)| *ts),
        SeriesSamples::Histogram { samples } => samples.first().map(|(ts, _)| *ts),
        SeriesSamples::ExponentialHistogram { samples } => samples.first().map(|(ts, _)| *ts),
        SeriesSamples::Summary { samples } => samples.first().map(|(ts, _)| *ts),
    }
}

fn record_segment_float_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, f64)],
    raw: bool,
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        if raw {
            writer.record_samples_raw_ordered_with_flat_interned_labels(series, samples, flat)?;
        } else {
            writer.record_samples_ordered_with_flat_interned_labels(series, samples, flat)?;
        }
        return Ok(());
    }

    if raw {
        writer.record_samples_raw_ordered_with_label_visitor(series, samples, |visit| {
            labelsets.visit_labelset(series, |key, value| visit(key, value));
        })?;
    } else {
        writer.record_samples_ordered_with_label_visitor(series, samples, |visit| {
            labelsets.visit_labelset(series, |key, value| visit(key, value));
        })?;
    }
    Ok(())
}

fn record_segment_histogram_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, HistogramValue)],
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        writer.record_histogram_samples_ordered_with_flat_interned_labels(series, samples, flat)?;
        return Ok(());
    }

    writer.record_histogram_samples_ordered_with_label_visitor(series, samples, |visit| {
        labelsets.visit_labelset(series, |key, value| visit(key, value));
    })?;
    Ok(())
}

fn record_segment_exponential_histogram_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, ExponentialHistogramValue)],
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        writer.record_exponential_histogram_samples_ordered_with_flat_interned_labels(
            series, samples, flat,
        )?;
        return Ok(());
    }

    writer.record_exponential_histogram_samples_ordered_with_label_visitor(
        series,
        samples,
        |visit| {
            labelsets.visit_labelset(series, |key, value| visit(key, value));
        },
    )?;
    Ok(())
}

fn record_segment_summary_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, SummaryValue)],
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        writer.record_summary_samples_ordered_with_flat_interned_labels(series, samples, flat)?;
        return Ok(());
    }

    writer.record_summary_samples_ordered_with_label_visitor(series, samples, |visit| {
        labelsets.visit_labelset(series, |key, value| visit(key, value));
    })?;
    Ok(())
}

fn data_type_counts_markdown(
    metric_types: &OtlpDataTypeCounts,
    observed_datapoint_types: &OtlpDataTypeCounts,
    accepted_datapoint_types: &OtlpDataTypeCounts,
) -> String {
    let mut md = String::new();
    md.push_str("## OTLP Data Type Counts\n\n");
    md.push_str("| Type | Metric Records | Observed Datapoints | Accepted Datapoints |\n");
    md.push_str("|---|---:|---:|---:|\n");
    for (label, metric_records, observed_datapoints, accepted_datapoints) in [
        (
            "Gauge",
            metric_types.gauge,
            observed_datapoint_types.gauge,
            accepted_datapoint_types.gauge,
        ),
        (
            "Sum",
            metric_types.sum,
            observed_datapoint_types.sum,
            accepted_datapoint_types.sum,
        ),
        (
            "Histogram",
            metric_types.histogram,
            observed_datapoint_types.histogram,
            accepted_datapoint_types.histogram,
        ),
        (
            "Exponential Histogram",
            metric_types.exponential_histogram,
            observed_datapoint_types.exponential_histogram,
            accepted_datapoint_types.exponential_histogram,
        ),
        (
            "Summary",
            metric_types.summary,
            observed_datapoint_types.summary,
            accepted_datapoint_types.summary,
        ),
    ] {
        md.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            label, metric_records, observed_datapoints, accepted_datapoints
        ));
    }
    md.push('\n');
    md
}

fn datapoint_policy_counts_markdown(
    totals: &DatapointPolicyCounts,
    window: &DatapointPolicyCounts,
) -> String {
    let mut md = String::new();
    md.push_str("## Datapoint Policy Counts\n\n");
    md.push_str("| Outcome | Total | Window |\n");
    md.push_str("|---|---:|---:|\n");
    for (label, total, window) in [
        (
            "Observed",
            totals.accepted.saturating_add(totals.rejected()),
            window.accepted.saturating_add(window.rejected()),
        ),
        ("Time-Policy Accepted", totals.accepted, window.accepted),
        (
            "Dropped Too Old",
            totals.dropped_too_old,
            window.dropped_too_old,
        ),
        (
            "Dropped Too Future",
            totals.dropped_too_future,
            window.dropped_too_future,
        ),
        (
            "Missing Timestamp",
            totals.missing_timestamp,
            window.missing_timestamp,
        ),
        ("Rejected Total", totals.rejected(), window.rejected()),
    ] {
        md.push_str(&format!("| {} | {} | {} |\n", label, total, window));
    }
    md.push('\n');
    md
}

fn datapoint_storage_counts_markdown(
    totals: &DatapointStorageCounts,
    window: &DatapointStorageCounts,
    policy_totals: &DatapointPolicyCounts,
    policy_window: &DatapointPolicyCounts,
) -> String {
    let mut md = String::new();
    md.push_str("## Datapoint Storage Counts\n\n");
    md.push_str("Recorded samples are datapoints successfully accepted by the head storage path. Missing number values are time-accepted Gauge/Sum datapoints without an OTLP numeric value.\n\n");
    md.push_str("| Outcome | Total | Window |\n|---|---:|---:|\n");
    for (label, total, window) in [
        (
            "Time-Policy Accepted",
            policy_totals.accepted,
            policy_window.accepted,
        ),
        (
            "Recorded Samples",
            totals.recorded_samples,
            window.recorded_samples,
        ),
        (
            "Missing Number Value",
            totals.missing_number_values,
            window.missing_number_values,
        ),
        (
            "Accepted Not Recorded",
            policy_totals
                .accepted
                .saturating_sub(totals.recorded_samples),
            policy_window
                .accepted
                .saturating_sub(window.recorded_samples),
        ),
    ] {
        md.push_str(&format!("| {} | {} | {} |\n", label, total, window));
    }
    md.push('\n');
    md
}

fn event_time_skew_markdown(skew: &EventTimeSkewSnapshot) -> String {
    if skew.all.is_none() {
        return String::new();
    }

    let mut md = String::new();
    md.push_str("## Event Time Skew\n\n");
    md.push_str(
        "Signed milliseconds between OTLP datapoint event time and capture time (`event_ms - captured_at_ms`). Negative values mean event time was before capture.\n\n",
    );
    md.push_str("| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |\n");
    md.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
    if let Some(dist) = skew.all {
        md.push_str(&dist.to_markdown_row("All Timestamped"));
    }
    if let Some(dist) = skew.accepted {
        md.push_str(&dist.to_markdown_row("Accepted"));
    }
    if let Some(dist) = skew.dropped_too_old {
        md.push_str(&dist.to_markdown_row("Dropped Too Old"));
    }
    if let Some(dist) = skew.dropped_too_future {
        md.push_str(&dist.to_markdown_row("Dropped Too Future"));
    }
    md.push('\n');
    md
}

fn ingest_number_datapoints<'a>(
    processor: &mut OtlpLabelSetProcessor,
    mut head_state: Option<&mut PartitionHead>,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [tonic::metrics::v1::NumberDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    captured_at_ms: i64,
) -> Result<DatapointIngestResult> {
    let mut result = DatapointIngestResult::default();
    for dp in points {
        let decision = processor.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
        let Some(ts_ms) = result.record(decision) else {
            continue;
        };
        let value = number_value(dp);
        if value.is_none() {
            processor.labelset_stats.record_missing_number_values(1);
        }
        let series = intern_labelset(
            &mut processor.labelsets,
            &mut processor.labelset_stats,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        )?;
        if let (Some(series), Some(value)) = (series, value) {
            if let Some(head_state) = head_state.as_deref_mut() {
                processor.record_head_sample(head_state, series, ts_ms, value)?;
            }
        }
    }
    Ok(result)
}

struct ProcessorLabelSetInterner<'a> {
    labelsets: &'a mut LabelSetInterner,
    stats: &'a mut OtlpMetricsIngestionStats,
}

impl<'a> OtlpLabelSetInterner for ProcessorLabelSetInterner<'a> {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {
        self.stats.record_skipped_non_scalar_value();
    }

    fn on_intern_error(&mut self, error: Self::Error) {
        self.stats.record_labelset_error();
        if should_log(Level::ERROR, "LabelSetStoreInternError", Instant::now()) {
            error!("LabelSetStore intern failed: {}", error);
        }
    }

    fn intern(
        &mut self,
        labels: &[KeyValueRef<'_>],
    ) -> std::result::Result<SeriesRef, Self::Error> {
        self.labelsets.intern(labels, self.stats)
    }
}

fn intern_labelset<'a>(
    labelsets: &mut LabelSetInterner,
    stats: &mut OtlpMetricsIngestionStats,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [tonic::common::v1::KeyValue],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) -> Result<Option<SeriesRef>> {
    let mut interner = ProcessorLabelSetInterner { labelsets, stats };
    Ok(intern_otlp_labelset(
        &mut interner,
        resource_attrs,
        metric_name,
        datapoint_attrs,
        scratch_values,
        tmp_labels,
    ))
}

#[derive(Default)]
struct LabelSetStoreStats {
    series: usize,
    symbols: Option<usize>,
    keysets: Option<usize>,
    alloc_bytes: usize,
    used_bytes: usize,
    symbols_alloc_bytes: usize,
    symbols_used_bytes: usize,
    buffer_stats: Option<String>,
    symbol_table_stats: Option<String>,
}

enum LabelSetInterner {
    Naive(NaiveLabelSetStore),
    FlatInterned(InternedStore),
    KeySetDictEncoded(KeysetStore),
}

impl LabelSetInterner {
    fn new(kind: LabelSetStoreKind) -> Self {
        match kind {
            LabelSetStoreKind::FlatInterned => Self::FlatInterned(InternedStore::default()),
            LabelSetStoreKind::KeySetDictEncoded => Self::KeySetDictEncoded(KeysetStore::default()),
            LabelSetStoreKind::Naive => Self::Naive(NaiveLabelSetStore::default()),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::FlatInterned(_) => "FlatInterned",
            Self::KeySetDictEncoded(_) => "KeySetDictEncoded",
            Self::Naive(_) => "Naive",
        }
    }

    fn as_flat_interned(&self) -> Option<&InternedStore> {
        match self {
            Self::FlatInterned(store) => Some(store),
            Self::Naive(_) | Self::KeySetDictEncoded(_) => None,
        }
    }

    fn intern(
        &mut self,
        labels: &[KeyValueRef<'_>],
        stats: &mut OtlpMetricsIngestionStats,
    ) -> std::result::Result<SeriesRef, LabelSetStoreError> {
        match self {
            Self::Naive(store) => {
                let start = Instant::now();
                let series = store.intern(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::Naive, elapsed);
                Ok(series)
            }
            Self::FlatInterned(store) => {
                let start = Instant::now();
                let series = store.intern(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::FlatInterned, elapsed);
                Ok(series)
            }
            Self::KeySetDictEncoded(store) => {
                let start = Instant::now();
                let series = store.intern(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::KeySetDictEncoded, elapsed);
                Ok(series)
            }
        }
    }

    #[cfg(test)]
    fn segment_metadata(&self, series: SeriesRef) -> SegmentSeriesMetadata {
        let mut builder = SegmentSeriesMetadataBuilder::new();
        match self {
            Self::Naive(store) => {
                store.visit_labelset(series, |key, value| {
                    builder.push_label(key, value);
                });
            }
            Self::FlatInterned(store) => {
                store.visit_labelset(series, |key, value| {
                    builder.push_label(key, value);
                });
            }
            Self::KeySetDictEncoded(store) => {
                store.visit_labelset(series, |key, value| {
                    builder.push_label(key, value);
                });
            }
        }
        builder.finish()
    }

    fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        match self {
            Self::Naive(store) => {
                store.visit_labelset(series, |key, value| visitor(key, value));
            }
            Self::FlatInterned(store) => {
                store.visit_labelset(series, |key, value| visitor(key, value));
            }
            Self::KeySetDictEncoded(store) => {
                store.visit_labelset(series, |key, value| visitor(key, value));
            }
        }
    }

    fn stats(&self) -> LabelSetStoreStats {
        match self {
            Self::Naive(store) => LabelSetStoreStats {
                series: store.len(),
                symbols: None,
                keysets: None,
                alloc_bytes: store.estimate_size_bytes(),
                used_bytes: store.estimate_used_bytes(),
                symbols_alloc_bytes: 0,
                symbols_used_bytes: 0,
                buffer_stats: Some(store.buffer_stats().to_string()),
                symbol_table_stats: None,
            },
            Self::FlatInterned(store) => {
                let symbols = store.symbols();
                LabelSetStoreStats {
                    series: store.len(),
                    symbols: Some(symbols.len()),
                    keysets: None,
                    alloc_bytes: store.estimate_size_bytes(),
                    used_bytes: store.estimate_used_bytes(),
                    symbols_alloc_bytes: symbols.estimate_allocated_bytes(),
                    symbols_used_bytes: symbols.estimate_used_bytes(),
                    buffer_stats: Some(store.buffer_stats().to_string()),
                    symbol_table_stats: Some(symbols.stats().to_string()),
                }
            }
            Self::KeySetDictEncoded(store) => {
                let symbols = store.symbols();
                LabelSetStoreStats {
                    series: store.len(),
                    symbols: Some(symbols.len()),
                    keysets: Some(store.keysets().len()),
                    alloc_bytes: store.estimate_size_bytes(),
                    used_bytes: store.estimate_used_bytes(),
                    symbols_alloc_bytes: symbols.estimate_allocated_bytes(),
                    symbols_used_bytes: symbols.estimate_used_bytes(),
                    buffer_stats: Some(store.buffer_stats().to_string()),
                    symbol_table_stats: Some(symbols.stats().to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
