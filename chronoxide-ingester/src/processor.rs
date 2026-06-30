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
    FloatEncoding, HeadBuffer, HeadConfig, HeadWindow, IntEncoding, SampleValue, SeriesSamples,
};
use chronoxide_core::storage::segment::{
    SegmentSeriesMetadata, SegmentSeriesMetadataBuilder, SegmentWriter,
};
use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{Level, error, info, warn};

mod head_stats;
mod metrics_ingestion_stats;

use self::head_stats::HeadBufferStats;
use self::metrics_ingestion_stats::{
    DatapointPolicyCounts, EventTimeSkewOutcome, EventTimeSkewSnapshot, MetricDataType,
    OtlpDataTypeCounts, OtlpMetricsIngestionStats,
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
    max_event_age_ms: i64,
    max_event_lead_ms: i64,
    drop_outdated: bool,
}

impl EventTimePolicy {
    pub fn new(max_event_age: TimeDelta, max_event_lead: TimeDelta, drop_outdated: bool) -> Self {
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
    pub label_clone: Duration,
    pub int_conversion: Duration,
    pub record_samples: Duration,
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
    segment_writer: Option<SegmentWriter>,
    last_head_window_write_profile: Option<HeadWindowWriteProfile>,
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
            segment_writer,
            last_head_window_write_profile: None,
        }
    }

    pub fn with_event_time_policy(mut self, policy: EventTimePolicy) -> Self {
        self.event_time_policy = policy;
        self
    }

    pub fn last_head_window_write_profile(&self) -> Option<&HeadWindowWriteProfile> {
        self.last_head_window_write_profile.as_ref()
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
            "| Total Datapoints | {} |\n",
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
        md.push('\n');

        md.push_str(&datapoint_policy_counts_markdown(
            &ingestion.totals.datapoint_policy,
            &ingestion.window.datapoint_policy,
        ));
        md.push_str(&event_time_skew_markdown(&ingestion.totals.event_time_skew));
        let general_stats_time = general_stats_start.elapsed();

        let data_type_counts_start = Instant::now();
        md.push_str(&data_type_counts_markdown(
            &ingestion.totals.metric_types,
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
        self.write_markdown_report();
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
        let record_non_number_samples = self.segment_writer.is_none();
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
        self.labelset_stats
            .finish_message(scope, elapsed, datapoints.accepted);
        self.labelset_stats.record_partition_watermark(
            metadata.topic,
            metadata.partition,
            metadata.timestamp_ms,
            datapoints.accepted,
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
                        writer.record_samples_ordered_with_label_visitor(
                            series,
                            &samples,
                            |visit| {
                                labelsets.visit_labelset(series, |key, value| visit(key, value));
                            },
                        )?;
                        profile.record_samples += record_start.elapsed();
                    }
                    FloatEncoding::Raw => {
                        let labelsets = &self.labelsets;
                        let Some(writer) = &mut self.segment_writer else {
                            return Ok(());
                        };
                        let record_start = Instant::now();
                        writer.record_samples_raw_ordered_with_label_visitor(
                            series,
                            &samples,
                            |visit| {
                                labelsets.visit_labelset(series, |key, value| visit(key, value));
                            },
                        )?;
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
                    writer.record_samples_ordered_with_label_visitor(
                        series,
                        &float_samples,
                        |visit| {
                            labelsets.visit_labelset(series, |key, value| visit(key, value));
                        },
                    )?;
                    profile.record_samples += record_start.elapsed();
                }
                SeriesSamples::Histogram { .. } => {
                    profile.dropped_histogram_series =
                        profile.dropped_histogram_series.saturating_add(1);
                    warn!(
                        "SegmentWriter does not support histogram samples yet; dropping series={}",
                        series.get()
                    );
                }
                SeriesSamples::ExponentialHistogram { .. } => {
                    profile.dropped_exponential_histogram_series = profile
                        .dropped_exponential_histogram_series
                        .saturating_add(1);
                    warn!(
                        "SegmentWriter does not support exponential histogram samples yet; dropping series={}",
                        series.get()
                    );
                }
                SeriesSamples::Summary { .. } => {
                    profile.dropped_summary_series =
                        profile.dropped_summary_series.saturating_add(1);
                    warn!(
                        "SegmentWriter does not support summary samples yet; dropping series={}",
                        series.get()
                    );
                }
            }
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
            label_clone_ms = duration_ms_u64(profile.label_clone),
            int_conversion_ms = duration_ms_u64(profile.int_conversion),
            record_samples_ms = duration_ms_u64(profile.record_samples),
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
        let dp_rate = if seconds > 0.0 {
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
        let avg_dp_time = if ingestion.window.datapoints == 0 {
            Duration::from_secs(0)
        } else {
            let denom = ingestion.window.datapoints.min(u64::from(u32::MAX)) as u32;
            ingestion.window.processing_time / denom
        };

        info!(
            "LabelSets store={} messages={} (+{}, {:.2} msg/s in {:?}) datapoints={} (+{}, {:.2} dp/s) accepted_datapoints={} dropped_too_old={} dropped_too_future={} missing_timestamp={} series={} symbols={} keysets={} skipped_non_scalar_values={} skipped_labelset_errors={} processing_time={:?} intern_time={:?} build_time={:?} avg_msg_time={:?} avg_dp_time={:?}",
            self.labelsets.kind(),
            ingestion.totals.messages,
            ingestion.window.messages,
            msg_rate,
            ingestion.window.elapsed,
            ingestion.totals.datapoints,
            ingestion.window.datapoints,
            dp_rate,
            ingestion.totals.datapoint_policy.accepted,
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
        let dp_rate = if seconds > 0.0 {
            ingestion.window.datapoints as f64 / seconds
        } else {
            0.0
        };

        info!(
            "OtlpDatapointTypes store={} datapoints={} (+{}, {:.2} dp/s) gauge={} sum={} histogram={} exponential_histogram={} summary={}",
            self.labelsets.kind(),
            ingestion.totals.datapoints,
            ingestion.window.datapoints,
            dp_rate,
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
                                    let value = histogram_value(dp);
                                    self.record_head_sample(head_state, series, ts_ms, value)?;
                                }
                            }
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Histogram,
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
                                    let value = exponential_histogram_value(dp);
                                    self.record_head_sample(head_state, series, ts_ms, value)?;
                                }
                            }
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::ExponentialHistogram,
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

fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn data_type_counts_markdown(
    metric_types: &OtlpDataTypeCounts,
    datapoint_types: &OtlpDataTypeCounts,
) -> String {
    let mut md = String::new();
    md.push_str("## OTLP Data Type Counts\n\n");
    md.push_str("| Type | Metric Records | Datapoints |\n");
    md.push_str("|---|---:|---:|\n");
    for (label, metric_records, datapoints) in [
        ("Gauge", metric_types.gauge, datapoint_types.gauge),
        ("Sum", metric_types.sum, datapoint_types.sum),
        (
            "Histogram",
            metric_types.histogram,
            datapoint_types.histogram,
        ),
        (
            "Exponential Histogram",
            metric_types.exponential_histogram,
            datapoint_types.exponential_histogram,
        ),
        ("Summary", metric_types.summary, datapoint_types.summary),
    ] {
        md.push_str(&format!(
            "| {} | {} | {} |\n",
            label, metric_records, datapoints
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
        ("Accepted", totals.accepted, window.accepted),
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
mod tests {
    use super::*;
    use crate::app_config::LabelSetStoreKind;
    use crate::source::SourceMessageMetadata;
    use chronoxide_core::labels::METRIC_NAME_LABEL;
    use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
    use chronoxide_core::storage::head::HeadConfig;
    use chronoxide_core::storage::index::read_segment_indexes;
    use chronoxide_core::storage::segment::{
        SegmentFile, SegmentReader, SegmentStoreReader, SegmentWriterConfig,
    };
    use chronoxide_core::storage::series::{read_series_bin, read_symbols_bin};
    use std::fs::{self, File};

    fn kv_any(
        key: &str,
        value: tonic::common::v1::any_value::Value,
    ) -> tonic::common::v1::KeyValue {
        tonic::common::v1::KeyValue {
            key: key.to_string(),
            value: Some(tonic::common::v1::AnyValue { value: Some(value) }),
        }
    }

    fn kv_str(key: &str, value: &str) -> tonic::common::v1::KeyValue {
        kv_any(
            key,
            tonic::common::v1::any_value::Value::StringValue(value.to_string()),
        )
    }

    fn kv_bool(key: &str, value: bool) -> tonic::common::v1::KeyValue {
        kv_any(key, tonic::common::v1::any_value::Value::BoolValue(value))
    }

    fn kv_int(key: &str, value: i64) -> tonic::common::v1::KeyValue {
        kv_any(key, tonic::common::v1::any_value::Value::IntValue(value))
    }

    fn kv_double(key: &str, value: f64) -> tonic::common::v1::KeyValue {
        kv_any(key, tonic::common::v1::any_value::Value::DoubleValue(value))
    }

    fn kv_bytes(key: &str, value: &[u8]) -> tonic::common::v1::KeyValue {
        kv_any(
            key,
            tonic::common::v1::any_value::Value::BytesValue(value.to_vec()),
        )
    }

    fn kv_array(key: &str) -> tonic::common::v1::KeyValue {
        kv_any(
            key,
            tonic::common::v1::any_value::Value::ArrayValue(tonic::common::v1::ArrayValue {
                values: vec![],
            }),
        )
    }

    fn kv_kvlist(key: &str) -> tonic::common::v1::KeyValue {
        kv_any(
            key,
            tonic::common::v1::any_value::Value::KvlistValue(tonic::common::v1::KeyValueList {
                values: vec![],
            }),
        )
    }

    fn number_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::NumberDataPoint {
        tonic::metrics::v1::NumberDataPoint {
            attributes: attrs,
            time_unix_nano: 2_000_000_000,
            ..Default::default()
        }
    }

    fn histogram_dp(
        attrs: Vec<tonic::common::v1::KeyValue>,
    ) -> tonic::metrics::v1::HistogramDataPoint {
        tonic::metrics::v1::HistogramDataPoint {
            attributes: attrs,
            time_unix_nano: 2_000_000_000,
            ..Default::default()
        }
    }

    fn exp_histogram_dp(
        attrs: Vec<tonic::common::v1::KeyValue>,
    ) -> tonic::metrics::v1::ExponentialHistogramDataPoint {
        tonic::metrics::v1::ExponentialHistogramDataPoint {
            attributes: attrs,
            time_unix_nano: 2_000_000_000,
            ..Default::default()
        }
    }

    fn summary_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::SummaryDataPoint {
        tonic::metrics::v1::SummaryDataPoint {
            attributes: attrs,
            time_unix_nano: 2_000_000_000,
            ..Default::default()
        }
    }

    fn metric_gauge(
        name: &str,
        dps: Vec<tonic::metrics::v1::NumberDataPoint>,
    ) -> tonic::metrics::v1::Metric {
        tonic::metrics::v1::Metric {
            name: name.to_string(),
            data: Some(tonic::metrics::v1::metric::Data::Gauge(
                tonic::metrics::v1::Gauge {
                    data_points: dps,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn metric_sum(
        name: &str,
        dps: Vec<tonic::metrics::v1::NumberDataPoint>,
    ) -> tonic::metrics::v1::Metric {
        tonic::metrics::v1::Metric {
            name: name.to_string(),
            data: Some(tonic::metrics::v1::metric::Data::Sum(
                tonic::metrics::v1::Sum {
                    data_points: dps,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn metric_histogram(
        name: &str,
        dps: Vec<tonic::metrics::v1::HistogramDataPoint>,
    ) -> tonic::metrics::v1::Metric {
        tonic::metrics::v1::Metric {
            name: name.to_string(),
            data: Some(tonic::metrics::v1::metric::Data::Histogram(
                tonic::metrics::v1::Histogram {
                    data_points: dps,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn metric_exp_histogram(
        name: &str,
        dps: Vec<tonic::metrics::v1::ExponentialHistogramDataPoint>,
    ) -> tonic::metrics::v1::Metric {
        tonic::metrics::v1::Metric {
            name: name.to_string(),
            data: Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(
                tonic::metrics::v1::ExponentialHistogram {
                    data_points: dps,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn metric_summary(
        name: &str,
        dps: Vec<tonic::metrics::v1::SummaryDataPoint>,
    ) -> tonic::metrics::v1::Metric {
        tonic::metrics::v1::Metric {
            name: name.to_string(),
            data: Some(tonic::metrics::v1::metric::Data::Summary(
                tonic::metrics::v1::Summary {
                    data_points: dps,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn request(
        resource_attrs: Vec<tonic::common::v1::KeyValue>,
        metrics: Vec<tonic::metrics::v1::Metric>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![tonic::metrics::v1::ResourceMetrics {
                resource: Some(tonic::resource::v1::Resource {
                    attributes: resource_attrs,
                    ..Default::default()
                }),
                scope_metrics: vec![tonic::metrics::v1::ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn segment_dir_count(segments_dir: &std::path::Path) -> usize {
        fs::read_dir(segments_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .count()
    }

    fn collect_labelset(
        processor: &OtlpLabelSetProcessor,
        series: SeriesRef,
    ) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        match &processor.labelsets {
            LabelSetInterner::Naive(store) => {
                store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
            }
            LabelSetInterner::FlatInterned(store) => {
                store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
            }
            LabelSetInterner::KeySetDictEncoded(store) => {
                store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
            }
        }
        out.sort();
        out
    }

    #[test]
    fn labelset_interner_builds_segment_metadata_for_all_store_kinds() {
        let labels = [
            KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
            KeyValueRef::from(("namespace", "default")),
            KeyValueRef::from(("pod.name", "backend-1")),
        ];
        let mut expected = SegmentSeriesMetadataBuilder::new();
        for label in &labels {
            expected.push_label(label.key, label.value);
        }
        let expected = expected.finish();

        for store in [
            LabelSetStoreKind::Naive,
            LabelSetStoreKind::FlatInterned,
            LabelSetStoreKind::KeySetDictEncoded,
        ] {
            let mut stats = OtlpMetricsIngestionStats::new();
            let mut interner = LabelSetInterner::new(store);
            let series = interner.intern(&labels, &mut stats).unwrap();

            let metadata = interner.segment_metadata(series);

            assert_eq!(metadata.series_id(), expected.series_id());
            assert_eq!(metadata.labels(), expected.labels());
        }
    }

    #[test]
    fn format_window_ms_formats_positive_and_negative() {
        assert_eq!(format_window_ms(0), "00:00:00.000");
        assert_eq!(format_window_ms(3_661_001), "01:01:01.001");
        assert_eq!(format_window_ms(-1), "-00:00:00.001");
    }

    #[test]
    fn processor_drops_old_and_future_datapoints_using_captured_at_ms() {
        let tempdir = tempfile::tempdir().unwrap();
        let writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();
        let head = Some(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ));
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            head,
            Some(writer),
        )
        .with_event_time_policy(EventTimePolicy::new(
            chrono::TimeDelta::seconds(10),
            chrono::TimeDelta::seconds(5),
            true,
        ));

        let mut accepted = number_dp(vec![kv_str("pod.name", "accepted")]);
        accepted.time_unix_nano = 95_000_000_000;
        accepted.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
        let mut too_old = number_dp(vec![kv_str("pod.name", "old")]);
        too_old.time_unix_nano = 89_999_000_000;
        too_old.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
        let mut too_future = number_dp(vec![kv_str("pod.name", "future")]);
        too_future.time_unix_nano = 105_001_000_000;
        too_future.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(3.0));

        let result = processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 1_000,
                    captured_at_ms: 100_000,
                },
                request(
                    vec![],
                    vec![metric_gauge(
                        "cpu.usage",
                        vec![accepted, too_old, too_future],
                    )],
                ),
            )
            .unwrap();

        assert_eq!(result, ProcessResult::Ok);
        let snap = processor.labelset_stats.snapshot();
        assert_eq!(snap.totals.datapoints, 1);
        assert_eq!(snap.totals.datapoint_policy.accepted, 1);
        assert_eq!(snap.totals.datapoint_policy.dropped_too_old, 1);
        assert_eq!(snap.totals.datapoint_policy.dropped_too_future, 1);
        assert_eq!(snap.totals.datapoint_policy.missing_timestamp, 0);
        let skew = snap.totals.event_time_skew;
        let all_skew = skew.all.unwrap();
        assert_eq!(all_skew.count, 3);
        assert_eq!(all_skew.min, -10_001);
        assert_eq!(all_skew.max, 5_001);
        assert_eq!(skew.accepted.unwrap().min, -5_000);
        assert_eq!(skew.accepted.unwrap().max, -5_000);
        assert_eq!(skew.dropped_too_old.unwrap().min, -10_001);
        assert_eq!(skew.dropped_too_future.unwrap().max, 5_001);
        assert_eq!(processor.labelsets.stats().series, 1);

        processor.flush_head().unwrap();
        let store = SegmentStoreReader::open(tempdir.path()).unwrap();
        let metric = normalize_metric_name("cpu.usage");
        let pod_label = normalize_label_name("pod.name");
        let results = store
            .query_exact(
                &[
                    (METRIC_NAME_LABEL, metric.as_str()),
                    (pod_label.as_str(), "accepted"),
                ],
                0,
                200_000,
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].samples, vec![(95_000, 1.0)]);
        assert_eq!(segment_dir_count(tempdir.path()), 1);
    }

    #[test]
    fn processor_rejects_missing_otlp_timestamp_instead_of_using_kafka_timestamp() {
        let tempdir = tempfile::tempdir().unwrap();
        let writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();
        let head = Some(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ));
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            head,
            Some(writer),
        )
        .with_event_time_policy(EventTimePolicy::new(
            chrono::TimeDelta::seconds(10),
            chrono::TimeDelta::seconds(5),
            true,
        ));

        let mut missing = number_dp(vec![kv_str("pod.name", "missing")]);
        missing.time_unix_nano = 0;
        missing.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));

        let result = processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 95_000,
                    captured_at_ms: 100_000,
                },
                request(vec![], vec![metric_gauge("cpu.usage", vec![missing])]),
            )
            .unwrap();

        assert_eq!(result, ProcessResult::DroppedOutdated);
        let snap = processor.labelset_stats.snapshot();
        assert_eq!(snap.totals.datapoints, 0);
        assert_eq!(snap.totals.datapoint_policy.accepted, 0);
        assert_eq!(snap.totals.datapoint_policy.missing_timestamp, 1);
        assert_eq!(processor.labelsets.stats().series, 0);

        processor.flush_head().unwrap();
        assert_eq!(segment_dir_count(tempdir.path()), 0);
    }

    #[test]
    fn processor_canonicalizes_labels_and_skips_non_scalar_values() {
        for store in [
            LabelSetStoreKind::FlatInterned,
            LabelSetStoreKind::KeySetDictEncoded,
        ] {
            let mut processor =
                OtlpLabelSetProcessor::new(store, Duration::from_secs(3600), None, None);

            let resource_attrs = vec![
                kv_str("cluster", "prod"),
                kv_str("resource_only", "r1"),
                kv_int("int_value", 42),
            ];
            let dp_attrs = vec![
                kv_str("cluster", "staging"), // overrides resource
                kv_str("pod", "backend-123"),
                kv_str(chronoxide_core::labels::METRIC_NAME_LABEL, "ignored"),
                kv_bool("bool_value", true),
                kv_double("double_value", 3.14),
                kv_bytes("bytes_value", b"abc"),
                kv_array("array_value"),
                kv_kvlist("kvlist_value"),
                kv_str("", "ignored_empty_key"),
            ];

            let req = request(
                resource_attrs,
                vec![metric_gauge("cpu_usage", vec![number_dp(dp_attrs)])],
            );

            processor
                .process(
                    SourceMessageMetadata {
                        topic: "t".to_string(),
                        partition: 0,
                        offset: 0,
                        timestamp_ms: 1_000,
                        captured_at_ms: 10_000,
                    },
                    req,
                )
                .unwrap();

            let store_stats = processor.labelsets.stats();
            assert_eq!(store_stats.series, 1);

            let labels = collect_labelset(&processor, SeriesRef::new(0));
            let mut expected = vec![
                ("__name__".to_string(), "cpu_usage".to_string()),
                ("bool_value".to_string(), "true".to_string()),
                ("cluster".to_string(), "staging".to_string()),
                ("double_value".to_string(), "3.14".to_string()),
                ("int_value".to_string(), "42".to_string()),
                ("pod".to_string(), "backend-123".to_string()),
                ("resource_only".to_string(), "r1".to_string()),
            ];
            expected.sort();
            assert_eq!(labels, expected);

            let snap = processor.labelset_stats.snapshot();
            assert_eq!(snap.totals.skipped_non_scalar_values, 3);
        }
    }

    #[test]
    fn processor_counts_metric_and_datapoint_types_and_dedups_series() {
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            None,
            None,
        );

        let same_attrs = vec![kv_str("pod", "same")];
        let req = request(
            vec![],
            vec![
                metric_gauge(
                    "m_gauge",
                    vec![number_dp(same_attrs.clone()), number_dp(same_attrs)],
                ),
                metric_sum("m_sum", vec![number_dp(vec![kv_str("pod", "sum")])]),
                metric_histogram("m_hist", vec![histogram_dp(vec![kv_str("pod", "hist")])]),
                metric_exp_histogram(
                    "m_exphist",
                    vec![exp_histogram_dp(vec![kv_str("pod", "exphist")])],
                ),
                metric_summary(
                    "m_summary",
                    vec![summary_dp(vec![kv_str("pod", "summary")])],
                ),
            ],
        );

        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 1,
                    offset: 123,
                    timestamp_ms: 2_000,
                    captured_at_ms: 10_001,
                },
                req,
            )
            .unwrap();

        let snap = processor.labelset_stats.snapshot();
        assert_eq!(snap.totals.messages, 1);
        assert_eq!(snap.totals.metrics, 5);
        assert_eq!(snap.totals.unique_metrics, 5);
        assert_eq!(snap.totals.datapoints, 6);

        assert_eq!(snap.totals.metric_types.gauge, 1);
        assert_eq!(snap.totals.metric_types.sum, 1);
        assert_eq!(snap.totals.metric_types.histogram, 1);
        assert_eq!(snap.totals.metric_types.exponential_histogram, 1);
        assert_eq!(snap.totals.metric_types.summary, 1);

        assert_eq!(snap.totals.datapoint_types.gauge, 2);
        assert_eq!(snap.totals.datapoint_types.sum, 1);
        assert_eq!(snap.totals.datapoint_types.histogram, 1);
        assert_eq!(snap.totals.datapoint_types.exponential_histogram, 1);
        assert_eq!(snap.totals.datapoint_types.summary, 1);

        let store_stats = processor.labelsets.stats();
        assert_eq!(store_stats.series, 5); // gauge datapoints dedup to 1 series

        assert_eq!(snap.partition_watermarks.len(), 1);
        let ((topic, partition), wm) = &snap.partition_watermarks[0];
        assert_eq!(topic, "t");
        assert_eq!(*partition, 1);
        assert_eq!(wm.messages, 1);
        assert_eq!(wm.datapoints, 6);

        processor.maybe_report_labelset_stats(true);
        let snap = processor.labelset_stats.snapshot();
        assert_eq!(snap.window.messages, 0);
        assert_eq!(snap.window.metrics, 0);
        assert_eq!(snap.window.datapoints, 0);
        assert_eq!(snap.window.unique_metrics, 0);
    }

    #[test]
    fn data_type_counts_markdown_reports_metric_records_and_datapoints() {
        let mut metric_types = OtlpDataTypeCounts::default();
        metric_types.gauge = 1;
        metric_types.sum = 2;
        metric_types.histogram = 3;
        metric_types.exponential_histogram = 4;
        metric_types.summary = 5;
        let mut datapoint_types = OtlpDataTypeCounts::default();
        datapoint_types.gauge = 10;
        datapoint_types.sum = 20;
        datapoint_types.histogram = 30;
        datapoint_types.exponential_histogram = 40;
        datapoint_types.summary = 50;

        let markdown = data_type_counts_markdown(&metric_types, &datapoint_types);

        assert!(markdown.contains("## OTLP Data Type Counts"));
        assert!(markdown.contains("| Type | Metric Records | Datapoints |"));
        assert!(markdown.contains("| Gauge | 1 | 10 |"));
        assert!(markdown.contains("| Sum | 2 | 20 |"));
        assert!(markdown.contains("| Histogram | 3 | 30 |"));
        assert!(markdown.contains("| Exponential Histogram | 4 | 40 |"));
        assert!(markdown.contains("| Summary | 5 | 50 |"));
    }

    #[test]
    fn datapoint_policy_counts_markdown_reports_drop_reasons() {
        let totals = DatapointPolicyCounts {
            accepted: 10,
            dropped_too_old: 2,
            dropped_too_future: 3,
            missing_timestamp: 4,
        };
        let window = DatapointPolicyCounts {
            accepted: 1,
            dropped_too_old: 0,
            dropped_too_future: 1,
            missing_timestamp: 0,
        };

        let markdown = datapoint_policy_counts_markdown(&totals, &window);

        assert!(markdown.contains("## Datapoint Policy Counts"));
        assert!(markdown.contains("| Accepted | 10 | 1 |"));
        assert!(markdown.contains("| Dropped Too Old | 2 | 0 |"));
        assert!(markdown.contains("| Dropped Too Future | 3 | 1 |"));
        assert!(markdown.contains("| Missing Timestamp | 4 | 0 |"));
        assert!(markdown.contains("| Rejected Total | 9 | 1 |"));
    }

    #[test]
    fn event_time_skew_markdown_reports_signed_distributions() {
        let mut stats = OtlpMetricsIngestionStats::new();
        stats.record_event_time_skew(metrics_ingestion_stats::EventTimeSkewOutcome::Accepted, -5);
        stats.record_event_time_skew(
            metrics_ingestion_stats::EventTimeSkewOutcome::DroppedTooOld,
            -10,
        );
        stats.record_event_time_skew(
            metrics_ingestion_stats::EventTimeSkewOutcome::DroppedTooFuture,
            3,
        );
        let snapshot = stats.snapshot();

        let markdown = event_time_skew_markdown(&snapshot.totals.event_time_skew);

        assert!(markdown.contains("## Event Time Skew"));
        assert!(markdown.contains("event_ms - captured_at_ms"));
        assert!(markdown.contains("| All Timestamped | 3 |"));
        assert!(markdown.contains("| Accepted | 1 |"));
        assert!(markdown.contains("| Dropped Too Old | 1 |"));
        assert!(markdown.contains("| Dropped Too Future | 1 |"));
    }

    #[test]
    fn number_value_handles_int_and_double() {
        let mut dp = number_dp(vec![]);
        dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(5));
        assert_eq!(number_value(&dp), Some(SampleValue::Int64(5)));

        dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.5));
        assert_eq!(number_value(&dp), Some(SampleValue::Float(2.5)));

        dp.value = None;
        assert_eq!(number_value(&dp), None);
    }

    #[test]
    fn processor_writes_segment_meta() {
        let tempdir = tempfile::tempdir().unwrap();
        let writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();

        let head = Some(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ));
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            head,
            Some(writer),
        );

        let mut dp = number_dp(vec![kv_str("pod", "backend-1")]);
        dp.time_unix_nano = 5_000_000_000;
        dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(3.14));
        let req = request(vec![], vec![metric_gauge("cpu_usage", vec![dp])]);

        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 1_000,
                    captured_at_ms: 10_002,
                },
                req,
            )
            .unwrap();
        processor.flush_head().unwrap();
        let profile = processor.last_head_window_write_profile().unwrap();
        assert_eq!(profile.series, 1);
        assert_eq!(profile.datapoints, 1);
        assert!(profile.total >= profile.writer_flush);

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();

        let reader = SegmentReader::open(seg_dir).unwrap();
        assert_eq!(reader.meta().datapoints, 1);
        assert_eq!(reader.meta().series, 1);
        let chunk_len = fs::metadata(reader.file_path(SegmentFile::Chunks))
            .unwrap()
            .len();
        assert!(chunk_len > 0);
    }

    #[test]
    fn processor_writes_segment_series_metadata_and_exact_postings() {
        let tempdir = tempfile::tempdir().unwrap();
        let writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();

        let head = Some(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ));
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            head,
            Some(writer),
        );

        let mut dp1 = number_dp(vec![
            kv_str("namespace", "default"),
            kv_str("pod.name", "backend-1"),
        ]);
        dp1.time_unix_nano = 5_000_000_000;
        dp1.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));

        let mut dp2 = number_dp(vec![
            kv_str("namespace", "default"),
            kv_str("pod.name", "backend-2"),
        ]);
        dp2.time_unix_nano = 6_000_000_000;
        dp2.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));

        let req = request(vec![], vec![metric_gauge("cpu.usage", vec![dp1, dp2])]);

        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 1_000,
                    captured_at_ms: 10_003,
                },
                req,
            )
            .unwrap();
        processor.flush_head().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        let reader = SegmentReader::open(seg_dir).unwrap();

        let symbols = read_symbols_bin(
            File::open(reader.file_path(SegmentFile::Symbols)).expect("open symbols"),
        )
        .unwrap();
        let series = read_series_bin(
            File::open(reader.file_path(SegmentFile::Series)).expect("open series"),
        )
        .unwrap();
        let indexes = read_segment_indexes(
            File::open(reader.file_path(SegmentFile::Indexes)).expect("open indexes"),
        )
        .unwrap();
        let postings = indexes.exact_postings;

        assert_eq!(series.len(), 2);
        let metric_sym = symbols.lookup(METRIC_NAME_LABEL).unwrap();
        let metric_value = series[0]
            .labels
            .iter()
            .find_map(|(key, value)| (*key == metric_sym).then_some(*value))
            .and_then(|sym| symbols.resolve(sym))
            .unwrap();
        assert!(metric_value.starts_with("cpu_usage_x"));

        let namespace_sym = symbols.lookup("namespace").unwrap();
        let default_sym = symbols.lookup("default").unwrap();
        assert_eq!(postings.get(namespace_sym, default_sym), Some(&[0, 1][..]));

        let labels: Vec<_> = series
            .iter()
            .flat_map(|entry| {
                entry.labels.iter().map(|(key, value)| {
                    (
                        symbols.resolve(*key).unwrap().to_string(),
                        symbols.resolve(*value).unwrap().to_string(),
                    )
                })
            })
            .collect();
        assert!(
            labels
                .iter()
                .any(|(key, value)| { key.starts_with("pod_name_x") && value == "backend-1" })
        );
        assert!(
            labels
                .iter()
                .any(|(key, value)| { key.starts_with("pod_name_x") && value == "backend-2" })
        );
    }

    #[test]
    fn processor_writes_integer_number_datapoints_as_promql_float_samples() {
        let tempdir = tempfile::tempdir().unwrap();
        let writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();

        let head = Some(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ));
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            head,
            Some(writer),
        );

        let mut dp = number_dp(vec![kv_str("pod.name", "backend-1")]);
        dp.time_unix_nano = 5_000_000_000;
        dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));
        let req = request(vec![], vec![metric_sum("requests.total", vec![dp])]);

        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 1_000,
                    captured_at_ms: 10_004,
                },
                req,
            )
            .unwrap();
        processor.flush_head().unwrap();

        let seg_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        let reader = SegmentReader::open(seg_dir).unwrap();

        let metric = normalize_metric_name("requests.total");
        let pod_label = normalize_label_name("pod.name");
        let results = reader
            .query_exact(
                &[
                    (METRIC_NAME_LABEL, metric.as_str()),
                    (pod_label.as_str(), "backend-1"),
                ],
                0,
                10_000,
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].samples, vec![(5_000, 42.0)]);
    }

    #[test]
    fn processor_flushes_bounded_late_sample_as_overlapping_segment() {
        let tempdir = tempfile::tempdir().unwrap();
        let writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(10),
        ))
        .unwrap();

        let head = Some(
            HeadConfig::new(
                Duration::from_secs(10),
                FloatEncoding::Gorilla,
                IntEncoding::DeltaZigZag,
            )
            .with_out_of_order_time_window(Duration::from_secs(6)),
        );
        let mut processor = OtlpLabelSetProcessor::new(
            LabelSetStoreKind::FlatInterned,
            Duration::from_secs(3600),
            head,
            Some(writer),
        );

        let mut first = number_dp(vec![kv_str("pod.name", "backend-1")]);
        first.time_unix_nano = 15_000_000_000;
        first.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 1_000,
                    captured_at_ms: 10_005,
                },
                request(vec![], vec![metric_gauge("cpu.usage", vec![first])]),
            )
            .unwrap();
        assert_eq!(segment_dir_count(tempdir.path()), 0);

        let mut late = number_dp(vec![kv_str("pod.name", "backend-1")]);
        late.time_unix_nano = 9_500_000_000;
        late.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 1,
                    timestamp_ms: 2_000,
                    captured_at_ms: 10_006,
                },
                request(vec![], vec![metric_gauge("cpu.usage", vec![late])]),
            )
            .unwrap();
        assert_eq!(segment_dir_count(tempdir.path()), 0);

        processor.flush_head().unwrap();
        assert_eq!(segment_dir_count(tempdir.path()), 2);

        let store = SegmentStoreReader::open(tempdir.path()).unwrap();
        let metric = normalize_metric_name("cpu.usage");
        let pod_label = normalize_label_name("pod.name");
        let results = store
            .query_exact(
                &[
                    (METRIC_NAME_LABEL, metric.as_str()),
                    (pod_label.as_str(), "backend-1"),
                ],
                0,
                20_000,
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].samples, vec![(9_500, 2.0), (15_000, 1.0)]);
    }
}
