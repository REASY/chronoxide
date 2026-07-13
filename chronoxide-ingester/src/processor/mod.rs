use crate::app_config::LabelSetStoreKind;
use crate::source::SourceMessageMetadata;
use crate::statistics::{label_tag_stats_from_store, per_key_value_stats_markdown_from_store};
use chrono::{DateTime, Local, TimeDelta, Utc};
use chronoxide_core::error::should_log;
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, LabelSetStoreError, METRIC_NAME_LABEL, NaiveLabelSetStore, SeriesRef, SymbolId,
    SymbolTable as _, TmpLabel,
};
use chronoxide_core::otlp::{
    exponential_histogram_value, histogram_value, number_value, summary_value,
};
use chronoxide_core::otlp_labelset::{
    OtlpLabelSetInterner, intern_labelset as intern_otlp_labelset,
};
use chronoxide_core::prelude::*;
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, FloatEncoding,
    HeadBuffer, HeadConfig, HeadWindow, HistogramValue, OtlpAggregationTemporality, SampleValue,
    SeriesSamples, SummaryValue, downscale_exponential_histogram_buckets_to_map,
};
use chronoxide_core::storage::segment::{
    SegmentRecordProfile, SegmentSeriesMetadata, SegmentSeriesMetadataBuilder, SegmentWriter,
};
use chronoxide_core::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM,
    SERIES_KIND_SUMMARY,
};
use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
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
}

#[path = "otlp/label_interner.rs"]
mod label_interner;
#[path = "otlp/pipeline.rs"]
mod pipeline;
#[path = "otlp/report.rs"]
mod report;
#[path = "otlp/report_format.rs"]
mod report_format;
#[path = "otlp/reset.rs"]
mod reset;
#[path = "otlp/segment_output.rs"]
mod segment_output;

use label_interner::*;
use report_format::*;
use reset::*;
use segment_output::*;

#[cfg(test)]
mod tests;
