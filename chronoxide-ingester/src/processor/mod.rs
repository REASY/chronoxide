use crate::app_config::LabelSetStoreKind;
use crate::error::should_log;
use crate::prelude::*;
use crate::source::SourceMessageMetadata;
use crate::statistics::{label_tag_stats_from_store, per_key_value_stats_markdown_from_store};
use chrono::{DateTime, Local, Utc};
use chronoxide_core::event_time::DatapointTimeDecision;
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetRow, FlatInternedLabelSetStore,
    KeySetDictEncodedLabelSetStore, KeyValueRef, LabelSetStore, LabelSetStoreError,
    METRIC_NAME_LABEL, NaiveLabelSetStore, SeriesRef, SymbolId, SymbolTable as _,
};
use chronoxide_core::otlp::{
    exponential_histogram_value_with_buckets, histogram_value_with_buckets, number_value,
    summary_value, take_exponential_histogram_buckets,
};
use chronoxide_core::otlp_labelset::{
    CanonicalLabelSet, OtlpLabelSetInterner, PreparedOtlpLabelSetScratch, PreparedOtlpMetricLabels,
    PreparedOtlpResourceLabels, intern_prepared_labelset as intern_prepared_otlp_labelset,
};
use chronoxide_core::otlp_reset::OtlpResetTracker;
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::head::{
    ExponentialHistogramValue, FloatEncoding, HeadBuffer, HeadConfig, HeadWindow, HistogramValue,
    SampleValue, SeriesSamples, SummaryValue,
};
use chronoxide_core::storage::segment::{
    DeferredFlatMetadataBatch, SegmentRecordProfile, SegmentSeriesMetadata,
    SegmentSeriesMetadataBuilder, SegmentWriter,
};
use chronoxide_core::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM,
    SERIES_KIND_SUMMARY,
};
use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
#[cfg(test)]
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{Level, error, info, warn};

pub use chronoxide_core::event_time::EventTimePolicy;

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

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct DatapointIngestResult {
    accepted: u64,
    dropped_too_old: u64,
    dropped_too_future: u64,
    missing_timestamp: u64,
    invalid_typed: u64,
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
        self.invalid_typed = self.invalid_typed.saturating_add(other.invalid_typed);
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

    fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
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
    otlp_reset_tracker: OtlpResetTracker,
    segment_writer: Option<SegmentWriter>,
    last_head_window_write_profile: Option<HeadWindowWriteProfile>,
    shutdown_report: bool,
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
            otlp_reset_tracker: OtlpResetTracker::default(),
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
        self.otlp_reset_tracker.stamp_histogram(series, value);
    }

    fn stamp_exponential_histogram_reset_hint(
        &mut self,
        series: SeriesRef,
        value: &mut ExponentialHistogramValue,
    ) {
        self.otlp_reset_tracker
            .stamp_exponential_histogram(series, value);
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
#[path = "otlp/segment_output.rs"]
mod segment_output;

use label_interner::*;
use report_format::*;
use segment_output::*;

#[cfg(test)]
mod tests;
