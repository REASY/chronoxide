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
    VersionedFlatInternedLabelSetRow, VersionedFlatInternedLabelSetSnapshot,
    VersionedFlatInternedLabelSetStore, VersionedFlatLabelStoreError, VersionedSymbolTable,
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
    SampleKind, SampleValue, SeriesSamples, SummaryValue,
};
use chronoxide_core::storage::live_coverage::{
    CoverageLedger, MessageSampleOrdinals, MessageSequence, PreparedRecordedSampleAppend,
    RecordedSampleContribution, RecordedSampleOrder, RecordedSampleOrderSet,
};
use chronoxide_core::storage::segment::{
    DeferredFlatMetadataBatch, SegmentPayloadLane, SegmentRecordProfile, SegmentSeriesMetadata,
    SegmentSeriesMetadataBuilder, SegmentStorageSchema, SegmentWriter,
};
use chronoxide_core::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM,
    SERIES_KIND_SUMMARY,
};
use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
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
type LiveInternedStore = VersionedFlatInternedLabelSetStore;
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
    /// Aligned in-order ranges whose active window rotated and may now be
    /// handed off. The windows themselves remain recoverably owned by
    /// `HeadBuffer` until a publication boundary freezes them.
    seal_ready_ranges: BTreeSet<(u64, u64)>,
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

    /// Whether the ingestion loop must assign exact acquired-message
    /// sequences and invoke the two boundary hooks below.
    fn live_message_tracking_enabled(&self) -> bool {
        false
    }

    /// Called after source acquisition and before protobuf decoding.
    fn begin_acquired_message(&mut self, _sequence: MessageSequence) -> Result<()> {
        Ok(())
    }

    /// Called after processing has reinserted any temporarily removed head.
    ///
    /// This is also called for empty, rejected-only, malformed-protobuf, and
    /// error-returning messages.
    fn complete_acquired_message(&mut self, _sequence: MessageSequence) -> Result<()> {
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedMessageCoverage {
    pub message_sequence: MessageSequence,
    pub coverage: CoverageLedger,
    pub completed_prefix: CoverageLedger,
    pub successful_orders: RecordedSampleOrderSet,
}

#[derive(Debug)]
struct ActiveMessageCoverage {
    ordinals: MessageSampleOrdinals,
    coverage: CoverageLedger,
    successful_orders: RecordedSampleOrderSet,
}

#[derive(Debug)]
struct PreparedMessageCoverage {
    coverage: CoverageLedger,
    ownership_append: PreparedRecordedSampleAppend,
}

#[derive(Debug, Default)]
struct LiveCoverageTracking {
    active: Option<ActiveMessageCoverage>,
    completed: VecDeque<CompletedMessageCoverage>,
    completed_prefix: CoverageLedger,
    last_completed: Option<MessageSequence>,
    semantic_scratch: Vec<u8>,
    #[cfg(test)]
    fail_next_completed_reserve: bool,
}

impl LiveCoverageTracking {
    fn begin_message(&mut self, sequence: MessageSequence) -> Result<()> {
        if self.active.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot begin a live message while another message is active",
            )
            .into());
        }
        if let Some(previous) = self.last_completed {
            let expected = previous.get().checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "completed live message sequence exhausted u64 capacity",
                )
            })?;
            if sequence.get() != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "live message sequence is not contiguous: previous={} next={}",
                        previous, sequence
                    ),
                )
                .into());
            }
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_completed_reserve) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "injected completed live-message coverage reservation failure",
            )
            .into());
        }
        self.completed.try_reserve(1).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to reserve completed live-message coverage: {error}"),
            )
        })?;
        self.active = Some(ActiveMessageCoverage {
            ordinals: MessageSampleOrdinals::new(sequence),
            coverage: CoverageLedger::empty(),
            successful_orders: RecordedSampleOrderSet::empty(),
        });
        Ok(())
    }

    fn complete_message(&mut self, sequence: MessageSequence) -> Result<()> {
        let active = self.active.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot complete a live message when none is active",
            )
        })?;
        if active.ordinals.sequence() != sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "completed live message sequence differs from active sequence: active={} completed={}",
                    active.ordinals.sequence(),
                    sequence
                ),
            )
            .into());
        }
        active.successful_orders.validate()?;
        if active.successful_orders.sample_count() != active.coverage.sample_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "completed message coverage disagrees with exact successful orders",
            )
            .into());
        }
        if active
            .successful_orders
            .runs()
            .iter()
            .any(|run| run.first().message_sequence() != sequence)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "completed message exact ownership crosses its message boundary",
            )
            .into());
        }
        let completed_prefix = self.completed_prefix.checked_merge(active.coverage)?;
        let active = self
            .active
            .take()
            .expect("active coverage was validated above");
        self.completed.push_back(CompletedMessageCoverage {
            message_sequence: sequence,
            coverage: active.coverage,
            completed_prefix,
            successful_orders: active.successful_orders,
        });
        self.completed_prefix = completed_prefix;
        self.last_completed = Some(sequence);
        Ok(())
    }

    fn next_sample_order(&mut self) -> Result<RecordedSampleOrder> {
        let active = self.active.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot assign a sample ordinal outside an acquired message",
            )
        })?;
        active.successful_orders.try_reserve_additional_run()?;
        active.ordinals.next_order().map_err(Into::into)
    }

    fn prepare_contribution(
        &mut self,
        order: RecordedSampleOrder,
        series: SeriesRef,
        timestamp_ms: u64,
        value: &SampleValue,
    ) -> Result<(RecordedSampleContribution, PreparedMessageCoverage)> {
        let Self {
            active,
            semantic_scratch,
            ..
        } = self;
        let active = active.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot prepare sample coverage outside an acquired message",
            )
        })?;
        if active.ordinals.sequence() != order.message_sequence() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sample order belongs to a different active message",
            )
            .into());
        }
        let contribution = RecordedSampleContribution::for_sample(
            order,
            series,
            timestamp_ms,
            value,
            semantic_scratch,
        )?;
        let coverage = active.coverage.checked_with_contribution(contribution)?;
        let ownership_append = active.successful_orders.try_prepare_append(order)?;
        Ok((
            contribution,
            PreparedMessageCoverage {
                coverage,
                ownership_append,
            },
        ))
    }

    fn commit_contribution(&mut self, prepared: PreparedMessageCoverage) {
        let active = self
            .active
            .as_mut()
            .expect("sample contribution was prepared against an active message");
        active
            .successful_orders
            .commit_prepared_append(prepared.ownership_append);
        active.coverage = prepared.coverage;
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
    live_coverage: Option<LiveCoverageTracking>,
    live_publisher: Option<LivePublisher>,
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
            live_coverage: None,
            live_publisher: None,
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

    /// Enables startup-only live message sequencing and exact head coverage.
    pub fn enable_live_coverage_tracking(&mut self) -> Result<()> {
        self.validate_live_tracking_pristine()?;
        if self.live_coverage.is_some() {
            return Ok(());
        }
        self.live_coverage = Some(LiveCoverageTracking::default());
        Ok(())
    }

    /// Enables the complete startup-only backing required by immutable live
    /// query views.
    ///
    /// This must run before the processor observes a message. It replaces only
    /// an empty production FlatInterned store; disabled mode retains the
    /// existing contiguous interner and hot path.
    pub fn enable_live_query_mode(&mut self) -> Result<()> {
        self.validate_live_query_mode_pristine()?;
        self.activate_live_query_mode();
        Ok(())
    }

    fn validate_live_tracking_pristine(&self) -> Result<()> {
        if self.head_config.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live coverage tracking requires an enabled head buffer",
            )
            .into());
        }
        let observed_tracking = self.live_coverage.as_ref().is_some_and(|tracking| {
            tracking.active.is_some()
                || !tracking.completed.is_empty()
                || tracking.last_completed.is_some()
                || tracking.completed_prefix != CoverageLedger::empty()
        });
        if observed_tracking || !self.partition_heads.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live coverage tracking must be enabled before processing messages",
            )
            .into());
        }
        Ok(())
    }

    fn validate_live_query_mode_pristine(&self) -> Result<()> {
        self.validate_live_tracking_pristine()?;
        match &self.labelsets {
            LabelSetInterner::VersionedFlatInterned(store) if store.is_empty() => {}
            LabelSetInterner::VersionedFlatInterned(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "live query mode must be enabled before processing messages",
                )
                .into());
            }
            LabelSetInterner::FlatInterned(store)
                if self.labelsets.kind() == "FlatInterned" && store.is_empty() => {}
            LabelSetInterner::FlatInterned(_) if self.labelsets.kind() != "FlatInterned" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "live query mode requires the production FlatInterned label store",
                )
                .into());
            }
            LabelSetInterner::FlatInterned(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "live query mode must be enabled before processing messages",
                )
                .into());
            }
            LabelSetInterner::Naive(_) | LabelSetInterner::KeySetDictEncoded(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "live query mode requires the production FlatInterned label store",
                )
                .into());
            }
        }
        Ok(())
    }

    fn activate_live_query_mode(&mut self) {
        if matches!(&self.labelsets, LabelSetInterner::FlatInterned(_)) {
            self.labelsets = LabelSetInterner::new_versioned_flat();
        } else {
            debug_assert!(matches!(
                &self.labelsets,
                LabelSetInterner::VersionedFlatInterned(_)
            ));
        }
        if self.live_coverage.is_none() {
            self.live_coverage = Some(LiveCoverageTracking::default());
        }
    }

    /// Enables the complete startup-only immutable live publication path and
    /// returns the handle consumed by the embedded HTTP router.
    pub fn enable_live_publication(
        &mut self,
        config: LivePublisherConfig,
    ) -> Result<
        Arc<
            chronoxide_core::storage::live_view::LiveQueryHandle<
                chronoxide_core::storage::live_view::LiveStorageView,
            >,
        >,
    > {
        if self.live_publisher.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "live publication is already enabled",
            )
            .into());
        }
        let segment_writer = match self.segment_writer.as_ref() {
            Some(writer) => writer,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "live publication requires a configured segment writer",
                )
                .into());
            }
        };
        self.validate_live_query_mode_pristine()?;
        let segment_writer_config = segment_writer.pristine_config_for_takeover()?;
        if segment_writer_config.storage_schema() != SegmentStorageSchema::Schema8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live publication requires a Schema 8 segment writer",
            )
            .into());
        }
        let head_config = self
            .head_config
            .as_ref()
            .expect("live query validation requires an enabled head");
        if head_config.window_duration != segment_writer_config.segment_duration {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "live head window duration {:?} must equal segment writer duration {:?}",
                    head_config.window_duration, segment_writer_config.segment_duration
                ),
            )
            .into());
        }
        // Validate the complete head configuration before the interner,
        // coverage tracker, or writer ownership changes.
        drop(HeadBuffer::new(head_config.clone())?);

        // Construct all fallible disk/query state before changing the label
        // interner or taking ownership of the ordinary segment writer.
        let publisher = LivePublisher::new(config, segment_writer_config)?;
        self.activate_live_query_mode();
        let handle = publisher.handle();
        let previous_writer = self
            .segment_writer
            .take()
            .expect("live publisher takeover validated a configured writer");
        self.live_publisher = Some(publisher);
        drop(previous_writer);
        Ok(handle)
    }

    pub fn live_memory_stats(
        &self,
    ) -> Option<chronoxide_core::storage::live_memory::LiveMemoryStats> {
        self.live_publisher
            .as_ref()
            .map(|publisher| publisher.memory_governor().stats())
    }

    #[cfg(test)]
    fn set_next_live_head_decode_hook(&mut self, hook: impl Fn() + Send + Sync + 'static) {
        self.live_publisher
            .as_mut()
            .expect("live publication must be enabled before installing a decode hook")
            .set_next_head_decode_hook(hook);
    }

    pub fn pop_completed_message_coverage(&mut self) -> Option<CompletedMessageCoverage> {
        self.live_coverage
            .as_mut()
            .and_then(|tracking| tracking.completed.pop_front())
    }

    pub fn completed_coverage_prefix(&self) -> Option<CoverageLedger> {
        self.live_coverage
            .as_ref()
            .map(|tracking| tracking.completed_prefix)
    }
}

#[path = "otlp/label_interner.rs"]
mod label_interner;
mod live_publisher;
mod live_seal;
#[path = "otlp/pipeline.rs"]
mod pipeline;
#[path = "otlp/report.rs"]
mod report;
#[path = "otlp/report_format.rs"]
mod report_format;
#[path = "otlp/segment_output.rs"]
mod segment_output;

use label_interner::*;
use live_publisher::LivePublisher;
pub use live_publisher::LivePublisherConfig;
use report_format::*;
use segment_output::*;

#[cfg(test)]
mod tests;
