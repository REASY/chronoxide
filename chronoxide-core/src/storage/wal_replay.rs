use std::fs::File;
use std::io::{self, Error, ErrorKind, Seek, SeekFrom};
use std::path::Path;

use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;

use crate::event_time::{DatapointTimeDecision, EventTimePolicy};
use crate::labels::{LabelSetStore, LabelSetStoreError, SeriesRef, TmpLabel};
use crate::otlp::{exponential_histogram_value, histogram_value, number_value, summary_value};
use crate::otlp_labelset::{
    CanonicalLabelSet, OtlpLabelSetInterner, intern_labelset as intern_otlp_labelset,
};
use crate::otlp_reset::OtlpResetTracker;
use crate::storage::head::{HeadBuffer, HeadWindow, SampleValue};
use crate::storage::wal::{
    WalCheckpoint, WalRecord, WalRecordType, decode_checkpoint_record, decode_otlp_batch_payload,
    read_checkpoint_meta, read_wal_record,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReplayStopReason {
    InvalidRecord,
    UnexpectedEof,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WalReplayReport {
    pub checkpoint_lsn: Option<u64>,
    pub replay_start_lsn: u64,
    pub records_read: u64,
    pub checkpoints_read: u64,
    pub batches_replayed: u64,
    /// CRC-valid OTLP batches rejected because their protobuf payload is malformed.
    pub invalid_otlp_batches: u64,
    /// Datapoints accepted by the event-time policy, including accepted number points with no value.
    pub policy_accepted_datapoints: u64,
    /// Time-policy-accepted typed datapoints rejected before labels, reset state, or head mutation.
    pub invalid_typed_datapoints: u64,
    pub dropped_too_old_datapoints: u64,
    pub dropped_too_future_datapoints: u64,
    pub missing_timestamp_datapoints: u64,
    /// Samples actually written after value and label validation.
    pub datapoints_replayed: u64,
    pub skipped_non_scalar_labels: u64,
    pub labelset_errors: u64,
    pub stopped_at_lsn: Option<u64>,
    pub stop_reason: Option<WalReplayStopReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplayPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug)]
#[must_use = "completed replay windows must be persisted or retained"]
pub struct WalReplayOutcome {
    pub report: WalReplayReport,
    /// The only source partition accepted by this single-head replay API.
    pub partition: Option<WalReplayPartition>,
    /// Complete head windows rotated while applying WAL samples.
    ///
    /// The caller must persist or otherwise consume these windows; the mutable
    /// `HeadBuffer` argument retains only the active and configured late windows.
    pub completed_windows: Vec<HeadWindow>,
}

#[derive(Default)]
struct WalReplayRuntime {
    report: WalReplayReport,
    reset_tracker: OtlpResetTracker,
    partition: Option<WalReplayPartition>,
    completed_windows: Vec<HeadWindow>,
}

/// Rebuilds one source partition into a fresh head and label store.
///
/// A fatal error may be returned after earlier records mutated `head` and
/// `labels`. The caller must therefore discard both arguments on `Err`; the
/// partially rebuilt state is not a valid recovery result.
pub fn replay_wal_file_into_head<L>(
    wal_path: impl AsRef<Path>,
    event_time_policy: EventTimePolicy,
    head: &mut HeadBuffer,
    labels: &mut L,
) -> io::Result<WalReplayOutcome>
where
    L: LabelSetStore,
{
    replay_wal_file_into_head_after_checkpoint(wal_path, None, event_time_policy, head, labels)
}

/// Validates the published checkpoint and rebuilds one source partition.
///
/// As with [`replay_wal_file_into_head`], `head` and `labels` must be fresh and
/// must be discarded if replay returns an error.
pub fn replay_wal_file_into_head_from_checkpoint<L>(
    wal_path: impl AsRef<Path>,
    checkpoint_dir: impl AsRef<Path>,
    event_time_policy: EventTimePolicy,
    head: &mut HeadBuffer,
    labels: &mut L,
) -> io::Result<WalReplayOutcome>
where
    L: LabelSetStore,
{
    let checkpoint = read_checkpoint_meta(checkpoint_dir)?;
    replay_wal_file_into_head_after_checkpoint(
        wal_path,
        checkpoint.as_ref(),
        event_time_policy,
        head,
        labels,
    )
}

fn replay_wal_file_into_head_after_checkpoint<L>(
    wal_path: impl AsRef<Path>,
    checkpoint: Option<&WalCheckpoint>,
    event_time_policy: EventTimePolicy,
    head: &mut HeadBuffer,
    labels: &mut L,
) -> io::Result<WalReplayOutcome>
where
    L: LabelSetStore,
{
    if !head.is_empty() || !labels.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "WAL replay requires a fresh empty head and label store",
        ));
    }

    let mut file = File::open(wal_path)?;
    let mut runtime = WalReplayRuntime::default();

    if let Some(checkpoint) = checkpoint {
        runtime.report.checkpoint_lsn = Some(checkpoint.wal_lsn);
        file.seek(SeekFrom::Start(checkpoint.wal_lsn))?;
        let record = read_wal_record(&mut file)?
            .ok_or_else(|| invalid_data("checkpoint.meta points past WAL EOF"))?;
        let decoded = decode_checkpoint_record(&record)?;
        if &decoded != checkpoint {
            return Err(invalid_data(
                "checkpoint.meta does not match WAL checkpoint record",
            ));
        }
    }

    // checkpoint.meta currently proves source offsets and a checkpoint record,
    // but carries no durable head snapshot or manifest publication boundary.
    // Rebuild the head from the complete single-file WAL to avoid data loss.
    file.seek(SeekFrom::Start(0))?;
    runtime.report.replay_start_lsn = file.stream_position()?;

    loop {
        let record_lsn = file.stream_position()?;
        let record = match read_wal_record(&mut file) {
            Ok(Some(record)) => record,
            Ok(None) => break,
            Err(err) => {
                if let Some(reason) = replay_stop_reason(&err) {
                    runtime.report.stopped_at_lsn = Some(record_lsn);
                    runtime.report.stop_reason = Some(reason);
                    break;
                }
                return Err(err);
            }
        };
        runtime.report.records_read = runtime.report.records_read.saturating_add(1);
        replay_record(
            record_lsn,
            &record,
            &event_time_policy,
            head,
            labels,
            &mut runtime,
        )?;
        if runtime.report.stop_reason.is_some() {
            break;
        }
    }

    Ok(WalReplayOutcome {
        report: runtime.report,
        partition: runtime.partition,
        completed_windows: runtime.completed_windows,
    })
}

fn replay_record<L>(
    record_lsn: u64,
    record: &WalRecord,
    event_time_policy: &EventTimePolicy,
    head: &mut HeadBuffer,
    labels: &mut L,
    runtime: &mut WalReplayRuntime,
) -> io::Result<()>
where
    L: LabelSetStore,
{
    match record.record_type {
        WalRecordType::OtlpBatch => {
            let batch = match decode_otlp_batch_payload(&record.payload) {
                Ok(batch) => batch,
                Err(err) => {
                    if let Some(reason) = replay_stop_reason(&err) {
                        runtime.report.stopped_at_lsn = Some(record_lsn);
                        runtime.report.stop_reason = Some(reason);
                        return Ok(());
                    }
                    return Err(err);
                }
            };
            observe_replay_partition(&mut runtime.partition, &batch)?;
            let request = match ExportMetricsServiceRequest::decode(batch.payload.as_slice()) {
                Ok(request) => request,
                Err(_) => {
                    runtime.report.invalid_otlp_batches =
                        runtime.report.invalid_otlp_batches.saturating_add(1);
                    return Ok(());
                }
            };
            let datapoints = replay_otlp_batch(
                &request,
                ReplayBatchMode {
                    captured_at_ms: batch.captured_at_ms,
                    event_time_policy,
                },
                head,
                labels,
                runtime,
            )?;
            runtime.report.batches_replayed = runtime.report.batches_replayed.saturating_add(1);
            runtime.report.datapoints_replayed = runtime
                .report
                .datapoints_replayed
                .saturating_add(datapoints);
        }
        WalRecordType::Checkpoint => {
            let _ = decode_checkpoint_record(record)?;
            runtime.report.checkpoints_read = runtime.report.checkpoints_read.saturating_add(1);
        }
        WalRecordType::SegmentSealed => {}
    }
    Ok(())
}

fn observe_replay_partition(
    replay_partition: &mut Option<WalReplayPartition>,
    batch: &crate::storage::wal::OtlpWalBatch,
) -> io::Result<()> {
    let next = WalReplayPartition {
        topic: batch.topic.clone(),
        partition: batch.partition,
    };
    match replay_partition {
        Some(current) if current != &next => Err(Error::new(
            ErrorKind::Unsupported,
            "single-head WAL replay encountered multiple source partitions",
        )),
        Some(_) => Ok(()),
        None => {
            *replay_partition = Some(next);
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
struct ReplayBatchMode<'a> {
    captured_at_ms: i64,
    event_time_policy: &'a EventTimePolicy,
}

fn replay_otlp_batch<L>(
    request: &ExportMetricsServiceRequest,
    mode: ReplayBatchMode<'_>,
    head: &mut HeadBuffer,
    labels: &mut L,
    runtime: &mut WalReplayRuntime,
) -> io::Result<u64>
where
    L: LabelSetStore,
{
    let mut recorded = 0u64;
    let mut label_scratch = ReplayLabelScratch::default();

    for resource_metrics in &request.resource_metrics {
        let resource_attrs = resource_metrics
            .resource
            .as_ref()
            .map(|resource| resource.attributes.as_slice())
            .unwrap_or(&[]);

        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                let metric_name = metric.name.as_str();
                let Some(metric_data) = metric.data.as_ref() else {
                    continue;
                };

                match metric_data {
                    tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                        recorded = recorded.saturating_add(replay_number_datapoints(
                            head,
                            labels,
                            NumberReplayBatch {
                                resource_attrs,
                                metric_name,
                                points: &gauge.data_points,
                            },
                            &mut label_scratch,
                            mode,
                            runtime,
                        )?);
                    }
                    tonic::metrics::v1::metric::Data::Sum(sum) => {
                        recorded = recorded.saturating_add(replay_number_datapoints(
                            head,
                            labels,
                            NumberReplayBatch {
                                resource_attrs,
                                metric_name,
                                points: &sum.data_points,
                            },
                            &mut label_scratch,
                            mode,
                            runtime,
                        )?);
                    }
                    tonic::metrics::v1::metric::Data::Histogram(histogram) => {
                        for dp in &histogram.data_points {
                            let Some(ts_ms) = evaluate_replay_datapoint_time(
                                mode.event_time_policy,
                                dp.time_unix_nano,
                                mode.captured_at_ms,
                                &mut runtime.report,
                            ) else {
                                continue;
                            };
                            let mut value = histogram_value(dp, histogram.aggregation_temporality);
                            if value.validate_for_storage().is_err() {
                                runtime.report.invalid_typed_datapoints =
                                    runtime.report.invalid_typed_datapoints.saturating_add(1);
                                continue;
                            }
                            if let Some(series) = intern_replay_labelset(
                                labels,
                                &mut runtime.report,
                                resource_attrs,
                                metric_name,
                                &dp.attributes,
                                &mut label_scratch.values,
                                &mut label_scratch.labels,
                            ) {
                                if let SampleValue::Histogram(histogram) = &mut value {
                                    runtime.reset_tracker.stamp_histogram(series, histogram);
                                }
                                record_replay_sample(
                                    head,
                                    &mut runtime.completed_windows,
                                    series,
                                    ts_ms,
                                    value,
                                )?;
                                recorded = recorded.saturating_add(1);
                            }
                        }
                    }
                    tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram) => {
                        for dp in &histogram.data_points {
                            let Some(ts_ms) = evaluate_replay_datapoint_time(
                                mode.event_time_policy,
                                dp.time_unix_nano,
                                mode.captured_at_ms,
                                &mut runtime.report,
                            ) else {
                                continue;
                            };
                            let mut value =
                                exponential_histogram_value(dp, histogram.aggregation_temporality);
                            if value.validate_for_storage().is_err() {
                                runtime.report.invalid_typed_datapoints =
                                    runtime.report.invalid_typed_datapoints.saturating_add(1);
                                continue;
                            }
                            if let Some(series) = intern_replay_labelset(
                                labels,
                                &mut runtime.report,
                                resource_attrs,
                                metric_name,
                                &dp.attributes,
                                &mut label_scratch.values,
                                &mut label_scratch.labels,
                            ) {
                                if let SampleValue::ExponentialHistogram(histogram) = &mut value {
                                    runtime
                                        .reset_tracker
                                        .stamp_exponential_histogram(series, histogram);
                                }
                                record_replay_sample(
                                    head,
                                    &mut runtime.completed_windows,
                                    series,
                                    ts_ms,
                                    value,
                                )?;
                                recorded = recorded.saturating_add(1);
                            }
                        }
                    }
                    tonic::metrics::v1::metric::Data::Summary(summary) => {
                        for dp in &summary.data_points {
                            let Some(ts_ms) = evaluate_replay_datapoint_time(
                                mode.event_time_policy,
                                dp.time_unix_nano,
                                mode.captured_at_ms,
                                &mut runtime.report,
                            ) else {
                                continue;
                            };
                            let value = summary_value(dp);
                            if value.validate_for_storage().is_err() {
                                runtime.report.invalid_typed_datapoints =
                                    runtime.report.invalid_typed_datapoints.saturating_add(1);
                                continue;
                            }
                            if let Some(series) = intern_replay_labelset(
                                labels,
                                &mut runtime.report,
                                resource_attrs,
                                metric_name,
                                &dp.attributes,
                                &mut label_scratch.values,
                                &mut label_scratch.labels,
                            ) {
                                record_replay_sample(
                                    head,
                                    &mut runtime.completed_windows,
                                    series,
                                    ts_ms,
                                    value,
                                )?;
                                recorded = recorded.saturating_add(1);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(recorded)
}

struct NumberReplayBatch<'a> {
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [tonic::metrics::v1::NumberDataPoint],
}

#[derive(Default)]
struct ReplayLabelScratch<'a> {
    values: Vec<Box<str>>,
    labels: Vec<TmpLabel<'a>>,
}

fn replay_number_datapoints<'a, L>(
    head: &mut HeadBuffer,
    labels: &mut L,
    batch: NumberReplayBatch<'a>,
    label_scratch: &mut ReplayLabelScratch<'a>,
    mode: ReplayBatchMode<'_>,
    runtime: &mut WalReplayRuntime,
) -> io::Result<u64>
where
    L: LabelSetStore,
{
    let mut recorded = 0u64;
    for dp in batch.points {
        let Some(ts_ms) = evaluate_replay_datapoint_time(
            mode.event_time_policy,
            dp.time_unix_nano,
            mode.captured_at_ms,
            &mut runtime.report,
        ) else {
            continue;
        };
        let series = intern_replay_labelset(
            labels,
            &mut runtime.report,
            batch.resource_attrs,
            batch.metric_name,
            &dp.attributes,
            &mut label_scratch.values,
            &mut label_scratch.labels,
        );
        if let (Some(series), Some(value)) = (series, number_value(dp)) {
            record_replay_sample(head, &mut runtime.completed_windows, series, ts_ms, value)?;
            recorded = recorded.saturating_add(1);
        }
    }
    Ok(recorded)
}

fn record_replay_sample(
    head: &mut HeadBuffer,
    completed_windows: &mut Vec<HeadWindow>,
    series: SeriesRef,
    timestamp_ms: u64,
    value: SampleValue,
) -> io::Result<()> {
    if let Some(window) = head.record_sample(series, timestamp_ms, value)? {
        completed_windows.push(window);
    }
    Ok(())
}

fn evaluate_replay_datapoint_time(
    event_time_policy: &EventTimePolicy,
    time_unix_nano: u64,
    captured_at_ms: i64,
    report: &mut WalReplayReport,
) -> Option<u64> {
    match event_time_policy
        .evaluate(time_unix_nano, captured_at_ms)
        .decision
    {
        DatapointTimeDecision::Accepted(timestamp_ms) => {
            report.policy_accepted_datapoints = report.policy_accepted_datapoints.saturating_add(1);
            Some(timestamp_ms)
        }
        DatapointTimeDecision::DroppedTooOld => {
            report.dropped_too_old_datapoints = report.dropped_too_old_datapoints.saturating_add(1);
            None
        }
        DatapointTimeDecision::DroppedTooFuture => {
            report.dropped_too_future_datapoints =
                report.dropped_too_future_datapoints.saturating_add(1);
            None
        }
        DatapointTimeDecision::MissingTimestamp => {
            report.missing_timestamp_datapoints =
                report.missing_timestamp_datapoints.saturating_add(1);
            None
        }
    }
}

fn intern_replay_labelset<'a, L>(
    labels: &mut L,
    report: &mut WalReplayReport,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [tonic::common::v1::KeyValue],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) -> Option<SeriesRef>
where
    L: LabelSetStore,
{
    let mut interner = ReplayLabelInterner {
        labels,
        skipped_non_scalar_labels: &mut report.skipped_non_scalar_labels,
        labelset_errors: &mut report.labelset_errors,
    };
    intern_otlp_labelset(
        &mut interner,
        resource_attrs,
        metric_name,
        datapoint_attrs,
        scratch_values,
        tmp_labels,
    )
}

struct ReplayLabelInterner<'a, L> {
    labels: &'a mut L,
    skipped_non_scalar_labels: &'a mut u64,
    labelset_errors: &'a mut u64,
}

impl<L> OtlpLabelSetInterner for ReplayLabelInterner<'_, L>
where
    L: LabelSetStore,
{
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {
        *self.skipped_non_scalar_labels = self.skipped_non_scalar_labels.saturating_add(1);
    }

    fn on_intern_error(&mut self, _error: Self::Error) {
        *self.labelset_errors = self.labelset_errors.saturating_add(1);
    }

    fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
        let labels = labels.iter().collect::<Vec<_>>();
        self.labels.intern(labels.as_slice())
    }
}

fn replay_stop_reason(err: &io::Error) -> Option<WalReplayStopReason> {
    match err.kind() {
        ErrorKind::InvalidData => Some(WalReplayStopReason::InvalidRecord),
        ErrorKind::UnexpectedEof => Some(WalReplayStopReason::UnexpectedEof),
        _ => None,
    }
}

fn invalid_data(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use std::io::{self, ErrorKind};

    use super::*;

    #[test]
    fn replay_stop_error_classifies_corrupt_wal_records() {
        assert_eq!(
            replay_stop_reason(&io::Error::new(ErrorKind::InvalidData, "bad wal")),
            Some(WalReplayStopReason::InvalidRecord)
        );
        assert_eq!(
            replay_stop_reason(&io::Error::new(ErrorKind::UnexpectedEof, "torn wal")),
            Some(WalReplayStopReason::UnexpectedEof)
        );
        assert_eq!(
            replay_stop_reason(&io::Error::new(ErrorKind::PermissionDenied, "not replay")),
            None
        );
    }
}
