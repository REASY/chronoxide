use std::fs::File;
use std::io::{self, Error, ErrorKind, Seek, SeekFrom};
use std::path::Path;

use opentelemetry_proto::tonic;

use crate::labels::{KeyValueRef, LabelSetStore, LabelSetStoreError, SeriesRef, TmpLabel};
use crate::otlp::{
    datapoint_time_ms, exponential_histogram_value, histogram_value, number_value, summary_value,
};
use crate::otlp_labelset::{OtlpLabelSetInterner, intern_labelset as intern_otlp_labelset};
use crate::storage::head::HeadBuffer;
use crate::storage::wal::{
    OtlpWalBatch, WalCheckpoint, WalRecord, WalRecordType, decode_checkpoint_record,
    decode_otlp_batch_payload, read_checkpoint_meta, read_wal_record,
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
    pub datapoints_replayed: u64,
    pub skipped_non_scalar_labels: u64,
    pub labelset_errors: u64,
    pub stopped_at_lsn: Option<u64>,
    pub stop_reason: Option<WalReplayStopReason>,
}

pub fn replay_wal_file_into_head<L>(
    wal_path: impl AsRef<Path>,
    head: &mut HeadBuffer,
    labels: &mut L,
) -> io::Result<WalReplayReport>
where
    L: LabelSetStore,
{
    replay_wal_file_into_head_after_checkpoint(wal_path, None, head, labels)
}

pub fn replay_wal_file_into_head_from_checkpoint<L>(
    wal_path: impl AsRef<Path>,
    checkpoint_dir: impl AsRef<Path>,
    head: &mut HeadBuffer,
    labels: &mut L,
) -> io::Result<WalReplayReport>
where
    L: LabelSetStore,
{
    let checkpoint = read_checkpoint_meta(checkpoint_dir)?;
    replay_wal_file_into_head_after_checkpoint(wal_path, checkpoint.as_ref(), head, labels)
}

fn replay_wal_file_into_head_after_checkpoint<L>(
    wal_path: impl AsRef<Path>,
    checkpoint: Option<&WalCheckpoint>,
    head: &mut HeadBuffer,
    labels: &mut L,
) -> io::Result<WalReplayReport>
where
    L: LabelSetStore,
{
    let mut file = File::open(wal_path)?;
    let mut report = WalReplayReport::default();

    if let Some(checkpoint) = checkpoint {
        report.checkpoint_lsn = Some(checkpoint.wal_lsn);
        file.seek(SeekFrom::Start(checkpoint.wal_lsn))?;
        let record = read_wal_record(&mut file)?
            .ok_or_else(|| invalid_data("checkpoint.meta points past WAL EOF"))?;
        let decoded = decode_checkpoint_record(&record)?;
        if &decoded != checkpoint {
            return Err(invalid_data(
                "checkpoint.meta does not match WAL checkpoint record",
            ));
        }
        report.records_read = report.records_read.saturating_add(1);
        report.checkpoints_read = report.checkpoints_read.saturating_add(1);
    }

    report.replay_start_lsn = file.stream_position()?;

    loop {
        let record_lsn = file.stream_position()?;
        let record = match read_wal_record(&mut file) {
            Ok(Some(record)) => record,
            Ok(None) => return Ok(report),
            Err(err) => {
                if let Some(reason) = replay_stop_reason(&err) {
                    report.stopped_at_lsn = Some(record_lsn);
                    report.stop_reason = Some(reason);
                    return Ok(report);
                }
                return Err(err);
            }
        };
        report.records_read = report.records_read.saturating_add(1);
        replay_record(record_lsn, &record, head, labels, &mut report)?;
        if report.stop_reason.is_some() {
            return Ok(report);
        }
    }
}

fn replay_record<L>(
    record_lsn: u64,
    record: &WalRecord,
    head: &mut HeadBuffer,
    labels: &mut L,
    report: &mut WalReplayReport,
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
                        report.stopped_at_lsn = Some(record_lsn);
                        report.stop_reason = Some(reason);
                        return Ok(());
                    }
                    return Err(err);
                }
            };
            let datapoints = replay_otlp_batch(&batch, head, labels, report)?;
            report.batches_replayed = report.batches_replayed.saturating_add(1);
            report.datapoints_replayed = report.datapoints_replayed.saturating_add(datapoints);
        }
        WalRecordType::Checkpoint => {
            let _ = decode_checkpoint_record(record)?;
            report.checkpoints_read = report.checkpoints_read.saturating_add(1);
        }
        WalRecordType::SegmentSealed => {}
    }
    Ok(())
}

fn replay_otlp_batch<L>(
    batch: &OtlpWalBatch,
    head: &mut HeadBuffer,
    labels: &mut L,
    report: &mut WalReplayReport,
) -> io::Result<u64>
where
    L: LabelSetStore,
{
    let mut recorded = 0u64;
    let mut scratch_values: Vec<Box<str>> = Vec::new();
    let mut tmp_labels: Vec<TmpLabel<'_>> = Vec::new();

    for resource_metrics in &batch.request.resource_metrics {
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
                            report,
                            resource_attrs,
                            metric_name,
                            &gauge.data_points,
                            &mut scratch_values,
                            &mut tmp_labels,
                            batch.fallback_ts_ms,
                        )?);
                    }
                    tonic::metrics::v1::metric::Data::Sum(sum) => {
                        recorded = recorded.saturating_add(replay_number_datapoints(
                            head,
                            labels,
                            report,
                            resource_attrs,
                            metric_name,
                            &sum.data_points,
                            &mut scratch_values,
                            &mut tmp_labels,
                            batch.fallback_ts_ms,
                        )?);
                    }
                    tonic::metrics::v1::metric::Data::Histogram(histogram) => {
                        for dp in &histogram.data_points {
                            if let (Some(series), Some(ts_ms)) = (
                                intern_replay_labelset(
                                    labels,
                                    report,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                ),
                                datapoint_time_ms(dp.time_unix_nano, batch.fallback_ts_ms),
                            ) {
                                head.record_sample(
                                    series,
                                    ts_ms,
                                    histogram_value(dp, histogram.aggregation_temporality),
                                )?;
                                recorded = recorded.saturating_add(1);
                            }
                        }
                    }
                    tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram) => {
                        for dp in &histogram.data_points {
                            if let (Some(series), Some(ts_ms)) = (
                                intern_replay_labelset(
                                    labels,
                                    report,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                ),
                                datapoint_time_ms(dp.time_unix_nano, batch.fallback_ts_ms),
                            ) {
                                head.record_sample(
                                    series,
                                    ts_ms,
                                    exponential_histogram_value(
                                        dp,
                                        histogram.aggregation_temporality,
                                    ),
                                )?;
                                recorded = recorded.saturating_add(1);
                            }
                        }
                    }
                    tonic::metrics::v1::metric::Data::Summary(summary) => {
                        for dp in &summary.data_points {
                            if let (Some(series), Some(ts_ms)) = (
                                intern_replay_labelset(
                                    labels,
                                    report,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                ),
                                datapoint_time_ms(dp.time_unix_nano, batch.fallback_ts_ms),
                            ) {
                                head.record_sample(series, ts_ms, summary_value(dp))?;
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

fn replay_number_datapoints<'a, L>(
    head: &mut HeadBuffer,
    labels: &mut L,
    report: &mut WalReplayReport,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [tonic::metrics::v1::NumberDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    fallback_ts_ms: Option<i64>,
) -> io::Result<u64>
where
    L: LabelSetStore,
{
    let mut recorded = 0u64;
    for dp in points {
        let series = intern_replay_labelset(
            labels,
            report,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        );
        if let (Some(series), Some(ts_ms), Some(value)) = (
            series,
            datapoint_time_ms(dp.time_unix_nano, fallback_ts_ms),
            number_value(dp),
        ) {
            head.record_sample(series, ts_ms, value)?;
            recorded = recorded.saturating_add(1);
        }
    }
    Ok(recorded)
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

    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, Self::Error> {
        self.labels.intern(labels)
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
