use crate::app_config::LabelSetStoreKind;
use crate::source::SourceMessageMetadata;
use crate::statistics::{label_tag_stats_from_store, per_key_value_stats_markdown_from_store};
use chrono::{DateTime, Local, Utc};
use chronoxide_core::error::should_log;
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, LabelSetStoreError, NaiveLabelSetStore, SeriesRef, SymbolTable as _,
};
use chronoxide_core::prelude::*;
use opentelemetry_proto::tonic;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{Level, error, info};

mod metrics_ingestion_stats;

use self::metrics_ingestion_stats::{MetricDataType, OtlpMetricsIngestionStats};

type InternedStore = FlatInternedLabelSetStore<DefaultSymbolTable>;
type KeysetStore = KeySetDictEncodedLabelSetStore<DefaultSymbolTable>;

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
    SinkChannelClosed(String),
    CapturedOnly,
    Ok,
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

pub struct OtlpLabelSetProcessor {
    report_interval: Duration,
    labelsets: LabelSetInterner,
    labelset_stats: OtlpMetricsIngestionStats,
}

impl OtlpLabelSetProcessor {
    pub fn new(store: LabelSetStoreKind, report_interval: Duration) -> Self {
        Self {
            report_interval,
            labelsets: LabelSetInterner::new(store),
            labelset_stats: OtlpMetricsIngestionStats::new(),
        }
    }

    fn write_markdown_report(&self) {
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
        let general_stats_time = general_stats_start.elapsed();

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
        let symbol_table_name = std::any::type_name::<DefaultSymbolTable>()
            .split("::")
            .last()
            .unwrap_or("Unknown");
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

        let label_tag_stats_total_time = label_tag_stats_compute_time
            .saturating_add(label_tag_stats_markdown_time)
            .saturating_add(label_tag_stats_append_time);
        // Use just build time, append time is super small
        let per_key_stats_total_time = per_key_stats_build_time;

        let report_build_time = report_start.elapsed();

        let accounted_time = store_stats_time
            .saturating_add(general_stats_time)
            .saturating_add(partition_watermarks_time)
            .saturating_add(latency_stats_time)
            .saturating_add(label_tag_stats_total_time)
            .saturating_add(per_key_stats_total_time)
            .saturating_add(store_section_time)
            .saturating_add(buffer_stats_time)
            .saturating_add(symbol_table_stats_time);
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
            "| Partition Watermarks Build Time | {:?} |\n",
            partition_watermarks_time
        ));
        md.push_str(&format!(
            "| Latency Stats Total Time | {:?} |\n",
            latency_stats_time
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
        let datapoints = self.ingest_otlp_metrics(&decoded);
        let elapsed = start.elapsed();
        self.labelset_stats
            .finish_message(scope, elapsed, datapoints);
        self.labelset_stats.record_partition_watermark(
            metadata.topic,
            metadata.partition,
            metadata.timestamp_ms,
            datapoints,
        );

        self.maybe_report_labelset_stats(false);

        Ok(ProcessResult::Ok)
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
        self.log_partition_watermarks(&ingestion);
        self.report_latency_window();

        self.labelset_stats.reset_window();
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
            "LabelSets store={} messages={} (+{}, {:.2} msg/s in {:?}) datapoints={} (+{}, {:.2} dp/s) series={} symbols={} keysets={} skipped_non_scalar_values={} skipped_labelset_errors={} processing_time={:?} intern_time={:?} build_time={:?} avg_msg_time={:?} avg_dp_time={:?}",
            self.labelsets.kind(),
            ingestion.totals.messages,
            ingestion.window.messages,
            msg_rate,
            ingestion.window.elapsed,
            ingestion.totals.datapoints,
            ingestion.window.datapoints,
            dp_rate,
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

    fn ingest_otlp_metrics(&mut self, req: &ExportMetricsServiceRequest) -> u64 {
        let mut datapoints = 0u64;

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
                                &mut self.labelsets,
                                &mut self.labelset_stats,
                                resource_attrs,
                                metric_name,
                                &gauge.data_points,
                                &mut scratch_values,
                                &mut tmp_labels,
                            );
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Gauge,
                                count,
                            );
                            datapoints += count;
                        }
                        tonic::metrics::v1::metric::Data::Sum(sum) => {
                            let count = ingest_number_datapoints(
                                &mut self.labelsets,
                                &mut self.labelset_stats,
                                resource_attrs,
                                metric_name,
                                &sum.data_points,
                                &mut scratch_values,
                                &mut tmp_labels,
                            );
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Sum,
                                count,
                            );
                            datapoints += count;
                        }
                        tonic::metrics::v1::metric::Data::Histogram(hist) => {
                            let count = hist.data_points.len() as u64;
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Histogram,
                                count,
                            );
                            datapoints += count;
                            for dp in &hist.data_points {
                                intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                );
                            }
                        }
                        tonic::metrics::v1::metric::Data::ExponentialHistogram(hist) => {
                            let count = hist.data_points.len() as u64;
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::ExponentialHistogram,
                                count,
                            );
                            datapoints += count;
                            for dp in &hist.data_points {
                                intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                );
                            }
                        }
                        tonic::metrics::v1::metric::Data::Summary(summary) => {
                            let count = summary.data_points.len() as u64;
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Summary,
                                count,
                            );
                            datapoints += count;
                            for dp in &summary.data_points {
                                intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    resource_attrs,
                                    metric_name,
                                    &dp.attributes,
                                    &mut scratch_values,
                                    &mut tmp_labels,
                                );
                            }
                        }
                    }
                }
            }
        }

        datapoints
    }
}

#[derive(Clone, Copy)]
struct TmpLabel<'a> {
    key: &'a str,
    value: TmpValue<'a>,
    rank: u8,
}

#[derive(Clone, Copy)]
enum TmpValue<'a> {
    Borrowed(&'a str),
    Scratch(usize),
}

impl<'a> TmpValue<'a> {
    fn as_str<'s>(self, scratch_values: &'s [Box<str>]) -> &'s str
    where
        'a: 's,
    {
        match self {
            Self::Borrowed(value) => value,
            Self::Scratch(index) => scratch_values[index].as_ref(),
        }
    }
}

fn ingest_number_datapoints<'a>(
    labelsets: &mut LabelSetInterner,
    stats: &mut OtlpMetricsIngestionStats,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [tonic::metrics::v1::NumberDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) -> u64 {
    for dp in points {
        intern_labelset(
            labelsets,
            stats,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        );
    }
    points.len() as u64
}

fn intern_labelset<'a>(
    labelsets: &mut LabelSetInterner,
    stats: &mut OtlpMetricsIngestionStats,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [tonic::common::v1::KeyValue],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) {
    tmp_labels.clear();
    scratch_values.clear();

    tmp_labels.push(TmpLabel {
        key: chronoxide_core::labels::METRIC_NAME_LABEL,
        value: TmpValue::Borrowed(metric_name),
        rank: 3,
    });

    push_kvs(tmp_labels, scratch_values, stats, resource_attrs, 0);
    push_kvs(tmp_labels, scratch_values, stats, datapoint_attrs, 2);

    tmp_labels.sort_by(|a, b| a.key.cmp(b.key).then_with(|| a.rank.cmp(&b.rank)));

    let mut canonical: Vec<KeyValueRef<'_>> = Vec::with_capacity(tmp_labels.len());
    let scratch_slice: &[Box<str>] = scratch_values.as_slice();

    let mut i = 0;
    while i < tmp_labels.len() {
        let key = tmp_labels[i].key;
        let mut j = i + 1;
        while j < tmp_labels.len() && tmp_labels[j].key == key {
            j += 1;
        }
        let chosen = tmp_labels[j - 1];
        let value = chosen.value.as_str(scratch_slice);
        canonical.push(KeyValueRef {
            key: chosen.key,
            value,
        });
        i = j;
    }

    if let Err(err) = labelsets.intern(&canonical, stats) {
        stats.record_labelset_error();
        if should_log(Level::ERROR, "LabelSetStoreInternError", Instant::now()) {
            error!("LabelSetStore intern failed: {}", err);
        }
    }
}

fn push_kvs<'a>(
    out: &mut Vec<TmpLabel<'a>>,
    scratch_values: &mut Vec<Box<str>>,
    stats: &mut OtlpMetricsIngestionStats,
    kvs: &'a [tonic::common::v1::KeyValue],
    rank: u8,
) {
    out.reserve(kvs.len());

    for kv in kvs {
        let key = kv.key.as_str();
        if key.is_empty() || key == chronoxide_core::labels::METRIC_NAME_LABEL {
            continue;
        }

        let Some(any_value) = kv.value.as_ref() else {
            continue;
        };

        let Some(value) = any_value.value.as_ref() else {
            continue;
        };

        let value = match value {
            tonic::common::v1::any_value::Value::StringValue(value) => {
                TmpValue::Borrowed(value.as_str())
            }
            tonic::common::v1::any_value::Value::BoolValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            tonic::common::v1::any_value::Value::IntValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            tonic::common::v1::any_value::Value::DoubleValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            tonic::common::v1::any_value::Value::BytesValue(_)
            | tonic::common::v1::any_value::Value::ArrayValue(_)
            | tonic::common::v1::any_value::Value::KvlistValue(_) => {
                stats.record_skipped_non_scalar_value();
                continue;
            }
        };

        out.push(TmpLabel { key, value, rank });
    }
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

    fn stats(&self) -> LabelSetStoreStats {
        match self {
            Self::Naive(store) => {
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
            ..Default::default()
        }
    }

    fn histogram_dp(
        attrs: Vec<tonic::common::v1::KeyValue>,
    ) -> tonic::metrics::v1::HistogramDataPoint {
        tonic::metrics::v1::HistogramDataPoint {
            attributes: attrs,
            ..Default::default()
        }
    }

    fn exp_histogram_dp(
        attrs: Vec<tonic::common::v1::KeyValue>,
    ) -> tonic::metrics::v1::ExponentialHistogramDataPoint {
        tonic::metrics::v1::ExponentialHistogramDataPoint {
            attributes: attrs,
            ..Default::default()
        }
    }

    fn summary_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::SummaryDataPoint {
        tonic::metrics::v1::SummaryDataPoint {
            attributes: attrs,
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
    fn format_window_ms_formats_positive_and_negative() {
        assert_eq!(format_window_ms(0), "00:00:00.000");
        assert_eq!(format_window_ms(3_661_001), "01:01:01.001");
        assert_eq!(format_window_ms(-1), "-00:00:00.001");
    }

    #[test]
    fn processor_canonicalizes_labels_and_skips_non_scalar_values() {
        for store in [
            LabelSetStoreKind::FlatInterned,
            LabelSetStoreKind::KeySetDictEncoded,
        ] {
            let mut processor = OtlpLabelSetProcessor::new(store, Duration::from_secs(3600));

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
        let mut processor =
            OtlpLabelSetProcessor::new(LabelSetStoreKind::FlatInterned, Duration::from_secs(3600));

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
}
