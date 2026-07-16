use super::*;

fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
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
        mut decoded: ExportMetricsServiceRequest,
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
            &mut decoded,
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

    pub(super) fn evaluate_datapoint_time(
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

    pub(super) fn maybe_report_labelset_stats(&mut self, force: bool) {
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

    pub(super) fn record_head_sample(
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

    pub(super) fn flush_head(&mut self) -> Result<()> {
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
        let mut series_samples = window.into_series_samples()?;
        order_series_samples_for_metric_query(&mut series_samples, &self.labelsets)?;
        profile.seal_decode = seal_decode_start.elapsed();

        if !series_samples.is_empty() {
            if let Some(writer) = &mut self.segment_writer {
                let reserve_start = Instant::now();
                writer.reserve_metric_query_ordered_window_series(
                    start_ms,
                    end_ms,
                    series_samples.len(),
                )?;
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
            if let Some(table) = state.stats.series_table_summary() {
                info!(
                    "head_series_table partition={} windows={} adaptive_windows={} series={} direct_pages={} direct_series={} direct_ratio={:.6} sparse_pages={} sparse_series={} high_refs={} max_page_directory_capacity={} max_sparse_capacity={} max_direct_slot_bytes={} max_direct_reverse_capacity={} max_direct_value_capacity={}",
                    partition,
                    table.windows,
                    table.adaptive_windows,
                    table.series_total,
                    table.direct_pages_total,
                    table.direct_series_total,
                    table.direct_series_ratio,
                    table.sparse_pages_total,
                    table.sparse_series_total,
                    table.refs_above_paged_limit_total,
                    table.max_page_directory_capacity,
                    table.max_sparse_capacity,
                    table.max_direct_slot_index_bytes,
                    table.max_direct_reverse_slot_capacity,
                    table.max_direct_value_capacity,
                );
            }
        }
    }

    pub(super) fn ingest_otlp_metrics(
        &mut self,
        req: &mut ExportMetricsServiceRequest,
        captured_at_ms: i64,
        mut head_state: Option<&mut PartitionHead>,
        record_non_number_samples: bool,
    ) -> Result<DatapointIngestResult> {
        let mut result = DatapointIngestResult::default();

        let mut label_scratch = PreparedOtlpLabelSetScratch::default();

        for resource_metrics in &mut req.resource_metrics {
            let resource_attrs = resource_metrics
                .resource
                .as_ref()
                .map(|res| res.attributes.as_slice())
                .unwrap_or(&[]);
            let prepared_resource_labels = PreparedOtlpResourceLabels::new(resource_attrs);

            for scope_metrics in &mut resource_metrics.scope_metrics {
                for metric in &mut scope_metrics.metrics {
                    let metric_name = metric.name.as_str();
                    let metric_labels = prepared_resource_labels.metric(metric_name);
                    let Some(metric_data) = metric.data.as_mut() else {
                        continue;
                    };

                    match metric_data {
                        tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            let count = ingest_number_datapoints(
                                self,
                                head_state.as_deref_mut(),
                                &metric_labels,
                                &gauge.data_points,
                                &mut label_scratch,
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
                                &metric_labels,
                                &sum.data_points,
                                &mut label_scratch,
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
                            let datapoint_count = hist.data_points.len() as u64;
                            let mut count = DatapointIngestResult::default();
                            for dp in &mut hist.data_points {
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let series = intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    &metric_labels,
                                    &dp.attributes,
                                    &mut label_scratch,
                                )?;
                                if record_non_number_samples
                                    && let Some(series) = series
                                    && let Some(head_state) = head_state.as_deref_mut()
                                {
                                    let explicit_bounds = std::mem::take(&mut dp.explicit_bounds);
                                    let bucket_counts = std::mem::take(&mut dp.bucket_counts);
                                    let mut value = histogram_value_with_buckets(
                                        dp,
                                        hist.aggregation_temporality,
                                        explicit_bounds,
                                        bucket_counts,
                                    );
                                    if let SampleValue::Histogram(histogram) = &mut value {
                                        self.stamp_histogram_reset_hint(series, histogram);
                                    }
                                    self.record_head_sample(head_state, series, ts_ms, value)?;
                                }
                            }
                            self.labelset_stats.record_metric_record(
                                metric_name,
                                MetricDataType::Histogram,
                                datapoint_count,
                                count.accepted,
                            );
                            result.merge(count);
                        }
                        tonic::metrics::v1::metric::Data::ExponentialHistogram(hist) => {
                            let datapoint_count = hist.data_points.len() as u64;
                            let mut count = DatapointIngestResult::default();
                            for dp in &mut hist.data_points {
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let series = intern_labelset(
                                    &mut self.labelsets,
                                    &mut self.labelset_stats,
                                    &metric_labels,
                                    &dp.attributes,
                                    &mut label_scratch,
                                )?;
                                if record_non_number_samples
                                    && let Some(series) = series
                                    && let Some(head_state) = head_state.as_deref_mut()
                                {
                                    let positive =
                                        take_exponential_histogram_buckets(&mut dp.positive);
                                    let negative =
                                        take_exponential_histogram_buckets(&mut dp.negative);
                                    let mut value = exponential_histogram_value_with_buckets(
                                        dp,
                                        hist.aggregation_temporality,
                                        positive,
                                        negative,
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
                                datapoint_count,
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
                                    &metric_labels,
                                    &dp.attributes,
                                    &mut label_scratch,
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
