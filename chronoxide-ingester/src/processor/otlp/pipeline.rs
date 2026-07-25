use super::*;

fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn record_deferred_flat_series_samples(
    batch: &mut DeferredFlatMetadataBatch<'_, '_, DefaultSymbolTable>,
    series_samples: Vec<(SeriesRef, SeriesSamples)>,
    profile: &mut HeadWindowWriteProfile,
) -> Result<()> {
    // Consume by value deliberately: the outer Vec allocation and each nested
    // sample buffer are released before deferred label metadata is populated.
    for (series, samples) in series_samples {
        match samples {
            SeriesSamples::Float { encoding, samples } => {
                let record_start = Instant::now();
                match encoding {
                    FloatEncoding::Raw => {
                        batch.record_samples_raw_ordered(series, &samples)?;
                    }
                    FloatEncoding::Gorilla
                    | FloatEncoding::Elf
                    | FloatEncoding::Alp
                    | FloatEncoding::AlpRd
                    | FloatEncoding::AlpSpiral
                    | FloatEncoding::AlpRdSpiral
                    | FloatEncoding::Chimp128DuckDB
                    | FloatEncoding::Chimp128Baseline => {
                        batch.record_samples_ordered(series, &samples)?;
                    }
                }
                profile.record_samples += record_start.elapsed();
            }
            SeriesSamples::Int64 { samples, .. } => {
                let conversion_start = Instant::now();
                let float_samples: Vec<(u64, f64)> = samples
                    .into_iter()
                    .map(|(ts, value)| (ts, value as f64))
                    .collect();
                profile.int_conversion += conversion_start.elapsed();

                let record_start = Instant::now();
                batch.record_samples_ordered(series, &float_samples)?;
                profile.record_samples += record_start.elapsed();
            }
            SeriesSamples::Histogram { samples } => {
                let record_start = Instant::now();
                batch.record_histogram_samples_ordered(series, &samples)?;
                profile.record_samples += record_start.elapsed();
            }
            SeriesSamples::ExponentialHistogram { samples } => {
                let record_start = Instant::now();
                batch.record_exponential_histogram_samples_ordered(series, &samples)?;
                profile.record_samples += record_start.elapsed();
            }
            SeriesSamples::Summary { samples } => {
                let record_start = Instant::now();
                batch.record_summary_samples_ordered(series, &samples)?;
                profile.record_samples += record_start.elapsed();
            }
        }
    }
    Ok(())
}

pub(super) fn record_series_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series_samples: Vec<(SeriesRef, SeriesSamples)>,
    profile: &mut HeadWindowWriteProfile,
) -> Result<()> {
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
                    let record_start = Instant::now();
                    record_segment_float_samples(labelsets, writer, series, &samples, false)?;
                    profile.record_samples += record_start.elapsed();
                }
                FloatEncoding::Raw => {
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

                let record_start = Instant::now();
                record_segment_float_samples(labelsets, writer, series, &float_samples, false)?;
                profile.record_samples += record_start.elapsed();
            }
            SeriesSamples::Histogram { samples } => {
                let record_start = Instant::now();
                record_segment_histogram_samples(labelsets, writer, series, &samples)?;
                profile.record_samples += record_start.elapsed();
            }
            SeriesSamples::ExponentialHistogram { samples } => {
                let record_start = Instant::now();
                record_segment_exponential_histogram_samples(labelsets, writer, series, &samples)?;
                profile.record_samples += record_start.elapsed();
            }
            SeriesSamples::Summary { samples } => {
                let record_start = Instant::now();
                record_segment_summary_samples(labelsets, writer, series, &samples)?;
                profile.record_samples += record_start.elapsed();
            }
        }
    }
    Ok(())
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

    fn live_message_tracking_enabled(&self) -> bool {
        self.live_coverage.is_some()
    }

    fn begin_acquired_message(&mut self, sequence: MessageSequence) -> Result<()> {
        if let Some(publisher) = self.live_publisher.as_mut() {
            publisher.prepare_for_next_message()?;
        }
        self.live_coverage
            .as_mut()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "live message hook invoked while coverage tracking is disabled",
                )
            })?
            .begin_message(sequence)
    }

    fn complete_acquired_message(&mut self, sequence: MessageSequence) -> Result<()> {
        self.live_coverage
            .as_mut()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "live message hook invoked while coverage tracking is disabled",
                )
            })?
            .complete_message(sequence)?;

        if self.live_publisher.is_some() {
            let completed = self.pop_completed_message_coverage().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "live publisher did not receive the completed message ledger",
                )
            })?;
            let mut publisher = self
                .live_publisher
                .take()
                .expect("live publisher was observed immediately above");
            let publication = publisher.on_message_boundary(
                sequence,
                completed,
                &mut self.partition_heads,
                &mut self.labelsets,
            );
            self.live_publisher = Some(publisher);
            if let Err(error) = publication
                && should_log(Level::ERROR, "LivePublicationError", Instant::now())
            {
                // Publication errors are reflected atomically in readiness and
                // retried at later message boundaries. They must not stop
                // ingestion or discard the completed message's head state.
                error!(error = %error, "Live publication boundary failed");
            }
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        self.force_report();
        let flush_result = if self.live_publisher.is_some() {
            let mut publisher = self
                .live_publisher
                .take()
                .expect("live publisher was observed immediately above");
            let result = publisher.shutdown(&mut self.partition_heads, &mut self.labelsets);
            self.live_publisher = Some(publisher);
            result
        } else {
            self.flush_head()
        };
        if let Err(err) = &flush_result
            && should_log(Level::ERROR, "HeadFlushError", Instant::now())
        {
            error!("Head flush failed: {}", err);
        }
        if self.shutdown_report {
            self.write_markdown_report();
        }
        flush_result
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
        self.labelset_stats
            .record_invalid_typed_values(datapoints.invalid_typed);
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
        let mut head = HeadBuffer::new(head_config.clone())?;
        if self.live_coverage.is_some() {
            head.enable_live_coverage_tracking()?;
        }
        let stats = HeadBufferStats::new();
        self.partition_heads.insert(
            partition.clone(),
            PartitionHead {
                head,
                stats,
                seal_ready_ranges: BTreeSet::new(),
            },
        );
        Ok(())
    }

    pub(super) fn record_head_sample(
        &mut self,
        head_state: &mut PartitionHead,
        series: SeriesRef,
        ts_ms: u64,
        mut value: SampleValue,
        order: Option<RecordedSampleOrder>,
    ) -> Result<()> {
        enum PreparedResetUpdate {
            Histogram(chronoxide_core::otlp_reset::PreparedHistogramReset),
            ExponentialHistogram(chronoxide_core::otlp_reset::PreparedExponentialHistogramReset),
        }

        // Compute the hint and reserve the tracker update before any coverage
        // or head mutation. The semantic reset history changes only after the
        // sample encoder reports that this sample was actually stored.
        let prepared_reset = match &mut value {
            SampleValue::Histogram(histogram) => Some(PreparedResetUpdate::Histogram(
                self.otlp_reset_tracker
                    .prepare_histogram(series, histogram)?,
            )),
            SampleValue::ExponentialHistogram(histogram) => {
                Some(PreparedResetUpdate::ExponentialHistogram(
                    self.otlp_reset_tracker
                        .prepare_exponential_histogram(series, histogram)?,
                ))
            }
            SampleValue::Float(_) | SampleValue::Int64(_) | SampleValue::Summary(_) => None,
        };
        let prepared_coverage = match order {
            Some(order) => {
                let tracking = self.live_coverage.as_mut().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "sample order supplied while live coverage tracking is disabled",
                    )
                })?;
                Some(tracking.prepare_contribution(order, series, ts_ms, &value)?)
            }
            None => {
                if self.live_coverage.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "live coverage sample omitted its recorded order",
                    )
                    .into());
                }
                None
            }
        };
        // Reserve ownership before recording can rotate a completed window
        // out of the mutable slot. A representable allocation failure
        // therefore rejects this sample before coverage or head state changes.
        let retained_window_slot = if prepared_coverage.is_some() {
            Some(
                head_state
                    .head
                    .try_reserve_retained_window_for_publication()?,
            )
        } else {
            None
        };
        let call_start = Instant::now();
        let outcome = match prepared_coverage.as_ref() {
            Some((contribution, _candidate)) => {
                head_state
                    .head
                    .record_sample_with_coverage(series, ts_ms, value, *contribution)?
            }
            None => head_state
                .head
                .record_sample_with_outcome(series, ts_ms, value)?,
        };
        let recorded = outcome.recorded;
        let elapsed = call_start.elapsed();
        head_state
            .stats
            .record_call(elapsed, 1, usize::from(outcome.completed_window.is_some()));

        if recorded {
            if let Some((_contribution, candidate)) = prepared_coverage {
                self.live_coverage
                    .as_mut()
                    .expect("tracked contribution requires live coverage")
                    .commit_contribution(candidate);
            }
            match prepared_reset {
                Some(PreparedResetUpdate::Histogram(prepared)) => {
                    self.otlp_reset_tracker.commit_histogram(prepared);
                }
                Some(PreparedResetUpdate::ExponentialHistogram(prepared)) => {
                    self.otlp_reset_tracker
                        .commit_exponential_histogram(prepared);
                }
                None => {}
            }
            self.labelset_stats.record_recorded_samples(1);
        }

        let window = if let Some(window) = outcome.completed_window {
            head_state.stats.record_rotated_window(&window);
            Some(window)
        } else {
            None
        };

        if let Some(window) = window {
            if self.live_coverage.is_some() {
                let range = (window.start_ms, window.end_ms);
                head_state.head.retain_completed_window_for_publication(
                    retained_window_slot.expect("live recording reserved a retained-window slot"),
                    window,
                )?;
                head_state.seal_ready_ranges.insert(range);
            } else {
                let ooo = head_state
                    .head
                    .take_out_of_order_window(window.start_ms, window.end_ms);
                if let Some(ooo) = &ooo {
                    head_state.stats.record_window(ooo);
                }
                self.write_head_window_samples(window, ooo, SegmentPayloadLane::InOrder)?;
            }
        }
        if recorded && let Some(publisher) = self.live_publisher.as_mut() {
            // The sample and any rotated-window ownership are now fully
            // committed. Age the old immutable root from this mutation, not
            // from the later end-of-message publication boundary.
            publisher.on_head_mutation(Instant::now())?;
        }
        Ok(())
    }

    pub(super) fn next_sample_order(&mut self) -> Result<Option<RecordedSampleOrder>> {
        if self.live_coverage.is_none() {
            return Ok(None);
        }
        if let Some(publisher) = self.live_publisher.as_mut() {
            publisher.reserve_expected_order_slot()?;
        }
        self.live_coverage
            .as_mut()
            .expect("live coverage was observed immediately above")
            .next_sample_order()
            .map(Some)
    }

    fn log_invalid_typed_value(kind: &'static str, error: &io::Error) {
        if should_log(Level::WARN, "InvalidTypedDatapoint", Instant::now()) {
            warn!(kind, error = %error, "Rejecting invalid typed OTLP datapoint");
        }
    }

    pub(super) fn flush_head(&mut self) -> Result<()> {
        if self.partition_heads.is_empty() {
            return Ok(());
        }
        let mut partitions: Vec<_> = self.partition_heads.keys().cloned().collect();
        partitions.sort_unstable();

        let mut grouped = std::collections::BTreeMap::<
            (u64, u64, PartitionKey),
            (Option<HeadWindow>, Option<HeadWindow>),
        >::new();
        for partition in partitions {
            let state = self
                .partition_heads
                .get_mut(&partition)
                .expect("partition key was collected from the same map");
            for window in state.head.drain_windows() {
                state.stats.record_window(&window);
                let key = (window.start_ms, window.end_ms, partition.clone());
                let lanes = grouped.entry(key).or_default();
                let target = if window.is_out_of_order() {
                    &mut lanes.1
                } else {
                    &mut lanes.0
                };
                if target.replace(window).is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "head drain produced duplicate windows for one range, partition, and lane",
                    )
                    .into());
                }
            }
        }

        // Grouping establishes one total `(range, partition)` order despite
        // randomized HashMap iteration. Co-resident active and OOO lanes seal
        // once into chunks.bin; an OOO lane with no matching mutable active
        // window belongs to an already-passed range and seals into
        // ooo_chunks.bin as a newer overlapping segment.
        for ((_start_ms, _end_ms, _partition), (active, ooo)) in grouped {
            match (active, ooo) {
                (Some(active), ooo) => {
                    self.write_head_window_samples(active, ooo, SegmentPayloadLane::InOrder)?;
                }
                (None, Some(ooo)) => {
                    self.write_head_window_samples(ooo, None, SegmentPayloadLane::OutOfOrder)?;
                }
                (None, None) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "head drain produced an empty window group",
                    )
                    .into());
                }
            }
        }
        if let Some(writer) = &mut self.segment_writer {
            writer.flush()?;
        }
        Ok(())
    }

    fn write_head_window_samples(
        &mut self,
        window: HeadWindow,
        preseal_ooo: Option<HeadWindow>,
        payload_lane: SegmentPayloadLane,
    ) -> Result<()> {
        if self.segment_writer.is_none() {
            return Ok(());
        }
        let profile_start = Instant::now();
        let start_ms = window.start_ms;
        let end_ms = window.end_ms;
        let datapoints = window.datapoints.saturating_add(
            preseal_ooo
                .as_ref()
                .map(|ooo| ooo.datapoints)
                .unwrap_or_default(),
        );
        let mut profile = HeadWindowWriteProfile {
            start_ms,
            end_ms,
            datapoints,
            ..HeadWindowWriteProfile::default()
        };

        let seal_decode_start = Instant::now();
        let (mut series_samples, unique_series_count) = match payload_lane {
            SegmentPayloadLane::InOrder => window
                .into_series_samples_with_ooo(preseal_ooo)?
                .into_parts(),
            SegmentPayloadLane::OutOfOrder => {
                if !window.is_out_of_order() || preseal_ooo.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "OOO segment payload requires one standalone out-of-order head window",
                    )
                    .into());
                }
                let series_samples = window.into_deduped_series_samples()?;
                let unique_series_count = series_samples.len();
                (series_samples, unique_series_count)
            }
        };
        let has_multi_kind_series = series_samples.len() != unique_series_count;
        let canonical_label_counts =
            order_series_samples_for_metric_query(&mut series_samples, &self.labelsets)?;
        if canonical_label_counts.len() != series_samples.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric-order label counts do not match the ordered series",
            )
            .into());
        }
        let series_count = unique_series_count as u64;
        profile.series = series_count;
        profile.seal_decode = seal_decode_start.elapsed();

        let record_profile_before = self
            .segment_writer
            .as_ref()
            .map(SegmentWriter::record_profile);

        if !series_samples.is_empty() {
            let labelsets = &self.labelsets;
            let writer = self
                .segment_writer
                .as_mut()
                .expect("segment writer presence was checked above");
            writer.set_next_segment_payload_lane(payload_lane)?;
            if has_multi_kind_series {
                // Deferred flat metadata intentionally models exactly one
                // record call per source series. A rare canonical series with
                // multiple native kinds uses the generic writer path, which
                // merges the kinds into one metadata row before final ordering.
                let reserve_start = Instant::now();
                writer.reserve_window_series(start_ms, end_ms, unique_series_count)?;
                profile.series_reserve = reserve_start.elapsed();
                record_series_samples(labelsets, writer, series_samples, &mut profile)?;
            } else if let Some(flat) = labelsets.as_flat_interned() {
                let reserve_start = Instant::now();
                let mut batch = writer.begin_metric_query_ordered_flat_metadata_batch(
                    start_ms,
                    end_ms,
                    series_samples.iter().map(|(series, _)| *series),
                    flat,
                )?;
                profile.series_reserve = reserve_start.elapsed();

                record_deferred_flat_series_samples(&mut batch, series_samples, &mut profile)?;
                let metadata_start = Instant::now();
                batch.finish(&canonical_label_counts)?;
                profile.record_samples += metadata_start.elapsed();
            } else {
                let reserve_start = Instant::now();
                writer.reserve_metric_query_ordered_window_series_with_label_counts(
                    start_ms,
                    end_ms,
                    series_samples
                        .iter()
                        .zip(canonical_label_counts.iter().copied())
                        .map(|((series, _), label_count)| (*series, label_count)),
                )?;
                profile.series_reserve = reserve_start.elapsed();
                record_series_samples(labelsets, writer, series_samples, &mut profile)?;
            }
        }
        drop(canonical_label_counts);
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
            "LabelSets store={} messages={} (+{}, {:.2} msg/s in {:?}) observed_datapoints={} (+{}, {:.2} dp/s) accepted_datapoints={} (+{}, {:.2} dp/s) recorded_samples={} missing_number_values={} invalid_typed_values={} dropped_too_old={} dropped_too_future={} missing_timestamp={} series={} symbols={} keysets={} skipped_non_scalar_values={} skipped_labelset_errors={} processing_time={:?} intern_time={:?} build_time={:?} avg_msg_time={:?} avg_observed_dp_time={:?}",
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
            ingestion.totals.datapoint_storage.invalid_typed_values,
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

        let fmt = |dist: Option<crate::statistics::DistI64>| {
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
        partitions.sort_by_key(|(partition, _)| *partition);

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
                    "head_series_table partition={} windows={} in_order_windows={} in_order_rotations={} out_of_order_windows={} adaptive_windows={} series={} direct_pages={} direct_series={} direct_ratio={:.6} sparse_pages={} sparse_series={} high_refs={} max_page_directory_capacity={} max_sparse_capacity={} max_direct_slot_bytes={} max_direct_reverse_capacity={} max_direct_value_capacity={}",
                    partition,
                    table.windows,
                    table.in_order_windows,
                    table.in_order_rotations,
                    table.out_of_order_windows,
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
            let last = state.head.last_timestamp_table_stats();
            info!(
                "last_timestamp_table partition={} adaptive={} series={} dense_pages={} dense_series={} dense_ratio={:.6} sparse_pages={} sparse_series={} high_refs={} page_directory_len={} page_directory_capacity={} sparse_capacity={} paged_allocated_bytes={}",
                partition,
                last.adaptive,
                last.series,
                last.dense_pages,
                last.dense_series,
                if last.series == 0 {
                    0.0
                } else {
                    last.dense_series as f64 / last.series as f64
                },
                last.sparse_pages,
                last.sparse_series,
                last.refs_above_paged_limit,
                last.page_directory_len,
                last.page_directory_capacity,
                last.sparse_capacity,
                last.paged_allocated_bytes,
            );
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
                                let order = self.next_sample_order()?;
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let mut value = if record_non_number_samples && head_state.is_some()
                                {
                                    let explicit_bounds = std::mem::take(&mut dp.explicit_bounds);
                                    let bucket_counts = std::mem::take(&mut dp.bucket_counts);
                                    Some(histogram_value_with_buckets(
                                        dp,
                                        hist.aggregation_temporality,
                                        explicit_bounds,
                                        bucket_counts,
                                    ))
                                } else {
                                    None
                                };
                                if let Some(value) = value.as_ref()
                                    && let Err(error) = value.validate_for_storage()
                                {
                                    count.invalid_typed = count.invalid_typed.saturating_add(1);
                                    Self::log_invalid_typed_value("histogram", &error);
                                    continue;
                                }
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
                                    && let Some(value) = value.take()
                                {
                                    self.record_head_sample(
                                        head_state, series, ts_ms, value, order,
                                    )?;
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
                                let order = self.next_sample_order()?;
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let mut value = if record_non_number_samples && head_state.is_some()
                                {
                                    let positive =
                                        take_exponential_histogram_buckets(&mut dp.positive);
                                    let negative =
                                        take_exponential_histogram_buckets(&mut dp.negative);
                                    Some(exponential_histogram_value_with_buckets(
                                        dp,
                                        hist.aggregation_temporality,
                                        positive,
                                        negative,
                                    ))
                                } else {
                                    None
                                };
                                if let Some(value) = value.as_ref()
                                    && let Err(error) = value.validate_for_storage()
                                {
                                    count.invalid_typed = count.invalid_typed.saturating_add(1);
                                    Self::log_invalid_typed_value("exponential_histogram", &error);
                                    continue;
                                }
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
                                    && let Some(value) = value.take()
                                {
                                    self.record_head_sample(
                                        head_state, series, ts_ms, value, order,
                                    )?;
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
                                let order = self.next_sample_order()?;
                                let decision =
                                    self.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
                                let Some(ts_ms) = count.record(decision) else {
                                    continue;
                                };
                                let value = record_non_number_samples.then(|| summary_value(dp));
                                if let Some(value) = value.as_ref()
                                    && let Err(error) = value.validate_for_storage()
                                {
                                    count.invalid_typed = count.invalid_typed.saturating_add(1);
                                    Self::log_invalid_typed_value("summary", &error);
                                    continue;
                                }
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
                                    && let Some(value) = value
                                {
                                    self.record_head_sample(
                                        head_state, series, ts_ms, value, order,
                                    )?;
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
