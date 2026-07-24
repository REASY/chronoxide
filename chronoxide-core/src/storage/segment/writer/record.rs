use super::*;

macro_rules! record_typed_samples_ordered {
    ($visitor:ident, $interned:ident, $value:ty, $kind:expr, $append:path) => {
        pub fn $visitor<F>(
            &mut self,
            series: SeriesRef,
            samples: &[(u64, $value)],
            visit_labels: F,
        ) -> io::Result<()>
        where
            F: FnMut(&mut dyn FnMut(&str, &str)),
        {
            self.record_typed_samples_ordered_with_label_visitor(
                series,
                samples,
                $kind,
                $append,
                visit_labels,
            )
        }

        pub fn $interned<S: SymbolTable>(
            &mut self,
            series: SeriesRef,
            samples: &[(u64, $value)],
            labelsets: &FlatInternedLabelSetStore<S>,
        ) -> io::Result<()> {
            self.record_typed_samples_ordered_with_flat_interned_labels(
                series, samples, $kind, $append, labelsets,
            )
        }
    };
}

macro_rules! record_float_samples_with_label_visitor {
    ($method:ident, $delegate:ident, $raw:expr) => {
        pub fn $method<F>(
            &mut self,
            series: SeriesRef,
            samples: &[(u64, f64)],
            visit_labels: F,
        ) -> io::Result<()>
        where
            F: FnMut(&mut dyn FnMut(&str, &str)),
        {
            self.$delegate(series, samples, $raw, visit_labels)
        }
    };
}
impl SegmentWriter {
    pub fn reserve_window_series(
        &mut self,
        start_ms: u64,
        end_ms: u64,
        series: usize,
    ) -> io::Result<()> {
        self.ensure_active_window(start_ms, end_ms)?;
        let Some(active) = &mut self.active else {
            return Ok(());
        };
        if active.recording_closed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot reserve series after segment recording is complete",
            ));
        }
        if active.metric_query_ordered_batch_seen {
            active.metric_query_ordered_input = false;
            active.metric_query_ordered_series_remaining = 0;
        }
        let additional = series.saturating_sub(active.series_map.len());
        active
            .series_map
            .try_reserve(additional)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        active.series_entries.try_reserve_series(additional)?;
        active.chunk_entries.reserve_series(additional);
        Ok(())
    }

    pub fn reserve_metric_query_ordered_window_series(
        &mut self,
        start_ms: u64,
        end_ms: u64,
        series: usize,
    ) -> io::Result<()> {
        self.reserve_window_series(start_ms, end_ms, series)?;
        let Some(active) = &mut self.active else {
            return Ok(());
        };
        active.metric_query_ordered_input = active.series_entries.is_empty()
            && active.datapoints == 0
            && !active.metric_query_ordered_batch_seen;
        active.metric_query_ordered_series_remaining = if active.metric_query_ordered_input {
            series.saturating_sub(active.series_map.len())
        } else {
            0
        };
        active.metric_query_ordered_batch_seen = true;
        Ok(())
    }

    /// Reserves one independently metric-query-ordered batch and its exact
    /// canonical label-pair inventory.
    ///
    /// `rows` must contain each source series at most once. The exact label
    /// counts are aligned with the order in which the rows will be recorded.
    pub fn reserve_metric_query_ordered_window_series_with_label_counts<I>(
        &mut self,
        start_ms: u64,
        end_ms: u64,
        rows: I,
    ) -> io::Result<()>
    where
        I: IntoIterator<Item = (SeriesRef, u32)>,
        I::IntoIter: ExactSizeIterator,
    {
        self.ensure_active_window(start_ms, end_ms)?;
        let Some(active) = &mut self.active else {
            return Ok(());
        };
        if active.recording_closed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot reserve series after segment recording is complete",
            ));
        }
        let rows = rows.into_iter();
        let fresh = active.series_entries.is_empty()
            && active.datapoints == 0
            && !active.metric_query_ordered_batch_seen;
        let mut additional_series = 0usize;
        let mut additional_label_pairs = 0usize;
        for (source_ref, label_count) in rows {
            if let Some(&local_ref) = active.series_map.get(&source_ref.get()) {
                if active.series_entries.metadata_present(local_ref as usize)? {
                    continue;
                }
            } else {
                additional_series = additional_series.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "series count exceeds usize")
                })?;
            }
            additional_label_pairs = additional_label_pairs
                .checked_add(usize::try_from(label_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "series label count exceeds usize",
                    )
                })?)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "segment label-pair count exceeds usize",
                    )
                })?;
        }

        let total_series = active
            .series_map
            .len()
            .checked_add(additional_series)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "series count exceeds usize")
            })?;
        if total_series > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment series count exceeds u32",
            ));
        }

        active
            .series_map
            .try_reserve(additional_series)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        active
            .series_entries
            .try_reserve_series_exact(additional_series)?;
        active
            .series_entries
            .try_reserve_label_page_directory(additional_label_pairs)?;
        active.chunk_entries.reserve_series(additional_series);
        active.metric_query_ordered_input = fresh;
        active.metric_query_ordered_series_remaining = if fresh { additional_series } else { 0 };
        active.metric_query_ordered_batch_seen = true;
        Ok(())
    }

    /// Starts a fresh metric-query-ordered batch whose flat label metadata is
    /// deferred until [`DeferredFlatMetadataBatch::finish`].
    pub fn begin_metric_query_ordered_flat_metadata_batch<'writer, 'labels, S, I>(
        &'writer mut self,
        start_ms: u64,
        end_ms: u64,
        ordered_series: I,
        labelsets: &'labels FlatInternedLabelSetStore<S>,
    ) -> io::Result<DeferredFlatMetadataBatch<'writer, 'labels, S>>
    where
        S: SymbolTable,
        I: IntoIterator<Item = SeriesRef>,
        I::IntoIter: ExactSizeIterator,
    {
        let result = self.reserve_metric_query_ordered_flat_metadata_rows(
            start_ms,
            end_ms,
            ordered_series,
            labelsets.buffer_stats().series_len,
        );
        if let Err(error) = result {
            self.abort_deferred_flat_metadata_batch();
            return Err(error);
        }
        Ok(DeferredFlatMetadataBatch {
            writer: self,
            labelsets,
            finished: false,
        })
    }

    fn reserve_metric_query_ordered_flat_metadata_rows<I>(
        &mut self,
        start_ms: u64,
        end_ms: u64,
        ordered_series: I,
        source_series_len: usize,
    ) -> io::Result<()>
    where
        I: IntoIterator<Item = SeriesRef>,
        I::IntoIter: ExactSizeIterator,
    {
        self.ensure_active_window(start_ms, end_ms)?;
        let Some(active) = &mut self.active else {
            return Ok(());
        };
        let fresh = active.series_entries.is_empty()
            && active.datapoints == 0
            && !active.metric_query_ordered_batch_seen;
        if !fresh {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "deferred flat metadata requires a fresh segment window",
            ));
        }
        let ordered_series = ordered_series.into_iter();
        let series = ordered_series.len();
        active.metric_query_ordered_input = true;
        active.metric_query_ordered_series_remaining = series;
        active.metric_query_ordered_batch_seen = true;
        active.deferred_flat_label_metadata = true;
        if series > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment series count exceeds u32",
            ));
        }

        active
            .series_map
            .try_reserve(series)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        active.series_entries.try_reserve_series_exact(series)?;
        active.chunk_entries.reserve_series(series);

        for source_series in ordered_series {
            let source_ref = source_series.get();
            if source_ref as usize >= source_series_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "deferred flat metadata source ref is absent from its label store",
                ));
            }
            let local_ref = u32::try_from(active.series_entries.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "deferred flat metadata local ref exceeds u32",
                )
            })?;
            if active.series_map.insert(source_ref, local_ref).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "deferred flat metadata batch contains a duplicate source ref",
                ));
            }
            active
                .series_entries
                .push_placeholder(u64::from(source_ref), 0)?;
            active.chunk_entries.push_empty_series();
        }
        if active.series_entries.len() != series {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "deferred flat metadata iterator length changed while reserving",
            ));
        }
        Ok(())
    }

    fn apply_deferred_metric_query_ordered_flat_metadata_inner<S: SymbolTable>(
        &mut self,
        canonical_label_counts: &[u32],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        let Some(active) = &mut self.active else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "deferred flat metadata has no active segment",
            ));
        };
        if !active.deferred_flat_label_metadata {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment has no deferred flat metadata batch",
            ));
        }
        if !active.metric_query_ordered_input || active.metric_query_ordered_series_remaining != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata batch is incomplete or no longer metric-query ordered",
            ));
        }
        if canonical_label_counts.len() != active.series_entries.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata label counts do not match the recorded series",
            ));
        }
        if active.series_map.len() != active.series_entries.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata source map does not match its series rows",
            ));
        }
        for index in 0..active.series_entries.len() {
            if active.series_entries.metadata_present(index)? {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "deferred flat metadata batch contains an already-populated series",
                ));
            }
            active.series_entries.placeholder_source_ref(index)?;
            if active.series_entries.kind_mask(index)? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "deferred flat metadata series was not recorded",
                ));
            }
        }
        let series_map = std::mem::take(&mut active.series_map);
        active.recording_closed = true;
        drop(series_map);

        let additional_label_pairs =
            canonical_label_counts
                .iter()
                .try_fold(0usize, |total, &count| {
                    total
                        .checked_add(usize::try_from(count).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "series label count exceeds usize",
                            )
                        })?)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "segment label-pair count exceeds usize",
                            )
                        })
                })?;
        let initial_label_pairs = active.series_entries.label_pair_count();
        active
            .series_entries
            .try_reserve_label_page_directory(additional_label_pairs)?;

        for (index, &label_count) in canonical_label_counts.iter().enumerate() {
            let source_ref = active.series_entries.placeholder_source_ref(index)?;
            let local_ref = u32::try_from(index).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "deferred flat metadata local ref exceeds u32",
                )
            })?;
            let kind_mask = active.series_entries.kind_mask(index)?;
            apply_flat_interned_label_metadata_counted(
                active,
                local_ref,
                kind_mask,
                SeriesRef::new(source_ref),
                usize::try_from(label_count).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "series label count exceeds usize",
                    )
                })?,
                labelsets,
            )?;
        }

        let expected_label_pairs = initial_label_pairs
            .checked_add(additional_label_pairs)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "segment label-pair count exceeds usize",
                )
            })?;
        if active.series_entries.label_pair_count() != expected_label_pairs {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata did not populate its exact label inventory",
            ));
        }
        active.deferred_flat_label_metadata = false;
        Ok(())
    }

    pub fn reserve_series_for_timestamp(
        &mut self,
        timestamp_ms: u64,
        series: usize,
    ) -> io::Result<()> {
        let duration_ms = self.segment_duration_ms()?;
        let (start_ms, end_ms) = segment_window(timestamp_ms, duration_ms);
        self.reserve_window_series(start_ms, end_ms, series)
    }

    pub fn record_sample(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: f64,
    ) -> io::Result<()> {
        self.record_samples(series, &[(timestamp_ms, value)])
    }

    pub fn record_sample_raw(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: f64,
    ) -> io::Result<()> {
        self.record_samples_raw(series, &[(timestamp_ms, value)])
    }

    pub fn record_samples(&mut self, series: SeriesRef, samples: &[(u64, f64)]) -> io::Result<()> {
        self.record_float_samples(series, None, samples, false)
    }

    pub fn record_samples_with_labels(
        &mut self,
        series: SeriesRef,
        labels: &[(String, String)],
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let metadata = canonical_segment_metadata(labels);
        self.record_float_samples(series, Some(&metadata), samples, false)
    }

    pub fn record_samples_with_metadata(
        &mut self,
        series: SeriesRef,
        metadata: &SegmentSeriesMetadata,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples_with_metadata_source(
            series,
            samples,
            false,
            |active, local_ref| apply_segment_metadata(active, local_ref, metadata),
        )
    }

    pub fn record_samples_raw_with_metadata(
        &mut self,
        series: SeriesRef,
        metadata: &SegmentSeriesMetadata,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples_with_metadata_source(
            series,
            samples,
            true,
            |active, local_ref| apply_segment_metadata(active, local_ref, metadata),
        )
    }

    record_float_samples_with_label_visitor!(
        record_samples_with_label_visitor,
        record_float_samples_with_label_visitor,
        false
    );
    record_float_samples_with_label_visitor!(
        record_samples_ordered_with_label_visitor,
        record_float_samples_ordered_with_label_visitor,
        false
    );

    pub fn record_samples_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_float_samples_ordered_with_flat_interned_labels(
            series, samples, false, labelsets,
        )
    }

    record_float_samples_with_label_visitor!(
        record_samples_raw_with_label_visitor,
        record_float_samples_with_label_visitor,
        true
    );
    record_float_samples_with_label_visitor!(
        record_samples_raw_ordered_with_label_visitor,
        record_float_samples_ordered_with_label_visitor,
        true
    );

    pub fn record_samples_raw_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_float_samples_ordered_with_flat_interned_labels(
            series, samples, true, labelsets,
        )
    }

    pub fn record_i64_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, i64)],
        mut visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_i64_samples_ordered_with_metadata_source(
            series,
            samples,
            |active, local_ref| {
                apply_label_visitor_with_kind(
                    active,
                    local_ref,
                    SERIES_KIND_INT64,
                    &mut visit_labels,
                )
            },
        )
    }

    record_typed_samples_ordered!(
        record_histogram_samples_ordered_with_label_visitor,
        record_histogram_samples_ordered_with_flat_interned_labels,
        HistogramValue,
        SERIES_KIND_HISTOGRAM,
        ChunkWriter::append_histogram_chunk_ordered
    );
    record_typed_samples_ordered!(
        record_exponential_histogram_samples_ordered_with_label_visitor,
        record_exponential_histogram_samples_ordered_with_flat_interned_labels,
        ExponentialHistogramValue,
        SERIES_KIND_EXPONENTIAL_HISTOGRAM,
        ChunkWriter::append_exponential_histogram_chunk_ordered
    );
    record_typed_samples_ordered!(
        record_summary_samples_ordered_with_label_visitor,
        record_summary_samples_ordered_with_flat_interned_labels,
        SummaryValue,
        SERIES_KIND_SUMMARY,
        ChunkWriter::append_summary_chunk_ordered
    );

    fn record_float_samples_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        mut visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_with_metadata_source(series, samples, raw, |active, local_ref| {
            apply_label_visitor(active, local_ref, &mut visit_labels)
        })
    }

    fn record_float_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        mut visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_ordered_with_metadata_source(
            series,
            samples,
            raw,
            |active, local_ref| apply_label_visitor(active, local_ref, &mut visit_labels),
        )
    }

    fn record_float_samples_ordered_with_flat_interned_labels<S: SymbolTable>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()> {
        self.record_float_samples_ordered_with_metadata_source(
            series,
            samples,
            raw,
            |active, local_ref| {
                apply_flat_interned_label_metadata(
                    active,
                    local_ref,
                    SERIES_KIND_FLOAT,
                    series,
                    labelsets,
                )
            },
        )
    }

    fn record_typed_samples_ordered_with_label_visitor<T, F, A>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, T)],
        kind_mask: u8,
        append_chunk: A,
        mut visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
        A: Fn(&mut ChunkWriter, u32, &[(u64, T)]) -> io::Result<ChunkIndexEntry>,
    {
        self.record_typed_samples_ordered_with_metadata_source(
            series,
            samples,
            kind_mask,
            append_chunk,
            |active, local_ref| {
                apply_label_visitor_with_kind(active, local_ref, kind_mask, &mut visit_labels)
            },
        )
    }

    fn record_typed_samples_ordered_with_flat_interned_labels<T, S, A>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, T)],
        kind_mask: u8,
        append_chunk: A,
        labelsets: &FlatInternedLabelSetStore<S>,
    ) -> io::Result<()>
    where
        S: SymbolTable,
        A: Fn(&mut ChunkWriter, u32, &[(u64, T)]) -> io::Result<ChunkIndexEntry>,
    {
        self.record_typed_samples_ordered_with_metadata_source(
            series,
            samples,
            kind_mask,
            append_chunk,
            |active, local_ref| {
                apply_flat_interned_label_metadata(active, local_ref, kind_mask, series, labelsets)
            },
        )
    }

    fn record_float_samples(
        &mut self,
        series: SeriesRef,
        metadata: Option<&SegmentSeriesMetadata>,
        samples: &[(u64, f64)],
        raw: bool,
    ) -> io::Result<()> {
        self.record_float_samples_with_metadata_source(series, samples, raw, |active, local_ref| {
            if let Some(metadata) = metadata {
                apply_segment_metadata(active, local_ref, metadata)?;
            }
            Ok(())
        })
    }

    fn record_float_samples_with_metadata_source<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        apply_metadata: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut ActiveSegment, u32) -> io::Result<()>,
    {
        if samples.is_empty() {
            return Ok(());
        }

        let mut ordered: Vec<(u64, f64)> = samples.to_vec();
        ordered.sort_by_key(|(ts, _)| *ts);
        self.record_float_samples_ordered_with_metadata_source(
            series,
            &ordered,
            raw,
            apply_metadata,
        )
    }

    fn record_float_samples_ordered_with_metadata_source<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
        mut apply_metadata: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut ActiveSegment, u32) -> io::Result<()>,
    {
        if samples.is_empty() {
            return Ok(());
        }
        validate_ordered_samples(samples)?;

        let duration_ms = self.segment_duration_ms()?;
        let mut idx = 0usize;
        while idx < samples.len() {
            let ts = samples[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < samples.len() {
                let next_start = segment_window(samples[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_FLOAT)?;
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref)?;
            let metadata = metadata_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = if raw {
                active
                    .chunks
                    .append_float_chunk_raw_ordered(local_ref, &samples[idx..end_idx])?
            } else {
                active
                    .chunks
                    .append_float_chunk_ordered(local_ref, &samples[idx..end_idx])?
            };
            let chunk_append = chunk_append_start.elapsed();

            let bookkeeping_start = Instant::now();
            active.chunk_entries.push_entry(local_ref as usize, entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata,
                    chunk_append,
                    label_time_range: Duration::ZERO,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
            idx = end_idx;
        }

        Ok(())
    }

    fn record_typed_samples_ordered_with_metadata_source<T, F, A>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, T)],
        kind_mask: u8,
        append_chunk: A,
        mut apply_metadata: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut ActiveSegment, u32) -> io::Result<()>,
        A: Fn(&mut ChunkWriter, u32, &[(u64, T)]) -> io::Result<ChunkIndexEntry>,
    {
        if samples.is_empty() {
            return Ok(());
        }
        validate_ordered_samples(samples)?;

        let duration_ms = self.segment_duration_ms()?;
        let mut idx = 0usize;
        while idx < samples.len() {
            let ts = samples[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < samples.len() {
                let next_start = segment_window(samples[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, kind_mask)?;
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref)?;
            active
                .series_entries
                .merge_kind(local_ref as usize, kind_mask)?;
            let metadata = metadata_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = append_chunk(&mut active.chunks, local_ref, &samples[idx..end_idx])?;
            let chunk_append = chunk_append_start.elapsed();

            let bookkeeping_start = Instant::now();
            active.chunk_entries.push_entry(local_ref as usize, entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata,
                    chunk_append,
                    label_time_range: Duration::ZERO,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
            idx = end_idx;
        }

        Ok(())
    }

    fn record_i64_samples_ordered_with_metadata_source<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, i64)],
        mut apply_metadata: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut ActiveSegment, u32) -> io::Result<()>,
    {
        if samples.is_empty() {
            return Ok(());
        }
        validate_ordered_samples(samples)?;

        let duration_ms = self.segment_duration_ms()?;
        let mut idx = 0usize;
        while idx < samples.len() {
            let ts = samples[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < samples.len() {
                let next_start = segment_window(samples[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64)?;
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref)?;
            let metadata = metadata_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = active
                .chunks
                .append_int_chunk_ordered(local_ref, &samples[idx..end_idx])?;
            let chunk_append = chunk_append_start.elapsed();

            let bookkeeping_start = Instant::now();
            active.chunk_entries.push_entry(local_ref as usize, entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata,
                    chunk_append,
                    label_time_range: Duration::ZERO,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
            idx = end_idx;
        }

        Ok(())
    }

    pub fn record_samples_raw(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples(series, None, samples, true)
    }

    pub fn record_sample_i64(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: i64,
    ) -> io::Result<()> {
        self.record_samples_i64(series, &[(timestamp_ms, value)])
    }

    pub fn record_sample_i64_raw(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: i64,
    ) -> io::Result<()> {
        self.record_samples_i64_raw(series, &[(timestamp_ms, value)])
    }

    pub fn record_samples_i64(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, i64)],
    ) -> io::Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let duration_ms = self.segment_duration_ms()?;
        let mut ordered: Vec<(u64, i64)> = samples.to_vec();
        ordered.sort_by_key(|(ts, _)| *ts);

        let mut idx = 0usize;
        while idx < ordered.len() {
            let ts = ordered[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < ordered.len() {
                let next_start = segment_window(ordered[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64)?;
            let ensure_window = ensure_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = active
                .chunks
                .append_int_chunk_ordered(local_ref, &ordered[idx..end_idx])?;
            let chunk_append = chunk_append_start.elapsed();

            let bookkeeping_start = Instant::now();
            active.chunk_entries.push_entry(local_ref as usize, entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata: Duration::ZERO,
                    chunk_append,
                    label_time_range: Duration::ZERO,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
            idx = end_idx;
        }

        Ok(())
    }

    pub fn record_samples_i64_raw(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, i64)],
    ) -> io::Result<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let duration_ms = self.segment_duration_ms()?;
        let mut ordered: Vec<(u64, i64)> = samples.to_vec();
        ordered.sort_by_key(|(ts, _)| *ts);

        let mut idx = 0usize;
        while idx < ordered.len() {
            let ts = ordered[idx].0;
            let (start_ms, end_ms) = segment_window(ts, duration_ms);

            let mut end_idx = idx + 1;
            while end_idx < ordered.len() {
                let next_start = segment_window(ordered[end_idx].0, duration_ms).0;
                if next_start != start_ms {
                    break;
                }
                end_idx += 1;
            }

            let wall_start = Instant::now();
            let ensure_start = Instant::now();
            self.ensure_active_window(start_ms, end_ms)?;
            let Some(active) = &mut self.active else {
                return Ok(());
            };
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64)?;
            let ensure_window = ensure_start.elapsed();

            let chunk_append_start = Instant::now();
            let entry = active
                .chunks
                .append_int_chunk_raw_ordered(local_ref, &ordered[idx..end_idx])?;
            let chunk_append = chunk_append_start.elapsed();

            let bookkeeping_start = Instant::now();
            active.chunk_entries.push_entry(local_ref as usize, entry);
            active.datapoints = active.datapoints.saturating_add((end_idx - idx) as u64);
            let bookkeeping = bookkeeping_start.elapsed();
            self.record_profile.add_chunk(
                SegmentRecordChunkTiming {
                    wall_elapsed: wall_start.elapsed(),
                    ensure_window,
                    metadata: Duration::ZERO,
                    chunk_append,
                    label_time_range: Duration::ZERO,
                    bookkeeping,
                },
                (end_idx - idx) as u64,
            );
            idx = end_idx;
        }

        Ok(())
    }

    // Writes metadata, chunk index, and placeholder files for non-chunk artifacts.
    fn segment_duration_ms(&self) -> io::Result<u64> {
        let ms = self.config.segment_duration.as_millis();
        if ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment_duration must be > 0",
            ));
        }
        if ms > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment_duration is too large",
            ));
        }
        Ok(ms as u64)
    }

    fn ensure_active_window(&mut self, start_ms: u64, end_ms: u64) -> io::Result<()> {
        let rotate = match &self.active {
            None => true,
            Some(active) => start_ms != active.start_ms || end_ms != active.end_ms,
        };

        if rotate {
            self.flush()?;
            let id = self
                .config
                .segment_id_provider
                .next_segment_id(start_ms, end_ms)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
            let temp_dir = SegmentPaths::new(&self.config.segments_dir, id).create_temp_dir()?;
            let payload_lane = self.next_payload_lane;
            let chunk_file = File::create(temp_dir.file_path(SegmentFile::Chunks))?;
            let ooo_chunk_file = File::create(temp_dir.file_path(SegmentFile::OooChunks))?;
            let payload_file = match payload_lane {
                SegmentPayloadLane::InOrder => chunk_file,
                SegmentPayloadLane::OutOfOrder => ooo_chunk_file,
            };
            let chunks = ChunkWriter::new_with_file_id(payload_file, payload_lane.file_id())?;
            self.active = Some(ActiveSegment {
                id,
                start_ms,
                end_ms,
                datapoints: 0,
                series_map: HashMap::new(),
                symbols: SegmentSymbols::default(),
                series_entries: WriterSeriesEntryStore::new(),
                normalized_names: NormalizedNameCache::default(),
                metadata_hash_scratch: Vec::new(),
                metadata_label_scratch: Vec::new(),
                chunk_entries: InlineOneChunkEntryStore::new(),
                chunks,
                payload_lane,
                temp_dir,
                metric_query_ordered_input: false,
                metric_query_ordered_batch_seen: false,
                metric_query_ordered_series_remaining: 0,
                deferred_flat_label_metadata: false,
                recording_closed: false,
            });
            self.next_payload_lane = SegmentPayloadLane::InOrder;
        }

        Ok(())
    }

    fn abort_deferred_flat_metadata_batch(&mut self) {
        let pending = self
            .active
            .as_ref()
            .is_some_and(|active| active.deferred_flat_label_metadata);
        if !pending {
            return;
        }
        let Some(active) = self.active.take() else {
            return;
        };
        let temp_dir = active.temp_dir.path().to_path_buf();
        drop(active);
        let _ = fs::remove_dir_all(temp_dir);
    }
}

impl<S: SymbolTable> DeferredFlatMetadataBatch<'_, '_, S> {
    #[cfg(test)]
    pub(in crate::storage::segment) fn label_arena_stats(&self) -> (usize, usize) {
        let entries = &self
            .writer
            .active
            .as_ref()
            .expect("deferred batch has an active segment")
            .series_entries;
        (entries.labels_len(), entries.labels_capacity())
    }

    pub fn record_samples_ordered(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples_ordered(series, samples, false)
    }

    pub fn record_samples_raw_ordered(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
    ) -> io::Result<()> {
        self.record_float_samples_ordered(series, samples, true)
    }

    pub fn record_histogram_samples_ordered(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, HistogramValue)],
    ) -> io::Result<()> {
        self.record_typed_samples_ordered(
            series,
            samples,
            SERIES_KIND_HISTOGRAM,
            ChunkWriter::append_histogram_chunk_ordered,
        )
    }

    pub fn record_exponential_histogram_samples_ordered(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, ExponentialHistogramValue)],
    ) -> io::Result<()> {
        self.record_typed_samples_ordered(
            series,
            samples,
            SERIES_KIND_EXPONENTIAL_HISTOGRAM,
            ChunkWriter::append_exponential_histogram_chunk_ordered,
        )
    }

    pub fn record_summary_samples_ordered(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, SummaryValue)],
    ) -> io::Result<()> {
        self.record_typed_samples_ordered(
            series,
            samples,
            SERIES_KIND_SUMMARY,
            ChunkWriter::append_summary_chunk_ordered,
        )
    }

    pub fn finish(mut self, canonical_label_counts: &[u32]) -> io::Result<()> {
        if self.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "deferred flat metadata batch is already finished",
            ));
        }
        let metadata_start = Instant::now();
        let result = self
            .writer
            .apply_deferred_metric_query_ordered_flat_metadata_inner(
                canonical_label_counts,
                self.labelsets,
            );
        self.writer
            .record_profile
            .add_metadata_batch(metadata_start.elapsed());
        if result.is_err() {
            self.writer.abort_deferred_flat_metadata_batch();
        }
        self.finished = true;
        result
    }

    fn record_float_samples_ordered(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        raw: bool,
    ) -> io::Result<()> {
        if let Err(error) = self.validate_samples_window(samples) {
            self.abort();
            return Err(error);
        }
        let result = self
            .writer
            .record_float_samples_ordered_with_metadata_source(
                series,
                samples,
                raw,
                |_active, _local_ref| Ok(()),
            );
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn record_typed_samples_ordered<T, A>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, T)],
        kind_mask: u8,
        append_chunk: A,
    ) -> io::Result<()>
    where
        A: Fn(&mut ChunkWriter, u32, &[(u64, T)]) -> io::Result<ChunkIndexEntry>,
    {
        if let Err(error) = self.validate_samples_window(samples) {
            self.abort();
            return Err(error);
        }
        let result = self
            .writer
            .record_typed_samples_ordered_with_metadata_source(
                series,
                samples,
                kind_mask,
                append_chunk,
                |_active, _local_ref| Ok(()),
            );
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn validate_samples_window<T>(&self, samples: &[(u64, T)]) -> io::Result<()> {
        if self.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "deferred flat metadata batch is already finished",
            ));
        }
        if samples.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "deferred flat metadata series has no samples",
            ));
        }
        let Some(active) = self.writer.active.as_ref() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata batch has no active segment",
            ));
        };
        if !active.deferred_flat_label_metadata {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata batch is no longer pending",
            ));
        }
        let expected_window = (active.start_ms, active.end_ms);
        let duration_ms = self.writer.segment_duration_ms()?;
        let first_window = segment_window(samples[0].0, duration_ms);
        let last_window = segment_window(samples[samples.len() - 1].0, duration_ms);
        if first_window != expected_window || last_window != expected_window {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "deferred flat metadata samples cross their reserved segment window",
            ));
        }
        Ok(())
    }

    fn abort(&mut self) {
        self.writer.abort_deferred_flat_metadata_batch();
        self.finished = true;
    }
}

impl<S: SymbolTable> Drop for DeferredFlatMetadataBatch<'_, '_, S> {
    fn drop(&mut self) {
        if !self.finished {
            self.writer.abort_deferred_flat_metadata_batch();
        }
    }
}

pub(in super::super) fn ensure_local_series_with_kind(
    active: &mut ActiveSegment,
    series: SeriesRef,
    kind_mask: u8,
) -> io::Result<u32> {
    if active.recording_closed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cannot record samples after segment recording is complete",
        ));
    }
    let source_ref = series.get();
    if active.deferred_flat_label_metadata {
        if active.metric_query_ordered_series_remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata batch received an extra series",
            ));
        }
        let expected_index = active
            .series_entries
            .len()
            .checked_sub(active.metric_query_ordered_series_remaining)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "deferred flat metadata remaining-series count is invalid",
                )
            })?;
        let expected_source_ref = active
            .series_entries
            .placeholder_source_ref(expected_index)?;
        if source_ref != expected_source_ref {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata series were not recorded in their reserved order",
            ));
        }
        let expected_local_ref = u32::try_from(expected_index).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata local ref exceeds u32",
            )
        })?;
        if active.series_map.get(&source_ref).copied() != Some(expected_local_ref) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "deferred flat metadata source-to-local mapping is inconsistent",
            ));
        }
        active
            .series_entries
            .merge_kind(expected_index, kind_mask)?;
        active.metric_query_ordered_series_remaining -= 1;
        return Ok(expected_local_ref);
    }

    match active.series_map.get(&source_ref) {
        Some(&id) => {
            let existing_kind_mask = active.series_entries.kind_mask(id as usize)?;
            if active.metric_query_ordered_input && existing_kind_mask & kind_mask != kind_mask {
                active.metric_query_ordered_input = false;
                active.metric_query_ordered_series_remaining = 0;
            }
            active.series_entries.merge_kind(id as usize, kind_mask)?;
            Ok(id)
        }
        None => {
            if active.metric_query_ordered_input {
                if active.metric_query_ordered_series_remaining == 0 {
                    active.metric_query_ordered_input = false;
                } else {
                    active.metric_query_ordered_series_remaining -= 1;
                }
            }
            let id = u32::try_from(active.series_map.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "series_ref exceeds u32")
            })?;
            active
                .series_entries
                .push_placeholder(u64::from(source_ref), kind_mask)?;
            active.series_map.insert(source_ref, id);
            active.chunk_entries.push_empty_series();
            Ok(id)
        }
    }
}

pub(in super::super) fn validate_ordered_samples<T>(samples: &[(u64, T)]) -> io::Result<()> {
    if samples.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ordered samples must be sorted by timestamp",
        ));
    }
    Ok(())
}
