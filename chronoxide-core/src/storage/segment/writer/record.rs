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
        let additional = series.saturating_sub(active.series_map.len());
        active.series_map.reserve(additional);
        active.metadata_present.reserve(additional);
        active.series_entries.reserve(additional);
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
        if active.series_entries.is_empty() && active.datapoints == 0 {
            active.metric_query_ordered_input = true;
        }
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
            |active, local_ref| {
                apply_segment_metadata(active, local_ref, metadata);
            },
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
            |active, local_ref| {
                apply_segment_metadata(active, local_ref, metadata);
            },
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
                );
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
            apply_label_visitor(active, local_ref, &mut visit_labels);
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
            |active, local_ref| {
                apply_label_visitor(active, local_ref, &mut visit_labels);
            },
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
                );
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
                apply_label_visitor_with_kind(active, local_ref, kind_mask, &mut visit_labels);
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
                apply_flat_interned_label_metadata(active, local_ref, kind_mask, series, labelsets);
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
                apply_segment_metadata(active, local_ref, metadata);
            }
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
        F: FnMut(&mut ActiveSegment, u32),
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
        F: FnMut(&mut ActiveSegment, u32),
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
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_FLOAT);
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref);
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
        F: FnMut(&mut ActiveSegment, u32),
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
            let local_ref = ensure_local_series_with_kind(active, series, kind_mask);
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref);
            active.series_entries[local_ref as usize].kind_mask |= kind_mask;
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
        F: FnMut(&mut ActiveSegment, u32),
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
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64);
            let ensure_window = ensure_start.elapsed();

            let metadata_start = Instant::now();
            apply_metadata(active, local_ref);
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
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64);
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
            let local_ref = ensure_local_series_with_kind(active, series, SERIES_KIND_INT64);
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
            let chunk_file = File::create(temp_dir.file_path(SegmentFile::Chunks))?;
            let chunks = ChunkWriter::new(chunk_file)?;
            self.active = Some(ActiveSegment {
                id,
                start_ms,
                end_ms,
                datapoints: 0,
                series_map: HashMap::new(),
                metadata_present: Vec::new(),
                symbols: SegmentSymbols::default(),
                series_entries: Vec::new(),
                normalized_names: NormalizedNameCache::default(),
                metadata_hash_scratch: Vec::new(),
                metadata_label_scratch: Vec::new(),
                chunk_entries: InlineOneChunkEntryStore::new(),
                chunks,
                temp_dir,
                metric_query_ordered_input: false,
            });
        }

        Ok(())
    }
}

pub(in super::super) fn ensure_local_series_with_kind(
    active: &mut ActiveSegment,
    series: SeriesRef,
    kind_mask: u8,
) -> u32 {
    let source_ref = series.get();
    match active.series_map.get(&source_ref) {
        Some(&id) => {
            active.series_entries[id as usize].kind_mask |= kind_mask;
            id
        }
        None => {
            let id = active.series_map.len() as u32;
            active.series_map.insert(source_ref, id);
            active.metadata_present.push(false);
            active.series_entries.push(WriterSeriesEntry {
                series_id: u64::from(source_ref),
                kind_mask,
                labels: Vec::new(),
            });
            active.chunk_entries.push_empty_series();
            id
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
