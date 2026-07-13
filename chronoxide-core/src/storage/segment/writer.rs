use super::*;

pub(super) struct ActiveSegment {
    pub(super) id: SegmentId,
    pub(super) start_ms: u64,
    pub(super) end_ms: u64,
    pub(super) datapoints: u64,
    pub(super) series_map: HashMap<u32, u32>,
    pub(super) metadata_present: Vec<bool>,
    pub(super) symbols: SegmentSymbols,
    pub(super) series_entries: Vec<SeriesEntry>,
    pub(super) normalized_names: NormalizedNameCache,
    pub(super) metadata_hash_scratch: Vec<u8>,
    pub(super) metadata_label_scratch: Vec<(Arc<str>, SourceLabelValue)>,
    pub(super) chunk_entries: Vec<Vec<ChunkIndexEntry>>,
    pub(super) chunks: ChunkWriter,
    pub(super) temp_dir: SegmentTempDir,
    pub(super) metric_query_ordered_input: bool,
}

#[derive(Debug, Clone)]
pub struct SegmentSeriesMetadata {
    pub(super) series_id: u64,
    pub(super) labels: Vec<(String, String)>,
}

impl SegmentSeriesMetadata {
    pub fn series_id(&self) -> u64 {
        self.series_id
    }

    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }
}

pub struct SegmentSeriesMetadataBuilder {
    labels: BTreeMap<String, String>,
    metric_name_seen: bool,
}

impl SegmentSeriesMetadataBuilder {
    pub fn new() -> Self {
        let mut labels = BTreeMap::new();
        labels.insert(METRIC_NAME_LABEL.to_string(), String::new());
        Self {
            labels,
            metric_name_seen: false,
        }
    }

    pub fn push_label(&mut self, name: &str, value: &str) {
        if name == METRIC_NAME_LABEL {
            if !self.metric_name_seen {
                self.labels
                    .insert(METRIC_NAME_LABEL.to_string(), normalize_metric_name(value));
                self.metric_name_seen = true;
            }
        } else {
            self.labels
                .insert(normalize_label_name(name), value.to_string());
        }
    }

    pub fn finish(self) -> SegmentSeriesMetadata {
        let labels: Vec<_> = self.labels.into_iter().collect();
        let series_id = segment_series_id(&labels);
        SegmentSeriesMetadata { series_id, labels }
    }
}

pub(super) fn encode_canonical_segment_labels(
    labels: Vec<(String, String)>,
    symbols: &mut SegmentSymbols,
) -> SeriesEntry {
    encode_borrowed_canonical_segment_labels(
        labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        symbols,
    )
}

pub(super) fn encode_borrowed_canonical_segment_labels<'a>(
    labels: impl IntoIterator<Item = (&'a str, &'a str)>,
    symbols: &mut SegmentSymbols,
) -> SeriesEntry {
    let mut bytes = Vec::new();
    let mut encoded_labels = Vec::new();
    for (key, value) in labels {
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);

        let key_sym = symbols.intern(key);
        let value_sym = symbols.intern(value);
        encoded_labels.push((key_sym, value_sym));
    }

    SeriesEntry {
        series_id: xxhash64(&bytes),
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: encoded_labels,
    }
}

impl Default for SegmentSeriesMetadataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SegmentWriter {
    pub(super) config: SegmentWriterConfig,
    pub(super) active: Option<ActiveSegment>,
    pub(super) last_flush_profile: Option<SegmentFlushProfile>,
    pub(super) record_profile: SegmentRecordProfile,
}

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

impl SegmentWriter {
    pub fn new(config: SegmentWriterConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.segments_dir)?;
        Ok(Self {
            config,
            active: None,
            last_flush_profile: None,
            record_profile: SegmentRecordProfile::default(),
        })
    }

    pub fn last_flush_profile(&self) -> Option<&SegmentFlushProfile> {
        self.last_flush_profile.as_ref()
    }

    pub fn record_profile(&self) -> SegmentRecordProfile {
        self.record_profile
    }

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
        active.chunk_entries.reserve(additional);
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

    pub fn record_samples_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_with_label_visitor(series, samples, false, visit_labels)
    }

    pub fn record_samples_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_ordered_with_label_visitor(series, samples, false, visit_labels)
    }

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

    pub fn record_samples_raw_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_with_label_visitor(series, samples, true, visit_labels)
    }

    pub fn record_samples_raw_ordered_with_label_visitor<F>(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, f64)],
        visit_labels: F,
    ) -> io::Result<()>
    where
        F: FnMut(&mut dyn FnMut(&str, &str)),
    {
        self.record_float_samples_ordered_with_label_visitor(series, samples, true, visit_labels)
    }

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
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
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
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
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
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
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
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
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
            active
                .chunk_entries
                .get_mut(local_ref as usize)
                .expect("chunk entries length mismatch")
                .push(entry);
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
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let ActiveSegment {
            id: segment_id,
            start_ms,
            end_ms,
            datapoints,
            symbols,
            series_entries,
            chunk_entries,
            chunks,
            temp_dir: tmp,
            metric_query_ordered_input,
            ..
        } = active;
        if series_entries.len() != chunk_entries.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series and chunk entry counts differ",
            ));
        }

        let total_start = Instant::now();
        let series = series_entries.len() as u64;
        let chunk_summary = SegmentChunkSummary::from_chunk_entries(&chunk_entries);
        let mut profile =
            SegmentFlushProfile::new(segment_id.dir_name(), start_ms, end_ms, datapoints, series);
        let series_order = if metric_query_ordered_input {
            let ordered = (0..series_entries.len()).collect::<Vec<_>>();
            #[cfg(debug_assertions)]
            {
                let expected = metric_query_series_order(&series_entries, &symbols)?;
                debug_assert_eq!(
                    expected, ordered,
                    "metric-query ordered input flag was set for non-metric-order series"
                );
            }
            ordered
        } else {
            metric_query_series_order(&series_entries, &symbols)?
        };
        let old_to_new_refs = old_to_new_series_refs(&series_order)?;

        let meta = SegmentMeta {
            segment_id: segment_id.dir_name(),
            start_ms,
            end_ms,
            datapoints,
            series,
            chunk_summary: Some(chunk_summary),
        };
        time_flush_stage(&mut profile, SegmentFlushStageKind::MetaJson, || {
            let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(io::Error::other)?;
            fs::write(tmp.file_path(SegmentFile::MetaJson), meta_bytes)
        })?;

        let mut chunk_entries = chunk_entries;
        let chunks_path = tmp.file_path(SegmentFile::Chunks);
        let chunk_rewrite =
            time_flush_stage(&mut profile, SegmentFlushStageKind::ChunksFlush, || {
                let mut chunks = chunks;
                chunks.flush()?;
                drop(chunks);
                rewrite_chunks_in_series_major_order(
                    &chunks_path,
                    &mut chunk_entries,
                    &series_order,
                    &old_to_new_refs,
                )
            })?;
        profile.add_chunk_rewrite(chunk_rewrite.frames, chunk_rewrite.payload_bytes);
        let mut series_entries =
            reorder_vec_by_old_indices(series_entries, &series_order, "series entries")?;
        let chunk_entries =
            reorder_vec_by_old_indices(chunk_entries, &series_order, "chunk entries")?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::ChunkIndex, || {
            let mut chunk_index = File::create(tmp.file_path(SegmentFile::ChunkIndex))?;
            write_chunk_index(&mut chunk_index, &chunk_entries)?;
            chunk_index.flush()
        })?;

        let chunk_ranges = chunk_index_ranges(&chunk_entries)?;
        let finalized_metadata =
            time_flush_stage(&mut profile, SegmentFlushStageKind::SegmentMetadata, || {
                if series_entries.len() != chunk_ranges.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "series and chunk index range counts differ",
                    ));
                }
                for (entry, range) in series_entries.iter_mut().zip(chunk_ranges.iter().copied()) {
                    entry.chunk_index = range;
                }
                finalize_segment_symbol_ids(symbols, series_entries, &chunk_entries)
            })?;
        let label_values =
            time_flush_stage(&mut profile, SegmentFlushStageKind::LabelValues, || {
                LabelValueFstIndex::from_series(
                    &finalized_metadata.series_entries,
                    &finalized_metadata.symbols,
                )
            })?;
        let label_value_time_ranges = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::LabelValueTimeRanges,
            || Ok(finalized_metadata.label_value_time_ranges),
        )?;
        let metric_series_ranges = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::MetricSeriesRanges,
            || {
                MetricSeriesRangeIndex::from_series(
                    &finalized_metadata.series_entries,
                    &finalized_metadata.symbols,
                    &label_value_time_ranges,
                )
            },
        )?;
        let routing_index = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::RoutingIndexBuild,
            || {
                SegmentRoutingIndex::from_indexes(
                    &finalized_metadata.symbols,
                    &finalized_metadata.postings,
                    &label_value_time_ranges,
                )
            },
        )?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Symbols, || {
            let mut symbols_file = File::create(tmp.file_path(SegmentFile::Symbols))?;
            write_symbols_bin(&mut symbols_file, &finalized_metadata.symbols)?;
            symbols_file.flush()
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Series, || {
            let mut series_file = File::create(tmp.file_path(SegmentFile::Series))?;
            write_series_bin(&mut series_file, &finalized_metadata.series_entries)?;
            series_file.flush()
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Indexes, || {
            let mut index_file = File::create(tmp.file_path(SegmentFile::Indexes))?;
            write_segment_indexes(
                &mut index_file,
                &SegmentIndexes {
                    exact_postings: finalized_metadata.postings,
                    label_values,
                    label_value_time_ranges,
                    metric_series_ranges,
                    routing_index: Some(routing_index),
                },
            )?;
            index_file.flush()
        })?;
        time_flush_stage(&mut profile, SegmentFlushStageKind::OooChunks, || {
            File::create(tmp.file_path(SegmentFile::OooChunks)).map(|_| ())
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Footer, || {
            write_segment_footer(tmp.path())
        })?;
        profile.set_file_sizes(collect_segment_file_sizes(tmp.path())?);
        let published_dir = time_flush_stage(&mut profile, SegmentFlushStageKind::Publish, || {
            tmp.publish()
        })?;
        append_segment_manifest_record(&self.config.segments_dir, &meta)?;
        profile.total = total_start.elapsed();
        let duration = Duration::from_millis(end_ms - start_ms);
        info!(
            segment_id = %segment_id,
            start_ms,
            end_ms,
            duration=?duration,
            datapoints,
            series,
            elapsed_ms = duration_ms_u64(profile.total),
            meta_json_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::MetaJson),
            chunks_flush_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::ChunksFlush),
            chunk_index_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::ChunkIndex),
            segment_metadata_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::SegmentMetadata),
            label_values_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::LabelValues),
            label_value_time_ranges_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::LabelValueTimeRanges),
            symbols_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Symbols),
            series_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Series),
            indexes_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Indexes),
            routing_index_build_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::RoutingIndexBuild),
            ooo_chunks_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::OooChunks),
            footer_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Footer),
            publish_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Publish),
            chunk_rewrite_frames = profile.chunk_rewrite_frames(),
            chunk_rewrite_payload_bytes = profile.chunk_rewrite_payload_bytes(),
            total_bytes = profile.total_file_bytes(),
            data_bytes = profile.data_file_bytes(),
            metadata_bytes = profile.metadata_file_bytes(),
            index_bytes = profile.index_file_bytes(),
            footer_bytes = profile.footer_file_bytes(),
            meta_json_bytes = profile.file_size_bytes(SegmentFile::MetaJson).unwrap_or_default(),
            symbols_bytes = profile.file_size_bytes(SegmentFile::Symbols).unwrap_or_default(),
            series_bytes = profile.file_size_bytes(SegmentFile::Series).unwrap_or_default(),
            chunks_bytes = profile.file_size_bytes(SegmentFile::Chunks).unwrap_or_default(),
            ooo_chunks_bytes = profile.file_size_bytes(SegmentFile::OooChunks).unwrap_or_default(),
            chunk_index_bytes = profile.file_size_bytes(SegmentFile::ChunkIndex).unwrap_or_default(),
            indexes_bytes = profile.file_size_bytes(SegmentFile::Indexes).unwrap_or_default(),
            footer_file_bytes = profile.file_size_bytes(SegmentFile::Footer).unwrap_or_default(),
            path = %published_dir.display(),
            "Segment published"
        );
        self.last_flush_profile = Some(profile);
        Ok(())
    }

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
                chunk_entries: Vec::new(),
                chunks,
                temp_dir,
                metric_query_ordered_input: false,
            });
        }

        Ok(())
    }
}

pub(super) fn ensure_local_series_with_kind(
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
            active.series_entries.push(SeriesEntry {
                series_id: u64::from(source_ref),
                kind_mask,
                chunk_index: Default::default(),
                labels: Vec::new(),
            });
            active.chunk_entries.push(Vec::new());
            id
        }
    }
}

pub(super) fn validate_ordered_samples<T>(samples: &[(u64, T)]) -> io::Result<()> {
    if samples.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ordered samples must be sorted by timestamp",
        ));
    }
    Ok(())
}

pub(super) fn time_flush_stage<T>(
    profile: &mut SegmentFlushProfile,
    kind: SegmentFlushStageKind,
    f: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let started = Instant::now();
    let result = f();
    profile.push_stage(kind, started.elapsed());
    result
}

pub(super) fn collect_segment_file_sizes(
    segment_dir: &Path,
) -> io::Result<Vec<SegmentFlushFileSize>> {
    SEGMENT_FLUSH_SIZE_FILES
        .into_iter()
        .map(|file| {
            fs::metadata(segment_dir.join(file.filename())).map(|metadata| SegmentFlushFileSize {
                file,
                bytes: metadata.len(),
            })
        })
        .collect()
}

pub(super) fn file_len(path: &Path) -> io::Result<u64> {
    Ok(fs::metadata(path)?.len())
}

pub(super) fn duration_ms_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub(super) fn deterministic_segment_ulid(
    seed: u64,
    start_ms: u64,
    end_ms: u64,
    ordinal: u64,
) -> Ulid {
    let mut bytes = Vec::with_capacity(56);
    bytes.extend_from_slice(b"chronoxide-segment-id-v1");
    bytes.extend_from_slice(&seed.to_le_bytes());
    bytes.extend_from_slice(&start_ms.to_le_bytes());
    bytes.extend_from_slice(&end_ms.to_le_bytes());
    bytes.extend_from_slice(&ordinal.to_le_bytes());

    let high = xxhash64(&bytes);
    bytes.extend_from_slice(&high.to_le_bytes());
    let low = xxhash64(&bytes);
    let random = (((high as u128) & 0xffff) << 64) | low as u128;
    Ulid::from_parts(start_ms, random)
}

pub(super) fn canonical_segment_metadata(labels: &[(String, String)]) -> SegmentSeriesMetadata {
    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in labels {
        builder.push_label(key, value);
    }
    builder.finish()
}

pub(super) fn apply_segment_metadata(
    active: &mut ActiveSegment,
    local_ref: u32,
    metadata: &SegmentSeriesMetadata,
) {
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        return;
    }

    let mut encoded_labels = Vec::with_capacity(metadata.labels.len());
    for (key, value) in &metadata.labels {
        let key_sym = active.symbols.intern(key);
        let value_sym = active.symbols.intern(value);
        encoded_labels.push((key_sym, value_sym));
    }

    active.series_entries[idx] = SeriesEntry {
        series_id: metadata.series_id,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: active.series_entries[idx].chunk_index,
        labels: encoded_labels,
    };
    active.metadata_present[idx] = true;
}

pub(super) fn apply_label_visitor<F>(
    active: &mut ActiveSegment,
    local_ref: u32,
    visit_labels: &mut F,
) where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    apply_label_visitor_with_kind(active, local_ref, SERIES_KIND_FLOAT, visit_labels);
}

pub(super) fn apply_label_visitor_with_kind<F>(
    active: &mut ActiveSegment,
    local_ref: u32,
    kind_mask: u8,
    visit_labels: &mut F,
) where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        active.series_entries[idx].kind_mask |= kind_mask;
        return;
    }

    let mut entry = encode_label_visitor_metadata(&mut active.symbols, |visit| {
        visit_labels(visit);
    });
    entry.kind_mask = kind_mask;
    active.series_entries[idx] = entry;
    active.metadata_present[idx] = true;
}

pub(super) fn apply_flat_interned_label_metadata<S: SymbolTable>(
    active: &mut ActiveSegment,
    local_ref: u32,
    kind_mask: u8,
    source_series: SeriesRef,
    labelsets: &FlatInternedLabelSetStore<S>,
) {
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        active.series_entries[idx].kind_mask |= kind_mask;
        return;
    }

    let mut entry = encode_flat_interned_label_metadata(
        &mut active.symbols,
        &mut active.normalized_names,
        &mut active.metadata_hash_scratch,
        &mut active.metadata_label_scratch,
        labelsets,
        source_series,
    );
    entry.kind_mask = kind_mask;
    active.series_entries[idx] = entry;
    active.metadata_present[idx] = true;
}

pub(super) enum SourceLabelValue {
    Symbol(SymbolId),
    Owned(Arc<str>),
}

pub(super) const MAX_NORMALIZED_NAME_CACHE_ENTRIES: usize = 262_144;

pub(super) struct NormalizedNameCache {
    metric_label_name: Arc<str>,
    label_names: HashMap<SymbolId, Arc<str>>,
    metric_names: HashMap<SymbolId, Arc<str>>,
    max_entries: usize,
}

impl Default for NormalizedNameCache {
    fn default() -> Self {
        Self::with_max_entries(MAX_NORMALIZED_NAME_CACHE_ENTRIES)
    }
}

impl NormalizedNameCache {
    pub(super) fn with_max_entries(max_entries: usize) -> Self {
        Self {
            metric_label_name: Arc::from(METRIC_NAME_LABEL),
            label_names: HashMap::new(),
            metric_names: HashMap::new(),
            max_entries,
        }
    }

    pub(super) fn metric_label_name(&self) -> Arc<str> {
        Arc::clone(&self.metric_label_name)
    }

    pub(super) fn label_name(
        &mut self,
        source_id: SymbolId,
        source_name: &str,
        normalize: impl FnOnce(&str) -> String,
    ) -> Arc<str> {
        if let Some(name) = self.label_names.get(&source_id) {
            return Arc::clone(name);
        }

        let name = Arc::from(normalize(source_name));
        if self.label_names.len() < self.max_entries {
            self.label_names.insert(source_id, Arc::clone(&name));
        }
        name
    }

    pub(super) fn metric_name(
        &mut self,
        source_id: SymbolId,
        source_name: &str,
        normalize: impl FnOnce(&str) -> String,
    ) -> Arc<str> {
        if let Some(name) = self.metric_names.get(&source_id) {
            return Arc::clone(name);
        }

        let name = Arc::from(normalize(source_name));
        if self.metric_names.len() < self.max_entries {
            self.metric_names.insert(source_id, Arc::clone(&name));
        }
        name
    }
}

pub(super) fn encode_flat_interned_label_metadata<S: SymbolTable>(
    symbols: &mut SegmentSymbols,
    normalized_names: &mut NormalizedNameCache,
    hash_scratch: &mut Vec<u8>,
    label_scratch: &mut Vec<(Arc<str>, SourceLabelValue)>,
    labelsets: &FlatInternedLabelSetStore<S>,
    source_series: SeriesRef,
) -> SeriesEntry {
    let source_symbols = labelsets.symbols();
    label_scratch.clear();
    let mut metric_name_seen = false;
    let mut labels_sorted = true;

    labelsets.visit_labelset_symbol_ids(source_series, |key_id, value_id| {
        let name = source_symbols.resolve(key_id);
        if name == METRIC_NAME_LABEL {
            if !metric_name_seen {
                let metric_name = normalized_names.metric_name(
                    value_id,
                    source_symbols.resolve(value_id),
                    normalize_metric_name,
                );
                let key = normalized_names.metric_label_name();
                if let Some((last_key, _)) = label_scratch.last()
                    && last_key.as_ref() > key.as_ref()
                {
                    labels_sorted = false;
                }
                label_scratch.push((key, SourceLabelValue::Owned(metric_name)));
                metric_name_seen = true;
            }
        } else {
            let key = normalized_names.label_name(key_id, name, normalize_label_name);
            if let Some((last_key, _)) = label_scratch.last()
                && last_key.as_ref() > key.as_ref()
            {
                labels_sorted = false;
            }
            label_scratch.push((key, SourceLabelValue::Symbol(value_id)));
        }
    });

    if !metric_name_seen {
        let key = normalized_names.metric_label_name();
        if let Some((last_key, _)) = label_scratch.last()
            && last_key.as_ref() > key.as_ref()
        {
            labels_sorted = false;
        }
        label_scratch.push((key, SourceLabelValue::Owned(Arc::from(""))));
    }

    if !labels_sorted {
        label_scratch.sort_by(|left, right| left.0.as_ref().cmp(right.0.as_ref()));
    }

    let entry =
        encode_flat_interned_sorted_labels(label_scratch, source_symbols, symbols, hash_scratch);
    label_scratch.clear();
    entry
}

pub(super) fn encode_flat_interned_sorted_labels<S: SymbolTable>(
    labels: &[(Arc<str>, SourceLabelValue)],
    source_symbols: &S,
    symbols: &mut SegmentSymbols,
    hash_scratch: &mut Vec<u8>,
) -> SeriesEntry {
    hash_scratch.clear();
    let mut encoded_labels = Vec::with_capacity(labels.len());

    let mut idx = 0usize;
    while idx < labels.len() {
        let mut next = idx + 1;
        while next < labels.len() && labels[next].0 == labels[idx].0 {
            next += 1;
        }

        let (key, value) = &labels[next - 1];
        let value = resolve_source_label_value(source_symbols, value);

        hash_scratch.extend_from_slice(key.as_ref().as_bytes());
        hash_scratch.push(0);
        hash_scratch.extend_from_slice(value.as_bytes());
        hash_scratch.push(0xff);

        let key_sym = symbols.intern(key.as_ref());
        let value_sym = symbols.intern(value);
        encoded_labels.push((key_sym, value_sym));
        idx = next;
    }

    let series_id = xxhash64(hash_scratch);
    hash_scratch.clear();

    SeriesEntry {
        series_id,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: encoded_labels,
    }
}

fn resolve_source_label_value<'a, S: SymbolTable>(
    source_symbols: &'a S,
    value: &'a SourceLabelValue,
) -> &'a str {
    match value {
        SourceLabelValue::Symbol(id) => source_symbols.resolve(*id),
        SourceLabelValue::Owned(value) => value.as_ref(),
    }
}

pub(super) fn encode_label_visitor_metadata<F>(
    symbols: &mut SegmentSymbols,
    mut visit_labels: F,
) -> SeriesEntry
where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    let mut labels = Vec::new();
    let mut metric_name = String::new();
    let mut metric_name_seen = false;
    let mut push_label = |name: &str, value: &str| {
        if name == METRIC_NAME_LABEL {
            if !metric_name_seen {
                metric_name = normalize_metric_name(value);
                metric_name_seen = true;
            }
        } else {
            labels.push((normalize_label_name(name), value.to_string()));
        }
    };
    visit_labels(&mut push_label);

    labels.push((METRIC_NAME_LABEL.to_string(), metric_name));
    labels.sort_by(|left, right| left.0.cmp(&right.0));

    let mut canonical = Vec::with_capacity(labels.len());
    for (key, value) in labels {
        if let Some((last_key, last_value)) = canonical.last_mut()
            && last_key == &key
        {
            *last_value = value;
            continue;
        }
        canonical.push((key, value));
    }

    encode_canonical_segment_labels(canonical, symbols)
}

pub(super) fn update_label_value_time_ranges(
    index: &mut LabelValueTimeRangeIndex,
    entry: &SeriesEntry,
    chunk: &ChunkIndexEntry,
) {
    index.insert_many(&entry.labels, chunk.min_time_ms, chunk.max_time_ms);
}

#[derive(Debug, Eq, PartialEq)]
struct SeriesQueryOrderKey {
    metric_name: String,
    kind_mask: u8,
    labels: Vec<(String, String)>,
    series_id: u64,
    old_ref: usize,
}

pub(super) fn metric_query_series_order(
    series_entries: &[SeriesEntry],
    symbols: &SegmentSymbols,
) -> io::Result<Vec<usize>> {
    let mut keys = Vec::with_capacity(series_entries.len());
    for (old_ref, entry) in series_entries.iter().enumerate() {
        let mut labels = Vec::with_capacity(entry.labels.len());
        let mut metric_name = String::new();
        for (key, value) in &entry.labels {
            let key = symbols.resolve(*key).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series references missing key symbol",
                )
            })?;
            let value = symbols.resolve(*value).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series references missing value symbol",
                )
            })?;
            if key == METRIC_NAME_LABEL {
                metric_name = value.to_string();
            }
            labels.push((key.to_string(), value.to_string()));
        }
        labels.sort();
        keys.push(SeriesQueryOrderKey {
            metric_name,
            kind_mask: entry.kind_mask,
            labels,
            series_id: entry.series_id,
            old_ref,
        });
    }

    keys.sort_by(|left, right| {
        left.metric_name
            .cmp(&right.metric_name)
            .then_with(|| left.kind_mask.cmp(&right.kind_mask))
            .then_with(|| left.labels.cmp(&right.labels))
            .then_with(|| left.series_id.cmp(&right.series_id))
            .then_with(|| left.old_ref.cmp(&right.old_ref))
    });

    Ok(keys.into_iter().map(|key| key.old_ref).collect())
}

pub(super) fn old_to_new_series_refs(order: &[usize]) -> io::Result<Vec<u32>> {
    let mut refs = vec![None; order.len()];
    for (new_ref, &old_ref) in order.iter().enumerate() {
        let Some(slot) = refs.get_mut(old_ref) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series order contains out-of-range ref",
            ));
        };
        if slot.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series order contains duplicate ref",
            ));
        }
        *slot =
            Some(u32::try_from(new_ref).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "series_ref exceeds u32")
            })?);
    }
    refs.into_iter()
        .map(|series_ref| {
            series_ref.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series order is missing a ref")
            })
        })
        .collect()
}

pub(super) fn reorder_vec_by_old_indices<T>(
    items: Vec<T>,
    order: &[usize],
    name: &str,
) -> io::Result<Vec<T>> {
    if items.len() != order.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{name} count does not match series order"),
        ));
    }

    let mut slots: Vec<_> = items.into_iter().map(Some).collect();
    let mut ordered = Vec::with_capacity(order.len());
    for &old_ref in order {
        let Some(slot) = slots.get_mut(old_ref) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{name} order contains out-of-range ref"),
            ));
        };
        let Some(item) = slot.take() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{name} order contains duplicate ref"),
            ));
        };
        ordered.push(item);
    }
    Ok(ordered)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ChunkRewriteStats {
    pub(super) frames: u64,
    pub(super) payload_bytes: u64,
}

pub(super) fn rewrite_chunks_in_series_major_order(
    chunks_path: &Path,
    chunk_entries: &mut [Vec<ChunkIndexEntry>],
    series_order: &[usize],
    old_to_new_refs: &[u32],
) -> io::Result<ChunkRewriteStats> {
    if chunk_entries.len() != old_to_new_refs.len() || chunk_entries.len() != series_order.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk entry count does not match final series order",
        ));
    }
    if chunks_are_already_series_major_order(chunk_entries, series_order, old_to_new_refs) {
        return Ok(ChunkRewriteStats::default());
    }

    let rewrite_path =
        chunks_path.with_file_name(format!("{}.rewrite", SegmentFile::Chunks.filename()));
    let result = (|| {
        let mut source = File::open(chunks_path)?;
        let mut rewritten = File::create(&rewrite_path)?;
        let mut output_offset = 0u64;
        let mut stats = ChunkRewriteStats::default();

        for &old_ref in series_order {
            let new_ref = *old_to_new_refs.get(old_ref).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series order contains ref missing from ref map",
                )
            })?;
            let Some(entries) = chunk_entries.get_mut(old_ref) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series order contains ref missing from chunk entries",
                ));
            };

            entries.sort_by(chunk_entry_time_order);
            for entry in entries {
                let payload_len = u64::from(entry.length);
                let frame_len = rewrite_single_chunk_frame(
                    &mut source,
                    &mut rewritten,
                    output_offset,
                    entry,
                    new_ref,
                )?;
                output_offset = output_offset.saturating_add(u64::from(frame_len));
                stats.frames = stats.frames.saturating_add(1);
                stats.payload_bytes = stats.payload_bytes.saturating_add(payload_len);
            }
        }

        rewritten.flush()?;
        Ok(stats)
    })();

    let stats = match result {
        Ok(stats) => stats,
        Err(err) => {
            let _ = fs::remove_file(&rewrite_path);
            return Err(err);
        }
    };

    fs::rename(rewrite_path, chunks_path)?;
    Ok(stats)
}

fn chunks_are_already_series_major_order(
    chunk_entries: &[Vec<ChunkIndexEntry>],
    series_order: &[usize],
    old_to_new_refs: &[u32],
) -> bool {
    if series_order
        .iter()
        .enumerate()
        .any(|(new_ref, &old_ref)| old_ref != new_ref)
    {
        return false;
    }
    if old_to_new_refs
        .iter()
        .enumerate()
        .any(|(old_ref, &new_ref)| new_ref as usize != old_ref)
    {
        return false;
    }

    let mut last_offset = None;
    for &old_ref in series_order {
        let Some(entries) = chunk_entries.get(old_ref) else {
            return false;
        };
        if entries
            .windows(2)
            .any(|pair| chunk_entry_time_order(&pair[0], &pair[1]).is_gt())
        {
            return false;
        }
        for entry in entries {
            if entry.file_id != 0 {
                return false;
            }
            if let Some(previous) = last_offset
                && entry.offset < previous
            {
                return false;
            }
            last_offset = Some(entry.offset);
        }
    }
    true
}

fn chunk_entry_time_order(left: &ChunkIndexEntry, right: &ChunkIndexEntry) -> std::cmp::Ordering {
    left.file_id
        .cmp(&right.file_id)
        .then_with(|| left.min_time_ms.cmp(&right.min_time_ms))
        .then_with(|| left.max_time_ms.cmp(&right.max_time_ms))
        .then_with(|| left.offset.cmp(&right.offset))
}

fn rewrite_single_chunk_frame(
    source: &mut File,
    rewritten: &mut File,
    output_offset: u64,
    entry: &mut ChunkIndexEntry,
    new_ref: u32,
) -> io::Result<u32> {
    if entry.file_id != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series-major chunk rewrite only supports chunks.bin entries",
        ));
    }
    if entry.length < CHUNK_FILE_HEADER_LEN as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk entry length is shorter than chunk header",
        ));
    }
    let frame_offset = entry
        .offset
        .checked_sub(CHUNK_FRAME_HEADER_LEN as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chunk offset before frame"))?;
    let mut frame_header = [0u8; CHUNK_FRAME_HEADER_LEN];
    source.seek(SeekFrom::Start(frame_offset))?;
    source.read_exact(&mut frame_header)?;
    let frame_len = u32::from_le_bytes(frame_header[0..4].try_into().unwrap());
    let num_chunks = u32::from_le_bytes(frame_header[10..14].try_into().unwrap());
    let entry_len = usize::try_from(entry.length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk entry length too large"))?;
    if num_chunks != 1 || frame_len as usize != CHUNK_FRAME_HEADER_LEN + entry_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series-major chunk rewrite requires single-chunk frames",
        ));
    }

    let mut chunk_payload = vec![0u8; entry_len];
    source.seek(SeekFrom::Start(entry.offset))?;
    source.read_exact(&mut chunk_payload)?;
    chunk_payload[4..8].copy_from_slice(&new_ref.to_le_bytes());
    let frame_crc = crc32c(&chunk_payload);
    frame_header[4..8].copy_from_slice(&frame_crc.to_le_bytes());

    let chunk_offset = output_offset.saturating_add(CHUNK_FRAME_HEADER_LEN as u64);
    rewritten.write_all(&frame_header)?;
    rewritten.write_all(&chunk_payload)?;
    entry.offset = chunk_offset;

    Ok(frame_len)
}

pub(super) struct FinalizedSegmentMetadata {
    symbols: SegmentSymbols,
    series_entries: Vec<SeriesEntry>,
    postings: ExactPostingsIndex,
    label_value_time_ranges: LabelValueTimeRangeIndex,
}

pub(super) fn finalize_segment_symbol_ids(
    mut symbols: SegmentSymbols,
    mut series_entries: Vec<SeriesEntry>,
    chunk_entries: &[Vec<ChunkIndexEntry>],
) -> io::Result<FinalizedSegmentMetadata> {
    if series_entries.len() != chunk_entries.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series and chunk entry counts differ",
        ));
    }

    for entry in &mut series_entries {
        synthesize_missing_metric_name(&mut symbols, entry)?;
    }

    let (sorted_symbols, remap) = symbols.sorted_remap()?;
    for entry in &mut series_entries {
        for (key, value) in &mut entry.labels {
            *key = remap_symbol_id(&remap, *key)?;
            *value = remap_symbol_id(&remap, *value)?;
        }
        entry.labels.sort_unstable_by_key(|(key, _)| *key);
    }

    let mut postings = ExactPostingsIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    for (local_ref, entry) in series_entries.iter().enumerate() {
        let local_ref = u32::try_from(local_ref)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "series_ref exceeds u32"))?;
        for (key, value) in &entry.labels {
            postings.insert_monotonic(*key, *value, local_ref);
        }
        for chunk in &chunk_entries[local_ref as usize] {
            update_label_value_time_ranges(&mut label_value_time_ranges, entry, chunk);
        }
    }

    Ok(FinalizedSegmentMetadata {
        symbols: sorted_symbols,
        series_entries,
        postings,
        label_value_time_ranges,
    })
}

fn synthesize_missing_metric_name(
    symbols: &mut SegmentSymbols,
    entry: &mut SeriesEntry,
) -> io::Result<()> {
    let mut labels = Vec::with_capacity(entry.labels.len() + 1);
    let mut has_metric_name = false;
    for (key_sym, value_sym) in &entry.labels {
        let key = symbols.resolve(*key_sym).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series references missing key symbol",
            )
        })?;
        let value = symbols.resolve(*value_sym).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series references missing value symbol",
            )
        })?;
        if key == METRIC_NAME_LABEL {
            has_metric_name = true;
        }
        labels.push((key.to_string(), value.to_string()));
    }

    if has_metric_name {
        return Ok(());
    }

    let key_sym = symbols.intern(METRIC_NAME_LABEL);
    let value_sym = symbols.intern("");
    entry.labels.push((key_sym, value_sym));
    labels.push((METRIC_NAME_LABEL.to_string(), String::new()));
    labels.sort_by(|left, right| left.0.cmp(&right.0));
    entry.series_id = segment_series_id(&labels);
    Ok(())
}

pub(super) fn remap_symbol_id(remap: &[u32], symbol_id: u32) -> io::Result<u32> {
    remap.get(symbol_id as usize).copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "series references missing symbol id",
        )
    })
}

pub(crate) fn segment_series_id(labels: &[(String, String)]) -> u64 {
    let mut bytes = Vec::new();
    for (name, value) in labels {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);
    }
    xxhash64(&bytes)
}
