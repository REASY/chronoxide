use super::*;

impl SegmentReader {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let meta_path = dir.join(SegmentFile::MetaJson.filename());
        let meta_bytes = fs::read(meta_path)?;
        let meta = serde_json::from_slice(&meta_bytes).map_err(io::Error::other)?;
        Ok(Self {
            dir,
            meta,
            query_cache: Arc::new(SegmentReaderQueryCache::default()),
        })
    }

    pub fn open_validated(dir: impl AsRef<Path>) -> io::Result<Self> {
        let reader = Self::open(dir)?;
        validate_segment_footer(&reader.dir)?;
        Ok(reader)
    }

    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    pub fn file_path(&self, file: SegmentFile) -> PathBuf {
        self.dir.join(file.filename())
    }

    pub(super) fn cached_index_reader(&self) -> io::Result<CachedIndexReader> {
        let mut cached = self
            .query_cache
            .index_reader
            .lock()
            .map_err(|_| io::Error::other("segment index reader cache lock poisoned"))?;
        if let Some(reader) = cached.as_ref() {
            return Ok(CachedIndexReader {
                reader: reader.try_clone_reader()?,
                cache_hit: true,
                file_bytes: 0,
                open_elapsed: Duration::ZERO,
                open_read_stats: crate::storage::index::SegmentIndexReadStats::default(),
            });
        }

        let path = self.file_path(SegmentFile::Indexes);
        let file_bytes = file_len(&path)?;
        let start = Instant::now();
        let reader = SegmentIndexReader::open(File::open(path)?)?;
        let open_elapsed = start.elapsed();
        let open_read_stats = reader.read_stats();
        let cloned = reader.try_clone_reader()?;
        *cached = Some(reader);
        Ok(CachedIndexReader {
            reader: cloned,
            cache_hit: false,
            file_bytes,
            open_elapsed,
            open_read_stats,
        })
    }

    pub(super) fn cached_symbols(&self) -> io::Result<CachedSymbols> {
        let mut cached = self
            .query_cache
            .symbols
            .lock()
            .map_err(|_| io::Error::other("segment symbols cache lock poisoned"))?;
        if let Some(symbols) = cached.as_ref() {
            return Ok(CachedSymbols {
                symbols: Arc::clone(symbols),
                cache_hit: true,
                file_bytes: 0,
                open_elapsed: Duration::ZERO,
            });
        }

        let path = self.file_path(SegmentFile::Symbols);
        let file_bytes = file_len(&path)?;
        let start = Instant::now();
        let symbols = Arc::new(read_symbols_bin(File::open(path)?)?);
        let open_elapsed = start.elapsed();
        *cached = Some(Arc::clone(&symbols));
        Ok(CachedSymbols {
            symbols,
            cache_hit: false,
            file_bytes,
            open_elapsed,
        })
    }

    pub fn open_chunks(&self) -> io::Result<File> {
        File::open(self.file_path(SegmentFile::Chunks))
    }

    pub fn read_chunk_index(&self) -> io::Result<Vec<Vec<ChunkIndexEntry>>> {
        let mut file = File::open(self.file_path(SegmentFile::ChunkIndex))?;
        read_chunk_index(&mut file)
    }

    pub fn query_exact(
        &self,
        matchers: &[(&str, &str)],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers: Vec<NormalizedMatcher> = matchers
            .iter()
            .map(|(name, value)| NormalizedMatcher::Eq {
                name: (*name).to_string(),
                value: (*value).to_string(),
            })
            .collect();
        let mut budget = QueryBudget::unlimited();
        self.query_normalized(
            &matchers,
            &SegmentProjection::None,
            start_ms,
            end_ms,
            &mut budget,
        )
    }

    pub fn query_selector(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut budget = QueryBudget::unlimited();
        self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)
    }

    pub(super) fn query_selector_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers = selector.normalized_matchers();
        self.query_normalized(&matchers, &selector.projection, start_ms, end_ms, budget)
    }

    pub fn metric_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metric_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_values(label_name, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    pub(super) fn collect_smoke_report(
        &self,
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
        collect_totals: bool,
        report: &mut SegmentStoreSmokeReport,
    ) -> io::Result<()> {
        if !collect_totals
            && self.meta.chunk_summary.as_ref().is_some_and(|summary| {
                report.sample_limits_reached_for_summary(summary, sample_limit_per_kind)
            })
        {
            return Ok(());
        }

        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let mut series_reader =
            SeriesReader::open(File::open(self.file_path(SegmentFile::Series))?)?;
        let mut chunk_index_reader =
            ChunkIndexReader::open(File::open(self.file_path(SegmentFile::ChunkIndex))?)?;
        let mut chunk_file = self.open_chunks()?;

        if collect_totals {
            chunk_index_reader.for_each_series_entries(|series_ref, entries| {
                Self::collect_smoke_entries_for_series(
                    &self.meta.segment_id,
                    series_ref,
                    entries,
                    start_ms,
                    end_ms,
                    sample_limit_per_kind,
                    collect_totals,
                    report,
                    &symbols,
                    &mut series_reader,
                    &mut chunk_file,
                )
            })?;
        } else {
            for series_ref in 0..chunk_index_reader.len() {
                if self.meta.chunk_summary.as_ref().is_some_and(|summary| {
                    report.sample_limits_reached_for_summary(summary, sample_limit_per_kind)
                }) {
                    break;
                }
                let series_ref = u32::try_from(series_ref).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "series_ref exceeds u32")
                })?;
                let Some(entries) = chunk_index_reader.read_entries(series_ref)? else {
                    continue;
                };
                Self::collect_smoke_entries_for_series(
                    &self.meta.segment_id,
                    series_ref,
                    &entries,
                    start_ms,
                    end_ms,
                    sample_limit_per_kind,
                    collect_totals,
                    report,
                    &symbols,
                    &mut series_reader,
                    &mut chunk_file,
                )?;
            }
        }

        Ok(())
    }

    pub(super) fn collect_smoke_entries_for_series(
        segment_id: &str,
        series_ref: u32,
        entries: &[ChunkIndexEntry],
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
        collect_totals: bool,
        report: &mut SegmentStoreSmokeReport,
        symbols: &SegmentSymbols,
        series_reader: &mut SeriesReader<File>,
        chunk_file: &mut File,
    ) -> io::Result<()> {
        let mut resolved_entry: Option<(SeriesEntry, Vec<(String, String)>)> = None;
        for entry in entries {
            if !chunk_overlaps_range(entry, start_ms, end_ms) {
                continue;
            }

            let chunk_bytes = u64::from(entry.length);
            if collect_totals {
                report.totals.chunks = report.totals.chunks.saturating_add(1);
                report.totals.chunk_bytes = report.totals.chunk_bytes.saturating_add(chunk_bytes);
                report.totals.by_kind.add_chunk(entry.kind, chunk_bytes);
            }

            if sample_limit_per_kind == 0
                || report.sample_count_for_kind(entry.kind) >= sample_limit_per_kind
            {
                continue;
            }

            if resolved_entry.is_none() {
                let Some(series_entry) = series_reader.read_entry(series_ref)? else {
                    continue;
                };
                let labels = Self::resolve_series_labels(symbols, &series_entry)?;
                resolved_entry = Some((series_entry, labels));
            }
            let Some((series_entry, labels)) = resolved_entry.as_ref() else {
                continue;
            };
            let record = read_chunk_record_at(chunk_file, entry.offset, entry.length)?;
            report.sample_series.push(smoke_series_sample(
                segment_id.to_string(),
                series_ref,
                series_entry.series_id,
                labels.clone(),
                &record,
                entry.length,
            ));
        }
        Ok(())
    }

    pub(super) fn query_normalized(
        &self,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut context = SegmentQueryContext::open(self, None)?;
        let mut label_cache = SeriesLabelCache::default();
        let mut projected_label_cache = ProjectedLabelCache::default();
        self.query_normalized_with_context(
            &mut context,
            matchers,
            projection,
            start_ms,
            end_ms,
            budget,
            &mut label_cache,
            &mut projected_label_cache,
        )
    }

    pub(super) fn query_normalized_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        projected_label_cache: &mut ProjectedLabelCache,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => Some(compile_label_matchers(matchers)?),
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::NativeHistogram
            | SegmentProjection::NativeExponentialHistogram
            | SegmentProjection::SummaryQuantile { .. } => None,
        };

        let equality_matchers =
            match plan_positive_equality_matchers(context, matchers, start_ms, end_ms)? {
                Ok(equality_matchers) => equality_matchers,
                Err(SegmentPruneReason::MissingEquality) => {
                    budget.observe_segment_skipped_by_missing_equality();
                    return Ok(Vec::new());
                }
                Err(SegmentPruneReason::MatcherTimeRange) => {
                    budget.observe_segment_skipped_by_matcher_time_range();
                    return Ok(Vec::new());
                }
            };
        budget.observe_segment_queried();

        let mut candidates: Option<Vec<u32>> = None;
        for matcher in &equality_matchers {
            let positive = self.positive_equality_candidates(
                context,
                candidates.as_deref(),
                matcher,
                start_ms,
                end_ms,
                budget,
            )?;

            if positive.is_empty() {
                return Ok(Vec::new());
            }
            candidates = Some(positive);
        }

        for matcher in matchers {
            let positive = match matcher {
                NormalizedMatcher::Eq { .. } => None,
                NormalizedMatcher::Regex { name, pattern } => Some(regex_postings(
                    name,
                    pattern,
                    &context.symbols,
                    &mut context.index_reader,
                    start_ms,
                    end_ms,
                    budget,
                    &mut context.profile,
                    projection_matches_promql_metric_name_regex(projection)
                        && name == METRIC_NAME_LABEL,
                )?),
                NormalizedMatcher::NotEq { .. } | NormalizedMatcher::NotRegex { .. } => None,
            };

            if let Some(positive) = positive {
                if positive.is_empty() {
                    return Ok(Vec::new());
                }
                candidates = Some(match candidates {
                    Some(existing) => intersect_sorted(&existing, &positive),
                    None => positive,
                });
            }
        }

        let series_count = u32::try_from(self.meta.series).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment series count exceeds local reference range",
            )
        })?;
        let mut candidate_refs = candidates.unwrap_or_else(|| (0..series_count).collect());
        for matcher in matchers {
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let (Some(name_sym), Some(value_sym)) =
                        (context.symbols.lookup(name), context.symbols.lookup(value))
                    else {
                        continue;
                    };
                    let Some(selection) = context
                        .index_reader
                        .select_exact_postings(name_sym, value_sym)?
                    else {
                        continue;
                    };
                    let postings = selection.metadata();
                    if !postings.time_range.overlaps(start_ms, end_ms) {
                        continue;
                    }
                    let posting = exact_postings_with_budget(
                        &context.index_reader,
                        selection,
                        budget,
                        &mut context.profile,
                    )?;
                    candidate_refs = subtract_sorted(&candidate_refs, &posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = regex_postings(
                        name,
                        pattern,
                        &context.symbols,
                        &mut context.index_reader,
                        start_ms,
                        end_ms,
                        budget,
                        &mut context.profile,
                        false,
                    )?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        budget.observe_candidate_series_refs(candidate_refs.len() as u64)?;

        struct PlannedSeriesEntry {
            series_ref: u32,
            series_id: u64,
            chunk_index: ChunkIndexRange,
            entry: Option<Arc<SeriesEntry>>,
        }

        let mut results = Vec::new();
        let mut matched_entries = Vec::new();

        if matches!(projection, SegmentProjection::AllPromql { .. }) {
            for (series_ref, entry) in context.read_series_entries(self, &candidate_refs)? {
                if !series_kind_mask_matches_projection(projection, entry.kind_mask) {
                    continue;
                }
                budget.observe_matched_series(entry.series_id)?;
                matched_entries.push(PlannedSeriesEntry {
                    series_ref,
                    series_id: entry.series_id,
                    chunk_index: entry.chunk_index,
                    entry: Some(entry),
                });
            }
        } else {
            for (series_ref, metadata) in
                context.read_series_metadata_entries(self, &candidate_refs)?
            {
                if !series_kind_mask_matches_projection(projection, metadata.kind_mask) {
                    continue;
                }
                budget.observe_matched_series(metadata.series_id)?;
                matched_entries.push(PlannedSeriesEntry {
                    series_ref,
                    series_id: metadata.series_id,
                    chunk_index: metadata.chunk_index,
                    entry: None,
                });
            }
        }

        let chunk_ranges = matched_entries
            .iter()
            .map(|entry| entry.chunk_index)
            .collect::<Vec<_>>();
        let chunk_entries_by_range = context.read_chunk_entry_ranges(self, &chunk_ranges)?;

        let mut missing_label_refs = Vec::new();
        for planned in &matched_entries {
            if !chunk_entries_by_range.contains_key(&planned.chunk_index)
                || label_cache.contains_key(&planned.series_id)
            {
                continue;
            }

            if let Some(entry) = &planned.entry {
                let labels =
                    shared_query_labels(Self::resolve_series_labels(&context.symbols, entry)?);
                label_cache.insert(planned.series_id, labels);
            } else {
                missing_label_refs.push(planned.series_ref);
            }
        }
        if !missing_label_refs.is_empty() {
            for (_, entry) in context.read_series_entries(self, &missing_label_refs)? {
                if label_cache.contains_key(&entry.series_id) {
                    continue;
                }
                let labels =
                    shared_query_labels(Self::resolve_series_labels(&context.symbols, &entry)?);
                label_cache.insert(entry.series_id, labels);
            }
        }

        let mut chunk_payload_requests = Vec::new();
        for planned in &matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            if !label_cache.contains_key(&planned.series_id) {
                continue;
            }

            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                let read_len = if typed_scalar_projection(projection, chunk_entry.kind).is_some() {
                    chunk_entry.scalar_projection_read_len()
                } else if chunk_kind_matches_projection(projection, chunk_entry.kind) {
                    chunk_entry.length
                } else {
                    continue;
                };
                let read_len = u64::from(read_len);
                budget.observe_chunk_read(read_len)?;
                chunk_payload_requests.push(ChunkPayloadRead {
                    offset: chunk_entry.offset,
                    len: read_len,
                });
            }
        }
        let chunk_payloads = context.read_chunk_payload_batch(self, &chunk_payload_requests)?;

        for planned in matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };

            let Some(shared_labels) = label_cache.get(&planned.series_id) else {
                continue;
            };
            let labels = shared_labels.as_ref();
            let metric_name = labels
                .iter()
                .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
                .unwrap_or_default();

            let mut samples = Vec::new();
            let mut projected_results: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if let Some((scalar_projection, metric_suffix)) =
                    typed_scalar_projection(projection, chunk_entry.kind)
                {
                    let projected = Self::projected_scalar_series(
                        projected_label_cache,
                        planned.series_id,
                        &labels,
                        metric_name,
                        metric_suffix,
                    );
                    let mut result = SegmentQueryResult::with_shared_labels(
                        projected.series_id,
                        projected.labels.clone(),
                    );
                    let mut decoded_samples = 0u64;
                    let mut delta_count_accumulator = 0u64;
                    let mut delta_sum_accumulator = 0.0f64;
                    let mut delta_fragment_started = false;
                    chunk_payloads.for_each_indexed_scalar_projection_sample(
                        chunk_entry,
                        scalar_projection,
                        |sample| {
                            decoded_samples = decoded_samples.saturating_add(1);
                            if let Some((
                                timestamp_ms,
                                value,
                                reset_hint,
                                temporality,
                                start_time_ms,
                            )) = Self::project_typed_scalar_sample(
                                sample,
                                start_ms,
                                end_ms,
                                &mut delta_count_accumulator,
                                &mut delta_sum_accumulator,
                                &mut delta_fragment_started,
                            ) {
                                result
                                    .push_sample_with_counter_reset_hint_temporality_and_start_time(
                                        timestamp_ms,
                                        value,
                                        reset_hint,
                                        temporality,
                                        start_time_ms,
                                    );
                            }
                            Ok(())
                        },
                    )?;
                    budget.observe_typed_scalar_chunk_decoded();
                    budget.observe_samples_decoded(decoded_samples)?;
                    if !result.samples.is_empty() {
                        match projected_results.entry(result.series_id) {
                            std::collections::btree_map::Entry::Occupied(mut entry) => {
                                entry.get_mut().extend_from(result);
                            }
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert(result);
                            }
                        }
                    }
                    continue;
                }
                if !chunk_kind_matches_projection(projection, chunk_entry.kind) {
                    continue;
                }
                let record =
                    chunk_payloads.decode_chunk_record(chunk_entry.offset, chunk_entry.length)?;
                if chunk_kind_is_typed(record.kind) {
                    budget.observe_typed_full_chunk_decoded();
                }
                match (projection, record.samples) {
                    (
                        SegmentProjection::None | SegmentProjection::AllPromql { .. },
                        ChunkSamples::Float(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms),
                        );
                    }
                    (
                        SegmentProjection::None | SegmentProjection::AllPromql { .. },
                        ChunkSamples::Int64(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        samples.extend(
                            values
                                .into_iter()
                                .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms)
                                .map(|(ts, value)| (ts, value as f64)),
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::ExponentialHistogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_exponential_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::ExponentialHistogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_exponential_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::HistogramBucket { le, .. },
                        ChunkSamples::Histogram(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        let le_filter = compile_bucket_le_filter(le)?;
                        Self::project_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            &le_filter,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::HistogramBucket {
                            le,
                            exponential_histogram_boundaries,
                        },
                        ChunkSamples::ExponentialHistogram(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        let le_filter = compile_bucket_le_filter(le)?;
                        Self::project_exponential_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            &le_filter,
                            exponential_histogram_boundaries,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::SummaryQuantile { quantile },
                        ChunkSamples::Summary(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_quantile_samples(
                            &mut projected_results,
                            &labels,
                            quantile.as_deref(),
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::AllPromql { .. }, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            &CompiledBucketLeFilter::All,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (
                        SegmentProjection::AllPromql {
                            exponential_histogram_boundaries,
                        },
                        ChunkSamples::ExponentialHistogram(values),
                    ) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_exponential_histogram_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_exponential_histogram_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_exponential_histogram_bucket_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            &CompiledBucketLeFilter::All,
                            exponential_histogram_boundaries,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::AllPromql { .. }, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_summary_count_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_summary_sum_samples(
                            &mut projected_results,
                            &labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_summary_quantile_samples(
                            &mut projected_results,
                            &labels,
                            None,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (_, ChunkSamples::Float(_))
                    | (_, ChunkSamples::Int64(_))
                    | (_, ChunkSamples::Histogram(_))
                    | (_, ChunkSamples::ExponentialHistogram(_))
                    | (_, ChunkSamples::Summary(_)) => {}
                }
            }

            if matches!(
                projection,
                SegmentProjection::None | SegmentProjection::AllPromql { .. }
            ) {
                if !samples.is_empty()
                    && projected_label_filter
                        .as_ref()
                        .is_none_or(|filter| labels_match_compiled(&labels, filter))
                {
                    samples.sort_by_key(|(ts, _)| *ts);
                    results.push(SegmentQueryResult::with_shared_samples(
                        planned.series_id,
                        shared_labels.clone(),
                        samples,
                    ));
                }
                if !matches!(projection, SegmentProjection::AllPromql { .. }) {
                    continue;
                }
            }

            if let Some(filter) = &projected_label_filter {
                projected_results.retain(|_, result| labels_match_compiled(&result.labels, filter));
            }
            results.extend(projected_results.into_values());
        }

        budget.observe_projected_results(&results)?;
        Ok(results)
    }

    pub(super) fn query_native_histogram_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let mut context = SegmentQueryContext::open(self, None)?;
        let matchers = selector.normalized_matchers();
        let mut label_cache = SeriesLabelCache::default();
        self.query_native_histogram_normalized_with_context(
            &mut context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            &mut label_cache,
        )
    }

    pub(super) fn query_native_histogram_normalized_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let projection = SegmentProjection::NativeHistogram;
        let equality_matchers =
            match plan_positive_equality_matchers(context, matchers, start_ms, end_ms)? {
                Ok(equality_matchers) => equality_matchers,
                Err(SegmentPruneReason::MissingEquality) => {
                    budget.observe_segment_skipped_by_missing_equality();
                    return Ok(Vec::new());
                }
                Err(SegmentPruneReason::MatcherTimeRange) => {
                    budget.observe_segment_skipped_by_matcher_time_range();
                    return Ok(Vec::new());
                }
            };
        budget.observe_segment_queried();

        let mut candidates: Option<Vec<u32>> = None;
        for matcher in &equality_matchers {
            let positive = self.positive_equality_candidates(
                context,
                candidates.as_deref(),
                matcher,
                start_ms,
                end_ms,
                budget,
            )?;

            if positive.is_empty() {
                return Ok(Vec::new());
            }
            candidates = Some(positive);
        }

        for matcher in matchers {
            let positive = match matcher {
                NormalizedMatcher::Eq { .. } => None,
                NormalizedMatcher::Regex { name, pattern } => Some(regex_postings(
                    name,
                    pattern,
                    &context.symbols,
                    &mut context.index_reader,
                    start_ms,
                    end_ms,
                    budget,
                    &mut context.profile,
                    false,
                )?),
                NormalizedMatcher::NotEq { .. } | NormalizedMatcher::NotRegex { .. } => None,
            };

            if let Some(positive) = positive {
                if positive.is_empty() {
                    return Ok(Vec::new());
                }
                candidates = Some(match candidates {
                    Some(existing) => intersect_sorted(&existing, &positive),
                    None => positive,
                });
            }
        }

        let series_count = u32::try_from(self.meta.series).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment series count exceeds local reference range",
            )
        })?;
        let mut candidate_refs = candidates.unwrap_or_else(|| (0..series_count).collect());
        for matcher in matchers {
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let (Some(name_sym), Some(value_sym)) =
                        (context.symbols.lookup(name), context.symbols.lookup(value))
                    else {
                        continue;
                    };
                    let Some(selection) = context
                        .index_reader
                        .select_exact_postings(name_sym, value_sym)?
                    else {
                        continue;
                    };
                    let postings = selection.metadata();
                    if !postings.time_range.overlaps(start_ms, end_ms) {
                        continue;
                    }
                    let posting = exact_postings_with_budget(
                        &context.index_reader,
                        selection,
                        budget,
                        &mut context.profile,
                    )?;
                    candidate_refs = subtract_sorted(&candidate_refs, &posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = regex_postings(
                        name,
                        pattern,
                        &context.symbols,
                        &mut context.index_reader,
                        start_ms,
                        end_ms,
                        budget,
                        &mut context.profile,
                        false,
                    )?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        budget.observe_candidate_series_refs(candidate_refs.len() as u64)?;

        struct PlannedSeriesEntry {
            series_ref: u32,
            series_id: u64,
            chunk_index: ChunkIndexRange,
        }

        let mut matched_entries = Vec::new();
        for (series_ref, metadata) in context.read_series_metadata_entries(self, &candidate_refs)? {
            if !series_kind_mask_matches_projection(&projection, metadata.kind_mask) {
                continue;
            }
            budget.observe_matched_series(metadata.series_id)?;
            matched_entries.push(PlannedSeriesEntry {
                series_ref,
                series_id: metadata.series_id,
                chunk_index: metadata.chunk_index,
            });
        }

        let chunk_ranges = matched_entries
            .iter()
            .map(|entry| entry.chunk_index)
            .collect::<Vec<_>>();
        let chunk_entries_by_range = context.read_chunk_entry_ranges(self, &chunk_ranges)?;

        let mut missing_label_refs = Vec::new();
        for planned in &matched_entries {
            if !chunk_entries_by_range.contains_key(&planned.chunk_index)
                || label_cache.contains_key(&planned.series_id)
            {
                continue;
            }
            missing_label_refs.push(planned.series_ref);
        }
        if !missing_label_refs.is_empty() {
            for (_, entry) in context.read_series_entries(self, &missing_label_refs)? {
                if label_cache.contains_key(&entry.series_id) {
                    continue;
                }
                let labels =
                    shared_query_labels(Self::resolve_series_labels(&context.symbols, &entry)?);
                label_cache.insert(entry.series_id, labels);
            }
        }

        let mut chunk_payload_requests = Vec::new();
        for planned in &matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            if !label_cache.contains_key(&planned.series_id) {
                continue;
            }

            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if !chunk_kind_matches_projection(&projection, chunk_entry.kind) {
                    continue;
                }
                let read_len = u64::from(chunk_entry.length);
                budget.observe_chunk_read(read_len)?;
                chunk_payload_requests.push(ChunkPayloadRead {
                    offset: chunk_entry.offset,
                    len: read_len,
                });
            }
        }
        let chunk_payloads = context.read_chunk_payload_batch(self, &chunk_payload_requests)?;

        let mut results = Vec::new();
        for planned in matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            let Some(shared_labels) = label_cache.get(&planned.series_id) else {
                continue;
            };
            let mut result = PromqlHistogramSeries::new(planned.series_id, shared_labels.clone());

            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if !chunk_kind_matches_projection(&projection, chunk_entry.kind) {
                    continue;
                }
                let record =
                    chunk_payloads.decode_chunk_record(chunk_entry.offset, chunk_entry.length)?;
                if chunk_kind_is_typed(record.kind) {
                    budget.observe_typed_full_chunk_decoded();
                }
                if let ChunkSamples::Histogram(values) = record.samples {
                    budget.observe_samples_decoded(values.len() as u64)?;
                    for (timestamp_ms, value) in values {
                        if timestamp_ms < start_ms || timestamp_ms > end_ms {
                            continue;
                        }
                        result.push_sample(PromqlHistogramSample::from_histogram_value(
                            timestamp_ms,
                            value,
                        ));
                    }
                }
            }

            if !result.samples.is_empty() {
                budget.observe_projected_series(result.series_id)?;
                results.push(result);
            }
        }

        Ok(merge_histogram_query_results(results))
    }

    pub(super) fn query_native_exponential_histogram_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let mut context = SegmentQueryContext::open(self, None)?;
        let matchers = selector.normalized_matchers();
        let mut label_cache = SeriesLabelCache::default();
        self.query_native_exponential_histogram_normalized_with_context(
            &mut context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            &mut label_cache,
        )
    }

    pub(super) fn query_native_exponential_histogram_normalized_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let projection = SegmentProjection::NativeExponentialHistogram;
        let equality_matchers =
            match plan_positive_equality_matchers(context, matchers, start_ms, end_ms)? {
                Ok(equality_matchers) => equality_matchers,
                Err(SegmentPruneReason::MissingEquality) => {
                    budget.observe_segment_skipped_by_missing_equality();
                    return Ok(Vec::new());
                }
                Err(SegmentPruneReason::MatcherTimeRange) => {
                    budget.observe_segment_skipped_by_matcher_time_range();
                    return Ok(Vec::new());
                }
            };
        budget.observe_segment_queried();

        let mut candidates: Option<Vec<u32>> = None;
        for matcher in &equality_matchers {
            let positive = self.positive_equality_candidates(
                context,
                candidates.as_deref(),
                matcher,
                start_ms,
                end_ms,
                budget,
            )?;

            if positive.is_empty() {
                return Ok(Vec::new());
            }
            candidates = Some(positive);
        }

        for matcher in matchers {
            let positive = match matcher {
                NormalizedMatcher::Eq { .. } => None,
                NormalizedMatcher::Regex { name, pattern } => Some(regex_postings(
                    name,
                    pattern,
                    &context.symbols,
                    &mut context.index_reader,
                    start_ms,
                    end_ms,
                    budget,
                    &mut context.profile,
                    false,
                )?),
                NormalizedMatcher::NotEq { .. } | NormalizedMatcher::NotRegex { .. } => None,
            };

            if let Some(positive) = positive {
                if positive.is_empty() {
                    return Ok(Vec::new());
                }
                candidates = Some(match candidates {
                    Some(existing) => intersect_sorted(&existing, &positive),
                    None => positive,
                });
            }
        }

        let series_count = u32::try_from(self.meta.series).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment series count exceeds local reference range",
            )
        })?;
        let mut candidate_refs = candidates.unwrap_or_else(|| (0..series_count).collect());
        for matcher in matchers {
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let (Some(name_sym), Some(value_sym)) =
                        (context.symbols.lookup(name), context.symbols.lookup(value))
                    else {
                        continue;
                    };
                    let Some(selection) = context
                        .index_reader
                        .select_exact_postings(name_sym, value_sym)?
                    else {
                        continue;
                    };
                    let postings = selection.metadata();
                    if !postings.time_range.overlaps(start_ms, end_ms) {
                        continue;
                    }
                    let posting = exact_postings_with_budget(
                        &context.index_reader,
                        selection,
                        budget,
                        &mut context.profile,
                    )?;
                    candidate_refs = subtract_sorted(&candidate_refs, &posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = regex_postings(
                        name,
                        pattern,
                        &context.symbols,
                        &mut context.index_reader,
                        start_ms,
                        end_ms,
                        budget,
                        &mut context.profile,
                        false,
                    )?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        budget.observe_candidate_series_refs(candidate_refs.len() as u64)?;

        struct PlannedSeriesEntry {
            series_ref: u32,
            series_id: u64,
            chunk_index: ChunkIndexRange,
        }

        let mut matched_entries = Vec::new();
        for (series_ref, metadata) in context.read_series_metadata_entries(self, &candidate_refs)? {
            if !series_kind_mask_matches_projection(&projection, metadata.kind_mask) {
                continue;
            }
            budget.observe_matched_series(metadata.series_id)?;
            matched_entries.push(PlannedSeriesEntry {
                series_ref,
                series_id: metadata.series_id,
                chunk_index: metadata.chunk_index,
            });
        }

        let chunk_ranges = matched_entries
            .iter()
            .map(|entry| entry.chunk_index)
            .collect::<Vec<_>>();
        let chunk_entries_by_range = context.read_chunk_entry_ranges(self, &chunk_ranges)?;

        let mut missing_label_refs = Vec::new();
        for planned in &matched_entries {
            if !chunk_entries_by_range.contains_key(&planned.chunk_index)
                || label_cache.contains_key(&planned.series_id)
            {
                continue;
            }
            missing_label_refs.push(planned.series_ref);
        }
        if !missing_label_refs.is_empty() {
            for (_, entry) in context.read_series_entries(self, &missing_label_refs)? {
                if label_cache.contains_key(&entry.series_id) {
                    continue;
                }
                let labels =
                    shared_query_labels(Self::resolve_series_labels(&context.symbols, &entry)?);
                label_cache.insert(entry.series_id, labels);
            }
        }

        let mut chunk_payload_requests = Vec::new();
        for planned in &matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            if !label_cache.contains_key(&planned.series_id) {
                continue;
            }

            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if !chunk_kind_matches_projection(&projection, chunk_entry.kind) {
                    continue;
                }
                let read_len = u64::from(chunk_entry.length);
                budget.observe_chunk_read(read_len)?;
                chunk_payload_requests.push(ChunkPayloadRead {
                    offset: chunk_entry.offset,
                    len: read_len,
                });
            }
        }
        let chunk_payloads = context.read_chunk_payload_batch(self, &chunk_payload_requests)?;

        let mut results = Vec::new();
        for planned in matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            let Some(shared_labels) = label_cache.get(&planned.series_id) else {
                continue;
            };
            let mut result =
                PromqlExponentialHistogramSeries::new(planned.series_id, shared_labels.clone());

            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if !chunk_kind_matches_projection(&projection, chunk_entry.kind) {
                    continue;
                }
                let record =
                    chunk_payloads.decode_chunk_record(chunk_entry.offset, chunk_entry.length)?;
                if chunk_kind_is_typed(record.kind) {
                    budget.observe_typed_full_chunk_decoded();
                }
                if let ChunkSamples::ExponentialHistogram(values) = record.samples {
                    budget.observe_samples_decoded(values.len() as u64)?;
                    for (timestamp_ms, value) in values {
                        if timestamp_ms < start_ms || timestamp_ms > end_ms {
                            continue;
                        }
                        result.push_sample(
                            PromqlExponentialHistogramSample::from_exponential_histogram_value(
                                timestamp_ms,
                                value,
                            ),
                        );
                    }
                }
            }

            if !result.samples.is_empty() {
                budget.observe_projected_series(result.series_id)?;
                results.push(result);
            }
        }

        Ok(merge_exponential_histogram_query_results(results))
    }

    pub(super) fn prefetch_normalized_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        prefetch_stats: &mut QueryDataPrefetchStats,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        let equality_matchers =
            match plan_positive_equality_matchers(context, matchers, start_ms, end_ms)? {
                Ok(equality_matchers) => equality_matchers,
                Err(SegmentPruneReason::MissingEquality) => {
                    budget.observe_segment_skipped_by_missing_equality();
                    return Ok(());
                }
                Err(SegmentPruneReason::MatcherTimeRange) => {
                    budget.observe_segment_skipped_by_matcher_time_range();
                    return Ok(());
                }
            };
        budget.observe_segment_queried();

        let mut candidates: Option<Vec<u32>> = None;
        for matcher in &equality_matchers {
            let positive = self.positive_equality_candidates(
                context,
                candidates.as_deref(),
                matcher,
                start_ms,
                end_ms,
                budget,
            )?;

            if positive.is_empty() {
                return Ok(());
            }
            candidates = Some(positive);
        }

        for matcher in matchers {
            let positive = match matcher {
                NormalizedMatcher::Eq { .. } => None,
                NormalizedMatcher::Regex { name, pattern } => Some(regex_postings(
                    name,
                    pattern,
                    &context.symbols,
                    &mut context.index_reader,
                    start_ms,
                    end_ms,
                    budget,
                    &mut context.profile,
                    projection_matches_promql_metric_name_regex(projection)
                        && name == METRIC_NAME_LABEL,
                )?),
                NormalizedMatcher::NotEq { .. } | NormalizedMatcher::NotRegex { .. } => None,
            };

            if let Some(positive) = positive {
                if positive.is_empty() {
                    return Ok(());
                }
                candidates = Some(match candidates {
                    Some(existing) => intersect_sorted(&existing, &positive),
                    None => positive,
                });
            }
        }

        let series_count = u32::try_from(self.meta.series).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment series count exceeds local reference range",
            )
        })?;
        let mut candidate_refs = candidates.unwrap_or_else(|| (0..series_count).collect());
        for matcher in matchers {
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let (Some(name_sym), Some(value_sym)) =
                        (context.symbols.lookup(name), context.symbols.lookup(value))
                    else {
                        continue;
                    };
                    let Some(selection) = context
                        .index_reader
                        .select_exact_postings(name_sym, value_sym)?
                    else {
                        continue;
                    };
                    let postings = selection.metadata();
                    if !postings.time_range.overlaps(start_ms, end_ms) {
                        continue;
                    }
                    let posting = exact_postings_with_budget(
                        &context.index_reader,
                        selection,
                        budget,
                        &mut context.profile,
                    )?;
                    candidate_refs = subtract_sorted(&candidate_refs, &posting);
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = regex_postings(
                        name,
                        pattern,
                        &context.symbols,
                        &mut context.index_reader,
                        start_ms,
                        end_ms,
                        budget,
                        &mut context.profile,
                        false,
                    )?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        budget.observe_candidate_series_refs(candidate_refs.len() as u64)?;

        let mut scratch = Vec::new();
        let mut matched_entries = Vec::new();
        for (_, entry) in context.read_series_metadata_entries(self, &candidate_refs)? {
            prefetch_stats.series_entries_read =
                prefetch_stats.series_entries_read.saturating_add(1);
            if !series_kind_mask_matches_projection(projection, entry.kind_mask) {
                continue;
            }
            budget.observe_matched_series(entry.series_id)?;
            matched_entries.push(entry);
        }

        let chunk_ranges = matched_entries
            .iter()
            .map(|entry| entry.chunk_index)
            .collect::<Vec<_>>();
        let chunk_entries_by_range = context.read_chunk_entry_ranges(self, &chunk_ranges)?;

        let mut chunk_payload_ranges = Vec::new();
        for entry in matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&entry.chunk_index) else {
                continue;
            };

            prefetch_stats.chunk_index_reads = prefetch_stats.chunk_index_reads.saturating_add(1);
            prefetch_stats.chunk_index_bytes_read = prefetch_stats
                .chunk_index_bytes_read
                .saturating_add(u64::from(entry.chunk_index.len));

            for chunk_entry in entries.iter() {
                if !chunk_overlaps_range(chunk_entry, start_ms, end_ms) {
                    continue;
                }
                let read_len = if typed_scalar_projection(projection, chunk_entry.kind).is_some() {
                    chunk_entry.scalar_projection_read_len()
                } else if chunk_kind_matches_projection(projection, chunk_entry.kind) {
                    chunk_entry.length
                } else {
                    continue;
                };
                let read_len = u64::from(read_len);
                budget.observe_chunk_read(read_len)?;
                chunk_payload_ranges.push((chunk_entry.offset, read_len));
                context.prefetch_chunk_range(self, chunk_entry.offset, read_len, &mut scratch)?;
            }
        }

        context
            .profile
            .observe_sorted_chunk_payload_ranges(&mut chunk_payload_ranges);
        Ok(())
    }

    pub(super) fn filter_candidates_by_equality_matcher(
        &self,
        context: &mut SegmentQueryContext,
        candidate_refs: &[u32],
        matcher: &ResolvedEqualityMatcher,
    ) -> io::Result<Vec<u32>> {
        let mut retained = Vec::new();
        for (series_ref, entry) in context.read_series_entries(self, candidate_refs)? {
            if series_entry_has_label(&entry, matcher.name_sym, matcher.value_sym) {
                retained.push(series_ref);
            }
        }
        Ok(retained)
    }

    fn positive_equality_candidates(
        &self,
        context: &mut SegmentQueryContext,
        candidates: Option<&[u32]>,
        matcher: &ResolvedEqualityMatcher,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<u32>> {
        if let Some(existing) = candidates
            && should_verify_equality_candidates(existing.len(), matcher.postings.byte_len)
        {
            return self.filter_candidates_by_equality_matcher(context, existing, matcher);
        }

        if let Some(metric_refs) =
            metric_series_range_candidates(self, context, matcher, start_ms, end_ms)?
        {
            return Ok(match candidates {
                Some(existing) => intersect_sorted(existing, &metric_refs),
                None => metric_refs,
            });
        }

        let posting = exact_postings_with_budget(
            &context.index_reader,
            matcher.selection,
            budget,
            &mut context.profile,
        )?;
        Ok(match candidates {
            Some(existing) => intersect_sorted(existing, &posting),
            None => posting,
        })
    }

    pub(super) fn resolve_series_labels(
        symbols: &SegmentSymbols,
        entry: &SeriesEntry,
    ) -> io::Result<Vec<(String, String)>> {
        let mut labels = Vec::with_capacity(entry.labels.len());
        for (key, value) in &entry.labels {
            let key = symbols.resolve(*key).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series key symbol missing")
            })?;
            let value = symbols.resolve(*value).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series value symbol missing")
            })?;
            labels.push((key.to_string(), value.to_string()));
        }
        Ok(labels)
    }

    pub(super) fn project_histogram_count_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, HistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_u64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_count",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        );
    }

    pub(super) fn project_exponential_histogram_count_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, ExponentialHistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_u64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_count",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        );
    }

    pub(super) fn project_summary_count_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, SummaryValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_u64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_count",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        );
    }

    pub(super) fn project_histogram_sum_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, HistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_optional_f64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_sum",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
    }

    pub(super) fn project_exponential_histogram_sum_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, ExponentialHistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_optional_f64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_sum",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
    }

    pub(super) fn project_summary_sum_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, SummaryValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        Self::project_typed_optional_f64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_sum",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata, Some(value.sum))),
            start_ms,
            end_ms,
        );
    }

    pub(super) fn project_typed_u64_counter_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &str,
        values: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let labels = Self::projected_labels(base_labels, metric_name, metric_suffix, None);
        let series_id = segment_series_id(&labels);
        let mut labels = Some(labels);
        let mut delta_accumulator = 0u64;
        let mut delta_fragment_started = false;
        for (ts, metadata, raw) in values {
            if ts > end_ms {
                continue;
            }
            let emit = ts >= start_ms;
            let (value, reset_hint) = if metadata.is_stale() {
                if metadata.temporality == OtlpAggregationTemporality::Delta {
                    delta_accumulator = 0;
                    delta_fragment_started = false;
                }
                (prometheus_stale_nan(), metadata.reset_hint)
            } else if metadata.temporality == OtlpAggregationTemporality::Delta {
                delta_accumulator = delta_accumulator.saturating_add(raw);
                let reset_hint = delta_projection_reset_hint(&mut delta_fragment_started);
                (delta_accumulator as f64, reset_hint)
            } else {
                (raw as f64, metadata.reset_hint)
            };
            if !emit {
                continue;
            }
            Self::push_projected_sample_with_cached_series_and_temporality(
                out,
                series_id,
                &mut labels,
                ts,
                value,
                reset_hint,
                metadata.temporality,
                metadata.start_time_ms,
            );
        }
    }

    pub(super) fn project_typed_optional_f64_counter_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &str,
        values: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let labels = Self::projected_labels(base_labels, metric_name, metric_suffix, None);
        let series_id = segment_series_id(&labels);
        let mut labels = Some(labels);
        let mut delta_accumulator = 0.0f64;
        let mut delta_fragment_started = false;
        for (ts, metadata, raw) in values {
            if ts > end_ms {
                continue;
            }
            let emit = ts >= start_ms;
            let (value, reset_hint) = if metadata.is_stale() {
                if metadata.temporality == OtlpAggregationTemporality::Delta {
                    delta_accumulator = 0.0;
                    delta_fragment_started = false;
                }
                (prometheus_stale_nan(), metadata.reset_hint)
            } else if let Some(raw) = raw {
                if metadata.temporality == OtlpAggregationTemporality::Delta {
                    delta_accumulator += raw;
                    let reset_hint = delta_projection_reset_hint(&mut delta_fragment_started);
                    (delta_accumulator, reset_hint)
                } else {
                    (raw, metadata.reset_hint)
                }
            } else {
                continue;
            };
            if !emit {
                continue;
            }
            Self::push_projected_sample_with_cached_series_and_temporality(
                out,
                series_id,
                &mut labels,
                ts,
                value,
                reset_hint,
                metadata.temporality,
                metadata.start_time_ms,
            );
        }
    }

    pub(super) fn project_typed_scalar_sample(
        sample: ChunkScalarSample,
        start_ms: u64,
        end_ms: u64,
        delta_count_accumulator: &mut u64,
        delta_sum_accumulator: &mut f64,
        delta_fragment_started: &mut bool,
    ) -> Option<(
        u64,
        f64,
        CounterResetHint,
        OtlpAggregationTemporality,
        Option<u64>,
    )> {
        if sample.timestamp_ms > end_ms {
            return None;
        }
        let emit = sample.timestamp_ms >= start_ms;
        let (value, reset_hint) = if sample.metadata.is_stale() {
            if sample.metadata.temporality == OtlpAggregationTemporality::Delta {
                *delta_count_accumulator = 0;
                *delta_sum_accumulator = 0.0;
                *delta_fragment_started = false;
            }
            (prometheus_stale_nan(), sample.metadata.reset_hint)
        } else {
            match sample.value {
                Some(ChunkScalarValue::Count(raw)) => {
                    if sample.metadata.temporality == OtlpAggregationTemporality::Delta {
                        *delta_count_accumulator = (*delta_count_accumulator).saturating_add(raw);
                        (
                            *delta_count_accumulator as f64,
                            delta_projection_reset_hint(delta_fragment_started),
                        )
                    } else {
                        (raw as f64, sample.metadata.reset_hint)
                    }
                }
                Some(ChunkScalarValue::Sum(raw)) => {
                    if sample.metadata.temporality == OtlpAggregationTemporality::Delta {
                        *delta_sum_accumulator += raw;
                        (
                            *delta_sum_accumulator,
                            delta_projection_reset_hint(delta_fragment_started),
                        )
                    } else {
                        (raw, sample.metadata.reset_hint)
                    }
                }
                None => return None,
            }
        };
        if !emit {
            return None;
        }
        Some((
            sample.timestamp_ms,
            value,
            reset_hint,
            sample.metadata.temporality,
            sample.metadata.start_time_ms,
        ))
    }

    pub(super) fn projected_scalar_series(
        cache: &mut ProjectedLabelCache,
        source_series_id: u64,
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &'static str,
    ) -> Arc<ProjectedSeriesLabels> {
        let key = ProjectedLabelCacheKey {
            source_series_id,
            metric_suffix,
        };
        if let Some(projected) = cache.entries.get(&key) {
            cache.hits = cache.hits.saturating_add(1);
            return Arc::clone(projected);
        }

        cache.misses = cache.misses.saturating_add(1);
        let labels = Self::projected_labels(base_labels, metric_name, metric_suffix, None);
        let series_id = segment_series_id(&labels);
        let projected = Arc::new(ProjectedSeriesLabels {
            series_id,
            labels: shared_query_labels(labels),
        });
        cache.entries.insert(key, Arc::clone(&projected));
        projected
    }

    pub(super) fn project_histogram_bucket_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        le_filter: &CompiledBucketLeFilter,
        values: Vec<(u64, HistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let mut delta_accumulators: BTreeMap<String, u64> = BTreeMap::new();
        let mut delta_fragments_started: BTreeSet<String> = BTreeSet::new();
        for (ts, value) in values {
            if ts > end_ms {
                continue;
            }
            let emit = ts >= start_ms;
            let mut cumulative = 0u64;
            for (idx, bound) in value.explicit_bounds.iter().enumerate() {
                cumulative =
                    cumulative.saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
                let le = format_promql_float_label(*bound);
                if le_filter.matches(&le) {
                    let (projected, reset_hint) = histogram_projected_bucket_value(
                        value.metadata,
                        cumulative,
                        &le,
                        &mut delta_accumulators,
                        &mut delta_fragments_started,
                    );
                    if !emit {
                        continue;
                    }
                    let labels = Self::projected_labels(
                        base_labels,
                        metric_name,
                        "_bucket",
                        Some(("le", le)),
                    );
                    Self::push_projected_sample_with_counter_reset_hint_and_temporality(
                        out,
                        labels,
                        ts,
                        projected,
                        reset_hint,
                        value.metadata.temporality,
                        value.metadata.start_time_ms,
                    );
                }
            }

            if le_filter.matches("+Inf") {
                let (projected, reset_hint) = histogram_projected_bucket_value(
                    value.metadata,
                    value.count,
                    "+Inf",
                    &mut delta_accumulators,
                    &mut delta_fragments_started,
                );
                if !emit {
                    continue;
                }
                let labels = Self::projected_labels(
                    base_labels,
                    metric_name,
                    "_bucket",
                    Some(("le", "+Inf".to_string())),
                );
                Self::push_projected_sample_with_counter_reset_hint_and_temporality(
                    out,
                    labels,
                    ts,
                    projected,
                    reset_hint,
                    value.metadata.temporality,
                    value.metadata.start_time_ms,
                );
            }
        }
    }

    pub(super) fn project_exponential_histogram_bucket_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        le_filter: &CompiledBucketLeFilter,
        boundaries: &[f64],
        values: Vec<(u64, ExponentialHistogramValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let mut delta_accumulators: BTreeMap<String, u64> = BTreeMap::new();
        let mut delta_fragments_started: BTreeSet<String> = BTreeSet::new();
        for (ts, value) in values {
            if ts > end_ms {
                continue;
            }
            let emit = ts >= start_ms;

            for boundary in boundaries {
                let le = format_promql_float_label(*boundary);
                if le_filter.matches(&le) {
                    let raw = exponential_histogram_projected_bucket_count(&value, *boundary);
                    let (projected, reset_hint) = histogram_projected_bucket_value(
                        value.metadata,
                        raw,
                        &le,
                        &mut delta_accumulators,
                        &mut delta_fragments_started,
                    );
                    if !emit {
                        continue;
                    }
                    let labels = Self::projected_labels(
                        base_labels,
                        metric_name,
                        "_bucket",
                        Some(("le", le)),
                    );
                    Self::push_projected_sample_with_counter_reset_hint_and_temporality(
                        out,
                        labels,
                        ts,
                        projected,
                        reset_hint,
                        value.metadata.temporality,
                        value.metadata.start_time_ms,
                    );
                }
            }

            if le_filter.matches("+Inf") {
                let (projected, reset_hint) = histogram_projected_bucket_value(
                    value.metadata,
                    value.count,
                    "+Inf",
                    &mut delta_accumulators,
                    &mut delta_fragments_started,
                );
                if !emit {
                    continue;
                }
                let labels = Self::projected_labels(
                    base_labels,
                    metric_name,
                    "_bucket",
                    Some(("le", "+Inf".to_string())),
                );
                Self::push_projected_sample_with_counter_reset_hint_and_temporality(
                    out,
                    labels,
                    ts,
                    projected,
                    reset_hint,
                    value.metadata.temporality,
                    value.metadata.start_time_ms,
                );
            }
        }
    }

    pub(super) fn project_summary_quantile_samples(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        quantile_filter: Option<&str>,
        values: Vec<(u64, SummaryValue)>,
        start_ms: u64,
        end_ms: u64,
    ) {
        let metric_name = base_labels
            .iter()
            .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
            .unwrap_or_default();
        for (ts, value) in values {
            if ts < start_ms || ts > end_ms {
                continue;
            }
            for quantile in value.quantiles {
                let label = format_promql_float_label(quantile.quantile);
                if quantile_filter.is_some_and(|filter| filter != label) {
                    continue;
                }
                let labels =
                    Self::projected_labels(base_labels, metric_name, "", Some(("quantile", label)));
                let projected = if value.metadata.is_stale() {
                    prometheus_stale_nan()
                } else {
                    quantile.value
                };
                Self::push_projected_sample(out, labels, ts, projected);
            }
        }
    }

    pub(super) fn projected_labels(
        base_labels: &[(String, String)],
        metric_name: &str,
        metric_suffix: &str,
        extra_label: Option<(&str, String)>,
    ) -> Vec<(String, String)> {
        let mut labels = Vec::with_capacity(base_labels.len() + usize::from(extra_label.is_some()));
        let mut metric_seen = false;
        let extra_key = extra_label.as_ref().map(|(key, _)| *key);
        for (key, value) in base_labels {
            if key == METRIC_NAME_LABEL {
                labels.push((key.clone(), format!("{metric_name}{metric_suffix}")));
                metric_seen = true;
            } else if extra_key != Some(key.as_str()) {
                labels.push((key.clone(), value.clone()));
            }
        }
        if !metric_seen {
            labels.push((
                METRIC_NAME_LABEL.to_string(),
                format!("{metric_name}{metric_suffix}"),
            ));
        }
        if let Some((key, value)) = extra_label {
            labels.push((key.to_string(), value));
        }
        labels.sort_by(|left, right| left.0.cmp(&right.0));
        labels
    }

    pub(super) fn push_projected_sample(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        labels: Vec<(String, String)>,
        timestamp_ms: u64,
        value: f64,
    ) {
        let series_id = segment_series_id(&labels);
        let entry = out
            .entry(series_id)
            .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
        entry.push_sample(timestamp_ms, value);
    }

    pub(super) fn push_projected_sample_with_counter_reset_hint_and_temporality(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        labels: Vec<(String, String)>,
        timestamp_ms: u64,
        value: f64,
        reset_hint: CounterResetHint,
        temporality: OtlpAggregationTemporality,
        start_time_ms: Option<u64>,
    ) {
        let series_id = segment_series_id(&labels);
        let entry = out
            .entry(series_id)
            .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
        entry.push_sample_with_counter_reset_hint_temporality_and_start_time(
            timestamp_ms,
            value,
            reset_hint,
            temporality,
            start_time_ms,
        );
    }

    pub(super) fn push_projected_sample_with_cached_series_and_temporality(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        series_id: u64,
        labels: &mut Option<Vec<(String, String)>>,
        timestamp_ms: u64,
        value: f64,
        reset_hint: CounterResetHint,
        temporality: OtlpAggregationTemporality,
        start_time_ms: Option<u64>,
    ) {
        let entry = out.entry(series_id).or_insert_with(|| {
            SegmentQueryResult::new(
                series_id,
                labels
                    .take()
                    .expect("projected labels must be available for first sample"),
            )
        });
        entry.push_sample_with_counter_reset_hint_temporality_and_start_time(
            timestamp_ms,
            value,
            reset_hint,
            temporality,
            start_time_ms,
        );
    }

    pub(super) fn collect_metric_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if !self.can_collect_metadata_for_range(start_ms, end_ms) {
            return Ok(());
        }

        let (symbols, mut index_reader) = self.read_symbols_and_index_reader()?;
        if !index_reader.has_label_values()? {
            return self.collect_metadata_from_series_chunks(start_ms, end_ms, metadata, &symbols);
        }

        collect_metric_names_from_index(&symbols, &mut index_reader, start_ms, end_ms, metadata)
    }

    pub(super) fn collect_label_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if !self.can_collect_metadata_for_range(start_ms, end_ms) {
            return Ok(());
        }

        let (symbols, mut index_reader) = self.read_symbols_and_index_reader()?;
        if !index_reader.has_label_values()? {
            return self.collect_metadata_from_series_chunks(start_ms, end_ms, metadata, &symbols);
        }

        collect_label_names_from_index(&symbols, &mut index_reader, start_ms, end_ms, metadata)
    }

    pub(super) fn collect_label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if !self.can_collect_metadata_for_range(start_ms, end_ms) {
            return Ok(());
        }

        let (symbols, mut index_reader) = self.read_symbols_and_index_reader()?;
        if !index_reader.has_label_values()? {
            return self.collect_metadata_from_series_chunks(start_ms, end_ms, metadata, &symbols);
        }

        collect_label_values_from_index(
            &symbols,
            &mut index_reader,
            label_name,
            start_ms,
            end_ms,
            metadata,
        )
    }

    pub(super) fn can_collect_metadata_for_range(&self, start_ms: u64, end_ms: u64) -> bool {
        end_ms >= start_ms && self.meta.end_ms >= start_ms && self.meta.start_ms <= end_ms
    }

    pub(super) fn read_symbols_and_index_reader(
        &self,
    ) -> io::Result<(SegmentSymbols, SegmentIndexReader<File>)> {
        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let index_reader =
            SegmentIndexReader::open(File::open(self.file_path(SegmentFile::Indexes))?)?;
        Ok((symbols, index_reader))
    }

    pub(super) fn collect_metadata_from_series_chunks(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
        symbols: &SegmentSymbols,
    ) -> io::Result<()> {
        let series = read_series_bin(File::open(self.file_path(SegmentFile::Series))?)?;
        let chunk_index = self.read_chunk_index()?;
        for (series_idx, entry) in series.iter().enumerate() {
            let Some(entries) = chunk_index.get(series_idx) else {
                continue;
            };
            if !entries
                .iter()
                .any(|chunk| chunk_overlaps_range(chunk, start_ms, end_ms))
            {
                continue;
            }

            let mut labels = Vec::with_capacity(entry.labels.len());
            for (key, value) in &entry.labels {
                let key = symbols.resolve(*key).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "series key symbol missing")
                })?;
                let value = symbols.resolve(*value).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "series value symbol missing")
                })?;
                labels.push((key.to_string(), value.to_string()));
            }
            metadata.add_labelset(&labels);
        }

        Ok(())
    }
}

fn metric_series_range_candidates(
    reader: &SegmentReader,
    context: &mut SegmentQueryContext,
    matcher: &ResolvedEqualityMatcher,
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Option<Vec<u32>>> {
    let Some(metric_name_sym) = context.symbols.lookup(METRIC_NAME_LABEL) else {
        return Ok(None);
    };
    if matcher.name_sym != metric_name_sym {
        return Ok(None);
    }

    let ranges = context.metric_series_ranges(reader, matcher.value_sym)?;
    metric_series_refs_from_ranges(&ranges, start_ms, end_ms).map(Some)
}

fn metric_series_refs_from_ranges(
    ranges: &[MetricSeriesRange],
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Vec<u32>> {
    let mut series_refs = Vec::new();
    let mut matched_ranges = 0usize;
    for range in ranges.iter().copied() {
        if !range.overlaps(start_ms, end_ms) {
            continue;
        }
        let end_series_ref = range
            .start_series_ref
            .checked_add(range.series_count)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range overflows u32",
                )
            })?;
        let range_len = usize::try_from(range.series_count).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "metric series range too large")
        })?;
        series_refs
            .try_reserve(range_len)
            .map_err(io::Error::other)?;
        series_refs.extend(range.start_series_ref..end_series_ref);
        matched_ranges += 1;
    }

    if matched_ranges > 1 {
        series_refs.sort_unstable();
        series_refs.dedup();
    }
    Ok(series_refs)
}

pub(super) fn delta_projection_reset_hint(started: &mut bool) -> CounterResetHint {
    if *started {
        CounterResetHint::NotCounterReset
    } else {
        *started = true;
        CounterResetHint::CounterReset
    }
}
