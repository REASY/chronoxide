use super::*;

impl SegmentReader {
    pub(in crate::storage::segment) fn prefetch_normalized_with_context(
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

    pub(in crate::storage::segment) fn filter_candidates_by_equality_matcher(
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

    pub(super) fn positive_equality_candidates(
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

    pub(in crate::storage::segment) fn resolve_series_labels(
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

    pub(in crate::storage::segment) fn project_typed_count_samples<T>(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, T)>,
        start_ms: u64,
        end_ms: u64,
    ) where
        T: TypedCounterValue,
    {
        Self::project_typed_u64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_count",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata(), value.count())),
            start_ms,
            end_ms,
        );
    }

    pub(in crate::storage::segment) fn project_typed_sum_samples<T>(
        out: &mut BTreeMap<u64, SegmentQueryResult>,
        base_labels: &[(String, String)],
        metric_name: &str,
        values: Vec<(u64, T)>,
        start_ms: u64,
        end_ms: u64,
    ) where
        T: TypedCounterValue,
    {
        Self::project_typed_optional_f64_counter_samples(
            out,
            base_labels,
            metric_name,
            "_sum",
            values
                .into_iter()
                .map(|(ts, value)| (ts, value.metadata(), value.sum())),
            start_ms,
            end_ms,
        );
    }

    pub(in crate::storage::segment) fn project_typed_u64_counter_samples(
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

    pub(in crate::storage::segment) fn project_typed_optional_f64_counter_samples(
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

    pub(in crate::storage::segment) fn project_typed_scalar_sample(
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

    pub(in crate::storage::segment) fn projected_scalar_series(
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

    pub(in crate::storage::segment) fn project_histogram_bucket_samples(
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
        let mut projected_series_by_bound: BTreeMap<u64, CachedHistogramBucketSeries> =
            BTreeMap::new();
        let mut projected_inf_series: CachedHistogramInfSeries = None;
        for (ts, value) in values {
            if ts > end_ms {
                continue;
            }
            let emit = ts >= start_ms;
            let mut cumulative = 0u64;
            for (idx, bound) in value.explicit_bounds.iter().enumerate() {
                cumulative =
                    cumulative.saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
                let projected_series = projected_series_by_bound
                    .entry(bound.to_bits())
                    .or_insert_with(|| {
                        let le = format_promql_float_label(*bound);
                        if !le_filter.matches(&le) {
                            return None;
                        }
                        let labels = Self::projected_labels(
                            base_labels,
                            metric_name,
                            "_bucket",
                            Some(("le", le.clone())),
                        );
                        Some((le, segment_series_id(&labels), Some(labels)))
                    });
                if let Some((le, series_id, labels)) = projected_series {
                    let (projected, reset_hint) = histogram_projected_bucket_value(
                        value.metadata,
                        cumulative,
                        le,
                        &mut delta_accumulators,
                        &mut delta_fragments_started,
                    );
                    if !emit {
                        continue;
                    }
                    Self::push_projected_sample_with_cached_series_and_temporality(
                        out,
                        *series_id,
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
                let (series_id, labels) = projected_inf_series.get_or_insert_with(|| {
                    let labels = Self::projected_labels(
                        base_labels,
                        metric_name,
                        "_bucket",
                        Some(("le", "+Inf".to_string())),
                    );
                    (segment_series_id(&labels), Some(labels))
                });
                Self::push_projected_sample_with_cached_series_and_temporality(
                    out,
                    *series_id,
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

    pub(in crate::storage::segment) fn project_exponential_histogram_bucket_samples(
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

    pub(in crate::storage::segment) fn project_summary_quantile_samples(
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

    pub(in crate::storage::segment) fn projected_labels(
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

    pub(in crate::storage::segment) fn push_projected_sample(
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

    pub(in crate::storage::segment) fn push_projected_sample_with_counter_reset_hint_and_temporality(
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

    pub(in crate::storage::segment) fn push_projected_sample_with_cached_series_and_temporality(
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

    pub(in crate::storage::segment) fn collect_metric_names(
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

    pub(in crate::storage::segment) fn collect_label_names(
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

    pub(in crate::storage::segment) fn collect_label_values(
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

    pub(in crate::storage::segment) fn can_collect_metadata_for_range(
        &self,
        start_ms: u64,
        end_ms: u64,
    ) -> bool {
        end_ms >= start_ms && self.meta.end_ms >= start_ms && self.meta.start_ms <= end_ms
    }

    pub(in crate::storage::segment) fn read_symbols_and_index_reader(
        &self,
    ) -> io::Result<(SegmentSymbols, SegmentIndexReader<File>)> {
        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let index_reader =
            SegmentIndexReader::open(File::open(self.file_path(SegmentFile::Indexes))?)?;
        Ok((symbols, index_reader))
    }

    pub(in crate::storage::segment) fn collect_metadata_from_series_chunks(
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
