use super::*;

fn query_metric_name(labels: &QueryLabels) -> &str {
    labels
        .pairs()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value))
        .unwrap_or_default()
}

impl SegmentReader {
    #[expect(
        clippy::too_many_arguments,
        reason = "cross-segment planning keeps query bounds, budget, and label cache explicit"
    )]
    pub(in crate::storage::segment) fn plan_generic_cross_segment_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<GenericCrossSegmentPlan> {
        let empty = |projected_label_filter| GenericCrossSegmentPlan {
            projection: projection.clone(),
            projected_label_filter,
            terminal_output_names: None,
            series: Vec::new(),
            payload_requests: Vec::new(),
        };
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
        if end_ms < start_ms {
            return Ok(empty(projected_label_filter));
        }

        let candidate_refs = match self
            .selector_candidate_refs(context, matchers, projection, start_ms, end_ms, budget)?
        {
            Ok(candidate_refs) => candidate_refs,
            Err(SegmentPruneReason::MissingEquality) => {
                budget.observe_segment_skipped_by_missing_equality();
                return Ok(empty(projected_label_filter));
            }
            Err(SegmentPruneReason::MatcherTimeRange) => {
                budget.observe_segment_skipped_by_matcher_time_range();
                return Ok(empty(projected_label_filter));
            }
        };
        if candidate_refs.is_empty() {
            return Ok(empty(projected_label_filter));
        }

        struct PlannedSeriesEntry {
            series_ref: u32,
            series_id: u64,
            chunk_index: ChunkIndexRange,
            entry: Option<Arc<SeriesEntry>>,
        }
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

        let mut direct_label_entries = Vec::new();
        let mut missing_label_refs = Vec::new();
        for planned in &matched_entries {
            if !chunk_entries_by_range.contains_key(&planned.chunk_index)
                || label_cache.contains_key(&planned.series_id)
            {
                continue;
            }
            if let Some(entry) = &planned.entry {
                direct_label_entries.push(entry.as_ref());
            } else {
                missing_label_refs.push(planned.series_ref);
            }
        }
        Self::populate_series_label_cache(&context.symbols, &direct_label_entries, label_cache)?;
        if !missing_label_refs.is_empty() {
            let missing_entries = context.read_series_entries(self, &missing_label_refs)?;
            let missing_entries = missing_entries
                .iter()
                .map(|(_, entry)| entry.as_ref())
                .collect::<Vec<_>>();
            Self::populate_series_label_cache(&context.symbols, &missing_entries, label_cache)?;
        }

        let mut series = Vec::new();
        let mut payload_requests = Vec::new();
        for planned in matched_entries {
            let Some(chunk_entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            let Some(labels) = label_cache.get(&planned.series_id) else {
                continue;
            };
            let mut has_payload = false;
            for chunk_entry in chunk_entries.iter() {
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
                budget.observe_chunk_read(u64::from(read_len))?;
                payload_requests.push(ChunkPayloadRead {
                    file_id: chunk_entry.file_id,
                    offset: chunk_entry.offset,
                    len: u64::from(read_len),
                });
                has_payload = true;
            }
            if has_payload {
                let chunks = chunk_entries
                    .iter()
                    .cloned()
                    .map(|entry| {
                        IndexedChunkLocator::try_schema6_v1(planned.series_ref, entry)
                            .map_err(io::Error::other)
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                series.push(GenericCrossSegmentSeries {
                    series_id: planned.series_id,
                    metric_name_dropped_series_id: None,
                    labels: labels.clone(),
                    labels_complete: true,
                    chunks: Arc::new(chunks),
                });
            }
        }
        Ok(GenericCrossSegmentPlan {
            projection: projection.clone(),
            projected_label_filter,
            terminal_output_names: None,
            series,
            payload_requests,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "decoding keeps query bounds, budget, label state, and optional scalar cache explicit"
    )]
    pub(in crate::storage::segment) fn decode_generic_cross_segment_plan(
        &self,
        plan: GenericCrossSegmentPlan,
        chunk_payloads: &ChunkPayloadBatch,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        mut label_interner: Option<&mut QueryLabelInterner>,
        projected_label_cache: &mut ProjectedLabelCache,
        mut range_cache: Option<GenericRangeScalarCache<'_>>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let projection = &plan.projection;
        let mut results = Vec::new();
        for planned in plan.series {
            let metric_name = query_metric_name(&planned.labels);
            let owned_labels = OnceLock::<Vec<(String, String)>>::new();
            let materialized_labels = || {
                owned_labels
                    .get_or_init(|| planned.labels.to_vec())
                    .as_slice()
            };
            let mut samples = Vec::new();
            let mut projected_results: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();

            for locator in planned.chunks.iter() {
                let chunk_entry = locator.entry();
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if let Some((scalar_projection, metric_suffix)) =
                    typed_scalar_projection(projection, chunk_entry.kind)
                {
                    let projected = match label_interner.as_deref_mut() {
                        Some(label_interner) => Self::projected_scalar_series_from_query_labels(
                            projected_label_cache,
                            label_interner,
                            planned.series_id,
                            &planned.labels,
                            metric_suffix,
                        )?,
                        None => Self::projected_scalar_series(
                            projected_label_cache,
                            None,
                            planned.series_id,
                            materialized_labels(),
                            metric_name,
                            metric_suffix,
                        )?,
                    };
                    let mut result = SegmentQueryResult::with_shared_labels(
                        projected.series_id,
                        projected.labels.clone(),
                    );
                    let mut decoded_samples = 0u64;
                    let mut delta_count_accumulator = 0u64;
                    let mut delta_sum_accumulator = 0.0f64;
                    let mut delta_fragment_started = false;
                    let mut on_sample = |sample| {
                        decoded_samples = decoded_samples.saturating_add(1);
                        if let Some((
                            timestamp_ms,
                            value,
                            reset_hint,
                            temporality,
                            start_time_ms,
                            delta_interval,
                        )) = Self::project_typed_scalar_sample(
                            sample,
                            start_ms,
                            end_ms,
                            &mut delta_count_accumulator,
                            &mut delta_sum_accumulator,
                            &mut delta_fragment_started,
                        ) {
                            result.push_sample_with_counter_reset_hint_temporality_and_start_time(
                                timestamp_ms,
                                value,
                                reset_hint,
                                temporality,
                                start_time_ms,
                            );
                            if let Some(interval) = delta_interval {
                                result.mark_last_delta_projection_interval(interval);
                            }
                        }
                        Ok(())
                    };

                    let key = range_cache.as_ref().and_then(|cache| {
                        range_scalar_cache_key(
                            cache.segment_ordinal,
                            chunk_entry,
                            scalar_projection,
                        )
                    });
                    let mut processed_from_cache = false;
                    if let (Some(cache), Some(key)) = (range_cache.as_mut(), key) {
                        let cache_call = &mut *cache.call;
                        if let Some((_header, cached_samples)) = cache_call.lookup(&key) {
                            for sample in cached_samples.iter().copied() {
                                on_sample(sample)?;
                            }
                            processed_from_cache = true;
                        } else if cache_call.cache_available() {
                            let authenticated_entry =
                                chunk_payloads.authenticate_indexed_locator(locator)?;
                            let (header, _read_len) = chunk_payloads
                                .indexed_scalar_projection_header(&authenticated_entry)?;
                            let sample_count =
                                usize::try_from(header.sample_count).map_err(|_| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "chunk scalar sample count exceeds usize",
                                    )
                                })?;
                            let admission =
                                cache_call.admit_with(key, header, sample_count, |emit| {
                                    let (validated_header, _read_len) = chunk_payloads
                                        .for_each_indexed_scalar_projection_sample_with_header(
                                            &authenticated_entry,
                                            scalar_projection,
                                            |sample| {
                                                emit(sample)?;
                                                on_sample(sample)
                                            },
                                        )?;
                                    if validated_header != header {
                                        return Err(io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "chunk scalar header changed during decode",
                                        ));
                                    }
                                    Ok(())
                                })?;
                            match admission {
                                RangeScalarCacheAdmission::Admitted => {
                                    processed_from_cache = true;
                                }
                                RangeScalarCacheAdmission::AlreadyPresent => {
                                    let (_header, cached_samples) =
                                        cache_call.lookup(&key).ok_or_else(|| {
                                            io::Error::other(
                                                "existing range scalar cache entry is missing",
                                            )
                                        })?;
                                    for sample in cached_samples.iter().copied() {
                                        on_sample(sample)?;
                                    }
                                    processed_from_cache = true;
                                }
                                RangeScalarCacheAdmission::EntryTableFull
                                | RangeScalarCacheAdmission::OversizedRecord
                                | RangeScalarCacheAdmission::Unavailable => {}
                            }
                        }
                    }
                    if !processed_from_cache {
                        let authenticated_entry =
                            chunk_payloads.authenticate_indexed_locator(locator)?;
                        chunk_payloads.for_each_indexed_scalar_projection_sample(
                            &authenticated_entry,
                            scalar_projection,
                            &mut on_sample,
                        )?;
                    }
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
                let chunk_entry = chunk_payloads.authenticate_indexed_locator(locator)?;
                let record = chunk_payloads.decode_indexed_chunk_record(&chunk_entry)?;
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
                        Self::project_typed_count_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::ExponentialHistogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_typed_count_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Count, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_typed_count_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::ExponentialHistogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::Sum, ChunkSamples::Summary(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            materialized_labels(),
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
                            materialized_labels(),
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
                            materialized_labels(),
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
                            materialized_labels(),
                            quantile.as_deref(),
                            values,
                            start_ms,
                            end_ms,
                        );
                    }
                    (SegmentProjection::AllPromql { .. }, ChunkSamples::Histogram(values)) => {
                        budget.observe_samples_decoded(values.len() as u64)?;
                        Self::project_typed_count_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_histogram_bucket_samples(
                            &mut projected_results,
                            materialized_labels(),
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
                        Self::project_typed_count_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_exponential_histogram_bucket_samples(
                            &mut projected_results,
                            materialized_labels(),
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
                        Self::project_typed_count_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            materialized_labels(),
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_summary_quantile_samples(
                            &mut projected_results,
                            materialized_labels(),
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
                    && plan
                        .projected_label_filter
                        .as_ref()
                        .is_none_or(|filter| query_labels_match_compiled(&planned.labels, filter))
                {
                    samples.sort_by_key(|(ts, _)| *ts);
                    let labels = if planned.labels_complete {
                        planned.labels.clone()
                    } else {
                        let output_names =
                            plan.terminal_output_names.as_deref().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "incomplete generic labels require terminal output names",
                                )
                            })?;
                        planned.labels.clone().try_retain_names(output_names)?
                    };
                    let mut result =
                        SegmentQueryResult::with_shared_samples(planned.series_id, labels, samples);
                    if !planned.labels_complete {
                        result.mark_labels_incomplete(planned.metric_name_dropped_series_id);
                    }
                    results.push(result);
                }
                if !matches!(projection, SegmentProjection::AllPromql { .. }) {
                    continue;
                }
            }
            if let Some(label_interner) = label_interner.as_deref_mut() {
                for result in projected_results.values_mut() {
                    label_interner.intern_result_labels(std::slice::from_mut(result))?;
                }
            }
            if let Some(filter) = &plan.projected_label_filter {
                projected_results
                    .retain(|_, result| query_labels_match_compiled(&result.labels, filter));
            }
            results.extend(projected_results.into_values());
        }
        budget.observe_projected_results(&results)?;
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_scalar_metric_name_borrows_the_shared_label_atom() {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
        let labels = interner.intern_labels(vec![
            (METRIC_NAME_LABEL.to_owned(), "requests_total".to_owned()),
            ("service".to_owned(), "api".to_owned()),
        ]);
        let expected = labels
            .pairs()
            .find_map(|(name, value)| (name == METRIC_NAME_LABEL).then_some(value))
            .unwrap();

        let actual = query_metric_name(&labels);

        assert!(std::ptr::eq(actual, expected));
        assert!(!labels.owned_compatibility_materialized());
    }
}
