use super::cached_query_plan::CachedQueryPlan;
use super::*;

impl SegmentReader {
    #[expect(
        clippy::too_many_arguments,
        reason = "cached query decoding keeps projection, cache, and result state explicit"
    )]
    pub(super) fn decode_cached_query_plan(
        &self,
        plan: CachedQueryPlan,
        chunk_payloads: &ChunkPayloadBatch,
        segment_ordinal: usize,
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &SeriesLabelCache,
        projected_label_cache: &mut ProjectedLabelCache,
        mut cache_call: Option<&mut RangeScalarCacheCall>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut chunk_payloads = chunk_payloads.decoder();
        let CachedQueryPlan {
            projected_label_filter,
            series: matched_entries,
            chunk_entries_by_range,
        } = plan;
        let mut results = Vec::new();
        for planned in matched_entries {
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };

            let Some(shared_labels) = label_cache.get(&planned.series_id) else {
                continue;
            };
            // Schema 6 cannot use compact source IDs. Keep its legacy typed
            // projection helpers slice-based without retaining an owned
            // compatibility view inside QueryLabels.
            let owned_labels = shared_labels.to_vec();
            let labels = owned_labels.as_slice();
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
                        None,
                        planned.series_id,
                        labels,
                        metric_name,
                        metric_suffix,
                    )?;
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

                    let key =
                        range_scalar_cache_key(segment_ordinal, chunk_entry, scalar_projection);
                    let mut processed_from_cache = false;
                    if let (Some(cache_call), Some(key)) = (cache_call.as_deref_mut(), key) {
                        if let Some((_header, cached_samples)) = cache_call.lookup(&key) {
                            for sample in cached_samples.iter().copied() {
                                on_sample(sample)?;
                            }
                            processed_from_cache = true;
                        } else if cache_call.cache_available() {
                            let (header, _read_len) =
                                chunk_payloads.indexed_scalar_projection_header(chunk_entry)?;
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
                                            chunk_entry,
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
                        chunk_payloads.for_each_indexed_scalar_projection_sample(
                            chunk_entry,
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
                let record = chunk_payloads.decode_indexed_chunk_record(chunk_entry)?;
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
                            labels,
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
                            labels,
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
                            labels,
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
                            labels,
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
                            labels,
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
                            labels,
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
                            labels,
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
                            labels,
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
                            labels,
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
                            labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_histogram_bucket_samples(
                            &mut projected_results,
                            labels,
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
                            labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_exponential_histogram_bucket_samples(
                            &mut projected_results,
                            labels,
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
                            labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_typed_sum_samples(
                            &mut projected_results,
                            labels,
                            metric_name,
                            values.clone(),
                            start_ms,
                            end_ms,
                        );
                        Self::project_summary_quantile_samples(
                            &mut projected_results,
                            labels,
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
                        .is_none_or(|filter| labels_match_compiled(labels, filter))
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
                projected_results
                    .retain(|_, result| query_labels_match_compiled(&result.labels, filter));
            }
            results.extend(projected_results.into_values());
        }

        budget.observe_projected_results(&results)?;
        Ok(results)
    }
}
