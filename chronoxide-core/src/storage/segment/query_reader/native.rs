use super::*;

impl SegmentReader {
    pub(in crate::storage::segment) fn query_native_histogram_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let mut context = self.standalone_facade_context()?;
        let matchers = selector.normalized_matchers();
        let mut label_cache = SeriesLabelCache::default();
        let mut label_interner = QueryLabelInterner::default();
        self.query_native_histogram_normalized_with_facade_context(
            &mut context,
            &matchers,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            &mut label_cache,
            &mut label_interner,
        )
    }

    #[expect(
        dead_code,
        reason = "retained schema-6 native query hook for layout comparison experiments"
    )]
    pub(in crate::storage::segment) fn query_native_histogram_normalized_with_context(
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
        let candidate_refs = match self.selector_candidate_refs(
            context,
            matchers,
            &projection,
            start_ms,
            end_ms,
            budget,
        )? {
            Ok(candidate_refs) => candidate_refs,
            Err(SegmentPruneReason::MissingEquality) => {
                budget.observe_segment_skipped_by_missing_equality();
                return Ok(Vec::new());
            }
            Err(SegmentPruneReason::MatcherTimeRange) => {
                budget.observe_segment_skipped_by_matcher_time_range();
                return Ok(Vec::new());
            }
        };
        if candidate_refs.is_empty() {
            return Ok(Vec::new());
        }

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
            let missing_entries = context.read_series_entries(self, &missing_label_refs)?;
            let missing_entries = missing_entries
                .iter()
                .map(|(_, entry)| entry.as_ref())
                .collect::<Vec<_>>();
            Self::populate_series_label_cache(&context.symbols, &missing_entries, label_cache)?;
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
                    file_id: chunk_entry.file_id,
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
                let record = chunk_payloads.decode_indexed_chunk_record(chunk_entry)?;
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

    #[expect(
        dead_code,
        reason = "retained schema-6 native planner hook for layout comparison experiments"
    )]
    pub(in crate::storage::segment) fn plan_native_histogram_cross_segment_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        self.plan_native_typed_cross_segment_with_context(
            context,
            matchers,
            SegmentProjection::NativeHistogram,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    #[expect(
        dead_code,
        reason = "retained schema-6 native planner hook for layout comparison experiments"
    )]
    pub(in crate::storage::segment) fn plan_native_exponential_histogram_cross_segment_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        self.plan_native_typed_cross_segment_with_context(
            context,
            matchers,
            SegmentProjection::NativeExponentialHistogram,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    #[expect(
        dead_code,
        clippy::too_many_arguments,
        reason = "retained schema-6 typed planner keeps query state explicit for layout experiments"
    )]
    pub(super) fn plan_native_typed_cross_segment_with_context(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        projection: SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let empty = || NativeTypedCrossSegmentPlan {
            series: Vec::new(),
            payload_requests: Vec::new(),
        };
        if end_ms < start_ms {
            return Ok(empty());
        }

        let candidate_refs = match self.selector_candidate_refs(
            context,
            matchers,
            &projection,
            start_ms,
            end_ms,
            budget,
        )? {
            Ok(candidate_refs) => candidate_refs,
            Err(SegmentPruneReason::MissingEquality) => {
                budget.observe_segment_skipped_by_missing_equality();
                return Ok(empty());
            }
            Err(SegmentPruneReason::MatcherTimeRange) => {
                budget.observe_segment_skipped_by_matcher_time_range();
                return Ok(empty());
            }
        };
        if candidate_refs.is_empty() {
            return Ok(empty());
        }

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
            let Some(entries) = chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            let Some(labels) = label_cache.get(&planned.series_id) else {
                continue;
            };
            let mut chunks = Vec::new();
            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                if !chunk_kind_matches_projection(&projection, chunk_entry.kind) {
                    continue;
                }
                let read_len = u64::from(chunk_entry.length);
                budget.observe_chunk_read(read_len)?;
                payload_requests.push(ChunkPayloadRead {
                    file_id: chunk_entry.file_id,
                    offset: chunk_entry.offset,
                    len: read_len,
                });
                chunks.push(
                    IndexedChunkLocator::try_schema6_v1(planned.series_ref, chunk_entry.clone())
                        .map_err(io::Error::other)?,
                );
            }
            if !chunks.is_empty() {
                series.push(NativeTypedCrossSegmentSeries {
                    series_id: planned.series_id,
                    metric_name_dropped_series_id: None,
                    labels: labels.clone(),
                    labels_complete: true,
                    chunks,
                });
            }
        }

        Ok(NativeTypedCrossSegmentPlan {
            series,
            payload_requests,
        })
    }

    pub(in crate::storage::segment) fn decode_native_histogram_cross_segment_plan(
        &self,
        plan: NativeTypedCrossSegmentPlan,
        chunk_payloads: &ChunkPayloadBatch,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        let mut results = Vec::new();
        for planned in plan.series {
            let mut result = PromqlHistogramSeries::new(planned.series_id, planned.labels);
            if !planned.labels_complete {
                result.mark_labels_incomplete(planned.metric_name_dropped_series_id);
            }
            for locator in planned.chunks {
                let chunk_entry = chunk_payloads.authenticate_indexed_locator(&locator)?;
                let record = chunk_payloads.decode_indexed_chunk_record(&chunk_entry)?;
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
        Ok(results)
    }

    pub(in crate::storage::segment) fn decode_native_exponential_histogram_cross_segment_plan(
        &self,
        plan: NativeTypedCrossSegmentPlan,
        chunk_payloads: &ChunkPayloadBatch,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        let mut results = Vec::new();
        for planned in plan.series {
            let mut result =
                PromqlExponentialHistogramSeries::new(planned.series_id, planned.labels);
            if !planned.labels_complete {
                result.mark_labels_incomplete(planned.metric_name_dropped_series_id);
            }
            for locator in planned.chunks {
                let chunk_entry = chunk_payloads.authenticate_indexed_locator(&locator)?;
                let record = chunk_payloads.decode_indexed_chunk_record(&chunk_entry)?;
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
        Ok(results)
    }

    pub(in crate::storage::segment) fn query_native_exponential_histogram_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let mut context = self.standalone_facade_context()?;
        let matchers = selector.normalized_matchers();
        let mut label_cache = SeriesLabelCache::default();
        let mut label_interner = QueryLabelInterner::default();
        self.query_native_exponential_histogram_normalized_with_facade_context(
            &mut context,
            &matchers,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            &mut label_cache,
            &mut label_interner,
        )
    }

    #[expect(
        dead_code,
        reason = "retained schema-6 native query hook for layout comparison experiments"
    )]
    pub(in crate::storage::segment) fn query_native_exponential_histogram_normalized_with_context(
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
        let candidate_refs = match self.selector_candidate_refs(
            context,
            matchers,
            &projection,
            start_ms,
            end_ms,
            budget,
        )? {
            Ok(candidate_refs) => candidate_refs,
            Err(SegmentPruneReason::MissingEquality) => {
                budget.observe_segment_skipped_by_missing_equality();
                return Ok(Vec::new());
            }
            Err(SegmentPruneReason::MatcherTimeRange) => {
                budget.observe_segment_skipped_by_matcher_time_range();
                return Ok(Vec::new());
            }
        };
        if candidate_refs.is_empty() {
            return Ok(Vec::new());
        }

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
            let missing_entries = context.read_series_entries(self, &missing_label_refs)?;
            let missing_entries = missing_entries
                .iter()
                .map(|(_, entry)| entry.as_ref())
                .collect::<Vec<_>>();
            Self::populate_series_label_cache(&context.symbols, &missing_entries, label_cache)?;
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
                    file_id: chunk_entry.file_id,
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
                let record = chunk_payloads.decode_indexed_chunk_record(chunk_entry)?;
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
}
