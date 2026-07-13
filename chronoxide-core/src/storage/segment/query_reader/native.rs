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

        let equality_matchers =
            match plan_positive_equality_matchers(context, matchers, start_ms, end_ms)? {
                Ok(equality_matchers) => equality_matchers,
                Err(SegmentPruneReason::MissingEquality) => {
                    budget.observe_segment_skipped_by_missing_equality();
                    return Ok(empty());
                }
                Err(SegmentPruneReason::MatcherTimeRange) => {
                    budget.observe_segment_skipped_by_matcher_time_range();
                    return Ok(empty());
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
                return Ok(empty());
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
                    return Ok(empty());
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
                    offset: chunk_entry.offset,
                    len: read_len,
                });
                chunks.push(chunk_entry.clone());
            }
            if !chunks.is_empty() {
                series.push(NativeTypedCrossSegmentSeries {
                    series_id: planned.series_id,
                    labels: labels.clone(),
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
            for chunk_entry in planned.chunks {
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
            for chunk_entry in planned.chunks {
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
}
