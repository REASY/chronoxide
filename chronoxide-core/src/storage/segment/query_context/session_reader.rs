use super::*;

pub(in crate::storage::segment) fn plan_positive_equality_matchers(
    context: &SegmentQueryContext,
    matchers: &[NormalizedMatcher],
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Result<Vec<ResolvedEqualityMatcher>, SegmentPruneReason>> {
    let mut equality_matchers = Vec::new();
    for matcher in matchers {
        let NormalizedMatcher::Eq { name, value } = matcher else {
            continue;
        };
        let Some(name_sym) = context.symbols.lookup(name) else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let Some(value_sym) = context.symbols.lookup(value) else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let Some(selection) = context
            .index_reader
            .select_exact_postings(name_sym, value_sym)?
        else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let postings = selection.metadata();
        if !postings.time_range.overlaps(start_ms, end_ms) {
            return Ok(Err(SegmentPruneReason::MatcherTimeRange));
        }
        equality_matchers.push(ResolvedEqualityMatcher {
            name_sym,
            value_sym,
            postings,
            selection,
        });
    }
    equality_matchers.sort_by_key(|matcher| matcher.postings.byte_len);
    Ok(Ok(equality_matchers))
}

pub(in crate::storage::segment) fn has_positive_equality_matcher(
    matchers: &[NormalizedMatcher],
) -> bool {
    matchers
        .iter()
        .any(|matcher| matches!(matcher, NormalizedMatcher::Eq { .. }))
}

impl<'a> SegmentQuerySessionReader<'a> {
    pub(in crate::storage::segment) fn open(
        reader: &'a SegmentReader,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> Self {
        Self {
            reader,
            context: None,
            index_routing_reader: None,
            stats: SegmentStoreQuerySessionStats::default(),
            profile: SegmentStoreQueryProfile::default(),
            chunk_reader,
        }
    }

    pub(in crate::storage::segment) fn context(&mut self) -> io::Result<&mut SegmentQueryContext> {
        if self.context.is_none() {
            let index_reader = self.index_routing_reader.take();
            self.context = Some(SegmentQueryContext::open_with_chunk_reader(
                self.reader,
                index_reader,
                Arc::clone(&self.chunk_reader),
            )?);
        }
        Ok(self.context.as_mut().unwrap())
    }

    pub(in crate::storage::segment) fn index_reader_for_routing(
        &mut self,
    ) -> io::Result<&mut SegmentIndexReader<File>> {
        if self.index_routing_reader.is_none() {
            let cached = self.reader.cached_index_reader()?;
            if !cached.cache_hit {
                self.profile.index_routing_file_bytes = self
                    .profile
                    .index_routing_file_bytes
                    .saturating_add(cached.file_bytes);
                self.profile.index_routing_open = self
                    .profile
                    .index_routing_open
                    .saturating_add(cached.open_elapsed);
                self.stats.index_routing_opens = self.stats.index_routing_opens.saturating_add(1);
            }
            self.profile.index_read_stats = self
                .profile
                .index_read_stats
                .saturating_add(cached.open_read_stats);
            self.index_routing_reader = Some(cached.reader);
        }
        Ok(self.index_routing_reader.as_mut().unwrap())
    }

    pub(in crate::storage::segment) fn plan_positive_equality_matchers_from_routing_index(
        &mut self,
        matchers: &[NormalizedMatcher],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Option<Result<(), SegmentPruneReason>>> {
        for matcher in matchers {
            let NormalizedMatcher::Eq { name, value } = matcher else {
                continue;
            };
            let reader = self.index_reader_for_routing()?;
            let start = Instant::now();
            let lookup = reader.routing_exact_postings_metadata(name, value)?;
            self.profile.routing_index_read = self
                .profile
                .routing_index_read
                .saturating_add(start.elapsed());
            self.profile.routing_index_bytes = self
                .profile
                .routing_index_bytes
                .saturating_add(lookup.bytes_read);
            if !lookup.index_present {
                return Ok(None);
            }
            let Some(postings) = lookup.metadata else {
                return Ok(Some(Err(SegmentPruneReason::MissingEquality)));
            };
            if !postings.time_range.overlaps(start_ms, end_ms) {
                return Ok(Some(Err(SegmentPruneReason::MatcherTimeRange)));
            }
        }
        Ok(Some(Ok(())))
    }

    pub(in crate::storage::segment) fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        segment_ordinal: usize,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        projected_label_cache: &mut ProjectedLabelCache,
        cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(Vec::new());
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(Vec::new());
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.query_normalized_with_context(
            context,
            segment_ordinal,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
            label_cache,
            projected_label_cache,
            cache_call,
        )
    }

    pub(in crate::storage::segment) fn plan_generic_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<GenericCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(GenericCrossSegmentPlan::empty(selector.projection.clone()));
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(GenericCrossSegmentPlan::empty(selector.projection.clone()));
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.plan_generic_cross_segment_with_context(
            context,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(in crate::storage::segment) fn query_native_histogram_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(Vec::new());
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(Vec::new());
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.query_native_histogram_normalized_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(in crate::storage::segment) fn plan_native_histogram_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.plan_native_histogram_cross_segment_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(in crate::storage::segment) fn plan_native_exponential_histogram_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.plan_native_exponential_histogram_cross_segment_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(in crate::storage::segment) fn query_native_exponential_histogram_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(Vec::new());
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(Vec::new());
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.query_native_exponential_histogram_normalized_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(in crate::storage::segment) fn prewarm_selector(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<()> {
        let matchers = selector.normalized_matchers();
        if !has_positive_equality_matcher(&matchers) {
            return Ok(());
        }

        if self.context.is_none() {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(
                        SegmentPruneReason::MissingEquality | SegmentPruneReason::MatcherTimeRange,
                    ) => {
                        return Ok(());
                    }
                }
            }
        }

        let reader = self.reader;
        let context = self.context()?;
        if plan_positive_equality_matchers(context, &matchers, start_ms, end_ms)?.is_err() {
            return Ok(());
        }
        context.prewarm_query_files(reader)
    }

    pub(in crate::storage::segment) fn prefetch_selector_data_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        prefetch_stats: &mut QueryDataPrefetchStats,
    ) -> io::Result<()> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(());
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(());
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.prefetch_normalized_with_context(
            context,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
            prefetch_stats,
        )
    }
}

pub(in crate::storage::segment) struct CrossSegmentGenericRead {
    pub(in crate::storage::segment) segment_ordinal: usize,
    pub(in crate::storage::segment) generic_plan: GenericCrossSegmentPlan,
    pub(in crate::storage::segment) payload_plan: ChunkPayloadBatchPlan,
    pub(in crate::storage::segment) file: Arc<File>,
}

pub(in crate::storage::segment) fn execute_cross_segment_generic_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentGenericRead>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
    projected_label_cache: &mut ProjectedLabelCache,
) -> io::Result<Vec<SegmentQueryResult>> {
    if group.is_empty() {
        return Ok(Vec::new());
    }
    let scheduler = ChunkReadScheduler::new(chunk_reader);
    let scheduler_items = group
        .iter()
        .map(|planned| ChunkReadSchedulerItem {
            segment_ordinal: planned.segment_ordinal,
            file: Arc::clone(&planned.file),
            plan: planned.payload_plan.clone(),
            logical_requests: planned.generic_plan.payload_requests.len() as u64,
        })
        .collect();
    let (payload_results, scheduler_stats) = scheduler.execute(scheduler_items)?;
    let first_segment_ordinal = group[0].segment_ordinal;
    segments[first_segment_ordinal]
        .context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .observe_chunk_read_scheduler(scheduler_stats);
    let mut duration_unassigned = Some(scheduler_stats.read_duration);
    let mut results = Vec::new();
    for (planned, payload_result) in group.into_iter().zip(payload_results) {
        if payload_result.segment_ordinal != planned.segment_ordinal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk scheduler changed segment result order",
            ));
        }
        let context = segments[planned.segment_ordinal]
            .context
            .as_mut()
            .expect("cross-segment plan requires an open context");
        context.observe_cross_segment_chunk_payload_read(
            duration_unassigned.take().unwrap_or(Duration::ZERO),
            &planned.payload_plan,
        );
        results.extend(
            segments[planned.segment_ordinal]
                .reader
                .decode_generic_cross_segment_plan(
                    planned.generic_plan,
                    &payload_result.payloads,
                    start_ms,
                    end_ms,
                    budget,
                    projected_label_cache,
                )?,
        );
    }
    Ok(results)
}

pub(in crate::storage::segment) struct CrossSegmentNativeRead {
    pub(in crate::storage::segment) segment_ordinal: usize,
    pub(in crate::storage::segment) native_plan: NativeTypedCrossSegmentPlan,
    pub(in crate::storage::segment) payload_plan: ChunkPayloadBatchPlan,
    pub(in crate::storage::segment) file: Arc<File>,
}

fn fetch_cross_segment_native_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentNativeRead>,
) -> io::Result<Vec<(usize, NativeTypedCrossSegmentPlan, ChunkPayloadBatch)>> {
    if group.is_empty() {
        return Ok(Vec::new());
    }

    let scheduler = ChunkReadScheduler::new(chunk_reader);
    let scheduler_items = group
        .iter()
        .map(|planned| ChunkReadSchedulerItem {
            segment_ordinal: planned.segment_ordinal,
            file: Arc::clone(&planned.file),
            plan: planned.payload_plan.clone(),
            logical_requests: planned.native_plan.payload_requests.len() as u64,
        })
        .collect();
    let (payload_results, scheduler_stats) = scheduler.execute(scheduler_items)?;
    let first_segment_ordinal = group[0].segment_ordinal;
    segments[first_segment_ordinal]
        .context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .observe_chunk_read_scheduler(scheduler_stats);
    let mut duration_unassigned = Some(scheduler_stats.read_duration);
    let mut fetched = Vec::with_capacity(group.len());
    for (planned, payload_result) in group.into_iter().zip(payload_results) {
        if payload_result.segment_ordinal != planned.segment_ordinal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk scheduler changed segment result order",
            ));
        }
        let context = segments[planned.segment_ordinal]
            .context
            .as_mut()
            .expect("cross-segment plan requires an open context");
        context.observe_cross_segment_chunk_payload_read(
            duration_unassigned.take().unwrap_or(Duration::ZERO),
            &planned.payload_plan,
        );
        fetched.push((
            planned.segment_ordinal,
            planned.native_plan,
            payload_result.payloads,
        ));
    }
    Ok(fetched)
}

pub(in crate::storage::segment) fn execute_cross_segment_native_histogram_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentNativeRead>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
) -> io::Result<Vec<PromqlHistogramSeries>> {
    let mut results = Vec::new();
    for (segment_ordinal, native_plan, payloads) in
        fetch_cross_segment_native_reads(segments, chunk_reader, group)?
    {
        results.extend(
            segments[segment_ordinal]
                .reader
                .decode_native_histogram_cross_segment_plan(
                    native_plan,
                    &payloads,
                    start_ms,
                    end_ms,
                    budget,
                )?,
        );
    }
    Ok(results)
}

pub(in crate::storage::segment) fn execute_cross_segment_native_exponential_histogram_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentNativeRead>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
    let mut results = Vec::new();
    for (segment_ordinal, native_plan, payloads) in
        fetch_cross_segment_native_reads(segments, chunk_reader, group)?
    {
        results.extend(
            segments[segment_ordinal]
                .reader
                .decode_native_exponential_histogram_cross_segment_plan(
                    native_plan,
                    &payloads,
                    start_ms,
                    end_ms,
                    budget,
                )?,
        );
    }
    Ok(results)
}
