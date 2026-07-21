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
        if value.is_empty() {
            continue;
        }
        let Some(name_sym) = context.symbols.lookup(name)? else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let Some(value_sym) = context.symbols.lookup(value)? else {
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
            postings,
            selection,
        });
    }
    equality_matchers.sort_by_key(|matcher| matcher.postings.byte_len);
    Ok(Ok(equality_matchers))
}

impl<'a> SegmentQuerySessionReader<'a> {
    pub(in crate::storage::segment) fn open(
        reader: &'a SegmentReader,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> Self {
        Self {
            reader,
            facade_context: None,
            context: None,
            stats: SegmentStoreQuerySessionStats::default(),
            profile: SegmentStoreQueryProfile::default(),
            chunk_reader,
            query_instrumentation_mode: QueryInstrumentationMode::Off,
        }
    }

    pub(in crate::storage::segment) fn facade_context(
        &mut self,
    ) -> io::Result<&mut FacadeSegmentQueryContext> {
        if self.facade_context.is_none() {
            let timer = QueryStageTimer::start(self.query_instrumentation_mode);
            let opened = FacadeSegmentQueryContext::open_with_instrumentation(
                &self.reader.metadata_reader,
                Arc::clone(&self.chunk_reader),
                self.query_instrumentation_mode,
            );
            self.profile.stages.metadata_visit_overhead = self
                .profile
                .stages
                .metadata_visit_overhead
                .saturating_add(timer.elapsed());
            self.facade_context = Some(opened?);
        }
        Ok(self.facade_context.as_mut().unwrap())
    }

    pub(in crate::storage::segment) fn context(&mut self) -> io::Result<&mut SegmentQueryContext> {
        if self.context.is_none() {
            self.context = Some(SegmentQueryContext::open_with_chunk_reader(
                self.reader,
                Arc::clone(&self.chunk_reader),
            )?);
        }
        Ok(self.context.as_mut().unwrap())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the session boundary keeps query limits, caches, and the optional range-cache call explicit"
    )]
    pub(in crate::storage::segment) fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        segment_ordinal: usize,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
        projected_label_cache: &mut ProjectedLabelCache,
        cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers = selector.normalized_matchers();
        let reader = self.reader;
        if reader.storage_schema_policy == SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb
            && cache_call.is_some()
        {
            let context = self.context()?;
            return reader.query_normalized_with_context(
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
            );
        }

        let context = self.facade_context()?;
        reader.query_normalized_with_facade_context(
            context,
            segment_ordinal,
            &matchers,
            &selector.projection,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
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
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<GenericCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        let reader = self.reader;
        let context = self.facade_context()?;
        reader.plan_generic_cross_segment_with_facade_context(
            context,
            &matchers,
            &selector.projection,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )
    }

    pub(in crate::storage::segment) fn query_native_histogram_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        let matchers = selector.normalized_matchers();
        let reader = self.reader;
        let context = self.facade_context()?;
        reader.query_native_histogram_normalized_with_facade_context(
            context,
            &matchers,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )
    }

    pub(in crate::storage::segment) fn plan_native_histogram_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        let reader = self.reader;
        let context = self.facade_context()?;
        reader.plan_native_histogram_cross_segment_with_facade_context(
            context,
            &matchers,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )
    }

    pub(in crate::storage::segment) fn plan_native_exponential_histogram_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        let reader = self.reader;
        let context = self.facade_context()?;
        reader.plan_native_exponential_histogram_cross_segment_with_facade_context(
            context,
            &matchers,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )
    }

    pub(in crate::storage::segment) fn query_native_exponential_histogram_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        let matchers = selector.normalized_matchers();
        let reader = self.reader;
        let context = self.facade_context()?;
        reader.query_native_exponential_histogram_normalized_with_facade_context(
            context,
            &matchers,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )
    }

    pub(in crate::storage::segment) fn prewarm_selector(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<()> {
        let _ = (selector, start_ms, end_ms);
        self.facade_context().map(|_| ())
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
        let reader = self.reader;
        let context = self.facade_context()?;
        let mut label_interner = QueryLabelInterner::default();
        // Prefetch discards the plan immediately, so atomizing these transient
        // labels cannot yield reuse and would only add hashing/Arc overhead.
        // Keep the established owned representation for this non-retained
        // planning pass; the selected session policy applies to query results
        // and session caches that survive planning.
        label_interner.set_policy(QueryLabelStoragePolicy::OwnedStrings);
        let plan = reader.plan_generic_cross_segment_with_facade_context(
            context,
            &matchers,
            &selector.projection,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
            &mut SeriesLabelCache::default(),
            &mut label_interner,
        )?;
        prefetch_stats.chunk_index_reads = prefetch_stats
            .chunk_index_reads
            .saturating_add(plan.payload_requests.len() as u64);
        let _ = context.read_chunk_payload_batch(reader, &plan.payload_requests)?;
        Ok(())
    }
}

pub(in crate::storage::segment) struct CrossSegmentGenericRead {
    pub(in crate::storage::segment) segment_ordinal: usize,
    pub(in crate::storage::segment) generic_plan: GenericCrossSegmentPlan,
    pub(in crate::storage::segment) payload_files: Vec<ChunkPayloadFilePlan>,
}

#[expect(
    clippy::too_many_arguments,
    reason = "cross-segment execution keeps scheduling, budget, and label-cache state explicit"
)]
pub(in crate::storage::segment) fn execute_cross_segment_generic_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentGenericRead>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
    label_interner: &mut QueryLabelInterner,
    projected_label_cache: &mut ProjectedLabelCache,
) -> io::Result<Vec<SegmentQueryResult>> {
    if group.is_empty() {
        return Ok(Vec::new());
    }
    let first_segment_ordinal = group[0].segment_ordinal;
    let instrumentation_mode = segments[first_segment_ordinal].query_instrumentation_mode;
    let scheduler = ChunkReadScheduler::new(chunk_reader);
    let scheduler_items = group
        .iter()
        .flat_map(|planned| {
            planned
                .payload_files
                .iter()
                .map(|payload| ChunkReadSchedulerItem {
                    segment_ordinal: planned.segment_ordinal,
                    file_id: payload.file_id,
                    file: ChunkReadSchedulerFile::Governed(payload.file.clone()),
                    plan: payload.plan.clone(),
                    logical_requests: payload.logical_requests,
                })
        })
        .collect();
    let io_started = QueryStageTimer::start(instrumentation_mode);
    let scheduled = scheduler.execute(scheduler_items);
    let io_elapsed = io_started.elapsed();
    let io_profile = &mut segments[first_segment_ordinal]
        .facade_context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .profile
        .stages
        .payload_io;
    *io_profile = io_profile.saturating_add(io_elapsed);
    let (payload_results, scheduler_stats) = scheduled?;
    segments[first_segment_ordinal]
        .facade_context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .observe_chunk_read_scheduler(scheduler_stats);
    let mut duration_unassigned = Some(scheduler_stats.read_duration);
    let mut results = Vec::new();
    let mut payload_results = payload_results.into_iter();
    for planned in group {
        let mut payloads = ChunkPayloadBatch::empty();
        let context = segments[planned.segment_ordinal]
            .facade_context
            .as_mut()
            .expect("cross-segment plan requires an open context");
        for payload_file in &planned.payload_files {
            let payload_result = payload_results.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scheduler omitted a payload-file result",
                )
            })?;
            if payload_result.segment_ordinal != planned.segment_ordinal
                || payload_result.file_id != payload_file.file_id
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scheduler changed payload-file result order",
                ));
            }
            context.observe_cross_segment_chunk_payload_read(
                duration_unassigned.take().unwrap_or(Duration::ZERO),
                &payload_file.plan,
            );
            payloads.append(payload_result.payloads);
        }
        let decode_started =
            QueryStageTimer::start(segments[planned.segment_ordinal].query_instrumentation_mode);
        let decoded = segments[planned.segment_ordinal]
            .reader
            .decode_generic_cross_segment_plan(
                planned.generic_plan,
                &payloads,
                start_ms,
                end_ms,
                budget,
                Some(&mut *label_interner),
                projected_label_cache,
                None,
            );
        let decode_profile = &mut segments[planned.segment_ordinal]
            .facade_context
            .as_mut()
            .expect("cross-segment plan requires an open context")
            .profile
            .stages
            .payload_decode;
        *decode_profile = decode_profile.saturating_add(decode_started.elapsed());
        results.extend(decoded?);
    }
    if payload_results.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk scheduler returned an excess payload-file result",
        ));
    }
    Ok(results)
}

pub(in crate::storage::segment) struct CrossSegmentNativeRead {
    pub(in crate::storage::segment) segment_ordinal: usize,
    pub(in crate::storage::segment) native_plan: NativeTypedCrossSegmentPlan,
    pub(in crate::storage::segment) payload_files: Vec<ChunkPayloadFilePlan>,
}

fn fetch_cross_segment_native_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentNativeRead>,
) -> io::Result<Vec<(usize, NativeTypedCrossSegmentPlan, ChunkPayloadBatch)>> {
    if group.is_empty() {
        return Ok(Vec::new());
    }

    let first_segment_ordinal = group[0].segment_ordinal;
    let instrumentation_mode = segments[first_segment_ordinal].query_instrumentation_mode;
    let scheduler = ChunkReadScheduler::new(chunk_reader);
    let scheduler_items = group
        .iter()
        .flat_map(|planned| {
            planned
                .payload_files
                .iter()
                .map(|payload| ChunkReadSchedulerItem {
                    segment_ordinal: planned.segment_ordinal,
                    file_id: payload.file_id,
                    file: ChunkReadSchedulerFile::Governed(payload.file.clone()),
                    plan: payload.plan.clone(),
                    logical_requests: payload.logical_requests,
                })
        })
        .collect();
    let io_started = QueryStageTimer::start(instrumentation_mode);
    let scheduled = scheduler.execute(scheduler_items);
    let io_elapsed = io_started.elapsed();
    let io_profile = &mut segments[first_segment_ordinal]
        .facade_context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .profile
        .stages
        .payload_io;
    *io_profile = io_profile.saturating_add(io_elapsed);
    let (payload_results, scheduler_stats) = scheduled?;
    segments[first_segment_ordinal]
        .facade_context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .observe_chunk_read_scheduler(scheduler_stats);
    let mut duration_unassigned = Some(scheduler_stats.read_duration);
    let mut fetched = Vec::with_capacity(group.len());
    let mut payload_results = payload_results.into_iter();
    for planned in group {
        let mut payloads = ChunkPayloadBatch::empty();
        let context = segments[planned.segment_ordinal]
            .facade_context
            .as_mut()
            .expect("cross-segment plan requires an open context");
        for payload_file in &planned.payload_files {
            let payload_result = payload_results.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scheduler omitted a payload-file result",
                )
            })?;
            if payload_result.segment_ordinal != planned.segment_ordinal
                || payload_result.file_id != payload_file.file_id
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scheduler changed payload-file result order",
                ));
            }
            context.observe_cross_segment_chunk_payload_read(
                duration_unassigned.take().unwrap_or(Duration::ZERO),
                &payload_file.plan,
            );
            payloads.append(payload_result.payloads);
        }
        fetched.push((planned.segment_ordinal, planned.native_plan, payloads));
    }
    if payload_results.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk scheduler returned an excess payload-file result",
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
        let decode_started =
            QueryStageTimer::start(segments[segment_ordinal].query_instrumentation_mode);
        let decoded = segments[segment_ordinal]
            .reader
            .decode_native_histogram_cross_segment_plan(
                native_plan,
                &payloads,
                start_ms,
                end_ms,
                budget,
            );
        let decode_profile = &mut segments[segment_ordinal]
            .facade_context
            .as_mut()
            .expect("cross-segment plan requires an open context")
            .profile
            .stages
            .payload_decode;
        *decode_profile = decode_profile.saturating_add(decode_started.elapsed());
        results.extend(decoded?);
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
        let decode_started =
            QueryStageTimer::start(segments[segment_ordinal].query_instrumentation_mode);
        let decoded = segments[segment_ordinal]
            .reader
            .decode_native_exponential_histogram_cross_segment_plan(
                native_plan,
                &payloads,
                start_ms,
                end_ms,
                budget,
            );
        let decode_profile = &mut segments[segment_ordinal]
            .facade_context
            .as_mut()
            .expect("cross-segment plan requires an open context")
            .profile
            .stages
            .payload_decode;
        *decode_profile = decode_profile.saturating_add(decode_started.elapsed());
        results.extend(decoded?);
    }
    Ok(results)
}
