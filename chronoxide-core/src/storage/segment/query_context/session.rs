use super::*;

impl<'a> SegmentStoreQuerySession<'a> {
    pub(in crate::storage::segment) fn should_use_cross_segment_flow(
        &self,
        start_ms: u64,
        end_ms: u64,
    ) -> bool {
        let Some(reader) = self.segments.first().map(|segment| &segment.chunk_reader) else {
            return false;
        };
        if reader.configured_mode() != crate::storage::io::ChunkReadMode::Auto {
            return true;
        }
        self.segments
            .iter()
            .filter(|segment| {
                segment.reader.meta.end_ms >= start_ms && segment.reader.meta.start_ms <= end_ms
            })
            .take(CHUNK_READ_AUTO_MIN_SPANS as usize)
            .count()
            >= CHUNK_READ_AUTO_MIN_SPANS as usize
    }

    pub(in crate::storage::segment) fn open(store: &'a SegmentStoreReader) -> io::Result<Self> {
        let chunk_reader = Arc::new(crate::storage::io::ChunkReader::new(
            crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 1,
                payload_coalesce_max_gap_bytes:
                    crate::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
            },
        )?);
        let mut segments = Vec::with_capacity(store.segments.len());
        for segment in store.segments_in_query_order() {
            segments.push(SegmentQuerySessionReader::open(
                segment,
                Arc::clone(&chunk_reader),
            ));
        }
        let mut label_interner = QueryLabelInterner::default();
        // Compact IDs are the production representation for native schema-7/8
        // stores. The schema-6 adapter cannot provide generation-bound encoded
        // labels, so it intentionally retains the owned-string comparator.
        // An empty store also remains owned because its open policy is not
        // retained on SegmentStoreReader and no labels can be constructed.
        if !segments.is_empty()
            && segments.iter().all(|segment| {
                segment.reader.storage_schema_policy
                    != SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb
            })
        {
            label_interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        }
        Ok(Self {
            query_projection_config: store.query_projection_config.clone(),
            segments,
            label_cache: SeriesLabelCache::default(),
            projected_label_cache: ProjectedLabelCache::default(),
            range_scalar_cache_budget_bytes: DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES,
            range_scalar_cache_governor:
                super::range_scalar_cache::process_range_scalar_cache_governor(),
            last_range_scalar_cache_summary: None,
            range_execution_mode: RangeExecutionMode::Repeated,
            last_range_execution_summary: None,
            experimental_cross_segment_chunk_reads: false,
            label_materialization_policy: QueryLabelMaterializationPolicy::DemandDriven,
            query_label_storage_policy_frozen: false,
            query_instrumentation_mode: QueryInstrumentationMode::Off,
            query_instrumentation_mode_frozen: false,
            label_interner,
            query_stages: QueryStageProfile::default(),
        })
    }

    pub fn set_chunk_read_config(
        &mut self,
        config: crate::storage::io::ChunkReadConfig,
    ) -> io::Result<()> {
        self.set_chunk_reader(Arc::new(crate::storage::io::ChunkReader::new(config)?))
    }

    pub fn set_chunk_reader(
        &mut self,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> io::Result<()> {
        if self
            .segments
            .iter()
            .any(|segment| segment.context.is_some() || segment.facade_context.is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk read configuration must be set before opening query contexts",
            ));
        }
        for segment in &mut self.segments {
            segment.chunk_reader = Arc::clone(&chunk_reader);
        }
        Ok(())
    }

    pub fn set_experimental_cross_segment_chunk_reads(&mut self, enabled: bool) {
        self.experimental_cross_segment_chunk_reads = enabled;
    }

    /// Selects the source-label ownership policy used by PromQL planning.
    /// `Full` exists for one-binary semantic/performance A/B; normal query
    /// sessions use `DemandDriven`.
    pub fn set_label_materialization_policy(&mut self, policy: QueryLabelMaterializationPolicy) {
        self.label_materialization_policy = policy;
    }

    /// Selects the source-label storage representation for this fresh query
    /// session. Schema-7/8 sessions default to `CompactIds`; `OwnedStrings`
    /// and `SharedAtoms` remain explicit same-binary comparators, never error
    /// fallbacks.
    pub fn set_query_label_storage_policy(
        &mut self,
        policy: QueryLabelStoragePolicy,
    ) -> io::Result<()> {
        if self.query_label_storage_policy_frozen
            || self.label_interner.stats().label_sets != 0
            || !self.label_cache.is_empty()
            || !self.projected_label_cache.entries.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query label storage policy must be set before any query or prefetch attempt",
            ));
        }
        if policy == QueryLabelStoragePolicy::CompactIds
            && self.segments.iter().any(|segment| {
                segment.reader.storage_schema_policy
                    == SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "compact-ids query labels require storage schema 7 or 8",
            ));
        }
        self.label_interner.set_policy(policy);
        Ok(())
    }

    /// Sets the aggregate modeled retained-allocation admission budget for the
    /// compact query-label arena. The portable model excludes allocator
    /// metadata/size-class slack; the value freezes with the storage policy.
    pub fn set_query_label_arena_max_bytes(&mut self, max_bytes: u64) -> io::Result<()> {
        if self.query_label_storage_policy_frozen
            || self.label_interner.stats().label_sets != 0
            || !self.label_cache.is_empty()
            || !self.projected_label_cache.entries.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query label arena budget must be set before any query or prefetch attempt",
            ));
        }
        self.label_interner.set_compact_arena_max_bytes(max_bytes)
    }

    pub(super) fn freeze_query_label_storage_policy(&mut self) {
        self.query_label_storage_policy_frozen = true;
        self.query_instrumentation_mode_frozen = true;
    }

    /// Enables or disables observer-heavy query-stage timers for this fresh
    /// session. The mode is frozen by the first query, prewarm, or prefetch so
    /// one report cannot silently mix profiled and unprofiled execution.
    pub fn set_query_instrumentation_mode(
        &mut self,
        mode: QueryInstrumentationMode,
    ) -> io::Result<()> {
        if self.query_instrumentation_mode_frozen
            || self
                .segments
                .iter()
                .any(|segment| segment.context.is_some() || segment.facade_context.is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query instrumentation mode must be set before any query or prefetch attempt",
            ));
        }
        self.query_instrumentation_mode = mode;
        for segment in &mut self.segments {
            segment.query_instrumentation_mode = mode;
        }
        Ok(())
    }

    pub fn query_instrumentation_mode(&self) -> QueryInstrumentationMode {
        self.query_instrumentation_mode
    }

    pub fn query_label_storage_policy(&self) -> QueryLabelStoragePolicy {
        self.label_interner.policy()
    }

    pub fn query_label_storage_stats(&self) -> QueryLabelStorageStats {
        self.label_interner.stats()
    }

    pub fn query_selector(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_limits(selector, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_selector_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        self.freeze_query_label_storage_policy();
        let mut budget = QueryBudget::new(limits);
        let results = self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
        Ok(QueryExecution {
            results,
            stats: budget.stats(),
        })
    }

    pub(in crate::storage::segment) fn query_selectors_with_limits(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        self.query_selectors_with_limits_with_cache(selectors, start_ms, end_ms, limits, None)
    }

    pub(in crate::storage::segment) fn query_selectors_with_limits_with_cache(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> io::Result<QueryExecution> {
        self.freeze_query_label_storage_policy();
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        let mut seen_branches = BTreeMap::new();
        for selector in selectors {
            let selector_results = self.query_selector_with_budget_with_cache(
                selector,
                start_ms,
                end_ms,
                &mut budget,
                cache_call.as_deref_mut(),
            )?;
            observe_promql_selector_branch_conflicts(
                &mut seen_branches,
                selector,
                &selector_results,
            )?;
            results.extend(selector_results);
        }
        let results = self.merge_query_results_profiled(results);
        Ok(QueryExecution {
            results,
            stats: budget.stats(),
        })
    }

    pub(in crate::storage::segment) fn query_native_histogram_selector_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlHistogramSeries>, QueryStats), PromqlQueryError> {
        self.freeze_query_label_storage_policy();
        if self.experimental_cross_segment_chunk_reads
            && self.should_use_cross_segment_flow(start_ms, end_ms)
        {
            return self.query_native_histogram_selector_cross_segment_with_limits(
                selector, start_ms, end_ms, limits,
            );
        }
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        let label_cache = &mut self.label_cache;
        let label_interner = &mut self.label_interner;
        for segment in &mut self.segments {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_histogram_with_budget(
                        selector,
                        start_ms,
                        end_ms,
                        &mut budget,
                        label_cache,
                        label_interner,
                    )
                    .map_err(promql_error_from_query_io)?,
            );
        }

        let merge_started =
            QueryStageTimer::start_if(self.query_instrumentation_mode, !results.is_empty());
        let results = merge_histogram_query_results(results);
        self.query_stages.source_merge = self
            .query_stages
            .source_merge
            .saturating_add(merge_started.elapsed());
        Ok((results, budget.stats()))
    }

    fn query_native_histogram_selector_cross_segment_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlHistogramSeries>, QueryStats), PromqlQueryError> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }
        let Some(chunk_reader) = self
            .segments
            .first()
            .map(|segment| Arc::clone(&segment.chunk_reader))
        else {
            return Ok((results, budget.stats()));
        };

        let mut group = Vec::new();
        let mut group_spans = 0u64;
        let mut group_bytes = 0u64;
        let mut deferred_error = None;

        for segment_ordinal in 0..self.segments.len() {
            budget.observe_segment_considered();
            let segment = &self.segments[segment_ordinal];
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            let planned = {
                let segment = &mut self.segments[segment_ordinal];
                segment.plan_native_histogram_cross_segment_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    &mut budget,
                    &mut self.label_cache,
                    &mut self.label_interner,
                )
            };
            let native_plan = match planned {
                Ok(plan) => plan,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            if native_plan.payload_requests.is_empty() {
                continue;
            }

            let physical = {
                let segment = &mut self.segments[segment_ordinal];
                let reader = segment.reader;
                let context = segment
                    .facade_context
                    .as_mut()
                    .expect("native histogram plan requires an open context");
                context
                    .plan_cross_segment_chunk_payload_batch(reader, &native_plan.payload_requests)
            };
            let payload_files = match physical {
                Ok(physical) => physical,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            let item_spans = payload_files
                .iter()
                .map(|payload| payload.plan.physical_read_count())
                .sum();
            let item_bytes = payload_files
                .iter()
                .map(|payload| payload.plan.physical_bytes_read())
                .sum();

            if chunk_read_group_would_exceed_bounds(
                group.len(),
                group_spans,
                group_bytes,
                item_spans,
                item_bytes,
            ) {
                results.extend(
                    execute_cross_segment_native_histogram_reads(
                        &mut self.segments,
                        Arc::clone(&chunk_reader),
                        std::mem::take(&mut group),
                        start_ms,
                        end_ms,
                        &mut budget,
                    )
                    .map_err(promql_error_from_query_io)?,
                );
                group_spans = 0;
                group_bytes = 0;
            }

            group_spans = group_spans.saturating_add(item_spans);
            group_bytes = group_bytes.saturating_add(item_bytes);
            group.push(CrossSegmentNativeRead {
                segment_ordinal,
                native_plan,
                payload_files,
            });
        }

        results.extend(
            execute_cross_segment_native_histogram_reads(
                &mut self.segments,
                chunk_reader,
                group,
                start_ms,
                end_ms,
                &mut budget,
            )
            .map_err(promql_error_from_query_io)?,
        );
        if let Some(error) = deferred_error {
            return Err(promql_error_from_query_io(error));
        }
        let merge_started =
            QueryStageTimer::start_if(self.query_instrumentation_mode, !results.is_empty());
        let results = merge_histogram_query_results(results);
        self.query_stages.source_merge = self
            .query_stages
            .source_merge
            .saturating_add(merge_started.elapsed());
        Ok((results, budget.stats()))
    }

    pub(in crate::storage::segment) fn query_native_exponential_histogram_selector_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlExponentialHistogramSeries>, QueryStats), PromqlQueryError> {
        self.freeze_query_label_storage_policy();
        if self.experimental_cross_segment_chunk_reads
            && self.should_use_cross_segment_flow(start_ms, end_ms)
        {
            return self.query_native_exponential_histogram_selector_cross_segment_with_limits(
                selector, start_ms, end_ms, limits,
            );
        }
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        let label_cache = &mut self.label_cache;
        let label_interner = &mut self.label_interner;
        for segment in &mut self.segments {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_exponential_histogram_with_budget(
                        selector,
                        start_ms,
                        end_ms,
                        &mut budget,
                        label_cache,
                        label_interner,
                    )
                    .map_err(promql_error_from_query_io)?,
            );
        }

        let merge_started =
            QueryStageTimer::start_if(self.query_instrumentation_mode, !results.is_empty());
        let results = merge_exponential_histogram_query_results(results);
        self.query_stages.source_merge = self
            .query_stages
            .source_merge
            .saturating_add(merge_started.elapsed());
        Ok((results, budget.stats()))
    }

    fn query_native_exponential_histogram_selector_cross_segment_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlExponentialHistogramSeries>, QueryStats), PromqlQueryError> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }
        let Some(chunk_reader) = self
            .segments
            .first()
            .map(|segment| Arc::clone(&segment.chunk_reader))
        else {
            return Ok((results, budget.stats()));
        };

        let mut group = Vec::new();
        let mut group_spans = 0u64;
        let mut group_bytes = 0u64;
        let mut deferred_error = None;

        for segment_ordinal in 0..self.segments.len() {
            budget.observe_segment_considered();
            let segment = &self.segments[segment_ordinal];
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            let planned = {
                let segment = &mut self.segments[segment_ordinal];
                segment.plan_native_exponential_histogram_cross_segment_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    &mut budget,
                    &mut self.label_cache,
                    &mut self.label_interner,
                )
            };
            let native_plan = match planned {
                Ok(plan) => plan,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            if native_plan.payload_requests.is_empty() {
                continue;
            }

            let physical = {
                let segment = &mut self.segments[segment_ordinal];
                let reader = segment.reader;
                let context = segment
                    .facade_context
                    .as_mut()
                    .expect("native exponential histogram plan requires an open context");
                context
                    .plan_cross_segment_chunk_payload_batch(reader, &native_plan.payload_requests)
            };
            let payload_files = match physical {
                Ok(physical) => physical,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            let item_spans = payload_files
                .iter()
                .map(|payload| payload.plan.physical_read_count())
                .sum();
            let item_bytes = payload_files
                .iter()
                .map(|payload| payload.plan.physical_bytes_read())
                .sum();

            if chunk_read_group_would_exceed_bounds(
                group.len(),
                group_spans,
                group_bytes,
                item_spans,
                item_bytes,
            ) {
                results.extend(
                    execute_cross_segment_native_exponential_histogram_reads(
                        &mut self.segments,
                        Arc::clone(&chunk_reader),
                        std::mem::take(&mut group),
                        start_ms,
                        end_ms,
                        &mut budget,
                    )
                    .map_err(promql_error_from_query_io)?,
                );
                group_spans = 0;
                group_bytes = 0;
            }

            group_spans = group_spans.saturating_add(item_spans);
            group_bytes = group_bytes.saturating_add(item_bytes);
            group.push(CrossSegmentNativeRead {
                segment_ordinal,
                native_plan,
                payload_files,
            });
        }

        results.extend(
            execute_cross_segment_native_exponential_histogram_reads(
                &mut self.segments,
                chunk_reader,
                group,
                start_ms,
                end_ms,
                &mut budget,
            )
            .map_err(promql_error_from_query_io)?,
        );
        if let Some(error) = deferred_error {
            return Err(promql_error_from_query_io(error));
        }
        let merge_started =
            QueryStageTimer::start_if(self.query_instrumentation_mode, !results.is_empty());
        let results = merge_exponential_histogram_query_results(results);
        self.query_stages.source_merge = self
            .query_stages
            .source_merge
            .saturating_add(merge_started.elapsed());
        Ok((results, budget.stats()))
    }

    pub fn stats(&self) -> SegmentStoreQuerySessionStats {
        let mut stats = SegmentStoreQuerySessionStats::default();
        for segment in &self.segments {
            stats.add(segment.stats);
            if let Some(context) = &segment.facade_context {
                stats.add(context.stats);
            }
            if let Some(context) = &segment.context {
                stats.add(context.stats);
            }
        }
        stats
    }

    pub fn profile(&self) -> SegmentStoreQueryProfile {
        let mut profile = SegmentStoreQueryProfile {
            stages: self.query_stages,
            ..SegmentStoreQueryProfile::default()
        };
        for segment in &self.segments {
            profile.add(segment.profile);
            if let Some(context) = &segment.facade_context {
                profile.add(context.profile);
            }
            if let Some(context) = &segment.context {
                let mut context_profile = context.profile;
                context_profile.index_read_stats = context_profile
                    .index_read_stats
                    .saturating_add(context.index_reader.read_stats());
                context_profile.symbol_read_stats = context_profile
                    .symbol_read_stats
                    .saturating_add(context.symbols.read_stats());
                profile.add(context_profile);
            }
        }
        profile.symbol_resources = SegmentStoreSymbolResources::snapshot_segment_readers(
            self.segments.iter().map(|segment| segment.reader),
        );
        profile
    }

    pub(super) fn merge_query_results_profiled(
        &mut self,
        results: Vec<SegmentQueryResult>,
    ) -> Vec<SegmentQueryResult> {
        let merge_started =
            QueryStageTimer::start_if(self.query_instrumentation_mode, !results.is_empty());
        let results = merge_query_results(results);
        self.query_stages.source_merge = self
            .query_stages
            .source_merge
            .saturating_add(merge_started.elapsed());
        results
    }

    pub fn set_range_scalar_cache_budget_bytes(
        &mut self,
        bytes: u64,
    ) -> Result<(), RangeScalarCacheConfigError> {
        validate_range_scalar_cache_budget_bytes(bytes)?;
        self.range_scalar_cache_budget_bytes = bytes;
        Ok(())
    }

    pub fn last_range_scalar_cache_summary(&self) -> Option<&RangeScalarCacheSummary> {
        self.last_range_scalar_cache_summary.as_ref()
    }

    /// Selects the sealed-store range executor for this fresh session.
    ///
    /// `OnePassAssumeScalar` is a diagnostic comparator, not an automatic
    /// production optimizer. It must be selected before any query, prewarm,
    /// or prefetch attempt freezes session state.
    pub fn set_range_execution_mode(&mut self, mode: RangeExecutionMode) -> io::Result<()> {
        if self.query_label_storage_policy_frozen
            || self
                .segments
                .iter()
                .any(|segment| segment.context.is_some() || segment.facade_context.is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range execution mode must be set before any query or prefetch attempt",
            ));
        }
        self.range_execution_mode = mode;
        Ok(())
    }

    pub fn range_execution_mode(&self) -> RangeExecutionMode {
        self.range_execution_mode
    }

    pub fn last_range_execution_summary(&self) -> Option<&RangeExecutionSummary> {
        self.last_range_execution_summary.as_ref()
    }

    pub fn query_promql(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        self.query_promql_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.freeze_query_label_storage_policy();
        let query = parse_query(query)?;
        let mut execution = self.execute_promql_query(&query, start_ms, end_ms, limits)?;
        let result_started = QueryStageTimer::start_if(
            self.query_instrumentation_mode,
            !execution.results.is_empty(),
        );
        self.label_interner
            .intern_result_labels(&mut execution.results)
            .map_err(promql_error_from_query_io)?;
        let completeness = ensure_query_result_labels_complete(&execution.results);
        self.query_stages.result_construction = self
            .query_stages
            .result_construction
            .saturating_add(result_started.elapsed());
        completeness?;
        Ok(execution)
    }

    pub fn query_promql_at(
        &mut self,
        query: &str,
        evaluation_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        self.query_promql_at_with_limits(query, evaluation_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_at_with_limits(
        &mut self,
        query: &str,
        evaluation_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.freeze_query_label_storage_policy();
        let query = parse_query(query)?;
        let mut execution = self.execute_promql_instant_query(&query, evaluation_ms, limits)?;
        let result_started = QueryStageTimer::start_if(
            self.query_instrumentation_mode,
            !execution.results.is_empty(),
        );
        execution.results = retimestamp_instant_results(execution.results, evaluation_ms);
        self.label_interner
            .intern_result_labels(&mut execution.results)
            .map_err(promql_error_from_query_io)?;
        let completeness = ensure_query_result_labels_complete(&execution.results);
        self.query_stages.result_construction = self
            .query_stages
            .result_construction
            .saturating_add(result_started.elapsed());
        completeness?;
        Ok(execution)
    }

    pub fn query_promql_range(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        self.query_promql_range_with_limits(
            query,
            start_ms,
            end_ms,
            step_ms,
            QueryLimits::unlimited(),
        )
        .map(|execution| execution.results)
    }

    pub fn query_promql_range_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.freeze_query_label_storage_policy();
        self.last_range_scalar_cache_summary = None;
        self.last_range_execution_summary = None;
        let mut range_execution_summary = RangeExecutionSummary::new(self.range_execution_mode);
        let mut cache_call = super::range_scalar_cache::RangeScalarCacheCall::new(
            self.range_scalar_cache_budget_bytes,
            Arc::clone(&self.range_scalar_cache_governor),
        );
        let result = (|| {
            let query = parse_query(query)?;
            validate_promql_range_bounds(start_ms, end_ms, step_ms)?;
            self.execute_validated_promql_range_query(
                &query,
                start_ms,
                end_ms,
                step_ms,
                limits,
                &mut cache_call,
                &mut range_execution_summary,
            )
        })();
        self.last_range_scalar_cache_summary = Some(cache_call.finish());
        if result.is_err()
            && range_execution_summary.requested_mode == RangeExecutionMode::OnePassAssumeScalar
            && range_execution_summary.effective_mode == RangeExecutionMode::Repeated
            && range_execution_summary.fallback_reason.is_none()
        {
            range_execution_summary.fallback_reason =
                Some(RangeExecutionFallbackReason::InvalidQuery);
        }
        self.last_range_execution_summary = Some(range_execution_summary);
        let mut execution = result?;
        let result_started = QueryStageTimer::start_if(
            self.query_instrumentation_mode,
            !execution.results.is_empty(),
        );
        self.label_interner
            .intern_result_labels(&mut execution.results)
            .map_err(promql_error_from_query_io)?;
        let completeness = ensure_query_result_labels_complete(&execution.results);
        self.query_stages.result_construction = self
            .query_stages
            .result_construction
            .saturating_add(result_started.elapsed());
        completeness?;
        Ok(execution)
    }

    pub fn prewarm_promql(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<SegmentStoreQuerySessionStats, PromqlQueryError> {
        self.prewarm_promql_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
    }

    pub fn prewarm_promql_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        _limits: QueryLimits,
    ) -> Result<SegmentStoreQuerySessionStats, PromqlQueryError> {
        self.freeze_query_label_storage_policy();
        let before = self.stats();
        let query = parse_query(query)?;
        self.prewarm_promql_query(&query, start_ms, end_ms)?;
        Ok(self.stats().delta_since(before))
    }

    pub fn prefetch_promql_data(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        self.prefetch_promql_data_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
    }

    pub fn prefetch_promql_data_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        self.freeze_query_label_storage_policy();
        let query = parse_query(query)?;
        self.prefetch_promql_data_query(&query, start_ms, end_ms, limits)
    }

    pub(in crate::storage::segment) fn prewarm_promql_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<(), PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.prewarm_selectors(&selectors, start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prewarm_promql_instant_query(&aggregation.input, end_ms)
            }
            PromqlQuery::Absent(absent) => self.prewarm_promql_instant_query(&absent.input, end_ms),
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Offset(offset) => self.prewarm_promql_instant_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => Ok(()),
            PromqlQuery::BinaryExpression(expression) => {
                self.prewarm_promql_binary_expression(expression, end_ms)
            }
        }
    }

    fn prewarm_promql_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
    ) -> Result<(), PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.prewarm_selectors(&selectors, start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prewarm_promql_instant_query(&aggregation.input, end_ms)
            }
            PromqlQuery::Absent(absent) => self.prewarm_promql_instant_query(&absent.input, end_ms),
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Offset(offset) => self.prewarm_promql_instant_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => Ok(()),
            PromqlQuery::BinaryExpression(expression) => {
                self.prewarm_promql_binary_expression(expression, end_ms)
            }
        }
    }

    fn prewarm_promql_binary_expression(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
    ) -> Result<(), PromqlQueryError> {
        for query in binary_expression_vector_sides(expression) {
            self.prewarm_promql_instant_query(query, end_ms)?;
        }
        Ok(())
    }

    pub(in crate::storage::segment) fn prefetch_promql_data_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.prefetch_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prefetch_promql_instant_data_query(&aggregation.input, end_ms, limits)
            }
            PromqlQuery::Absent(absent) => {
                self.prefetch_promql_instant_data_query(&absent.input, end_ms, limits)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Offset(offset) => self.prefetch_promql_instant_data_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
                limits,
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => {
                Ok(QueryDataPrefetchStats::default())
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.prefetch_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    fn prefetch_promql_instant_data_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.prefetch_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prefetch_promql_instant_data_query(&aggregation.input, end_ms, limits)
            }
            PromqlQuery::Absent(absent) => {
                self.prefetch_promql_instant_data_query(&absent.input, end_ms, limits)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Offset(offset) => self.prefetch_promql_instant_data_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
                limits,
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => {
                Ok(QueryDataPrefetchStats::default())
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.prefetch_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    fn prefetch_promql_binary_expression(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        let mut stats = QueryDataPrefetchStats::default();
        for query in binary_expression_vector_sides(expression) {
            stats.merge_from(self.prefetch_promql_instant_data_query(query, end_ms, limits)?);
        }
        stats.query_stats.check_limits(limits)?;
        Ok(stats)
    }
}
