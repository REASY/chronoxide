use super::*;
use crate::storage::segment::metadata_facade::{
    GovernedSeriesRefSet, SegmentExactPostingsSelection, SegmentMetadataFacadeError,
    SegmentMetadataRoot, SegmentMetadataSession, SegmentMetadataVisitControl,
    SegmentMetadataVisitError,
};
use crate::storage::segment::query_context::{FacadeSegmentQueryContext, QueryStageTimer};

impl SegmentReader {
    #[expect(
        clippy::too_many_arguments,
        reason = "the schema-neutral facade keeps query bounds, budgets, and cache state explicit"
    )]
    pub(in crate::storage::segment) fn query_normalized_with_facade_context(
        &self,
        context: &mut FacadeSegmentQueryContext,
        segment_ordinal: usize,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        label_demand: &QueryLabelDemand,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
        projected_label_cache: &mut ProjectedLabelCache,
        cache_call: Option<&mut RangeScalarCacheCall>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let instrumentation_mode = context.instrumentation_mode();
        let plan = self.plan_generic_cross_segment_with_facade_context(
            context,
            matchers,
            projection,
            label_demand,
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )?;
        let payload_stage_mode = if plan.payload_requests.is_empty() {
            QueryInstrumentationMode::Off
        } else {
            instrumentation_mode
        };
        let Some(cache_call) = cache_call else {
            let io_started = QueryStageTimer::start(payload_stage_mode);
            let payloads_result = context.read_chunk_payload_batch(self, &plan.payload_requests);
            context.profile.stages.payload_io = context
                .profile
                .stages
                .payload_io
                .saturating_add(io_started.elapsed());
            let payloads = payloads_result?;
            let decode_started = QueryStageTimer::start(payload_stage_mode);
            let decoded = self.decode_generic_cross_segment_plan(
                plan,
                &payloads,
                start_ms,
                end_ms,
                budget,
                Some(label_interner),
                projected_label_cache,
                None,
            );
            context.profile.stages.payload_decode = context
                .profile
                .stages
                .payload_decode
                .saturating_add(decode_started.elapsed());
            return decoded;
        };

        // Logical accounting and locality observe every planned request before
        // cache hits are removed. Only the filtered requests below represent
        // physical I/O, preserving cache-off QueryStats and limit behavior.
        context.observe_chunk_payload_requests(&plan.payload_requests);
        let mut physical_requests = Vec::new();
        for planned in &plan.series {
            for locator in planned.chunks.iter() {
                let entry = locator.entry();
                if entry.max_time_ms < start_ms || entry.min_time_ms > end_ms {
                    continue;
                }
                let scalar_projection = typed_scalar_projection(projection, entry.kind)
                    .map(|(projection, _metric_suffix)| projection);
                let read_len = if scalar_projection.is_some() {
                    entry.scalar_projection_read_len()
                } else if chunk_kind_matches_projection(projection, entry.kind) {
                    entry.length
                } else {
                    continue;
                };
                let logical_bytes = u64::from(read_len);
                let Some(key) = scalar_projection.and_then(|projection| {
                    range_scalar_cache_key(segment_ordinal, entry, projection)
                }) else {
                    cache_call.classify_unsupported(logical_bytes);
                    physical_requests.push(ChunkPayloadRead {
                        file_id: entry.file_id,
                        offset: entry.offset,
                        len: logical_bytes,
                    });
                    continue;
                };
                if cache_call.classify_eligible(&key, logical_bytes) == RangeScalarCacheLookup::Miss
                {
                    physical_requests.push(ChunkPayloadRead {
                        file_id: entry.file_id,
                        offset: entry.offset,
                        len: logical_bytes,
                    });
                }
            }
        }
        let physical_io_mode = if physical_requests.is_empty() {
            QueryInstrumentationMode::Off
        } else {
            instrumentation_mode
        };
        let io_started = QueryStageTimer::start(physical_io_mode);
        let payloads_result = context.read_chunk_payload_batch_physical(self, &physical_requests);
        context.profile.stages.payload_io = context
            .profile
            .stages
            .payload_io
            .saturating_add(io_started.elapsed());
        let payloads = payloads_result?;
        let decode_started = QueryStageTimer::start(payload_stage_mode);
        let decoded = self.decode_generic_cross_segment_plan(
            plan,
            &payloads,
            start_ms,
            end_ms,
            budget,
            Some(label_interner),
            projected_label_cache,
            Some(GenericRangeScalarCache {
                segment_ordinal,
                call: cache_call,
            }),
        );
        context.profile.stages.payload_decode = context
            .profile
            .stages
            .payload_decode
            .saturating_add(decode_started.elapsed());
        decoded
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the schema-neutral native query boundary keeps budget and label state explicit"
    )]
    pub(in crate::storage::segment) fn query_native_histogram_normalized_with_facade_context(
        &self,
        context: &mut FacadeSegmentQueryContext,
        matchers: &[NormalizedMatcher],
        label_demand: &QueryLabelDemand,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        let instrumentation_mode = context.instrumentation_mode();
        let plan = self.plan_native_histogram_cross_segment_with_facade_context(
            context,
            matchers,
            label_demand,
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )?;
        let payload_stage_mode = if plan.payload_requests.is_empty() {
            QueryInstrumentationMode::Off
        } else {
            instrumentation_mode
        };
        let io_started = QueryStageTimer::start(payload_stage_mode);
        let payloads_result = context.read_chunk_payload_batch(self, &plan.payload_requests);
        context.profile.stages.payload_io = context
            .profile
            .stages
            .payload_io
            .saturating_add(io_started.elapsed());
        let payloads = payloads_result?;
        let decode_started = QueryStageTimer::start(payload_stage_mode);
        let decoded = self
            .decode_native_histogram_cross_segment_plan(plan, &payloads, start_ms, end_ms, budget);
        context.profile.stages.payload_decode = context
            .profile
            .stages
            .payload_decode
            .saturating_add(decode_started.elapsed());
        decoded
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the schema-neutral native query boundary keeps budget and label state explicit"
    )]
    pub(in crate::storage::segment) fn query_native_exponential_histogram_normalized_with_facade_context(
        &self,
        context: &mut FacadeSegmentQueryContext,
        matchers: &[NormalizedMatcher],
        label_demand: &QueryLabelDemand,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        let instrumentation_mode = context.instrumentation_mode();
        let plan = self.plan_native_exponential_histogram_cross_segment_with_facade_context(
            context,
            matchers,
            label_demand,
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )?;
        let payload_stage_mode = if plan.payload_requests.is_empty() {
            QueryInstrumentationMode::Off
        } else {
            instrumentation_mode
        };
        let io_started = QueryStageTimer::start(payload_stage_mode);
        let payloads_result = context.read_chunk_payload_batch(self, &plan.payload_requests);
        context.profile.stages.payload_io = context
            .profile
            .stages
            .payload_io
            .saturating_add(io_started.elapsed());
        let payloads = payloads_result?;
        let decode_started = QueryStageTimer::start(payload_stage_mode);
        let decoded = self.decode_native_exponential_histogram_cross_segment_plan(
            plan, &payloads, start_ms, end_ms, budget,
        );
        context.profile.stages.payload_decode = context
            .profile
            .stages
            .payload_decode
            .saturating_add(decode_started.elapsed());
        decoded
    }

    /// Plans one generic query through the schema-neutral metadata facade.
    ///
    /// Both schema 6 and schema 7 deliberately use the same conservative
    /// candidate policy here. In particular, no postings time summary is used
    /// for pruning: schema-6 summaries are advisory, so applying schema-7's
    /// authenticated summaries would make the A/B execute different work.
    #[expect(
        clippy::too_many_arguments,
        reason = "cross-segment facade planning keeps projection, bounds, budget, and label state explicit"
    )]
    pub(in crate::storage::segment) fn plan_generic_cross_segment_with_facade_context(
        &self,
        context: &mut FacadeSegmentQueryContext,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        label_demand: &QueryLabelDemand,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<GenericCrossSegmentPlan> {
        let instrumentation_mode = context.instrumentation_mode();
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => {
                let started = QueryStageTimer::start(instrumentation_mode);
                let compiled = compile_label_matchers(matchers);
                context.profile.stages.candidate_selection = context
                    .profile
                    .stages
                    .candidate_selection
                    .saturating_add(started.elapsed());
                Some(compiled?)
            }
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::NativeHistogram
            | SegmentProjection::NativeExponentialHistogram
            | SegmentProjection::SummaryQuantile { .. } => None,
        };
        if end_ms < start_ms {
            return Ok(empty_generic_plan(projection, projected_label_filter));
        }

        let matcher_started = QueryStageTimer::start(instrumentation_mode);
        let compiled = compile_label_matchers(matchers);
        context.profile.stages.candidate_selection = context
            .profile
            .stages
            .candidate_selection
            .saturating_add(matcher_started.elapsed());
        let compiled = compiled?;
        let candidates_started = QueryStageTimer::start(instrumentation_mode);
        let symbol_lookup_before = context.profile.stages.symbol_lookup;
        let candidates_result =
            facade_candidate_refs(context, &compiled, projection, budget, instrumentation_mode);
        let symbol_lookup_elapsed = context
            .profile
            .stages
            .symbol_lookup
            .saturating_sub(symbol_lookup_before);
        context.profile.stages.candidate_selection =
            context.profile.stages.candidate_selection.saturating_add(
                candidates_started
                    .elapsed()
                    .saturating_sub(symbol_lookup_elapsed),
            );
        let candidates = match candidates_result? {
            Ok(candidates) => candidates,
            Err(SegmentPruneReason::MissingEquality) => {
                budget.observe_segment_skipped_by_missing_equality();
                return Ok(empty_generic_plan(projection, projected_label_filter));
            }
            Err(SegmentPruneReason::MatcherTimeRange) => {
                // The facade never emits this reason because time pruning is
                // intentionally identical and conservative for both layouts.
                budget.observe_segment_skipped_by_matcher_time_range();
                return Ok(empty_generic_plan(projection, projected_label_filter));
            }
        };
        if candidates.is_empty() {
            return Ok(empty_generic_plan(projection, projected_label_filter));
        }

        let match_projection_names = projection_matches_promql_metric_name_regex(projection);
        let mut series = Vec::new();
        let mut payload_requests = Vec::new();
        let metadata = &context.metadata;
        let root = &context.root;
        let profile = &mut context.profile;
        let stages_before_visit = profile.stages;
        let visit_started = QueryStageTimer::start(instrumentation_mode);
        let visit_result = if label_interner.policy() == QueryLabelStoragePolicy::CompactIds {
            let mut visit_verified =
                |verified: crate::storage::segment::metadata_facade::SegmentVerifiedEncodedSeries<'_>,
                 materialization: crate::storage::series::v3::CanonicalLabelMaterializationProfile|
                 -> io::Result<SegmentMetadataVisitControl> {
                    profile.stages.canonical_row_decode = profile
                        .stages
                        .canonical_row_decode
                        .saturating_add(materialization.canonical_row_decode);
                    profile.stages.symbol_resolution = profile
                        .stages
                        .symbol_resolution
                        .saturating_add(materialization.symbol_resolution);
                    profile.stages.canonical_identity = profile
                        .stages
                        .canonical_identity
                        .saturating_add(materialization.canonical_identity);
                    profile.stages.label_construction = profile
                        .stages
                        .label_construction
                        .saturating_add(materialization.label_construction);

                    let labels_started = QueryStageTimer::start(instrumentation_mode);
                    let cached_labels = verified
                        .labels_complete()
                        .then(|| label_cache.get(&verified.series_id()).cloned())
                        .flatten();
                    let should_cache = verified.labels_complete() && cached_labels.is_none();
                    let labels = match cached_labels {
                        Some(labels) => labels,
                        None => label_interner.try_intern_encoded_labels(verified.labels())?,
                    };
                    profile.stages.label_construction = profile
                        .stages
                        .label_construction
                        .saturating_add(labels_started.elapsed());
                    profile.observe_query_label_materialization(
                        verified.integrity_checked_label_count(),
                        verified.labels_complete(),
                        &labels,
                    );
                    let matcher_started = QueryStageTimer::start(instrumentation_mode);
                    let matches = labels_match_query(
                        &labels,
                        &compiled,
                        match_projection_names,
                    ) && series_kind_mask_matches_projection(projection, verified.kind_mask());
                    profile.stages.matcher_evaluation = profile
                        .stages
                        .matcher_evaluation
                        .saturating_add(matcher_started.elapsed());
                    if !matches {
                        return Ok(SegmentMetadataVisitControl::Continue);
                    }
                    budget.observe_matched_series(verified.series_id())?;
                    if should_cache {
                        label_cache.insert(verified.series_id(), labels.clone());
                    }
                    let locator_started = QueryStageTimer::start(instrumentation_mode);
                    let mut locators = Vec::with_capacity(verified.chunks().len());
                    let locator_result = (|| -> io::Result<bool> {
                        verified.chunks().visit(|locator| {
                            locators.push(locator.to_owned_indexed_locator());
                            Ok::<_, io::Error>(SegmentMetadataVisitControl::Continue)
                        })?;

                        let mut has_payload = false;
                        for locator in &locators {
                            let chunk = locator.entry();
                            if chunk.max_time_ms < start_ms || chunk.min_time_ms > end_ms {
                                continue;
                            }
                            let read_len =
                                if typed_scalar_projection(projection, chunk.kind).is_some() {
                                    chunk.scalar_projection_read_len()
                                } else if chunk_kind_matches_projection(projection, chunk.kind) {
                                    chunk.length
                                } else {
                                    continue;
                                };
                            budget.observe_chunk_read(u64::from(read_len))?;
                            payload_requests.push(ChunkPayloadRead {
                                file_id: chunk.file_id,
                                offset: chunk.offset,
                                len: u64::from(read_len),
                            });
                            has_payload = true;
                        }
                        Ok(has_payload)
                    })();
                    profile.stages.locator_planning = profile
                        .stages
                        .locator_planning
                        .saturating_add(locator_started.elapsed());
                    let has_payload = locator_result?;
                    if has_payload {
                        series.push(GenericCrossSegmentSeries {
                            series_id: verified.series_id(),
                            metric_name_dropped_series_id: verified
                                .metric_name_dropped_series_id(),
                            labels,
                            labels_complete: verified.labels_complete(),
                            chunks: Arc::new(locators),
                        });
                    }
                    Ok(SegmentMetadataVisitControl::Continue)
                };
            let visit = match (label_demand.included_names(), instrumentation_mode) {
                (Some(label_names), QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_encoded_series_selected_profiled(
                        root,
                        &candidates,
                        label_names,
                        SERIES_KIND_FLOAT | SERIES_KIND_INT64,
                        label_demand.derives_metric_name_dropped_identity(),
                        &mut visit_verified,
                    ),
                (Some(label_names), QueryInstrumentationMode::Off) => metadata
                    .visit_verified_encoded_series_selected(
                        root,
                        &candidates,
                        label_names,
                        SERIES_KIND_FLOAT | SERIES_KIND_INT64,
                        label_demand.derives_metric_name_dropped_identity(),
                        |verified| {
                            visit_verified(
                                verified,
                                crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                            )
                        },
                    ),
                (None, QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_encoded_series_profiled(
                        root,
                        &candidates,
                        &mut visit_verified,
                    ),
                (None, QueryInstrumentationMode::Off) => metadata
                    .visit_verified_encoded_series(root, &candidates, |verified| {
                        visit_verified(
                            verified,
                            crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                        )
                    }),
            };
            map_metadata_visit(visit)
        } else {
            let mut visit_verified =
                |verified: crate::storage::segment::metadata_facade::SegmentVerifiedSeries<'_>,
                 materialization: crate::storage::series::v3::CanonicalLabelMaterializationProfile|
                 -> io::Result<SegmentMetadataVisitControl> {
                profile.stages.canonical_row_decode = profile
                    .stages
                    .canonical_row_decode
                    .saturating_add(materialization.canonical_row_decode);
                profile.stages.symbol_resolution = profile
                    .stages
                    .symbol_resolution
                    .saturating_add(materialization.symbol_resolution);
                profile.stages.canonical_identity = profile
                    .stages
                    .canonical_identity
                    .saturating_add(materialization.canonical_identity);
                profile.stages.label_construction = profile
                    .stages
                    .label_construction
                    .saturating_add(materialization.label_construction);
                profile.observe_label_materialization(
                    verified.integrity_checked_label_count(),
                    verified.labels_complete(),
                    verified.labels(),
                );
                let matcher_started = QueryStageTimer::start(instrumentation_mode);
                let matches = labels_match_facade(
                    verified.labels(),
                    &compiled,
                    match_projection_names,
                ) && series_kind_mask_matches_projection(projection, verified.kind_mask());
                profile.stages.matcher_evaluation = profile
                    .stages
                    .matcher_evaluation
                    .saturating_add(matcher_started.elapsed());
                if !matches {
                    return Ok(SegmentMetadataVisitControl::Continue);
                }
                budget.observe_matched_series(verified.series_id())?;
                let labels_started = QueryStageTimer::start(instrumentation_mode);
                let labels = if verified.labels_complete() {
                    if let Some(labels) = label_cache.get(&verified.series_id()) {
                        labels.clone()
                    } else {
                        let labels =
                            label_interner.try_intern_labels(verified.labels().to_vec())?;
                        label_cache.insert(verified.series_id(), labels.clone());
                        labels
                    }
                } else {
                    // Partial labels are scoped to this terminal aggregation.
                    // They must never poison the session-wide full-label cache.
                    label_interner.try_intern_labels(verified.labels().to_vec())?
                };
                profile.stages.label_construction = profile
                    .stages
                    .label_construction
                    .saturating_add(labels_started.elapsed());
                let locator_started = QueryStageTimer::start(instrumentation_mode);
                let mut locators = Vec::with_capacity(verified.chunks().len());
                let locator_result = (|| -> io::Result<bool> {
                    verified.chunks().visit(|locator| {
                        locators.push(locator.to_owned_indexed_locator());
                        Ok::<_, io::Error>(SegmentMetadataVisitControl::Continue)
                    })?;

                    let mut has_payload = false;
                    for locator in &locators {
                        let chunk = locator.entry();
                        if chunk.max_time_ms < start_ms || chunk.min_time_ms > end_ms {
                            continue;
                        }
                        let read_len = if typed_scalar_projection(projection, chunk.kind).is_some() {
                            chunk.scalar_projection_read_len()
                        } else if chunk_kind_matches_projection(projection, chunk.kind) {
                            chunk.length
                        } else {
                            continue;
                        };
                        budget.observe_chunk_read(u64::from(read_len))?;
                        payload_requests.push(ChunkPayloadRead {
                            file_id: chunk.file_id,
                            offset: chunk.offset,
                            len: u64::from(read_len),
                        });
                        has_payload = true;
                    }
                    Ok(has_payload)
                })();
                profile.stages.locator_planning = profile
                    .stages
                    .locator_planning
                    .saturating_add(locator_started.elapsed());
                let has_payload = locator_result?;
                if has_payload {
                    series.push(GenericCrossSegmentSeries {
                        series_id: verified.series_id(),
                        metric_name_dropped_series_id: verified
                            .metric_name_dropped_series_id(),
                        labels,
                        labels_complete: verified.labels_complete(),
                        chunks: Arc::new(locators),
                    });
                }
                Ok(SegmentMetadataVisitControl::Continue)
                };
            let visit = match (label_demand.included_names(), instrumentation_mode) {
                (Some(label_names), QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_series_selected_profiled(
                        root,
                        &candidates,
                        label_names,
                        SERIES_KIND_FLOAT | SERIES_KIND_INT64,
                        label_demand.derives_metric_name_dropped_identity(),
                        &mut visit_verified,
                    ),
                (Some(label_names), QueryInstrumentationMode::Off) => metadata
                    .visit_verified_series_selected(
                        root,
                        &candidates,
                        label_names,
                        SERIES_KIND_FLOAT | SERIES_KIND_INT64,
                        label_demand.derives_metric_name_dropped_identity(),
                        |verified| {
                            visit_verified(
                                verified,
                                crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                            )
                        },
                    ),
                (None, QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_series_profiled(root, &candidates, &mut visit_verified),
                (None, QueryInstrumentationMode::Off) => metadata.visit_verified_series(
                    root,
                    &candidates,
                    |verified| {
                        visit_verified(
                            verified,
                            crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                        )
                    },
                ),
            };
            map_metadata_visit(visit)
        };
        let attributed = profile
            .stages
            .delta_since(stages_before_visit)
            .total_exclusive();
        profile.stages.metadata_visit_overhead = profile
            .stages
            .metadata_visit_overhead
            .saturating_add(visit_started.elapsed().saturating_sub(attributed));
        visit_result?;

        Ok(GenericCrossSegmentPlan {
            projection: projection.clone(),
            projected_label_filter,
            terminal_output_names: label_demand.output_names_arc(),
            series,
            payload_requests,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "cross-segment native planning keeps query bounds, budget, and label state explicit"
    )]
    pub(in crate::storage::segment) fn plan_native_histogram_cross_segment_with_facade_context(
        &self,
        context: &mut FacadeSegmentQueryContext,
        matchers: &[NormalizedMatcher],
        label_demand: &QueryLabelDemand,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        self.plan_native_typed_cross_segment_with_facade_context(
            context,
            matchers,
            label_demand,
            SegmentProjection::NativeHistogram,
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "cross-segment native planning keeps query bounds, budget, and label state explicit"
    )]
    pub(in crate::storage::segment) fn plan_native_exponential_histogram_cross_segment_with_facade_context(
        &self,
        context: &mut FacadeSegmentQueryContext,
        matchers: &[NormalizedMatcher],
        label_demand: &QueryLabelDemand,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        self.plan_native_typed_cross_segment_with_facade_context(
            context,
            matchers,
            label_demand,
            SegmentProjection::NativeExponentialHistogram,
            start_ms,
            end_ms,
            budget,
            label_cache,
            label_interner,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the shared typed planner keeps facade, projection, budget, and label state explicit"
    )]
    fn plan_native_typed_cross_segment_with_facade_context(
        &self,
        context: &mut FacadeSegmentQueryContext,
        matchers: &[NormalizedMatcher],
        label_demand: &QueryLabelDemand,
        projection: SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        label_interner: &mut QueryLabelInterner,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let instrumentation_mode = context.instrumentation_mode();
        let empty = || NativeTypedCrossSegmentPlan {
            series: Vec::new(),
            payload_requests: Vec::new(),
            terminal_output_names: None,
        };
        if end_ms < start_ms {
            return Ok(empty());
        }

        let matcher_started = QueryStageTimer::start(instrumentation_mode);
        let compiled = compile_label_matchers(matchers);
        context.profile.stages.candidate_selection = context
            .profile
            .stages
            .candidate_selection
            .saturating_add(matcher_started.elapsed());
        let compiled = compiled?;
        let candidates_started = QueryStageTimer::start(instrumentation_mode);
        let symbol_lookup_before = context.profile.stages.symbol_lookup;
        let candidates_result = facade_candidate_refs(
            context,
            &compiled,
            &projection,
            budget,
            instrumentation_mode,
        );
        let symbol_lookup_elapsed = context
            .profile
            .stages
            .symbol_lookup
            .saturating_sub(symbol_lookup_before);
        context.profile.stages.candidate_selection =
            context.profile.stages.candidate_selection.saturating_add(
                candidates_started
                    .elapsed()
                    .saturating_sub(symbol_lookup_elapsed),
            );
        let candidates = match candidates_result? {
            Ok(candidates) => candidates,
            Err(SegmentPruneReason::MissingEquality) => {
                budget.observe_segment_skipped_by_missing_equality();
                return Ok(empty());
            }
            Err(SegmentPruneReason::MatcherTimeRange) => {
                budget.observe_segment_skipped_by_matcher_time_range();
                return Ok(empty());
            }
        };
        if candidates.is_empty() {
            return Ok(empty());
        }

        let match_projection_names = projection_matches_promql_metric_name_regex(&projection);
        let mut series = Vec::new();
        let mut payload_requests = Vec::new();
        let metadata = &context.metadata;
        let root = &context.root;
        let profile = &mut context.profile;
        let stages_before_visit = profile.stages;
        let visit_started = QueryStageTimer::start(instrumentation_mode);
        let visit_result = if label_interner.policy() == QueryLabelStoragePolicy::CompactIds {
            let mut visit_verified =
                |verified: crate::storage::segment::metadata_facade::SegmentVerifiedEncodedSeries<'_>,
                 materialization: crate::storage::series::v3::CanonicalLabelMaterializationProfile|
                 -> io::Result<SegmentMetadataVisitControl> {
                    profile.stages.canonical_row_decode = profile
                        .stages
                        .canonical_row_decode
                        .saturating_add(materialization.canonical_row_decode);
                    profile.stages.symbol_resolution = profile
                        .stages
                        .symbol_resolution
                        .saturating_add(materialization.symbol_resolution);
                    profile.stages.canonical_identity = profile
                        .stages
                        .canonical_identity
                        .saturating_add(materialization.canonical_identity);
                    profile.stages.label_construction = profile
                        .stages
                        .label_construction
                        .saturating_add(materialization.label_construction);

                    let labels_started = QueryStageTimer::start(instrumentation_mode);
                    let cached_labels = verified
                        .labels_complete()
                        .then(|| label_cache.get(&verified.series_id()).cloned())
                        .flatten();
                    let should_cache = verified.labels_complete() && cached_labels.is_none();
                    let labels = match cached_labels {
                        Some(labels) => labels,
                        None => label_interner.try_intern_encoded_labels(verified.labels())?,
                    };
                    profile.stages.label_construction = profile
                        .stages
                        .label_construction
                        .saturating_add(labels_started.elapsed());
                    profile.observe_query_label_materialization(
                        verified.integrity_checked_label_count(),
                        verified.labels_complete(),
                        &labels,
                    );
                    let matcher_started = QueryStageTimer::start(instrumentation_mode);
                    let matches = labels_match_query(
                        &labels,
                        &compiled,
                        match_projection_names,
                    ) && series_kind_mask_matches_projection(&projection, verified.kind_mask());
                    profile.stages.matcher_evaluation = profile
                        .stages
                        .matcher_evaluation
                        .saturating_add(matcher_started.elapsed());
                    if !matches {
                        return Ok(SegmentMetadataVisitControl::Continue);
                    }
                    budget.observe_matched_series(verified.series_id())?;
                    if should_cache {
                        label_cache.insert(verified.series_id(), labels.clone());
                    }
                    let locator_started = QueryStageTimer::start(instrumentation_mode);
                    let mut chunks = Vec::new();
                    let locator_result = verified.chunks().visit(|locator| {
                        if locator.max_time_ms() < start_ms
                            || locator.min_time_ms() > end_ms
                            || !chunk_kind_matches_projection(&projection, locator.kind())
                        {
                            return Ok::<_, io::Error>(SegmentMetadataVisitControl::Continue);
                        }
                        let read_len = u64::from(locator.chunk_len());
                        budget.observe_chunk_read(read_len)?;
                        payload_requests.push(ChunkPayloadRead {
                            file_id: locator.file_id(),
                            offset: locator.file_offset(),
                            len: read_len,
                        });
                        chunks.push(locator.to_owned_indexed_locator());
                        Ok(SegmentMetadataVisitControl::Continue)
                    });
                    profile.stages.locator_planning = profile
                        .stages
                        .locator_planning
                        .saturating_add(locator_started.elapsed());
                    locator_result?;
                    if !chunks.is_empty() {
                        series.push(NativeTypedCrossSegmentSeries {
                            series_id: verified.series_id(),
                            metric_name_dropped_series_id: verified
                                .metric_name_dropped_series_id(),
                            labels,
                            labels_complete: verified.labels_complete(),
                            chunks,
                        });
                    }
                    Ok(SegmentMetadataVisitControl::Continue)
                };
            let selective_kind_mask = match projection {
                SegmentProjection::NativeHistogram => SERIES_KIND_HISTOGRAM,
                SegmentProjection::NativeExponentialHistogram => SERIES_KIND_EXPONENTIAL_HISTOGRAM,
                _ => unreachable!("native typed planning requires a native projection"),
            };
            let visit = match (label_demand.included_names(), instrumentation_mode) {
                (Some(label_names), QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_encoded_series_selected_profiled(
                        root,
                        &candidates,
                        label_names,
                        selective_kind_mask,
                        label_demand.derives_metric_name_dropped_identity(),
                        &mut visit_verified,
                    ),
                (Some(label_names), QueryInstrumentationMode::Off) => metadata
                    .visit_verified_encoded_series_selected(
                        root,
                        &candidates,
                        label_names,
                        selective_kind_mask,
                        label_demand.derives_metric_name_dropped_identity(),
                        |verified| {
                            visit_verified(
                                verified,
                                crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                            )
                        },
                    ),
                (None, QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_encoded_series_profiled(
                        root,
                        &candidates,
                        &mut visit_verified,
                    ),
                (None, QueryInstrumentationMode::Off) => metadata
                    .visit_verified_encoded_series(root, &candidates, |verified| {
                        visit_verified(
                            verified,
                            crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                        )
                    }),
            };
            map_metadata_visit(visit)
        } else {
            let mut visit_verified =
                |verified: crate::storage::segment::metadata_facade::SegmentVerifiedSeries<'_>,
                 materialization: crate::storage::series::v3::CanonicalLabelMaterializationProfile|
                 -> io::Result<SegmentMetadataVisitControl> {
                profile.stages.canonical_row_decode = profile
                    .stages
                    .canonical_row_decode
                    .saturating_add(materialization.canonical_row_decode);
                profile.stages.symbol_resolution = profile
                    .stages
                    .symbol_resolution
                    .saturating_add(materialization.symbol_resolution);
                profile.stages.canonical_identity = profile
                    .stages
                    .canonical_identity
                    .saturating_add(materialization.canonical_identity);
                profile.stages.label_construction = profile
                    .stages
                    .label_construction
                    .saturating_add(materialization.label_construction);
                profile.observe_label_materialization(
                    verified.integrity_checked_label_count(),
                    verified.labels_complete(),
                    verified.labels(),
                );
                let matcher_started = QueryStageTimer::start(instrumentation_mode);
                let matches = labels_match_facade(
                    verified.labels(),
                    &compiled,
                    match_projection_names,
                ) && series_kind_mask_matches_projection(&projection, verified.kind_mask());
                profile.stages.matcher_evaluation = profile
                    .stages
                    .matcher_evaluation
                    .saturating_add(matcher_started.elapsed());
                if !matches {
                    return Ok(SegmentMetadataVisitControl::Continue);
                }
                budget.observe_matched_series(verified.series_id())?;
                let labels_started = QueryStageTimer::start(instrumentation_mode);
                let labels = if verified.labels_complete() {
                    if let Some(labels) = label_cache.get(&verified.series_id()) {
                        labels.clone()
                    } else {
                        let labels =
                            label_interner.try_intern_labels(verified.labels().to_vec())?;
                        label_cache.insert(verified.series_id(), labels.clone());
                        labels
                    }
                } else {
                    // Selective native labels belong only to this terminal
                    // aggregation and must not enter the complete-label cache.
                    label_interner.try_intern_labels(verified.labels().to_vec())?
                };
                profile.stages.label_construction = profile
                    .stages
                    .label_construction
                    .saturating_add(labels_started.elapsed());
                let locator_started = QueryStageTimer::start(instrumentation_mode);
                let mut chunks = Vec::new();
                let locator_result = verified.chunks().visit(|locator| {
                    if locator.max_time_ms() < start_ms
                        || locator.min_time_ms() > end_ms
                        || !chunk_kind_matches_projection(&projection, locator.kind())
                    {
                        return Ok::<_, io::Error>(SegmentMetadataVisitControl::Continue);
                    }
                    let read_len = u64::from(locator.chunk_len());
                    budget.observe_chunk_read(read_len)?;
                    payload_requests.push(ChunkPayloadRead {
                        file_id: locator.file_id(),
                        offset: locator.file_offset(),
                        len: read_len,
                    });
                    chunks.push(locator.to_owned_indexed_locator());
                    Ok(SegmentMetadataVisitControl::Continue)
                });
                profile.stages.locator_planning = profile
                    .stages
                    .locator_planning
                    .saturating_add(locator_started.elapsed());
                locator_result?;
                if !chunks.is_empty() {
                    series.push(NativeTypedCrossSegmentSeries {
                        series_id: verified.series_id(),
                        metric_name_dropped_series_id: verified
                            .metric_name_dropped_series_id(),
                        labels,
                        labels_complete: verified.labels_complete(),
                        chunks,
                    });
                }
                Ok(SegmentMetadataVisitControl::Continue)
                };
            let selective_kind_mask = match projection {
                SegmentProjection::NativeHistogram => SERIES_KIND_HISTOGRAM,
                SegmentProjection::NativeExponentialHistogram => SERIES_KIND_EXPONENTIAL_HISTOGRAM,
                _ => unreachable!("native typed planning requires a native projection"),
            };
            let visit = match (label_demand.included_names(), instrumentation_mode) {
                (Some(label_names), QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_series_selected_profiled(
                        root,
                        &candidates,
                        label_names,
                        selective_kind_mask,
                        label_demand.derives_metric_name_dropped_identity(),
                        &mut visit_verified,
                    ),
                (Some(label_names), QueryInstrumentationMode::Off) => metadata
                    .visit_verified_series_selected(
                        root,
                        &candidates,
                        label_names,
                        selective_kind_mask,
                        label_demand.derives_metric_name_dropped_identity(),
                        |verified| {
                            visit_verified(
                                verified,
                                crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                            )
                        },
                    ),
                (None, QueryInstrumentationMode::Detailed) => metadata
                    .visit_verified_series_profiled(root, &candidates, &mut visit_verified),
                (None, QueryInstrumentationMode::Off) => metadata.visit_verified_series(
                    root,
                    &candidates,
                    |verified| {
                        visit_verified(
                            verified,
                            crate::storage::series::v3::CanonicalLabelMaterializationProfile::default(),
                        )
                    },
                ),
            };
            map_metadata_visit(visit)
        };
        let attributed = profile
            .stages
            .delta_since(stages_before_visit)
            .total_exclusive();
        profile.stages.metadata_visit_overhead = profile
            .stages
            .metadata_visit_overhead
            .saturating_add(visit_started.elapsed().saturating_sub(attributed));
        visit_result?;
        Ok(NativeTypedCrossSegmentPlan {
            series,
            payload_requests,
            terminal_output_names: label_demand.output_names_arc(),
        })
    }
}

fn empty_generic_plan(
    projection: &SegmentProjection,
    projected_label_filter: Option<Vec<CompiledLabelMatcher>>,
) -> GenericCrossSegmentPlan {
    GenericCrossSegmentPlan {
        projection: projection.clone(),
        projected_label_filter,
        terminal_output_names: None,
        series: Vec::new(),
        payload_requests: Vec::new(),
    }
}

fn facade_candidate_refs(
    context: &mut FacadeSegmentQueryContext,
    matchers: &[CompiledLabelMatcher],
    projection: &SegmentProjection,
    budget: &mut QueryBudget,
    instrumentation_mode: QueryInstrumentationMode,
) -> io::Result<Result<GovernedSeriesRefSet, SegmentPruneReason>> {
    let metadata = &context.metadata;
    let root = &context.root;
    let profile = &mut context.profile;

    // Resolve every exact positive first so a missing required equality can
    // prune without counting the segment as queried, matching the established
    // QueryBudget contract.
    let mut equality_selections = Vec::new();
    for matcher in matchers {
        let CompiledLabelMatcher::Eq { name, value } = matcher else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        let Some(selection) =
            exact_selection(metadata, root, name, value, profile, instrumentation_mode)?
        else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let cardinality_key = metadata
            .exact_postings_cardinality_key(root, &selection)
            .map_err(metadata_error_to_io)?;
        equality_selections.push((cardinality_key, selection));
    }
    sort_equality_selections_by_cardinality(&mut equality_selections);
    budget.observe_segment_queried();

    let mut candidates = None;
    for (_, selection) in equality_selections {
        let positive = read_postings_set(metadata, root, selection, budget, profile)?;
        candidates = Some(match candidates {
            Some(current) => metadata
                .intersect_series_ref_sets(root, &current, &positive)
                .map_err(metadata_error_to_io)?,
            None => positive,
        });
        if candidates
            .as_ref()
            .is_some_and(GovernedSeriesRefSet::is_empty)
        {
            return Ok(Ok(candidates.expect("empty candidate set exists")));
        }
    }

    let match_projection_names = projection_matches_promql_metric_name_regex(projection);
    for matcher in matchers {
        let CompiledLabelMatcher::Regex { pattern, .. } = matcher else {
            continue;
        };
        if pattern.is_match("") {
            continue;
        }
        let positive = regex_postings_set(
            metadata,
            root,
            matcher,
            match_projection_names,
            budget,
            profile,
            instrumentation_mode,
        )?;
        candidates = Some(match candidates {
            Some(current) => metadata
                .intersect_series_ref_sets(root, &current, &positive)
                .map_err(metadata_error_to_io)?,
            None => positive,
        });
        if candidates
            .as_ref()
            .is_some_and(GovernedSeriesRefSet::is_empty)
        {
            return Ok(Ok(candidates.expect("empty candidate set exists")));
        }
    }

    let mut candidates = match candidates {
        Some(candidates) => candidates,
        None => metadata
            .all_series_ref_set(root)
            .map_err(metadata_error_to_io)?,
    };

    for matcher in matchers {
        let excluded = match matcher {
            CompiledLabelMatcher::NotEq { name, value } if !value.is_empty() => {
                match exact_selection(metadata, root, name, value, profile, instrumentation_mode)? {
                    Some(selection) => Some(read_postings_set(
                        metadata, root, selection, budget, profile,
                    )?),
                    None => None,
                }
            }
            CompiledLabelMatcher::NotRegex { pattern, .. } if !pattern.is_match("") => {
                Some(regex_postings_set(
                    metadata,
                    root,
                    matcher,
                    match_projection_names,
                    budget,
                    profile,
                    instrumentation_mode,
                )?)
            }
            CompiledLabelMatcher::Eq { .. }
            | CompiledLabelMatcher::NotEq { .. }
            | CompiledLabelMatcher::Regex { .. }
            | CompiledLabelMatcher::NotRegex { .. } => None,
        };
        if let Some(excluded) = excluded {
            candidates = metadata
                .difference_series_ref_sets(root, &candidates, &excluded)
                .map_err(metadata_error_to_io)?;
            if candidates.is_empty() {
                break;
            }
        }
    }

    budget.observe_candidate_series_refs(candidates.len() as u64)?;
    Ok(Ok(candidates))
}

fn sort_equality_selections_by_cardinality<T>(selections: &mut [(u64, T)]) {
    selections.sort_by_key(|(cardinality_key, _)| *cardinality_key);
}

fn exact_selection(
    metadata: &SegmentMetadataSession,
    root: &SegmentMetadataRoot,
    name: &str,
    value: &str,
    profile: &mut SegmentStoreQueryProfile,
    instrumentation_mode: QueryInstrumentationMode,
) -> io::Result<Option<SegmentExactPostingsSelection>> {
    let lookup_started = QueryStageTimer::start(instrumentation_mode);
    let symbols = (|| -> io::Result<Option<(u32, u32)>> {
        let Some(name_sym) = metadata
            .lookup_symbol(root, name)
            .map_err(metadata_error_to_io)?
        else {
            return Ok(None);
        };
        let Some(value_sym) = metadata
            .lookup_symbol(root, value)
            .map_err(metadata_error_to_io)?
        else {
            return Ok(None);
        };
        Ok(Some((name_sym, value_sym)))
    })();
    profile.stages.symbol_lookup = profile
        .stages
        .symbol_lookup
        .saturating_add(lookup_started.elapsed());
    let Some((name_sym, value_sym)) = symbols? else {
        return Ok(None);
    };
    metadata
        .select_exact_postings(root, name_sym, value_sym)
        .map_err(metadata_error_to_io)
}

fn read_postings_set(
    metadata: &SegmentMetadataSession,
    root: &SegmentMetadataRoot,
    selection: SegmentExactPostingsSelection,
    budget: &mut QueryBudget,
    profile: &mut SegmentStoreQueryProfile,
) -> io::Result<GovernedSeriesRefSet> {
    let encoded_len = metadata
        .exact_postings_encoded_len(root, &selection)
        .map_err(metadata_error_to_io)?;
    budget.observe_index_postings_read(encoded_len);
    let started = Instant::now();
    let postings = metadata
        .read_exact_postings(root, &selection)
        .map_err(metadata_error_to_io)?;
    profile.exact_postings_read = profile
        .exact_postings_read
        .saturating_add(started.elapsed());
    profile.exact_postings_bytes = profile.exact_postings_bytes.saturating_add(encoded_len);
    metadata
        .exact_postings_ref_set(root, &postings)
        .map_err(metadata_error_to_io)
}

fn regex_postings_set(
    metadata: &SegmentMetadataSession,
    root: &SegmentMetadataRoot,
    matcher: &CompiledLabelMatcher,
    match_projection_names: bool,
    budget: &mut QueryBudget,
    profile: &mut SegmentStoreQueryProfile,
    instrumentation_mode: QueryInstrumentationMode,
) -> io::Result<GovernedSeriesRefSet> {
    let lookup_started = QueryStageTimer::start(instrumentation_mode);
    let name_sym = metadata
        .lookup_symbol(root, matcher.name())
        .map_err(metadata_error_to_io);
    profile.stages.symbol_lookup = profile
        .stages
        .symbol_lookup
        .saturating_add(lookup_started.elapsed());
    let Some(name_sym) = name_sym? else {
        return metadata
            .series_ref_set(root, &[])
            .map_err(metadata_error_to_io);
    };

    let mut matched = metadata
        .series_ref_set(root, &[])
        .map_err(metadata_error_to_io)?;
    let mut failure = None;
    let prefixes = regex_matcher_literal_prefixes(matcher, match_projection_names);
    let mut seen_value_symbols = BTreeSet::new();
    let visit_prefixes = if prefixes.is_empty() {
        vec![None]
    } else {
        prefixes
            .iter()
            .map(|prefix| Some(prefix.as_str()))
            .collect()
    };
    for prefix in visit_prefixes {
        metadata
            .visit_label_values(root, name_sym, prefix, None, |value_sym, value| {
                if !seen_value_symbols.insert(value_sym) {
                    return true;
                }
                if let Err(error) = budget.observe_regex_value() {
                    failure = Some(error);
                    return false;
                }
                if !regex_pattern_matches(matcher, value, match_projection_names) {
                    return true;
                }
                let next = (|| -> io::Result<GovernedSeriesRefSet> {
                    let Some(selection) = metadata
                        .select_exact_postings(root, name_sym, value_sym)
                        .map_err(metadata_error_to_io)?
                    else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "label-value FST entry has no exact postings record",
                        ));
                    };
                    let postings = read_postings_set(metadata, root, selection, budget, profile)?;
                    metadata
                        .union_series_ref_sets(root, &matched, &postings)
                        .map_err(metadata_error_to_io)
                })();
                match next {
                    Ok(next) => matched = next,
                    Err(error) => {
                        failure = Some(error);
                        return false;
                    }
                }
                true
            })
            .map_err(metadata_error_to_io)?;
        if failure.is_some() {
            break;
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(matched)
}

fn regex_matcher_literal_prefixes(
    matcher: &CompiledLabelMatcher,
    match_projection_names: bool,
) -> Vec<String> {
    let (name, pattern) = match matcher {
        CompiledLabelMatcher::Regex { name, pattern }
        | CompiledLabelMatcher::NotRegex { name, pattern } => (name, pattern),
        CompiledLabelMatcher::Eq { .. } | CompiledLabelMatcher::NotEq { .. } => {
            return Vec::new();
        }
    };
    let compiled = pattern.as_str();
    let source = compiled
        .strip_prefix("^(?:")
        .and_then(|source| source.strip_suffix(")$"))
        .unwrap_or(compiled);
    regex_literal_prefixes(source, match_projection_names && name == METRIC_NAME_LABEL)
}

fn regex_pattern_matches(
    matcher: &CompiledLabelMatcher,
    value: &str,
    match_projection_names: bool,
) -> bool {
    match matcher {
        CompiledLabelMatcher::Regex { .. } => matcher.matches_value(value, match_projection_names),
        CompiledLabelMatcher::NotRegex { .. } => {
            !matcher.matches_value(value, match_projection_names)
        }
        CompiledLabelMatcher::Eq { .. } | CompiledLabelMatcher::NotEq { .. } => false,
    }
}

fn labels_match_facade(
    labels: &[(String, String)],
    matchers: &[CompiledLabelMatcher],
    match_projection_names: bool,
) -> bool {
    matchers.iter().all(|matcher| {
        let value = labels
            .iter()
            .find_map(|(name, value)| (name == matcher.name()).then_some(value.as_str()))
            .unwrap_or("");
        matcher.matches_value(value, match_projection_names)
    })
}

fn labels_match_query(
    labels: &QueryLabels,
    matchers: &[CompiledLabelMatcher],
    match_projection_names: bool,
) -> bool {
    matchers.iter().all(|matcher| {
        let value = labels
            .pairs()
            .find_map(|(name, value)| (name == matcher.name()).then_some(value))
            .unwrap_or("");
        matcher.matches_value(value, match_projection_names)
    })
}

fn map_metadata_visit(
    result: Result<
        crate::storage::segment::metadata_facade::SegmentMetadataVisitOutcome,
        SegmentMetadataVisitError<io::Error>,
    >,
) -> io::Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(SegmentMetadataVisitError::Metadata(error)) => Err(metadata_error_to_io(error)),
        Err(SegmentMetadataVisitError::Visitor(error)) => Err(error),
    }
}

fn metadata_error_to_io(error: SegmentMetadataFacadeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_planning_ignores_adaptive_encoded_length_inversions() {
        // Adaptive bytes would order these as dense-three, dense-four,
        // sparse-two. The format-neutral raw-cardinality keys must preserve
        // the established two, three, four selectivity order instead.
        let mut selections = [
            (16, ("dense-three", 7u64)),
            (12, ("sparse-two", 12u64)),
            (20, ("dense-four", 8u64)),
        ];

        sort_equality_selections_by_cardinality(&mut selections);

        assert_eq!(
            selections.map(|(_, (name, _encoded_len))| name),
            ["sparse-two", "dense-three", "dense-four"]
        );
    }

    #[test]
    fn facade_regex_planning_recovers_literal_prefix_from_compiled_matcher() {
        let compiled = compile_label_matchers(&[NormalizedMatcher::Regex {
            name: METRIC_NAME_LABEL.to_string(),
            pattern: "http_client_duration_count".to_string(),
        }])
        .unwrap();

        assert_eq!(
            regex_matcher_literal_prefixes(&compiled[0], false),
            vec!["http_client_duration_count".to_string()]
        );
        assert_eq!(
            regex_matcher_literal_prefixes(&compiled[0], true),
            vec![
                "http_client_duration".to_string(),
                "http_client_duration_count".to_string(),
            ]
        );
    }

    #[test]
    fn facade_verification_applies_projected_metric_name_regex_semantics() {
        let labels = vec![(
            METRIC_NAME_LABEL.to_string(),
            "request_duration".to_string(),
        )];
        let positive = compile_label_matchers(&[NormalizedMatcher::Regex {
            name: METRIC_NAME_LABEL.to_string(),
            pattern: "request_duration_count".to_string(),
        }])
        .unwrap();
        assert!(!labels_match_facade(&labels, &positive, false));
        assert!(labels_match_facade(&labels, &positive, true));

        let negative = compile_label_matchers(&[NormalizedMatcher::NotRegex {
            name: METRIC_NAME_LABEL.to_string(),
            pattern: "request_duration_count".to_string(),
        }])
        .unwrap();
        assert!(labels_match_facade(&labels, &negative, false));
        assert!(!labels_match_facade(&labels, &negative, true));
    }

    #[test]
    fn facade_verification_preserves_missing_label_semantics() {
        let labels = Vec::new();
        for (matcher, expected) in [
            (
                NormalizedMatcher::Eq {
                    name: "zone".to_string(),
                    value: String::new(),
                },
                true,
            ),
            (
                NormalizedMatcher::NotEq {
                    name: "zone".to_string(),
                    value: String::new(),
                },
                false,
            ),
            (
                NormalizedMatcher::Regex {
                    name: "zone".to_string(),
                    pattern: ".*".to_string(),
                },
                true,
            ),
            (
                NormalizedMatcher::NotRegex {
                    name: "zone".to_string(),
                    pattern: ".*".to_string(),
                },
                false,
            ),
        ] {
            let compiled = compile_label_matchers(&[matcher]).unwrap();
            assert_eq!(labels_match_facade(&labels, &compiled, false), expected);
        }
    }
}
