use super::*;
use crate::storage::segment::metadata_facade::{
    GovernedSeriesRefSet, SegmentExactPostingsSelection, SegmentMetadataFacadeError,
    SegmentMetadataRoot, SegmentMetadataSession, SegmentMetadataVisitControl,
    SegmentMetadataVisitError,
};
use crate::storage::segment::query_context::FacadeSegmentQueryContext;

impl SegmentReader {
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
        let Some(cache_call) = cache_call else {
            let payloads = context.read_chunk_payload_batch(self, &plan.payload_requests)?;
            return self.decode_generic_cross_segment_plan(
                plan,
                &payloads,
                start_ms,
                end_ms,
                budget,
                Some(label_interner),
                projected_label_cache,
                None,
            );
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
        let payloads = context.read_chunk_payload_batch_physical(self, &physical_requests)?;
        self.decode_generic_cross_segment_plan(
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
        )
    }

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
        let payloads = context.read_chunk_payload_batch(self, &plan.payload_requests)?;
        self.decode_native_histogram_cross_segment_plan(plan, &payloads, start_ms, end_ms, budget)
    }

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
        let payloads = context.read_chunk_payload_batch(self, &plan.payload_requests)?;
        self.decode_native_exponential_histogram_cross_segment_plan(
            plan, &payloads, start_ms, end_ms, budget,
        )
    }

    /// Plans one generic query through the schema-neutral metadata facade.
    ///
    /// Both schema 6 and schema 7 deliberately use the same conservative
    /// candidate policy here. In particular, no postings time summary is used
    /// for pruning: schema-6 summaries are advisory, so applying schema-7's
    /// authenticated summaries would make the A/B execute different work.
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
            return Ok(empty_generic_plan(projection, projected_label_filter));
        }

        let compiled = compile_label_matchers(matchers)?;
        let candidates = match facade_candidate_refs(context, &compiled, projection, budget)? {
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
        let mut visit_verified =
            |verified: crate::storage::segment::metadata_facade::SegmentVerifiedSeries<'_>|
             -> io::Result<SegmentMetadataVisitControl> {
                profile.observe_label_materialization(
                    verified.integrity_checked_label_count(),
                    verified.labels_complete(),
                    verified.labels(),
                );
                if !labels_match_facade(verified.labels(), &compiled, match_projection_names)
                    || !series_kind_mask_matches_projection(projection, verified.kind_mask())
                {
                    return Ok(SegmentMetadataVisitControl::Continue);
                }

                budget.observe_matched_series(verified.series_id())?;
                let labels = if verified.labels_complete() {
                    label_cache
                        .entry(verified.series_id())
                        .or_insert_with(|| label_interner.intern_labels(verified.labels().to_vec()))
                        .clone()
                } else {
                    // Partial labels are scoped to this terminal aggregation.
                    // They must never poison the session-wide full-label cache.
                    label_interner.intern_labels(verified.labels().to_vec())
                };

                let mut locators = Vec::with_capacity(verified.chunks().len());
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
        let visit = match label_demand.included_names() {
            Some(label_names) => metadata.visit_verified_series_selected(
                root,
                &candidates,
                label_names,
                SERIES_KIND_FLOAT | SERIES_KIND_INT64,
                label_demand.derives_metric_name_dropped_identity(),
                &mut visit_verified,
            ),
            None => metadata.visit_verified_series(root, &candidates, &mut visit_verified),
        };
        map_metadata_visit(visit)?;

        Ok(GenericCrossSegmentPlan {
            projection: projection.clone(),
            projected_label_filter,
            series,
            payload_requests,
        })
    }

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
        let empty = || NativeTypedCrossSegmentPlan {
            series: Vec::new(),
            payload_requests: Vec::new(),
        };
        if end_ms < start_ms {
            return Ok(empty());
        }

        let compiled = compile_label_matchers(matchers)?;
        let candidates = match facade_candidate_refs(context, &compiled, &projection, budget)? {
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
        let mut visit_verified =
            |verified: crate::storage::segment::metadata_facade::SegmentVerifiedSeries<'_>|
             -> io::Result<SegmentMetadataVisitControl> {
                profile.observe_label_materialization(
                    verified.integrity_checked_label_count(),
                    verified.labels_complete(),
                    verified.labels(),
                );
                if !labels_match_facade(verified.labels(), &compiled, match_projection_names)
                    || !series_kind_mask_matches_projection(&projection, verified.kind_mask())
                {
                    return Ok(SegmentMetadataVisitControl::Continue);
                }

                budget.observe_matched_series(verified.series_id())?;
                let labels = if verified.labels_complete() {
                    label_cache
                        .entry(verified.series_id())
                        .or_insert_with(|| label_interner.intern_labels(verified.labels().to_vec()))
                        .clone()
                } else {
                    // Selective native labels belong only to this terminal
                    // aggregation and must not enter the complete-label cache.
                    label_interner.intern_labels(verified.labels().to_vec())
                };
                let mut chunks = Vec::new();
                verified.chunks().visit(|locator| {
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
                })?;
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
        let visit = match label_demand.included_names() {
            Some(label_names) => metadata.visit_verified_series_selected(
                root,
                &candidates,
                label_names,
                selective_kind_mask,
                label_demand.derives_metric_name_dropped_identity(),
                &mut visit_verified,
            ),
            None => metadata.visit_verified_series(root, &candidates, &mut visit_verified),
        };
        map_metadata_visit(visit)?;
        Ok(NativeTypedCrossSegmentPlan {
            series,
            payload_requests,
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
        series: Vec::new(),
        payload_requests: Vec::new(),
    }
}

fn facade_candidate_refs(
    context: &mut FacadeSegmentQueryContext,
    matchers: &[CompiledLabelMatcher],
    projection: &SegmentProjection,
    budget: &mut QueryBudget,
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
        let Some(selection) = exact_selection(metadata, root, name, value)? else {
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
                match exact_selection(metadata, root, name, value)? {
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
) -> io::Result<Option<SegmentExactPostingsSelection>> {
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
) -> io::Result<GovernedSeriesRefSet> {
    let Some(name_sym) = metadata
        .lookup_symbol(root, matcher.name())
        .map_err(metadata_error_to_io)?
    else {
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
