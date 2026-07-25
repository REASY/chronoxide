use super::*;

impl<'a> SegmentStoreQuerySession<'a> {
    pub(in crate::storage::segment) fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_budget_with_cache(selector, start_ms, end_ms, budget, None)
    }

    pub(in crate::storage::segment) fn query_selector_with_budget_with_cache(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.freeze_query_label_storage_policy();
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        if self.experimental_cross_segment_chunk_reads
            && cache_call.is_none()
            && self.should_use_cross_segment_flow(start_ms, end_ms)
        {
            return self
                .query_selector_cross_segment_with_budget(selector, start_ms, end_ms, budget);
        }

        let mut results = Vec::new();
        let label_cache = &mut self.label_cache;
        let label_interner = &mut self.label_interner;
        let projected_label_cache = &mut self.projected_label_cache;
        for (segment_ordinal, segment) in self.segments.iter_mut().enumerate() {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            results.extend(segment.query_selector_with_budget(
                selector,
                segment_ordinal,
                start_ms,
                end_ms,
                budget,
                label_cache,
                label_interner,
                projected_label_cache,
                cache_call.as_deref_mut(),
            )?);
        }
        self.extend_head_selector_results(selector, start_ms, end_ms, budget, &mut results)?;

        let results = self.merge_query_results_profiled(results);
        Ok(results)
    }

    fn query_selector_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let Some(chunk_reader) = self
            .segments
            .first()
            .map(|segment| Arc::clone(&segment.chunk_reader))
        else {
            let mut results = Vec::new();
            self.extend_head_selector_results(selector, start_ms, end_ms, budget, &mut results)?;
            return Ok(self.merge_query_results_profiled(results));
        };
        let mut results = Vec::new();
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
                segment.plan_generic_cross_segment_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    budget,
                    &mut self.label_cache,
                    &mut self.label_interner,
                )
            };
            let generic_plan = match planned {
                Ok(plan) => plan,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            if generic_plan.payload_requests.is_empty() {
                continue;
            }

            let physical = {
                let segment = &mut self.segments[segment_ordinal];
                let reader = segment.reader;
                let context = segment
                    .facade_context
                    .as_mut()
                    .expect("generic plan requires an open context");
                context
                    .plan_cross_segment_chunk_payload_batch(reader, &generic_plan.payload_requests)
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
                results.extend(execute_cross_segment_generic_reads(
                    &mut self.segments,
                    Arc::clone(&chunk_reader),
                    std::mem::take(&mut group),
                    start_ms,
                    end_ms,
                    budget,
                    &mut self.label_interner,
                    &mut self.projected_label_cache,
                )?);
                group_spans = 0;
                group_bytes = 0;
            }
            group_spans = group_spans.saturating_add(item_spans);
            group_bytes = group_bytes.saturating_add(item_bytes);
            group.push(CrossSegmentGenericRead {
                segment_ordinal,
                generic_plan,
                payload_files,
            });
        }

        results.extend(execute_cross_segment_generic_reads(
            &mut self.segments,
            chunk_reader,
            group,
            start_ms,
            end_ms,
            budget,
            &mut self.label_interner,
            &mut self.projected_label_cache,
        )?);
        if let Some(error) = deferred_error {
            return Err(error);
        }
        self.extend_head_selector_results(selector, start_ms, end_ms, budget, &mut results)?;
        let results = self.merge_query_results_profiled(results);
        Ok(results)
    }

    fn extend_head_selector_results(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        results: &mut Vec<SegmentQueryResult>,
    ) -> io::Result<()> {
        let Some(head_view) = self
            .head_view
            .as_ref()
            .filter(|head| !head.is_empty())
            .cloned()
        else {
            return Ok(());
        };
        let mut head_results =
            head_view.query_selector_with_budget(selector, start_ms, end_ms, budget)?;
        for result in &mut head_results {
            let (labels, labels_complete, metric_name_dropped_series_id) =
                self.prepare_head_labels(result.labels.clone(), selector)?;
            result.labels = labels;
            if !labels_complete {
                result.mark_labels_incomplete(metric_name_dropped_series_id);
            }
        }
        results.extend(head_results);
        Ok(())
    }

    pub(in crate::storage::segment) fn prepare_head_labels(
        &mut self,
        labels: QueryLabels,
        selector: &SegmentSelector,
    ) -> io::Result<(QueryLabels, bool, Option<u64>)> {
        let metric_name_dropped_series_id =
            match (self.label_materialization_policy, selector.label_demand()) {
                (
                    QueryLabelMaterializationPolicy::DemandDriven,
                    QueryLabelDemand::Include {
                        derive_metric_name_dropped_identity: true,
                        ..
                    },
                ) => Some(metric_name_dropped_query_series_id(&labels)),
                _ => None,
            };

        let labels = if self.label_interner.policy() == QueryLabelStoragePolicy::OwnedStrings {
            labels
        } else {
            self.label_interner.try_intern_labels(labels.to_vec())?
        };

        if self.label_materialization_policy == QueryLabelMaterializationPolicy::DemandDriven
            && let QueryLabelDemand::Include { output_names, .. } = selector.label_demand()
        {
            return Ok((
                labels.try_retain_names(output_names)?,
                false,
                metric_name_dropped_series_id,
            ));
        }
        Ok((labels, true, None))
    }

    pub(in crate::storage::segment) fn prewarm_selectors(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<()> {
        self.freeze_query_label_storage_policy();
        if end_ms < start_ms {
            return Ok(());
        }

        for selector in selectors {
            for segment in &mut self.segments {
                if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                    continue;
                }
                segment.prewarm_selector(selector, start_ms, end_ms)?;
            }
        }

        Ok(())
    }

    pub(in crate::storage::segment) fn prefetch_selectors_with_limits(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryDataPrefetchStats> {
        self.freeze_query_label_storage_policy();
        let mut budget = QueryBudget::new(limits);
        let mut prefetch_stats = QueryDataPrefetchStats::default();
        if end_ms < start_ms {
            return Ok(prefetch_stats);
        }

        for selector in selectors {
            for segment in &mut self.segments {
                budget.observe_segment_considered();
                if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                    budget.observe_segment_skipped_by_time();
                    continue;
                }
                segment.prefetch_selector_data_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    &mut budget,
                    &mut prefetch_stats,
                )?;
            }
        }

        prefetch_stats.query_stats = budget.stats();
        Ok(prefetch_stats)
    }
}

fn metric_name_dropped_query_series_id(labels: &QueryLabels) -> u64 {
    let mut hash = XxHash64::default();
    for (name, value) in labels.pairs() {
        if name == METRIC_NAME_LABEL {
            continue;
        }
        hash.update(name.as_bytes());
        hash.update(&[0]);
        hash.update(value.as_bytes());
        hash.update(&[0xff]);
    }
    hash.finish()
}
