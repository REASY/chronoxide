use super::*;

struct OnePassScalarRangePlan<'q> {
    aggregation: &'q PromqlAggregation,
    function: &'q PromqlRangeFunction,
    selectors: Vec<SegmentSelector>,
}

struct ScalarRangeCursor {
    source: SegmentQueryResult,
    left: usize,
    right: usize,
}

impl ScalarRangeCursor {
    fn new(source: SegmentQueryResult) -> Self {
        Self {
            source,
            left: 0,
            right: 0,
        }
    }

    fn window(
        &mut self,
        range_start_ms: u64,
        eval_time_ms: u64,
        include_start: bool,
    ) -> SegmentQueryResult {
        while self.left < self.source.samples.len()
            && if include_start {
                self.source.samples[self.left].0 < range_start_ms
            } else {
                self.source.samples[self.left].0 <= range_start_ms
            }
        {
            self.left += 1;
        }
        self.right = self.right.max(self.left);
        while self.right < self.source.samples.len()
            && self.source.samples[self.right].0 <= eval_time_ms
        {
            self.right += 1;
        }

        let original_len = self.source.samples.len();
        let mut result = SegmentQueryResult {
            series_id: self.source.series_id,
            labels: self.source.labels.clone(),
            samples: self.source.samples[self.left..self.right].to_vec(),
            counter_reset_hints: aligned_slice(
                &self.source.counter_reset_hints,
                original_len,
                self.left,
                self.right,
            ),
            sample_start_times: aligned_slice(
                &self.source.sample_start_times,
                original_len,
                self.left,
                self.right,
            ),
            sample_temporalities: aligned_slice(
                &self.source.sample_temporalities,
                original_len,
                self.left,
                self.right,
            ),
            temporality: self.source.temporality,
            labels_complete: self.source.labels_complete,
            metric_name_dropped_series_id: self.source.metric_name_dropped_series_id,
            delta_projection_intervals: aligned_slice(
                &self.source.delta_projection_intervals,
                original_len,
                self.left,
                self.right,
            ),
        };
        if result.samples.is_empty() {
            result.counter_reset_hints.clear();
            result.sample_start_times.clear();
            result.sample_temporalities.clear();
            result.temporality = QueryResultTemporality::Unknown;
            result.delta_projection_intervals.clear();
        } else if !result.sample_temporalities.is_empty() {
            result.recompute_temporality_from_samples();
        }
        result
    }
}

fn aligned_slice<T: Clone>(values: &[T], original_len: usize, start: usize, end: usize) -> Vec<T> {
    if values.len() == original_len {
        values[start..end].to_vec()
    } else {
        Vec::new()
    }
}

impl<'a> SegmentStoreQuerySession<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "the internal range dispatcher keeps validated bounds, limits, cache accounting, and finalized telemetry explicit"
    )]
    pub(in crate::storage::segment) fn execute_validated_promql_range_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
        cache_call: &mut super::range_scalar_cache::RangeScalarCacheCall,
        summary: &mut RangeExecutionSummary,
    ) -> Result<QueryExecution, PromqlQueryError> {
        if self.range_execution_mode == RangeExecutionMode::Repeated {
            return self.execute_repeated_validated_promql_range_query(
                query, start_ms, end_ms, step_ms, limits, cache_call, summary,
            );
        }

        if limits != QueryLimits::unlimited() {
            summary.fallback_reason = Some(RangeExecutionFallbackReason::FiniteLimits);
            return self.execute_repeated_validated_promql_range_query(
                query, start_ms, end_ms, step_ms, limits, cache_call, summary,
            );
        }

        let plan = match self.one_pass_scalar_range_plan(query, step_ms)? {
            Ok(plan) => plan,
            Err(reason) => {
                summary.fallback_reason = Some(reason);
                return self.execute_repeated_validated_promql_range_query(
                    query, start_ms, end_ms, step_ms, limits, cache_call, summary,
                );
            }
        };
        self.execute_one_pass_assume_scalar_range(plan, start_ms, end_ms, step_ms, summary)
    }

    fn one_pass_scalar_range_plan<'q>(
        &self,
        query: &'q PromqlQuery,
        step_ms: u64,
    ) -> Result<Result<OnePassScalarRangePlan<'q>, RangeExecutionFallbackReason>, PromqlQueryError>
    {
        let PromqlQuery::Aggregation(aggregation) = query else {
            return Ok(Err(RangeExecutionFallbackReason::UnsupportedRootExpression));
        };
        if !matches!(
            aggregation.op,
            PromqlAggregationOp::Sum | PromqlAggregationOp::Count
        ) {
            return Ok(Err(RangeExecutionFallbackReason::UnsupportedAggregation));
        }
        if !matches!(aggregation.grouping, PromqlAggregationGrouping::By(_)) {
            return Ok(Err(RangeExecutionFallbackReason::UnsupportedGrouping));
        }
        let PromqlQuery::RangeFunction(function) = aggregation.input.as_ref() else {
            return Ok(Err(RangeExecutionFallbackReason::UnsupportedRootExpression));
        };
        if function.kind != PromqlRangeFunctionKind::Rate {
            return Ok(Err(RangeExecutionFallbackReason::UnsupportedRangeFunction));
        }
        if step_ms > function.range_ms {
            return Ok(Err(RangeExecutionFallbackReason::StepExceedsWindow));
        }
        let Some(metric_name) = function.selector.metric_name.as_deref() else {
            return Ok(Err(RangeExecutionFallbackReason::MissingDirectMetricName));
        };
        if function
            .selector
            .matchers
            .iter()
            .any(|matcher| matcher.name == METRIC_NAME_LABEL)
        {
            return Ok(Err(RangeExecutionFallbackReason::MissingDirectMetricName));
        }
        if PROMQL_PROJECTION_SUFFIXES
            .iter()
            .any(|suffix| metric_name.ends_with(suffix))
        {
            return Ok(Err(RangeExecutionFallbackReason::ProjectionLikeMetricName));
        }

        let mut selectors = storage_selectors_from_promql_with_projection_config(
            function.selector.clone(),
            &self.query_projection_config,
        )?;
        if selectors.len() != 1
            || selectors.iter().any(|selector| {
                !matches!(selector.projection(), SegmentProjection::AllPromql { .. })
            })
        {
            return Ok(Err(RangeExecutionFallbackReason::UnsupportedProjection));
        }
        if self.label_materialization_policy == QueryLabelMaterializationPolicy::DemandDriven {
            let grouping_names = terminal_aggregation_grouping_names(aggregation)
                .expect("sum/count by grouping has a terminal label demand");
            selectors = selectors
                .into_iter()
                .map(|selector| {
                    selector.with_terminal_aggregation_label_demand(grouping_names, true)
                })
                .collect();
        }

        Ok(Ok(OnePassScalarRangePlan {
            aggregation,
            function,
            selectors,
        }))
    }

    fn execute_one_pass_assume_scalar_range(
        &mut self,
        plan: OnePassScalarRangePlan<'_>,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        summary: &mut RangeExecutionSummary,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let union_range_start_ms = range_function_start_ms(start_ms, plan.function.range_ms);
        let union_start_ms =
            range_selector_read_start_ms(&plan.selectors, union_range_start_ms, end_ms);
        summary.effective_mode = RangeExecutionMode::OnePassAssumeScalar;
        summary.cache_bypassed = true;
        summary.union_start_ms = Some(union_start_ms);
        summary.union_end_ms = Some(end_ms);

        let execution = self
            .query_selectors_with_limits(
                &plan.selectors,
                union_start_ms,
                end_ms,
                QueryLimits::unlimited(),
            )
            .map_err(promql_error_from_query_io)?;
        summary.source_series = u64::try_from(execution.results.len()).unwrap_or(u64::MAX);
        summary.source_samples = execution.results.iter().fold(0u64, |count, result| {
            count.saturating_add(u64::try_from(result.samples.len()).unwrap_or(u64::MAX))
        });
        summary.estimated_retained_bytes_peak =
            estimated_result_vector_bytes(&execution.results, execution.results.capacity());

        let typed_source_observed = execution.stats.typed_scalar_chunks_decoded != 0
            || execution.stats.typed_full_chunks_decoded != 0
            || execution
                .results
                .iter()
                .any(|result| result.temporality != QueryResultTemporality::Unknown)
            || execution
                .results
                .iter()
                .any(|result| !result.delta_projection_intervals.is_empty());
        if typed_source_observed {
            summary.terminal_reason =
                Some(RangeExecutionTerminalReason::TypedSourceObservedAfterDecode);
            summary.retained_bytes_after_finalize = 0;
            return Err(PromqlQueryError::Unsupported(
                "one_pass_assume_scalar observed typed source chunks after union decode"
                    .to_string(),
            ));
        }

        let stats = execution.stats;
        let mut cursors = execution
            .results
            .into_iter()
            .map(ScalarRangeCursor::new)
            .collect::<Vec<_>>();
        let source_retained_bytes = estimated_cursor_vector_bytes(&cursors, cursors.capacity());
        summary.estimated_retained_bytes_peak = summary
            .estimated_retained_bytes_peak
            .max(source_retained_bytes);
        let mut results = Vec::new();
        let mut eval_time_ms = start_ms;

        loop {
            let timer = QueryStageTimer::start(self.query_instrumentation_mode);
            let range_start_ms = range_function_start_ms(eval_time_ms, plan.function.range_ms);
            let include_start = plan.function.range_ms.saturating_sub(eval_time_ms) > 0;
            let window_results = cursors
                .iter_mut()
                .map(|cursor| cursor.window(range_start_ms, eval_time_ms, include_start))
                .collect::<Vec<_>>();
            summary.estimated_retained_bytes_peak =
                summary
                    .estimated_retained_bytes_peak
                    .max(
                        source_retained_bytes.saturating_add(estimated_result_vector_bytes(
                            &window_results,
                            window_results.capacity(),
                        )),
                    );

            let ranged = evaluate_range_function(plan.function, window_results, eval_time_ms);
            let evaluated = evaluate_aggregation(plan.aggregation, ranged, eval_time_ms);
            self.query_stages.promql_grouping_evaluation = self
                .query_stages
                .promql_grouping_evaluation
                .saturating_add(timer.elapsed());
            summary.evaluation_count = summary.evaluation_count.saturating_add(1);
            results.extend(evaluated);

            let Some(next_eval_time_ms) = eval_time_ms.checked_add(step_ms) else {
                break;
            };
            if next_eval_time_ms > end_ms {
                break;
            }
            eval_time_ms = next_eval_time_ms;
        }

        drop(cursors);
        summary.retained_bytes_after_finalize = 0;
        Ok(QueryExecution {
            results: self.merge_query_results_profiled(results),
            stats,
        })
    }
}

fn estimated_result_vector_bytes(results: &[SegmentQueryResult], outer_capacity: usize) -> u64 {
    let outer = allocation_bytes::<SegmentQueryResult>(outer_capacity);
    results.iter().fold(outer, |bytes, result| {
        bytes.saturating_add(estimated_result_inner_vector_bytes(result))
    })
}

fn estimated_cursor_vector_bytes(cursors: &[ScalarRangeCursor], outer_capacity: usize) -> u64 {
    let outer = allocation_bytes::<ScalarRangeCursor>(outer_capacity);
    cursors.iter().fold(outer, |bytes, cursor| {
        bytes.saturating_add(estimated_result_inner_vector_bytes(&cursor.source))
    })
}

fn estimated_result_inner_vector_bytes(result: &SegmentQueryResult) -> u64 {
    allocation_bytes::<(u64, f64)>(result.samples.capacity())
        .saturating_add(allocation_bytes::<CounterResetHint>(
            result.counter_reset_hints.capacity(),
        ))
        .saturating_add(allocation_bytes::<Option<u64>>(
            result.sample_start_times.capacity(),
        ))
        .saturating_add(allocation_bytes::<QueryResultTemporality>(
            result.sample_temporalities.capacity(),
        ))
        .saturating_add(allocation_bytes::<Option<DeltaProjectionInterval>>(
            result.delta_projection_intervals.capacity(),
        ))
}

fn allocation_bytes<T>(capacity: usize) -> u64 {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_cursor_preserves_left_open_and_pre_epoch_zero_boundaries() {
        let mut source = SegmentQueryResult::with_samples(
            7,
            vec![(METRIC_NAME_LABEL.to_string(), "counter".to_string())],
            vec![(0, 0.0), (1_000, 1.0), (2_000, 2.0), (3_000, 3.0)],
        );
        source.counter_reset_hints = vec![
            CounterResetHint::Unknown,
            CounterResetHint::NotCounterReset,
            CounterResetHint::CounterReset,
            CounterResetHint::NotCounterReset,
        ];
        source.sample_temporalities = vec![
            QueryResultTemporality::Cumulative,
            QueryResultTemporality::Cumulative,
            QueryResultTemporality::Delta,
            QueryResultTemporality::Delta,
        ];
        source.temporality = QueryResultTemporality::Mixed;
        let mut cursor = ScalarRangeCursor::new(source);

        let pre_epoch = cursor.window(0, 1_000, true);
        assert_eq!(pre_epoch.samples, vec![(0, 0.0), (1_000, 1.0)]);
        assert_eq!(
            pre_epoch.counter_reset_hints,
            vec![CounterResetHint::Unknown, CounterResetHint::NotCounterReset]
        );
        assert_eq!(
            pre_epoch.sample_temporalities,
            vec![
                QueryResultTemporality::Cumulative,
                QueryResultTemporality::Cumulative
            ]
        );
        assert_eq!(pre_epoch.temporality, QueryResultTemporality::Cumulative);

        let epoch_boundary = cursor.window(0, 2_000, false);
        assert_eq!(epoch_boundary.samples, vec![(1_000, 1.0), (2_000, 2.0)]);
        assert_eq!(
            epoch_boundary.counter_reset_hints,
            vec![
                CounterResetHint::NotCounterReset,
                CounterResetHint::CounterReset
            ]
        );
        assert_eq!(
            epoch_boundary.sample_temporalities,
            vec![
                QueryResultTemporality::Cumulative,
                QueryResultTemporality::Delta
            ]
        );
        assert_eq!(epoch_boundary.temporality, QueryResultTemporality::Mixed);

        let advanced = cursor.window(1_000, 3_000, false);
        assert_eq!(advanced.samples, vec![(2_000, 2.0), (3_000, 3.0)]);
        assert_eq!(
            advanced.counter_reset_hints,
            vec![
                CounterResetHint::CounterReset,
                CounterResetHint::NotCounterReset
            ]
        );
        assert_eq!(
            advanced.sample_temporalities,
            vec![QueryResultTemporality::Delta, QueryResultTemporality::Delta]
        );
        assert_eq!(advanced.temporality, QueryResultTemporality::Delta);

        let empty = cursor.window(4_000, 5_000, false);
        assert!(empty.samples.is_empty());
        assert!(empty.sample_temporalities.is_empty());
        assert_eq!(empty.temporality, QueryResultTemporality::Unknown);
    }

    #[test]
    fn retained_byte_estimate_charges_sample_temporality_capacity() {
        let mut result = SegmentQueryResult::with_samples(
            7,
            Vec::new(),
            vec![(1_000, 1.0), (2_000, 2.0), (3_000, 3.0)],
        );
        result.sample_temporalities = vec![QueryResultTemporality::Delta; 3];
        let temporality_charge =
            allocation_bytes::<QueryResultTemporality>(result.sample_temporalities.capacity());
        let with_temporality = estimated_result_inner_vector_bytes(&result);
        result.sample_temporalities = Vec::new();

        assert_eq!(
            with_temporality,
            estimated_result_inner_vector_bytes(&result).saturating_add(temporality_charge)
        );
    }
}
