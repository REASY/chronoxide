use super::*;

impl<'a> SegmentStoreQuerySession<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "the repeated comparator keeps validated bounds, limits, cache accounting, and finalized telemetry explicit"
    )]
    pub(in crate::storage::segment::query_context) fn execute_repeated_validated_promql_range_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
        cache_call: &mut super::range_scalar_cache::RangeScalarCacheCall,
        summary: &mut RangeExecutionSummary,
    ) -> Result<QueryExecution, PromqlQueryError> {
        summary.effective_mode = RangeExecutionMode::Repeated;
        summary.cache_bypassed = false;
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut eval_time_ms = start_ms;

        loop {
            let mut execution = self.execute_promql_instant_query_with_cache(
                query,
                eval_time_ms,
                limits,
                Some(&mut *cache_call),
                true,
            )?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            summary.evaluation_count = summary.evaluation_count.saturating_add(1);
            results.extend(retimestamp_instant_results(
                std::mem::take(&mut execution.results),
                eval_time_ms,
            ));

            let Some(next_eval_time_ms) = eval_time_ms.checked_add(step_ms) else {
                break;
            };
            if next_eval_time_ms > end_ms {
                break;
            }
            eval_time_ms = next_eval_time_ms;
        }

        let results = self.merge_query_results_profiled(results);
        Ok(QueryExecution { results, stats })
    }

    pub(in crate::storage::segment) fn execute_promql_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution =
                    self.execute_promql_nested_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_nested_instant_query(
                    &offset.input,
                    shifted_end_ms,
                    limits,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_nested_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_nested_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) = self
                    .execute_promql_native_histogram_scalar_range_function(
                        function, end_ms, limits,
                    )?
                {
                    return Ok(execution);
                }
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, read_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = profile_promql_evaluation!(
                    self,
                    evaluate_range_function(function, execution.results, end_ms)
                );
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation(
                            aggregation,
                            end_ms,
                            limits,
                            None,
                            true,
                        )?
                {
                    return Ok(execution);
                }
                if let Some(mut execution) = self.execute_terminal_aggregation_input_with_demand(
                    aggregation,
                    end_ms,
                    limits,
                    None,
                )? {
                    execution.results = profile_promql_evaluation!(
                        self,
                        evaluate_aggregation(aggregation, execution.results, end_ms)
                    );
                    ensure_query_result_labels_complete(&execution.results)?;
                    return Ok(execution);
                }
                let mut execution =
                    self.execute_promql_nested_instant_query(&aggregation.input, end_ms, limits)?;
                execution.results = profile_promql_evaluation!(
                    self,
                    evaluate_aggregation(aggregation, execution.results, end_ms)
                );
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_nested_instant_query(&absent.input, end_ms, limits)?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution =
                    self.execute_promql_nested_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.execute_promql_histogram_fraction(function, end_ms, limits, None)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.execute_promql_histogram_scalar_function(function, end_ms, limits, None)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.execute_promql_histogram_quantile(function, end_ms, limits, None)
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.execute_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    pub(in crate::storage::segment) fn execute_promql_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.execute_promql_instant_query_with_cache(query, end_ms, limits, None, true)
    }

    fn execute_promql_nested_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.execute_promql_instant_query_with_cache(query, end_ms, limits, None, false)
    }

    pub(super) fn execute_promql_instant_query_with_cache(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
        allow_label_demand: bool,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_selectors_with_limits_with_cache(
                    &selectors,
                    start_ms,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )
                .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                    false,
                )?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &offset.input,
                    shifted_end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                    false,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                    false,
                )?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                    false,
                )?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) = self
                    .execute_promql_native_histogram_scalar_range_function(
                        function, end_ms, limits,
                    )?
                {
                    return Ok(execution);
                }
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        read_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = profile_promql_evaluation!(
                    self,
                    evaluate_range_function(function, execution.results, end_ms)
                );
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation(
                            aggregation,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                            allow_label_demand,
                        )?
                {
                    return Ok(execution);
                }
                if allow_label_demand
                    && let Some(mut execution) = self
                        .execute_terminal_aggregation_input_with_demand(
                            aggregation,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                {
                    execution.results = profile_promql_evaluation!(
                        self,
                        evaluate_aggregation(aggregation, execution.results, end_ms)
                    );
                    ensure_query_result_labels_complete(&execution.results)?;
                    return Ok(execution);
                }
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &aggregation.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                    false,
                )?;
                execution.results = profile_promql_evaluation!(
                    self,
                    evaluate_aggregation(aggregation, execution.results, end_ms)
                );
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &absent.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                    false,
                )?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                    false,
                )?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => self.execute_promql_histogram_fraction(
                function,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            ),
            PromqlQuery::HistogramScalarFunction(function) => self
                .execute_promql_histogram_scalar_function(
                    function,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                ),
            PromqlQuery::HistogramQuantile(function) => self.execute_promql_histogram_quantile(
                function,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            ),
            PromqlQuery::BinaryExpression(expression) => self
                .execute_promql_binary_expression_with_cache(
                    expression, end_ms, limits, cache_call,
                ),
        }
    }

    fn execute_terminal_aggregation_input_with_demand(
        &mut self,
        aggregation: &PromqlAggregation,
        end_ms: u64,
        limits: QueryLimits,
        cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        let Some(grouping_names) = terminal_aggregation_grouping_names(aggregation) else {
            return Ok(None);
        };
        let (mut selectors, read_start_ms, range_function) = match aggregation.input.as_ref() {
            PromqlQuery::Vector(selector) => (
                storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?,
                instant_vector_start_ms(end_ms),
                None,
            ),
            PromqlQuery::RangeFunction(function)
                if matches!(
                    function.kind,
                    PromqlRangeFunctionKind::Rate | PromqlRangeFunctionKind::Increase
                ) =>
            {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                (selectors, read_start_ms, Some(function))
            }
            _ => return Ok(None),
        };
        if selectors
            .iter()
            .any(|selector| !matches!(selector.projection(), SegmentProjection::AllPromql { .. }))
        {
            return Ok(None);
        }
        if self.label_materialization_policy == QueryLabelMaterializationPolicy::DemandDriven {
            selectors = selectors
                .into_iter()
                .map(|selector| {
                    selector.with_terminal_aggregation_label_demand(
                        grouping_names,
                        range_function.is_some(),
                    )
                })
                .collect();
        }
        let mut execution = self
            .query_selectors_with_limits_with_cache(
                &selectors,
                read_start_ms,
                end_ms,
                limits,
                cache_call,
            )
            .map_err(promql_error_from_query_io)?;
        if let Some(function) = range_function {
            execution.results = profile_promql_evaluation!(
                self,
                evaluate_range_function(function, execution.results, end_ms)
            );
        }
        Ok(Some(execution))
    }
}
