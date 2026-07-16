use super::*;

impl<'a> SegmentStoreQuerySession<'a> {
    pub(in crate::storage::segment) fn execute_validated_promql_range_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
        cache_call: &mut super::range_scalar_cache::RangeScalarCacheCall,
    ) -> Result<QueryExecution, PromqlQueryError> {
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

        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
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
                execution.results = evaluate_range_function(function, execution.results, end_ms);
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
                    execution.results =
                        evaluate_aggregation(aggregation, execution.results, end_ms);
                    ensure_query_result_labels_complete(&execution.results)?;
                    return Ok(execution);
                }
                let mut execution =
                    self.execute_promql_nested_instant_query(&aggregation.input, end_ms, limits)?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
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

    fn execute_promql_instant_query_with_cache(
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
                execution.results = evaluate_range_function(function, execution.results, end_ms);
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
                    execution.results =
                        evaluate_aggregation(aggregation, execution.results, end_ms);
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
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
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
                    expression,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
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
            execution.results = evaluate_range_function(function, execution.results, end_ms);
        }
        Ok(Some(execution))
    }

    fn execute_promql_float_only_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_float_selectors_from_promql(selector.clone())?;
                let start_ms = instant_vector_start_ms(end_ms);
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
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_float_only_instant_query(
                    &offset.input,
                    shifted_end_ms,
                    limits,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                let mut execution = self.execute_promql_float_only_instant_query(
                    &aggregation.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&absent.input, end_ms, limits)?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::BinaryExpression(_) => Ok(QueryExecution {
                results: Vec::new(),
                stats: QueryStats::default(),
            }),
        }
    }

    fn execute_promql_histogram_fraction(
        &mut self,
        function: &PromqlHistogramFraction,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self.execute_promql_native_histogram_instant_query(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )? {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_fraction(function, series, end_ms));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_fraction(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_histogram_quantile(
        &mut self,
        function: &PromqlHistogramQuantile,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self.execute_promql_native_histogram_instant_query(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )? {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_quantile(function, series, end_ms));
            }
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_quantile(
                    function, series, end_ms,
                ));
            }
        }

        if saw_native_input {
            let mut classic_execution =
                self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
            stats.merge_from(classic_execution.stats);
            stats.check_limits(limits)?;
            classic_execution.results =
                evaluate_histogram_quantile(function, classic_execution.results, end_ms);
            results.extend(classic_execution.results);
            return Ok(QueryExecution {
                results: merge_query_results(results),
                stats,
            });
        }

        let mut execution = self.execute_promql_instant_query_with_cache(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
            false,
        )?;
        execution.results = evaluate_histogram_quantile(function, execution.results, end_ms);
        Ok(execution)
    }

    fn execute_promql_native_histogram_scalar_range_function(
        &mut self,
        function: &PromqlRangeFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        if !matches!(
            function.kind,
            PromqlRangeFunctionKind::Changes | PromqlRangeFunctionKind::Resets
        ) {
            return Ok(None);
        }

        let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some(selector) = native_histogram_selector_from_promql(function.selector.clone())? {
            let (series, native_stats) = self.query_native_histogram_selector_with_limits(
                &selector,
                range_start_ms,
                end_ms,
                limits,
            )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_scalar_range_function(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if let Some(selector) =
            native_exponential_histogram_selector_from_promql(function.selector.clone())?
        {
            let (series, native_stats) = self
                .query_native_exponential_histogram_selector_with_limits(
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_scalar_range_function(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if !saw_native_input {
            return Ok(None);
        }

        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
    }

    fn execute_promql_histogram_scalar_function(
        &mut self,
        function: &PromqlHistogramScalarFunction,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self.execute_promql_native_histogram_instant_query(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )? {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_scalar_function(
                function, series, end_ms,
            ));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_scalar_function(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_native_histogram_scalar_aggregation(
        &mut self,
        aggregation: &PromqlAggregation,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
        allow_label_demand: bool,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        let mut histogram_series = Vec::new();
        let mut exponential_histogram_series = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;
        let terminal_demand = if allow_label_demand {
            native_terminal_aggregation_label_demand(aggregation)
        } else {
            None
        };

        let histogram_execution = if let Some((grouping_names, drops_metric_name)) = terminal_demand
        {
            self.execute_native_histogram_terminal_aggregation_input(
                &aggregation.input,
                grouping_names,
                drops_metric_name,
                end_ms,
                limits,
            )?
        } else {
            self.execute_promql_native_histogram_instant_query(
                &aggregation.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        };
        if let Some((series, native_stats)) = histogram_execution {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                histogram_series = series;
            }
        }
        let exponential_execution =
            if let Some((grouping_names, drops_metric_name)) = terminal_demand {
                self.execute_native_exponential_histogram_terminal_aggregation_input(
                    &aggregation.input,
                    grouping_names,
                    drops_metric_name,
                    end_ms,
                    limits,
                )?
            } else {
                self.execute_promql_native_exponential_histogram_instant_query(
                    &aggregation.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?
            };
        if let Some((series, native_stats)) = exponential_execution {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                exponential_histogram_series = series;
            }
        }

        if !saw_native_input {
            return Ok(None);
        }
        let scalar_execution = if let Some((grouping_names, drops_metric_name)) = terminal_demand {
            self.execute_float_terminal_aggregation_input(
                &aggregation.input,
                grouping_names,
                drops_metric_name,
                end_ms,
                limits,
            )?
        } else {
            self.execute_promql_float_only_instant_query(&aggregation.input, end_ms, limits)?
        };
        stats.merge_from(scalar_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_native_histogram_scalar_aggregation(
            aggregation,
            scalar_execution.results,
            histogram_series,
            exponential_histogram_series,
            end_ms,
        );
        ensure_query_result_labels_complete(&results)?;
        Ok(Some(QueryExecution { results, stats }))
    }

    fn execute_native_histogram_terminal_aggregation_input(
        &mut self,
        input: &PromqlQuery,
        grouping_names: &[String],
        drops_metric_name: bool,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match input {
            PromqlQuery::Vector(selector) => {
                let Some(mut selector) = native_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                if self.label_materialization_policy
                    == QueryLabelMaterializationPolicy::DemandDriven
                {
                    selector = selector
                        .with_terminal_aggregation_label_demand(grouping_names, drops_metric_name);
                }
                self.query_native_histogram_selector_with_limits(
                    &selector,
                    instant_vector_start_ms(end_ms),
                    end_ms,
                    limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(mut selector) =
                    native_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                if self.label_materialization_policy
                    == QueryLabelMaterializationPolicy::DemandDriven
                {
                    selector = selector
                        .with_terminal_aggregation_label_demand(grouping_names, drops_metric_name);
                }
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self.query_native_histogram_selector_with_limits(
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
                Ok(Some((
                    evaluate_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            _ => unreachable!("native terminal label demand validates the child shape"),
        }
    }

    fn execute_native_exponential_histogram_terminal_aggregation_input(
        &mut self,
        input: &PromqlQuery,
        grouping_names: &[String],
        drops_metric_name: bool,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlExponentialHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match input {
            PromqlQuery::Vector(selector) => {
                let Some(mut selector) =
                    native_exponential_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                if self.label_materialization_policy
                    == QueryLabelMaterializationPolicy::DemandDriven
                {
                    selector = selector
                        .with_terminal_aggregation_label_demand(grouping_names, drops_metric_name);
                }
                self.query_native_exponential_histogram_selector_with_limits(
                    &selector,
                    instant_vector_start_ms(end_ms),
                    end_ms,
                    limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(mut selector) =
                    native_exponential_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                if self.label_materialization_policy
                    == QueryLabelMaterializationPolicy::DemandDriven
                {
                    selector = selector
                        .with_terminal_aggregation_label_demand(grouping_names, drops_metric_name);
                }
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self
                    .query_native_exponential_histogram_selector_with_limits(
                        &selector,
                        range_start_ms,
                        end_ms,
                        limits,
                    )?;
                Ok(Some((
                    evaluate_exponential_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            _ => unreachable!("native terminal label demand validates the child shape"),
        }
    }

    fn execute_float_terminal_aggregation_input(
        &mut self,
        input: &PromqlQuery,
        grouping_names: &[String],
        drops_metric_name: bool,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let (mut selectors, start_ms, range_function) = match input {
            PromqlQuery::Vector(selector) => (
                storage_float_selectors_from_promql(selector.clone())?,
                instant_vector_start_ms(end_ms),
                None,
            ),
            PromqlQuery::RangeFunction(function) => (
                storage_float_selectors_from_promql(function.selector.clone())?,
                range_function_start_ms(end_ms, function.range_ms),
                Some(function),
            ),
            _ => unreachable!("native terminal label demand validates the child shape"),
        };
        if self.label_materialization_policy == QueryLabelMaterializationPolicy::DemandDriven {
            selectors = selectors
                .into_iter()
                .map(|selector| {
                    selector
                        .with_terminal_aggregation_label_demand(grouping_names, drops_metric_name)
                })
                .collect();
        }
        let mut execution = self
            .query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
            .map_err(promql_error_from_query_io)?;
        if let Some(function) = range_function {
            execution.results = evaluate_range_function(function, execution.results, end_ms);
        }
        Ok(execution)
    }

    fn execute_promql_native_histogram_binary_bool_comparison(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        if !expression.return_bool
            || !matches!(
                expression.op,
                PromqlBinaryOp::Eq
                    | PromqlBinaryOp::NotEq
                    | PromqlBinaryOp::Gt
                    | PromqlBinaryOp::Gte
                    | PromqlBinaryOp::Lt
                    | PromqlBinaryOp::Lte
            )
        {
            return Ok(None);
        }

        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        let left_histogram = self.execute_promql_native_histogram_instant_query(
            &expression.left,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let right_histogram = self.execute_promql_native_histogram_instant_query(
            &expression.right,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let left_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.left,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let right_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.right,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;

        let left_histogram_series = if let Some((series, query_stats)) = left_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_histogram_series = if let Some((series, query_stats)) = right_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let left_exponential_series = if let Some((series, query_stats)) = left_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_exponential_series = if let Some((series, query_stats)) = right_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };

        results.extend(evaluate_native_histogram_binary_bool_vector_vector(
            expression,
            left_histogram_series.clone(),
            right_histogram_series.clone(),
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_binary_bool_vector_vector(
                expression,
                left_exponential_series.clone(),
                right_exponential_series.clone(),
                end_ms,
            )?,
        );
        results.extend(evaluate_native_histogram_mixed_binary_bool_vector_vector(
            expression,
            left_histogram_series,
            right_exponential_series,
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_mixed_binary_bool_vector_vector(
                expression,
                left_exponential_series,
                right_histogram_series,
                end_ms,
            )?,
        );

        if !saw_native_input {
            return Ok(None);
        }
        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
    }

    fn execute_promql_native_histogram_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<(Vec<PromqlHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) = native_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_histogram_selector_with_limits(
                    &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self.query_native_histogram_selector_with_limits(
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
                Ok(Some((
                    evaluate_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self.execute_promql_native_histogram_instant_query(
                    &aggregation.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Offset(offset) => self.execute_promql_native_histogram_instant_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
                limits,
                cache_call.as_deref_mut(),
            ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_combined_vector_set(
                            expression,
                            left_histogram_series,
                            right_histogram_series,
                            left_exponential_series,
                            right_exponential_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_exponential =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_exponential_histogram_instant_query(
                                &expression.right,
                                end_ms,
                                limits,
                                cache_call.as_deref_mut(),
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
                    stats.check_limits(limits)?;
                    let mut results = evaluate_native_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_exponential_series, right_exponential_stats)) =
                        right_exponential
                    {
                        stats.merge_from(right_exponential_stats);
                        results.extend(evaluate_native_histogram_mixed_binary_vector_vector(
                            expression,
                            left_series,
                            right_exponential_series,
                            end_ms,
                        )?);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand(
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand(
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
        }
    }

    fn execute_promql_native_exponential_histogram_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<(Vec<PromqlExponentialHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_exponential_histogram_selector_with_limits(
                    &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self
                    .query_native_exponential_histogram_selector_with_limits(
                        &selector,
                        range_start_ms,
                        end_ms,
                        limits,
                    )?;
                Ok(Some((
                    evaluate_exponential_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query(
                        &aggregation.input,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_exponential_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Offset(offset) => self
                .execute_promql_native_exponential_histogram_instant_query(
                    &offset.input,
                    offset_eval_time_ms(end_ms, offset.offset_ms),
                    limits,
                    cache_call.as_deref_mut(),
                ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;
                    let left_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_combined_vector_set(
                            expression,
                            left_exponential_series,
                            right_exponential_series,
                            left_histogram_series,
                            right_histogram_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_histogram =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_histogram_instant_query(
                                &expression.right,
                                end_ms,
                                limits,
                                cache_call.as_deref_mut(),
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
                    stats.check_limits(limits)?;
                    let mut results = evaluate_native_exponential_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_histogram_series, right_histogram_stats)) = right_histogram {
                        stats.merge_from(right_histogram_stats);
                        results.extend(
                            evaluate_native_exponential_histogram_mixed_binary_vector_vector(
                                expression,
                                left_series,
                                right_histogram_series,
                                end_ms,
                            )?,
                        );
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand(
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand(
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_exponential_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
        }
    }

    fn execute_promql_scalar_operand(
        &mut self,
        query: &PromqlQuery,
        static_value: Option<f64>,
        end_ms: u64,
        limits: QueryLimits,
        cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<(f64, QueryStats), PromqlQueryError> {
        if let Some(value) = static_value {
            return Ok((value, QueryStats::default()));
        }

        let execution =
            self.execute_promql_instant_query_with_cache(query, end_ms, limits, cache_call, false)?;
        let value = scalar_query_result_value(&execution.results)?;
        Ok((value, execution.stats))
    }

    fn execute_promql_binary_expression(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.execute_promql_binary_expression_with_cache(expression, end_ms, limits, None)
    }

    fn execute_promql_binary_expression_with_cache(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        if binary_operator_is_set(expression.op) {
            if is_scalar_expression(&expression.left) || is_scalar_expression(&expression.right) {
                return Err(PromqlQueryError::Unsupported(
                    "set binary operators require instant-vector operands".to_string(),
                ));
            }

            let left_execution = self.execute_promql_instant_query_with_cache(
                &expression.left,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
                false,
            )?;
            let right_execution = self.execute_promql_instant_query_with_cache(
                &expression.right,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
                false,
            )?;
            let mut stats = left_execution.stats;
            stats.merge_from(right_execution.stats);
            stats.check_limits(limits)?;
            let results = evaluate_binary_vector_set(
                expression,
                left_execution.results,
                right_execution.results,
                end_ms,
            )?;
            return Ok(QueryExecution { results, stats });
        }

        let left_static = scalar_expression_value(&expression.left, end_ms);
        let right_static = scalar_expression_value(&expression.right, end_ms);
        let left_is_scalar = left_static.is_some() || is_scalar_expression(&expression.left);
        let right_is_scalar = right_static.is_some() || is_scalar_expression(&expression.right);

        if !left_is_scalar
            && !right_is_scalar
            && let Some(execution) = self.execute_promql_native_histogram_binary_bool_comparison(
                expression,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            return Ok(execution);
        }

        if left_is_scalar && right_is_scalar {
            let (left, mut stats) = self.execute_promql_scalar_operand(
                &expression.left,
                left_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let (right, right_stats) = self.execute_promql_scalar_operand(
                &expression.right,
                right_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            stats.merge_from(right_stats);
            stats.check_limits(limits)?;
            return Ok(QueryExecution {
                results: evaluate_binary_scalar_scalar(expression.op, left, right, end_ms),
                stats,
            });
        }

        if left_is_scalar {
            let (left, mut stats) = self.execute_promql_scalar_operand(
                &expression.left,
                left_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let mut execution = self.execute_promql_instant_query_with_cache(
                &expression.right,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
                false,
            )?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, left, true, end_ms);
            execution.stats = stats;
            return Ok(execution);
        }

        if right_is_scalar {
            let (right, right_stats) = self.execute_promql_scalar_operand(
                &expression.right,
                right_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let mut execution = self.execute_promql_instant_query_with_cache(
                &expression.left,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
                false,
            )?;
            execution.stats.merge_from(right_stats);
            execution.stats.check_limits(limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, right, false, end_ms);
            return Ok(execution);
        }

        let left_execution = self.execute_promql_instant_query_with_cache(
            &expression.left,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
            false,
        )?;
        let right_execution = self.execute_promql_instant_query_with_cache(
            &expression.right,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
            false,
        )?;
        let mut stats = left_execution.stats;
        stats.merge_from(right_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_binary_vector_vector(
            expression,
            left_execution.results,
            right_execution.results,
            end_ms,
        )?;
        Ok(QueryExecution { results, stats })
    }

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

        Ok(merge_query_results(results))
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
            return Ok(Vec::new());
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
        Ok(merge_query_results(results))
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

pub(in crate::storage::segment) fn histogram_projected_bucket_value(
    metadata: TypedSampleMetadata,
    raw: u64,
    le: &str,
    delta_accumulators: &mut BTreeMap<String, u64>,
    delta_fragments_started: &mut BTreeSet<String>,
) -> (f64, CounterResetHint) {
    if metadata.is_stale() {
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            delta_accumulators.insert(le.to_string(), 0);
            delta_fragments_started.remove(le);
        }
        return (prometheus_stale_nan(), metadata.reset_hint);
    }
    if metadata.temporality == OtlpAggregationTemporality::Delta {
        let accumulator = delta_accumulators.entry(le.to_string()).or_insert(0);
        *accumulator = accumulator.saturating_add(raw);
        let reset_hint = if delta_fragments_started.insert(le.to_string()) {
            CounterResetHint::CounterReset
        } else {
            CounterResetHint::NotCounterReset
        };
        (*accumulator as f64, reset_hint)
    } else {
        (raw as f64, metadata.reset_hint)
    }
}
