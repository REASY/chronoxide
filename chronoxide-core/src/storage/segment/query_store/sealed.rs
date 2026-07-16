use super::*;

impl SegmentStoreReader {
    pub(in crate::storage::segment) fn execute_promql_query(
        &self,
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
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution =
                    self.execute_promql_instant_query(&offset.input, shifted_end_ms, limits)?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
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
                        )?
                {
                    return Ok(execution);
                }
                let mut execution =
                    self.execute_promql_instant_query(&aggregation.input, end_ms, limits)?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_instant_query(&absent.input, end_ms, limits)?;
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
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.execute_promql_histogram_fraction(function, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.execute_promql_histogram_scalar_function(function, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.execute_promql_histogram_quantile(function, end_ms, limits)
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.execute_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    pub(super) fn execute_promql_instant_query(
        &self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
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
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution =
                    self.execute_promql_instant_query(&offset.input, shifted_end_ms, limits)?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
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
                        )?
                {
                    return Ok(execution);
                }
                let mut execution =
                    self.execute_promql_instant_query(&aggregation.input, end_ms, limits)?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_instant_query(&absent.input, end_ms, limits)?;
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
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.execute_promql_histogram_fraction(function, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.execute_promql_histogram_scalar_function(function, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.execute_promql_histogram_quantile(function, end_ms, limits)
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.execute_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    pub(super) fn execute_promql_float_only_instant_query(
        &self,
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

    pub(super) fn execute_promql_histogram_fraction(
        &self,
        function: &PromqlHistogramFraction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&function.input, end_ms, limits)?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_fraction(function, series, end_ms));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
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

    pub(super) fn execute_promql_histogram_quantile(
        &self,
        function: &PromqlHistogramQuantile,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&function.input, end_ms, limits)?
            && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_quantile(function, series, end_ms));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
            )?
            && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_quantile(
                function, series, end_ms,
            ));
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

        let mut execution = self.execute_promql_instant_query(&function.input, end_ms, limits)?;
        execution.results = evaluate_histogram_quantile(function, execution.results, end_ms);
        Ok(execution)
    }

    pub(super) fn execute_promql_native_histogram_scalar_range_function(
        &self,
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

    pub(super) fn execute_promql_native_histogram_scalar_range_function_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        function: &PromqlRangeFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
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
            let (series, native_stats) = self
                .query_native_histogram_selector_with_head_with_limits(
                    head,
                    labels,
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
                .query_native_exponential_histogram_selector_with_head_with_limits(
                    head,
                    labels,
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

    pub(super) fn execute_promql_histogram_scalar_function(
        &self,
        function: &PromqlHistogramScalarFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&function.input, end_ms, limits)?
        {
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

    pub(super) fn execute_promql_native_histogram_scalar_aggregation(
        &self,
        aggregation: &PromqlAggregation,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        let mut histogram_series = Vec::new();
        let mut exponential_histogram_series = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&aggregation.input, end_ms, limits)?
            && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            histogram_series = series;
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &aggregation.input,
                end_ms,
                limits,
            )?
            && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            exponential_histogram_series = series;
        }

        if !saw_native_input {
            return Ok(None);
        }
        let scalar_execution =
            self.execute_promql_float_only_instant_query(&aggregation.input, end_ms, limits)?;
        stats.merge_from(scalar_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_native_histogram_scalar_aggregation(
            aggregation,
            scalar_execution.results,
            histogram_series,
            exponential_histogram_series,
            end_ms,
        );
        Ok(Some(QueryExecution { results, stats }))
    }

    pub(super) fn execute_promql_native_histogram_binary_bool_comparison(
        &self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
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

        let left_histogram =
            self.execute_promql_native_histogram_instant_query(&expression.left, end_ms, limits)?;
        let right_histogram =
            self.execute_promql_native_histogram_instant_query(&expression.right, end_ms, limits)?;
        let left_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.left,
            end_ms,
            limits,
        )?;
        let right_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.right,
            end_ms,
            limits,
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

    pub(super) fn execute_promql_scalar_operand(
        &self,
        query: &PromqlQuery,
        static_value: Option<f64>,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(f64, QueryStats), PromqlQueryError> {
        if let Some(value) = static_value {
            return Ok((value, QueryStats::default()));
        }

        let execution = self.execute_promql_instant_query(query, end_ms, limits)?;
        let value = scalar_query_result_value(&execution.results)?;
        Ok((value, execution.stats))
    }

    pub(super) fn execute_promql_binary_expression(
        &self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        if binary_operator_is_set(expression.op) {
            if is_scalar_expression(&expression.left) || is_scalar_expression(&expression.right) {
                return Err(PromqlQueryError::Unsupported(
                    "set binary operators require instant-vector operands".to_string(),
                ));
            }

            let left_execution =
                self.execute_promql_instant_query(&expression.left, end_ms, limits)?;
            let right_execution =
                self.execute_promql_instant_query(&expression.right, end_ms, limits)?;
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
                expression, end_ms, limits,
            )?
        {
            return Ok(execution);
        }

        if left_is_scalar && right_is_scalar {
            let (left, mut stats) =
                self.execute_promql_scalar_operand(&expression.left, left_static, end_ms, limits)?;
            let (right, right_stats) = self.execute_promql_scalar_operand(
                &expression.right,
                right_static,
                end_ms,
                limits,
            )?;
            stats.merge_from(right_stats);
            stats.check_limits(limits)?;
            return Ok(QueryExecution {
                results: evaluate_binary_scalar_scalar(expression.op, left, right, end_ms),
                stats,
            });
        }

        if left_is_scalar {
            let (left, mut stats) =
                self.execute_promql_scalar_operand(&expression.left, left_static, end_ms, limits)?;
            let mut execution =
                self.execute_promql_instant_query(&expression.right, end_ms, limits)?;
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
            )?;
            let mut execution =
                self.execute_promql_instant_query(&expression.left, end_ms, limits)?;
            execution.stats.merge_from(right_stats);
            execution.stats.check_limits(limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, right, false, end_ms);
            return Ok(execution);
        }

        let left_execution = self.execute_promql_instant_query(&expression.left, end_ms, limits)?;
        let right_execution =
            self.execute_promql_instant_query(&expression.right, end_ms, limits)?;
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
}
