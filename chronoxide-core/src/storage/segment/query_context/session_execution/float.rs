use super::*;

impl<'a> SegmentStoreQuerySession<'a> {
    pub(super) fn execute_promql_float_only_instant_query(
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
                execution.results = profile_promql_evaluation!(
                    self,
                    evaluate_range_function(function, execution.results, end_ms)
                );
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
                execution.results = profile_promql_evaluation!(
                    self,
                    evaluate_aggregation(aggregation, execution.results, end_ms)
                );
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
    pub(super) fn execute_float_terminal_aggregation_input(
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
            execution.results = profile_promql_evaluation!(
                self,
                evaluate_range_function(function, execution.results, end_ms)
            );
        }
        Ok(execution)
    }
}
