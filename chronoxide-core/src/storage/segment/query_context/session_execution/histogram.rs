use super::*;

impl<'a> SegmentStoreQuerySession<'a> {
    pub(super) fn execute_promql_histogram_fraction(
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
                cache_call,
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
            results: self.merge_query_results_profiled(results),
            stats,
        })
    }

    pub(super) fn execute_promql_histogram_quantile(
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
        )? && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(profile_promql_evaluation!(
                self,
                evaluate_native_histogram_quantile(function, series, end_ms)
            ));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
            && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(profile_promql_evaluation!(
                self,
                evaluate_native_exponential_histogram_quantile(function, series, end_ms)
            ));
        }

        if saw_native_input {
            let mut classic_execution =
                self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
            stats.merge_from(classic_execution.stats);
            stats.check_limits(limits)?;
            classic_execution.results = profile_promql_evaluation!(
                self,
                evaluate_histogram_quantile(function, classic_execution.results, end_ms)
            );
            results.extend(classic_execution.results);
            return Ok(QueryExecution {
                results: self.merge_query_results_profiled(results),
                stats,
            });
        }

        let mut execution = self.execute_promql_instant_query_with_cache(
            &function.input,
            end_ms,
            limits,
            cache_call,
            false,
        )?;
        execution.results = profile_promql_evaluation!(
            self,
            evaluate_histogram_quantile(function, execution.results, end_ms)
        );
        Ok(execution)
    }

    pub(super) fn execute_promql_native_histogram_scalar_range_function(
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
            results: self.merge_query_results_profiled(results),
            stats,
        }))
    }

    pub(super) fn execute_promql_histogram_scalar_function(
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
                cache_call,
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
            results: self.merge_query_results_profiled(results),
            stats,
        })
    }

    pub(super) fn execute_promql_native_histogram_scalar_aggregation(
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
        if let Some((series, native_stats)) = histogram_execution
            && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            histogram_series = series;
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
                    cache_call,
                )?
            };
        if let Some((series, native_stats)) = exponential_execution
            && (!series.is_empty() || native_stats.projected_series > 0)
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            exponential_histogram_series = series;
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
        let results = profile_promql_evaluation!(
            self,
            evaluate_native_histogram_scalar_aggregation(
                aggregation,
                scalar_execution.results,
                histogram_series,
                exponential_histogram_series,
                end_ms,
            )
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
                    profile_promql_evaluation!(
                        self,
                        evaluate_histogram_range_function(function, series, end_ms)
                    ),
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
                    profile_promql_evaluation!(
                        self,
                        evaluate_exponential_histogram_range_function(function, series, end_ms)
                    ),
                    stats,
                )))
            }
            _ => unreachable!("native terminal label demand validates the child shape"),
        }
    }
}
