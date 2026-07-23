use super::*;

impl<'a> SegmentStoreQuerySession<'a> {
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
            cache_call,
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
            results: self.merge_query_results_profiled(results),
            stats,
        }))
    }
    pub(super) fn execute_promql_scalar_operand(
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

    pub(super) fn execute_promql_binary_expression(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.execute_promql_binary_expression_with_cache(expression, end_ms, limits, None)
    }

    pub(super) fn execute_promql_binary_expression_with_cache(
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
            cache_call,
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
}
