use super::*;

impl SegmentStoreReader {
    pub(super) fn execute_promql_native_histogram_instant_query(
        &self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
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
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
                    )?;
                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
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
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
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
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
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
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
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
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
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

    pub(super) fn execute_promql_native_exponential_histogram_instant_query(
        &self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
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
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                        )?;
                    let left_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
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
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
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
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
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
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
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
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
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

    pub(super) fn execute_promql_native_histogram_instant_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlHistogramSeries>, QueryStats)>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) = native_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_histogram_selector_with_head_with_limits(
                    head, labels, &selector, start_ms, end_ms, limits,
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
                let (series, stats) = self.query_native_histogram_selector_with_head_with_limits(
                    head,
                    labels,
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
                let Some((series, stats)) = self
                    .execute_promql_native_histogram_instant_query_with_head(
                        head,
                        labels,
                        &aggregation.input,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Offset(offset) => self
                .execute_promql_native_histogram_instant_query_with_head(
                    head,
                    labels,
                    &offset.input,
                    offset_eval_time_ms(end_ms, offset.offset_ms),
                    limits,
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

                    let left_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?;
                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
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
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_exponential = if matches!(
                        expression.op,
                        PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq
                    ) {
                        self.execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    } else {
                        None
                    };
                    stats.merge_from(right_stats);
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
                    let (scalar, mut stats) = self.execute_promql_scalar_operand_with_head(
                        head,
                        labels,
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
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

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand_with_head(
                    head,
                    labels,
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_histogram_instant_query_with_head(
                        head,
                        labels,
                        &expression.left,
                        end_ms,
                        limits,
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

    pub(super) fn execute_promql_native_exponential_histogram_instant_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlExponentialHistogramSeries>, QueryStats)>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_exponential_histogram_selector_with_head_with_limits(
                    head, labels, &selector, start_ms, end_ms, limits,
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
                    .query_native_exponential_histogram_selector_with_head_with_limits(
                        head,
                        labels,
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
                    .execute_promql_native_exponential_histogram_instant_query_with_head(
                        head,
                        labels,
                        &aggregation.input,
                        end_ms,
                        limits,
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
                .execute_promql_native_exponential_histogram_instant_query_with_head(
                    head,
                    labels,
                    &offset.input,
                    offset_eval_time_ms(end_ms, offset.offset_ms),
                    limits,
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
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?;
                    let left_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
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
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_histogram =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_histogram_instant_query_with_head(
                                head,
                                labels,
                                &expression.right,
                                end_ms,
                                limits,
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
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
                    let (scalar, mut stats) = self.execute_promql_scalar_operand_with_head(
                        head,
                        labels,
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
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

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand_with_head(
                    head,
                    labels,
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query_with_head(
                        head,
                        labels,
                        &expression.left,
                        end_ms,
                        limits,
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
}
