use super::*;

impl SegmentStoreReader {
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
}
