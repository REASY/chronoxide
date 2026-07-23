use super::*;

impl SegmentReader {
    #[expect(
        clippy::too_many_arguments,
        reason = "the schema-6 query boundary keeps bounds, budgets, label caches, and range-cache state explicit"
    )]
    pub(in crate::storage::segment) fn query_normalized_with_context(
        &self,
        context: &mut SegmentQueryContext,
        segment_ordinal: usize,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        projected_label_cache: &mut ProjectedLabelCache,
        cache_call: Option<&mut RangeScalarCacheCall>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        let Some(cache_call) = cache_call else {
            let plan = self.plan_generic_cross_segment_with_context(
                context,
                matchers,
                projection,
                start_ms,
                end_ms,
                budget,
                label_cache,
            )?;
            let payloads = context.read_chunk_payload_batch(self, &plan.payload_requests)?;
            return self.decode_generic_cross_segment_plan(
                plan,
                &payloads,
                start_ms,
                end_ms,
                budget,
                None,
                projected_label_cache,
                None,
            );
        };

        let Some(plan) =
            self.plan_cached_query(context, matchers, projection, start_ms, end_ms, budget)?
        else {
            return Ok(Vec::new());
        };
        self.materialize_cached_query_labels(context, &plan, label_cache)?;
        let physical_requests = self.schedule_cached_query_payloads(
            context,
            segment_ordinal,
            &plan,
            projection,
            start_ms,
            end_ms,
            budget,
            label_cache,
            cache_call,
        )?;
        let chunk_payloads = context.read_chunk_payload_batch_physical(self, &physical_requests)?;
        self.decode_cached_query_plan(
            plan,
            &chunk_payloads,
            segment_ordinal,
            projection,
            start_ms,
            end_ms,
            budget,
            label_cache,
            projected_label_cache,
            Some(cache_call),
        )
    }
}
