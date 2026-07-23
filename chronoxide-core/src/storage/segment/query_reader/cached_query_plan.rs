use super::*;

pub(super) struct CachedQueryPlan {
    pub(super) projected_label_filter: Option<Vec<CompiledLabelMatcher>>,
    pub(super) series: Vec<CachedQuerySeries>,
    pub(super) chunk_entries_by_range: HashMap<ChunkIndexRange, Arc<Vec<ChunkIndexEntry>>>,
}

pub(super) struct CachedQuerySeries {
    pub(super) series_ref: u32,
    pub(super) series_id: u64,
    pub(super) chunk_index: ChunkIndexRange,
    pub(super) entry: Option<Arc<SeriesEntry>>,
}

impl SegmentReader {
    pub(super) fn plan_cached_query(
        &self,
        context: &mut SegmentQueryContext,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Option<CachedQueryPlan>> {
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => Some(compile_label_matchers(matchers)?),
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::NativeHistogram
            | SegmentProjection::NativeExponentialHistogram
            | SegmentProjection::SummaryQuantile { .. } => None,
        };

        let candidate_refs = match self
            .selector_candidate_refs(context, matchers, projection, start_ms, end_ms, budget)?
        {
            Ok(candidate_refs) => candidate_refs,
            Err(SegmentPruneReason::MissingEquality) => {
                budget.observe_segment_skipped_by_missing_equality();
                return Ok(None);
            }
            Err(SegmentPruneReason::MatcherTimeRange) => {
                budget.observe_segment_skipped_by_matcher_time_range();
                return Ok(None);
            }
        };
        if candidate_refs.is_empty() {
            return Ok(None);
        }

        let mut series = Vec::new();
        if matches!(projection, SegmentProjection::AllPromql { .. }) {
            for (series_ref, entry) in context.read_series_entries(self, &candidate_refs)? {
                if !series_kind_mask_matches_projection(projection, entry.kind_mask) {
                    continue;
                }
                budget.observe_matched_series(entry.series_id)?;
                series.push(CachedQuerySeries {
                    series_ref,
                    series_id: entry.series_id,
                    chunk_index: entry.chunk_index,
                    entry: Some(entry),
                });
            }
        } else {
            for (series_ref, metadata) in
                context.read_series_metadata_entries(self, &candidate_refs)?
            {
                if !series_kind_mask_matches_projection(projection, metadata.kind_mask) {
                    continue;
                }
                budget.observe_matched_series(metadata.series_id)?;
                series.push(CachedQuerySeries {
                    series_ref,
                    series_id: metadata.series_id,
                    chunk_index: metadata.chunk_index,
                    entry: None,
                });
            }
        }

        let chunk_ranges = series
            .iter()
            .map(|entry| entry.chunk_index)
            .collect::<Vec<_>>();
        let chunk_entries_by_range = context.read_chunk_entry_ranges(self, &chunk_ranges)?;

        Ok(Some(CachedQueryPlan {
            projected_label_filter,
            series,
            chunk_entries_by_range,
        }))
    }
}
