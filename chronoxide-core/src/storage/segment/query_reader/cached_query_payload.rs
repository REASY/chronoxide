use super::cached_query_plan::CachedQueryPlan;
use super::*;

impl SegmentReader {
    #[expect(
        clippy::too_many_arguments,
        reason = "payload scheduling keeps range-cache classification and logical accounting explicit"
    )]
    pub(super) fn schedule_cached_query_payloads(
        &self,
        context: &mut SegmentQueryContext,
        segment_ordinal: usize,
        plan: &CachedQueryPlan,
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &SeriesLabelCache,
        cache_call: &mut RangeScalarCacheCall,
    ) -> io::Result<Vec<ChunkPayloadRead>> {
        let mut payload_requests = Vec::new();
        for planned in &plan.series {
            let Some(entries) = plan.chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            if !label_cache.contains_key(&planned.series_id) {
                continue;
            }

            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                let read_len = if typed_scalar_projection(projection, chunk_entry.kind).is_some() {
                    chunk_entry.scalar_projection_read_len()
                } else if chunk_kind_matches_projection(projection, chunk_entry.kind) {
                    chunk_entry.length
                } else {
                    continue;
                };
                let read_len = u64::from(read_len);
                budget.observe_chunk_read(read_len)?;
                payload_requests.push(ChunkPayloadRead {
                    file_id: chunk_entry.file_id,
                    offset: chunk_entry.offset,
                    len: read_len,
                });
            }
        }
        context.observe_chunk_payload_requests(&payload_requests);

        payload_requests.clear();
        for planned in &plan.series {
            let Some(entries) = plan.chunk_entries_by_range.get(&planned.chunk_index) else {
                continue;
            };
            if !label_cache.contains_key(&planned.series_id) {
                continue;
            }

            for chunk_entry in entries.iter() {
                if chunk_entry.max_time_ms < start_ms || chunk_entry.min_time_ms > end_ms {
                    continue;
                }
                let scalar_projection = typed_scalar_projection(projection, chunk_entry.kind)
                    .map(|(projection, _metric_suffix)| projection);
                let read_len = if scalar_projection.is_some() {
                    chunk_entry.scalar_projection_read_len()
                } else if chunk_kind_matches_projection(projection, chunk_entry.kind) {
                    chunk_entry.length
                } else {
                    continue;
                };
                let logical_bytes = u64::from(read_len);
                let Some(key) = scalar_projection.and_then(|projection| {
                    range_scalar_cache_key(segment_ordinal, chunk_entry, projection)
                }) else {
                    cache_call.classify_unsupported(logical_bytes);
                    payload_requests.push(ChunkPayloadRead {
                        file_id: chunk_entry.file_id,
                        offset: chunk_entry.offset,
                        len: logical_bytes,
                    });
                    continue;
                };
                if cache_call.classify_eligible(&key, logical_bytes) == RangeScalarCacheLookup::Miss
                {
                    payload_requests.push(ChunkPayloadRead {
                        file_id: chunk_entry.file_id,
                        offset: chunk_entry.offset,
                        len: logical_bytes,
                    });
                }
            }
        }
        Ok(payload_requests)
    }
}
