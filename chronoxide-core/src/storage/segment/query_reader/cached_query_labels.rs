use super::cached_query_plan::CachedQueryPlan;
use super::*;

impl SegmentReader {
    pub(super) fn materialize_cached_query_labels(
        &self,
        context: &mut SegmentQueryContext,
        plan: &CachedQueryPlan,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<()> {
        let mut direct_label_entries = Vec::new();
        let mut missing_label_refs = Vec::new();
        for planned in &plan.series {
            if !plan
                .chunk_entries_by_range
                .contains_key(&planned.chunk_index)
                || label_cache.contains_key(&planned.series_id)
            {
                continue;
            }

            if let Some(entry) = &planned.entry {
                direct_label_entries.push(entry.as_ref());
            } else {
                missing_label_refs.push(planned.series_ref);
            }
        }
        Self::populate_series_label_cache(&context.symbols, &direct_label_entries, label_cache)?;
        if !missing_label_refs.is_empty() {
            let missing_entries = context.read_series_entries(self, &missing_label_refs)?;
            let missing_entries = missing_entries
                .iter()
                .map(|(_, entry)| entry.as_ref())
                .collect::<Vec<_>>();
            Self::populate_series_label_cache(&context.symbols, &missing_entries, label_cache)?;
        }
        Ok(())
    }
}
