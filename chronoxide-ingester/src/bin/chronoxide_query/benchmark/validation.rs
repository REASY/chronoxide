use super::*;

pub(in super::super) fn validate_query_label_storage_stats(
    stats: QueryLabelStorageStats,
) -> io::Result<()> {
    if stats.atom_hits.checked_add(stats.atom_misses) != Some(stats.atom_lookups) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "query-label atom counters do not reconcile: lookups={} hits={} misses={}",
                stats.atom_lookups, stats.atom_hits, stats.atom_misses,
            ),
        ));
    }
    if stats
        .compact_atom_hits
        .checked_add(stats.compact_atom_misses)
        != Some(stats.compact_atom_lookups)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compact query-label atom counters do not reconcile: lookups={} hits={} misses={}",
                stats.compact_atom_lookups, stats.compact_atom_hits, stats.compact_atom_misses,
            ),
        ));
    }
    if stats
        .compact_source_symbol_translation_hits
        .checked_add(stats.compact_source_symbol_translation_misses)
        != Some(stats.compact_source_symbol_translations)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compact source-symbol translation counters do not reconcile: translations={} hits={} misses={}",
                stats.compact_source_symbol_translations,
                stats.compact_source_symbol_translation_hits,
                stats.compact_source_symbol_translation_misses,
            ),
        ));
    }
    let categorized_retained_bytes = stats
        .compact_atom_bytes
        .checked_add(stats.compact_pair_bytes)
        .and_then(|bytes| bytes.checked_add(stats.compact_hash_directory_bytes))
        .and_then(|bytes| bytes.checked_add(stats.compact_translation_bytes))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "compact query-label retained-byte categories overflow u64",
            )
        })?;
    if stats.compact_retained_bytes != categorized_retained_bytes
        || stats.compact_arena_current_bytes != stats.compact_retained_bytes
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compact query-label retained bytes do not reconcile: current={} retained={} categorized={}",
                stats.compact_arena_current_bytes,
                stats.compact_retained_bytes,
                categorized_retained_bytes,
            ),
        ));
    }
    if stats.compact_arena_current_bytes > stats.compact_arena_peak_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compact query-label current charge exceeds peak: current={} peak={}",
                stats.compact_arena_current_bytes, stats.compact_arena_peak_bytes,
            ),
        ));
    }
    if stats.compact_arena_current_bytes > stats.compact_arena_budget_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compact query-label current charge exceeds budget: current={} budget={}",
                stats.compact_arena_current_bytes, stats.compact_arena_budget_bytes,
            ),
        ));
    }
    if stats.compact_arena_peak_bytes > stats.compact_arena_budget_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "compact query-label peak charge exceeds budget: peak={} budget={}",
                stats.compact_arena_peak_bytes, stats.compact_arena_budget_bytes,
            ),
        ));
    }
    Ok(())
}

pub(in super::super) fn validate_query_stage_accounting(
    mode: QueryInstrumentationArg,
    query: &str,
    query_duration: Duration,
    stages: QueryStageProfile,
) -> io::Result<()> {
    let exclusive_total = stages.total_exclusive();
    match mode {
        QueryInstrumentationArg::Off if exclusive_total.is_zero() => Ok(()),
        QueryInstrumentationArg::Off => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "query-stage attribution is nonzero while instrumentation is off for {query:?}: {} ns",
                exclusive_total.as_nanos(),
            ),
        )),
        QueryInstrumentationArg::Detailed if exclusive_total <= query_duration => Ok(()),
        QueryInstrumentationArg::Detailed => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "detailed query-stage attribution exceeds timed query wall for {query:?}: {} ns > {} ns",
                exclusive_total.as_nanos(),
                query_duration.as_nanos(),
            ),
        )),
    }
}
