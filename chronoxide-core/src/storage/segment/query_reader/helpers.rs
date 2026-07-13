use super::*;

pub(super) fn metric_series_range_candidates(
    reader: &SegmentReader,
    context: &mut SegmentQueryContext,
    matcher: &ResolvedEqualityMatcher,
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Option<Vec<u32>>> {
    let Some(metric_name_sym) = context.symbols.lookup(METRIC_NAME_LABEL) else {
        return Ok(None);
    };
    if matcher.name_sym != metric_name_sym {
        return Ok(None);
    }

    let ranges = context.metric_series_ranges(reader, matcher.value_sym)?;
    metric_series_refs_from_ranges(&ranges, start_ms, end_ms).map(Some)
}

pub(super) fn metric_series_refs_from_ranges(
    ranges: &[MetricSeriesRange],
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Vec<u32>> {
    let mut series_refs = Vec::new();
    let mut matched_ranges = 0usize;
    for range in ranges.iter().copied() {
        if !range.overlaps(start_ms, end_ms) {
            continue;
        }
        let end_series_ref = range
            .start_series_ref
            .checked_add(range.series_count)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range overflows u32",
                )
            })?;
        let range_len = usize::try_from(range.series_count).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "metric series range too large")
        })?;
        series_refs
            .try_reserve(range_len)
            .map_err(io::Error::other)?;
        series_refs.extend(range.start_series_ref..end_series_ref);
        matched_ranges += 1;
    }

    if matched_ranges > 1 {
        series_refs.sort_unstable();
        series_refs.dedup();
    }
    Ok(series_refs)
}

pub(in crate::storage::segment) fn delta_projection_reset_hint(
    started: &mut bool,
) -> CounterResetHint {
    if *started {
        CounterResetHint::NotCounterReset
    } else {
        *started = true;
        CounterResetHint::CounterReset
    }
}
