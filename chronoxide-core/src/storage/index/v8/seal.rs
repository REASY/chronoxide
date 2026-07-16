//! Same-seal authorization for the private container-v8 encoder.
//!
//! Counts alone are not authority: this module proves the supplied indexes
//! against the finalized series inventory and the exact symbol dictionary
//! which the segment seal will publish before it lets bytes reach a writer.

use std::io::{self, Seek, Write};

use fst::{Set, Streamer};

use crate::labels::METRIC_NAME_LABEL;
use crate::storage::series::{SegmentSymbols, SeriesEntry};

use super::{
    AuthenticatedIndexFormat, RootCounts, SegmentIndexes, encode_validated_segment_indexes,
};

pub(super) fn write_segment_indexes_v8_for_roots(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    write_segment_indexes_for_roots(
        writer,
        indexes,
        num_series,
        symbols,
        series,
        AuthenticatedIndexFormat::V8Raw,
    )
}

pub(super) fn write_segment_indexes_v9_for_roots(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    write_segment_indexes_for_roots(
        writer,
        indexes,
        num_series,
        symbols,
        series,
        AuthenticatedIndexFormat::V9Adaptive,
    )
}

fn write_segment_indexes_for_roots(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
    format: AuthenticatedIndexFormat,
) -> io::Result<()> {
    let authorized_symbols = validate_authoritative_roots(num_series, symbols, series)?;
    let counts = RootCounts {
        series: num_series,
        symbols: authorized_symbols.count,
    };

    validate_same_seal_inventory(indexes, authorized_symbols, series, format)?;

    encode_validated_segment_indexes(writer, indexes, counts, format)
}

#[derive(Clone, Copy)]
struct AuthoritativeSymbols<'a> {
    symbols: &'a SegmentSymbols,
    count: u32,
    metric_name_sym: Option<u32>,
}

fn validate_authoritative_roots<'a>(
    num_series: u32,
    symbols: &'a SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<AuthoritativeSymbols<'a>> {
    let actual_series_count = u32::try_from(series.len())
        .map_err(|_| invalid_data("authoritative series inventory exceeds u32"))?;
    if actual_series_count != num_series {
        return Err(invalid_data(
            "authoritative series inventory disagrees with the series root count",
        ));
    }

    let symbol_count = u32::try_from(symbols.len())
        .map_err(|_| invalid_data("authoritative symbol inventory exceeds u32"))?;
    let mut previous: Option<&[u8]> = None;
    let mut metric_name_sym = None;
    for symbol_id in 0..symbol_count {
        let value = symbols.resolve(symbol_id).ok_or_else(|| {
            invalid_data(format!(
                "authoritative symbol {symbol_id} cannot be resolved"
            ))
        })?;
        if previous.is_some_and(|previous| previous >= value.as_bytes()) {
            return Err(invalid_data(
                "authoritative symbols are not strictly byte-sorted and unique",
            ));
        }
        if value == METRIC_NAME_LABEL {
            metric_name_sym = Some(symbol_id);
        }
        previous = Some(value.as_bytes());
    }
    Ok(AuthoritativeSymbols {
        symbols,
        count: symbol_count,
        metric_name_sym,
    })
}

fn validate_same_seal_inventory(
    indexes: &SegmentIndexes,
    symbols: AuthoritativeSymbols<'_>,
    series: &[SeriesEntry],
    format: AuthenticatedIndexFormat,
) -> io::Result<()> {
    let series_count = u32::try_from(series.len())
        .map_err(|_| invalid_data("authoritative series inventory exceeds u32"))?;
    indexes
        .metric_series_ranges
        .validate_complete_partition(series_count, symbols.count)?;
    let metric_symbols = validate_exact_membership_linear(indexes, symbols, series)?;
    validate_label_inventory_linear(indexes, symbols)?;
    if let Some(routing_index) = &indexes.routing_index {
        match format {
            AuthenticatedIndexFormat::V8Raw => routing_index.validate_against_indexes(
                symbols.symbols,
                &indexes.exact_postings,
                &indexes.label_value_time_ranges,
            )?,
            AuthenticatedIndexFormat::V9Adaptive => routing_index
                .validate_against_indexes_adaptive(
                    symbols.symbols,
                    &indexes.exact_postings,
                    &indexes.label_value_time_ranges,
                )?,
        }
    }
    validate_metric_partition(indexes, symbols, series, &metric_symbols)
}

fn validate_exact_membership_linear(
    indexes: &SegmentIndexes,
    symbols: AuthoritativeSymbols<'_>,
    series: &[SeriesEntry],
) -> io::Result<Vec<u32>> {
    let mut membership_cursors = Vec::new();
    membership_cursors
        .try_reserve_exact(series.len())
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
    membership_cursors.resize(series.len(), 0u32);
    let mut metric_symbols = Vec::new();
    metric_symbols
        .try_reserve_exact(series.len())
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;

    for (series_ref, entry) in series.iter().enumerate() {
        let series_ref = u32::try_from(series_ref)
            .map_err(|_| invalid_data("authoritative series_ref exceeds u32"))?;
        if entry.kind_mask == 0
            || u16::from(entry.kind_mask) & !super::super::VALID_METRIC_SERIES_KIND_MASK != 0
        {
            return Err(invalid_data(format!(
                "series {series_ref} kind mask is zero or contains unknown bits"
            )));
        }

        let mut previous_name = None;
        let mut metric_count = 0u8;
        let mut metric_sym = None;
        for &(name_sym, value_sym) in &entry.labels {
            resolve_authorized(symbols, name_sym, "series label name")?;
            resolve_authorized(symbols, value_sym, "series label value")?;
            if previous_name.is_some_and(|previous| previous >= name_sym) {
                return Err(invalid_data(format!(
                    "series {series_ref} labels are not strictly ordered by name symbol"
                )));
            }
            previous_name = Some(name_sym);
            if Some(name_sym) == symbols.metric_name_sym {
                metric_count = metric_count
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("series metric-label count overflows"))?;
                metric_sym = Some(value_sym);
            }
        }
        if metric_count != 1 {
            return Err(invalid_data(format!(
                "series {series_ref} must contain exactly one __name__ label"
            )));
        }
        metric_symbols.push(metric_sym.ok_or_else(|| {
            invalid_data(format!(
                "series {series_ref} must contain exactly one __name__ label"
            ))
        })?);
    }

    for (name_sym, value_sym, refs) in indexes.exact_postings.entries() {
        resolve_authorized(symbols, name_sym, "exact-postings label name")?;
        resolve_authorized(symbols, value_sym, "exact-postings label value")?;
        if refs.is_empty() {
            return Err(invalid_data("exact-postings entry has no refs"));
        }
        if !refs.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(invalid_data(
                "exact-postings refs are not strictly ordered and unique",
            ));
        }
        let range = indexes
            .label_value_time_ranges
            .get(name_sym, value_sym)
            .ok_or_else(|| {
                invalid_data(format!(
                    "label-value time-range inventory omits exact key ({name_sym}, {value_sym})"
                ))
            })?;
        if range.min_time_ms > range.max_time_ms {
            return Err(invalid_data(format!(
                "label-value time range ({name_sym}, {value_sym}) is reversed"
            )));
        }

        for &series_ref in refs {
            let series_index = usize::try_from(series_ref)
                .map_err(|_| invalid_data("exact-postings ref exceeds the root series count"))?;
            let entry = series
                .get(series_index)
                .ok_or_else(|| invalid_data("exact-postings ref exceeds the root series count"))?;
            let cursor = membership_cursors
                .get_mut(series_index)
                .ok_or_else(|| invalid_data("exact-postings ref exceeds the root series count"))?;
            let label_index = usize::try_from(*cursor)
                .map_err(|_| invalid_data("series label cursor exceeds usize"))?;
            let expected = entry.labels.get(label_index).copied();
            if expected != Some((name_sym, value_sym)) {
                return Err(invalid_data(format!(
                    "exact postings omit series {series_ref} label ({name_sym}, {value_sym}) or contain a foreign membership"
                )));
            }
            *cursor = cursor
                .checked_add(1)
                .ok_or_else(|| invalid_data("series label cursor overflows"))?;
        }
    }

    for (series_ref, (entry, cursor)) in series.iter().zip(&membership_cursors).enumerate() {
        if usize::try_from(*cursor).ok() != Some(entry.labels.len()) {
            return Err(invalid_data(format!(
                "exact postings omit at least one label membership for series {series_ref}"
            )));
        }
    }
    if indexes.label_value_time_ranges.len() != indexes.exact_postings.len() {
        return Err(invalid_data(
            "exact postings and label-value time ranges have different key inventories",
        ));
    }
    Ok(metric_symbols)
}

fn validate_label_inventory_linear(
    indexes: &SegmentIndexes,
    symbols: AuthoritativeSymbols<'_>,
) -> io::Result<()> {
    let mut exact_entries = indexes.exact_postings.entries().peekable();
    let mut fst_group_count = 0usize;
    while let Some(&(name_sym, _, _)) = exact_entries.peek() {
        resolve_authorized(symbols, name_sym, "FST label name")?;
        let fst = indexes.label_values.fsts.get(&name_sym).ok_or_else(|| {
            invalid_data(format!(
                "label-value FST inventory omits exact label name {name_sym}"
            ))
        })?;
        let set = Set::new(fst.as_slice())
            .map_err(|error| invalid_data(format!("invalid label-value FST: {error}")))?;
        if set.is_empty() {
            return Err(invalid_data("label-value FST has no values"));
        }
        let mut stream = set.stream();
        while let Some(&(entry_name_sym, value_sym, _)) = exact_entries.peek() {
            if entry_name_sym != name_sym {
                break;
            }
            let expected = resolve_authorized(symbols, value_sym, "FST label value")?;
            let Some(actual) = stream.next() else {
                return Err(invalid_data(format!(
                    "label-value FST inventory omits exact value symbol {value_sym} for label {name_sym}"
                )));
            };
            if actual != expected.as_bytes() {
                return Err(unexpected_fst_value(symbols, name_sym, actual));
            }
            exact_entries.next();
        }
        if let Some(extra) = stream.next() {
            return Err(unexpected_fst_value(symbols, name_sym, extra));
        }
        fst_group_count = fst_group_count
            .checked_add(1)
            .ok_or_else(|| invalid_data("FST group count overflows"))?;
    }
    if fst_group_count != indexes.label_values.fsts.len() {
        return Err(invalid_data(
            "label-value FST inventory contains a label absent from exact postings",
        ));
    }
    Ok(())
}

fn unexpected_fst_value(
    symbols: AuthoritativeSymbols<'_>,
    name_sym: u32,
    value_bytes: &[u8],
) -> io::Error {
    let value = match std::str::from_utf8(value_bytes) {
        Ok(value) => value,
        Err(error) => return invalid_data(format!("invalid UTF-8 in FST: {error}")),
    };
    match symbols.symbols.lookup(value) {
        None => invalid_data(format!(
            "FST value {value:?} cannot be resolved through the authoritative symbol root"
        )),
        Some(value_sym) => invalid_data(format!(
            "FST inventory contains value symbol {value_sym} absent from exact postings for label {name_sym}"
        )),
    }
}

fn validate_metric_partition(
    indexes: &SegmentIndexes,
    symbols: AuthoritativeSymbols<'_>,
    series: &[SeriesEntry],
    metric_symbols: &[u32],
) -> io::Result<()> {
    if series.is_empty() {
        return Ok(());
    }
    let metric_name_sym = symbols.metric_name_sym.ok_or_else(|| {
        invalid_data("authoritative non-empty series inventory has no __name__ symbol")
    })?;

    for (metric_sym, ranges) in indexes.metric_series_ranges.entries() {
        resolve_authorized(symbols, metric_sym, "metric-series-range metric value")?;
        let postings = indexes
            .exact_postings
            .get(metric_name_sym, metric_sym)
            .ok_or_else(|| {
                invalid_data(format!(
                    "metric-series range {metric_sym} has no exact __name__ posting"
                ))
            })?;
        let mut posting_index = 0usize;
        let mut aggregate_min = u64::MAX;
        let mut aggregate_max = 0u64;

        for range in ranges {
            let end = u64::from(range.start_series_ref)
                .checked_add(u64::from(range.series_count))
                .ok_or_else(|| invalid_data("metric-series range end overflows"))?;
            let mut actual_kind_mask = 0u16;
            for series_ref in u64::from(range.start_series_ref)..end {
                let series_ref_u32 = u32::try_from(series_ref)
                    .map_err(|_| invalid_data("metric-series range exceeds u32"))?;
                let entry = series
                    .get(usize::try_from(series_ref).map_err(|_| {
                        invalid_data("metric-series range cannot address this platform")
                    })?)
                    .ok_or_else(|| {
                        invalid_data("metric-series range exceeds authoritative series inventory")
                    })?;
                let actual_metric_sym = metric_symbols
                    .get(usize::try_from(series_ref).map_err(|_| {
                        invalid_data("metric-series range cannot address this platform")
                    })?)
                    .copied()
                    .ok_or_else(|| {
                        invalid_data("metric-series range exceeds authoritative metric inventory")
                    })?;
                if actual_metric_sym != metric_sym {
                    return Err(invalid_data(format!(
                        "metric-series range {metric_sym} owns foreign series_ref {series_ref_u32}"
                    )));
                }
                if postings.get(posting_index).copied() != Some(series_ref_u32) {
                    return Err(invalid_data(format!(
                        "metric-series range {metric_sym} disagrees with its exact posting"
                    )));
                }
                posting_index = posting_index
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("metric posting cursor overflows"))?;
                actual_kind_mask |= u16::from(entry.kind_mask);
            }
            if actual_kind_mask != range.kind_mask {
                return Err(invalid_data(format!(
                    "metric-series range {metric_sym} kind mask disagrees with its series"
                )));
            }
            aggregate_min = aggregate_min.min(range.min_time_ms);
            aggregate_max = aggregate_max.max(range.max_time_ms);
        }
        if posting_index != postings.len() {
            return Err(invalid_data(format!(
                "metric-series range {metric_sym} does not cover its complete exact posting"
            )));
        }
        let label_range = indexes
            .label_value_time_ranges
            .get(metric_name_sym, metric_sym)
            .ok_or_else(|| {
                invalid_data(format!(
                    "metric-series range {metric_sym} has no __name__ time range"
                ))
            })?;
        if label_range.min_time_ms != aggregate_min || label_range.max_time_ms != aggregate_max {
            return Err(invalid_data(format!(
                "metric-series range {metric_sym} time summary disagrees with its __name__ range"
            )));
        }
    }

    for (name_sym, value_sym, _refs) in indexes.exact_postings.entries() {
        if name_sym == metric_name_sym && indexes.metric_series_ranges.ranges(value_sym).is_empty()
        {
            return Err(invalid_data(format!(
                "exact __name__ posting {value_sym} has no metric-series range"
            )));
        }
    }
    Ok(())
}

fn resolve_authorized<'a>(
    symbols: AuthoritativeSymbols<'a>,
    symbol_id: u32,
    description: &'static str,
) -> io::Result<&'a str> {
    if symbol_id >= symbols.count {
        return Err(invalid_data(format!(
            "{description} symbol exceeds the authoritative symbol count"
        )));
    }
    symbols.symbols.resolve(symbol_id).ok_or_else(|| {
        invalid_data(format!(
            "{description} symbol {symbol_id} cannot be resolved through the authoritative root"
        ))
    })
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests;
