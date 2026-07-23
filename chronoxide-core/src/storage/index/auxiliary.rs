use super::*;

#[cfg(test)]
pub(in crate::storage::index) fn write_label_value_time_ranges_blob(
    ranges: &[(u32, LabelValueTimeRange)],
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &(u32::try_from(ranges.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "label value time range count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for (value_sym, range) in ranges {
        bytes.extend_from_slice(&value_sym.to_le_bytes());
        bytes.extend_from_slice(&range.min_time_ms.to_le_bytes());
        bytes.extend_from_slice(&range.max_time_ms.to_le_bytes());
    }
    Ok(bytes)
}

pub(in crate::storage::index) fn read_label_value_time_ranges_blob(
    bytes: &[u8],
) -> io::Result<Vec<(u32, LabelValueTimeRange)>> {
    if bytes.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time ranges payload is shorter than its count",
        ));
    }
    let mut cursor = 0usize;
    let count = usize::try_from(read_u32(bytes, &mut cursor)?).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range count exceeds platform usize",
        )
    })?;
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range payload has no records",
        ));
    }
    let expected_len = count
        .checked_mul(20)
        .and_then(|len| len.checked_add(4))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "label value time range count overflows its payload length",
            )
        })?;
    if expected_len != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range count does not match payload length",
        ));
    }
    let mut ranges = Vec::new();
    ranges
        .try_reserve_exact(count)
        .map_err(|_| io::Error::other("label value time range allocation failed"))?;
    let mut previous_value_sym = None;
    for _ in 0..count {
        let value_sym = read_u32(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        if previous_value_sym.is_some_and(|previous| previous >= value_sym) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label value time ranges are not strictly ordered and unique",
            ));
        }
        if min_time_ms > max_time_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label value time range is reversed",
            ));
        }
        ranges.push((
            value_sym,
            LabelValueTimeRange {
                min_time_ms,
                max_time_ms,
            },
        ));
        previous_value_sym = Some(value_sym);
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time ranges blob has trailing bytes",
        ));
    }
    Ok(ranges)
}

pub(in crate::storage::index) fn write_metric_series_ranges_blob(
    index: &MetricSeriesRangeIndex,
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&METRIC_SERIES_RANGES_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(
        &(u32::try_from(index.ranges.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "metric range count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for (metric_sym, ranges) in index.entries() {
        bytes.extend_from_slice(&metric_sym.to_le_bytes());
        // We keep range_count because it costs little and keeps the format robust if a
        // future writer splits the same metric by kind or lane.
        bytes.extend_from_slice(
            &(u32::try_from(ranges.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "metric series range count exceeds u32",
                )
            })?)
            .to_le_bytes(),
        );
        for range in ranges {
            bytes.extend_from_slice(&range.start_series_ref.to_le_bytes());
            bytes.extend_from_slice(&range.series_count.to_le_bytes());
            bytes.extend_from_slice(&range.kind_mask.to_le_bytes());
            bytes.extend_from_slice(&0u16.to_le_bytes());
            bytes.extend_from_slice(&range.min_time_ms.to_le_bytes());
            bytes.extend_from_slice(&range.max_time_ms.to_le_bytes());
        }
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy)]
pub(in crate::storage::index) enum MetricSeriesRangeBlobEvent {
    Header {
        metric_count: usize,
    },
    Group {
        metric_sym: u32,
        range_count: usize,
        ranges_offset: usize,
    },
    Range {
        metric_sym: u32,
        range: MetricSeriesRange,
    },
}

#[derive(Debug, Clone, Copy)]
pub(in crate::storage::index) struct MetricSeriesRangeBlobBounds {
    pub(in crate::storage::index) num_series: u32,
    pub(in crate::storage::index) symbol_count: u32,
}

pub(in crate::storage::index) fn walk_metric_series_ranges_blob(
    bytes: &[u8],
    bounds: Option<MetricSeriesRangeBlobBounds>,
    mut visitor: impl FnMut(MetricSeriesRangeBlobEvent) -> io::Result<()>,
) -> io::Result<()> {
    let mut cursor = 0usize;
    let magic = read_u32(bytes, &mut cursor)?;
    if magic != METRIC_SERIES_RANGES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != METRIC_SERIES_RANGES_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported metric series ranges version",
        ));
    }
    let flags = read_u16(bytes, &mut cursor)?;
    if flags != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges flags are non-zero",
        ));
    }
    let metric_count = read_u32(bytes, &mut cursor)? as usize;
    if metric_count > bytes.len().saturating_sub(cursor) / (8 + METRIC_SERIES_RANGE_RECORD_LEN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series range metric count exceeds the minimum remaining group bytes",
        ));
    }
    visitor(MetricSeriesRangeBlobEvent::Header { metric_count })?;
    let mut previous_metric_sym = None;
    let mut next_series_ref = 0u64;
    for _ in 0..metric_count {
        let metric_sym = read_u32(bytes, &mut cursor)?;
        if previous_metric_sym.is_some_and(|previous| metric_sym <= previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range metric symbols are not strictly increasing",
            ));
        }
        if bounds.is_some_and(|bounds| metric_sym >= bounds.symbol_count) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range symbol exceeds the authoritative symbol count",
            ));
        }
        previous_metric_sym = Some(metric_sym);
        let range_count = read_u32(bytes, &mut cursor)? as usize;
        if range_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range metric has no ranges",
            ));
        }
        let range_bytes = range_count
            .checked_mul(METRIC_SERIES_RANGE_RECORD_LEN)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range count overflows",
                )
            })?;
        if range_bytes > bytes.len().saturating_sub(cursor) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metric series range count exceeds remaining bytes",
            ));
        }
        visitor(MetricSeriesRangeBlobEvent::Group {
            metric_sym,
            range_count,
            ranges_offset: cursor,
        })?;
        let mut previous_series_end = None;
        for _ in 0..range_count {
            let start_series_ref = read_u32(bytes, &mut cursor)?;
            let series_count = read_u32(bytes, &mut cursor)?;
            let kind_mask = read_u16(bytes, &mut cursor)?;
            let reserved = read_u16(bytes, &mut cursor)?;
            let min_time_ms = read_u64(bytes, &mut cursor)?;
            let max_time_ms = read_u64(bytes, &mut cursor)?;
            if reserved != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range reserved field is non-zero",
                ));
            }
            if series_count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range series count is zero",
                ));
            }
            let series_end = u64::from(start_series_ref)
                .checked_add(u64::from(series_count))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "metric series range series end overflows",
                    )
                })?;
            if series_end > u64::from(u32::MAX) + 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range series end exceeds the u32 domain",
                ));
            }
            if bounds.is_some_and(|bounds| series_end > u64::from(bounds.num_series)) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range exceeds the bound series count",
                ));
            }
            if bounds.is_some() && u64::from(start_series_ref) != next_series_ref {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series ranges do not form a canonical complete partition",
                ));
            }
            if previous_series_end.is_some_and(|previous| u64::from(start_series_ref) < previous) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series ranges are unordered or overlapping",
                ));
            }
            previous_series_end = Some(series_end);
            if min_time_ms > max_time_ms {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range time bounds are reversed",
                ));
            }
            if kind_mask == 0 || kind_mask & !VALID_METRIC_SERIES_KIND_MASK != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "metric series range kind mask is zero or contains unknown bits",
                ));
            }
            next_series_ref = series_end;
            visitor(MetricSeriesRangeBlobEvent::Range {
                metric_sym,
                range: MetricSeriesRange {
                    start_series_ref,
                    series_count,
                    kind_mask,
                    min_time_ms,
                    max_time_ms,
                },
            })?;
        }
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges blob has trailing bytes",
        ));
    }
    if bounds.is_some_and(|bounds| next_series_ref != u64::from(bounds.num_series)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metric series ranges do not cover the authoritative series count",
        ));
    }
    Ok(())
}

pub(in crate::storage::index) fn read_metric_series_ranges_blob(
    bytes: &[u8],
) -> io::Result<MetricSeriesRangeIndex> {
    let mut index = MetricSeriesRangeIndex::default();
    walk_metric_series_ranges_blob(bytes, None, |event| {
        match event {
            MetricSeriesRangeBlobEvent::Header { .. } => {}
            MetricSeriesRangeBlobEvent::Group {
                metric_sym,
                range_count,
                ..
            } => {
                let mut ranges = Vec::new();
                ranges
                    .try_reserve_exact(range_count)
                    .map_err(|_| io::Error::other("metric series range allocation failed"))?;
                index.ranges.insert(metric_sym, ranges);
            }
            MetricSeriesRangeBlobEvent::Range { metric_sym, range } => {
                index
                    .ranges
                    .get_mut(&metric_sym)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "metric series range group state is missing",
                        )
                    })?
                    .push(range);
            }
        }
        Ok(())
    })?;
    Ok(index)
}

pub(in crate::storage::index) fn read_fst_values(bytes: &[u8]) -> io::Result<Vec<String>> {
    read_fst_values_with_prefix(bytes, None)
}

pub(in crate::storage::index) fn read_fst_values_with_prefix(
    bytes: &[u8],
    prefix: Option<&str>,
) -> io::Result<Vec<String>> {
    let set = Set::new(bytes).map_err(fst_io_error)?;
    if set.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value FST contains no values",
        ));
    }
    let mut stream = match prefix {
        Some(prefix) if !prefix.is_empty() => {
            let mut builder = set.range().ge(prefix);
            if let Some(upper) = prefix_upper_bound(prefix.as_bytes()) {
                builder = builder.lt(upper);
            }
            builder.into_stream()
        }
        Some(_) | None => set.stream(),
    };
    let mut values = Vec::new();
    while let Some(value) = stream.next() {
        let value = std::str::from_utf8(value).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid utf8 fst value: {err}"),
            )
        })?;
        values.push(value.to_string());
    }
    Ok(values)
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    for index in (0..bound.len()).rev() {
        if bound[index] == u8::MAX {
            continue;
        }
        bound[index] = bound[index].saturating_add(1);
        bound.truncate(index + 1);
        return Some(bound);
    }
    None
}

pub(in crate::storage::index) fn read_label_value_fst_index_bytes(
    bytes: &[u8],
) -> io::Result<LabelValueFstIndex> {
    let mut cursor = 0usize;

    let magic = read_u32(bytes, &mut cursor)?;
    if magic != LABEL_VALUE_FST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value index magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported label value index version",
        ));
    }
    let _flags = read_u16(bytes, &mut cursor)?;
    let label_count = read_u32(bytes, &mut cursor)? as usize;

    let mut index = LabelValueFstIndex::default();
    for _ in 0..label_count {
        let name = read_u32(bytes, &mut cursor)?;
        let fst_len = read_u32(bytes, &mut cursor)? as usize;
        index.insert_fst(name, read_bytes(bytes, &mut cursor, fst_len)?.to_vec());
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value index has trailing bytes",
        ));
    }

    Ok(index)
}

pub(in crate::storage::index) fn fst_io_error(err: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
