use super::*;

pub(super) fn typed_scalar_projection(
    projection: &SegmentProjection,
    kind: ChunkKind,
) -> Option<(ChunkScalarProjection, &'static str)> {
    if !chunk_kind_is_typed(kind) {
        return None;
    }
    match projection {
        SegmentProjection::Count => Some((ChunkScalarProjection::Count, "_count")),
        SegmentProjection::Sum => Some((ChunkScalarProjection::Sum, "_sum")),
        SegmentProjection::None
        | SegmentProjection::AllPromql { .. }
        | SegmentProjection::HistogramBucket { .. }
        | SegmentProjection::NativeHistogram
        | SegmentProjection::NativeExponentialHistogram
        | SegmentProjection::SummaryQuantile { .. } => None,
    }
}

pub(crate) fn projection_matches_promql_metric_name_regex(projection: &SegmentProjection) -> bool {
    matches!(
        projection,
        SegmentProjection::AllPromql { .. } | SegmentProjection::Count | SegmentProjection::Sum
    )
}

pub(super) fn chunk_kind_matches_projection(
    projection: &SegmentProjection,
    kind: ChunkKind,
) -> bool {
    match projection {
        SegmentProjection::None => matches!(kind, ChunkKind::Float | ChunkKind::Int64),
        SegmentProjection::AllPromql { .. } => true,
        SegmentProjection::Count | SegmentProjection::Sum => chunk_kind_is_typed(kind),
        SegmentProjection::HistogramBucket { .. } => {
            matches!(kind, ChunkKind::Histogram | ChunkKind::ExponentialHistogram)
        }
        SegmentProjection::NativeHistogram => kind == ChunkKind::Histogram,
        SegmentProjection::NativeExponentialHistogram => kind == ChunkKind::ExponentialHistogram,
        SegmentProjection::SummaryQuantile { .. } => kind == ChunkKind::Summary,
    }
}

pub(super) fn chunk_kind_is_typed(kind: ChunkKind) -> bool {
    matches!(
        kind,
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
    )
}

pub(super) fn series_kind_mask_matches_projection(
    projection: &SegmentProjection,
    kind_mask: u8,
) -> bool {
    let required = match projection {
        SegmentProjection::None => SERIES_KIND_FLOAT | SERIES_KIND_INT64,
        SegmentProjection::AllPromql { .. } => return true,
        SegmentProjection::Count | SegmentProjection::Sum => {
            SERIES_KIND_HISTOGRAM | SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY
        }
        SegmentProjection::HistogramBucket { .. } => {
            SERIES_KIND_HISTOGRAM | SERIES_KIND_EXPONENTIAL_HISTOGRAM
        }
        SegmentProjection::NativeHistogram => SERIES_KIND_HISTOGRAM,
        SegmentProjection::NativeExponentialHistogram => SERIES_KIND_EXPONENTIAL_HISTOGRAM,
        SegmentProjection::SummaryQuantile { .. } => SERIES_KIND_SUMMARY,
    };
    kind_mask & required != 0
}

pub(super) fn collect_metric_names_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let Some(name_sym) = symbols.lookup(METRIC_NAME_LABEL) else {
        return Ok(());
    };
    collect_label_values_by_symbol_from_index(
        symbols,
        index_reader,
        name_sym,
        METRIC_NAME_LABEL,
        start_ms,
        end_ms,
        metadata,
    )
}

pub(super) fn collect_label_names_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    for name_sym in index_reader.label_name_symbols() {
        if !label_name_overlaps_range(index_reader, name_sym, start_ms, end_ms) {
            continue;
        }
        let name = symbols
            .resolve(name_sym)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "label symbol missing"))?
            .to_string();
        metadata.add_label_name(name);
    }

    Ok(())
}

pub(super) fn collect_label_values_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    label_name: &str,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let label_name = normalize_discovery_label_name(label_name);
    let Some(name_sym) = symbols.lookup(&label_name) else {
        return Ok(());
    };
    collect_label_values_by_symbol_from_index(
        symbols,
        index_reader,
        name_sym,
        &label_name,
        start_ms,
        end_ms,
        metadata,
    )
}

pub(super) fn collect_label_values_by_symbol_from_index(
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    name_sym: u32,
    label_name: &str,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let ranges = index_reader
        .label_value_time_ranges(name_sym)?
        .map(|ranges| ranges.into_iter().collect::<BTreeMap<_, _>>());

    for value in index_reader.label_values(name_sym)? {
        let overlaps = if let Some(ranges) = &ranges {
            let value_sym = symbols.lookup(&value).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
            })?;
            ranges
                .get(&value_sym)
                .is_some_and(|range| range.overlaps(start_ms, end_ms))
        } else {
            true
        };

        if overlaps {
            metadata.add_label_value(label_name.to_string(), value);
        }
    }
    Ok(())
}

pub(super) fn label_name_overlaps_range(
    index_reader: &SegmentIndexReader<impl Read + Seek>,
    name_sym: u32,
    start_ms: u64,
    end_ms: u64,
) -> bool {
    match index_reader.label_time_range(name_sym) {
        Some(range) => range.overlaps(start_ms, end_ms),
        None => true,
    }
}

pub(super) fn exact_postings_with_budget(
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    name_sym: u32,
    value_sym: u32,
    postings: ExactPostingsMetadata,
    budget: &mut QueryBudget,
    profile: &mut SegmentStoreQueryProfile,
) -> io::Result<Option<Vec<u32>>> {
    budget.observe_index_postings_read(postings.byte_len);
    let start = Instant::now();
    let postings_result = index_reader.exact_postings(name_sym, value_sym)?;
    profile.exact_postings_read = profile.exact_postings_read.saturating_add(start.elapsed());
    if postings_result.is_some() {
        profile.exact_postings_bytes = profile
            .exact_postings_bytes
            .saturating_add(postings.byte_len);
    }
    Ok(postings_result)
}

pub(super) fn should_verify_equality_candidates(
    candidate_count: usize,
    postings_byte_len: u64,
) -> bool {
    const MAX_SERIES_DRIVEN_CANDIDATES: usize = 64;
    if candidate_count == 0 || candidate_count > MAX_SERIES_DRIVEN_CANDIDATES {
        return false;
    }

    let estimated_series_verify_bytes = (candidate_count as u64).saturating_mul(32);
    estimated_series_verify_bytes < postings_byte_len
}

pub(super) fn series_entry_has_label(entry: &SeriesEntry, name_sym: u32, value_sym: u32) -> bool {
    entry
        .labels
        .iter()
        .any(|(name, value)| *name == name_sym && *value == value_sym)
}

pub(crate) fn compile_label_matchers(
    matchers: &[NormalizedMatcher],
) -> io::Result<Vec<CompiledLabelMatcher>> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        compiled.push(match matcher {
            NormalizedMatcher::Eq { name, value } => CompiledLabelMatcher::Eq {
                name: name.clone(),
                value: value.clone(),
            },
            NormalizedMatcher::NotEq { name, value } => CompiledLabelMatcher::NotEq {
                name: name.clone(),
                value: value.clone(),
            },
            NormalizedMatcher::Regex { name, pattern } => CompiledLabelMatcher::Regex {
                name: name.clone(),
                pattern: compile_promql_regex(pattern)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
            },
            NormalizedMatcher::NotRegex { name, pattern } => CompiledLabelMatcher::NotRegex {
                name: name.clone(),
                pattern: compile_promql_regex(pattern)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
            },
        });
    }
    Ok(compiled)
}

pub(crate) fn compile_promql_regex(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
}

pub(crate) fn labels_match_compiled(
    labels: &[(String, String)],
    matchers: &[CompiledLabelMatcher],
) -> bool {
    matchers.iter().all(|matcher| match matcher {
        CompiledLabelMatcher::Eq { name, value } => labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && label_value == value),
        CompiledLabelMatcher::NotEq { name, value } => !labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && label_value == value),
        CompiledLabelMatcher::Regex { name, pattern } => labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && pattern.is_match(label_value)),
        CompiledLabelMatcher::NotRegex { name, pattern } => !labels
            .iter()
            .any(|(label_name, label_value)| label_name == name && pattern.is_match(label_value)),
    })
}

pub(crate) enum CompiledBucketLeFilter {
    All,
    Exact(String),
    Matchers(Vec<CompiledBucketLeMatcher>),
}

impl CompiledBucketLeFilter {
    pub(crate) fn matches(&self, value: &str) -> bool {
        match self {
            Self::All => true,
            Self::Exact(expected) => expected == value,
            Self::Matchers(matchers) => matchers.iter().all(|matcher| matcher.matches(value)),
        }
    }
}

pub(crate) enum CompiledBucketLeMatcher {
    Eq(String),
    NotEq(String),
    Regex(regex::Regex),
    NotRegex(regex::Regex),
}

impl CompiledBucketLeMatcher {
    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Eq(expected) => expected == value,
            Self::NotEq(expected) => expected != value,
            Self::Regex(pattern) => pattern.is_match(value),
            Self::NotRegex(pattern) => !pattern.is_match(value),
        }
    }
}

pub(crate) fn compile_bucket_le_filter(
    filter: &BucketLeFilter,
) -> io::Result<CompiledBucketLeFilter> {
    match filter {
        BucketLeFilter::All => Ok(CompiledBucketLeFilter::All),
        BucketLeFilter::Exact(value) => Ok(CompiledBucketLeFilter::Exact(value.clone())),
        BucketLeFilter::Matchers(matchers) => {
            let mut compiled = Vec::with_capacity(matchers.len());
            for matcher in matchers {
                compiled.push(match matcher {
                    BucketLeMatcher::Eq(value) => CompiledBucketLeMatcher::Eq(value.clone()),
                    BucketLeMatcher::NotEq(value) => CompiledBucketLeMatcher::NotEq(value.clone()),
                    BucketLeMatcher::Regex(pattern) => CompiledBucketLeMatcher::Regex(
                        compile_promql_regex(pattern)
                            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
                    ),
                    BucketLeMatcher::NotRegex(pattern) => CompiledBucketLeMatcher::NotRegex(
                        compile_promql_regex(pattern)
                            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
                    ),
                });
            }
            Ok(CompiledBucketLeFilter::Matchers(compiled))
        }
    }
}

pub(crate) fn promql_projection_metric_name_matches(
    metric_name: &str,
    regex: &regex::Regex,
) -> bool {
    if regex.is_match(metric_name) {
        return true;
    }

    PROMQL_PROJECTION_SUFFIXES
        .iter()
        .any(|suffix| regex.is_match(&format!("{metric_name}{suffix}")))
}

pub(super) fn normalize_matcher_name_value(name: &str, value: &str) -> (String, String) {
    if name == METRIC_NAME_LABEL {
        (METRIC_NAME_LABEL.to_string(), normalize_metric_name(value))
    } else {
        (normalize_label_name(name), value.to_string())
    }
}

pub(super) fn normalize_matcher_name(name: &str) -> String {
    if name == METRIC_NAME_LABEL {
        METRIC_NAME_LABEL.to_string()
    } else {
        normalize_label_name(name)
    }
}

pub(super) fn normalize_discovery_label_name(name: &str) -> String {
    normalize_matcher_name(name)
}

pub(super) fn prefetch_file_range(
    file: &mut File,
    offset: u64,
    len: u64,
    scratch: &mut Vec<u8>,
) -> io::Result<()> {
    const PREFETCH_BUFFER_BYTES: usize = 64 * 1024;
    if len == 0 {
        return Ok(());
    }

    file.seek(SeekFrom::Start(offset))?;
    let mut remaining = len;
    while remaining > 0 {
        let read_len =
            usize::try_from(remaining.min(PREFETCH_BUFFER_BYTES as u64)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "prefetch range length exceeds usize",
                )
            })?;
        scratch.resize(read_len, 0);
        file.read_exact(scratch)?;
        remaining -= read_len as u64;
    }
    Ok(())
}

pub(super) fn chunk_overlaps_range(chunk: &ChunkIndexEntry, start_ms: u64, end_ms: u64) -> bool {
    chunk.max_time_ms >= start_ms && chunk.min_time_ms <= end_ms
}

pub(super) fn smoke_series_sample(
    segment_id: String,
    series_ref: u32,
    series_id: u64,
    labels: Vec<(String, String)>,
    record: &ChunkRecord,
    chunk_bytes: u32,
) -> SegmentStoreSmokeSeries {
    let (bucket_le, quantile) = match &record.samples {
        ChunkSamples::Histogram(values) => {
            let le = values
                .first()
                .and_then(|(_, value)| value.explicit_bounds.first().copied())
                .map(format_promql_float_label)
                .unwrap_or_else(|| "+Inf".to_string());
            (Some(le), None)
        }
        ChunkSamples::ExponentialHistogram(_) => (Some("+Inf".to_string()), None),
        ChunkSamples::Summary(values) => {
            let quantile = values
                .first()
                .and_then(|(_, value)| value.quantiles.first())
                .map(|value| format_promql_float_label(value.quantile));
            (None, quantile)
        }
        ChunkSamples::Float(_) | ChunkSamples::Int64(_) => (None, None),
    };

    SegmentStoreSmokeSeries {
        segment_id,
        series_ref,
        series_id,
        kind: record.kind,
        labels,
        min_time_ms: record.min_time_ms,
        max_time_ms: record.max_time_ms,
        samples: chunk_record_sample_count(record) as u64,
        chunk_bytes: u64::from(chunk_bytes),
        bucket_le,
        quantile,
    }
}

pub(super) fn chunk_record_sample_count(record: &ChunkRecord) -> usize {
    match &record.samples {
        ChunkSamples::Float(values) => values.len(),
        ChunkSamples::Int64(values) => values.len(),
        ChunkSamples::Histogram(values) => values.len(),
        ChunkSamples::ExponentialHistogram(values) => values.len(),
        ChunkSamples::Summary(values) => values.len(),
    }
}

pub(super) fn smoke_queries_for_sample(
    sample: &SegmentStoreSmokeSeries,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(ChunkKind, String, u64, u64)> {
    let Some(metric_name) = sample_metric_name(sample) else {
        return Vec::new();
    };
    let query_start_ms = sample.min_time_ms.max(start_ms);
    let query_end_ms = sample.max_time_ms.min(end_ms);
    if query_end_ms < query_start_ms {
        return Vec::new();
    }

    match sample.kind {
        ChunkKind::Float | ChunkKind::Int64 => {
            vec![(
                sample.kind,
                promql_exact_selector(metric_name, &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )]
        }
        ChunkKind::Histogram => {
            let mut queries = vec![(
                sample.kind,
                promql_exact_selector(&format!("{metric_name}_count"), &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )];
            if let Some(le) = &sample.bucket_le {
                queries.push((
                    sample.kind,
                    promql_exact_selector(
                        &format!("{metric_name}_bucket"),
                        &sample.labels,
                        Some(("le", le.as_str())),
                    ),
                    query_start_ms,
                    query_end_ms,
                ));
            }
            queries
        }
        ChunkKind::ExponentialHistogram => {
            let mut queries = vec![(
                sample.kind,
                promql_exact_selector(&format!("{metric_name}_count"), &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )];
            if let Some(le) = &sample.bucket_le {
                queries.push((
                    sample.kind,
                    promql_exact_selector(
                        &format!("{metric_name}_bucket"),
                        &sample.labels,
                        Some(("le", le.as_str())),
                    ),
                    query_start_ms,
                    query_end_ms,
                ));
            }
            queries
        }
        ChunkKind::Summary => {
            let mut queries = vec![(
                sample.kind,
                promql_exact_selector(&format!("{metric_name}_count"), &sample.labels, None),
                query_start_ms,
                query_end_ms,
            )];
            if let Some(quantile) = &sample.quantile {
                queries.push((
                    sample.kind,
                    promql_exact_selector(
                        metric_name,
                        &sample.labels,
                        Some(("quantile", quantile.as_str())),
                    ),
                    query_start_ms,
                    query_end_ms,
                ));
            }
            queries
        }
    }
}

pub(super) fn sample_metric_name(sample: &SegmentStoreSmokeSeries) -> Option<&str> {
    sample
        .labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
}

pub(super) fn promql_exact_selector(
    metric_name: &str,
    labels: &[(String, String)],
    extra_label: Option<(&str, &str)>,
) -> String {
    let mut matchers = Vec::with_capacity(labels.len() + 1 + usize::from(extra_label.is_some()));
    matchers.push(format!(
        r#"{}="{}""#,
        METRIC_NAME_LABEL,
        promql_escape_string(metric_name)
    ));
    for (key, value) in labels {
        if key == METRIC_NAME_LABEL || extra_label.is_some_and(|(extra_key, _)| extra_key == key) {
            continue;
        }
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    if let Some((key, value)) = extra_label {
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    format!("{{{}}}", matchers.join(","))
}

pub(super) fn promql_escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}

pub(super) fn smoke_query_limits() -> QueryLimits {
    QueryLimits {
        max_matched_series: Some(8),
        max_projected_series: Some(128),
        max_chunk_reads: Some(64),
        max_bytes_read: Some(16 * 1024 * 1024),
        max_samples_decoded: Some(4096),
        max_regex_values_examined: Some(0),
    }
}

pub(super) fn smoke_query_error(query: &str, err: PromqlQueryError) -> io::Error {
    io::Error::other(format!("smoke query failed: {query}: {err}"))
}
