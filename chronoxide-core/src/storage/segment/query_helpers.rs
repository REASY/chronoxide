use super::*;

const METADATA_SYMBOL_BATCH_SIZE: usize = 256;

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
    symbols: &crate::storage::symbols::SegmentSymbolReader<
        impl crate::storage::symbols::SegmentSymbolReadAt,
    >,
    index_reader: &mut SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let Some(name_sym) = symbols.lookup(METRIC_NAME_LABEL)? else {
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
    symbols: &crate::storage::symbols::SegmentSymbolReader<
        impl crate::storage::symbols::SegmentSymbolReadAt,
    >,
    index_reader: &mut SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let mut name_symbols = Vec::new();
    for name_sym in index_reader.label_name_symbols()? {
        if !label_name_overlaps_range(index_reader, name_sym, start_ms, end_ms)? {
            continue;
        }
        name_symbols.push(name_sym);
    }

    for name_symbol_batch in name_symbols.chunks(METADATA_SYMBOL_BATCH_SIZE) {
        let resolved = symbols.resolve_many(name_symbol_batch)?;
        if resolved.len() != name_symbol_batch.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label-name symbol batch changed result cardinality",
            ));
        }
        for name in resolved {
            let name = name.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "label symbol missing")
            })?;
            metadata.add_label_name(name.to_string());
        }
    }

    Ok(())
}

pub(super) fn collect_label_values_from_index(
    symbols: &crate::storage::symbols::SegmentSymbolReader<
        impl crate::storage::symbols::SegmentSymbolReadAt,
    >,
    index_reader: &mut SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    label_name: &str,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let label_name = normalize_discovery_label_name(label_name);
    let Some(name_sym) = symbols.lookup(&label_name)? else {
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
    symbols: &crate::storage::symbols::SegmentSymbolReader<
        impl crate::storage::symbols::SegmentSymbolReadAt,
    >,
    index_reader: &mut SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    name_sym: u32,
    label_name: &str,
    start_ms: u64,
    end_ms: u64,
    metadata: &mut MetadataAccumulator,
) -> io::Result<()> {
    let ranges = index_reader
        .label_value_time_ranges(name_sym)?
        .map(|ranges| ranges.into_iter().collect::<BTreeMap<_, _>>());
    let values = index_reader.label_values(name_sym)?;

    let Some(ranges) = ranges else {
        for value in values {
            metadata.add_label_value(label_name.to_string(), value);
        }
        return Ok(());
    };

    let mut values = values.into_iter();
    loop {
        let batch = values
            .by_ref()
            .take(METADATA_SYMBOL_BATCH_SIZE)
            .collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        let value_symbols = symbols.lookup_many(&batch)?;
        if value_symbols.len() != batch.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "label-value symbol batch changed result cardinality",
            ));
        }

        for (value, value_sym) in batch.into_iter().zip(value_symbols) {
            let value_sym = value_sym.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
            })?;
            if ranges
                .get(&value_sym)
                .is_some_and(|range| range.overlaps(start_ms, end_ms))
            {
                metadata.add_label_value(label_name.to_string(), value);
            }
        }
    }
    Ok(())
}

pub(super) fn label_name_overlaps_range(
    index_reader: &SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    name_sym: u32,
    start_ms: u64,
    end_ms: u64,
) -> io::Result<bool> {
    Ok(match index_reader.label_time_range(name_sym)? {
        Some(range) => range.overlaps(start_ms, end_ms),
        None => true,
    })
}

pub(super) fn exact_postings_with_budget(
    index_reader: &SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    selection: crate::storage::index::ExactPostingsSelection,
    budget: &mut QueryBudget,
    profile: &mut SegmentStoreQueryProfile,
) -> io::Result<Vec<u32>> {
    let postings = selection.metadata();
    budget.observe_index_postings_read(postings.byte_len);
    let start = Instant::now();
    let postings_result = index_reader.read_exact_postings(selection)?;
    profile.exact_postings_read = profile.exact_postings_read.saturating_add(start.elapsed());
    profile.exact_postings_bytes = profile
        .exact_postings_bytes
        .saturating_add(postings.byte_len);
    Ok(postings_result)
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

impl CompiledLabelMatcher {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Eq { name, .. }
            | Self::NotEq { name, .. }
            | Self::Regex { name, .. }
            | Self::NotRegex { name, .. } => name,
        }
    }

    pub(crate) fn requires_missing_label_scan(&self) -> bool {
        match self {
            Self::Eq { value, .. } | Self::NotEq { value, .. } => value.is_empty(),
            Self::Regex { pattern, .. } | Self::NotRegex { pattern, .. } => pattern.is_match(""),
        }
    }

    pub(crate) fn matches_value(&self, value: &str, match_promql_projection_names: bool) -> bool {
        match self {
            Self::Eq {
                value: expected, ..
            } => value == expected,
            Self::NotEq {
                value: expected, ..
            } => value != expected,
            Self::Regex { name, pattern } => {
                if match_promql_projection_names && name == METRIC_NAME_LABEL {
                    promql_projection_metric_name_matches(value, pattern)
                } else {
                    pattern.is_match(value)
                }
            }
            Self::NotRegex { name, pattern } => {
                if match_promql_projection_names && name == METRIC_NAME_LABEL {
                    !promql_projection_metric_name_matches(value, pattern)
                } else {
                    !pattern.is_match(value)
                }
            }
        }
    }
}

pub(crate) fn labels_match_compiled(
    labels: &[(String, String)],
    matchers: &[CompiledLabelMatcher],
) -> bool {
    matchers.iter().all(|matcher| {
        let value = labels
            .iter()
            .find_map(|(name, value)| (name == matcher.name()).then_some(value.as_str()))
            .unwrap_or("");
        matcher.matches_value(value, false)
    })
}

pub(crate) fn query_labels_match_compiled(
    labels: &QueryLabels,
    matchers: &[CompiledLabelMatcher],
) -> bool {
    matchers.iter().all(|matcher| {
        let value = labels
            .pairs()
            .find_map(|(name, value)| (name == matcher.name()).then_some(value))
            .unwrap_or("");
        matcher.matches_value(value, false)
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
