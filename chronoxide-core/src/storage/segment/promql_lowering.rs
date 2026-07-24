use super::*;

pub(super) fn storage_selectors_from_promql_with_projection_config(
    selector: PromqlSelector,
    query_projection_config: &QueryProjectionConfig,
) -> Result<Vec<SegmentSelector>, PromqlQueryError> {
    if let Some(metric_name) = selector.metric_name.as_deref() {
        if let Some(native) = metric_name.strip_suffix("_bucket") {
            let native_matchers = selector.matchers.clone();
            return bucket_selectors_from_promql_parts(
                Some(metric_name.to_string()),
                selector.matchers,
                Some(native.to_string()),
                native_matchers,
                query_projection_config,
            );
        }
        if let Some(native) = metric_name.strip_suffix("_count") {
            return Ok(vec![
                storage_selector_from_promql_parts(
                    Some(metric_name.to_string()),
                    selector.matchers.clone(),
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(
                    Some(native.to_string()),
                    selector.matchers,
                    SegmentProjection::Count,
                )?,
            ]);
        }
        if let Some(native) = metric_name.strip_suffix("_sum") {
            return Ok(vec![
                storage_selector_from_promql_parts(
                    Some(metric_name.to_string()),
                    selector.matchers.clone(),
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(
                    Some(native.to_string()),
                    selector.matchers,
                    SegmentProjection::Sum,
                )?,
            ]);
        }
    }

    if selector.metric_name.is_none()
        && let Some((idx, metric_name)) = exact_metric_name_matcher(&selector.matchers)
    {
        if let Some(native) = metric_name.strip_suffix("_bucket") {
            let mut native_matchers = selector.matchers.clone();
            native_matchers[idx].value = native.to_string();
            return bucket_selectors_from_promql_parts(
                None,
                selector.matchers,
                None,
                native_matchers,
                query_projection_config,
            );
        }
        if let Some(native) = metric_name.strip_suffix("_count") {
            let mut native_matchers = selector.matchers.clone();
            native_matchers[idx].value = native.to_string();
            return Ok(vec![
                storage_selector_from_promql_parts(
                    None,
                    selector.matchers,
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(
                    None,
                    native_matchers,
                    SegmentProjection::Count,
                )?,
            ]);
        }
        if let Some(native) = metric_name.strip_suffix("_sum") {
            let mut native_matchers = selector.matchers.clone();
            native_matchers[idx].value = native.to_string();
            return Ok(vec![
                storage_selector_from_promql_parts(
                    None,
                    selector.matchers,
                    SegmentProjection::None,
                )?,
                storage_selector_from_promql_parts(None, native_matchers, SegmentProjection::Sum)?,
            ]);
        }
    }

    if selector.metric_name.is_none()
        && let Some(projection) = metric_name_regex_projection(&selector.matchers)
    {
        return Ok(vec![
            storage_selector_from_promql_parts(
                None,
                selector.matchers.clone(),
                SegmentProjection::None,
            )?,
            storage_selector_from_promql_parts(None, selector.matchers, projection)?,
        ]);
    }

    storage_selector_from_promql_with_projection_config(selector, query_projection_config)
        .map(|selector| vec![selector])
}

pub(super) fn storage_float_selectors_from_promql(
    selector: PromqlSelector,
) -> Result<Vec<SegmentSelector>, PromqlQueryError> {
    storage_selector_from_promql_parts(
        selector.metric_name,
        selector.matchers,
        SegmentProjection::None,
    )
    .map(|selector| vec![selector])
}

fn bucket_selectors_from_promql_parts(
    real_metric_name: Option<String>,
    real_matchers: Vec<crate::promql::PromqlMatcher>,
    native_metric_name: Option<String>,
    mut native_matchers: Vec<crate::promql::PromqlMatcher>,
    query_projection_config: &QueryProjectionConfig,
) -> Result<Vec<SegmentSelector>, PromqlQueryError> {
    let mut selectors = vec![storage_selector_from_promql_parts(
        real_metric_name,
        real_matchers,
        SegmentProjection::None,
    )?];

    let le = take_virtual_le_filter(&mut native_matchers)?;
    selectors.push(storage_selector_from_promql_parts(
        native_metric_name,
        native_matchers,
        SegmentProjection::HistogramBucket {
            le,
            exponential_histogram_boundaries: query_projection_config
                .exponential_histogram_bucket_boundaries()
                .to_vec(),
        },
    )?);

    Ok(selectors)
}

pub(super) fn storage_selector_from_promql_with_projection_config(
    selector: PromqlSelector,
    query_projection_config: &QueryProjectionConfig,
) -> Result<SegmentSelector, PromqlQueryError> {
    let mut metric_name = selector.metric_name;
    let mut promql_matchers = selector.matchers;
    let mut projection = SegmentProjection::None;

    if let Some(name) = metric_name.as_deref() {
        if let Some(native) = name.strip_suffix("_bucket") {
            let le = take_virtual_le_filter(&mut promql_matchers)?;
            metric_name = Some(native.to_string());
            projection = SegmentProjection::HistogramBucket {
                le,
                exponential_histogram_boundaries: query_projection_config
                    .exponential_histogram_bucket_boundaries()
                    .to_vec(),
            };
        } else if let Some(native) = name.strip_suffix("_count") {
            metric_name = Some(native.to_string());
            projection = SegmentProjection::Count;
        } else if let Some(native) = name.strip_suffix("_sum") {
            metric_name = Some(native.to_string());
            projection = SegmentProjection::Sum;
        }
    }

    if matches!(projection, SegmentProjection::None) {
        if let Some(quantile) = take_virtual_eq_matcher(&mut promql_matchers, "quantile")? {
            projection = SegmentProjection::SummaryQuantile {
                quantile: Some(quantile),
            };
        } else {
            projection = SegmentProjection::AllPromql {
                exponential_histogram_boundaries: query_projection_config
                    .exponential_histogram_bucket_boundaries()
                    .to_vec(),
            };
        }
    }

    storage_selector_from_promql_parts(metric_name, promql_matchers, projection)
}

pub(super) fn storage_selector_from_promql_parts(
    metric_name: Option<String>,
    promql_matchers: Vec<crate::promql::PromqlMatcher>,
    projection: SegmentProjection,
) -> Result<SegmentSelector, PromqlQueryError> {
    let matchers = label_matchers_from_promql(promql_matchers)?;

    let storage_selector = match metric_name {
        Some(metric_name) => SegmentSelector::with_metric(metric_name, matchers),
        None => SegmentSelector::new(matchers),
    };
    Ok(storage_selector.with_projection(projection))
}

pub(super) fn native_histogram_selector_from_promql(
    selector: PromqlSelector,
) -> Result<Option<SegmentSelector>, PromqlQueryError> {
    native_histogram_selector_from_promql_with_projection(
        selector,
        SegmentProjection::NativeHistogram,
    )
}

pub(super) fn native_exponential_histogram_selector_from_promql(
    selector: PromqlSelector,
) -> Result<Option<SegmentSelector>, PromqlQueryError> {
    native_histogram_selector_from_promql_with_projection(
        selector,
        SegmentProjection::NativeExponentialHistogram,
    )
}

fn native_histogram_selector_from_promql_with_projection(
    selector: PromqlSelector,
    projection: SegmentProjection,
) -> Result<Option<SegmentSelector>, PromqlQueryError> {
    storage_selector_from_promql_parts(selector.metric_name, selector.matchers, projection)
        .map(Some)
}

pub(super) fn label_matchers_from_promql(
    promql_matchers: Vec<crate::promql::PromqlMatcher>,
) -> Result<Vec<LabelMatcher>, PromqlQueryError> {
    let mut matchers = Vec::with_capacity(promql_matchers.len());
    for matcher in promql_matchers {
        match matcher.op {
            PromqlMatcherOp::Eq => {
                matchers.push(LabelMatcher::eq(matcher.name, matcher.value));
            }
            PromqlMatcherOp::NotEq => {
                matchers.push(LabelMatcher::not_eq(matcher.name, matcher.value));
            }
            PromqlMatcherOp::Regex => {
                compile_promql_regex(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid regex matcher: {err}"))
                })?;
                matchers.push(LabelMatcher::regex(matcher.name, matcher.value));
            }
            PromqlMatcherOp::NotRegex => {
                compile_promql_regex(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid regex matcher: {err}"))
                })?;
                matchers.push(LabelMatcher::not_regex(matcher.name, matcher.value));
            }
        }
    }
    Ok(matchers)
}

pub(super) fn exact_metric_name_matcher(
    matchers: &[crate::promql::PromqlMatcher],
) -> Option<(usize, &str)> {
    matchers.iter().enumerate().find_map(|(idx, matcher)| {
        (matcher.name == METRIC_NAME_LABEL && matcher.op == PromqlMatcherOp::Eq)
            .then_some((idx, matcher.value.as_str()))
    })
}

pub(super) fn metric_name_regex_projection(
    matchers: &[crate::promql::PromqlMatcher],
) -> Option<SegmentProjection> {
    matchers.iter().find_map(|matcher| {
        if matcher.name != METRIC_NAME_LABEL || matcher.op != PromqlMatcherOp::Regex {
            return None;
        }
        metric_name_regex_projection_suffix(&matcher.value).map(|suffix| match suffix {
            "_count" => SegmentProjection::Count,
            "_sum" => SegmentProjection::Sum,
            _ => unreachable!("unsupported projection suffix"),
        })
    })
}

pub(super) fn metric_name_regex_projection_suffix(pattern: &str) -> Option<&'static str> {
    let pattern = pattern.strip_suffix('$').unwrap_or(pattern);
    if pattern.ends_with("_count") {
        Some("_count")
    } else if pattern.ends_with("_sum") {
        Some("_sum")
    } else {
        None
    }
}

pub(super) fn take_virtual_eq_matcher(
    matchers: &mut Vec<crate::promql::PromqlMatcher>,
    label_name: &str,
) -> Result<Option<String>, PromqlQueryError> {
    let mut value = None;
    let mut retained = Vec::with_capacity(matchers.len());
    for matcher in matchers.drain(..) {
        if matcher.name != label_name {
            retained.push(matcher);
            continue;
        }
        if matcher.op != PromqlMatcherOp::Eq {
            return Err(PromqlQueryError::Unsupported(format!(
                "{label_name} projection matcher currently supports only equality"
            )));
        }
        if value.replace(matcher.value).is_some() {
            return Err(PromqlQueryError::Invalid(format!(
                "duplicate {label_name} projection matcher"
            )));
        }
    }
    *matchers = retained;
    Ok(value)
}

pub(super) fn take_virtual_le_filter(
    matchers: &mut Vec<crate::promql::PromqlMatcher>,
) -> Result<BucketLeFilter, PromqlQueryError> {
    let mut le_matchers = Vec::new();
    let mut retained = Vec::with_capacity(matchers.len());
    for matcher in matchers.drain(..) {
        if matcher.name != "le" {
            retained.push(matcher);
            continue;
        }

        let le_matcher = match matcher.op {
            PromqlMatcherOp::Eq => BucketLeMatcher::Eq(matcher.value),
            PromqlMatcherOp::NotEq => BucketLeMatcher::NotEq(matcher.value),
            PromqlMatcherOp::Regex => {
                compile_promql_regex(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid le regex matcher: {err}"))
                })?;
                BucketLeMatcher::Regex(matcher.value)
            }
            PromqlMatcherOp::NotRegex => {
                compile_promql_regex(&matcher.value).map_err(|err| {
                    PromqlQueryError::Invalid(format!("invalid le regex matcher: {err}"))
                })?;
                BucketLeMatcher::NotRegex(matcher.value)
            }
        };
        le_matchers.push(le_matcher);
    }
    *matchers = retained;
    Ok(BucketLeFilter::from_matchers(le_matchers))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the postings query keeps its index resources, time bounds, budget, and profile explicit"
)]
pub(super) fn regex_postings(
    name: &str,
    pattern: &str,
    symbols: &crate::storage::symbols::SegmentSymbolReader<File>,
    index_reader: &mut SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
    profile: &mut SegmentStoreQueryProfile,
    match_promql_projection_names: bool,
) -> io::Result<Vec<u32>> {
    let regex = compile_promql_regex(pattern)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let Some(name_sym) = symbols.lookup(name)? else {
        return Ok(Vec::new());
    };
    if !label_name_overlaps_range(index_reader, name_sym, start_ms, end_ms)? {
        return Ok(Vec::new());
    }

    let ranges = index_reader
        .label_value_time_ranges(name_sym)?
        .map(|ranges| ranges.into_iter().collect::<BTreeMap<_, _>>());

    let values = regex_label_values(
        index_reader,
        name_sym,
        pattern,
        match_promql_projection_names,
    )?;
    // Preserve the regex-expansion budget boundary before any postings work.
    // The index currently returns owned FST values as one vector; paging must
    // not add a second unbounded vector of matching owned strings.
    for _ in &values {
        budget.observe_regex_value()?;
    }

    let mut out = Vec::new();
    for values in values.chunks(REGEX_SYMBOL_LOOKUP_BATCH_VALUES) {
        let matching_values = values
            .iter()
            .filter(|value| {
                if match_promql_projection_names {
                    promql_projection_metric_name_matches(value, &regex)
                } else {
                    regex.is_match(value)
                }
            })
            .map(String::as_str)
            .collect::<Vec<_>>();
        for value_sym in symbols.lookup_many(&matching_values)? {
            let value_sym = value_sym.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
            })?;
            if let Some(ranges) = &ranges
                && !ranges
                    .get(&value_sym)
                    .is_some_and(|range| range.overlaps(start_ms, end_ms))
            {
                continue;
            }
            let Some(selection) = index_reader.select_exact_postings(name_sym, value_sym)? else {
                continue;
            };
            let posting = exact_postings_with_budget(index_reader, selection, budget, profile)?;
            out = union_sorted(&out, &posting);
        }
    }

    Ok(out)
}

// A batch of 256 borrowed values costs at most 4 KiB of temporary slice
// metadata on 64-bit targets and is large enough to route across typical
// 32-KiB symbol pages. The owned strings remain in the index result vector;
// paging does not clone or retain another unbounded set of matching strings.
pub(super) const REGEX_SYMBOL_LOOKUP_BATCH_VALUES: usize = 256;

pub(super) fn regex_label_values(
    index_reader: &mut SegmentIndexReader<impl crate::storage::index::SegmentIndexReadAt>,
    name_sym: u32,
    pattern: &str,
    match_promql_projection_names: bool,
) -> io::Result<Vec<String>> {
    let prefixes = regex_literal_prefixes(pattern, match_promql_projection_names);
    if prefixes.is_empty() {
        return index_reader.label_values_with_prefix(name_sym, None);
    }

    let mut values = BTreeSet::new();
    for prefix in prefixes {
        values.extend(index_reader.label_values_with_prefix(name_sym, Some(&prefix))?);
    }
    Ok(values.into_iter().collect())
}

pub(super) fn regex_literal_prefixes(
    pattern: &str,
    match_promql_projection_names: bool,
) -> Vec<String> {
    let Some(prefix) = regex_literal_prefix(pattern) else {
        return Vec::new();
    };

    let mut prefixes = BTreeSet::from([prefix.clone()]);
    if match_promql_projection_names {
        for suffix in PROMQL_PROJECTION_SUFFIXES {
            if let Some(base_prefix) = prefix.strip_suffix(suffix)
                && !base_prefix.is_empty()
            {
                prefixes.insert(base_prefix.to_string());
            }
        }
    }
    prefixes.into_iter().collect()
}

pub(super) fn regex_literal_prefix(pattern: &str) -> Option<String> {
    if regex_has_unescaped_alternation(pattern) {
        return None;
    }

    let mut chars = pattern.chars().peekable();
    if matches!(chars.peek(), Some('^')) {
        chars.next();
    }

    let mut prefix = String::new();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let escaped = chars.next()?;
                if is_regex_literal_escape(escaped) {
                    prefix.push(escaped);
                } else {
                    break;
                }
            }
            '.' | '*' | '+' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                break;
            }
            ch => prefix.push(ch),
        }
    }

    (!prefix.is_empty()).then_some(prefix)
}

fn regex_has_unescaped_alternation(pattern: &str) -> bool {
    let mut escaped = false;
    let mut in_class = false;
    for ch in pattern.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '[' if !in_class => in_class = true,
            ']' if in_class => in_class = false,
            '|' if !in_class => return true,
            _ => {}
        }
    }
    false
}

pub(super) fn is_regex_literal_escape(ch: char) -> bool {
    matches!(
        ch,
        '\\' | '.' | '*' | '+' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '-'
    )
}

pub(super) fn merge_query_results(results: Vec<SegmentQueryResult>) -> Vec<SegmentQueryResult> {
    let mut merged: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
    for result in results {
        let labels_complete = result.labels_complete;
        let metric_name_dropped_series_id = result.metric_name_dropped_series_id;
        let entry = merged.entry(result.series_id).or_insert_with(|| {
            let mut merged =
                SegmentQueryResult::with_shared_labels(result.series_id, result.labels.clone());
            merged.labels_complete = labels_complete;
            merged.metric_name_dropped_series_id = metric_name_dropped_series_id;
            merged
        });
        entry.extend_from(result);
    }

    let mut results: Vec<_> = merged.into_values().collect();
    for result in &mut results {
        result.dedupe_samples_keep_last();
    }
    results
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PromqlSelectorBranch {
    Real,
    VirtualProjection,
}

pub(super) fn observe_promql_selector_branch_conflicts(
    seen: &mut BTreeMap<u64, PromqlSelectorBranch>,
    selector: &SegmentSelector,
    results: &[SegmentQueryResult],
) -> io::Result<()> {
    let Some(branch) = promql_selector_branch(selector) else {
        return Ok(());
    };

    for result in results {
        if let Some(previous) = seen.insert(result.series_id, branch)
            && previous != branch
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "conflicting real and virtual PromQL series for labelset {}",
                    format_query_labels(&result.labels)
                ),
            ));
        }
    }
    Ok(())
}

fn promql_selector_branch(selector: &SegmentSelector) -> Option<PromqlSelectorBranch> {
    match selector.projection {
        SegmentProjection::None => Some(PromqlSelectorBranch::Real),
        SegmentProjection::Count
        | SegmentProjection::Sum
        | SegmentProjection::HistogramBucket { .. }
        | SegmentProjection::SummaryQuantile { .. } => {
            Some(PromqlSelectorBranch::VirtualProjection)
        }
        SegmentProjection::AllPromql { .. }
        | SegmentProjection::NativeHistogram
        | SegmentProjection::NativeExponentialHistogram => None,
    }
}

fn format_query_labels(labels: &QueryLabels) -> String {
    labels
        .pairs()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn segment_window(timestamp_ms: u64, duration_ms: u64) -> (u64, u64) {
    let start_ms = timestamp_ms.saturating_sub(timestamp_ms % duration_ms);
    (start_ms, start_ms.saturating_add(duration_ms))
}
