use super::*;

pub(super) fn storage_selectors_from_promql_with_projection_config(
    selector: PromqlSelector,
    query_projection_config: &QueryProjectionConfig,
) -> Result<Vec<SegmentSelector>, PromqlQueryError> {
    if let Some(metric_name) = selector.metric_name.as_deref() {
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

pub(super) fn storage_selector_from_promql_with_projection_config(
    selector: PromqlSelector,
    query_projection_config: &QueryProjectionConfig,
) -> Result<SegmentSelector, PromqlQueryError> {
    let mut metric_name = selector.metric_name;
    let mut promql_matchers = selector.matchers;
    let mut projection = SegmentProjection::None;

    if let Some(name) = metric_name.as_deref() {
        if let Some(native) = name.strip_suffix("_bucket") {
            let le = take_virtual_eq_matcher(&mut promql_matchers, "le")?;
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
    if let Some(metric_name) = selector.metric_name.as_deref()
        && (metric_name.ends_with("_bucket")
            || metric_name.ends_with("_count")
            || metric_name.ends_with("_sum"))
    {
        return Ok(None);
    }
    if selector
        .matchers
        .iter()
        .any(|matcher| matcher.name == METRIC_NAME_LABEL && matcher.op == PromqlMatcherOp::Regex)
    {
        return Ok(None);
    }
    if selector
        .matchers
        .iter()
        .any(|matcher| matcher.name == "le" || matcher.name == "quantile")
    {
        return Ok(None);
    }
    storage_selector_from_promql_parts(
        selector.metric_name,
        selector.matchers,
        SegmentProjection::NativeHistogram,
    )
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

pub(super) fn regex_postings(
    name: &str,
    pattern: &str,
    symbols: &SegmentSymbols,
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
    profile: &mut SegmentStoreQueryProfile,
    match_promql_projection_names: bool,
) -> io::Result<Vec<u32>> {
    let regex = compile_promql_regex(pattern)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let Some(name_sym) = symbols.lookup(name) else {
        return Ok(Vec::new());
    };
    if !label_name_overlaps_range(index_reader, name_sym, start_ms, end_ms) {
        return Ok(Vec::new());
    }

    let ranges = index_reader
        .label_value_time_ranges(name_sym)?
        .map(|ranges| ranges.into_iter().collect::<BTreeMap<_, _>>());

    let mut out = Vec::new();
    for value in regex_label_values(
        index_reader,
        name_sym,
        pattern,
        match_promql_projection_names,
    )? {
        budget.observe_regex_value()?;
        let matches = if match_promql_projection_names {
            promql_projection_metric_name_matches(&value, &regex)
        } else {
            regex.is_match(&value)
        };
        if !matches {
            continue;
        }
        let value_sym = symbols.lookup(&value).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "label value fst symbol missing")
        })?;
        if let Some(ranges) = &ranges
            && !ranges
                .get(&value_sym)
                .is_some_and(|range| range.overlaps(start_ms, end_ms))
        {
            continue;
        }
        let Some(postings) = index_reader.exact_postings_metadata(name_sym, value_sym) else {
            continue;
        };
        if let Some(posting) = exact_postings_with_budget(
            index_reader,
            name_sym,
            value_sym,
            postings,
            budget,
            profile,
        )? {
            out = union_sorted(&out, &posting);
        }
    }

    Ok(out)
}

pub(super) fn regex_label_values(
    index_reader: &mut SegmentIndexReader<impl Read + Seek>,
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

pub(super) fn is_regex_literal_escape(ch: char) -> bool {
    matches!(
        ch,
        '\\' | '.' | '*' | '+' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '-'
    )
}

pub(super) fn intersect_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() && ri < right.len() {
        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => li += 1,
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

pub(super) fn union_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() || ri < right.len() {
        if li >= left.len() {
            out.extend_from_slice(&right[ri..]);
            break;
        }
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(right[ri]);
                ri += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

pub(super) fn subtract_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() {
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

pub(super) fn merge_query_results(results: Vec<SegmentQueryResult>) -> Vec<SegmentQueryResult> {
    let mut merged: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
    for result in results {
        let entry = merged.entry(result.series_id).or_insert_with(|| {
            SegmentQueryResult::with_shared_labels(result.series_id, result.labels.clone())
        });
        entry.extend_from(result);
    }

    let mut results: Vec<_> = merged.into_values().collect();
    for result in &mut results {
        result.dedupe_samples_keep_last();
    }
    results
}

pub(super) fn segment_window(timestamp_ms: u64, duration_ms: u64) -> (u64, u64) {
    let start_ms = timestamp_ms.saturating_sub(timestamp_ms % duration_ms);
    (start_ms, start_ms.saturating_add(duration_ms))
}
