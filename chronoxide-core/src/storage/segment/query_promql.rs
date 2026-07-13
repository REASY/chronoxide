use super::*;
use crate::promql::{
    PromqlInstantFunction, PromqlInstantFunctionKind, PromqlLabelJoin, PromqlLabelReplace,
};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use std::borrow::Cow;

mod native_binary;
pub(super) use native_binary::*;
mod native;
pub(super) use native::*;
mod range;
pub(super) use range::*;

pub(super) fn evaluate_aggregation(
    aggregation: &PromqlAggregation,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    if let PromqlAggregationOp::CountValues(value_label) = &aggregation.op {
        return evaluate_count_values_aggregation(
            value_label,
            &aggregation.grouping,
            results,
            eval_time_ms,
        );
    }

    if let Some((limit, largest)) = aggregation_rank_limit(&aggregation.op) {
        return evaluate_rank_aggregation(aggregation, results, eval_time_ms, limit, largest);
    }

    let collect_values = matches!(&aggregation.op, PromqlAggregationOp::Quantile(_));
    let mut groups = BTreeMap::<Vec<(String, String)>, AggregationAccumulator>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups
            .entry(labels)
            .or_default()
            .observe(value, collect_values);
    }

    let mut out = Vec::new();
    for (labels, accumulator) in groups {
        let Some(value) = accumulator.value(&aggregation.op) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_absent(
    absent: &PromqlAbsent,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    if results.iter().any(|result| {
        result
            .samples
            .last()
            .is_some_and(|(_, value)| !is_prometheus_stale_marker(*value))
    }) {
        return Vec::new();
    }

    let labels = absent.labels.clone();
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, 1.0);
    vec![result]
}

pub(super) fn evaluate_absent_over_time(
    function: &PromqlAbsentOverTime,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
    if results.iter().any(|result| {
        result.samples.iter().any(|(timestamp_ms, value)| {
            *timestamp_ms > range_start_ms
                && *timestamp_ms <= eval_time_ms
                && !is_prometheus_stale_marker(*value)
        })
    }) {
        return Vec::new();
    }

    let labels = function.labels.clone();
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, 1.0);
    vec![result]
}

pub(super) fn evaluate_instant_function(
    function: &PromqlInstantFunction,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    match function.kind {
        PromqlInstantFunctionKind::Sort => evaluate_sort(results, eval_time_ms, false),
        PromqlInstantFunctionKind::SortDesc => evaluate_sort(results, eval_time_ms, true),
        PromqlInstantFunctionKind::Abs => {
            evaluate_unary_value_function(results, eval_time_ms, f64::abs)
        }
        PromqlInstantFunctionKind::Ceil => {
            evaluate_unary_value_function(results, eval_time_ms, f64::ceil)
        }
        PromqlInstantFunctionKind::Floor => {
            evaluate_unary_value_function(results, eval_time_ms, f64::floor)
        }
        PromqlInstantFunctionKind::Round { to_nearest } => {
            evaluate_unary_value_function(results, eval_time_ms, |value| {
                round_to_nearest(value, to_nearest)
            })
        }
        PromqlInstantFunctionKind::Clamp { min, max } => {
            evaluate_clamp_function(results, eval_time_ms, min, max)
        }
        PromqlInstantFunctionKind::Ln => {
            evaluate_unary_value_function(results, eval_time_ms, f64::ln)
        }
        PromqlInstantFunctionKind::Log2 => {
            evaluate_unary_value_function(results, eval_time_ms, f64::log2)
        }
        PromqlInstantFunctionKind::Log10 => {
            evaluate_unary_value_function(results, eval_time_ms, f64::log10)
        }
        PromqlInstantFunctionKind::Sgn => evaluate_unary_value_function(results, eval_time_ms, sgn),
        PromqlInstantFunctionKind::Acos => {
            evaluate_unary_value_function(results, eval_time_ms, f64::acos)
        }
        PromqlInstantFunctionKind::Acosh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::acosh)
        }
        PromqlInstantFunctionKind::Asin => {
            evaluate_unary_value_function(results, eval_time_ms, f64::asin)
        }
        PromqlInstantFunctionKind::Asinh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::asinh)
        }
        PromqlInstantFunctionKind::Atan => {
            evaluate_unary_value_function(results, eval_time_ms, f64::atan)
        }
        PromqlInstantFunctionKind::Atanh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::atanh)
        }
        PromqlInstantFunctionKind::Cos => {
            evaluate_unary_value_function(results, eval_time_ms, f64::cos)
        }
        PromqlInstantFunctionKind::Cosh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::cosh)
        }
        PromqlInstantFunctionKind::Sin => {
            evaluate_unary_value_function(results, eval_time_ms, f64::sin)
        }
        PromqlInstantFunctionKind::Sinh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::sinh)
        }
        PromqlInstantFunctionKind::Tan => {
            evaluate_unary_value_function(results, eval_time_ms, f64::tan)
        }
        PromqlInstantFunctionKind::Tanh => {
            evaluate_unary_value_function(results, eval_time_ms, f64::tanh)
        }
        PromqlInstantFunctionKind::Deg => {
            evaluate_unary_value_function(results, eval_time_ms, |value| {
                value * 180.0 / std::f64::consts::PI
            })
        }
        PromqlInstantFunctionKind::Rad => {
            evaluate_unary_value_function(results, eval_time_ms, |value| {
                value * std::f64::consts::PI / 180.0
            })
        }
        PromqlInstantFunctionKind::Minute
        | PromqlInstantFunctionKind::Hour
        | PromqlInstantFunctionKind::DayOfMonth
        | PromqlInstantFunctionKind::DayOfWeek
        | PromqlInstantFunctionKind::DayOfYear
        | PromqlInstantFunctionKind::DaysInMonth
        | PromqlInstantFunctionKind::Month
        | PromqlInstantFunctionKind::Year => {
            evaluate_time_extraction_function(function.kind, results, eval_time_ms)
        }
        PromqlInstantFunctionKind::Timestamp => evaluate_timestamp_function(results, eval_time_ms),
    }
}

pub(super) fn evaluate_scalar_function(
    _function: &PromqlScalarFunction,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut values = results.iter().filter_map(|result| {
        result
            .samples
            .last()
            .and_then(|(_, value)| (!is_prometheus_stale_marker(*value)).then_some(*value))
    });
    let Some(value) = values.next() else {
        return evaluate_scalar(f64::NAN, eval_time_ms);
    };
    if values.next().is_some() {
        return evaluate_scalar(f64::NAN, eval_time_ms);
    }
    evaluate_scalar(value, eval_time_ms)
}

fn sgn(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else if value.is_sign_positive() {
        1.0
    } else {
        -1.0
    }
}

fn evaluate_unary_value_function(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    f: impl Fn(f64) -> f64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels = function_result_labels(result.labels.as_ref());
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, f(value));
        out.push(result);
    }
    merge_query_results(out)
}

fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    if to_nearest == 0.0 {
        return f64::NAN;
    }
    (value / to_nearest + 0.5).floor() * to_nearest
}

fn evaluate_clamp_function(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    min: Option<f64>,
    max: Option<f64>,
) -> Vec<SegmentQueryResult> {
    if min.is_some_and(f64::is_nan)
        || max.is_some_and(f64::is_nan)
        || min.zip(max).is_some_and(|(min, max)| min > max)
    {
        return Vec::new();
    }
    evaluate_unary_value_function(results, eval_time_ms, |value| {
        let value = min.map_or(value, |min| value.max(min));
        max.map_or(value, |max| value.min(max))
    })
}

fn evaluate_time_extraction_function(
    kind: PromqlInstantFunctionKind,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_unary_value_function(results, eval_time_ms, |value| {
        extract_utc_time_component(kind, value)
    })
}

fn extract_utc_time_component(kind: PromqlInstantFunctionKind, timestamp_secs: f64) -> f64 {
    if !timestamp_secs.is_finite() {
        return f64::NAN;
    }
    let millis = timestamp_secs * 1000.0;
    if millis < i64::MIN as f64 || millis > i64::MAX as f64 {
        return f64::NAN;
    }
    let Some(datetime) = Utc.timestamp_millis_opt(millis as i64).single() else {
        return f64::NAN;
    };
    match kind {
        PromqlInstantFunctionKind::Minute => datetime.minute() as f64,
        PromqlInstantFunctionKind::Hour => datetime.hour() as f64,
        PromqlInstantFunctionKind::DayOfMonth => datetime.day() as f64,
        PromqlInstantFunctionKind::DayOfWeek => datetime.weekday().num_days_from_sunday() as f64,
        PromqlInstantFunctionKind::DayOfYear => datetime.ordinal() as f64,
        PromqlInstantFunctionKind::DaysInMonth => {
            days_in_utc_month(datetime.year(), datetime.month()) as f64
        }
        PromqlInstantFunctionKind::Month => datetime.month() as f64,
        PromqlInstantFunctionKind::Year => datetime.year() as f64,
        _ => f64::NAN,
    }
}

fn days_in_utc_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let this_month = Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap();
    let next_month = Utc
        .with_ymd_and_hms(next_year, next_month, 1, 0, 0, 0)
        .unwrap();
    (next_month - this_month).num_days() as u32
}

fn evaluate_timestamp_function(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((timestamp_ms, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels = function_result_labels(result.labels.as_ref());
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, timestamp_ms as f64 / 1000.0);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_label_replace(
    function: &PromqlLabelReplace,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let regex = regex::Regex::new(&function.regex).map_err(|err| {
        PromqlQueryError::Invalid(format!("label_replace regex is invalid: {err}"))
    })?;
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut labels = result.labels.as_ref().to_vec();
        let src_value = label_value(&labels, &function.src_label).unwrap_or_default();
        if regex.is_match(&src_value) {
            let replacement = regex
                .replace(&src_value, function.replacement.as_str())
                .into_owned();
            set_label_value(&mut labels, &function.dst_label, replacement);
        }
        labels.sort();
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

pub(super) fn evaluate_label_join(
    function: &PromqlLabelJoin,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut labels = result.labels.as_ref().to_vec();
        let joined = function
            .src_labels
            .iter()
            .map(|label| label_value(&labels, label).unwrap_or_default())
            .collect::<Vec<_>>()
            .join(&function.separator);
        set_label_value(&mut labels, &function.dst_label, joined);
        labels.sort();
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

fn label_value(labels: &[(String, String)], name: &str) -> Option<String> {
    labels
        .iter()
        .find(|(label_name, _)| label_name == name)
        .map(|(_, value)| value.clone())
}

fn set_label_value(labels: &mut Vec<(String, String)>, name: &str, value: String) {
    if let Some((_, existing_value)) = labels.iter_mut().find(|(label_name, _)| label_name == name)
    {
        *existing_value = value;
    } else {
        labels.push((name.to_string(), value));
    }
}

fn evaluate_sort(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    descending: bool,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut sorted_result =
            SegmentQueryResult::with_shared_labels(result.series_id, result.labels);
        sorted_result.push_sample(eval_time_ms, value);
        out.push(sorted_result);
    }

    out.sort_by(|left, right| {
        let left_value = left.samples[0].1;
        let right_value = right.samples[0].1;
        let value_order = rank_value_order(left_value, right_value, descending);
        value_order.then_with(|| left.labels.cmp(&right.labels))
    });
    out
}

fn evaluate_count_values_aggregation(
    value_label: &str,
    grouping: &PromqlAggregationGrouping,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let effective_grouping = count_values_grouping(grouping, value_label);
    let mut groups = BTreeMap::<Vec<(String, String)>, u64>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut labels = result.labels.as_ref().to_vec();
        set_count_values_label(&mut labels, value_label, count_values_label_value(value));
        let labels = aggregation_group_labels(&effective_grouping, &labels);
        let count = groups.entry(labels).or_default();
        *count = count.saturating_add(1);
    }

    let mut out = Vec::new();
    for (labels, count) in groups {
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, count as f64);
        out.push(result);
    }
    merge_query_results(out)
}

fn count_values_grouping(
    grouping: &PromqlAggregationGrouping,
    value_label: &str,
) -> PromqlAggregationGrouping {
    match grouping {
        PromqlAggregationGrouping::All => {
            PromqlAggregationGrouping::By(vec![value_label.to_string()])
        }
        PromqlAggregationGrouping::By(labels) => {
            let mut labels = labels.clone();
            if !labels.iter().any(|label| label == value_label) {
                labels.push(value_label.to_string());
            }
            PromqlAggregationGrouping::By(labels)
        }
        PromqlAggregationGrouping::Without(labels) => {
            PromqlAggregationGrouping::Without(labels.clone())
        }
    }
}

fn set_count_values_label(labels: &mut Vec<(String, String)>, value_label: &str, value: String) {
    if let Some((_, existing)) = labels.iter_mut().find(|(key, _)| key == value_label) {
        *existing = value;
    } else {
        labels.push((value_label.to_string(), value));
    }
}

fn count_values_label_value(value: f64) -> String {
    format_promql_float_label(value)
}

fn aggregation_rank_limit(op: &PromqlAggregationOp) -> Option<(usize, bool)> {
    match op {
        PromqlAggregationOp::TopK(limit) => Some((*limit, true)),
        PromqlAggregationOp::BottomK(limit) => Some((*limit, false)),
        PromqlAggregationOp::Sum
        | PromqlAggregationOp::Count
        | PromqlAggregationOp::Avg
        | PromqlAggregationOp::Min
        | PromqlAggregationOp::Max
        | PromqlAggregationOp::Stddev
        | PromqlAggregationOp::Stdvar
        | PromqlAggregationOp::Group
        | PromqlAggregationOp::Quantile(_)
        | PromqlAggregationOp::CountValues(_) => None,
    }
}

fn evaluate_rank_aggregation(
    aggregation: &PromqlAggregation,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
    limit: usize,
    largest: bool,
) -> Vec<SegmentQueryResult> {
    if limit == 0 {
        return Vec::new();
    }

    let mut groups = BTreeMap::<Vec<(String, String)>, Vec<SegmentQueryResult>>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let group_labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        let ranked = SegmentQueryResult::with_shared_samples(
            result.series_id,
            result.labels,
            vec![(eval_time_ms, value)],
        );
        groups.entry(group_labels).or_default().push(ranked);
    }

    let mut out = Vec::new();
    for (_, mut group_results) in groups {
        group_results.sort_by(|left, right| {
            let left_value = left.samples[0].1;
            let right_value = right.samples[0].1;
            let value_order = rank_value_order(left_value, right_value, largest);
            value_order.then_with(|| left.labels.cmp(&right.labels))
        });
        out.extend(group_results.into_iter().take(limit));
    }
    merge_query_results(out)
}

fn rank_value_order(left: f64, right: f64, largest: bool) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => {
            if largest {
                right.total_cmp(&left)
            } else {
                left.total_cmp(&right)
            }
        }
    }
}

fn is_prometheus_stale_marker(value: f64) -> bool {
    value.to_bits() == prometheus_stale_nan().to_bits()
}

pub(super) fn evaluate_binary_vector_scalar(
    expression: &PromqlBinaryExpression,
    results: Vec<SegmentQueryResult>,
    scalar: f64,
    scalar_on_left: bool,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, vector_value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(vector_value) {
            continue;
        }
        let (value, labels) = if binary_operator_is_comparison(expression.op) {
            let matched = if scalar_on_left {
                compare_binary_operator(expression.op, scalar, vector_value)
            } else {
                compare_binary_operator(expression.op, vector_value, scalar)
            };
            if expression.return_bool {
                (
                    if matched { 1.0 } else { 0.0 },
                    function_result_labels(&result.labels),
                )
            } else if matched {
                (vector_value, result.labels.as_ref().to_vec())
            } else {
                continue;
            }
        } else {
            let value = if scalar_on_left {
                apply_binary_operator(expression.op, scalar, vector_value)
            } else {
                apply_binary_operator(expression.op, vector_value, scalar)
            };
            (value, function_result_labels(&result.labels))
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}

pub(super) fn evaluate_binary_vector_vector(
    expression: &PromqlBinaryExpression,
    left_results: Vec<SegmentQueryResult>,
    right_results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;

    let left_entries = binary_vector_entries(left_results, expression.vector_matching.as_ref());
    let right_entries = binary_vector_entries(right_results, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => evaluate_binary_vector_one_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
            comparison,
            bool_comparison,
        ),
        PromqlVectorMatchingCardinality::ManyToOne => evaluate_binary_vector_many_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
            comparison,
            bool_comparison,
        ),
        PromqlVectorMatchingCardinality::OneToMany => evaluate_binary_vector_one_to_many(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
            comparison,
            bool_comparison,
        ),
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

#[derive(Debug, Clone)]
struct BinaryVectorEntry {
    labels: Vec<(String, String)>,
    key: Vec<(String, String)>,
    value: f64,
}

fn binary_vector_entries(
    results: Vec<SegmentQueryResult>,
    matching: Option<&PromqlVectorMatching>,
) -> Vec<BinaryVectorEntry> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        out.push(BinaryVectorEntry {
            key: binary_vector_match_labels(result.labels.as_ref(), matching),
            labels: result.labels.as_ref().to_vec(),
            value,
        });
    }
    out
}

fn binary_vector_matching_cardinality(
    expression: &PromqlBinaryExpression,
) -> PromqlVectorMatchingCardinality {
    expression
        .vector_matching
        .as_ref()
        .map(|matching| matching.cardinality)
        .unwrap_or(PromqlVectorMatchingCardinality::OneToOne)
}

fn evaluate_binary_vector_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryVectorEntry>,
    right_entries: Vec<BinaryVectorEntry>,
    eval_time_ms: u64,
    comparison: bool,
    bool_comparison: bool,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, (Vec<(String, String)>, f64)>::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            comparison,
            bool_comparison,
        );
        if left_by_key
            .insert(entry.key.clone(), (labels, entry.value))
            .is_some()
        {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, f64>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, entry.value).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    for (key, (labels, left)) in left_by_key {
        let Some(right) = right_by_key.get(&key) else {
            continue;
        };
        let Some(value) = evaluate_binary_vector_value(expression, comparison, left, *right) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

fn binary_vector_output_labels(
    base_labels: &[(String, String)],
    include_labels_from: &[(String, String)],
    matching: Option<&PromqlVectorMatching>,
    comparison: bool,
    bool_comparison: bool,
) -> Vec<(String, String)> {
    let mut labels = base_labels.to_vec();

    if !comparison || bool_comparison {
        labels.retain(|(key, _)| key != METRIC_NAME_LABEL);
    }

    if let Some(matching) = matching {
        if matches!(
            matching.cardinality,
            PromqlVectorMatchingCardinality::OneToOne
        ) {
            match matching.mode {
                PromqlVectorMatchingMode::On => {
                    labels.retain(|(key, _)| {
                        key != METRIC_NAME_LABEL
                            && matching
                                .labels
                                .iter()
                                .any(|matching_label| matching_label == key)
                    });
                }
                PromqlVectorMatchingMode::Ignoring => {
                    labels.retain(|(key, _)| {
                        !matching
                            .labels
                            .iter()
                            .any(|matching_label| matching_label == key)
                    });
                }
            }
        }

        for include_label in &matching.include_labels {
            match include_labels_from
                .iter()
                .find(|(key, _)| key == include_label)
            {
                Some((_, include_value)) => {
                    if let Some((_, existing_value)) =
                        labels.iter_mut().find(|(key, _)| key == include_label)
                    {
                        *existing_value = include_value.clone();
                    } else {
                        labels.push((include_label.clone(), include_value.clone()));
                    }
                }
                None => labels.retain(|(key, _)| key != include_label),
            }
        }
    }

    labels.sort();
    labels
}

fn binary_vector_group_output_labels(
    many_side_labels: &[(String, String)],
    one_side_labels: &[(String, String)],
    matching: &PromqlVectorMatching,
    comparison: bool,
    bool_comparison: bool,
) -> Vec<(String, String)> {
    binary_vector_output_labels(
        many_side_labels,
        one_side_labels,
        Some(matching),
        comparison,
        bool_comparison,
    )
}

fn evaluate_binary_vector_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryVectorEntry>,
    right_entries: Vec<BinaryVectorEntry>,
    eval_time_ms: u64,
    comparison: bool,
    bool_comparison: bool,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_left vector matching metadata".to_string())
    })?;
    let mut right_by_key = BTreeMap::<Vec<(String, String)>, BinaryVectorEntry>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for group_left binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
            continue;
        };
        let Some(value) =
            evaluate_binary_vector_value(expression, comparison, left.value, right.value)
        else {
            continue;
        };
        let labels = binary_vector_group_output_labels(
            &left.labels,
            &right.labels,
            matching,
            comparison,
            bool_comparison,
        );
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_left binary vector matching".to_string(),
            ));
        }
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

fn evaluate_binary_vector_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryVectorEntry>,
    right_entries: Vec<BinaryVectorEntry>,
    eval_time_ms: u64,
    comparison: bool,
    bool_comparison: bool,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_right vector matching metadata".to_string())
    })?;
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, BinaryVectorEntry>::new();
    for entry in left_entries {
        if left_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for group_right binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for right in right_entries {
        let Some(left) = left_by_key.get(&right.key) else {
            continue;
        };
        let Some(value) =
            evaluate_binary_vector_value(expression, comparison, left.value, right.value)
        else {
            continue;
        };
        let labels = binary_vector_group_output_labels(
            &right.labels,
            &left.labels,
            matching,
            comparison,
            bool_comparison,
        );
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_right binary vector matching".to_string(),
            ));
        }
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    Ok(merge_query_results(out))
}

fn evaluate_binary_vector_value(
    expression: &PromqlBinaryExpression,
    comparison: bool,
    left: f64,
    right: f64,
) -> Option<f64> {
    if comparison {
        let matched = compare_binary_operator(expression.op, left, right);
        if expression.return_bool {
            Some(if matched { 1.0 } else { 0.0 })
        } else if matched {
            Some(left)
        } else {
            None
        }
    } else {
        Some(apply_binary_operator(expression.op, left, right))
    }
}

pub(super) fn evaluate_binary_vector_set(
    expression: &PromqlBinaryExpression,
    left_results: Vec<SegmentQueryResult>,
    right_results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let mut left_entries = Vec::<(Vec<(String, String)>, Vec<(String, String)>, f64)>::new();
    let mut left_keys = BTreeSet::<Vec<(String, String)>>::new();
    for result in left_results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let key = binary_vector_set_match_labels(result.labels.as_ref(), expression);
        left_keys.insert(key.clone());
        left_entries.push((key, result.labels.as_ref().to_vec(), value));
    }

    let mut right_entries = Vec::<(Vec<(String, String)>, Vec<(String, String)>, f64)>::new();
    let mut right_keys = BTreeSet::<Vec<(String, String)>>::new();
    for result in right_results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let key = binary_vector_set_match_labels(result.labels.as_ref(), expression);
        right_keys.insert(key.clone());
        right_entries.push((key, result.labels.as_ref().to_vec(), value));
    }

    let mut out = Vec::new();
    match expression.op {
        PromqlBinaryOp::And => {
            for (key, labels, value) in left_entries {
                if right_keys.contains(&key) {
                    push_instant_result(&mut out, labels, value, eval_time_ms);
                }
            }
        }
        PromqlBinaryOp::Or => {
            for (_, labels, value) in left_entries {
                push_instant_result(&mut out, labels, value, eval_time_ms);
            }
            for (key, labels, value) in right_entries {
                if !left_keys.contains(&key) {
                    push_instant_result(&mut out, labels, value, eval_time_ms);
                }
            }
        }
        PromqlBinaryOp::Unless => {
            for (key, labels, value) in left_entries {
                if !right_keys.contains(&key) {
                    push_instant_result(&mut out, labels, value, eval_time_ms);
                }
            }
        }
        _ => {
            return Err(PromqlQueryError::Invalid(
                "non-set operator used for binary set evaluation".to_string(),
            ));
        }
    }
    Ok(merge_query_results(out))
}

fn binary_vector_set_match_labels(
    labels: &[(String, String)],
    expression: &PromqlBinaryExpression,
) -> Vec<(String, String)> {
    match expression.vector_matching.as_ref() {
        Some(matching) => binary_vector_match_labels(labels, Some(matching)),
        None => binary_vector_match_labels(labels, None),
    }
}

fn binary_vector_match_labels(
    labels: &[(String, String)],
    matching: Option<&PromqlVectorMatching>,
) -> Vec<(String, String)> {
    let mut labels = match matching {
        None => function_result_labels(labels),
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: matching_labels,
            ..
        }) => labels
            .iter()
            .filter(|(key, _)| matching_labels.iter().any(|label| label == key))
            .cloned()
            .collect(),
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::Ignoring,
            labels: matching_labels,
            ..
        }) => labels
            .iter()
            .filter(|(key, _)| {
                key != METRIC_NAME_LABEL && !matching_labels.iter().any(|label| label == key)
            })
            .cloned()
            .collect(),
    };
    labels.sort();
    labels
}

fn push_instant_result(
    out: &mut Vec<SegmentQueryResult>,
    labels: Vec<(String, String)>,
    value: f64,
    eval_time_ms: u64,
) {
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, value);
    out.push(result);
}

pub(super) fn evaluate_binary_scalar_scalar(
    op: PromqlBinaryOp,
    left: f64,
    right: f64,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_scalar(apply_binary_operator(op, left, right), eval_time_ms)
}

pub(super) fn evaluate_scalar(value: f64, eval_time_ms: u64) -> Vec<SegmentQueryResult> {
    let labels = Vec::new();
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, value);
    vec![result]
}

pub(super) fn scalar_expression_value(query: &PromqlQuery, eval_time_ms: u64) -> Option<f64> {
    match query {
        PromqlQuery::Scalar(value) => Some(*value),
        PromqlQuery::Time => Some(eval_time_ms as f64 / 1000.0),
        PromqlQuery::BinaryExpression(expression) => {
            if binary_operator_is_set(expression.op) {
                return None;
            }
            let left = scalar_expression_value(&expression.left, eval_time_ms)?;
            let right = scalar_expression_value(&expression.right, eval_time_ms)?;
            Some(apply_binary_operator(expression.op, left, right))
        }
        PromqlQuery::Vector(_)
        | PromqlQuery::VectorFunction(_)
        | PromqlQuery::ScalarFunction(_)
        | PromqlQuery::Offset(_)
        | PromqlQuery::LabelReplace(_)
        | PromqlQuery::LabelJoin(_)
        | PromqlQuery::RangeFunction(_)
        | PromqlQuery::QuantileOverTime(_)
        | PromqlQuery::PredictLinear(_)
        | PromqlQuery::DoubleExponentialSmoothing(_)
        | PromqlQuery::Aggregation(_)
        | PromqlQuery::Absent(_)
        | PromqlQuery::AbsentOverTime(_)
        | PromqlQuery::InstantFunction(_)
        | PromqlQuery::HistogramQuantile(_)
        | PromqlQuery::HistogramFraction(_)
        | PromqlQuery::HistogramScalarFunction(_) => None,
    }
}

pub(super) fn is_scalar_expression(query: &PromqlQuery) -> bool {
    match query {
        PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::ScalarFunction(_) => true,
        PromqlQuery::BinaryExpression(expression)
            if !binary_operator_is_set(expression.op)
                && !expression.return_bool
                && expression.vector_matching.is_none() =>
        {
            is_scalar_expression(&expression.left) && is_scalar_expression(&expression.right)
        }
        _ => false,
    }
}

pub(super) fn scalar_query_result_value(
    results: &[SegmentQueryResult],
) -> Result<f64, PromqlQueryError> {
    let [result] = results else {
        return Err(PromqlQueryError::Invalid(format!(
            "scalar expression evaluated to {} series",
            results.len()
        )));
    };
    result
        .samples
        .last()
        .map(|(_, value)| *value)
        .ok_or_else(|| {
            PromqlQueryError::Invalid("scalar expression returned no sample".to_string())
        })
}

pub(super) fn binary_expression_vector_sides(
    expression: &PromqlBinaryExpression,
) -> Vec<&PromqlQuery> {
    let mut sides = Vec::with_capacity(2);
    if scalar_expression_value(&expression.left, 0).is_none() {
        sides.push(expression.left.as_ref());
    }
    if scalar_expression_value(&expression.right, 0).is_none() {
        sides.push(expression.right.as_ref());
    }
    sides
}

pub(super) fn offset_eval_time_ms(eval_time_ms: u64, offset_ms: i128) -> u64 {
    let shifted = i128::from(eval_time_ms).saturating_sub(offset_ms);
    shifted.clamp(0, i128::from(u64::MAX)) as u64
}

pub(super) fn retimestamp_instant_results(
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut out = Vec::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let mut shifted = SegmentQueryResult::with_shared_labels(result.series_id, result.labels);
        shifted.push_sample(eval_time_ms, value);
        out.push(shifted);
    }
    merge_query_results(out)
}

pub(super) fn validate_promql_range_bounds(
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
) -> Result<(), PromqlQueryError> {
    if step_ms == 0 {
        return Err(PromqlQueryError::Invalid(
            "query_range step_ms must be greater than zero".to_string(),
        ));
    }
    if end_ms < start_ms {
        return Err(PromqlQueryError::Invalid(
            "query_range end_ms must be greater than or equal to start_ms".to_string(),
        ));
    }
    Ok(())
}

fn apply_binary_operator(op: PromqlBinaryOp, left: f64, right: f64) -> f64 {
    match op {
        PromqlBinaryOp::Add => left + right,
        PromqlBinaryOp::Sub => left - right,
        PromqlBinaryOp::Mul => left * right,
        PromqlBinaryOp::Div => left / right,
        PromqlBinaryOp::Mod => left % right,
        PromqlBinaryOp::Pow => left.powf(right),
        PromqlBinaryOp::Eq
        | PromqlBinaryOp::NotEq
        | PromqlBinaryOp::Gt
        | PromqlBinaryOp::Gte
        | PromqlBinaryOp::Lt
        | PromqlBinaryOp::Lte => {
            if compare_binary_operator(op, left, right) {
                1.0
            } else {
                0.0
            }
        }
        PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless => f64::NAN,
    }
}

pub(super) fn binary_operator_is_set(op: PromqlBinaryOp) -> bool {
    matches!(
        op,
        PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless
    )
}

fn binary_operator_is_comparison(op: PromqlBinaryOp) -> bool {
    matches!(
        op,
        PromqlBinaryOp::Eq
            | PromqlBinaryOp::NotEq
            | PromqlBinaryOp::Gt
            | PromqlBinaryOp::Gte
            | PromqlBinaryOp::Lt
            | PromqlBinaryOp::Lte
    )
}

fn compare_binary_operator(op: PromqlBinaryOp, left: f64, right: f64) -> bool {
    match op {
        PromqlBinaryOp::Eq => left == right,
        PromqlBinaryOp::NotEq => left != right,
        PromqlBinaryOp::Gt => left > right,
        PromqlBinaryOp::Gte => left >= right,
        PromqlBinaryOp::Lt => left < right,
        PromqlBinaryOp::Lte => left <= right,
        PromqlBinaryOp::Add
        | PromqlBinaryOp::Sub
        | PromqlBinaryOp::Mul
        | PromqlBinaryOp::Div
        | PromqlBinaryOp::Mod
        | PromqlBinaryOp::Pow => false,
        PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless => false,
    }
}

#[derive(Default)]
struct AggregationAccumulator {
    sum: f64,
    count: u64,
    finite_count: u64,
    nan_count: u64,
    positive_infinity_count: u64,
    negative_infinity_count: u64,
    mean: f64,
    m2: f64,
    min: Option<f64>,
    max: Option<f64>,
    values: Vec<f64>,
}

impl AggregationAccumulator {
    fn observe(&mut self, value: f64, collect_values: bool) {
        self.sum += value;
        self.count = self.count.saturating_add(1);
        if value.is_nan() {
            self.nan_count = self.nan_count.saturating_add(1);
        } else if value == f64::INFINITY {
            self.positive_infinity_count = self.positive_infinity_count.saturating_add(1);
        } else if value == f64::NEG_INFINITY {
            self.negative_infinity_count = self.negative_infinity_count.saturating_add(1);
        } else {
            self.finite_count = self.finite_count.saturating_add(1);
            let count = self.finite_count as f64;
            let delta = value - self.mean;
            self.mean += delta / count;
            let delta2 = value - self.mean;
            self.m2 += delta * delta2;
        }
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
        if collect_values {
            self.values.push(value);
        }
    }

    fn value(&self, op: &PromqlAggregationOp) -> Option<f64> {
        match op {
            PromqlAggregationOp::Sum => (self.count > 0).then_some(self.sum),
            PromqlAggregationOp::Count => (self.count > 0).then_some(self.count as f64),
            PromqlAggregationOp::Avg => self.avg_value(),
            PromqlAggregationOp::Min => self.min,
            PromqlAggregationOp::Max => self.max,
            PromqlAggregationOp::Stddev => self.stdvar_value().map(|value| value.sqrt()),
            PromqlAggregationOp::Stdvar => self.stdvar_value(),
            PromqlAggregationOp::Group => (self.count > 0).then_some(1.0),
            PromqlAggregationOp::Quantile(quantile) => {
                let mut values = self.values.clone();
                Some(vector_quantile(*quantile, &mut values))
            }
            PromqlAggregationOp::TopK(_)
            | PromqlAggregationOp::BottomK(_)
            | PromqlAggregationOp::CountValues(_) => None,
        }
    }

    fn avg_value(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        if self.nan_count > 0
            || (self.positive_infinity_count > 0 && self.negative_infinity_count > 0)
        {
            return Some(f64::NAN);
        }
        if self.positive_infinity_count > 0 {
            return Some(f64::INFINITY);
        }
        if self.negative_infinity_count > 0 {
            return Some(f64::NEG_INFINITY);
        }
        Some(self.mean)
    }

    fn stdvar_value(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        if self.finite_count != self.count {
            return Some(f64::NAN);
        }
        Some((self.m2 / self.finite_count as f64).max(0.0))
    }
}

fn vector_quantile(quantile: f64, values: &mut [f64]) -> f64 {
    if values.is_empty() || quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }

    values.sort_by(quantile_value_order);

    let n = values.len() as f64;
    let rank = quantile * (n - 1.0);
    let lower_index = rank.floor().max(0.0);
    let upper_index = (lower_index + 1.0).min(n - 1.0);
    let weight = rank - rank.floor();
    values[lower_index as usize] * (1.0 - weight) + values[upper_index as usize] * weight
}

fn quantile_value_order(left: &f64, right: &f64) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => left.total_cmp(right),
    }
}
