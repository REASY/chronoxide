use super::*;
use crate::promql::{
    PromqlInstantFunction, PromqlInstantFunctionKind, PromqlLabelJoin, PromqlLabelReplace,
};
use chrono::{Datelike, TimeZone, Timelike, Utc};

pub(in crate::storage::segment) fn evaluate_absent(
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

pub(in crate::storage::segment) fn evaluate_absent_over_time(
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

pub(in crate::storage::segment) fn evaluate_instant_function(
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

pub(in crate::storage::segment) fn evaluate_scalar_function(
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
        let labels = function_result_labels(&result.labels);
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
        let labels = function_result_labels(&result.labels);
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, timestamp_ms as f64 / 1000.0);
        out.push(result);
    }
    merge_query_results(out)
}

pub(in crate::storage::segment) fn evaluate_label_replace(
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
        let mut labels = result.labels.to_vec();
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

pub(in crate::storage::segment) fn evaluate_label_join(
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
        let mut labels = result.labels.to_vec();
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

pub(in crate::storage::segment) fn evaluate_scalar(
    value: f64,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let labels = Vec::new();
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, value);
    vec![result]
}

pub(in crate::storage::segment) fn evaluate_promql_vector_function(
    function: &PromqlVectorFunction,
    end_ms: u64,
) -> Result<QueryExecution, PromqlQueryError> {
    let Some(value) = scalar_expression_value(&function.input, end_ms) else {
        return Err(PromqlQueryError::Invalid(
            "vector() requires a scalar expression".to_string(),
        ));
    };
    Ok(QueryExecution {
        results: evaluate_scalar(value, end_ms),
        stats: QueryStats::default(),
    })
}

pub(in crate::storage::segment) fn scalar_expression_value(
    query: &PromqlQuery,
    eval_time_ms: u64,
) -> Option<f64> {
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

pub(in crate::storage::segment) fn is_scalar_expression(query: &PromqlQuery) -> bool {
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

pub(in crate::storage::segment) fn scalar_query_result_value(
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

pub(in crate::storage::segment) fn binary_expression_vector_sides(
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

pub(in crate::storage::segment) fn offset_eval_time_ms(eval_time_ms: u64, offset_ms: i128) -> u64 {
    let shifted = i128::from(eval_time_ms).saturating_sub(offset_ms);
    shifted.clamp(0, i128::from(u64::MAX)) as u64
}

pub(in crate::storage::segment) fn retimestamp_instant_results(
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

pub(in crate::storage::segment) fn validate_promql_range_bounds(
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
