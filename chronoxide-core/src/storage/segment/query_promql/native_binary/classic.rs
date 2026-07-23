use std::collections::{BTreeMap, BTreeSet};

use crate::promql::{
    PromqlBinaryExpression, PromqlBinaryOp, PromqlQueryError, PromqlVectorMatching,
};
use crate::storage::head::{CounterResetHint, OtlpAggregationTemporality};

use super::super::{
    PromqlHistogramSample, PromqlHistogramSeries, SegmentQueryResult, add_custom_histogram_buckets,
    binary_operator_is_comparison, binary_vector_group_output_labels, binary_vector_match_labels,
    binary_vector_output_labels, merge_histogram_query_results, merge_query_results,
    push_instant_result, segment_series_id, shared_query_labels,
};

pub(super) fn scale_histogram_sample(sample: &mut PromqlHistogramSample, scale: f64) {
    sample.count *= scale;
    if let Some(sum) = &mut sample.sum {
        *sum *= scale;
    }
    for count in &mut sample.bucket_counts {
        *count *= scale;
    }
}

pub(super) fn evaluate_histogram_binary_bool_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let mut left_by_key =
        BTreeMap::<Vec<(String, String)>, (Vec<(String, String)>, PromqlHistogramSample)>::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            true,
            true,
        );
        if left_by_key
            .insert(entry.key.clone(), (labels, entry.sample))
            .is_some()
        {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, PromqlHistogramSample>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, entry.sample).is_some() {
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
        let Some(value) = evaluate_histogram_binary_bool_value(expression, &left, right) else {
            continue;
        };
        push_instant_result(&mut out, labels, value, eval_time_ms);
    }
    Ok(merge_query_results(out))
}

pub(super) fn evaluate_histogram_binary_bool_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_left vector matching metadata".to_string())
    })?;
    let mut right_by_key = BTreeMap::<Vec<(String, String)>, BinaryHistogramEntry>::new();
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
            evaluate_histogram_binary_bool_value(expression, &left.sample, &right.sample)
        else {
            continue;
        };
        let labels =
            binary_vector_group_output_labels(&left.labels, &right.labels, matching, true, true);
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_left binary vector matching".to_string(),
            ));
        }
        push_instant_result(&mut out, labels, value, eval_time_ms);
    }
    Ok(merge_query_results(out))
}

pub(super) fn evaluate_histogram_binary_bool_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_right vector matching metadata".to_string())
    })?;
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, BinaryHistogramEntry>::new();
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
            evaluate_histogram_binary_bool_value(expression, &left.sample, &right.sample)
        else {
            continue;
        };
        let labels =
            binary_vector_group_output_labels(&right.labels, &left.labels, matching, true, true);
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_right binary vector matching".to_string(),
            ));
        }
        push_instant_result(&mut out, labels, value, eval_time_ms);
    }
    Ok(merge_query_results(out))
}

fn evaluate_histogram_binary_bool_value(
    expression: &PromqlBinaryExpression,
    left: &PromqlHistogramSample,
    right: &PromqlHistogramSample,
) -> Option<f64> {
    if !expression.return_bool {
        return None;
    }
    let equal = histogram_samples_equal(left, right);
    match expression.op {
        PromqlBinaryOp::Eq => Some(if equal { 1.0 } else { 0.0 }),
        PromqlBinaryOp::NotEq => Some(if equal { 0.0 } else { 1.0 }),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub(super) struct BinaryHistogramEntry {
    pub(super) labels: Vec<(String, String)>,
    pub(super) key: Vec<(String, String)>,
    pub(super) sample: PromqlHistogramSample,
}

pub(super) fn binary_histogram_entries(
    results: Vec<PromqlHistogramSeries>,
    matching: Option<&PromqlVectorMatching>,
) -> Vec<BinaryHistogramEntry> {
    let mut out = Vec::new();
    for result in results {
        let Some(sample) = result.samples.last().cloned() else {
            continue;
        };
        if sample.stale {
            continue;
        }
        let labels = result.labels.to_vec();
        out.push(BinaryHistogramEntry {
            key: binary_vector_match_labels(&labels, matching),
            labels,
            sample,
        });
    }
    out
}

pub(super) fn evaluate_histogram_binary_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;
    let mut left_by_key =
        BTreeMap::<Vec<(String, String)>, (Vec<(String, String)>, PromqlHistogramSample)>::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            comparison,
            bool_comparison,
        );
        if left_by_key
            .insert(entry.key.clone(), (labels, entry.sample))
            .is_some()
        {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, PromqlHistogramSample>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, entry.sample).is_some() {
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
        let Some(sample) = evaluate_histogram_binary_sample(expression, &left, right, eval_time_ms)
        else {
            continue;
        };
        let mut result =
            PromqlHistogramSeries::new(segment_series_id(&labels), shared_query_labels(labels));
        result.push_sample(sample);
        out.push(result);
    }
    Ok(merge_histogram_query_results(out))
}

pub(super) fn push_histogram_set_result(
    out: &mut Vec<PromqlHistogramSeries>,
    labels: Vec<(String, String)>,
    mut sample: PromqlHistogramSample,
    eval_time_ms: u64,
) {
    sample.timestamp_ms = eval_time_ms;
    let mut result =
        PromqlHistogramSeries::new(segment_series_id(&labels), shared_query_labels(labels));
    result.push_sample(sample);
    out.push(result);
}

pub(super) fn evaluate_histogram_binary_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_left vector matching metadata".to_string())
    })?;
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;
    let mut right_by_key = BTreeMap::<Vec<(String, String)>, BinaryHistogramEntry>::new();
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
        let Some(sample) =
            evaluate_histogram_binary_sample(expression, &left.sample, &right.sample, eval_time_ms)
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
        let mut result =
            PromqlHistogramSeries::new(segment_series_id(&labels), shared_query_labels(labels));
        result.push_sample(sample);
        out.push(result);
    }
    Ok(merge_histogram_query_results(out))
}

pub(super) fn evaluate_histogram_binary_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_right vector matching metadata".to_string())
    })?;
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, BinaryHistogramEntry>::new();
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
        let Some(sample) =
            evaluate_histogram_binary_sample(expression, &left.sample, &right.sample, eval_time_ms)
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
        let mut result =
            PromqlHistogramSeries::new(segment_series_id(&labels), shared_query_labels(labels));
        result.push_sample(sample);
        out.push(result);
    }
    Ok(merge_histogram_query_results(out))
}

fn evaluate_histogram_binary_sample(
    expression: &PromqlBinaryExpression,
    left: &PromqlHistogramSample,
    right: &PromqlHistogramSample,
    eval_time_ms: u64,
) -> Option<PromqlHistogramSample> {
    if expression.return_bool {
        return None;
    }

    match expression.op {
        PromqlBinaryOp::Add | PromqlBinaryOp::Sub => {
            combine_histogram_samples(left, right, expression.op, eval_time_ms)
        }
        PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq => {
            let equal = histogram_samples_equal(left, right);
            let matched = match expression.op {
                PromqlBinaryOp::Eq => equal,
                PromqlBinaryOp::NotEq => !equal,
                _ => unreachable!(),
            };
            matched.then(|| {
                let mut sample = left.clone();
                sample.timestamp_ms = eval_time_ms;
                sample
            })
        }
        _ => None,
    }
}

fn combine_histogram_samples(
    left: &PromqlHistogramSample,
    right: &PromqlHistogramSample,
    op: PromqlBinaryOp,
    timestamp_ms: u64,
) -> Option<PromqlHistogramSample> {
    if left.stale || right.stale || !left.count.is_finite() || !right.count.is_finite() {
        return None;
    }

    let scale = match op {
        PromqlBinaryOp::Add => 1.0,
        PromqlBinaryOp::Sub => -1.0,
        _ => return None,
    };

    let mut explicit_bounds = None;
    let mut bucket_counts = Vec::new();
    if !add_custom_histogram_buckets(
        &mut explicit_bounds,
        &mut bucket_counts,
        &left.explicit_bounds,
        &left.bucket_counts,
    ) {
        return None;
    }
    let mut scaled_right_counts = right.bucket_counts.clone();
    for count in &mut scaled_right_counts {
        *count *= scale;
    }
    if !add_custom_histogram_buckets(
        &mut explicit_bounds,
        &mut bucket_counts,
        &right.explicit_bounds,
        &scaled_right_counts,
    ) {
        return None;
    }

    Some(PromqlHistogramSample {
        timestamp_ms,
        start_time_ms: None,
        count: left.count + (right.count * scale),
        sum: match (left.sum, right.sum) {
            (Some(left_sum), Some(right_sum)) => Some(left_sum + (right_sum * scale)),
            _ => None,
        },
        explicit_bounds: explicit_bounds?,
        bucket_counts,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::GaugeType,
        stale: false,
    })
}

fn histogram_samples_equal(left: &PromqlHistogramSample, right: &PromqlHistogramSample) -> bool {
    !left.stale
        && !right.stale
        && left.count == right.count
        && left.sum == right.sum
        && left.explicit_bounds.as_ref() == right.explicit_bounds.as_ref()
        && left.bucket_counts == right.bucket_counts
}
