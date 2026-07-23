use std::collections::{BTreeMap, BTreeSet};

use crate::promql::{PromqlBinaryExpression, PromqlBinaryOp, PromqlQueryError};

use super::super::{
    PromqlExponentialHistogramSample, PromqlExponentialHistogramSeries, PromqlHistogramSample,
    PromqlHistogramSeries, SegmentQueryResult, binary_vector_group_output_labels,
    binary_vector_output_labels, merge_exponential_histogram_query_results,
    merge_histogram_query_results, merge_query_results, push_instant_result,
};
use super::classic::{BinaryHistogramEntry, push_histogram_set_result};
use super::exponential::{BinaryExponentialHistogramEntry, push_exponential_histogram_set_result};

pub(super) fn mixed_native_histogram_equality_op(op: PromqlBinaryOp) -> bool {
    matches!(op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq)
}

fn mixed_native_histogram_bool_value(expression: &PromqlBinaryExpression) -> Option<f64> {
    match expression.op {
        PromqlBinaryOp::Eq => Some(0.0),
        PromqlBinaryOp::NotEq => Some(1.0),
        _ => None,
    }
}

fn mixed_native_histogram_keep_left(expression: &PromqlBinaryExpression) -> bool {
    matches!(expression.op, PromqlBinaryOp::NotEq)
}

pub(super) fn evaluate_histogram_exponential_mixed_binary_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    let mut left_by_key =
        BTreeMap::<Vec<(String, String)>, (Vec<(String, String)>, PromqlHistogramSample)>::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            true,
            false,
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

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, ()>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, ()).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    if !mixed_native_histogram_keep_left(expression) {
        return Ok(out);
    }
    for (key, (labels, sample)) in left_by_key {
        if right_by_key.contains_key(&key) {
            push_histogram_set_result(&mut out, labels, sample, eval_time_ms);
        }
    }
    Ok(merge_histogram_query_results(out))
}

pub(super) fn evaluate_histogram_exponential_mixed_binary_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_left vector matching metadata".to_string())
    })?;
    let mut right_by_key =
        BTreeMap::<Vec<(String, String)>, BinaryExponentialHistogramEntry>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for group_left binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    if !mixed_native_histogram_keep_left(expression) {
        return Ok(out);
    }
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
            continue;
        };
        let labels =
            binary_vector_group_output_labels(&left.labels, &right.labels, matching, true, false);
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_left binary vector matching".to_string(),
            ));
        }
        push_histogram_set_result(&mut out, labels, left.sample, eval_time_ms);
    }
    Ok(merge_histogram_query_results(out))
}

pub(super) fn evaluate_histogram_exponential_mixed_binary_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
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
    if !mixed_native_histogram_keep_left(expression) {
        return Ok(out);
    }
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for right in right_entries {
        let Some(left) = left_by_key.get(&right.key) else {
            continue;
        };
        let labels =
            binary_vector_group_output_labels(&right.labels, &left.labels, matching, true, false);
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_right binary vector matching".to_string(),
            ));
        }
        push_histogram_set_result(&mut out, labels, left.sample.clone(), eval_time_ms);
    }
    Ok(merge_histogram_query_results(out))
}

pub(super) fn evaluate_exponential_histogram_mixed_binary_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    let mut left_by_key = BTreeMap::<
        Vec<(String, String)>,
        (Vec<(String, String)>, PromqlExponentialHistogramSample),
    >::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            true,
            false,
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

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, ()>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, ()).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    if !mixed_native_histogram_keep_left(expression) {
        return Ok(out);
    }
    for (key, (labels, sample)) in left_by_key {
        if right_by_key.contains_key(&key) {
            push_exponential_histogram_set_result(&mut out, labels, sample, eval_time_ms);
        }
    }
    Ok(merge_exponential_histogram_query_results(out))
}

pub(super) fn evaluate_exponential_histogram_mixed_binary_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
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
    if !mixed_native_histogram_keep_left(expression) {
        return Ok(out);
    }
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
            continue;
        };
        let labels =
            binary_vector_group_output_labels(&left.labels, &right.labels, matching, true, false);
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_left binary vector matching".to_string(),
            ));
        }
        push_exponential_histogram_set_result(&mut out, labels, left.sample, eval_time_ms);
    }
    Ok(merge_exponential_histogram_query_results(out))
}

pub(super) fn evaluate_exponential_histogram_mixed_binary_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_right vector matching metadata".to_string())
    })?;
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, BinaryExponentialHistogramEntry>::new();
    for entry in left_entries {
        if left_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for group_right binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    if !mixed_native_histogram_keep_left(expression) {
        return Ok(out);
    }
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for right in right_entries {
        let Some(left) = left_by_key.get(&right.key) else {
            continue;
        };
        let labels =
            binary_vector_group_output_labels(&right.labels, &left.labels, matching, true, false);
        if !output_labels.insert(labels.clone()) {
            return Err(PromqlQueryError::Invalid(
                "duplicate result series for group_right binary vector matching".to_string(),
            ));
        }
        push_exponential_histogram_set_result(&mut out, labels, left.sample.clone(), eval_time_ms);
    }
    Ok(merge_exponential_histogram_query_results(out))
}

pub(super) fn evaluate_histogram_exponential_mixed_binary_bool_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, Vec<(String, String)>>::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            true,
            true,
        );
        if left_by_key.insert(entry.key.clone(), labels).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, ()>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, ()).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let Some(value) = mixed_native_histogram_bool_value(expression) else {
        return Ok(out);
    };
    for (key, labels) in left_by_key {
        if right_by_key.contains_key(&key) {
            push_instant_result(&mut out, labels, value, eval_time_ms);
        }
    }
    Ok(merge_query_results(out))
}

pub(super) fn evaluate_histogram_exponential_mixed_binary_bool_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_left vector matching metadata".to_string())
    })?;
    let mut right_by_key =
        BTreeMap::<Vec<(String, String)>, BinaryExponentialHistogramEntry>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for group_left binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let Some(value) = mixed_native_histogram_bool_value(expression) else {
        return Ok(out);
    };
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
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

pub(super) fn evaluate_histogram_exponential_mixed_binary_bool_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
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
    let Some(value) = mixed_native_histogram_bool_value(expression) else {
        return Ok(out);
    };
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for right in right_entries {
        let Some(left) = left_by_key.get(&right.key) else {
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

pub(super) fn evaluate_exponential_histogram_mixed_binary_bool_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, Vec<(String, String)>>::new();
    for entry in left_entries {
        let labels = binary_vector_output_labels(
            &entry.labels,
            &[],
            expression.vector_matching.as_ref(),
            true,
            true,
        );
        if left_by_key.insert(entry.key.clone(), labels).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut right_by_key = BTreeMap::<Vec<(String, String)>, ()>::new();
    for entry in right_entries {
        if right_by_key.insert(entry.key, ()).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate right-hand series for binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let Some(value) = mixed_native_histogram_bool_value(expression) else {
        return Ok(out);
    };
    for (key, labels) in left_by_key {
        if right_by_key.contains_key(&key) {
            push_instant_result(&mut out, labels, value, eval_time_ms);
        }
    }
    Ok(merge_query_results(out))
}

pub(super) fn evaluate_exponential_histogram_mixed_binary_bool_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
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
    let Some(value) = mixed_native_histogram_bool_value(expression) else {
        return Ok(out);
    };
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
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

pub(super) fn evaluate_exponential_histogram_mixed_binary_bool_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_right vector matching metadata".to_string())
    })?;
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, BinaryExponentialHistogramEntry>::new();
    for entry in left_entries {
        if left_by_key.insert(entry.key.clone(), entry).is_some() {
            return Err(PromqlQueryError::Invalid(
                "duplicate left-hand series for group_right binary vector matching".to_string(),
            ));
        }
    }

    let mut out = Vec::new();
    let Some(value) = mixed_native_histogram_bool_value(expression) else {
        return Ok(out);
    };
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for right in right_entries {
        let Some(left) = left_by_key.get(&right.key) else {
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
