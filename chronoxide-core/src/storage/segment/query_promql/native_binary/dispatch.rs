use std::collections::BTreeSet;

use crate::promql::{
    PromqlBinaryExpression, PromqlBinaryOp, PromqlQueryError, PromqlVectorMatchingCardinality,
};

use super::super::{
    PromqlExponentialHistogramSeries, PromqlHistogramSeries, SegmentQueryResult,
    binary_vector_matching_cardinality, merge_exponential_histogram_query_results,
    merge_histogram_query_results,
};
use super::classic::{
    binary_histogram_entries, evaluate_histogram_binary_bool_many_to_one,
    evaluate_histogram_binary_bool_one_to_many, evaluate_histogram_binary_bool_one_to_one,
    evaluate_histogram_binary_many_to_one, evaluate_histogram_binary_one_to_many,
    evaluate_histogram_binary_one_to_one, push_histogram_set_result, scale_histogram_sample,
};
use super::exponential::{
    binary_exponential_histogram_entries, evaluate_exponential_histogram_binary_bool_many_to_one,
    evaluate_exponential_histogram_binary_bool_one_to_many,
    evaluate_exponential_histogram_binary_bool_one_to_one,
    evaluate_exponential_histogram_binary_many_to_one,
    evaluate_exponential_histogram_binary_one_to_many,
    evaluate_exponential_histogram_binary_one_to_one, push_exponential_histogram_set_result,
    scale_exponential_histogram_sample,
};
use super::mixed::{
    evaluate_exponential_histogram_mixed_binary_bool_many_to_one,
    evaluate_exponential_histogram_mixed_binary_bool_one_to_many,
    evaluate_exponential_histogram_mixed_binary_bool_one_to_one,
    evaluate_exponential_histogram_mixed_binary_many_to_one,
    evaluate_exponential_histogram_mixed_binary_one_to_many,
    evaluate_exponential_histogram_mixed_binary_one_to_one,
    evaluate_histogram_exponential_mixed_binary_bool_many_to_one,
    evaluate_histogram_exponential_mixed_binary_bool_one_to_many,
    evaluate_histogram_exponential_mixed_binary_bool_one_to_one,
    evaluate_histogram_exponential_mixed_binary_many_to_one,
    evaluate_histogram_exponential_mixed_binary_one_to_many,
    evaluate_histogram_exponential_mixed_binary_one_to_one, mixed_native_histogram_equality_op,
};
use super::shared::histogram_scalar_binary_scale;

pub(in crate::storage::segment) fn evaluate_native_histogram_binary_vector_scalar(
    expression: &PromqlBinaryExpression,
    mut series: Vec<PromqlHistogramSeries>,
    scalar: f64,
    scalar_on_left: bool,
) -> Vec<PromqlHistogramSeries> {
    let Some(scale) = histogram_scalar_binary_scale(expression.op, scalar, scalar_on_left) else {
        return Vec::new();
    };

    for result in &mut series {
        for sample in &mut result.samples {
            scale_histogram_sample(sample, scale);
        }
    }
    merge_histogram_query_results(series)
}

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_binary_vector_scalar(
    expression: &PromqlBinaryExpression,
    mut series: Vec<PromqlExponentialHistogramSeries>,
    scalar: f64,
    scalar_on_left: bool,
) -> Vec<PromqlExponentialHistogramSeries> {
    let Some(scale) = histogram_scalar_binary_scale(expression.op, scalar, scalar_on_left) else {
        return Vec::new();
    };

    for result in &mut series {
        for sample in &mut result.samples {
            scale_exponential_histogram_sample(sample, scale);
        }
    }
    merge_exponential_histogram_query_results(series)
}

pub(in crate::storage::segment) fn evaluate_native_histogram_combined_vector_set(
    expression: &PromqlBinaryExpression,
    left_histogram_series: Vec<PromqlHistogramSeries>,
    right_histogram_series: Vec<PromqlHistogramSeries>,
    left_exponential_series: Vec<PromqlExponentialHistogramSeries>,
    right_exponential_series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    let left_histogram_entries =
        binary_histogram_entries(left_histogram_series, expression.vector_matching.as_ref());
    let right_histogram_entries =
        binary_histogram_entries(right_histogram_series, expression.vector_matching.as_ref());
    let left_exponential_entries = binary_exponential_histogram_entries(
        left_exponential_series,
        expression.vector_matching.as_ref(),
    );
    let right_exponential_entries = binary_exponential_histogram_entries(
        right_exponential_series,
        expression.vector_matching.as_ref(),
    );

    let mut left_keys = BTreeSet::<Vec<(String, String)>>::new();
    for entry in &left_histogram_entries {
        left_keys.insert(entry.key.clone());
    }
    for entry in &left_exponential_entries {
        left_keys.insert(entry.key.clone());
    }

    let mut right_keys = BTreeSet::<Vec<(String, String)>>::new();
    for entry in &right_histogram_entries {
        right_keys.insert(entry.key.clone());
    }
    for entry in &right_exponential_entries {
        right_keys.insert(entry.key.clone());
    }

    let mut out = Vec::new();
    match expression.op {
        PromqlBinaryOp::And => {
            for entry in left_histogram_entries {
                if right_keys.contains(&entry.key) {
                    push_histogram_set_result(&mut out, entry.labels, entry.sample, eval_time_ms);
                }
            }
        }
        PromqlBinaryOp::Or => {
            for entry in left_histogram_entries {
                push_histogram_set_result(&mut out, entry.labels, entry.sample, eval_time_ms);
            }
            for entry in right_histogram_entries {
                if !left_keys.contains(&entry.key) {
                    push_histogram_set_result(&mut out, entry.labels, entry.sample, eval_time_ms);
                }
            }
        }
        PromqlBinaryOp::Unless => {
            for entry in left_histogram_entries {
                if !right_keys.contains(&entry.key) {
                    push_histogram_set_result(&mut out, entry.labels, entry.sample, eval_time_ms);
                }
            }
        }
        _ => {
            return Err(PromqlQueryError::Invalid(
                "non-set operator used for combined native histogram set evaluation".to_string(),
            ));
        }
    }
    Ok(merge_histogram_query_results(out))
}

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_combined_vector_set(
    expression: &PromqlBinaryExpression,
    left_exponential_series: Vec<PromqlExponentialHistogramSeries>,
    right_exponential_series: Vec<PromqlExponentialHistogramSeries>,
    left_histogram_series: Vec<PromqlHistogramSeries>,
    right_histogram_series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    let left_exponential_entries = binary_exponential_histogram_entries(
        left_exponential_series,
        expression.vector_matching.as_ref(),
    );
    let right_exponential_entries = binary_exponential_histogram_entries(
        right_exponential_series,
        expression.vector_matching.as_ref(),
    );
    let left_histogram_entries =
        binary_histogram_entries(left_histogram_series, expression.vector_matching.as_ref());
    let right_histogram_entries =
        binary_histogram_entries(right_histogram_series, expression.vector_matching.as_ref());

    let mut left_keys = BTreeSet::<Vec<(String, String)>>::new();
    for entry in &left_histogram_entries {
        left_keys.insert(entry.key.clone());
    }
    for entry in &left_exponential_entries {
        left_keys.insert(entry.key.clone());
    }

    let mut right_keys = BTreeSet::<Vec<(String, String)>>::new();
    for entry in &right_histogram_entries {
        right_keys.insert(entry.key.clone());
    }
    for entry in &right_exponential_entries {
        right_keys.insert(entry.key.clone());
    }

    let mut out = Vec::new();
    match expression.op {
        PromqlBinaryOp::And => {
            for entry in left_exponential_entries {
                if right_keys.contains(&entry.key) {
                    push_exponential_histogram_set_result(
                        &mut out,
                        entry.labels,
                        entry.sample,
                        eval_time_ms,
                    );
                }
            }
        }
        PromqlBinaryOp::Or => {
            for entry in left_exponential_entries {
                push_exponential_histogram_set_result(
                    &mut out,
                    entry.labels,
                    entry.sample,
                    eval_time_ms,
                );
            }
            for entry in right_exponential_entries {
                if !left_keys.contains(&entry.key) {
                    push_exponential_histogram_set_result(
                        &mut out,
                        entry.labels,
                        entry.sample,
                        eval_time_ms,
                    );
                }
            }
        }
        PromqlBinaryOp::Unless => {
            for entry in left_exponential_entries {
                if !right_keys.contains(&entry.key) {
                    push_exponential_histogram_set_result(
                        &mut out,
                        entry.labels,
                        entry.sample,
                        eval_time_ms,
                    );
                }
            }
        }
        _ => {
            return Err(PromqlQueryError::Invalid(
                "non-set operator used for combined native exponential histogram set evaluation"
                    .to_string(),
            ));
        }
    }
    Ok(merge_exponential_histogram_query_results(out))
}

pub(in crate::storage::segment) fn evaluate_native_histogram_binary_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlHistogramSeries>,
    right_series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    let left_entries = binary_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries = binary_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => evaluate_histogram_binary_one_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
        ),
        PromqlVectorMatchingCardinality::ManyToOne => evaluate_histogram_binary_many_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
        ),
        PromqlVectorMatchingCardinality::OneToMany => evaluate_histogram_binary_one_to_many(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
        ),
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_binary_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlExponentialHistogramSeries>,
    right_series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    let left_entries =
        binary_exponential_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries =
        binary_exponential_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => {
            evaluate_exponential_histogram_binary_one_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToOne => {
            evaluate_exponential_histogram_binary_many_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::OneToMany => {
            evaluate_exponential_histogram_binary_one_to_many(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

pub(in crate::storage::segment) fn evaluate_native_histogram_binary_bool_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlHistogramSeries>,
    right_series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let left_entries = binary_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries = binary_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => evaluate_histogram_binary_bool_one_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
        ),
        PromqlVectorMatchingCardinality::ManyToOne => evaluate_histogram_binary_bool_many_to_one(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
        ),
        PromqlVectorMatchingCardinality::OneToMany => evaluate_histogram_binary_bool_one_to_many(
            expression,
            left_entries,
            right_entries,
            eval_time_ms,
        ),
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_binary_bool_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlExponentialHistogramSeries>,
    right_series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    let left_entries =
        binary_exponential_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries =
        binary_exponential_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => {
            evaluate_exponential_histogram_binary_bool_one_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToOne => {
            evaluate_exponential_histogram_binary_bool_many_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::OneToMany => {
            evaluate_exponential_histogram_binary_bool_one_to_many(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

pub(in crate::storage::segment) fn evaluate_native_histogram_mixed_binary_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlHistogramSeries>,
    right_series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlHistogramSeries>, PromqlQueryError> {
    if expression.return_bool || !mixed_native_histogram_equality_op(expression.op) {
        return Ok(Vec::new());
    }

    let left_entries = binary_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries =
        binary_exponential_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => {
            evaluate_histogram_exponential_mixed_binary_one_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToOne => {
            evaluate_histogram_exponential_mixed_binary_many_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::OneToMany => {
            evaluate_histogram_exponential_mixed_binary_one_to_many(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_mixed_binary_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlExponentialHistogramSeries>,
    right_series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    if expression.return_bool || !mixed_native_histogram_equality_op(expression.op) {
        return Ok(Vec::new());
    }

    let left_entries =
        binary_exponential_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries = binary_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => {
            evaluate_exponential_histogram_mixed_binary_one_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToOne => {
            evaluate_exponential_histogram_mixed_binary_many_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::OneToMany => {
            evaluate_exponential_histogram_mixed_binary_one_to_many(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

pub(in crate::storage::segment) fn evaluate_native_histogram_mixed_binary_bool_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlHistogramSeries>,
    right_series: Vec<PromqlExponentialHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    if !expression.return_bool || !mixed_native_histogram_equality_op(expression.op) {
        return Ok(Vec::new());
    }

    let left_entries = binary_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries =
        binary_exponential_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => {
            evaluate_histogram_exponential_mixed_binary_bool_one_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToOne => {
            evaluate_histogram_exponential_mixed_binary_bool_many_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::OneToMany => {
            evaluate_histogram_exponential_mixed_binary_bool_one_to_many(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

pub(in crate::storage::segment) fn evaluate_native_exponential_histogram_mixed_binary_bool_vector_vector(
    expression: &PromqlBinaryExpression,
    left_series: Vec<PromqlExponentialHistogramSeries>,
    right_series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
    if !expression.return_bool || !mixed_native_histogram_equality_op(expression.op) {
        return Ok(Vec::new());
    }

    let left_entries =
        binary_exponential_histogram_entries(left_series, expression.vector_matching.as_ref());
    let right_entries = binary_histogram_entries(right_series, expression.vector_matching.as_ref());

    match binary_vector_matching_cardinality(expression) {
        PromqlVectorMatchingCardinality::OneToOne => {
            evaluate_exponential_histogram_mixed_binary_bool_one_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToOne => {
            evaluate_exponential_histogram_mixed_binary_bool_many_to_one(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::OneToMany => {
            evaluate_exponential_histogram_mixed_binary_bool_one_to_many(
                expression,
                left_entries,
                right_entries,
                eval_time_ms,
            )
        }
        PromqlVectorMatchingCardinality::ManyToMany => Err(PromqlQueryError::Invalid(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}
