use super::*;

pub(super) fn is_prometheus_stale_marker(value: f64) -> bool {
    value.to_bits() == prometheus_stale_nan().to_bits()
}

pub(in crate::storage::segment) fn evaluate_binary_vector_scalar(
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
                (vector_value, result.labels.to_vec())
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

pub(in crate::storage::segment) fn evaluate_binary_vector_vector(
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
        let labels = result.labels.to_vec();
        out.push(BinaryVectorEntry {
            key: binary_vector_match_labels(&labels, matching),
            labels,
            value,
        });
    }
    out
}

pub(super) fn binary_vector_matching_cardinality(
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

pub(super) fn binary_vector_output_labels(
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

pub(super) fn binary_vector_group_output_labels(
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

pub(in crate::storage::segment) fn evaluate_binary_vector_set(
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
        let labels = result.labels.to_vec();
        let key = binary_vector_set_match_labels(&labels, expression);
        left_keys.insert(key.clone());
        left_entries.push((key, labels, value));
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
        let labels = result.labels.to_vec();
        let key = binary_vector_set_match_labels(&labels, expression);
        right_keys.insert(key.clone());
        right_entries.push((key, labels, value));
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

pub(super) fn binary_vector_match_labels(
    labels: &[(String, String)],
    matching: Option<&PromqlVectorMatching>,
) -> Vec<(String, String)> {
    let mut labels: Vec<(String, String)> = match matching {
        None => labels
            .iter()
            .filter(|(key, _)| key != METRIC_NAME_LABEL)
            .cloned()
            .collect(),
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

pub(super) fn push_instant_result(
    out: &mut Vec<SegmentQueryResult>,
    labels: Vec<(String, String)>,
    value: f64,
    eval_time_ms: u64,
) {
    let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
    result.push_sample(eval_time_ms, value);
    out.push(result);
}

pub(in crate::storage::segment) fn evaluate_binary_scalar_scalar(
    op: PromqlBinaryOp,
    left: f64,
    right: f64,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    evaluate_scalar(apply_binary_operator(op, left, right), eval_time_ms)
}

pub(super) fn apply_binary_operator(op: PromqlBinaryOp, left: f64, right: f64) -> f64 {
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

pub(in crate::storage::segment) fn binary_operator_is_set(op: PromqlBinaryOp) -> bool {
    matches!(
        op,
        PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless
    )
}

pub(super) fn binary_operator_is_comparison(op: PromqlBinaryOp) -> bool {
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
