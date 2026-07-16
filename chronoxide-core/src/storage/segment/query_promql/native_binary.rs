use super::*;

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

pub(in crate::storage::segment) fn native_histogram_input_present<T>(
    series: &[T],
    stats: QueryStats,
) -> bool {
    !series.is_empty() || stats.projected_series > 0
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

fn histogram_scalar_binary_scale(
    op: PromqlBinaryOp,
    scalar: f64,
    scalar_on_left: bool,
) -> Option<f64> {
    match (op, scalar_on_left) {
        (PromqlBinaryOp::Mul, _) => Some(scalar),
        (PromqlBinaryOp::Div, false) => Some(1.0 / scalar),
        _ => None,
    }
}

fn scale_histogram_sample(sample: &mut PromqlHistogramSample, scale: f64) {
    sample.count *= scale;
    if let Some(sum) = &mut sample.sum {
        *sum *= scale;
    }
    for count in &mut sample.bucket_counts {
        *count *= scale;
    }
}

fn scale_exponential_histogram_sample(sample: &mut PromqlExponentialHistogramSample, scale: f64) {
    sample.count *= scale;
    if let Some(sum) = &mut sample.sum {
        *sum *= scale;
    }
    sample.zero_count *= scale;
    sample.positive.scale_counts(scale);
    sample.negative.scale_counts(scale);
}

fn evaluate_histogram_binary_bool_one_to_one(
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

fn evaluate_histogram_binary_bool_many_to_one(
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

fn evaluate_histogram_binary_bool_one_to_many(
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
struct BinaryHistogramEntry {
    labels: Vec<(String, String)>,
    key: Vec<(String, String)>,
    sample: PromqlHistogramSample,
}

fn binary_histogram_entries(
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

fn evaluate_histogram_binary_one_to_one(
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

fn push_histogram_set_result(
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

fn evaluate_histogram_binary_many_to_one(
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

fn evaluate_histogram_binary_one_to_many(
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

#[derive(Debug, Clone)]
struct BinaryExponentialHistogramEntry {
    labels: Vec<(String, String)>,
    key: Vec<(String, String)>,
    sample: PromqlExponentialHistogramSample,
}

fn binary_exponential_histogram_entries(
    results: Vec<PromqlExponentialHistogramSeries>,
    matching: Option<&PromqlVectorMatching>,
) -> Vec<BinaryExponentialHistogramEntry> {
    let mut out = Vec::new();
    for result in results {
        let Some(sample) = result.samples.last().cloned() else {
            continue;
        };
        if sample.stale {
            continue;
        }
        let labels = result.labels.to_vec();
        out.push(BinaryExponentialHistogramEntry {
            key: binary_vector_match_labels(&labels, matching),
            labels,
            sample,
        });
    }
    out
}

fn evaluate_exponential_histogram_binary_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;
    let mut left_by_key = BTreeMap::<
        Vec<(String, String)>,
        (Vec<(String, String)>, PromqlExponentialHistogramSample),
    >::new();
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

    let mut right_by_key =
        BTreeMap::<Vec<(String, String)>, PromqlExponentialHistogramSample>::new();
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
        let Some(sample) =
            evaluate_exponential_histogram_binary_sample(expression, &left, right, eval_time_ms)
        else {
            continue;
        };
        let mut result = PromqlExponentialHistogramSeries::new(
            segment_series_id(&labels),
            shared_query_labels(labels),
        );
        result.push_sample(sample);
        out.push(result);
    }
    Ok(merge_exponential_histogram_query_results(out))
}

fn push_exponential_histogram_set_result(
    out: &mut Vec<PromqlExponentialHistogramSeries>,
    labels: Vec<(String, String)>,
    mut sample: PromqlExponentialHistogramSample,
    eval_time_ms: u64,
) {
    sample.timestamp_ms = eval_time_ms;
    let mut result = PromqlExponentialHistogramSeries::new(
        segment_series_id(&labels),
        shared_query_labels(labels),
    );
    result.push_sample(sample);
    out.push(result);
}

fn evaluate_exponential_histogram_binary_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_left vector matching metadata".to_string())
    })?;
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;
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
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
            continue;
        };
        let Some(sample) = evaluate_exponential_histogram_binary_sample(
            expression,
            &left.sample,
            &right.sample,
            eval_time_ms,
        ) else {
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
        let mut result = PromqlExponentialHistogramSeries::new(
            segment_series_id(&labels),
            shared_query_labels(labels),
        );
        result.push_sample(sample);
        out.push(result);
    }
    Ok(merge_exponential_histogram_query_results(out))
}

fn evaluate_exponential_histogram_binary_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<PromqlExponentialHistogramSeries>, PromqlQueryError> {
    let matching = expression.vector_matching.as_ref().ok_or_else(|| {
        PromqlQueryError::Invalid("missing group_right vector matching metadata".to_string())
    })?;
    let comparison = binary_operator_is_comparison(expression.op);
    let bool_comparison = comparison && expression.return_bool;
    let mut left_by_key = BTreeMap::<Vec<(String, String)>, BinaryExponentialHistogramEntry>::new();
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
        let Some(sample) = evaluate_exponential_histogram_binary_sample(
            expression,
            &left.sample,
            &right.sample,
            eval_time_ms,
        ) else {
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
        let mut result = PromqlExponentialHistogramSeries::new(
            segment_series_id(&labels),
            shared_query_labels(labels),
        );
        result.push_sample(sample);
        out.push(result);
    }
    Ok(merge_exponential_histogram_query_results(out))
}

fn evaluate_exponential_histogram_binary_sample(
    expression: &PromqlBinaryExpression,
    left: &PromqlExponentialHistogramSample,
    right: &PromqlExponentialHistogramSample,
    eval_time_ms: u64,
) -> Option<PromqlExponentialHistogramSample> {
    if expression.return_bool {
        return None;
    }

    match expression.op {
        PromqlBinaryOp::Add | PromqlBinaryOp::Sub => {
            combine_exponential_histogram_samples(left, right, expression.op, eval_time_ms)
        }
        PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq => {
            let equal = exponential_histogram_samples_equal(left, right);
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

fn combine_exponential_histogram_samples(
    left: &PromqlExponentialHistogramSample,
    right: &PromqlExponentialHistogramSample,
    op: PromqlBinaryOp,
    timestamp_ms: u64,
) -> Option<PromqlExponentialHistogramSample> {
    let scale = match op {
        PromqlBinaryOp::Add => 1.0,
        PromqlBinaryOp::Sub => -1.0,
        _ => return None,
    };
    let mut scaled_right = right.clone();
    scale_exponential_histogram_sample(&mut scaled_right, scale);

    let mut accumulator = ExponentialHistogramSumAccumulator::default();
    accumulator.observe(left);
    accumulator.observe(&scaled_right);
    accumulator.into_sample(timestamp_ms, &PromqlAggregationOp::Sum)
}

fn exponential_histogram_samples_equal(
    left: &PromqlExponentialHistogramSample,
    right: &PromqlExponentialHistogramSample,
) -> bool {
    !left.stale
        && !right.stale
        && left.count == right.count
        && left.sum == right.sum
        && left.scale == right.scale
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.zero_count == right.zero_count
        && left.positive == right.positive
        && left.negative == right.negative
}

fn evaluate_exponential_histogram_binary_bool_one_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
    eval_time_ms: u64,
) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
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

    let mut right_by_key =
        BTreeMap::<Vec<(String, String)>, PromqlExponentialHistogramSample>::new();
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
        let Some(value) =
            evaluate_exponential_histogram_binary_bool_value(expression, &left, right)
        else {
            continue;
        };
        push_instant_result(&mut out, labels, value, eval_time_ms);
    }
    Ok(merge_query_results(out))
}

fn evaluate_exponential_histogram_binary_bool_many_to_one(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
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
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for left in left_entries {
        let Some(right) = right_by_key.get(&left.key) else {
            continue;
        };
        let Some(value) = evaluate_exponential_histogram_binary_bool_value(
            expression,
            &left.sample,
            &right.sample,
        ) else {
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

fn evaluate_exponential_histogram_binary_bool_one_to_many(
    expression: &PromqlBinaryExpression,
    left_entries: Vec<BinaryExponentialHistogramEntry>,
    right_entries: Vec<BinaryExponentialHistogramEntry>,
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
    let mut output_labels = BTreeSet::<Vec<(String, String)>>::new();
    for right in right_entries {
        let Some(left) = left_by_key.get(&right.key) else {
            continue;
        };
        let Some(value) = evaluate_exponential_histogram_binary_bool_value(
            expression,
            &left.sample,
            &right.sample,
        ) else {
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

fn evaluate_exponential_histogram_binary_bool_value(
    expression: &PromqlBinaryExpression,
    left: &PromqlExponentialHistogramSample,
    right: &PromqlExponentialHistogramSample,
) -> Option<f64> {
    if !expression.return_bool {
        return None;
    }
    let equal = exponential_histogram_samples_equal(left, right);
    match expression.op {
        PromqlBinaryOp::Eq => Some(if equal { 1.0 } else { 0.0 }),
        PromqlBinaryOp::NotEq => Some(if equal { 0.0 } else { 1.0 }),
        _ => None,
    }
}

fn mixed_native_histogram_equality_op(op: PromqlBinaryOp) -> bool {
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

fn evaluate_histogram_exponential_mixed_binary_one_to_one(
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

fn evaluate_histogram_exponential_mixed_binary_many_to_one(
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

fn evaluate_histogram_exponential_mixed_binary_one_to_many(
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

fn evaluate_exponential_histogram_mixed_binary_one_to_one(
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

fn evaluate_exponential_histogram_mixed_binary_many_to_one(
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

fn evaluate_exponential_histogram_mixed_binary_one_to_many(
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

fn evaluate_histogram_exponential_mixed_binary_bool_one_to_one(
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

fn evaluate_histogram_exponential_mixed_binary_bool_many_to_one(
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

fn evaluate_histogram_exponential_mixed_binary_bool_one_to_many(
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

fn evaluate_exponential_histogram_mixed_binary_bool_one_to_one(
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

fn evaluate_exponential_histogram_mixed_binary_bool_many_to_one(
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

fn evaluate_exponential_histogram_mixed_binary_bool_one_to_many(
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
