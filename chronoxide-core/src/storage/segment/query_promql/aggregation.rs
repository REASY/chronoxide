use super::*;

/// Returns the only source label names a terminal aggregation can expose.
/// `None` means the input must retain its complete labels because the
/// operation or grouping can return or inspect labels outside this set.
pub(in crate::storage::segment) fn terminal_aggregation_grouping_names(
    aggregation: &PromqlAggregation,
) -> Option<&[String]> {
    match &aggregation.op {
        PromqlAggregationOp::TopK(_)
        | PromqlAggregationOp::BottomK(_)
        | PromqlAggregationOp::CountValues(_) => return None,
        PromqlAggregationOp::Sum
        | PromqlAggregationOp::Count
        | PromqlAggregationOp::Avg
        | PromqlAggregationOp::Min
        | PromqlAggregationOp::Max
        | PromqlAggregationOp::Stddev
        | PromqlAggregationOp::Stdvar
        | PromqlAggregationOp::Group
        | PromqlAggregationOp::Quantile(_) => {}
    }
    match &aggregation.grouping {
        PromqlAggregationGrouping::All => Some(&[]),
        PromqlAggregationGrouping::By(names) => Some(names),
        PromqlAggregationGrouping::Without(_) => None,
    }
}

/// Returns the grouping demand and whether the child drops `__name__` for the
/// only native terminal-aggregation shapes that may own selective labels.
/// Every other native expression retains complete labels before execution.
pub(in crate::storage::segment) fn native_terminal_aggregation_label_demand(
    aggregation: &PromqlAggregation,
) -> Option<(&[String], bool)> {
    if !native_histogram_scalar_aggregation_supported(&aggregation.op) {
        return None;
    }
    let grouping_names = terminal_aggregation_grouping_names(aggregation)?;
    match aggregation.input.as_ref() {
        PromqlQuery::Vector(_) => Some((grouping_names, false)),
        PromqlQuery::RangeFunction(function)
            if matches!(
                function.kind,
                PromqlRangeFunctionKind::Rate | PromqlRangeFunctionKind::Increase
            ) =>
        {
            Some((grouping_names, true))
        }
        _ => None,
    }
}

pub(in crate::storage::segment) fn evaluate_aggregation(
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
    #[expect(
        clippy::mutable_key_type,
        reason = "label compatibility caches and append-only arena state cannot change content ordering"
    )]
    let mut groups = BTreeMap::<QueryLabels, AggregationAccumulator>::new();
    for result in results {
        let Some((_, value)) = result.samples.last().copied() else {
            continue;
        };
        if is_prometheus_stale_marker(value) {
            continue;
        }
        let labels_complete = result.labels_are_complete();
        let labels =
            aggregation_group_query_key(&aggregation.grouping, result.labels, labels_complete);
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
        let mut result =
            SegmentQueryResult::with_shared_labels(query_labels_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
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
        let mut labels = result.labels.to_vec();
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
        let group_labels = aggregation_group_query_labels(&aggregation.grouping, &result.labels);
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

pub(super) fn rank_value_order(left: f64, right: f64, largest: bool) -> std::cmp::Ordering {
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

pub(super) fn vector_quantile(quantile: f64, values: &mut [f64]) -> f64 {
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
