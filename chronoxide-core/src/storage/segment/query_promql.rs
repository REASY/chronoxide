use super::*;
use std::borrow::Cow;

mod aggregation;
pub(super) use aggregation::{
    evaluate_aggregation, native_terminal_aggregation_label_demand,
    terminal_aggregation_grouping_names,
};
use aggregation::{rank_value_order, vector_quantile};

mod binary;
use binary::{
    apply_binary_operator, binary_operator_is_comparison, binary_vector_group_output_labels,
    binary_vector_match_labels, binary_vector_matching_cardinality, binary_vector_output_labels,
    is_prometheus_stale_marker, push_instant_result,
};
pub(super) use binary::{
    binary_operator_is_set, evaluate_binary_scalar_scalar, evaluate_binary_vector_scalar,
    evaluate_binary_vector_set, evaluate_binary_vector_vector,
};

mod functions;
pub(super) use functions::{
    binary_expression_vector_sides, evaluate_absent, evaluate_absent_over_time,
    evaluate_instant_function, evaluate_label_join, evaluate_label_replace,
    evaluate_promql_vector_function, evaluate_scalar, evaluate_scalar_function,
    is_scalar_expression, offset_eval_time_ms, retimestamp_instant_results,
    scalar_expression_value, scalar_query_result_value, validate_promql_range_bounds,
};

mod native_binary;
pub(super) use native_binary::*;
mod native;
pub(super) use native::*;
mod range;
pub(super) use range::*;
