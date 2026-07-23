use crate::promql::PromqlBinaryOp;

use super::super::QueryStats;

pub(in crate::storage::segment) fn native_histogram_input_present<T>(
    series: &[T],
    stats: QueryStats,
) -> bool {
    !series.is_empty() || stats.projected_series > 0
}

pub(super) fn histogram_scalar_binary_scale(
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
