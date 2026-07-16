use super::*;

pub(in crate::storage::segment) fn delta_projection_reset_hint(
    started: &mut bool,
) -> CounterResetHint {
    if *started {
        CounterResetHint::NotCounterReset
    } else {
        *started = true;
        CounterResetHint::CounterReset
    }
}
