mod classic;
mod dispatch;
mod exponential;
mod mixed;
mod shared;

pub(in crate::storage::segment) use dispatch::{
    evaluate_native_exponential_histogram_binary_bool_vector_vector,
    evaluate_native_exponential_histogram_binary_vector_scalar,
    evaluate_native_exponential_histogram_binary_vector_vector,
    evaluate_native_exponential_histogram_combined_vector_set,
    evaluate_native_exponential_histogram_mixed_binary_bool_vector_vector,
    evaluate_native_exponential_histogram_mixed_binary_vector_vector,
    evaluate_native_histogram_binary_bool_vector_vector,
    evaluate_native_histogram_binary_vector_scalar, evaluate_native_histogram_binary_vector_vector,
    evaluate_native_histogram_combined_vector_set,
    evaluate_native_histogram_mixed_binary_bool_vector_vector,
    evaluate_native_histogram_mixed_binary_vector_vector,
};
pub(in crate::storage::segment) use shared::native_histogram_input_present;
