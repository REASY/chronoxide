use crate::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue, SampleValue,
    SummaryQuantileValue, SummaryValue, TypedSampleMetadata,
};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, ExponentialHistogramDataPoint, HistogramDataPoint, NumberDataPoint,
    SummaryDataPoint,
};

pub fn datapoint_time_ms(time_unix_nano: u64) -> Option<u64> {
    (time_unix_nano > 0).then_some(time_unix_nano / 1_000_000)
}

pub fn number_value(dp: &NumberDataPoint) -> Option<SampleValue> {
    match dp.value.as_ref()? {
        Value::AsInt(value) => Some(SampleValue::Int64(*value)),
        Value::AsDouble(value) => Some(SampleValue::Float(*value)),
    }
}

pub fn histogram_value(dp: &HistogramDataPoint, aggregation_temporality: i32) -> SampleValue {
    histogram_value_with_buckets(
        dp,
        aggregation_temporality,
        dp.explicit_bounds.clone(),
        dp.bucket_counts.clone(),
    )
}

pub fn histogram_value_with_buckets(
    dp: &HistogramDataPoint,
    aggregation_temporality: i32,
    explicit_bounds: Vec<f64>,
    bucket_counts: Vec<u64>,
) -> SampleValue {
    SampleValue::Histogram(HistogramValue {
        count: dp.count,
        sum: dp.sum,
        min: dp.min,
        max: dp.max,
        metadata: typed_metadata(dp.start_time_unix_nano, dp.flags, aggregation_temporality),
        explicit_bounds,
        bucket_counts,
    })
}

pub fn exponential_histogram_value(
    dp: &ExponentialHistogramDataPoint,
    aggregation_temporality: i32,
) -> SampleValue {
    let positive = dp
        .positive
        .as_ref()
        .map(|buckets| ExponentialHistogramBuckets {
            offset: buckets.offset,
            counts: buckets.bucket_counts.clone(),
        })
        .unwrap_or_else(|| ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        });
    let negative = dp
        .negative
        .as_ref()
        .map(|buckets| ExponentialHistogramBuckets {
            offset: buckets.offset,
            counts: buckets.bucket_counts.clone(),
        })
        .unwrap_or_else(|| ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        });

    exponential_histogram_value_with_buckets(dp, aggregation_temporality, positive, negative)
}

pub fn take_exponential_histogram_buckets(
    buckets: &mut Option<
        opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets,
    >,
) -> ExponentialHistogramBuckets {
    buckets
        .take()
        .map(|buckets| ExponentialHistogramBuckets {
            offset: buckets.offset,
            counts: buckets.bucket_counts,
        })
        .unwrap_or_else(|| ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        })
}

pub fn exponential_histogram_value_with_buckets(
    dp: &ExponentialHistogramDataPoint,
    aggregation_temporality: i32,
    positive: ExponentialHistogramBuckets,
    negative: ExponentialHistogramBuckets,
) -> SampleValue {
    SampleValue::ExponentialHistogram(ExponentialHistogramValue {
        count: dp.count,
        sum: dp.sum,
        min: dp.min,
        max: dp.max,
        scale: dp.scale,
        zero_threshold: dp.zero_threshold,
        zero_count: dp.zero_count,
        metadata: typed_metadata(dp.start_time_unix_nano, dp.flags, aggregation_temporality),
        positive,
        negative,
    })
}

pub fn summary_value(dp: &SummaryDataPoint) -> SampleValue {
    let quantiles = dp
        .quantile_values
        .iter()
        .map(|value| SummaryQuantileValue {
            quantile: value.quantile,
            value: value.value,
        })
        .collect();

    SampleValue::Summary(SummaryValue {
        count: dp.count,
        sum: dp.sum,
        metadata: typed_metadata(dp.start_time_unix_nano, dp.flags, 0),
        quantiles,
    })
}

fn typed_metadata(
    start_time_unix_nano: u64,
    flags: u32,
    aggregation_temporality: i32,
) -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms: (start_time_unix_nano > 0).then_some(start_time_unix_nano / 1_000_000),
        flags,
        temporality: match AggregationTemporality::try_from(aggregation_temporality).ok() {
            Some(AggregationTemporality::Delta) => {
                crate::storage::head::OtlpAggregationTemporality::Delta
            }
            Some(AggregationTemporality::Cumulative) => {
                crate::storage::head::OtlpAggregationTemporality::Cumulative
            }
            Some(AggregationTemporality::Unspecified) | None => {
                crate::storage::head::OtlpAggregationTemporality::Unspecified
            }
        },
        reset_hint: crate::storage::head::CounterResetHint::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::head::{
        CounterResetHint, OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality,
    };
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, exponential_histogram_data_point::Buckets,
        summary_data_point::ValueAtQuantile,
    };

    #[test]
    fn datapoint_time_requires_an_otlp_timestamp() {
        assert_eq!(datapoint_time_ms(0), None);
        assert_eq!(datapoint_time_ms(999_999), Some(0));
        assert_eq!(datapoint_time_ms(1_999_999), Some(1));
    }

    #[test]
    fn histogram_value_maps_fields() {
        let dp = HistogramDataPoint {
            count: 3,
            sum: Some(9.0),
            min: Some(1.0),
            max: Some(5.0),
            start_time_unix_nano: 1_000_000_000,
            flags: OTLP_FLAG_NO_RECORDED_VALUE,
            explicit_bounds: vec![2.0, 4.0],
            bucket_counts: vec![1, 1, 1],
            ..Default::default()
        };

        let SampleValue::Histogram(value) =
            histogram_value(&dp, AggregationTemporality::Cumulative as i32)
        else {
            panic!("expected histogram sample");
        };
        assert_eq!(value.count, 3);
        assert_eq!(value.sum, Some(9.0));
        assert_eq!(value.min, Some(1.0));
        assert_eq!(value.max, Some(5.0));
        assert_eq!(value.explicit_bounds, vec![2.0, 4.0]);
        assert_eq!(value.bucket_counts, vec![1, 1, 1]);
        assert_eq!(value.metadata.start_time_ms, Some(1_000));
        assert_eq!(value.metadata.flags, OTLP_FLAG_NO_RECORDED_VALUE);
        assert_eq!(
            value.metadata.temporality,
            OtlpAggregationTemporality::Cumulative
        );
        assert_eq!(value.metadata.reset_hint, CounterResetHint::Unknown);
    }

    #[test]
    fn owned_histogram_buckets_transfer_allocations() {
        let mut dp = HistogramDataPoint {
            explicit_bounds: vec![1.0, 2.0, 3.0],
            bucket_counts: vec![4, 5, 6, 7],
            ..Default::default()
        };
        let bounds_ptr = dp.explicit_bounds.as_ptr();
        let bounds_len = dp.explicit_bounds.len();
        let bounds_capacity = dp.explicit_bounds.capacity();
        let counts_ptr = dp.bucket_counts.as_ptr();
        let counts_len = dp.bucket_counts.len();
        let counts_capacity = dp.bucket_counts.capacity();

        let explicit_bounds = std::mem::take(&mut dp.explicit_bounds);
        let bucket_counts = std::mem::take(&mut dp.bucket_counts);
        let SampleValue::Histogram(value) =
            histogram_value_with_buckets(&dp, 0, explicit_bounds, bucket_counts)
        else {
            panic!("expected histogram sample");
        };

        assert_eq!(value.explicit_bounds.as_ptr(), bounds_ptr);
        assert_eq!(value.explicit_bounds.len(), bounds_len);
        assert_eq!(value.explicit_bounds.capacity(), bounds_capacity);
        assert_eq!(value.bucket_counts.as_ptr(), counts_ptr);
        assert_eq!(value.bucket_counts.len(), counts_len);
        assert_eq!(value.bucket_counts.capacity(), counts_capacity);
        assert!(dp.explicit_bounds.is_empty());
        assert!(dp.bucket_counts.is_empty());
    }

    #[test]
    fn exponential_histogram_value_maps_fields() {
        let dp = ExponentialHistogramDataPoint {
            count: 5,
            sum: Some(10.0),
            min: None,
            max: Some(2.0),
            scale: 2,
            zero_threshold: 0.125,
            start_time_unix_nano: 2_000_000_000,
            flags: OTLP_FLAG_NO_RECORDED_VALUE,
            zero_count: 1,
            positive: Some(Buckets {
                offset: 1,
                bucket_counts: vec![1, 2],
            }),
            negative: Some(Buckets {
                offset: -1,
                bucket_counts: vec![3],
            }),
            ..Default::default()
        };

        let SampleValue::ExponentialHistogram(value) =
            exponential_histogram_value(&dp, AggregationTemporality::Delta as i32)
        else {
            panic!("expected exponential histogram sample");
        };
        assert_eq!(value.count, 5);
        assert_eq!(value.sum, Some(10.0));
        assert_eq!(value.min, None);
        assert_eq!(value.max, Some(2.0));
        assert_eq!(value.scale, 2);
        assert_eq!(value.zero_threshold, 0.125);
        assert_eq!(value.zero_count, 1);
        assert_eq!(value.positive.offset, 1);
        assert_eq!(value.positive.counts, vec![1, 2]);
        assert_eq!(value.negative.offset, -1);
        assert_eq!(value.negative.counts, vec![3]);
        assert_eq!(value.metadata.start_time_ms, Some(2_000));
        assert_eq!(value.metadata.flags, OTLP_FLAG_NO_RECORDED_VALUE);
        assert_eq!(
            value.metadata.temporality,
            OtlpAggregationTemporality::Delta
        );
        assert_eq!(value.metadata.reset_hint, CounterResetHint::Unknown);
    }

    #[test]
    fn owned_exponential_histogram_buckets_transfer_allocations() {
        let mut dp = ExponentialHistogramDataPoint {
            positive: Some(Buckets {
                offset: 3,
                bucket_counts: vec![1, 2, 3],
            }),
            negative: Some(Buckets {
                offset: -4,
                bucket_counts: vec![4, 5],
            }),
            ..Default::default()
        };
        let positive_ptr = dp.positive.as_ref().unwrap().bucket_counts.as_ptr();
        let positive_len = dp.positive.as_ref().unwrap().bucket_counts.len();
        let positive_capacity = dp.positive.as_ref().unwrap().bucket_counts.capacity();
        let negative_ptr = dp.negative.as_ref().unwrap().bucket_counts.as_ptr();
        let negative_len = dp.negative.as_ref().unwrap().bucket_counts.len();
        let negative_capacity = dp.negative.as_ref().unwrap().bucket_counts.capacity();

        let positive = take_exponential_histogram_buckets(&mut dp.positive);
        let negative = take_exponential_histogram_buckets(&mut dp.negative);
        let SampleValue::ExponentialHistogram(value) =
            exponential_histogram_value_with_buckets(&dp, 0, positive, negative)
        else {
            panic!("expected exponential histogram sample");
        };

        assert_eq!(value.positive.offset, 3);
        assert_eq!(value.positive.counts.as_ptr(), positive_ptr);
        assert_eq!(value.positive.counts.len(), positive_len);
        assert_eq!(value.positive.counts.capacity(), positive_capacity);
        assert_eq!(value.negative.offset, -4);
        assert_eq!(value.negative.counts.as_ptr(), negative_ptr);
        assert_eq!(value.negative.counts.len(), negative_len);
        assert_eq!(value.negative.counts.capacity(), negative_capacity);
        assert!(dp.positive.is_none());
        assert!(dp.negative.is_none());

        let mut missing = None;
        let missing = take_exponential_histogram_buckets(&mut missing);
        assert_eq!(missing.offset, 0);
        assert!(missing.counts.is_empty());

        let mut empty = Some(Buckets {
            offset: 7,
            bucket_counts: Vec::new(),
        });
        let empty_value = take_exponential_histogram_buckets(&mut empty);
        assert_eq!(empty_value.offset, 7);
        assert!(empty_value.counts.is_empty());
        assert!(empty.is_none());
    }

    #[test]
    fn summary_value_maps_fields() {
        let dp = SummaryDataPoint {
            count: 4,
            sum: 8.0,
            start_time_unix_nano: 3_000_000_000,
            flags: OTLP_FLAG_NO_RECORDED_VALUE,
            quantile_values: vec![
                ValueAtQuantile {
                    quantile: 0.5,
                    value: 2.0,
                },
                ValueAtQuantile {
                    quantile: 0.9,
                    value: 4.0,
                },
            ],
            ..Default::default()
        };

        let SampleValue::Summary(value) = summary_value(&dp) else {
            panic!("expected summary sample");
        };
        assert_eq!(value.count, 4);
        assert_eq!(value.sum, 8.0);
        assert_eq!(value.metadata.start_time_ms, Some(3_000));
        assert_eq!(value.metadata.flags, OTLP_FLAG_NO_RECORDED_VALUE);
        assert_eq!(
            value.metadata.temporality,
            OtlpAggregationTemporality::Unspecified
        );
        assert_eq!(value.quantiles.len(), 2);
        assert_eq!(value.quantiles[0].quantile, 0.5);
        assert_eq!(value.quantiles[0].value, 2.0);
        assert_eq!(value.quantiles[1].quantile, 0.9);
        assert_eq!(value.quantiles[1].value, 4.0);
    }
}
