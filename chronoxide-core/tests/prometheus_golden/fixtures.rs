use super::*;

#[path = "cases.rs"]
mod cases;

pub(super) use cases::{
    golden_cases, golden_error_cases, golden_head_range_cases, golden_range_cases,
};

fn write_float_series(
    writer: &mut SegmentWriter,
    series: u32,
    labels: &[(&str, &str)],
    samples: &[(u64, f64)],
) {
    let labels = labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect::<Vec<_>>();
    writer
        .record_samples_with_labels(SeriesRef::new(series), &labels, samples)
        .unwrap();
}

fn intern_labels(
    label_store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    labels: &[(&str, &str)],
) -> SeriesRef {
    let mut refs = labels
        .iter()
        .copied()
        .map(KeyValueRef::from)
        .collect::<Vec<_>>();
    refs.sort_unstable_by(|left, right| {
        left.key
            .cmp(right.key)
            .then_with(|| left.value.cmp(right.value))
    });
    label_store.intern(&refs).unwrap()
}

fn owned_labels(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut labels = labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect::<Vec<_>>();
    labels.sort_unstable();
    labels
}

pub(super) fn golden_head() -> HeadBuffer {
    HeadBuffer::new(HeadConfig::with_block_size(
        Duration::from_secs(60),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap()
}

fn write_head_counter_cross_segment(
    writer: &mut SegmentWriter,
    label_store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    head: &mut HeadBuffer,
) {
    let labels = &[
        (METRIC_NAME_LABEL, "head_requests_total"),
        ("job", "api"),
        ("instance", "a"),
    ];
    let series = intern_labels(label_store, labels);
    writer
        .record_samples_with_labels(series, &owned_labels(labels), &[(0, 0.0), (10_000, 10.0)])
        .unwrap();
    for (timestamp_ms, value) in [(20_000, 20.0), (30_000, 30.0), (40_000, 40.0)] {
        head.record_sample(series, timestamp_ms, SampleValue::Float(value))
            .unwrap();
    }
}

fn write_head_histogram_cross_segment(
    writer: &mut SegmentWriter,
    label_store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    head: &mut HeadBuffer,
) {
    let labels = &[
        (METRIC_NAME_LABEL, "head_request_duration_seconds"),
        ("route", "/head-typed"),
    ];
    let series = intern_labels(label_store, labels);
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            series,
            &[
                (0, histogram_value(5, 5.0, [2, 2, 1])),
                (10_000, histogram_value(10, 10.0, [4, 4, 2])),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "head_request_duration_seconds");
                visit("route", "/head-typed");
            },
        )
        .unwrap();
    for (timestamp_ms, value) in [
        (20_000, histogram_value(15, 15.0, [6, 6, 3])),
        (30_000, histogram_value(20, 20.0, [8, 8, 4])),
        (40_000, histogram_value(25, 25.0, [10, 10, 5])),
    ] {
        head.record_sample(series, timestamp_ms, SampleValue::Histogram(value))
            .unwrap();
    }
}

fn write_float_counter_rate_sum_by(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        1,
        &[
            (METRIC_NAME_LABEL, "http_requests_total"),
            ("job", "api"),
            ("route", "/checkout"),
            ("instance", "a"),
        ],
        &[
            (0, 0.0),
            (10_000, 10.0),
            (20_000, 20.0),
            (30_000, 30.0),
            (40_000, 40.0),
        ],
    );
    write_float_series(
        writer,
        2,
        &[
            (METRIC_NAME_LABEL, "http_requests_total"),
            ("job", "api"),
            ("route", "/checkout"),
            ("instance", "b"),
        ],
        &[
            (0, 0.0),
            (10_000, 5.0),
            (20_000, 10.0),
            (30_000, 15.0),
            (40_000, 20.0),
        ],
    );
    write_float_series(
        writer,
        3,
        &[
            (METRIC_NAME_LABEL, "http_requests_total"),
            ("job", "api"),
            ("route", "/search"),
            ("instance", "a"),
        ],
        &[
            (0, 0.0),
            (10_000, 2.0),
            (20_000, 4.0),
            (30_000, 6.0),
            (40_000, 8.0),
        ],
    );
}

fn write_range_error_request_counters(writer: &mut SegmentWriter) {
    write_float_counter_rate_sum_by(writer);
    for (series, route, code, samples) in [
        (
            194,
            "/checkout",
            "500",
            vec![
                (0, 0.0),
                (10_000, 1.0),
                (20_000, 2.0),
                (30_000, 3.0),
                (40_000, 4.0),
            ],
        ),
        (
            195,
            "/checkout",
            "404",
            vec![
                (0, 0.0),
                (10_000, 2.0),
                (20_000, 4.0),
                (30_000, 6.0),
                (40_000, 8.0),
            ],
        ),
        (
            196,
            "/search",
            "500",
            vec![
                (0, 0.0),
                (10_000, 1.0),
                (20_000, 1.0),
                (30_000, 2.0),
                (40_000, 2.0),
            ],
        ),
    ] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "http_errors_total"),
                ("job", "api"),
                ("route", route),
                ("code", code),
            ],
            &samples,
        );
    }
}

fn write_label_replace_and_join(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        10,
        &[
            (METRIC_NAME_LABEL, "cpu_usage"),
            ("job", "api"),
            ("instance", "a"),
        ],
        &[
            (0, 1.0),
            (10_000, 2.0),
            (20_000, 3.0),
            (30_000, 4.0),
            (40_000, 5.0),
        ],
    );
}

pub(super) fn write_missing_label_semantics(writer: &mut SegmentWriter) {
    for (series, labels, value) in [
        (
            11,
            vec![
                (METRIC_NAME_LABEL, "missing_semantics"),
                ("env", ""),
                ("shard", "explicit"),
            ],
            1.0,
        ),
        (
            12,
            vec![
                (METRIC_NAME_LABEL, "missing_semantics"),
                ("shard", "absent"),
            ],
            2.0,
        ),
        (
            13,
            vec![
                (METRIC_NAME_LABEL, "missing_semantics"),
                ("env", "prod"),
                ("shard", "nonempty"),
            ],
            3.0,
        ),
    ] {
        write_float_series(
            writer,
            series,
            &labels,
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
}

fn write_unrelated_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        20,
        &[(METRIC_NAME_LABEL, "unrelated_metric"), ("job", "api")],
        &[
            (0, 1.0),
            (10_000, 1.0),
            (20_000, 1.0),
            (30_000, 1.0),
            (40_000, 1.0),
        ],
    );
}

fn write_stale_only_absent_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        21,
        &[(METRIC_NAME_LABEL, "stale_only_total"), ("job", "api")],
        &[(40_000, prometheus_stale_nan())],
    );
}

pub(super) fn write_temperature_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        30,
        &[
            (METRIC_NAME_LABEL, "temperature_celsius"),
            ("sensor", "rack-a"),
        ],
        &[
            (0, 10.0),
            (10_000, 12.0),
            (20_000, 14.0),
            (30_000, 16.0),
            (40_000, 18.0),
        ],
    );
}

fn write_gauge_range_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        31,
        &[(METRIC_NAME_LABEL, "gauge_value"), ("series", "a")],
        &[
            (0, 1.0),
            (10_000, 2.0),
            (20_000, 4.0),
            (30_000, 8.0),
            (40_000, 16.0),
        ],
    );
}

fn write_reset_counter_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        32,
        &[(METRIC_NAME_LABEL, "reset_counter_total"), ("series", "a")],
        &[
            (0, 0.0),
            (10_000, 10.0),
            (20_000, 5.0),
            (30_000, 15.0),
            (40_000, 25.0),
        ],
    );
}

pub(super) fn write_cpu_multi_series(writer: &mut SegmentWriter) {
    for (series, instance, samples) in [
        (
            33,
            "a",
            vec![
                (0, 1.0),
                (10_000, 2.0),
                (20_000, 3.0),
                (30_000, 4.0),
                (40_000, 5.0),
            ],
        ),
        (
            34,
            "b",
            vec![
                (0, 2.0),
                (10_000, 3.0),
                (20_000, 4.0),
                (30_000, 5.0),
                (40_000, 6.0),
            ],
        ),
        (
            35,
            "c",
            vec![
                (0, 3.0),
                (10_000, 4.0),
                (20_000, 5.0),
                (30_000, 6.0),
                (40_000, 7.0),
            ],
        ),
    ] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "cpu_usage"),
                ("job", "api"),
                ("instance", instance),
            ],
            &samples,
        );
    }
}

fn write_error_request_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        36,
        &[
            (METRIC_NAME_LABEL, "errors_total"),
            ("job", "api"),
            ("instance", "a"),
            ("code", "500"),
        ],
        &[
            (0, 1.0),
            (10_000, 2.0),
            (20_000, 3.0),
            (30_000, 4.0),
            (40_000, 5.0),
        ],
    );
    write_float_series(
        writer,
        37,
        &[
            (METRIC_NAME_LABEL, "requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ],
        &[
            (0, 10.0),
            (10_000, 20.0),
            (20_000, 30.0),
            (30_000, 40.0),
            (40_000, 50.0),
        ],
    );
}

fn write_group_left_series(writer: &mut SegmentWriter) {
    for (series, method, code, value) in [
        (100, "get", "500", 24.0),
        (101, "get", "404", 30.0),
        (102, "post", "500", 6.0),
        (103, "post", "404", 21.0),
    ] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "http_errors"),
                ("method", method),
                ("code", code),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
    for (series, method, value) in [(104, "get", 600.0), (105, "post", 120.0)] {
        write_float_series(
            writer,
            series,
            &[(METRIC_NAME_LABEL, "http_requests"), ("method", method)],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
}

fn write_group_right_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        110,
        &[
            (METRIC_NAME_LABEL, "cpu_limit"),
            ("route", "/group-right"),
            ("service", "api"),
        ],
        &[
            (0, 10.0),
            (10_000, 10.0),
            (20_000, 10.0),
            (30_000, 10.0),
            (40_000, 10.0),
        ],
    );
    for (series, instance, value) in [(111, "a", 2.0), (112, "b", 4.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "cpu_usage_group_right"),
                ("route", "/group-right"),
                ("instance", instance),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
}

fn write_cardinality_duplicate_right_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        130,
        &[
            (METRIC_NAME_LABEL, "card_left"),
            ("job", "api"),
            ("instance", "a"),
        ],
        &[
            (0, 1.0),
            (10_000, 1.0),
            (20_000, 1.0),
            (30_000, 1.0),
            (40_000, 1.0),
        ],
    );
    for (series, code, value) in [(131, "500", 10.0), (132, "404", 20.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "card_right"),
                ("job", "api"),
                ("code", code),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
}

fn write_cardinality_duplicate_left_series(writer: &mut SegmentWriter) {
    for (series, instance, value) in [(133, "a", 1.0), (134, "b", 2.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "card_left"),
                ("job", "api"),
                ("instance", instance),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
    write_float_series(
        writer,
        135,
        &[
            (METRIC_NAME_LABEL, "card_right"),
            ("job", "api"),
            ("code", "500"),
        ],
        &[
            (0, 10.0),
            (10_000, 10.0),
            (20_000, 10.0),
            (30_000, 10.0),
            (40_000, 10.0),
        ],
    );
}

fn write_group_left_duplicate_one_side_series(writer: &mut SegmentWriter) {
    for (series, code, value) in [(136, "500", 24.0), (137, "404", 30.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "gl_errors"),
                ("method", "get"),
                ("code", code),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
    for (series, instance, value) in [(138, "a", 600.0), (139, "b", 700.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "gl_requests"),
                ("method", "get"),
                ("instance", instance),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
}

fn write_group_right_duplicate_one_side_series(writer: &mut SegmentWriter) {
    for (series, service, value) in [(140, "a", 10.0), (141, "b", 20.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "gr_limit"),
                ("route", "/api"),
                ("service", service),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
    for (series, instance, value) in [(142, "a", 2.0), (143, "b", 4.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "gr_usage"),
                ("route", "/api"),
                ("instance", instance),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
}

fn write_group_left_duplicate_result_series(writer: &mut SegmentWriter) {
    for (series, service, value) in [(144, "old-a", 24.0), (145, "old-b", 30.0)] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "gl_result_left"),
                ("method", "get"),
                ("service", service),
            ],
            &[
                (0, value),
                (10_000, value),
                (20_000, value),
                (30_000, value),
                (40_000, value),
            ],
        );
    }
    write_float_series(
        writer,
        146,
        &[
            (METRIC_NAME_LABEL, "gl_result_right"),
            ("method", "get"),
            ("service", "api"),
        ],
        &[
            (0, 600.0),
            (10_000, 600.0),
            (20_000, 600.0),
            (30_000, 600.0),
            (40_000, 600.0),
        ],
    );
}

fn write_stale_mix_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        120,
        &[
            (METRIC_NAME_LABEL, "stale_mix"),
            ("route", "/stale"),
            ("instance", "finite"),
        ],
        &[
            (0, 2.0),
            (10_000, 2.0),
            (20_000, 2.0),
            (30_000, 2.0),
            (40_000, 2.0),
        ],
    );
    write_float_series(
        writer,
        121,
        &[
            (METRIC_NAME_LABEL, "stale_mix"),
            ("route", "/stale"),
            ("instance", "stale"),
        ],
        &[
            (0, 1.0),
            (10_000, 1.0),
            (20_000, 1.0),
            (30_000, 1.0),
            (40_000, prometheus_stale_nan()),
        ],
    );
}

fn write_stale_binary_vector_matching_series(writer: &mut SegmentWriter) {
    for (series, instance, values) in [
        (
            155,
            "matched",
            [
                (0, 2.0),
                (10_000, 2.0),
                (20_000, 2.0),
                (30_000, 2.0),
                (40_000, 2.0),
            ],
        ),
        (
            156,
            "left-stale",
            [
                (0, 3.0),
                (10_000, 3.0),
                (20_000, 3.0),
                (30_000, 3.0),
                (40_000, prometheus_stale_nan()),
            ],
        ),
        (
            157,
            "right-stale",
            [
                (0, 5.0),
                (10_000, 5.0),
                (20_000, 5.0),
                (30_000, 5.0),
                (40_000, 5.0),
            ],
        ),
        (
            158,
            "left-only",
            [
                (0, 7.0),
                (10_000, 7.0),
                (20_000, 7.0),
                (30_000, 7.0),
                (40_000, 7.0),
            ],
        ),
    ] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "stale_binary_left"),
                ("route", "/stale-binary"),
                ("instance", instance),
            ],
            &values,
        );
    }

    for (series, instance, values) in [
        (
            159,
            "matched",
            [
                (0, 10.0),
                (10_000, 10.0),
                (20_000, 10.0),
                (30_000, 10.0),
                (40_000, 10.0),
            ],
        ),
        (
            160,
            "left-stale",
            [
                (0, 20.0),
                (10_000, 20.0),
                (20_000, 20.0),
                (30_000, 20.0),
                (40_000, 20.0),
            ],
        ),
        (
            161,
            "right-stale",
            [
                (0, 30.0),
                (10_000, 30.0),
                (20_000, 30.0),
                (30_000, 30.0),
                (40_000, prometheus_stale_nan()),
            ],
        ),
        (
            162,
            "right-only",
            [
                (0, 11.0),
                (10_000, 11.0),
                (20_000, 11.0),
                (30_000, 11.0),
                (40_000, 11.0),
            ],
        ),
    ] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "stale_binary_right"),
                ("route", "/stale-binary"),
                ("instance", instance),
            ],
            &values,
        );
    }
}

fn write_stale_range_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        124,
        &[(METRIC_NAME_LABEL, "stale_range_value"), ("series", "a")],
        &[
            (0, 1.0),
            (10_000, 2.0),
            (20_000, prometheus_stale_nan()),
            (30_000, 8.0),
            (40_000, 16.0),
        ],
    );
}

fn write_nonfinite_value_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        122,
        &[
            (METRIC_NAME_LABEL, "nonfinite_value"),
            ("route", "/nonfinite"),
            ("instance", "nan"),
        ],
        &[
            (0, f64::NAN),
            (10_000, f64::NAN),
            (20_000, f64::NAN),
            (30_000, f64::NAN),
            (40_000, f64::NAN),
        ],
    );
    write_float_series(
        writer,
        123,
        &[
            (METRIC_NAME_LABEL, "nonfinite_value"),
            ("route", "/nonfinite"),
            ("instance", "inf"),
        ],
        &[
            (0, f64::INFINITY),
            (10_000, f64::INFINITY),
            (20_000, f64::INFINITY),
            (30_000, f64::INFINITY),
            (40_000, f64::INFINITY),
        ],
    );
    write_float_series(
        writer,
        151,
        &[
            (METRIC_NAME_LABEL, "nonfinite_value"),
            ("route", "/nonfinite"),
            ("instance", "neg-inf"),
        ],
        &[
            (0, f64::NEG_INFINITY),
            (10_000, f64::NEG_INFINITY),
            (20_000, f64::NEG_INFINITY),
            (30_000, f64::NEG_INFINITY),
            (40_000, f64::NEG_INFINITY),
        ],
    );
}

fn write_positive_inf_aggregation_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        152,
        &[
            (METRIC_NAME_LABEL, "positive_inf_agg"),
            ("route", "/agg"),
            ("instance", "finite"),
        ],
        &[
            (0, 2.0),
            (10_000, 2.0),
            (20_000, 2.0),
            (30_000, 2.0),
            (40_000, 2.0),
        ],
    );
    write_float_series(
        writer,
        153,
        &[
            (METRIC_NAME_LABEL, "positive_inf_agg"),
            ("route", "/agg"),
            ("instance", "pos"),
        ],
        &[
            (0, f64::INFINITY),
            (10_000, f64::INFINITY),
            (20_000, f64::INFINITY),
            (30_000, f64::INFINITY),
            (40_000, f64::INFINITY),
        ],
    );
}

fn write_positive_inf_range_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        154,
        &[(METRIC_NAME_LABEL, "positive_inf_range"), ("case", "mixed")],
        &[
            (0, 1.0),
            (10_000, 2.0),
            (20_000, f64::INFINITY),
            (30_000, 4.0),
            (40_000, 5.0),
        ],
    );
}

fn write_classic_histogram_bucket_series(writer: &mut SegmentWriter) {
    for (series, le, samples) in [
        (
            40,
            "1",
            vec![
                (0, 2.0),
                (10_000, 4.0),
                (20_000, 6.0),
                (30_000, 8.0),
                (40_000, 10.0),
            ],
        ),
        (
            41,
            "2",
            vec![
                (0, 4.0),
                (10_000, 8.0),
                (20_000, 12.0),
                (30_000, 16.0),
                (40_000, 20.0),
            ],
        ),
        (
            42,
            "+Inf",
            vec![
                (0, 5.0),
                (10_000, 10.0),
                (20_000, 15.0),
                (30_000, 20.0),
                (40_000, 25.0),
            ],
        ),
    ] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "classic_request_duration_seconds_bucket"),
                ("route", "/checkout"),
                ("le", le),
            ],
            &samples,
        );
    }
}

fn write_otlp_histogram_series(writer: &mut SegmentWriter) {
    let samples = [
        (0, histogram_value(5, 5.0, [2, 2, 1])),
        (10_000, histogram_value(10, 10.0, [4, 4, 2])),
        (20_000, histogram_value(15, 15.0, [6, 6, 3])),
        (30_000, histogram_value(20, 20.0, [8, 8, 4])),
        (40_000, histogram_value(25, 25.0, [10, 10, 5])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(50),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_request_duration_seconds");
                visit("route", "/checkout");
            },
        )
        .unwrap();
}

fn write_otlp_delta_histogram_series(writer: &mut SegmentWriter) {
    let samples = [
        (1, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (10_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (20_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (30_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (40_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(51),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_delta_request_duration_seconds");
                visit("route", "/delta");
            },
        )
        .unwrap();
}

fn write_otlp_delta_histogram_reset_boundary_series(writer: &mut SegmentWriter) {
    let samples = [
        (1, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (10_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (
            20_000,
            histogram_value_with_metadata(5, 5.0, [2, 2, 1], delta_reset_metadata()),
        ),
        (30_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (40_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(54),
            &samples,
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "otlp_delta_reset_request_duration_seconds",
                );
                visit("route", "/delta-reset");
            },
        )
        .unwrap();
}

fn write_otlp_delta_histogram_stale_fragment_series(writer: &mut SegmentWriter) {
    let samples = [
        (1, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (10_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (
            20_000,
            histogram_value_with_metadata(0, 0.0, [0, 0, 0], delta_stale_metadata()),
        ),
        (30_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (40_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(52),
            &samples,
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "otlp_delta_stale_request_duration_seconds",
                );
                visit("route", "/delta-stale");
            },
        )
        .unwrap();
}

fn write_otlp_delta_histogram_stale_rate_series(writer: &mut SegmentWriter) {
    let samples = [
        (
            0,
            histogram_value_with_metadata(0, 0.0, [0, 0, 0], delta_stale_metadata()),
        ),
        (10_000, delta_histogram_value(10, 10.0, [4, 4, 2])),
        (
            20_000,
            histogram_value_with_metadata(0, 0.0, [0, 0, 0], delta_stale_metadata()),
        ),
        (30_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (40_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(55),
            &samples,
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "otlp_delta_stale_request_duration_seconds",
                );
                visit("route", "/delta-stale");
            },
        )
        .unwrap();
}

fn write_otlp_delta_histogram_nonfinite_sum_series(writer: &mut SegmentWriter) {
    let value = |count, sum, start_time_ms| HistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(start_time_ms),
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
            ..TypedSampleMetadata::default()
        },
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    };
    for (idx, (kind, non_finite_sum)) in [
        ("nan", f64::NAN),
        ("positive-infinity", f64::INFINITY),
        ("negative-infinity", f64::NEG_INFINITY),
    ]
    .into_iter()
    .enumerate()
    {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(56 + idx as u32),
                &[
                    (10_000, value(5, 5.0, 0)),
                    (20_000, value(5, non_finite_sum, 10_000)),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "otlp_delta_nonfinite_sum_seconds");
                    visit("kind", kind);
                    visit("path", "multi");
                },
            )
            .unwrap();
    }
}

fn write_otlp_delta_histogram_stale_native_series(writer: &mut SegmentWriter) {
    let samples = [
        (
            0,
            histogram_value_with_metadata(0, 0.0, [0, 0, 0], delta_stale_metadata()),
        ),
        (
            10_000,
            histogram_value_with_metadata(0, 0.0, [0, 0, 0], delta_stale_metadata()),
        ),
        (20_000, delta_histogram_value(5, 5.0, [2, 2, 1])),
        (30_000, delta_histogram_value(10, 10.0, [4, 4, 2])),
        (40_000, delta_histogram_value(10, 10.0, [4, 4, 2])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(53),
            &samples,
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "otlp_delta_stale_native_request_duration_seconds",
                );
                visit("route", "/delta-stale-native");
            },
        )
        .unwrap();
}

fn histogram_value(count: u64, sum: f64, bucket_counts: [u64; 3]) -> HistogramValue {
    histogram_value_with_metadata(count, sum, bucket_counts, cumulative_not_reset_metadata())
}

fn custom_histogram_value(
    count: u64,
    sum: f64,
    explicit_bounds: &[f64],
    bucket_counts: &[u64],
) -> HistogramValue {
    HistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        metadata: cumulative_not_reset_metadata(),
        explicit_bounds: explicit_bounds.to_vec(),
        bucket_counts: bucket_counts.to_vec(),
    }
}

fn delta_histogram_value(count: u64, sum: f64, bucket_counts: [u64; 3]) -> HistogramValue {
    histogram_value_with_metadata(count, sum, bucket_counts, delta_not_reset_metadata())
}

fn histogram_value_with_metadata(
    count: u64,
    sum: f64,
    bucket_counts: [u64; 3],
    metadata: TypedSampleMetadata,
) -> HistogramValue {
    HistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        metadata,
        explicit_bounds: vec![1.0, 2.0],
        bucket_counts: bucket_counts.into(),
    }
}

fn write_otlp_summary_series(writer: &mut SegmentWriter) {
    let samples = [
        (0, summary_value(10, 2.0, 0.42)),
        (10_000, summary_value(20, 4.0, 0.43)),
        (20_000, summary_value(30, 6.0, 0.44)),
        (30_000, summary_value(40, 8.0, 0.45)),
        (40_000, summary_value(50, 10.0, 0.46)),
    ];
    writer
        .record_summary_samples_ordered_with_label_visitor(SeriesRef::new(60), &samples, |visit| {
            visit(METRIC_NAME_LABEL, "rpc_duration_seconds");
            visit("route", "/summary");
        })
        .unwrap();
}

fn summary_value(count: u64, sum: f64, p90: f64) -> SummaryValue {
    SummaryValue {
        count,
        sum,
        metadata: cumulative_not_reset_metadata(),
        quantiles: vec![SummaryQuantileValue {
            quantile: 0.9,
            value: p90,
        }],
    }
}

fn write_otlp_exponential_histogram_series(writer: &mut SegmentWriter) {
    let samples = [
        (0, exphist_value(5, 5.0, [2, 3])),
        (10_000, exphist_value(10, 10.0, [4, 6])),
        (20_000, exphist_value(15, 15.0, [6, 9])),
        (30_000, exphist_value(20, 20.0, [8, 12])),
        (40_000, exphist_value(25, 25.0, [10, 15])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(70),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_size_bytes");
                visit("route", "/download");
            },
        )
        .unwrap();
}

fn write_otlp_delta_exponential_histogram_series(writer: &mut SegmentWriter) {
    let samples = [
        (1, delta_exphist_value(5, 5.0, [2, 3])),
        (10_000, delta_exphist_value(5, 5.0, [2, 3])),
        (20_000, delta_exphist_value(5, 5.0, [2, 3])),
        (30_000, delta_exphist_value(5, 5.0, [2, 3])),
        (40_000, delta_exphist_value(5, 5.0, [2, 3])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(71),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_delta_size_bytes");
                visit("route", "/delta-download");
            },
        )
        .unwrap();
}

fn write_otlp_delta_exponential_histogram_reset_boundary_series(writer: &mut SegmentWriter) {
    let samples = [
        (1, delta_exphist_value(5, 5.0, [2, 3])),
        (10_000, delta_exphist_value(5, 5.0, [2, 3])),
        (
            20_000,
            exphist_value_with_metadata(5, 5.0, [2, 3], delta_reset_metadata()),
        ),
        (30_000, delta_exphist_value(5, 5.0, [2, 3])),
        (40_000, delta_exphist_value(5, 5.0, [2, 3])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(74),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_delta_reset_size_bytes");
                visit("route", "/delta-reset-download");
            },
        )
        .unwrap();
}

fn write_otlp_delta_exponential_histogram_stale_fragment_series(writer: &mut SegmentWriter) {
    let samples = [
        (1, delta_exphist_value(5, 5.0, [2, 3])),
        (10_000, delta_exphist_value(5, 5.0, [2, 3])),
        (
            20_000,
            exphist_value_with_metadata(0, 0.0, [0, 0], delta_stale_metadata()),
        ),
        (30_000, delta_exphist_value(5, 5.0, [2, 3])),
        (40_000, delta_exphist_value(5, 5.0, [2, 3])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(72),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_delta_stale_size_bytes");
                visit("route", "/delta-stale-download");
            },
        )
        .unwrap();
}

fn write_otlp_delta_exponential_histogram_stale_rate_series(writer: &mut SegmentWriter) {
    let samples = [
        (
            0,
            exphist_value_with_metadata(0, 0.0, [0, 0], delta_stale_metadata()),
        ),
        (10_000, delta_exphist_value(10, 10.0, [4, 6])),
        (
            20_000,
            exphist_value_with_metadata(0, 0.0, [0, 0], delta_stale_metadata()),
        ),
        (30_000, delta_exphist_value(5, 5.0, [2, 3])),
        (40_000, delta_exphist_value(5, 5.0, [2, 3])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(75),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_delta_stale_size_bytes");
                visit("route", "/delta-stale-download");
            },
        )
        .unwrap();
}

fn write_otlp_delta_exponential_histogram_stale_native_series(writer: &mut SegmentWriter) {
    let samples = [
        (
            0,
            exphist_value_with_metadata(0, 0.0, [0, 0], delta_stale_metadata()),
        ),
        (
            10_000,
            exphist_value_with_metadata(0, 0.0, [0, 0], delta_stale_metadata()),
        ),
        (20_000, delta_exphist_value(5, 5.0, [2, 3])),
        (30_000, delta_exphist_value(10, 10.0, [4, 6])),
        (40_000, delta_exphist_value(10, 10.0, [4, 6])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(73),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "otlp_delta_stale_native_size_bytes");
                visit("route", "/delta-stale-native-download");
            },
        )
        .unwrap();
}

fn write_native_exponential_histogram_quantile(writer: &mut SegmentWriter) {
    let samples = [
        (1_000, exphist_value(5, 12.0, [2, 3])),
        (6_000, exphist_value(10, 24.0, [4, 6])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(80),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_exphist_seconds");
                visit("route", "/native");
            },
        )
        .unwrap();
}

fn write_native_exponential_histogram_resets_observable_decrease(writer: &mut SegmentWriter) {
    let samples = [
        (1_000, exphist_value(10, 20.0, [4, 6])),
        (6_000, exphist_value(5, 12.0, [2, 3])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(81),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_exphist_reset_seconds");
                visit("route", "/native-reset");
            },
        )
        .unwrap();
}

fn write_native_exponential_histogram_changes(writer: &mut SegmentWriter) {
    for (series_ref, case, samples) in [
        (
            SeriesRef::new(82),
            "change",
            [
                (1_000, exphist_value(10, 20.0, [4, 6])),
                (6_000, exphist_value(12, 24.0, [5, 7])),
            ],
        ),
        (
            SeriesRef::new(83),
            "same",
            [
                (1_000, exphist_value(10, 20.0, [4, 6])),
                (6_000, exphist_value(10, 20.0, [4, 6])),
            ],
        ),
    ] {
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, "native_exphist_changes_seconds");
                    visit("case", case);
                },
            )
            .unwrap();
    }
}

fn write_native_exponential_histogram_range_quantile(writer: &mut SegmentWriter) {
    let samples = [
        (1_000, exphist_value(5, 12.0, [2, 3])),
        (6_000, exphist_value(10, 24.0, [4, 6])),
        (11_000, exphist_value(15, 36.0, [6, 9])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(230),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_exphist_range_seconds");
                visit("route", "/native-range");
            },
        )
        .unwrap();
}

fn write_native_exponential_histogram_stale_latest(writer: &mut SegmentWriter) {
    let stale_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        ..cumulative_not_reset_metadata()
    };
    let samples = [
        (0, exphist_value(5, 5.0, [2, 3])),
        (10_000, exphist_value(10, 10.0, [4, 6])),
        (20_000, exphist_value(15, 15.0, [6, 9])),
        (30_000, exphist_value(20, 20.0, [8, 12])),
        (
            40_000,
            exphist_value_with_metadata(0, 0.0, [0, 0], stale_metadata),
        ),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(231),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_exphist_stale_seconds");
                visit("route", "/native-stale");
            },
        )
        .unwrap();
}

fn write_native_exponential_histogram_stale_vector_matching(writer: &mut SegmentWriter) {
    let stale_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        ..cumulative_not_reset_metadata()
    };

    for (series, metric, instance, count, counts, stale_latest) in [
        (
            232,
            "native_exphist_stale_left_seconds",
            "matched",
            5,
            [2, 3],
            false,
        ),
        (
            233,
            "native_exphist_stale_left_seconds",
            "left-stale",
            3,
            [1, 2],
            true,
        ),
        (
            234,
            "native_exphist_stale_left_seconds",
            "right-stale",
            11,
            [5, 6],
            false,
        ),
        (
            235,
            "native_exphist_stale_left_seconds",
            "left-only",
            13,
            [6, 7],
            false,
        ),
        (
            236,
            "native_exphist_stale_right_seconds",
            "matched",
            7,
            [3, 4],
            false,
        ),
        (
            237,
            "native_exphist_stale_right_seconds",
            "left-stale",
            17,
            [8, 9],
            false,
        ),
        (
            238,
            "native_exphist_stale_right_seconds",
            "right-stale",
            19,
            [9, 10],
            true,
        ),
        (
            239,
            "native_exphist_stale_right_seconds",
            "right-only",
            23,
            [11, 12],
            false,
        ),
    ] {
        let value = exphist_value(count, count as f64, counts);
        let stale_value = exphist_value_with_metadata(0, 0.0, [0, 0], stale_metadata);
        let samples = [
            (0, value.clone()),
            (10_000, value.clone()),
            (20_000, value.clone()),
            (30_000, value.clone()),
            (40_000, if stale_latest { stale_value } else { value }),
        ];
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(series),
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", "/native-stale-match");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
}

fn write_native_exponential_histogram_binary_vector_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, count, sum, positive_counts) in [
        (
            SeriesRef::new(156),
            "native_exphist_left_seconds",
            25,
            25.0,
            [10, 15],
        ),
        (
            SeriesRef::new(157),
            "native_exphist_right_seconds",
            7,
            7.0,
            [3, 4],
        ),
    ] {
        let samples = [
            (0, exphist_value(count, sum, positive_counts)),
            (10_000, exphist_value(count, sum, positive_counts)),
            (20_000, exphist_value(count, sum, positive_counts)),
            (30_000, exphist_value(count, sum, positive_counts)),
            (40_000, exphist_value(count, sum, positive_counts)),
        ];
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", "/native");
                },
            )
            .unwrap();
    }
}

fn write_native_exponential_histogram_nonfinite_sum_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, sum) in [
        (
            SeriesRef::new(158),
            "native_exphist_pos_inf_sum_seconds",
            f64::INFINITY,
        ),
        (
            SeriesRef::new(159),
            "native_exphist_neg_inf_sum_seconds",
            f64::NEG_INFINITY,
        ),
        (
            SeriesRef::new(162),
            "native_exphist_finite_sum_seconds",
            5.0,
        ),
    ] {
        let samples = [
            (0, exphist_value(5, sum, [2, 3])),
            (10_000, exphist_value(5, sum, [2, 3])),
            (20_000, exphist_value(5, sum, [2, 3])),
            (30_000, exphist_value(5, sum, [2, 3])),
            (40_000, exphist_value(5, sum, [2, 3])),
        ];
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", "/native-nonfinite");
                },
            )
            .unwrap();
    }
}

fn write_native_exponential_histogram_group_modifier_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, method, extra_label, extra_value, count, sum, positive_counts) in [
        (
            SeriesRef::new(180),
            "native_exphist_group_many_seconds",
            "get",
            "code",
            "500",
            25,
            25.0,
            [10, 15],
        ),
        (
            SeriesRef::new(181),
            "native_exphist_group_many_seconds",
            "get",
            "code",
            "404",
            11,
            11.0,
            [4, 7],
        ),
        (
            SeriesRef::new(182),
            "native_exphist_group_one_seconds",
            "get",
            "cluster",
            "primary",
            7,
            7.0,
            [3, 4],
        ),
        (
            SeriesRef::new(183),
            "native_exphist_group_one_left_seconds",
            "post",
            "cluster",
            "primary",
            5,
            5.0,
            [2, 3],
        ),
        (
            SeriesRef::new(184),
            "native_exphist_group_many_right_seconds",
            "post",
            "instance",
            "a",
            20,
            20.0,
            [8, 12],
        ),
        (
            SeriesRef::new(185),
            "native_exphist_group_many_right_seconds",
            "post",
            "instance",
            "b",
            30,
            30.0,
            [12, 18],
        ),
    ] {
        let samples = [
            (0, exphist_value(count, sum, positive_counts)),
            (10_000, exphist_value(count, sum, positive_counts)),
            (20_000, exphist_value(count, sum, positive_counts)),
            (30_000, exphist_value(count, sum, positive_counts)),
            (40_000, exphist_value(count, sum, positive_counts)),
        ];
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", "/native-exphist-group");
                    visit("method", method);
                    visit(extra_label, extra_value);
                },
            )
            .unwrap();
    }
}

fn write_native_exponential_histogram_set_operator_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, route, count, sum, positive_counts) in [
        (
            SeriesRef::new(166),
            "native_exphist_set_left_seconds",
            "/native-set-match",
            25,
            25.0,
            [10, 15],
        ),
        (
            SeriesRef::new(167),
            "native_exphist_set_left_seconds",
            "/native-set-left-only",
            11,
            11.0,
            [4, 7],
        ),
        (
            SeriesRef::new(168),
            "native_exphist_set_right_seconds",
            "/native-set-match",
            7,
            7.0,
            [3, 4],
        ),
        (
            SeriesRef::new(169),
            "native_exphist_set_right_seconds",
            "/native-set-right-only",
            13,
            13.0,
            [5, 8],
        ),
    ] {
        let samples = [
            (0, exphist_value(count, sum, positive_counts)),
            (10_000, exphist_value(count, sum, positive_counts)),
            (20_000, exphist_value(count, sum, positive_counts)),
            (30_000, exphist_value(count, sum, positive_counts)),
            (40_000, exphist_value(count, sum, positive_counts)),
        ];
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", route);
                },
            )
            .unwrap();
    }
}

fn write_native_classic_histogram_series(writer: &mut SegmentWriter) {
    let samples = [
        (0, histogram_value(5, 5.0, [2, 2, 1])),
        (10_000, histogram_value(10, 10.0, [4, 4, 2])),
        (20_000, histogram_value(15, 15.0, [6, 6, 3])),
        (30_000, histogram_value(20, 20.0, [8, 8, 4])),
        (40_000, histogram_value(25, 25.0, [10, 10, 5])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(90),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_classic_seconds");
                visit("route", "/native");
            },
        )
        .unwrap();
}

fn write_native_histogram_resets_observable_decrease(writer: &mut SegmentWriter) {
    let samples = [
        (
            1_000,
            custom_histogram_value(10, 20.0, &[1.0, 2.0], &[2, 5, 3]),
        ),
        (
            6_000,
            custom_histogram_value(5, 12.0, &[1.0, 2.0], &[1, 3, 1]),
        ),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(91),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_custom_reset_seconds");
                visit("route", "/native-reset");
            },
        )
        .unwrap();
}

fn write_native_histogram_changes(writer: &mut SegmentWriter) {
    for (series_ref, case, samples) in [
        (
            SeriesRef::new(92),
            "change",
            [
                (
                    1_000,
                    custom_histogram_value(10, 20.0, &[1.0, 2.0], &[2, 5, 3]),
                ),
                (
                    6_000,
                    custom_histogram_value(12, 24.0, &[1.0, 2.0], &[3, 5, 4]),
                ),
            ],
        ),
        (
            SeriesRef::new(93),
            "same",
            [
                (
                    1_000,
                    custom_histogram_value(10, 20.0, &[1.0, 2.0], &[2, 5, 3]),
                ),
                (
                    6_000,
                    custom_histogram_value(10, 20.0, &[1.0, 2.0], &[2, 5, 3]),
                ),
            ],
        ),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, "native_custom_changes_seconds");
                visit("case", case);
            })
            .unwrap();
    }
}

fn write_native_histogram_stale_latest(writer: &mut SegmentWriter) {
    let stale_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        ..cumulative_not_reset_metadata()
    };
    let samples = [
        (0, histogram_value(5, 5.0, [2, 2, 1])),
        (10_000, histogram_value(10, 10.0, [4, 4, 2])),
        (20_000, histogram_value(15, 15.0, [6, 6, 3])),
        (30_000, histogram_value(20, 20.0, [8, 8, 4])),
        (
            40_000,
            histogram_value_with_metadata(0, 0.0, [0, 0, 0], stale_metadata),
        ),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(94),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_custom_stale_seconds");
                visit("route", "/native-stale");
            },
        )
        .unwrap();
}

fn write_native_histogram_stale_vector_matching(writer: &mut SegmentWriter) {
    let stale_metadata = TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        ..cumulative_not_reset_metadata()
    };

    for (series, metric, instance, count, bucket_counts, stale_latest) in [
        (
            95,
            "native_custom_stale_left_seconds",
            "matched",
            5,
            [2, 2, 1],
            false,
        ),
        (
            96,
            "native_custom_stale_left_seconds",
            "left-stale",
            3,
            [1, 1, 1],
            true,
        ),
        (
            97,
            "native_custom_stale_left_seconds",
            "right-stale",
            11,
            [5, 4, 2],
            false,
        ),
        (
            98,
            "native_custom_stale_left_seconds",
            "left-only",
            13,
            [6, 5, 2],
            false,
        ),
        (
            99,
            "native_custom_stale_right_seconds",
            "matched",
            7,
            [3, 2, 2],
            false,
        ),
        (
            100,
            "native_custom_stale_right_seconds",
            "left-stale",
            17,
            [8, 6, 3],
            false,
        ),
        (
            101,
            "native_custom_stale_right_seconds",
            "right-stale",
            19,
            [9, 7, 3],
            true,
        ),
        (
            102,
            "native_custom_stale_right_seconds",
            "right-only",
            23,
            [11, 8, 4],
            false,
        ),
    ] {
        let value = histogram_value(count, count as f64, bucket_counts);
        let stale_value = histogram_value_with_metadata(0, 0.0, [0, 0, 0], stale_metadata);
        let samples = [
            (0, value.clone()),
            (10_000, value.clone()),
            (20_000, value.clone()),
            (30_000, value.clone()),
            (40_000, if stale_latest { stale_value } else { value }),
        ];
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(series),
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", "/native-stale-match");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
}

fn write_native_histogram_binary_vector_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, count, sum, bucket_counts) in [
        (
            SeriesRef::new(154),
            "native_left_seconds",
            25,
            25.0,
            [10, 10, 5],
        ),
        (
            SeriesRef::new(155),
            "native_right_seconds",
            7,
            7.0,
            [3, 2, 2],
        ),
    ] {
        let samples = [
            (0, histogram_value(count, sum, bucket_counts)),
            (10_000, histogram_value(count, sum, bucket_counts)),
            (20_000, histogram_value(count, sum, bucket_counts)),
            (30_000, histogram_value(count, sum, bucket_counts)),
            (40_000, histogram_value(count, sum, bucket_counts)),
        ];
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, metric);
                visit("route", "/native");
            })
            .unwrap();
    }
}

fn write_native_histogram_nonfinite_sum_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, sum) in [
        (
            SeriesRef::new(160),
            "native_pos_inf_sum_seconds",
            f64::INFINITY,
        ),
        (
            SeriesRef::new(161),
            "native_neg_inf_sum_seconds",
            f64::NEG_INFINITY,
        ),
        (SeriesRef::new(163), "native_finite_sum_seconds", 5.0),
    ] {
        let samples = [
            (0, histogram_value(5, sum, [2, 2, 1])),
            (10_000, histogram_value(5, sum, [2, 2, 1])),
            (20_000, histogram_value(5, sum, [2, 2, 1])),
            (30_000, histogram_value(5, sum, [2, 2, 1])),
            (40_000, histogram_value(5, sum, [2, 2, 1])),
        ];
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, metric);
                visit("route", "/native-nonfinite");
            })
            .unwrap();
    }
}

fn write_native_histogram_ordering_bool_drop_series(writer: &mut SegmentWriter) {
    write_native_histogram_binary_vector_series(writer);
    write_native_exponential_histogram_binary_vector_series(writer);
}

fn write_native_histogram_group_modifier_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, method, extra_label, extra_value, count, sum, bucket_counts) in [
        (
            SeriesRef::new(174),
            "native_group_many_seconds",
            "get",
            "code",
            "500",
            25,
            25.0,
            [10, 10, 5],
        ),
        (
            SeriesRef::new(175),
            "native_group_many_seconds",
            "get",
            "code",
            "404",
            11,
            11.0,
            [4, 4, 3],
        ),
        (
            SeriesRef::new(176),
            "native_group_one_seconds",
            "get",
            "cluster",
            "primary",
            7,
            7.0,
            [3, 2, 2],
        ),
        (
            SeriesRef::new(177),
            "native_group_one_left_seconds",
            "post",
            "cluster",
            "primary",
            5,
            5.0,
            [2, 2, 1],
        ),
        (
            SeriesRef::new(178),
            "native_group_many_right_seconds",
            "post",
            "instance",
            "a",
            20,
            20.0,
            [8, 8, 4],
        ),
        (
            SeriesRef::new(179),
            "native_group_many_right_seconds",
            "post",
            "instance",
            "b",
            30,
            30.0,
            [12, 12, 6],
        ),
    ] {
        let samples = [
            (0, histogram_value(count, sum, bucket_counts)),
            (10_000, histogram_value(count, sum, bucket_counts)),
            (20_000, histogram_value(count, sum, bucket_counts)),
            (30_000, histogram_value(count, sum, bucket_counts)),
            (40_000, histogram_value(count, sum, bucket_counts)),
        ];
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, metric);
                visit("route", "/native-group");
                visit("method", method);
                visit(extra_label, extra_value);
            })
            .unwrap();
    }
}

fn write_native_histogram_set_operator_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, route, count, sum, bucket_counts) in [
        (
            SeriesRef::new(164),
            "native_set_left_seconds",
            "/native-set-match",
            25,
            25.0,
            [10, 10, 5],
        ),
        (
            SeriesRef::new(165),
            "native_set_left_seconds",
            "/native-set-left-only",
            11,
            11.0,
            [4, 4, 3],
        ),
        (
            SeriesRef::new(170),
            "native_set_right_seconds",
            "/native-set-match",
            7,
            7.0,
            [3, 2, 2],
        ),
        (
            SeriesRef::new(171),
            "native_set_right_seconds",
            "/native-set-right-only",
            13,
            13.0,
            [5, 5, 3],
        ),
    ] {
        let samples = [
            (0, histogram_value(count, sum, bucket_counts)),
            (10_000, histogram_value(count, sum, bucket_counts)),
            (20_000, histogram_value(count, sum, bucket_counts)),
            (30_000, histogram_value(count, sum, bucket_counts)),
            (40_000, histogram_value(count, sum, bucket_counts)),
        ];
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, metric);
                visit("route", route);
            })
            .unwrap();
    }
}

fn write_mixed_native_histogram_set_operator_series(writer: &mut SegmentWriter) {
    write_native_histogram_set_operator_series(writer);
    write_native_exponential_histogram_set_operator_series(writer);
}

fn write_mixed_native_histogram_binary_vector_series(writer: &mut SegmentWriter) {
    let histogram_samples = [
        (0, histogram_value(25, 25.0, [10, 10, 5])),
        (10_000, histogram_value(25, 25.0, [10, 10, 5])),
        (20_000, histogram_value(25, 25.0, [10, 10, 5])),
        (30_000, histogram_value(25, 25.0, [10, 10, 5])),
        (40_000, histogram_value(25, 25.0, [10, 10, 5])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(172),
            &histogram_samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_mixed_left_seconds");
                visit("route", "/native-mixed");
            },
        )
        .unwrap();

    let exponential_samples = [
        (0, exphist_value(7, 7.0, [3, 4])),
        (10_000, exphist_value(7, 7.0, [3, 4])),
        (20_000, exphist_value(7, 7.0, [3, 4])),
        (30_000, exphist_value(7, 7.0, [3, 4])),
        (40_000, exphist_value(7, 7.0, [3, 4])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(173),
            &exponential_samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_exphist_mixed_right_seconds");
                visit("route", "/native-mixed");
            },
        )
        .unwrap();
}

fn write_mixed_native_histogram_comparison_vector_matching_series(writer: &mut SegmentWriter) {
    let histogram_samples = [
        (0, histogram_value(25, 25.0, [10, 10, 5])),
        (10_000, histogram_value(25, 25.0, [10, 10, 5])),
        (20_000, histogram_value(25, 25.0, [10, 10, 5])),
        (30_000, histogram_value(25, 25.0, [10, 10, 5])),
        (40_000, histogram_value(25, 25.0, [10, 10, 5])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(186),
            &histogram_samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_mixed_match_left_seconds");
                visit("route", "/native-mixed-match");
                visit("method", "get");
                visit("side", "custom");
            },
        )
        .unwrap();

    let exponential_samples = [
        (0, exphist_value(7, 7.0, [3, 4])),
        (10_000, exphist_value(7, 7.0, [3, 4])),
        (20_000, exphist_value(7, 7.0, [3, 4])),
        (30_000, exphist_value(7, 7.0, [3, 4])),
        (40_000, exphist_value(7, 7.0, [3, 4])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(187),
            &exponential_samples,
            |visit| {
                visit(
                    METRIC_NAME_LABEL,
                    "native_exphist_mixed_match_right_seconds",
                );
                visit("route", "/native-mixed-match");
                visit("method", "get");
                visit("side", "exponential");
            },
        )
        .unwrap();
}

fn write_mixed_native_histogram_comparison_group_modifier_series(writer: &mut SegmentWriter) {
    for (series_ref, metric, method, extra_label, extra_value, count, sum, bucket_counts) in [
        (
            SeriesRef::new(188),
            "native_mixed_group_many_seconds",
            "get",
            "code",
            "500",
            25,
            25.0,
            [10, 10, 5],
        ),
        (
            SeriesRef::new(189),
            "native_mixed_group_many_seconds",
            "get",
            "code",
            "404",
            11,
            11.0,
            [4, 4, 3],
        ),
        (
            SeriesRef::new(190),
            "native_mixed_group_one_seconds",
            "post",
            "cluster",
            "primary",
            5,
            5.0,
            [2, 2, 1],
        ),
    ] {
        let samples = [
            (0, histogram_value(count, sum, bucket_counts)),
            (10_000, histogram_value(count, sum, bucket_counts)),
            (20_000, histogram_value(count, sum, bucket_counts)),
            (30_000, histogram_value(count, sum, bucket_counts)),
            (40_000, histogram_value(count, sum, bucket_counts)),
        ];
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, metric);
                visit("route", "/native-mixed-group");
                visit("method", method);
                visit(extra_label, extra_value);
            })
            .unwrap();
    }

    for (series_ref, metric, method, extra_label, extra_value, count, sum, positive_counts) in [
        (
            SeriesRef::new(191),
            "native_exphist_mixed_group_one_seconds",
            "get",
            "cluster",
            "primary",
            7,
            7.0,
            [3, 4],
        ),
        (
            SeriesRef::new(192),
            "native_exphist_mixed_group_many_seconds",
            "post",
            "instance",
            "a",
            20,
            20.0,
            [8, 12],
        ),
        (
            SeriesRef::new(193),
            "native_exphist_mixed_group_many_seconds",
            "post",
            "instance",
            "b",
            30,
            30.0,
            [12, 18],
        ),
    ] {
        let samples = [
            (0, exphist_value(count, sum, positive_counts)),
            (10_000, exphist_value(count, sum, positive_counts)),
            (20_000, exphist_value(count, sum, positive_counts)),
            (30_000, exphist_value(count, sum, positive_counts)),
            (40_000, exphist_value(count, sum, positive_counts)),
        ];
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &samples,
                |visit| {
                    visit(METRIC_NAME_LABEL, metric);
                    visit("route", "/native-mixed-group");
                    visit("method", method);
                    visit(extra_label, extra_value);
                },
            )
            .unwrap();
    }
}

fn write_native_custom_layout_change_histogram_series(writer: &mut SegmentWriter) {
    let samples = [
        (
            1_000,
            custom_histogram_value(10, 20.0, &[1.0, 2.0, 4.0], &[2, 5, 3, 0]),
        ),
        (
            6_000,
            custom_histogram_value(20, 40.0, &[1.0, 3.0, 4.0], &[4, 10, 6, 0]),
        ),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(151),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "native_custom_layout_seconds");
                visit("route", "/native-layout-change");
            },
        )
        .unwrap();
}

fn write_native_custom_layout_sum_histogram_series(writer: &mut SegmentWriter) {
    for (series_ref, instance, bounds) in [
        (SeriesRef::new(152), "a", vec![1.0, 2.0, 4.0]),
        (SeriesRef::new(153), "b", vec![1.0, 3.0, 4.0]),
    ] {
        let samples = [
            (
                1_000,
                custom_histogram_value(10, 20.0, &bounds, &[2, 5, 3, 0]),
            ),
            (
                6_000,
                custom_histogram_value(20, 40.0, &bounds, &[4, 10, 6, 0]),
            ),
        ];
        writer
            .record_histogram_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                visit(METRIC_NAME_LABEL, "native_custom_sum_seconds");
                visit("route", "/native-layout-sum");
                visit("instance", instance);
            })
            .unwrap();
    }
}

fn write_histogram_scalar_float_only_series(writer: &mut SegmentWriter) {
    for (series, instance, samples) in [
        (
            147,
            "a",
            vec![
                (0, 1.0),
                (10_000, 2.0),
                (20_000, 3.0),
                (30_000, 4.0),
                (40_000, 5.0),
            ],
        ),
        (
            148,
            "b",
            vec![
                (0, 2.0),
                (10_000, 3.0),
                (20_000, 4.0),
                (30_000, 5.0),
                (40_000, 6.0),
            ],
        ),
    ] {
        write_float_series(
            writer,
            series,
            &[
                (METRIC_NAME_LABEL, "cpu_usage"),
                ("job", "api"),
                ("instance", instance),
            ],
            &samples,
        );
    }
}

fn write_mixed_float_and_native_histogram_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        149,
        &[
            (METRIC_NAME_LABEL, "mixed_histogram_seconds"),
            ("job", "api"),
            ("kind", "float"),
        ],
        &[
            (0, 7.0),
            (10_000, 7.0),
            (20_000, 7.0),
            (30_000, 7.0),
            (40_000, 7.0),
        ],
    );

    let samples = [
        (0, histogram_value(5, 5.0, [2, 2, 1])),
        (10_000, histogram_value(10, 10.0, [4, 4, 2])),
        (20_000, histogram_value(15, 15.0, [6, 6, 3])),
        (30_000, histogram_value(20, 20.0, [8, 8, 4])),
        (40_000, histogram_value(25, 25.0, [10, 10, 5])),
    ];
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(150),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "mixed_histogram_seconds");
                visit("job", "api");
                visit("kind", "hist");
            },
        )
        .unwrap();
}

fn write_mixed_float_and_native_exponential_histogram_series(writer: &mut SegmentWriter) {
    write_float_series(
        writer,
        232,
        &[
            (METRIC_NAME_LABEL, "mixed_exphist_seconds"),
            ("job", "api"),
            ("kind", "float"),
        ],
        &[
            (0, 7.0),
            (10_000, 7.0),
            (20_000, 7.0),
            (30_000, 7.0),
            (40_000, 7.0),
        ],
    );

    let samples = [
        (0, exphist_value(5, 5.0, [2, 3])),
        (10_000, exphist_value(10, 10.0, [4, 6])),
        (20_000, exphist_value(15, 15.0, [6, 9])),
        (30_000, exphist_value(20, 20.0, [8, 12])),
        (40_000, exphist_value(25, 25.0, [10, 15])),
    ];
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(233),
            &samples,
            |visit| {
                visit(METRIC_NAME_LABEL, "mixed_exphist_seconds");
                visit("job", "api");
                visit("kind", "hist");
            },
        )
        .unwrap();
}

fn exphist_value(count: u64, sum: f64, positive_counts: [u64; 2]) -> ExponentialHistogramValue {
    exphist_value_with_metadata(count, sum, positive_counts, cumulative_not_reset_metadata())
}

fn delta_exphist_value(
    count: u64,
    sum: f64,
    positive_counts: [u64; 2],
) -> ExponentialHistogramValue {
    exphist_value_with_metadata(count, sum, positive_counts, delta_not_reset_metadata())
}

fn exphist_value_with_metadata(
    count: u64,
    sum: f64,
    positive_counts: [u64; 2],
    metadata: TypedSampleMetadata,
) -> ExponentialHistogramValue {
    ExponentialHistogramValue {
        count,
        sum: Some(sum),
        min: None,
        max: None,
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 0,
        metadata,
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: positive_counts.into(),
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    }
}

fn delta_not_reset_metadata() -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint: CounterResetHint::NotCounterReset,
    }
}

fn delta_reset_metadata() -> TypedSampleMetadata {
    TypedSampleMetadata {
        reset_hint: CounterResetHint::CounterReset,
        ..delta_not_reset_metadata()
    }
}

fn delta_stale_metadata() -> TypedSampleMetadata {
    TypedSampleMetadata {
        flags: OTLP_FLAG_NO_RECORDED_VALUE,
        ..delta_not_reset_metadata()
    }
}

fn cumulative_not_reset_metadata() -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
    }
}

fn exphist_bucket_projection_config() -> QueryProjectionConfig {
    QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(vec![2.0])
}
