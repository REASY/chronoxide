use std::{
    env, fs, io,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use chronoxide_core::{
    labels::{
        DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore,
        METRIC_NAME_LABEL, SeriesRef,
    },
    storage::{
        head::{
            CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue,
            FloatEncoding, HeadBuffer, HeadConfig, HistogramValue, IntEncoding,
            OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality, SampleValue,
            SummaryQuantileValue, SummaryValue, TypedSampleMetadata, prometheus_stale_nan,
        },
        segment::{
            QueryProjectionConfig, SegmentQueryResult, SegmentStoreReader, SegmentWriter,
            SegmentWriterConfig,
        },
    },
};

#[test]
#[ignore = "requires promtool; set CHRONOXIDE_PROMTOOL or install promtool"]
fn prometheus_golden_suite_matches_current_promql_surface() {
    assert_prometheus_golden_cases();
}

struct PromInputSeries {
    series: &'static str,
    values: &'static str,
}

struct GoldenCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    eval_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide: fn(&mut SegmentWriter),
    projection_config: fn() -> QueryProjectionConfig,
    expect_non_empty: bool,
}

struct GoldenRangeCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    start_secs: u64,
    end_secs: u64,
    step_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide: fn(&mut SegmentWriter),
    projection_config: fn() -> QueryProjectionConfig,
}

struct GoldenHeadRangeCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    start_secs: u64,
    end_secs: u64,
    step_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide:
        fn(&mut SegmentWriter, &mut FlatInternedLabelSetStore<DefaultSymbolTable>, &mut HeadBuffer),
    projection_config: fn() -> QueryProjectionConfig,
}

struct GoldenErrorCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    eval_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide: fn(&mut SegmentWriter),
    projection_config: fn() -> QueryProjectionConfig,
    expected_chronoxide_error: &'static str,
    expected_promtool_error: &'static str,
}

fn assert_prometheus_golden_cases() {
    let promtool = find_promtool();
    let cases = golden_cases();
    assert!(!cases.is_empty(), "golden suite must contain cases");

    for case in cases {
        assert_prometheus_golden_case(&promtool, case);
    }

    let error_cases = golden_error_cases();
    assert!(
        !error_cases.is_empty(),
        "golden suite must contain error cases"
    );
    for case in error_cases {
        assert_prometheus_golden_error_case(&promtool, case);
    }

    let range_cases = golden_range_cases();
    assert!(
        !range_cases.is_empty(),
        "golden suite must contain range cases"
    );
    for case in range_cases {
        assert_prometheus_golden_range_case(&promtool, case);
    }

    let head_range_cases = golden_head_range_cases();
    assert!(
        !head_range_cases.is_empty(),
        "golden suite must contain head-aware range cases"
    );
    for case in head_range_cases {
        assert_prometheus_golden_head_range_case(&promtool, case);
    }
}

fn golden_cases() -> Vec<GoldenCase> {
    vec![
        GoldenCase {
            name: "float_counter_rate_sum_by",
            chronoxide_query: r#"sum by (route)(rate(http_requests_total{job="api"}[30s]))"#,
            prom_query: r#"sum by (route)(rate(http_requests_total{job="api"}[30s]))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="a"}"#,
                    values: "0 10 20 30 40",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="b"}"#,
                    values: "0 5 10 15 20",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/search",instance="a"}"#,
                    values: "0 2 4 6 8",
                },
            ],
            write_chronoxide: write_float_counter_rate_sum_by,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "label_replace_and_join",
            chronoxide_query: r#"label_replace(label_join(cpu_usage{job="api"}, "target", "/", "job", "instance"), "service", "$1", "job", "(.+)")"#,
            prom_query: r#"label_replace(label_join(cpu_usage{job="api"}, "target", "/", "job", "instance"), "service", "$1", "job", "(.+)")"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"cpu_usage{job="api",instance="a"}"#,
                values: "1 2 3 4 5",
            }],
            write_chronoxide: write_label_replace_and_join,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "absent_with_equality_labels",
            chronoxide_query: r#"absent(nonexistent_total{job="api",instance="a"})"#,
            prom_query: r#"absent(nonexistent_total{job="api",instance="a"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"unrelated_metric{job="api"}"#,
                values: "1 1 1 1 1",
            }],
            write_chronoxide: write_unrelated_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "absent_over_time_with_equality_labels",
            chronoxide_query: r#"absent_over_time(nonexistent_total{job="api",instance="a"}[30s])"#,
            prom_query: r#"absent_over_time(nonexistent_total{job="api",instance="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"unrelated_metric{job="api"}"#,
                values: "1 1 1 1 1",
            }],
            write_chronoxide: write_unrelated_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "absent_over_time_stale_only_range",
            chronoxide_query: r#"absent_over_time(stale_only_total{job="api"}[30s])"#,
            prom_query: r#"absent_over_time(stale_only_total{job="api"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"stale_only_total{job="api"}"#,
                values: "_ _ _ _ stale",
            }],
            write_chronoxide: write_stale_only_absent_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "quantile_over_time_median",
            chronoxide_query: r#"quantile_over_time(0.5, temperature_celsius{sensor="rack-a"}[30s])"#,
            prom_query: r#"quantile_over_time(0.5, temperature_celsius{sensor="rack-a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"temperature_celsius{sensor="rack-a"}"#,
                values: "10 12 14 16 18",
            }],
            write_chronoxide: write_temperature_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_last_over_time",
            chronoxide_query: r#"last_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"last_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_count_over_time",
            chronoxide_query: r#"count_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"count_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_present_over_time",
            chronoxide_query: r#"present_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"present_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_sum_over_time",
            chronoxide_query: r#"sum_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"sum_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_avg_over_time",
            chronoxide_query: r#"avg_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"avg_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_delta",
            chronoxide_query: r#"delta(gauge_value{series="a"}[30s])"#,
            prom_query: r#"delta(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_stdvar",
            chronoxide_query: r#"stdvar_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"stdvar_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_idelta",
            chronoxide_query: r#"idelta(gauge_value{series="a"}[30s])"#,
            prom_query: r#"idelta(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_stddev",
            chronoxide_query: r#"stddev_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"stddev_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_function_min_max_composition",
            chronoxide_query: r#"max_over_time(gauge_value{series="a"}[30s]) - min_over_time(gauge_value{series="a"}[30s])"#,
            prom_query: r#"max_over_time(gauge_value{series="a"}[30s]) - min_over_time(gauge_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "predict_linear",
            chronoxide_query: r#"predict_linear(temperature_celsius{sensor="rack-a"}[30s], 10)"#,
            prom_query: r#"predict_linear(temperature_celsius{sensor="rack-a"}[30s], 10)"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"temperature_celsius{sensor="rack-a"}"#,
                values: "10 12 14 16 18",
            }],
            write_chronoxide: write_temperature_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "deriv",
            chronoxide_query: r#"deriv(temperature_celsius{sensor="rack-a"}[30s])"#,
            prom_query: r#"deriv(temperature_celsius{sensor="rack-a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"temperature_celsius{sensor="rack-a"}"#,
                values: "10 12 14 16 18",
            }],
            write_chronoxide: write_temperature_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "counter_increase_with_reset",
            chronoxide_query: r#"increase(reset_counter_total{series="a"}[40s])"#,
            prom_query: r#"increase(reset_counter_total{series="a"}[40s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"reset_counter_total{series="a"}"#,
                values: "0 10 5 15 25",
            }],
            write_chronoxide: write_reset_counter_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "counter_irate_after_reset",
            chronoxide_query: r#"irate(reset_counter_total{series="a"}[30s])"#,
            prom_query: r#"irate(reset_counter_total{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"reset_counter_total{series="a"}"#,
                values: "0 10 5 15 25",
            }],
            write_chronoxide: write_reset_counter_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "counter_resets",
            chronoxide_query: r#"resets(reset_counter_total{series="a"}[40s])"#,
            prom_query: r#"resets(reset_counter_total{series="a"}[40s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"reset_counter_total{series="a"}"#,
                values: "0 10 5 15 25",
            }],
            write_chronoxide: write_reset_counter_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "counter_changes",
            chronoxide_query: r#"changes(reset_counter_total{series="a"}[40s])"#,
            prom_query: r#"changes(reset_counter_total{series="a"}[40s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"reset_counter_total{series="a"}"#,
                values: "0 10 5 15 25",
            }],
            write_chronoxide: write_reset_counter_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_topk",
            chronoxide_query: r#"topk(2, cpu_usage{job="api"})"#,
            prom_query: r#"topk(2, cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_quantile_by_job",
            chronoxide_query: r#"quantile by (job)(0.5, cpu_usage{job="api"})"#,
            prom_query: r#"quantile by (job)(0.5, cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_bottomk",
            chronoxide_query: r#"bottomk(2, cpu_usage{job="api"})"#,
            prom_query: r#"bottomk(2, cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_count_values",
            chronoxide_query: r#"count_values by (job)("value", cpu_usage{job="api"})"#,
            prom_query: r#"count_values by (job)("value", cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_common_float_ops_by_job",
            chronoxide_query: r#"sum by (job)(cpu_usage{job="api"}) + count by (job)(cpu_usage{job="api"}) + avg by (job)(cpu_usage{job="api"}) + min by (job)(cpu_usage{job="api"}) + max by (job)(cpu_usage{job="api"}) + stddev by (job)(cpu_usage{job="api"}) + stdvar by (job)(cpu_usage{job="api"}) + group by (job)(cpu_usage{job="api"})"#,
            prom_query: r#"sum by (job)(cpu_usage{job="api"}) + count by (job)(cpu_usage{job="api"}) + avg by (job)(cpu_usage{job="api"}) + min by (job)(cpu_usage{job="api"}) + max by (job)(cpu_usage{job="api"}) + stddev by (job)(cpu_usage{job="api"}) + stdvar by (job)(cpu_usage{job="api"}) + group by (job)(cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "sort_desc_function",
            chronoxide_query: r#"sort_desc(cpu_usage{job="api"})"#,
            prom_query: r#"sort_desc(cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "sort_function",
            chronoxide_query: r#"sort(cpu_usage{job="api"})"#,
            prom_query: r#"sort(cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "binary_vector_matching_ignoring",
            chronoxide_query: r#"errors_total{code="500"} / ignoring(code) requests_total"#,
            prom_query: r#"errors_total{code="500"} / ignoring(code) requests_total"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"errors_total{job="api",instance="a",code="500"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"requests_total{job="api",instance="a"}"#,
                    values: "10 20 30 40 50",
                },
            ],
            write_chronoxide: write_error_request_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "binary_group_left",
            chronoxide_query: r#"http_errors / ignoring(code) group_left http_requests"#,
            prom_query: r#"http_errors / ignoring(code) group_left http_requests"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"http_errors{method="get",code="500"}"#,
                    values: "24 24 24 24 24",
                },
                PromInputSeries {
                    series: r#"http_errors{method="get",code="404"}"#,
                    values: "30 30 30 30 30",
                },
                PromInputSeries {
                    series: r#"http_errors{method="post",code="500"}"#,
                    values: "6 6 6 6 6",
                },
                PromInputSeries {
                    series: r#"http_errors{method="post",code="404"}"#,
                    values: "21 21 21 21 21",
                },
                PromInputSeries {
                    series: r#"http_requests{method="get"}"#,
                    values: "600 600 600 600 600",
                },
                PromInputSeries {
                    series: r#"http_requests{method="post"}"#,
                    values: "120 120 120 120 120",
                },
            ],
            write_chronoxide: write_group_left_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "binary_group_right_include_labels",
            chronoxide_query: r#"cpu_limit{route="/group-right"} / on(route) group_right(service) cpu_usage_group_right{route="/group-right"}"#,
            prom_query: r#"cpu_limit{route="/group-right"} / on(route) group_right(service) cpu_usage_group_right{route="/group-right"}"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_limit{route="/group-right",service="api"}"#,
                    values: "10 10 10 10 10",
                },
                PromInputSeries {
                    series: r#"cpu_usage_group_right{route="/group-right",instance="a"}"#,
                    values: "2 2 2 2 2",
                },
                PromInputSeries {
                    series: r#"cpu_usage_group_right{route="/group-right",instance="b"}"#,
                    values: "4 4 4 4 4",
                },
            ],
            write_chronoxide: write_group_right_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "binary_comparison_bool",
            chronoxide_query: r#"cpu_usage{job="api"} > bool 5"#,
            prom_query: r#"cpu_usage{job="api"} > bool 5"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "set_operator_unless",
            chronoxide_query: r#"cpu_usage{job="api"} unless cpu_usage{instance="b"}"#,
            prom_query: r#"cpu_usage{job="api"} unless cpu_usage{instance="b"}"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "set_operator_and",
            chronoxide_query: r#"cpu_usage{job="api"} and cpu_usage{instance="b"}"#,
            prom_query: r#"cpu_usage{job="api"} and cpu_usage{instance="b"}"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "set_operator_or",
            chronoxide_query: r#"cpu_usage{instance="a"} or cpu_usage{instance="b"}"#,
            prom_query: r#"cpu_usage{instance="a"} or cpu_usage{instance="b"}"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "binary_comparison_filter",
            chronoxide_query: r#"cpu_usage{job="api"} > 5"#,
            prom_query: r#"cpu_usage{job="api"} > 5"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "stale_latest_sample_is_absent_from_aggregation",
            chronoxide_query: r#"sum by (route)(stale_mix{route="/stale"})"#,
            prom_query: r#"sum by (route)(stale_mix{route="/stale"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"stale_mix{route="/stale",instance="finite"}"#,
                    values: "2 2 2 2 2",
                },
                PromInputSeries {
                    series: r#"stale_mix{route="/stale",instance="stale"}"#,
                    values: "1 1 1 1 stale",
                },
            ],
            write_chronoxide: write_stale_mix_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "stale_binary_vector_matching",
            chronoxide_query: r#"sum(stale_binary_left{route="/stale-binary"} + on(route, instance) stale_binary_right{route="/stale-binary"}) + sum(stale_binary_left{route="/stale-binary"} or on(route, instance) stale_binary_right{route="/stale-binary"}) + sum(stale_binary_left{route="/stale-binary"} unless on(route, instance) stale_binary_right{route="/stale-binary"})"#,
            prom_query: r#"sum(stale_binary_left{route="/stale-binary"} + on(route, instance) stale_binary_right{route="/stale-binary"}) + sum(stale_binary_left{route="/stale-binary"} or on(route, instance) stale_binary_right{route="/stale-binary"}) + sum(stale_binary_left{route="/stale-binary"} unless on(route, instance) stale_binary_right{route="/stale-binary"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"stale_binary_left{route="/stale-binary",instance="matched"}"#,
                    values: "2 2 2 2 2",
                },
                PromInputSeries {
                    series: r#"stale_binary_left{route="/stale-binary",instance="left-stale"}"#,
                    values: "3 3 3 3 stale",
                },
                PromInputSeries {
                    series: r#"stale_binary_left{route="/stale-binary",instance="right-stale"}"#,
                    values: "5 5 5 5 5",
                },
                PromInputSeries {
                    series: r#"stale_binary_left{route="/stale-binary",instance="left-only"}"#,
                    values: "7 7 7 7 7",
                },
                PromInputSeries {
                    series: r#"stale_binary_right{route="/stale-binary",instance="matched"}"#,
                    values: "10 10 10 10 10",
                },
                PromInputSeries {
                    series: r#"stale_binary_right{route="/stale-binary",instance="left-stale"}"#,
                    values: "20 20 20 20 20",
                },
                PromInputSeries {
                    series: r#"stale_binary_right{route="/stale-binary",instance="right-stale"}"#,
                    values: "30 30 30 30 stale",
                },
                PromInputSeries {
                    series: r#"stale_binary_right{route="/stale-binary",instance="right-only"}"#,
                    values: "11 11 11 11 11",
                },
            ],
            write_chronoxide: write_stale_binary_vector_matching_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "count_values_nonfinite_label_spelling",
            chronoxide_query: r#"count_values by (route)("value", nonfinite_value{route="/nonfinite"})"#,
            prom_query: r#"count_values by (route)("value", nonfinite_value{route="/nonfinite"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="nan"}"#,
                    values: "NaN NaN NaN NaN NaN",
                },
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="inf"}"#,
                    values: "+Inf +Inf +Inf +Inf +Inf",
                },
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="neg-inf"}"#,
                    values: "-Inf -Inf -Inf -Inf -Inf",
                },
            ],
            write_chronoxide: write_nonfinite_value_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_sum_positive_infinity",
            chronoxide_query: r#"sum by (route)(positive_inf_agg{route="/agg"})"#,
            prom_query: r#"sum by (route)(positive_inf_agg{route="/agg"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"positive_inf_agg{route="/agg",instance="finite"}"#,
                    values: "2 2 2 2 2",
                },
                PromInputSeries {
                    series: r#"positive_inf_agg{route="/agg",instance="pos"}"#,
                    values: "+Inf +Inf +Inf +Inf +Inf",
                },
            ],
            write_chronoxide: write_positive_inf_aggregation_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_avg_positive_infinity",
            chronoxide_query: r#"avg by (route)(positive_inf_agg{route="/agg"})"#,
            prom_query: r#"avg by (route)(positive_inf_agg{route="/agg"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"positive_inf_agg{route="/agg",instance="finite"}"#,
                    values: "2 2 2 2 2",
                },
                PromInputSeries {
                    series: r#"positive_inf_agg{route="/agg",instance="pos"}"#,
                    values: "+Inf +Inf +Inf +Inf +Inf",
                },
            ],
            write_chronoxide: write_positive_inf_aggregation_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_sum_mixed_infinities_produces_nan",
            chronoxide_query: r#"sum by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"}) != bool sum by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"})"#,
            prom_query: r#"sum by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"}) != bool sum by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="nan"}"#,
                    values: "NaN NaN NaN NaN NaN",
                },
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="inf"}"#,
                    values: "+Inf +Inf +Inf +Inf +Inf",
                },
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="neg-inf"}"#,
                    values: "-Inf -Inf -Inf -Inf -Inf",
                },
            ],
            write_chronoxide: write_nonfinite_value_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "aggregation_avg_mixed_infinities_produces_nan",
            chronoxide_query: r#"avg by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"}) != bool avg by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"})"#,
            prom_query: r#"avg by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"}) != bool avg by (route)(nonfinite_value{route="/nonfinite",instance=~"inf|neg-inf"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="nan"}"#,
                    values: "NaN NaN NaN NaN NaN",
                },
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="inf"}"#,
                    values: "+Inf +Inf +Inf +Inf +Inf",
                },
                PromInputSeries {
                    series: r#"nonfinite_value{route="/nonfinite",instance="neg-inf"}"#,
                    values: "-Inf -Inf -Inf -Inf -Inf",
                },
            ],
            write_chronoxide: write_nonfinite_value_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "binary_scalar_preserves_positive_infinity",
            chronoxide_query: r#"nonfinite_value{route="/nonfinite",instance="inf"} + 1"#,
            prom_query: r#"nonfinite_value{route="/nonfinite",instance="inf"} + 1"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"nonfinite_value{route="/nonfinite",instance="inf"}"#,
                values: "+Inf +Inf +Inf +Inf +Inf",
            }],
            write_chronoxide: write_nonfinite_value_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_sum_over_time_positive_infinity",
            chronoxide_query: r#"sum_over_time(positive_inf_range{case="mixed"}[40s])"#,
            prom_query: r#"sum_over_time(positive_inf_range{case="mixed"}[40s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"positive_inf_range{case="mixed"}"#,
                values: "1 2 +Inf 4 5",
            }],
            write_chronoxide: write_positive_inf_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_avg_over_time_positive_infinity",
            chronoxide_query: r#"avg_over_time(positive_inf_range{case="mixed"}[40s])"#,
            prom_query: r#"avg_over_time(positive_inf_range{case="mixed"}[40s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"positive_inf_range{case="mixed"}"#,
                values: "1 2 +Inf 4 5",
            }],
            write_chronoxide: write_positive_inf_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "range_count_over_time_skips_stale_marker",
            chronoxide_query: r#"count_over_time(stale_range_value{series="a"}[30s])"#,
            prom_query: r#"count_over_time(stale_range_value{series="a"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"stale_range_value{series="a"}"#,
                values: "1 2 stale 8 16",
            }],
            write_chronoxide: write_stale_range_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "scalar_function_single_vector",
            chronoxide_query: r#"scalar(cpu_usage{job="api",instance="a"})"#,
            prom_query: r#"scalar(cpu_usage{job="api",instance="a"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "timestamp_function",
            chronoxide_query: r#"timestamp(cpu_usage{job="api",instance="a"})"#,
            prom_query: r#"timestamp(cpu_usage{job="api",instance="a"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="c"}"#,
                    values: "3 4 5 6 7",
                },
            ],
            write_chronoxide: write_cpu_multi_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "scalar_sgn_trigonometric",
            chronoxide_query: r#"sgn(sin(vector(pi() / 2)) - 0.5)"#,
            prom_query: r#"sgn(sin(vector(pi() / 2)) - 0.5)"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"unrelated_metric{job="api"}"#,
                values: "1 1 1 1 1",
            }],
            write_chronoxide: write_unrelated_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "math_function_family",
            chronoxide_query: r#"abs(vector(-4)) + ceil(vector(1.2)) + floor(vector(1.8)) + round(vector(1.25), 0.5) + clamp(vector(3), 1, 2) + clamp_min(vector(0.5), 1) + clamp_max(vector(3), 2) + ln(vector(8)) + log2(vector(8)) + log10(vector(100)) + deg(vector(pi())) + rad(vector(180)) + cos(vector(0)) + tan(vector(0)) + acos(vector(1)) + asin(vector(0)) + atan(vector(1)) + cosh(vector(0)) + sinh(vector(0)) + tanh(vector(0)) + acosh(vector(1)) + asinh(vector(0)) + atanh(vector(0))"#,
            prom_query: r#"abs(vector(-4)) + ceil(vector(1.2)) + floor(vector(1.8)) + round(vector(1.25), 0.5) + clamp(vector(3), 1, 2) + clamp_min(vector(0.5), 1) + clamp_max(vector(3), 2) + ln(vector(8)) + log2(vector(8)) + log10(vector(100)) + deg(vector(pi())) + rad(vector(180)) + cos(vector(0)) + tan(vector(0)) + acos(vector(1)) + asin(vector(0)) + atan(vector(1)) + cosh(vector(0)) + sinh(vector(0)) + tanh(vector(0)) + acosh(vector(1)) + asinh(vector(0)) + atanh(vector(0))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"unrelated_metric{job="api"}"#,
                values: "1 1 1 1 1",
            }],
            write_chronoxide: write_unrelated_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "calendar_function_hour",
            chronoxide_query: r#"hour(vector(3600))"#,
            prom_query: r#"hour(vector(3600))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"unrelated_metric{job="api"}"#,
                values: "1 1 1 1 1",
            }],
            write_chronoxide: write_unrelated_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "calendar_function_family",
            chronoxide_query: r#"minute(vector(0)) + hour(vector(0)) + day_of_month(vector(0)) + day_of_week(vector(0)) + day_of_year(vector(0)) + days_in_month(vector(0)) + month(vector(0)) + year(vector(0))"#,
            prom_query: r#"minute(vector(0)) + hour(vector(0)) + day_of_month(vector(0)) + day_of_week(vector(0)) + day_of_year(vector(0)) + days_in_month(vector(0)) + month(vector(0)) + year(vector(0))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"unrelated_metric{job="api"}"#,
                values: "1 1 1 1 1",
            }],
            write_chronoxide: write_unrelated_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "classic_histogram_quantile_over_bucket_rate",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(classic_request_duration_seconds_bucket[30s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(classic_request_duration_seconds_bucket[30s])))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"classic_request_duration_seconds_bucket{route="/checkout",le="1"}"#,
                    values: "2 4 6 8 10",
                },
                PromInputSeries {
                    series: r#"classic_request_duration_seconds_bucket{route="/checkout",le="2"}"#,
                    values: "4 8 12 16 20",
                },
                PromInputSeries {
                    series: r#"classic_request_duration_seconds_bucket{route="/checkout",le="+Inf"}"#,
                    values: "5 10 15 20 25",
                },
            ],
            write_chronoxide: write_classic_histogram_bucket_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_histogram_bucket_projection_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(otlp_request_duration_seconds_bucket[30s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(otlp_request_duration_seconds_bucket[30s])))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"otlp_request_duration_seconds_bucket{route="/checkout",le="1"}"#,
                    values: "2 4 6 8 10",
                },
                PromInputSeries {
                    series: r#"otlp_request_duration_seconds_bucket{route="/checkout",le="2"}"#,
                    values: "4 8 12 16 20",
                },
                PromInputSeries {
                    series: r#"otlp_request_duration_seconds_bucket{route="/checkout",le="+Inf"}"#,
                    values: "5 10 15 20 25",
                },
            ],
            write_chronoxide: write_otlp_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_summary_quantile_projection",
            chronoxide_query: r#"last_over_time(rpc_duration_seconds{route="/summary",quantile="0.9"}[30s])"#,
            prom_query: r#"last_over_time(rpc_duration_seconds{route="/summary",quantile="0.9"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"rpc_duration_seconds{route="/summary",quantile="0.9"}"#,
                values: "0.42 0.43 0.44 0.45 0.46",
            }],
            write_chronoxide: write_otlp_summary_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_summary_count_projection",
            chronoxide_query: r#"last_over_time(rpc_duration_seconds_count{route="/summary"}[30s])"#,
            prom_query: r#"last_over_time(rpc_duration_seconds_count{route="/summary"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"rpc_duration_seconds_count{route="/summary"}"#,
                values: "10 20 30 40 50",
            }],
            write_chronoxide: write_otlp_summary_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_summary_sum_projection",
            chronoxide_query: r#"last_over_time(rpc_duration_seconds_sum{route="/summary"}[30s])"#,
            prom_query: r#"last_over_time(rpc_duration_seconds_sum{route="/summary"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"rpc_duration_seconds_sum{route="/summary"}"#,
                values: "2 4 6 8 10",
            }],
            write_chronoxide: write_otlp_summary_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_delta_histogram_count_sum_projection",
            chronoxide_query: r#"last_over_time(otlp_delta_request_duration_seconds_count{route="/delta"}[30s]) + last_over_time(otlp_delta_request_duration_seconds_sum{route="/delta"}[30s])"#,
            prom_query: r#"last_over_time(otlp_delta_request_duration_seconds_count{route="/delta"}[30s]) + last_over_time(otlp_delta_request_duration_seconds_sum{route="/delta"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"otlp_delta_request_duration_seconds_count{route="/delta"}"#,
                    values: "5 10 15 20 25",
                },
                PromInputSeries {
                    series: r#"otlp_delta_request_duration_seconds_sum{route="/delta"}"#,
                    values: "5 10 15 20 25",
                },
            ],
            write_chronoxide: write_otlp_delta_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_delta_histogram_bucket_projection_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(otlp_delta_request_duration_seconds_bucket[30s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(otlp_delta_request_duration_seconds_bucket[30s])))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"otlp_delta_request_duration_seconds_bucket{route="/delta",le="1"}"#,
                    values: "2 4 6 8 10",
                },
                PromInputSeries {
                    series: r#"otlp_delta_request_duration_seconds_bucket{route="/delta",le="2"}"#,
                    values: "4 8 12 16 20",
                },
                PromInputSeries {
                    series: r#"otlp_delta_request_duration_seconds_bucket{route="/delta",le="+Inf"}"#,
                    values: "5 10 15 20 25",
                },
            ],
            write_chronoxide: write_otlp_delta_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_exponential_histogram_bucket_projection",
            chronoxide_query: r#"last_over_time(otlp_size_bytes_bucket{route="/download",le="2"}[30s])"#,
            prom_query: r#"last_over_time(otlp_size_bytes_bucket{route="/download",le="2"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"otlp_size_bytes_bucket{route="/download",le="2"}"#,
                values: "2 4 6 8 10",
            }],
            write_chronoxide: write_otlp_exponential_histogram_series,
            projection_config: exphist_bucket_projection_config,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_delta_exponential_histogram_bucket_projection",
            chronoxide_query: r#"last_over_time(otlp_delta_size_bytes_bucket{route="/delta-download",le="2"}[30s])"#,
            prom_query: r#"last_over_time(otlp_delta_size_bytes_bucket{route="/delta-download",le="2"}[30s])"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"otlp_delta_size_bytes_bucket{route="/delta-download",le="2"}"#,
                values: "2 4 6 8 10",
            }],
            write_chronoxide: write_otlp_delta_exponential_histogram_series,
            projection_config: exphist_bucket_projection_config,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "otlp_delta_exponential_histogram_native_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, rate(otlp_delta_size_bytes{route="/delta-download"}[30s]))"#,
            prom_query: r#"histogram_quantile(0.5, rate(otlp_delta_size_bytes{route="/delta-download"}[30s]))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"otlp_delta_size_bytes{route="/delta-download"}"#,
                values: r#"{{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:10 count:10 buckets:[4 6] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:15 count:15 buckets:[6 9] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_otlp_delta_exponential_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, rate(native_exphist_seconds{route="/native"}[6s]))"#,
            prom_query: r#"histogram_quantile(0.5, rate(native_exphist_seconds{route="/native"}[6s]))"#,
            interval_secs: 1,
            eval_secs: 6,
            prom_input_series: &[PromInputSeries {
                series: r#"native_exphist_seconds{route="/native"}"#,
                values: r#"_ {{schema:0 sum:12 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} _ _ _ _ {{schema:0 sum:24 count:10 buckets:[4 6] offset:1 counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_exponential_histogram_quantile,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_quantile_sum_by",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (route)(rate(native_exphist_seconds{route="/native"}[6s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (route)(rate(native_exphist_seconds{route="/native"}[6s])))"#,
            interval_secs: 1,
            eval_secs: 6,
            prom_input_series: &[PromInputSeries {
                series: r#"native_exphist_seconds{route="/native"}"#,
                values: r#"_ {{schema:0 sum:12 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} _ _ _ _ {{schema:0 sum:24 count:10 buckets:[4 6] offset:1 counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_exponential_histogram_quantile,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_fraction",
            chronoxide_query: r#"histogram_fraction(1, 2, rate(native_exphist_seconds{route="/native"}[6s]))"#,
            prom_query: r#"histogram_fraction(1, 2, rate(native_exphist_seconds{route="/native"}[6s]))"#,
            interval_secs: 1,
            eval_secs: 6,
            prom_input_series: &[PromInputSeries {
                series: r#"native_exphist_seconds{route="/native"}"#,
                values: r#"_ {{schema:0 sum:12 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} _ _ _ _ {{schema:0 sum:24 count:10 buckets:[4 6] offset:1 counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_exponential_histogram_quantile,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_fraction_infinite_bounds",
            chronoxide_query: r#"histogram_fraction(-Inf, Inf, rate(native_exphist_seconds{route="/native"}[6s]))"#,
            prom_query: r#"histogram_fraction(-Inf, Inf, rate(native_exphist_seconds{route="/native"}[6s]))"#,
            interval_secs: 1,
            eval_secs: 6,
            prom_input_series: &[PromInputSeries {
                series: r#"native_exphist_seconds{route="/native"}"#,
                values: r#"_ {{schema:0 sum:12 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} _ _ _ _ {{schema:0 sum:24 count:10 buckets:[4 6] offset:1 counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_exponential_histogram_quantile,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_stale_latest_is_absent",
            chronoxide_query: r#"histogram_count(native_exphist_stale_seconds{route="/native-stale"})"#,
            prom_query: r#"histogram_count(native_exphist_stale_seconds{route="/native-stale"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"native_exphist_stale_seconds{route="/native-stale"}"#,
                values: r#"{{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:10 count:10 buckets:[4 6] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:15 count:15 buckets:[6 9] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} stale"#,
            }],
            write_chronoxide: write_native_exponential_histogram_stale_latest,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: false,
        },
        GoldenCase {
            name: "native_exponential_histogram_stale_vector_matching",
            chronoxide_query: r#"sum(histogram_count(native_exphist_stale_left_seconds{route="/native-stale-match"} + on(route, instance) native_exphist_stale_right_seconds{route="/native-stale-match"})) + sum(histogram_count(native_exphist_stale_left_seconds{route="/native-stale-match"} or on(route, instance) native_exphist_stale_right_seconds{route="/native-stale-match"})) + sum(histogram_count(native_exphist_stale_left_seconds{route="/native-stale-match"} unless on(route, instance) native_exphist_stale_right_seconds{route="/native-stale-match"}))"#,
            prom_query: r#"sum(histogram_count(native_exphist_stale_left_seconds{route="/native-stale-match"} + on(route, instance) native_exphist_stale_right_seconds{route="/native-stale-match"})) + sum(histogram_count(native_exphist_stale_left_seconds{route="/native-stale-match"} or on(route, instance) native_exphist_stale_right_seconds{route="/native-stale-match"})) + sum(histogram_count(native_exphist_stale_left_seconds{route="/native-stale-match"} unless on(route, instance) native_exphist_stale_right_seconds{route="/native-stale-match"}))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_exphist_stale_left_seconds{route="/native-stale-match",instance="matched"}"#,
                    values: r#"{{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_stale_left_seconds{route="/native-stale-match",instance="left-stale"}"#,
                    values: r#"{{schema:0 sum:3 count:3 buckets:[1 2] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:3 count:3 buckets:[1 2] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:3 count:3 buckets:[1 2] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:3 count:3 buckets:[1 2] offset:1 counter_reset_hint:not_reset}} stale"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_stale_left_seconds{route="/native-stale-match",instance="right-stale"}"#,
                    values: r#"{{schema:0 sum:11 count:11 buckets:[5 6] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[5 6] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[5 6] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[5 6] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[5 6] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_stale_left_seconds{route="/native-stale-match",instance="left-only"}"#,
                    values: r#"{{schema:0 sum:13 count:13 buckets:[6 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[6 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[6 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[6 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[6 7] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_stale_right_seconds{route="/native-stale-match",instance="matched"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_stale_right_seconds{route="/native-stale-match",instance="left-stale"}"#,
                    values: r#"{{schema:0 sum:17 count:17 buckets:[8 9] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:17 count:17 buckets:[8 9] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:17 count:17 buckets:[8 9] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:17 count:17 buckets:[8 9] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:17 count:17 buckets:[8 9] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_stale_right_seconds{route="/native-stale-match",instance="right-stale"}"#,
                    values: r#"{{schema:0 sum:19 count:19 buckets:[9 10] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:19 count:19 buckets:[9 10] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:19 count:19 buckets:[9 10] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:19 count:19 buckets:[9 10] offset:1 counter_reset_hint:not_reset}} stale"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_stale_right_seconds{route="/native-stale-match",instance="right-only"}"#,
                    values: r#"{{schema:0 sum:23 count:23 buckets:[11 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:23 count:23 buckets:[11 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:23 count:23 buckets:[11 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:23 count:23 buckets:[11 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:23 count:23 buckets:[11 12] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_exponential_histogram_stale_vector_matching,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_binary_vector_arithmetic_and_comparison",
            chronoxide_query: r#"histogram_count(native_exphist_left_seconds{route="/native"} + native_exphist_right_seconds{route="/native"}) + histogram_sum(native_exphist_left_seconds{route="/native"} - native_exphist_right_seconds{route="/native"}) + histogram_count(native_exphist_left_seconds{route="/native"} == native_exphist_left_seconds{route="/native"}) + histogram_count(native_exphist_left_seconds{route="/native"} != native_exphist_right_seconds{route="/native"})"#,
            prom_query: r#"histogram_count(native_exphist_left_seconds{route="/native"} + native_exphist_right_seconds{route="/native"}) + histogram_sum(native_exphist_left_seconds{route="/native"} - native_exphist_right_seconds{route="/native"}) + histogram_count(native_exphist_left_seconds{route="/native"} == native_exphist_left_seconds{route="/native"}) + histogram_count(native_exphist_left_seconds{route="/native"} != native_exphist_right_seconds{route="/native"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_exphist_left_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_right_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_exponential_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_binary_vector_invalid_shapes_drop",
            chronoxide_query: r#"histogram_count(native_exphist_left_seconds{route="/native"} * native_exphist_right_seconds{route="/native"}) or histogram_count(native_exphist_left_seconds{route="/native"} > native_exphist_right_seconds{route="/native"})"#,
            prom_query: r#"histogram_count(native_exphist_left_seconds{route="/native"} * native_exphist_right_seconds{route="/native"}) or histogram_count(native_exphist_left_seconds{route="/native"} > native_exphist_right_seconds{route="/native"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_exphist_left_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_right_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_exponential_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: false,
        },
        GoldenCase {
            name: "native_exponential_histogram_binary_bool_comparison",
            chronoxide_query: r#"(native_exphist_left_seconds{route="/native"} == bool native_exphist_left_seconds{route="/native"}) + (native_exphist_left_seconds{route="/native"} == bool native_exphist_right_seconds{route="/native"}) + (native_exphist_left_seconds{route="/native"} != bool native_exphist_right_seconds{route="/native"}) + (native_exphist_left_seconds{route="/native"} != bool native_exphist_left_seconds{route="/native"})"#,
            prom_query: r#"(native_exphist_left_seconds{route="/native"} == bool native_exphist_left_seconds{route="/native"}) + (native_exphist_left_seconds{route="/native"} == bool native_exphist_right_seconds{route="/native"}) + (native_exphist_left_seconds{route="/native"} != bool native_exphist_right_seconds{route="/native"}) + (native_exphist_left_seconds{route="/native"} != bool native_exphist_left_seconds{route="/native"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_exphist_left_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_right_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_exponential_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_binary_group_modifiers",
            chronoxide_query: r#"sum(histogram_count(native_exphist_group_many_seconds{route="/native-exphist-group"} + on(route,method) group_left(cluster) native_exphist_group_one_seconds{route="/native-exphist-group"})) + sum(histogram_count(native_exphist_group_one_left_seconds{route="/native-exphist-group"} + on(route,method) group_right(cluster) native_exphist_group_many_right_seconds{route="/native-exphist-group"}))"#,
            prom_query: r#"sum(histogram_count(native_exphist_group_many_seconds{route="/native-exphist-group"} + on(route,method) group_left(cluster) native_exphist_group_one_seconds{route="/native-exphist-group"})) + sum(histogram_count(native_exphist_group_one_left_seconds{route="/native-exphist-group"} + on(route,method) group_right(cluster) native_exphist_group_many_right_seconds{route="/native-exphist-group"}))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_exphist_group_many_seconds{route="/native-exphist-group",method="get",code="500"}"#,
                    values: r#"{{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_group_many_seconds{route="/native-exphist-group",method="get",code="404"}"#,
                    values: r#"{{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_group_one_seconds{route="/native-exphist-group",method="get",cluster="primary"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_group_one_left_seconds{route="/native-exphist-group",method="post",cluster="primary"}"#,
                    values: r#"{{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:5 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_group_many_right_seconds{route="/native-exphist-group",method="post",instance="a"}"#,
                    values: r#"{{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_group_many_right_seconds{route="/native-exphist-group",method="post",instance="b"}"#,
                    values: r#"{{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_exponential_histogram_group_modifier_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_exponential_histogram_set_operators",
            chronoxide_query: r#"sum(histogram_count(native_exphist_set_left_seconds and native_exphist_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds unless native_exphist_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds or native_exphist_set_right_seconds))"#,
            prom_query: r#"sum(histogram_count(native_exphist_set_left_seconds and native_exphist_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds unless native_exphist_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds or native_exphist_set_right_seconds))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_exphist_set_left_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_set_left_seconds{route="/native-set-left-only"}"#,
                    values: r#"{{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_set_right_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_set_right_seconds{route="/native-set-right-only"}"#,
                    values: r#"{{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_exponential_histogram_set_operator_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_classic_histogram_count_avg_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, rate(native_classic_seconds{route="/native"}[30s])) + histogram_avg(rate(native_classic_seconds{route="/native"}[30s])) + histogram_count(rate(native_classic_seconds{route="/native"}[30s]))"#,
            prom_query: r#"histogram_quantile(0.5, rate(native_classic_seconds{route="/native"}[30s])) + histogram_avg(rate(native_classic_seconds{route="/native"}[30s])) + histogram_count(rate(native_classic_seconds{route="/native"}[30s]))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"native_classic_seconds{route="/native"}"#,
                values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:10 count:10 custom_values:[1 2] buckets:[4 4 2] counter_reset_hint:not_reset}} {{schema:-53 sum:15 count:15 custom_values:[1 2] buckets:[6 6 3] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_classic_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_classic_histogram_fraction",
            chronoxide_query: r#"histogram_fraction(1, 2, rate(native_classic_seconds{route="/native"}[30s]))"#,
            prom_query: r#"histogram_fraction(1, 2, rate(native_classic_seconds{route="/native"}[30s]))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"native_classic_seconds{route="/native"}"#,
                values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:10 count:10 custom_values:[1 2] buckets:[4 4 2] counter_reset_hint:not_reset}} {{schema:-53 sum:15 count:15 custom_values:[1 2] buckets:[6 6 3] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_classic_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_histogram_binary_scalar_arithmetic",
            chronoxide_query: r#"histogram_count(native_classic_seconds{route="/native"} * 2) + histogram_sum(2 * native_classic_seconds{route="/native"}) + histogram_count(native_classic_seconds{route="/native"} / 2)"#,
            prom_query: r#"histogram_count(native_classic_seconds{route="/native"} * 2) + histogram_sum(2 * native_classic_seconds{route="/native"}) + histogram_count(native_classic_seconds{route="/native"} / 2)"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"native_classic_seconds{route="/native"}"#,
                values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:10 count:10 custom_values:[1 2] buckets:[4 4 2] counter_reset_hint:not_reset}} {{schema:-53 sum:15 count:15 custom_values:[1 2] buckets:[6 6 3] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_classic_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_histogram_binary_invalid_scalar_shapes_drop",
            chronoxide_query: r#"histogram_count(2 / native_classic_seconds{route="/native"}) or histogram_count(native_classic_seconds{route="/native"} + 2)"#,
            prom_query: r#"histogram_count(2 / native_classic_seconds{route="/native"}) or histogram_count(native_classic_seconds{route="/native"} + 2)"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[PromInputSeries {
                series: r#"native_classic_seconds{route="/native"}"#,
                values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:10 count:10 custom_values:[1 2] buckets:[4 4 2] counter_reset_hint:not_reset}} {{schema:-53 sum:15 count:15 custom_values:[1 2] buckets:[6 6 3] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_classic_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: false,
        },
        GoldenCase {
            name: "native_histogram_binary_vector_arithmetic_and_comparison",
            chronoxide_query: r#"histogram_count(native_left_seconds{route="/native"} + native_right_seconds{route="/native"}) + histogram_sum(native_left_seconds{route="/native"} - native_right_seconds{route="/native"}) + histogram_count(native_left_seconds{route="/native"} == native_left_seconds{route="/native"}) + histogram_count(native_left_seconds{route="/native"} != native_right_seconds{route="/native"})"#,
            prom_query: r#"histogram_count(native_left_seconds{route="/native"} + native_right_seconds{route="/native"}) + histogram_sum(native_left_seconds{route="/native"} - native_right_seconds{route="/native"}) + histogram_count(native_left_seconds{route="/native"} == native_left_seconds{route="/native"}) + histogram_count(native_left_seconds{route="/native"} != native_right_seconds{route="/native"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_left_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_right_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_histogram_binary_vector_invalid_shapes_drop",
            chronoxide_query: r#"histogram_count(native_left_seconds{route="/native"} * native_right_seconds{route="/native"}) or histogram_count(native_left_seconds{route="/native"} > native_right_seconds{route="/native"})"#,
            prom_query: r#"histogram_count(native_left_seconds{route="/native"} * native_right_seconds{route="/native"}) or histogram_count(native_left_seconds{route="/native"} > native_right_seconds{route="/native"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_left_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_right_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: false,
        },
        GoldenCase {
            name: "native_histogram_binary_bool_comparison",
            chronoxide_query: r#"(native_left_seconds{route="/native"} == bool native_left_seconds{route="/native"}) + (native_left_seconds{route="/native"} == bool native_right_seconds{route="/native"}) + (native_left_seconds{route="/native"} != bool native_right_seconds{route="/native"}) + (native_left_seconds{route="/native"} != bool native_left_seconds{route="/native"})"#,
            prom_query: r#"(native_left_seconds{route="/native"} == bool native_left_seconds{route="/native"}) + (native_left_seconds{route="/native"} == bool native_right_seconds{route="/native"}) + (native_left_seconds{route="/native"} != bool native_right_seconds{route="/native"}) + (native_left_seconds{route="/native"} != bool native_left_seconds{route="/native"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_left_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_right_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_histogram_ordering_bool_comparison_drop",
            chronoxide_query: r#"(native_left_seconds{route="/native"} > bool native_right_seconds{route="/native"}) or (native_exphist_left_seconds{route="/native"} < bool native_exphist_right_seconds{route="/native"})"#,
            prom_query: r#"(native_left_seconds{route="/native"} > bool native_right_seconds{route="/native"}) or (native_exphist_left_seconds{route="/native"} < bool native_exphist_right_seconds{route="/native"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_left_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_right_seconds{route="/native"}"#,
                    values: r#"{{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_left_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_right_seconds{route="/native"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_histogram_ordering_bool_drop_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: false,
        },
        GoldenCase {
            name: "native_histogram_binary_group_modifiers",
            chronoxide_query: r#"sum(histogram_count(native_group_many_seconds{route="/native-group"} + on(route,method) group_left(cluster) native_group_one_seconds{route="/native-group"})) + sum(histogram_count(native_group_one_left_seconds{route="/native-group"} + on(route,method) group_right(cluster) native_group_many_right_seconds{route="/native-group"}))"#,
            prom_query: r#"sum(histogram_count(native_group_many_seconds{route="/native-group"} + on(route,method) group_left(cluster) native_group_one_seconds{route="/native-group"})) + sum(histogram_count(native_group_one_left_seconds{route="/native-group"} + on(route,method) group_right(cluster) native_group_many_right_seconds{route="/native-group"}))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_group_many_seconds{route="/native-group",method="get",code="500"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_group_many_seconds{route="/native-group",method="get",code="404"}"#,
                    values: r#"{{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_group_one_seconds{route="/native-group",method="get",cluster="primary"}"#,
                    values: r#"{{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_group_one_left_seconds{route="/native-group",method="post",cluster="primary"}"#,
                    values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_group_many_right_seconds{route="/native-group",method="post",instance="a"}"#,
                    values: r#"{{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_group_many_right_seconds{route="/native-group",method="post",instance="b"}"#,
                    values: r#"{{schema:-53 sum:30 count:30 custom_values:[1 2] buckets:[12 12 6] counter_reset_hint:not_reset}} {{schema:-53 sum:30 count:30 custom_values:[1 2] buckets:[12 12 6] counter_reset_hint:not_reset}} {{schema:-53 sum:30 count:30 custom_values:[1 2] buckets:[12 12 6] counter_reset_hint:not_reset}} {{schema:-53 sum:30 count:30 custom_values:[1 2] buckets:[12 12 6] counter_reset_hint:not_reset}} {{schema:-53 sum:30 count:30 custom_values:[1 2] buckets:[12 12 6] counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_histogram_group_modifier_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_histogram_set_operators",
            chronoxide_query: r#"sum(histogram_count(native_set_left_seconds and native_set_right_seconds)) + sum(histogram_count(native_set_left_seconds unless native_set_right_seconds)) + sum(histogram_count(native_set_left_seconds or native_set_right_seconds))"#,
            prom_query: r#"sum(histogram_count(native_set_left_seconds and native_set_right_seconds)) + sum(histogram_count(native_set_left_seconds unless native_set_right_seconds)) + sum(histogram_count(native_set_left_seconds or native_set_right_seconds))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_set_left_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_set_left_seconds{route="/native-set-left-only"}"#,
                    values: r#"{{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_set_right_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_set_right_seconds{route="/native-set-right-only"}"#,
                    values: r#"{{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_histogram_set_operator_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "mixed_native_histogram_set_operators",
            chronoxide_query: r#"sum(histogram_count(native_set_left_seconds and native_exphist_set_right_seconds)) + sum(histogram_count(native_set_left_seconds unless native_exphist_set_right_seconds)) + sum(histogram_count(native_set_left_seconds or native_exphist_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds and native_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds unless native_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds or native_set_right_seconds))"#,
            prom_query: r#"sum(histogram_count(native_set_left_seconds and native_exphist_set_right_seconds)) + sum(histogram_count(native_set_left_seconds unless native_exphist_set_right_seconds)) + sum(histogram_count(native_set_left_seconds or native_exphist_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds and native_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds unless native_set_right_seconds)) + sum(histogram_count(native_exphist_set_left_seconds or native_set_right_seconds))"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_set_left_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_set_left_seconds{route="/native-set-left-only"}"#,
                    values: r#"{{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_set_right_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}} {{schema:-53 sum:7 count:7 custom_values:[1 2] buckets:[3 2 2] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_set_right_seconds{route="/native-set-right-only"}"#,
                    values: r#"{{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}} {{schema:-53 sum:13 count:13 custom_values:[1 2] buckets:[5 5 3] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_set_left_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:25 count:25 buckets:[10 15] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_set_left_seconds{route="/native-set-left-only"}"#,
                    values: r#"{{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:11 count:11 buckets:[4 7] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_set_right_seconds{route="/native-set-match"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_set_right_seconds{route="/native-set-right-only"}"#,
                    values: r#"{{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:13 count:13 buckets:[5 8] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_mixed_native_histogram_set_operator_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "mixed_native_histogram_binary_invalid_shapes_drop",
            chronoxide_query: r#"histogram_count(native_mixed_left_seconds{route="/native-mixed"} + native_exphist_mixed_right_seconds{route="/native-mixed"}) or histogram_count(native_exphist_mixed_right_seconds{route="/native-mixed"} + native_mixed_left_seconds{route="/native-mixed"}) or histogram_sum(native_mixed_left_seconds{route="/native-mixed"} - native_exphist_mixed_right_seconds{route="/native-mixed"}) or histogram_sum(native_exphist_mixed_right_seconds{route="/native-mixed"} - native_mixed_left_seconds{route="/native-mixed"}) or histogram_count(native_mixed_left_seconds{route="/native-mixed"} == native_exphist_mixed_right_seconds{route="/native-mixed"}) or histogram_count(native_exphist_mixed_right_seconds{route="/native-mixed"} == native_mixed_left_seconds{route="/native-mixed"}) or (native_mixed_left_seconds{route="/native-mixed"} > bool native_exphist_mixed_right_seconds{route="/native-mixed"}) or (native_exphist_mixed_right_seconds{route="/native-mixed"} > bool native_mixed_left_seconds{route="/native-mixed"})"#,
            prom_query: r#"histogram_count(native_mixed_left_seconds{route="/native-mixed"} + native_exphist_mixed_right_seconds{route="/native-mixed"}) or histogram_count(native_exphist_mixed_right_seconds{route="/native-mixed"} + native_mixed_left_seconds{route="/native-mixed"}) or histogram_sum(native_mixed_left_seconds{route="/native-mixed"} - native_exphist_mixed_right_seconds{route="/native-mixed"}) or histogram_sum(native_exphist_mixed_right_seconds{route="/native-mixed"} - native_mixed_left_seconds{route="/native-mixed"}) or histogram_count(native_mixed_left_seconds{route="/native-mixed"} == native_exphist_mixed_right_seconds{route="/native-mixed"}) or histogram_count(native_exphist_mixed_right_seconds{route="/native-mixed"} == native_mixed_left_seconds{route="/native-mixed"}) or (native_mixed_left_seconds{route="/native-mixed"} > bool native_exphist_mixed_right_seconds{route="/native-mixed"}) or (native_exphist_mixed_right_seconds{route="/native-mixed"} > bool native_mixed_left_seconds{route="/native-mixed"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_mixed_left_seconds{route="/native-mixed"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_right_seconds{route="/native-mixed"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_mixed_native_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: false,
        },
        GoldenCase {
            name: "mixed_native_histogram_binary_equality_comparison",
            chronoxide_query: r#"histogram_count(native_mixed_left_seconds{route="/native-mixed"} != native_exphist_mixed_right_seconds{route="/native-mixed"}) + histogram_count(native_exphist_mixed_right_seconds{route="/native-mixed"} != native_mixed_left_seconds{route="/native-mixed"}) + (native_mixed_left_seconds{route="/native-mixed"} == bool native_exphist_mixed_right_seconds{route="/native-mixed"}) + (native_mixed_left_seconds{route="/native-mixed"} != bool native_exphist_mixed_right_seconds{route="/native-mixed"}) + (native_exphist_mixed_right_seconds{route="/native-mixed"} == bool native_mixed_left_seconds{route="/native-mixed"}) + (native_exphist_mixed_right_seconds{route="/native-mixed"} != bool native_mixed_left_seconds{route="/native-mixed"})"#,
            prom_query: r#"histogram_count(native_mixed_left_seconds{route="/native-mixed"} != native_exphist_mixed_right_seconds{route="/native-mixed"}) + histogram_count(native_exphist_mixed_right_seconds{route="/native-mixed"} != native_mixed_left_seconds{route="/native-mixed"}) + (native_mixed_left_seconds{route="/native-mixed"} == bool native_exphist_mixed_right_seconds{route="/native-mixed"}) + (native_mixed_left_seconds{route="/native-mixed"} != bool native_exphist_mixed_right_seconds{route="/native-mixed"}) + (native_exphist_mixed_right_seconds{route="/native-mixed"} == bool native_mixed_left_seconds{route="/native-mixed"}) + (native_exphist_mixed_right_seconds{route="/native-mixed"} != bool native_mixed_left_seconds{route="/native-mixed"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_mixed_left_seconds{route="/native-mixed"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_right_seconds{route="/native-mixed"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_mixed_native_histogram_binary_vector_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "mixed_native_histogram_binary_comparison_vector_matching",
            chronoxide_query: r#"histogram_count(native_mixed_match_left_seconds{route="/native-mixed-match"} != on(route,method) native_exphist_mixed_match_right_seconds{route="/native-mixed-match"}) + histogram_count(native_exphist_mixed_match_right_seconds{route="/native-mixed-match"} != on(route,method) native_mixed_match_left_seconds{route="/native-mixed-match"}) + (native_mixed_match_left_seconds{route="/native-mixed-match"} == bool on(route,method) native_exphist_mixed_match_right_seconds{route="/native-mixed-match"}) + (native_mixed_match_left_seconds{route="/native-mixed-match"} != bool on(route,method) native_exphist_mixed_match_right_seconds{route="/native-mixed-match"}) + (native_exphist_mixed_match_right_seconds{route="/native-mixed-match"} == bool on(route,method) native_mixed_match_left_seconds{route="/native-mixed-match"}) + (native_exphist_mixed_match_right_seconds{route="/native-mixed-match"} != bool on(route,method) native_mixed_match_left_seconds{route="/native-mixed-match"})"#,
            prom_query: r#"histogram_count(native_mixed_match_left_seconds{route="/native-mixed-match"} != on(route,method) native_exphist_mixed_match_right_seconds{route="/native-mixed-match"}) + histogram_count(native_exphist_mixed_match_right_seconds{route="/native-mixed-match"} != on(route,method) native_mixed_match_left_seconds{route="/native-mixed-match"}) + (native_mixed_match_left_seconds{route="/native-mixed-match"} == bool on(route,method) native_exphist_mixed_match_right_seconds{route="/native-mixed-match"}) + (native_mixed_match_left_seconds{route="/native-mixed-match"} != bool on(route,method) native_exphist_mixed_match_right_seconds{route="/native-mixed-match"}) + (native_exphist_mixed_match_right_seconds{route="/native-mixed-match"} == bool on(route,method) native_mixed_match_left_seconds{route="/native-mixed-match"}) + (native_exphist_mixed_match_right_seconds{route="/native-mixed-match"} != bool on(route,method) native_mixed_match_left_seconds{route="/native-mixed-match"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_mixed_match_left_seconds{route="/native-mixed-match",method="get",side="custom"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_match_right_seconds{route="/native-mixed-match",method="get",side="exponential"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_mixed_native_histogram_comparison_vector_matching_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "mixed_native_histogram_binary_comparison_group_modifiers",
            chronoxide_query: r#"histogram_count(native_mixed_group_many_seconds{route="/native-mixed-group"} != on(route,method) group_left(cluster) native_exphist_mixed_group_one_seconds{route="/native-mixed-group"}) or histogram_count(native_mixed_group_one_seconds{route="/native-mixed-group"} != on(route,method) group_right(cluster) native_exphist_mixed_group_many_seconds{route="/native-mixed-group"})"#,
            prom_query: r#"histogram_count(native_mixed_group_many_seconds{route="/native-mixed-group"} != on(route,method) group_left(cluster) native_exphist_mixed_group_one_seconds{route="/native-mixed-group"}) or histogram_count(native_mixed_group_one_seconds{route="/native-mixed-group"} != on(route,method) group_right(cluster) native_exphist_mixed_group_many_seconds{route="/native-mixed-group"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_mixed_group_many_seconds{route="/native-mixed-group",method="get",code="500"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_mixed_group_many_seconds{route="/native-mixed-group",method="get",code="404"}"#,
                    values: r#"{{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_group_one_seconds{route="/native-mixed-group",method="get",cluster="primary"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_mixed_group_one_seconds{route="/native-mixed-group",method="post",cluster="primary"}"#,
                    values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_group_many_seconds{route="/native-mixed-group",method="post",instance="a"}"#,
                    values: r#"{{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_group_many_seconds{route="/native-mixed-group",method="post",instance="b"}"#,
                    values: r#"{{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_mixed_native_histogram_comparison_group_modifier_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "mixed_native_histogram_binary_comparison_group_modifiers_reverse",
            chronoxide_query: r#"histogram_count(native_exphist_mixed_group_many_seconds{route="/native-mixed-group"} != on(route,method) group_left(cluster) native_mixed_group_one_seconds{route="/native-mixed-group"}) or histogram_count(native_exphist_mixed_group_one_seconds{route="/native-mixed-group"} != on(route,method) group_right(cluster) native_mixed_group_many_seconds{route="/native-mixed-group"})"#,
            prom_query: r#"histogram_count(native_exphist_mixed_group_many_seconds{route="/native-mixed-group"} != on(route,method) group_left(cluster) native_mixed_group_one_seconds{route="/native-mixed-group"}) or histogram_count(native_exphist_mixed_group_one_seconds{route="/native-mixed-group"} != on(route,method) group_right(cluster) native_mixed_group_many_seconds{route="/native-mixed-group"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_mixed_group_many_seconds{route="/native-mixed-group",method="get",code="500"}"#,
                    values: r#"{{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_mixed_group_many_seconds{route="/native-mixed-group",method="get",code="404"}"#,
                    values: r#"{{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}} {{schema:-53 sum:11 count:11 custom_values:[1 2] buckets:[4 4 3] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_group_one_seconds{route="/native-mixed-group",method="get",cluster="primary"}"#,
                    values: r#"{{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:7 count:7 buckets:[3 4] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_mixed_group_one_seconds{route="/native-mixed-group",method="post",cluster="primary"}"#,
                    values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_group_many_seconds{route="/native-mixed-group",method="post",instance="a"}"#,
                    values: r#"{{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:20 count:20 buckets:[8 12] offset:1 counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_exphist_mixed_group_many_seconds{route="/native-mixed-group",method="post",instance="b"}"#,
                    values: r#"{{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}} {{schema:0 sum:30 count:30 buckets:[12 18] offset:1 counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_mixed_native_histogram_comparison_group_modifier_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_histogram_rate_coarsens_custom_bucket_layout_change",
            chronoxide_query: r#"histogram_quantile(0.5, rate(native_custom_layout_seconds{route="/native-layout-change"}[6s]))"#,
            prom_query: r#"histogram_quantile(0.5, rate(native_custom_layout_seconds{route="/native-layout-change"}[6s]))"#,
            interval_secs: 1,
            eval_secs: 6,
            prom_input_series: &[PromInputSeries {
                series: r#"native_custom_layout_seconds{route="/native-layout-change"}"#,
                values: r#"_ {{schema:-53 sum:20 count:10 custom_values:[1 2 4] buckets:[2 5 3 0] counter_reset_hint:not_reset}} _ _ _ _ {{schema:-53 sum:40 count:20 custom_values:[1 3 4] buckets:[4 10 6 0] counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_custom_layout_change_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "native_histogram_sum_coarsens_custom_bucket_layouts",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (route)(rate(native_custom_sum_seconds{route="/native-layout-sum"}[6s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (route)(rate(native_custom_sum_seconds{route="/native-layout-sum"}[6s])))"#,
            interval_secs: 1,
            eval_secs: 6,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"native_custom_sum_seconds{route="/native-layout-sum",instance="a"}"#,
                    values: r#"_ {{schema:-53 sum:20 count:10 custom_values:[1 2 4] buckets:[2 5 3 0] counter_reset_hint:not_reset}} _ _ _ _ {{schema:-53 sum:40 count:20 custom_values:[1 2 4] buckets:[4 10 6 0] counter_reset_hint:not_reset}}"#,
                },
                PromInputSeries {
                    series: r#"native_custom_sum_seconds{route="/native-layout-sum",instance="b"}"#,
                    values: r#"_ {{schema:-53 sum:20 count:10 custom_values:[1 3 4] buckets:[2 5 3 0] counter_reset_hint:not_reset}} _ _ _ _ {{schema:-53 sum:40 count:20 custom_values:[1 3 4] buckets:[4 10 6 0] counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_native_custom_layout_sum_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
        GoldenCase {
            name: "histogram_avg_ignores_float_only_input",
            chronoxide_query: r#"histogram_avg(cpu_usage{job="api"})"#,
            prom_query: r#"histogram_avg(cpu_usage{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="a"}"#,
                    values: "1 2 3 4 5",
                },
                PromInputSeries {
                    series: r#"cpu_usage{job="api",instance="b"}"#,
                    values: "2 3 4 5 6",
                },
            ],
            write_chronoxide: write_histogram_avg_float_only_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: false,
        },
        GoldenCase {
            name: "native_histogram_avg_ignores_mixed_float_series",
            chronoxide_query: r#"histogram_avg(mixed_histogram_seconds{job="api"})"#,
            prom_query: r#"histogram_avg(mixed_histogram_seconds{job="api"})"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"mixed_histogram_seconds{job="api",kind="float"}"#,
                    values: "7 7 7 7 7",
                },
                PromInputSeries {
                    series: r#"mixed_histogram_seconds{job="api",kind="hist"}"#,
                    values: r#"{{schema:-53 sum:5 count:5 custom_values:[1 2] buckets:[2 2 1] counter_reset_hint:not_reset}} {{schema:-53 sum:10 count:10 custom_values:[1 2] buckets:[4 4 2] counter_reset_hint:not_reset}} {{schema:-53 sum:15 count:15 custom_values:[1 2] buckets:[6 6 3] counter_reset_hint:not_reset}} {{schema:-53 sum:20 count:20 custom_values:[1 2] buckets:[8 8 4] counter_reset_hint:not_reset}} {{schema:-53 sum:25 count:25 custom_values:[1 2] buckets:[10 10 5] counter_reset_hint:not_reset}}"#,
                },
            ],
            write_chronoxide: write_mixed_float_and_native_histogram_series,
            projection_config: QueryProjectionConfig::default,
            expect_non_empty: true,
        },
    ]
}

fn golden_range_cases() -> Vec<GoldenRangeCase> {
    vec![
        GoldenRangeCase {
            name: "range_query_rate_sum_by",
            chronoxide_query: r#"sum by (route)(rate(http_requests_total{job="api"}[20s]))"#,
            prom_query: r#"sum by (route)(rate(http_requests_total{job="api"}[20s]))"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="a"}"#,
                    values: "0 10 20 30 40",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="b"}"#,
                    values: "0 5 10 15 20",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/search",instance="a"}"#,
                    values: "0 2 4 6 8",
                },
            ],
            write_chronoxide: write_float_counter_rate_sum_by,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_binary_scalar_rate_composition",
            chronoxide_query: r#"sum by (route)(rate(http_requests_total{job="api"}[20s])) / scalar(count(http_requests_total{job="api"}))"#,
            prom_query: r#"sum by (route)(rate(http_requests_total{job="api"}[20s])) / scalar(count(http_requests_total{job="api"}))"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="a"}"#,
                    values: "0 10 20 30 40",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="b"}"#,
                    values: "0 5 10 15 20",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/search",instance="a"}"#,
                    values: "0 2 4 6 8",
                },
            ],
            write_chronoxide: write_float_counter_rate_sum_by,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_nested_vector_binary_composition",
            chronoxide_query: r#"(sum by (route)(rate(http_errors_total{job="api"}[20s])) / sum by (route)(rate(http_requests_total{job="api"}[20s]))) * 100"#,
            prom_query: r#"(sum by (route)(rate(http_errors_total{job="api"}[20s])) / sum by (route)(rate(http_requests_total{job="api"}[20s]))) * 100"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="a"}"#,
                    values: "0 10 20 30 40",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/checkout",instance="b"}"#,
                    values: "0 5 10 15 20",
                },
                PromInputSeries {
                    series: r#"http_requests_total{job="api",route="/search",instance="a"}"#,
                    values: "0 2 4 6 8",
                },
                PromInputSeries {
                    series: r#"http_errors_total{job="api",route="/checkout",code="500"}"#,
                    values: "0 1 2 3 4",
                },
                PromInputSeries {
                    series: r#"http_errors_total{job="api",route="/checkout",code="404"}"#,
                    values: "0 2 4 6 8",
                },
                PromInputSeries {
                    series: r#"http_errors_total{job="api",route="/search",code="500"}"#,
                    values: "0 1 1 2 2",
                },
            ],
            write_chronoxide: write_range_error_request_counters,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_label_join",
            chronoxide_query: r#"label_join(cpu_usage{job="api",instance="a"}, "target", "/", "job", "instance")"#,
            prom_query: r#"label_join(cpu_usage{job="api",instance="a"}, "target", "/", "job", "instance")"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[PromInputSeries {
                series: r#"cpu_usage{job="api",instance="a"}"#,
                values: "1 2 3 4 5",
            }],
            write_chronoxide: write_label_replace_and_join,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_offset_selector",
            chronoxide_query: r#"gauge_value{series="a"} offset 20s"#,
            prom_query: r#"gauge_value{series="a"} offset 20s"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[PromInputSeries {
                series: r#"gauge_value{series="a"}"#,
                values: "1 2 4 8 16",
            }],
            write_chronoxide: write_gauge_range_series,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_offset_rate",
            chronoxide_query: r#"rate(http_requests_total{job="api",route="/checkout",instance="a"}[20s] offset 10s)"#,
            prom_query: r#"rate(http_requests_total{job="api",route="/checkout",instance="a"}[20s] offset 10s)"#,
            interval_secs: 10,
            start_secs: 30,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[PromInputSeries {
                series: r#"http_requests_total{job="api",route="/checkout",instance="a"}"#,
                values: "0 10 20 30 40",
            }],
            write_chronoxide: write_float_counter_rate_sum_by,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_stale_aggregation_step",
            chronoxide_query: r#"sum by (series)(stale_range_value{series="a"})"#,
            prom_query: r#"sum by (series)(stale_range_value{series="a"})"#,
            interval_secs: 10,
            start_secs: 0,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[PromInputSeries {
                series: r#"stale_range_value{series="a"}"#,
                values: "1 2 stale 8 16",
            }],
            write_chronoxide: write_stale_range_series,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_classic_histogram_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(classic_request_duration_seconds_bucket[20s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(classic_request_duration_seconds_bucket[20s])))"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"classic_request_duration_seconds_bucket{route="/checkout",le="1"}"#,
                    values: "2 4 6 8 10",
                },
                PromInputSeries {
                    series: r#"classic_request_duration_seconds_bucket{route="/checkout",le="2"}"#,
                    values: "4 8 12 16 20",
                },
                PromInputSeries {
                    series: r#"classic_request_duration_seconds_bucket{route="/checkout",le="+Inf"}"#,
                    values: "5 10 15 20 25",
                },
            ],
            write_chronoxide: write_classic_histogram_bucket_series,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_otlp_histogram_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(otlp_request_duration_seconds_bucket[20s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(otlp_request_duration_seconds_bucket[20s])))"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"otlp_request_duration_seconds_bucket{route="/checkout",le="1"}"#,
                    values: "2 4 6 8 10",
                },
                PromInputSeries {
                    series: r#"otlp_request_duration_seconds_bucket{route="/checkout",le="2"}"#,
                    values: "4 8 12 16 20",
                },
                PromInputSeries {
                    series: r#"otlp_request_duration_seconds_bucket{route="/checkout",le="+Inf"}"#,
                    values: "5 10 15 20 25",
                },
            ],
            write_chronoxide: write_otlp_histogram_series,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenRangeCase {
            name: "range_query_native_exponential_histogram_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (route)(rate(native_exphist_range_seconds{route="/native-range"}[6s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (route)(rate(native_exphist_range_seconds{route="/native-range"}[6s])))"#,
            interval_secs: 1,
            start_secs: 6,
            end_secs: 11,
            step_secs: 5,
            prom_input_series: &[PromInputSeries {
                series: r#"native_exphist_range_seconds{route="/native-range"}"#,
                values: r#"_ {{schema:0 sum:12 count:5 buckets:[2 3] offset:1 counter_reset_hint:not_reset}} _ _ _ _ {{schema:0 sum:24 count:10 buckets:[4 6] offset:1 counter_reset_hint:not_reset}} _ _ _ _ {{schema:0 sum:36 count:15 buckets:[6 9] offset:1 counter_reset_hint:not_reset}}"#,
            }],
            write_chronoxide: write_native_exponential_histogram_range_quantile,
            projection_config: QueryProjectionConfig::default,
        },
    ]
}

fn golden_head_range_cases() -> Vec<GoldenHeadRangeCase> {
    vec![
        GoldenHeadRangeCase {
            name: "range_query_with_head_rate_cross_segment",
            chronoxide_query: r#"rate(head_requests_total{job="api",instance="a"}[20s])"#,
            prom_query: r#"rate(head_requests_total{job="api",instance="a"}[20s])"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[PromInputSeries {
                series: r#"head_requests_total{job="api",instance="a"}"#,
                values: "0 10 20 30 40",
            }],
            write_chronoxide: write_head_counter_cross_segment,
            projection_config: QueryProjectionConfig::default,
        },
        GoldenHeadRangeCase {
            name: "range_query_with_head_otlp_histogram_quantile",
            chronoxide_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(head_request_duration_seconds_bucket[20s])))"#,
            prom_query: r#"histogram_quantile(0.5, sum by (le, route)(rate(head_request_duration_seconds_bucket[20s])))"#,
            interval_secs: 10,
            start_secs: 20,
            end_secs: 40,
            step_secs: 10,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"head_request_duration_seconds_bucket{route="/head-typed",le="1"}"#,
                    values: "2 4 6 8 10",
                },
                PromInputSeries {
                    series: r#"head_request_duration_seconds_bucket{route="/head-typed",le="2"}"#,
                    values: "4 8 12 16 20",
                },
                PromInputSeries {
                    series: r#"head_request_duration_seconds_bucket{route="/head-typed",le="+Inf"}"#,
                    values: "5 10 15 20 25",
                },
            ],
            write_chronoxide: write_head_histogram_cross_segment,
            projection_config: QueryProjectionConfig::default,
        },
    ]
}

fn golden_error_cases() -> Vec<GoldenErrorCase> {
    vec![
        GoldenErrorCase {
            name: "binary_one_to_one_duplicate_right_errors",
            chronoxide_query: r#"card_left / on(job) card_right"#,
            prom_query: r#"card_left / on(job) card_right"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"card_left{job="api",instance="a"}"#,
                    values: "1 1 1 1 1",
                },
                PromInputSeries {
                    series: r#"card_right{job="api",code="500"}"#,
                    values: "10 10 10 10 10",
                },
                PromInputSeries {
                    series: r#"card_right{job="api",code="404"}"#,
                    values: "20 20 20 20 20",
                },
            ],
            write_chronoxide: write_cardinality_duplicate_right_series,
            projection_config: QueryProjectionConfig::default,
            expected_chronoxide_error: "duplicate right-hand series for binary vector matching",
            expected_promtool_error: "many-to-many matching not allowed",
        },
        GoldenErrorCase {
            name: "binary_one_to_one_duplicate_left_errors",
            chronoxide_query: r#"card_left / on(job) card_right"#,
            prom_query: r#"card_left / on(job) card_right"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"card_left{job="api",instance="a"}"#,
                    values: "1 1 1 1 1",
                },
                PromInputSeries {
                    series: r#"card_left{job="api",instance="b"}"#,
                    values: "2 2 2 2 2",
                },
                PromInputSeries {
                    series: r#"card_right{job="api",code="500"}"#,
                    values: "10 10 10 10 10",
                },
            ],
            write_chronoxide: write_cardinality_duplicate_left_series,
            projection_config: QueryProjectionConfig::default,
            expected_chronoxide_error: "duplicate left-hand series for binary vector matching",
            expected_promtool_error: "many-to-one matching must be explicit",
        },
        GoldenErrorCase {
            name: "binary_group_left_duplicate_one_side_errors",
            chronoxide_query: r#"gl_errors / on(method) group_left gl_requests"#,
            prom_query: r#"gl_errors / on(method) group_left gl_requests"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"gl_errors{method="get",code="500"}"#,
                    values: "24 24 24 24 24",
                },
                PromInputSeries {
                    series: r#"gl_errors{method="get",code="404"}"#,
                    values: "30 30 30 30 30",
                },
                PromInputSeries {
                    series: r#"gl_requests{method="get",instance="a"}"#,
                    values: "600 600 600 600 600",
                },
                PromInputSeries {
                    series: r#"gl_requests{method="get",instance="b"}"#,
                    values: "700 700 700 700 700",
                },
            ],
            write_chronoxide: write_group_left_duplicate_one_side_series,
            projection_config: QueryProjectionConfig::default,
            expected_chronoxide_error: "duplicate right-hand series for group_left binary vector matching",
            expected_promtool_error: "many-to-many matching not allowed",
        },
        GoldenErrorCase {
            name: "binary_group_right_duplicate_one_side_errors",
            chronoxide_query: r#"gr_limit / on(route) group_right gr_usage"#,
            prom_query: r#"gr_limit / on(route) group_right gr_usage"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"gr_limit{route="/api",service="a"}"#,
                    values: "10 10 10 10 10",
                },
                PromInputSeries {
                    series: r#"gr_limit{route="/api",service="b"}"#,
                    values: "20 20 20 20 20",
                },
                PromInputSeries {
                    series: r#"gr_usage{route="/api",instance="a"}"#,
                    values: "2 2 2 2 2",
                },
                PromInputSeries {
                    series: r#"gr_usage{route="/api",instance="b"}"#,
                    values: "4 4 4 4 4",
                },
            ],
            write_chronoxide: write_group_right_duplicate_one_side_series,
            projection_config: QueryProjectionConfig::default,
            expected_chronoxide_error: "duplicate left-hand series for group_right binary vector matching",
            expected_promtool_error: "many-to-many matching not allowed",
        },
        GoldenErrorCase {
            name: "binary_group_left_duplicate_result_errors",
            chronoxide_query: r#"gl_result_left / on(method) group_left(service) gl_result_right"#,
            prom_query: r#"gl_result_left / on(method) group_left(service) gl_result_right"#,
            interval_secs: 10,
            eval_secs: 40,
            prom_input_series: &[
                PromInputSeries {
                    series: r#"gl_result_left{method="get",service="old-a"}"#,
                    values: "24 24 24 24 24",
                },
                PromInputSeries {
                    series: r#"gl_result_left{method="get",service="old-b"}"#,
                    values: "30 30 30 30 30",
                },
                PromInputSeries {
                    series: r#"gl_result_right{method="get",service="api"}"#,
                    values: "600 600 600 600 600",
                },
            ],
            write_chronoxide: write_group_left_duplicate_result_series,
            projection_config: QueryProjectionConfig::default,
            expected_chronoxide_error: "duplicate result series for group_left binary vector matching",
            expected_promtool_error: "grouping labels must ensure unique matches",
        },
    ]
}

fn assert_prometheus_golden_case(promtool: &Path, case: GoldenCase) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    (case.write_chronoxide)(&mut writer);
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        (case.projection_config)(),
    )
    .unwrap();
    let results = store
        .query_promql(case.chronoxide_query, 0, case.eval_secs * 1_000)
        .unwrap_or_else(|err| panic!("{}: Chronoxide query failed: {err}", case.name));

    if case.expect_non_empty {
        assert!(
            !results.is_empty(),
            "{}: Chronoxide query unexpectedly returned no samples",
            case.name
        );
    }

    let test_file = tempdir.path().join(format!("{}.promtool.yml", case.name));
    fs::write(&test_file, promtool_yaml(&case, &results)).unwrap();

    let output = Command::new(promtool)
        .args(["test", "rules"])
        .arg(&test_file)
        .output()
        .unwrap_or_else(|err| panic!("{}: failed to run promtool: {err}", case.name));

    if !output.status.success() {
        panic!(
            "{}: promtool rejected Chronoxide results\nstatus: {}\n{}\nstdout:\n{}\nstderr:\n{}",
            case.name,
            output.status,
            fs::read_to_string(&test_file).unwrap(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

fn assert_prometheus_golden_error_case(promtool: &Path, case: GoldenErrorCase) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    (case.write_chronoxide)(&mut writer);
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        (case.projection_config)(),
    )
    .unwrap();
    let chronoxide_error =
        match store.query_promql(case.chronoxide_query, 0, case.eval_secs * 1_000) {
            Ok(results) => panic!(
                "{}: Chronoxide query unexpectedly succeeded with {} results",
                case.name,
                results.len()
            ),
            Err(err) => err.to_string(),
        };
    assert!(
        chronoxide_error.contains(case.expected_chronoxide_error),
        "{}: Chronoxide error did not contain {:?}\nactual: {}",
        case.name,
        case.expected_chronoxide_error,
        chronoxide_error
    );

    let test_file = tempdir.path().join(format!("{}.promtool.yml", case.name));
    fs::write(&test_file, promtool_error_yaml(&case)).unwrap();

    let output = Command::new(promtool)
        .args(["test", "rules"])
        .arg(&test_file)
        .output()
        .unwrap_or_else(|err| panic!("{}: failed to run promtool: {err}", case.name));

    if output.status.success() {
        panic!(
            "{}: promtool unexpectedly accepted error case\n{}",
            case.name,
            fs::read_to_string(&test_file).unwrap(),
        );
    }
    let promtool_output = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        promtool_output.contains(case.expected_promtool_error),
        "{}: promtool error did not contain {:?}\nstatus: {}\n{}\noutput:\n{}",
        case.name,
        case.expected_promtool_error,
        output.status,
        fs::read_to_string(&test_file).unwrap(),
        promtool_output,
    );
}

fn assert_prometheus_golden_range_case(promtool: &Path, case: GoldenRangeCase) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    (case.write_chronoxide)(&mut writer);
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        (case.projection_config)(),
    )
    .unwrap();
    let results = store
        .query_promql_range(
            case.chronoxide_query,
            case.start_secs * 1_000,
            case.end_secs * 1_000,
            case.step_secs * 1_000,
        )
        .unwrap_or_else(|err| panic!("{}: Chronoxide range query failed: {err}", case.name));

    let test_file = tempdir.path().join(format!("{}.promtool.yml", case.name));
    fs::write(&test_file, promtool_range_yaml(&case, &results)).unwrap();

    let output = Command::new(promtool)
        .args(["test", "rules"])
        .arg(&test_file)
        .output()
        .unwrap_or_else(|err| panic!("{}: failed to run promtool: {err}", case.name));

    if !output.status.success() {
        panic!(
            "{}: promtool rejected Chronoxide range results\nstatus: {}\n{}\nstdout:\n{}\nstderr:\n{}",
            case.name,
            output.status,
            fs::read_to_string(&test_file).unwrap(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

fn assert_prometheus_golden_head_range_case(promtool: &Path, case: GoldenHeadRangeCase) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let mut head = golden_head();
    (case.write_chronoxide)(&mut writer, &mut label_store, &mut head);
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_query_projection_config(
        tempdir.path(),
        (case.projection_config)(),
    )
    .unwrap();
    let results = store
        .query_promql_range_with_head(
            &head,
            &label_store,
            case.chronoxide_query,
            case.start_secs * 1_000,
            case.end_secs * 1_000,
            case.step_secs * 1_000,
        )
        .unwrap_or_else(|err| panic!("{}: Chronoxide head range query failed: {err}", case.name));

    let test_file = tempdir.path().join(format!("{}.promtool.yml", case.name));
    fs::write(
        &test_file,
        promtool_range_yaml_from(
            case.name,
            case.interval_secs,
            case.start_secs,
            case.end_secs,
            case.step_secs,
            case.prom_query,
            case.prom_input_series,
            &results,
        ),
    )
    .unwrap();

    let output = Command::new(promtool)
        .args(["test", "rules"])
        .arg(&test_file)
        .output()
        .unwrap_or_else(|err| panic!("{}: failed to run promtool: {err}", case.name));

    if !output.status.success() {
        panic!(
            "{}: promtool rejected Chronoxide head range results\nstatus: {}\n{}\nstdout:\n{}\nstderr:\n{}",
            case.name,
            output.status,
            fs::read_to_string(&test_file).unwrap(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

fn promtool_yaml(case: &GoldenCase, results: &[SegmentQueryResult]) -> String {
    let mut yaml = String::new();
    yaml.push_str("rule_files: []\n");
    yaml.push_str(&format!("evaluation_interval: {}s\n", case.interval_secs));
    yaml.push_str("fuzzy_compare: true\n");
    yaml.push_str("tests:\n");
    yaml.push_str(&format!("- name: {}\n", yaml_single(case.name)));
    yaml.push_str(&format!("  interval: {}s\n", case.interval_secs));
    yaml.push_str("  input_series:\n");
    for series in case.prom_input_series {
        yaml.push_str(&format!("  - series: {}\n", yaml_single(series.series)));
        yaml.push_str(&format!("    values: {}\n", yaml_single(series.values)));
    }
    yaml.push_str("  promql_expr_test:\n");
    yaml.push_str(&format!("  - expr: {}\n", yaml_single(case.prom_query)));
    yaml.push_str(&format!("    eval_time: {}s\n", case.eval_secs));
    append_exp_samples_field(&mut yaml, instant_expected_samples(results), 4);
    yaml
}

fn promtool_error_yaml(case: &GoldenErrorCase) -> String {
    let mut yaml = String::new();
    yaml.push_str("rule_files: []\n");
    yaml.push_str(&format!("evaluation_interval: {}s\n", case.interval_secs));
    yaml.push_str("tests:\n");
    yaml.push_str(&format!("- name: {}\n", yaml_single(case.name)));
    yaml.push_str(&format!("  interval: {}s\n", case.interval_secs));
    yaml.push_str("  input_series:\n");
    for series in case.prom_input_series {
        yaml.push_str(&format!("  - series: {}\n", yaml_single(series.series)));
        yaml.push_str(&format!("    values: {}\n", yaml_single(series.values)));
    }
    yaml.push_str("  promql_expr_test:\n");
    yaml.push_str(&format!("  - expr: {}\n", yaml_single(case.prom_query)));
    yaml.push_str(&format!("    eval_time: {}s\n", case.eval_secs));
    yaml.push_str("    exp_samples: []\n");
    yaml
}

fn promtool_range_yaml(case: &GoldenRangeCase, results: &[SegmentQueryResult]) -> String {
    promtool_range_yaml_from(
        case.name,
        case.interval_secs,
        case.start_secs,
        case.end_secs,
        case.step_secs,
        case.prom_query,
        case.prom_input_series,
        results,
    )
}

fn promtool_range_yaml_from(
    name: &str,
    interval_secs: u64,
    start_secs: u64,
    end_secs: u64,
    step_secs: u64,
    prom_query: &str,
    prom_input_series: &[PromInputSeries],
    results: &[SegmentQueryResult],
) -> String {
    let mut yaml = String::new();
    yaml.push_str("rule_files: []\n");
    yaml.push_str(&format!("evaluation_interval: {interval_secs}s\n"));
    yaml.push_str("fuzzy_compare: true\n");
    yaml.push_str("tests:\n");
    yaml.push_str(&format!("- name: {}\n", yaml_single(name)));
    yaml.push_str(&format!("  interval: {interval_secs}s\n"));
    yaml.push_str("  input_series:\n");
    for series in prom_input_series {
        yaml.push_str(&format!("  - series: {}\n", yaml_single(series.series)));
        yaml.push_str(&format!("    values: {}\n", yaml_single(series.values)));
    }
    yaml.push_str("  promql_expr_test:\n");
    let mut eval_secs = start_secs;
    while eval_secs <= end_secs {
        yaml.push_str(&format!("  - expr: {}\n", yaml_single(prom_query)));
        yaml.push_str(&format!("    eval_time: {}s\n", eval_secs));
        append_exp_samples_field(
            &mut yaml,
            range_expected_samples(results, eval_secs * 1_000),
            4,
        );
        eval_secs = eval_secs
            .checked_add(step_secs)
            .expect("range step overflow");
    }
    yaml
}

fn instant_expected_samples(results: &[SegmentQueryResult]) -> Vec<(String, f64)> {
    let mut samples = results
        .iter()
        .map(|result| {
            assert_eq!(
                result.samples.len(),
                1,
                "golden queries must produce one instant sample per result: {:?}",
                result
            );
            (promtool_labels(result.labels.as_ref()), result.samples[0].1)
        })
        .collect::<Vec<_>>();
    samples.sort_by(|left, right| left.0.cmp(&right.0));
    samples
}

fn range_expected_samples(results: &[SegmentQueryResult], eval_ms: u64) -> Vec<(String, f64)> {
    let mut samples = Vec::new();
    for result in results {
        for (timestamp_ms, value) in &result.samples {
            if *timestamp_ms == eval_ms {
                samples.push((promtool_labels(result.labels.as_ref()), *value));
            }
        }
    }
    samples.sort_by(|left, right| left.0.cmp(&right.0));
    samples
}

fn append_exp_samples_field(yaml: &mut String, samples: Vec<(String, f64)>, indent: usize) {
    let prefix = " ".repeat(indent);
    if samples.is_empty() {
        yaml.push_str(&format!("{prefix}exp_samples: []\n"));
    } else {
        yaml.push_str(&format!("{prefix}exp_samples:\n"));
        append_expected_samples(yaml, samples, indent);
    }
}

fn append_expected_samples(yaml: &mut String, samples: Vec<(String, f64)>, indent: usize) {
    let prefix = " ".repeat(indent);
    if samples.is_empty() {
        yaml.push_str(&format!("{prefix}[]\n"));
    } else {
        for (labels, value) in samples {
            yaml.push_str(&format!("{prefix}- labels: {}\n", yaml_single(&labels)));
            yaml.push_str(&format!("{prefix}  value: {}\n", promtool_float(value)));
        }
    }
}

fn promtool_labels(labels: &[(String, String)]) -> String {
    let metric_name = labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()));
    let mut rest = labels
        .iter()
        .filter(|(key, _)| key != METRIC_NAME_LABEL)
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    rest.sort_unstable();

    let body = rest
        .into_iter()
        .map(|(key, value)| format!("{key}=\"{}\"", escape_prom_label_value(value)))
        .collect::<Vec<_>>()
        .join(",");

    match (metric_name, body.is_empty()) {
        (Some(name), true) => name.to_string(),
        (Some(name), false) => format!("{name}{{{body}}}"),
        (None, true) => "{}".to_string(),
        (None, false) => format!("{{{body}}}"),
    }
}

fn promtool_float(value: f64) -> String {
    if value.is_nan() {
        ".NaN".to_string()
    } else if value == f64::INFINITY {
        ".Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-.Inf".to_string()
    } else {
        value.to_string()
    }
}

fn yaml_single(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn escape_prom_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn find_promtool() -> PathBuf {
    if let Ok(path) = env::var("CHRONOXIDE_PROMTOOL") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return path;
        }
        panic!(
            "CHRONOXIDE_PROMTOOL does not point to a file: {}",
            path.display()
        );
    }

    find_on_path("promtool").unwrap_or_else(|| {
        panic!("promtool not found; set CHRONOXIDE_PROMTOOL or install promtool")
    })
}

fn find_on_path(binary: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|dir| dir.join(binary))
        .find(|candidate| candidate.is_file() && is_executable(candidate).unwrap_or(true))
}

#[cfg(unix)]
fn is_executable(path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    Ok(fs::metadata(path)?.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(_path: &Path) -> io::Result<bool> {
    Ok(true)
}

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

fn golden_head() -> HeadBuffer {
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

fn write_temperature_series(writer: &mut SegmentWriter) {
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

fn write_cpu_multi_series(writer: &mut SegmentWriter) {
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
        (0, delta_histogram_value(5, 5.0, [2, 2, 1])),
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
        (0, delta_exphist_value(5, 5.0, [2, 3])),
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

fn write_histogram_avg_float_only_series(writer: &mut SegmentWriter) {
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
