use chronoxide_core::promql::{
    METRIC_NAME_LABEL, PromqlAbsent, PromqlAbsentOverTime, PromqlAggregation,
    PromqlAggregationGrouping, PromqlAggregationOp, PromqlBinaryOp, PromqlHistogramFraction,
    PromqlHistogramQuantile, PromqlHistogramScalarFunction, PromqlHistogramScalarFunctionKind,
    PromqlInstantFunction, PromqlInstantFunctionKind, PromqlLabelJoin, PromqlLabelReplace,
    PromqlMatcher, PromqlMatcherOp, PromqlQuery, PromqlQueryError, PromqlRangeFunction,
    PromqlRangeFunctionKind, PromqlSelector, PromqlVectorMatching, PromqlVectorMatchingCardinality,
    PromqlVectorMatchingMode, normalize_label_name, parse_query, parse_vector_selector,
};

#[test]
fn parse_metric_shorthand_selector() {
    let selector = parse_vector_selector("cpu_usage").unwrap();

    assert_eq!(
        selector,
        PromqlSelector {
            metric_name: Some("cpu_usage".to_string()),
            matchers: Vec::new(),
        }
    );
}

#[test]
fn parse_metric_selector_with_equality_and_inequality_matchers() {
    let selector =
        parse_vector_selector(r#"cpu_usage{pod="backend-1",namespace!="kube-system"}"#).unwrap();

    assert_eq!(selector.metric_name.as_deref(), Some("cpu_usage"));
    assert_eq!(
        selector.matchers,
        vec![
            PromqlMatcher {
                name: "pod".to_string(),
                op: PromqlMatcherOp::Eq,
                value: "backend-1".to_string(),
            },
            PromqlMatcher {
                name: "namespace".to_string(),
                op: PromqlMatcherOp::NotEq,
                value: "kube-system".to_string(),
            },
        ]
    );
}

#[test]
fn parse_otlp_style_dotted_metric_and_label_names() {
    let selector = parse_vector_selector(r#"cpu.usage{pod.name="backend-1"}"#).unwrap();

    assert_eq!(selector.metric_name.as_deref(), Some("cpu.usage"));
    assert_eq!(
        selector.matchers,
        vec![PromqlMatcher {
            name: "pod.name".to_string(),
            op: PromqlMatcherOp::Eq,
            value: "backend-1".to_string(),
        }]
    );
}

#[test]
fn parse_brace_only_selector_with_metric_name_matcher() {
    let selector = parse_vector_selector(r#"{__name__="cpu_usage",pod!="backend-2"}"#).unwrap();

    assert_eq!(
        selector,
        PromqlSelector {
            metric_name: Some("cpu_usage".to_string()),
            matchers: vec![PromqlMatcher {
                name: "pod".to_string(),
                op: PromqlMatcherOp::NotEq,
                value: "backend-2".to_string(),
            }],
        }
    );
}

#[test]
fn parse_quoted_matcher_values_with_escapes() {
    let selector = parse_vector_selector(r#"http_requests{route="\/api\n"}"#).unwrap();

    assert_eq!(selector.metric_name.as_deref(), Some("http_requests"));
    assert_eq!(
        selector.matchers,
        vec![PromqlMatcher {
            name: "route".to_string(),
            op: PromqlMatcherOp::Eq,
            value: "/api\n".to_string(),
        }]
    );
}

#[test]
fn parse_quoted_matcher_values_can_contain_expression_punctuation() {
    let selector = parse_vector_selector(r#"http_requests{route="/api[0](test)"}"#).unwrap();

    assert_eq!(
        selector.matchers,
        vec![PromqlMatcher {
            name: "route".to_string(),
            op: PromqlMatcherOp::Eq,
            value: "/api[0](test)".to_string(),
        }]
    );
}

#[test]
fn parse_selector_allows_promql_whitespace() {
    let selector = parse_vector_selector(r#" cpu_usage { pod = "backend-1" } "#).unwrap();

    assert_eq!(selector.metric_name.as_deref(), Some("cpu_usage"));
    assert_eq!(
        selector.matchers,
        vec![PromqlMatcher {
            name: "pod".to_string(),
            op: PromqlMatcherOp::Eq,
            value: "backend-1".to_string(),
        }]
    );
}

#[test]
fn parse_regex_matcher() {
    let selector = parse_vector_selector(r#"cpu_usage{pod=~"backend-.*"}"#).unwrap();

    assert_eq!(
        selector.matchers,
        vec![PromqlMatcher {
            name: "pod".to_string(),
            op: PromqlMatcherOp::Regex,
            value: "backend-.*".to_string(),
        }]
    );
}

#[test]
fn parse_negative_regex_matcher() {
    let selector = parse_vector_selector(r#"cpu_usage{pod!~"backend-.*"}"#).unwrap();

    assert_eq!(
        selector.matchers,
        vec![PromqlMatcher {
            name: "pod".to_string(),
            op: PromqlMatcherOp::NotRegex,
            value: "backend-.*".to_string(),
        }]
    );
}

#[test]
fn parse_metric_name_regex_matcher() {
    let selector = parse_vector_selector(r#"{__name__=~"http_.*"}"#).unwrap();

    assert_eq!(
        selector.matchers,
        vec![PromqlMatcher {
            name: "__name__".to_string(),
            op: PromqlMatcherOp::Regex,
            value: "http_.*".to_string(),
        }]
    );
}

#[test]
fn parse_function_expression_returns_unsupported() {
    let err = parse_vector_selector("rate(cpu_usage[5m])").unwrap_err();

    assert_eq!(
        err,
        PromqlQueryError::Unsupported("PromQL expressions are not implemented".to_string())
    );
}

#[test]
fn parse_offset_queries() {
    let query = parse_query("rate(cpu_usage[5m] offset 1m)").unwrap();
    let PromqlQuery::Offset(offset) = query else {
        panic!("expected offset query");
    };
    assert_eq!(offset.offset_ms, 60_000);
    assert!(matches!(*offset.input, PromqlQuery::RangeFunction(_)));

    let query = parse_query("cpu_usage offset -30s").unwrap();
    let PromqlQuery::Offset(offset) = query else {
        panic!("expected negative offset query");
    };
    assert_eq!(offset.offset_ms, -30_000);
    assert!(matches!(*offset.input, PromqlQuery::Vector(_)));
}

#[test]
fn parse_scalar_and_instant_function_queries() {
    assert!(matches!(parse_query("time()").unwrap(), PromqlQuery::Time));

    let PromqlQuery::VectorFunction(vector) = parse_query("vector(time())").unwrap() else {
        panic!("expected vector() query");
    };
    assert!(matches!(*vector.input, PromqlQuery::Time));

    let cases = [
        ("abs(cpu_usage)", PromqlInstantFunctionKind::Abs),
        ("ceil(cpu_usage)", PromqlInstantFunctionKind::Ceil),
        ("floor(cpu_usage)", PromqlInstantFunctionKind::Floor),
        (
            "round(cpu_usage, 0.5)",
            PromqlInstantFunctionKind::Round { to_nearest: 0.5 },
        ),
        (
            "clamp(cpu_usage, 0, 10)",
            PromqlInstantFunctionKind::Clamp {
                min: Some(0.0),
                max: Some(10.0),
            },
        ),
        ("ln(cpu_usage)", PromqlInstantFunctionKind::Ln),
        ("log2(cpu_usage)", PromqlInstantFunctionKind::Log2),
        ("log10(cpu_usage)", PromqlInstantFunctionKind::Log10),
        ("minute(cpu_usage)", PromqlInstantFunctionKind::Minute),
        ("hour(cpu_usage)", PromqlInstantFunctionKind::Hour),
        (
            "day_of_week(cpu_usage)",
            PromqlInstantFunctionKind::DayOfWeek,
        ),
        (
            "days_in_month(cpu_usage)",
            PromqlInstantFunctionKind::DaysInMonth,
        ),
    ];

    for (source, kind) in cases {
        let query = parse_query(source).unwrap();
        assert_eq!(
            query,
            PromqlQuery::InstantFunction(PromqlInstantFunction {
                kind,
                input: Box::new(PromqlQuery::Vector(PromqlSelector {
                    metric_name: Some("cpu_usage".to_string()),
                    matchers: Vec::new(),
                })),
            })
        );
    }
}

#[test]
fn parse_label_replace_and_label_join_queries() {
    let replace = parse_query(
        r#"label_replace(http_requests_total{job="api"}, "service", "$1", "job", "(.+)")"#,
    )
    .unwrap();
    assert_eq!(
        replace,
        PromqlQuery::LabelReplace(PromqlLabelReplace {
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "job".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "api".to_string(),
                }],
            })),
            dst_label: "service".to_string(),
            replacement: "$1".to_string(),
            src_label: "job".to_string(),
            regex: "(.+)".to_string(),
        })
    );

    let join = parse_query(r#"label_join(up, "target", "/", "job", "instance")"#).unwrap();
    assert_eq!(
        join,
        PromqlQuery::LabelJoin(PromqlLabelJoin {
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("up".to_string()),
                matchers: Vec::new(),
            })),
            dst_label: "target".to_string(),
            separator: "/".to_string(),
            src_labels: vec!["job".to_string(), "instance".to_string()],
        })
    );

    let dotted = parse_query(r#"label_join(up, "target.name", "/", "pod.name")"#).unwrap();
    assert_eq!(
        dotted,
        PromqlQuery::LabelJoin(PromqlLabelJoin {
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("up".to_string()),
                matchers: Vec::new(),
            })),
            dst_label: normalize_label_name("target.name"),
            separator: "/".to_string(),
            src_labels: vec![normalize_label_name("pod.name")],
        })
    );
}

#[test]
fn parse_rate_range_function_query() {
    let query = parse_query(r#" rate(http_requests_total{route="/api"}[5m]) "#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Rate,
            selector: PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "route".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "/api".to_string(),
                }],
            },
            range_ms: 300_000,
        })
    );
}

#[test]
fn parse_increase_range_function_query() {
    let query = parse_query("increase(http_requests_total[1h30m])").unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Increase,
            selector: PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: Vec::new(),
            },
            range_ms: 5_400_000,
        })
    );
}

#[test]
fn parse_delta_range_function_query() {
    let query = parse_query(r#"delta(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Delta,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_irate_range_function_query() {
    let query = parse_query(r#"irate(http_requests_total{route="/api"}[2m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Irate,
            selector: PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "route".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "/api".to_string(),
                }],
            },
            range_ms: 120_000,
        })
    );
}

#[test]
fn parse_idelta_range_function_query() {
    let query = parse_query(r#"idelta(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Idelta,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_changes_range_function_query() {
    let query = parse_query(r#"changes(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Changes,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_resets_range_function_query() {
    let query = parse_query(r#"resets(http_requests_total{route="/api"}[2m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::Resets,
            selector: PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "route".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "/api".to_string(),
                }],
            },
            range_ms: 120_000,
        })
    );
}

#[test]
fn parse_last_over_time_range_function_query() {
    let query =
        parse_query(r#"last_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::LastOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_count_over_time_range_function_query() {
    let query =
        parse_query(r#"count_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::CountOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_present_over_time_range_function_query() {
    let query =
        parse_query(r#"present_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::PresentOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_sum_over_time_range_function_query() {
    let query =
        parse_query(r#"sum_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::SumOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_avg_over_time_range_function_query() {
    let query =
        parse_query(r#"avg_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::AvgOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_stddev_over_time_range_function_query() {
    let query =
        parse_query(r#"stddev_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::StddevOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_stdvar_over_time_range_function_query() {
    let query =
        parse_query(r#"stdvar_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::StdvarOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_min_over_time_range_function_query() {
    let query =
        parse_query(r#"min_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::MinOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_max_over_time_range_function_query() {
    let query =
        parse_query(r#"max_over_time(cpu_temperature_celsius{sensor="rack-a"}[10m])"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind: PromqlRangeFunctionKind::MaxOverTime,
            selector: PromqlSelector {
                metric_name: Some("cpu_temperature_celsius".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "sensor".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "rack-a".to_string(),
                }],
            },
            range_ms: 600_000,
        })
    );
}

#[test]
fn parse_absent_instant_vector_query() {
    let query = parse_query(r#"absent(http_requests_total{job="api",instance=~".*"})"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Absent(PromqlAbsent {
            labels: vec![("job".to_string(), "api".to_string())],
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: vec![
                    PromqlMatcher {
                        name: "job".to_string(),
                        op: PromqlMatcherOp::Eq,
                        value: "api".to_string(),
                    },
                    PromqlMatcher {
                        name: "instance".to_string(),
                        op: PromqlMatcherOp::Regex,
                        value: ".*".to_string(),
                    },
                ],
            })),
        })
    );
}

#[test]
fn parse_absent_over_time_range_query() {
    let query =
        parse_query(r#"absent_over_time(http_requests_total{job="api",instance=~".*"}[5m])"#)
            .unwrap();

    assert_eq!(
        query,
        PromqlQuery::AbsentOverTime(PromqlAbsentOverTime {
            labels: vec![("job".to_string(), "api".to_string())],
            selector: PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: vec![
                    PromqlMatcher {
                        name: "job".to_string(),
                        op: PromqlMatcherOp::Eq,
                        value: "api".to_string(),
                    },
                    PromqlMatcher {
                        name: "instance".to_string(),
                        op: PromqlMatcherOp::Regex,
                        value: ".*".to_string(),
                    },
                ],
            },
            range_ms: 300_000,
        })
    );
}

#[test]
fn parse_absent_result_labels_normalize_otlp_style_dotted_labels() {
    let instant = parse_query(r#"absent(cpu.usage{pod.name="backend-1",instance=~".*"})"#).unwrap();
    assert_eq!(
        instant,
        PromqlQuery::Absent(PromqlAbsent {
            labels: vec![(normalize_label_name("pod.name"), "backend-1".to_string())],
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu.usage".to_string()),
                matchers: vec![
                    PromqlMatcher {
                        name: "pod.name".to_string(),
                        op: PromqlMatcherOp::Eq,
                        value: "backend-1".to_string(),
                    },
                    PromqlMatcher {
                        name: "instance".to_string(),
                        op: PromqlMatcherOp::Regex,
                        value: ".*".to_string(),
                    },
                ],
            })),
        })
    );

    let over_time =
        parse_query(r#"absent_over_time(cpu.usage{pod.name="backend-1",instance=~".*"}[5m])"#)
            .unwrap();
    assert_eq!(
        over_time,
        PromqlQuery::AbsentOverTime(PromqlAbsentOverTime {
            labels: vec![(normalize_label_name("pod.name"), "backend-1".to_string())],
            selector: PromqlSelector {
                metric_name: Some("cpu.usage".to_string()),
                matchers: vec![
                    PromqlMatcher {
                        name: "pod.name".to_string(),
                        op: PromqlMatcherOp::Eq,
                        value: "backend-1".to_string(),
                    },
                    PromqlMatcher {
                        name: "instance".to_string(),
                        op: PromqlMatcherOp::Regex,
                        value: ".*".to_string(),
                    },
                ],
            },
            range_ms: 300_000,
        })
    );
}

#[test]
fn parse_histogram_quantile_over_rate_query() {
    let query = parse_query(
        r#"histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{route="/api"}[5m]))"#,
    )
    .unwrap();

    assert_eq!(
        query,
        PromqlQuery::HistogramQuantile(PromqlHistogramQuantile {
            quantile: 0.95,
            input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Rate,
                selector: PromqlSelector {
                    metric_name: Some("http_request_duration_seconds_bucket".to_string()),
                    matchers: vec![PromqlMatcher {
                        name: "route".to_string(),
                        op: PromqlMatcherOp::Eq,
                        value: "/api".to_string(),
                    }],
                },
                range_ms: 300_000,
            })),
        })
    );
}

#[test]
fn parse_histogram_quantile_accepts_scalar_expression_argument() {
    let query = parse_query(
        r#"histogram_quantile(0.25 + 0.25, rate(http_request_duration_seconds_bucket[5m]))"#,
    )
    .unwrap();

    assert_eq!(
        query,
        PromqlQuery::HistogramQuantile(PromqlHistogramQuantile {
            quantile: 0.5,
            input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Rate,
                selector: PromqlSelector {
                    metric_name: Some("http_request_duration_seconds_bucket".to_string()),
                    matchers: Vec::new(),
                },
                range_ms: 5 * 60 * 1_000,
            })),
        })
    );
}

#[test]
fn parse_sum_by_over_rate_query() {
    let query =
        parse_query(r#"sum by (le, route)(rate(http_request_duration_bucket[5m]))"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Sum,
            grouping: PromqlAggregationGrouping::By(vec!["le".to_string(), "route".to_string(),]),
            input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Rate,
                selector: PromqlSelector {
                    metric_name: Some("http_request_duration_bucket".to_string()),
                    matchers: Vec::new(),
                },
                range_ms: 300_000,
            })),
        })
    );
}

#[test]
fn parse_sum_without_query() {
    let query = parse_query(r#"sum without (instance)(cpu_usage{job="api"})"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Sum,
            grouping: PromqlAggregationGrouping::Without(vec!["instance".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "job".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "api".to_string(),
                }],
            })),
        })
    );
}

#[test]
fn parse_aggregation_grouping_normalizes_otlp_style_dotted_labels() {
    let query = parse_query(r#"sum by (pod.name)(cpu.usage)"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Sum,
            grouping: PromqlAggregationGrouping::By(vec![normalize_label_name("pod.name")]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu.usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_count_by_query() {
    let query = parse_query(r#"count by (route)(http_requests_total)"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Count,
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_avg_without_grouping_query() {
    let query = parse_query(r#"avg(cpu_usage)"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Avg,
            grouping: PromqlAggregationGrouping::All,
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_min_by_query() {
    let query = parse_query(r#"min by (route)(cpu_usage)"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Min,
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_max_without_query() {
    let query = parse_query(r#"max without (instance)(cpu_usage)"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Max,
            grouping: PromqlAggregationGrouping::Without(vec!["instance".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_stddev_stdvar_and_group_aggregation_queries() {
    let stddev = parse_query(r#"stddev by (route)(cpu_usage)"#).unwrap();
    assert_eq!(
        stddev,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Stddev,
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );

    let stdvar = parse_query(r#"stdvar(cpu_usage)"#).unwrap();
    assert_eq!(
        stdvar,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Stdvar,
            grouping: PromqlAggregationGrouping::All,
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );

    let group = parse_query(r#"group without (instance)(cpu_usage)"#).unwrap();
    assert_eq!(
        group,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Group,
            grouping: PromqlAggregationGrouping::Without(vec!["instance".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_topk_and_bottomk_aggregation_queries() {
    let topk = parse_query(r#"topk(2, cpu_usage{job="api"})"#).unwrap();
    assert_eq!(
        topk,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::TopK(2),
            grouping: PromqlAggregationGrouping::All,
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "job".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "api".to_string(),
                }],
            })),
        })
    );

    let bottomk = parse_query(r#"bottomk by (route)(1, cpu_usage)"#).unwrap();
    assert_eq!(
        bottomk,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::BottomK(1),
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_rank_and_quantile_aggregation_accept_scalar_expression_parameters() {
    let topk = parse_query(r#"topk(1 + 1, cpu_usage)"#).unwrap();
    assert_eq!(
        topk,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::TopK(2),
            grouping: PromqlAggregationGrouping::All,
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );

    let quantile = parse_query(r#"quantile by (route)(1 / 4, cpu_usage)"#).unwrap();
    assert_eq!(
        quantile,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Quantile(0.25),
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_quantile_aggregation_query() {
    let query = parse_query(r#"quantile by (route)(0.75, cpu_usage)"#).unwrap();
    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Quantile(0.75),
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_count_values_aggregation_query() {
    let query = parse_query(r#"count_values by (route)("value", cpu_usage)"#).unwrap();
    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::CountValues("value".to_string()),
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_histogram_quantile_over_sum_by_rate_query() {
    let query = parse_query(
        r#"histogram_quantile(0.95, sum by (le, route)(rate(http_request_duration_bucket[5m])))"#,
    )
    .unwrap();

    assert!(matches!(query, PromqlQuery::HistogramQuantile(_)));
    let PromqlQuery::HistogramQuantile(function) = query else {
        unreachable!("matched above");
    };
    assert!(matches!(*function.input, PromqlQuery::Aggregation(_)));
}

#[test]
fn parse_native_histogram_scalar_function_queries() {
    let count = parse_query(r#"histogram_count(rate(http_request_duration[5m]))"#).unwrap();
    assert_eq!(
        count,
        PromqlQuery::HistogramScalarFunction(PromqlHistogramScalarFunction {
            kind: PromqlHistogramScalarFunctionKind::Count,
            input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Rate,
                selector: PromqlSelector {
                    metric_name: Some("http_request_duration".to_string()),
                    matchers: Vec::new(),
                },
                range_ms: 5 * 60 * 1_000,
            })),
        })
    );

    let sum = parse_query(r#"histogram_sum(http_request_duration)"#).unwrap();
    assert_eq!(
        sum,
        PromqlQuery::HistogramScalarFunction(PromqlHistogramScalarFunction {
            kind: PromqlHistogramScalarFunctionKind::Sum,
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("http_request_duration".to_string()),
                matchers: Vec::new(),
            })),
        })
    );

    let avg =
        parse_query(r#"histogram_avg(sum by (route)(rate(http_request_duration[5m])))"#).unwrap();
    assert_eq!(
        avg,
        PromqlQuery::HistogramScalarFunction(PromqlHistogramScalarFunction {
            kind: PromqlHistogramScalarFunctionKind::Avg,
            input: Box::new(PromqlQuery::Aggregation(PromqlAggregation {
                op: PromqlAggregationOp::Sum,
                grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
                input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                    kind: PromqlRangeFunctionKind::Rate,
                    selector: PromqlSelector {
                        metric_name: Some("http_request_duration".to_string()),
                        matchers: Vec::new(),
                    },
                    range_ms: 5 * 60 * 1_000,
                })),
            })),
        })
    );
}

#[test]
fn parse_native_histogram_fraction_query() {
    let fraction =
        parse_query(r#"histogram_fraction(1, 3, sum by (route)(rate(http_request_duration[5m])))"#)
            .unwrap();
    assert_eq!(
        fraction,
        PromqlQuery::HistogramFraction(PromqlHistogramFraction {
            lower: 1.0,
            upper: 3.0,
            input: Box::new(PromqlQuery::Aggregation(PromqlAggregation {
                op: PromqlAggregationOp::Sum,
                grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
                input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                    kind: PromqlRangeFunctionKind::Rate,
                    selector: PromqlSelector {
                        metric_name: Some("http_request_duration".to_string()),
                        matchers: Vec::new(),
                    },
                    range_ms: 5 * 60 * 1_000,
                })),
            })),
        })
    );
}

#[test]
fn parse_native_histogram_fraction_allows_infinite_bounds() {
    let fraction =
        parse_query(r#"histogram_fraction(-Inf, Inf, rate(http_request_duration[5m]))"#).unwrap();

    assert_eq!(
        fraction,
        PromqlQuery::HistogramFraction(PromqlHistogramFraction {
            lower: f64::NEG_INFINITY,
            upper: f64::INFINITY,
            input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Rate,
                selector: PromqlSelector {
                    metric_name: Some("http_request_duration".to_string()),
                    matchers: Vec::new(),
                },
                range_ms: 5 * 60 * 1_000,
            })),
        })
    );
}

#[test]
fn parse_native_histogram_fraction_accepts_scalar_expression_bounds() {
    let fraction =
        parse_query(r#"histogram_fraction(1 / 2, 2 + 1, rate(http_request_duration[5m]))"#)
            .unwrap();

    assert_eq!(
        fraction,
        PromqlQuery::HistogramFraction(PromqlHistogramFraction {
            lower: 0.5,
            upper: 3.0,
            input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Rate,
                selector: PromqlSelector {
                    metric_name: Some("http_request_duration".to_string()),
                    matchers: Vec::new(),
                },
                range_ms: 5 * 60 * 1_000,
            })),
        })
    );
}

#[test]
fn parse_sort_instant_function_queries() {
    parse_query("sort(cpu_usage)").unwrap();
    parse_query("sort_desc(sum by (route)(cpu_usage))").unwrap();
}

#[test]
fn parse_binary_expression_returns_unsupported() {
    let err = parse_vector_selector("cpu_usage + memory_usage").unwrap_err();

    assert_eq!(
        err,
        PromqlQueryError::Unsupported("PromQL expressions are not implemented".to_string())
    );
}

#[test]
fn parse_vector_scalar_binary_expression_query() {
    parse_query(r#"cpu_usage{route="/api"} * 100"#).unwrap();
    parse_query(r#"100 - cpu_usage{route="/api"}"#).unwrap();
}

#[test]
fn parse_modulo_and_power_binary_expression_queries() {
    let modulo = parse_query(r#"cpu_usage{route="/api"} % 4"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = modulo else {
        panic!("expected binary expression, got {modulo:?}");
    };
    assert_eq!(expression.op, PromqlBinaryOp::Mod);

    let power = parse_query(r#"cpu_usage{route="/api"} ^ 2"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = power else {
        panic!("expected binary expression, got {power:?}");
    };
    assert_eq!(expression.op, PromqlBinaryOp::Pow);
}

#[test]
fn parse_vector_comparison_binary_expression_query() {
    parse_query(r#"cpu_usage{route="/api"} > 0.5"#).unwrap();
    parse_query(r#"cpu_usage{route="/api"} <= cpu_limit{route="/api"}"#).unwrap();
}

#[test]
fn parse_bool_comparison_binary_expression_query() {
    parse_query(r#"cpu_usage{route="/api"} > bool 0.5"#).unwrap();
    parse_query(r#"cpu_usage{route="/api"} <= bool cpu_limit{route="/api"}"#).unwrap();
    parse_query(r#"1 > bool 0"#).unwrap();
}

#[test]
fn parse_scalar_scalar_comparison_requires_bool_modifier() {
    let err = parse_query("1 > 0").unwrap_err();

    assert_eq!(
        err,
        PromqlQueryError::Invalid("comparisons between scalars must use BOOL modifier".to_string())
    );
}

#[test]
fn parse_vector_set_binary_expression_query() {
    parse_query(r#"cpu_usage{route="/api"} and cpu_usage{route="/api",instance=~"a|c"}"#).unwrap();
    parse_query(
        r#"cpu_usage{route="/api",instance=~"a|b"} or cpu_usage{route="/api",instance=~"b|c"}"#,
    )
    .unwrap();
    parse_query(r#"cpu_usage{route="/api"} unless cpu_usage{route="/api",instance="b"}"#).unwrap();
    let set_on = parse_query(r#"cpu_usage and on(route) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = set_on else {
        panic!("expected binary expression, got {set_on:?}");
    };
    assert_eq!(expression.op, PromqlBinaryOp::And);
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: vec!["route".to_string()],
            cardinality: PromqlVectorMatchingCardinality::ManyToMany,
            include_labels: Vec::new(),
        })
    );

    let set_ignoring = parse_query(r#"cpu_usage unless ignoring(instance) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = set_ignoring else {
        panic!("expected binary expression, got {set_ignoring:?}");
    };
    assert_eq!(expression.op, PromqlBinaryOp::Unless);
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::Ignoring,
            labels: vec!["instance".to_string()],
            cardinality: PromqlVectorMatchingCardinality::ManyToMany,
            include_labels: Vec::new(),
        })
    );
}

#[test]
fn parse_binary_vector_matching_modifier_queries() {
    let ignoring = parse_query(r#"cpu_usage / ignoring(instance) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = ignoring else {
        panic!("expected binary expression, got {ignoring:?}");
    };
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::Ignoring,
            labels: vec!["instance".to_string()],
            cardinality: PromqlVectorMatchingCardinality::OneToOne,
            include_labels: Vec::new(),
        })
    );

    let on = parse_query(r#"cpu_usage / on(route) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = on else {
        panic!("expected binary expression, got {on:?}");
    };
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: vec!["route".to_string()],
            cardinality: PromqlVectorMatchingCardinality::OneToOne,
            include_labels: Vec::new(),
        })
    );

    let on_metric_name = parse_query(r#"cpu_usage / on(__name__, route) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = on_metric_name else {
        panic!("expected binary expression, got {on_metric_name:?}");
    };
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: vec![METRIC_NAME_LABEL.to_string(), "route".to_string()],
            cardinality: PromqlVectorMatchingCardinality::OneToOne,
            include_labels: Vec::new(),
        })
    );

    let bool_on = parse_query(r#"cpu_usage > bool on(route) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = bool_on else {
        panic!("expected binary expression, got {bool_on:?}");
    };
    assert!(expression.return_bool);
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: vec!["route".to_string()],
            cardinality: PromqlVectorMatchingCardinality::OneToOne,
            include_labels: Vec::new(),
        })
    );

    let group_left = parse_query(r#"cpu_usage / ignoring(instance) group_left cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = group_left else {
        panic!("expected binary expression, got {group_left:?}");
    };
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::Ignoring,
            labels: vec!["instance".to_string()],
            cardinality: PromqlVectorMatchingCardinality::ManyToOne,
            include_labels: Vec::new(),
        })
    );

    let group_left_include =
        parse_query(r#"cpu_usage / on(route) group_left(service.name) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = group_left_include else {
        panic!("expected binary expression, got {group_left_include:?}");
    };
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: vec!["route".to_string()],
            cardinality: PromqlVectorMatchingCardinality::ManyToOne,
            include_labels: vec![normalize_label_name("service.name")],
        })
    );

    let group_right =
        parse_query(r#"cpu_usage > bool on(route) group_right(namespace) cpu_limit"#).unwrap();
    let PromqlQuery::BinaryExpression(expression) = group_right else {
        panic!("expected binary expression, got {group_right:?}");
    };
    assert!(expression.return_bool);
    assert_eq!(
        expression.vector_matching,
        Some(PromqlVectorMatching {
            mode: PromqlVectorMatchingMode::On,
            labels: vec!["route".to_string()],
            cardinality: PromqlVectorMatchingCardinality::OneToMany,
            include_labels: vec!["namespace".to_string()],
        })
    );
}

#[test]
fn parse_unary_minus_vector_expression_query() {
    parse_query(r#"-cpu_usage{route="/api"}"#).unwrap();
}

#[test]
fn parse_invalid_selector_returns_invalid() {
    let err = parse_vector_selector("cpu_usage{pod=}").unwrap_err();

    assert!(matches!(err, PromqlQueryError::Invalid(_)));
}
