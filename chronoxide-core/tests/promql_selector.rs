use chronoxide_core::promql::{
    PromqlMatcher, PromqlMatcherOp, PromqlQueryError, PromqlSelector, parse_vector_selector,
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
fn parse_binary_expression_returns_unsupported() {
    let err = parse_vector_selector("cpu_usage + memory_usage").unwrap_err();

    assert_eq!(
        err,
        PromqlQueryError::Unsupported("PromQL expressions are not implemented".to_string())
    );
}

#[test]
fn parse_invalid_selector_returns_invalid() {
    let err = parse_vector_selector("cpu_usage{pod=}").unwrap_err();

    assert!(matches!(err, PromqlQueryError::Invalid(_)));
}
