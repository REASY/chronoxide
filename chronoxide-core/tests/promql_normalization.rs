use chronoxide_core::promql::{
    METRIC_NAME_LABEL, canonicalize_labelset, normalize_label_name, normalize_metric_name,
    series_id,
};

#[test]
fn metric_names_are_promql_legal_and_changed_names_get_stable_suffixes() {
    assert_eq!(
        normalize_metric_name("http_requests_total"),
        "http_requests_total"
    );
    assert_eq!(
        normalize_metric_name(":go_gc_duration_seconds"),
        ":go_gc_duration_seconds"
    );

    let dotted = normalize_metric_name("service.latency-ms");
    assert!(dotted.starts_with("service_latency_ms_x"));
    assert!(is_promql_metric_name(&dotted));
    assert_eq!(dotted, normalize_metric_name("service.latency-ms"));

    let slash = normalize_metric_name("service/latency-ms");
    assert!(slash.starts_with("service_latency_ms_x"));
    assert_ne!(dotted, slash);

    let leading_digit = normalize_metric_name("9lives");
    assert!(leading_digit.starts_with("_9lives_x"));
    assert!(is_promql_metric_name(&leading_digit));

    let empty = normalize_metric_name("");
    assert!(empty.starts_with("__x"));
    assert!(is_promql_metric_name(&empty));
}

#[test]
fn label_names_are_promql_legal_reserved_safe_and_disambiguated() {
    assert_eq!(normalize_label_name("service_name"), "service_name");

    let dotted = normalize_label_name("k8s.cluster.name");
    assert!(dotted.starts_with("k8s_cluster_name_x"));
    assert!(is_promql_label_name(&dotted));

    let dashed = normalize_label_name("k8s-cluster-name");
    assert!(dashed.starts_with("k8s_cluster_name_x"));
    assert_ne!(dotted, dashed);

    let leading_digit = normalize_label_name("9bad");
    assert!(leading_digit.starts_with("_9bad_x"));
    assert!(is_promql_label_name(&leading_digit));

    let reserved = normalize_label_name("__tenant");
    assert!(reserved.starts_with("otel___tenant_x"));
    assert!(is_promql_label_name(&reserved));

    let metric_label = normalize_label_name(METRIC_NAME_LABEL);
    assert!(metric_label.starts_with("otel___name___x"));
    assert!(is_promql_label_name(&metric_label));
}

#[test]
fn canonical_labelsets_sort_dedupe_and_include_metric_name() {
    let canonical = canonicalize_labelset(
        "service.latency-ms",
        &[
            ("pod.name", "backend-1"),
            ("namespace", "default"),
            ("pod-name", "backend-2"),
        ],
    );

    assert_eq!(canonical.labels()[0].name, METRIC_NAME_LABEL);
    assert!(
        canonical.labels()[0]
            .value
            .starts_with("service_latency_ms_x")
    );
    assert_eq!(canonical.labels()[1].name, "namespace");
    assert!(canonical.labels()[2].name.starts_with("pod_name_x"));
    assert!(canonical.labels()[3].name.starts_with("pod_name_x"));
    assert_ne!(canonical.labels()[2].name, canonical.labels()[3].name);
}

#[test]
fn series_id_is_stable_and_uses_canonical_label_order() {
    let a = canonicalize_labelset(
        "cpu.usage",
        &[("pod.name", "backend-1"), ("namespace", "default")],
    );
    let b = canonicalize_labelset(
        "cpu.usage",
        &[("namespace", "default"), ("pod.name", "backend-1")],
    );
    let c = canonicalize_labelset(
        "cpu.usage",
        &[("namespace", "default"), ("pod.name", "backend-2")],
    );

    assert_eq!(a, b);
    assert_eq!(series_id(&a), series_id(&b));
    assert_ne!(series_id(&a), series_id(&c));
}

fn is_promql_metric_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_' || first == ':') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == ':')
}

fn is_promql_label_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}
