use super::fixtures::{
    golden_cases, golden_error_cases, golden_head, golden_head_range_cases, golden_range_cases,
    write_cpu_multi_series, write_temperature_series,
};
use super::*;

pub(super) fn assert_prometheus_golden_cases() {
    let promtool = find_promtool();
    assert_prometheus_exact_counter_float_order(&promtool);
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

pub(super) fn assert_prometheus_exact_counter_float_order(promtool: &Path) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[(
                METRIC_NAME_LABEL.to_owned(),
                "prometheus_float_order_total".to_owned(),
            )],
            &[(2_000, 3.0), (7_000, 6.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let queries = [
        "increase(prometheus_float_order_total[7s])",
        "rate(prometheus_float_order_total[7s])",
    ];
    let results = queries.map(|query| {
        query_chronoxide_golden_instant(&store, query, 7_000, "exact counter float order")
    });

    let mut yaml = String::from(
        "rule_files: []\nevaluation_interval: 1s\nfuzzy_compare: false\ntests:\n- name: exact counter float operation order\n  interval: 1s\n  input_series:\n  - series: prometheus_float_order_total\n    values: '_ _ 3 _ _ _ _ 6'\n  promql_expr_test:\n",
    );
    for (query, results) in queries.into_iter().zip(&results) {
        yaml.push_str(&format!("  - expr: {}\n", yaml_single(query)));
        yaml.push_str("    eval_time: 7s\n");
        append_exp_samples_field(&mut yaml, instant_expected_samples(results), 4);
    }
    let test_file = tempdir
        .path()
        .join("exact-counter-float-order.promtool.yml");
    fs::write(&test_file, &yaml).unwrap();
    let output = Command::new(promtool)
        .args(["test", "rules"])
        .arg(&test_file)
        .output()
        .unwrap_or_else(|err| panic!("failed to run exact-order promtool case: {err}"));
    if !output.status.success() {
        panic!(
            "promtool rejected exact counter float operation order\nstatus: {}\n{}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            yaml,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

pub(super) fn assert_sort_order_matches_prometheus_http_api() {
    let query = r#"sort(cpu_usage{job="api"})"#;
    let desc_query = r#"sort_desc(cpu_usage{job="api"})"#;

    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_cpu_multi_series(&mut writer);
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let chronoxide_sort = query_chronoxide_golden_instant(&store, query, 40_000, "sort order");
    let chronoxide_sort_desc =
        query_chronoxide_golden_instant(&store, desc_query, 40_000, "sort-desc order");

    let prometheus = start_prometheus_sort_fixture(&tempdir);
    let prometheus_sort = prometheus_query_instances(prometheus.port, query, 40);
    let prometheus_sort_desc = prometheus_query_instances(prometheus.port, desc_query, 40);

    assert_eq!(
        result_label_values(&chronoxide_sort, "instance"),
        prometheus_sort,
        "sort() result order must match Prometheus HTTP API order"
    );
    assert_eq!(
        result_label_values(&chronoxide_sort_desc, "instance"),
        prometheus_sort_desc,
        "sort_desc() result order must match Prometheus HTTP API order"
    );
}

pub(super) fn assert_double_exponential_smoothing_matches_prometheus_http_api() {
    let query =
        r#"double_exponential_smoothing(temperature_celsius{sensor="rack-a"}[30s], 0.5, 0.5)"#;

    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_temperature_series(&mut writer);
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let chronoxide =
        query_chronoxide_golden_instant(&store, query, 40_000, "double exponential smoothing");

    let prometheus = start_prometheus_openmetrics_fixture(
        &tempdir,
        "double_exponential_smoothing",
        concat!(
            "# TYPE temperature_celsius gauge\n",
            "temperature_celsius{sensor=\"rack-a\"} 10 0\n",
            "temperature_celsius{sensor=\"rack-a\"} 12 10\n",
            "temperature_celsius{sensor=\"rack-a\"} 14 20\n",
            "temperature_celsius{sensor=\"rack-a\"} 16 30\n",
            "temperature_celsius{sensor=\"rack-a\"} 18 40\n",
            "# EOF\n",
        ),
        &["--enable-feature=promql-experimental-functions"],
    );
    let prometheus_value = prometheus_query_single_value(prometheus.port, query, 40);
    let chronoxide_value = single_result_value(&chronoxide);

    assert!(
        (chronoxide_value - prometheus_value).abs() < 1e-12,
        "double_exponential_smoothing result must match Prometheus HTTP API: Chronoxide={chronoxide_value}, Prometheus={prometheus_value}"
    );
}

struct PrometheusProcess {
    child: Child,
    port: u16,
}

impl Drop for PrometheusProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_prometheus_sort_fixture(tempdir: &tempfile::TempDir) -> PrometheusProcess {
    start_prometheus_openmetrics_fixture(
        tempdir,
        "sort_order",
        concat!(
            "# TYPE cpu_usage gauge\n",
            "cpu_usage{job=\"api\",instance=\"a\"} 5 40\n",
            "cpu_usage{job=\"api\",instance=\"b\"} 6 40\n",
            "cpu_usage{job=\"api\",instance=\"c\"} 7 40\n",
            "# EOF\n",
        ),
        &[],
    )
}

fn start_prometheus_openmetrics_fixture(
    tempdir: &tempfile::TempDir,
    name: &str,
    openmetrics: &str,
    prometheus_args: &[&str],
) -> PrometheusProcess {
    let promtool = find_promtool();
    let prometheus = find_prometheus(&promtool);
    let openmetrics_path = tempdir.path().join(format!("{name}.openmetrics"));
    let data_dir = tempdir.path().join(format!("{name}-prometheus-data"));
    let config_path = tempdir.path().join(format!("{name}-prometheus.yml"));

    fs::write(&openmetrics_path, openmetrics).unwrap();
    fs::write(
        &config_path,
        "global:\n  scrape_interval: 1h\nscrape_configs: []\n",
    )
    .unwrap();

    let output = Command::new(&promtool)
        .args(["tsdb", "create-blocks-from", "openmetrics"])
        .arg(&openmetrics_path)
        .arg(&data_dir)
        .output()
        .unwrap_or_else(|err| panic!("{name}: failed to create Prometheus fixture block: {err}"));
    if !output.status.success() {
        panic!(
            "{name}: failed to create Prometheus fixture block\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let port = reserve_local_port();
    let child = Command::new(&prometheus)
        .arg(format!("--config.file={}", config_path.display()))
        .arg(format!("--storage.tsdb.path={}", data_dir.display()))
        .arg(format!("--web.listen-address=127.0.0.1:{port}"))
        .arg("--log.level=error")
        .args(prometheus_args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|err| panic!("failed to start Prometheus: {err}"));

    let process = PrometheusProcess { child, port };
    wait_for_prometheus_ready(process.port);
    process
}

fn reserve_local_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_prometheus_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if http_get_local(port, "/-/ready").is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("Prometheus did not become ready on 127.0.0.1:{port}");
}

fn prometheus_query_instances(port: u16, query: &str, time_secs: u64) -> Vec<String> {
    prometheus_query_vector(port, query, time_secs)
        .iter()
        .map(|sample| {
            sample["metric"]["instance"]
                .as_str()
                .expect("Prometheus sample must contain instance label")
                .to_string()
        })
        .collect()
}

fn prometheus_query_single_value(port: u16, query: &str, time_secs: u64) -> f64 {
    let results = prometheus_query_vector(port, query, time_secs);
    assert_eq!(
        results.len(),
        1,
        "Prometheus query must return exactly one sample"
    );
    results[0]["value"][1]
        .as_str()
        .expect("Prometheus sample value must be a string")
        .parse::<f64>()
        .expect("Prometheus sample value must parse as f64")
}

fn prometheus_query_vector(port: u16, query: &str, time_secs: u64) -> Vec<serde_json::Value> {
    let path = format!(
        "/api/v1/query?query={}&time={}",
        url_query_component(query),
        time_secs
    );
    let body = http_get_local(port, &path).unwrap_or_else(|err| {
        panic!("failed to query Prometheus sort oracle at {path}: {err}");
    });
    let value: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("Prometheus returned invalid JSON: {err}\n{body}"));
    assert_eq!(
        value.get("status").and_then(|status| status.as_str()),
        Some("success"),
        "Prometheus query failed: {body}"
    );
    value["data"]["result"]
        .as_array()
        .expect("Prometheus vector result must be an array")
        .clone()
}

fn single_result_value(results: &[SegmentQueryResult]) -> f64 {
    assert_eq!(results.len(), 1, "Chronoxide query must return one result");
    assert_eq!(
        results[0].samples.len(),
        1,
        "Chronoxide result must contain one sample"
    );
    results[0]
        .labels
        .iter()
        .find(|(key, value)| *key == "sensor" && *value == "rack-a")
        .expect("Chronoxide sample must keep sensor label");
    results[0].samples[0].1
}

fn http_get_local(port: u16, path: &str) -> io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "HTTP response missing header separator",
        )
    })?;
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected HTTP response status: {head}"),
        ));
    }
    Ok(body.to_string())
}

fn url_query_component(input: &str) -> String {
    let mut encoded = String::new();
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn result_label_values(results: &[SegmentQueryResult], label_name: &str) -> Vec<String> {
    results
        .iter()
        .map(|result| {
            result
                .labels
                .iter()
                .find(|(key, _)| *key == label_name)
                .map(|(_, value)| value.to_string())
                .unwrap_or_else(|| panic!("result missing label {label_name}: {result:?}"))
        })
        .collect()
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
    let results = query_chronoxide_golden_instant(
        &store,
        case.chronoxide_query,
        case.eval_secs * 1_000,
        case.name,
    );

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
    let mut session = store
        .query_session()
        .unwrap_or_else(|err| panic!("{}: Chronoxide session failed: {err}", case.name));
    let chronoxide_error =
        match session.query_promql_at(case.chronoxide_query, case.eval_secs * 1_000) {
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

pub(super) fn query_chronoxide_golden_instant(
    store: &SegmentStoreReader,
    query: &str,
    evaluation_ms: u64,
    case_name: &str,
) -> Vec<SegmentQueryResult> {
    let mut session = store
        .query_session()
        .unwrap_or_else(|err| panic!("{case_name}: Chronoxide session failed: {err}"));
    session
        .query_promql_at(query, evaluation_ms)
        .unwrap_or_else(|err| panic!("{case_name}: Chronoxide instant query failed: {err}"))
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

#[expect(
    clippy::too_many_arguments,
    reason = "the Prometheus oracle helper keeps every test-case field and Chronoxide result explicit"
)]
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
            (
                promtool_labels(result.labels.to_vec().as_slice()),
                result.samples[0].1,
            )
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
                samples.push((promtool_labels(result.labels.to_vec().as_slice()), *value));
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

pub(super) fn find_promtool() -> PathBuf {
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

fn find_prometheus(promtool: &Path) -> PathBuf {
    if let Ok(path) = env::var("CHRONOXIDE_PROMETHEUS") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return path;
        }
        panic!(
            "CHRONOXIDE_PROMETHEUS does not point to a file: {}",
            path.display()
        );
    }

    if let Some(parent) = promtool.parent() {
        let sibling = parent.join("prometheus");
        if sibling.is_file() {
            return sibling;
        }
    }

    find_on_path("prometheus").unwrap_or_else(|| {
        panic!("prometheus not found; set CHRONOXIDE_PROMETHEUS or install prometheus")
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
