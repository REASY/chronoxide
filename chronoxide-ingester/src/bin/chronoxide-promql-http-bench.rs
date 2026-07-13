use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use chronoxide_core::storage::segment::{
    PortableQuerySeries, portable_query_result_fingerprint_sha256,
};
use clap::{Parser, ValueEnum};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;
use serde_json::Value;

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Parser)]
#[command(about = "Benchmark a Prometheus-compatible PromQL HTTP endpoint")]
struct Args {
    #[arg(long)]
    name: String,
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    query: String,
    #[arg(long, value_enum, default_value_t = QueryMode::Instant)]
    mode: QueryMode,
    #[arg(long)]
    time_ms: Option<u64>,
    #[arg(long)]
    start_ms: Option<u64>,
    #[arg(long)]
    end_ms: Option<u64>,
    #[arg(long)]
    step_ms: Option<u64>,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, default_value_t = 9)]
    repeats: usize,
    #[arg(long, default_value_t = 120)]
    request_timeout_secs: u64,
    #[arg(long = "header", value_name = "NAME=VALUE")]
    headers: Vec<HeaderArg>,
    #[arg(long = "label-rename", value_name = "SOURCE=CANONICAL")]
    label_renames: Vec<LabelRename>,
    #[arg(long = "drop-label")]
    drop_labels: Vec<String>,
    #[arg(long)]
    expected_fingerprint: Option<String>,
    #[arg(long)]
    expected_series: Option<u64>,
    #[arg(long)]
    expected_samples: Option<u64>,
    #[arg(long)]
    report: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum QueryMode {
    Instant,
    Range,
}

#[derive(Debug, Clone)]
struct HeaderArg {
    name: HeaderName,
    value: HeaderValue,
}

impl FromStr for HeaderArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (name, value) = value
            .split_once('=')
            .ok_or_else(|| "header must use NAME=VALUE syntax".to_string())?;
        Ok(Self {
            name: HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| format!("invalid header name: {error}"))?,
            value: HeaderValue::from_str(value)
                .map_err(|error| format!("invalid header value: {error}"))?,
        })
    }
}

#[derive(Debug, Clone)]
struct LabelRename {
    source: String,
    canonical: String,
}

impl FromStr for LabelRename {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (source, canonical) = value
            .split_once('=')
            .ok_or_else(|| "label rename must use SOURCE=CANONICAL syntax".to_string())?;
        if source.is_empty() || canonical.is_empty() {
            return Err("label rename names must not be empty".to_string());
        }
        Ok(Self {
            source: source.to_string(),
            canonical: canonical.to_string(),
        })
    }
}

#[derive(Debug, Default)]
struct LabelTransform {
    renames: HashMap<String, String>,
    drops: HashSet<String>,
}

#[derive(Debug, Serialize)]
struct HttpQueryRun {
    run_index: usize,
    duration_ns: u64,
    server_query_duration_ns: Option<u64>,
    server_timing: Option<String>,
    fingerprint_duration_ns: u64,
    response_bytes: u64,
    result_series: u64,
    result_samples: u64,
    portable_semantic_fingerprint_sha256: String,
}

#[derive(Debug, Serialize)]
struct HttpQueryReport {
    schema: &'static str,
    name: String,
    endpoint: String,
    header_names: Vec<String>,
    query: String,
    mode: QueryMode,
    time_ms: Option<u64>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    step_ms: Option<u64>,
    warmups: usize,
    repeats: usize,
    label_renames: BTreeMap<String, String>,
    drop_labels: Vec<String>,
    expected_fingerprint: Option<String>,
    result_series: u64,
    result_samples: u64,
    portable_semantic_fingerprint_sha256: String,
    median_duration_ns: u64,
    runs: Vec<HttpQueryRun>,
}

#[derive(Debug)]
struct CanonicalResponse {
    series: Vec<PortableQuerySeries>,
    response_bytes: usize,
    server_query_duration_ns: Option<u64>,
    server_timing: Option<String>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("PromQL HTTP benchmark failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), DynError> {
    validate_args(&args)?;
    let endpoint = reqwest::Url::parse(&args.endpoint)?;
    if endpoint.scheme() != "http" {
        return Err("this local benchmark client accepts only http endpoints".into());
    }
    let headers = build_headers(&args.headers)?;
    let transform = build_transform(&args)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .build()?;

    let mut reference: Option<(String, u64, u64)> = None;
    for _ in 0..args.warmups {
        let (_, canonical) = execute_query(&client, &endpoint, &headers, &args, &transform).await?;
        let shape = canonical_shape(&canonical.series);
        validate_expected(&args, &shape)?;
        validate_reference(&mut reference, &shape)?;
    }

    let mut runs = Vec::with_capacity(args.repeats);
    for run_index in 0..args.repeats {
        let (duration, canonical) =
            execute_query(&client, &endpoint, &headers, &args, &transform).await?;
        let fingerprint_started = Instant::now();
        let shape = canonical_shape(&canonical.series);
        let fingerprint_duration = fingerprint_started.elapsed();
        validate_expected(&args, &shape)?;
        validate_reference(&mut reference, &shape)?;
        runs.push(HttpQueryRun {
            run_index,
            duration_ns: duration_ns(duration),
            server_query_duration_ns: canonical.server_query_duration_ns,
            server_timing: canonical.server_timing,
            fingerprint_duration_ns: duration_ns(fingerprint_duration),
            response_bytes: canonical.response_bytes as u64,
            result_series: shape.1,
            result_samples: shape.2,
            portable_semantic_fingerprint_sha256: shape.0,
        });
    }
    let (fingerprint, result_series, result_samples) = reference
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no query result observed"))?;
    let mut durations = runs.iter().map(|run| run.duration_ns).collect::<Vec<_>>();
    durations.sort_unstable();
    let median_duration_ns = durations[durations.len() / 2];
    let report = HttpQueryReport {
        schema: "chronoxide/promql-http-benchmark/v1",
        name: args.name,
        endpoint: redacted_endpoint(&endpoint),
        header_names: headers
            .keys()
            .map(|name| name.as_str().to_string())
            .collect(),
        query: args.query,
        mode: args.mode,
        time_ms: args.time_ms,
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        step_ms: args.step_ms,
        warmups: args.warmups,
        repeats: args.repeats,
        label_renames: transform
            .renames
            .iter()
            .map(|(source, canonical)| (source.clone(), canonical.clone()))
            .collect(),
        drop_labels: {
            let mut labels = transform.drops.into_iter().collect::<Vec<_>>();
            labels.sort_unstable();
            labels
        },
        expected_fingerprint: args.expected_fingerprint,
        result_series,
        result_samples,
        portable_semantic_fingerprint_sha256: fingerprint,
        median_duration_ns,
        runs,
    };
    write_report(&args.report, &report)?;
    eprintln!(
        "{}: median {:.3} ms, {} series, {} samples, fingerprint {}; report: {}",
        report.name,
        report.median_duration_ns as f64 / 1_000_000.0,
        report.result_series,
        report.result_samples,
        report.portable_semantic_fingerprint_sha256,
        args.report.display()
    );
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), DynError> {
    if args.repeats == 0 {
        return Err("--repeats must be greater than zero".into());
    }
    if args.request_timeout_secs == 0 {
        return Err("--request-timeout-secs must be greater than zero".into());
    }
    match args.mode {
        QueryMode::Instant => {
            if args.time_ms.is_none() {
                return Err("instant mode requires --time-ms".into());
            }
            if args.start_ms.is_some() || args.end_ms.is_some() || args.step_ms.is_some() {
                return Err("instant mode does not accept range timestamps".into());
            }
        }
        QueryMode::Range => {
            let (Some(start), Some(end), Some(step)) = (args.start_ms, args.end_ms, args.step_ms)
            else {
                return Err("range mode requires --start-ms, --end-ms, and --step-ms".into());
            };
            if end < start {
                return Err("--end-ms must be greater than or equal to --start-ms".into());
            }
            if step == 0 {
                return Err("--step-ms must be greater than zero".into());
            }
            if args.time_ms.is_some() {
                return Err("range mode does not accept --time-ms".into());
            }
        }
    }
    if args.report.exists() {
        return Err(format!("report already exists: {}", args.report.display()).into());
    }
    Ok(())
}

fn build_headers(args: &[HeaderArg]) -> Result<HeaderMap, DynError> {
    let mut headers = HeaderMap::new();
    for header in args {
        if header.name == reqwest::header::CONTENT_LENGTH {
            return Err("content-length is managed by the HTTP client".into());
        }
        headers.insert(header.name.clone(), header.value.clone());
    }
    Ok(headers)
}

fn build_transform(args: &Args) -> Result<LabelTransform, DynError> {
    let mut transform = LabelTransform {
        drops: args.drop_labels.iter().cloned().collect(),
        ..LabelTransform::default()
    };
    for rename in &args.label_renames {
        if transform
            .renames
            .insert(rename.source.clone(), rename.canonical.clone())
            .is_some()
        {
            return Err(format!("duplicate label rename for {}", rename.source).into());
        }
    }
    Ok(transform)
}

async fn execute_query(
    client: &reqwest::Client,
    endpoint: &reqwest::Url,
    headers: &HeaderMap,
    args: &Args,
    transform: &LabelTransform,
) -> Result<(Duration, CanonicalResponse), DynError> {
    let mut parameters = vec![("query", args.query.clone())];
    match args.mode {
        QueryMode::Instant => parameters.push(("time", format_seconds(args.time_ms.unwrap()))),
        QueryMode::Range => {
            parameters.push(("start", format_seconds(args.start_ms.unwrap())));
            parameters.push(("end", format_seconds(args.end_ms.unwrap())));
            parameters.push(("step", format_seconds(args.step_ms.unwrap())));
        }
    }
    let started = Instant::now();
    let response = client
        .get(endpoint.clone())
        .headers(headers.clone())
        .query(&parameters)
        .send()
        .await?;
    let status = response.status();
    let (server_query_duration_ns, server_timing) = parse_server_diagnostics(response.headers())?;
    let body = response.bytes().await?;
    let duration = started.elapsed();
    if !status.is_success() {
        return Err(format!("HTTP {status}: {}", response_preview(&body)).into());
    }
    let response_bytes = body.len();
    let document: Value = serde_json::from_slice(&body)?;
    let series = parse_prometheus_response(&document, transform)?;
    Ok((
        duration,
        CanonicalResponse {
            series,
            response_bytes,
            server_query_duration_ns,
            server_timing,
        },
    ))
}

fn parse_server_diagnostics(
    headers: &HeaderMap,
) -> Result<(Option<u64>, Option<String>), DynError> {
    let query_duration_ns = headers
        .get("x-chronoxide-query-duration-ns")
        .map(|value| -> Result<u64, DynError> { Ok(value.to_str()?.parse::<u64>()?) })
        .transpose()?;
    let server_timing = headers
        .get("server-timing")
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()?;
    Ok((query_duration_ns, server_timing))
}

fn parse_prometheus_response(
    document: &Value,
    transform: &LabelTransform,
) -> Result<Vec<PortableQuerySeries>, DynError> {
    if document.get("status").and_then(Value::as_str) != Some("success") {
        return Err(format!(
            "Prometheus API error {}: {}",
            document
                .get("errorType")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            document
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("missing error message")
        )
        .into());
    }
    let data = document
        .get("data")
        .ok_or("Prometheus response is missing data")?;
    let result_type = data
        .get("resultType")
        .and_then(Value::as_str)
        .ok_or("Prometheus response is missing resultType")?;
    let result = data
        .get("result")
        .ok_or("Prometheus response is missing result")?;
    match result_type {
        "vector" => parse_vector(result, transform),
        "matrix" => parse_matrix(result, transform),
        "scalar" => Ok(vec![PortableQuerySeries {
            labels: Vec::new(),
            samples: vec![parse_sample(result)?],
        }]),
        "string" => Err("string PromQL results are not benchmarkable".into()),
        other => Err(format!("unsupported Prometheus resultType {other}").into()),
    }
}

fn parse_vector(
    result: &Value,
    transform: &LabelTransform,
) -> Result<Vec<PortableQuerySeries>, DynError> {
    result
        .as_array()
        .ok_or("vector result is not an array")?
        .iter()
        .map(|entry| {
            reject_native_histogram_result(entry)?;
            Ok(PortableQuerySeries {
                labels: parse_labels(entry.get("metric"), transform)?,
                samples: vec![parse_sample(
                    entry.get("value").ok_or("vector entry is missing value")?,
                )?],
            })
        })
        .collect()
}

fn parse_matrix(
    result: &Value,
    transform: &LabelTransform,
) -> Result<Vec<PortableQuerySeries>, DynError> {
    result
        .as_array()
        .ok_or("matrix result is not an array")?
        .iter()
        .map(|entry| {
            reject_native_histogram_result(entry)?;
            let samples = entry
                .get("values")
                .and_then(Value::as_array)
                .ok_or("matrix entry is missing values")?
                .iter()
                .map(parse_sample)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PortableQuerySeries {
                labels: parse_labels(entry.get("metric"), transform)?,
                samples,
            })
        })
        .collect()
}

fn reject_native_histogram_result(entry: &Value) -> Result<(), DynError> {
    if entry.get("histogram").is_some() || entry.get("histograms").is_some() {
        return Err(
            "native histogram API results require a separate canonical format and are not yet comparable"
                .into(),
        );
    }
    Ok(())
}

fn parse_labels(
    metric: Option<&Value>,
    transform: &LabelTransform,
) -> Result<Vec<(String, String)>, DynError> {
    let object = metric
        .and_then(Value::as_object)
        .ok_or("result metric is not an object")?;
    let mut labels = BTreeMap::new();
    for (source, value) in object {
        if transform.drops.contains(source) {
            continue;
        }
        let canonical = transform.renames.get(source).unwrap_or(source);
        let value = value
            .as_str()
            .ok_or_else(|| format!("label {source} is not a string"))?;
        if labels
            .insert(canonical.clone(), value.to_string())
            .is_some()
        {
            return Err(format!("label transform collides at {canonical}").into());
        }
    }
    Ok(labels.into_iter().collect())
}

fn parse_sample(value: &Value) -> Result<(u64, f64), DynError> {
    let pair = value.as_array().ok_or("sample is not an array")?;
    if pair.len() != 2 {
        return Err(format!("sample must contain two elements, found {}", pair.len()).into());
    }
    let seconds = match &pair[0] {
        Value::Number(value) => value.to_string().parse::<f64>()?,
        Value::String(value) => value.parse::<f64>()?,
        _ => return Err("sample timestamp is not numeric".into()),
    };
    if !seconds.is_finite() || seconds < 0.0 || seconds * 1000.0 > u64::MAX as f64 {
        return Err(format!("sample timestamp is out of range: {seconds}").into());
    }
    let timestamp_ms = (seconds * 1000.0).round() as u64;
    let encoded_value = pair[1].as_str().ok_or("sample value is not a string")?;
    let sample = match encoded_value {
        "NaN" => f64::NAN,
        "+Inf" | "Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        value => value.parse::<f64>()?,
    };
    Ok((timestamp_ms, sample))
}

fn canonical_shape(series: &[PortableQuerySeries]) -> (String, u64, u64) {
    let fingerprint = portable_query_result_fingerprint_sha256(series.to_vec()).to_hex();
    let sample_count = series
        .iter()
        .map(|series| series.samples.len() as u64)
        .sum();
    (fingerprint, series.len() as u64, sample_count)
}

fn validate_expected(args: &Args, shape: &(String, u64, u64)) -> Result<(), DynError> {
    if let Some(expected) = &args.expected_fingerprint
        && shape.0 != *expected
    {
        return Err(format!(
            "portable fingerprint mismatch: expected {expected}, got {}",
            shape.0
        )
        .into());
    }
    if let Some(expected) = args.expected_series
        && shape.1 != expected
    {
        return Err(format!(
            "result series mismatch: expected {expected}, got {}",
            shape.1
        )
        .into());
    }
    if let Some(expected) = args.expected_samples
        && shape.2 != expected
    {
        return Err(format!(
            "result samples mismatch: expected {expected}, got {}",
            shape.2
        )
        .into());
    }
    Ok(())
}

fn validate_reference(
    reference: &mut Option<(String, u64, u64)>,
    shape: &(String, u64, u64),
) -> Result<(), DynError> {
    match reference {
        Some(reference) if reference != shape => Err(format!(
            "query result changed across repetitions: expected {:?}, got {:?}",
            reference, shape
        )
        .into()),
        Some(_) => Ok(()),
        None => {
            *reference = Some(shape.clone());
            Ok(())
        }
    }
}

fn format_seconds(milliseconds: u64) -> String {
    let seconds = milliseconds / 1000;
    let remainder = milliseconds % 1000;
    if remainder == 0 {
        seconds.to_string()
    } else {
        format!("{seconds}.{remainder:03}")
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn redacted_endpoint(endpoint: &reqwest::Url) -> String {
    let mut endpoint = endpoint.clone();
    let _ = endpoint.set_username("");
    let _ = endpoint.set_password(None);
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    endpoint.to_string()
}

fn response_preview(body: &[u8]) -> String {
    const LIMIT: usize = 512;
    String::from_utf8_lossy(&body[..body.len().min(LIMIT)]).into_owned()
}

fn write_report(path: &Path, report: &HttpQueryReport) -> Result<(), DynError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, report)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_parser_maps_labels_and_uses_portable_core_fingerprint() {
        let document: Value = serde_json::from_str(
            r#"{
              "status":"success",
              "data":{"resultType":"vector","result":[
                {"metric":{"service_name":"api","__name__":"requests_total"},
                 "value":[1782985800,"-0"]}
              ]}
            }"#,
        )
        .unwrap();
        let transform = LabelTransform {
            renames: HashMap::from([(
                "service_name".to_string(),
                "service_name_canonical".to_string(),
            )]),
            drops: HashSet::new(),
        };
        let series = parse_prometheus_response(&document, &transform).unwrap();

        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0].labels,
            [
                ("__name__".to_string(), "requests_total".to_string()),
                ("service_name_canonical".to_string(), "api".to_string())
            ]
        );
        assert_eq!(series[0].samples[0].0, 1_782_985_800_000);
        assert_eq!(series[0].samples[0].1.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(
            canonical_shape(&series).0,
            portable_query_result_fingerprint_sha256(series).to_hex()
        );
    }

    #[test]
    fn matrix_parser_accepts_special_values_and_millisecond_timestamps() {
        let document: Value = serde_json::from_str(
            r#"{
              "status":"success",
              "data":{"resultType":"matrix","result":[
                {"metric":{"x":"y"},"values":[
                  [1.001,"NaN"],[2.002,"+Inf"],[3.003,"-Inf"]
                ]}
              ]}
            }"#,
        )
        .unwrap();
        let series = parse_prometheus_response(&document, &LabelTransform::default()).unwrap();

        assert_eq!(
            series[0]
                .samples
                .iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            [1_001, 2_002, 3_003]
        );
        assert!(series[0].samples[0].1.is_nan());
        assert_eq!(series[0].samples[1].1, f64::INFINITY);
        assert_eq!(series[0].samples[2].1, f64::NEG_INFINITY);
    }

    #[test]
    fn parser_rejects_native_histograms_until_their_canonical_format_is_defined() {
        let document: Value = serde_json::from_str(
            r#"{
              "status":"success",
              "data":{"resultType":"vector","result":[
                {"metric":{},"histogram":[1,"count:1 sum:1"]}
              ]}
            }"#,
        )
        .unwrap();
        let error = parse_prometheus_response(&document, &LabelTransform::default()).unwrap_err();
        assert!(error.to_string().contains("native histogram"));
    }

    #[test]
    fn label_transform_rejects_collisions() {
        let metric: Value = serde_json::from_str(r#"{"a":"1","b":"2"}"#).unwrap();
        let transform = LabelTransform {
            renames: HashMap::from([
                ("a".to_string(), "x".to_string()),
                ("b".to_string(), "x".to_string()),
            ]),
            drops: HashSet::new(),
        };

        let error = parse_labels(Some(&metric), &transform).unwrap_err();
        assert!(error.to_string().contains("collides"));
    }

    #[test]
    fn second_formatter_is_exact_for_milliseconds() {
        assert_eq!(format_seconds(1_000), "1");
        assert_eq!(format_seconds(1_001), "1.001");
        assert_eq!(format_seconds(1_010), "1.010");
    }

    #[test]
    fn server_diagnostics_are_optional_and_strict_when_present() {
        assert_eq!(
            parse_server_diagnostics(&HeaderMap::new()).unwrap(),
            (None, None)
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronoxide-query-duration-ns",
            HeaderValue::from_static("1234"),
        );
        headers.insert(
            "server-timing",
            HeaderValue::from_static("queue;dur=0.001, promql;dur=0.002"),
        );
        assert_eq!(
            parse_server_diagnostics(&headers).unwrap(),
            (
                Some(1_234),
                Some("queue;dur=0.001, promql;dur=0.002".to_string())
            )
        );

        headers.insert(
            "x-chronoxide-query-duration-ns",
            HeaderValue::from_static("invalid"),
        );
        assert!(parse_server_diagnostics(&headers).is_err());
    }
}
