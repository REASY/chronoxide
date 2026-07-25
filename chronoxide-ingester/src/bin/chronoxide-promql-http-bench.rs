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
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
struct HttpQueryStats {
    segments_considered: u64,
    segments_queried: u64,
    segments_skipped_by_time: u64,
    segments_skipped_by_missing_equality: u64,
    segments_skipped_by_matcher_time_range: u64,
    matched_series: u64,
    projected_series: u64,
    chunk_reads: u64,
    bytes_read: u64,
    samples_decoded: u64,
    regex_values_examined: u64,
    typed_scalar_chunks_decoded: u64,
    typed_full_chunks_decoded: u64,
    index_postings_reads: u64,
    index_postings_bytes_read: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
struct HttpQueryIo {
    chunk_payload_used_bytes: u64,
    chunk_payload_read_bytes: u64,
    chunk_payload_physical_reads: u64,
    series_entry_bytes: u64,
    chunk_index_range_bytes: u64,
    exact_postings_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveViewDiagnostics {
    generation: u64,
    visible_message_sequence: u64,
    catalog_revision: u64,
    age_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerDiagnostics {
    query_duration_ns: Option<u64>,
    serialize_duration_ns: Option<u64>,
    server_timing: Option<String>,
    live_view: Option<LiveViewDiagnostics>,
    view_pin_wait_ns: Option<u64>,
    view_pin_held_ns: Option<u64>,
    query_stats: Option<HttpQueryStats>,
    query_io: Option<HttpQueryIo>,
}

#[derive(Debug, Serialize)]
struct HttpQueryRun {
    run_index: usize,
    duration_ns: u64,
    server_query_duration_ns: Option<u64>,
    server_serialize_duration_ns: Option<u64>,
    server_timing: Option<String>,
    view_generation: Option<u64>,
    visible_message_sequence: Option<u64>,
    catalog_revision: Option<u64>,
    view_age_ms: Option<u64>,
    view_pin_wait_ns: Option<u64>,
    view_pin_held_ns: Option<u64>,
    query_stats: Option<HttpQueryStats>,
    query_io: Option<HttpQueryIo>,
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
    diagnostics: ServerDiagnostics,
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
        let ServerDiagnostics {
            query_duration_ns,
            serialize_duration_ns,
            server_timing,
            live_view,
            view_pin_wait_ns,
            view_pin_held_ns,
            query_stats,
            query_io,
        } = canonical.diagnostics;
        runs.push(HttpQueryRun {
            run_index,
            duration_ns: duration_ns(duration),
            server_query_duration_ns: query_duration_ns,
            server_serialize_duration_ns: serialize_duration_ns,
            server_timing,
            view_generation: live_view.map(|live| live.generation),
            visible_message_sequence: live_view.map(|live| live.visible_message_sequence),
            catalog_revision: live_view.map(|live| live.catalog_revision),
            view_age_ms: live_view.map(|live| live.age_ms),
            view_pin_wait_ns,
            view_pin_held_ns,
            query_stats,
            query_io,
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
    let diagnostics = parse_server_diagnostics(response.headers())?;
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
            diagnostics,
        },
    ))
}

fn parse_optional_u64_header(
    headers: &HeaderMap,
    name: &'static str,
) -> Result<Option<u64>, DynError> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let encoded = value
        .to_str()
        .map_err(|error| -> DynError { format!("{name} is not valid text: {error}").into() })?;
    encoded
        .parse::<u64>()
        .map(Some)
        .map_err(|error| format!("{name} is not a valid u64: {error}").into())
}

fn parse_server_diagnostics(headers: &HeaderMap) -> Result<ServerDiagnostics, DynError> {
    let query_duration_ns = parse_optional_u64_header(headers, "x-chronoxide-query-duration-ns")?;
    let serialize_duration_ns =
        parse_optional_u64_header(headers, "x-chronoxide-serialize-duration-ns")?;
    let server_timing = headers
        .get("server-timing")
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()?;

    let generation = parse_optional_u64_header(headers, "x-chronoxide-view-generation")?;
    let visible_message_sequence =
        parse_optional_u64_header(headers, "x-chronoxide-visible-message-sequence")?;
    let catalog_revision = parse_optional_u64_header(headers, "x-chronoxide-catalog-revision")?;
    let view_age_ms = parse_optional_u64_header(headers, "x-chronoxide-view-age-ms")?;
    let present_live_headers = [
        generation,
        visible_message_sequence,
        catalog_revision,
        view_age_ms,
    ]
    .iter()
    .filter(|value| value.is_some())
    .count();
    let live_view = match present_live_headers {
        0 => None,
        4 => Some(LiveViewDiagnostics {
            generation: generation.expect("all live headers were counted"),
            visible_message_sequence: visible_message_sequence
                .expect("all live headers were counted"),
            catalog_revision: catalog_revision.expect("all live headers were counted"),
            age_ms: view_age_ms.expect("all live headers were counted"),
        }),
        _ => {
            return Err(
                "live view headers must include generation, visible message sequence, catalog revision, and age together"
                    .into(),
            );
        }
    };

    let view_pin_wait_ns = parse_optional_u64_header(headers, "x-chronoxide-view-pin-wait-ns")?;
    let view_pin_held_ns = parse_optional_u64_header(headers, "x-chronoxide-view-pin-held-ns")?;
    if view_pin_wait_ns.is_some() != view_pin_held_ns.is_some() {
        return Err("live view pin wait and held headers must be present together".into());
    }

    let query_stats = headers
        .get("x-chronoxide-query-stats")
        .map(|value| -> Result<HttpQueryStats, DynError> {
            let encoded = value.to_str().map_err(|error| -> DynError {
                format!("x-chronoxide-query-stats is not valid text: {error}").into()
            })?;
            serde_json::from_str(encoded).map_err(|error| {
                format!("x-chronoxide-query-stats is not valid query stats JSON: {error}").into()
            })
        })
        .transpose()?;
    let query_io = headers
        .get("x-chronoxide-query-io")
        .map(|value| -> Result<HttpQueryIo, DynError> {
            let encoded = value.to_str().map_err(|error| -> DynError {
                format!("x-chronoxide-query-io is not valid text: {error}").into()
            })?;
            serde_json::from_str(encoded).map_err(|error| {
                format!("x-chronoxide-query-io is not valid query I/O JSON: {error}").into()
            })
        })
        .transpose()?;

    Ok(ServerDiagnostics {
        query_duration_ns,
        serialize_duration_ns,
        server_timing,
        live_view,
        view_pin_wait_ns,
        view_pin_held_ns,
        query_stats,
        query_io,
    })
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
    fn server_diagnostics_accept_absent_optional_headers() {
        assert_eq!(
            parse_server_diagnostics(&HeaderMap::new()).unwrap(),
            ServerDiagnostics {
                query_duration_ns: None,
                serialize_duration_ns: None,
                server_timing: None,
                live_view: None,
                view_pin_wait_ns: None,
                view_pin_held_ns: None,
                query_stats: None,
                query_io: None,
            }
        );
    }

    #[test]
    fn server_diagnostics_capture_complete_live_timings_and_typed_stats() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronoxide-query-duration-ns",
            HeaderValue::from_static("1234"),
        );
        headers.insert(
            "x-chronoxide-serialize-duration-ns",
            HeaderValue::from_static("5678"),
        );
        headers.insert(
            "server-timing",
            HeaderValue::from_static("queue;dur=0.001, promql;dur=0.002"),
        );
        headers.insert(
            "x-chronoxide-view-generation",
            HeaderValue::from_static("9"),
        );
        headers.insert(
            "x-chronoxide-visible-message-sequence",
            HeaderValue::from_static("123"),
        );
        headers.insert(
            "x-chronoxide-catalog-revision",
            HeaderValue::from_static("456"),
        );
        headers.insert("x-chronoxide-view-age-ms", HeaderValue::from_static("17"));
        headers.insert(
            "x-chronoxide-view-pin-wait-ns",
            HeaderValue::from_static("21"),
        );
        headers.insert(
            "x-chronoxide-view-pin-held-ns",
            HeaderValue::from_static("22"),
        );
        headers.insert(
            "x-chronoxide-query-stats",
            HeaderValue::from_static(
                r#"{"segments_considered":1,"segments_queried":2,"segments_skipped_by_time":3,"segments_skipped_by_missing_equality":4,"segments_skipped_by_matcher_time_range":5,"matched_series":6,"projected_series":7,"chunk_reads":8,"bytes_read":9,"samples_decoded":10,"regex_values_examined":11,"typed_scalar_chunks_decoded":12,"typed_full_chunks_decoded":13,"index_postings_reads":14,"index_postings_bytes_read":15}"#,
            ),
        );
        headers.insert(
            "x-chronoxide-query-io",
            HeaderValue::from_static(
                r#"{"chunk_payload_used_bytes":21,"chunk_payload_read_bytes":22,"chunk_payload_physical_reads":23,"series_entry_bytes":24,"chunk_index_range_bytes":25,"exact_postings_bytes":26}"#,
            ),
        );

        assert_eq!(
            parse_server_diagnostics(&headers).unwrap(),
            ServerDiagnostics {
                query_duration_ns: Some(1_234),
                serialize_duration_ns: Some(5_678),
                server_timing: Some("queue;dur=0.001, promql;dur=0.002".to_string()),
                live_view: Some(LiveViewDiagnostics {
                    generation: 9,
                    visible_message_sequence: 123,
                    catalog_revision: 456,
                    age_ms: 17,
                }),
                view_pin_wait_ns: Some(21),
                view_pin_held_ns: Some(22),
                query_stats: Some(HttpQueryStats {
                    segments_considered: 1,
                    segments_queried: 2,
                    segments_skipped_by_time: 3,
                    segments_skipped_by_missing_equality: 4,
                    segments_skipped_by_matcher_time_range: 5,
                    matched_series: 6,
                    projected_series: 7,
                    chunk_reads: 8,
                    bytes_read: 9,
                    samples_decoded: 10,
                    regex_values_examined: 11,
                    typed_scalar_chunks_decoded: 12,
                    typed_full_chunks_decoded: 13,
                    index_postings_reads: 14,
                    index_postings_bytes_read: 15,
                }),
                query_io: Some(HttpQueryIo {
                    chunk_payload_used_bytes: 21,
                    chunk_payload_read_bytes: 22,
                    chunk_payload_physical_reads: 23,
                    series_entry_bytes: 24,
                    chunk_index_range_bytes: 25,
                    exact_postings_bytes: 26,
                }),
            }
        );
    }

    #[test]
    fn server_diagnostics_reject_partial_live_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronoxide-view-generation",
            HeaderValue::from_static("1"),
        );
        let error = parse_server_diagnostics(&headers).unwrap_err();
        assert!(error.to_string().contains("must include"));
    }

    #[test]
    fn server_diagnostics_reject_malformed_u64_and_unpaired_pin_timing() {
        let mut malformed = HeaderMap::new();
        malformed.insert(
            "x-chronoxide-view-generation",
            HeaderValue::from_static("invalid"),
        );
        malformed.insert(
            "x-chronoxide-visible-message-sequence",
            HeaderValue::from_static("2"),
        );
        malformed.insert(
            "x-chronoxide-catalog-revision",
            HeaderValue::from_static("3"),
        );
        malformed.insert("x-chronoxide-view-age-ms", HeaderValue::from_static("4"));
        let error = parse_server_diagnostics(&malformed).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("x-chronoxide-view-generation is not a valid u64")
        );

        let mut unpaired = HeaderMap::new();
        unpaired.insert(
            "x-chronoxide-view-pin-wait-ns",
            HeaderValue::from_static("1"),
        );
        let error = parse_server_diagnostics(&unpaired).unwrap_err();
        assert!(error.to_string().contains("must be present together"));
    }

    #[test]
    fn server_diagnostics_reject_malformed_query_stats() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronoxide-query-stats",
            HeaderValue::from_static(r#"{"segments_considered":"not-a-number"}"#),
        );
        let error = parse_server_diagnostics(&headers).unwrap_err();
        assert!(error.to_string().contains("not valid query stats JSON"));
    }

    #[test]
    fn server_diagnostics_reject_malformed_query_io() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronoxide-query-io",
            HeaderValue::from_static(r#"{"chunk_payload_used_bytes":"not-a-number"}"#),
        );
        let error = parse_server_diagnostics(&headers).unwrap_err();
        assert!(error.to_string().contains("not valid query I/O JSON"));
    }
}
