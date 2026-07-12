use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};

use chronoxide_core::otlp_capture::{CaptureManifest, OtlpCaptureReader};
use clap::Parser;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::metric;
use prost::Message;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;
use sha2::{Digest, Sha256};

const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_BATCH_MESSAGES: usize = 512;

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Parser)]
#[command(about = "Replay a Chronoxide OTLP capture to an OTLP/HTTP metrics endpoint")]
struct Args {
    #[arg(long)]
    capture: PathBuf,
    #[arg(long)]
    endpoint: String,
    #[arg(long = "header", value_name = "NAME=VALUE")]
    headers: Vec<HeaderArg>,
    #[arg(long, default_value_t = DEFAULT_MAX_BATCH_BYTES)]
    max_batch_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_BATCH_MESSAGES)]
    max_batch_messages: usize,
    #[arg(long)]
    start_source_message: Option<u64>,
    #[arg(long)]
    max_source_messages: Option<u64>,
    #[arg(long, default_value_t = 120)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 10_000)]
    progress_every: u64,
    #[arg(long)]
    drop_missing_number_values: bool,
    #[arg(long)]
    max_event_age_secs: Option<u64>,
    #[arg(long)]
    max_event_lead_secs: Option<u64>,
    #[arg(long)]
    report: PathBuf,
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
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| format!("invalid header name: {error}"))?;
        let value = HeaderValue::from_str(value)
            .map_err(|error| format!("invalid header value: {error}"))?;
        Ok(Self { name, value })
    }
}

#[derive(Debug, Default)]
struct PendingBatch {
    request: ExportMetricsServiceRequest,
    source_messages: u64,
    estimated_source_bytes: u64,
    resource_metrics: u64,
    data_points: u64,
    filter_counts: DatapointFilterCounts,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
}

impl PendingBatch {
    fn is_empty(&self) -> bool {
        self.source_messages == 0
    }

    fn would_exceed(&self, payload_len: usize, max_bytes: usize, max_messages: usize) -> bool {
        !self.is_empty()
            && (self.source_messages as usize >= max_messages
                || self
                    .estimated_source_bytes
                    .saturating_add(payload_len as u64)
                    > max_bytes as u64)
    }

    fn push(
        &mut self,
        sequence: u64,
        payload_len: usize,
        mut request: ExportMetricsServiceRequest,
        filter_counts: DatapointFilterCounts,
    ) {
        let resource_metrics = request.resource_metrics.len() as u64;
        let data_points = count_data_points(&request);
        self.request
            .resource_metrics
            .append(&mut request.resource_metrics);
        self.source_messages = self.source_messages.saturating_add(1);
        self.estimated_source_bytes = self
            .estimated_source_bytes
            .saturating_add(payload_len as u64);
        self.resource_metrics = self.resource_metrics.saturating_add(resource_metrics);
        self.data_points = self.data_points.saturating_add(data_points);
        self.filter_counts.add(filter_counts);
        self.first_sequence.get_or_insert(sequence);
        self.last_sequence = Some(sequence);
    }
}

#[derive(Debug, Clone, Copy)]
struct EventTimeFilter {
    max_event_age_ms: u64,
    max_event_lead_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct DatapointFilterCounts {
    observed: u64,
    dropped_too_old: u64,
    dropped_too_future: u64,
    dropped_missing_timestamp: u64,
    dropped_missing_number_value: u64,
}

impl DatapointFilterCounts {
    fn add(&mut self, other: Self) {
        self.observed = self.observed.saturating_add(other.observed);
        self.dropped_too_old = self.dropped_too_old.saturating_add(other.dropped_too_old);
        self.dropped_too_future = self
            .dropped_too_future
            .saturating_add(other.dropped_too_future);
        self.dropped_missing_timestamp = self
            .dropped_missing_timestamp
            .saturating_add(other.dropped_missing_timestamp);
        self.dropped_missing_number_value = self
            .dropped_missing_number_value
            .saturating_add(other.dropped_missing_number_value);
    }
}

#[derive(Debug, Serialize)]
struct ReplayReport {
    schema: &'static str,
    capture: String,
    capture_manifest_sha256: Option<String>,
    capture_manifest: Option<CaptureManifest>,
    endpoint: String,
    header_names: Vec<String>,
    max_batch_bytes: usize,
    max_batch_messages: usize,
    start_source_message: Option<u64>,
    max_source_messages: Option<u64>,
    drop_missing_number_values: bool,
    max_event_age_secs: Option<u64>,
    max_event_lead_secs: Option<u64>,
    source_messages: u64,
    source_payload_bytes: u64,
    resource_metrics: u64,
    data_points: u64,
    datapoint_filter: DatapointFilterCounts,
    http_requests: u64,
    emitted_protobuf_bytes: u64,
    warning_responses: u64,
    last_warning: Option<String>,
    request_duration_ns_total: u64,
    request_duration_ns_min: u64,
    request_duration_ns_max: u64,
    elapsed_ns: u64,
}

#[derive(Debug, Default)]
struct ReplayStats {
    source_messages: u64,
    source_payload_bytes: u64,
    resource_metrics: u64,
    data_points: u64,
    datapoint_filter: DatapointFilterCounts,
    http_requests: u64,
    emitted_protobuf_bytes: u64,
    warning_responses: u64,
    last_warning: Option<String>,
    request_duration_ns_total: u64,
    request_duration_ns_min: u64,
    request_duration_ns_max: u64,
}

impl ReplayStats {
    fn observe_request(&mut self, batch: &PendingBatch, encoded_len: usize, duration: Duration) {
        let duration_ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.source_messages = self.source_messages.saturating_add(batch.source_messages);
        self.source_payload_bytes = self
            .source_payload_bytes
            .saturating_add(batch.estimated_source_bytes);
        self.resource_metrics = self.resource_metrics.saturating_add(batch.resource_metrics);
        self.data_points = self.data_points.saturating_add(batch.data_points);
        self.datapoint_filter.add(batch.filter_counts);
        self.http_requests = self.http_requests.saturating_add(1);
        self.emitted_protobuf_bytes = self
            .emitted_protobuf_bytes
            .saturating_add(encoded_len as u64);
        self.request_duration_ns_total = self.request_duration_ns_total.saturating_add(duration_ns);
        if self.request_duration_ns_min == 0 {
            self.request_duration_ns_min = duration_ns;
        } else {
            self.request_duration_ns_min = self.request_duration_ns_min.min(duration_ns);
        }
        self.request_duration_ns_max = self.request_duration_ns_max.max(duration_ns);
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("OTLP HTTP replay failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), DynError> {
    validate_args(&args)?;
    let event_time_filter = event_time_filter_from_args(&args)?;
    let endpoint = reqwest::Url::parse(&args.endpoint)?;
    if endpoint.scheme() != "http" {
        return Err("this local benchmark client accepts only http endpoints".into());
    }
    let endpoint_for_report = redacted_endpoint(&endpoint);
    let headers = build_headers(&args.headers)?;
    let header_names = headers
        .keys()
        .map(|name| name.as_str().to_string())
        .collect::<Vec<_>>();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .build()?;
    let mut reader = OtlpCaptureReader::open(&args.capture)?;
    let manifest = reader.manifest().cloned();
    let manifest_sha256 = manifest.as_ref().map(manifest_fingerprint).transpose()?;
    let started = Instant::now();
    let mut stats = ReplayStats::default();
    let mut batch = PendingBatch::default();

    let start_source_message = args.start_source_message.unwrap_or(0);
    while reader.messages_read() < start_source_message {
        if reader.next()?.is_none() {
            return Err(format!(
                "capture ended before --start-source-message {start_source_message}"
            )
            .into());
        }
    }

    loop {
        if args
            .max_source_messages
            .is_some_and(|maximum| reader.messages_read() >= maximum)
        {
            break;
        }
        let Some(record) = reader.next()? else {
            break;
        };
        let sequence = reader.messages_read().saturating_sub(1);
        if batch.would_exceed(
            record.payload.len(),
            args.max_batch_bytes,
            args.max_batch_messages,
        ) {
            send_batch(&client, endpoint.clone(), &headers, &mut batch, &mut stats).await?;
        }
        let mut request =
            ExportMetricsServiceRequest::decode(record.payload.as_slice()).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("capture sequence {sequence} is not valid OTLP metrics: {error}"),
                )
            })?;
        let filter_counts = filter_request_datapoints(
            &mut request,
            record.captured_at_ms,
            event_time_filter,
            args.drop_missing_number_values,
        );
        batch.push(sequence, record.payload.len(), request, filter_counts);
        if args.progress_every != 0 && reader.messages_read().is_multiple_of(args.progress_every) {
            eprintln!(
                "read {} source messages; sent {} HTTP requests and {} datapoints",
                reader.messages_read(),
                stats.http_requests,
                stats.data_points
            );
        }
    }
    if !batch.is_empty() {
        send_batch(&client, endpoint, &headers, &mut batch, &mut stats).await?;
    }

    let report = ReplayReport {
        schema: "chronoxide/otlp-http-replay/v2",
        capture: args.capture.display().to_string(),
        capture_manifest_sha256: manifest_sha256,
        capture_manifest: manifest,
        endpoint: endpoint_for_report,
        header_names,
        max_batch_bytes: args.max_batch_bytes,
        max_batch_messages: args.max_batch_messages,
        start_source_message: args.start_source_message,
        max_source_messages: args.max_source_messages,
        drop_missing_number_values: args.drop_missing_number_values,
        max_event_age_secs: args.max_event_age_secs,
        max_event_lead_secs: args.max_event_lead_secs,
        source_messages: stats.source_messages,
        source_payload_bytes: stats.source_payload_bytes,
        resource_metrics: stats.resource_metrics,
        data_points: stats.data_points,
        datapoint_filter: stats.datapoint_filter,
        http_requests: stats.http_requests,
        emitted_protobuf_bytes: stats.emitted_protobuf_bytes,
        warning_responses: stats.warning_responses,
        last_warning: stats.last_warning,
        request_duration_ns_total: stats.request_duration_ns_total,
        request_duration_ns_min: stats.request_duration_ns_min,
        request_duration_ns_max: stats.request_duration_ns_max,
        elapsed_ns: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
    };
    write_report(&args.report, &report)?;
    eprintln!(
        "replayed {} messages and {} datapoints in {} requests; report: {}",
        report.source_messages,
        report.data_points,
        report.http_requests,
        args.report.display()
    );
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), DynError> {
    if args.max_batch_bytes == 0 {
        return Err("--max-batch-bytes must be greater than zero".into());
    }
    if args.max_batch_messages == 0 {
        return Err("--max-batch-messages must be greater than zero".into());
    }
    if args.request_timeout_secs == 0 {
        return Err("--request-timeout-secs must be greater than zero".into());
    }
    if args.max_event_age_secs.is_some() != args.max_event_lead_secs.is_some() {
        return Err(
            "--max-event-age-secs and --max-event-lead-secs must be provided together".into(),
        );
    }
    if let (Some(start), Some(end)) = (args.start_source_message, args.max_source_messages)
        && end <= start
    {
        return Err("--max-source-messages must be greater than --start-source-message".into());
    }
    if args.report.exists() {
        return Err(format!("report already exists: {}", args.report.display()).into());
    }
    Ok(())
}

fn event_time_filter_from_args(args: &Args) -> Result<Option<EventTimeFilter>, DynError> {
    let (Some(max_age_secs), Some(max_lead_secs)) =
        (args.max_event_age_secs, args.max_event_lead_secs)
    else {
        return Ok(None);
    };
    let max_event_age_ms = max_age_secs
        .checked_mul(1_000)
        .ok_or("--max-event-age-secs overflows milliseconds")?;
    let max_event_lead_ms = max_lead_secs
        .checked_mul(1_000)
        .ok_or("--max-event-lead-secs overflows milliseconds")?;
    Ok(Some(EventTimeFilter {
        max_event_age_ms,
        max_event_lead_ms,
    }))
}

fn build_headers(args: &[HeaderArg]) -> Result<HeaderMap, DynError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-protobuf"),
    );
    for header in args {
        if header.name == reqwest::header::CONTENT_LENGTH {
            return Err("content-length is managed by the HTTP client".into());
        }
        headers.insert(header.name.clone(), header.value.clone());
    }
    Ok(headers)
}

async fn send_batch(
    client: &reqwest::Client,
    endpoint: reqwest::Url,
    headers: &HeaderMap,
    batch: &mut PendingBatch,
    stats: &mut ReplayStats,
) -> Result<(), DynError> {
    let first_sequence = batch.first_sequence.unwrap_or(0);
    let last_sequence = batch.last_sequence.unwrap_or(first_sequence);
    let body = batch.request.encode_to_vec();
    let started = Instant::now();
    let response = client
        .post(endpoint)
        .headers(headers.clone())
        .body(body.clone())
        .send()
        .await?;
    let duration = started.elapsed();
    let status = response.status();
    let response_body = response.bytes().await?;
    if !status.is_success() {
        return Err(format!(
            "HTTP {status} for capture sequences {first_sequence}..={last_sequence}: {}",
            response_preview(&response_body)
        )
        .into());
    }
    let decoded = ExportMetricsServiceResponse::decode(response_body.as_ref()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid OTLP response for capture sequences {first_sequence}..={last_sequence}: {error}"
            ),
        )
    })?;
    if let Some(partial) = decoded.partial_success {
        if partial.rejected_data_points != 0 {
            return Err(format!(
                "endpoint rejected {} datapoints for capture sequences {}..={}: {}",
                partial.rejected_data_points, first_sequence, last_sequence, partial.error_message
            )
            .into());
        }
        if !partial.error_message.is_empty() {
            stats.warning_responses = stats.warning_responses.saturating_add(1);
            stats.last_warning = Some(partial.error_message);
        }
    }
    stats.observe_request(batch, body.len(), duration);
    *batch = PendingBatch::default();
    Ok(())
}

fn count_data_points(request: &ExportMetricsServiceRequest) -> u64 {
    request
        .resource_metrics
        .iter()
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics)
        .map(|metric| match &metric.data {
            Some(metric::Data::Gauge(value)) => value.data_points.len() as u64,
            Some(metric::Data::Sum(value)) => value.data_points.len() as u64,
            Some(metric::Data::Histogram(value)) => value.data_points.len() as u64,
            Some(metric::Data::ExponentialHistogram(value)) => value.data_points.len() as u64,
            Some(metric::Data::Summary(value)) => value.data_points.len() as u64,
            None => 0,
        })
        .sum()
}

fn filter_request_datapoints(
    request: &mut ExportMetricsServiceRequest,
    captured_at_ms: i64,
    event_time_filter: Option<EventTimeFilter>,
    drop_missing_number_values: bool,
) -> DatapointFilterCounts {
    let mut counts = DatapointFilterCounts::default();
    for resource in &mut request.resource_metrics {
        for scope in &mut resource.scope_metrics {
            for metric in &mut scope.metrics {
                match &mut metric.data {
                    Some(metric::Data::Gauge(value)) => {
                        filter_points(
                            &mut value.data_points,
                            |point| point.time_unix_nano,
                            |point| point.value.is_none(),
                            captured_at_ms,
                            event_time_filter,
                            drop_missing_number_values,
                            &mut counts,
                        );
                    }
                    Some(metric::Data::Sum(value)) => {
                        filter_points(
                            &mut value.data_points,
                            |point| point.time_unix_nano,
                            |point| point.value.is_none(),
                            captured_at_ms,
                            event_time_filter,
                            drop_missing_number_values,
                            &mut counts,
                        );
                    }
                    Some(metric::Data::Histogram(value)) => {
                        filter_points(
                            &mut value.data_points,
                            |point| point.time_unix_nano,
                            |_| false,
                            captured_at_ms,
                            event_time_filter,
                            false,
                            &mut counts,
                        );
                    }
                    Some(metric::Data::ExponentialHistogram(value)) => {
                        filter_points(
                            &mut value.data_points,
                            |point| point.time_unix_nano,
                            |_| false,
                            captured_at_ms,
                            event_time_filter,
                            false,
                            &mut counts,
                        );
                    }
                    Some(metric::Data::Summary(value)) => {
                        filter_points(
                            &mut value.data_points,
                            |point| point.time_unix_nano,
                            |_| false,
                            captured_at_ms,
                            event_time_filter,
                            false,
                            &mut counts,
                        );
                    }
                    None => {}
                }
            }
            scope
                .metrics
                .retain(|metric| metric_data_point_count(metric) != 0);
        }
        resource
            .scope_metrics
            .retain(|scope| !scope.metrics.is_empty());
    }
    request
        .resource_metrics
        .retain(|resource| !resource.scope_metrics.is_empty());
    counts
}

fn filter_points<T>(
    points: &mut Vec<T>,
    timestamp: impl Fn(&T) -> u64,
    missing_number_value: impl Fn(&T) -> bool,
    captured_at_ms: i64,
    event_time_filter: Option<EventTimeFilter>,
    drop_missing_number_values: bool,
    counts: &mut DatapointFilterCounts,
) {
    counts.observed = counts.observed.saturating_add(points.len() as u64);
    points.retain(|point| {
        if let Some(filter) = event_time_filter {
            match event_time_decision(timestamp(point), captured_at_ms, filter) {
                EventTimeDecision::Accept => {}
                EventTimeDecision::TooOld => {
                    counts.dropped_too_old = counts.dropped_too_old.saturating_add(1);
                    return false;
                }
                EventTimeDecision::TooFuture => {
                    counts.dropped_too_future = counts.dropped_too_future.saturating_add(1);
                    return false;
                }
                EventTimeDecision::MissingTimestamp => {
                    counts.dropped_missing_timestamp =
                        counts.dropped_missing_timestamp.saturating_add(1);
                    return false;
                }
            }
        }
        if drop_missing_number_values && missing_number_value(point) {
            counts.dropped_missing_number_value =
                counts.dropped_missing_number_value.saturating_add(1);
            return false;
        }
        true
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventTimeDecision {
    Accept,
    TooOld,
    TooFuture,
    MissingTimestamp,
}

fn event_time_decision(
    time_unix_nano: u64,
    captured_at_ms: i64,
    filter: EventTimeFilter,
) -> EventTimeDecision {
    if time_unix_nano == 0 {
        return EventTimeDecision::MissingTimestamp;
    }
    let event_ms = i128::from(time_unix_nano / 1_000_000);
    let captured_at_ms = i128::from(captured_at_ms);
    if event_ms < captured_at_ms - i128::from(filter.max_event_age_ms) {
        EventTimeDecision::TooOld
    } else if event_ms > captured_at_ms + i128::from(filter.max_event_lead_ms) {
        EventTimeDecision::TooFuture
    } else {
        EventTimeDecision::Accept
    }
}

fn metric_data_point_count(metric: &opentelemetry_proto::tonic::metrics::v1::Metric) -> usize {
    match &metric.data {
        Some(metric::Data::Gauge(value)) => value.data_points.len(),
        Some(metric::Data::Sum(value)) => value.data_points.len(),
        Some(metric::Data::Histogram(value)) => value.data_points.len(),
        Some(metric::Data::ExponentialHistogram(value)) => value.data_points.len(),
        Some(metric::Data::Summary(value)) => value.data_points.len(),
        None => 0,
    }
}

fn manifest_fingerprint(manifest: &CaptureManifest) -> Result<String, DynError> {
    let bytes = serde_json::to_vec(manifest)?;
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(output)
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

fn write_report(path: &Path, report: &ReplayReport) -> Result<(), DynError> {
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess;
    use opentelemetry_proto::tonic::metrics::v1::{Gauge, Metric, NumberDataPoint};

    fn request_with_points(points: usize) -> ExportMetricsServiceRequest {
        use opentelemetry_proto::tonic::metrics::v1::{ResourceMetrics, ScopeMetrics};
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint::default(); points],
                        })),
                        ..Metric::default()
                    }],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            }],
        }
    }

    #[test]
    fn pending_batch_preserves_resource_order_and_counts_points() {
        let mut first = request_with_points(2);
        first.resource_metrics[0].schema_url = "first".to_string();
        let mut second = request_with_points(3);
        second.resource_metrics[0].schema_url = "second".to_string();
        let mut batch = PendingBatch::default();
        batch.push(7, 100, first, DatapointFilterCounts::default());
        batch.push(8, 200, second, DatapointFilterCounts::default());

        assert_eq!(batch.source_messages, 2);
        assert_eq!(batch.estimated_source_bytes, 300);
        assert_eq!(batch.resource_metrics, 2);
        assert_eq!(batch.data_points, 5);
        assert_eq!(batch.first_sequence, Some(7));
        assert_eq!(batch.last_sequence, Some(8));
        assert_eq!(batch.request.resource_metrics[0].schema_url, "first");
        assert_eq!(batch.request.resource_metrics[1].schema_url, "second");
    }

    #[test]
    fn batch_bounds_flush_before_overflow_but_allow_one_oversized_message() {
        let mut batch = PendingBatch::default();
        assert!(!batch.would_exceed(101, 100, 2));
        batch.push(
            0,
            101,
            request_with_points(1),
            DatapointFilterCounts::default(),
        );
        assert!(batch.would_exceed(1, 100, 2));

        let mut by_count = PendingBatch::default();
        by_count.push(
            0,
            1,
            request_with_points(1),
            DatapointFilterCounts::default(),
        );
        by_count.push(
            1,
            1,
            request_with_points(1),
            DatapointFilterCounts::default(),
        );
        assert!(by_count.would_exceed(1, 100, 2));
    }

    #[test]
    fn missing_number_filter_counts_drops_and_removes_empty_envelopes() {
        use opentelemetry_proto::tonic::metrics::v1::number_data_point;

        let mut partially_present = request_with_points(2);
        let Some(metric::Data::Gauge(gauge)) =
            &mut partially_present.resource_metrics[0].scope_metrics[0].metrics[0].data
        else {
            panic!("fixture must contain a gauge");
        };
        gauge.data_points[0].value = Some(number_data_point::Value::AsInt(7));

        let counts = filter_request_datapoints(&mut partially_present, 0, None, true);
        assert_eq!(counts.dropped_missing_number_value, 1);
        assert_eq!(count_data_points(&partially_present), 1);

        let mut entirely_missing = request_with_points(1);
        let counts = filter_request_datapoints(&mut entirely_missing, 0, None, true);
        assert_eq!(counts.dropped_missing_number_value, 1);
        assert!(entirely_missing.resource_metrics.is_empty());
    }

    #[test]
    fn event_time_filter_matches_inclusive_chronoxide_bounds_and_decision_order() {
        use opentelemetry_proto::tonic::metrics::v1::number_data_point;

        let mut request = request_with_points(6);
        let Some(metric::Data::Gauge(gauge)) =
            &mut request.resource_metrics[0].scope_metrics[0].metrics[0].data
        else {
            panic!("fixture must contain a gauge");
        };
        let timestamps_ms = [0, 99_000, 98_999, 105_000, 105_001, 100_000];
        for (point, timestamp_ms) in gauge.data_points.iter_mut().zip(timestamps_ms) {
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.value = Some(number_data_point::Value::AsInt(7));
        }
        gauge.data_points[0].value = None;
        gauge.data_points[5].value = None;

        let counts = filter_request_datapoints(
            &mut request,
            100_000,
            Some(EventTimeFilter {
                max_event_age_ms: 1_000,
                max_event_lead_ms: 5_000,
            }),
            true,
        );

        assert_eq!(counts.observed, 6);
        assert_eq!(counts.dropped_too_old, 1);
        assert_eq!(counts.dropped_too_future, 1);
        assert_eq!(counts.dropped_missing_timestamp, 1);
        assert_eq!(counts.dropped_missing_number_value, 1);
        let retained = match &request.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(metric::Data::Gauge(gauge)) => &gauge.data_points,
            _ => panic!("fixture must retain its gauge"),
        };
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].time_unix_nano, 99_000 * 1_000_000);
        assert_eq!(retained[1].time_unix_nano, 105_000 * 1_000_000);
    }

    #[test]
    fn header_parser_requires_valid_name_value_syntax() {
        let header = "X-Greptime-DB-Name=public".parse::<HeaderArg>().unwrap();
        assert_eq!(header.name, "x-greptime-db-name");
        assert_eq!(header.value, "public");
        assert!("missing-separator".parse::<HeaderArg>().is_err());
    }

    #[test]
    fn endpoint_reporting_removes_credentials_query_and_fragment() {
        let endpoint = reqwest::Url::parse(
            "https://user:password@example.test/v1/metrics?token=secret#fragment",
        )
        .unwrap();
        assert_eq!(
            redacted_endpoint(&endpoint),
            "https://example.test/v1/metrics"
        );
    }

    #[tokio::test]
    async fn send_batch_posts_ordered_protobuf_and_accounts_success() {
        let response = ExportMetricsServiceResponse::default().encode_to_vec();
        let (endpoint, server) = spawn_http_response(200, response);
        let mut first = request_with_points(2);
        first.resource_metrics[0].schema_url = "first".to_string();
        let mut second = request_with_points(3);
        second.resource_metrics[0].schema_url = "second".to_string();
        let mut batch = PendingBatch::default();
        batch.push(4, 100, first, DatapointFilterCounts::default());
        batch.push(5, 200, second, DatapointFilterCounts::default());
        let mut stats = ReplayStats::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        send_batch(
            &client,
            endpoint,
            &build_headers(&[]).unwrap(),
            &mut batch,
            &mut stats,
        )
        .await
        .unwrap();
        let received = server.join().unwrap();
        let decoded = ExportMetricsServiceRequest::decode(received.as_slice()).unwrap();

        assert_eq!(decoded.resource_metrics.len(), 2);
        assert_eq!(decoded.resource_metrics[0].schema_url, "first");
        assert_eq!(decoded.resource_metrics[1].schema_url, "second");
        assert!(batch.is_empty());
        assert_eq!(stats.source_messages, 2);
        assert_eq!(stats.source_payload_bytes, 300);
        assert_eq!(stats.resource_metrics, 2);
        assert_eq!(stats.data_points, 5);
        assert_eq!(stats.http_requests, 1);
        assert!(stats.emitted_protobuf_bytes > 0);
    }

    #[tokio::test]
    async fn send_batch_rejects_otlp_partial_success_without_publishing_stats() {
        let response = ExportMetricsServiceResponse {
            partial_success: Some(ExportMetricsPartialSuccess {
                rejected_data_points: 2,
                error_message: "invalid points".to_string(),
            }),
        }
        .encode_to_vec();
        let (endpoint, server) = spawn_http_response(200, response);
        let mut batch = PendingBatch::default();
        batch.push(
            9,
            100,
            request_with_points(2),
            DatapointFilterCounts::default(),
        );
        let mut stats = ReplayStats::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let error = send_batch(
            &client,
            endpoint,
            &build_headers(&[]).unwrap(),
            &mut batch,
            &mut stats,
        )
        .await
        .unwrap_err();
        server.join().unwrap();

        assert!(error.to_string().contains("rejected 2 datapoints"));
        assert_eq!(stats.http_requests, 0);
        assert_eq!(stats.data_points, 0);
        assert_eq!(batch.first_sequence, Some(9));
    }

    fn spawn_http_response(
        status: u16,
        response_body: Vec<u8>,
    ) -> (reqwest::Url, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut received = Vec::new();
            let header_end = loop {
                let mut buffer = [0u8; 4096];
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0, "request ended before HTTP headers");
                received.extend_from_slice(&buffer[..count]);
                if let Some(index) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    break index + 4;
                }
            };
            let headers = String::from_utf8(received[..header_end].to_vec()).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while received.len() - header_end < content_length {
                let mut buffer = [0u8; 4096];
                let count = stream.read(&mut buffer).unwrap();
                assert_ne!(count, 0, "request body ended early");
                received.extend_from_slice(&buffer[..count]);
            }
            let reason = if status == 200 { "OK" } else { "Error" };
            write!(
                stream,
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            )
            .unwrap();
            stream.write_all(&response_body).unwrap();
            stream.flush().unwrap();
            received[header_end..header_end + content_length].to_vec()
        });
        (
            reqwest::Url::parse(&format!("http://{address}/v1/metrics")).unwrap(),
            server,
        )
    }
}
