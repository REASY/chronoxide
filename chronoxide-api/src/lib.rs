use std::{
    collections::BTreeMap,
    io,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Form, Router,
    body::Body,
    extract::{
        Query, State,
        rejection::{FormRejection, QueryRejection},
    },
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::Response,
    routing::get,
};
use chrono::DateTime;
use chronoxide_core::{
    promql::{PromqlQuery, PromqlQueryError, parse_query},
    storage::{
        io::{ChunkReadConfig, ChunkReadMode},
        manifest::read_manifest_inventory,
        segment::{
            QueryExecution, QueryLimits, QueryProjectionConfig, QueryStats,
            SegmentStoreOpenOptions, SegmentStoreReader, SegmentStoreSchemaPolicy,
        },
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub query_limits: QueryLimits,
    pub chunk_read_config: ChunkReadConfig,
    pub experimental_cross_segment_chunk_reads: bool,
    pub range_scalar_cache_max_bytes: u64,
    pub max_concurrent_queries: usize,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            query_limits: QueryLimits::production_default(),
            chunk_read_config: ChunkReadConfig::default(),
            experimental_cross_segment_chunk_reads: false,
            range_scalar_cache_max_bytes:
                chronoxide_core::storage::segment::DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES,
            max_concurrent_queries: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
        }
    }
}

/// Immutable sealed-store configuration applied before the HTTP server starts.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreOpenConfig {
    pub validate_segment_footers: bool,
    /// Exact schema required for every manifest-published or discovered segment.
    pub storage_schema_policy: SegmentStoreSchemaPolicy,
    pub query_projection_config: QueryProjectionConfig,
}

impl Default for StoreOpenConfig {
    fn default() -> Self {
        Self {
            validate_segment_footers: false,
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
            query_projection_config: QueryProjectionConfig::default(),
        }
    }
}

#[derive(Clone)]
struct ApiState {
    store: Arc<SegmentStoreReader>,
    config: ApiConfig,
    query_permits: Arc<Semaphore>,
}

pub fn open_store(
    segments_dir: impl AsRef<Path>,
    config: StoreOpenConfig,
) -> io::Result<SegmentStoreReader> {
    let segments_dir = segments_dir.as_ref();
    let manifest_dir = segments_dir.join("manifest");
    let options = SegmentStoreOpenOptions {
        validate_segment_footers: config.validate_segment_footers,
        storage_schema_policy: config.storage_schema_policy,
        ..SegmentStoreOpenOptions::default()
    };
    let store = if read_manifest_inventory(&manifest_dir)?.is_some() {
        SegmentStoreReader::open_manifest_published_with_options(
            segments_dir,
            manifest_dir,
            options,
        )
    } else {
        SegmentStoreReader::open_with_options(segments_dir, options)
    }?;
    Ok(store.with_query_projection_config(config.query_projection_config))
}

pub fn router(store: SegmentStoreReader, config: ApiConfig) -> io::Result<Router> {
    if config.max_concurrent_queries == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max concurrent queries must be greater than zero",
        ));
    }
    chronoxide_core::storage::segment::validate_range_scalar_cache_budget_bytes(
        config.range_scalar_cache_max_bytes,
    )
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    // Validate the selected backend during startup, before the readiness route exists.
    chronoxide_core::storage::io::ChunkReader::new(config.chunk_read_config.clone())?;

    let state = ApiState {
        store: Arc::new(store),
        query_permits: Arc::new(Semaphore::new(config.max_concurrent_queries)),
        config,
    };
    Ok(Router::new()
        .route("/-/healthy", get(health))
        .route("/-/ready", get(health))
        .route("/api/v1/query", get(instant_get).post(instant_post))
        .route("/api/v1/query_range", get(range_get).post(range_post))
        .with_state(state))
}

async fn health() -> &'static str {
    "Chronoxide is Ready.\n"
}

#[derive(Debug, Deserialize)]
struct InstantParams {
    query: String,
    time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RangeParams {
    query: String,
    start: String,
    end: String,
    step: String,
}

async fn instant_get(
    State(state): State<ApiState>,
    params: Result<Query<InstantParams>, QueryRejection>,
) -> Response {
    match params {
        Ok(Query(params)) => execute_instant(state, params).await,
        Err(err) => error_response(StatusCode::BAD_REQUEST, "bad_data", err.body_text(), None),
    }
}

async fn instant_post(
    State(state): State<ApiState>,
    params: Result<Form<InstantParams>, FormRejection>,
) -> Response {
    match params {
        Ok(Form(params)) => execute_instant(state, params).await,
        Err(err) => error_response(StatusCode::BAD_REQUEST, "bad_data", err.body_text(), None),
    }
}

async fn range_get(
    State(state): State<ApiState>,
    params: Result<Query<RangeParams>, QueryRejection>,
) -> Response {
    match params {
        Ok(Query(params)) => execute_range(state, params).await,
        Err(err) => error_response(StatusCode::BAD_REQUEST, "bad_data", err.body_text(), None),
    }
}

async fn range_post(
    State(state): State<ApiState>,
    params: Result<Form<RangeParams>, FormRejection>,
) -> Response {
    match params {
        Ok(Form(params)) => execute_range(state, params).await,
        Err(err) => error_response(StatusCode::BAD_REQUEST, "bad_data", err.body_text(), None),
    }
}

async fn execute_instant(state: ApiState, params: InstantParams) -> Response {
    let evaluation_ms = match params.time {
        Some(value) => match parse_timestamp_ms(&value) {
            Ok(value) => value,
            Err(err) => return bad_data(err),
        },
        None => match now_ms() {
            Ok(value) => value,
            Err(err) => return internal_error(err),
        },
    };
    execute_query(state, params.query, QueryKind::Instant { evaluation_ms }).await
}

async fn execute_range(state: ApiState, params: RangeParams) -> Response {
    let start_ms = match parse_timestamp_ms(&params.start) {
        Ok(value) => value,
        Err(err) => return bad_data(format!("invalid start: {err}")),
    };
    let end_ms = match parse_timestamp_ms(&params.end) {
        Ok(value) => value,
        Err(err) => return bad_data(format!("invalid end: {err}")),
    };
    let step_ms = match parse_duration_ms(&params.step) {
        Ok(value) => value,
        Err(err) => return bad_data(format!("invalid step: {err}")),
    };
    if start_ms > end_ms {
        return bad_data("start must not be after end");
    }
    execute_query(
        state,
        params.query,
        QueryKind::Range {
            start_ms,
            end_ms,
            step_ms,
        },
    )
    .await
}

#[derive(Clone, Copy)]
enum QueryKind {
    Instant {
        evaluation_ms: u64,
    },
    Range {
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    },
}

struct TimedExecution {
    execution: Result<QueryExecution, PromqlQueryError>,
    query_duration: Duration,
    is_scalar: bool,
}

async fn execute_query(state: ApiState, query: String, kind: QueryKind) -> Response {
    let queue_started = Instant::now();
    let permit = match Arc::clone(&state.query_permits).acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => return internal_error("query admission is closed"),
    };
    let queue_duration = queue_started.elapsed();
    let store = Arc::clone(&state.store);
    let config = state.config.clone();
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let started = Instant::now();
        let scalar_result = match kind {
            QueryKind::Instant { .. } => {
                parse_query(&query).map(|query| query_returns_scalar(&query))
            }
            QueryKind::Range { .. } => Ok(false),
        };
        let execution = (|| {
            scalar_result.as_ref().map_err(Clone::clone)?;
            let mut session = store.query_session().map_err(PromqlQueryError::from)?;
            session
                .set_chunk_read_config(config.chunk_read_config)
                .map_err(PromqlQueryError::from)?;
            session.set_experimental_cross_segment_chunk_reads(
                config.experimental_cross_segment_chunk_reads,
            );
            session
                .set_range_scalar_cache_budget_bytes(config.range_scalar_cache_max_bytes)
                .map_err(|err| PromqlQueryError::Storage(err.to_string()))?;
            match kind {
                QueryKind::Instant { evaluation_ms } => {
                    session.query_promql_at_with_limits(&query, evaluation_ms, config.query_limits)
                }
                QueryKind::Range {
                    start_ms,
                    end_ms,
                    step_ms,
                } => session.query_promql_range_with_limits(
                    &query,
                    start_ms,
                    end_ms,
                    step_ms,
                    config.query_limits,
                ),
            }
        })();
        TimedExecution {
            execution,
            query_duration: started.elapsed(),
            is_scalar: scalar_result.unwrap_or(false),
        }
    })
    .await;

    let timed = match task {
        Ok(timed) => timed,
        Err(err) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                format!("query worker failed: {err}"),
                Some(Timings {
                    queue: queue_duration,
                    query: Duration::ZERO,
                    serialize: Duration::ZERO,
                }),
            );
        }
    };
    let execution = match timed.execution {
        Ok(execution) => execution,
        Err(err) => {
            return promql_error_response(
                err,
                Some(Timings {
                    queue: queue_duration,
                    query: timed.query_duration,
                    serialize: Duration::ZERO,
                }),
            );
        }
    };

    let stats = execution.stats;
    // Building the Prometheus response value formats samples and walks every
    // result label, so it is part of response serialization work.
    let serialize_started = Instant::now();
    let data = match kind {
        QueryKind::Instant { .. } if timed.is_scalar => encode_scalar(&execution),
        QueryKind::Instant { .. } => encode_vector(&execution),
        QueryKind::Range { .. } => encode_matrix(&execution),
    };
    let bytes = match serde_json::to_vec(&SuccessEnvelope {
        status: "success",
        data,
    }) {
        Ok(bytes) => bytes,
        Err(err) => return internal_error(format!("response serialization failed: {err}")),
    };
    let timings = Timings {
        queue: queue_duration,
        query: timed.query_duration,
        serialize: serialize_started.elapsed(),
    };
    json_response(StatusCode::OK, bytes, Some(timings), Some(stats))
}

#[derive(Serialize)]
struct SuccessEnvelope {
    status: &'static str,
    data: Value,
}

fn encode_scalar(execution: &QueryExecution) -> Value {
    let value = execution
        .results
        .iter()
        .find_map(|series| series.samples.last());
    json!({
        "resultType": "scalar",
        "result": value.map(|(timestamp, value)| sample_value(*timestamp, *value)).unwrap_or(Value::Null),
    })
}

fn encode_vector(execution: &QueryExecution) -> Value {
    let result: Vec<_> = execution
        .results
        .iter()
        .filter_map(|series| {
            let (timestamp, value) = series.samples.last()?;
            Some(json!({
                "metric": labels_value(series.labels.pairs()),
                "value": sample_value(*timestamp, *value),
            }))
        })
        .collect();
    json!({ "resultType": "vector", "result": result })
}

fn encode_matrix(execution: &QueryExecution) -> Value {
    let result: Vec<_> = execution
        .results
        .iter()
        .map(|series| {
            let values: Vec<_> = series
                .samples
                .iter()
                .map(|(timestamp, value)| sample_value(*timestamp, *value))
                .collect();
            json!({ "metric": labels_value(series.labels.pairs()), "values": values })
        })
        .collect();
    json!({ "resultType": "matrix", "result": result })
}

fn labels_value<'a>(labels: impl Iterator<Item = (&'a str, &'a str)>) -> Value {
    let labels: BTreeMap<_, _> = labels.collect();
    serde_json::to_value(labels).expect("string label map is serializable")
}

fn sample_value(timestamp_ms: u64, value: f64) -> Value {
    json!([timestamp_ms as f64 / 1_000.0, format_sample(value)])
}

fn format_sample(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        value.to_string()
    }
}

fn query_returns_scalar(query: &PromqlQuery) -> bool {
    match query {
        PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::ScalarFunction(_) => true,
        PromqlQuery::Offset(offset) => query_returns_scalar(&offset.input),
        PromqlQuery::BinaryExpression(binary) => {
            query_returns_scalar(&binary.left) && query_returns_scalar(&binary.right)
        }
        _ => false,
    }
}

fn parse_timestamp_ms(input: &str) -> Result<u64, String> {
    if let Ok(seconds) = input.parse::<f64>() {
        if !seconds.is_finite() || seconds < 0.0 {
            return Err("timestamp must be a finite, non-negative Unix time".to_string());
        }
        let millis = seconds * 1_000.0;
        if millis > u64::MAX as f64 {
            return Err("timestamp is too large".to_string());
        }
        return Ok(millis.round() as u64);
    }
    let timestamp = DateTime::parse_from_rfc3339(input)
        .map_err(|_| "expected Unix seconds or RFC3339".to_string())?;
    u64::try_from(timestamp.timestamp_millis())
        .map_err(|_| "timestamps before Unix epoch are unsupported".to_string())
}

fn parse_duration_ms(input: &str) -> Result<u64, String> {
    if let Ok(seconds) = input.parse::<f64>() {
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err("duration must be finite and greater than zero".to_string());
        }
        let millis = seconds * 1_000.0;
        if millis > u64::MAX as f64 || millis.round() < 1.0 {
            return Err("duration is outside the supported millisecond range".to_string());
        }
        return Ok(millis.round() as u64);
    }

    let mut rest = input;
    let mut total = 0_u64;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err("expected a positive number followed by a duration unit".to_string());
        }
        let value = rest[..digits]
            .parse::<u64>()
            .map_err(|_| "duration component is too large".to_string())?;
        rest = &rest[digits..];
        let (unit, multiplier) = if rest.starts_with("ms") {
            ("ms", 1_u64)
        } else {
            let unit = rest
                .get(..1)
                .ok_or_else(|| "duration unit is missing".to_string())?;
            let multiplier = match unit {
                "s" => 1_000,
                "m" => 60_000,
                "h" => 3_600_000,
                "d" => 86_400_000,
                "w" => 604_800_000,
                "y" => 31_536_000_000,
                _ => return Err(format!("unsupported duration unit {unit:?}")),
            };
            (unit, multiplier)
        };
        rest = &rest[unit.len()..];
        total = total
            .checked_add(
                value
                    .checked_mul(multiplier)
                    .ok_or_else(|| "duration is too large".to_string())?,
            )
            .ok_or_else(|| "duration is too large".to_string())?;
    }
    if total == 0 {
        Err("duration must be greater than zero".to_string())
    } else {
        Ok(total)
    }
}

fn now_ms() -> Result<u64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock precedes Unix epoch: {err}"))?;
    u64::try_from(duration.as_millis()).map_err(|_| "system time is too large".to_string())
}

#[derive(Clone, Copy)]
struct Timings {
    queue: Duration,
    query: Duration,
    serialize: Duration,
}

fn bad_data(message: impl Into<String>) -> Response {
    error_response(StatusCode::BAD_REQUEST, "bad_data", message, None)
}

fn internal_error(message: impl Into<String>) -> Response {
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal", message, None)
}

fn promql_error_response(err: PromqlQueryError, timings: Option<Timings>) -> Response {
    let (status, error_type) = match err {
        PromqlQueryError::Invalid(_) => (StatusCode::BAD_REQUEST, "bad_data"),
        PromqlQueryError::Unsupported(_)
        | PromqlQueryError::LimitExceeded { .. }
        | PromqlQueryError::Storage(_) => (StatusCode::UNPROCESSABLE_ENTITY, "execution"),
    };
    error_response(status, error_type, err.to_string(), timings)
}

fn error_response(
    status: StatusCode,
    error_type: &'static str,
    message: impl Into<String>,
    timings: Option<Timings>,
) -> Response {
    let serialize_started = Instant::now();
    let bytes = serde_json::to_vec(&json!({
        "status": "error",
        "errorType": error_type,
        "error": message.into(),
    }))
    .expect("error envelope is serializable");
    let timings = timings.map(|mut timings| {
        timings.serialize = serialize_started.elapsed();
        timings
    });
    json_response(status, bytes, timings, None)
}

fn json_response(
    status: StatusCode,
    bytes: Vec<u8>,
    timings: Option<Timings>,
    stats: Option<QueryStats>,
) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(timings) = timings {
        let server_timing = format!(
            "queue;dur={:.3}, promql;dur={:.3}, serialize;dur={:.3}",
            timings.queue.as_secs_f64() * 1_000.0,
            timings.query.as_secs_f64() * 1_000.0,
            timings.serialize.as_secs_f64() * 1_000.0,
        );
        response.headers_mut().insert(
            "server-timing",
            HeaderValue::from_str(&server_timing).expect("timing header is valid"),
        );
        response.headers_mut().insert(
            "x-chronoxide-query-duration-ns",
            HeaderValue::from_str(&timings.query.as_nanos().to_string())
                .expect("duration header is valid"),
        );
        response.headers_mut().insert(
            "x-chronoxide-serialize-duration-ns",
            HeaderValue::from_str(&timings.serialize.as_nanos().to_string())
                .expect("serialization duration header is valid"),
        );
    }
    if let Some(stats) = stats {
        let value = json!({
            "segments_considered": stats.segments_considered,
            "segments_queried": stats.segments_queried,
            "matched_series": stats.matched_series,
            "projected_series": stats.projected_series,
            "chunk_reads": stats.chunk_reads,
            "bytes_read": stats.bytes_read,
            "samples_decoded": stats.samples_decoded,
            "regex_values_examined": stats.regex_values_examined,
        })
        .to_string();
        response.headers_mut().insert(
            "x-chronoxide-query-stats",
            HeaderValue::from_str(&value).expect("query stats header is valid"),
        );
    }
    response
}

pub fn parse_chunk_read_mode(value: &str) -> Result<ChunkReadMode, String> {
    match value {
        "auto" => Ok(ChunkReadMode::Auto),
        "io-uring" | "iouring" => Ok(ChunkReadMode::IoUring),
        "pread" => Ok(ChunkReadMode::Pread),
        _ => Err("expected auto, io-uring, or pread".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronoxide_core::{
        labels::SeriesRef,
        promql::METRIC_NAME_LABEL,
        storage::segment::{QueryLabelStoragePolicy, SegmentWriter, SegmentWriterConfig},
    };

    fn shared_atom_execution(range: bool) -> QueryExecution {
        let tempdir = tempfile::tempdir().expect("temporary corpus");
        let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
            tempdir.path(),
            Duration::from_secs(60),
        ))
        .expect("segment writer");
        writer
            .record_samples_with_labels(
                SeriesRef::new(1),
                &[
                    (METRIC_NAME_LABEL.to_owned(), "cpu_usage".to_owned()),
                    ("host".to_owned(), "api-1".to_owned()),
                ],
                &[(5_000, 1.5), (15_000, 2.5)],
            )
            .expect("record samples");
        writer.flush().expect("flush corpus");

        let store = SegmentStoreReader::open(tempdir.path()).expect("open corpus");
        let mut session = store.query_session().expect("query session");
        session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .expect("select shared labels before querying");
        if range {
            session
                .query_promql_range_with_limits(
                    "cpu_usage",
                    5_000,
                    15_000,
                    10_000,
                    QueryLimits::unlimited(),
                )
                .expect("range query")
        } else {
            session
                .query_promql_at_with_limits("cpu_usage", 15_000, QueryLimits::unlimited())
                .expect("instant query")
        }
    }

    fn assert_shared_labels_are_not_compatibility_materialized(execution: &QueryExecution) {
        assert!(!execution.results.is_empty());
        for result in &execution.results {
            assert_eq!(
                result
                    .labels
                    .shared_atoms_compatibility_view_materialized_for_test(),
                Some(false)
            );
        }
    }

    #[test]
    fn timestamps_accept_unix_seconds_and_rfc3339() {
        assert_eq!(parse_timestamp_ms("12.345"), Ok(12_345));
        assert_eq!(parse_timestamp_ms("1970-01-01T00:00:12.345Z"), Ok(12_345));
        assert!(parse_timestamp_ms("-1").is_err());
    }

    #[test]
    fn durations_accept_seconds_and_composed_prometheus_units() {
        assert_eq!(parse_duration_ms("1.5"), Ok(1_500));
        assert_eq!(parse_duration_ms("1h30m5s250ms"), Ok(5_405_250));
        assert!(parse_duration_ms("0").is_err());
    }

    #[test]
    fn sample_format_preserves_prometheus_special_values() {
        assert_eq!(format_sample(f64::NAN), "NaN");
        assert_eq!(format_sample(f64::INFINITY), "+Inf");
        assert_eq!(format_sample(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_sample(-0.0), "-0");
    }

    #[test]
    fn vector_encoding_keeps_shared_labels_borrowed() {
        let execution = shared_atom_execution(false);
        assert_shared_labels_are_not_compatibility_materialized(&execution);

        let data = encode_vector(&execution);
        let bytes = serde_json::to_vec(&SuccessEnvelope {
            status: "success",
            data,
        })
        .expect("serialize vector response");

        assert_shared_labels_are_not_compatibility_materialized(&execution);
        let body: Value = serde_json::from_slice(&bytes).expect("decode vector response");
        assert_eq!(body["data"]["resultType"], "vector");
        assert_eq!(body["data"]["result"][0]["metric"]["__name__"], "cpu_usage");
        assert_eq!(body["data"]["result"][0]["metric"]["host"], "api-1");
    }

    #[test]
    fn matrix_encoding_keeps_shared_labels_borrowed() {
        let execution = shared_atom_execution(true);
        assert_shared_labels_are_not_compatibility_materialized(&execution);

        let data = encode_matrix(&execution);
        let bytes = serde_json::to_vec(&SuccessEnvelope {
            status: "success",
            data,
        })
        .expect("serialize matrix response");

        assert_shared_labels_are_not_compatibility_materialized(&execution);
        let body: Value = serde_json::from_slice(&bytes).expect("decode matrix response");
        assert_eq!(body["data"]["resultType"], "matrix");
        assert_eq!(body["data"]["result"][0]["metric"]["__name__"], "cpu_usage");
        assert_eq!(body["data"]["result"][0]["metric"]["host"], "api-1");
    }
}
