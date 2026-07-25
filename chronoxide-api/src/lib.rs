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
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::DateTime;
use chronoxide_core::{
    promql::{PromqlQuery, PromqlQueryError, parse_query},
    storage::{
        io::{ChunkReadConfig, ChunkReadMode},
        live_view::{
            LiveQueryHandle, LiveQueryPin, LiveRootLockTiming, LiveStorageView, LiveViewError,
        },
        manifest::read_manifest_inventory,
        segment::{
            QueryExecution, QueryLimits, QueryProjectionConfig, QueryStats,
            SegmentStoreOpenOptions, SegmentStoreQueryProfile, SegmentStoreReader,
            SegmentStoreSchemaPolicy,
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
    store: ApiStore,
    config: ApiConfig,
    query_permits: Arc<Semaphore>,
}

#[derive(Clone)]
enum ApiStore {
    Sealed(Arc<SegmentStoreReader>),
    Live(Arc<LiveQueryHandle<LiveStorageView>>),
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
    router_for_store(ApiStore::Sealed(Arc::new(store)), config)
}

/// Builds the embedded ingester router over one atomically published live
/// storage generation.
///
/// Query admission acquires the concurrency permit before pinning the view.
/// The standalone [`router`] remains sealed-only.
pub fn live_router(
    handle: Arc<LiveQueryHandle<LiveStorageView>>,
    config: ApiConfig,
) -> io::Result<Router> {
    if !handle.query_admission_configured() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "live router requires configured query-retention admission",
        ));
    }
    router_for_store(ApiStore::Live(handle), config)
}

fn router_for_store(store: ApiStore, config: ApiConfig) -> io::Result<Router> {
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
        store,
        query_permits: Arc::new(Semaphore::new(config.max_concurrent_queries)),
        config,
    };
    Ok(Router::new()
        .route("/-/healthy", get(health))
        .route("/-/ready", get(readiness))
        .route("/api/v1/query", get(instant_get).post(instant_post))
        .route("/api/v1/query_range", get(range_get).post(range_post))
        .with_state(state))
}

async fn health() -> &'static str {
    // Preserve the sealed API's historical probe body exactly. Liveness and
    // readiness differ by status behavior in live mode, not by wire spelling.
    "Chronoxide is Ready.\n"
}

async fn readiness(State(state): State<ApiState>) -> Response {
    match &state.store {
        ApiStore::Sealed(_) => "Chronoxide is Ready.\n".into_response(),
        ApiStore::Live(handle) => match handle.can_admit_query(Instant::now()) {
            Ok(()) => "Chronoxide is Ready.\n".into_response(),
            Err(error) => live_unavailable(error),
        },
    }
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
    query_io: Option<QueryIoDiagnostics>,
    query_duration: Duration,
    is_scalar: bool,
    // Keep the exact generation alive through response serialization, not
    // merely through storage evaluation.
    _live_pin: Option<LiveQueryPin<LiveStorageView>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct QueryIoDiagnostics {
    chunk_payload_used_bytes: u64,
    chunk_payload_read_bytes: u64,
    chunk_payload_physical_reads: u64,
    series_entry_bytes: u64,
    chunk_index_range_bytes: u64,
    exact_postings_bytes: u64,
}

impl QueryIoDiagnostics {
    fn from_profile(profile: &SegmentStoreQueryProfile) -> Self {
        Self {
            chunk_payload_used_bytes: profile.chunk_payload_bytes,
            chunk_payload_read_bytes: profile.chunk_payload_physical_bytes,
            chunk_payload_physical_reads: profile.chunk_payload_physical_reads,
            series_entry_bytes: profile.series_entry_bytes,
            chunk_index_range_bytes: profile.chunk_index_range_bytes,
            exact_postings_bytes: profile.exact_postings_bytes,
        }
    }
}

enum PinnedApiStore {
    Sealed(Arc<SegmentStoreReader>),
    Live(LiveQueryPin<LiveStorageView>),
}

#[derive(Clone, Copy)]
struct LiveResponseMeta {
    generation: u64,
    published_at: Instant,
    visible_message_sequence: u64,
    catalog_revision: u64,
    pin_root_lock: LiveRootLockTiming,
}

async fn execute_query(state: ApiState, query: String, kind: QueryKind) -> Response {
    let queue_started = Instant::now();
    let permit = match Arc::clone(&state.query_permits).acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => return internal_error("query admission is closed"),
    };
    let queue_duration = queue_started.elapsed();
    // The permit deliberately precedes the pin: time spent queued must not
    // retain an obsolete generation or its payload allocations.
    let (store, live_meta) = match &state.store {
        ApiStore::Sealed(store) => (PinnedApiStore::Sealed(Arc::clone(store)), None),
        ApiStore::Live(handle) => match handle.try_pin_admitted(Instant::now()) {
            Ok(view) => {
                let meta = LiveResponseMeta {
                    generation: view.generation(),
                    published_at: view.published_at(),
                    visible_message_sequence: view.visible_message_sequence(),
                    catalog_revision: view.catalog_revision(),
                    pin_root_lock: view.root_lock_timing(),
                };
                (PinnedApiStore::Live(view), Some(meta))
            }
            Err(error) => return live_unavailable(error),
        },
    };
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
        let execution_with_io = (|| {
            scalar_result.as_ref().map_err(Clone::clone)?;
            let mut session = match &store {
                PinnedApiStore::Sealed(store) => {
                    store.query_session().map_err(PromqlQueryError::from)?
                }
                PinnedApiStore::Live(view) => view
                    .payload()
                    .sealed()
                    .query_session_with_head_view(view.payload().head())
                    .map_err(PromqlQueryError::from)?,
            };
            session
                .set_chunk_read_config(config.chunk_read_config)
                .map_err(PromqlQueryError::from)?;
            session.set_experimental_cross_segment_chunk_reads(
                config.experimental_cross_segment_chunk_reads,
            );
            session
                .set_range_scalar_cache_budget_bytes(config.range_scalar_cache_max_bytes)
                .map_err(|err| PromqlQueryError::Storage(err.to_string()))?;
            let execution = match kind {
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
            }?;
            let query_io = QueryIoDiagnostics::from_profile(&session.profile());
            Ok((execution, query_io))
        })();
        let (execution, query_io) = match execution_with_io {
            Ok((execution, query_io)) => (Ok(execution), Some(query_io)),
            Err(error) => (Err(error), None),
        };
        let live_pin = match store {
            PinnedApiStore::Sealed(_) => None,
            PinnedApiStore::Live(pin) => Some(pin),
        };
        TimedExecution {
            execution,
            query_io,
            query_duration: started.elapsed(),
            is_scalar: scalar_result.unwrap_or(false),
            _live_pin: live_pin,
        }
    })
    .await;

    let timed = match task {
        Ok(timed) => timed,
        Err(err) => {
            return with_live_headers(
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    format!("query worker failed: {err}"),
                    Some(Timings {
                        queue: queue_duration,
                        query: Duration::ZERO,
                        serialize: Duration::ZERO,
                    }),
                ),
                live_meta,
            );
        }
    };
    let query_io = timed.query_io;
    let execution = match timed.execution {
        Ok(execution) => execution,
        Err(err) => {
            return with_live_headers(
                promql_error_response(
                    err,
                    Some(Timings {
                        queue: queue_duration,
                        query: timed.query_duration,
                        serialize: Duration::ZERO,
                    }),
                ),
                live_meta,
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
        Err(err) => {
            return with_live_headers(
                internal_error(format!("response serialization failed: {err}")),
                live_meta,
            );
        }
    };
    let timings = Timings {
        queue: queue_duration,
        query: timed.query_duration,
        serialize: serialize_started.elapsed(),
    };
    with_live_headers(
        json_response(StatusCode::OK, bytes, Some(timings), Some(stats), query_io),
        live_meta,
    )
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

fn live_unavailable(error: LiveViewError) -> Response {
    error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        error.to_string(),
        None,
    )
}

fn with_live_headers(mut response: Response, live: Option<LiveResponseMeta>) -> Response {
    let Some(live) = live else {
        return response;
    };
    let age_ms = Instant::now()
        .saturating_duration_since(live.published_at)
        .as_millis();
    response.headers_mut().insert(
        "x-chronoxide-view-generation",
        HeaderValue::from_str(&live.generation.to_string())
            .expect("live view generation header is valid"),
    );
    response.headers_mut().insert(
        "x-chronoxide-view-age-ms",
        HeaderValue::from_str(&age_ms.to_string()).expect("live view age header is valid"),
    );
    response.headers_mut().insert(
        "x-chronoxide-visible-message-sequence",
        HeaderValue::from_str(&live.visible_message_sequence.to_string())
            .expect("live view message sequence header is valid"),
    );
    response.headers_mut().insert(
        "x-chronoxide-catalog-revision",
        HeaderValue::from_str(&live.catalog_revision.to_string())
            .expect("live view catalog revision header is valid"),
    );
    response.headers_mut().insert(
        "x-chronoxide-view-pin-wait-ns",
        HeaderValue::from_str(&live.pin_root_lock.wait.as_nanos().to_string())
            .expect("live view pin wait header is valid"),
    );
    response.headers_mut().insert(
        "x-chronoxide-view-pin-held-ns",
        HeaderValue::from_str(&live.pin_root_lock.held.as_nanos().to_string())
            .expect("live view pin hold header is valid"),
    );
    response
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
    json_response(status, bytes, timings, None, None)
}

fn json_response(
    status: StatusCode,
    bytes: Vec<u8>,
    timings: Option<Timings>,
    stats: Option<QueryStats>,
    query_io: Option<QueryIoDiagnostics>,
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
            "segments_skipped_by_time": stats.segments_skipped_by_time,
            "segments_skipped_by_missing_equality": stats.segments_skipped_by_missing_equality,
            "segments_skipped_by_matcher_time_range": stats.segments_skipped_by_matcher_time_range,
            "segments_queried": stats.segments_queried,
            "matched_series": stats.matched_series,
            "projected_series": stats.projected_series,
            "chunk_reads": stats.chunk_reads,
            "bytes_read": stats.bytes_read,
            "samples_decoded": stats.samples_decoded,
            "typed_scalar_chunks_decoded": stats.typed_scalar_chunks_decoded,
            "typed_full_chunks_decoded": stats.typed_full_chunks_decoded,
            "regex_values_examined": stats.regex_values_examined,
            "index_postings_reads": stats.index_postings_reads,
            "index_postings_bytes_read": stats.index_postings_bytes_read,
        })
        .to_string();
        response.headers_mut().insert(
            "x-chronoxide-query-stats",
            HeaderValue::from_str(&value).expect("query stats header is valid"),
        );
    }
    if let Some(query_io) = query_io {
        let value = json!({
            "chunk_payload_used_bytes": query_io.chunk_payload_used_bytes,
            "chunk_payload_read_bytes": query_io.chunk_payload_read_bytes,
            "chunk_payload_physical_reads": query_io.chunk_payload_physical_reads,
            "series_entry_bytes": query_io.series_entry_bytes,
            "chunk_index_range_bytes": query_io.chunk_index_range_bytes,
            "exact_postings_bytes": query_io.exact_postings_bytes,
        })
        .to_string();
        response.headers_mut().insert(
            "x-chronoxide-query-io",
            HeaderValue::from_str(&value).expect("query I/O diagnostics header is valid"),
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

    fn label_storage_execution(policy: QueryLabelStoragePolicy, range: bool) -> QueryExecution {
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
            .set_query_label_storage_policy(policy)
            .expect("select label storage before querying");
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
        let execution = label_storage_execution(QueryLabelStoragePolicy::SharedAtoms, false);
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
        let execution = label_storage_execution(QueryLabelStoragePolicy::SharedAtoms, true);
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

    #[test]
    fn vector_and_matrix_encoding_resolve_compact_ids_without_owned_compatibility() {
        for range in [false, true] {
            let execution = label_storage_execution(QueryLabelStoragePolicy::CompactIds, range);
            assert!(!execution.results.is_empty());
            assert!(execution.results.iter().all(|result| {
                result
                    .labels
                    .compact_ids_compatibility_view_materialized_for_test()
                    == Some(false)
            }));

            let data = if range {
                encode_matrix(&execution)
            } else {
                encode_vector(&execution)
            };
            let bytes = serde_json::to_vec(&SuccessEnvelope {
                status: "success",
                data,
            })
            .expect("serialize compact-label response");

            assert!(execution.results.iter().all(|result| {
                result
                    .labels
                    .compact_ids_compatibility_view_materialized_for_test()
                    == Some(false)
            }));
            let body: Value = serde_json::from_slice(&bytes).expect("decode compact response");
            let expected_type = if range { "matrix" } else { "vector" };
            assert_eq!(body["data"]["resultType"], expected_type);
            assert_eq!(body["data"]["result"][0]["metric"]["__name__"], "cpu_usage");
            assert_eq!(body["data"]["result"][0]["metric"]["host"], "api-1");
        }
    }
}
