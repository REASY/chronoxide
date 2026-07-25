use chronoxide_api::live_router;
use chronoxide_capture::{CompressionMethod, OtlpCaptureWriter};
use chronoxide_core::storage::head::HeadConfig;
use chronoxide_core::storage::segment::SegmentWriter;
use chronoxide_ingester::allocator_policy::{
    AllocatorRuntimePolicy, allocator_preflight_requested,
};
use chronoxide_ingester::app_config::AppConfig;
use chronoxide_ingester::error::{ChronoxideError, ErrorKind};
use chronoxide_ingester::ingester::{Ingester, IngestionConfig, KafkaConsumerConfig};
use chronoxide_ingester::processor::{EventTimePolicy, LivePublisherConfig, OtlpLabelSetProcessor};
use chronoxide_ingester::runtime::load_config;
use chronoxide_ingester::source::{CapturingSource, FileSource, KafkaSource};
use chronoxide_ingester::telemetry::{init_meter_provider, init_otlp_logging, setup_local_logging};
use opentelemetry::global;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing::{error, info, warn};

#[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

struct EmbeddedApiServer {
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl EmbeddedApiServer {
    fn start(listener: tokio::net::TcpListener, app: axum::Router) -> Self {
        let shutdown = CancellationToken::new();
        let shutdown_signal = shutdown.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal.cancelled_owned())
                .await
        });
        Self { shutdown, task }
    }

    #[cfg(test)]
    async fn shutdown(self) -> io::Result<()> {
        self.shutdown.cancel();
        flatten_api_join(self.task.await)
    }
}

fn main() -> Result<(), ChronoxideError> {
    // Capture this before argument parsing, allocator introspection, logging,
    // and construction of Tokio's worker pool.  The diagnostic checkpoint is
    // therefore a main-entry-to-workload-boundary timer, not merely an async
    // body timer.
    let main_started = Instant::now();
    let allocator_preflight = allocator_preflight_requested(std::env::args().skip(1))
        .map_err(|error| ChronoxideError::new(ErrorKind::ConfigError(error)))?;
    let allocator_policy =
        AllocatorRuntimePolicy::from_environment(main_started, allocator_preflight)
            .map_err(|error| ChronoxideError::new(ErrorKind::ConfigError(error)))?;
    if allocator_preflight {
        let preflight = allocator_policy
            .preflight()
            .map_err(|error| ChronoxideError::new(ErrorKind::ConfigError(error)))?;
        println!("{}", serde_json::to_string(&preflight)?);
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async_main(allocator_policy))
}

async fn async_main(allocator_policy: AllocatorRuntimePolicy) -> Result<(), ChronoxideError> {
    let runtime_evidence = if allocator_policy.runtime_diagnostics_enabled() {
        let evidence = allocator_policy
            .runtime_evidence()
            .map_err(|error| ChronoxideError::new(ErrorKind::ConfigError(error)))?;
        eprintln!(
            "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON={}",
            serde_json::to_string(&evidence)?
        );
        Some(evidence)
    } else {
        None
    };

    let otlp_logs_enabled = otlp_logs_enabled();
    let otlp_metrics_enabled = otlp_metrics_enabled();

    let logger_provider = if otlp_logs_enabled {
        Some(init_otlp_logging(
            "chronoxide-ingester",
            LevelFilter::INFO,
            &["chronoxide_ingester", "chronoxide_core"],
        )?)
    } else {
        setup_local_logging(
            LevelFilter::INFO,
            &["chronoxide_ingester", "chronoxide_core"],
        )?;
        None
    };

    let meter_provider = if otlp_metrics_enabled {
        init_meter_provider("chronoxide-ingester", std::time::Duration::from_secs(5))?
    } else {
        SdkMeterProvider::builder().build()
    };
    global::set_meter_provider(meter_provider.clone());
    if !otlp_logs_enabled {
        info!("OTLP logging disabled (no OTEL endpoint configured)");
    }
    if !otlp_metrics_enabled {
        info!("OTLP metrics disabled (no OTEL endpoint configured)");
    }
    if let Some(runtime_evidence) = runtime_evidence.as_ref() {
        info!(
            rust_global_allocator = allocator_policy.identity().as_str(),
            jemalloc_conf_env = runtime_evidence.jemalloc_conf_env,
            requested_policy_raw = ?runtime_evidence.requested_policy_raw,
            requested_policy_canonical = ?runtime_evidence.requested_policy_canonical,
            effective_policy = ?runtime_evidence.effective_policy,
            allocator_internal_telemetry = if runtime_evidence.effective_policy.is_some() {
                "fixed_startup_options_and_release_stats"
            } else {
                "unavailable"
            },
            post_ingester_drop_hold_secs = runtime_evidence.post_ingester_drop_hold_secs,
            post_ingester_drop_telemetry_enabled = runtime_evidence
                .post_ingester_drop_telemetry_enabled,
            "Rust global allocator policy selected"
        );
    } else {
        info!(
            rust_global_allocator = allocator_policy.identity().as_str(),
            "Rust global allocator selected"
        );
    }
    info!("Meter provider initialized");

    let config_file = std::env::var("CONFIG_FILE").expect("CONFIG_FILE env var not set");
    let config: AppConfig = load_config(&config_file)?;
    config.validate().map_err(ErrorKind::ConfigError)?;
    info!(
        "\n{}",
        toml::to_string_pretty(&config).expect("Failed to serialize config")
    );

    let kafka_consumer_config = config.kafka.to_kafka_consumer_config(None);

    let shutdown = Arc::new(Notify::new());
    let ct = CancellationToken::new();
    let copied_ct = ct.clone();
    let shutdown_task = tokio::spawn(async move {
        let sigterm = async {
            #[cfg(unix)]
            {
                match signal::unix::signal(signal::unix::SignalKind::terminate()) {
                    Ok(mut sigterm) => {
                        sigterm.recv().await;
                    }
                    Err(_) => {
                        std::future::pending::<()>().await;
                    }
                }
            }
            #[cfg(not(unix))]
            {
                std::future::pending::<()>().await;
            }
        };

        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Ctrl+C received");
            },
            _ = sigterm => {
                info!("SIGTERM received");
            }
        }

        copied_ct.cancel();
    });

    let meter = meter_provider.meter("chronoxide-ingester");

    let replay_from = config.ingestion.replay_from.clone().map(PathBuf::from);
    let capture_to = config.ingestion.capture_to.clone().map(PathBuf::from);

    let segment_writer_config = config.ingestion.segment_writer.to_core_config();

    let ingestion_config = IngestionConfig {
        max_event_age: chrono::TimeDelta::seconds(config.ingestion.max_event_age_secs as i64),
        max_event_lead: chrono::TimeDelta::seconds(config.ingestion.max_event_lead_secs),
        drop_outdated: config.ingestion.drop_outdated,
        labelset_store: config.ingestion.labelset_store,
        labelset_report_interval: std::time::Duration::from_secs(
            config.ingestion.labelset_report_interval_secs,
        ),
        stop_after_messages: config.ingestion.stop_after_messages,
        replay_from: replay_from.clone(),
        capture_to: capture_to.clone(),
        capture_only: config.ingestion.capture_only,
        segment_writer: segment_writer_config.clone(),
    };

    let head_config = if let Some(cfg) = segment_writer_config.as_ref() {
        Some(
            HeadConfig::new(
                cfg.segment_duration,
                config.ingestion.segment_writer.float_encoding,
                config.ingestion.segment_writer.int_encoding,
            )
            .with_varlen_encoding(config.ingestion.segment_writer.varlen_encoding)
            .with_out_of_order_time_window(std::time::Duration::from_secs(
                config.ingestion.head_buffer.out_of_order_time_window_secs,
            ))
            .with_compact_numeric_series(config.ingestion.head_buffer.compact_numeric_series)
            .with_adaptive_series_table(config.ingestion.head_buffer.adaptive_series_table)
            .with_adaptive_last_timestamp_table(
                config.ingestion.head_buffer.adaptive_last_timestamp_table,
            ),
        )
    } else if config.ingestion.head_buffer.enabled {
        Some(
            HeadConfig::new(
                std::time::Duration::from_secs(config.ingestion.head_buffer.window_duration_secs),
                config.ingestion.head_buffer.float_encoding,
                config.ingestion.head_buffer.int_encoding,
            )
            .with_varlen_encoding(config.ingestion.head_buffer.varlen_encoding)
            .with_out_of_order_time_window(std::time::Duration::from_secs(
                config.ingestion.head_buffer.out_of_order_time_window_secs,
            ))
            .with_compact_numeric_series(config.ingestion.head_buffer.compact_numeric_series)
            .with_adaptive_series_table(config.ingestion.head_buffer.adaptive_series_table)
            .with_adaptive_last_timestamp_table(
                config.ingestion.head_buffer.adaptive_last_timestamp_table,
            ),
        )
    } else {
        None
    };

    let segment_writer = match segment_writer_config.clone() {
        Some(cfg) => Some(SegmentWriter::new(cfg)?),
        None => None,
    };

    let mut processor = OtlpLabelSetProcessor::new(
        ingestion_config.labelset_store,
        ingestion_config.labelset_report_interval,
        head_config,
        segment_writer,
    )
    .with_event_time_policy(EventTimePolicy::new(
        ingestion_config.max_event_age,
        ingestion_config.max_event_lead,
        ingestion_config.drop_outdated,
    ));

    let live_handle = if config.api.enabled {
        segment_writer_config.as_ref().ok_or_else(|| {
            ChronoxideError::new(ErrorKind::ConfigError(
                "api.enabled=true requires an enabled segment writer".to_string(),
            ))
        })?;
        let memory_admission_bytes = config.api.live_memory_admission_bytes.ok_or_else(|| {
            ChronoxideError::new(ErrorKind::ConfigError(
                "api.live_memory_admission_bytes is required when api.enabled=true".to_string(),
            ))
        })?;
        let max_view_staleness = Duration::from_millis(
            config
                .api
                .resolved_max_view_staleness_ms()
                .map_err(ErrorKind::ConfigError)?,
        );
        Some(processor.enable_live_publication(LivePublisherConfig {
            publish_interval: Duration::from_millis(config.api.head_publish_interval_ms),
            max_view_staleness,
            memory_admission_bytes,
        })?)
    } else {
        None
    };

    // Binding is deliberately completed before source construction or
    // ingestion starts. A configured address conflict therefore fails before
    // Chronoxide accepts a Kafka message or opens a replay source.
    let api_server = if let Some(handle) = live_handle {
        let listen = config.api.listen.parse::<SocketAddr>().map_err(|error| {
            ChronoxideError::new(ErrorKind::ConfigError(format!(
                "api.listen is not a socket address: {error}"
            )))
        })?;
        let app = live_router(handle, config.api.to_api_config())?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        let bound_address = listener.local_addr()?;
        info!(
            listen = %bound_address,
            "Embedded Chronoxide Prometheus API bound"
        );
        Some(EmbeddedApiServer::start(listener, app))
    } else {
        None
    };

    let start_result = match api_server {
        Some(api_server) => {
            // Kafka polling, capture/replay I/O, decoding, publication, and
            // sealing are synchronous. Keep them off Tokio's async workers
            // while the embedded HTTP server is active.
            let ingestion_task = spawn_ingestion(
                replay_from,
                capture_to,
                kafka_consumer_config,
                ingestion_config,
                processor,
                meter,
                ct.clone(),
            );
            await_ingestion_and_api(ingestion_task, api_server, ct.clone()).await
        }
        None => {
            // Preserve the existing disabled-mode execution path, including
            // its thread/allocator behavior and concrete source dispatch.
            run_ingestion(
                replay_from,
                capture_to,
                kafka_consumer_config,
                ingestion_config,
                processor,
                meter,
                ct.clone(),
            )
        }
    };

    if let Err(err) = &start_result {
        error!("Ingester exited with error: {}", err);
    }

    // Every branch-local Ingester (and therefore its source and processor)
    // has left scope before this diagnostic checkpoint. The zero-default path
    // returns without a file operation, clock read, or sleep.
    if allocator_policy.post_drop_hold_secs() > 0 {
        info!(
            post_ingester_drop_hold_secs = allocator_policy.post_drop_hold_secs(),
            "Ingester state dropped; beginning diagnostic allocator release hold"
        );
    }
    allocator_policy
        .hold_after_ingester_drop()
        .map_err(|error| ChronoxideError::new(ErrorKind::ConfigError(error)))?;
    if allocator_policy.post_drop_hold_secs() > 0 {
        info!("Diagnostic allocator release hold complete");
    }

    info!("Notifying all tasks waiting for shutdown..");
    shutdown.notify_waiters();
    shutdown_task.abort();
    if let Err(err) = shutdown_task.await
        && !err.is_cancelled()
    {
        warn!("Shutdown task exited with error: {}", err);
    }

    logger_provider.inspect(|logger_provider| {
        info!("OTLP log provider shutdown: {:?}", logger_provider);
    });

    info!("Shutting down OTLP log meter provider");
    let _ = meter_provider.shutdown().inspect_err(|err| {
        error!("Failed to shutdown OTLP meter provider: {:?}", err);
    });

    info!("Shutdown complete");
    start_result
}

fn spawn_ingestion(
    replay_from: Option<PathBuf>,
    capture_to: Option<PathBuf>,
    kafka_config: KafkaConsumerConfig,
    ingestion_config: IngestionConfig,
    processor: OtlpLabelSetProcessor,
    meter: opentelemetry::metrics::Meter,
    ct: CancellationToken,
) -> JoinHandle<Result<(), ChronoxideError>> {
    tokio::task::spawn_blocking(move || {
        run_ingestion(
            replay_from,
            capture_to,
            kafka_config,
            ingestion_config,
            processor,
            meter,
            ct,
        )
    })
}

fn run_ingestion(
    replay_from: Option<PathBuf>,
    capture_to: Option<PathBuf>,
    kafka_config: KafkaConsumerConfig,
    ingestion_config: IngestionConfig,
    processor: OtlpLabelSetProcessor,
    meter: opentelemetry::metrics::Meter,
    ct: CancellationToken,
) -> Result<(), ChronoxideError> {
    match replay_from {
        None => {
            let source = KafkaSource::new(kafka_config.clone(), ct.clone())?;
            match capture_to {
                Some(path) => {
                    let writer = OtlpCaptureWriter::create(
                        path,
                        kafka_config.topic.clone(),
                        CompressionMethod::Zstd,
                    )?;
                    let source = CapturingSource::new(source, writer);
                    let mut ingester =
                        Ingester::new(source, ingestion_config, processor, meter, ct)?;
                    ingester.start()
                }
                None => {
                    let mut ingester =
                        Ingester::new(source, ingestion_config, processor, meter, ct)?;
                    ingester.start()
                }
            }
        }
        Some(path) => {
            if capture_to.is_some() {
                warn!("capture_to ignored in replay mode");
            }
            let source = FileSource::new(path)?;
            let mut ingester = Ingester::new(source, ingestion_config, processor, meter, ct)?;
            ingester.start()
        }
    }
}

async fn await_ingestion_and_api(
    mut ingestion_task: JoinHandle<Result<(), ChronoxideError>>,
    api_server: EmbeddedApiServer,
    ct: CancellationToken,
) -> Result<(), ChronoxideError> {
    let EmbeddedApiServer { shutdown, mut task } = api_server;

    tokio::select! {
        ingestion_join = &mut ingestion_task => {
            let ingestion_result = flatten_ingestion_join(ingestion_join);
            // Processor shutdown and its final publication have completed
            // before admission is closed.
            shutdown.cancel();
            let api_result = flatten_api_join(task.await);
            combine_ingestion_and_api(ingestion_result, api_result)
        }
        api_join = &mut task => {
            let api_result = match flatten_api_join(api_join) {
                Ok(()) => Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "embedded API server stopped before ingestion",
                )),
                Err(error) => Err(error),
            };
            // An accept-loop failure is terminal. Stop source admission and
            // still let the processor perform its normal safe shutdown.
            ct.cancel();
            let ingestion_result = flatten_ingestion_join(ingestion_task.await);
            combine_ingestion_and_api(ingestion_result, api_result)
        }
    }
}

fn flatten_ingestion_join(
    joined: Result<Result<(), ChronoxideError>, JoinError>,
) -> Result<(), ChronoxideError> {
    joined.map_err(|error| {
        ChronoxideError::from(io::Error::other(format!(
            "blocking ingestion task failed: {error}"
        )))
    })?
}

fn flatten_api_join(joined: Result<io::Result<()>, JoinError>) -> io::Result<()> {
    joined.map_err(|error| io::Error::other(format!("embedded API task failed: {error}")))?
}

fn combine_ingestion_and_api(
    ingestion_result: Result<(), ChronoxideError>,
    api_result: io::Result<()>,
) -> Result<(), ChronoxideError> {
    match ingestion_result {
        Err(error) => {
            if let Err(api_error) = api_result {
                warn!(
                    "Embedded API also failed while ingestion was shutting down: {}",
                    api_error
                );
            }
            Err(error)
        }
        Ok(()) => api_result.map_err(ChronoxideError::from),
    }
}

fn env_has_value(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn otlp_logs_enabled() -> bool {
    env_has_value("OTEL_EXPORTER_OTLP_ENDPOINT")
        || env_has_value("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
}

fn otlp_metrics_enabled() -> bool {
    env_has_value("OTEL_EXPORTER_OTLP_ENDPOINT")
        || env_has_value("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get};
    use chronoxide_core::storage::live_memory::LiveMemoryGovernor;
    use chronoxide_core::storage::live_view::{LiveQueryHandle, LiveStorageView};

    #[tokio::test]
    async fn embedded_live_server_binds_before_any_view_is_published() {
        let handle = LiveQueryHandle::<LiveStorageView>::new(Duration::from_secs(1)).unwrap();
        handle
            .configure_query_admission(LiveMemoryGovernor::new(1).unwrap(), 1)
            .unwrap();
        let app = live_router(handle, chronoxide_api::ApiConfig::default()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = EmbeddedApiServer::start(listener, app);
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let health = client
            .get(format!("http://{address}/-/healthy"))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), reqwest::StatusCode::OK);

        let readiness = client
            .get(format!("http://{address}/-/ready"))
            .send()
            .await
            .unwrap();
        assert_eq!(readiness.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_waits_for_an_admitted_request() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_for_route = Arc::clone(&entered);
        let release_for_route = Arc::clone(&release);
        let app = Router::new().route(
            "/hold",
            get(move || {
                let entered = Arc::clone(&entered_for_route);
                let release = Arc::clone(&release_for_route);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    "done"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = EmbeddedApiServer::start(listener, app);
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let entered_request = entered.notified();
        let request =
            tokio::spawn(async move { client.get(format!("http://{address}/hold")).send().await });

        entered_request.await;
        let shutdown = tokio::spawn(server.shutdown());
        tokio::task::yield_now().await;
        assert!(
            !shutdown.is_finished(),
            "graceful shutdown must retain an admitted request"
        );

        release.notify_one();
        assert_eq!(
            request.await.unwrap().unwrap().status(),
            reqwest::StatusCode::OK
        );
        shutdown.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn api_admission_closes_only_after_blocking_ingestion_finishes() {
        let app = Router::new().route("/health", get(|| async { "up" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = EmbeddedApiServer::start(listener, app);
        let (ingestion_started_tx, ingestion_started_rx) = tokio::sync::oneshot::channel();
        let (finish_ingestion_tx, finish_ingestion_rx) = std::sync::mpsc::channel();
        let ingestion = tokio::task::spawn_blocking(move || {
            ingestion_started_tx.send(()).unwrap();
            finish_ingestion_rx.recv().unwrap();
            Ok::<(), ChronoxideError>(())
        });
        ingestion_started_rx.await.unwrap();
        let lifecycle = tokio::spawn(await_ingestion_and_api(
            ingestion,
            server,
            CancellationToken::new(),
        ));
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        let health = client
            .get(format!("http://{address}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), reqwest::StatusCode::OK);

        finish_ingestion_tx.send(()).unwrap();
        lifecycle.await.unwrap().unwrap();
        assert!(
            client
                .get(format!("http://{address}/health"))
                .send()
                .await
                .is_err(),
            "the listener must be closed after ingestion and graceful shutdown"
        );
    }
}
