mod app_config;
mod ingester;
mod processor;
mod source;
mod statistics;

use crate::app_config::AppConfig;
use crate::ingester::{Ingester, IngestionConfig};
use crate::processor::{EventTimePolicy, OtlpLabelSetProcessor};
use crate::source::{CapturingSource, FileSource, KafkaSource};
use chronoxide_core::error::{ChronoxideError, ErrorKind};
use chronoxide_core::otlp_capture::{CompressionMethod, OtlpCaptureWriter};
use chronoxide_core::storage::head::HeadConfig;
use chronoxide_core::storage::segment::SegmentWriter;
use chronoxide_core::telemetry::{init_meter_provider, init_otlp_logging, setup_local_logging};
use chronoxide_core::util::load_config;
use opentelemetry::global;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), ChronoxideError> {
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
            )),
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
            )),
        )
    } else {
        None
    };

    let segment_writer = match segment_writer_config.clone() {
        Some(cfg) => Some(SegmentWriter::new(cfg)?),
        None => None,
    };

    let processor = OtlpLabelSetProcessor::new(
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

    let start_result = match replay_from {
        None => {
            let source = KafkaSource::new(kafka_consumer_config.clone(), ct.clone())?;
            match capture_to {
                Some(path) => {
                    let writer = OtlpCaptureWriter::create(
                        path,
                        kafka_consumer_config.topic.clone(),
                        CompressionMethod::Zstd,
                    )?;
                    let source = CapturingSource::new(source, writer);
                    let mut ingester = Ingester::new(
                        source,
                        ingestion_config.clone(),
                        processor,
                        meter.clone(),
                        ct.clone(),
                    )?;
                    ingester.start()
                }
                None => {
                    let mut ingester = Ingester::new(
                        source,
                        ingestion_config.clone(),
                        processor,
                        meter.clone(),
                        ct.clone(),
                    )?;
                    ingester.start()
                }
            }
        }
        Some(path) => {
            let source = FileSource::new(path)?;
            if capture_to.is_some() {
                warn!("capture_to ignored in replay mode");
            }
            let mut ingester = Ingester::new(
                source,
                ingestion_config.clone(),
                processor,
                meter.clone(),
                ct.clone(),
            )?;
            ingester.start()
        }
    };

    if let Err(err) = &start_result {
        error!("Ingester exited with error: {}", err);
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
