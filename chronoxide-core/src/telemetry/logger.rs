use crate::error::{ChronoxideError, ErrorKind};
use crate::telemetry::TelemetryResult;
use opentelemetry_appender_tracing::layer;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use std::env;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::{EnvFilter, layer::Layer, prelude::*};

fn set_default_rust_log(default_level: LevelFilter, crate_filters: &[&str]) {
    if env::var_os("RUST_LOG").is_none() {
        let mut entries: Vec<String> = crate_filters
            .iter()
            .map(|name| format!("{name}={default_level}"))
            .collect();
        entries.push("tower_http=WARN".to_string());
        entries.push("hyper=WARN".to_string());
        unsafe {
            env::set_var("RUST_LOG", entries.join(","));
        }
    }
}

pub fn setup_logging(
    default_level: LevelFilter,
    provider: &SdkLoggerProvider,
    crate_filters: &[&str],
) -> TelemetryResult<()> {
    set_default_rust_log(default_level, crate_filters);
    init_logger(provider, default_level)
}

pub fn setup_local_logging(
    default_level: LevelFilter,
    crate_filters: &[&str],
) -> TelemetryResult<()> {
    set_default_rust_log(default_level, crate_filters);
    init_local_logger(default_level)
}

pub fn init_logger(
    provider: &SdkLoggerProvider,
    default_level: LevelFilter,
) -> TelemetryResult<()> {
    let filter_otel = env_filter_from_env(
        default_level,
        &["hyper=off", "tonic=off", "h2=off", "reqwest=off"],
    )?;
    let otel_layer = layer::OpenTelemetryTracingBridge::new(provider).with_filter(filter_otel);

    let filter_fmt = env_filter_from_env(default_level, &["opentelemetry=info"])?;
    let format = LogFormatOptions::from_env();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(format.ansi)
        .with_file(format.include_source)
        .with_line_number(format.include_source)
        .with_thread_ids(format.include_threads)
        .with_thread_names(format.include_threads)
        .with_filter(filter_fmt);

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer)
        .try_init()
        .map_err(|err| ChronoxideError::new(ErrorKind::TracingSubscriberError(err.to_string())))?;

    info!("Logger initialized");
    Ok(())
}

fn init_local_logger(default_level: LevelFilter) -> TelemetryResult<()> {
    let filter_fmt = env_filter_from_env(default_level, &["opentelemetry=info"])?;
    let format = LogFormatOptions::from_env();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(format.ansi)
        .with_file(format.include_source)
        .with_line_number(format.include_source)
        .with_thread_ids(format.include_threads)
        .with_thread_names(format.include_threads)
        .with_filter(filter_fmt);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .try_init()
        .map_err(|err| ChronoxideError::new(ErrorKind::TracingSubscriberError(err.to_string())))?;

    info!("Logger initialized");
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LogFormatOptions {
    ansi: bool,
    include_source: bool,
    include_threads: bool,
}

impl LogFormatOptions {
    fn from_env() -> Self {
        let format = env::var("CHRONOXIDE_LOG_FORMAT").ok();
        let ansi = env::var("CHRONOXIDE_LOG_ANSI").ok();
        Self::from_env_values(format.as_deref(), ansi.as_deref())
    }

    fn from_env_values(format: Option<&str>, ansi: Option<&str>) -> Self {
        let debug = format.is_some_and(|value| {
            value.eq_ignore_ascii_case("debug")
                || value.eq_ignore_ascii_case("full")
                || value.eq_ignore_ascii_case("verbose")
        });
        Self {
            ansi: ansi.and_then(parse_bool).unwrap_or(false),
            include_source: debug,
            include_threads: debug,
        }
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn env_filter_from_env(
    default_level: LevelFilter,
    extra_directives: &[&str],
) -> TelemetryResult<EnvFilter> {
    let rust_log = env::var("RUST_LOG").ok();
    env_filter_from_spec(default_level, rust_log.as_deref(), extra_directives)
}

fn env_filter_from_spec(
    default_level: LevelFilter,
    rust_log: Option<&str>,
    extra_directives: &[&str],
) -> TelemetryResult<EnvFilter> {
    let spec = rust_log
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| default_level.to_string());
    let mut filter = EnvFilter::try_new(spec)
        .map_err(|err| ChronoxideError::new(ErrorKind::TracingSubscriberError(err.to_string())))?;
    for directive in extra_directives {
        filter = filter.add_directive(parse_directive(directive)?);
    }
    Ok(filter)
}

fn parse_directive(spec: &str) -> TelemetryResult<Directive> {
    spec.parse::<Directive>()
        .map_err(|err| ChronoxideError::new(ErrorKind::TracingSubscriberError(err.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;

    #[test]
    fn default_log_format_is_compact_and_file_safe() {
        let options = LogFormatOptions::from_env_values(None, None);

        assert_eq!(
            options,
            LogFormatOptions {
                ansi: false,
                include_source: false,
                include_threads: false,
            }
        );
    }

    #[test]
    fn debug_log_format_includes_source_and_threads() {
        let options = LogFormatOptions::from_env_values(Some("debug"), None);

        assert_eq!(
            options,
            LogFormatOptions {
                ansi: false,
                include_source: true,
                include_threads: true,
            }
        );
    }

    #[test]
    fn log_format_ansi_can_be_enabled_explicitly() {
        let options = LogFormatOptions::from_env_values(Some("compact"), Some("true"));

        assert_eq!(
            options,
            LogFormatOptions {
                ansi: true,
                include_source: false,
                include_threads: false,
            }
        );
    }

    #[test]
    fn env_filter_honors_rust_log_module_overrides() {
        let filter = env_filter_from_spec(
            LevelFilter::INFO,
            Some("chronoxide_core=warn,chronoxide_core::storage::segment=info"),
            &[],
        )
        .unwrap();
        let subscriber = tracing_subscriber::registry().with(filter);
        let dispatch = tracing::Dispatch::new(subscriber);

        tracing::dispatcher::with_default(&dispatch, || {
            assert!(!tracing::enabled!(
                target: "chronoxide_core::storage::head",
                Level::INFO
            ));
            assert!(tracing::enabled!(
                target: "chronoxide_core::storage::head",
                Level::WARN
            ));
            assert!(tracing::enabled!(
                target: "chronoxide_core::storage::segment",
                Level::INFO
            ));
        });
    }
}
