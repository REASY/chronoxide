use opentelemetry_otlp::ExporterBuildError;
use std::collections::HashMap;
use strum_macros::AsRefStr;
use thiserror::Error;
use tracing::Level;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct ChronoxideError(Box<ErrorKind>);

impl ChronoxideError {
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

    pub fn new(kind: ErrorKind) -> Self {
        ChronoxideError(Box::new(kind))
    }
}

#[derive(Error, Debug, AsRefStr)]
#[error(transparent)]
pub enum ErrorKind {
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("IoError: {0}")]
    IoError(#[from] std::io::Error),

    #[error("KafkaError: {0}")]
    KafkaError(#[from] rdkafka::error::KafkaError),

    #[error("ChannelError: {0}")]
    ChannelError(String),

    #[error("SystemTimeError: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("TomlError: {0}")]
    TomlError(#[from] toml::de::Error),

    #[error("OtlpError: {0}")]
    OtlpError(#[from] ExporterBuildError),

    #[error("TracingSubscriberError: {0}")]
    TracingSubscriberError(String),

    #[error("ConfigError: {0}")]
    ConfigError(String),

    #[error("ProtobufDecodeError: {0}")]
    ProtobufDecodeError(#[from] prost::DecodeError),
}

impl<E> From<E> for ChronoxideError
where
    ErrorKind: From<E>,
{
    fn from(err: E) -> Self {
        ChronoxideError(Box::new(ErrorKind::from(err)))
    }
}

static LOG_RATE_LIMITER: std::sync::OnceLock<LogRateLimiter> = std::sync::OnceLock::new();

struct LogRateLimiter {
    rate_limiter_per_level: HashMap<Level, dashmap::DashMap<String, (u64, std::time::Instant)>>,
    max_messages_per_second_per_level: HashMap<Level, u64>,
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ErrorKind
where
    T: std::fmt::Debug,
{
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ErrorKind::ChannelError(err.to_string())
    }
}

impl LogRateLimiter {
    fn new() -> Self {
        let max_messages_per_second_per_level = HashMap::from([
            (Level::ERROR, 10),
            (Level::WARN, 10),
            (Level::INFO, 10),
            (Level::DEBUG, 10),
            (Level::TRACE, 10),
        ]);
        Self {
            rate_limiter_per_level: HashMap::from([
                (Level::ERROR, dashmap::DashMap::new()),
                (Level::WARN, dashmap::DashMap::new()),
                (Level::INFO, dashmap::DashMap::new()),
                (Level::DEBUG, dashmap::DashMap::new()),
                (Level::TRACE, dashmap::DashMap::new()),
            ]),
            max_messages_per_second_per_level,
        }
    }

    fn should_log(&self, log_level: Level, err: &str, now: std::time::Instant) -> bool {
        let rate_limiter = self
            .rate_limiter_per_level
            .get(&log_level)
            .unwrap_or_else(|| panic!("Rate limiter for log level {log_level} not found"));
        let limit = *self
            .max_messages_per_second_per_level
            .get(&log_level)
            .unwrap_or_else(|| panic!("The limit for log level {log_level} not found"));
        let current = rate_limiter
            .entry(err.to_string())
            .and_modify(|entry| {
                if now.duration_since(entry.1).as_secs() >= 1 {
                    entry.0 = 1;
                    entry.1 = now;
                } else {
                    entry.0 += 1;
                }
            })
            .or_insert((1, now));

        current.0 <= limit
    }
}

pub fn should_log(log_level: Level, err: &str, now: std::time::Instant) -> bool {
    let rate_limiter = LOG_RATE_LIMITER.get_or_init(LogRateLimiter::new);
    rate_limiter.should_log(log_level, err, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_error() -> ChronoxideError {
        ChronoxideError(Box::new(ErrorKind::ChannelError("test error".to_string())))
    }

    #[test]
    fn test_log_rate_limiter_initialization() {
        let rate_limiter = LogRateLimiter::new();
        assert_eq!(rate_limiter.max_messages_per_second_per_level.len(), 5);
        assert_eq!(
            rate_limiter.max_messages_per_second_per_level[&Level::ERROR],
            10
        );
    }

    #[test]
    fn test_should_log_within_limits() {
        let rate_limiter = LogRateLimiter::new();
        let now = std::time::Instant::now();
        let err = create_test_error().kind().to_string();

        for _ in 0..10 {
            assert!(rate_limiter.should_log(Level::ERROR, &err, now));
        }
    }

    #[test]
    fn test_should_not_log_exceeding_limits() {
        let rate_limiter = LogRateLimiter::new();
        let now = std::time::Instant::now();
        let err = create_test_error().kind().to_string();

        for _ in 0..10 {
            assert!(rate_limiter.should_log(Level::ERROR, &err, now));
        }
        assert!(!rate_limiter.should_log(Level::ERROR, &err, now));
    }

    #[test]
    fn test_rate_limit_reset_after_one_second() {
        let rate_limiter = LogRateLimiter::new();
        let now = std::time::Instant::now();
        let err = create_test_error().kind().to_string();

        for _ in 0..10 {
            assert!(rate_limiter.should_log(Level::ERROR, &err, now));
        }
        assert!(!rate_limiter.should_log(Level::ERROR, &err, now));

        let one_second_later = now + Duration::from_secs(1);
        assert!(rate_limiter.should_log(Level::ERROR, &err, one_second_later));
    }

    #[test]
    fn test_multiple_error_types_rate_limiting() {
        let rate_limiter = LogRateLimiter::new();
        let now = std::time::Instant::now();
        let error1 = create_test_error().kind().to_string();
        let error2 = ChronoxideError(Box::new(ErrorKind::IoError(std::io::Error::other(
            "test error",
        ))))
        .kind()
        .to_string();

        for _ in 0..10 {
            assert!(rate_limiter.should_log(Level::ERROR, &error1, now));
            assert!(rate_limiter.should_log(Level::ERROR, &error2, now));
        }

        assert!(!rate_limiter.should_log(Level::ERROR, &error1, now));
        assert!(!rate_limiter.should_log(Level::ERROR, &error2, now));
    }
}
