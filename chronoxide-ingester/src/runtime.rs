use crate::error::ChronoxideError;
use serde::Deserialize;
use std::io::Read;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub fn get_env_default(name: &str) -> Option<String> {
    std::env::var(name).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub async fn sleep_for(duration: Duration, cancellation_token: &CancellationToken) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = cancellation_token.cancelled() => {
            info!(target: "chronoxide_core::util", "Cancelled, exiting...");
        }
    }
}

pub fn load_config<T>(path: &str) -> Result<T, ChronoxideError>
where
    T: for<'de> Deserialize<'de>,
{
    info!(target: "chronoxide_core::util", "Loading config from {}", path);
    let mut f = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let cfg = toml::from_slice::<T>(&buf)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct TestConfig {
        answer: u32,
    }

    #[test]
    fn load_config_reads_toml() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "answer = 42\n").unwrap();

        let cfg: TestConfig = load_config(file.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg, TestConfig { answer: 42 });
    }

    #[test]
    fn test_get_env_default() {
        let _guard = ENV_LOCK.lock().unwrap();

        let key = "CHRONOXIDE_TEST_ENV_VAR";
        unsafe {
            std::env::set_var(key, "  value  ");
        }
        assert_eq!(get_env_default(key), Some("value".to_string()));

        unsafe {
            std::env::set_var(key, "   ");
        }
        assert_eq!(get_env_default(key), None);

        unsafe {
            std::env::remove_var(key);
        }
        assert_eq!(get_env_default(key), None);
    }

    #[tokio::test]
    async fn sleep_for_returns_when_cancelled() {
        let ct = CancellationToken::new();
        ct.cancel();

        tokio::time::timeout(
            Duration::from_millis(200),
            sleep_for(Duration::from_secs(60), &ct),
        )
        .await
        .unwrap();
    }
}
