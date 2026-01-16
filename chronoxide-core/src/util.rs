use crate::error::ChronoxideError;
use serde::Deserialize;
use std::io::Read;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn sleep_for(duration: Duration, cancellation_token: &CancellationToken) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = cancellation_token.cancelled() => {
            info!("Cancelled, exiting...");
            return;
        }
    }
}

pub fn load_config<T>(path: &str) -> Result<T, ChronoxideError>
where
    T: for<'de> Deserialize<'de>,
{
    info!("Loading config from {}", path);
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
    use std::path::PathBuf;
    use std::time::SystemTime;

    fn temp_path(stem: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!(
            "chronoxide_core_test_{}_{}_{}.toml",
            stem,
            std::process::id(),
            nanos
        ));
        path
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct TestConfig {
        answer: u32,
    }

    #[test]
    fn load_config_reads_toml() {
        let path = temp_path("load_config");
        std::fs::write(&path, "answer = 42\n").unwrap();

        let cfg: TestConfig = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg, TestConfig { answer: 42 });

        let _ = std::fs::remove_file(path);
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
