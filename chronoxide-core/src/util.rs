use crate::error::ChronoxideError;
use serde::Deserialize;
use std::io::Read;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub(crate) fn xxhash64(input: &[u8]) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;
    const P3: u64 = 1_609_587_929_392_839_161;
    const P4: u64 = 9_650_029_242_287_828_579;
    const P5: u64 = 2_870_177_450_012_600_261;

    let mut cursor = 0usize;
    let mut hash;

    if input.len() >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0;
        let mut v4 = 0u64.wrapping_sub(P1);

        while cursor + 32 <= input.len() {
            v1 = xxhash64_round(v1, read_u64(input, cursor));
            cursor += 8;
            v2 = xxhash64_round(v2, read_u64(input, cursor));
            cursor += 8;
            v3 = xxhash64_round(v3, read_u64(input, cursor));
            cursor += 8;
            v4 = xxhash64_round(v4, read_u64(input, cursor));
            cursor += 8;
        }

        hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        hash = xxhash64_merge_round(hash, v1);
        hash = xxhash64_merge_round(hash, v2);
        hash = xxhash64_merge_round(hash, v3);
        hash = xxhash64_merge_round(hash, v4);
    } else {
        hash = P5;
    }

    hash = hash.wrapping_add(input.len() as u64);

    while cursor + 8 <= input.len() {
        let k1 = xxhash64_round(0, read_u64(input, cursor));
        hash ^= k1;
        hash = hash.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        cursor += 8;
    }

    if cursor + 4 <= input.len() {
        hash ^= u64::from(read_u32(input, cursor)).wrapping_mul(P1);
        hash = hash.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        cursor += 4;
    }

    while cursor < input.len() {
        hash ^= u64::from(input[cursor]).wrapping_mul(P5);
        hash = hash.rotate_left(11).wrapping_mul(P1);
        cursor += 1;
    }

    hash ^= hash >> 33;
    hash = hash.wrapping_mul(P2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(P3);
    hash ^ (hash >> 32)
}

fn xxhash64_round(accumulator: u64, input: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;

    accumulator
        .wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

fn xxhash64_merge_round(accumulator: u64, value: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P4: u64 = 9_650_029_242_287_828_579;

    (accumulator ^ xxhash64_round(0, value))
        .wrapping_mul(P1)
        .wrapping_add(P4)
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().unwrap())
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().unwrap())
}

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
            info!("Cancelled, exiting...");
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
    fn xxhash64_matches_seed_zero_reference_vectors() {
        assert_eq!(xxhash64(b""), 0xef46_db37_51d8_e999);
        assert_eq!(xxhash64(b"a"), 0xd24e_c4f1_a98c_6e5b);
        assert_eq!(xxhash64(b"hello"), 0x26c7_827d_889f_6da3);
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
