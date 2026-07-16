use crate::error::ChronoxideError;
use serde::Deserialize;
use std::io::Read;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

const XXHASH64_P1: u64 = 11_400_714_785_074_694_791;
const XXHASH64_P2: u64 = 14_029_467_366_897_019_727;
const XXHASH64_P3: u64 = 1_609_587_929_392_839_161;
const XXHASH64_P4: u64 = 9_650_029_242_287_828_579;
const XXHASH64_P5: u64 = 2_870_177_450_012_600_261;

#[derive(Debug, Clone)]
pub(crate) struct XxHash64 {
    total_len: u64,
    lanes: [u64; 4],
    buffered: [u8; 32],
    buffered_len: usize,
}

impl Default for XxHash64 {
    fn default() -> Self {
        Self {
            total_len: 0,
            lanes: [
                XXHASH64_P1.wrapping_add(XXHASH64_P2),
                XXHASH64_P2,
                0,
                0u64.wrapping_sub(XXHASH64_P1),
            ],
            buffered: [0; 32],
            buffered_len: 0,
        }
    }
}

impl XxHash64 {
    pub(crate) fn update(&mut self, mut input: &[u8]) {
        self.total_len = self.total_len.wrapping_add(input.len() as u64);

        if self.buffered_len != 0 {
            let missing = 32 - self.buffered_len;
            if input.len() < missing {
                self.buffered[self.buffered_len..self.buffered_len + input.len()]
                    .copy_from_slice(input);
                self.buffered_len += input.len();
                return;
            }
            self.buffered[self.buffered_len..].copy_from_slice(&input[..missing]);
            let block = self.buffered;
            self.consume_block(&block);
            self.buffered_len = 0;
            input = &input[missing..];
        }

        let mut chunks = input.chunks_exact(32);
        for block in &mut chunks {
            self.consume_block(block);
        }
        let remainder = chunks.remainder();
        self.buffered[..remainder.len()].copy_from_slice(remainder);
        self.buffered_len = remainder.len();
    }

    pub(crate) fn finish(&self) -> u64 {
        let mut hash = if self.total_len >= 32 {
            let mut hash = self.lanes[0]
                .rotate_left(1)
                .wrapping_add(self.lanes[1].rotate_left(7))
                .wrapping_add(self.lanes[2].rotate_left(12))
                .wrapping_add(self.lanes[3].rotate_left(18));
            for lane in self.lanes {
                hash = xxhash64_merge_round(hash, lane);
            }
            hash
        } else {
            XXHASH64_P5
        };
        hash = hash.wrapping_add(self.total_len);

        let input = &self.buffered[..self.buffered_len];
        let mut cursor = 0usize;
        while cursor + 8 <= input.len() {
            hash ^= xxhash64_round(0, read_u64(input, cursor));
            hash = hash
                .rotate_left(27)
                .wrapping_mul(XXHASH64_P1)
                .wrapping_add(XXHASH64_P4);
            cursor += 8;
        }
        if cursor + 4 <= input.len() {
            hash ^= u64::from(read_u32(input, cursor)).wrapping_mul(XXHASH64_P1);
            hash = hash
                .rotate_left(23)
                .wrapping_mul(XXHASH64_P2)
                .wrapping_add(XXHASH64_P3);
            cursor += 4;
        }
        while cursor < input.len() {
            hash ^= u64::from(input[cursor]).wrapping_mul(XXHASH64_P5);
            hash = hash.rotate_left(11).wrapping_mul(XXHASH64_P1);
            cursor += 1;
        }

        hash ^= hash >> 33;
        hash = hash.wrapping_mul(XXHASH64_P2);
        hash ^= hash >> 29;
        hash = hash.wrapping_mul(XXHASH64_P3);
        hash ^ (hash >> 32)
    }

    fn consume_block(&mut self, block: &[u8]) {
        debug_assert_eq!(block.len(), 32);
        for (index, lane) in self.lanes.iter_mut().enumerate() {
            *lane = xxhash64_round(*lane, read_u64(block, index * 8));
        }
    }
}

pub(crate) fn xxhash64(input: &[u8]) -> u64 {
    let mut hash = XxHash64::default();
    hash.update(input);
    hash.finish()
}

fn xxhash64_round(accumulator: u64, input: u64) -> u64 {
    accumulator
        .wrapping_add(input.wrapping_mul(XXHASH64_P2))
        .rotate_left(31)
        .wrapping_mul(XXHASH64_P1)
}

fn xxhash64_merge_round(accumulator: u64, value: u64) -> u64 {
    (accumulator ^ xxhash64_round(0, value))
        .wrapping_mul(XXHASH64_P1)
        .wrapping_add(XXHASH64_P4)
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
    fn streaming_xxhash64_is_independent_of_update_boundaries() {
        for len in 0..=300usize {
            let bytes: Vec<u8> = (0..len)
                .map(|index| (index as u8).wrapping_mul(37).wrapping_add(11))
                .collect();
            let expected = xxhash64(&bytes);
            for chunk_len in [1, 3, 7, 31, 32, 33, 64, 257] {
                let mut hash = XxHash64::default();
                for chunk in bytes.chunks(chunk_len) {
                    hash.update(chunk);
                }
                assert_eq!(
                    hash.finish(),
                    expected,
                    "length={len} chunk_len={chunk_len}"
                );
            }
        }
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
