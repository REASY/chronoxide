use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentId {
    start_ms: u64,
    end_ms: u64,
    ulid: Ulid,
}

impl SegmentId {
    pub fn new(start_ms: u64, end_ms: u64) -> Result<Self, SegmentIdError> {
        Self::with_ulid(start_ms, end_ms, Ulid::new())
    }

    pub fn with_ulid(start_ms: u64, end_ms: u64, ulid: Ulid) -> Result<Self, SegmentIdError> {
        if start_ms >= end_ms {
            return Err(SegmentIdError::InvalidRange { start_ms, end_ms });
        }
        Ok(Self {
            start_ms,
            end_ms,
            ulid,
        })
    }

    pub fn start_ms(&self) -> u64 {
        self.start_ms
    }

    pub fn end_ms(&self) -> u64 {
        self.end_ms
    }

    pub fn ulid(&self) -> Ulid {
        self.ulid
    }

    pub fn dir_name(&self) -> String {
        format!("seg-{}-{}-{}", self.start_ms, self.end_ms, self.ulid)
    }

    pub fn parse_dir_name(name: &str) -> Result<Self, SegmentIdError> {
        let stripped = name
            .strip_prefix("seg-")
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;
        let mut parts = stripped.splitn(3, '-');
        let start = parts
            .next()
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;
        let end = parts
            .next()
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;
        let ulid_str = parts
            .next()
            .ok_or_else(|| SegmentIdError::InvalidFormat(name.to_string()))?;

        let start_ms = start
            .parse::<u64>()
            .map_err(|_| SegmentIdError::InvalidNumber(start.to_string()))?;
        let end_ms = end
            .parse::<u64>()
            .map_err(|_| SegmentIdError::InvalidNumber(end.to_string()))?;
        let ulid = ulid_str
            .parse::<Ulid>()
            .map_err(|_| SegmentIdError::InvalidUlid(ulid_str.to_string()))?;

        Self::with_ulid(start_ms, end_ms, ulid)
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.dir_name())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SegmentIdError {
    #[error("segment range invalid: start_ms={start_ms} end_ms={end_ms}")]
    InvalidRange { start_ms: u64, end_ms: u64 },
    #[error("segment dir format invalid: {0}")]
    InvalidFormat(String),
    #[error("segment dir number invalid: {0}")]
    InvalidNumber(String),
    #[error("segment ulid invalid: {0}")]
    InvalidUlid(String),
}

pub trait SegmentIdProvider: fmt::Debug + Send + Sync {
    fn next_segment_id(&self, start_ms: u64, end_ms: u64) -> Result<SegmentId, SegmentIdError>;
}

#[derive(Debug, Default)]
pub struct RandomSegmentIdProvider;

impl SegmentIdProvider for RandomSegmentIdProvider {
    fn next_segment_id(&self, start_ms: u64, end_ms: u64) -> Result<SegmentId, SegmentIdError> {
        SegmentId::new(start_ms, end_ms)
    }
}

#[derive(Debug)]
pub struct DeterministicSegmentIdProvider {
    seed: u64,
    next_ordinal: AtomicU64,
}

impl DeterministicSegmentIdProvider {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            next_ordinal: AtomicU64::new(0),
        }
    }
}

impl SegmentIdProvider for DeterministicSegmentIdProvider {
    fn next_segment_id(&self, start_ms: u64, end_ms: u64) -> Result<SegmentId, SegmentIdError> {
        let ordinal = self.next_ordinal.fetch_add(1, Ordering::Relaxed);
        SegmentId::with_ulid(
            start_ms,
            end_ms,
            deterministic_segment_ulid(self.seed, start_ms, end_ms, ordinal),
        )
    }
}
