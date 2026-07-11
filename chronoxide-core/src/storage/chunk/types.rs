use super::*;

pub(crate) const FRAME_HEADER_LEN: usize = 14;
pub(crate) const CHUNK_HEADER_LEN: usize = 40;
pub(super) const CHUNK_ENTRY_LEN: usize = 40;
pub(super) const CHUNK_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"CHIX");
pub(super) const CHUNK_INDEX_HEADER_LEN: u64 = 12;
pub(super) const CHUNK_WRITE_BUFFER_BYTES: usize = 1024 * 1024;
pub(super) const TYPED_SCALAR_LANE_MAGIC: u32 = u32::from_le_bytes(*b"TSCL");
pub(super) const TYPED_SCALAR_LANE_VERSION: u16 = 1;
pub(super) const TYPED_SCALAR_LANE_HEADER_LEN: usize = 16;

pub const CHUNK_FLAG_HAS_START_TIME: u16 = 1 << 1;
pub const CHUNK_FLAG_HAS_PER_SAMPLE_FLAGS: u16 = 1 << 2;
pub const CHUNK_FLAG_HAS_COUNTER_RESET_HINTS: u16 = 1 << 3;
pub const CHUNK_FLAG_TEMPORALITY_DELTA: u16 = 1 << 4;

pub(super) fn typed_chunk_flags(metadata: impl IntoIterator<Item = TypedSampleMetadata>) -> u16 {
    let mut flags = 0u16;
    let mut saw_any = false;
    let mut all_delta = true;
    for metadata in metadata {
        saw_any = true;
        if metadata.start_time_ms.is_some() {
            flags |= CHUNK_FLAG_HAS_START_TIME;
        }
        if metadata.flags != 0 {
            flags |= CHUNK_FLAG_HAS_PER_SAMPLE_FLAGS;
        }
        if metadata.reset_hint != CounterResetHint::Unknown {
            flags |= CHUNK_FLAG_HAS_COUNTER_RESET_HINTS;
        }
        if metadata.temporality != OtlpAggregationTemporality::Delta {
            all_delta = false;
        }
    }
    if saw_any && all_delta {
        flags |= CHUNK_FLAG_TEMPORALITY_DELTA;
    }
    flags
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChunkKind {
    Float = 0,
    Int64 = 1,
    Histogram = 2,
    ExponentialHistogram = 3,
    Summary = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkScalarRecordHeader {
    pub series_ref: u32,
    pub kind: ChunkKind,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub sample_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkEncoding {
    SchemaVarLen = 0,
    RawF64 = 1,
    RawI64 = 2,
    Gorilla = 3,
    IntDeltaZigZag = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkIndexEntry {
    pub file_id: u8,
    pub kind: ChunkKind,
    pub flags: u16,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub offset: u64,
    pub length: u32,
    pub scalar_lane_offset: u32,
    pub scalar_lane_len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ChunkIndexRange {
    pub offset: u64,
    pub len: u32,
}

impl ChunkIndexEntry {
    pub fn scalar_projection_read_len(&self) -> u32 {
        if self.scalar_lane_offset == 0 || self.scalar_lane_len == 0 {
            return self.length;
        }
        (CHUNK_HEADER_LEN as u32).saturating_add(self.scalar_lane_len)
    }
}
