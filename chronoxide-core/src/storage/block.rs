use std::io;
use std::marker::PhantomData;
use std::mem;

use crate::storage::arena::{BlockArena, BufferRef};
use crate::storage::encoding::chimp::{
    decode_chimp128_baseline_values, decode_chimp128_duckdb_values,
    encode_chimp128_baseline_values, encode_chimp128_duckdb_values,
};
use crate::storage::encoding::{
    AlpEncoder, AlpRdEncoder, AlpRdSpiralEncoder, AlpSpiralEncoder, ElfEncoder, GorillaEncoder,
    SchemaVarLenCodec, SchemaVarLenEncoding, VarLenCodec, VarLenEncoding,
    decode_alp_rd_spiral_values, decode_alp_rd_values, decode_alp_spiral_values, decode_alp_values,
    decode_elf_values, decode_gorilla_values, decode_varint, decode_zigzag_i64, encode_varint,
    encode_zigzag_i64, varint_len,
};

pub(crate) trait BlockCodec: Sized {
    type Value;

    fn new(first: Self::Value) -> io::Result<Self>;
    fn push(&mut self, value: Self::Value) -> io::Result<()>;
    fn reserve(&mut self, additional_samples: usize) {
        let _ = additional_samples;
    }
    fn encoded_len_bytes(&self) -> usize;
    fn snapshot_bytes(&self) -> Vec<u8>;
    fn into_bytes(self) -> Vec<u8>;
    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>>;
}

impl<T: VarLenEncoding> BlockCodec for VarLenCodec<T> {
    type Value = T;

    fn new(first: Self::Value) -> io::Result<Self> {
        VarLenCodec::new(first)
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        VarLenCodec::push(self, value)
    }

    fn encoded_len_bytes(&self) -> usize {
        VarLenCodec::encoded_len_bytes(self)
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        VarLenCodec::snapshot_bytes(self)
    }

    fn into_bytes(self) -> Vec<u8> {
        VarLenCodec::into_bytes(self)
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        VarLenCodec::decode_values(buf, count)
    }
}

impl<T: SchemaVarLenEncoding> BlockCodec for SchemaVarLenCodec<T> {
    type Value = T;

    fn new(first: Self::Value) -> io::Result<Self> {
        SchemaVarLenCodec::new(first)
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        SchemaVarLenCodec::push(self, value)
    }

    fn encoded_len_bytes(&self) -> usize {
        SchemaVarLenCodec::encoded_len_bytes(self)
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        SchemaVarLenCodec::snapshot_bytes(self)
    }

    fn into_bytes(self) -> Vec<u8> {
        SchemaVarLenCodec::into_bytes(self)
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        SchemaVarLenCodec::decode_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct BlockBuilder<C: BlockCodec> {
    base_ms: u64,
    min_ts: u64,
    max_ts: u64,
    timestamps: Vec<u8>,
    values: C,
    samples: u32,
    reserve_full_done: bool,
}

impl<C: BlockCodec> BlockBuilder<C> {
    const INITIAL_RESERVE_SAMPLES: usize = 8;
    const FULL_RESERVE_THRESHOLD: usize = 64;

    pub(crate) fn new(
        base_ms: u64,
        timestamp_ms: u64,
        value: C::Value,
        block_size: usize,
    ) -> io::Result<Self> {
        if timestamp_ms < base_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timestamp precedes window start",
            ));
        }
        let initial_samples = block_size.clamp(1, Self::INITIAL_RESERVE_SAMPLES);
        let per_ts = varint_len(timestamp_ms.saturating_sub(base_ms)).max(1);
        let mut timestamps = Vec::with_capacity(per_ts.saturating_mul(initial_samples));
        encode_varint(timestamp_ms - base_ms, &mut timestamps);
        let mut values = C::new(value)?;
        values.reserve(initial_samples.saturating_sub(1));
        Ok(Self {
            base_ms,
            min_ts: timestamp_ms,
            max_ts: timestamp_ms,
            timestamps,
            values,
            samples: 1,
            reserve_full_done: block_size <= Self::INITIAL_RESERVE_SAMPLES,
        })
    }

    pub(crate) fn push_sample(
        &mut self,
        timestamp_ms: u64,
        value: C::Value,
        block_size: usize,
    ) -> io::Result<()> {
        if timestamp_ms < self.base_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timestamp precedes window start",
            ));
        }
        self.maybe_reserve_more(block_size);
        encode_varint(timestamp_ms - self.base_ms, &mut self.timestamps);
        self.values.push(value)?;
        self.samples = self.samples.saturating_add(1);
        if timestamp_ms < self.min_ts {
            self.min_ts = timestamp_ms;
        }
        if timestamp_ms > self.max_ts {
            self.max_ts = timestamp_ms;
        }
        Ok(())
    }

    pub(crate) fn is_full(&self, block_size: usize) -> bool {
        self.samples as usize >= block_size
    }

    pub(crate) fn sample_count(&self) -> u32 {
        self.samples
    }

    pub(crate) fn min_ts(&self) -> u64 {
        self.min_ts
    }

    pub(crate) fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub(crate) fn payload_bytes(&self) -> usize {
        self.timestamps
            .len()
            .saturating_add(self.values.encoded_len_bytes())
    }

    pub(crate) fn overlaps(&self, start_ms: u64, end_ms: u64) -> bool {
        self.max_ts >= start_ms && self.min_ts < end_ms
    }

    fn maybe_reserve_more(&mut self, block_size: usize) {
        if self.reserve_full_done {
            return;
        }
        let samples = self.samples as usize;
        if samples < Self::FULL_RESERVE_THRESHOLD || samples >= block_size {
            return;
        }
        let avg_ts = (self
            .timestamps
            .len()
            .saturating_add(samples.saturating_sub(1))
            / samples)
            .max(1);
        let remaining = block_size.saturating_sub(samples);
        self.timestamps.reserve(avg_ts.saturating_mul(remaining));
        self.values.reserve(remaining);
        self.reserve_full_done = true;
    }

    pub(crate) fn decode_samples(&self) -> io::Result<Vec<(u64, C::Value)>> {
        let count = self.samples as usize;
        let mut cursor = 0usize;
        let mut timestamps = Vec::with_capacity(count);
        for _ in 0..count {
            let dt = decode_varint(&self.timestamps, &mut cursor)?;
            timestamps.push(self.base_ms.saturating_add(dt));
        }
        if cursor != self.timestamps.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "timestamp buffer has trailing bytes",
            ));
        }

        let values = C::decode_values(&self.values.snapshot_bytes(), count)?;
        if values.len() != timestamps.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded value count mismatch",
            ));
        }
        Ok(timestamps.into_iter().zip(values).collect())
    }

    pub(crate) fn seal(self, arena: &mut BlockArena) -> Block<C> {
        let timestamps = arena.write(&self.timestamps);
        let values = arena.write(&self.values.into_bytes());
        Block {
            base_ms: self.base_ms,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            timestamps,
            values,
            samples: self.samples,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Block<C: BlockCodec> {
    base_ms: u64,
    min_ts: u64,
    max_ts: u64,
    timestamps: BufferRef,
    values: BufferRef,
    samples: u32,
    _marker: PhantomData<C>,
}

impl<C: BlockCodec> Block<C> {
    #[allow(dead_code)]
    pub(crate) fn codec_name(&self) -> &'static str {
        std::any::type_name::<C>()
    }

    #[allow(dead_code)]
    pub(crate) fn min_ts(&self) -> u64 {
        self.min_ts
    }

    #[allow(dead_code)]
    pub(crate) fn max_ts(&self) -> u64 {
        self.max_ts
    }

    #[allow(dead_code)]
    pub(crate) fn sample_count(&self) -> u32 {
        self.samples
    }

    pub(crate) fn overlaps(&self, start_ms: u64, end_ms: u64) -> bool {
        self.max_ts >= start_ms && self.min_ts < end_ms
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.timestamps.len().saturating_add(self.values.len())
    }

    pub(crate) fn decode_samples(&self, arena: &BlockArena) -> io::Result<Vec<(u64, C::Value)>> {
        let count = self.samples as usize;
        let mut cursor = 0usize;
        let ts_buf = arena.slice(self.timestamps);
        let mut timestamps = Vec::with_capacity(count);
        for _ in 0..count {
            let dt = decode_varint(ts_buf, &mut cursor)?;
            timestamps.push(self.base_ms.saturating_add(dt));
        }
        if cursor != ts_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "timestamp buffer has trailing bytes",
            ));
        }

        let values = C::decode_values(arena.slice(self.values), count)?;
        if values.len() != timestamps.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded value count mismatch",
            ));
        }
        Ok(timestamps.into_iter().zip(values).collect())
    }
}

#[derive(Debug)]
pub(crate) struct FloatGorillaCodec {
    values: GorillaEncoder,
}

impl BlockCodec for FloatGorillaCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        let mut values = GorillaEncoder::new();
        values.push(first)?;
        Ok(Self { values })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value)
    }

    fn reserve(&mut self, _additional_samples: usize) {}

    fn encoded_len_bytes(&self) -> usize {
        self.values.len_bytes()
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.snapshot()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.values.finish()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_gorilla_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatElfCodec {
    values: ElfEncoder,
}

impl BlockCodec for FloatElfCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        let mut values = ElfEncoder::new();
        values.push(first)?;
        Ok(Self { values })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value)
    }

    fn reserve(&mut self, _additional_samples: usize) {}

    fn encoded_len_bytes(&self) -> usize {
        self.values.len_bytes()
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.snapshot()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.values.finish()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_elf_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatChimp128DuckDBDeferredCodec {
    values: Vec<f64>,
}

impl BlockCodec for FloatChimp128DuckDBDeferredCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        Ok(Self {
            values: vec![first],
        })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value);
        Ok(())
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.snapshot_bytes().len()
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        encode_chimp128_duckdb_values(&self.values).unwrap_or_default()
    }

    fn into_bytes(self) -> Vec<u8> {
        encode_chimp128_duckdb_values(&self.values).unwrap_or_default()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_chimp128_duckdb_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatChimp128BaselineDeferredCodec {
    values: Vec<f64>,
}

impl BlockCodec for FloatChimp128BaselineDeferredCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        Ok(Self {
            values: vec![first],
        })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value);
        Ok(())
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.snapshot_bytes().len()
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        encode_chimp128_baseline_values(&self.values).unwrap_or_default()
    }

    fn into_bytes(self) -> Vec<u8> {
        encode_chimp128_baseline_values(&self.values).unwrap_or_default()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_chimp128_baseline_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatAlpCodec {
    values: AlpEncoder,
}

impl BlockCodec for FloatAlpCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        let mut values = AlpEncoder::new();
        values.push(first)?;
        Ok(Self { values })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value)
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.values.len_bytes().unwrap_or(0)
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.snapshot().unwrap_or_default()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.values.finish().unwrap_or_default()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_alp_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatAlpRdCodec {
    values: AlpRdEncoder,
}

impl BlockCodec for FloatAlpRdCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        let mut values = AlpRdEncoder::new();
        values.push(first)?;
        Ok(Self { values })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value)
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.values.len_bytes().unwrap_or(0)
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.snapshot().unwrap_or_default()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.values.finish().unwrap_or_default()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_alp_rd_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatAlpSpiralCodec {
    values: AlpSpiralEncoder,
}

impl BlockCodec for FloatAlpSpiralCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        let mut values = AlpSpiralEncoder::new();
        values.push(first)?;
        Ok(Self { values })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value)
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.values.len_bytes().unwrap_or(0)
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.snapshot().unwrap_or_default()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.values.finish().unwrap_or_default()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_alp_spiral_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatAlpRdSpiralCodec {
    values: AlpRdSpiralEncoder,
}

impl BlockCodec for FloatAlpRdSpiralCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        let mut values = AlpRdSpiralEncoder::new();
        values.push(first)?;
        Ok(Self { values })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value)
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.values.len_bytes().unwrap_or(0)
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.snapshot().unwrap_or_default()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.values.finish().unwrap_or_default()
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_alp_rd_spiral_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct FloatRawCodec {
    values: Vec<f64>,
}

impl BlockCodec for FloatRawCodec {
    type Value = f64;

    fn new(first: Self::Value) -> io::Result<Self> {
        Ok(Self {
            values: vec![first],
        })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value);
        Ok(())
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.values.len().saturating_mul(mem::size_of::<f64>())
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len_bytes());
        for value in &self.values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len_bytes());
        for value in self.values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        let expected_len = count.saturating_mul(mem::size_of::<f64>());
        if buf.len() != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw float value count mismatch",
            ));
        }
        let mut values = Vec::with_capacity(count);
        for chunk in buf.chunks_exact(mem::size_of::<f64>()) {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(chunk);
            values.push(f64::from_le_bytes(bytes));
        }
        Ok(values)
    }
}

#[derive(Debug)]
pub(crate) struct IntDeltaCodec {
    values: Vec<u8>,
    last_value: i64,
}

impl BlockCodec for IntDeltaCodec {
    type Value = i64;

    fn new(first: Self::Value) -> io::Result<Self> {
        let mut values = Vec::new();
        encode_varint(encode_zigzag_i64(first), &mut values);
        Ok(Self {
            values,
            last_value: first,
        })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        let delta = value.wrapping_sub(self.last_value);
        encode_varint(encode_zigzag_i64(delta), &mut self.values);
        self.last_value = value;
        Ok(())
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.values.len()
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        self.values.clone()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.values
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        decode_int_values(buf, count)
    }
}

#[derive(Debug)]
pub(crate) struct IntRawCodec {
    values: Vec<i64>,
}

impl BlockCodec for IntRawCodec {
    type Value = i64;

    fn new(first: Self::Value) -> io::Result<Self> {
        Ok(Self {
            values: vec![first],
        })
    }

    fn push(&mut self, value: Self::Value) -> io::Result<()> {
        self.values.push(value);
        Ok(())
    }

    fn reserve(&mut self, additional_samples: usize) {
        self.values.reserve(additional_samples);
    }

    fn encoded_len_bytes(&self) -> usize {
        self.values.len().saturating_mul(mem::size_of::<i64>())
    }

    fn snapshot_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len_bytes());
        for value in &self.values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len_bytes());
        for value in self.values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn decode_values(buf: &[u8], count: usize) -> io::Result<Vec<Self::Value>> {
        let expected_len = count.saturating_mul(mem::size_of::<i64>());
        if buf.len() != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw int value count mismatch",
            ));
        }
        let mut values = Vec::with_capacity(count);
        for chunk in buf.chunks_exact(mem::size_of::<i64>()) {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(chunk);
            values.push(i64::from_le_bytes(bytes));
        }
        Ok(values)
    }
}

fn decode_int_values(buf: &[u8], count: usize) -> io::Result<Vec<i64>> {
    let mut cursor = 0usize;
    let mut values = Vec::with_capacity(count);
    let mut prev = 0i64;
    for _ in 0..count {
        let encoded = decode_varint(buf, &mut cursor)?;
        let delta = decode_zigzag_i64(encoded);
        let value = prev.wrapping_add(delta);
        values.push(value);
        prev = value;
    }
    if cursor != buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "value buffer has trailing bytes",
        ));
    }
    Ok(values)
}
