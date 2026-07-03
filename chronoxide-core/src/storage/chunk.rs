use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use crc32c::crc32c;

use crate::storage::encoding::{
    SchemaVarLenCodec, SchemaVarLenEncoding, decode_gorilla_values, decode_varint,
    decode_zigzag_i64, encode_gorilla_values, encode_varint, encode_zigzag_i64,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramValue, HistogramValue, OtlpAggregationTemporality,
    SummaryValue, TypedSampleMetadata, decode_opt_f64, decode_typed_metadata,
};

const FRAME_HEADER_LEN: usize = 14;
const CHUNK_HEADER_LEN: usize = 40;
const CHUNK_ENTRY_LEN: usize = 40;
const CHUNK_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"CHIX");
const CHUNK_INDEX_HEADER_LEN: u64 = 12;
const CHUNK_WRITE_BUFFER_BYTES: usize = 1024 * 1024;

pub const CHUNK_FLAG_HAS_START_TIME: u16 = 1 << 1;
pub const CHUNK_FLAG_HAS_PER_SAMPLE_FLAGS: u16 = 1 << 2;
pub const CHUNK_FLAG_HAS_COUNTER_RESET_HINTS: u16 = 1 << 3;
pub const CHUNK_FLAG_TEMPORALITY_DELTA: u16 = 1 << 4;

fn typed_chunk_flags(metadata: impl IntoIterator<Item = TypedSampleMetadata>) -> u16 {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkKind {
    Float = 0,
    Int64 = 1,
    Histogram = 2,
    ExponentialHistogram = 3,
    Summary = 4,
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
    pub reserved0: u32,
    pub reserved1: u32,
}

pub struct ChunkWriter {
    file: BufWriter<File>,
    offset: u64,
}

impl ChunkWriter {
    pub fn new(file: File) -> io::Result<Self> {
        let offset = file.metadata()?.len();
        Ok(Self {
            file: BufWriter::with_capacity(CHUNK_WRITE_BUFFER_BYTES, file),
            offset,
        })
    }

    pub fn append_float_sample(
        &mut self,
        series_ref: u32,
        timestamp_ms: u64,
        value: f64,
    ) -> io::Result<ChunkIndexEntry> {
        self.append_float_chunk(series_ref, &[(timestamp_ms, value)])
    }

    pub fn append_float_sample_raw(
        &mut self,
        series_ref: u32,
        timestamp_ms: u64,
        value: f64,
    ) -> io::Result<ChunkIndexEntry> {
        self.append_float_chunk_raw(series_ref, &[(timestamp_ms, value)])
    }

    pub fn append_int_sample(
        &mut self,
        series_ref: u32,
        timestamp_ms: u64,
        value: i64,
    ) -> io::Result<ChunkIndexEntry> {
        self.append_int_chunk(series_ref, &[(timestamp_ms, value)])
    }

    pub fn append_int_sample_raw(
        &mut self,
        series_ref: u32,
        timestamp_ms: u64,
        value: i64,
    ) -> io::Result<ChunkIndexEntry> {
        self.append_int_chunk_raw(series_ref, &[(timestamp_ms, value)])
    }

    pub fn append_histogram_chunk_ordered(
        &mut self,
        series_ref: u32,
        samples: &[(u64, HistogramValue)],
    ) -> io::Result<ChunkIndexEntry> {
        self.append_schema_varlen_chunk_ordered(
            ChunkKind::Histogram,
            series_ref,
            samples,
            typed_chunk_flags(samples.iter().map(|(_, value)| value.metadata)),
        )
    }

    pub fn append_exponential_histogram_chunk_ordered(
        &mut self,
        series_ref: u32,
        samples: &[(u64, ExponentialHistogramValue)],
    ) -> io::Result<ChunkIndexEntry> {
        self.append_schema_varlen_chunk_ordered(
            ChunkKind::ExponentialHistogram,
            series_ref,
            samples,
            typed_chunk_flags(samples.iter().map(|(_, value)| value.metadata)),
        )
    }

    pub fn append_summary_chunk_ordered(
        &mut self,
        series_ref: u32,
        samples: &[(u64, SummaryValue)],
    ) -> io::Result<ChunkIndexEntry> {
        self.append_schema_varlen_chunk_ordered(
            ChunkKind::Summary,
            series_ref,
            samples,
            typed_chunk_flags(samples.iter().map(|(_, value)| value.metadata)),
        )
    }

    fn append_schema_varlen_chunk_ordered<T>(
        &mut self,
        kind: ChunkKind,
        series_ref: u32,
        samples: &[(u64, T)],
        flags: u16,
    ) -> io::Result<ChunkIndexEntry>
    where
        T: SchemaVarLenEncoding + Clone,
    {
        validate_ordered_samples(samples)?;

        let min_time_ms = samples.first().unwrap().0;
        let max_time_ms = samples.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut dt_buf = Vec::new();
        for (ts, _) in samples {
            let dt = ts.saturating_sub(t0_ms);
            encode_varint(dt, &mut dt_buf);
        }

        let Some((_, first_value)) = samples.first() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "samples must be non-empty",
            ));
        };
        let mut codec = SchemaVarLenCodec::new(first_value.clone())?;
        for (_, value) in samples.iter().skip(1) {
            codec.push(value.clone())?;
        }

        let mut payload = Vec::new();
        payload.extend_from_slice(&t0_ms.to_le_bytes());
        payload.extend_from_slice(&dt_buf);
        payload.extend_from_slice(&codec.into_bytes());

        self.append_chunk_payload(
            kind,
            ChunkEncoding::SchemaVarLen,
            flags,
            series_ref,
            min_time_ms,
            max_time_ms,
            samples.len() as u32,
            &payload,
        )
    }

    pub fn append_float_chunk(
        &mut self,
        series_ref: u32,
        samples: &[(u64, f64)],
    ) -> io::Result<ChunkIndexEntry> {
        if samples.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "samples must be non-empty",
            ));
        }

        let mut sorted: Vec<(u64, f64)> = samples.to_vec();
        sorted.sort_by_key(|(ts, _)| *ts);
        self.append_float_chunk_ordered(series_ref, &sorted)
    }

    pub fn append_float_chunk_ordered(
        &mut self,
        series_ref: u32,
        samples: &[(u64, f64)],
    ) -> io::Result<ChunkIndexEntry> {
        validate_ordered_samples(samples)?;

        let min_time_ms = samples.first().unwrap().0;
        let max_time_ms = samples.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut dt_buf = Vec::new();
        let mut values = Vec::with_capacity(samples.len());
        for (ts, value) in samples {
            let dt = ts.saturating_sub(t0_ms);
            encode_varint(dt, &mut dt_buf);
            values.push(*value);
        }
        let value_buf = encode_gorilla_values(&values)?;

        let mut payload = Vec::new();
        payload.extend_from_slice(&t0_ms.to_le_bytes());
        payload.extend_from_slice(&dt_buf);
        payload.extend_from_slice(&value_buf);
        let payload_len = payload.len() as u32;
        let chunk_crc = crc32c(&payload);

        let mut chunk_header = Vec::with_capacity(CHUNK_HEADER_LEN);
        chunk_header.push(ChunkKind::Float as u8);
        chunk_header.push(ChunkEncoding::Gorilla as u8);
        chunk_header.extend_from_slice(&0u16.to_le_bytes());
        chunk_header.extend_from_slice(&series_ref.to_le_bytes());
        chunk_header.extend_from_slice(&min_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&max_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&(samples.len() as u32).to_le_bytes());
        chunk_header.extend_from_slice(&(CHUNK_HEADER_LEN as u32).to_le_bytes());
        chunk_header.extend_from_slice(&payload_len.to_le_bytes());
        chunk_header.extend_from_slice(&chunk_crc.to_le_bytes());

        let mut frame_crc_buf = Vec::with_capacity(chunk_header.len() + payload.len());
        frame_crc_buf.extend_from_slice(&chunk_header);
        frame_crc_buf.extend_from_slice(&payload);
        let frame_crc = crc32c(&frame_crc_buf);
        let frame_len = (FRAME_HEADER_LEN + frame_crc_buf.len()) as u32;

        let mut frame_header = Vec::with_capacity(FRAME_HEADER_LEN);
        frame_header.extend_from_slice(&frame_len.to_le_bytes());
        frame_header.extend_from_slice(&frame_crc.to_le_bytes());
        frame_header.extend_from_slice(&0u16.to_le_bytes());
        frame_header.extend_from_slice(&(1u32).to_le_bytes());

        let chunk_offset = self.offset + FRAME_HEADER_LEN as u64;
        let chunk_length = (CHUNK_HEADER_LEN + payload.len()) as u32;

        self.file.write_all(&frame_header)?;
        self.file.write_all(&chunk_header)?;
        self.file.write_all(&payload)?;
        self.offset = self.offset.saturating_add(frame_len as u64);

        Ok(ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset: chunk_offset,
            length: chunk_length,
            reserved0: 0,
            reserved1: 0,
        })
    }

    pub fn append_float_chunk_raw(
        &mut self,
        series_ref: u32,
        samples: &[(u64, f64)],
    ) -> io::Result<ChunkIndexEntry> {
        if samples.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "samples must be non-empty",
            ));
        }

        let mut sorted: Vec<(u64, f64)> = samples.to_vec();
        sorted.sort_by_key(|(ts, _)| *ts);
        self.append_float_chunk_raw_ordered(series_ref, &sorted)
    }

    pub fn append_float_chunk_raw_ordered(
        &mut self,
        series_ref: u32,
        samples: &[(u64, f64)],
    ) -> io::Result<ChunkIndexEntry> {
        validate_ordered_samples(samples)?;

        let min_time_ms = samples.first().unwrap().0;
        let max_time_ms = samples.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut payload = Vec::new();
        payload.extend_from_slice(&t0_ms.to_le_bytes());
        for (ts, value) in samples {
            let dt = ts.saturating_sub(t0_ms);
            encode_varint(dt, &mut payload);
            payload.extend_from_slice(&value.to_le_bytes());
        }
        let payload_len = payload.len() as u32;
        let chunk_crc = crc32c(&payload);

        let mut chunk_header = Vec::with_capacity(CHUNK_HEADER_LEN);
        chunk_header.push(ChunkKind::Float as u8);
        chunk_header.push(ChunkEncoding::RawF64 as u8);
        chunk_header.extend_from_slice(&0u16.to_le_bytes());
        chunk_header.extend_from_slice(&series_ref.to_le_bytes());
        chunk_header.extend_from_slice(&min_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&max_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&(samples.len() as u32).to_le_bytes());
        chunk_header.extend_from_slice(&(CHUNK_HEADER_LEN as u32).to_le_bytes());
        chunk_header.extend_from_slice(&payload_len.to_le_bytes());
        chunk_header.extend_from_slice(&chunk_crc.to_le_bytes());

        let mut frame_crc_buf = Vec::with_capacity(chunk_header.len() + payload.len());
        frame_crc_buf.extend_from_slice(&chunk_header);
        frame_crc_buf.extend_from_slice(&payload);
        let frame_crc = crc32c(&frame_crc_buf);
        let frame_len = (FRAME_HEADER_LEN + frame_crc_buf.len()) as u32;

        let mut frame_header = Vec::with_capacity(FRAME_HEADER_LEN);
        frame_header.extend_from_slice(&frame_len.to_le_bytes());
        frame_header.extend_from_slice(&frame_crc.to_le_bytes());
        frame_header.extend_from_slice(&0u16.to_le_bytes());
        frame_header.extend_from_slice(&(1u32).to_le_bytes());

        let chunk_offset = self.offset + FRAME_HEADER_LEN as u64;
        let chunk_length = (CHUNK_HEADER_LEN + payload.len()) as u32;

        self.file.write_all(&frame_header)?;
        self.file.write_all(&chunk_header)?;
        self.file.write_all(&payload)?;
        self.offset = self.offset.saturating_add(frame_len as u64);

        Ok(ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset: chunk_offset,
            length: chunk_length,
            reserved0: 0,
            reserved1: 0,
        })
    }

    pub fn append_int_chunk(
        &mut self,
        series_ref: u32,
        samples: &[(u64, i64)],
    ) -> io::Result<ChunkIndexEntry> {
        if samples.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "samples must be non-empty",
            ));
        }

        let mut sorted: Vec<(u64, i64)> = samples.to_vec();
        sorted.sort_by_key(|(ts, _)| *ts);
        self.append_int_chunk_ordered(series_ref, &sorted)
    }

    pub fn append_int_chunk_ordered(
        &mut self,
        series_ref: u32,
        samples: &[(u64, i64)],
    ) -> io::Result<ChunkIndexEntry> {
        validate_ordered_samples(samples)?;

        let min_time_ms = samples.first().unwrap().0;
        let max_time_ms = samples.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut dt_buf = Vec::new();
        let mut value_buf = Vec::new();
        let mut prev = 0i64;
        for (ts, value) in samples {
            let dt = ts.saturating_sub(t0_ms);
            encode_varint(dt, &mut dt_buf);
            let delta = value.wrapping_sub(prev);
            encode_varint(encode_zigzag_i64(delta), &mut value_buf);
            prev = *value;
        }

        let mut payload = Vec::new();
        payload.extend_from_slice(&t0_ms.to_le_bytes());
        payload.extend_from_slice(&dt_buf);
        payload.extend_from_slice(&value_buf);
        let payload_len = payload.len() as u32;
        let chunk_crc = crc32c(&payload);

        let mut chunk_header = Vec::with_capacity(CHUNK_HEADER_LEN);
        chunk_header.push(ChunkKind::Int64 as u8);
        chunk_header.push(ChunkEncoding::IntDeltaZigZag as u8);
        chunk_header.extend_from_slice(&0u16.to_le_bytes());
        chunk_header.extend_from_slice(&series_ref.to_le_bytes());
        chunk_header.extend_from_slice(&min_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&max_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&(samples.len() as u32).to_le_bytes());
        chunk_header.extend_from_slice(&(CHUNK_HEADER_LEN as u32).to_le_bytes());
        chunk_header.extend_from_slice(&payload_len.to_le_bytes());
        chunk_header.extend_from_slice(&chunk_crc.to_le_bytes());

        let mut frame_crc_buf = Vec::with_capacity(chunk_header.len() + payload.len());
        frame_crc_buf.extend_from_slice(&chunk_header);
        frame_crc_buf.extend_from_slice(&payload);
        let frame_crc = crc32c(&frame_crc_buf);
        let frame_len = (FRAME_HEADER_LEN + frame_crc_buf.len()) as u32;

        let mut frame_header = Vec::with_capacity(FRAME_HEADER_LEN);
        frame_header.extend_from_slice(&frame_len.to_le_bytes());
        frame_header.extend_from_slice(&frame_crc.to_le_bytes());
        frame_header.extend_from_slice(&0u16.to_le_bytes());
        frame_header.extend_from_slice(&(1u32).to_le_bytes());

        let chunk_offset = self.offset + FRAME_HEADER_LEN as u64;
        let chunk_length = (CHUNK_HEADER_LEN + payload.len()) as u32;

        self.file.write_all(&frame_header)?;
        self.file.write_all(&chunk_header)?;
        self.file.write_all(&payload)?;
        self.offset = self.offset.saturating_add(frame_len as u64);

        Ok(ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Int64,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset: chunk_offset,
            length: chunk_length,
            reserved0: 0,
            reserved1: 0,
        })
    }

    pub fn append_int_chunk_raw(
        &mut self,
        series_ref: u32,
        samples: &[(u64, i64)],
    ) -> io::Result<ChunkIndexEntry> {
        if samples.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "samples must be non-empty",
            ));
        }

        let mut sorted: Vec<(u64, i64)> = samples.to_vec();
        sorted.sort_by_key(|(ts, _)| *ts);
        self.append_int_chunk_raw_ordered(series_ref, &sorted)
    }

    pub fn append_int_chunk_raw_ordered(
        &mut self,
        series_ref: u32,
        samples: &[(u64, i64)],
    ) -> io::Result<ChunkIndexEntry> {
        validate_ordered_samples(samples)?;

        let min_time_ms = samples.first().unwrap().0;
        let max_time_ms = samples.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut payload = Vec::new();
        payload.extend_from_slice(&t0_ms.to_le_bytes());
        for (ts, value) in samples {
            let dt = ts.saturating_sub(t0_ms);
            encode_varint(dt, &mut payload);
            payload.extend_from_slice(&value.to_le_bytes());
        }
        let payload_len = payload.len() as u32;
        let chunk_crc = crc32c(&payload);

        let mut chunk_header = Vec::with_capacity(CHUNK_HEADER_LEN);
        chunk_header.push(ChunkKind::Int64 as u8);
        chunk_header.push(ChunkEncoding::RawI64 as u8);
        chunk_header.extend_from_slice(&0u16.to_le_bytes());
        chunk_header.extend_from_slice(&series_ref.to_le_bytes());
        chunk_header.extend_from_slice(&min_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&max_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&(samples.len() as u32).to_le_bytes());
        chunk_header.extend_from_slice(&(CHUNK_HEADER_LEN as u32).to_le_bytes());
        chunk_header.extend_from_slice(&payload_len.to_le_bytes());
        chunk_header.extend_from_slice(&chunk_crc.to_le_bytes());

        let mut frame_crc_buf = Vec::with_capacity(chunk_header.len() + payload.len());
        frame_crc_buf.extend_from_slice(&chunk_header);
        frame_crc_buf.extend_from_slice(&payload);
        let frame_crc = crc32c(&frame_crc_buf);
        let frame_len = (FRAME_HEADER_LEN + frame_crc_buf.len()) as u32;

        let mut frame_header = Vec::with_capacity(FRAME_HEADER_LEN);
        frame_header.extend_from_slice(&frame_len.to_le_bytes());
        frame_header.extend_from_slice(&frame_crc.to_le_bytes());
        frame_header.extend_from_slice(&0u16.to_le_bytes());
        frame_header.extend_from_slice(&(1u32).to_le_bytes());

        let chunk_offset = self.offset + FRAME_HEADER_LEN as u64;
        let chunk_length = (CHUNK_HEADER_LEN + payload.len()) as u32;

        self.file.write_all(&frame_header)?;
        self.file.write_all(&chunk_header)?;
        self.file.write_all(&payload)?;
        self.offset = self.offset.saturating_add(frame_len as u64);

        Ok(ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Int64,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset: chunk_offset,
            length: chunk_length,
            reserved0: 0,
            reserved1: 0,
        })
    }

    fn append_chunk_payload(
        &mut self,
        kind: ChunkKind,
        encoding: ChunkEncoding,
        flags: u16,
        series_ref: u32,
        min_time_ms: u64,
        max_time_ms: u64,
        num_points: u32,
        payload: &[u8],
    ) -> io::Result<ChunkIndexEntry> {
        let payload_len = payload.len() as u32;
        let chunk_crc = crc32c(payload);

        let mut chunk_header = Vec::with_capacity(CHUNK_HEADER_LEN);
        chunk_header.push(kind as u8);
        chunk_header.push(encoding as u8);
        chunk_header.extend_from_slice(&flags.to_le_bytes());
        chunk_header.extend_from_slice(&series_ref.to_le_bytes());
        chunk_header.extend_from_slice(&min_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&max_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&num_points.to_le_bytes());
        chunk_header.extend_from_slice(&(CHUNK_HEADER_LEN as u32).to_le_bytes());
        chunk_header.extend_from_slice(&payload_len.to_le_bytes());
        chunk_header.extend_from_slice(&chunk_crc.to_le_bytes());

        let mut frame_crc_buf = Vec::with_capacity(chunk_header.len() + payload.len());
        frame_crc_buf.extend_from_slice(&chunk_header);
        frame_crc_buf.extend_from_slice(payload);
        let frame_crc = crc32c(&frame_crc_buf);
        let frame_len = (FRAME_HEADER_LEN + frame_crc_buf.len()) as u32;

        let mut frame_header = Vec::with_capacity(FRAME_HEADER_LEN);
        frame_header.extend_from_slice(&frame_len.to_le_bytes());
        frame_header.extend_from_slice(&frame_crc.to_le_bytes());
        frame_header.extend_from_slice(&0u16.to_le_bytes());
        frame_header.extend_from_slice(&(1u32).to_le_bytes());

        let chunk_offset = self.offset + FRAME_HEADER_LEN as u64;
        let chunk_length = (CHUNK_HEADER_LEN + payload.len()) as u32;

        self.file.write_all(&frame_header)?;
        self.file.write_all(&chunk_header)?;
        self.file.write_all(payload)?;
        self.offset = self.offset.saturating_add(frame_len as u64);

        Ok(ChunkIndexEntry {
            file_id: 0,
            kind,
            flags,
            min_time_ms,
            max_time_ms,
            offset: chunk_offset,
            length: chunk_length,
            reserved0: 0,
            reserved1: 0,
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn validate_ordered_samples<T>(samples: &[(u64, T)]) -> io::Result<()> {
    if samples.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "samples must be non-empty",
        ));
    }
    if samples.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ordered samples must be sorted by timestamp",
        ));
    }
    Ok(())
}

pub fn write_chunk_index(writer: impl Write, entries: &[Vec<ChunkIndexEntry>]) -> io::Result<()> {
    let mut writer = BufWriter::with_capacity(CHUNK_WRITE_BUFFER_BYTES, writer);
    let num_series = entries.len() as u32;
    let header_len = 4 + 2 + 2 + 4;
    let offsets_len = (num_series as usize + 1) * 8;

    let mut offsets: Vec<u64> = Vec::with_capacity(num_series as usize + 1);
    let mut cursor = (header_len + offsets_len) as u64;
    for series_entries in entries {
        offsets.push(cursor);
        cursor = cursor.saturating_add((series_entries.len() * chunk_entry_len()) as u64);
    }
    offsets.push(cursor);

    writer.write_all(&CHUNK_INDEX_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&num_series.to_le_bytes())?;
    for offset in offsets {
        writer.write_all(&offset.to_le_bytes())?;
    }

    for series_entries in entries {
        let mut ordered = series_entries.clone();
        ordered.sort_by(|a, b| {
            a.min_time_ms
                .cmp(&b.min_time_ms)
                .then_with(|| a.max_time_ms.cmp(&b.max_time_ms))
                .then_with(|| a.offset.cmp(&b.offset))
        });
        for entry in ordered {
            write_chunk_entry(&mut writer, &entry)?;
        }
    }
    writer.flush()
}

pub fn read_chunk_index(file: &mut File) -> io::Result<Vec<Vec<ChunkIndexEntry>>> {
    let offsets = read_chunk_index_offsets(file)?;
    let num_series = offsets.len().saturating_sub(1);
    let entry_len = chunk_entry_len() as u64;
    let mut entries = Vec::with_capacity(num_series);
    file.seek(SeekFrom::Start(
        offsets.first().copied().unwrap_or_default(),
    ))?;
    let mut reader = BufReader::with_capacity(CHUNK_WRITE_BUFFER_BYTES, file);
    for i in 0..num_series {
        entries.push(read_chunk_index_entries_from_reader(
            &mut reader,
            &offsets,
            i,
            entry_len,
        )?);
    }

    Ok(entries)
}

pub struct ChunkIndexReader {
    file: File,
    num_series: usize,
    data_start: u64,
    offsets: Option<Vec<u64>>,
}

impl ChunkIndexReader {
    pub fn open(mut file: File) -> io::Result<Self> {
        let (num_series, data_start) = read_chunk_index_header(&mut file)?;
        Ok(Self {
            file,
            num_series,
            data_start,
            offsets: None,
        })
    }

    pub fn len(&self) -> usize {
        self.num_series
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read_entries(&mut self, series_ref: u32) -> io::Result<Option<Vec<ChunkIndexEntry>>> {
        let series_ref = series_ref as usize;
        if series_ref >= self.len() {
            return Ok(None);
        }
        let offsets = read_chunk_index_offset_pair(
            &mut self.file,
            series_ref,
            self.num_series,
            self.data_start,
        )?;
        read_chunk_index_entries(&mut self.file, &offsets, 0, chunk_entry_len() as u64).map(Some)
    }

    pub fn for_each_series_entries<F>(&mut self, mut visit: F) -> io::Result<()>
    where
        F: FnMut(u32, &[ChunkIndexEntry]) -> io::Result<()>,
    {
        if self.offsets.is_none() {
            self.offsets = Some(read_chunk_index_offsets(&mut self.file)?);
        }
        let offsets = self.offsets.as_ref().unwrap().clone();
        let Some(first_offset) = offsets.first().copied() else {
            return Ok(());
        };
        let entry_len = chunk_entry_len() as u64;
        let num_series = self.len();
        self.file.seek(SeekFrom::Start(first_offset))?;
        let mut reader = BufReader::with_capacity(CHUNK_WRITE_BUFFER_BYTES, &mut self.file);
        let mut entries = Vec::new();

        for series_ref in 0..num_series {
            entries.clear();
            read_chunk_index_entries_into(
                &mut reader,
                &offsets,
                series_ref,
                entry_len,
                &mut entries,
            )?;
            let series_ref = u32::try_from(series_ref).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "series_ref exceeds u32")
            })?;
            visit(series_ref, &entries)?;
        }

        Ok(())
    }
}

fn read_chunk_index_header(file: &mut File) -> io::Result<(usize, u64)> {
    file.seek(SeekFrom::Start(0))?;
    let magic = read_exact_u32(file)?;
    if magic != CHUNK_INDEX_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index magic mismatch",
        ));
    }
    let version = read_exact_u16(file)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported chunk index version",
        ));
    }
    let _reserved = read_exact_u16(file)?;
    let num_series = read_exact_u32(file)? as usize;
    let data_start = CHUNK_INDEX_HEADER_LEN + ((num_series as u64 + 1) * 8);
    let first_offset = read_exact_u64(file)?;
    if first_offset != data_start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index offsets header invalid",
        ));
    }
    Ok((num_series, data_start))
}

fn read_chunk_index_offset_pair(
    file: &mut File,
    series_ref: usize,
    num_series: usize,
    data_start: u64,
) -> io::Result<[u64; 2]> {
    if series_ref >= num_series {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "series_ref out of chunk index range",
        ));
    }
    let offset_pos = CHUNK_INDEX_HEADER_LEN + (series_ref as u64 * 8);
    file.seek(SeekFrom::Start(offset_pos))?;
    let start = read_exact_u64(file)?;
    let end = read_exact_u64(file)?;
    if start < data_start || end < start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index offsets out of order",
        ));
    }
    Ok([start, end])
}

fn read_chunk_index_offsets(file: &mut File) -> io::Result<Vec<u64>> {
    file.seek(SeekFrom::Start(0))?;
    let magic = read_exact_u32(file)?;
    if magic != CHUNK_INDEX_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index magic mismatch",
        ));
    }
    let version = read_exact_u16(file)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported chunk index version",
        ));
    }
    let _reserved = read_exact_u16(file)?;
    let num_series = read_exact_u32(file)? as usize;
    let offsets_len = (num_series + 1) * 8;
    let expected_start = CHUNK_INDEX_HEADER_LEN + offsets_len as u64;

    let mut offsets = Vec::with_capacity(num_series + 1);
    for _ in 0..=num_series {
        offsets.push(read_exact_u64(file)?);
    }

    if offsets.first().copied() != Some(expected_start) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index offsets header invalid",
        ));
    }

    for pair in offsets.windows(2) {
        if pair[1] < pair[0] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk index offsets out of order",
            ));
        }
    }

    Ok(offsets)
}

fn read_chunk_index_entries(
    file: &mut File,
    offsets: &[u64],
    series_ref: usize,
    entry_len: u64,
) -> io::Result<Vec<ChunkIndexEntry>> {
    let start = offsets[series_ref];
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::with_capacity(CHUNK_WRITE_BUFFER_BYTES, file);
    read_chunk_index_entries_from_reader(&mut reader, offsets, series_ref, entry_len)
}

fn read_chunk_index_entries_from_reader<R: Read>(
    reader: &mut R,
    offsets: &[u64],
    series_ref: usize,
    entry_len: u64,
) -> io::Result<Vec<ChunkIndexEntry>> {
    let count = chunk_index_entry_count(offsets, series_ref, entry_len)?;
    let mut series_entries = Vec::with_capacity(count);
    for _ in 0..count {
        series_entries.push(read_chunk_entry(reader)?);
    }
    Ok(series_entries)
}

fn read_chunk_index_entries_into<R: Read>(
    reader: &mut R,
    offsets: &[u64],
    series_ref: usize,
    entry_len: u64,
    entries: &mut Vec<ChunkIndexEntry>,
) -> io::Result<()> {
    let count = chunk_index_entry_count(offsets, series_ref, entry_len)?;
    entries.reserve(count);
    for _ in 0..count {
        entries.push(read_chunk_entry(reader)?);
    }
    Ok(())
}

fn chunk_index_entry_count(
    offsets: &[u64],
    series_ref: usize,
    entry_len: u64,
) -> io::Result<usize> {
    let start = offsets[series_ref];
    let end = offsets[series_ref + 1];
    let len = end - start;
    if len % entry_len != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index entry length misaligned",
        ));
    }
    Ok((len / entry_len) as usize)
}

pub struct ChunkReader {
    file: File,
}

impl ChunkReader {
    pub fn new(file: File) -> Self {
        Self { file }
    }

    pub fn read_next(&mut self) -> io::Result<Option<ChunkRecord>> {
        let mut header = [0u8; FRAME_HEADER_LEN];
        if let Err(err) = self.file.read_exact(&mut header) {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(err);
        }

        let frame_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let frame_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let num_chunks = u32::from_le_bytes(header[10..14].try_into().unwrap());
        if num_chunks != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "only single-chunk frames are supported",
            ));
        }

        let payload_len = frame_len
            .checked_sub(FRAME_HEADER_LEN)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "frame_len too small"))?;
        let mut payload = vec![0u8; payload_len];
        self.file.read_exact(&mut payload)?;
        if crc32c(&payload) != frame_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame crc mismatch",
            ));
        }

        Ok(Some(decode_chunk_record(&payload)?))
    }
}

pub fn read_chunk_record_at(file: &mut File, offset: u64, length: u32) -> io::Result<ChunkRecord> {
    file.seek(SeekFrom::Start(offset))?;
    let mut payload = vec![0u8; length as usize];
    file.read_exact(&mut payload)?;
    decode_chunk_record(&payload)
}

pub fn read_chunk_scalar_projection_at(
    file: &mut File,
    offset: u64,
    length: u32,
    projection: ChunkScalarProjection,
) -> io::Result<ChunkScalarProjectionRecord> {
    file.seek(SeekFrom::Start(offset))?;
    let mut payload = vec![0u8; length as usize];
    file.read_exact(&mut payload)?;
    decode_chunk_scalar_projection(&payload, projection)
}

struct DecodedChunkPayload<'a> {
    kind: ChunkKind,
    encoding: ChunkEncoding,
    series_ref: u32,
    min_time_ms: u64,
    max_time_ms: u64,
    num_points: u32,
    payload: &'a [u8],
}

fn decode_chunk_payload(payload: &[u8]) -> io::Result<DecodedChunkPayload<'_>> {
    if payload.len() < CHUNK_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "chunk header short read",
        ));
    }
    let chunk_header = &payload[..CHUNK_HEADER_LEN];
    let kind = chunk_kind_from_u8(chunk_header[0])?;
    let encoding = chunk_encoding_from_u8(chunk_header[1])?;
    let series_ref = u32::from_le_bytes(chunk_header[4..8].try_into().unwrap());
    let min_time_ms = u64::from_le_bytes(chunk_header[8..16].try_into().unwrap());
    let max_time_ms = u64::from_le_bytes(chunk_header[16..24].try_into().unwrap());
    let num_points = u32::from_le_bytes(chunk_header[24..28].try_into().unwrap());
    let header_len = u32::from_le_bytes(chunk_header[28..32].try_into().unwrap()) as usize;
    let payload_len = u32::from_le_bytes(chunk_header[32..36].try_into().unwrap()) as usize;
    let chunk_crc = u32::from_le_bytes(chunk_header[36..40].try_into().unwrap());

    if header_len > payload.len() || header_len + payload_len > payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk payload bounds invalid",
        ));
    }

    let chunk_payload = &payload[header_len..header_len + payload_len];
    if crc32c(chunk_payload) != chunk_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk crc mismatch",
        ));
    }

    Ok(DecodedChunkPayload {
        kind,
        encoding,
        series_ref,
        min_time_ms,
        max_time_ms,
        num_points,
        payload: chunk_payload,
    })
}

fn decode_chunk_record(payload: &[u8]) -> io::Result<ChunkRecord> {
    let decoded = decode_chunk_payload(payload)?;
    let mut cursor = 0usize;
    let t0_ms = read_u64(decoded.payload, &mut cursor)?;

    let samples = match decoded.kind {
        ChunkKind::Float => match decoded.encoding {
            ChunkEncoding::RawF64 => {
                let mut samples = Vec::with_capacity(decoded.num_points as usize);
                for _ in 0..decoded.num_points {
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    let value = read_f64(decoded.payload, &mut cursor)?;
                    samples.push((t0_ms.saturating_add(dt), value));
                }
                ChunkSamples::Float(samples)
            }
            ChunkEncoding::Gorilla => {
                let mut timestamps = Vec::with_capacity(decoded.num_points as usize);
                for _ in 0..decoded.num_points {
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    timestamps.push(t0_ms.saturating_add(dt));
                }
                let values =
                    decode_gorilla_values(&decoded.payload[cursor..], decoded.num_points as usize)?;
                let mut samples = Vec::with_capacity(decoded.num_points as usize);
                for (ts, value) in timestamps.into_iter().zip(values.into_iter()) {
                    samples.push((ts, value));
                }
                ChunkSamples::Float(samples)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported float chunk encoding",
                ));
            }
        },
        ChunkKind::Int64 => match decoded.encoding {
            ChunkEncoding::IntDeltaZigZag => {
                let mut timestamps = Vec::with_capacity(decoded.num_points as usize);
                for _ in 0..decoded.num_points {
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    timestamps.push(t0_ms.saturating_add(dt));
                }
                let mut values = Vec::with_capacity(decoded.num_points as usize);
                let mut prev = 0i64;
                for _ in 0..decoded.num_points {
                    let encoded = decode_varint(decoded.payload, &mut cursor)?;
                    let delta = decode_zigzag_i64(encoded);
                    let value = prev.wrapping_add(delta);
                    values.push(value);
                    prev = value;
                }
                let mut samples = Vec::with_capacity(decoded.num_points as usize);
                for (ts, value) in timestamps.into_iter().zip(values.into_iter()) {
                    samples.push((ts, value));
                }
                ChunkSamples::Int64(samples)
            }
            ChunkEncoding::RawI64 => {
                let mut samples = Vec::with_capacity(decoded.num_points as usize);
                for _ in 0..decoded.num_points {
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    let value = read_i64(decoded.payload, &mut cursor)?;
                    samples.push((t0_ms.saturating_add(dt), value));
                }
                ChunkSamples::Int64(samples)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported int chunk encoding",
                ));
            }
        },
        ChunkKind::Histogram => match decoded.encoding {
            ChunkEncoding::SchemaVarLen => {
                let timestamps =
                    decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
                let values = SchemaVarLenCodec::<HistogramValue>::decode_values(
                    &decoded.payload[cursor..],
                    decoded.num_points as usize,
                )?;
                ChunkSamples::Histogram(timestamps.into_iter().zip(values).collect())
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported histogram chunk encoding",
                ));
            }
        },
        ChunkKind::ExponentialHistogram => match decoded.encoding {
            ChunkEncoding::SchemaVarLen => {
                let timestamps =
                    decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
                let values = SchemaVarLenCodec::<ExponentialHistogramValue>::decode_values(
                    &decoded.payload[cursor..],
                    decoded.num_points as usize,
                )?;
                ChunkSamples::ExponentialHistogram(timestamps.into_iter().zip(values).collect())
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported exponential histogram chunk encoding",
                ));
            }
        },
        ChunkKind::Summary => match decoded.encoding {
            ChunkEncoding::SchemaVarLen => {
                let timestamps =
                    decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
                let values = SchemaVarLenCodec::<SummaryValue>::decode_values(
                    &decoded.payload[cursor..],
                    decoded.num_points as usize,
                )?;
                ChunkSamples::Summary(timestamps.into_iter().zip(values).collect())
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported summary chunk encoding",
                ));
            }
        },
    };

    Ok(ChunkRecord {
        series_ref: decoded.series_ref,
        kind: decoded.kind,
        min_time_ms: decoded.min_time_ms,
        max_time_ms: decoded.max_time_ms,
        samples,
    })
}

fn decode_chunk_scalar_projection(
    payload: &[u8],
    projection: ChunkScalarProjection,
) -> io::Result<ChunkScalarProjectionRecord> {
    let decoded = decode_chunk_payload(payload)?;
    if decoded.encoding != ChunkEncoding::SchemaVarLen {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar projection requires schema varlen encoding",
        ));
    }
    if !matches!(
        decoded.kind,
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "typed scalar projection requires a typed chunk",
        ));
    }

    let mut cursor = 0usize;
    let t0_ms = read_u64(decoded.payload, &mut cursor)?;
    let timestamps = decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
    let samples = decode_schema_varlen_scalar_samples(
        decoded.kind,
        &decoded.payload[cursor..],
        timestamps,
        projection,
    )?;

    Ok(ChunkScalarProjectionRecord {
        series_ref: decoded.series_ref,
        kind: decoded.kind,
        min_time_ms: decoded.min_time_ms,
        max_time_ms: decoded.max_time_ms,
        samples,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkRecord {
    pub series_ref: u32,
    pub kind: ChunkKind,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub samples: ChunkSamples,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkSamples {
    Float(Vec<(u64, f64)>),
    Int64(Vec<(u64, i64)>),
    Histogram(Vec<(u64, HistogramValue)>),
    ExponentialHistogram(Vec<(u64, ExponentialHistogramValue)>),
    Summary(Vec<(u64, SummaryValue)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkScalarProjection {
    Count,
    Sum,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkScalarProjectionRecord {
    pub series_ref: u32,
    pub kind: ChunkKind,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub samples: Vec<ChunkScalarSample>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkScalarSample {
    pub timestamp_ms: u64,
    pub metadata: TypedSampleMetadata,
    pub value: Option<ChunkScalarValue>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChunkScalarValue {
    Count(u64),
    Sum(f64),
}

#[derive(Debug, Clone, Copy)]
enum ScalarProjectionSchema {
    Histogram { bucket_len: usize },
    ExponentialHistogram,
    Summary { quantile_len: usize },
}

fn decode_schema_varlen_scalar_samples(
    kind: ChunkKind,
    buf: &[u8],
    timestamps: Vec<u64>,
    projection: ChunkScalarProjection,
) -> io::Result<Vec<ChunkScalarSample>> {
    let mut cursor = 0usize;
    let schemas = decode_scalar_projection_schemas(kind, buf, &mut cursor)?;
    let mut samples = Vec::with_capacity(timestamps.len());
    for timestamp_ms in timestamps {
        let schema_id = decode_varint(buf, &mut cursor)?;
        let schema_idx = usize::try_from(schema_id)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "schema id overflow"))?;
        let schema = schemas
            .get(schema_idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "schema id out of range"))?;
        let (metadata, value) =
            decode_scalar_projection_value(kind, *schema, buf, &mut cursor, projection)?;
        samples.push(ChunkScalarSample {
            timestamp_ms,
            metadata,
            value,
        });
    }
    if cursor != buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "value buffer has trailing bytes",
        ));
    }
    Ok(samples)
}

fn decode_scalar_projection_schemas(
    kind: ChunkKind,
    buf: &[u8],
    cursor: &mut usize,
) -> io::Result<Vec<ScalarProjectionSchema>> {
    let schema_count = decode_len(buf, cursor)?;
    let mut schemas = Vec::with_capacity(schema_count);
    for _ in 0..schema_count {
        let len = decode_len(buf, cursor)?;
        if (*cursor).saturating_add(len) > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "schema buffer truncated",
            ));
        }
        let schema_buf = &buf[*cursor..*cursor + len];
        *cursor = (*cursor).saturating_add(len);
        schemas.push(decode_scalar_projection_schema(kind, schema_buf)?);
    }
    Ok(schemas)
}

fn decode_scalar_projection_schema(
    kind: ChunkKind,
    schema_buf: &[u8],
) -> io::Result<ScalarProjectionSchema> {
    let mut cursor = 0usize;
    let schema = match kind {
        ChunkKind::Histogram => {
            let bounds_len = decode_len(schema_buf, &mut cursor)?;
            skip_f64s(schema_buf, &mut cursor, bounds_len)?;
            let bucket_len = decode_len(schema_buf, &mut cursor)?;
            ScalarProjectionSchema::Histogram { bucket_len }
        }
        ChunkKind::ExponentialHistogram => {
            let _scale = decode_i32(schema_buf, &mut cursor)?;
            let _zero_threshold = read_f64(schema_buf, &mut cursor)?;
            ScalarProjectionSchema::ExponentialHistogram
        }
        ChunkKind::Summary => {
            let quantile_len = decode_len(schema_buf, &mut cursor)?;
            skip_f64s(schema_buf, &mut cursor, quantile_len)?;
            ScalarProjectionSchema::Summary { quantile_len }
        }
        ChunkKind::Float | ChunkKind::Int64 => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scalar projection schema requires typed chunk kind",
            ));
        }
    };
    if cursor != schema_buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "schema buffer has trailing bytes",
        ));
    }
    Ok(schema)
}

fn decode_scalar_projection_value(
    kind: ChunkKind,
    schema: ScalarProjectionSchema,
    buf: &[u8],
    cursor: &mut usize,
    projection: ChunkScalarProjection,
) -> io::Result<(TypedSampleMetadata, Option<ChunkScalarValue>)> {
    let metadata = decode_typed_metadata(buf, cursor)?;
    let value = match (kind, schema) {
        (ChunkKind::Histogram, ScalarProjectionSchema::Histogram { bucket_len }) => {
            let count = decode_varint(buf, cursor)?;
            let sum = decode_opt_f64(buf, cursor)?;
            let _min = decode_opt_f64(buf, cursor)?;
            let _max = decode_opt_f64(buf, cursor)?;
            skip_varints(buf, cursor, bucket_len)?;
            match projection {
                ChunkScalarProjection::Count => Some(ChunkScalarValue::Count(count)),
                ChunkScalarProjection::Sum => sum.map(ChunkScalarValue::Sum),
            }
        }
        (ChunkKind::ExponentialHistogram, ScalarProjectionSchema::ExponentialHistogram) => {
            let count = decode_varint(buf, cursor)?;
            let sum = decode_opt_f64(buf, cursor)?;
            let _min = decode_opt_f64(buf, cursor)?;
            let _max = decode_opt_f64(buf, cursor)?;
            let _zero_count = decode_varint(buf, cursor)?;
            skip_exponential_histogram_buckets(buf, cursor)?;
            skip_exponential_histogram_buckets(buf, cursor)?;
            match projection {
                ChunkScalarProjection::Count => Some(ChunkScalarValue::Count(count)),
                ChunkScalarProjection::Sum => sum.map(ChunkScalarValue::Sum),
            }
        }
        (ChunkKind::Summary, ScalarProjectionSchema::Summary { quantile_len }) => {
            let count = decode_varint(buf, cursor)?;
            let sum = read_f64(buf, cursor)?;
            skip_f64s(buf, cursor, quantile_len)?;
            match projection {
                ChunkScalarProjection::Count => Some(ChunkScalarValue::Count(count)),
                ChunkScalarProjection::Sum => Some(ChunkScalarValue::Sum(sum)),
            }
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "scalar projection schema kind mismatch",
            ));
        }
    };
    Ok((metadata, value))
}

fn decode_len(buf: &[u8], cursor: &mut usize) -> io::Result<usize> {
    let len = decode_varint(buf, cursor)?;
    usize::try_from(len).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length overflow"))
}

fn decode_i32(buf: &[u8], cursor: &mut usize) -> io::Result<i32> {
    let encoded = decode_varint(buf, cursor)?;
    let decoded = decode_zigzag_i64(encoded);
    i32::try_from(decoded).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "i32 overflow"))
}

fn skip_varints(buf: &[u8], cursor: &mut usize, count: usize) -> io::Result<()> {
    for _ in 0..count {
        let _ = decode_varint(buf, cursor)?;
    }
    Ok(())
}

fn skip_f64s(buf: &[u8], cursor: &mut usize, count: usize) -> io::Result<()> {
    for _ in 0..count {
        let _ = read_f64(buf, cursor)?;
    }
    Ok(())
}

fn skip_exponential_histogram_buckets(buf: &[u8], cursor: &mut usize) -> io::Result<()> {
    let _offset = decode_i32(buf, cursor)?;
    let len = decode_len(buf, cursor)?;
    skip_varints(buf, cursor, len)
}

fn decode_timestamps(
    buf: &[u8],
    cursor: &mut usize,
    t0_ms: u64,
    num_points: u32,
) -> io::Result<Vec<u64>> {
    let mut timestamps = Vec::with_capacity(num_points as usize);
    for _ in 0..num_points {
        let dt = decode_varint(buf, cursor)?;
        timestamps.push(t0_ms.saturating_add(dt));
    }
    Ok(timestamps)
}

fn read_u64(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    if *cursor + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_f64(buf: &[u8], cursor: &mut usize) -> io::Result<f64> {
    if *cursor + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = f64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_i64(buf: &[u8], cursor: &mut usize) -> io::Result<i64> {
    if *cursor + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = i64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_exact_u16(file: &mut File) -> io::Result<u16> {
    let mut buf = [0u8; 2];
    file.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_exact_u32(file: &mut File) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_exact_u64(file: &mut File) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn write_chunk_entry(writer: &mut impl Write, entry: &ChunkIndexEntry) -> io::Result<()> {
    let mut buf = [0u8; CHUNK_ENTRY_LEN];
    buf[0] = entry.file_id;
    buf[1] = entry.kind as u8;
    buf[2..4].copy_from_slice(&entry.flags.to_le_bytes());
    buf[4..12].copy_from_slice(&entry.min_time_ms.to_le_bytes());
    buf[12..20].copy_from_slice(&entry.max_time_ms.to_le_bytes());
    buf[20..28].copy_from_slice(&entry.offset.to_le_bytes());
    buf[28..32].copy_from_slice(&entry.length.to_le_bytes());
    buf[32..36].copy_from_slice(&entry.reserved0.to_le_bytes());
    buf[36..40].copy_from_slice(&entry.reserved1.to_le_bytes());
    writer.write_all(&buf)
}

fn read_chunk_entry(reader: &mut impl Read) -> io::Result<ChunkIndexEntry> {
    let mut buf = [0u8; CHUNK_ENTRY_LEN];
    reader.read_exact(&mut buf)?;

    let file_id = buf[0];
    let kind_raw = buf[1];
    let kind = chunk_kind_from_u8(kind_raw)?;
    let flags = u16::from_le_bytes(buf[2..4].try_into().unwrap());
    let min_time_ms = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let max_time_ms = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let offset = u64::from_le_bytes(buf[20..28].try_into().unwrap());
    let length = u32::from_le_bytes(buf[28..32].try_into().unwrap());
    let reserved0 = u32::from_le_bytes(buf[32..36].try_into().unwrap());
    let reserved1 = u32::from_le_bytes(buf[36..40].try_into().unwrap());

    Ok(ChunkIndexEntry {
        file_id,
        kind,
        flags,
        min_time_ms,
        max_time_ms,
        offset,
        length,
        reserved0,
        reserved1,
    })
}

fn chunk_encoding_from_u8(value: u8) -> io::Result<ChunkEncoding> {
    match value {
        x if x == ChunkEncoding::SchemaVarLen as u8 => Ok(ChunkEncoding::SchemaVarLen),
        x if x == ChunkEncoding::RawF64 as u8 => Ok(ChunkEncoding::RawF64),
        x if x == ChunkEncoding::Gorilla as u8 => Ok(ChunkEncoding::Gorilla),
        x if x == ChunkEncoding::IntDeltaZigZag as u8 => Ok(ChunkEncoding::IntDeltaZigZag),
        x if x == ChunkEncoding::RawI64 as u8 => Ok(ChunkEncoding::RawI64),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown chunk encoding",
        )),
    }
}

fn chunk_kind_from_u8(value: u8) -> io::Result<ChunkKind> {
    match value {
        x if x == ChunkKind::Float as u8 => Ok(ChunkKind::Float),
        x if x == ChunkKind::Int64 as u8 => Ok(ChunkKind::Int64),
        x if x == ChunkKind::Histogram as u8 => Ok(ChunkKind::Histogram),
        x if x == ChunkKind::ExponentialHistogram as u8 => Ok(ChunkKind::ExponentialHistogram),
        x if x == ChunkKind::Summary as u8 => Ok(ChunkKind::Summary),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown chunk kind",
        )),
    }
}

fn chunk_entry_len() -> usize {
    CHUNK_ENTRY_LEN
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::head::{
        CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
        OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality, SummaryQuantileValue,
        SummaryValue, TypedSampleMetadata,
    };
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;

    #[derive(Default)]
    struct CountingWriter {
        bytes: Vec<u8>,
        write_calls: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.write_calls += 1;
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn chunk_writer_roundtrip_single_sample() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

        let entry = writer.append_float_sample(7, 10_000, 42.5).unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.min_time_ms, 10_000);
        assert_eq!(entry.max_time_ms, 10_000);
        assert!(entry.length > 0);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 7);
        assert_eq!(record.samples, ChunkSamples::Float(vec![(10_000, 42.5)]));
    }

    #[test]
    fn chunk_writer_roundtrip_multiple_samples() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

        let entry = writer
            .append_float_chunk(
                3,
                &[(12_000, 1.25), (10_000, 1.0), (10_000, 1.0), (14_000, 2.5)],
            )
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.min_time_ms, 10_000);
        assert_eq!(entry.max_time_ms, 14_000);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 3);
        assert_eq!(
            record.samples,
            ChunkSamples::Float(vec![
                (10_000, 1.0),
                (10_000, 1.0),
                (12_000, 1.25),
                (14_000, 2.5)
            ])
        );
    }

    #[test]
    fn chunk_writer_ordered_float_samples_roundtrip_without_resorting() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

        let entry = writer
            .append_float_chunk_ordered(3, &[(10_000, 1.0), (12_000, 1.25), (14_000, 2.5)])
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.min_time_ms, 10_000);
        assert_eq!(entry.max_time_ms, 14_000);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 3);
        assert_eq!(
            record.samples,
            ChunkSamples::Float(vec![(10_000, 1.0), (12_000, 1.25), (14_000, 2.5)])
        );
    }

    #[test]
    fn chunk_writer_ordered_float_samples_reject_unsorted_input() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

        let err = writer
            .append_float_chunk_ordered(3, &[(12_000, 1.25), (10_000, 1.0)])
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn chunk_writer_roundtrip_float_samples_raw() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

        let entry = writer
            .append_float_chunk_raw(3, &[(12_000, 1.25), (10_000, 1.0), (14_000, 2.5)])
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.min_time_ms, 10_000);
        assert_eq!(entry.max_time_ms, 14_000);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 3);
        assert_eq!(
            record.samples,
            ChunkSamples::Float(vec![(10_000, 1.0), (12_000, 1.25), (14_000, 2.5)])
        );
    }

    #[test]
    fn chunk_writer_roundtrip_int_samples() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

        let entry = writer
            .append_int_chunk(9, &[(10_000, 5), (10_500, -2), (11_000, 10)])
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.min_time_ms, 10_000);
        assert_eq!(entry.max_time_ms, 11_000);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 9);
        assert_eq!(
            record.samples,
            ChunkSamples::Int64(vec![(10_000, 5), (10_500, -2), (11_000, 10)])
        );
    }

    #[test]
    fn chunk_writer_roundtrip_int_samples_raw() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();

        let entry = writer
            .append_int_chunk_raw(9, &[(10_000, 5), (10_500, -2), (11_000, 10)])
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.min_time_ms, 10_000);
        assert_eq!(entry.max_time_ms, 11_000);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 9);
        assert_eq!(
            record.samples,
            ChunkSamples::Int64(vec![(10_000, 5), (10_500, -2), (11_000, 10)])
        );
    }

    #[test]
    fn chunk_writer_roundtrip_histogram_samples() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
        let first = HistogramValue {
            count: 4,
            sum: Some(10.0),
            min: Some(1.0),
            max: Some(4.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0],
            bucket_counts: vec![1, 2, 1],
        };
        let second = HistogramValue {
            count: 7,
            sum: Some(21.0),
            min: Some(1.0),
            max: Some(6.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0],
            bucket_counts: vec![2, 3, 2],
        };

        let entry = writer
            .append_histogram_chunk_ordered(4, &[(10_000, first.clone()), (12_000, second.clone())])
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.kind, ChunkKind::Histogram);
        assert_eq!(entry.min_time_ms, 10_000);
        assert_eq!(entry.max_time_ms, 12_000);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 4);
        assert_eq!(record.kind, ChunkKind::Histogram);
        assert_eq!(
            record.samples,
            ChunkSamples::Histogram(vec![(10_000, first), (12_000, second)])
        );
    }

    #[test]
    fn chunk_reader_decodes_histogram_scalar_projections_without_full_values() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
        let first = HistogramValue {
            count: 4,
            sum: Some(10.0),
            min: Some(1.0),
            max: Some(4.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0, 5.0, 10.0],
            bucket_counts: vec![1, 2, 1, 0],
        };
        let second_metadata = TypedSampleMetadata {
            start_time_ms: Some(11_000),
            flags: 0,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::NotCounterReset,
        };
        let second = HistogramValue {
            count: 7,
            sum: Some(21.0),
            min: Some(1.0),
            max: Some(6.0),
            metadata: second_metadata,
            explicit_bounds: vec![1.0, 5.0, 10.0],
            bucket_counts: vec![2, 3, 2, 0],
        };

        let entry = writer
            .append_histogram_chunk_ordered(4, &[(10_000, first.clone()), (12_000, second)])
            .unwrap();
        writer.flush().unwrap();

        let mut file = temp.reopen().unwrap();
        let count = read_chunk_scalar_projection_at(
            &mut file,
            entry.offset,
            entry.length,
            ChunkScalarProjection::Count,
        )
        .unwrap();
        assert_eq!(count.series_ref, 4);
        assert_eq!(count.kind, ChunkKind::Histogram);
        assert_eq!(
            count.samples,
            vec![
                ChunkScalarSample {
                    timestamp_ms: 10_000,
                    metadata: TypedSampleMetadata::default(),
                    value: Some(ChunkScalarValue::Count(4)),
                },
                ChunkScalarSample {
                    timestamp_ms: 12_000,
                    metadata: second_metadata,
                    value: Some(ChunkScalarValue::Count(7)),
                },
            ]
        );

        let mut file = temp.reopen().unwrap();
        let sum = read_chunk_scalar_projection_at(
            &mut file,
            entry.offset,
            entry.length,
            ChunkScalarProjection::Sum,
        )
        .unwrap();
        assert_eq!(
            sum.samples,
            vec![
                ChunkScalarSample {
                    timestamp_ms: 10_000,
                    metadata: TypedSampleMetadata::default(),
                    value: Some(ChunkScalarValue::Sum(10.0)),
                },
                ChunkScalarSample {
                    timestamp_ms: 12_000,
                    metadata: second_metadata,
                    value: Some(ChunkScalarValue::Sum(21.0)),
                },
            ]
        );
    }

    #[test]
    fn chunk_writer_roundtrip_exponential_histogram_samples() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
        let first = ExponentialHistogramValue {
            count: 6,
            sum: Some(15.0),
            min: Some(1.0),
            max: Some(8.0),
            scale: 2,
            zero_threshold: 0.125,
            zero_count: 1,
            metadata: TypedSampleMetadata {
                start_time_ms: Some(9_000),
                flags: OTLP_FLAG_NO_RECORDED_VALUE,
                temporality: OtlpAggregationTemporality::Delta,
                reset_hint: CounterResetHint::NotCounterReset,
            },
            positive: ExponentialHistogramBuckets {
                offset: -1,
                counts: vec![2, 3],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![0],
            },
        };
        let second = ExponentialHistogramValue {
            count: 9,
            sum: Some(27.0),
            min: Some(1.0),
            max: Some(10.0),
            scale: 2,
            zero_threshold: 0.125,
            zero_count: 2,
            metadata: TypedSampleMetadata {
                start_time_ms: Some(10_000),
                flags: 0,
                temporality: OtlpAggregationTemporality::Delta,
                reset_hint: CounterResetHint::CounterReset,
            },
            positive: ExponentialHistogramBuckets {
                offset: -1,
                counts: vec![3, 4],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![0],
            },
        };

        let entry = writer
            .append_exponential_histogram_chunk_ordered(
                5,
                &[(10_000, first.clone()), (12_000, second.clone())],
            )
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.kind, ChunkKind::ExponentialHistogram);
        assert!(entry.flags & CHUNK_FLAG_HAS_START_TIME != 0);
        assert!(entry.flags & CHUNK_FLAG_HAS_PER_SAMPLE_FLAGS != 0);
        assert!(entry.flags & CHUNK_FLAG_HAS_COUNTER_RESET_HINTS != 0);
        assert!(entry.flags & CHUNK_FLAG_TEMPORALITY_DELTA != 0);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 5);
        assert_eq!(record.kind, ChunkKind::ExponentialHistogram);
        assert_eq!(
            record.samples,
            ChunkSamples::ExponentialHistogram(vec![(10_000, first), (12_000, second)])
        );
    }

    #[test]
    fn chunk_reader_decodes_exponential_histogram_scalar_projections_without_full_values() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
        let metadata = TypedSampleMetadata {
            start_time_ms: Some(9_000),
            flags: 0,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::CounterReset,
        };
        let sample = ExponentialHistogramValue {
            count: 6,
            sum: Some(15.0),
            min: Some(1.0),
            max: Some(8.0),
            scale: 2,
            zero_threshold: 0.125,
            zero_count: 1,
            metadata,
            positive: ExponentialHistogramBuckets {
                offset: -1,
                counts: vec![2, 3],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![0],
            },
        };

        let entry = writer
            .append_exponential_histogram_chunk_ordered(5, &[(10_000, sample)])
            .unwrap();
        writer.flush().unwrap();

        let mut file = temp.reopen().unwrap();
        let count = read_chunk_scalar_projection_at(
            &mut file,
            entry.offset,
            entry.length,
            ChunkScalarProjection::Count,
        )
        .unwrap();
        assert_eq!(
            count.samples,
            vec![ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata,
                value: Some(ChunkScalarValue::Count(6)),
            }]
        );

        let mut file = temp.reopen().unwrap();
        let sum = read_chunk_scalar_projection_at(
            &mut file,
            entry.offset,
            entry.length,
            ChunkScalarProjection::Sum,
        )
        .unwrap();
        assert_eq!(
            sum.samples,
            vec![ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata,
                value: Some(ChunkScalarValue::Sum(15.0)),
            }]
        );
    }

    #[test]
    fn chunk_writer_roundtrip_summary_samples() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
        let first = SummaryValue {
            count: 10,
            sum: 50.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![
                SummaryQuantileValue {
                    quantile: 0.5,
                    value: 4.0,
                },
                SummaryQuantileValue {
                    quantile: 0.9,
                    value: 8.0,
                },
            ],
        };
        let second = SummaryValue {
            count: 12,
            sum: 66.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![
                SummaryQuantileValue {
                    quantile: 0.5,
                    value: 5.0,
                },
                SummaryQuantileValue {
                    quantile: 0.9,
                    value: 9.0,
                },
            ],
        };

        let entry = writer
            .append_summary_chunk_ordered(6, &[(10_000, first.clone()), (12_000, second.clone())])
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(entry.kind, ChunkKind::Summary);

        let mut file = temp.reopen().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = ChunkReader::new(file);
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.series_ref, 6);
        assert_eq!(record.kind, ChunkKind::Summary);
        assert_eq!(
            record.samples,
            ChunkSamples::Summary(vec![(10_000, first), (12_000, second)])
        );
    }

    #[test]
    fn chunk_reader_decodes_summary_scalar_projections_without_full_values() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ChunkWriter::new(temp.reopen().unwrap()).unwrap();
        let metadata = TypedSampleMetadata::default();
        let sample = SummaryValue {
            count: 10,
            sum: 50.0,
            metadata,
            quantiles: vec![
                SummaryQuantileValue {
                    quantile: 0.5,
                    value: 4.0,
                },
                SummaryQuantileValue {
                    quantile: 0.9,
                    value: 8.0,
                },
            ],
        };

        let entry = writer
            .append_summary_chunk_ordered(6, &[(10_000, sample)])
            .unwrap();
        writer.flush().unwrap();

        let mut file = temp.reopen().unwrap();
        let count = read_chunk_scalar_projection_at(
            &mut file,
            entry.offset,
            entry.length,
            ChunkScalarProjection::Count,
        )
        .unwrap();
        assert_eq!(
            count.samples,
            vec![ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata,
                value: Some(ChunkScalarValue::Count(10)),
            }]
        );

        let mut file = temp.reopen().unwrap();
        let sum = read_chunk_scalar_projection_at(
            &mut file,
            entry.offset,
            entry.length,
            ChunkScalarProjection::Sum,
        )
        .unwrap();
        assert_eq!(
            sum.samples,
            vec![ChunkScalarSample {
                timestamp_ms: 10_000,
                metadata,
                value: Some(ChunkScalarValue::Sum(50.0)),
            }]
        );
    }

    #[test]
    fn chunk_index_writer_writes_offsets() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut entries = Vec::new();
        entries.push(vec![ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 1,
            max_time_ms: 2,
            offset: 10,
            length: 20,
            reserved0: 0,
            reserved1: 0,
        }]);
        entries.push(Vec::new());

        let mut file = temp.reopen().unwrap();
        write_chunk_index(&mut file, &entries).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut header = [0u8; 4];
        file.read_exact(&mut header).unwrap();
        assert_eq!(u32::from_le_bytes(header), CHUNK_INDEX_MAGIC);
    }

    #[test]
    fn chunk_index_reader_reads_target_offsets_lazily() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let num_series = 100_000u32;
        let series_ref = 42usize;
        let offset_table_start = 4 + 2 + 2 + 4;
        let data_start = offset_table_start as u64 + (u64::from(num_series) + 1) * 8;
        let entry = ChunkIndexEntry {
            file_id: 0,
            kind: ChunkKind::Float,
            flags: 0,
            min_time_ms: 100,
            max_time_ms: 200,
            offset: 10,
            length: 20,
            reserved0: 0,
            reserved1: 0,
        };

        let mut file = temp.reopen().unwrap();
        file.write_all(&CHUNK_INDEX_MAGIC.to_le_bytes()).unwrap();
        file.write_all(&1u16.to_le_bytes()).unwrap();
        file.write_all(&0u16.to_le_bytes()).unwrap();
        file.write_all(&num_series.to_le_bytes()).unwrap();
        file.write_all(&data_start.to_le_bytes()).unwrap();
        file.seek(SeekFrom::Start(
            offset_table_start as u64 + (series_ref as u64) * 8,
        ))
        .unwrap();
        file.write_all(&data_start.to_le_bytes()).unwrap();
        file.write_all(&(data_start + chunk_entry_len() as u64).to_le_bytes())
            .unwrap();
        file.seek(SeekFrom::Start(data_start)).unwrap();
        write_chunk_entry(&mut file, &entry).unwrap();
        file.flush().unwrap();

        let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();
        let entries = reader.read_entries(series_ref as u32).unwrap().unwrap();

        assert_eq!(entries, vec![entry]);
    }

    #[test]
    fn chunk_index_writer_buffers_underlying_writes() {
        let entries = vec![
            vec![ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Float,
                flags: 0,
                min_time_ms: 100,
                max_time_ms: 200,
                offset: 10,
                length: 20,
                reserved0: 0,
                reserved1: 0,
            }],
            vec![ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Int64,
                flags: 0,
                min_time_ms: 300,
                max_time_ms: 400,
                offset: 30,
                length: 40,
                reserved0: 0,
                reserved1: 0,
            }],
        ];
        let mut writer = CountingWriter::default();

        write_chunk_index(&mut writer, &entries).unwrap();

        assert!(
            writer.write_calls <= 2,
            "chunk index writer used {} underlying writes",
            writer.write_calls
        );
        let mut cursor = std::io::Cursor::new(writer.bytes);
        let mut magic = [0u8; 4];
        cursor.read_exact(&mut magic).unwrap();
        assert_eq!(u32::from_le_bytes(magic), CHUNK_INDEX_MAGIC);
    }

    #[test]
    fn chunk_index_roundtrips_entries() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let entries = vec![
            vec![
                ChunkIndexEntry {
                    file_id: 0,
                    kind: ChunkKind::Float,
                    flags: 0,
                    min_time_ms: 100,
                    max_time_ms: 200,
                    offset: 10,
                    length: 20,
                    reserved0: 0,
                    reserved1: 0,
                },
                ChunkIndexEntry {
                    file_id: 0,
                    kind: ChunkKind::Int64,
                    flags: 1,
                    min_time_ms: 300,
                    max_time_ms: 400,
                    offset: 30,
                    length: 40,
                    reserved0: 0,
                    reserved1: 0,
                },
            ],
            Vec::new(),
        ];

        let mut file = temp.reopen().unwrap();
        write_chunk_index(&mut file, &entries).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let read = read_chunk_index(&mut file).unwrap();
        assert_eq!(read, entries);
    }

    #[test]
    fn chunk_index_reader_fetches_entries_for_one_series() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let entries = vec![
            vec![ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Float,
                flags: 0,
                min_time_ms: 100,
                max_time_ms: 200,
                offset: 10,
                length: 20,
                reserved0: 0,
                reserved1: 0,
            }],
            vec![
                ChunkIndexEntry {
                    file_id: 0,
                    kind: ChunkKind::Int64,
                    flags: 1,
                    min_time_ms: 300,
                    max_time_ms: 400,
                    offset: 30,
                    length: 40,
                    reserved0: 0,
                    reserved1: 0,
                },
                ChunkIndexEntry {
                    file_id: 0,
                    kind: ChunkKind::Float,
                    flags: 0,
                    min_time_ms: 500,
                    max_time_ms: 600,
                    offset: 70,
                    length: 80,
                    reserved0: 0,
                    reserved1: 0,
                },
            ],
        ];

        let mut file = temp.reopen().unwrap();
        write_chunk_index(&mut file, &entries).unwrap();
        let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();

        assert_eq!(reader.len(), 2);
        assert_eq!(reader.read_entries(1).unwrap(), Some(entries[1].clone()));
        assert_eq!(reader.read_entries(99).unwrap(), None);
    }

    #[test]
    fn chunk_index_reader_streams_series_entries_in_order() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let entries = vec![
            vec![ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Float,
                flags: 0,
                min_time_ms: 100,
                max_time_ms: 200,
                offset: 10,
                length: 20,
                reserved0: 0,
                reserved1: 0,
            }],
            Vec::new(),
            vec![
                ChunkIndexEntry {
                    file_id: 0,
                    kind: ChunkKind::Histogram,
                    flags: 1,
                    min_time_ms: 300,
                    max_time_ms: 400,
                    offset: 30,
                    length: 40,
                    reserved0: 0,
                    reserved1: 0,
                },
                ChunkIndexEntry {
                    file_id: 0,
                    kind: ChunkKind::Summary,
                    flags: 2,
                    min_time_ms: 500,
                    max_time_ms: 600,
                    offset: 70,
                    length: 80,
                    reserved0: 0,
                    reserved1: 0,
                },
            ],
        ];

        let mut file = temp.reopen().unwrap();
        write_chunk_index(&mut file, &entries).unwrap();
        let mut reader = ChunkIndexReader::open(temp.reopen().unwrap()).unwrap();
        let mut streamed = Vec::new();

        reader
            .for_each_series_entries(|series_ref, series_entries| {
                streamed.push((series_ref, series_entries.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            streamed,
            vec![
                (0, entries[0].clone()),
                (1, entries[1].clone()),
                (2, entries[2].clone()),
            ]
        );
    }
}
