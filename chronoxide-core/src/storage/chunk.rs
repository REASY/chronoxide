use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use crc32c::crc32c;

use crate::storage::encoding::{
    decode_gorilla_values, decode_varint, decode_zigzag_i64, encode_gorilla_values, encode_varint,
    encode_zigzag_i64,
};

const FRAME_HEADER_LEN: usize = 14;
const CHUNK_HEADER_LEN: usize = 40;
const CHUNK_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"CHIX");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkKind {
    Float = 0,
    Int64 = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkEncoding {
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
    file: File,
    offset: u64,
}

impl ChunkWriter {
    pub fn new(file: File) -> io::Result<Self> {
        let offset = file.metadata()?.len();
        Ok(Self { file, offset })
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

        let min_time_ms = sorted.first().unwrap().0;
        let max_time_ms = sorted.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut dt_buf = Vec::new();
        let mut values = Vec::with_capacity(sorted.len());
        for (ts, value) in &sorted {
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
        chunk_header.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
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

        let min_time_ms = sorted.first().unwrap().0;
        let max_time_ms = sorted.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut payload = Vec::new();
        payload.extend_from_slice(&t0_ms.to_le_bytes());
        for (ts, value) in &sorted {
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
        chunk_header.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
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

        let min_time_ms = sorted.first().unwrap().0;
        let max_time_ms = sorted.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut dt_buf = Vec::new();
        let mut value_buf = Vec::new();
        let mut prev = 0i64;
        for (ts, value) in &sorted {
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
        chunk_header.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
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

        let min_time_ms = sorted.first().unwrap().0;
        let max_time_ms = sorted.last().unwrap().0;
        let t0_ms = min_time_ms;

        let mut payload = Vec::new();
        payload.extend_from_slice(&t0_ms.to_le_bytes());
        for (ts, value) in &sorted {
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
        chunk_header.extend_from_slice(&(sorted.len() as u32).to_le_bytes());
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

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub fn write_chunk_index(file: &mut File, entries: &[Vec<ChunkIndexEntry>]) -> io::Result<()> {
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

    file.write_all(&CHUNK_INDEX_MAGIC.to_le_bytes())?;
    file.write_all(&1u16.to_le_bytes())?;
    file.write_all(&0u16.to_le_bytes())?;
    file.write_all(&num_series.to_le_bytes())?;
    for offset in offsets {
        file.write_all(&offset.to_le_bytes())?;
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
            write_chunk_entry(file, &entry)?;
        }
    }
    Ok(())
}

pub fn read_chunk_index(file: &mut File) -> io::Result<Vec<Vec<ChunkIndexEntry>>> {
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
    let expected_start = (4 + 2 + 2 + 4 + offsets_len) as u64;

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

    let entry_len = chunk_entry_len() as u64;
    let mut entries = Vec::with_capacity(num_series);
    for i in 0..num_series {
        let start = offsets[i];
        let end = offsets[i + 1];
        if end < start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk index offsets out of order",
            ));
        }
        let len = end - start;
        if len % entry_len != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk index entry length misaligned",
            ));
        }
        let count = (len / entry_len) as usize;
        file.seek(SeekFrom::Start(start))?;
        let mut series_entries = Vec::with_capacity(count);
        for _ in 0..count {
            series_entries.push(read_chunk_entry(file)?);
        }
        entries.push(series_entries);
    }

    Ok(entries)
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

        let mut cursor = 0usize;
        let t0_ms = read_u64(chunk_payload, &mut cursor)?;

        let samples = match kind {
            ChunkKind::Float => match encoding {
                ChunkEncoding::RawF64 => {
                    let mut samples = Vec::with_capacity(num_points as usize);
                    for _ in 0..num_points {
                        let dt = decode_varint(chunk_payload, &mut cursor)?;
                        let value = read_f64(chunk_payload, &mut cursor)?;
                        samples.push((t0_ms.saturating_add(dt), value));
                    }
                    ChunkSamples::Float(samples)
                }
                ChunkEncoding::Gorilla => {
                    let mut timestamps = Vec::with_capacity(num_points as usize);
                    for _ in 0..num_points {
                        let dt = decode_varint(chunk_payload, &mut cursor)?;
                        timestamps.push(t0_ms.saturating_add(dt));
                    }
                    let values =
                        decode_gorilla_values(&chunk_payload[cursor..], num_points as usize)?;
                    let mut samples = Vec::with_capacity(num_points as usize);
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
            ChunkKind::Int64 => match encoding {
                ChunkEncoding::IntDeltaZigZag => {
                    let mut timestamps = Vec::with_capacity(num_points as usize);
                    for _ in 0..num_points {
                        let dt = decode_varint(chunk_payload, &mut cursor)?;
                        timestamps.push(t0_ms.saturating_add(dt));
                    }
                    let mut values = Vec::with_capacity(num_points as usize);
                    let mut prev = 0i64;
                    for _ in 0..num_points {
                        let encoded = decode_varint(chunk_payload, &mut cursor)?;
                        let delta = decode_zigzag_i64(encoded);
                        let value = prev.wrapping_add(delta);
                        values.push(value);
                        prev = value;
                    }
                    let mut samples = Vec::with_capacity(num_points as usize);
                    for (ts, value) in timestamps.into_iter().zip(values.into_iter()) {
                        samples.push((ts, value));
                    }
                    ChunkSamples::Int64(samples)
                }
                ChunkEncoding::RawI64 => {
                    let mut samples = Vec::with_capacity(num_points as usize);
                    for _ in 0..num_points {
                        let dt = decode_varint(chunk_payload, &mut cursor)?;
                        let value = read_i64(chunk_payload, &mut cursor)?;
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
        };

        Ok(Some(ChunkRecord {
            series_ref,
            kind,
            min_time_ms,
            max_time_ms,
            samples,
        }))
    }
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

fn read_exact_u8(file: &mut File) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf)?;
    Ok(buf[0])
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

fn write_chunk_entry(file: &mut File, entry: &ChunkIndexEntry) -> io::Result<()> {
    file.write_all(&[entry.file_id])?;
    file.write_all(&[entry.kind as u8])?;
    file.write_all(&entry.flags.to_le_bytes())?;
    file.write_all(&entry.min_time_ms.to_le_bytes())?;
    file.write_all(&entry.max_time_ms.to_le_bytes())?;
    file.write_all(&entry.offset.to_le_bytes())?;
    file.write_all(&entry.length.to_le_bytes())?;
    file.write_all(&entry.reserved0.to_le_bytes())?;
    file.write_all(&entry.reserved1.to_le_bytes())?;
    Ok(())
}

fn read_chunk_entry(file: &mut File) -> io::Result<ChunkIndexEntry> {
    let file_id = read_exact_u8(file)?;
    let kind_raw = read_exact_u8(file)?;
    let kind = chunk_kind_from_u8(kind_raw)?;
    let flags = read_exact_u16(file)?;
    let min_time_ms = read_exact_u64(file)?;
    let max_time_ms = read_exact_u64(file)?;
    let offset = read_exact_u64(file)?;
    let length = read_exact_u32(file)?;
    let reserved0 = read_exact_u32(file)?;
    let reserved1 = read_exact_u32(file)?;

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
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown chunk kind",
        )),
    }
}

fn chunk_entry_len() -> usize {
    1 + 1 + 2 + 8 + 8 + 8 + 4 + 4 + 4
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Seek;
    use std::io::SeekFrom;

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
}
