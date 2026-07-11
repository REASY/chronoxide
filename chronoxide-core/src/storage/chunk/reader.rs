use super::*;

pub struct ChunkReader {
    file: File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPayloadRead {
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone)]
pub struct ChunkPayloadBatch {
    spans: Vec<ChunkPayloadSpan>,
    physical_bytes_read: u64,
}

#[derive(Debug, Clone)]
struct ChunkPayloadSpan {
    offset: u64,
    bytes: Vec<u8>,
}

impl ChunkPayloadBatch {
    pub fn empty() -> Self {
        Self {
            spans: Vec::new(),
            physical_bytes_read: 0,
        }
    }

    pub fn physical_read_count(&self) -> u64 {
        self.spans.len() as u64
    }

    pub fn physical_bytes_read(&self) -> u64 {
        self.physical_bytes_read
    }

    pub fn decode_chunk_record(&self, offset: u64, length: u32) -> io::Result<ChunkRecord> {
        decode_chunk_record(self.slice(offset, u64::from(length))?)
    }

    pub fn decode_indexed_scalar_projection(
        &self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
    ) -> io::Result<(ChunkScalarProjectionRecord, u32)> {
        let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
            let record = decode_chunk_scalar_projection(
                self.slice(entry.offset, u64::from(entry.length))?,
                projection,
            )?;
            return Ok((record, entry.length));
        };

        let read_len = entry.scalar_projection_read_len();
        let buf = self.slice(entry.offset, u64::from(read_len))?;
        let decoded = decode_chunk_header(buf)?;
        let lane_start = lane_offset as usize;
        let lane_end = lane_start.saturating_add(lane_len as usize);
        if lane_end > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk scalar lane range exceeds projected read",
            ));
        }
        let lane = &buf[lane_start..lane_end];
        let record = decode_typed_scalar_lane(&decoded, lane, projection)?;
        Ok((record, read_len))
    }

    pub fn for_each_indexed_scalar_projection_sample<F>(
        &self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
        on_sample: F,
    ) -> io::Result<u32>
    where
        F: FnMut(ChunkScalarSample) -> io::Result<()>,
    {
        self.for_each_indexed_scalar_projection_sample_with_header(entry, projection, on_sample)
            .map(|(_, read_len)| read_len)
    }

    /// Parses the declared header for a dedicated scalar lane without walking its rows.
    /// Lane contents are validated by the callback decoder before it returns success.
    pub(crate) fn indexed_scalar_projection_header(
        &self,
        entry: &ChunkIndexEntry,
    ) -> io::Result<(ChunkScalarRecordHeader, u32)> {
        let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "indexed scalar projection has no dedicated lane",
            ));
        };
        let read_len = entry.scalar_projection_read_len();
        let buf = self.slice(entry.offset, u64::from(read_len))?;
        let decoded = decode_indexed_scalar_projection_header(buf, entry)?;
        validate_scalar_lane_slice(buf, lane_offset, lane_len)?;
        Ok((decoded.scalar_record_header(), read_len))
    }

    pub(crate) fn for_each_indexed_scalar_projection_sample_with_header<F>(
        &self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
        mut on_sample: F,
    ) -> io::Result<(ChunkScalarRecordHeader, u32)>
    where
        F: FnMut(ChunkScalarSample) -> io::Result<()>,
    {
        let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
            let record = decode_chunk_scalar_projection(
                self.slice(entry.offset, u64::from(entry.length))?,
                projection,
            )?;
            validate_indexed_scalar_projection_kind(entry, record.kind)?;
            let sample_count = u32::try_from(record.samples.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "decoded scalar sample count exceeds u32",
                )
            })?;
            let header = ChunkScalarRecordHeader {
                series_ref: record.series_ref,
                kind: record.kind,
                min_time_ms: record.min_time_ms,
                max_time_ms: record.max_time_ms,
                sample_count,
            };
            for sample in record.samples {
                on_sample(sample)?;
            }
            return Ok((header, entry.length));
        };

        let read_len = entry.scalar_projection_read_len();
        let buf = self.slice(entry.offset, u64::from(read_len))?;
        let decoded = decode_indexed_scalar_projection_header(buf, entry)?;
        let lane = validate_scalar_lane_slice(buf, lane_offset, lane_len)?;
        for_each_typed_scalar_lane_sample(&decoded, lane, projection, on_sample)?;
        Ok((decoded.scalar_record_header(), read_len))
    }

    fn slice(&self, offset: u64, len: u64) -> io::Result<&[u8]> {
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "chunk payload range overflows")
        })?;
        for span in &self.spans {
            let span_len = u64::try_from(span.bytes.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "chunk payload span too large")
            })?;
            let span_end = span.offset.checked_add(span_len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "chunk payload span overflows")
            })?;
            if offset >= span.offset && end <= span_end {
                let start = usize::try_from(offset - span.offset).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "chunk payload offset too large")
                })?;
                let len = usize::try_from(len).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "chunk payload length too large")
                })?;
                return Ok(&span.bytes[start..start + len]);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk payload request missing from batch",
        ))
    }
}

fn decode_indexed_scalar_projection_header(
    buf: &[u8],
    entry: &ChunkIndexEntry,
) -> io::Result<DecodedChunkHeader> {
    let decoded = decode_chunk_header(buf)?;
    validate_indexed_scalar_projection_kind(entry, decoded.scalar_record_header().kind)?;
    Ok(decoded)
}

fn validate_indexed_scalar_projection_kind(
    entry: &ChunkIndexEntry,
    decoded_kind: ChunkKind,
) -> io::Result<()> {
    if decoded_kind != entry.kind {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index kind does not match chunk header",
        ));
    }
    Ok(())
}

fn validate_scalar_lane_slice(buf: &[u8], lane_offset: u32, lane_len: u32) -> io::Result<&[u8]> {
    let lane_start = lane_offset as usize;
    let lane_end = lane_start.saturating_add(lane_len as usize);
    if lane_end > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk scalar lane range exceeds projected read",
        ));
    }
    Ok(&buf[lane_start..lane_end])
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

pub fn read_chunk_payload_batch(
    file: &mut File,
    requests: &[ChunkPayloadRead],
    max_gap: u64,
) -> io::Result<ChunkPayloadBatch> {
    let mut ranges = Vec::with_capacity(requests.len());
    for request in requests {
        if request.len == 0 {
            continue;
        }
        let end = request.offset.checked_add(request.len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "chunk payload range overflows")
        })?;
        ranges.push((request.offset, end));
    }
    ranges.sort_unstable_by_key(|(offset, _)| *offset);

    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (offset, end) in ranges {
        let Some((_, run_end)) = merged.last_mut() else {
            merged.push((offset, end));
            continue;
        };
        if offset > run_end.saturating_add(max_gap) {
            merged.push((offset, end));
        } else if end > *run_end {
            *run_end = end;
        }
    }

    let mut spans = Vec::with_capacity(merged.len());
    let mut physical_bytes_read = 0u64;
    for (offset, end) in merged {
        let len = end
            .checked_sub(offset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk span"))?;
        let len_usize = usize::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "chunk payload span too large")
        })?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0u8; len_usize];
        file.read_exact(&mut bytes)?;
        physical_bytes_read = physical_bytes_read.saturating_add(len);
        spans.push(ChunkPayloadSpan { offset, bytes });
    }

    Ok(ChunkPayloadBatch {
        spans,
        physical_bytes_read,
    })
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

pub fn read_chunk_indexed_scalar_projection_at(
    file: &mut File,
    entry: &ChunkIndexEntry,
    projection: ChunkScalarProjection,
) -> io::Result<(ChunkScalarProjectionRecord, u32)> {
    let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
        let record = read_chunk_scalar_projection_at(file, entry.offset, entry.length, projection)?;
        return Ok((record, entry.length));
    };

    let read_len = entry.scalar_projection_read_len();
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut buf = vec![0u8; read_len as usize];
    file.read_exact(&mut buf)?;
    let decoded = decode_chunk_header(&buf[..CHUNK_HEADER_LEN])?;
    let lane_start = lane_offset as usize;
    let lane_end = lane_start.saturating_add(lane_len as usize);
    if lane_end > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk scalar lane range exceeds projected read",
        ));
    }
    let lane = &buf[lane_start..lane_end];
    let record = decode_typed_scalar_lane(&decoded, &lane, projection)?;
    Ok((record, read_len))
}

pub(super) fn scalar_lane_range(entry: &ChunkIndexEntry) -> io::Result<Option<(u32, u32)>> {
    match (entry.scalar_lane_offset, entry.scalar_lane_len) {
        (0, 0) => Ok(None),
        (0, _) | (_, 0) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk scalar lane range is incomplete",
        )),
        (offset, len) => {
            if offset < CHUNK_HEADER_LEN as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scalar lane offset points into chunk header",
                ));
            }
            let end = offset.checked_add(len).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scalar lane range overflow",
                )
            })?;
            if end > entry.length {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scalar lane range exceeds chunk length",
                ));
            }
            Ok(Some((offset, len)))
        }
    }
}
