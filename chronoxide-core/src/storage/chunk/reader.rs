use super::*;

pub struct ChunkReader {
    file: File,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPayloadRead {
    pub file_id: u8,
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
    file_id: u8,
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ChunkPayloadBatchPlan {
    file_id: u8,
    spans: Vec<ChunkPayloadRead>,
    physical_bytes_read: u64,
}

impl ChunkPayloadBatchPlan {
    pub fn file_id(&self) -> u8 {
        self.file_id
    }

    pub fn physical_read_count(&self) -> u64 {
        self.spans.len() as u64
    }

    pub fn physical_bytes_read(&self) -> u64 {
        self.physical_bytes_read
    }

    pub fn read_requests(
        &self,
        file: impl Into<crate::storage::io::ReadFile>,
    ) -> io::Result<Vec<crate::storage::io::ReadRequest>> {
        let file = file.into();
        self.spans
            .iter()
            .map(|span| {
                Ok(crate::storage::io::ReadRequest {
                    file: file.clone(),
                    offset: span.offset,
                    len: usize::try_from(span.len).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "chunk payload span too large")
                    })?,
                })
            })
            .collect()
    }

    pub fn finish(
        self,
        results: Vec<crate::storage::io::ReadResult>,
    ) -> io::Result<ChunkPayloadBatch> {
        if results.len() != self.spans.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk payload result count does not match planned spans",
            ));
        }
        let mut spans = Vec::with_capacity(self.spans.len());
        for (span, result) in self.spans.into_iter().zip(results) {
            if result.bytes.len() as u64 != span.len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            spans.push(ChunkPayloadSpan {
                file_id: span.file_id,
                offset: span.offset,
                bytes: result.bytes,
            });
        }
        Ok(ChunkPayloadBatch {
            spans,
            physical_bytes_read: self.physical_bytes_read,
        })
    }
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

    pub(crate) fn append(&mut self, mut other: Self) {
        self.spans.append(&mut other.spans);
        self.physical_bytes_read = self
            .physical_bytes_read
            .saturating_add(other.physical_bytes_read);
    }

    /// Authenticates one schema-neutral metadata locator against the exact
    /// indexed prefix carried by this payload batch.
    ///
    /// Schema 6 already stores the chunk flags in its v1 index, so its legacy
    /// locator is returned unchanged. Schema 7 deliberately stores no flags in
    /// series metadata: the independently authenticated 40/56-byte chunk
    /// prefix is the sole source of those flags. No semantic chunk decoder may
    /// consume a schema-7 locator before this conversion succeeds.
    pub(crate) fn authenticate_indexed_locator(
        &self,
        locator: &IndexedChunkLocator,
    ) -> io::Result<ChunkIndexEntry> {
        let entry = locator.entry();
        let IndexedChunkAuthentication::Schema7 {
            indexed_prefix_crc32c,
        } = locator.authentication()
        else {
            return Ok(entry.clone());
        };

        let prefix = self.slice(
            entry.file_id,
            entry.offset,
            locator.indexed_prefix_len() as u64,
        )?;
        let verified = verify_schema7_indexed_prefix(
            &Schema7ChunkPrefixExpectation {
                series_ref: locator.series_ref(),
                kind: entry.kind,
                min_time_ms: entry.min_time_ms,
                max_time_ms: entry.max_time_ms,
                length: entry.length,
                scalar_lane_offset: entry.scalar_lane_offset,
                scalar_lane_len: entry.scalar_lane_len,
                indexed_prefix_crc32c,
            },
            prefix,
        )?;

        let mut authenticated = entry.clone();
        authenticated.flags = verified.flags;
        Ok(authenticated)
    }

    pub fn decode_chunk_record(&self, offset: u64, length: u32) -> io::Result<ChunkRecord> {
        decode_chunk_record(self.slice(0, offset, u64::from(length))?)
    }

    pub(crate) fn decode_indexed_chunk_record(
        &self,
        entry: &ChunkIndexEntry,
    ) -> io::Result<ChunkRecord> {
        decode_chunk_record(self.slice(entry.file_id, entry.offset, u64::from(entry.length))?)
    }

    pub fn decode_indexed_scalar_projection(
        &self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
    ) -> io::Result<(ChunkScalarProjectionRecord, u32)> {
        let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
            let record = decode_chunk_scalar_projection(
                self.slice(entry.file_id, entry.offset, u64::from(entry.length))?,
                projection,
            )?;
            return Ok((record, entry.length));
        };

        let read_len = entry.scalar_projection_read_len();
        let buf = self.slice(entry.file_id, entry.offset, u64::from(read_len))?;
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
        let buf = self.slice(entry.file_id, entry.offset, u64::from(read_len))?;
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
                self.slice(entry.file_id, entry.offset, u64::from(entry.length))?,
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
        let buf = self.slice(entry.file_id, entry.offset, u64::from(read_len))?;
        let decoded = decode_indexed_scalar_projection_header(buf, entry)?;
        let lane = validate_scalar_lane_slice(buf, lane_offset, lane_len)?;
        for_each_typed_scalar_lane_sample(&decoded, lane, projection, on_sample)?;
        Ok((decoded.scalar_record_header(), read_len))
    }

    fn slice(&self, file_id: u8, offset: u64, len: u64) -> io::Result<&[u8]> {
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "chunk payload range overflows")
        })?;
        for span in &self.spans {
            if span.file_id != file_id {
                continue;
            }
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
    payload_coalesce_max_gap_bytes: u64,
) -> io::Result<ChunkPayloadBatch> {
    let reader = crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
        mode: crate::storage::io::ChunkReadMode::Pread,
        queue_depth: 1,
        payload_coalesce_max_gap_bytes,
    })?;
    read_chunk_payload_batch_with_reader(std::sync::Arc::new(file.try_clone()?), requests, &reader)
}

pub fn read_chunk_payload_batch_with_reader(
    file: std::sync::Arc<File>,
    requests: &[ChunkPayloadRead],
    reader: &crate::storage::io::ChunkReader,
) -> io::Result<ChunkPayloadBatch> {
    let plan = plan_chunk_payload_batch(requests, reader.payload_coalesce_max_gap_bytes())?;
    let read_requests = plan.read_requests(file)?;
    let results = reader
        .read_many(&read_requests)
        .map_err(normalize_chunk_payload_read_error)?;
    plan.finish(results)
}

pub fn plan_chunk_payload_batch(
    requests: &[ChunkPayloadRead],
    max_gap: u64,
) -> io::Result<ChunkPayloadBatchPlan> {
    if max_gap > crate::storage::io::MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "payload_coalesce_max_gap_bytes must be <= {}",
                crate::storage::io::MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES
            ),
        ));
    }
    let mut file_id = None;
    let mut ranges = Vec::with_capacity(requests.len());
    for request in requests {
        if request.file_id > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk payload file_id must be 0 or 1",
            ));
        }
        match file_id {
            Some(file_id) if file_id != request.file_id => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "chunk payload batch spans multiple files",
                ));
            }
            None => file_id = Some(request.file_id),
            Some(_) => {}
        }
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
        usize::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "chunk payload span too large")
        })?;
        spans.push(ChunkPayloadRead {
            file_id: file_id.unwrap_or(0),
            offset,
            len,
        });
        physical_bytes_read = physical_bytes_read.saturating_add(len);
    }

    Ok(ChunkPayloadBatchPlan {
        file_id: file_id.unwrap_or(0),
        spans,
        physical_bytes_read,
    })
}

fn normalize_chunk_payload_read_error(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer")
    } else {
        error
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
    let record = decode_typed_scalar_lane(&decoded, lane, projection)?;
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
