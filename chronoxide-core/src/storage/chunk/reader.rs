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
    // Invariant: ordered by (file_id, offset), with disjoint spans per file.
    // Construction is private; append callers preserve payload-file order.
    spans: Vec<ChunkPayloadSpan>,
    physical_bytes_read: u64,
}

#[derive(Debug, Clone)]
struct ChunkPayloadSpan {
    file_id: u8,
    offset: u64,
    bytes: Vec<u8>,
}

pub(crate) struct ChunkPayloadDecoder<'a> {
    batch: &'a ChunkPayloadBatch,
    file_ranges: [(usize, usize); 2],
    span_cursors: [Option<usize>; 2],
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
        debug_assert!(chunk_payload_spans_are_sorted_and_disjoint(&spans));
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
        debug_assert!(chunk_payload_spans_are_sorted_and_disjoint(&self.spans));
        debug_assert!(chunk_payload_spans_are_sorted_and_disjoint(&other.spans));
        if let (Some(left), Some(right)) = (self.spans.last(), other.spans.first()) {
            debug_assert!(chunk_payload_spans_are_ordered_and_disjoint(left, right));
        }
        self.spans.append(&mut other.spans);
        self.physical_bytes_read = self
            .physical_bytes_read
            .saturating_add(other.physical_bytes_read);
    }

    pub(crate) fn decoder(&self) -> ChunkPayloadDecoder<'_> {
        ChunkPayloadDecoder::new(self)
    }

    #[cfg(test)]
    pub(crate) fn authenticate_indexed_locator(
        &self,
        locator: &IndexedChunkLocator,
    ) -> io::Result<ChunkIndexEntry> {
        self.decoder().authenticate_indexed_locator(locator)
    }

    pub fn decode_chunk_record(&self, offset: u64, length: u32) -> io::Result<ChunkRecord> {
        self.decoder().decode_chunk_record(offset, length)
    }

    #[cfg(test)]
    pub(crate) fn decode_indexed_chunk_record(
        &self,
        entry: &ChunkIndexEntry,
    ) -> io::Result<ChunkRecord> {
        self.decoder().decode_indexed_chunk_record(entry)
    }

    pub fn decode_indexed_scalar_projection(
        &self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
    ) -> io::Result<(ChunkScalarProjectionRecord, u32)> {
        self.decoder()
            .decode_indexed_scalar_projection(entry, projection)
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
        self.decoder()
            .for_each_indexed_scalar_projection_sample(entry, projection, on_sample)
    }

    #[cfg(test)]
    pub(crate) fn indexed_scalar_projection_header(
        &self,
        entry: &ChunkIndexEntry,
    ) -> io::Result<(ChunkScalarRecordHeader, u32)> {
        self.decoder().indexed_scalar_projection_header(entry)
    }

    #[cfg(test)]
    pub(crate) fn for_each_indexed_scalar_projection_sample_with_header<F>(
        &self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
        on_sample: F,
    ) -> io::Result<(ChunkScalarRecordHeader, u32)>
    where
        F: FnMut(ChunkScalarSample) -> io::Result<()>,
    {
        self.decoder()
            .for_each_indexed_scalar_projection_sample_with_header(entry, projection, on_sample)
    }
}

impl<'a> ChunkPayloadDecoder<'a> {
    fn new(batch: &'a ChunkPayloadBatch) -> Self {
        let file_0_end = batch.spans.partition_point(|span| span.file_id == 0);
        let file_1_end = batch.spans.partition_point(|span| span.file_id <= 1);
        debug_assert_eq!(file_1_end, batch.spans.len());
        Self {
            batch,
            file_ranges: [(0, file_0_end), (file_0_end, file_1_end)],
            span_cursors: [None, None],
        }
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
        &mut self,
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

    pub(crate) fn decode_chunk_record(
        &mut self,
        offset: u64,
        length: u32,
    ) -> io::Result<ChunkRecord> {
        decode_chunk_record(self.slice(0, offset, u64::from(length))?)
    }

    pub(crate) fn decode_indexed_chunk_record(
        &mut self,
        entry: &ChunkIndexEntry,
    ) -> io::Result<ChunkRecord> {
        let buf = self.slice(entry.file_id, entry.offset, u64::from(entry.length))?;
        let decoded = decode_indexed_scalar_projection_header(buf, entry)?;
        let record = decode_chunk_record(buf)?;
        validate_indexed_chunk_lengths(&decoded, entry)?;
        Ok(record)
    }

    pub(crate) fn decode_indexed_scalar_projection(
        &mut self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
    ) -> io::Result<(ChunkScalarProjectionRecord, u32)> {
        let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
            let record = decode_indexed_scalar_fallback(
                self.slice(entry.file_id, entry.offset, u64::from(entry.length))?,
                entry,
                projection,
            )?;
            return Ok((record, entry.length));
        };

        let read_len = entry.scalar_projection_read_len();
        let buf = self.slice(entry.file_id, entry.offset, u64::from(read_len))?;
        let decoded = decode_indexed_scalar_projection_header(buf, entry)?;
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
        validate_indexed_chunk_lengths(&decoded, entry)?;
        Ok((record, read_len))
    }

    pub(crate) fn for_each_indexed_scalar_projection_sample<F>(
        &mut self,
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
        &mut self,
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
        &mut self,
        entry: &ChunkIndexEntry,
        projection: ChunkScalarProjection,
        mut on_sample: F,
    ) -> io::Result<(ChunkScalarRecordHeader, u32)>
    where
        F: FnMut(ChunkScalarSample) -> io::Result<()>,
    {
        let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
            let record = decode_indexed_scalar_fallback(
                self.slice(entry.file_id, entry.offset, u64::from(entry.length))?,
                entry,
                projection,
            )?;
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
        validate_indexed_chunk_lengths(&decoded, entry)?;
        Ok((decoded.scalar_record_header(), read_len))
    }

    fn slice(&mut self, file_id: u8, offset: u64, len: u64) -> io::Result<&[u8]> {
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "chunk payload range overflows")
        })?;
        let file_index = usize::from(file_id);
        let Some(&(range_start, range_end)) = self.file_ranges.get(file_index) else {
            return Err(chunk_payload_request_missing_error());
        };
        if range_start == range_end {
            return Err(chunk_payload_request_missing_error());
        }

        let span_index = match self.span_cursors[file_index] {
            Some(mut span_index)
                if self.batch.spans[span_index].file_id == file_id
                    && offset >= self.batch.spans[span_index].offset =>
            {
                while span_index + 1 < range_end
                    && self.batch.spans[span_index + 1].offset <= offset
                {
                    span_index += 1;
                }
                span_index
            }
            _ => {
                let relative_index = self.batch.spans[range_start..range_end]
                    .partition_point(|span| span.offset <= offset);
                let Some(relative_index) = relative_index.checked_sub(1) else {
                    return Err(chunk_payload_request_missing_error());
                };
                range_start + relative_index
            }
        };
        self.span_cursors[file_index] = Some(span_index);
        let span = &self.batch.spans[span_index];
        if let Some(bytes) = chunk_payload_span_slice(span, offset, end, len)? {
            return Ok(bytes);
        }

        Err(chunk_payload_request_missing_error())
    }
}

fn chunk_payload_request_missing_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "chunk payload request missing from batch",
    )
}

fn chunk_payload_span_slice(
    span: &ChunkPayloadSpan,
    offset: u64,
    end: u64,
    len: u64,
) -> io::Result<Option<&[u8]>> {
    let span_len = u64::try_from(span.bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk payload span too large"))?;
    let span_end = span.offset.checked_add(span_len).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "chunk payload span overflows")
    })?;
    if offset < span.offset || end > span_end {
        return Ok(None);
    }
    let start = usize::try_from(offset - span.offset).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "chunk payload offset too large")
    })?;
    let len = usize::try_from(len).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "chunk payload length too large")
    })?;
    Ok(Some(&span.bytes[start..start + len]))
}

fn chunk_payload_spans_are_sorted_and_disjoint(spans: &[ChunkPayloadSpan]) -> bool {
    spans
        .windows(2)
        .all(|pair| chunk_payload_spans_are_ordered_and_disjoint(&pair[0], &pair[1]))
}

fn chunk_payload_spans_are_ordered_and_disjoint(
    left: &ChunkPayloadSpan,
    right: &ChunkPayloadSpan,
) -> bool {
    if left.file_id != right.file_id {
        return left.file_id < right.file_id;
    }
    let Ok(left_len) = u64::try_from(left.bytes.len()) else {
        return false;
    };
    left.offset
        .checked_add(left_len)
        .is_some_and(|left_end| left_end <= right.offset)
}

fn decode_indexed_scalar_projection_header(
    buf: &[u8],
    entry: &ChunkIndexEntry,
) -> io::Result<DecodedChunkHeader> {
    let _ = scalar_lane_range(entry)?;
    let decoded = decode_chunk_header(buf)?;
    let header = decoded.scalar_record_header();
    validate_indexed_scalar_projection_kind(entry, header.kind)?;
    if header.min_time_ms != entry.min_time_ms
        || header.max_time_ms != entry.max_time_ms
        || decoded.flags() != entry.flags
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index metadata does not match chunk header",
        ));
    }
    Ok(decoded)
}

fn validate_indexed_chunk_lengths(
    decoded: &DecodedChunkHeader,
    entry: &ChunkIndexEntry,
) -> io::Result<()> {
    let scalar_lane_length_matches = if entry.scalar_lane_len == 0 {
        // A locator may deliberately omit the scalar-lane optimization and
        // fall back to decoding the complete authenticated chunk.
        true
    } else {
        let expected_header_len = CHUNK_HEADER_LEN
            .checked_add(entry.scalar_lane_len as usize)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "chunk header length overflows")
            })?;
        decoded.header_len() == expected_header_len
    };
    if !scalar_lane_length_matches || decoded.record_len()? != entry.length as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index lengths do not match chunk header",
        ));
    }
    Ok(())
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

fn decode_indexed_scalar_fallback(
    buf: &[u8],
    entry: &ChunkIndexEntry,
    projection: ChunkScalarProjection,
) -> io::Result<ChunkScalarProjectionRecord> {
    let decoded = decode_indexed_scalar_projection_header(buf, entry)?;
    let record = decode_chunk_scalar_projection(buf, projection)?;
    validate_indexed_chunk_lengths(&decoded, entry)?;
    Ok(record)
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
        if self.file.read(&mut header[..1])? == 0 {
            return Ok(None);
        }
        self.file.read_exact(&mut header[1..])?;

        let frame_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let frame_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let flags = u16::from_le_bytes(header[8..10].try_into().unwrap());
        if flags != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk frame flags must be zero",
            ));
        }
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
        let payload_offset = self.file.stream_position()?;
        validate_file_range(
            &self.file,
            payload_offset,
            u64::try_from(payload_len).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk frame payload exceeds u64",
                )
            })?,
            "chunk frame payload",
        )?;
        let mut payload = try_zeroed_chunk_bytes(payload_len, "chunk frame payload")?;
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
    for request in requests {
        if request.len != 0 {
            validate_file_range(
                file.as_ref(),
                request.offset,
                request.len,
                "chunk payload request",
            )?;
        }
    }
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
    validate_file_range(file, offset, u64::from(length), "chunk record")?;
    file.seek(SeekFrom::Start(offset))?;
    let mut payload = try_zeroed_chunk_bytes(length as usize, "chunk record")?;
    file.read_exact(&mut payload)?;
    decode_chunk_record(&payload)
}

pub fn read_chunk_scalar_projection_at(
    file: &mut File,
    offset: u64,
    length: u32,
    projection: ChunkScalarProjection,
) -> io::Result<ChunkScalarProjectionRecord> {
    validate_file_range(file, offset, u64::from(length), "chunk scalar projection")?;
    file.seek(SeekFrom::Start(offset))?;
    let mut payload = try_zeroed_chunk_bytes(length as usize, "chunk scalar projection")?;
    file.read_exact(&mut payload)?;
    decode_chunk_scalar_projection(&payload, projection)
}

pub fn read_chunk_indexed_scalar_projection_at(
    file: &mut File,
    entry: &ChunkIndexEntry,
    projection: ChunkScalarProjection,
) -> io::Result<(ChunkScalarProjectionRecord, u32)> {
    let Some((lane_offset, lane_len)) = scalar_lane_range(entry)? else {
        validate_file_range(
            file,
            entry.offset,
            u64::from(entry.length),
            "indexed chunk scalar fallback",
        )?;
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut payload =
            try_zeroed_chunk_bytes(entry.length as usize, "indexed chunk scalar fallback")?;
        file.read_exact(&mut payload)?;
        let record = decode_indexed_scalar_fallback(&payload, entry, projection)?;
        return Ok((record, entry.length));
    };

    let read_len = entry.scalar_projection_read_len();
    validate_file_range(
        file,
        entry.offset,
        u64::from(read_len),
        "indexed chunk scalar lane",
    )?;
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut buf = try_zeroed_chunk_bytes(read_len as usize, "indexed chunk scalar lane")?;
    file.read_exact(&mut buf)?;
    let decoded = decode_indexed_scalar_projection_header(&buf[..CHUNK_HEADER_LEN], entry)?;
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
    validate_indexed_chunk_lengths(&decoded, entry)?;
    Ok((record, read_len))
}

fn try_zeroed_chunk_bytes(len: usize, field: &'static str) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{field} allocation failed: {error}"),
        )
    })?;
    bytes.resize(len, 0);
    Ok(bytes)
}

fn validate_file_range(
    file: &File,
    offset: u64,
    length: u64,
    field: &'static str,
) -> io::Result<()> {
    let end = offset.checked_add(length).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} file range overflows"),
        )
    })?;
    if end > file.metadata()?.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{field} exceeds the file length"),
        ));
    }
    Ok(())
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
            if offset > CHUNK_HEADER_LEN as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scalar lane offset is noncanonical",
                ));
            }
            Ok(Some((offset, len)))
        }
    }
}

#[cfg(test)]
mod payload_batch_lookup_tests;
