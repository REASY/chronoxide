use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct DecodedChunkHeader {
    kind: ChunkKind,
    encoding: ChunkEncoding,
    flags: u16,
    series_ref: u32,
    min_time_ms: u64,
    max_time_ms: u64,
    num_points: u32,
    header_len: usize,
    payload_len: usize,
    chunk_crc: u32,
}

impl DecodedChunkHeader {
    pub(super) fn flags(&self) -> u16 {
        self.flags
    }

    pub(super) fn header_len(&self) -> usize {
        self.header_len
    }

    pub(super) fn record_len(&self) -> io::Result<usize> {
        self.header_len
            .checked_add(self.payload_len)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "chunk record length overflows")
            })
    }

    pub(super) fn scalar_record_header(&self) -> ChunkScalarRecordHeader {
        ChunkScalarRecordHeader {
            series_ref: self.series_ref,
            kind: self.kind,
            min_time_ms: self.min_time_ms,
            max_time_ms: self.max_time_ms,
            sample_count: self.num_points,
        }
    }
}

pub(super) struct DecodedChunkPayload<'a> {
    kind: ChunkKind,
    encoding: ChunkEncoding,
    flags: u16,
    series_ref: u32,
    min_time_ms: u64,
    max_time_ms: u64,
    num_points: u32,
    payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecodedChunkLayout {
    pub(crate) kind: ChunkKind,
    pub(crate) encoding: ChunkEncoding,
    pub(crate) flags: u16,
    pub(crate) num_points: u32,
    pub(crate) common_header_bytes: u32,
    pub(crate) scalar_lane_bytes: u32,
    pub(crate) payload_bytes: u32,
    pub(crate) timestamp_base_bytes: u32,
    pub(crate) timestamp_delta_bytes: u32,
    pub(crate) value_bytes: u32,
}

pub(super) fn decode_chunk_header(payload: &[u8]) -> io::Result<DecodedChunkHeader> {
    if payload.len() < CHUNK_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "chunk header short read",
        ));
    }
    let chunk_header = &payload[..CHUNK_HEADER_LEN];
    let kind = chunk_kind_from_u8(chunk_header[0])?;
    let encoding = chunk_encoding_from_u8(chunk_header[1])?;
    let flags = u16::from_le_bytes(chunk_header[2..4].try_into().unwrap());
    let series_ref = u32::from_le_bytes(chunk_header[4..8].try_into().unwrap());
    let min_time_ms = u64::from_le_bytes(chunk_header[8..16].try_into().unwrap());
    let max_time_ms = u64::from_le_bytes(chunk_header[16..24].try_into().unwrap());
    let num_points = u32::from_le_bytes(chunk_header[24..28].try_into().unwrap());
    let header_len = u32::from_le_bytes(chunk_header[28..32].try_into().unwrap()) as usize;
    let payload_len = u32::from_le_bytes(chunk_header[32..36].try_into().unwrap()) as usize;
    let chunk_crc = u32::from_le_bytes(chunk_header[36..40].try_into().unwrap());

    let _record_len = header_len.checked_add(payload_len).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "chunk record length overflows")
    })?;

    Ok(DecodedChunkHeader {
        kind,
        encoding,
        flags,
        series_ref,
        min_time_ms,
        max_time_ms,
        num_points,
        header_len,
        payload_len,
        chunk_crc,
    })
}

pub(super) fn decode_chunk_payload(payload: &[u8]) -> io::Result<DecodedChunkPayload<'_>> {
    let decoded = decode_chunk_header(payload)?;
    let record_len = decoded
        .header_len
        .checked_add(decoded.payload_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chunk length overflows"))?;
    if decoded.header_len < CHUNK_HEADER_LEN || record_len != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk payload length is not exact",
        ));
    }

    let chunk_payload = &payload[decoded.header_len..record_len];
    if crc32c(chunk_payload) != decoded.chunk_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk crc mismatch",
        ));
    }

    Ok(DecodedChunkPayload {
        kind: decoded.kind,
        encoding: decoded.encoding,
        flags: decoded.flags,
        series_ref: decoded.series_ref,
        min_time_ms: decoded.min_time_ms,
        max_time_ms: decoded.max_time_ms,
        num_points: decoded.num_points,
        payload: chunk_payload,
    })
}

pub(super) fn encode_typed_scalar_lane<T>(t0_ms: u64, samples: &[(u64, T)]) -> io::Result<Vec<u8>>
where
    T: TypedCounterValue,
{
    let mut body = Vec::new();
    body.extend_from_slice(&t0_ms.to_le_bytes());
    for (ts, _) in samples {
        encode_varint(ts.saturating_sub(t0_ms), &mut body);
    }
    for (_, value) in samples {
        encode_scalar_lane_metadata(value.metadata(), &mut body);
        encode_varint(value.count(), &mut body);
        encode_scalar_lane_opt_f64(value.sum(), &mut body);
    }

    let body_len = u32::try_from(body.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "typed scalar lane body exceeds the u32 on-disk limit",
        )
    })?;
    let body_crc = crc32c(&body);
    let lane_len = TYPED_SCALAR_LANE_HEADER_LEN
        .checked_add(body.len())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "typed scalar lane length overflows",
            )
        })?;
    let mut lane = Vec::new();
    lane.try_reserve_exact(lane_len).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("typed scalar lane allocation failed: {error}"),
        )
    })?;
    lane.extend_from_slice(&TYPED_SCALAR_LANE_MAGIC.to_le_bytes());
    lane.extend_from_slice(&TYPED_SCALAR_LANE_VERSION.to_le_bytes());
    lane.extend_from_slice(&0u16.to_le_bytes());
    lane.extend_from_slice(&body_len.to_le_bytes());
    lane.extend_from_slice(&body_crc.to_le_bytes());
    lane.extend_from_slice(&body);
    Ok(lane)
}

pub(super) fn encode_scalar_lane_metadata(metadata: TypedSampleMetadata, out: &mut Vec<u8>) {
    encode_varint(u64::from(metadata.flags), out);
    encode_varint(metadata.temporality as u64, out);
    encode_varint(metadata.reset_hint as u64, out);
    match metadata.start_time_ms {
        Some(start_time_ms) => {
            out.push(1);
            encode_varint(start_time_ms, out);
        }
        None => out.push(0),
    }
}

pub(super) fn encode_scalar_lane_opt_f64(value: Option<f64>, out: &mut Vec<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

pub(super) fn decode_typed_scalar_lane(
    header: &DecodedChunkHeader,
    lane: &[u8],
    projection: ChunkScalarProjection,
) -> io::Result<ChunkScalarProjectionRecord> {
    let body = typed_scalar_lane_body(header, lane)?;
    let mut samples = try_vec_with_capacity(
        header.num_points as usize,
        "decoded typed scalar-lane samples",
    )?;
    for_each_typed_scalar_lane_sample_body(header, body, projection, |sample| {
        samples.push(sample);
        Ok(())
    })?;

    Ok(ChunkScalarProjectionRecord {
        series_ref: header.series_ref,
        kind: header.kind,
        min_time_ms: header.min_time_ms,
        max_time_ms: header.max_time_ms,
        samples,
    })
}

pub(super) fn for_each_typed_scalar_lane_sample<F>(
    header: &DecodedChunkHeader,
    lane: &[u8],
    projection: ChunkScalarProjection,
    on_sample: F,
) -> io::Result<()>
where
    F: FnMut(ChunkScalarSample) -> io::Result<()>,
{
    let body = typed_scalar_lane_body(header, lane)?;
    for_each_typed_scalar_lane_sample_body(header, body, projection, on_sample)
}

fn for_each_typed_scalar_lane_sample_body<F>(
    header: &DecodedChunkHeader,
    body: &[u8],
    projection: ChunkScalarProjection,
    mut on_sample: F,
) -> io::Result<()>
where
    F: FnMut(ChunkScalarSample) -> io::Result<()>,
{
    for_each_typed_scalar_lane_row_body(header, body, |row| {
        let value = match projection {
            ChunkScalarProjection::Count => Some(ChunkScalarValue::Count(row.count)),
            ChunkScalarProjection::Sum => row.sum.map(ChunkScalarValue::Sum),
        };
        on_sample(ChunkScalarSample {
            timestamp_ms: row.timestamp_ms,
            metadata: row.metadata,
            value,
        })
    })
}

#[derive(Debug, Clone, Copy)]
struct DecodedTypedScalarLaneRow {
    timestamp_ms: u64,
    metadata: TypedSampleMetadata,
    count: u64,
    sum: Option<f64>,
}

fn for_each_typed_scalar_lane_row<F>(
    header: &DecodedChunkHeader,
    lane: &[u8],
    on_row: F,
) -> io::Result<()>
where
    F: FnMut(DecodedTypedScalarLaneRow) -> io::Result<()>,
{
    let body = typed_scalar_lane_body(header, lane)?;
    for_each_typed_scalar_lane_row_body(header, body, on_row)
}

fn for_each_typed_scalar_lane_row_body<F>(
    header: &DecodedChunkHeader,
    body: &[u8],
    mut on_row: F,
) -> io::Result<()>
where
    F: FnMut(DecodedTypedScalarLaneRow) -> io::Result<()>,
{
    let mut timestamp_cursor = 0usize;
    let t0_ms = read_u64(body, &mut timestamp_cursor)?;
    if t0_ms != header.min_time_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane timestamp base disagrees with min_time_ms",
        ));
    }
    let mut value_cursor = timestamp_cursor;
    // The lane stores all timestamp deltas before metadata/value rows.
    // Walk them once to position the value cursor, then replay them while decoding rows.
    for _ in 0..header.num_points {
        let _ = decode_varint(body, &mut value_cursor)?;
    }
    let values_start = value_cursor;

    let mut first_timestamp = None;
    let mut previous_timestamp = None;
    let mut flags = TypedChunkFlagsAccumulator::default();
    for _ in 0..header.num_points {
        let timestamp_ms =
            checked_timestamp_ms(t0_ms, decode_varint(body, &mut timestamp_cursor)?)?;
        if previous_timestamp.is_some_and(|previous| timestamp_ms < previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "typed scalar lane timestamps are not ordered",
            ));
        }
        first_timestamp.get_or_insert(timestamp_ms);
        previous_timestamp = Some(timestamp_ms);
        let metadata = decode_typed_metadata(body, &mut value_cursor)?;
        flags.observe(metadata);
        let count = decode_varint(body, &mut value_cursor)?;
        let sum = decode_opt_f64(body, &mut value_cursor)?;
        on_row(DecodedTypedScalarLaneRow {
            timestamp_ms,
            metadata,
            count,
            sum,
        })?;
    }
    if timestamp_cursor != values_start || value_cursor != body.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane has trailing bytes",
        ));
    }
    if first_timestamp != Some(header.min_time_ms) || previous_timestamp != Some(header.max_time_ms)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane timestamp range disagrees with the chunk header",
        ));
    }
    if flags.finish() != header.flags {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed chunk header flags disagree with scalar-lane metadata",
        ));
    }

    Ok(())
}

fn typed_scalar_lane_body<'a>(header: &DecodedChunkHeader, lane: &'a [u8]) -> io::Result<&'a [u8]> {
    if header.num_points == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane has no points",
        ));
    }
    if header.encoding != ChunkEncoding::SchemaVarLen {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane requires schema varlen encoding",
        ));
    }
    if !matches!(
        header.kind,
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "typed scalar lane requires a typed chunk",
        ));
    }
    if lane.len() < TYPED_SCALAR_LANE_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "typed scalar lane header short read",
        ));
    }

    let magic = u32::from_le_bytes(lane[0..4].try_into().unwrap());
    if magic != TYPED_SCALAR_LANE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane magic mismatch",
        ));
    }
    let version = u16::from_le_bytes(lane[4..6].try_into().unwrap());
    if version != TYPED_SCALAR_LANE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported typed scalar lane version",
        ));
    }
    let flags = u16::from_le_bytes(lane[6..8].try_into().unwrap());
    if flags != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane flags must be zero",
        ));
    }
    let body_len = u32::from_le_bytes(lane[8..12].try_into().unwrap()) as usize;
    let body_crc = u32::from_le_bytes(lane[12..16].try_into().unwrap());
    if TYPED_SCALAR_LANE_HEADER_LEN + body_len != lane.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane body length mismatch",
        ));
    }
    let body = &lane[TYPED_SCALAR_LANE_HEADER_LEN..];
    if crc32c(body) != body_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane crc mismatch",
        ));
    }
    let minimum_body_len = (header.num_points as usize)
        .checked_mul(7)
        .and_then(|bytes| bytes.checked_add(8))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "typed scalar lane minimum body size overflows",
            )
        })?;
    if body.len() < minimum_body_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane point count is infeasible for its body bytes",
        ));
    }
    Ok(body)
}

pub(crate) fn verify_chunk_scalar_lane_and_flags(
    record_bytes: &[u8],
    samples: &ChunkSamples,
) -> io::Result<()> {
    let header = decode_chunk_header(record_bytes)?;
    if header.header_len < CHUNK_HEADER_LEN || header.header_len > record_bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk header length is invalid while verifying redundant metadata",
        ));
    }
    let lane = &record_bytes[CHUNK_HEADER_LEN..header.header_len];
    match samples {
        ChunkSamples::Float(_) if header.kind == ChunkKind::Float => {
            verify_scalar_chunk_redundant_metadata(&header, lane)
        }
        ChunkSamples::Int64(_) if header.kind == ChunkKind::Int64 => {
            verify_scalar_chunk_redundant_metadata(&header, lane)
        }
        ChunkSamples::Histogram(values) if header.kind == ChunkKind::Histogram => {
            verify_typed_chunk_redundant_metadata(&header, lane, values)
        }
        ChunkSamples::ExponentialHistogram(values)
            if header.kind == ChunkKind::ExponentialHistogram =>
        {
            verify_typed_chunk_redundant_metadata(&header, lane, values)
        }
        ChunkSamples::Summary(values) if header.kind == ChunkKind::Summary => {
            verify_typed_chunk_redundant_metadata(&header, lane, values)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded chunk samples disagree with the redundant-metadata kind",
        )),
    }
}

fn verify_scalar_chunk_redundant_metadata(
    header: &DecodedChunkHeader,
    lane: &[u8],
) -> io::Result<()> {
    if header.flags != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scalar chunk header flags must be zero",
        ));
    }
    if !lane.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scalar chunk has a typed scalar lane",
        ));
    }
    Ok(())
}

fn verify_typed_chunk_redundant_metadata<T: TypedCounterValue>(
    header: &DecodedChunkHeader,
    lane: &[u8],
    values: &[(u64, T)],
) -> io::Result<()> {
    let expected_flags = typed_chunk_flags(values.iter().map(|(_, value)| value.metadata()));
    if header.flags != expected_flags {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed chunk header flags disagree with native metadata",
        ));
    }
    if lane.is_empty() {
        return Ok(());
    }

    let mut observed = 0usize;
    for_each_typed_scalar_lane_row(header, lane, |row| {
        let (expected_timestamp_ms, expected_value) = values.get(observed).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "typed scalar lane has more rows than the native payload",
            )
        })?;
        if row.timestamp_ms != *expected_timestamp_ms
            || row.metadata != expected_value.metadata()
            || row.count != expected_value.count()
            || !optional_f64_bits_equal(row.sum, expected_value.sum())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "typed scalar lane row disagrees with the native payload",
            ));
        }
        observed = observed.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "typed scalar lane row count overflows",
            )
        })?;
        Ok(())
    })?;
    if observed != values.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane has fewer rows than the native payload",
        ));
    }
    Ok(())
}

fn optional_f64_bits_equal(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.to_bits() == right.to_bits(),
        (None, None) => true,
        _ => false,
    }
}

pub(crate) fn decode_chunk_record(payload: &[u8]) -> io::Result<ChunkRecord> {
    decode_chunk_record_with_layout(payload).map(|(record, _layout)| record)
}

pub(crate) fn decode_chunk_record_with_layout(
    payload: &[u8],
) -> io::Result<(ChunkRecord, DecodedChunkLayout)> {
    let decoded = decode_chunk_payload(payload)?;
    let mut cursor = 0usize;
    let t0_ms = read_u64(decoded.payload, &mut cursor)?;
    if decoded.num_points == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk has no points",
        ));
    }
    if t0_ms != decoded.min_time_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk timestamp base disagrees with min_time_ms",
        ));
    }
    validate_full_chunk_point_count_feasible(
        decoded.kind,
        decoded.encoding,
        decoded.num_points as usize,
        decoded.payload.len().saturating_sub(cursor),
    )?;
    let timestamp_base_bytes = cursor;
    let timestamp_delta_bytes;
    let value_bytes;

    let samples = match decoded.kind {
        ChunkKind::Float => match decoded.encoding {
            ChunkEncoding::RawF64 => {
                let mut samples =
                    try_vec_with_capacity(decoded.num_points as usize, "decoded RawF64 samples")?;
                let mut encoded_timestamp_bytes = 0usize;
                for _ in 0..decoded.num_points {
                    let before_timestamp = cursor;
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    encoded_timestamp_bytes = encoded_timestamp_bytes
                        .checked_add(cursor - before_timestamp)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "timestamp byte count overflows",
                            )
                        })?;
                    let value = read_f64(decoded.payload, &mut cursor)?;
                    samples.push((checked_timestamp_ms(t0_ms, dt)?, value));
                }
                require_payload_end(decoded.payload, cursor)?;
                timestamp_delta_bytes = encoded_timestamp_bytes;
                value_bytes = decoded
                    .payload
                    .len()
                    .checked_sub(timestamp_base_bytes + timestamp_delta_bytes)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "float payload size underflows")
                    })?;
                ChunkSamples::Float(samples)
            }
            ChunkEncoding::Gorilla => {
                let mut timestamps = try_vec_with_capacity(
                    decoded.num_points as usize,
                    "decoded Gorilla timestamps",
                )?;
                let timestamps_start = cursor;
                for _ in 0..decoded.num_points {
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    timestamps.push(checked_timestamp_ms(t0_ms, dt)?);
                }
                timestamp_delta_bytes = cursor - timestamps_start;
                value_bytes = decoded.payload.len() - cursor;
                let values =
                    decode_gorilla_values(&decoded.payload[cursor..], decoded.num_points as usize)?;
                let mut samples =
                    try_vec_with_capacity(decoded.num_points as usize, "decoded Gorilla samples")?;
                for (ts, value) in timestamps.into_iter().zip(values) {
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
                let mut timestamps = try_vec_with_capacity(
                    decoded.num_points as usize,
                    "decoded IntDelta timestamps",
                )?;
                let timestamps_start = cursor;
                for _ in 0..decoded.num_points {
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    timestamps.push(checked_timestamp_ms(t0_ms, dt)?);
                }
                timestamp_delta_bytes = cursor - timestamps_start;
                let values_start = cursor;
                let mut values =
                    try_vec_with_capacity(decoded.num_points as usize, "decoded IntDelta values")?;
                let mut prev = 0i64;
                for _ in 0..decoded.num_points {
                    let encoded = decode_varint(decoded.payload, &mut cursor)?;
                    let delta = decode_zigzag_i64(encoded);
                    let value = prev.wrapping_add(delta);
                    values.push(value);
                    prev = value;
                }
                require_payload_end(decoded.payload, cursor)?;
                value_bytes = cursor - values_start;
                let mut samples =
                    try_vec_with_capacity(decoded.num_points as usize, "decoded IntDelta samples")?;
                for (ts, value) in timestamps.into_iter().zip(values) {
                    samples.push((ts, value));
                }
                ChunkSamples::Int64(samples)
            }
            ChunkEncoding::RawI64 => {
                let mut samples =
                    try_vec_with_capacity(decoded.num_points as usize, "decoded RawI64 samples")?;
                let mut encoded_timestamp_bytes = 0usize;
                for _ in 0..decoded.num_points {
                    let before_timestamp = cursor;
                    let dt = decode_varint(decoded.payload, &mut cursor)?;
                    encoded_timestamp_bytes = encoded_timestamp_bytes
                        .checked_add(cursor - before_timestamp)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "timestamp byte count overflows",
                            )
                        })?;
                    let value = read_i64(decoded.payload, &mut cursor)?;
                    samples.push((checked_timestamp_ms(t0_ms, dt)?, value));
                }
                require_payload_end(decoded.payload, cursor)?;
                timestamp_delta_bytes = encoded_timestamp_bytes;
                value_bytes = decoded
                    .payload
                    .len()
                    .checked_sub(timestamp_base_bytes + timestamp_delta_bytes)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "int payload size underflows")
                    })?;
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
                let timestamps_start = cursor;
                let timestamps =
                    decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
                timestamp_delta_bytes = cursor - timestamps_start;
                value_bytes = decoded.payload.len() - cursor;
                let values = SchemaVarLenCodec::<HistogramValue>::decode_values(
                    &decoded.payload[cursor..],
                    decoded.num_points as usize,
                )?;
                ChunkSamples::Histogram(zip_decoded_samples(
                    timestamps,
                    values,
                    "decoded histogram samples",
                )?)
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
                let timestamps_start = cursor;
                let timestamps =
                    decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
                timestamp_delta_bytes = cursor - timestamps_start;
                value_bytes = decoded.payload.len() - cursor;
                let values = SchemaVarLenCodec::<ExponentialHistogramValue>::decode_values(
                    &decoded.payload[cursor..],
                    decoded.num_points as usize,
                )?;
                ChunkSamples::ExponentialHistogram(zip_decoded_samples(
                    timestamps,
                    values,
                    "decoded exponential histogram samples",
                )?)
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
                let timestamps_start = cursor;
                let timestamps =
                    decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
                timestamp_delta_bytes = cursor - timestamps_start;
                value_bytes = decoded.payload.len() - cursor;
                let values = SchemaVarLenCodec::<SummaryValue>::decode_values(
                    &decoded.payload[cursor..],
                    decoded.num_points as usize,
                )?;
                ChunkSamples::Summary(zip_decoded_samples(
                    timestamps,
                    values,
                    "decoded summary samples",
                )?)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported summary chunk encoding",
                ));
            }
        },
    };

    validate_decoded_sample_timestamps(
        decoded.num_points,
        decoded.min_time_ms,
        decoded.max_time_ms,
        &samples,
    )?;
    verify_chunk_scalar_lane_and_flags(payload, &samples)?;

    let scalar_lane_bytes = payload
        .len()
        .checked_sub(decoded.payload.len())
        .and_then(|bytes| bytes.checked_sub(CHUNK_HEADER_LEN))
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "chunk header size underflows")
        })?;
    let layout = DecodedChunkLayout {
        kind: decoded.kind,
        encoding: decoded.encoding,
        flags: decoded.flags,
        num_points: decoded.num_points,
        common_header_bytes: CHUNK_HEADER_LEN as u32,
        scalar_lane_bytes: u32::try_from(scalar_lane_bytes).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "scalar lane length exceeds u32")
        })?,
        payload_bytes: u32::try_from(decoded.payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk payload length exceeds u32",
            )
        })?,
        timestamp_base_bytes: u32::try_from(timestamp_base_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "timestamp base length exceeds u32",
            )
        })?,
        timestamp_delta_bytes: u32::try_from(timestamp_delta_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "timestamp delta length exceeds u32",
            )
        })?,
        value_bytes: u32::try_from(value_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "value length exceeds u32"))?,
    };
    Ok((
        ChunkRecord {
            series_ref: decoded.series_ref,
            kind: decoded.kind,
            min_time_ms: decoded.min_time_ms,
            max_time_ms: decoded.max_time_ms,
            samples,
        },
        layout,
    ))
}

pub(super) fn decode_chunk_scalar_projection(
    payload: &[u8],
    projection: ChunkScalarProjection,
) -> io::Result<ChunkScalarProjectionRecord> {
    let decoded = decode_chunk_payload(payload)?;
    if decoded.num_points == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk has no points",
        ));
    }
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
    if t0_ms != decoded.min_time_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk timestamp base disagrees with min_time_ms",
        ));
    }
    let timestamps = decode_timestamps(decoded.payload, &mut cursor, t0_ms, decoded.num_points)?;
    validate_timestamp_iter(
        decoded.num_points,
        decoded.min_time_ms,
        decoded.max_time_ms,
        timestamps.iter().copied(),
    )?;
    let header = decode_chunk_header(payload)?;
    let lane = &payload[CHUNK_HEADER_LEN..header.header_len];
    let lane_rows = decode_typed_scalar_lane_rows(&header, lane)?;
    let samples = decode_schema_varlen_scalar_samples(
        decoded.kind,
        decoded.flags,
        &decoded.payload[cursor..],
        timestamps,
        projection,
        &lane_rows,
    )?;

    Ok(ChunkScalarProjectionRecord {
        series_ref: decoded.series_ref,
        kind: decoded.kind,
        min_time_ms: decoded.min_time_ms,
        max_time_ms: decoded.max_time_ms,
        samples,
    })
}

fn decode_typed_scalar_lane_rows(
    header: &DecodedChunkHeader,
    lane: &[u8],
) -> io::Result<Vec<DecodedTypedScalarLaneRow>> {
    if lane.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = try_vec_with_capacity(
        header.num_points as usize,
        "decoded redundant typed scalar-lane rows",
    )?;
    for_each_typed_scalar_lane_row(header, lane, |row| {
        rows.push(row);
        Ok(())
    })?;
    Ok(rows)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, Copy, PartialEq)]
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
pub(super) enum ScalarProjectionSchema {
    Histogram { bucket_len: usize },
    ExponentialHistogram,
    Summary { quantile_len: usize },
}

fn decode_schema_varlen_scalar_samples(
    kind: ChunkKind,
    expected_flags: u16,
    buf: &[u8],
    timestamps: Vec<u64>,
    projection: ChunkScalarProjection,
    lane_rows: &[DecodedTypedScalarLaneRow],
) -> io::Result<Vec<ChunkScalarSample>> {
    let mut cursor = 0usize;
    let schemas = decode_scalar_projection_schemas(kind, buf, &mut cursor, timestamps.len())?;
    ensure_minimum_encoded_items(
        buf.len().saturating_sub(cursor),
        timestamps.len(),
        1,
        "schema-varlen scalar values",
    )?;
    let mut samples =
        try_vec_with_capacity(timestamps.len(), "decoded schema-varlen scalar samples")?;
    let mut next_first_seen_schema = 0usize;
    let mut flags = TypedChunkFlagsAccumulator::default();
    for timestamp_ms in timestamps {
        let schema_id = decode_varint(buf, &mut cursor)?;
        let schema_idx = usize::try_from(schema_id)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "schema id overflow"))?;
        let schema = schemas
            .get(schema_idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "schema id out of range"))?;
        if schema_idx > next_first_seen_schema {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schema IDs are not in deterministic first-seen order",
            ));
        }
        if schema_idx == next_first_seen_schema {
            next_first_seen_schema = next_first_seen_schema.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "schema first-use count overflows",
                )
            })?;
        }
        let (metadata, count, sum) =
            decode_scalar_projection_value(kind, *schema, buf, &mut cursor)?;
        if !lane_rows.is_empty() {
            let lane_row = lane_rows.get(samples.len()).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "typed scalar lane has fewer rows than the native projection",
                )
            })?;
            if lane_row.timestamp_ms != timestamp_ms
                || lane_row.metadata != metadata
                || lane_row.count != count
                || !optional_f64_bits_equal(lane_row.sum, sum)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "typed scalar lane row disagrees with the native projection",
                ));
            }
        }
        flags.observe(metadata);
        let value = match projection {
            ChunkScalarProjection::Count => Some(ChunkScalarValue::Count(count)),
            ChunkScalarProjection::Sum => sum.map(ChunkScalarValue::Sum),
        };
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
    if next_first_seen_schema != schemas.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "schema table contains an unused schema",
        ));
    }
    if flags.finish() != expected_flags {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed chunk header flags disagree with native metadata",
        ));
    }
    if !lane_rows.is_empty() && lane_rows.len() != samples.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane has more rows than the native projection",
        ));
    }
    Ok(samples)
}

pub(super) fn decode_scalar_projection_schemas(
    kind: ChunkKind,
    buf: &[u8],
    cursor: &mut usize,
    point_count: usize,
) -> io::Result<Vec<ScalarProjectionSchema>> {
    let schema_count = decode_len(buf, cursor)?;
    if !(1..=point_count).contains(&schema_count) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "schema-varlen scalar schema count is inconsistent with the point count",
        ));
    }
    ensure_minimum_encoded_items(
        buf.len().saturating_sub(*cursor),
        schema_count,
        1,
        "schema-varlen scalar schemas",
    )?;
    let mut schemas = try_vec_with_capacity(schema_count, "decoded scalar schemas")?;
    let mut encoded_schemas = HashSet::new();
    encoded_schemas.try_reserve(schema_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("encoded scalar schema set allocation failed: {error}"),
        )
    })?;
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
        if !encoded_schemas.insert(schema_buf) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate schema definition is noncanonical",
            ));
        }
        schemas.push(decode_scalar_projection_schema(kind, schema_buf)?);
    }
    Ok(schemas)
}

pub(super) fn decode_scalar_projection_schema(
    kind: ChunkKind,
    schema_buf: &[u8],
) -> io::Result<ScalarProjectionSchema> {
    let mut cursor = 0usize;
    let schema = match kind {
        ChunkKind::Histogram => {
            let bounds_len = decode_len(schema_buf, &mut cursor)?;
            ensure_minimum_encoded_items(
                schema_buf.len().saturating_sub(cursor),
                bounds_len,
                8,
                "histogram schema bounds",
            )?;
            let mut previous_bound = None;
            for _ in 0..bounds_len {
                let bound = read_f64(schema_buf, &mut cursor)?;
                if !bound.is_finite() || previous_bound.is_some_and(|previous| previous >= bound) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "histogram explicit bounds must be finite and strictly ascending",
                    ));
                }
                previous_bound = Some(bound);
            }
            let bucket_len = decode_len(schema_buf, &mut cursor)?;
            let expected_bucket_len = bounds_len.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "histogram bucket length overflows",
                )
            })?;
            if bucket_len != expected_bucket_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "histogram bucket length must equal explicit bounds plus one",
                ));
            }
            ScalarProjectionSchema::Histogram { bucket_len }
        }
        ChunkKind::ExponentialHistogram => {
            let _scale = decode_i32(schema_buf, &mut cursor)?;
            let _zero_threshold = read_f64(schema_buf, &mut cursor)?;
            ScalarProjectionSchema::ExponentialHistogram
        }
        ChunkKind::Summary => {
            let quantile_len = decode_len(schema_buf, &mut cursor)?;
            ensure_minimum_encoded_items(
                schema_buf.len().saturating_sub(cursor),
                quantile_len,
                8,
                "summary schema quantiles",
            )?;
            let mut previous_quantile = None;
            for _ in 0..quantile_len {
                let quantile = read_f64(schema_buf, &mut cursor)?;
                if !quantile.is_finite()
                    || !(0.0..=1.0).contains(&quantile)
                    || previous_quantile.is_some_and(|previous| previous >= quantile)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "summary quantile positions must be finite, within [0, 1], and strictly ascending",
                    ));
                }
                previous_quantile = Some(quantile);
            }
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

pub(super) fn decode_scalar_projection_value(
    kind: ChunkKind,
    schema: ScalarProjectionSchema,
    buf: &[u8],
    cursor: &mut usize,
) -> io::Result<(TypedSampleMetadata, u64, Option<f64>)> {
    let metadata = decode_typed_metadata(buf, cursor)?;
    let (count, sum) = match (kind, schema) {
        (ChunkKind::Histogram, ScalarProjectionSchema::Histogram { bucket_len }) => {
            let count = decode_varint(buf, cursor)?;
            let sum = decode_opt_f64(buf, cursor)?;
            let _min = decode_opt_f64(buf, cursor)?;
            let _max = decode_opt_f64(buf, cursor)?;
            let bucket_total = decode_count_total(buf, cursor, bucket_len, "histogram")?;
            if bucket_total != count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "histogram bucket total must equal count",
                ));
            }
            (count, sum)
        }
        (ChunkKind::ExponentialHistogram, ScalarProjectionSchema::ExponentialHistogram) => {
            let count = decode_varint(buf, cursor)?;
            let sum = decode_opt_f64(buf, cursor)?;
            let _min = decode_opt_f64(buf, cursor)?;
            let _max = decode_opt_f64(buf, cursor)?;
            let zero_count = decode_varint(buf, cursor)?;
            let positive_total = decode_exponential_histogram_bucket_total(buf, cursor)?;
            let negative_total = decode_exponential_histogram_bucket_total(buf, cursor)?;
            let bucket_total = zero_count
                .checked_add(positive_total)
                .and_then(|total| total.checked_add(negative_total))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "exponential histogram bucket total overflows u64",
                    )
                })?;
            if bucket_total != count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "exponential histogram bucket total must equal count",
                ));
            }
            (count, sum)
        }
        (ChunkKind::Summary, ScalarProjectionSchema::Summary { quantile_len }) => {
            let count = decode_varint(buf, cursor)?;
            let sum = read_f64(buf, cursor)?;
            skip_f64s(buf, cursor, quantile_len)?;
            (count, Some(sum))
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "scalar projection schema kind mismatch",
            ));
        }
    };
    Ok((metadata, count, sum))
}

pub(super) fn decode_len(buf: &[u8], cursor: &mut usize) -> io::Result<usize> {
    let len = decode_varint(buf, cursor)?;
    usize::try_from(len).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length overflow"))
}

pub(super) fn decode_i32(buf: &[u8], cursor: &mut usize) -> io::Result<i32> {
    let encoded = decode_varint(buf, cursor)?;
    let decoded = decode_zigzag_i64(encoded);
    i32::try_from(decoded).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "i32 overflow"))
}

fn decode_count_total(
    buf: &[u8],
    cursor: &mut usize,
    count: usize,
    field: &'static str,
) -> io::Result<u64> {
    ensure_minimum_encoded_items(
        buf.len().saturating_sub(*cursor),
        count,
        1,
        "skipped varints",
    )?;
    let mut total = 0u64;
    for _ in 0..count {
        total = total
            .checked_add(decode_varint(buf, cursor)?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{field} bucket total overflows u64"),
                )
            })?;
    }
    Ok(total)
}

pub(super) fn skip_f64s(buf: &[u8], cursor: &mut usize, count: usize) -> io::Result<()> {
    ensure_minimum_encoded_items(
        buf.len().saturating_sub(*cursor),
        count,
        8,
        "skipped f64 values",
    )?;
    for _ in 0..count {
        let _ = read_f64(buf, cursor)?;
    }
    Ok(())
}

fn decode_exponential_histogram_bucket_total(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let _offset = decode_i32(buf, cursor)?;
    let len = decode_len(buf, cursor)?;
    decode_count_total(buf, cursor, len, "exponential histogram")
}

pub(super) fn decode_timestamps(
    buf: &[u8],
    cursor: &mut usize,
    t0_ms: u64,
    num_points: u32,
) -> io::Result<Vec<u64>> {
    let point_count = num_points as usize;
    ensure_minimum_encoded_items(
        buf.len().saturating_sub(*cursor),
        point_count,
        1,
        "timestamp deltas",
    )?;
    let mut timestamps = try_vec_with_capacity(point_count, "decoded timestamps")?;
    for _ in 0..num_points {
        let dt = decode_varint(buf, cursor)?;
        timestamps.push(checked_timestamp_ms(t0_ms, dt)?);
    }
    Ok(timestamps)
}

fn validate_full_chunk_point_count_feasible(
    kind: ChunkKind,
    encoding: ChunkEncoding,
    point_count: usize,
    available_payload_bytes_after_t0: usize,
) -> io::Result<()> {
    let minimum_bytes = match (kind, encoding) {
        (ChunkKind::Float, ChunkEncoding::RawF64) | (ChunkKind::Int64, ChunkEncoding::RawI64) => {
            point_count.checked_mul(9)
        }
        (ChunkKind::Float, ChunkEncoding::Gorilla) => {
            point_count.checked_add(minimum_gorilla_encoded_len_bytes(point_count)?)
        }
        (ChunkKind::Int64, ChunkEncoding::IntDeltaZigZag) => point_count.checked_mul(2),
        (
            ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary,
            ChunkEncoding::SchemaVarLen,
        ) => point_count
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(2)),
        _ => return Ok(()),
    }
    .ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk minimum payload size overflows",
        )
    })?;
    if minimum_bytes > available_payload_bytes_after_t0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk point count is infeasible for its encoded payload bytes",
        ));
    }
    Ok(())
}

fn ensure_minimum_encoded_items(
    available_bytes: usize,
    item_count: usize,
    minimum_bytes_per_item: usize,
    field: &'static str,
) -> io::Result<()> {
    let minimum_bytes = item_count
        .checked_mul(minimum_bytes_per_item)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{field} minimum encoded size overflows"),
            )
        })?;
    if minimum_bytes > available_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} count is infeasible for the remaining encoded bytes"),
        ));
    }
    Ok(())
}

fn try_vec_with_capacity<T>(count: usize, field: &'static str) -> io::Result<Vec<T>> {
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{field} allocation failed: {error}"),
        )
    })?;
    Ok(values)
}

fn zip_decoded_samples<T>(
    timestamps: Vec<u64>,
    values: Vec<T>,
    field: &'static str,
) -> io::Result<Vec<(u64, T)>> {
    if timestamps.len() != values.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded timestamp and value counts disagree",
        ));
    }
    let mut samples = try_vec_with_capacity(timestamps.len(), field)?;
    for sample in timestamps.into_iter().zip(values) {
        samples.push(sample);
    }
    Ok(samples)
}

fn validate_decoded_sample_timestamps(
    num_points: u32,
    min_time_ms: u64,
    max_time_ms: u64,
    samples: &ChunkSamples,
) -> io::Result<()> {
    match samples {
        ChunkSamples::Float(values) => validate_timestamp_iter(
            num_points,
            min_time_ms,
            max_time_ms,
            values.iter().map(|(timestamp, _)| *timestamp),
        ),
        ChunkSamples::Int64(values) => validate_timestamp_iter(
            num_points,
            min_time_ms,
            max_time_ms,
            values.iter().map(|(timestamp, _)| *timestamp),
        ),
        ChunkSamples::Histogram(values) => validate_timestamp_iter(
            num_points,
            min_time_ms,
            max_time_ms,
            values.iter().map(|(timestamp, _)| *timestamp),
        ),
        ChunkSamples::ExponentialHistogram(values) => validate_timestamp_iter(
            num_points,
            min_time_ms,
            max_time_ms,
            values.iter().map(|(timestamp, _)| *timestamp),
        ),
        ChunkSamples::Summary(values) => validate_timestamp_iter(
            num_points,
            min_time_ms,
            max_time_ms,
            values.iter().map(|(timestamp, _)| *timestamp),
        ),
    }
}

fn validate_timestamp_iter(
    num_points: u32,
    min_time_ms: u64,
    max_time_ms: u64,
    timestamps: impl ExactSizeIterator<Item = u64>,
) -> io::Result<()> {
    if timestamps.len() != num_points as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded point count disagrees with the chunk header",
        ));
    }
    let mut first = None;
    let mut previous = None;
    for timestamp in timestamps {
        first.get_or_insert(timestamp);
        if previous.is_some_and(|previous| timestamp < previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded chunk timestamps are not ordered",
            ));
        }
        previous = Some(timestamp);
    }
    if first != Some(min_time_ms) || previous != Some(max_time_ms) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded timestamp range disagrees with the chunk header",
        ));
    }
    Ok(())
}

fn checked_timestamp_ms(t0_ms: u64, delta_ms: u64) -> io::Result<u64> {
    t0_ms
        .checked_add(delta_ms)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chunk timestamp overflows u64"))
}

fn require_payload_end(payload: &[u8], cursor: usize) -> io::Result<()> {
    if cursor != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk value payload has trailing bytes",
        ));
    }
    Ok(())
}

pub(super) fn read_u64(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    if *cursor + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

pub(super) fn read_f64(buf: &[u8], cursor: &mut usize) -> io::Result<f64> {
    if *cursor + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = f64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

pub(super) fn read_i64(buf: &[u8], cursor: &mut usize) -> io::Result<i64> {
    if *cursor + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = i64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

pub(super) fn read_exact_u16(file: &mut File) -> io::Result<u16> {
    let mut buf = [0u8; 2];
    file.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

pub(super) fn read_exact_u32(file: &mut File) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub(super) fn read_exact_u64(file: &mut File) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

pub(super) fn write_chunk_entry(
    writer: &mut impl Write,
    entry: &ChunkIndexEntry,
) -> io::Result<()> {
    let mut buf = [0u8; CHUNK_ENTRY_LEN];
    buf[0] = entry.file_id;
    buf[1] = entry.kind as u8;
    buf[2..4].copy_from_slice(&entry.flags.to_le_bytes());
    buf[4..12].copy_from_slice(&entry.min_time_ms.to_le_bytes());
    buf[12..20].copy_from_slice(&entry.max_time_ms.to_le_bytes());
    buf[20..28].copy_from_slice(&entry.offset.to_le_bytes());
    buf[28..32].copy_from_slice(&entry.length.to_le_bytes());
    buf[32..36].copy_from_slice(&entry.scalar_lane_offset.to_le_bytes());
    buf[36..40].copy_from_slice(&entry.scalar_lane_len.to_le_bytes());
    writer.write_all(&buf)
}

pub(super) fn read_chunk_entry(reader: &mut impl Read) -> io::Result<ChunkIndexEntry> {
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
    let scalar_lane_offset = u32::from_le_bytes(buf[32..36].try_into().unwrap());
    let scalar_lane_len = u32::from_le_bytes(buf[36..40].try_into().unwrap());

    Ok(ChunkIndexEntry {
        file_id,
        kind,
        flags,
        min_time_ms,
        max_time_ms,
        offset,
        length,
        scalar_lane_offset,
        scalar_lane_len,
    })
}

macro_rules! decode_chunk_enum {
    ($function:ident, $enum:ident, [$($variant:ident),+ $(,)?], $error:literal) => {
        pub(super) fn $function(value: u8) -> io::Result<$enum> {
            match value {
                $(value if value == $enum::$variant as u8 => Ok($enum::$variant),)+
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, $error)),
            }
        }
    };
}

decode_chunk_enum!(
    chunk_encoding_from_u8,
    ChunkEncoding,
    [SchemaVarLen, RawF64, Gorilla, IntDeltaZigZag, RawI64],
    "unknown chunk encoding"
);
decode_chunk_enum!(
    chunk_kind_from_u8,
    ChunkKind,
    [Float, Int64, Histogram, ExponentialHistogram, Summary],
    "unknown chunk kind"
);

pub(super) fn chunk_entry_len() -> usize {
    CHUNK_ENTRY_LEN
}
