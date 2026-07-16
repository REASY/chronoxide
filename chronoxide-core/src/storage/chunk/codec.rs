use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct DecodedChunkHeader {
    kind: ChunkKind,
    encoding: ChunkEncoding,
    series_ref: u32,
    min_time_ms: u64,
    max_time_ms: u64,
    num_points: u32,
    header_len: usize,
    payload_len: usize,
    chunk_crc: u32,
}

impl DecodedChunkHeader {
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
    series_ref: u32,
    min_time_ms: u64,
    max_time_ms: u64,
    num_points: u32,
    payload: &'a [u8],
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
    let series_ref = u32::from_le_bytes(chunk_header[4..8].try_into().unwrap());
    let min_time_ms = u64::from_le_bytes(chunk_header[8..16].try_into().unwrap());
    let max_time_ms = u64::from_le_bytes(chunk_header[16..24].try_into().unwrap());
    let num_points = u32::from_le_bytes(chunk_header[24..28].try_into().unwrap());
    let header_len = u32::from_le_bytes(chunk_header[28..32].try_into().unwrap()) as usize;
    let payload_len = u32::from_le_bytes(chunk_header[32..36].try_into().unwrap()) as usize;
    let chunk_crc = u32::from_le_bytes(chunk_header[36..40].try_into().unwrap());

    Ok(DecodedChunkHeader {
        kind,
        encoding,
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
    if decoded.header_len > payload.len()
        || decoded.header_len + decoded.payload_len > payload.len()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk payload bounds invalid",
        ));
    }

    let chunk_payload = &payload[decoded.header_len..decoded.header_len + decoded.payload_len];
    if crc32c(chunk_payload) != decoded.chunk_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk crc mismatch",
        ));
    }

    Ok(DecodedChunkPayload {
        kind: decoded.kind,
        encoding: decoded.encoding,
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

    let body_len = body.len() as u32;
    let body_crc = crc32c(&body);
    let mut lane = Vec::with_capacity(TYPED_SCALAR_LANE_HEADER_LEN + body.len());
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
    let mut samples = Vec::with_capacity(header.num_points as usize);
    for_each_typed_scalar_lane_sample(header, lane, projection, |sample| {
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
    mut on_sample: F,
) -> io::Result<()>
where
    F: FnMut(ChunkScalarSample) -> io::Result<()>,
{
    let body = typed_scalar_lane_body(header, lane)?;

    let mut timestamp_cursor = 0usize;
    let t0_ms = read_u64(body, &mut timestamp_cursor)?;
    let mut value_cursor = timestamp_cursor;
    // The lane stores all timestamp deltas before metadata/value rows.
    // Walk them once to position the value cursor, then replay them while decoding rows.
    for _ in 0..header.num_points {
        let _ = decode_varint(body, &mut value_cursor)?;
    }
    let values_start = value_cursor;

    for _ in 0..header.num_points {
        let timestamp_ms = t0_ms.saturating_add(decode_varint(body, &mut timestamp_cursor)?);
        let metadata = decode_typed_metadata(body, &mut value_cursor)?;
        let count = decode_varint(body, &mut value_cursor)?;
        let sum = decode_opt_f64(body, &mut value_cursor)?;
        let value = match projection {
            ChunkScalarProjection::Count => Some(ChunkScalarValue::Count(count)),
            ChunkScalarProjection::Sum => sum.map(ChunkScalarValue::Sum),
        };
        on_sample(ChunkScalarSample {
            timestamp_ms,
            metadata,
            value,
        })?;
    }
    if timestamp_cursor != values_start || value_cursor != body.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed scalar lane has trailing bytes",
        ));
    }

    Ok(())
}

fn typed_scalar_lane_body<'a>(header: &DecodedChunkHeader, lane: &'a [u8]) -> io::Result<&'a [u8]> {
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
    Ok(body)
}

pub(crate) fn decode_chunk_record(payload: &[u8]) -> io::Result<ChunkRecord> {
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

pub(super) fn decode_chunk_scalar_projection(
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

pub(super) fn decode_schema_varlen_scalar_samples(
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

pub(super) fn decode_scalar_projection_schemas(
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

pub(super) fn decode_scalar_projection_schema(
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

pub(super) fn decode_scalar_projection_value(
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

pub(super) fn decode_len(buf: &[u8], cursor: &mut usize) -> io::Result<usize> {
    let len = decode_varint(buf, cursor)?;
    usize::try_from(len).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length overflow"))
}

pub(super) fn decode_i32(buf: &[u8], cursor: &mut usize) -> io::Result<i32> {
    let encoded = decode_varint(buf, cursor)?;
    let decoded = decode_zigzag_i64(encoded);
    i32::try_from(decoded).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "i32 overflow"))
}

pub(super) fn skip_varints(buf: &[u8], cursor: &mut usize, count: usize) -> io::Result<()> {
    for _ in 0..count {
        let _ = decode_varint(buf, cursor)?;
    }
    Ok(())
}

pub(super) fn skip_f64s(buf: &[u8], cursor: &mut usize, count: usize) -> io::Result<()> {
    for _ in 0..count {
        let _ = read_f64(buf, cursor)?;
    }
    Ok(())
}

pub(super) fn skip_exponential_histogram_buckets(buf: &[u8], cursor: &mut usize) -> io::Result<()> {
    let _offset = decode_i32(buf, cursor)?;
    let len = decode_len(buf, cursor)?;
    skip_varints(buf, cursor, len)
}

pub(super) fn decode_timestamps(
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
