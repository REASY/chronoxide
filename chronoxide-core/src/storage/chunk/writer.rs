use super::*;

pub struct ChunkWriter {
    file: BufWriter<File>,
    offset: u64,
}

macro_rules! append_typed_chunk_ordered {
    ($method:ident, $kind:expr, $value:ty) => {
        pub fn $method(
            &mut self,
            series_ref: u32,
            samples: &[(u64, $value)],
        ) -> io::Result<ChunkIndexEntry> {
            self.append_schema_varlen_chunk_ordered(
                $kind,
                series_ref,
                samples,
                typed_chunk_flags(samples.iter().map(|(_, value)| value.metadata())),
            )
        }
    };
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

    append_typed_chunk_ordered!(
        append_histogram_chunk_ordered,
        ChunkKind::Histogram,
        HistogramValue
    );
    append_typed_chunk_ordered!(
        append_exponential_histogram_chunk_ordered,
        ChunkKind::ExponentialHistogram,
        ExponentialHistogramValue
    );
    append_typed_chunk_ordered!(
        append_summary_chunk_ordered,
        ChunkKind::Summary,
        SummaryValue
    );

    fn append_schema_varlen_chunk_ordered<T>(
        &mut self,
        kind: ChunkKind,
        series_ref: u32,
        samples: &[(u64, T)],
        flags: u16,
    ) -> io::Result<ChunkIndexEntry>
    where
        T: SchemaVarLenEncoding + Clone + TypedCounterValue,
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
        let scalar_lane = encode_typed_scalar_lane(t0_ms, samples)?;

        self.append_chunk_payload_with_scalar_lane(
            kind,
            ChunkEncoding::SchemaVarLen,
            flags,
            series_ref,
            min_time_ms,
            max_time_ms,
            samples.len() as u32,
            &payload,
            Some(&scalar_lane),
        )
    }

    pub fn append_float_chunk(
        &mut self,
        series_ref: u32,
        samples: &[(u64, f64)],
    ) -> io::Result<ChunkIndexEntry> {
        let sorted = samples_sorted_by_timestamp(samples)?;
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
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        })
    }

    pub fn append_float_chunk_raw(
        &mut self,
        series_ref: u32,
        samples: &[(u64, f64)],
    ) -> io::Result<ChunkIndexEntry> {
        let sorted = samples_sorted_by_timestamp(samples)?;
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
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        })
    }

    pub fn append_int_chunk(
        &mut self,
        series_ref: u32,
        samples: &[(u64, i64)],
    ) -> io::Result<ChunkIndexEntry> {
        let sorted = samples_sorted_by_timestamp(samples)?;
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
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        })
    }

    pub fn append_int_chunk_raw(
        &mut self,
        series_ref: u32,
        samples: &[(u64, i64)],
    ) -> io::Result<ChunkIndexEntry> {
        let sorted = samples_sorted_by_timestamp(samples)?;
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
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the arguments map directly to fixed chunk-header fields and its two payload lanes"
    )]
    fn append_chunk_payload_with_scalar_lane(
        &mut self,
        kind: ChunkKind,
        encoding: ChunkEncoding,
        flags: u16,
        series_ref: u32,
        min_time_ms: u64,
        max_time_ms: u64,
        num_points: u32,
        payload: &[u8],
        scalar_lane: Option<&[u8]>,
    ) -> io::Result<ChunkIndexEntry> {
        let payload_len = payload.len() as u32;
        let chunk_crc = crc32c(payload);
        let scalar_lane_len = scalar_lane.map(|bytes| bytes.len()).unwrap_or_default();
        let scalar_lane_offset = if scalar_lane_len == 0 {
            0
        } else {
            CHUNK_HEADER_LEN as u32
        };
        let scalar_lane_len_u32 = scalar_lane_len as u32;
        let header_len = (CHUNK_HEADER_LEN + scalar_lane_len) as u32;

        let mut chunk_header = Vec::with_capacity(CHUNK_HEADER_LEN);
        chunk_header.push(kind as u8);
        chunk_header.push(encoding as u8);
        chunk_header.extend_from_slice(&flags.to_le_bytes());
        chunk_header.extend_from_slice(&series_ref.to_le_bytes());
        chunk_header.extend_from_slice(&min_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&max_time_ms.to_le_bytes());
        chunk_header.extend_from_slice(&num_points.to_le_bytes());
        chunk_header.extend_from_slice(&header_len.to_le_bytes());
        chunk_header.extend_from_slice(&payload_len.to_le_bytes());
        chunk_header.extend_from_slice(&chunk_crc.to_le_bytes());

        let mut frame_crc_buf =
            Vec::with_capacity(chunk_header.len() + payload.len() + scalar_lane_len);
        frame_crc_buf.extend_from_slice(&chunk_header);
        if let Some(scalar_lane) = scalar_lane {
            frame_crc_buf.extend_from_slice(scalar_lane);
        }
        frame_crc_buf.extend_from_slice(payload);
        let frame_crc = crc32c(&frame_crc_buf);
        let frame_len = (FRAME_HEADER_LEN + frame_crc_buf.len()) as u32;

        let mut frame_header = Vec::with_capacity(FRAME_HEADER_LEN);
        frame_header.extend_from_slice(&frame_len.to_le_bytes());
        frame_header.extend_from_slice(&frame_crc.to_le_bytes());
        frame_header.extend_from_slice(&0u16.to_le_bytes());
        frame_header.extend_from_slice(&(1u32).to_le_bytes());

        let chunk_offset = self.offset + FRAME_HEADER_LEN as u64;
        let chunk_length = (CHUNK_HEADER_LEN + payload.len() + scalar_lane_len) as u32;

        self.file.write_all(&frame_header)?;
        self.file.write_all(&chunk_header)?;
        if let Some(scalar_lane) = scalar_lane {
            self.file.write_all(scalar_lane)?;
        }
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
            scalar_lane_offset,
            scalar_lane_len: scalar_lane_len_u32,
        })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn samples_sorted_by_timestamp<T: Clone>(samples: &[(u64, T)]) -> io::Result<Vec<(u64, T)>> {
    if samples.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "samples must be non-empty",
        ));
    }

    let mut sorted = samples.to_vec();
    sorted.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
    Ok(sorted)
}

pub(super) fn validate_ordered_samples<T>(samples: &[(u64, T)]) -> io::Result<()> {
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
