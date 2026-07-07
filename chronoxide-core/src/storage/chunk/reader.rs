use super::*;

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
