use super::*;

pub fn write_chunk_index(writer: impl Write, entries: &[Vec<ChunkIndexEntry>]) -> io::Result<()> {
    let mut writer = BufWriter::with_capacity(CHUNK_WRITE_BUFFER_BYTES, writer);
    let num_series = entries.len() as u32;
    let ranges = chunk_index_ranges(entries)?;

    writer.write_all(&CHUNK_INDEX_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&num_series.to_le_bytes())?;
    for range in &ranges {
        writer.write_all(&range.offset.to_le_bytes())?;
    }
    let end_offset = ranges
        .last()
        .map(|range| range.offset.saturating_add(u64::from(range.len)))
        .unwrap_or(CHUNK_INDEX_HEADER_LEN + 8);
    writer.write_all(&end_offset.to_le_bytes())?;

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

pub fn chunk_index_ranges(entries: &[Vec<ChunkIndexEntry>]) -> io::Result<Vec<ChunkIndexRange>> {
    let header_len = 4usize + 2 + 2 + 4;
    let offsets_len = entries
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_mul(8))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk index too large"))?;
    let mut cursor = u64::try_from(header_len + offsets_len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "chunk index too large"))?;
    let mut ranges = Vec::with_capacity(entries.len());
    for series_entries in entries {
        let len = series_entries
            .len()
            .checked_mul(chunk_entry_len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk index too large"))?;
        let len = u32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "per-series chunk index range exceeds u32",
            )
        })?;
        ranges.push(ChunkIndexRange {
            offset: cursor,
            len,
        });
        cursor = cursor
            .checked_add(u64::from(len))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk index too large"))?;
    }
    Ok(ranges)
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

    pub fn read_entries_range(
        &mut self,
        range: ChunkIndexRange,
    ) -> io::Result<Vec<ChunkIndexEntry>> {
        if range.len == 0 {
            return Ok(Vec::new());
        }
        self.validate_entry_range(range)?;
        self.file.seek(SeekFrom::Start(range.offset))?;
        let mut bytes = vec![0u8; range.len as usize];
        self.file.read_exact(&mut bytes)?;
        decode_chunk_entries_from_bytes(&bytes)
    }

    pub fn read_entries_ranges(
        &mut self,
        ranges: &[ChunkIndexRange],
    ) -> io::Result<HashMap<ChunkIndexRange, Vec<ChunkIndexEntry>>> {
        let mut out = HashMap::with_capacity(ranges.len());
        let mut non_empty = Vec::new();
        for range in ranges.iter().copied() {
            if range.len == 0 {
                out.insert(range, Vec::new());
            } else {
                self.validate_entry_range(range)?;
                non_empty.push(range);
            }
        }
        if non_empty.is_empty() {
            return Ok(out);
        }

        non_empty.sort_by_key(|range| (range.offset, range.len));
        non_empty.dedup();

        let mut span_start_idx = 0usize;
        while span_start_idx < non_empty.len() {
            let span_offset = non_empty[span_start_idx].offset;
            let mut span_end = range_end(non_empty[span_start_idx])?;
            let mut span_end_idx = span_start_idx + 1;
            while span_end_idx < non_empty.len() && non_empty[span_end_idx].offset == span_end {
                span_end = range_end(non_empty[span_end_idx])?;
                span_end_idx += 1;
            }

            let span_len = span_end.checked_sub(span_offset).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "chunk index span underflows")
            })?;
            let mut bytes = vec![
                0u8;
                usize::try_from(span_len).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "chunk index span too large")
                })?
            ];
            self.file.seek(SeekFrom::Start(span_offset))?;
            self.file.read_exact(&mut bytes)?;

            for range in &non_empty[span_start_idx..span_end_idx] {
                let start = usize::try_from(range.offset - span_offset).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "chunk index range too large")
                })?;
                let len = range.len as usize;
                let end = start.checked_add(len).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "chunk index range overflows")
                })?;
                out.insert(*range, decode_chunk_entries_from_bytes(&bytes[start..end])?);
            }

            span_start_idx = span_end_idx;
        }

        Ok(out)
    }

    fn validate_entry_range(&self, range: ChunkIndexRange) -> io::Result<()> {
        if range.offset < self.data_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk index range starts before entry data",
            ));
        }
        range_end(range)?;
        if !(range.len as usize).is_multiple_of(chunk_entry_len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk index entry length misaligned",
            ));
        }
        Ok(())
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

pub(super) fn range_end(range: ChunkIndexRange) -> io::Result<u64> {
    range
        .offset
        .checked_add(u64::from(range.len))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chunk index range overflows"))
}

pub(super) fn read_chunk_index_header(file: &mut File) -> io::Result<(usize, u64)> {
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

pub(super) fn read_chunk_index_offset_pair(
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

pub(super) fn read_chunk_index_offsets(file: &mut File) -> io::Result<Vec<u64>> {
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

pub(super) fn read_chunk_index_entries(
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

pub(super) fn read_chunk_index_entries_from_reader<R: Read>(
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

pub(super) fn decode_chunk_entries_from_bytes(bytes: &[u8]) -> io::Result<Vec<ChunkIndexEntry>> {
    if !bytes.len().is_multiple_of(chunk_entry_len()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index entry length misaligned",
        ));
    }
    let count = bytes.len() / chunk_entry_len();
    let mut reader = bytes;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(read_chunk_entry(&mut reader)?);
    }
    Ok(entries)
}

pub(super) fn read_chunk_index_entries_into<R: Read>(
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

pub(super) fn chunk_index_entry_count(
    offsets: &[u64],
    series_ref: usize,
    entry_len: u64,
) -> io::Result<usize> {
    let start = offsets[series_ref];
    let end = offsets[series_ref + 1];
    let len = end - start;
    if !len.is_multiple_of(entry_len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk index entry length misaligned",
        ));
    }
    Ok((len / entry_len) as usize)
}
