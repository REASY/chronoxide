use super::*;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentIndexDirectoryEntry {
    kind: u16,
    label_name_sym: u32,
    label_value_sym: u32,
    offset: u64,
    len: u64,
    min_time_ms: u64,
    max_time_ms: u64,
}

pub fn write_exact_postings_index(
    mut writer: impl Write,
    index: &ExactPostingsIndex,
) -> io::Result<()> {
    writer.write_all(&EXACT_POSTINGS_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.postings.len() as u32).to_le_bytes())?;

    for ((name, value), refs) in &index.postings {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
        writer.write_all(&(refs.len() as u32).to_le_bytes())?;
        for series_ref in refs {
            writer.write_all(&series_ref.to_le_bytes())?;
        }
    }

    Ok(())
}

pub fn read_exact_postings_index(mut reader: impl Read) -> io::Result<ExactPostingsIndex> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let mut cursor = 0usize;

    let magic = read_u32(&bytes, &mut cursor)?;
    if magic != EXACT_POSTINGS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "postings magic mismatch",
        ));
    }
    let version = read_u16(&bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported postings version",
        ));
    }
    let _flags = read_u16(&bytes, &mut cursor)?;
    let term_count = read_u32(&bytes, &mut cursor)? as usize;

    let mut index = ExactPostingsIndex::default();
    for _ in 0..term_count {
        let name = read_u32(&bytes, &mut cursor)?;
        let value = read_u32(&bytes, &mut cursor)?;
        let count = read_u32(&bytes, &mut cursor)? as usize;
        for _ in 0..count {
            let series_ref = read_u32(&bytes, &mut cursor)?;
            index.insert(name, value, series_ref);
        }
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "postings index has trailing bytes",
        ));
    }

    Ok(index)
}

pub fn write_label_value_fst_index(
    mut writer: impl Write,
    index: &LabelValueFstIndex,
) -> io::Result<()> {
    writer.write_all(&LABEL_VALUE_FST_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.fsts.len() as u32).to_le_bytes())?;

    for (name, bytes) in &index.fsts {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        writer.write_all(bytes)?;
    }

    Ok(())
}

pub fn read_label_value_fst_index(mut reader: impl Read) -> io::Result<LabelValueFstIndex> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    read_label_value_fst_index_bytes(&bytes)
}

pub fn write_label_value_time_range_index(
    mut writer: impl Write,
    index: &LabelValueTimeRangeIndex,
) -> io::Result<()> {
    writer.write_all(&LABEL_VALUE_TIME_RANGE_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.ranges.len() as u32).to_le_bytes())?;

    let mut ranges: Vec<_> = index.ranges.iter().collect();
    ranges.sort_unstable_by_key(|((name, value), _range)| (*name, *value));
    for ((name, value), range) in ranges {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
        writer.write_all(&range.min_time_ms.to_le_bytes())?;
        writer.write_all(&range.max_time_ms.to_le_bytes())?;
    }

    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
fn write_segment_indexes_v6(mut writer: impl Write, indexes: &SegmentIndexes) -> io::Result<()> {
    writer.write_all(&SEGMENT_INDEXES_MAGIC.to_le_bytes())?;
    writer.write_all(&SEGMENT_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;

    let mut entries = Vec::new();
    let mut offset = SEGMENT_INDEX_HEADER_LEN;
    let label_time_ranges = indexes.label_value_time_ranges.label_time_ranges();

    if let Some(routing_index) = &indexes.routing_index {
        let payload = routing_index.encode()?;
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_ROUTING,
                label_name_sym: NO_LABEL_VALUE_SYM,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            },
            &payload,
        )?;
    }

    let payload = write_metric_series_ranges_blob(&indexes.metric_series_ranges)?;
    write_segment_index_blob(
        &mut writer,
        &mut entries,
        &mut offset,
        SegmentIndexDirectoryEntry {
            kind: SEGMENT_INDEX_BLOB_METRIC_SERIES_RANGES,
            label_name_sym: NO_LABEL_VALUE_SYM,
            label_value_sym: NO_LABEL_VALUE_SYM,
            offset: 0,
            len: 0,
            min_time_ms: 0,
            max_time_ms: u64::MAX,
        },
        &payload,
    )?;

    for ((name, value), refs) in &indexes.exact_postings.postings {
        let payload = write_exact_postings_blob(refs)?;
        let range = indexes
            .label_value_time_ranges
            .get(*name, *value)
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_EXACT_POSTINGS,
                label_name_sym: *name,
                label_value_sym: *value,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            &payload,
        )?;
    }

    for (name, fst_bytes) in &indexes.label_values.fsts {
        let range = label_time_ranges
            .get(name)
            .copied()
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
                label_name_sym: *name,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            fst_bytes,
        )?;
    }

    for (name, ranges) in indexes.label_value_time_ranges.ranges_by_label() {
        let payload = write_label_value_time_ranges_blob(&ranges)?;
        let range = label_time_ranges
            .get(&name)
            .copied()
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                label_name_sym: name,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            &payload,
        )?;
    }

    let footer = encode_segment_index_footer(&entries)?;
    writer.write_all(&footer)?;
    writer.write_all(&(footer.len() as u64).to_le_bytes())?;
    writer.write_all(&SEGMENT_INDEX_TRAILER_MAGIC.to_le_bytes())?;

    Ok(())
}

#[cfg(test)]
fn write_segment_index_blob(
    writer: &mut impl Write,
    entries: &mut Vec<SegmentIndexDirectoryEntry>,
    offset: &mut u64,
    mut entry: SegmentIndexDirectoryEntry,
    payload: &[u8],
) -> io::Result<()> {
    entry.offset = *offset;
    entry.len = u64::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment index blob length exceeds u64",
        )
    })?;
    writer.write_all(payload)?;
    *offset = offset
        .checked_add(entry.len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "segment index too large"))?;
    entries.push(entry);
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
fn read_segment_indexes_v6_bytes(bytes: &[u8]) -> io::Result<SegmentIndexes> {
    let entries = parse_segment_index_directory(bytes)?;
    let mut exact_postings = ExactPostingsIndex::default();
    let mut label_values = LabelValueFstIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    let mut metric_series_ranges = None;
    let mut routing_index = None;

    for entry in entries {
        let payload = segment_index_blob_bytes(bytes, entry)?;
        match entry.kind {
            SEGMENT_INDEX_BLOB_ROUTING => {
                routing_index = Some(SegmentRoutingIndex::decode(payload)?);
            }
            SEGMENT_INDEX_BLOB_EXACT_POSTINGS => {
                for series_ref in read_exact_postings_blob(payload)? {
                    exact_postings.insert(entry.label_name_sym, entry.label_value_sym, series_ref);
                }
            }
            SEGMENT_INDEX_BLOB_LABEL_VALUE_FST => {
                label_values.insert_fst(entry.label_name_sym, payload.to_vec());
            }
            SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES => {
                for (value_sym, range) in read_label_value_time_ranges_blob(payload)? {
                    label_value_time_ranges.insert(
                        entry.label_name_sym,
                        value_sym,
                        range.min_time_ms,
                        range.max_time_ms,
                    );
                }
            }
            SEGMENT_INDEX_BLOB_METRIC_SERIES_RANGES => {
                metric_series_ranges = Some(read_metric_series_ranges_blob(payload)?);
            }
            _ => {}
        }
    }
    let metric_series_ranges = metric_series_ranges.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "required metric series ranges index blob is missing",
        )
    })?;

    Ok(SegmentIndexes {
        exact_postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges,
        routing_index,
    })
}

#[cfg(test)]
#[allow(dead_code)]
fn read_segment_index_directory(
    reader: &mut (impl Read + Seek),
) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    let len = reader.seek(SeekFrom::End(0))?;
    if len < SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index truncated",
        ));
    }

    reader.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; SEGMENT_INDEX_HEADER_LEN as usize];
    reader.read_exact(&mut header)?;
    validate_segment_index_header(&header)?;

    reader.seek(SeekFrom::End(-(SEGMENT_INDEX_TRAILER_LEN as i64)))?;
    let mut trailer = [0u8; SEGMENT_INDEX_TRAILER_LEN as usize];
    reader.read_exact(&mut trailer)?;
    let footer_len = u64::from_le_bytes(trailer[0..8].try_into().unwrap());
    let trailer_magic = u32::from_le_bytes(trailer[8..12].try_into().unwrap());
    if trailer_magic != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index trailer magic mismatch",
        ));
    }
    if footer_len > len.saturating_sub(SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length invalid",
        ));
    }

    let footer_start = len - SEGMENT_INDEX_TRAILER_LEN - footer_len;
    reader.seek(SeekFrom::Start(footer_start))?;
    let footer_len = usize::try_from(footer_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length exceeds platform usize",
        )
    })?;
    let mut footer = vec![0u8; footer_len];
    reader.read_exact(&mut footer)?;
    decode_segment_index_footer(&footer)
}

#[cfg(test)]
fn parse_segment_index_directory(bytes: &[u8]) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    if bytes.len() < (SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN) as usize {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index truncated",
        ));
    }
    validate_segment_index_header(&bytes[..SEGMENT_INDEX_HEADER_LEN as usize])?;

    let trailer_start = bytes.len() - SEGMENT_INDEX_TRAILER_LEN as usize;
    let footer_len =
        u64::from_le_bytes(bytes[trailer_start..trailer_start + 8].try_into().unwrap());
    let trailer_magic = u32::from_le_bytes(
        bytes[trailer_start + 8..trailer_start + 12]
            .try_into()
            .unwrap(),
    );
    if trailer_magic != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index trailer magic mismatch",
        ));
    }
    let footer_len = usize::try_from(footer_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length exceeds platform usize",
        )
    })?;
    if footer_len > trailer_start.saturating_sub(SEGMENT_INDEX_HEADER_LEN as usize) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length invalid",
        ));
    }
    let footer_start = trailer_start - footer_len;
    decode_segment_index_footer(&bytes[footer_start..trailer_start])
}

#[cfg(test)]
fn validate_segment_index_header(header: &[u8]) -> io::Result<()> {
    let mut cursor = 0usize;
    let magic = read_u32(header, &mut cursor)?;
    if magic != SEGMENT_INDEXES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment indexes magic mismatch",
        ));
    }
    let version = read_u16(header, &mut cursor)?;
    if version != SEGMENT_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported segment indexes version",
        ));
    }
    let _flags = read_u16(header, &mut cursor)?;
    Ok(())
}

#[cfg(test)]
fn encode_segment_index_footer(entries: &[SegmentIndexDirectoryEntry]) -> io::Result<Vec<u8>> {
    let mut footer = Vec::new();
    footer.extend_from_slice(&SEGMENT_INDEX_FOOTER_MAGIC.to_le_bytes());
    footer.extend_from_slice(&SEGMENT_INDEX_VERSION.to_le_bytes());
    footer.extend_from_slice(&0u16.to_le_bytes());
    footer.extend_from_slice(
        &(u32::try_from(entries.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment index directory entry count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    footer.extend_from_slice(&0u32.to_le_bytes());

    for entry in entries {
        footer.extend_from_slice(&entry.kind.to_le_bytes());
        footer.extend_from_slice(&0u16.to_le_bytes());
        footer.extend_from_slice(&entry.label_name_sym.to_le_bytes());
        footer.extend_from_slice(&entry.label_value_sym.to_le_bytes());
        footer.extend_from_slice(&entry.offset.to_le_bytes());
        footer.extend_from_slice(&entry.len.to_le_bytes());
        footer.extend_from_slice(&entry.min_time_ms.to_le_bytes());
        footer.extend_from_slice(&entry.max_time_ms.to_le_bytes());
    }

    Ok(footer)
}

#[cfg(test)]
fn decode_segment_index_footer(bytes: &[u8]) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    let mut cursor = 0usize;
    let magic = read_u32(bytes, &mut cursor)?;
    if magic != SEGMENT_INDEX_FOOTER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != SEGMENT_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported segment index footer version",
        ));
    }
    let _flags = read_u16(bytes, &mut cursor)?;
    let entry_count = read_u32(bytes, &mut cursor)? as usize;
    let _reserved = read_u32(bytes, &mut cursor)?;

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let kind = read_u16(bytes, &mut cursor)?;
        let _flags = read_u16(bytes, &mut cursor)?;
        let label_name_sym = read_u32(bytes, &mut cursor)?;
        let label_value_sym = read_u32(bytes, &mut cursor)?;
        let offset = read_u64(bytes, &mut cursor)?;
        let len = read_u64(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        entries.push(SegmentIndexDirectoryEntry {
            kind,
            label_name_sym,
            label_value_sym,
            offset,
            len,
            min_time_ms,
            max_time_ms,
        });
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer has trailing bytes",
        ));
    }
    Ok(entries)
}

#[cfg(test)]
fn segment_index_blob_bytes(bytes: &[u8], entry: SegmentIndexDirectoryEntry) -> io::Result<&[u8]> {
    let mut cursor = usize::try_from(entry.offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index blob offset exceeds platform usize",
        )
    })?;
    let len = usize::try_from(entry.len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index blob length exceeds platform usize",
        )
    })?;
    read_bytes(bytes, &mut cursor, len)
}

#[cfg(test)]
pub(in crate::storage::index) fn write_exact_postings_blob(refs: &[u32]) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &(u32::try_from(refs.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "postings list length exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for series_ref in refs {
        bytes.extend_from_slice(&series_ref.to_le_bytes());
    }
    Ok(bytes)
}

#[cfg(test)]
fn read_exact_postings_blob(bytes: &[u8]) -> io::Result<Vec<u32>> {
    let mut cursor = 0usize;
    let count = read_u32(bytes, &mut cursor)? as usize;
    let mut refs = Vec::with_capacity(count);
    for _ in 0..count {
        refs.push(read_u32(bytes, &mut cursor)?);
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "exact postings blob has trailing bytes",
        ));
    }
    Ok(refs)
}
