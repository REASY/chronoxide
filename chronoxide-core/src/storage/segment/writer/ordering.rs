use super::*;

#[derive(Debug, Eq, PartialEq)]
struct SeriesQueryOrderKey {
    metric_name: String,
    kind_mask: u8,
    labels: Vec<(String, String)>,
    series_id: u64,
    old_ref: usize,
}

pub(in super::super) fn metric_query_series_order(
    series_entries: &[SeriesEntry],
    symbols: &SegmentSymbols,
) -> io::Result<Vec<usize>> {
    let mut keys = Vec::with_capacity(series_entries.len());
    for (old_ref, entry) in series_entries.iter().enumerate() {
        let mut labels = Vec::with_capacity(entry.labels.len());
        let mut metric_name = String::new();
        for (key, value) in &entry.labels {
            let key = symbols.resolve(*key).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series references missing key symbol",
                )
            })?;
            let value = symbols.resolve(*value).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series references missing value symbol",
                )
            })?;
            if key == METRIC_NAME_LABEL {
                metric_name = value.to_string();
            }
            labels.push((key.to_string(), value.to_string()));
        }
        labels.sort();
        keys.push(SeriesQueryOrderKey {
            metric_name,
            kind_mask: entry.kind_mask,
            labels,
            series_id: entry.series_id,
            old_ref,
        });
    }

    keys.sort_by(|left, right| {
        left.metric_name
            .cmp(&right.metric_name)
            .then_with(|| left.kind_mask.cmp(&right.kind_mask))
            .then_with(|| left.labels.cmp(&right.labels))
            .then_with(|| left.series_id.cmp(&right.series_id))
            .then_with(|| left.old_ref.cmp(&right.old_ref))
    });

    Ok(keys.into_iter().map(|key| key.old_ref).collect())
}

pub(in super::super) fn old_to_new_series_refs(order: &[usize]) -> io::Result<Vec<u32>> {
    let mut refs = vec![None; order.len()];
    for (new_ref, &old_ref) in order.iter().enumerate() {
        let Some(slot) = refs.get_mut(old_ref) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series order contains out-of-range ref",
            ));
        };
        if slot.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series order contains duplicate ref",
            ));
        }
        *slot =
            Some(u32::try_from(new_ref).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "series_ref exceeds u32")
            })?);
    }
    refs.into_iter()
        .map(|series_ref| {
            series_ref.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series order is missing a ref")
            })
        })
        .collect()
}

pub(in super::super) fn reorder_vec_by_old_indices<T>(
    items: Vec<T>,
    order: &[usize],
    name: &str,
) -> io::Result<Vec<T>> {
    if items.len() != order.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{name} count does not match series order"),
        ));
    }

    let mut slots: Vec<_> = items.into_iter().map(Some).collect();
    let mut ordered = Vec::with_capacity(order.len());
    for &old_ref in order {
        let Some(slot) = slots.get_mut(old_ref) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{name} order contains out-of-range ref"),
            ));
        };
        let Some(item) = slot.take() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{name} order contains duplicate ref"),
            ));
        };
        ordered.push(item);
    }
    Ok(ordered)
}

pub(in super::super) fn rewrite_chunks_in_series_major_order<L>(
    chunks_path: &Path,
    chunk_entries: &mut [L],
    series_order: &[usize],
    old_to_new_refs: &[u32],
) -> io::Result<ChunkRewriteStats>
where
    L: SeriesChunkEntries,
{
    if chunk_entries.len() != old_to_new_refs.len() || chunk_entries.len() != series_order.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk entry count does not match final series order",
        ));
    }
    if chunks_are_already_series_major_order(chunk_entries, series_order, old_to_new_refs) {
        return Ok(ChunkRewriteStats::default());
    }

    let rewrite_path =
        chunks_path.with_file_name(format!("{}.rewrite", SegmentFile::Chunks.filename()));
    let result = (|| {
        let mut source = File::open(chunks_path)?;
        let mut rewritten = File::create(&rewrite_path)?;
        let mut output_offset = 0u64;
        let mut stats = ChunkRewriteStats::default();

        for &old_ref in series_order {
            let new_ref = *old_to_new_refs.get(old_ref).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series order contains ref missing from ref map",
                )
            })?;
            let Some(entries) = chunk_entries.get_mut(old_ref) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series order contains ref missing from chunk entries",
                ));
            };

            entries.as_mut_slice().sort_by(chunk_entry_time_order);
            for entry in entries.as_mut_slice() {
                let payload_len = u64::from(entry.length);
                let frame_len = rewrite_single_chunk_frame(
                    &mut source,
                    &mut rewritten,
                    output_offset,
                    entry,
                    new_ref,
                )?;
                output_offset = output_offset.saturating_add(u64::from(frame_len));
                stats.frames = stats.frames.saturating_add(1);
                stats.payload_bytes = stats.payload_bytes.saturating_add(payload_len);
            }
        }

        rewritten.flush()?;
        Ok(stats)
    })();

    let stats = match result {
        Ok(stats) => stats,
        Err(err) => {
            let _ = fs::remove_file(&rewrite_path);
            return Err(err);
        }
    };

    fs::rename(rewrite_path, chunks_path)?;
    Ok(stats)
}

pub(in super::super) fn rewrite_chunks_in_identity_series_order<L>(
    chunks_path: &Path,
    chunk_entries: &mut [L],
) -> io::Result<ChunkRewriteStats>
where
    L: SeriesChunkEntries,
{
    if chunks_are_already_identity_series_major_order(chunk_entries) {
        return Ok(ChunkRewriteStats::default());
    }

    let series_order = (0..chunk_entries.len()).collect::<Vec<_>>();
    let old_to_new_refs = old_to_new_series_refs(&series_order)?;
    rewrite_chunks_in_series_major_order(
        chunks_path,
        chunk_entries,
        &series_order,
        &old_to_new_refs,
    )
}

fn chunks_are_already_series_major_order<L>(
    chunk_entries: &[L],
    series_order: &[usize],
    old_to_new_refs: &[u32],
) -> bool
where
    L: SeriesChunkEntries,
{
    if series_order
        .iter()
        .enumerate()
        .any(|(new_ref, &old_ref)| old_ref != new_ref)
    {
        return false;
    }
    if old_to_new_refs
        .iter()
        .enumerate()
        .any(|(old_ref, &new_ref)| new_ref as usize != old_ref)
    {
        return false;
    }

    chunks_are_already_identity_series_major_order(chunk_entries)
}

fn chunks_are_already_identity_series_major_order<L>(chunk_entries: &[L]) -> bool
where
    L: SeriesChunkEntries,
{
    let mut last_offset = None;
    for entries in chunk_entries {
        let entries = entries.as_slice();
        if entries
            .windows(2)
            .any(|pair| chunk_entry_time_order(&pair[0], &pair[1]).is_gt())
        {
            return false;
        }
        for entry in entries {
            if entry.file_id != 0 {
                return false;
            }
            if let Some(previous) = last_offset
                && entry.offset < previous
            {
                return false;
            }
            last_offset = Some(entry.offset);
        }
    }
    true
}

fn chunk_entry_time_order(left: &ChunkIndexEntry, right: &ChunkIndexEntry) -> std::cmp::Ordering {
    left.file_id
        .cmp(&right.file_id)
        .then_with(|| left.min_time_ms.cmp(&right.min_time_ms))
        .then_with(|| left.max_time_ms.cmp(&right.max_time_ms))
        .then_with(|| left.offset.cmp(&right.offset))
}

fn rewrite_single_chunk_frame(
    source: &mut File,
    rewritten: &mut File,
    output_offset: u64,
    entry: &mut ChunkIndexEntry,
    new_ref: u32,
) -> io::Result<u32> {
    if entry.file_id != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series-major chunk rewrite only supports chunks.bin entries",
        ));
    }
    if entry.length < CHUNK_FILE_HEADER_LEN as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk entry length is shorter than chunk header",
        ));
    }
    let frame_offset = entry
        .offset
        .checked_sub(CHUNK_FRAME_HEADER_LEN as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "chunk offset before frame"))?;
    let mut frame_header = [0u8; CHUNK_FRAME_HEADER_LEN];
    source.seek(SeekFrom::Start(frame_offset))?;
    source.read_exact(&mut frame_header)?;
    let frame_len = u32::from_le_bytes(frame_header[0..4].try_into().unwrap());
    let num_chunks = u32::from_le_bytes(frame_header[10..14].try_into().unwrap());
    let entry_len = usize::try_from(entry.length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunk entry length too large"))?;
    if num_chunks != 1 || frame_len as usize != CHUNK_FRAME_HEADER_LEN + entry_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series-major chunk rewrite requires single-chunk frames",
        ));
    }

    let mut chunk_payload = vec![0u8; entry_len];
    source.seek(SeekFrom::Start(entry.offset))?;
    source.read_exact(&mut chunk_payload)?;
    chunk_payload[4..8].copy_from_slice(&new_ref.to_le_bytes());
    let frame_crc = crc32c(&chunk_payload);
    frame_header[4..8].copy_from_slice(&frame_crc.to_le_bytes());

    let chunk_offset = output_offset.saturating_add(CHUNK_FRAME_HEADER_LEN as u64);
    rewritten.write_all(&frame_header)?;
    rewritten.write_all(&chunk_payload)?;
    entry.offset = chunk_offset;

    Ok(frame_len)
}

pub(in super::super) fn finalize_segment_symbol_ids<L>(
    mut symbols: SegmentSymbols,
    mut series_entries: Vec<SeriesEntry>,
    chunk_entries: &[L],
) -> io::Result<FinalizedSegmentMetadata>
where
    L: SeriesChunkEntries,
{
    if series_entries.len() != chunk_entries.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series and chunk entry counts differ",
        ));
    }

    for entry in &mut series_entries {
        synthesize_missing_metric_name(&mut symbols, entry)?;
    }

    let (sorted_symbols, remap) = symbols.sorted_remap()?;
    for entry in &mut series_entries {
        for (key, value) in &mut entry.labels {
            *key = remap_symbol_id(&remap, *key)?;
            *value = remap_symbol_id(&remap, *value)?;
        }
        entry.labels.sort_unstable_by_key(|(key, _)| *key);
    }

    let mut postings = ExactPostingsIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    for (local_ref, entry) in series_entries.iter().enumerate() {
        let local_ref = u32::try_from(local_ref)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "series_ref exceeds u32"))?;
        for (key, value) in &entry.labels {
            postings.insert_monotonic(*key, *value, local_ref);
        }
        for chunk in chunk_entries[local_ref as usize].as_slice() {
            update_label_value_time_ranges(&mut label_value_time_ranges, entry, chunk);
        }
    }

    Ok(FinalizedSegmentMetadata {
        symbols: sorted_symbols,
        series_entries,
        postings,
        label_value_time_ranges,
    })
}

pub(in super::super) fn synthesize_missing_metric_name(
    symbols: &mut SegmentSymbols,
    entry: &mut SeriesEntry,
) -> io::Result<()> {
    let mut has_metric_name = false;
    for (key_sym, value_sym) in &entry.labels {
        let key = symbols.resolve(*key_sym).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series references missing key symbol",
            )
        })?;
        symbols.resolve(*value_sym).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series references missing value symbol",
            )
        })?;
        if key == METRIC_NAME_LABEL {
            has_metric_name = true;
        }
    }

    if has_metric_name {
        return Ok(());
    }

    let mut labels = Vec::with_capacity(entry.labels.len() + 1);
    for (key_sym, value_sym) in &entry.labels {
        let key = symbols.resolve(*key_sym).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series references missing key symbol",
            )
        })?;
        let value = symbols.resolve(*value_sym).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series references missing value symbol",
            )
        })?;
        labels.push((key.to_string(), value.to_string()));
    }

    let key_sym = symbols.intern(METRIC_NAME_LABEL);
    let value_sym = symbols.intern("");
    entry.labels.push((key_sym, value_sym));
    labels.push((METRIC_NAME_LABEL.to_string(), String::new()));
    labels.sort_by(|left, right| left.0.cmp(&right.0));
    entry.series_id = segment_series_id(&labels);
    Ok(())
}

pub(in super::super) fn remap_symbol_id(remap: &[u32], symbol_id: u32) -> io::Result<u32> {
    remap.get(symbol_id as usize).copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "series references missing symbol id",
        )
    })
}
