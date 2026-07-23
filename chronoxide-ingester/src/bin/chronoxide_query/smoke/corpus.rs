use super::oracle::expected_readbacks_for_record;
use super::*;

#[derive(Debug)]
struct CorpusReadbackCandidate {
    kind: ChunkKind,
    labels: Vec<(String, String)>,
    records: Vec<ChunkRecord>,
}

pub(in super::super) fn collect_expected_readbacks(
    config: &QuerySmokeConfig,
    storage_layout: StorageLayoutArg,
    required_kinds: &[bool; 5],
) -> io::Result<Vec<ExpectedReadback>> {
    if matches!(
        storage_layout,
        StorageLayoutArg::Schema7 | StorageLayoutArg::Schema8
    ) {
        return collect_schema7_corpus_readbacks(config, required_kinds);
    }

    let mut expected = Vec::new();
    let mut samples_by_kind = [0usize; 5];

    for segment_dir in segment_dirs(&config.segments_dir)? {
        if sample_limits_reached(
            &samples_by_kind,
            config.sample_limit_per_kind,
            required_kinds,
        ) {
            break;
        }
        let meta: SegmentMeta = serde_json::from_reader(File::open(
            segment_dir.join(SegmentFile::MetaJson.filename()),
        )?)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid segment metadata in {}: {error}",
                    segment_dir.display()
                ),
            )
        })?;
        if meta.end_ms < config.start_ms || meta.start_ms > config.end_ms {
            continue;
        }
        // This expected-value oracle intentionally decodes the immutable files
        // independently of the production segment reader. In particular, do
        // not let the production reader's strict default schema policy decide
        // how test and A/B corpora are opened here.
        let symbols = SegmentSymbolReader::open(File::open(
            segment_dir.join(SegmentFile::Symbols.filename()),
        )?)?;
        collect_schema6_segment_readbacks(
            config,
            required_kinds,
            &segment_dir,
            &symbols,
            &mut samples_by_kind,
            &mut expected,
        )?;
    }

    Ok(expected)
}

fn collect_schema7_corpus_readbacks(
    config: &QuerySmokeConfig,
    required_kinds: &[bool; 5],
) -> io::Result<Vec<ExpectedReadback>> {
    let mut candidates = Vec::<CorpusReadbackCandidate>::new();
    let mut candidate_by_key = BTreeMap::<(u64, ChunkKind), usize>::new();
    let mut candidates_by_kind = [0usize; 5];

    for segment_dir in segment_dirs(&config.segments_dir)? {
        let meta: SegmentMeta = serde_json::from_reader(File::open(
            segment_dir.join(SegmentFile::MetaJson.filename()),
        )?)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid segment metadata in {}: {error}",
                    segment_dir.display()
                ),
            )
        })?;
        if meta.end_ms < config.start_ms || meta.start_ms > config.end_ms {
            continue;
        }
        let symbols = SegmentSymbolReader::open(File::open(
            segment_dir.join(SegmentFile::Symbols.filename()),
        )?)?;
        let mut oracle = schema7_readback_oracle::Schema7OracleSegment::open(&segment_dir, &meta)?;

        for series_ref in 0..oracle.len() {
            let series = oracle.read_series(series_ref)?;
            let mut relevant_kinds = [false; 5];
            for chunk in &series.chunks {
                let entry = &chunk.entry;
                let kind_index = chunk_kind_index(entry.kind);
                if entry.max_time_ms >= config.start_ms
                    && entry.min_time_ms <= config.end_ms
                    && required_kinds[kind_index]
                    && config.sample_limit_per_kind != 0
                    && (candidate_by_key.contains_key(&(series.series_id, entry.kind))
                        || candidates_by_kind[kind_index] < config.sample_limit_per_kind)
                {
                    relevant_kinds[kind_index] = true;
                }
            }
            if !relevant_kinds.into_iter().any(|relevant| relevant) {
                continue;
            }

            let labels = resolve_label_ids(&symbols, &oracle.read_label_ids(&series)?)?;
            for chunk in &series.chunks {
                let entry = &chunk.entry;
                let kind_index = chunk_kind_index(entry.kind);
                if !relevant_kinds[kind_index]
                    || entry.max_time_ms < config.start_ms
                    || entry.min_time_ms > config.end_ms
                {
                    continue;
                }

                let key = (series.series_id, entry.kind);
                let candidate_index = if let Some(index) = candidate_by_key.get(&key).copied() {
                    if candidates[index].labels != labels {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "schema-7 oracle series identity resolves to different labels",
                        ));
                    }
                    index
                } else {
                    if candidates_by_kind[kind_index] >= config.sample_limit_per_kind {
                        continue;
                    }
                    let index = candidates.len();
                    candidates.push(CorpusReadbackCandidate {
                        kind: entry.kind,
                        labels: labels.clone(),
                        records: Vec::new(),
                    });
                    candidate_by_key.insert(key, index);
                    candidates_by_kind[kind_index] =
                        candidates_by_kind[kind_index].saturating_add(1);
                    index
                };
                candidates[candidate_index]
                    .records
                    .push(oracle.read_verified_chunk(series.series_ref, chunk)?);
            }
        }
    }

    let mut expected = Vec::new();
    for candidate in candidates {
        let record = merge_candidate_records(candidate.kind, candidate.records)?;
        let readback_start_ms = config.start_ms.max(record.min_time_ms);
        let readback_end_ms = config.end_ms.min(record.max_time_ms);
        expected.extend(expected_readbacks_for_record(
            &candidate.labels,
            &record,
            readback_start_ms,
            readback_end_ms,
            &config.exponential_histogram_bucket_boundaries,
        ));
    }
    Ok(expected)
}

fn merge_candidate_records(kind: ChunkKind, records: Vec<ChunkRecord>) -> io::Result<ChunkRecord> {
    let min_time_ms = records
        .iter()
        .map(|record| record.min_time_ms)
        .min()
        .ok_or_else(|| invalid_data_error("schema-7 oracle candidate has no records"))?;
    let max_time_ms = records
        .iter()
        .map(|record| record.max_time_ms)
        .max()
        .ok_or_else(|| invalid_data_error("schema-7 oracle candidate has no records"))?;
    let mut samples = match kind {
        ChunkKind::Float => ChunkSamples::Float(Vec::new()),
        ChunkKind::Int64 => ChunkSamples::Int64(Vec::new()),
        ChunkKind::Histogram => ChunkSamples::Histogram(Vec::new()),
        ChunkKind::ExponentialHistogram => ChunkSamples::ExponentialHistogram(Vec::new()),
        ChunkKind::Summary => ChunkSamples::Summary(Vec::new()),
    };
    for record in records {
        if record.kind != kind {
            return Err(invalid_data_error(
                "schema-7 oracle candidate mixes chunk kinds",
            ));
        }
        match (&mut samples, record.samples) {
            (ChunkSamples::Float(merged), ChunkSamples::Float(mut next)) => {
                merged.append(&mut next);
            }
            (ChunkSamples::Int64(merged), ChunkSamples::Int64(mut next)) => {
                merged.append(&mut next);
            }
            (ChunkSamples::Histogram(merged), ChunkSamples::Histogram(mut next)) => {
                merged.append(&mut next);
            }
            (
                ChunkSamples::ExponentialHistogram(merged),
                ChunkSamples::ExponentialHistogram(mut next),
            ) => {
                merged.append(&mut next);
            }
            (ChunkSamples::Summary(merged), ChunkSamples::Summary(mut next)) => {
                merged.append(&mut next);
            }
            _ => {
                return Err(invalid_data_error(
                    "schema-7 oracle candidate payload kind is inconsistent",
                ));
            }
        }
    }
    match &mut samples {
        ChunkSamples::Float(samples) => sort_dedupe_samples_keep_last(samples),
        ChunkSamples::Int64(samples) => sort_dedupe_samples_keep_last(samples),
        ChunkSamples::Histogram(samples) => sort_dedupe_samples_keep_last(samples),
        ChunkSamples::ExponentialHistogram(samples) => sort_dedupe_samples_keep_last(samples),
        ChunkSamples::Summary(samples) => sort_dedupe_samples_keep_last(samples),
    }
    Ok(ChunkRecord {
        series_ref: 0,
        kind,
        min_time_ms,
        max_time_ms,
        samples,
    })
}

fn sort_dedupe_samples_keep_last<T>(samples: &mut Vec<(u64, T)>) {
    samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
    samples.reverse();
    samples.dedup_by_key(|(timestamp_ms, _)| *timestamp_ms);
    samples.reverse();
}

fn collect_schema6_segment_readbacks(
    config: &QuerySmokeConfig,
    required_kinds: &[bool; 5],
    segment_dir: &Path,
    symbols: &SegmentSymbolReader<File>,
    samples_by_kind: &mut [usize; 5],
    expected: &mut Vec<ExpectedReadback>,
) -> io::Result<()> {
    let mut series_reader = SeriesReader::open(File::open(
        segment_dir.join(SegmentFile::Series.filename()),
    )?)?;
    let mut chunk_index_reader = ChunkIndexReader::open(File::open(
        segment_dir.join(SegmentFile::ChunkIndex.filename()),
    )?)?;
    let mut chunk_files = [
        File::open(segment_dir.join(SegmentFile::Chunks.filename()))?,
        File::open(segment_dir.join(SegmentFile::OooChunks.filename()))?,
    ];

    for series_ref in 0..chunk_index_reader.len() {
        if sample_limits_reached(
            samples_by_kind,
            config.sample_limit_per_kind,
            required_kinds,
        ) {
            break;
        }
        let series_ref =
            u32::try_from(series_ref).map_err(|_| invalid_data_error("series_ref exceeds u32"))?;
        let Some(entries) = chunk_index_reader.read_entries(series_ref)? else {
            continue;
        };
        let mut labels = None;
        for entry in entries {
            let kind_index = chunk_kind_index(entry.kind);
            if !readback_candidate_is_needed(config, required_kinds, samples_by_kind, &entry) {
                continue;
            }
            if labels.is_none() {
                let Some(series_entry) = series_reader.read_entry(series_ref)? else {
                    continue;
                };
                labels = Some(resolve_series_labels(symbols, &series_entry)?);
            }
            let record = read_chunk_record_from_payload_files(
                &mut chunk_files,
                entry.file_id,
                entry.offset,
                entry.length,
            )?;
            append_record_readbacks(
                config,
                labels.as_deref().unwrap_or_default(),
                &record,
                kind_index,
                samples_by_kind,
                expected,
            );
        }
    }
    Ok(())
}

fn readback_candidate_is_needed(
    config: &QuerySmokeConfig,
    required_kinds: &[bool; 5],
    samples_by_kind: &[usize; 5],
    entry: &chronoxide_core::storage::chunk::ChunkIndexEntry,
) -> bool {
    let kind_index = chunk_kind_index(entry.kind);
    entry.max_time_ms >= config.start_ms
        && entry.min_time_ms <= config.end_ms
        && required_kinds[kind_index]
        && config.sample_limit_per_kind != 0
        && samples_by_kind[kind_index] < config.sample_limit_per_kind
}

fn append_record_readbacks(
    config: &QuerySmokeConfig,
    labels: &[(String, String)],
    record: &ChunkRecord,
    kind_index: usize,
    samples_by_kind: &mut [usize; 5],
    expected: &mut Vec<ExpectedReadback>,
) {
    let readback_start_ms = config.start_ms.max(record.min_time_ms);
    let readback_end_ms = config.end_ms.min(record.max_time_ms);
    let mut readbacks = expected_readbacks_for_record(
        labels,
        record,
        readback_start_ms,
        readback_end_ms,
        &config.exponential_histogram_bucket_boundaries,
    );
    if !readbacks.is_empty() {
        samples_by_kind[kind_index] = samples_by_kind[kind_index].saturating_add(1);
        expected.append(&mut readbacks);
    }
}

fn invalid_data_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(in super::super) fn read_chunk_record_from_payload_files(
    chunk_files: &mut [File; 2],
    file_id: u8,
    offset: u64,
    length: u32,
) -> io::Result<ChunkRecord> {
    let chunk_file = chunk_files.get_mut(usize::from(file_id)).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk payload file_id must be 0 or 1",
        )
    })?;
    read_chunk_record_at(chunk_file, offset, length)
}

pub(in super::super) fn sample_limits_reached(
    samples_by_kind: &[usize; 5],
    sample_limit_per_kind: usize,
    required_kinds: &[bool; 5],
) -> bool {
    if sample_limit_per_kind == 0 {
        return true;
    }
    required_kinds
        .iter()
        .zip(samples_by_kind.iter())
        .all(|(required, samples)| !*required || *samples >= sample_limit_per_kind)
}

pub(in super::super) fn segment_dirs(segments_dir: &Path) -> io::Result<Vec<PathBuf>> {
    if let Some(inventory) = read_manifest_inventory(segments_dir.join("manifest"))? {
        return Ok(inventory
            .segments
            .into_iter()
            .map(|segment| segments_dir.join(segment.segment_id))
            .collect());
    }

    let mut dirs = Vec::new();
    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("seg-") {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn resolve_series_labels(
    symbols: &SegmentSymbolReader<File>,
    series_entry: &SeriesEntry,
) -> io::Result<Vec<(String, String)>> {
    resolve_label_ids(symbols, &series_entry.labels)
}

fn resolve_label_ids(
    symbols: &SegmentSymbolReader<File>,
    label_ids: &[(u32, u32)],
) -> io::Result<Vec<(String, String)>> {
    let mut labels = Vec::with_capacity(label_ids.len());
    for (key, value) in label_ids {
        let key = symbols.resolve(*key)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "series label key missing")
        })?;
        let value = symbols.resolve(*value)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "series label value missing")
        })?;
        labels.push((key.to_string(), value.to_string()));
    }
    Ok(labels)
}

pub(super) fn chunk_kind_index(kind: ChunkKind) -> usize {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}
