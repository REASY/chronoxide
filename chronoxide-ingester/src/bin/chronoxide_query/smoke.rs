fn render_markdown(
    config: &QuerySmokeConfig,
    storage_layout: StorageLayoutArg,
    report: &SegmentStoreSmokeReport,
    verification: Option<&QueryReadbackVerification>,
    diagnostics: Option<&QuerySmokeDiagnostics>,
) -> String {
    let mut markdown = String::new();

    markdown.push_str("# Chronoxide Query Smoke Report\n\n");
    markdown.push_str(&format!(
        "- Generated At: {}\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    markdown.push_str(&format!(
        "- Segments Directory: `{}`\n",
        config.segments_dir.display()
    ));
    markdown.push_str(&format!(
        "- Time Range: {}..{}\n",
        config.start_ms,
        format_end_ms(config.end_ms)
    ));
    markdown.push_str(&format!(
        "- Sample Limit Per Kind: {}\n\n",
        config.sample_limit_per_kind
    ));
    markdown.push_str(&format!("- Storage Layout: {}\n\n", storage_layout.name()));
    markdown.push_str(&format!(
        "- Requested Segment Footer Validation: {}\n\n",
        config.validate_segment_footers
    ));
    markdown.push_str(&format!(
        "- Effective Segment Footer Validation: {}\n\n",
        config.validate_segment_footers || storage_layout.forces_footer_validation()
    ));

    markdown.push_str("## Segment Totals\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Segments | {} |\n", report.totals.segments));
    markdown.push_str(&format!(
        "| Segment Datapoints | {} |\n",
        report.totals.datapoints
    ));
    markdown.push_str(&format!("| Segment Series | {} |\n", report.totals.series));
    markdown.push_str(&format!("| Chunks | {} |\n", report.totals.chunks));
    markdown.push_str(&format!(
        "| Chunk Bytes | {} |\n\n",
        report.totals.chunk_bytes
    ));

    markdown.push_str("## Chunk Kinds\n\n");
    markdown.push_str("| Kind | Chunks | Chunk Bytes |\n");
    markdown.push_str("| --- | ---: | ---: |\n");
    for kind in [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ] {
        let stats = kind_stats(report, kind);
        markdown.push_str(&format!(
            "| {} | {} | {} |\n",
            kind_name(kind),
            stats.chunks,
            stats.chunk_bytes
        ));
    }
    markdown.push('\n');

    markdown.push_str("## Sampled Native Series\n\n");
    markdown.push_str(
        "| Kind | Metric | Segment | Series Ref | Samples | Time Range Ms | Chunk Bytes | Labels |\n",
    );
    markdown.push_str("| --- | --- | --- | ---: | ---: | --- | ---: | --- |\n");
    for sample in &report.sample_series {
        markdown.push_str(&format!(
            "| {} | `{}` | `{}` | {} | {} | {}..{} | {} | `{}` |\n",
            kind_name(sample.kind),
            markdown_escape_inline(sample_metric_name(&sample.labels)),
            markdown_escape_inline(&sample.segment_id),
            sample.series_ref,
            sample.samples,
            sample.min_time_ms,
            sample.max_time_ms,
            sample.chunk_bytes,
            markdown_escape_inline(&format_labels(&sample.labels))
        ));
    }
    markdown.push('\n');

    markdown.push_str("## PromQL Readbacks\n\n");
    markdown.push_str("| Kind | Query | result_series | result_samples | matched_series | projected_series | chunk_reads | bytes_read | samples_decoded | typed_scalar_chunks_decoded | typed_full_chunks_decoded |\n");
    markdown
        .push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for query in &report.queries {
        markdown.push_str(&format!(
            "| {} | `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            kind_name(query.kind),
            markdown_escape_inline(&query.query),
            query.result_series,
            query.result_samples,
            query.matched_series,
            query.projected_series,
            query.chunk_reads,
            query.bytes_read,
            query.samples_decoded,
            query.typed_scalar_chunks_decoded,
            query.typed_full_chunks_decoded
        ));
    }

    if let Some(verification) = verification {
        markdown.push_str("\n## Readback Verification\n\n");
        markdown.push_str("| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Checked Queries | {} |\n",
            verification.checked_queries
        ));
        markdown.push_str(&format!(
            "| Mismatches | {} |\n",
            verification.mismatches.len()
        ));

        if !verification.mismatches.is_empty() {
            markdown.push_str("\n| Query | Expected Missing Samples | Actual Samples |\n");
            markdown.push_str("| --- | --- | --- |\n");
            for mismatch in &verification.mismatches {
                markdown.push_str(&format!(
                    "| `{}` | `{}` | `{}` |\n",
                    markdown_escape_inline(&mismatch.query),
                    markdown_escape_inline(&format_samples(&mismatch.missing_expected_samples)),
                    markdown_escape_inline(&format_samples(&mismatch.actual_samples))
                ));
            }
        }
    }

    if let Some(diagnostics) = diagnostics {
        append_query_diagnostics(&mut markdown, diagnostics);
    }

    markdown
}

fn append_query_diagnostics(markdown: &mut String, diagnostics: &QuerySmokeDiagnostics) {
    markdown.push_str("\n## Query Diagnostics\n\n");
    markdown.push_str("| Phase | Duration |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Store Open | {} |\n",
        format_duration(diagnostics.store_open)
    ));
    markdown.push_str(&format!(
        "| Smoke Verify | {} |\n",
        format_duration(diagnostics.smoke_verify)
    ));

    if let Some(readback) = &diagnostics.readback {
        markdown.push_str(&format!(
            "| Collect Expected Readbacks | {} |\n",
            format_duration(readback.collect_expected_readbacks)
        ));
        markdown.push_str(&format!(
            "| Readback Store Open | {} |\n",
            format_duration(readback.store_open)
        ));
        markdown.push_str(&format!(
            "| Query Session Open | {} |\n",
            format_duration(readback.query_session_open)
        ));
        markdown.push_str(&format!(
            "| Readback PromQL Queries | {} |\n",
            format_duration(readback.promql_queries)
        ));

        markdown.push_str("\n| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Expected Readback Queries | {} |\n",
            readback.expected_queries
        ));
        markdown.push_str(&format!(
            "| Executed Readback Queries | {} |\n",
            readback.executed_queries
        ));
        markdown.push_str(&format!(
            "| Skipped Readback Queries | {} |\n",
            readback.skipped_queries
        ));
        markdown.push_str(&format!(
            "| Isolation Check Skips | {} |\n",
            readback.isolation_check_skips
        ));
        markdown.push_str(&format!(
            "| Index Routing Opens | {} |\n",
            readback.session_stats.index_routing_opens
        ));
        markdown.push_str(&format!(
            "| Segment Context Opens | {} |\n",
            readback.session_stats.segment_context_opens
        ));
        markdown.push_str(&format!(
            "| Symbols Opens | {} |\n",
            readback.session_stats.symbols_bin_opens
        ));
        markdown.push_str(&format!(
            "| Indexes Opens | {} |\n",
            readback.session_stats.indexes_puffin_opens
        ));
        markdown.push_str(&format!(
            "| Series Opens | {} |\n",
            readback.session_stats.series_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunk Index Opens | {} |\n",
            readback.session_stats.chunk_index_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunks Opens | {} |\n",
            readback.session_stats.chunks_bin_opens
        ));
        render_profile_table(
            markdown,
            "Readback Query Session Read Profile",
            readback.session_profile,
        );
    }
}

fn format_duration(duration: Duration) -> String {
    format!("{duration:?}")
}

#[cfg(test)]
fn run_query_smoke(config: &QuerySmokeConfig) -> io::Result<SegmentStoreSmokeReport> {
    run_query_smoke_with_storage_layout(config, StorageLayoutArg::Schema8)
}

fn run_query_smoke_with_storage_layout(
    config: &QuerySmokeConfig,
    storage_layout: StorageLayoutArg,
) -> io::Result<SegmentStoreSmokeReport> {
    let mut diagnostics = QuerySmokeDiagnostics::default();
    let phase_start = Instant::now();
    let store = open_segment_store_for_layout_ab(
        &config.segments_dir,
        config.validate_segment_footers,
        query_projection_config(&config.exponential_histogram_bucket_boundaries),
        storage_layout,
    )?;
    diagnostics.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let report =
        store.smoke_verify(config.start_ms, config.end_ms, config.sample_limit_per_kind)?;
    diagnostics.smoke_verify = phase_start.elapsed();

    let verification = if config.verify_readbacks {
        let (verification, readback_diagnostics) =
            verify_readbacks(config, storage_layout, &report)?;
        diagnostics.readback = Some(readback_diagnostics);
        Some(verification)
    } else {
        None
    };
    let markdown = render_markdown(
        config,
        storage_layout,
        &report,
        verification.as_ref(),
        Some(&diagnostics),
    );

    if let Some(parent) = config
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.output, markdown)?;

    if let Some(verification) = verification
        && !verification.mismatches.is_empty()
    {
        return Err(io::Error::other(format!(
            "readback verification found {} mismatches",
            verification.mismatches.len()
        )));
    }

    Ok(report)
}

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryReadbackVerification {
    checked_queries: usize,
    mismatches: Vec<QueryReadbackMismatch>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QuerySmokeDiagnostics {
    store_open: Duration,
    smoke_verify: Duration,
    readback: Option<QueryReadbackDiagnostics>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QueryReadbackDiagnostics {
    collect_expected_readbacks: Duration,
    store_open: Duration,
    query_session_open: Duration,
    promql_queries: Duration,
    expected_queries: usize,
    executed_queries: usize,
    skipped_queries: usize,
    isolation_check_skips: usize,
    session_stats: SegmentStoreQuerySessionStats,
    session_profile: SegmentStoreQueryProfile,
}

#[derive(Debug, Clone, PartialEq)]
struct QueryReadbackMismatch {
    query: String,
    missing_expected_samples: Vec<(u64, f64)>,
    actual_samples: Vec<(u64, f64)>,
}

#[derive(Debug, Clone, PartialEq)]
struct ExpectedReadback {
    query: String,
    start_ms: u64,
    end_ms: u64,
    samples: Vec<(u64, f64)>,
    isolation_check: Option<ReadbackIsolationCheck>,
}

#[derive(Debug, Clone, PartialEq)]
struct ReadbackIsolationCheck {
    query: String,
    start_ms: u64,
    end_ms: u64,
    samples: Vec<(u64, f64)>,
}

impl ExpectedReadback {
    fn isolation_check(&self) -> ReadbackIsolationCheck {
        ReadbackIsolationCheck {
            query: self.query.clone(),
            start_ms: self.start_ms,
            end_ms: self.end_ms,
            samples: self.samples.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ProjectedCounterReadback {
    readback: ExpectedReadback,
    range_hints: Option<Vec<CounterResetHint>>,
}

#[derive(Debug)]
struct CorpusReadbackCandidate {
    kind: ChunkKind,
    labels: Vec<(String, String)>,
    records: Vec<ChunkRecord>,
}

fn verify_readbacks(
    config: &QuerySmokeConfig,
    storage_layout: StorageLayoutArg,
    report: &SegmentStoreSmokeReport,
) -> io::Result<(QueryReadbackVerification, QueryReadbackDiagnostics)> {
    let mut diagnostics = QueryReadbackDiagnostics::default();
    let required_kinds = required_readback_kinds(report);

    let phase_start = Instant::now();
    let expected = collect_expected_readbacks(config, storage_layout, &required_kinds)?;
    diagnostics.collect_expected_readbacks = phase_start.elapsed();
    diagnostics.expected_queries = expected.len();

    let phase_start = Instant::now();
    let store = open_segment_store_for_layout_ab(
        &config.segments_dir,
        config.validate_segment_footers,
        query_projection_config(&config.exponential_histogram_bucket_boundaries),
        storage_layout,
    )?;
    diagnostics.store_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let mut query_session = store.query_session()?;
    diagnostics.query_session_open = phase_start.elapsed();

    let phase_start = Instant::now();
    let verification = verify_expected_readbacks(&mut query_session, &expected, &mut diagnostics)?;
    diagnostics.promql_queries = phase_start.elapsed();
    diagnostics.session_stats = query_session.stats();
    diagnostics.session_profile = query_session.profile();

    Ok((verification, diagnostics))
}

fn verify_expected_readbacks(
    query_session: &mut SegmentStoreQuerySession<'_>,
    expected: &[ExpectedReadback],
    diagnostics: &mut QueryReadbackDiagnostics,
) -> io::Result<QueryReadbackVerification> {
    let mut mismatches = Vec::new();
    let mut actual_cache = BTreeMap::<(String, u64, u64), Vec<(u64, f64)>>::new();
    let mut checked_queries = 0usize;

    for expected in expected {
        if let Some(isolation_check) = &expected.isolation_check {
            let actual_samples = cached_readback_samples(
                query_session,
                &mut actual_cache,
                &isolation_check.query,
                isolation_check.start_ms,
                isolation_check.end_ms,
            )?;
            if !promql_samples_eq(&actual_samples, &isolation_check.samples) {
                diagnostics.skipped_queries = diagnostics.skipped_queries.saturating_add(1);
                diagnostics.isolation_check_skips =
                    diagnostics.isolation_check_skips.saturating_add(1);
                continue;
            }
        }

        let actual_samples = cached_readback_samples(
            query_session,
            &mut actual_cache,
            &expected.query,
            expected.start_ms,
            expected.end_ms,
        )?;
        diagnostics.executed_queries = diagnostics.executed_queries.saturating_add(1);
        checked_queries = checked_queries.saturating_add(1);
        let missing_expected_samples = expected
            .samples
            .iter()
            .copied()
            .filter(|sample| {
                !actual_samples
                    .iter()
                    .any(|actual| promql_sample_eq(*actual, *sample))
            })
            .collect::<Vec<_>>();
        if !missing_expected_samples.is_empty() {
            mismatches.push(QueryReadbackMismatch {
                query: expected.query.clone(),
                missing_expected_samples,
                actual_samples,
            });
        }
    }

    Ok(QueryReadbackVerification {
        checked_queries,
        mismatches,
    })
}

fn cached_readback_samples(
    query_session: &mut SegmentStoreQuerySession<'_>,
    actual_cache: &mut BTreeMap<(String, u64, u64), Vec<(u64, f64)>>,
    query: &str,
    start_ms: u64,
    end_ms: u64,
) -> io::Result<Vec<(u64, f64)>> {
    let key = (query.to_string(), start_ms, end_ms);
    if let Some(samples) = actual_cache.get(&key) {
        return Ok(samples.clone());
    }

    let results = query_session
        .query_promql(query, start_ms, end_ms)
        .map_err(|err| io::Error::other(format!("query failed: {query}: {err}")))?;
    let samples = results
        .iter()
        .flat_map(|result| result.samples.iter().copied())
        .collect::<Vec<_>>();
    actual_cache.insert(key, samples.clone());
    Ok(samples)
}

fn required_readback_kinds(report: &SegmentStoreSmokeReport) -> [bool; 5] {
    let mut required = [false; 5];
    for sample in &report.sample_series {
        required[chunk_kind_index(sample.kind)] = true;
    }
    required
}

fn collect_expected_readbacks(
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
                format!("invalid segment metadata in {}: {error}", segment_dir.display()),
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
                format!("invalid segment metadata in {}: {error}", segment_dir.display()),
            )
        })?;
        if meta.end_ms < config.start_ms || meta.start_ms > config.end_ms {
            continue;
        }
        let symbols = SegmentSymbolReader::open(File::open(
            segment_dir.join(SegmentFile::Symbols.filename()),
        )?)?;
        let mut oracle =
            schema7_readback_oracle::Schema7OracleSegment::open(&segment_dir, &meta)?;

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

fn merge_candidate_records(
    kind: ChunkKind,
    records: Vec<ChunkRecord>,
) -> io::Result<ChunkRecord> {
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
        let series_ref = u32::try_from(series_ref)
            .map_err(|_| invalid_data_error("series_ref exceeds u32"))?;
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

fn read_chunk_record_from_payload_files(
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

fn sample_limits_reached(
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

fn segment_dirs(segments_dir: &Path) -> io::Result<Vec<PathBuf>> {
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

#[cfg(test)]
fn open_segment_store(
    segments_dir: &Path,
    validate_segment_footers: bool,
    query_projection_config: QueryProjectionConfig,
) -> io::Result<SegmentStoreReader> {
    open_segment_store_for_layout_ab(
        segments_dir,
        validate_segment_footers,
        query_projection_config,
        StorageLayoutArg::Schema8,
    )
}

fn open_segment_store_for_layout_ab(
    segments_dir: &Path,
    validate_segment_footers: bool,
    query_projection_config: QueryProjectionConfig,
    storage_layout: StorageLayoutArg,
) -> io::Result<SegmentStoreReader> {
    let manifest_dir = segments_dir.join("manifest");
    let store = if read_manifest_inventory(&manifest_dir)?.is_some() {
        SegmentStoreReader::open_manifest_published_with_options(
            segments_dir,
            &manifest_dir,
            SegmentStoreOpenOptions {
                validate_segment_footers,
                storage_schema_policy: storage_layout.core_policy(),
                ..SegmentStoreOpenOptions::default()
            },
        )
    } else {
        SegmentStoreReader::open_with_options(
            segments_dir,
            SegmentStoreOpenOptions {
                validate_segment_footers,
                storage_schema_policy: storage_layout.core_policy(),
                ..SegmentStoreOpenOptions::default()
            },
        )
    }?;
    Ok(store.with_query_projection_config(query_projection_config))
}

fn query_projection_config(
    exponential_histogram_bucket_boundaries: &[f64],
) -> QueryProjectionConfig {
    QueryProjectionConfig::default().with_exponential_histogram_bucket_boundaries(
        exponential_histogram_bucket_boundaries.to_vec(),
    )
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

fn expected_readbacks_for_record(
    labels: &[(String, String)],
    record: &ChunkRecord,
    start_ms: u64,
    end_ms: u64,
    exponential_histogram_bucket_boundaries: &[f64],
) -> Vec<ExpectedReadback> {
    let Some(metric_name) = labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
    else {
        return Vec::new();
    };

    match &record.samples {
        ChunkSamples::Float(samples) => scalar_expected_readbacks(ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            start_ms,
            end_ms,
            samples: filter_samples(samples.iter().copied(), start_ms, end_ms),
            isolation_check: None,
        }),
        ChunkSamples::Int64(samples) => scalar_expected_readbacks(ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            start_ms,
            end_ms,
            samples: filter_samples(
                samples.iter().map(|(ts, value)| (*ts, *value as f64)),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        }),
        ChunkSamples::Histogram(samples) => {
            histogram_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
        ChunkSamples::ExponentialHistogram(samples) => exponential_histogram_expected_readbacks(
            metric_name,
            labels,
            samples,
            start_ms,
            end_ms,
            exponential_histogram_bucket_boundaries,
        ),
        ChunkSamples::Summary(samples) => {
            summary_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
    }
    .into_iter()
    .filter(|readback| !readback.samples.is_empty())
    .collect()
}

fn scalar_expected_readbacks(base: ExpectedReadback) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![base];
    if let Some((latest_ts, latest_value)) = readbacks[0]
        .samples
        .iter()
        .rev()
        .copied()
        .find(|(_, value)| value.is_finite())
        && latest_ts == readbacks[0].end_ms
    {
        readbacks.push(ExpectedReadback {
            query: format!("({}) * 2", readbacks[0].query),
            start_ms: latest_ts,
            end_ms: latest_ts,
            samples: vec![(latest_ts, latest_value * 2.0)],
            isolation_check: None,
        });
        readbacks.push(ExpectedReadback {
            query: format!("sum({})", readbacks[0].query),
            start_ms: latest_ts,
            end_ms: latest_ts,
            samples: vec![(latest_ts, latest_value)],
            isolation_check: None,
        });
    }

    let base = readbacks[0].clone();
    push_counter_range_readbacks(&mut readbacks, &base, None);
    readbacks
}

fn push_counter_range_readbacks(
    readbacks: &mut Vec<ExpectedReadback>,
    base: &ExpectedReadback,
    counter_reset_hints: Option<&[CounterResetHint]>,
) {
    let Some((range_ms, increase)) = scalar_counter_range_increase(base, counter_reset_hints)
    else {
        return;
    };
    let range_seconds = range_ms as f64 / 1_000.0;
    if range_seconds <= 0.0 {
        return;
    }

    readbacks.push(ExpectedReadback {
        query: format!("rate({}[{}ms])", base.query, range_ms),
        start_ms: base.end_ms,
        end_ms: base.end_ms,
        samples: vec![(base.end_ms, increase / range_seconds)],
        isolation_check: Some(base.isolation_check()),
    });
    readbacks.push(ExpectedReadback {
        query: format!("increase({}[{}ms])", base.query, range_ms),
        start_ms: base.end_ms,
        end_ms: base.end_ms,
        samples: vec![(base.end_ms, increase)],
        isolation_check: Some(base.isolation_check()),
    });
}

fn scalar_counter_range_increase(
    readback: &ExpectedReadback,
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<(u64, f64)> {
    let latest_ts = readback.end_ms;
    let earliest_ts = readback.samples.first()?.0;
    let range_ms = latest_ts.saturating_sub(earliest_ts).saturating_add(1);
    if range_ms == 0 {
        return None;
    }
    let range_start_ms = latest_ts.saturating_sub(range_ms);
    let range_start_before_epoch_ms = range_ms.saturating_sub(latest_ts);
    let include_range_start = range_start_before_epoch_ms > 0;
    let counter_reset_hints =
        counter_reset_hints.filter(|hints| hints.len() == readback.samples.len());
    let mut selected = Vec::new();
    let mut selected_hints = counter_reset_hints.map(|_| Vec::new());
    for (idx, sample) in readback.samples.iter().copied().enumerate() {
        let before_range = if include_range_start {
            sample.0 < range_start_ms
        } else {
            sample.0 <= range_start_ms
        };
        if before_range || sample.0 > latest_ts {
            continue;
        }
        if sample.1.to_bits() == prometheus_stale_nan().to_bits() {
            continue;
        }
        selected.push(sample);
        if let (Some(hints), Some(selected_hints)) = (counter_reset_hints, selected_hints.as_mut())
        {
            if let Some(hint) = hints.get(idx).copied() {
                selected_hints.push(hint);
            }
        }
    }
    if selected.len() < 2 {
        return None;
    }

    expected_extrapolated_counter_increase(
        &selected,
        selected_hints.as_deref(),
        range_start_ms,
        range_start_before_epoch_ms,
        latest_ts,
    )
    .map(|increase| (range_ms, increase))
}

fn expected_extrapolated_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    range_start_ms: u64,
    range_start_before_epoch_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if samples.len() < 2 || range_end_ms <= range_start_ms {
        return None;
    }

    let (first_ts, first_value) = samples.first().copied()?;
    let (last_ts, _) = samples.last().copied()?;
    if last_ts <= first_ts {
        return None;
    }

    let raw_increase = expected_counter_increase(samples, counter_reset_hints)?;
    let sampled_interval = (last_ts - first_ts) as f64 / 1_000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;
    let mut duration_to_start = first_ts
        .saturating_sub(range_start_ms)
        .saturating_add(range_start_before_epoch_ms) as f64
        / 1_000.0;
    let mut duration_to_end = range_end_ms.saturating_sub(last_ts) as f64 / 1_000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if raw_increase > 0.0 && first_value >= 0.0 {
        let duration_to_zero = sampled_interval * (first_value / raw_increase);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    Some(raw_increase * (sampled_interval + duration_to_start + duration_to_end) / sampled_interval)
}

fn expected_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
) -> Option<f64> {
    if let Some(counter_reset_hints) = counter_reset_hints {
        return expected_counter_increase_with_reset_hints(samples, counter_reset_hints);
    }
    expected_counter_increase_from_value_decreases(samples)
}

fn expected_counter_increase_with_reset_hints(
    samples: &[(u64, f64)],
    counter_reset_hints: &[CounterResetHint],
) -> Option<f64> {
    if counter_reset_hints.len() != samples.len() {
        return expected_counter_increase_from_value_decreases(samples);
    }
    if samples.len() < 2 {
        return None;
    }
    let mut iter = samples
        .iter()
        .copied()
        .zip(counter_reset_hints.iter().copied());
    let ((_, first), _) = iter.next()?;
    let last = samples.last()?.1;

    let mut previous = first;
    let mut increase = last - first;
    for ((_, current), reset_hint) in iter {
        let adjustment = match reset_hint {
            CounterResetHint::CounterReset => previous,
            CounterResetHint::NotCounterReset => {
                if previous.is_finite() && current.is_finite() && current < previous {
                    return None;
                }
                0.0
            }
            CounterResetHint::Unknown => {
                if current < previous {
                    previous
                } else {
                    0.0
                }
            }
            CounterResetHint::GaugeType => return None,
        };
        increase += adjustment;
        previous = current;
    }
    Some(increase)
}

fn expected_counter_increase_from_value_decreases(samples: &[(u64, f64)]) -> Option<f64> {
    let (_, first) = samples.first().copied()?;
    let (_, last) = samples.last().copied()?;

    let mut previous = first;
    let mut increase = last - first;
    for (_, current) in samples.iter().skip(1).copied() {
        if current < previous {
            increase += previous;
        }
        previous = current;
    }
    Some(increase)
}

fn histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let (count_samples, count_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    let mut projected = vec![ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            samples: count_samples,
            isolation_check: None,
        },
        range_hints: count_hints,
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        let (sum_samples, sum_hints) = project_optional_f64_counter_samples_with_range_hints(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
                start_ms,
                end_ms,
                samples: sum_samples,
                isolation_check: None,
            },
            range_hints: sum_hints,
        });
    }

    if let Some(le) = samples
        .first()
        .and_then(|(_, value)| value.explicit_bounds.first().copied())
        .map(format_promql_float_label)
    {
        let (bucket_samples, bucket_hints) = project_histogram_bucket_samples_with_range_hints(
            samples,
            Some(le.as_str()),
            start_ms,
            end_ms,
        );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(
                    &format!("{metric_name}_bucket"),
                    labels,
                    Some(("le", le.as_str())),
                ),
                start_ms,
                end_ms,
                samples: bucket_samples,
                isolation_check: None,
            },
            range_hints: bucket_hints,
        });
    }

    let (inf_bucket_samples, inf_bucket_hints) =
        project_histogram_bucket_samples_with_range_hints(samples, Some("+Inf"), start_ms, end_ms);
    projected.push(ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(
                &format!("{metric_name}_bucket"),
                labels,
                Some(("le", "+Inf")),
            ),
            start_ms,
            end_ms,
            samples: inf_bucket_samples,
            isolation_check: None,
        },
        range_hints: inf_bucket_hints,
    });

    let mut readbacks = projected
        .iter()
        .map(|projected| projected.readback.clone())
        .collect::<Vec<_>>();
    for projected in &projected {
        if let Some(hints) = &projected.range_hints {
            push_counter_range_readbacks(&mut readbacks, &projected.readback, Some(hints));
        }
    }
    readbacks
}

fn exponential_histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(
        u64,
        chronoxide_core::storage::head::ExponentialHistogramValue,
    )],
    start_ms: u64,
    end_ms: u64,
    exponential_histogram_bucket_boundaries: &[f64],
) -> Vec<ExpectedReadback> {
    let (count_samples, count_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    let mut projected = vec![ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            samples: count_samples,
            isolation_check: None,
        },
        range_hints: count_hints,
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        let (sum_samples, sum_hints) = project_optional_f64_counter_samples_with_range_hints(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.sum)),
            start_ms,
            end_ms,
        );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
                start_ms,
                end_ms,
                samples: sum_samples,
                isolation_check: None,
            },
            range_hints: sum_hints,
        });
    }

    for boundary in exponential_histogram_bucket_boundaries {
        let le = format_promql_float_label(*boundary);
        let (bucket_samples, bucket_hints) =
            project_exponential_histogram_bucket_samples_with_range_hints(
                samples, *boundary, start_ms, end_ms,
            );
        projected.push(ProjectedCounterReadback {
            readback: ExpectedReadback {
                query: promql_exact_selector(
                    &format!("{metric_name}_bucket"),
                    labels,
                    Some(("le", le.as_str())),
                ),
                start_ms,
                end_ms,
                samples: bucket_samples,
                isolation_check: None,
            },
            range_hints: bucket_hints,
        });
    }

    let (inf_bucket_samples, inf_bucket_hints) = project_u64_counter_samples_with_range_hints(
        samples
            .iter()
            .map(|(ts, value)| (*ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
    projected.push(ProjectedCounterReadback {
        readback: ExpectedReadback {
            query: promql_exact_selector(
                &format!("{metric_name}_bucket"),
                labels,
                Some(("le", "+Inf")),
            ),
            start_ms,
            end_ms,
            samples: inf_bucket_samples,
            isolation_check: None,
        },
        range_hints: inf_bucket_hints,
    });

    let mut readbacks = projected
        .iter()
        .map(|projected| projected.readback.clone())
        .collect::<Vec<_>>();
    for projected in &projected {
        if let Some(hints) = &projected.range_hints {
            push_counter_range_readbacks(&mut readbacks, &projected.readback, Some(hints));
        }
    }
    readbacks
}

fn project_exponential_histogram_bucket_samples_with_range_hints(
    samples: &[(
        u64,
        chronoxide_core::storage::head::ExponentialHistogramValue,
    )],
    le: f64,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0u64;
    let mut previous_non_stale_delta_timestamp_ms = None;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, value) in samples {
        if *ts < start_ms || *ts > end_ms {
            continue;
        }

        let raw = exponential_histogram_projected_bucket_count(value, le);
        let projected = if value.metadata.is_stale() {
            reset_delta_projection_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                &mut accumulator,
            );
            prometheus_stale_nan()
        } else if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
            if delta_interval_starts_new_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                *ts,
                value.metadata,
            ) {
                accumulator = 0;
            }
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            previous_non_stale_delta_timestamp_ms = None;
            raw as f64
        };
        if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(value.metadata.reset_hint);
        }
        out.push((*ts, projected));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn exponential_histogram_projected_bucket_count(
    value: &chronoxide_core::storage::head::ExponentialHistogramValue,
    le: f64,
) -> u64 {
    if le.is_infinite() && le.is_sign_positive() {
        return value.count;
    }

    let base = 2.0f64.powf(2.0f64.powi(-value.scale));
    let negative = exponential_histogram_negative_bucket_count_le(&value.negative, base, le);
    let zero = if le >= value.zero_threshold {
        value.zero_count
    } else {
        0
    };
    let positive = exponential_histogram_positive_bucket_count_le(&value.positive, base, le);
    negative
        .saturating_add(zero)
        .saturating_add(positive)
        .min(value.count)
}

fn exponential_histogram_positive_bucket_count_le(
    buckets: &chronoxide_core::storage::head::ExponentialHistogramBuckets,
    base: f64,
    le: f64,
) -> u64 {
    buckets
        .counts
        .iter()
        .enumerate()
        .filter_map(|(idx, count)| {
            let bucket_index = buckets
                .offset
                .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
            let upper = base.powi(bucket_index.saturating_add(1));
            (upper <= le).then_some(*count)
        })
        .fold(0u64, u64::saturating_add)
}

fn exponential_histogram_negative_bucket_count_le(
    buckets: &chronoxide_core::storage::head::ExponentialHistogramBuckets,
    base: f64,
    le: f64,
) -> u64 {
    buckets
        .counts
        .iter()
        .enumerate()
        .filter_map(|(idx, count)| {
            let bucket_index = buckets
                .offset
                .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
            let upper = -base.powi(bucket_index);
            (upper <= le).then_some(*count)
        })
        .fold(0u64, u64::saturating_add)
}

fn summary_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::SummaryValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            start_ms,
            end_ms,
            samples: project_u64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.count)),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        },
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            start_ms,
            end_ms,
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, Some(value.sum))),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        },
    ];

    if let Some(quantile) = samples
        .first()
        .and_then(|(_, value)| value.quantiles.first())
        .map(|quantile| format_promql_float_label(quantile.quantile))
    {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(
                metric_name,
                labels,
                Some(("quantile", quantile.as_str())),
            ),
            start_ms,
            end_ms,
            samples: filter_samples(
                samples.iter().map(|(ts, value)| {
                    let sample_value = value
                        .quantiles
                        .first()
                        .map(|quantile| quantile.value)
                        .unwrap_or(f64::NAN);
                    (
                        *ts,
                        typed_f64_value(value.metadata.is_stale(), sample_value),
                    )
                }),
                start_ms,
                end_ms,
            ),
            isolation_check: None,
        });
    }

    readbacks
}

fn project_u64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    project_u64_counter_samples_with_range_hints(samples, start_ms, end_ms).0
}

fn project_u64_counter_samples_with_range_hints(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0u64;
    let mut previous_non_stale_delta_timestamp_ms = None;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, metadata, raw) in samples {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            reset_delta_projection_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                &mut accumulator,
            );
            prometheus_stale_nan()
        } else if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
            if delta_interval_starts_new_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                ts,
                metadata,
            ) {
                accumulator = 0;
            }
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            previous_non_stale_delta_timestamp_ms = None;
            raw as f64
        };
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(metadata.reset_hint);
        }
        out.push((ts, value));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn project_optional_f64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    project_optional_f64_counter_samples_with_range_hints(samples, start_ms, end_ms).0
}

fn project_optional_f64_counter_samples_with_range_hints(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0.0f64;
    let mut previous_non_stale_delta_timestamp_ms = None;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, metadata, raw) in samples {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            reset_delta_projection_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                &mut accumulator,
            );
            prometheus_stale_nan()
        } else if let Some(raw) = raw {
            if metadata.temporality == OtlpAggregationTemporality::Delta {
                range_supported = false;
                if delta_interval_starts_new_fragment(
                    &mut previous_non_stale_delta_timestamp_ms,
                    ts,
                    metadata,
                ) {
                    accumulator = 0.0;
                }
                accumulator += raw;
                accumulator
            } else {
                previous_non_stale_delta_timestamp_ms = None;
                raw
            }
        } else {
            if metadata.temporality != OtlpAggregationTemporality::Delta {
                previous_non_stale_delta_timestamp_ms = None;
            }
            continue;
        };
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(metadata.reset_hint);
        }
        out.push((ts, value));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn project_histogram_bucket_samples_with_range_hints(
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    le_filter: Option<&str>,
    start_ms: u64,
    end_ms: u64,
) -> (Vec<(u64, f64)>, Option<Vec<CounterResetHint>>) {
    let mut accumulator = 0u64;
    let mut previous_non_stale_delta_timestamp_ms = None;
    let mut out = Vec::new();
    let mut range_hints = Vec::new();
    let mut range_supported = true;
    for (ts, value) in samples {
        if *ts < start_ms || *ts > end_ms {
            continue;
        }

        let mut cumulative = 0u64;
        let mut raw = None;
        for (idx, bound) in value.explicit_bounds.iter().enumerate() {
            cumulative =
                cumulative.saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
            let le = format_promql_float_label(*bound);
            if le_filter.is_some_and(|filter| filter == le) {
                raw = Some(cumulative);
                break;
            }
        }
        if le_filter.is_some_and(|filter| filter == "+Inf") {
            raw = Some(value.count);
        }
        let Some(raw) = raw else {
            continue;
        };

        let projected = if value.metadata.is_stale() {
            reset_delta_projection_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                &mut accumulator,
            );
            prometheus_stale_nan()
        } else if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
            if delta_interval_starts_new_fragment(
                &mut previous_non_stale_delta_timestamp_ms,
                *ts,
                value.metadata,
            ) {
                accumulator = 0;
            }
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            previous_non_stale_delta_timestamp_ms = None;
            raw as f64
        };
        if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            range_supported = false;
        } else {
            range_hints.push(value.metadata.reset_hint);
        }
        out.push((*ts, projected));
    }
    let range_hints = (range_supported && range_hints.len() == out.len()).then_some(range_hints);
    (out, range_hints)
}

fn delta_interval_starts_new_fragment(
    previous_non_stale_delta_timestamp_ms: &mut Option<u64>,
    timestamp_ms: u64,
    metadata: TypedSampleMetadata,
) -> bool {
    let discontinuous = previous_non_stale_delta_timestamp_ms
        .is_none_or(|previous_timestamp_ms| metadata.start_time_ms != Some(previous_timestamp_ms));
    *previous_non_stale_delta_timestamp_ms = Some(timestamp_ms);
    discontinuous
        || matches!(
            metadata.reset_hint,
            CounterResetHint::CounterReset | CounterResetHint::GaugeType
        )
}

fn reset_delta_projection_fragment<T: Default>(
    previous_non_stale_delta_timestamp_ms: &mut Option<u64>,
    accumulator: &mut T,
) {
    *previous_non_stale_delta_timestamp_ms = None;
    *accumulator = T::default();
}

fn filter_samples(
    samples: impl IntoIterator<Item = (u64, f64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    samples
        .into_iter()
        .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms)
        .collect()
}

fn typed_f64_value(stale: bool, value: f64) -> f64 {
    if stale { prometheus_stale_nan() } else { value }
}

fn promql_sample_eq(left: (u64, f64), right: (u64, f64)) -> bool {
    left.0 == right.0 && left.1.to_bits() == right.1.to_bits()
}

fn promql_samples_eq(left: &[(u64, f64)], right: &[(u64, f64)]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .copied()
            .zip(right.iter().copied())
            .all(|(left, right)| promql_sample_eq(left, right))
}

fn chunk_kind_index(kind: ChunkKind) -> usize {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}

fn format_end_ms(end_ms: u64) -> String {
    if end_ms == u64::MAX {
        "max".to_string()
    } else {
        end_ms.to_string()
    }
}

fn format_query_limit(limit: Option<u64>) -> String {
    limit
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unlimited".to_string())
}

fn kind_stats(report: &SegmentStoreSmokeReport, kind: ChunkKind) -> SegmentStoreSmokeKindStats {
    match kind {
        ChunkKind::Float => report.totals.by_kind.float,
        ChunkKind::Int64 => report.totals.by_kind.int64,
        ChunkKind::Histogram => report.totals.by_kind.histogram,
        ChunkKind::ExponentialHistogram => report.totals.by_kind.exponential_histogram,
        ChunkKind::Summary => report.totals.by_kind.summary,
    }
}

fn kind_name(kind: ChunkKind) -> &'static str {
    match kind {
        ChunkKind::Float => "Float",
        ChunkKind::Int64 => "Int64",
        ChunkKind::Histogram => "Histogram",
        ChunkKind::ExponentialHistogram => "ExponentialHistogram",
        ChunkKind::Summary => "Summary",
    }
}

fn sample_metric_name(labels: &[(String, String)]) -> &str {
    labels
        .iter()
        .find_map(|(name, value)| (name == METRIC_NAME_LABEL).then_some(value.as_str()))
        .unwrap_or("<missing>")
}

fn format_labels(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn promql_exact_selector(
    metric_name: &str,
    labels: &[(String, String)],
    extra_label: Option<(&str, &str)>,
) -> String {
    let mut matchers = Vec::with_capacity(labels.len() + usize::from(extra_label.is_some()));
    matchers.push(format!(
        r#"{}="{}""#,
        METRIC_NAME_LABEL,
        promql_escape_string(metric_name)
    ));
    for (key, value) in labels {
        if key == METRIC_NAME_LABEL || extra_label.is_some_and(|(extra_key, _)| extra_key == key) {
            continue;
        }
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    if let Some((key, value)) = extra_label {
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    format!("{{{}}}", matchers.join(","))
}

fn promql_escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}

fn format_promql_float_label(value: f64) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "+Inf".to_string()
    } else {
        value.to_string()
    }
}

fn format_samples(samples: &[(u64, f64)]) -> String {
    samples
        .iter()
        .map(|(ts, value)| format!("({ts}, {value:?})"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn markdown_escape_inline(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace('|', "\\|")
        .replace(['\n', '\r'], " ")
}
