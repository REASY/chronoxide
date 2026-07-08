use super::*;

pub(super) struct SegmentQuerySessionReader<'a> {
    pub(super) reader: &'a SegmentReader,
    pub(super) context: Option<SegmentQueryContext>,
    pub(super) index_routing_reader: Option<SegmentIndexReader<File>>,
    pub(super) stats: SegmentStoreQuerySessionStats,
    pub(super) profile: SegmentStoreQueryProfile,
}

const CHUNK_PAYLOAD_COALESCE_MAX_GAP: u64 = 4096;

pub(super) struct SegmentQueryContext {
    pub(super) symbols: Arc<SegmentSymbols>,
    pub(super) index_reader: SegmentIndexReader<File>,
    pub(super) series_reader: Option<SeriesReader<File>>,
    pub(super) chunk_index_reader: Option<ChunkIndexReader>,
    pub(super) chunk_file: Option<File>,
    pub(super) stats: SegmentStoreQuerySessionStats,
    pub(super) profile: SegmentStoreQueryProfile,
}

impl SegmentQueryContext {
    pub(super) fn open(
        reader: &SegmentReader,
        index_reader: Option<SegmentIndexReader<File>>,
    ) -> io::Result<Self> {
        let context_start = Instant::now();
        let mut profile = SegmentStoreQueryProfile::default();
        let (index_reader, indexes_puffin_opens) = match index_reader {
            Some(index_reader) => (index_reader, 0),
            None => {
                let cached = reader.cached_index_reader()?;
                if !cached.cache_hit {
                    profile.indexes_file_bytes = cached.file_bytes;
                    profile.indexes_open = cached.open_elapsed;
                }
                (cached.reader, if cached.cache_hit { 0 } else { 1 })
            }
        };
        let symbols = reader.cached_symbols()?;
        if !symbols.cache_hit {
            profile.symbols_file_bytes = symbols.file_bytes;
            profile.symbols_read = symbols.open_elapsed;
        }
        profile.segment_context_open = context_start.elapsed();
        Ok(Self {
            symbols: symbols.symbols,
            index_reader,
            series_reader: None,
            chunk_index_reader: None,
            chunk_file: None,
            stats: SegmentStoreQuerySessionStats {
                segment_context_opens: 1,
                symbols_bin_opens: 1,
                indexes_puffin_opens,
                ..SegmentStoreQuerySessionStats::default()
            },
            profile,
        })
    }

    pub(super) fn series_reader(
        &mut self,
        reader: &SegmentReader,
    ) -> io::Result<&mut SeriesReader<File>> {
        if self.series_reader.is_none() {
            let path = reader.file_path(SegmentFile::Series);
            self.profile.series_file_bytes = self
                .profile
                .series_file_bytes
                .saturating_add(file_len(&path)?);
            let start = Instant::now();
            self.series_reader = Some(SeriesReader::open(File::open(path)?)?);
            self.profile.series_open = self.profile.series_open.saturating_add(start.elapsed());
            self.stats.series_bin_opens = self.stats.series_bin_opens.saturating_add(1);
        }
        Ok(self.series_reader.as_mut().unwrap())
    }

    pub(super) fn chunk_index_reader(
        &mut self,
        reader: &SegmentReader,
    ) -> io::Result<&mut ChunkIndexReader> {
        if self.chunk_index_reader.is_none() {
            let path = reader.file_path(SegmentFile::ChunkIndex);
            self.profile.chunk_index_file_bytes = self
                .profile
                .chunk_index_file_bytes
                .saturating_add(file_len(&path)?);
            let start = Instant::now();
            self.chunk_index_reader = Some(ChunkIndexReader::open(File::open(path)?)?);
            self.profile.chunk_index_open = self
                .profile
                .chunk_index_open
                .saturating_add(start.elapsed());
            self.stats.chunk_index_bin_opens = self.stats.chunk_index_bin_opens.saturating_add(1);
        }
        Ok(self.chunk_index_reader.as_mut().unwrap())
    }

    pub(super) fn chunk_file(&mut self, reader: &SegmentReader) -> io::Result<&mut File> {
        if self.chunk_file.is_none() {
            let path = reader.file_path(SegmentFile::Chunks);
            self.profile.chunks_file_bytes = self
                .profile
                .chunks_file_bytes
                .saturating_add(file_len(&path)?);
            let start = Instant::now();
            self.chunk_file = Some(reader.open_chunks()?);
            self.profile.chunks_open = self.profile.chunks_open.saturating_add(start.elapsed());
            self.stats.chunks_bin_opens = self.stats.chunks_bin_opens.saturating_add(1);
        }
        Ok(self.chunk_file.as_mut().unwrap())
    }

    pub(super) fn read_series_entries(
        &mut self,
        reader: &SegmentReader,
        series_refs: &[u32],
    ) -> io::Result<Vec<(u32, Arc<SeriesEntry>)>> {
        let mut cached_entries = HashMap::with_capacity(series_refs.len());
        let mut missing_refs = Vec::new();
        {
            let cached = reader
                .query_cache
                .series_entries
                .lock()
                .map_err(|_| io::Error::other("segment series entry cache lock poisoned"))?;
            for &series_ref in series_refs {
                if let Some(entry) = cached.get(&series_ref) {
                    cached_entries.insert(series_ref, entry.clone());
                } else {
                    missing_refs.push(series_ref);
                }
            }
        }

        if !missing_refs.is_empty() {
            let start = Instant::now();
            let mut locator_entries = Vec::new();
            let mut uncached_refs = Vec::new();
            {
                let cached =
                    reader.query_cache.series_locators.lock().map_err(|_| {
                        io::Error::other("segment series locator cache lock poisoned")
                    })?;
                for &series_ref in &missing_refs {
                    if let Some(locator) = cached.get(&series_ref) {
                        locator_entries.push((series_ref, **locator));
                    } else {
                        uncached_refs.push(series_ref);
                    }
                }
            }

            let mut bytes_read = 0u64;
            let mut loaded = Vec::new();
            if !locator_entries.is_empty() {
                let (entries, locator_bytes_read) = self
                    .series_reader(reader)?
                    .read_entries_from_locators_with_bytes(&locator_entries)?;
                bytes_read = bytes_read.saturating_add(locator_bytes_read);
                loaded.extend(entries);
            }
            if !uncached_refs.is_empty() {
                let (entries, uncached_bytes_read) = self
                    .series_reader(reader)?
                    .read_entries_with_bytes(&uncached_refs)?;
                bytes_read = bytes_read.saturating_add(uncached_bytes_read);
                loaded.extend(entries);
            }
            self.profile.series_entry_bytes =
                self.profile.series_entry_bytes.saturating_add(bytes_read);
            let loaded_entries = loaded
                .into_iter()
                .map(|(series_ref, entry)| (series_ref, Arc::new(entry)))
                .collect::<Vec<_>>();
            self.profile.series_entry_read = self
                .profile
                .series_entry_read
                .saturating_add(start.elapsed());
            self.profile.series_entries_read = self
                .profile
                .series_entries_read
                .saturating_add(loaded_entries.len() as u64);
            self.profile.series_entry_read_batches =
                self.profile.series_entry_read_batches.saturating_add(1);

            let mut cached = reader
                .query_cache
                .series_entries
                .lock()
                .map_err(|_| io::Error::other("segment series entry cache lock poisoned"))?;
            for (series_ref, entry) in loaded_entries {
                cached.insert(series_ref, Arc::clone(&entry));
                cached_entries.insert(series_ref, entry);
            }
        }

        Ok(series_refs
            .iter()
            .filter_map(|series_ref| {
                cached_entries
                    .remove(series_ref)
                    .map(|entry| (*series_ref, entry))
            })
            .collect())
    }

    pub(super) fn read_series_metadata_entries(
        &mut self,
        reader: &SegmentReader,
        series_refs: &[u32],
    ) -> io::Result<Vec<(u32, Arc<SeriesEntryMetadata>)>> {
        let mut cached_entries = HashMap::with_capacity(series_refs.len());
        let mut missing_refs = Vec::new();
        {
            let cached = reader
                .query_cache
                .series_metadata
                .lock()
                .map_err(|_| io::Error::other("segment series metadata cache lock poisoned"))?;
            for &series_ref in series_refs {
                if let Some(entry) = cached.get(&series_ref) {
                    cached_entries.insert(series_ref, entry.clone());
                } else {
                    missing_refs.push(series_ref);
                }
            }
        }

        if !missing_refs.is_empty() {
            let start = Instant::now();
            let (loaded, bytes_read) = self
                .series_reader(reader)?
                .read_entry_locators_with_bytes(&missing_refs)?;
            self.profile.series_entry_bytes =
                self.profile.series_entry_bytes.saturating_add(bytes_read);
            let loaded_entries = loaded
                .into_iter()
                .map(|(series_ref, locator)| {
                    (series_ref, Arc::new(locator), Arc::new(locator.metadata()))
                })
                .collect::<Vec<_>>();
            self.profile.series_entry_read = self
                .profile
                .series_entry_read
                .saturating_add(start.elapsed());

            {
                let mut cached =
                    reader.query_cache.series_metadata.lock().map_err(|_| {
                        io::Error::other("segment series metadata cache lock poisoned")
                    })?;
                for (series_ref, _, entry) in &loaded_entries {
                    cached.insert(*series_ref, Arc::clone(entry));
                    cached_entries.insert(*series_ref, Arc::clone(entry));
                }
            }
            {
                let mut cached =
                    reader.query_cache.series_locators.lock().map_err(|_| {
                        io::Error::other("segment series locator cache lock poisoned")
                    })?;
                for (series_ref, locator, _) in loaded_entries {
                    cached.insert(series_ref, locator);
                }
            }
        }

        Ok(series_refs
            .iter()
            .filter_map(|series_ref| {
                cached_entries
                    .remove(series_ref)
                    .map(|entry| (*series_ref, entry))
            })
            .collect())
    }

    pub(super) fn read_chunk_entry_ranges(
        &mut self,
        reader: &SegmentReader,
        ranges: &[ChunkIndexRange],
    ) -> io::Result<HashMap<ChunkIndexRange, Arc<Vec<ChunkIndexEntry>>>> {
        let mut cached_entries = HashMap::with_capacity(ranges.len());
        let mut missing_ranges = Vec::new();
        {
            let cached = reader
                .query_cache
                .chunk_entries
                .lock()
                .map_err(|_| io::Error::other("segment chunk entry cache lock poisoned"))?;
            for &range in ranges {
                if let Some(entries) = cached.get(&range) {
                    cached_entries.insert(range, entries.clone());
                } else {
                    missing_ranges.push(range);
                }
            }
        }

        if !missing_ranges.is_empty() {
            let start = Instant::now();
            let bytes_read = missing_ranges
                .iter()
                .map(|range| u64::from(range.len))
                .sum::<u64>();
            let loaded = self
                .chunk_index_reader(reader)?
                .read_entries_ranges(&missing_ranges)?;
            self.profile.chunk_index_range_bytes = self
                .profile
                .chunk_index_range_bytes
                .saturating_add(bytes_read);
            let loaded_entries = missing_ranges
                .into_iter()
                .filter_map(|range| loaded.get(&range).cloned().map(|entries| (range, entries)))
                .map(|(range, entries)| (range, Arc::new(entries)))
                .collect::<Vec<_>>();
            self.profile.chunk_index_range_read = self
                .profile
                .chunk_index_range_read
                .saturating_add(start.elapsed());

            let mut cached = reader
                .query_cache
                .chunk_entries
                .lock()
                .map_err(|_| io::Error::other("segment chunk entry cache lock poisoned"))?;
            for (range, entries) in loaded_entries {
                cached.insert(range, Arc::clone(&entries));
                cached_entries.insert(range, entries);
            }
        }

        Ok(cached_entries)
    }

    pub(super) fn read_chunk_payload_batch(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<ChunkPayloadBatch> {
        if requests.is_empty() {
            return Ok(ChunkPayloadBatch::empty());
        }

        let mut logical_ranges = Vec::with_capacity(requests.len());
        for request in requests {
            self.profile
                .observe_chunk_payload_read(request.offset, request.len);
            logical_ranges.push((request.offset, request.len));
        }
        self.profile
            .observe_sorted_chunk_payload_ranges(&mut logical_ranges);

        let start = Instant::now();
        let batch = read_chunk_payload_batch_from_file(
            self.chunk_file(reader)?,
            requests,
            CHUNK_PAYLOAD_COALESCE_MAX_GAP,
        )?;
        self.profile.chunk_read = self.profile.chunk_read.saturating_add(start.elapsed());
        self.profile.observe_chunk_payload_physical_reads(
            batch.physical_read_count(),
            batch.physical_bytes_read(),
        );
        Ok(batch)
    }

    pub(super) fn prefetch_chunk_range(
        &mut self,
        reader: &SegmentReader,
        offset: u64,
        len: u64,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let start = Instant::now();
        prefetch_file_range(self.chunk_file(reader)?, offset, len, scratch)?;
        self.profile.chunk_read = self.profile.chunk_read.saturating_add(start.elapsed());
        self.profile.observe_chunk_payload_read(offset, len);
        self.profile.observe_chunk_payload_physical_reads(1, len);
        Ok(())
    }

    pub(super) fn prewarm_query_files(&mut self, reader: &SegmentReader) -> io::Result<()> {
        self.series_reader(reader)?;
        self.chunk_index_reader(reader)?;
        self.chunk_file(reader)?;
        Ok(())
    }

    pub(super) fn metric_series_ranges(
        &mut self,
        reader: &SegmentReader,
        metric_sym: u32,
    ) -> io::Result<Vec<MetricSeriesRange>> {
        {
            let cached = reader
                .query_cache
                .metric_series_ranges
                .lock()
                .map_err(|_| io::Error::other("metric series range cache lock poisoned"))?;
            if let Some(index) = cached.as_ref() {
                return Ok(index.ranges(metric_sym).to_vec());
            }
        }

        let byte_len = self.index_reader.metric_series_ranges_byte_len();
        let start = Instant::now();
        let index = Arc::new(self.index_reader.metric_series_range_index()?);
        self.profile.metric_series_ranges_read = self
            .profile
            .metric_series_ranges_read
            .saturating_add(start.elapsed());
        self.profile.metric_series_ranges_bytes = self
            .profile
            .metric_series_ranges_bytes
            .saturating_add(byte_len);

        let ranges = index.ranges(metric_sym).to_vec();
        let mut cached = reader
            .query_cache
            .metric_series_ranges
            .lock()
            .map_err(|_| io::Error::other("metric series range cache lock poisoned"))?;
        if cached.is_none() {
            *cached = Some(index);
        }
        Ok(ranges)
    }
}

// Metadata-only segment pruning step. Keep this independent of postings/chunk decoding so
// future scan planners, including a DataFusion TableProvider, can reuse the same decision.
pub(super) fn plan_positive_equality_matchers(
    context: &SegmentQueryContext,
    matchers: &[NormalizedMatcher],
    start_ms: u64,
    end_ms: u64,
) -> Result<Vec<ResolvedEqualityMatcher>, SegmentPruneReason> {
    let mut equality_matchers = Vec::new();
    for matcher in matchers {
        let NormalizedMatcher::Eq { name, value } = matcher else {
            continue;
        };
        let Some(name_sym) = context.symbols.lookup(name) else {
            return Err(SegmentPruneReason::MissingEquality);
        };
        let Some(value_sym) = context.symbols.lookup(value) else {
            return Err(SegmentPruneReason::MissingEquality);
        };
        let Some(postings) = context
            .index_reader
            .exact_postings_metadata(name_sym, value_sym)
        else {
            return Err(SegmentPruneReason::MissingEquality);
        };
        if !postings.time_range.overlaps(start_ms, end_ms) {
            return Err(SegmentPruneReason::MatcherTimeRange);
        }
        equality_matchers.push(ResolvedEqualityMatcher {
            name_sym,
            value_sym,
            postings,
        });
    }
    equality_matchers.sort_by_key(|matcher| matcher.postings.byte_len);
    Ok(equality_matchers)
}

pub(super) fn has_positive_equality_matcher(matchers: &[NormalizedMatcher]) -> bool {
    matchers
        .iter()
        .any(|matcher| matches!(matcher, NormalizedMatcher::Eq { .. }))
}

impl<'a> SegmentQuerySessionReader<'a> {
    pub(super) fn open(reader: &'a SegmentReader) -> Self {
        Self {
            reader,
            context: None,
            index_routing_reader: None,
            stats: SegmentStoreQuerySessionStats::default(),
            profile: SegmentStoreQueryProfile::default(),
        }
    }

    pub(super) fn context(&mut self) -> io::Result<&mut SegmentQueryContext> {
        if self.context.is_none() {
            let index_reader = self.index_routing_reader.take();
            self.context = Some(SegmentQueryContext::open(self.reader, index_reader)?);
        }
        Ok(self.context.as_mut().unwrap())
    }

    pub(super) fn index_reader_for_routing(&mut self) -> io::Result<&mut SegmentIndexReader<File>> {
        if self.index_routing_reader.is_none() {
            let cached = self.reader.cached_index_reader()?;
            if !cached.cache_hit {
                self.profile.index_routing_file_bytes = self
                    .profile
                    .index_routing_file_bytes
                    .saturating_add(cached.file_bytes);
                self.profile.index_routing_open = self
                    .profile
                    .index_routing_open
                    .saturating_add(cached.open_elapsed);
                self.stats.index_routing_opens = self.stats.index_routing_opens.saturating_add(1);
            }
            self.index_routing_reader = Some(cached.reader);
        }
        Ok(self.index_routing_reader.as_mut().unwrap())
    }

    pub(super) fn plan_positive_equality_matchers_from_routing_index(
        &mut self,
        matchers: &[NormalizedMatcher],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Option<Result<(), SegmentPruneReason>>> {
        for matcher in matchers {
            let NormalizedMatcher::Eq { name, value } = matcher else {
                continue;
            };
            let reader = self.index_reader_for_routing()?;
            let start = Instant::now();
            let lookup = reader.routing_exact_postings_metadata(name, value)?;
            self.profile.routing_index_read = self
                .profile
                .routing_index_read
                .saturating_add(start.elapsed());
            self.profile.routing_index_bytes = self
                .profile
                .routing_index_bytes
                .saturating_add(lookup.bytes_read);
            if !lookup.index_present {
                return Ok(None);
            }
            let Some(postings) = lookup.metadata else {
                return Ok(Some(Err(SegmentPruneReason::MissingEquality)));
            };
            if !postings.time_range.overlaps(start_ms, end_ms) {
                return Ok(Some(Err(SegmentPruneReason::MatcherTimeRange)));
            }
        }
        Ok(Some(Ok(())))
    }

    pub(super) fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        projected_label_cache: &mut ProjectedLabelCache,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(Vec::new());
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(Vec::new());
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.query_normalized_with_context(
            context,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
            label_cache,
            projected_label_cache,
        )
    }

    pub(super) fn prewarm_selector(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<()> {
        let matchers = selector.normalized_matchers();
        if !has_positive_equality_matcher(&matchers) {
            return Ok(());
        }

        if self.context.is_none() {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(
                        SegmentPruneReason::MissingEquality | SegmentPruneReason::MatcherTimeRange,
                    ) => {
                        return Ok(());
                    }
                }
            }
        }

        let reader = self.reader;
        let context = self.context()?;
        if plan_positive_equality_matchers(context, &matchers, start_ms, end_ms).is_err() {
            return Ok(());
        }
        context.prewarm_query_files(reader)
    }

    pub(super) fn prefetch_selector_data_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        prefetch_stats: &mut QueryDataPrefetchStats,
    ) -> io::Result<()> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(());
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(());
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.prefetch_normalized_with_context(
            context,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
            prefetch_stats,
        )
    }
}

impl<'a> SegmentStoreQuerySession<'a> {
    pub(super) fn open(store: &'a SegmentStoreReader) -> io::Result<Self> {
        let mut segments = Vec::with_capacity(store.segments.len());
        for segment in &store.segments {
            segments.push(SegmentQuerySessionReader::open(segment));
        }
        Ok(Self {
            query_projection_config: store.query_projection_config.clone(),
            segments,
            label_cache: SeriesLabelCache::default(),
            projected_label_cache: ProjectedLabelCache::default(),
        })
    }

    pub fn query_selector(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_limits(selector, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_selector_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let results = self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
        Ok(QueryExecution {
            results,
            stats: budget.stats(),
        })
    }

    pub(super) fn query_selectors_with_limits(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        for selector in selectors {
            results.extend(self.query_selector_with_budget(
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )?);
        }
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub fn stats(&self) -> SegmentStoreQuerySessionStats {
        let mut stats = SegmentStoreQuerySessionStats::default();
        for segment in &self.segments {
            stats.add(segment.stats);
            if let Some(context) = &segment.context {
                stats.add(context.stats);
            }
        }
        stats
    }

    pub fn profile(&self) -> SegmentStoreQueryProfile {
        let mut profile = SegmentStoreQueryProfile::default();
        for segment in &self.segments {
            profile.add(segment.profile);
            if let Some(context) = &segment.context {
                profile.add(context.profile);
            }
        }
        profile
    }

    pub fn query_promql(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, limits)
    }

    pub fn prewarm_promql(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<SegmentStoreQuerySessionStats, PromqlQueryError> {
        self.prewarm_promql_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
    }

    pub fn prewarm_promql_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        _limits: QueryLimits,
    ) -> Result<SegmentStoreQuerySessionStats, PromqlQueryError> {
        let before = self.stats();
        let query = parse_query(query)?;
        self.prewarm_promql_query(&query, start_ms, end_ms)?;
        Ok(self.stats().delta_since(before))
    }

    pub fn prefetch_promql_data(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        self.prefetch_promql_data_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
    }

    pub fn prefetch_promql_data_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        let query = parse_query(query)?;
        self.prefetch_promql_data_query(&query, start_ms, end_ms, limits)
    }

    pub(super) fn prewarm_promql_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<(), PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.prewarm_selectors(&selectors, start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prewarm_promql_query(&aggregation.input, start_ms, end_ms)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prewarm_promql_query(&function.input, start_ms, end_ms)
            }
        }
    }

    pub(super) fn prefetch_promql_data_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.prefetch_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prefetch_promql_data_query(&aggregation.input, start_ms, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prefetch_promql_data_query(&function.input, start_ms, end_ms, limits)
            }
        }
    }

    pub(super) fn execute_promql_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                let mut execution =
                    self.execute_promql_query(&aggregation.input, start_ms, end_ms, limits)?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramQuantile(function) => {
                let mut execution =
                    self.execute_promql_query(&function.input, start_ms, end_ms, limits)?;
                execution.results =
                    evaluate_histogram_quantile(function, execution.results, end_ms);
                Ok(execution)
            }
        }
    }

    pub(super) fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let label_cache = &mut self.label_cache;
        let projected_label_cache = &mut self.projected_label_cache;
        for segment in &mut self.segments {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            results.extend(segment.query_selector_with_budget(
                selector,
                start_ms,
                end_ms,
                budget,
                label_cache,
                projected_label_cache,
            )?);
        }

        Ok(merge_query_results(results))
    }

    pub(super) fn prewarm_selectors(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        for selector in selectors {
            for segment in &mut self.segments {
                if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                    continue;
                }
                segment.prewarm_selector(selector, start_ms, end_ms)?;
            }
        }

        Ok(())
    }

    pub(super) fn prefetch_selectors_with_limits(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryDataPrefetchStats> {
        let mut budget = QueryBudget::new(limits);
        let mut prefetch_stats = QueryDataPrefetchStats::default();
        if end_ms < start_ms {
            return Ok(prefetch_stats);
        }

        for selector in selectors {
            for segment in &mut self.segments {
                budget.observe_segment_considered();
                if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                    budget.observe_segment_skipped_by_time();
                    continue;
                }
                segment.prefetch_selector_data_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    &mut budget,
                    &mut prefetch_stats,
                )?;
            }
        }

        prefetch_stats.query_stats = budget.stats();
        Ok(prefetch_stats)
    }
}

pub(super) fn histogram_projected_bucket_value(
    metadata: TypedSampleMetadata,
    raw: u64,
    le: &str,
    delta_accumulators: &mut BTreeMap<String, u64>,
) -> f64 {
    if metadata.is_stale() {
        return prometheus_stale_nan();
    }
    if metadata.temporality == OtlpAggregationTemporality::Delta {
        let accumulator = delta_accumulators.entry(le.to_string()).or_insert(0);
        *accumulator = accumulator.saturating_add(raw);
        *accumulator as f64
    } else {
        raw as f64
    }
}
